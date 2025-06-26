package config

import (
	"bytes"
	"encoding"
	"errors"
	"fmt"
	"github.com/joho/godotenv"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"text/template"

	"github.com/Masterminds/sprig/v3"
	"github.com/go-viper/mapstructure/v2"
	"github.com/infamousity/distributed-cache/internal/log"
	"github.com/spf13/viper"
	"mvdan.cc/sh/v3/shell"
)

// Config is the root of your application configuration.
type Config struct {
	Common struct {
		Cache struct {
			Cluster struct {
				MemberList struct {
					NodeName      string   `mapstructure:"node_name"`
					BindAddr      string   `mapstructure:"bind_address"`      // e.g. "0.0.0.0"
					BindPort      int      `mapstructure:"bind_port"`         // gossip port, e.g. 8946
					AdvertiseAddr string   `mapstructure:"advertise_address"` // optional; falls back to BindAddr
					AdvertisePort int      `mapstructure:"advertise_port"`    // usually same as BindPort
					SeedNodes     []string `mapstructure:"seed_nodes"`        // gossip seed nodes: ["10.10.1.3:8946", ...]
				} `mapstructure:"memberlist"`
			} `mapstructure:"cluster"`
			Doppio struct {
				BindAddr string `mapstructure:"bind_addr"`
				BindPort int    `mapstructure:"bind_port"`
			} `mapstructure:"doppio"`
			Log struct {
				Level string `mapstructure:"level"`
			} `mapstructure:"log"`
			SizeBytes int `mapstructure:"size_bytes"`
		} `mapstructure:"cache"`
	} `mapstructure:"common"`
}

// Load reads, merges, and decodes one or more config files in order.
// It falls back to $CACHE_CONFIG env (comma‐delimited) if no paths are passed.
func Load(paths ...string) (*Config, error) {
	l := log.Default()
	envFiles := make([]string, 0)
	rawEnvFiles := []string{
		ShouldExpand("./.env"),
		ShouldExpand("./.env.local"),
	}

	for _, re := range rawEnvFiles {
		if absRe, aerr := filepath.Abs(re); aerr != nil {
			continue
		} else {
			if fi, err := os.Stat(absRe); err == nil {
				if fi.Mode().IsRegular() && fi.Size() > 0 && !fi.IsDir() && !slices.Contains(envFiles, fi.Name()) {
					envFiles = append(envFiles, absRe)
				}
			}
		}
	}
	if err := godotenv.Overload(envFiles...); err != nil {
		l.With("error", err).Errorf("failed to load .env files: %s", strings.Join(envFiles, ", "))
		panic(err)
	}

	base := viper.New()
	base.SetEnvPrefix("CACHE")
	base.AutomaticEnv()
	base.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))

	// === defaults ===
	base.SetDefault("common.cache.size_bytes", 1<<30)
	// Env var bindings for deeply nested config fields
	_ = base.BindEnv("common.cache.cluster.memberlist.node_name", "NODE_NAME")
	_ = base.BindEnv("common.cache.cluster.memberlist.bind_port", "BIND_PORT")
	_ = base.BindEnv("common.cache.cluster.memberlist.api_port", "API_PORT")
	_ = base.BindEnv("common.cache.cluster.memberlist.seed_nodes", "SEED_NODES")
	_ = base.BindEnv("common.cache.doppio.bind_addr", "DOPPIO_ADDR")
	_ = base.BindEnv("common.cache.doppio.bind_port", "DOPPIO_PORT")
	_ = base.BindEnv("common.cache.log.level", "LOG_LEVEL")
	_ = base.BindEnv("common.cache.size", "CACHE_SIZE")
	sn, ok := os.LookupEnv("SEED_NODES")
	if ok {
		log.Default().Infof("Seed nodes: %s", sn)
	}

	// if no explicit paths, fallback to CACHE_CONFIG
	if len(paths) == 0 {
		if env := os.Getenv("CACHE_CONFIG"); env != "" {
			paths = strings.Split(env, ",")
		}
	}

	for _, path := range paths {
		if _, err := os.Stat(path); os.IsNotExist(err) {
			l.Warnf("warning: config file not found (%s), skipping", path)
			continue
		}
		ext := strings.TrimPrefix(strings.ToLower(filepath.Ext(path)), ".")
		tmp := viper.New()
		tmp.SetConfigFile(path)
		tmp.SetConfigType(ext)
		tmp.AutomaticEnv()
		tmp.SetEnvPrefix("CACHE")
		_ = tmp.BindEnv("common.cache.cluster.memberlist.node_name", "NODE_NAME")
		_ = tmp.BindEnv("common.cache.cluster.memberlist.bind_port", "BIND_PORT")
		_ = tmp.BindEnv("common.cache.cluster.memberlist.api_port", "API_PORT")
		_ = tmp.BindEnv("common.cache.cluster.memberlist.seed_nodes", "SEED_NODES")
		_ = tmp.BindEnv("common.cache.doppio.bind_addr", "DOPPIO_ADDR")
		_ = tmp.BindEnv("common.cache.doppio.bind_port", "DOPPIO_PORT")
		_ = tmp.BindEnv("common.cache.log.level", "LOG_LEVEL")
		_ = tmp.BindEnv("common.cache.size", "CACHE_SIZE")

		if err := tmp.ReadInConfig(); err != nil {
			return nil, fmt.Errorf("read config %s: %w", path, err)
		}
		if err := base.MergeConfigMap(tmp.AllSettings()); err != nil {
			return nil, fmt.Errorf("merge config %s: %w", path, err)
		}
	}

	// decode
	cfg := new(Config)
	hooks := mapstructure.ComposeDecodeHookFunc(
		mapstructure.StringToTimeDurationHookFunc(),
		mapstructure.StringToSliceHookFunc(","),
		TextUnmarshallerHookFunc(),
	)
	decCfg := &mapstructure.DecoderConfig{
		DecodeHook:       hooks,
		Result:           cfg,
		TagName:          "mapstructure",
		WeaklyTypedInput: true,
	}
	if dec, err := mapstructure.NewDecoder(decCfg); err != nil {
		return nil, fmt.Errorf("mapstructure decoder: %w", err)
	} else if err = dec.Decode(base.AllSettings()); err != nil {
		return nil, fmt.Errorf("decode config: %w", err)
	}
	potentialTemplates := []*string{
		&cfg.Common.Cache.Cluster.MemberList.NodeName,
		&cfg.Common.Cache.Cluster.MemberList.BindAddr,
		&cfg.Common.Cache.Cluster.MemberList.AdvertiseAddr,
		&cfg.Common.Cache.Doppio.BindAddr,
	}
	root := template.New("")
	root.Funcs(sprig.FuncMap())
	root.Funcs(template.FuncMap{
		"hostname": func(args ...string) (string, error) {
			if s, err := os.Hostname(); err != nil {
				return "", err
			} else {
				return strings.Split(s, ".")[0], nil
			}
		},
		"ip": func() (string, error) {
			return defaultBindAddress()
		},
	})
	for _, t := range potentialTemplates {
		if t == nil {
			continue
		}
		if strings.Contains(*t, "{{") && strings.Contains(*t, "}}") {
			if tmpl, err := root.Parse(*t); err != nil {
				continue
			} else {
				buf := new(bytes.Buffer)
				if err = tmpl.Execute(buf, struct{}{}); err != nil {
					continue
				} else {
					*t = buf.String()
				}
			}
		}
	}

	return cfg, nil
}

func TextUnmarshallerHookFunc() mapstructure.DecodeHookFuncType {
	return func(f reflect.Type, t reflect.Type, data interface{}) (interface{}, error) {
		if f.Kind() != reflect.String {
			return data, nil
		}
		result := reflect.New(t).Interface()
		unmarshaller, ok := result.(encoding.TextUnmarshaler)
		if !ok {
			return data, nil
		}
		str, ok := data.(string)
		if !ok {
			str = fmt.Sprintf("%v", data)
		}
		if err := unmarshaller.UnmarshalText([]byte(str)); err != nil {
			return nil, err
		}
		return reflect.ValueOf(result).Elem().Interface(), nil
	}
}

// defaultBindAddress returns the first non-loopback, non-docker-bridge IPv4 address.
func defaultBindAddress() (string, error) {
	validCIDRs := []string{
		"10.0.0.0/8",
		"172.0.0.0/8",
		"192.0.0.0/8",
	} // Acceptable LAN IPs
	ipnets := make([]*net.IPNet, 0)
	for _, validCIDR := range validCIDRs {
		_, ipnet, _ := net.ParseCIDR(validCIDR)
		ipnets = append(ipnets, ipnet)
	}

	ifaces, err := net.Interfaces()
	if err != nil {
		return "", err
	}
	for _, iface := range ifaces {
		if iface.Flags&net.FlagUp == 0 || iface.Flags&net.FlagLoopback != 0 {
			continue
		}
		addrs, addrerr := iface.Addrs()
		if addrerr != nil {
			continue
		}
		for _, addr := range addrs {
			ip := extractIP(addr)
			if ip == nil || ip.IsLoopback() || ip.To4() == nil {
				continue
			}
			if slices.ContainsFunc(ipnets, func(ipNet *net.IPNet) bool {
				return ipNet.Contains(ip)
			}) {
				return ip.String(), nil
			}
		}
	}
	return "", errors.New("no suitable LAN IPv4 address found")
}

func extractIP(addr net.Addr) net.IP {
	switch v := addr.(type) {
	case *net.IPNet:
		return v.IP
	case *net.IPAddr:
		return v.IP
	default:
		return nil
	}
}

func RecursiveExpand(v string) (expanded string, err error) {
	expanded, err = shell.Expand(v, nil)
	if err != nil {
		return
	}
	_tmp := expanded
	expanded, err = shell.Expand(_tmp, nil)
	if err != nil {
		return
	}
	if expanded != _tmp {
		return RecursiveExpand(expanded)
	}

	return
}

func ShouldExpand(v string) string {
	if e, err := RecursiveExpand(v); err == nil {
		return e
	} else {
		return v
	}
}

func MustExpand(v string) string {
	if e, err := RecursiveExpand(v); err != nil {
		panic(err)
	} else {
		return e
	}
}
