package config

import (
	"bytes"
	"encoding"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"sync"
	"text/template"
	"time"

	"github.com/Masterminds/sprig/v3"
	regexp "github.com/dlclark/regexp2"
	"github.com/go-viper/mapstructure/v2"
	"github.com/joho/godotenv"
	"github.com/spf13/viper"
	"mvdan.cc/sh/v3/shell"

	"github.com/infamousity/distributed-cache/internal/log"
)

var (
	tyTime          = reflect.TypeOf((*time.Time)(nil)).Elem()
	tyTimePtr       = reflect.TypeOf((*time.Time)(nil))
	bindAddressOnce sync.Once
	bindAddress     string
	bindAddressErr  error
	bindFilter      = []string{"10.0.0.0/8", "172.0.0.0/8", "192.0.0.0/8"}
	ifcPriority     = []string{
		`en.*`,
		`eth.*`,
		`wl.*`,
	}
)

// Config is the root application configuration.
type Config struct {
	Common struct {
		Cache struct {
			Cluster struct {
				Tls struct {
					Enabled           bool   `mapstructure:"enabled"`
					CertFile          string `mapstructure:"cert_file,omitempty"`
					KeyFile           string `mapstructure:"key_file,omitempty"`
					CaFile            string `mapstructure:"ca_file,omitempty"`
					ServerName        string `mapstructure:"server_name,omitempty"`
					ClientCertFile    string `mapstructure:"client_cert_file,omitempty"`
					ClientKeyFile     string `mapstructure:"client_key_file,omitempty"`
					RequireClientCert bool   `mapstructure:"require_client_cert"`
				} `mapstructure:"tls"`
				MemberList struct {
					NodeName          string   `mapstructure:"node_name"`
					BindAddr          string   `mapstructure:"bind_address"`            // e.g. "0.0.0.0"
					BindAddrFilter    []string `mapstructure:"bind_address_filter"`     // defaults to ["10.0.0.0/8", "172.0.0.0/8", "192.0.0.0/8"]
					BindIfcPriority   []string `mapstructure:"bind_interface_priority"` // defaults to ["^en.*$", "^eth.*$", "^wl.*$"]
					BindPort          int      `mapstructure:"bind_port"`               // gossip port, e.g. 8946
					AdvertiseAddr     string   `mapstructure:"advertise_address"`       // optional; falls back to BindAddr
					AdvertisePort     int      `mapstructure:"advertise_port"`          // usually same as BindPort
					SeedNodes         []string `mapstructure:"seed_nodes"`              // gossip seed nodes: ["10.10.1.3:8946", ...]
					SeedDNSName       string   `mapstructure:"seed_dns_name"`           // optional DNS name resolved into seed nodes
					SeedDNSPort       int      `mapstructure:"seed_dns_port"`           // port used with seed_dns_name
					PartitionCount    int      `mapstructure:"partition_count"`         // max partitions in this memberlist (default: 271)
					ReplicationFactor int      `mapstructure:"replication_factor"`      // number of replicas (default: 3)
				} `mapstructure:"memberlist"`
			} `mapstructure:"cluster"`
			Control struct {
				BindAddr      string `mapstructure:"bind_addr"`
				BindPort      int    `mapstructure:"bind_port"`
				AdvertiseAddr string `mapstructure:"advertise_addr"`
			} `mapstructure:"api"`
			Log struct {
				Level string `mapstructure:"level"`
			} `mapstructure:"log"`
			SizeBytes    int    `mapstructure:"size_bytes"`
			SharedKey    string `mapstructure:"shared_key"`
			Namespace    string `mapstructure:"namespace"`
			WriteConcern string `mapstructure:"write_concern"`
			TombstoneTTL int    `mapstructure:"tombstone_ttl_ms"`
			Retry        struct {
				IntervalMs  int `mapstructure:"interval_ms"`
				MaxAttempts int `mapstructure:"max_attempts"`
				QueueSize   int `mapstructure:"queue_size"`
			} `mapstructure:"retry"`
			Repair struct {
				IntervalMs      int `mapstructure:"interval_ms"`
				MaxKeysPerCycle int `mapstructure:"max_keys_per_cycle"`
			} `mapstructure:"repair"`
			Churn struct {
				GracePeriodMs int `mapstructure:"grace_period_ms"`
			} `mapstructure:"churn"`
			Seeds struct {
				RefreshIntervalMs int `mapstructure:"refresh_interval_ms"`
			} `mapstructure:"seeds"`
			Metrics struct {
				BindAddr string `mapstructure:"bind_addr"`
				BindPort int    `mapstructure:"bind_port"`
			} `mapstructure:"metrics"`
			Diagnostics struct {
				FailFast         bool `mapstructure:"fail_fast"`
				SelfCheck        bool `mapstructure:"self_check"`
				SelfCheckTimeout int  `mapstructure:"self_check_timeout_ms"`
				RequireSharedKey bool `mapstructure:"require_shared_key"`
				AllowInsecure    bool `mapstructure:"allow_insecure"`
				PeerWarnInterval int  `mapstructure:"peer_warn_interval_ms"`
				MinReadyPeers    int  `mapstructure:"min_ready_peers"`
			} `mapstructure:"diagnostics"`
		} `mapstructure:"cache"`
	} `mapstructure:"common"`
}

func internalBinds(v *viper.Viper) error {
	if err := v.BindEnv("common.cache.cluster.memberlist.node_name", "CACHE_CLUSTER_MEMBERLIST_NODE_NAME"); err != nil {
		return err
	}
	if err := v.BindEnv("common.cache.cluster.memberlist.bind_address", "CACHE_CLUSTER_MEMBERLIST_BIND_ADDRESS"); err != nil {
		return err
	}
	if err := v.BindEnv("common.cache.cluster.memberlist.bind_address_filter", "CACHE_CLUSTER_MEMBERLIST_BIND_ADDRESS_FILTER"); err != nil {
		return err
	}
	if err := v.BindEnv("common.cache.cluster.memberlist.bind_interface_priority", "CACHE_CLUSTER_MEMBERLIST_BIND_INTERFACE_PRIORITY"); err != nil {
		return err
	}
	if err := v.BindEnv("common.cache.cluster.memberlist.bind_port", "CACHE_CLUSTER_MEMBERLIST_BIND_PORT"); err != nil {
		return err
	}
	if err := v.BindEnv("common.cache.cluster.memberlist.advertise_address", "CACHE_CLUSTER_MEMBERLIST_ADVERTISE_ADDRESS", "CACHE_GOSSIP_ADVERTISE_ADDR", "CACHE_ADVERTISE_ADDR"); err != nil {
		return err
	}
	if err := v.BindEnv("common.cache.cluster.memberlist.advertise_port", "CACHE_CLUSTER_MEMBERLIST_ADVERTISE_PORT", "CACHE_GOSSIP_ADVERTISE_PORT"); err != nil {
		return err
	}
	if err := v.BindEnv("common.cache.cluster.memberlist.seed_nodes", "CACHE_CLUSTER_MEMBERLIST_SEED_NODES"); err != nil {
		return err
	}
	if err := v.BindEnv("common.cache.cluster.memberlist.seed_dns_name", "CACHE_CLUSTER_MEMBERLIST_SEED_DNS_NAME"); err != nil {
		return err
	}
	if err := v.BindEnv("common.cache.cluster.memberlist.seed_dns_port", "CACHE_CLUSTER_MEMBERLIST_SEED_DNS_PORT"); err != nil {
		return err
	}
	if err := v.BindEnv("common.cache.cluster.memberlist.partition_count", "CACHE_CLUSTER_MEMBERLIST_PARTITION_COUNT", "CACHE_PARTITION_COUNT"); err != nil {
		return err
	}
	if err := v.BindEnv("common.cache.cluster.memberlist.replication_factor", "CACHE_CLUSTER_MEMBERLIST_REPLICATION_FACTOR", "CACHE_REPLICATION_FACTOR"); err != nil {
		return err
	}
	if err := v.BindEnv("common.cache.api.bind_addr", "CACHE_API_BIND_ADDR"); err != nil {
		return err
	}
	if err := v.BindEnv("common.cache.api.bind_port", "CACHE_API_BIND_PORT"); err != nil {
		return err
	}
	if err := v.BindEnv("common.cache.api.advertise_addr", "CACHE_API_ADVERTISE_ADDR", "CACHE_CONTROL_ADVERTISE_ADDR"); err != nil {
		return err
	}
	if err := v.BindEnv("common.cache.log.level", "CACHE_LOG_LEVEL"); err != nil {
		return err
	}
	if err := v.BindEnv("common.cache.size_bytes", "CACHE_SIZE_BYTES"); err != nil {
		return err
	}
	if err := v.BindEnv("common.cache.shared_key", "CACHE_SHARED_KEY"); err != nil {
		return err
	}
	if err := v.BindEnv("common.cache.namespace", "CACHE_NAMESPACE"); err != nil {
		return err
	}
	if err := v.BindEnv("common.cache.write_concern", "CACHE_WRITE_CONCERN"); err != nil {
		return err
	}
	if err := v.BindEnv("common.cache.tombstone_ttl_ms", "CACHE_TOMBSTONE_TTL_MS"); err != nil {
		return err
	}
	if err := v.BindEnv("common.cache.retry.interval_ms", "CACHE_RETRY_INTERVAL_MS"); err != nil {
		return err
	}
	if err := v.BindEnv("common.cache.retry.max_attempts", "CACHE_RETRY_MAX_ATTEMPTS"); err != nil {
		return err
	}
	if err := v.BindEnv("common.cache.retry.queue_size", "CACHE_RETRY_QUEUE_SIZE"); err != nil {
		return err
	}
	if err := v.BindEnv("common.cache.repair.interval_ms", "CACHE_REPAIR_INTERVAL_MS"); err != nil {
		return err
	}
	if err := v.BindEnv("common.cache.repair.max_keys_per_cycle", "CACHE_REPAIR_MAX_KEYS_PER_CYCLE"); err != nil {
		return err
	}
	if err := v.BindEnv("common.cache.churn.grace_period_ms", "CACHE_CHURN_GRACE_PERIOD_MS"); err != nil {
		return err
	}
	if err := v.BindEnv("common.cache.seeds.refresh_interval_ms", "CACHE_SEEDS_REFRESH_INTERVAL_MS"); err != nil {
		return err
	}
	if err := v.BindEnv("common.cache.metrics.bind_addr", "CACHE_METRICS_BIND_ADDR"); err != nil {
		return err
	}
	if err := v.BindEnv("common.cache.metrics.bind_port", "CACHE_METRICS_BIND_PORT"); err != nil {
		return err
	}
	if err := v.BindEnv("common.cache.diagnostics.fail_fast", "CACHE_DIAGNOSTICS_FAIL_FAST"); err != nil {
		return err
	}
	if err := v.BindEnv("common.cache.diagnostics.self_check", "CACHE_DIAGNOSTICS_SELF_CHECK"); err != nil {
		return err
	}
	if err := v.BindEnv("common.cache.diagnostics.self_check_timeout_ms", "CACHE_DIAGNOSTICS_SELF_CHECK_TIMEOUT_MS"); err != nil {
		return err
	}
	if err := v.BindEnv("common.cache.diagnostics.require_shared_key", "CACHE_DIAGNOSTICS_REQUIRE_SHARED_KEY"); err != nil {
		return err
	}
	if err := v.BindEnv("common.cache.diagnostics.allow_insecure", "CACHE_DIAGNOSTICS_ALLOW_INSECURE", "CACHE_ALLOW_INSECURE"); err != nil {
		return err
	}
	if err := v.BindEnv("common.cache.diagnostics.peer_warn_interval_ms", "CACHE_DIAGNOSTICS_PEER_WARN_INTERVAL_MS"); err != nil {
		return err
	}
	if err := v.BindEnv("common.cache.diagnostics.min_ready_peers", "CACHE_DIAGNOSTICS_MIN_READY_PEERS"); err != nil {
		return err
	}
	if err := v.BindEnv("common.cache.cluster.tls.enabled", "CACHE_CLUSTER_TLS_ENABLED"); err != nil {
		return err
	}
	if err := v.BindEnv("common.cache.cluster.tls.cert_file", "CACHE_CLUSTER_TLS_CERT_FILE"); err != nil {
		return err
	}
	if err := v.BindEnv("common.cache.cluster.tls.key_file", "CACHE_CLUSTER_TLS_KEY_FILE"); err != nil {
		return err
	}
	if err := v.BindEnv("common.cache.cluster.tls.ca_file", "CACHE_CLUSTER_TLS_CA_FILE"); err != nil {
		return err
	}
	if err := v.BindEnv("common.cache.cluster.tls.server_name", "CACHE_CLUSTER_TLS_SERVER_NAME"); err != nil {
		return err
	}
	if err := v.BindEnv("common.cache.cluster.tls.require_client_cert", "CACHE_CLUSTER_TLS_REQUIRE_CLIENT_CERT"); err != nil {
		return err
	}
	if err := v.BindEnv("common.cache.cluster.tls.client_cert_file", "CACHE_CLUSTER_TLS_CLIENT_CERT_FILE"); err != nil {
		return err
	}
	if err := v.BindEnv("common.cache.cluster.tls.client_key_file", "CACHE_CLUSTER_TLS_CLIENT_KEY_FILE"); err != nil {
		return err
	}

	return nil
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
	if len(envFiles) > 0 {
		if err := godotenv.Overload(envFiles...); err != nil {
			l.With("error", err).Errorf("failed to load .env files: %s", strings.Join(envFiles, ", "))
			panic(err)
		}
	}

	base := viper.New()
	base.SetEnvPrefix("CACHE")
	base.AutomaticEnv()
	dotReplacer := strings.NewReplacer(".", "_")
	base.SetEnvKeyReplacer(dotReplacer)

	// === defaults ===
	base.SetDefault("common.cache.size_bytes", 1<<30)
	base.SetDefault("common.cache.cluster.memberlist.replication_factor", 3)
	base.SetDefault("common.cache.retry.interval_ms", 500)
	base.SetDefault("common.cache.retry.max_attempts", 3)
	base.SetDefault("common.cache.retry.queue_size", 1024)
	base.SetDefault("common.cache.repair.interval_ms", 30000)
	base.SetDefault("common.cache.repair.max_keys_per_cycle", 1000)
	base.SetDefault("common.cache.churn.grace_period_ms", 30000)
	base.SetDefault("common.cache.seeds.refresh_interval_ms", 30000)
	base.SetDefault("common.cache.write_concern", "one")
	base.SetDefault("common.cache.tombstone_ttl_ms", 300000)
	base.SetDefault("common.cache.diagnostics.self_check_timeout_ms", 1000)
	base.SetDefault("common.cache.diagnostics.peer_warn_interval_ms", 10000)
	// Env var bindings for deeply nested config fields
	if err := internalBinds(base); err != nil {
		return nil, err
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
		tmp.SetEnvKeyReplacer(dotReplacer)

		if err := internalBinds(tmp); err != nil {
			return nil, err
		}

		if err := tmp.ReadInConfig(); err != nil {
			return nil, fmt.Errorf("read config %s: %w", path, err)
		}
		if err := base.MergeConfigMap(tmp.AllSettings()); err != nil {
			return nil, fmt.Errorf("merge config %s: %w", path, err)
		}
	}
	defaultBindFilter := base.GetStringSlice("common.cache.cluster.memberlist.bind_address_filter")
	if defaultBindFilter == nil {
		defaultBindFilter = bindFilter
		base.Set("common.cache.cluster.memberlist.bind_address_filter", defaultBindFilter)
	}
	var ifacePrefixPriority []*regexp.Regexp
	defaultIfcPriority := base.GetStringSlice("common.cache.cluster.memberlist.bind_interface_priority")
	if defaultIfcPriority == nil {
		ifacePrefixPriority = make([]*regexp.Regexp, 0)
		for _, ifc := range ifcPriority {
			if !strings.HasPrefix(ifc, "^") {
				ifc = fmt.Sprintf("^%s", ifc)
			}
			if !strings.HasSuffix(ifc, "$") {
				ifc = fmt.Sprintf("%s$", ifc)
			}

			ifacePrefixPriority = append(ifacePrefixPriority, regexp.MustCompile(ifc, regexp.RE2))
		}
		base.Set("common.cache.cluster.memberlist.bind_interface_priority", ifcPriority)
	}

	// decode
	cfg := new(Config)
	hooks := mapstructure.ComposeDecodeHookFunc(
		RecursiveStructToMapHookFunc(newRootTemplate(defaultBindAddress(defaultBindFilter, ifacePrefixPriority))),
		mapstructure.StringToTimeDurationHookFunc(),
		mapstructure.StringToSliceHookFunc(","),
		TextUnmarshallerHookFunc(),
		TemplateUnmarshallerHookFunc(newRootTemplate(defaultBindAddress(defaultBindFilter, ifacePrefixPriority))),
	)

	if err := base.Unmarshal(cfg, func(vc *mapstructure.DecoderConfig) {
		vc.DecodeHook = hooks
	}); err != nil {
		return nil, fmt.Errorf("unmarshal config: %w", err)
	}

	return cfg, nil
}

func newRootTemplate(ipFunc func() (string, error)) *template.Template {
	return template.New("").
		Funcs(sprig.FuncMap()).
		Funcs(template.FuncMap{
			"hostname": func(args ...string) (string, error) {
				if s, err := os.Hostname(); err != nil {
					return "", err
				} else {
					return strings.Split(s, ".")[0], nil
				}
			},
			"ip": func() (string, error) {
				return ipFunc()
			},
		})
}

func TemplateUnmarshallerHookFunc(root *template.Template) mapstructure.DecodeHookFuncType {
	return func(f reflect.Type, t reflect.Type, data any) (any, error) {
		if f.Kind() != reflect.String {
			return data, nil
		}

		sdata := data.(string)
		if strings.Contains(sdata, "{{") && strings.Contains(sdata, "}}") {
			if tmpl, err := root.Parse(sdata); err != nil {
				return data, nil
			} else {
				buf := new(bytes.Buffer)
				if err = tmpl.Execute(buf, struct{}{}); err != nil {
					return data, nil
				} else {
					return buf.String(), nil
				}
			}
		}

		return data, nil
	}
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

func RecursiveStructToMapHookFunc(root *template.Template) mapstructure.DecodeHookFunc {
	var toMap func(val reflect.Value) any

	toMap = func(val reflect.Value) any {
		if !val.IsValid() {
			return nil
		}

		// Preserve *time.Time
		if val.Type().AssignableTo(tyTimePtr) {
			return val.Interface()
		}

		// Handle time.Time value
		if val.Type().AssignableTo(tyTime) {
			v := val.Interface().(time.Time)
			return &v
		}

		// Handle pointer dereferencing carefully
		deref := false
		for val.Kind() == reflect.Ptr {
			if val.IsNil() {
				return nil
			}

			// If it's a pointer to struct, traverse the struct deeply,
			// but don't flatten pointer shape. This fixes nested pointers.
			if val.Elem().Kind() == reflect.Struct {
				return toMap(val.Elem())
			}

			deref = true
			val = val.Elem()
		}

		switch val.Kind() {
		case reflect.Array:
			n := val.Len()
			out := make([]any, n)
			for i := 0; i < n; i++ {
				out[i] = toMap(val.Index(i))
			}
			return out // arrays treated as []any for mapstructure

		case reflect.Slice:
			n := val.Len()
			out := make([]any, n)
			for i := 0; i < n; i++ {
				out[i] = toMap(val.Index(i))
			}
			if deref {
				return &out
			}
			return out

		case reflect.Map:
			out := make(map[string]any)
			for _, key := range val.MapKeys() {
				out[key.String()] = toMap(val.MapIndex(key))
			}
			if deref {
				return &out
			}
			return out

		case reflect.Struct:
			m := make(map[string]any)
			vtype := val.Type()

			for i := 0; i < vtype.NumField(); i++ {
				field := vtype.Field(i)
				if !field.IsExported() {
					continue
				}

				tag := field.Tag.Get("mapstructure")
				tagParts := strings.Split(tag, ",")
				squash := field.Anonymous
				omitEmpty := false
				key := field.Name
				maybeTemplate := false

				for _, part := range tagParts {
					switch part {
					case "omitempty":
						omitEmpty = true
					case "squash":
						squash = true
					case "template":
						maybeTemplate = field.Type.Kind() == reflect.String
					default:
						if part != "" && part != "-" {
							key = part
						}
					}
				}

				fv := val.Field(i)
				if maybeTemplate {
					if idx := strings.Index(fv.String(), "{{"); idx > 0 && strings.Index(fv.String(), "}}") > idx {
						if tmpl, err := root.Parse(fv.String()); err != nil {
							continue
						} else {
							buf := new(bytes.Buffer)
							if err = tmpl.Execute(buf, struct{}{}); err != nil {
								continue
							} else {
								fv.SetString(buf.String())
							}
						}
					}
				}
				processed := toMap(fv)

				if squash {
					// Flatten embedded structs
					rv := reflect.ValueOf(processed)
					if rv.IsValid() && rv.Kind() == reflect.Ptr {
						if !rv.IsNil() {
							rv = rv.Elem()
							processed = rv.Interface()
						} else {
							continue
						}
					}

					if processedMap, ok := processed.(map[string]any); ok {
						for k, v := range processedMap {
							m[k] = v
						}
						continue
					}
				}

				// Handle omitempty
				if omitEmpty && isEmptyValue(reflect.ValueOf(processed)) {
					continue
				}

				m[key] = processed
			}

			if deref {
				return &m
			}
			return m

		default:
			if deref {
				v := val.Interface()
				return &v
			}
			return val.Interface()
		}
	}

	return func(from reflect.Value, to reflect.Value) (any, error) {
		fv := from
		for fv.Kind() == reflect.Ptr {
			if fv.IsNil() {
				return nil, nil
			}
			fv = fv.Elem()
		}

		if fv.Kind() != reflect.Struct {
			return from.Interface(), nil
		}
		return toMap(from), nil
	}
}

// Helper for omitempty
func isEmptyValue(v reflect.Value) bool {
	if !v.IsValid() {
		return true
	}
	switch v.Kind() {
	case reflect.Array, reflect.Map, reflect.Slice, reflect.String:
		return v.Len() == 0
	case reflect.Bool:
		return !v.Bool()
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return v.Int() == 0
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		return v.Uint() == 0
	case reflect.Float32, reflect.Float64:
		return v.Float() == 0
	case reflect.Interface, reflect.Ptr:
		return v.IsNil()
	case reflect.Struct:
		// special-case time.Time
		if v.Type() == tyTime {
			return v.Interface().(time.Time).IsZero()
		}
	default:
	}
	return false
}

func hasAnyPrefix(prefixes []*regexp.Regexp, ss ...string) bool {
	for _, str := range ss {
		matched := false
		for _, p := range prefixes {
			if ok, err := p.MatchString(str); err == nil && ok {
				matched = true
				break
			}
		}
		if !matched {
			return false
		}
	}

	return true
}

// defaultBindAddress returns the first non-loopback, non-docker-bridge IPv4 address.
func defaultBindAddress(validCIDRs []string, ifacePrefixPriority []*regexp.Regexp) func() (string, error) {
	return func() (string, error) {
		bindAddressOnce.Do(func() {
			l := log.Default()
			if len(validCIDRs) == 0 {
				validCIDRs = bindFilter
			}

			ipnets := make([]*net.IPNet, 0)
			for _, validCIDR := range validCIDRs {
				_, ipnet, _ := net.ParseCIDR(validCIDR)
				ipnets = append(ipnets, ipnet)
			}

			ifaces, err := net.Interfaces()
			slices.SortFunc(ifaces, func(a, b net.Interface) int {
				if hasAnyPrefix(ifacePrefixPriority, a.Name, b.Name) {
					return strings.Compare(a.Name, b.Name)
				}
				if hasAnyPrefix(ifacePrefixPriority, a.Name) {
					return -1
				}
				if hasAnyPrefix(ifacePrefixPriority, b.Name) {
					return 1
				}
				return strings.Compare(a.Name, b.Name)
			})
			l.Tracef("Checking first interface %s ", ifaces[0].Name)
			if err != nil {
				bindAddress, bindAddressErr = "", err
				return
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
						if ipNet.Contains(ip) {
							l.Tracef("IP %s is in %s on %s", ip, ipNet, iface.Name)
						}
						return ipNet.Contains(ip)
					}) {
						bindAddress, bindAddressErr = ip.String(), nil
						return
					}
				}
			}
			bindAddress, bindAddressErr = "", errors.New("no suitable LAN IPv4 address found")
			return
		})

		return bindAddress, bindAddressErr
	}
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
