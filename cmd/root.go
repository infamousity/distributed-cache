package cmd

import (
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/spf13/cobra"

	"github.com/infamousity/distributed-cache/cache"
	"github.com/infamousity/distributed-cache/config"
	"github.com/infamousity/distributed-cache/internal/log"
)

var (
	defaultConfigFiles = []string{"config.yml"}
	configFiles        []string
	logLevelOverride   string
	rootCmd            = &cobra.Command{
		Use:   "distributed-cache",
		Short: "Start the distributed cache node",
		RunE: func(cmd *cobra.Command, args []string) error {
			paths := resolveConfigFiles(cmd, configFiles)
			cfg, err := config.Load(paths...)
			if err != nil {
				return err
			}

			ll, err := resolveLogLevel(cmd, logLevelOverride, cfg.Common.Cache.Log.Level)
			if err != nil {
				return err
			}
			log.SetDefault(log.WithLevel(ll))
			l := log.Default()

			dc, err := cache.StartFromConfig(cfg)
			if err != nil {
				l.Errorf("Failed to start cache: %v", err)
				return err
			}
			l.Infof("Local node name: %s", dc.NodeName())

			sigCh := make(chan os.Signal, 1)
			signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
			<-sigCh
			return dc.Close()
		},
	}
)

func init() {
	rootCmd.PersistentFlags().StringVarP(&logLevelOverride, "level", "l", "", "log level override (trace, debug, info, warn, error)")
	rootCmd.PersistentFlags().StringArrayVarP(&configFiles, "config", "c", nil, "Config file paths to load (right overrides left)")
}

func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func resolveConfigFiles(cmd *cobra.Command, values []string) []string {
	if flag := cmd.Flag("config"); flag != nil && flag.Changed {
		return values
	}
	if os.Getenv("CACHE_CONFIG") != "" {
		return nil
	}
	return defaultConfigFiles
}

func resolveLogLevel(cmd *cobra.Command, override, configured string) (slog.Level, error) {
	level := strings.TrimSpace(configured)
	if flag := cmd.Flag("level"); flag != nil && flag.Changed {
		level = strings.TrimSpace(override)
	}
	if level == "" {
		level = "info"
	}

	var out slog.Level
	if err := out.UnmarshalText([]byte(level)); err != nil {
		if strings.EqualFold(level, "trace") {
			return log.LevelTrace, nil
		}
		return out, fmt.Errorf("invalid log level %q", level)
	}
	return out, nil
}
