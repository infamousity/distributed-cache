package cmd

import (
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"strings"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	"github.com/infamousity/distributed-cache/internal/api"
	"github.com/infamousity/distributed-cache/internal/cluster"
	"github.com/infamousity/distributed-cache/internal/config"
	"github.com/infamousity/distributed-cache/internal/log"
)

var (
	defaultConfigFiles = []string{"config.yml"}
	rootCmd            = &cobra.Command{
		Use:   "distributed-cache",
		Short: "Start the distributed cache node",
		RunE: func(cmd *cobra.Command, args []string) error {
			logLevel := viper.GetString("level")
			var ll slog.Level

			if err := ll.UnmarshalText([]byte(logLevel)); err != nil {
				if strings.EqualFold(logLevel, "trace") {
					ll = log.LevelTrace
				} else {
					panic("invalid log level: " + logLevel)
				}
			}
			log.SetDefault(log.WithLevel(ll))
			l := log.Default()

			configFiles := viper.GetStringSlice("config_files")
			cfg, err := config.Load(configFiles...)
			if err != nil {
				l.Errorf("Failed to load config: %v", err)
				return err
			}

			cl, err := cluster.NewCluster(cfg)
			if err != nil {
				l.Errorf("Failed to initialize cluster: %v", err)
				return err
			}

			l.Infof("Local node name: %s, known members: [%s]", cl.GetNode().GetSelf(), strings.Join(cl.GetNode().List(), ","))

			if err := cl.Rebalance(); err != nil {
				if !errors.Is(err, cluster.ErrOneNodeInCluster) {
					l.Errorf("Failed to rebalance cache: %v", err)
					return nil
				}
			} else {
				l.Infof("Rebalanced cache")
			}

			server := api.New(net.JoinHostPort(cfg.Common.Cache.Http.BindAddr, fmt.Sprintf("%d", cfg.Common.Cache.Http.BindPort)), cl)
			return server.Run()
		},
	}
)

func init() {
	// CLI flag bindings
	rootCmd.PersistentFlags().StringP("level", "l", "trace", "log level (trace, debug, info, warn, error)")
	rootCmd.PersistentFlags().StringArrayP("config", "c", defaultConfigFiles, "Config file paths to load (right overrides left)")
	_ = viper.BindPFlag("config_files", rootCmd.PersistentFlags().Lookup("config"))
	_ = viper.BindPFlag("level", rootCmd.PersistentFlags().Lookup("level"))

	// Sensible defaults
	viper.SetDefault("level", "trace")
	viper.SetDefault("config_files", defaultConfigFiles)
}

func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
