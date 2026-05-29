package cmd

import (
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	"github.com/infamousity/distributed-cache/cache"
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
			dc, err := cache.StartFromConfigFiles(configFiles...)
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
