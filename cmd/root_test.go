package cmd

import (
	"log/slog"
	"reflect"
	"testing"

	"github.com/spf13/cobra"

	"github.com/infamousity/distributed-cache/internal/log"
)

func TestResolveConfigFiles(t *testing.T) {
	t.Run("default", func(t *testing.T) {
		cmd, values := testRootCommand(t)
		if got := resolveConfigFiles(cmd, values); !reflect.DeepEqual(got, defaultConfigFiles) {
			t.Fatalf("config files = %v, want %v", got, defaultConfigFiles)
		}
	})

	t.Run("cache config env", func(t *testing.T) {
		t.Setenv("CACHE_CONFIG", "env-a.yml,env-b.yml")
		cmd, values := testRootCommand(t)
		if got := resolveConfigFiles(cmd, values); got != nil {
			t.Fatalf("config files = %v, want nil to delegate CACHE_CONFIG to config.Load", got)
		}
	})

	t.Run("explicit flag", func(t *testing.T) {
		t.Setenv("CACHE_CONFIG", "env-a.yml,env-b.yml")
		cmd, values := testRootCommand(t, "-c", "flag-a.yml", "-c", "flag-b.yml")
		want := []string{"flag-a.yml", "flag-b.yml"}
		if got := resolveConfigFiles(cmd, values); !reflect.DeepEqual(got, want) {
			t.Fatalf("config files = %v, want %v", got, want)
		}
	})
}

func TestResolveLogLevel(t *testing.T) {
	t.Run("configured", func(t *testing.T) {
		cmd, _ := testRootCommand(t)
		got, err := resolveLogLevel(cmd, "", "debug")
		if err != nil {
			t.Fatalf("resolve log level: %v", err)
		}
		if got != slog.LevelDebug {
			t.Fatalf("level = %v, want %v", got, slog.LevelDebug)
		}
	})

	t.Run("flag override", func(t *testing.T) {
		cmd, _ := testRootCommand(t, "--level", "warn")
		got, err := resolveLogLevel(cmd, "warn", "debug")
		if err != nil {
			t.Fatalf("resolve log level: %v", err)
		}
		if got != slog.LevelWarn {
			t.Fatalf("level = %v, want %v", got, slog.LevelWarn)
		}
	})

	t.Run("default", func(t *testing.T) {
		cmd, _ := testRootCommand(t)
		got, err := resolveLogLevel(cmd, "", "")
		if err != nil {
			t.Fatalf("resolve log level: %v", err)
		}
		if got != slog.LevelInfo {
			t.Fatalf("level = %v, want %v", got, slog.LevelInfo)
		}
	})

	t.Run("trace", func(t *testing.T) {
		cmd, _ := testRootCommand(t)
		got, err := resolveLogLevel(cmd, "", "trace")
		if err != nil {
			t.Fatalf("resolve log level: %v", err)
		}
		if got != log.LevelTrace {
			t.Fatalf("level = %v, want %v", got, log.LevelTrace)
		}
	})

	t.Run("invalid", func(t *testing.T) {
		cmd, _ := testRootCommand(t)
		if _, err := resolveLogLevel(cmd, "", "verbose"); err == nil {
			t.Fatalf("expected invalid level to fail")
		}
	})
}

func testRootCommand(t *testing.T, args ...string) (*cobra.Command, []string) {
	t.Helper()
	var configs []string
	var level string
	cmd := &cobra.Command{Use: "test"}
	cmd.Flags().StringArrayVarP(&configs, "config", "c", nil, "")
	cmd.Flags().StringVarP(&level, "level", "l", "", "")
	cmd.SetArgs(args)
	if err := cmd.ParseFlags(args); err != nil {
		t.Fatalf("parse flags: %v", err)
	}
	return cmd, configs
}
