package cluster

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

const (
	hashHelperOutputEnv = "DISTRIBUTED_CACHE_HASH_HELPER_OUTPUT"
	hashHelperKey       = "namespace:key"
)

func TestHasherPreservesClusterMapping(t *testing.T) {
	// These values are the mappings produced by the former Ristretto fork.
	// Changing them would make mixed-version cluster members disagree.
	tests := []struct {
		key  string
		want uint64
	}{
		{key: "", want: 0},
		{key: "key", want: 11599170318058208956},
		{key: "namespace:key", want: 17584872329183713422},
		{key: "ключ", want: 17753313917685924547},
		{key: "a\x00b", want: 3310025685034513883},
	}
	for _, test := range tests {
		if got := (hasher{}).Sum64([]byte(test.key)); got != test.want {
			t.Errorf("Sum64(%q) = %d, want %d", test.key, got, test.want)
		}
	}
}

func TestHasherIsStableAcrossProcesses(t *testing.T) {
	want := (hasher{}).Sum64([]byte(hashHelperKey))
	for i := range 2 {
		outputPath := filepath.Join(t.TempDir(), fmt.Sprintf("hash-%d", i))
		cmd := exec.Command(os.Args[0], "-test.run=^TestHasherHelperProcess$")
		cmd.Env = append(os.Environ(), hashHelperOutputEnv+"="+outputPath)
		if output, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("hash helper process %d: %v\n%s", i, err, output)
		}
		raw, err := os.ReadFile(outputPath)
		if err != nil {
			t.Fatalf("read hash helper output %d: %v", i, err)
		}
		got, err := strconv.ParseUint(strings.TrimSpace(string(raw)), 10, 64)
		if err != nil {
			t.Fatalf("parse hash helper output %d: %v", i, err)
		}
		if got != want {
			t.Fatalf("hash helper process %d = %d, want %d", i, got, want)
		}
	}
}

func TestHasherHelperProcess(t *testing.T) {
	outputPath := os.Getenv(hashHelperOutputEnv)
	if outputPath == "" {
		return
	}
	hash := (hasher{}).Sum64([]byte(hashHelperKey))
	if err := os.WriteFile(outputPath, []byte(strconv.FormatUint(hash, 10)), 0o600); err != nil {
		t.Fatalf("write hash helper output: %v", err)
	}
}
