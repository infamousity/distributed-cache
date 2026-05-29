package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	dcache "github.com/infamousity/distributed-cache/cache"
)

func main() {
	nodeName := getenv("CACHE_NODE_NAME", "app-1")
	controlAddr := getenv("CACHE_CONTROL_BIND_ADDR", "0.0.0.0")
	controlPort := getenvInt("CACHE_CONTROL_BIND_PORT", 9090)
	gossipAddr := getenv("CACHE_GOSSIP_BIND_ADDR", "0.0.0.0")
	gossipPort := getenvInt("CACHE_GOSSIP_BIND_PORT", 8946)
	seedNodes := parseCSV(getenv("CACHE_SEED_NODES", ""))
	sharedKey := getenv("CACHE_SHARED_KEY", "dev-shared-key")

	cache, err := dcache.Start(dcache.Options{
		NodeName:          nodeName,
		ControlBindAddr:   controlAddr,
		ControlBindPort:   controlPort,
		GossipBindAddr:    gossipAddr,
		GossipBindPort:    gossipPort,
		SeedNodes:         seedNodes,
		SharedKey:         sharedKey,
		ReplicationFactor: 3,
		CacheSizeBytes:    64 << 20,
	})
	if err != nil {
		log.Fatalf("start cache: %v", err)
	}
	defer cache.Close()

	ctx := context.Background()
	key := "swarm-key"
	if nodeName == "app-1" || nodeName == "app" {
		_ = cache.Set(ctx, key, []byte("from-"+nodeName), 30*time.Second)
	}

	for {
		value, found, err := cache.Get(ctx, key)
		if err != nil {
			log.Printf("get error: %v", err)
		} else {
			fmt.Printf("node=%s found=%v value=%s\n", nodeName, found, value)
		}
		time.Sleep(5 * time.Second)
	}
}

func getenv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func getenvInt(key string, def int) int {
	v := getenv(key, "")
	if v == "" {
		return def
	}
	var out int
	if _, err := fmt.Sscanf(v, "%d", &out); err != nil {
		return def
	}
	return out
}

func parseCSV(v string) []string {
	if v == "" {
		return nil
	}
	parts := strings.Split(v, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}
