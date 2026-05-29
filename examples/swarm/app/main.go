package main

import (
	"context"
	"fmt"
	"log"
	"net"
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
	advertiseAddr := getenv("CACHE_ADVERTISE_ADDR", defaultAdvertiseAddr())
	seedNodes := parseCSV(getenv("CACHE_SEED_NODES", ""))
	sharedKey := getenv("CACHE_SHARED_KEY", "dev-shared-key")
	startupWait := time.Duration(getenvInt("CACHE_STARTUP_WAIT_MS", 5000)) * time.Millisecond
	tombstoneTTL := time.Duration(getenvInt("CACHE_TOMBSTONE_TTL_MS", 300000)) * time.Millisecond

	cache, err := dcache.Start(dcache.Options{
		NodeName:          nodeName,
		ControlBindAddr:   controlAddr,
		ControlBindPort:   controlPort,
		GossipBindAddr:    gossipAddr,
		GossipBindPort:    gossipPort,
		AdvertiseAddr:     advertiseAddr,
		AdvertisePort:     gossipPort,
		SeedNodes:         seedNodes,
		SharedKey:         sharedKey,
		ReplicationFactor: 3,
		CacheSizeBytes:    64 << 20,
		TombstoneTTL:      tombstoneTTL,
	})
	if err != nil {
		log.Fatalf("start cache: %v", err)
	}
	defer cache.Close()

	ctx := context.Background()
	key := "swarm-key"
	time.Sleep(startupWait)
	if isFirstReplica(nodeName) {
		value := []byte("from-" + nodeName)
		if err := cache.Set(ctx, key, value, time.Minute); err != nil {
			log.Printf("set error: %v", err)
		} else {
			fmt.Printf("node=%s wrote key=%s value=%s\n", nodeName, key, value)
		}
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

func isFirstReplica(nodeName string) bool {
	return nodeName == "app-1" ||
		nodeName == "app" ||
		strings.Contains(nodeName, "_app.1.") ||
		strings.Contains(nodeName, ".app.1.") ||
		strings.Contains(nodeName, ".1.")
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

func defaultAdvertiseAddr() string {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return ""
	}
	for _, addr := range addrs {
		ipNet, ok := addr.(*net.IPNet)
		if !ok || ipNet.IP.IsLoopback() {
			continue
		}
		ip := ipNet.IP.To4()
		if ip != nil {
			return ip.String()
		}
	}
	return ""
}
