package main

import (
	"context"
	"fmt"
	"log"
	"time"

	dcache "github.com/infamousity/distributed-cache/cache"
)

func main() {
	cache, err := dcache.Start(dcache.Options{
		NodeName:          "single-node",
		ControlBindAddr:   "127.0.0.1",
		ControlBindPort:   9090,
		GossipBindAddr:    "127.0.0.1",
		GossipBindPort:    8946,
		SeedNodes:         []string{},
		SharedKey:         "dev-shared-key",
		ReplicationFactor: 2,
		CacheSizeBytes:    64 << 20,
	})
	if err != nil {
		log.Fatalf("start cache: %v", err)
	}
	defer cache.Close()

	ctx := context.Background()
	if err := cache.Set(ctx, "hello", []byte("world"), 5*time.Second); err != nil {
		log.Fatalf("set: %v", err)
	}

	value, found, err := cache.Get(ctx, "hello")
	if err != nil {
		log.Fatalf("get: %v", err)
	}
	fmt.Printf("found=%v value=%s\n", found, value)
}
