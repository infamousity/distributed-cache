package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"strings"
	"time"

	dcache "github.com/infamousity/distributed-cache/cache"
)

func main() {
	var (
		nodeName    = flag.String("name", "node-1", "node name")
		controlAddr = flag.String("control-addr", "127.0.0.1", "control-plane bind address")
		controlPort = flag.Int("control-port", 9090, "control-plane bind port")
		gossipAddr  = flag.String("gossip-addr", "127.0.0.1", "gossip bind address")
		gossipPort  = flag.Int("gossip-port", 8946, "gossip bind port")
		seedNodes   = flag.String("seeds", "", "comma-delimited seed nodes host:port")
	)
	flag.Parse()

	seeds := []string{}
	if *seedNodes != "" {
		seeds = strings.Split(*seedNodes, ",")
	}

	cache, err := dcache.Start(dcache.Options{
		NodeName:          *nodeName,
		ControlBindAddr:   *controlAddr,
		ControlBindPort:   *controlPort,
		GossipBindAddr:    *gossipAddr,
		GossipBindPort:    *gossipPort,
		SeedNodes:         seeds,
		SharedKey:         "dev-shared-key",
		ReplicationFactor: 2,
		CacheSizeBytes:    64 << 20,
	})
	if err != nil {
		log.Fatalf("start cache: %v", err)
	}
	defer cache.Close()

	ctx := context.Background()
	key := "multi-key"

	if *nodeName == "node-1" {
		if err := cache.Set(ctx, key, []byte("from-node-1"), 10*time.Second); err != nil {
			log.Fatalf("set: %v", err)
		}
		fmt.Println("node-1 wrote value")
	}

	time.Sleep(1 * time.Second)
	value, found, err := cache.Get(ctx, key)
	if err != nil {
		log.Fatalf("get: %v", err)
	}
	fmt.Printf("%s read found=%v value=%s\n", *nodeName, found, value)

	select {}
}
