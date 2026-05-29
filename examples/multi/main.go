package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"strconv"
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
		writer      = flag.Bool("writer", false, "write the example value from this node")
		startupWait = flag.Duration("startup-wait", 2*time.Second, "time to wait for gossip membership before writing")
		readEvery   = flag.Duration("read-every", 5*time.Second, "read interval")
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
		TombstoneTTL:      30 * time.Second,
	})
	if err != nil {
		log.Fatalf("start cache: %v", err)
	}
	defer cache.Close()

	ctx := context.Background()
	key := "multi-key"

	time.Sleep(*startupWait)
	if *writer {
		value := []byte("from-" + *nodeName + "-at-" + strconv.FormatInt(time.Now().Unix(), 10))
		if err := cache.Set(ctx, key, value, time.Minute); err != nil {
			log.Fatalf("set: %v", err)
		}
		fmt.Printf("%s wrote key=%s value=%s\n", *nodeName, key, value)
	}

	for {
		value, found, err := cache.Get(ctx, key)
		if err != nil {
			log.Printf("%s get error: %v", *nodeName, err)
		} else {
			fmt.Printf("%s read found=%v value=%s\n", *nodeName, found, value)
		}
		time.Sleep(*readEvery)
	}
}
