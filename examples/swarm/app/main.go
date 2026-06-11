package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	dcache "github.com/infamousity/distributed-cache/cache"
)

func main() {
	nodeName := getenv("CACHE_NODE_NAME", "app-1")
	harnessHTTPAddr := getenv("CACHE_HARNESS_HTTP_BIND_ADDR", getenv("CACHE_HTTP_BIND_ADDR", ":8080"))
	controlAddr := getenv("CACHE_CONTROL_BIND_ADDR", "0.0.0.0")
	controlPort := getenvInt("CACHE_CONTROL_BIND_PORT", 9090)
	gossipAddr := getenv("CACHE_GOSSIP_BIND_ADDR", "0.0.0.0")
	gossipPort := getenvInt("CACHE_GOSSIP_BIND_PORT", 8946)
	peerNodes := parseCSV(getenv("CACHE_PEER_NODES", ""))
	peerDNSName := getenv("CACHE_CLUSTER_MEMBERLIST_PEER_DNS_NAME", getenv("CACHE_PEER_DNS_NAME", defaultPeerDNSName()))
	peerDNSPort := getenvInt("CACHE_CLUSTER_MEMBERLIST_PEER_DNS_PORT", getenvInt("CACHE_PEER_DNS_PORT", gossipPort))
	advertiseAddr := getenv("CACHE_ADVERTISE_ADDR", defaultAdvertiseAddr(peerDNSName))
	controlAdvertiseAddr := getenv("CACHE_CONTROL_ADVERTISE_ADDR", net.JoinHostPort(advertiseAddr, strconv.Itoa(controlPort)))
	sharedKey := getenv("CACHE_SHARED_KEY", "dev-shared-key")
	startupWait := time.Duration(getenvInt("CACHE_STARTUP_WAIT_MS", 5000)) * time.Millisecond
	tombstoneTTL := time.Duration(getenvInt("CACHE_TOMBSTONE_TTL_MS", 300000)) * time.Millisecond
	valueTTL := time.Duration(getenvInt("CACHE_VALUE_TTL_MS", 600000)) * time.Millisecond
	minReadyPeers := getenvInt("CACHE_DIAGNOSTICS_MIN_READY_PEERS", 0)

	dc, err := dcache.Start(dcache.Options{
		NodeName:             nodeName,
		ControlBindAddr:      controlAddr,
		ControlBindPort:      controlPort,
		ControlAdvertiseAddr: controlAdvertiseAddr,
		GossipBindAddr:       gossipAddr,
		GossipBindPort:       gossipPort,
		AdvertiseAddr:        advertiseAddr,
		AdvertisePort:        gossipPort,
		PeerNodes:            peerNodes,
		PeerDNSName:          peerDNSName,
		PeerDNSPort:          peerDNSPort,
		SharedKey:            sharedKey,
		ReplicationFactor:    3,
		CacheSizeBytes:       64 << 20,
		TombstoneTTL:         tombstoneTTL,
		MinReadyPeers:        minReadyPeers,
	})
	if err != nil {
		log.Fatalf("start cache: %v", err)
	}
	defer dc.Close()
	log.Printf("cache advertise gossip=%s:%d control=%s peer_dns=%s", advertiseAddr, gossipPort, controlAdvertiseAddr, peerDNSName)

	go serveHarnessHTTP(harnessHTTPAddr, nodeName, dc, valueTTL)

	ctx := context.Background()
	key := "swarm-key"
	time.Sleep(startupWait)
	wrote := false

	for {
		if isFirstReplica(nodeName) && !wrote {
			if _, found, err := dc.Get(ctx, key); err != nil {
				log.Printf("initial get error: %v", err)
			} else if found {
				wrote = true
			} else {
				value := []byte("from-" + nodeName)
				if err := dc.Set(ctx, key, value, valueTTL); err != nil {
					log.Printf("set error: %v", err)
				} else {
					wrote = true
					fmt.Printf("node=%s wrote key=%s value=%s\n", nodeName, key, value)
				}
			}
		}
		value, found, err := dc.Get(ctx, key)
		if err != nil {
			log.Printf("get error: %v", err)
		} else {
			fmt.Printf("node=%s found=%v value=%s\n", nodeName, found, value)
		}
		time.Sleep(5 * time.Second)
	}
}

// serveHarnessHTTP is example-only. Production services that expose cache-backed
// routes should do so through their own service API and security model.
func serveHarnessHTTP(addr, nodeName string, dc *dcache.DistributedCache, valueTTL time.Duration) {
	mux := http.NewServeMux()
	mux.HandleFunc("/mark", func(w http.ResponseWriter, r *http.Request) {
		phase := r.URL.Query().Get("phase")
		if phase == "" {
			http.Error(w, "phase is required", http.StatusBadRequest)
			return
		}
		value := []byte("phase-" + phase + "-from-" + nodeName)
		if err := dc.Set(r.Context(), "swarm-key", value, valueTTL); err != nil {
			writeCacheError(w, err)
			return
		}
		fmt.Printf("node=%s chaos_phase=%s\n", nodeName, phase)
		fmt.Printf("node=%s wrote key=swarm-key value=%s\n", nodeName, value)
		writeJSON(w, map[string]any{"ok": true, "node": nodeName, "phase": phase})
	})
	mux.HandleFunc("/set", func(w http.ResponseWriter, r *http.Request) {
		key := r.URL.Query().Get("key")
		value := r.URL.Query().Get("value")
		if key == "" {
			http.Error(w, "key is required", http.StatusBadRequest)
			return
		}
		ttl := time.Duration(getQueryInt(r, "ttl_ms", 600000)) * time.Millisecond
		if err := dc.Set(r.Context(), key, []byte(value), ttl); err != nil {
			writeCacheError(w, err)
			return
		}
		writeJSON(w, map[string]any{"ok": true, "node": nodeName})
	})
	mux.HandleFunc("/del", func(w http.ResponseWriter, r *http.Request) {
		key := r.URL.Query().Get("key")
		if key == "" {
			http.Error(w, "key is required", http.StatusBadRequest)
			return
		}
		if err := dc.Del(r.Context(), key); err != nil {
			writeCacheError(w, err)
			return
		}
		writeJSON(w, map[string]any{"ok": true, "node": nodeName})
	})
	mux.HandleFunc("/get", func(w http.ResponseWriter, r *http.Request) {
		key := r.URL.Query().Get("key")
		if key == "" {
			http.Error(w, "key is required", http.StatusBadRequest)
			return
		}
		value, found, err := dc.Get(r.Context(), key)
		if err != nil {
			writeCacheError(w, err)
			return
		}
		writeJSON(w, map[string]any{
			"found": found,
			"key":   key,
			"node":  nodeName,
			"value": string(value),
		})
	})
	mux.HandleFunc("/status", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, dc.Status())
	})
	log.Printf("example harness HTTP listening on %s", addr)
	if err := http.ListenAndServe(addr, mux); err != nil {
		log.Printf("harness http server error: %v", err)
	}
}

func writeCacheError(w http.ResponseWriter, err error) {
	status := http.StatusBadGateway
	if errors.Is(err, dcache.ErrNotReady) || errors.Is(err, dcache.ErrWriteIndeterminate) {
		status = http.StatusServiceUnavailable
	}
	http.Error(w, err.Error(), status)
}

func getQueryInt(r *http.Request, key string, def int) int {
	v := r.URL.Query().Get(key)
	if v == "" {
		return def
	}
	out, err := strconv.Atoi(v)
	if err != nil {
		return def
	}
	return out
}

func writeJSON(w http.ResponseWriter, value any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(value)
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

func defaultPeerDNSName() string {
	if !strings.EqualFold(getenv("CACHE_RUNTIME", ""), "swarm") {
		return ""
	}
	serviceName := getenv("CACHE_SWARM_SERVICE_NAME", getenv("CACHE_SERVICE_NAME", getenv("SERVICE_NAME", "")))
	if serviceName == "" {
		return ""
	}
	if strings.HasPrefix(serviceName, "tasks.") {
		return serviceName
	}
	return "tasks." + serviceName
}

func defaultAdvertiseAddr(peerDNSName string) string {
	localIPs := localInterfaceIPs()
	if peerDNSName != "" {
		if ip := advertiseAddrForPeerNetwork(peerDNSName, localIPs); ip != "" {
			return ip
		}
	}
	for _, local := range localIPs {
		if ip := local.IP.To4(); ip != nil {
			return ip.String()
		}
	}
	return ""
}

func localInterfaceIPs() []*net.IPNet {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return nil
	}
	out := make([]*net.IPNet, 0, len(addrs))
	for _, addr := range addrs {
		ipNet, ok := addr.(*net.IPNet)
		if !ok || ipNet.IP.IsLoopback() {
			continue
		}
		ip := ipNet.IP.To4()
		if ip != nil {
			out = append(out, &net.IPNet{IP: ip, Mask: ipNet.Mask})
		}
	}
	return out
}

func advertiseAddrForPeerNetwork(peerDNSName string, localIPs []*net.IPNet) string {
	peerIPs, err := net.LookupIP(peerDNSName)
	if err != nil {
		log.Printf("resolve peer DNS for advertise addr failed: %v", err)
		return ""
	}
	for _, peerIP := range peerIPs {
		peerIPv4 := peerIP.To4()
		if peerIPv4 == nil {
			continue
		}
		for _, local := range localIPs {
			if local.Contains(peerIPv4) {
				return local.IP.String()
			}
		}
	}
	return ""
}
