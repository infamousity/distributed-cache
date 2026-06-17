package cache

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"fmt"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	internalcache "github.com/infamousity/distributed-cache/internal/cache"
	"github.com/infamousity/distributed-cache/internal/config"
	"github.com/infamousity/distributed-cache/internal/control"
	internallog "github.com/infamousity/distributed-cache/internal/log"
	"github.com/infamousity/distributed-cache/internal/version"
)

func testVersion(physical int64, nodeID string) version.Version {
	return version.Version{Physical: physical, NodeID: nodeID}
}

func getFreePort(t *testing.T) int {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}

func waitForValue(t *testing.T, c *DistributedCache, key string, want []byte, timeout time.Duration) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		val, found, err := c.Get(context.Background(), key)
		if err == nil && found && string(val) == string(want) {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	val, found, err := c.Get(context.Background(), key)
	t.Fatalf("timeout waiting for value: found=%v err=%v val=%q", found, err, string(val))
}

func waitForStoreValue(t *testing.T, c *DistributedCache, key string, want []byte, timeout time.Duration) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		val, found := c.store.Get(key)
		if found && string(val) == string(want) {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	val, found := c.store.Get(key)
	t.Fatalf("timeout waiting for store value: found=%v val=%q", found, string(val))
}

func waitForStoreMiss(t *testing.T, c *DistributedCache, key string, timeout time.Duration) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		_, found := c.store.Get(key)
		if !found {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	_, found := c.store.Get(key)
	t.Fatalf("timeout waiting for store miss: found=%v", found)
}

func waitForStoreEntry(t *testing.T, c *DistributedCache, key string, timeout time.Duration) internalcache.Entry {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		entry, found := c.store.GetEntry(key)
		if found {
			return entry
		}
		time.Sleep(50 * time.Millisecond)
	}
	entry, found := c.store.GetEntry(key)
	t.Fatalf("timeout waiting for store entry: found=%v entry=%+v", found, entry)
	return internalcache.Entry{}
}

func TestValidateConfigRejectsNegativeTombstoneTTL(t *testing.T) {
	err := validateConfig(&config.Config{}, Options{TombstoneTTL: -time.Second})
	if err == nil {
		t.Fatalf("expected negative tombstone ttl to be rejected")
	}
	if c, err := Start(Options{TombstoneTTL: -time.Second}); err == nil {
		_ = c.Close()
		t.Fatalf("expected Start to reject negative tombstone ttl")
	}
}

func TestValidateConfigRejectsNegativeOperationalOptions(t *testing.T) {
	cases := []struct {
		name string
		opts Options
	}{
		{name: "control timeout", opts: Options{ControlTimeout: -time.Second}},
		{name: "retry interval", opts: Options{ReplicationRetryInterval: -time.Second}},
		{name: "retry queue size", opts: Options{ReplicationRetryQueueSize: -1}},
		{name: "self check timeout", opts: Options{SelfCheckTimeout: -time.Second}},
		{name: "peer warn interval", opts: Options{PeerWarnInterval: -time.Second}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if err := validateConfig(&config.Config{}, tc.opts); err == nil {
				t.Fatalf("expected validation error")
			}
		})
	}
}

func TestValidateConfigRejectsInvalidControlAdvertiseAddr(t *testing.T) {
	cfg := &config.Config{}
	cfg.Common.Cache.Control.AdvertiseAddr = "127.0.0.1"
	if err := validateConfig(cfg, Options{}); err == nil {
		t.Fatalf("expected control advertise addr without port to be rejected")
	}

	cfg.Common.Cache.Control.AdvertiseAddr = "0.0.0.0:9090"
	if err := validateConfig(cfg, Options{}); err == nil {
		t.Fatalf("expected wildcard control advertise addr to be rejected")
	}
}

func TestValidateConfigRejectsInvalidMemberlistAdvertiseAddr(t *testing.T) {
	cfg := &config.Config{}
	cfg.Common.Cache.Cluster.MemberList.AdvertiseAddr = "node1"
	if err := validateConfig(cfg, Options{}); err == nil {
		t.Fatalf("expected DNS memberlist advertise addr to be rejected")
	}

	cfg.Common.Cache.Cluster.MemberList.AdvertiseAddr = "0.0.0.0"
	if err := validateConfig(cfg, Options{}); err == nil {
		t.Fatalf("expected wildcard memberlist advertise addr to be rejected")
	}
}

func TestNormalizeMemberlistAdvertiseAddr(t *testing.T) {
	cases := []struct {
		name     string
		addr     string
		port     int
		wantAddr string
		wantPort int
		wantErr  bool
	}{
		{
			name:     "ipv4 endpoint",
			addr:     "127.0.0.1:8946",
			wantAddr: "127.0.0.1",
			wantPort: 8946,
		},
		{
			name:     "ipv6 endpoint",
			addr:     "[2001:db8::1]:8946",
			wantAddr: "2001:db8::1",
			wantPort: 8946,
		},
		{
			name:     "ipv6 address",
			addr:     "2001:db8::1",
			wantAddr: "2001:db8::1",
		},
		{
			name:     "dns address",
			addr:     "node1",
			wantAddr: "node1",
		},
		{
			name:    "conflicting port",
			addr:    "127.0.0.1:8946",
			port:    18946,
			wantErr: true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := &config.Config{}
			cfg.Common.Cache.Cluster.MemberList.AdvertiseAddr = tc.addr
			cfg.Common.Cache.Cluster.MemberList.AdvertisePort = tc.port

			err := normalizeMemberlistAdvertise(cfg)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("expected error")
				}
				return
			}
			if err != nil {
				t.Fatalf("normalize: %v", err)
			}
			got := cfg.Common.Cache.Cluster.MemberList
			if got.AdvertiseAddr != tc.wantAddr || got.AdvertisePort != tc.wantPort {
				t.Fatalf("memberlist advertise = (%q, %d), want (%q, %d)", got.AdvertiseAddr, got.AdvertisePort, tc.wantAddr, tc.wantPort)
			}
		})
	}
}

func TestControlAdvertiseAddrSetsSelfForwardAddress(t *testing.T) {
	controlPort := getFreePort(t)
	controlAddr := fmt.Sprintf("127.0.0.1:%d", controlPort)
	c, err := Start(Options{
		NodeName:             "node-advertise",
		ControlBindAddr:      "127.0.0.1",
		ControlBindPort:      controlPort,
		ControlAdvertiseAddr: controlAddr,
		GossipBindAddr:       "127.0.0.1",
		GossipBindPort:       getFreePort(t),
		SharedKey:            "test-key",
		ReplicationFactor:    2,
	})
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	defer c.Close()

	got, ok := c.cluster.GetNode().GetForwardAddr(c.cluster.GetNode().GetSelf())
	if !ok {
		t.Fatalf("self forward address missing")
	}
	if got != controlAddr {
		t.Fatalf("self forward address = %q, want %q", got, controlAddr)
	}
}

func TestStartAcceptsMemberlistAdvertiseEndpoint(t *testing.T) {
	gossipPort := getFreePort(t)
	c, err := Start(Options{
		NodeName:          "node-memberlist-advertise-endpoint",
		ControlBindAddr:   "127.0.0.1",
		ControlBindPort:   getFreePort(t),
		GossipBindAddr:    "127.0.0.1",
		GossipBindPort:    gossipPort,
		AdvertiseAddr:     fmt.Sprintf("127.0.0.1:%d", gossipPort),
		SharedKey:         "test-key",
		ReplicationFactor: 2,
	})
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	defer c.Close()

	if c.opts.AdvertiseAddr != "127.0.0.1" || c.opts.AdvertisePort != gossipPort {
		t.Fatalf("memberlist advertise = (%q, %d), want (%q, %d)", c.opts.AdvertiseAddr, c.opts.AdvertisePort, "127.0.0.1", gossipPort)
	}
}

func TestReplicaSelectionPrefersTombstoneOnEqualVersion(t *testing.T) {
	best := control.Entry{Value: []byte("value"), Version: testVersion(10, "a")}
	tombstone := control.Entry{Version: testVersion(10, "a"), Tombstone: true}
	if !betterReplicaEntry(tombstone, best, true) {
		t.Fatalf("expected equal-version tombstone to beat value")
	}
	if betterReplicaEntry(best, tombstone, true) {
		t.Fatalf("expected equal-version value not to beat tombstone")
	}
	if !betterReplicaEntry(control.Entry{Value: []byte("newer"), Version: testVersion(11, "a")}, tombstone, true) {
		t.Fatalf("expected newer value to beat older tombstone")
	}
}

func TestVerifyPeerEvictsClientOnIdentityMismatch(t *testing.T) {
	controlPort := getFreePort(t)
	controlAddr := fmt.Sprintf("127.0.0.1:%d", controlPort)
	c, err := Start(Options{
		NodeName:             "node-actual",
		ControlBindAddr:      "127.0.0.1",
		ControlBindPort:      controlPort,
		ControlAdvertiseAddr: controlAddr,
		GossipBindAddr:       "127.0.0.1",
		GossipBindPort:       getFreePort(t),
		SharedKey:            "test-key",
		ReplicationFactor:    2,
	})
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	defer c.Close()

	client, err := c.clientFor(controlAddr)
	if err != nil {
		t.Fatalf("clientFor: %v", err)
	}
	if client == nil {
		t.Fatalf("clientFor returned nil client")
	}

	c.verifyPeer("node-expected", controlAddr)
	c.clientMu.Lock()
	_, ok := c.clients[controlAddr]
	c.clientMu.Unlock()
	if ok {
		t.Fatalf("expected identity mismatch to evict cached client")
	}
}

func TestReadyRequiresMinimumVerifiedPeers(t *testing.T) {
	c, err := Start(Options{
		NodeName:          "node-ready",
		ControlBindAddr:   "127.0.0.1",
		ControlBindPort:   getFreePort(t),
		GossipBindAddr:    "127.0.0.1",
		GossipBindPort:    getFreePort(t),
		SharedKey:         "test-key",
		ReplicationFactor: 2,
		MinReadyPeers:     1,
	})
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	defer c.Close()

	if err := c.Ready(context.Background()); err == nil {
		t.Fatalf("expected readiness to fail without verified peers")
	} else if !errors.Is(err, ErrNotReady) {
		t.Fatalf("expected ErrNotReady, got %v", err)
	}
	waitCtx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	go func() {
		time.Sleep(100 * time.Millisecond)
		c.setPeerState("peer-1", "127.0.0.1:1", PeerStateVerified, "")
	}()
	if err := c.WaitReady(waitCtx); err != nil {
		t.Fatalf("expected WaitReady after verified peer: %v", err)
	}
	c.setPeerState("peer-1", "127.0.0.1:1", PeerStateVerified, "")
	if err := c.Ready(context.Background()); err != nil {
		t.Fatalf("expected readiness after verified peer: %v", err)
	}
	status := c.Status()
	if !status.Ready || status.VerifiedPeers != 1 {
		t.Fatalf("unexpected status: %+v", status)
	}
}

func TestSetPeerStateReportsVerifiedTransitionOnce(t *testing.T) {
	c, err := Start(Options{
		NodeName:          "node-transition",
		ControlBindAddr:   "127.0.0.1",
		ControlBindPort:   getFreePort(t),
		GossipBindAddr:    "127.0.0.1",
		GossipBindPort:    getFreePort(t),
		SharedKey:         "test-key",
		ReplicationFactor: 2,
	})
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	defer c.Close()

	if c.setPeerState("peer-1", "127.0.0.1:1", PeerStateJoined, "") {
		t.Fatalf("joined state should not report verified transition")
	}
	if !c.setPeerState("peer-1", "127.0.0.1:1", PeerStateVerified, "") {
		t.Fatalf("first verified state should report transition")
	}
	if c.setPeerState("peer-1", "127.0.0.1:1", PeerStateVerified, "") {
		t.Fatalf("repeated verified state should not report transition")
	}
}

func TestScheduleRepairUsesRepairIntervalAsDebounce(t *testing.T) {
	c := &DistributedCache{
		opts:      Options{RepairInterval: time.Hour},
		repairNow: make(chan struct{}, 1),
	}

	c.scheduleRepair()
	select {
	case <-c.repairNow:
	default:
		t.Fatalf("expected first repair request to be scheduled")
	}

	c.scheduleRepair()
	select {
	case <-c.repairNow:
		t.Fatalf("expected repair request inside interval to be suppressed")
	default:
	}

	c.repairMu.Lock()
	c.repairLast = time.Now().Add(-2 * time.Hour)
	c.repairMu.Unlock()
	c.scheduleRepair()
	select {
	case <-c.repairNow:
	default:
		t.Fatalf("expected repair request after interval to be scheduled")
	}
}

func TestScheduleRepairDebounceReservesConcurrentRequests(t *testing.T) {
	c := &DistributedCache{
		opts:      Options{RepairInterval: time.Hour},
		repairNow: make(chan struct{}, 100),
	}
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			c.scheduleRepair()
		}()
	}
	wg.Wait()

	if got := len(c.repairNow); got != 1 {
		t.Fatalf("scheduled repair requests = %d, want 1", got)
	}
}

func TestStartGeneratesSharedKeyUnlessInsecureAllowed(t *testing.T) {
	opts := Options{
		NodeName:          "node-generated-shared-key",
		ControlBindAddr:   "127.0.0.1",
		ControlBindPort:   getFreePort(t),
		GossipBindAddr:    "127.0.0.1",
		GossipBindPort:    getFreePort(t),
		ReplicationFactor: 1,
	}
	c, err := Start(opts)
	if err != nil {
		t.Fatalf("expected generated shared key startup: %v", err)
	}
	if c.opts.SharedKey == "" {
		t.Fatalf("expected generated shared key")
	}
	if c.opts.SharedKey == "dev-shared-key" || c.opts.SharedKey == "test-key" {
		t.Fatalf("generated shared key used a static example value")
	}
	if err := c.Close(); err != nil {
		t.Fatalf("close generated shared key cache: %v", err)
	}

	opts.AllowInsecure = true
	opts.NodeName = "node-explicit-insecure"
	opts.ControlBindPort = getFreePort(t)
	opts.GossipBindPort = getFreePort(t)
	c, err = Start(opts)
	if err != nil {
		t.Fatalf("expected explicit insecure mode to start: %v", err)
	}
	if c.opts.SharedKey != "" {
		t.Fatalf("explicit insecure mode generated shared key %q, want empty", c.opts.SharedKey)
	}
	defer c.Close()
}

func TestStartRequiresConfiguredSharedKeyWhenPolicyEnabled(t *testing.T) {
	opts := Options{
		NodeName:          "node-require-shared-key",
		ControlBindAddr:   "127.0.0.1",
		ControlBindPort:   getFreePort(t),
		GossipBindAddr:    "127.0.0.1",
		GossipBindPort:    getFreePort(t),
		ReplicationFactor: 1,
		RequireSharedKey:  true,
	}
	if c, err := Start(opts); err == nil {
		_ = c.Close()
		t.Fatalf("expected Start with require_shared_key and no shared key to fail")
	} else {
		msg := err.Error()
		for _, want := range []string{"common.cache.shared_key", "config.secrets.yml", "CACHE_SHARED_KEY"} {
			if !strings.Contains(msg, want) {
				t.Fatalf("missing shared key error %q did not mention %q", msg, want)
			}
		}
	}
}

func TestStartCleansUpClusterWhenControlListenFails(t *testing.T) {
	gossipPort := getFreePort(t)
	controlPort := getFreePort(t)
	l, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", controlPort))
	if err != nil {
		t.Fatalf("listen occupied control port: %v", err)
	}
	defer l.Close()

	_, err = Start(Options{
		NodeName:        "cleanup-failed-start",
		ControlBindAddr: "127.0.0.1",
		ControlBindPort: controlPort,
		GossipBindAddr:  "127.0.0.1",
		GossipBindPort:  gossipPort,
		SharedKey:       "test-key",
	})
	if err == nil {
		t.Fatalf("expected occupied control port to fail start")
	}

	c, err := Start(Options{
		NodeName:        "cleanup-reuse-gossip",
		ControlBindAddr: "127.0.0.1",
		ControlBindPort: getFreePort(t),
		GossipBindAddr:  "127.0.0.1",
		GossipBindPort:  gossipPort,
		SharedKey:       "test-key",
	})
	if err != nil {
		t.Fatalf("start after failed start reused gossip port: %v", err)
	}
	defer c.Close()
}

func TestMetricsStartFailureIsReturned(t *testing.T) {
	metricsPort := getFreePort(t)
	ln, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", metricsPort))
	if err != nil {
		t.Fatalf("listen metrics port: %v", err)
	}
	defer ln.Close()

	c, err := Start(Options{
		NodeName:          "node-metrics-conflict",
		ControlBindAddr:   "127.0.0.1",
		ControlBindPort:   getFreePort(t),
		GossipBindAddr:    "127.0.0.1",
		GossipBindPort:    getFreePort(t),
		SharedKey:         "test-key",
		ReplicationFactor: 1,
		MetricsBindAddr:   "127.0.0.1",
		MetricsBindPort:   metricsPort,
	})
	if err == nil {
		_ = c.Close()
		t.Fatalf("expected metrics bind conflict to fail Start")
	}
}

func TestResolveDNSPeersHonorsContext(t *testing.T) {
	c := &DistributedCache{opts: Options{
		PeerDNSName:    "localhost",
		PeerDNSPort:    8946,
		GossipBindPort: 8946,
	}}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if _, err := c.resolveDNSPeers(ctx); err == nil {
		t.Fatalf("expected canceled DNS context to return an error")
	}
}

func TestResolveDNSPeers(t *testing.T) {
	c, err := Start(Options{
		NodeName:            "node-dns-peers",
		ControlBindAddr:     "127.0.0.1",
		ControlBindPort:     getFreePort(t),
		GossipBindAddr:      "127.0.0.1",
		GossipBindPort:      getFreePort(t),
		SharedKey:           "test-key",
		ReplicationFactor:   2,
		PeerDNSName:         "localhost",
		PeerDNSPort:         8946,
		PeerRefreshInterval: time.Hour,
	})
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	defer c.Close()

	peers, err := c.resolveDNSPeers(context.Background())
	if err != nil {
		t.Fatalf("resolveDNSPeers: %v", err)
	}
	if len(peers) == 0 {
		t.Fatalf("expected at least one localhost peer")
	}
	for _, peer := range peers {
		_, port, err := net.SplitHostPort(peer)
		if err != nil {
			t.Fatalf("peer %q is not host:port: %v", peer, err)
		}
		if port != "8946" {
			t.Fatalf("peer %q used port %s, want 8946", peer, port)
		}
	}
}

func TestResolveDNSPeersUsesSingularAndPluralNames(t *testing.T) {
	c, err := Start(Options{
		NodeName:            "node-dns-peer-names",
		ControlBindAddr:     "127.0.0.1",
		ControlBindPort:     getFreePort(t),
		GossipBindAddr:      "127.0.0.1",
		GossipBindPort:      getFreePort(t),
		SharedKey:           "test-key",
		ReplicationFactor:   2,
		PeerDNSName:         "localhost",
		PeerDNSNames:        []string{"localhost", " localhost "},
		PeerDNSPort:         8946,
		PeerRefreshInterval: time.Hour,
	})
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	defer c.Close()

	names := c.peerDNSNames()
	if len(names) != 1 || names[0] != "localhost" {
		t.Fatalf("peer DNS names = %#v, want only localhost", names)
	}

	peers, err := c.resolveDNSPeers(context.Background())
	if err != nil {
		t.Fatalf("resolveDNSPeers: %v", err)
	}
	if len(peers) == 0 {
		t.Fatalf("expected at least one localhost peer")
	}
}

func TestOwnershipCleanupChurnGrace(t *testing.T) {
	c, err := Start(Options{
		NodeName:          "node-churn",
		ControlBindAddr:   "127.0.0.1",
		ControlBindPort:   getFreePort(t),
		GossipBindAddr:    "127.0.0.1",
		GossipBindPort:    getFreePort(t),
		SharedKey:         "test-key",
		ReplicationFactor: 2,
		ChurnGracePeriod:  100 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	defer c.Close()

	now := time.Now()
	if !c.shouldDelayOwnershipCleanup("k", now) {
		t.Fatalf("expected first ownership loss to be delayed")
	}
	if !c.shouldDelayOwnershipCleanup("k", now.Add(50*time.Millisecond)) {
		t.Fatalf("expected ownership loss inside grace period to be delayed")
	}
	if c.shouldDelayOwnershipCleanup("k", now.Add(150*time.Millisecond)) {
		t.Fatalf("expected ownership loss after grace period not to be delayed")
	}
	c.clearOwnershipLost("k")
	if !c.shouldDelayOwnershipCleanup("k", now.Add(200*time.Millisecond)) {
		t.Fatalf("expected ownership recovery to reset grace tracking")
	}
}

func waitForMembers(t *testing.T, c *DistributedCache, want int, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if len(c.cluster.GetNode().List()) >= want {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("timeout waiting for %d members, got %d", want, len(c.cluster.GetNode().List()))
}

func keyOwnedByOtherNode(t *testing.T, c *DistributedCache) string {
	t.Helper()
	self := c.cluster.GetNode().GetSelf()
	for i := 0; i < 10000; i++ {
		key := fmt.Sprintf("forwarded-key-%d", i)
		owner, ok := c.cluster.GetNode().Get(key)
		if ok && owner != self {
			return key
		}
	}
	t.Fatalf("failed to find key owned by another node")
	return ""
}

func TestForwardedSetReturnsNotReadyForUnverifiedOwner(t *testing.T) {
	c, err := Start(Options{
		NodeName:          "set-forwarder",
		ControlBindAddr:   "127.0.0.1",
		ControlBindPort:   getFreePort(t),
		GossipBindAddr:    "127.0.0.1",
		GossipBindPort:    getFreePort(t),
		SharedKey:         "test-key",
		ReplicationFactor: 2,
	})
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	defer c.Close()

	c.cluster.GetNode().Add("stale-owner", "127.0.0.1:1")
	c.setPeerState("stale-owner", "127.0.0.1:1", PeerStateLeft, "")
	key := keyOwnedByOtherNode(t, c)

	err = c.Set(context.Background(), key, []byte("value"), time.Minute)
	if !errors.Is(err, ErrNotReady) {
		t.Fatalf("Set error = %v, want ErrNotReady", err)
	}
}

func TestForwardedDelReturnsNotReadyForUnverifiedOwner(t *testing.T) {
	c, err := Start(Options{
		NodeName:          "del-forwarder",
		ControlBindAddr:   "127.0.0.1",
		ControlBindPort:   getFreePort(t),
		GossipBindAddr:    "127.0.0.1",
		GossipBindPort:    getFreePort(t),
		SharedKey:         "test-key",
		ReplicationFactor: 2,
	})
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	defer c.Close()

	c.cluster.GetNode().Add("stale-owner", "127.0.0.1:1")
	c.setPeerState("stale-owner", "127.0.0.1:1", PeerStateLeft, "")
	key := keyOwnedByOtherNode(t, c)

	err = c.Del(context.Background(), key)
	if !errors.Is(err, ErrNotReady) {
		t.Fatalf("Del error = %v, want ErrNotReady", err)
	}
}

func poisonVersionCounter(c *DistributedCache) version.Version {
	future := testVersion(time.Now().Add(24*time.Hour).UnixMilli(), "future")
	c.versionMu.Lock()
	c.lastVersion = future
	c.versionMu.Unlock()
	return future
}

func resetVersionCounter(c *DistributedCache) {
	c.versionMu.Lock()
	c.lastVersion = version.Zero()
	c.versionMu.Unlock()
}

func TestNextVersionIncludesNodeIDTieBreaker(t *testing.T) {
	physical := time.Now().Add(time.Hour).UnixMilli()
	c1 := &DistributedCache{opts: Options{NodeName: "node-a"}}
	c2 := &DistributedCache{opts: Options{NodeName: "node-b"}}
	c1.lastVersion = testVersion(physical, "observed")
	c2.lastVersion = testVersion(physical, "observed")

	v1 := c1.nextVersion()
	v2 := c2.nextVersion()
	if v1.Compare(v2) == 0 {
		t.Fatalf("versions should differ by node tie-breaker: %s", v1)
	}
	if v1.NodeID != "node-a" {
		t.Fatalf("v1 node id = %q, want node-a", v1.NodeID)
	}
	if v2.NodeID != "node-b" {
		t.Fatalf("v2 node id = %q, want node-b", v2.NodeID)
	}
}

func TestNextVersionAdvancesFromObservedRemoteVersion(t *testing.T) {
	physical := time.Now().Add(time.Hour).UnixMilli()
	remote := version.Version{Physical: physical, Logical: 10, NodeID: "remote"}
	c := &DistributedCache{opts: Options{NodeName: "local"}}
	c.observeVersion(remote)

	next := c.nextVersion()
	if next.Compare(remote) <= 0 {
		t.Fatalf("next version %s did not advance beyond observed remote %s", next, remote)
	}
	if next.Logical != 11 {
		t.Fatalf("logical = %d, want 11", next.Logical)
	}
	if next.NodeID != "local" {
		t.Fatalf("node id = %q, want local", next.NodeID)
	}
}

func waitForControlAddr(t *testing.T, addr, sharedKey string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		client, err := control.Dial(addr, control.ClientOptions{SharedKey: sharedKey})
		if err == nil {
			_, err = client.Ping(context.Background())
			_ = client.Close()
			if err == nil {
				return
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("timeout waiting for control plane at %s", addr)
}

type pemPair struct {
	certPath string
	keyPath  string
}

func writePEMFile(t *testing.T, dir, name string, pemBytes []byte, mode os.FileMode) string {
	t.Helper()
	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, pemBytes, mode); err != nil {
		t.Fatalf("write %s: %v", name, err)
	}
	return path
}

var caCounter uint64

func generateCA(t *testing.T, dir string) pemPair {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("generate ca key: %v", err)
	}

	serial, _ := rand.Int(rand.Reader, big.NewInt(1<<62))
	template := x509.Certificate{
		SerialNumber: serial,
		Subject: pkix.Name{
			CommonName: "test-ca",
		},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(24 * time.Hour),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageCRLSign,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}

	der, err := x509.CreateCertificate(rand.Reader, &template, &template, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("create ca cert: %v", err)
	}

	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)})
	suffix := atomic.AddUint64(&caCounter, 1)
	return pemPair{
		certPath: writePEMFile(t, dir, fmt.Sprintf("ca-%d.crt", suffix), certPEM, 0644),
		keyPath:  writePEMFile(t, dir, fmt.Sprintf("ca-%d.key", suffix), keyPEM, 0600),
	}
}

func generateCert(t *testing.T, dir, name, commonName string, ca pemPair, isClient bool) pemPair {
	t.Helper()
	caCertBytes, err := os.ReadFile(ca.certPath)
	if err != nil {
		t.Fatalf("read ca cert: %v", err)
	}
	caKeyBytes, err := os.ReadFile(ca.keyPath)
	if err != nil {
		t.Fatalf("read ca key: %v", err)
	}
	caBlock, _ := pem.Decode(caCertBytes)
	if caBlock == nil {
		t.Fatalf("decode ca cert")
	}
	caKeyBlock, _ := pem.Decode(caKeyBytes)
	if caKeyBlock == nil {
		t.Fatalf("decode ca key")
	}
	caCert, err := x509.ParseCertificate(caBlock.Bytes)
	if err != nil {
		t.Fatalf("parse ca cert: %v", err)
	}
	caKey, err := x509.ParsePKCS1PrivateKey(caKeyBlock.Bytes)
	if err != nil {
		t.Fatalf("parse ca key: %v", err)
	}

	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	serial, _ := rand.Int(rand.Reader, big.NewInt(1<<62))
	template := x509.Certificate{
		SerialNumber: serial,
		Subject: pkix.Name{
			CommonName: commonName,
		},
		NotBefore:   time.Now().Add(-time.Hour),
		NotAfter:    time.Now().Add(24 * time.Hour),
		KeyUsage:    x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	if isClient {
		template.ExtKeyUsage = []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}
	}
	template.DNSNames = []string{commonName, "localhost"}
	template.IPAddresses = []net.IP{net.ParseIP("127.0.0.1")}

	der, err := x509.CreateCertificate(rand.Reader, &template, caCert, &key.PublicKey, caKey)
	if err != nil {
		t.Fatalf("create cert: %v", err)
	}

	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)})
	return pemPair{
		certPath: writePEMFile(t, dir, fmt.Sprintf("%s.crt", name), certPEM, 0644),
		keyPath:  writePEMFile(t, dir, fmt.Sprintf("%s.key", name), keyPEM, 0600),
	}
}

func mustCAPool(t *testing.T, caPath string) *x509.CertPool {
	t.Helper()
	caBytes, err := os.ReadFile(caPath)
	if err != nil {
		t.Fatalf("read ca file: %v", err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(caBytes) {
		t.Fatalf("append ca certs")
	}
	return pool
}

func mustClientCert(t *testing.T, certPath, keyPath string) tls.Certificate {
	t.Helper()
	cert, err := tls.LoadX509KeyPair(certPath, keyPath)
	if err != nil {
		t.Fatalf("load client cert: %v", err)
	}
	return cert
}

func TestDistributedCacheReplicationAndForwarding(t *testing.T) {
	gossip1 := getFreePort(t)
	gossip2 := getFreePort(t)
	control1 := getFreePort(t)
	control2 := getFreePort(t)

	peer := fmt.Sprintf("127.0.0.1:%d", gossip1)

	c1, err := Start(Options{
		NodeName:          "node-1",
		ControlBindAddr:   "127.0.0.1",
		ControlBindPort:   control1,
		GossipBindAddr:    "127.0.0.1",
		GossipBindPort:    gossip1,
		PeerNodes:         []string{},
		SharedKey:         "test-key",
		ReplicationFactor: 2,
	})
	if err != nil {
		t.Fatalf("start node-1: %v", err)
	}
	defer c1.Close()

	c2, err := Start(Options{
		NodeName:          "node-2",
		ControlBindAddr:   "127.0.0.1",
		ControlBindPort:   control2,
		GossipBindAddr:    "127.0.0.1",
		GossipBindPort:    gossip2,
		PeerNodes:         []string{peer},
		SharedKey:         "test-key",
		ReplicationFactor: 2,
	})
	if err != nil {
		t.Fatalf("start node-2: %v", err)
	}
	defer c2.Close()

	waitForMembers(t, c1, 2, 2*time.Second)
	waitForMembers(t, c2, 2, 2*time.Second)
	waitForControlAddr(t, fmt.Sprintf("127.0.0.1:%d", control1), "test-key", 2*time.Second)
	waitForControlAddr(t, fmt.Sprintf("127.0.0.1:%d", control2), "test-key", 2*time.Second)

	key := "alpha"
	value := []byte("value-1")
	if err := c1.Set(context.Background(), key, value, 2*time.Second); err != nil {
		t.Fatalf("set: %v", err)
	}

	waitForValue(t, c2, key, value, 2*time.Second)
}

func TestDistributedCacheRepairsExistingKeysWhenPeerJoins(t *testing.T) {
	gossip1 := getFreePort(t)
	gossip2 := getFreePort(t)
	control1 := getFreePort(t)
	control2 := getFreePort(t)

	c1, err := Start(Options{
		NodeName:                 "late-owner",
		ControlBindAddr:          "127.0.0.1",
		ControlBindPort:          control1,
		GossipBindAddr:           "127.0.0.1",
		GossipBindPort:           gossip1,
		SharedKey:                "test-key",
		ReplicationFactor:        2,
		RepairInterval:           time.Hour,
		ReplicationRetryInterval: 50 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("start late-owner: %v", err)
	}
	defer c1.Close()

	key := "late-key"
	value := []byte("late-value")
	if err := c1.Set(context.Background(), key, value, time.Minute); err != nil {
		t.Fatalf("set before peer joins: %v", err)
	}
	waitForStoreValue(t, c1, key, value, time.Second)

	c2, err := Start(Options{
		NodeName:                 "late-peer",
		ControlBindAddr:          "127.0.0.1",
		ControlBindPort:          control2,
		GossipBindAddr:           "127.0.0.1",
		GossipBindPort:           gossip2,
		PeerNodes:                []string{fmt.Sprintf("127.0.0.1:%d", gossip1)},
		SharedKey:                "test-key",
		ReplicationFactor:        2,
		RepairInterval:           time.Hour,
		ReplicationRetryInterval: 50 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("start late-peer: %v", err)
	}
	defer c2.Close()

	waitForMembers(t, c1, 2, 2*time.Second)
	waitForMembers(t, c2, 2, 2*time.Second)
	waitForStoreValue(t, c2, key, value, 3*time.Second)
}

func TestGetUsesLocalReplicaWhenOwnerControlPlaneFails(t *testing.T) {
	gossip1 := getFreePort(t)
	gossip2 := getFreePort(t)
	control1 := getFreePort(t)
	control2 := getFreePort(t)

	c1, err := Start(Options{
		NodeName:          "owner-node",
		ControlBindAddr:   "127.0.0.1",
		ControlBindPort:   control1,
		GossipBindAddr:    "127.0.0.1",
		GossipBindPort:    gossip1,
		SharedKey:         "test-key",
		ReplicationFactor: 2,
	})
	if err != nil {
		t.Fatalf("start owner: %v", err)
	}
	defer c1.Close()

	c2, err := Start(Options{
		NodeName:          "replica-node",
		ControlBindAddr:   "127.0.0.1",
		ControlBindPort:   control2,
		GossipBindAddr:    "127.0.0.1",
		GossipBindPort:    gossip2,
		PeerNodes:         []string{fmt.Sprintf("127.0.0.1:%d", gossip1)},
		SharedKey:         "test-key",
		ReplicationFactor: 2,
	})
	if err != nil {
		t.Fatalf("start replica: %v", err)
	}
	defer c2.Close()

	waitForMembers(t, c1, 2, 2*time.Second)
	waitForMembers(t, c2, 2, 2*time.Second)
	key := keyOwnedByOtherNode(t, c2)
	value := []byte("replica-survives-owner-control-failure")
	if err := c1.Set(context.Background(), key, value, time.Minute); err != nil {
		t.Fatalf("set via owner: %v", err)
	}
	waitForStoreValue(t, c2, key, value, 2*time.Second)

	c1.control.Stop()
	got, found, err := c2.Get(context.Background(), key)
	if err != nil {
		t.Fatalf("get should fall back to local replica: %v", err)
	}
	if !found || string(got) != string(value) {
		t.Fatalf("local replica get = found %v value %q, want %q", found, got, value)
	}
}

func TestMajoritySetFailureIsIndeterminateAndLocallyVisible(t *testing.T) {
	gossip1 := getFreePort(t)
	gossip2 := getFreePort(t)
	control1 := getFreePort(t)
	control2 := getFreePort(t)

	c1, err := Start(Options{
		NodeName:          "majority-set-owner",
		ControlBindAddr:   "127.0.0.1",
		ControlBindPort:   control1,
		GossipBindAddr:    "127.0.0.1",
		GossipBindPort:    gossip1,
		SharedKey:         "test-key",
		ReplicationFactor: 2,
		WriteConcern:      WriteConcernMajority,
	})
	if err != nil {
		t.Fatalf("start owner: %v", err)
	}
	defer c1.Close()

	c2, err := Start(Options{
		NodeName:          "majority-set-replica",
		ControlBindAddr:   "127.0.0.1",
		ControlBindPort:   control2,
		GossipBindAddr:    "127.0.0.1",
		GossipBindPort:    gossip2,
		PeerNodes:         []string{fmt.Sprintf("127.0.0.1:%d", gossip1)},
		SharedKey:         "test-key",
		ReplicationFactor: 2,
		WriteConcern:      WriteConcernMajority,
	})
	if err != nil {
		t.Fatalf("start replica: %v", err)
	}
	defer c2.Close()

	waitForMembers(t, c1, 2, 2*time.Second)
	waitForMembers(t, c2, 2, 2*time.Second)
	key := keyOwnedByOtherNode(t, c2)
	c2.control.Stop()

	value := []byte("visible-after-indeterminate-set")
	err = c1.Set(context.Background(), key, value, time.Minute)
	if !errors.Is(err, ErrWriteIndeterminate) {
		t.Fatalf("set error = %v, want ErrWriteIndeterminate", err)
	}
	got, found := c1.store.Get(key)
	if !found || string(got) != string(value) {
		t.Fatalf("owner local store = found %v value %q, want %q", found, got, value)
	}
}

func TestMajorityDelFailureIsIndeterminateAndLocallyVisible(t *testing.T) {
	gossip1 := getFreePort(t)
	gossip2 := getFreePort(t)
	control1 := getFreePort(t)
	control2 := getFreePort(t)

	c1, err := Start(Options{
		NodeName:          "majority-del-owner",
		ControlBindAddr:   "127.0.0.1",
		ControlBindPort:   control1,
		GossipBindAddr:    "127.0.0.1",
		GossipBindPort:    gossip1,
		SharedKey:         "test-key",
		ReplicationFactor: 2,
		WriteConcern:      WriteConcernMajority,
	})
	if err != nil {
		t.Fatalf("start owner: %v", err)
	}
	defer c1.Close()

	c2, err := Start(Options{
		NodeName:          "majority-del-replica",
		ControlBindAddr:   "127.0.0.1",
		ControlBindPort:   control2,
		GossipBindAddr:    "127.0.0.1",
		GossipBindPort:    gossip2,
		PeerNodes:         []string{fmt.Sprintf("127.0.0.1:%d", gossip1)},
		SharedKey:         "test-key",
		ReplicationFactor: 2,
		WriteConcern:      WriteConcernMajority,
	})
	if err != nil {
		t.Fatalf("start replica: %v", err)
	}
	defer c2.Close()

	waitForMembers(t, c1, 2, 2*time.Second)
	waitForMembers(t, c2, 2, 2*time.Second)
	key := keyOwnedByOtherNode(t, c2)
	value := []byte("delete-after-replication")
	if err := c1.Set(context.Background(), key, value, time.Minute); err != nil {
		t.Fatalf("initial set: %v", err)
	}
	waitForStoreValue(t, c2, key, value, 2*time.Second)

	c2.control.Stop()
	err = c1.Del(context.Background(), key)
	if !errors.Is(err, ErrWriteIndeterminate) {
		t.Fatalf("del error = %v, want ErrWriteIndeterminate", err)
	}
	entry, found := c1.store.GetEntry(key)
	if !found || !entry.Tombstone {
		t.Fatalf("owner local entry = found %v entry %+v, want tombstone", found, entry)
	}
}

func TestForwardedMajoritySetFailureIsIndeterminate(t *testing.T) {
	gossip1 := getFreePort(t)
	gossip2 := getFreePort(t)
	control1 := getFreePort(t)
	control2 := getFreePort(t)
	peer := fmt.Sprintf("127.0.0.1:%d", gossip1)

	c1, err := Start(Options{
		NodeName:          "forwarded-majority-set-caller",
		ControlBindAddr:   "127.0.0.1",
		ControlBindPort:   control1,
		GossipBindAddr:    "127.0.0.1",
		GossipBindPort:    gossip1,
		SharedKey:         "test-key",
		ReplicationFactor: 2,
		WriteConcern:      WriteConcernMajority,
	})
	if err != nil {
		t.Fatalf("start caller: %v", err)
	}
	defer c1.Close()

	c2, err := Start(Options{
		NodeName:          "forwarded-majority-set-owner",
		ControlBindAddr:   "127.0.0.1",
		ControlBindPort:   control2,
		GossipBindAddr:    "127.0.0.1",
		GossipBindPort:    gossip2,
		PeerNodes:         []string{peer},
		SharedKey:         "test-key",
		ReplicationFactor: 2,
		WriteConcern:      WriteConcernMajority,
	})
	if err != nil {
		t.Fatalf("start owner: %v", err)
	}
	defer c2.Close()

	waitForMembers(t, c1, 2, 2*time.Second)
	waitForMembers(t, c2, 2, 2*time.Second)
	key := keyOwnedByOtherNode(t, c1)
	owner, _ := c1.cluster.GetNode().Get(key)
	if owner != c2.cluster.GetNode().GetSelf() {
		t.Fatalf("test expected key owner %s to be c2", owner)
	}

	c1.control.Stop()
	err = c1.Set(context.Background(), key, []byte("forwarded-indeterminate"), time.Minute)
	if !errors.Is(err, ErrWriteIndeterminate) {
		t.Fatalf("set error = %v, want ErrWriteIndeterminate", err)
	}
}

func TestForwardedMajorityDelFailureIsIndeterminate(t *testing.T) {
	gossip1 := getFreePort(t)
	gossip2 := getFreePort(t)
	control1 := getFreePort(t)
	control2 := getFreePort(t)
	peer := fmt.Sprintf("127.0.0.1:%d", gossip1)

	c1, err := Start(Options{
		NodeName:          "forwarded-majority-del-caller",
		ControlBindAddr:   "127.0.0.1",
		ControlBindPort:   control1,
		GossipBindAddr:    "127.0.0.1",
		GossipBindPort:    gossip1,
		SharedKey:         "test-key",
		ReplicationFactor: 2,
		WriteConcern:      WriteConcernMajority,
	})
	if err != nil {
		t.Fatalf("start caller: %v", err)
	}
	defer c1.Close()

	c2, err := Start(Options{
		NodeName:          "forwarded-majority-del-owner",
		ControlBindAddr:   "127.0.0.1",
		ControlBindPort:   control2,
		GossipBindAddr:    "127.0.0.1",
		GossipBindPort:    gossip2,
		PeerNodes:         []string{peer},
		SharedKey:         "test-key",
		ReplicationFactor: 2,
		WriteConcern:      WriteConcernMajority,
	})
	if err != nil {
		t.Fatalf("start owner: %v", err)
	}
	defer c2.Close()

	waitForMembers(t, c1, 2, 2*time.Second)
	waitForMembers(t, c2, 2, 2*time.Second)
	key := keyOwnedByOtherNode(t, c1)
	owner, _ := c1.cluster.GetNode().Get(key)
	if owner != c2.cluster.GetNode().GetSelf() {
		t.Fatalf("test expected key owner %s to be c2", owner)
	}
	if err := c1.Set(context.Background(), key, []byte("delete-me"), time.Minute); err != nil {
		t.Fatalf("initial set: %v", err)
	}
	waitForStoreValue(t, c2, key, []byte("delete-me"), 2*time.Second)

	c1.control.Stop()
	err = c1.Del(context.Background(), key)
	if !errors.Is(err, ErrWriteIndeterminate) {
		t.Fatalf("del error = %v, want ErrWriteIndeterminate", err)
	}
}

func TestDistributedCacheCloseIsIdempotent(t *testing.T) {
	c, err := Start(Options{
		NodeName:          "node-close",
		ControlBindAddr:   "127.0.0.1",
		ControlBindPort:   getFreePort(t),
		GossipBindAddr:    "127.0.0.1",
		GossipBindPort:    getFreePort(t),
		SharedKey:         "test-key",
		ReplicationFactor: 2,
	})
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	if err := c.Close(); err != nil {
		t.Fatalf("first close: %v", err)
	}
	if err := c.Close(); err != nil {
		t.Fatalf("second close: %v", err)
	}
}

func TestDistributedCacheOperationsAfterCloseReturnErrClosed(t *testing.T) {
	c, err := Start(Options{
		NodeName:          "node-closed-contract",
		ControlBindAddr:   "127.0.0.1",
		ControlBindPort:   getFreePort(t),
		GossipBindAddr:    "127.0.0.1",
		GossipBindPort:    getFreePort(t),
		SharedKey:         "test-key",
		ReplicationFactor: 1,
	})
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	if err := c.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	status := c.Status()
	if !status.Closed {
		t.Fatalf("status closed=false, want true: %+v", status)
	}
	if status.Ready {
		t.Fatalf("status ready=true after close: %+v", status)
	}
	if status.ControlReady {
		t.Fatalf("status control ready=true after close: %+v", status)
	}
	if err := c.Ready(context.Background()); !errors.Is(err, ErrClosed) {
		t.Fatalf("Ready error = %v, want ErrClosed", err)
	}
	waitCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := c.WaitReady(waitCtx); !errors.Is(err, ErrClosed) {
		t.Fatalf("WaitReady error = %v, want ErrClosed", err)
	}
	if _, _, err := c.Get(context.Background(), "k"); !errors.Is(err, ErrClosed) {
		t.Fatalf("Get error = %v, want ErrClosed", err)
	}
	if err := c.Set(context.Background(), "k", []byte("v"), time.Minute); !errors.Is(err, ErrClosed) {
		t.Fatalf("Set error = %v, want ErrClosed", err)
	}
	if err := c.Del(context.Background(), "k"); !errors.Is(err, ErrClosed) {
		t.Fatalf("Del error = %v, want ErrClosed", err)
	}
	if _, _, err := c.Fetch(context.Background(), "k"); !errors.Is(err, ErrClosed) {
		t.Fatalf("Fetch error = %v, want ErrClosed", err)
	}
	if err := c.Store(context.Background(), "k", []byte("v"), time.Minute, version.Zero(), control.WriteConcernOne); !errors.Is(err, ErrClosed) {
		t.Fatalf("Store error = %v, want ErrClosed", err)
	}
	if err := c.Delete(context.Background(), "k", version.Zero(), control.WriteConcernOne); !errors.Is(err, ErrClosed) {
		t.Fatalf("Delete error = %v, want ErrClosed", err)
	}
}

func TestForwardedPublicSetUsesOwnerAssignedVersion(t *testing.T) {
	gossip1 := getFreePort(t)
	gossip2 := getFreePort(t)
	control1 := getFreePort(t)
	control2 := getFreePort(t)
	peer := fmt.Sprintf("127.0.0.1:%d", gossip1)

	c1, err := Start(Options{
		NodeName:          "node-forward-set-1",
		ControlBindAddr:   "127.0.0.1",
		ControlBindPort:   control1,
		GossipBindAddr:    "127.0.0.1",
		GossipBindPort:    gossip1,
		SharedKey:         "test-key",
		ReplicationFactor: 2,
	})
	if err != nil {
		t.Fatalf("start node 1: %v", err)
	}
	defer c1.Close()

	c2, err := Start(Options{
		NodeName:          "node-forward-set-2",
		ControlBindAddr:   "127.0.0.1",
		ControlBindPort:   control2,
		GossipBindAddr:    "127.0.0.1",
		GossipBindPort:    gossip2,
		PeerNodes:         []string{peer},
		SharedKey:         "test-key",
		ReplicationFactor: 2,
	})
	if err != nil {
		t.Fatalf("start node 2: %v", err)
	}
	defer c2.Close()

	waitForMembers(t, c1, 2, 2*time.Second)
	waitForMembers(t, c2, 2, 2*time.Second)
	key := keyOwnedByOtherNode(t, c1)
	owner, _ := c1.cluster.GetNode().Get(key)
	ownerCache := c2
	if owner == c1.cluster.GetNode().GetSelf() {
		t.Fatalf("test key unexpectedly owned by caller")
	}
	if owner != c2.cluster.GetNode().GetSelf() {
		t.Fatalf("test expected owner %s to be node 2", owner)
	}

	future := poisonVersionCounter(c1)
	if err := c1.Set(context.Background(), key, []byte("owner-version"), time.Minute); err != nil {
		t.Fatalf("set: %v", err)
	}
	entry := waitForStoreEntry(t, ownerCache, key, 2*time.Second)
	if entry.Version.IsZero() {
		t.Fatalf("owner did not assign version")
	}
	if entry.Version.Compare(future) >= 0 {
		t.Fatalf("forwarded set used caller version %s >= poisoned caller version %s", entry.Version, future)
	}
}

func TestForwardedPublicDelUsesOwnerAssignedVersion(t *testing.T) {
	gossip1 := getFreePort(t)
	gossip2 := getFreePort(t)
	control1 := getFreePort(t)
	control2 := getFreePort(t)
	peer := fmt.Sprintf("127.0.0.1:%d", gossip1)

	c1, err := Start(Options{
		NodeName:          "node-forward-del-1",
		ControlBindAddr:   "127.0.0.1",
		ControlBindPort:   control1,
		GossipBindAddr:    "127.0.0.1",
		GossipBindPort:    gossip1,
		SharedKey:         "test-key",
		ReplicationFactor: 2,
	})
	if err != nil {
		t.Fatalf("start node 1: %v", err)
	}
	defer c1.Close()

	c2, err := Start(Options{
		NodeName:          "node-forward-del-2",
		ControlBindAddr:   "127.0.0.1",
		ControlBindPort:   control2,
		GossipBindAddr:    "127.0.0.1",
		GossipBindPort:    gossip2,
		PeerNodes:         []string{peer},
		SharedKey:         "test-key",
		ReplicationFactor: 2,
	})
	if err != nil {
		t.Fatalf("start node 2: %v", err)
	}
	defer c2.Close()

	waitForMembers(t, c1, 2, 2*time.Second)
	waitForMembers(t, c2, 2, 2*time.Second)
	key := keyOwnedByOtherNode(t, c1)
	owner, _ := c1.cluster.GetNode().Get(key)
	if owner != c2.cluster.GetNode().GetSelf() {
		t.Fatalf("test expected owner %s to be node 2", owner)
	}
	if err := c1.Set(context.Background(), key, []byte("delete-me"), time.Minute); err != nil {
		t.Fatalf("set: %v", err)
	}
	waitForStoreValue(t, c2, key, []byte("delete-me"), 2*time.Second)

	future := poisonVersionCounter(c1)
	if err := c1.Del(context.Background(), key); err != nil {
		t.Fatalf("del: %v", err)
	}
	entry := waitForStoreEntry(t, c2, key, 2*time.Second)
	if !entry.Tombstone {
		t.Fatalf("expected tombstone, got %+v", entry)
	}
	if entry.Version.Compare(future) >= 0 {
		t.Fatalf("forwarded del used caller version %s >= poisoned caller version %s", entry.Version, future)
	}
}

func TestLocalSetAdvancesBeyondObservedTombstoneVersion(t *testing.T) {
	c, err := Start(Options{
		NodeName:          "node-version-set",
		ControlBindAddr:   "127.0.0.1",
		ControlBindPort:   getFreePort(t),
		GossipBindAddr:    "127.0.0.1",
		GossipBindPort:    getFreePort(t),
		SharedKey:         "test-key",
		ReplicationFactor: 2,
	})
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	defer c.Close()

	key := "observed-tombstone"
	observed := testVersion(time.Now().Add(24*time.Hour).UnixMilli(), "future")
	if err := c.deleteLocalVersioned(key, observed); err != nil {
		t.Fatalf("peer tombstone: %v", err)
	}
	resetVersionCounter(c)

	value := []byte("after-observed-tombstone")
	if err := c.Set(context.Background(), key, value, time.Minute); err != nil {
		t.Fatalf("set: %v", err)
	}
	entry := waitForStoreEntry(t, c, key, time.Second)
	if entry.Tombstone || string(entry.Value) != string(value) {
		t.Fatalf("entry=%+v, want value %q", entry, value)
	}
	if entry.Version.Compare(observed) <= 0 {
		t.Fatalf("version %s did not advance beyond observed %s", entry.Version, observed)
	}
}

func TestLocalDeleteAdvancesBeyondObservedValueVersion(t *testing.T) {
	c, err := Start(Options{
		NodeName:          "node-version-del",
		ControlBindAddr:   "127.0.0.1",
		ControlBindPort:   getFreePort(t),
		GossipBindAddr:    "127.0.0.1",
		GossipBindPort:    getFreePort(t),
		SharedKey:         "test-key",
		ReplicationFactor: 2,
	})
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	defer c.Close()

	key := "observed-value"
	observed := testVersion(time.Now().Add(24*time.Hour).UnixMilli(), "future")
	if err := c.storeLocalVersioned(key, []byte("observed"), time.Minute, observed); err != nil {
		t.Fatalf("peer value: %v", err)
	}
	resetVersionCounter(c)

	if err := c.Del(context.Background(), key); err != nil {
		t.Fatalf("del: %v", err)
	}
	entry := waitForStoreEntry(t, c, key, time.Second)
	if !entry.Tombstone {
		t.Fatalf("entry=%+v, want tombstone", entry)
	}
	if entry.Version.Compare(observed) <= 0 {
		t.Fatalf("version %s did not advance beyond observed %s", entry.Version, observed)
	}
}

func TestRetryTTLUsesAbsoluteExpiry(t *testing.T) {
	expiresAt := time.Now().Add(30 * time.Millisecond)
	ttl, ok := ttlUntil(expiresAt, time.Minute)
	if !ok {
		t.Fatalf("expected ttl before expiry")
	}
	if ttl <= 0 || ttl > time.Minute {
		t.Fatalf("ttl=%v, want remaining expiry duration", ttl)
	}

	time.Sleep(40 * time.Millisecond)
	if ttl, ok := ttlUntil(expiresAt, time.Minute); ok {
		t.Fatalf("ttlUntil returned ok after expiry with ttl=%v", ttl)
	}
}

func TestRetrySchedulerReleasesDelayedRetry(t *testing.T) {
	c := &DistributedCache{
		logger:       internallog.Default(),
		retryCh:      make(chan retryTask, 1),
		retryDelayCh: make(chan scheduledRetryTask, 1),
		retryStop:    make(chan struct{}),
		opts: Options{
			ReplicationRetryInterval:    10 * time.Millisecond,
			ReplicationRetryMaxAttempts: 2,
			ReplicationRetryQueueSize:   1,
		},
	}
	c.retryWg.Add(1)
	go func() {
		defer c.retryWg.Done()
		c.runRetryScheduler()
	}()
	defer func() {
		close(c.retryStop)
		c.retryWg.Wait()
	}()

	c.scheduleRetry(retryTask{kind: retryDelete, addr: "127.0.0.1:1", key: "k", version: testVersion(1, "retry"), attempts: 1})
	select {
	case task := <-c.retryCh:
		if task.key != "k" || task.attempts != 2 {
			t.Fatalf("retry task = %+v, want key k attempts 2", task)
		}
	case <-time.After(time.Second):
		t.Fatalf("timed out waiting for delayed retry")
	}
}

func TestRetrySchedulerDelayedQueueIsBounded(t *testing.T) {
	c := &DistributedCache{
		logger:       internallog.Default(),
		retryCh:      make(chan retryTask, 1),
		retryDelayCh: make(chan scheduledRetryTask, 1),
		retryStop:    make(chan struct{}),
		opts: Options{
			ReplicationRetryInterval:    time.Hour,
			ReplicationRetryMaxAttempts: 2,
			ReplicationRetryQueueSize:   1,
		},
	}
	defer close(c.retryStop)

	c.scheduleRetry(retryTask{kind: retryDelete, addr: "127.0.0.1:1", key: "first", version: testVersion(1, "retry"), attempts: 1})
	c.scheduleRetry(retryTask{kind: retryDelete, addr: "127.0.0.1:1", key: "second", version: testVersion(1, "retry"), attempts: 1})
	if got := len(c.retryDelayCh); got != 1 {
		t.Fatalf("delayed retry queue length = %d, want bounded length 1", got)
	}
	scheduled := <-c.retryDelayCh
	if scheduled.task.key != "first" {
		t.Fatalf("scheduled task key = %q, want first", scheduled.task.key)
	}
}

func TestDistributedCacheReplicationStoresOnReplicas(t *testing.T) {
	gossip1 := getFreePort(t)
	gossip2 := getFreePort(t)
	control1 := getFreePort(t)
	control2 := getFreePort(t)

	peer := fmt.Sprintf("127.0.0.1:%d", gossip1)

	c1, err := Start(Options{
		NodeName:          "node-1",
		ControlBindAddr:   "127.0.0.1",
		ControlBindPort:   control1,
		GossipBindAddr:    "127.0.0.1",
		GossipBindPort:    gossip1,
		PeerNodes:         []string{},
		SharedKey:         "test-key",
		ReplicationFactor: 2,
	})
	if err != nil {
		t.Fatalf("start node-1: %v", err)
	}
	defer c1.Close()

	c2, err := Start(Options{
		NodeName:          "node-2",
		ControlBindAddr:   "127.0.0.1",
		ControlBindPort:   control2,
		GossipBindAddr:    "127.0.0.1",
		GossipBindPort:    gossip2,
		PeerNodes:         []string{peer},
		SharedKey:         "test-key",
		ReplicationFactor: 2,
	})
	if err != nil {
		t.Fatalf("start node-2: %v", err)
	}
	defer c2.Close()

	waitForMembers(t, c1, 2, 2*time.Second)
	waitForMembers(t, c2, 2, 2*time.Second)
	waitForControlAddr(t, fmt.Sprintf("127.0.0.1:%d", control1), "test-key", 2*time.Second)
	waitForControlAddr(t, fmt.Sprintf("127.0.0.1:%d", control2), "test-key", 2*time.Second)

	key := "replica-key"
	value := []byte("replica-value")
	if err := c1.Set(context.Background(), key, value, 2*time.Second); err != nil {
		t.Fatalf("set: %v", err)
	}

	owner, ok := c1.cluster.GetNode().Get(key)
	if !ok {
		t.Fatalf("no owner for key")
	}
	if owner == c1.cluster.GetNode().GetSelf() {
		waitForStoreValue(t, c1, key, value, 2*time.Second)
		waitForStoreValue(t, c2, key, value, 2*time.Second)
	} else {
		waitForStoreValue(t, c2, key, value, 2*time.Second)
		waitForStoreValue(t, c1, key, value, 2*time.Second)
	}
}

func TestDistributedCacheDeleteReplicates(t *testing.T) {
	gossip1 := getFreePort(t)
	gossip2 := getFreePort(t)
	control1 := getFreePort(t)
	control2 := getFreePort(t)

	peer := fmt.Sprintf("127.0.0.1:%d", gossip1)

	c1, err := Start(Options{
		NodeName:          "node-1",
		ControlBindAddr:   "127.0.0.1",
		ControlBindPort:   control1,
		GossipBindAddr:    "127.0.0.1",
		GossipBindPort:    gossip1,
		PeerNodes:         []string{},
		SharedKey:         "test-key",
		ReplicationFactor: 2,
	})
	if err != nil {
		t.Fatalf("start node-1: %v", err)
	}
	defer c1.Close()

	c2, err := Start(Options{
		NodeName:          "node-2",
		ControlBindAddr:   "127.0.0.1",
		ControlBindPort:   control2,
		GossipBindAddr:    "127.0.0.1",
		GossipBindPort:    gossip2,
		PeerNodes:         []string{peer},
		SharedKey:         "test-key",
		ReplicationFactor: 2,
	})
	if err != nil {
		t.Fatalf("start node-2: %v", err)
	}
	defer c2.Close()

	waitForMembers(t, c1, 2, 2*time.Second)
	waitForMembers(t, c2, 2, 2*time.Second)
	waitForControlAddr(t, fmt.Sprintf("127.0.0.1:%d", control1), "test-key", 2*time.Second)
	waitForControlAddr(t, fmt.Sprintf("127.0.0.1:%d", control2), "test-key", 2*time.Second)

	key := "delete-key"
	value := []byte("delete-value")
	if err := c1.Set(context.Background(), key, value, 2*time.Second); err != nil {
		t.Fatalf("set: %v", err)
	}
	waitForStoreValue(t, c1, key, value, 2*time.Second)
	waitForStoreValue(t, c2, key, value, 2*time.Second)

	if err := c1.Del(context.Background(), key); err != nil {
		t.Fatalf("del: %v", err)
	}
	waitForStoreMiss(t, c1, key, 2*time.Second)
	waitForStoreMiss(t, c2, key, 2*time.Second)
}

func TestSharedKeyAuthFailure(t *testing.T) {
	gossip1 := getFreePort(t)
	control1 := getFreePort(t)

	c1, err := Start(Options{
		NodeName:          "node-1",
		ControlBindAddr:   "127.0.0.1",
		ControlBindPort:   control1,
		GossipBindAddr:    "127.0.0.1",
		GossipBindPort:    gossip1,
		PeerNodes:         []string{},
		SharedKey:         "key-a",
		ReplicationFactor: 2,
	})
	if err != nil {
		t.Fatalf("start node-1: %v", err)
	}
	defer c1.Close()

	addr := fmt.Sprintf("127.0.0.1:%d", control1)
	client, err := control.Dial(addr, control.ClientOptions{SharedKey: "key-b"})
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer client.Close()

	if _, err := client.Ping(context.Background()); err == nil {
		t.Fatalf("expected auth failure, got nil")
	}
}

func TestMTLSHandshake(t *testing.T) {
	dir := t.TempDir()
	ca := generateCA(t, dir)
	server := generateCert(t, dir, "server", "cache.local", ca, false)
	clientPair := generateCert(t, dir, "client", "cache-client", ca, true)

	gossip1 := getFreePort(t)
	gossip2 := getFreePort(t)
	control1 := getFreePort(t)
	control2 := getFreePort(t)

	peer := fmt.Sprintf("127.0.0.1:%d", gossip1)

	tlsOpts := TLSOptions{
		Enabled:           true,
		CertFile:          server.certPath,
		KeyFile:           server.keyPath,
		CAFile:            ca.certPath,
		ClientCertFile:    clientPair.certPath,
		ClientKeyFile:     clientPair.keyPath,
		RequireClientCert: true,
		ServerName:        "cache.local",
	}

	c1, err := Start(Options{
		NodeName:          "node-1",
		ControlBindAddr:   "127.0.0.1",
		ControlBindPort:   control1,
		GossipBindAddr:    "127.0.0.1",
		GossipBindPort:    gossip1,
		PeerNodes:         []string{},
		SharedKey:         "mtls-key",
		ReplicationFactor: 2,
		TLS:               tlsOpts,
	})
	if err != nil {
		t.Fatalf("start node-1: %v", err)
	}
	defer c1.Close()

	c2, err := Start(Options{
		NodeName:          "node-2",
		ControlBindAddr:   "127.0.0.1",
		ControlBindPort:   control2,
		GossipBindAddr:    "127.0.0.1",
		GossipBindPort:    gossip2,
		PeerNodes:         []string{peer},
		SharedKey:         "mtls-key",
		ReplicationFactor: 2,
		TLS:               tlsOpts,
	})
	if err != nil {
		t.Fatalf("start node-2: %v", err)
	}
	defer c2.Close()

	waitForMembers(t, c1, 2, 2*time.Second)
	waitForMembers(t, c2, 2, 2*time.Second)

	cp, err := control.Dial(fmt.Sprintf("127.0.0.1:%d", control2), control.ClientOptions{SharedKey: "mtls-key", TLS: &tls.Config{
		RootCAs:      mustCAPool(t, ca.certPath),
		ServerName:   "cache.local",
		Certificates: []tls.Certificate{mustClientCert(t, clientPair.certPath, clientPair.keyPath)},
	}})
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer cp.Close()

	if _, err := cp.Ping(context.Background()); err != nil {
		t.Fatalf("mtls ping failed: %v", err)
	}
}

func TestMTLSMissingClientCertFails(t *testing.T) {
	dir := t.TempDir()
	ca := generateCA(t, dir)
	server := generateCert(t, dir, "server", "cache.local", ca, false)

	gossip1 := getFreePort(t)
	control1 := getFreePort(t)

	tlsServer := TLSOptions{
		Enabled:           true,
		CertFile:          server.certPath,
		KeyFile:           server.keyPath,
		CAFile:            ca.certPath,
		RequireClientCert: true,
		ServerName:        "cache.local",
	}

	c1, err := Start(Options{
		NodeName:          "node-1",
		ControlBindAddr:   "127.0.0.1",
		ControlBindPort:   control1,
		GossipBindAddr:    "127.0.0.1",
		GossipBindPort:    gossip1,
		PeerNodes:         []string{},
		SharedKey:         "mtls-key",
		ReplicationFactor: 2,
		TLS:               tlsServer,
	})
	if err != nil {
		t.Fatalf("start node-1: %v", err)
	}
	defer c1.Close()

	addr := fmt.Sprintf("127.0.0.1:%d", control1)
	client, err := control.Dial(addr, control.ClientOptions{SharedKey: "mtls-key", TLS: &tls.Config{RootCAs: mustCAPool(t, ca.certPath), ServerName: "cache.local"}})
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer client.Close()

	if _, err := client.Ping(context.Background()); err == nil {
		t.Fatalf("expected mTLS failure without client cert, got nil")
	}
}

func TestMTLSWrongCAFails(t *testing.T) {
	dir := t.TempDir()
	ca := generateCA(t, dir)
	otherCA := generateCA(t, dir)
	server := generateCert(t, dir, "server", "cache.local", ca, false)
	client := generateCert(t, dir, "client", "cache-client", otherCA, true)

	gossip1 := getFreePort(t)
	control1 := getFreePort(t)

	tlsServer := TLSOptions{
		Enabled:           true,
		CertFile:          server.certPath,
		KeyFile:           server.keyPath,
		CAFile:            ca.certPath,
		RequireClientCert: true,
		ServerName:        "cache.local",
		ClientCertFile:    client.certPath,
		ClientKeyFile:     client.keyPath,
	}

	c1, err := Start(Options{
		NodeName:          "node-1",
		ControlBindAddr:   "127.0.0.1",
		ControlBindPort:   control1,
		GossipBindAddr:    "127.0.0.1",
		GossipBindPort:    gossip1,
		PeerNodes:         []string{},
		SharedKey:         "mtls-key",
		ReplicationFactor: 2,
		TLS:               tlsServer,
	})
	if err != nil {
		t.Fatalf("start node-1: %v", err)
	}
	defer c1.Close()

	clientTLS := TLSOptions{
		Enabled:           true,
		CertFile:          client.certPath,
		KeyFile:           client.keyPath,
		CAFile:            ca.certPath,
		RequireClientCert: true,
		ServerName:        "cache.local",
	}
	serverCfg, clientCfg, err := tlsConfigs(clientTLS)
	if err != nil || serverCfg == nil {
		t.Fatalf("tls config: %v", err)
	}
	clientConn, err := control.Dial(fmt.Sprintf("127.0.0.1:%d", control1), control.ClientOptions{SharedKey: "mtls-key", TLS: clientCfg})
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer clientConn.Close()

	if _, err := clientConn.Ping(context.Background()); err == nil {
		t.Fatalf("expected mTLS failure with wrong client cert, got nil")
	}
}
