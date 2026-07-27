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

	"github.com/infamousity/distributed-cache/config"
	internalcache "github.com/infamousity/distributed-cache/internal/cache"
	"github.com/infamousity/distributed-cache/internal/control"
	internallog "github.com/infamousity/distributed-cache/internal/log"
	"github.com/infamousity/distributed-cache/internal/version"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
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
		{name: "retry max attempts", opts: Options{ReplicationRetryMaxAttempts: -1}},
		{name: "retry queue size", opts: Options{ReplicationRetryQueueSize: -1}},
		{name: "repair interval", opts: Options{RepairInterval: -time.Second}},
		{name: "repair max keys", opts: Options{RepairMaxKeysPerCycle: -1}},
		{name: "self check timeout", opts: Options{SelfCheckTimeout: -time.Second}},
		{name: "peer warn interval", opts: Options{PeerWarnInterval: -time.Second}},
		{name: "partition count", opts: Options{PartitionCount: -1}},
		{name: "replication factor", opts: Options{ReplicationFactor: -1}},
		{name: "cache size", opts: Options{CacheSizeBytes: -1}},
		{name: "write concern", opts: Options{WriteConcern: WriteConcern(99)}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if err := validateConfig(&config.Config{}, tc.opts); err == nil {
				t.Fatalf("expected validation error")
			}
		})
	}
}

func TestConfigFromOptionsPreservesPeerNetworkCIDRs(t *testing.T) {
	cfg, err := configFromOptions(Options{
		NodeName:         "node-1",
		ControlBindAddr:  "127.0.0.1",
		ControlBindPort:  9090,
		GossipBindAddr:   "127.0.0.1",
		GossipBindPort:   8946,
		PeerNetworkCIDRs: []string{"10.60.0.0/24"},
	})
	if err != nil {
		t.Fatalf("config from options: %v", err)
	}
	got := cfg.Common.Cache.Cluster.MemberList.PeerNetworkCIDRs
	if len(got) != 1 || got[0] != "10.60.0.0/24" {
		t.Fatalf("peer network CIDRs = %v", got)
	}
}

func TestValidateConfigRejectsClientCertWithoutTLS(t *testing.T) {
	cfg := &config.Config{}
	cfg.Common.Cache.Cluster.Tls.RequireClientCert = true
	if err := validateConfig(cfg, Options{}); err == nil {
		t.Fatalf("expected require_client_cert without TLS to fail")
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

func TestStartFromConfigRejectsNil(t *testing.T) {
	if c, err := StartFromConfig(nil); err == nil {
		_ = c.Close()
		t.Fatalf("expected nil config to fail")
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

func TestValidateConfigRejectsInvalidPeerNetworkCIDR(t *testing.T) {
	cfg := &config.Config{}
	cfg.Common.Cache.Cluster.MemberList.PeerNetworkCIDRs = []string{"not-a-cidr"}
	if err := validateConfig(cfg, Options{}); err == nil {
		t.Fatalf("expected invalid peer_network_cidrs to fail validation")
	}
}

func TestValidateConfigRejectsMemberlistAdvertiseOutsidePeerNetworkCIDR(t *testing.T) {
	cfg := &config.Config{}
	cfg.Common.Cache.Cluster.MemberList.AdvertiseAddr = "10.136.6.188"
	cfg.Common.Cache.Cluster.MemberList.PeerNetworkCIDRs = []string{"10.136.10.0/24"}
	if err := validateConfig(cfg, Options{}); err == nil {
		t.Fatalf("expected memberlist advertise addr outside peer_network_cidrs to fail validation")
	}
}

func TestValidateConfigAllowsMemberlistAdvertiseInsidePeerNetworkCIDR(t *testing.T) {
	cfg := &config.Config{}
	cfg.Common.Cache.Cluster.MemberList.AdvertiseAddr = "10.136.10.188"
	cfg.Common.Cache.Cluster.MemberList.PeerNetworkCIDRs = []string{"10.136.10.0/24"}
	if err := validateConfig(cfg, Options{}); err != nil {
		t.Fatalf("expected memberlist advertise addr inside peer_network_cidrs to pass validation: %v", err)
	}
}

func TestValidateConfigRejectsControlAdvertiseOutsidePeerNetworkCIDR(t *testing.T) {
	cfg := &config.Config{}
	cfg.Common.Cache.Control.AdvertiseAddr = "10.136.6.188:9090"
	cfg.Common.Cache.Cluster.MemberList.PeerNetworkCIDRs = []string{"10.136.10.0/24"}
	if err := validateConfig(cfg, Options{}); err == nil {
		t.Fatalf("expected control advertise addr outside peer_network_cidrs to fail validation")
	}
}

func TestValidateConfigAllowsControlAdvertiseDNSWithPeerNetworkCIDR(t *testing.T) {
	cfg := &config.Config{}
	cfg.Common.Cache.Control.AdvertiseAddr = "cache-1.internal:9090"
	cfg.Common.Cache.Cluster.MemberList.PeerNetworkCIDRs = []string{"10.136.10.0/24"}
	if err := validateConfig(cfg, Options{}); err != nil {
		t.Fatalf("expected DNS control advertise addr with peer_network_cidrs to pass validation: %v", err)
	}
}

func TestNormalizeMemberlistAdvertiseAutoRequiresPeerNetworkCIDR(t *testing.T) {
	cfg := &config.Config{}
	cfg.Common.Cache.Cluster.MemberList.AdvertiseAddr = "auto"
	if err := normalizeMemberlistAdvertise(cfg); err == nil {
		t.Fatalf("expected auto advertise without peer_network_cidrs to fail")
	}
}

func TestPeerNetworkCIDRFiltering(t *testing.T) {
	networks, err := parsePeerNetworkCIDRs([]string{"10.0.7.0/24, 10.0.8.0/24"})
	if err != nil {
		t.Fatalf("parse peer networks: %v", err)
	}
	if !ipInAnyNetwork(net.ParseIP("10.0.7.12"), networks) {
		t.Fatalf("expected 10.0.7.12 to match peer networks")
	}
	if ipInAnyNetwork(net.ParseIP("10.0.9.12"), networks) {
		t.Fatalf("expected 10.0.9.12 to be filtered out")
	}
}

func TestResolveDNSPeersFiltersPeerNetworkCIDRs(t *testing.T) {
	originalLookup := lookupIPAddr
	lookupIPAddr = func(ctx context.Context, name string) ([]net.IPAddr, error) {
		if name != "tasks.cache" {
			t.Fatalf("lookup name = %q", name)
		}
		return []net.IPAddr{
			{IP: net.ParseIP("10.0.7.11")},
			{IP: net.ParseIP("10.0.8.11")},
			{IP: net.ParseIP("10.0.7.12")},
		}, nil
	}
	defer func() {
		lookupIPAddr = originalLookup
	}()

	c := &DistributedCache{opts: Options{
		PeerDNSName:      "tasks.cache",
		PeerDNSPort:      8946,
		GossipBindPort:   8946,
		PeerNetworkCIDRs: []string{"10.0.7.0/24"},
	}}
	peers, err := c.resolveDNSPeers(context.Background())
	if err != nil {
		t.Fatalf("resolveDNSPeers: %v", err)
	}
	want := []string{"10.0.7.11:8946", "10.0.7.12:8946"}
	if len(peers) != len(want) {
		t.Fatalf("peers = %#v, want %#v", peers, want)
	}
	for i := range want {
		if peers[i] != want[i] {
			t.Fatalf("peers = %#v, want %#v", peers, want)
		}
	}
}

func TestResolveDNSPeersFailsWhenNoAddressMatchesPeerNetworkCIDRs(t *testing.T) {
	originalLookup := lookupIPAddr
	lookupIPAddr = func(context.Context, string) ([]net.IPAddr, error) {
		return []net.IPAddr{{IP: net.ParseIP("10.0.8.11")}}, nil
	}
	defer func() {
		lookupIPAddr = originalLookup
	}()

	c := &DistributedCache{opts: Options{
		PeerDNSName:      "tasks.cache",
		PeerDNSPort:      8946,
		GossipBindPort:   8946,
		PeerNetworkCIDRs: []string{"10.0.7.0/24"},
	}}
	if _, err := c.resolveDNSPeers(context.Background()); err == nil {
		t.Fatalf("expected no matching peer network address to fail")
	}
}

func TestSuppressPostQuorumCanceled(t *testing.T) {
	var quorumReached atomic.Bool
	if suppressPostQuorumCanceled(context.Canceled, &quorumReached) {
		t.Fatalf("pre-quorum context.Canceled should not be suppressed")
	}
	quorumReached.Store(true)
	if !suppressPostQuorumCanceled(context.Canceled, &quorumReached) {
		t.Fatalf("post-quorum context.Canceled should be suppressed")
	}
	if !suppressPostQuorumCanceled(status.Error(codes.Canceled, "caller canceled"), &quorumReached) {
		t.Fatalf("post-quorum grpc canceled should be suppressed")
	}
	if suppressPostQuorumCanceled(context.DeadlineExceeded, &quorumReached) {
		t.Fatalf("deadline exceeded should not be suppressed")
	}
	if suppressPostQuorumCanceled(errors.New("connection refused"), &quorumReached) {
		t.Fatalf("ordinary peer errors should not be suppressed")
	}
}

func TestQuorumReachedMarkedBySuccessfulReplicaBeforeResultConsumed(t *testing.T) {
	quorum := 2
	var ackCount atomic.Int32
	var quorumReached atomic.Bool
	ackCount.Store(1)

	if int(ackCount.Add(1)) >= quorum {
		quorumReached.Store(true)
	}
	if !suppressPostQuorumCanceled(context.Canceled, &quorumReached) {
		t.Fatalf("post-quorum cancellation should be suppressed as soon as replica success reaches quorum")
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

	c.cluster.GetNode().Add("node-expected", controlAddr)
	c.verifyPeer("node-expected", controlAddr)
	c.clientMu.Lock()
	_, ok := c.clients[controlAddr]
	c.clientMu.Unlock()
	if ok {
		t.Fatalf("expected identity mismatch to evict cached client")
	}
	if _, ok := c.cluster.GetNode().GetForwardAddr("node-expected"); ok {
		t.Fatalf("expected identity-mismatched peer to be removed from routing ring")
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

func TestReadyRechecksKnownPeerControlPlane(t *testing.T) {
	gossip1 := getFreePort(t)
	gossip2 := getFreePort(t)
	control1 := getFreePort(t)
	control2 := getFreePort(t)

	c1, err := Start(Options{
		NodeName:            "readiness-caller",
		ControlBindAddr:     "127.0.0.1",
		ControlBindPort:     control1,
		GossipBindAddr:      "127.0.0.1",
		GossipBindPort:      gossip1,
		SharedKey:           "test-key",
		ReplicationFactor:   2,
		MinReadyPeers:       1,
		PeerRefreshInterval: 25 * time.Millisecond,
		ControlTimeout:      50 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("start caller: %v", err)
	}
	defer c1.Close()

	c2, err := Start(Options{
		NodeName:            "readiness-peer",
		ControlBindAddr:     "127.0.0.1",
		ControlBindPort:     control2,
		GossipBindAddr:      "127.0.0.1",
		GossipBindPort:      gossip2,
		PeerNodes:           []string{fmt.Sprintf("127.0.0.1:%d", gossip1)},
		SharedKey:           "test-key",
		ReplicationFactor:   2,
		PeerRefreshInterval: 25 * time.Millisecond,
		ControlTimeout:      50 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("start peer: %v", err)
	}
	defer c2.Close()

	waitForMembers(t, c1, 2, 2*time.Second)
	readyCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := c1.WaitReady(readyCtx); err != nil {
		t.Fatalf("wait ready: %v", err)
	}

	c2.control.Stop()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if errors.Is(c1.Ready(context.Background()), ErrNotReady) {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("readiness stayed true after peer control plane stopped: %+v", c1.Status())
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
	c.peerWarnMu.Lock()
	c.peerWarnLast["127.0.0.1:1"] = time.Now()
	c.peerWarnMu.Unlock()
	c.PeerLeft("peer-1", "127.0.0.1:1")
	c.peerWarnMu.Lock()
	_, warningRetained := c.peerWarnLast["127.0.0.1:1"]
	c.peerWarnMu.Unlock()
	if warningRetained {
		t.Fatalf("peer warning throttle retained after leave")
	}
	if c.setPeerState("peer-1", "127.0.0.1:1", PeerStateVerified, "") {
		t.Fatalf("late verification after leave should not report transition")
	}
	if got := c.Status().Peers[0].State; got != PeerStateLeft {
		t.Fatalf("late verification changed peer state to %q, want %q", got, PeerStateLeft)
	}
	if c.setPeerState("peer-1", "127.0.0.1:1", PeerStateJoined, "") {
		t.Fatalf("rejoin state should not report verified transition")
	}
	if !c.setPeerState("peer-1", "127.0.0.1:1", PeerStateVerified, "") {
		t.Fatalf("verification after rejoin should report transition")
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

func TestSetReportsOwnerAdmissionRejection(t *testing.T) {
	c, err := Start(Options{
		NodeName:          "node-admission-rejection",
		ControlBindAddr:   "127.0.0.1",
		ControlBindPort:   getFreePort(t),
		GossipBindAddr:    "127.0.0.1",
		GossipBindPort:    getFreePort(t),
		SharedKey:         "test-key",
		ReplicationFactor: 1,
		CacheSizeBytes:    1,
	})
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	defer c.Close()

	err = c.Set(context.Background(), "oversized", []byte("value"), time.Minute)
	if !errors.Is(err, ErrEntryRejected) {
		t.Fatalf("set error = %v, want ErrEntryRejected", err)
	}
	if value, found, getErr := c.Get(context.Background(), "oversized"); getErr != nil || found {
		t.Fatalf("rejected value = %q found=%v err=%v, want miss", value, found, getErr)
	}
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

func keyOwnedByNode(t *testing.T, c *DistributedCache, nodeName string) string {
	t.Helper()
	for i := 0; i < 10000; i++ {
		key := fmt.Sprintf("owned-by-%s-%d", nodeName, i)
		if owner, ok := c.cluster.GetNode().Get(key); ok && owner == nodeName {
			return key
		}
	}
	t.Fatalf("failed to find key owned by %s", nodeName)
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

func TestForwardedGetRejectsOwnerIdentityMismatch(t *testing.T) {
	c, err := Start(Options{
		NodeName:          "get-forwarder",
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

	c.cluster.GetNode().Add("claimed-owner", c.control.Addr())
	key := keyOwnedByNode(t, c, "claimed-owner")

	_, _, err = c.Get(context.Background(), key)
	if !errors.Is(err, ErrNotReady) {
		t.Fatalf("Get error = %v, want ErrNotReady", err)
	}
	if _, ok := c.cluster.GetNode().GetForwardAddr("claimed-owner"); ok {
		t.Fatalf("identity-mismatched owner remained in routing ring")
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

func TestReplicaFallbackUsesCallerContextAfterOwnerTimeout(t *testing.T) {
	c, err := Start(Options{
		NodeName:          "fallback-caller",
		ControlBindAddr:   "127.0.0.1",
		ControlBindPort:   getFreePort(t),
		GossipBindAddr:    "127.0.0.1",
		GossipBindPort:    getFreePort(t),
		SharedKey:         "test-key",
		ReplicationFactor: 3,
		ControlTimeout:    50 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("start caller: %v", err)
	}
	defer c.Close()

	owner, err := control.NewServer(&testControlHandler{
		name: "fallback-owner",
		fetch: func(ctx context.Context, _ string) (control.Entry, bool, error) {
			<-ctx.Done()
			return control.Entry{}, false, ctx.Err()
		},
	}, control.ServerOptions{BindAddr: "127.0.0.1:0", SharedKey: "test-key"})
	if err != nil {
		t.Fatalf("start owner server: %v", err)
	}
	owner.Start()
	defer owner.Stop()

	wantVersion := testVersion(time.Now().UnixMilli(), "fallback-replica")
	replica, err := control.NewServer(&testControlHandler{
		name: "fallback-replica",
		fetch: func(context.Context, string) (control.Entry, bool, error) {
			return control.Entry{Value: []byte("replica-value"), Version: wantVersion}, true, nil
		},
	}, control.ServerOptions{BindAddr: "127.0.0.1:0", SharedKey: "test-key"})
	if err != nil {
		t.Fatalf("start replica server: %v", err)
	}
	replica.Start()
	defer replica.Stop()

	c.cluster.GetNode().Add("fallback-owner", owner.Addr())
	c.cluster.GetNode().Add("fallback-replica", replica.Addr())
	key := keyOwnedByNode(t, c, "fallback-owner")

	value, found, err := c.Get(context.Background(), key)
	if err != nil {
		t.Fatalf("get through replica fallback: %v", err)
	}
	if !found || string(value) != "replica-value" {
		t.Fatalf("fallback value = found %v value %q", found, value)
	}
}

func TestClientLookupIsNotBlockedByUnrelatedDial(t *testing.T) {
	c, err := Start(Options{
		NodeName:          "client-lock",
		ControlBindAddr:   "127.0.0.1",
		ControlBindPort:   getFreePort(t),
		GossipBindAddr:    "127.0.0.1",
		GossipBindPort:    getFreePort(t),
		SharedKey:         "test-key",
		ReplicationFactor: 1,
		ControlTimeout:    500 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	defer c.Close()

	if _, err := c.clientFor(c.control.Addr()); err != nil {
		t.Fatalf("cache healthy client: %v", err)
	}

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen stalled peer: %v", err)
	}
	defer listener.Close()
	accepted := make(chan net.Conn, 1)
	go func() {
		conn, _ := listener.Accept()
		accepted <- conn
	}()

	dialDone := make(chan error, 1)
	go func() {
		_, err := c.clientFor(listener.Addr().String())
		dialDone <- err
	}()
	stalledConn := <-accepted
	if stalledConn == nil {
		t.Fatal("stalled peer did not accept connection")
	}

	start := time.Now()
	if _, err := c.clientFor(c.control.Addr()); err != nil {
		t.Fatalf("reuse healthy client: %v", err)
	}
	if elapsed := time.Since(start); elapsed >= 150*time.Millisecond {
		t.Fatalf("healthy client lookup blocked for %s behind unrelated dial", elapsed)
	}

	_ = stalledConn.Close()
	_ = listener.Close()
	select {
	case <-dialDone:
	case <-time.After(time.Second):
		t.Fatal("stalled dial did not terminate")
	}
}

func TestMajorityReplicationRetainsValueAfterSetReturns(t *testing.T) {
	c, err := Start(Options{
		NodeName:                    "value-owner",
		ControlBindAddr:             "127.0.0.1",
		ControlBindPort:             getFreePort(t),
		GossipBindAddr:              "127.0.0.1",
		GossipBindPort:              getFreePort(t),
		SharedKey:                   "test-key",
		ReplicationFactor:           3,
		ControlTimeout:              time.Second,
		ReplicationRetryInterval:    50 * time.Millisecond,
		ReplicationRetryMaxAttempts: 3,
	})
	if err != nil {
		t.Fatalf("start owner: %v", err)
	}
	defer c.Close()

	fastStore := make(chan []byte, 1)
	fast, err := control.NewServer(
		&testControlHandler{name: "value-fast", storeCh: fastStore},
		control.ServerOptions{BindAddr: "127.0.0.1:0", SharedKey: "test-key"},
	)
	if err != nil {
		t.Fatalf("start fast replica: %v", err)
	}
	fast.Start()
	defer fast.Stop()

	slowAddr := fmt.Sprintf("127.0.0.1:%d", getFreePort(t))
	c.cluster.GetNode().Add("value-fast", fast.Addr())
	c.cluster.GetNode().Add("value-slow", slowAddr)
	key := keyOwnedByNode(t, c, c.NodeName())
	value := []byte("original")

	if err := c.Set(context.Background(), key, value, time.Minute, WithWriteConcern(WriteConcernMajority)); err != nil {
		t.Fatalf("majority set: %v", err)
	}
	select {
	case <-fastStore:
	case <-time.After(time.Second):
		t.Fatal("fast replica did not acknowledge")
	}
	copy(value, []byte("mutated!"))

	slowStore := make(chan []byte, 1)
	slow, err := control.NewServer(
		&testControlHandler{name: "value-slow", storeCh: slowStore},
		control.ServerOptions{BindAddr: slowAddr, SharedKey: "test-key"},
	)
	if err != nil {
		t.Fatalf("start slow replica: %v", err)
	}
	slow.Start()
	defer slow.Stop()

	select {
	case got := <-slowStore:
		if string(got) != "original" {
			t.Fatalf("slow replica value = %q, want original", got)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("slow replica did not receive pending replication")
	}
}

func TestCloseWaitsForPostQuorumReplication(t *testing.T) {
	c, err := Start(Options{
		NodeName:          "close-owner",
		ControlBindAddr:   "127.0.0.1",
		ControlBindPort:   getFreePort(t),
		GossipBindAddr:    "127.0.0.1",
		GossipBindPort:    getFreePort(t),
		SharedKey:         "test-key",
		ReplicationFactor: 3,
		ControlTimeout:    time.Second,
	})
	if err != nil {
		t.Fatalf("start owner: %v", err)
	}

	fast, err := control.NewServer(
		&testControlHandler{name: "close-fast", storeCh: make(chan []byte, 1)},
		control.ServerOptions{BindAddr: "127.0.0.1:0", SharedKey: "test-key"},
	)
	if err != nil {
		t.Fatalf("start fast replica: %v", err)
	}
	fast.Start()
	defer fast.Stop()

	slowStarted := make(chan struct{}, 1)
	releaseSlow := make(chan struct{})
	slow, err := control.NewServer(
		&testControlHandler{
			name: "close-slow",
			store: func(context.Context, string, []byte, time.Duration, version.Version, control.WriteConcern) error {
				select {
				case slowStarted <- struct{}{}:
				default:
				}
				<-releaseSlow
				return nil
			},
		},
		control.ServerOptions{BindAddr: "127.0.0.1:0", SharedKey: "test-key"},
	)
	if err != nil {
		t.Fatalf("start slow replica: %v", err)
	}
	slow.Start()
	defer slow.Stop()

	c.cluster.GetNode().Add("close-fast", fast.Addr())
	c.cluster.GetNode().Add("close-slow", slow.Addr())
	key := keyOwnedByNode(t, c, c.NodeName())
	if err := c.Set(context.Background(), key, []byte("value"), time.Minute, WithWriteConcern(WriteConcernMajority)); err != nil {
		t.Fatalf("majority set: %v", err)
	}
	select {
	case <-slowStarted:
	case <-time.After(time.Second):
		t.Fatal("slow post-quorum replication did not start")
	}

	closeDone := make(chan error, 1)
	go func() {
		closeDone <- c.Close()
	}()
	select {
	case err := <-closeDone:
		t.Fatalf("Close returned before post-quorum replication finished: %v", err)
	case <-time.After(100 * time.Millisecond):
	}

	close(releaseSlow)
	select {
	case err := <-closeDone:
		if err != nil {
			t.Fatalf("Close: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Close did not return after post-quorum replication finished")
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

func TestMajorityWritesRebaseAfterStaleReplicaRejection(t *testing.T) {
	gossip1 := getFreePort(t)
	gossip2 := getFreePort(t)
	control1 := getFreePort(t)
	control2 := getFreePort(t)

	c1, err := Start(Options{
		NodeName:          "stale-ack-owner",
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
		NodeName:          "stale-ack-replica",
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
	future := version.Version{Physical: time.Now().Add(24 * time.Hour).UnixMilli(), NodeID: c2.NodeName()}
	if err := c2.store.ApplyVersioned(key, []byte("replica-newer"), time.Minute, future); err != nil {
		t.Fatalf("seed newer replica version: %v", err)
	}

	err = c1.Set(context.Background(), key, []byte("owner-write"), time.Minute)
	if err != nil {
		t.Fatalf("set after version conflict: %v", err)
	}
	entry, found := c2.store.GetEntry(key)
	if !found || entry.Version.Compare(future) <= 0 || string(entry.Value) != "owner-write" {
		t.Fatalf("replica entry = found %v %+v, want rebased owner write", found, entry)
	}

	newer := version.Version{Physical: future.Physical + int64((24*time.Hour)/time.Millisecond), NodeID: c2.NodeName()}
	if err := c2.store.ApplyVersioned(key, []byte("replica-newer-again"), time.Minute, newer); err != nil {
		t.Fatalf("seed newer replica version before delete: %v", err)
	}
	if err := c1.Del(context.Background(), key); err != nil {
		t.Fatalf("delete after version conflict: %v", err)
	}
	entry, found = c2.store.GetEntry(key)
	if !found || !entry.Tombstone || entry.Version.Compare(newer) <= 0 {
		t.Fatalf("replica entry = found %v %+v, want rebased tombstone", found, entry)
	}
}

func TestWriteConcernAllRequiresEveryRF3Replica(t *testing.T) {
	gossip1, gossip2, gossip3 := getFreePort(t), getFreePort(t), getFreePort(t)
	control1, control2, control3 := getFreePort(t), getFreePort(t), getFreePort(t)

	newNode := func(name string, gossipPort, controlPort int, peers []string) *DistributedCache {
		t.Helper()
		c, err := Start(Options{
			NodeName:          name,
			ControlBindAddr:   "127.0.0.1",
			ControlBindPort:   controlPort,
			GossipBindAddr:    "127.0.0.1",
			GossipBindPort:    gossipPort,
			PeerNodes:         peers,
			SharedKey:         "test-key",
			ReplicationFactor: 3,
			ControlTimeout:    100 * time.Millisecond,
		})
		if err != nil {
			t.Fatalf("start %s: %v", name, err)
		}
		return c
	}

	c1 := newNode("all-owner", gossip1, control1, nil)
	defer c1.Close()
	peer := []string{fmt.Sprintf("127.0.0.1:%d", gossip1)}
	c2 := newNode("all-caller", gossip2, control2, peer)
	defer c2.Close()
	c3 := newNode("all-unavailable", gossip3, control3, peer)
	defer c3.Close()

	waitForMembers(t, c1, 3, 3*time.Second)
	waitForMembers(t, c2, 3, 3*time.Second)
	waitForMembers(t, c3, 3, 3*time.Second)

	keyOwnedByC1 := func(prefix string) string {
		t.Helper()
		for i := 0; i < 10000; i++ {
			key := fmt.Sprintf("%s-%d", prefix, i)
			owner, ok := c2.cluster.GetNode().Get(key)
			if ok && owner == c1.NodeName() {
				return key
			}
		}
		t.Fatalf("failed to find key owned by %s", c1.NodeName())
		return ""
	}

	c3.control.Stop()
	if err := c2.Set(context.Background(), keyOwnedByC1("majority"), []byte("v"), time.Minute, WithWriteConcern(WriteConcernMajority)); err != nil {
		t.Fatalf("majority write with one unavailable replica: %v", err)
	}
	err := c2.Set(context.Background(), keyOwnedByC1("all"), []byte("v"), time.Minute, WithWriteConcern(WriteConcernAll))
	if !errors.Is(err, ErrWriteIndeterminate) {
		t.Fatalf("all write error = %v, want ErrWriteIndeterminate", err)
	}
	err = c2.Del(context.Background(), keyOwnedByC1("all-delete"), WithWriteConcern(WriteConcernAll))
	if !errors.Is(err, ErrWriteIndeterminate) {
		t.Fatalf("all delete error = %v, want ErrWriteIndeterminate", err)
	}
}

func TestConfiguredWriteConcernDoesNotDegradeWithUndersizedRing(t *testing.T) {
	c, err := Start(Options{
		NodeName:          "undersized-ring",
		ControlBindAddr:   "127.0.0.1",
		ControlBindPort:   getFreePort(t),
		GossipBindAddr:    "127.0.0.1",
		GossipBindPort:    getFreePort(t),
		SharedKey:         "test-key",
		ReplicationFactor: 3,
	})
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	defer c.Close()

	for _, writeConcern := range []WriteConcern{WriteConcernMajority, WriteConcernAll} {
		key := fmt.Sprintf("undersized-%d", writeConcern)
		err := c.Set(context.Background(), key, []byte("local"), time.Minute, WithWriteConcern(writeConcern))
		if !errors.Is(err, ErrWriteIndeterminate) {
			t.Fatalf("write concern %d error = %v, want ErrWriteIndeterminate", writeConcern, err)
		}
		value, found := c.store.Get(key)
		if !found || string(value) != "local" {
			t.Fatalf("write concern %d local value = found %v value %q", writeConcern, found, value)
		}

		deleteKey := fmt.Sprintf("undersized-delete-%d", writeConcern)
		if err := c.Set(context.Background(), deleteKey, []byte("delete"), time.Minute, WithWriteConcern(WriteConcernOne)); err != nil {
			t.Fatalf("seed delete key for write concern %d: %v", writeConcern, err)
		}
		err = c.Del(context.Background(), deleteKey, WithWriteConcern(writeConcern))
		if !errors.Is(err, ErrWriteIndeterminate) {
			t.Fatalf("delete concern %d error = %v, want ErrWriteIndeterminate", writeConcern, err)
		}
		entry, found := c.store.GetEntry(deleteKey)
		if !found || !entry.Tombstone {
			t.Fatalf("delete concern %d local entry = found %v entry %+v", writeConcern, found, entry)
		}
	}
}

func TestPerCallWriteConcernRejectsUnknownValue(t *testing.T) {
	c, err := Start(Options{
		NodeName:          "invalid-call-write-concern",
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
	defer c.Close()

	if err := c.Set(context.Background(), "set", []byte("value"), time.Minute, WithWriteConcern(WriteConcern(99))); err == nil {
		t.Fatal("expected Set to reject unknown write concern")
	}
	if _, found := c.store.Get("set"); found {
		t.Fatal("invalid Set mutated local state")
	}
	if err := c.Del(context.Background(), "del", WithWriteConcern(WriteConcern(99))); err == nil {
		t.Fatal("expected Del to reject unknown write concern")
	}
	if _, found := c.store.GetEntry("del"); found {
		t.Fatal("invalid Del created a tombstone")
	}
}

func TestSetRejectsNegativeTTLWithoutMutation(t *testing.T) {
	c, err := Start(Options{
		NodeName:          "invalid-ttl",
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
	defer c.Close()

	if err := c.Set(context.Background(), "set", []byte("value"), -time.Nanosecond); !errors.Is(err, ErrInvalidTTL) {
		t.Fatalf("Set error = %v, want ErrInvalidTTL", err)
	}
	if _, found := c.store.GetEntry("set"); found {
		t.Fatal("negative-TTL Set mutated local state")
	}
	if err := c.Store(context.Background(), "store", []byte("value"), -time.Nanosecond, version.Zero(), control.WriteConcernOne); !errors.Is(err, ErrInvalidTTL) {
		t.Fatalf("Store error = %v, want ErrInvalidTTL", err)
	}
	if _, found := c.store.GetEntry("store"); found {
		t.Fatal("negative-TTL Store mutated local state")
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

func TestCloseWaitsForAdmittedOperation(t *testing.T) {
	c, err := Start(Options{
		NodeName:          "close-caller",
		ControlBindAddr:   "127.0.0.1",
		ControlBindPort:   getFreePort(t),
		GossipBindAddr:    "127.0.0.1",
		GossipBindPort:    getFreePort(t),
		SharedKey:         "test-key",
		ReplicationFactor: 1,
		ControlTimeout:    time.Second,
	})
	if err != nil {
		t.Fatalf("start caller: %v", err)
	}

	entered := make(chan struct{})
	release := make(chan struct{})
	owner, err := control.NewServer(&testControlHandler{
		name: "close-owner",
		fetch: func(context.Context, string) (control.Entry, bool, error) {
			close(entered)
			<-release
			return control.Entry{Value: []byte("value"), Version: testVersion(time.Now().UnixMilli(), "close-owner")}, true, nil
		},
	}, control.ServerOptions{BindAddr: "127.0.0.1:0", SharedKey: "test-key"})
	if err != nil {
		t.Fatalf("start owner server: %v", err)
	}
	owner.Start()
	defer owner.Stop()

	c.cluster.GetNode().Add("close-owner", owner.Addr())
	key := keyOwnedByNode(t, c, "close-owner")
	getDone := make(chan error, 1)
	go func() {
		_, _, err := c.Get(context.Background(), key)
		getDone <- err
	}()
	<-entered

	closeDone := make(chan error, 1)
	go func() {
		closeDone <- c.Close()
	}()
	select {
	case err := <-closeDone:
		t.Fatalf("Close returned before admitted Get completed: %v", err)
	case <-time.After(50 * time.Millisecond):
	}

	close(release)
	if err := <-getDone; err != nil {
		t.Fatalf("admitted Get failed during Close: %v", err)
	}
	if err := <-closeDone; err != nil {
		t.Fatalf("Close: %v", err)
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

type testControlHandler struct {
	name    string
	fetch   func(context.Context, string) (control.Entry, bool, error)
	store   func(context.Context, string, []byte, time.Duration, version.Version, control.WriteConcern) error
	storeCh chan []byte
}

func (h *testControlHandler) NodeName() string {
	return h.name
}

func (h *testControlHandler) Fetch(ctx context.Context, key string) (control.Entry, bool, error) {
	if h.fetch == nil {
		return control.Entry{}, false, nil
	}
	return h.fetch(ctx, key)
}

func (h *testControlHandler) Store(ctx context.Context, key string, value []byte, ttl time.Duration, ver version.Version, wc control.WriteConcern) error {
	if h.store != nil {
		return h.store(ctx, key, value, ttl, ver, wc)
	}
	if h.storeCh != nil {
		h.storeCh <- cloneBytes(value)
	}
	return nil
}

func (h *testControlHandler) Delete(context.Context, string, version.Version, control.WriteConcern) error {
	return nil
}
