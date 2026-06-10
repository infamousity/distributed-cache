package config

import (
	"errors"
	"net"
	"testing"
)

func TestLoadBindsCacheEnvOnlyDeploymentFields(t *testing.T) {
	t.Setenv("CACHE_CONFIG", "")
	t.Setenv("CACHE_SHARED_KEY", "env-shared-key")
	t.Setenv("CACHE_CLUSTER_MEMBERLIST_NODE_NAME", "env-node")
	t.Setenv("CACHE_CLUSTER_MEMBERLIST_BIND_ADDRESS", "127.0.0.1")
	t.Setenv("CACHE_CLUSTER_MEMBERLIST_BIND_PORT", "8946")
	t.Setenv("CACHE_CLUSTER_MEMBERLIST_ADVERTISE_ADDRESS", "cache-1")
	t.Setenv("CACHE_CLUSTER_MEMBERLIST_ADVERTISE_PORT", "18946")
	t.Setenv("CACHE_CLUSTER_MEMBERLIST_PEER_NODES", "cache-1:8946,cache-2:8946")
	t.Setenv("CACHE_CLUSTER_MEMBERLIST_PEER_DNS_NAME", "tasks.cache")
	t.Setenv("CACHE_CLUSTER_MEMBERLIST_PEER_DNS_PORT", "8946")
	t.Setenv("CACHE_CLUSTER_MEMBERLIST_PARTITION_COUNT", "509")
	t.Setenv("CACHE_REPLICATION_FACTOR", "5")
	t.Setenv("CACHE_PEERS_REFRESH_INTERVAL_MS", "15000")
	t.Setenv("CACHE_API_BIND_ADDR", "127.0.0.1")
	t.Setenv("CACHE_API_BIND_PORT", "9090")
	t.Setenv("CACHE_CONTROL_ADVERTISE_ADDR", "cache-1:9090")
	t.Setenv("CACHE_CLUSTER_TLS_ENABLED", "true")
	t.Setenv("CACHE_CLUSTER_TLS_CERT_FILE", "/tls/server.crt")
	t.Setenv("CACHE_CLUSTER_TLS_KEY_FILE", "/tls/server.key")
	t.Setenv("CACHE_CLUSTER_TLS_CA_FILE", "/tls/ca.crt")
	t.Setenv("CACHE_CLUSTER_TLS_SERVER_NAME", "cache.local")
	t.Setenv("CACHE_CLUSTER_TLS_CLIENT_CERT_FILE", "/tls/client.crt")
	t.Setenv("CACHE_CLUSTER_TLS_CLIENT_KEY_FILE", "/tls/client.key")
	t.Setenv("CACHE_CLUSTER_TLS_REQUIRE_CLIENT_CERT", "true")
	t.Setenv("CACHE_ALLOW_INSECURE", "true")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if cfg.Common.Cache.SharedKey != "env-shared-key" {
		t.Fatalf("shared key = %q", cfg.Common.Cache.SharedKey)
	}
	memberlist := cfg.Common.Cache.Cluster.MemberList
	if memberlist.NodeName != "env-node" ||
		memberlist.BindAddr != "127.0.0.1" ||
		memberlist.BindPort != 8946 ||
		memberlist.AdvertiseAddr != "cache-1" ||
		memberlist.AdvertisePort != 18946 ||
		len(memberlist.PeerNodes) != 2 ||
		memberlist.PeerNodes[0] != "cache-1:8946" ||
		memberlist.PeerNodes[1] != "cache-2:8946" ||
		memberlist.PeerDNSName != "tasks.cache" ||
		memberlist.PeerDNSPort != 8946 ||
		memberlist.PartitionCount != 509 ||
		memberlist.ReplicationFactor != 5 {
		t.Fatalf("unexpected memberlist config: %+v", memberlist)
	}
	if cfg.Common.Cache.Peers.RefreshIntervalMs != 15000 {
		t.Fatalf("peer refresh interval = %d", cfg.Common.Cache.Peers.RefreshIntervalMs)
	}
	if cfg.Common.Cache.Control.AdvertiseAddr != "cache-1:9090" {
		t.Fatalf("control advertise addr = %q", cfg.Common.Cache.Control.AdvertiseAddr)
	}
	tls := cfg.Common.Cache.Cluster.Tls
	if !tls.Enabled ||
		tls.CertFile != "/tls/server.crt" ||
		tls.KeyFile != "/tls/server.key" ||
		tls.CaFile != "/tls/ca.crt" ||
		tls.ServerName != "cache.local" ||
		tls.ClientCertFile != "/tls/client.crt" ||
		tls.ClientKeyFile != "/tls/client.key" ||
		!tls.RequireClientCert {
		t.Fatalf("unexpected tls config: %+v", tls)
	}
	if !cfg.Common.Cache.Diagnostics.AllowInsecure {
		t.Fatalf("expected allow insecure from env")
	}
}

func TestPeerIPFromSelectsLocalAddressOnPeerSubnet(t *testing.T) {
	local := mustIPNet(t, "10.10.1.7/24")
	other := mustIPNet(t, "172.20.0.4/16")

	got, err := peerIPFrom("tasks.cache", []*net.IPNet{other, local}, func(name string) ([]net.IP, error) {
		if name != "tasks.cache" {
			t.Fatalf("lookup name = %q", name)
		}
		return []net.IP{net.ParseIP("10.10.1.23")}, nil
	})
	if err != nil {
		t.Fatalf("peerIPFrom: %v", err)
	}
	if got != "10.10.1.7" {
		t.Fatalf("peer IP = %q", got)
	}
}

func TestPeerIPFromRejectsAmbiguousOrMissingInputs(t *testing.T) {
	local := mustIPNet(t, "10.10.1.7/24")

	if _, err := peerIPFrom("", []*net.IPNet{local}, func(string) ([]net.IP, error) {
		return nil, nil
	}); err == nil {
		t.Fatalf("expected empty peer DNS name to fail")
	}

	if _, err := peerIPFrom("tasks.cache", []*net.IPNet{local}, func(string) ([]net.IP, error) {
		return nil, errors.New("dns failed")
	}); err == nil {
		t.Fatalf("expected DNS failure to fail")
	}

	if _, err := peerIPFrom("tasks.cache", []*net.IPNet{local}, func(string) ([]net.IP, error) {
		return []net.IP{net.ParseIP("192.168.1.12")}, nil
	}); err == nil {
		t.Fatalf("expected unmatched peer subnet to fail")
	}
}

func mustIPNet(t *testing.T, cidr string) *net.IPNet {
	t.Helper()
	ip, ipNet, err := net.ParseCIDR(cidr)
	if err != nil {
		t.Fatalf("parse CIDR %q: %v", cidr, err)
	}
	ipNet.IP = ip.To4()
	return ipNet
}
