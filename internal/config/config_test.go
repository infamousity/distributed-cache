package config

import "testing"

func TestLoadBindsCacheEnvOnlyDeploymentFields(t *testing.T) {
	t.Setenv("CACHE_CONFIG", "")
	t.Setenv("CACHE_SHARED_KEY", "env-shared-key")
	t.Setenv("CACHE_CLUSTER_MEMBERLIST_NODE_NAME", "env-node")
	t.Setenv("CACHE_CLUSTER_MEMBERLIST_BIND_ADDRESS", "127.0.0.1")
	t.Setenv("CACHE_CLUSTER_MEMBERLIST_BIND_PORT", "8946")
	t.Setenv("CACHE_CLUSTER_MEMBERLIST_ADVERTISE_ADDRESS", "cache-1")
	t.Setenv("CACHE_CLUSTER_MEMBERLIST_ADVERTISE_PORT", "18946")
	t.Setenv("CACHE_CLUSTER_MEMBERLIST_PARTITION_COUNT", "509")
	t.Setenv("CACHE_REPLICATION_FACTOR", "5")
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
		memberlist.PartitionCount != 509 ||
		memberlist.ReplicationFactor != 5 {
		t.Fatalf("unexpected memberlist config: %+v", memberlist)
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
