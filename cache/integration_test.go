package cache

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	internalcache "github.com/infamousity/distributed-cache/internal/cache"
	"github.com/infamousity/distributed-cache/internal/config"
	"github.com/infamousity/distributed-cache/internal/control"
)

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

func TestReplicaSelectionPrefersTombstoneOnEqualVersion(t *testing.T) {
	best := control.Entry{Value: []byte("value"), Version: 10}
	tombstone := control.Entry{Version: 10, Tombstone: true}
	if !betterReplicaEntry(tombstone, best, true) {
		t.Fatalf("expected equal-version tombstone to beat value")
	}
	if betterReplicaEntry(best, tombstone, true) {
		t.Fatalf("expected equal-version value not to beat tombstone")
	}
	if !betterReplicaEntry(control.Entry{Value: []byte("newer"), Version: 11}, tombstone, true) {
		t.Fatalf("expected newer value to beat older tombstone")
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

func poisonVersionCounter(c *DistributedCache) uint64 {
	future := uint64(time.Now().Add(24 * time.Hour).UnixNano())
	c.versionMu.Lock()
	c.lastVersion = future
	c.versionMu.Unlock()
	return future
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

	seed := fmt.Sprintf("127.0.0.1:%d", gossip1)

	c1, err := Start(Options{
		NodeName:          "node-1",
		ControlBindAddr:   "127.0.0.1",
		ControlBindPort:   control1,
		GossipBindAddr:    "127.0.0.1",
		GossipBindPort:    gossip1,
		SeedNodes:         []string{},
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
		SeedNodes:         []string{seed},
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

func TestForwardedPublicSetUsesOwnerAssignedVersion(t *testing.T) {
	gossip1 := getFreePort(t)
	gossip2 := getFreePort(t)
	control1 := getFreePort(t)
	control2 := getFreePort(t)
	seed := fmt.Sprintf("127.0.0.1:%d", gossip1)

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
		SeedNodes:         []string{seed},
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
	if entry.Version == 0 {
		t.Fatalf("owner did not assign version")
	}
	if entry.Version >= future {
		t.Fatalf("forwarded set used caller version %d >= poisoned caller version %d", entry.Version, future)
	}
}

func TestForwardedPublicDelUsesOwnerAssignedVersion(t *testing.T) {
	gossip1 := getFreePort(t)
	gossip2 := getFreePort(t)
	control1 := getFreePort(t)
	control2 := getFreePort(t)
	seed := fmt.Sprintf("127.0.0.1:%d", gossip1)

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
		SeedNodes:         []string{seed},
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
	if entry.Version >= future {
		t.Fatalf("forwarded del used caller version %d >= poisoned caller version %d", entry.Version, future)
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

func TestDistributedCacheReplicationStoresOnReplicas(t *testing.T) {
	gossip1 := getFreePort(t)
	gossip2 := getFreePort(t)
	control1 := getFreePort(t)
	control2 := getFreePort(t)

	seed := fmt.Sprintf("127.0.0.1:%d", gossip1)

	c1, err := Start(Options{
		NodeName:          "node-1",
		ControlBindAddr:   "127.0.0.1",
		ControlBindPort:   control1,
		GossipBindAddr:    "127.0.0.1",
		GossipBindPort:    gossip1,
		SeedNodes:         []string{},
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
		SeedNodes:         []string{seed},
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

	seed := fmt.Sprintf("127.0.0.1:%d", gossip1)

	c1, err := Start(Options{
		NodeName:          "node-1",
		ControlBindAddr:   "127.0.0.1",
		ControlBindPort:   control1,
		GossipBindAddr:    "127.0.0.1",
		GossipBindPort:    gossip1,
		SeedNodes:         []string{},
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
		SeedNodes:         []string{seed},
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
		SeedNodes:         []string{},
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

	seed := fmt.Sprintf("127.0.0.1:%d", gossip1)

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
		SeedNodes:         []string{},
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
		SeedNodes:         []string{seed},
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
		SeedNodes:         []string{},
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
		SeedNodes:         []string{},
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
