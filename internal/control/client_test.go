package control

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/infamousity/distributed-cache/internal/version"
)

func TestTTLMillisecondsRoundsPositiveTTLUp(t *testing.T) {
	if got := ttlMilliseconds(0); got != 0 {
		t.Fatalf("ttlMilliseconds(0)=%d, want 0", got)
	}
	if got := ttlMilliseconds(500 * time.Microsecond); got != 1 {
		t.Fatalf("ttlMilliseconds(500us)=%d, want 1", got)
	}
	if got := ttlMilliseconds(2 * time.Millisecond); got != 2 {
		t.Fatalf("ttlMilliseconds(2ms)=%d, want 2", got)
	}
}

func TestClientStoreAndDeleteSendUnassignedVersion(t *testing.T) {
	handler := &captureHandler{}
	server, err := NewServer(handler, ServerOptions{BindAddr: "127.0.0.1:0"})
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	server.Start()
	defer server.Stop()

	client, err := Dial(server.Addr(), ClientOptions{DialTimeout: time.Second})
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := client.Store(ctx, "k", []byte("v"), time.Second, WriteConcernOne); err != nil {
		t.Fatalf("store: %v", err)
	}
	if err := client.Delete(ctx, "k", WriteConcernOne); err != nil {
		t.Fatalf("delete: %v", err)
	}

	storeVersion, deleteVersion := handler.versions()
	if !storeVersion.IsZero() {
		t.Fatalf("store version=%s, want zero", storeVersion)
	}
	if !deleteVersion.IsZero() {
		t.Fatalf("delete version=%s, want zero", deleteVersion)
	}
}

func TestClientRoundTripsAssignedVersion(t *testing.T) {
	assigned := version.Version{
		Physical: 123456789,
		Logical:  uint64(^uint32(0)) + 1,
		NodeID:   "node-a",
	}
	handler := &captureHandler{
		fetchEntry: Entry{
			Value:     []byte("remote-value"),
			Version:   assigned,
			Tombstone: true,
		},
		fetchFound: true,
	}
	server, err := NewServer(handler, ServerOptions{BindAddr: "127.0.0.1:0"})
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	server.Start()
	defer server.Stop()

	client, err := Dial(server.Addr(), ClientOptions{DialTimeout: time.Second})
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := client.StoreVersioned(ctx, "k", []byte("v"), time.Second, assigned, WriteConcernMajority); err != nil {
		t.Fatalf("store: %v", err)
	}
	if err := client.DeleteVersioned(ctx, "k", assigned, WriteConcernMajority); err != nil {
		t.Fatalf("delete: %v", err)
	}
	entry, found, err := client.Fetch(ctx, "k")
	if err != nil {
		t.Fatalf("fetch: %v", err)
	}
	if !found {
		t.Fatal("fetch found=false, want true")
	}

	storeVersion, deleteVersion := handler.versions()
	assertVersionEqual(t, storeVersion, assigned)
	assertVersionEqual(t, deleteVersion, assigned)
	assertVersionEqual(t, entry.Version, assigned)
	if string(entry.Value) != "remote-value" {
		t.Fatalf("fetch value=%q, want remote-value", entry.Value)
	}
	if !entry.Tombstone {
		t.Fatal("fetch tombstone=false, want true")
	}
}

type captureHandler struct {
	mu            sync.Mutex
	storeVersion  version.Version
	deleteVersion version.Version
	fetchEntry    Entry
	fetchFound    bool
}

func (h *captureHandler) NodeName() string {
	return "test"
}

func (h *captureHandler) Fetch(context.Context, string) (Entry, bool, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.fetchEntry, h.fetchFound, nil
}

func (h *captureHandler) Store(_ context.Context, _ string, _ []byte, _ time.Duration, version version.Version, _ WriteConcern) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.storeVersion = version
	return nil
}

func (h *captureHandler) Delete(_ context.Context, _ string, version version.Version, _ WriteConcern) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.deleteVersion = version
	return nil
}

func (h *captureHandler) versions() (version.Version, version.Version) {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.storeVersion, h.deleteVersion
}

func assertVersionEqual(t *testing.T, got, want version.Version) {
	t.Helper()
	if got != want {
		t.Fatalf("version=%s, want %s", got, want)
	}
}
