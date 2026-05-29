package control

import (
	"context"
	"sync"
	"testing"
	"time"
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
	if storeVersion != 0 {
		t.Fatalf("store version=%d, want 0", storeVersion)
	}
	if deleteVersion != 0 {
		t.Fatalf("delete version=%d, want 0", deleteVersion)
	}
}

type captureHandler struct {
	mu            sync.Mutex
	storeVersion  uint64
	deleteVersion uint64
}

func (h *captureHandler) NodeName() string {
	return "test"
}

func (h *captureHandler) Fetch(context.Context, string) (Entry, bool, error) {
	return Entry{}, false, nil
}

func (h *captureHandler) Store(_ context.Context, _ string, _ []byte, _ time.Duration, version uint64, _ WriteConcern) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.storeVersion = version
	return nil
}

func (h *captureHandler) Delete(_ context.Context, _ string, version uint64, _ WriteConcern) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.deleteVersion = version
	return nil
}

func (h *captureHandler) versions() (uint64, uint64) {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.storeVersion, h.deleteVersion
}
