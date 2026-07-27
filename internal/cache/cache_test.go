package cache

import (
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/infamousity/distributed-cache/internal/version"
)

func v(n int64) version.Version {
	return version.Version{Physical: n, NodeID: fmt.Sprintf("node-%03d", n)}
}

func TestStoreSetIsImmediatelyReadable(t *testing.T) {
	store, err := NewStore(1 << 20)
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	defer store.Close()

	if ok := store.Set("k", []byte("v"), time.Minute); !ok {
		t.Fatalf("set returned false")
	}
	value, found := store.Get("k")
	if !found {
		t.Fatalf("expected immediate hit")
	}
	if string(value) != "v" {
		t.Fatalf("value=%q, want v", value)
	}
}

func TestStoreReportsAdmissionRejection(t *testing.T) {
	store, err := NewStore(1)
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	defer store.Close()

	err = store.ApplyVersioned("oversized", []byte("value"), time.Minute, v(1))
	if !errors.Is(err, ErrEntryRejected) {
		t.Fatalf("apply error = %v, want ErrEntryRejected", err)
	}
	if entry, found := store.GetEntry("oversized"); found {
		t.Fatalf("rejected entry remained readable: %+v", entry)
	}
	store.mu.Lock()
	_, retained := store.meta["oversized"]
	store.mu.Unlock()
	if retained {
		t.Fatal("rejected entry retained version metadata")
	}
}

func TestAdmissionCountersScaleWithCacheCapacity(t *testing.T) {
	if got := admissionCounters(1 << 20); got <= minAdmissionCounters || got >= maxAdmissionCounters {
		t.Fatalf("1 MiB admission counters = %d, want capacity-derived value", got)
	}
	if got := admissionCounters(1 << 30); got != maxAdmissionCounters {
		t.Fatalf("1 GiB admission counters = %d, want capped value %d", got, maxAdmissionCounters)
	}
	if got := admissionCounters(1); got != minAdmissionCounters {
		t.Fatalf("minimal admission counters = %d, want %d", got, minAdmissionCounters)
	}
}

func TestSnapshotKeysUsesStoreMetadataIndex(t *testing.T) {
	store, err := NewStore(1 << 20)
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	defer store.Close()

	if err := store.ApplyVersioned("value", []byte("v"), time.Minute, v(1)); err != nil {
		t.Fatalf("store value: %v", err)
	}
	if err := store.ApplyDeleteVersioned("tombstone", v(2), time.Minute); err != nil {
		t.Fatalf("store tombstone: %v", err)
	}
	value, ok := store.GetEntry("value")
	if !ok || value.ExpiresAt.IsZero() {
		t.Fatalf("value expiry = %v, found = %t; want a tracked expiry", value.ExpiresAt, ok)
	}
	tombstone, ok := store.GetEntry("tombstone")
	if !ok || tombstone.ExpiresAt.IsZero() {
		t.Fatalf("tombstone expiry = %v, found = %t; want a tracked expiry", tombstone.ExpiresAt, ok)
	}

	keys := store.SnapshotKeys(1)
	if len(keys) != 1 {
		t.Fatalf("snapshot size = %d, want 1", len(keys))
	}
	all := store.SnapshotKeys(0)
	if len(all) != 2 {
		t.Fatalf("full snapshot size = %d, want 2", len(all))
	}
}

func TestEntryCostIncludesKeyBytes(t *testing.T) {
	entry := Entry{Value: []byte("value")}
	if got, want := entryCost("key", entry), int64(len("key")+len("value")); got != want {
		t.Fatalf("entry cost = %d, want %d", got, want)
	}
	entry = Entry{Tombstone: true}
	if got, want := entryCost("key", entry), int64(len("key")+1); got != want {
		t.Fatalf("tombstone cost = %d, want %d", got, want)
	}
}

func TestStoreCopiesValuesAtBoundary(t *testing.T) {
	store, err := NewStore(1 << 20)
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	defer store.Close()

	original := []byte("value")
	if ok := store.Set("k", original, time.Minute); !ok {
		t.Fatalf("set returned false")
	}
	original[0] = 'X'

	value, found := store.Get("k")
	if !found {
		t.Fatalf("expected hit")
	}
	if string(value) != "value" {
		t.Fatalf("stored value changed through input slice: %q", value)
	}

	value[1] = 'X'
	value, found = store.Get("k")
	if !found {
		t.Fatalf("expected hit")
	}
	if string(value) != "value" {
		t.Fatalf("stored value changed through returned slice: %q", value)
	}
}

func TestStoreTombstoneRejectsOlderWrite(t *testing.T) {
	store, err := NewStore(1 << 20)
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	defer store.Close()

	if ok := store.SetVersioned("k", []byte("old"), time.Minute, v(10)); !ok {
		t.Fatalf("initial set returned false")
	}
	if ok := store.DeleteVersioned("k", v(20), time.Minute); !ok {
		t.Fatalf("delete returned false")
	}
	if ok := store.SetVersioned("k", []byte("stale"), time.Minute, v(10)); ok {
		t.Fatalf("stale set was accepted")
	}

	if value, found := store.Get("k"); found {
		t.Fatalf("stale write resurrected value %q", value)
	}
	entry, found := store.GetEntry("k")
	if !found {
		t.Fatalf("expected retained tombstone")
	}
	if !entry.Tombstone || entry.Version.Compare(v(20)) != 0 {
		t.Fatalf("entry=%+v, want tombstone version 20", entry)
	}
}

func TestApplyVersionedReportsStaleCurrentVersion(t *testing.T) {
	store, err := NewStore(1 << 20)
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	defer store.Close()

	if err := store.ApplyDeleteVersioned("k", v(20), time.Minute); err != nil {
		t.Fatalf("delete: %v", err)
	}
	err = store.ApplyVersioned("k", []byte("stale"), time.Minute, v(10))
	if !errors.Is(err, ErrStaleVersion) {
		t.Fatalf("apply error = %v, want ErrStaleVersion", err)
	}
	var stale *StaleVersionError
	if !errors.As(err, &stale) || stale.Current.Compare(v(20)) != 0 {
		t.Fatalf("stale error = %#v, want current version 20", err)
	}
}

func TestStoreNewerWriteReplacesTombstone(t *testing.T) {
	store, err := NewStore(1 << 20)
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	defer store.Close()

	if ok := store.DeleteVersioned("k", v(20), time.Minute); !ok {
		t.Fatalf("delete returned false")
	}
	if ok := store.SetVersioned("k", []byte("new"), time.Minute, v(30)); !ok {
		t.Fatalf("newer set returned false")
	}

	value, found := store.Get("k")
	if !found {
		t.Fatalf("expected newer value")
	}
	if string(value) != "new" {
		t.Fatalf("value=%q, want new", value)
	}
}

func TestStoreUnversionedSetAdvancesBeyondObservedVersion(t *testing.T) {
	store, err := NewStore(1 << 20)
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	defer store.Close()

	observed := version.Version{Physical: time.Now().Add(24 * time.Hour).UnixMilli(), NodeID: "future"}
	if ok := store.DeleteVersioned("k", observed, time.Minute); !ok {
		t.Fatalf("delete returned false")
	}
	if ok := store.Set("k", []byte("new"), time.Minute); !ok {
		t.Fatalf("set returned false")
	}

	entry, found := store.GetEntry("k")
	if !found {
		t.Fatalf("expected value")
	}
	if entry.Tombstone || string(entry.Value) != "new" {
		t.Fatalf("entry=%+v, want value new", entry)
	}
	if entry.Version.Compare(observed) <= 0 {
		t.Fatalf("version %s did not advance beyond observed %s", entry.Version, observed)
	}
}

func TestStoreConcurrentOlderWriteCannotBeatNewerTombstone(t *testing.T) {
	store, err := NewStore(1 << 20)
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	defer store.Close()

	if ok := store.SetVersioned("k", []byte("old"), time.Minute, v(10)); !ok {
		t.Fatalf("initial set returned false")
	}

	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			_ = store.SetVersioned("k", []byte("stale"), time.Minute, v(10))
		}()
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		<-start
		_ = store.DeleteVersioned("k", v(20), time.Minute)
	}()
	close(start)
	wg.Wait()

	entry, found := store.GetEntry("k")
	if !found {
		t.Fatalf("expected retained entry")
	}
	if !entry.Tombstone || entry.Version.Compare(v(20)) != 0 {
		t.Fatalf("entry=%+v, want tombstone version 20", entry)
	}
}

func TestStoreTombstoneMetadataSurvivesPayloadEviction(t *testing.T) {
	store, err := NewStore(1 << 20)
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	defer store.Close()

	if ok := store.DeleteVersioned("k", v(20), time.Minute); !ok {
		t.Fatalf("delete returned false")
	}
	store.cache.Del("k")

	if ok := store.SetVersioned("k", []byte("stale"), time.Minute, v(10)); ok {
		t.Fatalf("stale set was accepted")
	}
	entry, found := store.GetEntry("k")
	if !found {
		t.Fatalf("expected metadata tombstone")
	}
	if !entry.Tombstone || entry.Version.Compare(v(20)) != 0 {
		t.Fatalf("entry=%+v, want tombstone version 20", entry)
	}
}

func TestStoreReclaimsLiveMetadataAfterPayloadEviction(t *testing.T) {
	store, err := NewStore(1 << 20)
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	defer store.Close()

	if ok := store.SetVersioned("k", []byte("value"), time.Minute, v(20)); !ok {
		t.Fatalf("set returned false")
	}
	store.cache.Del("k")

	if entry, found := store.GetEntry("k"); found {
		t.Fatalf("expected evicted value to miss, got %+v", entry)
	}
	store.mu.Lock()
	_, retained := store.meta["k"]
	store.mu.Unlock()
	if retained {
		t.Fatalf("live metadata remained after payload eviction")
	}
}

func TestStoreMetadataExpiresAfterRetention(t *testing.T) {
	store, err := NewStore(1 << 20)
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	defer store.Close()

	if ok := store.DeleteVersioned("k", v(20), 10*time.Millisecond); !ok {
		t.Fatalf("delete returned false")
	}
	time.Sleep(20 * time.Millisecond)
	if entry, found := store.GetEntry("k"); found {
		t.Fatalf("expected expired metadata, got %+v", entry)
	}

	if ok := store.SetVersioned("k", []byte("after-expiry"), time.Minute, v(10)); !ok {
		t.Fatalf("set after expiry returned false")
	}
	value, found := store.Get("k")
	if !found || string(value) != "after-expiry" {
		t.Fatalf("value=%q found=%v, want after-expiry", value, found)
	}
}

func TestStoreGetEntryExpiresOnlyRequestedKey(t *testing.T) {
	store, err := NewStore(1 << 20)
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	defer store.Close()

	now := time.Now()
	store.mu.Lock()
	store.meta["expired-other"] = entryMeta{version: v(1), expiresAt: now.Add(-time.Hour)}
	store.meta["target"] = entryMeta{version: v(2), tombstone: true, expiresAt: now.Add(time.Hour)}
	store.mu.Unlock()

	entry, found := store.GetEntry("target")
	if !found {
		t.Fatalf("expected target tombstone")
	}
	if !entry.Tombstone || entry.Version.Compare(v(2)) != 0 {
		t.Fatalf("entry=%+v, want tombstone version 2", entry)
	}

	store.mu.Lock()
	_, otherPresent := store.meta["expired-other"]
	store.mu.Unlock()
	if !otherPresent {
		t.Fatalf("GetEntry cleaned unrelated expired metadata")
	}
}

func TestStorePutExpiresRequestedKeyBeforeCompare(t *testing.T) {
	store, err := NewStore(1 << 20)
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	defer store.Close()

	if ok := store.DeleteVersioned("k", v(20), 10*time.Millisecond); !ok {
		t.Fatalf("delete returned false")
	}
	time.Sleep(20 * time.Millisecond)
	if ok := store.SetVersioned("k", []byte("after-expiry"), time.Minute, v(10)); !ok {
		t.Fatalf("set after requested-key expiry returned false")
	}
	value, found := store.Get("k")
	if !found || string(value) != "after-expiry" {
		t.Fatalf("value=%q found=%v, want after-expiry", value, found)
	}
}

func TestStoreMetadataCapCleansExpiredEntriesBoundedly(t *testing.T) {
	store, err := NewStore(1 << 20)
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	defer store.Close()

	store.mu.Lock()
	store.maxMetaEntries = 1
	store.meta["expired"] = entryMeta{version: v(1), expiresAt: time.Now().Add(-time.Hour)}
	store.mu.Unlock()

	if ok := store.SetVersioned("new", []byte("value"), time.Minute, v(2)); !ok {
		t.Fatalf("set with expired metadata at cap returned false")
	}
	value, found := store.Get("new")
	if !found || string(value) != "value" {
		t.Fatalf("value=%q found=%v, want value", value, found)
	}
}

func TestStoreMetadataCapReclaimsEvictedLiveEntries(t *testing.T) {
	store, err := NewStore(1 << 20)
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	defer store.Close()

	store.maxMetaEntries = 1
	if ok := store.SetVersioned("evicted", []byte("value"), time.Minute, v(1)); !ok {
		t.Fatalf("initial set returned false")
	}
	store.cache.Del("evicted")

	if ok := store.SetVersioned("new", []byte("value"), time.Minute, v(2)); !ok {
		t.Fatalf("set at metadata cap did not reclaim evicted live entry")
	}
	value, found := store.Get("new")
	if !found || string(value) != "value" {
		t.Fatalf("value=%q found=%v, want value", value, found)
	}
}
