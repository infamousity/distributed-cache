package cache

import (
	"fmt"
	"sync"
	"time"

	"github.com/dgraph-io/ristretto/v2"

	"github.com/infamousity/distributed-cache/internal/version"
)

const (
	defaultMaxMetadataEntries = 1_000_000
	metadataCleanupScanLimit  = 64
)

type Store struct {
	mu             sync.Mutex
	cache          *ristretto.Cache[string, Entry]
	meta           map[string]entryMeta
	maxMetaEntries int
	lastVersion    version.Version
	nodeID         string
}

type Entry struct {
	Value     []byte
	Version   version.Version
	Tombstone bool
}

type entryMeta struct {
	version   version.Version
	tombstone bool
	expiresAt time.Time
}

func NewStore(maxCost int64) (*Store, error) {
	if maxCost <= 0 {
		return nil, fmt.Errorf("maxCost must be > 0")
	}
	c, err := ristretto.NewCache[string, Entry](&ristretto.Config[string, Entry]{
		NumCounters: 1e7,
		MaxCost:     maxCost,
		BufferItems: 64,
	})
	if err != nil {
		return nil, fmt.Errorf("create ristretto cache: %w", err)
	}
	return &Store{
		cache:          c,
		meta:           make(map[string]entryMeta),
		maxMetaEntries: defaultMaxMetadataEntries,
		nodeID:         "local",
	}, nil
}

func (s *Store) SetNodeID(nodeID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if nodeID != "" {
		s.nodeID = nodeID
	}
}

func (s *Store) Get(key string) ([]byte, bool) {
	entry, ok := s.GetEntry(key)
	if !ok {
		return nil, false
	}
	if entry.Tombstone {
		return nil, false
	}
	return entry.Value, true
}

func (s *Store) GetEntry(key string) (Entry, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now()
	if s.expireKeyLocked(key, now) {
		return Entry{}, false
	}
	meta, metaOK := s.meta[key]
	if metaOK && meta.tombstone {
		return Entry{Version: meta.version, Tombstone: true}, true
	}
	entry, ok := s.cache.Get(key)
	if !ok {
		return Entry{}, false
	}
	if metaOK {
		entry.Version = meta.version
		entry.Tombstone = meta.tombstone
	}
	entry.Value = cloneBytes(entry.Value)
	return entry, true
}

func (s *Store) Set(key string, value []byte, ttl time.Duration) bool {
	return s.SetVersioned(key, value, ttl, s.nextVersionLocked(time.Now()))
}

func (s *Store) SetVersioned(key string, value []byte, ttl time.Duration, version version.Version) bool {
	return s.put(key, Entry{
		Value:   cloneBytes(value),
		Version: version,
	}, ttl)
}

func (s *Store) DeleteVersioned(key string, version version.Version, tombstoneTTL time.Duration) bool {
	return s.put(key, Entry{
		Version:   version,
		Tombstone: true,
	}, tombstoneTTL)
}

func (s *Store) put(key string, entry Entry, ttl time.Duration) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now()
	s.expireKeyLocked(key, now)
	nextMeta := entryMeta{
		version:   entry.Version,
		tombstone: entry.Tombstone,
		expiresAt: expiresAtForTTL(now, ttl),
	}
	if current, ok := s.meta[key]; ok && metaIsOlder(nextMeta, current) {
		return true
	}
	if _, ok := s.meta[key]; !ok && len(s.meta) >= s.maxMetaEntries {
		s.cleanupSomeExpiredLocked(now)
		if len(s.meta) >= s.maxMetaEntries {
			return false
		}
	}
	cost := int64(len(entry.Value))
	if entry.Tombstone {
		cost = 1
	}
	var ok bool
	if ttl > 0 {
		ok = s.cache.SetWithTTL(key, entry, cost, ttl)
	} else {
		ok = s.cache.Set(key, entry, cost)
	}
	if ok {
		s.cache.Wait()
		s.meta[key] = nextMeta
		if entry.Version.Compare(s.lastVersion) > 0 {
			s.lastVersion = entry.Version
		}
		s.cleanupSomeExpiredLocked(now)
	}
	return ok
}

func (s *Store) Del(key string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.cache.Del(key)
	delete(s.meta, key)
}

func (s *Store) Close() {
	s.cache.Close()
}

func (s *Store) nextVersionLocked(now time.Time) version.Version {
	s.mu.Lock()
	defer s.mu.Unlock()
	next := s.lastVersion.Next(now, s.nodeID)
	s.lastVersion = next
	return next
}

func cloneBytes(value []byte) []byte {
	if value == nil {
		return nil
	}
	out := make([]byte, len(value))
	copy(out, value)
	return out
}

func isOlder(next, current Entry) bool {
	if next.Version.Compare(current.Version) < 0 {
		return true
	}
	if next.Version.Compare(current.Version) > 0 {
		return false
	}
	return current.Tombstone && !next.Tombstone
}

func metaIsOlder(next, current entryMeta) bool {
	if next.version.Compare(current.version) < 0 {
		return true
	}
	if next.version.Compare(current.version) > 0 {
		return false
	}
	return current.tombstone && !next.tombstone
}

func expiresAtForTTL(now time.Time, ttl time.Duration) time.Time {
	if ttl <= 0 {
		return time.Time{}
	}
	return now.Add(ttl)
}

func (s *Store) expireKeyLocked(key string, now time.Time) bool {
	meta, ok := s.meta[key]
	if !ok || meta.expiresAt.IsZero() || meta.expiresAt.After(now) {
		return false
	}
	delete(s.meta, key)
	s.cache.Del(key)
	return true
}

func (s *Store) cleanupSomeExpiredLocked(now time.Time) {
	scanned := 0
	for key := range s.meta {
		if scanned >= metadataCleanupScanLimit {
			return
		}
		scanned++
		s.expireKeyLocked(key, now)
	}
}
