package cache

import (
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/dgraph-io/ristretto/v2"
	"github.com/dgraph-io/ristretto/v2/z"
)

type Cache = ristretto.Cache[string, any]

type Item[V any] ristretto.Item[any]

var (
	cacheInstance *Cache
	once          sync.Once
)

func New() *Cache {
	once.Do(func() {
		c, err := ristretto.NewCache[string, any](&ristretto.Config[string, any]{
			NumCounters: 1e7,
			MaxCost:     1 << 30,
			BufferItems: 64,
		})
		if err != nil {
			log.Fatalf("failed to create ristretto cache: %v", err)
		}
		cacheInstance = c
	})
	return cacheInstance
}

type ItemDTO[V any] struct {
	Key        string
	Value      V
	Cost       int64
	Expiration FlexibleTime
}

func (x *ItemDTO[V]) ToItem() *Item[V] {
	keyHash, cHash := z.KeyToHash(x.Key)
	eTime := time.Time{}
	if !x.Expiration.IsZero() {
		eTime = x.Expiration.Time
	}
	return &Item[V]{
		Key:        keyHash,
		Conflict:   cHash,
		Value:      x.Value,
		Cost:       x.Cost,
		Expiration: eTime,
	}
}

type FlexibleTime struct {
	time.Time
}

func (ft *FlexibleTime) UnmarshalJSON(data []byte) error {
	// Try to unmarshal as string (RFC3339)
	var s string
	if err := json.Unmarshal(data, &s); err == nil {
		parsed, perr := time.Parse(time.RFC3339, s)
		if perr != nil {
			return fmt.Errorf("invalid RFC3339 time: %w", perr)
		}
		ft.Time = parsed
		return nil
	}

	// Try to unmarshal as number (Unix timestamp)
	var ts int64
	if err := json.Unmarshal(data, &ts); err == nil {
		ft.Time = time.Unix(ts, 0)
		return nil
	}

	return fmt.Errorf("invalid time format: must be RFC3339 string or Unix timestamp")
}

func (ft FlexibleTime) MarshalJSON() ([]byte, error) {
	// Default to RFC3339 string for output
	return json.Marshal(ft.Time.Format(time.RFC3339))
}
