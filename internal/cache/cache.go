package cache

import (
	"github.com/dgraph-io/ristretto/v2"
	"log"
	"sync"
)

type Cache = ristretto.Cache[string, any]

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
