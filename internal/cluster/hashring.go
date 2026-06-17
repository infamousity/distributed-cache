package cluster

import (
	"fmt"
	"sync"

	"github.com/dgraph-io/ristretto/v2/z"

	"github.com/buraksezer/consistent"

	"github.com/infamousity/distributed-cache/config"
)

// Node is the interface your server and cluster code uses to add/remove members
// and look up who owns a given key (returning the control-plane address for forwarding).
type Node interface {
	Add(name, controlAddr string)              // register a new node
	Remove(name string)                        // unregister a node by name
	Get(key string) (string, bool)             // owner of key, returns Name
	GetFromHash(hash uint64) (string, bool)    // hash of key, returns owner of key hash Name
	GetN(key string, n int) ([]string, bool)   // top-n owners
	GetForwardAddr(name string) (string, bool) // for the node's Name, find the control-plane address
	List() []string                            // list of all nodes in the ring
	LoadDistribution() map[string]float64      // exposes LoadDistribution
	GetSelf() string                           // the ring’s “self” name (hostname)
	GetConfig() *config.Config                 // the memberlist configuration
}

// ringMember holds exactly the control-plane address we want to forward to.
type ringMember struct {
	Name        string
	ControlAddr string
}

// String identifies the ring member by stable node name.
func (m ringMember) String() string {
	return m.Name
}

// hasher implements consistent.Hasher via xxhash
type hasher struct{}

func (h hasher) Sum64(data []byte) uint64 {
	keyHash, _ := z.KeyToHash(string(data))
	return keyHash
}

type ring struct {
	mu   sync.RWMutex
	hash *consistent.Consistent
	self string
	cfg  *config.Config
}

// NewHashRing creates a new empty ring.  Pass in your node‐name so GetSelf works.
func NewHashRing(selfName string, c *config.Config) Node {
	if c.Common.Cache.Cluster.MemberList.PartitionCount == 0 {
		c.Common.Cache.Cluster.MemberList.PartitionCount = consistent.DefaultPartitionCount
	}
	replicationFactor := c.Common.Cache.Cluster.MemberList.ReplicationFactor
	if replicationFactor < 2 {
		// consistent panics with replicationFactor=1 when there is only a single member.
		// We clamp to 2 to keep single-node startup safe.
		replicationFactor = 2
	}
	cfg := consistent.Config{
		PartitionCount:    c.Common.Cache.Cluster.MemberList.PartitionCount,
		ReplicationFactor: replicationFactor,
		Load:              1.25,
		Hasher:            hasher{},
	}
	controlAddr := c.Common.Cache.Control.AdvertiseAddr
	if controlAddr == "" {
		controlHost := c.Common.Cache.Control.BindAddr
		if controlHost == "" || controlHost == "0.0.0.0" {
			controlHost = c.Common.Cache.Cluster.MemberList.AdvertiseAddr
		}
		if controlHost == "" {
			controlHost = c.Common.Cache.Cluster.MemberList.BindAddr
		}
		controlAddr = fmt.Sprintf("%s:%d", controlHost, c.Common.Cache.Control.BindPort)
	}
	r := &ring{
		hash: consistent.New([]consistent.Member{
			ringMember{Name: selfName, ControlAddr: controlAddr},
		}, cfg),
		self: selfName,
		cfg:  c,
	}

	return r
}

func (r *ring) Add(name, controlAddr string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	// remove any existing with same name
	for _, m := range r.hash.GetMembers() {
		if rm := m.(ringMember); rm.Name == name {
			r.hash.Remove(rm.Name)
			break
		}
	}
	r.hash.Add(ringMember{Name: name, ControlAddr: controlAddr})
}

func (r *ring) Remove(name string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	for _, m := range r.hash.GetMembers() {
		if rm := m.(ringMember); rm.Name == name {
			r.hash.Remove(rm.Name)
			return
		}
	}
}

func (r *ring) Get(key string) (string, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	m := r.hash.LocateKey([]byte(key))
	if m == nil {
		return "", false
	}
	return m.(ringMember).Name, true
}

func (r *ring) GetFromHash(hash uint64) (string, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	m := r.hash.GetPartitionOwner(int(hash % uint64(r.cfg.Common.Cache.Cluster.MemberList.PartitionCount)))
	if m == nil {
		return "", false
	}

	return m.(ringMember).Name, true
}

func (r *ring) GetForwardAddr(name string) (string, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	for _, m := range r.hash.GetMembers() {
		if rm := m.(ringMember); rm.Name == name {
			return rm.ControlAddr, true
		}
	}

	return "", false
}

func (r *ring) GetN(key string, n int) ([]string, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	members, err := r.hash.GetClosestN([]byte(key), n)
	if err != nil {
		return nil, false
	}
	out := make([]string, 0, len(members))
	for _, m := range members {
		out = append(out, m.(ringMember).Name)
	}
	return out, true
}

func (r *ring) List() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	members := r.hash.GetMembers()
	out := make([]string, 0, len(members))
	for _, m := range members {
		out = append(out, m.(ringMember).Name)
	}
	return out
}

func (r *ring) LoadDistribution() map[string]float64 {
	r.mu.RLock()
	defer r.mu.RUnlock()

	return r.hash.LoadDistribution()
}

func (r *ring) GetSelf() string {
	return r.self
}

func (r *ring) GetConfig() *config.Config {
	return r.cfg
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
