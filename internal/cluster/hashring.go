package cluster

import (
	"fmt"
	"github.com/buraksezer/consistent"
	"github.com/cespare/xxhash/v2"
	"github.com/infamousity/distributed-cache/internal/config"
	"sync"
)

// Node is the interface your server and cluster code uses to add/remove members
// and look up who owns a given key (returning the HTTPAddr for forwarding).
type Node interface {
	Add(name, httpAddr string)                 // register a new node
	Remove(name string)                        // unregister a node by name
	Get(key string) (string, bool)             // owner of key, returns Name
	GetN(key string, n int) ([]string, bool)   // top-n owners
	GetForwardAddr(name string) (string, bool) // for the node's Name, find the forwarding address
	List() []string                            // list of all nodes in the ring
	GetSelf() string                           // the ring’s “self” name (hostname)
	GetConfig() *config.Config                 // the memberlist configuration
}

// ringMember holds exactly the HTTPAddr we want to forward to.
type ringMember struct {
	Name     string
	HTTPAddr string
}

// String() is used by consistent to identify a member; we return the HTTP Addr
func (m ringMember) String() string {
	return m.Name
}

// hasher implements consistent.Hasher via xxhash
type hasher struct{}

func (h hasher) Sum64(data []byte) uint64 {
	return xxhash.Sum64(data)
}

type ring struct {
	mu   sync.RWMutex
	hash *consistent.Consistent
	self string
	cfg  *config.Config
}

// NewHashRing creates a new empty ring.  Pass in your node‐name so GetSelf works.
func NewHashRing(selfName string, c *config.Config) Node {
	cfg := consistent.Config{
		PartitionCount:    271,
		ReplicationFactor: 3,
		Load:              1.25,
		Hasher:            hasher{},
	}
	httpAddr := fmt.Sprintf("%s:%d", c.Common.Cache.Doppio.BindAddr, c.Common.Cache.Doppio.BindPort)
	r := &ring{
		hash: consistent.New([]consistent.Member{
			ringMember{Name: selfName, HTTPAddr: httpAddr},
		}, cfg),
		self: selfName,
		cfg:  c,
	}
	// you can optionally seed yourself here, or let memberlist-driven code add you
	return r
}

func (r *ring) Add(name, httpAddr string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	// remove any existing with same name
	for _, m := range r.hash.GetMembers() {
		if rm := m.(ringMember); rm.Name == name {
			r.hash.Remove(rm.Name)
			break
		}
	}
	r.hash.Add(ringMember{Name: name, HTTPAddr: httpAddr})
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

func (r *ring) GetForwardAddr(name string) (string, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	for _, m := range r.hash.GetMembers() {
		if rm := m.(ringMember); rm.Name == name {
			return rm.HTTPAddr, true
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

func (r *ring) GetSelf() string {
	return r.self
}

func (r *ring) GetConfig() *config.Config {
	return r.cfg
}
