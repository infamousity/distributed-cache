package cluster

import (
	"bytes"
	"crypto/sha256"
	"encoding/gob"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"

	"github.com/infamousity/distributed-cache/internal/cache"

	"github.com/hashicorp/memberlist"

	"github.com/infamousity/distributed-cache/internal/config"
	"github.com/infamousity/distributed-cache/internal/log"
)

var (
	ErrOneNodeInCluster = errors.New("only one node in cluster")
)

type Cluster struct {
	memberlist *memberlist.Memberlist
	ring       Node
	cache      *cache.Cache
	localName  string
	keyFile    string
	certFile   string
	sharedKey  []byte
	SharedKey  string
}

func NewCluster(cfg *config.Config) (*Cluster, error) {
	l := log.Default()
	c := cfg.Common.Cache.Cluster.MemberList

	mlc := memberlist.DefaultLANConfig()
	mlc.Name = c.NodeName

	// BindAddr and BindPort are required.
	mlc.BindAddr = c.BindAddr
	mlc.BindPort = c.BindPort

	// AdvertiseAddr and AdvertisePort are optional and default to BindAddr and BindPort.
	mlc.AdvertiseAddr = c.AdvertiseAddr
	if mlc.AdvertiseAddr == "" {
		mlc.AdvertiseAddr = c.BindAddr
	}
	mlc.AdvertisePort = c.AdvertisePort
	if mlc.AdvertisePort == 0 {
		mlc.AdvertisePort = c.BindPort
	}
	if len(c.BindAddrFilter) > 0 {
	}
	mlc.LogOutput = log.Writer(l)

	// create nodeRing
	nodeRing := NewHashRing(mlc.Name, cfg)
	mlc.Events = newEventDelegate(l, nodeRing)
	localCache := cache.New()
	mlc.Delegate = newDelegate(l, nodeRing, localCache, cfg)

	ml, err := memberlist.Create(mlc)
	if err != nil {
		return nil, fmt.Errorf("memberlist.Create: %w", err)
	}
	l.Info("Memberlist created attempting to join seeds...")
	if len(c.SeedNodes) > 0 {
		nodes := make([]string, 0)
		for _, seed := range c.SeedNodes {
			if !strings.HasPrefix(seed, c.NodeName) {
				nodes = append(nodes, seed)
			}
		}
		if _, err = ml.Join(nodes); err != nil {
			l.Warnf("failed to join seeds: %v", err)
		} else {
			l.Infof("joined seeds: %v", nodes)
		}
	}
	var keyFile, certFile string
	if cfg.Common.Cache.Cluster.Tls.Enabled {
		keyFile = fmt.Sprintf("/etc/ssl/private/%s.key", nodeRing.GetSelf())
		if !canRead(keyFile) {
			l.Warnf("key file '%s' is not readable, disabling TLS", keyFile)
			keyFile = ""
		}
		if len(keyFile) > 0 {
			certFile = fmt.Sprintf("/etc/ssl/certs/%s_server.crt", nodeRing.GetSelf())
			if !canRead(certFile) {
				l.Warnf("cert file '%s' is not readable, disabling TLS", certFile)
				certFile = ""
			}
		} else {
			l.Trace("key file is empty, cert file does not matter")
		}
	}
	var (
		sharedKey       []byte
		hashedSharedKey string
	)
	if len(cfg.Common.Cache.SharedKey) > 0 {
		sharedKey = []byte(cfg.Common.Cache.SharedKey)
		hashedSharedKeyBytes := sha256.Sum256(sharedKey)
		hashedSharedKey = fmt.Sprintf("%x", hashedSharedKeyBytes)

	}
	return &Cluster{
		memberlist: ml,
		ring:       nodeRing,
		cache:      localCache,
		localName:  ml.LocalNode().Name,
		keyFile:    keyFile,
		certFile:   certFile,
		sharedKey:  sharedKey,
		SharedKey:  hashedSharedKey,
	}, nil
}

func canRead(filePath string) bool {
	file, err := os.Open(filePath)
	if err != nil {
		log.Default().Tracef("File '%s' is not readable: %v", filePath, err)
		return false
	}
	defer func() {
		_ = file.Close()
	}()

	return true
}

func (c *Cluster) LocalNodeName() string {
	return c.localName
}

func (c *Cluster) GetNode() Node {
	return c.ring
}

func (c *Cluster) LocalCache() *cache.Cache {
	return c.cache
}

func (c *Cluster) Rebalance() error {
	l := log.Default()
	members := c.ring.List()
	if len(members) == 1 {
		l.Info("Only one node in cluster, skipping rebalance")
		return ErrOneNodeInCluster
	}
	var allItems []*cache.Item[any]
	for _, member := range members {
		if member == c.ring.GetSelf() {
			continue
		}
		addr, ok := c.ring.GetForwardAddr(member)
		if !ok {
			continue
		}
		scheme := "http"
		if c.IsTLS() {
			scheme = "https"
		}

		url := fmt.Sprintf("%s://%s/export", scheme, addr)
		resp, err := http.Get(url)
		if err != nil {
			l.Errorf("failed to get items from %s: %v", addr, err)
			continue
		}
		var items []*cache.Item[any]
		if err = gob.NewDecoder(resp.Body).Decode(&items); err != nil {
			continue
		}
		allItems = append(allItems, items...)
		_ = resp.Body.Close()
	}
	l.Infof("Got %d items from other nodes", len(allItems))
	deduped := make(map[uint64]*cache.Item[any], len(allItems))
	ddcount := 0
	for _, it := range allItems {
		prev, seen := deduped[it.Key]
		if !seen || it.Expiration.After(prev.Expiration) {
			deduped[it.Key] = it
		} else {
			ddcount++
		}
	}
	l.Infof("Deduped %d items from other nodes", ddcount)
	// convert back to slice
	merged := make([]*cache.Item[any], 0, len(deduped))
	for _, it := range deduped {
		merged = append(merged, it)
	}
	var myItems []*cache.Item[any]
	for _, it := range merged {
		owner, _ := c.ring.GetFromHash(it.Key)
		if owner == c.ring.GetSelf() {
			myItems = append(myItems, it)
		}
	}
	buf := new(bytes.Buffer)
	if err := gob.NewEncoder(buf).Encode(myItems); err != nil {
		return fmt.Errorf("failed to re-encode filtered items: %w", err)
	}

	// UnmarshalBinary will clear the cache and then insert exactly those items.
	if err := c.cache.UnmarshalBinary(buf.Bytes()); err != nil {
		return fmt.Errorf("failed to import items via UnmarshalBinary: %w", err)
	}

	return nil
}

func (c *Cluster) Trim(data []byte) error {
	buf := bytes.NewBuffer(data)
	var items []*cache.Item[any]
	if err := gob.NewDecoder(buf).Decode(&items); err != nil {
		return err
	}
	var myItems []*cache.Item[any]
	for _, it := range items {
		owner, _ := c.ring.GetFromHash(it.Key)
		if owner == c.ring.GetSelf() {
			myItems = append(myItems, it)
		}
	}
	nbuf := new(bytes.Buffer)
	if err := gob.NewEncoder(nbuf).Encode(myItems); err != nil {
		return fmt.Errorf("failed to re-encode filtered items: %w", err)
	}

	// UnmarshalBinary will clear the cache and then insert exactly those items.
	if err := c.cache.UnmarshalBinary(nbuf.Bytes()); err != nil {
		return fmt.Errorf("failed to import items via UnmarshalBinary: %w", err)
	}

	return nil
}

func (c *Cluster) IsTLS() bool {
	if len(c.keyFile) == 0 || len(c.certFile) == 0 {
		return false
	}

	return canRead(c.keyFile) && canRead(c.certFile)
}

func (c *Cluster) KeyFile() string {
	return c.keyFile
}

func (c *Cluster) CertFile() string {
	return c.certFile
}

func (c *Cluster) VerifySharedKey(hashedKey string) bool {
	if len(c.sharedKey) == 0 {
		return true
	}
	shaSum := sha256.Sum256(c.sharedKey)

	return fmt.Sprintf("%x", shaSum) == hashedKey
}

type delegate struct {
	mu         sync.Mutex
	meta       []byte
	broadcasts [][]byte
	state      []byte
	localCache *cache.Cache

	ring Node
	l    log.Interface
}

func newDelegate(l log.Interface, ring Node, localCache *cache.Cache, cfg *config.Config) *delegate {
	return &delegate{
		meta:       []byte(fmt.Sprintf("%s:%d", cfg.Common.Cache.Cluster.MemberList.BindAddr, cfg.Common.Cache.Http.BindPort)),
		broadcasts: make([][]byte, 0),
		state:      make([]byte, 0),
		localCache: localCache,
		l:          l,
		ring:       ring,
	}
}

func (d *delegate) NodeMeta(limit int) []byte {
	d.l.Infof("NodeMeta: [%s]: (%d) %s", d.ring.GetSelf(), limit, string(d.meta))
	return d.meta
}

func (d *delegate) NotifyMsg(bytes []byte) {
	d.l.Infof("NotifyMsg: [%s]: %s", d.ring.GetSelf(), string(bytes))
}

func (d *delegate) GetBroadcasts(overhead, limit int) [][]byte {
	//d.l.Infof("GetBroadcasts: [%s]: overhead: [%d] limit: [%d]", d.ring.GetSelf(), overhead, limit)
	return d.broadcasts
}

func (d *delegate) LocalState(join bool) []byte {
	if join {
		d.l.Infof("LocalState: [%s]: join: [%v] members: [%s]", d.ring.GetSelf(), join, strings.Join(d.ring.List(), ","))
	}
	return d.state
}

func (d *delegate) MergeRemoteState(buf []byte, join bool) {
	if buf == nil {
		d.l.Infof("MergeRemoteState: [%s]: buf [nil] join: [%v]", d.ring.GetSelf(), join)
	} else {
		d.l.Infof("MergeRemoteState: [%s]: buf [%v] join: [%v]", d.ring.GetSelf(), buf, join)
	}
}

type eventDelegate struct {
	logger log.Interface
	ring   Node
}

func newEventDelegate(logger log.Interface, ring Node) *eventDelegate {
	return &eventDelegate{logger: logger, ring: ring}
}

func (e *eventDelegate) NotifyJoin(node *memberlist.Node) {
	httpAddr, _, err := net.SplitHostPort(node.Address())
	if err != nil {
		e.logger.Errorf("failed to parse node address: %v", err)
		return
	}
	httpAddr = net.JoinHostPort(httpAddr, fmt.Sprintf("%d", e.ring.GetConfig().Common.Cache.Http.BindPort))
	e.ring.Add(node.Name, httpAddr)
	e.logger.Infof("[Memberlist] Node joined: %s (%s) [%s]", node.Name, node.Address(), strings.Join(e.ring.List(), ","))
}

func (e *eventDelegate) NotifyLeave(node *memberlist.Node) {
	e.logger.Infof("[Memberlist] Node left: %s (%s)", node.Name, node.Address())
	e.ring.Remove(node.Name)
}

func (e *eventDelegate) NotifyUpdate(node *memberlist.Node) {
	e.logger.Infof("[Memberlist] Node updated: %s (%s)", node.Name, node.Address())
}
