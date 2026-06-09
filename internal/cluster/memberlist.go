package cluster

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hashicorp/memberlist"

	"github.com/infamousity/distributed-cache/internal/config"
	"github.com/infamousity/distributed-cache/internal/log"
)

type Cluster struct {
	memberlist  *memberlist.Memberlist
	ring        Node
	localName   string
	events      *eventDelegate
	diagnostics *GossipDiagnostics
}

type GossipDiagnostics struct {
	messageTotal     atomic.Uint64
	degradedTotal    atomic.Uint64
	lastMessage      atomic.Value
	lastDegraded     atomic.Value
	lastDegradedUnix atomic.Int64
}

type GossipDiagnosticsSnapshot struct {
	MessageTotal     uint64
	DegradedTotal    uint64
	LastMessage      string
	LastDegraded     string
	LastDegradedTime time.Time
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

	if cfg.Common.Cache.SharedKey != "" {
		sum := sha256.Sum256([]byte(cfg.Common.Cache.SharedKey))
		mlc.SecretKey = sum[:]
	}

	diagnostics := &GossipDiagnostics{}
	mlc.LogOutput = newGossipLogWriter(l, diagnostics)

	nodeRing := NewHashRing(mlc.Name, cfg)
	events := newEventDelegate(l, nodeRing, cfg)
	mlc.Events = events
	mlc.Delegate = newMetaDelegate(l, nodeRing, cfg)

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

	return &Cluster{
		memberlist:  ml,
		ring:        nodeRing,
		localName:   ml.LocalNode().Name,
		events:      events,
		diagnostics: diagnostics,
	}, nil
}

func (c *Cluster) LocalNodeName() string {
	return c.localName
}

func (c *Cluster) GetNode() Node {
	return c.ring
}

func (c *Cluster) GossipDiagnostics() GossipDiagnosticsSnapshot {
	if c == nil || c.diagnostics == nil {
		return GossipDiagnosticsSnapshot{}
	}
	return c.diagnostics.Snapshot()
}

func (c *Cluster) JoinSeeds(seeds []string) (int, error) {
	if c == nil || c.memberlist == nil || len(seeds) == 0 {
		return 0, nil
	}
	nodes := make([]string, 0, len(seeds))
	for _, seed := range seeds {
		if seed == "" || strings.HasPrefix(seed, c.localName) {
			continue
		}
		nodes = append(nodes, seed)
	}
	if len(nodes) == 0 {
		return 0, nil
	}
	return c.memberlist.Join(nodes)
}

type PeerEventHandler interface {
	PeerJoined(name, controlAddr string)
	PeerLeft(name, controlAddr string)
	PeerUpdated(name, oldControlAddr, controlAddr string)
}

func (c *Cluster) SetPeerEventHandler(handler PeerEventHandler) {
	if c == nil || c.events == nil {
		return
	}
	c.events.SetHandler(handler)
}

func (c *Cluster) Shutdown() error {
	if c.memberlist == nil {
		return nil
	}
	_ = c.memberlist.Leave(0)
	return c.memberlist.Shutdown()
}

func (d *GossipDiagnostics) Snapshot() GossipDiagnosticsSnapshot {
	if d == nil {
		return GossipDiagnosticsSnapshot{}
	}
	snapshot := GossipDiagnosticsSnapshot{
		MessageTotal:  d.messageTotal.Load(),
		DegradedTotal: d.degradedTotal.Load(),
	}
	if v, ok := d.lastMessage.Load().(string); ok {
		snapshot.LastMessage = v
	}
	if v, ok := d.lastDegraded.Load().(string); ok {
		snapshot.LastDegraded = v
	}
	if unix := d.lastDegradedUnix.Load(); unix > 0 {
		snapshot.LastDegradedTime = time.Unix(0, unix)
	}
	return snapshot
}

func (d *GossipDiagnostics) observe(message string) {
	if d == nil {
		return
	}
	message = strings.TrimSpace(strings.ReplaceAll(message, "\n", " "))
	if message == "" {
		return
	}
	d.messageTotal.Add(1)
	d.lastMessage.Store(message)
	if isDegradedGossipMessage(message) {
		d.degradedTotal.Add(1)
		d.lastDegraded.Store(message)
		d.lastDegradedUnix.Store(time.Now().UnixNano())
	}
}

func isDegradedGossipMessage(message string) bool {
	msg := strings.ToLower(message)
	return strings.Contains(msg, "[err]") ||
		strings.Contains(msg, "failed") ||
		strings.Contains(msg, "timeout") ||
		strings.Contains(msg, "operation not permitted") ||
		strings.Contains(msg, "sendto") ||
		strings.Contains(msg, "connection refused") ||
		strings.Contains(msg, "no route to host") ||
		strings.Contains(msg, "network is unreachable")
}

type gossipLogWriter struct {
	logger     log.Interface
	diagnostic *GossipDiagnostics
}

func newGossipLogWriter(logger log.Interface, diagnostic *GossipDiagnostics) *gossipLogWriter {
	return &gossipLogWriter{logger: logger, diagnostic: diagnostic}
}

func (w *gossipLogWriter) Write(p []byte) (int, error) {
	message := strings.TrimSpace(strings.ReplaceAll(string(p), "\n", " "))
	w.diagnostic.observe(message)
	if w.logger != nil {
		_, _ = log.Writer(w.logger).Write(p)
	}
	return len(p), nil
}

type metaDelegate struct {
	mu   sync.Mutex
	meta []byte
	l    log.Interface
	ring Node
}

func newMetaDelegate(l log.Interface, ring Node, cfg *config.Config) *metaDelegate {
	if cfg.Common.Cache.Control.AdvertiseAddr != "" {
		return &metaDelegate{
			meta: []byte(cfg.Common.Cache.Control.AdvertiseAddr),
			l:    l,
			ring: ring,
		}
	}
	host := cfg.Common.Cache.Control.BindAddr
	if host == "" || host == "0.0.0.0" {
		if cfg.Common.Cache.Cluster.MemberList.AdvertiseAddr != "" {
			host = cfg.Common.Cache.Cluster.MemberList.AdvertiseAddr
		} else {
			host = cfg.Common.Cache.Cluster.MemberList.BindAddr
		}
	}
	meta := fmt.Sprintf("%s:%d", host, cfg.Common.Cache.Control.BindPort)
	return &metaDelegate{
		meta: []byte(meta),
		l:    l,
		ring: ring,
	}
}

func (d *metaDelegate) NodeMeta(limit int) []byte {
	if len(d.meta) > limit {
		return d.meta[:limit]
	}
	return d.meta
}

func (d *metaDelegate) NotifyMsg(_ []byte) {}

func (d *metaDelegate) GetBroadcasts(_ int, _ int) [][]byte {
	return nil
}

func (d *metaDelegate) LocalState(_ bool) []byte {
	return nil
}

func (d *metaDelegate) MergeRemoteState(_ []byte, _ bool) {}

type eventDelegate struct {
	mu      sync.RWMutex
	logger  log.Interface
	ring    Node
	cfg     *config.Config
	handler PeerEventHandler
}

func newEventDelegate(logger log.Interface, ring Node, cfg *config.Config) *eventDelegate {
	return &eventDelegate{logger: logger, ring: ring, cfg: cfg}
}

func (e *eventDelegate) NotifyJoin(node *memberlist.Node) {
	controlAddr := e.controlAddrForNode(node)
	e.ring.Add(node.Name, controlAddr)
	e.logger.Infof("[Memberlist] Node joined: %s (%s) [%s]", node.Name, node.Address(), strings.Join(e.ring.List(), ","))
	e.callHandler(func(handler PeerEventHandler) {
		handler.PeerJoined(node.Name, controlAddr)
	})
}

func (e *eventDelegate) NotifyLeave(node *memberlist.Node) {
	controlAddr := e.controlAddrForNode(node)
	e.logger.Infof("[Memberlist] Node left: %s (%s)", node.Name, node.Address())
	e.ring.Remove(node.Name)
	e.callHandler(func(handler PeerEventHandler) {
		handler.PeerLeft(node.Name, controlAddr)
	})
}

func (e *eventDelegate) NotifyUpdate(node *memberlist.Node) {
	oldControlAddr, _ := e.ring.GetForwardAddr(node.Name)
	controlAddr := e.controlAddrForNode(node)
	e.ring.Add(node.Name, controlAddr)
	e.logger.Infof("[Memberlist] Node updated: %s (%s)", node.Name, node.Address())
	e.callHandler(func(handler PeerEventHandler) {
		handler.PeerUpdated(node.Name, oldControlAddr, controlAddr)
	})
}

func (e *eventDelegate) SetHandler(handler PeerEventHandler) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.handler = handler
}

func (e *eventDelegate) callHandler(call func(PeerEventHandler)) {
	e.mu.RLock()
	handler := e.handler
	e.mu.RUnlock()
	if handler != nil {
		call(handler)
	}
}

func (e *eventDelegate) controlAddrForNode(node *memberlist.Node) string {
	meta := node.Meta
	if idx := bytes.IndexByte(meta, 0); idx >= 0 {
		meta = meta[:idx]
	}
	controlAddr := strings.TrimSpace(string(meta))
	parsedFromMeta := controlAddr != ""
	if controlAddr != "" {
		if host, port, err := net.SplitHostPort(controlAddr); err == nil && host != "" && port != "" {
			controlAddr = net.JoinHostPort(host, port)
		} else if _, err := fmt.Sscanf(controlAddr, "%d", new(int)); err == nil {
			host, _, err := net.SplitHostPort(node.Address())
			if err != nil {
				host = node.Address()
			}
			controlAddr = net.JoinHostPort(host, controlAddr)
		} else {
			controlAddr = ""
		}
	}
	if controlAddr == "" {
		host, _, err := net.SplitHostPort(node.Address())
		if err != nil {
			host = node.Address()
		}
		controlAddr = net.JoinHostPort(host, fmt.Sprintf("%d", e.cfg.Common.Cache.Control.BindPort))
		e.logger.Warnf("memberlist node meta missing/invalid; using fallback control addr %s for %s (meta=%q)", controlAddr, node.Name, string(meta))
	} else if !parsedFromMeta {
		e.logger.Warnf("memberlist node meta empty; using control addr %s for %s", controlAddr, node.Name)
	}
	return controlAddr
}
