package cluster

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"net"
	"strings"
	"sync"

	"github.com/hashicorp/memberlist"

	"github.com/infamousity/distributed-cache/internal/config"
	"github.com/infamousity/distributed-cache/internal/log"
)

type Cluster struct {
	memberlist *memberlist.Memberlist
	ring       Node
	localName  string
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

	mlc.LogOutput = log.Writer(l)

	nodeRing := NewHashRing(mlc.Name, cfg)
	mlc.Events = newEventDelegate(l, nodeRing, cfg)
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
		memberlist: ml,
		ring:       nodeRing,
		localName:  ml.LocalNode().Name,
	}, nil
}

func (c *Cluster) LocalNodeName() string {
	return c.localName
}

func (c *Cluster) GetNode() Node {
	return c.ring
}

func (c *Cluster) Shutdown() error {
	if c.memberlist == nil {
		return nil
	}
	_ = c.memberlist.Leave(0)
	return c.memberlist.Shutdown()
}

type metaDelegate struct {
	mu   sync.Mutex
	meta []byte
	l    log.Interface
	ring Node
}

func newMetaDelegate(l log.Interface, ring Node, cfg *config.Config) *metaDelegate {
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
	logger log.Interface
	ring   Node
	cfg    *config.Config
}

func newEventDelegate(logger log.Interface, ring Node, cfg *config.Config) *eventDelegate {
	return &eventDelegate{logger: logger, ring: ring, cfg: cfg}
}

func (e *eventDelegate) NotifyJoin(node *memberlist.Node) {
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
	e.ring.Add(node.Name, controlAddr)
	e.logger.Infof("[Memberlist] Node joined: %s (%s) [%s]", node.Name, node.Address(), strings.Join(e.ring.List(), ","))
}

func (e *eventDelegate) NotifyLeave(node *memberlist.Node) {
	e.logger.Infof("[Memberlist] Node left: %s (%s)", node.Name, node.Address())
	e.ring.Remove(node.Name)
}

func (e *eventDelegate) NotifyUpdate(node *memberlist.Node) {
	e.logger.Infof("[Memberlist] Node updated: %s (%s)", node.Name, node.Address())
}
