package cluster

import (
	"fmt"
	"github.com/hashicorp/memberlist"
	"github.com/infamousity/distributed-cache/internal/config"
	"github.com/infamousity/distributed-cache/internal/log"
	"net"
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

	mlc.BindAddr = c.BindAddr
	mlc.BindPort = c.BindPort
	mlc.AdvertiseAddr = c.AdvertiseAddr
	if mlc.AdvertiseAddr == "" {
		mlc.AdvertiseAddr = c.BindAddr
	}
	mlc.AdvertisePort = c.AdvertisePort
	if mlc.AdvertisePort == 0 {
		mlc.AdvertisePort = c.BindPort
	}
	mlc.LogOutput = log.Writer(l)

	// create nodeRing
	nodeRing := NewHashRing(mlc.Name, cfg)
	mlc.Events = newEventDelegate(l, nodeRing)

	ml, err := memberlist.Create(mlc)
	if err != nil {
		return nil, fmt.Errorf("memberlist.Create: %w", err)
	}
	if len(c.SeedNodes) > 0 {
		if _, err = ml.Join(c.SeedNodes); err != nil {
			l.Warnf("failed to join seeds: %v", err)
		} else {
			l.Infof("joined seeds: %v", c.SeedNodes)
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

type eventDelegate struct {
	logger log.Interface
	ring   Node
}

func newEventDelegate(logger log.Interface, ring Node) *eventDelegate {
	return &eventDelegate{logger: logger, ring: ring}
}

func (e *eventDelegate) NotifyJoin(node *memberlist.Node) {
	e.logger.Infof("[Memberlist] Node joined: %s (%s)", node.Name, node.Address())
	cfg := e.ring.GetConfig()
	httpAddr, _, err := net.SplitHostPort(node.Address())
	if err != nil {
		e.logger.Errorf("failed to parse node address: %v", err)
		return
	}
	httpPort := cfg.Common.Cache.Doppio.BindPort
	httpAddr = net.JoinHostPort(httpAddr, fmt.Sprintf("%d", httpPort))
	e.ring.Add(node.Name, httpAddr)
}

func (e *eventDelegate) NotifyLeave(node *memberlist.Node) {
	e.logger.Infof("[Memberlist] Node left: %s (%s)", node.Name, node.Address())
	e.ring.Remove(node.Name)
}

func (e *eventDelegate) NotifyUpdate(node *memberlist.Node) {
	e.logger.Infof("[Memberlist] Node updated: %s (%s)", node.Name, node.Address())
	// No-op for now
}
