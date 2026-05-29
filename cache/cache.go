package cache

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/infamousity/distributed-cache/internal/cache"
	"github.com/infamousity/distributed-cache/internal/cluster"
	"github.com/infamousity/distributed-cache/internal/config"
	"github.com/infamousity/distributed-cache/internal/control"
	"github.com/infamousity/distributed-cache/internal/log"
	"github.com/infamousity/distributed-cache/internal/metrics"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type DistributedCache struct {
	store   *cache.Store
	cluster *cluster.Cluster
	control *control.Server
	logger  log.Interface
	opts    Options

	clientMu  sync.Mutex
	clients   map[string]*control.Client
	clientTLS *tls.Config

	retryCh   chan retryTask
	retryStop chan struct{}
	retryWg   sync.WaitGroup

	repairStop chan struct{}
	repairWg   sync.WaitGroup

	keyMu sync.RWMutex
	keys  map[string]keyMeta

	versionMu   sync.Mutex
	lastVersion uint64

	metrics *metrics.Metrics

	peerWarnMu   sync.Mutex
	peerWarnLast map[string]time.Time

	closeOnce sync.Once
	closeErr  error
}

func Start(opts Options) (*DistributedCache, error) {
	opts = opts.withDefaults()
	cfg, err := configFromOptions(opts)
	if err != nil {
		return nil, err
	}
	return startFromConfig(cfg, opts)
}

func StartFromConfigFiles(paths ...string) (*DistributedCache, error) {
	cfg, err := config.Load(paths...)
	if err != nil {
		return nil, err
	}
	opts := optionsFromConfig(cfg).withDefaults()
	return startFromConfig(cfg, opts)
}

func (d *DistributedCache) Get(ctx context.Context, key string, opts ...Option) ([]byte, bool, error) {
	call := d.resolveOptions(opts)
	nsKey := d.namespacedKey(key, call)

	if d.metrics != nil {
		d.metrics.GetTotal.Inc()
	}

	owner, ok := d.cluster.GetNode().Get(nsKey)
	if !ok {
		return nil, false, fmt.Errorf("no nodes in ring")
	}
	if owner == d.cluster.GetNode().GetSelf() {
		entry, found := d.store.GetEntry(nsKey)
		if found && entry.Tombstone {
			d.recordMiss()
			return nil, false, nil
		}
		if d.metrics != nil {
			if found {
				d.metrics.HitTotal.Inc()
			} else {
				d.metrics.MissTotal.Inc()
			}
		}
		if !found {
			d.removeKey(nsKey)
		}
		return entry.Value, found, nil
	}

	addr, ok := d.cluster.GetNode().GetForwardAddr(owner)
	if !ok {
		return nil, false, fmt.Errorf("owner address not found")
	}
	if d.metrics != nil {
		d.metrics.ForwardTotal.Inc()
	}
	client, err := d.clientFor(addr)
	if err != nil {
		if d.metrics != nil {
			d.metrics.ForwardErrorTotal.Inc()
		}
		d.handlePeerError(addr, "get-forward", err)
		return nil, false, err
	}
	ctx, cancel := d.withTimeout(ctx)
	defer cancel()
	entry, found, err := client.Fetch(ctx, nsKey)
	if err == nil {
		if found && entry.Tombstone {
			d.recordMiss()
			return nil, false, nil
		}
		if d.metrics != nil {
			if found {
				d.metrics.HitTotal.Inc()
			} else {
				d.metrics.MissTotal.Inc()
			}
		}
		return entry.Value, found, nil
	}
	if d.metrics != nil {
		d.metrics.ForwardErrorTotal.Inc()
	}

	// Owner failed; try replicas for availability.
	members, ok := d.cluster.GetNode().GetN(nsKey, d.opts.ReplicationFactor)
	if !ok || len(members) == 0 {
		return nil, false, err
	}
	var best control.Entry
	var bestFound bool
	self := d.cluster.GetNode().GetSelf()
	for _, member := range members {
		if member == self || member == owner {
			continue
		}
		replicaAddr, ok := d.cluster.GetNode().GetForwardAddr(member)
		if !ok {
			continue
		}
		replicaClient, rerr := d.clientFor(replicaAddr)
		if rerr != nil {
			continue
		}
		rctx, cancel := d.withTimeout(ctx)
		entry, found, rerr = replicaClient.Fetch(rctx, nsKey)
		cancel()
		if rerr == nil && found && betterReplicaEntry(entry, best, bestFound) {
			best = entry
			bestFound = true
		}
	}
	if bestFound {
		if best.Tombstone {
			if ownerAddr, ok := d.cluster.GetNode().GetForwardAddr(owner); ok {
				d.enqueueRetry(retryTask{
					kind:     retryDelete,
					addr:     ownerAddr,
					key:      nsKey,
					version:  best.Version,
					attempts: 1,
				})
			}
			if d.metrics != nil {
				d.metrics.ReadRepairTotal.Inc()
			}
			d.recordMiss()
			return nil, false, nil
		}
		if d.metrics != nil {
			d.metrics.HitTotal.Inc()
			d.metrics.ReadRepairTotal.Inc()
		}
		if meta, ok := d.keyMeta(nsKey); ok {
			ttl := time.Until(meta.expiresAt)
			if meta.expiresAt.IsZero() || ttl < 0 {
				ttl = 0
			}
			if ownerAddr, ok := d.cluster.GetNode().GetForwardAddr(owner); ok {
				d.enqueueRetry(retryTask{
					kind:      retryStore,
					addr:      ownerAddr,
					key:       nsKey,
					value:     best.Value,
					ttl:       ttl,
					expiresAt: meta.expiresAt,
					version:   best.Version,
					attempts:  1,
				})
			}
		}
		return best.Value, true, nil
	}
	return nil, false, err
}

func (d *DistributedCache) Set(ctx context.Context, key string, value []byte, ttl time.Duration, opts ...SetOption) error {
	call := d.resolveOptions(opts)
	nsKey := d.namespacedKey(key, call)
	wc := d.writeConcern(call)

	owner, ok := d.cluster.GetNode().Get(nsKey)
	if !ok {
		return fmt.Errorf("no nodes in ring")
	}
	if owner != d.cluster.GetNode().GetSelf() {
		addr, ok := d.cluster.GetNode().GetForwardAddr(owner)
		if !ok {
			return fmt.Errorf("owner address not found")
		}
		if d.metrics != nil {
			d.metrics.ForwardTotal.Inc()
		}
		client, err := d.clientFor(addr)
		if err != nil {
			if d.metrics != nil {
				d.metrics.ForwardErrorTotal.Inc()
			}
			d.handlePeerError(addr, "set-forward", err)
			return err
		}
		ctx, cancel := d.withTimeout(ctx)
		defer cancel()
		return client.StoreVersioned(ctx, nsKey, value, ttl, 0, toControlWriteConcern(wc))
	}

	version := d.nextVersion()
	if err := d.storeLocalVersioned(nsKey, value, ttl, version); err != nil {
		return err
	}
	if wc == WriteConcernMajority {
		if err := d.replicateWithQuorum(ctx, nsKey, value, ttl, version); err != nil {
			if d.metrics != nil {
				d.metrics.WriteQuorumFailed.Inc()
			}
			return err
		}
		return nil
	}
	return d.replicate(ctx, nsKey, value, ttl, version)
}

func (d *DistributedCache) Del(ctx context.Context, key string, opts ...DelOption) error {
	call := d.resolveOptions(opts)
	nsKey := d.namespacedKey(key, call)
	wc := d.writeConcern(call)

	owner, ok := d.cluster.GetNode().Get(nsKey)
	if !ok {
		return fmt.Errorf("no nodes in ring")
	}
	if owner != d.cluster.GetNode().GetSelf() {
		addr, ok := d.cluster.GetNode().GetForwardAddr(owner)
		if !ok {
			return fmt.Errorf("owner address not found")
		}
		if d.metrics != nil {
			d.metrics.ForwardTotal.Inc()
		}
		client, err := d.clientFor(addr)
		if err != nil {
			if d.metrics != nil {
				d.metrics.ForwardErrorTotal.Inc()
			}
			d.handlePeerError(addr, "del-forward", err)
			return err
		}
		ctx, cancel := d.withTimeout(ctx)
		defer cancel()
		return client.DeleteVersioned(ctx, nsKey, 0, toControlWriteConcern(wc))
	}

	version := d.nextVersion()
	if err := d.deleteLocalVersioned(nsKey, version); err != nil {
		return err
	}
	if wc == WriteConcernMajority {
		if err := d.replicateDeleteWithQuorum(ctx, nsKey, version); err != nil {
			if d.metrics != nil {
				d.metrics.WriteQuorumFailed.Inc()
			}
			return err
		}
		return nil
	}
	return d.replicateDelete(ctx, nsKey, version)
}

func (d *DistributedCache) Close() error {
	d.closeOnce.Do(func() {
		d.stopRepairWorker()
		d.stopRetryWorker()
		if d.control != nil {
			d.control.Stop()
		}
		if d.cluster != nil {
			if err := d.cluster.Shutdown(); err != nil && d.closeErr == nil {
				d.closeErr = err
			}
		}
		if d.metrics != nil {
			if err := d.metrics.Stop(); err != nil && d.closeErr == nil {
				d.closeErr = err
			}
		}
		d.clientMu.Lock()
		for _, c := range d.clients {
			if err := c.Close(); err != nil && d.closeErr == nil {
				d.closeErr = err
			}
		}
		d.clientMu.Unlock()
		if d.store != nil {
			d.store.Close()
		}
	})
	return d.closeErr
}

func startFromConfig(cfg *config.Config, opts Options) (*DistributedCache, error) {
	logger := log.Default()

	if err := validateConfig(cfg, opts); err != nil {
		return nil, err
	}

	svc := &DistributedCache{
		logger:       logger,
		opts:         opts,
		clients:      make(map[string]*control.Client),
		clientTLS:    nil,
		retryCh:      make(chan retryTask, opts.ReplicationRetryQueueSize),
		retryStop:    make(chan struct{}),
		repairStop:   make(chan struct{}),
		keys:         make(map[string]keyMeta),
		peerWarnLast: make(map[string]time.Time),
	}

	store, err := cache.NewStore(int64(cfg.Common.Cache.SizeBytes))
	if err != nil {
		return nil, err
	}

	cl, err := cluster.NewCluster(cfg)
	if err != nil {
		return nil, err
	}

	serverTLS, clientTLS, err := tlsConfigs(opts.TLS)
	if err != nil {
		return nil, err
	}

	svc.store = store
	svc.cluster = cl
	svc.clientTLS = clientTLS
	svc.startRetryWorker()
	svc.startRepairWorker()

	m, err := metrics.New(cfg.Common.Cache.Metrics.BindAddr, cfg.Common.Cache.Metrics.BindPort)
	if err != nil {
		return nil, err
	}
	svc.metrics = m
	if svc.metrics != nil {
		_ = svc.metrics.Start()
	}

	controlServer, err := control.NewServer(svc, control.ServerOptions{
		BindAddr:  fmt.Sprintf("%s:%d", cfg.Common.Cache.Control.BindAddr, cfg.Common.Cache.Control.BindPort),
		SharedKey: cfg.Common.Cache.SharedKey,
		TLS:       serverTLS,
	})
	if err != nil {
		return nil, err
	}
	svc.control = controlServer
	controlServer.Start()
	logger.Infof("control plane listening on %s", controlServer.Addr())

	if opts.SelfCheck || opts.FailFast {
		if err := svc.selfCheckControl(); err != nil {
			if opts.FailFast {
				_ = svc.Close()
				return nil, err
			}
			logger.Warnf("control plane self-check failed: %v", err)
		}
	}

	logger.Infof("cache node started: %s", cl.LocalNodeName())
	return svc, nil
}

func (d *DistributedCache) NodeName() string {
	return d.cluster.LocalNodeName()
}

func (d *DistributedCache) Fetch(_ context.Context, key string) (control.Entry, bool, error) {
	entry, found := d.store.GetEntry(key)
	return control.Entry{
		Value:     entry.Value,
		Version:   entry.Version,
		Tombstone: entry.Tombstone,
	}, found, nil
}

func (d *DistributedCache) Store(ctx context.Context, key string, value []byte, ttl time.Duration, version uint64, wc control.WriteConcern) error {
	if version == 0 {
		version = d.nextVersion()
	}
	if err := d.storeLocalVersioned(key, value, ttl, version); err != nil {
		return err
	}
	switch wc {
	case control.WriteConcernReplica:
		return nil
	case control.WriteConcernMajority:
		return d.replicateWithQuorum(ctx, key, value, ttl, version)
	default:
		return d.replicate(ctx, key, value, ttl, version)
	}
}

func (d *DistributedCache) Delete(ctx context.Context, key string, version uint64, wc control.WriteConcern) error {
	if version == 0 {
		version = d.nextVersion()
	}
	if err := d.deleteLocalVersioned(key, version); err != nil {
		return err
	}
	switch wc {
	case control.WriteConcernReplica:
		return nil
	case control.WriteConcernMajority:
		return d.replicateDeleteWithQuorum(ctx, key, version)
	default:
		return d.replicateDelete(ctx, key, version)
	}
}

func (d *DistributedCache) storeLocal(key string, value []byte, ttl time.Duration) error {
	return d.storeLocalVersioned(key, value, ttl, d.nextVersion())
}

func (d *DistributedCache) storeLocalVersioned(key string, value []byte, ttl time.Duration, version uint64) error {
	if ok := d.store.SetVersioned(key, value, ttl, version); !ok {
		return fmt.Errorf("failed to store value")
	}
	if entry, ok := d.store.GetEntry(key); ok && entry.Version == version && !entry.Tombstone {
		d.addKey(key, ttl, version, false)
	}
	return nil
}

func (d *DistributedCache) deleteLocalVersioned(key string, version uint64) error {
	if ok := d.store.DeleteVersioned(key, version, d.opts.TombstoneTTL); !ok {
		return fmt.Errorf("failed to store tombstone")
	}
	if entry, ok := d.store.GetEntry(key); ok && entry.Version == version && entry.Tombstone {
		d.addKey(key, d.opts.TombstoneTTL, version, true)
	}
	return nil
}

func (d *DistributedCache) replicate(ctx context.Context, key string, value []byte, ttl time.Duration, version uint64) error {
	members, ok := d.cluster.GetNode().GetN(key, d.opts.ReplicationFactor)
	if !ok || len(members) == 0 {
		return nil
	}
	self := d.cluster.GetNode().GetSelf()
	expiresAt := expiresAtForTTL(ttl)
	for _, member := range members {
		if member == self {
			continue
		}
		addr, ok := d.cluster.GetNode().GetForwardAddr(member)
		if !ok {
			continue
		}
		client, err := d.clientFor(addr)
		if err != nil {
			d.handlePeerError(addr, "replicate", err)
			if d.metrics != nil {
				d.metrics.ReplicationError.Inc()
			}
			d.enqueueRetry(retryTask{
				kind:      retryStore,
				addr:      addr,
				key:       key,
				value:     value,
				ttl:       ttl,
				expiresAt: expiresAt,
				version:   version,
				attempts:  1,
			})
			continue
		}
		cctx, cancel := d.withTimeout(ctx)
		if err := client.StoreVersioned(cctx, key, value, ttl, version, control.WriteConcernReplica); err != nil {
			cancel()
			d.handlePeerError(addr, "replicate", err)
			if d.metrics != nil {
				d.metrics.ReplicationError.Inc()
			}
			d.enqueueRetry(retryTask{
				kind:      retryStore,
				addr:      addr,
				key:       key,
				value:     value,
				ttl:       ttl,
				expiresAt: expiresAt,
				version:   version,
				attempts:  1,
			})
		} else if d.metrics != nil {
			cancel()
			d.metrics.ReplicationSuccess.Inc()
		} else {
			cancel()
		}
	}
	return nil
}

func (d *DistributedCache) replicateDelete(ctx context.Context, key string, version uint64) error {
	members, ok := d.cluster.GetNode().GetN(key, d.opts.ReplicationFactor)
	if !ok || len(members) == 0 {
		return nil
	}
	self := d.cluster.GetNode().GetSelf()
	for _, member := range members {
		if member == self {
			continue
		}
		addr, ok := d.cluster.GetNode().GetForwardAddr(member)
		if !ok {
			continue
		}
		client, err := d.clientFor(addr)
		if err != nil {
			d.handlePeerError(addr, "replicate-delete", err)
			if d.metrics != nil {
				d.metrics.ReplicationError.Inc()
			}
			d.enqueueRetry(retryTask{
				kind:     retryDelete,
				addr:     addr,
				key:      key,
				version:  version,
				attempts: 1,
			})
			continue
		}
		cctx, cancel := d.withTimeout(ctx)
		if err := client.DeleteVersioned(cctx, key, version, control.WriteConcernReplica); err != nil {
			cancel()
			d.handlePeerError(addr, "replicate-delete", err)
			if d.metrics != nil {
				d.metrics.ReplicationError.Inc()
			}
			d.enqueueRetry(retryTask{
				kind:     retryDelete,
				addr:     addr,
				key:      key,
				version:  version,
				attempts: 1,
			})
		} else if d.metrics != nil {
			cancel()
			d.metrics.ReplicationSuccess.Inc()
		} else {
			cancel()
		}
	}
	return nil
}

func (d *DistributedCache) replicateWithQuorum(ctx context.Context, key string, value []byte, ttl time.Duration, version uint64) error {
	members, ok := d.cluster.GetNode().GetN(key, d.opts.ReplicationFactor)
	if !ok || len(members) == 0 {
		return nil
	}
	quorum := len(members)/2 + 1
	acks := 1 // self
	if quorum <= 1 {
		return nil
	}
	self := d.cluster.GetNode().GetSelf()
	expiresAt := expiresAtForTTL(ttl)
	resultCh := make(chan bool, len(members))
	var wg sync.WaitGroup
	for _, member := range members {
		if member == self {
			continue
		}
		addr, ok := d.cluster.GetNode().GetForwardAddr(member)
		if !ok {
			continue
		}
		wg.Add(1)
		go func(addr string) {
			defer wg.Done()
			client, err := d.clientFor(addr)
			if err != nil {
				d.handlePeerError(addr, "replicate-quorum", err)
				d.enqueueRetry(retryTask{kind: retryStore, addr: addr, key: key, value: value, ttl: ttl, expiresAt: expiresAt, version: version, attempts: 1})
				if d.metrics != nil {
					d.metrics.ReplicationError.Inc()
				}
				return
			}
			cctx, cancel := d.withTimeout(ctx)
			if err := client.StoreVersioned(cctx, key, value, ttl, version, control.WriteConcernReplica); err != nil {
				cancel()
				d.handlePeerError(addr, "replicate-quorum", err)
				d.enqueueRetry(retryTask{kind: retryStore, addr: addr, key: key, value: value, ttl: ttl, expiresAt: expiresAt, version: version, attempts: 1})
				if d.metrics != nil {
					d.metrics.ReplicationError.Inc()
				}
				return
			}
			cancel()
			if d.metrics != nil {
				d.metrics.ReplicationSuccess.Inc()
			}
			resultCh <- true
		}(addr)
	}
	go func() {
		wg.Wait()
		close(resultCh)
	}()
	for range resultCh {
		acks++
		if acks >= quorum {
			return nil
		}
	}
	return fmt.Errorf("write quorum not reached: %d/%d", acks, quorum)
}

func (d *DistributedCache) replicateDeleteWithQuorum(ctx context.Context, key string, version uint64) error {
	members, ok := d.cluster.GetNode().GetN(key, d.opts.ReplicationFactor)
	if !ok || len(members) == 0 {
		return nil
	}
	quorum := len(members)/2 + 1
	acks := 1
	if quorum <= 1 {
		return nil
	}
	self := d.cluster.GetNode().GetSelf()
	resultCh := make(chan bool, len(members))
	var wg sync.WaitGroup
	for _, member := range members {
		if member == self {
			continue
		}
		addr, ok := d.cluster.GetNode().GetForwardAddr(member)
		if !ok {
			continue
		}
		wg.Add(1)
		go func(addr string) {
			defer wg.Done()
			client, err := d.clientFor(addr)
			if err != nil {
				d.handlePeerError(addr, "replicate-delete-quorum", err)
				d.enqueueRetry(retryTask{kind: retryDelete, addr: addr, key: key, version: version, attempts: 1})
				if d.metrics != nil {
					d.metrics.ReplicationError.Inc()
				}
				return
			}
			cctx, cancel := d.withTimeout(ctx)
			if err := client.DeleteVersioned(cctx, key, version, control.WriteConcernReplica); err != nil {
				cancel()
				d.handlePeerError(addr, "replicate-delete-quorum", err)
				d.enqueueRetry(retryTask{kind: retryDelete, addr: addr, key: key, version: version, attempts: 1})
				if d.metrics != nil {
					d.metrics.ReplicationError.Inc()
				}
				return
			}
			cancel()
			if d.metrics != nil {
				d.metrics.ReplicationSuccess.Inc()
			}
			resultCh <- true
		}(addr)
	}
	go func() {
		wg.Wait()
		close(resultCh)
	}()
	for range resultCh {
		acks++
		if acks >= quorum {
			return nil
		}
	}
	return fmt.Errorf("delete quorum not reached: %d/%d", acks, quorum)
}

func (d *DistributedCache) clientFor(addr string) (*control.Client, error) {
	d.clientMu.Lock()
	defer d.clientMu.Unlock()
	if c, ok := d.clients[addr]; ok {
		return c, nil
	}
	client, err := control.Dial(addr, control.ClientOptions{
		SharedKey:   d.opts.SharedKey,
		TLS:         d.clientTLS,
		DialTimeout: d.opts.ControlTimeout,
	})
	if err != nil {
		return nil, err
	}
	d.clients[addr] = client
	return client, nil
}

func (d *DistributedCache) withTimeout(ctx context.Context) (context.Context, context.CancelFunc) {
	if d.opts.ControlTimeout <= 0 {
		return ctx, func() {}
	}
	return context.WithTimeout(ctx, d.opts.ControlTimeout)
}

func (d *DistributedCache) resolveOptions(opts []Option) CallOptions {
	out := CallOptions{}
	for _, opt := range opts {
		opt(&out)
	}
	return out
}

func (d *DistributedCache) namespacedKey(key string, opts CallOptions) string {
	ns := d.opts.Namespace
	if opts.namespaceSet {
		ns = opts.namespace
	}
	if ns == "" {
		return key
	}
	return ns + ":" + key
}

func (d *DistributedCache) writeConcern(opts CallOptions) WriteConcern {
	if opts.writeConcernSet {
		return opts.writeConcern
	}
	return d.opts.WriteConcern
}

func (d *DistributedCache) recordMiss() {
	if d.metrics != nil {
		d.metrics.MissTotal.Inc()
	}
}

func (d *DistributedCache) nextVersion() uint64 {
	now := uint64(time.Now().UnixNano())
	d.versionMu.Lock()
	defer d.versionMu.Unlock()
	if now <= d.lastVersion {
		now = d.lastVersion + 1
	}
	d.lastVersion = now
	return now
}

func toControlWriteConcern(wc WriteConcern) control.WriteConcern {
	switch wc {
	case WriteConcernMajority:
		return control.WriteConcernMajority
	default:
		return control.WriteConcernOne
	}
}

func parseWriteConcern(v string) WriteConcern {
	switch strings.ToLower(strings.TrimSpace(v)) {
	case "majority", "quorum":
		return WriteConcernMajority
	default:
		return WriteConcernOne
	}
}

func writeConcernString(wc WriteConcern) string {
	switch wc {
	case WriteConcernMajority:
		return "majority"
	default:
		return "one"
	}
}

type peerErrKind int

const (
	peerErrOther peerErrKind = iota
	peerErrAuth
	peerErrTLS
	peerErrUnreachable
)

func (d *DistributedCache) handlePeerError(addr, op string, err error) {
	kind := classifyPeerError(err)
	if d.metrics != nil {
		switch kind {
		case peerErrAuth:
			d.metrics.PeerAuthFail.Inc()
		case peerErrTLS:
			d.metrics.PeerTLSFail.Inc()
		case peerErrUnreachable:
			d.metrics.PeerUnreachable.Inc()
		}
	}
	d.peerWarnMu.Lock()
	last := d.peerWarnLast[addr]
	if time.Since(last) < d.opts.PeerWarnInterval {
		d.peerWarnMu.Unlock()
		return
	}
	d.peerWarnLast[addr] = time.Now()
	d.peerWarnMu.Unlock()

	switch kind {
	case peerErrAuth:
		d.logger.Warnf("peer auth failure (%s) for %s: %v", op, addr, err)
	case peerErrTLS:
		d.logger.Warnf("peer tls failure (%s) for %s: %v", op, addr, err)
	case peerErrUnreachable:
		d.logger.Warnf("peer unreachable (%s) for %s: %v", op, addr, err)
	default:
		d.logger.Warnf("peer error (%s) for %s: %v", op, addr, err)
	}
}

func classifyPeerError(err error) peerErrKind {
	if err == nil {
		return peerErrOther
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return peerErrUnreachable
	}
	if s, ok := status.FromError(err); ok {
		switch s.Code() {
		case codes.Unauthenticated:
			return peerErrAuth
		case codes.Unavailable:
			msg := strings.ToLower(s.Message())
			if strings.Contains(msg, "authentication handshake failed") || strings.Contains(msg, "tls") || strings.Contains(msg, "x509") {
				return peerErrTLS
			}
			return peerErrUnreachable
		}
	}
	msg := strings.ToLower(err.Error())
	if strings.Contains(msg, "tls") || strings.Contains(msg, "x509") {
		return peerErrTLS
	}
	if strings.Contains(msg, "connection refused") || strings.Contains(msg, "no such host") {
		return peerErrUnreachable
	}
	return peerErrOther
}

type keyMeta struct {
	expiresAt time.Time
	version   uint64
	tombstone bool
}

func (d *DistributedCache) addKey(key string, ttl time.Duration, version uint64, tombstone bool) {
	var exp time.Time
	if ttl > 0 {
		exp = time.Now().Add(ttl)
	}
	d.keyMu.Lock()
	d.keys[key] = keyMeta{expiresAt: exp, version: version, tombstone: tombstone}
	d.keyMu.Unlock()
}

func (d *DistributedCache) removeKey(key string) {
	d.keyMu.Lock()
	delete(d.keys, key)
	d.keyMu.Unlock()
}

func (d *DistributedCache) snapshotKeys(max int) []string {
	d.keyMu.RLock()
	defer d.keyMu.RUnlock()
	if max <= 0 || max > len(d.keys) {
		max = len(d.keys)
	}
	out := make([]string, 0, max)
	for k := range d.keys {
		out = append(out, k)
		if len(out) >= max {
			break
		}
	}
	return out
}

func (d *DistributedCache) keyMeta(key string) (keyMeta, bool) {
	d.keyMu.RLock()
	defer d.keyMu.RUnlock()
	m, ok := d.keys[key]
	return m, ok
}

type retryKind int

const (
	retryStore retryKind = iota
	retryDelete
)

type retryTask struct {
	kind      retryKind
	addr      string
	key       string
	value     []byte
	ttl       time.Duration
	expiresAt time.Time
	version   uint64
	attempts  int
}

func (d *DistributedCache) startRetryWorker() {
	d.retryWg.Add(1)
	go func() {
		defer d.retryWg.Done()
		for {
			select {
			case task := <-d.retryCh:
				d.handleRetry(task)
			case <-d.retryStop:
				return
			}
		}
	}()
}

func (d *DistributedCache) stopRetryWorker() {
	close(d.retryStop)
	d.retryWg.Wait()
}

func (d *DistributedCache) startRepairWorker() {
	if d.opts.RepairInterval <= 0 {
		return
	}
	d.repairWg.Add(1)
	go func() {
		defer d.repairWg.Done()
		ticker := time.NewTicker(d.opts.RepairInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				d.runRepairCycle()
			case <-d.repairStop:
				return
			}
		}
	}()
}

func (d *DistributedCache) stopRepairWorker() {
	close(d.repairStop)
	d.repairWg.Wait()
}

func (d *DistributedCache) runRepairCycle() {
	keys := d.snapshotKeys(d.opts.RepairMaxKeysPerCycle)
	if len(keys) == 0 {
		return
	}
	self := d.cluster.GetNode().GetSelf()
	now := time.Now()
	for _, key := range keys {
		meta, ok := d.keyMeta(key)
		if ok && !meta.expiresAt.IsZero() && now.After(meta.expiresAt) {
			d.store.Del(key)
			d.removeKey(key)
			if d.metrics != nil {
				d.metrics.AntiEntropyTotal.Inc()
			}
			continue
		}
		entry, found := d.store.GetEntry(key)
		if !found {
			d.removeKey(key)
			continue
		}
		members, ok := d.cluster.GetNode().GetN(key, d.opts.ReplicationFactor)
		if !ok || len(members) == 0 {
			continue
		}
		owner, ok := d.cluster.GetNode().Get(key)
		if !ok {
			continue
		}
		inOwners := false
		for _, m := range members {
			if m == self {
				inOwners = true
				break
			}
		}
		if !inOwners {
			d.store.Del(key)
			d.removeKey(key)
			if d.metrics != nil {
				d.metrics.AntiEntropyTotal.Inc()
			}
			continue
		}

		ttl := time.Duration(0)
		if !meta.expiresAt.IsZero() {
			if meta.expiresAt.After(now) {
				ttl = time.Until(meta.expiresAt)
			} else {
				d.store.Del(key)
				d.removeKey(key)
				if d.metrics != nil {
					d.metrics.AntiEntropyTotal.Inc()
				}
				continue
			}
		}

		if owner == self {
			for _, member := range members {
				if member == self {
					continue
				}
				addr, ok := d.cluster.GetNode().GetForwardAddr(member)
				if !ok {
					continue
				}
				d.enqueueRetry(retryTask{
					kind:      retryKindForEntry(entry),
					addr:      addr,
					key:       key,
					value:     entry.Value,
					ttl:       ttl,
					expiresAt: meta.expiresAt,
					version:   entry.Version,
					attempts:  1,
				})
				if d.metrics != nil {
					d.metrics.AntiEntropyTotal.Inc()
				}
			}
		} else {
			if addr, ok := d.cluster.GetNode().GetForwardAddr(owner); ok {
				d.enqueueRetry(retryTask{
					kind:      retryKindForEntry(entry),
					addr:      addr,
					key:       key,
					value:     entry.Value,
					ttl:       ttl,
					expiresAt: meta.expiresAt,
					version:   entry.Version,
					attempts:  1,
				})
				if d.metrics != nil {
					d.metrics.AntiEntropyTotal.Inc()
				}
			}
		}
	}
}

func (d *DistributedCache) enqueueRetry(task retryTask) {
	if d.opts.ReplicationRetryMaxAttempts <= 0 {
		return
	}
	if task.kind == retryStore {
		task.value = cloneBytes(task.value)
		if task.expiresAt.IsZero() {
			task.expiresAt = expiresAtForTTL(task.ttl)
		}
	}
	if task.version == 0 {
		task.version = d.nextVersion()
	}
	select {
	case d.retryCh <- task:
		if d.metrics != nil {
			d.metrics.ReplicationRetry.Inc()
		}
	case <-d.retryStop:
		return
	default:
		d.logger.Warnf("replication retry queue full, dropping task for %s", task.key)
		if d.metrics != nil {
			d.metrics.ReplicationRetryDrop.Inc()
		}
	}
}

func (d *DistributedCache) handleRetry(task retryTask) {
	if task.attempts > d.opts.ReplicationRetryMaxAttempts {
		return
	}
	client, err := d.clientFor(task.addr)
	if err != nil {
		d.scheduleRetry(task)
		return
	}
	ttl := task.ttl
	if task.kind == retryStore {
		var ok bool
		ttl, ok = ttlUntil(task.expiresAt, task.ttl)
		if !ok {
			return
		}
	}
	ctx, cancel := d.withTimeout(context.Background())
	defer cancel()
	switch task.kind {
	case retryStore:
		err = client.StoreVersioned(ctx, task.key, task.value, ttl, task.version, control.WriteConcernReplica)
	case retryDelete:
		err = client.DeleteVersioned(ctx, task.key, task.version, control.WriteConcernReplica)
	}
	if err != nil {
		d.handlePeerError(task.addr, "retry", err)
		d.scheduleRetry(task)
	}
}

func retryKindForEntry(entry cache.Entry) retryKind {
	if entry.Tombstone {
		return retryDelete
	}
	return retryStore
}

func expiresAtForTTL(ttl time.Duration) time.Time {
	if ttl <= 0 {
		return time.Time{}
	}
	return time.Now().Add(ttl)
}

func ttlUntil(expiresAt time.Time, fallback time.Duration) (time.Duration, bool) {
	if expiresAt.IsZero() {
		return fallback, true
	}
	ttl := time.Until(expiresAt)
	if ttl <= 0 {
		return 0, false
	}
	return ttl, true
}

func cloneBytes(value []byte) []byte {
	if value == nil {
		return nil
	}
	out := make([]byte, len(value))
	copy(out, value)
	return out
}

func (d *DistributedCache) scheduleRetry(task retryTask) {
	if task.attempts >= d.opts.ReplicationRetryMaxAttempts {
		return
	}
	next := task
	next.attempts++
	go func() {
		timer := time.NewTimer(d.opts.ReplicationRetryInterval)
		defer timer.Stop()
		select {
		case <-timer.C:
			d.enqueueRetry(next)
		case <-d.retryStop:
		}
	}()
}

func configFromOptions(opts Options) (*config.Config, error) {
	cfg := &config.Config{}
	cfg.Common.Cache.Cluster.MemberList.NodeName = opts.NodeName
	cfg.Common.Cache.Cluster.MemberList.BindAddr = opts.GossipBindAddr
	cfg.Common.Cache.Cluster.MemberList.BindPort = opts.GossipBindPort
	cfg.Common.Cache.Cluster.MemberList.AdvertiseAddr = opts.AdvertiseAddr
	cfg.Common.Cache.Cluster.MemberList.AdvertisePort = opts.AdvertisePort
	cfg.Common.Cache.Cluster.MemberList.SeedNodes = opts.SeedNodes
	cfg.Common.Cache.Cluster.MemberList.PartitionCount = opts.PartitionCount
	cfg.Common.Cache.Cluster.MemberList.ReplicationFactor = opts.ReplicationFactor
	cfg.Common.Cache.Control.BindAddr = opts.ControlBindAddr
	cfg.Common.Cache.Control.BindPort = opts.ControlBindPort
	cfg.Common.Cache.SizeBytes = int(opts.CacheSizeBytes)
	cfg.Common.Cache.SharedKey = opts.SharedKey
	cfg.Common.Cache.Namespace = opts.Namespace
	cfg.Common.Cache.WriteConcern = writeConcernString(opts.WriteConcern)
	cfg.Common.Cache.Retry.IntervalMs = int(opts.ReplicationRetryInterval / time.Millisecond)
	cfg.Common.Cache.Retry.MaxAttempts = opts.ReplicationRetryMaxAttempts
	cfg.Common.Cache.Retry.QueueSize = opts.ReplicationRetryQueueSize
	cfg.Common.Cache.Repair.IntervalMs = int(opts.RepairInterval / time.Millisecond)
	cfg.Common.Cache.Repair.MaxKeysPerCycle = opts.RepairMaxKeysPerCycle
	cfg.Common.Cache.TombstoneTTL = int(opts.TombstoneTTL / time.Millisecond)
	cfg.Common.Cache.Metrics.BindAddr = opts.MetricsBindAddr
	cfg.Common.Cache.Metrics.BindPort = opts.MetricsBindPort
	cfg.Common.Cache.Diagnostics.FailFast = opts.FailFast
	cfg.Common.Cache.Diagnostics.SelfCheck = opts.SelfCheck
	cfg.Common.Cache.Diagnostics.SelfCheckTimeout = int(opts.SelfCheckTimeout / time.Millisecond)
	cfg.Common.Cache.Diagnostics.RequireSharedKey = opts.RequireSharedKey
	cfg.Common.Cache.Diagnostics.PeerWarnInterval = int(opts.PeerWarnInterval / time.Millisecond)
	cfg.Common.Cache.Cluster.Tls.Enabled = opts.TLS.Enabled
	cfg.Common.Cache.Cluster.Tls.CertFile = opts.TLS.CertFile
	cfg.Common.Cache.Cluster.Tls.KeyFile = opts.TLS.KeyFile
	cfg.Common.Cache.Cluster.Tls.CaFile = opts.TLS.CAFile
	cfg.Common.Cache.Cluster.Tls.ServerName = opts.TLS.ServerName

	if cfg.Common.Cache.Control.BindAddr == "" || cfg.Common.Cache.Control.BindPort == 0 {
		return nil, fmt.Errorf("control bind address and port are required")
	}
	if cfg.Common.Cache.Cluster.MemberList.BindAddr == "" || cfg.Common.Cache.Cluster.MemberList.BindPort == 0 {
		return nil, fmt.Errorf("gossip bind address and port are required")
	}
	if cfg.Common.Cache.Cluster.MemberList.NodeName == "" {
		return nil, fmt.Errorf("node name is required")
	}
	return cfg, nil
}

func optionsFromConfig(cfg *config.Config) Options {
	return Options{
		NodeName:                    cfg.Common.Cache.Cluster.MemberList.NodeName,
		ControlBindAddr:             cfg.Common.Cache.Control.BindAddr,
		ControlBindPort:             cfg.Common.Cache.Control.BindPort,
		GossipBindAddr:              cfg.Common.Cache.Cluster.MemberList.BindAddr,
		GossipBindPort:              cfg.Common.Cache.Cluster.MemberList.BindPort,
		AdvertiseAddr:               cfg.Common.Cache.Cluster.MemberList.AdvertiseAddr,
		AdvertisePort:               cfg.Common.Cache.Cluster.MemberList.AdvertisePort,
		SeedNodes:                   cfg.Common.Cache.Cluster.MemberList.SeedNodes,
		SharedKey:                   cfg.Common.Cache.SharedKey,
		Namespace:                   cfg.Common.Cache.Namespace,
		WriteConcern:                parseWriteConcern(cfg.Common.Cache.WriteConcern),
		PartitionCount:              cfg.Common.Cache.Cluster.MemberList.PartitionCount,
		ReplicationFactor:           cfg.Common.Cache.Cluster.MemberList.ReplicationFactor,
		CacheSizeBytes:              int64(cfg.Common.Cache.SizeBytes),
		ReplicationRetryInterval:    time.Duration(cfg.Common.Cache.Retry.IntervalMs) * time.Millisecond,
		ReplicationRetryMaxAttempts: cfg.Common.Cache.Retry.MaxAttempts,
		ReplicationRetryQueueSize:   cfg.Common.Cache.Retry.QueueSize,
		RepairInterval:              time.Duration(cfg.Common.Cache.Repair.IntervalMs) * time.Millisecond,
		RepairMaxKeysPerCycle:       cfg.Common.Cache.Repair.MaxKeysPerCycle,
		TombstoneTTL:                time.Duration(cfg.Common.Cache.TombstoneTTL) * time.Millisecond,
		MetricsBindAddr:             cfg.Common.Cache.Metrics.BindAddr,
		MetricsBindPort:             cfg.Common.Cache.Metrics.BindPort,
		FailFast:                    cfg.Common.Cache.Diagnostics.FailFast,
		SelfCheck:                   cfg.Common.Cache.Diagnostics.SelfCheck,
		SelfCheckTimeout:            time.Duration(cfg.Common.Cache.Diagnostics.SelfCheckTimeout) * time.Millisecond,
		RequireSharedKey:            cfg.Common.Cache.Diagnostics.RequireSharedKey,
		PeerWarnInterval:            time.Duration(cfg.Common.Cache.Diagnostics.PeerWarnInterval) * time.Millisecond,
		TLS: TLSOptions{
			Enabled:           cfg.Common.Cache.Cluster.Tls.Enabled,
			CertFile:          cfg.Common.Cache.Cluster.Tls.CertFile,
			KeyFile:           cfg.Common.Cache.Cluster.Tls.KeyFile,
			CAFile:            cfg.Common.Cache.Cluster.Tls.CaFile,
			ServerName:        cfg.Common.Cache.Cluster.Tls.ServerName,
			ClientCertFile:    cfg.Common.Cache.Cluster.Tls.ClientCertFile,
			ClientKeyFile:     cfg.Common.Cache.Cluster.Tls.ClientKeyFile,
			RequireClientCert: cfg.Common.Cache.Cluster.Tls.RequireClientCert,
		},
	}
}

func validateConfig(cfg *config.Config, opts Options) error {
	if opts.TombstoneTTL < 0 {
		return fmt.Errorf("tombstone_ttl must be >= 0")
	}
	if opts.RequireSharedKey && cfg.Common.Cache.SharedKey == "" {
		return fmt.Errorf("diagnostics.require_shared_key enabled but shared_key is empty")
	}
	if opts.FailFast {
		controlBind := cfg.Common.Cache.Control.BindAddr
		if (controlBind == "" || controlBind == "0.0.0.0") && cfg.Common.Cache.Cluster.MemberList.AdvertiseAddr == "" {
			return fmt.Errorf("control bind address is %q without advertise address; set advertise_address or explicit control bind addr", controlBind)
		}
	}
	return nil
}

func betterReplicaEntry(candidate, best control.Entry, bestFound bool) bool {
	if !bestFound {
		return true
	}
	if candidate.Version != best.Version {
		return candidate.Version > best.Version
	}
	return candidate.Tombstone && !best.Tombstone
}

func (d *DistributedCache) selfCheckControl() error {
	if d.control == nil {
		return fmt.Errorf("control server not initialized")
	}
	addr := d.control.Addr()
	if addr == "" {
		return fmt.Errorf("control server address unavailable")
	}
	client, err := control.Dial(addr, control.ClientOptions{
		SharedKey:   d.opts.SharedKey,
		TLS:         d.clientTLS,
		DialTimeout: d.opts.SelfCheckTimeout,
	})
	if err != nil {
		return err
	}
	defer client.Close()
	ctx, cancel := context.WithTimeout(context.Background(), d.opts.SelfCheckTimeout)
	defer cancel()
	_, err = client.Ping(ctx)
	return err
}

func tlsConfigs(opts TLSOptions) (*tls.Config, *tls.Config, error) {
	if !opts.Enabled {
		return nil, nil, nil
	}

	cert, err := tls.LoadX509KeyPair(opts.CertFile, opts.KeyFile)
	if err != nil {
		return nil, nil, fmt.Errorf("load tls keypair: %w", err)
	}

	clientCertFile := opts.ClientCertFile
	clientKeyFile := opts.ClientKeyFile
	if clientCertFile == "" {
		clientCertFile = opts.CertFile
	}
	if clientKeyFile == "" {
		clientKeyFile = opts.KeyFile
	}
	clientCert, err := tls.LoadX509KeyPair(clientCertFile, clientKeyFile)
	if err != nil {
		return nil, nil, fmt.Errorf("load client tls keypair: %w", err)
	}

	var caPool *x509.CertPool
	if opts.CAFile != "" {
		caBytes, err := os.ReadFile(opts.CAFile)
		if err != nil {
			return nil, nil, fmt.Errorf("read ca file: %w", err)
		}
		caPool = x509.NewCertPool()
		if !caPool.AppendCertsFromPEM(caBytes) {
			return nil, nil, fmt.Errorf("failed to append ca certs")
		}
	}

	serverTLS := &tls.Config{
		Certificates: []tls.Certificate{cert},
		ClientAuth:   tls.NoClientCert,
		ClientCAs:    caPool,
	}
	if opts.RequireClientCert {
		if caPool == nil {
			return nil, nil, fmt.Errorf("require_client_cert enabled but CA file not provided")
		}
		serverTLS.ClientAuth = tls.RequireAndVerifyClientCert
	}
	clientTLS := &tls.Config{
		RootCAs:      caPool,
		ServerName:   opts.ServerName,
		Certificates: []tls.Certificate{clientCert},
	}
	return serverTLS, clientTLS, nil
}
