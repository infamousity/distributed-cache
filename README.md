# Distributed Cache (In-Process Data Plane + gRPC Control Plane)

A lightweight distributed caching module built on Ristretto v2, consistent hashing, and gossip-based node discovery. The library cache API is **in-process**, while **control-plane communication** between nodes uses gRPC.

## Features

- Fast local cache (Ristretto v2)
- In-process data-plane API for Go services
- Memberlist-based node discovery
- Key sharding via consistent hashing
- Configurable replication factor
- gRPC control-plane with shared-key auth
- Optional TLS and future-ready mTLS
- gRPC health/readiness endpoints
- Runtime-neutral deployment contract with Swarm-oriented examples
- Prometheus metrics endpoint (optional)
- Read repair + background anti-entropy

## Usage (Go)

```go
import (
  "context"
  "time"

  dcache "github.com/infamousity/distributed-cache/cache"
)

func main() {
  c, err := dcache.Start(dcache.Options{
    NodeName:             "service-1",
    ControlBindAddr:      "10.0.0.12",
    ControlBindPort:      9090,
    ControlAdvertiseAddr: "10.0.0.12:9090",
    GossipBindAddr:       "10.0.0.12",
    GossipBindPort:       8946,
    SeedNodes:            []string{"10.0.0.10:8946"},
    SharedKey:            "super-secret",
    ReplicationFactor:    3,
  })
  if err != nil {
    panic(err)
  }
  defer c.Close()

  if err := c.WaitReady(context.Background()); err != nil {
    panic(err)
  }

  _ = c.Set(context.Background(), "k", []byte("v"), 10*time.Second)
  v, found, _ := c.Get(context.Background(), "k")
  _ = v
  _ = found
}
```

### Write Concern

```go
_ = c.Set(ctx, "k", []byte("v"), time.Second, dcache.WithWriteConcern(dcache.WriteConcernMajority))
_ = c.Del(ctx, "k", dcache.WithWriteConcern(dcache.WriteConcernMajority))
```

Default write concern is `ONE`.

With `WriteConcernMajority`, owner-side quorum failure returns an error matching
`ErrWriteIndeterminate`:

```go
err := c.Del(ctx, "k", dcache.WithWriteConcern(dcache.WriteConcernMajority))
if errors.Is(err, dcache.ErrWriteIndeterminate) {
  // The owner accepted the tombstone locally, but quorum replication did not
  // complete. The delete may already be visible and may later repair outward.
}
```

This cache is availability-oriented: a majority write error does not mean the
write definitely did not happen. Callers that use majority writes for invalidation
or delete semantics must treat `ErrWriteIndeterminate` as a distinct outcome.

### Namespace (Transparent Prefixing)

Configure a namespace once and keep normal keys in your code:

```yaml
common:
  cache:
    namespace: service-a
```

You can override per call:

```go
_ = c.Set(ctx, "k", []byte("v"), time.Second, dcache.WithNamespace("tenant-1"))
v, found, _ := c.Get(ctx, "k", dcache.WithNamespace("tenant-1"))
```

## CLI

The CLI starts a cache node using config files:

```bash
./cache-node -c config.yml -c config.secrets.yml
```

Config files are merged from left to right, so keep non-secret defaults in
`config.yml` and layer secrets at runtime with a later file such as
`config.secrets.yml`. That secrets file is intentionally local/untracked; create
it on the target host or mount it from your secret manager:

```yaml
common:
  cache:
    shared_key: "${CACHE_SHARED_KEY}"
```

Do not put production shared keys in the base config.

## Swarm

Build and push any images before using `docker stack deploy`; Swarm does not build images from `build:` directives.

Example standalone cache-node stack for control-plane/cluster testing:

```bash
docker stack deploy -c docker-compose.swarm.yml cache
```

Example app stack using the in-process cache module:

```bash
docker stack deploy -c docker-stack.swarm.yml app
```

Notes:
- `cache_control` is an internal, encrypted overlay network to isolate control-plane traffic.
- In application stacks, each app replica embeds the cache and uses `tasks.<service>` for gossip seed discovery.
- The library cache API is in-process; any HTTP/JSON/GraphQL/etc. route that uses the cache is owned by the host service.
- The gRPC control plane is only for node-to-node coordination. Do not publish gossip or control-plane ports externally.

## API Boundary

This module is an embedded cache library, not a standalone cache gateway. The
CRUD cache API is the Go API used by the host service process.

Host services decide whether to expose cache-backed operations through their own
service APIs. If they do, those routes must use the service's normal authn,
authz, tenant isolation, validation, rate limiting, and audit model.

The cache control plane is different: it is private infrastructure between cache
nodes. Keep gossip and gRPC control-plane listeners on private/internal networks
and protect them with `SharedKey` and, where appropriate, TLS or mTLS.

## Runtime Contract

The cache does not depend on Swarm, Kubernetes, Podman, or any specific
orchestrator. A runtime only needs to satisfy these contracts:

- every cache node has a stable unique `NodeName`
- every node binds memberlist gossip on a reachable TCP/UDP port
- every node advertises a peer-reachable gossip address and port
- every node binds the gRPC control plane on a reachable TCP port
- every node advertises a peer-reachable gRPC control-plane `host:port`
- peers can discover at least one live seed through static `SeedNodes` or DNS
  seed refresh
- gossip and gRPC control-plane traffic stay on private/internal networks
- `SharedKey` is configured consistently across all nodes

Memberlist is the membership and gossip layer. It uses UDP for most gossip and
failure-detection traffic and TCP for larger state sync. gRPC is the private
node-to-node control-plane RPC layer used for fetch/store/delete/ping.

Runtime-specific notes:

- **Docker Swarm:** use `tasks.<service>` as the DNS seed name for task-level
  discovery. Put gossip and gRPC control-plane traffic on an internal overlay
  network. If the overlay is multi-host, allow both TCP and UDP on the gossip
  port and TCP on the control-plane port between tasks.
- **Kubernetes:** prefer a headless Service for DNS seed discovery and set
  advertised addresses to pod-reachable IPs or stable DNS names. NetworkPolicy
  should allow TCP/UDP gossip between pods and TCP gRPC control-plane traffic
  between pods, while denying external ingress to those ports.
- **Podman / systemd / VMs:** use static `SeedNodes` or a DNS name that resolves
  to peer addresses. Ensure host firewalls allow the gossip TCP/UDP port and the
  gRPC control-plane TCP port between nodes only.

See [Runtime Profiles](docs/runtime-profiles.md) for concrete Swarm,
Kubernetes, and static deployment guidance.

Kubernetes manifest examples live in [examples/kubernetes](examples/kubernetes).

## Health

The control-plane exposes standard gRPC health endpoints:
- Service name: `control.ControlPlane`
- Overall status: empty service name (`""`)

For application lifecycle wiring, call `WaitReady(ctx)` before serving traffic when
the service requires a minimum number of verified peers. Configure that threshold
with `MinReadyPeers` or `common.cache.diagnostics.min_ready_peers`. `Ready(ctx)`
performs a single readiness check; `WaitReady(ctx)` waits until ready or until the
context is canceled.

`Close()` is terminal. After shutdown starts, `Status().Closed` is true,
`Status().Ready` and `Status().ControlReady` are false, and `Ready(ctx)` /
`WaitReady(ctx)` return `ErrClosed`. Public cache operations (`Get`, `Set`,
`Del`) and private control-plane handlers also return `ErrClosed` after close
instead of reporting incidental topology errors.

`Status()` includes gossip diagnostics derived from memberlist logs:

- total memberlist log messages observed by the node
- total degraded-looking gossip transport events
- the last memberlist log message
- the last degraded-looking gossip message and timestamp

These diagnostics are always collected. Prometheus export is optional. A cache
can continue to serve reads and writes while gossip transport is degraded, but
repeated degraded events should be investigated because membership convergence
and failure detection may be impaired.

`SelfCheck` and `FailFast` are startup policy knobs. Enable them only when the
runtime can route from a node back to its advertised gRPC control-plane address.
They are useful in static and some orchestrated deployments, but not every
runtime supports self-dialing its advertised peer address.

Production services should set `MinReadyPeers` and call `WaitReady(ctx)` before
serving traffic when they require a minimum cluster shape. Metrics are an
optional export path, not a prerequisite for diagnostics or cache correctness.

## Security Defaults

`SharedKey` is required by default. This protects memberlist gossip encryption and
gRPC control-plane authentication from being accidentally disabled in deployed
services.

For local-only development without a shared key, opt in explicitly:

```go
c, err := dcache.Start(dcache.Options{
  NodeName:        "dev-node",
  ControlBindAddr: "127.0.0.1",
  ControlBindPort: 9090,
  GossipBindAddr:  "127.0.0.1",
  GossipBindPort:  8946,
  AllowInsecure:   true,
})
```

The equivalent config/env setting is `common.cache.diagnostics.allow_insecure`
or `CACHE_ALLOW_INSECURE=true`.

## TLS / mTLS

TLS is optional and configured in `common.cache.cluster.tls`. To enable mutual TLS later, set:
- `require_client_cert: true`
- `client_cert_file` and `client_key_file` (or reuse `cert_file` and `key_file`)
- `ca_file`

## Notes

- The config `common.cache.api` section now defines the **control-plane** bind address/port.
- `common.cache.api.advertise_addr` may be set to the peer-reachable `host:port` for the control plane. This is separate from memberlist gossip advertisement and is useful in runtimes where bind and peer-reachable addresses differ.
- `common.cache.cluster.memberlist.seed_dns_name` plus `seed_dns_port` enables generic DNS seed discovery with periodic refresh; static `seed_nodes` still works.
- `common.cache.churn.grace_period_ms` delays only ownership-loss cleanup during membership churn. Explicit deletes still write tombstones immediately.
- The library cache data-plane is always in-process; the library does not expose
  an HTTP CRUD API. Host services may expose cache-backed routes as part of their
  own API contract.
- Replication factor values below `2` are clamped to `2` to avoid a startup panic in the underlying consistent hashing library when running with a single node.
- Replication retries are best-effort and configurable via `common.cache.retry` or the `CACHE_RETRY_*` env vars.
- Read repair and anti-entropy are best-effort; they prioritize availability over strict consistency.
- Versions use an internal HLC-style tuple `{physical, logical, nodeID}` with explicit compare rules. A node advances its local generator from versions it observes through local storage, fetches, forwarded writes, and replica writes. This prevents local owner writes from going backward after ownership movement, restart-like counter loss, or observing an older wall-clock-derived version from a previous release.
- A node cannot order itself after a version it has never observed. During a hard partition, the first post-partition reconciliation still depends on replication/read-repair/tombstone exchange.
- `common.cache.tombstone_ttl_ms` controls how long delete tombstones are retained to reject stale replica/retry writes. The default is `300000` (5 minutes); this is the stale-delete protection window for the memberlist/consistent-hash cache.
- `cache_gossip_log_messages` and `cache_gossip_degraded_events` expose
  memberlist diagnostic counts when Prometheus metrics are enabled.

## Metrics

Enable Prometheus metrics by setting:

```yaml
common:
  cache:
    metrics:
      bind_addr: 0.0.0.0
      bind_port: 9102
```

Then scrape `http://<addr>:<port>/metrics`.
