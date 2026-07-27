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
    PeerNodes:            []string{"10.0.0.10:8946"},
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

To use this repository's config-file/env loader from a service, load the public
`config` package and hand the decoded config to the cache package:

```go
import (
  dcache "github.com/infamousity/distributed-cache/cache"
  cacheconfig "github.com/infamousity/distributed-cache/config"
)

cfg, err := cacheconfig.Load("config.yml", "config.secrets.yml")
if err != nil {
  panic(err)
}

c, err := dcache.StartFromConfig(cfg)
if err != nil {
  panic(err)
}
defer c.Close()
```

### Write Concern

```go
_ = c.Set(ctx, "k", []byte("v"), time.Second, dcache.WithWriteConcern(dcache.WriteConcernMajority))
_ = c.Del(ctx, "k", dcache.WithWriteConcern(dcache.WriteConcernMajority))

// Require the configured replication factor to acknowledge the version.
_ = c.Set(ctx, "critical", []byte("v"), time.Second, dcache.WithWriteConcern(dcache.WriteConcernAll))
```

Configuration-driven startup defaults to `MAJORITY`. Configure `one`, `majority`,
or `all` with `common.cache.write_concern` / `CACHE_WRITE_CONCERN`.

For Go API compatibility, `Start(Options{})` continues to default to `ONE`.
Programmatic callers that want the recommended general-purpose default must set
`Options.WriteConcern: WriteConcernMajority`. The exported numeric values of
`WriteConcernOne` and `WriteConcernMajority` remain `0` and `1` respectively.

With `WriteConcernMajority`, the required acknowledgement count is derived from
the configured replication factor, not the number of members currently visible
in the ring. RF3 therefore always requires two acknowledgements. Owner-side
quorum failure returns an error matching `ErrWriteIndeterminate`:

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

`WriteConcernAll` requires the configured replication factor. If the ring is
undersized, or any assigned replica is unavailable or rejects the version, it
returns `ErrWriteIndeterminate`. Neither majority nor all silently weakens when
members leave the ring. This favors consistency over latency and write
availability. Deploy support for the `all` wire value to every node before
enabling it during a rolling upgrade.

If the configured cache capacity or admission policy cannot retain a value,
`Set` returns an error matching `ErrEntryRejected`. This is a definite local
admission failure, unlike `ErrWriteIndeterminate`; callers may fall back to the
source of truth without assuming the cache contains the value.

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

If `--config` is omitted, the standalone CLI uses `CACHE_CONFIG` when it is set;
otherwise it falls back to `config.yml`. `--level` is a process-level log
override. When it is omitted, the CLI uses `common.cache.log.level` /
`CACHE_LOG_LEVEL`, then defaults to `info`.

Every named config file is required; a missing path fails startup instead of
silently falling back to defaults. The config package reads the process
environment but does not automatically load or mutate `.env` files. Local
development can load an env file in the shell or container runtime before
starting the process.

For Swarm-style overlay deployments, use `config.swarm.example.yml` as the
starting profile. It binds cache internals to `0.0.0.0`, derives `peer_dns_name`
from explicit peer DNS env or Swarm runtime metadata, and derives peer-reachable
memberlist advertisement with `advertise_addr: auto`. Use `peer_dns_names` or
`CACHE_CLUSTER_MEMBERLIST_PEER_DNS_NAMES` when discovery must span multiple
connected stacks. If a task is attached to more than one overlay network, set
`peer_network_cidrs` / `CACHE_CLUSTER_MEMBERLIST_PEER_NETWORK_CIDRS` to the cache
overlay CIDR so DNS peer discovery ignores addresses from non-cache networks.
Prefer declaring that subnet in the Swarm network IPAM block; otherwise inspect
the chosen overlay network with `docker network inspect`.

For the full runtime contract, including required fields, derived advertise
addresses, bind-address fallbacks, and operational defaults, see
`docs/runtime-profiles.md`.

For images that bake both configs, run the cache node with the Swarm profile as
a later override:

```bash
./cache-node -c config.yml -c config.swarm.example.yml -c config.secrets.yml
```

Do not put production shared keys in the base config.

## Versions

Stable releases use root Go module tags and can be referenced directly from Go
projects:

```bash
go get github.com/infamousity/distributed-cache@v0.1.1
```

Feature branches may also publish Go-friendly prerelease tags for integration
testing before merge. See [CONTRIBUTING.md](CONTRIBUTING.md) for the release
and prerelease workflow.

## Swarm

Build and push any images before using `docker stack deploy`; Swarm does not
build images from `build:` directives.

The maintained Swarm example lives in `examples/swarm`. It runs an app service
where each replica embeds the cache, plus an internal harness service used only
by the proof script:

```bash
cd examples/swarm
DOCKER_CONTEXT=default ./chaos.sh
```

For manual deployment:

```bash
docker --context default build -t distributed-cache-example-app:latest -f examples/swarm/app/Dockerfile .
docker --context default stack deploy -c examples/swarm/docker-stack.yml example
```

Notes:
- The maintained runtime images use a pinned Alpine base and run as the unprivileged numeric user `65532:65532`. Mounted configuration, certificate, and key files must be readable by that user.
- `cache_control` is an internal, encrypted overlay network to isolate control-plane traffic.
- In application stacks, each app replica embeds the cache and uses `tasks.<service>` for gossip peer discovery.
- The library cache API is in-process; any HTTP/JSON/GraphQL/etc. route that uses the cache is owned by the host service.
- The Swarm example does not publish the app harness port by default. The proof script runs probes from an internal harness service on the same overlay.
- The gRPC control plane is only for node-to-node coordination. Do not publish gossip or control-plane ports externally.

See [examples/swarm](examples/swarm) for the current stack, proof harness, and
runtime notes.

## API Boundary

This module is an embedded cache library, not a standalone cache gateway. The
CRUD cache API is the Go API used by the host service process.

Host services decide whether to expose cache-backed operations through their own
service APIs. If they do, those routes must use the service's normal
authentication and authorization, tenant isolation, validation, rate limiting,
and audit model.

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
- peers can discover at least one live peer through static `PeerNodes` or DNS
  peer refresh
- gossip and gRPC control-plane traffic stay on private/internal networks
- `SharedKey` is configured consistently across all nodes

Memberlist is the membership and gossip layer. It uses UDP for most gossip and
failure-detection traffic and TCP for larger state sync. gRPC is the private
node-to-node control-plane RPC layer used for fetch/store/delete/ping.

Runtime-specific notes:

- **Docker Swarm:** use `tasks.<service>` as the DNS peer name for task-level
  discovery. If peers span multiple connected stacks, use `peer_dns_names` /
  `PeerDNSNames` with each stack's `tasks.<service>` name. Put gossip and gRPC
  control-plane traffic on an internal overlay network. If the overlay is
  multi-host, allow both TCP and UDP on the gossip port and TCP on the
  control-plane port between tasks. When services share more than one overlay,
  configure `peer_network_cidrs` so only the cache overlay addresses are joined.
- **Kubernetes:** prefer a headless Service for DNS peer discovery and set
  advertised addresses to pod-reachable IPs or stable DNS names. NetworkPolicy
  should allow TCP/UDP gossip between pods and TCP gRPC control-plane traffic
  between pods, while denying external ingress to those ports.
- **Podman / systemd / VMs:** use static `PeerNodes` or a DNS name that resolves
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

`SharedKey` protects memberlist gossip encryption and gRPC control-plane
authentication. The key is only for cache nodes; external clients do not need to
know it.

If `common.cache.shared_key` / `CACHE_SHARED_KEY` is not configured and
`AllowInsecure` is false, the cache generates an ephemeral internal key for the
current process and logs that it did so without logging the value. This avoids
accidentally starting an unauthenticated control plane.

For multi-node deployments, every cache node still needs the same key. Provide
that shared value through layered config, env, or a secret manager. If each node
generates its own key, nodes will start but will not authenticate with one
another.

Set `RequireSharedKey` / `common.cache.diagnostics.require_shared_key` when a
deployment should fail startup unless the key is explicitly configured.

For local-only development with no auth/encryption key at all, opt in explicitly:

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
- `common.cache.cluster.memberlist.advertise_addr` may be set to a peer-reachable IP or `IP:port`. Endpoint form is normalized internally to memberlist's address and port fields. Memberlist requires an IP advertise address, not a DNS name.
- `common.cache.cluster.memberlist.advertise_addr: auto` derives the local memberlist advertise IP from `peer_network_cidrs`, or from `peer_dns_name` when that resolves to exactly one matching local network. If `peer_network_cidrs` is set and `advertise_addr` is omitted, the library derives the advertise IP from those CIDRs.
- `common.cache.cluster.memberlist.peer_dns_name` plus `peer_dns_port` enables generic DNS peer discovery with periodic refresh; `peer_dns_names` accepts multiple DNS names for multi-stack or multi-service discovery. Static `peer_nodes` still works.
- `common.cache.cluster.memberlist.peer_network_cidrs` filters DNS peer discovery and `advertise_addr: auto` to the selected cache network. Use it in Swarm when `tasks.<service>` can return task IPs from multiple overlay networks.
  The value should come from the cache/discovery network's IPAM subnet, not from
  every network attached to the task. Explicit memberlist `advertise_addr`
  values and IP-shaped `api.advertise_addr` values must also be inside these
  CIDRs.
- The automatic bind-address filter defaults to the RFC1918 networks
  `10.0.0.0/8`, `172.16.0.0/12`, and `192.168.0.0/16`. Set
  `bind_address_filter` explicitly when the cache network uses another range.
- `peerDNSName`, `peerIP`, and `peerAddr` are config-template functions
  evaluated by the repository config loader before YAML is decoded. They are not
  env vars, YAML fields, or `cache.Options` members. `peerIP` and `peerAddr` can
  also take an explicit DNS name, such as `peerIP "tasks.app"` or
  `peerAddr "tasks.app" 9090`.
- If `memberlist.advertise_addr` is omitted, memberlist falls back to the
  memberlist bind address. That is only valid when the bind address is a
  specific peer-reachable IP, not `0.0.0.0`.
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
- A replica that remains disconnected longer than the tombstone protection
  window can rejoin with an older value after healthy nodes have forgotten the
  delete version. Set the tombstone TTL longer than the maximum tolerated peer
  outage and value lifetime when stale resurrection is unacceptable. Values
  without an expiry require an operationally bounded outage or a durable source
  of truth; this cache does not provide permanent delete history.
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
