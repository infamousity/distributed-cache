# Distributed Cache (In-Process Data Plane + gRPC Control Plane)

A lightweight distributed caching module built on Ristretto v2, consistent hashing, and gossip-based node discovery. The cache API is **in-process**, while **control-plane communication** between nodes uses gRPC.

## Features

- Fast local cache (Ristretto v2)
- In-process data-plane API for Go services
- Memberlist-based node discovery
- Key sharding via consistent hashing
- Configurable replication factor
- gRPC control-plane with shared-key auth
- Optional TLS and future-ready mTLS
- gRPC health/readiness endpoints
- Swarm-compatible deployment
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
    NodeName:          "service-1",
    ControlBindAddr:   "10.0.0.12",
    ControlBindPort:   9090,
    GossipBindAddr:    "10.0.0.12",
    GossipBindPort:    8946,
    SeedNodes:         []string{"10.0.0.10:8946"},
    SharedKey:         "super-secret",
    ReplicationFactor: 3,
  })
  if err != nil {
    panic(err)
  }
  defer c.Close()

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
./cache-node -c config.yml
```

## Swarm

Example cache-only stack:

```bash
docker stack deploy -c docker-compose.swarm.yml cache
```

Example app + cache stack:

```bash
docker stack deploy -c docker-stack.swarm.yml app
```

Notes:
- `cache_control` is an internal, encrypted overlay network to isolate control-plane traffic.
- Use `tasks.cache` for seed discovery inside Swarm.

## Health

The control-plane exposes standard gRPC health endpoints:
- Service name: `control.ControlPlane`
- Overall status: empty service name (`""`)

## TLS / mTLS

TLS is optional and configured in `common.cache.cluster.tls`. To enable mutual TLS later, set:
- `require_client_cert: true`
- `client_cert_file` and `client_key_file` (or reuse `cert_file` and `key_file`)
- `ca_file`

## Notes

- The config `common.cache.api` section now defines the **control-plane** bind address/port.
- The cache data-plane is always in-process; no HTTP API is exposed.
- Replication factor values below `2` are clamped to `2` to avoid a startup panic in the underlying consistent hashing library when running with a single node.
- Replication retries are best-effort and configurable via `common.cache.retry` or the `CACHE_RETRY_*` env vars.
- Read repair and anti-entropy are best-effort; they prioritize availability over strict consistency.
- `common.cache.tombstone_ttl_ms` controls how long delete tombstones are retained to reject stale replica/retry writes. The default is `300000` (5 minutes); this is the stale-delete protection window for the memberlist/consistent-hash cache.

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
