# Runtime Profiles

This cache is an embedded Go library. It does not depend on a specific
orchestrator, but every runtime must satisfy the same cluster contract.

## Common Contract

Each cache node needs:

- a stable, unique `NodeName`
- a peer-reachable memberlist gossip address and port
- a peer-reachable gRPC control-plane `host:port`
- at least one peer discovered through static `PeerNodes` or DNS peer refresh
- private node-to-node network reachability for memberlist and gRPC traffic
- the same `SharedKey` on every node

Traffic requirements:

- memberlist gossip port: TCP and UDP between cache nodes
- gRPC control-plane port: TCP between cache nodes
- metrics port: TCP from the metrics scraper only, when enabled
- service API ports: owned by the host service, not by this cache library

Do not publish memberlist gossip or gRPC control-plane ports externally.

When using the repository config loader, prefer the canonical env names shown
below, such as `CACHE_CLUSTER_MEMBERLIST_NODE_NAME` and `CACHE_API_BIND_ADDR`.
Host services that construct `cache.Options` directly may choose their own env
mapping. The Swarm example app uses shorter example-local names such as
`CACHE_NODE_NAME` and `CACHE_GOSSIP_BIND_ADDR`.

## Docker Swarm

Swarm works well when each service task is also a cache node.

Use:

- `CACHE_CLUSTER_MEMBERLIST_NODE_NAME="{{.Task.Name}}"` for a unique node name
- `CACHE_CLUSTER_MEMBERLIST_PEER_DNS_NAME=tasks.<service>` for task-level DNS
  peer discovery
- `CACHE_CLUSTER_MEMBERLIST_PEER_DNS_PORT=<gossip-port>`
- `CACHE_CLUSTER_MEMBERLIST_BIND_ADDRESS=0.0.0.0`
- `CACHE_CLUSTER_MEMBERLIST_BIND_PORT=<gossip-port>`
- `CACHE_API_BIND_ADDR=0.0.0.0`
- `CACHE_API_BIND_PORT=<control-port>`
- `CACHE_CLUSTER_MEMBERLIST_ADVERTISE_ADDRESS=<task-reachable-ip-or-name>` when
  auto-detection is not reliable
- `CACHE_CLUSTER_MEMBERLIST_ADVERTISE_PORT=<gossip-port>`
- `CACHE_CONTROL_ADVERTISE_ADDR=<task-reachable-host>:<control-port>` when the
  bind address is not peer-reachable

Network expectations:

- put cache traffic on an internal overlay network
- enable overlay encryption when available
- allow TCP and UDP gossip traffic between tasks
- allow TCP gRPC control-plane traffic between tasks
- do not publish gossip or gRPC control-plane ports

Swarm DNS note: `tasks.<service>` returns task IPs instead of the service VIP.
That is the right peer discovery shape because peers must discover
individual cache nodes, not only the load-balanced service endpoint.

When using config files, `peerIP` and `peerAddr` can derive advertise addresses
from the same task-level DNS name:

```yaml
common:
  cache:
    api:
      bind_addr: "0.0.0.0"
      bind_port: 9090
      advertise_addr: '{{ peerAddr "tasks.app" 9090 }}'
    cluster:
      memberlist:
        node_name: '{{ env "HOSTNAME" }}'
        bind_address: "0.0.0.0"
        bind_port: 8946
        advertise_addr: '{{ peerIP "tasks.app" }}'
        advertise_port: 8946
        peer_dns_name: "tasks.app"
        peer_dns_port: 8946
```

`peerIP` selects this node's local IPv4 address on the same subnet as the
resolved peer DNS records. This is safer than `ip` when a task is attached to
multiple networks.

## Kubernetes

Kubernetes should use pod-level discovery, not Service load balancing, for cache
membership.

Use:

- a headless Service for peer DNS
- StatefulSet pod names when stable identity matters
- pod IPs or stable pod DNS names as advertised addresses
- NetworkPolicy to allow cache node-to-node traffic and deny external access to
  gossip/control-plane ports

Example environment shape:

```yaml
env:
  - name: CACHE_CLUSTER_MEMBERLIST_NODE_NAME
    valueFrom:
      fieldRef:
        fieldPath: metadata.name
  - name: CACHE_CLUSTER_MEMBERLIST_BIND_ADDRESS
    value: "0.0.0.0"
  - name: CACHE_CLUSTER_MEMBERLIST_BIND_PORT
    value: "8946"
  - name: CACHE_API_BIND_ADDR
    value: "0.0.0.0"
  - name: CACHE_API_BIND_PORT
    value: "9090"
  - name: CACHE_CLUSTER_MEMBERLIST_PEER_DNS_NAME
    value: "cache-peers.default.svc.cluster.local"
  - name: CACHE_CLUSTER_MEMBERLIST_PEER_DNS_PORT
    value: "8946"
  - name: CACHE_SHARED_KEY
    valueFrom:
      secretKeyRef:
        name: cache-shared-key
        key: shared-key
```

If using pod IP advertisement:

```yaml
env:
  - name: POD_IP
    valueFrom:
      fieldRef:
        fieldPath: status.podIP
  - name: CACHE_CLUSTER_MEMBERLIST_ADVERTISE_ADDRESS
    value: "$(POD_IP)"
  - name: CACHE_CLUSTER_MEMBERLIST_ADVERTISE_PORT
    value: "8946"
  - name: CACHE_CONTROL_ADVERTISE_ADDR
    value: "$(POD_IP):9090"
```

NetworkPolicy should allow:

- TCP and UDP on the gossip port between selected pods
- TCP on the gRPC control-plane port between selected pods
- TCP on metrics only from the scraper namespace, when metrics are enabled

It should not allow external ingress to gossip or gRPC control-plane ports.

See `examples/kubernetes/` for an example headless Service, StatefulSet, Secret,
and NetworkPolicy.

Kubernetes usually has a simpler source of truth for the local pod IP via the
Downward API, so prefer `status.podIP` for advertised addresses there. `peerIP`
is still useful for config-file-driven runtimes where the local peer network
must be inferred from DNS.

## Podman, Systemd, and VMs

Static deployments work with explicit peers or DNS records.

Use:

- `NodeName` set to a stable hostname or instance ID
- `PeerNodes` with `host:port` entries, or a DNS name that resolves to peers
- `AdvertiseAddr` set to an address other cache nodes can reach
- `ControlAdvertiseAddr` set to the peer-reachable gRPC `host:port`
- host firewalls that allow cache traffic only between trusted nodes

Example static config:

```yaml
common:
  cache:
    shared_key: "${CACHE_SHARED_KEY}"
    api:
      bind_addr: "0.0.0.0"
      bind_port: 9090
      advertise_addr: "cache-1.internal:9090"
    cluster:
      memberlist:
        node_name: "cache-1"
        bind_address: "0.0.0.0"
        bind_port: 8946
        advertise_addr: "cache-1.internal"
        advertise_port: 8946
        peer_nodes:
          - "cache-1.internal:8946"
          - "cache-2.internal:8946"
          - "cache-3.internal:8946"
```

Firewall expectations:

- allow TCP and UDP `8946` only from peer cache nodes
- allow TCP `9090` only from peer cache nodes
- deny external access to both ports

## Readiness

Use `WaitReady(ctx)` before serving traffic when the service requires a minimum
cluster shape. Set `MinReadyPeers` or
`common.cache.diagnostics.min_ready_peers` to the number of verified peers the
service expects before it should become ready.

`Ready(ctx)` is a point-in-time check. `WaitReady(ctx)` waits until readiness is
true or the context is canceled.

`Close()` is terminal. After shutdown starts, `Status().Closed` is true,
`Status().Ready` and `Status().ControlReady` are false, and `Ready(ctx)` /
`WaitReady(ctx)` return `ErrClosed`. Public cache operations (`Get`, `Set`,
`Del`) and private control-plane handlers also return `ErrClosed` after close
instead of reporting incidental topology errors.

For a three-node production profile, set the readiness threshold to `2` verified
peers. Lower values are useful for local development or intentional degraded
operation, but should be an explicit service decision.

## Production Posture

The following options are lifecycle or deployment policy, not hidden core
behavior:

- `common.cache.diagnostics.min_ready_peers`: service readiness threshold.
- `common.cache.diagnostics.self_check`: startup self-dial of the advertised
  gRPC control-plane address. Enable only when the runtime can route from a node
  back to its advertised address.
- `common.cache.diagnostics.fail_fast`: fail `Start` when self-check fails.
  Pair with `self_check` only in runtimes where self-dial is valid.
- `common.cache.cluster.tls.enabled`: encrypt gRPC control-plane transport.
- `common.cache.cluster.tls.require_client_cert`: require mTLS for peer control
  RPCs.
- `common.cache.metrics`: optional Prometheus export. Core diagnostics remain
  available through `Status()` even when metrics are disabled.

Keep peer refresh, replication retry, read repair, churn grace, tombstone
retention, and memberlist gossip diagnostics enabled unless you have a specific
reason to tune them. Those mechanisms are part of the cache's operational
resilience.

## Security

Production deployments should set the same `SharedKey` on every cache node. If
no key is configured and insecure mode is not enabled, each process generates an
ephemeral internal key and logs that fact without logging the value. That keeps a
single process authenticated by default, but different nodes with independently
generated keys will not authenticate with each other.

Use `RequireSharedKey` or `common.cache.diagnostics.require_shared_key` when a
deployment should fail startup unless the shared key is explicitly configured.
Use `AllowInsecure` or `CACHE_ALLOW_INSECURE=true` only for local development
when you intentionally want to run without a key.

Use TLS or mTLS when the private runtime network is not enough for the service's
threat model. TLS settings live under `common.cache.cluster.tls`.
