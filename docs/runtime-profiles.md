# Runtime Profiles

This cache is an embedded Go library. It does not depend on a specific
orchestrator, but every runtime must satisfy the same cluster contract.

## Common Contract

Each cache node needs:

- a stable, unique `NodeName`
- a bind address and port for memberlist gossip
- a peer-reachable memberlist gossip advertise IP and port
- a bind address and port for the gRPC control plane
- a peer-reachable gRPC control-plane advertise `host:port`
- at least one peer discovered through static `PeerNodes` or DNS peer refresh
- private node-to-node network reachability for memberlist and gRPC traffic
- the same `SharedKey` on every node

Bind addresses answer "where should this process listen?" Advertise addresses
answer "what address should peers use to reach this process?" In simple VM or
host-networked deployments those can be the same address. In Swarm,
Kubernetes, Podman networks, NAT, and multi-interface hosts, bind and advertise
often differ.

Memberlist requires its advertise address to be an IP address. DNS names are
valid for peer discovery and for the gRPC control-plane advertise address, but
not for memberlist gossip advertisement. `memberlist.advertise_addr` accepts an
IP or `IP:port`; endpoint form is split internally into memberlist's address
and port fields.

When using config files, the repository config loader can derive local IPs with
template helpers:

- `{{ ip }}` selects a local private interface using
  `bind_address_filter` and `bind_interface_priority`
- `{{ peerDNSName }}` returns explicit peer DNS env or, in Swarm, derives
  `tasks.<service>` from `CACHE_RUNTIME=swarm` and
  `CACHE_SWARM_SERVICE_NAME`
- `{{ peerIP }}` selects this node's local IPv4 address on the same subnet as
  the peer DNS records
- `{{ peerAddr <port> }}` returns `{{ peerIP }}:<port>` and is the preferred
  helper for endpoint-shaped advertise fields

If `memberlist.advertise_addr` is omitted, memberlist falls back to the
memberlist bind address. That is only correct when the bind address is a
specific peer-reachable IP. If the bind address is `0.0.0.0`, set
`memberlist.advertise_addr` explicitly or use a template helper/runtime field
that derives the task or pod IP.

If `api.advertise_addr` is omitted, peers use the control-plane bind address
and port where possible. Set it explicitly when the bind address is wildcard,
NATed, container-local, or otherwise not the address peers should dial.

Config files merge from left to right. Keep portable defaults in a base config,
then layer runtime-specific overrides and secrets after it.

Important defaults:

- `size_bytes`: `1073741824` bytes
- `replication_factor`: `3`
- `write_concern`: `one`
- `retry.interval_ms`: `500`
- `retry.max_attempts`: `3`
- `retry.queue_size`: `1024`
- `repair.interval_ms`: `30000`
- `repair.max_keys_per_cycle`: `1000`
- `churn.grace_period_ms`: `30000`
- `peers.refresh_interval_ms`: `30000`
- `tombstone_ttl_ms`: `300000`
- `diagnostics.self_check_timeout_ms`: `1000`
- `diagnostics.peer_warn_interval_ms`: `10000`

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

Go services that opt into this repository's config shape can call
`config.Load(...)` from `github.com/infamousity/distributed-cache/config` and
then pass the result to `cache.StartFromConfig(cfg)`. Services that already own
their config system can skip this package and call `cache.Start(cache.Options{})`
directly.

## Docker Swarm

Swarm works well when each service task is also a cache node.

Use:

- `CACHE_CLUSTER_MEMBERLIST_NODE_NAME="{{.Task.Name}}"` for a unique node name
- `CACHE_CLUSTER_MEMBERLIST_PEER_DNS_NAME=tasks.<service>` for task-level DNS
  peer discovery in one service
- `CACHE_CLUSTER_MEMBERLIST_PEER_DNS_NAMES=tasks.auth,tasks.user` when peers
  are split across multiple connected stacks or services
- `CACHE_CLUSTER_MEMBERLIST_PEER_DNS_PORT=<gossip-port>`
- `CACHE_CLUSTER_MEMBERLIST_BIND_ADDRESS=0.0.0.0`
- `CACHE_CLUSTER_MEMBERLIST_BIND_PORT=<gossip-port>`
- `CACHE_API_BIND_ADDR=0.0.0.0`
- `CACHE_API_BIND_PORT=<control-port>`
- `CACHE_CLUSTER_MEMBERLIST_ADVERTISE_ADDRESS=<task-reachable-ip>` when you want
  to set the gossip advertise IP explicitly; config-file users can set
  `advertise_addr: auto`
- `CACHE_CLUSTER_MEMBERLIST_ADVERTISE_PORT=<gossip-port>`
- `CACHE_CLUSTER_MEMBERLIST_PEER_NETWORK_CIDRS=<cache-overlay-cidr>` when tasks
  are attached to more than one overlay network
- `CACHE_CONTROL_ADVERTISE_ADDR=<task-reachable-host>:<control-port>` when the
  control plane must advertise something different from the memberlist advertise
  IP plus `CACHE_API_BIND_PORT`

Network expectations:

- put cache traffic on an internal overlay network
- enable overlay encryption when available
- allow TCP and UDP gossip traffic between tasks
- allow TCP gRPC control-plane traffic between tasks
- do not publish gossip or gRPC control-plane ports

Swarm DNS note: `tasks.<service>` returns task IPs instead of the service VIP.
That is the right peer discovery shape because peers must discover
individual cache nodes, not only the load-balanced service endpoint.

If services are attached to multiple overlay networks, the same `tasks.<service>`
name can return one task IP per shared overlay. Configure
`peer_network_cidrs`/`CACHE_CLUSTER_MEMBERLIST_PEER_NETWORK_CIDRS` with the cache
overlay subnet in that case. The filter is applied to DNS peer discovery before
memberlist join, and the same filter constrains this node's memberlist advertise
IP. If `advertise_addr` is omitted, the library derives it from
`peer_network_cidrs`; `advertise_addr: auto` does the same explicitly. If
memberlist `advertise_addr` or an IP-shaped `api.advertise_addr` is set outside
those CIDRs, startup fails. Without the filter, `advertise_addr: auto` only
succeeds when peer DNS maps to exactly one local network; ambiguous multi-overlay
matches fail at startup.

### Choosing `peer_network_cidrs`

`peer_network_cidrs` answers one question: which network plane is
distributed-cache allowed to use for peer discovery and node-to-node traffic? It
should be the subnet or subnets for networks that all intended cache peers can
use to dial each other. It should not be every subnet attached to the container.

The easiest production answer is to make the cache network explicit in the stack
file and reuse that same subnet in distributed-cache config:

```yaml
networks:
  cache_control:
    driver: overlay
    internal: true
    attachable: false
    ipam:
      config:
        - subnet: 10.60.0.0/24
```

```yaml
common:
  cache:
    cluster:
      memberlist:
        advertise_addr: auto
        peer_network_cidrs:
          - 10.60.0.0/24
```

This removes guesswork. The config developer reads the cache overlay IPAM block
and copies that subnet into `peer_network_cidrs`.

If the overlay subnet is assigned by Docker instead of declared in the stack,
inspect the network from a Swarm manager:

```bash
docker network inspect <stack>_cache_control \
  --format '{{range .IPAM.Config}}{{println .Subnet}}{{end}}'
```

For an existing shared internal discovery network, such as a Traefik/proxy
overlay that every relevant service already joins, inspect that network instead:

```bash
docker network inspect traefik-public \
  --format '{{range .IPAM.Config}}{{println .Subnet}}{{end}}'
```

Using a shared internal discovery overlay can work when it is the intended trust
boundary, but a dedicated cache overlay is preferred because it limits who can
reach memberlist gossip and the gRPC control plane. If the chosen shared network
is broader than the cache participants, configure shared-key/TLS policy
accordingly and keep cache ports unpublished.

To verify the value from inside a running task, inspect the task container's
network attachments on its node:

```bash
docker ps --filter label=com.docker.swarm.service.name=<stack>_<service>
docker inspect <container-id> --format '{{json .NetworkSettings.Networks}}'
```

The selected `peer_network_cidrs` should include the IP address for the intended
cache/discovery network and exclude unrelated overlays such as ingress, egress,
database, queue, or broad application networks. When multiple cache networks are
intentionally valid, include all of them. If this node is attached to more than
one configured CIDR, `advertise_addr: auto` is ambiguous; narrow the CIDRs for
that service or set `advertise_addr` explicitly to an IP inside one of those
CIDRs. If `api.advertise_addr` is also set to an IP address, it must be inside
the same selected cache network CIDRs; DNS control advertise names are accepted
because the library cannot know their runtime resolution in every platform.

When using config files, `peerDNSName`, `peerIP`, and `peerAddr` are
config-template functions evaluated by the repository config loader before YAML
is decoded. They are not env vars, YAML fields, or `cache.Options` members.
`peerDNSName` can fill `peer_dns_name` from explicit peer DNS env or from Swarm
runtime metadata. `peerAddr` then derives IP endpoint advertise addresses from
that effective peer DNS name. Memberlist advertise values must resolve to this
task's peer-reachable IP; DNS names are valid for peer discovery, not memberlist
advertisement. See `config.swarm.example.yml` for a complete image-oriented
profile.

```yaml
common:
  cache:
    api:
      bind_addr: "0.0.0.0"
      bind_port: 9090
    cluster:
      memberlist:
        node_name: '{{ env "CACHE_CLUSTER_MEMBERLIST_NODE_NAME" }}'
        bind_address: "0.0.0.0"
        bind_port: 8946
        advertise_addr: auto
        peer_dns_name: "{{ peerDNSName }}"
        peer_dns_names: []
        peer_dns_port: 8946
        peer_network_cidrs: []
```

`peerDNSName` prefers explicit `CACHE_CLUSTER_MEMBERLIST_PEER_DNS_NAME` or
`CACHE_PEER_DNS_NAME`. With `CACHE_RUNTIME=swarm`, it can derive
`tasks.<service>` from `CACHE_SWARM_SERVICE_NAME`. `peerIP` selects this node's
local IPv4 address on the same subnet as the resolved peer DNS records. It is
useful for simple deployments and explicit templates, but `advertise_addr: auto`
plus `peer_network_cidrs` is the safer Swarm shape when tasks can share multiple
overlay networks.

## Kubernetes

Kubernetes should use pod-level discovery, not Service load balancing, for cache
membership.

Use:

- a headless Service for peer DNS
- StatefulSet pod names when stable identity matters
- pod IPs as memberlist advertised addresses
- pod IPs or stable pod DNS names as gRPC control-plane advertised addresses
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
Downward API, so prefer `status.podIP` for memberlist advertise addresses
there. `peerIP` is still useful for config-file-driven runtimes where the local
peer network must be inferred from DNS.

Kubernetes usually does not need `peer_network_cidrs`: headless Service DNS
returns pod IPs, and `status.podIP` provides this pod's memberlist advertise IP
directly. Add `peer_network_cidrs` only when DNS can return addresses from
multiple routable pod networks, or when the workload has multiple network
attachments and only one is the cache traffic plane.

For ordinary Kubernetes clusters, the relevant subnet is the pod CIDR. Depending
on cluster permissions and distribution, a config developer can find it from one
of these sources:

```bash
kubectl get nodes \
  -o jsonpath='{range .items[*]}{.metadata.name}{"\t"}{.spec.podCIDR}{"\n"}{end}'
kubectl cluster-info dump | grep -i cluster-cidr
kubectl -n kube-system get configmap kube-proxy -o yaml | grep -i clusterCIDR
```

Many managed Kubernetes and Rancher-provisioned clusters hide or vary these
fields. In that case, prefer the normal Kubernetes shape: advertise `status.podIP`,
discover peers through a headless Service, and leave `peer_network_cidrs` unset
unless there is a concrete multi-network problem to solve.

For Multus or another multi-network CNI, use the CIDR assigned to the specific
network attachment that carries cache gossip/control traffic. The source of truth
is usually the CNI/IPAM section in the `NetworkAttachmentDefinition`:

```bash
kubectl get network-attachment-definition <name> -o yaml
```

Rancher does not change the distributed-cache rule. For RKE/RKE2/K3s clusters,
the answer is still the pod network CIDR or the Multus/cache attachment CIDR,
not the Service CIDR and not an ingress/load-balancer network.

## Podman, Docker Compose, Systemd, and VMs

Static deployments work with explicit peers or DNS records.

Use:

- `NodeName` set to a stable hostname or instance ID
- `PeerNodes` with `host:port` entries, or one or more DNS names that resolve
  to peers
- `AdvertiseAddr` set to an IP address other cache nodes can reach, optionally
  as `IP:port`
- `ControlAdvertiseAddr` set to the peer-reachable gRPC `host:port`; DNS is
  acceptable here when peers can resolve it
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
        advertise_addr: "10.10.1.10:8946"
        peer_nodes:
          - "cache-1.internal:8946"
          - "cache-2.internal:8946"
          - "cache-3.internal:8946"
```

Firewall expectations:

- allow TCP and UDP `8946` only from peer cache nodes
- allow TCP `9090` only from peer cache nodes
- deny external access to both ports

Single-network Compose and Podman setups normally do not need
`peer_network_cidrs`. It becomes useful when a container joins multiple networks
and DNS discovery can return addresses from more than one of them.

For Docker Compose, either declare the cache network subnet:

```yaml
networks:
  cache_control:
    ipam:
      config:
        - subnet: 172.30.10.0/24
```

or inspect the created network:

```bash
docker network inspect <project>_cache_control \
  --format '{{range .IPAM.Config}}{{println .Subnet}}{{end}}'
```

For Podman, inspect the network that carries cache traffic:

```bash
podman network inspect cache_control
```

Use the subnet from that network's IPAM config. As with Swarm, do not use every
network attached to the container; use only the network where distributed-cache
peers are supposed to discover and dial each other.

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
