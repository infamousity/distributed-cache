# Examples

These examples all embed the cache in an application process. There is no
separate cache data-plane service. The same runtime contract applies in every
example:

- every cache node needs a unique node name
- every node needs memberlist gossip bind settings
- clustered nodes need a peer-reachable memberlist advertise IP and port
- clustered nodes need a peer-reachable gRPC control-plane advertise address
- peers must be discovered through `PeerNodes` or DNS peer refresh
- all nodes in one cluster must use the same `SharedKey`

Bind addresses are where the process listens. Advertise addresses are what
other peers dial. Local examples can usually bind and advertise `127.0.0.1`.
Container and orchestrated examples usually bind `0.0.0.0` and advertise a
container, task, or pod IP.

## Single instance

```bash
go run ./examples/single
```

Expected output:

```
found=true value=world
```

This is the smallest embedding example. It uses loopback bind addresses,
no peer discovery, and a hard-coded development shared key. It is useful for
seeing the in-process API shape, not for proving replication.

## Multiple instances (local)

Terminal 1:

```bash
go run ./examples/multi -name node-1 -control-port 9090 -gossip-port 8946 -writer
```

Terminal 2:

```bash
go run ./examples/multi -name node-2 -control-port 9091 -gossip-port 8947 -peers 127.0.0.1:8946
```

Both nodes wait briefly for gossip membership, then `node-1` writes through the in-process cache API. If the key is owned by another node, the write is forwarded with owner-assigned versioning. Both terminals should then print the replicated value.

Notes:
- Ports must be unique per node when running locally.
- `-peers` takes memberlist gossip addresses as `host:port` entries.
- Because this example uses loopback addresses, the bind addresses are also
  peer-reachable advertise addresses.
- `SharedKey` is hard-coded in the example and must match across nodes.
- `TombstoneTTL` is set explicitly in the example to show the stale-delete protection window.

## Docker Swarm

See `examples/swarm/README.md`.

The Swarm example demonstrates DNS peer refresh through `tasks.<service>`,
task-derived node names, overlay task IP advertisement, private gossip/control
ports, and an internal proof harness that avoids host-published cache ports.

## Kubernetes

See `examples/kubernetes/README.md`.

The Kubernetes example demonstrates headless Service peer discovery,
StatefulSet pod identity, `status.podIP` memberlist advertisement, pod-local
control-plane advertisement, a shared-key Secret, and a NetworkPolicy that keeps
cache internals private.
