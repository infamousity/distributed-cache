# Examples

## Single instance

```bash
go run ./examples/single
```

Expected output:

```
found=true value=world
```

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
- `SharedKey` is hard-coded in the example and must match across nodes.
- `TombstoneTTL` is set explicitly in the example to show the stale-delete protection window.
