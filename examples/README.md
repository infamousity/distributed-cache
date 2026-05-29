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
go run ./examples/multi -name node-1 -control-port 9090 -gossip-port 8946
```

Terminal 2:

```bash
go run ./examples/multi -name node-2 -control-port 9091 -gossip-port 8947 -seeds 127.0.0.1:8946
```

You should see `node-2` read the value written by `node-1` via the control-plane.

Notes:
- Ports must be unique per node when running locally.
- `SharedKey` is hard-coded in the example and must match across nodes.
