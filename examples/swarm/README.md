# Swarm Example

This example runs a small replicated app using the in-process cache module in Docker Swarm. Each app replica is also a cache node; there is no separate cache data-plane service.

## Build

```bash
# from repo root
docker build -t distributed-cache-example-app:latest -f examples/swarm/app/Dockerfile .
```

For a multi-node Swarm, push the image to a registry reachable by every worker and update `image:` in `docker-stack.yml`.

## Deploy

```bash
cd examples/swarm
docker stack deploy -c docker-stack.yml example
```

## Inspect

```bash
docker service ls

docker service logs -f example_app
```

You should see cache reads printing values across tasks.

## Notes

- The cache control-plane uses the `cache_control` overlay network which is internal and encrypted.
- The app uses `tasks.app` for gossip seed discovery so app replicas can find each other.
- `CACHE_NODE_NAME` uses Swarm task name for uniqueness.
- The app chooses a non-loopback container IP for `AdvertiseAddr` unless `CACHE_ADVERTISE_ADDR` is set.
- One replica writes after a startup delay; all replicas read through the public in-process API.
- `CACHE_TOMBSTONE_TTL_MS` configures the stale-delete protection window.
