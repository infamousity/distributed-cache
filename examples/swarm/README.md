# Swarm Example

This example runs a small app using the in-process cache module in Docker Swarm.

## Build

```bash
# from repo root
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
- The app uses `tasks.app` for gossip seed discovery so nodes can find each other.
- `CACHE_NODE_NAME` uses Swarm task name for uniqueness.
