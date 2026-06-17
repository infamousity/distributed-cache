# Swarm Example

This example runs a small replicated app using the in-process cache module in Docker Swarm. Each app replica is also a cache node; there is no separate cache data-plane service.

The goal is to prove the generic runtime contracts used by the library:

- each task has a unique node name
- each task advertises a peer-reachable gossip/control address
- peer discovery uses DNS peer refresh
- peers verify each other through the gRPC control plane
- reads continue through task churn

The core cache code does not depend on Docker Swarm. Swarm is just one runtime profile that satisfies those contracts.

The generic runtime contract is documented in the root `README.md`, with
additional deployment notes in `docs/runtime-profiles.md`. This example only
covers the Swarm-specific profile: `tasks.<service>` DNS peer discovery,
task-scoped node names, and an internal overlay network for memberlist gossip,
gRPC control-plane traffic, and example-only harness probes. For peers spread
across multiple connected stacks, set `CACHE_CLUSTER_MEMBERLIST_PEER_DNS_NAMES`
to a comma-separated list of `tasks.<service>` names.

## Files

- `docker-stack.yml`: example Swarm stack with three app/cache replicas and one internal harness replica.
- `app/main.go`: sample app embedding the cache and logging reads/writes.
- Harness HTTP endpoints exposed by the sample app: `/mark`, `/set`, `/get`, `/del`, `/status`.
- `app/Dockerfile`: image build for the sample app.
- `chaos.sh`: example-stack proof harness for deploy, scale churn, and forced update.

## Prerequisites

- Docker CLI with access to a Swarm manager.
- An initialized Swarm:

```bash
docker --context default swarm init
```

Use a different Docker context by setting `DOCKER_CONTEXT`. The proof script passes `--context` explicitly on every Docker command:

```bash
DOCKER_CONTEXT=my-swarm ./chaos.sh
```

For a single-node local Swarm, the locally built image is enough. For a multi-node Swarm, push the image to a registry reachable by every worker and update `image:` in `docker-stack.yml`.

If you intentionally want to run the example on only one Swarm node, set a placement constraint. This is useful when testing against a multi-node Swarm where the example image exists only on the manager/build node:

```bash
PLACEMENT_CONSTRAINT='node.hostname == swarm-node-1' ./chaos.sh
```

## Build

From the repository root:

```bash
docker --context default build -t distributed-cache-example-app:latest -f examples/swarm/app/Dockerfile .
```

For multi-node Swarm:

```bash
docker --context default tag distributed-cache-example-app:latest registry.example.com/distributed-cache-example-app:latest
docker --context default push registry.example.com/distributed-cache-example-app:latest
```

Then set the same image in `docker-stack.yml`.

## Deploy

```bash
cd examples/swarm
docker --context default stack deploy -c docker-stack.yml example
```

Inspect service state:

```bash
docker --context default service ls
docker --context default service ps example_app
docker --context default service logs -f example_app
```

You should see one task write `swarm-key` and all tasks repeatedly read it:

```text
node=example_app.1... wrote key=swarm-key value=from-example_app.1...
node=example_app.2... found=true value=from-example_app.1...
```

## Proof Harness

Run:

```bash
cd examples/swarm
DOCKER_CONTEXT=default ./chaos.sh
```

The script does the following:

- verifies the selected Docker context is attached to an active Swarm node
- builds the example image
- deploys the `example` stack
- waits for three running `example_app` tasks
- executes harness HTTP requests from the internal `example_harness` service on
  the same private overlay
- marks each proof phase through the example harness HTTP surface
- waits for the phase marker value to become readable through the harness HTTP
  surface
- performs active HTTP set/get/delete assertions
- scales the service from `3 -> 1`
- scales the service from `1 -> 3`
- forces a rolling update
- waits for the forced rolling update to report completion
- checks structured status for unreachable or identity-mismatched peers
- warns or fails on recent memberlist gossip transport degradation, depending on
  `GOSSIP_DEGRADATION_MODE`

On failure, it prints service status, task status, and recent service logs.

The script is intentionally example-specific. It is not a generic production validation tool.

## Useful Options

```bash
DOCKER_CONTEXT=default ./chaos.sh
STACK_NAME=dcache ./chaos.sh
STACK_FILE=/path/to/docker-stack.yml ./chaos.sh
STACK_SERVICE_NAME=web ./chaos.sh
SERVICE_NAME=dcache_web ./chaos.sh
HARNESS_SERVICE_NAME=dcache_harness ./chaos.sh
PRINT_CONFIG=1 ./chaos.sh
REPLICAS=5 ./chaos.sh
WAIT_SECONDS=180 ./chaos.sh
STEADY_SECONDS=15 ./chaos.sh
DEBUG_LOG_LINES=200 ./chaos.sh
CLEANUP=1 ./chaos.sh
IMAGE=registry.example.com/distributed-cache-example-app:latest ./chaos.sh
PLACEMENT_CONSTRAINT='node.hostname == swarm-node-1' ./chaos.sh
HARNESS_TRANSPORT=service ./chaos.sh
GOSSIP_DEGRADATION_MODE=fail ./chaos.sh
```

`CLEANUP=1` removes the stack when the script exits. `DEBUG_LOG_LINES` controls
how many Docker service log lines are printed only when the harness fails; logs
are not used for normal assertions.
`STACK_FILE` selects the stack manifest to deploy. Relative paths are resolved
from the current directory when they exist, then from this `examples/swarm`
directory. `STACK_SERVICE_NAME` is the service name inside the stack file and
defaults to `app`, so the Swarm service defaults to
`${STACK_NAME}_${STACK_SERVICE_NAME}`. Set `SERVICE_NAME` only when you need to
target a full precomputed Swarm service name.
`PRINT_CONFIG=1` prints the resolved Docker context, stack file, stack name,
stack service name, full Swarm service name, harness service name, harness
transport, internal harness URL, and image without building or deploying
anything.
`STEADY_SECONDS` controls the quiet period before the final steady-state gossip
check.
If `HARNESS_URL` is not set, the default `HARNESS_TRANSPORT=service` executes
curl inside the stack's internal harness service and calls
`HARNESS_INTERNAL_URL`, which defaults to `http://${SERVICE_NAME}:8080`. This
keeps harness traffic on the private `cache_control` overlay and avoids
publishing host ports from an internal network. Use `HARNESS_TRANSPORT=host`,
`ssh`, or `ssh-exec` only as explicit debug fallbacks when you intentionally
publish and expose the app harness HTTP port outside the overlay.
`GOSSIP_DEGRADATION_MODE` accepts `off`, `warn`, or `fail`; the default is
`warn`. This check is separate from active cache correctness. It compares
`Status().Gossip.DegradedTotal` before and after each proof phase. Gossip
degradation during deploy, scale, and forced update is classified as intentional
churn when the expected topology is restored and active cache operations pass.
The final steady-state phase uses normal `warn` or `fail` behavior because no
membership change is expected. Gossip counters are node-local, so the harness
compares before/after diagnostics from the same reported cache node.

## API Boundary

This example embeds the cache inside the app service. The library cache API is
Go/in-process only.

The HTTP routes in this example are internal harness endpoints. They let the
proof script drive writes, reads, deletes, and phase markers through Swarm
service DNS from inside the private example overlay. They are not published by
the stack and are not part of the cache library API.

Production services may expose cache-backed routes if that is part of their
service contract. Those routes belong to the host service and must use the
service's authn, authz, tenant isolation, validation, rate limiting, and audit
model.

The cache control plane is private node-to-node infrastructure. Do not publish
the gossip or gRPC control-plane ports externally.

## Runtime Settings

The stack uses:

```yaml
CACHE_RUNTIME: swarm
CACHE_SWARM_SERVICE_NAME: "{{.Service.Name}}"
CACHE_NODE_NAME: "{{.Task.Name}}"
CACHE_HARNESS_HTTP_BIND_ADDR: ":8080"
CACHE_CONTROL_BIND_ADDR: "0.0.0.0"
CACHE_CONTROL_BIND_PORT: 9090
CACHE_GOSSIP_BIND_ADDR: "0.0.0.0"
CACHE_GOSSIP_BIND_PORT: 8946
CACHE_PEER_DNS_PORT: 8946
CACHE_SHARED_KEY: dev-shared-key
CACHE_TOMBSTONE_TTL_MS: 300000
CACHE_VALUE_TTL_MS: 600000
CACHE_DIAGNOSTICS_MIN_READY_PEERS: 2
```

Important details:

- The example derives Swarm task-level DNS from `CACHE_RUNTIME=swarm` and
  `CACHE_SWARM_SERVICE_NAME`.
- `../../config.swarm.example.yml` shows the equivalent config-file profile
  using `peerDNSName` for peer discovery and `advertise_addr: auto` for
  memberlist advertisement.
- The app chooses a container IP from
  `CACHE_CLUSTER_MEMBERLIST_PEER_NETWORK_CIDRS` when set, otherwise it can derive
  one from `CACHE_PEER_DNS_NAME` for simple one-overlay examples. Set the peer
  network CIDR when a task is attached to more than one overlay network.
- The `cache_control` network is internal, encrypted, and not attachable. The
  proof harness runs as a Swarm service on that overlay instead of creating
  one-shot containers or publishing host ports.
- Memberlist gossip uses the configured gossip port for TCP and UDP traffic
  between tasks.
- The gRPC control plane uses the configured control-plane TCP port for
  node-to-node cache RPCs.
- The gossip and gRPC control-plane ports should stay on private/internal
  networks and should not be published externally.
- `CACHE_DIAGNOSTICS_MIN_READY_PEERS=2` means each replica expects both other
  replicas to be verified before `Ready(ctx)` reports healthy in this three-node
  example.
- The first replica retries the example write until it succeeds. This keeps the proof focused on cache convergence instead of failing permanently if the one writer starts during topology churn.
- The harness HTTP endpoint is example-only. It exists so the proof harness can issue active cache commands and phase markers; it is not part of the library API.

## Harness HTTP

The app exposes its example-only harness HTTP surface on port `8080` inside the
private `cache_control` overlay. The stack does not publish this port. The
default proof harness reaches that surface from the internal `harness` service:

```bash
DOCKER_CONTEXT=default HARNESS_TRANSPORT=service ./chaos.sh
```

For ad hoc debugging, execute a request from the harness service container:

```bash
docker --context default exec <harness-container> \
  curl -fsS 'http://example_app:8080/status'
```

The app does not publish the harness HTTP port by default. If you intentionally
add a published port for debugging, the proof script still supports the older
host-oriented transports:

```bash
DOCKER_CONTEXT=default HARNESS_TRANSPORT=host ./chaos.sh
DOCKER_CONTEXT=default HARNESS_TRANSPORT=ssh ./chaos.sh
DOCKER_CONTEXT=default HARNESS_TRANSPORT=ssh-exec ./chaos.sh
```

Requests land on the replica running on the selected task node. The app then
uses the in-process distributed cache API, so this exercises forwarded owner
routing as well as local reads.

## What This Proves

This proves the example stack can:

- discover peers through DNS peer refresh
- verify peers through the control plane
- replicate the example key
- accept active set/get/delete harness commands through any service replica
- keep reads working through scale churn
- recover through forced task replacement

## What This Does Not Prove Yet

The current proof harness proves ordinary delete visibility through the active harness HTTP surface. It does not yet prove tombstone safety while deliberately injecting stale older-version writes.

To prove delete safety next, we need one of these:

- add a scripted scenario mode to the example app
- add a second test app/job that joins the same cache cluster and issues writes/deletes

That design should be discussed before expanding the example because it affects how much active harness surface we want in the sample app.

## Cleanup

```bash
docker --context default stack rm example
```
