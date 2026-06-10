# Kubernetes Example

This example shows the Kubernetes shape for a service that embeds the cache. It
is intentionally a deployment profile, not a new cache server mode.

The important pieces are:

- a headless Service for pod-level DNS peer discovery
- a StatefulSet for stable pod names
- pod IP advertisement for memberlist gossip and gRPC control-plane traffic
- a Secret for the shared key
- a NetworkPolicy that allows cache node-to-node traffic but does not expose
  gossip or gRPC control-plane ports externally

## Files

- `manifests.yaml`: example Secret, headless Service, StatefulSet, and
  NetworkPolicy.

## Apply

Review and replace the placeholder image and shared key first:

```bash
kubectl apply -f examples/kubernetes/manifests.yaml
```

The example uses namespace `default`. If you deploy into a different namespace,
update `CACHE_CLUSTER_MEMBERLIST_PEER_DNS_NAME` in the StatefulSet.

## Runtime Notes

`cache-peers` is a headless Service. Its DNS name resolves to pod IPs, which is
what memberlist needs for peer discovery. A normal ClusterIP Service would give
the load-balanced service endpoint, not the individual cache nodes.

The manifest uses canonical config-loader env names:

- `CACHE_CLUSTER_MEMBERLIST_NODE_NAME`
- `CACHE_CLUSTER_MEMBERLIST_BIND_ADDRESS`
- `CACHE_CLUSTER_MEMBERLIST_BIND_PORT`
- `CACHE_CLUSTER_MEMBERLIST_ADVERTISE_ADDRESS`
- `CACHE_CLUSTER_MEMBERLIST_ADVERTISE_PORT`
- `CACHE_CLUSTER_MEMBERLIST_PEER_DNS_NAME`
- `CACHE_CLUSTER_MEMBERLIST_PEER_DNS_PORT`
- `CACHE_API_BIND_ADDR`
- `CACHE_API_BIND_PORT`
- `CACHE_CONTROL_ADVERTISE_ADDR`
- `CACHE_SHARED_KEY`

If your host service constructs `cache.Options` directly instead of using the
repository config loader, map these same values into your own option-building
code.

## Network Boundary

The NetworkPolicy allows:

- TCP and UDP `8946` between cache pods for memberlist
- TCP `9090` between cache pods for the gRPC control plane

It does not expose those ports externally. Any HTTP/JSON/GraphQL/etc. endpoint
that uses the cache belongs to the host service and should be exposed through a
separate Service and the host service's normal authn/authz model.

## Readiness

Wire application readiness to `WaitReady(ctx)` when the service needs a minimum
cluster shape before serving traffic. Configure the threshold with
`CACHE_DIAGNOSTICS_MIN_READY_PEERS`.
