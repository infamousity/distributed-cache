# Changelog

## [0.3.0](https://github.com/infamousity/distributed-cache/compare/v0.2.3...v0.3.0) (2026-07-27)


### Features

* **cache:** add explicit all write concern ([10518e3](https://github.com/infamousity/distributed-cache/commit/10518e37de4e582f1a7aa60eda92127a0f0f0265))
* **swarm:** verify write concern rollouts ([05e2c46](https://github.com/infamousity/distributed-cache/commit/05e2c46827b5023dad2f97d1e2cc6e25bb4e3ebb))


### Bug Fixes

* **build:** exclude local secrets from Docker contexts ([0b57d1c](https://github.com/infamousity/distributed-cache/commit/0b57d1ccec31516628f800ddd06b3ec6072174ce))
* **build:** run maintained images as non-root ([df94d06](https://github.com/infamousity/distributed-cache/commit/df94d06fa6310c850e8318ca1d92937b4331faf3))
* **cache:** centralize repair metadata ownership ([93d1d8d](https://github.com/infamousity/distributed-cache/commit/93d1d8d90bb0374689c4c9d9b8648663859b31ff))
* **cache:** drain post-quorum replication on close ([2216bc1](https://github.com/infamousity/distributed-cache/commit/2216bc1eab70507d35560861edaa6b25f1f434bf))
* **cache:** gate routing on current peer identity ([d3d00a4](https://github.com/infamousity/distributed-cache/commit/d3d00a40b12d9da0003c6a7f8e266037abad77e0))
* **cache:** harden consistency and admission semantics ([789700e](https://github.com/infamousity/distributed-cache/commit/789700ef84776b59fe051c2706a4680051003036))
* **cache:** reject negative entry TTLs ([41c4c90](https://github.com/infamousity/distributed-cache/commit/41c4c902fa7b6a4b28a8e8fde266e35c49125ae4))
* **cache:** reject stale versions and reclaim metadata ([cdd73db](https://github.com/infamousity/distributed-cache/commit/cdd73dbe5725c218cc05539ed413e9c7f167a5b2))
* **cluster:** preserve ring hash with upstream ristretto ([cfb746b](https://github.com/infamousity/distributed-cache/commit/cfb746b3d1ffe8a2183baedd24803150b9ad2b94))
* **config:** fail closed on unsafe runtime configuration ([7ea7d89](https://github.com/infamousity/distributed-cache/commit/7ea7d8919ff635d43dbbc0e2fcf0be86dc165fe7))
* **config:** validate production startup settings ([ddfc52d](https://github.com/infamousity/distributed-cache/commit/ddfc52d4b012c7c5b91ac784ac70c522e5758822))
* **log:** format error messages before emission ([a52fbcc](https://github.com/infamousity/distributed-cache/commit/a52fbcc1f39302c37b6aaffafc9437b3b42e7c89))


### Refactoring

* **cache:** isolate coordinator lifecycle state ([f5c6100](https://github.com/infamousity/distributed-cache/commit/f5c61008a0cdfbff8f05a34f231d23cf88950dda))
* **cluster:** remove unused internal state ([19670dc](https://github.com/infamousity/distributed-cache/commit/19670dc3aa0f9d4aaa4f553aa220a268f02f0e2d))

## [0.2.3](https://github.com/infamousity/distributed-cache/compare/v0.2.2...v0.2.3) (2026-06-17)


### Bug Fixes

* **cache:** suppress post-quorum cancellation retries ([3dd6d58](https://github.com/infamousity/distributed-cache/commit/3dd6d58c20c3d72878f91021b3487a31dcd4c993))

## [0.2.2](https://github.com/infamousity/distributed-cache/compare/v0.2.1...v0.2.2) (2026-06-17)


### Bug Fixes

* **config:** constrain advertise addresses to peer cidrs ([bc19cdc](https://github.com/infamousity/distributed-cache/commit/bc19cdc0c89f134e437ccfa87078497b7e4e5c7b))

## [0.2.1](https://github.com/infamousity/distributed-cache/compare/v0.2.0...v0.2.1) (2026-06-17)


### Bug Fixes

* **config:** filter swarm peer discovery by network cidr ([8cf2542](https://github.com/infamousity/distributed-cache/commit/8cf2542e0369d6bc976e3dc7734331b921971f23))

## [0.2.0](https://github.com/infamousity/distributed-cache/compare/v0.1.3...v0.2.0) (2026-06-17)


### Features

* **config:** expose public config startup path ([1b6d60d](https://github.com/infamousity/distributed-cache/commit/1b6d60d28bee315de904d75725bef36b00719d77))
* **config:** normalize memberlist advertise endpoints ([7cda3ec](https://github.com/infamousity/distributed-cache/commit/7cda3ecbd002a3731c9a49b593f6e7cad11f5d96))

## [0.1.3](https://github.com/infamousity/distributed-cache/compare/v0.1.2...v0.1.3) (2026-06-16)


### Bug Fixes

* **cache:** trigger bounded repair on peer verification ([3b5f130](https://github.com/infamousity/distributed-cache/commit/3b5f130dd18a92a1641259b8e87c2d5bfa8c20c6))

## [0.1.2](https://github.com/infamousity/distributed-cache/compare/v0.1.1...v0.1.2) (2026-06-16)


### Features

* support multiple peer DNS names ([9fb6845](https://github.com/infamousity/distributed-cache/commit/9fb68453af56b67df2c59301eea80887228db32e))

## [0.1.1](https://github.com/infamousity/distributed-cache/compare/v0.1.0...v0.1.1) (2026-06-11)


### Bug Fixes

* **deps:** update grpc for CVE-2026-33186 ([ead68e2](https://github.com/infamousity/distributed-cache/commit/ead68e229a6de0a84bad5b3bcda5f22cd3725795))
