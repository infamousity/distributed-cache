# Changelog

## [0.2.0](https://github.com/infamousity/distributed-cache/compare/github.com/infamousity/distributed-cache-v0.1.0...github.com/infamousity/distributed-cache-v0.2.0) (2026-06-11)


### ⚠ BREAKING CHANGES

* rename seed discovery to peer discovery

### Features

* add distributed cache tombstone versioning ([8cffc7c](https://github.com/infamousity/distributed-cache/commit/8cffc7cfcdf5af2d31e0aa3a973394a86d328d02))
* derive peer dns from swarm metadata ([3ea442c](https://github.com/infamousity/distributed-cache/commit/3ea442cf100a99d72b2b4537d89096cb7fd2b348))
* **examples:** probe swarm chaos through internal harness ([d5b4b87](https://github.com/infamousity/distributed-cache/commit/d5b4b87fc6055d025960a5144d161ec3c2fd0b1a))
* generate internal shared key by default ([cbdd41c](https://github.com/infamousity/distributed-cache/commit/cbdd41cf23ff73483e29219ee760016f8ac3b8dd))
* harden distributed cache runtime ([c41d19a](https://github.com/infamousity/distributed-cache/commit/c41d19abf3d37caba52c1a98fdd8cd6363c4579b))
* rename seed discovery to peer discovery ([1fb611d](https://github.com/infamousity/distributed-cache/commit/1fb611dd536d02f3e8e4e6ee82177472d4fe230b))


### Bug Fixes

* **cache:** reject stale forwarded owners as not ready ([71f7c44](https://github.com/infamousity/distributed-cache/commit/71f7c44e88161adcb81c2f6cd70f865f8285fcf6))
