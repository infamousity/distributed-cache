# Contributing

## Releases And Prereleases

This repository is a Go module. Release and prerelease tags must be valid Go
module versions that can be used directly after `@` in a `go get` command:

```bash
go get github.com/infamousity/distributed-cache@v0.1.1
```

### Stable Releases

Stable releases are cut from `main` and use plain root-module tags:

```text
v0.1.0
v0.1.1
v0.2.0
```

Do not use component-prefixed tags such as
`github.com/infamousity/distributed-cache-v0.2.0`; those are not root Go module
tags and Go will not resolve them as `@v0.2.0`.

Release automation is handled by Release Please:

- `release-please` runs only on pushes to `main`.
- Release Please opens or updates a release PR when conventional commits on
  `main` imply a new version.
- Merging that release PR creates the release commit, tag, changelog update,
  and GitHub Release.
- `release-please-config.json` sets `include-component-in-tag: false` so tags
  remain Go-friendly root module tags.

The `.release-please-manifest.json` file records the last released stable
version. It should only move as part of an intentional release.

### Feature Prereleases

Feature branches can publish prerelease tags that are safe to reference from
other Go projects before the feature is merged.

Only branches under `feature/` publish prerelease tags:

```bash
git switch -c feature/write-through-cache
git push -u origin feature/write-through-cache
```

The `feature/` prefix is stripped before the prerelease tag is built. The
remaining branch name must already be a valid SemVer prerelease identifier:

- use only ASCII letters, digits, hyphens, and dots
- each dot-separated segment must be non-empty
- each segment must start with a letter or digit
- numeric-only segments must not have leading zeroes

Good branch names:

```text
feature/write-through-cache
feature/write-through-cache.2
feature/multi-node-repair
```

Bad branch names:

```text
feature/write_through_cache
feature/write-through/cache
feature/.write-through-cache
feature/write-through-cache.
feature/write-through-cache.01
```

On each push to a valid `feature/*` branch, the `feature prerelease` workflow:

1. Checks out the full tag history.
2. Runs `go test ./...`.
3. Runs `go vet ./...`.
4. Finds the latest stable tag matching `vMAJOR.MINOR.PATCH`.
5. Computes the next patch version.
6. Creates an immutable annotated prerelease tag for the pushed commit.

For example, if the latest stable release is:

```text
v0.1.1
```

then a push to:

```text
feature/write-through-cache
```

creates a tag shaped like:

```text
v0.1.2-write-through-cache.20260611170530.abc1234def56
```

That version can be consumed from another Go module:

```bash
go get github.com/infamousity/distributed-cache@v0.1.2-write-through-cache.20260611170530.abc1234def56
```

Prerelease tags are intentionally unique per commit and must not be moved. If a
feature branch changes, push another commit and let the workflow create another
prerelease tag.
