# Development

## Building

```
go build ./...
go vet ./...
go test ./...
./scripts/check-trie-imports.sh   # fails if any L1-only trie API is referenced
```

## CI

`.github/workflows/ci.yml` runs on every push to `main` and on every pull request, on Linux and macOS. It checks out this repository at `zeno/zeno-executor` and go-zenon from `GO_ZENON_REPO` at `GO_ZENON_REF` at `zenon-network/go-zenon`, the layout the `go.mod` `replace` expects. It then runs the four commands under [Building](#building).

GitHub CI runs as non-root. Its Linux leg runs the key probe's `portable` and `linux` subtests (`TestWorkerCannotReachKey/{FILE,ENV}/{portable,linux}` in `cmd/executor/main_test.go`); the root-only tests (uid separation, cgroup limits, and the probe's `uid separated` subtests) skip there and run instead in the Docker test environment (`docker compose -f docker/compose.test.yml run --build --rm test`, which runs as root). The macOS leg runs the `portable` subtests too; its `linux` and `uid separated` subtests skip there with a named reason, since the `/proc` and ptrace attempts they exercise are Linux-only.

The worker file-mode test across uids (`TestModesUnderUIDSeparation` in `internal/recovery`) is root-only too and runs in the same Docker test environment. So do the gateway's storage-path tests that change an owner (`TestSecureLeaf`, `TestPrepareStorageAncestors`, `TestPrepareStorageSymlinkComponents`, `TestPrepareStorageRefusesForeignLeaf`, `TestOpenFinalRefusesForeignOwner`, `TestOpenFinalRefusesForeignLeaf`).

## Dependency on go-zenon

This module requires `github.com/zenon-network/go-zenon`. The module imports these go-zenon packages: `common/trie` (its path-native API), `common/types`, `common/crypto`, `chain/nom`, `vm/embedded/definition`, `wallet` and `dp`, and the devnet integration test also imports `vm/constants`; go-zenon never imports this module.

`go.mod` replaces the canonical path with a checkout of the [`digitalSloth/go-zenon`](https://github.com/digitalSloth/go-zenon) fork, on a branch carrying the path-native trie API and the Settlement contract:

```
replace github.com/zenon-network/go-zenon => ../../zenon-network/go-zenon
```

The path assumes this repository at `<root>/zeno/zeno-executor` and the fork at `<root>/zenon-network/go-zenon`. CI, the Docker build, the devnet script and the release workflow use the same layout.

The fork's `go.mod` still declares itself `module github.com/zenon-network/go-zenon`, so `go get github.com/digitalSloth/go-zenon@<ref>` fails the module-path check. To build against another go-zenon commit:

1. Check the commit out at `../../zenon-network/go-zenon`.
2. Run `go mod tidy` and the commands under [Building](#building).
3. Set `GO_ZENON_REF` in `.github/workflows/ci.yml` and `.github/workflows/zeno-exec-release.yml` to the commit's full hash.
4. Push the commit to `GO_ZENON_REPO` before pushing this repository.

Once Settlement lands in `zenon-network/go-zenon`, drop the `replace` and require `github.com/zenon-network/go-zenon` at a released version.
