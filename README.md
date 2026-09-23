# go-executor

The off-chain executor binary for Zenon's Phase 1 off-chain execution layer: a generic,
domain-agnostic runtime that runs a domain's deterministic state-transition function
off-consensus and settles commitments on L1 via the Settlement embedded contract.

Normative specs live in the **`zeno`** documentation repository, under `phase-1/`:
`SPEC.md` (on-chain protocol), `EXECUTOR.md` (this binary), `ARCHITECTURE.md` (design
companion), `BRIDGES.md` (building bridges on the two), and `CHANGELOG.md` (revision
history, with the reasoning behind each change). Those copies are canonical: a change is
applied there first and this repo builds against the result. This repo
implements the executor side of the split and `zeno`'s `phase-1/` set governs on any
conflict.

## Layout

| Package | Responsibility |
|---|---|
| `domain` | The plugin seam every execution domain implements (`Domain`, `Source`, `StateAccess`, `Effects`) — public, importable by out-of-tree domain plugins. |
| `runtime` | The generic runtime: plugin registration, the single-instance lease, the boot-time recovery, and the execution loop that commits batches, handles reorgs and halts on a consistency fault. |
| `plugins/wasm` | The WASM execution domain: the deterministic Core 1.0 interpreter — binary reader and validator, gas metering and instrumentation, the three-import host ABI, and `Apply` for all six input kinds. |
| `internal/lease` | The exclusive file lock behind the single-instance lease. |
| `internal/state` | The L2 leaf cache: a WAL-backed `Store` (`Root`/`Prove`/`Apply`, wrapping only the path-native trie API) and the witnessing `Accessor` a domain's `Apply` reads through. |
| `internal/codec` | The canonical binary encoding for every consensus-visible structure. |
| `internal/ledger` | The input pipeline: a `Follower` that reads confirmed momenta from a `MomentumSource`, reconstructs the canonical per-block order, assigns the global input index, and filters to a configured domain via a `SettlementFilter`. |
| `internal/batch` | The `Driver`: the per-input execution pipeline against a `domain.Domain` and `internal/state.Store`, batch slicing under the §25.1/§27.9 caps, and the self-verified batch commitment and DA bundle. |
| `internal/recovery` | The on-disk storage layout, atomic cursor persistence, snapshot retention, and the boot-to-ready recovery state machine with reorg rollback and fail-closed halts. |
| `cmd/executor` | The binary entry point: wires a build's plugins into the runtime. |

## Building

```
go build ./...
go vet ./...
go test ./...
./scripts/check-trie-imports.sh   # fails if any L1-only trie API is referenced
```

## Dependency on go-zenon

This module requires `github.com/zenon-network/go-zenon` and imports only its
consensus-frozen packages (`common/trie`'s path-native API, `common/types`) — never the
reverse.

Development happens on personal forks before the two repos move under the `zenon-network`
GitHub org, so `go.mod` currently `replace`s the canonical path with a pinned commit on
[`digitalSloth/go-zenon`](https://github.com/digitalSloth/go-zenon), tracking the
`feature/merkle-state-root` branch (the SMT work this executor's state layer depends on):

```
replace github.com/zenon-network/go-zenon => github.com/digitalSloth/go-zenon v0.0.0-20260729090002-56ce2c384966
```

The fork's `go.mod` still declares itself `module github.com/zenon-network/go-zenon` (it hasn't been
renamed), so `go get github.com/digitalSloth/go-zenon@<ref>` fails the module-path check — that
command resolves the path directly, and `replace` only skips the check for its right-hand side
target, not for a plain `go get`. To bump the pin: get the target commit's hash and UTC commit
timestamp (`git log -1 --format='%H %cI' <ref>` in a checkout of the fork), build the pseudo-version
`v0.0.0-<YYYYMMDDHHMMSS>-<first 12 hex chars of the hash>`, and set both the `require` and `replace`
lines in `go.mod` to it directly, then run `go mod tidy`. Once the Settlement contract lands and
both repos move to `zenon-network`, drop the `replace` entirely and pin
`require github.com/zenon-network/go-zenon` directly.
