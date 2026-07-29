# go-executor

The off-chain executor binary for Zenon's Phase 1 off-chain execution layer: a generic,
domain-agnostic runtime that runs a domain's deterministic state-transition function
off-consensus and settles commitments on L1 via the Settlement embedded contract.

Normative specs live in `go-zenon`:
[`SPEC.md`](../go-zenon/docs/notes/offchain-execution/phase-1/SPEC.md) (on-chain protocol),
[`EXECUTOR.md`](../go-zenon/docs/notes/offchain-execution/phase-1/EXECUTOR.md) (this binary),
[`ARCHITECTURE.md`](../go-zenon/docs/notes/offchain-execution/phase-1/ARCHITECTURE.md) (design
companion). This repo implements the executor side of that split; `go-zenon` governs on any
conflict.

## Layout

| Package | Responsibility |
|---|---|
| `domain` | The plugin seam every execution domain implements (`Domain`, `Source`, `StateAccess`, `Effects`) — public, importable by out-of-tree domain plugins. |
| `runtime` | The generic runtime: plugin registration, the single-instance lease, and the run loop. |
| `plugins/wasm` | The WASM execution domain. Currently a stub; the deterministic interpreter is a separate, larger effort. |
| `internal/lease` | The exclusive file lock behind the single-instance lease. |
| `internal/state` | The L2 leaf cache: a WAL-backed `Store` (`Root`/`Prove`/`Apply`, wrapping only the path-native trie API) and the witnessing `Accessor` a domain's `Apply` reads through. |
| `internal/codec` | The canonical binary encoding for every consensus-visible structure. |
| `internal/ledger` | (Empty) The input pipeline: ledger following, canonical ordering, per-domain cursor. |
| `internal/batch` | (Empty) The STF driver, batch slicing, commitment, and DA bundle builder. |
| `internal/recovery` | (Empty) On-disk layout and the crash-recovery state machine. |
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
