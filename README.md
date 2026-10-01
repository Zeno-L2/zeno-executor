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
| `runtime` | The generic runtime: plugin registration, the per-domain lock (`worker.lock`), the boot-time recovery, and the execution loop that commits batches, handles reorgs and halts on a consistency fault. |
| `plugins/wasm` | The WASM execution domain: the deterministic Core 1.0 interpreter — binary reader and validator, gas metering and instrumentation, the three-import host ABI, and `Apply` for all six input kinds. |
| `internal/lease` | The exclusive file lock behind the single-instance lease. |
| `internal/state` | The L2 leaf cache: a WAL-backed `Store` (`Root`/`Prove`/`Apply`, wrapping only the path-native trie API) and the witnessing `Accessor` a domain's `Apply` reads through. |
| `internal/codec` | The canonical binary encoding for every consensus-visible structure. |
| `internal/ledger` | The input pipeline: a `Follower` that reads confirmed momenta from a `MomentumSource`, reconstructs the canonical per-block order, assigns the global input index, and filters to a configured domain via a `SettlementFilter`. |
| `internal/batch` | The `Driver`: the per-input execution pipeline against a `domain.Domain` and `internal/state.Store`, batch slicing under the §25.1/§27.9 caps, and the self-verified batch commitment and DA bundle. |
| `internal/recovery` | The on-disk storage layout, atomic cursor persistence, snapshot retention, and the boot-to-ready recovery state machine with reorg rollback and fail-closed halts. |
| `internal/workerproc` | Starts and supervises one isolated worker process: an explicit environment, descriptors 0 to 3 only, its own session, and, where the host allows it, a dedicated uid and cgroup v2 memory and CPU limits. |
| `internal/ipc` | The private channel between the supervisor and one domain worker: a stream of length-prefixed, typed frames carrying configuration, momenta, anchors and commitments. |
| `worker` | The domain-agnostic worker process entry point: wraps the unchanged `runtime.Run` over a supervisor channel on descriptor 3 and translates the result into a process exit status the supervisor's crash-loop logic can act on. |
| `internal/supervisor` | The supervisor: configuration, the submission key, the restart policy, worker channel serving, and spawning and supervising one worker process per configured domain. |
| `cmd/executor` | The supervisor binary: loads the configuration and submission key, then runs `internal/supervisor` until shutdown. |
| `cmd/worker-wasm` | The WASM domain worker binary: the supervisor starts it with its channel on descriptor 3. |
| `internal/testbin` | Test-only binaries the test suites build and exec as real, separate processes: a domain worker stub and the key-isolation probe. |

## Building

```
go build ./...
go vet ./...
go test ./...
./scripts/check-trie-imports.sh   # fails if any L1-only trie API is referenced
```

## Running

```
go build -o <dir>/ ./cmd/...
<dir>/executor -config <file>
```

`-config` names the supervisor's JSON configuration file. The decoder rejects any field not
listed below. `docker/executor.json` is a complete example.

| Field | Type | Notes |
|---|---|---|
| `executorAddress` | Zenon address | Required; identifies this executor instance and names the supervisor's own lease file. |
| `networkName` | string | Required. |
| `confirmationDepth` | integer | Required; 1 to 1023. |
| `storagePath` | absolute path | Required; see "On-disk layout" below. |
| `submissionKeySource` | object | Required; see "Submission key" below. |
| `workers` | array | Required, non-empty; one entry per domain. |
| `workers[].domainId` | integer | Unique within the file; also sets the worker's uid (`10000 + domainId`) when uids are separated. |
| `workers[].binary` | absolute path | Required; must be an executable regular file. |
| `workers[].role` | string | Must be `"EXECUTOR"`. |
| `workers[].stfSpecHash` | 32-byte hex hash | Required. |
| `workers[].memoryLimit` | integer, bytes | Required, positive; sets the worker's `GOMEMLIMIT` (90% of this value) and, where enforced, its cgroup `memory.max`. |
| `workers[].cpuLimit` | number, CPUs | Required, 0.01 to 1024; sets the worker's cgroup `cpu.max` where enforced. |

### Submission key

`submissionKeySource.kind` is `FILE`, `ENV` or `KMS`:

- `FILE` (`path`): the file must be a regular file, absolute, with no group or other
  permission bits, and exactly one hard link — checked by `fstat` on the already-open
  descriptor, so it cannot be relinked out from under the check. As root on Linux, the
  supervisor additionally rejects a key owned by a configured worker uid or sitting inside a
  domain's storage directory.
- `ENV` (`name`): the named variable is read once, then unset, so `os.Environ()` no longer holds
  it. `/proc/<pid>/environ` still does, for the supervisor's own lifetime — nothing clears a
  process's own environment block by unsetting a variable. What keeps a worker from reading it
  there is the non-dumpable flag and, as root, the separate worker uid, not the value being gone.
- `KMS`: not implemented; naming it is a configuration error.

No worker ever receives the key, its source kind, or its path or variable name: a worker's
channel `Config` frame carries only its `domainId`, the executor address, its `stfSpecHash`,
`confirmationDepth`, `role`, `networkName` and `storagePath`.

### Isolation

On Linux running as root, each worker gets its own uid (`10000 + domainId`) and cgroup v2
memory and CPU limits. Everywhere else, the `supervisor: enforcement: limits <on|off>, uids
<on|off>[: reason]` log line names what could not be enforced and why. Without uid separation a
worker runs as the supervisor's own user, and a `FILE` submission key reachable by that user is
reachable by the worker too — there is no OS-level barrier between them.

### Exit statuses

A worker's exit status, from `internal/ipc/exit.go`, drives the supervisor's restart decision:

| Status | Meaning | Supervisor's action |
|---|---|---|
| 0 | clean exit | Treated the same as any other unexpected exit: a counted failure unless the run progressed or stayed up long enough to reset the count. Restarted with backoff, or HALTED after 5 consecutive failures without progress (a crash loop). |
| 1 | worker error, or a broken supervisor channel | Same as 0. |
| 2 (or any other code) | anything not listed below | Same as 0. |
| 3 (`ipc.ExitHalted`) | consistency fault | HALTED; never restarted. |
| 75 (`ipc.ExitTempFail`) | source unavailable, or the domain lock is already held | Restarted with backoff; not counted toward the crash loop. |
| 78 (`ipc.ExitConfig`) | the worker rejected its configuration | CONFIG_ERROR; never restarted. |
| killed by a signal | a liveness timeout, a malformed frame, an OOM kill, or (during shutdown) the supervisor's own SIGTERM/SIGKILL | Same bucket as 0/1/2, except during the supervisor's own shutdown, when the worker simply stops. |

The supervisor itself exits 0 on a clean shutdown, 1 on any other failure to run, and 78
(`ipc.ExitConfig`) on an invalid configuration or submission key.

### On-disk layout

```
<storagePath>/supervisor/<executorAddress>.lock   # the supervisor's own lease
<storagePath>/<domainId>/worker.lock               # that domain's per-worker lock
<storagePath>/<domainId>/...                        # its recovery state and wasm-code/
```

An older `<storagePath>/<executorAddress>.lock` or `<storagePath>/wasm-code/` from before
workers moved into their own processes is ignored under the new layout: `mv` the code store
into the domain directory it now belongs under, or let the worker re-sync it.

## Docker

The build context expects a sibling checkout of `../go-zenon` (see "Dependency on go-zenon"
below); `docker-compose.yml` and `docker/compose.test.yml` both pass it in as the `go-zenon`
additional build context.

Before the first `docker compose up`, set the submission key the example configuration expects,
keeping the value out of shell history and off the command line:

```
read -rs EXECUTOR_KEY
export EXECUTOR_KEY
```

`docker-compose.yml` passes it to the container as a Compose secret, a file Compose creates
inside the container, rather than a bind mount from the host: on Docker Desktop, a 0400
root-owned key bind-mounted read-only was still readable by a separated worker uid — the bind
mount's permission bits did not carry through its VM's file-sharing layer, silently, with no
error from the supervisor's own checks. The Compose secret does not go through that layer. The
key's content is a placeholder until submission signing exists; nothing in this environment
reads it for anything but the file checks `LoadKey` and `checkKeyNotWorkerReadable` apply to it.

Commands:

```
docker compose up --build            # run the executor
docker compose down                  # stop and remove it
docker compose -f docker/compose.test.yml run --build --rm test   # run the test suite as root
```

`docker compose logs executor` shows, in order: the `supervisor: enforcement: ...` line, then
`supervisor: domain 1 worker started: pid <n>, uid 10001, limits on`, then `supervisor: domain 1
HALTED: worker halted (exit status 3)` — every worker in this environment halts at boot, because
the znnd client and Settlement anchor reader are not wired up yet.

The elevated privileges above (`privileged: true`, `cgroup: private`) are for this local
environment only, to let the supervisor delegate a cgroup and separate worker uids inside the
container; linking the environment to a real go-zenon devnet is later work.

## CI

GitHub CI runs as non-root. Its Linux leg runs the key probe's `portable` and `linux` subtests
(`TestWorkerCannotReachKey/{FILE,ENV}/{portable,linux}` in `cmd/executor/main_test.go`); the
root-only tests — uid separation, cgroup limits, and the probe's `uid separated` subtests — skip
there and run instead in the Docker test environment (`docker compose -f
docker/compose.test.yml run --build --rm test`, which runs as root). The macOS leg runs the
`portable` subtests too; its `linux` and `uid separated` subtests skip there with a named
reason, since the `/proc` and ptrace attempts they exercise are Linux-only.

## Dependency on go-zenon

This module requires `github.com/zenon-network/go-zenon` and imports only its
consensus-frozen packages (`common/trie`'s path-native API, `common/types`) — never the
reverse.

Development happens on personal forks before the two repos move under the `zenon-network`
GitHub org, so `go.mod` currently `replace`s the canonical path with a sibling checkout of the
[`digitalSloth/go-zenon`](https://github.com/digitalSloth/go-zenon) fork, which must be on a
branch carrying the path-native trie API:

```
replace github.com/zenon-network/go-zenon => ../go-zenon
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
