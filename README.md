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
| `internal/ledger` | The input pipeline: a `Follower` that reads confirmed momenta from a `MomentumSource`, reconstructs the canonical per-block order, and takes each settlement momentum's inputs for the configured domain from an `AssignmentSource`, which supplies the global input index. |
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
| `executorAddress` | Zenon address | Required; identifies this executor instance and names the supervisor's own lease file and submission log. |
| `networkName` | string | Required. |
| `pillarName` | string | Required, non-empty; the pillar whose executor-set member is this executor. |
| `znndEndpoint` | URL | Required; the znnd JSON-RPC endpoint, an absolute `http` or `https` URL. The supervisor reads momenta and Settlement state from it, and the workers never see it. |
| `confirmationDepth` | integer | Required; 1 to 1023. |
| `maxPendingBatches` | integer | Optional, default 8; 1 to 64. A worker stops sealing batches once this many sit above Settlement's `nextBatchId`, committed or pending, and resumes as the chain accepts them. |
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

The key is the 32-byte ed25519 seed as 64 hex characters, with no `0x` prefix; surrounding
whitespace is trimmed. For a go-zenon wallet account it is `KeyPair.Private.Seed()`, not a
mnemonic or a BIP-39 seed. A seed whose address is not `executorAddress` fails boot with exit
status 78.

No worker ever receives the key, its source kind, or its path or variable name: a worker's
channel `Config` frame carries only its `domainId`, the executor address, its `stfSpecHash`,
`confirmationDepth`, `role`, `networkName`, `storagePath` and `maxPendingBatches`.

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
| killed by a signal | a liveness timeout, a malformed frame, a withdrawal store write failure, an OOM kill, or (during shutdown) the supervisor's own SIGTERM/SIGKILL | Same bucket as 0/1/2, except during the supervisor's own shutdown, when the worker simply stops. |

A worker that reports a batch whose commitment differs from the one the supervisor already holds
for that batchId, without retracting it first, is a consistency fault for that domain: it is
killed and HALTED, never restarted, and its last exit reads "consistency fault". Other domains
continue.

The supervisor itself exits 0 on a clean shutdown, 1 on any other failure to run, and 78
(`ipc.ExitConfig`) on an invalid configuration or submission key.

### Submission

The supervisor submits each domain's batches from the executor address, one unresolved send at a
time across all domains. It checks each batch against Settlement at the confirmed height first. A
contradiction halts that domain as a consistency fault, as does a worker retracting a batch
Settlement already accepted.

It confirms each send by reading Settlement once the receive is `confirmationDepth` momenta deep.
A rejected batch, or a send the node no longer knows, is retried without limit under the restart
backoff and never halts the domain. After 5 consecutive rejections of the same batch, the
supervisor logs an error and raises an error status for that domain, and keeps retrying. The count
and the status clear when Settlement accepts the batch, when Settlement's `nextBatchId` moves
past it, or when the executor restarts. A `nextBatchId` read lower than an earlier one comes from
a lagging node or a reorg deeper than `confirmationDepth`: it clears nothing and is logged as a warning, and each read of `nextBatchId` that
is higher than the highest seen raises the observed value. A send the node no longer knows neither counts as a
rejection nor clears the count.

Before each send, the supervisor reads at the confirmed height Settlement's submit pause, global
and for the domain, and the standing of `pillarName`: its executor-set member must name
`executorAddress` with status active, and the pillar must be registered and not revoked. When a
check fails, or the read fails, it logs a warning, sends nothing for that domain and checks again
on its next pass; it never halts the domain. A domain whose record status is `PAUSED` still has
its batches submitted. These reads trail the chain by `confirmationDepth` momenta, and
behind a load-balanced endpoint they can trail by more than the batch read does, so a pause set
at the frontier can still let a send through; Settlement rejects it, and the send costs plasma.

A send not received after `confirmationDepth` x 10 momenta raises an error status and is never
replaced automatically. Insufficient plasma raises an error status and retries.

When a lagging node or a reorg deeper than `confirmationDepth` keeps Settlement's `nextBatchId`
below the supervisor's observed value, that domain stops submitting and raises an error status after
`confirmationDepth` x 10 momenta. The remedy is to check the node, then restart the executor. After a
reorg like that, move the submission log files aside before restarting, because a restart reads the
observed value back from them.

The supervisor records each send in its submission log before publishing it, and the send's outcome once it is known, one JSON object per line, each written and fsynced before it goes on. At boot, once the node is reachable, a send with no outcome is followed to its outcome before anything is sent for any domain, so a restart follows that send instead of forgetting it. As in a running process, a lagging node can still make the send look dropped; the supervisor then rebuilds the batch, and Settlement accepts at most one copy. This holds for a domain that is no longer configured too: the supervisor records the outcome, logs it as a warning naming the domain, and sends nothing for that domain. A "dropped" outcome read from a lagging node may then be wrong, and nothing acts on it. While the node is unreachable, the supervisor retries and sends nothing. A send recorded as abandoned, because its domain halted first, is not followed again. At boot, each domain's observed `nextBatchId` starts one above its latest batch recorded as accepted, so a lagging node cannot make the supervisor send an accepted batch again.

Each line starts with a log-format version (`"version":2` for this release) and names the send by plain fields: domain, method, batch, the withdrawal's outbox id on a relay line, account height, block hash, data hash and outcome. The log does not store the batch commitment; the supervisor reads it from the send's block. Across an upgrade:

- A release that changes the batch commitment encoding still follows a send left with no outcome by an older release. It reads the outcome from Settlement's `nextBatchId`, skips only the comparison with Settlement's accepted entry, and logs one INFO line naming the send.
- A line with a log-format version this release does not know, as after a downgrade, or a line whose fields it cannot read, is never followed. When that line is the send left with no outcome, the supervisor halts the send's domain with an error naming the send and records the send as abandoned; for a domain that is no longer configured, it logs a warning instead. Restarting the executor clears the halt. The supervisor may then send that batch a second time; Settlement accepts at most one of the two, and the other costs plasma.
- Fields a release does not know are ignored, so a newer release can add fields without changing the log-format version.
- This release reads version 1 lines. An older release reads every version 2 line as unreadable, so a downgrade is not supported.

A crash in the middle of a write leaves an incomplete last line, which the supervisor drops at boot with a warning. A line that is not a JSON object with a log-format version and a block hash stops the supervisor at boot with an error naming the file and the line. Moving the file and its `.1` file aside starts a new log, and the supervisor then forgets a send that had no outcome: it may send that batch a second time, Settlement accepts at most one of the two, and the other costs plasma. If the supervisor cannot write or sync the log, it logs an error, raises an error status and sends nothing more until the executor restarts.

Each send adds about 500 bytes. Once the log reaches 16 MiB, the supervisor renames it to `<file>.1` before the next send, replacing any older one, and starts a new file. It reads both files at boot.

The log is named after `executorAddress`. Changing `executorAddress` starts a new, empty log; the old one stays on disk at `<storagePath>/supervisor/<old executorAddress>.submissions.jsonl`, with its `.1` file, and the supervisor no longer reads it.

### Withdrawals

After each committed or re-announced batch, the worker hands over every L1 withdrawal with its inclusion proof. The supervisor checks order, position, proof and per-asset totals, then writes and fsyncs the batch's file before it holds the batch for submission.

A failed check halts the domain as a consistency fault. Only a zero-amount withdrawal can be left out undetected, because the totals check cannot see it.

A batch Settlement already accepted is compared with its file. With no file, it is checked against Settlement's stored outbox root and asset flow summary, one read per batch, then stored. If that read fails, the supervisor checks the order and format and stores the batch marked unverified. While the worker runs, the supervisor retries the check once a second, one unverified batch per domain at a time. Each failed retry doubles the wait, up to 60 seconds, and a success resets it. A retry gives up on a Settlement read after 2 seconds, so a hung node delays a reply to the worker by at most that. A match marks the file verified; a mismatch halts the domain. An unverified batch is never submitted, and only verified batches are relayed.

Once a batch is finalized at the confirmed height, the supervisor registers each of its withdrawals with `RelayMessage` from the executor address, lowest stored batch first. It relays verified batches only. Before each relay it checks that Settlement's accepted outbox root equals the file's; a mismatch halts the domain. It skips a withdrawal Settlement already records as processed, by this executor or anyone else. It never relays a zero-amount withdrawal or one to an embedded contract, because Settlement cannot register either.

Once 16 or more withdrawals wait in a domain's release queue (the most Settlement pays out per call), the supervisor sends `ReleaseWithdrawals` for that domain before its next relay. It also releases a shorter queue whenever the domain has no relay it can send, so a batch's last withdrawals are paid out once its relays are done or waiting. A received release is done whatever Settlement returned. The next release is built only once that receive is `confirmationDepth` momenta deep, so each domain gets at most one release per momentum.

Settlement's relay pause, global or for the domain, stops that domain's relays and releases, with a warning. A halted domain sends neither.

Relays and releases share the one unresolved send across all domains, alternating with batch sends. Each send takes about `confirmationDepth` + 2 momenta. With `confirmationDepth` 10 and 10-second momenta, a full batch of 2,112 withdrawals needs about 2,250 sends (2,112 relays and 132 releases) at about 2 minutes each: about 3 days, or about 6 days while the domain also sends batches. Each withdrawal costs about 57,000 plasma plus its call data: one relay at 52,500 and a sixteenth of a release at 73,500. Both methods are permissionless: a recipient can relay its own withdrawal with the gateway's outbox proof, and anyone can release.

A rejected relay is retried without limit under the restart backoff. After 5 consecutive rejections of the same withdrawal, the supervisor logs an error and raises the domain's relay error status. The domain's later withdrawals wait behind that one. The count and the status clear when the withdrawal is registered, by anyone, or when the executor restarts. A relay the node no longer knows, and a relay or release that cannot be built (for example for lack of plasma), waits out the same backoff without counting.

After a restart, a relay or release left with no outcome in the submission log is followed to its outcome instead of being sent a second time. The supervisor then re-reads which withdrawals are registered, up to 64 per pass.

A Retract deletes verified files from the retracted batch upward, never one below the observed `nextBatchId`, and never an unverified file. The supervisor also deletes a batch's file once the batch is finalized and every withdrawal in it is registered or not relayable. Until then, each batch keeps one file of up to about 1.5 MB.

A failed write or delete during a hand-off or Retract kills the worker, which re-sends at its restart. A disk that keeps failing makes the domain HALTED as a crash loop after 5 failures without progress. The remedy: fix the disk, then restart the executor. A failed write while marking a file verified is logged once and retried on the same backoff.

A damaged file stops the supervisor at boot with an error naming the file. Moving it aside drops that batch's withdrawals until the worker re-announces the batch while unfinalized. A domain removed from the configuration keeps its directory on disk, unread and untouched. Changing `executorAddress` starts a new, empty store and the old one stays on disk.

### On-disk layout

```
<storagePath>/supervisor/<executorAddress>.lock                  # the supervisor's own lease
<storagePath>/supervisor/<executorAddress>.submissions.jsonl     # the submission log (created with mode 0600)
<storagePath>/supervisor/<executorAddress>.submissions.jsonl.1   # the previous submission log, once rotated
<storagePath>/supervisor/<executorAddress>.withdrawals/<domainId>/<batchId>.json   # one batch's withdrawals, verified or not (mode 0600, directories 0700)
<storagePath>/<domainId>/worker.lock                             # that domain's per-worker lock
<storagePath>/<domainId>/...                                     # its recovery state and wasm-code/
```

An older `<storagePath>/<executorAddress>.lock` or `<storagePath>/wasm-code/` from before
workers moved into their own processes is ignored under the new layout: `mv` the code store
into the domain directory it now belongs under, or let the worker re-sync it.

Every existing data directory with at least one batch record halts on its first boot under
this build. No earlier build submitted, so no domain has a finalized batch and every record is above
the anchor, and records written by an earlier build carry no stored commitment. The worker
exits HALTED, naming the batchId and the domain directory. The remedy for every domain on
upgrade: stop the executor, remove that domain's directory and re-sync.

While a worker rebuilds batches below Settlement's `nextBatchId`, it closes each one exactly at the
accepted entry's input range and checks it against that entry: the protocol version, the input
range, the six roots, the data-availability hash and mode, the asset flow summary and the proof
data. A re-sync from an empty domain directory therefore reproduces the accepted batches and
continues past them. While it rebuilds, the worker reads ahead only to the end of the batch it is
rebuilding, so it commits after every accepted batch that ends at the domain's last input in its
momentum. A history this executor built therefore commits during a re-sync as often as it did when
it was first built.

Accepted batches that never end at the domain's last input in a momentum, such as batches built by a
different executor, stay uncommitted in memory until one does or the rebuild passes `nextBatchId`.
Each costs about 2 KB, about 190 MB per 100,000 batches. The domain's state log in its directory
also grows on disk through that stretch, because only a commit truncates it. A crash during that
stretch repeats the rebuild from the last commit.

A rebuilt batch that differs from its accepted entry, or an input stream that cannot end a batch
where the entry does, halts the domain: the worker exits HALTED, naming the batchId and the domain
directory, and the supervisor does not restart it. The usual cause is a domain directory whose
batches are not the accepted ones. The remedy: stop the executor, remove that domain's directory and
re-sync. If a re-sync from an empty directory halts the same way, this worker binary and its
`stfSpecHash` do not reproduce the accepted batches, and re-syncing cannot fix it.

A domain directory written by an earlier build can also hold more unsubmitted batches than the
supervisor now accepts (`maxPendingBatches` plus 2,048). Its worker is killed at every boot until
the domain is HALTED as a crash loop. The remedy is the same: stop the executor, remove that
domain's directory and re-sync.

## Docker

The build context expects a sibling checkout of `../go-zenon` (see "Dependency on go-zenon"
below); `docker-compose.yml` and `docker/compose.test.yml` both pass it in as the `go-zenon`
additional build context.

Before the first `docker compose up`, set the submission key the example configuration expects:

```
export EXECUTOR_KEY=bd2b1f4b42334f923064f466ed5590379498585b2dd4e42152db0e5ddbe2023e
```

This is the seed of devnet account index 3 (path `m/44'/73404'/3'` of the devnet mnemonic
committed in go-zenon's `docker/devnet/README.md`), the `executorAddress` in
`docker/executor.json`. It is public and for the devnet only, never for any other network.

For any real key, read it into the variable without echo (`read -rs EXECUTOR_KEY; export
EXECUTOR_KEY`) or use a secret file (`FILE` source). Never put a real seed on a command line,
where it lands in shell history and process listings.

`docker-compose.yml` passes it to the container as a Compose secret, a file Compose creates
inside the container, rather than a bind mount from the host: on Docker Desktop, a 0400
root-owned key bind-mounted read-only was still readable by a separated worker uid — the bind
mount's permission bits did not carry through its VM's file-sharing layer, silently, with no
error from the supervisor's own checks. The Compose secret does not go through that layer.

Commands:

```
docker compose up --build            # run the executor
docker compose down                  # stop and remove it
docker compose -f docker/compose.test.yml run --build --rm test   # run the test suite as root
```

`docker compose logs executor` shows, in order: the `supervisor: enforcement: ...` line, then
repeating `executor: znnd node at http://znnd:35997 is unusable: ...; retrying in <delay>` lines.
No worker starts, because this environment runs no znnd.

The elevated privileges above (`privileged: true`, `cgroup: private`) are for this local
environment only, to let the supervisor delegate a cgroup and separate worker uids inside the
container. To run the executor against a real go-zenon devnet, see "Devnet integration test"
below.

## Devnet integration test

`scripts/devnet-test.sh` runs the executor against a real go-zenon devnet and checks that Settlement accepts its batches and that the executor registers and releases their withdrawals. It needs Docker, Go 1.23 and the sibling `../go-zenon` checkout on a branch with the devnet build that go-zenon's `docker/devnet/README.md` describes. Run it as a non-root user, one run at a time:

```
./scripts/devnet-test.sh
```

The script wipes its own go-zenon devnet, starts its five znnd nodes (not the explorer) and runs `go test -tags devnet ./internal/devnet/`. The test builds `cmd/executor` and `cmd/worker-wasm` without the tag, so it runs the binaries a deployment runs, against `http://127.0.0.1:35997`. In order, it:

1. waits for the Settlement contract to activate at momentum 50, then, as the devnet's network admin (dev address index 1), registers domain 1 with the devnet build's 30-momentum withdrawal delay and the best-effort data-availability mode, configures ZNN as its asset, registers dev address index 3 as the executor of pillar `dev1`, and fuses QSR so that dev address index 3 has plasma, then sets domain 1's relay pause, so that steps 2 to 4 send no relays;
2. deposits into domain 1, starts the executor, stops it while its first send is unresolved and restarts it, then checks that the restarted executor follows that send to acceptance without sending again, that Settlement's entry equals the commitment the executor signed, and that the pillar and executor-set reads succeed on the real chain;
3. sets domain 1's submit pause, deposits again and sets the domain's status to `PAUSED`, checks that nothing is sent while the pause holds, then clears the pause and checks that the batch is accepted while the domain is still `PAUSED`;
4. stops the executor, removes domain 1's directory, restarts it and deposits again, then checks that the rebuilt batch records for batches 0, 1 and 2 match Settlement's accepted entries and that Settlement accepts the next batch;
5. clears the relay pause, then checks that the executor registers each of the three deposits' refunds with exactly one `RelayMessage`, that each registered withdrawal is its batch's refund to the depositor, that `ReleaseWithdrawals` empties domain 1's release queue, that Settlement's payout of each refund to the depositor is in a momentum, that every withdrawals file is removed, and that the executor's account chain holds exactly the sends its submission log records.

The devnet domain runs no contract code, so every state root is the empty-state zero root. The test does not cover a state-root change, an inactive pillar, the global pause, rejection retries, or a restart while a relay is unresolved.

A run takes about 47 minutes (measured: 2799 seconds for the test), most of it waiting for momenta; the first run also builds the devnet image. On success the script wipes the devnet. On failure it leaves the devnet running, and the test keeps its directory, whose path it logs, with the executor logs, the configuration and the storage directory holding the submission log. The next run of the script wipes a devnet it left running.

The script runs the devnet as the Docker Compose project `go-executor-devnet`, so its wipes never remove the volumes of a devnet started with go-zenon's `make devnet-up`. Both devnets use the same container names and ports, so stop that devnet before running the script. `docker compose -f ../go-zenon/docker-compose.yml -p go-executor-devnet down -v` stops and wipes the script's devnet by hand.

`go test ./...` never runs this test and never needs Docker: every file in `internal/devnet` requires the `devnet` build tag.

## CI

GitHub CI runs as non-root. Its Linux leg runs the key probe's `portable` and `linux` subtests
(`TestWorkerCannotReachKey/{FILE,ENV}/{portable,linux}` in `cmd/executor/main_test.go`); the
root-only tests — uid separation, cgroup limits, and the probe's `uid separated` subtests — skip
there and run instead in the Docker test environment (`docker compose -f
docker/compose.test.yml run --build --rm test`, which runs as root). The macOS leg runs the
`portable` subtests too; its `linux` and `uid separated` subtests skip there with a named
reason, since the `/proc` and ptrace attempts they exercise are Linux-only.

## Dependency on go-zenon

This module requires `github.com/zenon-network/go-zenon`. The module imports these go-zenon
packages: `common/trie` (its path-native API), `common/types`, `common/crypto`, `chain/nom`,
`vm/embedded/definition`, `wallet` and `dp`, and the devnet integration test also imports
`vm/constants`; go-zenon never imports this module.

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
