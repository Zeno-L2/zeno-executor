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
| `plugins/wasm` | The WASM execution domain: the deterministic Core 1.0 interpreter — binary reader and validator, gas metering and instrumentation, the three-import host ABI, and `Apply` for all six input kinds. In simulate mode its code store lives in memory: a miss fetches the as-deployed module over the channel, checks it against state, and keeps at most 32 MiB of instrumented modules. |
| `internal/lease` | The exclusive file lock behind the single-instance lease. |
| `internal/state` | The L2 leaf cache: a WAL-backed `Store` (`Root`/`Prove`/`Apply`, wrapping only the path-native trie API), the witnessing `Accessor` a domain's `Apply` reads through, and the immutable `Version` the gateway keeps one of per served batch, sharing structure with the versions derived from it. |
| `internal/codec` | The canonical binary encoding for every consensus-visible structure. |
| `internal/ledger` | The input pipeline: a `Follower` that reads confirmed momenta from a `MomentumSource`, reconstructs the canonical per-block order, and takes each settlement momentum's inputs for the configured domain from an `AssignmentSource`, which supplies the global input index. |
| `internal/batch` | The `Driver`: the per-input execution pipeline against a `domain.Domain` and `internal/state.Store`, batch slicing under the §25.1/§27.9 caps, the self-verified batch commitment and DA bundle, and the batch's receipt sidecar. |
| `internal/recovery` | The on-disk storage layout and its file modes, the bundle and receipt-sidecar stores, atomic cursor persistence, snapshot retention, and the boot-to-ready recovery state machine with reorg rollback and fail-closed halts. |
| `internal/workerproc` | Starts and supervises one isolated worker process: an explicit environment, descriptors 0 to 3 only, its own session, and, where the host allows it, a dedicated uid and cgroup v2 memory and CPU limits. |
| `internal/ipc` | The private channel between the supervisor and one domain worker: a stream of length-prefixed, typed frames carrying configuration, momenta, anchors and commitments. |
| `internal/simipc` | The private channel between the gateway and one simulate-mode worker: a stream of length-prefixed, typed frames carrying the simulator's configuration and memory limit, one input per simulation, the state, code and withdrawal-cap reads the simulator asks for, and its result. It imports neither `internal/ipc` nor `internal/batch`. |
| `worker` | The domain-agnostic worker process entry point: wraps the unchanged `runtime.Run` over a supervisor channel on descriptor 3 and translates the result into a process exit status the supervisor's crash-loop logic can act on. When the channel's first frame is an `internal/simipc` configuration, it runs in simulate mode instead: no domain directory and no cursor, one input per request against state, code and caps read over the channel, under the memory limit the configuration sets. In simulate mode a state value is fetched over the channel at most once per simulation. |
| `internal/supervisor` | The supervisor: configuration, the submission key, the restart policy, worker channel serving, and spawning and supervising one worker process per configured domain. |
| `internal/gateway` | The gateway's core (EXECUTOR §21): reads an executor domain directory's `batches/`, `bundles/` and `receipts/` read-only (regular files only, no symlinks, each under a size limit), checks each batch against Settlement before serving it at `latest`, retracts pending batches whose records disappear or change, and keeps the state versions from `finalized` through `pending` with the domain status (`SYNCING`, `OK`, `INCONSISTENT`), with its configuration and a snapshot cache of each domain's finalized state, checked against Settlement at boot, and an on-disk index per domain of every checked batch's inputs, receipts, events, outbox messages, L1 senders, deploys and code hashes, reconciled with Settlement at boot, and the JSON-RPC server with its HTTP bounds and browser-origin rule, and the simulator pools behind `zeno_simulate`. |
| `cmd/executor` | The supervisor binary: loads the configuration and submission key, then runs `internal/supervisor` until shutdown. |
| `cmd/gateway` | The gateway binary (EXECUTOR §21): loads its configuration, waits for the `znnd` node, boots each served domain from its snapshot cache, follows every domain and serves JSON-RPC over HTTP at `listenAddress` until shutdown. |
| `cmd/worker-wasm` | The WASM domain worker binary: the supervisor starts it with its channel on descriptor 3. It also runs in simulate mode: the gateway starts it in simulate mode, one pool per domain. |
| `internal/testbin` | Test-only binaries the test suites build and exec as real, separate processes: a domain worker stub, which also runs in simulate mode, and the key-isolation probe. |

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
<storagePath>/<domainId>/                                        # the domain directory (mode 0711)
<storagePath>/<domainId>/worker.lock                             # that domain's per-worker lock
<storagePath>/<domainId>/bundles/<daHash>.bundle                 # DA bundles (files 0644, directory 0755)
<storagePath>/<domainId>/receipts/<daHash>.receipts              # receipt sidecars (files 0644, directory 0755)
<storagePath>/<domainId>/batches/<batchId>.json                  # batch records, ids zero-padded to 20 digits (files 0644, directory 0755)
<storagePath>/<domainId>/...                                     # its other recovery state and wasm-code/ (files 0600)
```

`bundles/`, `receipts/` and `batches/` are the gateway's read interface to a worker (EXECUTOR §16). A receipt sidecar is the batch's receipts in their canonical encoding, which are the `receiptRoot` leaves, as one `List<Bytes>` with no header; the `.receipts` extension is its format version. The worker writes a batch's sidecar after its bundle and before its batch record. The worker sets the modes of the domain directory and those three directories at each boot, and the mode of each file in those three directories and of its recovery files, with an explicit chmod, never through the umask. A separate gateway user can therefore read no file under the domain directory outside those three directories, except the empty `worker.lock`. Bundles and batch records written by an earlier build keep mode 0600, and batches built by an earlier build have no sidecar.

Any local user who can reach the domain directory can open `worker.lock` and hold a lock on it, and a read-only mount does not prevent that. While such a lock is held, the domain's worker cannot start: the supervisor keeps restarting it and the domain makes no progress. A running worker holds the lock itself, so the user blocks its next start (after a crash, a kill or an executor restart). The `gateway` service in `docker-compose.yml` is such a user: it runs caller-chosen simulators as uid 9999 on the same kernel, with `executor-data` mounted. Run the executor where no untrusted local user can reach its storage path, and treat a compromised compose gateway as able to stop the domain. A later build adds a lock file that other users cannot open.

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

## Running the gateway

```
go build -o <dir>/ ./cmd/gateway
<dir>/gateway -config <file>
```

`-config` names the gateway's JSON configuration file. The decoder rejects any field not
listed below. Simulation is on by default and then needs `domains[].workerBinary` for every domain, so a
configuration without `workerBinary` exits 78 until it sets `workerBinary` for each domain or sets
`simulation.enabled` to `false`. One gateway runs per `storagePath`: it holds `<storagePath>/gateway.lock`,
which holds no data.

The gateway checks every batch it serves against Settlement and marks a domain `INCONSISTENT` when they disagree, but it is not a watcher: it sends nothing to L1, and its only signals are its log and its API (`zeno_getHead` status, `-32003`). By design a `LOCAL` gateway counts a failed check on the executor's read-interface files as the executor's fault, so a hostile or broken worker can mark a healthy domain `INCONSISTENT`; the log names the batch and the cause.

| Field | Type | Notes |
|---|---|---|
| `znndEndpoint` | URL | Required; an absolute `http` or `https` URL of a node serving version-3 momenta. |
| `networkName` | string | Required. |
| `confirmationDepth` | integer | Required; 1 to 1023, no default. |
| `storagePath` | absolute path | Required; the gateway's own store. It may not equal, contain or sit inside any `executorStoragePath`, checked as written, with symlinks resolved, and by file identity, at configuration load and again before a domain boots, because the gateway deletes directories under it. A domain directory that holds `batches/`, `cursor`, `meta.json` or `momentum_hashes/` is refused. |
| `listenAddress` | `IP:port` | Optional; default `127.0.0.1:35999`. The host must be an IP address, never a hostname (`0.0.0.0` listens on every interface); port 0 binds a free port. |
| `allowedOrigins` | array of strings | Optional; default empty, which refuses every browser request. Each entry is an origin exactly as a browser sends it: lowercase `http` or `https` `scheme://host[:port]`, without the default port, a path or a trailing `/`. |
| `simulation.enabled` | boolean | Optional; default `true`. `false` turns `zeno_simulate` off (it answers `-32006`, "simulation is disabled") and needs no `workerBinary`. |
| `simulation.maxGas` | integer | Optional; default 100,000,000, 1 to 1,000,000,000; 0 or absent means the default. The largest `plasmaLimit` a simulated input may carry. |
| `simulation.timeoutMs` | integer | Optional; default 2,000, 1 to 5,000; 0 or absent means the default. The time one simulation has before the gateway kills its simulator. |
| `simulation.maxConcurrent` | integer | Optional; default 2, 1 to 8; 0 or absent means the default. Simulators per domain, so simulations running at once per domain. |
| `simulation.uid` | integer | 1 to 9999. Required as root on Linux with simulation enabled, and ignored elsewhere. 0 and the owner of the resolved `storagePath` (or the gateway's own uid when the path does not exist yet) are refused whenever the field is set. It must be a dedicated uid and gid that own no file the gateway or the executor needs, and it must be able to execute `workerBinary`. The simulator's gid equals its uid, so a system uid such as 6 gets the `disk` group on a host install. |
| `domains` | array | Required, non-empty. |
| `domains[].domainId` | integer | Unique within the file. |
| `domains[].source` | string | `"LOCAL"` (read an executor's directory) or `"REMOTE"` (fetch from other gateways). |
| `domains[].executorStoragePath` | absolute path | Required for `LOCAL`: the executor's `storagePath`, read-only. The gateway reads only `batches/`, `bundles/` and `receipts/` under the domain directory. Absent or `null` for `REMOTE`. |
| `domains[].upstreams` | array of URLs | Required for `REMOTE`, 1 to 8 entries, and refused for `LOCAL`. Each is an absolute `http` or `https` base URL of another gateway, without a query or fragment; a path is kept (`https://host/gw` fetches `https://host/gw/bundles/<daHash>`) and userinfo is sent as basic auth. |
| `domains[].workerBinary` | absolute path | Required for every domain, `LOCAL` and `REMOTE`, while simulation is enabled, and checked whenever given: absolute, an existing regular file, executable. The gateway does not check it against the domain record's `stfSpecHash`, so a binary of another version gives previews that differ from the chain; deploy the executor's own worker binary. |

Exit statuses: 0 on a clean shutdown (SIGINT or SIGTERM, including during boot), 1 when a domain
fails to boot (including a `znnd` read that does not answer within 30 s), another gateway holds
the lock, or the listener cannot bind or the server stops, 78 on an invalid configuration or command line.
78 also covers a missing `simulation.uid` when the gateway runs as root on Linux with simulation enabled, and a
missing `workerBinary` while simulation is enabled. 1 also covers simulation refused as root on Linux because the
cgroup v2 memory and cpu limits are off, simulation refused as root on a host that does not separate uids (a `sudo`
gateway on macOS), and a failure to seal inherited descriptors or turn non-dumpable; each logs a line naming the cause.

The cache lives in `<storagePath>/<domainId>/state/wal`, `<storagePath>/<domainId>/snapshots/` and
`<storagePath>/<domainId>/index/` (directories it creates 0700, files 0600). The index holds every batch the gateway has checked. At boot it keeps the batches above its finalized batch that still match Settlement or, while pending, the executor's batch record; it removes the first one that does not and every batch above it, and marks the domain `INCONSISTENT` when an accepted batch's entry changed. An index that cannot be opened, or that disagrees with the snapshot or with Settlement's entry for its finalized batch, deletes the cache and rebuilds the domain from batch 0. A cache written by a gateway without the index has no `index/`, so the first boot after upgrading rebuilds every domain from batch 0. Each input's L1 sender is read once from `znnd` (`ledger.getAccountBlockByHash`, checked against the send hash, and accepted only from a user send to the Settlement contract that names the domain) and kept for good, and a batch is served only once every sender is stored. An accepted batch whose input names a send that is not an input of the domain marks the domain `INCONSISTENT`; a pending batch that does so stays unpublished. The kept senders are bounded by the L1 sends to Settlement that name the domain. A rebuild re-reads every sender, one `znnd` round trip per input, so every trigger that deletes the cache costs one round trip per input in the domain's history: an index that cannot be opened, an index that disagrees with the snapshot or with Settlement, and a snapshot whose batch entry is not visible (a lagging node or a raised `confirmationDepth`), which discards the whole cache and the index with it. A gateway log line that says "the index is damaged" while running means the index changed under it; stop the gateway and remove `<storagePath>/<domainId>`, and it rebuilds on the next start. At boot the gateway requires `storagePath` to be a real directory owned by its own uid and sets it to 0700, uses its resolved path from then on, refuses a symlink in the path owned by a user other than root or itself, and refuses to start when any parent of its resolved path is owned by a user other than root or itself, or can be written by another user without the sticky bit, and before it creates, truncates or deletes anything in a domain's cache it refuses a symlink or a directory owned by another uid. The gateway creates `storagePath` with `MkdirAll` and resolves it with `EvalSymlinks` before its ownership walk, and the walk of the resolved path refuses a link planted between those steps when it leads to a directory another user owns. Run the gateway as its own non-root uid. When the gateway first catches up after boot, it saves a snapshot
of the finalized state; after that it saves at most one every 10 minutes while batches finalize,
and each save deletes the older ones. A restart rebuilds from the executor's bundles the batches
finalized since the last snapshot, about 10 minutes' worth while saves succeed. A clean shutdown saves no snapshot, so a restart after one also rebuilds those batches. At boot it checks
the newest snapshot's root against that batch's `postStateRoot` in Settlement at the confirmed
height; a mismatch, an invisible entry, or a snapshot or WAL it cannot open deletes the cache and
rebuilds the domain from batch 0. A node that has not synced past the snapshot's batch, or a
raised `confirmationDepth` that hides its entry, discards the cache. A `znnd` error or timeout at
boot never deletes the cache. Deleting a domain's cache while the gateway is stopped is always
safe.

Limits: the gateway serves only domains with an empty genesis, and a seeded domain goes
`INCONSISTENT` with the reason "the domain's genesis state is not empty; the gateway serves only
domains with an empty genesis". Pending batches hold at most 256 MiB of decoded data per domain,
and `pending` stops below a batch that would pass it until Settlement accepts batches below. A
bundle above 1 GiB is never loaded and stalls its domain at `SYNCING`. A `REMOTE` domain must also fetch a bundle within one 30 s poll turn (below): about 300 MB at 10 MB/s, so a larger bundle stalls its domain at `SYNCING` on a slow upstream. A record whose input data
is longer than the longest canonical encoding of its input kind (8,333 bytes for a `CallL2` with an
8 KiB payload, 369 for a `DeployContractStart` with 256 bytes of metadata, 33 for a kind with no
tail), whose deploy chunk is longer than 15 KiB, or whose execution result is longer than 32 KiB
fails its batch's check and marks the domain `INCONSISTENT`; no executor within Settlement's and
the runtime's limits builds one. A bundle's witnesses are bounded only by the 1 GiB ceiling: they
never enter the index, but the check decodes every record and witness of the bundle before it
counts them, so a hostile 1 GiB bundle needs several GiB of memory. Every `znnd` read has a
30 s deadline and each domain's turn waits at most 30 s on `znnd` reads, so a hung read delays the other domains
by at most 30 s per round but never stops them, and the next turn retries. A turn that publishes
nothing logs its error once. A turn cut off after publishing a batch logs `the poll used its whole
30s slice; resuming next round` once, until a turn completes. A node that answers some reads and
hangs others (a re-read during finalization, say) stays at that one line while every round waits
out the full 30 s; its sign is a `finalized` tag that stops advancing while `latest` advances. A
local file read that blocks inside the kernel (a hung network or FUSE filesystem under
`executorStoragePath` or `storagePath`) still stalls every domain.

### JSON-RPC API

The gateway serves JSON-RPC 2.0 (EXECUTOR §21.3) on `POST /` at `listenAddress` once every domain has booted. Requests need `Content-Type: application/json`, params are positional (at most 16), and a request without an `id` is a notification, which is neither run nor answered. This build serves `zeno_chainInfo(domainId)`, `zeno_getHead(domainId)`, `zeno_getBatch(domainId[, tag])`, `zeno_getReceipt(sendHash)`, `zeno_getInputs(filter)`, `zeno_getStateAt(domainId, path[, tag])`, `zeno_getProof(domainId, paths[, tag])`, `zeno_getEvents(filter)`, `zeno_getOutbox(filter)`, `zeno_getBundle(daHash)`, `zeno_getReceiptSidecar(daHash)`, `zeno_getOutboxProof(outboxId)`, `zeno_subscribe(kind, params[, tag])`, `zeno_unsubscribe(id)`, `zeno_read(domainId, view[, params[, tag]])` and `zeno_simulate(domainId, input[, tag])`. Error codes follow EXECUTOR §21.3; an internal error answers `-32603` and is logged once until its message changes. A response echoes the request's `id` bytes as sent; an `id` string that holds invalid UTF-8 is echoed unchanged, so that response is not valid UTF-8 JSON, and only its sender sees it.

`zeno_getHead` returns `pending`, `latest` and `finalized`, each `null` until a batch reaches it and otherwise `batchId`, `lastInputSeq`, `postStateRoot` and `verified`, plus `status` and `inconsistentAt`. A tag is `verified` when its batch is at or below `latest` and below `inconsistentAt`. `SYNCING` covers catch-up and every stall (a missing bundle or sidecar, a bundle above 1 GiB, a failing sender read). For an `INCONSISTENT` domain, `inconsistentAt` is the batch whose check failed, no tag at or above it is verified, and the gateway's log names the cause. `zeno_chainInfo` adds `networkName`, `runtimeKind` and `stfSpecHash` from the domain record (re-read every 10 minutes; `null` until the first read, and always `null` for a domain already `INCONSISTENT` at boot), and `chain_id`, `profiles` and `views` from the domain's reader. A domain already `INCONSISTENT` at boot has no record, so it has no reader: no views, and `input` stays `null`.

A tag is `"pending"`, `"latest"`, `"finalized"` or a batch id as a JSON integer. The tag is optional and a left-out tag means `"latest"`. An explicit `null` for an optional parameter means it is left out, so a `null` tag also means `"latest"`; a `null` required parameter answers `-32602`. Parameters are positional; `params` must be an array. Every result names its `domainId`, the `batchId` it was read at and `verified`, by the rule of `zeno_getHead`. A tag no batch has reached, a batch above `pending`, and a send hash or daHash no served batch holds answer `-32004`, as does a batch the gateway retracted while the request read it and, for `zeno_getReceipt` and the raw-file methods, a batch it finalized in that moment (a retry resolves both). A batch at or above `inconsistentAt` answers `-32003`, including a `"latest"` or `"pending"` tag that resolves to one. State is kept from the `finalized` batch through `pending`: `zeno_getStateAt`, `zeno_getProof` and `zeno_read` answer `-32002` for an older batch, and every other method serves every batch the gateway has indexed.

Hashes and paths are 64 lowercase hex characters without a prefix (parameters accept either case), L1 addresses are `z1...`, raw bytes are lowercase hex, amounts are decimal strings, receipt statuses are names (`SUCCESS`, `REVERT`, ...), and arrays are `[]`, never `null`. `zeno_getBatch` returns the commitment fields the gateway checks (not `daMode`, `executorId`, `submittedAtHeight`, `assetFlowSummary` or the version fields) and each input's `globalInputIndex`, `sendHash` and receipt `status`. `zeno_getReceipt` searches the domains in configuration order and returns the receipt, `returnData`, events, outbox messages (each with its `outboxId`), the L1 `sender`, the `tag` its batch has reached, and `input` (the reader's decoded input, or `null`). `zeno_getStateAt` returns `present` and `value`: present-empty is `present: true` with `value` `""`, and absent is `present: false` with `value` `null`. `zeno_getProof` takes 1 to 64 paths and returns each one's `present`, `value` and `proof`, a path-native SMT proof against the returned `postStateRoot`.

`zeno_read(domainId, view[, params[, tag]])` runs one named view of the domain's reader at a batch, with `params` an object of the view's parameters (an unknown member answers `-32602`, and a `null` optional parameter is left out). The WASM domain serves the `contracts/1` views `contract(contractId)`, `storage(contractId, key)` (`key` is at most 32 bytes of hex), `contractsByDeployer(address[, cursor])` and `module(sourceCodeHash)`; `zeno_chainInfo` lists each view's parameters and result fields. `module` takes the `contract` view's `sourceCodeHash` (the as-deployed `code_hash`), not its `codeHash` (the instrumented module's hash). The result names `postStateRoot`, `view`, `result` and `proofs`. A state view proves every path it reads against `postStateRoot`, `owner` is checked against `ownerCaller`, and `contractsByDeployer` pages by `cursor` (pass `nextCursor` back unchanged). A domain with no reader, or a view it lacks, answers `-32005`, and `module` answers `-32004` when no successful deploy of that code hash exists at the batch. A gateway cache whose index lacks the current schema version is discarded and rebuilt from batch 0 at boot, so a cache built before this release rebuilds itself; the rebuild re-reads every input's L1 sender from `znnd` (one node call per input on a long history).

`zeno_simulate(domainId, input[, tag])` runs one input against the state at a batch and returns what the batch driver would record for it, without submitting anything. The `input` is an object with `kind` (`"CallL2"` or `"Deposit"` only), `sender` (an L1 address), `targetContract`, `payload` (hex, `CallL2` only), `deposit` (`asset` and `amount`, a decimal string) and `plasmaLimit` (at most `simulation.maxGas`). The input's index is one past the `pending` batch's last input, and the momentum height and time come from the newest poll's Settlement view; a withdrawal's caps are read from Settlement at that height. The state rule is the one of `zeno_read`: kept from `finalized` through `pending`, and `-32002` for an older batch. The result names `domainId`, `batchId`, `postStateRoot`, `inputIndex`, `momentumHeight`, `momentumTime`, `status`, `gasUsed`, `abortCode`, `contractId`, `codeHash`, `returnData`, `events`, `outbox` and `stateDiff`, with `verified` always `false`, `simulated` always `true` and every outbox message's `outboxId` `null`, because no batch holds it. The errors, by code:

- `-32602`: a wrong parameter count, a bad `domainId`, a bad input member (each message names the member and its rule) or a bad tag.
- `-32001`: an unknown domain.
- `-32002`: `"pending"` on a `REMOTE` domain, a batch whose state is no longer kept, and "simulation is not available until the gateway's first poll reads Settlement; retry later".
- `-32003`: the domain is `INCONSISTENT` from the tag's batch. `-32004`: no batch has reached the tag yet, an unknown batch id, or a batch that is no longer available.
- `-32006`: "simulation is disabled", "plasmaLimit <n> exceeds simulation.maxGas <m>", "no simulator is free; retry later", "the request ended before a simulator was free", "the gateway is shutting down", "simulation exceeded timeoutMs (<n> ms)", "the simulator ran out of memory", "the simulator stopped during the simulation; retry", "the simulator could not run the input: <reason>" and "simulation exceeded 65,536 state reads".
- `-32600`: "request body exceeds its simulation budget; send this request alone", for a third simulation in one body. `-32603`: "internal error".

A preview can differ from the real input (EXECUTOR section 21.8): the chain's state can move before the input is submitted, and an input that reads more than 65,536 distinct state keys succeeds in a batch but answers `-32006` "simulation exceeded 65,536 state reads" or the timeout error.

`zeno_getBundle` and `zeno_getReceiptSidecar` return an object with `domainId`, `batchId`, `verified`, `daHash` and the served batch's file as hex in `bundle` or `sidecar`, after checking the file against the batch's `daHash` or `receiptRoot` again; a file that no longer matches answers `-32603`, and a missing file `-32004`. A bundle is returned only when the whole object fits the response; otherwise the error is `-32600` "bundle <daHash> is larger than this response can carry (<n> bytes); fetch GET /bundles/<daHash>", where `<n>` is the most bundle bytes that fit beside the object's other fields; the file routes below serve the whole file. A request sent alone carries a bundle of up to about 8 MiB, which the gateway holds as raw bytes and as hex (about 24 MiB), so 32 bundle requests in flight hold about 768 MiB on top of the request bodies. The Go heap can reach about twice the live bytes unless `GOMEMLIMIT` is set.

`zeno_getInputs(filter)`, `zeno_getEvents(filter)` and `zeno_getOutbox(filter)` take one filter object. Every filter needs `domainId` and may hold `fromBatch` (a batch id, default 0), `toBatch` (a tag, default `"latest"`, so a list shows only batches checked against Settlement unless asked for more), `limit` (1 to 100, default 100) and `cursor`; a `null` member is left out; an unknown member answers `-32602`, a member name that differs in case, or repeats, answers `-32602`, and so does a `fromBatch` above a `toBatch` batch id. A named or default `toBatch` that has not reached `fromBatch` returns no items and a `null` `nextCursor`, with `batchId` naming the batch the tag stands at, or `null` (with `verified` `false`) when no batch has reached the tag, where other methods answer `-32004`; so a client polling from N+1 to `"latest"` needs no error handling. `zeno_getInputs` also takes `contractId`, `sender` and `inputKind` (0 to 255): a `contractId` returns every input whose receipt names that contract from its first successful `DeployContractFinalize` on, failed ones included, and nothing for a contract not deployed by `toBatch`. `zeno_getEvents` also takes `contractId` and `topics`, up to 4 entries matched by position, where `null` matches any topic. `zeno_getOutbox` also takes `recipient`, the L1 address of a withdrawal. A result names `domainId`, the batch `toBatch` resolved to as `batchId`, its `verified`, `items` and `nextCursor`. Items run newest first (by `globalInputIndex`, then event or outbox index), each with its own `batchId` and `verified`. An input item holds `globalInputIndex`, `sendHash`, `sender`, `inputKind`, a `receipt` summary (`status`, `contractId`, `gasUsed`, `abortCode`) and `input` (the reader's decoded input, or `null`); event and outbox items hold the fields `zeno_getReceipt` returns for them. A `nextCursor` of `null` means the range is done; otherwise pass it back unchanged, with the same filter, for the next page. A cursor is an opaque position: batches that arrive later never shift a later page, a page after a retraction repeats no item, and a `toBatch` batch id that was retracted answers `-32004`. A page with fewer than `limit` items, even none, and a `nextCursor` was cut by its request body's scan or response budget: send the next page alone. A page that filled `limit` has a `nextCursor` even when nothing follows, so the last page can be empty.

The same address serves WebSocket: a `GET /` with an upgrade opens a connection, and each message is one request body, answered with the same bytes as over HTTP (a message of only notifications gets no answer). `zeno_subscribe` and `zeno_unsubscribe` answer `-32601` over HTTP. `zeno_subscribe("heads", domainId)` notifies when a domain's `pending`, `latest` or `finalized` batch moves; `zeno_subscribe("receipt", sendHash)` notifies when that send is added pending, accepted, finalized or withdrawn by a retraction; `zeno_subscribe("events", filter[, tag])` notifies when a batch holding events that match `filter` (`domainId`, and optionally `contractId` and `topics` as in `zeno_getEvents`) is accepted, and with the tag `"pending"` also when such a batch is added pending or retracted (the default tag is `"latest"`). Each returns the subscription id, a string. A notification is `{"jsonrpc":"2.0","method":"zeno_subscription","params":{"subscription":"<id>","result":...}}`. A `heads` result is `zeno_getHead`'s result and a poll sends at most one, so a catch-up over many batches is one notification; a status-only change sends none. A `receipt` result holds `domainId`, `batchId`, `daHash`, `sendHash`, `tag` (`"pending"`, `"latest"` or `"finalized"`), `verified` and `removed` (`true`, with `tag` `"pending"`, when a retraction withdrew the send); the client reads the receipt with `zeno_getReceipt`. An `events` result holds `domainId`, `batchId`, `daHash`, `verified`, `removed` and `count`, the batch's events that match the filter; the client pages `zeno_getEvents` with the same filter and `fromBatch` and `toBatch` set to that `batchId` for them. A retraction notifies every `"pending"` events subscription of the domain, with `removed` `true` and `count` 0. A subscription reports only changes after its `zeno_subscribe` response is written, so a client reads the current state with `zeno_getHead`, `zeno_getReceipt` or `zeno_getEvents` after subscribing, and no notification for a subscription follows its `zeno_unsubscribe` response (which is `true` for a subscription that existed and `false` otherwise).

`zeno_getOutboxProof(outboxId)` returns what an L1 `RelayMessage` call needs for one outbox message: `domainId`, `batchId`, `verified`, `tag`, `outboxId`, `outboxRoot`, `message` (the canonical encoding, hex), `inclusionProof` (the Merkle proof of `message` against `outboxRoot`, hex), `decoded` (the message as `zeno_getOutbox` returns it) and `processedOutbox`, whether Settlement has registered the message. `processedOutbox` is `false` without a read for a batch not yet checked against Settlement; for a `latest` or `finalized` batch it is read from Settlement at the height of the gateway's latest poll, so it can lag the node by one poll interval. It answers `-32002` before the first poll has read Settlement, and when 4 such reads are already running. A message of a batch the gateway does not serve answers `-32004`, and one at or above `inconsistentAt` answers `-32003`.

A request body holds at most 1 MiB (413 above it, at once when `Content-Length` declares more) and a batch at most 100 requests (one `-32600` error above it). The responses to one body hold at most 16 MiB: a response that would pass it becomes `-32600` ("batch response exceeds 16 MiB; send this request alone", or "response exceeds 16 MiB" alone), and a request that starts with no room left is not run. At most 32 requests run at once, and another gets 503. A request line and headers hold at most 16 KiB (431 above it, before a request slot is taken). A connection has 5 s to send its headers. Once a request is admitted, it has 5 s to send its whole body and gets 408 after that, so a 1 MiB body needs about 210 KB/s. Its response must finish within 30 s, and an idle connection closes after 120 s. The gateway does not cap HTTP connections, and 32 clients that stall their bodies can hold every request slot for 5 s at a time: expose it through a proxy that caps connections. One body may also name at most 256 `zeno_getProof` paths in total (a request that would pass it answers `-32600`, "request body exceeds its proof path budget", and the earlier responses stay), and its list requests together visit at most 10,000 index keys and read at most 32 MiB of index values (a list request that must scan an index after either is used up answers `-32600`, "request body exceeds its index scan budget; send this request alone"; a list request that scans nothing, such as one for a range with no batches, is answered normally). The list request that uses up the 32 MiB finishes the index entry it is reading, so a body's list requests read at most 32 MiB plus that one entry. The key and byte budgets bound what list requests and `zeno_getOutboxProof` read. An outbox proof checks the budgets once, before it scans, and then finishes its batch's whole outbox scan, under 3 MiB (one input's entries total about 45 KiB, and a batch holds at most 64 inputs). Every index entry a method reads is small, because of the input and execution result bounds in the limits above: an input's entry is at most about 17 KiB, an outbox message's about 8 KiB and an event's about 1 KiB, and one input's entries together at most about 45 KiB. A method holds about three copies of what it reads at once, so `zeno_getReceipt`, which reads one input's entries, holds under 150 KiB. None of these bounds limits request rate, so a public gateway also needs a request-rate limit in front of it.

A WebSocket message holds at most 1 MiB (the connection closes with 1009 above it). The gateway holds at most 64 WebSocket connections in total, not per client (503 above it), 16 subscriptions per connection (`-32600` above it) and 256 queued notifications per connection (it closes the connection with 1008 above it, which only a client that stops reading reaches, because every notification is under 1 KiB). A write has 10 s, the gateway pings every 30 s, and a connection that sends neither a message nor a pong for 60 s closes. WebSocket requests use at most 16 of the 32 request slots, so HTTP keeps 16. The 64-connection cap is global, so one client can take every connection: a reverse proxy in front of the gateway must cap WebSocket connections and request rate per client address. One body may hold at most 8 `zeno_getOutboxProof` requests (the next answers `-32600`, "request body exceeds its outbox proof budget; send this request alone"), their Settlement reads share one 10 s deadline, and at most 4 `processedOutbox` reads run at once across the gateway. A WebSocket response is buffered whole before it is written, so WebSocket adds up to 592 MiB on top of the HTTP figures above: 512 MiB for 16 requests in flight, each holding a whole response of up to 16 MiB (up to 32 MiB of buffer capacity), 64 MiB for 64 connections that each hold a 1 MiB message, and 16 MiB for 64 queues of 256 notifications under 1 KiB each. The 512 MiB term alone is what 16 clients that never read their responses can pin, each for the 10 s write time.

Simulators: with simulation enabled the gateway starts one pool per domain of `simulation.maxConcurrent` processes of `workerBinary`, each in simulate mode, and `zeno_simulate` hands each simulation to a free one. A simulator runs only `CallL2` and `Deposit` inputs. It fetches the contract's code from the gateway and checks it itself, with a 32 MiB module cache. A simulator crash, protocol break or timeout answers `-32006` and replaces the process after a wait that doubles with each failure of that slot (0, 1, 2, 4, 8, 16, then 30 s, reset after 60 s without one); a `Failed` reply, a read-budget error and a withdrawal-cap read failure keep the process. A request waits 1 s for a free simulator, one body holds at most 2 simulations, and a simulation reads at most 65,536 state keys: a repeated read of one key is served from the simulator's own cache and costs nothing, and the 65,537th distinct key answers `-32006`. A body's worst case is about 14 s, inside the 30 s response limit, and at most 4 withdrawal-cap reads from `znnd` run at once across the gateway. At startup the gateway logs `gateway: simulation enforcement:` with what the host enforces.

Simulator CPU: each state read costs the gateway one proof and short-lived allocations: the proof allocates about 6 KiB for an absent key and about 18 KiB for a 16 KiB value, and the encoded reply adds about twice the value's size. Measured with `go test -run '^$' -bench BenchmarkSimulateProve -benchtime 2000x ./internal/gateway/` on an Apple M1 Pro (`goos` darwin, `goarch` arm64, 8 cores), the proof of a key that is absent from a state of 1,024 leaves costs 72 µs on average over 2,048 distinct absent paths, and one proof of a 16 KiB value costs 44 µs. A simulation of 65,536 absent keys therefore costs about 4.7 s of one core, and 65,536 values of 16 KiB cost about 2.9 s, so at the default `timeoutMs` of 2,000 an absent-heavy input meets the time limit before the read budget. At the 5,000 maximum the proofs alone take about 4.7 s, and with the IPC round trip of each read the time limit and the read budget are within about 10% of each other, so a host or a state that is slower than this one meets the time limit first. Another host differs: run the command to measure yours. The slot serving a read-heavy simulation keeps one gateway core busy for up to `timeoutMs`, and the garbage collector adds work on other cores. One caller can keep every slot busy with inputs that end just under `timeoutMs`, which costs no restart wait, so `maxConcurrent` gateway cores per domain stay busy, plus GC work, for as long as that caller keeps sending. Keep the sum of `simulation.maxConcurrent` over domains at or below half the host's cores: that bounds the cores the requests keep busy. Simulators set no `GOMAXPROCS`, so off root the Go runtime uses every core of the host, and a running simulator's garbage collector can take up to half the host's cores on top of that. On the root path `cpu.max` caps each simulator at one CPU.

Simulator memory: each simulator has a 1 GiB soft memory limit, which is a GC target and not a cap. A hard cap needs root on Linux: only there does a simulator run as `simulation.uid` with `memory.max` 2 GiB and 1 CPU. Elsewhere it runs as the gateway's user with no memory cap. As root on Linux with simulation enabled the gateway refuses to start when it cannot set up those cgroup limits, names the cause, and never runs simulators uncapped there; it moves itself into a `supervisor` cgroup leaf to delegate the controllers, as the executor's supervisor does. The rule is that `memory.max` must be at least the soft limit plus 1 GiB, because a worst-case compile pushes the heap about 770 MiB past the soft limit. A simulation that reads many large values holds them for its duration, up to about 1.1 GiB more per simulator (about 1.7 GiB live, near the cap, and the cap may kill it as "the simulator ran out of memory"). If that coincides with a compile, a simulator can briefly need about 2.9 GiB, and as root on Linux the cap kills it and the gateway replaces it. At `simulation.maxConcurrent` 2 a domain uses about 2 GiB steady and about 3.5 GiB in bursts: size the host for D x 3.5 GiB (about D x 5.8 GiB off root for the rare overlap), or lower `simulation.maxConcurrent`.

Simulator isolation: with simulation enabled the gateway makes itself non-dumpable on Linux before it starts a simulator, which stops a simulator that shares its uid from reading the gateway's memory or environment. That is the only protection the gateway adds off root, and it is a no-op on macOS. The cache paragraph recommends a non-root gateway uid. Off root, which is how the `gateway` service in `docker-compose.yml` runs (uid 9999, see "Docker"), a sandbox escape from a simulator:

- runs as the gateway's uid with no memory cap and no filesystem restriction, so it reads the gateway's configuration file (the `znndEndpoint` credentials), writes `storagePath` and can signal the gateway;
- can alter executor state, or a FILE submission key kept there, when `executorStoragePath` is writable by the gateway's uid;
- can leave a forked child that outlives the simulator, because nothing but a cgroup can reap it;
- in the compose deployment, cannot write executor state or reach the submission key, because `executor-data` is mounted `:ro` and the key is a Compose secret of the executor container only, but can hold the domain's `worker.lock` (see "On-disk layout").

On both paths a simulator has network access (`znnd` on localhost or the container network, cloud metadata, internal services). A simulator process serves the simulations of many callers over its life, and all simulators of the gateway share one uid, so a sandbox escape persists across callers, where it can read their inputs and forge their previews, and it reaches the other simulators, which it can signal. Off root that uid is the gateway's uid. On the root path it is `simulation.uid`, which the configuration refuses when it is 0 or owns `storagePath`, so the escape does not own the gateway's cache, but it still reaches every other simulator. On the root path a configuration file readable by others (the usual bind-mount default) is readable by `simulation.uid`, so keep it mode 0600 and owned by the gateway's user. Running as root on Linux gives each simulator `simulation.uid` and the cgroup caps, but the gateway process itself runs as root. Operators choose: a public gateway that runs non-root keeps `simulation.maxConcurrent` low or disables simulation. A later build adds a filesystem and network sandbox for simulators.

Simulator availability: simulation is best-effort under a single hostile caller. One caller can keep every slot busy with inputs that run just under `timeoutMs`, or keep slots in restart backoff by forcing timeouts or crashes, so other callers get "no simulator is free; retry later". A per-client cap needs a proxy.

Access and exposure: the gateway listens on `127.0.0.1` by default. Expose it by changing `listenAddress` or through a proxy, which must cap connections and request rate per client. `zeno_simulate` runs caller-chosen inputs on the gateway host, so a public gateway keeps `simulation.maxConcurrent` low or sets `simulation.enabled` to `false`. For a `LOCAL` domain, the gateway's uid needs search permission on `executorStoragePath` and each parent, and read access to `bundles/`, `receipts/` and `batches/`. The executor sets those modes (see "On-disk layout"). Files written by an earlier build stay 0600 and need the domain re-synced, and a read-only mount grants nothing by itself.

`GET` and `HEAD` for `/bundles/<daHash>` and `/receipts/<daHash>` (64 hex characters, either case) serve the raw file of exactly the batches `zeno_getBundle` and `zeno_getReceiptSidecar` serve, pending ones included: an unknown daHash or a missing file answers 404, a batch at or above `inconsistentAt` answers 503 with the `-32003` message, and any other path under those prefixes answers 404. Ranges, `If-Range` and `If-None-Match` work against the `ETag`, which is the quoted lowercase `daHash`. The gateway does not re-hash a bundle it serves, so a range request costs no hash of the whole file; a client verifies the bundle against its `daHash`. A sidecar (at most about 17 KiB) is checked against `receiptRoot` again before it is served, and a mismatch answers 500. At most 8 file responses run at once, apart from the 32 request slots, and another gets 503. Each write has 30 s and a whole response has 10 minutes, so a transfer that makes progress is not cut at the 30 s response limit above; a client whose response ends at 10 minutes resumes with `Range` and `If-Range` against the `ETag`. The 10-minute cap is the only bound on a slow client: 8 clients that each read about 32 KiB per 30 s (about 1 KiB per second) hold every file slot for 10 minutes, so a public gateway belongs behind a proxy that limits slow clients.

A `REMOTE` domain keeps no executor directory. For each batch Settlement accepted, it downloads the bundle and the receipt sidecar from `upstreams` over `GET /bundles/<daHash>` and `GET /receipts/<daHash>`, and then runs the same checks as a `LOCAL` domain. An upstream is skipped for a file when its bundle does not hash to the batch's `daHash`, or its sidecar does not decode or its root is not the batch's `receiptRoot`; only the correct bytes can pass, so a tampering upstream never makes a healthy domain `INCONSISTENT`, and any later check failure marks the domain `INCONSISTENT` as it does for `LOCAL`. An attempt on one upstream is cancelled when no response headers arrive for 10 s, or no body bytes for 10 s, and the fetch moves to the next upstream in the same poll. Each file starts at the upstream that served the last file, or at the one after a skipped upstream, and a poll turn that ends mid-download moves past that upstream only when it had the whole turn. A fetch must finish inside one 30 s poll turn: a bundle too large for that stalls its domain at `SYNCING`, is fetched again from zero each round, and logs `the bundle fetch from <upstream> did not finish within the 30s poll slice; a bundle this large needs a faster upstream` once per batch; a later build adds resumable downloads. The gateway refuses redirects, turns response compression off, caps response headers at 64 KiB, streams each body to a temporary file through SHA3-256 without holding it in memory, and applies `HTTP_PROXY` and `HTTPS_PROXY` from the environment. Upstream URLs come only from the configuration, so the gateway does not filter addresses. A disk error in the gateway's own cache shows as a poll failure, never as a skipped upstream.

Verified files live in `<storagePath>/<domainId>/bundles/` and `receipts/` (directories 0700, files 0600), and a cache discard keeps them; a file that no longer verifies is removed and fetched again, and a download interrupted by a crash leaves a `.tmp-` file that the next boot removes. A `REMOTE` domain serves no `pending` batch: `zeno_getHead` returns `pending` `null`, and a `"pending"` tag, a list `toBatch` of `"pending"` or an `events` subscription with that tag answers `-32002`. Each skipped upstream logs one line per upstream and file kind until its reason changes, hashes and ports aside. When every upstream fails for a file, the domain stays `SYNCING` and logs one line naming the file kind and `daHash`.

A request with no `Origin` header (curl, a server) is served. A browser request is served only when its `Origin` equals an `allowedOrigins` entry; any other `Origin`, including `null`, gets 403 and runs nothing, and the same rule applies to a WebSocket upgrade. Preflight `OPTIONS` requests are answered for `POST` with `Content-Type`, and on the file routes for `GET` and `HEAD` with `Range`.

On SIGINT or SIGTERM the gateway first closes every WebSocket connection with 1001, then stops accepting, waits up to 10 s for the requests in flight, closes the remaining connections, stops every simulator (a running simulation finishes or times out first), and then closes each domain's index. A second SIGINT or SIGTERM during that wait ends the process at once.

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
docker compose up --build            # run the executor and the gateway
docker compose down                  # stop and remove them
docker compose -f docker/compose.test.yml run --build --rm test   # run the test suite as root
```

`docker compose logs executor` shows, in order: the `supervisor: enforcement: ...` line, then
repeating `executor: znnd node at http://znnd:35997 is unusable: ...; retrying in <delay>` lines.
No worker starts, because this environment runs no znnd.

The elevated privileges above (`privileged: true`, `cgroup: private`) are for this local
environment only, to let the supervisor delegate a cgroup and separate worker uids inside the
container. To run the executor against a real go-zenon devnet, see "Devnet integration test"
below.

The `gateway` service runs the image's `gateway` binary as uid 9999, the image's default user, so the executor service sets `user: "0:0"`. Running the image's `executor` outside Compose needs `--user 0:0` for the same reason, because the supervisor separates the worker's uid only as root. The gateway service serves one `LOCAL` domain from `docker/gateway.json`, which holds no credential. The gateway's data volume `gateway-data` takes the owner and mode of the image's `/var/lib/gateway` (9999:9999, 0700), which the gateway's storage check requires; a bind mount in its place needs the same owner and mode. `executor-data` is mounted read-only. The port is published on `127.0.0.1:35999` only. `simulation.maxConcurrent` is 1, and `simulation.uid` stays unset because off root the gateway ignores it.

The service sets `mem_limit` and `memswap_limit` to 3 GiB, which caps the gateway and its simulators together with no swap. When the cap is reached, the kernel's choice is usually a simulator, which the gateway replaces, but it can be the gateway, which stops the container. `cpus: 2` comes with `GOMAXPROCS` 2 for the gateway process. Each simulator starts with an empty environment and sizes its Go runtime to every core of the Docker VM inside the same 2-CPU quota, so a read-heavy simulation can meet `timeoutMs` sooner than on an unthrottled host. The gateway log keeps at most 3 files of 10 MB. `stop_grace_period: 15s` covers the 10 s request wait and the index close.

`docker compose logs gateway` shows repeated `gateway: znnd node at http://znnd:35997 is unusable: ...` lines here, because this environment runs no znnd and the gateway serves nothing until it answers. With a node, query it like this:

```
curl -s -X POST http://127.0.0.1:35999/ -H 'Content-Type: application/json' \
  -d '{"jsonrpc":"2.0","id":1,"method":"zeno_getHead","params":[1]}'
```

## Devnet integration test

`scripts/devnet-test.sh` runs the executor against a real go-zenon devnet and checks that Settlement accepts its batches and that the executor registers and releases their withdrawals. It needs Docker, Go 1.23 and the sibling `../go-zenon` checkout on a branch with the devnet build that go-zenon's `docker/devnet/README.md` describes. Run it as a non-root user, one run at a time:

```
./scripts/devnet-test.sh
```

The script wipes its own go-zenon devnet, starts its five znnd nodes (not the explorer) and runs `go test -tags devnet ./internal/devnet/`. The test builds `cmd/executor`, `cmd/worker-wasm` and `cmd/gateway` without the tag, so it runs the binaries a deployment runs, against `http://127.0.0.1:35997`. In order, it:

1. waits for the Settlement contract to activate at momentum 50, then, as the devnet's network admin (dev address index 1), registers domain 1 with the devnet build's 30-momentum withdrawal delay and the best-effort data-availability mode, configures ZNN as its asset, registers dev address index 3 as the executor of pillar `dev1`, and fuses QSR so that dev address index 3 has plasma, then sets domain 1's relay pause, so that steps 2 to 4 send no relays;
2. deposits into domain 1, starts the executor, stops it while its first send is unresolved and restarts it, then starts a `LOCAL` gateway on the executor's directory. It checks that the restarted executor follows that send to acceptance without sending again, that Settlement's entry equals the commitment the executor signed, and that the pillar and executor-set reads succeed on the real chain;
3. sets domain 1's submit pause, deposits again, sends a zero-deposit `CallL2` to a target with no contract and sets the domain's status to `PAUSED`, checks that nothing is sent while the pause holds, then clears the pause and checks that the batch is accepted while the domain is still `PAUSED`. It checks the call's receipt by send hash at `pending` (not verified) while the pause holds, then at `latest` (verified);
4. stops the executor, removes domain 1's directory, restarts it and deposits again, then checks that the rebuilt batch records for every batch up to the one this step's deposit closes (batches 0 to 2, or 0 to 3 when the `CallL2` lands in a batch of its own) match Settlement's accepted entries and that Settlement accepts the next batch. It checks the call's receipt at `finalized` across the directory removal and starts a `REMOTE` gateway on the `LOCAL` one;
5. clears the relay pause, then checks that the executor registers each of the three deposits' refunds with exactly one `RelayMessage`, that each registered withdrawal is its batch's refund to the depositor, that `ReleaseWithdrawals` empties domain 1's release queue, that Settlement's payout of each refund to the depositor is in a momentum, that every withdrawals file is removed, and that the executor's account chain holds exactly the sends its submission log records. It ends with the byte comparison of the two gateways' answers and a check that `REMOTE` serves no `pending` batch.

Each gateway starts one simulator. The test calls no `zeno_simulate`. A failed run's directory also holds `gateway-local.log` and `gateway-remote.log`.

The devnet domain runs no contract code, so every state root is the empty-state zero root. The test does not cover a state-root change, an inactive pillar, the global pause, rejection retries, or a restart while a relay is unresolved.

A run takes about 52 minutes (measured: 3109 seconds for the test), most of it waiting for momenta; the first run also builds the devnet image. The script builds and runs that image as `go-executor-devnet-znnd:latest` (through `docker/devnet.override.yml`), so it never changes the `go-zenon-devnet:latest` tag of a devnet started with `make devnet-up`. On success the script wipes the devnet. On failure it leaves the devnet running, and the test keeps its directory, whose path it logs, with the executor logs, the configuration and the storage directory holding the submission log. The next run of the script wipes a devnet it left running.

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

The worker file-mode test across uids (`TestModesUnderUIDSeparation` in `internal/recovery`) is root-only too and runs in the same Docker test environment. So do the gateway's storage-path tests that change an owner (`TestSecureLeaf`, `TestPrepareStorageAncestors`, `TestPrepareStorageSymlinkComponents`, `TestPrepareStorageRefusesForeignLeaf`, `TestOpenFinalRefusesForeignOwner`, `TestOpenFinalRefusesForeignLeaf`).

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
