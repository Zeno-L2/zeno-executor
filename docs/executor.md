# Running the executor

```
go build -o <dir>/ ./cmd/...
<dir>/executor -config <file>
```

`-config` names the supervisor's JSON configuration file. The decoder rejects any field not listed below. `docker/executor.json` is a complete example.

| Field | Type | Notes |
|---|---|---|
| `executorAddress` | Zenon address | Required; identifies this executor instance and names the supervisor's own lease file and submission log. |
| `networkName` | string | Required. |
| `pillarName` | string | Required, non-empty; the pillar whose executor-set member is this executor. |
| `znndEndpoint` | URL | Required; the znnd JSON-RPC endpoint, an absolute `http` or `https` URL. The supervisor reads momenta and Settlement state from it, and the workers never see it. |
| `confirmationDepth` | integer | Required; 1 to 1023. |
| `maxPendingBatches` | integer | Optional, default 8; 1 to 64. A worker stops sealing batches once this many sit above Settlement's `nextBatchId`, committed or pending, and resumes as the chain accepts them. |
| `storagePath` | absolute path | Required; see [On-disk layout](storage.md). |
| `submissionKeySource` | object | Required; see [Submission key](#submission-key) below. |
| `workers` | array | Required, non-empty; one entry per domain. |
| `workers[].domainId` | integer | Unique within the file; also sets the worker's uid (`10000 + domainId`) when uids are separated. |
| `workers[].binary` | absolute path | Required; must be an executable regular file. |
| `workers[].stfSpecHash` | 32-byte hex hash | Required. |
| `workers[].memoryLimit` | integer, bytes | Required, positive; sets the worker's `GOMEMLIMIT` (90% of this value) and, where enforced, its cgroup `memory.max`. |
| `workers[].cpuLimit` | number, CPUs | Required, 0.01 to 1024; sets the worker's cgroup `cpu.max` where enforced. |

## Submission key

`submissionKeySource.kind` is `FILE`, `ENV` or `KMS`:

- `FILE` (`path`): the file must be a regular file, absolute, with no group or other permission bits, and exactly one hard link, checked by `fstat` on the already-open descriptor, so it cannot be relinked out from under the check. As root on Linux, the supervisor additionally rejects a key owned by a configured worker uid or sitting inside a domain's storage directory.
- `ENV` (`name`): the named variable is read once, then unset, so `os.Environ()` no longer holds it. `/proc/<pid>/environ` still does, for the supervisor's own lifetime: nothing clears a process's own environment block by unsetting a variable. What keeps a worker from reading it there is the non-dumpable flag and, as root, the separate worker uid, not the value being gone.
- `KMS`: not implemented; naming it is a configuration error.

The key is the 32-byte ed25519 seed as 64 hex characters, with no `0x` prefix; surrounding whitespace is trimmed. For a go-zenon wallet account it is `KeyPair.Private.Seed()`, not a mnemonic or a BIP-39 seed. A seed whose address is not `executorAddress` fails boot with exit status 78.

No worker ever receives the key, its source kind, or its path or variable name: a worker's channel `Config` frame carries only its `domainId`, the executor address, its `stfSpecHash`, `confirmationDepth`, `networkName`, `storagePath` and `maxPendingBatches`.

## Isolation

On Linux running as root, each worker gets its own uid (`10000 + domainId`) and cgroup v2 memory and CPU limits. Everywhere else, the `supervisor: enforcement: limits <on|off>, uids <on|off>[: reason]` log line names what could not be enforced and why. Without uid separation a worker runs as the supervisor's own user, and a `FILE` submission key reachable by that user is reachable by the worker too: there is no OS-level barrier between them.

## Exit statuses

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

A worker that reports a batch whose commitment differs from the one the supervisor already holds for that batchId, without retracting it first, is a consistency fault for that domain: it is killed and HALTED, never restarted, and its last exit reads "consistency fault". Other domains continue.

The supervisor itself exits 0 on a clean shutdown, 1 on any other failure to run, and 78 (`ipc.ExitConfig`) on an invalid configuration or submission key.
