# On-disk layout

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

An older `<storagePath>/<executorAddress>.lock` or `<storagePath>/wasm-code/` from before workers moved into their own processes is ignored under the new layout: `mv` the code store into the domain directory it now belongs under, or let the worker re-sync it.

Every existing data directory with at least one batch record halts on its first boot under this build. No earlier build submitted, so no domain has a finalized batch and every record is above the anchor, and records written by an earlier build carry no stored commitment. The worker exits HALTED, naming the batchId and the domain directory. The remedy for every domain on upgrade: stop the executor, remove that domain's directory and re-sync.

While a worker rebuilds batches below Settlement's `nextBatchId`, it closes each one exactly at the accepted entry's input range and checks it against that entry: the protocol version, the input range, the six roots, the data-availability hash and mode, the asset flow summary and the proof data. A re-sync from an empty domain directory therefore reproduces the accepted batches and continues past them. While it rebuilds, the worker reads ahead only to the end of the batch it is rebuilding, so it commits after every accepted batch that ends at the domain's last input in its momentum. A history this executor built therefore commits during a re-sync as often as it did when it was first built.

Accepted batches that never end at the domain's last input in a momentum, such as batches built by a different executor, stay uncommitted in memory until one does or the rebuild passes `nextBatchId`. Each costs about 2 KB, about 190 MB per 100,000 batches. A crash during that stretch repeats the rebuild from the last commit.

A rebuilt batch that differs from its accepted entry, or an input stream that cannot end a batch where the entry does, halts the domain: the worker exits HALTED, naming the batchId and the domain directory, and the supervisor does not restart it. The usual cause is a domain directory whose batches are not the accepted ones. The remedy: stop the executor, remove that domain's directory and re-sync. If a re-sync from an empty directory halts the same way, this worker binary and its `stfSpecHash` do not reproduce the accepted batches, and re-syncing cannot fix it.

A domain directory written by an earlier build can also hold more unsubmitted batches than the supervisor now accepts (`maxPendingBatches` plus 2,048). Its worker is killed at every boot until the domain is HALTED as a crash loop. The remedy is the same: stop the executor, remove that domain's directory and re-sync.
