// Package recovery owns the executor's on-disk storage layout, atomic cursor persistence, and
// the boot-to-ready recovery state machine. A position is durable only when the cursor file
// names it: Commit writes the snapshot, batch records and momentum window first and renames
// the cursor over last. Boot loads the cursor's snapshot, reconciles it against the most
// recently finalized on-chain root, and halts on any consistency fault; Rollback restores the
// newest committed position at or below a fork parent when the ledger reorgs.
//
// bundles/, receipts/ and batches/ are the gateway's read interface to
// a worker. Their file formats are versioned like a wire format: a
// change a reader cannot parse is a new version, never an edit in
// place. The batch driver stores a batch's receipt sidecar after its
// bundle and before Commit writes the batch record, so a record always
// names a bundle and a sidecar that exist. Those three directories are
// 0755 and their files 0644, inside a 0711 domain directory, so another
// user can read them by name but cannot list the domain directory.
// Every other file this package writes is 0600. NewLayout, the two
// stores and writeFileAtomic set these modes with an explicit chmod,
// never through the umask.
package recovery
