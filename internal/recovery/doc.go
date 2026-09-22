// Package recovery owns the executor's on-disk storage layout, atomic cursor persistence, and
// the boot-to-ready recovery state machine. A position is durable only when the cursor file
// names it: Commit writes the snapshot, batch records and momentum window first and renames
// the cursor over last. Boot loads the cursor's snapshot, reconciles it against the most
// recently finalized on-chain root, and halts on any consistency fault; Rollback restores the
// newest committed position at or below a fork parent when the ledger reorgs.
package recovery
