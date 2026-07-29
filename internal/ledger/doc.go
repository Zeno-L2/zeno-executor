// Package ledger will hold the input pipeline: following the znnd confirmation frontier,
// reconstructing the canonical input stream from confirmed momenta, assigning the global
// input index, filtering to a configured domain, and maintaining that domain's
// input-sequence cursor.
package ledger
