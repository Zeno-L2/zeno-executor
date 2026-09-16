// Package ledger holds the input pipeline: following the znnd confirmation frontier,
// reconstructing the canonical input stream from confirmed momenta, assigning the global
// input index, filtering to a configured domain, and maintaining that domain's
// input-sequence cursor. A Follower is single-goroutine, not safe for concurrent use.
package ledger
