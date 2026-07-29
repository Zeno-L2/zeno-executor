// Package recovery will hold the on-disk storage layout, atomic cursor persistence, and the
// boot-to-ready recovery state machine that reconciles local state against the most
// recently finalized on-chain root before resuming, including reorg and fail-closed
// handling.
package recovery
