// Package codec holds the canonical binary encoding for every consensus-visible structure
// the executor produces or consumes: execution results, receipts, events, outbox messages,
// batch commitments, and DA bundles. Every structure has exactly one valid encoding, and
// decoding rejects anything else.
package codec
