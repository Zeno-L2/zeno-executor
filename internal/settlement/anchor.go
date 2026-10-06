package settlement

import (
	"github.com/zenon-network/go-zenon/vm/embedded/definition"

	"github.com/digitalSloth/go-executor/internal/recovery"
)

// AnchorSource reads each domain's recovery anchor from Settlement storage at the confirmed
// height, which is the frontier minus the confirmation depth.
type AnchorSource struct {
	r     *Reader
	depth uint64
}

// NewAnchorSource returns an anchor source over r.
func NewAnchorSource(r *Reader, confirmationDepth uint64) AnchorSource {
	return AnchorSource{r: r, depth: confirmationDepth}
}

// FinalizedAnchor returns the post-state root and last input sequence of the domain's latest
// finalized batch, or false when no batch is finalized (genesis).
func (a AnchorSource) FinalizedAnchor(domainID uint32) (recovery.Anchor, bool, error) {
	h, err := a.r.ConfirmedHeight(a.depth)
	if err != nil {
		return recovery.Anchor{}, false, err
	}
	v, err := a.r.At(h)
	if err != nil {
		return recovery.Anchor{}, false, err
	}
	return finalizedAnchor(v, h, domainID)
}

// batchReader is the part of a View finalizedAnchor reads.
type batchReader interface {
	InputSequenceCursor(domainID uint32) (*definition.InputSequenceCursor, error)
	BatchEntry(domainID uint32, batchID uint64) (*definition.BatchEntry, bool, error)
}

// finalizedAnchor walks the domain's batches in order and returns the last one finalized at
// height, stopping at the first that is not.
//
// Settlement sweeps only finalized batches, in batch order, at heights up to the swept height,
// and a batch finalized at one height stays finalized at every later one. So every batch below
// RecordsSweptThrough is finalized at height, and the walk starts there to read only the
// unswept tail.
func finalizedAnchor(v batchReader, height uint64, domainID uint32) (recovery.Anchor, bool, error) {
	cur, err := v.InputSequenceCursor(domainID)
	if err != nil {
		return recovery.Anchor{}, false, err
	}
	if cur.NextBatchId == 0 {
		return recovery.Anchor{}, false, nil
	}

	start := cur.RecordsSweptThrough
	var last *definition.BatchEntry
	for b := start; b < cur.NextBatchId; b++ {
		e, ok, err := v.BatchEntry(domainID, b)
		if err != nil {
			return recovery.Anchor{}, false, err
		}
		if !ok {
			return recovery.Anchor{}, false, halted(height, "domain %d batch %d is below nextBatchId %d but has no entry", domainID, b, cur.NextBatchId)
		}
		if !e.IsFinalized(height) {
			break
		}
		last = e
	}

	if last == nil {
		if start == 0 {
			return recovery.Anchor{}, false, nil
		}
		e, ok, err := v.BatchEntry(domainID, start-1)
		if err != nil {
			return recovery.Anchor{}, false, err
		}
		if !ok {
			return recovery.Anchor{}, false, halted(height, "domain %d batch %d is below recordsSweptThrough %d but has no entry", domainID, start-1, start)
		}
		last = e
	}
	return recovery.Anchor{PostStateRoot: last.PostStateRoot, LastInputSeq: last.LastInputSeq}, true, nil
}
