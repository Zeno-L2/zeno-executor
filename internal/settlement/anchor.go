package settlement

import (
	"bytes"
	"errors"
	"fmt"

	"github.com/zenon-network/go-zenon/common/types"
	"github.com/zenon-network/go-zenon/vm/embedded/definition"

	"github.com/digitalSloth/go-executor/internal/codec"
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

// Settled is a domain's recovery anchor and Settlement's batch cursor, read in one view.
type Settled struct {
	Anchor      recovery.Anchor
	HaveAnchor  bool   // false at genesis
	NextBatchID uint64 // the cursor's NextBatchId
}

// Settled returns the domain's latest finalized batch anchor and its nextBatchId at the
// confirmed height.
func (a AnchorSource) Settled(domainID uint32) (Settled, error) {
	h, err := a.r.ConfirmedHeight(a.depth)
	if err != nil {
		return Settled{}, err
	}
	v, err := a.r.At(h)
	if err != nil {
		return Settled{}, err
	}
	return settled(v, h, domainID)
}

// NextBatchID returns the domain's nextBatchId at the confirmed height, reading only the cursor.
func (a AnchorSource) NextBatchID(domainID uint32) (uint64, error) {
	h, err := a.r.ConfirmedHeight(a.depth)
	if err != nil {
		return 0, err
	}
	v, err := a.r.At(h)
	if err != nil {
		return 0, err
	}
	cur, err := v.InputSequenceCursor(domainID)
	if err != nil {
		return 0, err
	}
	return cur.NextBatchId, nil
}

// AcceptedEntry returns the domain's stored entry for batchID at the confirmed height. A batchID
// not below that height's nextBatchId is a plain error; a missing entry below it is an error
// wrapping recovery.ErrHalted.
func (a AnchorSource) AcceptedEntry(domainID uint32, batchID uint64) (*definition.BatchEntry, error) {
	h, err := a.r.ConfirmedHeight(a.depth)
	if err != nil {
		return nil, err
	}
	v, err := a.r.At(h)
	if err != nil {
		return nil, err
	}
	return acceptedEntry(v, h, domainID, batchID)
}

// Accepted is a run of a domain's accepted entries and its nextBatchId, read in one view.
type Accepted struct {
	Height      uint64
	NextBatchID uint64
	Entries     []*definition.BatchEntry // consecutive batchIds from the one asked for, all below NextBatchID
}

// AcceptedEntries returns the domain's nextBatchId and at most max of its stored entries for
// consecutive batchIds from `from`, each below that nextBatchId, in one view at the confirmed
// height. The list is empty when from is at or above nextBatchId. A missing entry below
// nextBatchId is an error wrapping recovery.ErrHalted.
func (a AnchorSource) AcceptedEntries(domainID uint32, from uint64, max int) (Accepted, error) {
	h, err := a.r.ConfirmedHeight(a.depth)
	if err != nil {
		return Accepted{}, err
	}
	v, err := a.r.At(h)
	if err != nil {
		return Accepted{}, err
	}
	return acceptedEntries(v, h, domainID, from, max)
}

// Tip is what a domain's next batch must extend, read in one view at the confirmed height.
type Tip struct {
	Height      uint64                 // the confirmed height read
	NextBatchID uint64                 // the batchId the next accepted batch must carry
	Prev        *definition.BatchEntry // the entry at NextBatchID-1; nil when NextBatchID is 0
	FirstInput  uint64                 // GlobalInputIndex at ordinal ConsumedInputCount
	HaveInput   bool                   // false when that ordinal is absent
}

// Tip returns what the domain's next batch must extend, at the confirmed height.
func (a AnchorSource) Tip(domainID uint32) (Tip, error) {
	h, err := a.r.ConfirmedHeight(a.depth)
	if err != nil {
		return Tip{}, err
	}
	v, err := a.r.At(h)
	if err != nil {
		return Tip{}, err
	}
	return tip(v, h, domainID)
}

// Gates is what Settlement's submit checks read besides the batch and the cursor, read in one
// view at the confirmed height.
type Gates struct {
	Height       uint64                        // the confirmed height read
	GlobalPause  bool                          // the global PauseState's SubmitPaused
	DomainPause  bool                          // the domain's PauseState's SubmitPaused
	Member       *definition.ExecutorSetMember // pillarName's executor-set member; nil when it has none
	PillarActive bool                          // pillarName is registered and not revoked
}

// Gates reads the submit pauses and pillarName's standing, at the confirmed height.
func (a AnchorSource) Gates(domainID uint32, pillarName string) (Gates, error) {
	h, err := a.r.ConfirmedHeight(a.depth)
	if err != nil {
		return Gates{}, err
	}
	v, err := a.r.At(h)
	if err != nil {
		return Gates{}, err
	}
	return gates(v, h, domainID, pillarName)
}

// gateReader is the part of a View gates reads.
type gateReader interface {
	GlobalPauseState() (definition.PauseState, error)
	DomainPauseState(domainID uint32) (definition.PauseState, error)
	ExecutorSetMember(pillarName string) (*definition.ExecutorSetMember, bool, error)
	PillarActive(name string) (bool, error)
}

// gates reads the global pause, the domain's pause, the member and the pillar, in that order,
// all from v. Read errors return unchanged.
func gates(v gateReader, height uint64, domainID uint32, pillarName string) (Gates, error) {
	global, err := v.GlobalPauseState()
	if err != nil {
		return Gates{}, err
	}
	domain, err := v.DomainPauseState(domainID)
	if err != nil {
		return Gates{}, err
	}
	g := Gates{Height: height, GlobalPause: global.SubmitPaused, DomainPause: domain.SubmitPaused}
	m, ok, err := v.ExecutorSetMember(pillarName)
	if err != nil {
		return Gates{}, err
	}
	if ok {
		g.Member = m
	}
	g.PillarActive, err = v.PillarActive(pillarName)
	if err != nil {
		return Gates{}, err
	}
	return g, nil
}

// batchReader is the part of a View settled, acceptedEntry and tip read.
type batchReader interface {
	InputSequenceCursor(domainID uint32) (*definition.InputSequenceCursor, error)
	BatchEntry(domainID uint32, batchID uint64) (*definition.BatchEntry, bool, error)
	DomainInputOrdinal(domainID uint32, ordinal uint64) (*definition.DomainInputOrdinal, bool, error)
}

// tip reads the cursor, then the entry below its nextBatchId, then the domain's next unconsumed
// input, all from v. A missing entry below nextBatchId is an error wrapping recovery.ErrHalted.
func tip(v batchReader, height uint64, domainID uint32) (Tip, error) {
	cur, err := v.InputSequenceCursor(domainID)
	if err != nil {
		return Tip{}, err
	}
	t := Tip{Height: height, NextBatchID: cur.NextBatchId}
	if cur.NextBatchId > 0 {
		e, ok, err := v.BatchEntry(domainID, cur.NextBatchId-1)
		if err != nil {
			return Tip{}, err
		}
		if !ok {
			return Tip{}, halted(height, "domain %d batch %d is below nextBatchId %d but has no entry", domainID, cur.NextBatchId-1, cur.NextBatchId)
		}
		t.Prev = e
	}
	o, ok, err := v.DomainInputOrdinal(domainID, cur.ConsumedInputCount)
	if err != nil {
		return Tip{}, err
	}
	if ok {
		t.FirstInput = o.GlobalInputIndex
		t.HaveInput = true
	}
	return t, nil
}

// settled walks the domain's batches in order and returns the last one finalized at height,
// stopping at the first that is not, with the cursor's nextBatchId.
//
// Settlement sweeps only finalized batches, in batch order, at heights up to the swept height,
// and a batch finalized at one height stays finalized at every later one. So every batch below
// RecordsSweptThrough is finalized at height, and the walk starts there to read only the
// unswept tail.
func settled(v batchReader, height uint64, domainID uint32) (Settled, error) {
	cur, err := v.InputSequenceCursor(domainID)
	if err != nil {
		return Settled{}, err
	}
	if cur.NextBatchId == 0 {
		return Settled{}, nil
	}

	start := cur.RecordsSweptThrough
	var last *definition.BatchEntry
	for b := start; b < cur.NextBatchId; b++ {
		e, ok, err := v.BatchEntry(domainID, b)
		if err != nil {
			return Settled{}, err
		}
		if !ok {
			return Settled{}, halted(height, "domain %d batch %d is below nextBatchId %d but has no entry", domainID, b, cur.NextBatchId)
		}
		if !e.IsFinalized(height) {
			break
		}
		last = e
	}

	if last == nil {
		if start == 0 {
			return Settled{NextBatchID: cur.NextBatchId}, nil
		}
		e, ok, err := v.BatchEntry(domainID, start-1)
		if err != nil {
			return Settled{}, err
		}
		if !ok {
			return Settled{}, halted(height, "domain %d batch %d is below recordsSweptThrough %d but has no entry", domainID, start-1, start)
		}
		last = e
	}
	return Settled{
		Anchor:      recovery.Anchor{PostStateRoot: last.PostStateRoot, LastInputSeq: last.LastInputSeq},
		HaveAnchor:  true,
		NextBatchID: cur.NextBatchId,
	}, nil
}

// acceptedEntry reads the cursor and the entry for batchID from one view.
//
// A batchID not below the cursor is a plain error rather than a halt: the view's frontier can
// sit below the one an earlier cursor read came from (a reorg in progress, or a load-balanced
// node), so the batch may not be visible yet.
func acceptedEntry(v batchReader, height uint64, domainID uint32, batchID uint64) (*definition.BatchEntry, error) {
	cur, err := v.InputSequenceCursor(domainID)
	if err != nil {
		return nil, err
	}
	if batchID >= cur.NextBatchId {
		return nil, fmt.Errorf("settlement: domain %d batch %d is not below nextBatchId %d at height %d", domainID, batchID, cur.NextBatchId, height)
	}
	e, ok, err := v.BatchEntry(domainID, batchID)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, halted(height, "domain %d batch %d is below nextBatchId %d but has no entry", domainID, batchID, cur.NextBatchId)
	}
	return e, nil
}

// acceptedEntries reads the cursor, then up to max entries from `from` below it, from one view.
func acceptedEntries(v batchReader, height uint64, domainID uint32, from uint64, max int) (Accepted, error) {
	cur, err := v.InputSequenceCursor(domainID)
	if err != nil {
		return Accepted{}, err
	}
	out := Accepted{Height: height, NextBatchID: cur.NextBatchId}
	for b := from; b < cur.NextBatchId && len(out.Entries) < max; b++ {
		e, ok, err := v.BatchEntry(domainID, b)
		if err != nil {
			return Accepted{}, err
		}
		if !ok {
			return Accepted{}, halted(height, "domain %d batch %d is below nextBatchId %d but has no entry", domainID, b, cur.NextBatchId)
		}
		out.Entries = append(out.Entries, e)
	}
	return out, nil
}

// EntryMatches returns nil when e equals c on the protocol version, the input range, the six
// roots, the data-availability hash and mode, the asset flow summary and the proof data, else an
// error naming the first differing field. It ignores the fields Settlement assigns on
// acceptance. The error is not wrapped in recovery.ErrHalted.
func EntryMatches(e *definition.BatchEntry, c codec.BatchCommitment) error {
	if e.ProtocolVersion != c.Versions.ProtocolVersion {
		return fmt.Errorf("protocolVersion %d, want %d", e.ProtocolVersion, c.Versions.ProtocolVersion)
	}
	if e.FirstInputSeq != c.FirstInputSeq {
		return fmt.Errorf("firstInputSeq %d, want %d", e.FirstInputSeq, c.FirstInputSeq)
	}
	if e.LastInputSeq != c.LastInputSeq {
		return fmt.Errorf("lastInputSeq %d, want %d", e.LastInputSeq, c.LastInputSeq)
	}
	roots := []struct {
		name      string
		got, want types.Hash
	}{
		{"preStateRoot", e.PreStateRoot, c.PreStateRoot},
		{"postStateRoot", e.PostStateRoot, c.PostStateRoot},
		{"inputRoot", e.InputRoot, c.InputRoot},
		{"receiptRoot", e.ReceiptRoot, c.ReceiptRoot},
		{"eventRoot", e.EventRoot, c.EventRoot},
		{"outboxRoot", e.OutboxRoot, c.OutboxRoot},
		{"daHash", e.DaHash, c.DAHash},
	}
	for _, r := range roots {
		if r.got != r.want {
			return fmt.Errorf("%s %s, want %s", r.name, r.got, r.want)
		}
	}
	if e.DaMode != c.DAMode {
		return fmt.Errorf("daMode %d, want %d", e.DaMode, c.DAMode)
	}
	flow, err := c.AssetFlowSummary.Encode()
	if err != nil {
		return fmt.Errorf("assetFlowSummary: %w", err)
	}
	if !bytes.Equal(e.AssetFlowSummary, flow) {
		return errors.New("assetFlowSummary differs")
	}
	if !bytes.Equal(e.ProofData, c.ProofData) {
		return errors.New("proofData differs")
	}
	return nil
}
