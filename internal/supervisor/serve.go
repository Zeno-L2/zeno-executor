package supervisor

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"reflect"
	"sync"
	"time"

	"github.com/zenon-network/go-zenon/common/types"
	"github.com/zenon-network/go-zenon/vm/embedded/definition"

	"github.com/digitalSloth/go-executor/internal/codec"
	"github.com/digitalSloth/go-executor/internal/ipc"
	"github.com/digitalSloth/go-executor/internal/ledger"
	"github.com/digitalSloth/go-executor/internal/recovery"
	"github.com/digitalSloth/go-executor/internal/settlement"
	"github.com/digitalSloth/go-executor/internal/znnd"
)

// ErrMalformed marks a worker frame that breaks the protocol: over the frame cap, an unknown or
// wrong-direction type, a bad payload, a Committed or Recovered that fails a field check or
// exceeds the held-commitment cap, or a Committed or Recovered out of the session's frame order
// (internal/ipc/frame.go).
var ErrMalformed = errors.New("supervisor: malformed worker message")

// ErrInconsistent marks a worker frame that conflicts with a commitment the supervisor holds for
// the same batchId.
var ErrInconsistent = errors.New("supervisor: consistency fault")

// workerBufferedInputs mirrors the follower buffer the worker runs with, commitInputInterval in
// runtime/loop.go, which is passed as the follower's MaxBufferedInputs.
const workerBufferedInputs = 1024

// heldCap is the most commitments the supervisor holds for one domain: a new batchId over it is
// a malformed frame. It is maxPendingBatches plus two worker buffers of inputs.
//
// The held set's floor is the supervisor's observed nextBatchId: every batchId below it is
// pruned or ignored. The worker's own view of nextBatchId is a Cursor reply, so it is never above
// that mark. The worker stops sealing at exactly maxPendingBatches batches above its view,
// pending ones included. After that it keeps building until the follower reports a checkpoint,
// which needs an empty buffer (internal/ledger/follower.go). That buffer holds one full refill of
// up to workerBufferedInputs inputs, plus the rest of the momentum the refill ends in. The
// momentum carries at most workerBufferedInputs Settlement receives, and Settlement hands each
// receive at most one domain input (settlement.oneInputPerSend), so it carries at most that many
// inputs. Every later batch takes at least one input, so the batches built after the stop number
// at most 2*workerBufferedInputs.
//
// After the dynamic plasma spork, the Settlement receives in one momentum are bounded by
// maxBasePlasmaInMomentum / EmbeddedSimplePlasma (go-zenon dp/dp.go), 80 at the default. The
// bound on a momentum holds unless that variable rises to roughly 10 times its default.
func heldCap(maxPendingBatches uint64) int {
	return int(maxPendingBatches) + 2*workerBufferedInputs
}

// AnchorSource reads a domain's Settlement state at the confirmed height.
type AnchorSource interface {
	// Settled returns the domain's latest finalized anchor and its nextBatchId.
	Settled(domainID uint32) (settlement.Settled, error)
	// NextBatchID returns the domain's nextBatchId.
	NextBatchID(domainID uint32) (uint64, error)
	// AcceptedEntry returns the stored entry for a batch below nextBatchId.
	AcceptedEntry(domainID uint32, batchID uint64) (*definition.BatchEntry, error)
	// AcceptedEntries returns the domain's nextBatchId and at most max accepted entries from
	// batchId `from`, each below that nextBatchId, read in one view.
	AcceptedEntries(domainID uint32, from uint64, max int) (settlement.Accepted, error)
	// Tip returns what the domain's next batch must extend.
	Tip(domainID uint32) (settlement.Tip, error)
	// Gates returns the submit pauses for the domain and pillarName's standing.
	Gates(domainID uint32, pillarName string) (settlement.Gates, error)
}

// Sources is what serve reads to answer a worker's requests. Run also reads it: it calls Ready
// before starting any worker.
type Sources struct {
	Momenta ledger.MomentumSource
	Anchors AnchorSource
	// Assignments is required once a worker sends an AssignmentRequest.
	Assignments ledger.AssignmentSource
	// Caps is required once a worker sends a CapRequest.
	Caps  ledger.CapSource
	Ready func(ctx context.Context) bool // nil: Run starts workers without waiting
	// Node is the node the submitter publishes through. nil: Run submits nothing and opens no
	// submission log. New's key must be non-nil when it is set.
	Node SendNode
}

// workerState is shared by serve and the worker's supervision goroutine, guarded by mu.
type workerState struct {
	mu            sync.Mutex
	lastFrame     time.Time
	servedHeight  uint64
	momentaCount  uint64 // count serveMomenta tries first; 0 until a read is clipped or halved
	lastCommitted *codec.BatchCommitment
	highWater     uint64 // highest LastInputSeq committed; a Committed above it is progress
	progressed    bool   // set by a Committed above highWater; cleared by the supervision goroutine
	// commitments holds every batch the worker reported and has not retracted, keyed by batchId.
	// It carries across worker runs like highWater and is created on first write.
	commitments map[uint64]codec.BatchCommitment
	fault       string // the consistency fault's error text; empty until one occurs

	// chainNext is the highest nextBatchId read at the confirmed height, 0 before any read. Serve's
	// reads, the submitter's Tip read and the submitter's accepted batches all raise it, and the
	// submission log's latest accepted batches seed it at boot; nothing lowers it. Every batchId
	// below it is pruned from commitments and ignored when reported.
	chainNext uint64
	// accepted is the chain entry compareEntry read most recently; nil until then.
	accepted *definition.BatchEntry
	// owed is a comparison against the chain's entry that an outage left unmade; nil when none.
	// A later owed comparison replaces an earlier one: the state-root chain covers every
	// commitment that is not compared.
	owed *codec.BatchCommitment
	// unresolved is the submitter's pending send for this domain; nil when none.
	unresolved *unresolvedSend
	// submitError is the submitter's stall status for this domain; empty when none.
	submitError string
	// rejectError is the submitter's rejection status for this domain: set once Settlement has
	// rejected the domain's next batch rejectAlert times in a row; empty otherwise.
	rejectError string

	maxHeld  int           // the held-commitment cap; set by New and never written after
	halt     chan struct{} // closed by haltDomain when it records a fault; set by New and never written after
	endpoint string        // the znnd endpoint's scheme and host, named in log lines; set by New and never written after
}

// serve answers conn's requests from src until EOF, a short read or a write failure (nil), or a
// malformed frame (an error wrapping ErrMalformed) or a consistency fault (an error wrapping
// ErrInconsistent).
func serve(conn io.ReadWriter, w WorkerConfig, executor types.Address, src Sources, st *workerState) error {
	var order frameOrder
	for {
		m, err := ipc.ReadMessage(conn, ipc.Worker)
		if err != nil {
			if errors.Is(err, ipc.ErrMalformed) {
				return fmt.Errorf("%w: %w", ErrMalformed, err)
			}
			return nil
		}

		st.mu.Lock()
		st.lastFrame = time.Now()
		st.mu.Unlock()

		if err := order.next(m); err != nil {
			return err
		}

		switch m.(type) {
		case ipc.FrontierRequest, ipc.AnchorRequest, ipc.AssignmentRequest, ipc.CapRequest, ipc.MomentaRequest, ipc.CursorRequest, ipc.AcceptedRequest:
			if err := settleOwed(src, st); err != nil {
				return err
			}
		}

		switch v := m.(type) {
		case ipc.FrontierRequest:
			if !serveFrontier(conn, src) {
				return nil
			}

		case ipc.AnchorRequest:
			if !serveAnchor(conn, w, src, st) {
				return nil
			}

		case ipc.CursorRequest:
			if !serveCursor(conn, w, src, st) {
				return nil
			}

		case ipc.AcceptedRequest:
			if !serveAccepted(conn, v, w, src, st) {
				return nil
			}

		case ipc.AssignmentRequest:
			if !serveAssignment(conn, v, w, src) {
				return nil
			}

		case ipc.CapRequest:
			if !serveCap(conn, v, w, src) {
				return nil
			}

		case ipc.MomentaRequest:
			if !serveMomenta(conn, v, src, st) {
				return nil
			}

		case ipc.Committed:
			if err := recordCommitted(v.Commitment, w, executor, src, st); err != nil {
				return err
			}

		case ipc.Recovered:
			if err := recordRecovered(v.Commitment, w, executor, src, st); err != nil {
				return err
			}

		case ipc.Retract:
			retract(v.FromBatchID, st)
		}
	}
}

// serveFrontier answers a FrontierRequest. It returns false when the reply write failed, telling
// serve to end the session with nil.
func serveFrontier(conn io.ReadWriter, src Sources) bool {
	height, err := src.Momenta.FrontierHeight()
	var reply ipc.Message
	if err != nil {
		reply = sourceError(err)
	} else {
		reply = ipc.Frontier{Height: height}
	}
	return ipc.WriteMessage(conn, reply) == nil
}

// serveAnchor answers an AnchorRequest for w's own domain. A source anchor reported not found is
// sent as the zero anchor: ipc.Encode rejects a not-found anchor with a non-zero root or seq. The
// read's nextBatchId goes through observeNext.
func serveAnchor(conn io.ReadWriter, w WorkerConfig, src Sources, st *workerState) bool {
	s, err := src.Anchors.Settled(w.DomainID)
	var reply ipc.Message
	switch {
	case err != nil:
		reply = sourceError(err)
	default:
		observeNext(w.DomainID, s.NextBatchID, st)
		if s.HaveAnchor {
			reply = ipc.Anchor{Found: true, PostStateRoot: s.Anchor.PostStateRoot, LastInputSeq: s.Anchor.LastInputSeq}
		} else {
			reply = ipc.Anchor{}
		}
	}
	return ipc.WriteMessage(conn, reply) == nil
}

// serveCursor answers a CursorRequest for w's own domain with the observed nextBatchId, which is
// the highest read so far rather than this read. It returns false when the reply write failed.
func serveCursor(conn io.ReadWriter, w WorkerConfig, src Sources, st *workerState) bool {
	n, err := src.Anchors.NextBatchID(w.DomainID)
	if err != nil {
		return ipc.WriteMessage(conn, sourceError(err)) == nil
	}
	return ipc.WriteMessage(conn, ipc.Cursor{NextBatchID: observeNext(w.DomainID, n, st)}) == nil
}

// serveAccepted answers an AcceptedRequest for w's own domain with the entries from
// v.FromBatchID and the observed nextBatchId, which is the highest read so far rather than this
// read. A read that reports no entries from a batchId below the observed nextBatchId is a lagging
// node: it gets a SourceError instead of an empty Accepted. It returns false when the reply write
// failed.
func serveAccepted(conn io.ReadWriter, v ipc.AcceptedRequest, w WorkerConfig, src Sources, st *workerState) bool {
	a, err := src.Anchors.AcceptedEntries(w.DomainID, v.FromBatchID, ipc.MaxAcceptedEntries)
	if err != nil {
		return ipc.WriteMessage(conn, sourceError(err)) == nil
	}
	mark := observeNext(w.DomainID, a.NextBatchID, st)
	if len(a.Entries) == 0 && v.FromBatchID < mark {
		msg := fmt.Sprintf("supervisor: domain %d batch %d is below the observed nextBatchId %d, but the node reports %d at height %d", w.DomainID, v.FromBatchID, mark, a.NextBatchID, a.Height)
		return ipc.WriteMessage(conn, ipc.SourceError{Kind: ipc.SourceUnavailable, Message: cut(msg)}) == nil
	}
	return ipc.WriteMessage(conn, ipc.Accepted{NextBatchID: mark, Entries: a.Entries}) == nil
}

// observeNext raises st.chainNext to read when read is higher and drops every held commitment
// below the result. A read below st.chainNext (a lagging node behind a load-balanced endpoint)
// changes nothing and is logged. It returns st.chainNext after the read. The caller holds no
// lock.
func observeNext(domainID uint32, read uint64, st *workerState) uint64 {
	st.mu.Lock()
	mark := raiseNext(st, read)
	st.mu.Unlock()
	if read < mark {
		log.Printf("supervisor: domain %d WARN: znnd node at %s reported nextBatchId %d, below the observed %d; keeping the observed value", domainID, st.endpoint, read, mark)
	}
	return mark
}

// raiseNext raises st.chainNext to read when read is higher and drops every held commitment
// below the result. It returns st.chainNext. The caller holds st.mu.
func raiseNext(st *workerState, read uint64) uint64 {
	if read > st.chainNext {
		st.chainNext = read
		for id := range st.commitments {
			if id < read {
				delete(st.commitments, id)
			}
		}
	}
	return st.chainNext
}

// serveAssignment answers an AssignmentRequest for w's own domain. It returns false when the
// reply write failed.
func serveAssignment(conn io.ReadWriter, v ipc.AssignmentRequest, w WorkerConfig, src Sources) bool {
	entries, err := src.Assignments.Assignment(w.DomainID, v.Height, v.Hash, v.NextOrdinal)
	if err != nil {
		return ipc.WriteMessage(conn, sourceError(err)) == nil
	}
	b, err := ipc.Encode(ipc.Assignment{Entries: entries})
	if err != nil {
		msg := cut(fmt.Sprintf("supervisor: assignment at height %d cannot be sent: %s", v.Height, err))
		return ipc.WriteMessage(conn, ipc.SourceError{Kind: ipc.SourceHalted, Message: msg}) == nil
	}
	_, err = conn.Write(b)
	return err == nil
}

// serveCap answers a CapRequest for w's own domain. It returns false when the reply write
// failed.
func serveCap(conn io.ReadWriter, v ipc.CapRequest, w WorkerConfig, src Sources) bool {
	amounts, err := src.Caps.MaxBatchWithdrawals(w.DomainID, v.Height, v.Hash, v.Assets)
	if err != nil {
		return ipc.WriteMessage(conn, sourceError(err)) == nil
	}
	return ipc.WriteMessage(conn, ipc.Cap{Amounts: amounts}) == nil
}

// serveMomenta answers a MomentaRequest. It reads at most v.Count momenta, starting at
// st.momentaCount when that is lower. It halves the count while the node's response exceeds znnd's
// body bound, down to 1, and records in st.momentaCount the count the next read starts at: double a
// clipped count that succeeded, up to v.Count, or the last count tried after a halving that
// failed. It keeps at most v.Count results, encodes every kept entry before writing anything, and
// raises st.servedHeight only once RangeEnd is written. It returns false when a reply write failed.
func serveMomenta(conn io.ReadWriter, v ipc.MomentaRequest, src Sources, st *workerState) bool {
	st.mu.Lock()
	start := st.momentaCount
	st.mu.Unlock()
	if start == 0 || start > v.Count {
		start = v.Count
	}
	count := start
	ms, err := src.Momenta.DetailedMomentumsByHeight(v.Height, count)
	for errors.Is(err, znnd.ErrResponseTooLarge) && count > 1 {
		count /= 2
		ms, err = src.Momenta.DetailedMomentumsByHeight(v.Height, count)
	}
	st.mu.Lock()
	switch {
	case err == nil && count < v.Count:
		st.momentaCount = min(2*count, v.Count)
	case err != nil && count < start:
		st.momentaCount = count
	}
	st.mu.Unlock()
	if err != nil {
		return ipc.WriteMessage(conn, sourceError(err)) == nil
	}

	kept := ms
	if uint64(len(kept)) > v.Count {
		kept = kept[:v.Count]
	}

	encoded := make([][]byte, 0, len(kept))
	for i, dm := range kept {
		b, err := ipc.Encode(ipc.Momentum{Detailed: dm})
		if err != nil {
			msg := cut(fmt.Sprintf("supervisor: momentum %d of the range from height %d cannot be sent: %s", i, v.Height, err))
			return ipc.WriteMessage(conn, ipc.SourceError{Kind: ipc.SourceHalted, Message: msg}) == nil
		}
		encoded = append(encoded, b)
	}

	for _, b := range encoded {
		if _, err := conn.Write(b); err != nil {
			return false
		}
	}
	if err := ipc.WriteMessage(conn, ipc.RangeEnd{Count: uint32(len(kept))}); err != nil {
		return false
	}

	if len(kept) > 0 {
		maxHeight := kept[0].Momentum.Height
		for _, dm := range kept[1:] {
			if dm.Momentum.Height > maxHeight {
				maxHeight = dm.Momentum.Height
			}
		}
		st.mu.Lock()
		if maxHeight > st.servedHeight {
			st.servedHeight = maxHeight
		}
		st.mu.Unlock()
	}
	return true
}

// recordCommitted validates a worker's Committed frame and, if valid, records it. c is already
// strictly decoded by ReadMessage (codec.DecodeBatchCommitment, internal/ipc/messages.go).
//
// A frame below the observed nextBatchId is not held: Settlement already accepted that batch. It
// still counts as progress.
func recordCommitted(c codec.BatchCommitment, w WorkerConfig, executor types.Address, src Sources, st *workerState) error {
	if err := checkCommitment(c, w, executor); err != nil {
		return err
	}
	if err := checkTip(c, src, st); err != nil {
		return err
	}

	st.mu.Lock()
	defer st.mu.Unlock()
	if c.BatchID >= st.chainNext {
		added, err := hold(c, st)
		if err != nil || !added {
			return err
		}
	}
	cc := c
	st.lastCommitted = &cc
	if c.LastInputSeq > st.highWater {
		st.highWater = c.LastInputSeq
		st.progressed = true
	}
	return nil
}

// recordRecovered validates a worker's Recovered frame and, if valid and new, holds it and raises
// highWater. A re-report is never progress and never changes lastCommitted. A frame below the
// observed nextBatchId changes nothing.
func recordRecovered(c codec.BatchCommitment, w WorkerConfig, executor types.Address, src Sources, st *workerState) error {
	if err := checkCommitment(c, w, executor); err != nil {
		return err
	}
	if err := checkTip(c, src, st); err != nil {
		return err
	}

	st.mu.Lock()
	defer st.mu.Unlock()
	if c.BatchID < st.chainNext {
		return nil
	}
	added, err := hold(c, st)
	if err != nil || !added {
		return err
	}
	if c.LastInputSeq > st.highWater {
		st.highWater = c.LastInputSeq
	}
	return nil
}

// checkTip compares a commitment at the batch just below the observed nextBatchId with
// Settlement's accepted entry. The mark can rise between this check and the hold, because the
// submitter raises it on a Tip read or an acceptance; a frame the mark passes in between is not held, and the
// guard's state-root check on the next batch covers its content.
func checkTip(c codec.BatchCommitment, src Sources, st *workerState) error {
	st.mu.Lock()
	next := st.chainNext
	st.mu.Unlock()
	if next > 0 && c.BatchID == next-1 {
		return compareEntry(c, src, st)
	}
	return nil
}

// compareEntry checks c against the chain's accepted entry for its batchId. It reads the entry
// once and caches it, so repeated frames for one batchId cost one read. An outage on the read
// leaves the comparison owed to the next request frame. A mismatch, or an entry that is missing
// below nextBatchId, is a consistency fault. The caller holds no lock.
func compareEntry(c codec.BatchCommitment, src Sources, st *workerState) error {
	st.mu.Lock()
	e := st.accepted
	st.mu.Unlock()
	if e == nil || e.BatchId != c.BatchID {
		var err error
		e, err = src.Anchors.AcceptedEntry(c.DomainID, c.BatchID)
		if err != nil {
			if errors.Is(err, recovery.ErrHalted) {
				return fault(st, fmt.Errorf("%w: domain %d batch %d has no readable accepted entry: %w", ErrInconsistent, c.DomainID, c.BatchID, err))
			}
			st.mu.Lock()
			st.owed = &c
			st.mu.Unlock()
			return nil
		}
		st.mu.Lock()
		st.accepted = e
		st.mu.Unlock()
	}
	if err := settlement.EntryMatches(e, c); err != nil {
		return fault(st, fmt.Errorf("%w: domain %d batch %d differs from Settlement's accepted entry: %w", ErrInconsistent, c.DomainID, c.BatchID, err))
	}
	return nil
}

// fault records err as the consistency fault unless one is recorded, and returns err.
func fault(st *workerState, err error) error {
	st.mu.Lock()
	if st.fault == "" {
		st.fault = err.Error()
	}
	st.mu.Unlock()
	return err
}

// haltDomain records err as st's consistency fault unless one is recorded, and then closes
// st.halt. It reports whether it recorded err. err wraps ErrInconsistent. st comes from New. The
// caller holds no lock.
func haltDomain(st *workerState, err error) bool {
	st.mu.Lock()
	if st.fault != "" {
		st.mu.Unlock()
		return false
	}
	st.fault = err.Error()
	st.mu.Unlock()
	close(st.halt)
	return true
}

// frameOrder is one worker session's frame-order state, per the session rules in
// internal/ipc/frame.go.
type frameOrder struct {
	retracted bool   // a Retract arrived this session
	from      uint64 // the latest Retract's FromBatchID
	committed bool   // a Committed arrived since that Retract
	recovered bool   // a Recovered arrived since that Retract
	last      uint64 // the batchId of the latest Recovered since that Retract
}

// next checks m against the session's frame order and records it. Only Committed, Recovered and
// Retract are checked; every other message returns nil. A violation is an error wrapping
// ErrMalformed.
func (o *frameOrder) next(m ipc.Message) error {
	switch v := m.(type) {
	case ipc.Retract:
		o.retracted = true
		o.from = v.FromBatchID
		o.committed = false
		o.recovered = false
	case ipc.Committed:
		if !o.retracted {
			return fmt.Errorf("%w: Committed batch %d arrived before the session's first Retract", ErrMalformed, v.Commitment.BatchID)
		}
		o.committed = true
	case ipc.Recovered:
		id := v.Commitment.BatchID
		switch {
		case !o.retracted:
			return fmt.Errorf("%w: Recovered batch %d arrived before the session's first Retract", ErrMalformed, id)
		case o.committed:
			return fmt.Errorf("%w: Recovered batch %d follows a Committed with no Retract between", ErrMalformed, id)
		case id >= o.from:
			return fmt.Errorf("%w: Recovered batch %d is at or above the Retract from batch %d", ErrMalformed, id, o.from)
		case o.recovered && id <= o.last:
			return fmt.Errorf("%w: Recovered batch %d is not above the previous Recovered batch %d", ErrMalformed, id, o.last)
		}
		o.recovered = true
		o.last = id
	}
	return nil
}

// settleOwed makes the comparison an outage left owed. serve calls it before each request frame.
// The comparison runs even when the observed nextBatchId has risen since: an accepted entry does
// not change on any compared field. The caller holds no lock.
func settleOwed(src Sources, st *workerState) error {
	st.mu.Lock()
	o := st.owed
	if o == nil || o.BatchID >= st.chainNext {
		st.mu.Unlock()
		return nil
	}
	st.owed = nil
	st.mu.Unlock()
	return compareEntry(*o, src, st)
}

// retract drops every held commitment whose batchId is at or above from. It also flags the
// submitter's pending send as retracted when that send's batchId is at or above from.
func retract(from uint64, st *workerState) {
	st.mu.Lock()
	defer st.mu.Unlock()
	if st.unresolved != nil && st.unresolved.batchID >= from {
		st.unresolved.retracted = true
	}
	for id := range st.commitments {
		if id >= from {
			delete(st.commitments, id)
		}
	}
}

// hold stores c unless the supervisor already holds its batchId. An equal held commitment is a
// no-op; a different one is a consistency fault; a new batchId when the cap is reached is
// malformed. The caller holds st.mu.
func hold(c codec.BatchCommitment, st *workerState) (added bool, err error) {
	held, ok := st.commitments[c.BatchID]
	if !ok {
		if len(st.commitments) >= st.maxHeld {
			return false, fmt.Errorf("%w: domain %d holds %d commitments, batch %d would exceed the cap",
				ErrMalformed, c.DomainID, len(st.commitments), c.BatchID)
		}
		if st.commitments == nil {
			st.commitments = make(map[uint64]codec.BatchCommitment)
		}
		st.commitments[c.BatchID] = c
		return true, nil
	}
	if reflect.DeepEqual(held, c) {
		return false, nil
	}
	err = fmt.Errorf("%w: domain %d batch %d differs from the held commitment and was not retracted", ErrInconsistent, c.DomainID, c.BatchID)
	if st.fault == "" {
		st.fault = err.Error()
	}
	return false, err
}

// checkCommitment applies the field checks every Committed and Recovered frame must pass.
func checkCommitment(c codec.BatchCommitment, w WorkerConfig, executor types.Address) error {
	if c.DomainID != w.DomainID {
		return fmt.Errorf("%w: commitment domainId %d, want %d", ErrMalformed, c.DomainID, w.DomainID)
	}
	if c.ExecutorID != executor {
		return fmt.Errorf("%w: commitment executorId %s, want %s", ErrMalformed, c.ExecutorID, executor)
	}
	if c.SubmittedAtHeight != 0 {
		return fmt.Errorf("%w: commitment submittedAtHeight %d, want 0", ErrMalformed, c.SubmittedAtHeight)
	}
	return nil
}

// sourceError maps a source-call error to the SourceError the worker sees: errors.Is(err,
// recovery.ErrHalted) becomes kind halted, anything else kind unavailable. The message is cut to
// ipc.MaxSourceErrorLen bytes, which may split a UTF-8 sequence.
func sourceError(err error) ipc.SourceError {
	kind := ipc.SourceUnavailable
	if errors.Is(err, recovery.ErrHalted) {
		kind = ipc.SourceHalted
	}
	return ipc.SourceError{Kind: kind, Message: cut(err.Error())}
}

func cut(s string) string {
	if len(s) <= ipc.MaxSourceErrorLen {
		return s
	}
	return s[:ipc.MaxSourceErrorLen]
}
