package supervisor

import (
	"bytes"
	"errors"
	"fmt"
	"log"
	"math/big"
	"sort"
	"time"

	"github.com/zenon-network/go-zenon/common/types"
	"github.com/zenon-network/go-zenon/vm/embedded/definition"

	"github.com/digitalSloth/go-executor/internal/codec"
	"github.com/digitalSloth/go-executor/internal/ipc"
	"github.com/digitalSloth/go-executor/internal/recovery"
)

type handOffMode int

const (
	verifyHandOff     handOffMode = iota // verify against ref, store verified
	compareHandOff                       // compare with the stored hand-off
	unverifiedHandOff                    // Settlement unreadable: store unverified
)

const (
	// unverifiedRetryInterval is the wait between two retries of a
	// domain's unverified files while Settlement answers.
	unverifiedRetryInterval = time.Second
	// unverifiedRetryMax caps the wait after repeated failures.
	unverifiedRetryMax = 60 * time.Second
	// unverifiedReadTimeout bounds how long one retry's Settlement read
	// may delay a reply to the worker.
	unverifiedReadTimeout = 2 * time.Second
)

// verifyRetry is one domain's retry schedule for unverified files.
// serve reads and writes it under st.mu.
type verifyRetry struct {
	base    time.Duration      // wait after a success; set by New
	max     time.Duration      // cap on the wait after failures; set by New
	timeout time.Duration      // bound on one Settlement read; 0 waits without a bound; set by New
	delay   time.Duration      // current wait; base after a success, doubled after a failure
	after   time.Time          // no retry before this
	failing bool               // the current run of failures is already logged
	pending <-chan entryResult // a timed-out read still running; nil when none
}

// entryResult is one finished AcceptedEntry read.
type entryResult struct {
	e   *definition.BatchEntry
	err error
}

// reference is what a batch's withdrawals are verified against.
type reference struct {
	root    types.Hash
	first   uint64 // lowest inputIndex allowed
	last    uint64 // highest inputIndex allowed
	summary codec.AssetFlowSummary
}

// handOff is one batch's open hand-off, from its Committed or Recovered
// to its WithdrawalsEnd.
type handOff struct {
	c              codec.BatchCommitment
	committed      bool // opened by a Committed; false for a Recovered
	mode           handOffMode
	ref            reference          // verify mode
	stored         []StoredWithdrawal // compare mode
	storedVerified bool               // compare mode: the stored file is verified
	got            []StoredWithdrawal // received withdrawals, in order
	prevInput      uint64             // the previous withdrawal's inputIndex, valid when got is non-empty
	prevOutbox     uint32             // the previous withdrawal's outboxIndex, valid when got is non-empty
}

// storeError wraps err as a withdrawal store failure.
func storeError(what string, err error) error {
	return fmt.Errorf("%w: %s: %w", ErrWithdrawalStore, what, err)
}

// openHandOff checks c, decides how its withdrawals are checked and
// returns the open hand-off. A batch with an unverified file is
// compared with it whatever the observed nextBatchId. A batch at or
// above the observed nextBatchId is verified against its own
// commitment. A batch below it with a verified file is compared with
// the file. A batch below it with no file is verified against
// Settlement's accepted entry, or stored unverified when Settlement
// cannot be read.
func openHandOff(c codec.BatchCommitment, committed bool, w WorkerConfig, executor types.Address, src Sources, st *workerState) (*handOff, error) {
	if err := checkCommitment(c, w, executor); err != nil {
		return nil, err
	}
	if err := checkTip(c, src, st); err != nil {
		return nil, err
	}
	h := &handOff{c: c, committed: committed}

	verified, ok := st.withdrawals.Lookup(c.BatchID)
	if ok && !verified {
		ho, found, err := h.compareWith(st)
		if err == nil && !found {
			err = storeError(fmt.Sprintf("batch %d", c.BatchID), errors.New("file is missing"))
		}
		return ho, err
	}

	st.mu.Lock()
	next := st.chainNext
	if c.BatchID >= next {
		err := checkHold(c, st)
		st.mu.Unlock()
		if err != nil {
			return nil, err
		}
		h.mode = verifyHandOff
		h.ref = reference{root: c.OutboxRoot, first: c.FirstInputSeq, last: c.LastInputSeq, summary: c.AssetFlowSummary}
		return h, nil
	}
	st.mu.Unlock()

	if ok {
		ho, found, err := h.compareWith(st)
		if err != nil {
			return nil, err
		}
		if found {
			return ho, nil
		}
		// The relayer can remove a verified file between the lookup and
		// the load; the batch is then checked against Settlement.
	}

	e, err := acceptedEntry(c.DomainID, c.BatchID, src, st)
	if err != nil {
		if errors.Is(err, recovery.ErrHalted) {
			return nil, fault(st, fmt.Errorf("%w: domain %d batch %d has no readable accepted entry: %w", ErrInconsistent, c.DomainID, c.BatchID, err))
		}
		log.Printf("supervisor: domain %d WARN: batch %d withdrawals stored unverified: Settlement read: %s; the supervisor checks them once Settlement answers", c.DomainID, c.BatchID, redactEndpointErr(st.rawURL, err))
		h.mode = unverifiedHandOff
		return h, nil
	}
	ref, err := referenceOf(e)
	if err != nil {
		return nil, fault(st, fmt.Errorf("%w: domain %d batch %d: Settlement's accepted entry cannot be used: %w", ErrInconsistent, c.DomainID, c.BatchID, err))
	}
	h.mode = verifyHandOff
	h.ref = ref
	return h, nil
}

// compareWith loads the batch's stored file into a compare hand-off.
// found is false, with a nil error, when the store has no file.
func (h *handOff) compareWith(st *workerState) (ho *handOff, found bool, err error) {
	b, ok, err := st.withdrawals.Batch(h.c.BatchID)
	if err != nil {
		return nil, false, storeError(fmt.Sprintf("batch %d", h.c.BatchID), err)
	}
	if !ok {
		return nil, false, nil
	}
	h.mode = compareHandOff
	h.stored = b.Withdrawals
	h.storedVerified = b.Verified
	return h, true, nil
}

// referenceOf builds the reference from Settlement's accepted entry.
func referenceOf(e *definition.BatchEntry) (reference, error) {
	s, err := codec.DecodeAssetFlowSummary(e.AssetFlowSummary)
	if err != nil {
		return reference{}, err
	}
	return reference{root: e.OutboxRoot, first: e.FirstInputSeq, last: e.LastInputSeq, summary: s}, nil
}

// add appends one received withdrawal. The frame's placement and the
// hand-off's size are already checked by frameOrder; the content checks
// run in close.
func (h *handOff) add(v ipc.Withdrawal, st *workerState) error {
	m, err := codec.DecodeOutboxMessage(v.Message)
	if err != nil {
		return fmt.Errorf("%w: batch %d withdrawal: %w", ErrMalformed, v.BatchID, err)
	}
	if _, ok := m.Target.(codec.L1Withdrawal); !ok {
		return fmt.Errorf("%w: batch %d withdrawal is not a kind 1 outbox message", ErrMalformed, v.BatchID)
	}
	if len(h.got) > 0 && !after(m.InputIndex, m.OutboxIndex, h.prevInput, h.prevOutbox) {
		return fault(st, fmt.Errorf("%w: domain %d batch %d withdrawal (%d, %d) is not after (%d, %d)",
			ErrInconsistent, h.c.DomainID, h.c.BatchID, m.InputIndex, m.OutboxIndex, h.prevInput, h.prevOutbox))
	}
	h.got = append(h.got, StoredWithdrawal{Message: v.Message, Proof: v.Proof})
	h.prevInput, h.prevOutbox = m.InputIndex, m.OutboxIndex
	return nil
}

// close finishes the hand-off: it checks and stores the received
// withdrawals as the mode says and, once they are stored and verified,
// records the batch.
func (h *handOff) close(st *workerState) error {
	c := h.c
	switch h.mode {
	case verifyHandOff:
		if err := verifyBatch(h.ref, h.got); err != nil {
			return fault(st, fmt.Errorf("%w: domain %d batch %d withdrawals fail verification: %w", ErrInconsistent, c.DomainID, c.BatchID, err))
		}
		b := StoredBatch{DomainID: c.DomainID, BatchID: c.BatchID, Verified: true, OutboxRoot: h.ref.root, Withdrawals: h.got}
		if err := st.withdrawals.Put(b); err != nil {
			return storeError(fmt.Sprintf("write batch %d", c.BatchID), err)
		}
	case compareHandOff:
		if len(h.got) != len(h.stored) {
			return fault(st, fmt.Errorf("%w: domain %d batch %d differs from the stored hand-off: %d withdrawals, stored %d",
				ErrInconsistent, c.DomainID, c.BatchID, len(h.got), len(h.stored)))
		}
		for i, g := range h.got {
			s := h.stored[i]
			if !bytes.Equal(g.Message, s.Message) || !bytes.Equal(g.Proof, s.Proof) {
				return fault(st, fmt.Errorf("%w: domain %d batch %d differs from the stored hand-off at withdrawal %d",
					ErrInconsistent, c.DomainID, c.BatchID, i))
			}
		}
	case unverifiedHandOff:
		b := StoredBatch{DomainID: c.DomainID, BatchID: c.BatchID, Withdrawals: h.got}
		if err := st.withdrawals.Put(b); err != nil {
			return storeError(fmt.Sprintf("write batch %d", c.BatchID), err)
		}
	}

	if h.mode == unverifiedHandOff || h.mode == compareHandOff && !h.storedVerified {
		return nil
	}
	if h.committed {
		return recordCommitted(c, st)
	}
	return recordRecovered(c, st)
}

// verifyBatch checks each withdrawal's position and proof against ref,
// then the totals.
func verifyBatch(ref reference, ws []StoredWithdrawal) error {
	sums := make(map[codec.AssetID]*big.Int)
	for i, w := range ws {
		m, err := codec.DecodeOutboxMessage(w.Message)
		if err != nil {
			return fmt.Errorf("withdrawal %d: %w", i, err)
		}
		l1, ok := m.Target.(codec.L1Withdrawal)
		if !ok {
			return fmt.Errorf("withdrawal %d is not a kind 1 outbox message", i)
		}
		if m.InputIndex < ref.first || m.InputIndex > ref.last {
			return fmt.Errorf("withdrawal %d has inputIndex %d, outside the batch's range [%d, %d]", i, m.InputIndex, ref.first, ref.last)
		}
		if err := codec.VerifyMerkleProof(ref.root, w.Message, w.Proof); err != nil {
			return fmt.Errorf("withdrawal %d (%d, %d) proof: %w", i, m.InputIndex, m.OutboxIndex, err)
		}
		sum, ok := sums[l1.Asset]
		if !ok {
			sum = new(big.Int)
			sums[l1.Asset] = sum
		}
		sum.Add(sum, new(big.Int).SetBytes(l1.Amount[:]))
	}
	return checkTotals(sums, ref.summary)
}

// checkTotals compares the received per-asset sums with summary's
// WithdrawalDebit. A summary asset with no withdrawal must have a zero
// debit, and an asset outside the summary must sum to zero.
func checkTotals(sums map[codec.AssetID]*big.Int, summary codec.AssetFlowSummary) error {
	seen := make(map[codec.AssetID]bool, len(summary))
	for _, e := range summary {
		got := sums[e.Asset]
		if got == nil {
			got = new(big.Int)
		}
		want := new(big.Int).SetBytes(e.WithdrawalDebit[:])
		if got.Cmp(want) != 0 {
			return fmt.Errorf("asset %x: withdrawals sum to %s, the batch's withdrawalDebit is %s", e.Asset[:], got, want)
		}
		seen[e.Asset] = true
	}
	extra := make([]codec.AssetID, 0, len(sums))
	for a, sum := range sums {
		if !seen[a] && sum.Sign() != 0 {
			extra = append(extra, a)
		}
	}
	if len(extra) == 0 {
		return nil
	}
	sort.Slice(extra, func(i, j int) bool { return bytes.Compare(extra[i][:], extra[j][:]) < 0 })
	return fmt.Errorf("asset %x: withdrawals sum to %s, the batch has no withdrawalDebit for it", extra[0][:], sums[extra[0]])
}

// retryUnverified checks the domain's lowest unverified stored batch
// against Settlement's accepted entry when the schedule says a retry is
// due. A mismatch or an entry that cannot be read at all is a
// consistency fault. Any other failure is recorded on the schedule and
// the batch stays unverified. serve calls it before a request frame's
// reply. The submitter goroutine also reads and removes verified
// batches; the store's own lock serialises the calls. The caller holds
// no lock.
func retryUnverified(w WorkerConfig, src Sources, st *workerState) error {
	st.mu.Lock()
	due := !time.Now().Before(st.verify.after)
	st.mu.Unlock()
	if !due {
		return nil
	}

	id, found := st.withdrawals.lowestUnverified()
	if !found {
		return nil
	}

	// A read that timed out earlier may have finished since: its entry
	// is used in place of a new read, so a node that always answers
	// slowly still verifies the batch.
	var done *entryResult
	st.mu.Lock()
	if st.verify.pending != nil {
		select {
		case r := <-st.verify.pending:
			st.verify.pending = nil
			done = &r
		default:
			st.mu.Unlock()
			recordRetryFailure(w.DomainID, id, errors.New("Settlement read still running"), st)
			return nil
		}
	}
	st.mu.Unlock()

	var e *definition.BatchEntry
	if done != nil && done.err == nil && done.e != nil && done.e.BatchId == id {
		e = done.e
	} else {
		var err error
		e, err = readEntry(w, id, src, st)
		if err != nil {
			if errors.Is(err, recovery.ErrHalted) {
				return fault(st, fmt.Errorf("%w: domain %d batch %d has no readable accepted entry: %w", ErrInconsistent, w.DomainID, id, err))
			}
			recordRetryFailure(w.DomainID, id, err, st)
			return nil
		}
	}

	b, ok, err := st.withdrawals.Batch(id)
	if err != nil {
		recordRetryFailure(w.DomainID, id, err, st)
		return nil
	}
	if !ok {
		return nil
	}
	ref, err := referenceOf(e)
	if err == nil {
		err = verifyBatch(ref, b.Withdrawals)
	}
	if err != nil {
		return fault(st, fmt.Errorf("%w: domain %d batch %d stored withdrawals differ from Settlement's accepted entry: %w", ErrInconsistent, w.DomainID, id, err))
	}
	b.Verified, b.OutboxRoot = true, e.OutboxRoot
	if err := st.withdrawals.Put(b); err != nil {
		recordRetryFailure(w.DomainID, id, err, st)
		return nil
	}

	st.mu.Lock()
	st.verify.delay = st.verify.base
	st.verify.after = time.Now().Add(st.verify.base)
	st.verify.failing = false
	st.mu.Unlock()
	return nil
}

// recordRetryFailure doubles the retry wait up to the cap, pushes the
// next retry out by it and logs the first failure of a run.
func recordRetryFailure(domainID uint32, batchID uint64, err error, st *workerState) {
	st.mu.Lock()
	v := &st.verify
	v.delay = min(2*v.delay, v.max)
	v.after = time.Now().Add(v.delay)
	first := !v.failing
	v.failing = true
	delay := v.delay
	st.mu.Unlock()
	if first {
		log.Printf("supervisor: domain %d WARN: batch %d stored withdrawals still unverified: %s; retrying in %s", domainID, batchID, redactEndpointErr(st.rawURL, err), delay)
	}
}

// readEntry reads batchID's accepted entry, waiting at most
// st.verify.timeout. A read that times out keeps running and its
// channel is kept in st.verify.pending.
func readEntry(w WorkerConfig, batchID uint64, src Sources, st *workerState) (*definition.BatchEntry, error) {
	st.mu.Lock()
	if e := st.accepted; e != nil && e.BatchId == batchID {
		st.mu.Unlock()
		return e, nil
	}
	timeout := st.verify.timeout
	st.mu.Unlock()

	ch := make(chan entryResult, 1)
	go func() {
		e, err := src.Anchors.AcceptedEntry(w.DomainID, batchID)
		ch <- entryResult{e: e, err: err}
	}()

	var expired <-chan time.Time
	if timeout > 0 {
		t := time.NewTimer(timeout)
		defer t.Stop()
		expired = t.C
	}
	select {
	case r := <-ch:
		return r.e, r.err
	case <-expired:
		st.mu.Lock()
		st.verify.pending = ch
		st.mu.Unlock()
		return nil, fmt.Errorf("Settlement read timed out after %s", timeout)
	}
}
