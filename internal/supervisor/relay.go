package supervisor

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/zenon-network/go-zenon/common/types"
	"github.com/zenon-network/go-zenon/vm/constants"
	"github.com/zenon-network/go-zenon/vm/embedded/definition"

	"github.com/Zeno-L2/zeno-executor/internal/codec"
	"github.com/Zeno-L2/zeno-executor/internal/recovery"
	"github.com/Zeno-L2/zeno-executor/internal/settlement"
	"github.com/Zeno-L2/zeno-executor/internal/znnd"
)

// relayScanMax bounds the outbox ids one Relay read checks.
const relayScanMax = 64

// relayBatch is the stored batch a domain's relays walk, loaded once
// per batch.
type relayBatch struct {
	id   uint64
	root types.Hash         // the file's OutboxRoot
	ws   []StoredWithdrawal // the relayable withdrawals, in stored order
	ids  []types.Hash       // ws[i]'s outboxId
	next int                // ws[:next] are known to be processed
}

// relayRetry is a domain's backoff after a relay of one withdrawal
// failed, or after its release failed to build (id is zero).
type relayRetry struct {
	id         types.Hash // the withdrawal's outboxId
	delay      time.Duration
	until      time.Time
	rejections int // consecutive rejected outcomes of id
}

// withdrawalPass visits every slot once, from u.wnext, and sends the
// relay or release of the first domain that is ready.
func (u *submitter) withdrawalPass(ctx context.Context) {
	n := len(u.s.slots)
	start := u.wnext
	for k := 0; k < n; k++ {
		if ctx.Err() != nil {
			return
		}
		if u.visitWithdrawals(ctx, (start+k)%n) {
			return
		}
	}
}

// visitWithdrawals sends slot i's next relay, or its release, when the
// checks pass. It reports whether the pass ends.
func (u *submitter) visitWithdrawals(ctx context.Context, i int) bool {
	s := u.s
	sl := s.slots[i]
	st := sl.st
	d := sl.w.DomainID

	s.mu.Lock()
	state := sl.state
	s.mu.Unlock()
	if state == StateHalted || state == StateConfigError {
		return false
	}
	st.mu.Lock()
	faulted := st.fault != ""
	st.mu.Unlock()
	if faulted {
		return false
	}

	rb := u.relayBatchFor(st, d)
	var q *settlement.RelayQuery
	if rb != nil {
		q = &settlement.RelayQuery{BatchID: rb.id, IDs: rb.ids[rb.next:min(rb.next+relayScanMax, len(rb.ids))]}
	}
	rs, err := s.src.Anchors.Relay(d, q)
	if err != nil {
		if errors.Is(err, recovery.ErrHalted) {
			u.halt(st, d, fmt.Errorf("%w: domain %d: Settlement read for relay: %w", ErrInconsistent, d, err))
		} else {
			u.outage(err)
		}
		return false
	}
	if rs.Paused {
		u.warnf(fmt.Sprintf("relay-pause-%d", d), "supervisor: WARN: submitter: domain %d relays and releases not sent (Settlement at height %d): Settlement's relay pause is set; checking again", d, rs.Height)
		return false
	}

	releaseReady := rs.QueueLen > 0
	if rr := u.releaseRetry[d]; rr != nil && u.now().Before(rr.until) {
		releaseReady = false
	}
	// idle sends the release when it is ready, else lets the pass move on.
	idle := func() bool {
		if !releaseReady {
			return false
		}
		data, err := definition.ABISettlement.PackMethod(definition.SettlementReleaseWithdrawalsMethodName, d)
		if err != nil {
			u.halt(st, d, fmt.Errorf("%w: domain %d cannot be encoded for release: %w", ErrInconsistent, d, err))
			return false
		}
		return u.sendWithdrawal(ctx, i, SendRecord{DomainID: d, Method: definition.SettlementReleaseWithdrawalsMethodName}, data)
	}

	// A release pays out at most constants.MaxWithdrawalsPerUpdate
	// withdrawals, so a queue that long goes first.
	if releaseReady && rs.QueueLen >= constants.MaxWithdrawalsPerUpdate {
		return idle()
	}
	if rb == nil || rs.Entry == nil {
		return idle()
	}
	if rs.Entry.OutboxRoot != rb.root {
		u.halt(st, d, fmt.Errorf("%w: domain %d batch %d: the stored withdrawals' outbox root %s differs from Settlement's accepted %s", ErrInconsistent, d, rb.id, rb.root, rs.Entry.OutboxRoot))
		return false
	}
	if !rs.Entry.IsFinalized(rs.Height) {
		return idle()
	}

	rb.next += rs.Processed
	if rb.next == len(rb.ids) {
		if err := st.withdrawals.Remove(rb.id); err != nil {
			u.warnf(fmt.Sprintf("relay-remove-%d", d), "supervisor: domain %d WARN: withdrawal store: removing batch %d: %v; retrying", d, rb.id, err)
		} else {
			log.Printf("supervisor: domain %d batch %d: every withdrawal is registered or not relayable; its withdrawals file is removed", d, rb.id)
			delete(u.relays, d)
			u.dropRelayRetry(st, d)
		}
		return idle()
	}
	if rs.Processed == len(q.IDs) {
		// The next pass reads the next window.
		return idle()
	}

	id := rb.ids[rb.next]
	if rr := u.relayRetry[d]; rr != nil {
		if rr.id != id {
			u.dropRelayRetry(st, d)
		} else if u.now().Before(rr.until) {
			return idle()
		}
	}
	w := rb.ws[rb.next]
	data, err := definition.ABISettlement.PackMethod(definition.SettlementRelayMessageMethodName, d, rb.id, w.Message, w.Proof)
	if err != nil {
		u.halt(st, d, fmt.Errorf("%w: domain %d batch %d withdrawal %s cannot be encoded for relay: %w", ErrInconsistent, d, rb.id, id, err))
		return false
	}
	return u.sendWithdrawal(ctx, i, SendRecord{DomainID: d, Method: definition.SettlementRelayMessageMethodName, BatchID: rb.id, OutboxID: id}, data)
}

// relayBatchFor returns the cached batch of domain d's lowest verified
// stored batch below the observed nextBatchId, loading it when the
// cache holds another. It returns nil when there is none or the store
// cannot be read.
func (u *submitter) relayBatchFor(st *workerState, d uint32) *relayBatch {
	st.mu.Lock()
	mark := st.chainNext
	st.mu.Unlock()

	id, ok := st.withdrawals.lowestVerified(mark)
	if !ok {
		delete(u.relays, d)
		return nil
	}
	if rb := u.relays[d]; rb != nil && rb.id == id {
		return rb
	}

	fail := func(err error) *relayBatch {
		u.warnf(fmt.Sprintf("relay-store-%d", d), "supervisor: domain %d WARN: withdrawal store: %v; relays for the domain wait", d, err)
		delete(u.relays, d)
		return nil
	}
	b, found, err := st.withdrawals.Batch(id)
	if err != nil {
		return fail(err)
	}
	if !found {
		delete(u.relays, d)
		return nil
	}
	rb := &relayBatch{id: id, root: b.OutboxRoot}
	for _, w := range b.Withdrawals {
		m, err := codec.DecodeOutboxMessage(w.Message)
		if err != nil {
			return fail(err)
		}
		l, ok := m.Target.(codec.L1Withdrawal)
		if !ok {
			return fail(fmt.Errorf("batch %d withdrawal (%d, %d) is not a kind 1 outbox message", id, m.InputIndex, m.OutboxIndex))
		}
		// Settlement's send validation rejects both, so no relay by anyone
		// can register them.
		switch {
		case l.Amount == (codec.U256{}):
			log.Printf("supervisor: domain %d batch %d: withdrawal (%d, %d) is not relayed: its amount is zero", d, id, m.InputIndex, m.OutboxIndex)
			continue
		case types.IsEmbeddedAddress(l.RecipientL1):
			log.Printf("supervisor: domain %d batch %d: withdrawal (%d, %d) is not relayed: its recipient %s is an embedded contract", d, id, m.InputIndex, m.OutboxIndex, l.RecipientL1)
			continue
		}
		rb.ws = append(rb.ws, w)
		rb.ids = append(rb.ids, codec.OutboxID(d, id, m.InputIndex, m.OutboxIndex))
	}
	u.relays[d] = rb
	return rb
}

// sendWithdrawal builds, records and publishes the relay or release r
// carries, as send does for a batch. It reports whether the pass ends.
func (u *submitter) sendWithdrawal(ctx context.Context, i int, r SendRecord, data []byte) bool {
	s := u.s
	sl := s.slots[i]
	st := sl.st
	d := sl.w.DomainID
	release := r.Method == definition.SettlementReleaseWithdrawalsMethodName

	b, err := buildSend(s.src.Node, s.key, data)
	if err != nil {
		if errors.Is(err, errInsufficientPlasma) {
			u.mu.Lock()
			u.plasmaText = err.Error()
			u.mu.Unlock()
			u.warnf("plasma", "supervisor: WARN: submitter: domain %d %s: %s; retrying", d, sendLabel(r), err.Error())
		} else {
			u.outage(err)
		}
		// The send waits out its backoff and the pass moves on, so a
		// build that keeps failing cannot starve the other domains.
		if release {
			u.withdrawalBackoff(u.releaseRetry, d, types.Hash{})
		} else {
			u.withdrawalBackoff(u.relayRetry, d, r.OutboxID)
		}
		u.wnext = (i + 1) % len(s.slots)
		return false
	}
	u.mu.Lock()
	u.plasmaText = ""
	u.mu.Unlock()
	if release {
		delete(u.releaseRetry, d)
	}

	if ctx.Err() != nil {
		return true
	}
	s.mu.Lock()
	state := sl.state
	s.mu.Unlock()
	st.mu.Lock()
	halted := state == StateHalted || state == StateConfigError || st.fault != ""
	st.mu.Unlock()
	if halted {
		// The domain halted while the send was built; a halted domain
		// publishes nothing.
		return false
	}

	r.Height = b.Height
	r.Hash = b.Hash
	r.DataHash = types.NewHash(data)
	if err := u.log.Sent(r); err != nil {
		u.logFailed(err)
		return true
	}

	if err := s.src.Node.PublishRawTransaction(b); err != nil {
		// The failed publish may still have reached the pool, so the send
		// is followed anyway.
		log.Printf("supervisor: domain %d %s: publishing %s failed: %q; following it", d, sendLabel(r), b.Hash, u.redactErr(err))
	} else {
		log.Printf("supervisor: domain %d %s: sent %s at account height %d", d, sendLabel(r), b.Hash, b.Height)
	}

	u.pending = &pendingSend{slot: i, r: r}
	u.wnext = (i + 1) % len(s.slots)
	return true
}

// withdrawalBackoff doubles the delay of m[d] when it names id, or
// starts it at the restart backoff's minimum, and returns it with its
// wait restarted.
func (u *submitter) withdrawalBackoff(m map[uint32]*relayRetry, d uint32, id types.Hash) *relayRetry {
	rr := m[d]
	if rr != nil && rr.id == id {
		rr.delay = double(rr.delay, u.s.policy)
	} else {
		rr = &relayRetry{id: id, delay: u.s.policy.BackoffMin}
		m[d] = rr
	}
	rr.until = u.now().Add(rr.delay)
	return rr
}

// dropRelayRetry forgets domain d's relay backoff and rejection count,
// and clears its relay status. It leaves the release backoff alone.
func (u *submitter) dropRelayRetry(st *workerState, d uint32) {
	delete(u.relayRetry, d)
	st.mu.Lock()
	st.relayError = ""
	st.mu.Unlock()
}

// finishRelay closes the pending relay with outcome o. other names the
// block that superseded it.
func (u *submitter) finishRelay(p *pendingSend, st *workerState, o Outcome, other types.Hash) {
	r := p.r
	d := r.DomainID
	r.Outcome = o
	if err := u.log.Resolved(r); err != nil {
		u.logFailed(err)
	}
	u.release(p)

	label := sendLabel(r)
	switch o {
	case OutcomeAccepted:
		u.dropRelayRetry(st, d)
		log.Printf("supervisor: domain %d %s registered (send %s)", d, label, r.Hash)

	case OutcomeRejected:
		rr := u.withdrawalBackoff(u.relayRetry, d, r.OutboxID)
		rr.rejections++
		log.Printf("supervisor: domain %d %s rejected by Settlement (send %s); retrying in %s", d, label, r.Hash, rr.delay)
		if rr.rejections >= rejectAlert {
			text := fmt.Sprintf("Settlement rejected the relay of %s %d consecutive times (latest send %s); retrying without limit, and each attempt costs plasma", label, rr.rejections, r.Hash)
			st.mu.Lock()
			st.relayError = text
			st.mu.Unlock()
			if rr.rejections == rejectAlert {
				log.Printf("supervisor: domain %d ERROR: %s", d, text)
			} else {
				log.Printf("supervisor: domain %d WARN: %s", d, text)
			}
		}

	case OutcomeDropped:
		rr := u.withdrawalBackoff(u.relayRetry, d, r.OutboxID)
		log.Printf("supervisor: domain %d %s: send %s at account height %d is unknown to the node; sending again in %s", d, label, r.Hash, r.Height, rr.delay)

	case OutcomeSuperseded:
		rr := u.withdrawalBackoff(u.relayRetry, d, r.OutboxID)
		log.Printf("supervisor: domain %d %s: send %s superseded (%s); sending again in %s", d, label, r.Hash, other, rr.delay)
	}
}

// finishRelease closes the pending release with outcome o. other names
// the block that superseded it. A release that was received has no
// backoff: the next withdrawal pass reads the queue again. A release
// that was dropped or superseded waits before it is sent again.
func (u *submitter) finishRelease(p *pendingSend, o Outcome, other types.Hash) {
	r := p.r
	r.Outcome = o
	if err := u.log.Resolved(r); err != nil {
		u.logFailed(err)
	}
	u.release(p)

	var by string
	if other != (types.Hash{}) {
		by = fmt.Sprintf(" (superseded by %s)", other)
	}
	log.Printf("supervisor: domain %d release: send %s finished %s%s", r.DomainID, r.Hash, o, by)
	if o == OutcomeDropped || o == OutcomeSuperseded {
		rr := u.withdrawalBackoff(u.releaseRetry, r.DomainID, types.Hash{})
		log.Printf("supervisor: domain %d release: sending again in %s", r.DomainID, rr.delay)
	}
}

// relayOutcome decides a relay whose receive is in a momentum, from the
// processed flag of its outbox id. Settlement sets the flag only on a
// successful registration and never clears it, so a set flag means the
// id is registered, by this send or by anyone else.
func relayOutcome(anchors AnchorSource, r SendRecord, ss znnd.SendStatus) (sendCheck, error) {
	rs, err := anchors.Relay(r.DomainID, &settlement.RelayQuery{BatchID: r.BatchID, IDs: []types.Hash{r.OutboxID}})
	if err != nil {
		if errors.Is(err, recovery.ErrHalted) {
			return sendCheck{Fault: fmt.Errorf("%w: domain %d: Settlement read for %s's outcome: %w", ErrInconsistent, r.DomainID, sendLabel(r), err)}, nil
		}
		return sendCheck{}, err
	}
	if rs.Height < ss.ReceiveHeight {
		return sendCheck{}, nil
	}
	if rs.Processed == 1 {
		return sendCheck{Outcome: OutcomeAccepted}, nil
	}
	return sendCheck{Outcome: OutcomeRejected}, nil
}

// releaseOutcome decides a release whose receive is in a momentum. A
// received release is done whatever Settlement returned.
func releaseOutcome(anchors AnchorSource, r SendRecord, ss znnd.SendStatus) (sendCheck, error) {
	rs, err := anchors.Relay(r.DomainID, nil)
	if err != nil {
		if errors.Is(err, recovery.ErrHalted) {
			return sendCheck{Fault: fmt.Errorf("%w: domain %d: Settlement read for %s's outcome: %w", ErrInconsistent, r.DomainID, sendLabel(r), err)}, nil
		}
		return sendCheck{}, err
	}
	if rs.Height < ss.ReceiveHeight {
		return sendCheck{}, nil
	}
	return sendCheck{Outcome: OutcomeReceived}, nil
}

// sendLabel names what r sends, for log lines and status texts.
func sendLabel(r SendRecord) string {
	switch r.Method {
	case definition.SettlementRelayMessageMethodName:
		return fmt.Sprintf("batch %d withdrawal %s", r.BatchID, r.OutboxID)
	case definition.SettlementReleaseWithdrawalsMethodName:
		return "release"
	}
	return fmt.Sprintf("batch %d", r.BatchID)
}
