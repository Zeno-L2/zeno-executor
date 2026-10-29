package supervisor

import (
	"context"
	"errors"
	"fmt"
	"log"
	"reflect"
	"sync"
	"time"

	"github.com/zenon-network/go-zenon/chain/nom"
	"github.com/zenon-network/go-zenon/common/types"
	"github.com/zenon-network/go-zenon/vm/embedded/definition"

	"github.com/Zeno-L2/zeno-executor/internal/codec"
	"github.com/Zeno-L2/zeno-executor/internal/recovery"
	"github.com/Zeno-L2/zeno-executor/internal/settlement"
	"github.com/Zeno-L2/zeno-executor/internal/znnd"
)

// submitPoll is the wait between two submitter steps.
const submitPoll = time.Second

// alertDepths is the factor of confirmationDepth after which an unconfirmed send, or a domain
// whose Settlement cursor stays below the supervisor's mark, needs the operator.
const alertDepths = 10

// rejectAlert is the number of consecutive rejections of one batch after which the domain's
// status shows an error.
const rejectAlert = 5

// SendNode is the node the submitter builds, publishes and follows sends through. *znnd.Client
// implements it.
type SendNode interface {
	blockNode
	PublishRawTransaction(b *nom.AccountBlock) error
	SendStatus(hash types.Hash) (znnd.SendStatus, error)
	AccountBlockAtHeight(addr types.Address, height uint64) (*nom.AccountBlock, error)
}

var _ SendNode = (*znnd.Client)(nil)

// Outcome is how a send ended.
type Outcome string

const (
	OutcomeAccepted   Outcome = "accepted"
	OutcomeRejected   Outcome = "rejected"
	OutcomeDropped    Outcome = "dropped"
	OutcomeSuperseded Outcome = "superseded"
	OutcomeReceived   Outcome = "received"                 // a ReleaseWithdrawals send Settlement received, whatever its result
	OutcomeAbandoned  Outcome = "abandoned: domain halted" // the domain halted before the outcome was known
)

// SendRecord is one send from the executor address.
type SendRecord struct {
	DomainID uint32     `json:"domainId"`
	Method   string     `json:"method"`            // definition.SettlementSubmitBatchMethodName, SettlementRelayMessageMethodName or SettlementReleaseWithdrawalsMethodName
	BatchID  uint64     `json:"batchId"`           // the batch a SubmitBatch or RelayMessage send names; 0 for ReleaseWithdrawals
	Height   uint64     `json:"accountHeight"`     // the block's height on the executor address's account chain
	Hash     types.Hash `json:"hash"`              // names the block built or adopted; not authoritative for which copy Settlement accepted
	DataHash types.Hash `json:"dataHash"`          // types.NewHash (SHA3-256) of the block's Data
	OutboxID types.Hash `json:"outboxId"`          // the withdrawal a RelayMessage send registers; zero otherwise
	Outcome  Outcome    `json:"outcome,omitempty"` // empty while unresolved
}

// SubmitterStatus is the submitter's state for an operator.
type SubmitterStatus struct {
	Last  *SendRecord // the latest send, OutcomeAbandoned included; nil before the first
	Error string      // why submission needs the operator now; empty when nothing does
}

// sendLog records each send before it is published, and its outcome once known.
type sendLog interface {
	Sent(r SendRecord) error
	Resolved(r SendRecord) error // r.Outcome is set (OutcomeAbandoned included); r.Hash names the send
	Last() (SendRecord, bool)
}

// memLog is a sendLog that keeps only the latest send in memory. It never returns an error.
type memLog struct {
	mu   sync.Mutex
	last SendRecord
	ok   bool
}

func (l *memLog) Sent(r SendRecord) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.last, l.ok = r, true
	return nil
}

// Resolved replaces the latest record only when its hash is r's.
func (l *memLog) Resolved(r SendRecord) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.ok && l.last.Hash == r.Hash {
		l.last = r
	}
	return nil
}

func (l *memLog) Last() (SendRecord, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.last, l.ok
}

// unresolvedSend is the submitter's pending send for a domain, as workerState holds it.
type unresolvedSend struct {
	batchID   uint64
	retracted bool // a Retract covered batchID after the send was marked
}

// sendCheck is what one check of a send found.
type sendCheck struct {
	Outcome     Outcome    // empty while the send is pending
	Other       types.Hash // the block at the send's height; set with OutcomeSuperseded
	Unconfirmed bool       // the send or its Settlement receive is not yet in a momentum
	Age         uint64     // momenta from the send's acknowledged momentum to the node's frontier; set with Unconfirmed
	Fault       error      // wraps ErrInconsistent: a contradiction for r.DomainID
}

// resolveSend checks the send r names, from r and reads of the node and Settlement alone. An
// error is an outage or an inconsistent read, and the send stays pending.
func resolveSend(node SendNode, anchors AnchorSource, executor types.Address, r SendRecord) (sendCheck, error) {
	ss, err := node.SendStatus(r.Hash)
	if err != nil {
		return sendCheck{}, err
	}

	var blk *nom.AccountBlock
	unconfirmed := false
	switch {
	case ss.Block != nil && !ss.Received:
		blk, unconfirmed = ss.Block, true

	case ss.Block == nil:
		// No momentum holds the send. The node's account chain says whether the send is still
		// pooled, replaced, or gone.
		f, err := node.FrontierAccountBlock(executor)
		if err != nil {
			return sendCheck{}, err
		}
		switch {
		case f == nil || f.Height < r.Height:
			return sendCheck{Outcome: OutcomeDropped}, nil
		case f.Height == r.Height:
			blk = f
		default:
			blk, err = node.AccountBlockAtHeight(executor, r.Height)
			if err != nil {
				return sendCheck{}, err
			}
			if blk == nil {
				return sendCheck{}, fmt.Errorf("supervisor: znnd reported frontier height %d for %s but no block at height %d", f.Height, executor, r.Height)
			}
		}

		switch {
		case blk.Hash == r.Hash:
			unconfirmed = true
		case blk.ToAddress == types.SettlementContract && types.NewHash(blk.Data) == r.DataHash:
			// A copy of the send holds the account height. It is followed through its own status.
			cs, err := node.SendStatus(blk.Hash)
			if err != nil {
				return sendCheck{}, err
			}
			if cs.Block != nil && cs.Received {
				ss = cs
			} else {
				unconfirmed = true
			}
		default:
			return sendCheck{Outcome: OutcomeSuperseded, Other: blk.Hash}, nil
		}
	}

	if unconfirmed {
		m, err := node.FrontierMomentum()
		if err != nil {
			return sendCheck{}, err
		}
		var age uint64
		if m.Height > blk.MomentumAcknowledged.Height {
			age = m.Height - blk.MomentumAcknowledged.Height
		}
		return sendCheck{Unconfirmed: true, Age: age}, nil
	}
	return readOutcome(anchors, r, ss)
}

// readOutcome decides a send whose receive is in a momentum, by comparing the commitment in the
// send's Data with what Settlement holds at the confirmed height.
func readOutcome(anchors AnchorSource, r SendRecord, ss znnd.SendStatus) (sendCheck, error) {
	if h := types.NewHash(ss.Block.Data); h != r.DataHash {
		return sendCheck{}, fmt.Errorf("supervisor: znnd returned send %s whose data hash %s differs from the record's %s", ss.Block.Hash, h, r.DataHash)
	}
	switch r.Method {
	case definition.SettlementRelayMessageMethodName:
		return relayOutcome(anchors, r, ss)
	case definition.SettlementReleaseWithdrawalsMethodName:
		return releaseOutcome(anchors, r, ss)
	}
	// A commitment this build cannot unpack or decode (another build's codec version) leaves c
	// zero and skips only the comparison with Settlement's accepted entry.
	var c codec.BatchCommitment
	var undecodable error
	var p struct{ Commitment []byte }
	if err := definition.ABISettlement.UnpackMethod(&p, definition.SettlementSubmitBatchMethodName, ss.Block.Data); err != nil {
		undecodable = err
	} else if c, err = codec.DecodeBatchCommitment(p.Commitment); err != nil {
		undecodable = err
	}

	t, err := anchors.Tip(r.DomainID)
	if err != nil {
		if errors.Is(err, recovery.ErrHalted) {
			return sendCheck{Fault: fmt.Errorf("%w: domain %d: Settlement read for batch %d's outcome: %w", ErrInconsistent, r.DomainID, r.BatchID, err)}, nil
		}
		return sendCheck{}, err
	}
	if t.Height < ss.ReceiveHeight {
		return sendCheck{}, nil
	}
	// A receive executed while nextBatchId was not B cannot have accepted B.
	if t.NextBatchID <= r.BatchID {
		return sendCheck{Outcome: OutcomeRejected}, nil
	}

	e := t.Prev
	if t.NextBatchID != r.BatchID+1 {
		e, err = anchors.AcceptedEntry(r.DomainID, r.BatchID)
		if err != nil {
			if errors.Is(err, recovery.ErrHalted) {
				return sendCheck{Fault: fmt.Errorf("%w: domain %d: Settlement read for batch %d's outcome: %w", ErrInconsistent, r.DomainID, r.BatchID, err)}, nil
			}
			return sendCheck{}, err
		}
	}
	if undecodable != nil {
		log.Printf("supervisor: domain %d INFO: batch %d: send %s carries a commitment this build cannot decode (%v); recorded accepted from Settlement's nextBatchId %d without comparing its accepted entry", r.DomainID, r.BatchID, ss.Block.Hash, undecodable, t.NextBatchID)
		return sendCheck{Outcome: OutcomeAccepted}, nil
	}
	if err := settlement.EntryMatches(e, c); err != nil {
		return sendCheck{Outcome: OutcomeRejected, Fault: fmt.Errorf("%w: domain %d batch %d differs from Settlement's accepted entry: %w", ErrInconsistent, r.DomainID, r.BatchID, err)}, nil
	}
	return sendCheck{Outcome: OutcomeAccepted}, nil
}

// retryState is a batch's backoff after a rejection or a dropped send.
type retryState struct {
	batchID uint64
	delay   time.Duration
	until   time.Time
	dropped uint64 // the account height of this batch's latest dropped send; 0 when none

	rejections int // consecutive rejected outcomes of batchID
}

// pendingSend is the one unresolved send across all domains.
type pendingSend struct {
	slot       int // -1 when the send's domain is not configured
	r          SendRecord
	alerted    bool
	unreadable error // why this build cannot read the restored record; nil for every other send
}

// submitter sends each domain's held batches from the executor address, one unresolved send at a
// time, and follows each send to its outcome. Only its own goroutine calls step; mu guards the
// status texts and the log that SubmitterStatus reads.
type submitter struct {
	s         *Supervisor
	log       sendLog
	poll      time.Duration
	warnEvery time.Duration
	now       func() time.Time

	pending *pendingSend
	next    int // the slot a pick pass starts at
	retry   map[uint32]*retryState
	below   map[uint32]uint64    // the view height at which a domain's cursor was first seen below its mark
	warned  map[string]time.Time // when each rate-limited line was last logged

	withdrawalTurn bool                   // the latest send was a batch send, so the next pick tries the withdrawal pass first
	wnext          int                    // the slot the withdrawal pass starts at
	relays         map[uint32]*relayBatch // each domain's cached relay batch
	relayRetry     map[uint32]*relayRetry // each domain's relay backoff
	releaseRetry   map[uint32]*relayRetry // each domain's release backoff after a failed build; id is zero

	mu         sync.Mutex
	alertText  string
	plasmaText string
	logText    string // why sending stopped after a log write failed; empty otherwise
}

func newSubmitter(s *Supervisor) *submitter {
	return &submitter{
		s:         s,
		log:       &memLog{},
		poll:      submitPoll,
		warnEvery: time.Minute,
		now:       time.Now,
		retry:     make(map[uint32]*retryState),
		below:     make(map[uint32]uint64),
		warned:    make(map[string]time.Time),

		relays:       make(map[uint32]*relayBatch),
		relayRetry:   make(map[uint32]*relayRetry),
		releaseRetry: make(map[uint32]*relayRetry),
	}
}

// run steps the submitter, waiting u.poll between steps, until ctx ends.
func (u *submitter) run(ctx context.Context) {
	for ctx.Err() == nil {
		u.step(ctx)
		t := time.NewTimer(u.poll)
		select {
		case <-ctx.Done():
			t.Stop()
			return
		case <-t.C:
		}
	}
}

// step checks the pending send when there is one, else picks a domain and sends its batch. It
// stops at the next slot visit, or before recording a send, once ctx is done.
func (u *submitter) step(ctx context.Context) {
	if u.pending != nil {
		u.check()
		return
	}
	u.pick(ctx)
}

func (u *submitter) threshold() uint64 {
	return alertDepths * u.s.cfg.ConfirmationDepth
}

// warnf logs the line unless key was logged within u.warnEvery.
func (u *submitter) warnf(key, format string, args ...any) {
	now := u.now()
	if last, ok := u.warned[key]; ok && now.Sub(last) < u.warnEvery {
		return
	}
	u.warned[key] = now
	log.Printf(format, args...)
}

// outage logs a node or Settlement read failure, at most once per u.warnEvery.
func (u *submitter) outage(err error) {
	u.warnf("outage", "supervisor: WARN: submitter: znnd node at %s: %q; retrying", redactEndpoint(u.s.cfg.ZnndEndpoint), u.redactErr(err))
}

// redactErr returns err's text with the request URL of a failed node call replaced by the
// endpoint's scheme and host, so userinfo, path and query never reach the log.
func (u *submitter) redactErr(err error) string {
	return redactEndpointErr(u.s.cfg.ZnndEndpoint, err)
}

// halt halts the domain with err, which wraps ErrInconsistent, and logs it when it is the
// domain's first fault.
func (u *submitter) halt(st *workerState, domainID uint32, err error) {
	if haltDomain(st, err) {
		log.Printf("supervisor: domain %d %v", domainID, err)
	}
}

// pick runs the batch pass and the withdrawal pass, the one that did
// not send last first, and stops at the first send. A pass with nothing
// ready never blocks the other.
func (u *submitter) pick(ctx context.Context) {
	if u.stopped() {
		return
	}
	if u.withdrawalTurn {
		u.withdrawalPass(ctx)
	} else {
		u.batchPass(ctx)
	}
	if u.pending == nil && ctx.Err() == nil && !u.stopped() {
		if u.withdrawalTurn {
			u.batchPass(ctx)
		} else {
			u.withdrawalPass(ctx)
		}
	}
	if u.pending != nil {
		u.withdrawalTurn = u.pending.r.Method == definition.SettlementSubmitBatchMethodName
	}
}

// stopped reports whether sending stopped after a log write failed.
func (u *submitter) stopped() bool {
	u.mu.Lock()
	defer u.mu.Unlock()
	return u.logText != ""
}

// batchPass visits every slot once, from u.next, and sends for the
// first domain that is ready.
func (u *submitter) batchPass(ctx context.Context) {
	n := len(u.s.slots)
	start := u.next
	for k := 0; k < n; k++ {
		if ctx.Err() != nil {
			return
		}
		if u.visit(ctx, (start+k)%n) {
			return
		}
	}
}

// visit applies the guard to slot i and sends its batch when the guard passes. It reports
// whether the pass ends.
func (u *submitter) visit(ctx context.Context, i int) bool {
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

	t, err := s.src.Anchors.Tip(d)
	if err != nil {
		if errors.Is(err, recovery.ErrHalted) {
			u.halt(st, d, fmt.Errorf("%w: domain %d: Settlement read for submission: %w", ErrInconsistent, d, err))
		} else {
			u.outage(err)
		}
		return false
	}

	// The Tip read raises the mark like any confirmed-height read, so a later read below it is
	// a lagging node and fails the guard. No send is pending here, so the raise cannot prune a
	// batch whose outcome is unresolved.
	n := t.NextBatchID
	st.mu.Lock()
	mark := raiseNext(st, n)
	c, ok := st.commitments[n]
	st.mu.Unlock()

	if n < mark {
		u.warnf(fmt.Sprintf("next-%d", d), "supervisor: domain %d WARN: znnd node at %s reported nextBatchId %d, below the observed %d; keeping the observed value", d, redactEndpoint(s.cfg.ZnndEndpoint), n, mark)
		u.stall(st, d, t.Height, n, mark)
		return false
	}
	delete(u.below, d)
	st.mu.Lock()
	st.submitError = ""
	st.mu.Unlock()

	if r := u.retry[d]; r != nil && n > r.batchID {
		u.dropRetry(st, d)
	}
	if r := u.retry[d]; r != nil && u.now().Before(r.until) {
		return false
	}

	if !ok || !t.HaveInput {
		return false
	}
	if t.FirstInput != c.FirstInputSeq {
		u.halt(st, d, fmt.Errorf("%w: domain %d batch %d starts at input %d, but Settlement's next input is %d", ErrInconsistent, d, n, c.FirstInputSeq, t.FirstInput))
		return false
	}
	prev := types.ZeroHash
	if t.Prev != nil {
		prev = t.Prev.PostStateRoot
	}
	if c.PreStateRoot != prev {
		u.halt(st, d, fmt.Errorf("%w: domain %d batch %d preStateRoot %s differs from Settlement's previous postStateRoot %s", ErrInconsistent, d, n, c.PreStateRoot, prev))
		return false
	}
	enc, err := c.Encode()
	var data []byte
	if err == nil {
		data, err = definition.ABISettlement.PackMethod(definition.SettlementSubmitBatchMethodName, enc)
	}
	if err != nil {
		u.halt(st, d, fmt.Errorf("%w: domain %d batch %d cannot be encoded for submission: %w", ErrInconsistent, d, n, err))
		return false
	}

	g, err := s.src.Anchors.Gates(d, s.cfg.PillarName)
	if err != nil {
		u.outage(err)
		return false
	}
	if why := gateBlock(g, d, s.cfg.ExecutorAddress, s.cfg.PillarName); why != "" {
		u.warnf(fmt.Sprintf("gate-%d", d), "supervisor: WARN: submitter: domain %d batch %d not sent (Settlement at height %d): %s; checking again", d, n, g.Height, why)
		return false
	}

	return u.send(ctx, i, t, c, data)
}

// gateBlock returns why g forbids a send for domainID now, or "" when it allows one.
func gateBlock(g settlement.Gates, domainID uint32, executor types.Address, pillarName string) string {
	switch {
	case g.GlobalPause:
		return "Settlement's global submit pause is set"
	case g.DomainPause:
		return fmt.Sprintf("Settlement's submit pause for domain %d is set", domainID)
	case g.Member == nil:
		return fmt.Sprintf("pillar %q has no executor-set member", pillarName)
	case g.Member.ExecutorAddress != executor:
		return fmt.Sprintf("pillar %q's executor-set member names %s, not this executor's %s", pillarName, g.Member.ExecutorAddress, executor)
	case g.Member.Status != definition.ExecutorStatusActive:
		return fmt.Sprintf("pillar %q's executor-set member has status %d, not active", pillarName, g.Member.Status)
	case !g.PillarActive:
		return fmt.Sprintf("pillar %q is not an active pillar", pillarName)
	}
	return ""
}

// stall records that domain d's Settlement cursor n is below the mark, and raises the domain's
// error status once the view has stayed there for the threshold.
func (u *submitter) stall(st *workerState, d uint32, height, n, mark uint64) {
	start, ok := u.below[d]
	if !ok {
		start = height
		u.below[d] = height
	}
	if height < start+u.threshold() {
		return
	}
	text := fmt.Sprintf("Settlement's nextBatchId %d stayed below the observed %d for %d momenta: the znnd node is lagging, or a reorg deeper than confirmationDepth undid accepted batches the supervisor no longer holds; check the node, then restart the executor; after such a reorg, move the submission log files aside first, because a restart reads the observed value back from them", n, mark, height-start)
	st.mu.Lock()
	st.submitError = text
	st.mu.Unlock()
	u.warnf(fmt.Sprintf("stall-%d", d), "supervisor: domain %d ERROR: %s", d, text)
}

// send adopts or builds the block that carries c, records it and publishes it. It reports whether
// the pass ends.
func (u *submitter) send(ctx context.Context, i int, t settlement.Tip, c codec.BatchCommitment, data []byte) bool {
	s := u.s
	sl := s.slots[i]
	st := sl.st
	d := sl.w.DomainID
	n := t.NextBatchID
	executor := s.cfg.ExecutorAddress
	dh := types.NewHash(data)

	var b *nom.AccountBlock
	adopted := false
	readFrontier := false
	var f *nom.AccountBlock
	if r := u.retry[d]; r != nil && r.dropped > 0 {
		var err error
		f, err = s.src.Node.FrontierAccountBlock(executor)
		if err != nil {
			u.outage(err)
			return true
		}
		readFrontier = true
		if f != nil && r.dropped <= f.Height {
			blk, err := s.src.Node.AccountBlockAtHeight(executor, r.dropped)
			if err != nil {
				u.outage(err)
				return true
			}
			if blk == nil {
				u.outage(fmt.Errorf("supervisor: znnd reported frontier height %d for %s but no block at height %d", f.Height, executor, r.dropped))
				return true
			}
			if blk.ToAddress == types.SettlementContract && types.NewHash(blk.Data) == dh {
				b, adopted = blk, true
			}
		}
	}

	if !adopted {
		var err error
		b, err = buildSend(s.src.Node, s.key, data)
		if err != nil {
			if errors.Is(err, errInsufficientPlasma) {
				u.mu.Lock()
				u.plasmaText = err.Error()
				u.mu.Unlock()
				u.warnf("plasma", "supervisor: WARN: submitter: domain %d batch %d: %s; retrying", d, n, err.Error())
			} else {
				u.outage(err)
			}
			// The domain waits out its batch backoff and the pass moves on, so a build
			// that keeps failing cannot starve the other domains.
			u.backoff(d, n)
			u.next = (i + 1) % len(s.slots)
			return false
		}
		u.mu.Lock()
		u.plasmaText = ""
		u.mu.Unlock()
		if readFrontier {
			want := uint64(1)
			if f != nil {
				want = f.Height + 1
			}
			if b.Height != want {
				u.outage(fmt.Errorf("supervisor: account frontier of %s moved from height %d to %d while building", executor, want-1, b.Height-1))
				return true
			}
		}
	}

	if ctx.Err() != nil {
		return true
	}
	s.mu.Lock()
	state := sl.state
	s.mu.Unlock()
	st.mu.Lock()
	if state == StateHalted || state == StateConfigError || st.fault != "" {
		// The domain halted while the send was built; a halted domain publishes nothing.
		st.mu.Unlock()
		return false
	}
	held, ok := st.commitments[n]
	if !ok || !reflect.DeepEqual(held, c) {
		// A Retract or re-announce changed the batch; the next pass re-reads it.
		st.mu.Unlock()
		return false
	}
	st.unresolved = &unresolvedSend{batchID: n}
	st.mu.Unlock()

	r := SendRecord{
		DomainID: d,
		Method:   definition.SettlementSubmitBatchMethodName,
		BatchID:  n,
		Height:   b.Height,
		Hash:     b.Hash,
		DataHash: dh,
	}
	if err := u.log.Sent(r); err != nil {
		st.mu.Lock()
		st.unresolved = nil
		st.mu.Unlock()
		u.logFailed(err)
		return true
	}

	switch {
	case adopted:
		log.Printf("supervisor: domain %d batch %d: following send %s at account height %d, already on the node", d, n, b.Hash, b.Height)
	default:
		if err := s.src.Node.PublishRawTransaction(b); err != nil {
			// The failed publish may still have reached the pool, so the send is followed anyway.
			// The batch's backoff paces the attempts, and so these lines.
			log.Printf("supervisor: domain %d batch %d: publishing %s failed: %q; following it", d, n, b.Hash, u.redactErr(err))
		} else {
			log.Printf("supervisor: domain %d batch %d: sent %s at account height %d", d, n, b.Hash, b.Height)
		}
	}

	u.pending = &pendingSend{slot: i, r: r}
	u.next = (i + 1) % len(s.slots)
	return true
}

// check follows the pending send one step.
func (u *submitter) check() {
	s := u.s
	p := u.pending
	var st *workerState
	if p.slot >= 0 {
		st = s.slots[p.slot].st
	}
	d := p.r.DomainID

	var ck sendCheck
	var err error
	if p.unreadable == nil {
		ck, err = resolveSend(s.src.Node, s.src.Anchors, s.cfg.ExecutorAddress, p.r)
	} else {
		ck = sendCheck{Fault: fmt.Errorf("%w: domain %d %s: send %s in the submission log cannot be read by this build: %w; it is recorded abandoned and not followed", ErrInconsistent, d, sendLabel(p.r), p.r.Hash, p.unreadable)}
	}
	switch {
	case err != nil:
		u.outage(err)

	case st == nil && (ck.Outcome != "" || ck.Fault != nil):
		u.dropRemoved(p, ck)

	case ck.Fault != nil && ck.Outcome == "":
		u.halt(st, d, ck.Fault)
		u.abandon(p, st)

	case ck.Fault != nil:
		u.halt(st, d, ck.Fault)
		u.finish(p, st, ck.Outcome, ck.Other)

	case ck.Unconfirmed:
		if ck.Age >= u.threshold() && !p.alerted {
			text := fmt.Sprintf("send %s for domain %d %s has not been received by Settlement after %d momenta (confirmationDepth x 10 is %d); it is not replaced automatically", p.r.Hash, d, sendLabel(p.r), ck.Age, u.threshold())
			u.mu.Lock()
			u.alertText = text
			u.mu.Unlock()
			p.alerted = true
			log.Printf("supervisor: WARN: %s", text)
		}

	case ck.Outcome != "":
		u.finish(p, st, ck.Outcome, ck.Other)
	}
}

// abandon closes the pending send of a domain that halted before the send's outcome was known.
func (u *submitter) abandon(p *pendingSend, st *workerState) {
	st.mu.Lock()
	st.unresolved = nil
	st.mu.Unlock()
	r := p.r
	r.Outcome = OutcomeAbandoned
	if err := u.log.Resolved(r); err != nil {
		u.logFailed(err)
	}
	u.release(p)
	log.Printf("supervisor: domain %d %s: send %s abandoned: the domain halted", r.DomainID, sendLabel(r), r.Hash)
}

// dropRemoved records the outcome ck gives the pending send of a domain that is not configured,
// logs it and ends the send.
func (u *submitter) dropRemoved(p *pendingSend, ck sendCheck) {
	r := p.r
	r.Outcome = ck.Outcome
	if r.Outcome == "" {
		r.Outcome = OutcomeAbandoned
	}
	if err := u.log.Resolved(r); err != nil {
		u.logFailed(err)
	}
	u.release(p)
	var fault string
	if ck.Fault != nil {
		fault = fmt.Sprintf(" (%v)", ck.Fault)
	}
	log.Printf("supervisor: WARN: submitter: domain %d is not configured: its send %s for %s from the submission log finished %s%s; nothing more is sent for it", r.DomainID, r.Hash, sendLabel(r), r.Outcome, fault)
}

// logFailed stops sending after a submission log write failed: the first call sets logText and
// logs one ERROR line; later calls do nothing.
func (u *submitter) logFailed(err error) {
	u.mu.Lock()
	if u.logText != "" {
		u.mu.Unlock()
		return
	}
	text := fmt.Sprintf("%v; nothing is sent until the executor restarts", err)
	u.logText = text
	u.mu.Unlock()
	log.Printf("supervisor: ERROR: submitter: %s", text)
}

// resume makes lg the submitter's log, raises each configured domain's mark to one above its
// batchId in accepted, and makes lg's last send the pending send when it has no outcome;
// unreadable is why that send's record cannot be read, or nil. Run calls it before any worker
// or the submitter starts.
func (u *submitter) resume(lg sendLog, accepted map[uint32]uint64, unreadable error) {
	u.mu.Lock()
	u.log = lg
	u.mu.Unlock()

	for _, sl := range u.s.slots {
		b, ok := accepted[sl.w.DomainID]
		if !ok {
			continue
		}
		sl.st.mu.Lock()
		mark := raiseNext(sl.st, b+1)
		sl.st.mu.Unlock()
		log.Printf("supervisor: domain %d: observed nextBatchId %d from the submission log", sl.w.DomainID, mark)
	}

	r, ok := lg.Last()
	if !ok || r.Outcome != "" {
		return
	}
	for i, sl := range u.s.slots {
		if sl.w.DomainID != r.DomainID {
			continue
		}
		if r.Method != definition.SettlementRelayMessageMethodName && r.Method != definition.SettlementReleaseWithdrawalsMethodName {
			sl.st.mu.Lock()
			sl.st.unresolved = &unresolvedSend{batchID: r.BatchID}
			sl.st.mu.Unlock()
		}
		u.pending = &pendingSend{slot: i, r: r, unreadable: unreadable}
		if unreadable == nil {
			log.Printf("supervisor: domain %d %s: following send %s at account height %d from the submission log", r.DomainID, sendLabel(r), r.Hash, r.Height)
		}
		return
	}
	u.pending = &pendingSend{slot: -1, r: r, unreadable: unreadable}
	if unreadable == nil {
		log.Printf("supervisor: WARN: submitter: domain %d is not configured; following its send %s for %s from the submission log to its outcome before sending anything", r.DomainID, r.Hash, sendLabel(r))
	}
}

// release ends the pending send and clears its alert.
func (u *submitter) release(p *pendingSend) {
	if p.alerted {
		u.mu.Lock()
		u.alertText = ""
		u.mu.Unlock()
	}
	u.pending = nil
}

// backoff doubles the batch's delay, or starts it at the restart backoff's minimum, and returns
// the batch's retry state with its wait restarted.
func (u *submitter) backoff(d uint32, batchID uint64) *retryState {
	r := u.retry[d]
	if r != nil && r.batchID == batchID {
		r.delay = double(r.delay, u.s.policy)
	} else {
		r = &retryState{batchID: batchID, delay: u.s.policy.BackoffMin}
		u.retry[d] = r
	}
	r.until = u.now().Add(r.delay)
	return r
}

// dropRetry forgets domain d's batch backoff and rejection count, and clears its rejection status.
func (u *submitter) dropRetry(st *workerState, d uint32) {
	delete(u.retry, d)
	st.mu.Lock()
	st.rejectError = ""
	st.mu.Unlock()
}

// finish closes the pending send with outcome o. other names the block that superseded it.
func (u *submitter) finish(p *pendingSend, st *workerState, o Outcome, other types.Hash) {
	switch p.r.Method {
	case definition.SettlementRelayMessageMethodName:
		u.finishRelay(p, st, o, other)
		return
	case definition.SettlementReleaseWithdrawalsMethodName:
		u.finishRelease(p, o, other)
		return
	}
	r := p.r
	d, b := r.DomainID, r.BatchID

	st.mu.Lock()
	retracted := st.unresolved != nil && st.unresolved.retracted
	st.unresolved = nil
	if o == OutcomeAccepted {
		raiseNext(st, b+1)
	}
	st.mu.Unlock()

	if retracted {
		if o == OutcomeAccepted {
			u.halt(st, d, fmt.Errorf("%w: domain %d batch %d was retracted by its worker after Settlement accepted it", ErrInconsistent, d, b))
		} else {
			o = OutcomeSuperseded
		}
	}

	r.Outcome = o
	if err := u.log.Resolved(r); err != nil {
		u.logFailed(err)
	}
	u.release(p)

	switch o {
	case OutcomeAccepted:
		u.dropRetry(st, d)
		log.Printf("supervisor: domain %d batch %d accepted (send %s)", d, b, r.Hash)

	case OutcomeRejected:
		rs := u.backoff(d, b)
		rs.dropped = 0
		rs.rejections++
		log.Printf("supervisor: domain %d batch %d rejected by Settlement (send %s); retrying in %s", d, b, r.Hash, rs.delay)
		if rs.rejections >= rejectAlert {
			text := fmt.Sprintf("Settlement rejected batch %d %d consecutive times (latest send %s); retrying without limit, and each attempt costs plasma", b, rs.rejections, r.Hash)
			st.mu.Lock()
			st.rejectError = text
			st.mu.Unlock()
			if rs.rejections == rejectAlert {
				log.Printf("supervisor: domain %d ERROR: %s", d, text)
			} else {
				log.Printf("supervisor: domain %d WARN: %s", d, text)
			}
		}

	case OutcomeDropped:
		rs := u.backoff(d, b)
		rs.dropped = r.Height
		log.Printf("supervisor: domain %d batch %d: send %s at account height %d is unknown to the node; rebuilding in %s", d, b, r.Hash, r.Height, rs.delay)

	case OutcomeSuperseded:
		u.backoff(d, b).dropped = 0
		by := "retracted by the worker"
		if other != (types.Hash{}) {
			by = other.String()
		}
		log.Printf("supervisor: domain %d batch %d: send %s superseded (%s)", d, b, r.Hash, by)
	}
}
