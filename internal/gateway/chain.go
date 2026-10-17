// Package gateway is the gateway's core: it rebuilds a served domain's
// state from the bundles and receipt sidecars an executor publishes,
// checks every batch against Settlement before serving it at latest,
// and keeps the state versions that VersionAt serves.
package gateway

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io/fs"
	"log"
	"math"
	"runtime"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/zenon-network/go-zenon/common/crypto"
	"github.com/zenon-network/go-zenon/common/types"
	"github.com/zenon-network/go-zenon/vm/constants"
	"github.com/zenon-network/go-zenon/vm/embedded/definition"

	"github.com/digitalSloth/go-executor/domain"
	"github.com/digitalSloth/go-executor/internal/codec"
	"github.com/digitalSloth/go-executor/internal/recovery"
	"github.com/digitalSloth/go-executor/internal/state"
)

// Status is a served domain's status in zeno_getHead.
type Status string

const (
	StatusSyncing      Status = "SYNCING"
	StatusOK           Status = "OK"
	StatusInconsistent Status = "INCONSISTENT"
)

// BatchHead is the batch one tag resolves to.
type BatchHead struct {
	BatchID       uint64
	LastInputSeq  uint64
	PostStateRoot types.Hash
}

// Head is a domain's three tags and its status. Latest and Finalized
// are nil until a batch reaches them. Pending equals Latest when no
// pending batch is held.
type Head struct {
	Pending, Latest, Finalized *BatchHead
	Status                     Status
	// InconsistentAt is the batch whose failed check made the domain
	// INCONSISTENT; nil unless Status is INCONSISTENT.
	InconsistentAt *uint64
}

// Settlement is the Settlement state the engine reads, verified
// against momentum headers.
type Settlement interface {
	// ConfirmedHeight returns the newest momentum height the gateway
	// treats as confirmed.
	ConfirmedHeight() (uint64, error)
	// At returns Settlement storage as of height.
	At(height uint64) (SettlementView, error)
}

// SettlementView is Settlement storage at one height.
// *settlement.View satisfies it.
type SettlementView interface {
	InputSequenceCursor(domainID uint32) (*definition.InputSequenceCursor, error)
	BatchEntry(domainID uint32, batchID uint64) (*definition.BatchEntry, bool, error)
	DomainRecord(domainID uint32) (*definition.DomainRecord, bool, error)
	OutboxProcessed(outboxID types.Hash) (bool, error)
	// Momentum returns the height, unix time and hash of the momentum
	// the view reads at.
	Momentum() (height, unixTime uint64, hash types.Hash)
	AssetConfig(domainID uint32, assetID types.Hash) (*definition.AssetConfig, bool, error)
}

// withDeadline runs read on its own goroutine and returns its result,
// or an error wrapping errNoAnswer and the context's error once ctx
// ends or timeout passes, whichever comes first. When ctx has already
// ended, it returns that error without starting read. An abandoned
// read runs until it returns, and its result is dropped. A read that
// panics returns an error naming the panic and the first frame outside
// the Go runtime, so a malformed node answer cannot end the process
// and the same panic yields the same text on every call.
func withDeadline[T any](ctx context.Context, timeout time.Duration, read func() (T, error)) (T, error) {
	var zero T
	if err := ctx.Err(); err != nil {
		return zero, fmt.Errorf("%w: %w", errNoAnswer, err)
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	type result struct {
		v   T
		err error
	}
	done := make(chan result, 1)
	go func() {
		defer func() {
			if p := recover(); p != nil {
				done <- result{err: fmt.Errorf("the read panicked: %v (at %s)", p, panicSite())}
			}
		}()
		v, err := read()
		done <- result{v, err}
	}()
	select {
	case r := <-done:
		return r.v, r.err
	case <-ctx.Done():
		return zero, fmt.Errorf("%w: %w", errNoAnswer, ctx.Err())
	}
}

// panicSite returns "function file:line" of the first frame above its
// caller, a deferred recover, that is not in the Go runtime: the
// frame that panicked. It has no goroutine id or pointer, so it is the
// same on every call.
func panicSite() string {
	pcs := make([]uintptr, 32)
	n := runtime.Callers(3, pcs)
	frames := runtime.CallersFrames(pcs[:n])
	for {
		f, more := frames.Next()
		if !strings.HasPrefix(f.Function, "runtime.") {
			return fmt.Sprintf("%s %s:%d", f.Function, f.File, f.Line)
		}
		if !more {
			return "unknown"
		}
	}
}

// timedView is a SettlementView whose every read runs under
// withDeadline with ctx and timeout.
type timedView struct {
	ctx     context.Context
	timeout time.Duration
	view    SettlementView
}

func (t timedView) InputSequenceCursor(domainID uint32) (*definition.InputSequenceCursor, error) {
	return withDeadline(t.ctx, t.timeout, func() (*definition.InputSequenceCursor, error) {
		return t.view.InputSequenceCursor(domainID)
	})
}

func (t timedView) BatchEntry(domainID uint32, batchID uint64) (*definition.BatchEntry, bool, error) {
	type r struct {
		e  *definition.BatchEntry
		ok bool
	}
	out, err := withDeadline(t.ctx, t.timeout, func() (r, error) {
		e, ok, err := t.view.BatchEntry(domainID, batchID)
		return r{e, ok}, err
	})
	return out.e, out.ok, err
}

func (t timedView) DomainRecord(domainID uint32) (*definition.DomainRecord, bool, error) {
	type r struct {
		rec *definition.DomainRecord
		ok  bool
	}
	out, err := withDeadline(t.ctx, t.timeout, func() (r, error) {
		rec, ok, err := t.view.DomainRecord(domainID)
		return r{rec, ok}, err
	})
	return out.rec, out.ok, err
}

func (t timedView) OutboxProcessed(outboxID types.Hash) (bool, error) {
	return withDeadline(t.ctx, t.timeout, func() (bool, error) {
		return t.view.OutboxProcessed(outboxID)
	})
}

func (t timedView) Momentum() (height, unixTime uint64, hash types.Hash) {
	return t.view.Momentum()
}

func (t timedView) AssetConfig(domainID uint32, assetID types.Hash) (*definition.AssetConfig, bool, error) {
	type r struct {
		cfg *definition.AssetConfig
		ok  bool
	}
	out, err := withDeadline(t.ctx, t.timeout, func() (r, error) {
		cfg, ok, err := t.view.AssetConfig(domainID, assetID)
		return r{cfg, ok}, err
	})
	return out.cfg, out.ok, err
}

// ChainConfig configures one domain's engine.
type ChainConfig struct {
	DomainID   uint32
	Source     Source
	Settlement Settlement
	// Final is the finalized state, which the engine applies each
	// batch to as it finalizes. It holds the state after Start, or the
	// empty state when Start is nil.
	Final *state.Store
	// Start is the finalized batch Final holds; nil at genesis.
	Start *BatchHead
	// Snapshots is the directory the engine saves Final's snapshots in.
	Snapshots string
	// Index is the domain's on-disk index; nil indexes nothing.
	Index *Index
	// Logf receives operator log lines; nil selects log.Printf.
	Logf func(format string, args ...any)
}

// Chain follows one domain and holds its state versions. Poll is its
// only writer and is not safe for concurrent use; Head and VersionAt
// are safe from any goroutine, including while Poll runs.
type Chain struct {
	cfg ChainConfig

	// mu guards every field below up to the writer-only fields. Poll
	// reads them without the lock, because it is their only writer.
	mu sync.RWMutex
	// finalized is nil until the first batch finalizes, and
	// finalVersion is the state after it (the empty state before).
	finalized    *BatchHead
	finalVersion state.Version
	// batches runs from finalized+1 (or 0) through pending with
	// contiguous ids; batches[:nVerified] are checked against
	// Settlement entries.
	batches   []batchState
	nVerified int
	status    Status
	// inconsistentAt is the batch markInconsistent first named.
	inconsistentAt uint64
	// record is the domain record Poll last found; nil until then.
	record *definition.DomainRecord
	// view is the raw Settlement view of the newest poll; nil until a
	// poll reads one.
	view SettlementView

	// lastBlocked is the last blocking reason logged.
	lastBlocked string
	// prevMismatch is the pre-state mismatch the previous poll saw, and
	// warned the mismatches already logged.
	prevMismatch *mismatchKey
	warned       map[mismatchKey]bool
	// overlapWarned maps the pending batch id whose record overlaps the
	// batch below it to that batch's lastInputSeq at the time of the
	// warning. It holds at most the next pending id.
	overlapWarned map[uint64]uint64

	// snapshotDue is set when a batch finalizes and cleared by a
	// successful snapshot save.
	snapshotDue bool
	// lastSave is when the last snapshot save succeeded; zero until the
	// first.
	lastSave time.Time
	// recordAt is when Poll last found the domain record; zero until
	// then.
	recordAt time.Time
	// now is the engine's clock; NewChain sets it to time.Now.
	now func() time.Time
	// heldBudget is the most cost the pending batches may hold.
	heldBudget int64
	// refused is the pending batch the budget last refused.
	refused *refusal
	// foreign is the pending record whose batch names a send that is not
	// an input of the domain; the pass skips it until the record changes.
	foreign *foreignRefusal
	// senderWait is the pending record whose sender read last failed
	// and when; the pass skips it until senderRetryInterval passes or
	// the record changes.
	senderWait *senderBackoff
	// readTimeout is the deadline of each Settlement read; NewChain sets
	// it to readTimeout.
	readTimeout time.Duration
	// notify is the server told of batch and head changes; nil tells no
	// one.
	notify atomic.Pointer[Server]
	// cutBatch is the batch whose cut-fetch line was last logged, and
	// cutLogged whether one was. Poll is their only user.
	cutBatch  uint64
	cutLogged bool
}

// refusal is the pending batch the budget refused and the pending cost
// when it did.
type refusal struct {
	key         mismatchKey
	pendingHeld int64
}

// foreignRefusal identifies a pending record by its id, daHash and
// commitment (which holds the receiptRoot).
type foreignRefusal struct {
	id         uint64
	daHash     types.Hash
	commitment string
}

// senderBackoff is a pending record whose L1 sender read failed and
// the time of that failure.
type senderBackoff struct {
	key foreignRefusal
	at  time.Time
}

// batchState is one retained batch above finalized.
type batchState struct {
	// want holds the fields the batch's data matched: its entry's when
	// verified, its record commitment's when pending.
	want expect
	// version is the state after this batch.
	version state.Version
	// diff is every input's StateDiff in order, values copied.
	diff []domain.StateDiffEntry
	// finalizeAt is the newest read entry's FinalizeAtHeight, and
	// entryHeight the confirmed height that entry was read at. Both
	// are set for verified batches only.
	finalizeAt, entryHeight uint64
	// held is the batch's cost as checkBatch counts it.
	held int64
	// sends is each input's send hash in record order, copied.
	sends []types.Hash
}

// sendsOf returns the send hash of each input, copied so that no bundle
// buffer is kept. checkBatch rejects input data shorter than 33 bytes.
func sendsOf(inputs []batchInput) []types.Hash {
	out := make([]types.Hash, len(inputs))
	for i, in := range inputs {
		copy(out[i][:], in.inputData[1:33])
	}
	return out
}

// mismatchKey names one pending record whose preStateRoot does not
// chain onto the state below it.
type mismatchKey struct {
	batchID uint64
	daHash  types.Hash
}

// poll is the state of one Poll call.
type poll struct {
	ctx  context.Context
	h    uint64
	view SettlementView
	next uint64
	// blocked is the poll's first blocking reason.
	blocked string
	// mismatch is the pre-state mismatch the pending pass met, with the
	// roots it logs.
	mismatch         *mismatchKey
	recordPre, below types.Hash
	// finalizeHeld is set when finalize met an entry that is not
	// visible, which ends finalize for the rest of the poll.
	finalizeHeld bool
	// accepted is set once the poll has accepted a batch.
	accepted bool
}

// NewChain returns an engine at genesis with status SYNCING.
func NewChain(cfg ChainConfig) *Chain {
	if cfg.Logf == nil {
		cfg.Logf = log.Printf
	}
	c := &Chain{
		cfg:           cfg,
		status:        StatusSyncing,
		warned:        make(map[mismatchKey]bool),
		overlapWarned: make(map[uint64]uint64),
		finalVersion:  cfg.Final.Version(),
		now:           time.Now,
		heldBudget:    maxHeldBytes,
		readTimeout:   readTimeout,
	}
	if cfg.Start != nil {
		start := *cfg.Start
		c.finalized = &start
	}
	return c
}

// base returns the id of the first retained batch: 0 before
// anything finalizes, else the id after finalized. Batch id is
// batches[id-base()].
func (c *Chain) base() uint64 {
	if c.finalized == nil {
		return 0
	}
	return c.finalized.BatchID + 1
}

// latestNext returns the id after latest.
func (c *Chain) latestNext() uint64 {
	return c.base() + uint64(c.nVerified)
}

// baseVersion returns the state batch id applies to.
func (c *Chain) baseVersion(id uint64) state.Version {
	if id == c.base() {
		return c.finalVersion
	}
	return c.batches[id-c.base()-1].version
}

// emit tells the notified server that a batch changed.
func (c *Chain) emit(kind batchEventKind, want expect, sends []types.Hash) {
	if s := c.notify.Load(); s != nil {
		s.batchChanged(c, batchEvent{kind: kind, want: want, sends: sends})
	}
}

// retract drops batch id and every pending batch above it, then
// retracts the same batches in the index and reports each as removed.
func (c *Chain) retract(id uint64, reason string) error {
	c.mu.Lock()
	dropped := slices.Clone(c.batches[id-c.base():])
	c.batches = c.batches[:id-c.base()]
	fallback := "genesis"
	if n := len(c.batches); n > 0 {
		fallback = fmt.Sprint(c.base() + uint64(n) - 1)
	} else if c.finalized != nil {
		fallback = fmt.Sprint(c.finalized.BatchID)
	}
	c.mu.Unlock()
	c.cfg.Logf("gateway: domain %d: pending batch %d retracted with every pending batch above it (%s); pending falls back to %s", c.cfg.DomainID, id, reason, fallback)
	err := c.indexRetract(id)
	for _, b := range dropped {
		c.emit(evRemoved, b.want, b.sends)
	}
	return err
}

// markInconsistent ends the engine's advance for good.
func (c *Chain) markInconsistent(id uint64, err error) {
	c.mu.Lock()
	if c.status != StatusInconsistent {
		c.inconsistentAt = id
	}
	c.status = StatusInconsistent
	c.mu.Unlock()
	c.cfg.Logf("gateway: ALARM: domain %d is INCONSISTENT at batch %d: %v; it stops advancing and serves only the batches it already held", c.cfg.DomainID, id, err)
}

// remote reports whether the domain's source is a REMOTE one.
func (c *Chain) remote() bool {
	_, ok := c.cfg.Source.(*RemoteSource)
	return ok
}

func (c *Chain) inconsistent() bool {
	return c.status == StatusInconsistent
}

// setStatus publishes a status, except after INCONSISTENT.
func (c *Chain) setStatus(s Status) {
	c.mu.Lock()
	if c.status != StatusInconsistent {
		c.status = s
	}
	c.mu.Unlock()
}

// block records the poll's blocking reason; the first one wins.
func (p *poll) block(reason string) {
	if p.blocked == "" {
		p.blocked = reason
	}
}

// Poll reads Settlement at the confirmed height and the executor's
// directory once, and advances, finalizes, retracts and checks
// batches. It returns a Settlement error, a finalize error, a snapshot
// error, or a file read error other than a missing or unreadable file.
// A Settlement read that does not answer within readTimeout, or before
// ctx ends, returns as a Settlement error wrapping errNoAnswer. Every
// batch completed before the error stays published, and a snapshot or
// pending pass error returns after the poll publishes its status. An
// index write or L1 sender read error also returns as an error. Poll
// also refreshes the cached domain record; a failed record read
// returns after the poll publishes its status, like a snapshot error.
func (c *Chain) Poll(ctx context.Context) error {
	if c.inconsistent() {
		return nil
	}
	before := c.Head()
	defer func() {
		if s := c.notify.Load(); s != nil {
			if h := c.Head(); headChanged(before, h) {
				s.headMoved(c, h)
			}
		}
	}()
	id := c.cfg.DomainID
	h, err := withDeadline(ctx, c.readTimeout, c.cfg.Settlement.ConfirmedHeight)
	if err != nil {
		return fmt.Errorf("gateway: domain %d: settlement: %w", id, err)
	}
	view, err := withDeadline(ctx, c.readTimeout, func() (SettlementView, error) {
		return c.cfg.Settlement.At(h)
	})
	if err != nil {
		return fmt.Errorf("gateway: domain %d: settlement: %w", id, err)
	}
	c.mu.Lock()
	c.view = view
	c.mu.Unlock()
	tv := timedView{ctx: ctx, timeout: c.readTimeout, view: view}
	cur, err := tv.InputSequenceCursor(id)
	if err != nil {
		return fmt.Errorf("gateway: domain %d: settlement: %w", id, err)
	}
	var recErr error
	if err := c.refreshRecord(tv); err != nil {
		recErr = fmt.Errorf("gateway: domain %d: settlement: %w", id, err)
	}
	p := &poll{ctx: ctx, h: h, view: tv, next: cur.NextBatchId}
	if c.latestNext() < p.next {
		c.setStatus(StatusSyncing)
	}

	for c.latestNext() < p.next {
		if err := ctx.Err(); err != nil {
			return err
		}
		stop, err := c.accept(p, c.latestNext())
		if err != nil {
			return err
		}
		if c.inconsistent() {
			return nil
		}
		if stop {
			break
		}
		p.accepted = true
		if err := c.finalize(p); err != nil {
			return err
		}
		if c.inconsistent() {
			return nil
		}
	}
	if err := c.finalize(p); err != nil {
		return err
	}
	if c.inconsistent() {
		return nil
	}
	snapErr := c.snapshot(p)
	var pendingErr error
	if c.latestNext() >= p.next && !c.remote() {
		pendingErr = c.pending(p)
		if c.inconsistent() {
			return nil
		}
	}

	if c.latestNext() < p.next || p.blocked != "" {
		c.setStatus(StatusSyncing)
	} else {
		c.setStatus(StatusOK)
	}
	if p.blocked != "" && p.blocked != c.lastBlocked {
		c.cfg.Logf("%s", p.blocked)
	}
	c.lastBlocked = p.blocked
	c.warnPreStateMismatch(p)
	return errors.Join(recErr, snapErr, pendingErr)
}

// refreshRecord reads the domain record for the chain-info method when
// none is cached or recordInterval has passed since the last read that
// found it. An invisible record leaves the cache as it is.
func (c *Chain) refreshRecord(v SettlementView) error {
	if !c.recordAt.IsZero() && c.now().Sub(c.recordAt) < recordInterval {
		return nil
	}
	rec, ok, err := v.DomainRecord(c.cfg.DomainID)
	if err != nil || !ok {
		return err
	}
	c.mu.Lock()
	c.record = rec
	c.mu.Unlock()
	c.recordAt = c.now()
	return nil
}

// settlementView returns the raw Settlement view of the newest poll,
// nil until a poll has read one.
func (c *Chain) settlementView() SettlementView {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.view
}

// domainRecord returns the cached domain record, nil until a poll has
// found it. The caller must not modify it.
func (c *Chain) domainRecord() *definition.DomainRecord {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.record
}

// snapshot saves the finalized state when a batch has finalized since
// the last save, the throttle allows it and the poll left Final at the
// finalized batch's root.
func (c *Chain) snapshot(p *poll) error {
	if !c.snapshotDue || p.finalizeHeld {
		return nil
	}
	if !c.lastSave.IsZero() && c.now().Sub(c.lastSave) < snapshotInterval {
		return nil
	}
	id := c.cfg.DomainID
	root, err := c.cfg.Final.Root()
	if err != nil {
		return fmt.Errorf("gateway: domain %d: snapshot: %w", id, err)
	}
	if root != c.finalized.PostStateRoot {
		return nil
	}
	rec, ok, err := p.view.DomainRecord(id)
	if err != nil {
		return fmt.Errorf("gateway: domain %d: settlement: %w", id, err)
	}
	if !ok {
		return fmt.Errorf("gateway: domain %d: the domain record is not visible, so the snapshot waits", id)
	}
	if err := writeSnapshot(c.cfg.Final, c.cfg.Snapshots, *c.finalized, rec.StfSpecHash); err != nil {
		return fmt.Errorf("gateway: domain %d: save the finalized state at batch %d: %w", id, c.finalized.BatchID, err)
	}
	c.snapshotDue = false
	c.lastSave = c.now()
	c.cfg.Logf("gateway: domain %d: saved the finalized state at batch %d", id, c.finalized.BatchID)
	return nil
}

// warnPreStateMismatch logs one WARN when the same pending record has
// failed to chain on two consecutive polls. When the base is a held
// pending batch, the next poll retracts or rebuilds it; otherwise
// pending waits for the executor to rewrite its directory. The status
// does not change.
func (c *Chain) warnPreStateMismatch(p *poll) {
	prev := c.prevMismatch
	c.prevMismatch = p.mismatch
	if p.mismatch == nil || prev == nil || *prev != *p.mismatch || c.warned[*p.mismatch] {
		return
	}
	c.warned[*p.mismatch] = true
	c.cfg.Logf("gateway: WARN: domain %d: batch %d's record (daHash %s) has preStateRoot %s, but the state below it has root %s on two consecutive polls; pending stays below it until the executor rewrites its directory", c.cfg.DomainID, p.mismatch.batchID, p.mismatch.daHash, p.recordPre, p.below)
}

// readClass sorts a file read error.
type readClass int

const (
	readFailed readClass = iota
	readMissing
	readDenied
)

func classifyRead(err error) readClass {
	switch {
	case errors.Is(err, fs.ErrNotExist):
		return readMissing
	case errors.Is(err, fs.ErrPermission):
		return readDenied
	}
	return readFailed
}

func (c *Chain) deniedMessage(err error) string {
	return fmt.Sprintf("gateway: domain %d: %v; status SYNCING. The gateway cannot read this file; files written before the executor set read-interface modes stay unreadable until the executor's domain directory is re-synced: stop the executor, remove that domain directory and restart it", c.cfg.DomainID, err)
}

// accept builds batch id from its Settlement entry, or promotes the
// pending batch that already matches it. stop is true when the batch
// cannot be completed this poll.
func (c *Chain) accept(p *poll, id uint64) (stop bool, err error) {
	dom := c.cfg.DomainID
	e, ok, err := p.view.BatchEntry(dom, id)
	if err != nil {
		return false, err
	}
	if !ok {
		p.block(fmt.Sprintf("gateway: domain %d: Settlement's nextBatchId is %d but batch %d's entry is not visible at height %d", dom, p.next, id, p.h))
		return true, nil
	}
	want := entryExpect(id, e)

	if c.nVerified < len(c.batches) {
		if c.batches[c.nVerified].want == want {
			if err := c.indexAccept(id); err != nil {
				return false, err
			}
			c.mu.Lock()
			b := &c.batches[c.nVerified]
			b.finalizeAt = e.FinalizeAtHeight
			b.entryHeight = p.h
			c.nVerified++
			pw, ps := b.want, b.sends
			c.mu.Unlock()
			c.emit(evLatest, pw, ps)
			return false, nil
		}
		if err := c.retract(id, "Settlement's entry differs from the batch record's commitment"); err != nil {
			return false, err
		}
	}

	if rs, ok := c.cfg.Source.(*RemoteSource); ok {
		if err := rs.fetch(p.ctx, id, e.DaHash, e.ReceiptRoot, !p.accepted); err != nil {
			var cut *fetchCut
			if errors.As(err, &cut) && cut.kind == "bundle" && cut.first &&
				errors.Is(p.ctx.Err(), context.DeadlineExceeded) && !(c.cutLogged && c.cutBatch == id) {
				c.cutBatch, c.cutLogged = id, true
				c.cfg.Logf("gateway: domain %d batch %d: the bundle fetch from %s did not finish within the %s poll slice; a bundle this large needs a faster upstream", dom, id, redactEndpoint(rs.raw[cut.upstream]), pollSlice)
			}
			if errors.Is(err, errNoUpstream) {
				p.block(fmt.Sprintf("gateway: domain %d: batch %d: %v; status SYNCING", dom, id, err))
				return true, nil
			}
			return false, err
		}
	}

	bundle, err := c.cfg.Source.Bundle(e.DaHash)
	if err != nil {
		switch classifyRead(err) {
		case readMissing:
			p.block(fmt.Sprintf("gateway: domain %d: batch %d: bundle %s is not in the executor's directory yet; status SYNCING", dom, id, e.DaHash))
			return true, nil
		case readDenied:
			p.block(c.deniedMessage(err))
			return true, nil
		}
		return false, err
	}
	sidecar, err := c.cfg.Source.Receipts(e.DaHash)
	if err != nil {
		switch classifyRead(err) {
		case readMissing:
			p.block(fmt.Sprintf("gateway: domain %d: batch %d: receipt sidecar %s is missing; status SYNCING. A batch built before receipt sidecars needs the executor's domain directory re-synced: stop the executor, remove that domain directory and restart it", dom, id, e.DaHash))
			return true, nil
		case readDenied:
			p.block(c.deniedMessage(err))
			return true, nil
		}
		return false, err
	}

	ck, err := checkBatch(c.baseVersion(id), want, bundle, sidecar, math.MaxInt64)
	if err != nil {
		c.markInconsistent(id, err)
		return true, nil
	}
	if err := c.indexAdd(p, id, want, ck, true); err != nil {
		if errors.Is(err, errIndexConflict) || errors.Is(err, errNotDomainInput) {
			c.markInconsistent(id, err)
			return true, nil
		}
		return false, err
	}
	c.mu.Lock()
	sends := sendsOf(ck.inputs)
	c.batches = append(c.batches, batchState{want: want, version: ck.version, diff: ck.diff, finalizeAt: e.FinalizeAtHeight, entryHeight: p.h, held: ck.held, sends: sends})
	c.nVerified++
	c.mu.Unlock()
	c.emit(evLatest, want, sends)
	return false, nil
}

// finalize applies every verified batch whose finality height the
// confirmed height has reached to Final, re-reading each accepted
// entry once before it does.
func (c *Chain) finalize(p *poll) error {
	if p.finalizeHeld {
		return nil
	}
	for c.nVerified > 0 && p.h >= c.batches[0].finalizeAt {
		b := &c.batches[0]
		id := b.want.batchID
		if b.entryHeight != p.h {
			e, ok, err := p.view.BatchEntry(c.cfg.DomainID, id)
			if err != nil {
				return err
			}
			if !ok {
				p.finalizeHeld = true
				return nil
			}
			if entryExpect(id, e) != b.want {
				c.markInconsistent(id, errors.New("the accepted Settlement entry changed after the gateway verified it"))
				return nil
			}
			c.mu.Lock()
			b.finalizeAt = e.FinalizeAtHeight
			b.entryHeight = p.h
			c.mu.Unlock()
			if p.h < b.finalizeAt {
				return nil
			}
		}
		if err := c.indexFinalize(id); err != nil {
			return err
		}
		if _, err := c.cfg.Final.Apply(b.diff); err != nil {
			return fmt.Errorf("gateway: domain %d: finalize batch %d: %w", c.cfg.DomainID, id, err)
		}
		head := b.want.head()
		c.mu.Lock()
		c.finalized = &head
		c.finalVersion = b.version
		c.batches = c.batches[1:]
		c.nVerified--
		c.mu.Unlock()
		c.snapshotDue = true
		c.emit(evFinalized, b.want, b.sends)
	}
	return nil
}

// maxHeldBytes is the held-bytes budget: the most cost, as checkBatch
// counts it, that the pending batches above latest may hold. A
// typical full batch costs about 1.3 MiB, so 64 of them fit with
// room to spare.
const maxHeldBytes = 256 << 20

// heldEntryCost approximates the cost of one state diff entry beyond
// its value bytes: its diff slot, its leaf and the trie nodes its
// insert copies. Counting input data, which no held batch keeps,
// offsets the trie nodes it under-counts in a large state.
const heldEntryCost = 1 << 10

// errOverBudget reports a batch whose cost exceeds the limit
// checkBatch was given. It is not a failed check.
var errOverBudget = errors.New("the batch's decoded data exceeds the held-bytes budget")

// readTimeout bounds each znnd and Settlement read, so a node that
// never answers costs a turn an error instead of stalling it.
const readTimeout = 30 * time.Second

// errNoAnswer reports a znnd or Settlement read abandoned because its
// context ended before the node answered.
var errNoAnswer = errors.New("the znnd node did not answer before the read's deadline")

// snapshotInterval is the least time between two snapshot saves after
// the first save since boot, so a large state is not re-encoded on
// every finalized batch.
const snapshotInterval = 10 * time.Minute

// recordInterval is the most time the domain record the chain-info
// method reports goes without a re-read, so a runtime upgrade's new
// stfSpecHash shows within it.
const recordInterval = 10 * time.Minute

// senderRetryInterval is the least time between two checks of one
// pending record whose L1 sender read keeps failing. Accepted batches
// are not throttled.
const senderRetryInterval = time.Minute

// maxPendingBatches bounds the pending batches held above latest. A
// worker builds only a few batches ahead of Settlement, so the
// pending pass stops extending at this many without an error.
const maxPendingBatches = 64

// pending re-verifies the held pending batches against their records,
// then extends the pending chain from the executor's records.
func (c *Chain) pending(p *poll) error {
	for i := c.nVerified; i < len(c.batches); i++ {
		id := c.base() + uint64(i)
		rec, ok, err := c.cfg.Source.Record(id)
		if err != nil {
			if classifyRead(err) == readDenied {
				p.block(c.deniedMessage(err))
				return nil
			}
			return err
		}
		if !ok || rec.DAHash != c.batches[i].want.daHash || !recordHasReceiptRoot(rec, c.batches[i].want.receiptRoot) {
			if err := c.retract(id, "its batch record was removed or changed"); err != nil {
				return err
			}
			break
		}
	}

	for id := c.base() + uint64(len(c.batches)); len(c.batches)-c.nVerified < maxPendingBatches && c.pendingHeld() < c.heldBudget; id++ {
		if err := p.ctx.Err(); err != nil {
			return err
		}
		for k := range c.overlapWarned {
			if k != id {
				delete(c.overlapWarned, k)
			}
		}
		rec, ok, err := c.cfg.Source.Record(id)
		if err != nil {
			if classifyRead(err) == readDenied {
				p.block(c.deniedMessage(err))
				return nil
			}
			return err
		}
		if !ok {
			return nil
		}

		below := c.baseVersion(id).Root()
		if rec.PreStateRoot != below {
			if id == 0 {
				return c.failPending(p, id, rec.DAHash, errGenesisNotEmpty)
			}
			p.mismatch = &mismatchKey{batchID: id, daHash: rec.DAHash}
			p.recordPre, p.below = rec.PreStateRoot, below
			return nil
		}

		if lo, ok := c.lastInputSeqBelow(id); ok && rec.FirstInputSeq <= lo {
			if seen, warned := c.overlapWarned[id]; !warned || seen != lo {
				c.overlapWarned[id] = lo
				c.cfg.Logf("gateway: WARN: domain %d: batch %d's record (daHash %s) starts at input %d, not above input %d where the batch below it ends; pending stays below it until the executor rewrites its directory", c.cfg.DomainID, id, rec.DAHash, rec.FirstInputSeq, lo)
			}
			return nil
		}

		key := mismatchKey{batchID: id, daHash: rec.DAHash}
		if c.refused != nil && c.refused.key == key && c.pendingHeld() >= c.refused.pendingHeld {
			return nil
		}

		foreign := foreignRefusal{id: id, daHash: rec.DAHash, commitment: string(rec.Commitment)}
		if c.foreign != nil && *c.foreign == foreign {
			return nil
		}

		if w := c.senderWait; w != nil && w.key == foreign && c.now().Sub(w.at) < senderRetryInterval {
			return nil
		}

		bundle, err := c.cfg.Source.Bundle(rec.DAHash)
		if err == nil {
			var sidecar []byte
			if sidecar, err = c.cfg.Source.Receipts(rec.DAHash); err == nil {
				ck, want, err := c.extend(id, rec, bundle, sidecar, c.heldBudget-c.pendingHeld())
				if err != nil {
					if errors.Is(err, errOverBudget) {
						c.refused = &refusal{key: key, pendingHeld: c.pendingHeld()}
						return nil
					}
					return c.failPending(p, id, rec.DAHash, err)
				}
				if err := c.indexAdd(p, id, want, ck, false); err != nil {
					if errors.Is(err, errIndexConflict) {
						return nil
					}
					if errors.Is(err, errNotDomainInput) {
						c.foreign = &foreign
					} else if p.ctx.Err() == nil {
						c.senderWait = &senderBackoff{key: foreign, at: c.now()}
					}
					return err
				}
				c.senderWait = nil
				sends := sendsOf(ck.inputs)
				c.mu.Lock()
				c.batches = append(c.batches, batchState{want: want, version: ck.version, diff: ck.diff, held: ck.held, sends: sends})
				c.mu.Unlock()
				c.emit(evPending, want, sends)
				continue
			}
		}
		switch classifyRead(err) {
		case readMissing:
			return nil
		case readDenied:
			p.block(c.deniedMessage(err))
			return nil
		}
		return err
	}
	return nil
}

// pendingHeld returns the cost of the pending batches above latest.
func (c *Chain) pendingHeld() int64 {
	var n int64
	for _, b := range c.batches[c.nVerified:] {
		n += b.held
	}
	return n
}

// extend checks pending batch id's record commitment and data and
// returns the checked batch with the fields it matched. It returns
// errOverBudget unwrapped when the batch costs more than limit, and
// appends nothing.
func (c *Chain) extend(id uint64, rec recovery.BatchRecord, bundle, sidecar []byte, limit int64) (checked, expect, error) {
	cm, err := codec.DecodeBatchCommitment(rec.Commitment)
	if err != nil {
		return checked{}, expect{}, fmt.Errorf("batch record commitment does not decode: %v", err)
	}
	if field := c.commitmentMismatch(id, rec, cm); field != "" {
		return checked{}, expect{}, fmt.Errorf("batch record %s does not match its commitment", field)
	}
	want := commitmentExpect(cm)
	ck, err := checkBatch(c.baseVersion(id), want, bundle, sidecar, limit)
	if err != nil {
		return checked{}, expect{}, err
	}
	return ck, want, nil
}

// recordHasReceiptRoot reports whether rec's commitment decodes and
// carries receiptRoot.
func recordHasReceiptRoot(rec recovery.BatchRecord, receiptRoot types.Hash) bool {
	cm, err := codec.DecodeBatchCommitment(rec.Commitment)
	return err == nil && cm.ReceiptRoot == receiptRoot
}

// indexAdd writes batch id to the index, when the engine has one.
func (c *Chain) indexAdd(p *poll, id uint64, want expect, ck checked, accepted bool) error {
	if c.cfg.Index == nil {
		return nil
	}
	err := c.cfg.Index.add(p.ctx, indexBatch{
		domainID:     c.cfg.DomainID,
		id:           id,
		want:         want,
		preStateRoot: c.baseVersion(id).Root(),
		inputs:       ck.inputs,
	}, accepted)
	return c.indexErr(id, err)
}

// indexAccept marks batch id's manifest accepted.
func (c *Chain) indexAccept(id uint64) error {
	if c.cfg.Index == nil {
		return nil
	}
	return c.indexErr(id, c.cfg.Index.accept(id))
}

// indexFinalize drops batch id's manifest and raises the index's
// finalized batch.
func (c *Chain) indexFinalize(id uint64) error {
	if c.cfg.Index == nil {
		return nil
	}
	return c.indexErr(id, c.cfg.Index.finalize(id))
}

// indexRetract removes batch id and every indexed batch above it.
func (c *Chain) indexRetract(id uint64) error {
	if c.cfg.Index == nil {
		return nil
	}
	return c.indexErr(id, c.cfg.Index.retract(id))
}

// indexErr wraps an index error for batch id, keeping its sentinels
// reachable through errors.Is.
func (c *Chain) indexErr(id uint64, err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("gateway: domain %d: index batch %d: %w", c.cfg.DomainID, id, err)
}

// lastInputSeqBelow returns the lastInputSeq of the batch below id,
// and false for batch 0 at genesis.
func (c *Chain) lastInputSeqBelow(id uint64) (uint64, bool) {
	if id == c.base() {
		if c.finalized == nil {
			return 0, false
		}
		return c.finalized.LastInputSeq, true
	}
	return c.batches[id-c.base()-1].want.lastInputSeq, true
}

// commitmentMismatch names the first field of rec that differs from
// the commitment cm, or returns "" when none does.
func (c *Chain) commitmentMismatch(id uint64, rec recovery.BatchRecord, cm codec.BatchCommitment) string {
	switch {
	case cm.DomainID != c.cfg.DomainID:
		return "domainId"
	case cm.BatchID != id:
		return "batchId"
	case cm.DAHash != rec.DAHash:
		return "daHash"
	case cm.PreStateRoot != rec.PreStateRoot:
		return "preStateRoot"
	case cm.PostStateRoot != rec.PostStateRoot:
		return "postStateRoot"
	case cm.FirstInputSeq != rec.FirstInputSeq:
		return "firstInputSeq"
	case cm.LastInputSeq != rec.LastInputSeq:
		return "lastInputSeq"
	}
	return ""
}

// failPending handles a failed check on pending batch id. It re-reads
// the record first: a record that is gone or names another daHash was
// being rewritten, so the batch is dropped and the domain stays
// healthy. Otherwise the domain is INCONSISTENT.
func (c *Chain) failPending(p *poll, id uint64, daHash types.Hash, checkErr error) error {
	rec, ok, err := c.cfg.Source.Record(id)
	if err != nil {
		if classifyRead(err) == readDenied {
			p.block(c.deniedMessage(err))
			return nil
		}
		return err
	}
	if !ok || rec.DAHash != daHash {
		return nil
	}
	c.markInconsistent(id, checkErr)
	return nil
}

// Head returns the domain's tags and status.
func (c *Chain) Head() Head {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.headLocked()
}

// headLocked is Head for a caller that holds c.mu.
func (c *Chain) headLocked() Head {
	var h Head
	h.Status = c.status
	if c.status == StatusInconsistent {
		at := c.inconsistentAt
		h.InconsistentAt = &at
	}
	if c.finalized != nil {
		f := *c.finalized
		h.Finalized = &f
		h.Latest = &f
	}
	if c.nVerified > 0 {
		l := c.batches[c.nVerified-1].want.head()
		h.Latest = &l
	}
	h.Pending = h.Latest
	if len(c.batches) > c.nVerified {
		pd := c.batches[len(c.batches)-1].want.head()
		h.Pending = &pd
	}
	return h
}

// VersionAt returns the state after batchID, for the finalized batch
// and every batch above it through pending. ok is false for any other
// batchID.
func (c *Chain) VersionAt(batchID uint64) (state.Version, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.finalized != nil && batchID == c.finalized.BatchID {
		return c.finalVersion, true
	}
	if base := c.base(); batchID >= base && batchID-base < uint64(len(c.batches)) {
		return c.batches[batchID-base].version, true
	}
	return state.Version{}, false
}

// tagRead is one tag resolved against the head, read under one lock.
type tagRead struct {
	head Head
	// id is the batch the tag names; ok is false when no batch has
	// reached a named tag, or the batch id is above pending.
	id uint64
	ok bool
	// kept is set for the finalized batch and every batch above it:
	// root is then its postStateRoot and version its state.
	kept    bool
	root    types.Hash
	version state.Version
	// above is set for a batch above finalized: want then holds the
	// fields the engine checked it against.
	above bool
	want  expect
	// noPending is set when the tag is "pending" on a REMOTE domain,
	// which serves no pending batches.
	noPending bool
}

// readTag resolves t against the head and reads the resolved batch's
// state and checked fields under the same lock.
func (c *Chain) readTag(t tag) tagRead {
	c.mu.RLock()
	defer c.mu.RUnlock()
	r := tagRead{head: c.headLocked()}
	if t.name == "pending" && c.remote() {
		r.noPending = true
		return r
	}
	var b *BatchHead
	switch t.name {
	case "pending":
		b = r.head.Pending
	case "latest":
		b = r.head.Latest
	case "finalized":
		b = r.head.Finalized
	default:
		r.id = t.id
		r.ok = r.head.Pending != nil && t.id <= r.head.Pending.BatchID
	}
	if b != nil {
		r.id, r.ok = b.BatchID, true
	}
	if !r.ok {
		return r
	}
	if c.finalized != nil && r.id == c.finalized.BatchID {
		r.kept, r.root, r.version = true, c.finalized.PostStateRoot, c.finalVersion
	} else if base := c.base(); r.id >= base {
		bs := c.batches[r.id-base]
		r.kept, r.root, r.version = true, bs.want.postStateRoot, bs.version
		r.above, r.want = true, bs.want
	}
	return r
}

// errGenesisNotEmpty is the genesis failure: the gateway starts every
// domain at the empty state, so a domain whose genesis seeds leaves
// cannot match batch 0's pre-state or post-state root.
var errGenesisNotEmpty = errors.New("the domain's genesis state is not empty; the gateway serves only domains with an empty genesis")

// expect is what one batch's published data must match: an accepted
// batch's Settlement entry, or a pending batch's record commitment.
type expect struct {
	batchID, firstInputSeq, lastInputSeq uint64
	daHash, inputRoot, receiptRoot       types.Hash
	eventRoot, outboxRoot, postStateRoot types.Hash
}

// entryExpect returns the fields of e, the entry for batch id.
func entryExpect(id uint64, e *definition.BatchEntry) expect {
	return expect{
		batchID:       id,
		firstInputSeq: e.FirstInputSeq,
		lastInputSeq:  e.LastInputSeq,
		daHash:        e.DaHash,
		inputRoot:     e.InputRoot,
		receiptRoot:   e.ReceiptRoot,
		eventRoot:     e.EventRoot,
		outboxRoot:    e.OutboxRoot,
		postStateRoot: e.PostStateRoot,
	}
}

// commitmentExpect returns the fields of cm.
func commitmentExpect(cm codec.BatchCommitment) expect {
	return expect{
		batchID:       cm.BatchID,
		firstInputSeq: cm.FirstInputSeq,
		lastInputSeq:  cm.LastInputSeq,
		daHash:        cm.DAHash,
		inputRoot:     cm.InputRoot,
		receiptRoot:   cm.ReceiptRoot,
		eventRoot:     cm.EventRoot,
		outboxRoot:    cm.OutboxRoot,
		postStateRoot: cm.PostStateRoot,
	}
}

// head returns the batch head the tags serve for w.
func (w expect) head() BatchHead {
	return BatchHead{BatchID: w.batchID, LastInputSeq: w.lastInputSeq, PostStateRoot: w.postStateRoot}
}

// checked is one batch that passed every check.
type checked struct {
	version state.Version
	diff    []domain.StateDiffEntry
	// held is the batch's cost: its records' input data and execution
	// results, plus heldEntryCost per state diff entry.
	held int64
	// inputs holds each record's data for the index, in record order.
	// Poll hands it to the index and keeps none of it.
	inputs []batchInput
}

// batchInput is one record's data as the index stores it. Its slices
// alias the bundle and sidecar buffers, so nothing keeps it past the
// poll that checked it.
type batchInput struct {
	gidx      uint64
	inputData []byte
	receipt   codec.Receipt
	leaf      []byte // the receipt's canonical encoding
	result    codec.ExecutionResult
	events    [][]byte // each event's canonical encoding
	outbox    [][]byte // each outbox message's canonical encoding
	chunk     []byte   // a DeployContractChunk record's chunk bytes
}

// maxInputData returns the longest canonical input encoding of kind:
// input_kind(1) and source_account_block_hash(32), then the kind's
// tail, with a CallL2 payload at MaxCallPayload and DeployContractStart
// metadata at MaxContractMetadataSize, the limit Settlement enforces at
// receive. Any other kind has no tail.
func maxInputData(kind byte) int {
	const prefix = 1 + 32
	switch codec.InputKind(kind) {
	case codec.CallL2:
		// target_contract, plasma_limit, deposit_asset,
		// deposit_amount, payload
		return prefix + 32 + 8 + 32 + 32 + 4 + codec.MaxCallPayload
	case codec.Deposit:
		// target_contract, plasma_limit, deposit_asset,
		// deposit_amount
		return prefix + 32 + 8 + 32 + 32
	case codec.DeployStart:
		// code_hash, metadata_hash, total_size, chunk_count,
		// metadata
		return prefix + 32 + 32 + 8 + 4 + 4 + constants.MaxContractMetadataSize
	case codec.DeployChunk:
		// code_hash, chunk_index, chunk_hash
		return prefix + 32 + 4 + 32
	case codec.DeployFinalize:
		// code_hash
		return prefix + 32
	case codec.Upgrade:
		// contract_id, new_code_hash
		return prefix + 32 + 32
	case codec.TransferOwnership:
		// contract_id, new_owner
		return prefix + 32 + 20
	case codec.BuyStorage:
		// contract_id, bytes
		return prefix + 32 + 8
	}
	return prefix
}

// checkBatch runs the four batch checks, the receipt-to-record match
// and the commitment binding over one batch's bundle and sidecar,
// applying its StateDiffs to base. It bounds each record's input data
// by maxInputData, its deploy chunk data by MaxDeployChunkData and its
// execution result by MaxExecutionResult. Every error it returns is a
// failed check, except errOverBudget. It returns errOverBudget, after
// every check but check 3, when the batch costs more than limit. It
// also returns each record's data for the index.
func checkBatch(base state.Version, want expect, bundle, sidecar []byte, limit int64) (checked, error) {
	if !bytes.Equal(crypto.Hash(bundle), want.daHash[:]) {
		return checked{}, fmt.Errorf("check 1: bundle does not hash to daHash %s", want.daHash)
	}
	records, err := codec.DecodeBundle(bundle)
	if err != nil {
		return checked{}, fmt.Errorf("bundle does not decode: %v", err)
	}
	if len(records) == 0 {
		return checked{}, errors.New("bundle has no records")
	}
	if len(records) > codec.MaxBatchInputs {
		return checked{}, fmt.Errorf("bundle has %d records, above the limit of %d", len(records), codec.MaxBatchInputs)
	}
	receipts, leaves, err := codec.DecodeReceiptSidecar(sidecar)
	if err != nil {
		return checked{}, fmt.Errorf("receipt sidecar does not decode: %v", err)
	}
	if root := codec.MerkleRoot(leaves); root != want.receiptRoot {
		return checked{}, fmt.Errorf("check 2: sidecar root %s is not receiptRoot %s", root, want.receiptRoot)
	}
	if len(receipts) != len(records) {
		return checked{}, fmt.Errorf("sidecar has %d receipts for %d bundle records", len(receipts), len(records))
	}

	if first := records[0].GlobalInputIndex; first != want.firstInputSeq {
		return checked{}, fmt.Errorf("firstInputSeq %d, the bundle's first record is input %d", want.firstInputSeq, first)
	}
	if last := records[len(records)-1].GlobalInputIndex; last != want.lastInputSeq {
		return checked{}, fmt.Errorf("lastInputSeq %d, the bundle's last record is input %d", want.lastInputSeq, last)
	}
	inputs := make([][]byte, len(records))
	for i, rec := range records {
		inputs[i] = rec.InputData
	}
	if root := codec.MerkleRoot(inputs); root != want.inputRoot {
		return checked{}, fmt.Errorf("inputRoot %s is not the bundle's input root %s", want.inputRoot, root)
	}

	var diff []domain.StateDiffEntry
	var events, outbox [][]byte
	var held int64
	batchInputs := make([]batchInput, 0, len(records))
	for i, rec := range records {
		if len(rec.InputData) < 33 {
			return checked{}, fmt.Errorf("record %d: input data is %d bytes, shorter than its kind and send hash", i, len(rec.InputData))
		}
		if n, most := len(rec.InputData), maxInputData(rec.InputData[0]); n > most {
			return checked{}, fmt.Errorf("record %d: input data is %d bytes, longer than the %d bytes of the longest canonical input of kind %d", i, n, most, rec.InputData[0])
		}
		if n := len(rec.DeployChunkData); n > constants.MaxDeployChunkData {
			return checked{}, fmt.Errorf("record %d: deploy chunk data is %d bytes, longer than MaxDeployChunkData (%d bytes)", i, n, constants.MaxDeployChunkData)
		}
		if n := len(rec.ExecutionResult); n > constants.MaxExecutionResult {
			return checked{}, fmt.Errorf("record %d: execution result is %d bytes, longer than MaxExecutionResult (%d bytes)", i, n, constants.MaxExecutionResult)
		}
		r := receipts[i]
		if r.InputIndex != rec.GlobalInputIndex {
			return checked{}, fmt.Errorf("receipt %d: inputIndex %d, want %d", i, r.InputIndex, rec.GlobalInputIndex)
		}
		if wantHash := types.NewHash(rec.InputData); r.InputHash != wantHash {
			return checked{}, fmt.Errorf("receipt %d: inputHash %s, want %s", i, r.InputHash, wantHash)
		}
		if r.BatchID != want.batchID {
			return checked{}, fmt.Errorf("receipt %d: batchId %d, want %d", i, r.BatchID, want.batchID)
		}

		er, err := codec.DecodeExecutionResult(rec.ExecutionResult)
		if err != nil {
			return checked{}, fmt.Errorf("record %d: execution result does not decode: %v", i, err)
		}
		if er.GlobalInputIndex != rec.GlobalInputIndex {
			return checked{}, fmt.Errorf("record %d: execution result is for input %d", i, er.GlobalInputIndex)
		}

		evs := make([][]byte, len(er.Events))
		for j, ev := range er.Events {
			if evs[j], err = ev.Encode(); err != nil {
				return checked{}, fmt.Errorf("record %d: %v", i, err)
			}
		}
		outs := make([][]byte, len(er.Outbox))
		for j, m := range er.Outbox {
			if outs[j], err = m.Encode(); err != nil {
				return checked{}, fmt.Errorf("record %d: %v", i, err)
			}
		}
		sdh, err := codec.StateDiffHash(er.StateDiff)
		if err != nil {
			return checked{}, fmt.Errorf("record %d: %v", i, err)
		}
		fields := []struct {
			name      string
			got, want types.Hash
		}{
			{"stateDiffHash", sdh, r.StateDiffHash},
			{"returnHash", codec.ReturnHash(er.ReturnData), r.ReturnHash},
			{"eventRoot", codec.MerkleRoot(evs), r.EventRoot},
			{"outboxRoot", codec.MerkleRoot(outs), r.OutboxRoot},
		}
		for _, f := range fields {
			if f.got != f.want {
				return checked{}, fmt.Errorf("check 4: receipt %d %s does not match the bundle", i, f.name)
			}
		}
		if int(r.EventCount) != len(evs) {
			return checked{}, fmt.Errorf("receipt %d: eventCount %d, the bundle has %d events", i, r.EventCount, len(evs))
		}
		if int(r.OutboxCount) != len(outs) {
			return checked{}, fmt.Errorf("receipt %d: outboxCount %d, the bundle has %d outbox messages", i, r.OutboxCount, len(outs))
		}
		batchInputs = append(batchInputs, batchInput{gidx: rec.GlobalInputIndex, inputData: rec.InputData, receipt: r, leaf: leaves[i], result: er, events: evs, outbox: outs, chunk: rec.DeployChunkData})
		held += int64(len(rec.InputData) + len(rec.ExecutionResult) + len(er.StateDiff)*heldEntryCost)
		events = append(events, evs...)
		outbox = append(outbox, outs...)
		diff = append(diff, stateDiff(er.StateDiff)...)
	}

	if root := codec.MerkleRoot(events); root != want.eventRoot {
		return checked{}, fmt.Errorf("eventRoot %s is not the bundle's event root %s", want.eventRoot, root)
	}
	if root := codec.MerkleRoot(outbox); root != want.outboxRoot {
		return checked{}, fmt.Errorf("outboxRoot %s is not the bundle's outbox root %s", want.outboxRoot, root)
	}
	if held > limit {
		return checked{}, errOverBudget
	}
	v := base.Apply(diff)
	if root := v.Root(); root != want.postStateRoot {
		if want.batchID == 0 {
			return checked{}, errGenesisNotEmpty
		}
		return checked{}, fmt.Errorf("check 3: post-state root %s is not postStateRoot %s", root, want.postStateRoot)
	}
	return checked{version: v, diff: diff, held: held, inputs: batchInputs}, nil
}

// stateDiff returns entries as applier entries. It copies every
// non-nil value and keeps a zero-length value non-nil (present-empty),
// so no version holds a slice its caller can still write.
func stateDiff(entries []codec.StateDiffEntry) []domain.StateDiffEntry {
	out := make([]domain.StateDiffEntry, len(entries))
	for i, e := range entries {
		out[i] = domain.StateDiffEntry{Path: e.Key, Value: bytes.Clone(e.Value)}
	}
	return out
}
