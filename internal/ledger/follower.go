package ledger

import (
	"bytes"
	"errors"
	"fmt"
	"math"
	"sort"

	"github.com/zenon-network/go-zenon/chain/nom"
	"github.com/zenon-network/go-zenon/common/types"

	"github.com/digitalSloth/go-executor/domain"
)

// rpcMaxCountSize mirrors upstream's RpcMaxCountSize (rpc/api/utils.go:14), the largest
// window DetailedMomentumsByHeight is asked to serve in one call.
const rpcMaxCountSize = 1024

// MomentumSource is the confirmed-ledger read surface, derived from znnd's ledger RPC:
// FrontierHeight is the node's frontier momentum height, and DetailedMomentumsByHeight
// returns up to count momenta with their account-blocks, ascending from height.
//
// The Follower trusts its source for content integrity and performs no L1 validation. It
// cross-checks each momentum's walked account-blocks against m.Content, which detects a
// response inconsistent with itself, but it does not recompute m.Hash or any account-block's
// Hash against its Data, and it does not verify a producer signature — a source that forges
// an account-block's Data can equally forge that block's header, m.Content and m.Hash
// together, so this package gives no protection against a fully malicious or colluding
// source. An implementation MUST therefore obtain momenta from a node it trusts; verifying
// momenta against L1 belongs to the component that implements this interface.
type MomentumSource interface {
	FrontierHeight() (uint64, error)
	DetailedMomentumsByHeight(height, count uint64) ([]*nom.DetailedMomentum, error)
}

// Config is a Follower's boot configuration. Every field is validated by New.
type Config struct {
	// DomainID is the domain whose subsequence this follower emits.
	DomainID uint32
	// ConfirmationDepth is how many momenta behind the frontier are treated as confirmed
	// (SPEC §4.3). It MUST be at least 1.
	ConfirmationDepth uint64
	// StartHeight is the first momentum height to read. Momentum heights are 1-based, so it
	// MUST be at least 1.
	StartHeight uint64
	// StartGlobalIndex is the globalInputIndex the Follower assigns to the first block it
	// accepts at StartHeight. It is 0 for a fresh stream.
	//
	// Resume is expressed at momentum granularity only: the caller sets StartHeight to the
	// height of the momentum containing the resume point and StartGlobalIndex to the index of
	// that momentum's first accepted block — not to the index it wants emitted first. A batch
	// boundary MAY fall between two inputs of one momentum (SPEC §4.6), so a resumed follower
	// can re-emit inputs the caller has already committed, and re-walks every account-block of
	// the momentum at StartHeight. The Follower does not detect or reconcile that, and does not
	// validate the two fields against each other: dropping inputs at or below the committed
	// cursor belongs to the caller's checkpoint/resume logic, which this package does not
	// implement.
	StartGlobalIndex uint64
	// MaxBufferedInputs bounds how many inputs one refill buffers. Once the buffer holds at
	// least this many, refill stops processing the current ranged read at that momentum
	// boundary and discards the rest of the read; the next refill reads those momenta again.
	// The buffer therefore holds fewer than MaxBufferedInputs inputs plus one momentum's
	// remainder, which bounds how long Checkpoint stays not ok under a saturated stream. Zero
	// means unbounded: a refill buffers a whole ranged read.
	MaxBufferedInputs int
	Source            MomentumSource
	Filter            SettlementFilter
}

// Input is one canonical input for the configured domain: the opaque payload plus the L1
// context, in exactly the shape domain.Domain.DecodeInput consumes.
type Input struct {
	Ctx     domain.InputContext
	Payload []byte
}

// Follower reconstructs the canonical global input stream from confirmed momenta and emits
// the configured domain's subsequence.
type Follower struct {
	cfg Config

	nextHeight uint64
	lastHeight uint64
	lastHash   types.Hash
	haveLast   bool

	// recent is the trailing window of consumed momenta, at most ConfirmationDepth+1 entries.
	recent []ConsumedMomentum

	buf []Input

	counter indexCounter

	cursor    uint64
	cursorSet bool

	pendingReorg *ReorgError
	halted       error
}

// New validates cfg and returns a Follower seeded to start at cfg.StartHeight /
// cfg.StartGlobalIndex.
func New(cfg Config) (*Follower, error) {
	if cfg.Source == nil {
		return nil, errors.New("ledger: Config.Source is nil")
	}
	if cfg.Filter == nil {
		return nil, errors.New("ledger: Config.Filter is nil")
	}
	if cfg.ConfirmationDepth == 0 {
		return nil, errors.New("ledger: Config.ConfirmationDepth must be at least 1")
	}
	if cfg.StartHeight == 0 {
		return nil, errors.New("ledger: Config.StartHeight must be at least 1")
	}

	return &Follower{
		cfg:        cfg,
		nextHeight: cfg.StartHeight,
		counter:    indexCounter{next: cfg.StartGlobalIndex},
	}, nil
}

// Next returns the next input for the configured domain, reading further confirmed momenta as
// needed. ok is false with a nil error only when the follower has read every confirmed
// momentum: the next unread height is above the confirmed ceiling, or the source has no
// confirmed momenta to serve yet. It never means "the momenta just read held nothing for this
// domain" — Next keeps reading up to the ceiling until it has an input. On ok == false with a
// nil error the caller waits and calls again.
//
// Errors fall into three classes. A *ReorgError is returned once and then re-derived from a
// fresh read on the following call. An error from a momentum-processing failure — validation,
// the link check, Accept, or index overflow — is permanent: the Follower latches it and every
// later Next returns that same error without reading further. An error from a MomentumSource
// read call itself is wrapped and returned as-is, matching ErrSourceRead: it is not latched and
// nothing is advanced, so the caller may retry by calling Next again.
func (f *Follower) Next() (Input, bool, error) {
	if in, ok := f.pop(); ok {
		return in, true, nil
	}
	if in, ok, err, done := f.drainTerminal(); done {
		return in, ok, err
	}

	if err := f.refill(); err != nil {
		return Input{}, false, err
	}

	if in, ok := f.pop(); ok {
		return in, true, nil
	}
	if in, ok, err, done := f.drainTerminal(); done {
		return in, ok, err
	}
	return Input{}, false, nil
}

// pop returns the head of buf, setting cursor/cursorSet, when buf is non-empty.
func (f *Follower) pop() (Input, bool) {
	if len(f.buf) == 0 {
		return Input{}, false
	}
	in := f.buf[0]
	f.buf = f.buf[1:]
	f.cursor = in.Ctx.GlobalInputIndex
	f.cursorSet = true
	return in, true
}

// drainTerminal implements steps 2-3 of Next against an empty buffer: the two permanent
// latches, checked in fixed order, then the one-shot pending reorg.
func (f *Follower) drainTerminal() (Input, bool, error, bool) {
	if f.halted != nil {
		return Input{}, false, f.halted, true
	}
	if f.counter.exhausted {
		return Input{}, false, ErrIndexOverflow, true
	}
	if f.pendingReorg != nil {
		re := f.pendingReorg
		f.pendingReorg = nil
		return Input{}, false, re, true
	}
	return Input{}, false, nil, false
}

// LastEmittedIndex returns the globalInputIndex of the last input Next returned. ok is false
// before the first input. This is the follower's own read cursor; it is not SPEC's
// lastInputSeq, which is the per-domain cursor Settlement stores when it accepts a batch, and
// nothing in this package commits or transmits it.
func (f *Follower) LastEmittedIndex() (index uint64, ok bool) {
	return f.cursor, f.cursorSet
}

// ConsumedMomentum is one momentum a Follower has fully walked, in the shape EXECUTOR §16's
// momentum_hashes/ records it.
type ConsumedMomentum struct {
	Height uint64
	Hash   types.Hash
}

// RecentMomenta returns the momenta this Follower has most recently consumed, ascending by
// height and contiguous — a Follower consumes every confirmed momentum in order, including one
// holding nothing for its domain — at most ConfirmationDepth+1 entries. The caller persists it:
// a Follower's link check lives in memory and cannot see a reorg that landed while the process
// was down, and a window this long is exactly the depth within which §14 permits a rollback.
func (f *Follower) RecentMomenta() []ConsumedMomentum {
	return append([]ConsumedMomentum(nil), f.recent...)
}

// Checkpoint is a resume point: a Follower built with StartHeight = Height+1 and
// StartGlobalIndex = NextGlobalIndex continues this Follower's stream exactly, re-emitting
// nothing and skipping nothing.
type Checkpoint struct {
	Height          uint64
	Hash            types.Hash
	NextGlobalIndex uint64
}

// Checkpoint reports the last momentum boundary this Follower has fully delivered. ok is false
// before the first momentum is consumed, and while inputs read but not yet returned by Next
// remain buffered: resuming past such a boundary would skip an input the caller never saw.
func (f *Follower) Checkpoint() (Checkpoint, bool) {
	if !f.haveLast || len(f.buf) != 0 {
		return Checkpoint{}, false
	}
	return Checkpoint{Height: f.lastHeight, Hash: f.lastHash, NextGlobalIndex: f.counter.next}, true
}

// ErrSourceRead reports that a MomentumSource read call failed. It is the machine-readable form
// of the unlatched error class Next documents: nothing advanced and nothing latched, so the
// caller may retry, which EXECUTOR §15 requires for an unavailable znnd.
var ErrSourceRead = errors.New("ledger: momentum source read")

// ReorgError reports that the ledger no longer extends the last momentum this follower
// consumed: the momentum now at Height links back to Got where the follower consumed Want.
// Got is the zero Hash when the node no longer has any momentum at Height at all. It is a
// liveness signal that the source stopped extending the consumed chain, not an integrity
// check: Want and Got are both source-supplied hashes and are only as trustworthy as the
// source (see the MomentumSource doc comment).
type ReorgError struct {
	Height uint64
	Want   types.Hash
	Got    types.Hash
}

func (e *ReorgError) Error() string {
	return fmt.Sprintf("ledger: reorg at height %d: want %s, got %s", e.Height, e.Want, e.Got)
}

// ErrIndexOverflow reports that globalInputIndex has reached 2^64-1; admission halts rather
// than wrapping (SPEC §4.5). It is permanent: once returned, every later Next returns it.
var ErrIndexOverflow = errors.New("ledger: globalInputIndex exhausted")

// indexCounter keeps assignment testable alone and makes per-momentum atomicity a struct copy.
type indexCounter struct {
	next      uint64
	exhausted bool
}

// assign returns the next globalInputIndex and advances, or fails permanently once 2^64-1 has
// been assigned.
func (c *indexCounter) assign() (uint64, error) {
	if c.exhausted {
		return 0, ErrIndexOverflow
	}
	v := c.next
	if v == math.MaxUint64 {
		c.exhausted = true
	} else {
		c.next++
	}
	return v, nil
}

// refill reads further confirmed momenta into buf until it has an input, hits the confirmed
// ceiling, or stops on a reorg or a momentum-processing failure. A MomentumSource read error is
// returned directly, wrapped, with nothing advanced or latched; every other outcome is recorded
// into pendingReorg/halted/counter and refill returns nil.
func (f *Follower) refill() error {
	frontier, err := f.cfg.Source.FrontierHeight()
	if err != nil {
		return fmt.Errorf("%w: read frontier height: %w", ErrSourceRead, err)
	}

	if f.haveLast && frontier < f.lastHeight {
		f.pendingReorg = &ReorgError{Height: f.lastHeight, Want: f.lastHash, Got: types.Hash{}}
		return nil
	}
	if frontier < f.cfg.ConfirmationDepth {
		return nil
	}
	ceiling := frontier - f.cfg.ConfirmationDepth
	if ceiling < f.nextHeight {
		return nil
	}

	for f.pendingReorg == nil && f.halted == nil && len(f.buf) == 0 && f.nextHeight <= ceiling {
		remaining := ceiling - f.nextHeight + 1
		count := remaining
		if count > rpcMaxCountSize {
			count = rpcMaxCountSize
		}

		moms, err := f.cfg.Source.DetailedMomentumsByHeight(f.nextHeight, count)
		if err != nil {
			return fmt.Errorf("%w: read momenta from height %d: %w", ErrSourceRead, f.nextHeight, err)
		}
		if len(moms) == 0 {
			return nil
		}

		for _, dm := range moms {
			stop, ceilingExceeded := f.processMomentum(dm, ceiling)
			if ceilingExceeded {
				// Not malformed content: the source returned a momentum beyond what count
				// asked for. Stop this call without an error, the same as reaching the
				// ceiling before this momentum was ever read — see Edge cases.
				return nil
			}
			if stop || (f.cfg.MaxBufferedInputs > 0 && len(f.buf) >= f.cfg.MaxBufferedInputs) {
				break
			}
		}
	}
	return nil
}

// processMomentum validates dm, checks the link, orders and walks its account-blocks, and
// commits the result atomically against a scratch copy of counter. It reports stop == true when
// the refill loop must not process any further momentum this call: either a *ReorgError landed
// in pendingReorg, or a momentum-processing failure landed in halted. ceilingExceeded == true is
// a distinct, non-error outcome (see Edge cases): the momentum's height is beyond what the
// requested window's count should ever have returned, so nothing here failed validation — the
// refill simply stops, exactly as if the ceiling had been reached before this entry was read.
func (f *Follower) processMomentum(dm *nom.DetailedMomentum, ceiling uint64) (stop, ceilingExceeded bool) {
	if dm == nil {
		f.halted = errors.New("ledger: momentum entry is nil")
		return true, false
	}
	m := dm.Momentum
	if m == nil {
		f.halted = errors.New("ledger: momentum entry has a nil Momentum")
		return true, false
	}
	for _, b := range dm.AccountBlocks {
		if b == nil {
			f.halted = fmt.Errorf("ledger: momentum at height %d has a nil account-block entry", m.Height)
			return true, false
		}
	}
	if m.Height != f.nextHeight {
		f.halted = fmt.Errorf("ledger: momentum height %d does not match expected height %d", m.Height, f.nextHeight)
		return true, false
	}
	if m.Height > ceiling {
		return true, true
	}
	if f.haveLast && m.PreviousHash != f.lastHash {
		f.pendingReorg = &ReorgError{Height: m.Height, Want: f.lastHash, Got: m.PreviousHash}
		return true, false
	}

	type headeredBlock struct {
		block  *nom.AccountBlock
		header []byte
	}
	items := make([]headeredBlock, len(dm.AccountBlocks))
	for i, block := range dm.AccountBlocks {
		h := block.Header()
		items[i] = headeredBlock{block: block, header: h.Bytes()}
	}
	sort.Slice(items, func(i, j int) bool {
		return bytes.Compare(items[i].header, items[j].header) < 0
	})
	for i := 1; i < len(items); i++ {
		if bytes.Equal(items[i-1].header, items[i].header) {
			f.halted = fmt.Errorf("ledger: momentum at height %d has duplicate account-block header %x", m.Height, items[i].header)
			return true, false
		}
	}

	// Cross-check the walked headers against what the momentum's own content claims
	// (chain/nom/momentum_content.go:53's approach): m.Content is node-supplied like
	// dm.AccountBlocks, so its order is not trusted either — sort a copy the same way
	// before comparing. This catches a source inconsistent with its own stated content, not
	// a source that forges both consistently — see the MomentumSource doc comment above.
	for _, h := range m.Content {
		if h == nil {
			f.halted = fmt.Errorf("ledger: momentum at height %d has a nil content entry", m.Height)
			return true, false
		}
	}
	content := append(nom.MomentumContent(nil), m.Content...)
	sort.Slice(content, func(i, j int) bool {
		return bytes.Compare(content[i].Bytes(), content[j].Bytes()) < 0
	})
	var walkedBytes, contentBytes []byte
	for _, it := range items {
		walkedBytes = append(walkedBytes, it.header...)
	}
	for _, h := range content {
		contentBytes = append(contentBytes, h.Bytes()...)
	}
	if !bytes.Equal(walkedBytes, contentBytes) {
		f.halted = fmt.Errorf("ledger: momentum at height %d account-blocks do not match the committed content", m.Height)
		return true, false
	}

	blocks := make([]*nom.AccountBlock, len(items))
	for i, it := range items {
		blocks[i] = it.block
	}

	scratch := make([]Input, 0, len(blocks))
	counter := f.counter
	for _, block := range blocks {
		env, err := f.cfg.Filter.Accept(m, block)
		if err != nil {
			f.halted = fmt.Errorf("ledger: filter accept at height %d: %w", m.Height, err)
			return true, false
		}
		if env == nil {
			continue
		}

		idx, err := counter.assign()
		if err != nil {
			f.halted = err
			return true, false
		}
		f.counter.exhausted = counter.exhausted

		if env.DomainID != f.cfg.DomainID {
			continue
		}
		scratch = append(scratch, Input{
			Ctx: domain.InputContext{
				GlobalInputIndex: idx,
				MomentumHeight:   m.Height,
				MomentumTime:     m.TimestampUnix,
				L1Sender:         block.Address,
				SourceData:       env.SourceData,
			},
			Payload: env.Payload,
		})
	}

	f.buf = append(f.buf, scratch...)
	f.counter = counter
	f.lastHeight = m.Height
	f.lastHash = m.Hash
	f.haveLast = true
	f.nextHeight = m.Height + 1
	f.recent = append(f.recent, ConsumedMomentum{Height: m.Height, Hash: m.Hash})
	if max := f.cfg.ConfirmationDepth + 1; uint64(len(f.recent)) > max {
		f.recent = append([]ConsumedMomentum(nil), f.recent[uint64(len(f.recent))-max:]...)
	}
	return false, false
}
