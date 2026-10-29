package ledger

import (
	"bytes"
	"errors"
	"fmt"
	"sort"

	"github.com/zenon-network/go-zenon/chain/nom"
	"github.com/zenon-network/go-zenon/common/types"

	"github.com/Zeno-L2/zeno-executor/domain"
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
	// ConfirmationDepth is how many momenta behind the frontier are
	// treated as confirmed. It MUST be at least 1.
	ConfirmationDepth uint64
	// StartHeight is the first momentum height to read. Momentum heights are 1-based, so it
	// MUST be at least 1.
	StartHeight uint64
	// StartOrdinal is the domain ordinal of the first input Settlement
	// admits for DomainID at or above StartHeight: the next expected
	// ordinal. It is 0 for a fresh stream.
	//
	// Resume is momentum-granular: StartHeight is the momentum after
	// the resume point and StartOrdinal is that Checkpoint's
	// NextOrdinal. A batch boundary MAY fall inside a momentum, so a
	// resumed follower can re-emit inputs the caller has already
	// committed. The Follower does not detect or reconcile that:
	// dropping them belongs to the caller's checkpoint/resume logic.
	StartOrdinal uint64
	// MaxBufferedInputs bounds how many inputs one refill buffers. Once the buffer holds at
	// least this many, refill stops processing the current ranged read at that momentum
	// boundary and discards the rest of the read; the next refill reads those momenta again.
	// The buffer therefore holds fewer than MaxBufferedInputs inputs plus one momentum's
	// remainder, which bounds how long Checkpoint stays not ok under a saturated stream. Zero
	// means unbounded: a refill buffers a whole ranged read.
	MaxBufferedInputs int
	Source            MomentumSource
	// Assignments answers Settlement's assignment for every momentum whose content lists a
	// SettlementContract block.
	Assignments AssignmentSource
	// Caps answers Settlement's withdrawal caps. When set, every emitted input's Ctx.Caps is a
	// lookup bound to DomainID and the momentum whose assignment emitted it, shared and memoised
	// across that momentum's inputs. Nil leaves Ctx.Caps nil.
	Caps CapSource
}

// Input is one canonical input for the configured domain: the opaque payload plus the L1
// context, in exactly the shape domain.Domain.DecodeInput consumes.
type Input struct {
	Ctx     domain.InputContext
	Payload []byte
}

// Follower reads confirmed momenta and emits the configured domain's inputs as Settlement
// assigned them.
type Follower struct {
	cfg Config

	nextHeight uint64
	lastHeight uint64
	lastHash   types.Hash
	haveLast   bool

	// recent is the trailing window of consumed momenta, at most ConfirmationDepth+1 entries.
	recent []ConsumedMomentum

	buf []Input

	nextOrdinal uint64

	cursor    uint64
	cursorSet bool

	pendingReorg *ReorgError
	halted       error

	// limit is the read-ahead limit while limited is set (see LimitReadAhead).
	limit   uint64
	limited bool
	// held is the unprocessed rest of a ranged read that a limit stop left behind.
	held heldRead
}

// heldRead is the unprocessed rest of a ranged read: moms is the whole read, next indexes the
// first unprocessed momentum, lk is the read's linked record.
type heldRead struct {
	moms []*nom.DetailedMomentum
	next int
	lk   *linked
}

// LimitReadAhead sets the read-ahead limit when on is true and clears it otherwise. While it is
// set, a refill stops after the first momentum that leaves an input whose GlobalInputIndex is at
// or above last buffered, and keeps the rest of that ranged read for the next refill. Clearing
// it drops that kept rest, so the next refill reads from the first unprocessed height.
func (f *Follower) LimitReadAhead(last uint64, on bool) {
	f.limit = last
	f.limited = on
	if !on {
		f.held = heldRead{}
	}
}

// New validates cfg and returns a Follower seeded to start at cfg.StartHeight /
// cfg.StartOrdinal.
func New(cfg Config) (*Follower, error) {
	if cfg.Source == nil {
		return nil, errors.New("ledger: Config.Source is nil")
	}
	if cfg.Assignments == nil {
		return nil, errors.New("ledger: Config.Assignments is nil")
	}
	if cfg.ConfirmationDepth == 0 {
		return nil, errors.New("ledger: Config.ConfirmationDepth must be at least 1")
	}
	if cfg.StartHeight == 0 {
		return nil, errors.New("ledger: Config.StartHeight must be at least 1")
	}

	return &Follower{
		cfg:         cfg,
		nextHeight:  cfg.StartHeight,
		nextOrdinal: cfg.StartOrdinal,
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
// fresh read on the following call. A fault is permanent: validation, the link check, or an
// assignment fault (matching ErrAssignmentFault) is latched, and every later Next returns that
// same error without reading further. A read failure matches ErrSourceRead: it is not latched
// and nothing is advanced, so the caller may retry by calling Next again. A read failure is
// returned only with nothing buffered.
//
// An AssignmentSource error is a read failure that keeps its cause wrapped, so a cause wrapping
// recovery.ErrHalted is a fault the caller halts on. A send below the RecentMomenta window is
// reached through a hash-linked walk up to the oldest momentum this Follower holds, and one walk
// serves every send of the same ranged read: a walk that does not link is a read failure, and a
// send missing from the momentum its entry names is a fault.
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

// drainTerminal handles Next's terminal states against an empty buffer:
// the permanent latch, then the one-shot pending reorg.
func (f *Follower) drainTerminal() (Input, bool, error, bool) {
	if f.halted != nil {
		return Input{}, false, f.halted, true
	}
	if f.pendingReorg != nil {
		re := f.pendingReorg
		f.pendingReorg = nil
		return Input{}, false, re, true
	}
	return Input{}, false, nil, false
}

// LastEmittedIndex returns the globalInputIndex of the last input Next
// returned. ok is false before the first input. This is the follower's
// own read cursor; it is not lastInputSeq, the per-domain cursor
// Settlement stores when it accepts a batch, and nothing in this
// package commits or transmits it.
func (f *Follower) LastEmittedIndex() (index uint64, ok bool) {
	return f.cursor, f.cursorSet
}

// ConsumedMomentum is one momentum a Follower has fully walked, in the
// shape recovery's momentum_hashes/ window records it.
type ConsumedMomentum struct {
	Height uint64
	Hash   types.Hash
}

// RecentMomenta returns the momenta this Follower has most recently
// consumed, ascending by height and contiguous (a Follower consumes
// every confirmed momentum in order, including one holding nothing for
// its domain), at most ConfirmationDepth+1 entries. The caller persists
// it: a Follower's link check lives in memory and cannot see a reorg
// that landed while the process was down, and a window this long is
// exactly the depth within which a rollback is permitted.
func (f *Follower) RecentMomenta() []ConsumedMomentum {
	return append([]ConsumedMomentum(nil), f.recent...)
}

// Checkpoint is a resume point: a Follower built with StartHeight = Height+1 and
// StartOrdinal = NextOrdinal continues this Follower's stream exactly, re-emitting nothing and
// skipping nothing.
type Checkpoint struct {
	Height      uint64
	Hash        types.Hash
	NextOrdinal uint64
}

// Checkpoint reports the last momentum boundary this Follower has fully delivered. ok is false
// before the first momentum is consumed, and while inputs read but not yet returned by Next
// remain buffered: resuming past such a boundary would skip an input the caller never saw.
func (f *Follower) Checkpoint() (Checkpoint, bool) {
	if !f.haveLast || len(f.buf) != 0 {
		return Checkpoint{}, false
	}
	return Checkpoint{Height: f.lastHeight, Hash: f.lastHash, NextOrdinal: f.nextOrdinal}, true
}

// ErrSourceRead reports that a MomentumSource or AssignmentSource read
// failed, or that the source could not back an entry's send momentum.
// It is the machine-readable form of the unlatched error class Next
// documents: nothing advanced and nothing latched, so the caller may
// retry, as an unavailable znnd calls for.
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

// refill reads further confirmed momenta into buf until it has an input, hits the confirmed
// ceiling, or stops on a reorg or a momentum-processing failure. A read failure is returned
// directly, wrapped, with nothing advanced or latched; every other outcome is recorded into
// pendingReorg/halted and refill returns nil.
//
// A limit stop (see LimitReadAhead) keeps the unprocessed rest of its ranged read in f.held, and
// the next refill processes it before reading again. Every other return drops it: the held
// momenta are unprocessed, so nextHeight already names the first of them.
func (f *Follower) refill() error {
	held := f.held
	f.held = heldRead{}

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
		var moms []*nom.DetailedMomentum
		var lk *linked
		start := 0
		if held.moms != nil {
			moms, lk, start = held.moms, held.lk, held.next
			held = heldRead{}
		} else {
			remaining := ceiling - f.nextHeight + 1
			count := remaining
			if count > rpcMaxCountSize {
				count = rpcMaxCountSize
			}

			var err error
			moms, err = f.cfg.Source.DetailedMomentumsByHeight(f.nextHeight, count)
			if err != nil {
				return fmt.Errorf("%w: read momenta from height %d: %w", ErrSourceRead, f.nextHeight, err)
			}
			if len(moms) == 0 {
				return nil
			}

			lk = &linked{}
			if len(f.recent) > 0 {
				lk.low = f.recent[0].Height
				for _, c := range f.recent {
					lk.hashes = append(lk.hashes, c.Hash)
				}
			}
		}
		for i := start; i < len(moms); i++ {
			stop, ceilingExceeded, err := f.processMomentum(moms[i], ceiling, moms[:i], lk)
			if err != nil {
				// A read failure surfaces only with nothing buffered; otherwise the next
				// refill re-reads this momentum.
				if len(f.buf) > 0 {
					return nil
				}
				return err
			}
			if ceilingExceeded {
				// Not malformed content: the source returned a momentum
				// beyond what count asked for. Stop this call without
				// an error, the same as reaching the ceiling before
				// this momentum was ever read.
				return nil
			}
			if !stop && f.limited && len(f.buf) > 0 && f.buf[len(f.buf)-1].Ctx.GlobalInputIndex >= f.limit {
				if i+1 < len(moms) {
					f.held = heldRead{moms: moms, next: i + 1, lk: lk}
				}
				break
			}
			if stop || (f.cfg.MaxBufferedInputs > 0 && len(f.buf) >= f.cfg.MaxBufferedInputs) {
				break
			}
		}
	}
	return nil
}

// processMomentum validates dm, checks the link, cross-checks its
// account-blocks against its content, reads Settlement's assignment for
// it and commits the resulting inputs atomically. read holds the
// momenta of the same ranged read that precede dm, and lk is the read's
// linked record (see linked). It reports stop == true when the refill
// loop must not process any further momentum this call: either a
// *ReorgError landed in pendingReorg, or a fault landed in halted. A
// non-nil err is a read failure matching ErrSourceRead: nothing
// changed, and the caller retries. ceilingExceeded == true is a
// distinct, non-error outcome: the momentum's height is beyond what the
// requested window's count should ever have returned, so nothing here
// failed validation. The refill simply stops, exactly as if the ceiling
// had been reached before this entry was read.
func (f *Follower) processMomentum(dm *nom.DetailedMomentum, ceiling uint64, read []*nom.DetailedMomentum, lk *linked) (stop, ceilingExceeded bool, err error) {
	if dm == nil {
		f.halted = errors.New("ledger: momentum entry is nil")
		return true, false, nil
	}
	m := dm.Momentum
	if m == nil {
		f.halted = errors.New("ledger: momentum entry has a nil Momentum")
		return true, false, nil
	}
	for _, b := range dm.AccountBlocks {
		if b == nil {
			f.halted = fmt.Errorf("ledger: momentum at height %d has a nil account-block entry", m.Height)
			return true, false, nil
		}
	}
	if m.Height != f.nextHeight {
		f.halted = fmt.Errorf("ledger: momentum height %d does not match expected height %d", m.Height, f.nextHeight)
		return true, false, nil
	}
	if m.Height > ceiling {
		return true, true, nil
	}
	if f.haveLast && m.PreviousHash != f.lastHash {
		f.pendingReorg = &ReorgError{Height: m.Height, Want: f.lastHash, Got: m.PreviousHash}
		return true, false, nil
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
			return true, false, nil
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
			return true, false, nil
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
		return true, false, nil
	}

	inputs, err := f.assigned(dm, read, lk)
	if err != nil {
		if errors.Is(err, ErrSourceRead) {
			return true, false, err
		}
		f.halted = err
		return true, false, nil
	}

	f.buf = append(f.buf, inputs...)
	f.nextOrdinal += uint64(len(inputs))
	f.lastHeight = m.Height
	f.lastHash = m.Hash
	f.haveLast = true
	f.nextHeight = m.Height + 1
	f.recent = append(f.recent, ConsumedMomentum{Height: m.Height, Hash: m.Hash})
	if max := f.cfg.ConfirmationDepth + 1; uint64(len(f.recent)) > max {
		f.recent = append([]ConsumedMomentum(nil), f.recent[uint64(len(f.recent))-max:]...)
	}
	return false, false, nil
}

// assigned returns dm's inputs for the configured domain, in entry order. An error wrapping
// ErrSourceRead is a read failure; any other error is an assignment fault.
func (f *Follower) assigned(dm *nom.DetailedMomentum, read []*nom.DetailedMomentum, lk *linked) ([]Input, error) {
	m := dm.Momentum
	settles := false
	for _, h := range m.Content {
		if h.Address == types.SettlementContract {
			settles = true
			break
		}
	}
	if !settles {
		return nil, nil
	}

	entries, err := f.cfg.Assignments.Assignment(f.cfg.DomainID, m.Height, m.Hash, f.nextOrdinal)
	if err != nil {
		return nil, fmt.Errorf("%w: assignment at height %d: %w", ErrSourceRead, m.Height, err)
	}
	if len(entries) > 0 && entries[0].Ordinal != f.nextOrdinal {
		return nil, fmt.Errorf("%w: assignment at height %d starts at ordinal %d, want %d", ErrAssignmentFault, m.Height, entries[0].Ordinal, f.nextOrdinal)
	}

	for _, e := range entries {
		if e.SendMomentumHeight == 0 || e.SendMomentumHeight > m.Height {
			return nil, fmt.Errorf("%w: send momentum height %d is outside [1, %d]", ErrSourceRead, e.SendMomentumHeight, m.Height)
		}
	}

	top, topHash := m.Height, m.Hash
	if len(read) > 0 {
		top, topHash = read[0].Momentum.Height, read[0].Momentum.Hash
	}
	if len(lk.hashes) > 0 {
		top, topHash = lk.low, lk.hashes[0]
	}
	need := map[uint64]bool{}
	var lowest uint64
	for _, e := range entries {
		if h := e.SendMomentumHeight; h < top {
			need[h] = true
			if lowest == 0 || h < lowest {
				lowest = h
			}
		}
	}
	fetched := map[uint64]*nom.DetailedMomentum{}
	if len(need) > 0 {
		fetched, lk.hashes, err = f.walk(lowest, top, topHash, need, lk.hashes)
		if err != nil {
			return nil, err
		}
		lk.low = lowest
	}

	inputs := make([]Input, 0, len(entries))
	for _, e := range entries {
		sdm, err := f.sendMomentum(e.SendMomentumHeight, dm, read, lk, fetched)
		if err != nil {
			return nil, err
		}
		in, err := DecodeAssignedInput(f.cfg.DomainID, sdm, e.GlobalInputIndex, e.SourceAccountBlockHash)
		if err != nil {
			return nil, err
		}
		inputs = append(inputs, in)
	}
	if f.cfg.Caps != nil && len(inputs) > 0 {
		caps := &boundCaps{src: f.cfg.Caps, domainID: f.cfg.DomainID, height: m.Height, hash: m.Hash}
		for i := range inputs {
			inputs[i].Ctx.Caps = caps
		}
	}
	return inputs, nil
}

// linked holds the hashes of the momenta just below one ranged read that this Follower has
// hash-linked to the read's first momentum: hashes[i] is the hash at height low+i, and the
// last entry is the parent of the read's first momentum. refill seeds it from the
// RecentMomenta window when the read starts and drops it when the read ends, or keeps it with
// the held tail of the read; each walk extends it downward.
type linked struct {
	low    uint64
	hashes []types.Hash
}

// walk reads heights [lowest, top] from the source and checks that they form one hash chain
// whose momentum at top hashes to topHash, the held hash. It returns the walked momenta at the
// heights in need, and the hashes of heights [lowest, top-1] in height order followed by tail,
// in one slice allocated once. Every error wraps ErrSourceRead. The link proof rests on the
// source's hash verification, as processMomentum's link check does (see MomentumSource).
func (f *Follower) walk(lowest, top uint64, topHash types.Hash, need map[uint64]bool, tail []types.Hash) (fetched map[uint64]*nom.DetailedMomentum, below []types.Hash, err error) {
	fail := func(format string, args ...any) (map[uint64]*nom.DetailedMomentum, []types.Hash, error) {
		return nil, nil, fmt.Errorf("%w: "+format, append([]any{ErrSourceRead}, args...)...)
	}
	fetched = map[uint64]*nom.DetailedMomentum{}
	below = make([]types.Hash, 0, int(top-lowest)+len(tail))
	var prev types.Hash
	h := lowest
	for h <= top {
		count := min(top-h+1, uint64(rpcMaxCountSize))
		ms, err := f.cfg.Source.DetailedMomentumsByHeight(h, count)
		if err != nil {
			return fail("read momenta from height %d: %w", h, err)
		}
		if len(ms) == 0 {
			return fail("source returned no momentum at height %d", h)
		}
		for _, sdm := range ms {
			if h > top {
				break
			}
			if sdm == nil || sdm.Momentum == nil || sdm.Momentum.Height != h {
				return fail("source returned no momentum at height %d", h)
			}
			if h > lowest && sdm.Momentum.PreviousHash != prev {
				return fail("momentum at height %d does not link to the momentum below it", h)
			}
			if need[h] {
				if hasNilEntry(sdm) {
					return fail("momentum at height %d has a nil entry", h)
				}
				fetched[h] = sdm
			}
			if h < top {
				below = append(below, sdm.Momentum.Hash)
			}
			prev = sdm.Momentum.Hash
			h++
		}
	}
	if prev != topHash {
		return fail("momentum at height %d is %s, not the held %s", top, prev, topHash)
	}
	return fetched, append(below, tail...), nil
}

// sendMomentum returns the momentum at height sh, which assigned has checked is in [1, dm's
// height]: dm itself, a momentum of the same read, a momentum in fetched, or a momentum fetched
// from the source and matched against its hash in lk. Every momentum it returns is hash-linked
// to dm. Every error wraps ErrSourceRead.
func (f *Follower) sendMomentum(sh uint64, dm *nom.DetailedMomentum, read []*nom.DetailedMomentum, lk *linked, fetched map[uint64]*nom.DetailedMomentum) (*nom.DetailedMomentum, error) {
	fail := func(format string, args ...any) (*nom.DetailedMomentum, error) {
		return nil, fmt.Errorf("%w: "+format, append([]any{ErrSourceRead}, args...)...)
	}
	if sh == dm.Momentum.Height {
		return dm, nil
	}
	if len(read) > 0 && sh >= read[0].Momentum.Height {
		return read[sh-read[0].Momentum.Height], nil
	}
	if got, ok := fetched[sh]; ok {
		return got, nil
	}
	if sh < lk.low || sh-lk.low >= uint64(len(lk.hashes)) {
		return fail("no linked hash at height %d", sh)
	}
	want := lk.hashes[sh-lk.low]

	ms, err := f.cfg.Source.DetailedMomentumsByHeight(sh, 1)
	if err != nil {
		return fail("read momentum at height %d: %w", sh, err)
	}
	if len(ms) == 0 || ms[0] == nil || ms[0].Momentum == nil || ms[0].Momentum.Height != sh {
		return fail("source returned no momentum at height %d", sh)
	}
	if hasNilEntry(ms[0]) {
		return fail("momentum at height %d has a nil entry", sh)
	}
	if ms[0].Momentum.Hash != want {
		return fail("momentum at height %d is %s, not the linked %s", sh, ms[0].Momentum.Hash, want)
	}
	fetched[sh] = ms[0]
	return ms[0], nil
}

// hasNilEntry reports a nil Content or AccountBlocks entry, on which findSend would panic.
func hasNilEntry(dm *nom.DetailedMomentum) bool {
	for _, h := range dm.Momentum.Content {
		if h == nil {
			return true
		}
	}
	for _, b := range dm.AccountBlocks {
		if b == nil {
			return true
		}
	}
	return false
}
