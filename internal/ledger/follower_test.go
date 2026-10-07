package ledger

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/zenon-network/go-zenon/chain/nom"
	"github.com/zenon-network/go-zenon/common/types"
	"github.com/zenon-network/go-zenon/vm/embedded/definition"

	"github.com/digitalSloth/go-executor/internal/codec"
)

// --- fixtures ---

// fakeSource is a MomentumSource backed by a slice of momenta plus a settable frontier
// height. perCallCap, when non-zero, is the number of momenta DetailedMomentumsByHeight
// returns regardless of the requested count — below the requested count to exercise a short
// read, or above the confirmed ceiling's derived count to exercise a source that folds an
// unconfirmed momentum into an otherwise valid window. frontierErr and readErr, when set, are
// returned once by the next call to the matching method and then cleared.
type fakeSource struct {
	frontier    uint64
	moms        []*nom.DetailedMomentum
	perCallCap  uint64
	reads       int
	frontierErr error
	readErr     error
	// heights records the start height of every DetailedMomentumsByHeight call.
	heights []uint64
	// counts records the count of every DetailedMomentumsByHeight call.
	counts []uint64
	// fetchErr maps a start height to an error the next call starting there returns, once.
	fetchErr map[uint64]error
}

func (s *fakeSource) FrontierHeight() (uint64, error) {
	if s.frontierErr != nil {
		err := s.frontierErr
		s.frontierErr = nil
		return 0, err
	}
	return s.frontier, nil
}

func (s *fakeSource) DetailedMomentumsByHeight(height, count uint64) ([]*nom.DetailedMomentum, error) {
	s.reads++
	s.heights = append(s.heights, height)
	s.counts = append(s.counts, count)
	if err, ok := s.fetchErr[height]; ok {
		delete(s.fetchErr, height)
		return nil, err
	}
	if s.readErr != nil {
		err := s.readErr
		s.readErr = nil
		return nil, err
	}

	limit := count
	if s.perCallCap != 0 {
		limit = s.perCallCap
	}

	var out []*nom.DetailedMomentum
	for _, dm := range s.moms {
		if dm != nil && dm.Momentum != nil && dm.Momentum.Height < height {
			continue
		}
		if uint64(len(out)) >= limit {
			break
		}
		out = append(out, dm)
	}
	return out, nil
}

// sendBlock is newBlock whose Data is a packed CallL2 send for domainID, payload []byte{address}.
func sendBlock(address byte, height uint64, hash byte, domainID uint32) *nom.AccountBlock {
	data := definition.ABISettlement.PackMethodPanic(definition.SettlementCallL2MethodName, domainID, types.Hash{}, uint64(0), []byte{address})
	return newBlock(address, height, hash, data)
}

// receiveBlock is a Settlement contract block at height (Address types.SettlementContract,
// Hash testHash(0xFE)). Its presence makes the follower request the momentum's assignment.
func receiveBlock(height uint64) *nom.AccountBlock {
	return &nom.AccountBlock{Address: types.SettlementContract, Height: height, Hash: testHash(0xFE)}
}

type assignmentCall struct {
	height uint64
	hash   types.Hash
	next   uint64
}

var errFakeHalt = errors.New("fake: settlement halt")

// fakeAssignments answers Settlement's assignment over src. By default every top-level block
// whose Data unpacks as a CallL2 call is an accepted send, numbered in moms order and then
// AccountBlocks order: global indices count every domain's sends, ordinals count the
// requested domain's.
type fakeAssignments struct {
	src    *fakeSource
	base   uint64                                                                       // added to every global index
	accept func(b *nom.AccountBlock) bool                                               // nil accepts every send
	fn     func(height uint64, hash types.Hash, next uint64) ([]AssignmentEntry, error) // replaces the default
	err    error                                                                        // returned once by the next call, then cleared
	calls  []assignmentCall
}

func (a *fakeAssignments) Assignment(domainID uint32, height uint64, hash types.Hash, next uint64) ([]AssignmentEntry, error) {
	a.calls = append(a.calls, assignmentCall{height: height, hash: hash, next: next})
	if a.err != nil {
		err := a.err
		a.err = nil
		return nil, err
	}
	if a.fn != nil {
		return a.fn(height, hash, next)
	}

	var (
		entries []AssignmentEntry
		global  = a.base
		ordinal uint64
		found   bool
	)
	for _, dm := range a.src.moms {
		if dm == nil || dm.Momentum == nil {
			continue
		}
		h := dm.Momentum.Height
		if h > height {
			break
		}
		if h == height {
			if dm.Momentum.Hash != hash {
				break
			}
			found = true
			if next != ordinal {
				return nil, fmt.Errorf("%w: next ordinal %d, Settlement's is %d at height %d", errFakeHalt, next, ordinal, height)
			}
		}
		for _, b := range dm.AccountBlocks {
			if b == nil {
				continue
			}
			var p callL2Params
			if err := definition.ABISettlement.UnpackMethod(&p, definition.SettlementCallL2MethodName, b.Data); err != nil {
				continue
			}
			if a.accept != nil && !a.accept(b) {
				continue
			}
			if p.DomainId == domainID {
				if h == height {
					entries = append(entries, AssignmentEntry{Ordinal: ordinal, GlobalInputIndex: global, SourceAccountBlockHash: b.Hash, SendMomentumHeight: height})
				}
				ordinal++
			}
			global++
		}
	}
	if !found {
		return nil, fmt.Errorf("fake: no momentum at height %d with hash %s", height, hash)
	}
	return entries, nil
}

func testAddr(b byte) types.Address {
	var a types.Address
	a[0] = b
	return a
}

func testHash(b byte) types.Hash {
	var h types.Hash
	h[0] = b
	return h
}

func momHash(height uint64) types.Hash {
	var h types.Hash
	binary.BigEndian.PutUint64(h[:8], height)
	return h
}

func newBlock(address byte, height uint64, hash byte, data []byte) *nom.AccountBlock {
	return &nom.AccountBlock{
		Address: testAddr(address),
		Height:  height,
		Hash:    testHash(hash),
		Data:    data,
	}
}

func newMomentum(height uint64, prev types.Hash, blocks []*nom.AccountBlock) *nom.DetailedMomentum {
	// Content commits the same headers a real momentum's hash would, so the follower's
	// cross-check against m.Content passes for fixtures the tests build as valid. Filter
	// out nils so fixtures exercising the nil-account-block case don't panic here — the
	// follower rejects those before it would ever read Content.
	present := make([]*nom.AccountBlock, 0, len(blocks))
	for _, b := range blocks {
		if b != nil {
			present = append(present, b)
		}
	}
	return &nom.DetailedMomentum{
		Momentum: &nom.Momentum{
			Height:        height,
			Hash:          momHash(height),
			PreviousHash:  prev,
			TimestampUnix: 1000 + height,
			Content:       nom.NewMomentumContent(present),
		},
		AccountBlocks: blocks,
	}
}

// chain builds n linked momenta starting at startHeight, each with the blocks blocksFor
// returns for its height.
func chain(startHeight uint64, n int, blocksFor func(height uint64) []*nom.AccountBlock) []*nom.DetailedMomentum {
	out := make([]*nom.DetailedMomentum, 0, n)
	prev := types.Hash{}
	if startHeight > 1 {
		prev = momHash(startHeight - 1)
	}
	for i := 0; i < n; i++ {
		h := startHeight + uint64(i)
		out = append(out, newMomentum(h, prev, blocksFor(h)))
		prev = momHash(h)
	}
	return out
}

func drain(t *testing.T, f *Follower) []Input {
	t.Helper()
	var out []Input
	for {
		in, ok, err := f.Next()
		if err != nil {
			t.Fatalf("Next: %v", err)
		}
		if !ok {
			return out
		}
		out = append(out, in)
	}
}

// --- tests ---

func TestNextDeterminismAndOrdering(t *testing.T) {
	// Non-canonical AccountBlocks order: addresses 3, 1, 2.
	blocks := []*nom.AccountBlock{
		sendBlock(3, 1, 1, 1),
		sendBlock(1, 1, 3, 1),
		sendBlock(2, 1, 2, 1),
		receiveBlock(1),
	}
	// Settlement's assignment names the sends in the order 2, 3, 1, which is neither the
	// supplied order nor the header order.
	assign := func(height uint64, hash types.Hash, next uint64) ([]AssignmentEntry, error) {
		return []AssignmentEntry{
			{Ordinal: 0, GlobalInputIndex: 4, SourceAccountBlockHash: testHash(2), SendMomentumHeight: 1},
			{Ordinal: 1, GlobalInputIndex: 7, SourceAccountBlockHash: testHash(1), SendMomentumHeight: 1},
			{Ordinal: 2, GlobalInputIndex: 9, SourceAccountBlockHash: testHash(3), SendMomentumHeight: 1},
		}, nil
	}
	supplied := append([]*nom.AccountBlock(nil), blocks...)

	build := func() *fakeSource {
		moms := chain(1, 1, func(h uint64) []*nom.AccountBlock { return blocks })
		return &fakeSource{frontier: 2, moms: moms}
	}

	run := func() []Input {
		f, err := New(Config{DomainID: 1, ConfirmationDepth: 1, StartHeight: 1, Source: build(), Assignments: &fakeAssignments{fn: assign}})
		if err != nil {
			t.Fatalf("New: %v", err)
		}
		return drain(t, f)
	}

	got1 := run()
	got2 := run()

	wantAddrs := []byte{2, 3, 1}
	wantIndices := []uint64{4, 7, 9}
	if len(got1) != len(wantAddrs) {
		t.Fatalf("got %d inputs, want %d", len(got1), len(wantAddrs))
	}
	for i, want := range wantAddrs {
		if got1[i].Ctx.L1Sender != testAddr(want) {
			t.Fatalf("input %d: got sender %s, want address %d", i, got1[i].Ctx.L1Sender, want)
		}
		if got1[i].Ctx.GlobalInputIndex != wantIndices[i] {
			t.Fatalf("input %d: got index %d, want %d", i, got1[i].Ctx.GlobalInputIndex, wantIndices[i])
		}
	}
	if len(got2) != len(got1) {
		t.Fatalf("second run: got %d inputs, want %d", len(got2), len(got1))
	}
	for i := range got1 {
		a, b := got1[i], got2[i]
		same := a.Ctx.GlobalInputIndex == b.Ctx.GlobalInputIndex &&
			a.Ctx.MomentumHeight == b.Ctx.MomentumHeight &&
			a.Ctx.MomentumTime == b.Ctx.MomentumTime &&
			a.Ctx.L1Sender == b.Ctx.L1Sender &&
			bytes.Equal(a.Ctx.SourceData, b.Ctx.SourceData) &&
			bytes.Equal(a.Payload, b.Payload)
		if !same {
			t.Fatalf("input %d: runs differ: %+v vs %+v", i, a, b)
		}
	}

	for i, b := range supplied {
		if blocks[i] != b {
			t.Fatalf("fixture AccountBlocks order mutated at %d", i)
		}
	}
}

func TestNextPerDomainJumps(t *testing.T) {
	// Five sends, addresses ascending so header order equals supplied order, alternating
	// domain A (0), B (1), A, B, A.
	blocks := []*nom.AccountBlock{
		sendBlock(1, 1, 1, 0),
		sendBlock(2, 1, 2, 1),
		sendBlock(3, 1, 3, 0),
		sendBlock(4, 1, 4, 1),
		sendBlock(5, 1, 5, 0),
		receiveBlock(1),
	}
	moms := chain(1, 1, func(h uint64) []*nom.AccountBlock { return blocks })
	source := &fakeSource{frontier: 2, moms: moms}

	f, err := New(Config{DomainID: 0, ConfirmationDepth: 1, StartHeight: 1, Source: source, Assignments: &fakeAssignments{src: source, base: 5}})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	wantIndices := []uint64{5, 7, 9}
	for i, want := range wantIndices {
		in, ok, err := f.Next()
		if err != nil || !ok {
			t.Fatalf("Next %d: got (ok=%v, err=%v), want an input", i, ok, err)
		}
		if in.Ctx.GlobalInputIndex != want {
			t.Fatalf("Next %d: got index %d, want %d", i, in.Ctx.GlobalInputIndex, want)
		}
		idx, ok := f.LastEmittedIndex()
		if !ok || idx != want {
			t.Fatalf("LastEmittedIndex after Next %d: got (%d, %v), want (%d, true)", i, idx, ok, want)
		}
	}
	if _, ok, err := f.Next(); ok || err != nil {
		t.Fatalf("Next after drain: got (ok=%v, err=%v), want (false, nil)", ok, err)
	}
	if cp, ok := f.Checkpoint(); !ok || cp.NextOrdinal != 3 {
		t.Fatalf("Checkpoint = (%+v, %v), want NextOrdinal 3", cp, ok)
	}
}

func TestNextConfirmationFrontier(t *testing.T) {
	oneBlock := func(h uint64) []*nom.AccountBlock {
		return []*nom.AccountBlock{sendBlock(1, h, 1, 1), receiveBlock(h)}
	}

	t.Run("well-behaved source", func(t *testing.T) {
		moms := chain(1, 10, oneBlock)
		source := &fakeSource{frontier: 10, moms: moms}
		f, err := New(Config{DomainID: 1, ConfirmationDepth: 3, StartHeight: 1, Source: source, Assignments: &fakeAssignments{src: source}})
		if err != nil {
			t.Fatalf("New: %v", err)
		}
		got := drain(t, f)
		if len(got) != 7 {
			t.Fatalf("got %d inputs, want 7", len(got))
		}
		for i, in := range got {
			if in.Ctx.MomentumHeight != uint64(i+1) {
				t.Fatalf("input %d: got height %d, want %d", i, in.Ctx.MomentumHeight, i+1)
			}
		}
	})

	t.Run("source folds unconfirmed momentum into window", func(t *testing.T) {
		moms := chain(1, 10, oneBlock)
		source := &fakeSource{frontier: 10, moms: moms, perCallCap: 8}
		f, err := New(Config{DomainID: 1, ConfirmationDepth: 3, StartHeight: 1, Source: source, Assignments: &fakeAssignments{src: source}})
		if err != nil {
			t.Fatalf("New: %v", err)
		}
		for i := 0; i < 7; i++ {
			in, ok, err := f.Next()
			if err != nil || !ok {
				t.Fatalf("Next %d: got (ok=%v, err=%v), want an input", i, ok, err)
			}
			if in.Ctx.MomentumHeight != uint64(i+1) {
				t.Fatalf("Next %d: got height %d, want %d", i, in.Ctx.MomentumHeight, i+1)
			}
		}
		// The eighth momentum is beyond what the requested window's count should ever
		// have returned — a source protocol slip, not malformed content — so the refill
		// stops silently rather than erroring (spec.md Edge cases).
		if _, ok, err := f.Next(); ok || err != nil {
			t.Fatalf("Next after drain: got (ok=%v, err=%v), want (false, nil)", ok, err)
		}
	})
}

func TestNextRejectedBlockAndReorg(t *testing.T) {
	m1blocks := []*nom.AccountBlock{
		sendBlock(1, 1, 1, 1), // rejected
		sendBlock(2, 1, 2, 1), // accepted
		receiveBlock(1),
	}
	m2blocks := []*nom.AccountBlock{sendBlock(1, 2, 1, 1), receiveBlock(2)}
	m3blocks := []*nom.AccountBlock{sendBlock(1, 3, 1, 1), receiveBlock(3)}

	moms := []*nom.DetailedMomentum{
		newMomentum(1, types.Hash{}, m1blocks),
		newMomentum(2, momHash(1), m2blocks),
		newMomentum(3, testHash(0xAA), m3blocks), // wrong PreviousHash: reorg
	}
	source := &fakeSource{frontier: 5, moms: moms}
	assignments := &fakeAssignments{src: source, accept: func(b *nom.AccountBlock) bool {
		return !(b.Address == testAddr(1) && b.Height == 1)
	}}

	f, err := New(Config{DomainID: 1, ConfirmationDepth: 1, StartHeight: 1, Source: source, Assignments: assignments})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	in1, ok, err := f.Next()
	if err != nil || !ok || in1.Ctx.GlobalInputIndex != 0 {
		t.Fatalf("Next 1: got (idx=%d, ok=%v, err=%v), want (0, true, nil)", in1.Ctx.GlobalInputIndex, ok, err)
	}
	in2, ok, err := f.Next()
	if err != nil || !ok || in2.Ctx.GlobalInputIndex != 1 {
		t.Fatalf("Next 2: got (idx=%d, ok=%v, err=%v), want (1, true, nil)", in2.Ctx.GlobalInputIndex, ok, err)
	}

	_, ok, err = f.Next()
	if ok || err == nil {
		t.Fatalf("Next 3: got (ok=%v, err=%v), want a *ReorgError", ok, err)
	}
	var reorg *ReorgError
	if !errors.As(err, &reorg) {
		t.Fatalf("Next 3: got %v, want a *ReorgError", err)
	}
	if reorg.Height != 3 || reorg.Want != momHash(2) || reorg.Got != testHash(0xAA) {
		t.Fatalf("Next 3: got %+v, want {Height:3 Want:%v Got:%v}", reorg, momHash(2), testHash(0xAA))
	}
}

func TestNextSparseDomains(t *testing.T) {
	blocksFor := func(h uint64) []*nom.AccountBlock {
		if h == 3 {
			return []*nom.AccountBlock{sendBlock(1, h, 1, 1), receiveBlock(h)}
		}
		return []*nom.AccountBlock{sendBlock(1, h, 1, 99), sendBlock(2, h, 2, 99), receiveBlock(h)}
	}
	moms := chain(1, 3, blocksFor)
	source := &fakeSource{frontier: 4, moms: moms}

	f, err := New(Config{DomainID: 1, ConfirmationDepth: 1, StartHeight: 1, Source: source, Assignments: &fakeAssignments{src: source}})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	in, ok, err := f.Next()
	if err != nil || !ok {
		t.Fatalf("Next: got (ok=%v, err=%v), want an input", ok, err)
	}
	if in.Ctx.MomentumHeight != 3 {
		t.Fatalf("got momentum height %d, want 3", in.Ctx.MomentumHeight)
	}
}

func TestNextDrainThenError(t *testing.T) {
	m1blocks := []*nom.AccountBlock{sendBlock(1, 1, 1, 1), sendBlock(2, 1, 2, 1), receiveBlock(1)}
	m2blocks := []*nom.AccountBlock{sendBlock(1, 2, 1, 1), receiveBlock(2)}
	moms := []*nom.DetailedMomentum{
		newMomentum(1, types.Hash{}, m1blocks),
		newMomentum(2, testHash(0xAA), m2blocks), // wrong PreviousHash
	}
	source := &fakeSource{frontier: 3, moms: moms}

	f, err := New(Config{DomainID: 1, ConfirmationDepth: 1, StartHeight: 1, Source: source, Assignments: &fakeAssignments{src: source}})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	if _, ok, err := f.Next(); !ok || err != nil {
		t.Fatalf("Next 1: got (ok=%v, err=%v), want an input", ok, err)
	}
	if _, ok, err := f.Next(); !ok || err != nil {
		t.Fatalf("Next 2: got (ok=%v, err=%v), want an input", ok, err)
	}

	// Next 3 returns the reorg detected while draining the window in Next 1's refill — the
	// pendingReorg latch was already computed, so no new read happens here.
	readsBefore := source.reads
	_, ok, err := f.Next()
	if ok || err == nil {
		t.Fatalf("Next 3: got (ok=%v, err=%v), want a *ReorgError", ok, err)
	}
	var first *ReorgError
	if !errors.As(err, &first) {
		t.Fatalf("Next 3: got %v, want a *ReorgError", err)
	}
	if source.reads != readsBefore {
		t.Fatalf("Next 3: source was read again, want no read")
	}

	// Next 4 re-derives the reorg from a fresh read, since pendingReorg was cleared by Next 3.
	readsBefore = source.reads
	_, ok, err = f.Next()
	if ok || err == nil {
		t.Fatalf("Next 4: got (ok=%v, err=%v), want a *ReorgError", ok, err)
	}
	var second *ReorgError
	if !errors.As(err, &second) {
		t.Fatalf("Next 4: got %v, want a *ReorgError", err)
	}
	if *first != *second {
		t.Fatalf("Next 4: got %+v, want the same reorg as Next 3 (%+v)", second, first)
	}
	if source.reads == readsBefore {
		t.Fatalf("Next 4: source was not re-read")
	}
}

func TestNextShortRead(t *testing.T) {
	oneBlock := func(h uint64) []*nom.AccountBlock {
		return []*nom.AccountBlock{sendBlock(1, h, 1, 1), receiveBlock(h)}
	}
	moms := chain(1, 5, oneBlock)
	source := &fakeSource{frontier: 6, moms: moms, perCallCap: 2}

	f, err := New(Config{DomainID: 1, ConfirmationDepth: 1, StartHeight: 1, Source: source, Assignments: &fakeAssignments{src: source}})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	got := drain(t, f)
	if len(got) != 5 {
		t.Fatalf("got %d inputs, want 5", len(got))
	}
	for i, in := range got {
		if in.Ctx.GlobalInputIndex != uint64(i) || in.Ctx.MomentumHeight != uint64(i+1) {
			t.Fatalf("input %d: got (idx=%d, height=%d), want (%d, %d)", i, in.Ctx.GlobalInputIndex, in.Ctx.MomentumHeight, i, i+1)
		}
	}
	if source.reads < 3 {
		t.Fatalf("got %d reads, want at least 3 for a capped-at-2 short read over 5 momenta", source.reads)
	}
}

func TestNextValidationErrors(t *testing.T) {
	cases := []struct {
		name string
		moms []*nom.DetailedMomentum
	}{
		{
			name: "height not nextHeight",
			moms: []*nom.DetailedMomentum{newMomentum(5, types.Hash{}, nil)},
		},
		{
			name: "nil DetailedMomentum",
			moms: []*nom.DetailedMomentum{nil},
		},
		{
			name: "nil Momentum",
			moms: []*nom.DetailedMomentum{{Momentum: nil, AccountBlocks: nil}},
		},
		{
			name: "nil account-block entry",
			moms: []*nom.DetailedMomentum{newMomentum(1, types.Hash{}, []*nom.AccountBlock{nil})},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			source := &fakeSource{frontier: 5, moms: tc.moms}
			f, err := New(Config{DomainID: 1, ConfirmationDepth: 1, StartHeight: 1, Source: source, Assignments: &fakeAssignments{src: source}})
			if err != nil {
				t.Fatalf("New: %v", err)
			}
			if _, ok, err := f.Next(); ok || err == nil {
				t.Fatalf("Next: got (ok=%v, err=%v), want an error", ok, err)
			}
		})
	}
}

func TestNextEmptyMomentum(t *testing.T) {
	moms := []*nom.DetailedMomentum{
		newMomentum(1, types.Hash{}, nil),
		newMomentum(2, momHash(1), []*nom.AccountBlock{sendBlock(1, 2, 1, 1), receiveBlock(2)}),
	}
	source := &fakeSource{frontier: 3, moms: moms}

	f, err := New(Config{DomainID: 1, ConfirmationDepth: 1, StartHeight: 1, Source: source, Assignments: &fakeAssignments{src: source}})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	in, ok, err := f.Next()
	if err != nil || !ok {
		t.Fatalf("Next: got (ok=%v, err=%v), want an input", ok, err)
	}
	if in.Ctx.MomentumHeight != 2 {
		t.Fatalf("got momentum height %d, want 2", in.Ctx.MomentumHeight)
	}
}

func TestNewValidation(t *testing.T) {
	valid := Config{DomainID: 1, ConfirmationDepth: 1, StartHeight: 1, Source: &fakeSource{}, Assignments: &fakeAssignments{}}

	cases := []struct {
		name string
		cfg  Config
	}{
		{"nil Source", func() Config { c := valid; c.Source = nil; return c }()},
		{"nil Assignments", func() Config { c := valid; c.Assignments = nil; return c }()},
		{"zero ConfirmationDepth", func() Config { c := valid; c.ConfirmationDepth = 0; return c }()},
		{"zero StartHeight", func() Config { c := valid; c.StartHeight = 0; return c }()},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := New(tc.cfg); err == nil {
				t.Fatalf("New: got nil error, want an error")
			}
		})
	}
}

func TestNextShrinkingFrontier(t *testing.T) {
	oneBlock := func(h uint64) []*nom.AccountBlock {
		return []*nom.AccountBlock{sendBlock(1, h, 1, 1), receiveBlock(h)}
	}
	moms := chain(1, 5, oneBlock)
	source := &fakeSource{frontier: 6, moms: moms}

	f, err := New(Config{DomainID: 1, ConfirmationDepth: 1, StartHeight: 1, Source: source, Assignments: &fakeAssignments{src: source}})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	got := drain(t, f)
	if len(got) != 5 {
		t.Fatalf("got %d inputs, want 5", len(got))
	}

	source.frontier = 3
	_, ok, err := f.Next()
	if ok || err == nil {
		t.Fatalf("Next after shrinking frontier: got (ok=%v, err=%v), want a *ReorgError", ok, err)
	}
	var reorg *ReorgError
	if !errors.As(err, &reorg) {
		t.Fatalf("got %v, want a *ReorgError", err)
	}
	if reorg.Height != 5 || reorg.Want != momHash(5) || reorg.Got != (types.Hash{}) {
		t.Fatalf("got %+v, want {Height:5 Want:%v Got:zero}", reorg, momHash(5))
	}
}

func TestNextAssignmentFaultLatches(t *testing.T) {
	blocks := []*nom.AccountBlock{
		sendBlock(1, 1, 1, 1),
		sendBlock(2, 1, 2, 1),
		sendBlock(3, 1, 3, 2),
		receiveBlock(1),
	}
	moms := []*nom.DetailedMomentum{newMomentum(1, types.Hash{}, blocks)}
	source := &fakeSource{frontier: 2, moms: moms}

	// Settlement names all three sends, including the one for domain 2.
	assignments := &fakeAssignments{fn: func(height uint64, hash types.Hash, next uint64) ([]AssignmentEntry, error) {
		return []AssignmentEntry{
			{Ordinal: 0, GlobalInputIndex: 0, SourceAccountBlockHash: testHash(1), SendMomentumHeight: 1},
			{Ordinal: 1, GlobalInputIndex: 1, SourceAccountBlockHash: testHash(2), SendMomentumHeight: 1},
			{Ordinal: 2, GlobalInputIndex: 2, SourceAccountBlockHash: testHash(3), SendMomentumHeight: 1},
		}, nil
	}}

	f, err := New(Config{DomainID: 1, ConfirmationDepth: 1, StartHeight: 1, Source: source, Assignments: assignments})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	_, ok, err := f.Next()
	if ok || !errors.Is(err, ErrAssignmentFault) {
		t.Fatalf("Next 1: got (ok=%v, err=%v), want (false, ErrAssignmentFault)", ok, err)
	}

	callsBefore, readsBefore := len(assignments.calls), source.reads
	_, ok, err = f.Next()
	if ok || !errors.Is(err, ErrAssignmentFault) {
		t.Fatalf("Next 2: got (ok=%v, err=%v), want (false, ErrAssignmentFault)", ok, err)
	}
	if len(assignments.calls) != callsBefore || source.reads != readsBefore {
		t.Fatalf("Next 2: calls/reads changed: got (%d, %d), want (%d, %d)", len(assignments.calls), source.reads, callsBefore, readsBefore)
	}
}

func TestNextTransientReadError(t *testing.T) {
	t.Run("DetailedMomentumsByHeight fails once", func(t *testing.T) {
		sentinel := errors.New("transient")
		oneBlock := func(h uint64) []*nom.AccountBlock {
			return []*nom.AccountBlock{sendBlock(1, h, 1, 1), receiveBlock(h)}
		}
		moms := chain(1, 3, oneBlock)
		source := &fakeSource{frontier: 4, moms: moms, readErr: sentinel}

		f, err := New(Config{DomainID: 1, ConfirmationDepth: 1, StartHeight: 1, Source: source, Assignments: &fakeAssignments{src: source}})
		if err != nil {
			t.Fatalf("New: %v", err)
		}
		if _, ok, err := f.Next(); ok || !errors.Is(err, sentinel) {
			t.Fatalf("Next 1: got (ok=%v, err=%v), want (false, wrapped sentinel)", ok, err)
		}
		got := drain(t, f)
		if len(got) != 3 {
			t.Fatalf("got %d inputs, want 3", len(got))
		}
		if got[0].Ctx.GlobalInputIndex != 0 {
			t.Fatalf("first input index: got %d, want 0", got[0].Ctx.GlobalInputIndex)
		}
	})

	t.Run("FrontierHeight fails once", func(t *testing.T) {
		sentinel := errors.New("transient")
		oneBlock := func(h uint64) []*nom.AccountBlock {
			return []*nom.AccountBlock{sendBlock(1, h, 1, 1), receiveBlock(h)}
		}
		moms := chain(1, 3, oneBlock)
		source := &fakeSource{frontier: 4, moms: moms, frontierErr: sentinel}

		f, err := New(Config{DomainID: 1, ConfirmationDepth: 1, StartHeight: 1, Source: source, Assignments: &fakeAssignments{src: source}})
		if err != nil {
			t.Fatalf("New: %v", err)
		}
		if _, ok, err := f.Next(); ok || !errors.Is(err, sentinel) {
			t.Fatalf("Next 1: got (ok=%v, err=%v), want (false, wrapped sentinel)", ok, err)
		}
		got := drain(t, f)
		if len(got) != 3 {
			t.Fatalf("got %d inputs, want 3", len(got))
		}
		if got[0].Ctx.GlobalInputIndex != 0 {
			t.Fatalf("first input index: got %d, want 0", got[0].Ctx.GlobalInputIndex)
		}
	})
}

func TestNextConfirmationDepthUnderflowGuard(t *testing.T) {
	oneBlock := func(h uint64) []*nom.AccountBlock {
		return []*nom.AccountBlock{sendBlock(1, h, 1, 1), receiveBlock(h)}
	}
	moms := chain(1, 3, oneBlock)
	source := &fakeSource{frontier: 2, moms: moms}

	f, err := New(Config{DomainID: 1, ConfirmationDepth: 5, StartHeight: 1, Source: source, Assignments: &fakeAssignments{src: source}})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	// frontier (2) < ConfirmationDepth (5): ceiling := frontier - ConfirmationDepth must not
	// underflow and consume every momentum the source holds.
	if _, ok, err := f.Next(); ok || err != nil {
		t.Fatalf("Next: got (ok=%v, err=%v), want (false, nil)", ok, err)
	}
}

func TestNextEmptyRefillDoesNotHang(t *testing.T) {
	source := &fakeSource{frontier: 10, moms: nil}
	f, err := New(Config{DomainID: 1, ConfirmationDepth: 1, StartHeight: 1, Source: source, Assignments: &fakeAssignments{src: source}})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	type result struct {
		ok  bool
		err error
	}
	done := make(chan result, 1)
	go func() {
		_, ok, err := f.Next()
		done <- result{ok: ok, err: err}
	}()

	select {
	case r := <-done:
		if r.ok || r.err != nil {
			t.Fatalf("Next: got (ok=%v, err=%v), want (false, nil)", r.ok, r.err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Next did not return: an empty momentum list below the ceiling must not spin the refill loop forever")
	}
}

func TestNextDuplicateAccountBlockHeader(t *testing.T) {
	// Two blocks share {Address, Height, Hash} but differ in Data, supplied non-adjacently
	// (a distinct block between them) to show the rejection does not depend on supply order.
	dup1 := newBlock(1, 1, 1, []byte{0})
	dup2 := newBlock(1, 1, 1, []byte{1})
	other := newBlock(2, 1, 2, nil)
	moms := []*nom.DetailedMomentum{newMomentum(1, types.Hash{}, []*nom.AccountBlock{dup1, other, dup2})}
	source := &fakeSource{frontier: 2, moms: moms}

	f, err := New(Config{DomainID: 1, ConfirmationDepth: 1, StartHeight: 1, Source: source, Assignments: &fakeAssignments{src: source}})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	_, ok, err := f.Next()
	if ok || err == nil || !strings.Contains(err.Error(), "duplicate account-block header") {
		t.Fatalf("Next 1: got (ok=%v, err=%v), want a duplicate account-block header error", ok, err)
	}
	_, ok, err2 := f.Next()
	if ok || err2 == nil || err2.Error() != err.Error() {
		t.Fatalf("Next 2: got (ok=%v, err=%v), want the same latched error (%v)", ok, err2, err)
	}
}

func TestNextContentAccountBlockMismatch(t *testing.T) {
	// m.Content commits to only block1; dm.AccountBlocks actually walks block1 and block2, so
	// the two node-supplied views of the momentum disagree.
	block1 := newBlock(1, 1, 1, nil)
	block2 := newBlock(2, 1, 2, nil)
	dm := &nom.DetailedMomentum{
		Momentum: &nom.Momentum{
			Height:        1,
			Hash:          momHash(1),
			PreviousHash:  types.Hash{},
			TimestampUnix: 1001,
			Content:       nom.NewMomentumContent([]*nom.AccountBlock{block1}),
		},
		AccountBlocks: []*nom.AccountBlock{block1, block2},
	}
	source := &fakeSource{frontier: 2, moms: []*nom.DetailedMomentum{dm}}

	f, err := New(Config{DomainID: 1, ConfirmationDepth: 1, StartHeight: 1, Source: source, Assignments: &fakeAssignments{src: source}})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	_, ok, err := f.Next()
	if ok || err == nil || !strings.Contains(err.Error(), "do not match the committed content") {
		t.Fatalf("Next 1: got (ok=%v, err=%v), want a content-mismatch error", ok, err)
	}
	_, ok, err2 := f.Next()
	if ok || err2 == nil || err2.Error() != err.Error() {
		t.Fatalf("Next 2: got (ok=%v, err=%v), want the same latched error (%v)", ok, err2, err)
	}
}

func TestNextNilContentEntry(t *testing.T) {
	// A nil m.Content entry (e.g. "content": [null] over JSON-RPC) must be rejected before any
	// Bytes() call on it, not panic.
	block1 := newBlock(1, 1, 1, nil)
	dm := newMomentum(1, types.Hash{}, []*nom.AccountBlock{block1})
	dm.Momentum.Content = append(dm.Momentum.Content, nil)
	source := &fakeSource{frontier: 2, moms: []*nom.DetailedMomentum{dm}}

	f, err := New(Config{DomainID: 1, ConfirmationDepth: 1, StartHeight: 1, Source: source, Assignments: &fakeAssignments{src: source}})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if _, ok, err := f.Next(); ok || err == nil || !strings.Contains(err.Error(), "nil content entry") {
		t.Fatalf("Next: got (ok=%v, err=%v), want a nil-content-entry error", ok, err)
	}
}

// --- checkpoint, recent momenta and source-read errors ---

func TestCheckpointBoundaryAndResume(t *testing.T) {
	// Momentum 1 holds two blocks for domain 0 and one for domain 1, in header order, so the
	// other domain's block follows this domain's last one. Momentum 2 holds one more for domain 0.
	moms := chain(1, 3, func(h uint64) []*nom.AccountBlock {
		switch h {
		case 1:
			return []*nom.AccountBlock{
				sendBlock(1, 1, 1, 0),
				sendBlock(2, 1, 2, 0),
				sendBlock(3, 1, 3, 1),
				receiveBlock(1),
			}
		case 2:
			return []*nom.AccountBlock{sendBlock(4, 2, 4, 0), receiveBlock(2)}
		}
		return nil
	})
	// The frontier confirms momentum 1 only, so the follower's buffer holds exactly its inputs.
	source := &fakeSource{frontier: 2, moms: moms}

	f, err := New(Config{DomainID: 0, ConfirmationDepth: 1, StartHeight: 1, Source: source, Assignments: &fakeAssignments{src: source}})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if _, ok := f.Checkpoint(); ok {
		t.Fatalf("Checkpoint ok before the first momentum")
	}

	first, ok, err := f.Next()
	if err != nil || !ok {
		t.Fatalf("Next: ok=%v err=%v", ok, err)
	}
	if first.Ctx.GlobalInputIndex != 0 {
		t.Fatalf("first index = %d, want 0", first.Ctx.GlobalInputIndex)
	}
	if _, ok := f.Checkpoint(); ok {
		t.Fatalf("Checkpoint ok while an input is still buffered")
	}

	if _, ok, err := f.Next(); err != nil || !ok {
		t.Fatalf("Next: ok=%v err=%v", ok, err)
	}
	cp, ok := f.Checkpoint()
	if !ok {
		t.Fatalf("Checkpoint not ok at a drained boundary")
	}
	// Ordinals 0 and 1 are domain 0's, so the next input takes ordinal 2. Index 2 is domain
	// 1's, so that input takes global index 3.
	if cp.Height != 1 || cp.Hash != momHash(1) || cp.NextOrdinal != 2 {
		t.Fatalf("Checkpoint = %+v, want height 1, hash of 1, next ordinal 2", cp)
	}

	source.frontier = 10
	rest := drain(t, f)

	resumedSource := &fakeSource{frontier: 10, moms: moms}
	resumed, err := New(Config{
		DomainID: 0, ConfirmationDepth: 1, StartHeight: cp.Height + 1, StartOrdinal: cp.NextOrdinal,
		Source: resumedSource, Assignments: &fakeAssignments{src: resumedSource},
	})
	if err != nil {
		t.Fatalf("New resumed: %v", err)
	}
	got := drain(t, resumed)
	if len(got) != len(rest) || len(got) == 0 {
		t.Fatalf("resumed emitted %d inputs, original continued with %d", len(got), len(rest))
	}
	for i := range got {
		if got[i].Ctx.GlobalInputIndex != rest[i].Ctx.GlobalInputIndex || !bytes.Equal(got[i].Payload, rest[i].Payload) {
			t.Fatalf("input %d differs: resumed %+v, original %+v", i, got[i], rest[i])
		}
	}
	if got[0].Ctx.GlobalInputIndex != 3 {
		t.Fatalf("resumed first index = %d, want 3", got[0].Ctx.GlobalInputIndex)
	}
}

func TestRecentMomentaWindow(t *testing.T) {
	// Only heights divisible by 3 hold a block, so most consumed momenta are empty.
	moms := chain(1, 12, func(h uint64) []*nom.AccountBlock {
		if h%3 == 0 {
			return []*nom.AccountBlock{sendBlock(byte(h), h, byte(h), 0), receiveBlock(h)}
		}
		return nil
	})
	source := &fakeSource{frontier: 13, moms: moms}
	f, err := New(Config{DomainID: 0, ConfirmationDepth: 3, StartHeight: 1, Source: source, Assignments: &fakeAssignments{src: source}})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if got := f.RecentMomenta(); len(got) != 0 {
		t.Fatalf("RecentMomenta before any read = %v", got)
	}

	check := func() {
		t.Helper()
		got := f.RecentMomenta()
		if uint64(len(got)) > 4 {
			t.Fatalf("RecentMomenta has %d entries, want at most ConfirmationDepth+1 = 4", len(got))
		}
		for i, e := range got {
			if e.Hash != momHash(e.Height) {
				t.Fatalf("entry %d hash does not match its height", i)
			}
			if i > 0 && e.Height != got[i-1].Height+1 {
				t.Fatalf("entries not contiguous ascending: %v", got)
			}
		}
		if cp, ok := f.Checkpoint(); ok && got[len(got)-1].Height != cp.Height {
			t.Fatalf("last entry %d is not the last consumed height %d", got[len(got)-1].Height, cp.Height)
		}
	}
	for {
		_, ok, err := f.Next()
		if err != nil {
			t.Fatalf("Next: %v", err)
		}
		if len(f.RecentMomenta()) > 0 {
			check()
		}
		if !ok {
			break
		}
	}
	got := f.RecentMomenta()
	if len(got) != 4 || got[3].Height != 10 {
		t.Fatalf("final RecentMomenta = %v, want the four momenta ending at the ceiling 10", got)
	}

	// The returned slice is a copy.
	got[0].Height = 999
	if f.RecentMomenta()[0].Height == 999 {
		t.Fatalf("RecentMomenta aliases internal state")
	}
}

func TestErrSourceRead(t *testing.T) {
	moms := chain(1, 3, func(h uint64) []*nom.AccountBlock {
		return []*nom.AccountBlock{sendBlock(byte(h), h, byte(h), 0), receiveBlock(h)}
	})

	t.Run("frontier read failure", func(t *testing.T) {
		source := &fakeSource{frontier: 10, moms: moms, frontierErr: errors.New("boom")}
		f, _ := New(Config{DomainID: 0, ConfirmationDepth: 1, StartHeight: 1, Source: source, Assignments: &fakeAssignments{src: source}})
		if _, _, err := f.Next(); !errors.Is(err, ErrSourceRead) {
			t.Fatalf("frontier failure: err = %v, want ErrSourceRead", err)
		}
	})
	t.Run("momenta read failure", func(t *testing.T) {
		source := &fakeSource{frontier: 10, moms: moms, readErr: errors.New("boom")}
		f, _ := New(Config{DomainID: 0, ConfirmationDepth: 1, StartHeight: 1, Source: source, Assignments: &fakeAssignments{src: source}})
		if _, _, err := f.Next(); !errors.Is(err, ErrSourceRead) {
			t.Fatalf("momenta failure: err = %v, want ErrSourceRead", err)
		}
		if _, ok, err := f.Next(); err != nil || !ok {
			t.Fatalf("retry after a source failure: ok=%v err=%v", ok, err)
		}
	})
	t.Run("latched failure is not a source read error", func(t *testing.T) {
		source := &fakeSource{frontier: 10, moms: moms}
		// The entry names a send the confirming momentum does not hold.
		assignments := &fakeAssignments{fn: func(height uint64, _ types.Hash, next uint64) ([]AssignmentEntry, error) {
			return []AssignmentEntry{{Ordinal: next, SourceAccountBlockHash: testHash(0x77), SendMomentumHeight: height}}, nil
		}}
		f, _ := New(Config{DomainID: 0, ConfirmationDepth: 1, StartHeight: 1, Source: source, Assignments: assignments})
		_, _, err := f.Next()
		if err == nil || errors.Is(err, ErrSourceRead) {
			t.Fatalf("latched failure: err = %v, want a non-ErrSourceRead error", err)
		}
	})
	t.Run("assignment read failure", func(t *testing.T) {
		source := &fakeSource{frontier: 10, moms: moms}
		f, _ := New(Config{DomainID: 0, ConfirmationDepth: 1, StartHeight: 1, Source: source,
			Assignments: &fakeAssignments{src: source, err: errors.New("down")}})
		if _, _, err := f.Next(); !errors.Is(err, ErrSourceRead) || errors.Is(err, ErrAssignmentFault) {
			t.Fatalf("assignment failure: err = %v, want ErrSourceRead and not ErrAssignmentFault", err)
		}
		if _, ok, err := f.Next(); err != nil || !ok {
			t.Fatalf("retry after an assignment failure: ok=%v err=%v", ok, err)
		}
	})
	t.Run("reorg is not a source read error", func(t *testing.T) {
		source := &fakeSource{frontier: 2, moms: moms}
		f, _ := New(Config{DomainID: 0, ConfirmationDepth: 1, StartHeight: 1, Source: source, Assignments: &fakeAssignments{src: source}})
		drain(t, f)
		source.frontier = 3
		source.moms = chain(1, 3, func(h uint64) []*nom.AccountBlock { return nil })
		source.moms[1].Momentum.PreviousHash = testHash(0xEE)
		_, _, err := f.Next()
		var re *ReorgError
		if !errors.As(err, &re) || errors.Is(err, ErrSourceRead) {
			t.Fatalf("reorg: err = %v, want a *ReorgError that is not ErrSourceRead", err)
		}
	})
}

// --- MaxBufferedInputs ---

// twoBlocksPerMomentum gives every momentum two domain-1 sends and a Settlement block, so a
// lost or duplicated input is visible in the drained global indices.
func twoBlocksPerMomentum(h uint64) []*nom.AccountBlock {
	return []*nom.AccountBlock{
		sendBlock(1, h, 1, 1),
		sendBlock(2, h, 2, 1),
		receiveBlock(h),
	}
}

func newBoundedFollower(t *testing.T, source *fakeSource, bound int, depth uint64) *Follower {
	t.Helper()
	f, err := New(Config{
		DomainID:          1,
		ConfirmationDepth: depth,
		StartHeight:       1,
		MaxBufferedInputs: bound,
		Source:            source,
		Assignments:       &fakeAssignments{src: source},
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return f
}

func TestMaxBufferedInputsStopsAtMomentumBoundary(t *testing.T) {
	source := &fakeSource{frontier: 7, moms: chain(1, 6, twoBlocksPerMomentum)}
	f := newBoundedFollower(t, source, 3, 1)

	if _, ok, err := f.Next(); err != nil || !ok {
		t.Fatalf("Next: got (ok=%v, err=%v), want (true, nil)", ok, err)
	}
	// Two momenta (four inputs) are the first whole-momentum total at or above the bound;
	// one input has been handed out.
	if got := len(f.buf); got != 3 {
		t.Fatalf("buffered inputs after first Next: got %d, want 3", got)
	}
	if got := f.nextHeight; got != 3 {
		t.Fatalf("nextHeight after first refill: got %d, want 3", got)
	}
}

func TestMaxBufferedInputsRereadsDiscardedMomentaWithoutLossOrDuplication(t *testing.T) {
	bounded := &fakeSource{frontier: 7, moms: chain(1, 6, twoBlocksPerMomentum)}
	unbounded := &fakeSource{frontier: 7, moms: chain(1, 6, twoBlocksPerMomentum)}

	got := drain(t, newBoundedFollower(t, bounded, 3, 1))
	want := drain(t, newBoundedFollower(t, unbounded, 0, 1))

	if len(got) != len(want) || len(got) != 12 {
		t.Fatalf("drained inputs: got %d, unbounded %d, want 12", len(got), len(want))
	}
	for i := range want {
		if !reflect.DeepEqual(got[i], want[i]) {
			t.Fatalf("input %d: got %+v, want %+v", i, got[i], want[i])
		}
		if got[i].Ctx.GlobalInputIndex != uint64(i) {
			t.Fatalf("input %d: globalInputIndex %d, want %d", i, got[i].Ctx.GlobalInputIndex, i)
		}
	}
	if bounded.reads <= unbounded.reads {
		t.Fatalf("source reads: bounded %d, unbounded %d, want bounded to read more often", bounded.reads, unbounded.reads)
	}
}

func TestMaxBufferedInputsZeroBuffersWholeRangedRead(t *testing.T) {
	source := &fakeSource{frontier: 7, moms: chain(1, 6, twoBlocksPerMomentum)}
	f := newBoundedFollower(t, source, 0, 1)

	if _, ok, err := f.Next(); err != nil || !ok {
		t.Fatalf("Next: got (ok=%v, err=%v), want (true, nil)", ok, err)
	}
	if got := len(f.buf); got != 11 {
		t.Fatalf("buffered inputs after first Next: got %d, want 11", got)
	}
	if source.reads != 1 {
		t.Fatalf("source reads: got %d, want 1", source.reads)
	}
}

func TestMaxBufferedInputsReorgBeforeBoundSurfaces(t *testing.T) {
	moms := chain(1, 4, func(h uint64) []*nom.AccountBlock {
		return []*nom.AccountBlock{sendBlock(1, h, 1, 1), receiveBlock(h)}
	})
	moms[1] = newMomentum(2, testHash(0xAA), []*nom.AccountBlock{sendBlock(1, 2, 1, 1), receiveBlock(2)})
	source := &fakeSource{frontier: 5, moms: moms}
	f := newBoundedFollower(t, source, 3, 1)

	if _, ok, err := f.Next(); err != nil || !ok {
		t.Fatalf("Next 1: got (ok=%v, err=%v), want (true, nil)", ok, err)
	}
	_, ok, err := f.Next()
	var reorg *ReorgError
	if ok || !errors.As(err, &reorg) {
		t.Fatalf("Next 2: got (ok=%v, err=%v), want a *ReorgError", ok, err)
	}
	if reorg.Height != 2 || reorg.Want != momHash(1) || reorg.Got != testHash(0xAA) {
		t.Fatalf("Next 2: got %+v, want {Height:2 Want:%v Got:%v}", reorg, momHash(1), testHash(0xAA))
	}
}

func TestMaxBufferedInputsReorgInDiscardedMomentumSurfacesOnLaterRefill(t *testing.T) {
	moms := chain(1, 4, twoBlocksPerMomentum)
	moms[2] = newMomentum(3, testHash(0xAA), twoBlocksPerMomentum(3))
	source := &fakeSource{frontier: 5, moms: moms}
	f := newBoundedFollower(t, source, 3, 1)

	for i := 0; i < 4; i++ {
		if _, ok, err := f.Next(); err != nil || !ok {
			t.Fatalf("Next %d: got (ok=%v, err=%v), want (true, nil)", i, ok, err)
		}
	}
	_, ok, err := f.Next()
	var reorg *ReorgError
	if ok || !errors.As(err, &reorg) || reorg.Height != 3 {
		t.Fatalf("Next 5: got (ok=%v, err=%v), want a *ReorgError at height 3", ok, err)
	}
}

func TestMaxBufferedInputsCeilingBeforeBoundEndsCleanly(t *testing.T) {
	// Frontier 4 with depth 1 confirms heights 1..3, but the source folds height 4 into
	// the window.
	source := &fakeSource{frontier: 4, moms: chain(1, 5, twoBlocksPerMomentum), perCallCap: 5}
	f := newBoundedFollower(t, source, 100, 1)

	got := drain(t, f)
	if len(got) != 6 {
		t.Fatalf("drained inputs: got %d, want 6", len(got))
	}
	if got[len(got)-1].Ctx.MomentumHeight != 3 {
		t.Fatalf("last input height: got %d, want 3", got[len(got)-1].Ctx.MomentumHeight)
	}
}

// --- assignment-driven input derivation ---

func TestAssignmentAcceptedAndRejected(t *testing.T) {
	// Momentum 2 holds three domain-1 sends, one domain-2 send and a Settlement block.
	// Settlement admits the first and third domain-1 sends only.
	moms := []*nom.DetailedMomentum{
		newMomentum(1, types.Hash{}, []*nom.AccountBlock{sendBlock(9, 1, 9, 1)}),
		newMomentum(2, momHash(1), []*nom.AccountBlock{
			sendBlock(1, 2, 1, 1),
			sendBlock(2, 2, 2, 1),
			sendBlock(3, 2, 3, 1),
			sendBlock(4, 2, 4, 2),
			receiveBlock(2),
		}),
	}
	source := &fakeSource{frontier: 3, moms: moms}
	assignments := &fakeAssignments{fn: func(height uint64, hash types.Hash, next uint64) ([]AssignmentEntry, error) {
		return []AssignmentEntry{
			{Ordinal: 0, GlobalInputIndex: 11, SourceAccountBlockHash: testHash(1), SendMomentumHeight: 2},
			{Ordinal: 1, GlobalInputIndex: 14, SourceAccountBlockHash: testHash(3), SendMomentumHeight: 2},
		}, nil
	}}
	f, err := New(Config{DomainID: 1, ConfirmationDepth: 1, StartHeight: 1, Source: source, Assignments: assignments})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	got := drain(t, f)
	want := make([]Input, 0, 2)
	for _, e := range []struct {
		idx  uint64
		hash types.Hash
	}{{11, testHash(1)}, {14, testHash(3)}} {
		in, err := DecodeAssignedInput(1, moms[1], e.idx, e.hash)
		if err != nil {
			t.Fatalf("DecodeAssignedInput: %v", err)
		}
		want = append(want, in)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("inputs = %+v, want %+v", got, want)
	}
	if len(assignments.calls) != 1 || assignments.calls[0].height != 2 {
		t.Fatalf("assignment calls = %+v, want one call at height 2", assignments.calls)
	}
	if cp, ok := f.Checkpoint(); !ok || cp.NextOrdinal != 2 {
		t.Fatalf("Checkpoint = (%+v, %v), want NextOrdinal 2", cp, ok)
	}
}

func TestAssignmentOnlyForSettlementMomenta(t *testing.T) {
	moms := chain(1, 5, func(h uint64) []*nom.AccountBlock {
		switch h {
		case 2:
			return []*nom.AccountBlock{sendBlock(1, h, 1, 1), sendBlock(2, h, 2, 1), receiveBlock(h)}
		case 4:
			return []*nom.AccountBlock{sendBlock(3, h, 3, 1), receiveBlock(h)}
		}
		return nil
	})
	source := &fakeSource{frontier: 6, moms: moms}
	assignments := &fakeAssignments{src: source}
	f, err := New(Config{DomainID: 1, ConfirmationDepth: 1, StartHeight: 1, Source: source, Assignments: assignments})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	if got := drain(t, f); len(got) != 3 {
		t.Fatalf("got %d inputs, want 3", len(got))
	}
	want := []assignmentCall{{height: 2, hash: momHash(2), next: 0}, {height: 4, hash: momHash(4), next: 2}}
	if !reflect.DeepEqual(assignments.calls, want) {
		t.Fatalf("calls = %+v, want %+v", assignments.calls, want)
	}
}

func TestAssignmentSendsAcrossHeights(t *testing.T) {
	moms := chain(1, 5, func(h uint64) []*nom.AccountBlock {
		switch h {
		case 2:
			return []*nom.AccountBlock{sendBlock(1, h, 0x0A, 1), sendBlock(2, h, 0x0B, 1)}
		case 4:
			return []*nom.AccountBlock{sendBlock(3, h, 0x0C, 1)}
		case 5:
			return []*nom.AccountBlock{sendBlock(4, h, 0x0D, 1), receiveBlock(h)}
		}
		return nil
	})
	source := &fakeSource{frontier: 6, moms: moms}
	assignments := &fakeAssignments{fn: func(height uint64, hash types.Hash, next uint64) ([]AssignmentEntry, error) {
		return []AssignmentEntry{
			{Ordinal: 0, GlobalInputIndex: 3, SourceAccountBlockHash: testHash(0x0A), SendMomentumHeight: 2},
			{Ordinal: 1, GlobalInputIndex: 4, SourceAccountBlockHash: testHash(0x0B), SendMomentumHeight: 2},
			{Ordinal: 2, GlobalInputIndex: 6, SourceAccountBlockHash: testHash(0x0C), SendMomentumHeight: 4},
			{Ordinal: 3, GlobalInputIndex: 9, SourceAccountBlockHash: testHash(0x0D), SendMomentumHeight: 5},
		}, nil
	}}
	f, err := New(Config{DomainID: 1, ConfirmationDepth: 1, StartHeight: 4, Source: source, Assignments: assignments})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	got := drain(t, f)
	wantSenders := []byte{1, 2, 3, 4}
	wantIndices := []uint64{3, 4, 6, 9}
	wantHeights := []uint64{2, 2, 4, 5}
	if len(got) != 4 {
		t.Fatalf("got %d inputs, want 4", len(got))
	}
	for i, in := range got {
		if in.Ctx.L1Sender != testAddr(wantSenders[i]) || in.Ctx.GlobalInputIndex != wantIndices[i] {
			t.Fatalf("input %d: got (sender %s, index %d), want (address %d, index %d)", i, in.Ctx.L1Sender, in.Ctx.GlobalInputIndex, wantSenders[i], wantIndices[i])
		}
		if in.Ctx.MomentumHeight != wantHeights[i] || in.Ctx.MomentumTime != 1000+wantHeights[i] {
			t.Fatalf("input %d: got (height %d, time %d), want (%d, %d)", i, in.Ctx.MomentumHeight, in.Ctx.MomentumTime, wantHeights[i], 1000+wantHeights[i])
		}
	}
	if want := []uint64{4, 2}; !reflect.DeepEqual(source.heights, want) {
		t.Fatalf("read heights = %v, want %v", source.heights, want)
	}
}

func TestAssignmentFetchedHashMustMatchFollowed(t *testing.T) {
	moms := []*nom.DetailedMomentum{
		newMomentum(1, types.Hash{}, []*nom.AccountBlock{sendBlock(1, 1, 0x0A, 1), sendBlock(2, 1, 0x0B, 1), receiveBlock(1)}),
		newMomentum(2, momHash(1), []*nom.AccountBlock{receiveBlock(2)}),
	}
	source := &fakeSource{frontier: 10, moms: moms, perCallCap: 1}
	assignments := &fakeAssignments{fn: func(height uint64, hash types.Hash, next uint64) ([]AssignmentEntry, error) {
		if height == 1 {
			return []AssignmentEntry{{Ordinal: next, GlobalInputIndex: 0, SourceAccountBlockHash: testHash(0x0A), SendMomentumHeight: 1}}, nil
		}
		return []AssignmentEntry{{Ordinal: next, GlobalInputIndex: 1, SourceAccountBlockHash: testHash(0x0B), SendMomentumHeight: 1}}, nil
	}}
	f, err := New(Config{DomainID: 1, ConfirmationDepth: 3, StartHeight: 1, Source: source, Assignments: assignments})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	in, ok, err := f.Next()
	if err != nil || !ok || in.Ctx.L1Sender != testAddr(1) {
		t.Fatalf("Next 1: got (sender %s, ok=%v, err=%v), want send a", in.Ctx.L1Sender, ok, err)
	}

	original := moms[0]
	forged := *original.Momentum
	forged.Hash = testHash(0x99)
	source.moms[0] = &nom.DetailedMomentum{Momentum: &forged, AccountBlocks: original.AccountBlocks}
	if _, ok, err := f.Next(); ok || !errors.Is(err, ErrSourceRead) {
		t.Fatalf("Next 2: got (ok=%v, err=%v), want (false, ErrSourceRead)", ok, err)
	}
	if f.nextHeight != 2 {
		t.Fatalf("nextHeight = %d, want 2", f.nextHeight)
	}

	source.moms[0] = original
	in, ok, err = f.Next()
	if err != nil || !ok || in.Ctx.L1Sender != testAddr(2) || in.Ctx.MomentumHeight != 1 {
		t.Fatalf("Next 3: got (sender %s, height %d, ok=%v, err=%v), want send b at height 1", in.Ctx.L1Sender, in.Ctx.MomentumHeight, ok, err)
	}
}

func TestAssignmentFaults(t *testing.T) {
	entryFor := func(hash types.Hash, sendHeight uint64) func(uint64, types.Hash, uint64) ([]AssignmentEntry, error) {
		return func(height uint64, _ types.Hash, next uint64) ([]AssignmentEntry, error) {
			return []AssignmentEntry{{Ordinal: next, SourceAccountBlockHash: hash, SendMomentumHeight: sendHeight}}, nil
		}
	}
	cases := []struct {
		name    string
		moms    []*nom.DetailedMomentum
		cap     uint64
		fn      func(uint64, types.Hash, uint64) ([]AssignmentEntry, error)
		fetched bool
		start   uint64 // StartHeight; 0 means 1
		walked  bool
	}{
		{
			name: "send not in the confirming momentum",
			moms: []*nom.DetailedMomentum{newMomentum(1, types.Hash{}, []*nom.AccountBlock{sendBlock(1, 1, 1, 1), receiveBlock(1)})},
			fn:   entryFor(testHash(0x77), 1),
		},
		{
			name: "send not in a momentum of the same read",
			moms: []*nom.DetailedMomentum{
				newMomentum(1, types.Hash{}, nil),
				newMomentum(2, momHash(1), []*nom.AccountBlock{sendBlock(1, 2, 1, 1), receiveBlock(2)}),
			},
			fn: entryFor(testHash(1), 1),
		},
		{
			name: "send not in a fetched followed momentum",
			moms: []*nom.DetailedMomentum{
				newMomentum(1, types.Hash{}, []*nom.AccountBlock{sendBlock(1, 1, 1, 1)}),
				newMomentum(2, momHash(1), []*nom.AccountBlock{sendBlock(2, 2, 2, 1), receiveBlock(2)}),
			},
			cap:     1,
			fn:      entryFor(testHash(2), 1),
			fetched: true,
		},
		{
			name: "send not in a walked momentum",
			moms: []*nom.DetailedMomentum{
				newMomentum(1, types.Hash{}, []*nom.AccountBlock{sendBlock(1, 1, 1, 1)}),
				newMomentum(2, momHash(1), []*nom.AccountBlock{sendBlock(2, 2, 2, 1), receiveBlock(2)}),
			},
			start:  2,
			fn:     entryFor(testHash(2), 1),
			walked: true,
		},
		{
			name: "undecodable send",
			moms: []*nom.DetailedMomentum{newMomentum(1, types.Hash{}, []*nom.AccountBlock{newBlock(1, 1, 1, []byte{1, 2, 3, 4}), receiveBlock(1)})},
			fn:   entryFor(testHash(1), 1),
		},
		{
			name: "send naming domain 2",
			moms: []*nom.DetailedMomentum{newMomentum(1, types.Hash{}, []*nom.AccountBlock{sendBlock(1, 1, 1, 2), receiveBlock(1)})},
			fn:   entryFor(testHash(1), 1),
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			source := &fakeSource{frontier: 10, moms: tc.moms, perCallCap: tc.cap}
			assignments := &fakeAssignments{fn: tc.fn}
			start := tc.start
			if start == 0 {
				start = 1
			}
			f, err := New(Config{DomainID: 1, ConfirmationDepth: 8, StartHeight: start, Source: source, Assignments: assignments})
			if err != nil {
				t.Fatalf("New: %v", err)
			}

			in, ok, err := f.Next()
			if ok || !errors.Is(err, ErrAssignmentFault) || errors.Is(err, ErrSourceRead) {
				t.Fatalf("Next 1: got (%+v, ok=%v, err=%v), want a fault that is not a read failure", in, ok, err)
			}
			if tc.fetched {
				if n := len(source.heights); n < 3 || source.heights[n-1] != 1 || source.heights[n-2] != 2 {
					t.Fatalf("read heights = %v, want a fetch at 1 after the read at 2", source.heights)
				}
			}

			if tc.walked {
				if want := []uint64{2, 1}; !reflect.DeepEqual(source.heights, want) {
					t.Fatalf("read heights = %v, want %v", source.heights, want)
				}
				if want := []uint64{1, 2}; !reflect.DeepEqual(source.counts, want) {
					t.Fatalf("read counts = %v, want %v", source.counts, want)
				}
			}

			calls := len(assignments.calls)
			_, ok, err2 := f.Next()
			if ok || err2 == nil || err2.Error() != err.Error() {
				t.Fatalf("Next 2: got (ok=%v, err=%v), want the latched error (%v)", ok, err2, err)
			}
			if len(assignments.calls) != calls {
				t.Fatalf("Next 2 requested another assignment")
			}
		})
	}
}

func TestAssignmentReadFailures(t *testing.T) {
	moms := chain(1, 5, func(h uint64) []*nom.AccountBlock {
		switch h {
		case 2:
			return []*nom.AccountBlock{sendBlock(1, h, 0x0A, 1), sendBlock(2, h, 0x0B, 1)}
		case 5:
			return []*nom.AccountBlock{receiveBlock(h)}
		}
		return nil
	})
	entries := func(height uint64) []AssignmentEntry {
		return []AssignmentEntry{
			{Ordinal: 0, GlobalInputIndex: 0, SourceAccountBlockHash: testHash(0x0A), SendMomentumHeight: height},
			{Ordinal: 1, GlobalInputIndex: 1, SourceAccountBlockHash: testHash(0x0B), SendMomentumHeight: height},
		}
	}
	cases := []struct {
		name     string
		first    []AssignmentEntry
		firstErr error
		fetchErr map[uint64]error
	}{
		{name: "assignment source error", firstErr: fmt.Errorf("halt: %w", errFakeHalt)},
		{name: "send momentum height 0", first: entries(0)},
		{name: "send momentum height above the confirming momentum", first: entries(6)},
		{name: "fetch failure", first: entries(2), fetchErr: map[uint64]error{2: errors.New("fetch down")}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			source := &fakeSource{frontier: 6, moms: moms, fetchErr: tc.fetchErr}
			first := true
			assignments := &fakeAssignments{fn: func(height uint64, hash types.Hash, next uint64) ([]AssignmentEntry, error) {
				if first {
					first = false
					return tc.first, tc.firstErr
				}
				return entries(2), nil
			}}
			f, err := New(Config{DomainID: 1, ConfirmationDepth: 1, StartHeight: 5, Source: source, Assignments: assignments})
			if err != nil {
				t.Fatalf("New: %v", err)
			}

			_, ok, err := f.Next()
			if ok || !errors.Is(err, ErrSourceRead) || errors.Is(err, ErrAssignmentFault) {
				t.Fatalf("Next 1: got (ok=%v, err=%v), want a read failure that is not a fault", ok, err)
			}
			if tc.firstErr != nil && !errors.Is(err, errFakeHalt) {
				t.Fatalf("Next 1: err = %v, want the cause wrapped", err)
			}
			if f.nextHeight != 5 {
				t.Fatalf("nextHeight = %d, want 5", f.nextHeight)
			}

			for i, addr := range []byte{1, 2} {
				in, ok, err := f.Next()
				if err != nil || !ok || in.Ctx.L1Sender != testAddr(addr) || in.Ctx.MomentumHeight != 2 {
					t.Fatalf("Next %d: got (sender %s, height %d, ok=%v, err=%v), want send %d at height 2", i+2, in.Ctx.L1Sender, in.Ctx.MomentumHeight, ok, err, addr)
				}
			}
			want := []assignmentCall{{height: 5, hash: momHash(5), next: 0}, {height: 5, hash: momHash(5), next: 0}}
			if !reflect.DeepEqual(assignments.calls, want) {
				t.Fatalf("calls = %+v, want %+v", assignments.calls, want)
			}
		})
	}
}

func TestAssignmentReadFailureWithBufferedInputs(t *testing.T) {
	moms := chain(1, 2, func(h uint64) []*nom.AccountBlock {
		return []*nom.AccountBlock{sendBlock(byte(h), h, byte(h), 1), receiveBlock(h)}
	})
	source := &fakeSource{frontier: 3, moms: moms}
	failed := false
	assignments := &fakeAssignments{fn: func(height uint64, hash types.Hash, next uint64) ([]AssignmentEntry, error) {
		if height == 2 && !failed {
			failed = true
			return nil, errors.New("down")
		}
		return []AssignmentEntry{{Ordinal: next, GlobalInputIndex: height - 1, SourceAccountBlockHash: testHash(byte(height)), SendMomentumHeight: height}}, nil
	}}
	f, err := New(Config{DomainID: 1, ConfirmationDepth: 1, StartHeight: 1, Source: source, Assignments: assignments})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	for i := 1; i <= 2; i++ {
		in, ok, err := f.Next()
		if err != nil || !ok || in.Ctx.MomentumHeight != uint64(i) {
			t.Fatalf("Next %d: got (height %d, ok=%v, err=%v), want the input at height %d", i, in.Ctx.MomentumHeight, ok, err, i)
		}
	}
	var atTwo int
	for _, c := range assignments.calls {
		if c.height == 2 {
			atTwo++
		}
	}
	if atTwo != 2 {
		t.Fatalf("calls at height 2 = %d, want 2 (calls %+v)", atTwo, assignments.calls)
	}
}

func TestAssignmentFirstOrdinalMismatch(t *testing.T) {
	cases := []struct {
		name  string
		first uint64
	}{
		{"matches", 5},
		{"ahead", 6},
		{"behind", 4},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			moms := []*nom.DetailedMomentum{newMomentum(1, types.Hash{}, []*nom.AccountBlock{sendBlock(1, 1, 1, 1), sendBlock(2, 1, 2, 1), receiveBlock(1)})}
			source := &fakeSource{frontier: 2, moms: moms}
			assignments := &fakeAssignments{fn: func(height uint64, hash types.Hash, next uint64) ([]AssignmentEntry, error) {
				return []AssignmentEntry{
					{Ordinal: tc.first, GlobalInputIndex: 0, SourceAccountBlockHash: testHash(1), SendMomentumHeight: 1},
					{Ordinal: tc.first + 1, GlobalInputIndex: 1, SourceAccountBlockHash: testHash(2), SendMomentumHeight: 1},
				}, nil
			}}
			f, err := New(Config{DomainID: 1, ConfirmationDepth: 1, StartHeight: 1, StartOrdinal: 5, Source: source, Assignments: assignments})
			if err != nil {
				t.Fatalf("New: %v", err)
			}

			if tc.name == "matches" {
				if got := drain(t, f); len(got) != 2 {
					t.Fatalf("got %d inputs, want 2", len(got))
				}
				if cp, ok := f.Checkpoint(); !ok || cp.NextOrdinal != 7 {
					t.Fatalf("Checkpoint = (%+v, %v), want NextOrdinal 7", cp, ok)
				}
				return
			}

			want := fmt.Sprintf("starts at ordinal %d, want 5", tc.first)
			in, ok, err := f.Next()
			if ok || !errors.Is(err, ErrAssignmentFault) || errors.Is(err, ErrSourceRead) || !strings.Contains(err.Error(), want) {
				t.Fatalf("Next 1: got (%+v, ok=%v, err=%v), want a fault containing %q", in, ok, err, want)
			}
			_, ok, err2 := f.Next()
			if ok || err2 == nil || err2.Error() != err.Error() {
				t.Fatalf("Next 2: got (ok=%v, err=%v), want the latched error", ok, err2)
			}
			if len(assignments.calls) != 1 {
				t.Fatalf("calls = %d, want 1", len(assignments.calls))
			}
			if f.nextOrdinal != 5 {
				t.Fatalf("nextOrdinal = %d, want 5", f.nextOrdinal)
			}
		})
	}
}

// --- withdrawal caps ---

type capCall struct {
	domainID uint32
	height   uint64
	hash     types.Hash
	assets   []codec.AssetID
}

// fakeCaps is a CapSource that records every call and answers 5 for every asset.
type fakeCaps struct{ calls []capCall }

func (c *fakeCaps) MaxBatchWithdrawals(domainID uint32, height uint64, hash types.Hash, assets []codec.AssetID) ([]codec.U256, error) {
	c.calls = append(c.calls, capCall{domainID: domainID, height: height, hash: hash, assets: append([]codec.AssetID(nil), assets...)})
	out := make([]codec.U256, len(assets))
	for i := range out {
		out[i] = codec.U256{31: 5}
	}
	return out, nil
}

// capChain is a chain where momentum 1 holds send a and no receive, and momentum 2 holds send b
// and the receive, so one assignment at height 2 names a send from each momentum.
func capChain() (*fakeSource, *fakeAssignments) {
	moms := []*nom.DetailedMomentum{
		newMomentum(1, types.Hash{}, []*nom.AccountBlock{sendBlock(1, 1, 0x0A, 1)}),
		newMomentum(2, momHash(1), []*nom.AccountBlock{sendBlock(2, 2, 0x0B, 1), receiveBlock(2)}),
	}
	source := &fakeSource{frontier: 3, moms: moms}
	assignments := &fakeAssignments{fn: func(height uint64, hash types.Hash, next uint64) ([]AssignmentEntry, error) {
		return []AssignmentEntry{
			{Ordinal: 0, GlobalInputIndex: 0, SourceAccountBlockHash: testHash(0x0A), SendMomentumHeight: 1},
			{Ordinal: 1, GlobalInputIndex: 1, SourceAccountBlockHash: testHash(0x0B), SendMomentumHeight: 2},
		}, nil
	}}
	return source, assignments
}

func TestCapsBoundToConfirmingMomentum(t *testing.T) {
	source, assignments := capChain()
	caps := &fakeCaps{}
	f, err := New(Config{DomainID: 1, ConfirmationDepth: 1, StartHeight: 1, Source: source, Assignments: assignments, Caps: caps})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	got := drain(t, f)
	if len(got) != 2 {
		t.Fatalf("got %d inputs, want 2", len(got))
	}
	a, b := got[0], got[1]
	if a.Ctx.MomentumHeight != 1 {
		t.Fatalf("input a MomentumHeight = %d, want 1", a.Ctx.MomentumHeight)
	}
	if a.Ctx.Caps == nil || b.Ctx.Caps == nil || a.Ctx.Caps != b.Ctx.Caps {
		t.Fatalf("Ctx.Caps: a %v, b %v; want one shared non-nil lookup", a.Ctx.Caps, b.Ctx.Caps)
	}

	x := codec.AssetIDFromZTS(types.ZnnTokenStandard)
	v, err := a.Ctx.Caps.MaxBatchWithdrawals([]codec.AssetID{x})
	if err != nil || len(v) != 1 || v[0] != (codec.U256{31: 5}) {
		t.Fatalf("a cap = %x, %v; want [5]", v, err)
	}
	want := []capCall{{domainID: 1, height: 2, hash: momHash(2), assets: []codec.AssetID{x}}}
	if !reflect.DeepEqual(caps.calls, want) {
		t.Fatalf("source calls = %+v, want %+v", caps.calls, want)
	}
	if _, err := b.Ctx.Caps.MaxBatchWithdrawals([]codec.AssetID{x}); err != nil {
		t.Fatalf("b cap: %v", err)
	}
	if len(caps.calls) != 1 {
		t.Fatalf("source calls after b = %d, want 1 (memoised)", len(caps.calls))
	}
}

func TestCapsNilSource(t *testing.T) {
	source, assignments := capChain()
	f, err := New(Config{DomainID: 1, ConfirmationDepth: 1, StartHeight: 1, Source: source, Assignments: assignments})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	got := drain(t, f)
	if len(got) != 2 {
		t.Fatalf("got %d inputs, want 2", len(got))
	}
	for i, in := range got {
		if in.Ctx.Caps != nil {
			t.Fatalf("input %d Ctx.Caps = %v, want nil", i, in.Ctx.Caps)
		}
	}
}

// --- walks below the window ---

// walkChain is momenta 1 to n. Momentum 2 holds domain-1 sends a (0x0A) and b (0x0B), momentum
// 5 holds send c (0x0C), and momentum n holds the receive. The assignment at n names a at 2, c at
// 5 and b at 2, with ordinals 0-2 and global indices 0-2. The frontier is n+1.
func walkChain(n uint64) (*fakeSource, *fakeAssignments) {
	moms := chain(1, int(n), func(h uint64) []*nom.AccountBlock {
		switch h {
		case 2:
			return []*nom.AccountBlock{sendBlock(1, h, 0x0A, 1), sendBlock(2, h, 0x0B, 1)}
		case 5:
			return []*nom.AccountBlock{sendBlock(3, h, 0x0C, 1)}
		case n:
			return []*nom.AccountBlock{receiveBlock(h)}
		}
		return nil
	})
	source := &fakeSource{frontier: n + 1, moms: moms}
	assignments := &fakeAssignments{fn: func(height uint64, hash types.Hash, next uint64) ([]AssignmentEntry, error) {
		return []AssignmentEntry{
			{Ordinal: 0, GlobalInputIndex: 0, SourceAccountBlockHash: testHash(0x0A), SendMomentumHeight: 2},
			{Ordinal: 1, GlobalInputIndex: 1, SourceAccountBlockHash: testHash(0x0C), SendMomentumHeight: 5},
			{Ordinal: 2, GlobalInputIndex: 2, SourceAccountBlockHash: testHash(0x0B), SendMomentumHeight: 2},
		}, nil
	}}
	return source, assignments
}

// readChain is momenta 1 to 8. Momenta 2, 3 and 4 hold domain-1 sends a (0x0A), b (0x0B) and
// c (0x0C); momenta 7 and 8 hold receives. The assignment at 7 is one entry naming send
// firstHash at height firstHeight, and the one at 8 names secondHash at secondHeight; each
// entry's Ordinal and GlobalInputIndex equal next. The frontier is 9.
func readChain(firstHash types.Hash, firstHeight uint64, secondHash types.Hash, secondHeight uint64) (*fakeSource, *fakeAssignments) {
	moms := chain(1, 8, func(h uint64) []*nom.AccountBlock {
		switch h {
		case 2:
			return []*nom.AccountBlock{sendBlock(1, h, 0x0A, 1)}
		case 3:
			return []*nom.AccountBlock{sendBlock(2, h, 0x0B, 1)}
		case 4:
			return []*nom.AccountBlock{sendBlock(3, h, 0x0C, 1)}
		case 7, 8:
			return []*nom.AccountBlock{receiveBlock(h)}
		}
		return nil
	})
	source := &fakeSource{frontier: 9, moms: moms}
	assignments := &fakeAssignments{fn: func(height uint64, hash types.Hash, next uint64) ([]AssignmentEntry, error) {
		e := AssignmentEntry{Ordinal: next, GlobalInputIndex: next, SourceAccountBlockHash: firstHash, SendMomentumHeight: firstHeight}
		if height == 8 {
			e.SourceAccountBlockHash, e.SendMomentumHeight = secondHash, secondHeight
		}
		return []AssignmentEntry{e}, nil
	}}
	return source, assignments
}

func senderAndHeights(ins []Input) (senders []types.Address, heights []uint64) {
	for _, in := range ins {
		senders = append(senders, in.Ctx.L1Sender)
		heights = append(heights, in.Ctx.MomentumHeight)
	}
	return senders, heights
}

func checkEmitted(t *testing.T, got []Input, wantSenders []byte, wantHeights []uint64) {
	t.Helper()
	senders, heights := senderAndHeights(got)
	var want []types.Address
	for _, a := range wantSenders {
		want = append(want, testAddr(a))
	}
	if !reflect.DeepEqual(senders, want) || !reflect.DeepEqual(heights, wantHeights) {
		t.Fatalf("emitted senders %v heights %v, want senders %v heights %v", senders, heights, want, wantHeights)
	}
}

func TestWalkOlderThanWindow(t *testing.T) {
	source, assignments := walkChain(8)
	f, err := New(Config{DomainID: 1, ConfirmationDepth: 1, StartHeight: 7, Source: source, Assignments: assignments})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	got := drain(t, f)
	checkEmitted(t, got, []byte{1, 3, 2}, []uint64{2, 5, 2})
	for i, want := range []uint64{1002, 1005, 1002} {
		if got[i].Ctx.MomentumTime != want {
			t.Fatalf("input %d MomentumTime = %d, want %d", i, got[i].Ctx.MomentumTime, want)
		}
	}
	if want := []uint64{7, 2}; !reflect.DeepEqual(source.heights, want) {
		t.Fatalf("read heights = %v, want %v", source.heights, want)
	}
	if want := []uint64{2, 6}; !reflect.DeepEqual(source.counts, want) {
		t.Fatalf("read counts = %v, want %v", source.counts, want)
	}
	if cp, ok := f.Checkpoint(); !ok || cp.NextOrdinal != 3 {
		t.Fatalf("Checkpoint = %+v, %v; want NextOrdinal 3", cp, ok)
	}
}

func TestWalkAllocatesOnce(t *testing.T) {
	source, assignments := walkChain(8)
	f, err := New(Config{DomainID: 1, ConfirmationDepth: 1, StartHeight: 7, Source: source, Assignments: assignments})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	tail := []types.Hash{momHash(7), testHash(0xAA)}
	_, below, err := f.walk(2, 7, momHash(7), map[uint64]bool{}, tail)
	if err != nil {
		t.Fatalf("walk: %v", err)
	}
	want := []types.Hash{momHash(2), momHash(3), momHash(4), momHash(5), momHash(6), momHash(7), testHash(0xAA)}
	if !reflect.DeepEqual(below, want) {
		t.Fatalf("hashes = %v, want %v", below, want)
	}
	if cap(below) != len(want) {
		t.Fatalf("cap = %d, want %d: the walk grew the slice past its single allocation", cap(below), len(want))
	}
}

func TestWalkBrokenLink(t *testing.T) {
	source, assignments := walkChain(8)
	f, err := New(Config{DomainID: 1, ConfirmationDepth: 1, StartHeight: 7, Source: source, Assignments: assignments})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	source.moms[3].Momentum.PreviousHash = testHash(0xEE)
	in, ok, err := f.Next()
	if ok || !errors.Is(err, ErrSourceRead) || errors.Is(err, ErrAssignmentFault) {
		t.Fatalf("Next 1: got (%+v, ok=%v, err=%v), want a read failure that is not a fault", in, ok, err)
	}
	if f.nextHeight != 8 {
		t.Fatalf("nextHeight = %d, want 8", f.nextHeight)
	}

	source.moms[3].Momentum.PreviousHash = momHash(3)
	in, ok, err = f.Next()
	if err != nil || !ok || in.Ctx.MomentumHeight != 2 {
		t.Fatalf("Next 2: got (height %d, ok=%v, err=%v), want send a at height 2", in.Ctx.MomentumHeight, ok, err)
	}
}

func TestWalkTopMismatch(t *testing.T) {
	moms := chain(1, 5, func(h uint64) []*nom.AccountBlock {
		switch h {
		case 1:
			return []*nom.AccountBlock{sendBlock(1, 1, 0x0A, 1)}
		case 3:
			return []*nom.AccountBlock{sendBlock(2, 3, 0x0B, 1), receiveBlock(3)}
		case 5:
			return []*nom.AccountBlock{receiveBlock(5)}
		}
		return nil
	})
	source := &fakeSource{frontier: 6, moms: moms}
	assignments := &fakeAssignments{fn: func(height uint64, hash types.Hash, next uint64) ([]AssignmentEntry, error) {
		if height == 3 {
			return []AssignmentEntry{{Ordinal: next, GlobalInputIndex: 0, SourceAccountBlockHash: testHash(0x0B), SendMomentumHeight: 3}}, nil
		}
		return []AssignmentEntry{{Ordinal: next, GlobalInputIndex: 1, SourceAccountBlockHash: testHash(0x0A), SendMomentumHeight: 1}}, nil
	}}
	f, err := New(Config{DomainID: 1, ConfirmationDepth: 1, StartHeight: 3, MaxBufferedInputs: 1, Source: source, Assignments: assignments})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	in, ok, err := f.Next()
	if err != nil || !ok || in.Ctx.L1Sender != testAddr(2) {
		t.Fatalf("Next 1: got (sender %s, ok=%v, err=%v), want send b", in.Ctx.L1Sender, ok, err)
	}

	originals := append([]*nom.DetailedMomentum(nil), moms[0:3]...)
	m1 := newMomentum(1, types.Hash{}, moms[0].AccountBlocks)
	m1.Momentum.Hash = testHash(0xA1)
	m2 := newMomentum(2, testHash(0xA1), nil)
	m2.Momentum.Hash = testHash(0xA2)
	m3 := newMomentum(3, testHash(0xA2), moms[2].AccountBlocks)
	m3.Momentum.Hash = testHash(0xA3)
	source.moms[0], source.moms[1], source.moms[2] = m1, m2, m3

	in, ok, err = f.Next()
	if ok || !errors.Is(err, ErrSourceRead) || errors.Is(err, ErrAssignmentFault) {
		t.Fatalf("Next 2: got (%+v, ok=%v, err=%v), want a read failure that is not a fault", in, ok, err)
	}
	if f.nextHeight != 5 {
		t.Fatalf("nextHeight = %d, want 5", f.nextHeight)
	}

	copy(source.moms[0:3], originals)
	in, ok, err = f.Next()
	if err != nil || !ok || in.Ctx.MomentumHeight != 1 || in.Ctx.L1Sender != testAddr(1) {
		t.Fatalf("Next 3: got (sender %s, height %d, ok=%v, err=%v), want send a at height 1", in.Ctx.L1Sender, in.Ctx.MomentumHeight, ok, err)
	}
}

func TestWalkAcrossCalls(t *testing.T) {
	t.Run("short reads", func(t *testing.T) {
		source, assignments := walkChain(8)
		source.perCallCap = 2
		f, err := New(Config{DomainID: 1, ConfirmationDepth: 1, StartHeight: 7, Source: source, Assignments: assignments})
		if err != nil {
			t.Fatalf("New: %v", err)
		}
		checkEmitted(t, drain(t, f), []byte{1, 3, 2}, []uint64{2, 5, 2})
		if want := []uint64{7, 2, 4, 6}; !reflect.DeepEqual(source.heights, want) {
			t.Fatalf("read heights = %v, want %v", source.heights, want)
		}
	})

	t.Run("link broken at a call boundary", func(t *testing.T) {
		source, assignments := walkChain(8)
		source.perCallCap = 2
		source.moms[3].Momentum.PreviousHash = testHash(0xEE)
		f, err := New(Config{DomainID: 1, ConfirmationDepth: 1, StartHeight: 7, Source: source, Assignments: assignments})
		if err != nil {
			t.Fatalf("New: %v", err)
		}
		if _, ok, err := f.Next(); ok || !errors.Is(err, ErrSourceRead) {
			t.Fatalf("Next: got (ok=%v, err=%v), want (false, ErrSourceRead)", ok, err)
		}
	})

	t.Run("range longer than rpcMaxCountSize", func(t *testing.T) {
		source, assignments := walkChain(1100)
		f, err := New(Config{DomainID: 1, ConfirmationDepth: 1, StartHeight: 1099, Source: source, Assignments: assignments})
		if err != nil {
			t.Fatalf("New: %v", err)
		}
		checkEmitted(t, drain(t, f), []byte{1, 3, 2}, []uint64{2, 5, 2})
		if want := []uint64{2, 1024, 74}; !reflect.DeepEqual(source.counts, want) {
			t.Fatalf("read counts = %v, want %v", source.counts, want)
		}
		if want := []uint64{1099, 2, 1026}; !reflect.DeepEqual(source.heights, want) {
			t.Fatalf("read heights = %v, want %v", source.heights, want)
		}
	})
}

func TestWalkServesWholeRead(t *testing.T) {
	cases := []struct {
		name        string
		first       types.Hash
		firstH      uint64
		second      types.Hash
		secondH     uint64
		maxBuffered int
		wantSenders []byte
		wantHeights []uint64
		wantReads   []uint64
		wantCounts  []uint64
	}{
		{
			name: "inside the walked range", first: testHash(0x0A), firstH: 2, second: testHash(0x0B), secondH: 3,
			wantSenders: []byte{1, 2}, wantHeights: []uint64{2, 3},
			wantReads: []uint64{7, 2, 3}, wantCounts: []uint64{2, 6, 1},
		},
		{
			name: "below the walked range", first: testHash(0x0C), firstH: 4, second: testHash(0x0A), secondH: 2,
			wantSenders: []byte{3, 1}, wantHeights: []uint64{4, 2},
			wantReads: []uint64{7, 4, 2}, wantCounts: []uint64{2, 4, 3},
		},
		{
			name: "a new read walks again", first: testHash(0x0A), firstH: 2, second: testHash(0x0B), secondH: 3, maxBuffered: 1,
			wantSenders: []byte{1, 2}, wantHeights: []uint64{2, 3},
			wantReads: []uint64{7, 2, 8, 3}, wantCounts: []uint64{2, 6, 1, 5},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			source, assignments := readChain(tc.first, tc.firstH, tc.second, tc.secondH)
			f, err := New(Config{DomainID: 1, ConfirmationDepth: 1, StartHeight: 7, MaxBufferedInputs: tc.maxBuffered, Source: source, Assignments: assignments})
			if err != nil {
				t.Fatalf("New: %v", err)
			}
			checkEmitted(t, drain(t, f), tc.wantSenders, tc.wantHeights)
			if !reflect.DeepEqual(source.heights, tc.wantReads) {
				t.Fatalf("read heights = %v, want %v", source.heights, tc.wantReads)
			}
			if !reflect.DeepEqual(source.counts, tc.wantCounts) {
				t.Fatalf("read counts = %v, want %v", source.counts, tc.wantCounts)
			}
		})
	}
}

func TestSendMomentumNoLinkedHash(t *testing.T) {
	source := &fakeSource{}
	f, err := New(Config{DomainID: 1, ConfirmationDepth: 1, StartHeight: 1, Source: source, Assignments: &fakeAssignments{src: source}})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	dm := newMomentum(8, momHash(7), nil)
	lk := &linked{low: 5, hashes: []types.Hash{momHash(5)}}
	for _, sh := range []uint64{3, 6} {
		_, err := f.sendMomentum(sh, dm, nil, lk, map[uint64]*nom.DetailedMomentum{})
		if !errors.Is(err, ErrSourceRead) || !strings.Contains(err.Error(), "no linked hash at height") {
			t.Fatalf("sendMomentum(%d) = %v, want a read failure naming no linked hash", sh, err)
		}
	}
	if source.reads != 0 {
		t.Fatalf("source reads = %d, want 0", source.reads)
	}
}

// misnumberedSource answers a read starting at height from with a first momentum numbered
// from+100.
type misnumberedSource struct {
	*fakeSource
	from uint64
}

func (s misnumberedSource) DetailedMomentumsByHeight(height, count uint64) ([]*nom.DetailedMomentum, error) {
	ms, err := s.fakeSource.DetailedMomentumsByHeight(height, count)
	if err != nil || height != s.from || len(ms) == 0 {
		return ms, err
	}
	bad := *ms[0].Momentum
	bad.Height += 100
	first := *ms[0]
	first.Momentum = &bad
	return append([]*nom.DetailedMomentum{&first}, ms[1:]...), nil
}

func TestWalkEdgeChecks(t *testing.T) {
	cases := []struct {
		name  string
		build func() (MomentumSource, *fakeAssignments)
	}{
		{"link broken between the first two walked heights", func() (MomentumSource, *fakeAssignments) {
			source, assignments := walkChain(8)
			source.moms[2].Momentum.PreviousHash = testHash(0xEE)
			return source, assignments
		}},
		{"momentum at the wrong height", func() (MomentumSource, *fakeAssignments) {
			source, assignments := walkChain(8)
			return misnumberedSource{source, 2}, assignments
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			source, assignments := tc.build()
			f, err := New(Config{DomainID: 1, ConfirmationDepth: 1, StartHeight: 7, Source: source, Assignments: assignments})
			if err != nil {
				t.Fatalf("New: %v", err)
			}
			in, ok, err := f.Next()
			if ok || !errors.Is(err, ErrSourceRead) || errors.Is(err, ErrAssignmentFault) {
				t.Fatalf("Next: got (%+v, ok=%v, err=%v), want a read failure that is not a fault", in, ok, err)
			}
			if f.nextHeight != 8 {
				t.Fatalf("nextHeight = %d, want 8", f.nextHeight)
			}
		})
	}
}
