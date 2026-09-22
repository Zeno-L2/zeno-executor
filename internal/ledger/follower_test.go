package ledger

import (
	"bytes"
	"encoding/binary"
	"errors"
	"math"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/zenon-network/go-zenon/chain/nom"
	"github.com/zenon-network/go-zenon/common/types"
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

// fakeFilter is a func-typed SettlementFilter that counts its calls.
type fakeFilter struct {
	calls int
	fn    func(m *nom.Momentum, block *nom.AccountBlock) (*Envelope, error)
}

func (f *fakeFilter) Accept(m *nom.Momentum, block *nom.AccountBlock) (*Envelope, error) {
	f.calls++
	return f.fn(m, block)
}

func acceptDomain(domainID uint32) *fakeFilter {
	return &fakeFilter{fn: func(m *nom.Momentum, block *nom.AccountBlock) (*Envelope, error) {
		return &Envelope{DomainID: domainID, Payload: []byte{block.Address[0]}}, nil
	}}
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
	// Non-canonical AccountBlocks order: addresses 3, 1, 2. Header-bytes ascending sorts by
	// address first, giving 1, 2, 3 — distinct from the supplied order and from sorting by
	// hash alone (hashes are supplied in the reverse of address order).
	blocks := []*nom.AccountBlock{
		newBlock(3, 1, 1, nil),
		newBlock(1, 1, 3, nil),
		newBlock(2, 1, 2, nil),
	}
	supplied := append([]*nom.AccountBlock(nil), blocks...)

	build := func() *fakeSource {
		moms := chain(1, 1, func(h uint64) []*nom.AccountBlock { return blocks })
		return &fakeSource{frontier: 2, moms: moms}
	}

	run := func() []Input {
		f, err := New(Config{DomainID: 1, ConfirmationDepth: 1, StartHeight: 1, Source: build(), Filter: acceptDomain(1)})
		if err != nil {
			t.Fatalf("New: %v", err)
		}
		return drain(t, f)
	}

	got1 := run()
	got2 := run()

	wantAddrs := []byte{1, 2, 3}
	if len(got1) != len(wantAddrs) {
		t.Fatalf("got %d inputs, want %d", len(got1), len(wantAddrs))
	}
	for i, want := range wantAddrs {
		if got1[i].Payload[0] != want {
			t.Fatalf("input %d: got address %d, want %d", i, got1[i].Payload[0], want)
		}
		if got1[i].Ctx.GlobalInputIndex != uint64(i) {
			t.Fatalf("input %d: got index %d, want %d", i, got1[i].Ctx.GlobalInputIndex, i)
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
	// Five blocks, addresses ascending so header order equals supplied order, alternating
	// domain A (0), B (1), A, B, A via each block's Data byte.
	blocks := []*nom.AccountBlock{
		newBlock(1, 1, 1, []byte{0}),
		newBlock(2, 1, 2, []byte{1}),
		newBlock(3, 1, 3, []byte{0}),
		newBlock(4, 1, 4, []byte{1}),
		newBlock(5, 1, 5, []byte{0}),
	}
	moms := chain(1, 1, func(h uint64) []*nom.AccountBlock { return blocks })
	source := &fakeSource{frontier: 2, moms: moms}
	filter := &fakeFilter{fn: func(m *nom.Momentum, block *nom.AccountBlock) (*Envelope, error) {
		return &Envelope{DomainID: uint32(block.Data[0]), Payload: []byte{block.Address[0]}}, nil
	}}

	f, err := New(Config{DomainID: 0, ConfirmationDepth: 1, StartHeight: 1, StartGlobalIndex: 5, Source: source, Filter: filter})
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
}

func TestNextConfirmationFrontier(t *testing.T) {
	oneBlock := func(h uint64) []*nom.AccountBlock {
		return []*nom.AccountBlock{newBlock(1, h, 1, nil)}
	}

	t.Run("well-behaved source", func(t *testing.T) {
		moms := chain(1, 10, oneBlock)
		source := &fakeSource{frontier: 10, moms: moms}
		f, err := New(Config{DomainID: 1, ConfirmationDepth: 3, StartHeight: 1, Source: source, Filter: acceptDomain(1)})
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
		f, err := New(Config{DomainID: 1, ConfirmationDepth: 3, StartHeight: 1, Source: source, Filter: acceptDomain(1)})
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

func TestNextOverflow(t *testing.T) {
	oneBlock := func(h uint64) []*nom.AccountBlock {
		return []*nom.AccountBlock{newBlock(1, h, 1, nil)}
	}
	moms := chain(1, 3, oneBlock)
	source := &fakeSource{frontier: 4, moms: moms}
	f, err := New(Config{DomainID: 1, ConfirmationDepth: 1, StartHeight: 1, StartGlobalIndex: math.MaxUint64 - 1, Source: source, Filter: acceptDomain(1)})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	in1, ok, err := f.Next()
	if err != nil || !ok || in1.Ctx.GlobalInputIndex != math.MaxUint64-1 {
		t.Fatalf("Next 1: got (idx=%d, ok=%v, err=%v), want (%d, true, nil)", in1.Ctx.GlobalInputIndex, ok, err, uint64(math.MaxUint64-1))
	}
	in2, ok, err := f.Next()
	if err != nil || !ok || in2.Ctx.GlobalInputIndex != math.MaxUint64 {
		t.Fatalf("Next 2: got (idx=%d, ok=%v, err=%v), want (%d, true, nil)", in2.Ctx.GlobalInputIndex, ok, err, uint64(math.MaxUint64))
	}
	if _, ok, err := f.Next(); ok || !errors.Is(err, ErrIndexOverflow) {
		t.Fatalf("Next 3: got (ok=%v, err=%v), want (false, ErrIndexOverflow)", ok, err)
	}
	if _, ok, err := f.Next(); ok || !errors.Is(err, ErrIndexOverflow) {
		t.Fatalf("Next 4: got (ok=%v, err=%v), want (false, ErrIndexOverflow)", ok, err)
	}
}

func TestNextRejectedBlockAndReorg(t *testing.T) {
	m1blocks := []*nom.AccountBlock{
		newBlock(1, 1, 1, []byte{0}), // rejected
		newBlock(2, 1, 2, []byte{1}), // accepted
	}
	m2blocks := []*nom.AccountBlock{newBlock(1, 2, 1, []byte{1})}
	m3blocks := []*nom.AccountBlock{newBlock(1, 3, 1, []byte{1})}

	moms := []*nom.DetailedMomentum{
		newMomentum(1, types.Hash{}, m1blocks),
		newMomentum(2, momHash(1), m2blocks),
		newMomentum(3, testHash(0xAA), m3blocks), // wrong PreviousHash: reorg
	}
	source := &fakeSource{frontier: 5, moms: moms}
	filter := &fakeFilter{fn: func(m *nom.Momentum, block *nom.AccountBlock) (*Envelope, error) {
		if block.Data[0] == 0 {
			return nil, nil
		}
		return &Envelope{DomainID: 1, Payload: []byte{block.Address[0]}}, nil
	}}

	f, err := New(Config{DomainID: 1, ConfirmationDepth: 1, StartHeight: 1, Source: source, Filter: filter})
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
			return []*nom.AccountBlock{newBlock(1, h, 1, nil)}
		}
		return []*nom.AccountBlock{newBlock(1, h, 1, nil), newBlock(2, h, 2, nil)}
	}
	moms := chain(1, 3, blocksFor)
	source := &fakeSource{frontier: 4, moms: moms}
	filter := &fakeFilter{fn: func(m *nom.Momentum, block *nom.AccountBlock) (*Envelope, error) {
		if m.Height == 3 {
			return &Envelope{DomainID: 1, Payload: []byte{block.Address[0]}}, nil
		}
		return &Envelope{DomainID: 99, Payload: nil}, nil
	}}

	f, err := New(Config{DomainID: 1, ConfirmationDepth: 1, StartHeight: 1, Source: source, Filter: filter})
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
	m1blocks := []*nom.AccountBlock{newBlock(1, 1, 1, nil), newBlock(2, 1, 2, nil)}
	m2blocks := []*nom.AccountBlock{newBlock(1, 2, 1, nil)}
	moms := []*nom.DetailedMomentum{
		newMomentum(1, types.Hash{}, m1blocks),
		newMomentum(2, testHash(0xAA), m2blocks), // wrong PreviousHash
	}
	source := &fakeSource{frontier: 3, moms: moms}

	f, err := New(Config{DomainID: 1, ConfirmationDepth: 1, StartHeight: 1, Source: source, Filter: acceptDomain(1)})
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
		return []*nom.AccountBlock{newBlock(1, h, 1, nil)}
	}
	moms := chain(1, 5, oneBlock)
	source := &fakeSource{frontier: 6, moms: moms, perCallCap: 2}

	f, err := New(Config{DomainID: 1, ConfirmationDepth: 1, StartHeight: 1, Source: source, Filter: acceptDomain(1)})
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
	sentinel := errors.New("boom")

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
			f, err := New(Config{DomainID: 1, ConfirmationDepth: 1, StartHeight: 1, Source: source, Filter: acceptDomain(1)})
			if err != nil {
				t.Fatalf("New: %v", err)
			}
			if _, ok, err := f.Next(); ok || err == nil {
				t.Fatalf("Next: got (ok=%v, err=%v), want an error", ok, err)
			}
		})
	}

	t.Run("Accept returns sentinel", func(t *testing.T) {
		moms := []*nom.DetailedMomentum{newMomentum(1, types.Hash{}, []*nom.AccountBlock{newBlock(1, 1, 1, nil)})}
		source := &fakeSource{frontier: 5, moms: moms}
		filter := &fakeFilter{fn: func(m *nom.Momentum, block *nom.AccountBlock) (*Envelope, error) {
			return nil, sentinel
		}}
		f, err := New(Config{DomainID: 1, ConfirmationDepth: 1, StartHeight: 1, Source: source, Filter: filter})
		if err != nil {
			t.Fatalf("New: %v", err)
		}
		_, ok, err := f.Next()
		if ok || !errors.Is(err, sentinel) {
			t.Fatalf("Next: got (ok=%v, err=%v), want (false, wrapped sentinel)", ok, err)
		}
	})
}

func TestNextEmptyMomentum(t *testing.T) {
	moms := []*nom.DetailedMomentum{
		newMomentum(1, types.Hash{}, nil),
		newMomentum(2, momHash(1), []*nom.AccountBlock{newBlock(1, 2, 1, nil)}),
	}
	source := &fakeSource{frontier: 3, moms: moms}

	f, err := New(Config{DomainID: 1, ConfirmationDepth: 1, StartHeight: 1, Source: source, Filter: acceptDomain(1)})
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
	valid := Config{DomainID: 1, ConfirmationDepth: 1, StartHeight: 1, Source: &fakeSource{}, Filter: acceptDomain(1)}

	cases := []struct {
		name string
		cfg  Config
	}{
		{"nil Source", func() Config { c := valid; c.Source = nil; return c }()},
		{"nil Filter", func() Config { c := valid; c.Filter = nil; return c }()},
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
		return []*nom.AccountBlock{newBlock(1, h, 1, nil)}
	}
	moms := chain(1, 5, oneBlock)
	source := &fakeSource{frontier: 6, moms: moms}

	f, err := New(Config{DomainID: 1, ConfirmationDepth: 1, StartHeight: 1, Source: source, Filter: acceptDomain(1)})
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

func TestNextAcceptErrorLatches(t *testing.T) {
	sentinel := errors.New("boom")
	blocks := []*nom.AccountBlock{
		newBlock(1, 1, 1, nil),
		newBlock(2, 1, 2, nil),
		newBlock(3, 1, 3, nil),
	}
	moms := []*nom.DetailedMomentum{newMomentum(1, types.Hash{}, blocks)}
	source := &fakeSource{frontier: 2, moms: moms}

	filter := &fakeFilter{fn: func(m *nom.Momentum, block *nom.AccountBlock) (*Envelope, error) {
		if block.Address[0] == 3 {
			return nil, sentinel
		}
		return &Envelope{DomainID: 1, Payload: []byte{block.Address[0]}}, nil
	}}

	f, err := New(Config{DomainID: 1, ConfirmationDepth: 1, StartHeight: 1, Source: source, Filter: filter})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	_, ok, err := f.Next()
	if ok || !errors.Is(err, sentinel) {
		t.Fatalf("Next 1: got (ok=%v, err=%v), want (false, wrapped sentinel)", ok, err)
	}

	callsBefore, readsBefore := filter.calls, source.reads
	_, ok, err = f.Next()
	if ok || !errors.Is(err, sentinel) {
		t.Fatalf("Next 2: got (ok=%v, err=%v), want (false, wrapped sentinel)", ok, err)
	}
	if filter.calls != callsBefore || source.reads != readsBefore {
		t.Fatalf("Next 2: calls/reads changed: got (%d, %d), want (%d, %d)", filter.calls, source.reads, callsBefore, readsBefore)
	}
}

func TestNextTransientReadError(t *testing.T) {
	t.Run("DetailedMomentumsByHeight fails once", func(t *testing.T) {
		sentinel := errors.New("transient")
		oneBlock := func(h uint64) []*nom.AccountBlock {
			return []*nom.AccountBlock{newBlock(1, h, 1, nil)}
		}
		moms := chain(1, 3, oneBlock)
		source := &fakeSource{frontier: 4, moms: moms, readErr: sentinel}

		f, err := New(Config{DomainID: 1, ConfirmationDepth: 1, StartHeight: 1, Source: source, Filter: acceptDomain(1)})
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
			return []*nom.AccountBlock{newBlock(1, h, 1, nil)}
		}
		moms := chain(1, 3, oneBlock)
		source := &fakeSource{frontier: 4, moms: moms, frontierErr: sentinel}

		f, err := New(Config{DomainID: 1, ConfirmationDepth: 1, StartHeight: 1, Source: source, Filter: acceptDomain(1)})
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
		return []*nom.AccountBlock{newBlock(1, h, 1, nil)}
	}
	moms := chain(1, 3, oneBlock)
	source := &fakeSource{frontier: 2, moms: moms}

	f, err := New(Config{DomainID: 1, ConfirmationDepth: 5, StartHeight: 1, Source: source, Filter: acceptDomain(1)})
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
	f, err := New(Config{DomainID: 1, ConfirmationDepth: 1, StartHeight: 1, Source: source, Filter: acceptDomain(1)})
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

func TestNextExhaustedWithoutHalted(t *testing.T) {
	moms := chain(1, 1, func(h uint64) []*nom.AccountBlock {
		return []*nom.AccountBlock{newBlock(1, h, 1, nil)}
	})
	source := &fakeSource{frontier: 2, moms: moms}

	f, err := New(Config{DomainID: 1, ConfirmationDepth: 1, StartHeight: 1, StartGlobalIndex: math.MaxUint64, Source: source, Filter: acceptDomain(1)})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	in, ok, err := f.Next()
	if err != nil || !ok || in.Ctx.GlobalInputIndex != math.MaxUint64 {
		t.Fatalf("Next 1: got (idx=%d, ok=%v, err=%v), want (%d, true, nil)", in.Ctx.GlobalInputIndex, ok, err, uint64(math.MaxUint64))
	}
	// The momentum that assigned MaxUint64 committed successfully — counter.exhausted is set
	// by assign itself, not by a later momentum's failed processing, so halted stays nil.
	if f.halted != nil {
		t.Fatalf("after Next 1: halted = %v, want nil", f.halted)
	}
	if !f.counter.exhausted {
		t.Fatal("after Next 1: counter.exhausted = false, want true")
	}

	if _, ok, err := f.Next(); ok || !errors.Is(err, ErrIndexOverflow) {
		t.Fatalf("Next 2: got (ok=%v, err=%v), want (false, ErrIndexOverflow)", ok, err)
	}
	if f.halted != nil {
		t.Fatalf("after Next 2: halted = %v, want nil", f.halted)
	}
	if _, ok, err := f.Next(); ok || !errors.Is(err, ErrIndexOverflow) {
		t.Fatalf("Next 3: got (ok=%v, err=%v), want (false, ErrIndexOverflow)", ok, err)
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

	f, err := New(Config{DomainID: 1, ConfirmationDepth: 1, StartHeight: 1, Source: source, Filter: acceptDomain(1)})
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

	f, err := New(Config{DomainID: 1, ConfirmationDepth: 1, StartHeight: 1, Source: source, Filter: acceptDomain(1)})
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

	f, err := New(Config{DomainID: 1, ConfirmationDepth: 1, StartHeight: 1, Source: source, Filter: acceptDomain(1)})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if _, ok, err := f.Next(); ok || err == nil || !strings.Contains(err.Error(), "nil content entry") {
		t.Fatalf("Next: got (ok=%v, err=%v), want a nil-content-entry error", ok, err)
	}
}

func TestUnwiredFilterAccept(t *testing.T) {
	moms := chain(1, 1, func(h uint64) []*nom.AccountBlock {
		return []*nom.AccountBlock{newBlock(1, h, 1, nil)}
	})
	source := &fakeSource{frontier: 2, moms: moms}

	f, err := New(Config{DomainID: 1, ConfirmationDepth: 1, StartHeight: 1, Source: source, Filter: UnwiredFilter{}})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if _, ok, err := f.Next(); ok || !errors.Is(err, ErrSettlementUnwired) {
		t.Fatalf("Next: got (ok=%v, err=%v), want (false, wrapped ErrSettlementUnwired)", ok, err)
	}
}

// --- checkpoint, recent momenta and source-read errors ---

// domainByData accepts every block, assigning it to the domain named by its first Data byte.
func domainByData() *fakeFilter {
	return &fakeFilter{fn: func(m *nom.Momentum, block *nom.AccountBlock) (*Envelope, error) {
		return &Envelope{DomainID: uint32(block.Data[0]), Payload: []byte{block.Address[0]}}, nil
	}}
}

func TestCheckpointBoundaryAndResume(t *testing.T) {
	// Momentum 1 holds two blocks for domain 0 and one for domain 1, in header order, so the
	// other domain's block follows this domain's last one. Momentum 2 holds one more for domain 0.
	moms := chain(1, 3, func(h uint64) []*nom.AccountBlock {
		switch h {
		case 1:
			return []*nom.AccountBlock{
				newBlock(1, 1, 1, []byte{0}),
				newBlock(2, 1, 2, []byte{0}),
				newBlock(3, 1, 3, []byte{1}),
			}
		case 2:
			return []*nom.AccountBlock{newBlock(4, 2, 4, []byte{0})}
		}
		return nil
	})
	// The frontier confirms momentum 1 only, so the follower's buffer holds exactly its inputs.
	source := &fakeSource{frontier: 2, moms: moms}

	f, err := New(Config{DomainID: 0, ConfirmationDepth: 1, StartHeight: 1, Source: source, Filter: domainByData()})
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
	// Indices 0 and 1 are domain 0's, index 2 is domain 1's: the next block takes index 3.
	if cp.Height != 1 || cp.Hash != momHash(1) || cp.NextGlobalIndex != 3 {
		t.Fatalf("Checkpoint = %+v, want height 1, hash of 1, next index 3", cp)
	}

	source.frontier = 10
	rest := drain(t, f)

	resumed, err := New(Config{
		DomainID: 0, ConfirmationDepth: 1, StartHeight: cp.Height + 1, StartGlobalIndex: cp.NextGlobalIndex,
		Source: &fakeSource{frontier: 10, moms: moms}, Filter: domainByData(),
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
			return []*nom.AccountBlock{newBlock(byte(h), h, byte(h), []byte{0})}
		}
		return nil
	})
	source := &fakeSource{frontier: 13, moms: moms}
	f, err := New(Config{DomainID: 0, ConfirmationDepth: 3, StartHeight: 1, Source: source, Filter: domainByData()})
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
		return []*nom.AccountBlock{newBlock(byte(h), h, byte(h), []byte{0})}
	})

	t.Run("frontier read failure", func(t *testing.T) {
		source := &fakeSource{frontier: 10, moms: moms, frontierErr: errors.New("boom")}
		f, _ := New(Config{DomainID: 0, ConfirmationDepth: 1, StartHeight: 1, Source: source, Filter: domainByData()})
		if _, _, err := f.Next(); !errors.Is(err, ErrSourceRead) {
			t.Fatalf("frontier failure: err = %v, want ErrSourceRead", err)
		}
	})
	t.Run("momenta read failure", func(t *testing.T) {
		source := &fakeSource{frontier: 10, moms: moms, readErr: errors.New("boom")}
		f, _ := New(Config{DomainID: 0, ConfirmationDepth: 1, StartHeight: 1, Source: source, Filter: domainByData()})
		if _, _, err := f.Next(); !errors.Is(err, ErrSourceRead) {
			t.Fatalf("momenta failure: err = %v, want ErrSourceRead", err)
		}
		if _, ok, err := f.Next(); err != nil || !ok {
			t.Fatalf("retry after a source failure: ok=%v err=%v", ok, err)
		}
	})
	t.Run("latched failure is not a source read error", func(t *testing.T) {
		source := &fakeSource{frontier: 10, moms: moms}
		f, _ := New(Config{DomainID: 0, ConfirmationDepth: 1, StartHeight: 1, Source: source,
			Filter: &fakeFilter{fn: func(*nom.Momentum, *nom.AccountBlock) (*Envelope, error) { return nil, errors.New("reject") }}})
		_, _, err := f.Next()
		if err == nil || errors.Is(err, ErrSourceRead) {
			t.Fatalf("latched failure: err = %v, want a non-ErrSourceRead error", err)
		}
	})
	t.Run("reorg is not a source read error", func(t *testing.T) {
		source := &fakeSource{frontier: 2, moms: moms}
		f, _ := New(Config{DomainID: 0, ConfirmationDepth: 1, StartHeight: 1, Source: source, Filter: domainByData()})
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

// twoBlocksPerMomentum gives every momentum two blocks whose payloads identify height and
// address, so a lost or duplicated input is visible in the drained sequence.
func twoBlocksPerMomentum(h uint64) []*nom.AccountBlock {
	return []*nom.AccountBlock{
		newBlock(1, h, 1, []byte{byte(h), 1}),
		newBlock(2, h, 2, []byte{byte(h), 2}),
	}
}

func payloadFilter(domainID uint32) *fakeFilter {
	return &fakeFilter{fn: func(m *nom.Momentum, block *nom.AccountBlock) (*Envelope, error) {
		return &Envelope{DomainID: domainID, Payload: block.Data}, nil
	}}
}

func newBoundedFollower(t *testing.T, source *fakeSource, bound int, depth uint64) *Follower {
	t.Helper()
	f, err := New(Config{
		DomainID:          1,
		ConfirmationDepth: depth,
		StartHeight:       1,
		MaxBufferedInputs: bound,
		Source:            source,
		Filter:            payloadFilter(1),
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
		return []*nom.AccountBlock{newBlock(1, h, 1, []byte{byte(h)})}
	})
	moms[1] = newMomentum(2, testHash(0xAA), []*nom.AccountBlock{newBlock(1, 2, 1, []byte{2})})
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
