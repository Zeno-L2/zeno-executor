package settlement

import (
	"errors"
	"testing"

	"github.com/zenon-network/go-zenon/common/types"
	"github.com/zenon-network/go-zenon/vm/embedded/definition"

	"github.com/digitalSloth/go-executor/internal/recovery"
	"github.com/digitalSloth/go-executor/internal/znnd/znndtest"
)

type fakeBatches struct {
	cur     definition.InputSequenceCursor
	entries map[uint64]*definition.BatchEntry
	curErr  error
}

func (f *fakeBatches) InputSequenceCursor(uint32) (*definition.InputSequenceCursor, error) {
	if f.curErr != nil {
		return nil, f.curErr
	}
	c := f.cur
	return &c, nil
}

func (f *fakeBatches) BatchEntry(_ uint32, b uint64) (*definition.BatchEntry, bool, error) {
	e, ok := f.entries[b]
	return e, ok, nil
}

const anchorHeight = 100

// batch builds an entry that is finalized at anchorHeight when final is true.
func batch(id uint64, final bool) *definition.BatchEntry {
	e := &definition.BatchEntry{
		PostStateRoot: types.Hash{byte(id + 1)},
		LastInputSeq:  id * 10,
	}
	e.BatchId = id
	if final {
		e.FinalizeAtHeight = anchorHeight
	} else {
		e.FinalizeAtHeight = anchorHeight + 1
	}
	return e
}

func TestFinalizedAnchorWalk(t *testing.T) {
	cases := []struct {
		name    string
		next    uint64
		swept   uint64
		final   []bool // finality by batch id; its length is the number of stored entries
		wantOK  bool
		wantIdx uint64
	}{
		{name: "no batches"},
		{name: "batch 0 unfinalized", next: 1, final: []bool{false}},
		{name: "0 and 1 finalized, 2 not", next: 3, final: []bool{true, true, false}, wantOK: true, wantIdx: 1},
		{name: "1 finalized, 0 not", next: 2, final: []bool{false, true}},
		{name: "0 finalized, 1 not, 2 finalized", next: 3, final: []bool{true, false, true}, wantOK: true, wantIdx: 0},
		{name: "swept through 2, 2 and 3 unfinalized", next: 4, swept: 2, final: []bool{true, true, false, false}, wantOK: true, wantIdx: 1},
		{name: "swept through 2, 2 finalized", next: 4, swept: 2, final: []bool{true, true, true, false}, wantOK: true, wantIdx: 2},
	}
	for _, tc := range cases {
		f := &fakeBatches{
			cur:     definition.InputSequenceCursor{DomainId: 1, NextBatchId: tc.next, RecordsSweptThrough: tc.swept},
			entries: map[uint64]*definition.BatchEntry{},
		}
		for i, fin := range tc.final {
			f.entries[uint64(i)] = batch(uint64(i), fin)
		}
		got, ok, err := finalizedAnchor(f, anchorHeight, 1)
		if err != nil {
			t.Fatalf("%s: %v", tc.name, err)
		}
		if ok != tc.wantOK {
			t.Fatalf("%s: ok = %v, want %v", tc.name, ok, tc.wantOK)
		}
		if ok {
			want := batch(tc.wantIdx, true)
			if got.PostStateRoot != want.PostStateRoot || got.LastInputSeq != want.LastInputSeq {
				t.Fatalf("%s: anchor = %+v, want batch %d", tc.name, got, tc.wantIdx)
			}
		}
	}
}

func TestFinalizedAnchorErrors(t *testing.T) {
	missing := &fakeBatches{
		cur:     definition.InputSequenceCursor{DomainId: 1, NextBatchId: 2},
		entries: map[uint64]*definition.BatchEntry{0: batch(0, true)},
	}
	if _, _, err := finalizedAnchor(missing, anchorHeight, 1); !errors.Is(err, recovery.ErrHalted) {
		t.Fatalf("missing entry: err = %v, want ErrHalted", err)
	}

	sweptMissing := &fakeBatches{
		cur:     definition.InputSequenceCursor{DomainId: 1, NextBatchId: 3, RecordsSweptThrough: 2},
		entries: map[uint64]*definition.BatchEntry{2: batch(2, false)},
	}
	if _, _, err := finalizedAnchor(sweptMissing, anchorHeight, 1); !errors.Is(err, recovery.ErrHalted) {
		t.Fatalf("missing swept entry: err = %v, want ErrHalted", err)
	}

	boom := errors.New("cursor read failed")
	if _, _, err := finalizedAnchor(&fakeBatches{curErr: boom}, anchorHeight, 1); err != boom {
		t.Fatalf("cursor error = %v, want it returned unchanged", err)
	}
}

func TestAnchorSourceGolden(t *testing.T) {
	fx := znndtest.Load(t)
	_, srv, r := newReader(t, 10)
	src := NewAnchorSource(r, 10)

	got, ok, err := src.FinalizedAnchor(fx.DomainID)
	if err != nil || !ok {
		t.Fatalf("FinalizedAnchor(%d) = %+v, %v, %v", fx.DomainID, got, ok, err)
	}
	want := fx.Expected.BatchEntries[0]
	if got.PostStateRoot != want.PostStateRoot || got.LastInputSeq != want.LastInputSeq {
		t.Fatalf("anchor = %+v, want batch 0 (%s, %d)", got, want.PostStateRoot, want.LastInputSeq)
	}

	if _, ok, err := src.FinalizedAnchor(fx.EmptyDomainID); err != nil || ok {
		t.Fatalf("FinalizedAnchor(%d) = %v, %v; want not found", fx.EmptyDomainID, ok, err)
	}

	srv.SetDown(true)
	_, _, err = src.FinalizedAnchor(fx.DomainID)
	mustPlain(t, "down", err)
	srv.SetDown(false)

	srv.TamperProof(func(p *znndtest.Proof) { p.Root[0] ^= 0x01 })
	_, _, err = src.FinalizedAnchor(fx.DomainID)
	mustHalt(t, "tampered root", err)
}
