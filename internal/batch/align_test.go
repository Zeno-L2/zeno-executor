package batch

import (
	"errors"
	"math/big"
	"testing"

	"github.com/zenon-network/go-zenon/common/types"

	"github.com/digitalSloth/go-executor/domain"
	"github.com/digitalSloth/go-executor/internal/codec"
	"github.com/digitalSloth/go-executor/internal/ledger"
)

// alignedFixture is a Driver over a scripted source of plain SUCCESS inputs at the given global
// indices.
type alignedFixture struct {
	t    *testing.T
	d    *Driver
	dom  *fakeDomain
	src  *fakeInputSource
	cfg  Config
	cID  types.Hash
	from types.Hash
}

func newAligned(t *testing.T, indices ...uint64) *alignedFixture {
	t.Helper()
	f := &alignedFixture{t: t, cID: hashN(2), from: hashN(1)}
	f.src = &fakeInputSource{}
	f.dom = &fakeDomain{}
	f.queue(indices...)
	f.cfg = testConfig(t, f.dom, f.src, newFakeBundleStore())
	d, err := New(f.cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	f.d = d
	return f
}

// queue appends successful inputs at the given global indices.
func (f *alignedFixture) queue(indices ...uint64) {
	for _, idx := range indices {
		f.src.inputs = append(f.src.inputs, ledgerInput(idx, callPayload(f.t, f.from, f.cID, codec.AssetID{}, codec.U256{})))
		f.dom.steps = append(f.dom.steps, fakeStep{effects: domain.Effects{
			Status:     codec.StatusSuccess,
			ContractID: f.cID,
			StateDiff:  []domain.StateDiffEntry{{Path: pathN(byte(idx + 1)), Value: []byte{byte(idx)}}},
		}})
	}
}

func (f *alignedFixture) root() types.Hash {
	f.t.Helper()
	r, err := f.cfg.Store.Root()
	if err != nil {
		f.t.Fatalf("Root: %v", err)
	}
	return r
}

func seq(from, to uint64) []uint64 {
	var out []uint64
	for i := from; i <= to; i++ {
		out = append(out, i)
	}
	return out
}

func TestNextBatchToClosesAtTheGivenIndex(t *testing.T) {
	t.Run("ClosesAndContinues", func(t *testing.T) {
		f := newAligned(t, seq(0, 5)...)
		b1, ok, err := f.d.NextBatchTo(2)
		if err != nil || !ok {
			t.Fatalf("NextBatchTo(2): ok=%v err=%v", ok, err)
		}
		if len(b1.Receipts) != 3 || b1.Commitment.FirstInputSeq != 0 || b1.Commitment.LastInputSeq != 2 {
			t.Fatalf("batch 1 = %d records %d..%d, want 3 records 0..2", len(b1.Receipts), b1.Commitment.FirstInputSeq, b1.Commitment.LastInputSeq)
		}
		b2, ok, err := f.d.NextBatchTo(5)
		if err != nil || !ok {
			t.Fatalf("NextBatchTo(5): ok=%v err=%v", ok, err)
		}
		if len(b2.Receipts) != 3 || b2.Commitment.FirstInputSeq != 3 || b2.Commitment.LastInputSeq != 5 {
			t.Fatalf("batch 2 = %d records %d..%d, want 3 records 3..5", len(b2.Receipts), b2.Commitment.FirstInputSeq, b2.Commitment.LastInputSeq)
		}
		if b2.Commitment.BatchID != b1.Commitment.BatchID+1 {
			t.Fatalf("batch ids %d then %d, want consecutive", b1.Commitment.BatchID, b2.Commitment.BatchID)
		}
	})

	t.Run("FirstInputIsTheLastInput", func(t *testing.T) {
		f := newAligned(t, seq(0, 3)...)
		b, ok, err := f.d.NextBatchTo(0)
		if err != nil || !ok {
			t.Fatalf("NextBatchTo(0): ok=%v err=%v", ok, err)
		}
		if len(b.Receipts) != 1 || b.Commitment.LastInputSeq != 0 {
			t.Fatalf("batch = %d records ending at %d, want 1 record ending at 0", len(b.Receipts), b.Commitment.LastInputSeq)
		}
		if f.src.pos != 1 {
			t.Fatalf("source read %d inputs, want 1 (the batch closes before asking again)", f.src.pos)
		}
	})
}

func TestNextBatchToResumesAfterADrainedSource(t *testing.T) {
	f := newAligned(t, 0, 1)
	pre := f.root()

	_, ok, err := f.d.NextBatchTo(3)
	if err != nil || ok {
		t.Fatalf("drained NextBatchTo(3): ok=%v err=%v, want (false, nil)", ok, err)
	}
	if f.root() == pre {
		t.Fatalf("store root did not move: the open records are applied")
	}

	f.queue(2, 3)
	b, ok, err := f.d.NextBatchTo(3)
	if err != nil || !ok {
		t.Fatalf("resumed NextBatchTo(3): ok=%v err=%v", ok, err)
	}
	if len(b.Receipts) != 4 || b.Commitment.FirstInputSeq != 0 || b.Commitment.LastInputSeq != 3 {
		t.Fatalf("batch = %d records %d..%d, want 4 records 0..3", len(b.Receipts), b.Commitment.FirstInputSeq, b.Commitment.LastInputSeq)
	}
	if b.Commitment.PreStateRoot != pre {
		t.Fatalf("PreStateRoot = %x, want the root before input 0 (%x)", b.Commitment.PreStateRoot, pre)
	}
}

func TestNextBatchToSourceErrors(t *testing.T) {
	t.Run("TransientErrorKeepsOpenRecords", func(t *testing.T) {
		f := newAligned(t, 0, 1)
		f.src.errTail = ledger.ErrSourceRead
		pre := f.root()

		_, ok, err := f.d.NextBatchTo(3)
		if ok || !errors.Is(err, ledger.ErrSourceRead) {
			t.Fatalf("NextBatchTo(3): ok=%v err=%v, want ErrSourceRead", ok, err)
		}

		f.queue(2, 3)
		b, ok, err := f.d.NextBatchTo(3)
		if err != nil || !ok {
			t.Fatalf("resumed NextBatchTo(3): ok=%v err=%v", ok, err)
		}
		if len(b.Receipts) != 4 || b.Commitment.PreStateRoot != pre {
			t.Fatalf("batch = %d records pre %x, want 4 records pre %x", len(b.Receipts), b.Commitment.PreStateRoot, pre)
		}
	})

	t.Run("ReorgDropsTheRecords", func(t *testing.T) {
		f := newAligned(t, 0, 1)
		f.src.errTail = &ledger.ReorgError{Height: 4, Want: hashN(3), Got: hashN(4)}
		f.src.permanent = true

		_, ok, err := f.d.NextBatchTo(3)
		var re *ledger.ReorgError
		if ok || !errors.As(err, &re) {
			t.Fatalf("NextBatchTo(3): ok=%v err=%v, want *ledger.ReorgError", ok, err)
		}
		_, ok, err = f.d.NextBatchTo(3)
		if ok || !errors.Is(err, ErrDriverReorged) {
			t.Fatalf("second NextBatchTo: ok=%v err=%v, want ErrDriverReorged", ok, err)
		}
	})
}

func TestNextBatchToMisaligned(t *testing.T) {
	t.Run("IndexSkipsTheTarget", func(t *testing.T) {
		f := newAligned(t, 0, 2, 4)
		_, ok, err := f.d.NextBatchTo(3)
		if ok || !errors.Is(err, ErrMisaligned) {
			t.Fatalf("NextBatchTo(3): ok=%v err=%v, want ErrMisaligned", ok, err)
		}
		if f.dom.idx != 2 {
			t.Fatalf("domain applied %d inputs, want 2: input 4 must not reach the store", f.dom.idx)
		}

		ctl := newAligned(t, 0, 2)
		if _, ok, err := ctl.d.NextBatch(); err != nil || !ok {
			t.Fatalf("control NextBatch: ok=%v err=%v", ok, err)
		}
		if f.root() != ctl.root() {
			t.Fatalf("store root = %x, want the root after input 2 (%x)", f.root(), ctl.root())
		}
	})

	t.Run("FirstInputPastTheTarget", func(t *testing.T) {
		f := newAligned(t, 5)
		_, ok, err := f.d.NextBatchTo(3)
		if ok || !errors.Is(err, ErrMisaligned) {
			t.Fatalf("NextBatchTo(3): ok=%v err=%v, want ErrMisaligned", ok, err)
		}
		if f.dom.idx != 0 {
			t.Fatalf("domain applied %d inputs, want 0", f.dom.idx)
		}
	})

	t.Run("MaxBatchInputsWithoutReachingTheTarget", func(t *testing.T) {
		f := newAligned(t, seq(0, MaxBatchInputs)...)
		_, ok, err := f.d.NextBatchTo(MaxBatchInputs)
		if ok || !errors.Is(err, ErrMisaligned) {
			t.Fatalf("NextBatchTo(%d): ok=%v err=%v, want ErrMisaligned", MaxBatchInputs, ok, err)
		}
	})

	t.Run("MaxBatchInputsEndingAtTheTargetCloses", func(t *testing.T) {
		f := newAligned(t, seq(0, MaxBatchInputs)...)
		b, ok, err := f.d.NextBatchTo(MaxBatchInputs - 1)
		if err != nil || !ok {
			t.Fatalf("NextBatchTo(%d): ok=%v err=%v", MaxBatchInputs-1, ok, err)
		}
		if len(b.Receipts) != MaxBatchInputs {
			t.Fatalf("records = %d, want %d", len(b.Receipts), MaxBatchInputs)
		}
	})
}

// TestNextBatchToIgnoresTheCaps builds the fixture NextBatch splits on a combined cap breach and
// checks the aligned call keeps both records in one batch.
func TestNextBatchToIgnoresTheCaps(t *testing.T) {
	cap := new(big.Int).Lsh(big.NewInt(1), 255)
	asset := assetN(1)
	build := func(t *testing.T, n int) (*Driver, *fakeInputSource) {
		contractID := hashN(2)
		src := hashN(1)
		inputs := make([]ledger.Input, n)
		steps := make([]fakeStep, n)
		for i := 0; i < n; i++ {
			inputs[i] = ledgerInput(uint64(i), callPayload(t, src, contractID, codec.AssetID{}, codec.U256{}))
			steps[i] = fakeStep{effects: domain.Effects{Status: codec.StatusSuccess, ContractID: contractID}}
		}
		steps[0].effects = withdrawalEffects(contractID, 0, asset, u256Big(new(big.Int).Sub(cap, big.NewInt(100))))
		steps[1].effects = withdrawalEffects(contractID, 1, asset, u256N(200))
		fsrc := &fakeInputSource{inputs: inputs}
		d, err := New(testConfig(t, &fakeDomain{steps: steps}, fsrc, newFakeBundleStore()))
		if err != nil {
			t.Fatalf("New: %v", err)
		}
		return d, fsrc
	}

	t.Run("BreachDoesNotClose", func(t *testing.T) {
		d, _ := build(t, 2)
		b, ok, err := d.NextBatchTo(1)
		if err != nil || !ok {
			t.Fatalf("NextBatchTo(1): ok=%v err=%v", ok, err)
		}
		if len(b.Receipts) != 2 {
			t.Fatalf("records = %d, want 2: an aligned batch ignores the withdrawal cap", len(b.Receipts))
		}
	})

	t.Run("CarriedRecordStartsAnAlignedBatch", func(t *testing.T) {
		d, _ := build(t, 3)
		b1, ok, err := d.NextBatch()
		if err != nil || !ok || len(b1.Receipts) != 1 {
			t.Fatalf("NextBatch: ok=%v err=%v records=%d, want 1 record", ok, err, len(b1.Receipts))
		}
		b2, ok, err := d.NextBatchTo(2)
		if err != nil || !ok {
			t.Fatalf("NextBatchTo(2): ok=%v err=%v", ok, err)
		}
		if len(b2.Receipts) != 2 || b2.Commitment.FirstInputSeq != 1 || b2.Commitment.LastInputSeq != 2 {
			t.Fatalf("batch = %d records %d..%d, want 2 records 1..2", len(b2.Receipts), b2.Commitment.FirstInputSeq, b2.Commitment.LastInputSeq)
		}
	})

	t.Run("CarriedRecordPastTheTargetIsMisaligned", func(t *testing.T) {
		d, _ := build(t, 2)
		if _, ok, err := d.NextBatch(); err != nil || !ok {
			t.Fatalf("NextBatch: ok=%v err=%v", ok, err)
		}
		_, ok, err := d.NextBatchTo(0)
		if ok || !errors.Is(err, ErrMisaligned) {
			t.Fatalf("NextBatchTo(0): ok=%v err=%v, want ErrMisaligned", ok, err)
		}
	})
}
