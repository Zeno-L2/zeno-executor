package batch

import (
	"bytes"
	"errors"
	"fmt"
	"math/big"
	"path/filepath"
	"testing"

	"github.com/zenon-network/go-zenon/common/crypto"
	"github.com/zenon-network/go-zenon/common/trie"
	"github.com/zenon-network/go-zenon/common/types"

	"github.com/digitalSloth/go-executor/domain"
	"github.com/digitalSloth/go-executor/internal/codec"
	"github.com/digitalSloth/go-executor/internal/ledger"
	"github.com/digitalSloth/go-executor/internal/state"
)

// --- fakes ---

// fakeStep is one input's scripted DecodeInput/Apply outcome: an error from either call, the
// paths Apply should read against the accessor (to produce witnesses), and the Effects Apply
// returns.
type fakeStep struct {
	decodeErr error
	reads     []trie.Path
	applyErr  error
	effects   domain.Effects
}

// fakeDomain is a scripted domain.Domain: DecodeInput/Apply pairs advance through steps in
// order, one pair per input, in the style of internal/ledger/follower_test.go's fakes.
type fakeDomain struct {
	steps []fakeStep
	idx   int
}

func (d *fakeDomain) Genesis() domain.StateInit { return domain.StateInit{} }

func (d *fakeDomain) DecodeInput(ctx domain.InputContext, payload []byte) (domain.Input, error) {
	s := d.steps[d.idx]
	if s.decodeErr != nil {
		return nil, s.decodeErr
	}
	return payload, nil
}

func (d *fakeDomain) Apply(acc domain.StateAccess, in domain.Input) (domain.Effects, error) {
	s := d.steps[d.idx]
	d.idx++
	for _, p := range s.reads {
		if _, _, err := acc.Read(p); err != nil {
			panic(err)
		}
	}
	if s.applyErr != nil {
		return domain.Effects{}, s.applyErr
	}
	return s.effects, nil
}

// fakeInputSource serves a fixed input list, then errTail once nothing remains — repeatedly if
// permanent, else cleared after the first read, exactly as the real MomentumSource classes the
// Driver's error contract distinguishes.
type fakeInputSource struct {
	inputs    []ledger.Input
	pos       int
	errTail   error
	permanent bool
}

func (s *fakeInputSource) Next() (ledger.Input, bool, error) {
	if s.pos < len(s.inputs) {
		in := s.inputs[s.pos]
		s.pos++
		return in, true, nil
	}
	if s.errTail != nil {
		err := s.errTail
		if !s.permanent {
			s.errTail = nil
		}
		return ledger.Input{}, false, err
	}
	return ledger.Input{}, false, nil
}

// fakeBundleStore is an in-memory BundleStore; flipByte, when true, makes Get return the stored
// bundle with its first byte flipped, to exercise the DAHash round-trip check.
type fakeBundleStore struct {
	m        map[types.Hash][]byte
	flipByte bool
}

func newFakeBundleStore() *fakeBundleStore { return &fakeBundleStore{m: map[types.Hash][]byte{}} }

func (s *fakeBundleStore) Put(daHash types.Hash, bundle []byte) error {
	s.m[daHash] = append([]byte(nil), bundle...)
	return nil
}

func (s *fakeBundleStore) Get(daHash types.Hash) ([]byte, error) {
	b, ok := s.m[daHash]
	if !ok {
		return nil, fmt.Errorf("fakeBundleStore: no bundle for %x", daHash)
	}
	out := append([]byte(nil), b...)
	if s.flipByte && len(out) > 0 {
		out[0] ^= 0xFF
	}
	return out, nil
}

// --- helpers ---

func openTestStore(t *testing.T) *state.Store {
	t.Helper()
	s, err := state.Open(filepath.Join(t.TempDir(), "state.wal"))
	if err != nil {
		t.Fatalf("state.Open: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func testConfig(t *testing.T, dom domain.Domain, inputs InputSource, bundles BundleStore) Config {
	t.Helper()
	return Config{
		DomainID:     7,
		ExecutorID:   types.Address{9},
		Domain:       dom,
		Store:        openTestStore(t),
		Inputs:       inputs,
		Bundles:      bundles,
		FirstBatchID: 100,
	}
}

func hashN(b byte) types.Hash {
	var h types.Hash
	h[0] = b
	return h
}

func pathN(b byte) trie.Path {
	var p trie.Path
	p[0] = b
	return p
}

func assetN(b byte) codec.AssetID {
	var a codec.AssetID
	a[31] = b
	return a
}

func u256Big(v *big.Int) codec.U256 {
	var out codec.U256
	b := v.Bytes()
	copy(out[32-len(b):], b)
	return out
}

func u256N(v uint64) codec.U256 { return u256Big(big.NewInt(int64(v))) }

// callPayload builds a canonical kind-0 (CallL2) input payload.
func callPayload(t *testing.T, src, target types.Hash, asset codec.AssetID, amount codec.U256) []byte {
	t.Helper()
	in := codec.Input{
		SourceAccountBlockHash: src,
		Args:                   codec.CallL2Args{TargetContract: target, PlasmaLimit: 0, DepositAsset: asset, DepositAmount: amount},
	}
	b, err := in.Encode()
	if err != nil {
		t.Fatalf("encode input: %v", err)
	}
	return b
}

// unsupportedPayload is a raw input_kind || source_account_block_hash prefix for a kind
// outside 0..5, with no defined tail.
func unsupportedPayload(kind byte, src types.Hash) []byte {
	b := make([]byte, 33)
	b[0] = kind
	copy(b[1:], src[:])
	return b
}

func ledgerInput(idx uint64, payload []byte) ledger.Input {
	return ledger.Input{Ctx: domain.InputContext{GlobalInputIndex: idx}, Payload: payload}
}

// withdrawalEffects is a SUCCESS Effects carrying one contract-emitted L1Withdrawal.
func withdrawalEffects(contractID types.Hash, idx uint64, asset codec.AssetID, amount codec.U256) domain.Effects {
	return domain.Effects{
		Status:     codec.StatusSuccess,
		ContractID: contractID,
		OutboxMessages: []codec.OutboxMessage{
			{SourceContract: contractID, InputIndex: idx, OutboxIndex: 0, Target: codec.L1Withdrawal{RecipientL1: types.Address{1}, Asset: asset, Amount: amount}},
		},
	}
}

// manyWithdrawalsEffects is a SUCCESS Effects carrying one contract-emitted L1Withdrawal per
// asset in assets, each of amount.
func manyWithdrawalsEffects(contractID types.Hash, idx uint64, assets []codec.AssetID, amount codec.U256) domain.Effects {
	msgs := make([]codec.OutboxMessage, len(assets))
	for i, a := range assets {
		msgs[i] = codec.OutboxMessage{SourceContract: contractID, InputIndex: idx, OutboxIndex: uint32(i), Target: codec.L1Withdrawal{RecipientL1: types.Address{1}, Asset: a, Amount: amount}}
	}
	return domain.Effects{Status: codec.StatusSuccess, ContractID: contractID, OutboxMessages: msgs}
}

// refundEffects is the failure shape: no contract content, one runtime-emitted refund (edge
// case 7).
func refundEffects(idx uint64, asset codec.AssetID, amount codec.U256) domain.Effects {
	return domain.Effects{
		Status: codec.StatusRuntimeFault,
		OutboxMessages: []codec.OutboxMessage{
			{SourceContract: types.Hash{}, InputIndex: idx, OutboxIndex: 0, Target: codec.L1Withdrawal{RecipientL1: types.Address{1}, Asset: asset, Amount: amount}},
		},
	}
}

// --- test 1: replay determinism ---

func TestDriverReplayDeterminism(t *testing.T) {
	contractID := hashN(2)
	src := hashN(1)

	buildInputsAndDomain := func() (*fakeDomain, []ledger.Input) {
		inputs := []ledger.Input{
			ledgerInput(0, callPayload(t, src, contractID, codec.AssetID{}, codec.U256{})),
			ledgerInput(1, callPayload(t, src, contractID, codec.AssetID{}, codec.U256{})),
		}
		dom := &fakeDomain{steps: []fakeStep{
			{effects: withdrawalEffects(contractID, 0, assetN(1), u256N(10))},
			{effects: withdrawalEffects(contractID, 1, assetN(1), u256N(20))},
		}}
		return dom, inputs
	}

	run := func() Batch {
		dom, inputs := buildInputsAndDomain()
		cfg := testConfig(t, dom, &fakeInputSource{inputs: inputs}, newFakeBundleStore())
		d, err := New(cfg)
		if err != nil {
			t.Fatalf("New: %v", err)
		}
		got, ok, err := d.NextBatch()
		if err != nil || !ok {
			t.Fatalf("NextBatch: ok=%v err=%v", ok, err)
		}
		return got
	}

	a := run()
	b := run()

	if a.Commitment.PreStateRoot != b.Commitment.PreStateRoot || a.Commitment.PostStateRoot != b.Commitment.PostStateRoot {
		t.Fatalf("roots differ:\n a = %+v\n b = %+v", a.Commitment, b.Commitment)
	}
	if !bytes.Equal(a.Bundle, b.Bundle) {
		t.Fatalf("Bundle differs:\n a = %x\n b = %x", a.Bundle, b.Bundle)
	}
	if a.Commitment.DAHash != b.Commitment.DAHash {
		t.Fatalf("DAHash differs: %x vs %x", a.Commitment.DAHash, b.Commitment.DAHash)
	}
	if len(a.Receipts) != len(b.Receipts) {
		t.Fatalf("receipt count differs: %d vs %d", len(a.Receipts), len(b.Receipts))
	}
	for i := range a.Receipts {
		ea, err := a.Receipts[i].Encode()
		if err != nil {
			t.Fatalf("encode receipt %d (a): %v", i, err)
		}
		eb, err := b.Receipts[i].Encode()
		if err != nil {
			t.Fatalf("encode receipt %d (b): %v", i, err)
		}
		if !bytes.Equal(ea, eb) {
			t.Fatalf("receipt %d differs:\n a = %x\n b = %x", i, ea, eb)
		}
	}
}

// --- test 2: DAHash round-trip ---

func TestDriverDAHashRoundTripMismatch(t *testing.T) {
	contractID := hashN(2)
	src := hashN(1)
	inputs := []ledger.Input{ledgerInput(0, callPayload(t, src, contractID, codec.AssetID{}, codec.U256{}))}
	dom := &fakeDomain{steps: []fakeStep{{effects: domain.Effects{Status: codec.StatusSuccess, ContractID: contractID}}}}
	bundles := newFakeBundleStore()
	bundles.flipByte = true
	cfg := testConfig(t, dom, &fakeInputSource{inputs: inputs}, bundles)
	d, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	got, ok, err := d.NextBatch()
	if !errors.Is(err, ErrDAHashMismatch) {
		t.Fatalf("err = %v, want ErrDAHashMismatch", err)
	}
	if ok {
		t.Fatalf("ok = true, want false")
	}
	if got.Bundle != nil || got.Receipts != nil {
		t.Fatalf("got non-zero Batch on mismatch: %+v", got)
	}
}

// --- test 4: slicing ---

func TestDriverSlicing(t *testing.T) {
	t.Run("MaxBatchInputsBoundary", func(t *testing.T) {
		contractID := hashN(2)
		src := hashN(1)
		const n = MaxBatchInputs + 1
		inputs := make([]ledger.Input, n)
		steps := make([]fakeStep, n)
		for i := 0; i < n; i++ {
			inputs[i] = ledgerInput(uint64(i), callPayload(t, src, contractID, codec.AssetID{}, codec.U256{}))
			steps[i] = fakeStep{effects: domain.Effects{Status: codec.StatusSuccess, ContractID: contractID}}
		}
		dom := &fakeDomain{steps: steps}
		cfg := testConfig(t, dom, &fakeInputSource{inputs: inputs}, newFakeBundleStore())
		d, err := New(cfg)
		if err != nil {
			t.Fatalf("New: %v", err)
		}

		b1, ok, err := d.NextBatch()
		if err != nil || !ok {
			t.Fatalf("batch1: ok=%v err=%v", ok, err)
		}
		if len(b1.Receipts) != MaxBatchInputs {
			t.Fatalf("batch1 len = %d, want %d", len(b1.Receipts), MaxBatchInputs)
		}
		if b1.Commitment.BatchID != 100 {
			t.Fatalf("batch1 BatchID = %d, want 100", b1.Commitment.BatchID)
		}

		b2, ok, err := d.NextBatch()
		if err != nil || !ok {
			t.Fatalf("batch2: ok=%v err=%v", ok, err)
		}
		if len(b2.Receipts) != 1 {
			t.Fatalf("batch2 len = %d, want 1", len(b2.Receipts))
		}
		if b2.Commitment.BatchID != 101 {
			t.Fatalf("batch2 BatchID = %d, want 101", b2.Commitment.BatchID)
		}
		if b2.Commitment.FirstInputSeq != MaxBatchInputs {
			t.Fatalf("batch2 FirstInputSeq = %d, want %d", b2.Commitment.FirstInputSeq, MaxBatchInputs)
		}
	})

	t.Run("NonContiguousDomainSubsequence", func(t *testing.T) {
		contractID := hashN(2)
		src := hashN(1)
		idxs := []uint64{5, 7, 9}
		inputs := make([]ledger.Input, len(idxs))
		steps := make([]fakeStep, len(idxs))
		for i, idx := range idxs {
			inputs[i] = ledgerInput(idx, callPayload(t, src, contractID, codec.AssetID{}, codec.U256{}))
			steps[i] = fakeStep{effects: domain.Effects{Status: codec.StatusSuccess, ContractID: contractID}}
		}
		dom := &fakeDomain{steps: steps}
		cfg := testConfig(t, dom, &fakeInputSource{inputs: inputs}, newFakeBundleStore())
		d, err := New(cfg)
		if err != nil {
			t.Fatalf("New: %v", err)
		}

		got, ok, err := d.NextBatch()
		if err != nil || !ok {
			t.Fatalf("NextBatch: ok=%v err=%v", ok, err)
		}
		if got.Commitment.FirstInputSeq != 5 || got.Commitment.LastInputSeq != 9 {
			t.Fatalf("FirstInputSeq/LastInputSeq = %d/%d, want 5/9", got.Commitment.FirstInputSeq, got.Commitment.LastInputSeq)
		}
	})
}

// --- test 5: cap close and carry ---

func TestDriverCapCloseAndCarry(t *testing.T) {
	cap := new(big.Int).Lsh(big.NewInt(1), 255)

	t.Run("CombinedBreachCloseAndCarry", func(t *testing.T) {
		contractID := hashN(2)
		src := hashN(1)
		asset := assetN(1)
		aAmt := u256Big(new(big.Int).Sub(cap, big.NewInt(100)))
		bAmt := u256N(200)

		inputs := []ledger.Input{
			ledgerInput(0, callPayload(t, src, contractID, codec.AssetID{}, codec.U256{})),
			ledgerInput(1, callPayload(t, src, contractID, codec.AssetID{}, codec.U256{})),
		}
		dom := &fakeDomain{steps: []fakeStep{
			{effects: withdrawalEffects(contractID, 0, asset, aAmt)},
			{effects: withdrawalEffects(contractID, 1, asset, bAmt)},
		}}
		cfg := testConfig(t, dom, &fakeInputSource{inputs: inputs}, newFakeBundleStore())
		store := cfg.Store
		d, err := New(cfg)
		if err != nil {
			t.Fatalf("New: %v", err)
		}

		b1, ok, err := d.NextBatch()
		if err != nil || !ok {
			t.Fatalf("batch1: ok=%v err=%v", ok, err)
		}
		if len(b1.Receipts) != 1 {
			t.Fatalf("batch1 len = %d, want 1 (B carried, unapplied)", len(b1.Receipts))
		}

		root, err := store.Root()
		if err != nil {
			t.Fatalf("Root: %v", err)
		}
		if root != b1.Commitment.PostStateRoot {
			t.Fatalf("Store.Root() = %x, want batch1 PostStateRoot %x (the carry must be unapplied)", root, b1.Commitment.PostStateRoot)
		}

		b2, ok, err := d.NextBatch()
		if err != nil || !ok {
			t.Fatalf("batch2: ok=%v err=%v", ok, err)
		}
		if len(b2.Receipts) != 1 {
			t.Fatalf("batch2 len = %d, want 1", len(b2.Receipts))
		}
		if b2.Commitment.FirstInputSeq != 1 {
			t.Fatalf("batch2 FirstInputSeq = %d, want 1 (B, resumed at step 8)", b2.Commitment.FirstInputSeq)
		}
	})

	t.Run("FirstEverInputAloneOverCapUnbatchable", func(t *testing.T) {
		contractID := hashN(2)
		src := hashN(1)
		asset := assetN(1)
		overAmt := u256Big(new(big.Int).Add(cap, big.NewInt(1)))

		inputs := []ledger.Input{ledgerInput(0, callPayload(t, src, contractID, codec.AssetID{}, codec.U256{}))}
		dom := &fakeDomain{steps: []fakeStep{{effects: withdrawalEffects(contractID, 0, asset, overAmt)}}}
		cfg := testConfig(t, dom, &fakeInputSource{inputs: inputs}, newFakeBundleStore())
		d, err := New(cfg)
		if err != nil {
			t.Fatalf("New: %v", err)
		}

		_, ok, err := d.NextBatch()
		if ok {
			t.Fatalf("ok = true, want false")
		}
		if !errors.Is(err, ErrUnbatchable) {
			t.Fatalf("err = %v, want ErrUnbatchable", err)
		}
	})

	t.Run("CarriedAloneOverCapUnbatchable", func(t *testing.T) {
		contractID := hashN(2)
		src := hashN(1)
		asset := assetN(1)
		xAmt := u256N(1)
		overAmt := u256Big(new(big.Int).Add(cap, big.NewInt(1)))

		inputs := []ledger.Input{
			ledgerInput(0, callPayload(t, src, contractID, codec.AssetID{}, codec.U256{})),
			ledgerInput(1, callPayload(t, src, contractID, codec.AssetID{}, codec.U256{})),
		}
		dom := &fakeDomain{steps: []fakeStep{
			{effects: withdrawalEffects(contractID, 0, assetN(2), xAmt)},
			{effects: withdrawalEffects(contractID, 1, asset, overAmt)},
		}}
		cfg := testConfig(t, dom, &fakeInputSource{inputs: inputs}, newFakeBundleStore())
		d, err := New(cfg)
		if err != nil {
			t.Fatalf("New: %v", err)
		}

		b1, ok, err := d.NextBatch()
		if err != nil || !ok {
			t.Fatalf("batch1: ok=%v err=%v", ok, err)
		}
		if len(b1.Receipts) != 1 {
			t.Fatalf("batch1 len = %d, want 1", len(b1.Receipts))
		}

		_, ok, err = d.NextBatch()
		if ok {
			t.Fatalf("batch2: ok = true, want false")
		}
		if !errors.Is(err, ErrUnbatchable) {
			t.Fatalf("batch2 err = %v, want ErrUnbatchable", err)
		}
	})

	t.Run("MaxAssetsPerBatchCloseAndCarry", func(t *testing.T) {
		contractID := hashN(2)
		src := hashN(1)
		assets16 := make([]codec.AssetID, codec.MaxAssetsPerBatch)
		for i := range assets16 {
			assets16[i] = assetN(byte(i + 1))
		}
		inputs := []ledger.Input{
			ledgerInput(0, callPayload(t, src, contractID, codec.AssetID{}, codec.U256{})),
			ledgerInput(1, callPayload(t, src, contractID, codec.AssetID{}, codec.U256{})),
		}
		dom := &fakeDomain{steps: []fakeStep{
			{effects: manyWithdrawalsEffects(contractID, 0, assets16, u256N(1))},
			{effects: withdrawalEffects(contractID, 1, assetN(200), u256N(1))},
		}}
		cfg := testConfig(t, dom, &fakeInputSource{inputs: inputs}, newFakeBundleStore())
		d, err := New(cfg)
		if err != nil {
			t.Fatalf("New: %v", err)
		}

		b1, ok, err := d.NextBatch()
		if err != nil || !ok {
			t.Fatalf("batch1: ok=%v err=%v", ok, err)
		}
		if len(b1.Receipts) != 1 {
			t.Fatalf("batch1 len = %d, want 1", len(b1.Receipts))
		}

		b2, ok, err := d.NextBatch()
		if err != nil || !ok {
			t.Fatalf("batch2: ok=%v err=%v", ok, err)
		}
		if len(b2.Receipts) != 1 {
			t.Fatalf("batch2 len = %d, want 1", len(b2.Receipts))
		}
	})

	t.Run("RefundsExemptFromCap", func(t *testing.T) {
		src := hashN(1)
		asset := assetN(1)
		half := new(big.Int).Rsh(cap, 1)
		amtBig := new(big.Int).Add(half, big.NewInt(1))
		amt := u256Big(amtBig)

		inputs := []ledger.Input{
			ledgerInput(0, callPayload(t, src, hashN(2), asset, amt)),
			ledgerInput(1, callPayload(t, src, hashN(3), asset, amt)),
		}
		dom := &fakeDomain{steps: []fakeStep{
			{effects: refundEffects(0, asset, amt)},
			{effects: refundEffects(1, asset, amt)},
		}}
		cfg := testConfig(t, dom, &fakeInputSource{inputs: inputs}, newFakeBundleStore())
		d, err := New(cfg)
		if err != nil {
			t.Fatalf("New: %v", err)
		}

		got, ok, err := d.NextBatch()
		if err != nil || !ok {
			t.Fatalf("NextBatch: ok=%v err=%v (refunds must be exempt from the cap)", ok, err)
		}
		if len(got.Receipts) != 2 {
			t.Fatalf("len(Receipts) = %d, want 2", len(got.Receipts))
		}

		want := new(big.Int).Mul(amtBig, big.NewInt(2))
		var found bool
		for _, e := range got.Commitment.AssetFlowSummary {
			if e.Asset == asset {
				found = true
				got := new(big.Int).SetBytes(e.WithdrawalDebit[:])
				if got.Cmp(want) != 0 {
					t.Fatalf("WithdrawalDebit = %s, want %s", got, want)
				}
			}
		}
		if !found {
			t.Fatalf("no AssetFlowSummary entry for asset %x", asset)
		}
	})
}

// --- test 6: receipt fields ---

func TestDriverReceiptFields(t *testing.T) {
	t.Run("Failed", func(t *testing.T) {
		src := hashN(1)
		asset := assetN(1)
		depositAmt := u256N(500)
		inputs := []ledger.Input{ledgerInput(0, callPayload(t, src, hashN(2), asset, depositAmt))}
		dom := &fakeDomain{steps: []fakeStep{{effects: refundEffects(0, asset, depositAmt)}}}
		cfg := testConfig(t, dom, &fakeInputSource{inputs: inputs}, newFakeBundleStore())
		d, err := New(cfg)
		if err != nil {
			t.Fatalf("New: %v", err)
		}

		got, ok, err := d.NextBatch()
		if err != nil || !ok {
			t.Fatalf("NextBatch: ok=%v err=%v", ok, err)
		}
		rec := got.Receipts[0]
		if rec.StateDiffHash != (types.Hash{}) || rec.ReturnHash != (types.Hash{}) {
			t.Fatalf("StateDiffHash/ReturnHash = %x/%x, want zero", rec.StateDiffHash, rec.ReturnHash)
		}
		if rec.OutboxCount != 1 {
			t.Fatalf("OutboxCount = %d, want 1", rec.OutboxCount)
		}
		refundMsg, err := codec.OutboxMessage{SourceContract: types.Hash{}, InputIndex: 0, OutboxIndex: 0, Target: codec.L1Withdrawal{RecipientL1: types.Address{1}, Asset: asset, Amount: depositAmt}}.Encode()
		if err != nil {
			t.Fatalf("encode refund: %v", err)
		}
		wantRoot := codec.MerkleRoot([][]byte{refundMsg})
		if rec.OutboxRoot != wantRoot {
			t.Fatalf("OutboxRoot = %x, want %x", rec.OutboxRoot, wantRoot)
		}

		var found bool
		for _, e := range got.Commitment.AssetFlowSummary {
			if e.Asset == asset {
				found = true
				if e.DepositCredit != depositAmt {
					t.Fatalf("DepositCredit = %x, want %x", e.DepositCredit, depositAmt)
				}
			}
		}
		if !found {
			t.Fatalf("no AssetFlowSummary entry for asset %x (edge case 11: depositCredit regardless of status)", asset)
		}
	})

	t.Run("Success", func(t *testing.T) {
		src := hashN(1)
		contractID := hashN(2)
		diff := []domain.StateDiffEntry{{Path: pathN(1), Value: []byte("v")}}
		eff := domain.Effects{
			Status:     codec.StatusSuccess,
			ContractID: contractID,
			CodeHash:   hashN(3),
			StateDiff:  diff,
			ReturnData: []byte("ret"),
		}
		inputs := []ledger.Input{ledgerInput(0, callPayload(t, src, contractID, codec.AssetID{}, codec.U256{}))}
		dom := &fakeDomain{steps: []fakeStep{{effects: eff}}}
		cfg := testConfig(t, dom, &fakeInputSource{inputs: inputs}, newFakeBundleStore())
		d, err := New(cfg)
		if err != nil {
			t.Fatalf("New: %v", err)
		}

		got, ok, err := d.NextBatch()
		if err != nil || !ok {
			t.Fatalf("NextBatch: ok=%v err=%v", ok, err)
		}
		rec := got.Receipts[0]
		wantSD, err := codec.StateDiffHash([]codec.StateDiffEntry{{Key: diff[0].Path, Value: diff[0].Value}})
		if err != nil {
			t.Fatalf("StateDiffHash: %v", err)
		}
		if rec.StateDiffHash != wantSD {
			t.Fatalf("StateDiffHash = %x, want %x", rec.StateDiffHash, wantSD)
		}
		wantRet := codec.ReturnHash([]byte("ret"))
		if rec.ReturnHash != wantRet {
			t.Fatalf("ReturnHash = %x, want %x", rec.ReturnHash, wantRet)
		}

		// edge case 8: diff[0].Path is written but never read (the fake's step has no reads),
		// so its witness must still carry a pre-state proof for it.
		decoded, err := codec.DecodeBundle(got.Bundle)
		if err != nil {
			t.Fatalf("DecodeBundle: %v", err)
		}
		if len(decoded) != 1 {
			t.Fatalf("len(decoded) = %d, want 1", len(decoded))
		}
		var proof []byte
		var found bool
		for _, w := range decoded[0].Witnesses {
			if w.DerivedKey == diff[0].Path {
				proof = w.Proof
				found = true
			}
		}
		if !found {
			t.Fatalf("no witness for the written-but-never-read path %x", diff[0].Path)
		}
		absent, err := trie.VerifyAbsenceByPath(got.Commitment.PreStateRoot, diff[0].Path, proof)
		if err != nil {
			t.Fatalf("VerifyAbsenceByPath: %v", err)
		}
		if !absent {
			t.Fatalf("VerifyAbsenceByPath = false, want true")
		}
	})
}

// --- test 7: unknown kind ---

func TestDriverUnknownKind(t *testing.T) {
	src := hashN(1)
	payload := unsupportedPayload(9, src)
	inputs := []ledger.Input{ledgerInput(0, payload)}
	dom := &fakeDomain{steps: []fakeStep{{effects: domain.Effects{Status: codec.StatusUnsupportedOperation}}}}
	cfg := testConfig(t, dom, &fakeInputSource{inputs: inputs}, newFakeBundleStore())
	d, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	got, ok, err := d.NextBatch()
	if err != nil || !ok {
		t.Fatalf("NextBatch: ok=%v err=%v", ok, err)
	}

	wantPrefix := payload[:33]
	rec := got.Receipts[0]
	wantHash := hashOf(crypto.Hash(wantPrefix))
	if rec.InputHash != wantHash {
		t.Fatalf("InputHash = %x, want %x", rec.InputHash, wantHash)
	}
	wantRoot := codec.MerkleRoot([][]byte{wantPrefix})
	if got.Commitment.InputRoot != wantRoot {
		t.Fatalf("InputRoot = %x, want %x", got.Commitment.InputRoot, wantRoot)
	}

	decoded, err := codec.DecodeBundle(got.Bundle)
	if err != nil {
		t.Fatalf("DecodeBundle: %v", err)
	}
	if !bytes.Equal(decoded[0].InputData, wantPrefix) {
		t.Fatalf("bundle inputData = %x, want %x", decoded[0].InputData, wantPrefix)
	}
}

// TestDriverClaimedDepositExceedsAmount drives a fake domain.Domain that returns a SUCCESS
// Effects with ClaimedDeposit above the input's own deposit_amount — a shape plugins/wasm's
// own step 4 already rejects before it ever reaches the Driver, so this exercises the driver's
// defense-in-depth checkClaimedDeposit directly rather than through the real plugin.
func TestDriverClaimedDepositExceedsAmount(t *testing.T) {
	src := hashN(1)
	contractID := hashN(2)
	asset := assetN(1)
	depositAmt := u256N(100)
	claimed := u256N(101)
	inputs := []ledger.Input{ledgerInput(0, callPayload(t, src, contractID, asset, depositAmt))}
	dom := &fakeDomain{steps: []fakeStep{
		{effects: domain.Effects{Status: codec.StatusSuccess, ContractID: contractID, ClaimedDeposit: claimed}},
	}}
	cfg := testConfig(t, dom, &fakeInputSource{inputs: inputs}, newFakeBundleStore())
	d, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	_, ok, err := d.NextBatch()
	if ok {
		t.Fatalf("NextBatch: ok = true, want false")
	}
	if !errors.Is(err, ErrEffectsInvariant) {
		t.Fatalf("NextBatch err = %v, want ErrEffectsInvariant", err)
	}
}

// --- tests 11-13: source error classes ---

func TestDriverReorgNothingAccumulated(t *testing.T) {
	reorgErr := &ledger.ReorgError{Height: 5, Want: hashN(1), Got: hashN(2)}
	fsrc := &fakeInputSource{errTail: reorgErr, permanent: true}
	cfg := testConfig(t, &fakeDomain{}, fsrc, newFakeBundleStore())
	d, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	_, ok, err := d.NextBatch()
	if ok {
		t.Fatalf("ok = true, want false")
	}
	var re *ledger.ReorgError
	if !errors.As(err, &re) {
		t.Fatalf("errors.As(err, *ledger.ReorgError) = false, err = %v", err)
	}
}

func TestDriverReorgMidBatch(t *testing.T) {
	contractID := hashN(2)
	src := hashN(1)
	inputs := []ledger.Input{
		ledgerInput(0, callPayload(t, src, contractID, codec.AssetID{}, codec.U256{})),
		ledgerInput(1, callPayload(t, src, contractID, codec.AssetID{}, codec.U256{})),
		ledgerInput(2, callPayload(t, src, contractID, codec.AssetID{}, codec.U256{})),
	}
	dom := &fakeDomain{steps: []fakeStep{
		{effects: domain.Effects{Status: codec.StatusSuccess, ContractID: contractID}},
		{effects: domain.Effects{Status: codec.StatusSuccess, ContractID: contractID}},
		{effects: domain.Effects{Status: codec.StatusSuccess, ContractID: contractID}},
	}}
	reorgErr := &ledger.ReorgError{Height: 9, Want: hashN(3), Got: hashN(4)}
	fsrc := &fakeInputSource{inputs: inputs, errTail: reorgErr, permanent: true}
	cfg := testConfig(t, dom, fsrc, newFakeBundleStore())
	d, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	_, ok, err := d.NextBatch()
	if ok {
		t.Fatalf("ok = true, want false")
	}
	var re *ledger.ReorgError
	if !errors.As(err, &re) {
		t.Fatalf("errors.As(err, *ledger.ReorgError) = false, err = %v", err)
	}
}

// TestDriverReorgThenReused drives a Driver into a *ledger.ReorgError, then calls NextBatch
// again on the same instance and confirms it fails closed with ErrDriverReorged rather than
// silently caching a stale root past the external rollback the Driver cannot observe.
func TestDriverReorgThenReused(t *testing.T) {
	reorgErr := &ledger.ReorgError{Height: 5, Want: hashN(1), Got: hashN(2)}
	fsrc := &fakeInputSource{errTail: reorgErr, permanent: true}
	cfg := testConfig(t, &fakeDomain{}, fsrc, newFakeBundleStore())
	d, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	if _, ok, err := d.NextBatch(); ok {
		t.Fatalf("first NextBatch: ok = true, want false (err = %v)", err)
	} else {
		var re *ledger.ReorgError
		if !errors.As(err, &re) {
			t.Fatalf("first NextBatch err classified as *ledger.ReorgError = false, err = %v", err)
		}
	}

	_, ok, err := d.NextBatch()
	if ok {
		t.Fatalf("second NextBatch: ok = true, want false")
	}
	if !errors.Is(err, ErrDriverReorged) {
		t.Fatalf("second NextBatch err = %v, want ErrDriverReorged", err)
	}
}

func TestDriverSourceErrorMidBatch(t *testing.T) {
	contractID := hashN(2)
	src := hashN(1)
	inputs := []ledger.Input{
		ledgerInput(0, callPayload(t, src, contractID, codec.AssetID{}, codec.U256{})),
		ledgerInput(1, callPayload(t, src, contractID, codec.AssetID{}, codec.U256{})),
		ledgerInput(2, callPayload(t, src, contractID, codec.AssetID{}, codec.U256{})),
	}
	dom := &fakeDomain{steps: []fakeStep{
		{effects: domain.Effects{Status: codec.StatusSuccess, ContractID: contractID}},
		{effects: domain.Effects{Status: codec.StatusSuccess, ContractID: contractID}},
		{effects: domain.Effects{Status: codec.StatusSuccess, ContractID: contractID}},
	}}
	plainErr := errors.New("transient read failure")
	fsrc := &fakeInputSource{inputs: inputs, errTail: plainErr, permanent: true}
	cfg := testConfig(t, dom, fsrc, newFakeBundleStore())
	d, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	got, ok, err := d.NextBatch()
	if err != nil || !ok {
		t.Fatalf("NextBatch: ok=%v err=%v, want a closed batch with a nil error", ok, err)
	}
	if len(got.Receipts) != 3 {
		t.Fatalf("len(Receipts) = %d, want 3", len(got.Receipts))
	}

	_, ok, err = d.NextBatch()
	if ok {
		t.Fatalf("second NextBatch: ok = true, want false")
	}
	if !errors.Is(err, plainErr) {
		t.Fatalf("second NextBatch err = %v, want it to wrap the re-presented error", err)
	}
}
