package batch

import (
	"bytes"
	"errors"
	"fmt"
	"math/big"
	"reflect"
	"strings"
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
	accs  []domain.StateAccess
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
	d.accs = append(d.accs, acc)
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

// fakeReceiptStore is an in-memory ReceiptStore that records every Put; when err is set, Put
// returns it and records nothing.
type fakeReceiptStore struct {
	puts []receiptPut
	err  error
}

type receiptPut struct {
	daHash, receiptRoot types.Hash
	sidecar             []byte
}

func newFakeReceiptStore() *fakeReceiptStore { return &fakeReceiptStore{} }

func (s *fakeReceiptStore) Put(daHash, receiptRoot types.Hash, sidecar []byte) error {
	if s.err != nil {
		return s.err
	}
	s.puts = append(s.puts, receiptPut{daHash, receiptRoot, append([]byte(nil), sidecar...)})
	return nil
}

// --- helpers ---

func openTestStore(t *testing.T) *state.Store {
	t.Helper()
	s := state.OpenEmpty()
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
		Receipts:     newFakeReceiptStore(),
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
// outside 0..7, with no defined tail.
func unsupportedPayload(kind byte, src types.Hash) []byte {
	b := make([]byte, 33)
	b[0] = kind
	copy(b[1:], src[:])
	return b
}

// capFunc is a domain.CapLookup backed by a function.
type capFunc func(codec.AssetID) (codec.U256, error)

func (f capFunc) MaxBatchWithdrawals(assets []codec.AssetID) ([]codec.U256, error) {
	out := make([]codec.U256, len(assets))
	for i, a := range assets {
		v, err := f(a)
		if err != nil {
			return nil, err
		}
		out[i] = v
	}
	return out, nil
}

// allCaps answers v for every asset.
func allCaps(v codec.U256) capFunc {
	return func(codec.AssetID) (codec.U256, error) { return v, nil }
}

// ledgerInput's cap is 2^255 for every asset, the cap the driver tests were written against.
func ledgerInput(idx uint64, payload []byte) ledger.Input {
	return ledger.Input{Ctx: domain.InputContext{GlobalInputIndex: idx, Caps: allCaps(codec.U256{0x80})}, Payload: payload}
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

// twoInputDriver returns a driver over the two-input batch of TestDriverReplayDeterminism and
// its receipt store.
func twoInputDriver(t *testing.T) (*Driver, *fakeReceiptStore) {
	t.Helper()
	contractID := hashN(2)
	src := hashN(1)
	inputs := []ledger.Input{
		ledgerInput(0, callPayload(t, src, contractID, codec.AssetID{}, codec.U256{})),
		ledgerInput(1, callPayload(t, src, contractID, codec.AssetID{}, codec.U256{})),
	}
	dom := &fakeDomain{steps: []fakeStep{
		{effects: withdrawalEffects(contractID, 0, assetN(1), u256N(10))},
		{effects: withdrawalEffects(contractID, 1, assetN(1), u256N(20))},
	}}
	cfg := testConfig(t, dom, &fakeInputSource{inputs: inputs}, newFakeBundleStore())
	rs := cfg.Receipts.(*fakeReceiptStore)
	d, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return d, rs
}

func TestDriverWritesReceiptSidecar(t *testing.T) {
	d, rs := twoInputDriver(t)
	got, ok, err := d.NextBatch()
	if err != nil || !ok {
		t.Fatalf("NextBatch: ok=%v err=%v", ok, err)
	}
	if len(rs.puts) != 1 {
		t.Fatalf("receipt puts = %d, want 1", len(rs.puts))
	}
	p := rs.puts[0]
	if p.daHash != got.Commitment.DAHash {
		t.Fatalf("put daHash = %x, want %x", p.daHash, got.Commitment.DAHash)
	}
	if p.receiptRoot != got.Commitment.ReceiptRoot {
		t.Fatalf("put receiptRoot = %x, want %x", p.receiptRoot, got.Commitment.ReceiptRoot)
	}
	receipts, leaves, err := codec.DecodeReceiptSidecar(p.sidecar)
	if err != nil {
		t.Fatalf("DecodeReceiptSidecar: %v", err)
	}
	if !reflect.DeepEqual(receipts, got.Receipts) {
		t.Fatalf("sidecar receipts = %+v, want %+v", receipts, got.Receipts)
	}
	if codec.MerkleRoot(leaves) != got.Commitment.ReceiptRoot {
		t.Fatalf("sidecar leaves do not hash to ReceiptRoot")
	}
}

func TestDriverReceiptStoreError(t *testing.T) {
	d, rs := twoInputDriver(t)
	boom := errors.New("receipt disk full")
	rs.err = boom
	got, ok, err := d.NextBatch()
	if !errors.Is(err, boom) {
		t.Fatalf("NextBatch err = %v, want it to wrap %v", err, boom)
	}
	if ok {
		t.Fatalf("NextBatch ok = true on a receipt store error")
	}
	if !reflect.DeepEqual(got, Batch{}) {
		t.Fatalf("NextBatch returned a non-zero Batch on error")
	}
}

func TestNewRejectsNilReceipts(t *testing.T) {
	cfg := testConfig(t, &fakeDomain{}, &fakeInputSource{}, newFakeBundleStore())
	cfg.Receipts = nil
	_, err := New(cfg)
	if err == nil || !strings.Contains(err.Error(), "Config.Receipts is nil") {
		t.Fatalf("New err = %v, want Config.Receipts is nil", err)
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
			t.Fatalf("batch2 FirstInputSeq = %d, want 1 (B, resumed at cap evaluation)", b2.Commitment.FirstInputSeq)
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

	t.Run("LoneInputDepositNearCapU256Invariant", func(t *testing.T) {
		// A lone input's withdrawalDebit is at most its cap plus its deposit, here 2^255 +
		// (2^255 - 1) = 2^256 - 1: breach() must never reject this input for overflow.
		contractID := hashN(2)
		src := hashN(1)
		asset := assetN(1)
		depositAmt := u256Big(new(big.Int).Sub(cap, big.NewInt(1))) // 2^255 - 1

		inputs := []ledger.Input{ledgerInput(0, callPayload(t, src, contractID, asset, depositAmt))}
		dom := &fakeDomain{steps: []fakeStep{{effects: domain.Effects{
			Status:     codec.StatusSuccess,
			ContractID: contractID,
			OutboxMessages: []codec.OutboxMessage{
				{SourceContract: contractID, InputIndex: 0, OutboxIndex: 0, Target: codec.L1Withdrawal{RecipientL1: types.Address{1}, Asset: asset, Amount: u256Big(cap)}},
				{SourceContract: types.Hash{}, InputIndex: 0, OutboxIndex: 1, Target: codec.L1Withdrawal{RecipientL1: types.Address{1}, Asset: asset, Amount: depositAmt}},
			},
		}}}}
		cfg := testConfig(t, dom, &fakeInputSource{inputs: inputs}, newFakeBundleStore())
		d, err := New(cfg)
		if err != nil {
			t.Fatalf("New: %v", err)
		}

		got, ok, err := d.NextBatch()
		if err != nil || !ok {
			t.Fatalf("NextBatch: ok=%v err=%v", ok, err)
		}
		if len(got.Receipts) != 1 {
			t.Fatalf("len(Receipts) = %d, want 1", len(got.Receipts))
		}

		want := new(big.Int).Sub(new(big.Int).Lsh(big.NewInt(1), 256), big.NewInt(1)) // 2^256 - 1
		var found bool
		for _, e := range got.Commitment.AssetFlowSummary {
			if e.Asset == asset {
				found = true
				gotDebit := new(big.Int).SetBytes(e.WithdrawalDebit[:])
				if gotDebit.Cmp(want) != 0 {
					t.Fatalf("WithdrawalDebit = %s, want %s (2^256 - 1)", gotDebit, want)
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
			t.Fatalf("no AssetFlowSummary entry for asset %x (depositCredit regardless of status)", asset)
		}
	})

	t.Run("Revert", func(t *testing.T) {
		src := hashN(1)
		asset := assetN(1)
		depositAmt := u256N(500)
		data := bytes.Repeat([]byte{0x5A}, 40)
		eff := refundEffects(0, asset, depositAmt)
		eff.Status = codec.StatusRevert
		eff.AbortCode = 7
		eff.ReturnData = data
		inputs := []ledger.Input{ledgerInput(0, callPayload(t, src, hashN(2), asset, depositAmt))}
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
		var wantRet types.Hash
		copy(wantRet[:], crypto.Hash(data))
		if rec.ReturnHash != wantRet {
			t.Fatalf("ReturnHash = %x, want %x", rec.ReturnHash, wantRet)
		}
		if rec.StateDiffHash != (types.Hash{}) || rec.EventRoot != (types.Hash{}) {
			t.Fatalf("StateDiffHash/EventRoot = %x/%x, want zero", rec.StateDiffHash, rec.EventRoot)
		}
		if rec.AbortCode != 7 {
			t.Fatalf("AbortCode = %d, want 7", rec.AbortCode)
		}

		decoded, err := codec.DecodeBundle(got.Bundle)
		if err != nil {
			t.Fatalf("DecodeBundle: %v", err)
		}
		if len(decoded) != 1 {
			t.Fatalf("len(decoded) = %d, want 1", len(decoded))
		}
		er, err := codec.DecodeExecutionResult(decoded[0].ExecutionResult)
		if err != nil {
			t.Fatalf("DecodeExecutionResult: %v", err)
		}
		if !bytes.Equal(er.ReturnData, data) {
			t.Fatalf("ExecutionResult.ReturnData = %x, want %x", er.ReturnData, data)
		}
		if er.PostStateRoot != er.PreStateRoot {
			t.Fatalf("PostStateRoot %x != PreStateRoot %x on a REVERT", er.PostStateRoot, er.PreStateRoot)
		}
	})

	t.Run("ReturnDataOutsideRevert", func(t *testing.T) {
		asset := assetN(1)
		depositAmt := u256N(500)
		eff := refundEffects(0, asset, depositAmt)
		eff.ReturnData = []byte{0x01}
		inputs := []ledger.Input{ledgerInput(0, callPayload(t, hashN(1), hashN(2), asset, depositAmt))}
		dom := &fakeDomain{steps: []fakeStep{{effects: eff}}}
		cfg := testConfig(t, dom, &fakeInputSource{inputs: inputs}, newFakeBundleStore())
		d, err := New(cfg)
		if err != nil {
			t.Fatalf("New: %v", err)
		}
		if _, ok, err := d.NextBatch(); ok || !errors.Is(err, ErrEffectsInvariant) {
			t.Fatalf("NextBatch: ok=%v err=%v, want ErrEffectsInvariant", ok, err)
		}
	})

	t.Run("OversizedRevert", func(t *testing.T) {
		asset := assetN(1)
		depositAmt := u256N(500)
		eff := refundEffects(0, asset, depositAmt)
		eff.Status = codec.StatusRevert
		eff.AbortCode = 7
		eff.ReturnData = make([]byte, codec.MaxReturnData+1)
		if err := checkNonSuccessInvariant(eff); err != nil {
			t.Fatalf("checkNonSuccessInvariant = %v, want nil", err)
		}
		inputs := []ledger.Input{ledgerInput(0, callPayload(t, hashN(1), hashN(2), asset, depositAmt))}
		dom := &fakeDomain{steps: []fakeStep{{effects: eff}}}
		cfg := testConfig(t, dom, &fakeInputSource{inputs: inputs}, newFakeBundleStore())
		d, err := New(cfg)
		if err != nil {
			t.Fatalf("New: %v", err)
		}
		if _, ok, err := d.NextBatch(); ok || !errors.Is(err, codec.ErrLimitExceeded) {
			t.Fatalf("NextBatch: ok=%v err=%v, want codec.ErrLimitExceeded", ok, err)
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

		// diff[0].Path is written but never read (the fake's step has
		// no reads), so its witness must still carry a pre-state proof
		// for it.
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

// --- test 6b: partial claim asset flow ---

// TestDriverPartialClaimAssetFlow drives one payable CallL2 whose
// contract claims 400 of a 1000 deposit and checks that DepositCredit -
// WithdrawalDebit on the batch's AssetFlowSummary equals the claim.
func TestDriverPartialClaimAssetFlow(t *testing.T) {
	asset := assetN(1)
	deposit := u256N(1000)
	inputs := []ledger.Input{ledgerInput(0, callPayload(t, hashN(1), hashN(2), asset, deposit))}
	eff := domain.Effects{
		Status:         codec.StatusSuccess,
		ContractID:     hashN(2),
		ClaimedDeposit: u256N(400),
		OutboxMessages: []codec.OutboxMessage{
			{SourceContract: types.Hash{}, InputIndex: 0, OutboxIndex: 0, Target: codec.L1Withdrawal{RecipientL1: types.Address{1}, Asset: asset, Amount: u256N(600)}},
		},
	}
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
	summary := got.Commitment.AssetFlowSummary
	if len(summary) != 1 {
		t.Fatalf("len(AssetFlowSummary) = %d, want 1", len(summary))
	}
	e := summary[0]
	if e.Asset != asset {
		t.Fatalf("AssetFlowSummary[0].Asset = %x, want %x", e.Asset, asset)
	}
	if e.DepositCredit != deposit {
		t.Fatalf("DepositCredit = %x, want %x", e.DepositCredit, deposit)
	}
	if e.WithdrawalDebit != u256N(600) {
		t.Fatalf("WithdrawalDebit = %x, want %x", e.WithdrawalDebit, u256N(600))
	}
	credit := new(big.Int).SetBytes(e.DepositCredit[:])
	debit := new(big.Int).SetBytes(e.WithdrawalDebit[:])
	if net := new(big.Int).Sub(credit, debit); net.Cmp(big.NewInt(400)) != 0 {
		t.Fatalf("DepositCredit - WithdrawalDebit = %s, want the claim 400", net)
	}
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
	if !strings.Contains(err.Error(), "ClaimedDeposit exceeds the input's deposit_amount") {
		t.Fatalf("NextBatch err = %v, want the claimed-deposit invariant", err)
	}
}

func TestCheckEffects(t *testing.T) {
	refund := func(mut func(*codec.OutboxMessage)) domain.Effects {
		eff := refundEffects(0, assetN(1), u256N(100))
		mut(&eff.OutboxMessages[0])
		return eff
	}
	revertWithRefund := refundEffects(0, assetN(1), u256N(100))
	revertWithRefund.Status = codec.StatusRevert
	revertWithRefund.AbortCode = 7
	revertWithRefund.ReturnData = []byte("r")
	contractContent := withdrawalEffects(hashN(2), 0, assetN(1), u256N(5))
	contractContent.StateDiff = []domain.StateDiffEntry{{Path: pathN(1), Value: []byte("v")}}
	contractContent.Events = []codec.Event{{ContractID: hashN(2)}}
	contractContent.ReturnData = []byte("r")
	twoRefunds := refundEffects(0, assetN(1), u256N(100))
	twoRefunds.OutboxMessages = append(twoRefunds.OutboxMessages, twoRefunds.OutboxMessages[0])

	for _, tc := range []struct {
		name    string
		eff     domain.Effects
		deposit codec.U256
		reason  string
	}{
		{"revert with state diff", domain.Effects{Status: codec.StatusRevert, StateDiff: []domain.StateDiffEntry{{Path: pathN(1), Value: []byte("v")}}}, u256N(100), "non-empty StateDiff/Events"},
		{"runtime fault with event", domain.Effects{Status: codec.StatusRuntimeFault, Events: []codec.Event{{ContractID: hashN(2)}}}, u256N(100), "non-empty StateDiff/Events"},
		{"runtime fault with return data", domain.Effects{Status: codec.StatusRuntimeFault, ReturnData: []byte{1}}, u256N(100), "non-empty ReturnData outside REVERT"},
		{"revert with claimed deposit", domain.Effects{Status: codec.StatusRevert, ClaimedDeposit: u256N(1)}, u256N(100), "non-zero ClaimedDeposit"},
		{"out of gas with abort code", domain.Effects{Status: codec.StatusOutOfGas, AbortCode: 7}, u256N(100), "non-zero AbortCode outside REVERT"},
		{"outbox message is not a withdrawal", domain.Effects{Status: codec.StatusRevert, OutboxMessages: []codec.OutboxMessage{{Target: codec.L2Delivery{TargetContract: hashN(2)}}}}, u256N(100), "outbox message is not an L1Withdrawal"},
		{"refund outbox index", refund(func(m *codec.OutboxMessage) { m.OutboxIndex = 1 }), u256N(100), "refund OutboxIndex != 0"},
		{"refund source contract", refund(func(m *codec.OutboxMessage) { m.SourceContract = hashN(2) }), u256N(100), "refund SourceContract != zero"},
		{"two refunds", twoRefunds, u256N(100), "more than one outbox message"},
		{"success claims above deposit", domain.Effects{Status: codec.StatusSuccess, ClaimedDeposit: u256N(101)}, u256N(100), "ClaimedDeposit exceeds the input's deposit_amount"},
		{"success claims with no deposit", domain.Effects{Status: codec.StatusSuccess, ClaimedDeposit: u256N(1)}, codec.U256{}, "ClaimedDeposit exceeds the input's deposit_amount"},
		{"runtime fault with refund", refundEffects(0, assetN(1), u256N(100)), u256N(100), ""},
		{"revert with refund, abort code and return data", revertWithRefund, u256N(100), ""},
		{"bare revert", domain.Effects{Status: codec.StatusRevert}, u256N(100), ""},
		{"success with contract content", contractContent, u256N(100), ""},
		{"success claims below deposit", domain.Effects{Status: codec.StatusSuccess, ClaimedDeposit: u256N(40)}, u256N(100), ""},
		{"success claims the deposit", domain.Effects{Status: codec.StatusSuccess, ClaimedDeposit: u256N(100)}, u256N(100), ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := CheckEffects(tc.eff, tc.deposit)
			if tc.reason == "" {
				if err != nil {
					t.Fatalf("CheckEffects = %v, want nil", err)
				}
				return
			}
			if !errors.Is(err, ErrEffectsInvariant) || !strings.Contains(err.Error(), tc.reason) {
				t.Fatalf("CheckEffects = %v, want ErrEffectsInvariant containing %q", err, tc.reason)
			}
		})
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

// --- test 11: accessor released before Apply ---

// TestDriverReleasesAccessor proves every accessor computeRecord hands to a domain's Apply is
// released by the time NextBatch returns, in both its success and error paths. The accessor is
// released before any Store.Apply call runs: computeRecord's defer acc.Release() executes
// before finishApply's Store.Apply, since a defer in computeRecord fires when computeRecord
// returns, which is always before finishApply is called. That ordering, not this test, is what
// rules out a clone on the driver's path; a reviewer confirms it by checking the defer's
// placement in computeRecord directly.
func TestDriverReleasesAccessor(t *testing.T) {
	t.Run("batch", func(t *testing.T) {
		contractID := hashN(2)
		src := hashN(1)
		p := pathN(3)
		inputs := []ledger.Input{
			ledgerInput(0, callPayload(t, src, contractID, codec.AssetID{}, codec.U256{})),
			ledgerInput(1, unsupportedPayload(9, src)),
		}
		dom := &fakeDomain{steps: []fakeStep{
			{reads: []trie.Path{p}, effects: domain.Effects{
				Status:     codec.StatusSuccess,
				ContractID: contractID,
				StateDiff:  []domain.StateDiffEntry{{Path: p, Value: []byte("v1")}},
			}},
			{effects: domain.Effects{Status: codec.StatusRuntimeFault}},
		}}
		cfg := testConfig(t, dom, &fakeInputSource{inputs: inputs}, newFakeBundleStore())
		d, err := New(cfg)
		if err != nil {
			t.Fatalf("New: %v", err)
		}

		if _, ok, err := d.NextBatch(); err != nil || !ok {
			t.Fatalf("NextBatch: ok=%v err=%v", ok, err)
		}
		if len(dom.accs) != 2 {
			t.Fatalf("len(dom.accs) = %d, want 2", len(dom.accs))
		}
		for i, acc := range dom.accs {
			if _, _, err := acc.Read(p); !errors.Is(err, state.ErrAccessorReleased) {
				t.Fatalf("accs[%d].Read after NextBatch: err = %v, want ErrAccessorReleased", i, err)
			}
		}
	})

	t.Run("apply error", func(t *testing.T) {
		src := hashN(1)
		applyErr := errors.New("domain apply failed")
		inputs := []ledger.Input{ledgerInput(0, unsupportedPayload(9, src))}
		dom := &fakeDomain{steps: []fakeStep{{applyErr: applyErr}}}
		cfg := testConfig(t, dom, &fakeInputSource{inputs: inputs}, newFakeBundleStore())
		d, err := New(cfg)
		if err != nil {
			t.Fatalf("New: %v", err)
		}

		if _, _, err := d.NextBatch(); err == nil {
			t.Fatalf("NextBatch: err = nil, want an error")
		}
		if len(dom.accs) != 1 {
			t.Fatalf("len(dom.accs) = %d, want 1", len(dom.accs))
		}
		if _, _, err := dom.accs[0].Read(pathN(1)); !errors.Is(err, state.ErrAccessorReleased) {
			t.Fatalf("accs[0].Read after NextBatch: err = %v, want ErrAccessorReleased", err)
		}
	})
}

// --- withdrawal caps ---

func TestDriverHighestPerInputCap(t *testing.T) {
	contractID := hashN(2)
	src := hashN(1)
	asset := assetN(1)

	// run feeds one withdrawal per input, each input carrying its own cap for asset.
	run := func(t *testing.T, caps, amounts []uint64) *Driver {
		t.Helper()
		inputs := make([]ledger.Input, len(caps))
		steps := make([]fakeStep, len(caps))
		for i := range caps {
			inputs[i] = ledgerInput(uint64(i), callPayload(t, src, contractID, codec.AssetID{}, codec.U256{}))
			inputs[i].Ctx.Caps = allCaps(u256N(caps[i]))
			steps[i] = fakeStep{effects: withdrawalEffects(contractID, uint64(i), asset, u256N(amounts[i]))}
		}
		cfg := testConfig(t, &fakeDomain{steps: steps}, &fakeInputSource{inputs: inputs}, newFakeBundleStore())
		d, err := New(cfg)
		if err != nil {
			t.Fatalf("New: %v", err)
		}
		return d
	}

	t.Run("HigherCapFirst", func(t *testing.T) {
		d := run(t, []uint64{150, 100}, []uint64{60, 60})
		b, ok, err := d.NextBatch()
		if err != nil || !ok || len(b.Receipts) != 2 {
			t.Fatalf("NextBatch: ok=%v err=%v receipts=%d, want one batch of 2", ok, err, len(b.Receipts))
		}
	})

	t.Run("HigherCapSecond", func(t *testing.T) {
		d := run(t, []uint64{100, 150}, []uint64{60, 60})
		b, ok, err := d.NextBatch()
		if err != nil || !ok || len(b.Receipts) != 2 {
			t.Fatalf("NextBatch: ok=%v err=%v receipts=%d, want one batch of 2", ok, err, len(b.Receipts))
		}
	})

	t.Run("AboveHighestCloses", func(t *testing.T) {
		d := run(t, []uint64{100, 150}, []uint64{60, 100})
		b1, ok, err := d.NextBatch()
		if err != nil || !ok || len(b1.Receipts) != 1 {
			t.Fatalf("batch1: ok=%v err=%v receipts=%d, want 1", ok, err, len(b1.Receipts))
		}
		b2, ok, err := d.NextBatch()
		if err != nil || !ok || b2.Commitment.FirstInputSeq != 1 {
			t.Fatalf("batch2: ok=%v err=%v FirstInputSeq=%d, want 1", ok, err, b2.Commitment.FirstInputSeq)
		}
	})

	t.Run("UnconfiguredZeroAmount", func(t *testing.T) {
		d := run(t, []uint64{0}, []uint64{0})
		b, ok, err := d.NextBatch()
		if err != nil || !ok || len(b.Receipts) != 1 {
			t.Fatalf("NextBatch: ok=%v err=%v receipts=%d, want 1", ok, err, len(b.Receipts))
		}
		for _, e := range b.Commitment.AssetFlowSummary {
			if e.Asset == asset {
				t.Fatalf("AssetFlowSummary has an entry for asset %x, want none", asset)
			}
		}
	})
}

func TestDriverCapsPerAsset(t *testing.T) {
	contractID := hashN(2)
	src := hashN(1)
	caps := capFunc(func(a codec.AssetID) (codec.U256, error) {
		if a == assetN(2) {
			return u256N(50), nil
		}
		return u256N(100), nil
	})

	inputs := make([]ledger.Input, 2)
	for i := range inputs {
		inputs[i] = ledgerInput(uint64(i), callPayload(t, src, contractID, codec.AssetID{}, codec.U256{}))
		inputs[i].Ctx.Caps = caps
	}
	steps := []fakeStep{
		{effects: manyWithdrawalsEffects(contractID, 0, []codec.AssetID{assetN(1), assetN(2)}, u256N(40))},
		{effects: withdrawalEffects(contractID, 1, assetN(2), u256N(20))},
	}
	d, err := New(testConfig(t, &fakeDomain{steps: steps}, &fakeInputSource{inputs: inputs}, newFakeBundleStore()))
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	b1, ok, err := d.NextBatch()
	if err != nil || !ok || len(b1.Receipts) != 1 {
		t.Fatalf("batch1: ok=%v err=%v receipts=%d, want 1", ok, err, len(b1.Receipts))
	}
	b2, ok, err := d.NextBatch()
	if err != nil || !ok || b2.Commitment.FirstInputSeq != 1 {
		t.Fatalf("batch2: ok=%v err=%v FirstInputSeq=%d, want 1", ok, err, b2.Commitment.FirstInputSeq)
	}
}

func TestDriverCapLookup(t *testing.T) {
	contractID := hashN(2)
	src := hashN(1)
	errCap := errors.New("cap source down")

	next := func(t *testing.T, caps domain.CapLookup, eff domain.Effects) (Batch, bool, error) {
		t.Helper()
		in := ledgerInput(0, callPayload(t, src, contractID, codec.AssetID{}, codec.U256{}))
		in.Ctx.Caps = caps
		cfg := testConfig(t, &fakeDomain{steps: []fakeStep{{effects: eff}}}, &fakeInputSource{inputs: []ledger.Input{in}}, newFakeBundleStore())
		d, err := New(cfg)
		if err != nil {
			t.Fatalf("New: %v", err)
		}
		return d.NextBatch()
	}

	t.Run("LookupError", func(t *testing.T) {
		_, ok, err := next(t, capFunc(func(codec.AssetID) (codec.U256, error) { return codec.U256{}, errCap }),
			withdrawalEffects(contractID, 0, assetN(1), u256N(1)))
		if ok || !errors.Is(err, errCap) {
			t.Fatalf("ok=%v err=%v, want false and errCap", ok, err)
		}
	})

	t.Run("NilCapsWithContractWithdrawal", func(t *testing.T) {
		_, ok, err := next(t, nil, withdrawalEffects(contractID, 0, assetN(1), u256N(1)))
		if ok || err == nil {
			t.Fatalf("ok=%v err=%v, want false and an error", ok, err)
		}
	})

	t.Run("RefundNeedsNoCap", func(t *testing.T) {
		b, ok, err := next(t, nil, refundEffects(0, assetN(1), u256N(1)))
		if err != nil || !ok || len(b.Receipts) != 1 {
			t.Fatalf("ok=%v err=%v receipts=%d, want a batch of 1", ok, err, len(b.Receipts))
		}
	})
}

// bundleCheckingReceipts fails a Put unless the bundle for the same DAHash is already stored.
type bundleCheckingReceipts struct {
	bundles BundleStore
	puts    int
}

func (s *bundleCheckingReceipts) Put(daHash, _ types.Hash, _ []byte) error {
	if _, err := s.bundles.Get(daHash); err != nil {
		return fmt.Errorf("receipt put before its bundle: %w", err)
	}
	s.puts++
	return nil
}

func TestDriverWritesSidecarAfterBundle(t *testing.T) {
	contractID := hashN(2)
	inputs := []ledger.Input{ledgerInput(0, callPayload(t, hashN(1), contractID, codec.AssetID{}, codec.U256{}))}
	dom := &fakeDomain{steps: []fakeStep{{effects: withdrawalEffects(contractID, 0, assetN(1), u256N(10))}}}
	bundles := newFakeBundleStore()
	cfg := testConfig(t, dom, &fakeInputSource{inputs: inputs}, bundles)
	rs := &bundleCheckingReceipts{bundles: bundles}
	cfg.Receipts = rs
	d, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if _, ok, err := d.NextBatch(); err != nil || !ok {
		t.Fatalf("NextBatch: ok=%v err=%v", ok, err)
	}
	if rs.puts != 1 {
		t.Fatalf("receipt puts = %d, want 1", rs.puts)
	}
}
