package batch

import (
	"sort"
	"testing"

	"github.com/zenon-network/go-zenon/common/crypto"
	"github.com/zenon-network/go-zenon/common/types"

	"github.com/Zeno-L2/zeno-executor/domain"
	"github.com/Zeno-L2/zeno-executor/internal/codec"
	"github.com/Zeno-L2/zeno-executor/internal/ledger"
)

// specRoot is the Merkle root over leaves, written independently of
// codec.MerkleRoot: for an odd node count at a level, the last node is
// promoted unchanged. It rejects an empty leaf list.
func specRoot(t *testing.T, leaves [][]byte) types.Hash {
	t.Helper()
	if len(leaves) == 0 {
		t.Fatalf("specRoot: empty leaf list")
	}
	level := make([][]byte, len(leaves))
	for i, l := range leaves {
		level[i] = crypto.Hash([]byte{0x00}, l)
	}
	for len(level) > 1 {
		var next [][]byte
		for i := 0; i+1 < len(level); i += 2 {
			next = append(next, crypto.Hash([]byte{0x01}, level[i], level[i+1]))
		}
		if len(level)%2 == 1 {
			next = append(next, level[len(level)-1])
		}
		level = next
	}
	var root types.Hash
	copy(root[:], level[0])
	return root
}

// scriptedBatch runs inputs through one NextBatch against a fakeDomain
// scripted with one mk() call, and returns the batch and a second mk()
// call, indexed by global input index.
func scriptedBatch(t *testing.T, inputs []ledger.Input, mk func() []domain.Effects) (Batch, []domain.Effects) {
	t.Helper()
	scripted := mk()
	steps := make([]fakeStep, len(scripted))
	for i, e := range scripted {
		steps[i] = fakeStep{effects: e}
	}
	dom := &fakeDomain{steps: steps}
	d, err := New(testConfig(t, dom, &fakeInputSource{inputs: inputs}, newFakeBundleStore()))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	b, ok, err := d.NextBatch()
	if err != nil || !ok {
		t.Fatalf("NextBatch: ok=%v err=%v", ok, err)
	}
	if len(b.Receipts) != len(inputs) {
		t.Fatalf("len(Receipts) = %d, want %d", len(b.Receipts), len(inputs))
	}
	return b, mk()
}

// rootsEffects returns the ordering fixture's four scripted Effects.
// Every call allocates new slices.
func rootsEffects() []domain.Effects {
	refund := codec.L1Withdrawal{RecipientL1: types.Address{1}, Asset: assetN(1), Amount: u256N(600)}
	return []domain.Effects{
		{
			Status:         codec.StatusSuccess,
			ContractID:     hashN(4),
			ClaimedDeposit: u256N(400),
			Events: []codec.Event{
				{ContractID: hashN(4), InputIndex: 0, EventIndex: 0, Data: []byte{0x01}},
				{ContractID: hashN(4), InputIndex: 0, EventIndex: 1, Data: []byte{0x02}},
			},
			OutboxMessages: []codec.OutboxMessage{
				{SourceContract: hashN(4), InputIndex: 0, OutboxIndex: 0, Target: codec.L2Delivery{TargetContract: hashN(9)}},
				{SourceContract: hashN(4), InputIndex: 0, OutboxIndex: 1, Target: codec.L1Withdrawal{RecipientL1: types.Address{1}, Asset: assetN(2), Amount: u256N(10)}},
				{SourceContract: types.Hash{}, InputIndex: 0, OutboxIndex: 2, Target: refund},
			},
		},
		{Status: codec.StatusRuntimeFault, ContractID: hashN(5)},
		{Status: codec.StatusSuccess, ContractID: hashN(3)},
		{
			Status:     codec.StatusSuccess,
			ContractID: hashN(2),
			Events: []codec.Event{
				{ContractID: hashN(2), InputIndex: 3, EventIndex: 0, Data: []byte{0x03}},
				{ContractID: hashN(2), InputIndex: 3, EventIndex: 1, Data: []byte{0x04}},
			},
			OutboxMessages: []codec.OutboxMessage{
				{SourceContract: hashN(2), InputIndex: 3, OutboxIndex: 0, Target: codec.L2Delivery{TargetContract: hashN(9)}},
			},
		},
	}
}

// rootsBatch runs the ordering fixture's four inputs through
// scriptedBatch with rootsEffects.
func rootsBatch(t *testing.T) (Batch, []domain.Effects) {
	t.Helper()
	inputs := []ledger.Input{
		ledgerInput(0, callPayload(t, hashN(1), hashN(4), assetN(1), u256N(1000))),
		ledgerInput(1, callPayload(t, hashN(1), hashN(5), codec.AssetID{}, codec.U256{})),
		ledgerInput(2, callPayload(t, hashN(1), hashN(3), codec.AssetID{}, codec.U256{})),
		ledgerInput(3, callPayload(t, hashN(1), hashN(2), codec.AssetID{}, codec.U256{})),
	}
	return scriptedBatch(t, inputs, rootsEffects)
}

// fieldsEffects returns the receipt-field fixture's three scripted
// Effects. Every call allocates new slices.
func fieldsEffects() []domain.Effects {
	ev := func(idx uint32, data byte) codec.Event {
		return codec.Event{ContractID: hashN(4), InputIndex: 0, EventIndex: idx, Data: []byte{data}}
	}
	return []domain.Effects{
		{
			Status:     codec.StatusSuccess,
			ContractID: hashN(4),
			CodeHash:   hashN(0x14),
			GasUsed:    1000,
			Events:     []codec.Event{ev(0, 0x01), ev(1, 0x02), ev(2, 0x03)},
		},
		{Status: codec.StatusRevert, ContractID: hashN(5), CodeHash: hashN(0x15), GasUsed: 300, AbortCode: 7},
		{Status: codec.StatusValidationFailed, ContractID: hashN(6)},
	}
}

// encodeEvents encodes events in the given order.
func encodeEvents(t *testing.T, events []codec.Event) [][]byte {
	t.Helper()
	out := make([][]byte, len(events))
	for i, e := range events {
		b, err := e.Encode()
		if err != nil {
			t.Fatalf("encode event %d: %v", i, err)
		}
		out[i] = b
	}
	return out
}

// The batch eventRoot covers all batch events ordered by
// (input_index, event_index), with inputs that emit nothing adding no
// leaf.
func TestBatchEventRootOrder(t *testing.T) {
	b, effs := rootsBatch(t)

	var events []codec.Event
	for i := len(effs) - 1; i >= 0; i-- {
		events = append(events, effs[i].Events...)
	}
	sort.Slice(events, func(i, j int) bool {
		if events[i].InputIndex != events[j].InputIndex {
			return events[i].InputIndex < events[j].InputIndex
		}
		return events[i].EventIndex < events[j].EventIndex
	})
	if len(events) != 4 {
		t.Fatalf("len(events) = %d, want 4", len(events))
	}
	if events[0].InputIndex != 0 || events[0].EventIndex != 0 {
		t.Fatalf("sorted[0] = (%d, %d), want (0, 0)", events[0].InputIndex, events[0].EventIndex)
	}
	if events[2].InputIndex != 3 || events[2].EventIndex != 0 {
		t.Fatalf("sorted[2] = (%d, %d), want (3, 0)", events[2].InputIndex, events[2].EventIndex)
	}

	want := specRoot(t, encodeEvents(t, events))
	if got := b.Commitment.EventRoot; got != want {
		t.Errorf("EventRoot = %x, want %x", got[:], want[:])
	}
}

// The batch outboxRoot covers all batch messages ordered by
// (input_index, outbox_index), with each refund after its input's
// contract messages.
func TestBatchOutboxRootOrder(t *testing.T) {
	b, effs := rootsBatch(t)

	var msgs []codec.OutboxMessage
	for i := len(effs) - 1; i >= 0; i-- {
		msgs = append(msgs, effs[i].OutboxMessages...)
	}
	sort.Slice(msgs, func(i, j int) bool {
		if msgs[i].InputIndex != msgs[j].InputIndex {
			return msgs[i].InputIndex < msgs[j].InputIndex
		}
		return msgs[i].OutboxIndex < msgs[j].OutboxIndex
	})
	if len(msgs) != 4 {
		t.Fatalf("len(msgs) = %d, want 4", len(msgs))
	}
	contractMsgs := uint32(0)
	for _, m := range effs[0].OutboxMessages {
		if m.SourceContract != (types.Hash{}) {
			contractMsgs++
		}
	}
	if msgs[2].SourceContract != (types.Hash{}) || msgs[2].InputIndex != 0 || msgs[2].OutboxIndex != contractMsgs {
		t.Fatalf("sorted[2] = (source %x, input %d, outbox %d), want the refund of input 0 at outbox index %d",
			msgs[2].SourceContract[:], msgs[2].InputIndex, msgs[2].OutboxIndex, contractMsgs)
	}
	if msgs[3].InputIndex != 3 {
		t.Fatalf("sorted[3].InputIndex = %d, want 3", msgs[3].InputIndex)
	}

	leaves := make([][]byte, len(msgs))
	for i, m := range msgs {
		enc, err := m.Encode()
		if err != nil {
			t.Fatalf("encode message %d: %v", i, err)
		}
		leaves[i] = enc
	}
	want := specRoot(t, leaves)
	if got := b.Commitment.OutboxRoot; got != want {
		t.Errorf("OutboxRoot = %x, want %x", got[:], want[:])
	}
}

// The batch receiptRoot covers one receipt per input in input order,
// including inputs that fail or emit nothing.
func TestBatchReceiptRootOrder(t *testing.T) {
	b, effs := rootsBatch(t)

	leaves := make([][]byte, len(effs))
	for i, e := range effs {
		var match []codec.Receipt
		for _, r := range b.Receipts {
			if r.ContractID == e.ContractID {
				match = append(match, r)
			}
		}
		if len(match) != 1 {
			t.Fatalf("input %d: %d receipts with ContractID %x, want 1", i, len(match), e.ContractID[:])
		}
		r := match[0]
		if r.InputIndex != uint64(i) {
			t.Errorf("input %d: receipt InputIndex = %d, want %d", i, r.InputIndex, i)
		}
		if r.Status != e.Status {
			t.Errorf("input %d: receipt Status = %d, want %d", i, r.Status, e.Status)
		}
		enc, err := r.Encode()
		if err != nil {
			t.Fatalf("encode receipt %d: %v", i, err)
		}
		leaves[i] = enc
	}
	want := specRoot(t, leaves)
	if got := b.Commitment.ReceiptRoot; got != want {
		t.Errorf("ReceiptRoot = %x, want %x", got[:], want[:])
	}
}

// Each receipt carries the batch id and the domain's code hash, gas
// used, abort code and event count, and an event root over its own
// events (zero when it has none).
func TestBatchReceiptFields(t *testing.T) {
	targets := []types.Hash{hashN(4), hashN(5), hashN(6)}
	inputs := make([]ledger.Input, len(targets))
	for i, target := range targets {
		inputs[i] = ledgerInput(uint64(i), callPayload(t, hashN(1), target, codec.AssetID{}, codec.U256{}))
	}
	b, effs := scriptedBatch(t, inputs, fieldsEffects)

	for i, e := range effs {
		var match []codec.Receipt
		for _, r := range b.Receipts {
			if r.ContractID == e.ContractID {
				match = append(match, r)
			}
		}
		if len(match) != 1 {
			t.Fatalf("input %d: %d receipts with ContractID %x, want 1", i, len(match), e.ContractID[:])
		}
		r := match[0]

		var wantRoot types.Hash
		if len(e.Events) > 0 {
			wantRoot = specRoot(t, encodeEvents(t, e.Events))
		}
		if r.BatchID != 100 {
			t.Errorf("input %d: BatchID = %d, want 100", i, r.BatchID)
		}
		if r.CodeHash != e.CodeHash {
			t.Errorf("input %d: CodeHash = %x, want %x", i, r.CodeHash[:], e.CodeHash[:])
		}
		if r.GasUsed != e.GasUsed {
			t.Errorf("input %d: GasUsed = %d, want %d", i, r.GasUsed, e.GasUsed)
		}
		if r.AbortCode != e.AbortCode {
			t.Errorf("input %d: AbortCode = %d, want %d", i, r.AbortCode, e.AbortCode)
		}
		if r.EventCount != uint32(len(e.Events)) {
			t.Errorf("input %d: EventCount = %d, want %d", i, r.EventCount, len(e.Events))
		}
		if r.EventRoot != wantRoot {
			t.Errorf("input %d: EventRoot = %x, want %x", i, r.EventRoot[:], wantRoot[:])
		}
	}
}
