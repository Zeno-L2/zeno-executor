package batch

import (
	"testing"

	"github.com/zenon-network/go-zenon/common/types"

	"github.com/Zeno-L2/zeno-executor/internal/codec"
)

// requireStrictlyAscending fails unless the decoded (InputIndex,
// OutboxIndex) pairs of ws are each strictly greater than the one
// before.
func requireStrictlyAscending(t testing.TB, ws []Withdrawal) {
	t.Helper()
	var prevIn uint64
	var prevOut uint32
	for i, w := range ws {
		m, err := codec.DecodeOutboxMessage(w.Message)
		if err != nil {
			t.Fatalf("withdrawal %d: decode: %v", i, err)
		}
		if i > 0 && (m.InputIndex < prevIn || (m.InputIndex == prevIn && m.OutboxIndex <= prevOut)) {
			t.Fatalf("withdrawal %d (%d, %d) is not after (%d, %d)", i, m.InputIndex, m.OutboxIndex, prevIn, prevOut)
		}
		prevIn, prevOut = m.InputIndex, m.OutboxIndex
	}
}

// proofSteps returns the step count of a MerkleProof.
func proofSteps(proof []byte) int {
	return int(proof[0])<<24 | int(proof[1])<<16 | int(proof[2])<<8 | int(proof[3])
}

// recordFor encodes a bundle record whose input data is a single zero
// byte and whose result carries msgs, with the result's
// GlobalInputIndex resultIndex.
func recordFor(tb testing.TB, recordIndex, resultIndex uint64, msgs []codec.OutboxMessage) codec.BundleInputRecord {
	tb.Helper()
	res, err := codec.ExecutionResult{
		Version:          codec.ExecutionResultVersion,
		GlobalInputIndex: resultIndex,
		Outbox:           msgs,
	}.Encode()
	if err != nil {
		tb.Fatalf("encode execution result: %v", err)
	}
	return codec.BundleInputRecord{GlobalInputIndex: recordIndex, InputData: []byte{0}, ExecutionResult: res}
}

func withdrawalMsg(input uint64, outbox uint32) codec.OutboxMessage {
	return codec.OutboxMessage{
		SourceContract: hashN(4),
		InputIndex:     input,
		OutboxIndex:    outbox,
		Target:         codec.L1Withdrawal{RecipientL1: types.Address{1}, Asset: assetN(1), Amount: u256N(uint64(outbox) + 1)},
	}
}

func deliveryMsg(input uint64, outbox uint32) codec.OutboxMessage {
	return codec.OutboxMessage{
		SourceContract: hashN(4),
		InputIndex:     input,
		OutboxIndex:    outbox,
		Target:         codec.L2Delivery{TargetContract: hashN(9)},
		Payload:        []byte{1},
	}
}

// bundleAndRoot encodes records and returns the bundle with the root
// over every outbox leaf.
func bundleAndRoot(tb testing.TB, records []codec.BundleInputRecord, results ...[]codec.OutboxMessage) ([]byte, types.Hash) {
	tb.Helper()
	b, err := codec.EncodeBundle(records)
	if err != nil {
		tb.Fatalf("encode bundle: %v", err)
	}
	var leaves [][]byte
	for _, msgs := range results {
		for _, m := range msgs {
			enc, err := m.Encode()
			if err != nil {
				tb.Fatalf("encode message: %v", err)
			}
			leaves = append(leaves, enc)
		}
	}
	return b, codec.MerkleRoot(leaves)
}

func TestWithdrawalsRootsBatch(t *testing.T) {
	b, effs := rootsBatch(t)
	ws, err := Withdrawals(b.Bundle, b.Commitment.OutboxRoot)
	if err != nil {
		t.Fatalf("Withdrawals: %v", err)
	}
	if len(ws) != 2 {
		t.Fatalf("got %d withdrawals, want 2", len(ws))
	}
	for i, idx := range []int{1, 2} {
		want, err := effs[0].OutboxMessages[idx].Encode()
		if err != nil {
			t.Fatal(err)
		}
		if string(ws[i].Message) != string(want) {
			t.Errorf("withdrawal %d message differs from OutboxMessages[%d]", i, idx)
		}
		if err := codec.VerifyMerkleProof(b.Commitment.OutboxRoot, ws[i].Message, ws[i].Proof); err != nil {
			t.Errorf("withdrawal %d proof: %v", i, err)
		}
	}
	requireStrictlyAscending(t, ws)
}

func TestWithdrawalsNoKindOne(t *testing.T) {
	empty, emptyRoot := bundleAndRoot(t, []codec.BundleInputRecord{recordFor(t, 0, 0, nil)}, nil)
	msgs := []codec.OutboxMessage{deliveryMsg(0, 0)}
	only0, only0Root := bundleAndRoot(t, []codec.BundleInputRecord{recordFor(t, 0, 0, msgs)}, msgs)

	for name, tc := range map[string]struct {
		bundle []byte
		root   types.Hash
	}{"no outbox": {empty, emptyRoot}, "kind 0 only": {only0, only0Root}} {
		ws, err := Withdrawals(tc.bundle, tc.root)
		if err != nil || ws != nil {
			t.Errorf("%s: got %v, %v; want nil, nil", name, ws, err)
		}
		if _, err := Withdrawals(tc.bundle, hashN(0xEE)); err == nil {
			t.Errorf("%s: wrong root accepted", name)
		}
	}
}

// Zero-amount withdrawals and refunds (zero source contract) are
// returned like any other withdrawal.
func TestWithdrawalsZeroAmountAndRefund(t *testing.T) {
	zero := withdrawalMsg(0, 0)
	zero.Target = codec.L1Withdrawal{RecipientL1: types.Address{1}, Asset: assetN(1)}
	refund := withdrawalMsg(0, 1)
	refund.SourceContract = types.Hash{}
	msgs := []codec.OutboxMessage{zero, refund, deliveryMsg(0, 2), withdrawalMsg(0, 3)}
	bundle, root := bundleAndRoot(t, []codec.BundleInputRecord{recordFor(t, 0, 0, msgs)}, msgs)

	ws, err := Withdrawals(bundle, root)
	if err != nil {
		t.Fatalf("Withdrawals: %v", err)
	}
	if len(ws) != 3 {
		t.Fatalf("got %d withdrawals, want 3", len(ws))
	}
	for i, idx := range []int{0, 1, 3} {
		want, err := msgs[idx].Encode()
		if err != nil {
			t.Fatal(err)
		}
		if string(ws[i].Message) != string(want) {
			t.Errorf("withdrawal %d is not message %d", i, idx)
		}
		if err := codec.VerifyMerkleProof(root, ws[i].Message, ws[i].Proof); err != nil {
			t.Errorf("withdrawal %d proof: %v", i, err)
		}
	}
	requireStrictlyAscending(t, ws)
}

func TestWithdrawalsRootMismatch(t *testing.T) {
	b, _ := rootsBatch(t)
	if _, err := Withdrawals(b.Bundle, hashN(0xEE)); err == nil {
		t.Fatal("wrong root accepted")
	}
}

func TestWithdrawalsTruncatedBundle(t *testing.T) {
	b, _ := rootsBatch(t)
	if _, err := Withdrawals(b.Bundle[:len(b.Bundle)-1], b.Commitment.OutboxRoot); err == nil {
		t.Fatal("truncated bundle accepted")
	}
}

// The records ascend (5, 6) but their results carry input indexes 6
// then 5 (and 5, 5), which the codec does not tie together. The root
// covers the leaves as they sit, so only the order check rejects them.
func TestWithdrawalsOrderCheck(t *testing.T) {
	for name, idx := range map[string][2]uint64{"descending": {6, 5}, "repeated": {5, 5}} {
		a := []codec.OutboxMessage{withdrawalMsg(idx[0], 0)}
		c := []codec.OutboxMessage{withdrawalMsg(idx[1], 0)}
		bundle, root := bundleAndRoot(t, []codec.BundleInputRecord{recordFor(t, 5, idx[0], a), recordFor(t, 6, idx[1], c)}, a, c)
		if _, err := Withdrawals(bundle, root); err == nil {
			t.Errorf("%s: out-of-order results accepted", name)
		}
	}
}

// bigBundle builds 64 records of 33 outbox messages each (2,112
// leaves), alternating kind 0 and kind 1 and starting with kind 0, so
// 1,024 are withdrawals.
func bigBundle(tb testing.TB) ([]byte, types.Hash) {
	tb.Helper()
	var records []codec.BundleInputRecord
	var results [][]codec.OutboxMessage
	for in := uint64(0); in < MaxBatchInputs; in++ {
		msgs := make([]codec.OutboxMessage, codec.MaxOutboxPerResult)
		for o := range msgs {
			if o%2 == 0 {
				msgs[o] = deliveryMsg(in, uint32(o))
			} else {
				msgs[o] = withdrawalMsg(in, uint32(o))
			}
		}
		records = append(records, recordFor(tb, in, in, msgs))
		results = append(results, msgs)
	}
	return bundleAndRoot(tb, records, results...)
}

func TestWithdrawalsMaximalBatch(t *testing.T) {
	bundle, root := bigBundle(t)
	ws, err := Withdrawals(bundle, root)
	if err != nil {
		t.Fatalf("Withdrawals: %v", err)
	}
	if len(ws) != 1024 {
		t.Fatalf("got %d withdrawals, want 1024", len(ws))
	}
	requireStrictlyAscending(t, ws)
	for i, w := range ws {
		if err := codec.VerifyMerkleProof(root, w.Message, w.Proof); err != nil {
			t.Fatalf("withdrawal %d: %v", i, err)
		}
		m, err := codec.DecodeOutboxMessage(w.Message)
		if err != nil {
			t.Fatal(err)
		}
		pos := int(m.InputIndex)*codec.MaxOutboxPerResult + int(m.OutboxIndex)
		want := 12
		if pos >= 2048 {
			want = 7
		}
		if got := proofSteps(w.Proof); got != want {
			t.Fatalf("withdrawal at leaf %d: %d proof steps, want %d", pos, got, want)
		}
	}
}
