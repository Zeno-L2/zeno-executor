package batch

import (
	"bytes"
	"testing"

	"github.com/zenon-network/go-zenon/common/trie"

	"github.com/digitalSloth/go-executor/domain"
	"github.com/digitalSloth/go-executor/internal/codec"
	"github.com/digitalSloth/go-executor/internal/ledger"
)

// --- test 3: absent read produces a verifiable non-inclusion witness ---

func TestBuilderAbsentReadWitness(t *testing.T) {
	src := hashN(1)
	contractID := hashN(2)
	absent := pathN(5)

	inputs := []ledger.Input{ledgerInput(0, callPayload(t, src, contractID, codec.AssetID{}, codec.U256{}))}
	dom := &fakeDomain{steps: []fakeStep{
		{reads: []trie.Path{absent}, effects: domain.Effects{Status: codec.StatusSuccess, ContractID: contractID}},
	}}
	cfg := testConfig(t, dom, &fakeInputSource{inputs: inputs}, newFakeBundleStore())
	d, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	got, ok, err := d.NextBatch()
	if err != nil || !ok {
		t.Fatalf("NextBatch: ok=%v err=%v", ok, err)
	}

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
		if w.DerivedKey == absent {
			proof = w.Proof
			found = true
		}
	}
	if !found {
		t.Fatalf("no witness for the absent path %x", absent)
	}

	ok, err = trie.VerifyAbsenceByPath(got.Commitment.PreStateRoot, absent, proof)
	if err != nil {
		t.Fatalf("VerifyAbsenceByPath: %v", err)
	}
	if !ok {
		t.Fatalf("VerifyAbsenceByPath = false, want true")
	}
}

// --- witness Value: present, present-empty, absent ---

func TestBuilderWitnessValuePresentPresentEmptyAbsent(t *testing.T) {
	src := hashN(1)
	contractID := hashN(2)
	present := pathN(6)
	presentEmpty := pathN(7)
	absent := pathN(8)

	inputs := []ledger.Input{
		ledgerInput(0, callPayload(t, src, contractID, codec.AssetID{}, codec.U256{})),
		ledgerInput(1, callPayload(t, src, contractID, codec.AssetID{}, codec.U256{})),
	}
	dom := &fakeDomain{steps: []fakeStep{
		{effects: domain.Effects{
			Status:     codec.StatusSuccess,
			ContractID: contractID,
			StateDiff: []domain.StateDiffEntry{
				{Path: present, Value: []byte("v")},
				{Path: presentEmpty, Value: []byte{}},
			},
		}},
		{reads: []trie.Path{present, presentEmpty, absent}, effects: domain.Effects{Status: codec.StatusSuccess, ContractID: contractID}},
	}}
	cfg := testConfig(t, dom, &fakeInputSource{inputs: inputs}, newFakeBundleStore())
	d, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	got, ok, err := d.NextBatch()
	if err != nil || !ok {
		t.Fatalf("NextBatch: ok=%v err=%v", ok, err)
	}
	if len(got.Receipts) != 2 {
		t.Fatalf("len(Receipts) = %d, want 2", len(got.Receipts))
	}

	decoded, err := codec.DecodeBundle(got.Bundle)
	if err != nil {
		t.Fatalf("DecodeBundle: %v", err)
	}
	if len(decoded) != 2 {
		t.Fatalf("len(decoded) = %d, want 2", len(decoded))
	}

	values := map[trie.Path][]byte{}
	for _, w := range decoded[1].Witnesses {
		values[w.DerivedKey] = w.Value
	}

	if v, ok := values[present]; !ok || !bytes.Equal(v, []byte("v")) {
		t.Fatalf("present witness Value = %v, want %q", v, "v")
	}
	if v, ok := values[presentEmpty]; !ok || v == nil || len(v) != 0 {
		t.Fatalf("present-empty witness Value = %v, want non-nil zero-length", v)
	}
	if v, ok := values[absent]; !ok || v != nil {
		t.Fatalf("absent witness Value = %v, want nil", v)
	}
}

// --- test 10: two-asset summary ---

func TestBuilderTwoAssetSummary(t *testing.T) {
	contractID := hashN(2)
	src := hashN(1)
	assetA := assetN(1)
	assetB := assetN(2)

	inputs := []ledger.Input{
		ledgerInput(0, callPayload(t, src, contractID, assetA, u256N(50))),
		ledgerInput(1, callPayload(t, src, contractID, assetB, u256N(30))),
	}
	// Input 0: a deposit in assetA plus a contract withdrawal in assetA (merged into one
	// entry). Input 1: a deposit in assetB alone.
	dom := &fakeDomain{steps: []fakeStep{
		{effects: withdrawalEffects(contractID, 0, assetA, u256N(5))},
		{effects: domain.Effects{Status: codec.StatusSuccess, ContractID: contractID}},
	}}
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
	if len(summary) != 2 {
		t.Fatalf("len(AssetFlowSummary) = %d, want 2: %+v", len(summary), summary)
	}
	if bytes.Compare(summary[0].Asset[:], summary[1].Asset[:]) >= 0 {
		t.Fatalf("summary not ascending by asset bytes: %+v", summary)
	}
	if summary[0].Asset != assetA {
		t.Fatalf("summary[0].Asset = %x, want assetA %x", summary[0].Asset, assetA)
	}
	if summary[0].DepositCredit != u256N(50) {
		t.Fatalf("summary[0].DepositCredit = %x, want %x", summary[0].DepositCredit, u256N(50))
	}
	if summary[0].WithdrawalDebit != u256N(5) {
		t.Fatalf("summary[0].WithdrawalDebit = %x, want %x (merged with the deposit contribution)", summary[0].WithdrawalDebit, u256N(5))
	}
	if summary[1].Asset != assetB {
		t.Fatalf("summary[1].Asset = %x, want assetB %x", summary[1].Asset, assetB)
	}
	if summary[1].DepositCredit != u256N(30) {
		t.Fatalf("summary[1].DepositCredit = %x, want %x", summary[1].DepositCredit, u256N(30))
	}

	if _, err := summary.Encode(); err != nil {
		t.Fatalf("AssetFlowSummary.Encode: %v", err)
	}
}
