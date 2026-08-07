package wasm

import (
	"encoding/binary"
	"math"
	"reflect"
	"testing"

	"github.com/zenon-network/go-zenon/common/types"

	"github.com/digitalSloth/go-executor/domain"
	"github.com/digitalSloth/go-executor/internal/codec"
	"github.com/digitalSloth/go-executor/plugins/wasm/internal/engine"
)

// testContractID, testCodeHash and testL1Sender are the fixture identities every effects_test
// settlement is built from.
var (
	testContractID = types.Hash{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	testCodeHash   = types.Hash{9, 9, 9, 9}
	testL1Sender   = types.Address{5, 5, 5, 5, 5}
	testOtherHash  = types.Hash{0xAA, 0xBB, 0xCC}
)

// u256 packs v into the low bytes of a big-endian U256.
func u256(v uint64) codec.U256 {
	var out codec.U256
	binary.BigEndian.PutUint64(out[24:], v)
	return out
}

// baseSettlement is the fixture settlement every effects_test builds on: GlobalInputIndex 42,
// a 1000-unit deposit and a 1,000,000-gas budget.
func baseSettlement() settlement {
	return settlement{
		ContractID:       testContractID,
		CodeHash:         testCodeHash,
		GlobalInputIndex: 42,
		L1Sender:         testL1Sender,
		DepositAsset:     codec.AssetID{},
		DepositAmount:    u256(1000),
		GasBudget:        1_000_000,
	}
}

// validEffects returns a ContractEffects blob correctly attributed to s.ContractID: one
// StateDiff entry, one present and one deletion, one event, one outbox message, non-empty
// ReturnData, and claimed at claimed units.
func validEffects(s settlement, claimed uint64) codec.ContractEffects {
	return codec.ContractEffects{
		Version:          codec.ExecutionResultVersion,
		GlobalInputIndex: s.GlobalInputIndex,
		StateDiff: []codec.LocalStateDiffEntry{
			{LocalKey: []byte("k1"), Value: []byte("v1")},
			{LocalKey: []byte("k2"), Value: nil},
		},
		Events: []codec.Event{
			{ContractID: s.ContractID, InputIndex: s.GlobalInputIndex, EventIndex: 0, Data: []byte("event-data")},
		},
		Outbox: []codec.OutboxMessage{
			{SourceContract: s.ContractID, InputIndex: s.GlobalInputIndex, OutboxIndex: 0, Target: codec.L2Delivery{TargetContract: testOtherHash}, Payload: []byte("payload")},
		},
		ReturnData:     []byte("return-data"),
		ClaimedDeposit: u256(claimed),
	}
}

func encodeEffects(t *testing.T, eff codec.ContractEffects) []byte {
	t.Helper()
	b, err := eff.Encode()
	if err != nil {
		t.Fatalf("ContractEffects.Encode: %v", err)
	}
	return b
}

// assertRefundOnlyShape asserts eff carries no contract-declared content: nil StateDiff/Events,
// zero ClaimedDeposit, nil ReturnData, and exactly the refund s.refund(zero, 0) would produce.
func assertRefundOnlyShape(t *testing.T, s settlement, eff domainEffects) {
	t.Helper()
	if eff.StateDiff != nil {
		t.Errorf("StateDiff = %v, want nil", eff.StateDiff)
	}
	if eff.Events != nil {
		t.Errorf("Events = %v, want nil", eff.Events)
	}
	if eff.ReturnData != nil {
		t.Errorf("ReturnData = %v, want nil", eff.ReturnData)
	}
	if eff.ClaimedDeposit != (codec.U256{}) {
		t.Errorf("ClaimedDeposit = %x, want zero", eff.ClaimedDeposit)
	}
	want := s.refund(codec.U256{}, 0)
	if !reflect.DeepEqual(eff.OutboxMessages, want) {
		t.Errorf("OutboxMessages = %+v, want %+v", eff.OutboxMessages, want)
	}
}

func TestSettleStatusMap(t *testing.T) {
	s := baseSettlement()
	metered := uint64(100_000)

	validBlobEff := validEffects(s, 500)
	validCharge := effectCharge(validBlobEff)

	forgedEventEff := validEffects(s, 500)
	forgedEventEff.Events[0].ContractID = testOtherHash

	reservedKeyEff := validEffects(s, 500)
	reservedKeyEff.StateDiff = []codec.LocalStateDiffEntry{{LocalKey: keyCodeHash, Value: []byte{1}}}

	oversizedReturnEff := validEffects(s, 500)
	oversizedReturnEff.ReturnData = make([]byte, MaxReturnData+1)
	oversizedReturnCharge := effectCharge(oversizedReturnEff)

	oversizedResultEff := validEffects(s, 500)
	oversizedResultEff.StateDiff = []codec.LocalStateDiffEntry{
		{LocalKey: []byte("big1"), Value: make([]byte, codec.MaxStateValueSize)},
		{LocalKey: []byte("big2"), Value: make([]byte, codec.MaxStateValueSize)},
		{LocalKey: []byte("big3"), Value: make([]byte, codec.MaxStateValueSize)},
	}
	oversizedResultCharge := effectCharge(oversizedResultEff)

	unaffordableSettlement := s
	unaffordableSettlement.GasBudget = metered
	unaffordableEff := codec.ContractEffects{Version: codec.ExecutionResultVersion, GlobalInputIndex: s.GlobalInputIndex, ClaimedDeposit: codec.U256{}}

	cases := []struct {
		name          string
		s             settlement
		out           engine.Outcome
		wantStatus    codec.Status
		wantGasUsed   uint64
		wantAbortCode int32
	}{
		{"OutOfGas", s, engine.Outcome{Kind: engine.OutOfGas}, codec.StatusOutOfGas, s.GasBudget, 0},
		{"Trapped", s, engine.Outcome{Kind: engine.Trapped, GasUsed: metered}, codec.StatusRuntimeFault, metered, 0},
		{"Aborted", s, engine.Outcome{Kind: engine.Aborted, GasUsed: metered, AbortCode: 7}, codec.StatusRevert, metered, 7},
		{"ReturnedNegative", s, engine.Outcome{Kind: engine.Returned, Ret: -5, GasUsed: metered}, codec.StatusRevert, metered, 5},
		{"ValidBlob", s, engine.Outcome{Kind: engine.Returned, Ret: 0, GasUsed: metered, ResultBlob: encodeEffects(t, validBlobEff)}, codec.StatusSuccess, metered + validCharge, 0},
		{"MalformedBlob", s, engine.Outcome{Kind: engine.Returned, Ret: 0, GasUsed: metered, ResultBlob: []byte{0xFF, 0xFF}}, codec.StatusRuntimeFault, metered, 0},
		{"ForgedEventContractID", s, engine.Outcome{Kind: engine.Returned, Ret: 0, GasUsed: metered, ResultBlob: encodeEffects(t, forgedEventEff)}, codec.StatusRuntimeFault, metered, 0},
		{"ReservedKeyWrite", s, engine.Outcome{Kind: engine.Returned, Ret: 0, GasUsed: metered, ResultBlob: encodeEffects(t, reservedKeyEff)}, codec.StatusUnauthorized, metered, 0},
		{"UnaffordableEffectCharge", unaffordableSettlement, engine.Outcome{Kind: engine.Returned, Ret: 0, GasUsed: metered, ResultBlob: encodeEffects(t, unaffordableEff)}, codec.StatusOutOfGas, unaffordableSettlement.GasBudget, 0},
		{"ResultTooLargeReturnData", s, engine.Outcome{Kind: engine.Returned, Ret: 0, GasUsed: metered, ResultBlob: encodeEffects(t, oversizedReturnEff)}, codec.StatusResultTooLarge, metered + oversizedReturnCharge, 0},
		{"ResultTooLargeExecutionResult", s, engine.Outcome{Kind: engine.Returned, Ret: 0, GasUsed: metered, ResultBlob: encodeEffects(t, oversizedResultEff)}, codec.StatusResultTooLarge, metered + oversizedResultCharge, 0},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			eff, err := tc.s.settle(tc.out)
			if err != nil {
				t.Fatalf("settle: %v", err)
			}
			if eff.Status != tc.wantStatus {
				t.Errorf("Status = %v, want %v", eff.Status, tc.wantStatus)
			}
			if eff.GasUsed != tc.wantGasUsed {
				t.Errorf("GasUsed = %d, want %d", eff.GasUsed, tc.wantGasUsed)
			}
			if eff.AbortCode != tc.wantAbortCode {
				t.Errorf("AbortCode = %d, want %d", eff.AbortCode, tc.wantAbortCode)
			}
		})
	}
}

func TestSettleUnknownOutcomeKindHalts(t *testing.T) {
	s := baseSettlement()
	_, err := s.settle(engine.Outcome{Kind: engine.OutcomeKind(99)})
	if err == nil {
		t.Fatal("settle: err = nil, want non-nil error for an unrecognized outcome kind")
	}
}

func TestSettleAbortCode(t *testing.T) {
	s := baseSettlement()
	metered := uint64(50_000)

	abortCases := []struct {
		name string
		code int32
	}{
		{"zero", 0},
		{"one", 1},
		{"negativeOne", -1},
		{"maxInt32", math.MaxInt32},
	}
	for _, tc := range abortCases {
		t.Run("Aborted/"+tc.name, func(t *testing.T) {
			out := engine.Outcome{Kind: engine.Aborted, GasUsed: metered, AbortCode: tc.code}
			eff, err := s.settle(out)
			if err != nil {
				t.Fatalf("settle: %v", err)
			}
			if eff.Status != codec.StatusRevert {
				t.Fatalf("Status = %v, want StatusRevert", eff.Status)
			}
			if eff.AbortCode != tc.code {
				t.Fatalf("AbortCode = %d, want %d", eff.AbortCode, tc.code)
			}
			assertRefundOnlyShape(t, s, eff)
		})
	}

	retCases := []struct {
		name string
		ret  int64
		want int32
	}{
		{"minusOne", -1, 1},
		{"minusTwo", -2, 2},
		{"minus1Shl32", -(int64(1) << 32), 0},
		{"minus1Shl32Plus5", -(int64(1)<<32 + 5), 5},
		{"minInt64", math.MinInt64, 0},
	}
	for _, tc := range retCases {
		t.Run("Returned/"+tc.name, func(t *testing.T) {
			out := engine.Outcome{Kind: engine.Returned, Ret: tc.ret, GasUsed: metered}
			eff, err := s.settle(out)
			if err != nil {
				t.Fatalf("settle: %v", err)
			}
			if eff.Status != codec.StatusRevert {
				t.Fatalf("Status = %v, want StatusRevert", eff.Status)
			}
			if eff.AbortCode != tc.want {
				t.Fatalf("AbortCode = %d, want %d", eff.AbortCode, tc.want)
			}
			assertRefundOnlyShape(t, s, eff)
		})
	}
}

func TestSettleRootsInBlobRejected(t *testing.T) {
	s := baseSettlement()
	execResult := codec.ExecutionResult{Version: codec.ExecutionResultVersion, GlobalInputIndex: s.GlobalInputIndex, ClaimedDeposit: codec.U256{}}
	b, err := execResult.Encode()
	if err != nil {
		t.Fatalf("ExecutionResult.Encode: %v", err)
	}
	eff, err := s.settle(engine.Outcome{Kind: engine.Returned, GasUsed: 1000, ResultBlob: b})
	if err != nil {
		t.Fatalf("settle: %v", err)
	}
	if eff.Status != codec.StatusRuntimeFault {
		t.Fatalf("Status = %v, want StatusRuntimeFault", eff.Status)
	}
}

func TestSettleTruncatedBlobRejected(t *testing.T) {
	s := baseSettlement()
	b := encodeEffects(t, validEffects(s, 500))
	truncated := b[:len(b)-32] // drop claimed_deposit

	eff, err := s.settle(engine.Outcome{Kind: engine.Returned, GasUsed: 1000, ResultBlob: truncated})
	if err != nil {
		t.Fatalf("settle: %v", err)
	}
	if eff.Status != codec.StatusRuntimeFault {
		t.Fatalf("Status = %v, want StatusRuntimeFault", eff.Status)
	}
}

func TestSettleGlobalInputIndexMismatch(t *testing.T) {
	s := baseSettlement()
	// No events/outbox, so the blob's own internal InputIndex-consistency check (unrelated to
	// step 2) has nothing to enforce; only c.GlobalInputIndex itself disagrees with s's.
	eff := codec.ContractEffects{Version: codec.ExecutionResultVersion, GlobalInputIndex: s.GlobalInputIndex + 1, ClaimedDeposit: codec.U256{}}

	got, err := s.settle(engine.Outcome{Kind: engine.Returned, GasUsed: 1000, ResultBlob: encodeEffects(t, eff)})
	if err != nil {
		t.Fatalf("settle: %v", err)
	}
	if got.Status != codec.StatusRuntimeFault {
		t.Fatalf("Status = %v, want StatusRuntimeFault", got.Status)
	}
}

func TestSettleAttribution(t *testing.T) {
	s := baseSettlement()
	metered := uint64(1000)

	t.Run("EventForged", func(t *testing.T) {
		eff := validEffects(s, 500)
		eff.Events[0].ContractID = testOtherHash
		got, err := s.settle(engine.Outcome{Kind: engine.Returned, GasUsed: metered, ResultBlob: encodeEffects(t, eff)})
		if err != nil {
			t.Fatalf("settle: %v", err)
		}
		if got.Status != codec.StatusRuntimeFault || got.GasUsed != metered {
			t.Fatalf("Status/GasUsed = %v/%d, want RuntimeFault/%d", got.Status, got.GasUsed, metered)
		}
		assertRefundOnlyShape(t, s, got)
	})

	t.Run("OutboxForged", func(t *testing.T) {
		eff := validEffects(s, 500)
		eff.Outbox[0].SourceContract = testOtherHash
		got, err := s.settle(engine.Outcome{Kind: engine.Returned, GasUsed: metered, ResultBlob: encodeEffects(t, eff)})
		if err != nil {
			t.Fatalf("settle: %v", err)
		}
		if got.Status != codec.StatusRuntimeFault || got.GasUsed != metered {
			t.Fatalf("Status/GasUsed = %v/%d, want RuntimeFault/%d", got.Status, got.GasUsed, metered)
		}
		assertRefundOnlyShape(t, s, got)
	})

	t.Run("OutboxForgedZero", func(t *testing.T) {
		eff := validEffects(s, 500)
		eff.Outbox[0].SourceContract = types.Hash{} // the SPEC.md:1190 refund sentinel
		got, err := s.settle(engine.Outcome{Kind: engine.Returned, GasUsed: metered, ResultBlob: encodeEffects(t, eff)})
		if err != nil {
			t.Fatalf("settle: %v", err)
		}
		if got.Status != codec.StatusRuntimeFault || got.GasUsed != metered {
			t.Fatalf("Status/GasUsed = %v/%d, want RuntimeFault/%d", got.Status, got.GasUsed, metered)
		}
		assertRefundOnlyShape(t, s, got)
	})

	t.Run("OutboxForgedZeroContractIDAlsoZero", func(t *testing.T) {
		zeroContract := s
		zeroContract.ContractID = types.Hash{}
		eff := validEffects(zeroContract, 500)
		eff.Outbox[0].SourceContract = types.Hash{} // the SPEC.md:1190 refund sentinel
		got, err := zeroContract.settle(engine.Outcome{Kind: engine.Returned, GasUsed: metered, ResultBlob: encodeEffects(t, eff)})
		if err != nil {
			t.Fatalf("settle: %v", err)
		}
		if got.Status != codec.StatusRuntimeFault || got.GasUsed != metered {
			t.Fatalf("Status/GasUsed = %v/%d, want RuntimeFault/%d", got.Status, got.GasUsed, metered)
		}
	})

	t.Run("SecondEventForged", func(t *testing.T) {
		eff := validEffects(s, 500)
		eff.Events = []codec.Event{
			{ContractID: s.ContractID, InputIndex: s.GlobalInputIndex, EventIndex: 0, Data: []byte("ok")},
			{ContractID: testOtherHash, InputIndex: s.GlobalInputIndex, EventIndex: 1, Data: []byte("forged")},
		}
		got, err := s.settle(engine.Outcome{Kind: engine.Returned, GasUsed: metered, ResultBlob: encodeEffects(t, eff)})
		if err != nil {
			t.Fatalf("settle: %v", err)
		}
		if got.Status != codec.StatusRuntimeFault || got.GasUsed != metered {
			t.Fatalf("Status/GasUsed = %v/%d, want RuntimeFault/%d", got.Status, got.GasUsed, metered)
		}
	})

	t.Run("PrecedesReservedKeyCheck", func(t *testing.T) {
		eff := validEffects(s, 500)
		eff.Events[0].ContractID = testOtherHash
		eff.StateDiff = []codec.LocalStateDiffEntry{{LocalKey: keyCodeHash, Value: []byte{1}}}
		got, err := s.settle(engine.Outcome{Kind: engine.Returned, GasUsed: metered, ResultBlob: encodeEffects(t, eff)})
		if err != nil {
			t.Fatalf("settle: %v", err)
		}
		if got.Status != codec.StatusRuntimeFault {
			t.Fatalf("Status = %v, want StatusRuntimeFault (2a precedes step 3)", got.Status)
		}
	})
}

func TestSettleDuplicateDerivedKeys(t *testing.T) {
	s := baseSettlement()
	eff := validEffects(s, 500)
	eff.StateDiff = []codec.LocalStateDiffEntry{
		{LocalKey: []byte("dup"), Value: []byte("a")},
		{LocalKey: []byte("dup"), Value: []byte("b")},
	}
	got, err := s.settle(engine.Outcome{Kind: engine.Returned, GasUsed: 1000, ResultBlob: encodeEffects(t, eff)})
	if err != nil {
		t.Fatalf("settle: %v", err)
	}
	if got.Status != codec.StatusRuntimeFault {
		t.Fatalf("Status = %v, want StatusRuntimeFault", got.Status)
	}
}

func TestSettleClaimBounds(t *testing.T) {
	t.Run("ClaimedExceedsDepositAmount", func(t *testing.T) {
		s := baseSettlement()
		eff := validEffects(s, 1001) // depositAmount = 1000
		got, err := s.settle(engine.Outcome{Kind: engine.Returned, GasUsed: 1000, ResultBlob: encodeEffects(t, eff)})
		if err != nil {
			t.Fatalf("settle: %v", err)
		}
		if got.Status != codec.StatusRuntimeFault {
			t.Fatalf("Status = %v, want StatusRuntimeFault", got.Status)
		}
	})

	t.Run("ClaimedNonZeroWithZeroDeposit", func(t *testing.T) {
		s := baseSettlement()
		s.DepositAmount = codec.U256{}
		eff := validEffects(s, 1)
		got, err := s.settle(engine.Outcome{Kind: engine.Returned, GasUsed: 1000, ResultBlob: encodeEffects(t, eff)})
		if err != nil {
			t.Fatalf("settle: %v", err)
		}
		if got.Status != codec.StatusRuntimeFault {
			t.Fatalf("Status = %v, want StatusRuntimeFault", got.Status)
		}
	})
}

func TestSettleReservedKeyWrite(t *testing.T) {
	s := baseSettlement()
	for _, k := range reservedKeys {
		t.Run(string(k), func(t *testing.T) {
			eff := validEffects(s, 500)
			eff.StateDiff = []codec.LocalStateDiffEntry{{LocalKey: append([]byte(nil), k...), Value: []byte{1}}}
			got, err := s.settle(engine.Outcome{Kind: engine.Returned, GasUsed: 1000, ResultBlob: encodeEffects(t, eff)})
			if err != nil {
				t.Fatalf("settle: %v", err)
			}
			if got.Status != codec.StatusUnauthorized {
				t.Fatalf("Status = %v, want StatusUnauthorized", got.Status)
			}
		})
	}
}

func TestSettleStep3PositionalPrecedence(t *testing.T) {
	s := baseSettlement()

	t.Run("ReservedThenDuplicate", func(t *testing.T) {
		eff := validEffects(s, 500)
		eff.StateDiff = []codec.LocalStateDiffEntry{
			{LocalKey: append([]byte(nil), keyCodeHash...), Value: []byte{1}},
			{LocalKey: []byte("k"), Value: []byte("a")},
			{LocalKey: []byte("k"), Value: []byte("b")},
		}
		got, err := s.settle(engine.Outcome{Kind: engine.Returned, GasUsed: 1000, ResultBlob: encodeEffects(t, eff)})
		if err != nil {
			t.Fatalf("settle: %v", err)
		}
		if got.Status != codec.StatusUnauthorized {
			t.Fatalf("Status = %v, want StatusUnauthorized (reserved entry reached first)", got.Status)
		}
	})

	t.Run("DuplicateThenReserved", func(t *testing.T) {
		eff := validEffects(s, 500)
		eff.StateDiff = []codec.LocalStateDiffEntry{
			{LocalKey: []byte("k"), Value: []byte("a")},
			{LocalKey: []byte("k"), Value: []byte("b")},
			{LocalKey: append([]byte(nil), keyCodeHash...), Value: []byte{1}},
		}
		got, err := s.settle(engine.Outcome{Kind: engine.Returned, GasUsed: 1000, ResultBlob: encodeEffects(t, eff)})
		if err != nil {
			t.Fatalf("settle: %v", err)
		}
		if got.Status != codec.StatusRuntimeFault {
			t.Fatalf("Status = %v, want StatusRuntimeFault (duplicate key reached first)", got.Status)
		}
	})
}

func TestSettleOutboxCapacity(t *testing.T) {
	s := baseSettlement()
	eff := validEffects(s, 500)
	outbox := make([]codec.OutboxMessage, codec.MaxOutboxPerExecution)
	for i := range outbox {
		outbox[i] = codec.OutboxMessage{SourceContract: s.ContractID, InputIndex: s.GlobalInputIndex, OutboxIndex: uint32(i), Target: codec.L2Delivery{TargetContract: testOtherHash}}
	}
	eff.Outbox = outbox

	got, err := s.settle(engine.Outcome{Kind: engine.Returned, GasUsed: 1000, ResultBlob: encodeEffects(t, eff)})
	if err != nil {
		t.Fatalf("settle: %v", err)
	}
	if got.Status != codec.StatusRuntimeFault {
		t.Fatalf("Status = %v, want StatusRuntimeFault", got.Status)
	}
	want := s.refund(codec.U256{}, 0)
	if !reflect.DeepEqual(got.OutboxMessages, want) {
		t.Fatalf("OutboxMessages = %+v, want %+v", got.OutboxMessages, want)
	}
}

func TestSettleRefundIndex(t *testing.T) {
	t.Run("SuccessWithNContractMessages", func(t *testing.T) {
		s := baseSettlement()
		eff := validEffects(s, 500) // one contract message, remainder 500
		got, err := s.settle(engine.Outcome{Kind: engine.Returned, GasUsed: 1000, ResultBlob: encodeEffects(t, eff)})
		if err != nil {
			t.Fatalf("settle: %v", err)
		}
		if got.Status != codec.StatusSuccess {
			t.Fatalf("Status = %v, want StatusSuccess", got.Status)
		}
		if len(got.OutboxMessages) != 2 {
			t.Fatalf("len(OutboxMessages) = %d, want 2", len(got.OutboxMessages))
		}
		refund := got.OutboxMessages[1]
		if refund.OutboxIndex != 1 {
			t.Fatalf("refund OutboxIndex = %d, want 1", refund.OutboxIndex)
		}
	})

	t.Run("FailureRefundAtZero", func(t *testing.T) {
		s := baseSettlement()
		eff := validEffects(s, 500)
		eff.StateDiff = []codec.LocalStateDiffEntry{{LocalKey: keyCodeHash, Value: []byte{1}}} // forces UNAUTHORIZED
		got, err := s.settle(engine.Outcome{Kind: engine.Returned, GasUsed: 1000, ResultBlob: encodeEffects(t, eff)})
		if err != nil {
			t.Fatalf("settle: %v", err)
		}
		if len(got.OutboxMessages) != 1 || got.OutboxMessages[0].OutboxIndex != 0 {
			t.Fatalf("OutboxMessages = %+v, want single refund at index 0", got.OutboxMessages)
		}
	})

	t.Run("ZeroRemainderNoRefundSuccess", func(t *testing.T) {
		s := baseSettlement()
		eff := validEffects(s, 1000) // claims the whole deposit
		got, err := s.settle(engine.Outcome{Kind: engine.Returned, GasUsed: 1000, ResultBlob: encodeEffects(t, eff)})
		if err != nil {
			t.Fatalf("settle: %v", err)
		}
		if len(got.OutboxMessages) != 1 {
			t.Fatalf("len(OutboxMessages) = %d, want 1 (contract message only, no refund)", len(got.OutboxMessages))
		}
	})

	t.Run("ZeroRemainderNoRefundFailure", func(t *testing.T) {
		s := baseSettlement()
		s.DepositAmount = codec.U256{}
		eff := validEffects(s, 0)
		eff.StateDiff = []codec.LocalStateDiffEntry{{LocalKey: keyCodeHash, Value: []byte{1}}}
		got, err := s.settle(engine.Outcome{Kind: engine.Returned, GasUsed: 1000, ResultBlob: encodeEffects(t, eff)})
		if err != nil {
			t.Fatalf("settle: %v", err)
		}
		if len(got.OutboxMessages) != 0 {
			t.Fatalf("OutboxMessages = %+v, want none", got.OutboxMessages)
		}
	})
}

func TestSettleResultTooLargeBecauseOfRefund(t *testing.T) {
	s := baseSettlement()
	eff := validEffects(s, 500) // non-zero remainder forces a refund message
	eff.Outbox = nil
	eff.Events = nil

	// Size two StateDiff entries, each within codec.MaxStateValueSize, so the contract-only
	// ExecutionResult fits under MaxExecutionResult but the ~133-byte refund pushes it over.
	eff.StateDiff = []codec.LocalStateDiffEntry{
		{LocalKey: []byte("k1"), Value: make([]byte, codec.MaxStateValueSize)},
		{LocalKey: []byte("k2"), Value: make([]byte, codec.MaxStateValueSize-215)},
	}
	eff.ReturnData = nil

	got, err := s.settle(engine.Outcome{Kind: engine.Returned, GasUsed: 1000, ResultBlob: encodeEffects(t, eff)})
	if err != nil {
		t.Fatalf("settle: %v", err)
	}
	if got.Status != codec.StatusResultTooLarge {
		t.Fatalf("Status = %v, want StatusResultTooLarge", got.Status)
	}
	want := s.refund(codec.U256{}, 0)
	if !reflect.DeepEqual(got.OutboxMessages, want) {
		t.Fatalf("OutboxMessages = %+v, want %+v (refund-only)", got.OutboxMessages, want)
	}
}

func TestSettleFailureShapeDiscardsContent(t *testing.T) {
	s := baseSettlement()
	eff := validEffects(s, 500)
	eff.StateDiff = []codec.LocalStateDiffEntry{{LocalKey: keyCodeHash, Value: []byte{1}}}

	got, err := s.settle(engine.Outcome{Kind: engine.Returned, GasUsed: 1000, ResultBlob: encodeEffects(t, eff)})
	if err != nil {
		t.Fatalf("settle: %v", err)
	}
	if got.Status != codec.StatusUnauthorized {
		t.Fatalf("Status = %v, want StatusUnauthorized", got.Status)
	}
	assertRefundOnlyShape(t, s, got)
}

func TestSettleSuccessStateDiffSortedAndDeletionsPreserved(t *testing.T) {
	s := baseSettlement()
	eff := validEffects(s, 500)
	// Blob order deliberately not ascending by derived key; derive() is a hash so the sorted
	// order is unpredictable a priori -- assert the result equals a fresh sort of what was
	// derived, and that the deletion (nil Value) entry survives as nil.
	eff.StateDiff = []codec.LocalStateDiffEntry{
		{LocalKey: []byte("zzz"), Value: []byte("present")},
		{LocalKey: []byte("aaa"), Value: nil}, // deletion
	}

	got, err := s.settle(engine.Outcome{Kind: engine.Returned, GasUsed: 1000, ResultBlob: encodeEffects(t, eff)})
	if err != nil {
		t.Fatalf("settle: %v", err)
	}
	if got.Status != codec.StatusSuccess {
		t.Fatalf("Status = %v, want StatusSuccess", got.Status)
	}
	if len(got.StateDiff) != 2 {
		t.Fatalf("len(StateDiff) = %d, want 2", len(got.StateDiff))
	}
	for i := 1; i < len(got.StateDiff); i++ {
		if string(got.StateDiff[i-1].Path[:]) > string(got.StateDiff[i].Path[:]) {
			t.Fatalf("StateDiff not sorted ascending by derived key: %v", got.StateDiff)
		}
	}
	var foundDeletion bool
	for _, e := range got.StateDiff {
		if e.Path == derive(s.ContractID, []byte("aaa")) {
			foundDeletion = true
			if e.Value != nil {
				t.Fatalf("deletion entry Value = %v, want nil", e.Value)
			}
		}
	}
	if !foundDeletion {
		t.Fatal("deletion entry not found in StateDiff")
	}
}

// domainEffects is a local alias so this file's helper signatures stay short.
type domainEffects = domain.Effects
