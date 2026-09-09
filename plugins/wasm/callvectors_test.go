package wasm

import (
	"bytes"
	"errors"
	"sort"
	"testing"

	"github.com/zenon-network/go-zenon/common/types"

	"github.com/digitalSloth/go-executor/domain"
	"github.com/digitalSloth/go-executor/internal/codec"
	"github.com/digitalSloth/go-executor/plugins/wasm/internal/module"
)

// This file holds the call-path gap-coverage vectorSpecs, the fixture bodies they need and the
// small helpers those bodies share. All fixtures go through buildWasmModule/runModuleSpec
// (fixtures_test.go); no ad-hoc WASM bytes, no new builder.

// blobOffset is where a fixture's pre-encoded ContractEffects blob is placed when it is the
// module's only data segment. It is clear of the input frame, which the runtime writes at
// offset 0 after segment initialization (D8).
const blobOffset = 1024

// returnBlobBody is the tail every returning fixture ends with: i64.const (offset<<32)|len; end.
func returnBlobBody(offset uint32, blobLen int) []byte {
	b := []byte{0x42}
	b = appendVarS64(b, (int64(offset)<<32)|int64(blobLen))
	b = append(b, 0x0B)
	return b
}

// blobModuleBody is the one-page, no-import shape with blob in a single data segment at
// blobOffset and body as run's body (which must end in 0x0B).
func blobModuleBody(blob, body []byte) []byte {
	return buildWasmModule(runModuleSpec(nil, 1, []byte{0x00}, body, []dataSeg{{offset: blobOffset, bytes: blob}}))
}

// blobModule is blobModuleBody with the ordinary body: return the blob's packed pointer.
func blobModule(blob []byte) []byte {
	return blobModuleBody(blob, returnBlobBody(blobOffset, len(blob)))
}

// bigBlobModule is blobModule sized for a blob larger than one WASM page (65536 bytes),
// computing enough initial memory pages to hold blobOffset + len(blob).
func bigBlobModule(blob []byte) []byte {
	const wasmPageSize = 65536
	pages := uint32((blobOffset + len(blob) + wasmPageSize - 1) / wasmPageSize)
	return buildWasmModule(runModuleSpec(nil, pages, []byte{0x00}, returnBlobBody(blobOffset, len(blob)), []dataSeg{{offset: blobOffset, bytes: blob}}))
}

// costOf sums the gas table's cost for each opcode in ops, the way
// internal/engine/instrument_exec_test.go's opsCost does. A plasma_limit placed exactly at a
// charge boundary is derived from the table, never hardcoded.
func costOf(t *testing.T, ops ...byte) uint64 {
	t.Helper()
	var total uint64
	for _, op := range ops {
		c, ok := module.OpcodeCost(module.Opcode(op))
		if !ok {
			t.Fatalf("costOf: no gas-table cost for opcode 0x%02X", op)
		}
		total += c
	}
	return total
}

// hostResultGuardBody proves at run time which branch of storage_read was taken, so a SUCCESS
// vector is evidence for the return value it claims to exercise.
func hostResultGuardBody(keyPtr uint32, keyLen int, outPtr, outLen uint32, want int64, resultOffset uint32, blobLen int) []byte {
	b := []byte{0x41}
	b = appendVarS64(b, int64(keyPtr))
	b = append(b, 0x41)
	b = appendVarS64(b, int64(keyLen))
	b = append(b, 0x41)
	b = appendVarS64(b, int64(outPtr))
	b = append(b, 0x41)
	b = appendVarS64(b, int64(outLen))
	b = append(b, 0x10, 0x00) // call 0 (env.storage_read)
	b = append(b, 0x42)
	b = appendVarS64(b, want)
	b = append(b, 0x52)       // i64.ne
	b = append(b, 0x04, 0x40) // if (void)
	b = append(b, 0x00)       // unreachable
	b = append(b, 0x0B)       // end
	b = append(b, returnBlobBody(resultOffset, blobLen)...)
	return b
}

// growGuardBody proves at run time that memory.grow past the limit returned -1 and grew
// nothing, so a SUCCESS vector is evidence the refusal happened.
func growGuardBody(blobLen int) []byte {
	b := []byte{0x41}
	b = appendVarS64(b, 64)   // i32.const 64
	b = append(b, 0x40, 0x00) // memory.grow
	b = append(b, 0x41)
	b = appendVarS64(b, -1)   // i32.const -1
	b = append(b, 0x47)       // i32.ne
	b = append(b, 0x04, 0x40) // if (void)
	b = append(b, 0x00)       // unreachable
	b = append(b, 0x0B)       // end
	b = append(b, returnBlobBody(blobOffset, blobLen)...)
	return b
}

// sortLocalStateDiffDescByDerivedKey returns entries sorted in strictly descending order by
// derive(cid, LocalKey) -- the reverse of the runtime's own canonical ordering. EXEC-006 emits
// its puts this way so the ascending order the ExecutionResult commits can only come from
// sortStateDiff, never from blob order.
func sortLocalStateDiffDescByDerivedKey(cid types.Hash, entries []codec.LocalStateDiffEntry) []codec.LocalStateDiffEntry {
	out := append([]codec.LocalStateDiffEntry(nil), entries...)
	sort.Slice(out, func(i, j int) bool {
		pi := derive(cid, out[i].LocalKey)
		pj := derive(cid, out[j].LocalKey)
		return bytes.Compare(pi[:], pj[:]) > 0
	})
	return out
}

// withdrawalPayloadBlob returns a ContractEffects encoding whose single L1Withdrawal carries a
// two-byte payload -- a blob the codec will not produce, since Encode rejects it, but which a
// contract can return. The splice offset holds only while eff's L1Withdrawal is the LAST message
// in Outbox and ReturnData is empty: the encoding's last 40 bytes are then that message's
// payload length (4), the return-data length (4) and claimed_deposit (32), so the payload length
// is the u32 at len(blob)-40. internal/codec/effects_test.go:212 splices the same field the same
// way for the decode-side rejection.
func withdrawalPayloadBlob(t *testing.T, eff codec.ContractEffects) []byte {
	t.Helper()
	blob, err := eff.Encode()
	if err != nil {
		t.Fatalf("withdrawalPayloadBlob: Encode: %v", err)
	}
	n := len(blob)
	out := make([]byte, 0, n+2)
	out = append(out, blob[:n-40]...)
	out = append(out, 0x00, 0x00, 0x00, 0x02)
	out = append(out, 0xAA, 0xBB)
	out = append(out, blob[n-36:]...)
	if _, err := codec.DecodeContractEffects(out); !errors.Is(err, codec.ErrNonEmptyWithdrawalPayload) {
		t.Fatalf("withdrawalPayloadBlob: DecodeContractEffects err = %v, want errors.Is(..., ErrNonEmptyWithdrawalPayload)", err)
	}
	return out
}

// segBlob returns the minimal blob CV-GROW-1 and the four CV-SEG-* vectors share: ReturnData
// only, two bytes, no state diff, no messages, so their gas_used values differ only by the
// opcode cost of the path charged.
func segBlob(t *testing.T, idx uint64) []byte {
	return encodeEffects(t, codec.ContractEffects{
		Version:          codec.ExecutionResultVersion,
		GlobalInputIndex: idx,
		ReturnData:       []byte{0xCC, 0xDD},
	})
}

// testAsset is a valid non-zero AssetID (bytes [0:22] zero), the same construction spineVectors
// uses for its own deposit fixtures.
func testAsset() codec.AssetID {
	var a codec.AssetID
	copy(a[32-types.ZenonTokenStandardSize:], []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a})
	return a
}

// stdCtx is the domain.InputContext every call-path vector in this file shares beyond its own
// GlobalInputIndex: the spine's momentum_height/momentum_time and l1_sender.
func stdCtx(idx uint64) domain.InputContext {
	return domain.InputContext{
		GlobalInputIndex: idx,
		MomentumHeight:   1000,
		MomentumTime:     1700000000,
		L1Sender:         repeatedAddr(0x01),
	}
}

// callPlasmaLimit is the plasma_limit every call-path vector uses unless its whole point is a
// boundary plasma_limit (the four CV-OOG-* vectors).
const callPlasmaLimit = 10_000_000

// cid1/cid2 are the two ContractIDs this ticket's vectors use: cid1 for the regenerated
// EXEC-00x group (batch_id 1), cid2 for every CV-*/EXEC-DEP-* vector (batch_id 2).
var (
	cid1 = repeatedHash(0x11)
	cid2 = repeatedHash(0x33)
)

// callVectors returns every gap-coverage call-shape vectorSpec this ticket adds, in the file's
// table order. unsupportedVector (shape unsupported_kind) is generated separately.
func callVectors(t *testing.T) []vectorSpec {
	t.Helper()

	// newIdentity's allocation is append-only: GlobalInputIndex and SourceAccountBlockHash are
	// handed out in order starting at 100/0xB1, one pair per call, and a spec already using a
	// value from a prior call must never be renumbered. A vector added later must call
	// newIdentity for the next unused pair rather than picking its own; unsupportedVector below
	// already claims 0xD4, the next value this sequence would hand out. assertUniqueIdentities in
	// vectors_test.go enforces this.
	n := 0
	newIdentity := func() (idx uint64, src types.Hash) {
		idx = uint64(100 + n)
		src = repeatedHash(byte(0xB1 + n))
		n++
		return
	}

	var specs []vectorSpec

	// --- regenerated EXEC-003..EXEC-012.

	{
		idx, src := newIdentity()
		blob := encodeEffects(t, codec.ContractEffects{
			Version:          codec.ExecutionResultVersion,
			GlobalInputIndex: idx,
			StateDiff:        []codec.LocalStateDiffEntry{{LocalKey: []byte("counter"), Value: []byte{0x00, 0x00, 0x00, 0x2b}}},
		})
		specs = append(specs, vectorSpec{
			ID:                     "EXEC-003",
			Purpose:                "update an existing key",
			Shape:                  "call",
			BatchID:                1,
			Ctx:                    stdCtx(idx),
			SourceAccountBlockHash: src,
			Args:                   codec.CallL2Args{TargetContract: cid1, PlasmaLimit: callPlasmaLimit},
			ContractID:             cid1,
			Module:                 blobModule(blob),
			ExtraPreState:          map[domain.Path][]byte{derive(cid1, []byte("counter")): {0x00, 0x00, 0x00, 0x2a}},
			WantStatus:             codec.StatusSuccess,
		})
	}

	{
		idx, src := newIdentity()
		blob := encodeEffects(t, codec.ContractEffects{
			Version:          codec.ExecutionResultVersion,
			GlobalInputIndex: idx,
			StateDiff:        []codec.LocalStateDiffEntry{{LocalKey: []byte("counter"), Value: nil}},
		})
		specs = append(specs, vectorSpec{
			ID:                     "EXEC-004",
			Purpose:                "delete an existing key via the EMPTY sentinel",
			Shape:                  "call",
			BatchID:                1,
			Ctx:                    stdCtx(idx),
			SourceAccountBlockHash: src,
			Args:                   codec.CallL2Args{TargetContract: cid1, PlasmaLimit: callPlasmaLimit},
			ContractID:             cid1,
			Module:                 blobModule(blob),
			ExtraPreState: map[domain.Path][]byte{
				derive(cid1, []byte("counter")): {0x00, 0x00, 0x00, 0x2a},
				derive(cid1, []byte("other")):   {0xff},
			},
			WantStatus: codec.StatusSuccess,
		})
	}

	{
		idx, src := newIdentity()
		blob := encodeEffects(t, codec.ContractEffects{
			Version:          codec.ExecutionResultVersion,
			GlobalInputIndex: idx,
			StateDiff:        []codec.LocalStateDiffEntry{{LocalKey: []byte("flag"), Value: []byte{}}},
		})
		specs = append(specs, vectorSpec{
			ID:                     "EXEC-005",
			Purpose:                "write a present-empty value",
			Shape:                  "call",
			BatchID:                1,
			Ctx:                    stdCtx(idx),
			SourceAccountBlockHash: src,
			Args:                   codec.CallL2Args{TargetContract: cid1, PlasmaLimit: callPlasmaLimit},
			ContractID:             cid1,
			Module:                 blobModule(blob),
			WantStatus:             codec.StatusSuccess,
		})
	}

	{
		idx, src := newIdentity()
		raw := []codec.LocalStateDiffEntry{
			{LocalKey: []byte("a"), Value: []byte{0x01}},
			{LocalKey: []byte("b"), Value: []byte{0x02}},
			{LocalKey: []byte("c"), Value: []byte{0x03}},
		}
		blob := encodeEffects(t, codec.ContractEffects{
			Version:          codec.ExecutionResultVersion,
			GlobalInputIndex: idx,
			StateDiff:        sortLocalStateDiffDescByDerivedKey(cid1, raw),
		})
		specs = append(specs, vectorSpec{
			ID:                     "EXEC-006",
			Purpose:                "multi-entry StateDiff, sorted by the runtime",
			Shape:                  "call",
			BatchID:                1,
			Ctx:                    stdCtx(idx),
			SourceAccountBlockHash: src,
			Args:                   codec.CallL2Args{TargetContract: cid1, PlasmaLimit: callPlasmaLimit},
			ContractID:             cid1,
			Module:                 blobModule(blob),
			WantStatus:             codec.StatusSuccess,
		})
	}

	{
		idx, src := newIdentity()
		blob := encodeEffects(t, codec.ContractEffects{
			Version:          codec.ExecutionResultVersion,
			GlobalInputIndex: idx,
			Events: []codec.Event{
				{ContractID: cid1, InputIndex: idx, EventIndex: 0, Topics: []types.Hash{repeatedHash(0xAA)}, Data: []byte("hello")},
			},
		})
		specs = append(specs, vectorSpec{
			ID:                     "EXEC-007",
			Purpose:                "one event, no state change",
			Shape:                  "call",
			BatchID:                1,
			Ctx:                    stdCtx(idx),
			SourceAccountBlockHash: src,
			Args:                   codec.CallL2Args{TargetContract: cid1, PlasmaLimit: callPlasmaLimit},
			ContractID:             cid1,
			Module:                 blobModule(blob),
			WantStatus:             codec.StatusSuccess,
		})
	}

	{
		idx, src := newIdentity()
		blob := encodeEffects(t, codec.ContractEffects{
			Version:          codec.ExecutionResultVersion,
			GlobalInputIndex: idx,
			Outbox: []codec.OutboxMessage{
				{SourceContract: cid1, InputIndex: idx, OutboxIndex: 0, Target: codec.L1Withdrawal{RecipientL1: repeatedAddr(0xBB), Asset: testAsset(), Amount: u256(1000)}, Payload: nil},
			},
		})
		specs = append(specs, vectorSpec{
			ID:                     "EXEC-008",
			Purpose:                "one contract-emitted L1 withdrawal",
			Shape:                  "call",
			BatchID:                1,
			Ctx:                    stdCtx(idx),
			SourceAccountBlockHash: src,
			Args:                   codec.CallL2Args{TargetContract: cid1, PlasmaLimit: callPlasmaLimit},
			ContractID:             cid1,
			Module:                 blobModule(blob),
			WantStatus:             codec.StatusSuccess,
		})
	}

	{
		idx, src := newIdentity()
		blob := encodeEffects(t, codec.ContractEffects{
			Version:          codec.ExecutionResultVersion,
			GlobalInputIndex: idx,
			StateDiff:        []codec.LocalStateDiffEntry{{LocalKey: []byte("k"), Value: []byte{0x09}}},
			Events: []codec.Event{
				{ContractID: cid1, InputIndex: idx, EventIndex: 0, Data: []byte("evt")},
			},
			Outbox: []codec.OutboxMessage{
				{SourceContract: cid1, InputIndex: idx, OutboxIndex: 0, Target: codec.L2Delivery{TargetContract: repeatedHash(0x44)}, Payload: []byte("ping")},
			},
			ReturnData: []byte{0x01},
		})
		specs = append(specs, vectorSpec{
			ID:                     "EXEC-009",
			Purpose:                "combined put + event + L2 delivery + return data",
			Shape:                  "call",
			BatchID:                1,
			Ctx:                    stdCtx(idx),
			SourceAccountBlockHash: src,
			Args:                   codec.CallL2Args{TargetContract: cid1, PlasmaLimit: callPlasmaLimit},
			ContractID:             cid1,
			Module:                 blobModule(blob),
			WantStatus:             codec.StatusSuccess,
		})
	}

	{
		idx, src := newIdentity()
		body := []byte{0x41, 0x07, 0x10, 0x00, 0x42, 0x00, 0x0B} // i32.const 7; call 0; i64.const 0; end
		mod := buildWasmModule(runModuleSpec([]string{"abort"}, 1, []byte{0x00}, body, nil))
		specs = append(specs, vectorSpec{
			ID:                     "EXEC-010",
			Purpose:                "abort(7)",
			Shape:                  "call",
			BatchID:                1,
			Ctx:                    stdCtx(idx),
			SourceAccountBlockHash: src,
			Args:                   codec.CallL2Args{TargetContract: cid1, PlasmaLimit: callPlasmaLimit},
			ContractID:             cid1,
			Module:                 mod,
			WantStatus:             codec.StatusRevert,
		})
	}

	{
		idx, src := newIdentity()
		body := []byte{0x00, 0x0B} // unreachable; end
		mod := buildWasmModule(runModuleSpec(nil, 1, []byte{0x00}, body, nil))
		specs = append(specs, vectorSpec{
			ID:                     "EXEC-011",
			Purpose:                "unreachable",
			Shape:                  "call",
			BatchID:                1,
			Ctx:                    stdCtx(idx),
			SourceAccountBlockHash: src,
			Args:                   codec.CallL2Args{TargetContract: cid1, PlasmaLimit: callPlasmaLimit},
			ContractID:             cid1,
			Module:                 mod,
			WantStatus:             codec.StatusRuntimeFault,
		})
	}

	{
		idx, src := newIdentity()
		blob := encodeEffects(t, codec.ContractEffects{
			Version:          codec.ExecutionResultVersion,
			GlobalInputIndex: idx,
			StateDiff:        []codec.LocalStateDiffEntry{{LocalKey: []byte("new"), Value: []byte{0x01}}},
		})
		key := []byte("missing")
		body := hostResultGuardBody(1024, len(key), 2048, 32, -1, 1032, len(blob))
		mod := buildWasmModule(runModuleSpec([]string{"storage_read"}, 1, []byte{0x00}, body,
			[]dataSeg{{offset: 1024, bytes: key}, {offset: 1032, bytes: blob}}))
		specs = append(specs, vectorSpec{
			ID:                     "EXEC-012",
			Purpose:                "absent-key storage_read, then write a different key",
			Shape:                  "call",
			BatchID:                1,
			Ctx:                    stdCtx(idx),
			SourceAccountBlockHash: src,
			Args:                   codec.CallL2Args{TargetContract: cid1, PlasmaLimit: callPlasmaLimit},
			ContractID:             cid1,
			Module:                 mod,
			WantStatus:             codec.StatusSuccess,
		})
	}

	// --- deposit/claim/refund.

	{
		idx, src := newIdentity()
		blob := encodeEffects(t, codec.ContractEffects{
			Version:          codec.ExecutionResultVersion,
			GlobalInputIndex: idx,
			StateDiff:        []codec.LocalStateDiffEntry{{LocalKey: []byte("d1"), Value: []byte{0x01}}},
			ClaimedDeposit:   u256(1000),
		})
		specs = append(specs, vectorSpec{
			ID:                     "EXEC-DEP-1",
			Purpose:                "full claim, no refund",
			Shape:                  "call",
			BatchID:                2,
			Ctx:                    stdCtx(idx),
			SourceAccountBlockHash: src,
			Args:                   codec.DepositArgs{TargetContract: cid2, PlasmaLimit: callPlasmaLimit, DepositAsset: testAsset(), DepositAmount: u256(1000)},
			ContractID:             cid2,
			Module:                 blobModule(blob),
			WantStatus:             codec.StatusSuccess,
		})
	}

	{
		idx, src := newIdentity()
		blob := encodeEffects(t, codec.ContractEffects{
			Version:          codec.ExecutionResultVersion,
			GlobalInputIndex: idx,
			Outbox: []codec.OutboxMessage{
				{SourceContract: cid2, InputIndex: idx, OutboxIndex: 0, Target: codec.L2Delivery{TargetContract: repeatedHash(0x55)}, Payload: nil},
			},
			ClaimedDeposit: u256(400),
		})
		specs = append(specs, vectorSpec{
			ID:                     "EXEC-DEP-2",
			Purpose:                "partial claim, refund after the contract's messages",
			Shape:                  "call",
			BatchID:                2,
			Ctx:                    stdCtx(idx),
			SourceAccountBlockHash: src,
			Args:                   codec.DepositArgs{TargetContract: cid2, PlasmaLimit: callPlasmaLimit, DepositAsset: testAsset(), DepositAmount: u256(1000)},
			ContractID:             cid2,
			Module:                 blobModule(blob),
			WantStatus:             codec.StatusSuccess,
		})
	}

	{
		idx, src := newIdentity()
		blob := encodeEffects(t, codec.ContractEffects{
			Version:          codec.ExecutionResultVersion,
			GlobalInputIndex: idx,
			ClaimedDeposit:   u256(1001),
		})
		specs = append(specs, vectorSpec{
			ID:                     "EXEC-DEP-3",
			Purpose:                "claim above deposit_amount",
			Shape:                  "call",
			BatchID:                2,
			Ctx:                    stdCtx(idx),
			SourceAccountBlockHash: src,
			Args:                   codec.DepositArgs{TargetContract: cid2, PlasmaLimit: callPlasmaLimit, DepositAsset: testAsset(), DepositAmount: u256(1000)},
			ContractID:             cid2,
			Module:                 blobModule(blob),
			WantStatus:             codec.StatusRuntimeFault,
		})
	}

	{
		idx, src := newIdentity()
		blob := encodeEffects(t, codec.ContractEffects{
			Version:          codec.ExecutionResultVersion,
			GlobalInputIndex: idx,
			ClaimedDeposit:   u256(1),
		})
		specs = append(specs, vectorSpec{
			ID:                     "EXEC-DEP-4",
			Purpose:                "non-zero claim against a zero deposit",
			Shape:                  "call",
			BatchID:                2,
			Ctx:                    stdCtx(idx),
			SourceAccountBlockHash: src,
			Args:                   codec.CallL2Args{TargetContract: cid2, PlasmaLimit: callPlasmaLimit},
			ContractID:             cid2,
			Module:                 blobModule(blob),
			WantStatus:             codec.StatusRuntimeFault,
		})
	}

	// --- blob-content rejections.

	{
		idx, src := newIdentity()
		eff := codec.ContractEffects{
			Version:          codec.ExecutionResultVersion,
			GlobalInputIndex: idx,
			Outbox: []codec.OutboxMessage{
				{SourceContract: cid2, InputIndex: idx, OutboxIndex: 0, Target: codec.L1Withdrawal{RecipientL1: repeatedAddr(0xCC), Asset: testAsset(), Amount: u256(1)}, Payload: nil},
			},
		}
		blob := withdrawalPayloadBlob(t, eff)
		specs = append(specs, vectorSpec{
			ID:                     "CV-OUTBOX-1",
			Purpose:                "a withdrawal carrying a payload is rejected at the runtime layer",
			Shape:                  "call",
			BatchID:                2,
			Ctx:                    stdCtx(idx),
			SourceAccountBlockHash: src,
			Args:                   codec.CallL2Args{TargetContract: cid2, PlasmaLimit: callPlasmaLimit},
			ContractID:             cid2,
			Module:                 blobModule(blob),
			WantStatus:             codec.StatusRuntimeFault,
		})
	}

	{
		idx, src := newIdentity()
		er := codec.ExecutionResult{
			Version:          codec.ExecutionResultVersion,
			GlobalInputIndex: idx,
			PreStateRoot:     repeatedHash(0xEE),
			PostStateRoot:    repeatedHash(0xEE),
			ClaimedDeposit:   codec.U256{},
		}
		blob, err := er.Encode()
		if err != nil {
			t.Fatalf("CV-ROOTS-1: ExecutionResult.Encode: %v", err)
		}
		specs = append(specs, vectorSpec{
			ID:                     "CV-ROOTS-1",
			Purpose:                "an ExecutionResult returned where a ContractEffects is expected does not decode as a ContractEffects and the input faults",
			Shape:                  "call",
			BatchID:                2,
			Ctx:                    stdCtx(idx),
			SourceAccountBlockHash: src,
			Args:                   codec.CallL2Args{TargetContract: cid2, PlasmaLimit: callPlasmaLimit},
			ContractID:             cid2,
			Module:                 blobModule(blob),
			WantStatus:             codec.StatusRuntimeFault,
		})
	}

	{
		idx, src := newIdentity()
		blob := encodeEffects(t, codec.ContractEffects{
			Version:          codec.ExecutionResultVersion,
			GlobalInputIndex: idx,
			StateDiff: []codec.LocalStateDiffEntry{
				{LocalKey: []byte("dup"), Value: []byte("a")},
				{LocalKey: []byte("dup"), Value: []byte("b")},
			},
		})
		specs = append(specs, vectorSpec{
			ID:                     "CV-DUPKEY-1",
			Purpose:                "two StateDiff entries deriving to one key",
			Shape:                  "call",
			BatchID:                2,
			Ctx:                    stdCtx(idx),
			SourceAccountBlockHash: src,
			Args:                   codec.CallL2Args{TargetContract: cid2, PlasmaLimit: callPlasmaLimit},
			ContractID:             cid2,
			Module:                 blobModule(blob),
			WantStatus:             codec.StatusRuntimeFault,
		})
	}

	{
		idx, src := newIdentity()
		blob := encodeEffects(t, codec.ContractEffects{
			Version:          codec.ExecutionResultVersion,
			GlobalInputIndex: idx,
			StateDiff: []codec.LocalStateDiffEntry{
				{LocalKey: append([]byte(nil), keyCodeHash...), Value: []byte{0x01}},
				{LocalKey: []byte("ok"), Value: []byte{0x02}},
			},
			ReturnData: []byte{0x03},
		})
		specs = append(specs, vectorSpec{
			ID:                     "CV-UNAUTH-1",
			Purpose:                "a write to a reserved local key",
			Shape:                  "call",
			BatchID:                2,
			Ctx:                    stdCtx(idx),
			SourceAccountBlockHash: src,
			Args:                   codec.CallL2Args{TargetContract: cid2, PlasmaLimit: callPlasmaLimit},
			ContractID:             cid2,
			Module:                 blobModule(blob),
			WantStatus:             codec.StatusUnauthorized,
		})
	}

	// --- size rejections.

	{
		idx, src := newIdentity()
		blob := encodeEffects(t, codec.ContractEffects{
			Version:          codec.ExecutionResultVersion,
			GlobalInputIndex: idx,
			ReturnData:       bytes.Repeat([]byte{0xAB}, MaxReturnData+1),
		})
		specs = append(specs, vectorSpec{
			ID:                     "CV-RTL-RETURN-1",
			Purpose:                "len(ReturnData) > MaxReturnData alone",
			Shape:                  "call",
			BatchID:                2,
			Ctx:                    stdCtx(idx),
			SourceAccountBlockHash: src,
			Args:                   codec.CallL2Args{TargetContract: cid2, PlasmaLimit: callPlasmaLimit},
			ContractID:             cid2,
			Module:                 blobModule(blob),
			WantStatus:             codec.StatusResultTooLarge,
		})
	}

	{
		idx, src := newIdentity()
		// A contract-only ExecutionResult with N state entries, no events, no outbox, empty
		// ReturnData encodes to 122 + Σ(36+len(v)) bytes; k2's length is derived so k1+k2 land
		// exactly at MaxExecutionResult, the largest contract-only result that still passes step
		// 8, and the appended refund (133 bytes) pushes it over.
		k2Len := MaxExecutionResult - 122 - 36 - codec.MaxStateValueSize - 36
		blob := encodeEffects(t, codec.ContractEffects{
			Version:          codec.ExecutionResultVersion,
			GlobalInputIndex: idx,
			StateDiff: []codec.LocalStateDiffEntry{
				{LocalKey: []byte("k1"), Value: make([]byte, codec.MaxStateValueSize)},
				{LocalKey: []byte("k2"), Value: make([]byte, k2Len)},
			},
		})
		specs = append(specs, vectorSpec{
			ID:                     "CV-RTL-RESULT-1",
			Purpose:                "the refund pushes the ExecutionResult past MaxExecutionResult",
			Shape:                  "call",
			BatchID:                2,
			Ctx:                    stdCtx(idx),
			SourceAccountBlockHash: src,
			Args:                   codec.DepositArgs{TargetContract: cid2, PlasmaLimit: callPlasmaLimit, DepositAsset: testAsset(), DepositAmount: u256(1000)},
			ContractID:             cid2,
			Module:                 blobModule(blob),
			WantStatus:             codec.StatusResultTooLarge,
		})
	}

	// --- OUT_OF_GAS, all four forms.

	{
		idx, src := newIdentity()
		blob := encodeEffects(t, codec.ContractEffects{Version: codec.ExecutionResultVersion, GlobalInputIndex: idx})
		specs = append(specs, vectorSpec{
			ID:                     "CV-OOG-1",
			Purpose:                "pre-execution initial-memory charge; nothing runs",
			Shape:                  "call",
			BatchID:                2,
			Ctx:                    stdCtx(idx),
			SourceAccountBlockHash: src,
			Args:                   codec.CallL2Args{TargetContract: cid2, PlasmaLimit: module.InitialMemoryCost(1) - 1},
			ContractID:             cid2,
			Module:                 blobModule(blob),
			WantStatus:             codec.StatusOutOfGas,
		})
	}

	{
		idx, src := newIdentity()
		blob := encodeEffects(t, codec.ContractEffects{Version: codec.ExecutionResultVersion, GlobalInputIndex: idx})
		specs = append(specs, vectorSpec{
			ID:                     "CV-OOG-2",
			Purpose:                "an injected segment charge",
			Shape:                  "call",
			BatchID:                2,
			Ctx:                    stdCtx(idx),
			SourceAccountBlockHash: src,
			Args:                   codec.CallL2Args{TargetContract: cid2, PlasmaLimit: module.InitialMemoryCost(1)},
			ContractID:             cid2,
			Module:                 blobModule(blob),
			WantStatus:             codec.StatusOutOfGas,
		})
	}

	{
		idx, src := newIdentity()
		blob := encodeEffects(t, codec.ContractEffects{Version: codec.ExecutionResultVersion, GlobalInputIndex: idx})
		key := []byte("abcde")
		body := []byte{0x41}
		body = appendVarS64(body, 1024)
		body = append(body, 0x41)
		body = appendVarS64(body, 5)
		body = append(body, 0x10, 0x00) // call 0 (env.storage_len)
		body = append(body, 0x1A)       // drop
		body = append(body, returnBlobBody(1032, len(blob))...)
		mod := buildWasmModule(runModuleSpec([]string{"storage_len"}, 1, []byte{0x00}, body,
			[]dataSeg{{offset: 1024, bytes: key}, {offset: 1032, bytes: blob}}))
		specs = append(specs, vectorSpec{
			ID:                     "CV-OOG-3",
			Purpose:                "a host-function charge",
			Shape:                  "call",
			BatchID:                2,
			Ctx:                    stdCtx(idx),
			SourceAccountBlockHash: src,
			Args:                   codec.CallL2Args{TargetContract: cid2, PlasmaLimit: module.InitialMemoryCost(1) + costOf(t, 0x41, 0x41, 0x10, 0x1A, 0x42, 0x0B)},
			ContractID:             cid2,
			Module:                 mod,
			WantStatus:             codec.StatusOutOfGas,
		})
	}

	{
		idx, src := newIdentity()
		blob := encodeEffects(t, codec.ContractEffects{
			Version:          codec.ExecutionResultVersion,
			GlobalInputIndex: idx,
			StateDiff:        []codec.LocalStateDiffEntry{{LocalKey: []byte("x"), Value: []byte{0x01}}},
			ReturnData:       []byte{0x02},
		})
		specs = append(specs, vectorSpec{
			ID:                     "CV-OOG-4",
			Purpose:                "the §10.4 effect charge, step 6",
			Shape:                  "call",
			BatchID:                2,
			Ctx:                    stdCtx(idx),
			SourceAccountBlockHash: src,
			Args:                   codec.CallL2Args{TargetContract: cid2, PlasmaLimit: module.InitialMemoryCost(1) + costOf(t, 0x42, 0x0B)},
			ContractID:             cid2,
			Module:                 blobModule(blob),
			WantStatus:             codec.StatusOutOfGas,
		})
	}

	// --- host and engine behaviour.

	{
		idx, src := newIdentity()
		blob := encodeEffects(t, codec.ContractEffects{Version: codec.ExecutionResultVersion, GlobalInputIndex: idx})
		value8 := []byte{1, 2, 3, 4, 5, 6, 7, 8}
		key := []byte("value8")
		body := hostResultGuardBody(1024, len(key), 2048, 4, -2, 1032, len(blob))
		mod := buildWasmModule(runModuleSpec([]string{"storage_read"}, 1, []byte{0x00}, body,
			[]dataSeg{{offset: 1024, bytes: key}, {offset: 1032, bytes: blob}}))
		specs = append(specs, vectorSpec{
			ID:                     "CV-READ-2",
			Purpose:                "storage_read returns -2 when the out buffer is too small",
			Shape:                  "call",
			BatchID:                2,
			Ctx:                    stdCtx(idx),
			SourceAccountBlockHash: src,
			Args:                   codec.CallL2Args{TargetContract: cid2, PlasmaLimit: callPlasmaLimit},
			ContractID:             cid2,
			Module:                 mod,
			ExtraPreState:          map[domain.Path][]byte{derive(cid2, []byte("value8")): value8},
			WantStatus:             codec.StatusSuccess,
		})
	}

	{
		idx, src := newIdentity()
		body := appendVarS64([]byte{0x42}, (int64(65530)<<32)|int64(100))
		body = append(body, 0x0B)
		mod := buildWasmModule(runModuleSpec(nil, 1, []byte{0x00}, body, nil))
		specs = append(specs, vectorSpec{
			ID:                     "CV-OOB-1",
			Purpose:                "result_ptr + result_len outside memory",
			Shape:                  "call",
			BatchID:                2,
			Ctx:                    stdCtx(idx),
			SourceAccountBlockHash: src,
			Args:                   codec.CallL2Args{TargetContract: cid2, PlasmaLimit: callPlasmaLimit},
			ContractID:             cid2,
			Module:                 mod,
			WantStatus:             codec.StatusRuntimeFault,
		})
	}

	{
		idx, src := newIdentity()
		blob := segBlob(t, idx)
		mod := blobModuleBody(blob, growGuardBody(len(blob)))
		specs = append(specs, vectorSpec{
			ID:                     "CV-GROW-1",
			Purpose:                "memory.grow past MaxMemory returns -1, grows nothing, pays no per-page surcharge",
			Shape:                  "call",
			BatchID:                2,
			Ctx:                    stdCtx(idx),
			SourceAccountBlockHash: src,
			Args:                   codec.CallL2Args{TargetContract: cid2, PlasmaLimit: callPlasmaLimit},
			ContractID:             cid2,
			Module:                 mod,
			WantStatus:             codec.StatusSuccess,
		})
	}

	{
		idx, src := newIdentity()
		blob := encodeEffects(t, codec.ContractEffects{Version: codec.ExecutionResultVersion, GlobalInputIndex: idx})
		runBody := []byte{0x41}
		runBody = appendVarS64(runBody, 511)
		runBody = append(runBody, 0x10, 0x01) // call 1 (f)
		runBody = append(runBody, returnBlobBody(blobOffset, len(blob))...)
		fBody := []byte{
			0x20, 0x00, // local.get 0
			0x04, 0x40, // if (void)
			0x20, 0x00, // local.get 0
			0x41, 0x01, // i32.const 1
			0x6B,       // i32.sub
			0x10, 0x01, // call 1 (f)
			0x0B, // end (if)
			0x0B, // end (function)
		}
		mod := buildWasmModule(wasmModuleSpec{
			Types: []wasmFuncType{
				{Params: []byte{0x7F, 0x7F}, Results: []byte{0x7E}}, // run: (i32,i32) -> i64
				{Params: []byte{0x7F}, Results: nil},                // f: (i32) -> ()
			},
			Funcs: []wasmFunc{
				{TypeIdx: 0, Locals: []byte{0x00}, Body: runBody},
				{TypeIdx: 1, Locals: []byte{0x00}, Body: fBody},
			},
			MemoryPages: 1,
			Exports: []wasmExport{
				{Name: "memory", Kind: 0x02, Index: 0},
				{Name: "run", Kind: 0x00, Index: 0},
			},
			Data: []dataSeg{{offset: blobOffset, bytes: blob}},
		})
		specs = append(specs, vectorSpec{
			ID:                     "CV-DEPTH-1",
			Purpose:                "the 513th frame traps",
			Shape:                  "call",
			BatchID:                2,
			Ctx:                    stdCtx(idx),
			SourceAccountBlockHash: src,
			Args:                   codec.CallL2Args{TargetContract: cid2, PlasmaLimit: callPlasmaLimit},
			ContractID:             cid2,
			Module:                 mod,
			WantStatus:             codec.StatusRuntimeFault,
		})
	}

	// --- segmentation.

	{
		idx, src := newIdentity()
		blob := segBlob(t, idx)
		body := []byte{
			0x02, 0x40, // block (void)
			0x41, 0x01, // i32.const 1
			0x0D, 0x00, // br_if 0
			0x01, // nop
			0x0B, // end (block)
		}
		body = append(body, returnBlobBody(blobOffset, len(blob))...)
		specs = append(specs, vectorSpec{
			ID:                     "CV-SEG-BRIF-1",
			Purpose:                "a taken br_if skips the rest of its block",
			Shape:                  "call",
			BatchID:                2,
			Ctx:                    stdCtx(idx),
			SourceAccountBlockHash: src,
			Args:                   codec.CallL2Args{TargetContract: cid2, PlasmaLimit: callPlasmaLimit},
			ContractID:             cid2,
			Module:                 blobModuleBody(blob, body),
			WantStatus:             codec.StatusSuccess,
		})
	}

	{
		idx, src := newIdentity()
		blob := segBlob(t, idx)
		body := []byte{
			0x41, 0x00, // i32.const 0
			0x04, 0x40, // if (void)
			0x01, 0x01, // nop; nop
			0x0B, // end (if)
		}
		body = append(body, returnBlobBody(blobOffset, len(blob))...)
		specs = append(specs, vectorSpec{
			ID:                     "CV-SEG-IF-1",
			Purpose:                "a false if skips its arm",
			Shape:                  "call",
			BatchID:                2,
			Ctx:                    stdCtx(idx),
			SourceAccountBlockHash: src,
			Args:                   codec.CallL2Args{TargetContract: cid2, PlasmaLimit: callPlasmaLimit},
			ContractID:             cid2,
			Module:                 blobModuleBody(blob, body),
			WantStatus:             codec.StatusSuccess,
		})
	}

	{
		idx, src := newIdentity()
		blob := segBlob(t, idx)
		body := []byte{
			0x02, 0x40, // block (outer, void)
			0x02, 0x40, // block (inner, void)
			0x0C, 0x01, // br 1
			0x0B, // end (inner)
			0x01, // nop
			0x0B, // end (outer)
		}
		body = append(body, returnBlobBody(blobOffset, len(blob))...)
		specs = append(specs, vectorSpec{
			ID:                     "CV-SEG-AFTEREND-1",
			Purpose:                "a branch landing after an end",
			Shape:                  "call",
			BatchID:                2,
			Ctx:                    stdCtx(idx),
			SourceAccountBlockHash: src,
			Args:                   codec.CallL2Args{TargetContract: cid2, PlasmaLimit: callPlasmaLimit},
			ContractID:             cid2,
			Module:                 blobModuleBody(blob, body),
			WantStatus:             codec.StatusSuccess,
		})
	}

	{
		idx, src := newIdentity()
		blob := segBlob(t, idx)
		body := appendVarS64([]byte{0x42}, (int64(blobOffset)<<32)|int64(len(blob)))
		body = append(body, 0x0C, 0x00, 0x0B) // br 0; end
		specs = append(specs, vectorSpec{
			ID:                     "CV-SEG-OUTER-1",
			Purpose:                "a branch to the function's outermost label",
			Shape:                  "call",
			BatchID:                2,
			Ctx:                    stdCtx(idx),
			SourceAccountBlockHash: src,
			Args:                   codec.CallL2Args{TargetContract: cid2, PlasmaLimit: callPlasmaLimit},
			ContractID:             cid2,
			Module:                 blobModuleBody(blob, body),
			WantStatus:             codec.StatusSuccess,
		})
	}

	// --- deposit refund exempt from the outbox cap; blob-length rejections ahead of decode.

	{
		idx, src := newIdentity()
		outbox := make([]codec.OutboxMessage, codec.MaxOutboxPerExecution)
		for i := range outbox {
			outbox[i] = codec.OutboxMessage{SourceContract: cid2, InputIndex: idx, OutboxIndex: uint32(i), Target: codec.L2Delivery{TargetContract: repeatedHash(0x55)}, Payload: nil}
		}
		blob := encodeEffects(t, codec.ContractEffects{
			Version:          codec.ExecutionResultVersion,
			GlobalInputIndex: idx,
			Outbox:           outbox,
			ClaimedDeposit:   u256(400),
		})
		specs = append(specs, vectorSpec{
			ID:                     "EXEC-DEP-5",
			Purpose:                "a contract at MaxOutboxPerExecution with an unclaimed remainder: SUCCESS, outbox_count = 33, the refund at outbox_index = 32",
			Shape:                  "call",
			BatchID:                2,
			Ctx:                    stdCtx(idx),
			SourceAccountBlockHash: src,
			Args:                   codec.DepositArgs{TargetContract: cid2, PlasmaLimit: callPlasmaLimit, DepositAsset: testAsset(), DepositAmount: u256(1000)},
			ContractID:             cid2,
			Module:                 blobModule(blob),
			WantStatus:             codec.StatusSuccess,
		})
	}

	{
		idx, src := newIdentity()
		blob := encodeEffects(t, codec.ContractEffects{
			Version:          codec.ExecutionResultVersion,
			GlobalInputIndex: idx,
			ReturnData:       make([]byte, MaxResultBlob+1),
		})
		specs = append(specs, vectorSpec{
			ID:                     "CV-RTL-BLOB-1",
			Purpose:                "a well-formed ContractEffects blob longer than MaxResultBlob: RESULT_TOO_LARGE, metered gas_used with no effect charge",
			Shape:                  "call",
			BatchID:                2,
			Ctx:                    stdCtx(idx),
			SourceAccountBlockHash: src,
			Args:                   codec.CallL2Args{TargetContract: cid2, PlasmaLimit: callPlasmaLimit},
			ContractID:             cid2,
			Module:                 bigBlobModule(blob),
			WantStatus:             codec.StatusResultTooLarge,
		})
	}

	{
		idx, src := newIdentity()
		blob := encodeEffects(t, codec.ContractEffects{
			Version:          codec.ExecutionResultVersion,
			GlobalInputIndex: idx,
			ReturnData:       make([]byte, MaxResultBlob+1),
		})
		blob[0] = 0xFF // corrupt the version field: malformed, still over MaxResultBlob
		specs = append(specs, vectorSpec{
			ID:                     "CV-RTL-BLOB-2",
			Purpose:                "the same, with the over-long blob also malformed, proving step 0 precedes the decode",
			Shape:                  "call",
			BatchID:                2,
			Ctx:                    stdCtx(idx),
			SourceAccountBlockHash: src,
			Args:                   codec.CallL2Args{TargetContract: cid2, PlasmaLimit: callPlasmaLimit},
			ContractID:             cid2,
			Module:                 bigBlobModule(blob),
			WantStatus:             codec.StatusResultTooLarge,
		})
	}

	return specs
}

// unsupportedVector returns EXEC-UNSUP-1's vectorSpec: input_kind 6 is outside the Phase-1 set,
// so DecodeInput returns Input{Unsupported: true} and Apply commits UNSUPPORTED_OPERATION (D18).
func unsupportedVector() vectorSpec {
	return vectorSpec{
		ID:      "EXEC-UNSUP-1",
		Purpose: "an unrecognized input_kind is refused deterministically as UNSUPPORTED_OPERATION",
		Shape:   "unsupported_kind",
		BatchID: 3,
		Ctx: domain.InputContext{
			GlobalInputIndex: 200,
			MomentumHeight:   1000,
			MomentumTime:     1700000000,
			L1Sender:         repeatedAddr(0x01),
		},
		SourceAccountBlockHash: repeatedHash(0xD4),
		WantStatus:             codec.StatusUnsupportedOperation,
	}
}
