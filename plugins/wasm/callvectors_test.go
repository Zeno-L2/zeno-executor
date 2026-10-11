package wasm

import (
	"bytes"
	"errors"
	"math/big"
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

// vectorQuota is the purchased quota seedQuota gives a state-writing vector's contract.
const vectorQuota = 1 << 16

// seedQuota returns a copy of leaves plus the quota leaf of cid: purchased as given, and used
// as the summed quota cost (StateLeafOverhead plus value length) of leaves. Every leaf passed
// must be a non-reserved leaf of cid.
func seedQuota(cid types.Hash, leaves map[domain.Path][]byte, purchased uint64) map[domain.Path][]byte {
	out := make(map[domain.Path][]byte, len(leaves)+1)
	var used uint64
	for path, value := range leaves {
		out[path] = value
		used += StateLeafOverhead + uint64(len(value))
	}
	out[derive(cid, keyQuota)] = encodeQuota(purchased, used)
	return out
}

// blobOffset is where a fixture's pre-encoded ContractEffects blob is placed when it is the
// module's only data segment. It is clear of the input frame, which the runtime writes at
// offset 0 after segment initialization (§7.3, §27.1).
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

// corruptBlob encodes eff with the codec, passes a copy of the encoding to edit, and fails
// the test unless codec.DecodeContractEffects rejects edit's result with an error matching
// wantErr (errors.Is). It returns edit's result.
func corruptBlob(t *testing.T, eff codec.ContractEffects, wantErr error, edit func(blob []byte) []byte) []byte {
	t.Helper()
	blob, err := eff.Encode()
	if err != nil {
		t.Fatalf("corruptBlob: Encode: %v", err)
	}
	out := edit(append([]byte(nil), blob...))
	if _, err := codec.DecodeContractEffects(out); !errors.Is(err, wantErr) {
		t.Fatalf("corruptBlob: DecodeContractEffects err = %v, want errors.Is(..., %v)", err, wantErr)
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

// capsOfOne gives testAsset() a withdrawal cap of 1000.
func capsOfOne() capTable { return capTable{testAsset(): u256(1000)} }

// wideWithdrawal is cid2's L1Withdrawal to repeatedAddr(0xCC) at input idx, outbox index i.
func wideWithdrawal(idx uint64, i uint32, asset codec.AssetID, amount codec.U256) codec.OutboxMessage {
	return codec.OutboxMessage{SourceContract: cid2, InputIndex: idx, OutboxIndex: i, Target: codec.L1Withdrawal{RecipientL1: repeatedAddr(0xCC), Asset: asset, Amount: amount}}
}

// withdrawal is wideWithdrawal with a uint64 amount.
func withdrawal(idx uint64, i uint32, asset codec.AssetID, amount uint64) codec.OutboxMessage {
	return wideWithdrawal(idx, i, asset, u256(amount))
}

// seeded is cid2's quota leaf alone: purchased vectorQuota, used 0.
func seeded() map[domain.Path][]byte { return seedQuota(cid2, nil, vectorQuota) }

// orderVector is the batch-2 CallL2 vectorSpec to cid2 at index idx whose module returns
// build(idx).
func orderVector(idx uint64, id, purpose string, status codec.Status, plasmaLimit uint64, preState map[domain.Path][]byte, caps capTable, build func(idx uint64) []byte) vectorSpec {
	return vectorSpec{
		ID:             id,
		Purpose:        purpose,
		Shape:          "call",
		BatchID:        2,
		Ctx:            stdCtx(idx),
		Args:           codec.CallL2Args{TargetContract: cid2, PlasmaLimit: plasmaLimit},
		ContractID:     cid2,
		Module:         blobModule(build(idx)),
		ExtraPreState:  preState,
		WithdrawalCaps: caps,
		WantStatus:     status,
	}
}

// effectsBlob returns a build function that encodes a version-1 ContractEffects for idx
// after mutate edits it.
func effectsBlob(t *testing.T, mutate func(idx uint64, eff *codec.ContractEffects)) func(idx uint64) []byte {
	return func(idx uint64) []byte {
		eff := codec.ContractEffects{Version: codec.ExecutionResultVersion, GlobalInputIndex: idx}
		mutate(idx, &eff)
		return encodeEffects(t, eff)
	}
}

// callVectors returns every gap-coverage call-shape vectorSpec this ticket adds, in the file's
// table order. unsupportedVector (shape unsupported_kind) is generated separately.
func callVectors(t *testing.T) []vectorSpec {
	t.Helper()

	// The index block is [1000, 2000), append-only: a vector added later takes the next
	// nextIndex() and a vector already using a value is never renumbered. Each generator owns a
	// disjoint block, and assertUniqueIdentities in vectors_test.go checks the pairs.
	n := 0
	nextIndex := func() uint64 {
		idx := uint64(1000 + n)
		n++
		return idx
	}

	var specs []vectorSpec

	// --- regenerated EXEC-003..EXEC-012.

	{
		idx := nextIndex()
		blob := encodeEffects(t, codec.ContractEffects{
			Version:          codec.ExecutionResultVersion,
			GlobalInputIndex: idx,
			StateDiff:        []codec.LocalStateDiffEntry{{LocalKey: []byte("counter"), Value: []byte{0x00, 0x00, 0x00, 0x2b}}},
		})
		specs = append(specs, vectorSpec{
			ID:            "EXEC-003",
			Purpose:       "update an existing key",
			Shape:         "call",
			BatchID:       1,
			Ctx:           stdCtx(idx),
			Args:          codec.CallL2Args{TargetContract: cid1, PlasmaLimit: callPlasmaLimit},
			ContractID:    cid1,
			Module:        blobModule(blob),
			ExtraPreState: seedQuota(cid1, map[domain.Path][]byte{derive(cid1, []byte("counter")): {0x00, 0x00, 0x00, 0x2a}}, vectorQuota),
			WantStatus:    codec.StatusSuccess,
		})
	}

	{
		idx := nextIndex()
		blob := encodeEffects(t, codec.ContractEffects{
			Version:          codec.ExecutionResultVersion,
			GlobalInputIndex: idx,
			StateDiff:        []codec.LocalStateDiffEntry{{LocalKey: []byte("counter"), Value: nil}},
		})
		specs = append(specs, vectorSpec{
			ID:         "EXEC-004",
			Purpose:    "delete an existing key via the EMPTY sentinel",
			Shape:      "call",
			BatchID:    1,
			Ctx:        stdCtx(idx),
			Args:       codec.CallL2Args{TargetContract: cid1, PlasmaLimit: callPlasmaLimit},
			ContractID: cid1,
			Module:     blobModule(blob),
			ExtraPreState: seedQuota(cid1, map[domain.Path][]byte{
				derive(cid1, []byte("counter")): {0x00, 0x00, 0x00, 0x2a},
				derive(cid1, []byte("other")):   {0xff},
			}, vectorQuota),
			WantStatus: codec.StatusSuccess,
		})
	}

	{
		idx := nextIndex()
		blob := encodeEffects(t, codec.ContractEffects{
			Version:          codec.ExecutionResultVersion,
			GlobalInputIndex: idx,
			StateDiff:        []codec.LocalStateDiffEntry{{LocalKey: []byte("flag"), Value: []byte{}}},
		})
		specs = append(specs, vectorSpec{
			ID:            "EXEC-005",
			Purpose:       "write a present-empty value",
			Shape:         "call",
			BatchID:       1,
			Ctx:           stdCtx(idx),
			Args:          codec.CallL2Args{TargetContract: cid1, PlasmaLimit: callPlasmaLimit},
			ContractID:    cid1,
			Module:        blobModule(blob),
			ExtraPreState: seedQuota(cid1, nil, vectorQuota),
			WantStatus:    codec.StatusSuccess,
		})
	}

	{
		idx := nextIndex()
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
			ID:            "EXEC-006",
			Purpose:       "multi-entry StateDiff, sorted by the runtime",
			Shape:         "call",
			BatchID:       1,
			Ctx:           stdCtx(idx),
			Args:          codec.CallL2Args{TargetContract: cid1, PlasmaLimit: callPlasmaLimit},
			ContractID:    cid1,
			Module:        blobModule(blob),
			ExtraPreState: seedQuota(cid1, nil, vectorQuota),
			WantStatus:    codec.StatusSuccess,
		})
	}

	{
		idx := nextIndex()
		blob := encodeEffects(t, codec.ContractEffects{
			Version:          codec.ExecutionResultVersion,
			GlobalInputIndex: idx,
			Events: []codec.Event{
				{ContractID: cid1, InputIndex: idx, EventIndex: 0, Topics: []types.Hash{repeatedHash(0xAA)}, Data: []byte("hello")},
			},
		})
		specs = append(specs, vectorSpec{
			ID:         "EXEC-007",
			Purpose:    "one event, no state change",
			Shape:      "call",
			BatchID:    1,
			Ctx:        stdCtx(idx),
			Args:       codec.CallL2Args{TargetContract: cid1, PlasmaLimit: callPlasmaLimit},
			ContractID: cid1,
			Module:     blobModule(blob),
			WantStatus: codec.StatusSuccess,
		})
	}

	{
		idx := nextIndex()
		blob := encodeEffects(t, codec.ContractEffects{
			Version:          codec.ExecutionResultVersion,
			GlobalInputIndex: idx,
			Outbox: []codec.OutboxMessage{
				{SourceContract: cid1, InputIndex: idx, OutboxIndex: 0, Target: codec.L1Withdrawal{RecipientL1: repeatedAddr(0xBB), Asset: testAsset(), Amount: u256(1000)}, Payload: nil},
			},
		})
		specs = append(specs, vectorSpec{
			ID:             "EXEC-008",
			Purpose:        "one contract-emitted L1 withdrawal",
			Shape:          "call",
			BatchID:        1,
			Ctx:            stdCtx(idx),
			Args:           codec.CallL2Args{TargetContract: cid1, PlasmaLimit: callPlasmaLimit},
			ContractID:     cid1,
			Module:         blobModule(blob),
			WithdrawalCaps: capTable{testAsset(): u256(2000)},
			WantStatus:     codec.StatusSuccess,
		})
	}

	{
		idx := nextIndex()
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
			ID:            "EXEC-009",
			Purpose:       "combined put + event + L2 delivery + return data",
			Shape:         "call",
			BatchID:       1,
			Ctx:           stdCtx(idx),
			Args:          codec.CallL2Args{TargetContract: cid1, PlasmaLimit: callPlasmaLimit},
			ContractID:    cid1,
			Module:        blobModule(blob),
			ExtraPreState: seedQuota(cid1, nil, vectorQuota),
			WantStatus:    codec.StatusSuccess,
		})
	}

	{
		idx := nextIndex()
		body := abortCallBody(7, 0, 0)
		mod := buildWasmModule(runModuleSpec([]string{"abort"}, 1, []byte{0x00}, body, nil))
		specs = append(specs, vectorSpec{
			ID:         "EXEC-010",
			Purpose:    "abort(7) with data_len 0",
			Shape:      "call",
			BatchID:    1,
			Ctx:        stdCtx(idx),
			Args:       codec.CallL2Args{TargetContract: cid1, PlasmaLimit: callPlasmaLimit},
			ContractID: cid1,
			Module:     mod,
			WantStatus: codec.StatusRevert,
		})
	}

	{
		idx := nextIndex()
		body := []byte{0x00, 0x0B} // unreachable; end
		mod := buildWasmModule(runModuleSpec(nil, 1, []byte{0x00}, body, nil))
		specs = append(specs, vectorSpec{
			ID:         "EXEC-011",
			Purpose:    "unreachable",
			Shape:      "call",
			BatchID:    1,
			Ctx:        stdCtx(idx),
			Args:       codec.CallL2Args{TargetContract: cid1, PlasmaLimit: callPlasmaLimit},
			ContractID: cid1,
			Module:     mod,
			WantStatus: codec.StatusRuntimeFault,
		})
	}

	{
		idx := nextIndex()
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
			ID:            "EXEC-012",
			Purpose:       "absent-key storage_read, then write a different key",
			Shape:         "call",
			BatchID:       1,
			Ctx:           stdCtx(idx),
			Args:          codec.CallL2Args{TargetContract: cid1, PlasmaLimit: callPlasmaLimit},
			ContractID:    cid1,
			Module:        mod,
			ExtraPreState: seedQuota(cid1, nil, vectorQuota),
			WantStatus:    codec.StatusSuccess,
		})
	}

	// --- deposit/claim/refund.

	{
		idx := nextIndex()
		blob := encodeEffects(t, codec.ContractEffects{
			Version:          codec.ExecutionResultVersion,
			GlobalInputIndex: idx,
			StateDiff:        []codec.LocalStateDiffEntry{{LocalKey: []byte("d1"), Value: []byte{0x01}}},
			ClaimedDeposit:   u256(1000),
		})
		specs = append(specs, vectorSpec{
			ID:            "EXEC-DEP-1",
			Purpose:       "full claim, no refund",
			Shape:         "call",
			BatchID:       2,
			Ctx:           stdCtx(idx),
			Args:          codec.DepositArgs{TargetContract: cid2, PlasmaLimit: callPlasmaLimit, DepositAsset: testAsset(), DepositAmount: u256(1000)},
			ContractID:    cid2,
			Module:        blobModule(blob),
			ExtraPreState: seedQuota(cid2, nil, vectorQuota),
			WantStatus:    codec.StatusSuccess,
		})
	}

	{
		idx := nextIndex()
		blob := encodeEffects(t, codec.ContractEffects{
			Version:          codec.ExecutionResultVersion,
			GlobalInputIndex: idx,
			Outbox: []codec.OutboxMessage{
				{SourceContract: cid2, InputIndex: idx, OutboxIndex: 0, Target: codec.L2Delivery{TargetContract: repeatedHash(0x55)}, Payload: nil},
			},
			ClaimedDeposit: u256(400),
		})
		specs = append(specs, vectorSpec{
			ID:         "EXEC-DEP-2",
			Purpose:    "partial claim, refund after the contract's messages",
			Shape:      "call",
			BatchID:    2,
			Ctx:        stdCtx(idx),
			Args:       codec.DepositArgs{TargetContract: cid2, PlasmaLimit: callPlasmaLimit, DepositAsset: testAsset(), DepositAmount: u256(1000)},
			ContractID: cid2,
			Module:     blobModule(blob),
			WantStatus: codec.StatusSuccess,
		})
	}

	{
		idx := nextIndex()
		blob := encodeEffects(t, codec.ContractEffects{
			Version:          codec.ExecutionResultVersion,
			GlobalInputIndex: idx,
			ClaimedDeposit:   u256(1001),
		})
		specs = append(specs, vectorSpec{
			ID:         "EXEC-DEP-3",
			Purpose:    "claim above deposit_amount",
			Shape:      "call",
			BatchID:    2,
			Ctx:        stdCtx(idx),
			Args:       codec.DepositArgs{TargetContract: cid2, PlasmaLimit: callPlasmaLimit, DepositAsset: testAsset(), DepositAmount: u256(1000)},
			ContractID: cid2,
			Module:     blobModule(blob),
			WantStatus: codec.StatusRuntimeFault,
		})
	}

	{
		idx := nextIndex()
		blob := encodeEffects(t, codec.ContractEffects{
			Version:          codec.ExecutionResultVersion,
			GlobalInputIndex: idx,
			ClaimedDeposit:   u256(1),
		})
		specs = append(specs, vectorSpec{
			ID:         "EXEC-DEP-4",
			Purpose:    "non-zero claim against a zero deposit",
			Shape:      "call",
			BatchID:    2,
			Ctx:        stdCtx(idx),
			Args:       codec.CallL2Args{TargetContract: cid2, PlasmaLimit: callPlasmaLimit},
			ContractID: cid2,
			Module:     blobModule(blob),
			WantStatus: codec.StatusRuntimeFault,
		})
	}

	// --- blob-content rejections.

	{
		idx := nextIndex()
		eff := codec.ContractEffects{
			Version:          codec.ExecutionResultVersion,
			GlobalInputIndex: idx,
			Outbox: []codec.OutboxMessage{
				{SourceContract: cid2, InputIndex: idx, OutboxIndex: 0, Target: codec.L1Withdrawal{RecipientL1: repeatedAddr(0xCC), Asset: testAsset(), Amount: u256(1)}, Payload: nil},
			},
		}
		blob := withdrawalPayloadBlob(t, eff)
		specs = append(specs, vectorSpec{
			ID:         "CV-OUTBOX-1",
			Purpose:    "a withdrawal carrying a payload is rejected at the runtime layer",
			Shape:      "call",
			BatchID:    2,
			Ctx:        stdCtx(idx),
			Args:       codec.CallL2Args{TargetContract: cid2, PlasmaLimit: callPlasmaLimit},
			ContractID: cid2,
			Module:     blobModule(blob),
			WantStatus: codec.StatusRuntimeFault,
		})
	}

	{
		idx := nextIndex()
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
			ID:         "CV-ROOTS-1",
			Purpose:    "an ExecutionResult returned where a ContractEffects is expected does not decode as a ContractEffects and the input faults",
			Shape:      "call",
			BatchID:    2,
			Ctx:        stdCtx(idx),
			Args:       codec.CallL2Args{TargetContract: cid2, PlasmaLimit: callPlasmaLimit},
			ContractID: cid2,
			Module:     blobModule(blob),
			WantStatus: codec.StatusRuntimeFault,
		})
	}

	{
		idx := nextIndex()
		blob := encodeEffects(t, codec.ContractEffects{
			Version:          codec.ExecutionResultVersion,
			GlobalInputIndex: idx,
			StateDiff: []codec.LocalStateDiffEntry{
				{LocalKey: []byte("dup"), Value: []byte("a")},
				{LocalKey: []byte("dup"), Value: []byte("b")},
			},
		})
		specs = append(specs, vectorSpec{
			ID:            "CV-DUPKEY-1",
			Purpose:       "two StateDiff entries deriving to one key",
			Shape:         "call",
			BatchID:       2,
			Ctx:           stdCtx(idx),
			Args:          codec.CallL2Args{TargetContract: cid2, PlasmaLimit: callPlasmaLimit},
			ContractID:    cid2,
			Module:        blobModule(blob),
			ExtraPreState: seedQuota(cid2, nil, vectorQuota),
			WantStatus:    codec.StatusRuntimeFault,
		})
	}

	{
		idx := nextIndex()
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
			ID:            "CV-UNAUTH-1",
			Purpose:       "a write to a reserved local key",
			Shape:         "call",
			BatchID:       2,
			Ctx:           stdCtx(idx),
			Args:          codec.CallL2Args{TargetContract: cid2, PlasmaLimit: callPlasmaLimit},
			ContractID:    cid2,
			Module:        blobModule(blob),
			ExtraPreState: seedQuota(cid2, nil, vectorQuota),
			WantStatus:    codec.StatusUnauthorized,
		})
	}

	// --- size rejections.

	{
		idx := nextIndex()
		blob := encodeEffects(t, codec.ContractEffects{
			Version:          codec.ExecutionResultVersion,
			GlobalInputIndex: idx,
			ReturnData:       bytes.Repeat([]byte{0xAB}, codec.MaxReturnData+1),
		})
		specs = append(specs, vectorSpec{
			ID:         "CV-RTL-RETURN-1",
			Purpose:    "len(ReturnData) > MaxReturnData alone",
			Shape:      "call",
			BatchID:    2,
			Ctx:        stdCtx(idx),
			Args:       codec.CallL2Args{TargetContract: cid2, PlasmaLimit: callPlasmaLimit},
			ContractID: cid2,
			Module:     blobModule(blob),
			WantStatus: codec.StatusResultTooLarge,
		})
	}

	{
		idx := nextIndex()
		// A contract-only ExecutionResult with N state entries, no events, no outbox, empty
		// ReturnData encodes to 122 + Σ(36+len(v)) bytes; k2's length is derived so k1+k2 land
		// exactly at MaxExecutionResult, the largest contract-only result that still passes step
		// 8, and the appended refund (133 bytes) pushes it over. The runtime's quota entry
		// (36 + 16 bytes) is part of that result, so k2 is shortened by it.
		k2Len := MaxExecutionResult - 122 - 36 - codec.MaxStateValueSize - 36 - (36 + 16)
		blob := encodeEffects(t, codec.ContractEffects{
			Version:          codec.ExecutionResultVersion,
			GlobalInputIndex: idx,
			StateDiff: []codec.LocalStateDiffEntry{
				{LocalKey: []byte("k1"), Value: make([]byte, codec.MaxStateValueSize)},
				{LocalKey: []byte("k2"), Value: make([]byte, k2Len)},
			},
		})
		specs = append(specs, vectorSpec{
			ID:            "CV-RTL-RESULT-1",
			Purpose:       "the refund pushes the ExecutionResult past MaxExecutionResult",
			Shape:         "call",
			BatchID:       2,
			Ctx:           stdCtx(idx),
			Args:          codec.DepositArgs{TargetContract: cid2, PlasmaLimit: callPlasmaLimit, DepositAsset: testAsset(), DepositAmount: u256(1000)},
			ContractID:    cid2,
			Module:        blobModule(blob),
			ExtraPreState: seedQuota(cid2, nil, vectorQuota),
			WantStatus:    codec.StatusResultTooLarge,
		})
	}

	// --- OUT_OF_GAS, all four forms.

	{
		idx := nextIndex()
		blob := encodeEffects(t, codec.ContractEffects{Version: codec.ExecutionResultVersion, GlobalInputIndex: idx})
		specs = append(specs, vectorSpec{
			ID:         "CV-OOG-1",
			Purpose:    "pre-execution initial-memory charge; nothing runs",
			Shape:      "call",
			BatchID:    2,
			Ctx:        stdCtx(idx),
			Args:       codec.CallL2Args{TargetContract: cid2, PlasmaLimit: module.InitialMemoryCost(1) - 1},
			ContractID: cid2,
			Module:     blobModule(blob),
			WantStatus: codec.StatusOutOfGas,
		})
	}

	{
		idx := nextIndex()
		blob := encodeEffects(t, codec.ContractEffects{Version: codec.ExecutionResultVersion, GlobalInputIndex: idx})
		specs = append(specs, vectorSpec{
			ID:         "CV-OOG-2",
			Purpose:    "an injected segment charge",
			Shape:      "call",
			BatchID:    2,
			Ctx:        stdCtx(idx),
			Args:       codec.CallL2Args{TargetContract: cid2, PlasmaLimit: module.InitialMemoryCost(1)},
			ContractID: cid2,
			Module:     blobModule(blob),
			WantStatus: codec.StatusOutOfGas,
		})
	}

	{
		idx := nextIndex()
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
			ID:         "CV-OOG-3",
			Purpose:    "a host-function charge",
			Shape:      "call",
			BatchID:    2,
			Ctx:        stdCtx(idx),
			Args:       codec.CallL2Args{TargetContract: cid2, PlasmaLimit: module.InitialMemoryCost(1) + costOf(t, 0x41, 0x41, 0x10, 0x1A, 0x42, 0x0B)},
			ContractID: cid2,
			Module:     mod,
			WantStatus: codec.StatusOutOfGas,
		})
	}

	{
		idx := nextIndex()
		blob := encodeEffects(t, codec.ContractEffects{
			Version:          codec.ExecutionResultVersion,
			GlobalInputIndex: idx,
			StateDiff:        []codec.LocalStateDiffEntry{{LocalKey: []byte("x"), Value: []byte{0x01}}},
			ReturnData:       []byte{0x02},
		})
		specs = append(specs, vectorSpec{
			ID:            "CV-OOG-4",
			Purpose:       "the §10.4 effect charge, step 6",
			Shape:         "call",
			BatchID:       2,
			Ctx:           stdCtx(idx),
			Args:          codec.CallL2Args{TargetContract: cid2, PlasmaLimit: module.InitialMemoryCost(1) + costOf(t, 0x42, 0x0B)},
			ContractID:    cid2,
			Module:        blobModule(blob),
			ExtraPreState: seedQuota(cid2, nil, vectorQuota),
			WantStatus:    codec.StatusOutOfGas,
		})
	}

	// --- host and engine behaviour.

	{
		idx := nextIndex()
		blob := encodeEffects(t, codec.ContractEffects{Version: codec.ExecutionResultVersion, GlobalInputIndex: idx})
		value8 := []byte{1, 2, 3, 4, 5, 6, 7, 8}
		key := []byte("value8")
		body := hostResultGuardBody(1024, len(key), 2048, 4, -2, 1032, len(blob))
		mod := buildWasmModule(runModuleSpec([]string{"storage_read"}, 1, []byte{0x00}, body,
			[]dataSeg{{offset: 1024, bytes: key}, {offset: 1032, bytes: blob}}))
		specs = append(specs, vectorSpec{
			ID:            "CV-READ-2",
			Purpose:       "storage_read returns -2 when the out buffer is too small",
			Shape:         "call",
			BatchID:       2,
			Ctx:           stdCtx(idx),
			Args:          codec.CallL2Args{TargetContract: cid2, PlasmaLimit: callPlasmaLimit},
			ContractID:    cid2,
			Module:        mod,
			ExtraPreState: map[domain.Path][]byte{derive(cid2, []byte("value8")): value8},
			WantStatus:    codec.StatusSuccess,
		})
	}

	{
		idx := nextIndex()
		body := appendVarS64([]byte{0x42}, (int64(65530)<<32)|int64(100))
		body = append(body, 0x0B)
		mod := buildWasmModule(runModuleSpec(nil, 1, []byte{0x00}, body, nil))
		specs = append(specs, vectorSpec{
			ID:         "CV-OOB-1",
			Purpose:    "result_ptr + result_len outside memory",
			Shape:      "call",
			BatchID:    2,
			Ctx:        stdCtx(idx),
			Args:       codec.CallL2Args{TargetContract: cid2, PlasmaLimit: callPlasmaLimit},
			ContractID: cid2,
			Module:     mod,
			WantStatus: codec.StatusRuntimeFault,
		})
	}

	{
		idx := nextIndex()
		blob := segBlob(t, idx)
		mod := blobModuleBody(blob, growGuardBody(len(blob)))
		specs = append(specs, vectorSpec{
			ID:         "CV-GROW-1",
			Purpose:    "memory.grow past MaxMemory returns -1, grows nothing, pays no per-page surcharge",
			Shape:      "call",
			BatchID:    2,
			Ctx:        stdCtx(idx),
			Args:       codec.CallL2Args{TargetContract: cid2, PlasmaLimit: callPlasmaLimit},
			ContractID: cid2,
			Module:     mod,
			WantStatus: codec.StatusSuccess,
		})
	}

	{
		idx := nextIndex()
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
			ID:         "CV-DEPTH-1",
			Purpose:    "the 513th frame traps",
			Shape:      "call",
			BatchID:    2,
			Ctx:        stdCtx(idx),
			Args:       codec.CallL2Args{TargetContract: cid2, PlasmaLimit: callPlasmaLimit},
			ContractID: cid2,
			Module:     mod,
			WantStatus: codec.StatusRuntimeFault,
		})
	}

	// --- segmentation.

	{
		idx := nextIndex()
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
			ID:         "CV-SEG-BRIF-1",
			Purpose:    "a taken br_if skips the rest of its block",
			Shape:      "call",
			BatchID:    2,
			Ctx:        stdCtx(idx),
			Args:       codec.CallL2Args{TargetContract: cid2, PlasmaLimit: callPlasmaLimit},
			ContractID: cid2,
			Module:     blobModuleBody(blob, body),
			WantStatus: codec.StatusSuccess,
		})
	}

	{
		idx := nextIndex()
		blob := segBlob(t, idx)
		body := []byte{
			0x41, 0x00, // i32.const 0
			0x04, 0x40, // if (void)
			0x01, 0x01, // nop; nop
			0x0B, // end (if)
		}
		body = append(body, returnBlobBody(blobOffset, len(blob))...)
		specs = append(specs, vectorSpec{
			ID:         "CV-SEG-IF-1",
			Purpose:    "a false if skips its arm",
			Shape:      "call",
			BatchID:    2,
			Ctx:        stdCtx(idx),
			Args:       codec.CallL2Args{TargetContract: cid2, PlasmaLimit: callPlasmaLimit},
			ContractID: cid2,
			Module:     blobModuleBody(blob, body),
			WantStatus: codec.StatusSuccess,
		})
	}

	{
		idx := nextIndex()
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
			ID:         "CV-SEG-AFTEREND-1",
			Purpose:    "a branch landing after an end",
			Shape:      "call",
			BatchID:    2,
			Ctx:        stdCtx(idx),
			Args:       codec.CallL2Args{TargetContract: cid2, PlasmaLimit: callPlasmaLimit},
			ContractID: cid2,
			Module:     blobModuleBody(blob, body),
			WantStatus: codec.StatusSuccess,
		})
	}

	{
		idx := nextIndex()
		blob := segBlob(t, idx)
		body := appendVarS64([]byte{0x42}, (int64(blobOffset)<<32)|int64(len(blob)))
		body = append(body, 0x0C, 0x00, 0x0B) // br 0; end
		specs = append(specs, vectorSpec{
			ID:         "CV-SEG-OUTER-1",
			Purpose:    "a branch to the function's outermost label",
			Shape:      "call",
			BatchID:    2,
			Ctx:        stdCtx(idx),
			Args:       codec.CallL2Args{TargetContract: cid2, PlasmaLimit: callPlasmaLimit},
			ContractID: cid2,
			Module:     blobModuleBody(blob, body),
			WantStatus: codec.StatusSuccess,
		})
	}

	// --- deposit refund exempt from the outbox cap; blob-length rejections ahead of decode.

	{
		idx := nextIndex()
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
			ID:         "EXEC-DEP-5",
			Purpose:    "a contract at MaxOutboxPerExecution with an unclaimed remainder: SUCCESS, outbox_count = 33, the refund at outbox_index = 32",
			Shape:      "call",
			BatchID:    2,
			Ctx:        stdCtx(idx),
			Args:       codec.DepositArgs{TargetContract: cid2, PlasmaLimit: callPlasmaLimit, DepositAsset: testAsset(), DepositAmount: u256(1000)},
			ContractID: cid2,
			Module:     blobModule(blob),
			WantStatus: codec.StatusSuccess,
		})
	}

	{
		idx := nextIndex()
		blob := encodeEffects(t, codec.ContractEffects{
			Version:          codec.ExecutionResultVersion,
			GlobalInputIndex: idx,
			ReturnData:       make([]byte, MaxResultBlob+1),
		})
		specs = append(specs, vectorSpec{
			ID:         "CV-RTL-BLOB-1",
			Purpose:    "a well-formed ContractEffects blob longer than MaxResultBlob: RESULT_TOO_LARGE, metered gas_used with no effect charge",
			Shape:      "call",
			BatchID:    2,
			Ctx:        stdCtx(idx),
			Args:       codec.CallL2Args{TargetContract: cid2, PlasmaLimit: callPlasmaLimit},
			ContractID: cid2,
			Module:     bigBlobModule(blob),
			WantStatus: codec.StatusResultTooLarge,
		})
	}

	{
		idx := nextIndex()
		blob := encodeEffects(t, codec.ContractEffects{
			Version:          codec.ExecutionResultVersion,
			GlobalInputIndex: idx,
			ReturnData:       make([]byte, MaxResultBlob+1),
		})
		blob[0] = 0xFF // corrupt the version field: malformed, still over MaxResultBlob
		specs = append(specs, vectorSpec{
			ID:         "CV-RTL-BLOB-2",
			Purpose:    "the same, with the over-long blob also malformed, proving step 0 precedes the decode",
			Shape:      "call",
			BatchID:    2,
			Ctx:        stdCtx(idx),
			Args:       codec.CallL2Args{TargetContract: cid2, PlasmaLimit: callPlasmaLimit},
			ContractID: cid2,
			Module:     bigBlobModule(blob),
			WantStatus: codec.StatusResultTooLarge,
		})
	}

	// --- step 5a footprint: CV-ASSETS-1/2.

	{
		idx := nextIndex()
		outbox := make([]codec.OutboxMessage, 17)
		for i := range outbox {
			outbox[i] = codec.OutboxMessage{SourceContract: cid2, InputIndex: idx, OutboxIndex: uint32(i), Target: codec.L1Withdrawal{RecipientL1: repeatedAddr(0xCC), Asset: assetsTestAssetDistinct(byte(i + 1)), Amount: u256(1)}}
		}
		blob := encodeEffects(t, codec.ContractEffects{Version: codec.ExecutionResultVersion, GlobalInputIndex: idx, Outbox: outbox})
		specs = append(specs, vectorSpec{
			ID:             "CV-ASSETS-1",
			Purpose:        "17 distinct-asset withdrawals in one input, every asset capped above its total, breach the per-input asset footprint, so only the footprint faults: RUNTIME_FAULT at the metered gas",
			Shape:          "call",
			BatchID:        2,
			Ctx:            stdCtx(idx),
			Args:           codec.CallL2Args{TargetContract: cid2, PlasmaLimit: callPlasmaLimit},
			ContractID:     cid2,
			Module:         blobModule(blob),
			WithdrawalCaps: capsOf(1000, assetsTestAssetRange(1, 17)...),
			WantStatus:     codec.StatusRuntimeFault,
		})
	}

	{
		idx := nextIndex()
		outbox := make([]codec.OutboxMessage, 16)
		for i := range outbox {
			outbox[i] = codec.OutboxMessage{SourceContract: cid2, InputIndex: idx, OutboxIndex: uint32(i), Target: codec.L1Withdrawal{RecipientL1: repeatedAddr(0xCC), Asset: assetsTestAssetDistinct(byte(i + 1)), Amount: u256(1)}}
		}
		blob := encodeEffects(t, codec.ContractEffects{Version: codec.ExecutionResultVersion, GlobalInputIndex: idx, Outbox: outbox, ClaimedDeposit: codec.U256{}})
		specs = append(specs, vectorSpec{
			ID:             "CV-ASSETS-2",
			Purpose:        "16 distinct-asset withdrawals plus a non-zero deposit in a 17th asset, every asset capped above its total, breach the footprint, so only the footprint faults: RUNTIME_FAULT, full refund",
			Shape:          "call",
			BatchID:        2,
			Ctx:            stdCtx(idx),
			Args:           codec.CallL2Args{TargetContract: cid2, PlasmaLimit: callPlasmaLimit, DepositAsset: testAsset(), DepositAmount: u256(1000)},
			ContractID:     cid2,
			Module:         blobModule(blob),
			WithdrawalCaps: capsOf(1000, append(assetsTestAssetRange(1, 16), testAsset())...),
			WantStatus:     codec.StatusRuntimeFault,
		})
	}

	// --- storage quota (step 6a).

	v8 := bytes.Repeat([]byte{0x08}, 8)
	v4 := bytes.Repeat([]byte{0x04}, 4)
	q := func(name string) domain.Path { return derive(cid2, []byte(name)) }

	{
		idx := nextIndex()
		blob := encodeEffects(t, codec.ContractEffects{
			Version:          codec.ExecutionResultVersion,
			GlobalInputIndex: idx,
			StateDiff:        []codec.LocalStateDiffEntry{{LocalKey: []byte("q1"), Value: v8}},
		})
		specs = append(specs, vectorSpec{
			ID:            "CV-QUOTA-1",
			Purpose:       "a write that brings used exactly to purchased succeeds, and the committed StateDiff carries the updated 0x0005 leaf",
			Shape:         "call",
			BatchID:       2,
			Ctx:           stdCtx(idx),
			Args:          codec.CallL2Args{TargetContract: cid2, PlasmaLimit: callPlasmaLimit},
			ContractID:    cid2,
			Module:        blobModule(blob),
			ExtraPreState: seedQuota(cid2, map[domain.Path][]byte{q("q0"): v8}, 144),
			WantStatus:    codec.StatusSuccess,
		})
	}

	{
		idx := nextIndex()
		blob := encodeEffects(t, codec.ContractEffects{
			Version:          codec.ExecutionResultVersion,
			GlobalInputIndex: idx,
			StateDiff:        []codec.LocalStateDiffEntry{{LocalKey: []byte("q1"), Value: v8}},
			ClaimedDeposit:   u256(1000),
		})
		specs = append(specs, vectorSpec{
			ID:            "CV-QUOTA-2",
			Purpose:       "net state growth one byte over the remaining quota is STORAGE_QUOTA_EXCEEDED at metered plus effect gas, with the full deposit refunded",
			Shape:         "call",
			BatchID:       2,
			Ctx:           stdCtx(idx),
			Args:          codec.CallL2Args{TargetContract: cid2, PlasmaLimit: callPlasmaLimit, DepositAsset: testAsset(), DepositAmount: u256(1000)},
			ContractID:    cid2,
			Module:        blobModule(blob),
			ExtraPreState: seedQuota(cid2, map[domain.Path][]byte{q("q0"): v8}, 143),
			WantStatus:    codec.StatusStorageQuotaExceeded,
		})
	}

	{
		idx := nextIndex()
		blob := encodeEffects(t, codec.ContractEffects{
			Version:          codec.ExecutionResultVersion,
			GlobalInputIndex: idx,
			StateDiff: []codec.LocalStateDiffEntry{
				{LocalKey: []byte("q0"), Value: nil},
				{LocalKey: []byte("q2"), Value: v4},
			},
		})
		specs = append(specs, vectorSpec{
			ID:            "CV-QUOTA-3",
			Purpose:       "at a fully used quota, deleting a leaf frees its bytes and a smaller write reuses them without buying more",
			Shape:         "call",
			BatchID:       2,
			Ctx:           stdCtx(idx),
			Args:          codec.CallL2Args{TargetContract: cid2, PlasmaLimit: callPlasmaLimit},
			ContractID:    cid2,
			Module:        blobModule(blob),
			ExtraPreState: seedQuota(cid2, map[domain.Path][]byte{q("q0"): v8, q("q1"): v8}, 144),
			WantStatus:    codec.StatusSuccess,
		})
	}

	{
		idx := nextIndex()
		blob := encodeEffects(t, codec.ContractEffects{
			Version:          codec.ExecutionResultVersion,
			GlobalInputIndex: idx,
			StateDiff: []codec.LocalStateDiffEntry{
				{LocalKey: []byte("q0"), Value: nil},
				{LocalKey: []byte("q1"), Value: v8},
			},
		})
		specs = append(specs, vectorSpec{
			ID:            "CV-QUOTA-4",
			Purpose:       "writes that net to delta = 0 succeed at a fully used quota, commit no quota entry, and leave 0x0005 out of the witness set",
			Shape:         "call",
			BatchID:       2,
			Ctx:           stdCtx(idx),
			Args:          codec.CallL2Args{TargetContract: cid2, PlasmaLimit: callPlasmaLimit},
			ContractID:    cid2,
			Module:        blobModule(blob),
			ExtraPreState: seedQuota(cid2, map[domain.Path][]byte{q("q0"): v8}, 72),
			WantStatus:    codec.StatusSuccess,
		})
	}

	{
		idx := nextIndex()
		blob := encodeEffects(t, codec.ContractEffects{
			Version:          codec.ExecutionResultVersion,
			GlobalInputIndex: idx,
			StateDiff:        []codec.LocalStateDiffEntry{{LocalKey: keyQuota, Value: encodeQuota(1<<20, 0)}},
		})
		specs = append(specs, vectorSpec{
			ID:         "CV-QUOTA-5",
			Purpose:    "a contract writing the reserved quota key 0x0005 is UNAUTHORIZED",
			Shape:      "call",
			BatchID:    2,
			Ctx:        stdCtx(idx),
			Args:       codec.CallL2Args{TargetContract: cid2, PlasmaLimit: callPlasmaLimit},
			ContractID: cid2,
			Module:     blobModule(blob),
			WantStatus: codec.StatusUnauthorized,
		})
	}

	// --- withdrawal caps (step 5a): CV-CAP-1..12.

	pow2 := func(n uint) *big.Int { return new(big.Int).Lsh(big.NewInt(1), n) }

	{
		idx := nextIndex()
		blob := encodeEffects(t, codec.ContractEffects{
			Version:          codec.ExecutionResultVersion,
			GlobalInputIndex: idx,
			Outbox:           []codec.OutboxMessage{withdrawal(idx, 0, testAsset(), 600), withdrawal(idx, 1, testAsset(), 400)},
		})
		specs = append(specs, vectorSpec{
			ID:             "CV-CAP-1",
			Purpose:        "two withdrawals in one asset summing exactly to its MaxBatchWithdrawal pass step 5a",
			Shape:          "call",
			BatchID:        2,
			Ctx:            stdCtx(idx),
			Args:           codec.CallL2Args{TargetContract: cid2, PlasmaLimit: callPlasmaLimit},
			ContractID:     cid2,
			Module:         blobModule(blob),
			WithdrawalCaps: capsOfOne(),
			WantStatus:     codec.StatusSuccess,
		})
	}

	{
		idx := nextIndex()
		blob := encodeEffects(t, codec.ContractEffects{
			Version:          codec.ExecutionResultVersion,
			GlobalInputIndex: idx,
			StateDiff:        []codec.LocalStateDiffEntry{{LocalKey: []byte("c"), Value: []byte{0x01}}},
			Events:           []codec.Event{{ContractID: cid2, InputIndex: idx, EventIndex: 0, Data: []byte("evt")}},
			Outbox:           []codec.OutboxMessage{withdrawal(idx, 0, testAsset(), 600), withdrawal(idx, 1, testAsset(), 401)},
		})
		specs = append(specs, vectorSpec{
			ID:             "CV-CAP-2",
			Purpose:        "two withdrawals in one asset summing to one above its MaxBatchWithdrawal: RUNTIME_FAULT at the metered gas, full rollback and the full deposit refunded",
			Shape:          "call",
			BatchID:        2,
			Ctx:            stdCtx(idx),
			Args:           codec.CallL2Args{TargetContract: cid2, PlasmaLimit: callPlasmaLimit, DepositAsset: testAsset(), DepositAmount: u256(1000)},
			ContractID:     cid2,
			Module:         blobModule(blob),
			WithdrawalCaps: capsOfOne(),
			WantStatus:     codec.StatusRuntimeFault,
		})
	}

	{
		idx := nextIndex()
		blob := encodeEffects(t, codec.ContractEffects{
			Version:          codec.ExecutionResultVersion,
			GlobalInputIndex: idx,
			StateDiff:        []codec.LocalStateDiffEntry{{LocalKey: []byte("c"), Value: []byte{0x01}}},
			Outbox:           []codec.OutboxMessage{withdrawal(idx, 0, assetsTestAssetDistinct(1), 1)},
		})
		specs = append(specs, vectorSpec{
			ID:             "CV-CAP-3",
			Purpose:        "a non-zero withdrawal in an asset with no AssetConfig (absent from withdrawal_caps, cap 0): RUNTIME_FAULT, full rollback and the full deposit refunded",
			Shape:          "call",
			BatchID:        2,
			Ctx:            stdCtx(idx),
			Args:           codec.CallL2Args{TargetContract: cid2, PlasmaLimit: callPlasmaLimit, DepositAsset: testAsset(), DepositAmount: u256(1000)},
			ContractID:     cid2,
			Module:         blobModule(blob),
			WithdrawalCaps: capsOfOne(),
			WantStatus:     codec.StatusRuntimeFault,
		})
	}

	{
		idx := nextIndex()
		blob := encodeEffects(t, codec.ContractEffects{
			Version:          codec.ExecutionResultVersion,
			GlobalInputIndex: idx,
			Outbox:           []codec.OutboxMessage{withdrawal(idx, 0, assetsTestAssetDistinct(1), 0)},
		})
		specs = append(specs, vectorSpec{
			ID:             "CV-CAP-4",
			Purpose:        "a zero-amount withdrawal in an asset with no AssetConfig (cap 0) passes step 5a",
			Shape:          "call",
			BatchID:        2,
			Ctx:            stdCtx(idx),
			Args:           codec.CallL2Args{TargetContract: cid2, PlasmaLimit: callPlasmaLimit},
			ContractID:     cid2,
			Module:         blobModule(blob),
			WithdrawalCaps: capsOfOne(),
			WantStatus:     codec.StatusSuccess,
		})
	}

	// Three assets in canonical layout (bytes [0:22] zero), ascending in
	// the order d1, d2, testAsset(). Each pair differs within the ZTS
	// bytes [22:32]. d2 differs from testAsset() only in byte 31, so a
	// total keyed by the high ZTS bytes (id[22:30]) merges them. d1
	// differs from d2 only in byte 22, the first ZTS byte, so a total
	// keyed by the low 8 bytes (id[24:32]) merges them. A key taken from
	// bytes [0:22] merges all three. The caps are not monotonic in
	// ascending order: the first is the largest and the middle the
	// smallest.
	d2 := testAsset()
	d2[31] = 0x09
	d1 := d2
	d1[22] = 0x00
	threeCaps := capTable{d1: u256(300), d2: u256(100), testAsset(): u256(200)}

	{
		idx := nextIndex()
		blob := encodeEffects(t, codec.ContractEffects{
			Version:          codec.ExecutionResultVersion,
			GlobalInputIndex: idx,
			Outbox:           []codec.OutboxMessage{withdrawal(idx, 0, d2, 25), withdrawal(idx, 1, d1, 100), withdrawal(idx, 2, d2, 75), withdrawal(idx, 3, testAsset(), 200), withdrawal(idx, 4, d1, 200)},
		})
		specs = append(specs, vectorSpec{
			ID:             "CV-CAP-5",
			Purpose:        "withdrawals in three assets with three different caps, each totalling exactly its own asset's MaxBatchWithdrawal, pass step 5a: each asset is checked against its own cap only",
			Shape:          "call",
			BatchID:        2,
			Ctx:            stdCtx(idx),
			Args:           codec.CallL2Args{TargetContract: cid2, PlasmaLimit: callPlasmaLimit},
			ContractID:     cid2,
			Module:         blobModule(blob),
			WithdrawalCaps: threeCaps,
			WantStatus:     codec.StatusSuccess,
		})
	}

	// --- CV-CAP-6/7.

	{
		idx := nextIndex()
		blob := encodeEffects(t, codec.ContractEffects{
			Version:          codec.ExecutionResultVersion,
			GlobalInputIndex: idx,
			StateDiff:        []codec.LocalStateDiffEntry{{LocalKey: []byte("c"), Value: []byte{0x01}}},
			Events:           []codec.Event{{ContractID: cid2, InputIndex: idx, EventIndex: 0, Data: []byte("evt")}},
			Outbox:           []codec.OutboxMessage{withdrawal(idx, 0, testAsset(), 25), withdrawal(idx, 1, testAsset(), 25), withdrawal(idx, 2, d2, 60), withdrawal(idx, 3, d1, 50), withdrawal(idx, 4, d2, 60), withdrawal(idx, 5, d1, 150)},
		})
		specs = append(specs, vectorSpec{
			ID:             "CV-CAP-6",
			Purpose:        "the middle of three assets in ascending and in first-appearance order over its own cap in total through two identical withdrawals, each alone within the cap, the other two assets within their own caps: RUNTIME_FAULT at the metered gas, full rollback and the full deposit refunded",
			Shape:          "call",
			BatchID:        2,
			Ctx:            stdCtx(idx),
			Args:           codec.CallL2Args{TargetContract: cid2, PlasmaLimit: callPlasmaLimit, DepositAsset: testAsset(), DepositAmount: u256(1000)},
			ContractID:     cid2,
			Module:         blobModule(blob),
			WithdrawalCaps: threeCaps,
			WantStatus:     codec.StatusRuntimeFault,
		})
	}

	{
		idx := nextIndex()
		blob := encodeEffects(t, codec.ContractEffects{
			Version:          codec.ExecutionResultVersion,
			GlobalInputIndex: idx,
			StateDiff:        []codec.LocalStateDiffEntry{{LocalKey: []byte("c"), Value: []byte{0x01}}},
			Outbox:           []codec.OutboxMessage{withdrawal(idx, 0, testAsset(), 1001)},
		})
		specs = append(specs, vectorSpec{
			ID:             "CV-CAP-7",
			Purpose:        "a withdrawal over its cap at a plasma_limit that leaves nothing for the §10.4 effect charge: RUNTIME_FAULT from step 5a, not OUT_OF_GAS from step 6",
			Shape:          "call",
			BatchID:        2,
			Ctx:            stdCtx(idx),
			Args:           codec.CallL2Args{TargetContract: cid2, PlasmaLimit: module.InitialMemoryCost(1) + costOf(t, 0x42, 0x0B)},
			ContractID:     cid2,
			Module:         blobModule(blob),
			WithdrawalCaps: capsOfOne(),
			WantStatus:     codec.StatusRuntimeFault,
		})
	}

	// 2^256 - 1, the largest representable cap.
	maxU256 := new(big.Int).Sub(pow2(256), big.NewInt(1))
	capVector := func(id, purpose string, amounts []*big.Int, capValue *big.Int, status codec.Status) {
		idx := nextIndex()
		outbox := make([]codec.OutboxMessage, len(amounts))
		for i, amount := range amounts {
			outbox[i] = wideWithdrawal(idx, uint32(i), testAsset(), u256FromBig(t, amount))
		}
		effects := codec.ContractEffects{
			Version:          codec.ExecutionResultVersion,
			GlobalInputIndex: idx,
			Outbox:           outbox,
		}
		args := codec.CallL2Args{TargetContract: cid2, PlasmaLimit: callPlasmaLimit}
		var preState map[domain.Path][]byte
		if status == codec.StatusRuntimeFault {
			// A state write, an event and a deposit give the full rollback and the full
			// refund something to discard and return.
			effects.StateDiff = []codec.LocalStateDiffEntry{{LocalKey: []byte("c"), Value: []byte{0x01}}}
			effects.Events = []codec.Event{{ContractID: cid2, InputIndex: idx, EventIndex: 0, Data: []byte("evt")}}
			args.DepositAsset = testAsset()
			args.DepositAmount = u256(1000)
			preState = seedQuota(cid2, nil, vectorQuota)
		}
		blob := encodeEffects(t, effects)
		specs = append(specs, vectorSpec{
			ID:             id,
			Purpose:        purpose,
			Shape:          "call",
			BatchID:        2,
			Ctx:            stdCtx(idx),
			Args:           args,
			ContractID:     cid2,
			Module:         blobModule(blob),
			ExtraPreState:  preState,
			WithdrawalCaps: capTable{testAsset(): u256FromBig(t, capValue)},
			WantStatus:     status,
		})
	}

	capVector("CV-CAP-8",
		"two withdrawals of 2^255 in one asset capped at 2^256 - 1: each is within the cap and the exact total 2^256 is over it, though a u256 sum wraps to 0: RUNTIME_FAULT at the metered gas from step 5a; with a state write, an event and a deposit: full rollback and the full deposit refunded",
		[]*big.Int{pow2(255), pow2(255)}, maxU256, codec.StatusRuntimeFault)
	capVector("CV-CAP-9",
		"withdrawals of 2^64 + 1 and 2^128 + 1 in one asset capped at 2^129: the exact total is within the cap and step 5a passes",
		[]*big.Int{new(big.Int).Add(pow2(64), big.NewInt(1)), new(big.Int).Add(pow2(128), big.NewInt(1))}, pow2(129), codec.StatusSuccess)
	capVector("CV-CAP-10",
		"withdrawals of 2^128 and 2^128 + 2^64 in one asset capped at 2^128 + 2^64: each is within the cap and the exact total is over it, though the total truncated to 64 or 128 bits is within it: RUNTIME_FAULT at the metered gas; with a state write, an event and a deposit: full rollback and the full deposit refunded",
		[]*big.Int{pow2(128), new(big.Int).Add(pow2(128), pow2(64))}, new(big.Int).Add(pow2(128), pow2(64)), codec.StatusRuntimeFault)
	capVector("CV-CAP-11",
		"withdrawals of 2^192 - 1 and 1 in one asset capped at 2^192 - 1, with a state write, an event and a deposit: the exact total 2^192 carries across the 2^64, 2^128 and 2^192 limb boundaries and is over the cap: RUNTIME_FAULT at the metered gas, full rollback and the full deposit refunded",
		[]*big.Int{new(big.Int).Sub(pow2(192), big.NewInt(1)), big.NewInt(1)}, new(big.Int).Sub(pow2(192), big.NewInt(1)), codec.StatusRuntimeFault)

	{
		idx := nextIndex()
		blob := encodeEffects(t, codec.ContractEffects{
			Version:          codec.ExecutionResultVersion,
			GlobalInputIndex: idx,
			Outbox:           []codec.OutboxMessage{withdrawal(idx, 0, testAsset(), 600), withdrawal(idx, 1, testAsset(), 400)},
		})
		specs = append(specs, vectorSpec{
			ID:             "CV-CAP-12",
			Purpose:        "withdrawals of 600 and 400 in the deposit asset, capped at 1000, with an unclaimed deposit of 1000: the deposit and its refund do not count toward the cap, so step 5a passes and the refund follows the two withdrawals",
			Shape:          "call",
			BatchID:        2,
			Ctx:            stdCtx(idx),
			Args:           codec.CallL2Args{TargetContract: cid2, PlasmaLimit: callPlasmaLimit, DepositAsset: testAsset(), DepositAmount: u256(1000)},
			ContractID:     cid2,
			Module:         blobModule(blob),
			WithdrawalCaps: capsOfOne(),
			WantStatus:     codec.StatusSuccess,
		})
	}

	// --- step 5a footprint boundary: CV-ASSETS-3..5.

	footprintOutbox := func(idx uint64) []codec.OutboxMessage {
		outbox := make([]codec.OutboxMessage, 16)
		for i := range outbox {
			outbox[i] = withdrawal(idx, uint32(i), assetsTestAssetDistinct(byte(i+1)), 1)
		}
		return outbox
	}

	{
		idx := nextIndex()
		blob := encodeEffects(t, codec.ContractEffects{Version: codec.ExecutionResultVersion, GlobalInputIndex: idx, Outbox: footprintOutbox(idx)})
		specs = append(specs, vectorSpec{
			ID:             "CV-ASSETS-3",
			Purpose:        "16 distinct-asset withdrawals, every asset capped above its total, fill the per-input asset footprint exactly and pass step 5a",
			Shape:          "call",
			BatchID:        2,
			Ctx:            stdCtx(idx),
			Args:           codec.CallL2Args{TargetContract: cid2, PlasmaLimit: callPlasmaLimit},
			ContractID:     cid2,
			Module:         blobModule(blob),
			WithdrawalCaps: capsOf(1000, assetsTestAssetRange(1, 16)...),
			WantStatus:     codec.StatusSuccess,
		})
	}

	{
		idx := nextIndex()
		blob := encodeEffects(t, codec.ContractEffects{Version: codec.ExecutionResultVersion, GlobalInputIndex: idx, Outbox: footprintOutbox(idx), ClaimedDeposit: u256(100)})
		specs = append(specs, vectorSpec{
			ID:             "CV-ASSETS-4",
			Purpose:        "16 distinct-asset withdrawals plus a non-zero deposit in one of those assets: the deposit asset is already in the footprint, which stays at 16, and step 5a passes",
			Shape:          "call",
			BatchID:        2,
			Ctx:            stdCtx(idx),
			Args:           codec.CallL2Args{TargetContract: cid2, PlasmaLimit: callPlasmaLimit, DepositAsset: assetsTestAssetDistinct(1), DepositAmount: u256(100)},
			ContractID:     cid2,
			Module:         blobModule(blob),
			WithdrawalCaps: capsOf(1000, assetsTestAssetRange(1, 16)...),
			WantStatus:     codec.StatusSuccess,
		})
	}

	{
		idx := nextIndex()
		outbox := append(footprintOutbox(idx), withdrawal(idx, 16, assetsTestAssetDistinct(17), 0))
		blob := encodeEffects(t, codec.ContractEffects{Version: codec.ExecutionResultVersion, GlobalInputIndex: idx, Outbox: outbox})
		specs = append(specs, vectorSpec{
			ID:             "CV-ASSETS-5",
			Purpose:        "16 capped distinct-asset withdrawals plus a zero-amount withdrawal in a 17th, uncapped asset breach the footprint, since a zero-amount withdrawal still names its asset: RUNTIME_FAULT at the metered gas",
			Shape:          "call",
			BatchID:        2,
			Ctx:            stdCtx(idx),
			Args:           codec.CallL2Args{TargetContract: cid2, PlasmaLimit: callPlasmaLimit},
			ContractID:     cid2,
			Module:         blobModule(blob),
			WithdrawalCaps: capsOf(1000, assetsTestAssetRange(1, 16)...),
			WantStatus:     codec.StatusRuntimeFault,
		})
	}

	// --- validation order (§10.6): CV-ORDER-1..13. Each row trips two conditions, so its status
	// names the one the runtime checks first.

	reservedWrite := codec.LocalStateDiffEntry{LocalKey: append([]byte(nil), keyCodeHash...), Value: []byte{0x01}}
	plainWrite := codec.LocalStateDiffEntry{LocalKey: []byte("x"), Value: []byte{0x01}}
	dupA := codec.LocalStateDiffEntry{LocalKey: []byte("dup"), Value: []byte("a")}
	dupB := codec.LocalStateDiffEntry{LocalKey: []byte("dup"), Value: []byte("b")}
	zeroChargeLimit := module.InitialMemoryCost(1) + costOf(t, 0x42, 0x0B)
	oversizedReturn := bytes.Repeat([]byte{0xAB}, codec.MaxReturnData+1)

	specs = append(specs, orderVector(nextIndex(), "CV-ORDER-1",
		"a wrong input index and a reserved-key write: step 2 precedes step 3, so RUNTIME_FAULT, not UNAUTHORIZED",
		codec.StatusRuntimeFault, callPlasmaLimit, seeded(), nil,
		effectsBlob(t, func(idx uint64, eff *codec.ContractEffects) {
			eff.GlobalInputIndex = idx + 1
			eff.StateDiff = []codec.LocalStateDiffEntry{reservedWrite}
		})))
	specs = append(specs, orderVector(nextIndex(), "CV-ORDER-2",
		"an event attributed to another contract and a reserved-key write: step 2's attribution check precedes step 3, so RUNTIME_FAULT, not UNAUTHORIZED",
		codec.StatusRuntimeFault, callPlasmaLimit, seeded(), nil,
		effectsBlob(t, func(idx uint64, eff *codec.ContractEffects) {
			eff.Events = []codec.Event{{ContractID: repeatedHash(0x44), InputIndex: idx, EventIndex: 0, Data: []byte("evt")}}
			eff.StateDiff = []codec.LocalStateDiffEntry{reservedWrite}
		})))
	specs = append(specs, orderVector(nextIndex(), "CV-ORDER-3",
		"a duplicated local key, then a reserved key: the step 3 scan runs in blob order and meets the duplicate first, so RUNTIME_FAULT, not UNAUTHORIZED",
		codec.StatusRuntimeFault, callPlasmaLimit, seeded(), nil,
		effectsBlob(t, func(_ uint64, eff *codec.ContractEffects) {
			eff.StateDiff = []codec.LocalStateDiffEntry{dupA, dupB, reservedWrite}
		})))
	specs = append(specs, orderVector(nextIndex(), "CV-ORDER-4",
		"a reserved key, then a duplicated local key: the step 3 scan runs in blob order and meets the reserved key first, so UNAUTHORIZED, not RUNTIME_FAULT",
		codec.StatusUnauthorized, callPlasmaLimit, seeded(), nil,
		effectsBlob(t, func(_ uint64, eff *codec.ContractEffects) {
			eff.StateDiff = []codec.LocalStateDiffEntry{reservedWrite, dupA, dupB}
		})))
	specs = append(specs, orderVector(nextIndex(), "CV-ORDER-5",
		"a reserved-key write followed by one trailing byte: step 1 decodes the whole blob before step 3 scans it, so RUNTIME_FAULT, not UNAUTHORIZED",
		codec.StatusRuntimeFault, callPlasmaLimit, seeded(), nil,
		func(idx uint64) []byte {
			blob := encodeEffects(t, codec.ContractEffects{
				Version:          codec.ExecutionResultVersion,
				GlobalInputIndex: idx,
				StateDiff:        []codec.LocalStateDiffEntry{reservedWrite},
			})
			return append(blob, 0x00)
		}))
	specs = append(specs, orderVector(nextIndex(), "CV-ORDER-6",
		"a reserved-key write and a non-zero claim with no deposit: step 3 precedes step 4, so UNAUTHORIZED, not RUNTIME_FAULT",
		codec.StatusUnauthorized, callPlasmaLimit, seeded(), nil,
		effectsBlob(t, func(_ uint64, eff *codec.ContractEffects) {
			eff.StateDiff = []codec.LocalStateDiffEntry{reservedWrite}
			eff.ClaimedDeposit = u256(1)
		})))
	specs = append(specs, orderVector(nextIndex(), "CV-ORDER-7",
		"a reserved-key write and a withdrawal over its cap: step 3 precedes step 5a, so UNAUTHORIZED, not RUNTIME_FAULT",
		codec.StatusUnauthorized, callPlasmaLimit, seeded(), capsOfOne(),
		effectsBlob(t, func(idx uint64, eff *codec.ContractEffects) {
			eff.StateDiff = []codec.LocalStateDiffEntry{reservedWrite}
			eff.Outbox = []codec.OutboxMessage{withdrawal(idx, 0, testAsset(), 1001)}
		})))
	specs = append(specs, orderVector(nextIndex(), "CV-ORDER-8",
		"a wrong input index at a plasma_limit that leaves nothing for the effect charge: step 2 precedes step 6, so RUNTIME_FAULT, not OUT_OF_GAS",
		codec.StatusRuntimeFault, zeroChargeLimit, seeded(), nil,
		effectsBlob(t, func(idx uint64, eff *codec.ContractEffects) {
			eff.GlobalInputIndex = idx + 1
			eff.StateDiff = []codec.LocalStateDiffEntry{plainWrite}
		})))
	specs = append(specs, orderVector(nextIndex(), "CV-ORDER-9",
		"a reserved-key write at a plasma_limit that leaves nothing for the effect charge: step 3 precedes step 6, so UNAUTHORIZED, not OUT_OF_GAS",
		codec.StatusUnauthorized, zeroChargeLimit, seeded(), nil,
		effectsBlob(t, func(_ uint64, eff *codec.ContractEffects) {
			eff.StateDiff = []codec.LocalStateDiffEntry{reservedWrite}
		})))
	specs = append(specs, orderVector(nextIndex(), "CV-ORDER-10",
		"a non-zero claim with no deposit at a plasma_limit that leaves nothing for the effect charge: step 4 precedes step 6, so RUNTIME_FAULT, not OUT_OF_GAS",
		codec.StatusRuntimeFault, zeroChargeLimit, seeded(), nil,
		effectsBlob(t, func(_ uint64, eff *codec.ContractEffects) {
			eff.StateDiff = []codec.LocalStateDiffEntry{plainWrite}
			eff.ClaimedDeposit = u256(1)
		})))
	specs = append(specs, orderVector(nextIndex(), "CV-ORDER-11",
		"a write one byte over the remaining quota at a plasma_limit that leaves nothing for the effect charge: step 6 precedes step 6a, so OUT_OF_GAS, not STORAGE_QUOTA_EXCEEDED",
		codec.StatusOutOfGas, zeroChargeLimit, seedQuota(cid2, nil, 64), nil,
		effectsBlob(t, func(_ uint64, eff *codec.ContractEffects) {
			eff.StateDiff = []codec.LocalStateDiffEntry{plainWrite}
		})))
	specs = append(specs, orderVector(nextIndex(), "CV-ORDER-12",
		"a write one byte over the remaining quota and ReturnData over MaxReturnData: step 6a precedes step 7, so STORAGE_QUOTA_EXCEEDED at metered plus effect gas, not RESULT_TOO_LARGE",
		codec.StatusStorageQuotaExceeded, callPlasmaLimit, seedQuota(cid2, nil, 64), nil,
		effectsBlob(t, func(_ uint64, eff *codec.ContractEffects) {
			eff.StateDiff = []codec.LocalStateDiffEntry{plainWrite}
			eff.ReturnData = oversizedReturn
		})))
	specs = append(specs, orderVector(nextIndex(), "CV-ORDER-13",
		"ReturnData over MaxReturnData at a plasma_limit that leaves nothing for the effect charge: step 6 precedes step 7, so OUT_OF_GAS, not RESULT_TOO_LARGE",
		codec.StatusOutOfGas, zeroChargeLimit, nil, nil,
		effectsBlob(t, func(_ uint64, eff *codec.ContractEffects) {
			eff.ReturnData = oversizedReturn
		})))
	specs = append(specs, orderVector(nextIndex(), "CV-ORDER-14",
		"an outbox message whose source_contract is another contract, and a reserved-key write: step 2's outbox attribution check precedes step 3, so RUNTIME_FAULT, not UNAUTHORIZED",
		codec.StatusRuntimeFault, callPlasmaLimit, seeded(), capsOfOne(),
		effectsBlob(t, func(idx uint64, eff *codec.ContractEffects) {
			foreign := withdrawal(idx, 0, testAsset(), 1)
			foreign.SourceContract = repeatedHash(0x44)
			eff.Outbox = []codec.OutboxMessage{foreign}
			eff.StateDiff = []codec.LocalStateDiffEntry{reservedWrite}
		})))
	footprintBreach := func(idx uint64) []codec.OutboxMessage {
		return append(footprintOutbox(idx), withdrawal(idx, 16, assetsTestAssetDistinct(17), 1))
	}
	specs = append(specs, orderVector(nextIndex(), "CV-ORDER-15",
		"withdrawals naming 17 distinct capped assets at a plasma_limit that leaves nothing for the effect charge: the step 5a footprint check precedes step 6, so RUNTIME_FAULT, not OUT_OF_GAS",
		codec.StatusRuntimeFault, zeroChargeLimit, nil, capsOf(1000, assetsTestAssetRange(1, 17)...),
		effectsBlob(t, func(idx uint64, eff *codec.ContractEffects) {
			eff.Outbox = footprintBreach(idx)
		})))
	specs = append(specs, orderVector(nextIndex(), "CV-ORDER-16",
		"withdrawals naming 17 distinct capped assets and a reserved-key write: step 3 precedes the step 5a footprint check, so UNAUTHORIZED, not RUNTIME_FAULT",
		codec.StatusUnauthorized, callPlasmaLimit, seeded(), capsOf(1000, assetsTestAssetRange(1, 17)...),
		effectsBlob(t, func(idx uint64, eff *codec.ContractEffects) {
			eff.Outbox = footprintBreach(idx)
			eff.StateDiff = []codec.LocalStateDiffEntry{reservedWrite}
		})))

	specs = append(specs, blobVectors(t, nextIndex)...)

	// --- state, reserved-key, quota and deposit rows.

	specs = append(specs, orderVector(nextIndex(), "CV-STATE-1",
		"deleting a key absent from the pre-state: a well-formed no-op that commits its EMPTY entry, leaves the root unchanged and reads no quota leaf",
		codec.StatusSuccess, callPlasmaLimit, nil, nil,
		effectsBlob(t, func(_ uint64, eff *codec.ContractEffects) {
			eff.StateDiff = []codec.LocalStateDiffEntry{{LocalKey: []byte("gone"), Value: nil}}
		})))
	specs = append(specs, orderVector(nextIndex(), "CV-STATE-2",
		"an EMPTY deletion sentinel followed by stray value bytes: EMPTY carries no value bytes, so RUNTIME_FAULT at the metered gas",
		codec.StatusRuntimeFault, callPlasmaLimit, nil, nil,
		func(idx uint64) []byte {
			eff := codec.ContractEffects{Version: codec.ExecutionResultVersion, GlobalInputIndex: idx, StateDiff: []codec.LocalStateDiffEntry{{LocalKey: []byte("gone"), Value: nil}}}
			return corruptBlob(t, eff, codec.ErrOverflow, func(b []byte) []byte {
				return append(append(append([]byte{}, b[:26]...), 0x00, 0x00, 0x00, 0x01, 0xAB), b[26:]...)
			})
		}))
	specs = append(specs, orderVector(nextIndex(), "CV-STATE-3",
		"rewriting a key with its current value: the entry is committed, the root is unchanged, and the net quota change is 0, so no quota entry is committed",
		codec.StatusSuccess, callPlasmaLimit,
		seedQuota(cid2, map[domain.Path][]byte{derive(cid2, []byte("same")): {0x07}}, vectorQuota), nil,
		effectsBlob(t, func(_ uint64, eff *codec.ContractEffects) {
			eff.StateDiff = []codec.LocalStateDiffEntry{{LocalKey: []byte("same"), Value: []byte{0x07}}}
		})))
	specs = append(specs, orderVector(nextIndex(), "CV-RESERVED-1",
		"writes to 0x00, 0x0000, 0x0006 and 0x000100, each near a reserved key but none equal to one: reserved keys match exactly, never by prefix or range, so SUCCESS",
		codec.StatusSuccess, callPlasmaLimit, seeded(), nil,
		effectsBlob(t, func(_ uint64, eff *codec.ContractEffects) {
			eff.StateDiff = []codec.LocalStateDiffEntry{
				{LocalKey: []byte{0x00}, Value: []byte{0x01}},
				{LocalKey: []byte{0x00, 0x00}, Value: []byte{0x01}},
				{LocalKey: []byte{0x00, 0x06}, Value: []byte{0x01}},
				{LocalKey: []byte{0x00, 0x01, 0x00}, Value: []byte{0x01}},
			}
		})))
	specs = append(specs, orderVector(nextIndex(), "CV-QUOTA-9",
		"a write by a contract with no quota leaf: an absent leaf means purchased = used = 0, so STORAGE_QUOTA_EXCEEDED at metered plus effect gas",
		codec.StatusStorageQuotaExceeded, callPlasmaLimit, nil, nil,
		effectsBlob(t, func(_ uint64, eff *codec.ContractEffects) {
			eff.StateDiff = []codec.LocalStateDiffEntry{{LocalKey: []byte("q"), Value: []byte{0x01}}}
		})))

	{
		idx := nextIndex()
		blob := encodeEffects(t, codec.ContractEffects{
			Version:          codec.ExecutionResultVersion,
			GlobalInputIndex: idx,
			Outbox:           []codec.OutboxMessage{{SourceContract: cid2, InputIndex: idx, OutboxIndex: 0, Target: codec.L2Delivery{TargetContract: repeatedHash(0x55)}}},
			ClaimedDeposit:   u256(400),
		})
		specs = append(specs, vectorSpec{
			ID:             "EXEC-DEP-6",
			Purpose:        "a payable CallL2 with a payload and a deposit of 1000 claims 400: SUCCESS, and the 600 refund follows the contract's message",
			Shape:          "call",
			BatchID:        2,
			Ctx:            stdCtx(idx),
			Args:           codec.CallL2Args{TargetContract: cid2, PlasmaLimit: callPlasmaLimit, DepositAsset: testAsset(), DepositAmount: u256(1000), Payload: []byte{0xCA, 0xFE}},
			ContractID:     cid2,
			Module:         blobModule(blob),
			WithdrawalCaps: capsOfOne(),
			WantStatus:     codec.StatusSuccess,
		})
	}

	{
		idx := nextIndex()
		body := abortCallBody(7, 0, 0)
		specs = append(specs, vectorSpec{
			ID:             "EXEC-DEP-7",
			Purpose:        "abort(7) with data_len 0 on an input carrying a deposit: REVERT, and the full deposit is refunded",
			Shape:          "call",
			BatchID:        2,
			Ctx:            stdCtx(idx),
			Args:           codec.DepositArgs{TargetContract: cid2, PlasmaLimit: callPlasmaLimit, DepositAsset: testAsset(), DepositAmount: u256(1000)},
			ContractID:     cid2,
			Module:         buildWasmModule(runModuleSpec([]string{"abort"}, 1, []byte{0x00}, body, nil)),
			WithdrawalCaps: capsOfOne(),
			WantStatus:     codec.StatusRevert,
		})
	}

	{
		idx := nextIndex()
		blob := encodeEffects(t, codec.ContractEffects{Version: codec.ExecutionResultVersion, GlobalInputIndex: idx})
		specs = append(specs, vectorSpec{
			ID:             "EXEC-DEP-8",
			Purpose:        "the pre-execution initial-memory charge is unaffordable on an input carrying a deposit: OUT_OF_GAS, and the full deposit is refunded",
			Shape:          "call",
			BatchID:        2,
			Ctx:            stdCtx(idx),
			Args:           codec.DepositArgs{TargetContract: cid2, PlasmaLimit: module.InitialMemoryCost(1) - 1, DepositAsset: testAsset(), DepositAmount: u256(1000)},
			ContractID:     cid2,
			Module:         blobModule(blob),
			WithdrawalCaps: capsOfOne(),
			WantStatus:     codec.StatusOutOfGas,
		})
	}

	{
		idx := nextIndex()
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
			ID:             "EXEC-DEP-9",
			Purpose:        "a reserved-key write on an input carrying a deposit: UNAUTHORIZED, and the full deposit is refunded",
			Shape:          "call",
			BatchID:        2,
			Ctx:            stdCtx(idx),
			Args:           codec.DepositArgs{TargetContract: cid2, PlasmaLimit: callPlasmaLimit, DepositAsset: testAsset(), DepositAmount: u256(1000)},
			ContractID:     cid2,
			Module:         blobModule(blob),
			ExtraPreState:  seeded(),
			WithdrawalCaps: capsOfOne(),
			WantStatus:     codec.StatusUnauthorized,
		})
	}

	{
		idx := nextIndex()
		specs = append(specs, vectorSpec{
			ID:             "EXEC-DEP-10",
			Purpose:        "a blob longer than MaxResultBlob on an input carrying a deposit: RESULT_TOO_LARGE from step 0, and the full deposit is refunded",
			Shape:          "call",
			BatchID:        2,
			Ctx:            stdCtx(idx),
			Args:           codec.DepositArgs{TargetContract: cid2, PlasmaLimit: callPlasmaLimit, DepositAsset: testAsset(), DepositAmount: u256(1000)},
			ContractID:     cid2,
			Module:         buildWasmModule(runModuleSpec(nil, 2, []byte{0x00}, returnBlobBody(blobOffset, MaxResultBlob+1), nil)),
			WithdrawalCaps: capsOfOne(),
			WantStatus:     codec.StatusResultTooLarge,
		})
	}

	specs = append(specs, blobOrderVectors(t, nextIndex)...)
	specs = append(specs, abortVectors(t, nextIndex)...)

	return specs
}

// abortCallBody returns i32.const code; i32.const ptr; i32.const dataLen; call 0 (abort);
// i64.const 0; end.
func abortCallBody(code, ptr, dataLen int32) []byte {
	b := []byte{0x41}
	b = appendVarS64(b, int64(code))
	b = append(b, 0x41)
	b = appendVarS64(b, int64(ptr))
	b = append(b, 0x41)
	b = appendVarS64(b, int64(dataLen))
	return append(b, 0x10, 0x00, 0x42, 0x00, 0x0B)
}

// revertPattern returns n bytes with b[i] = byte(i).
func revertPattern(n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(i)
	}
	return b
}

// abortVectors returns the env.abort revert-data vectors, drawing their indices from nextIndex.
func abortVectors(t *testing.T, nextIndex func() uint64) []vectorSpec {
	t.Helper()

	abortModule := func(body []byte, data []dataSeg) []byte {
		return buildWasmModule(runModuleSpec([]string{"abort"}, 1, []byte{0x00}, body, data))
	}
	atBlob := func(b []byte) []dataSeg { return []dataSeg{{offset: blobOffset, bytes: b}} }
	callArgs := codec.CallL2Args{TargetContract: cid2, PlasmaLimit: callPlasmaLimit}
	spec := func(id, purpose string, mod []byte, args codec.InputArgs, caps capTable, want codec.Status) vectorSpec {
		return vectorSpec{
			ID:             id,
			Purpose:        purpose,
			Shape:          "call",
			BatchID:        2,
			Ctx:            stdCtx(nextIndex()),
			Args:           args,
			ContractID:     cid2,
			Module:         mod,
			WithdrawalCaps: caps,
			WantStatus:     want,
		}
	}

	exhausted := callArgs
	exhausted.PlasmaLimit = module.InitialMemoryCost(1) + costOf(t, 0x41, 0x41, 0x41, 0x10, 0x42, 0x0B) + module.HostCost(module.HostAbort, 64) - 1
	payable := callArgs
	payable.DepositAsset = testAsset()
	payable.DepositAmount = u256(1000)
	payable.Payload = []byte{0xCA, 0xFE}

	revert40 := abortModule(abortCallBody(7, blobOffset, 40), atBlob(revertPattern(40)))
	return []vectorSpec{
		spec("CV-ABORT-1", "abort(7) with 40 bytes of revert data: REVERT, abort_code 7, return_hash = SHA3-256 of the revert data",
			revert40, callArgs, nil, codec.StatusRevert),
		spec("CV-ABORT-2", "abort with data_len 0 and data_ptr 0xFFFFFFFF: REVERT with empty revert data and a zero return_hash; the pointer is never read",
			abortModule(abortCallBody(7, -1, 0), nil), callArgs, nil, codec.StatusRevert),
		spec("CV-ABORT-3", "data_len = MaxReturnData is accepted: REVERT carrying all of it",
			abortModule(abortCallBody(7, blobOffset, codec.MaxReturnData), atBlob(revertPattern(codec.MaxReturnData))), callArgs, nil, codec.StatusRevert),
		spec("CV-ABORT-4", "data_len = MaxReturnData + 1 over a range outside memory: RESULT_TOO_LARGE, not RUNTIME_FAULT, so memory is not read",
			abortModule(abortCallBody(7, -16, codec.MaxReturnData+1), nil), callArgs, nil, codec.StatusResultTooLarge),
		spec("CV-ABORT-5", "a revert-data range crossing the end of memory traps: RUNTIME_FAULT at the metered gas, including abort's charge",
			abortModule(abortCallBody(7, 65520, 32), nil), callArgs, nil, codec.StatusRuntimeFault),
		spec("CV-ABORT-6", "abort's data charge is unaffordable though its base is not: OUT_OF_GAS",
			abortModule(abortCallBody(7, blobOffset, 64), atBlob(revertPattern(64))), exhausted, nil, codec.StatusOutOfGas),
		spec("EXEC-DEP-11", "a payable CallL2 that aborts with revert data: REVERT, and the full deposit refund sits beside the revert data",
			revert40, payable, capsOfOne(), codec.StatusRevert),
	}
}

// assetsTestAssetDistinct returns a valid AssetID distinct from testAsset() for every b in
// [1,17]: only the trailing byte varies, and testAsset() sets bytes within
// [32-ZenonTokenStandardSize:32) to a fixed non-trailing-byte-1..17 pattern.
func assetsTestAssetDistinct(b byte) codec.AssetID {
	var a codec.AssetID
	a[31] = b
	return a
}

// assetsTestAssetRange returns assetsTestAssetDistinct(b) for b from lo to hi inclusive.
func assetsTestAssetRange(lo, hi byte) []codec.AssetID {
	var out []codec.AssetID
	for b := lo; b <= hi; b++ {
		out = append(out, assetsTestAssetDistinct(b))
	}
	return out
}

// capsOf returns a capTable giving every asset in assets the cap capValue.
func capsOf(capValue uint64, assets ...codec.AssetID) capTable {
	out := make(capTable, len(assets))
	for _, a := range assets {
		out[a] = u256(capValue)
	}
	return out
}

// unsupportedVector returns EXEC-UNSUP-1's vectorSpec: input_kind 8 is outside the Phase-1 set,
// so DecodeInput returns Input{Unsupported: true} and Apply commits UNSUPPORTED_OPERATION (§15).
func unsupportedVector() vectorSpec {
	return vectorSpec{
		ID:      "EXEC-UNSUP-1",
		Purpose: "an unrecognized input_kind is refused deterministically as UNSUPPORTED_OPERATION",
		Shape:   "unsupported_kind",
		BatchID: 3,
		Ctx: domain.InputContext{
			GlobalInputIndex: 3000,
			MomentumHeight:   1000,
			MomentumTime:     1700000000,
			L1Sender:         repeatedAddr(0x01),
		},
		WantStatus: codec.StatusUnsupportedOperation,
	}
}
