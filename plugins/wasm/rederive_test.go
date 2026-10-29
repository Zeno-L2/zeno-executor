package wasm

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"math/big"
	"os"
	"reflect"
	"slices"
	"sort"
	"strings"
	"testing"

	"github.com/zenon-network/go-zenon/common/crypto"
	"github.com/zenon-network/go-zenon/common/trie"
	"github.com/zenon-network/go-zenon/common/types"

	"github.com/Zeno-L2/zeno-executor/internal/codec"
	"github.com/Zeno-L2/zeno-executor/plugins/wasm/internal/module"
)

// mustHex decodes s or fails the test.
func mustHex(t *testing.T, s string) []byte {
	t.Helper()
	b, err := hex.DecodeString(s)
	if err != nil {
		t.Fatalf("bad hex %q: %v", s, err)
	}
	return b
}

// mustHash decodes a 32-byte hex string into a types.Hash.
func mustHash(t *testing.T, s string) types.Hash {
	t.Helper()
	var h types.Hash
	b := mustHex(t, s)
	if len(b) != len(h) {
		t.Fatalf("hash %q has length %d, want %d", s, len(b), len(h))
	}
	copy(h[:], b)
	return h
}

// mustPath decodes a 32-byte hex string into a trie.Path.
func mustPath(t *testing.T, s string) trie.Path {
	return trie.Path(mustHash(t, s))
}

// leavesFromPreState parses a pre_state object (derived-path hex -> value hex, "" == present-empty)
// into parallel path/value slices.
func leavesFromPreState(t *testing.T, m map[string]string) ([]trie.Path, [][]byte) {
	t.Helper()
	paths := make([]trie.Path, 0, len(m))
	values := make([][]byte, 0, len(m))
	for pathHex, valueHex := range m {
		paths = append(paths, mustPath(t, pathHex))
		if valueHex == "" {
			values = append(values, []byte{})
		} else {
			values = append(values, mustHex(t, valueHex))
		}
	}
	return paths, values
}

// replayStateDiff applies diff onto the leaf set given by paths/values
// (nil deletes, any other value sets), returning the resulting parallel
// slices.
func replayStateDiff(paths []trie.Path, values [][]byte, diff []codec.StateDiffEntry) ([]trie.Path, [][]byte) {
	leaves := make(map[trie.Path][]byte, len(paths)+len(diff))
	for i, p := range paths {
		leaves[p] = values[i]
	}
	for _, e := range diff {
		if e.Value == nil {
			delete(leaves, e.Key)
			continue
		}
		leaves[e.Key] = e.Value
	}
	outPaths := make([]trie.Path, 0, len(leaves))
	outValues := make([][]byte, 0, len(leaves))
	for p, v := range leaves {
		outPaths = append(outPaths, p)
		outValues = append(outValues, v)
	}
	return outPaths, outValues
}

// inlineMerkleRoot recomputes the Merkle construction inline: leaf
// sha3(0x00 || element), node sha3(0x01 || left || right), a lone
// trailing node promoted unchanged, bytes32(0) for the empty set. It
// must not call codec.MerkleRoot.
func inlineMerkleRoot(elements [][]byte) types.Hash {
	if len(elements) == 0 {
		return types.Hash{}
	}
	level := make([]types.Hash, len(elements))
	for i, e := range elements {
		level[i] = hashOf(crypto.Hash(append([]byte{0x00}, e...)))
	}
	for len(level) > 1 {
		next := make([]types.Hash, 0, (len(level)+1)/2)
		i := 0
		for ; i+1 < len(level); i += 2 {
			left, right := level[i], level[i+1]
			next = append(next, hashOf(crypto.Hash([]byte{0x01}, left[:], right[:])))
		}
		if i < len(level) {
			next = append(next, level[i])
		}
		level = next
	}
	return level[0]
}

// statusOrdinal maps a status name back to its ordinal byte, by
// scanning codec.Status's own String() rendering.
func statusOrdinal(t *testing.T, name string) byte {
	t.Helper()
	for s := codec.StatusSuccess; s <= codec.StatusStorageQuotaExceeded; s++ {
		if s.String() == name {
			return byte(s)
		}
	}
	t.Fatalf("unknown status name %q", name)
	return 0
}

var zeroHashHex = hex.EncodeToString(make([]byte, 32))
var zeroAddrHex = hex.EncodeToString(make([]byte, 20))

// --- A. file-level checks (once).

func checkFileLevel(t *testing.T, file vectorFile) {
	t.Helper()

	// A1: chain_id, computed inline from the chain_id formula, with no
	// call to New, p.chainID or hashOf-via-plugin.
	wantChainID := hashOf(crypto.Hash([]byte("ZENON_WASM_L2:"), []byte(file.NetworkName)))
	if hex.EncodeToString(wantChainID[:]) != file.ChainID {
		t.Fatalf("A1: chain_id = %s, want inline sha3(\"ZENON_WASM_L2:\" || network_name) %x", file.ChainID, wantChainID)
	}

	// A2: header versions.
	if file.ExecutionResultVersion != codec.ExecutionResultVersion {
		t.Fatalf("A2: executionresult_version = %d, want %d", file.ExecutionResultVersion, codec.ExecutionResultVersion)
	}
	if file.ReceiptVersion != codec.ReceiptVersion {
		t.Fatalf("A2: receipt_version = %d, want %d", file.ReceiptVersion, codec.ReceiptVersion)
	}
	if file.ContextVersion != codec.RuntimeProfileVersion {
		t.Fatalf("A2: context_version = %d, want %d", file.ContextVersion, codec.RuntimeProfileVersion)
	}
	if file.MeteringVersion != codec.MeteringVersion {
		t.Fatalf("A2: metering_version = %d, want %d", file.MeteringVersion, codec.MeteringVersion)
	}
	if file.Artifact != "execution-conformance-v1" {
		t.Fatalf("A2: artifact = %q, want %q", file.Artifact, "execution-conformance-v1")
	}
	if file.Hash != "SHA3-256 (FIPS-202)" {
		t.Fatalf("A2: hash = %q, want %q", file.Hash, "SHA3-256 (FIPS-202)")
	}
}

// --- B. per-vector checks, every shape.

func checkPerVector(t *testing.T, vec vectorJSON) {
	t.Helper()

	// B0: the vector's momentum hash is non-zero.
	if mustHash(t, vec.MomentumHash) == (types.Hash{}) {
		t.Fatalf("B0: momentum_hash is all zero")
	}

	// B1: canonical input.
	canonical := mustHex(t, vec.CanonicalInputHex)
	gotInputHash := hex.EncodeToString(crypto.Hash(canonical))
	if gotInputHash != vec.Receipt.InputHash {
		t.Fatalf("B1: sha3(canonical_input_hex) = %s, want receipt.input_hash %s", gotInputHash, vec.Receipt.InputHash)
	}
	if vec.InputKind <= 7 {
		decodedInput, err := codec.DecodeInput(canonical)
		if err != nil {
			t.Fatalf("B1: DecodeInput: %v", err)
		}
		reEncoded, err := decodedInput.Encode()
		if err != nil {
			t.Fatalf("B1: Encode: %v", err)
		}
		if !bytes.Equal(reEncoded, canonical) {
			t.Fatalf("B1: DecodeInput().Encode() does not reproduce canonical_input_hex:\n got  %x\n want %x", reEncoded, canonical)
		}
		if vec.Input != nil {
			got := inputView(decodedInput.Args)
			if !reflect.DeepEqual(got, vec.Input) {
				t.Fatalf("B1: inputView(decoded.Args) = %+v, want %+v", got, vec.Input)
			}
		}
	}

	// B2: pre-state.
	prePaths, preValues := leavesFromPreState(t, vec.PreState)
	preRoot, err := trie.RootOfLeaves(prePaths, preValues)
	if err != nil {
		t.Fatalf("B2: RootOfLeaves: %v", err)
	}
	if hex.EncodeToString(preRoot[:]) != vec.PreStateRoot {
		t.Fatalf("B2: RootOfLeaves(pre_state) = %x, want pre_state_root %s", preRoot, vec.PreStateRoot)
	}

	// B3: receipt bytes.
	checkReceiptBytes(t, vec)

	// B4: when execution_result_blob_hex is present.
	if vec.ExecutionResultBlobHex != "" {
		checkExecutionResultBlob(t, vec, prePaths, preValues)
	}

	// B5: non-SUCCESS vectors.
	if vec.Status != "SUCCESS" {
		checkNonSuccess(t, vec)
	}
}

func checkReceiptBytes(t *testing.T, vec vectorJSON) {
	t.Helper()
	rec := vec.Receipt

	var buf []byte
	buf = binary.BigEndian.AppendUint16(buf, rec.Version)
	buf = binary.BigEndian.AppendUint64(buf, rec.InputIndex)
	inputHash := mustHash(t, rec.InputHash)
	buf = append(buf, inputHash[:]...)
	buf = binary.BigEndian.AppendUint64(buf, rec.BatchID)
	contractID := mustHash(t, rec.ContractID)
	buf = append(buf, contractID[:]...)
	codeHash := mustHash(t, rec.CodeHash)
	buf = append(buf, codeHash[:]...)
	buf = append(buf, statusOrdinal(t, rec.Status))
	buf = binary.BigEndian.AppendUint64(buf, rec.GasUsed)
	buf = binary.BigEndian.AppendUint32(buf, uint32(rec.AbortCode))
	stateDiffHash := mustHash(t, rec.StateDiffHash)
	buf = append(buf, stateDiffHash[:]...)
	returnHash := mustHash(t, rec.ReturnHash)
	buf = append(buf, returnHash[:]...)
	eventRoot := mustHash(t, rec.EventRoot)
	buf = append(buf, eventRoot[:]...)
	buf = binary.BigEndian.AppendUint32(buf, rec.EventCount)
	outboxRoot := mustHash(t, rec.OutboxRoot)
	buf = append(buf, outboxRoot[:]...)
	buf = binary.BigEndian.AppendUint32(buf, rec.OutboxCount)

	receiptHex := mustHex(t, vec.ReceiptHex)
	if !bytes.Equal(buf, receiptHex) {
		t.Fatalf("B3: inline receipt rebuild does not match receipt_hex:\n got  %x\n want %x", buf, receiptHex)
	}

	decoded, err := codec.DecodeReceipt(receiptHex)
	if err != nil {
		t.Fatalf("B3: DecodeReceipt: %v", err)
	}
	if decoded.Version != rec.Version {
		t.Fatalf("B3: decoded.Version = %d, want %d", decoded.Version, rec.Version)
	}
	if hex.EncodeToString(decoded.InputHash[:]) != rec.InputHash {
		t.Fatalf("B3: decoded.InputHash = %x, want %s", decoded.InputHash, rec.InputHash)
	}
	if decoded.BatchID != rec.BatchID {
		t.Fatalf("B3: decoded.BatchID = %d, want %d", decoded.BatchID, rec.BatchID)
	}
	if hex.EncodeToString(decoded.ContractID[:]) != rec.ContractID {
		t.Fatalf("B3: decoded.ContractID = %x, want %s", decoded.ContractID, rec.ContractID)
	}
	if hex.EncodeToString(decoded.CodeHash[:]) != rec.CodeHash {
		t.Fatalf("B3: decoded.CodeHash = %x, want %s", decoded.CodeHash, rec.CodeHash)
	}
	if decoded.Status.String() != rec.Status {
		t.Fatalf("B3: decoded.Status = %s, want %s", decoded.Status.String(), rec.Status)
	}
	if decoded.GasUsed != rec.GasUsed {
		t.Fatalf("B3: decoded.GasUsed = %d, want %d", decoded.GasUsed, rec.GasUsed)
	}
	if decoded.AbortCode != rec.AbortCode {
		t.Fatalf("B3: decoded.AbortCode = %d, want %d", decoded.AbortCode, rec.AbortCode)
	}
	if hex.EncodeToString(decoded.StateDiffHash[:]) != rec.StateDiffHash {
		t.Fatalf("B3: decoded.StateDiffHash = %x, want %s", decoded.StateDiffHash, rec.StateDiffHash)
	}
	if hex.EncodeToString(decoded.ReturnHash[:]) != rec.ReturnHash {
		t.Fatalf("B3: decoded.ReturnHash = %x, want %s", decoded.ReturnHash, rec.ReturnHash)
	}
	if hex.EncodeToString(decoded.EventRoot[:]) != rec.EventRoot {
		t.Fatalf("B3: decoded.EventRoot = %x, want %s", decoded.EventRoot, rec.EventRoot)
	}
	if decoded.EventCount != rec.EventCount {
		t.Fatalf("B3: decoded.EventCount = %d, want %d", decoded.EventCount, rec.EventCount)
	}
	if hex.EncodeToString(decoded.OutboxRoot[:]) != rec.OutboxRoot {
		t.Fatalf("B3: decoded.OutboxRoot = %x, want %s", decoded.OutboxRoot, rec.OutboxRoot)
	}
	if decoded.OutboxCount != rec.OutboxCount {
		t.Fatalf("B3: decoded.OutboxCount = %d, want %d", decoded.OutboxCount, rec.OutboxCount)
	}
	if decoded.Status.String() != vec.Status {
		t.Fatalf("B3: decoded.Status = %s, want top-level status %s", decoded.Status.String(), vec.Status)
	}
	if decoded.GasUsed != vec.GasUsed {
		t.Fatalf("B3: decoded.GasUsed = %d, want top-level gas_used %d", decoded.GasUsed, vec.GasUsed)
	}
	if decoded.InputIndex != vec.GlobalInputIndex {
		t.Fatalf("B3: decoded.InputIndex = %d, want global_input_index %d", decoded.InputIndex, vec.GlobalInputIndex)
	}
	if decoded.BatchID != vec.BatchID {
		t.Fatalf("B3: decoded.BatchID = %d, want top-level batch_id %d", decoded.BatchID, vec.BatchID)
	}
}

func checkExecutionResultBlob(t *testing.T, vec vectorJSON, prePaths []trie.Path, preValues [][]byte) {
	t.Helper()

	blob := mustHex(t, vec.ExecutionResultBlobHex)
	er, err := codec.DecodeExecutionResult(blob)
	if err != nil {
		t.Fatalf("B4: DecodeExecutionResult: %v", err)
	}

	if hex.EncodeToString(er.PreStateRoot[:]) != vec.PreStateRoot {
		t.Fatalf("B4: blob PreStateRoot = %x, want pre_state_root %s", er.PreStateRoot, vec.PreStateRoot)
	}

	postPaths, postValues := replayStateDiff(prePaths, preValues, er.StateDiff)
	postRoot, err := trie.RootOfLeaves(postPaths, postValues)
	if err != nil {
		t.Fatalf("B4: RootOfLeaves(post-state): %v", err)
	}
	if postRoot != er.PostStateRoot {
		t.Fatalf("B4: replayed post-state root = %x, want blob PostStateRoot %x", postRoot, er.PostStateRoot)
	}
	if hex.EncodeToString(postRoot[:]) != vec.PostStateRoot {
		t.Fatalf("B4: replayed post-state root = %x, want post_state_root %s", postRoot, vec.PostStateRoot)
	}

	eventElements := make([][]byte, len(er.Events))
	for i, e := range er.Events {
		b, err := e.Encode()
		if err != nil {
			t.Fatalf("B4: Event.Encode: %v", err)
		}
		eventElements[i] = b
	}
	eventRoot := inlineMerkleRoot(eventElements)
	if hex.EncodeToString(eventRoot[:]) != vec.Receipt.EventRoot {
		t.Fatalf("B4: inline event root = %x, want receipt.event_root %s", eventRoot, vec.Receipt.EventRoot)
	}
	if uint32(len(er.Events)) != vec.Receipt.EventCount {
		t.Fatalf("B4: len(events) = %d, want receipt.event_count %d", len(er.Events), vec.Receipt.EventCount)
	}

	outboxElements := make([][]byte, len(er.Outbox))
	for i, m := range er.Outbox {
		b, err := m.Encode()
		if err != nil {
			t.Fatalf("B4: OutboxMessage.Encode: %v", err)
		}
		outboxElements[i] = b
	}
	outboxRoot := inlineMerkleRoot(outboxElements)
	if hex.EncodeToString(outboxRoot[:]) != vec.Receipt.OutboxRoot {
		t.Fatalf("B4: inline outbox root = %x, want receipt.outbox_root %s", outboxRoot, vec.Receipt.OutboxRoot)
	}
	if uint32(len(er.Outbox)) != vec.Receipt.OutboxCount {
		t.Fatalf("B4: len(outbox) = %d, want receipt.outbox_count %d", len(er.Outbox), vec.Receipt.OutboxCount)
	}

	// state_diff_hash: the region length is computed arithmetically
	// from the decoded entries and sliced out of the encoded
	// ExecutionResult at the fixed prefix offset 74 (version:u16 +
	// global_input_index:u64 + pre_state_root:32 + post_state_root:32).
	const stateDiffRegionOffset = 74
	regionLen := 4
	for _, e := range er.StateDiff {
		valueLen := 0
		if e.Value != nil {
			valueLen = len(e.Value)
		}
		regionLen += 32 + 4 + valueLen
	}
	if stateDiffRegionOffset+regionLen > len(blob) {
		t.Fatalf("B4: computed StateDiff region [%d, %d) exceeds blob length %d", stateDiffRegionOffset, stateDiffRegionOffset+regionLen, len(blob))
	}
	region := blob[stateDiffRegionOffset : stateDiffRegionOffset+regionLen]
	if len(er.StateDiff) == 0 {
		if vec.Receipt.StateDiffHash != zeroHashHex {
			t.Fatalf("B4: receipt.state_diff_hash = %s, want zero for an empty StateDiff", vec.Receipt.StateDiffHash)
		}
	} else {
		wantStateDiffHash := hex.EncodeToString(crypto.Hash(region))
		if vec.Receipt.StateDiffHash != wantStateDiffHash {
			t.Fatalf("B4: receipt.state_diff_hash = %s, want inline sha3(region) %s", vec.Receipt.StateDiffHash, wantStateDiffHash)
		}
	}

	// return_hash.
	if len(er.ReturnData) == 0 {
		if vec.Receipt.ReturnHash != zeroHashHex {
			t.Fatalf("B4: receipt.return_hash = %s, want zero for empty ReturnData", vec.Receipt.ReturnHash)
		}
	} else {
		wantReturnHash := hex.EncodeToString(crypto.Hash(er.ReturnData))
		if vec.Receipt.ReturnHash != wantReturnHash {
			t.Fatalf("B4: receipt.return_hash = %s, want inline sha3(ReturnData) %s", vec.Receipt.ReturnHash, wantReturnHash)
		}
	}

	// witness keys (generic half).
	for i := 1; i < len(vec.WitnessKeys); i++ {
		if vec.WitnessKeys[i] <= vec.WitnessKeys[i-1] {
			t.Fatalf("B4: witness_keys not strictly ascending at index %d: %s <= %s", i, vec.WitnessKeys[i], vec.WitnessKeys[i-1])
		}
	}
	witnessSet := make(map[string]bool, len(vec.WitnessKeys))
	for _, k := range vec.WitnessKeys {
		witnessSet[k] = true
	}
	for _, e := range er.StateDiff {
		k := hex.EncodeToString(e.Key[:])
		if !witnessSet[k] {
			t.Fatalf("B4: derived StateDiff key %s is not in witness_keys", k)
		}
	}
}

// blobDetail describes vec's returned ContractEffects blob for B5's failure messages. A
// non-SUCCESS vector may deliberately carry a blob the codec must refuse; the assertion that it
// is refused, and with which error, belongs to that vector's named invariant, so a decode
// failure here costs this check only its message detail.
func blobDetail(t *testing.T, blobHex string) string {
	t.Helper()
	if blobHex == "" {
		return "no blob was returned"
	}
	eff, err := codec.DecodeContractEffects(mustHex(t, blobHex))
	if err != nil {
		return fmt.Sprintf("the returned blob does not decode (%v)", err)
	}
	return fmt.Sprintf("the returned blob carried %d StateDiff entries and %d bytes of ReturnData",
		len(eff.StateDiff), len(eff.ReturnData))
}

func checkNonSuccess(t *testing.T, vec vectorJSON) {
	t.Helper()

	detail := blobDetail(t, vec.ContractEffectsBlobHex)

	if vec.Receipt.StateDiffHash != zeroHashHex {
		t.Fatalf("B5: status %s carries state_diff_hash %s, want zero; %s", vec.Status, vec.Receipt.StateDiffHash, detail)
	}
	if vec.Status != "REVERT" && vec.Receipt.ReturnHash != zeroHashHex {
		t.Fatalf("B5: status %s carries return_hash %s, want zero; %s", vec.Status, vec.Receipt.ReturnHash, detail)
	}

	if vec.ExecutionResultBlobHex != "" {
		blob := mustHex(t, vec.ExecutionResultBlobHex)
		er, err := codec.DecodeExecutionResult(blob)
		if err != nil {
			t.Fatalf("B5: DecodeExecutionResult: %v", err)
		}
		if len(er.StateDiff) != 0 {
			t.Fatalf("B5: ExecutionResult.StateDiff has %d entries, want 0", len(er.StateDiff))
		}
		if len(er.Events) != 0 {
			t.Fatalf("B5: ExecutionResult.Events has %d entries, want 0", len(er.Events))
		}
		if vec.Status == "REVERT" {
			if len(er.ReturnData) > codec.MaxReturnData {
				t.Fatalf("B5: ExecutionResult.ReturnData has %d bytes, want <= %d", len(er.ReturnData), codec.MaxReturnData)
			}
		} else if len(er.ReturnData) != 0 {
			t.Fatalf("B5: ExecutionResult.ReturnData has %d bytes, want 0", len(er.ReturnData))
		}
		if er.ClaimedDeposit != (codec.U256{}) {
			t.Fatalf("B5: ExecutionResult.ClaimedDeposit = %x, want zero", er.ClaimedDeposit)
		}
	}

	if vec.PreStateRoot != vec.PostStateRoot {
		t.Fatalf("B5: pre_state_root %s != post_state_root %s on a non-SUCCESS status", vec.PreStateRoot, vec.PostStateRoot)
	}
}

// --- C. shape-dispatched checks.

func checkShape(t *testing.T, file vectorFile, vec vectorJSON) {
	t.Helper()

	switch vec.Shape {
	case "call":
		checkCallModule(t, vec)
		checkCallFrame(t, file, vec)
		checkCallReservedWitnesses(t, vec)
		checkCallEffects(t, vec)
		checkCallRefund(t, vec)
		checkCallSuccessRefund(t, vec)
		if vec.Status == "SUCCESS" || vec.Status == "STORAGE_QUOTA_EXCEEDED" {
			checkCallQuota(t, vec)
		}
		if vec.Status == "SUCCESS" {
			checkCallWithdrawalCaps(t, vec)
		}
	case "unsupported_kind":
		checkUnsupportedKind(t, vec)
	case "deploy":
		checkDeploy(t, file, vec)
	case "ownership":
		checkOwnership(t, vec)
	case "buy_storage":
		checkBuyStorage(t, vec)
	case "validation_rejection":
		checkValidationRejection(t, vec)
	case "undeployed_target":
		checkUndeployedTarget(t, vec)
	default:
		t.Fatalf("unhandled shape %q: add its branch with its first vector", vec.Shape)
	}
}

// C1. Module (call).
func checkCallModule(t *testing.T, vec vectorJSON) {
	t.Helper()

	raw := mustHex(t, vec.ModuleWasmHex)
	if err := module.Validate(raw, moduleLimits()); err != nil {
		t.Fatalf("C1: Validate: %v", err)
	}
	codeHash := hex.EncodeToString(crypto.Hash(raw))
	if codeHash != vec.CodeHash {
		t.Fatalf("C1: sha3(module_wasm_hex) = %s, want code_hash %s", codeHash, vec.CodeHash)
	}

	instrumented, err := module.Instrument(raw, moduleLimits())
	if err != nil {
		t.Fatalf("C1: Instrument: %v", err)
	}
	instrumentedHash := hex.EncodeToString(crypto.Hash(instrumented))
	if instrumentedHash != vec.InstrumentedCodeHash {
		t.Fatalf("C1: sha3(instrument(module)) = %s, want instrumented_code_hash %s", instrumentedHash, vec.InstrumentedCodeHash)
	}
	if instrumentedHash != vec.Receipt.CodeHash {
		t.Fatalf("C1: instrumented_code_hash %s != receipt.code_hash %s", instrumentedHash, vec.Receipt.CodeHash)
	}
}

// C2. Input frame (call).
func checkCallFrame(t *testing.T, file vectorFile, vec vectorJSON) {
	t.Helper()

	if vec.Input == nil {
		t.Fatalf("C2: input is absent for a call-shape vector")
	}

	frameBytes := mustHex(t, vec.InputFrameHex)
	frame, err := codec.DecodeExecutionInputFrame(frameBytes)
	if err != nil {
		t.Fatalf("C2: DecodeExecutionInputFrame: %v", err)
	}

	if frame.ContextVersion != file.ContextVersion {
		t.Fatalf("C2: ContextVersion = %d, want context_version %d", frame.ContextVersion, file.ContextVersion)
	}
	if hex.EncodeToString(frame.ChainID[:]) != file.ChainID {
		t.Fatalf("C2: ChainID = %x, want chain_id %s", frame.ChainID, file.ChainID)
	}

	l1Sender := mustHex(t, vec.L1Sender)
	wantCaller := hashOf(crypto.Hash([]byte("L1_CALLER:"), l1Sender))
	if frame.Caller != wantCaller {
		t.Fatalf("C2: Caller = %x, want inline sha3(\"L1_CALLER:\" || l1_sender) %x", frame.Caller, wantCaller)
	}

	gotContractID := hex.EncodeToString(frame.ContractID[:])
	if gotContractID != vec.Receipt.ContractID {
		t.Fatalf("C2: ContractID = %s, want receipt.contract_id %s", gotContractID, vec.Receipt.ContractID)
	}
	if gotContractID != vec.Input.TargetContract {
		t.Fatalf("C2: ContractID = %s, want input view target_contract %s", gotContractID, vec.Input.TargetContract)
	}
	if hex.EncodeToString(frame.CodeHash[:]) != vec.InstrumentedCodeHash {
		t.Fatalf("C2: CodeHash = %x, want instrumented_code_hash %s", frame.CodeHash, vec.InstrumentedCodeHash)
	}
	if frame.MomentumHeight != vec.MomentumHeight {
		t.Fatalf("C2: MomentumHeight = %d, want %d", frame.MomentumHeight, vec.MomentumHeight)
	}
	if frame.MomentumTime != vec.MomentumTime {
		t.Fatalf("C2: MomentumTime = %d, want %d", frame.MomentumTime, vec.MomentumTime)
	}
	if hex.EncodeToString(frame.MomentumHash[:]) != vec.MomentumHash {
		t.Fatalf("C2: MomentumHash = %x, want %s", frame.MomentumHash, vec.MomentumHash)
	}
	if frame.InputIndex != vec.GlobalInputIndex {
		t.Fatalf("C2: InputIndex = %d, want global_input_index %d", frame.InputIndex, vec.GlobalInputIndex)
	}
	if hex.EncodeToString(frame.DepositAsset[:]) != vec.Input.DepositAsset {
		t.Fatalf("C2: DepositAsset = %x, want input view deposit_asset %s", frame.DepositAsset, vec.Input.DepositAsset)
	}
	if hex.EncodeToString(frame.DepositAmount[:]) != vec.Input.DepositAmount {
		t.Fatalf("C2: DepositAmount = %x, want input view deposit_amount %s", frame.DepositAmount, vec.Input.DepositAmount)
	}

	switch vec.InputKind {
	case 0:
		wantPayload := ""
		if vec.Input.PayloadHex != nil {
			wantPayload = *vec.Input.PayloadHex
		}
		if hex.EncodeToString(frame.Payload) != wantPayload {
			t.Fatalf("C2: Payload = %x, want input view payload_hex %s", frame.Payload, wantPayload)
		}
	case 1:
		if len(frame.Payload) != 0 {
			t.Fatalf("C2: Payload = %x, want empty for a Deposit (kind 1)", frame.Payload)
		}
	}
}

// C3. Reserved witness paths (call).
func checkCallReservedWitnesses(t *testing.T, vec vectorJSON) {
	t.Helper()

	contractID := mustHex(t, vec.Receipt.ContractID)
	key1 := hex.EncodeToString(crypto.Hash(contractID, []byte{0x00, 0x01}))
	key2 := hex.EncodeToString(crypto.Hash(contractID, []byte{0x00, 0x02}))

	set := make(map[string]bool, len(vec.WitnessKeys))
	for _, k := range vec.WitnessKeys {
		set[k] = true
	}
	if !set[key1] {
		t.Fatalf("C3: witness_keys is missing sha3(contract_id || 0x0001) = %s", key1)
	}
	if !set[key2] {
		t.Fatalf("C3: witness_keys is missing sha3(contract_id || 0x0002) = %s", key2)
	}
}

// C4. Contract effects (call).
func checkCallEffects(t *testing.T, vec vectorJSON) {
	t.Helper()
	if vec.Status != "SUCCESS" {
		return
	}
	if vec.ContractEffectsBlobHex == "" {
		t.Fatalf("C4: contract_effects_blob_hex is absent for a SUCCESS call vector")
	}

	blob := mustHex(t, vec.ContractEffectsBlobHex)
	eff, err := codec.DecodeContractEffects(blob)
	if err != nil {
		t.Fatalf("C4: DecodeContractEffects: %v", err)
	}

	erBlob := mustHex(t, vec.ExecutionResultBlobHex)
	er, err := codec.DecodeExecutionResult(erBlob)
	if err != nil {
		t.Fatalf("C4: DecodeExecutionResult: %v", err)
	}

	if !bytes.Equal(eff.ReturnData, er.ReturnData) {
		t.Fatalf("C4: ContractEffects.ReturnData disagrees with ExecutionResult.ReturnData")
	}
	if eff.ClaimedDeposit != er.ClaimedDeposit {
		t.Fatalf("C4: ContractEffects.ClaimedDeposit disagrees with ExecutionResult.ClaimedDeposit")
	}

	checkCallEffectsTie(t, vec, eff, er)

	contractID := mustHex(t, vec.Receipt.ContractID)
	wantSet := make(map[string]bool, len(eff.StateDiff))
	for _, e := range eff.StateDiff {
		wantSet[hex.EncodeToString(crypto.Hash(contractID, e.LocalKey))] = true
	}
	gotSet := make(map[string]bool, len(er.StateDiff))
	for _, e := range er.StateDiff {
		gotSet[hex.EncodeToString(e.Key[:])] = true
	}
	// The runtime's quota entry is not a contract-declared key; C6 owns it.
	delete(gotSet, hex.EncodeToString(crypto.Hash(contractID, []byte{0x00, 0x05})))
	if !reflect.DeepEqual(wantSet, gotSet) {
		t.Fatalf("C4: inline-derived local StateDiff key set disagrees with ExecutionResult.StateDiff key set:\n got  %v\n want %v", gotSet, wantSet)
	}
}

// checkCallEffectsTie ties the ExecutionResult's events and outbox to
// the contract effects blob on SUCCESS: the events are the blob's
// events in order, the first n outbox messages are the blob's n
// contract messages in order, and the only message after them is the
// runtime-emitted refund at outbox_index n, present exactly when the
// unclaimed remainder deposit_amount - claimed_deposit is non-zero.
// Both sides come from the codec decoders; the production result
// assembly is not consulted. The tie owns the events, the contract
// messages in order and the message count; C5s owns the refund's
// fields.
func checkCallEffectsTie(t *testing.T, vec vectorJSON, eff codec.ContractEffects, er codec.ExecutionResult) {
	t.Helper()

	if len(er.Events) != len(eff.Events) {
		t.Fatalf("C4: ExecutionResult has %d events, want the blob's %d", len(er.Events), len(eff.Events))
	}
	for i := range eff.Events {
		if !reflect.DeepEqual(er.Events[i], eff.Events[i]) {
			t.Fatalf("C4: ExecutionResult event %d = %+v, want the blob's %+v", i, er.Events[i], eff.Events[i])
		}
	}

	n := len(eff.Outbox)
	if len(er.Outbox) < n {
		t.Fatalf("C4: ExecutionResult has %d outbox messages, want at least the blob's %d contract messages", len(er.Outbox), n)
	}
	for i := 0; i < n; i++ {
		if !reflect.DeepEqual(er.Outbox[i], eff.Outbox[i]) {
			t.Fatalf("C4: ExecutionResult outbox message %d = %+v, want the blob's %+v", i, er.Outbox[i], eff.Outbox[i])
		}
	}

	remainder := new(big.Int)
	if vec.Input != nil && vec.Input.DepositAmount != "" {
		remainder.SetBytes(mustHex(t, vec.Input.DepositAmount))
	}
	claim := new(big.Int).SetBytes(eff.ClaimedDeposit[:])
	remainder.Sub(remainder, claim)
	if remainder.Sign() < 0 {
		t.Fatalf("C4: claimed_deposit exceeds deposit_amount by %s on a SUCCESS row", new(big.Int).Neg(remainder))
	}
	want := n
	if remainder.Sign() > 0 {
		want = n + 1
	}
	if len(er.Outbox) != want {
		t.Fatalf("C4: ExecutionResult has %d outbox messages, want %d (%d contract messages, then the refund only when the remainder %s is non-zero)", len(er.Outbox), want, n, remainder)
	}
}

// quotaLeaf reads the quota leaf (local key 0x0005) of the vector's contract from pre_state,
// returning purchased and used, both 0 when the leaf is absent. The leaf layout is
// purchased:u64 || used:u64, big-endian.
func quotaLeaf(t *testing.T, vec vectorJSON) (quotaKey string, purchased, used uint64) {
	t.Helper()
	contractID := mustHex(t, vec.Receipt.ContractID)
	quotaKey = hex.EncodeToString(crypto.Hash(contractID, []byte{0x00, 0x05}))
	leafHex, present := vec.PreState[quotaKey]
	if !present {
		return quotaKey, 0, 0
	}
	leaf := mustHex(t, leafHex)
	if len(leaf) != 16 {
		t.Fatalf("quota leaf in pre_state has width %d, want 16", len(leaf))
	}
	return quotaKey, binary.BigEndian.Uint64(leaf[:8]), binary.BigEndian.Uint64(leaf[8:])
}

// blobQuotaDelta is the net change in counted leaf bytes the returned blob's StateDiff makes
// against pre_state: a leaf counts as 64 plus its value length, and a deletion or an absent
// pre-state leaf counts as 0.
func blobQuotaDelta(t *testing.T, vec vectorJSON) int64 {
	t.Helper()
	eff, err := codec.DecodeContractEffects(mustHex(t, vec.ContractEffectsBlobHex))
	if err != nil {
		t.Fatalf("quota delta: DecodeContractEffects: %v", err)
	}
	contractID := mustHex(t, vec.Receipt.ContractID)
	var delta int64
	for _, e := range eff.StateDiff {
		if e.Value != nil {
			delta += 64 + int64(len(e.Value))
		}
		preHex, present := vec.PreState[hex.EncodeToString(crypto.Hash(contractID, e.LocalKey))]
		if present {
			delta -= 64 + int64(len(preHex)/2)
		}
	}
	return delta
}

// C6. Storage quota (call, SUCCESS or STORAGE_QUOTA_EXCEEDED).
func checkCallQuota(t *testing.T, vec vectorJSON) {
	t.Helper()
	if vec.ContractEffectsBlobHex == "" {
		t.Fatalf("C6: contract_effects_blob_hex is absent")
	}
	quotaKey, purchased, used := quotaLeaf(t, vec)
	delta := blobQuotaDelta(t, vec)

	er, err := codec.DecodeExecutionResult(mustHex(t, vec.ExecutionResultBlobHex))
	if err != nil {
		t.Fatalf("C6: DecodeExecutionResult: %v", err)
	}
	witnessed := make(map[string]bool, len(vec.WitnessKeys))
	for _, k := range vec.WitnessKeys {
		witnessed[k] = true
	}
	var quotaEntry *codec.StateDiffEntry
	for i, e := range er.StateDiff {
		if hex.EncodeToString(e.Key[:]) == quotaKey {
			quotaEntry = &er.StateDiff[i]
		}
	}

	if vec.Status == "STORAGE_QUOTA_EXCEEDED" {
		after := new(big.Int).Add(new(big.Int).SetUint64(used), big.NewInt(delta))
		if delta <= 0 || after.Cmp(new(big.Int).SetUint64(purchased)) <= 0 {
			t.Fatalf("C6: STORAGE_QUOTA_EXCEEDED: delta = %d, used = %d, purchased = %d, want delta > 0 and used + delta > purchased", delta, used, purchased)
		}
		if !witnessed[quotaKey] {
			t.Fatalf("C6: STORAGE_QUOTA_EXCEEDED: witness_keys is missing the quota key %s", quotaKey)
		}
		eff, err := codec.DecodeContractEffects(mustHex(t, vec.ContractEffectsBlobHex))
		if err != nil {
			t.Fatalf("C6: DecodeContractEffects: %v", err)
		}
		contractID := mustHex(t, vec.Receipt.ContractID)
		for _, e := range eff.StateDiff {
			if k := hex.EncodeToString(crypto.Hash(contractID, e.LocalKey)); !witnessed[k] {
				t.Fatalf("C6: STORAGE_QUOTA_EXCEEDED: witness_keys is missing the derived blob key %s", k)
			}
		}
		return
	}

	if delta == 0 {
		if quotaEntry != nil {
			t.Fatalf("C6: delta = 0: ExecutionResult.StateDiff carries a quota entry, want none")
		}
		if witnessed[quotaKey] {
			t.Fatalf("C6: delta = 0: witness_keys holds the quota key %s, want it absent", quotaKey)
		}
		return
	}
	after := new(big.Int).Add(new(big.Int).SetUint64(used), big.NewInt(delta))
	if after.Sign() < 0 || after.Cmp(new(big.Int).SetUint64(purchased)) > 0 {
		t.Fatalf("C6: SUCCESS: used + delta = %s outside [0, purchased %d]", after, purchased)
	}
	if quotaEntry == nil {
		t.Fatalf("C6: delta = %d: ExecutionResult.StateDiff has no quota entry", delta)
	}
	want := make([]byte, 16)
	binary.BigEndian.PutUint64(want[:8], purchased)
	binary.BigEndian.PutUint64(want[8:], after.Uint64())
	if !bytes.Equal(quotaEntry.Value, want) {
		t.Fatalf("C6: quota entry value = %x, want purchased || used + delta %x", quotaEntry.Value, want)
	}
}

// C5. Refund shape (call, non-SUCCESS, non-zero deposit).
func checkCallRefund(t *testing.T, vec vectorJSON) {
	t.Helper()
	if vec.Status == "SUCCESS" || vec.Input == nil {
		return
	}
	if vec.Input.DepositAmount == "" || vec.Input.DepositAmount == zeroHashHex {
		return
	}

	erBlob := mustHex(t, vec.ExecutionResultBlobHex)
	er, err := codec.DecodeExecutionResult(erBlob)
	if err != nil {
		t.Fatalf("C5: DecodeExecutionResult: %v", err)
	}
	if len(er.Outbox) != 1 {
		t.Fatalf("C5: len(Outbox) = %d, want exactly 1 (the full refund)", len(er.Outbox))
	}
	msg := er.Outbox[0]
	if msg.SourceContract != (types.Hash{}) {
		t.Fatalf("C5: SourceContract = %x, want zero", msg.SourceContract)
	}
	if msg.InputIndex != vec.Receipt.InputIndex {
		t.Fatalf("C5: InputIndex = %d, want receipt.input_index %d", msg.InputIndex, vec.Receipt.InputIndex)
	}
	if msg.OutboxIndex != 0 {
		t.Fatalf("C5: OutboxIndex = %d, want 0", msg.OutboxIndex)
	}
	if len(msg.Payload) != 0 {
		t.Fatalf("C5: Payload = %x, want empty", msg.Payload)
	}
	withdrawal, ok := msg.Target.(codec.L1Withdrawal)
	if !ok {
		t.Fatalf("C5: Target = %T, want codec.L1Withdrawal", msg.Target)
	}
	if hex.EncodeToString(withdrawal.RecipientL1[:]) != vec.L1Sender {
		t.Fatalf("C5: RecipientL1 = %x, want l1_sender %s", withdrawal.RecipientL1, vec.L1Sender)
	}
	if hex.EncodeToString(withdrawal.Asset[:]) != vec.Input.DepositAsset {
		t.Fatalf("C5: Asset = %x, want input view deposit_asset %s", withdrawal.Asset, vec.Input.DepositAsset)
	}
	if hex.EncodeToString(withdrawal.Amount[:]) != vec.Input.DepositAmount {
		t.Fatalf("C5: Amount = %x, want input view deposit_amount %s (full refund)", withdrawal.Amount, vec.Input.DepositAmount)
	}
	if vec.Receipt.OutboxCount != 1 {
		t.Fatalf("C5: receipt.outbox_count = %d, want 1", vec.Receipt.OutboxCount)
	}
	if vec.Receipt.OutboxRoot == zeroHashHex {
		t.Fatalf("C5: receipt.outbox_root is zero, want non-zero")
	}
}

// checkCallSuccessRefund re-derives the SUCCESS refund of a call row
// with a deposit from the row's own inputs: the unclaimed remainder
// deposit_amount - claimed_deposit is refunded as a zero-source
// L1Withdrawal at outbox_index = the number of contract messages, and
// no refund message exists when the remainder is 0. The claim and the
// contract messages come from the contract effects blob, the deposit
// from the input view; the production refund code is not consulted. Its
// failure messages carry the label C5s, the SUCCESS-row counterpart of
// C5.
func checkCallSuccessRefund(t *testing.T, vec vectorJSON) {
	t.Helper()
	if vec.Status != "SUCCESS" || vec.Input == nil || vec.Input.DepositAmount == "" {
		return
	}
	deposit := new(big.Int).SetBytes(mustHex(t, vec.Input.DepositAmount))
	if deposit.Sign() == 0 {
		return
	}

	eff, err := codec.DecodeContractEffects(mustHex(t, vec.ContractEffectsBlobHex))
	if err != nil {
		t.Fatalf("C5s: DecodeContractEffects: %v", err)
	}
	er, err := codec.DecodeExecutionResult(mustHex(t, vec.ExecutionResultBlobHex))
	if err != nil {
		t.Fatalf("C5s: DecodeExecutionResult: %v", err)
	}
	claim := new(big.Int).SetBytes(eff.ClaimedDeposit[:])
	remainder := new(big.Int).Sub(deposit, claim)
	if remainder.Sign() < 0 {
		t.Fatalf("C5s: claimed_deposit %s exceeds deposit_amount %s on a SUCCESS row", claim, deposit)
	}

	n := len(eff.Outbox)
	wantMessages := n
	if remainder.Sign() > 0 {
		wantMessages = n + 1
	}
	if len(er.Outbox) != wantMessages {
		t.Fatalf("C5s: ExecutionResult has %d outbox messages, want %d (%d contract messages, remainder %s)", len(er.Outbox), wantMessages, n, remainder)
	}
	if int(vec.Receipt.OutboxCount) != wantMessages {
		t.Fatalf("C5s: receipt.outbox_count = %d, want %d", vec.Receipt.OutboxCount, wantMessages)
	}
	if remainder.Sign() == 0 {
		return
	}

	refund := er.Outbox[n]
	if refund.SourceContract != (types.Hash{}) {
		t.Fatalf("C5s: refund SourceContract = %x, want zero", refund.SourceContract)
	}
	if refund.OutboxIndex != uint32(n) {
		t.Fatalf("C5s: refund OutboxIndex = %d, want %d (the number of contract messages)", refund.OutboxIndex, n)
	}
	if refund.InputIndex != vec.Receipt.InputIndex {
		t.Fatalf("C5s: refund InputIndex = %d, want receipt.input_index %d", refund.InputIndex, vec.Receipt.InputIndex)
	}
	if len(refund.Payload) != 0 {
		t.Fatalf("C5s: refund Payload = %x, want empty", refund.Payload)
	}
	w, ok := refund.Target.(codec.L1Withdrawal)
	if !ok {
		t.Fatalf("C5s: refund Target = %T, want codec.L1Withdrawal", refund.Target)
	}
	if hex.EncodeToString(w.RecipientL1[:]) != vec.L1Sender || hex.EncodeToString(w.Asset[:]) != vec.Input.DepositAsset {
		t.Fatalf("C5s: refund to %x in %x, want l1_sender %s in %s", w.RecipientL1, w.Asset, vec.L1Sender, vec.Input.DepositAsset)
	}
	if got := new(big.Int).SetBytes(w.Amount[:]); got.Cmp(remainder) != 0 {
		t.Fatalf("C5s: refund amount %s, want deposit - claimed_deposit %s", got, remainder)
	}
}

// checkUnsupportedKind is the unsupported_kind shape's whole check,
// from the JSON alone.
func checkUnsupportedKind(t *testing.T, vec vectorJSON) {
	t.Helper()

	if vec.InputKind <= 7 {
		t.Fatalf("unsupported_kind: input_kind = %d, want > 7", vec.InputKind)
	}
	if vec.Input != nil {
		t.Fatalf("unsupported_kind: input = %+v, want absent", vec.Input)
	}
	if vec.ModuleWasmHex != "" || vec.CodeHash != "" || vec.InstrumentedCodeHash != "" || vec.InputFrameHex != "" || vec.ContractEffectsBlobHex != "" {
		t.Fatalf("unsupported_kind: module_wasm_hex/code_hash/instrumented_code_hash/input_frame_hex/contract_effects_blob_hex must all be empty")
	}

	sourceHash := mustHex(t, vec.SourceAccountBlockHash)
	wantCanonical := append([]byte{vec.InputKind}, sourceHash...)
	gotCanonical := mustHex(t, vec.CanonicalInputHex)
	if !bytes.Equal(gotCanonical, wantCanonical) {
		t.Fatalf("unsupported_kind: canonical_input_hex = %x, want input_kind ‖ source_account_block_hash %x", gotCanonical, wantCanonical)
	}

	if vec.Status != "UNSUPPORTED_OPERATION" {
		t.Fatalf("unsupported_kind: status = %s, want UNSUPPORTED_OPERATION", vec.Status)
	}
	if vec.GasUsed != 0 {
		t.Fatalf("unsupported_kind: gas_used = %d, want 0", vec.GasUsed)
	}
	if vec.Receipt.ContractID != zeroHashHex || vec.Receipt.CodeHash != zeroHashHex {
		t.Fatalf("unsupported_kind: receipt.contract_id = %s, receipt.code_hash = %s, want both zero", vec.Receipt.ContractID, vec.Receipt.CodeHash)
	}
	if vec.Receipt.OutboxCount != 0 {
		t.Fatalf("unsupported_kind: receipt.outbox_count = %d, want 0", vec.Receipt.OutboxCount)
	}
	if vec.Receipt.EventCount != 0 {
		t.Fatalf("unsupported_kind: receipt.event_count = %d, want 0", vec.Receipt.EventCount)
	}
}

// checkDeploy is the deploy shape's whole check: the schema's shape
// rules, then the receipt identity and the ReturnData shape for the
// vector's own kind and status.
func checkDeploy(t *testing.T, file vectorFile, vec vectorJSON) {
	t.Helper()

	// Shape rules, every kind.
	if vec.InputFrameHex != "" {
		t.Fatalf("deploy: input_frame_hex = %q, want empty", vec.InputFrameHex)
	}
	if vec.ContractEffectsBlobHex != "" {
		t.Fatalf("deploy: contract_effects_blob_hex = %q, want empty", vec.ContractEffectsBlobHex)
	}
	if vec.Input == nil {
		t.Fatalf("deploy: input is absent")
	}
	if (vec.SourceDataHex != "") != (vec.InputKind == 3) {
		t.Fatalf("deploy: source_data_hex present = %v, want (input_kind == 3) = %v", vec.SourceDataHex != "", vec.InputKind == 3)
	}
	moduleRaw := mustHex(t, vec.ModuleWasmHex)
	moduleHash := hex.EncodeToString(crypto.Hash(moduleRaw))
	if moduleHash != vec.CodeHash {
		t.Fatalf("deploy: sha3(module_wasm_hex) = %s, want code_hash %s", moduleHash, vec.CodeHash)
	}
	if vec.GasUsed != 0 {
		t.Fatalf("deploy: gas_used = %d, want 0", vec.GasUsed)
	}
	if vec.Receipt.GasUsed != 0 {
		t.Fatalf("deploy: receipt.gas_used = %d, want 0", vec.Receipt.GasUsed)
	}

	switch vec.InputKind {
	case 2:
		if vec.Input.CodeHash != vec.CodeHash {
			t.Fatalf("deploy: kind 2: input.code_hash = %s, want code_hash %s", vec.Input.CodeHash, vec.CodeHash)
		}
		if vec.Input.MetadataHex == nil {
			t.Fatalf("deploy: kind 2: metadata_hex is absent")
		}
		metadataHash := hex.EncodeToString(crypto.Hash(mustHex(t, *vec.Input.MetadataHex)))
		if metadataHash != vec.Input.MetadataHash {
			t.Fatalf("deploy: kind 2: sha3(metadata_hex) = %s, want metadata_hash %s", metadataHash, vec.Input.MetadataHash)
		}
		if _, err := codec.DecodeContractMetadata(mustHex(t, *vec.Input.MetadataHex)); err != nil {
			t.Fatalf("deploy: kind 2: DecodeContractMetadata(metadata_hex): %v, want nil", err)
		}
		if vec.Input.ChunkCount == nil || *vec.Input.ChunkCount != 1 {
			t.Fatalf("deploy: kind 2: chunk_count = %v, want 1", vec.Input.ChunkCount)
		}
		if vec.Input.TotalSize == nil || *vec.Input.TotalSize != uint64(len(moduleRaw)) {
			t.Fatalf("deploy: kind 2: total_size = %v, want len(module_wasm_hex) %d", vec.Input.TotalSize, len(moduleRaw))
		}
		wantChunkCount := uint32((*vec.Input.TotalSize + MaxDeployChunkData - 1) / MaxDeployChunkData)
		if *vec.Input.ChunkCount != wantChunkCount {
			t.Fatalf("deploy: kind 2: chunk_count = %d, want ceil(total_size / MaxDeployChunkData) %d", *vec.Input.ChunkCount, wantChunkCount)
		}
		checkDeployNoEffect(t, vec)
	case 3:
		if vec.Input.CodeHash != vec.CodeHash {
			t.Fatalf("deploy: kind 3: input.code_hash = %s, want code_hash %s", vec.Input.CodeHash, vec.CodeHash)
		}
		sourceData := mustHex(t, vec.SourceDataHex)
		chunkHash := hex.EncodeToString(crypto.Hash(sourceData))
		if chunkHash != vec.Input.ChunkHash {
			t.Fatalf("deploy: kind 3: sha3(source_data_hex) = %s, want chunk_hash %s", chunkHash, vec.Input.ChunkHash)
		}
		if len(sourceData) > MaxDeployChunkData {
			t.Fatalf("deploy: kind 3: len(source_data) = %d, want <= MaxDeployChunkData %d", len(sourceData), MaxDeployChunkData)
		}
		if vec.Input.ChunkIndex == nil || *vec.Input.ChunkIndex != 0 {
			t.Fatalf("deploy: kind 3: chunk_index = %v, want 0", vec.Input.ChunkIndex)
		}
		if vec.SourceDataHex != vec.ModuleWasmHex {
			t.Fatalf("deploy: kind 3: source_data_hex != module_wasm_hex")
		}
		checkDeployNoEffect(t, vec)
	case 4:
		if vec.Status != "SUCCESS" {
			t.Fatalf("deploy: kind 4: status = %s, want SUCCESS (a failed finalize is the validation_rejection shape)", vec.Status)
		}
		l1Sender := mustHex(t, vec.L1Sender)
		var idxBuf [8]byte
		binary.BigEndian.PutUint64(idxBuf[:], vec.GlobalInputIndex)
		chainID := mustHex(t, file.ChainID)
		wantContractID := hex.EncodeToString(crypto.Hash(l1Sender, idxBuf[:], chainID))
		if wantContractID != vec.Receipt.ContractID {
			t.Fatalf("deploy: kind 4: inline contract_id = %s, want receipt.contract_id %s", wantContractID, vec.Receipt.ContractID)
		}
		instrumented, err := module.Instrument(moduleRaw, moduleLimits())
		if err != nil {
			t.Fatalf("deploy: kind 4: Instrument: %v", err)
		}
		instrumentedHash := hex.EncodeToString(crypto.Hash(instrumented))
		if instrumentedHash != vec.InstrumentedCodeHash || instrumentedHash != vec.Receipt.CodeHash {
			t.Fatalf("deploy: kind 4: sha3(instrument(module_wasm_hex)) = %s, want instrumented_code_hash %s and receipt.code_hash %s", instrumentedHash, vec.InstrumentedCodeHash, vec.Receipt.CodeHash)
		}

		erBlob := mustHex(t, vec.ExecutionResultBlobHex)
		er, err := codec.DecodeExecutionResult(erBlob)
		if err != nil {
			t.Fatalf("deploy: kind 4: DecodeExecutionResult: %v", err)
		}
		contractID := mustHex(t, vec.Receipt.ContractID)
		key1 := hex.EncodeToString(crypto.Hash(contractID, []byte{0x00, 0x01}))
		key2 := hex.EncodeToString(crypto.Hash(contractID, []byte{0x00, 0x02}))
		key3 := hex.EncodeToString(crypto.Hash(contractID, []byte{0x00, 0x03}))
		key4 := hex.EncodeToString(crypto.Hash(contractID, []byte{0x00, 0x04}))

		if vec.PendingMetadataHex == nil {
			t.Fatalf("deploy: kind 4: pending_metadata_hex is absent")
		}
		meta, err := codec.DecodeContractMetadata(mustHex(t, *vec.PendingMetadataHex))
		if err != nil {
			t.Fatalf("deploy: kind 4: DecodeContractMetadata(pending_metadata_hex): %v, want nil (a SUCCESS finalize needs a well-formed pending record)", err)
		}
		wantEntries := 3
		if meta.UpgradePolicy == codec.PolicyOwnerUpgradeable {
			wantEntries = 4
		}
		if len(er.StateDiff) != wantEntries {
			t.Fatalf("deploy: kind 4: ExecutionResult.StateDiff has %d entries, want exactly %d", len(er.StateDiff), wantEntries)
		}
		gotByKey := make(map[string][]byte, wantEntries)
		for _, e := range er.StateDiff {
			gotByKey[hex.EncodeToString(e.Key[:])] = e.Value
		}
		instrumentedCodeHashBytes := mustHex(t, instrumentedHash)
		codeHashBytes := mustHex(t, vec.CodeHash)
		if v, ok := gotByKey[key1]; !ok || !bytes.Equal(v, instrumentedCodeHashBytes) {
			t.Fatalf("deploy: kind 4: StateDiff[sha3(contract_id||0x0001)] = %x, want instrumented_code_hash %x", v, instrumentedCodeHashBytes)
		}
		if v, ok := gotByKey[key2]; !ok || !bytes.Equal(v, codeHashBytes) {
			t.Fatalf("deploy: kind 4: StateDiff[sha3(contract_id||0x0002)] = %x, want input.code_hash %x", v, codeHashBytes)
		}
		if v, ok := gotByKey[key3]; !ok || !bytes.Equal(v, []byte{meta.UpgradePolicy}) {
			t.Fatalf("deploy: kind 4: StateDiff[sha3(contract_id||0x0003)] = %x, want policy byte %d (pending metadata's upgrade_policy)", v, meta.UpgradePolicy)
		}
		if meta.UpgradePolicy == codec.PolicyOwnerUpgradeable {
			wantOwner := hashOf(crypto.Hash([]byte("L1_CALLER:"), meta.Owner[:]))
			if v, ok := gotByKey[key4]; !ok || !bytes.Equal(v, wantOwner[:]) {
				t.Fatalf("deploy: kind 4: StateDiff[sha3(contract_id||0x0004)] = %x, want caller(pending metadata's owner) %x", v, wantOwner[:])
			}
		} else if _, ok := gotByKey[key4]; ok {
			t.Fatalf("deploy: kind 4: StateDiff has an owner entry for an IMMUTABLE deploy, want none")
		}

		wantReturnData := append(append([]byte{}, codeHashBytes...), instrumentedCodeHashBytes...)
		if !bytes.Equal(er.ReturnData, wantReturnData) {
			t.Fatalf("deploy: kind 4: ExecutionResult.ReturnData = %x, want input.code_hash || instrumented_code_hash %x", er.ReturnData, wantReturnData)
		}
	case 5:
		contractID := mustHex(t, vec.Receipt.ContractID)
		if vec.Input.ContractID != vec.Receipt.ContractID {
			t.Fatalf("deploy: kind 5: input.contract_id = %s, want receipt.contract_id %s", vec.Input.ContractID, vec.Receipt.ContractID)
		}
		hashMismatch := vec.Input.NewCodeHash != vec.CodeHash
		if hashMismatch && (vec.Status != "VALIDATION_FAILED" || vec.Receipt.CodeHash == zeroHashHex) {
			t.Fatalf("deploy: kind 5: input.new_code_hash = %s, want code_hash %s", vec.Input.NewCodeHash, vec.CodeHash)
		}
		key1 := hex.EncodeToString(crypto.Hash(contractID, []byte{0x00, 0x01}))
		key2 := hex.EncodeToString(crypto.Hash(contractID, []byte{0x00, 0x02}))
		key3 := hex.EncodeToString(crypto.Hash(contractID, []byte{0x00, 0x03}))
		key4 := hex.EncodeToString(crypto.Hash(contractID, []byte{0x00, 0x04}))
		wantWitness := []string{key1, key2, key3, key4}
		sort.Strings(wantWitness)
		if !reflect.DeepEqual(vec.WitnessKeys, wantWitness) {
			t.Fatalf("deploy: kind 5: witness_keys = %v, want the four reserved keys ascending %v", vec.WitnessKeys, wantWitness)
		}

		switch vec.Status {
		case "SUCCESS":
			instrumented, err := module.Instrument(moduleRaw, moduleLimits())
			if err != nil {
				t.Fatalf("deploy: kind 5 SUCCESS: Instrument: %v", err)
			}
			instrumentedHash := hex.EncodeToString(crypto.Hash(instrumented))
			if instrumentedHash != vec.InstrumentedCodeHash || instrumentedHash != vec.Receipt.CodeHash {
				t.Fatalf("deploy: kind 5 SUCCESS: sha3(instrument(module_wasm_hex)) = %s, want instrumented_code_hash %s and receipt.code_hash %s", instrumentedHash, vec.InstrumentedCodeHash, vec.Receipt.CodeHash)
			}

			erBlob := mustHex(t, vec.ExecutionResultBlobHex)
			er, err := codec.DecodeExecutionResult(erBlob)
			if err != nil {
				t.Fatalf("deploy: kind 5 SUCCESS: DecodeExecutionResult: %v", err)
			}
			if len(er.StateDiff) != 2 {
				t.Fatalf("deploy: kind 5 SUCCESS: ExecutionResult.StateDiff has %d entries, want exactly 2", len(er.StateDiff))
			}
			gotByKey := make(map[string][]byte, 2)
			for _, e := range er.StateDiff {
				gotByKey[hex.EncodeToString(e.Key[:])] = e.Value
			}
			instrumentedCodeHashBytes := mustHex(t, instrumentedHash)
			newCodeHashBytes := mustHex(t, vec.CodeHash)
			if v, ok := gotByKey[key1]; !ok || !bytes.Equal(v, instrumentedCodeHashBytes) {
				t.Fatalf("deploy: kind 5 SUCCESS: StateDiff[sha3(contract_id||0x0001)] = %x, want instrumented_code_hash %x", v, instrumentedCodeHashBytes)
			}
			if v, ok := gotByKey[key2]; !ok || !bytes.Equal(v, newCodeHashBytes) {
				t.Fatalf("deploy: kind 5 SUCCESS: StateDiff[sha3(contract_id||0x0002)] = %x, want input.new_code_hash %x", v, newCodeHashBytes)
			}

			oldHashHex, oldPresent := vec.PreState[key1]
			if !oldPresent {
				t.Fatalf("deploy: kind 5 SUCCESS: pre_state has no entry for sha3(contract_id||0x0001)")
			}
			oldHash := mustHex(t, oldHashHex)
			wantReturnData := append(append([]byte{}, oldHash...), instrumentedCodeHashBytes...)
			if !bytes.Equal(er.ReturnData, wantReturnData) {
				t.Fatalf("deploy: kind 5 SUCCESS: ExecutionResult.ReturnData = %x, want old || new CodeHash %x", er.ReturnData, wantReturnData)
			}
			if _, present := vec.PreState[key4]; !present {
				t.Fatalf("deploy: kind 5 SUCCESS: pre_state has no entry for sha3(contract_id||0x0004)")
			}
			policyHex, present := vec.PreState[key3]
			if !present || policyHex != "01" {
				t.Fatalf("deploy: kind 5 SUCCESS: pre_state[sha3(contract_id||0x0003)] = %q, want \"01\"", policyHex)
			}
		case "VALIDATION_FAILED":
			if vec.Receipt.CodeHash != zeroHashHex {
				// The authorized refusal: the contract is deployed and the caller is its owner.
				oldHashHex, present := vec.PreState[key1]
				if !present || oldHashHex != vec.Receipt.CodeHash {
					t.Fatalf("deploy: kind 5 VALIDATION_FAILED (authorized): pre_state[sha3(contract_id||0x0001)] = %q, want receipt.code_hash %s", oldHashHex, vec.Receipt.CodeHash)
				}
				l1Sender := mustHex(t, vec.L1Sender)
				caller := hashOf(crypto.Hash([]byte("L1_CALLER:"), l1Sender))
				if vec.PreState[key3] != "01" || vec.PreState[key4] != hex.EncodeToString(caller[:]) {
					t.Fatalf("deploy: kind 5 VALIDATION_FAILED (authorized): pre_state's policy leaf is %q and owner leaf is %q, want 01 and the caller %x", vec.PreState[key3], vec.PreState[key4], caller)
				}
				if vec.InstrumentedCodeHash != "" {
					t.Fatalf("deploy: kind 5 VALIDATION_FAILED (authorized): instrumented_code_hash = %q, want empty", vec.InstrumentedCodeHash)
				}
				reason := rejectionReasons[vec.ID]
				validateErr := module.Validate(moduleRaw, moduleLimits())
				switch {
				case hashMismatch:
					if reason != errAssemblyHash {
						t.Fatalf("deploy: kind 5 VALIDATION_FAILED (authorized): new_code_hash differs from sha3(module_wasm_hex) but rejectionReasons[%s] = %v, want errAssemblyHash", vec.ID, reason)
					}
				case validateErr != nil:
					if reason == nil || !errors.Is(validateErr, reason) {
						t.Fatalf("deploy: kind 5 VALIDATION_FAILED (authorized): Validate err = %v, want errors.Is(..., %v)", validateErr, reason)
					}
				default:
					instrumented, err := module.Instrument(moduleRaw, moduleLimits())
					if err != nil {
						t.Fatalf("deploy: kind 5 VALIDATION_FAILED (authorized): Instrument: %v", err)
					}
					if hex.EncodeToString(crypto.Hash(instrumented)) != vec.Receipt.CodeHash {
						t.Fatalf("deploy: kind 5 VALIDATION_FAILED (authorized): no section 11.3 cause holds: an authorized upgrade of a valid module, staged under its own hash, with a new CodeHash must be SUCCESS")
					}
				}
				break
			}
			if vec.InstrumentedCodeHash != "" {
				t.Fatalf("deploy: kind 5 VALIDATION_FAILED (undeployed): instrumented_code_hash = %q, want empty", vec.InstrumentedCodeHash)
			}
			if len(vec.PreState) != 0 {
				t.Fatalf("deploy: kind 5 VALIDATION_FAILED (undeployed): pre_state has %d entries, want 0", len(vec.PreState))
			}
			if err := module.Validate(moduleRaw, moduleLimits()); err != nil {
				t.Fatalf("deploy: kind 5 VALIDATION_FAILED (undeployed): Validate: %v, want nil", err)
			}
		case "UNAUTHORIZED":
			oldHashHex, present := vec.PreState[key1]
			if !present {
				t.Fatalf("deploy: kind 5 UNAUTHORIZED: pre_state has no entry for sha3(contract_id||0x0001)")
			}
			policyHex, policyPresent := vec.PreState[key3]
			l1Sender := mustHex(t, vec.L1Sender)
			caller := hashOf(crypto.Hash([]byte("L1_CALLER:"), l1Sender))
			ownerHex, ownerPresent := vec.PreState[key4]
			policyOK := policyPresent && policyHex == "01"
			ownerOK := ownerPresent && ownerHex == hex.EncodeToString(caller[:])
			if policyOK && ownerOK {
				t.Fatalf("deploy: kind 5 UNAUTHORIZED: pre_state's policy leaf is 01 and owner leaf is the caller, want at least one to disqualify the caller")
			}
			if vec.Receipt.CodeHash != oldHashHex || vec.Receipt.CodeHash == zeroHashHex {
				t.Fatalf("deploy: kind 5 UNAUTHORIZED: receipt.code_hash = %s, want pre_state's old hash %s, non-zero", vec.Receipt.CodeHash, oldHashHex)
			}
			if vec.InstrumentedCodeHash != "" {
				t.Fatalf("deploy: kind 5 UNAUTHORIZED: instrumented_code_hash = %q, want empty", vec.InstrumentedCodeHash)
			}
			if err := module.Validate(moduleRaw, moduleLimits()); !errors.Is(err, wantValidateErr(vec.ID)) {
				t.Fatalf("deploy: kind 5 UNAUTHORIZED: Validate: %v, want %v", err, wantValidateErr(vec.ID))
			}
		default:
			t.Fatalf("deploy: kind 5 status %s has no branch: add one with its first vector", vec.Status)
		}
	default:
		t.Fatalf("deploy: input_kind %d has no branch: add one with its first vector", vec.InputKind)
	}
}

// checkDeployNoEffect is kind 2 and 3's shared no-effect assertion: the input's own DecodeInput
// side effect is the whole point, so Apply's receipt is empty.
func checkDeployNoEffect(t *testing.T, vec vectorJSON) {
	t.Helper()

	if vec.Status != "SUCCESS" {
		t.Fatalf("deploy: kind %d: status = %s, want SUCCESS", vec.InputKind, vec.Status)
	}
	if vec.Receipt.ContractID != zeroHashHex || vec.Receipt.CodeHash != zeroHashHex {
		t.Fatalf("deploy: kind %d: receipt.contract_id = %s, receipt.code_hash = %s, want both zero", vec.InputKind, vec.Receipt.ContractID, vec.Receipt.CodeHash)
	}
	if vec.Receipt.StateDiffHash != zeroHashHex || vec.Receipt.ReturnHash != zeroHashHex {
		t.Fatalf("deploy: kind %d: receipt.state_diff_hash = %s, receipt.return_hash = %s, want both zero", vec.InputKind, vec.Receipt.StateDiffHash, vec.Receipt.ReturnHash)
	}
	if vec.Receipt.EventCount != 0 || vec.Receipt.OutboxCount != 0 {
		t.Fatalf("deploy: kind %d: receipt.event_count = %d, receipt.outbox_count = %d, want both 0", vec.InputKind, vec.Receipt.EventCount, vec.Receipt.OutboxCount)
	}
	if len(vec.WitnessKeys) != 0 {
		t.Fatalf("deploy: kind %d: witness_keys has %d entries, want 0", vec.InputKind, len(vec.WitnessKeys))
	}
	if vec.PreStateRoot != vec.PostStateRoot {
		t.Fatalf("deploy: kind %d: pre_state_root %s != post_state_root %s", vec.InputKind, vec.PreStateRoot, vec.PostStateRoot)
	}
	if vec.ExecutionResultBlobHex == "" {
		t.Fatalf("deploy: kind %d: execution_result_blob_hex is absent", vec.InputKind)
	}
	er, err := codec.DecodeExecutionResult(mustHex(t, vec.ExecutionResultBlobHex))
	if err != nil {
		t.Fatalf("deploy: kind %d: DecodeExecutionResult: %v", vec.InputKind, err)
	}
	if len(er.StateDiff) != 0 || len(er.Events) != 0 || len(er.Outbox) != 0 || len(er.ReturnData) != 0 {
		t.Fatalf("deploy: kind %d: ExecutionResult has StateDiff=%d Events=%d Outbox=%d ReturnData=%d bytes, want all empty",
			vec.InputKind, len(er.StateDiff), len(er.Events), len(er.Outbox), len(er.ReturnData))
	}
}

// checkOwnership is the ownership shape's whole check, from the JSON
// alone.
func checkOwnership(t *testing.T, vec vectorJSON) {
	t.Helper()

	if vec.InputKind != 6 {
		t.Fatalf("ownership: input_kind = %d, want 6", vec.InputKind)
	}
	if vec.ModuleWasmHex != "" || vec.InputFrameHex != "" || vec.SourceDataHex != "" || vec.ContractEffectsBlobHex != "" {
		t.Fatalf("ownership: module_wasm_hex/input_frame_hex/source_data_hex/contract_effects_blob_hex must all be empty")
	}
	if vec.GasUsed != 0 || vec.Receipt.GasUsed != 0 {
		t.Fatalf("ownership: gas_used = %d, receipt.gas_used = %d, want both 0", vec.GasUsed, vec.Receipt.GasUsed)
	}
	if vec.Input == nil {
		t.Fatalf("ownership: input is absent")
	}
	if vec.Input.ContractID != vec.Receipt.ContractID {
		t.Fatalf("ownership: input.contract_id = %s, want receipt.contract_id %s", vec.Input.ContractID, vec.Receipt.ContractID)
	}
	if vec.Receipt.EventCount != 0 || vec.Receipt.OutboxCount != 0 {
		t.Fatalf("ownership: receipt.event_count = %d, receipt.outbox_count = %d, want both 0", vec.Receipt.EventCount, vec.Receipt.OutboxCount)
	}

	contractID := mustHex(t, vec.Input.ContractID)
	key1 := hex.EncodeToString(crypto.Hash(contractID, []byte{0x00, 0x01}))
	key3 := hex.EncodeToString(crypto.Hash(contractID, []byte{0x00, 0x03}))
	key4 := hex.EncodeToString(crypto.Hash(contractID, []byte{0x00, 0x04}))
	wantWitness := []string{key1, key3, key4}
	sort.Strings(wantWitness)
	if !reflect.DeepEqual(vec.WitnessKeys, wantWitness) {
		t.Fatalf("ownership: witness_keys = %v, want the three reserved keys ascending %v", vec.WitnessKeys, wantWitness)
	}

	l1Sender := mustHex(t, vec.L1Sender)
	caller := hashOf(crypto.Hash([]byte("L1_CALLER:"), l1Sender))
	callerHex := hex.EncodeToString(caller[:])

	switch vec.Status {
	case "VALIDATION_FAILED":
		if _, present := vec.PreState[key1]; present {
			t.Fatalf("ownership: VALIDATION_FAILED: pre_state has a 0x0001 leaf, want absent")
		}
		if vec.Receipt.CodeHash != zeroHashHex {
			t.Fatalf("ownership: VALIDATION_FAILED: receipt.code_hash = %s, want zero", vec.Receipt.CodeHash)
		}
	case "UNAUTHORIZED":
		codeHashHex, present := vec.PreState[key1]
		if !present {
			t.Fatalf("ownership: UNAUTHORIZED: pre_state has no 0x0001 leaf")
		}
		if vec.Receipt.CodeHash != codeHashHex {
			t.Fatalf("ownership: UNAUTHORIZED: receipt.code_hash = %s, want pre_state's 0x0001 leaf %s", vec.Receipt.CodeHash, codeHashHex)
		}
		policyHex, policyPresent := vec.PreState[key3]
		ownerHex, ownerPresent := vec.PreState[key4]
		policyOK := policyPresent && policyHex == "01"
		ownerOK := ownerPresent && ownerHex == callerHex
		if policyOK && ownerOK {
			t.Fatalf("ownership: UNAUTHORIZED: pre_state's policy leaf is 01 and owner leaf is the caller, want at least one to disqualify the caller")
		}
	case "SUCCESS":
		codeHashHex, present := vec.PreState[key1]
		if !present {
			t.Fatalf("ownership: SUCCESS: pre_state has no 0x0001 leaf")
		}
		if vec.Receipt.CodeHash != codeHashHex {
			t.Fatalf("ownership: SUCCESS: receipt.code_hash = %s, want pre_state's 0x0001 leaf %s", vec.Receipt.CodeHash, codeHashHex)
		}
		policyHex, policyPresent := vec.PreState[key3]
		if !policyPresent || policyHex != "01" {
			t.Fatalf("ownership: SUCCESS: pre_state[0x0003] = %q, want \"01\"", policyHex)
		}
		ownerHex, ownerPresent := vec.PreState[key4]
		if !ownerPresent || ownerHex != callerHex {
			t.Fatalf("ownership: SUCCESS: pre_state[0x0004] = %q, want the caller %s", ownerHex, callerHex)
		}

		erBlob := mustHex(t, vec.ExecutionResultBlobHex)
		er, err := codec.DecodeExecutionResult(erBlob)
		if err != nil {
			t.Fatalf("ownership: SUCCESS: DecodeExecutionResult: %v", err)
		}
		if len(er.ReturnData) != 0 {
			t.Fatalf("ownership: SUCCESS: ExecutionResult.ReturnData = %x, want empty", er.ReturnData)
		}

		if vec.Input.NewOwner != "" && vec.Input.NewOwner != zeroAddrHex {
			if len(er.StateDiff) != 1 {
				t.Fatalf("ownership: SUCCESS non-zero new_owner: ExecutionResult.StateDiff has %d entries, want exactly 1", len(er.StateDiff))
			}
			newOwner := mustHex(t, vec.Input.NewOwner)
			wantOwner := hashOf(crypto.Hash([]byte("L1_CALLER:"), newOwner))
			if hex.EncodeToString(er.StateDiff[0].Key[:]) != key4 || !bytes.Equal(er.StateDiff[0].Value, wantOwner[:]) {
				t.Fatalf("ownership: SUCCESS non-zero new_owner: StateDiff[0] = %+v, want {0x0004, caller(new_owner) %x}", er.StateDiff[0], wantOwner[:])
			}
		} else {
			if len(er.StateDiff) != 2 {
				t.Fatalf("ownership: SUCCESS zero new_owner: ExecutionResult.StateDiff has %d entries, want exactly 2", len(er.StateDiff))
			}
			gotByKey := make(map[string][]byte, 2)
			for _, e := range er.StateDiff {
				gotByKey[hex.EncodeToString(e.Key[:])] = e.Value
			}
			if v, ok := gotByKey[key3]; !ok || !bytes.Equal(v, []byte{0x00}) {
				t.Fatalf("ownership: SUCCESS zero new_owner: StateDiff[0x0003] = %x, want {0x00} (IMMUTABLE)", v)
			}
			if v, ok := gotByKey[key4]; !ok || v != nil {
				t.Fatalf("ownership: SUCCESS zero new_owner: StateDiff[0x0004] = %x, want a deletion (nil)", v)
			}
		}
	default:
		t.Fatalf("ownership: status %s has no branch: add one with its first vector", vec.Status)
	}
}

// checkBuyStorage is the buy_storage shape's whole check, from the JSON
// alone.
func checkBuyStorage(t *testing.T, vec vectorJSON) {
	t.Helper()

	if vec.InputKind != 7 {
		t.Fatalf("buy_storage: input_kind = %d, want 7", vec.InputKind)
	}
	if vec.ModuleWasmHex != "" || vec.InputFrameHex != "" || vec.SourceDataHex != "" || vec.ContractEffectsBlobHex != "" {
		t.Fatalf("buy_storage: module_wasm_hex/input_frame_hex/source_data_hex/contract_effects_blob_hex must all be empty")
	}
	if vec.GasUsed != 0 || vec.Receipt.GasUsed != 0 {
		t.Fatalf("buy_storage: gas_used = %d, receipt.gas_used = %d, want both 0", vec.GasUsed, vec.Receipt.GasUsed)
	}
	if vec.Receipt.EventCount != 0 || vec.Receipt.OutboxCount != 0 {
		t.Fatalf("buy_storage: receipt.event_count = %d, receipt.outbox_count = %d, want both 0", vec.Receipt.EventCount, vec.Receipt.OutboxCount)
	}
	if vec.Input == nil {
		t.Fatalf("buy_storage: input is absent")
	}
	if vec.Input.ContractID != vec.Receipt.ContractID {
		t.Fatalf("buy_storage: input.contract_id = %s, want receipt.contract_id %s", vec.Input.ContractID, vec.Receipt.ContractID)
	}
	if vec.Input.Bytes == nil || *vec.Input.Bytes == 0 {
		t.Fatalf("buy_storage: input.bytes is absent or zero, want non-zero")
	}

	contractID := mustHex(t, vec.Input.ContractID)
	key1 := hex.EncodeToString(crypto.Hash(contractID, []byte{0x00, 0x01}))
	key5 := hex.EncodeToString(crypto.Hash(contractID, []byte{0x00, 0x05}))
	wantWitness := []string{key1, key5}
	sort.Strings(wantWitness)
	if !reflect.DeepEqual(vec.WitnessKeys, wantWitness) {
		t.Fatalf("buy_storage: witness_keys = %v, want the 0x0001 and 0x0005 keys ascending %v", vec.WitnessKeys, wantWitness)
	}

	_, purchased, used := quotaLeaf(t, vec)
	sum := new(big.Int).Add(new(big.Int).SetUint64(purchased), new(big.Int).SetUint64(*vec.Input.Bytes))
	overflow := sum.Cmp(new(big.Int).SetUint64(^uint64(0))) > 0

	switch vec.Status {
	case "VALIDATION_FAILED":
		codeHashHex, present := vec.PreState[key1]
		if !present {
			if vec.Receipt.CodeHash != zeroHashHex {
				t.Fatalf("buy_storage: VALIDATION_FAILED without a 0x0001 leaf: receipt.code_hash = %s, want zero", vec.Receipt.CodeHash)
			}
			return
		}
		if vec.Receipt.CodeHash != codeHashHex {
			t.Fatalf("buy_storage: VALIDATION_FAILED: receipt.code_hash = %s, want pre_state's 0x0001 leaf %s", vec.Receipt.CodeHash, codeHashHex)
		}
		if !overflow {
			t.Fatalf("buy_storage: VALIDATION_FAILED with a 0x0001 leaf: purchased %d + bytes %d = %s does not exceed 2^64 - 1", purchased, *vec.Input.Bytes, sum)
		}
	case "SUCCESS":
		codeHashHex, present := vec.PreState[key1]
		if !present {
			t.Fatalf("buy_storage: SUCCESS: pre_state has no 0x0001 leaf")
		}
		if vec.Receipt.CodeHash != codeHashHex {
			t.Fatalf("buy_storage: SUCCESS: receipt.code_hash = %s, want pre_state's 0x0001 leaf %s", vec.Receipt.CodeHash, codeHashHex)
		}
		er, err := codec.DecodeExecutionResult(mustHex(t, vec.ExecutionResultBlobHex))
		if err != nil {
			t.Fatalf("buy_storage: SUCCESS: DecodeExecutionResult: %v", err)
		}
		if len(er.ReturnData) != 0 {
			t.Fatalf("buy_storage: SUCCESS: ExecutionResult.ReturnData = %x, want empty", er.ReturnData)
		}
		if len(er.StateDiff) != 1 {
			t.Fatalf("buy_storage: SUCCESS: ExecutionResult.StateDiff has %d entries, want exactly 1", len(er.StateDiff))
		}
		want := make([]byte, 16)
		binary.BigEndian.PutUint64(want[:8], purchased+*vec.Input.Bytes)
		binary.BigEndian.PutUint64(want[8:], used)
		if hex.EncodeToString(er.StateDiff[0].Key[:]) != key5 || !bytes.Equal(er.StateDiff[0].Value, want) {
			t.Fatalf("buy_storage: SUCCESS: StateDiff[0] = %+v, want {0x0005, purchased+bytes || used %x}", er.StateDiff[0], want)
		}
	default:
		t.Fatalf("buy_storage: status %s has no branch: add one with its first vector", vec.Status)
	}
}

// checkUndeployedTarget is the undeployed_target shape's whole check: a
// kind-0/1 input whose target has no code-hash leaf is VALIDATION_FAILED
// before any module or frame exists, refunds the full deposit, and
// witnesses exactly the target's two code-hash leaves (§10.6, §15, §18.2).
func checkUndeployedTarget(t *testing.T, vec vectorJSON) {
	t.Helper()

	if (vec.InputKind != 0 && vec.InputKind != 1) || vec.Input == nil {
		t.Fatalf("undeployed_target: input_kind = %d, input present = %v, want kind 0 or 1 with an input", vec.InputKind, vec.Input != nil)
	}
	if vec.ModuleWasmHex != "" || vec.CodeHash != "" || vec.InstrumentedCodeHash != "" || vec.InputFrameHex != "" || vec.SourceDataHex != "" || vec.ContractEffectsBlobHex != "" {
		t.Fatalf("undeployed_target: module_wasm_hex/code_hash/instrumented_code_hash/input_frame_hex/source_data_hex/contract_effects_blob_hex must all be empty")
	}
	if vec.PendingMetadataHex != nil {
		t.Fatalf("undeployed_target: pending_metadata_hex = %q, want absent", *vec.PendingMetadataHex)
	}
	if vec.Status != "VALIDATION_FAILED" || vec.GasUsed != 0 || vec.Receipt.AbortCode != 0 {
		t.Fatalf("undeployed_target: status = %s, gas_used = %d, receipt.abort_code = %d, want VALIDATION_FAILED, 0, 0", vec.Status, vec.GasUsed, vec.Receipt.AbortCode)
	}
	if vec.Receipt.ContractID != vec.Input.TargetContract {
		t.Fatalf("undeployed_target: receipt.contract_id = %s, want input.target_contract %s", vec.Receipt.ContractID, vec.Input.TargetContract)
	}
	if vec.Receipt.CodeHash != zeroHashHex {
		t.Fatalf("undeployed_target: receipt.code_hash = %s, want zero", vec.Receipt.CodeHash)
	}

	target := mustHex(t, vec.Input.TargetContract)
	key1 := hex.EncodeToString(crypto.Hash(target, []byte{0x00, 0x01}))
	key2 := hex.EncodeToString(crypto.Hash(target, []byte{0x00, 0x02}))
	for _, k := range []string{key1, key2} {
		if _, ok := vec.PreState[k]; ok {
			t.Fatalf("undeployed_target: pre_state holds the target's leaf %s, want it absent", k)
		}
	}
	wantWitness := []string{key1, key2}
	sort.Strings(wantWitness)
	if !reflect.DeepEqual(vec.WitnessKeys, wantWitness) {
		t.Fatalf("undeployed_target: witness_keys = %v, want the 0x0001 and 0x0002 keys ascending %v", vec.WitnessKeys, wantWitness)
	}

	if vec.Input.DepositAmount == "" || vec.Input.DepositAmount == zeroHashHex {
		t.Fatalf("undeployed_target: input.deposit_amount = %q, want a non-zero deposit", vec.Input.DepositAmount)
	}
	checkCallRefund(t, vec)
}

// checkUndeployedRow is the named invariant of an undeployed_target row:
// its input kind, a payload on a CallL2, a deposit of 1000 in an asset
// withdrawal_caps caps above zero, and an empty pre_state.
func checkUndeployedRow(t *testing.T, vec vectorJSON, kind uint8) {
	t.Helper()

	requireStatus(t, vec, "VALIDATION_FAILED")
	if vec.Input == nil {
		t.Fatalf("%s: input is absent", vec.ID)
	}
	if vec.InputKind != kind {
		t.Fatalf("%s: input_kind = %d, want %d", vec.ID, vec.InputKind, kind)
	}
	if kind == 0 && (vec.Input.PayloadHex == nil || *vec.Input.PayloadHex == "") {
		t.Fatalf("%s: a CallL2 row needs a non-empty payload", vec.ID)
	}
	if new(big.Int).SetBytes(mustHex(t, vec.Input.DepositAmount)).Cmp(big.NewInt(1000)) != 0 {
		t.Fatalf("%s: input.deposit_amount = %s, want 1000", vec.ID, vec.Input.DepositAmount)
	}
	if capOf(t, vec, vec.Input.DepositAsset).Sign() <= 0 {
		t.Fatalf("%s: withdrawal_caps has no cap above zero for deposit_asset %s", vec.ID, vec.Input.DepositAsset)
	}
	if len(vec.PreState) != 0 {
		t.Fatalf("%s: pre_state has %d entries, want none", vec.ID, len(vec.PreState))
	}
}

// checkValidationRejection is the validation_rejection shape's whole
// check: a kind-4 finalize rejected by a module the validator refuses,
// by a validated module that module.Instrument refuses, by pending
// metadata that DecodeContractMetadata rejects, or by staged bytes
// that do not hash to input.code_hash, exactly one of the four, with
// the rule it refuses it by named in rejectionReasons.
func checkValidationRejection(t *testing.T, vec vectorJSON) {
	t.Helper()

	if vec.InputKind != 4 {
		t.Fatalf("validation_rejection: input_kind = %d, want 4", vec.InputKind)
	}
	if vec.Status != "VALIDATION_FAILED" {
		t.Fatalf("validation_rejection: status = %s, want VALIDATION_FAILED", vec.Status)
	}
	if vec.GasUsed != 0 {
		t.Fatalf("validation_rejection: gas_used = %d, want 0", vec.GasUsed)
	}
	if vec.InputFrameHex != "" || vec.SourceDataHex != "" || vec.ContractEffectsBlobHex != "" || vec.InstrumentedCodeHash != "" {
		t.Fatalf("validation_rejection: input_frame_hex/source_data_hex/contract_effects_blob_hex/instrumented_code_hash must all be empty")
	}
	if vec.ModuleWasmHex == "" {
		t.Fatalf("validation_rejection: module_wasm_hex is absent")
	}
	moduleRaw := mustHex(t, vec.ModuleWasmHex)
	moduleHash := hex.EncodeToString(crypto.Hash(moduleRaw))
	if moduleHash != vec.CodeHash {
		t.Fatalf("validation_rejection: sha3(module_wasm_hex) = %s, want code_hash %s", moduleHash, vec.CodeHash)
	}
	if vec.Input == nil {
		t.Fatalf("validation_rejection: input is absent")
	}
	var hashErr error
	if vec.Input.CodeHash != vec.CodeHash {
		hashErr = errAssemblyHash
	}
	if vec.Receipt.ContractID != zeroHashHex || vec.Receipt.CodeHash != zeroHashHex {
		t.Fatalf("validation_rejection: receipt.contract_id = %s, receipt.code_hash = %s, want both zero", vec.Receipt.ContractID, vec.Receipt.CodeHash)
	}
	if len(vec.WitnessKeys) != 0 {
		t.Fatalf("validation_rejection: witness_keys has %d entries, want 0", len(vec.WitnessKeys))
	}
	if len(vec.PreState) != 0 {
		t.Fatalf("validation_rejection: pre_state has %d entries, want 0", len(vec.PreState))
	}

	want, ok := rejectionReasons[vec.ID]
	if !ok {
		t.Fatalf("validation_rejection: %s: add its rule to rejectionReasons", vec.ID)
	}
	if vec.PendingMetadataHex == nil {
		t.Fatalf("validation_rejection: %s: pending_metadata_hex is absent", vec.ID)
	}

	validateErr := module.Validate(moduleRaw, moduleLimits())
	var instrumentErr error
	if validateErr == nil {
		_, instrumentErr = module.Instrument(moduleRaw, moduleLimits())
	}
	_, metadataErr := codec.DecodeContractMetadata(mustHex(t, *vec.PendingMetadataHex))

	var cause error
	causes := 0
	for _, err := range []error{validateErr, instrumentErr, metadataErr, hashErr} {
		if err != nil {
			cause = err
			causes++
		}
	}
	if causes != 1 {
		t.Fatalf("validation_rejection: %s: got Validate=%v Instrument=%v DecodeContractMetadata=%v assemblyHash=%v, want exactly one non-nil", vec.ID, validateErr, instrumentErr, metadataErr, hashErr)
	}
	if !errors.Is(cause, want) {
		t.Fatalf("validation_rejection: %s: cause = %v, want errors.Is(..., %v)", vec.ID, cause, want)
	}
}

// --- N. named per-vector invariants.

// namedInvariants is keyed by vector ID; TestConformanceVectors asserts every key was seen.

// checkN1WitnessBackendTripwire is N1: "probe" is a local key the fixture only ever passes to
// storage_len and never writes, so its derived path is in the witness set only because
// state.Accessor.Len records an absent-key observation -- exactly the entry a mapState-backed
// generator, whose Len records nothing, would drop.
func checkN1WitnessBackendTripwire(t *testing.T, vec vectorJSON) {
	t.Helper()
	if len(vec.WitnessKeys) != 5 {
		t.Fatalf("N1: witness_keys has %d entries, want exactly 5 (the two reserved leaves, the quota leaf EXEC-002's write adds, the written key and the probe; a mapState-backed generator, whose Len records nothing for an absent key, would produce 4)", len(vec.WitnessKeys))
	}
	contractID := mustHex(t, vec.Receipt.ContractID)
	probeKey := hex.EncodeToString(crypto.Hash(contractID, []byte("probe")))
	for _, k := range vec.WitnessKeys {
		if k == probeKey {
			return
		}
	}
	t.Fatalf("N1: witness_keys is missing sha3(contract_id || \"probe\") = %s; a mapState-backed generator would drop it", probeKey)
}

// checkN2DiscardNotVacuous is N2: proves the discard actually discarded
// something, by asserting the returned blob carried a non-empty
// StateDiff and non-empty ReturnData.
func checkN2DiscardNotVacuous(t *testing.T, vec vectorJSON) {
	t.Helper()
	if vec.ContractEffectsBlobHex == "" {
		t.Fatalf("N2: contract_effects_blob_hex is absent")
	}
	blob := mustHex(t, vec.ContractEffectsBlobHex)
	eff, err := codec.DecodeContractEffects(blob)
	if err != nil {
		t.Fatalf("N2: DecodeContractEffects: %v", err)
	}
	if len(eff.StateDiff) == 0 {
		t.Fatalf("N2: returned blob has an empty StateDiff; the discard would be vacuous")
	}
	if len(eff.ReturnData) == 0 {
		t.Fatalf("N2: returned blob has empty ReturnData; the discard would be vacuous")
	}
}

// inlineEffectCharge sums the effect cost of every entry in eff, from
// the gas table alone.
func inlineEffectCharge(eff codec.ContractEffects) uint64 {
	var total uint64
	for _, e := range eff.StateDiff {
		total += module.EffectCost(module.Effect{Kind: module.EffectStateDiff, Bytes: len(e.Value)})
	}
	for _, e := range eff.Events {
		total += module.EffectCost(module.Effect{Kind: module.EffectEvent, Bytes: len(e.Data)})
	}
	for _, m := range eff.Outbox {
		total += module.EffectCost(module.Effect{Kind: module.EffectOutboxMessage, Bytes: len(m.Payload)})
	}
	total += module.EffectCost(module.Effect{Kind: module.EffectReturnData, Bytes: len(eff.ReturnData)})
	return total
}

// checkPathGas recomputes gas_used from the gas table alone:
// module.InitialMemoryCost(pages), plus module.OpcodeCost for each
// original opcode on the executed path, plus hostGas, plus the effect
// charge over the returned blob, computed inline from
// module.EffectCost. It must not call plugins/wasm's effectCharge,
// which is the generator's own path.
func checkPathGas(t *testing.T, vec vectorJSON, pages uint32, hostGas uint64, path []byte) {
	t.Helper()
	if vec.Status != "SUCCESS" && vec.Status != "STORAGE_QUOTA_EXCEEDED" {
		t.Fatalf("checkPathGas: status = %s, want SUCCESS or STORAGE_QUOTA_EXCEEDED", vec.Status)
	}
	if vec.ContractEffectsBlobHex == "" {
		t.Fatalf("checkPathGas: contract_effects_blob_hex is absent")
	}
	eff, err := codec.DecodeContractEffects(mustHex(t, vec.ContractEffectsBlobHex))
	if err != nil {
		t.Fatalf("checkPathGas: DecodeContractEffects: %v", err)
	}

	total := module.InitialMemoryCost(pages)
	for _, op := range path {
		cost, ok := module.OpcodeCost(module.Opcode(op))
		if !ok {
			t.Fatalf("checkPathGas: no gas-table cost for opcode 0x%02X", op)
		}
		total += cost
	}
	total += hostGas

	total += inlineEffectCharge(eff)

	if vec.GasUsed != total {
		t.Fatalf("checkPathGas: gas_used = %d, want %d (InitialMemoryCost(%d) + path opcodes + hostGas %d + effect charge)", vec.GasUsed, total, pages, hostGas)
	}
}

// checkEXEC006DescendingLocalKeys is INV-1: the returned blob's local keys, hashed inline, are
// strictly descending, while the ExecutionResult's StateDiff keys are strictly ascending -- so
// the ordering is the runtime's, not the contract's.
func checkEXEC006DescendingLocalKeys(t *testing.T, vec vectorJSON) {
	t.Helper()
	eff, err := codec.DecodeContractEffects(mustHex(t, vec.ContractEffectsBlobHex))
	if err != nil {
		t.Fatalf("INV-1: DecodeContractEffects: %v", err)
	}
	contractID := mustHex(t, vec.Receipt.ContractID)
	derived := make([][]byte, len(eff.StateDiff))
	for i, e := range eff.StateDiff {
		derived[i] = crypto.Hash(contractID, e.LocalKey)
	}
	for i := 1; i < len(derived); i++ {
		if bytes.Compare(derived[i], derived[i-1]) >= 0 {
			t.Fatalf("INV-1: blob local keys are not strictly descending by derived key at index %d", i)
		}
	}

	er, err := codec.DecodeExecutionResult(mustHex(t, vec.ExecutionResultBlobHex))
	if err != nil {
		t.Fatalf("INV-1: DecodeExecutionResult: %v", err)
	}
	for i := 1; i < len(er.StateDiff); i++ {
		if bytes.Compare(er.StateDiff[i].Key[:], er.StateDiff[i-1].Key[:]) <= 0 {
			t.Fatalf("INV-1: ExecutionResult StateDiff keys are not strictly ascending at index %d", i)
		}
	}
}

// checkEXEC012AbsentRead is INV-2: the key the fixture reads is in witness_keys and absent from
// pre_state, then checkPathGas.
func checkEXEC012AbsentRead(t *testing.T, vec vectorJSON) {
	t.Helper()
	contractID := mustHex(t, vec.Receipt.ContractID)
	key := hex.EncodeToString(crypto.Hash(contractID, []byte("missing")))
	var inWitness bool
	for _, k := range vec.WitnessKeys {
		if k == key {
			inWitness = true
			break
		}
	}
	if !inWitness {
		t.Fatalf("INV-2: witness_keys is missing sha3(contract_id || \"missing\") = %s", key)
	}
	if _, present := vec.PreState[key]; present {
		t.Fatalf("INV-2: pre_state has an entry for %s, want absent", key)
	}
	checkPathGas(t, vec, 1, module.HostCost(module.HostStorageRead, 0),
		[]byte{0x41, 0x41, 0x41, 0x41, 0x10, 0x42, 0x52, 0x04, 0x42, 0x0B})
}

// checkCVREAD2PresentRead is INV-3: the key the fixture reads is in witness_keys and present in
// pre_state with a value longer than the fixture's 4-byte out buffer, then checkPathGas.
func checkCVREAD2PresentRead(t *testing.T, vec vectorJSON) {
	t.Helper()
	contractID := mustHex(t, vec.Receipt.ContractID)
	key := hex.EncodeToString(crypto.Hash(contractID, []byte("value8")))
	var inWitness bool
	for _, k := range vec.WitnessKeys {
		if k == key {
			inWitness = true
			break
		}
	}
	if !inWitness {
		t.Fatalf("INV-3: witness_keys is missing sha3(contract_id || \"value8\") = %s", key)
	}
	valueHex, present := vec.PreState[key]
	if !present {
		t.Fatalf("INV-3: pre_state has no entry for %s, want present", key)
	}
	if len(mustHex(t, valueHex)) <= 4 {
		t.Fatalf("INV-3: pre_state value for %s has length %d, want > 4 (the fixture's out buffer)", key, len(mustHex(t, valueHex)))
	}
	checkPathGas(t, vec, 1, module.HostCost(module.HostStorageRead, 8),
		[]byte{0x41, 0x41, 0x41, 0x41, 0x10, 0x42, 0x52, 0x04, 0x42, 0x0B})
}

// checkEXECDEP3ClaimExceedsDeposit is INV-4: the decoded blob's ClaimedDeposit is strictly
// greater than input.deposit_amount, so the claim-bounds breach this vector freezes is really
// present.
func checkEXECDEP3ClaimExceedsDeposit(t *testing.T, vec vectorJSON) {
	t.Helper()
	if vec.ContractEffectsBlobHex == "" {
		t.Fatalf("INV-4: contract_effects_blob_hex is absent")
	}
	eff, err := codec.DecodeContractEffects(mustHex(t, vec.ContractEffectsBlobHex))
	if err != nil {
		t.Fatalf("INV-4: DecodeContractEffects: %v", err)
	}
	if vec.Input == nil {
		t.Fatalf("INV-4: input is absent")
	}
	deposit := mustHex(t, vec.Input.DepositAmount)
	if bytes.Compare(eff.ClaimedDeposit[:], deposit) <= 0 {
		t.Fatalf("INV-4: ClaimedDeposit %x <= deposit_amount %x, want strictly greater", eff.ClaimedDeposit, deposit)
	}
}

// checkEXECDEP4ZeroDepositNonZeroClaim is INV-5: receipt.outbox_count is 0, and the decoded
// blob's ClaimedDeposit is non-zero while input.deposit_amount is zero -- the zero-remainder
// no-refund path is witnessed together with the breach that caused the failure.
func checkEXECDEP4ZeroDepositNonZeroClaim(t *testing.T, vec vectorJSON) {
	t.Helper()
	if vec.Receipt.OutboxCount != 0 {
		t.Fatalf("INV-5: receipt.outbox_count = %d, want 0", vec.Receipt.OutboxCount)
	}
	if vec.Input == nil || vec.Input.DepositAmount != zeroHashHex {
		t.Fatalf("INV-5: input.deposit_amount is not zero")
	}
	if vec.ContractEffectsBlobHex == "" {
		t.Fatalf("INV-5: contract_effects_blob_hex is absent")
	}
	eff, err := codec.DecodeContractEffects(mustHex(t, vec.ContractEffectsBlobHex))
	if err != nil {
		t.Fatalf("INV-5: DecodeContractEffects: %v", err)
	}
	if eff.ClaimedDeposit == (codec.U256{}) {
		t.Fatalf("INV-5: ClaimedDeposit is zero, want non-zero")
	}
}

// checkCVOUTBOX1UndecodableBlob is INV-6: the returned blob fails to decode with
// ErrNonEmptyWithdrawalPayload.
func checkCVOUTBOX1UndecodableBlob(t *testing.T, vec vectorJSON) {
	t.Helper()
	if vec.ContractEffectsBlobHex == "" {
		t.Fatalf("INV-6: contract_effects_blob_hex is absent")
	}
	_, err := codec.DecodeContractEffects(mustHex(t, vec.ContractEffectsBlobHex))
	if !errors.Is(err, codec.ErrNonEmptyWithdrawalPayload) {
		t.Fatalf("INV-6: DecodeContractEffects err = %v, want errors.Is(..., ErrNonEmptyWithdrawalPayload)", err)
	}
}

// checkCVROOTS1RootsBlob is INV-7: the blob decodes as an ExecutionResult (it really is a
// roots-carrying one) but not as a ContractEffects.
func checkCVROOTS1RootsBlob(t *testing.T, vec vectorJSON) {
	t.Helper()
	if vec.ContractEffectsBlobHex == "" {
		t.Fatalf("INV-7: contract_effects_blob_hex is absent")
	}
	raw := mustHex(t, vec.ContractEffectsBlobHex)
	if _, err := codec.DecodeExecutionResult(raw); err != nil {
		t.Fatalf("INV-7: DecodeExecutionResult: %v, want success", err)
	}
	if _, err := codec.DecodeContractEffects(raw); err == nil {
		t.Fatalf("INV-7: DecodeContractEffects succeeded, want failure")
	}
}

// checkCVDUPKEY1DuplicateLocalKeys is INV-8: the decoded blob has two entries whose LocalKeys are
// equal.
func checkCVDUPKEY1DuplicateLocalKeys(t *testing.T, vec vectorJSON) {
	t.Helper()
	eff, err := codec.DecodeContractEffects(mustHex(t, vec.ContractEffectsBlobHex))
	if err != nil {
		t.Fatalf("INV-8: DecodeContractEffects: %v", err)
	}
	for i := 0; i < len(eff.StateDiff); i++ {
		for j := i + 1; j < len(eff.StateDiff); j++ {
			if bytes.Equal(eff.StateDiff[i].LocalKey, eff.StateDiff[j].LocalKey) {
				return
			}
		}
	}
	t.Fatalf("INV-8: no two StateDiff entries share a LocalKey")
}

// checkCVUNAUTH1ReservedKeyDiscardNotVacuous is INV-9: the decoded blob
// has a reserved-key entry and a second, non-reserved entry, and a
// non-empty ReturnData, so the discard is not vacuous.
func checkCVUNAUTH1ReservedKeyDiscardNotVacuous(t *testing.T, vec vectorJSON) {
	t.Helper()
	eff, err := codec.DecodeContractEffects(mustHex(t, vec.ContractEffectsBlobHex))
	if err != nil {
		t.Fatalf("INV-9: DecodeContractEffects: %v", err)
	}
	var reservedFound, otherFound bool
	for _, e := range eff.StateDiff {
		isReserved := false
		for _, rk := range reservedKeys {
			if bytes.Equal(e.LocalKey, rk) {
				isReserved = true
				break
			}
		}
		if isReserved {
			reservedFound = true
		} else {
			otherFound = true
		}
	}
	if !reservedFound {
		t.Fatalf("INV-9: no reserved-key StateDiff entry found")
	}
	if !otherFound {
		t.Fatalf("INV-9: no second, non-reserved StateDiff entry found")
	}
	if len(eff.ReturnData) == 0 {
		t.Fatalf("INV-9: ReturnData is empty, want non-empty")
	}
}

// checkCVRTLRETURN1ReturnDataOversized is INV-10: the decoded blob's ReturnData is longer than
// codec.MaxReturnData.
func checkCVRTLRETURN1ReturnDataOversized(t *testing.T, vec vectorJSON) {
	t.Helper()
	eff, err := codec.DecodeContractEffects(mustHex(t, vec.ContractEffectsBlobHex))
	if err != nil {
		t.Fatalf("INV-10: DecodeContractEffects: %v", err)
	}
	if len(eff.ReturnData) <= codec.MaxReturnData {
		t.Fatalf("INV-10: len(ReturnData) = %d, want > MaxReturnData %d", len(eff.ReturnData), codec.MaxReturnData)
	}
}

// checkCVRTLRESULT1ResultTooLargeFromRefund is INV-11: the decoded blob's ReturnData is not
// longer than codec.MaxReturnData (so the other bound is not what tripped), the contract-only
// ExecutionResult length computed inline is <= MaxExecutionResult, and input.deposit_amount is
// non-zero (so a refund exists to push it over).
func checkCVRTLRESULT1ResultTooLargeFromRefund(t *testing.T, vec vectorJSON) {
	t.Helper()
	eff, err := codec.DecodeContractEffects(mustHex(t, vec.ContractEffectsBlobHex))
	if err != nil {
		t.Fatalf("INV-11: DecodeContractEffects: %v", err)
	}
	if len(eff.ReturnData) > codec.MaxReturnData {
		t.Fatalf("INV-11: len(ReturnData) = %d > codec.MaxReturnData %d, want <=", len(eff.ReturnData), codec.MaxReturnData)
	}
	// A contract-only ExecutionResult with N state entries, no events,
	// no outbox, empty ReturnData encodes to 122 + Σ(36+len(v)) bytes
	// (the fixed prefix/suffix plus one 36-byte-overhead StateDiffEntry
	// per entry). The runtime's quota entry adds 36 + 16.
	contractOnly := 122 + 36 + 16
	for _, e := range eff.StateDiff {
		contractOnly += 36 + len(e.Value)
	}
	if contractOnly > MaxExecutionResult {
		t.Fatalf("INV-11: contract-only ExecutionResult length %d > MaxExecutionResult %d, want <=", contractOnly, MaxExecutionResult)
	}
	if vec.Input == nil || vec.Input.DepositAmount == "" || vec.Input.DepositAmount == zeroHashHex {
		t.Fatalf("INV-11: input.deposit_amount is zero, want non-zero (a refund must exist)")
	}
}

// checkOutOfGasBase is the shared assertion every CV-OOG-* named
// invariant starts with: status OUT_OF_GAS and gas_used equal to the
// vector's own plasma_limit.
func checkOutOfGasBase(t *testing.T, vec vectorJSON) {
	t.Helper()
	if vec.Status != "OUT_OF_GAS" {
		t.Fatalf("status = %s, want OUT_OF_GAS", vec.Status)
	}
	if vec.Input == nil || vec.Input.PlasmaLimit == nil {
		t.Fatalf("input.plasma_limit is absent")
	}
	if vec.GasUsed != *vec.Input.PlasmaLimit {
		t.Fatalf("gas_used = %d, want plasma_limit %d", vec.GasUsed, *vec.Input.PlasmaLimit)
	}
}

// checkCVOOG1 is INV-12: the shared OUT_OF_GAS base, then no returned blob.
func checkCVOOG1(t *testing.T, vec vectorJSON) {
	t.Helper()
	checkOutOfGasBase(t, vec)
	if vec.ContractEffectsBlobHex != "" {
		t.Fatalf("INV-12: contract_effects_blob_hex is present, want absent")
	}
}

// checkCVOOG2 is INV-13: the shared OUT_OF_GAS base alone.
func checkCVOOG2(t *testing.T, vec vectorJSON) {
	t.Helper()
	checkOutOfGasBase(t, vec)
}

// checkCVOOG3 is INV-14: the shared OUT_OF_GAS base alone.
func checkCVOOG3(t *testing.T, vec vectorJSON) {
	t.Helper()
	checkOutOfGasBase(t, vec)
}

// checkCVOOG4 is INV-15: the shared OUT_OF_GAS base, then a returned blob whose StateDiff is
// non-empty -- the charge it could not pay.
func checkCVOOG4(t *testing.T, vec vectorJSON) {
	t.Helper()
	checkOutOfGasBase(t, vec)
	if vec.ContractEffectsBlobHex == "" {
		t.Fatalf("INV-15: contract_effects_blob_hex is absent, want present")
	}
	eff, err := codec.DecodeContractEffects(mustHex(t, vec.ContractEffectsBlobHex))
	if err != nil {
		t.Fatalf("INV-15: DecodeContractEffects: %v", err)
	}
	if len(eff.StateDiff) == 0 {
		t.Fatalf("INV-15: StateDiff is empty, want non-empty (the charge it could not pay)")
	}
}

// checkCVSEGBRIF1 is INV-16: checkPathGas alone, over the taken-br_if executed path.
func checkCVSEGBRIF1(t *testing.T, vec vectorJSON) {
	checkPathGas(t, vec, 1, 0, []byte{0x02, 0x41, 0x0D, 0x42, 0x0B})
}

// checkCVSEGIF1 is INV-17: checkPathGas alone, over the false-if executed path.
func checkCVSEGIF1(t *testing.T, vec vectorJSON) {
	checkPathGas(t, vec, 1, 0, []byte{0x41, 0x04, 0x42, 0x0B})
}

// checkCVSEGAFTEREND1 is INV-18: checkPathGas alone, over the branch-landing-after-end executed
// path.
func checkCVSEGAFTEREND1(t *testing.T, vec vectorJSON) {
	checkPathGas(t, vec, 1, 0, []byte{0x02, 0x02, 0x0C, 0x42, 0x0B})
}

// checkCVSEGOUTER1 is INV-19: checkPathGas alone, over the branch-to-outermost-label executed
// path.
func checkCVSEGOUTER1(t *testing.T, vec vectorJSON) {
	checkPathGas(t, vec, 1, 0, []byte{0x42, 0x0C})
}

// checkCVGROW1 is INV-20: checkPathGas alone; memory.grow contributes its base opcode cost only.
func checkCVGROW1(t *testing.T, vec vectorJSON) {
	checkPathGas(t, vec, 1, 0, []byte{0x41, 0x40, 0x41, 0x47, 0x04, 0x42, 0x0B})
}

// checkCVASSETSBase decodes contract_effects_blob_hex and asserts the common facts CV-ASSETS-1
// and CV-ASSETS-2 both pin: attribution, no reserved local key, the claim bound, and the u256
// half-range bound on every withdrawal amount. It returns the decoded blob's withdrawal assets,
// in blob order, for the caller's own distinct-asset-count assertion.
func checkCVASSETSBase(t *testing.T, vec vectorJSON) []codec.AssetID {
	t.Helper()
	if vec.ContractEffectsBlobHex == "" {
		t.Fatalf("CV-ASSETS: contract_effects_blob_hex is absent")
	}
	eff, err := codec.DecodeContractEffects(mustHex(t, vec.ContractEffectsBlobHex))
	if err != nil {
		t.Fatalf("CV-ASSETS: DecodeContractEffects: %v", err)
	}
	if eff.GlobalInputIndex != vec.GlobalInputIndex {
		t.Fatalf("CV-ASSETS: blob GlobalInputIndex = %d, want global_input_index %d", eff.GlobalInputIndex, vec.GlobalInputIndex)
	}
	reserved := map[string]bool{"0001": true, "0002": true, "0003": true, "0004": true, "0005": true}
	for _, e := range eff.StateDiff {
		if reserved[hex.EncodeToString(e.LocalKey)] {
			t.Fatalf("CV-ASSETS: StateDiff uses reserved local key %x", e.LocalKey)
		}
	}
	if vec.Input == nil || vec.Input.DepositAmount == "" {
		t.Fatalf("CV-ASSETS: input.deposit_amount is absent")
	}
	depositAmount := new(big.Int).SetBytes(mustHex(t, vec.Input.DepositAmount))
	claimed := new(big.Int).SetBytes(eff.ClaimedDeposit[:])
	if claimed.Cmp(depositAmount) > 0 {
		t.Fatalf("CV-ASSETS: ClaimedDeposit %s exceeds input.deposit_amount %s", claimed, depositAmount)
	}
	cap := new(big.Int).Lsh(big.NewInt(1), 255)
	contractID := mustHex(t, vec.Receipt.ContractID)
	var assets []codec.AssetID
	for _, m := range eff.Outbox {
		if !bytes.Equal(m.SourceContract[:], contractID) {
			t.Fatalf("CV-ASSETS: outbox message SourceContract = %x, want receipt.contract_id %s", m.SourceContract, vec.Receipt.ContractID)
		}
		w, ok := m.Target.(codec.L1Withdrawal)
		if !ok {
			continue
		}
		amt := new(big.Int).SetBytes(w.Amount[:])
		if amt.Cmp(cap) > 0 {
			t.Fatalf("CV-ASSETS: withdrawal amount %s exceeds 2^255", amt)
		}
		assets = append(assets, w.Asset)
	}
	return assets
}

func distinctAssetCount(assets []codec.AssetID) int {
	seen := map[codec.AssetID]bool{}
	for _, a := range assets {
		seen[a] = true
	}
	return len(seen)
}

// checkCVASSETS1 pins the RUNTIME_FAULT to the step-5a footprint: 17 distinct withdrawal
// assets, one over MaxAssetsPerBatch.
func checkCVASSETS1(t *testing.T, vec vectorJSON) {
	t.Helper()
	assets := checkCVASSETSBase(t, vec)
	if n := distinctAssetCount(assets); n != 17 {
		t.Fatalf("CV-ASSETS-1: %d distinct withdrawal assets, want 17", n)
	}
	want := noConditions()
	want.FootprintOver = true
	checkConditions(t, vec, "RUNTIME_FAULT", want)
}

// checkCVASSETS2 pins the RUNTIME_FAULT to the step-5a footprint: 16 distinct withdrawal
// assets, none equal to the non-zero-amount deposit asset, so the deposit asset is the 17th
// footprint entry.
func checkCVASSETS2(t *testing.T, vec vectorJSON) {
	t.Helper()
	assets := checkCVASSETSBase(t, vec)
	if n := distinctAssetCount(assets); n != 16 {
		t.Fatalf("CV-ASSETS-2: %d distinct withdrawal assets, want 16", n)
	}
	depositAsset := vec.Input.DepositAsset
	for _, a := range assets {
		if hex.EncodeToString(a[:]) == depositAsset {
			t.Fatalf("CV-ASSETS-2: a withdrawal asset equals the deposit asset %s, want all 16 distinct from it", depositAsset)
		}
	}
	want := noConditions()
	want.FootprintOver = true
	checkConditions(t, vec, "RUNTIME_FAULT", want)
}

// checkCVQUOTA4ZeroDelta is the CV-QUOTA-4 invariant: the returned blob's StateDiff is
// non-empty yet nets to a zero quota delta, so step 6a reads no quota leaf.
func checkCVQUOTA4ZeroDelta(t *testing.T, vec vectorJSON) {
	t.Helper()
	eff, err := codec.DecodeContractEffects(mustHex(t, vec.ContractEffectsBlobHex))
	if err != nil {
		t.Fatalf("CV-QUOTA-4: DecodeContractEffects: %v", err)
	}
	if len(eff.StateDiff) == 0 {
		t.Fatalf("CV-QUOTA-4: StateDiff is empty, want non-empty")
	}
	if delta := blobQuotaDelta(t, vec); delta != 0 {
		t.Fatalf("CV-QUOTA-4: quota delta = %d, want 0", delta)
	}
}

// checkCVQUOTA5ReservedQuotaKey is the CV-QUOTA-5 invariant: the returned blob's StateDiff
// carries the reserved local key 0x0005.
func checkCVQUOTA5ReservedQuotaKey(t *testing.T, vec vectorJSON) {
	t.Helper()
	eff, err := codec.DecodeContractEffects(mustHex(t, vec.ContractEffectsBlobHex))
	if err != nil {
		t.Fatalf("CV-QUOTA-5: DecodeContractEffects: %v", err)
	}
	for _, e := range eff.StateDiff {
		if bytes.Equal(e.LocalKey, []byte{0x00, 0x05}) {
			return
		}
	}
	t.Fatalf("CV-QUOTA-5: StateDiff carries no local key 0x0005")
}

// capOf returns the withdrawal_caps entry for asset (hex), 0 when absent.
func capOf(t *testing.T, vec vectorJSON, asset string) *big.Int {
	t.Helper()
	v, ok := vec.WithdrawalCaps[asset]
	if !ok {
		return new(big.Int)
	}
	return new(big.Int).SetBytes(mustHex(t, v))
}

// withdrawalsOf returns the L1Withdrawal messages of vec's ContractEffects blob, in order.
func withdrawalsOf(t *testing.T, vec vectorJSON) []codec.L1Withdrawal {
	t.Helper()
	eff, err := codec.DecodeContractEffects(mustHex(t, vec.ContractEffectsBlobHex))
	if err != nil {
		t.Fatalf("%s: DecodeContractEffects: %v", vec.ID, err)
	}
	var out []codec.L1Withdrawal
	for _, m := range eff.Outbox {
		if w, ok := m.Target.(codec.L1Withdrawal); ok {
			out = append(out, w)
		}
	}
	return out
}

// withdrawalTotals sums vec's withdrawals per asset, keyed by asset hex.
func withdrawalTotals(t *testing.T, vec vectorJSON) map[string]*big.Int {
	t.Helper()
	totals := map[string]*big.Int{}
	for _, w := range withdrawalsOf(t, vec) {
		asset := hex.EncodeToString(w.Asset[:])
		if totals[asset] == nil {
			totals[asset] = new(big.Int)
		}
		totals[asset].Add(totals[asset], new(big.Int).SetBytes(w.Amount[:]))
	}
	return totals
}

// C7. Withdrawal caps (call, SUCCESS): each asset's withdrawals sum to
// at most its withdrawal_caps value, 0 when absent.
func checkCallWithdrawalCaps(t *testing.T, vec vectorJSON) {
	t.Helper()
	for asset, total := range withdrawalTotals(t, vec) {
		if limit := capOf(t, vec, asset); total.Cmp(limit) > 0 {
			t.Fatalf("C7: asset %s withdraws %s, above its withdrawal_caps value %s", asset, total, limit)
		}
	}
}

// capWithdrawalsInOneAsset returns the vector's withdrawals after checking there are at least
// min of them, all in one asset, and returns that asset (hex) and its total.
func capWithdrawalsInOneAsset(t *testing.T, vec vectorJSON, min int) (string, *big.Int, []*big.Int) {
	t.Helper()
	ws := withdrawalsOf(t, vec)
	if len(ws) < min {
		t.Fatalf("%s: %d withdrawal messages, want at least %d", vec.ID, len(ws), min)
	}
	asset := hex.EncodeToString(ws[0].Asset[:])
	total := new(big.Int)
	amounts := make([]*big.Int, len(ws))
	for i, w := range ws {
		if hex.EncodeToString(w.Asset[:]) != asset {
			t.Fatalf("%s: withdrawal %d is in asset %x, want every withdrawal in asset %s", vec.ID, i, w.Asset, asset)
		}
		amounts[i] = new(big.Int).SetBytes(w.Amount[:])
		total.Add(total, amounts[i])
	}
	return asset, total, amounts
}

// checkCVCAP1 pins CV-CAP-1 as the boundary: the total equals a non-zero cap.
func checkCVCAP1(t *testing.T, vec vectorJSON) {
	t.Helper()
	if vec.Status != "SUCCESS" {
		t.Fatalf("CV-CAP-1: status %s, want SUCCESS", vec.Status)
	}
	asset, total, _ := capWithdrawalsInOneAsset(t, vec, 2)
	limit := capOf(t, vec, asset)
	if limit.Sign() == 0 || total.Cmp(limit) != 0 {
		t.Fatalf("CV-CAP-1: total %s, cap %s; want a non-zero cap equal to the total", total, limit)
	}
}

// checkCVCAP2 pins the RUNTIME_FAULT to the cap: each message fits the cap alone and the total
// is one above it.
func checkCVCAP2(t *testing.T, vec vectorJSON) {
	t.Helper()
	checkCVASSETSBase(t, vec)
	asset, total, amounts := capWithdrawalsInOneAsset(t, vec, 2)
	if _, ok := vec.WithdrawalCaps[asset]; !ok {
		t.Fatalf("CV-CAP-2: asset %s is absent from withdrawal_caps", asset)
	}
	limit := capOf(t, vec, asset)
	for i, a := range amounts {
		if a.Cmp(limit) > 0 {
			t.Fatalf("CV-CAP-2: withdrawal %d amount %s exceeds the cap %s alone", i, a, limit)
		}
	}
	if want := new(big.Int).Add(limit, big.NewInt(1)); total.Cmp(want) != 0 {
		t.Fatalf("CV-CAP-2: total %s, want the cap plus 1 (%s)", total, want)
	}
	if new(big.Int).SetBytes(mustHex(t, vec.Input.DepositAmount)).Sign() == 0 {
		t.Fatalf("CV-CAP-2: input.deposit_amount is zero")
	}
}

// checkCVCAP3 pins the RUNTIME_FAULT to an unconfigured asset: one non-zero withdrawal in an
// asset absent from withdrawal_caps.
func checkCVCAP3(t *testing.T, vec vectorJSON) {
	t.Helper()
	checkCVASSETSBase(t, vec)
	ws := withdrawalsOf(t, vec)
	if len(ws) != 1 {
		t.Fatalf("CV-CAP-3: %d withdrawal messages, want exactly 1", len(ws))
	}
	if new(big.Int).SetBytes(ws[0].Amount[:]).Sign() == 0 {
		t.Fatalf("CV-CAP-3: the withdrawal amount is zero, want non-zero")
	}
	if _, ok := vec.WithdrawalCaps[hex.EncodeToString(ws[0].Asset[:])]; ok {
		t.Fatalf("CV-CAP-3: asset %x is present in withdrawal_caps, want it absent", ws[0].Asset)
	}
	if len(vec.WithdrawalCaps) == 0 {
		t.Fatalf("CV-CAP-3: withdrawal_caps is empty, want a non-empty table")
	}
	if new(big.Int).SetBytes(mustHex(t, vec.Input.DepositAmount)).Sign() == 0 {
		t.Fatalf("CV-CAP-3: input.deposit_amount is zero")
	}
}

// checkCVCAP4 pins SUCCESS to a zero-amount withdrawal in an asset absent from withdrawal_caps.
func checkCVCAP4(t *testing.T, vec vectorJSON) {
	t.Helper()
	if vec.Status != "SUCCESS" {
		t.Fatalf("CV-CAP-4: status %s, want SUCCESS", vec.Status)
	}
	ws := withdrawalsOf(t, vec)
	if len(ws) != 1 {
		t.Fatalf("CV-CAP-4: %d withdrawal messages, want exactly 1", len(ws))
	}
	if new(big.Int).SetBytes(ws[0].Amount[:]).Sign() != 0 {
		t.Fatalf("CV-CAP-4: the withdrawal amount is non-zero, want 0")
	}
	if _, ok := vec.WithdrawalCaps[hex.EncodeToString(ws[0].Asset[:])]; ok {
		t.Fatalf("CV-CAP-4: asset %x is present in withdrawal_caps, want it absent", ws[0].Asset)
	}
	if len(vec.WithdrawalCaps) == 0 {
		t.Fatalf("CV-CAP-4: withdrawal_caps is empty, want a non-empty table")
	}
}

// threeAssetTotals returns the totals of a vector that withdraws exactly three assets, all present
// in withdrawal_caps, as the three asset hex keys sorted ascending, and the assets in outbox
// first-appearance order. The first-appearance order must not be ascending, so an implementation
// that reads the caps in sorted order but applies them in outbox order is told apart from one that
// matches by asset. Some asset must also repeat with another asset between its withdrawals, so a
// cap list fetched per withdrawal message is told apart from one fetched per distinct asset.
func threeAssetTotals(t *testing.T, vec vectorJSON) (keys, seen []string, totals map[string]*big.Int) {
	t.Helper()
	totals = withdrawalTotals(t, vec)
	if len(totals) != 3 {
		t.Fatalf("%s: %d assets withdrawn, want exactly 3", vec.ID, len(totals))
	}
	for a, total := range totals {
		if _, ok := vec.WithdrawalCaps[a]; !ok {
			t.Fatalf("%s: asset %s is absent from withdrawal_caps", vec.ID, a)
		}
		if total.Sign() == 0 {
			t.Fatalf("%s: asset %s total is zero, want non-zero", vec.ID, a)
		}
		keys = append(keys, a)
	}
	sort.Strings(keys)
	var order []string
	for _, w := range withdrawalsOf(t, vec) {
		a := hex.EncodeToString(w.Asset[:])
		order = append(order, a)
		if !slices.Contains(seen, a) {
			seen = append(seen, a)
		}
	}
	if sort.StringsAreSorted(seen) {
		t.Fatalf("%s: the outbox mentions the assets in ascending order %v, want a non-ascending first-appearance order", vec.ID, seen)
	}
	repeats := false
	for i, a := range order {
		for j := i + 2; j < len(order); j++ {
			if order[j] == a && order[i+1] != a {
				repeats = true
			}
		}
	}
	if !repeats {
		t.Fatalf("%s: no asset repeats with another asset between its withdrawals in %v", vec.ID, order)
	}
	return keys, seen, totals
}

// checkCVCAP5 pins SUCCESS to three assets, each totalling exactly its own cap, with repeated
// assets in a first-appearance order that is neither ascending nor descending, so a cap list
// fetched in a reversed order is told apart from one fetched by asset, and a repeat among the first as many
// withdrawals as there are assets, so a cap list requested per withdrawal and applied to the
// distinct assets in either order is told apart. In ascending asset order the first cap is the
// strict maximum and the middle cap the strict minimum, so the first, last, maximum and minimum
// cap each differ from the others. Applying any one of them to every asset, or the sum of the
// caps, then either faults this vector or leaves it to CV-CAP-6. Two of the assets differ only
// in byte 22 and two only in byte 31, so a total keyed by the low 8 bytes merges the first pair,
// a total keyed by the high ZTS bytes merges the second, and with every total equal to its cap
// either merge faults.
func checkCVCAP5(t *testing.T, vec vectorJSON) {
	t.Helper()
	if vec.Status != "SUCCESS" {
		t.Fatalf("CV-CAP-5: status %s, want SUCCESS", vec.Status)
	}
	keys, seen, totals := threeAssetTotals(t, vec)
	distinct := map[string]bool{}
	for _, w := range withdrawalsOf(t, vec)[:len(seen)] {
		distinct[hex.EncodeToString(w.Asset[:])] = true
	}
	if slices.IsSortedFunc(seen, func(x, y string) int { return strings.Compare(y, x) }) {
		t.Fatalf("CV-CAP-5: the outbox mentions the assets in descending order %v, want an order that is neither ascending nor descending", seen)
	}
	if len(distinct) == len(seen) {
		t.Fatalf("CV-CAP-5: the first %d withdrawals are all distinct assets, want a repeat among them", len(seen))
	}
	first, mid, last := capOf(t, vec, keys[0]), capOf(t, vec, keys[1]), capOf(t, vec, keys[2])
	for _, k := range keys {
		if limit := capOf(t, vec, k); limit.Sign() == 0 || totals[k].Cmp(limit) != 0 {
			t.Fatalf("CV-CAP-5: asset %s total %s, cap %s; want a non-zero cap equal to the total", k, totals[k], limit)
		}
	}
	if first.Cmp(last) <= 0 || last.Cmp(mid) <= 0 {
		t.Fatalf("CV-CAP-5: caps %s, %s, %s in ascending asset order; want the first the largest and the middle the smallest", first, mid, last)
	}
	sum := new(big.Int).Add(totals[keys[0]], totals[keys[1]])
	if sum.Add(sum, totals[keys[2]]); sum.Cmp(first) <= 0 {
		t.Fatalf("CV-CAP-5: the summed totals %s do not exceed the largest cap %s", sum, first)
	}
	// differsOnlyAt reports whether two asset keys differ in byte pos and in no other byte.
	differsOnlyAt := func(x, y string, pos int) bool {
		bx, by := mustHex(t, x), mustHex(t, y)
		for i := range bx {
			if (bx[i] != by[i]) != (i == pos) {
				return false
			}
		}
		return true
	}
	for _, pos := range []int{22, 31} {
		found := false
		for i := range keys {
			for j := i + 1; j < len(keys); j++ {
				found = found || differsOnlyAt(keys[i], keys[j], pos)
			}
		}
		if !found {
			t.Fatalf("CV-CAP-5: no pair of assets differs only in byte %d", pos)
		}
	}
}

// checkCVCAP6 pins the RUNTIME_FAULT to the middle asset in both ascending and outbox
// first-appearance order: its total alone is over its own cap while each of its withdrawals is
// within the cap, by an amount both other caps cover, and the three totals together stay within
// the sum of the caps. The last withdrawal is of another asset and the over-cap asset does not
// have the largest total, so a check of only the last withdrawal's asset or only the largest-total
// asset passes it only by missing the fault. The over-cap asset has exactly two withdrawals of
// equal amount, so a total that counts identical withdrawals once passes it only by missing the
// fault.
func checkCVCAP6(t *testing.T, vec vectorJSON) {
	t.Helper()
	checkCVASSETSBase(t, vec)
	keys, seen, totals := threeAssetTotals(t, vec)
	if seen[1] != keys[1] {
		t.Fatalf("CV-CAP-6: the over-cap asset %s is not in the middle of the first-appearance order %v", keys[1], seen)
	}
	for _, w := range withdrawalsOf(t, vec) {
		if a := hex.EncodeToString(w.Asset[:]); a == keys[1] && new(big.Int).SetBytes(w.Amount[:]).Cmp(capOf(t, vec, a)) > 0 {
			t.Fatalf("CV-CAP-6: a single withdrawal of %s exceeds its cap %s, want each within the cap", w.Amount, capOf(t, vec, a))
		}
	}
	ws := withdrawalsOf(t, vec)
	var overCapAmounts []codec.U256
	for _, w := range ws {
		if hex.EncodeToString(w.Asset[:]) == keys[1] {
			overCapAmounts = append(overCapAmounts, w.Amount)
		}
	}
	if len(overCapAmounts) != 2 || overCapAmounts[0] != overCapAmounts[1] {
		t.Fatalf("CV-CAP-6: the over-cap asset has withdrawals %v, want exactly two of equal amount", overCapAmounts)
	}
	if last := hex.EncodeToString(ws[len(ws)-1].Asset[:]); last == keys[1] {
		t.Fatalf("CV-CAP-6: the last withdrawal is of the over-cap asset %s, want another asset", last)
	}
	largerElsewhere := false
	for _, k := range []string{keys[0], keys[2]} {
		if totals[k].Cmp(totals[keys[1]]) > 0 {
			largerElsewhere = true
		}
	}
	if !largerElsewhere {
		t.Fatalf("CV-CAP-6: the over-cap asset has the largest total, want a larger total in another asset")
	}
	capSum, totalSum := new(big.Int), new(big.Int)
	for i, k := range keys {
		capSum.Add(capSum, capOf(t, vec, k))
		totalSum.Add(totalSum, totals[k])
		if over := totals[k].Cmp(capOf(t, vec, k)) > 0; over != (i == 1) {
			t.Fatalf("CV-CAP-6: asset %s (position %d) over its own cap is %v, want only the middle asset over", k, i, over)
		}
	}
	mid := totals[keys[1]]
	for _, k := range []string{keys[0], keys[2]} {
		if mid.Cmp(capOf(t, vec, k)) > 0 {
			t.Fatalf("CV-CAP-6: the over-cap total %s exceeds the other asset's cap %s", mid, capOf(t, vec, k))
		}
	}
	if totalSum.Cmp(capSum) > 0 {
		t.Fatalf("CV-CAP-6: the summed totals %s exceed the summed caps %s", totalSum, capSum)
	}
	if new(big.Int).SetBytes(mustHex(t, vec.Input.DepositAmount)).Sign() == 0 {
		t.Fatalf("CV-CAP-6: input.deposit_amount is zero")
	}
}

// checkCVCAP7 pins step 5a before step 6: the vector is over its cap, runs out of plasma exactly
// at its limit, and has a non-zero effect charge it never paid.
func checkCVCAP7(t *testing.T, vec vectorJSON) {
	t.Helper()
	if vec.Status != "RUNTIME_FAULT" {
		t.Fatalf("CV-CAP-7: status %s, want RUNTIME_FAULT", vec.Status)
	}
	if vec.Input == nil || vec.Input.PlasmaLimit == nil || vec.GasUsed != *vec.Input.PlasmaLimit {
		t.Fatalf("CV-CAP-7: gas_used %d, want it equal to input.plasma_limit", vec.GasUsed)
	}
	over := false
	for k, total := range withdrawalTotals(t, vec) {
		if total.Cmp(capOf(t, vec, k)) > 0 {
			over = true
		}
	}
	if !over {
		t.Fatalf("CV-CAP-7: no asset is over its cap")
	}
	eff, err := codec.DecodeContractEffects(mustHex(t, vec.ContractEffectsBlobHex))
	if err != nil {
		t.Fatalf("CV-CAP-7: DecodeContractEffects: %v", err)
	}
	if inlineEffectCharge(eff) == 0 {
		t.Fatalf("CV-CAP-7: the effect charge is zero, want non-zero")
	}
}

// checkCVCAP8 pins the RUNTIME_FAULT to the exact total: every amount is within the cap, the
// cap is the largest u256 and the exact total reaches 2^256, which a u256 sum wraps to 0.
func checkCVCAP8(t *testing.T, vec vectorJSON) {
	t.Helper()
	if vec.Status != "RUNTIME_FAULT" {
		t.Fatalf("CV-CAP-8: status %s, want RUNTIME_FAULT", vec.Status)
	}
	checkCVASSETSBase(t, vec)
	checkCapFaultExtras(t, vec)
	asset, total, amounts := capWithdrawalsInOneAsset(t, vec, 2)
	limit := capOf(t, vec, asset)
	if want := new(big.Int).Sub(new(big.Int).Lsh(big.NewInt(1), 256), big.NewInt(1)); limit.Cmp(want) != 0 {
		t.Fatalf("CV-CAP-8: cap %s, want 2^256 - 1", limit)
	}
	for i, a := range amounts {
		if a.Cmp(limit) > 0 {
			t.Fatalf("CV-CAP-8: withdrawal %d amount %s exceeds the cap %s alone", i, a, limit)
		}
	}
	if total.Cmp(new(big.Int).Lsh(big.NewInt(1), 256)) < 0 {
		t.Fatalf("CV-CAP-8: total %s is below 2^256", total)
	}
}

// checkCVCAP9 pins SUCCESS to a total above 2^64 and 2^128 that is within a wide cap: the
// exact total fits the cap, while the total truncated to 64 or 128 bits does not fit the cap
// truncated to the same width, so a cap truncated to those widths, alone or together with the
// sum, faults it.
func checkCVCAP9(t *testing.T, vec vectorJSON) {
	t.Helper()
	if vec.Status != "SUCCESS" {
		t.Fatalf("CV-CAP-9: status %s, want SUCCESS", vec.Status)
	}
	asset, total, amounts := capWithdrawalsInOneAsset(t, vec, 2)
	limit := capOf(t, vec, asset)
	for i, a := range amounts {
		if a.Cmp(new(big.Int).Lsh(big.NewInt(1), 64)) <= 0 {
			t.Fatalf("CV-CAP-9: withdrawal %d amount %s is not above 2^64", i, a)
		}
	}
	if limit.Cmp(new(big.Int).Lsh(big.NewInt(1), 128)) <= 0 {
		t.Fatalf("CV-CAP-9: cap %s is not above 2^128", limit)
	}
	if total.Cmp(limit) > 0 {
		t.Fatalf("CV-CAP-9: total %s exceeds the cap %s", total, limit)
	}
	for _, k := range []uint{64, 128} {
		mod := new(big.Int).Lsh(big.NewInt(1), k)
		if new(big.Int).Mod(total, mod).Cmp(new(big.Int).Mod(limit, mod)) <= 0 {
			t.Fatalf("CV-CAP-9: total mod 2^%d is not above the cap mod 2^%d", k, k)
		}
	}
}

// checkCVCAP10 pins the RUNTIME_FAULT to a total above the cap: each amount is within the cap,
// while the total truncated to 64 or 128 bits is within the cap truncated to the same width.
func checkCVCAP10(t *testing.T, vec vectorJSON) {
	t.Helper()
	if vec.Status != "RUNTIME_FAULT" {
		t.Fatalf("CV-CAP-10: status %s, want RUNTIME_FAULT", vec.Status)
	}
	checkCVASSETSBase(t, vec)
	checkCapFaultExtras(t, vec)
	asset, total, amounts := capWithdrawalsInOneAsset(t, vec, 2)
	limit := capOf(t, vec, asset)
	for i, a := range amounts {
		if a.Cmp(new(big.Int).Lsh(big.NewInt(1), 64)) <= 0 || a.Cmp(limit) > 0 {
			t.Fatalf("CV-CAP-10: withdrawal %d amount %s is not above 2^64 and within the cap %s", i, a, limit)
		}
	}
	if total.Cmp(limit) <= 0 {
		t.Fatalf("CV-CAP-10: total %s is not above the cap %s", total, limit)
	}
	for _, k := range []uint{64, 128} {
		mod := new(big.Int).Lsh(big.NewInt(1), k)
		if new(big.Int).Mod(total, mod).Cmp(new(big.Int).Mod(limit, mod)) > 0 {
			t.Fatalf("CV-CAP-10: total mod 2^%d is above the cap mod 2^%d", k, k)
		}
	}
}

// checkCapFaultExtras asserts the capVector RUNTIME_FAULT extras: a non-empty StateDiff,
// exactly one event, a non-zero deposit, and conditions {CapOver} at RUNTIME_FAULT.
func checkCapFaultExtras(t *testing.T, vec vectorJSON) {
	t.Helper()
	want := noConditions()
	want.CapOver = true
	eff := checkConditions(t, vec, "RUNTIME_FAULT", want)
	if len(eff.StateDiff) == 0 {
		t.Fatalf("%s: StateDiff is empty, want a state write for the rollback to discard", vec.ID)
	}
	if len(eff.Events) != 1 {
		t.Fatalf("%s: %d events, want exactly 1", vec.ID, len(eff.Events))
	}
	if vec.Input == nil || new(big.Int).SetBytes(mustHex(t, vec.Input.DepositAmount)).Sign() == 0 {
		t.Fatalf("%s: input.deposit_amount is zero, want a deposit for the refund to return", vec.ID)
	}
}

// carryDroppedSum sums amounts in order, losing exactly the carry out of bit k at each step.
func carryDroppedSum(amounts []*big.Int, k uint) *big.Int {
	mod := new(big.Int).Lsh(big.NewInt(1), k)
	sum := new(big.Int)
	for _, a := range amounts {
		low := new(big.Int).Mod(sum, mod)
		low.Add(low, new(big.Int).Mod(a, mod)).Mod(low, mod)
		high := new(big.Int).Rsh(sum, k)
		high.Add(high, new(big.Int).Rsh(a, k)).Lsh(high, k)
		sum = low.Add(low, high)
	}
	return sum
}

// checkCVCAP11 pins the RUNTIME_FAULT to a total that carries across the 2^64, 2^128 and 2^192
// limb boundaries: each amount is within the cap, the exact total 2^192 is over it, and a sum
// that loses the carry out of bit 64, 128 or 192 stays within it.
func checkCVCAP11(t *testing.T, vec vectorJSON) {
	t.Helper()
	if vec.Status != "RUNTIME_FAULT" {
		t.Fatalf("CV-CAP-11: status %s, want RUNTIME_FAULT", vec.Status)
	}
	checkCVASSETSBase(t, vec)
	asset, total, amounts := capWithdrawalsInOneAsset(t, vec, 2)
	if len(amounts) != 2 {
		t.Fatalf("CV-CAP-11: %d withdrawals, want exactly 2", len(amounts))
	}
	limit := capOf(t, vec, asset)
	if want := new(big.Int).Sub(new(big.Int).Lsh(big.NewInt(1), 192), big.NewInt(1)); limit.Cmp(want) != 0 {
		t.Fatalf("CV-CAP-11: cap %s, want 2^192 - 1", limit)
	}
	for i, a := range amounts {
		if a.Cmp(limit) > 0 {
			t.Fatalf("CV-CAP-11: withdrawal %d amount %s exceeds the cap %s alone", i, a, limit)
		}
	}
	if want := new(big.Int).Lsh(big.NewInt(1), 192); total.Cmp(want) != 0 {
		t.Fatalf("CV-CAP-11: total %s, want 2^192", total)
	}
	for _, k := range []uint{64, 128, 192} {
		if dropped := carryDroppedSum(amounts, k); dropped.Cmp(limit) > 0 {
			t.Fatalf("CV-CAP-11: the sum losing the carry out of bit %d is %s, above the cap %s", k, dropped, limit)
		}
	}
	checkCapFaultExtras(t, vec)
}

// checkCVCAP12 pins SUCCESS to the deposit not counting toward the cap: two withdrawals in the
// deposit asset total exactly its cap, so adding the non-zero deposit would put it over.
func checkCVCAP12(t *testing.T, vec vectorJSON) {
	t.Helper()
	eff := checkConditions(t, vec, "SUCCESS", noConditions())
	ws := withdrawalsOf(t, vec)
	if len(ws) != 2 {
		t.Fatalf("CV-CAP-12: %d withdrawals, want exactly 2", len(ws))
	}
	if vec.Input == nil {
		t.Fatalf("CV-CAP-12: input is absent")
	}
	for i, w := range ws {
		if hex.EncodeToString(w.Asset[:]) != vec.Input.DepositAsset {
			t.Fatalf("CV-CAP-12: withdrawal %d is in asset %x, want input.deposit_asset %s", i, w.Asset, vec.Input.DepositAsset)
		}
	}
	total := withdrawalTotals(t, vec)[vec.Input.DepositAsset]
	limit := capOf(t, vec, vec.Input.DepositAsset)
	if limit.Sign() == 0 || total.Cmp(limit) != 0 {
		t.Fatalf("CV-CAP-12: total %s, cap %s; want a non-zero cap equal to the total", total, limit)
	}
	if new(big.Int).SetBytes(mustHex(t, vec.Input.DepositAmount)).Sign() == 0 {
		t.Fatalf("CV-CAP-12: input.deposit_amount is zero, want a non-zero deposit")
	}
	if eff.ClaimedDeposit != (codec.U256{}) {
		t.Fatalf("CV-CAP-12: blob ClaimedDeposit is non-zero, want the deposit unclaimed")
	}
}

// checkZeroChargeBudget asserts gas_used == plasma_limit == the one-page blob module's metered
// gas, computed inline, and a non-zero effect charge for eff.
func checkZeroChargeBudget(t *testing.T, vec vectorJSON, eff codec.ContractEffects) {
	t.Helper()
	metered := module.InitialMemoryCost(1)
	for _, op := range []byte{0x42, 0x0B} {
		cost, ok := module.OpcodeCost(module.Opcode(op))
		if !ok {
			t.Fatalf("%s: no gas-table cost for opcode 0x%02X", vec.ID, op)
		}
		metered += cost
	}
	if vec.Input == nil || vec.Input.PlasmaLimit == nil || *vec.Input.PlasmaLimit != metered {
		t.Fatalf("%s: input.plasma_limit is not the metered gas %d", vec.ID, metered)
	}
	if vec.GasUsed != metered {
		t.Fatalf("%s: gas_used %d, want the metered gas %d", vec.ID, vec.GasUsed, metered)
	}
	if inlineEffectCharge(eff) == 0 {
		t.Fatalf("%s: the effect charge is zero, want non-zero", vec.ID)
	}
}

// blobModuleMetered is the gas of a one-page module whose run executes i64.const then end,
// from the gas table alone: module.InitialMemoryCost(1) plus module.OpcodeCost of 0x42 and
// 0x0B. It fails the test, naming id, on a missing opcode cost.
func blobModuleMetered(t *testing.T, id string) uint64 {
	t.Helper()
	metered := module.InitialMemoryCost(1)
	for _, op := range []byte{0x42, 0x0B} {
		cost, ok := module.OpcodeCost(module.Opcode(op))
		if !ok {
			t.Fatalf("%s: no gas-table cost for opcode 0x%02X", id, op)
		}
		metered += cost
	}
	return metered
}

// checkOneDefect asserts status RUNTIME_FAULT and gas_used == blobModuleMetered, that the
// row's blob fails codec.DecodeContractEffects with errors.Is(err, wantErr), and that repair,
// given a copy of the blob, returns bytes that decode, re-encode with ContractEffects.Encode
// to the same bytes, and trip no condition (conditionsOf equals noConditions()).
func checkOneDefect(t *testing.T, vec vectorJSON, wantErr error, repair func(blob []byte) []byte) {
	t.Helper()
	if vec.Status != "RUNTIME_FAULT" {
		t.Fatalf("%s: status %s, want RUNTIME_FAULT", vec.ID, vec.Status)
	}
	if want := blobModuleMetered(t, vec.ID); vec.GasUsed != want {
		t.Fatalf("%s: gas_used %d, want the metered gas %d", vec.ID, vec.GasUsed, want)
	}
	blob := mustHex(t, vec.ContractEffectsBlobHex)
	if _, err := codec.DecodeContractEffects(blob); !errors.Is(err, wantErr) {
		t.Fatalf("%s: DecodeContractEffects error = %v, want errors.Is(..., %v)", vec.ID, err, wantErr)
	}
	repaired := repair(append([]byte(nil), blob...))
	eff, err := codec.DecodeContractEffects(repaired)
	if err != nil {
		t.Fatalf("%s: DecodeContractEffects of the repaired blob: %v", vec.ID, err)
	}
	again, err := eff.Encode()
	if err != nil {
		t.Fatalf("%s: Encode of the repaired blob: %v", vec.ID, err)
	}
	if !bytes.Equal(again, repaired) {
		t.Fatalf("%s: the repaired blob does not re-encode to the same bytes", vec.ID)
	}
	if got := conditionsOf(t, vec, eff); !reflect.DeepEqual(got, noConditions()) {
		t.Fatalf("%s: repaired blob conditions\n got  %+v\n want %+v", vec.ID, got, noConditions())
	}
}

// conditionsWith is noConditions with set applied.
func conditionsWith(set func(c *blobConditions)) blobConditions {
	c := noConditions()
	set(&c)
	return c
}

// checkCVASSETS3 pins SUCCESS at exactly the footprint limit: 16 distinct withdrawal assets and
// no deposit.
func checkCVASSETS3(t *testing.T, vec vectorJSON) {
	t.Helper()
	if n := distinctAssetCount(checkCVASSETSBase(t, vec)); n != 16 {
		t.Fatalf("CV-ASSETS-3: %d distinct withdrawal assets, want 16", n)
	}
	if new(big.Int).SetBytes(mustHex(t, vec.Input.DepositAmount)).Sign() != 0 {
		t.Fatalf("CV-ASSETS-3: input.deposit_amount is non-zero, want no deposit")
	}
	checkConditions(t, vec, "SUCCESS", noConditions())
}

// checkCVASSETS4 pins SUCCESS for a deposit in an asset that is already withdrawn: the footprint
// stays at 16.
func checkCVASSETS4(t *testing.T, vec vectorJSON) {
	t.Helper()
	assets := checkCVASSETSBase(t, vec)
	if n := distinctAssetCount(assets); n != 16 {
		t.Fatalf("CV-ASSETS-4: %d distinct withdrawal assets, want 16", n)
	}
	deposit := new(big.Int).SetBytes(mustHex(t, vec.Input.DepositAmount))
	if deposit.Sign() == 0 {
		t.Fatalf("CV-ASSETS-4: input.deposit_amount is zero, want a deposit")
	}
	withdrawn := false
	for _, a := range assets {
		if hex.EncodeToString(a[:]) == vec.Input.DepositAsset {
			withdrawn = true
		}
	}
	if !withdrawn {
		t.Fatalf("CV-ASSETS-4: deposit asset %s is not one of the withdrawn assets", vec.Input.DepositAsset)
	}
	need := new(big.Int).Add(withdrawalTotals(t, vec)[vec.Input.DepositAsset], deposit)
	if limit := capOf(t, vec, vec.Input.DepositAsset); limit.Cmp(need) < 0 {
		t.Fatalf("CV-ASSETS-4: cap %s is below the withdrawal total plus the deposit %s", limit, need)
	}
	checkConditions(t, vec, "SUCCESS", noConditions())
}

// checkCVASSETS5 pins the RUNTIME_FAULT to a zero-amount withdrawal that names a 17th asset.
func checkCVASSETS5(t *testing.T, vec vectorJSON) {
	t.Helper()
	if n := distinctAssetCount(checkCVASSETSBase(t, vec)); n != 17 {
		t.Fatalf("CV-ASSETS-5: %d distinct withdrawal assets, want 17", n)
	}
	zero := 0
	for _, total := range withdrawalTotals(t, vec) {
		if total.Sign() == 0 {
			zero++
		}
	}
	if zero != 1 {
		t.Fatalf("CV-ASSETS-5: %d assets with a zero total, want exactly 1", zero)
	}
	checkConditions(t, vec, "RUNTIME_FAULT", conditionsWith(func(c *blobConditions) { c.FootprintOver = true }))
}

func checkCVORDER1(t *testing.T, vec vectorJSON) {
	t.Helper()
	checkConditions(t, vec, "RUNTIME_FAULT", conditionsWith(func(c *blobConditions) { c.IndexMismatch = true; c.FirstReserved = 0 }))
}

func checkCVORDER2(t *testing.T, vec vectorJSON) {
	t.Helper()
	checkConditions(t, vec, "RUNTIME_FAULT", conditionsWith(func(c *blobConditions) { c.ForeignAttribution = true; c.FirstReserved = 0 }))
}

func checkCVORDER3(t *testing.T, vec vectorJSON) {
	t.Helper()
	checkConditions(t, vec, "RUNTIME_FAULT", conditionsWith(func(c *blobConditions) { c.FirstDuplicate = 1; c.FirstReserved = 2 }))
}

func checkCVORDER4(t *testing.T, vec vectorJSON) {
	t.Helper()
	checkConditions(t, vec, "UNAUTHORIZED", conditionsWith(func(c *blobConditions) { c.FirstReserved = 0; c.FirstDuplicate = 2 }))
}

// checkCVORDER5 pins the trailing byte: the blob does not decode, and without its last byte it
// decodes to a reserved-key write.
func checkCVORDER5(t *testing.T, vec vectorJSON) {
	t.Helper()
	if vec.Status != "RUNTIME_FAULT" {
		t.Fatalf("CV-ORDER-5: status %s, want RUNTIME_FAULT", vec.Status)
	}
	blob := mustHex(t, vec.ContractEffectsBlobHex)
	if _, err := codec.DecodeContractEffects(blob); !errors.Is(err, codec.ErrTrailingBytes) {
		t.Fatalf("CV-ORDER-5: DecodeContractEffects error = %v, want ErrTrailingBytes", err)
	}
	eff, err := codec.DecodeContractEffects(blob[:len(blob)-1])
	if err != nil {
		t.Fatalf("CV-ORDER-5: DecodeContractEffects without the trailing byte: %v", err)
	}
	want := conditionsWith(func(c *blobConditions) { c.FirstReserved = 0 })
	if got := conditionsOf(t, vec, eff); !reflect.DeepEqual(got, want) {
		t.Fatalf("CV-ORDER-5: blob conditions\n got  %+v\n want %+v", got, want)
	}
}

func checkCVORDER6(t *testing.T, vec vectorJSON) {
	t.Helper()
	checkConditions(t, vec, "UNAUTHORIZED", conditionsWith(func(c *blobConditions) { c.FirstReserved = 0; c.OverClaim = true }))
}

func checkCVORDER7(t *testing.T, vec vectorJSON) {
	t.Helper()
	checkConditions(t, vec, "UNAUTHORIZED", conditionsWith(func(c *blobConditions) { c.FirstReserved = 0; c.CapOver = true }))
}

func checkCVORDER8(t *testing.T, vec vectorJSON) {
	t.Helper()
	eff := checkConditions(t, vec, "RUNTIME_FAULT", conditionsWith(func(c *blobConditions) { c.IndexMismatch = true }))
	checkZeroChargeBudget(t, vec, eff)
}

func checkCVORDER9(t *testing.T, vec vectorJSON) {
	t.Helper()
	eff := checkConditions(t, vec, "UNAUTHORIZED", conditionsWith(func(c *blobConditions) { c.FirstReserved = 0 }))
	checkZeroChargeBudget(t, vec, eff)
}

func checkCVORDER10(t *testing.T, vec vectorJSON) {
	t.Helper()
	eff := checkConditions(t, vec, "RUNTIME_FAULT", conditionsWith(func(c *blobConditions) { c.OverClaim = true }))
	checkZeroChargeBudget(t, vec, eff)
}

func checkCVORDER11(t *testing.T, vec vectorJSON) {
	t.Helper()
	eff := checkConditions(t, vec, "OUT_OF_GAS", conditionsWith(func(c *blobConditions) { c.QuotaOver = true }))
	checkZeroChargeBudget(t, vec, eff)
}

func checkCVORDER12(t *testing.T, vec vectorJSON) {
	t.Helper()
	checkConditions(t, vec, "STORAGE_QUOTA_EXCEEDED", conditionsWith(func(c *blobConditions) { c.QuotaOver = true; c.ReturnDataOver = true }))
	checkPathGas(t, vec, 1, 0, []byte{0x42, 0x0B})
}

func checkCVORDER13(t *testing.T, vec vectorJSON) {
	t.Helper()
	eff := checkConditions(t, vec, "OUT_OF_GAS", conditionsWith(func(c *blobConditions) { c.ReturnDataOver = true }))
	checkZeroChargeBudget(t, vec, eff)
}

// checkCVORDER14 pins the foreign outbox source: step 2 precedes the reserved-key scan.
func checkCVORDER14(t *testing.T, vec vectorJSON) {
	t.Helper()
	eff := checkConditions(t, vec, "RUNTIME_FAULT", conditionsWith(func(c *blobConditions) { c.ForeignAttribution = true; c.FirstReserved = 0 }))
	if len(eff.Events) != 0 || len(eff.Outbox) != 1 {
		t.Fatalf("CV-ORDER-14: %d events and %d outbox messages, want 0 and 1", len(eff.Events), len(eff.Outbox))
	}
	m := eff.Outbox[0]
	if _, ok := m.Target.(codec.L1Withdrawal); !ok {
		t.Fatalf("CV-ORDER-14: outbox message is %T, want an L1Withdrawal", m.Target)
	}
	if m.SourceContract == (types.Hash{}) || bytes.Equal(m.SourceContract[:], mustHex(t, vec.Receipt.ContractID)) {
		t.Fatalf("CV-ORDER-14: source_contract %x, want a non-zero contract other than the executing one", m.SourceContract[:])
	}
}

// checkCVORDER15 pins the footprint breach at a plasma_limit with no room for the effect charge.
func checkCVORDER15(t *testing.T, vec vectorJSON) {
	t.Helper()
	eff := checkConditions(t, vec, "RUNTIME_FAULT", conditionsWith(func(c *blobConditions) { c.FootprintOver = true }))
	checkZeroChargeBudget(t, vec, eff)
}

// checkCVORDER16 pins the reserved-key write ahead of the footprint breach.
func checkCVORDER16(t *testing.T, vec vectorJSON) {
	t.Helper()
	checkConditions(t, vec, "UNAUTHORIZED", conditionsWith(func(c *blobConditions) { c.FirstReserved = 0; c.FootprintOver = true }))
}

// checkCVATTR1 pins the foreign event source: the one event is attributed to another contract
// and is clean once attributed to the executing contract.
func checkCVATTR1(t *testing.T, vec vectorJSON) {
	t.Helper()
	eff := checkConditions(t, vec, "RUNTIME_FAULT", conditionsWith(func(c *blobConditions) { c.ForeignAttribution = true }))
	if len(eff.Events) != 1 || len(eff.Outbox) != 0 {
		t.Fatalf("CV-ATTR-1: %d events and %d outbox messages, want 1 and 0", len(eff.Events), len(eff.Outbox))
	}
	copy(eff.Events[0].ContractID[:], mustHex(t, vec.Receipt.ContractID))
	if got := conditionsOf(t, vec, eff); !reflect.DeepEqual(got, noConditions()) {
		t.Fatalf("CV-ATTR-1: attributed to the contract, conditions\n got  %+v\n want %+v", got, noConditions())
	}
}

// checkCVATTR2 pins the foreign, non-zero L2 delivery source.
func checkCVATTR2(t *testing.T, vec vectorJSON) {
	t.Helper()
	eff := checkConditions(t, vec, "RUNTIME_FAULT", conditionsWith(func(c *blobConditions) { c.ForeignAttribution = true }))
	if len(eff.Events) != 0 || len(eff.Outbox) != 1 {
		t.Fatalf("CV-ATTR-2: %d events and %d outbox messages, want 0 and 1", len(eff.Events), len(eff.Outbox))
	}
	m := eff.Outbox[0]
	if _, ok := m.Target.(codec.L2Delivery); !ok {
		t.Fatalf("CV-ATTR-2: outbox message is %T, want an L2Delivery", m.Target)
	}
	contract := mustHex(t, vec.Receipt.ContractID)
	if m.SourceContract == (types.Hash{}) || bytes.Equal(m.SourceContract[:], contract) {
		t.Fatalf("CV-ATTR-2: source_contract %x, want a non-zero contract other than the executing one", m.SourceContract[:])
	}
	copy(eff.Outbox[0].SourceContract[:], contract)
	if got := conditionsOf(t, vec, eff); !reflect.DeepEqual(got, noConditions()) {
		t.Fatalf("CV-ATTR-2: sourced from the contract, conditions\n got  %+v\n want %+v", got, noConditions())
	}
}

// checkCVATTR3 pins the zero source_contract on a withdrawal.
func checkCVATTR3(t *testing.T, vec vectorJSON) {
	t.Helper()
	eff := checkConditions(t, vec, "RUNTIME_FAULT", conditionsWith(func(c *blobConditions) { c.ForeignAttribution = true }))
	if len(eff.Events) != 0 || len(eff.Outbox) != 1 {
		t.Fatalf("CV-ATTR-3: %d events and %d outbox messages, want 0 and 1", len(eff.Events), len(eff.Outbox))
	}
	m := eff.Outbox[0]
	if _, ok := m.Target.(codec.L1Withdrawal); !ok {
		t.Fatalf("CV-ATTR-3: outbox message is %T, want an L1Withdrawal", m.Target)
	}
	if m.SourceContract != (types.Hash{}) {
		t.Fatalf("CV-ATTR-3: source_contract %x, want bytes32(0)", m.SourceContract[:])
	}
	copy(eff.Outbox[0].SourceContract[:], mustHex(t, vec.Receipt.ContractID))
	if got := conditionsOf(t, vec, eff); !reflect.DeepEqual(got, noConditions()) {
		t.Fatalf("CV-ATTR-3: sourced from the contract, conditions\n got  %+v\n want %+v", got, noConditions())
	}
}

// checkCVDECODE1 pins version 2 in the first two bytes.
func checkCVDECODE1(t *testing.T, vec vectorJSON) {
	t.Helper()
	checkOneDefect(t, vec, codec.ErrUnknownVersion, func(b []byte) []byte {
		b[0], b[1] = 0x00, 0x01
		return b
	})
}

// checkCVDECODE2 pins the event input_index at [50:58].
func checkCVDECODE2(t *testing.T, vec vectorJSON) {
	t.Helper()
	checkOneDefect(t, vec, codec.ErrIndexMismatch, func(b []byte) []byte {
		copy(b[50:58], b[2:10])
		return b
	})
}

// checkCVDECODE3 pins the outbox message input_index at [54:62].
func checkCVDECODE3(t *testing.T, vec vectorJSON) {
	t.Helper()
	checkOneDefect(t, vec, codec.ErrIndexMismatch, func(b []byte) []byte {
		copy(b[54:62], b[2:10])
		return b
	})
}

// checkCVDECODE4 pins the one trailing byte.
func checkCVDECODE4(t *testing.T, vec vectorJSON) {
	t.Helper()
	checkOneDefect(t, vec, codec.ErrTrailingBytes, func(b []byte) []byte {
		return b[:len(b)-1]
	})
}

// checkCVDECODE5 pins the blob that ends before claimed_deposit.
func checkCVDECODE5(t *testing.T, vec vectorJSON) {
	t.Helper()
	checkOneDefect(t, vec, codec.ErrTruncated, func(b []byte) []byte {
		return append(append([]byte{}, b...), make([]byte, 32)...)
	})
}

// checkCVDECODE6 pins the ReturnData length prefix at [22:26].
func checkCVDECODE6(t *testing.T, vec vectorJSON) {
	t.Helper()
	checkOneDefect(t, vec, codec.ErrTruncated, func(b []byte) []byte {
		binary.BigEndian.PutUint32(b[22:26], 2)
		return b
	})
}

// checkCVDECODE7 pins the outbox kind byte at [66].
func checkCVDECODE7(t *testing.T, vec vectorJSON) {
	t.Helper()
	checkOneDefect(t, vec, codec.ErrInvalidKind, func(b []byte) []byte {
		b[66] = 0
		return b
	})
}

// checkCVDECODE8 pins the withdrawal's AssetID byte 21 at [108], the last byte that must be
// zero: bytes [87:108] are zero and byte [108] is the only non-zero byte in [0:22].
func checkCVDECODE8(t *testing.T, vec vectorJSON) {
	t.Helper()
	checkOneDefect(t, vec, codec.ErrInvalidAssetID, func(b []byte) []byte {
		if !bytes.Equal(b[87:108], make([]byte, 21)) || b[108] == 0 {
			t.Fatalf("CV-DECODE-8: asset bytes [0:21] = %x, byte 21 = %#x, want zeros and a non-zero byte", b[87:108], b[108])
		}
		b[108] = 0
		return b
	})
}

// checkCVDECODE9 pins the empty blob. The module returns ptr 1024, len 0, in bounds; gas_used
// is the module's metered gas with no effect charge. CV-OOB-1 also has RUNTIME_FAULT, gas 1002
// and no blob field, so the module's code section is checked last.
func checkCVDECODE9(t *testing.T, vec vectorJSON) {
	t.Helper()
	if vec.Status != "RUNTIME_FAULT" {
		t.Fatalf("%s: status %s, want RUNTIME_FAULT", vec.ID, vec.Status)
	}
	if vec.ContractEffectsBlobHex != "" {
		t.Fatalf("%s: contract_effects_blob_hex is %q, want empty", vec.ID, vec.ContractEffectsBlobHex)
	}
	if want := blobModuleMetered(t, vec.ID); vec.GasUsed != want {
		t.Fatalf("%s: gas_used %d, want the metered gas %d", vec.ID, vec.GasUsed, want)
	}
	if !strings.HasSuffix(vec.ModuleWasmHex, "0a0c010a0042808080808080010b") {
		t.Fatalf("%s: module_wasm_hex does not end with the code section that returns ptr 1024, len 0", vec.ID)
	}
}

// checkCVDECODE10 pins the second event's event_index at [110:114].
func checkCVDECODE10(t *testing.T, vec vectorJSON) {
	t.Helper()
	checkOneDefect(t, vec, codec.ErrIndexGap, func(b []byte) []byte {
		binary.BigEndian.PutUint32(b[110:114], 1)
		return b
	})
}

// checkCVDECODE11 pins the second message's outbox_index at [143:147].
func checkCVDECODE11(t *testing.T, vec vectorJSON) {
	t.Helper()
	checkOneDefect(t, vec, codec.ErrIndexGap, func(b []byte) []byte {
		binary.BigEndian.PutUint32(b[143:147], 1)
		return b
	})
}

// checkCVLIMIT1 pins the 33-byte local key: the repair drops its last byte.
func checkCVLIMIT1(t *testing.T, vec vectorJSON) {
	t.Helper()
	checkOneDefect(t, vec, codec.ErrLimitExceeded, func(b []byte) []byte {
		out := append(append([]byte{}, b[:50]...), b[51:]...)
		binary.BigEndian.PutUint32(out[14:18], 32)
		return out
	})
}

// checkCVLIMIT2 pins the 32-byte local key that is accepted.
func checkCVLIMIT2(t *testing.T, vec vectorJSON) {
	t.Helper()
	eff := checkConditions(t, vec, "SUCCESS", noConditions())
	if len(eff.StateDiff) != 1 || len(eff.StateDiff[0].LocalKey) != codec.MaxStateKeySize {
		t.Fatalf("CV-LIMIT-2: want exactly one StateDiff entry with a %d-byte local key", codec.MaxStateKeySize)
	}
}

// checkCVLIMIT3 pins the value one byte over the bound: the repair drops its last byte.
func checkCVLIMIT3(t *testing.T, vec vectorJSON) {
	t.Helper()
	checkOneDefect(t, vec, codec.ErrReservedLength, func(b []byte) []byte {
		n := len(b)
		out := append(append([]byte{}, b[:n-45]...), b[n-44:]...)
		binary.BigEndian.PutUint32(out[19:23], codec.MaxStateValueSize)
		return out
	})
}

// checkCVLIMIT4 pins the 65th event: the repair drops it.
func checkCVLIMIT4(t *testing.T, vec vectorJSON) {
	t.Helper()
	checkOneDefect(t, vec, codec.ErrLimitExceeded, func(b []byte) []byte {
		n := len(b)
		extra, err := codec.DecodeEvent(b[n-92 : n-40])
		if err != nil {
			t.Fatalf("CV-LIMIT-4: the 65th event does not decode: %v", err)
		}
		if !bytes.Equal(extra.ContractID[:], mustHex(t, vec.Receipt.ContractID)) || extra.InputIndex != binary.BigEndian.Uint64(b[2:10]) || extra.EventIndex != codec.MaxEventsPerExecution {
			t.Fatalf("CV-LIMIT-4: the 65th event is %+v, want contract_id %s, the blob's input index and event_index %d", extra, vec.Receipt.ContractID, codec.MaxEventsPerExecution)
		}
		out := append(append([]byte{}, b[:n-92]...), b[n-40:]...)
		binary.BigEndian.PutUint32(out[14:18], 64)
		return out
	})
}

// checkCVLIMIT5 pins the 64 events that are accepted.
func checkCVLIMIT5(t *testing.T, vec vectorJSON) {
	t.Helper()
	eff := checkConditions(t, vec, "SUCCESS", noConditions())
	if len(eff.Events) != codec.MaxEventsPerExecution {
		t.Fatalf("CV-LIMIT-5: %d events, want %d", len(eff.Events), codec.MaxEventsPerExecution)
	}
}

// checkCVLIMIT6 pins the fifth topic: the repair drops it.
func checkCVLIMIT6(t *testing.T, vec vectorJSON) {
	t.Helper()
	checkOneDefect(t, vec, codec.ErrLimitExceeded, func(b []byte) []byte {
		n := len(b)
		out := append(append([]byte{}, b[:n-76]...), b[n-44:]...)
		binary.BigEndian.PutUint32(out[62:66], 4)
		return out
	})
}

// checkCVLIMIT7 pins the four topics that are accepted.
func checkCVLIMIT7(t *testing.T, vec vectorJSON) {
	t.Helper()
	eff := checkConditions(t, vec, "SUCCESS", noConditions())
	if len(eff.Events) != 1 || len(eff.Events[0].Topics) != codec.MaxTopicsPerEvent {
		t.Fatalf("CV-LIMIT-7: want exactly one event with %d topics", codec.MaxTopicsPerEvent)
	}
}

// checkCVLIMIT8 pins the 1025th data byte: the repair drops it.
func checkCVLIMIT8(t *testing.T, vec vectorJSON) {
	t.Helper()
	checkOneDefect(t, vec, codec.ErrLimitExceeded, func(b []byte) []byte {
		n := len(b)
		out := append(append([]byte{}, b[:n-41]...), b[n-40:]...)
		binary.BigEndian.PutUint32(out[66:70], 1024)
		return out
	})
}

// checkCVLIMIT9 pins the 1024 data bytes that are accepted.
func checkCVLIMIT9(t *testing.T, vec vectorJSON) {
	t.Helper()
	eff := checkConditions(t, vec, "SUCCESS", noConditions())
	if len(eff.Events) != 1 || len(eff.Events[0].Data) != codec.MaxEventDataSize {
		t.Fatalf("CV-LIMIT-9: want exactly one event with %d data bytes", codec.MaxEventDataSize)
	}
}

// checkCVLIMIT10 pins the 8193rd payload byte: the repair drops it.
func checkCVLIMIT10(t *testing.T, vec vectorJSON) {
	t.Helper()
	checkOneDefect(t, vec, codec.ErrLimitExceeded, func(b []byte) []byte {
		n := len(b)
		out := append(append([]byte{}, b[:n-37]...), b[n-36:]...)
		binary.BigEndian.PutUint32(out[99:103], 8192)
		return out
	})
}

// checkCVLIMIT11 pins the 8192-byte payload that is accepted.
func checkCVLIMIT11(t *testing.T, vec vectorJSON) {
	t.Helper()
	eff := checkConditions(t, vec, "SUCCESS", noConditions())
	if len(eff.Outbox) != 1 {
		t.Fatalf("CV-LIMIT-11: %d outbox messages, want 1", len(eff.Outbox))
	}
	_, ok := eff.Outbox[0].Target.(codec.L2Delivery)
	if !ok || len(eff.Outbox[0].Payload) != codec.MaxOutboxPayloadSize {
		t.Fatalf("CV-LIMIT-11: want an L2Delivery (got %T) with a %d-byte payload", eff.Outbox[0].Target, codec.MaxOutboxPayloadSize)
	}
}

// checkCVLIMIT12 pins the 33rd message: the repair drops it.
func checkCVLIMIT12(t *testing.T, vec vectorJSON) {
	t.Helper()
	checkOneDefect(t, vec, codec.ErrLimitExceeded, func(b []byte) []byte {
		n := len(b)
		extra, err := codec.DecodeOutboxMessage(b[n-117 : n-36])
		if err != nil {
			t.Fatalf("CV-LIMIT-12: the 33rd message does not decode: %v", err)
		}
		if _, ok := extra.Target.(codec.L2Delivery); !ok || !bytes.Equal(extra.SourceContract[:], mustHex(t, vec.Receipt.ContractID)) || extra.InputIndex != binary.BigEndian.Uint64(b[2:10]) || extra.OutboxIndex != codec.MaxOutboxPerExecution {
			t.Fatalf("CV-LIMIT-12: the 33rd message is %+v, want an L2Delivery from contract_id %s with the blob's input index and outbox_index %d", extra, vec.Receipt.ContractID, codec.MaxOutboxPerExecution)
		}
		out := append(append([]byte{}, b[:n-117]...), b[n-36:]...)
		binary.BigEndian.PutUint32(out[18:22], 32)
		return out
	})
}

// checkCVLIMIT13 pins the StateDiff value of exactly MaxStateValueSize bytes that is accepted.
func checkCVLIMIT13(t *testing.T, vec vectorJSON) {
	t.Helper()
	eff := checkConditions(t, vec, "SUCCESS", noConditions())
	if len(eff.StateDiff) != 1 || len(eff.StateDiff[0].Value) != codec.MaxStateValueSize {
		t.Fatalf("CV-LIMIT-13: want exactly one StateDiff entry with a %d-byte value", codec.MaxStateValueSize)
	}
	checkPathGas(t, vec, 1, 0, []byte{0x42, 0x0B})
}

// checkCVDECODE12 pins version 0 in the first two bytes.
func checkCVDECODE12(t *testing.T, vec vectorJSON) {
	t.Helper()
	checkOneDefect(t, vec, codec.ErrUnknownVersion, func(b []byte) []byte {
		if b[0] != 0x00 || b[1] != 0x00 {
			t.Fatalf("CV-DECODE-12: version bytes %x, want 0000", b[0:2])
		}
		b[0], b[1] = 0x00, 0x01
		return b
	})
}

// checkCVDECODE13 pins the lone event's event_index 1 at [58:62].
func checkCVDECODE13(t *testing.T, vec vectorJSON) {
	t.Helper()
	checkOneDefect(t, vec, codec.ErrIndexGap, func(b []byte) []byte {
		if got := binary.BigEndian.Uint32(b[58:62]); got != 1 {
			t.Fatalf("CV-DECODE-13: event_index %d, want 1", got)
		}
		binary.BigEndian.PutUint32(b[58:62], 0)
		return b
	})
}

// checkCVDECODE14 pins the lone message's outbox_index 1 at [62:66].
func checkCVDECODE14(t *testing.T, vec vectorJSON) {
	t.Helper()
	checkOneDefect(t, vec, codec.ErrIndexGap, func(b []byte) []byte {
		if got := binary.BigEndian.Uint32(b[62:66]); got != 1 {
			t.Fatalf("CV-DECODE-14: outbox_index %d, want 1", got)
		}
		binary.BigEndian.PutUint32(b[62:66], 0)
		return b
	})
}

// checkCVDECODE15 pins kind 2 at [66] on a withdrawal-shaped body: the repair to kind 1 decodes.
func checkCVDECODE15(t *testing.T, vec vectorJSON) {
	t.Helper()
	checkOneDefect(t, vec, codec.ErrInvalidKind, func(b []byte) []byte {
		if b[66] != 2 {
			t.Fatalf("CV-DECODE-15: kind %d, want 2", b[66])
		}
		b[66] = 1
		return b
	})
}

// checkCVDECODE16 pins the Events count 2 at [14:18] over a single event.
func checkCVDECODE16(t *testing.T, vec vectorJSON) {
	t.Helper()
	checkOneDefect(t, vec, codec.ErrOverflow, func(b []byte) []byte {
		if got := binary.BigEndian.Uint32(b[14:18]); got != 2 {
			t.Fatalf("CV-DECODE-16: Events count %d, want 2", got)
		}
		binary.BigEndian.PutUint32(b[14:18], 1)
		return b
	})
}

// checkCVDECODE17 pins the withdrawal's AssetID byte 0 at [87], the first byte that must be
// zero: byte [87] is the only non-zero byte in [0:22] and bytes [88:109] are zero.
func checkCVDECODE17(t *testing.T, vec vectorJSON) {
	t.Helper()
	checkOneDefect(t, vec, codec.ErrInvalidAssetID, func(b []byte) []byte {
		if b[87] == 0 || !bytes.Equal(b[88:109], make([]byte, 21)) {
			t.Fatalf("CV-DECODE-17: asset byte 0 = %#x, bytes [1:22] = %x, want a non-zero byte 0 and zeros", b[87], b[88:109])
		}
		b[87] = 0
		return b
	})
}

// permutedIndexRepair returns a repair for a three-element list whose elements start at
// first and are size bytes wide, each carrying a u32 index at offset indexAt within the
// element. It asserts the indices read 0, 2, 1, that the elements differ only in their index,
// and swaps the second and third index so the list reads 0, 1, 2.
func permutedIndexRepair(t *testing.T, id string, first, size, indexAt int) func(b []byte) []byte {
	return func(b []byte) []byte {
		at := func(i int) int { return first + i*size + indexAt }
		for i, want := range []uint32{0, 2, 1} {
			if got := binary.BigEndian.Uint32(b[at(i) : at(i)+4]); got != want {
				t.Fatalf("%s: element %d index %d, want %d", id, i, got, want)
			}
		}
		masked := func(i int) []byte {
			e := append([]byte(nil), b[first+i*size:first+(i+1)*size]...)
			copy(e[indexAt:indexAt+4], []byte{0, 0, 0, 0})
			return e
		}
		if !bytes.Equal(masked(0), masked(1)) || !bytes.Equal(masked(1), masked(2)) {
			t.Fatalf("%s: the three elements differ in more than their index", id)
		}
		binary.BigEndian.PutUint32(b[at(1):at(1)+4], 1)
		binary.BigEndian.PutUint32(b[at(2):at(2)+4], 2)
		return b
	}
}

// checkCVDECODE18 pins three events whose event_index values read 0, 2, 1: three 52-byte
// events from [18:174], each carrying its index at element offset 40.
func checkCVDECODE18(t *testing.T, vec vectorJSON) {
	t.Helper()
	checkOneDefect(t, vec, codec.ErrIndexGap, permutedIndexRepair(t, vec.ID, 18, 52, 40))
}

// checkCVDECODE19 pins three outbox messages whose outbox_index values read 0, 2, 1: three
// 81-byte messages from [22:265], each carrying its index at element offset 40.
func checkCVDECODE19(t *testing.T, vec vectorJSON) {
	t.Helper()
	checkOneDefect(t, vec, codec.ErrIndexGap, permutedIndexRepair(t, vec.ID, 22, 81, 40))
}

// checkStateDiffResult decodes the row's ExecutionResult and asserts its StateDiff is exactly
// the one entry (key, value), a nil value meaning the EMPTY sentinel.
func checkStateDiffResult(t *testing.T, vec vectorJSON, key string, value []byte) {
	t.Helper()
	er, err := codec.DecodeExecutionResult(mustHex(t, vec.ExecutionResultBlobHex))
	if err != nil {
		t.Fatalf("%s: DecodeExecutionResult: %v", vec.ID, err)
	}
	if len(er.StateDiff) != 1 || hex.EncodeToString(er.StateDiff[0].Key[:]) != key {
		t.Fatalf("%s: ExecutionResult.StateDiff = %+v, want exactly the entry at %s", vec.ID, er.StateDiff, key)
	}
	got := er.StateDiff[0].Value
	if (got == nil) != (value == nil) || !bytes.Equal(got, value) {
		t.Fatalf("%s: ExecutionResult.StateDiff value %x (nil %v), want %x (nil %v)", vec.ID, got, got == nil, value, value == nil)
	}
}

// checkCVSTATE1 pins the deletion of a key absent from pre_state: one nil-valued blob entry,
// its derived key and the quota key absent from pre_state, the entry committed, the root
// unchanged and no quota witness.
func checkCVSTATE1(t *testing.T, vec vectorJSON) {
	t.Helper()
	eff := checkConditions(t, vec, "SUCCESS", noConditions())
	if len(eff.StateDiff) != 1 || eff.StateDiff[0].Value != nil {
		t.Fatalf("CV-STATE-1: blob StateDiff = %+v, want exactly one entry with a nil value", eff.StateDiff)
	}
	key := hex.EncodeToString(crypto.Hash(mustHex(t, vec.Receipt.ContractID), eff.StateDiff[0].LocalKey))
	quotaKey, _, _ := quotaLeaf(t, vec)
	if _, ok := vec.PreState[key]; ok {
		t.Fatalf("CV-STATE-1: pre_state holds the deleted key %s", key)
	}
	if _, ok := vec.PreState[quotaKey]; ok {
		t.Fatalf("CV-STATE-1: pre_state holds the quota key %s", quotaKey)
	}
	for _, k := range vec.WitnessKeys {
		if k == quotaKey {
			t.Fatalf("CV-STATE-1: witness_keys holds the quota key %s", quotaKey)
		}
	}
	checkStateDiffResult(t, vec, key, nil)
	if vec.Receipt.StateDiffHash == zeroHashHex {
		t.Fatalf("CV-STATE-1: receipt.state_diff_hash is zero, want non-zero")
	}
	if vec.PostStateRoot != vec.PreStateRoot {
		t.Fatalf("CV-STATE-1: post_state_root %s, want pre_state_root %s", vec.PostStateRoot, vec.PreStateRoot)
	}
	checkPathGas(t, vec, 1, 0, []byte{0x42, 0x0B})
}

// checkCVSTATE2 pins the EMPTY sentinel at [22:26] followed by the stray bytes at [26:31].
func checkCVSTATE2(t *testing.T, vec vectorJSON) {
	t.Helper()
	checkOneDefect(t, vec, codec.ErrOverflow, func(b []byte) []byte {
		if hex.EncodeToString(b[22:26]) != "ffffffff" || hex.EncodeToString(b[26:31]) != "00000001ab" {
			t.Fatalf("CV-STATE-2: bytes [22:26] = %x and [26:31] = %x, want ffffffff and 00000001ab", b[22:26], b[26:31])
		}
		return append(append([]byte{}, b[:26]...), b[31:]...)
	})
}

// checkCVSTATE3 pins the rewrite of a key with its current value: the blob value equals the
// pre_state value, the entry is committed, and the root is unchanged.
func checkCVSTATE3(t *testing.T, vec vectorJSON) {
	t.Helper()
	eff := checkConditions(t, vec, "SUCCESS", noConditions())
	if len(eff.StateDiff) != 1 {
		t.Fatalf("CV-STATE-3: blob StateDiff has %d entries, want 1", len(eff.StateDiff))
	}
	key := hex.EncodeToString(crypto.Hash(mustHex(t, vec.Receipt.ContractID), eff.StateDiff[0].LocalKey))
	pre, ok := vec.PreState[key]
	if !ok || !bytes.Equal(mustHex(t, pre), eff.StateDiff[0].Value) {
		t.Fatalf("CV-STATE-3: pre_state value at %s is %q (present %v), want the blob value %x", key, pre, ok, eff.StateDiff[0].Value)
	}
	checkStateDiffResult(t, vec, key, eff.StateDiff[0].Value)
	if vec.Receipt.StateDiffHash == zeroHashHex {
		t.Fatalf("CV-STATE-3: receipt.state_diff_hash is zero, want non-zero")
	}
	if vec.PostStateRoot != vec.PreStateRoot {
		t.Fatalf("CV-STATE-3: post_state_root %s, want pre_state_root %s", vec.PostStateRoot, vec.PreStateRoot)
	}
	checkPathGas(t, vec, 1, 0, []byte{0x42, 0x0B})
}

// checkCVRESERVED1 pins four local keys that each sit next to a reserved key without equalling
// one: a one-byte prefix, two two-byte keys outside 0x0001 to 0x0005, and a three-byte key
// with a reserved key as its prefix.
func checkCVRESERVED1(t *testing.T, vec vectorJSON) {
	t.Helper()
	eff := checkConditions(t, vec, "SUCCESS", noConditions())
	want := [][]byte{{0x00}, {0x00, 0x00}, {0x00, 0x06}, {0x00, 0x01, 0x00}}
	if len(eff.StateDiff) != len(want) {
		t.Fatalf("CV-RESERVED-1: blob StateDiff has %d entries, want %d", len(eff.StateDiff), len(want))
	}
	// keys are the write keys decoded from the vector's own effects blob; every assertion
	// below reads them, not the literals in want.
	keys := make([][]byte, len(eff.StateDiff))
	for i, e := range eff.StateDiff {
		keys[i] = e.LocalKey
		if !bytes.Equal(keys[i], want[i]) {
			t.Fatalf("CV-RESERVED-1: local key %d = %x, want %x", i, keys[i], want[i])
		}
	}
	reserved := [][]byte{{0x00, 0x01}, {0x00, 0x02}, {0x00, 0x03}, {0x00, 0x04}, {0x00, 0x05}}
	for _, r := range reserved {
		if !bytes.HasPrefix(r, keys[0]) || len(r) <= len(keys[0]) {
			t.Fatalf("CV-RESERVED-1: blob key 0 %x is not a proper prefix of reserved key %x", keys[0], r)
		}
	}
	if !bytes.HasPrefix(keys[3], reserved[0]) || len(keys[3]) <= len(reserved[0]) {
		t.Fatalf("CV-RESERVED-1: reserved key 0001 is not a proper prefix of blob key 3 %x", keys[3])
	}
	for _, k := range keys[1:3] {
		if len(k) != 2 {
			t.Fatalf("CV-RESERVED-1: blob key %x is not two bytes long", k)
		}
		for _, r := range reserved {
			if bytes.Equal(k, r) {
				t.Fatalf("CV-RESERVED-1: blob key %x equals a reserved key", k)
			}
		}
		if k[0] == 0x00 && k[1] >= 0x01 && k[1] <= 0x05 {
			t.Fatalf("CV-RESERVED-1: blob key %x lies inside 0x0001 to 0x0005", k)
		}
	}
	checkPathGas(t, vec, 1, 0, []byte{0x42, 0x0B})
}

// checkCVQUOTA9 pins the absent quota leaf: purchased = used = 0, so the first write exceeds it.
func checkCVQUOTA9(t *testing.T, vec vectorJSON) {
	t.Helper()
	quotaKey, _, _ := quotaLeaf(t, vec)
	if _, ok := vec.PreState[quotaKey]; ok {
		t.Fatalf("CV-QUOTA-9: pre_state holds the quota key %s", quotaKey)
	}
	checkConditions(t, vec, "STORAGE_QUOTA_EXCEEDED", conditionsWith(func(c *blobConditions) { c.QuotaOver = true }))
	checkPathGas(t, vec, 1, 0, []byte{0x42, 0x0B})
}

// checkFailureRefund asserts status wantStatus, a non-zero input.deposit_amount and the
// deposit asset in withdrawal_caps. Check C5 (checkCallRefund) owns the refund's shape.
func checkFailureRefund(t *testing.T, vec vectorJSON, wantStatus string) {
	t.Helper()
	if vec.Status != wantStatus {
		t.Fatalf("%s: status %s, want %s", vec.ID, vec.Status, wantStatus)
	}
	if vec.Input == nil || new(big.Int).SetBytes(mustHex(t, vec.Input.DepositAmount)).Sign() == 0 {
		t.Fatalf("%s: input.deposit_amount is absent or zero, want a deposit", vec.ID)
	}
	if _, ok := vec.WithdrawalCaps[vec.Input.DepositAsset]; !ok {
		t.Fatalf("%s: withdrawal_caps has no entry for the deposit asset %s", vec.ID, vec.Input.DepositAsset)
	}
}

// checkEXECDEP6 pins a payable CallL2 with a payload and a partial claim: the refund of
// deposit - claim follows the contract's messages.
func checkEXECDEP6(t *testing.T, vec vectorJSON) {
	t.Helper()
	eff := checkConditions(t, vec, "SUCCESS", noConditions())
	if vec.InputKind != 0 || vec.Input == nil || vec.Input.PayloadHex == nil || *vec.Input.PayloadHex == "" {
		t.Fatalf("EXEC-DEP-6: want input_kind 0 with a non-empty payload_hex")
	}
	deposit := new(big.Int).SetBytes(mustHex(t, vec.Input.DepositAmount))
	claim := new(big.Int).SetBytes(eff.ClaimedDeposit[:])
	if claim.Sign() <= 0 || claim.Cmp(deposit) >= 0 {
		t.Fatalf("EXEC-DEP-6: claim %s, want 0 < claim < deposit %s", claim, deposit)
	}
	er, err := codec.DecodeExecutionResult(mustHex(t, vec.ExecutionResultBlobHex))
	if err != nil {
		t.Fatalf("EXEC-DEP-6: DecodeExecutionResult: %v", err)
	}
	n := len(eff.Outbox)
	if len(er.Outbox) != n+1 {
		t.Fatalf("EXEC-DEP-6: ExecutionResult has %d messages, want the blob's %d plus the refund", len(er.Outbox), n)
	}
	refund := er.Outbox[n]
	w, ok := refund.Target.(codec.L1Withdrawal)
	if !ok || refund.SourceContract != (types.Hash{}) || refund.OutboxIndex != uint32(n) || len(refund.Payload) != 0 {
		t.Fatalf("EXEC-DEP-6: last message %+v, want a zero-source L1Withdrawal at outbox_index %d with no payload", refund, n)
	}
	if hex.EncodeToString(w.RecipientL1[:]) != vec.L1Sender || hex.EncodeToString(w.Asset[:]) != vec.Input.DepositAsset {
		t.Fatalf("EXEC-DEP-6: refund to %x in %x, want l1_sender %s in %s", w.RecipientL1, w.Asset, vec.L1Sender, vec.Input.DepositAsset)
	}
	if want := new(big.Int).Sub(deposit, claim); new(big.Int).SetBytes(w.Amount[:]).Cmp(want) != 0 {
		t.Fatalf("EXEC-DEP-6: refund amount %x, want deposit - claim %s", w.Amount, want)
	}
	if int(vec.Receipt.OutboxCount) != n+1 {
		t.Fatalf("EXEC-DEP-6: receipt.outbox_count = %d, want %d", vec.Receipt.OutboxCount, n+1)
	}
	checkPathGas(t, vec, 1, 0, []byte{0x42, 0x0B})
}

// checkCVFRAME1 checks that the contract returns the frame's momentum
// hash and that its blob carries the frame's input index.
func checkCVFRAME1(t *testing.T, vec vectorJSON) {
	t.Helper()
	if vec.Status != "SUCCESS" {
		t.Fatalf("CV-FRAME-1: status %s, want SUCCESS", vec.Status)
	}
	er, err := codec.DecodeExecutionResult(mustHex(t, vec.ExecutionResultBlobHex))
	if err != nil {
		t.Fatalf("CV-FRAME-1: DecodeExecutionResult: %v", err)
	}
	if want := mustHex(t, vec.MomentumHash); !bytes.Equal(er.ReturnData, want) {
		t.Fatalf("CV-FRAME-1: ReturnData %x, want the momentum hash %x", er.ReturnData, want)
	}
	eff, err := codec.DecodeContractEffects(mustHex(t, vec.ContractEffectsBlobHex))
	if err != nil {
		t.Fatalf("CV-FRAME-1: DecodeContractEffects: %v", err)
	}
	if eff.GlobalInputIndex != vec.GlobalInputIndex {
		t.Fatalf("CV-FRAME-1: blob global_input_index %d, want %d", eff.GlobalInputIndex, vec.GlobalInputIndex)
	}
}

// localsEntryCharge is the entry charge of a function declaring MaxLocalsPerFunction locals,
// derived from testLocalsPerGas rather than module.LocalsEntryCost, which is the generator's own
// path.
const localsEntryCharge = (MaxLocalsPerFunction + testLocalsPerGas - 1) / testLocalsPerGas

// localsCallPath is the original opcodes the CV-LOCALS-* modules execute outside the callee's
// locals: call, i64.const, the callee's end and run's end.
var localsCallPath = []byte{0x10, 0x42, 0x0B, 0x0B}

// checkCVLOCALS1 checks that the call pays the callee's entry charge once: gas_used is the
// initial memory charge, the executed opcodes, the entry charge and the effect charge.
func checkCVLOCALS1(t *testing.T, vec vectorJSON) {
	t.Helper()
	checkPathGas(t, vec, 1, localsEntryCharge, localsCallPath)
}

// checkCVLOCALS2 checks that the budget sits one gas short of everything but the effect charge:
// the shared OUT_OF_GAS base, then a plasma_limit one below the entry segment's end.
func checkCVLOCALS2(t *testing.T, vec vectorJSON) {
	t.Helper()
	checkOutOfGasBase(t, vec)
	want := module.InitialMemoryCost(1) + localsEntryCharge - 1
	for _, op := range localsCallPath {
		cost, ok := module.OpcodeCost(module.Opcode(op))
		if !ok {
			t.Fatalf("no gas-table cost for opcode 0x%02X", op)
		}
		want += cost
	}
	if *vec.Input.PlasmaLimit != want {
		t.Fatalf("plasma_limit = %d, want %d", *vec.Input.PlasmaLimit, want)
	}
}

// abortCharge is abort's charge for dataLen bytes: 1 + 10 *
// ceil(dataLen / 32).
func abortCharge(n uint64) uint64 { return 1 + 10*((n+31)/32) }

// abortGas is the gas_used of a vector whose body is abortCallBody and whose abort carries
// dataLen bytes: the initial memory charge, the metered opcodes through i64.const and end, and
// abort's charge.
func abortGas(t *testing.T, dataLen uint64) uint64 {
	t.Helper()
	return module.InitialMemoryCost(1) + costOf(t, 0x41, 0x41, 0x41, 0x10, 0x42, 0x0B) + abortCharge(dataLen)
}

// checkRevertData decodes the vector's ExecutionResult, asserts it carries n bytes of
// ReturnData and, for n > 0, that module_wasm_hex contains them.
func checkRevertData(t *testing.T, vec vectorJSON, n int) {
	t.Helper()
	if vec.ExecutionResultBlobHex == "" {
		t.Fatalf("%s: execution_result_blob_hex is absent", vec.ID)
	}
	er, err := codec.DecodeExecutionResult(mustHex(t, vec.ExecutionResultBlobHex))
	if err != nil {
		t.Fatalf("%s: DecodeExecutionResult: %v", vec.ID, err)
	}
	if len(er.ReturnData) != n {
		t.Fatalf("%s: ExecutionResult.ReturnData has %d bytes, want %d", vec.ID, len(er.ReturnData), n)
	}
	if n > 0 && !strings.Contains(vec.ModuleWasmHex, hex.EncodeToString(er.ReturnData)) {
		t.Fatalf("%s: module_wasm_hex does not contain the revert data", vec.ID)
	}
}

// checkAbortRow pins an abort vector's status, receipt.abort_code and gas_used.
func checkAbortRow(t *testing.T, vec vectorJSON, status string, abortCode int32, gas uint64) {
	t.Helper()
	if vec.Status != status {
		t.Fatalf("%s: status %s, want %s", vec.ID, vec.Status, status)
	}
	if vec.Receipt.AbortCode != abortCode {
		t.Fatalf("%s: receipt.abort_code = %d, want %d", vec.ID, vec.Receipt.AbortCode, abortCode)
	}
	if vec.GasUsed != gas {
		t.Fatalf("%s: gas_used %d, want %d", vec.ID, vec.GasUsed, gas)
	}
}

// checkEXECDEP7 pins abort(7) with data_len 0 on a deposit input: REVERT, no blob, empty
// revert data, and the gas of the abort body.
func checkEXECDEP7(t *testing.T, vec vectorJSON) {
	t.Helper()
	checkFailureRefund(t, vec, "REVERT")
	if vec.Receipt.AbortCode != 7 {
		t.Fatalf("EXEC-DEP-7: receipt.abort_code = %d, want 7", vec.Receipt.AbortCode)
	}
	if vec.ContractEffectsBlobHex != "" {
		t.Fatalf("EXEC-DEP-7: contract_effects_blob_hex is present, want absent")
	}
	if want := abortGas(t, 0); vec.GasUsed != want {
		t.Fatalf("EXEC-DEP-7: gas_used %d, want %d", vec.GasUsed, want)
	}
	checkRevertData(t, vec, 0)
}

// checkEXECDEP8 pins the unaffordable initial-memory charge on a deposit input.
func checkEXECDEP8(t *testing.T, vec vectorJSON) {
	t.Helper()
	checkFailureRefund(t, vec, "OUT_OF_GAS")
	checkOutOfGasBase(t, vec)
	if *vec.Input.PlasmaLimit >= module.InitialMemoryCost(1) {
		t.Fatalf("EXEC-DEP-8: plasma_limit %d, want below InitialMemoryCost(1) %d", *vec.Input.PlasmaLimit, module.InitialMemoryCost(1))
	}
	if vec.ContractEffectsBlobHex != "" {
		t.Fatalf("EXEC-DEP-8: contract_effects_blob_hex is present, want absent")
	}
}

// checkEXECDEP9 pins the reserved-key write on a deposit input: the first entry is reserved,
// and gas_used is the module's metered gas.
func checkEXECDEP9(t *testing.T, vec vectorJSON) {
	t.Helper()
	checkFailureRefund(t, vec, "UNAUTHORIZED")
	checkConditions(t, vec, "UNAUTHORIZED", conditionsWith(func(c *blobConditions) { c.FirstReserved = 0 }))
	if want := blobModuleMetered(t, vec.ID); vec.GasUsed != want {
		t.Fatalf("EXEC-DEP-9: gas_used %d, want the metered gas %d", vec.GasUsed, want)
	}
}

// checkEXECDEP10 pins the two-page module that returns a MaxResultBlob + 1 byte blob:
// RESULT_TOO_LARGE at the metered gas of its two opcodes.
func checkEXECDEP10(t *testing.T, vec vectorJSON) {
	t.Helper()
	checkFailureRefund(t, vec, "RESULT_TOO_LARGE")
	if !strings.Contains(vec.ModuleWasmHex, hex.EncodeToString(returnBlobBody(blobOffset, MaxResultBlob+1))) {
		t.Fatalf("EXEC-DEP-10: module_wasm_hex does not return a blob of %d bytes", MaxResultBlob+1)
	}
	if want := module.InitialMemoryCost(2) + costOf(t, 0x42, 0x0B); vec.GasUsed != want {
		t.Fatalf("EXEC-DEP-10: gas_used %d, want %d", vec.GasUsed, want)
	}
}

// blobConditions is which content condition a decoded ContractEffects
// trips, evaluated from the vector's JSON fields alone.
type blobConditions struct {
	IndexMismatch      bool // step 2: blob globalInputIndex != global_input_index
	ForeignAttribution bool // step 2: an event contract_id or outbox source_contract != receipt.contract_id
	FirstReserved      int  // step 3: blob position of the first reserved local key, -1 if none
	FirstDuplicate     int  // step 3: position of the first entry repeating an earlier local key, -1 if none
	OverClaim          bool // step 4: claimed_deposit > input.deposit_amount
	FootprintOver      bool // step 5a: distinct withdrawal assets, plus the deposit asset when deposit_amount > 0 and not withdrawn, > MaxAssetsPerBatch
	CapOver            bool // step 5a: some asset's withdrawal total > its withdrawal_caps value (0 when absent)
	QuotaOver          bool // step 6a: net growth > 0 and used + growth > purchased
	ReturnDataOver     bool // step 7: len(ReturnData) > codec.MaxReturnData
}

// noConditions is the blobConditions of a blob that trips nothing.
func noConditions() blobConditions {
	return blobConditions{FirstReserved: -1, FirstDuplicate: -1}
}

// conditionsOf evaluates every blobConditions field for eff against the vector's own JSON
// fields. It shares no code with the runtime's validation.
func conditionsOf(t *testing.T, vec vectorJSON, eff codec.ContractEffects) blobConditions {
	t.Helper()
	got := noConditions()
	contractID := mustHex(t, vec.Receipt.ContractID)

	got.IndexMismatch = eff.GlobalInputIndex != vec.GlobalInputIndex
	for _, e := range eff.Events {
		if !bytes.Equal(e.ContractID[:], contractID) {
			got.ForeignAttribution = true
		}
	}
	for _, m := range eff.Outbox {
		if !bytes.Equal(m.SourceContract[:], contractID) {
			got.ForeignAttribution = true
		}
	}

	reservedLocal := [][]byte{{0x00, 0x01}, {0x00, 0x02}, {0x00, 0x03}, {0x00, 0x04}, {0x00, 0x05}}
	for i, e := range eff.StateDiff {
		if got.FirstReserved == -1 {
			for _, r := range reservedLocal {
				if bytes.Equal(e.LocalKey, r) {
					got.FirstReserved = i
				}
			}
		}
		if got.FirstDuplicate == -1 {
			for _, earlier := range eff.StateDiff[:i] {
				if bytes.Equal(e.LocalKey, earlier.LocalKey) {
					got.FirstDuplicate = i
					break
				}
			}
		}
	}

	deposit := new(big.Int)
	if vec.Input != nil && vec.Input.DepositAmount != "" {
		deposit.SetBytes(mustHex(t, vec.Input.DepositAmount))
	}
	got.OverClaim = new(big.Int).SetBytes(eff.ClaimedDeposit[:]).Cmp(deposit) > 0

	totals := map[string]*big.Int{}
	for _, m := range eff.Outbox {
		w, ok := m.Target.(codec.L1Withdrawal)
		if !ok {
			continue
		}
		asset := hex.EncodeToString(w.Asset[:])
		if totals[asset] == nil {
			totals[asset] = new(big.Int)
		}
		totals[asset].Add(totals[asset], new(big.Int).SetBytes(w.Amount[:]))
	}
	footprint := len(totals)
	if deposit.Sign() > 0 {
		if _, withdrawn := totals[vec.Input.DepositAsset]; !withdrawn {
			footprint++
		}
	}
	got.FootprintOver = footprint > codec.MaxAssetsPerBatch
	for asset, total := range totals {
		if total.Cmp(capOf(t, vec, asset)) > 0 {
			got.CapOver = true
		}
	}

	var growth int64
	for _, e := range eff.StateDiff {
		if e.Value != nil {
			growth += 64 + int64(len(e.Value))
		}
		if pre, present := vec.PreState[hex.EncodeToString(crypto.Hash(contractID, e.LocalKey))]; present {
			growth -= 64 + int64(len(pre)/2)
		}
	}
	_, purchased, used := quotaLeaf(t, vec)
	got.QuotaOver = growth > 0 && used+uint64(growth) > purchased

	got.ReturnDataOver = len(eff.ReturnData) > codec.MaxReturnData
	return got
}

// checkConditions asserts vec.Status == status, decodes the row's blob (fatal on error),
// asserts conditionsOf equals want, and returns the decoded blob.
func checkConditions(t *testing.T, vec vectorJSON, status string, want blobConditions) codec.ContractEffects {
	t.Helper()
	if vec.Status != status {
		t.Fatalf("%s: status %s, want %s", vec.ID, vec.Status, status)
	}
	eff, err := codec.DecodeContractEffects(mustHex(t, vec.ContractEffectsBlobHex))
	if err != nil {
		t.Fatalf("%s: DecodeContractEffects: %v", vec.ID, err)
	}
	if got := conditionsOf(t, vec, eff); !reflect.DeepEqual(got, want) {
		t.Fatalf("%s: blob conditions\n got  %+v\n want %+v", vec.ID, got, want)
	}
	return eff
}

// checkUpgradeCaller asserts that vec's pre_state holds an
// OWNER_UPGRADEABLE policy leaf and an owner leaf, and that the owner leaf
// is the caller exactly when wantOwner is set.
func checkUpgradeCaller(t *testing.T, vec vectorJSON, wantOwner bool) {
	t.Helper()
	contractID := mustHex(t, vec.Receipt.ContractID)
	key3 := hex.EncodeToString(crypto.Hash(contractID, []byte{0x00, 0x03}))
	key4 := hex.EncodeToString(crypto.Hash(contractID, []byte{0x00, 0x04}))
	if vec.PreState[key3] != "01" {
		t.Fatalf("%s: pre_state policy leaf = %q, want 01 (OWNER_UPGRADEABLE)", vec.ID, vec.PreState[key3])
	}
	ownerHex, ok := vec.PreState[key4]
	if !ok {
		t.Fatalf("%s: pre_state has no owner leaf", vec.ID)
	}
	caller := hashOf(crypto.Hash([]byte("L1_CALLER:"), mustHex(t, vec.L1Sender)))
	if isOwner := ownerHex == hex.EncodeToString(caller[:]); isOwner != wantOwner {
		t.Fatalf("%s: owner leaf is the caller = %v, want %v", vec.ID, isOwner, wantOwner)
	}
}

// upgradeKeys returns the hex keys of the code-hash and pre-code-hash leaves of vec's contract.
func upgradeKeys(t *testing.T, vec vectorJSON) (key1, key2 string) {
	t.Helper()
	contractID := mustHex(t, vec.Receipt.ContractID)
	return hex.EncodeToString(crypto.Hash(contractID, []byte{0x00, 0x01})), hex.EncodeToString(crypto.Hash(contractID, []byte{0x00, 0x02}))
}

func requireStatus(t *testing.T, vec vectorJSON, want string) {
	t.Helper()
	if vec.Status != want {
		t.Fatalf("%s: status = %s, want %s", vec.ID, vec.Status, want)
	}
}

func requireValid(t *testing.T, vec vectorJSON, moduleRaw []byte) {
	t.Helper()
	if err := module.Validate(moduleRaw, moduleLimits()); err != nil {
		t.Fatalf("%s: Validate: %v, want nil", vec.ID, err)
	}
}

func instrumentedHashOf(t *testing.T, vec vectorJSON, moduleRaw []byte) string {
	t.Helper()
	instrumented, err := module.Instrument(moduleRaw, moduleLimits())
	if err != nil {
		t.Fatalf("%s: Instrument: %v", vec.ID, err)
	}
	return hex.EncodeToString(crypto.Hash(instrumented))
}

// checkCVDEPLOY3 is the CV-DEPLOY-3 invariant: the module spans exactly two chunks, and
// assembling them in descending chunk_index order would not hash to input.code_hash.
func checkCVDEPLOY3(t *testing.T, vec vectorJSON) {
	t.Helper()
	requireStatus(t, vec, "SUCCESS")
	if vec.InputKind != 4 {
		t.Fatalf("CV-DEPLOY-3: input_kind = %d, want 4", vec.InputKind)
	}
	raw := mustHex(t, vec.ModuleWasmHex)
	if len(raw) <= MaxDeployChunkData || len(raw) > 2*MaxDeployChunkData {
		t.Fatalf("CV-DEPLOY-3: len(module) = %d, want in (%d, %d]", len(raw), MaxDeployChunkData, 2*MaxDeployChunkData)
	}
	swapped := append(append([]byte{}, raw[MaxDeployChunkData:]...), raw[:MaxDeployChunkData]...)
	if hex.EncodeToString(crypto.Hash(swapped)) == vec.Input.CodeHash {
		t.Fatalf("CV-DEPLOY-3: descending-order assembly hashes to input.code_hash, want a different hash")
	}
}

// checkCVDEPLOYHASH1 is the CV-DEPLOY-HASH-1 invariant: the assembly hash is the only
// failing check.
func checkCVDEPLOYHASH1(t *testing.T, vec vectorJSON) {
	t.Helper()
	requireStatus(t, vec, "VALIDATION_FAILED")
	if vec.InputKind != 4 {
		t.Fatalf("CV-DEPLOY-HASH-1: input_kind = %d, want 4", vec.InputKind)
	}
	raw := mustHex(t, vec.ModuleWasmHex)
	if hex.EncodeToString(crypto.Hash(raw)) == vec.Input.CodeHash {
		t.Fatalf("CV-DEPLOY-HASH-1: sha3(module) equals input.code_hash, want a mismatch")
	}
	requireValid(t, vec, raw)
	if _, err := codec.DecodeContractMetadata(mustHex(t, *vec.PendingMetadataHex)); err != nil {
		t.Fatalf("CV-DEPLOY-HASH-1: DecodeContractMetadata: %v, want nil", err)
	}
}

// checkCVUPGRADEUNAUTH2 is the CV-UPGRADE-UNAUTH-2 invariant: the owner leaf is not the
// caller and the module is valid.
func checkCVUPGRADEUNAUTH2(t *testing.T, vec vectorJSON) {
	t.Helper()
	requireStatus(t, vec, "UNAUTHORIZED")
	checkUpgradeCaller(t, vec, false)
	requireValid(t, vec, mustHex(t, vec.ModuleWasmHex))
}

// checkCVUPGRADEORDER1 is the CV-UPGRADE-ORDER-1 invariant: the module fails validation, so
// the UNAUTHORIZED status shows authorization ran first.
func checkCVUPGRADEORDER1(t *testing.T, vec vectorJSON) {
	t.Helper()
	requireStatus(t, vec, "UNAUTHORIZED")
	checkUpgradeCaller(t, vec, false)
	if module.Validate(mustHex(t, vec.ModuleWasmHex), moduleLimits()) == nil {
		t.Fatalf("CV-UPGRADE-ORDER-1: Validate = nil, want an error")
	}
}

// checkCVUPGRADEINVALID1 is the CV-UPGRADE-INVALID-1 invariant: the caller is the owner, the
// module fails validation and the receipt keeps the current CodeHash.
func checkCVUPGRADEINVALID1(t *testing.T, vec vectorJSON) {
	t.Helper()
	requireStatus(t, vec, "VALIDATION_FAILED")
	checkUpgradeCaller(t, vec, true)
	if module.Validate(mustHex(t, vec.ModuleWasmHex), moduleLimits()) == nil {
		t.Fatalf("CV-UPGRADE-INVALID-1: Validate = nil, want an error")
	}
	key1, _ := upgradeKeys(t, vec)
	if vec.Receipt.CodeHash != vec.PreState[key1] {
		t.Fatalf("CV-UPGRADE-INVALID-1: receipt.code_hash = %s, want pre_state's %s", vec.Receipt.CodeHash, vec.PreState[key1])
	}
}

// checkCVUPGRADEHASH1 is the CV-UPGRADE-HASH-1 invariant: the module is valid and
// instruments to a new CodeHash, so the assembly hash is the only cause of the refusal.
func checkCVUPGRADEHASH1(t *testing.T, vec vectorJSON) {
	t.Helper()
	requireStatus(t, vec, "VALIDATION_FAILED")
	checkUpgradeCaller(t, vec, true)
	raw := mustHex(t, vec.ModuleWasmHex)
	if hex.EncodeToString(crypto.Hash(raw)) == vec.Input.NewCodeHash {
		t.Fatalf("CV-UPGRADE-HASH-1: sha3(module) equals input.new_code_hash, want a mismatch")
	}
	requireValid(t, vec, raw)
	key1, _ := upgradeKeys(t, vec)
	if instrumentedHashOf(t, vec, raw) == vec.PreState[key1] {
		t.Fatalf("CV-UPGRADE-HASH-1: the module instruments to the current CodeHash, want a new one")
	}
	if vec.Receipt.CodeHash != vec.PreState[key1] {
		t.Fatalf("CV-UPGRADE-HASH-1: receipt.code_hash = %s, want pre_state's %s", vec.Receipt.CodeHash, vec.PreState[key1])
	}
}

// checkCVUPGRADESAMEHASH1 is the CV-UPGRADE-SAMEHASH-1 invariant: the staged module is the
// contract's current module.
func checkCVUPGRADESAMEHASH1(t *testing.T, vec vectorJSON) {
	t.Helper()
	requireStatus(t, vec, "VALIDATION_FAILED")
	checkUpgradeCaller(t, vec, true)
	raw := mustHex(t, vec.ModuleWasmHex)
	requireValid(t, vec, raw)
	key1, key2 := upgradeKeys(t, vec)
	if hex.EncodeToString(crypto.Hash(raw)) != vec.PreState[key2] {
		t.Fatalf("CV-UPGRADE-SAMEHASH-1: sha3(module) differs from pre_state's current module hash %s", vec.PreState[key2])
	}
	if got := instrumentedHashOf(t, vec, raw); got != vec.PreState[key1] || got != vec.Receipt.CodeHash {
		t.Fatalf("CV-UPGRADE-SAMEHASH-1: instrumented hash = %s, want pre_state's %s and receipt.code_hash %s", got, vec.PreState[key1], vec.Receipt.CodeHash)
	}
}

// checkCVUPGRADESAMEHASH2 is the CV-UPGRADE-SAMEHASH-2 invariant: the staged module differs
// from the current one in its source bytes but instruments to the current CodeHash.
func checkCVUPGRADESAMEHASH2(t *testing.T, vec vectorJSON) {
	t.Helper()
	requireStatus(t, vec, "VALIDATION_FAILED")
	checkUpgradeCaller(t, vec, true)
	raw := mustHex(t, vec.ModuleWasmHex)
	requireValid(t, vec, raw)
	key1, key2 := upgradeKeys(t, vec)
	if hex.EncodeToString(crypto.Hash(raw)) == vec.PreState[key2] {
		t.Fatalf("CV-UPGRADE-SAMEHASH-2: sha3(module) equals pre_state's current module hash, want a distinct source")
	}
	if got := instrumentedHashOf(t, vec, raw); got != vec.PreState[key1] || got != vec.Receipt.CodeHash {
		t.Fatalf("CV-UPGRADE-SAMEHASH-2: instrumented hash = %s, want pre_state's %s and receipt.code_hash %s", got, vec.PreState[key1], vec.Receipt.CodeHash)
	}
}

var namedInvariants = map[string]func(t *testing.T, vec vectorJSON){
	"EXEC-002":           checkN1WitnessBackendTripwire,
	"CV-FRAME-1":         checkCVFRAME1,
	"CV-LOCALS-1":        checkCVLOCALS1,
	"CV-LOCALS-2":        checkCVLOCALS2,
	"EXEC-FAIL-OUTBOX-1": checkN2DiscardNotVacuous,
	"EXEC-006":           checkEXEC006DescendingLocalKeys,
	"EXEC-012":           checkEXEC012AbsentRead,
	"CV-READ-2":          checkCVREAD2PresentRead,
	"EXEC-DEP-3":         checkEXECDEP3ClaimExceedsDeposit,
	"EXEC-DEP-4":         checkEXECDEP4ZeroDepositNonZeroClaim,
	"CV-OUTBOX-1":        checkCVOUTBOX1UndecodableBlob,
	"CV-ROOTS-1":         checkCVROOTS1RootsBlob,
	"CV-DUPKEY-1":        checkCVDUPKEY1DuplicateLocalKeys,
	"CV-UNAUTH-1":        checkCVUNAUTH1ReservedKeyDiscardNotVacuous,
	"CV-RTL-RETURN-1":    checkCVRTLRETURN1ReturnDataOversized,
	"CV-RTL-RESULT-1":    checkCVRTLRESULT1ResultTooLargeFromRefund,
	"CV-OOG-1":           checkCVOOG1,
	"CV-OOG-2":           checkCVOOG2,
	"CV-OOG-3":           checkCVOOG3,
	"CV-OOG-4":           checkCVOOG4,
	"CV-SEG-BRIF-1":      checkCVSEGBRIF1,
	"CV-SEG-IF-1":        checkCVSEGIF1,
	"CV-SEG-AFTEREND-1":  checkCVSEGAFTEREND1,
	"CV-SEG-OUTER-1":     checkCVSEGOUTER1,
	"CV-GROW-1":          checkCVGROW1,
	"CV-ASSETS-1":        checkCVASSETS1,
	"CV-ASSETS-2":        checkCVASSETS2,
	"CV-QUOTA-2": func(t *testing.T, vec vectorJSON) {
		checkPathGas(t, vec, 1, 0, []byte{0x42, 0x0B})
	},
	"CV-QUOTA-4":            checkCVQUOTA4ZeroDelta,
	"CV-DEPLOY-3":           checkCVDEPLOY3,
	"CV-DEPLOY-HASH-1":      checkCVDEPLOYHASH1,
	"CV-UPGRADE-UNAUTH-2":   checkCVUPGRADEUNAUTH2,
	"CV-UPGRADE-ORDER-1":    checkCVUPGRADEORDER1,
	"CV-UPGRADE-INVALID-1":  checkCVUPGRADEINVALID1,
	"CV-UPGRADE-HASH-1":     checkCVUPGRADEHASH1,
	"CV-UPGRADE-SAMEHASH-1": checkCVUPGRADESAMEHASH1,
	"CV-UPGRADE-SAMEHASH-2": checkCVUPGRADESAMEHASH2,
	"CV-QUOTA-5":            checkCVQUOTA5ReservedQuotaKey,
	"CV-CAP-1":              checkCVCAP1,
	"CV-CAP-2":              checkCVCAP2,
	"CV-CAP-3":              checkCVCAP3,
	"CV-CAP-4":              checkCVCAP4,
	"CV-CAP-5":              checkCVCAP5,
	"CV-CAP-6":              checkCVCAP6,
	"CV-CAP-7":              checkCVCAP7,
	"CV-CAP-8":              checkCVCAP8,
	"CV-CAP-9":              checkCVCAP9,
	"CV-CAP-10":             checkCVCAP10,
	"CV-CAP-11":             checkCVCAP11,
	"CV-CAP-12":             checkCVCAP12,
	"CV-ASSETS-3":           checkCVASSETS3,
	"CV-ASSETS-4":           checkCVASSETS4,
	"CV-ASSETS-5":           checkCVASSETS5,
	"CV-ORDER-1":            checkCVORDER1,
	"CV-ORDER-2":            checkCVORDER2,
	"CV-ORDER-3":            checkCVORDER3,
	"CV-ORDER-4":            checkCVORDER4,
	"CV-ORDER-5":            checkCVORDER5,
	"CV-ORDER-6":            checkCVORDER6,
	"CV-ORDER-7":            checkCVORDER7,
	"CV-ORDER-8":            checkCVORDER8,
	"CV-ORDER-9":            checkCVORDER9,
	"CV-ORDER-10":           checkCVORDER10,
	"CV-ORDER-11":           checkCVORDER11,
	"CV-ORDER-12":           checkCVORDER12,
	"CV-ORDER-13":           checkCVORDER13,
	"CV-ORDER-14":           checkCVORDER14,
	"CV-ORDER-15":           checkCVORDER15,
	"CV-ORDER-16":           checkCVORDER16,
	"CV-ATTR-1":             checkCVATTR1,
	"CV-ATTR-2":             checkCVATTR2,
	"CV-ATTR-3":             checkCVATTR3,
	"CV-DECODE-1":           checkCVDECODE1,
	"CV-DECODE-2":           checkCVDECODE2,
	"CV-DECODE-3":           checkCVDECODE3,
	"CV-DECODE-4":           checkCVDECODE4,
	"CV-DECODE-5":           checkCVDECODE5,
	"CV-DECODE-6":           checkCVDECODE6,
	"CV-DECODE-7":           checkCVDECODE7,
	"CV-DECODE-8":           checkCVDECODE8,
	"CV-DECODE-9":           checkCVDECODE9,
	"CV-DECODE-10":          checkCVDECODE10,
	"CV-DECODE-11":          checkCVDECODE11,
	"CV-LIMIT-1":            checkCVLIMIT1,
	"CV-LIMIT-2":            checkCVLIMIT2,
	"CV-LIMIT-3":            checkCVLIMIT3,
	"CV-LIMIT-4":            checkCVLIMIT4,
	"CV-LIMIT-5":            checkCVLIMIT5,
	"CV-LIMIT-6":            checkCVLIMIT6,
	"CV-LIMIT-7":            checkCVLIMIT7,
	"CV-LIMIT-8":            checkCVLIMIT8,
	"CV-LIMIT-9":            checkCVLIMIT9,
	"CV-LIMIT-10":           checkCVLIMIT10,
	"CV-LIMIT-11":           checkCVLIMIT11,
	"CV-LIMIT-12":           checkCVLIMIT12,
	"CV-LIMIT-13":           checkCVLIMIT13,
	"CV-DECODE-12":          checkCVDECODE12,
	"CV-DECODE-13":          checkCVDECODE13,
	"CV-DECODE-14":          checkCVDECODE14,
	"CV-DECODE-15":          checkCVDECODE15,
	"CV-DECODE-16":          checkCVDECODE16,
	"CV-DECODE-17":          checkCVDECODE17,
	"CV-DECODE-18":          checkCVDECODE18,
	"CV-DECODE-19":          checkCVDECODE19,
	"CV-STATE-1":            checkCVSTATE1,
	"CV-STATE-2":            checkCVSTATE2,
	"CV-STATE-3":            checkCVSTATE3,
	"CV-RESERVED-1":         checkCVRESERVED1,
	"CV-QUOTA-9":            checkCVQUOTA9,
	"EXEC-DEP-6":            checkEXECDEP6,
	"EXEC-DEP-7":            checkEXECDEP7,
	"EXEC-DEP-8":            checkEXECDEP8,
	"EXEC-DEP-9":            checkEXECDEP9,
	"EXEC-DEP-10":           checkEXECDEP10,
	"CV-ABORT-1": func(t *testing.T, vec vectorJSON) {
		checkAbortRow(t, vec, "REVERT", 7, abortGas(t, 40))
		checkRevertData(t, vec, 40)
	},
	"CV-ABORT-2": func(t *testing.T, vec vectorJSON) {
		checkAbortRow(t, vec, "REVERT", 7, abortGas(t, 0))
		checkRevertData(t, vec, 0)
	},
	"CV-ABORT-3": func(t *testing.T, vec vectorJSON) {
		checkAbortRow(t, vec, "REVERT", 7, abortGas(t, 4096))
		checkRevertData(t, vec, 4096)
	},
	"CV-ABORT-4": func(t *testing.T, vec vectorJSON) {
		checkAbortRow(t, vec, "RESULT_TOO_LARGE", 0, abortGas(t, 4097))
	},
	"CV-ABORT-5": func(t *testing.T, vec vectorJSON) {
		checkAbortRow(t, vec, "RUNTIME_FAULT", 0, abortGas(t, 32))
	},
	"CV-ABORT-6": func(t *testing.T, vec vectorJSON) {
		checkAbortRow(t, vec, "OUT_OF_GAS", 0, abortGas(t, 64)-1)
		if got := *vec.Input.PlasmaLimit; got != vec.GasUsed {
			t.Fatalf("CV-ABORT-6: plasma_limit %d, want gas_used %d", got, vec.GasUsed)
		}
		if got := *vec.Input.PlasmaLimit; got < abortGas(t, 0) {
			t.Fatalf("CV-ABORT-6: plasma_limit %d, want >= abortGas(0) %d", got, abortGas(t, 0))
		}
	},
	"EXEC-DEP-11": func(t *testing.T, vec vectorJSON) {
		checkFailureRefund(t, vec, "REVERT")
		if vec.InputKind != 0 {
			t.Fatalf("EXEC-DEP-11: input_kind %d, want 0", vec.InputKind)
		}
		checkAbortRow(t, vec, "REVERT", 7, abortGas(t, 40))
		checkRevertData(t, vec, 40)
	},

	"CV-TRAP-DIV-1":   checkCVTRAPDIV1,
	"CV-TRAP-OVF-1":   checkCVTRAPOVF1,
	"CV-TRAP-MEM-1":   checkCVTRAPMEM1,
	"CV-TRAP-TABLE-1": checkCVTRAPTABLE1,
	"CV-TRAP-NULL-1":  checkCVTRAPNULL1,
	"CV-TRAP-SIG-1":   checkCVTRAPSIG1,
	"CV-NEG-1":        checkCVNEG1,
	"CV-NEG-2":        checkCVNEG2,
	"CV-FRAME-2":      checkCVFRAME2,
	"CV-FRAME-3":      checkCVFRAME3,
	"CV-GROW-2":       checkCVGROW2,
	"CV-GROW-3":       checkCVGROW3,
	"CV-GROW-4":       checkCVGROW4,
	"CV-GROW-5":       checkCVGROW5,
	"CV-READ-3":       checkCVREAD3,
	"CV-READ-4":       checkCVREAD4,
	"CV-READ-5":       checkCVREAD5,
	"CV-READ-6":       checkCVREAD6,
	"CV-READ-7":       checkCVREAD7,
	"CV-EVENTS-1":     checkCVEVENTS1,
	"EXEC-DEP-12":     checkEXECDEP12,
	"CV-MEM-MIN-1":    checkCVMEMMIN1,

	"CV-STRUCT-TYPES-1":       checkCVSTRUCTTYPES1,
	"CV-STRUCT-TYPEPARAMS-1":  checkCVSTRUCTTYPEPARAMS1,
	"CV-STRUCT-PARAMS-1":      checkCVSTRUCTPARAMS1,
	"CV-STRUCT-FUNCS-1":       checkCVSTRUCTFUNCS1,
	"CV-STRUCT-TABLE-1":       checkCVSTRUCTTABLE1,
	"CV-STRUCT-GLOBALS-1":     checkCVSTRUCTGLOBALS1,
	"CV-STRUCT-ELEMS-1":       checkCVSTRUCTELEMS1,
	"CV-STRUCT-DATA-1":        checkCVSTRUCTDATA1,
	"CV-STRUCT-LOCALS-1":      checkCVSTRUCTLOCALS1,
	"CV-STRUCT-TOTALLOCALS-1": checkCVSTRUCTTOTALLOCALS1,
	"CV-STRUCT-ATCAP-1":       checkCVSTRUCTATCAP1,
	"CV-FIT-DATA-1":           checkCVFITDATA1,
	"CV-FIT-ELEM-1":           checkCVFITELEM1,
	"CV-IMPORT-MODULE-1":      checkCVIMPORTMODULE1,
	"CV-IMPORT-NAME-1":        checkCVIMPORTNAME1,
	"CV-IMPORT-DUP-1":         checkCVIMPORTDUP1,
	"CV-IMPORT-SIG-1":         checkCVIMPORTSIG1,
	"CV-IMPORT-KIND-1":        checkCVIMPORTKIND1,
	"CV-SECTION-START-1":      checkCVSECTIONSTART1,
	"CV-SECTION-CUSTOM-1":     checkCVSECTIONCUSTOM1,
	"CV-SECTION-CUSTOM-2":     checkCVSECTIONCUSTOM2,
	"CV-EXPORT-MUTGLOBAL-1":   checkCVEXPORTMUTGLOBAL1,
	"CV-EXPORT-START-1":       checkCVEXPORTSTART1,
	"CV-EXPORT-ZENON-1":       checkCVEXPORTZENON1,
	"CV-EXPORT-NORUN-1":       checkCVEXPORTNORUN1,
	"CV-EXPORT-NOMEMORY-1":    checkCVEXPORTNOMEMORY1,
	"CV-EXPORT-RUNSIG-1":      checkCVEXPORTRUNSIG1,
	"CV-EXPORT-RUNIMPORT-1":   checkCVEXPORTRUNIMPORT1,
	"CV-INSTR-SIZE-1":         checkCVINSTRSIZE1,
	"CV-LEB-PADDED-1":         checkCVLEBPADDED1,

	"CV-DEPOSIT-UNDEPLOYED-1": func(t *testing.T, vec vectorJSON) { checkUndeployedRow(t, vec, 1) },
	"CV-CALL-UNDEPLOYED-1":    func(t *testing.T, vec vectorJSON) { checkUndeployedRow(t, vec, 0) },
}

// TestConformanceVectors re-derives every committed value in the
// artifact from the JSON fields alone, by a second path. It must not
// call refDerive or any ref* helper, must not use codec.MerkleRoot,
// codec.Receipt.Encode or plugins/wasm's derive/CodeHash/callerHash for
// a value it is re-deriving, and must not use mapState.
func TestConformanceVectors(t *testing.T) {
	data, err := os.ReadFile(vectorFilePath)
	if err != nil {
		t.Fatalf("ReadFile(%s): %v", vectorFilePath, err)
	}
	var file vectorFile
	if err := json.Unmarshal(data, &file); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}

	checkFileLevel(t, file)

	seen := make(map[string]bool, len(namedInvariants))
	for _, vec := range file.Vectors {
		vec := vec
		t.Run(vec.ID, func(t *testing.T) {
			checkPerVector(t, vec)
			checkShape(t, file, vec)
			if fn, ok := namedInvariants[vec.ID]; ok {
				fn(t, vec)
			}
		})
		if _, ok := namedInvariants[vec.ID]; ok {
			seen[vec.ID] = true
		}
	}

	for id := range namedInvariants {
		if !seen[id] {
			t.Fatalf("named invariant for vector %q was never exercised: the artifact no longer contains that vector", id)
		}
	}
}

// capVariant re-derives step 5a for one vector: it reports whether the implementation faults.
// outbox is the withdrawals in outbox order, sorted the distinct assets ascending, seen the
// distinct assets in first-appearance order, totals the per-asset totals and capOf the cap of an
// asset (0 when absent).
type capVariant func(outbox []codec.L1Withdrawal, sorted, seen []string, totals map[string]*big.Int, capOf func(string) *big.Int) bool

// capVariants returns the control step 5a check and every wrong implementation the cap vectors
// must reject, by name.
func capVariants() map[string]capVariant {
	over := func(total, limit *big.Int) bool { return total.Cmp(limit) > 0 }
	// only checks the first or the last asset of the chosen list and no other.
	only := func(pick func(sorted, seen []string) []string, last bool) capVariant {
		return func(_ []codec.L1Withdrawal, sorted, seen []string, totals map[string]*big.Int, c func(string) *big.Int) bool {
			list := pick(sorted, seen)
			a := list[0]
			if last {
				a = list[len(list)-1]
			}
			return over(totals[a], c(a))
		}
	}
	sortedList := func(sorted, _ []string) []string { return sorted }
	seenList := func(_, seen []string) []string { return seen }
	// shared applies one cap, chosen from the distinct assets' caps in sorted order, to every asset.
	shared := func(choose func(caps []*big.Int) *big.Int) capVariant {
		return func(_ []codec.L1Withdrawal, sorted, _ []string, totals map[string]*big.Int, c func(string) *big.Int) bool {
			var caps []*big.Int
			for _, a := range sorted {
				caps = append(caps, c(a))
			}
			limit := choose(caps)
			for _, a := range sorted {
				if over(totals[a], limit) {
					return true
				}
			}
			return false
		}
	}
	maxCap := func(caps []*big.Int) *big.Int {
		m := caps[0]
		for _, c := range caps {
			if c.Cmp(m) > 0 {
				m = c
			}
		}
		return m
	}
	minCap := func(caps []*big.Int) *big.Int {
		m := caps[0]
		for _, c := range caps {
			if c.Cmp(m) < 0 {
				m = c
			}
		}
		return m
	}
	sumCap := func(caps []*big.Int) *big.Int {
		s := new(big.Int)
		for _, c := range caps {
			s.Add(s, c)
		}
		return s
	}
	// zip fetches one cap per asset name fetch returns and applies the caps by index to the
	// distinct assets in the order of to.
	zip := func(fetch func(outbox []codec.L1Withdrawal, sorted []string) []string, to func(sorted, seen []string) []string) capVariant {
		return func(outbox []codec.L1Withdrawal, sorted, seen []string, totals map[string]*big.Int, c func(string) *big.Int) bool {
			names := fetch(outbox, sorted)
			for i, a := range to(sorted, seen) {
				if over(totals[a], c(names[i])) {
					return true
				}
			}
			return false
		}
	}
	// reorder fetches the caps in the order of from and applies them by index in the order of to.
	reorder := func(from, to func(sorted, seen []string) []string) capVariant {
		return func(_ []codec.L1Withdrawal, sorted, seen []string, totals map[string]*big.Int, c func(string) *big.Int) bool {
			src, dst := from(sorted, seen), to(sorted, seen)
			for i, a := range dst {
				if over(totals[a], c(src[i])) {
					return true
				}
			}
			return false
		}
	}

	// mostWithdrawn checks only the asset with the most withdrawals; of equal counts the one later in
	// the chosen list wins.
	mostWithdrawn := func(pick func(sorted, seen []string) []string) capVariant {
		return func(outbox []codec.L1Withdrawal, sorted, seen []string, totals map[string]*big.Int, c func(string) *big.Int) bool {
			count := map[string]int{}
			for _, w := range outbox {
				count[hex.EncodeToString(w.Asset[:])]++
			}
			list := pick(sorted, seen)
			a := list[0]
			for _, b := range list {
				if count[b] >= count[a] {
					a = b
				}
			}
			return over(totals[a], c(a))
		}
	}

	// truncatedKey sums exact amounts under the asset ID bytes [from:to] alone, so assets that
	// agree on those bytes share one total, and checks each asset against its own cap.
	truncatedKey := func(from, to int) capVariant {
		return func(outbox []codec.L1Withdrawal, sorted, _ []string, _ map[string]*big.Int, c func(string) *big.Int) bool {
			sums := map[string]*big.Int{}
			keyOf := map[string]string{}
			for _, w := range outbox {
				key := string(w.Asset[from:to])
				keyOf[hex.EncodeToString(w.Asset[:])] = key
				if sums[key] == nil {
					sums[key] = new(big.Int)
				}
				sums[key].Add(sums[key], new(big.Int).SetBytes(w.Amount[:]))
			}
			for _, a := range sorted {
				if over(sums[keyOf[a]], c(a)) {
					return true
				}
			}
			return false
		}
	}
	// carryDropped sums each asset's withdrawals in outbox order, losing the carry out of bit k at
	// every step, and compares the sum to the exact cap.
	carryDropped := func(k uint) capVariant {
		return func(outbox []codec.L1Withdrawal, _, seen []string, _ map[string]*big.Int, c func(string) *big.Int) bool {
			for _, a := range seen {
				var amounts []*big.Int
				for _, w := range outbox {
					if hex.EncodeToString(w.Asset[:]) == a {
						amounts = append(amounts, new(big.Int).SetBytes(w.Amount[:]))
					}
				}
				if over(carryDroppedSum(amounts, k), c(a)) {
					return true
				}
			}
			return false
		}
	}
	// wrapped keeps each asset's running sum, and the cap it is compared to, modulo 2^bits.
	wrapped := func(bits uint) capVariant {
		return func(outbox []codec.L1Withdrawal, sorted, _ []string, _ map[string]*big.Int, c func(string) *big.Int) bool {
			mod := new(big.Int).Lsh(big.NewInt(1), bits)
			sums := map[string]*big.Int{}
			for _, w := range outbox {
				a := hex.EncodeToString(w.Asset[:])
				if sums[a] == nil {
					sums[a] = new(big.Int)
				}
				sums[a].Add(sums[a], new(big.Int).SetBytes(w.Amount[:])).Mod(sums[a], mod)
			}
			for _, a := range sorted {
				if over(sums[a], new(big.Int).Mod(c(a), mod)) {
					return true
				}
			}
			return false
		}
	}

	variants := map[string]capVariant{
		"control": func(_ []codec.L1Withdrawal, sorted, _ []string, totals map[string]*big.Int, c func(string) *big.Int) bool {
			for _, a := range sorted {
				if over(totals[a], c(a)) {
					return true
				}
			}
			return false
		},
		"totals keyed by the asset ID's first 10 bytes": truncatedKey(0, 10),
		"totals keyed by the asset ID's low 8 bytes":    truncatedKey(24, 32),
		"totals keyed by the asset ID's high ZTS bytes": truncatedKey(22, 30),
		// identical withdrawals are one withdrawal: a repeat of all three fields is skipped.
		"identical withdrawals summed once": func(outbox []codec.L1Withdrawal, sorted, _ []string, _ map[string]*big.Int, c func(string) *big.Int) bool {
			sums := map[string]*big.Int{}
			for i, w := range outbox {
				if slices.Contains(outbox[:i], w) {
					continue
				}
				a := hex.EncodeToString(w.Asset[:])
				if sums[a] == nil {
					sums[a] = new(big.Int)
				}
				sums[a].Add(sums[a], new(big.Int).SetBytes(w.Amount[:]))
			}
			for _, a := range sorted {
				if over(sums[a], c(a)) {
					return true
				}
			}
			return false
		},
		"wrapping u256 sum":          wrapped(256),
		"u64 sum":                    wrapped(64),
		"u128 sum":                   wrapped(128),
		"carry dropped at 2^64":      carryDropped(64),
		"carry dropped at 2^128":     carryDropped(128),
		"carry dropped at 2^192":     carryDropped(192),
		"first sorted asset only":    only(sortedList, false),
		"last sorted asset only":     only(sortedList, true),
		"first outbox asset only":    only(seenList, false),
		"last outbox asset only":     only(seenList, true),
		"sorted fetch, outbox apply": reorder(sortedList, seenList),
		"outbox fetch, sorted apply": reorder(seenList, sortedList),
		"no per-asset totals": func(outbox []codec.L1Withdrawal, _, _ []string, _ map[string]*big.Int, c func(string) *big.Int) bool {
			for _, w := range outbox {
				if over(new(big.Int).SetBytes(w.Amount[:]), c(hex.EncodeToString(w.Asset[:]))) {
					return true
				}
			}
			return false
		},
		"one cap per withdrawal, outbox order": zip(func(outbox []codec.L1Withdrawal, _ []string) []string {
			var out []string
			for _, w := range outbox {
				out = append(out, hex.EncodeToString(w.Asset[:]))
			}
			return out
		}, sortedList),
		"one cap per withdrawal, outbox order, applied in first-appearance order": zip(func(outbox []codec.L1Withdrawal, _ []string) []string {
			var out []string
			for _, w := range outbox {
				out = append(out, hex.EncodeToString(w.Asset[:]))
			}
			return out
		}, seenList),
		"one cap per withdrawal, sorted order": zip(func(outbox []codec.L1Withdrawal, _ []string) []string {
			var out []string
			for _, w := range outbox {
				out = append(out, hex.EncodeToString(w.Asset[:]))
			}
			sort.Strings(out)
			return out
		}, sortedList),
		"one cap per withdrawal, sorted order, applied in first-appearance order": zip(func(outbox []codec.L1Withdrawal, _ []string) []string {
			var out []string
			for _, w := range outbox {
				out = append(out, hex.EncodeToString(w.Asset[:]))
			}
			sort.Strings(out)
			return out
		}, seenList),
		"last withdrawal's asset only": func(outbox []codec.L1Withdrawal, _, _ []string, totals map[string]*big.Int, c func(string) *big.Int) bool {
			a := hex.EncodeToString(outbox[len(outbox)-1].Asset[:])
			return over(totals[a], c(a))
		},
		"largest-total asset only": func(_ []codec.L1Withdrawal, sorted, _ []string, totals map[string]*big.Int, c func(string) *big.Int) bool {
			a := sorted[0]
			for _, b := range sorted {
				if totals[b].Cmp(totals[a]) > 0 {
					a = b
				}
			}
			return over(totals[a], c(a))
		},
		"second withdrawal's asset only": func(outbox []codec.L1Withdrawal, _, _ []string, totals map[string]*big.Int, c func(string) *big.Int) bool {
			a := hex.EncodeToString(outbox[min(1, len(outbox)-1)].Asset[:])
			return over(totals[a], c(a))
		},
		"most-withdrawn asset only, a tie going to the later asset in first-appearance order": mostWithdrawn(seenList),
		"most-withdrawn asset only, a tie going to the later asset in sorted order":           mostWithdrawn(sortedList),
		"first cap for every asset":   shared(func(caps []*big.Int) *big.Int { return caps[0] }),
		"last cap for every asset":    shared(func(caps []*big.Int) *big.Int { return caps[len(caps)-1] }),
		"max cap for every asset":     shared(maxCap),
		"min cap for every asset":     shared(minCap),
		"sum of caps for every asset": shared(sumCap),
		"grand total against summed caps": func(_ []codec.L1Withdrawal, sorted, _ []string, totals map[string]*big.Int, c func(string) *big.Int) bool {
			sum, limit := new(big.Int), new(big.Int)
			for _, a := range sorted {
				sum.Add(sum, totals[a])
				limit.Add(limit, c(a))
			}
			return over(sum, limit)
		},
		"grand total against max cap": func(_ []codec.L1Withdrawal, sorted, _ []string, totals map[string]*big.Int, c func(string) *big.Int) bool {
			sum := new(big.Int)
			var caps []*big.Int
			for _, a := range sorted {
				sum.Add(sum, totals[a])
				caps = append(caps, c(a))
			}
			return over(sum, maxCap(caps))
		},
		"stop after first asset": func(_ []codec.L1Withdrawal, sorted, _ []string, totals map[string]*big.Int, c func(string) *big.Int) bool {
			return over(totals[sorted[0]], c(sorted[0]))
		},
		"previous asset's cap": func(_ []codec.L1Withdrawal, sorted, _ []string, totals map[string]*big.Int, c func(string) *big.Int) bool {
			for i, a := range sorted {
				limit := c(a)
				if i > 0 {
					limit = c(sorted[i-1])
				}
				if over(totals[a], limit) {
					return true
				}
			}
			return false
		},
	}

	// Every pairing of two different orders of the distinct assets: the caps are fetched in the
	// order of one and applied by index in the order of the other.
	orders := map[string]func(outbox []codec.L1Withdrawal, sorted, seen []string) []string{
		"sorted":                    func(_ []codec.L1Withdrawal, sorted, _ []string) []string { return sorted },
		"reversed sorted":           func(_ []codec.L1Withdrawal, sorted, _ []string) []string { return reversed(sorted) },
		"first-appearance":          func(_ []codec.L1Withdrawal, _, seen []string) []string { return seen },
		"reversed first-appearance": func(_ []codec.L1Withdrawal, _, seen []string) []string { return reversed(seen) },
		"last-appearance":           func(outbox []codec.L1Withdrawal, _, _ []string) []string { return lastAppearance(outbox) },
		"reversed last-appearance":  func(outbox []codec.L1Withdrawal, _, _ []string) []string { return reversed(lastAppearance(outbox)) },
	}
	for fromName, from := range orders {
		for toName, to := range orders {
			if fromName == toName {
				continue
			}
			from, to := from, to
			variants["caps fetched in "+fromName+" order, applied in "+toName+" order"] = func(outbox []codec.L1Withdrawal, sorted, seen []string, totals map[string]*big.Int, c func(string) *big.Int) bool {
				src := from(outbox, sorted, seen)
				for i, a := range to(outbox, sorted, seen) {
					if over(totals[a], c(src[i])) {
						return true
					}
				}
				return false
			}
		}
	}
	return variants
}

// reversed returns a copy of list in reverse order.
func reversed(list []string) []string {
	out := slices.Clone(list)
	slices.Reverse(out)
	return out
}

// lastAppearance returns the distinct assets of outbox ordered by their last withdrawal.
func lastAppearance(outbox []codec.L1Withdrawal) []string {
	var out []string
	for i := len(outbox) - 1; i >= 0; i-- {
		if a := hex.EncodeToString(outbox[i].Asset[:]); !slices.Contains(out, a) {
			out = append(out, a)
		}
	}
	return reversed(out)
}

// TestCapVectorsRejectWrongImplementations re-derives step 5a for every CV-CAP vector under each
// wrong implementation and requires every one to get at least one vector's status wrong. The
// control variant is the correct per-asset check and must get every status right.
func TestCapVectorsRejectWrongImplementations(t *testing.T) {
	data, err := os.ReadFile(vectorFilePath)
	if err != nil {
		t.Fatalf("ReadFile(%s): %v", vectorFilePath, err)
	}
	var file vectorFile
	if err := json.Unmarshal(data, &file); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}

	variants := capVariants()

	vectors := map[string]vectorJSON{}
	for _, vec := range file.Vectors {
		if strings.HasPrefix(vec.ID, "CV-CAP-") {
			vectors[vec.ID] = vec
		}
	}
	if len(vectors) != 12 {
		t.Fatalf("%d CV-CAP vectors in the artifact, want 12", len(vectors))
	}

	for name, v := range variants {
		var wrong []string
		for id, vec := range vectors {
			outbox := withdrawalsOf(t, vec)
			totals := withdrawalTotals(t, vec)
			var sorted, seen []string
			for _, w := range outbox {
				if a := hex.EncodeToString(w.Asset[:]); !slices.Contains(seen, a) {
					seen = append(seen, a)
				}
			}
			sorted = append(sorted, seen...)
			sort.Strings(sorted)
			capFn := func(a string) *big.Int { return capOf(t, vec, a) }
			want := vec.Status == "RUNTIME_FAULT"
			if got := v(outbox, sorted, seen, totals, capFn); got != want {
				wrong = append(wrong, id)
			}
		}
		if name == "control" {
			if len(wrong) != 0 {
				t.Errorf("the correct per-asset check gets %v wrong", wrong)
			}
			continue
		}
		if len(wrong) == 0 {
			t.Errorf("wrong implementation %q gets every CV-CAP vector's status right", name)
		}
	}
}

// hx decodes whitespace-separated hex bytes.
func hx(t *testing.T, s string) []byte {
	t.Helper()
	return mustHex(t, strings.Join(strings.Fields(s), ""))
}

// checkModuleHas fails unless vec's module_wasm_hex contains the hex of want.
func checkModuleHas(t *testing.T, vec vectorJSON, want []byte) {
	t.Helper()
	if !strings.Contains(vec.ModuleWasmHex, hex.EncodeToString(want)) {
		t.Fatalf("%s: module_wasm_hex does not contain %x", vec.ID, want)
	}
}

// checkTrapRow asserts a run-time trap: status RUNTIME_FAULT, abort_code 0, no returned blob,
// a module containing prefix (the trapping code), and gas_used equal to InitialMemoryCost(1)
// plus the table cost of each opcode in ops plus hostGas. ops lists every opcode of the
// segment the trap interrupts and of the segments before it: each segment is charged in full
// at its entry.
func checkTrapRow(t *testing.T, vec vectorJSON, prefix, ops []byte, hostGas uint64) {
	t.Helper()
	requireStatus(t, vec, "RUNTIME_FAULT")
	if vec.Receipt.AbortCode != 0 {
		t.Fatalf("%s: receipt.abort_code = %d, want 0", vec.ID, vec.Receipt.AbortCode)
	}
	if vec.ContractEffectsBlobHex != "" {
		t.Fatalf("%s: contract_effects_blob_hex is present, want absent", vec.ID)
	}
	checkModuleHas(t, vec, prefix)
	want := module.InitialMemoryCost(1) + hostGas
	for _, op := range ops {
		cost, ok := module.OpcodeCost(module.Opcode(op))
		if !ok {
			t.Fatalf("%s: no gas-table cost for opcode 0x%02X", vec.ID, op)
		}
		want += cost
	}
	if vec.GasUsed != want {
		t.Fatalf("%s: gas_used = %d, want %d", vec.ID, vec.GasUsed, want)
	}
}

// readCallPrefix is the storage_read call a fixture body starts with: i32.const keyPtr,
// keyLen, outPtr and outLen, then call 0.
func readCallPrefix(keyPtr, keyLen, outPtr, outLen uint32) []byte {
	return cat(constI32(int64(keyPtr)), constI32(int64(keyLen)), constI32(int64(outPtr)), constI32(int64(outLen)), []byte{0x10, 0x00})
}

// readPath is the executed opcodes of a hostResultGuardBody fixture that returns normally.
var readPath = []byte{0x41, 0x41, 0x41, 0x41, 0x10, 0x42, 0x52, 0x04, 0x42, 0x0B}

// readTrapOps is readPath up to the guard's `if`: the segments a storage_read trap interrupts.
var readTrapOps = []byte{0x41, 0x41, 0x41, 0x41, 0x10, 0x42, 0x52, 0x04}

// requireLeaf asserts sha3(contract_id || key) is in witness_keys and returns its pre_state
// value and whether it is present.
func requireLeaf(t *testing.T, vec vectorJSON, key string) ([]byte, bool) {
	t.Helper()
	leaf := hex.EncodeToString(crypto.Hash(mustHex(t, vec.Receipt.ContractID), []byte(key)))
	if !slices.Contains(vec.WitnessKeys, leaf) {
		t.Fatalf("%s: witness_keys is missing sha3(contract_id || %q) = %s", vec.ID, key, leaf)
	}
	v, ok := vec.PreState[leaf]
	return mustHex(t, v), ok
}

// requireValue8 asserts the "value8" leaf is witnessed and present with an 8-byte value, and
// returns the value.
func requireValue8(t *testing.T, vec vectorJSON) []byte {
	t.Helper()
	v, ok := requireLeaf(t, vec, "value8")
	if !ok || len(v) != 8 {
		t.Fatalf("%s: pre_state value8 present = %v, length %d, want present with 8 bytes", vec.ID, ok, len(v))
	}
	return v
}

func checkCVTRAPDIV1(t *testing.T, vec vectorJSON) {
	checkTrapRow(t, vec, cat(constI32(1), constI32(0), []byte{0x6E, 0x1A}), hx(t, "41 41 6E 1A 42 0B"), 0)
}

func checkCVTRAPOVF1(t *testing.T, vec vectorJSON) {
	checkTrapRow(t, vec, cat(constI32(-2147483648), constI32(-1), []byte{0x6D, 0x1A}), hx(t, "41 41 6D 1A 42 0B"), 0)
}

func checkCVTRAPMEM1(t *testing.T, vec vectorJSON) {
	checkTrapRow(t, vec, cat(constI32(65533), []byte{0x28, 0x02, 0x00, 0x1A}), hx(t, "41 28 1A 42 0B"), 0)
	// One page, no maximum: byte 65536 is outside memory.
	checkModuleHas(t, vec, hx(t, "05 03 01 00 01"))
}

func checkCVTRAPTABLE1(t *testing.T, vec vectorJSON) {
	checkTrapRow(t, vec, cat(constI32(2), []byte{0x11, 0x01, 0x00}), hx(t, "41 11 42 0B"), 0)
	checkModuleHas(t, vec, hx(t, "04 04 01 70 00 02")) // two table slots
}

func checkCVTRAPNULL1(t *testing.T, vec vectorJSON) {
	checkTrapRow(t, vec, cat(constI32(1), []byte{0x11, 0x01, 0x00}), hx(t, "41 11 42 0B"), 0)
	checkModuleHas(t, vec, hx(t, "04 04 01 70 00 02"))
	checkModuleHas(t, vec, hx(t, "09 07 01 00 41 00 0B 01 01")) // only slot 0 initialized
}

func checkCVTRAPSIG1(t *testing.T, vec vectorJSON) {
	checkTrapRow(t, vec, cat(constI32(0), constI32(0), constI32(0), []byte{0x11, 0x00, 0x00, 0x1A}), hx(t, "41 41 41 11 1A 42 0B"), 0)
	// Types (i32,i32)->i64 and ()->(); run is type 0 and f is type 1.
	checkModuleHas(t, vec, hx(t, "02 60 02 7F 7F 01 7E 60 00 00"))
	checkModuleHas(t, vec, hx(t, "03 03 02 00 01"))
	// Slot 0 holds function 1, the ()->() function, and the call expects type 0.
	checkModuleHas(t, vec, hx(t, "09 07 01 00 41 00 0B 01 01"))
}

func checkNegRow(t *testing.T, vec vectorJSON, abortCode int32, r int64) {
	t.Helper()
	checkAbortRow(t, vec, "REVERT", abortCode, module.InitialMemoryCost(1)+costOf(t, 0x42, 0x0B))
	checkRevertData(t, vec, 0)
	if vec.ContractEffectsBlobHex != "" {
		t.Fatalf("%s: contract_effects_blob_hex is present, want absent", vec.ID)
	}
	if vec.Receipt.ReturnHash != zeroHashHex {
		t.Fatalf("%s: return_hash = %s, want zero", vec.ID, vec.Receipt.ReturnHash)
	}
	checkModuleHas(t, vec, cat(constI64(r), []byte{0x0B}))
}

func checkCVNEG1(t *testing.T, vec vectorJSON) { checkNegRow(t, vec, 7, -7) }

func checkCVNEG2(t *testing.T, vec vectorJSON) { checkNegRow(t, vec, 0, math.MinInt64) }

func checkCVFRAME2(t *testing.T, vec vectorJSON) {
	requireStatus(t, vec, "SUCCESS")
	frame := mustHex(t, vec.InputFrameHex)
	if len(frame) != 258 {
		t.Fatalf("CV-FRAME-2: input frame has %d bytes, want 258", len(frame))
	}
	er, err := codec.DecodeExecutionResult(mustHex(t, vec.ExecutionResultBlobHex))
	if err != nil {
		t.Fatalf("CV-FRAME-2: DecodeExecutionResult: %v", err)
	}
	want := cat(frame[250:258], bytes.Repeat([]byte{0xAA}, 8), make([]byte, 8))
	if !bytes.Equal(er.ReturnData, want) {
		t.Fatalf("CV-FRAME-2: ReturnData %x, want %x", er.ReturnData, want)
	}
	checkModuleHas(t, vec, bytes.Repeat([]byte{0xAA}, 266))
	checkPathGas(t, vec, 1, 0, cat(bytes.Repeat([]byte{0x41, 0x41, 0x29, 0x37}, 3), []byte{0x42, 0x0B}))
}

func checkCVFRAME3(t *testing.T, vec vectorJSON) {
	requireStatus(t, vec, "RUNTIME_FAULT")
	if want := module.InitialMemoryCost(0); vec.GasUsed != want {
		t.Fatalf("CV-FRAME-3: gas_used = %d, want %d", vec.GasUsed, want)
	}
	if vec.ContractEffectsBlobHex != "" {
		t.Fatalf("CV-FRAME-3: contract_effects_blob_hex is present, want absent")
	}
	if vec.InputFrameHex == "" {
		t.Fatalf("CV-FRAME-3: input_frame_hex is empty")
	}
	checkModuleHas(t, vec, hx(t, "05 03 01 00 00")) // 0 pages
}

// growPath is the executed opcodes of one growExpectBody that does not trap.
var growPath = []byte{0x41, 0x40, 0x41, 0x47, 0x04}

func checkCVGROW2(t *testing.T, vec vectorJSON) {
	requireStatus(t, vec, "SUCCESS")
	checkModuleHas(t, vec, hx(t, "05 04 01 01 01 01"))
	checkModuleHas(t, vec, growExpectBody(1, -1))
	checkPathGas(t, vec, 1, 0, cat(growPath, []byte{0x42, 0x0B}))
}

func checkGrow64(t *testing.T, vec vectorJSON, memSection string) {
	t.Helper()
	requireStatus(t, vec, "SUCCESS")
	checkModuleHas(t, vec, hx(t, memSection))
	checkModuleHas(t, vec, cat(growExpectBody(63, 1), growExpectBody(1, -1)))
	checkPathGas(t, vec, 1, module.MemoryGrowPageCost(63), cat(growPath, growPath, []byte{0x42, 0x0B}))
}

func checkCVGROW3(t *testing.T, vec vectorJSON) { checkGrow64(t, vec, "05 03 01 00 01") }

func checkCVGROW4(t *testing.T, vec vectorJSON) { checkGrow64(t, vec, "05 04 01 01 01 41") }

func checkCVGROW5(t *testing.T, vec vectorJSON) {
	requireStatus(t, vec, "SUCCESS")
	checkModuleHas(t, vec, hx(t, "05 04 01 01 01 02"))
	checkModuleHas(t, vec, cat(growExpectBody(1, 1), []byte{0x3F, 0x00}, constI32(2), []byte{0x47, 0x04, 0x40, 0x00, 0x0B}))
	checkPathGas(t, vec, 1, module.MemoryGrowPageCost(1), hx(t, "41 40 41 47 04 3F 41 47 04 42 0B"))
}

func checkCVREAD3(t *testing.T, vec vectorJSON) {
	requireStatus(t, vec, "SUCCESS")
	if _, present := requireLeaf(t, vec, "missing"); present {
		t.Fatalf("CV-READ-3: pre_state has an entry for the key, want absent")
	}
	checkModuleHas(t, vec, readCallPrefix(1024, 7, 65532, 8))
	checkPathGas(t, vec, 1, module.HostCost(module.HostStorageRead, 0), readPath)
}

func checkCVREAD4(t *testing.T, vec vectorJSON) {
	requireStatus(t, vec, "SUCCESS")
	requireValue8(t, vec)
	checkModuleHas(t, vec, readCallPrefix(1024, 6, 65534, 4))
	checkPathGas(t, vec, 1, module.HostCost(module.HostStorageRead, 8), readPath)
}

func checkCVREAD5(t *testing.T, vec vectorJSON) {
	requireValue8(t, vec)
	checkTrapRow(t, vec, readCallPrefix(1024, 6, 65528, 16), readTrapOps, module.HostCost(module.HostStorageRead, 8))
}

func checkCVREAD6(t *testing.T, vec vectorJSON) {
	requireStatus(t, vec, "SUCCESS")
	v := requireValue8(t, vec)
	er, err := codec.DecodeExecutionResult(mustHex(t, vec.ExecutionResultBlobHex))
	if err != nil {
		t.Fatalf("CV-READ-6: DecodeExecutionResult: %v", err)
	}
	if want := cat(v, bytes.Repeat([]byte{0xEE}, 8)); !bytes.Equal(er.ReturnData, want) {
		t.Fatalf("CV-READ-6: ReturnData %x, want %x", er.ReturnData, want)
	}
	checkModuleHas(t, vec, bytes.Repeat([]byte{0xEE}, 16))
	// The contract returns its effects blob from 1032, so the read's out_ptr is 1032 plus the
	// offset of the return data in that blob. The call reads key (1024, 6) into 16 bytes there.
	blob := mustHex(t, vec.ContractEffectsBlobHex)
	if n := bytes.Count(blob, er.ReturnData); n != 1 {
		t.Fatalf("CV-READ-6: the return data occurs %d times in the effects blob, want 1", n)
	}
	at := uint32(bytes.Index(blob, er.ReturnData))
	checkModuleHas(t, vec, readCallPrefix(1024, 6, 1032+at, 16))
	checkPathGas(t, vec, 1, module.HostCost(module.HostStorageRead, 8), readPath)
}

func checkCVREAD7(t *testing.T, vec vectorJSON) {
	checkTrapRow(t, vec, readCallPrefix(65532, 8, 2048, 8), readTrapOps, module.HostCost(module.HostStorageRead, 0))
}

func checkCVEVENTS1(t *testing.T, vec vectorJSON) {
	requireStatus(t, vec, "SUCCESS")
	er, err := codec.DecodeExecutionResult(mustHex(t, vec.ExecutionResultBlobHex))
	if err != nil {
		t.Fatalf("CV-EVENTS-1: DecodeExecutionResult: %v", err)
	}
	if len(er.Events) != 3 {
		t.Fatalf("CV-EVENTS-1: %d events, want 3", len(er.Events))
	}
	enc := make([][]byte, 3)
	leaf := make([]types.Hash, 3)
	for i, e := range er.Events {
		if e.EventIndex != uint32(i) {
			t.Fatalf("CV-EVENTS-1: event %d has event_index %d", i, e.EventIndex)
		}
		b, err := e.Encode()
		if err != nil {
			t.Fatalf("CV-EVENTS-1: Encode event %d: %v", i, err)
		}
		for j := 0; j < i; j++ {
			if bytes.Equal(enc[j], b) {
				t.Fatalf("CV-EVENTS-1: events %d and %d have the same encoding", j, i)
			}
		}
		enc[i] = b
		leaf[i] = hashOf(crypto.Hash(append([]byte{0x00}, b...)))
	}
	node := func(l, r types.Hash) types.Hash { return hashOf(crypto.Hash([]byte{0x01}, l[:], r[:])) }
	duplicated := node(node(leaf[0], leaf[1]), node(leaf[2], leaf[2]))
	reversed := inlineMerkleRoot([][]byte{enc[2], enc[1], enc[0]})
	got := vec.Receipt.EventRoot
	if got == hex.EncodeToString(duplicated[:]) {
		t.Fatalf("CV-EVENTS-1: event_root equals the root that duplicates the odd leaf")
	}
	if got == hex.EncodeToString(reversed[:]) {
		t.Fatalf("CV-EVENTS-1: event_root equals the root of the reversed order")
	}
	checkPathGas(t, vec, 1, 0, []byte{0x42, 0x0B})
}

func checkEXECDEP12(t *testing.T, vec vectorJSON) {
	requireStatus(t, vec, "SUCCESS")
	if vec.InputKind != 1 {
		t.Fatalf("EXEC-DEP-12: input_kind %d, want 1", vec.InputKind)
	}
	if vec.Input == nil || new(big.Int).SetBytes(mustHex(t, vec.Input.DepositAmount)).Cmp(big.NewInt(1000)) != 0 {
		t.Fatalf("EXEC-DEP-12: input.deposit_amount is not 1000")
	}
	eff, err := codec.DecodeContractEffects(mustHex(t, vec.ContractEffectsBlobHex))
	if err != nil {
		t.Fatalf("EXEC-DEP-12: DecodeContractEffects: %v", err)
	}
	if len(eff.StateDiff) != 0 || len(eff.Events) != 0 || len(eff.Outbox) != 0 || len(eff.ReturnData) != 0 || eff.ClaimedDeposit != (codec.U256{}) {
		t.Fatalf("EXEC-DEP-12: the returned blob is not empty with a zero claim")
	}
	if vec.Receipt.OutboxCount != 1 || vec.Receipt.EventCount != 0 {
		t.Fatalf("EXEC-DEP-12: outbox_count %d, event_count %d, want 1 and 0", vec.Receipt.OutboxCount, vec.Receipt.EventCount)
	}
	checkPathGas(t, vec, 1, 0, []byte{0x42, 0x0B})
}

func checkCVMEMMIN1(t *testing.T, vec vectorJSON) {
	requireStatus(t, vec, "VALIDATION_FAILED")
	checkModuleHas(t, vec, hx(t, "05 03 01 00 41")) // 65 pages
}

// moduleSection is one section of a module: its id and its payload. A custom section's
// payload starts with its length-prefixed name.
type moduleSection struct {
	id      byte
	payload []byte
}

// moduleSections splits vec's module_wasm_hex into its sections, in order, after checking the
// 8-byte preamble 00 61 73 6D 01 00 00 00 and that every section's size fits the bytes left.
func moduleSections(t *testing.T, vec vectorJSON) []moduleSection {
	t.Helper()
	raw := mustHex(t, vec.ModuleWasmHex)
	if len(raw) < 8 || !bytes.Equal(raw[:8], []byte{0x00, 0x61, 0x73, 0x6D, 0x01, 0x00, 0x00, 0x00}) {
		t.Fatalf("%s: module preamble is not 00 61 73 6D 01 00 00 00", vec.ID)
	}
	var out []moduleSection
	pos := 8
	for pos < len(raw) {
		id := raw[pos]
		pos++
		size := int(readULEB(t, raw, &pos))
		if size > len(raw)-pos {
			t.Fatalf("%s: section %d declares %d bytes, %d remain", vec.ID, id, size, len(raw)-pos)
		}
		out = append(out, moduleSection{id: id, payload: raw[pos : pos+size]})
		pos += size
	}
	return out
}

// sectionOf returns the payload of vec's only section with the given non-zero id, failing
// when the module has none or more than one.
func sectionOf(t *testing.T, vec vectorJSON, id byte) []byte {
	t.Helper()
	var found []byte
	n := 0
	for _, s := range moduleSections(t, vec) {
		if s.id == id {
			found = s.payload
			n++
		}
	}
	if n != 1 {
		t.Fatalf("%s: module has %d sections with id %d, want 1", vec.ID, n, id)
	}
	return found
}

// readULEB decodes the unsigned LEB128 at b[*pos] and advances *pos past it.
func readULEB(t *testing.T, b []byte, pos *int) uint64 {
	t.Helper()
	var v uint64
	for shift := uint(0); shift < 64; shift += 7 {
		if *pos >= len(b) {
			t.Fatalf("LEB128 at offset %d runs past the end of %d bytes", *pos, len(b))
		}
		c := b[*pos]
		*pos++
		v |= uint64(c&0x7F) << shift
		if c&0x80 == 0 {
			return v
		}
	}
	t.Fatalf("LEB128 longer than 10 bytes")
	return 0
}

// vecCount returns the leading vector count of vec's section id.
func vecCount(t *testing.T, vec vectorJSON, id byte) uint64 {
	t.Helper()
	payload := sectionOf(t, vec, id)
	pos := 0
	return readULEB(t, payload, &pos)
}

// typeParams returns each declared function type's parameter count, from the type section.
// It fails on a form byte other than 0x60.
func typeParams(t *testing.T, vec vectorJSON) []uint64 {
	t.Helper()
	payload := sectionOf(t, vec, 1)
	pos := 0
	n := readULEB(t, payload, &pos)
	out := make([]uint64, 0, n)
	for i := uint64(0); i < n; i++ {
		if pos >= len(payload) || payload[pos] != 0x60 {
			t.Fatalf("%s: type %d: form byte is not 0x60", vec.ID, i)
		}
		pos++
		params := readULEB(t, payload, &pos)
		pos += int(params)
		results := readULEB(t, payload, &pos)
		pos += int(results)
		if pos > len(payload) {
			t.Fatalf("%s: type %d runs past the type section", vec.ID, i)
		}
		out = append(out, params)
	}
	return out
}

// funcTypes returns each locally defined function's type index, from the function section.
func funcTypes(t *testing.T, vec vectorJSON) []uint64 {
	t.Helper()
	payload := sectionOf(t, vec, 3)
	pos := 0
	n := readULEB(t, payload, &pos)
	out := make([]uint64, 0, n)
	for i := uint64(0); i < n; i++ {
		out = append(out, readULEB(t, payload, &pos))
	}
	return out
}

// funcLocals returns, for each locally defined function, its type's parameter count plus its
// declared locals (total) and its declared locals alone (declared), read from the type,
// function and code sections.
func funcLocals(t *testing.T, vec vectorJSON) (total, declared []uint64) {
	t.Helper()
	params := typeParams(t, vec)
	types := funcTypes(t, vec)
	code := sectionOf(t, vec, 10)
	pos := 0
	n := readULEB(t, code, &pos)
	if n != uint64(len(types)) {
		t.Fatalf("%s: %d function bodies for %d functions", vec.ID, n, len(types))
	}
	for i := uint64(0); i < n; i++ {
		size := int(readULEB(t, code, &pos))
		end := pos + size
		if end > len(code) {
			t.Fatalf("%s: function body %d runs past the code section", vec.ID, i)
		}
		groups := readULEB(t, code, &pos)
		var d uint64
		for g := uint64(0); g < groups; g++ {
			d += readULEB(t, code, &pos)
			pos++ // the value type
		}
		pos = end
		if types[i] >= uint64(len(params)) {
			t.Fatalf("%s: function %d names type %d of %d", vec.ID, i, types[i], len(params))
		}
		declared = append(declared, d)
		total = append(total, params[types[i]]+d)
	}
	return total, declared
}

// elemSegmentSizes returns each element segment's entry count. It fails unless every segment
// starts 00 41 00 0B (active, at i32.const 0).
func elemSegmentSizes(t *testing.T, vec vectorJSON) []uint64 {
	t.Helper()
	payload := sectionOf(t, vec, 9)
	pos := 0
	n := readULEB(t, payload, &pos)
	out := make([]uint64, 0, n)
	for i := uint64(0); i < n; i++ {
		if pos+4 > len(payload) || !bytes.Equal(payload[pos:pos+4], []byte{0x00, 0x41, 0x00, 0x0B}) {
			t.Fatalf("%s: element segment %d does not start 00 41 00 0B", vec.ID, i)
		}
		pos += 4
		entries := readULEB(t, payload, &pos)
		for e := uint64(0); e < entries; e++ {
			readULEB(t, payload, &pos)
		}
		out = append(out, entries)
	}
	return out
}

// wasmName is s with its one-byte length prefix; len(s) must be below 128.
func wasmName(s string) []byte {
	if len(s) >= 128 {
		panic("wasmName: name too long")
	}
	return append([]byte{byte(len(s))}, s...)
}

// checkTrailingCustom asserts that vec's last section is a custom section whose payload is
// want (shorter than 128 bytes), and that the module without that section validates.
func checkTrailingCustom(t *testing.T, vec vectorJSON, want []byte) {
	t.Helper()
	sections := moduleSections(t, vec)
	last := sections[len(sections)-1]
	if last.id != 0 || !bytes.Equal(last.payload, want) {
		t.Fatalf("%s: last section is id %d payload %x, want custom payload %x", vec.ID, last.id, last.payload, want)
	}
	raw := mustHex(t, vec.ModuleWasmHex)
	requireValid(t, vec, raw[:len(raw)-(2+len(want))])
}

// memRunAt is the export payload of runModuleSpec with run at function index i (below 128).
func memRunAt(t *testing.T, i byte) []byte {
	return cat(hx(t, "02"), wasmName("memory"), hx(t, "02 00"), wasmName("run"), []byte{0x00, i})
}

// memRunTail is the export payload of runModuleSpec with run at index 0 and one more export x.
func memRunTail(t *testing.T, x []byte) []byte {
	return cat(hx(t, "03"), wasmName("memory"), hx(t, "02 00"), wasmName("run"), hx(t, "00 00"), x)
}

// requireSection fails unless vec's only section with the given id has the payload want.
func requireSection(t *testing.T, vec vectorJSON, id byte, want []byte) {
	t.Helper()
	if got := sectionOf(t, vec, id); !bytes.Equal(got, want) {
		t.Fatalf("%s: section %d = %x, want %x", vec.ID, id, got, want)
	}
}

func sumU64(xs []uint64) uint64 {
	var s uint64
	for _, x := range xs {
		s += x
	}
	return s
}

func maxU64(xs []uint64) uint64 {
	var m uint64
	for _, x := range xs {
		m = max(m, x)
	}
	return m
}

func requireCount(t *testing.T, vec vectorJSON, id byte, want uint64) {
	t.Helper()
	if got := vecCount(t, vec, id); got != want {
		t.Fatalf("%s: section %d vector count = %d, want %d", vec.ID, id, got, want)
	}
}

func checkCVSTRUCTTYPES1(t *testing.T, vec vectorJSON) {
	requireStatus(t, vec, "VALIDATION_FAILED")
	requireCount(t, vec, 1, MaxTypeCount+1)
}

func checkCVSTRUCTTYPEPARAMS1(t *testing.T, vec vectorJSON) {
	requireStatus(t, vec, "VALIDATION_FAILED")
	params := typeParams(t, vec)
	at := -1
	for i, p := range params {
		if p == MaxLocalsPerFunction+1 {
			if at != -1 {
				t.Fatalf("%s: more than one type declares %d parameters", vec.ID, MaxLocalsPerFunction+1)
			}
			at = i
		}
	}
	if at == -1 {
		t.Fatalf("%s: no type declares %d parameters", vec.ID, MaxLocalsPerFunction+1)
	}
	if sumU64(params) > MaxTotalParams {
		t.Fatalf("%s: type parameters sum to %d, want at most %d", vec.ID, sumU64(params), MaxTotalParams)
	}
	for _, ft := range funcTypes(t, vec) {
		if ft == uint64(at) {
			t.Fatalf("%s: a function uses type %d, want it unused", vec.ID, at)
		}
	}
}

func checkCVSTRUCTPARAMS1(t *testing.T, vec vectorJSON) {
	requireStatus(t, vec, "VALIDATION_FAILED")
	params := typeParams(t, vec)
	if sumU64(params) != MaxTotalParams+1 {
		t.Fatalf("%s: type parameters sum to %d, want %d", vec.ID, sumU64(params), MaxTotalParams+1)
	}
	if maxU64(params) > MaxLocalsPerFunction {
		t.Fatalf("%s: a type declares %d parameters, want at most %d", vec.ID, maxU64(params), MaxLocalsPerFunction)
	}
}

func checkCVSTRUCTFUNCS1(t *testing.T, vec vectorJSON) {
	requireStatus(t, vec, "VALIDATION_FAILED")
	requireCount(t, vec, 3, MaxFunctions+1)
	requireCount(t, vec, 10, MaxFunctions+1)
}

func checkCVSTRUCTTABLE1(t *testing.T, vec vectorJSON) {
	requireStatus(t, vec, "VALIDATION_FAILED")
	requireSection(t, vec, 4, hx(t, "01 70 00 91 4E")) // minimum 10,001
}

func checkCVSTRUCTGLOBALS1(t *testing.T, vec vectorJSON) {
	requireStatus(t, vec, "VALIDATION_FAILED")
	requireCount(t, vec, 6, MaxGlobals+1)
}

func checkCVSTRUCTELEMS1(t *testing.T, vec vectorJSON) {
	requireStatus(t, vec, "VALIDATION_FAILED")
	requireSection(t, vec, 4, hx(t, "01 70 00 90 4E")) // minimum 10,000
	sizes := elemSegmentSizes(t, vec)
	if sumU64(sizes) != MaxTotalElementEntries+1 {
		t.Fatalf("%s: element segments hold %d entries, want %d", vec.ID, sumU64(sizes), MaxTotalElementEntries+1)
	}
	if maxU64(sizes) > MaxTableEntries {
		t.Fatalf("%s: an element segment holds %d entries, want at most %d so each fits the table", vec.ID, maxU64(sizes), MaxTableEntries)
	}
}

func checkCVSTRUCTDATA1(t *testing.T, vec vectorJSON) {
	requireStatus(t, vec, "VALIDATION_FAILED")
	requireCount(t, vec, 11, MaxDataSegments+1)
}

func checkCVSTRUCTLOCALS1(t *testing.T, vec vectorJSON) {
	requireStatus(t, vec, "VALIDATION_FAILED")
	total, declared := funcLocals(t, vec)
	if len(total) != 1 {
		t.Fatalf("%s: %d functions, want 1", vec.ID, len(total))
	}
	if total[0] != MaxLocalsPerFunction+1 {
		t.Fatalf("%s: run has %d parameters plus locals, want %d", vec.ID, total[0], MaxLocalsPerFunction+1)
	}
	if declared[0] > MaxLocalsPerFunction {
		t.Fatalf("%s: run declares %d locals, want at most %d", vec.ID, declared[0], MaxLocalsPerFunction)
	}
}

func checkCVSTRUCTTOTALLOCALS1(t *testing.T, vec vectorJSON) {
	requireStatus(t, vec, "VALIDATION_FAILED")
	total, declared := funcLocals(t, vec)
	if sumU64(total) != MaxTotalLocals+1 {
		t.Fatalf("%s: parameters plus locals sum to %d, want %d", vec.ID, sumU64(total), MaxTotalLocals+1)
	}
	if sumU64(declared) > MaxTotalLocals {
		t.Fatalf("%s: declared locals sum to %d, want at most %d", vec.ID, sumU64(declared), MaxTotalLocals)
	}
	if maxU64(total) > MaxLocalsPerFunction {
		t.Fatalf("%s: a function has %d parameters plus locals, want at most %d", vec.ID, maxU64(total), MaxLocalsPerFunction)
	}
}

func checkCVSTRUCTATCAP1(t *testing.T, vec vectorJSON) {
	requireStatus(t, vec, "SUCCESS")
	if vec.InputKind != 4 {
		t.Fatalf("%s: input_kind = %d, want 4", vec.ID, vec.InputKind)
	}
	requireCount(t, vec, 1, MaxTypeCount)
	requireCount(t, vec, 3, MaxFunctions)
	requireCount(t, vec, 6, MaxGlobals)
	requireCount(t, vec, 11, MaxDataSegments)
	requireSection(t, vec, 4, hx(t, "01 70 00 90 4E")) // minimum 10,000
	sizes := elemSegmentSizes(t, vec)
	if sumU64(sizes) != MaxTotalElementEntries || sizes[0] != MaxTableEntries {
		t.Fatalf("%s: element segments %v, want first %d and total %d", vec.ID, sizes, MaxTableEntries, MaxTotalElementEntries)
	}
	requireSection(t, vec, 5, hx(t, "01 00 01")) // one page, no maximum
	if !bytes.Contains(sectionOf(t, vec, 11), hx(t, "00 41 FF FF 03 0B 01 AA")) {
		t.Fatalf("%s: no data segment of one byte at offset 65,535", vec.ID)
	}
	total, _ := funcLocals(t, vec)
	if maxU64(total) != MaxLocalsPerFunction {
		t.Fatalf("%s: largest parameters plus locals = %d, want %d", vec.ID, maxU64(total), MaxLocalsPerFunction)
	}
	params := typeParams(t, vec)
	if sumU64(params) != MaxTotalParams || maxU64(params) > MaxLocalsPerFunction {
		t.Fatalf("%s: type parameters sum to %d with largest %d, want %d and at most %d", vec.ID, sumU64(params), maxU64(params), MaxTotalParams, MaxLocalsPerFunction)
	}
}

func checkCVFITDATA1(t *testing.T, vec vectorJSON) {
	requireStatus(t, vec, "VALIDATION_FAILED")
	requireSection(t, vec, 5, hx(t, "01 00 01"))
	requireSection(t, vec, 11, hx(t, "01 00 41 FF FF 03 0B 02 AA BB"))
}

func checkCVFITELEM1(t *testing.T, vec vectorJSON) {
	requireStatus(t, vec, "VALIDATION_FAILED")
	requireSection(t, vec, 4, hx(t, "01 70 00 02"))
	requireSection(t, vec, 9, hx(t, "01 00 41 01 0B 02 00 00"))
}

func checkCVIMPORTMODULE1(t *testing.T, vec vectorJSON) {
	requireStatus(t, vec, "VALIDATION_FAILED")
	requireSection(t, vec, 1, hx(t, "01 60 02 7F 7F 01 7E"))
	requireSection(t, vec, 2, cat(hx(t, "01"), wasmName("zenon"), wasmName("storage_len"), hx(t, "00 00")))
	requireSection(t, vec, 7, memRunAt(t, 1))
}

func checkCVIMPORTNAME1(t *testing.T, vec vectorJSON) {
	requireStatus(t, vec, "VALIDATION_FAILED")
	requireSection(t, vec, 2, cat(hx(t, "01"), wasmName("env"), wasmName("storage_write"), hx(t, "00 00")))
	requireSection(t, vec, 7, memRunAt(t, 1))
}

func checkCVIMPORTDUP1(t *testing.T, vec vectorJSON) {
	requireStatus(t, vec, "VALIDATION_FAILED")
	requireSection(t, vec, 2, cat(hx(t, "02"), wasmName("env"), wasmName("storage_len"), hx(t, "00 00"), wasmName("env"), wasmName("storage_len"), hx(t, "00 00")))
	requireSection(t, vec, 7, memRunAt(t, 2))
}

func checkCVIMPORTSIG1(t *testing.T, vec vectorJSON) {
	requireStatus(t, vec, "VALIDATION_FAILED")
	requireSection(t, vec, 1, hx(t, "02 60 02 7F 7F 01 7E 60 01 7F 00"))
	requireSection(t, vec, 2, cat(hx(t, "01"), wasmName("env"), wasmName("abort"), hx(t, "00 01")))
	requireSection(t, vec, 7, memRunAt(t, 1))
}

func checkCVIMPORTKIND1(t *testing.T, vec vectorJSON) {
	requireStatus(t, vec, "VALIDATION_FAILED")
	requireSection(t, vec, 2, cat(hx(t, "01"), wasmName("env"), wasmName("storage_len"), hx(t, "03 7F 00")))
	requireSection(t, vec, 7, memRunAt(t, 0)) // a global import adds no function index
}

func checkCVSECTIONSTART1(t *testing.T, vec vectorJSON) {
	requireStatus(t, vec, "VALIDATION_FAILED")
	requireSection(t, vec, 8, hx(t, "01"))
	requireSection(t, vec, 3, hx(t, "02 00 01"))
	requireSection(t, vec, 1, hx(t, "02 60 02 7F 7F 01 7E 60 00 00"))
}

func checkCVSECTIONCUSTOM1(t *testing.T, vec vectorJSON) {
	requireStatus(t, vec, "VALIDATION_FAILED")
	checkTrailingCustom(t, vec, cat(wasmName("name"), hx(t, "00 02 01 6D")))
}

func checkCVSECTIONCUSTOM2(t *testing.T, vec vectorJSON) {
	requireStatus(t, vec, "VALIDATION_FAILED")
	checkTrailingCustom(t, vec, wasmName("zeno"))
}

func checkCVEXPORTMUTGLOBAL1(t *testing.T, vec vectorJSON) {
	requireStatus(t, vec, "VALIDATION_FAILED")
	requireSection(t, vec, 6, hx(t, "01 7F 01 41 00 0B"))
	requireSection(t, vec, 7, memRunTail(t, cat(wasmName("g"), hx(t, "03 00"))))
}

func checkCVEXPORTSTART1(t *testing.T, vec vectorJSON) {
	requireStatus(t, vec, "VALIDATION_FAILED")
	requireSection(t, vec, 7, memRunTail(t, cat(wasmName("_start"), hx(t, "00 00"))))
}

func checkCVEXPORTZENON1(t *testing.T, vec vectorJSON) {
	requireStatus(t, vec, "VALIDATION_FAILED")
	requireSection(t, vec, 7, memRunTail(t, cat(wasmName("__zenon_gas"), hx(t, "00 00"))))
}

func checkCVEXPORTNORUN1(t *testing.T, vec vectorJSON) {
	requireStatus(t, vec, "VALIDATION_FAILED")
	requireSection(t, vec, 7, cat(hx(t, "01"), wasmName("memory"), hx(t, "02 00")))
}

func checkCVEXPORTNOMEMORY1(t *testing.T, vec vectorJSON) {
	requireStatus(t, vec, "VALIDATION_FAILED")
	requireSection(t, vec, 5, hx(t, "01 00 01"))
	requireSection(t, vec, 7, cat(hx(t, "01"), wasmName("run"), hx(t, "00 00")))
}

func checkCVEXPORTRUNSIG1(t *testing.T, vec vectorJSON) {
	requireStatus(t, vec, "VALIDATION_FAILED")
	requireSection(t, vec, 1, hx(t, "01 60 02 7F 7F 01 7F"))
	requireSection(t, vec, 7, memRunAt(t, 0))
}

func checkCVEXPORTRUNIMPORT1(t *testing.T, vec vectorJSON) {
	requireStatus(t, vec, "VALIDATION_FAILED")
	requireSection(t, vec, 1, hx(t, "01 60 02 7F 7F 01 7E"))
	requireSection(t, vec, 2, cat(hx(t, "01"), wasmName("env"), wasmName("storage_len"), hx(t, "00 00")))
	requireSection(t, vec, 7, memRunAt(t, 0)) // run is function 0, the import
}

func checkCVINSTRSIZE1(t *testing.T, vec vectorJSON) {
	requireStatus(t, vec, "VALIDATION_FAILED")
	if n := len(mustHex(t, vec.ModuleWasmHex)); n > MaxCodeSize {
		t.Fatalf("%s: module is %d bytes, want at most %d", vec.ID, n, MaxCodeSize)
	}
	requireCount(t, vec, 10, 1)
	want := cat(hx(t, "00 42 00"), bytes.Repeat(hx(t, "0F"), instrSizeReturns), hx(t, "0B"))
	if !bytes.HasSuffix(sectionOf(t, vec, 10), want) {
		t.Fatalf("%s: run's body is not i64.const 0 then %d returns", vec.ID, instrSizeReturns)
	}
}

func checkCVLEBPADDED1(t *testing.T, vec vectorJSON) {
	requireStatus(t, vec, "VALIDATION_FAILED")
	requireSection(t, vec, 10, hx(t, "01 05 80 00 42 00 0B"))
	raw := mustHex(t, vec.ModuleWasmHex)
	padded := hx(t, "0A 07 01 05 80 00 42 00 0B")
	if !bytes.HasSuffix(raw, padded) {
		t.Fatalf("%s: module does not end with the padded code section %x", vec.ID, padded)
	}
	requireValid(t, vec, append(append([]byte{}, raw[:len(raw)-len(padded)]...), hx(t, "0A 06 01 04 00 42 00 0B")...))
}
