package wasm

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"os"
	"reflect"
	"sort"
	"testing"

	"github.com/zenon-network/go-zenon/common/crypto"
	"github.com/zenon-network/go-zenon/common/trie"
	"github.com/zenon-network/go-zenon/common/types"

	"github.com/digitalSloth/go-executor/internal/codec"
	"github.com/digitalSloth/go-executor/plugins/wasm/internal/module"
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

// replayStateDiff applies diff onto the leaf set given by paths/values per §13.5.1 (nil
// deletes, any other value sets), returning the resulting parallel slices.
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

// inlineMerkleRoot recomputes the §27.3 construction inline: leaf sha3(0x00 || element), node
// sha3(0x01 || left || right), a lone trailing node promoted unchanged, bytes32(0) for the
// empty set. It must not call codec.MerkleRoot.
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

// statusOrdinal maps a §15 status name back to its ordinal byte, by scanning codec.Status's own
// String() rendering.
func statusOrdinal(t *testing.T, name string) byte {
	t.Helper()
	for s := codec.StatusSuccess; s <= codec.StatusUnsupportedOperation; s++ {
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

	// A1: chain_id, computed inline from §9.1's formula, with no call to New, p.chainID or
	// hashOf-via-plugin.
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

	// B1: canonical input.
	canonical := mustHex(t, vec.CanonicalInputHex)
	gotInputHash := hex.EncodeToString(crypto.Hash(canonical))
	if gotInputHash != vec.Receipt.InputHash {
		t.Fatalf("B1: sha3(canonical_input_hex) = %s, want receipt.input_hash %s", gotInputHash, vec.Receipt.InputHash)
	}
	if vec.InputKind <= 6 {
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

	// B5: non-SUCCESS vectors (§14.3).
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

	// state_diff_hash (§15): the region length is computed arithmetically from the decoded
	// entries and sliced out of the encoded ExecutionResult at the fixed prefix offset 74
	// (version:u16 + global_input_index:u64 + pre_state_root:32 + post_state_root:32).
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

	// return_hash (§15/§14.3).
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

	// witness keys (§20, generic half).
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
		t.Fatalf("B5 (§14.3): status %s carries state_diff_hash %s, want zero; %s", vec.Status, vec.Receipt.StateDiffHash, detail)
	}
	if vec.Receipt.ReturnHash != zeroHashHex {
		t.Fatalf("B5 (§14.3): status %s carries return_hash %s, want zero; %s", vec.Status, vec.Receipt.ReturnHash, detail)
	}

	if vec.ExecutionResultBlobHex != "" {
		blob := mustHex(t, vec.ExecutionResultBlobHex)
		er, err := codec.DecodeExecutionResult(blob)
		if err != nil {
			t.Fatalf("B5: DecodeExecutionResult: %v", err)
		}
		if len(er.StateDiff) != 0 {
			t.Fatalf("B5: ExecutionResult.StateDiff has %d entries, want 0 (§14.3)", len(er.StateDiff))
		}
		if len(er.Events) != 0 {
			t.Fatalf("B5: ExecutionResult.Events has %d entries, want 0 (§14.3)", len(er.Events))
		}
		if len(er.ReturnData) != 0 {
			t.Fatalf("B5: ExecutionResult.ReturnData has %d bytes, want 0 (§14.3)", len(er.ReturnData))
		}
		if er.ClaimedDeposit != (codec.U256{}) {
			t.Fatalf("B5: ExecutionResult.ClaimedDeposit = %x, want zero (§14.3)", er.ClaimedDeposit)
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
	case "unsupported_kind":
		checkUnsupportedKind(t, vec)
	case "deploy":
		checkDeploy(t, file, vec)
	case "ownership":
		checkOwnership(t, vec)
	case "validation_rejection":
		checkValidationRejection(t, vec)
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
			t.Fatalf("C2: Payload = %x, want empty for a Deposit (kind 1, §27.5)", frame.Payload)
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

	contractID := mustHex(t, vec.Receipt.ContractID)
	wantSet := make(map[string]bool, len(eff.StateDiff))
	for _, e := range eff.StateDiff {
		wantSet[hex.EncodeToString(crypto.Hash(contractID, e.LocalKey))] = true
	}
	gotSet := make(map[string]bool, len(er.StateDiff))
	for _, e := range er.StateDiff {
		gotSet[hex.EncodeToString(e.Key[:])] = true
	}
	if !reflect.DeepEqual(wantSet, gotSet) {
		t.Fatalf("C4: inline-derived local StateDiff key set disagrees with ExecutionResult.StateDiff key set:\n got  %v\n want %v", gotSet, wantSet)
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

// checkUnsupportedKind is the unsupported_kind shape's whole check, from the JSON alone (§15,
// §27.5).
func checkUnsupportedKind(t *testing.T, vec vectorJSON) {
	t.Helper()

	if vec.InputKind <= 6 {
		t.Fatalf("unsupported_kind: input_kind = %d, want > 6", vec.InputKind)
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

// checkDeploy is the deploy shape's whole check: the schema's shape rules, then the §15
// receipt identity and the §11.3/§12 ReturnData shape for the vector's own kind and status.
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
			t.Fatalf("deploy: kind 2: sha3(metadata_hex) = %s, want metadata_hash %s (§11.1)", metadataHash, vec.Input.MetadataHash)
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
		if vec.Input.NewCodeHash != vec.CodeHash {
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
				t.Fatalf("deploy: kind 5 VALIDATION_FAILED with a non-zero receipt.code_hash is deploy.go:245's authorized-but-refused outcome: add its branch with its first vector")
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
			if err := module.Validate(moduleRaw, moduleLimits()); err != nil {
				t.Fatalf("deploy: kind 5 UNAUTHORIZED: Validate: %v, want nil", err)
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

// checkOwnership is the ownership shape's whole check, from the JSON alone (§12, §15).
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

// checkValidationRejection is the validation_rejection shape's whole check: a kind-4 finalize
// rejected by a module the validator refuses, or by pending metadata §27.12 rejects, exactly
// one of the two, with the rule it refuses it by named in rejectionReasons.
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
	if vec.Input == nil || vec.Input.CodeHash != vec.CodeHash {
		t.Fatalf("validation_rejection: input.code_hash disagrees with code_hash %s", vec.CodeHash)
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
	_, metadataErr := codec.DecodeContractMetadata(mustHex(t, *vec.PendingMetadataHex))

	switch {
	case validateErr != nil && metadataErr == nil:
		if !errors.Is(validateErr, want) {
			t.Fatalf("validation_rejection: %s: Validate err = %v, want errors.Is(..., %v)", vec.ID, validateErr, want)
		}
	case validateErr == nil && metadataErr != nil:
		if !errors.Is(metadataErr, want) {
			t.Fatalf("validation_rejection: %s: DecodeContractMetadata err = %v, want errors.Is(..., %v)", vec.ID, metadataErr, want)
		}
	default:
		t.Fatalf("validation_rejection: %s: got Validate=%v DecodeContractMetadata=%v, want exactly one non-nil", vec.ID, validateErr, metadataErr)
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
	if len(vec.WitnessKeys) != 4 {
		t.Fatalf("N1: witness_keys has %d entries, want exactly 4 (a mapState-backed generator, whose Len records nothing for an absent key, would produce 3)", len(vec.WitnessKeys))
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

// checkN2DiscardNotVacuous is N2: proves §14.3's discard actually discarded something, by
// asserting the returned blob carried a non-empty StateDiff and non-empty ReturnData.
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
		t.Fatalf("N2: returned blob has an empty StateDiff; the §14.3 discard would be vacuous")
	}
	if len(eff.ReturnData) == 0 {
		t.Fatalf("N2: returned blob has empty ReturnData; the §14.3 discard would be vacuous")
	}
}

// checkPathGas recomputes gas_used from the gas table alone: module.InitialMemoryCost(pages),
// plus module.OpcodeCost for each original opcode on the executed path, plus hostGas, plus the
// §10.4 effect charge over the returned blob, computed inline from module.EffectCost. It must not
// call plugins/wasm's effectCharge, which is the generator's own path.
func checkPathGas(t *testing.T, vec vectorJSON, pages uint32, hostGas uint64, path []byte) {
	t.Helper()
	if vec.Status != "SUCCESS" {
		t.Fatalf("checkPathGas: status = %s, want SUCCESS", vec.Status)
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

// checkCVUNAUTH1ReservedKeyDiscardNotVacuous is INV-9: the decoded blob has a reserved-key entry
// and a second, non-reserved entry, and a non-empty ReturnData, so the §14.3 discard is not
// vacuous.
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
// MaxReturnData.
func checkCVRTLRETURN1ReturnDataOversized(t *testing.T, vec vectorJSON) {
	t.Helper()
	eff, err := codec.DecodeContractEffects(mustHex(t, vec.ContractEffectsBlobHex))
	if err != nil {
		t.Fatalf("INV-10: DecodeContractEffects: %v", err)
	}
	if len(eff.ReturnData) <= MaxReturnData {
		t.Fatalf("INV-10: len(ReturnData) = %d, want > MaxReturnData %d", len(eff.ReturnData), MaxReturnData)
	}
}

// checkCVRTLRESULT1ResultTooLargeFromRefund is INV-11: the decoded blob's ReturnData is not
// longer than MaxReturnData (so the other bound is not what tripped), the contract-only
// ExecutionResult length computed inline is <= MaxExecutionResult, and input.deposit_amount is
// non-zero (so a refund exists to push it over).
func checkCVRTLRESULT1ResultTooLargeFromRefund(t *testing.T, vec vectorJSON) {
	t.Helper()
	eff, err := codec.DecodeContractEffects(mustHex(t, vec.ContractEffectsBlobHex))
	if err != nil {
		t.Fatalf("INV-11: DecodeContractEffects: %v", err)
	}
	if len(eff.ReturnData) > MaxReturnData {
		t.Fatalf("INV-11: len(ReturnData) = %d > MaxReturnData %d, want <=", len(eff.ReturnData), MaxReturnData)
	}
	// A contract-only ExecutionResult with N state entries, no events, no outbox, empty
	// ReturnData encodes to 122 + Σ(36+len(v)) bytes (§27.1a's fixed prefix/suffix plus one
	// 36-byte-overhead StateDiffEntry per entry).
	contractOnly := 122
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

// checkOutOfGasBase is the shared assertion every CV-OOG-* named invariant starts with (§10.6):
// status OUT_OF_GAS and gas_used equal to the vector's own plasma_limit.
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
	reserved := map[string]bool{"0001": true, "0002": true, "0003": true, "0004": true}
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
}

var namedInvariants = map[string]func(t *testing.T, vec vectorJSON){
	"EXEC-002":           checkN1WitnessBackendTripwire,
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
}

// TestConformanceVectors re-derives every committed value in the artifact from the JSON
// fields alone, by a second path. It must not call refDerive or any ref* helper, must not use
// codec.MerkleRoot, codec.Receipt.Encode, codec.Input.Hash, codec.InputRoot or
// plugins/wasm's derive/CodeHash/callerHash for a value it is re-deriving, and must not use
// mapState.
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
