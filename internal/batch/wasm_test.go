package batch

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/zenon-network/go-zenon/common/crypto"
	"github.com/zenon-network/go-zenon/common/trie"
	"github.com/zenon-network/go-zenon/common/types"

	"github.com/Zeno-L2/zeno-executor/domain"
	"github.com/Zeno-L2/zeno-executor/internal/codec"
	"github.com/Zeno-L2/zeno-executor/internal/ledger"
	"github.com/Zeno-L2/zeno-executor/plugins/wasm"
)

// exec008Fixture is the minimal subset of the execution-conformance-v1.json schema this test
// needs from vector EXEC-008: the module bytes and the two pre_state leaves. The fixture file
// is normative, read here, never written.
type exec008Fixture struct {
	Vectors []struct {
		ID            string            `json:"id"`
		ModuleWasmHex string            `json:"module_wasm_hex"`
		PreState      map[string]string `json:"pre_state"`
	} `json:"vectors"`
}

func loadExec008(t *testing.T) (moduleBytes []byte, preState map[trie.Path][]byte) {
	t.Helper()
	v := loadExecVector(t, "EXEC-008")
	mb, err := hex.DecodeString(v.ModuleWasmHex)
	if err != nil {
		t.Fatalf("decode module_wasm_hex: %v", err)
	}
	ps := make(map[trie.Path][]byte, len(v.PreState))
	for pathHex, valHex := range v.PreState {
		pb, err := hex.DecodeString(pathHex)
		if err != nil {
			t.Fatalf("decode pre_state path %q: %v", pathHex, err)
		}
		var p trie.Path
		copy(p[:], pb)
		val, err := hex.DecodeString(valHex)
		if err != nil {
			t.Fatalf("decode pre_state value %q: %v", valHex, err)
		}
		ps[p] = val
	}
	return mb, ps
}

// loadExecVector reads one vector's raw fields out of the checked-in execution conformance
// artifact, normative, read here, never written.
func loadExecVector(t *testing.T, id string) struct {
	ID            string            `json:"id"`
	ModuleWasmHex string            `json:"module_wasm_hex"`
	PreState      map[string]string `json:"pre_state"`
} {
	t.Helper()
	data, err := os.ReadFile(filepath.Join("..", "..", "plugins", "wasm", "testdata", "execution-conformance-v1.json"))
	if err != nil {
		t.Fatalf("read fixture: %v", err)
	}
	var f exec008Fixture
	if err := json.Unmarshal(data, &f); err != nil {
		t.Fatalf("parse fixture: %v", err)
	}
	for _, v := range f.Vectors {
		if v.ID == id {
			return v
		}
	}
	t.Fatalf("%s not found in plugins/wasm/testdata/execution-conformance-v1.json", id)
	panic("unreachable")
}

// loadModule reads one vector's module_wasm_hex.
func loadModule(t *testing.T, id string) []byte {
	t.Helper()
	v := loadExecVector(t, id)
	mb, err := hex.DecodeString(v.ModuleWasmHex)
	if err != nil {
		t.Fatalf("%s: decode module_wasm_hex: %v", id, err)
	}
	return mb
}

func mustHexHash(t *testing.T, s string) types.Hash {
	t.Helper()
	b, err := hex.DecodeString(s)
	if err != nil {
		t.Fatalf("decode hash %q: %v", s, err)
	}
	var h types.Hash
	if len(b) != len(h) {
		t.Fatalf("hash %q is %d bytes, want %d", s, len(b), len(h))
	}
	copy(h[:], b)
	return h
}

// --- test 8: end-to-end through the real wasm.Plugin ---

func TestDriverEndToEndThroughWasmPlugin(t *testing.T) {
	moduleBytes, preState := loadExec008(t)

	plugin, err := wasm.New(wasm.Config{NetworkName: "testnet", CodeStorePath: t.TempDir()})
	if err != nil {
		t.Fatalf("wasm.New: %v", err)
	}
	t.Cleanup(func() { plugin.Close() })

	store := openTestStore(t)

	// Seed the target's two pre_state leaves directly, before batch.New: Config.Store requires
	// the driver to own the store exclusively from construction, and the kind-3 deploy only
	// populates the code store (below), never this contract's own leaves.
	var seed []domain.StateDiffEntry
	for path, val := range preState {
		seed = append(seed, domain.StateDiffEntry{Path: path, Value: val})
	}
	if _, err := store.Apply(seed); err != nil {
		t.Fatalf("seed pre_state: %v", err)
	}

	preCodeHash := wasm.CodeHash(moduleBytes)
	chunkHash := wasm.CodeHash(moduleBytes) // single chunk: the chunk's bytes are the whole module
	deployer := types.Address{7}
	src := hashN(1)
	target := mustHexHash(t, "1111111111111111111111111111111111111111111111111111111111111111")

	encode := func(args codec.InputArgs) []byte {
		t.Helper()
		b, err := (codec.Input{SourceAccountBlockHash: src, Args: args}).Encode()
		if err != nil {
			t.Fatalf("encode input: %v", err)
		}
		return b
	}

	metadata := codec.ContractMetadata{Version: codec.ContractMetadataVersion, UpgradePolicy: codec.PolicyImmutable}.Encode()
	metadataHash := hashOf(crypto.Hash(metadata))
	deployStart := encode(codec.DeployStartArgs{
		CodeHash:     preCodeHash,
		MetadataHash: metadataHash,
		TotalSize:    uint64(len(moduleBytes)),
		ChunkCount:   1,
		Metadata:     metadata,
	})
	deployChunk := encode(codec.DeployChunkArgs{CodeHash: preCodeHash, ChunkIndex: 0, ChunkHash: chunkHash})
	deployFinalize := encode(codec.DeployFinalizeArgs{CodeHash: preCodeHash})
	call := encode(codec.CallL2Args{TargetContract: target, PlasmaLimit: 10_000_000, DepositAsset: codec.AssetID{}, DepositAmount: codec.U256{}})

	inputs := []ledger.Input{
		{Ctx: domain.InputContext{GlobalInputIndex: 0, L1Sender: deployer}, Payload: deployStart},
		{Ctx: domain.InputContext{GlobalInputIndex: 1, L1Sender: deployer, SourceData: moduleBytes}, Payload: deployChunk},
		{Ctx: domain.InputContext{GlobalInputIndex: 2, L1Sender: deployer}, Payload: deployFinalize},
		// The module hard-codes global_input_index 1005; the deploy inputs' indices are
		// otherwise unconstrained (the driver does no index arithmetic).
		{Ctx: domain.InputContext{GlobalInputIndex: 1005, L1Sender: deployer, Caps: allCaps(codec.U256{0x80})}, Payload: call},
	}

	cfg := Config{
		DomainID:     1,
		ExecutorID:   types.Address{9},
		Domain:       plugin,
		Store:        store,
		Inputs:       &fakeInputSource{inputs: inputs},
		Bundles:      newFakeBundleStore(),
		Receipts:     newFakeReceiptStore(),
		FirstBatchID: 1,
	}
	d, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	got, ok, err := d.NextBatch()
	if err != nil || !ok {
		t.Fatalf("NextBatch: ok=%v err=%v", ok, err)
	}
	if len(got.Receipts) != 4 {
		t.Fatalf("len(Receipts) = %d, want 4", len(got.Receipts))
	}

	decoded, err := codec.DecodeBundle(got.Bundle)
	if err != nil {
		t.Fatalf("DecodeBundle: %v", err)
	}
	if len(decoded) != 4 {
		t.Fatalf("len(decoded) = %d, want 4", len(decoded))
	}

	var kind3Found bool
	var callResult codec.ExecutionResult
	for _, rec := range decoded {
		if len(rec.InputData) == 0 {
			continue
		}
		switch rec.InputData[0] {
		case 3:
			kind3Found = true
			if !bytes.Equal(rec.DeployChunkData, moduleBytes) {
				t.Fatalf("DeployChunkData = %x, want the module bytes", rec.DeployChunkData)
			}
		case 0:
			callResult, err = codec.DecodeExecutionResult(rec.ExecutionResult)
			if err != nil {
				t.Fatalf("decode call ExecutionResult: %v", err)
			}
		}
	}
	if !kind3Found {
		t.Fatalf("no kind-3 bundle record found")
	}

	finalizeReceipt := got.Receipts[2]
	if finalizeReceipt.Status != codec.StatusSuccess {
		t.Fatalf("finalize receipt Status = %v, want SUCCESS", finalizeReceipt.Status)
	}

	callReceipt := got.Receipts[3]
	if callReceipt.Status != codec.StatusSuccess {
		t.Fatalf("call receipt Status = %v, want SUCCESS", callReceipt.Status)
	}
	if callReceipt.OutboxCount != 1 {
		t.Fatalf("call receipt OutboxCount = %d, want 1", callReceipt.OutboxCount)
	}

	if len(callResult.Outbox) != 1 {
		t.Fatalf("call ExecutionResult Outbox = %+v, want exactly one message", callResult.Outbox)
	}
	withdrawal, ok := callResult.Outbox[0].Target.(codec.L1Withdrawal)
	if !ok {
		t.Fatalf("call outbox message Target = %T, want codec.L1Withdrawal", callResult.Outbox[0].Target)
	}

	var summaryDebit codec.U256
	var found bool
	for _, e := range got.Commitment.AssetFlowSummary {
		if e.Asset == withdrawal.Asset {
			found = true
			summaryDebit = e.WithdrawalDebit
		}
	}
	if !found {
		t.Fatalf("no AssetFlowSummary entry for the withdrawn asset %x", withdrawal.Asset)
	}
	if summaryDebit != withdrawal.Amount {
		t.Fatalf("AssetFlowSummary WithdrawalDebit = %x, want the withdrawn amount %x", summaryDebit, withdrawal.Amount)
	}
}

// --- ownership lifecycle: deploy, upgrade, transfer, replayed from the DA bundle alone.

// TestDriverOwnershipLifecycleReplaysFromDA drives a full deploy/upgrade/transfer sequence
// through the real driver and wasm.Plugin, then replays the decoded bundle against a fresh
// plugin and code store, verifying every witness and re-deriving each record's
// ExecutionResult from nothing but the record's own input, DeployChunkData and witnesses.
func TestDriverOwnershipLifecycleReplaysFromDA(t *testing.T) {
	moduleA := loadModule(t, "EXEC-DEPLOY-1")
	moduleB := loadModule(t, "EXEC-UPGRADE-1")

	plugin, err := wasm.New(wasm.Config{NetworkName: "testnet", CodeStorePath: t.TempDir()})
	if err != nil {
		t.Fatalf("wasm.New: %v", err)
	}
	t.Cleanup(func() { plugin.Close() })

	store := openTestStore(t)

	deployer := types.Address{0xAA} // D
	newOwner := types.Address{0xBB} // N
	src := hashN(1)

	codeHashA := wasm.CodeHash(moduleA)
	codeHashB := wasm.CodeHash(moduleB)

	ownerMetadata := codec.ContractMetadata{Version: codec.ContractMetadataVersion, UpgradePolicy: codec.PolicyOwnerUpgradeable, Owner: deployer}.Encode()
	ownerMetadataHash := hashOf(crypto.Hash(ownerMetadata))

	const finalizeIdx = 2
	testnetChainID := hashOf(crypto.Hash([]byte("ZENON_WASM_L2:"), []byte("testnet")))
	var idxBuf [8]byte
	binary.BigEndian.PutUint64(idxBuf[:], finalizeIdx)
	contractC := hashOf(crypto.Hash(deployer[:], idxBuf[:], testnetChainID[:]))

	encode := func(args codec.InputArgs) []byte {
		t.Helper()
		b, err := (codec.Input{SourceAccountBlockHash: src, Args: args}).Encode()
		if err != nil {
			t.Fatalf("encode input: %v", err)
		}
		return b
	}

	inputs := []ledger.Input{
		{Ctx: domain.InputContext{GlobalInputIndex: 0, L1Sender: deployer}, Payload: encode(codec.DeployStartArgs{
			CodeHash: codeHashA, MetadataHash: ownerMetadataHash, TotalSize: uint64(len(moduleA)), ChunkCount: 1, Metadata: ownerMetadata,
		})},
		{Ctx: domain.InputContext{GlobalInputIndex: 1, L1Sender: deployer, SourceData: moduleA}, Payload: encode(codec.DeployChunkArgs{
			CodeHash: codeHashA, ChunkIndex: 0, ChunkHash: codeHashA,
		})},
		{Ctx: domain.InputContext{GlobalInputIndex: finalizeIdx, L1Sender: deployer}, Payload: encode(codec.DeployFinalizeArgs{CodeHash: codeHashA})},
		{Ctx: domain.InputContext{GlobalInputIndex: 3, L1Sender: deployer}, Payload: encode(codec.DeployStartArgs{
			CodeHash: codeHashB, MetadataHash: hashOf(crypto.Hash(nil)), TotalSize: uint64(len(moduleB)), ChunkCount: 1, Metadata: []byte{},
		})},
		{Ctx: domain.InputContext{GlobalInputIndex: 4, L1Sender: deployer, SourceData: moduleB}, Payload: encode(codec.DeployChunkArgs{
			CodeHash: codeHashB, ChunkIndex: 0, ChunkHash: codeHashB,
		})},
		{Ctx: domain.InputContext{GlobalInputIndex: 5, L1Sender: deployer}, Payload: encode(codec.UpgradeArgs{ContractID: contractC, NewCodeHash: codeHashB})},
		{Ctx: domain.InputContext{GlobalInputIndex: 6, L1Sender: deployer}, Payload: encode(codec.TransferOwnershipArgs{ContractID: contractC, NewOwner: newOwner})},
	}

	cfg := Config{
		DomainID:     1,
		ExecutorID:   types.Address{9},
		Domain:       plugin,
		Store:        store,
		Inputs:       &fakeInputSource{inputs: inputs},
		Bundles:      newFakeBundleStore(),
		Receipts:     newFakeReceiptStore(),
		FirstBatchID: 1,
	}
	d, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	got, ok, err := d.NextBatch()
	if err != nil || !ok {
		t.Fatalf("NextBatch: ok=%v err=%v", ok, err)
	}
	if len(got.Receipts) != len(inputs) {
		t.Fatalf("len(Receipts) = %d, want %d", len(got.Receipts), len(inputs))
	}
	for i, rec := range got.Receipts {
		if rec.Status != codec.StatusSuccess {
			t.Fatalf("receipt %d Status = %v, want SUCCESS", i, rec.Status)
		}
	}

	decoded, err := codec.DecodeBundle(got.Bundle)
	if err != nil {
		t.Fatalf("DecodeBundle: %v", err)
	}
	if len(decoded) != len(inputs) {
		t.Fatalf("len(decoded) = %d, want %d", len(decoded), len(inputs))
	}

	// Replay against a fresh plugin (so the code store accumulates only what this replay
	// itself decodes) and a fresh state store reseeded, per record, from that record's own
	// witness set alone.
	replayPlugin, err := wasm.New(wasm.Config{NetworkName: "testnet", CodeStorePath: t.TempDir()})
	if err != nil {
		t.Fatalf("wasm.New (replay): %v", err)
	}
	t.Cleanup(func() { replayPlugin.Close() })

	for i, rec := range decoded {
		er, err := codec.DecodeExecutionResult(rec.ExecutionResult)
		if err != nil {
			t.Fatalf("record %d: DecodeExecutionResult: %v", i, err)
		}

		witnessKeys := make(map[trie.Path]bool, len(rec.Witnesses))
		var seed []domain.StateDiffEntry
		for _, w := range rec.Witnesses {
			witnessKeys[w.DerivedKey] = true
			if w.Value != nil {
				ok, err := trie.VerifyProofByPath(er.PreStateRoot, w.DerivedKey, w.Value, w.Proof)
				if err != nil || !ok {
					t.Fatalf("record %d: VerifyProofByPath(%x): ok=%v err=%v", i, w.DerivedKey, ok, err)
				}
				seed = append(seed, domain.StateDiffEntry{Path: w.DerivedKey, Value: w.Value})
			} else {
				ok, err := trie.VerifyAbsenceByPath(er.PreStateRoot, w.DerivedKey, w.Proof)
				if err != nil || !ok {
					t.Fatalf("record %d: VerifyAbsenceByPath(%x): ok=%v err=%v", i, w.DerivedKey, ok, err)
				}
			}
		}

		replayStore := openTestStore(t)
		if _, err := replayStore.Apply(seed); err != nil {
			t.Fatalf("record %d: seed replay store: %v", i, err)
		}
		acc := replayStore.Accessor()

		ctx := inputs[i].Ctx
		ctx.SourceData = rec.DeployChunkData
		decodedInput, err := replayPlugin.DecodeInput(ctx, rec.InputData)
		if err != nil {
			t.Fatalf("record %d: DecodeInput: %v", i, err)
		}
		eff, err := replayPlugin.Apply(acc, decodedInput)
		if err != nil {
			t.Fatalf("record %d: Apply: %v", i, err)
		}
		if eff.Status != codec.StatusSuccess {
			t.Fatalf("record %d: replayed Status = %v, want SUCCESS", i, eff.Status)
		}

		for _, w := range acc.Witnesses() {
			if !witnessKeys[w.Path] {
				t.Fatalf("record %d: accessor observed %x, which is not a witness key", i, w.Path)
			}
		}

		replayedER := codec.ExecutionResult{
			Version:          codec.ExecutionResultVersion,
			GlobalInputIndex: rec.GlobalInputIndex,
			PreStateRoot:     er.PreStateRoot,
			PostStateRoot:    er.PostStateRoot,
			StateDiff:        toDerivedStateDiff(eff.StateDiff),
			Events:           eff.Events,
			Outbox:           eff.OutboxMessages,
			ReturnData:       eff.ReturnData,
			ClaimedDeposit:   eff.ClaimedDeposit,
		}
		replayedBytes, err := replayedER.Encode()
		if err != nil {
			t.Fatalf("record %d: Encode replayed ExecutionResult: %v", i, err)
		}
		if !bytes.Equal(replayedBytes, rec.ExecutionResult) {
			t.Fatalf("record %d: replayed ExecutionResult != bundle record's:\n got  %x\n want %x", i, replayedBytes, rec.ExecutionResult)
		}
	}
}
