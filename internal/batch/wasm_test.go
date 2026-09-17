package batch

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/zenon-network/go-zenon/common/trie"
	"github.com/zenon-network/go-zenon/common/types"

	"github.com/digitalSloth/go-executor/domain"
	"github.com/digitalSloth/go-executor/internal/codec"
	"github.com/digitalSloth/go-executor/internal/ledger"
	"github.com/digitalSloth/go-executor/plugins/wasm"
)

// exec008Fixture is the minimal subset of the execution-conformance-v1.json schema this test
// needs from vector EXEC-008: the module bytes and the two pre_state leaves. The fixture file
// is normative and append-only — read here, never written.
type exec008Fixture struct {
	Vectors []struct {
		ID            string            `json:"id"`
		ModuleWasmHex string            `json:"module_wasm_hex"`
		PreState      map[string]string `json:"pre_state"`
	} `json:"vectors"`
}

func loadExec008(t *testing.T) (moduleBytes []byte, preState map[trie.Path][]byte) {
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
		if v.ID != "EXEC-008" {
			continue
		}
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
	t.Fatal("EXEC-008 not found in plugins/wasm/testdata/execution-conformance-v1.json")
	return nil, nil
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

	deployStart := encode(codec.DeployStartArgs{
		CodeHash:     preCodeHash,
		MetadataHash: hashN(2),
		TotalSize:    uint64(len(moduleBytes)),
		ChunkCount:   1,
	})
	deployChunk := encode(codec.DeployChunkArgs{CodeHash: preCodeHash, ChunkIndex: 0, ChunkHash: chunkHash})
	deployFinalize := encode(codec.DeployFinalizeArgs{CodeHash: preCodeHash})
	call := encode(codec.CallL2Args{TargetContract: target, PlasmaLimit: 10_000_000, DepositAsset: codec.AssetID{}, DepositAmount: codec.U256{}})

	inputs := []ledger.Input{
		{Ctx: domain.InputContext{GlobalInputIndex: 0, L1Sender: deployer}, Payload: deployStart},
		{Ctx: domain.InputContext{GlobalInputIndex: 1, L1Sender: deployer, SourceData: moduleBytes}, Payload: deployChunk},
		{Ctx: domain.InputContext{GlobalInputIndex: 2, L1Sender: deployer}, Payload: deployFinalize},
		// The module hard-codes global_input_index 105; the deploy inputs' indices are
		// otherwise unconstrained (the driver does no index arithmetic).
		{Ctx: domain.InputContext{GlobalInputIndex: 105, L1Sender: deployer}, Payload: call},
	}

	cfg := Config{
		DomainID:     1,
		ExecutorID:   types.Address{9},
		Domain:       plugin,
		Store:        store,
		Inputs:       &fakeInputSource{inputs: inputs},
		Bundles:      newFakeBundleStore(),
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
