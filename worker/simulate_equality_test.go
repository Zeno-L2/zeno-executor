package worker

import (
	"bufio"
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/zenon-network/go-zenon/common/crypto"
	"github.com/zenon-network/go-zenon/common/trie"
	"github.com/zenon-network/go-zenon/common/types"

	"github.com/digitalSloth/go-executor/domain"
	"github.com/digitalSloth/go-executor/internal/batch"
	"github.com/digitalSloth/go-executor/internal/codec"
	"github.com/digitalSloth/go-executor/internal/ledger"
	"github.com/digitalSloth/go-executor/internal/simipc"
	"github.com/digitalSloth/go-executor/internal/state"
	"github.com/digitalSloth/go-executor/plugins/wasm"
)

// equalityVector is the subset of an execution-conformance vector the
// equality test needs.
type equalityVector struct {
	ID               string            `json:"id"`
	GlobalInputIndex uint64            `json:"global_input_index"`
	MomentumHeight   uint64            `json:"momentum_height"`
	MomentumTime     uint64            `json:"momentum_time"`
	MomentumHash     string            `json:"momentum_hash"`
	L1Sender         string            `json:"l1_sender"`
	CanonicalInput   string            `json:"canonical_input_hex"`
	ModuleWasmHex    string            `json:"module_wasm_hex"`
	PreState         map[string]string `json:"pre_state"`
	WithdrawalCaps   map[string]string `json:"withdrawal_caps"`
	Status           string            `json:"status"`
	GasUsed          uint64            `json:"gas_used"`
	ExecutionResult  string            `json:"execution_result_blob_hex"`
}

func loadEqualityVector(t *testing.T, id string) equalityVector {
	t.Helper()
	data, err := os.ReadFile(filepath.Join("..", "plugins", "wasm", "testdata", "execution-conformance-v1.json"))
	if err != nil {
		t.Fatalf("read fixture: %v", err)
	}
	var f struct {
		Vectors []equalityVector `json:"vectors"`
	}
	if err := json.Unmarshal(data, &f); err != nil {
		t.Fatalf("parse fixture: %v", err)
	}
	for _, v := range f.Vectors {
		if v.ID == id {
			return v
		}
	}
	t.Fatalf("%s not found in the conformance vectors", id)
	panic("unreachable")
}

func mustHex(t *testing.T, s string) []byte {
	t.Helper()
	b, err := hex.DecodeString(s)
	if err != nil {
		t.Fatalf("decode hex %q: %v", s, err)
	}
	return b
}

// vectorCaps is the domain.CapLookup of a vector's withdrawal_caps: 0
// for an absent asset.
type vectorCaps map[codec.AssetID]codec.U256

func (c vectorCaps) MaxBatchWithdrawals(assets []codec.AssetID) ([]codec.U256, error) {
	out := make([]codec.U256, len(assets))
	for i, a := range assets {
		out[i] = c[a]
	}
	return out, nil
}

// inputQueue serves appended inputs in order and reports false once
// drained.
type inputQueue struct {
	inputs []ledger.Input
	pos    int
}

func (q *inputQueue) Next() (ledger.Input, bool, error) {
	if q.pos >= len(q.inputs) {
		return ledger.Input{}, false, nil
	}
	in := q.inputs[q.pos]
	q.pos++
	return in, true, nil
}

type memBundles map[types.Hash][]byte

func (m memBundles) Put(h types.Hash, b []byte) error { m[h] = append([]byte(nil), b...); return nil }

func (m memBundles) Get(h types.Hash) ([]byte, error) {
	b, ok := m[h]
	if !ok {
		return nil, fmt.Errorf("no bundle for %x", h)
	}
	return append([]byte(nil), b...), nil
}

type nilReceipts struct{}

func (nilReceipts) Put(types.Hash, types.Hash, []byte) error { return nil }

func TestSimulatorEqualsBatch(t *testing.T) {
	restoreMemoryLimit(t)
	for _, id := range []string{"EXEC-008", "EXEC-009", "CV-ABORT-1", "EXEC-DEP-2", "CV-CAP-5", "CV-CAP-6", "CV-FRAME-1"} {
		t.Run(id, func(t *testing.T) { equalityRun(t, loadEqualityVector(t, id)) })
	}
}

func equalityRun(t *testing.T, v equalityVector) {
	module := mustHex(t, v.ModuleWasmHex)
	caps := vectorCaps{}
	for k, amount := range v.WithdrawalCaps {
		var a codec.AssetID
		var c codec.U256
		copy(a[:], mustHex(t, k))
		copy(c[:], mustHex(t, amount))
		caps[a] = c
	}
	var l1Sender types.Address
	copy(l1Sender[:], mustHex(t, v.L1Sender))
	callPayload := mustHex(t, v.CanonicalInput)

	// Batch 1: seed the vector's pre-state and deploy its module into a
	// disk-store plugin.
	disk, err := wasm.New(wasm.Config{NetworkName: "testnet", CodeStorePath: t.TempDir()})
	if err != nil {
		t.Fatalf("wasm.New: %v", err)
	}
	store := state.OpenEmpty()
	t.Cleanup(func() { store.Close() })
	var seed []domain.StateDiffEntry
	for p, val := range v.PreState {
		var path trie.Path
		copy(path[:], mustHex(t, p))
		seed = append(seed, domain.StateDiffEntry{Path: path, Value: mustHex(t, val)})
	}
	if _, err := store.Apply(seed); err != nil {
		t.Fatalf("seed pre-state: %v", err)
	}

	preCodeHash := wasm.CodeHash(module)
	deployer := types.Address{7}
	encode := func(args codec.InputArgs) []byte {
		t.Helper()
		b, err := codec.Input{Args: args}.Encode()
		if err != nil {
			t.Fatalf("encode input: %v", err)
		}
		return b
	}
	metadata := codec.ContractMetadata{Version: codec.ContractMetadataVersion, UpgradePolicy: codec.PolicyImmutable}.Encode()
	var metadataHash types.Hash
	copy(metadataHash[:], crypto.Hash(metadata))
	queue := &inputQueue{inputs: []ledger.Input{
		{Ctx: domain.InputContext{GlobalInputIndex: 0, L1Sender: deployer}, Payload: encode(codec.DeployStartArgs{
			CodeHash: preCodeHash, MetadataHash: metadataHash, TotalSize: uint64(len(module)), ChunkCount: 1, Metadata: metadata,
		})},
		{Ctx: domain.InputContext{GlobalInputIndex: 1, L1Sender: deployer, SourceData: module}, Payload: encode(codec.DeployChunkArgs{
			CodeHash: preCodeHash, ChunkIndex: 0, ChunkHash: preCodeHash,
		})},
		{Ctx: domain.InputContext{GlobalInputIndex: 2, L1Sender: deployer}, Payload: encode(codec.DeployFinalizeArgs{CodeHash: preCodeHash})},
	}}
	driver, err := batch.New(batch.Config{
		DomainID: 1, ExecutorID: types.Address{9}, Domain: disk, Store: store,
		Inputs: queue, Bundles: memBundles{}, Receipts: nilReceipts{}, FirstBatchID: 1,
	})
	if err != nil {
		t.Fatalf("batch.New: %v", err)
	}
	if _, ok, err := driver.NextBatch(); err != nil || !ok {
		t.Fatalf("batch 1: ok=%v err=%v", ok, err)
	}

	// Batch 2 and the simulation run the vector's input from the same
	// pre-state.
	version := store.Version()
	var momentumHash types.Hash
	copy(momentumHash[:], mustHex(t, v.MomentumHash))
	sim := simipc.Simulate{
		GlobalInputIndex: v.GlobalInputIndex, MomentumHeight: v.MomentumHeight, MomentumTime: v.MomentumTime,
		MomentumHash: momentumHash,
		L1Sender:     l1Sender, InputData: callPayload,
	}

	var simPlugin *wasm.Plugin
	newSim := func(c SimConfig, code CodeFetcher) (domain.Domain, error) {
		p, err := wasm.New(wasm.Config{NetworkName: c.NetworkName, FetchModule: code})
		simPlugin = p
		return p, err
	}
	a, b := net.Pipe()
	done := make(chan int, 1)
	go func() {
		code := simulate(context.Background(), bufio.NewReader(b), b, newSim)
		b.Close()
		done <- code
	}()
	t.Cleanup(func() {
		a.Close()
		select {
		case code := <-done:
			if code != 0 {
				t.Errorf("simulate exited %d, want 0", code)
			}
		case <-time.After(10 * time.Second):
			t.Error("simulate did not return")
		}
		if simPlugin != nil {
			simPlugin.Close()
		}
		disk.Close()
	})

	codeRequests := 0
	run := func() simipc.Result {
		t.Helper()
		if err := simipc.WriteMessage(a, sim); err != nil {
			t.Fatal(err)
		}
		for {
			m, err := simipc.ReadFromSimulator(a)
			if err != nil {
				t.Fatalf("read: %v", err)
			}
			var reply simipc.Message
			switch r := m.(type) {
			case simipc.Result:
				return r
			case simipc.Failed:
				t.Fatalf("simulation failed: %s", r.Message)
			case simipc.ReadRequest:
				present, value, _ := version.Prove(r.Path)
				reply = simipc.Value{Present: present, Value: value}
			case simipc.CodeRequest:
				codeRequests++
				if r.SourceCodeHash != preCodeHash {
					reply = simipc.Code{}
				} else {
					reply = simipc.Code{Found: true, Module: module}
				}
			case simipc.CapRequest:
				amounts, _ := caps.MaxBatchWithdrawals(r.Assets)
				reply = simipc.Caps{Amounts: amounts}
			default:
				t.Fatalf("unexpected %T", m)
			}
			if err := simipc.WriteMessage(a, reply); err != nil {
				t.Fatal(err)
			}
		}
	}

	var ready simipc.Message
	if err := simipc.WriteMessage(a, simipc.Config{DomainID: 1, NetworkName: "testnet"}); err != nil {
		t.Fatal(err)
	}
	if ready, err = simipc.ReadFromSimulator(a); err != nil {
		t.Fatal(err)
	}
	if _, ok := ready.(simipc.Ready); !ok {
		t.Fatalf("first reply is %T, want Ready", ready)
	}
	got := run()

	queue.inputs = append(queue.inputs, ledger.Input{
		Ctx: domain.InputContext{
			GlobalInputIndex: v.GlobalInputIndex, MomentumHeight: v.MomentumHeight, MomentumTime: v.MomentumTime,
			MomentumHash: momentumHash,
			L1Sender:     l1Sender, Caps: caps,
		},
		Payload: callPayload,
	})
	built, ok, err := driver.NextBatch()
	if err != nil || !ok {
		t.Fatalf("batch 2: ok=%v err=%v", ok, err)
	}
	if len(built.Receipts) != 1 {
		t.Fatalf("batch 2 has %d receipts", len(built.Receipts))
	}
	rec := built.Receipts[0]
	if got.Status != rec.Status || got.GasUsed != rec.GasUsed || got.ContractID != rec.ContractID ||
		got.CodeHash != rec.CodeHash || got.AbortCode != rec.AbortCode {
		t.Errorf("simulated %+v, driver receipt status %v gas %d contract %x code %x abort %d",
			got, rec.Status, rec.GasUsed, rec.ContractID, rec.CodeHash, rec.AbortCode)
	}

	records, err := codec.DecodeBundle(built.Bundle)
	if err != nil || len(records) != 1 {
		t.Fatalf("decode bundle: %d records, %v", len(records), err)
	}
	want, err := codec.DecodeExecutionResult(records[0].ExecutionResult)
	if err != nil {
		t.Fatalf("decode driver result: %v", err)
	}
	want.PreStateRoot, want.PostStateRoot = types.Hash{}, types.Hash{}
	wantBytes, err := want.Encode()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got.ExecutionResult, wantBytes) {
		t.Errorf("execution result differs:\n sim    %x\n driver %x", got.ExecutionResult, wantBytes)
	}

	// The simulator and the driver agreeing is not enough: the vector's
	// own outcome pins the status, gas and effects (claimed deposit,
	// refund, rollback) it exists to exercise.
	if got.Status.String() != v.Status || got.GasUsed != v.GasUsed {
		t.Errorf("simulated status %v gas %d, vector expects %s gas %d",
			got.Status, got.GasUsed, v.Status, v.GasUsed)
	}
	vectorResult, err := codec.DecodeExecutionResult(mustHex(t, v.ExecutionResult))
	if err != nil {
		t.Fatalf("decode vector result: %v", err)
	}
	vectorResult.PreStateRoot, vectorResult.PostStateRoot = types.Hash{}, types.Hash{}
	vectorBytes, err := vectorResult.Encode()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got.ExecutionResult, vectorBytes) {
		t.Errorf("execution result differs from the vector:\n sim    %x\n vector %x", got.ExecutionResult, vectorBytes)
	}

	if again := run(); !equalResults(again, got) {
		t.Errorf("a second simulation returned %+v, want %+v", again, got)
	}
	if codeRequests != 1 {
		t.Errorf("%d code requests over two simulations, want 1", codeRequests)
	}
}

func equalResults(a, b simipc.Result) bool {
	return a.Status == b.Status && a.GasUsed == b.GasUsed && a.ContractID == b.ContractID &&
		a.CodeHash == b.CodeHash && a.AbortCode == b.AbortCode && bytes.Equal(a.ExecutionResult, b.ExecutionResult)
}
