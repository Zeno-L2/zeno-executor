package wasm

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/zenon-network/go-zenon/common/crypto"
	"github.com/zenon-network/go-zenon/common/types"

	"github.com/digitalSloth/go-executor/domain"
	"github.com/digitalSloth/go-executor/internal/codec"
)

func newTestPlugin(t *testing.T) *Plugin {
	t.Helper()
	p, err := New(Config{NetworkName: "testnet", CodeStorePath: t.TempDir()})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = p.Close() })
	return p
}

func TestNewRejectsEmptyNetworkName(t *testing.T) {
	if _, err := New(Config{NetworkName: "", CodeStorePath: t.TempDir()}); err == nil {
		t.Fatal("New: got nil error for empty NetworkName, want an error")
	}
}

func TestNewRejectsUnusableStorePath(t *testing.T) {
	if _, err := New(Config{NetworkName: "testnet", CodeStorePath: ""}); err == nil {
		t.Fatal("New: got nil error for empty CodeStorePath, want an error")
	}
}

// encodeInput is a small test helper that builds a canonical §27.5 payload for args.
func encodeInput(t *testing.T, args codec.InputArgs) []byte {
	t.Helper()
	b, err := codec.Input{SourceAccountBlockHash: types.Hash{}, Args: args}.Encode()
	if err != nil {
		t.Fatalf("encode input: %v", err)
	}
	return b
}

func TestDecodeInputAcceptsAllSixKinds(t *testing.T) {
	p := newTestPlugin(t)

	chunkData := []byte("chunk bytes")
	chunkHash := hashOf(crypto.Hash(chunkData))

	cases := []struct {
		name string
		args codec.InputArgs
		ctx  domain.InputContext
	}{
		{"CallL2", codec.CallL2Args{TargetContract: types.Hash{1}, PlasmaLimit: 1000}, domain.InputContext{}},
		{"Deposit", codec.DepositArgs{TargetContract: types.Hash{1}, PlasmaLimit: 1000}, domain.InputContext{}},
		{"DeployStart", codec.DeployStartArgs{CodeHash: types.Hash{2}, MetadataHash: types.Hash{3}, TotalSize: 100, ChunkCount: 1}, domain.InputContext{}},
		{"DeployChunk", codec.DeployChunkArgs{CodeHash: types.Hash{2}, ChunkIndex: 0, ChunkHash: chunkHash}, domain.InputContext{SourceData: chunkData}},
		{"DeployFinalize", codec.DeployFinalizeArgs{CodeHash: types.Hash{2}}, domain.InputContext{}},
		{"Upgrade", codec.UpgradeArgs{ContractID: types.Hash{4}, NewCodeHash: types.Hash{5}}, domain.InputContext{}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			payload := encodeInput(t, tc.args)
			got, err := p.DecodeInput(tc.ctx, payload)
			if err != nil {
				t.Fatalf("DecodeInput: %v", err)
			}
			in, ok := got.(Input)
			if !ok {
				t.Fatalf("DecodeInput: got %T, want wasm.Input", got)
			}
			if in.Unsupported {
				t.Fatal("DecodeInput: Unsupported = true for a Phase-1 kind")
			}
			if in.Args.Kind() != tc.args.Kind() {
				t.Fatalf("DecodeInput: kind = %d, want %d", in.Args.Kind(), tc.args.Kind())
			}
		})
	}
}

func TestDecodeInputUnsupportedKindNoError(t *testing.T) {
	p := newTestPlugin(t)

	// input_kind = 6 is outside the Phase-1 set {0..5}; the tail is irrelevant since
	// codec.DecodeInput rejects on the kind byte before reading it.
	payload := append([]byte{6}, make([]byte, 32)...)

	got, err := p.DecodeInput(domain.InputContext{}, payload)
	if err != nil {
		t.Fatalf("DecodeInput: got error %v, want nil (unsupported kind is a value, not a halt)", err)
	}
	in, ok := got.(Input)
	if !ok {
		t.Fatalf("DecodeInput: got %T, want wasm.Input", got)
	}
	if !in.Unsupported {
		t.Fatal("DecodeInput: Unsupported = false, want true for input_kind = 6")
	}
}

func TestDecodeInputHaltsOnMalformedEncoding(t *testing.T) {
	p := newTestPlugin(t)

	if _, err := p.DecodeInput(domain.InputContext{}, []byte{}); err == nil {
		t.Fatal("DecodeInput: got nil error for a truncated payload, want a halt error")
	}
}

func TestDecodeInputHaltsOnChunkHashMismatch(t *testing.T) {
	p := newTestPlugin(t)

	args := codec.DeployChunkArgs{CodeHash: types.Hash{2}, ChunkIndex: 0, ChunkHash: types.Hash{0xAA}}
	payload := encodeInput(t, args)
	ctx := domain.InputContext{SourceData: []byte("does not hash to ChunkHash")}

	if _, err := p.DecodeInput(ctx, payload); err == nil {
		t.Fatal("DecodeInput: got nil error for a chunk_hash mismatch, want a halt error")
	}
}

func TestDecodeInputHaltsOnOversizedChunk(t *testing.T) {
	p := newTestPlugin(t)

	data := make([]byte, MaxDeployChunkData+1)
	args := codec.DeployChunkArgs{CodeHash: types.Hash{2}, ChunkIndex: 0, ChunkHash: hashOf(crypto.Hash(data))}
	payload := encodeInput(t, args)
	ctx := domain.InputContext{SourceData: data}

	if _, err := p.DecodeInput(ctx, payload); err == nil {
		t.Fatal("DecodeInput: got nil error for an oversized chunk, want a halt error")
	}
}

func TestDecodeInputHaltsOnUnexpectedSourceData(t *testing.T) {
	p := newTestPlugin(t)

	args := codec.CallL2Args{TargetContract: types.Hash{1}, PlasmaLimit: 1000}
	payload := encodeInput(t, args)
	ctx := domain.InputContext{SourceData: []byte("not allowed on kind 0")}

	if _, err := p.DecodeInput(ctx, payload); err == nil {
		t.Fatal("DecodeInput: got nil error for SourceData on a non-kind-3 input, want a halt error")
	}
}

func TestDecodeInputHaltsOnMissingSourceDataForKind3(t *testing.T) {
	p := newTestPlugin(t)

	args := codec.DeployChunkArgs{CodeHash: types.Hash{2}, ChunkIndex: 0, ChunkHash: types.Hash{0xAA}}
	payload := encodeInput(t, args)

	if _, err := p.DecodeInput(domain.InputContext{}, payload); err == nil {
		t.Fatal("DecodeInput: got nil error for empty SourceData on kind 3, want a halt error")
	}
}

func TestChainIDMatchesFormulaAndIsPerNetwork(t *testing.T) {
	p1, err := New(Config{NetworkName: "mainnet", CodeStorePath: t.TempDir()})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = p1.Close() })
	want := hashOf(crypto.Hash([]byte("ZENON_WASM_L2:"), []byte("mainnet")))
	if p1.chainID != want {
		t.Fatalf("chainID = %x, want %x", p1.chainID, want)
	}

	p2, err := New(Config{NetworkName: "mainnet", CodeStorePath: t.TempDir()})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = p2.Close() })
	if p1.chainID != p2.chainID {
		t.Fatalf("chainID is not deterministic across constructions for the same network: %x != %x", p1.chainID, p2.chainID)
	}

	p3, err := New(Config{NetworkName: "testnet", CodeStorePath: t.TempDir()})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = p3.Close() })
	if p1.chainID == p3.chainID {
		t.Fatal("chainID is the same for two different network names, want distinct")
	}
}

func TestDecodeInputAcceptsChunkAtMaxSize(t *testing.T) {
	p := newTestPlugin(t)

	data := make([]byte, MaxDeployChunkData)
	args := codec.DeployChunkArgs{CodeHash: types.Hash{2}, ChunkIndex: 0, ChunkHash: hashOf(crypto.Hash(data))}
	payload := encodeInput(t, args)
	ctx := domain.InputContext{SourceData: data}

	if _, err := p.DecodeInput(ctx, payload); err != nil {
		t.Fatalf("DecodeInput: got error %v for a chunk exactly at MaxDeployChunkData, want nil", err)
	}
}

func TestDecodeInputHaltsOnCodeStoreWriteFailure(t *testing.T) {
	root := t.TempDir()
	p, err := New(Config{NetworkName: "testnet", CodeStorePath: root})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = p.Close() })

	// Make the deploy-chunk subdirectory unwritable so the store's temp-file-then-rename write
	// fails with a genuine I/O fault, distinct from the decode-time rejections above.
	dir := filepath.Join(root, entryDeployChunk.dirName())
	if err := os.Chmod(dir, 0o500); err != nil {
		t.Fatalf("chmod: %v", err)
	}
	t.Cleanup(func() { os.Chmod(dir, 0o755) })

	data := []byte("chunk bytes")
	args := codec.DeployChunkArgs{CodeHash: types.Hash{2}, ChunkIndex: 0, ChunkHash: hashOf(crypto.Hash(data))}
	payload := encodeInput(t, args)
	ctx := domain.InputContext{SourceData: data}

	if _, err := p.DecodeInput(ctx, payload); err == nil {
		t.Fatal("DecodeInput: got nil error when the code store cannot be written, want a halt error")
	}
}

func TestDecodeInputKind3WritesChunkToStore(t *testing.T) {
	p := newTestPlugin(t)

	data := []byte("some module chunk")
	args := codec.DeployChunkArgs{CodeHash: types.Hash{7}, ChunkIndex: 3, ChunkHash: hashOf(crypto.Hash(data))}
	payload := encodeInput(t, args)
	ctx := domain.InputContext{SourceData: data}

	if _, err := p.DecodeInput(ctx, payload); err != nil {
		t.Fatalf("DecodeInput: %v", err)
	}

	got, ok, err := p.store.get(entryDeployChunk, deployChunkKey(args.CodeHash, args.ChunkIndex))
	if err != nil {
		t.Fatalf("store.get: %v", err)
	}
	if !ok {
		t.Fatal("store.get: chunk not found after DecodeInput")
	}
	if string(got) != string(data) {
		t.Fatalf("store.get: chunk = %q, want %q", got, data)
	}
}

func TestDecodeInputKind2WritesPendingRecord(t *testing.T) {
	p := newTestPlugin(t)

	args := codec.DeployStartArgs{CodeHash: types.Hash{9}, MetadataHash: types.Hash{10}, TotalSize: 12345, ChunkCount: 3}
	payload := encodeInput(t, args)
	sender := types.Address{1, 2, 3}
	ctx := domain.InputContext{L1Sender: sender}

	if _, err := p.DecodeInput(ctx, payload); err != nil {
		t.Fatalf("DecodeInput: %v", err)
	}

	got, ok, err := p.store.get(entryPendingRecord, args.CodeHash[:])
	if err != nil {
		t.Fatalf("store.get: %v", err)
	}
	if !ok {
		t.Fatal("store.get: pending record not found after DecodeInput")
	}
	want := encodePendingRecord(args.MetadataHash, args.TotalSize, args.ChunkCount, sender)
	if string(got) != string(want) {
		t.Fatalf("store.get: record = %x, want %x", got, want)
	}
}

func TestApplyUnsupportedKind(t *testing.T) {
	p := newTestPlugin(t)

	eff, err := p.Apply(nil, Input{Unsupported: true})
	if err != nil {
		t.Fatalf("Apply: got error %v, want nil", err)
	}
	if eff.Status != codec.StatusUnsupportedOperation {
		t.Fatalf("Status = %v, want StatusUnsupportedOperation", eff.Status)
	}
	if eff.GasUsed != 0 {
		t.Fatalf("GasUsed = %d, want 0", eff.GasUsed)
	}
	if eff.ContractID != (types.Hash{}) {
		t.Fatalf("ContractID = %x, want zero", eff.ContractID)
	}
	if eff.CodeHash != (types.Hash{}) {
		t.Fatalf("CodeHash = %x, want zero", eff.CodeHash)
	}
	if len(eff.OutboxMessages) != 0 {
		t.Fatalf("OutboxMessages = %v, want none", eff.OutboxMessages)
	}
}

func TestApplyRejectsForeignInputType(t *testing.T) {
	p := newTestPlugin(t)
	if _, err := p.Apply(nil, "not a wasm.Input"); err == nil {
		t.Fatal("Apply: got nil error for a foreign Input type, want an error")
	}
}

func TestPluginClose(t *testing.T) {
	p, err := New(Config{NetworkName: "testnet", CodeStorePath: t.TempDir()})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := p.Close(); err != nil {
		t.Fatalf("Close: got error %v, want nil", err)
	}
}
