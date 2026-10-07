package wasm

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"flag"
	"os"
	"path/filepath"
	"testing"

	"github.com/zenon-network/go-zenon/common/crypto"
	"github.com/zenon-network/go-zenon/common/types"

	"github.com/digitalSloth/go-executor/domain"
	"github.com/digitalSloth/go-executor/internal/codec"
	"github.com/digitalSloth/go-executor/internal/state"
	"github.com/digitalSloth/go-executor/plugins/wasm/internal/engine"
	"github.com/digitalSloth/go-executor/plugins/wasm/internal/module"
)

// --- the JSON schema, frozen here for later volume tickets to add rows to. It changes only
// alongside a change to the committed bytes it describes: the regenerated artifact and its
// vectorFileDigest.

type vectorFile struct {
	Artifact               string       `json:"artifact"` // "execution-conformance-v1"
	ExecutionResultVersion uint16       `json:"executionresult_version"`
	ReceiptVersion         uint16       `json:"receipt_version"`
	ContextVersion         uint16       `json:"context_version"`
	MeteringVersion        uint16       `json:"metering_version"` // codec.MeteringVersion; gas_used is a function of it
	Hash                   string       `json:"hash"`             // "SHA3-256 (FIPS-202)"
	NetworkName            string       `json:"network_name"`     // "testnet"
	ChainID                string       `json:"chain_id"`
	Notes                  []string     `json:"notes"`
	Vectors                []vectorJSON `json:"vectors"`
}

type vectorJSON struct {
	ID                     string            `json:"id"`
	Purpose                string            `json:"purpose"`
	Shape                  string            `json:"shape"` // "call" | "deploy" | "buy_storage" | "validation_rejection" | "unsupported_kind"
	BatchID                uint64            `json:"batch_id"`
	GlobalInputIndex       uint64            `json:"global_input_index"`
	MomentumHeight         uint64            `json:"momentum_height"`
	MomentumTime           uint64            `json:"momentum_time"`
	L1Sender               string            `json:"l1_sender"` // 20-byte hex
	SourceAccountBlockHash string            `json:"source_account_block_hash"`
	InputKind              uint8             `json:"input_kind"`
	Input                  *vectorInput      `json:"input,omitempty"` // absent for an unrecognized kind
	CanonicalInputHex      string            `json:"canonical_input_hex"`
	SourceDataHex          string            `json:"source_data_hex,omitempty"`        // kind 3 only (InputContext.SourceData)
	ModuleWasmHex          string            `json:"module_wasm_hex,omitempty"`        // schema-optional
	CodeHash               string            `json:"code_hash,omitempty"`              // SPEC's code_hash (pre-instrumentation)
	InstrumentedCodeHash   string            `json:"instrumented_code_hash,omitempty"` // SPEC's CodeHash (§11.4)
	InputFrameHex          string            `json:"input_frame_hex,omitempty"`        // call shape only
	WithdrawalCaps         map[string]string `json:"withdrawal_caps"`                  // AssetID hex -> big-endian u256 hex; an absent asset has cap 0
	PreState               map[string]string `json:"pre_state"`                        // derived path hex -> value hex ("" is present-empty)
	PendingMetadataHex     *string           `json:"pending_metadata_hex,omitempty"`   // kinds 4,5 only
	PreStateRoot           string            `json:"pre_state_root"`
	WitnessKeys            []string          `json:"witness_keys"` // read ∪ written (§20), ascending
	Status                 string            `json:"status"`
	GasUsed                uint64            `json:"gas_used"`
	ContractEffectsBlobHex string            `json:"contract_effects_blob_hex,omitempty"`
	ExecutionResultBlobHex string            `json:"execution_result_blob_hex,omitempty"`
	PostStateRoot          string            `json:"post_state_root"`
	Receipt                vectorReceipt     `json:"receipt"`
	ReceiptHex             string            `json:"receipt_hex"`
}

// vectorInput is the decoded, human-readable §27.5 kind-specific tail. canonical_input_hex is
// authoritative; this view exists so a reader can see what those bytes encode. A field the
// vector's kind carries is always present, including at its zero value; a field the kind does
// not carry is always absent. Fields whose rendering can legitimately be empty or zero are
// therefore pointers, since omitempty would erase them.
type vectorInput struct {
	TargetContract string  `json:"target_contract,omitempty"` // kinds 0,1
	PlasmaLimit    *uint64 `json:"plasma_limit,omitempty"`    // kinds 0,1
	DepositAsset   string  `json:"deposit_asset,omitempty"`   // kinds 0,1; 32-byte hex
	DepositAmount  string  `json:"deposit_amount,omitempty"`  // kinds 0,1; 32-byte BE hex
	PayloadHex     *string `json:"payload_hex,omitempty"`     // kind 0; "" for an empty payload
	CodeHash       string  `json:"code_hash,omitempty"`       // kinds 2,3,4
	MetadataHash   string  `json:"metadata_hash,omitempty"`   // kind 2
	MetadataHex    *string `json:"metadata_hex,omitempty"`    // kind 2
	TotalSize      *uint64 `json:"total_size,omitempty"`      // kind 2
	ChunkCount     *uint32 `json:"chunk_count,omitempty"`     // kind 2
	ChunkIndex     *uint32 `json:"chunk_index,omitempty"`     // kind 3; 0 is the single-chunk case
	ChunkHash      string  `json:"chunk_hash,omitempty"`      // kind 3
	ContractID     string  `json:"contract_id,omitempty"`     // kinds 5,6,7
	NewCodeHash    string  `json:"new_code_hash,omitempty"`   // kind 5
	NewOwner       string  `json:"new_owner,omitempty"`       // kind 6; 20-byte hex
	Bytes          *uint64 `json:"bytes,omitempty"`           // kind 7; non-zero
}

// vectorReceipt is the decoded §27.7 receipt view. Its code_hash is §11.4's CodeHash, i.e.
// the vector's instrumented_code_hash, not its top-level code_hash.
type vectorReceipt struct {
	Version       uint16 `json:"version"`
	InputIndex    uint64 `json:"input_index"`
	InputHash     string `json:"input_hash"`
	BatchID       uint64 `json:"batch_id"`
	ContractID    string `json:"contract_id"`
	CodeHash      string `json:"code_hash"`
	Status        string `json:"status"`
	GasUsed       uint64 `json:"gas_used"`
	AbortCode     int32  `json:"abort_code"`
	StateDiffHash string `json:"state_diff_hash"`
	ReturnHash    string `json:"return_hash"`
	EventRoot     string `json:"event_root"`
	EventCount    uint32 `json:"event_count"`
	OutboxRoot    string `json:"outbox_root"`
	OutboxCount   uint32 `json:"outbox_count"`
}

func u64p(v uint64) *uint64 { return &v }
func u32p(v uint32) *uint32 { return &v }
func strp(s string) *string { return &s }

// --- vector specs.

// vectorSpec is everything hand-authored about one vector. Every committed value in the
// artifact is derived by running the vector; none is written by hand.
type vectorSpec struct {
	ID                     string
	Purpose                string
	Shape                  string
	BatchID                uint64
	Ctx                    domain.InputContext
	SourceAccountBlockHash types.Hash
	Args                   codec.InputArgs
	ContractID             types.Hash
	Module                 []byte                 // raw, pre-instrumentation
	ExtraPreState          map[domain.Path][]byte // seeded beyond the two reserved leaves
	WantStatus             codec.Status           // the §15 status this vector exists to freeze
	PendingMetadata        []byte                 // seeded into a kind-4/5 vector's pending record
	WithdrawalCaps         capTable               // step 5a's cap table; nil is empty
}

// capTable is a domain.CapLookup over a fixed table. An asset absent from it has cap 0, the
// rule the artifact's withdrawal_caps states.
type capTable map[codec.AssetID]codec.U256

func (c capTable) MaxBatchWithdrawals(assets []codec.AssetID) ([]codec.U256, error) {
	out := make([]codec.U256, len(assets))
	for i, a := range assets {
		out[i] = c[a]
	}
	return out, nil
}

// capsHex renders c as a withdrawal_caps object, lowercase hex, never nil.
func capsHex(c capTable) map[string]string {
	out := make(map[string]string, len(c))
	for a, v := range c {
		out[hex.EncodeToString(a[:])] = hex.EncodeToString(v[:])
	}
	return out
}

// capTableFromJSON parses a withdrawal_caps object, failing the test on a key or value that is
// not 32 bytes of hex.
func capTableFromJSON(t *testing.T, m map[string]string) capTable {
	t.Helper()
	out := capTable{}
	for k, v := range m {
		kb, err := hex.DecodeString(k)
		if err != nil || len(kb) != 32 {
			t.Fatalf("withdrawal_caps key %q is not 32 bytes of hex", k)
		}
		vb, err := hex.DecodeString(v)
		if err != nil || len(vb) != 32 {
			t.Fatalf("withdrawal_caps value %q is not 32 bytes of hex", v)
		}
		var a codec.AssetID
		var u codec.U256
		copy(a[:], kb)
		copy(u[:], vb)
		out[a] = u
	}
	return out
}

// repeatedHash builds a 32-byte hash whose every byte is b.
func repeatedHash(b byte) types.Hash {
	var h types.Hash
	for i := range h {
		h[i] = b
	}
	return h
}

// repeatedAddr builds a 20-byte address whose every byte is b.
func repeatedAddr(b byte) types.Address {
	var a types.Address
	for i := range a {
		a[i] = b
	}
	return a
}

// spineVectors returns the three vectors this artifact is proven against.
func spineVectors(t *testing.T) []vectorSpec {
	t.Helper()

	const plasmaLimit = 10_000_000

	// EXEC-001: pure-return success.
	blob1 := encodeEffects(t, codec.ContractEffects{
		Version:          codec.ExecutionResultVersion,
		GlobalInputIndex: 5,
		ReturnData:       []byte{0x01, 0x02, 0x03, 0x04},
	})
	body1 := appendVarS64([]byte{0x42}, (int64(1024)<<32)|int64(len(blob1)))
	body1 = append(body1, 0x0B)
	module1 := buildWasmModule(runModuleSpec(nil, 1, []byte{0x00}, body1, []dataSeg{{offset: 1024, bytes: blob1}}))

	contract1 := repeatedHash(0x11)
	sender1 := repeatedAddr(0x01)

	exec001 := vectorSpec{
		ID:      "EXEC-001",
		Purpose: "pure-return success",
		Shape:   "call",
		BatchID: 1,
		Ctx: domain.InputContext{
			GlobalInputIndex: 5,
			MomentumHeight:   1000,
			MomentumTime:     1700000000,
			L1Sender:         sender1,
		},
		SourceAccountBlockHash: repeatedHash(0xA1),
		Args: codec.CallL2Args{
			TargetContract: contract1,
			PlasmaLimit:    plasmaLimit,
			DepositAsset:   codec.AssetID{},
			DepositAmount:  codec.U256{},
			Payload:        []byte{0xde, 0xad},
		},
		ContractID: contract1,
		Module:     module1,
		WantStatus: codec.StatusSuccess,
	}

	// EXEC-002: single-write success with an absent-key observation.
	probe := []byte("probe")
	blob2 := encodeEffects(t, codec.ContractEffects{
		Version:          codec.ExecutionResultVersion,
		GlobalInputIndex: 6,
		StateDiff:        []codec.LocalStateDiffEntry{{LocalKey: []byte("counter"), Value: []byte{0x00, 0x00, 0x00, 0x2a}}},
	})
	body2 := []byte{0x41}
	body2 = appendVarS64(body2, 1024)
	body2 = append(body2, 0x41)
	body2 = appendVarS64(body2, 5)
	body2 = append(body2, 0x10, 0x00) // call 0 (env.storage_len)
	body2 = append(body2, 0x1A)       // drop
	body2 = append(body2, 0x42)
	body2 = appendVarS64(body2, (int64(1032)<<32)|int64(len(blob2)))
	body2 = append(body2, 0x0B)
	module2 := buildWasmModule(runModuleSpec([]string{"storage_len"}, 1, []byte{0x00}, body2,
		[]dataSeg{{offset: 1024, bytes: probe}, {offset: 1032, bytes: blob2}}))

	exec002 := vectorSpec{
		ID:      "EXEC-002",
		Purpose: "single-write success with an absent-key observation",
		Shape:   "call",
		BatchID: 1,
		Ctx: domain.InputContext{
			GlobalInputIndex: 6,
			MomentumHeight:   1000,
			MomentumTime:     1700000000,
			L1Sender:         sender1,
		},
		SourceAccountBlockHash: repeatedHash(0xA2),
		Args: codec.CallL2Args{
			TargetContract: contract1,
			PlasmaLimit:    plasmaLimit,
			DepositAsset:   codec.AssetID{},
			DepositAmount:  codec.U256{},
		},
		ContractID:    contract1,
		Module:        module2,
		ExtraPreState: seedQuota(contract1, nil, vectorQuota),
		WantStatus:    codec.StatusSuccess,
	}

	// EXEC-FAIL-OUTBOX-1: a failed input carrying a deposit.
	blob3 := encodeEffects(t, codec.ContractEffects{
		Version:          codec.ExecutionResultVersion,
		GlobalInputIndex: 8, // one less than the executing index (9): rejected by effects.go step 2
		StateDiff:        []codec.LocalStateDiffEntry{{LocalKey: []byte("doomed"), Value: []byte{0xbe, 0xef}}},
		ReturnData:       []byte{0xc0, 0xff, 0xee},
	})
	body3 := appendVarS64([]byte{0x42}, (int64(1024)<<32)|int64(len(blob3)))
	body3 = append(body3, 0x0B)
	module3 := buildWasmModule(runModuleSpec(nil, 1, []byte{0x00}, body3, []dataSeg{{offset: 1024, bytes: blob3}}))

	contract3 := repeatedHash(0x22)
	var asset3 codec.AssetID
	copy(asset3[32-types.ZenonTokenStandardSize:], []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a})

	exec3 := vectorSpec{
		ID:      "EXEC-FAIL-OUTBOX-1",
		Purpose: "a failed input carrying a deposit",
		Shape:   "call",
		BatchID: 7,
		Ctx: domain.InputContext{
			GlobalInputIndex: 9,
			MomentumHeight:   1001,
			MomentumTime:     1700000600,
			L1Sender:         repeatedAddr(0x02),
		},
		SourceAccountBlockHash: repeatedHash(0xA3),
		Args: codec.DepositArgs{
			TargetContract: contract3,
			PlasmaLimit:    plasmaLimit,
			DepositAsset:   asset3,
			DepositAmount:  u256(1000),
		},
		ContractID:    contract3,
		Module:        module3,
		ExtraPreState: seedQuota(contract3, nil, vectorQuota),
		WantStatus:    codec.StatusRuntimeFault,
	}

	return []vectorSpec{exec001, exec002, exec3}
}

// --- the generator.

var updateVectors = flag.Bool("update", false,
	"regenerate plugins/wasm/testdata/execution-conformance-v1.json")

const vectorFilePath = "testdata/execution-conformance-v1.json"

// inputView renders a decoded §27.5 tail as the artifact's human-readable input view. It is a
// view renderer, not a derivation, so both the generator and the runner use it.
func inputView(args codec.InputArgs) *vectorInput {
	switch a := args.(type) {
	case codec.CallL2Args:
		payload := hex.EncodeToString(a.Payload)
		return &vectorInput{
			TargetContract: hex.EncodeToString(a.TargetContract[:]),
			PlasmaLimit:    u64p(a.PlasmaLimit),
			DepositAsset:   hex.EncodeToString(a.DepositAsset[:]),
			DepositAmount:  hex.EncodeToString(a.DepositAmount[:]),
			PayloadHex:     strp(payload),
		}
	case codec.DepositArgs:
		return &vectorInput{
			TargetContract: hex.EncodeToString(a.TargetContract[:]),
			PlasmaLimit:    u64p(a.PlasmaLimit),
			DepositAsset:   hex.EncodeToString(a.DepositAsset[:]),
			DepositAmount:  hex.EncodeToString(a.DepositAmount[:]),
		}
	case codec.DeployStartArgs:
		metadataHex := hex.EncodeToString(a.Metadata)
		return &vectorInput{
			CodeHash:     hex.EncodeToString(a.CodeHash[:]),
			MetadataHash: hex.EncodeToString(a.MetadataHash[:]),
			MetadataHex:  strp(metadataHex),
			TotalSize:    u64p(a.TotalSize),
			ChunkCount:   u32p(a.ChunkCount),
		}
	case codec.DeployChunkArgs:
		return &vectorInput{
			CodeHash:   hex.EncodeToString(a.CodeHash[:]),
			ChunkIndex: u32p(a.ChunkIndex),
			ChunkHash:  hex.EncodeToString(a.ChunkHash[:]),
		}
	case codec.DeployFinalizeArgs:
		return &vectorInput{CodeHash: hex.EncodeToString(a.CodeHash[:])}
	case codec.UpgradeArgs:
		return &vectorInput{
			ContractID:  hex.EncodeToString(a.ContractID[:]),
			NewCodeHash: hex.EncodeToString(a.NewCodeHash[:]),
		}
	case codec.TransferOwnershipArgs:
		return &vectorInput{
			ContractID: hex.EncodeToString(a.ContractID[:]),
			NewOwner:   hex.EncodeToString(a.NewOwner[:]),
		}
	case codec.BuyStorageArgs:
		return &vectorInput{
			ContractID: hex.EncodeToString(a.ContractID[:]),
			Bytes:      u64p(a.Bytes),
		}
	default:
		return nil
	}
}

// toVectorReceipt renders a codec.Receipt as the artifact's hex-string receipt view.
func toVectorReceipt(rec codec.Receipt) vectorReceipt {
	return vectorReceipt{
		Version:       rec.Version,
		InputIndex:    rec.InputIndex,
		InputHash:     hex.EncodeToString(rec.InputHash[:]),
		BatchID:       rec.BatchID,
		ContractID:    hex.EncodeToString(rec.ContractID[:]),
		CodeHash:      hex.EncodeToString(rec.CodeHash[:]),
		Status:        rec.Status.String(),
		GasUsed:       rec.GasUsed,
		AbortCode:     rec.AbortCode,
		StateDiffHash: hex.EncodeToString(rec.StateDiffHash[:]),
		ReturnHash:    hex.EncodeToString(rec.ReturnHash[:]),
		EventRoot:     hex.EncodeToString(rec.EventRoot[:]),
		EventCount:    rec.EventCount,
		OutboxRoot:    hex.EncodeToString(rec.OutboxRoot[:]),
		OutboxCount:   rec.OutboxCount,
	}
}

// generateVector runs one call-path vector through the real Plugin.DecodeInput/Plugin.Apply
// seam against a file-backed state.Store and returns its JSON row.
func generateVector(t *testing.T, spec vectorSpec, chainID types.Hash) vectorJSON {
	t.Helper()

	if err := module.Validate(spec.Module, moduleLimits()); err != nil {
		t.Fatalf("%s: Validate: %v", spec.ID, err)
	}
	instrumented, err := module.Instrument(spec.Module, moduleLimits())
	if err != nil {
		t.Fatalf("%s: Instrument: %v", spec.ID, err)
	}
	codeHash := CodeHash(spec.Module)
	instrumentedCodeHash := CodeHash(instrumented)

	p, err := New(Config{NetworkName: "testnet", CodeStorePath: t.TempDir()})
	if err != nil {
		t.Fatalf("%s: New: %v", spec.ID, err)
	}
	t.Cleanup(func() { _ = p.Close() })

	if err := p.store.put(entryAssembledModule, codeHash[:], spec.Module); err != nil {
		t.Fatalf("%s: store.put assembled: %v", spec.ID, err)
	}
	if err := p.store.put(entryInstrumentedModule, instrumentedCodeHash[:], instrumented); err != nil {
		t.Fatalf("%s: store.put instrumented: %v", spec.ID, err)
	}

	// Witness backend (hard requirement): the committed accessor MUST be a state.Store-backed
	// state.Accessor. mapState records nothing on an absent-key Len observation, so a
	// mapState-backed generator would silently drop exactly the entries N1 checks for.
	store, err := state.Open(filepath.Join(t.TempDir(), "state.log"))
	if err != nil {
		t.Fatalf("%s: state.Open: %v", spec.ID, err)
	}
	t.Cleanup(func() { _ = store.Close() })

	seedDiff := []domain.StateDiffEntry{
		{Path: derive(spec.ContractID, keyCodeHash), Value: instrumentedCodeHash[:]},
		{Path: derive(spec.ContractID, keyPreCodeHash), Value: codeHash[:]},
	}
	preState := map[string]string{
		hex.EncodeToString(seedDiff[0].Path[:]): hex.EncodeToString(seedDiff[0].Value),
		hex.EncodeToString(seedDiff[1].Path[:]): hex.EncodeToString(seedDiff[1].Value),
	}
	for path, value := range spec.ExtraPreState {
		seedDiff = append(seedDiff, domain.StateDiffEntry{Path: path, Value: value})
		preState[hex.EncodeToString(path[:])] = hex.EncodeToString(value)
	}

	preStateRoot, err := store.Apply(seedDiff)
	if err != nil {
		t.Fatalf("%s: seed store.Apply: %v", spec.ID, err)
	}
	acc := store.Accessor()

	canonical, err := codec.Input{SourceAccountBlockHash: spec.SourceAccountBlockHash, Args: spec.Args}.Encode()
	if err != nil {
		t.Fatalf("%s: encode canonical input: %v", spec.ID, err)
	}
	inputHash := hashOf(crypto.Hash(canonical))

	ctx := spec.Ctx
	ctx.Caps = spec.WithdrawalCaps
	in, err := p.DecodeInput(ctx, canonical)
	if err != nil {
		t.Fatalf("%s: DecodeInput: %v", spec.ID, err)
	}
	decoded, ok := in.(Input)
	if !ok {
		t.Fatalf("%s: DecodeInput returned %T, want wasm.Input", spec.ID, in)
	}

	args, ok := toCallArgs(decoded.Args)
	if !ok {
		t.Fatalf("%s: toCallArgs: ok = false for %T", spec.ID, decoded.Args)
	}
	frame := buildFrame(p.chainID, spec.Ctx, args, instrumentedCodeHash)
	encodedFrame, err := frame.Encode()
	if err != nil {
		t.Fatalf("%s: frame.Encode: %v", spec.ID, err)
	}

	// The returned blob: an independent engine.Run over a throwaway accessor, so this run's
	// own storage_len observation never enters the committed witness set (acc's).
	probeAcc := store.Accessor()
	out, err := p.engine.Run(engine.HostContext{State: probeAcc, ContractID: spec.ContractID}, instrumentedCodeHash, instrumented, encodedFrame, args.PlasmaLimit)
	if err != nil {
		t.Fatalf("%s: engine.Run: %v", spec.ID, err)
	}
	haveBlob := out.Kind == engine.Returned && out.Ret >= 0

	eff, err := p.Apply(acc, decoded)
	if err != nil {
		t.Fatalf("%s: Apply: %v", spec.ID, err)
	}
	if eff.Status != spec.WantStatus {
		t.Fatalf("%s: Status = %v, want %v", spec.ID, eff.Status, spec.WantStatus)
	}

	// Cross-checks tying the independent run to Apply's own.
	if eff.ContractID != spec.ContractID {
		t.Fatalf("%s: eff.ContractID = %x, want %x", spec.ID, eff.ContractID, spec.ContractID)
	}
	if eff.CodeHash != instrumentedCodeHash {
		t.Fatalf("%s: eff.CodeHash = %x, want %x", spec.ID, eff.CodeHash, instrumentedCodeHash)
	}
	switch eff.Status {
	case codec.StatusSuccess:
		decodedBlob, err := codec.DecodeContractEffects(out.ResultBlob)
		if err != nil {
			t.Fatalf("%s: DecodeContractEffects: %v", spec.ID, err)
		}
		wantGasUsed := out.GasUsed + effectCharge(decodedBlob)
		if eff.GasUsed != wantGasUsed {
			t.Fatalf("%s: eff.GasUsed = %d, want %d", spec.ID, eff.GasUsed, wantGasUsed)
		}
		if !bytes.Equal(decodedBlob.ReturnData, eff.ReturnData) {
			t.Fatalf("%s: decoded blob ReturnData disagrees with eff.ReturnData", spec.ID)
		}
		if decodedBlob.ClaimedDeposit != eff.ClaimedDeposit {
			t.Fatalf("%s: decoded blob ClaimedDeposit disagrees with eff.ClaimedDeposit", spec.ID)
		}
	case codec.StatusRuntimeFault:
		if eff.GasUsed != out.GasUsed {
			t.Fatalf("%s: eff.GasUsed = %d, want %d (metered)", spec.ID, eff.GasUsed, out.GasUsed)
		}
	}

	postStateRoot, err := store.Apply(eff.StateDiff)
	if err != nil {
		t.Fatalf("%s: post store.Apply: %v", spec.ID, err)
	}

	ref := refDerive(t, eff, acc, spec.Ctx.GlobalInputIndex, spec.BatchID, preStateRoot, postStateRoot, inputHash)

	witnessKeys := make([]string, len(ref.WitnessKeys))
	for i, path := range ref.WitnessKeys {
		witnessKeys[i] = hex.EncodeToString(path[:])
	}

	vj := vectorJSON{
		ID:                     spec.ID,
		Purpose:                spec.Purpose,
		Shape:                  spec.Shape,
		BatchID:                spec.BatchID,
		GlobalInputIndex:       spec.Ctx.GlobalInputIndex,
		MomentumHeight:         spec.Ctx.MomentumHeight,
		MomentumTime:           spec.Ctx.MomentumTime,
		L1Sender:               hex.EncodeToString(spec.Ctx.L1Sender[:]),
		SourceAccountBlockHash: hex.EncodeToString(spec.SourceAccountBlockHash[:]),
		InputKind:              uint8(spec.Args.Kind()),
		Input:                  inputView(spec.Args),
		CanonicalInputHex:      hex.EncodeToString(canonical),
		ModuleWasmHex:          hex.EncodeToString(spec.Module),
		CodeHash:               hex.EncodeToString(codeHash[:]),
		InstrumentedCodeHash:   hex.EncodeToString(instrumentedCodeHash[:]),
		InputFrameHex:          hex.EncodeToString(encodedFrame),
		WithdrawalCaps:         capsHex(spec.WithdrawalCaps),
		PreState:               preState,
		PreStateRoot:           hex.EncodeToString(preStateRoot[:]),
		WitnessKeys:            witnessKeys,
		Status:                 eff.Status.String(),
		GasUsed:                eff.GasUsed,
		ExecutionResultBlobHex: hex.EncodeToString(ref.ExecutionResultBlob),
		PostStateRoot:          hex.EncodeToString(postStateRoot[:]),
		Receipt:                toVectorReceipt(ref.Receipt),
		ReceiptHex:             hex.EncodeToString(ref.ReceiptBlob),
	}
	if haveBlob {
		vj.ContractEffectsBlobHex = hex.EncodeToString(out.ResultBlob)
	}

	return vj
}

// generateUnsupportedVector drives one unrecognized-kind input through the real
// DecodeInput/Apply seam (§15). Its canonical input is assembled here rather than by
// codec.Input.Encode, which refuses a kind outside {0..7} by design: §27.5 fixes the encoding as
// input_kind:u8 ‖ source_account_block_hash:bytes32, with no tail.
func generateUnsupportedVector(t *testing.T, spec vectorSpec, kind uint8, chainID types.Hash) vectorJSON {
	t.Helper()

	p, err := New(Config{NetworkName: "testnet", CodeStorePath: t.TempDir()})
	if err != nil {
		t.Fatalf("%s: New: %v", spec.ID, err)
	}
	t.Cleanup(func() { _ = p.Close() })

	store, err := state.Open(filepath.Join(t.TempDir(), "state.log"))
	if err != nil {
		t.Fatalf("%s: state.Open: %v", spec.ID, err)
	}
	t.Cleanup(func() { _ = store.Close() })

	preStateRoot, err := store.Apply(nil)
	if err != nil {
		t.Fatalf("%s: seed store.Apply: %v", spec.ID, err)
	}
	acc := store.Accessor()

	canonical := append([]byte{kind}, spec.SourceAccountBlockHash[:]...)
	if len(canonical) != 33 {
		t.Fatalf("%s: len(canonical) = %d, want 33", spec.ID, len(canonical))
	}
	inputHash := hashOf(crypto.Hash(canonical))

	in, err := p.DecodeInput(spec.Ctx, canonical)
	if err != nil {
		t.Fatalf("%s: DecodeInput: %v", spec.ID, err)
	}
	decoded, ok := in.(Input)
	if !ok {
		t.Fatalf("%s: DecodeInput returned %T, want wasm.Input", spec.ID, in)
	}
	if !decoded.Unsupported {
		t.Fatalf("%s: DecodeInput: Unsupported = false, want true", spec.ID)
	}

	eff, err := p.Apply(acc, decoded)
	if err != nil {
		t.Fatalf("%s: Apply: %v", spec.ID, err)
	}
	if eff.Status != spec.WantStatus {
		t.Fatalf("%s: Status = %v, want %v", spec.ID, eff.Status, spec.WantStatus)
	}
	if eff.GasUsed != 0 {
		t.Fatalf("%s: eff.GasUsed = %d, want 0", spec.ID, eff.GasUsed)
	}
	if eff.ContractID != (types.Hash{}) {
		t.Fatalf("%s: eff.ContractID = %x, want zero", spec.ID, eff.ContractID)
	}
	if eff.CodeHash != (types.Hash{}) {
		t.Fatalf("%s: eff.CodeHash = %x, want zero", spec.ID, eff.CodeHash)
	}
	if len(eff.OutboxMessages) != 0 {
		t.Fatalf("%s: eff.OutboxMessages = %+v, want none", spec.ID, eff.OutboxMessages)
	}

	postStateRoot, err := store.Apply(eff.StateDiff)
	if err != nil {
		t.Fatalf("%s: post store.Apply: %v", spec.ID, err)
	}

	ref := refDerive(t, eff, acc, spec.Ctx.GlobalInputIndex, spec.BatchID, preStateRoot, postStateRoot, inputHash)

	witnessKeys := make([]string, len(ref.WitnessKeys))
	for i, path := range ref.WitnessKeys {
		witnessKeys[i] = hex.EncodeToString(path[:])
	}

	return vectorJSON{
		ID:                     spec.ID,
		Purpose:                spec.Purpose,
		Shape:                  spec.Shape,
		BatchID:                spec.BatchID,
		GlobalInputIndex:       spec.Ctx.GlobalInputIndex,
		MomentumHeight:         spec.Ctx.MomentumHeight,
		MomentumTime:           spec.Ctx.MomentumTime,
		L1Sender:               hex.EncodeToString(spec.Ctx.L1Sender[:]),
		SourceAccountBlockHash: hex.EncodeToString(spec.SourceAccountBlockHash[:]),
		InputKind:              kind,
		Input:                  nil,
		CanonicalInputHex:      hex.EncodeToString(canonical),
		WithdrawalCaps:         capsHex(nil),
		PreState:               map[string]string{},
		PreStateRoot:           hex.EncodeToString(preStateRoot[:]),
		WitnessKeys:            witnessKeys,
		Status:                 eff.Status.String(),
		GasUsed:                eff.GasUsed,
		ExecutionResultBlobHex: hex.EncodeToString(ref.ExecutionResultBlob),
		PostStateRoot:          hex.EncodeToString(postStateRoot[:]),
		Receipt:                toVectorReceipt(ref.Receipt),
		ReceiptHex:             hex.EncodeToString(ref.ReceiptBlob),
	}
}

// generateDeployVector runs one deploy-, buy_storage- or validation_rejection-shape vector through the
// real Plugin.DecodeInput/Plugin.Apply seam. No frame is built and no module is run: kinds 2-5 and 7
// invoke no contract.
func generateDeployVector(t *testing.T, spec vectorSpec) vectorJSON {
	t.Helper()

	p, err := New(Config{NetworkName: "testnet", CodeStorePath: t.TempDir()})
	if err != nil {
		t.Fatalf("%s: New: %v", spec.ID, err)
	}
	t.Cleanup(func() { _ = p.Close() })

	kind := spec.Args.Kind()
	if kind == codec.DeployFinalize || kind == codec.Upgrade {
		gotCodeHash := seedDeployment(t, p, spec.Module, spec.Ctx.L1Sender, spec.PendingMetadata)
		var wantCodeHash types.Hash
		switch a := spec.Args.(type) {
		case codec.DeployFinalizeArgs:
			wantCodeHash = a.CodeHash
		case codec.UpgradeArgs:
			wantCodeHash = a.NewCodeHash
		}
		if gotCodeHash != wantCodeHash {
			t.Fatalf("%s: seedDeployment code_hash = %x, want %x", spec.ID, gotCodeHash, wantCodeHash)
		}
	}

	var codeHash types.Hash
	var haveModule bool
	if spec.Module != nil {
		codeHash = CodeHash(spec.Module)
		haveModule = true
	}

	var instrumentedCodeHash types.Hash
	var haveInstrumented bool
	switch {
	case (kind == codec.DeployFinalize || kind == codec.Upgrade) && spec.WantStatus == codec.StatusSuccess:
		if err := module.Validate(spec.Module, moduleLimits()); err != nil {
			t.Fatalf("%s: Validate: %v", spec.ID, err)
		}
		instrumented, err := module.Instrument(spec.Module, moduleLimits())
		if err != nil {
			t.Fatalf("%s: Instrument: %v", spec.ID, err)
		}
		instrumentedCodeHash = CodeHash(instrumented)
		haveInstrumented = true
	case spec.Shape == "validation_rejection":
		// Exactly one of module.Validate and codec.DecodeContractMetadata must fail; a rejected
		// vector must have exactly one cause.
		validateErr := module.Validate(spec.Module, moduleLimits())
		_, metadataErr := codec.DecodeContractMetadata(spec.PendingMetadata)
		failures := 0
		if validateErr != nil {
			failures++
		}
		if metadataErr != nil {
			failures++
		}
		if failures != 1 {
			t.Fatalf("%s: validation_rejection: got %d failure causes (Validate=%v, DecodeContractMetadata=%v), want exactly 1", spec.ID, failures, validateErr, metadataErr)
		}
	case kind == codec.Upgrade && spec.WantStatus != codec.StatusSuccess:
		if err := module.Validate(spec.Module, moduleLimits()); err != nil {
			t.Fatalf("%s: Validate: %v, want nil", spec.ID, err)
		}
	}

	store, err := state.Open(filepath.Join(t.TempDir(), "state.log"))
	if err != nil {
		t.Fatalf("%s: state.Open: %v", spec.ID, err)
	}
	t.Cleanup(func() { _ = store.Close() })

	var seedDiff []domain.StateDiffEntry
	preState := map[string]string{}
	for path, value := range spec.ExtraPreState {
		seedDiff = append(seedDiff, domain.StateDiffEntry{Path: path, Value: value})
		preState[hex.EncodeToString(path[:])] = hex.EncodeToString(value)
	}

	preStateRoot, err := store.Apply(seedDiff)
	if err != nil {
		t.Fatalf("%s: seed store.Apply: %v", spec.ID, err)
	}
	acc := store.Accessor()

	canonical, err := codec.Input{SourceAccountBlockHash: spec.SourceAccountBlockHash, Args: spec.Args}.Encode()
	if err != nil {
		t.Fatalf("%s: encode canonical input: %v", spec.ID, err)
	}
	inputHash := hashOf(crypto.Hash(canonical))

	in, err := p.DecodeInput(spec.Ctx, canonical)
	if err != nil {
		t.Fatalf("%s: DecodeInput: %v", spec.ID, err)
	}
	decoded, ok := in.(Input)
	if !ok {
		t.Fatalf("%s: DecodeInput returned %T, want wasm.Input", spec.ID, in)
	}
	if decoded.Unsupported {
		t.Fatalf("%s: DecodeInput: Unsupported = true, want false", spec.ID)
	}

	eff, err := p.Apply(acc, decoded)
	if err != nil {
		t.Fatalf("%s: Apply: %v", spec.ID, err)
	}
	if eff.Status != spec.WantStatus {
		t.Fatalf("%s: Status = %v, want %v", spec.ID, eff.Status, spec.WantStatus)
	}
	if eff.GasUsed != 0 {
		t.Fatalf("%s: eff.GasUsed = %d, want 0", spec.ID, eff.GasUsed)
	}

	postStateRoot, err := store.Apply(eff.StateDiff)
	if err != nil {
		t.Fatalf("%s: post store.Apply: %v", spec.ID, err)
	}

	ref := refDerive(t, eff, acc, spec.Ctx.GlobalInputIndex, spec.BatchID, preStateRoot, postStateRoot, inputHash)

	witnessKeys := make([]string, len(ref.WitnessKeys))
	for i, path := range ref.WitnessKeys {
		witnessKeys[i] = hex.EncodeToString(path[:])
	}

	vj := vectorJSON{
		ID:                     spec.ID,
		Purpose:                spec.Purpose,
		Shape:                  spec.Shape,
		BatchID:                spec.BatchID,
		GlobalInputIndex:       spec.Ctx.GlobalInputIndex,
		MomentumHeight:         spec.Ctx.MomentumHeight,
		MomentumTime:           spec.Ctx.MomentumTime,
		L1Sender:               hex.EncodeToString(spec.Ctx.L1Sender[:]),
		SourceAccountBlockHash: hex.EncodeToString(spec.SourceAccountBlockHash[:]),
		InputKind:              uint8(spec.Args.Kind()),
		Input:                  inputView(spec.Args),
		CanonicalInputHex:      hex.EncodeToString(canonical),
		SourceDataHex:          hex.EncodeToString(spec.Ctx.SourceData),
		WithdrawalCaps:         capsHex(nil),
		PreState:               preState,
		PreStateRoot:           hex.EncodeToString(preStateRoot[:]),
		WitnessKeys:            witnessKeys,
		Status:                 eff.Status.String(),
		GasUsed:                eff.GasUsed,
		ExecutionResultBlobHex: hex.EncodeToString(ref.ExecutionResultBlob),
		PostStateRoot:          hex.EncodeToString(postStateRoot[:]),
		Receipt:                toVectorReceipt(ref.Receipt),
		ReceiptHex:             hex.EncodeToString(ref.ReceiptBlob),
	}
	if haveModule {
		vj.ModuleWasmHex = hex.EncodeToString(spec.Module)
		vj.CodeHash = hex.EncodeToString(codeHash[:])
	}
	if haveInstrumented {
		vj.InstrumentedCodeHash = hex.EncodeToString(instrumentedCodeHash[:])
	}
	if kind == codec.DeployFinalize || kind == codec.Upgrade {
		metadataHex := hex.EncodeToString(spec.PendingMetadata)
		vj.PendingMetadataHex = strp(metadataHex)
	}

	return vj
}

// assertUniqueIdentities fails the test if any two specs in the full artifact share an ID, a
// SourceAccountBlockHash or a (BatchID, GlobalInputIndex) pair. Ticket-local identity values
// (see newIdentity in callvectors_test.go) are append-only: a spec added by a later ticket must
// take the next unused value in each sequence rather than reusing or renumbering one already in
// this list, and this check is what catches it if it doesn't.
func assertUniqueIdentities(t *testing.T, specs []vectorSpec) {
	t.Helper()

	byID := make(map[string]string)
	bySrc := make(map[types.Hash]string)
	type batchInput struct {
		batchID uint64
		idx     uint64
	}
	byBatchInput := make(map[batchInput]string)

	for _, spec := range specs {
		if other, ok := byID[spec.ID]; ok {
			t.Fatalf("duplicate vector ID %q used by both %q and %q", spec.ID, other, spec.ID)
		}
		byID[spec.ID] = spec.ID

		if other, ok := bySrc[spec.SourceAccountBlockHash]; ok {
			t.Fatalf("duplicate SourceAccountBlockHash %x shared by %q and %q", spec.SourceAccountBlockHash, other, spec.ID)
		}
		bySrc[spec.SourceAccountBlockHash] = spec.ID

		key := batchInput{batchID: spec.BatchID, idx: spec.Ctx.GlobalInputIndex}
		if other, ok := byBatchInput[key]; ok {
			t.Fatalf("duplicate (batch_id, global_input_index) %+v shared by %q and %q", key, other, spec.ID)
		}
		byBatchInput[key] = spec.ID
	}
}

// TestGenerateVectors rebuilds the whole artifact by running every vector end to end and
// compares it against the checked-in file, writing the file instead when -update is set.
func TestGenerateVectors(t *testing.T) {
	chainID := hashOf(crypto.Hash([]byte("ZENON_WASM_L2:"), []byte("testnet")))

	specs := spineVectors(t)
	specs = append(specs, callVectors(t)...)
	deploySpecs := deployVectors(t)
	unsupported := unsupportedVector()
	assertUniqueIdentities(t, append(append(append([]vectorSpec{}, specs...), deploySpecs...), unsupported))

	vectors := make([]vectorJSON, 0, len(specs)+len(deploySpecs)+1)
	for _, spec := range specs {
		vectors = append(vectors, generateVector(t, spec, chainID))
	}
	for _, spec := range deploySpecs {
		vectors = append(vectors, generateDeployVector(t, spec))
	}
	vectors = append(vectors, generateUnsupportedVector(t, unsupported, 8, chainID))

	assertChainedRoots(t, vectors, [][2]string{
		{"EXEC-UPGRADE-2", "EXEC-DEPLOY-2"},
		{"EXEC-TRANSFER-1", "EXEC-UPGRADE-2"},
		{"EXEC-TRANSFER-2", "EXEC-TRANSFER-1"},
		{"CV-UPGRADE-RENOUNCED-1", "EXEC-TRANSFER-2"},
		{"CV-TRANSFER-UNAUTH-1", "EXEC-UPGRADE-2"},
	})

	file := vectorFile{
		Artifact:               "execution-conformance-v1",
		ExecutionResultVersion: codec.ExecutionResultVersion,
		ReceiptVersion:         codec.ReceiptVersion,
		ContextVersion:         codec.RuntimeProfileVersion,
		MeteringVersion:        codec.MeteringVersion,
		Hash:                   "SHA3-256 (FIPS-202)",
		NetworkName:            "testnet",
		ChainID:                hex.EncodeToString(chainID[:]),
		Notes: []string{
			"gas_used is a function of metering_version and of the wazero version pinned in go.mod, which this file does not record.",
		},
		Vectors: vectors,
	}

	got, err := json.MarshalIndent(file, "", "  ")
	if err != nil {
		t.Fatalf("MarshalIndent: %v", err)
	}
	got = append(got, '\n')

	if *updateVectors {
		if err := os.WriteFile(vectorFilePath, got, 0o644); err != nil {
			t.Fatalf("WriteFile(%s): %v", vectorFilePath, err)
		}
	}

	want, err := os.ReadFile(vectorFilePath)
	if err != nil {
		t.Fatalf("ReadFile(%s): %v", vectorFilePath, err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("generated artifact does not match %s; run with -update to regenerate it", vectorFilePath)
	}
}

// assertChainedRoots asserts, for each (y, x) pair, that vector y's pre_state_root equals
// vector x's post_state_root: y's ExtraPreState was hand-copied from x's post-state, and this
// is the exact check that the copy is right, exact because every vector named here starts
// generateDeployVector's store from empty.
func assertChainedRoots(t *testing.T, vectors []vectorJSON, pairs [][2]string) {
	t.Helper()
	byID := make(map[string]vectorJSON, len(vectors))
	for _, v := range vectors {
		byID[v.ID] = v
	}
	for _, pair := range pairs {
		y, ok := byID[pair[0]]
		if !ok {
			t.Fatalf("assertChainedRoots: vector %q not found", pair[0])
		}
		x, ok := byID[pair[1]]
		if !ok {
			t.Fatalf("assertChainedRoots: vector %q not found", pair[1])
		}
		if y.PreStateRoot != x.PostStateRoot {
			t.Fatalf("%s.pre_state_root = %s, want %s.post_state_root = %s", y.ID, y.PreStateRoot, x.ID, x.PostStateRoot)
		}
	}
}

// --- the tripwire.

// vectorFileDigest is the hand-recorded SHA-256 of the whole checked-in artifact. -update can
// rewrite the JSON but cannot rewrite this literal. Digesting the whole file, not just the
// module bytes, is what stops a hand-edited gas_used or receipt_hex being laundered through a
// regeneration. Stdlib SHA-256, not the protocol's SHA3-256: this is a repository integrity
// check, not a consensus value.
const vectorFileDigest = "e5344622a57f2dfdc281b8bdbae4656fd5d041b46e0bbbde835480e4576768c2"

// TestVectorFileDigest fails loudly if the artifact's bytes changed.
func TestVectorFileDigest(t *testing.T) {
	data, err := os.ReadFile(vectorFilePath)
	if err != nil {
		t.Fatalf("ReadFile(%s): %v", vectorFilePath, err)
	}
	sum := sha256.Sum256(data)
	got := hex.EncodeToString(sum[:])
	if got != vectorFileDigest {
		t.Fatalf("%s changed: got SHA-256 %s, want %s. A changed conformance artifact is a consensus change and must be reviewed; if the change is intentional, update the vectorFileDigest literal in vectors_test.go to %s.",
			vectorFilePath, got, vectorFileDigest, got)
	}
}
