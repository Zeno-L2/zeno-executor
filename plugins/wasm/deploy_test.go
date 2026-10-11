package wasm

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/zenon-network/go-zenon/common/crypto"
	"github.com/zenon-network/go-zenon/common/types"

	"github.com/digitalSloth/go-executor/domain"
	"github.com/digitalSloth/go-executor/internal/codec"
	"github.com/digitalSloth/go-executor/plugins/wasm/internal/module"
)

// --- seeding helpers: drive kinds 2 and 3 through DecodeInput with real canonical payloads, so
// the 04a caching path is exercised end to end.

func decodeStart(t *testing.T, p *Plugin, args codec.DeployStartArgs, ctx domain.InputContext) {
	t.Helper()
	if _, err := p.DecodeInput(ctx, encodeInput(t, args)); err != nil {
		t.Fatalf("DecodeInput(DeployStart): %v", err)
	}
}

func decodeChunk(t *testing.T, p *Plugin, sender types.Address, codeHash types.Hash, idx uint32, data []byte) {
	t.Helper()
	args := codec.DeployChunkArgs{CodeHash: codeHash, ChunkIndex: idx, ChunkHash: hashOf(crypto.Hash(data))}
	ctx := domain.InputContext{L1Sender: sender, SourceData: data}
	if _, err := p.DecodeInput(ctx, encodeInput(t, args)); err != nil {
		t.Fatalf("DecodeInput(DeployChunk %d): %v", idx, err)
	}
}

// splitChunks splits raw per SPEC.md:610: every chunk but the last exactly MaxDeployChunkData,
// the last the remainder.
func splitChunks(raw []byte) [][]byte {
	var chunks [][]byte
	for i := 0; i < len(raw); i += MaxDeployChunkData {
		end := i + MaxDeployChunkData
		if end > len(raw) {
			end = len(raw)
		}
		chunks = append(chunks, raw[i:end])
	}
	return chunks
}

// immutableMetadata is the 23-byte version-1 IMMUTABLE, zero-owner ContractMetadata encoding.
var immutableMetadata = codec.ContractMetadata{Version: codec.ContractMetadataVersion, UpgradePolicy: codec.PolicyImmutable}.Encode()

// seedDeployment drives a complete DeployStart + DeployChunk* sequence for raw through
// DecodeInput, returning the pre-instrumentation code_hash. metadata is passed in the
// DeployStartArgs it decodes.
func seedDeployment(t *testing.T, p *Plugin, raw []byte, sender types.Address, metadata []byte) types.Hash {
	t.Helper()
	codeHash := CodeHash(raw)
	chunks := splitChunks(raw)
	decodeStart(t, p, codec.DeployStartArgs{
		CodeHash:     codeHash,
		MetadataHash: types.Hash{0x09},
		TotalSize:    uint64(len(raw)),
		ChunkCount:   uint32(len(chunks)),
		Metadata:     metadata,
	}, domain.InputContext{L1Sender: sender})
	for i, c := range chunks {
		decodeChunk(t, p, sender, codeHash, uint32(i), c)
	}
	return codeHash
}

// upgradeState builds a mapState seeding a deployed contract's keyCodeHash leaf plus an
// optionally present policy and owner.
func upgradeState(id, oldCodeHash types.Hash, policyPresent bool, policy []byte, ownerPresent bool, owner []byte) *mapState {
	values := map[domain.Path][]byte{
		derive(id, keyCodeHash): oldCodeHash[:],
	}
	if policyPresent {
		values[derive(id, keyUpgradePolicy)] = policy
	}
	if ownerPresent {
		values[derive(id, keyOwner)] = owner
	}
	return &mapState{values: values}
}

// assertUpgradeReads checks §20's fixed, branch-independent four-key read order.
func assertUpgradeReads(t *testing.T, state *mapState, id types.Hash) {
	t.Helper()
	want := []domain.Path{
		derive(id, keyCodeHash),
		derive(id, keyPreCodeHash),
		derive(id, keyUpgradePolicy),
		derive(id, keyOwner),
	}
	if !reflect.DeepEqual(state.reads, want) {
		t.Fatalf("read sequence = %v, want %v", state.reads, want)
	}
}

// buildElementModule builds a module.Validate-shaped module carrying a table of tableMin
// entries and one active element segment of one function index at elemOffset, so
// parseElements' bounds check is reachable. Sections are emitted in ascending id order:
// 1 type, 3 function, 4 table, 5 memory, 7 export, 9 element, 10 code.
func buildElementModule(tableMin, elemOffset uint32) []byte {
	var out []byte
	out = append(out, wasmHeader...)

	runType := encodeFuncType([]byte{0x7F, 0x7F}, []byte{0x7E})
	typeSec := appendVarU32(nil, 1)
	typeSec = append(typeSec, runType...)
	out = append(out, section(1, typeSec)...)

	funcSec := []byte{0x01, 0x00}
	out = append(out, section(3, funcSec)...)

	tableSec := appendVarU32([]byte{0x01, 0x70, 0x00}, tableMin)
	out = append(out, section(4, tableSec)...)

	memSec := []byte{0x01, 0x00, 0x01}
	out = append(out, section(5, memSec)...)

	exportSec := []byte{0x02}
	exportSec = appendName(exportSec, "memory")
	exportSec = append(exportSec, 0x02, 0x00)
	exportSec = appendName(exportSec, "run")
	exportSec = append(exportSec, 0x00, 0x00)
	out = append(out, section(7, exportSec)...)

	elemSec := appendVarU32(nil, 1) // 1 segment
	elemSec = append(elemSec, 0x00) // flags: active, implicit table 0
	elemSec = append(elemSec, 0x41)
	elemSec = appendVarS64(elemSec, int64(elemOffset))
	elemSec = append(elemSec, 0x0B)
	elemSec = appendVarU32(elemSec, 1) // 1 function index
	elemSec = append(elemSec, 0x00)    // func 0
	out = append(out, section(9, elemSec)...)

	body := []byte{0x00, 0x42, 0x00, 0x0B}
	codeSec := []byte{0x01}
	codeSec = appendVarU32(codeSec, uint32(len(body)))
	codeSec = append(codeSec, body...)
	out = append(out, section(10, codeSec)...)

	return out
}

// --- pure-function tests: decodePendingRecord, contractID.

func TestDecodePendingRecordRoundTrip(t *testing.T) {
	h := types.Hash{1, 2, 3}
	b := encodePendingRecord(h, 12345, 3, immutableMetadata)

	rec, ok := decodePendingRecord(b)
	if !ok {
		t.Fatal("decodePendingRecord: ok = false for a well-formed record")
	}
	if rec.MetadataHash != h || rec.TotalSize != 12345 || rec.ChunkCount != 3 || !bytes.Equal(rec.Metadata, immutableMetadata) {
		t.Fatalf("decodePendingRecord = %+v, want MetadataHash=%x TotalSize=12345 ChunkCount=3 Metadata=%x", rec, h, immutableMetadata)
	}

	// A length below pendingRecordPrefixSize is rejected; exactly the prefix width (empty
	// metadata) and a length above it are accepted.
	for _, n := range []int{43} {
		if _, ok := decodePendingRecord(make([]byte, n)); ok {
			t.Fatalf("decodePendingRecord: ok = true for length %d, want false", n)
		}
	}
	for _, n := range []int{44, 45} {
		if _, ok := decodePendingRecord(make([]byte, n)); !ok {
			t.Fatalf("decodePendingRecord: ok = false for length %d, want true", n)
		}
	}
}

func TestContractIDFixedVector(t *testing.T) {
	var deployer types.Address
	for i := range deployer {
		deployer[i] = byte(i)
	}
	idx := uint64(0x0102030405060708)
	var chainID types.Hash
	for i := range chainID {
		chainID[i] = byte(i)
	}

	var beIdx [8]byte
	binary.BigEndian.PutUint64(beIdx[:], idx)
	preimage := append(append(append([]byte{}, deployer[:]...), beIdx[:]...), chainID[:]...)
	if len(preimage) != 60 {
		t.Fatalf("len(preimage) = %d, want 60", len(preimage))
	}
	want := hashOf(crypto.Hash(preimage))
	got := contractID(deployer, idx, chainID)
	if got != want {
		t.Fatalf("contractID = %x, want %x", got, want)
	}

	var leIdx [8]byte
	binary.LittleEndian.PutUint64(leIdx[:], idx)
	lePreimage := append(append(append([]byte{}, deployer[:]...), leIdx[:]...), chainID[:]...)
	leResult := hashOf(crypto.Hash(lePreimage))
	if got == leResult {
		t.Fatal("contractID matches a little-endian index encoding, want big-endian")
	}

	if contractID(deployer, idx+1, chainID) == got {
		t.Fatal("contractID unaffected by a change to globalInputIndex")
	}
	otherDeployer := deployer
	otherDeployer[0] ^= 0xFF
	if contractID(otherDeployer, idx, chainID) == got {
		t.Fatal("contractID unaffected by a change to deployer")
	}
	otherChain := chainID
	otherChain[0] ^= 0xFF
	if contractID(deployer, idx, otherChain) == got {
		t.Fatal("contractID unaffected by a change to chainID")
	}
}

// --- pending records and chunks are keyed by (deployer, code_hash).

// finalizeAs applies a DeployFinalize for codeHash from sender at input index idx.
func finalizeAs(t *testing.T, p *Plugin, sender types.Address, idx uint64, codeHash types.Hash) domain.Effects {
	t.Helper()
	ctx := domain.InputContext{GlobalInputIndex: idx, L1Sender: sender}
	eff, err := p.Apply(nil, Input{Ctx: ctx, Args: codec.DeployFinalizeArgs{CodeHash: codeHash}})
	if err != nil {
		t.Fatalf("Apply(finalize by %x): %v", sender[:], err)
	}
	if eff.Status != codec.StatusSuccess {
		t.Fatalf("Status = %v, want StatusSuccess", eff.Status)
	}
	return eff
}

func hasOwnerLeaf(eff domain.Effects) bool {
	for _, e := range eff.StateDiff {
		if e.Path == derive(eff.ContractID, keyOwner) {
			return true
		}
	}
	return false
}

func TestApplyDeployFinalizeTwoDeployersSameCodeHash(t *testing.T) {
	raw := buildModule(nil, []byte{0x00}, []byte{0x42, 0x00, 0x0B}, 1)
	chunks := splitChunks(raw)
	codeHash := CodeHash(raw)
	a := types.Address{1}
	b := types.Address{2}
	bMeta := codec.ContractMetadata{Version: codec.ContractMetadataVersion, UpgradePolicy: codec.PolicyOwnerUpgradeable, Owner: b}.Encode()

	p := newTestPlugin(t)
	start := func(sender types.Address, metadata []byte) {
		decodeStart(t, p, codec.DeployStartArgs{
			CodeHash:     codeHash,
			MetadataHash: types.Hash{0x09},
			TotalSize:    uint64(len(raw)),
			ChunkCount:   uint32(len(chunks)),
			Metadata:     metadata,
		}, domain.InputContext{L1Sender: sender})
	}

	// A and B interleave their starts and chunks for one code_hash.
	start(a, immutableMetadata)
	start(b, bMeta)
	for i, c := range chunks {
		decodeChunk(t, p, a, codeHash, uint32(i), c)
		decodeChunk(t, p, b, codeHash, uint32(i), c)
	}

	effA := finalizeAs(t, p, a, 7, codeHash)
	effB := finalizeAs(t, p, b, 8, codeHash)

	if effA.ContractID != contractID(a, 7, p.chainID) || effB.ContractID != contractID(b, 8, p.chainID) {
		t.Fatalf("ContractIDs = %x/%x, want each derived from its own sender", effA.ContractID, effB.ContractID)
	}
	if hasOwnerLeaf(effA) {
		t.Fatal("A's finalize carries a keyOwner leaf: it read B's metadata")
	}
	if !hasOwnerLeaf(effB) {
		t.Fatal("B's finalize has no keyOwner leaf: it did not read its own metadata")
	}
}

func TestDeployStartLeavesOtherDeployersEntriesIntact(t *testing.T) {
	raw := buildModule(nil, []byte{0x00}, []byte{0x42, 0x00, 0x0B}, 1)
	a := types.Address{1}
	b := types.Address{2}

	p := newTestPlugin(t)
	codeHash := seedDeployment(t, p, raw, a, immutableMetadata)
	recordBefore, ok, err := p.store.get(entryPendingRecord, pendingRecordKey(a, codeHash))
	if err != nil || !ok {
		t.Fatalf("store.get(A record) = ok %v, err %v", ok, err)
	}

	seedDeployment(t, p, raw, b, immutableMetadata)
	decodeStart(t, p, codec.DeployStartArgs{CodeHash: codeHash, MetadataHash: types.Hash{0x09}, TotalSize: uint64(len(raw)), ChunkCount: 1}, domain.InputContext{L1Sender: b})

	recordAfter, ok, err := p.store.get(entryPendingRecord, pendingRecordKey(a, codeHash))
	if err != nil || !ok {
		t.Fatalf("A's record is gone after B's start: ok %v, err %v", ok, err)
	}
	if !bytes.Equal(recordBefore, recordAfter) {
		t.Fatal("A's record changed after B's start")
	}
	if _, ok, err := p.store.get(entryDeployChunk, deployChunkKey(a, codeHash, 0)); err != nil || !ok {
		t.Fatalf("A's chunk 0 is gone after B's start: ok %v, err %v", ok, err)
	}
	if _, ok, err := p.store.get(entryDeployChunk, deployChunkKey(b, codeHash, 0)); err != nil {
		t.Fatalf("store.get: %v", err)
	} else if ok {
		t.Fatal("B's chunk 0 survived B's own start")
	}
}

func TestApplyUpgradeByOtherDeployerHalts(t *testing.T) {
	raw := buildModule(nil, []byte{0x00}, []byte{0x42, 0x00, 0x0B}, 1)
	a := types.Address{1}
	b := types.Address{2}

	p := newTestPlugin(t)
	codeHash := seedDeployment(t, p, raw, a, immutableMetadata)

	id := types.Hash{0x20}
	owner := callerHash(b)
	state := upgradeState(id, types.Hash{0xBB}, true, []byte{policyOwnerUpgradeable}, true, owner[:])
	ctx := domain.InputContext{L1Sender: b}
	_, err := p.Apply(state, Input{Ctx: ctx, Args: codec.UpgradeArgs{ContractID: id, NewCodeHash: codeHash}})
	if err == nil || !strings.Contains(err.Error(), "no pending record") {
		t.Fatalf("Apply: err = %v, want a \"no pending record\" halt", err)
	}
}

// --- kinds 2/3 through Apply.

func TestApplyDeployStartAndChunkAreSuccessNoop(t *testing.T) {
	p := newTestPlugin(t)
	cases := []codec.InputArgs{
		codec.DeployStartArgs{CodeHash: types.Hash{1}, MetadataHash: types.Hash{2}, TotalSize: 10, ChunkCount: 1},
		codec.DeployChunkArgs{CodeHash: types.Hash{1}, ChunkIndex: 0, ChunkHash: types.Hash{3}},
	}
	for _, args := range cases {
		t.Run(fmt.Sprintf("%T", args), func(t *testing.T) {
			state := &mapState{}
			eff, err := p.Apply(state, Input{Args: args})
			if err != nil {
				t.Fatalf("Apply: %v", err)
			}
			if eff.Status != codec.StatusSuccess {
				t.Fatalf("Status = %v, want StatusSuccess", eff.Status)
			}
			if eff.GasUsed != 0 {
				t.Fatalf("GasUsed = %d, want 0", eff.GasUsed)
			}
			if eff.ContractID != (types.Hash{}) || eff.CodeHash != (types.Hash{}) {
				t.Fatalf("ContractID/CodeHash = %x/%x, want zero/zero", eff.ContractID, eff.CodeHash)
			}
			if eff.StateDiff != nil || eff.Events != nil || eff.OutboxMessages != nil || eff.ReturnData != nil {
				t.Fatalf("Effects = %+v, want every collection nil", eff)
			}
			if len(state.reads) != 0 {
				t.Fatalf("reads = %v, want none", state.reads)
			}
		})
	}
}

// --- DecodeInput's kind-2 delete-then-write.

func TestDeployStartClearsPreviousAttempt(t *testing.T) {
	p := newTestPlugin(t)
	x := types.Hash{0xAB}
	sender := types.Address{1}

	chunk0 := bytes.Repeat([]byte{0x11}, MaxDeployChunkData)
	chunk1 := bytes.Repeat([]byte{0x22}, 100)
	total := uint64(len(chunk0) + len(chunk1))

	decodeStart(t, p, codec.DeployStartArgs{CodeHash: x, MetadataHash: types.Hash{9}, TotalSize: total, ChunkCount: 2}, domain.InputContext{L1Sender: sender})
	decodeChunk(t, p, sender, x, 0, chunk0)
	decodeChunk(t, p, sender, x, 1, chunk1)

	// A second DeployStart for the same code_hash, declaring a smaller attempt.
	newChunk0 := bytes.Repeat([]byte{0x33}, MaxDeployChunkData)
	decodeStart(t, p, codec.DeployStartArgs{CodeHash: x, MetadataHash: types.Hash{9}, TotalSize: uint64(MaxDeployChunkData), ChunkCount: 1}, domain.InputContext{L1Sender: sender})
	decodeChunk(t, p, sender, x, 0, newChunk0)

	if _, ok, err := p.store.get(entryDeployChunk, deployChunkKey(sender, x, 1)); err != nil {
		t.Fatalf("store.get: %v", err)
	} else if ok {
		t.Fatal("attempt one's chunk 1 is still present after a fresh DeployStart")
	}

	// A third DeployStart declaring chunk_count = 2, with no chunks supplied at all: the
	// finalize must halt on a missing chunk, never assemble attempt one's surviving bytes.
	decodeStart(t, p, codec.DeployStartArgs{CodeHash: x, MetadataHash: types.Hash{9}, TotalSize: uint64(MaxDeployChunkData) + 1, ChunkCount: 2}, domain.InputContext{L1Sender: sender})

	if _, err := p.Apply(nil, Input{Args: codec.DeployFinalizeArgs{CodeHash: x}}); err == nil {
		t.Fatal("Apply(finalize): got nil error for a chunk_count=2 record with no chunks stored, want a halt")
	}
}

// --- finalize success, single- and multi-chunk.

func TestApplyDeployFinalizeSuccess(t *testing.T) {
	cases := []struct {
		name string
		raw  []byte
	}{
		{"SingleChunk", buildModule(nil, []byte{0x00}, []byte{0x42, 0x00, 0x0B}, 1)},
		{"MultiChunk", buildModule(&dataSeg{offset: 0, bytes: make([]byte, 16000)}, []byte{0x00}, []byte{0x42, 0x00, 0x0B}, 1)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			p := newTestPlugin(t)
			sender := types.Address{1, 2, 3}
			codeHash := seedDeployment(t, p, tc.raw, sender, immutableMetadata)

			ctx := domain.InputContext{GlobalInputIndex: 42, L1Sender: sender}
			eff, err := p.Apply(nil, Input{Ctx: ctx, Args: codec.DeployFinalizeArgs{CodeHash: codeHash}})
			if err != nil {
				t.Fatalf("Apply: %v", err)
			}
			if eff.Status != codec.StatusSuccess {
				t.Fatalf("Status = %v, want StatusSuccess", eff.Status)
			}
			if eff.GasUsed != 0 {
				t.Fatalf("GasUsed = %d, want 0", eff.GasUsed)
			}
			wantID := contractID(sender, ctx.GlobalInputIndex, p.chainID)
			if eff.ContractID != wantID {
				t.Fatalf("ContractID = %x, want %x", eff.ContractID, wantID)
			}

			instrumented, err := module.Instrument(tc.raw, moduleLimits())
			if err != nil {
				t.Fatalf("Instrument: %v", err)
			}
			wantCodeHash := CodeHash(instrumented)
			if eff.CodeHash != wantCodeHash {
				t.Fatalf("CodeHash = %x, want %x", eff.CodeHash, wantCodeHash)
			}

			wantReturn := append(append([]byte{}, codeHash[:]...), wantCodeHash[:]...)
			if len(eff.ReturnData) != 64 || !bytes.Equal(eff.ReturnData, wantReturn) {
				t.Fatalf("ReturnData = %x, want %x", eff.ReturnData, wantReturn)
			}

			wantDiff := sortStateDiff([]domain.StateDiffEntry{
				{Path: derive(wantID, keyCodeHash), Value: wantCodeHash[:]},
				{Path: derive(wantID, keyPreCodeHash), Value: codeHash[:]},
				{Path: derive(wantID, keyUpgradePolicy), Value: []byte{policyImmutable}},
			})
			if !reflect.DeepEqual(eff.StateDiff, wantDiff) {
				t.Fatalf("StateDiff = %+v, want %+v", eff.StateDiff, wantDiff)
			}
			if eff.Events != nil || eff.OutboxMessages != nil {
				t.Fatalf("Events/OutboxMessages = %+v/%+v, want nil/nil", eff.Events, eff.OutboxMessages)
			}

			gotAssembled, ok, err := p.store.get(entryAssembledModule, codeHash[:])
			if err != nil || !ok {
				t.Fatalf("store.get(assembled): ok=%v err=%v", ok, err)
			}
			if CodeHash(gotAssembled) != codeHash {
				t.Fatal("stored assembled bytes do not hash to codeHash")
			}
			gotInstrumented, ok, err := p.store.get(entryInstrumentedModule, wantCodeHash[:])
			if err != nil || !ok {
				t.Fatalf("store.get(instrumented): ok=%v err=%v", ok, err)
			}
			if CodeHash(gotInstrumented) != wantCodeHash {
				t.Fatal("stored instrumented bytes do not hash to wantCodeHash")
			}
		})
	}
}

// --- finalize metadata: OWNER_UPGRADEABLE writes the owner leaf; malformed metadata is
// VALIDATION_FAILED.

func TestApplyDeployFinalizeOwnerUpgradeableWritesOwnerLeaf(t *testing.T) {
	p := newTestPlugin(t)
	sender := types.Address{1, 2, 3}
	raw := buildModule(nil, []byte{0x00}, []byte{0x42, 0x00, 0x0B}, 1)
	owner := types.Address{9, 9, 9}
	metadata := codec.ContractMetadata{Version: codec.ContractMetadataVersion, UpgradePolicy: codec.PolicyOwnerUpgradeable, Owner: owner}.Encode()
	codeHash := seedDeployment(t, p, raw, sender, metadata)

	ctx := domain.InputContext{GlobalInputIndex: 42, L1Sender: sender}
	eff, err := p.Apply(nil, Input{Ctx: ctx, Args: codec.DeployFinalizeArgs{CodeHash: codeHash}})
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if eff.Status != codec.StatusSuccess {
		t.Fatalf("Status = %v, want StatusSuccess", eff.Status)
	}

	instrumented, err := module.Instrument(raw, moduleLimits())
	if err != nil {
		t.Fatalf("Instrument: %v", err)
	}
	wantCodeHash := CodeHash(instrumented)
	wantOwnerHash := callerHash(owner)
	wantDiff := sortStateDiff([]domain.StateDiffEntry{
		{Path: derive(eff.ContractID, keyCodeHash), Value: wantCodeHash[:]},
		{Path: derive(eff.ContractID, keyPreCodeHash), Value: codeHash[:]},
		{Path: derive(eff.ContractID, keyUpgradePolicy), Value: []byte{policyOwnerUpgradeable}},
		{Path: derive(eff.ContractID, keyOwner), Value: wantOwnerHash[:]},
	})
	if !reflect.DeepEqual(eff.StateDiff, wantDiff) {
		t.Fatalf("StateDiff = %+v, want %+v", eff.StateDiff, wantDiff)
	}
}

func TestApplyDeployFinalizeMalformedMetadataValidationFailed(t *testing.T) {
	cases := []struct {
		name     string
		metadata []byte
	}{
		{"UnknownVersion", codec.ContractMetadata{Version: 2, UpgradePolicy: codec.PolicyImmutable}.Encode()},
		{"Truncated", []byte{0x00, 0x01}},
		{"TrailingBytes", append(append([]byte{}, immutableMetadata...), 0x00)},
		{"InvalidPolicy", codec.ContractMetadata{Version: codec.ContractMetadataVersion, UpgradePolicy: 2}.Encode()},
		{"ImmutableWithOwner", codec.ContractMetadata{Version: codec.ContractMetadataVersion, UpgradePolicy: codec.PolicyImmutable, Owner: types.Address{1}}.Encode()},
		{"OwnerUpgradeableZeroOwner", codec.ContractMetadata{Version: codec.ContractMetadataVersion, UpgradePolicy: codec.PolicyOwnerUpgradeable}.Encode()},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			p := newTestPlugin(t)
			sender := types.Address{1, 2, 3}
			raw := buildModule(nil, []byte{0x00}, []byte{0x42, 0x00, 0x0B}, 1)
			codeHash := seedDeployment(t, p, raw, sender, tc.metadata)

			eff, err := p.Apply(nil, Input{Ctx: domain.InputContext{L1Sender: sender}, Args: codec.DeployFinalizeArgs{CodeHash: codeHash}})
			if err != nil {
				t.Fatalf("Apply: %v", err)
			}
			if eff.Status != codec.StatusValidationFailed {
				t.Fatalf("Status = %v, want StatusValidationFailed", eff.Status)
			}
			if eff.ContractID != (types.Hash{}) || eff.CodeHash != (types.Hash{}) {
				t.Fatalf("ContractID/CodeHash = %x/%x, want zero/zero", eff.ContractID, eff.CodeHash)
			}
			if len(eff.StateDiff) != 0 {
				t.Fatalf("StateDiff = %+v, want empty", eff.StateDiff)
			}
		})
	}
}

// --- record bounds, checked before any chunk lookup.

func TestApplyDeployFinalizeRecordBounds(t *testing.T) {
	cases := []struct {
		name       string
		chunkCount uint32
		totalSize  uint64
	}{
		{"ZeroChunkCount", 0, 100},
		{"ChunkCountTooLarge", MaxChunkCount + 1, uint64(MaxChunkCount+1) * MaxDeployChunkData},
		{"ZeroTotalSize", 1, 0},
		{"TotalSizeTooLarge", 1, MaxCodeSize + 1},
		{"ChunkCountNotCeiling", 2, 100},
		{"AbsurdChunkCount", 4_000_000_000, 100},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			p := newTestPlugin(t)
			codeHash := types.Hash{0x77}
			decodeStart(t, p, codec.DeployStartArgs{CodeHash: codeHash, MetadataHash: types.Hash{1}, TotalSize: tc.totalSize, ChunkCount: tc.chunkCount}, domain.InputContext{})

			eff, err := p.Apply(nil, Input{Args: codec.DeployFinalizeArgs{CodeHash: codeHash}})
			if err != nil {
				t.Fatalf("Apply: got error %v, want nil (an absurd record is VALIDATION_FAILED, not a halt)", err)
			}
			if eff.Status != codec.StatusValidationFailed {
				t.Fatalf("Status = %v, want StatusValidationFailed", eff.Status)
			}
			if eff.GasUsed != 0 {
				t.Fatalf("GasUsed = %d, want 0", eff.GasUsed)
			}
			if eff.ContractID != (types.Hash{}) || eff.CodeHash != (types.Hash{}) {
				t.Fatalf("ContractID/CodeHash = %x/%x, want zero/zero", eff.ContractID, eff.CodeHash)
			}
			if len(eff.StateDiff) != 0 {
				t.Fatalf("StateDiff = %+v, want empty", eff.StateDiff)
			}

			if _, ok, err := p.store.get(entryDeployChunk, deployChunkKey(types.Address{}, codeHash, 0)); err != nil {
				t.Fatalf("store.get: %v", err)
			} else if ok {
				t.Fatal("chunk 0 present though the bound check should have rejected the record before any lookup")
			}
		})
	}
}

// --- bounds check precedes any chunk-store access, proven by breaking chunk reads and
// asserting the record-bounds status still comes back rather than a store-fault halt.

func TestApplyDeployFinalizeAbsurdChunkCountNeverTouchesChunkStore(t *testing.T) {
	root := t.TempDir()
	p, err := New(Config{NetworkName: "testnet", CodeStorePath: root})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = p.Close() })
	codeHash := types.Hash{0x99}
	decodeStart(t, p, codec.DeployStartArgs{CodeHash: codeHash, MetadataHash: types.Hash{1}, TotalSize: 100, ChunkCount: 4_000_000_000}, domain.InputContext{})

	// Make any chunk read fail with a genuine I/O error rather than absence, so if
	// prepareModule reached the chunk-lookup loop at all, the finalize would halt instead of
	// returning VALIDATION_FAILED.
	dir := filepath.Join(root, entryDeployChunk.dirName())
	if err := os.Chmod(dir, 0o000); err != nil {
		t.Fatalf("chmod: %v", err)
	}
	t.Cleanup(func() { os.Chmod(dir, 0o755) })

	eff, err := p.Apply(nil, Input{Args: codec.DeployFinalizeArgs{CodeHash: codeHash}})
	if err != nil {
		t.Fatalf("Apply: got error %v, want nil (the bound must reject before any chunk-store access, so a broken chunk directory is never reached)", err)
	}
	if eff.Status != codec.StatusValidationFailed {
		t.Fatalf("Status = %v, want StatusValidationFailed", eff.Status)
	}
}

// --- assembly verification.

func TestApplyDeployFinalizeAssemblyVerification(t *testing.T) {
	t.Run("LengthMismatch", func(t *testing.T) {
		p := newTestPlugin(t)
		codeHash := types.Hash{0x55}
		data := []byte("hello world, this is some chunk data")
		decodeStart(t, p, codec.DeployStartArgs{CodeHash: codeHash, MetadataHash: types.Hash{1}, TotalSize: uint64(len(data)) + 1, ChunkCount: 1}, domain.InputContext{})
		decodeChunk(t, p, types.Address{}, codeHash, 0, data)

		eff, err := p.Apply(nil, Input{Args: codec.DeployFinalizeArgs{CodeHash: codeHash}})
		if err != nil {
			t.Fatalf("Apply: %v", err)
		}
		if eff.Status != codec.StatusValidationFailed {
			t.Fatalf("Status = %v, want StatusValidationFailed", eff.Status)
		}
		if len(eff.StateDiff) != 0 {
			t.Fatalf("StateDiff = %+v, want empty", eff.StateDiff)
		}
		if _, ok, _ := p.store.get(entryAssembledModule, codeHash[:]); ok {
			t.Fatal("assembled tier written despite a length mismatch")
		}
	})

	t.Run("HashMismatch", func(t *testing.T) {
		p := newTestPlugin(t)
		declaredCodeHash := types.Hash{0x66}
		data := []byte("some assembled module bytes that do not hash to declaredCodeHash")
		decodeStart(t, p, codec.DeployStartArgs{CodeHash: declaredCodeHash, MetadataHash: types.Hash{1}, TotalSize: uint64(len(data)), ChunkCount: 1}, domain.InputContext{})
		decodeChunk(t, p, types.Address{}, declaredCodeHash, 0, data)

		eff, err := p.Apply(nil, Input{Args: codec.DeployFinalizeArgs{CodeHash: declaredCodeHash}})
		if err != nil {
			t.Fatalf("Apply: %v", err)
		}
		if eff.Status != codec.StatusValidationFailed {
			t.Fatalf("Status = %v, want StatusValidationFailed", eff.Status)
		}
		if len(eff.StateDiff) != 0 {
			t.Fatalf("StateDiff = %+v, want empty", eff.StateDiff)
		}
		if _, ok, _ := p.store.get(entryAssembledModule, declaredCodeHash[:]); ok {
			t.Fatal("assembled tier written despite a code_hash mismatch")
		}
	})
}

// --- module rejection: data- and element-segment bounds, and the engine's own type checking.

func TestApplyDeployFinalizeModuleRejection(t *testing.T) {
	oobData := buildModule(&dataSeg{offset: 65530, bytes: make([]byte, 16)}, []byte{0x00}, []byte{0x42, 0x00, 0x0B}, 1)
	illTyped := buildModule(nil, []byte{0x00}, []byte{0x41, 0x00, 0x0B}, 1)
	oneParamAbort := runModuleSpec([]string{"abort"}, 1, []byte{0x00}, []byte{0x42, 0x00, 0x0B}, nil)
	oneParamAbort.Types[1].Params = []byte{0x7F}

	cases := []struct {
		name        string
		raw         []byte
		wantSuccess bool
	}{
		{"DataSegmentOutOfBounds", oobData, false},
		{"ElementSegmentOutOfBounds", buildElementModule(1, 1), false},
		{"ElementSegmentInBounds", buildElementModule(2, 0), true},
		{"EngineRejectedIllTyped", illTyped, false},
		{"OneParameterAbortImport", buildWasmModule(oneParamAbort), false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			p := newTestPlugin(t)
			sender := types.Address{1}
			codeHash := seedDeployment(t, p, tc.raw, sender, immutableMetadata)

			eff, err := p.Apply(nil, Input{Ctx: domain.InputContext{L1Sender: sender}, Args: codec.DeployFinalizeArgs{CodeHash: codeHash}})
			if err != nil {
				t.Fatalf("Apply: %v", err)
			}
			if tc.wantSuccess {
				if eff.Status != codec.StatusSuccess {
					t.Fatalf("Status = %v, want StatusSuccess", eff.Status)
				}
				return
			}
			if eff.Status != codec.StatusValidationFailed {
				t.Fatalf("Status = %v, want StatusValidationFailed", eff.Status)
			}
			if len(eff.StateDiff) != 0 {
				t.Fatalf("StateDiff = %+v, want empty", eff.StateDiff)
			}
			if _, ok, _ := p.store.get(entryAssembledModule, codeHash[:]); ok {
				t.Fatal("assembled tier written for a rejected module")
			}
		})
	}
}

// --- halts: absent record, malformed record, missing chunk.

func TestApplyDeployFinalizeHalts(t *testing.T) {
	t.Run("NoPendingRecord", func(t *testing.T) {
		p := newTestPlugin(t)
		codeHash := types.Hash{0x11}
		if _, err := p.Apply(nil, Input{Args: codec.DeployFinalizeArgs{CodeHash: codeHash}}); err == nil {
			t.Fatal("Apply: got nil error for a missing pending record, want a halt")
		}
	})

	t.Run("PendingRecordWrongLength", func(t *testing.T) {
		p := newTestPlugin(t)
		codeHash := types.Hash{0x22}
		if err := p.store.put(entryPendingRecord, pendingRecordKey(types.Address{}, codeHash), make([]byte, 8)); err != nil {
			t.Fatalf("store.put: %v", err)
		}
		if _, err := p.Apply(nil, Input{Args: codec.DeployFinalizeArgs{CodeHash: codeHash}}); err == nil {
			t.Fatal("Apply: got nil error for an 8-byte pending record, want a halt")
		}
	})

	t.Run("MissingChunk", func(t *testing.T) {
		p := newTestPlugin(t)
		codeHash := types.Hash{0x33}
		decodeStart(t, p, codec.DeployStartArgs{CodeHash: codeHash, MetadataHash: types.Hash{1}, TotalSize: uint64(MaxDeployChunkData) + 1, ChunkCount: 2}, domain.InputContext{})
		decodeChunk(t, p, types.Address{}, codeHash, 0, make([]byte, MaxDeployChunkData))
		if _, err := p.Apply(nil, Input{Args: codec.DeployFinalizeArgs{CodeHash: codeHash}}); err == nil {
			t.Fatal("Apply: got nil error for chunk_count=2 with only chunk 0 stored, want a halt")
		}
	})
}

// --- store fault halts.

func TestApplyDeployFinalizeStoreFaultHalts(t *testing.T) {
	t.Run("ReadFault", func(t *testing.T) {
		if os.Geteuid() == 0 {
			t.Skip("root bypasses directory permissions")
		}
		root := t.TempDir()
		p, err := New(Config{NetworkName: "testnet", CodeStorePath: root})
		if err != nil {
			t.Fatalf("New: %v", err)
		}
		t.Cleanup(func() { _ = p.Close() })
		sender := types.Address{1}
		raw := buildModule(nil, []byte{0x00}, []byte{0x42, 0x00, 0x0B}, 1)
		codeHash := seedDeployment(t, p, raw, sender, immutableMetadata)

		dir := filepath.Join(root, entryPendingRecord.dirName())
		if err := os.Chmod(dir, 0o000); err != nil {
			t.Fatalf("chmod: %v", err)
		}
		t.Cleanup(func() { os.Chmod(dir, 0o755) })

		if _, err := p.Apply(nil, Input{Ctx: domain.InputContext{L1Sender: sender}, Args: codec.DeployFinalizeArgs{CodeHash: codeHash}}); err == nil {
			t.Fatal("Apply: got nil error when the pending record cannot be read, want a halt")
		}
	})

	t.Run("WriteFault", func(t *testing.T) {
		if os.Geteuid() == 0 {
			t.Skip("root bypasses directory permissions")
		}
		root := t.TempDir()
		p, err := New(Config{NetworkName: "testnet", CodeStorePath: root})
		if err != nil {
			t.Fatalf("New: %v", err)
		}
		t.Cleanup(func() { _ = p.Close() })
		sender := types.Address{1}
		raw := buildModule(nil, []byte{0x00}, []byte{0x42, 0x00, 0x0B}, 1)
		codeHash := seedDeployment(t, p, raw, sender, immutableMetadata)

		dir := filepath.Join(root, entryAssembledModule.dirName())
		if err := os.Chmod(dir, 0o500); err != nil {
			t.Fatalf("chmod: %v", err)
		}
		t.Cleanup(func() { os.Chmod(dir, 0o755) })

		if _, err := p.Apply(nil, Input{Ctx: domain.InputContext{L1Sender: sender}, Args: codec.DeployFinalizeArgs{CodeHash: codeHash}}); err == nil {
			t.Fatal("Apply: got nil error when the assembled tier cannot be written, want a halt")
		}
	})
}

// --- deploy, buy, call: the reserved-key paths and the two code-store keys agree across the
// lifecycle, and a deployed contract's first state write needs purchased quota.

func TestDeployThenCall(t *testing.T) {
	p := newTestPlugin(t)
	sender := types.Address{9, 9, 9}
	callGlobalIndex := uint64(6)

	decoded := codec.ContractEffects{
		Version:          codec.ExecutionResultVersion,
		GlobalInputIndex: callGlobalIndex,
		StateDiff:        []codec.LocalStateDiffEntry{{LocalKey: []byte("k"), Value: []byte("v")}},
		ReturnData:       []byte("ok"),
		ClaimedDeposit:   codec.U256{},
	}
	blob := encodeEffects(t, decoded)

	offset := uint32(1024)
	body := appendVarS64([]byte{0x42}, (int64(offset)<<32)|int64(len(blob)))
	body = append(body, 0x0B)
	raw := buildModule(&dataSeg{offset: offset, bytes: blob}, []byte{0x00}, body, 1)

	codeHash := seedDeployment(t, p, raw, sender, immutableMetadata)

	finalizeCtx := domain.InputContext{GlobalInputIndex: 5, L1Sender: sender}
	eff, err := p.Apply(nil, Input{Ctx: finalizeCtx, Args: codec.DeployFinalizeArgs{CodeHash: codeHash}})
	if err != nil {
		t.Fatalf("Apply(finalize): %v", err)
	}
	if eff.Status != codec.StatusSuccess {
		t.Fatalf("finalize Status = %v, want StatusSuccess", eff.Status)
	}

	state := &mapState{values: map[domain.Path][]byte{}}
	for _, e := range eff.StateDiff {
		state.values[e.Path] = e.Value
	}

	buyEff, err := p.Apply(state, Input{Ctx: domain.InputContext{GlobalInputIndex: 5}, Args: codec.BuyStorageArgs{ContractID: eff.ContractID, Bytes: 4096}})
	if err != nil {
		t.Fatalf("Apply(buy): %v", err)
	}
	if buyEff.Status != codec.StatusSuccess {
		t.Fatalf("buy Status = %v, want StatusSuccess", buyEff.Status)
	}
	for _, e := range buyEff.StateDiff {
		state.values[e.Path] = e.Value
	}

	callCtx := domain.InputContext{GlobalInputIndex: callGlobalIndex, L1Sender: sender}
	callArgs := codec.CallL2Args{TargetContract: eff.ContractID, PlasmaLimit: 10_000_000, DepositAsset: codec.AssetID{}, DepositAmount: codec.U256{}}
	callEff, err := p.Apply(state, Input{Ctx: callCtx, Args: callArgs})
	if err != nil {
		t.Fatalf("Apply(call): %v", err)
	}
	if callEff.Status != codec.StatusSuccess {
		t.Fatalf("call Status = %v, want StatusSuccess", callEff.Status)
	}
}

// --- upgrade: statuses.

func TestApplyUpgradeStatuses(t *testing.T) {
	id := types.Hash{0x10}
	sender := types.Address{1, 2, 3}
	oldCodeHash := types.Hash{0xAA}
	owner := callerHash(sender)

	cases := []struct {
		name          string
		policyPresent bool
		policy        []byte
		ownerPresent  bool
		owner         []byte
	}{
		{"ImmutablePolicy", true, []byte{policyImmutable}, false, nil},
		{"WrongOwner", true, []byte{policyOwnerUpgradeable}, true, []byte{0xFF}},
		{"NoOwnerLeaf", true, []byte{policyOwnerUpgradeable}, false, nil},
		{"NoPolicyLeaf", false, nil, true, owner[:]},
		{"ReservedFuturePolicy", true, []byte{2}, true, owner[:]},
		{"WrongWidthPolicy", true, []byte{policyOwnerUpgradeable, 0x00}, true, owner[:]},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			p := newTestPlugin(t)
			state := upgradeState(id, oldCodeHash, tc.policyPresent, tc.policy, tc.ownerPresent, tc.owner)
			ctx := domain.InputContext{L1Sender: sender}
			eff, err := p.Apply(state, Input{Ctx: ctx, Args: codec.UpgradeArgs{ContractID: id, NewCodeHash: types.Hash{0x99}}})
			if err != nil {
				t.Fatalf("Apply: %v", err)
			}
			if eff.Status != codec.StatusUnauthorized {
				t.Fatalf("Status = %v, want StatusUnauthorized", eff.Status)
			}
			if eff.ContractID != id {
				t.Fatalf("ContractID = %x, want %x", eff.ContractID, id)
			}
			if eff.CodeHash != oldCodeHash {
				t.Fatalf("CodeHash = %x, want %x (unchanged)", eff.CodeHash, oldCodeHash)
			}
			if len(eff.StateDiff) != 0 {
				t.Fatalf("StateDiff = %+v, want empty", eff.StateDiff)
			}
			assertUpgradeReads(t, state, id)
		})
	}
}

func TestApplyUpgradeNotDeployed(t *testing.T) {
	p := newTestPlugin(t)
	id := types.Hash{0x50}
	sender := types.Address{1}
	state := &mapState{values: map[domain.Path][]byte{}}
	ctx := domain.InputContext{L1Sender: sender}

	eff, err := p.Apply(state, Input{Ctx: ctx, Args: codec.UpgradeArgs{ContractID: id, NewCodeHash: types.Hash{0x99}}})
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if eff.Status != codec.StatusValidationFailed {
		t.Fatalf("Status = %v, want StatusValidationFailed", eff.Status)
	}
	if eff.ContractID != id {
		t.Fatalf("ContractID = %x, want %x", eff.ContractID, id)
	}
	if eff.CodeHash != (types.Hash{}) {
		t.Fatalf("CodeHash = %x, want zero", eff.CodeHash)
	}
	if len(eff.StateDiff) != 0 {
		t.Fatalf("StateDiff = %+v, want empty", eff.StateDiff)
	}
	assertUpgradeReads(t, state, id)
}

func TestApplyUpgradeSuccess(t *testing.T) {
	p := newTestPlugin(t)
	id := types.Hash{0x20}
	sender := types.Address{4, 5, 6}
	oldCodeHash := types.Hash{0xBB}
	owner := callerHash(sender)

	raw := buildModule(nil, []byte{0x00}, []byte{0x42, 0x00, 0x0B}, 1)
	newPreCodeHash := seedDeployment(t, p, raw, sender, immutableMetadata)

	state := upgradeState(id, oldCodeHash, true, []byte{policyOwnerUpgradeable}, true, owner[:])
	contractPath := derive(id, []byte{0x01, 0x00})
	state.values[contractPath] = []byte("contract state")

	ctx := domain.InputContext{L1Sender: sender}
	eff, err := p.Apply(state, Input{Ctx: ctx, Args: codec.UpgradeArgs{ContractID: id, NewCodeHash: newPreCodeHash}})
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if eff.Status != codec.StatusSuccess {
		t.Fatalf("Status = %v, want StatusSuccess", eff.Status)
	}
	if eff.GasUsed != 0 {
		t.Fatalf("GasUsed = %d, want 0", eff.GasUsed)
	}
	if eff.ContractID != id {
		t.Fatalf("ContractID = %x, want %x (unchanged)", eff.ContractID, id)
	}

	instrumented, err := module.Instrument(raw, moduleLimits())
	if err != nil {
		t.Fatalf("Instrument: %v", err)
	}
	wantCodeHash := CodeHash(instrumented)
	if eff.CodeHash != wantCodeHash {
		t.Fatalf("CodeHash = %x, want %x", eff.CodeHash, wantCodeHash)
	}

	wantReturn := append(append([]byte{}, oldCodeHash[:]...), wantCodeHash[:]...)
	if len(eff.ReturnData) != 64 || !bytes.Equal(eff.ReturnData, wantReturn) {
		t.Fatalf("ReturnData = %x, want %x", eff.ReturnData, wantReturn)
	}
	if bytes.Equal(eff.ReturnData[:32], eff.ReturnData[32:]) {
		t.Fatal("ReturnData halves are equal, want the old and new code hash to differ")
	}

	wantDiff := sortStateDiff([]domain.StateDiffEntry{
		{Path: derive(id, keyCodeHash), Value: wantCodeHash[:]},
		{Path: derive(id, keyPreCodeHash), Value: newPreCodeHash[:]},
	})
	if !reflect.DeepEqual(eff.StateDiff, wantDiff) {
		t.Fatalf("StateDiff = %+v, want %+v", eff.StateDiff, wantDiff)
	}
	for _, e := range eff.StateDiff {
		if e.Path == derive(id, keyUpgradePolicy) || e.Path == derive(id, keyOwner) || e.Path == contractPath {
			t.Fatalf("StateDiff touches a path that must be preserved by absence: %x", e.Path)
		}
	}

	assertUpgradeReads(t, state, id)
}

func TestApplyUpgradeAssemblyVerificationFailure(t *testing.T) {
	p := newTestPlugin(t)
	id := types.Hash{0x30}
	sender := types.Address{7, 8, 9}
	oldCodeHash := types.Hash{0xCC}
	owner := callerHash(sender)

	newCodeHash := types.Hash{0x44}
	data := []byte("bytes that do not hash to newCodeHash")
	decodeStart(t, p, codec.DeployStartArgs{CodeHash: newCodeHash, MetadataHash: types.Hash{1}, TotalSize: uint64(len(data)), ChunkCount: 1}, domain.InputContext{L1Sender: sender})
	decodeChunk(t, p, sender, newCodeHash, 0, data)

	state := upgradeState(id, oldCodeHash, true, []byte{policyOwnerUpgradeable}, true, owner[:])
	ctx := domain.InputContext{L1Sender: sender}
	eff, err := p.Apply(state, Input{Ctx: ctx, Args: codec.UpgradeArgs{ContractID: id, NewCodeHash: newCodeHash}})
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if eff.Status != codec.StatusValidationFailed {
		t.Fatalf("Status = %v, want StatusValidationFailed", eff.Status)
	}
	if eff.CodeHash != oldCodeHash {
		t.Fatalf("CodeHash = %x, want %x (unchanged)", eff.CodeHash, oldCodeHash)
	}
	if len(eff.StateDiff) != 0 {
		t.Fatalf("StateDiff = %+v, want empty", eff.StateDiff)
	}
	assertUpgradeReads(t, state, id)
}

func TestApplyUpgradeNoPendingRecordHalts(t *testing.T) {
	p := newTestPlugin(t)
	id := types.Hash{0x40}
	sender := types.Address{1}
	oldCodeHash := types.Hash{0xDD}
	owner := callerHash(sender)

	state := upgradeState(id, oldCodeHash, true, []byte{policyOwnerUpgradeable}, true, owner[:])
	ctx := domain.InputContext{L1Sender: sender}
	if _, err := p.Apply(state, Input{Ctx: ctx, Args: codec.UpgradeArgs{ContractID: id, NewCodeHash: types.Hash{0x99}}}); err == nil {
		t.Fatal("Apply: got nil error for an authorized upgrade naming a code hash with no pending record, want a halt")
	}
}

// --- upgrade halt paths on state, and the deliberate keyCodeHash/keyPreCodeHash asymmetry.

func TestApplyUpgradeHaltsOnStateReadError(t *testing.T) {
	p := newTestPlugin(t)
	state := &mapState{err: fmt.Errorf("boom")}
	if _, err := p.Apply(state, Input{Args: codec.UpgradeArgs{ContractID: types.Hash{1}, NewCodeHash: types.Hash{2}}}); err == nil {
		t.Fatal("Apply: got nil error when StateAccess.Read fails, want a halt")
	}
}

func TestApplyUpgradeHaltsOnMalformedCodeHashWidth(t *testing.T) {
	p := newTestPlugin(t)
	id := types.Hash{0x60}
	state := &mapState{values: map[domain.Path][]byte{
		derive(id, keyCodeHash): make([]byte, 31),
	}}
	if _, err := p.Apply(state, Input{Args: codec.UpgradeArgs{ContractID: id, NewCodeHash: types.Hash{2}}}); err == nil {
		t.Fatal("Apply: got nil error for a 31-byte keyCodeHash leaf, want a halt")
	}
}

func TestApplyUpgradeSucceedsWithMalformedPreCodeHashWidth(t *testing.T) {
	p := newTestPlugin(t)
	id := types.Hash{0x70}
	sender := types.Address{1}
	oldCodeHash := types.Hash{0xEE}
	owner := callerHash(sender)

	raw := buildModule(nil, []byte{0x00}, []byte{0x42, 0x00, 0x0B}, 1)
	newPreCodeHash := seedDeployment(t, p, raw, sender, immutableMetadata)

	state := upgradeState(id, oldCodeHash, true, []byte{policyOwnerUpgradeable}, true, owner[:])
	state.values[derive(id, keyPreCodeHash)] = make([]byte, 31)

	ctx := domain.InputContext{L1Sender: sender}
	eff, err := p.Apply(state, Input{Ctx: ctx, Args: codec.UpgradeArgs{ContractID: id, NewCodeHash: newPreCodeHash}})
	if err != nil {
		t.Fatalf("Apply: got error %v, want nil", err)
	}
	if eff.Status != codec.StatusSuccess {
		t.Fatalf("Status = %v, want StatusSuccess despite a malformed keyPreCodeHash leaf", eff.Status)
	}
}

// --- transfer ownership: read order and every status row.

// assertTransferReads checks §20's fixed, branch-independent three-key read order (no
// keyPreCodeHash read).
func assertTransferReads(t *testing.T, state *mapState, id types.Hash) {
	t.Helper()
	want := []domain.Path{
		derive(id, keyCodeHash),
		derive(id, keyUpgradePolicy),
		derive(id, keyOwner),
	}
	if !reflect.DeepEqual(state.reads, want) {
		t.Fatalf("read sequence = %v, want %v", state.reads, want)
	}
}

func TestApplyTransferOwnershipRows(t *testing.T) {
	id := types.Hash{0x90}
	codeHash := types.Hash{0xAB}
	owner := callerHash(types.Address{1, 2, 3})
	newOwnerAddr := types.Address{4, 5, 6}

	t.Run("NoCodeHashLeafValidationFailed", func(t *testing.T) {
		p := newTestPlugin(t)
		state := &mapState{values: map[domain.Path][]byte{}}
		eff, err := p.Apply(state, Input{Args: codec.TransferOwnershipArgs{ContractID: id, NewOwner: newOwnerAddr}})
		if err != nil {
			t.Fatalf("Apply: %v", err)
		}
		if eff.Status != codec.StatusValidationFailed {
			t.Fatalf("Status = %v, want StatusValidationFailed", eff.Status)
		}
		if eff.ContractID != id {
			t.Fatalf("ContractID = %x, want %x", eff.ContractID, id)
		}
		if eff.CodeHash != (types.Hash{}) {
			t.Fatalf("CodeHash = %x, want zero", eff.CodeHash)
		}
		assertTransferReads(t, state, id)
	})

	t.Run("UnauthorizedNoOwnerLeaf", func(t *testing.T) {
		p := newTestPlugin(t)
		state := &mapState{values: map[domain.Path][]byte{
			derive(id, keyCodeHash): codeHash[:],
		}}
		eff, err := p.Apply(state, Input{Args: codec.TransferOwnershipArgs{ContractID: id, NewOwner: newOwnerAddr}})
		if err != nil {
			t.Fatalf("Apply: %v", err)
		}
		if eff.Status != codec.StatusUnauthorized {
			t.Fatalf("Status = %v, want StatusUnauthorized", eff.Status)
		}
		if eff.CodeHash != codeHash {
			t.Fatalf("CodeHash = %x, want %x (current)", eff.CodeHash, codeHash)
		}
		assertTransferReads(t, state, id)
	})

	t.Run("SuccessNonZeroNewOwnerRewritesOwnerLeaf", func(t *testing.T) {
		p := newTestPlugin(t)
		state := &mapState{values: map[domain.Path][]byte{
			derive(id, keyCodeHash):      codeHash[:],
			derive(id, keyUpgradePolicy): {policyOwnerUpgradeable},
			derive(id, keyOwner):         owner[:],
		}}
		ctx := domain.InputContext{L1Sender: types.Address{1, 2, 3}}
		eff, err := p.Apply(state, Input{Ctx: ctx, Args: codec.TransferOwnershipArgs{ContractID: id, NewOwner: newOwnerAddr}})
		if err != nil {
			t.Fatalf("Apply: %v", err)
		}
		if eff.Status != codec.StatusSuccess {
			t.Fatalf("Status = %v, want StatusSuccess", eff.Status)
		}
		if eff.GasUsed != 0 {
			t.Fatalf("GasUsed = %d, want 0", eff.GasUsed)
		}
		if eff.ContractID != id || eff.CodeHash != codeHash {
			t.Fatalf("ContractID/CodeHash = %x/%x, want %x/%x", eff.ContractID, eff.CodeHash, id, codeHash)
		}
		newOwnerHash := callerHash(newOwnerAddr)
		want := []domain.StateDiffEntry{{Path: derive(id, keyOwner), Value: newOwnerHash[:]}}
		if !reflect.DeepEqual(eff.StateDiff, want) {
			t.Fatalf("StateDiff = %+v, want %+v", eff.StateDiff, want)
		}
		if eff.Events != nil || eff.OutboxMessages != nil || eff.ReturnData != nil {
			t.Fatalf("Events/OutboxMessages/ReturnData = %+v/%+v/%+v, want nil/nil/nil", eff.Events, eff.OutboxMessages, eff.ReturnData)
		}
		assertTransferReads(t, state, id)
	})

	t.Run("SuccessZeroNewOwnerRenounces", func(t *testing.T) {
		p := newTestPlugin(t)
		state := &mapState{values: map[domain.Path][]byte{
			derive(id, keyCodeHash):      codeHash[:],
			derive(id, keyUpgradePolicy): {policyOwnerUpgradeable},
			derive(id, keyOwner):         owner[:],
		}}
		ctx := domain.InputContext{L1Sender: types.Address{1, 2, 3}}
		eff, err := p.Apply(state, Input{Ctx: ctx, Args: codec.TransferOwnershipArgs{ContractID: id, NewOwner: types.Address{}}})
		if err != nil {
			t.Fatalf("Apply: %v", err)
		}
		if eff.Status != codec.StatusSuccess {
			t.Fatalf("Status = %v, want StatusSuccess", eff.Status)
		}
		want := sortStateDiff([]domain.StateDiffEntry{
			{Path: derive(id, keyUpgradePolicy), Value: []byte{policyImmutable}},
			{Path: derive(id, keyOwner), Value: nil},
		})
		if !reflect.DeepEqual(eff.StateDiff, want) {
			t.Fatalf("StateDiff = %+v, want %+v", eff.StateDiff, want)
		}
		assertTransferReads(t, state, id)
	})

	t.Run("HaltsOnMalformedCodeHashWidth", func(t *testing.T) {
		p := newTestPlugin(t)
		state := &mapState{values: map[domain.Path][]byte{
			derive(id, keyCodeHash): make([]byte, 31),
		}}
		if _, err := p.Apply(state, Input{Args: codec.TransferOwnershipArgs{ContractID: id, NewOwner: newOwnerAddr}}); err == nil {
			t.Fatal("Apply: got nil error for a 31-byte keyCodeHash leaf, want a halt")
		}
	})
}

// --- upgrade behaviour must not depend on code-store cache warmth.

func TestApplyUpgradeWarmVsColdCodeStore(t *testing.T) {
	sender := types.Address{1, 2, 3}
	id := types.Hash{0x80}
	oldCodeHash := types.Hash{0xFF}
	owner := callerHash(sender)
	raw := buildModule(nil, []byte{0x00}, []byte{0x42, 0x00, 0x0B}, 1)

	run := func(t *testing.T, warm bool) (domain.Effects, []domain.Path) {
		t.Helper()
		p := newTestPlugin(t)
		newPreCodeHash := seedDeployment(t, p, raw, sender, immutableMetadata)
		if warm {
			instrumented, err := module.Instrument(raw, moduleLimits())
			if err != nil {
				t.Fatalf("Instrument: %v", err)
			}
			codeHash := CodeHash(instrumented)
			if err := p.store.put(entryAssembledModule, newPreCodeHash[:], raw); err != nil {
				t.Fatalf("store.put: %v", err)
			}
			if err := p.store.put(entryInstrumentedModule, codeHash[:], instrumented); err != nil {
				t.Fatalf("store.put: %v", err)
			}
		}
		state := upgradeState(id, oldCodeHash, true, []byte{policyOwnerUpgradeable}, true, owner[:])
		ctx := domain.InputContext{L1Sender: sender}
		eff, err := p.Apply(state, Input{Ctx: ctx, Args: codec.UpgradeArgs{ContractID: id, NewCodeHash: newPreCodeHash}})
		if err != nil {
			t.Fatalf("Apply: %v", err)
		}
		return eff, state.reads
	}

	coldEff, coldReads := run(t, false)
	warmEff, warmReads := run(t, true)

	if !reflect.DeepEqual(coldEff, warmEff) {
		t.Fatalf("Effects differ between cold and warm code store: %+v vs %+v", coldEff, warmEff)
	}
	if !reflect.DeepEqual(coldReads, warmReads) {
		t.Fatalf("read sequences differ between cold and warm code store: %v vs %v", coldReads, warmReads)
	}
}
