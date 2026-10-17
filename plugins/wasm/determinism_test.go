package wasm

// This file proves determinism across cache warmth, not just across
// inputs: the set of observed keys must be a deterministic function of
// the input and the pre-state alone, and no local cache, memoization or
// other node-local state may change which keys are observed, since two
// honest nodes would otherwise commit different DAHash values for the
// same batch. A byte-identical ExecutionResult is also required across
// compliant runtimes. TestDeterminismCacheMatrix runs every frozen
// conformance vector three times in each of four cache states
// (compiled-module cache and code-store) and byte-compares every
// committed outcome field against the vector's frozen row;
// TestDeterminismWitnessBackendTripwire proves the comparison itself is
// not vacuous.

import (
	"encoding/hex"
	"encoding/json"
	"os"
	"testing"

	"github.com/zenon-network/go-zenon/common/crypto"
	"github.com/zenon-network/go-zenon/common/types"

	"github.com/digitalSloth/go-executor/domain"
	"github.com/digitalSloth/go-executor/internal/state"
	"github.com/digitalSloth/go-executor/plugins/wasm/internal/engine"
	"github.com/digitalSloth/go-executor/plugins/wasm/internal/module"
)

// determinismRuns is N: how many times each vector runs in each cell. Two is the minimum that
// makes a warm cell hit the compiled-module cache at all; three keeps a repetition beyond that
// first hit at negligible cost.
const determinismRuns = 3

// cacheCell is one cell of the 2x2 cache-state matrix.
type cacheCell struct {
	name              string
	freshPluginPerRun bool // cold compiled-module cache: a new Engine, hence an empty cache
	warmInstrumented  bool // whether entryInstrumentedModule is present when Apply runs
}

var cacheCells = []cacheCell{
	{name: "warm-cache_warm-store", freshPluginPerRun: false, warmInstrumented: true},
	{name: "cold-cache_warm-store", freshPluginPerRun: true, warmInstrumented: true},
	{name: "warm-cache_cold-store", freshPluginPerRun: false, warmInstrumented: false},
	{name: "cold-cache_cold-store", freshPluginPerRun: true, warmInstrumented: false},
}

// runOutcome is everything one run of one vector produces that must be byte-identical across
// every cell. Hex strings, so a mismatch prints the bytes.
type runOutcome struct {
	Status              string
	GasUsed             uint64
	ContractEffectsBlob string // "" when the run returned no blob
	ExecutionResultBlob string
	ReceiptBlob         string
	PostStateRoot       string
	WitnessKeys         []string
}

// loadVectorFile reads and unmarshals the committed conformance artifact.
func loadVectorFile(t *testing.T) vectorFile {
	t.Helper()
	data, err := os.ReadFile(vectorFilePath)
	if err != nil {
		t.Fatalf("loadVectorFile: ReadFile(%s): %v", vectorFilePath, err)
	}
	var file vectorFile
	if err := json.Unmarshal(data, &file); err != nil {
		t.Fatalf("loadVectorFile: Unmarshal: %v", err)
	}
	return file
}

// newPluginAt builds a Plugin over dir. It registers no cleanup: the caller defers the close, so
// a cold-cache cell holds one wazero runtime at a time rather than one per run.
func newPluginAt(t *testing.T, dir string) *Plugin {
	t.Helper()
	p, err := New(Config{NetworkName: "testnet", CodeStorePath: dir})
	if err != nil {
		t.Fatalf("newPluginAt: New: %v", err)
	}
	return p
}

// mustAddress decodes a 20-byte hex string into a types.Address.
func mustAddress(t *testing.T, s string) types.Address {
	t.Helper()
	var a types.Address
	b := mustHex(t, s)
	if len(b) != len(a) {
		t.Fatalf("address %q has length %d, want %d", s, len(b), len(a))
	}
	copy(a[:], b)
	return a
}

// seedCodeStore puts p's code store into the state vec's cell requires: the tiers Apply must
// read, and the instrumented tier present or absent per warmInstrumented. It asserts the
// resulting presence, so a cell can never silently degenerate into the warm one.
func seedCodeStore(t *testing.T, p *Plugin, vec vectorJSON, warmInstrumented bool) {
	t.Helper()

	if vec.ModuleWasmHex == "" {
		return // the unsupported_kind, ownership and buy_storage vectors: nothing to seed.
	}
	raw := mustHex(t, vec.ModuleWasmHex)

	switch {
	case vec.InputKind == 4 || vec.InputKind == 5:
		// The pending record and the chunks prepareModule assembles from. A kind-4 finalize halts
		// when the record's deployer differs from its sender, so the row's own l1_sender is seeded.
		seedDeployment(t, p, raw, mustAddress(t, vec.L1Sender), mustHex(t, *vec.PendingMetadataHex))
	case vec.Shape == "call":
		// The key loadInstrumented looks the assembled tier up by: the row's pre_state carries
		// pre_code_hash as this same value.
		codeHash := mustHash(t, vec.CodeHash)
		if err := p.store.put(entryAssembledModule, codeHash[:], raw); err != nil {
			t.Fatalf("seedCodeStore: %s: put assembled: %v", vec.ID, err)
		}
	case vec.InputKind == 2 || vec.InputKind == 3:
		// DecodeInput is what writes their entries; that happens inside runVector.
	default:
		t.Fatalf("seedCodeStore: %s: unhandled shape %q / input_kind %d: add its seeding rule with its first vector", vec.ID, vec.Shape, vec.InputKind)
	}

	if vec.InstrumentedCodeHash == "" {
		return
	}
	instrumentedCodeHash := mustHash(t, vec.InstrumentedCodeHash)
	key := instrumentedCodeHash[:]
	if warmInstrumented {
		instrumented, err := module.Instrument(raw, moduleLimits())
		if err != nil {
			t.Fatalf("seedCodeStore: %s: Instrument: %v", vec.ID, err)
		}
		if got := CodeHash(instrumented); got != instrumentedCodeHash {
			t.Fatalf("seedCodeStore: %s: CodeHash(instrumented) = %x, want instrumented_code_hash %s", vec.ID, got, vec.InstrumentedCodeHash)
		}
		if err := p.store.put(entryInstrumentedModule, key, instrumented); err != nil {
			t.Fatalf("seedCodeStore: %s: put instrumented: %v", vec.ID, err)
		}
	} else {
		if err := p.store.delete(entryInstrumentedModule, key); err != nil {
			t.Fatalf("seedCodeStore: %s: delete instrumented: %v", vec.ID, err)
		}
	}
	if _, ok, err := p.store.get(entryInstrumentedModule, key); err != nil {
		t.Fatalf("seedCodeStore: %s: get instrumented: %v", vec.ID, err)
	} else if ok != warmInstrumented {
		t.Fatalf("seedCodeStore: %s: instrumented tier present = %v, want %v", vec.ID, ok, warmInstrumented)
	}
}

// runVector applies vec's input against p and returns everything the comparison covers. It
// opens, seeds and closes its own state.Store, so no state crosses runs. coldCache is the cell's
// freshPluginPerRun: it decides whether the step-8 probe gets its own fresh engine.
func runVector(t *testing.T, p *Plugin, vec vectorJSON, coldCache bool) runOutcome {
	t.Helper()

	raw := mustHex(t, vec.ModuleWasmHex)
	ctx := domain.InputContext{
		GlobalInputIndex: vec.GlobalInputIndex,
		MomentumHeight:   vec.MomentumHeight,
		MomentumTime:     vec.MomentumTime,
		MomentumHash:     mustHash(t, vec.MomentumHash),
		L1Sender:         mustAddress(t, vec.L1Sender),
		SourceData:       mustHex(t, vec.SourceDataHex),
		Caps:             capTableFromJSON(t, vec.WithdrawalCaps),
	}

	store := state.OpenEmpty()
	defer store.Close()

	paths, values := leavesFromPreState(t, vec.PreState)
	diff := make([]domain.StateDiffEntry, len(paths))
	for i := range paths {
		diff[i] = domain.StateDiffEntry{Path: paths[i], Value: values[i]}
	}
	preRoot, err := store.Apply(diff)
	if err != nil {
		t.Fatalf("runVector: %s: seed store.Apply: %v", vec.ID, err)
	}
	if hex.EncodeToString(preRoot[:]) != vec.PreStateRoot {
		t.Fatalf("runVector: %s: preRoot = %x, want pre_state_root %s (a seeding bug, not a determinism result)", vec.ID, preRoot, vec.PreStateRoot)
	}

	// Both taken here, before any store.Apply of the run's own diff: Store.Accessor snapshots
	// s.leaves at construction, and the step-8 probe below must execute against the pre-state.
	acc := store.Accessor()
	probeAcc := store.Accessor()

	canonical := mustHex(t, vec.CanonicalInputHex)
	in, err := p.DecodeInput(ctx, canonical)
	if err != nil {
		t.Fatalf("runVector: %s: DecodeInput: %v", vec.ID, err)
	}
	decoded, ok := in.(Input)
	if !ok {
		t.Fatalf("runVector: %s: DecodeInput returned %T, want wasm.Input", vec.ID, in)
	}

	eff, err := p.Apply(acc, decoded)
	if err != nil {
		t.Fatalf("runVector: %s: Apply: %v (a cold-code-store cell keeps the assembled tier present)", vec.ID, err)
	}

	postRoot, err := store.Apply(eff.StateDiff)
	if err != nil {
		t.Fatalf("runVector: %s: post store.Apply: %v", vec.ID, err)
	}

	// probeAcc must still read the pre-state: for every diff entry that overwrote a pre_state
	// leaf, its value through probeAcc must match the frozen pre_state entry, not the value the
	// diff just wrote. This is the regression check for the ordering invariant at :191 above -
	// a snapshot taken after store.Apply(eff.StateDiff) would fail it.
	for _, entry := range eff.StateDiff {
		pathHex := hex.EncodeToString(entry.Path[:])
		frozenValueHex, ok := vec.PreState[pathHex]
		if !ok {
			continue
		}
		value, present, err := probeAcc.Read(entry.Path)
		if err != nil {
			t.Fatalf("runVector: %s: probeAcc.Read(%s): %v", vec.ID, pathHex, err)
		}
		if !present || hex.EncodeToString(value) != frozenValueHex {
			t.Fatalf("runVector: %s: probeAcc is not a pre-state snapshot: %s = %s (present %v), want frozen pre_state %s", vec.ID, pathHex, hex.EncodeToString(value), present, frozenValueHex)
		}
	}

	ref := refDerive(t, eff, acc, vec.GlobalInputIndex, vec.BatchID, preRoot, postRoot, hashOf(crypto.Hash(canonical)))

	witnessKeys := make([]string, len(ref.WitnessKeys))
	for i, path := range ref.WitnessKeys {
		witnessKeys[i] = hex.EncodeToString(path[:])
	}

	out := runOutcome{
		Status:              eff.Status.String(),
		GasUsed:             eff.GasUsed,
		ExecutionResultBlob: hex.EncodeToString(ref.ExecutionResultBlob),
		ReceiptBlob:         hex.EncodeToString(ref.ReceiptBlob),
		PostStateRoot:       hex.EncodeToString(postRoot[:]),
		WitnessKeys:         witnessKeys,
	}

	// The ContractEffects blob, call shape only, and after Apply, never before — a pre-Apply
	// run would compile the module and silently warm the cold cells. The engine is picked so the
	// probe's cache warmth tracks the cell.
	if vec.Shape == "call" {
		instrumented, err := module.Instrument(raw, moduleLimits())
		if err != nil {
			t.Fatalf("runVector: %s: probe Instrument: %v", vec.ID, err)
		}
		pe := p.engine
		if coldCache {
			pe, err = engine.New()
			if err != nil {
				t.Fatalf("runVector: %s: probe engine.New: %v", vec.ID, err)
			}
			defer pe.Close()
		}
		result, err := pe.Run(
			engine.HostContext{State: probeAcc, ContractID: mustHash(t, vec.Receipt.ContractID)},
			mustHash(t, vec.InstrumentedCodeHash), instrumented, mustHex(t, vec.InputFrameHex), *vec.Input.PlasmaLimit)
		if err != nil {
			t.Fatalf("runVector: %s: probe engine.Run: %v", vec.ID, err)
		}
		if result.Kind == engine.Returned && result.Ret >= 0 {
			out.ContractEffectsBlob = hex.EncodeToString(result.ResultBlob)
		}
	}

	return out
}

// assertOutcome fails with cell, run index and vector ID named for any field of got that
// differs from vec's frozen bytes.
func assertOutcome(t *testing.T, cell string, run int, vec vectorJSON, got runOutcome) {
	t.Helper()
	if got.Status != vec.Status {
		t.Fatalf("[%s run %d] %s: Status = %s, want %s", cell, run, vec.ID, got.Status, vec.Status)
	}
	if got.GasUsed != vec.GasUsed {
		t.Fatalf("[%s run %d] %s: GasUsed = %d, want %d", cell, run, vec.ID, got.GasUsed, vec.GasUsed)
	}
	if got.ContractEffectsBlob != vec.ContractEffectsBlobHex {
		t.Fatalf("[%s run %d] %s: ContractEffectsBlob = %s, want %s", cell, run, vec.ID, got.ContractEffectsBlob, vec.ContractEffectsBlobHex)
	}
	if got.ExecutionResultBlob != vec.ExecutionResultBlobHex {
		t.Fatalf("[%s run %d] %s: ExecutionResultBlob = %s, want %s", cell, run, vec.ID, got.ExecutionResultBlob, vec.ExecutionResultBlobHex)
	}
	if got.ReceiptBlob != vec.ReceiptHex {
		t.Fatalf("[%s run %d] %s: ReceiptBlob = %s, want %s", cell, run, vec.ID, got.ReceiptBlob, vec.ReceiptHex)
	}
	if got.PostStateRoot != vec.PostStateRoot {
		t.Fatalf("[%s run %d] %s: PostStateRoot = %s, want %s", cell, run, vec.ID, got.PostStateRoot, vec.PostStateRoot)
	}
	if len(got.WitnessKeys) != len(vec.WitnessKeys) {
		t.Fatalf("[%s run %d] %s: len(WitnessKeys) = %d, want %d (%v vs %v)", cell, run, vec.ID, len(got.WitnessKeys), len(vec.WitnessKeys), got.WitnessKeys, vec.WitnessKeys)
	}
	for i := range got.WitnessKeys {
		if got.WitnessKeys[i] != vec.WitnessKeys[i] {
			t.Fatalf("[%s run %d] %s: WitnessKeys[%d] = %s, want %s", cell, run, vec.ID, i, got.WitnessKeys[i], vec.WitnessKeys[i])
		}
	}
}

// TestDeterminismCacheMatrix runs every vector determinismRuns times in each of the four cells.
func TestDeterminismCacheMatrix(t *testing.T) {
	file := loadVectorFile(t)

	// Named once, here, so a future artifact regenerated on another network fails with a named
	// cause rather than as 35 individual call-vector blob mismatches.
	probe := newPluginAt(t, t.TempDir())
	defer probe.Close()
	if probe.chainID != mustHash(t, file.ChainID) {
		t.Fatalf("TestDeterminismCacheMatrix: p.chainID = %x, want chain_id %s (network name mismatch)", probe.chainID, file.ChainID)
	}

	for _, cell := range cacheCells {
		cell := cell
		t.Run(cell.name, func(t *testing.T) {
			cellDir := t.TempDir()
			var shared *Plugin
			if !cell.freshPluginPerRun {
				shared = newPluginAt(t, cellDir)
				defer shared.Close()
			}

			for _, vec := range file.Vectors {
				vec := vec
				t.Run(vec.ID, func(t *testing.T) {
					for run := 0; run < determinismRuns; run++ {
						func() {
							plugin := shared
							if cell.freshPluginPerRun {
								plugin = newPluginAt(t, cellDir)
								defer plugin.Close()
							}
							seedCodeStore(t, plugin, vec, cell.warmInstrumented)
							got := runVector(t, plugin, vec, cell.freshPluginPerRun)
							assertOutcome(t, cell.name, run, vec, got)
						}()
					}
				})
			}
		})
	}
}

// TestDeterminismWitnessBackendTripwire proves a mapState-shaped backend fails the matrix's
// witness comparison rather than passing it silently.
//
// EXEC-002 is the vector that makes this observable: its module passes a "probe" local key to
// env.storage_len and never writes it, so the derived path is in the frozen witness set only
// because state.Accessor.Len records an absent-key observation and mapState.Len records
// nothing. This does not duplicate checkN1WitnessBackendTripwire, which asserts the shape of
// the frozen artifact; this test executes the substitution the acceptance criterion names and
// shows the resulting witness set is strictly smaller — exactly what makes
// assertOutcome's length-before-element comparison fail loudly rather than pass.
func TestDeterminismWitnessBackendTripwire(t *testing.T) {
	file := loadVectorFile(t)

	var vec vectorJSON
	found := false
	for _, v := range file.Vectors {
		if v.ID == "EXEC-002" {
			vec = v
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("TestDeterminismWitnessBackendTripwire: vector EXEC-002 not found in the artifact")
	}

	p := newPluginAt(t, t.TempDir())
	defer p.Close()
	seedCodeStore(t, p, vec, true)

	values := make(map[domain.Path][]byte, len(vec.PreState))
	for pathHex, valueHex := range vec.PreState {
		path := mustPath(t, pathHex)
		if valueHex == "" {
			values[path] = []byte{}
		} else {
			values[path] = mustHex(t, valueHex)
		}
	}
	backend := &mapState{values: values}

	ctx := domain.InputContext{
		GlobalInputIndex: vec.GlobalInputIndex,
		MomentumHeight:   vec.MomentumHeight,
		MomentumTime:     vec.MomentumTime,
		MomentumHash:     mustHash(t, vec.MomentumHash),
		L1Sender:         mustAddress(t, vec.L1Sender),
		SourceData:       mustHex(t, vec.SourceDataHex),
		Caps:             capTableFromJSON(t, vec.WithdrawalCaps),
	}
	in, err := p.DecodeInput(ctx, mustHex(t, vec.CanonicalInputHex))
	if err != nil {
		t.Fatalf("TestDeterminismWitnessBackendTripwire: DecodeInput: %v", err)
	}
	decoded, ok := in.(Input)
	if !ok {
		t.Fatalf("TestDeterminismWitnessBackendTripwire: DecodeInput returned %T, want wasm.Input", in)
	}

	eff, err := p.Apply(backend, decoded)
	if err != nil {
		t.Fatalf("TestDeterminismWitnessBackendTripwire: Apply: %v", err)
	}
	if eff.Status.String() != vec.Status {
		t.Fatalf("TestDeterminismWitnessBackendTripwire: Status = %s, want frozen %s (a failed run would make the union vacuously small)", eff.Status.String(), vec.Status)
	}
	if eff.GasUsed != vec.GasUsed {
		t.Fatalf("TestDeterminismWitnessBackendTripwire: GasUsed = %d, want frozen %d", eff.GasUsed, vec.GasUsed)
	}

	union := make(map[string]bool, len(backend.reads)+len(eff.StateDiff))
	for _, path := range backend.reads {
		union[hex.EncodeToString(path[:])] = true
	}
	for _, entry := range eff.StateDiff {
		union[hex.EncodeToString(entry.Path[:])] = true
	}

	var missing []string
	for _, k := range vec.WitnessKeys {
		if !union[k] {
			missing = append(missing, k)
		}
	}
	if len(missing) == 0 {
		t.Fatalf("TestDeterminismWitnessBackendTripwire: mapState-backed witness union carries every frozen witness key %v; want at least one missing (mapState.Len records nothing on an absent-key observation)", vec.WitnessKeys)
	}
}
