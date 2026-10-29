package engine

import (
	"errors"
	"fmt"
	"math"
	"reflect"
	"testing"

	"github.com/zenon-network/go-zenon/common/crypto"
	"github.com/zenon-network/go-zenon/common/types"

	"github.com/Zeno-L2/zeno-executor/domain"
	"github.com/Zeno-L2/zeno-executor/internal/state"
	"github.com/Zeno-L2/zeno-executor/plugins/wasm/internal/module"
)

// appendVarS64 appends the shortest signed LEB128 encoding of v to dst, the same shape as
// appendVarU32.
func appendVarS64(dst []byte, v int64) []byte {
	for {
		b := byte(v & 0x7F)
		v >>= 7
		signBitSet := b&0x40 != 0
		if (v == 0 && !signBitSet) || (v == -1 && signBitSet) {
			return append(dst, b)
		}
		dst = append(dst, b|0x80)
	}
}

// appendVarS32 appends the shortest signed LEB128 encoding of v to dst.
func appendVarS32(dst []byte, v int32) []byte {
	return appendVarS64(dst, int64(v))
}

// encodeFuncType appends one func type entry (form 0x60, params, results).
func encodeFuncType(params, results []byte) []byte {
	var b []byte
	b = append(b, 0x60)
	b = appendVarU32(b, uint32(len(params)))
	b = append(b, params...)
	b = appendVarU32(b, uint32(len(results)))
	b = append(b, results...)
	return b
}

// appendName appends a length-prefixed name.
func appendName(dst []byte, s string) []byte {
	dst = appendVarU32(dst, uint32(len(s)))
	return append(dst, s...)
}

// dataSeg is one active data segment buildModule may append.
type dataSeg struct {
	offset uint32
	bytes  []byte
}

// buildModule builds a module.Validate-accepted module: a type section carrying run's
// (i32,i32)->i64 plus, on demand, the imported env functions' own types (env.storage_len
// shares run's type); an import section for the requested subset of env.storage_len /
// env.storage_read / env.abort in the given order; a function section (run only); a memory
// section {min pages}; an export section exporting memory and run (run's function index is
// len(imports)); the code section (run's body only); and, when data != nil, an active data
// segment emitted after the code section.
func buildModule(imports []string, data *dataSeg, locals, body []byte, pages uint32) []byte {
	var out []byte
	out = append(out, wasmHeader...)

	runType := encodeFuncType([]byte{0x7F, 0x7F}, []byte{0x7E})
	types := [][]byte{runType} // index 0: run and storage_len share this

	storageReadIdx, abortIdx := -1, -1
	for _, name := range imports {
		switch name {
		case "storage_read":
			if storageReadIdx == -1 {
				storageReadIdx = len(types)
				types = append(types, encodeFuncType([]byte{0x7F, 0x7F, 0x7F, 0x7F}, []byte{0x7E}))
			}
		case "abort":
			if abortIdx == -1 {
				abortIdx = len(types)
				types = append(types, encodeFuncType([]byte{0x7F, 0x7F, 0x7F}, nil))
			}
		}
	}

	typeSec := appendVarU32(nil, uint32(len(types)))
	for _, t := range types {
		typeSec = append(typeSec, t...)
	}
	out = append(out, section(1, typeSec)...)

	if len(imports) > 0 {
		importSec := appendVarU32(nil, uint32(len(imports)))
		for _, name := range imports {
			importSec = appendName(importSec, "env")
			importSec = appendName(importSec, name)
			importSec = append(importSec, 0x00)
			var typeIdx uint32
			switch name {
			case "storage_len":
				typeIdx = 0
			case "storage_read":
				typeIdx = uint32(storageReadIdx)
			case "abort":
				typeIdx = uint32(abortIdx)
			}
			importSec = appendVarU32(importSec, typeIdx)
		}
		out = append(out, section(2, importSec)...)
	}

	funcSec := []byte{0x01, 0x00} // run: 1 function, type index 0
	out = append(out, section(3, funcSec)...)

	memSec := appendVarU32([]byte{0x01, 0x00}, pages) // 1 memory, flags 0, min pages
	out = append(out, section(5, memSec)...)

	runIdx := uint32(len(imports))
	exportSec := []byte{0x02}
	exportSec = appendName(exportSec, "memory")
	exportSec = append(exportSec, 0x02, 0x00)
	exportSec = appendName(exportSec, "run")
	exportSec = append(exportSec, 0x00)
	exportSec = appendVarU32(exportSec, runIdx)
	out = append(out, section(7, exportSec)...)

	fbody := append(append([]byte{}, locals...), body...)
	codeSec := []byte{0x01}
	codeSec = appendVarU32(codeSec, uint32(len(fbody)))
	codeSec = append(codeSec, fbody...)
	out = append(out, section(10, codeSec)...)

	if data != nil {
		dataPayload := []byte{0x01, 0x00, 0x41}
		dataPayload = appendVarS32(dataPayload, int32(data.offset))
		dataPayload = append(dataPayload, 0x0B)
		dataPayload = appendVarU32(dataPayload, uint32(len(data.bytes)))
		dataPayload = append(dataPayload, data.bytes...)
		out = append(out, section(11, dataPayload)...)
	}

	return out
}

// codeHashOf computes SHA3-256(instrumented), independently of plugins/wasm.CodeHash (which
// this package must not import).
func codeHashOf(instrumented []byte) types.Hash {
	var out types.Hash
	copy(out[:], crypto.Hash(instrumented))
	return out
}

// buildAndInstrument runs a raw module through module.Validate then module.Instrument, so
// every fixture reaching Run is bytes the real pipeline would produce.
func buildAndInstrument(t *testing.T, wasm []byte) ([]byte, types.Hash) {
	t.Helper()
	if err := module.Validate(wasm, testLimits); err != nil {
		t.Fatalf("Validate = %v, want nil", err)
	}
	instrumented, err := module.Instrument(wasm, testLimits)
	if err != nil {
		t.Fatalf("Instrument = %v, want nil", err)
	}
	return instrumented, codeHashOf(instrumented)
}

// newTestState opens a real state.Store under t.TempDir(), Applies leaves, and returns its
// Accessor.
func newTestState(t *testing.T, leaves map[domain.Path][]byte) *state.Accessor {
	t.Helper()
	store := state.OpenEmpty()
	t.Cleanup(func() { _ = store.Close() })

	var diff []domain.StateDiffEntry
	for p, v := range leaves {
		diff = append(diff, domain.StateDiffEntry{Path: p, Value: v})
	}
	if _, err := store.Apply(diff); err != nil {
		t.Fatalf("state.Apply: %v", err)
	}
	return store.Accessor()
}

// errFailingState is the sentinel failingState returns.
var errFailingState = errors.New("engine: test state failure")

// failingState is a domain.StateAccess whose every call returns errFailingState.
type failingState struct{}

func (failingState) Read(domain.Path) ([]byte, bool, error) { return nil, false, errFailingState }
func (failingState) Len(domain.Path) (int, bool, error)     { return 0, false, errFailingState }

// depthHostCallBody is depthCeilingBody's shape with one import (env.storage_len at function
// index 0, so run is function index 1): recurses via call 1, and its base case calls
// storage_len(0, 0) before returning 42, proving the nested ledger accessor call disturbs
// neither __zenon_depth nor wazero's own call-stack ceiling.
func depthHostCallBody() []byte {
	return []byte{
		0x20, 0x01, // local.get 1
		0x45,       // i32.eqz
		0x04, 0x7E, // if (result i64)
		0x41, 0x00, //   i32.const 0 (key_ptr)
		0x41, 0x00, //   i32.const 0 (key_len)
		0x10, 0x00, //   call 0 (storage_len)
		0x1A,       //   drop
		0x42, 0x2A, //   i64.const 42
		0x05,       // else
		0x20, 0x00, //   local.get 0
		0x20, 0x01, //   local.get 1
		0x41, 0x01, //   i32.const 1
		0x6B,       //   i32.sub
		0x10, 0x01, //   call 1 (run, self)
		0x0B, // end (if)
		0x0B, // end (function)
	}
}

func testHostContext(t *testing.T, accessor domain.StateAccess) HostContext {
	t.Helper()
	var contractID types.Hash
	for i := range contractID {
		contractID[i] = byte(i + 1)
	}
	return HostContext{State: accessor, ContractID: contractID}
}

func newTestEngine(t *testing.T) *Engine {
	t.Helper()
	e, err := New()
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = e.Close() })
	return e
}

func TestRunReturnsAndMeters(t *testing.T) {
	// body: i64.const 42; end -- one segment, no host calls.
	body := []byte{0x42, 0x2A, 0x0B}
	instrumented, codeHash := buildAndInstrument(t, buildModule(nil, nil, []byte{0x00}, body, 1))

	e := newTestEngine(t)
	hc := testHostContext(t, newTestState(t, nil))

	out, err := e.Run(hc, codeHash, instrumented, nil, 1_000_000)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if out.Kind != Returned {
		t.Fatalf("Kind = %v, want Returned", out.Kind)
	}
	if out.Ret != 42 {
		t.Fatalf("Ret = %d, want 42", out.Ret)
	}
	want := module.InitialMemoryCost(1) + opsCost(t, 0x42, 0x0B)
	if out.GasUsed != want {
		t.Fatalf("GasUsed = %d, want %d", out.GasUsed, want)
	}
}

func TestRunChargesInitialMemory(t *testing.T) {
	body := []byte{0x42, 0x2A, 0x0B}
	ops := opsCost(t, 0x42, 0x0B)

	for _, pages := range []uint32{1, 2} {
		t.Run(fmt.Sprintf("pages=%d", pages), func(t *testing.T) {
			instrumented, codeHash := buildAndInstrument(t, buildModule(nil, nil, []byte{0x00}, body, pages))
			e := newTestEngine(t)
			hc := testHostContext(t, newTestState(t, nil))

			out, err := e.Run(hc, codeHash, instrumented, nil, 1_000_000)
			if err != nil {
				t.Fatalf("Run: %v", err)
			}
			if out.Kind != Returned {
				t.Fatalf("Kind = %v, want Returned", out.Kind)
			}
			want := module.InitialMemoryCost(pages) + ops
			if out.GasUsed != want {
				t.Fatalf("GasUsed = %d, want %d", out.GasUsed, want)
			}
		})
	}
}

func TestRunInitialMemoryUnaffordable(t *testing.T) {
	body := storageLenBody(0)
	instrumented, codeHash := buildAndInstrument(t, buildModule([]string{"storage_len"}, nil, []byte{0x00}, body, 1))
	memCost := module.InitialMemoryCost(1)
	segTotal := opsCost(t, 0x41, 0x41, 0x10, 0x0B)
	hostCost := module.HostCost(module.HostStorageLen, 0)

	t.Run("unaffordable is OutOfGas before anything runs", func(t *testing.T) {
		e := newTestEngine(t)
		accessor := newTestState(t, nil)
		out, err := e.Run(testHostContext(t, accessor), codeHash, instrumented, nil, memCost-1)
		if err != nil {
			t.Fatalf("Run: %v", err)
		}
		if out.Kind != OutOfGas {
			t.Fatalf("Kind = %v, want OutOfGas", out.Kind)
		}
		if out.GasUsed != memCost-1 {
			t.Fatalf("GasUsed = %d, want %d", out.GasUsed, memCost-1)
		}
		if len(accessor.Witnesses()) != 0 {
			t.Fatalf("Witnesses() = %v, want empty", accessor.Witnesses())
		}
	})

	t.Run("affordable runs normally", func(t *testing.T) {
		e := newTestEngine(t)
		accessor := newTestState(t, nil)
		budget := memCost + segTotal + hostCost
		out, err := e.Run(testHostContext(t, accessor), codeHash, instrumented, nil, budget)
		if err != nil {
			t.Fatalf("Run: %v", err)
		}
		if out.Kind != Returned {
			t.Fatalf("Kind = %v, want Returned", out.Kind)
		}
	})
}

func TestRunZeroedMemoryAndDataSegments(t *testing.T) {
	t.Run("data segment beyond the frame", func(t *testing.T) {
		segBytes := []byte{0xAA, 0xBB, 0xCC, 0xDD}
		body := appendVarS64([]byte{0x42}, int64(1100))
		body = append(body, 0x0B)
		instrumented, codeHash := buildAndInstrument(t, buildModule(nil, &dataSeg{offset: 1000, bytes: segBytes}, []byte{0x00}, body, 1))

		e := newTestEngine(t)
		hc := testHostContext(t, newTestState(t, nil))

		frame := make([]byte, 100)
		for i := range frame {
			frame[i] = byte(i + 1)
		}
		out, err := e.Run(hc, codeHash, instrumented, frame, 1_000_000)
		if err != nil {
			t.Fatalf("Run: %v", err)
		}
		if out.Kind != Returned {
			t.Fatalf("Kind = %v, want Returned", out.Kind)
		}
		if len(out.ResultBlob) != 1100 {
			t.Fatalf("len(ResultBlob) = %d, want 1100", len(out.ResultBlob))
		}
		if !reflect.DeepEqual(out.ResultBlob[:len(frame)], frame) {
			t.Errorf("ResultBlob[:frame_len] does not match frame")
		}
		for i := len(frame); i < 1000; i++ {
			if out.ResultBlob[i] != 0 {
				t.Fatalf("ResultBlob[%d] = %d, want 0 (zeroed gap)", i, out.ResultBlob[i])
			}
		}
		if !reflect.DeepEqual(out.ResultBlob[1000:1004], segBytes) {
			t.Errorf("ResultBlob[1000:1004] = %x, want %x (data segment)", out.ResultBlob[1000:1004], segBytes)
		}
	})

	t.Run("data segment at offset 0 shorter than the frame", func(t *testing.T) {
		segBytes := []byte{0xFF, 0xFF}
		body := appendVarS64([]byte{0x42}, int64(10))
		body = append(body, 0x0B)
		instrumented, codeHash := buildAndInstrument(t, buildModule(nil, &dataSeg{offset: 0, bytes: segBytes}, []byte{0x00}, body, 1))

		e := newTestEngine(t)
		hc := testHostContext(t, newTestState(t, nil))

		frame := []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
		out, err := e.Run(hc, codeHash, instrumented, frame, 1_000_000)
		if err != nil {
			t.Fatalf("Run: %v", err)
		}
		if out.Kind != Returned {
			t.Fatalf("Kind = %v, want Returned", out.Kind)
		}
		if !reflect.DeepEqual(out.ResultBlob, frame) {
			t.Errorf("ResultBlob = %v, want %v (frame overwrites the segment)", out.ResultBlob, frame)
		}
	})
}

func TestRunFreshMemoryPerInput(t *testing.T) {
	body := appendVarS64([]byte{0x42}, int64(20))
	body = append(body, 0x0B)
	instrumented, codeHash := buildAndInstrument(t, buildModule(nil, nil, []byte{0x00}, body, 1))

	e := newTestEngine(t)
	hc := testHostContext(t, newTestState(t, nil))

	frame1 := []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20}
	out1, err := e.Run(hc, codeHash, instrumented, frame1, 1_000_000)
	if err != nil {
		t.Fatalf("Run 1: %v", err)
	}
	if !reflect.DeepEqual(out1.ResultBlob, frame1) {
		t.Fatalf("ResultBlob 1 = %v, want %v", out1.ResultBlob, frame1)
	}

	frame2 := []byte{9, 9, 9}
	out2, err := e.Run(hc, codeHash, instrumented, frame2, 1_000_000)
	if err != nil {
		t.Fatalf("Run 2: %v", err)
	}
	if len(out2.ResultBlob) != 20 {
		t.Fatalf("len(ResultBlob 2) = %d, want 20", len(out2.ResultBlob))
	}
	if !reflect.DeepEqual(out2.ResultBlob[:3], frame2) {
		t.Errorf("ResultBlob 2[:3] = %v, want %v", out2.ResultBlob[:3], frame2)
	}
	for i := 3; i < 20; i++ {
		if out2.ResultBlob[i] != 0 {
			t.Fatalf("ResultBlob 2[%d] = %d, want 0 (no residue of frame 1)", i, out2.ResultBlob[i])
		}
	}
}

func TestRunFrameLongerThanMemory(t *testing.T) {
	body := []byte{0x42, 0x2A, 0x0B}
	instrumented, codeHash := buildAndInstrument(t, buildModule(nil, nil, []byte{0x00}, body, 1))

	e := newTestEngine(t)
	hc := testHostContext(t, newTestState(t, nil))

	frame := make([]byte, 70000) // over the one declared page (65536 bytes)
	out, err := e.Run(hc, codeHash, instrumented, frame, 1_000_000)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if out.Kind != Trapped {
		t.Fatalf("Kind = %v, want Trapped", out.Kind)
	}
	want := module.InitialMemoryCost(1)
	if out.GasUsed != want {
		t.Fatalf("GasUsed = %d, want %d (the initial-memory charge, and nothing else)", out.GasUsed, want)
	}
}

func TestRunBudgetClampedToInt64(t *testing.T) {
	body := []byte{0x42, 0x2A, 0x0B}
	instrumented, codeHash := buildAndInstrument(t, buildModule(nil, nil, []byte{0x00}, body, 1))

	e := newTestEngine(t)
	hc := testHostContext(t, newTestState(t, nil))

	out, err := e.Run(hc, codeHash, instrumented, nil, math.MaxUint64)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if out.Kind != Returned {
		t.Fatalf("Kind = %v, want Returned (clamp), got %v", out.Kind, out)
	}
}

func TestRunWarmAndColdCacheAgree(t *testing.T) {
	body := []byte{0x42, 0x2A, 0x0B}
	instrumented, codeHash := buildAndInstrument(t, buildModule(nil, nil, []byte{0x00}, body, 1))

	e := newTestEngine(t)
	hc := testHostContext(t, newTestState(t, nil))

	cold, err := e.Run(hc, codeHash, instrumented, nil, 1_000_000)
	if err != nil {
		t.Fatalf("cold Run: %v", err)
	}
	cacheLen := len(e.cache)

	warm, err := e.Run(hc, codeHash, instrumented, nil, 1_000_000)
	if err != nil {
		t.Fatalf("warm Run: %v", err)
	}
	if len(e.cache) != cacheLen {
		t.Errorf("len(cache) changed on warm call: %d -> %d", cacheLen, len(e.cache))
	}
	if !reflect.DeepEqual(cold, warm) {
		t.Errorf("cold Outcome %+v != warm Outcome %+v", cold, warm)
	}
}

// distinctModules returns n instrumented fixtures with their CodeHashes, differing only in a
// body constant so each hashes distinctly.
func distinctModules(t *testing.T, n int) []struct {
	instrumented []byte
	codeHash     types.Hash
} {
	t.Helper()
	out := make([]struct {
		instrumented []byte
		codeHash     types.Hash
	}, n)
	for i := 0; i < n; i++ {
		body := appendVarS64([]byte{0x42}, int64(i))
		body = append(body, 0x0B)
		instrumented, codeHash := buildAndInstrument(t, buildModule(nil, nil, []byte{0x00}, body, 1))
		out[i].instrumented = instrumented
		out[i].codeHash = codeHash
	}
	return out
}

func TestCompiledCacheEvictsByCount(t *testing.T) {
	e := newTestEngine(t)
	hc := testHostContext(t, newTestState(t, nil))

	type entry struct {
		instrumented []byte
		codeHash     types.Hash
		want         Outcome
	}
	var entries []entry
	for _, m := range distinctModules(t, e.maxCacheEntries+2) {
		out, err := e.Run(hc, m.codeHash, m.instrumented, nil, 1_000_000)
		if err != nil {
			t.Fatalf("Run: %v", err)
		}
		entries = append(entries, entry{m.instrumented, m.codeHash, out})
	}

	if len(e.cache) != e.maxCacheEntries {
		t.Fatalf("len(cache) = %d, want %d", len(e.cache), e.maxCacheEntries)
	}
	if len(e.order) != len(e.cache) {
		t.Fatalf("len(order) = %d, want %d", len(e.order), len(e.cache))
	}

	first := entries[0]
	if _, ok := e.cache[first.codeHash]; ok {
		t.Fatalf("first module should have been evicted")
	}
	got, err := e.Run(hc, first.codeHash, first.instrumented, nil, 1_000_000)
	if err != nil {
		t.Fatalf("re-Run evicted module: %v", err)
	}
	if !reflect.DeepEqual(got, first.want) {
		t.Errorf("evicted module Outcome %+v != original %+v", got, first.want)
	}
}

func TestCompiledCacheEvictsByBytes(t *testing.T) {
	e := newTestEngine(t)
	hc := testHostContext(t, newTestState(t, nil))

	modules := distinctModules(t, 4)
	out0, err := e.Run(hc, modules[0].codeHash, modules[0].instrumented, nil, 1_000_000)
	if err != nil {
		t.Fatalf("Run 0: %v", err)
	}
	oneSize := e.cache[modules[0].codeHash].size

	// Lower maxCacheBytes to just under three fixtures' size, well clear of the count bound.
	e.maxCacheBytes = 3*oneSize - 1

	for i := 1; i < len(modules); i++ {
		if _, err := e.Run(hc, modules[i].codeHash, modules[i].instrumented, nil, 1_000_000); err != nil {
			t.Fatalf("Run %d: %v", i, err)
		}
	}

	if e.bytes > e.maxCacheBytes {
		t.Fatalf("e.bytes = %d, want <= %d", e.bytes, e.maxCacheBytes)
	}
	if len(e.cache) != 2 {
		t.Fatalf("len(cache) = %d, want 2 (byte bound evicted, not the count bound)", len(e.cache))
	}
	var sum int
	for _, entry := range e.cache {
		sum += entry.size
	}
	if sum != e.bytes {
		t.Fatalf("sum of surviving entry sizes = %d, want e.bytes = %d", sum, e.bytes)
	}

	got, err := e.Run(hc, modules[0].codeHash, modules[0].instrumented, nil, 1_000_000)
	if err != nil {
		t.Fatalf("re-Run evicted module: %v", err)
	}
	if !reflect.DeepEqual(got, out0) {
		t.Errorf("evicted module Outcome %+v != original %+v", got, out0)
	}
}

func TestCompiledCacheNeverEvictsJustInsertedEntry(t *testing.T) {
	e := newTestEngine(t)
	hc := testHostContext(t, newTestState(t, nil))

	modules := distinctModules(t, 2)
	a, b := modules[0], modules[1]

	outA, err := e.Run(hc, a.codeHash, a.instrumented, nil, 1_000_000)
	if err != nil {
		t.Fatalf("Run A: %v", err)
	}
	aSize := e.cache[a.codeHash].size

	// A single entry's own size alone exceeds the byte budget: the eviction loop must not evict
	// the entry it just inserted, degrading to a one-entry cache instead of leaving e.cache
	// pointing at a closed CompiledModule.
	e.maxCacheBytes = aSize - 1

	if _, ok := e.cache[a.codeHash]; !ok {
		t.Fatalf("A should still be cached immediately after insertion, even over budget")
	}

	gotA, err := e.Run(hc, a.codeHash, a.instrumented, nil, 1_000_000)
	if err != nil {
		t.Fatalf("re-Run A (warm, over budget): %v", err)
	}
	if !reflect.DeepEqual(gotA, outA) {
		t.Errorf("warm over-budget Outcome %+v != original %+v", gotA, outA)
	}

	// A second, distinct module still evicts A rather than accumulating: the guard only protects
	// the entry just inserted, not every entry once the budget is impossible to satisfy.
	if _, err := e.Run(hc, b.codeHash, b.instrumented, nil, 1_000_000); err != nil {
		t.Fatalf("Run B: %v", err)
	}
	if len(e.cache) != 1 {
		t.Fatalf("len(cache) = %d, want 1 (only the just-inserted entry survives)", len(e.cache))
	}
	if _, ok := e.cache[b.codeHash]; !ok {
		t.Errorf("B should be cached (just inserted)")
	}
	if _, ok := e.cache[a.codeHash]; ok {
		t.Errorf("A should have been evicted once B was inserted")
	}
}

func TestCompiledCacheEvictsLRU(t *testing.T) {
	e := newTestEngine(t)
	e.maxCacheEntries = 2
	hc := testHostContext(t, newTestState(t, nil))

	modules := distinctModules(t, 3)
	a, b, c := modules[0], modules[1], modules[2]

	if _, err := e.Run(hc, a.codeHash, a.instrumented, nil, 1_000_000); err != nil {
		t.Fatalf("Run A: %v", err)
	}
	if _, err := e.Run(hc, b.codeHash, b.instrumented, nil, 1_000_000); err != nil {
		t.Fatalf("Run B: %v", err)
	}
	if _, err := e.Run(hc, a.codeHash, a.instrumented, nil, 1_000_000); err != nil {
		t.Fatalf("Run A again: %v", err)
	}
	if _, err := e.Run(hc, c.codeHash, c.instrumented, nil, 1_000_000); err != nil {
		t.Fatalf("Run C: %v", err)
	}

	if _, ok := e.cache[a.codeHash]; !ok {
		t.Errorf("A should still be cached (recently used)")
	}
	if _, ok := e.cache[b.codeHash]; ok {
		t.Errorf("B should have been evicted (least recently used), not A")
	}
	if _, ok := e.cache[c.codeHash]; !ok {
		t.Errorf("C should be cached")
	}
}

func TestRunRebuildsRuntime(t *testing.T) {
	e := newTestEngine(t)
	e.compilesPerRebuild = 2
	hc := testHostContext(t, newTestState(t, nil))
	rt0 := e.rt

	modules := distinctModules(t, 3)
	a, b, c := modules[0], modules[1], modules[2]

	wantA, err := e.Run(hc, a.codeHash, a.instrumented, nil, 1_000_000)
	if err != nil {
		t.Fatalf("Run A: %v", err)
	}

	if _, err := e.Run(hc, a.codeHash, a.instrumented, nil, 1_000_000); err != nil {
		t.Fatalf("Run A (hit): %v", err)
	}
	if _, err := e.Run(hc, b.codeHash, b.instrumented, nil, 1_000_000); err != nil {
		t.Fatalf("Run B: %v", err)
	}
	if _, ok := e.cache[a.codeHash]; !ok {
		t.Errorf("cache should still hold A after A, A, B (second A was a hit, no rebuild yet)")
	}
	if _, ok := e.cache[b.codeHash]; !ok {
		t.Errorf("cache should still hold B after A, A, B")
	}

	if _, err := e.Run(hc, c.codeHash, c.instrumented, nil, 1_000_000); err != nil {
		t.Fatalf("Run C: %v", err)
	}
	if e.compiles != 1 {
		t.Errorf("e.compiles = %d, want 1 (rebuilt before compiling C)", e.compiles)
	}
	if len(e.cache) != 1 {
		t.Fatalf("len(cache) = %d, want 1 (only C, post-rebuild)", len(e.cache))
	}
	if entry, ok := e.cache[c.codeHash]; !ok {
		t.Errorf("cache should hold C")
	} else if e.bytes != entry.size {
		t.Errorf("e.bytes = %d, want %d (C's size)", e.bytes, entry.size)
	}
	if e.rt == rt0 {
		t.Errorf("e.rt was not replaced by the rebuild")
	}

	gotA, err := e.Run(hc, a.codeHash, a.instrumented, nil, 1_000_000)
	if err != nil {
		t.Fatalf("Run A (cold, post-rebuild): %v", err)
	}
	if !reflect.DeepEqual(gotA, wantA) {
		t.Errorf("post-rebuild Outcome %+v != pre-rebuild %+v", gotA, wantA)
	}

	// The env module was re-instantiated on the new runtime.
	slInstrumented, slCodeHash := buildAndInstrument(t, buildModule([]string{"storage_len"}, nil, []byte{0x00}, storageLenBody(0), 1))
	out, err := e.Run(hc, slCodeHash, slInstrumented, nil, 1_000_000)
	if err != nil {
		t.Fatalf("Run storage_len post-rebuild: %v", err)
	}
	if out.Kind != Returned || out.Ret != -1 {
		t.Fatalf("Kind/Ret = %v/%d, want Returned/-1", out.Kind, out.Ret)
	}

	if err := e.Close(); err != nil {
		t.Errorf("Close after rebuild: %v", err)
	}
}

func TestRunDepthCeiling(t *testing.T) {
	instrumented, codeHash := buildAndInstrument(t, buildModule(nil, nil, []byte{0x00}, depthCeilingBody(), 1))

	e := newTestEngine(t)
	hc := testHostContext(t, newTestState(t, nil))

	frame511 := make([]byte, 511)
	out, err := e.Run(hc, codeHash, instrumented, frame511, 1_000_000_000)
	if err != nil {
		t.Fatalf("Run(511): %v", err)
	}
	if out.Kind != Returned {
		t.Fatalf("Kind(511) = %v, want Returned", out.Kind)
	}

	frame512 := make([]byte, 512)
	out2a, err := e.Run(hc, codeHash, instrumented, frame512, 1_000_000_000)
	if err != nil {
		t.Fatalf("Run(512) a: %v", err)
	}
	if out2a.Kind != Trapped {
		t.Fatalf("Kind(512) = %v, want Trapped (not OutOfGas)", out2a.Kind)
	}

	out2b, err := e.Run(hc, codeHash, instrumented, frame512, 1_000_000_000)
	if err != nil {
		t.Fatalf("Run(512) b: %v", err)
	}
	if out2a.GasUsed != out2b.GasUsed {
		t.Errorf("GasUsed(512) not deterministic: %d != %d", out2a.GasUsed, out2b.GasUsed)
	}
}

func TestRunGrowLimiter(t *testing.T) {
	instrumented, codeHash := buildAndInstrument(t, buildModule(nil, nil, []byte{0x00}, growLimiterBody(), 1))

	e := newTestEngine(t)
	hc := testHostContext(t, newTestState(t, nil))

	memCost := module.InitialMemoryCost(1)
	segTotal := opsCost(t, 0x20, 0x40, 0x1A, 0x3F, 0xAD, 0x0B)

	t.Run("refused", func(t *testing.T) {
		frame := make([]byte, 100)
		out, err := e.Run(hc, codeHash, instrumented, frame, 1_000_000)
		if err != nil {
			t.Fatalf("Run: %v", err)
		}
		if out.Kind != Returned || out.Ret != 1 {
			t.Fatalf("Kind/Ret = %v/%d, want Returned/1", out.Kind, out.Ret)
		}
		want := memCost + segTotal
		if out.GasUsed != want {
			t.Errorf("GasUsed = %d, want %d (no per-page surcharge)", out.GasUsed, want)
		}
	})

	t.Run("succeeds", func(t *testing.T) {
		frame := make([]byte, 1)
		out, err := e.Run(hc, codeHash, instrumented, frame, 1_000_000)
		if err != nil {
			t.Fatalf("Run: %v", err)
		}
		if out.Kind != Returned || out.Ret != 2 {
			t.Fatalf("Kind/Ret = %v/%d, want Returned/2", out.Kind, out.Ret)
		}
		want := memCost + segTotal + module.MemoryGrowPageCost(1)
		if out.GasUsed != want {
			t.Errorf("GasUsed = %d, want %d", out.GasUsed, want)
		}
	})
}

func TestRunNegativeReturnPassesThrough(t *testing.T) {
	// A negative Ret is the plugin's REVERT signal; the engine only
	// passes it through untouched, with no attempted result-pointer
	// decode.
	body := appendVarS64([]byte{0x42}, int64(-5))
	body = append(body, 0x0B)
	instrumented, codeHash := buildAndInstrument(t, buildModule(nil, nil, []byte{0x00}, body, 1))

	e := newTestEngine(t)
	hc := testHostContext(t, newTestState(t, nil))

	out, err := e.Run(hc, codeHash, instrumented, nil, 1_000_000)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if out.Kind != Returned {
		t.Fatalf("Kind = %v, want Returned", out.Kind)
	}
	if out.Ret != -5 {
		t.Fatalf("Ret = %d, want -5", out.Ret)
	}
	if out.ResultBlob != nil {
		t.Errorf("ResultBlob = %v, want nil", out.ResultBlob)
	}
}

func TestRunResultPointerOutOfBounds(t *testing.T) {
	// (ptr<<32)|len with ptr past the one declared page.
	packed := (int64(70000) << 32) | 1
	body := appendVarS64([]byte{0x42}, packed)
	body = append(body, 0x0B)
	instrumented, codeHash := buildAndInstrument(t, buildModule(nil, nil, []byte{0x00}, body, 1))

	e := newTestEngine(t)
	hc := testHostContext(t, newTestState(t, nil))

	out, err := e.Run(hc, codeHash, instrumented, nil, 1_000_000)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if out.Kind != Trapped {
		t.Fatalf("Kind = %v, want Trapped", out.Kind)
	}
	if out.ResultBlob != nil {
		t.Errorf("ResultBlob = %v, want nil", out.ResultBlob)
	}
}

func TestRunResultBlobIsWholeRange(t *testing.T) {
	// ptr=0, len=60000: the whole ContractEffects byte range the plugin
	// decodes, never shortened.
	body := appendVarS64([]byte{0x42}, int64(60000))
	body = append(body, 0x0B)
	instrumented, codeHash := buildAndInstrument(t, buildModule(nil, nil, []byte{0x00}, body, 1))

	e := newTestEngine(t)
	hc := testHostContext(t, newTestState(t, nil))

	frame := make([]byte, 500)
	for i := range frame {
		frame[i] = byte(i + 1)
	}
	out, err := e.Run(hc, codeHash, instrumented, frame, 10_000_000)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if out.Kind != Returned {
		t.Fatalf("Kind = %v, want Returned", out.Kind)
	}
	if uint32(out.Ret) != 60000 {
		t.Fatalf("uint32(Ret) = %d, want 60000", uint32(out.Ret))
	}
	if len(out.ResultBlob) != 60000 {
		t.Fatalf("len(ResultBlob) = %d, want 60000 (never truncated)", len(out.ResultBlob))
	}
	if !reflect.DeepEqual(out.ResultBlob[:len(frame)], frame) {
		t.Errorf("ResultBlob[:frame_len] does not match frame")
	}
	for i := len(frame); i < 60000; i++ {
		if out.ResultBlob[i] != 0 {
			t.Fatalf("ResultBlob[%d] = %d, want 0", i, out.ResultBlob[i])
		}
	}
}

// The two guard tests below are the deliberate exception to "every fixture is validated": their
// fixtures exist precisely to exercise what happens when validation is bypassed.

func TestRunWithoutMemoryExportHalts(t *testing.T) {
	// No memory section, exporting only run. requireCoreExports would
	// reject this; mod.Memory() on it is a typed nil, so an unguarded
	// Write would nil-deref.
	var raw []byte
	raw = append(raw, wasmHeader...)

	typeSec := appendVarU32(nil, 1)
	typeSec = append(typeSec, encodeFuncType([]byte{0x7F, 0x7F}, []byte{0x7E})...)
	raw = append(raw, section(1, typeSec)...)

	raw = append(raw, section(3, []byte{0x01, 0x00})...)

	exportSec := []byte{0x01}
	exportSec = appendName(exportSec, "run")
	exportSec = append(exportSec, 0x00, 0x00)
	raw = append(raw, section(7, exportSec)...)

	body := []byte{0x00, 0x42, 0x00, 0x0B} // 0 locals; i64.const 0; end
	codeSec := appendVarU32([]byte{0x01}, uint32(len(body)))
	codeSec = append(codeSec, body...)
	raw = append(raw, section(10, codeSec)...)

	codeHash := codeHashOf(raw)
	e := newTestEngine(t)
	hc := testHostContext(t, newTestState(t, nil))

	got, err := e.Run(hc, codeHash, raw, nil, 1_000_000)
	if err == nil {
		t.Fatal("Run err = nil, want non-nil")
	}
	if !reflect.DeepEqual(got, Outcome{}) {
		t.Errorf("Outcome = %+v, want zero", got)
	}
}

func TestRunWithNonSingleResultHalts(t *testing.T) {
	// run typed (i32,i32) -> (), put through module.Instrument directly (so the ledger
	// accessors exist) but not through Validate, which requires (i32,i32) -> i64. Call succeeds
	// with a zero-length results, so an unguarded results[0] would index out of range.
	var raw []byte
	raw = append(raw, wasmHeader...)

	typeSec := appendVarU32(nil, 1)
	typeSec = append(typeSec, encodeFuncType([]byte{0x7F, 0x7F}, nil)...)
	raw = append(raw, section(1, typeSec)...)

	raw = append(raw, section(3, []byte{0x01, 0x00})...)
	raw = append(raw, section(5, []byte{0x01, 0x00, 0x01})...)

	exportSec := []byte{0x02}
	exportSec = appendName(exportSec, "memory")
	exportSec = append(exportSec, 0x02, 0x00)
	exportSec = appendName(exportSec, "run")
	exportSec = append(exportSec, 0x00, 0x00)
	raw = append(raw, section(7, exportSec)...)

	body := []byte{0x00, 0x0B} // 0 locals; end
	codeSec := appendVarU32([]byte{0x01}, uint32(len(body)))
	codeSec = append(codeSec, body...)
	raw = append(raw, section(10, codeSec)...)

	instrumented, err := module.Instrument(raw, testLimits)
	if err != nil {
		t.Fatalf("Instrument: %v", err)
	}
	codeHash := codeHashOf(instrumented)

	e := newTestEngine(t)
	hc := testHostContext(t, newTestState(t, nil))

	got, err := e.Run(hc, codeHash, instrumented, nil, 1_000_000)
	if err == nil {
		t.Fatal("Run err = nil, want non-nil")
	}
	if !reflect.DeepEqual(got, Outcome{}) {
		t.Errorf("Outcome = %+v, want zero", got)
	}
}
