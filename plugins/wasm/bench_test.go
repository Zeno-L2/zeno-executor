package wasm

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/zenon-network/go-zenon/common/types"

	"github.com/Zeno-L2/zeno-executor/domain"
	"github.com/Zeno-L2/zeno-executor/internal/codec"
	"github.com/Zeno-L2/zeno-executor/plugins/wasm/internal/engine"
	"github.com/Zeno-L2/zeno-executor/plugins/wasm/internal/module"
)

// calibrationReps is the repeat count each per-opcode calibration body loops its pattern, kept
// large enough that the one-time instantiation cost calibration's ns/gas overstates by (edge
// cases) stays small.
const calibrationReps = 32768

// benchModule is one instrumented module ready for engine.Run: its bytes and its CodeHash.
type benchModule struct {
	inst []byte
	hash types.Hash
}

// instrumentFor validates and instruments raw at the plugin's structural limits, failing the
// benchmark on any error.
func instrumentFor(b *testing.B, raw []byte) benchModule {
	b.Helper()
	if err := module.Validate(raw, moduleLimits()); err != nil {
		b.Fatalf("Validate: %v", err)
	}
	inst, err := module.Instrument(raw, moduleLimits())
	if err != nil {
		b.Fatalf("Instrument: %v", err)
	}
	return benchModule{inst: inst, hash: CodeHash(inst)}
}

// benchFrame returns the encoded input frame every benchmark's run call receives.
func benchFrame(b *testing.B) []byte {
	b.Helper()
	frame, err := buildFrame(types.Hash{}, domain.InputContext{}, callArgs{}, types.Hash{}).Encode()
	if err != nil {
		b.Fatalf("buildFrame: Encode: %v", err)
	}
	return frame
}

// newBenchEngine returns an Engine closed in b.Cleanup.
func newBenchEngine(b *testing.B) *engine.Engine {
	b.Helper()
	eng, err := engine.New()
	if err != nil {
		b.Fatalf("engine.New: %v", err)
	}
	b.Cleanup(func() { _ = eng.Close() })
	return eng
}

// boundModules returns n instrumented variants at MaxInstrumentedCodeSize: a chain of
// unreachable instructions whose length is binary-searched to the largest pre-instrumentation
// size that still validates and instruments, then repeated for v in [0, n) so each variant's
// bytes, and so its CodeHash, differ.
func boundModules(b *testing.B, n int) []benchModule {
	b.Helper()

	rawAt := func(k int, v int64) []byte {
		body := []byte{0x41}
		body = appendVarS64(body, v)
		body = append(body, 0x1A)
		body = append(body, bytes.Repeat([]byte{0x00}, k)...)
		body = append(body, 0x0B)
		return buildWasmModule(runModuleSpec(nil, 0, []byte{0x00}, body, nil))
	}
	fits := func(k int) bool {
		raw := rawAt(k, 0)
		if err := module.Validate(raw, moduleLimits()); err != nil {
			return false
		}
		if _, err := module.Instrument(raw, moduleLimits()); err != nil {
			return false
		}
		return true
	}

	lo, hi := 1, MaxCodeSize
	for lo < hi {
		mid := (lo + hi + 1) / 2
		if fits(mid) {
			lo = mid
		} else {
			hi = mid - 1
		}
	}
	k := lo

	boundary := instrumentFor(b, rawAt(k, 0))
	if len(boundary.inst) < MaxInstrumentedCodeSize-64 {
		b.Fatalf("boundModules: k=%d instrumented len=%d, want >= %d", k, len(boundary.inst), MaxInstrumentedCodeSize-64)
	}

	mods := make([]benchModule, n)
	for v := 0; v < n; v++ {
		mods[v] = instrumentFor(b, rawAt(k, int64(v)))
	}
	return mods
}

// manyFunctionsModules returns n instrumented variants at MaxInstrumentedCodeSize built as nf
// small functions, each its own unreachable chain, rather than boundModules' one giant function.
// unreachable is a segment terminator (plugins/wasm/internal/module/instrument_test.go's
// TestInstrumentWorstCaseExpansion), so a chain of them already maximizes basic-block count per
// byte; this shape holds that block count roughly fixed and instead maximizes function-boundary
// count, to compare per-function compilation overhead against per-block instrumentation overhead
// at the same instrumented-size budget. Only variant 0's first function carries a distinguishing
// i32.const immediate, so CodeHash differs per variant without materially changing total size.
func manyFunctionsModules(b *testing.B, nf, n int) []benchModule {
	b.Helper()

	rawAt := func(perFunc int, v int64) []byte {
		funcs := make([]wasmFunc, nf)
		for i := 1; i < nf; i++ {
			funcs[i] = wasmFunc{TypeIdx: 0, Locals: []byte{0x00}, Body: append(bytes.Repeat([]byte{0x00}, perFunc), 0x0B)}
		}
		body0 := []byte{0x41}
		body0 = appendVarS64(body0, v)
		body0 = append(body0, 0x1A)
		body0 = append(body0, bytes.Repeat([]byte{0x00}, perFunc)...)
		body0 = append(body0, 0x0B)
		funcs[0] = wasmFunc{TypeIdx: 0, Locals: []byte{0x00}, Body: body0}

		spec := wasmModuleSpec{
			Types:       []wasmFuncType{{Params: []byte{0x7F, 0x7F}, Results: []byte{0x7E}}},
			Funcs:       funcs,
			MemoryPages: 0,
			Exports: []wasmExport{
				{Name: "memory", Kind: 0x02, Index: 0},
				{Name: "run", Kind: 0x00, Index: 0},
			},
		}
		return buildWasmModule(spec)
	}
	fits := func(perFunc int) bool {
		raw := rawAt(perFunc, 0)
		if err := module.Validate(raw, moduleLimits()); err != nil {
			return false
		}
		if _, err := module.Instrument(raw, moduleLimits()); err != nil {
			return false
		}
		return true
	}

	lo, hi := 1, MaxCodeSize/nf+1
	for lo < hi {
		mid := (lo + hi + 1) / 2
		if fits(mid) {
			lo = mid
		} else {
			hi = mid - 1
		}
	}
	perFunc := lo

	// nf separate functions carry more fixed framing overhead (a type index and a code-length
	// varint each) than boundModules' single function, so this shape cannot reach as tight a
	// tolerance of MaxInstrumentedCodeSize; 95% still leaves the two shapes comparable at
	// materially the same size.
	boundary := instrumentFor(b, rawAt(perFunc, 0))
	if len(boundary.inst) < MaxInstrumentedCodeSize*95/100 {
		b.Fatalf("manyFunctionsModules: nf=%d perFunc=%d instrumented len=%d, want >= %d", nf, perFunc, len(boundary.inst), MaxInstrumentedCodeSize*95/100)
	}

	mods := make([]benchModule, n)
	for v := 0; v < n; v++ {
		mods[v] = instrumentFor(b, rawAt(perFunc, int64(v)))
	}
	return mods
}

// BenchmarkCalibrateNsPerGas times four opcode patterns against the gas the real engine
// metered for them, to calibrate the table's ns-per-gas-unit against wall time.
func BenchmarkCalibrateNsPerGas(b *testing.B) {
	repeat := func(pattern []byte) []byte {
		body := make([]byte, 0, len(pattern)*calibrationReps+3)
		for i := 0; i < calibrationReps; i++ {
			body = append(body, pattern...)
		}
		return append(body, 0x42, 0x00, 0x0B)
	}

	cost1 := runModuleSpec(nil, 1, []byte{0x00}, repeat([]byte{0x20, 0x00, 0x41, 0x01, 0x6A, 0x21, 0x00}), nil)

	cost2 := runModuleSpec(nil, 1, []byte{0x00}, repeat([]byte{0x23, 0x00, 0x23, 0x00, 0x6C, 0x24, 0x00}), nil)
	cost2.Globals = 1

	cost5 := runModuleSpec(nil, 1, []byte{0x00}, repeat([]byte{0x20, 0x01, 0x20, 0x01, 0x6E, 0x1A}), nil)

	cost10 := runModuleSpec(nil, 1, []byte{0x00}, repeat([]byte{0x41, 0x00, 0x11, 0x01, 0x00}), nil)
	cost10.Types = append(cost10.Types, wasmFuncType{})
	cost10.Funcs = append(cost10.Funcs, wasmFunc{TypeIdx: 1, Locals: []byte{0x00}, Body: []byte{0x0B}})
	cost10.TableMin = 1
	cost10.Elements = []wasmElem{{Offset: 0, Funcs: []uint32{1}}}

	children := []struct {
		name string
		spec wasmModuleSpec
	}{
		{"cost-1", cost1},
		{"cost-2", cost2},
		{"cost-5", cost5},
		{"cost-10", cost10},
	}

	eng := newBenchEngine(b)
	frame := benchFrame(b)

	for _, c := range children {
		m := instrumentFor(b, buildWasmModule(c.spec))

		out, err := eng.Run(engine.HostContext{State: &mapState{}}, m.hash, m.inst, frame, callPlasmaLimit)
		if err != nil {
			b.Fatalf("%s: warm-up Run: %v", c.name, err)
		}
		if out.Kind != engine.Returned {
			b.Fatalf("%s: warm-up Run: Kind = %v, want Returned", c.name, out.Kind)
		}
		gas := out.GasUsed - module.InitialMemoryCost(1)

		b.Run(c.name, func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := eng.Run(engine.HostContext{State: &mapState{}}, m.hash, m.inst, frame, callPlasmaLimit); err != nil {
					b.Fatal(err)
				}
			}
			b.ReportMetric(float64(gas), "gas/op")
			b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N)/float64(gas), "ns/gas")
		})
	}
}

// BenchmarkResultBlobAtBound times settlement of a returned blob of one page minus the input
// frame, len(frame) bytes short of MaxResultBlob, packed with the maximum number of distinct
// StateDiff entries a one-page module's returned blob can carry.
func BenchmarkResultBlobAtBound(b *testing.B) {
	frame := benchFrame(b)
	l := (1 << 16) - len(frame)

	base, err := (codec.ContractEffects{Version: codec.ExecutionResultVersion, GlobalInputIndex: 42, ClaimedDeposit: u256(1)}).Encode()
	if err != nil {
		b.Fatalf("Encode: %v", err)
	}

	n := (l - len(base) - 8 - 256*9) / 10
	rem := (l - len(base) - 8 - 256*9) % 10

	entries := make([]codec.LocalStateDiffEntry, 0, 1+256+n)
	entries = append(entries, codec.LocalStateDiffEntry{})
	for i := 0; i < 256; i++ {
		entries = append(entries, codec.LocalStateDiffEntry{LocalKey: []byte{byte(i)}})
	}
	for k, added := 0, 0; added < n; k++ {
		key := []byte{byte(k >> 8), byte(k)}
		if reserved(key) {
			continue
		}
		entries = append(entries, codec.LocalStateDiffEntry{LocalKey: key})
		added++
	}

	blob, err := (codec.ContractEffects{
		Version:          codec.ExecutionResultVersion,
		GlobalInputIndex: 42,
		StateDiff:        entries,
		ReturnData:       make([]byte, rem),
		ClaimedDeposit:   u256(1),
	}).Encode()
	if err != nil {
		b.Fatalf("Encode: %v", err)
	}
	if len(blob) != l {
		b.Fatalf("len(blob) = %d, want %d", len(blob), l)
	}

	m := instrumentFor(b, buildWasmModule(runModuleSpec(nil, 1, []byte{0x00}, returnBlobBody(uint32(len(frame)), l), []dataSeg{{offset: uint32(len(frame)), bytes: blob}})))

	eng := newBenchEngine(b)
	out, err := eng.Run(engine.HostContext{State: &mapState{}}, m.hash, m.inst, frame, callPlasmaLimit)
	if err != nil {
		b.Fatalf("warm-up Run: %v", err)
	}
	if out.Kind != engine.Returned {
		b.Fatalf("warm-up Run: Kind = %v, want Returned", out.Kind)
	}
	if len(out.ResultBlob) != l {
		b.Fatalf("warm-up Run: len(ResultBlob) = %d, want %d", len(out.ResultBlob), l)
	}
	if !bytes.Equal(out.ResultBlob, blob) {
		b.Fatalf("warm-up Run: ResultBlob differs from the encoded blob")
	}

	s := settlement{ContractID: repeatedHash(0x33), CodeHash: m.hash, GlobalInputIndex: 42, GasBudget: callPlasmaLimit}
	warmEff, err := s.settle(out)
	if err != nil {
		b.Fatalf("warm-up settle: %v", err)
	}
	if warmEff.Status != codec.StatusRuntimeFault {
		b.Fatalf("warm-up settle: Status = %v, want StatusRuntimeFault", warmEff.Status)
	}
	if warmEff.GasUsed != out.GasUsed {
		b.Fatalf("warm-up settle: GasUsed = %d, want %d", warmEff.GasUsed, out.GasUsed)
	}
	gas := out.GasUsed

	b.Run("min-entries", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if _, err := s.settle(out); err != nil {
				b.Fatal(err)
			}
		}
		b.ReportMetric(float64(gas), "gas/op")
	})
}

// BenchmarkResultBlobAtTrueBound times settlement of a returned blob of
// exactly MaxResultBlob bytes (64 KiB), the literal consensus bound,
// packed with the maximum number of distinct StateDiff entries a
// two-page module's returned blob can carry. This is the true-bound
// sibling of BenchmarkResultBlobAtBound, which uses the cheaper
// one-page shape len(frame) bytes short of the bound instead.
func BenchmarkResultBlobAtTrueBound(b *testing.B) {
	frame := benchFrame(b)
	l := MaxResultBlob

	base, err := (codec.ContractEffects{Version: codec.ExecutionResultVersion, GlobalInputIndex: 42, ClaimedDeposit: u256(1)}).Encode()
	if err != nil {
		b.Fatalf("Encode: %v", err)
	}

	n := (l - len(base) - 8 - 256*9) / 10
	rem := (l - len(base) - 8 - 256*9) % 10

	entries := make([]codec.LocalStateDiffEntry, 0, 1+256+n)
	entries = append(entries, codec.LocalStateDiffEntry{})
	for i := 0; i < 256; i++ {
		entries = append(entries, codec.LocalStateDiffEntry{LocalKey: []byte{byte(i)}})
	}
	for k, added := 0, 0; added < n; k++ {
		key := []byte{byte(k >> 8), byte(k)}
		if reserved(key) {
			continue
		}
		entries = append(entries, codec.LocalStateDiffEntry{LocalKey: key})
		added++
	}

	blob, err := (codec.ContractEffects{
		Version:          codec.ExecutionResultVersion,
		GlobalInputIndex: 42,
		StateDiff:        entries,
		ReturnData:       make([]byte, rem),
		ClaimedDeposit:   u256(1),
	}).Encode()
	if err != nil {
		b.Fatalf("Encode: %v", err)
	}
	if len(blob) != l {
		b.Fatalf("len(blob) = %d, want %d", len(blob), l)
	}

	pages := uint32((uint64(len(frame)) + uint64(l) + 65535) / 65536)
	m := instrumentFor(b, buildWasmModule(runModuleSpec(nil, pages, []byte{0x00}, returnBlobBody(uint32(len(frame)), l), []dataSeg{{offset: uint32(len(frame)), bytes: blob}})))

	eng := newBenchEngine(b)
	out, err := eng.Run(engine.HostContext{State: &mapState{}}, m.hash, m.inst, frame, callPlasmaLimit)
	if err != nil {
		b.Fatalf("warm-up Run: %v", err)
	}
	if out.Kind != engine.Returned {
		b.Fatalf("warm-up Run: Kind = %v, want Returned", out.Kind)
	}
	if len(out.ResultBlob) != l {
		b.Fatalf("warm-up Run: len(ResultBlob) = %d, want %d", len(out.ResultBlob), l)
	}
	if !bytes.Equal(out.ResultBlob, blob) {
		b.Fatalf("warm-up Run: ResultBlob differs from the encoded blob")
	}

	s := settlement{ContractID: repeatedHash(0x33), CodeHash: m.hash, GlobalInputIndex: 42, GasBudget: callPlasmaLimit}
	warmEff, err := s.settle(out)
	if err != nil {
		b.Fatalf("warm-up settle: %v", err)
	}
	if warmEff.Status != codec.StatusRuntimeFault {
		b.Fatalf("warm-up settle: Status = %v, want StatusRuntimeFault", warmEff.Status)
	}
	if warmEff.GasUsed != out.GasUsed {
		b.Fatalf("warm-up settle: GasUsed = %d, want %d", warmEff.GasUsed, out.GasUsed)
	}
	gas := out.GasUsed

	b.Run("min-entries", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if _, err := s.settle(out); err != nil {
				b.Fatal(err)
			}
		}
		b.ReportMetric(float64(gas), "gas/op")
	})
}

// BenchmarkCompileMissAtBound times eng.Run when the engine's compiled-module cache misses on
// every call: a 16-entry pool of MaxInstrumentedCodeSize modules cycled past the engine's
// 8-entry byte budget.
func BenchmarkCompileMissAtBound(b *testing.B) {
	pool := boundModules(b, 16)
	frame := benchFrame(b)
	eng := newBenchEngine(b)

	cursor := 0
	next := func() benchModule {
		m := pool[cursor%len(pool)]
		cursor++
		return m
	}

	m := next()
	out, err := eng.Run(engine.HostContext{State: &mapState{}}, m.hash, m.inst, frame, callPlasmaLimit)
	if err != nil {
		b.Fatalf("warm-up Run: %v", err)
	}
	if out.Kind != engine.Trapped {
		b.Fatalf("warm-up Run: Kind = %v, want Trapped", out.Kind)
	}
	gas := out.GasUsed

	b.Run("unreachable-chain", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			m := next()
			if _, err := eng.Run(engine.HostContext{State: &mapState{}}, m.hash, m.inst, frame, callPlasmaLimit); err != nil {
				b.Fatal(err)
			}
		}
		b.ReportMetric(float64(gas), "gas/op")
	})

	// many-functions is the same instrumented-size budget spread across MaxFunctions/2
	// functions instead of one, to check whether unreachable-chain is really the worst-case
	// compile-miss shape or whether function-boundary count costs more per byte.
	poolMF := manyFunctionsModules(b, MaxFunctions/2, 16)

	cursorMF := 0
	nextMF := func() benchModule {
		m := poolMF[cursorMF%len(poolMF)]
		cursorMF++
		return m
	}

	mMF := nextMF()
	outMF, err := eng.Run(engine.HostContext{State: &mapState{}}, mMF.hash, mMF.inst, frame, callPlasmaLimit)
	if err != nil {
		b.Fatalf("many-functions warm-up Run: %v", err)
	}
	if outMF.Kind != engine.Trapped {
		b.Fatalf("many-functions warm-up Run: Kind = %v, want Trapped", outMF.Kind)
	}
	gasMF := outMF.GasUsed

	b.Run("many-functions", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			m := nextMF()
			if _, err := eng.Run(engine.HostContext{State: &mapState{}}, m.hash, m.inst, frame, callPlasmaLimit); err != nil {
				b.Fatal(err)
			}
		}
		b.ReportMetric(float64(gasMF), "gas/op")
	})
}

// BenchmarkInstantiateAtCaps times eng.Run against a compiled and cached module at each
// individual instantiation-time structural cap, plus every cap combined, plus a module packed
// with data bytes to MaxCodeSize.
func BenchmarkInstantiateAtCaps(b *testing.B) {
	frame := benchFrame(b)
	eng := newBenchEngine(b)

	base := func() wasmModuleSpec {
		return runModuleSpec(nil, 0, []byte{0x00}, []byte{0x42, 0x00, 0x0B}, nil)
	}
	repeatFuncs := func(n int, idx uint32) []uint32 {
		out := make([]uint32, n)
		for i := range out {
			out[i] = idx
		}
		return out
	}
	extraFuncs := func(n int) []wasmFunc {
		out := make([]wasmFunc, n)
		for i := range out {
			out[i] = wasmFunc{TypeIdx: 1, Locals: []byte{0x00}, Body: []byte{0x0B}}
		}
		return out
	}

	globals := base()
	globals.Globals = MaxGlobals

	functions := base()
	functions.Types = append(functions.Types, wasmFuncType{})
	functions.Funcs = append(functions.Funcs, extraFuncs(MaxFunctions-1)...)

	dataSegments := base()
	dataSegments.Data = make([]dataSeg, MaxDataSegments)

	elementEntries := base()
	elementEntries.TableMin = MaxTotalElementEntries / 2
	elementEntries.Elements = []wasmElem{
		{Offset: 0, Funcs: repeatFuncs(MaxTotalElementEntries/2, 0)},
		{Offset: 0, Funcs: repeatFuncs(MaxTotalElementEntries/2, 0)},
	}

	tableEntries := base()
	tableEntries.TableMin = MaxTableEntries

	all := base()
	all.Globals = MaxGlobals
	all.Types = append(all.Types, wasmFuncType{})
	all.Funcs = append(all.Funcs, extraFuncs(MaxFunctions-1)...)
	all.Data = make([]dataSeg, MaxDataSegments)
	all.TableMin = MaxTableEntries
	all.Elements = []wasmElem{
		{Offset: 0, Funcs: repeatFuncs(MaxTableEntries, 0)},
		{Offset: 0, Funcs: repeatFuncs(MaxTotalElementEntries-MaxTableEntries, 0)},
	}

	dbSpec := runModuleSpec(nil, 1, []byte{0x00}, []byte{0x42, 0x00, 0x0B}, []dataSeg{
		{offset: 0, bytes: make([]byte, 65536)},
		{offset: 0, bytes: make([]byte, 65536)},
		{offset: 0, bytes: make([]byte, 65536)},
		{offset: 0, bytes: nil},
	})
	withoutFourth := len(buildWasmModule(dbSpec))
	r := MaxCodeSize - withoutFourth - 2
	dbSpec.Data[3].bytes = make([]byte, r)
	dbRaw := buildWasmModule(dbSpec)
	if len(dbRaw) != MaxCodeSize {
		b.Fatalf("data-bytes: len(raw) = %d, want %d", len(dbRaw), MaxCodeSize)
	}
	if r >= 65536 {
		b.Fatalf("data-bytes: r = %d, want < 65536", r)
	}

	children := []struct {
		name string
		raw  []byte
		want engine.OutcomeKind
	}{
		{"globals", buildWasmModule(globals), engine.Trapped},
		{"functions", buildWasmModule(functions), engine.Trapped},
		{"data-segments", buildWasmModule(dataSegments), engine.Trapped},
		{"element-entries", buildWasmModule(elementEntries), engine.Trapped},
		{"table-entries", buildWasmModule(tableEntries), engine.Trapped},
		{"all", buildWasmModule(all), engine.Trapped},
		{"data-bytes", dbRaw, engine.Returned},
	}

	for _, c := range children {
		m := instrumentFor(b, c.raw)

		out, err := eng.Run(engine.HostContext{State: &mapState{}}, m.hash, m.inst, frame, callPlasmaLimit)
		if err != nil {
			b.Fatalf("%s: warm-up Run: %v", c.name, err)
		}
		if out.Kind != c.want {
			b.Fatalf("%s: warm-up Run: Kind = %v, want %v", c.name, out.Kind, c.want)
		}
		gas := out.GasUsed

		b.Run(c.name, func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := eng.Run(engine.HostContext{State: &mapState{}}, m.hash, m.inst, frame, callPlasmaLimit); err != nil {
					b.Fatal(err)
				}
			}
			b.ReportMetric(float64(gas), "gas/op")
		})
	}
}

// BenchmarkCodeStoreReloadAtBound times loadInstrumented's disk-tier read of a
// MaxInstrumentedCodeSize module already resident in the engine's compiled-module cache, i.e.
// the reload path a warm page cache serves.
func BenchmarkCodeStoreReloadAtBound(b *testing.B) {
	p, err := New(Config{NetworkName: "testnet", CodeStorePath: b.TempDir()})
	if err != nil {
		b.Fatalf("New: %v", err)
	}
	b.Cleanup(func() { _ = p.Close() })

	m := boundModules(b, 1)[0]
	if err := p.store.put(entryInstrumentedModule, m.hash[:], m.inst); err != nil {
		b.Fatalf("store.put: %v", err)
	}

	frame := benchFrame(b)

	if _, err := p.engine.Run(engine.HostContext{State: &mapState{}}, m.hash, m.inst, frame, callPlasmaLimit); err != nil {
		b.Fatalf("compile Run: %v", err)
	}
	out, err := p.engine.Run(engine.HostContext{State: &mapState{}}, m.hash, m.inst, frame, callPlasmaLimit)
	if err != nil {
		b.Fatalf("warm Run: %v", err)
	}
	if out.Kind != engine.Trapped {
		b.Fatalf("warm Run: Kind = %v, want Trapped", out.Kind)
	}
	gas := out.GasUsed

	loaded, err := p.loadInstrumented(m.hash, types.Hash{}, false)
	if err != nil {
		b.Fatalf("loadInstrumented: %v", err)
	}
	if !bytes.Equal(loaded, m.inst) {
		b.Fatalf("loadInstrumented: got %d bytes, want equal to m.inst (%d bytes)", len(loaded), len(m.inst))
	}

	b.Run("warm-page-cache", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if _, err := p.loadInstrumented(m.hash, types.Hash{}, false); err != nil {
				b.Fatal(err)
			}
		}
		b.ReportMetric(float64(gas), "gas/op")
	})
}

// countedLoopModule returns a one-page module whose run (type (i32,i32)->i64, function 0) runs
// calls iters times, counting in mutable i32 global 0, then returns i64 0. types follow run's
// type at index 1; callees follow run at function index 1.
func countedLoopModule(calls []byte, iters int32, types []wasmFuncType, callees []wasmFunc) []byte {
	body := []byte{0x03, 0x40}
	body = append(body, calls...)
	body = append(body, 0x23, 0x00, 0x41, 0x01, 0x6A, 0x24, 0x00)
	body = append(body, 0x23, 0x00, 0x41)
	body = appendVarS64(body, int64(iters))
	body = append(body, 0x47, 0x0D, 0x00, 0x0B, 0x42, 0x00, 0x0B)

	return buildWasmModule(wasmModuleSpec{
		Types:       append([]wasmFuncType{{Params: []byte{0x7F, 0x7F}, Results: []byte{0x7E}}}, types...),
		Funcs:       append([]wasmFunc{{TypeIdx: 0, Locals: []byte{0x00}, Body: body}}, callees...),
		MemoryPages: 1,
		Globals:     1,
		Exports: []wasmExport{
			{Name: "memory", Kind: 0x02, Index: 0},
			{Name: "run", Kind: 0x00, Index: 0},
		},
	})
}

// BenchmarkLocalsZeroing times calls to functions that declare locals, at MaxInputGas, and
// reports ns/gas over the gas each module actually used and ns/call. The zero-locals child is
// the plain-call baseline.
func BenchmarkLocalsZeroing(b *testing.B) {
	sixteen := bytes.Repeat([]byte{0x10, 0x01}, 16)

	type child struct {
		name    string
		calls   []byte
		iters   int32
		types   []wasmFuncType
		callees []wasmFunc
		perOp   int
	}
	flat := func(name string, n uint32, vt byte) child {
		callee := wasmFunc{TypeIdx: 1, Locals: localsDecl(n, vt), Body: []byte{0x0B}}
		return child{name, sixteen, 4096, []wasmFuncType{{}}, []wasmFunc{callee}, 16 * 4096}
	}

	children := []child{flat("flat-0", 0, 0x7F)}
	for _, vt := range []struct {
		name string
		b    byte
	}{{"i32", 0x7F}, {"i64", 0x7E}} {
		for _, n := range []uint32{64, 1024, 8192} {
			children = append(children, flat(fmt.Sprintf("flat-%s-%d", vt.name, n), n, vt.b))
		}
	}

	var wideCalls []byte
	var wideCallees []wasmFunc
	for i := 1; i <= 127; i++ {
		wideCalls = append(wideCalls, 0x10)
		wideCalls = appendVarU32(wideCalls, uint32(i))
		wideCallees = append(wideCallees, wasmFunc{TypeIdx: 1, Locals: localsDecl(8192, 0x7E), Body: []byte{0x0B}})
	}
	children = append(children, child{"wide-i64-8192", wideCalls, 512, []wasmFuncType{{}}, wideCallees, 127 * 512})

	deepCalls := []byte{0x41}
	deepCalls = appendVarS64(deepCalls, 510)
	deepCalls = append(deepCalls, 0x10, 0x01)
	deepCallee := wasmFunc{
		TypeIdx: 1,
		Locals:  localsDecl(8191, 0x7E),
		Body:    []byte{0x20, 0x00, 0x04, 0x40, 0x20, 0x00, 0x41, 0x01, 0x6B, 0x10, 0x01, 0x0B, 0x0B},
	}
	children = append(children, child{"deep-i64-8191", deepCalls, 128, []wasmFuncType{{Params: []byte{0x7F}}}, []wasmFunc{deepCallee}, 511 * 128})

	eng := newBenchEngine(b)
	frame := benchFrame(b)

	for _, c := range children {
		m := instrumentFor(b, countedLoopModule(c.calls, c.iters, c.types, c.callees))

		out, err := eng.Run(engine.HostContext{State: &mapState{}}, m.hash, m.inst, frame, MaxInputGas)
		if err != nil {
			b.Fatalf("%s: warm-up Run: %v", c.name, err)
		}
		if out.Kind != engine.Returned {
			b.Fatalf("%s: warm-up Run: Kind = %v, want Returned", c.name, out.Kind)
		}
		gas := out.GasUsed - module.InitialMemoryCost(1)

		b.Run(c.name, func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := eng.Run(engine.HostContext{State: &mapState{}}, m.hash, m.inst, frame, MaxInputGas); err != nil {
					b.Fatal(err)
				}
			}
			elapsed := float64(b.Elapsed().Nanoseconds()) / float64(b.N)
			b.ReportMetric(float64(gas), "gas/op")
			b.ReportMetric(elapsed/float64(gas), "ns/gas")
			b.ReportMetric(elapsed/float64(c.perOp), "ns/call")
		})
	}
}

// BenchmarkLocalsLoopAtBudget times a loop that calls a function declaring MaxLocalsPerFunction
// i64 locals until its gas budget runs out, at three budgets, and reports ns/gas over the budget.
func BenchmarkLocalsLoopAtBudget(b *testing.B) {
	m := instrumentFor(b, buildWasmModule(wasmModuleSpec{
		Types: []wasmFuncType{
			{Params: []byte{0x7F, 0x7F}, Results: []byte{0x7E}},
			{},
		},
		Funcs: []wasmFunc{
			{TypeIdx: 0, Locals: []byte{0x00}, Body: []byte{0x03, 0x40, 0x10, 0x01, 0x0C, 0x00, 0x0B, 0x42, 0x00, 0x0B}},
			{TypeIdx: 1, Locals: localsDecl(MaxLocalsPerFunction, 0x7E), Body: []byte{0x0B}},
		},
		MemoryPages: 1,
		Exports: []wasmExport{
			{Name: "memory", Kind: 0x02, Index: 0},
			{Name: "run", Kind: 0x00, Index: 0},
		},
	}))

	eng := newBenchEngine(b)
	frame := benchFrame(b)

	out, err := eng.Run(engine.HostContext{State: &mapState{}}, m.hash, m.inst, frame, callPlasmaLimit)
	if err != nil {
		b.Fatalf("warm-up Run: %v", err)
	}
	if out.Kind != engine.OutOfGas {
		b.Fatalf("warm-up Run: Kind = %v, want OutOfGas", out.Kind)
	}
	if out.GasUsed != callPlasmaLimit {
		b.Fatalf("warm-up Run: GasUsed = %d, want %d", out.GasUsed, uint64(callPlasmaLimit))
	}

	for _, c := range []struct {
		name   string
		budget uint64
	}{
		{"budget-100k", 100_000},
		{"plasmaLimit", callPlasmaLimit},
		{"MaxInputGas", MaxInputGas},
	} {
		b.Run(c.name, func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				out, err := eng.Run(engine.HostContext{State: &mapState{}}, m.hash, m.inst, frame, c.budget)
				if err != nil {
					b.Fatal(err)
				}
				if out.Kind != engine.OutOfGas {
					b.Fatalf("Kind = %v, want OutOfGas", out.Kind)
				}
			}
			b.ReportMetric(float64(c.budget), "gas/op")
			b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N)/float64(c.budget), "ns/gas")
		})
	}
}
