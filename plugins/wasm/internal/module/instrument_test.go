package module

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

var update = flag.Bool("update", false, "update golden files in testdata/instrument")

// instrumentLimits mirrors the real values in plugins/wasm/limits.go (MaxCodeSize,
// MaxMemoryPages, MaxLocalsPerFunction, MaxTotalLocals, MaxTableEntries, MaxTotalParams).
// validate_test.go's testLimits is deliberately not reused: its MaxMemoryPages feeds memory
// normalization and therefore the golden bytes, and its MaxCodeSize of 4096 would make the
// ceiling test (7) impossible to state honestly.
var instrumentLimits = Limits{
	MaxCodeSize:            256 << 10,
	MaxMemoryPages:         64,
	MaxLocalsPerFunction:   8192,
	MaxTotalLocals:         1 << 20,
	MaxTableEntries:        10000,
	MaxTotalParams:         1 << 16,
	MaxGlobals:             1024,
	MaxDataSegments:        1024,
	MaxFunctions:           8192,
	MaxTotalElementEntries: 16384,
	MaxTypeCount:           1024,
}

// fixtureMinimal is newBuilder()'s module as-is: one function, body i64.const 0; end.
func fixtureMinimal() []byte {
	return newBuilder().bytes()
}

// fixtureBranches exercises a nested block, if/else, br, br_if, loop, br_table and a return,
// under newBuilder()'s (i32,i32) -> i64 run signature. Byte-identical to
// internal/engine/instrument_exec_test.go's branches body -- cross-referenced there -- so that
// TestInstrumentedModulesCompile proves this exact shape type-checks under wazero.
func branchesBody() []byte {
	return []byte{
		0x02, 0x40, // block (void)
		0x20, 0x00, // local.get 0
		0x04, 0x40, // if (void)
		0x20, 0x01, // local.get 1
		0x0D, 0x00, // br_if 0 (branches to the if's own end)
		0x05,       // else
		0x01,       // nop
		0x0B,       // end (if)
		0x0C, 0x00, // br 0 (branches to the block's end)
		0x0B,       // end (block)
		0x03, 0x40, // loop (void)
		0x41, 0x00, // i32.const 0
		0x0E, 0x01, 0x00, 0x00, // br_table [0] default=0 (branches to the loop header)
		0x0B,       // end (loop)
		0x20, 0x00, // local.get 0
		0x04, 0x40, // if (void)
		0x42, 0x01, // i64.const 1
		0x0F,       // return
		0x0B,       // end (if)
		0x42, 0x2A, // i64.const 42
		0x0B, // end (function)
	}
}

func fixtureBranches() []byte {
	b := newBuilder()
	b.bodies = []body{{code: branchesBody()}}
	return b.bytes()
}

// fixtureEndleader's body's final end is itself a segment leader (its predecessor, the inner
// block's end, is a terminator): the fixture that pins the step-3 emission slot.
func fixtureEndleader() []byte {
	b := newBuilder()
	b.bodies = []body{{code: []byte{0x42, 0x2A, 0x02, 0x40, 0x01, 0x0B, 0x0B}}} // i64.const 42; block 0x40; nop; end; end
	return b.bytes()
}

// fixtureGrow contains memory.size and memory.grow.
func fixtureGrow() []byte {
	b := newBuilder()
	b.bodies = []body{{code: []byte{
		0x20, 0x00, // local.get 0
		0x40, 0x00, // memory.grow
		0x1A,       // drop
		0x3F, 0x00, // memory.size
		0xAD, // i64.extend_i32_u
		0x0B, // end
	}}}
	return b.bytes()
}

// fixtureImports declares all three env imports, one global, one table, an element segment and
// a data segment, so I and G are non-zero and the index math is exercised away from 0.
func fixtureImports() []byte {
	b := &builder{}
	storageLenSig := funcType{form: 0x60, params: []byte{0x7F, 0x7F}, results: []byte{0x7E}}
	storageReadSig := funcType{form: 0x60, params: []byte{0x7F, 0x7F, 0x7F, 0x7F}, results: []byte{0x7E}}
	abortSig := funcType{form: 0x60, params: []byte{0x7F, 0x7F, 0x7F}, results: nil}
	runSig := funcType{form: 0x60, params: []byte{0x7F, 0x7F}, results: []byte{0x7E}}
	b.types = []funcType{storageLenSig, storageReadSig, abortSig, runSig}
	b.imports = []importEntry{
		{module: "env", name: "storage_len", desc: 0x00, typeIdx: 0},
		{module: "env", name: "storage_read", desc: 0x00, typeIdx: 1},
		{module: "env", name: "abort", desc: 0x00, typeIdx: 2},
	}
	b.funcs = []uint32{3} // run, locally defined at function index 3 (after the 3 imports)
	b.tables = []tableEntry{{elemType: 0x70, lim: limits{flags: 0x00, min: 1}}}
	b.memories = []limits{{flags: 0x00, min: 1}}
	b.globals = []globalEntry{{valType: 0x7F, mut: 0x00, init: constI32(7)}}
	b.exports = []exportEntry{
		{name: "memory", kind: 0x02, index: 0},
		{name: "run", kind: 0x00, index: 3},
	}
	b.elements = []elemSeg{{flags: 0x00, offset: constI32(0), funcs: []uint32{3}}}
	b.data = []dataSeg{{flags: 0x00, offset: constI32(0), bytes: []byte{0xAA, 0xBB}}}
	b.bodies = []body{{code: []byte{0x42, 0x00, 0x0B}}} // i64.const 0; end
	return b.bytes()
}

// fixtureVoid adds a second, unexported function whose type has no result, so the wrapper
// block type 0x40 is covered alongside the run fixture's 0x7E.
func fixtureVoid() []byte {
	b := newBuilder()
	voidTypeIdx := uint32(len(b.types))
	b.types = append(b.types, funcType{form: 0x60})
	b.funcs = append(b.funcs, voidTypeIdx)
	b.bodies = append(b.bodies, body{code: []byte{0x0B}}) // end
	return b.bytes()
}

type namedFixture struct {
	name string
	wasm []byte
}

func instrumentFixtures() []namedFixture {
	return []namedFixture{
		{"minimal", fixtureMinimal()},
		{"branches", fixtureBranches()},
		{"endleader", fixtureEndleader()},
		{"grow", fixtureGrow()},
		{"imports", fixtureImports()},
		{"void", fixtureVoid()},
	}
}

// requireValidFixture asserts the given fixture is Instrument-legal input (module.Validate
// accepts it under instrumentLimits), so the suite can never drift onto an input Instrument was
// never contracted to handle.
func requireValidFixture(t *testing.T, name string, wasm []byte) {
	t.Helper()
	if err := Validate(wasm, instrumentLimits); err != nil {
		t.Fatalf("fixture %s: Validate = %v, want nil", name, err)
	}
}

// TestMirroredConstants pins maxStackDepth and maxInstrumentedCodeSize to the literal values
// they are meant to mirror by hand (plugins/wasm.MaxStackDepth and
// plugins/wasm.MaxInstrumentedCodeSize). A true cross-package equality check isn't possible
// here: plugins/wasm imports internal/module, so internal/module's tests can't import
// plugins/wasm back without a cycle. Keeping the two definitions in sync is a manual review
// obligation whenever either changes.
func TestMirroredConstants(t *testing.T) {
	if maxStackDepth != 512 {
		t.Errorf("maxStackDepth = %d, want 512", maxStackDepth)
	}
	if maxInstrumentedCodeSize != 1<<20 {
		t.Errorf("maxInstrumentedCodeSize = %d, want %d", maxInstrumentedCodeSize, 1<<20)
	}
}

func TestInstrumentDeterministic(t *testing.T) {
	for _, f := range instrumentFixtures() {
		f := f
		t.Run(f.name, func(t *testing.T) {
			requireValidFixture(t, f.name, f.wasm)
			orig := append([]byte(nil), f.wasm...)

			first, err := Instrument(f.wasm, instrumentLimits)
			if err != nil {
				t.Fatalf("Instrument(%s) = %v, want nil", f.name, err)
			}
			for i := 0; i < 16; i++ {
				got, err := Instrument(f.wasm, instrumentLimits)
				if err != nil {
					t.Fatalf("Instrument(%s) run %d = %v, want nil", f.name, i, err)
				}
				if !bytes.Equal(got, first) {
					t.Fatalf("Instrument(%s) run %d not byte-equal to run 0", f.name, i)
				}
			}
			if !bytes.Equal(f.wasm, orig) {
				t.Fatalf("Instrument(%s) modified its input slice", f.name)
			}
		})
	}
}

func goldenPath(name string) string {
	return filepath.Join("testdata", "instrument", name+".wasm")
}

func TestInstrumentGolden(t *testing.T) {
	for _, f := range instrumentFixtures() {
		f := f
		t.Run(f.name, func(t *testing.T) {
			requireValidFixture(t, f.name, f.wasm)
			got, err := Instrument(f.wasm, instrumentLimits)
			if err != nil {
				t.Fatalf("Instrument(%s) = %v, want nil", f.name, err)
			}

			path := goldenPath(f.name)
			if *update {
				if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
					t.Fatalf("MkdirAll: %v", err)
				}
				if err := os.WriteFile(path, got, 0o644); err != nil {
					t.Fatalf("WriteFile(%s): %v", path, err)
				}
			}
			want, err := os.ReadFile(path)
			if err != nil {
				t.Fatalf("reading golden %s: %v (run with -update to generate it)", path, err)
			}
			if !bytes.Equal(got, want) {
				t.Fatalf("Instrument(%s) does not match golden %s", f.name, path)
			}
		})
	}

	// Tripwires over the checked-in goldens: -update can rewrite testdata/instrument/*.wasm,
	// but it cannot rewrite these hex literals, computed once when the goldens were first
	// generated. Stdlib SHA-256, not SHA3-256: this package cannot import SHA3, and the
	// SHA3-vs-anything-else property is pinned separately by keys_test.go's NIST vectors
	// against plugins/wasm.CodeHash.
	wantDigests := map[string]string{
		"minimal":   "78ee73d754a4370541b1ebed93d95001c4764eb32d9cfb70dfdd8a69edaa3478",
		"branches":  "8fe614267fa8e70c7ad85cefc0fd34c50cf2a2b3dbd0c7e83965d6c79cd7517f",
		"endleader": "a17362c2c484f3a6e4af713e9e3aac4d39b3d02fc7fab2b6172dfbb7968b4049",
		"grow":      "ce59d568b628b4114dc048ae0fc47804f3120d87da366d0c82f8269a0022be82",
		"imports":   "9d10ac216120a86b51ba53221e85e695ce66b2659f3001b384b35a429c649b2f",
		"void":      "51960d6236255f37ec5eb1b41674ab69bf1da5565942284d0f61ed4538d8d632",
	}
	for name, wantHex := range wantDigests {
		golden, err := os.ReadFile(goldenPath(name))
		if err != nil {
			t.Fatalf("reading golden %s: %v", name, err)
		}
		got := sha256.Sum256(golden)
		if gotHex := hex.EncodeToString(got[:]); gotHex != wantHex {
			t.Errorf("SHA-256(testdata/instrument/%s.wasm) = %s, want %s (a golden changed: review the diff -- this is a consensus change)", name, gotHex, wantHex)
		}
	}
}

// sectionMap decodes wasm's non-custom sections into a map keyed by id, for structural
// comparison. Not used in the emit path of any production code -- test-only, and iterated only
// after being built (the emit path itself never ranges over a map, see instrument.go).
func sectionMap(t *testing.T, wasm []byte) map[byte][]byte {
	t.Helper()
	rest, err := parsePreamble(wasm)
	if err != nil {
		t.Fatalf("parsePreamble: %v", err)
	}
	secs, err := splitSections(rest)
	if err != nil {
		t.Fatalf("splitSections: %v", err)
	}
	m := make(map[byte][]byte, len(secs))
	for _, s := range secs {
		m[s.id] = s.payload
	}
	return m
}

func TestInstrumentAppendOnly(t *testing.T) {
	for _, f := range instrumentFixtures() {
		f := f
		t.Run(f.name, func(t *testing.T) {
			requireValidFixture(t, f.name, f.wasm)
			got, err := Instrument(f.wasm, instrumentLimits)
			if err != nil {
				t.Fatalf("Instrument(%s) = %v, want nil", f.name, err)
			}

			origSecs := sectionMap(t, f.wasm)
			newSecs := sectionMap(t, got)

			if _, ok := newSecs[0]; ok {
				t.Errorf("%s: instrumented output has a custom section", f.name)
			}
			if _, ok := newSecs[8]; ok {
				t.Errorf("%s: instrumented output has a start section", f.name)
			}

			checkGrown := func(id byte, delta uint32, label string) {
				origCount, origRest := uint32(0), []byte(nil)
				if p, ok := origSecs[id]; ok {
					c, r, err := splitVecCount(p)
					if err != nil {
						t.Fatalf("%s: %s: original: %v", f.name, label, err)
					}
					origCount, origRest = c, r
				}
				newP, ok := newSecs[id]
				if !ok {
					t.Fatalf("%s: %s: section absent from instrumented output", f.name, label)
				}
				newCount, newRest, err := splitVecCount(newP)
				if err != nil {
					t.Fatalf("%s: %s: instrumented: %v", f.name, label, err)
				}
				if newCount != origCount+delta {
					t.Errorf("%s: %s: count = %d, want %d+%d = %d", f.name, label, newCount, origCount, delta, origCount+delta)
				}
				if len(newRest) < len(origRest) || !bytes.Equal(newRest[:len(origRest)], origRest) {
					t.Errorf("%s: %s: instrumented payload does not begin with the original's", f.name, label)
				}
			}
			checkGrown(1, 4, "types")
			checkGrown(3, 6, "functions")
			checkGrown(6, 2, "globals")
			checkGrown(7, 2, "exports")

			checkVerbatim := func(id byte, label string) {
				origP, origOK := origSecs[id]
				newP, newOK := newSecs[id]
				if origOK != newOK {
					t.Errorf("%s: %s: presence changed (orig %v, instrumented %v)", f.name, label, origOK, newOK)
					return
				}
				if origOK && !bytes.Equal(origP, newP) {
					t.Errorf("%s: %s: not byte-identical", f.name, label)
				}
			}
			checkVerbatim(2, "imports")
			checkVerbatim(4, "table")
			checkVerbatim(9, "elements")
			checkVerbatim(11, "data")

			// The two appended exports resolve to base+4/base+5 and no other export was
			// renamed or reindexed; no export has kind 0x03 (global).
			gotExports, err := decodeExports(newSecs[7])
			if err != nil {
				t.Fatalf("%s: decoding instrumented exports: %v", f.name, err)
			}
			origExports, err := decodeExports(origSecs[7])
			if err != nil {
				t.Fatalf("%s: decoding original exports: %v", f.name, err)
			}
			if len(gotExports) != len(origExports)+2 {
				t.Fatalf("%s: %d exports, want %d+2", f.name, len(gotExports), len(origExports)+2)
			}
			for i, e := range origExports {
				if gotExports[i] != e {
					t.Errorf("%s: export %d changed: got %+v, want %+v", f.name, i, gotExports[i], e)
				}
			}
			for _, e := range gotExports {
				if e.kind == 0x03 {
					t.Errorf("%s: export %q has kind 0x03 (global)", f.name, e.name)
				}
			}
			gotGasGet := gotExports[len(gotExports)-2]
			gotGasSet := gotExports[len(gotExports)-1]
			if gotGasGet.name != "__zenon_gas_get" || gotGasSet.name != "__zenon_gas_set" {
				t.Errorf("%s: appended export names = %q, %q, want __zenon_gas_get, __zenon_gas_set", f.name, gotGasGet.name, gotGasSet.name)
			}
			if gotGasGet.index+1 != gotGasSet.index {
				t.Errorf("%s: __zenon_gas_get index %d, __zenon_gas_set index %d, want consecutive", f.name, gotGasGet.index, gotGasSet.index)
			}
		})
	}
}

type exportRecord struct {
	name  string
	kind  byte
	index uint32
}

func decodeExports(payload []byte) ([]exportRecord, error) {
	if payload == nil {
		return nil, nil
	}
	r := newReader(payload)
	count, err := r.readVecCount()
	if err != nil {
		return nil, err
	}
	out := make([]exportRecord, 0, count)
	for i := uint32(0); i < count; i++ {
		name, err := r.readName()
		if err != nil {
			return nil, err
		}
		kind, err := r.readByte()
		if err != nil {
			return nil, err
		}
		idx, err := r.readVarU32()
		if err != nil {
			return nil, err
		}
		out = append(out, exportRecord{name: name, kind: kind, index: idx})
	}
	return out, nil
}

func TestInstrumentMemoryNormalized(t *testing.T) {
	cases := []struct {
		name string
		mem  limits
	}{
		{"no declared max", limits{flags: 0x00, min: 1}},
		{"declared max 100", limits{flags: 0x01, min: 1, max: 100}},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			b := newBuilder()
			b.memories = []limits{c.mem}
			wasm := b.bytes()
			requireValidFixture(t, c.name, wasm)

			got, err := Instrument(wasm, instrumentLimits)
			if err != nil {
				t.Fatalf("Instrument = %v, want nil", err)
			}
			secs := sectionMap(t, got)
			want := []byte{0x01, 0x01}
			want = appendVarU32(want, c.mem.min)
			want = appendVarU32(want, instrumentLimits.MaxMemoryPages)
			if !bytes.Equal(secs[5], want) {
				t.Errorf("memory section = % X, want % X", secs[5], want)
			}
		})
	}
}

// firstBody extracts the first (only) original function's instrumented body bytes from wasm's
// code section, skipping past the code section's own vector count and the body's own length
// prefix.
func firstBody(t *testing.T, wasm []byte) []byte {
	t.Helper()
	secs := sectionMap(t, wasm)
	r := newReader(secs[10])
	if _, err := r.readVecCount(); err != nil {
		t.Fatalf("code section: count: %v", err)
	}
	bodyLen, err := r.readVarU32()
	if err != nil {
		t.Fatalf("code section: body length: %v", err)
	}
	body, err := r.readBytes(int(bodyLen))
	if err != nil {
		t.Fatalf("code section: body: %v", err)
	}
	return body
}

func TestInstrumentBodyShape(t *testing.T) {
	// Every fixture here is newBuilder()'s module with I=0, F=1, so base = 1: call
	// __zenon_charge = call 1, call __zenon_enter = call 2, call __zenon_exit = call 3, call
	// __zenon_grow = call 4. The wrapper block type is 0x7E (run's i64 result).
	cases := []struct {
		name string
		wasm []byte
		want []byte
	}{
		{
			name: "minimal",
			wasm: fixtureMinimal(),
			// i64.const 0; end: one segment [i64.const, end], cost 1+1=2.
			want: []byte{
				0x00,       // locals
				0x10, 0x02, // call __zenon_enter
				0x02, 0x7E, // block i64
				0x42, 0x02, // i64.const 2
				0x10, 0x01, // call __zenon_charge
				0x42, 0x00, // i64.const 0
				0x0B,       // end (wrapper block)
				0x10, 0x03, // call __zenon_exit
				0x0B, // end (function)
			},
		},
		{
			name: "grow",
			wasm: fixtureGrow(),
			// local.get 0; memory.grow; drop; memory.size; i64.extend_i32_u; end: one segment
			// (none of the interior opcodes terminate it), cost 1+5+1+2+1+1=11.
			want: []byte{
				0x00,       // locals
				0x10, 0x02, // call __zenon_enter
				0x02, 0x7E, // block i64
				0x42, 0x0B, // i64.const 11
				0x10, 0x01, // call __zenon_charge
				0x20, 0x00, // local.get 0
				0x10, 0x04, // call __zenon_grow (replaces memory.grow)
				0x1A,       // drop
				0x3F, 0x00, // memory.size
				0xAD,       // i64.extend_i32_u
				0x0B,       // end (wrapper block)
				0x10, 0x03, // call __zenon_exit
				0x0B, // end (function)
			},
		},
		{
			name: "endleader",
			wasm: fixtureEndleader(),
			// i64.const 42; block 0x40; nop; end; end: S1=[i64.const,block,nop,end] cost 4,
			// S2=[end] (the final end, itself a leader) cost 1.
			want: []byte{
				0x00,       // locals
				0x10, 0x02, // call __zenon_enter
				0x02, 0x7E, // block i64
				0x42, 0x04, // i64.const 4
				0x10, 0x01, // call __zenon_charge
				0x42, 0x2A, // i64.const 42
				0x02, 0x40, // block 0x40
				0x01,       // nop
				0x0B,       // end (inner block)
				0x42, 0x01, // i64.const 1
				0x10, 0x01, // call __zenon_charge (the final end's own segment)
				0x0B,       // end (wrapper block)
				0x10, 0x03, // call __zenon_exit
				0x0B, // end (function)
			},
		},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			requireValidFixture(t, c.name, c.wasm)
			got, err := Instrument(c.wasm, instrumentLimits)
			if err != nil {
				t.Fatalf("Instrument = %v, want nil", err)
			}
			body := firstBody(t, got)
			if !bytes.Equal(body, c.want) {
				t.Errorf("%s body = % X, want % X", c.name, body, c.want)
			}
		})
	}
}

func TestInstrumentTotality(t *testing.T) {
	assertTotal := func(t *testing.T, name string, wasm []byte) {
		t.Helper()
		defer func() {
			if r := recover(); r != nil {
				t.Fatalf("%s: Instrument panicked: %v", name, r)
			}
		}()
		if _, err := Instrument(wasm, instrumentLimits); err == nil {
			t.Errorf("%s: Instrument = nil error, want non-nil", name)
		}
	}

	// Every strict prefix of a validated module is itself malformed input (both minimal and
	// branches end with the code section and have nothing after it, so any boundary-exact
	// prefix drops at least part of the code section and is never a coincidentally
	// well-formed shorter module).
	for _, f := range []namedFixture{{"minimal", fixtureMinimal()}, {"branches", fixtureBranches()}} {
		requireValidFixture(t, f.name, f.wasm)
		for i := 0; i < len(f.wasm); i++ {
			assertTotal(t, fmt.Sprintf("%s[:%d]", f.name, i), f.wasm[:i])
		}
	}

	b := newBuilder()
	b.bodies = []body{{code: nil}}
	assertTotal(t, "zero-length body", b.bytes())

	trailing := newBuilder()
	trailing.bodies = []body{{code: []byte{0x42, 0x00, 0x0B, 0xFF}}} // end followed by a trailing byte
	assertTotal(t, "trailing byte after final end", trailing.bytes())

	// An opcode outside the accepted profile (0xFD, SIMD) inside a body must be rejected via
	// classifyOpcode, not desynchronize the decoder into a slice panic.
	simd := newBuilder()
	simd.bodies = []body{{code: []byte{0xFD, 0x0B}}}
	assertTotal(t, "out-of-profile opcode in body", simd.bytes())
}

// unreachableChainBody returns k one-byte unreachable instructions followed by end: a dense
// leader-packing case, since unreachable is itself a segment terminator, so every instruction
// is its own segment.
func unreachableChainBody(k int) []byte {
	code := make([]byte, k+1)
	for i := 0; i < k; i++ {
		code[i] = 0x00 // unreachable
	}
	code[k] = 0x0B // end
	return code
}

func TestInstrumentWorstCaseExpansion(t *testing.T) {
	sized := func(k int) []byte {
		b := newBuilder()
		b.bodies = []body{{code: unreachableChainBody(k)}}
		return b.bytes()
	}

	// Per-terminator cost: k1=200 vs k2=300, both instrumented bodies (~1013, ~1513 bytes)
	// and both code sections (~1016, ~1516 bytes) sit inside the same 2-byte LEB128 bucket
	// (128..16383), so no length-prefix width change contaminates the delta.
	w1, w2 := sized(200), sized(300)
	requireValidFixture(t, "k=200", w1)
	requireValidFixture(t, "k=300", w2)
	got1, err := Instrument(w1, instrumentLimits)
	if err != nil {
		t.Fatalf("Instrument(k=200) = %v, want nil", err)
	}
	got2, err := Instrument(w2, instrumentLimits)
	if err != nil {
		t.Fatalf("Instrument(k=300) = %v, want nil", err)
	}
	if delta := len(got2) - len(got1); delta != 5*(300-200) {
		t.Errorf("len(got2)-len(got1) = %d, want %d", delta, 5*(300-200))
	}

	// Ceiling: a validator-accepted module of this shape sized just under MaxCodeSize must be
	// rejected by Instrument with ErrInstrumentedTooLarge -- a deterministic rejection, not a
	// fault.
	kCeil := (instrumentLimits.MaxCodeSize - 512) // comfortably under MaxCodeSize once wrapped in a module
	ceilWasm := sized(kCeil)
	for len(ceilWasm) > instrumentLimits.MaxCodeSize {
		kCeil -= 1024
		ceilWasm = sized(kCeil)
	}
	if err := Validate(ceilWasm, instrumentLimits); err != nil {
		t.Fatalf("Validate(ceiling fixture) = %v, want nil", err)
	}
	_, err = Instrument(ceilWasm, instrumentLimits)
	if !errors.Is(err, ErrInstrumentedTooLarge) {
		t.Errorf("Instrument(ceiling fixture) = %v, want ErrInstrumentedTooLarge", err)
	}

	// Headroom: a 150 KiB fixture of the same shape validates and instruments successfully.
	kHeadroom := 150 << 10
	headroomWasm := sized(kHeadroom)
	for len(headroomWasm) > instrumentLimits.MaxCodeSize {
		kHeadroom -= 1024
		headroomWasm = sized(kHeadroom)
	}
	if err := Validate(headroomWasm, instrumentLimits); err != nil {
		t.Fatalf("Validate(150 KiB fixture) = %v, want nil", err)
	}
	instrumented, err := Instrument(headroomWasm, instrumentLimits)
	if err != nil {
		t.Fatalf("Instrument(150 KiB fixture) = %v, want nil", err)
	}
	t.Logf("150 KiB fixture: original %d bytes, instrumented %d bytes, ratio %.2fx", len(headroomWasm), len(instrumented), float64(len(instrumented))/float64(len(headroomWasm)))
}

func TestInstrumentPreconditions(t *testing.T) {
	t.Run("custom section", func(t *testing.T) {
		b := newBuilder()
		b.customs = []customSection{{name: "x", payload: nil, afterID: 0xFF}}
		_, err := Instrument(b.bytes(), instrumentLimits)
		if !errors.Is(err, ErrCustomSection) {
			t.Errorf("Instrument(custom section) = %v, want ErrCustomSection", err)
		}
	})
	t.Run("no memory section", func(t *testing.T) {
		b := newBuilder()
		b.memories = nil
		_, err := Instrument(b.bytes(), instrumentLimits)
		if !errors.Is(err, ErrNotValidated) {
			t.Errorf("Instrument(no memory) = %v, want ErrNotValidated", err)
		}
	})
	t.Run("memory min exceeds MaxMemoryPages", func(t *testing.T) {
		b := newBuilder()
		b.memories = []limits{{flags: 0x00, min: instrumentLimits.MaxMemoryPages + 1}}
		_, err := Instrument(b.bytes(), instrumentLimits)
		if !errors.Is(err, ErrNotValidated) {
			t.Errorf("Instrument(memory min too large) = %v, want ErrNotValidated", err)
		}
	})
	t.Run("function type with more than one result", func(t *testing.T) {
		b := newBuilder()
		b.types[0].results = []byte{0x7E, 0x7E}
		_, err := Instrument(b.bytes(), instrumentLimits)
		if !errors.Is(err, ErrNotValidated) {
			t.Errorf("Instrument(multi-result type) = %v, want ErrNotValidated", err)
		}
	})
	t.Run("code section body count does not match function section count", func(t *testing.T) {
		b := newBuilder()
		b.funcs = append(b.funcs, b.funcs[0]) // two declared functions, still one body
		_, err := Instrument(b.bytes(), instrumentLimits)
		if !errors.Is(err, ErrNotValidated) {
			t.Errorf("Instrument(code/func count mismatch) = %v, want ErrNotValidated", err)
		}
	})
	// A non-function import (here, an imported global) would silently shift the global index
	// space Instrument derives from I+F if it were let through: this fixture is exactly the
	// "some future caller routed around Validate" input Validate itself would reject, so
	// Instrument must reject it independently rather than trust an assumption it never checks.
	t.Run("import section contains a non-function import", func(t *testing.T) {
		b := newBuilder()
		b.imports = []importEntry{{module: "env", name: "g", desc: 0x03, typeIdx: 0}}
		_, err := Instrument(b.bytes(), instrumentLimits)
		if !errors.Is(err, ErrNotValidated) {
			t.Errorf("Instrument(imported global) = %v, want ErrNotValidated", err)
		}
	})
}
