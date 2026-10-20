package engine

import (
	"context"
	"testing"

	"github.com/tetratelabs/wazero"
	"github.com/tetratelabs/wazero/api"

	"github.com/digitalSloth/go-executor/plugins/wasm/internal/module"
)

// This file is the execution-observable proof for internal/module's Instrument: properties
// like "per-path gas charge equals the sum of the executed original instructions' costs" and
// "the stack-depth counter never leaks" cannot be proven statically, since internal/module
// cannot import wazero to run anything itself. A test failure here means a bug in
// internal/module/instrument.go, in a different package from the one that actually changed --
// that is deliberate, not a mistake, and is why every test below re-derives its expectation from
// module.OpcodeCost/module.BrTableCost/module.MemoryGrowPageCost rather than a hardcoded number.
// This file stays as-is once the engine gains a host module and entry point: it remains the
// direct -- no host module, no Engine wrapper -- control, and later work adds tests to it rather
// than absorbing or deleting it.

// moduleWithBody builds the same minimal conforming module compile_test.go's minimalModule does,
// parameterized on the function body: one type (i32,i32) -> i64, one function, memory {min 1},
// exports "memory" and "run". locals is the body's raw locals-vector encoding (e.g. {0x00} for
// none); code is the instruction stream including the function's own closing end.
func moduleWithBody(locals, code []byte) []byte {
	var out []byte
	out = append(out, wasmHeader...)

	typeSec := []byte{0x01, 0x60, 0x02, 0x7F, 0x7F, 0x01, 0x7E}
	out = append(out, section(1, typeSec)...)

	funcSec := []byte{0x01, 0x00}
	out = append(out, section(3, funcSec)...)

	memSec := []byte{0x01, 0x00, 0x01}
	out = append(out, section(5, memSec)...)

	exportSec := []byte{
		0x02,
		0x06, 'm', 'e', 'm', 'o', 'r', 'y', 0x02, 0x00,
		0x03, 'r', 'u', 'n', 0x00, 0x00,
	}
	out = append(out, section(7, exportSec)...)

	body := append(append([]byte{}, locals...), code...)
	codeSec := []byte{0x01}
	codeSec = appendVarU32(codeSec, uint32(len(body)))
	codeSec = append(codeSec, body...)
	out = append(out, section(10, codeSec)...)

	return out
}

// gasUsed instantiates instrumented fresh, seeds its gas ledger to budget, calls run(arg0,
// arg1), and returns run's result alongside the gas actually spent.
func gasUsed(t *testing.T, instrumented []byte, budget int64, arg0, arg1 int32) (ret, used int64) {
	t.Helper()
	ctx := context.Background()
	r := wazero.NewRuntimeWithConfig(ctx, runtimeConfig())
	defer func() { _ = r.Close(ctx) }()

	mod, err := r.Instantiate(ctx, instrumented)
	if err != nil {
		t.Fatalf("instantiate: %v", err)
	}
	defer func() { _ = mod.Close(ctx) }()

	if _, err := mod.ExportedFunction("__zenon_gas_set").Call(ctx, uint64(budget)); err != nil {
		t.Fatalf("__zenon_gas_set: %v", err)
	}
	results, err := mod.ExportedFunction("run").Call(ctx, api.EncodeI32(arg0), api.EncodeI32(arg1))
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	remaining, err := mod.ExportedFunction("__zenon_gas_get").Call(ctx)
	if err != nil {
		t.Fatalf("__zenon_gas_get: %v", err)
	}
	return int64(results[0]), budget - int64(remaining[0])
}

// opsCost sums module.OpcodeCost over ops, the hand-written list of original opcodes a test
// case's path actually executes -- never a hardcoded gas number, so the assertion is independent
// of the instrumenter's own segmentation logic.
func opsCost(t *testing.T, ops ...byte) uint64 {
	t.Helper()
	var total uint64
	for _, op := range ops {
		c, ok := module.OpcodeCost(module.Opcode(op))
		if !ok {
			t.Fatalf("no gas-table cost for opcode 0x%02X", op)
		}
		total += c
	}
	return total
}

// branchesBody exercises a nested block, if/else, br, br_if, loop, br_table and a return, under
// moduleWithBody's (i32,i32) -> i64 run signature. Byte-identical to
// internal/module/instrument_test.go's branchesBody -- cross-referenced there -- so that
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

// growLimiterBody is local.get 1; memory.grow; drop; memory.size; i64.extend_i32_u; end -- the
// return value is the page count after the attempt.
func growLimiterBody() []byte {
	return []byte{
		0x20, 0x01, // local.get 1
		0x40, 0x00, // memory.grow
		0x1A,       // drop
		0x3F, 0x00, // memory.size
		0xAD, // i64.extend_i32_u
		0x0B, // end
	}
}

// depthLeakBody's only exit is a br to the function's outermost label.
func depthLeakBody() []byte {
	return []byte{0x42, 0x2A, 0x0C, 0x00, 0x0B} // i64.const 42; br 0; end
}

// depthCeilingBody is a self-recursive run(_, n): returns 42 when n == 0, otherwise recurses via
// call 0 (run's own function index -- there are no imports, so run is function 0, an index
// instrumentation's append-only injection never disturbs) with n-1. frames = n+1.
func depthCeilingBody() []byte {
	return []byte{
		0x20, 0x01, // local.get 1
		0x45,       // i32.eqz
		0x04, 0x7E, // if (result i64)
		0x42, 0x2A, // i64.const 42
		0x05,       // else
		0x20, 0x00, // local.get 0
		0x20, 0x01, // local.get 1
		0x41, 0x01, // i32.const 1
		0x6B,       // i32.sub
		0x10, 0x00, // call 0
		0x0B, // end (if)
		0x0B, // end (function)
	}
}

// chargeCase is one required per-path charge-accounting case.
type chargeCase struct {
	name    string
	locals  []byte
	code    []byte
	arg0    int32
	wantOps []byte
	// declared is the sum of the counts in locals: the function's entry segment pays
	// module.LocalsEntryCost(declared) on top of its instructions' costs.
	declared uint64
}

// chargeCases returns the six required cases: a taken br_if, a false if condition, a branch
// landing after end, both arms of an else, a loop-back edge, and the endleader shape whose final
// end is itself a segment leader.
func chargeCases() []chargeCase {
	elseArmCode := []byte{
		0x20, 0x00, // local.get 0
		0x04, 0x40, // if (void)
		0x42, 0x01, // i64.const 1
		0x1A,       // drop
		0x05,       // else
		0x42, 0x02, // i64.const 2
		0x1A,       // drop
		0x0B,       // end (if/else)
		0x42, 0x2A, // i64.const 42
		0x0B, // end (function)
	}

	return []chargeCase{
		{
			name:   "taken br_if",
			locals: []byte{0x00},
			code: []byte{
				0x02, 0x40, // block (void)
				0x41, 0x01, // i32.const 1
				0x0D, 0x00, // br_if 0
				0x01,       // nop
				0x0B,       // end (block)
				0x42, 0x2A, // i64.const 42
				0x0B, // end (function)
			},
			wantOps: []byte{0x02, 0x41, 0x0D, 0x42, 0x0B},
		},
		{
			name:   "false if",
			locals: []byte{0x00},
			code: []byte{
				0x20, 0x00, // local.get 0
				0x04, 0x40, // if (void)
				0x01,       // nop
				0x01,       // nop
				0x0B,       // end (if)
				0x42, 0x2A, // i64.const 42
				0x0B, // end (function)
			},
			arg0:    0,
			wantOps: []byte{0x20, 0x04, 0x42, 0x0B},
		},
		{
			name:   "branch landing after end",
			locals: []byte{0x00},
			code: []byte{
				0x02, 0x40, // block (outer, void)
				0x02, 0x40, // block (inner, void)
				0x0C, 0x01, // br 1
				0x0B,       // end (inner)
				0x01,       // nop
				0x0B,       // end (outer)
				0x42, 0x2A, // i64.const 42
				0x0B, // end (function)
			},
			wantOps: []byte{0x02, 0x02, 0x0C, 0x42, 0x0B},
		},
		{
			name:    "else arm: false",
			locals:  []byte{0x00},
			code:    elseArmCode,
			arg0:    0,
			wantOps: []byte{0x20, 0x04, 0x42, 0x1A, 0x0B, 0x42, 0x0B},
		},
		{
			name:    "else arm: true",
			locals:  []byte{0x00},
			code:    elseArmCode,
			arg0:    1,
			wantOps: []byte{0x20, 0x04, 0x42, 0x1A, 0x05, 0x42, 0x0B},
		},
		{
			name:     "loop-back edge",
			locals:   []byte{0x01, 0x01, 0x7F}, // 1 group: 1 i32 local (index 2)
			declared: 1,
			code: []byte{
				0x41, 0x03, // i32.const 3
				0x21, 0x02, // local.set 2
				0x03, 0x40, // loop (void)
				0x20, 0x02, // local.get 2
				0x41, 0x01, // i32.const 1
				0x6B,       // i32.sub
				0x22, 0x02, // local.tee 2
				0x0D, 0x00, // br_if 0
				0x0B,       // end (loop)
				0x42, 0x2A, // i64.const 42
				0x0B, // end (function)
			},
			// loop (0x03) is itself a terminator, so it belongs to the prologue's segment, not
			// the segment after it; the loop's own end is reached by iteration 3's fall-through
			// and forms its own one-instruction segment (br_if, the segment before it, is also
			// a terminator), distinct from the final i64.const/end segment that follows it. The
			// declared local's entry charge is part of the prologue segment and is paid once, not
			// per iteration.
			wantOps: []byte{
				0x41, 0x21, 0x03, // prologue: i32.const, local.set, loop
				0x20, 0x41, 0x6B, 0x22, 0x0D, // loop-body segment, iteration 1
				0x20, 0x41, 0x6B, 0x22, 0x0D, // iteration 2
				0x20, 0x41, 0x6B, 0x22, 0x0D, // iteration 3 (br_if false, falls through)
				0x0B,       // the loop's own end, its own segment
				0x42, 0x0B, // i64.const, end (function)
			},
		},
		{
			// A body whose first instruction is a loop: the loop terminates the entry segment, so
			// the back-edges land in the next segment and the entry charge stays outside the loop.
			name:     "loop first",
			locals:   []byte{0x01, 0x01, 0x7F}, // 1 group: 1 i32 local (index 2)
			declared: 1,
			code: []byte{
				0x03, 0x40, // loop (void)
				0x20, 0x02, // local.get 2
				0x41, 0x01, // i32.const 1
				0x6A,       // i32.add
				0x22, 0x02, // local.tee 2
				0x41, 0x03, // i32.const 3
				0x49,       // i32.lt_u
				0x0D, 0x00, // br_if 0
				0x0B,       // end (loop)
				0x42, 0x2A, // i64.const 42
				0x0B, // end (function)
			},
			wantOps: []byte{
				0x03,                                     // loop
				0x20, 0x41, 0x6A, 0x22, 0x41, 0x49, 0x0D, // iteration 1
				0x20, 0x41, 0x6A, 0x22, 0x41, 0x49, 0x0D, // iteration 2
				0x20, 0x41, 0x6A, 0x22, 0x41, 0x49, 0x0D, // iteration 3 (br_if false, falls through)
				0x0B,       // the loop's own end
				0x42, 0x0B, // i64.const, end (function)
			},
		},
		{
			name:   "taken return",
			locals: []byte{0x00},
			code: []byte{
				0x20, 0x00, // local.get 0
				0x04, 0x7E, // if (result i64)
				0x42, 0x2A, // i64.const 42
				0x0F,       // return
				0x05,       // else
				0x42, 0x07, // i64.const 7
				0x0B, // end (if)
				0x0B, // end (function)
			},
			arg0:    1,
			wantOps: []byte{0x20, 0x04, 0x42, 0x0F},
		},
		{
			// The endleader shape: the final end's predecessor is itself an end, so it is a
			// segment leader with its own non-zero charge, pinning rewriteBody's final-end
			// emission slot at run time rather than only in a compile-only fixture.
			name:    "final end is a leader",
			locals:  []byte{0x00},
			code:    []byte{0x42, 0x2A, 0x02, 0x40, 0x01, 0x0B, 0x0B}, // i64.const 42; block; nop; end; end
			wantOps: []byte{0x42, 0x02, 0x01, 0x0B, 0x0B},
		},
	}
}

func TestInstrumentedChargeMatchesExecutedPath(t *testing.T) {
	for _, c := range chargeCases() {
		c := c
		t.Run(c.name, func(t *testing.T) {
			original := moduleWithBody(c.locals, c.code)
			if err := module.Validate(original, testLimits); err != nil {
				t.Fatalf("Validate = %v, want nil", err)
			}
			instrumented, err := module.Instrument(original, testLimits)
			if err != nil {
				t.Fatalf("Instrument = %v, want nil", err)
			}

			want := opsCost(t, c.wantOps...) + module.LocalsEntryCost(c.declared)
			ret, used := gasUsed(t, instrumented, 1_000_000, c.arg0, 0)
			if ret != 42 {
				t.Errorf("run = %d, want 42", ret)
			}
			if uint64(used) != want {
				t.Errorf("gas used = %d, want %d", used, want)
			}
		})
	}
}

// TestInstrumentedOutOfGasTraps proves the sole out-of-gas marker: __zenon_charge sets the gas
// global to -1 before trapping, so a budget too small for the first segment's charge both traps
// the call and leaves __zenon_gas_get reporting exactly -1, never a partially-consumed positive
// remainder.
func TestInstrumentedOutOfGasTraps(t *testing.T) {
	original := moduleWithBody([]byte{0x00}, depthLeakBody()) // i64.const 42; br 0; end
	if err := module.Validate(original, testLimits); err != nil {
		t.Fatalf("Validate = %v, want nil", err)
	}
	instrumented, err := module.Instrument(original, testLimits)
	if err != nil {
		t.Fatalf("Instrument = %v, want nil", err)
	}

	// br (0x0C) is itself a segment terminator, so the body's first segment is [i64.const, br]
	// only; the trailing end forms its own segment, never reached once br 0 jumps to the
	// wrapper's outermost label. A budget one short of the first segment's cost must trap on
	// that segment's own charge.
	segCost := opsCost(t, 0x42, 0x0C)

	ctx := context.Background()
	r := wazero.NewRuntimeWithConfig(ctx, runtimeConfig())
	defer func() { _ = r.Close(ctx) }()
	mod, err := r.Instantiate(ctx, instrumented)
	if err != nil {
		t.Fatalf("instantiate: %v", err)
	}
	defer func() { _ = mod.Close(ctx) }()

	budget := int64(segCost) - 1
	if _, err := mod.ExportedFunction("__zenon_gas_set").Call(ctx, uint64(budget)); err != nil {
		t.Fatalf("__zenon_gas_set: %v", err)
	}
	if _, err := mod.ExportedFunction("run").Call(ctx, api.EncodeI32(0), api.EncodeI32(0)); err == nil {
		t.Fatal("run = nil error, want a trap (out of gas)")
	}

	remaining, err := mod.ExportedFunction("__zenon_gas_get").Call(ctx)
	if err != nil {
		t.Fatalf("__zenon_gas_get: %v", err)
	}
	if int64(remaining[0]) != -1 {
		t.Errorf("gas after trap = %d, want -1", int64(remaining[0]))
	}
}

func TestInstrumentedDepthDoesNotLeak(t *testing.T) {
	original := moduleWithBody([]byte{0x00}, depthLeakBody())
	if err := module.Validate(original, testLimits); err != nil {
		t.Fatalf("Validate = %v, want nil", err)
	}
	instrumented, err := module.Instrument(original, testLimits)
	if err != nil {
		t.Fatalf("Instrument = %v, want nil", err)
	}

	ctx := context.Background()
	r := wazero.NewRuntimeWithConfig(ctx, runtimeConfig())
	defer func() { _ = r.Close(ctx) }()
	mod, err := r.Instantiate(ctx, instrumented)
	if err != nil {
		t.Fatalf("instantiate: %v", err)
	}
	defer func() { _ = mod.Close(ctx) }()

	if _, err := mod.ExportedFunction("__zenon_gas_set").Call(ctx, uint64(1_000_000_000)); err != nil {
		t.Fatalf("__zenon_gas_set: %v", err)
	}

	// A leaked __zenon_depth increment would trap on the 513th call; 600 non-trapping calls on
	// one instance is the observable proxy for "__zenon_depth is 0 after every return", since the
	// global is deliberately not exported.
	for i := 0; i < 600; i++ {
		results, err := mod.ExportedFunction("run").Call(ctx, api.EncodeI32(0), api.EncodeI32(0))
		if err != nil {
			t.Fatalf("run call %d = %v, want nil (depth leaked?)", i, err)
		}
		if int64(results[0]) != 42 {
			t.Fatalf("run call %d = %d, want 42", i, int64(results[0]))
		}
	}
}

func TestInstrumentedDepthCeiling(t *testing.T) {
	original := moduleWithBody([]byte{0x00}, depthCeilingBody())
	if err := module.Validate(original, testLimits); err != nil {
		t.Fatalf("Validate = %v, want nil", err)
	}
	instrumented, err := module.Instrument(original, testLimits)
	if err != nil {
		t.Fatalf("Instrument = %v, want nil", err)
	}

	// A fresh instance per call, since __zenon_depth starts at 0 only at instantiation.
	run := func(n int32) (ret int64, callErr error) {
		ctx := context.Background()
		r := wazero.NewRuntimeWithConfig(ctx, runtimeConfig())
		defer func() { _ = r.Close(ctx) }()
		mod, err := r.Instantiate(ctx, instrumented)
		if err != nil {
			t.Fatalf("instantiate: %v", err)
		}
		defer func() { _ = mod.Close(ctx) }()
		if _, err := mod.ExportedFunction("__zenon_gas_set").Call(ctx, uint64(1_000_000_000)); err != nil {
			t.Fatalf("__zenon_gas_set: %v", err)
		}
		results, err := mod.ExportedFunction("run").Call(ctx, api.EncodeI32(0), api.EncodeI32(n))
		if err != nil {
			return 0, err
		}
		return int64(results[0]), nil
	}

	// 512 frames admitted.
	ret, err := run(511)
	if err != nil {
		t.Fatalf("run(0,511) = %v, want nil (512 frames, at the limit)", err)
	}
	if ret != 42 {
		t.Fatalf("run(0,511) = %d, want 42", ret)
	}

	// The 513th frame traps.
	if _, err := run(512); err == nil {
		t.Fatal("run(0,512) = nil error, want a trap (depth ceiling exceeded)")
	}
}

func TestInstrumentedModulesCompile(t *testing.T) {
	type namedModule struct {
		name string
		wasm []byte
	}
	var modules []namedModule

	for _, c := range chargeCases() {
		modules = append(modules, namedModule{c.name, moduleWithBody(c.locals, c.code)})
	}
	modules = append(modules,
		namedModule{"depth leak body", moduleWithBody([]byte{0x00}, depthLeakBody())},
		namedModule{"depth ceiling body", moduleWithBody([]byte{0x00}, depthCeilingBody())},
		namedModule{"memory.grow", moduleWithBody([]byte{0x00}, growLimiterBody())},
		namedModule{"branches", moduleWithBody([]byte{0x00}, branchesBody())},
		namedModule{"env import", storageLenImportModule()},
	)

	for _, m := range modules {
		m := m
		t.Run(m.name, func(t *testing.T) {
			if err := module.Validate(m.wasm, testLimits); err != nil {
				t.Fatalf("Validate = %v, want nil", err)
			}
			instrumented, err := module.Instrument(m.wasm, testLimits)
			if err != nil {
				t.Fatalf("Instrument = %v, want nil", err)
			}
			if err := Compile(context.Background(), instrumented); err != nil {
				t.Errorf("Compile(instrumented %s) = %v, want nil", m.name, err)
			}
		})
	}
}

func TestInstrumentedGrowLimiter(t *testing.T) {
	original := moduleWithBody([]byte{0x00}, growLimiterBody())
	if err := module.Validate(original, testLimits); err != nil {
		t.Fatalf("Validate = %v, want nil", err)
	}
	instrumented, err := module.Instrument(original, testLimits)
	if err != nil {
		t.Fatalf("Instrument = %v, want nil", err)
	}

	// The whole body is one segment (no terminators before the final end), so its static charge
	// -- including memory.grow's own base opcode cost -- is paid once regardless of the delta.
	segTotal := opsCost(t, 0x20, 0x40, 0x1A, 0x3F, 0xAD, 0x0B)

	t.Run("grow succeeds", func(t *testing.T) {
		ret, used := gasUsed(t, instrumented, 1_000_000, 0, 1)
		if ret != 2 {
			t.Errorf("run(0,1) = %d, want 2", ret)
		}
		want := segTotal + module.MemoryGrowPageCost(1)
		if uint64(used) != want {
			t.Errorf("gas used = %d, want %d", used, want)
		}
	})

	t.Run("grow rejected", func(t *testing.T) {
		ret, used := gasUsed(t, instrumented, 1_000_000, 0, 100)
		if ret != 1 {
			t.Errorf("run(0,100) = %d, want 1 (memory unchanged)", ret)
		}
		if uint64(used) != segTotal {
			t.Errorf("gas used = %d, want %d (no per-page surcharge)", used, segTotal)
		}
	})
}
