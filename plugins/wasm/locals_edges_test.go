package wasm

import (
	"testing"

	"github.com/digitalSloth/go-executor/domain"
	"github.com/digitalSloth/go-executor/plugins/wasm/internal/engine"
	"github.com/digitalSloth/go-executor/plugins/wasm/internal/module"

	"github.com/zenon-network/go-zenon/common/types"
)

// runLocalsModule instruments raw, runs its run export and returns the gas used.
func runLocalsModule(t *testing.T, raw []byte) uint64 {
	t.Helper()
	if err := module.Validate(raw, moduleLimits()); err != nil {
		t.Fatalf("Validate: %v", err)
	}
	inst, err := module.Instrument(raw, moduleLimits())
	if err != nil {
		t.Fatalf("Instrument: %v", err)
	}
	frame, err := buildFrame(types.Hash{}, domain.InputContext{}, callArgs{}, types.Hash{}).Encode()
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	eng, err := engine.New()
	if err != nil {
		t.Fatalf("engine.New: %v", err)
	}
	t.Cleanup(func() { _ = eng.Close() })
	out, err := eng.Run(engine.HostContext{State: &mapState{}}, CodeHash(inst), inst, frame, callPlasmaLimit)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if out.Kind != engine.Returned {
		t.Fatalf("Kind = %v, want Returned", out.Kind)
	}
	return out.GasUsed
}

func exportsRun() []wasmExport {
	return []wasmExport{{Name: "memory", Kind: 0x02, Index: 0}, {Name: "run", Kind: 0x00, Index: 0}}
}

// TestLocalsEntryChargeEdges checks the entry charge in the shapes around the main case: no
// declared locals, parameters, a callee reached through call_indirect, one charge per call, and
// a callee whose body loops.
func TestLocalsEntryChargeEdges(t *testing.T) {
	big := uint64((MaxLocalsPerFunction + testLocalsPerGas - 1) / testLocalsPerGas)
	runTy := wasmFuncType{Params: []byte{0x7F, 0x7F}, Results: []byte{0x7E}}

	t.Run("zero declared locals", func(t *testing.T) {
		raw := localsCallModule(localsDecl(0, 0x7F), []byte{0x42, 0x00, 0x0B}, nil)
		want := module.InitialMemoryCost(1) + costOf(t, 0x10, 0x42, 0x0B, 0x0B)
		if got := runLocalsModule(t, raw); got != want {
			t.Errorf("GasUsed = %d, want %d", got, want)
		}
	})

	t.Run("parameters are not charged", func(t *testing.T) {
		// The callee takes 100 i64 parameters and declares no locals.
		params := make([]byte, 100)
		for i := range params {
			params[i] = 0x7E
		}
		var body []byte
		for i := 0; i < 100; i++ {
			body = append(body, 0x42, 0x00)
		}
		body = append(body, 0x10, 0x01, 0x42, 0x00, 0x0B)
		raw := buildWasmModule(wasmModuleSpec{
			Types: []wasmFuncType{runTy, {Params: params}},
			Funcs: []wasmFunc{
				{TypeIdx: 0, Locals: []byte{0x00}, Body: body},
				{TypeIdx: 1, Locals: []byte{0x00}, Body: []byte{0x0B}},
			},
			MemoryPages: 1,
			Exports:     exportsRun(),
		})
		ops := make([]byte, 0, 104)
		for i := 0; i < 100; i++ {
			ops = append(ops, 0x42)
		}
		ops = append(ops, 0x10, 0x42, 0x0B, 0x0B)
		want := module.InitialMemoryCost(1) + costOf(t, ops...)
		if got := runLocalsModule(t, raw); got != want {
			t.Errorf("GasUsed = %d, want %d", got, want)
		}
	})

	t.Run("call_indirect target", func(t *testing.T) {
		raw := buildWasmModule(wasmModuleSpec{
			Types: []wasmFuncType{runTy, {}},
			Funcs: []wasmFunc{
				{TypeIdx: 0, Locals: []byte{0x00}, Body: []byte{0x41, 0x00, 0x11, 0x01, 0x00, 0x42, 0x00, 0x0B}},
				{TypeIdx: 1, Locals: localsDecl(MaxLocalsPerFunction, 0x7E), Body: []byte{0x0B}},
			},
			MemoryPages: 1,
			TableMin:    1,
			Elements:    []wasmElem{{Offset: 0, Funcs: []uint32{1}}},
			Exports:     exportsRun(),
		})
		ops := []byte{0x41, 0x11, 0x42, 0x0B, 0x0B}
		want := module.InitialMemoryCost(1) + costOf(t, ops...) + big
		if got := runLocalsModule(t, raw); got != want {
			t.Errorf("GasUsed = %d, want %d", got, want)
		}
	})

	t.Run("one charge per call", func(t *testing.T) {
		raw := localsCallModule(localsDecl(MaxLocalsPerFunction, 0x7E), []byte{0x10, 0x01, 0x42, 0x00, 0x0B}, nil)
		want := module.InitialMemoryCost(1) + costOf(t, 0x10, 0x10, 0x42, 0x0B, 0x0B, 0x0B) + 2*big
		if got := runLocalsModule(t, raw); got != want {
			t.Errorf("GasUsed = %d, want %d", got, want)
		}
	})

	t.Run("callee loop pays entry once", func(t *testing.T) {
		// callee: loop; local.get 0; i32.const 1; i32.add; local.tee 0; i32.const 5; i32.lt_u;
		// br_if 0; end; end, with 8192 i32 locals.
		callee := []byte{
			0x03, 0x40, 0x20, 0x00, 0x41, 0x01, 0x6A, 0x22, 0x00, 0x41, 0x05, 0x49, 0x0D, 0x00, 0x0B, 0x0B,
		}
		raw := buildWasmModule(wasmModuleSpec{
			Types: []wasmFuncType{runTy, {}},
			Funcs: []wasmFunc{
				{TypeIdx: 0, Locals: []byte{0x00}, Body: []byte{0x10, 0x01, 0x42, 0x00, 0x0B}},
				{TypeIdx: 1, Locals: localsDecl(MaxLocalsPerFunction, 0x7F), Body: callee},
			},
			MemoryPages: 1,
			Exports:     exportsRun(),
		})
		ops := []byte{0x10, 0x42, 0x0B, 0x03}
		for i := 0; i < 5; i++ {
			ops = append(ops, 0x20, 0x41, 0x6A, 0x22, 0x41, 0x49, 0x0D)
		}
		ops = append(ops, 0x0B, 0x0B)
		want := module.InitialMemoryCost(1) + costOf(t, ops...) + big
		if got := runLocalsModule(t, raw); got != want {
			t.Errorf("GasUsed = %d, want %d", got, want)
		}
	})
}
