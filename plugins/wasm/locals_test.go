package wasm

import (
	"testing"

	"github.com/digitalSloth/go-executor/domain"
	"github.com/digitalSloth/go-executor/plugins/wasm/internal/engine"
	"github.com/digitalSloth/go-executor/plugins/wasm/internal/module"

	"github.com/zenon-network/go-zenon/common/types"
)

// testLocalsPerGas is the gas table's function_entry.locals_per_gas, written out so tests that
// re-derive the entry charge do not reuse the code that computes it.
const testLocalsPerGas = 2

// localsDecl returns a locals vector declaring n locals of value type vt as one group, or the
// empty vector (0x00) when n is 0.
func localsDecl(n uint32, vt byte) []byte {
	if n == 0 {
		return []byte{0x00}
	}
	out := appendVarU32([]byte{0x01}, n)
	return append(out, vt)
}

// localsCallModule returns a one-page module: run (type (i32,i32)->i64, function 0) whose body
// is call 1 followed by tail, and function 1 (type ()->()) with the raw locals vector locals and
// body end. data is the module's data segments.
func localsCallModule(locals, tail []byte, data []dataSeg) []byte {
	return buildWasmModule(wasmModuleSpec{
		Types: []wasmFuncType{
			{Params: []byte{0x7F, 0x7F}, Results: []byte{0x7E}},
			{},
		},
		Funcs: []wasmFunc{
			{TypeIdx: 0, Locals: []byte{0x00}, Body: append([]byte{0x10, 0x01}, tail...)},
			{TypeIdx: 1, Locals: locals, Body: []byte{0x0B}},
		},
		MemoryPages: 1,
		Exports: []wasmExport{
			{Name: "memory", Kind: 0x02, Index: 0},
			{Name: "run", Kind: 0x00, Index: 0},
		},
		Data: data,
	})
}

// TestLocalsEntryCharge checks the exact gas of one call to a function that declares locals:
// the entry segment pays ceil(declared / locals_per_gas) once, summed over every locals group.
func TestLocalsEntryCharge(t *testing.T) {
	k := uint32(testLocalsPerGas)
	twoGroups := []byte{0x02}
	twoGroups = appendVarU32(twoGroups, k)
	twoGroups = append(twoGroups, 0x7F)
	twoGroups = appendVarU32(twoGroups, k+1)
	twoGroups = append(twoGroups, 0x7E)

	cases := []struct {
		name   string
		locals []byte
		entry  uint64
	}{
		{"cap", localsDecl(MaxLocalsPerFunction, 0x7E), (MaxLocalsPerFunction + testLocalsPerGas - 1) / testLocalsPerGas},
		{"two groups", twoGroups, 3},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			raw := localsCallModule(c.locals, []byte{0x42, 0x00, 0x0B}, nil)
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
			want := module.InitialMemoryCost(1) + costOf(t, 0x10, 0x42, 0x0B, 0x0B) + c.entry
			if out.GasUsed != want {
				t.Errorf("GasUsed = %d, want %d", out.GasUsed, want)
			}
		})
	}
}
