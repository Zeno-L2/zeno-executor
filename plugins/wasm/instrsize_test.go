package wasm

import (
	"bytes"
	"errors"
	"testing"

	"github.com/Zeno-L2/zeno-executor/plugins/wasm/internal/module"
)

// TestInstrumentedSizeBoundary pins the instrumented-size limit that CV-INSTR-SIZE-1 relies on:
// a run body of n `return`s instruments to 7 bytes each, 149,763 of them land exactly on
// MaxInstrumentedCodeSize, one more exceeds it, and the vector's count exceeds it while the raw
// module stays under MaxCodeSize.
func TestInstrumentedSizeBoundary(t *testing.T) {
	build := func(n int) []byte {
		body := append([]byte{0x42, 0x00}, bytes.Repeat([]byte{0x0F}, n)...)
		return buildWasmModule(runModuleSpec(nil, 1, []byte{0x00}, append(body, 0x0B), nil))
	}
	const atCap = 149_763

	for _, tc := range []struct {
		name    string
		returns int
		tooBig  bool
	}{
		{"exactly at MaxInstrumentedCodeSize", atCap, false},
		{"one return over MaxInstrumentedCodeSize", atCap + 1, true},
		{"the vector's return count", instrSizeReturns, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			raw := build(tc.returns)
			if len(raw) > MaxCodeSize {
				t.Fatalf("raw module is %d bytes, want at most %d", len(raw), MaxCodeSize)
			}
			if err := module.Validate(raw, moduleLimits()); err != nil {
				t.Fatalf("Validate: %v", err)
			}
			out, err := module.Instrument(raw, moduleLimits())
			if tc.tooBig {
				if !errors.Is(err, module.ErrInstrumentedTooLarge) {
					t.Fatalf("Instrument error = %v, want ErrInstrumentedTooLarge", err)
				}
				if tc.returns == instrSizeReturns {
					// The deploy path compiles the module before instrumenting it, so
					// the size check is reached only if the compile accepts the body.
					if _, _, err := deriveModuleErr(raw); !errors.Is(err, module.ErrInstrumentedTooLarge) {
						t.Fatalf("deriveModuleErr error = %v, want ErrInstrumentedTooLarge", err)
					}
				}
				return
			}
			if err != nil {
				t.Fatalf("Instrument: %v", err)
			}
			if len(out) != MaxInstrumentedCodeSize {
				t.Fatalf("instrumented module is %d bytes, want %d", len(out), MaxInstrumentedCodeSize)
			}
		})
	}
}
