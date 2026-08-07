package engine

import (
	"errors"
	"testing"

	"github.com/digitalSloth/go-executor/plugins/wasm/internal/module"
)

func TestUnreachableIsTrapped(t *testing.T) {
	body := []byte{0x00, 0x0B} // unreachable; end
	instrumented, codeHash := buildAndInstrument(t, buildModule(nil, nil, []byte{0x00}, body, 1))
	e := newTestEngine(t)

	out, err := e.Run(testHostContext(t, newTestState(t, nil)), codeHash, instrumented, nil, 1_000_000)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if out.Kind != Trapped {
		t.Fatalf("Kind = %v, want Trapped", out.Kind)
	}
}

func TestOutOfGasBeatsCoincidentTrap(t *testing.T) {
	body := []byte{0x00, 0x0B} // unreachable; end
	instrumented, codeHash := buildAndInstrument(t, buildModule(nil, nil, []byte{0x00}, body, 1))
	// unreachable (0x00) is itself a segment terminator, so it forms its own one-instruction
	// segment; the trailing end is never reached once unreachable traps.
	segTotal := opsCost(t, 0x00)

	t.Run("ample budget: Trapped", func(t *testing.T) {
		e := newTestEngine(t)
		out, err := e.Run(testHostContext(t, newTestState(t, nil)), codeHash, instrumented, nil, 1_000_000)
		if err != nil {
			t.Fatalf("Run: %v", err)
		}
		if out.Kind != Trapped {
			t.Fatalf("Kind = %v, want Trapped", out.Kind)
		}
	})

	t.Run("one short of the segment: OutOfGas", func(t *testing.T) {
		e := newTestEngine(t)
		budget := module.InitialMemoryCost(1) + segTotal - 1
		out, err := e.Run(testHostContext(t, newTestState(t, nil)), codeHash, instrumented, nil, budget)
		if err != nil {
			t.Fatalf("Run: %v", err)
		}
		if out.Kind != OutOfGas {
			t.Fatalf("Kind = %v, want OutOfGas", out.Kind)
		}
		if out.GasUsed != budget {
			t.Fatalf("GasUsed = %d, want %d", out.GasUsed, budget)
		}
	})
}

func TestExactExhaustionIsReturned(t *testing.T) {
	body := []byte{0x42, 0x2A, 0x0B} // i64.const 42; end
	instrumented, codeHash := buildAndInstrument(t, buildModule(nil, nil, []byte{0x00}, body, 1))
	e := newTestEngine(t)

	budget := module.InitialMemoryCost(1) + opsCost(t, 0x42, 0x0B)
	out, err := e.Run(testHostContext(t, newTestState(t, nil)), codeHash, instrumented, nil, budget)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if out.Kind != Returned {
		t.Fatalf("Kind = %v, want Returned, not OutOfGas", out.Kind)
	}
	if out.GasUsed != budget {
		t.Fatalf("GasUsed = %d, want %d", out.GasUsed, budget)
	}
}

func TestClassifyOrder(t *testing.T) {
	plainTrap := errors.New("engine: test trap")

	cases := []struct {
		name string
		err  error
		gas  int64
		want OutcomeKind
	}{
		{"abort with negative gas is Aborted", &abortError{code: 5}, -1, Aborted},
		{"abort with non-negative gas is Aborted", &abortError{code: 5}, 100, Aborted},
		{"plain trap with negative gas is OutOfGas", plainTrap, -1, OutOfGas},
		{"plain trap with non-negative gas is Trapped", plainTrap, 0, Trapped},
		{"no error is Returned", nil, 100, Returned},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := classify(c.err, c.gas)
			if got != c.want {
				t.Errorf("classify(%v, %d) = %v, want %v", c.err, c.gas, got, c.want)
			}
		})
	}
}
