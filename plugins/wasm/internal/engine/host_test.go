package engine

import (
	"bytes"
	"errors"
	"testing"

	"github.com/digitalSloth/go-executor/domain"
	"github.com/digitalSloth/go-executor/internal/codec"
	"github.com/digitalSloth/go-executor/plugins/wasm/internal/keys"
	"github.com/digitalSloth/go-executor/plugins/wasm/internal/module"
)

// storageLenBody returns storage_len(0, keyLen)'s own i64 result directly, key bytes coming
// from the frame written at offset 0.
func storageLenBody(keyLen int32) []byte {
	b := []byte{0x41, 0x00} // i32.const 0 (key_ptr)
	b = append(b, 0x41)
	b = appendVarS32(b, keyLen) // i32.const key_len
	b = append(b, 0x10, 0x00)   // call 0 (storage_len)
	b = append(b, 0x0B)         // end
	return b
}

// storageReadBody returns storage_read(0, keyLen, outPtr, outLen)'s own i64 result directly.
func storageReadBody(keyLen, outPtr, outLen int32) []byte {
	b := []byte{0x41, 0x00} // i32.const 0 (key_ptr)
	b = append(b, 0x41)
	b = appendVarS32(b, keyLen)
	b = append(b, 0x41)
	b = appendVarS32(b, outPtr)
	b = append(b, 0x41)
	b = appendVarS32(b, outLen)
	b = append(b, 0x10, 0x00) // call 0 (storage_read)
	b = append(b, 0x0B)       // end
	return b
}

// storageReadWriteBody calls storage_read(0, keyLen, outPtr, outLen), drops its result, and
// returns the packed (outPtr<<32)|resultLen word.
func storageReadWriteBody(keyLen, outPtr, outLen int32, resultLen int64) []byte {
	b := []byte{0x41, 0x00}
	b = append(b, 0x41)
	b = appendVarS32(b, keyLen)
	b = append(b, 0x41)
	b = appendVarS32(b, outPtr)
	b = append(b, 0x41)
	b = appendVarS32(b, outLen)
	b = append(b, 0x10, 0x00) // call storage_read
	b = append(b, 0x1A)       // drop
	b = append(b, 0x42)
	b = appendVarS64(b, (int64(outPtr)<<32)|resultLen)
	b = append(b, 0x0B)
	return b
}

func testContractID() (contractID [32]byte) {
	for i := range contractID {
		contractID[i] = byte(i + 1)
	}
	return contractID
}

func TestStorageLenAbsentAndPresent(t *testing.T) {
	key := []byte{0xAA, 0xBB}
	contractID := testContractID()
	path := keys.Derive(contractID, key)

	cases := []struct {
		name  string
		state domain.StateAccess
		want  int64
	}{
		{"absent", newTestState(t, nil), -1},
		{"present-empty", newTestState(t, map[domain.Path][]byte{path: {}}), 0},
		{"present 5-byte", newTestState(t, map[domain.Path][]byte{path: {1, 2, 3, 4, 5}}), 5},
	}

	instrumented, codeHash := buildAndInstrument(t, buildModule([]string{"storage_len"}, nil, []byte{0x00}, storageLenBody(int32(len(key))), 1))
	e := newTestEngine(t)

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			out, err := e.Run(HostContext{State: c.state, ContractID: contractID}, codeHash, instrumented, key, 1_000_000)
			if err != nil {
				t.Fatalf("Run: %v", err)
			}
			if out.Kind != Returned {
				t.Fatalf("Kind = %v, want Returned", out.Kind)
			}
			if out.Ret != c.want {
				t.Fatalf("Ret = %d, want %d", out.Ret, c.want)
			}
		})
	}
}

func TestStorageLenWitnessesAbsentRead(t *testing.T) {
	key := []byte{0x01, 0x02, 0x03}
	contractID := testContractID()
	accessor := newTestState(t, nil)

	instrumented, codeHash := buildAndInstrument(t, buildModule([]string{"storage_len"}, nil, []byte{0x00}, storageLenBody(int32(len(key))), 1))
	e := newTestEngine(t)

	if _, err := e.Run(HostContext{State: accessor, ContractID: contractID}, codeHash, instrumented, key, 1_000_000); err != nil {
		t.Fatalf("Run: %v", err)
	}

	witnesses := accessor.Witnesses()
	if len(witnesses) != 1 {
		t.Fatalf("len(Witnesses) = %d, want 1", len(witnesses))
	}
	want := keys.Derive(contractID, key)
	if witnesses[0].Path != want {
		t.Errorf("witness path = %x, want %x", witnesses[0].Path, want)
	}
	if witnesses[0].Present {
		t.Errorf("witness Present = true, want false")
	}
}

func TestStorageReadReturnCodes(t *testing.T) {
	key := []byte{0x10, 0x20}
	contractID := testContractID()
	presentEmptyPath := keys.Derive(contractID, key)
	presentLPath := keys.Derive(contractID, append(append([]byte{}, key...), 0x01))

	t.Run("absent", func(t *testing.T) {
		instrumented, codeHash := buildAndInstrument(t, buildModule([]string{"storage_read"}, nil, []byte{0x00}, storageReadBody(int32(len(key)), 2000, 64), 1))
		e := newTestEngine(t)
		out, err := e.Run(HostContext{State: newTestState(t, nil), ContractID: contractID}, codeHash, instrumented, key, 1_000_000)
		if err != nil {
			t.Fatalf("Run: %v", err)
		}
		if out.Ret != -1 {
			t.Fatalf("Ret = %d, want -1", out.Ret)
		}
	})

	t.Run("out_len < L", func(t *testing.T) {
		state := newTestState(t, map[domain.Path][]byte{presentEmptyPath: {1, 2, 3, 4, 5}})
		instrumented, codeHash := buildAndInstrument(t, buildModule([]string{"storage_read"}, nil, []byte{0x00}, storageReadBody(int32(len(key)), 2000, 2), 1))
		e := newTestEngine(t)
		out, err := e.Run(HostContext{State: state, ContractID: contractID}, codeHash, instrumented, key, 1_000_000)
		if err != nil {
			t.Fatalf("Run: %v", err)
		}
		if out.Ret != -2 {
			t.Fatalf("Ret = %d, want -2", out.Ret)
		}
	})

	t.Run("present-empty", func(t *testing.T) {
		state := newTestState(t, map[domain.Path][]byte{presentEmptyPath: {}})
		instrumented, codeHash := buildAndInstrument(t, buildModule([]string{"storage_read"}, nil, []byte{0x00}, storageReadBody(int32(len(key)), 2000, 64), 1))
		e := newTestEngine(t)
		out, err := e.Run(HostContext{State: state, ContractID: contractID}, codeHash, instrumented, key, 1_000_000)
		if err != nil {
			t.Fatalf("Run: %v", err)
		}
		if out.Ret != 0 {
			t.Fatalf("Ret = %d, want 0", out.Ret)
		}
	})

	t.Run("present L-byte", func(t *testing.T) {
		longKey := append(append([]byte{}, key...), 0x01)
		value := []byte{9, 8, 7, 6, 5, 4}
		state := newTestState(t, map[domain.Path][]byte{presentLPath: value})
		instrumented, codeHash := buildAndInstrument(t, buildModule([]string{"storage_read"}, nil, []byte{0x00}, storageReadBody(int32(len(longKey)), 2000, 64), 1))
		e := newTestEngine(t)
		out, err := e.Run(HostContext{State: state, ContractID: contractID}, codeHash, instrumented, longKey, 1_000_000)
		if err != nil {
			t.Fatalf("Run: %v", err)
		}
		if out.Ret != int64(len(value)) {
			t.Fatalf("Ret = %d, want %d", out.Ret, len(value))
		}
	})
}

func TestStorageReadWritesValue(t *testing.T) {
	key := []byte{0x01}
	contractID := testContractID()
	value := []byte{11, 22, 33, 44, 55, 66, 77}
	path := keys.Derive(contractID, key)
	state := newTestState(t, map[domain.Path][]byte{path: value})

	body := storageReadWriteBody(int32(len(key)), 1000, 64, int64(len(value)))
	instrumented, codeHash := buildAndInstrument(t, buildModule([]string{"storage_read"}, nil, []byte{0x00}, body, 1))
	e := newTestEngine(t)

	out, err := e.Run(HostContext{State: state, ContractID: contractID}, codeHash, instrumented, key, 1_000_000)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if out.Kind != Returned {
		t.Fatalf("Kind = %v, want Returned", out.Kind)
	}
	if string(out.ResultBlob) != string(value) {
		t.Fatalf("ResultBlob = %v, want %v", out.ResultBlob, value)
	}
}

func TestStorageReadWritesNothingOnMinusOneAndMinusTwo(t *testing.T) {
	key := []byte{0x02}
	contractID := testContractID()

	t.Run("absent", func(t *testing.T) {
		body := storageReadWriteBody(int32(len(key)), 1000, 64, 8)
		instrumented, codeHash := buildAndInstrument(t, buildModule([]string{"storage_read"}, nil, []byte{0x00}, body, 1))
		e := newTestEngine(t)
		out, err := e.Run(HostContext{State: newTestState(t, nil), ContractID: contractID}, codeHash, instrumented, key, 1_000_000)
		if err != nil {
			t.Fatalf("Run: %v", err)
		}
		for i, b := range out.ResultBlob {
			if b != 0 {
				t.Fatalf("ResultBlob[%d] = %d, want 0", i, b)
			}
		}
	})

	t.Run("out_len too small", func(t *testing.T) {
		path := keys.Derive(contractID, key)
		state := newTestState(t, map[domain.Path][]byte{path: {1, 2, 3, 4, 5, 6, 7, 8}})
		body := storageReadWriteBody(int32(len(key)), 1000, 2, 8) // out_len=2 < L=8
		instrumented, codeHash := buildAndInstrument(t, buildModule([]string{"storage_read"}, nil, []byte{0x00}, body, 1))
		e := newTestEngine(t)
		out, err := e.Run(HostContext{State: state, ContractID: contractID}, codeHash, instrumented, key, 1_000_000)
		if err != nil {
			t.Fatalf("Run: %v", err)
		}
		for i, b := range out.ResultBlob {
			if b != 0 {
				t.Fatalf("ResultBlob[%d] = %d, want 0", i, b)
			}
		}
	})
}

func TestStorageReadAbsentChargesBaseOnly(t *testing.T) {
	key := []byte{0x03}
	contractID := testContractID()
	body := storageReadBody(int32(len(key)), 2000, 64)
	instrumented, codeHash := buildAndInstrument(t, buildModule([]string{"storage_read"}, nil, []byte{0x00}, body, 1))
	e := newTestEngine(t)

	out, err := e.Run(HostContext{State: newTestState(t, nil), ContractID: contractID}, codeHash, instrumented, key, 1_000_000)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	segTotal := opsCost(t, 0x41, 0x41, 0x41, 0x41, 0x10, 0x0B)
	want := module.InitialMemoryCost(1) + segTotal + module.HostCost(module.HostStorageRead, 0)
	if out.GasUsed != want {
		t.Fatalf("GasUsed = %d, want %d", out.GasUsed, want)
	}
}

func TestStorageReadChargesPer32(t *testing.T) {
	key := []byte{0x04}
	contractID := testContractID()
	value := make([]byte, 40) // spans two 32-byte units
	path := keys.Derive(contractID, key)
	state := newTestState(t, map[domain.Path][]byte{path: value})

	body := storageReadBody(int32(len(key)), 2000, int32(len(value)))
	instrumented, codeHash := buildAndInstrument(t, buildModule([]string{"storage_read"}, nil, []byte{0x00}, body, 1))
	e := newTestEngine(t)

	out, err := e.Run(HostContext{State: state, ContractID: contractID}, codeHash, instrumented, key, 1_000_000)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	segTotal := opsCost(t, 0x41, 0x41, 0x41, 0x41, 0x10, 0x0B)
	want := module.InitialMemoryCost(1) + segTotal + module.HostCost(module.HostStorageRead, len(value))
	if out.GasUsed != want {
		t.Fatalf("GasUsed = %d, want %d", out.GasUsed, want)
	}
}

func TestHostChargeRefusesBeforeAnyWork(t *testing.T) {
	contractID := testContractID()
	// key_ptr = 100000 is out of bounds for the one declared 65536-byte page.
	body := []byte{0x41}
	body = appendVarS32(body, 100000)
	body = append(body, 0x41, 0x04) // key_len = 4
	body = append(body, 0x10, 0x00) // call storage_len
	body = append(body, 0x0B)

	instrumented, codeHash := buildAndInstrument(t, buildModule([]string{"storage_len"}, nil, []byte{0x00}, body, 1))
	memCost := module.InitialMemoryCost(1)
	segTotal := opsCost(t, 0x41, 0x41, 0x10, 0x0B)
	hostCost := module.HostCost(module.HostStorageLen, 0)

	t.Run("ample budget traps on the OOB read", func(t *testing.T) {
		e := newTestEngine(t)
		accessor := newTestState(t, nil)
		out, err := e.Run(HostContext{State: accessor, ContractID: contractID}, codeHash, instrumented, nil, 1_000_000)
		if err != nil {
			t.Fatalf("Run: %v", err)
		}
		if out.Kind != Trapped {
			t.Fatalf("Kind = %v, want Trapped", out.Kind)
		}
	})

	t.Run("budget one short of the host charge is OutOfGas before any state access", func(t *testing.T) {
		e := newTestEngine(t)
		accessor := newTestState(t, nil)
		budget := memCost + segTotal + hostCost - 1
		out, err := e.Run(HostContext{State: accessor, ContractID: contractID}, codeHash, instrumented, nil, budget)
		if err != nil {
			t.Fatalf("Run: %v", err)
		}
		if out.Kind != OutOfGas {
			t.Fatalf("Kind = %v, want OutOfGas", out.Kind)
		}
		if out.GasUsed != budget {
			t.Fatalf("GasUsed = %d, want %d", out.GasUsed, budget)
		}
		if len(accessor.Witnesses()) != 0 {
			t.Fatalf("Witnesses() = %v, want empty", accessor.Witnesses())
		}
	})
}

func TestStorageReadPer32ChargeRefusesBeforeWriting(t *testing.T) {
	key := []byte{0x05}
	contractID := testContractID()
	value := make([]byte, 10)
	for i := range value {
		value[i] = byte(i)
	}
	path := keys.Derive(contractID, key)

	body := storageReadBody(int32(len(key)), 2000, int32(len(value)))
	instrumented, codeHash := buildAndInstrument(t, buildModule([]string{"storage_read"}, nil, []byte{0x00}, body, 1))
	segTotal := opsCost(t, 0x41, 0x41, 0x41, 0x41, 0x10, 0x0B)
	full := module.InitialMemoryCost(1) + segTotal + module.HostCost(module.HostStorageRead, len(value))

	t.Run("one short of per-32 charge", func(t *testing.T) {
		e := newTestEngine(t)
		state := newTestState(t, map[domain.Path][]byte{path: value})
		budget := full - 1
		out, err := e.Run(HostContext{State: state, ContractID: contractID}, codeHash, instrumented, key, budget)
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

	t.Run("exactly the per-32 charge", func(t *testing.T) {
		e := newTestEngine(t)
		state := newTestState(t, map[domain.Path][]byte{path: value})
		out, err := e.Run(HostContext{State: state, ContractID: contractID}, codeHash, instrumented, key, full)
		if err != nil {
			t.Fatalf("Run: %v", err)
		}
		if out.Kind != Returned {
			t.Fatalf("Kind = %v, want Returned", out.Kind)
		}
		if out.Ret != int64(len(value)) {
			t.Fatalf("Ret = %d, want %d", out.Ret, len(value))
		}
	})
}

func TestKeyLenAboveMaxTraps(t *testing.T) {
	contractID := testContractID()
	body := storageLenBody(33)
	instrumented, codeHash := buildAndInstrument(t, buildModule([]string{"storage_len"}, nil, []byte{0x00}, body, 1))
	e := newTestEngine(t)
	accessor := newTestState(t, nil)

	frame := make([]byte, 33)
	out, err := e.Run(HostContext{State: accessor, ContractID: contractID}, codeHash, instrumented, frame, 1_000_000)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if out.Kind != Trapped {
		t.Fatalf("Kind = %v, want Trapped", out.Kind)
	}
	if len(accessor.Witnesses()) != 0 {
		t.Fatalf("Witnesses() = %v, want empty", accessor.Witnesses())
	}
}

func TestKeyLenNegativeTraps(t *testing.T) {
	contractID := testContractID()
	// key_len = -1 decodes as u32 to a huge value, so it is rejected by the same
	// MaxStateKeySize comparison as an over-large positive key_len.
	body := storageLenBody(-1)
	instrumented, codeHash := buildAndInstrument(t, buildModule([]string{"storage_len"}, nil, []byte{0x00}, body, 1))
	e := newTestEngine(t)
	accessor := newTestState(t, nil)

	out, err := e.Run(HostContext{State: accessor, ContractID: contractID}, codeHash, instrumented, nil, 1_000_000)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if out.Kind != Trapped {
		t.Fatalf("Kind = %v, want Trapped", out.Kind)
	}
	if len(accessor.Witnesses()) != 0 {
		t.Fatalf("Witnesses() = %v, want empty", accessor.Witnesses())
	}
}

func TestStorageReadOutOfBoundsBufferAlsoTooSmallReturnsMinusTwo(t *testing.T) {
	key := []byte{0x07}
	contractID := testContractID()
	path := keys.Derive(contractID, key)
	value := make([]byte, 10)
	state := newTestState(t, map[domain.Path][]byte{path: value})

	// out_ptr+out_len is past memory.size, and out_len (2) is also < L (10). The -2 check must
	// fire first: an out-of-range buffer that is also too small returns -2, not Trapped.
	body := storageReadBody(int32(len(key)), 70000, 2)
	instrumented, codeHash := buildAndInstrument(t, buildModule([]string{"storage_read"}, nil, []byte{0x00}, body, 1))
	e := newTestEngine(t)

	out, err := e.Run(HostContext{State: state, ContractID: contractID}, codeHash, instrumented, key, 1_000_000)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if out.Kind != Returned {
		t.Fatalf("Kind = %v, want Returned", out.Kind)
	}
	if out.Ret != -2 {
		t.Fatalf("Ret = %d, want -2", out.Ret)
	}
}

func TestStorageReadStateErrorHalts(t *testing.T) {
	contractID := testContractID()
	body := storageReadBody(2, 2000, 64)
	instrumented, codeHash := buildAndInstrument(t, buildModule([]string{"storage_read"}, nil, []byte{0x00}, body, 1))
	e := newTestEngine(t)

	out, err := e.Run(HostContext{State: failingState{}, ContractID: contractID}, codeHash, instrumented, []byte{0, 0}, 1_000_000)
	if err == nil {
		t.Fatal("Run err = nil, want non-nil")
	}
	if !errors.Is(err, errFailingState) {
		t.Errorf("Run err = %v, want to unwrap errFailingState", err)
	}
	if out.Kind != Returned || out.Ret != 0 || out.AbortCode != 0 || out.GasUsed != 0 || out.ResultBlob != nil {
		t.Errorf("Outcome = %+v, want zero", out)
	}
}

func TestOutPtrOutOfBoundsTraps(t *testing.T) {
	key := []byte{0x06}
	contractID := testContractID()
	path := keys.Derive(contractID, key)
	state := newTestState(t, map[domain.Path][]byte{path: {1, 2, 3, 4, 5}})

	// out_ptr + out_len = 65530 + 100 > 65536 (memory.size), while out_len (100) >= L (5) so
	// the -2 check cannot preempt it.
	body := storageReadBody(int32(len(key)), 65530, 100)
	instrumented, codeHash := buildAndInstrument(t, buildModule([]string{"storage_read"}, nil, []byte{0x00}, body, 1))
	e := newTestEngine(t)

	out, err := e.Run(HostContext{State: state, ContractID: contractID}, codeHash, instrumented, key, 1_000_000)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if out.Kind != Trapped {
		t.Fatalf("Kind = %v, want Trapped", out.Kind)
	}
}

func TestSingleLedgerAcrossChargeSites(t *testing.T) {
	contractID := testContractID()
	const k = 3
	var body []byte
	var ops []byte
	for i := 0; i < k; i++ {
		body = append(body, 0x41, 0x00, 0x41, 0x00) // i32.const 0, i32.const 0
		body = append(body, 0x10, 0x00)             // call storage_len
		body = append(body, 0x1A)                   // drop
		ops = append(ops, 0x41, 0x41, 0x10, 0x1A)
	}
	body = append(body, 0x42, 0x2A) // i64.const 42
	body = append(body, 0x0B)       // end
	ops = append(ops, 0x42, 0x0B)

	instrumented, codeHash := buildAndInstrument(t, buildModule([]string{"storage_len"}, nil, []byte{0x00}, body, 1))
	e := newTestEngine(t)

	out, err := e.Run(HostContext{State: newTestState(t, nil), ContractID: contractID}, codeHash, instrumented, nil, 1_000_000)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if out.Kind != Returned || out.Ret != 42 {
		t.Fatalf("Kind/Ret = %v/%d, want Returned/42", out.Kind, out.Ret)
	}
	want := module.InitialMemoryCost(1) + opsCost(t, ops...) + k*module.HostCost(module.HostStorageLen, 0)
	if out.GasUsed != want {
		t.Fatalf("GasUsed = %d, want %d", out.GasUsed, want)
	}
}

func TestBudgetExhaustedInHostCallsTrapsAtNextInjectedCharge(t *testing.T) {
	contractID := testContractID()
	const k = 2
	var body []byte
	var opsA []byte
	for i := 0; i < k; i++ {
		body = append(body, 0x41, 0x00, 0x41, 0x00)
		body = append(body, 0x10, 0x00)
		body = append(body, 0x1A)
		opsA = append(opsA, 0x41, 0x41, 0x10, 0x1A)
	}
	body = append(body, 0x02, 0x40) // block (void)
	body = append(body, 0x01)       // nop
	body = append(body, 0x0B)       // end (closes block; segment A terminator)
	opsA = append(opsA, 0x02, 0x01, 0x0B)

	body = append(body, 0x42, 0x2A) // i64.const 42 (segment B leader)
	body = append(body, 0x0B)       // end (function)
	opsB := []byte{0x42, 0x0B}

	instrumented, codeHash := buildAndInstrument(t, buildModule([]string{"storage_len"}, nil, []byte{0x00}, body, 1))

	memCost := module.InitialMemoryCost(1)
	costA := opsCost(t, opsA...)
	costB := opsCost(t, opsB...)
	hostTotal := uint64(k) * module.HostCost(module.HostStorageLen, 0)

	t.Run("one short: OutOfGas at segment B", func(t *testing.T) {
		e := newTestEngine(t)
		budget := memCost + costA + hostTotal + costB - 1
		out, err := e.Run(HostContext{State: newTestState(t, nil), ContractID: contractID}, codeHash, instrumented, nil, budget)
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

	t.Run("exact: Returned", func(t *testing.T) {
		e := newTestEngine(t)
		budget := memCost + costA + hostTotal + costB
		out, err := e.Run(HostContext{State: newTestState(t, nil), ContractID: contractID}, codeHash, instrumented, nil, budget)
		if err != nil {
			t.Fatalf("Run: %v", err)
		}
		if out.Kind != Returned {
			t.Fatalf("Kind = %v, want Returned", out.Kind)
		}
		if out.GasUsed != budget {
			t.Fatalf("GasUsed = %d, want %d", out.GasUsed, budget)
		}
	})
}

func TestHostChargeAtDepth512(t *testing.T) {
	instrumented, codeHash := buildAndInstrument(t, buildModule([]string{"storage_len"}, nil, []byte{0x00}, depthHostCallBody(), 1))
	e := newTestEngine(t)
	contractID := testContractID()

	frame := make([]byte, 511)
	out, err := e.Run(HostContext{State: newTestState(t, nil), ContractID: contractID}, codeHash, instrumented, frame, 1_000_000_000)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if out.Kind != Returned {
		t.Fatalf("Kind = %v, want Returned", out.Kind)
	}
	if out.Ret != 42 {
		t.Fatalf("Ret = %d, want 42", out.Ret)
	}
}

// abortBody returns i32.const code; i32.const ptr; i32.const dataLen; call abort; unreachable;
// end -- the trailing unreachable satisfies the i64 result type even though abort's host body
// always panics before it could ever execute.
func abortBody(code, ptr, dataLen int32) []byte {
	b := []byte{0x41}
	b = appendVarS32(b, code)
	b = append(b, 0x41)
	b = appendVarS32(b, ptr)
	b = append(b, 0x41)
	b = appendVarS32(b, dataLen)
	b = append(b, 0x10, 0x00) // call 0 (abort)
	b = append(b, 0x00)       // unreachable
	b = append(b, 0x0B)       // end
	return b
}

func TestAbortReturnsCode(t *testing.T) {
	contractID := testContractID()

	cases := []struct {
		name string
		code int32
	}{
		{"positive", 7},
		{"negative", -1},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			instrumented, codeHash := buildAndInstrument(t, buildModule([]string{"abort"}, nil, []byte{0x00}, abortBody(c.code, 0, 0), 1))
			e := newTestEngine(t)
			out, err := e.Run(HostContext{State: newTestState(t, nil), ContractID: contractID}, codeHash, instrumented, nil, 1_000_000)
			if err != nil {
				t.Fatalf("Run: %v", err)
			}
			if out.Kind != Aborted {
				t.Fatalf("Kind = %v, want Aborted", out.Kind)
			}
			if out.AbortCode != c.code {
				t.Fatalf("AbortCode = %d, want %d", out.AbortCode, c.code)
			}
			want := module.InitialMemoryCost(1) + opsCost(t, 0x41, 0x41, 0x41, 0x10, 0x00) + module.HostCost(module.HostAbort, 0)
			if out.GasUsed != want {
				t.Fatalf("GasUsed = %d, want %d", out.GasUsed, want)
			}
		})
	}
}

// TestAbortResolution covers env.abort's revert-data resolution: the size check, the
// data_len 0 short-circuit, the out-of-bounds trap and the data charge.
func TestAbortResolution(t *testing.T) {
	contractID := testContractID()
	pattern := make([]byte, codec.MaxReturnData)
	for i := range pattern {
		pattern[i] = byte(i)
	}
	memCost := module.InitialMemoryCost(1)
	segTotal := opsCost(t, 0x41, 0x41, 0x41, 0x10, 0x00)
	// The 4,096-byte data segment alone exceeds testLimits.MaxCodeSize.
	limits := testLimits
	limits.MaxCodeSize = 8192

	cases := []struct {
		name     string
		ptr      int32
		dataLen  int32
		budget   func(charge uint64) uint64
		wantKind OutcomeKind
		wantData []byte
	}{
		{"40 bytes", 1024, 40, nil, Aborted, pattern[:40]},
		{"zero length at any pointer", -1, 0, nil, Aborted, nil},
		{"MaxReturnData bytes", 1024, codec.MaxReturnData, nil, Aborted, pattern},
		{"over MaxReturnData out of memory", -16, codec.MaxReturnData + 1, nil, AbortTooLarge, nil},
		{"range crossing the end of memory", 65520, 32, nil, Trapped, nil},
		{"unaffordable data charge", 1024, 64, func(charge uint64) uint64 { return memCost + segTotal + charge - 1 }, OutOfGas, nil},
		{"unaffordable charge with an out-of-memory range", 65520, 64, func(charge uint64) uint64 { return memCost + segTotal + charge - 1 }, OutOfGas, nil},
		{"unaffordable charge with an oversized length", -16, codec.MaxReturnData + 1, func(charge uint64) uint64 { return memCost + segTotal + charge - 1 }, OutOfGas, nil},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			spec := &dataSeg{offset: 1024, bytes: pattern}
			wasm := buildModule([]string{"abort"}, spec, []byte{0x00}, abortBody(7, c.ptr, c.dataLen), 1)
			if err := module.Validate(wasm, limits); err != nil {
				t.Fatalf("Validate = %v, want nil", err)
			}
			instrumented, err := module.Instrument(wasm, limits)
			if err != nil {
				t.Fatalf("Instrument = %v, want nil", err)
			}
			codeHash := codeHashOf(instrumented)
			charge := module.HostCostU32(module.HostAbort, uint32(c.dataLen))
			budget := uint64(1_000_000)
			if c.budget != nil {
				budget = c.budget(charge)
			}
			e := newTestEngine(t)
			out, err := e.Run(HostContext{State: newTestState(t, nil), ContractID: contractID}, codeHash, instrumented, nil, budget)
			if err != nil {
				t.Fatalf("Run: %v", err)
			}
			if out.Kind != c.wantKind {
				t.Fatalf("Kind = %v, want %v", out.Kind, c.wantKind)
			}
			if c.wantKind == OutOfGas {
				return
			}
			if want := memCost + segTotal + charge; out.GasUsed != want {
				t.Fatalf("GasUsed = %d, want %d", out.GasUsed, want)
			}
			if !bytes.Equal(out.RevertData, c.wantData) || (c.wantData == nil && out.RevertData != nil) {
				t.Fatalf("RevertData = %d bytes, want %d bytes", len(out.RevertData), len(c.wantData))
			}
			if c.wantKind == Aborted && out.AbortCode != 7 {
				t.Fatalf("AbortCode = %d, want 7", out.AbortCode)
			}
			if c.wantKind != Aborted && out.AbortCode != 0 {
				t.Fatalf("AbortCode = %d, want 0", out.AbortCode)
			}
		})
	}
}

func TestAbortWithoutGasIsOutOfGas(t *testing.T) {
	contractID := testContractID()
	instrumented, codeHash := buildAndInstrument(t, buildModule([]string{"abort"}, nil, []byte{0x00}, abortBody(7, 0, 0), 1))
	memCost := module.InitialMemoryCost(1)
	segTotal := opsCost(t, 0x41, 0x41, 0x41, 0x10, 0x00)
	hostCost := module.HostCost(module.HostAbort, 0)

	t.Run("one short: OutOfGas", func(t *testing.T) {
		e := newTestEngine(t)
		budget := memCost + segTotal + hostCost - 1
		out, err := e.Run(HostContext{State: newTestState(t, nil), ContractID: contractID}, codeHash, instrumented, nil, budget)
		if err != nil {
			t.Fatalf("Run: %v", err)
		}
		if out.Kind != OutOfGas {
			t.Fatalf("Kind = %v, want OutOfGas", out.Kind)
		}
	})

	t.Run("exact: Aborted", func(t *testing.T) {
		e := newTestEngine(t)
		budget := memCost + segTotal + hostCost
		out, err := e.Run(HostContext{State: newTestState(t, nil), ContractID: contractID}, codeHash, instrumented, nil, budget)
		if err != nil {
			t.Fatalf("Run: %v", err)
		}
		if out.Kind != Aborted {
			t.Fatalf("Kind = %v, want Aborted", out.Kind)
		}
		if out.AbortCode != 7 {
			t.Fatalf("AbortCode = %d, want 7", out.AbortCode)
		}
	})
}

func TestStateErrorHalts(t *testing.T) {
	contractID := testContractID()
	instrumented, codeHash := buildAndInstrument(t, buildModule([]string{"storage_len"}, nil, []byte{0x00}, storageLenBody(2), 1))
	e := newTestEngine(t)

	out, err := e.Run(HostContext{State: failingState{}, ContractID: contractID}, codeHash, instrumented, []byte{0, 0}, 1_000_000)
	if err == nil {
		t.Fatal("Run err = nil, want non-nil")
	}
	if !errors.Is(err, errFailingState) {
		t.Errorf("Run err = %v, want to unwrap errFailingState", err)
	}
	if out.Kind != Returned || out.Ret != 0 || out.AbortCode != 0 || out.GasUsed != 0 || out.ResultBlob != nil {
		t.Errorf("Outcome = %+v, want zero", out)
	}
}
