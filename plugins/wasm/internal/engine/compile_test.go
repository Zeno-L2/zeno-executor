package engine

import (
	"context"
	"errors"
	"testing"

	"github.com/Zeno-L2/zeno-executor/plugins/wasm/internal/module"
)

// wasmHeader is the \0asm magic and version 1.
var wasmHeader = []byte{0x00, 'a', 's', 'm', 0x01, 0x00, 0x00, 0x00}

// section builds one section: id, length-prefixed payload.
func section(id byte, payload []byte) []byte {
	out := []byte{id}
	out = appendVarU32(out, uint32(len(payload)))
	return append(out, payload...)
}

// appendVarU32 is the same shortest-form unsigned LEB128 encoder module.appendVarU32
// implements; duplicated here because the module builder is internal/module-private (a
// duplicated ~40-byte literal is cheaper than exporting a builder across packages).
func appendVarU32(dst []byte, v uint32) []byte {
	for {
		b := byte(v & 0x7F)
		v >>= 7
		if v != 0 {
			dst = append(dst, b|0x80)
			continue
		}
		return append(dst, b)
	}
}

// minimalModule builds the same minimal conforming module module.Validate accepts: one type
// (i32,i32) -> i64, one function, one memory {min 1}, exports "memory" and "run", and a body
// of i64.const 0; end.
func minimalModule() []byte {
	var out []byte
	out = append(out, wasmHeader...)

	typeSec := []byte{0x01, 0x60, 0x02, 0x7F, 0x7F, 0x01, 0x7E} // 1 type, form 0x60, params(i32,i32), results(i64)
	out = append(out, section(1, typeSec)...)

	funcSec := []byte{0x01, 0x00} // 1 function, type index 0
	out = append(out, section(3, funcSec)...)

	memSec := []byte{0x01, 0x00, 0x01} // 1 memory, flags 0, min 1
	out = append(out, section(5, memSec)...)

	exportSec := []byte{
		0x02,                                           // 2 exports
		0x06, 'm', 'e', 'm', 'o', 'r', 'y', 0x02, 0x00, // "memory" -> memory 0
		0x03, 'r', 'u', 'n', 0x00, 0x00, // "run" -> func 0
	}
	out = append(out, section(7, exportSec)...)

	body := []byte{0x00, 0x42, 0x00, 0x0B} // 0 locals, i64.const 0, end
	codeSec := []byte{0x01, byte(len(body))}
	codeSec = append(codeSec, body...)
	out = append(out, section(10, codeSec)...)

	return out
}

// illTypedModule is module.Validate-accepted (our validator performs no type checking) but
// not well-typed: run is declared -> i64 with a body that leaves an i32 on the stack.
func illTypedModule() []byte {
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

	body := []byte{0x00, 0x41, 0x00, 0x0B} // 0 locals, i32.const 0, end (wrong result type)
	codeSec := []byte{0x01, byte(len(body))}
	codeSec = append(codeSec, body...)
	out = append(out, section(10, codeSec)...)

	return out
}

// storageLenImportModule imports env.storage_len and otherwise is the minimal module.
func storageLenImportModule() []byte {
	var out []byte
	out = append(out, wasmHeader...)

	typeSec := []byte{0x01, 0x60, 0x02, 0x7F, 0x7F, 0x01, 0x7E}
	out = append(out, section(1, typeSec)...)

	importSec := []byte{
		0x01, // 1 import
		0x03, 'e', 'n', 'v', 0x0B, 's', 't', 'o', 'r', 'a', 'g', 'e', '_', 'l', 'e', 'n', 0x00, 0x00,
	}
	out = append(out, section(2, importSec)...)

	funcSec := []byte{0x01, 0x00} // 1 locally defined function (index 1, after the 1 import)
	out = append(out, section(3, funcSec)...)

	memSec := []byte{0x01, 0x00, 0x01}
	out = append(out, section(5, memSec)...)

	exportSec := []byte{
		0x02,
		0x06, 'm', 'e', 'm', 'o', 'r', 'y', 0x02, 0x00,
		0x03, 'r', 'u', 'n', 0x00, 0x01, // run -> func index 1 (the local one)
	}
	out = append(out, section(7, exportSec)...)

	body := []byte{0x00, 0x42, 0x00, 0x0B}
	codeSec := []byte{0x01, byte(len(body))}
	codeSec = append(codeSec, body...)
	out = append(out, section(10, codeSec)...)

	return out
}

// memoryMax100Module declares memory {min 1, max 100}, which module.Validate accepts (rule
// 7's deliberate acceptance) and which the pinned engine must clamp down to its own
// WithMemoryLimitPages value rather than reject.
func memoryMax100Module() []byte {
	var out []byte
	out = append(out, wasmHeader...)

	typeSec := []byte{0x01, 0x60, 0x02, 0x7F, 0x7F, 0x01, 0x7E}
	out = append(out, section(1, typeSec)...)

	funcSec := []byte{0x01, 0x00}
	out = append(out, section(3, funcSec)...)

	memSec := []byte{0x01, 0x01, 0x01, 0x64} // 1 memory, flags 1 (has max), min 1, max 100
	out = append(out, section(5, memSec)...)

	exportSec := []byte{
		0x02,
		0x06, 'm', 'e', 'm', 'o', 'r', 'y', 0x02, 0x00,
		0x03, 'r', 'u', 'n', 0x00, 0x00,
	}
	out = append(out, section(7, exportSec)...)

	body := []byte{0x00, 0x42, 0x00, 0x0B}
	codeSec := []byte{0x01, byte(len(body))}
	codeSec = append(codeSec, body...)
	out = append(out, section(10, codeSec)...)

	return out
}

var testLimits = module.Limits{
	MaxCodeSize:            4096,
	MaxMemoryPages:         64,
	MaxLocalsPerFunction:   8,
	MaxTotalLocals:         10,
	MaxTableEntries:        16,
	MaxTotalParams:         20,
	MaxGlobals:             4,
	MaxDataSegments:        4,
	MaxFunctions:           8,
	MaxTotalElementEntries: 16,
	MaxTypeCount:           8,
}

func TestCompileAcceptsValidatorAcceptedModule(t *testing.T) {
	wasm := minimalModule()
	if err := module.Validate(wasm, testLimits); err != nil {
		t.Fatalf("module.Validate(minimal) = %v, want nil", err)
	}
	if err := Compile(context.Background(), wasm); err != nil {
		t.Errorf("Compile(minimal) = %v, want nil", err)
	}
}

func TestCompileRejectsIllTypedModule(t *testing.T) {
	wasm := illTypedModule()
	if err := module.Validate(wasm, testLimits); err != nil {
		t.Fatalf("module.Validate(ill-typed) = %v, want nil (our validator does not type-check)", err)
	}
	err := Compile(context.Background(), wasm)
	if !errors.Is(err, ErrEngineRejected) {
		t.Errorf("Compile(ill-typed) = %v, want ErrEngineRejected", err)
	}
}

func TestCompileResolvesNoImports(t *testing.T) {
	wasm := storageLenImportModule()
	if err := module.Validate(wasm, testLimits); err != nil {
		t.Fatalf("module.Validate(storage_len import) = %v, want nil", err)
	}
	if err := Compile(context.Background(), wasm); err != nil {
		t.Errorf("Compile(storage_len import) = %v, want nil (no host module registered)", err)
	}
}

func TestCompileClampsDeclaredMaxAboveLimit(t *testing.T) {
	wasm := memoryMax100Module()
	if err := module.Validate(wasm, testLimits); err != nil {
		t.Fatalf("module.Validate(memory max 100) = %v, want nil", err)
	}
	if err := Compile(context.Background(), wasm); err != nil {
		t.Errorf("Compile(memory max 100) = %v, want nil (clamped, not rejected)", err)
	}
}
