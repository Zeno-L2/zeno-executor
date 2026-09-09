package wasm

import (
	"bytes"
	"fmt"
	"testing"
)

// wasmFuncType is one declared function type: raw value-type bytes for params and results.
type wasmFuncType struct {
	Params  []byte
	Results []byte
}

// wasmImport is one declared import. Module and Name are arbitrary: fixtures for §7.2
// import-rejection classes need pairs the validator refuses, which the env-only builders
// elsewhere in the tree cannot express.
type wasmImport struct {
	Module  string
	Name    string
	TypeIdx uint32
}

// wasmFunc is one locally defined function: its declared type index, its raw locals vector
// (0x00 for none) and its body, which must end in 0x0B.
type wasmFunc struct {
	TypeIdx uint32
	Locals  []byte
	Body    []byte
}

// wasmExport is one export entry; Kind is the section-7 kind byte (0x00 func, 0x02 memory).
type wasmExport struct {
	Name  string
	Kind  byte
	Index uint32
}

// wasmModuleSpec describes a whole module: arbitrary types, arbitrary imports, arbitrary
// function bodies, one memory, arbitrary exports and any number of active data segments.
// A data segment's offset must be below 2^31: it is emitted as an i32.const immediate, whose
// signed LEB128 encoding of a larger value is six bytes and is not a valid i32 constant.
type wasmModuleSpec struct {
	Types       []wasmFuncType
	Imports     []wasmImport
	Funcs       []wasmFunc
	MemoryPages uint32
	Exports     []wasmExport
	Data        []dataSeg
}

// buildWasmModule encodes spec as a WASM binary. Sections whose slice is empty are omitted,
// except the memory section, which is always emitted.
func buildWasmModule(spec wasmModuleSpec) []byte {
	var out []byte
	out = append(out, wasmHeader...)

	if len(spec.Types) > 0 {
		typeSec := appendVarU32(nil, uint32(len(spec.Types)))
		for _, ty := range spec.Types {
			typeSec = append(typeSec, encodeFuncType(ty.Params, ty.Results)...)
		}
		out = append(out, section(1, typeSec)...)
	}

	if len(spec.Imports) > 0 {
		importSec := appendVarU32(nil, uint32(len(spec.Imports)))
		for _, im := range spec.Imports {
			importSec = appendName(importSec, im.Module)
			importSec = appendName(importSec, im.Name)
			importSec = append(importSec, 0x00) // func import kind
			importSec = appendVarU32(importSec, im.TypeIdx)
		}
		out = append(out, section(2, importSec)...)
	}

	if len(spec.Funcs) > 0 {
		funcSec := appendVarU32(nil, uint32(len(spec.Funcs)))
		for _, f := range spec.Funcs {
			funcSec = appendVarU32(funcSec, f.TypeIdx)
		}
		out = append(out, section(3, funcSec)...)
	}

	memSec := appendVarU32([]byte{0x01, 0x00}, spec.MemoryPages)
	out = append(out, section(5, memSec)...)

	if len(spec.Exports) > 0 {
		exportSec := appendVarU32(nil, uint32(len(spec.Exports)))
		for _, ex := range spec.Exports {
			exportSec = appendName(exportSec, ex.Name)
			exportSec = append(exportSec, ex.Kind)
			exportSec = appendVarU32(exportSec, ex.Index)
		}
		out = append(out, section(7, exportSec)...)
	}

	if len(spec.Funcs) > 0 {
		codeSec := appendVarU32(nil, uint32(len(spec.Funcs)))
		for _, f := range spec.Funcs {
			fbody := append(append([]byte{}, f.Locals...), f.Body...)
			codeSec = appendVarU32(codeSec, uint32(len(fbody)))
			codeSec = append(codeSec, fbody...)
		}
		out = append(out, section(10, codeSec)...)
	}

	if len(spec.Data) > 0 {
		dataSec := appendVarU32(nil, uint32(len(spec.Data)))
		for _, d := range spec.Data {
			dataSec = append(dataSec, 0x00, 0x41)
			dataSec = appendVarS64(dataSec, int64(d.offset))
			dataSec = append(dataSec, 0x0B)
			dataSec = appendVarU32(dataSec, uint32(len(d.bytes)))
			dataSec = append(dataSec, d.bytes...)
		}
		out = append(out, section(11, dataSec)...)
	}

	return out
}

// runModuleSpec is the ordinary contract shape: run's (i32,i32)->i64 at type index 0, the
// named env imports declared ahead of it (env.storage_len shares run's type; env.storage_read
// is (i32,i32,i32,i32)->i64; env.abort is (i32)->()), one memory of pages, the "memory" and
// "run" exports with run at function index len(imports), one function carrying locals/body,
// and data.
func runModuleSpec(imports []string, pages uint32, locals, body []byte, data []dataSeg) wasmModuleSpec {
	runType := wasmFuncType{Params: []byte{0x7F, 0x7F}, Results: []byte{0x7E}}
	types := []wasmFuncType{runType}
	typeIdx := map[string]uint32{"storage_len": 0}

	var imps []wasmImport
	for _, name := range imports {
		switch name {
		case "storage_len":
			imps = append(imps, wasmImport{Module: "env", Name: name, TypeIdx: typeIdx["storage_len"]})
		case "storage_read":
			idx, ok := typeIdx["storage_read"]
			if !ok {
				types = append(types, wasmFuncType{Params: []byte{0x7F, 0x7F, 0x7F, 0x7F}, Results: []byte{0x7E}})
				idx = uint32(len(types) - 1)
				typeIdx["storage_read"] = idx
			}
			imps = append(imps, wasmImport{Module: "env", Name: name, TypeIdx: idx})
		case "abort":
			idx, ok := typeIdx["abort"]
			if !ok {
				types = append(types, wasmFuncType{Params: []byte{0x7F}, Results: nil})
				idx = uint32(len(types) - 1)
				typeIdx["abort"] = idx
			}
			imps = append(imps, wasmImport{Module: "env", Name: name, TypeIdx: idx})
		default:
			panic(fmt.Sprintf("wasm: runModuleSpec: unknown import %q", name))
		}
	}

	runIdx := uint32(len(imps))
	return wasmModuleSpec{
		Types:       types,
		Imports:     imps,
		Funcs:       []wasmFunc{{TypeIdx: 0, Locals: locals, Body: body}},
		MemoryPages: pages,
		Exports: []wasmExport{
			{Name: "memory", Kind: 0x02, Index: 0},
			{Name: "run", Kind: 0x00, Index: runIdx},
		},
		Data: data,
	}
}

// TestBuildWasmModuleSignsDataOffsets pins the data segment's i32.const immediate at offset
// 100, where signed LEB128 (e4 00) and unsigned LEB128 (64, which decodes as -28) diverge.
func TestBuildWasmModuleSignsDataOffsets(t *testing.T) {
	spec := runModuleSpec(nil, 1, []byte{0x00}, []byte{0x42, 0x00, 0x0B}, []dataSeg{{offset: 100, bytes: []byte{0xAA}}})
	got := buildWasmModule(spec)

	want := []byte{0x41, 0xE4, 0x00, 0x0B, 0x01, 0xAA}
	if !bytes.Contains(got, want) {
		t.Fatalf("buildWasmModule output does not contain the signed LEB128 encoding %x", want)
	}

	notWant := []byte{0x41, 0x64, 0x0B}
	if bytes.Contains(got, notWant) {
		t.Fatalf("buildWasmModule output contains the unsigned LEB128 encoding %x, want only the signed form", notWant)
	}
}
