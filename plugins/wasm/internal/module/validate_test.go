package module

import (
	"errors"
	"testing"
)

// testLimits is the small Limits used throughout so the cap fixtures stay a few hundred bytes
// rather than megabytes.
var testLimits = Limits{
	MaxCodeSize:            4096,
	MaxMemoryPages:         64,
	MaxLocalsPerFunction:   8,
	MaxTotalLocals:         10,
	MaxTableEntries:        16,
	MaxTotalParams:         20,
	MaxGlobals:             4,
	MaxDataSegments:        2,
	MaxFunctions:           4,
	MaxTotalElementEntries: 24,
	MaxTypeCount:           8,
}

// constI32 returns a literal i32.const v; end const-expr, the shape every offset and every
// i32-typed global initializer in these fixtures uses.
func constI32(v int32) []byte {
	return append(append([]byte{0x41}, appendVarS32(nil, v)...), 0x0B)
}

// setRunIndex points the "run" export at the local function index implied by the current
// import count, for fixtures that mutate imports after the export was set up.
func setRunIndex(b *builder) {
	for i := range b.exports {
		if b.exports[i].name == "run" {
			b.exports[i].index = uint32(len(b.imports))
		}
	}
}

// builderWithHostImports returns a builder importing exactly the named host functions (in
// order), each with its required signature, with "run" a locally defined function declared
// after them.
func builderWithHostImports(names []string) *builder {
	sigs := map[string]funcType{
		"storage_len":  {form: 0x60, params: []byte{0x7F, 0x7F}, results: []byte{0x7E}},
		"storage_read": {form: 0x60, params: []byte{0x7F, 0x7F, 0x7F, 0x7F}, results: []byte{0x7E}},
		"abort":        {form: 0x60, params: []byte{0x7F, 0x7F, 0x7F}, results: nil},
	}
	b := &builder{}
	for _, n := range names {
		b.types = append(b.types, sigs[n])
	}
	runTypeIdx := uint32(len(b.types))
	b.types = append(b.types, funcType{form: 0x60, params: []byte{0x7F, 0x7F}, results: []byte{0x7E}})
	for i, n := range names {
		b.imports = append(b.imports, importEntry{module: "env", name: n, desc: 0x00, typeIdx: uint32(i)})
	}
	b.funcs = []uint32{runTypeIdx}
	b.memories = []limits{{flags: 0x00, min: 1}}
	b.exports = []exportEntry{
		{name: "memory", kind: 0x02, index: 0},
		{name: "run", kind: 0x00, index: uint32(len(names))},
	}
	b.bodies = []body{{code: []byte{0x42, 0x00, 0x0B}}}
	return b
}

// bigBody exercises block/loop/if/else/br/br_if/br_table, call, call_indirect, a load and a
// store, memory.size/memory.grow, and i32.const/i64.const, ending with the function's final
// end.
func bigBody() []byte {
	var code []byte
	emit := func(bs ...byte) { code = append(code, bs...) }
	u32 := func(v uint32) { code = appendVarU32(code, v) }
	s32 := func(v int32) { code = appendVarS32(code, v) }

	emit(0x02, 0x40) // block (empty)
	emit(0x41)
	s32(0) // i32.const 0
	emit(0x04, 0x40)
	emit(0x41)
	s32(1)     // i32.const 1
	emit(0x1A) // drop
	emit(0x05) // else
	emit(0x41)
	s32(2)           // i32.const 2
	emit(0x1A)       // drop
	emit(0x0B)       // end (if)
	emit(0x03, 0x40) // loop (empty)
	emit(0x0D)
	u32(0)     // br_if 0 (loop label)
	emit(0x0B) // end (loop)
	emit(0x0B) // end (block)
	emit(0x41)
	s32(0) // i32.const 0 (br_table selector)
	emit(0x0E)
	u32(1)     // 1 target
	u32(0)     // target[0]
	u32(0)     // default
	emit(0x10) // call
	u32(0)
	emit(0x11) // call_indirect
	u32(0)
	u32(0)
	emit(0x28) // i32.load
	u32(0)
	u32(0)
	emit(0x36) // i32.store
	u32(0)
	u32(0)
	emit(0x3F, 0x00) // memory.size
	emit(0x40, 0x00) // memory.grow
	emit(0x42)
	code = appendVarS64(code, 0) // i64.const 0
	emit(0x0B)                   // end (function)
	return code
}

// rejectionCases is the binding fixture-to-reason mapping; the error-coverage guard
// enumerates it.
var rejectionCases = []struct {
	name string
	wasm []byte
	want error
}{
	{
		name: "bad magic",
		wasm: newBuilder().patchAt(0, 0xFF).bytes(),
		want: ErrBadMagic,
	},
	{
		name: "version 02 00 00 00",
		wasm: newBuilder().patchAt(4, 0x02, 0x00, 0x00, 0x00).bytes(),
		want: ErrBadVersion,
	},
	{
		name: "memory section emitted before type section",
		wasm: func() []byte {
			b := newBuilder()
			b.order = []byte{5, 1, 3, 7, 10}
			return b.bytes()
		}(),
		want: ErrSectionOrder,
	},
	{
		name: "two type sections",
		wasm: func() []byte {
			b := newBuilder()
			b.raws = []rawSection{{id: 1, payload: nil, replace: false}}
			return b.bytes()
		}(),
		want: ErrDuplicateSection,
	},
	{
		name: "section id 12",
		wasm: func() []byte {
			b := newBuilder()
			b.raws = []rawSection{{id: 12, payload: nil, replace: false}}
			return b.bytes()
		}(),
		want: ErrUnknownSection,
	},
	{
		name: "trailing byte after the last section",
		wasm: newBuilder().appendRaw(0xFF).bytes(),
		want: ErrTrailingBytes,
	},
	{
		name: "custom section with trivial contents",
		wasm: func() []byte {
			b := newBuilder()
			b.customs = []customSection{{name: ".debug_info", payload: []byte{1, 2, 3}, afterID: 0}}
			return b.bytes()
		}(),
		want: ErrCustomSection,
	},
	{
		// The 66-byte "name" section repro: a function-names subsection declaring
		// functionCount = 2^32-1, which wazero's decoder would otherwise size a NameMap from
		// with no bound check.
		name: "custom name section with functionCount 2^32-1",
		wasm: func() []byte {
			b := newBuilder()
			b.customs = []customSection{{
				name:    "name",
				payload: []byte{0x01, 0x05, 0xFF, 0xFF, 0xFF, 0xFF, 0x0F},
				afterID: 0,
			}}
			return b.bytes()
		}(),
		want: ErrCustomSection,
	},
	{
		// The same subsection kind, but a local-names subsection declaring localCount =
		// 2^32-1.
		name: "custom name section with localCount 2^32-1",
		wasm: func() []byte {
			b := newBuilder()
			b.customs = []customSection{{
				name:    "name",
				payload: []byte{0x02, 0x07, 0x01, 0x00, 0xFF, 0xFF, 0xFF, 0xFF, 0x0F},
				afterID: 0,
			}}
			return b.bytes()
		}(),
		want: ErrCustomSection,
	},
	{
		// The module-name subsection declaring a byte-string size of 2^32-1.
		name: "custom name section with module-name size 2^32-1",
		wasm: func() []byte {
			b := newBuilder()
			b.customs = []customSection{{
				name:    "name",
				payload: []byte{0x00, 0x05, 0xFF, 0xFF, 0xFF, 0xFF, 0x0F},
				afterID: 0,
			}}
			return b.bytes()
		}(),
		want: ErrCustomSection,
	},
	{
		name: "section length longer than the module",
		wasm: newBuilder().patchFirst(
			[]byte{0x01, 0x07, 0x01, 0x60},
			[]byte{0x01, 0x7F, 0x01, 0x60},
		).bytes(),
		want: ErrTruncated,
	},
	{
		name: "section with one unconsumed payload byte",
		wasm: newBuilder().patchFirst(
			[]byte{0x01, 0x07, 0x01, 0x60},
			[]byte{0x01, 0x08, 0x01, 0x60},
		).bytes(),
		want: ErrSectionSizeMismatch,
	},
	{
		name: "type section declaring 0xFFFFFFFF entries with an empty payload",
		wasm: func() []byte {
			b := newBuilder()
			b.raws = []rawSection{{id: 1, payload: appendVarU32(nil, 0xFFFFFFFF), replace: true}}
			return b.bytes()
		}(),
		want: ErrSectionSizeMismatch,
	},
	{
		name: "i32.const immediate encoded non-canonically",
		wasm: func() []byte {
			b := newBuilder()
			b.bodies = []body{{code: []byte{0x41, 0x80, 0x80, 0x80, 0x80, 0x00, 0x1A, 0x42, 0x00, 0x0B}}}
			return b.bytes()
		}(),
		want: ErrNonCanonicalLEB128,
	},
	{
		name: "local.get with a six-continuation-byte varu32",
		wasm: func() []byte {
			b := newBuilder()
			b.bodies = []body{{code: []byte{0x20, 0x80, 0x80, 0x80, 0x80, 0x80, 0x01}}}
			return b.bytes()
		}(),
		want: ErrIntegerTooLarge,
	},
	{
		name: "function type with two results",
		wasm: func() []byte {
			b := newBuilder()
			b.types[0].results = []byte{0x7E, 0x7E}
			return b.bytes()
		}(),
		want: ErrMultiValueResult,
	},
	{
		name: "type form byte 0x61",
		wasm: func() []byte {
			b := newBuilder()
			b.types[0].form = 0x61
			return b.bytes()
		}(),
		want: ErrMalformedSection,
	},
	{
		name: "import descriptor byte 0x04",
		wasm: func() []byte {
			b := newBuilder()
			b.imports = []importEntry{{module: "env", name: "abort", desc: 0x04, typeIdx: 0}}
			return b.bytes()
		}(),
		want: ErrMalformedSection,
	},
	{
		name: "valtype 0x69 in a function type",
		wasm: func() []byte {
			b := newBuilder()
			b.types[0].params = []byte{0x69}
			return b.bytes()
		}(),
		want: ErrInvalidValType,
	},
	{
		name: "block type 0x7A",
		wasm: func() []byte {
			b := newBuilder()
			b.bodies = []body{{code: []byte{0x02, 0x7A}}}
			return b.bytes()
		}(),
		want: ErrInvalidValType,
	},
	{
		name: "block type 0x00 (a type index)",
		wasm: func() []byte {
			b := newBuilder()
			b.bodies = []body{{code: []byte{0x02, 0x00}}}
			return b.bytes()
		}(),
		want: ErrBlockTypeIndex,
	},
	{
		name: "f32 in a function type",
		wasm: func() []byte {
			b := newBuilder()
			b.types[0].params = []byte{0x7D}
			return b.bytes()
		}(),
		want: ErrFloatType,
	},
	{
		name: "f32 in a global type",
		wasm: func() []byte {
			b := newBuilder()
			b.globals = []globalEntry{{valType: 0x7D, mut: 0x00, init: []byte{0x41, 0x00, 0x0B}}}
			return b.bytes()
		}(),
		want: ErrFloatType,
	},
	{
		name: "f32 in a local declaration",
		wasm: func() []byte {
			b := newBuilder()
			b.bodies = []body{{locals: []localDecl{{count: 1, valType: 0x7D}}, code: []byte{0x42, 0x00, 0x0B}}}
			return b.bytes()
		}(),
		want: ErrFloatType,
	},
	{
		name: "f64 param in an imported abort signature's type",
		wasm: func() []byte {
			b := newBuilder()
			b.types = append(b.types, funcType{form: 0x60, params: []byte{0x7C}})
			b.imports = []importEntry{{module: "env", name: "abort", desc: 0x00, typeIdx: 1}}
			setRunIndex(b)
			return b.bytes()
		}(),
		want: ErrFloatType,
	},
	{
		name: "f64.add in a body",
		wasm: func() []byte {
			b := newBuilder()
			b.bodies = []body{{code: []byte{0xA0}}}
			return b.bytes()
		}(),
		want: ErrFloatInstruction,
	},
	{
		name: "i32.extend8_s",
		wasm: func() []byte {
			b := newBuilder()
			b.bodies = []body{{code: []byte{0xC0}}}
			return b.bytes()
		}(),
		want: ErrSignExtension,
	},
	{
		name: "i32.trunc_sat_f32_s",
		wasm: func() []byte {
			b := newBuilder()
			b.bodies = []body{{code: []byte{0xFC, 0x00}}}
			return b.bytes()
		}(),
		want: ErrSaturatingConversion,
	},
	{
		name: "memory.copy",
		wasm: func() []byte {
			b := newBuilder()
			b.bodies = []body{{code: []byte{0xFC, 0x0A}}}
			return b.bytes()
		}(),
		want: ErrBulkMemory,
	},
	{
		name: "memory.fill",
		wasm: func() []byte {
			b := newBuilder()
			b.bodies = []body{{code: []byte{0xFC, 0x0B}}}
			return b.bytes()
		}(),
		want: ErrBulkMemory,
	},
	{
		name: "typed select",
		wasm: func() []byte {
			b := newBuilder()
			b.bodies = []body{{code: []byte{0x1C}}}
			return b.bytes()
		}(),
		want: ErrTypedSelect,
	},
	{
		name: "v128.const",
		wasm: func() []byte {
			b := newBuilder()
			b.bodies = []body{{code: []byte{0xFD, 0x0C}}}
			return b.bytes()
		}(),
		want: ErrSIMD,
	},
	{
		name: "i32.atomic.load",
		wasm: func() []byte {
			b := newBuilder()
			b.bodies = []body{{code: []byte{0xFE, 0x10}}}
			return b.bytes()
		}(),
		want: ErrAtomics,
	},
	{
		name: "ref.null",
		wasm: func() []byte {
			b := newBuilder()
			b.bodies = []body{{code: []byte{0xD0}}}
			return b.bytes()
		}(),
		want: ErrReferenceType,
	},
	{
		name: "try",
		wasm: func() []byte {
			b := newBuilder()
			b.bodies = []body{{code: []byte{0x06}}}
			return b.bytes()
		}(),
		want: ErrExceptionHandling,
	},
	{
		name: "table.get",
		wasm: func() []byte {
			b := newBuilder()
			b.bodies = []body{{code: []byte{0x25, 0x00}}}
			return b.bytes()
		}(),
		want: ErrUnmeteredOpcode,
	},
	{
		name: "return_call",
		wasm: func() []byte {
			b := newBuilder()
			b.bodies = []body{{code: []byte{0x12, 0x00}}}
			return b.bytes()
		}(),
		want: ErrUnmeteredOpcode,
	},
	{
		name: "opcode 0xFF",
		wasm: func() []byte {
			b := newBuilder()
			b.bodies = []body{{code: []byte{0xFF}}}
			return b.bytes()
		}(),
		want: ErrUnmeteredOpcode,
	},
	{
		name: "import from wasi_snapshot_preview1",
		wasm: func() []byte {
			b := newBuilder()
			b.imports = []importEntry{{module: "wasi_snapshot_preview1", name: "fd_write", desc: 0x00, typeIdx: 0}}
			return b.bytes()
		}(),
		want: ErrDisallowedImport,
	},
	{
		name: "fourth env import",
		wasm: func() []byte {
			b := builderWithHostImports([]string{"storage_len", "storage_read", "abort"})
			b.imports = append(b.imports, importEntry{module: "env", name: "abort", desc: 0x00, typeIdx: 2})
			setRunIndex(b)
			return b.bytes()
		}(),
		want: ErrDisallowedImport,
	},
	{
		name: "imported memory",
		wasm: func() []byte {
			b := newBuilder()
			b.imports = []importEntry{{module: "env", name: "mem", desc: 0x02, typeIdx: 0}}
			return b.bytes()
		}(),
		want: ErrDisallowedImport,
	},
	{
		name: "env.storage_len typed (i32) -> i64",
		wasm: func() []byte {
			b := newBuilder()
			b.types = append(b.types, funcType{form: 0x60, params: []byte{0x7F}, results: []byte{0x7E}})
			b.imports = []importEntry{{module: "env", name: "storage_len", desc: 0x00, typeIdx: 1}}
			setRunIndex(b)
			return b.bytes()
		}(),
		want: ErrImportSignature,
	},
	{
		name: "env.abort typed (i32) -> ()",
		wasm: func() []byte {
			b := newBuilder()
			b.types = append(b.types, funcType{form: 0x60, params: []byte{0x7F}, results: nil})
			b.imports = []importEntry{{module: "env", name: "abort", desc: 0x00, typeIdx: 1}}
			setRunIndex(b)
			return b.bytes()
		}(),
		want: ErrImportSignature,
	},
	{
		name: "run typed (i32,i32) -> i32",
		wasm: func() []byte {
			b := newBuilder()
			b.types[0].results = []byte{0x7F}
			return b.bytes()
		}(),
		want: ErrRunSignature,
	},
	{
		name: "run exported as func 0 where func 0 is the imported env.storage_len",
		wasm: func() []byte {
			b := builderWithHostImports([]string{"storage_len"})
			for i := range b.exports {
				if b.exports[i].name == "run" {
					b.exports[i].index = 0
				}
			}
			return b.bytes()
		}(),
		want: ErrRunImported,
	},
	{
		name: "no memory export",
		wasm: func() []byte {
			b := newBuilder()
			b.exports = []exportEntry{{name: "run", kind: 0x00, index: 0}}
			return b.bytes()
		}(),
		want: ErrMissingExport,
	},
	{
		name: "no run export",
		wasm: func() []byte {
			b := newBuilder()
			b.exports = []exportEntry{{name: "memory", kind: 0x02, index: 0}}
			return b.bytes()
		}(),
		want: ErrMissingExport,
	},
	{
		name: "start section",
		wasm: func() []byte {
			b := newBuilder()
			s := uint32(0)
			b.start = &s
			return b.bytes()
		}(),
		want: ErrStartSection,
	},
	{
		name: "export named _start",
		wasm: func() []byte {
			b := newBuilder()
			b.exports = append(b.exports, exportEntry{name: "_start", kind: 0x00, index: 0})
			return b.bytes()
		}(),
		want: ErrReservedExport,
	},
	{
		name: "export named __zenon_gas",
		wasm: func() []byte {
			b := newBuilder()
			b.exports = append(b.exports, exportEntry{name: "__zenon_gas", kind: 0x00, index: 0})
			return b.bytes()
		}(),
		want: ErrReservedExport,
	},
	{
		name: "exported mutable global",
		wasm: func() []byte {
			b := newBuilder()
			b.globals = []globalEntry{{valType: 0x7F, mut: 0x01, init: []byte{0x41, 0x00, 0x0B}}}
			b.exports = append(b.exports, exportEntry{name: "g", kind: 0x03, index: 0})
			return b.bytes()
		}(),
		want: ErrMutableGlobalExport,
	},
	{
		name: "global mutability byte 0x02",
		wasm: func() []byte {
			b := newBuilder()
			b.globals = []globalEntry{{valType: 0x7F, mut: 0x02, init: []byte{0x41, 0x00, 0x0B}}}
			return b.bytes()
		}(),
		want: ErrInvalidMutability,
	},
	{
		name: "global initializer global.get 0",
		wasm: func() []byte {
			b := newBuilder()
			b.globals = []globalEntry{{valType: 0x7F, mut: 0x00, init: []byte{0x23, 0x00, 0x0B}}}
			return b.bytes()
		}(),
		want: ErrNonConstantInitializer,
	},
	{
		name: "global.get 0 with no globals",
		wasm: func() []byte {
			b := newBuilder()
			b.bodies = []body{{code: []byte{0x23, 0x00}}}
			return b.bytes()
		}(),
		want: ErrGlobalIndexOutOfBounds,
	},
	{
		name: "call 7 with one function",
		wasm: func() []byte {
			b := newBuilder()
			b.bodies = []body{{code: []byte{0x10, 0x07}}}
			return b.bytes()
		}(),
		want: ErrFunctionIndexOutOfBounds,
	},
	{
		name: "br 3 at control depth 1",
		wasm: func() []byte {
			b := newBuilder()
			b.bodies = []body{{code: []byte{0x0C, 0x03}}}
			return b.bytes()
		}(),
		want: ErrLabelOutOfBounds,
	},
	{
		name: "local.get 9 with 2 params, 0 locals",
		wasm: func() []byte {
			b := newBuilder()
			b.bodies = []body{{code: []byte{0x20, 0x09}}}
			return b.bytes()
		}(),
		want: ErrLocalIndexOutOfBounds,
	},
	{
		name: "call_indirect type index past the type section",
		wasm: func() []byte {
			b := newBuilder()
			b.bodies = []body{{code: []byte{0x11, 0x05, 0x00}}}
			return b.bytes()
		}(),
		want: ErrTypeIndexOutOfBounds,
	},
	{
		name: "element segment with no table section",
		wasm: func() []byte {
			b := newBuilder()
			b.elements = []elemSeg{{flags: 0x00, offset: constI32(0), funcs: []uint32{0}}}
			return b.bytes()
		}(),
		want: ErrTableIndexOutOfBounds,
	},
	{
		name: "memory.size with reserved byte 0x01",
		wasm: func() []byte {
			b := newBuilder()
			b.bodies = []body{{code: []byte{0x3F, 0x01}}}
			return b.bytes()
		}(),
		want: ErrMemoryIndexOutOfBounds,
	},
	{
		name: "code section with two bodies, function section with one",
		wasm: func() []byte {
			b := newBuilder()
			b.bodies = append(b.bodies, body{code: []byte{0x0B}})
			return b.bytes()
		}(),
		want: ErrFunctionCodeMismatch,
	},
	{
		name: "body whose final end is missing",
		wasm: func() []byte {
			b := newBuilder()
			b.bodies = []body{{code: []byte{0x42, 0x00}}}
			return b.bytes()
		}(),
		want: ErrMalformedFunctionBody,
	},
	{
		name: "module one byte over MaxCodeSize",
		wasm: func() []byte {
			base := len(newBuilder().bytes())
			pad := testLimits.MaxCodeSize + 1 - base
			b := newBuilder()
			b.appendRaw(make([]byte, pad)...)
			return b.bytes()
		}(),
		want: ErrModuleTooLarge,
	},
	{
		name: "declared locals MaxLocalsPerFunction + 1",
		wasm: func() []byte {
			b := newBuilder()
			b.bodies = []body{{locals: []localDecl{{count: 7, valType: 0x7F}}, code: []byte{0x42, 0x00, 0x0B}}}
			return b.bytes()
		}(),
		want: ErrTooManyLocals,
	},
	{
		// A used type's params over the per-type cap is rejected by parseTypes, before the
		// function or code sections that would otherwise hit parseLocalsAndCheck's
		// MaxLocalsPerFunction check are even parsed.
		name: "type params exceed MaxLocalsPerFunction",
		wasm: func() []byte {
			b := newBuilder()
			b.types = append(b.types, funcType{form: 0x60, params: make([]byte, testLimits.MaxLocalsPerFunction+1)})
			for i := range b.types[1].params {
				b.types[1].params[i] = 0x7F
			}
			b.funcs = append(b.funcs, 1)
			b.bodies = append(b.bodies, body{code: []byte{0x0B}})
			return b.bytes()
		}(),
		want: ErrTooManyParams,
	},
	{
		// An unused type's params over the per-type cap is rejected the same way: nothing in
		// parseTypes depends on whether any function references the type.
		name: "unused type params exceed MaxLocalsPerFunction",
		wasm: func() []byte {
			b := newBuilder()
			params := make([]byte, testLimits.MaxLocalsPerFunction+1)
			for i := range params {
				params[i] = 0x7F
			}
			b.types = append(b.types, funcType{form: 0x60, params: params})
			return b.bytes()
		}(),
		want: ErrTooManyParams,
	},
	{
		name: "type section total params one over MaxTotalParams",
		wasm: func() []byte {
			b := newBuilder() // base type: 2 params
			eightParams := func() []byte {
				p := make([]byte, testLimits.MaxLocalsPerFunction)
				for i := range p {
					p[i] = 0x7F
				}
				return p
			}
			b.types = append(b.types,
				funcType{form: 0x60, params: eightParams()},
				funcType{form: 0x60, params: eightParams()},
				funcType{form: 0x60, params: []byte{0x7F, 0x7F, 0x7F}},
			) // 2 + 8 + 8 + 3 = 21
			return b.bytes()
		}(),
		want: ErrTotalParamsExceeded,
	},
	{
		name: "total locals across functions exceed MaxTotalLocals",
		wasm: func() []byte {
			b := newBuilder()
			voidType := funcType{form: 0x60}
			b.types = append(b.types, voidType)
			runTypeIdx := uint32(1)
			b.funcs = append(b.funcs, runTypeIdx, runTypeIdx)
			b.bodies = append(b.bodies,
				body{locals: []localDecl{{count: 8, valType: 0x7F}}, code: []byte{0x0B}},
				body{locals: []localDecl{{count: 1, valType: 0x7F}}, code: []byte{0x0B}},
			)
			return b.bytes()
		}(),
		want: ErrTotalLocalsExceeded,
	},
	{
		name: "function section with no code section",
		wasm: func() []byte {
			b := newBuilder()
			b.bodies = nil
			return b.bytes()
		}(),
		want: ErrFunctionCodeMismatch,
	},
	{
		name: "type section length re-encoded as a padded two-byte LEB128",
		wasm: newBuilder().patchFirst(
			[]byte{0x01, 0x07, 0x01, 0x60},
			[]byte{0x01, 0x87, 0x00, 0x01, 0x60},
		).bytes(),
		want: ErrNonCanonicalLEB128,
	},
	{
		name: "table minimum MaxTableEntries + 1",
		wasm: func() []byte {
			b := newBuilder()
			b.tables = []tableEntry{{elemType: 0x70, lim: limits{flags: 0x00, min: 17}}}
			return b.bytes()
		}(),
		want: ErrTableTooLarge,
	},
	{
		name: "declared initial memory MaxMemoryPages + 1",
		wasm: func() []byte {
			b := newBuilder()
			b.memories = []limits{{flags: 0x00, min: 65}}
			return b.bytes()
		}(),
		want: ErrMemoryTooLarge,
	},
	{
		name: "two memories",
		wasm: func() []byte {
			b := newBuilder()
			b.memories = []limits{{flags: 0x00, min: 1}, {flags: 0x00, min: 1}}
			return b.bytes()
		}(),
		want: ErrTooManyMemories,
	},
	{
		name: "two tables",
		wasm: func() []byte {
			b := newBuilder()
			b.tables = []tableEntry{
				{elemType: 0x70, lim: limits{flags: 0x00, min: 1}},
				{elemType: 0x70, lim: limits{flags: 0x00, min: 1}},
			}
			return b.bytes()
		}(),
		want: ErrTooManyTables,
	},
	{
		name: "memory limits flags 0x03 (shared)",
		wasm: func() []byte {
			b := newBuilder()
			b.memories = []limits{{flags: 0x03, min: 1, max: 2}}
			return b.bytes()
		}(),
		want: ErrSharedMemory,
	},
	{
		name: "memory limits {flags 0x01, min 2, max 1}",
		wasm: func() []byte {
			b := newBuilder()
			b.memories = []limits{{flags: 0x01, min: 2, max: 1}}
			return b.bytes()
		}(),
		want: ErrInvalidLimits,
	},
	{
		name: "table limits {flags 0x01, min 2, max 1}",
		wasm: func() []byte {
			b := newBuilder()
			b.tables = []tableEntry{{elemType: 0x70, lim: limits{flags: 0x01, min: 2, max: 1}}}
			return b.bytes()
		}(),
		want: ErrInvalidLimits,
	},
	{
		name: "call_indirect tableidx 1 with one declared table",
		wasm: func() []byte {
			b := newBuilder()
			b.tables = []tableEntry{{elemType: 0x70, lim: limits{flags: 0x00, min: 1}}}
			b.bodies = []body{{code: []byte{0x11, 0x00, 0x01}}}
			return b.bytes()
		}(),
		want: ErrTableIndexOutOfBounds,
	},
	{
		name: "active data segment near the top of one page",
		wasm: func() []byte {
			b := newBuilder()
			b.data = []dataSeg{{flags: 0x00, offset: constI32(65536 - 2), bytes: []byte{1, 2, 3, 4}}}
			return b.bytes()
		}(),
		want: ErrDataSegmentOutOfBounds,
	},
	{
		name: "active data segment at i32.const -1",
		wasm: func() []byte {
			b := newBuilder()
			b.data = []dataSeg{{flags: 0x00, offset: constI32(-1), bytes: []byte{1}}}
			return b.bytes()
		}(),
		want: ErrDataSegmentOutOfBounds,
	},
	{
		name: "active element segment at offset tableMin",
		wasm: func() []byte {
			b := newBuilder()
			b.tables = []tableEntry{{elemType: 0x70, lim: limits{flags: 0x00, min: 5}}}
			b.elements = []elemSeg{{flags: 0x00, offset: constI32(5), funcs: []uint32{0}}}
			return b.bytes()
		}(),
		want: ErrElementSegmentOutOfBounds,
	},
	{
		name: "data segment offset global.get 0",
		wasm: func() []byte {
			b := newBuilder()
			b.data = []dataSeg{{flags: 0x00, offset: []byte{0x23, 0x00, 0x0B}, bytes: nil}}
			return b.bytes()
		}(),
		want: ErrNonConstantSegmentOffset,
	},
	{
		name: "passive data segment",
		wasm: func() []byte {
			b := newBuilder()
			b.data = []dataSeg{{flags: 0x01}}
			return b.bytes()
		}(),
		want: ErrBulkMemory,
	},
	{
		name: "type section MaxTypeCount + 1",
		wasm: func() []byte {
			b := newBuilder() // base type: 2 params
			for i := 0; i < int(testLimits.MaxTypeCount); i++ {
				b.types = append(b.types, funcType{form: 0x60})
			}
			return b.bytes()
		}(),
		want: ErrTooManyTypes,
	},
	{
		name: "global section MaxGlobals + 1",
		wasm: func() []byte {
			b := newBuilder()
			for i := 0; i < int(testLimits.MaxGlobals)+1; i++ {
				b.globals = append(b.globals, globalEntry{valType: 0x7F, mut: 0x00, init: constI32(0)})
			}
			return b.bytes()
		}(),
		want: ErrTooManyGlobals,
	},
	{
		name: "data section MaxDataSegments + 1",
		wasm: func() []byte {
			b := newBuilder()
			for i := 0; i < int(testLimits.MaxDataSegments)+1; i++ {
				b.data = append(b.data, dataSeg{flags: 0x00, offset: constI32(0), bytes: []byte{1}})
			}
			return b.bytes()
		}(),
		want: ErrTooManyDataSegments,
	},
	{
		name: "function section MaxFunctions + 1",
		wasm: func() []byte {
			b := newBuilder()
			b.types = append(b.types, funcType{form: 0x60})
			voidIdx := uint32(len(b.types) - 1)
			for i := 0; i < int(testLimits.MaxFunctions); i++ {
				b.funcs = append(b.funcs, voidIdx)
				b.bodies = append(b.bodies, body{code: []byte{0x0B}})
			}
			return b.bytes()
		}(),
		want: ErrTooManyFunctions,
	},
	{
		// The aggregate is what is new here: a single over-sized segment is already rejected by
		// the table-bounds check, so two segments summing over the cap, each individually within
		// the declared table minimum, is what proves the accumulator.
		name: "element section total entries MaxTotalElementEntries + 1",
		wasm: func() []byte {
			b := newBuilder()
			b.tables = []tableEntry{{elemType: 0x70, lim: limits{flags: 0x00, min: testLimits.MaxTableEntries}}}
			seg := func(n int) elemSeg {
				return elemSeg{flags: 0x00, offset: constI32(0), funcs: make([]uint32, n)}
			}
			b.elements = []elemSeg{
				seg(int(testLimits.MaxTableEntries)),
				seg(int(testLimits.MaxTotalElementEntries) - int(testLimits.MaxTableEntries) + 1),
			}
			return b.bytes()
		}(),
		want: ErrTotalElementEntriesExceeded,
	},
}

func TestRejectionCases(t *testing.T) {
	for _, c := range rejectionCases {
		t.Run(c.name, func(t *testing.T) {
			err := Validate(c.wasm, testLimits)
			if !errors.Is(err, c.want) {
				t.Errorf("Validate(%s) = %v, want %v", c.name, err, c.want)
			}
		})
	}
}

func TestAcceptanceCases(t *testing.T) {
	cases := []struct {
		name string
		wasm []byte
	}{
		{
			name: "minimal module",
			wasm: newBuilder().bytes(),
		},
		{
			// No custom sections: every custom section is rejected outright (see the
			// ErrCustomSection rejectionCases), so this exercises everything else that used to
			// share this fixture with them.
			name: "all three host imports, table, element segment, data segment, rich body",
			wasm: func() []byte {
				b := builderWithHostImports([]string{"storage_len", "storage_read", "abort"})
				b.tables = []tableEntry{{elemType: 0x70, lim: limits{flags: 0x00, min: 2}}}
				runIdx := uint32(len(b.imports))
				b.elements = []elemSeg{{flags: 0x00, offset: constI32(0), funcs: []uint32{runIdx}}}
				b.globals = []globalEntry{{valType: 0x7F, mut: 0x00, init: constI32(42)}}
				b.data = []dataSeg{{flags: 0x00, offset: constI32(0), bytes: []byte{1, 2, 3, 4}}}
				b.bodies = []body{{code: bigBody()}}
				return b.bytes()
			}(),
		},
		{
			name: "unused type at exactly the per-type params cap",
			wasm: func() []byte {
				b := newBuilder() // base type: 2 params
				params := make([]byte, testLimits.MaxLocalsPerFunction)
				for i := range params {
					params[i] = 0x7F
				}
				b.types = append(b.types, funcType{form: 0x60, params: params}) // 2 + 8 = 10
				return b.bytes()
			}(),
		},
		{
			name: "type section total params exactly at MaxTotalParams",
			wasm: func() []byte {
				b := newBuilder() // base type: 2 params
				eightParams := func() []byte {
					p := make([]byte, testLimits.MaxLocalsPerFunction)
					for i := range p {
						p[i] = 0x7F
					}
					return p
				}
				b.types = append(b.types,
					funcType{form: 0x60, params: eightParams()},
					funcType{form: 0x60, params: eightParams()},
					funcType{form: 0x60, params: []byte{0x7F, 0x7F}},
				) // 2 + 8 + 8 + 2 = 20
				return b.bytes()
			}(),
		},
		{
			name: "immutable global exported",
			wasm: func() []byte {
				b := newBuilder()
				b.globals = []globalEntry{{valType: 0x7F, mut: 0x00, init: constI32(7)}}
				b.exports = append(b.exports, exportEntry{name: "g", kind: 0x03, index: 0})
				return b.bytes()
			}(),
		},
		{
			name: "memory {min 1, max 100}",
			wasm: func() []byte {
				b := newBuilder()
				b.memories = []limits{{flags: 0x01, min: 1, max: 100}}
				return b.bytes()
			}(),
		},
		{
			name: "total locals across functions well under MaxTotalLocals",
			wasm: func() []byte {
				b := newBuilder()
				voidType := funcType{form: 0x60}
				b.types = append(b.types, voidType)
				runTypeIdx := uint32(1)
				b.funcs = append(b.funcs, runTypeIdx)
				b.bodies = append(b.bodies, body{locals: []localDecl{{count: 6, valType: 0x7F}}, code: []byte{0x0B}})
				return b.bytes()
			}(),
		},
		{
			name: "total locals across functions exactly at MaxTotalLocals",
			wasm: func() []byte {
				b := newBuilder() // run: 2 params
				b.types = append(b.types, funcType{form: 0x60})
				b.funcs = append(b.funcs, uint32(1))
				b.bodies = append(b.bodies, body{locals: []localDecl{{count: 8, valType: 0x7F}}, code: []byte{0x0B}}) // 2 + 8 = 10
				return b.bytes()
			}(),
		},
		{
			name: "type section exactly at MaxTypeCount",
			wasm: func() []byte {
				b := newBuilder() // base type: 2 params
				for i := 0; i < int(testLimits.MaxTypeCount)-1; i++ {
					b.types = append(b.types, funcType{form: 0x60})
				}
				return b.bytes()
			}(),
		},
		{
			name: "global section exactly at MaxGlobals",
			wasm: func() []byte {
				b := newBuilder()
				for i := 0; i < int(testLimits.MaxGlobals); i++ {
					b.globals = append(b.globals, globalEntry{valType: 0x7F, mut: 0x00, init: constI32(0)})
				}
				return b.bytes()
			}(),
		},
		{
			name: "data section exactly at MaxDataSegments",
			wasm: func() []byte {
				b := newBuilder()
				for i := 0; i < int(testLimits.MaxDataSegments); i++ {
					b.data = append(b.data, dataSeg{flags: 0x00, offset: constI32(0), bytes: []byte{1}})
				}
				return b.bytes()
			}(),
		},
		{
			name: "function section exactly at MaxFunctions",
			wasm: func() []byte {
				b := newBuilder()
				b.types = append(b.types, funcType{form: 0x60})
				voidIdx := uint32(len(b.types) - 1)
				for i := 0; i < int(testLimits.MaxFunctions)-1; i++ {
					b.funcs = append(b.funcs, voidIdx)
					b.bodies = append(b.bodies, body{code: []byte{0x0B}})
				}
				return b.bytes()
			}(),
		},
		{
			name: "element section total entries exactly at MaxTotalElementEntries",
			wasm: func() []byte {
				b := newBuilder()
				b.tables = []tableEntry{{elemType: 0x70, lim: limits{flags: 0x00, min: testLimits.MaxTableEntries}}}
				seg := func(n int) elemSeg {
					return elemSeg{flags: 0x00, offset: constI32(0), funcs: make([]uint32, n)}
				}
				b.elements = []elemSeg{
					seg(int(testLimits.MaxTableEntries)),
					seg(int(testLimits.MaxTotalElementEntries) - int(testLimits.MaxTableEntries)),
				}
				return b.bytes()
			}(),
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if err := Validate(c.wasm, testLimits); err != nil {
				t.Errorf("Validate(%s) = %v, want nil", c.name, err)
			}
		})
	}
}
