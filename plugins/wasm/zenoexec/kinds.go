// Package zenoexec is the core of cmd/zeno-exec: the error kinds, the
// command session behind "run", the command decoding, and the local
// HTTP server behind "serve".
package zenoexec

import (
	"errors"

	"github.com/digitalSloth/go-executor/internal/codec"
	"github.com/digitalSloth/go-executor/plugins/wasm"
	"github.com/digitalSloth/go-executor/plugins/wasm/internal/module"
)

// kindEntry names one module sentinel and the kind Kind reports for it.
type kindEntry struct {
	name     string
	sentinel error
	kind     string
}

// moduleKinds maps every sentinel the module package declares to its
// kind: the name without the Err prefix, in snake_case, with acronyms
// kept whole.
var moduleKinds = []kindEntry{
	{"ErrModuleTooLarge", module.ErrModuleTooLarge, "module_too_large"},
	{"ErrBadMagic", module.ErrBadMagic, "bad_magic"},
	{"ErrBadVersion", module.ErrBadVersion, "bad_version"},
	{"ErrDuplicateSection", module.ErrDuplicateSection, "duplicate_section"},
	{"ErrSectionOrder", module.ErrSectionOrder, "section_order"},
	{"ErrUnknownSection", module.ErrUnknownSection, "unknown_section"},
	{"ErrCustomSection", module.ErrCustomSection, "custom_section"},
	{"ErrTruncated", module.ErrTruncated, "truncated"},
	{"ErrSectionSizeMismatch", module.ErrSectionSizeMismatch, "section_size_mismatch"},
	{"ErrTrailingBytes", module.ErrTrailingBytes, "trailing_bytes"},
	{"ErrStartSection", module.ErrStartSection, "start_section"},
	{"ErrMalformedSection", module.ErrMalformedSection, "malformed_section"},
	{"ErrMultiValueResult", module.ErrMultiValueResult, "multi_value_result"},
	{"ErrFloatType", module.ErrFloatType, "float_type"},
	{"ErrSIMD", module.ErrSIMD, "simd"},
	{"ErrReferenceType", module.ErrReferenceType, "reference_type"},
	{"ErrInvalidValType", module.ErrInvalidValType, "invalid_val_type"},
	{"ErrDisallowedImport", module.ErrDisallowedImport, "disallowed_import"},
	{"ErrImportSignature", module.ErrImportSignature, "import_signature"},
	{"ErrTypeIndexOutOfBounds", module.ErrTypeIndexOutOfBounds, "type_index_out_of_bounds"},
	{"ErrTooManyTables", module.ErrTooManyTables, "too_many_tables"},
	{"ErrInvalidLimits", module.ErrInvalidLimits, "invalid_limits"},
	{"ErrTableTooLarge", module.ErrTableTooLarge, "table_too_large"},
	{"ErrTooManyMemories", module.ErrTooManyMemories, "too_many_memories"},
	{"ErrSharedMemory", module.ErrSharedMemory, "shared_memory"},
	{"ErrMemoryTooLarge", module.ErrMemoryTooLarge, "memory_too_large"},
	{"ErrInvalidMutability", module.ErrInvalidMutability, "invalid_mutability"},
	{"ErrNonConstantInitializer", module.ErrNonConstantInitializer, "non_constant_initializer"},
	{"ErrMissingExport", module.ErrMissingExport, "missing_export"},
	{"ErrFunctionIndexOutOfBounds", module.ErrFunctionIndexOutOfBounds, "function_index_out_of_bounds"},
	{"ErrRunImported", module.ErrRunImported, "run_imported"},
	{"ErrRunSignature", module.ErrRunSignature, "run_signature"},
	{"ErrReservedExport", module.ErrReservedExport, "reserved_export"},
	{"ErrMutableGlobalExport", module.ErrMutableGlobalExport, "mutable_global_export"},
	{"ErrBulkMemory", module.ErrBulkMemory, "bulk_memory"},
	{"ErrNonConstantSegmentOffset", module.ErrNonConstantSegmentOffset, "non_constant_segment_offset"},
	{"ErrTableIndexOutOfBounds", module.ErrTableIndexOutOfBounds, "table_index_out_of_bounds"},
	{"ErrMemoryIndexOutOfBounds", module.ErrMemoryIndexOutOfBounds, "memory_index_out_of_bounds"},
	{"ErrDataSegmentOutOfBounds", module.ErrDataSegmentOutOfBounds, "data_segment_out_of_bounds"},
	{"ErrElementSegmentOutOfBounds", module.ErrElementSegmentOutOfBounds, "element_segment_out_of_bounds"},
	{"ErrFunctionCodeMismatch", module.ErrFunctionCodeMismatch, "function_code_mismatch"},
	{"ErrTooManyLocals", module.ErrTooManyLocals, "too_many_locals"},
	{"ErrTotalLocalsExceeded", module.ErrTotalLocalsExceeded, "total_locals_exceeded"},
	{"ErrTooManyParams", module.ErrTooManyParams, "too_many_params"},
	{"ErrTotalParamsExceeded", module.ErrTotalParamsExceeded, "total_params_exceeded"},
	{"ErrMalformedFunctionBody", module.ErrMalformedFunctionBody, "malformed_function_body"},
	{"ErrExceptionHandling", module.ErrExceptionHandling, "exception_handling"},
	{"ErrTypedSelect", module.ErrTypedSelect, "typed_select"},
	{"ErrFloatInstruction", module.ErrFloatInstruction, "float_instruction"},
	{"ErrSignExtension", module.ErrSignExtension, "sign_extension"},
	{"ErrSaturatingConversion", module.ErrSaturatingConversion, "saturating_conversion"},
	{"ErrAtomics", module.ErrAtomics, "atomics"},
	{"ErrUnmeteredOpcode", module.ErrUnmeteredOpcode, "unmetered_opcode"},
	{"ErrGlobalIndexOutOfBounds", module.ErrGlobalIndexOutOfBounds, "global_index_out_of_bounds"},
	{"ErrLabelOutOfBounds", module.ErrLabelOutOfBounds, "label_out_of_bounds"},
	{"ErrLocalIndexOutOfBounds", module.ErrLocalIndexOutOfBounds, "local_index_out_of_bounds"},
	{"ErrBlockTypeIndex", module.ErrBlockTypeIndex, "block_type_index"},
	{"ErrNonCanonicalLEB128", module.ErrNonCanonicalLEB128, "non_canonical_leb128"},
	{"ErrIntegerTooLarge", module.ErrIntegerTooLarge, "integer_too_large"},
	{"ErrTooManyTypes", module.ErrTooManyTypes, "too_many_types"},
	{"ErrTooManyFunctions", module.ErrTooManyFunctions, "too_many_functions"},
	{"ErrTooManyGlobals", module.ErrTooManyGlobals, "too_many_globals"},
	{"ErrTooManyDataSegments", module.ErrTooManyDataSegments, "too_many_data_segments"},
	{"ErrTotalElementEntriesExceeded", module.ErrTotalElementEntriesExceeded, "total_element_entries_exceeded"},
	{"ErrInstrumentedTooLarge", module.ErrInstrumentedTooLarge, "instrumented_too_large"},
	{"ErrNotValidated", module.ErrNotValidated, "not_validated"},
}

// Kind returns the stable kind of a module-check error, or "unknown"
// when nothing matches.
func Kind(err error) string {
	switch {
	case errors.Is(err, wasm.ErrEngineCompile):
		return "engine_compile"
	case errors.Is(err, wasm.ErrDerivePanic):
		return "internal_panic"
	case errors.Is(err, codec.ErrUnknownVersion),
		errors.Is(err, codec.ErrInvalidPolicy),
		errors.Is(err, codec.ErrInvalidOwner):
		return "invalid_metadata"
	}
	for _, e := range moduleKinds {
		if errors.Is(err, e.sentinel) {
			return e.kind
		}
	}
	return "unknown"
}
