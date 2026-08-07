package module

import "errors"

// Every named rejection reason Validate can return, one per top-level line (never a grouped
// var () block: guards_test.go's error-coverage guard scans this file for lines matching
// `^var (Err[A-Za-z0-9]+)\b`, so the shape here is load-bearing). Each is wrapped with
// fmt.Errorf("<what and where>: %w", ErrX) at its call site, so callers compare with
// errors.Is and the message still names the site.

// ErrModuleTooLarge is returned when the module exceeds Limits.MaxCodeSize.
var ErrModuleTooLarge = errors.New("module: module too large")

// ErrBadMagic is returned when the module does not begin with the \0asm magic.
var ErrBadMagic = errors.New("module: bad magic")

// ErrBadVersion is returned when the version field is not 01 00 00 00.
var ErrBadVersion = errors.New("module: bad version")

// ErrDuplicateSection is returned when two sections share the same non-custom id.
var ErrDuplicateSection = errors.New("module: duplicate section")

// ErrSectionOrder is returned when a non-custom section's id is less than the previous
// non-custom section's id.
var ErrSectionOrder = errors.New("module: section out of order")

// ErrUnknownSection is returned for a non-custom section id greater than 11.
var ErrUnknownSection = errors.New("module: unknown section")

// ErrCustomSection is returned for any custom section (id 0). wazero's decoder parses a
// section literally named "name" unconditionally at decode time, reading declared counts
// (function/local name-map sizes, the module-name byte length) with no bound against the
// section's own length or the bytes remaining, regardless of any engine config knob. Accepting
// a custom section and relying on later stripping does not close that hole, since the
// unstripped bytes still reach the engine's own compile step; the whole class is rejected, not
// just sections named "name".
var ErrCustomSection = errors.New("module: custom section")

// ErrTruncated is returned when a declared length or count runs off the end of the input.
var ErrTruncated = errors.New("module: truncated")

// ErrSectionSizeMismatch is returned when a section's or vector's declared size is not
// consumed exactly by its own contents, or when a declared count cannot fit in the bytes
// that remain.
var ErrSectionSizeMismatch = errors.New("module: section size mismatch")

// ErrTrailingBytes is returned when bytes remain after the last section.
var ErrTrailingBytes = errors.New("module: trailing bytes")

// ErrStartSection is returned when a start section (id 8) is present.
var ErrStartSection = errors.New("module: start section present")

// ErrMalformedSection is returned for a structurally invalid byte this package has no other
// specific reason for (an unknown form, descriptor, export kind or segment flags byte).
var ErrMalformedSection = errors.New("module: malformed section")

// ErrMultiValueResult is returned when a function type declares more than one result.
var ErrMultiValueResult = errors.New("module: multi-value result")

// ErrFloatType is returned for a float valtype (f32/f64) wherever a valtype is expected.
var ErrFloatType = errors.New("module: float type")

// ErrSIMD is returned for a v128 valtype, instruction or sub-opcode.
var ErrSIMD = errors.New("module: SIMD")

// ErrReferenceType is returned for a reference type (funcref/externref) or instruction where
// Phase 1 does not accept one.
var ErrReferenceType = errors.New("module: reference type")

// ErrInvalidValType is returned for a byte that is none of the recognized valtype encodings.
var ErrInvalidValType = errors.New("module: invalid valtype")

// ErrDisallowedImport is returned for any import outside the exact three "env" host
// functions: a different module name, an unrecognized name, a repeated name, more than three
// imports, or a non-function import kind.
var ErrDisallowedImport = errors.New("module: disallowed import")

// ErrImportSignature is returned when a whitelisted import's referenced type does not match
// its required signature.
var ErrImportSignature = errors.New("module: import signature mismatch")

// ErrTypeIndexOutOfBounds is returned when a type index names an entry outside the type
// section.
var ErrTypeIndexOutOfBounds = errors.New("module: type index out of bounds")

// ErrTooManyTables is returned when more than one table is declared.
var ErrTooManyTables = errors.New("module: too many tables")

// ErrInvalidLimits is returned for a limits flags byte outside 0x00/0x01, or a declared
// maximum below the declared minimum.
var ErrInvalidLimits = errors.New("module: invalid limits")

// ErrTableTooLarge is returned when a table's declared minimum exceeds Limits.MaxTableEntries.
var ErrTableTooLarge = errors.New("module: table too large")

// ErrTooManyMemories is returned when more than one memory is declared.
var ErrTooManyMemories = errors.New("module: too many memories")

// ErrSharedMemory is returned when a memory's limits flags declare it shared.
var ErrSharedMemory = errors.New("module: shared memory")

// ErrMemoryTooLarge is returned when a memory's declared minimum exceeds
// Limits.MaxMemoryPages.
var ErrMemoryTooLarge = errors.New("module: memory too large")

// ErrInvalidMutability is returned for a global mutability byte outside 0x00/0x01.
var ErrInvalidMutability = errors.New("module: invalid mutability")

// ErrNonConstantInitializer is returned when a global's initializer is not exactly a literal
// const of the declared valtype followed by end.
var ErrNonConstantInitializer = errors.New("module: non-constant initializer")

// ErrMissingExport is returned when the required memory or run export is absent.
var ErrMissingExport = errors.New("module: missing required export")

// ErrFunctionIndexOutOfBounds is returned when a function index names an entry outside the
// function index space.
var ErrFunctionIndexOutOfBounds = errors.New("module: function index out of bounds")

// ErrRunImported is returned when the run export names an imported function rather than a
// locally defined one.
var ErrRunImported = errors.New("module: run export is imported")

// ErrRunSignature is returned when the run export's function type is not exactly
// (i32,i32) -> i64.
var ErrRunSignature = errors.New("module: run export signature mismatch")

// ErrReservedExport is returned for an export named _start or beginning __zenon_.
var ErrReservedExport = errors.New("module: reserved export name")

// ErrMutableGlobalExport is returned when an exported global is mutable.
var ErrMutableGlobalExport = errors.New("module: mutable global export")

// ErrBulkMemory is returned for any bulk-memory or reference-types construct: a
// passive/declarative element or data segment, or the memory.copy/memory.fill instructions.
var ErrBulkMemory = errors.New("module: bulk memory")

// ErrNonConstantSegmentOffset is returned when an element or data segment's offset is not
// exactly a literal i32.const followed by end.
var ErrNonConstantSegmentOffset = errors.New("module: non-constant segment offset")

// ErrTableIndexOutOfBounds is returned when a table index names an entry outside the table
// index space (including an element segment with no table declared).
var ErrTableIndexOutOfBounds = errors.New("module: table index out of bounds")

// ErrMemoryIndexOutOfBounds is returned when a memory index or reference names an entry
// outside the memory index space (including no memory declared).
var ErrMemoryIndexOutOfBounds = errors.New("module: memory index out of bounds")

// ErrDataSegmentOutOfBounds is returned when an active data segment's offset and length
// would run past the declared memory minimum.
var ErrDataSegmentOutOfBounds = errors.New("module: data segment out of bounds")

// ErrElementSegmentOutOfBounds is returned when an active element segment's offset and
// length would run past the declared table minimum.
var ErrElementSegmentOutOfBounds = errors.New("module: element segment out of bounds")

// ErrFunctionCodeMismatch is returned when the code section's entry count does not equal the
// function section's.
var ErrFunctionCodeMismatch = errors.New("module: function/code count mismatch")

// ErrTooManyLocals is returned when a function's params plus declared locals exceeds
// Limits.MaxLocalsPerFunction.
var ErrTooManyLocals = errors.New("module: too many locals")

// ErrTotalLocalsExceeded is returned when the sum of params plus declared locals across every
// function in the module exceeds Limits.MaxTotalLocals.
var ErrTotalLocalsExceeded = errors.New("module: total locals exceeded")

// ErrTooManyParams is returned when a function type's parameter count exceeds
// Limits.MaxLocalsPerFunction. wazero's FunctionType.key() builds a decode-time cache key with
// O(n^2) string concatenation over a type's params for every declared type, whether or not any
// function references it, so an unused huge-param type must be capped independently of the
// per-function locals check.
var ErrTooManyParams = errors.New("module: too many params")

// ErrTotalParamsExceeded is returned when the sum of every declared function type's parameter
// count exceeds Limits.MaxTotalParams. A per-type cap alone still lets a type section packed
// with many capped types cost hundreds of milliseconds and gigabytes of allocation churn to
// compile.
var ErrTotalParamsExceeded = errors.New("module: total params exceeded")

// ErrMalformedFunctionBody is returned when a function body's instruction stream does not
// end with the end that pops its implicit block exactly at the declared body length.
var ErrMalformedFunctionBody = errors.New("module: malformed function body")

// ErrExceptionHandling is returned for an exception-handling instruction (0x06-0x08).
var ErrExceptionHandling = errors.New("module: exception handling")

// ErrTypedSelect is returned for the typed select instruction (0x1C).
var ErrTypedSelect = errors.New("module: typed select")

// ErrFloatInstruction is returned for any f32.*/f64.* instruction.
var ErrFloatInstruction = errors.New("module: float instruction")

// ErrSignExtension is returned for the sign-extension instructions (0xC0-0xC4).
var ErrSignExtension = errors.New("module: sign extension")

// ErrSaturatingConversion is returned for the saturating float-to-int conversions
// (0xFC sub-opcodes 0-7).
var ErrSaturatingConversion = errors.New("module: saturating conversion")

// ErrAtomics is returned for a threads/atomics instruction (0xFE).
var ErrAtomics = errors.New("module: atomics")

// ErrUnmeteredOpcode is returned for any opcode absent from the embedded gas table: the
// deny-by-default instruction set.
var ErrUnmeteredOpcode = errors.New("module: unmetered opcode")

// ErrGlobalIndexOutOfBounds is returned when a global index names an entry outside the
// global index space.
var ErrGlobalIndexOutOfBounds = errors.New("module: global index out of bounds")

// ErrLabelOutOfBounds is returned when a branch label names a depth outside the current
// control stack.
var ErrLabelOutOfBounds = errors.New("module: label out of bounds")

// ErrLocalIndexOutOfBounds is returned when a local index names an entry outside a
// function's params plus declared locals.
var ErrLocalIndexOutOfBounds = errors.New("module: local index out of bounds")

// ErrBlockTypeIndex is returned for a block type immediate encoding a multi-value type
// index, which Phase 1 does not accept.
var ErrBlockTypeIndex = errors.New("module: block type index")

// ErrNonCanonicalLEB128 is returned for a LEB128 encoding longer than the shortest one that
// represents its value.
var ErrNonCanonicalLEB128 = errors.New("module: non-canonical LEB128")

// ErrIntegerTooLarge is returned for a LEB128 encoding using more bytes than its width
// allows, or with unused high bits set in its final byte.
var ErrIntegerTooLarge = errors.New("module: integer too large")

// ErrTooManyTypes is returned when the type section declares more entries than
// Limits.MaxTypeCount. The engine interns every declared type into its Store for the
// runtime's lifetime and never evicts one, so the declared type count bounds a module's
// permanent contribution to node heap independently of MaxTotalParams, which bounds only the
// params contribution.
var ErrTooManyTypes = errors.New("module: too many types")

// ErrTooManyFunctions is returned when the function section declares more locally defined
// functions than Limits.MaxFunctions. Nothing but MaxCodeSize bounds this count today, and
// each one costs a []function entry built at every instantiation.
var ErrTooManyFunctions = errors.New("module: too many functions")

// ErrTooManyGlobals is returned when the global section declares more entries than
// Limits.MaxGlobals. Nothing but MaxCodeSize bounds this count today, and each one costs a
// heap-allocated GlobalInstance built at every instantiation.
var ErrTooManyGlobals = errors.New("module: too many globals")

// ErrTooManyDataSegments is returned when the data section declares more entries than
// Limits.MaxDataSegments. Nothing but MaxCodeSize bounds this count today, and each active
// segment costs a copy applied at every instantiation.
var ErrTooManyDataSegments = errors.New("module: too many data segments")

// ErrTotalElementEntriesExceeded is returned when the sum of every element segment's
// function-index count exceeds Limits.MaxTotalElementEntries. MaxTableEntries caps one
// table's declared minimum, not how many segments write into it, so it does not bound the
// aggregate entry count a module can force applied at every instantiation.
var ErrTotalElementEntriesExceeded = errors.New("module: total element entries exceeded")
