package module

import (
	"bytes"
	"fmt"
	"strings"
)

// Limits are the plugin's structural caps. They are passed in rather than redeclared here so
// plugins/wasm/limits.go stays their single definition: internal/module cannot import
// plugins/wasm, since plugins/wasm imports internal/module once the instrumentation pass
// lands.
type Limits struct {
	MaxCodeSize            int    // bytes, pre-instrumentation
	MaxMemoryPages         uint32 // declared initial memory ceiling
	MaxLocalsPerFunction   uint32 // params + declared locals, per function
	MaxTotalLocals         uint64 // params + declared locals, summed across every function
	MaxTableEntries        uint32 // declared table minimum ceiling
	MaxTotalParams         uint64 // a declared function type's params, summed across the type section
	MaxGlobals             uint32 // declared globals
	MaxDataSegments        uint32 // data section segment count
	MaxFunctions           uint32 // locally defined functions
	MaxTotalElementEntries uint64 // function-index entries summed across the element section
	MaxTypeCount           uint32 // declared function types
}

// funcSig is one type section entry's parameter and result valtypes.
type funcSig struct {
	params  []byte
	results []byte
}

func sigEqual(a, b funcSig) bool {
	return bytes.Equal(a.params, b.params) && bytes.Equal(a.results, b.results)
}

// tableInfo and memInfo are the one declared table's and one declared memory's limits
// (rules 6 and 7 cap each index space at a single entry).
type tableInfo struct {
	min, max uint32
	hasMax   bool
}

type memInfo struct {
	min, max uint32
	hasMax   bool
}

type globalInfo struct {
	valType byte
	mutable bool
}

// moduleInfo accumulates the cross-referenced state later rules need, built up as sections
// are parsed in ascending id order.
type moduleInfo struct {
	types    []funcSig
	imports  uint32 // count of (function) imports; the function index space's imported prefix
	funcs    []uint32
	table    tableInfo
	hasTable bool
	memory   memInfo
	hasMem   bool
	globals  []globalInfo

	sawMemoryExport bool
	sawRunExport    bool
}

// Validate reports whether wasm is in the Phase-1 accepted set, returning a wrapped named
// reason for the first rule it breaks. It performs no type checking: operand-stack typing,
// block signatures and call_indirect type agreement are the engine's, via engine.Compile.
//
// Two rules here are stricter than Core 1.0, deliberately: canonical LEB128 only (any padded
// encoding is rejected, for every count, length, index and immediate), and a deny-by-default
// instruction set (anything absent from the embedded gas table is rejected).
func Validate(wasm []byte, lim Limits) error {
	if len(wasm) > lim.MaxCodeSize {
		return fmt.Errorf("module: %d bytes exceeds MaxCodeSize %d: %w", len(wasm), lim.MaxCodeSize, ErrModuleTooLarge)
	}
	rest, err := parsePreamble(wasm)
	if err != nil {
		return err
	}
	secs, err := splitSections(rest)
	if err != nil {
		return err
	}

	var m moduleInfo
	exportsChecked := false
	sawCode := false
	for _, s := range secs {
		if !exportsChecked && s.id > 7 {
			if err := requireCoreExports(&m); err != nil {
				return err
			}
			exportsChecked = true
		}

		var perr error
		switch s.id {
		case 1:
			perr = parseTypes(s.payload, &m, lim)
		case 2:
			perr = parseImports(s.payload, &m)
		case 3:
			perr = parseFunctions(s.payload, &m, lim)
		case 4:
			perr = parseTables(s.payload, &m, lim)
		case 5:
			perr = parseMemories(s.payload, &m, lim)
		case 6:
			perr = parseGlobals(s.payload, &m, lim)
		case 7:
			perr = parseExports(s.payload, &m)
		case 8:
			perr = fmt.Errorf("start section present: %w", ErrStartSection)
		case 9:
			perr = parseElements(s.payload, &m, lim)
		case 10:
			perr = parseCode(s.payload, &m, lim)
			sawCode = true
		case 11:
			perr = parseData(s.payload, &m, lim)
		}
		if perr != nil {
			return perr
		}
	}
	if !exportsChecked {
		if err := requireCoreExports(&m); err != nil {
			return err
		}
	}
	if !sawCode && len(m.funcs) > 0 {
		return fmt.Errorf("code section absent, function section has %d: %w", len(m.funcs), ErrFunctionCodeMismatch)
	}
	return nil
}

// requireCoreExports enforces rule 9's two required exports, called once at the point in
// section order rule 9 occupies, whether or not an export section is present.
func requireCoreExports(m *moduleInfo) error {
	if !m.sawMemoryExport {
		return fmt.Errorf("export section: no memory export named \"memory\": %w", ErrMissingExport)
	}
	if !m.sawRunExport {
		return fmt.Errorf("export section: no function export named \"run\": %w", ErrMissingExport)
	}
	return nil
}

// parseTypes implements rule 3. It also caps a declared type's parameter count against
// lim.MaxLocalsPerFunction and accumulates the running total across the whole type section
// against lim.MaxTotalParams, in the same early-exit shape as parseCode's MaxTotalLocals
// accumulator: wazero's decode-time cache key for a function type is built with O(n^2) string
// concatenation over its params, for every declared type whether or not any function
// references it, so a type's params must be bounded here rather than relying on the
// per-function locals check a function using it would otherwise hit.
func parseTypes(payload []byte, m *moduleInfo, lim Limits) error {
	r := newReader(payload)
	count, err := r.readVecCount()
	if err != nil {
		return fmt.Errorf("type section: count: %w", err)
	}
	if count > lim.MaxTypeCount {
		return fmt.Errorf("type section: %d types exceeds MaxTypeCount %d: %w", count, lim.MaxTypeCount, ErrTooManyTypes)
	}
	var totalParams uint64
	for i := uint32(0); i < count; i++ {
		form, err := r.readByte()
		if err != nil {
			return fmt.Errorf("type %d: form: %w", i, err)
		}
		if form != 0x60 {
			return fmt.Errorf("type %d: form 0x%02X: %w", i, form, ErrMalformedSection)
		}
		params, err := readValTypeVec(r)
		if err != nil {
			return fmt.Errorf("type %d: params: %w", i, err)
		}
		if uint64(len(params)) > uint64(lim.MaxLocalsPerFunction) {
			return fmt.Errorf("type %d: %d params exceeds MaxLocalsPerFunction %d: %w", i, len(params), lim.MaxLocalsPerFunction, ErrTooManyParams)
		}
		totalParams += uint64(len(params))
		if totalParams > lim.MaxTotalParams {
			return fmt.Errorf("type %d: total params %d exceeds MaxTotalParams %d: %w", i, totalParams, lim.MaxTotalParams, ErrTotalParamsExceeded)
		}
		results, err := readValTypeVec(r)
		if err != nil {
			return fmt.Errorf("type %d: results: %w", i, err)
		}
		if len(results) > 1 {
			return fmt.Errorf("type %d: %d results: %w", i, len(results), ErrMultiValueResult)
		}
		m.types = append(m.types, funcSig{params: params, results: results})
	}
	if r.remaining() != 0 {
		return fmt.Errorf("type section: %w", ErrSectionSizeMismatch)
	}
	return nil
}

var (
	storageLenSig  = funcSig{params: []byte{0x7F, 0x7F}, results: []byte{0x7E}}
	storageReadSig = funcSig{params: []byte{0x7F, 0x7F, 0x7F, 0x7F}, results: []byte{0x7E}}
	abortSig       = funcSig{params: []byte{0x7F}, results: nil}
	runSig         = funcSig{params: []byte{0x7F, 0x7F}, results: []byte{0x7E}}
)

// parseImports implements rule 4.
func parseImports(payload []byte, m *moduleInfo) error {
	r := newReader(payload)
	count, err := r.readVecCount()
	if err != nil {
		return fmt.Errorf("import section: count: %w", err)
	}
	seen := map[string]bool{}
	for i := uint32(0); i < count; i++ {
		modName, err := r.readName()
		if err != nil {
			return fmt.Errorf("import %d: module name: %w", i, err)
		}
		name, err := r.readName()
		if err != nil {
			return fmt.Errorf("import %d: name: %w", i, err)
		}
		desc, err := r.readByte()
		if err != nil {
			return fmt.Errorf("import %d: descriptor: %w", i, err)
		}
		switch desc {
		case 0x00:
			typeIdx, err := r.readVarU32()
			if err != nil {
				return fmt.Errorf("import %d: type index: %w", i, err)
			}
			if modName != "env" || seen[name] || m.imports >= 3 {
				return fmt.Errorf("import %d %s.%s: %w", i, modName, name, ErrDisallowedImport)
			}
			var want funcSig
			switch name {
			case "storage_len":
				want = storageLenSig
			case "storage_read":
				want = storageReadSig
			case "abort":
				want = abortSig
			default:
				return fmt.Errorf("import %d %s.%s: %w", i, modName, name, ErrDisallowedImport)
			}
			if uint64(typeIdx) >= uint64(len(m.types)) {
				return fmt.Errorf("import %d: type index %d: %w", i, typeIdx, ErrTypeIndexOutOfBounds)
			}
			if !sigEqual(m.types[typeIdx], want) {
				return fmt.Errorf("import %d %s.%s: %w", i, modName, name, ErrImportSignature)
			}
			seen[name] = true
			m.imports++
		case 0x01, 0x02, 0x03:
			return fmt.Errorf("import %d %s.%s: kind 0x%02X: %w", i, modName, name, desc, ErrDisallowedImport)
		default:
			return fmt.Errorf("import %d: descriptor 0x%02X: %w", i, desc, ErrMalformedSection)
		}
	}
	if r.remaining() != 0 {
		return fmt.Errorf("import section: %w", ErrSectionSizeMismatch)
	}
	return nil
}

// parseFunctions implements rule 5.
func parseFunctions(payload []byte, m *moduleInfo, lim Limits) error {
	r := newReader(payload)
	count, err := r.readVecCount()
	if err != nil {
		return fmt.Errorf("function section: count: %w", err)
	}
	if count > lim.MaxFunctions {
		return fmt.Errorf("function section: %d functions exceeds MaxFunctions %d: %w", count, lim.MaxFunctions, ErrTooManyFunctions)
	}
	for i := uint32(0); i < count; i++ {
		typeIdx, err := r.readVarU32()
		if err != nil {
			return fmt.Errorf("function %d: type index: %w", i, err)
		}
		if uint64(typeIdx) >= uint64(len(m.types)) {
			return fmt.Errorf("function %d: type index %d: %w", i, typeIdx, ErrTypeIndexOutOfBounds)
		}
		m.funcs = append(m.funcs, typeIdx)
	}
	if r.remaining() != 0 {
		return fmt.Errorf("function section: %w", ErrSectionSizeMismatch)
	}
	return nil
}

// parseTables implements rule 6.
func parseTables(payload []byte, m *moduleInfo, lim Limits) error {
	r := newReader(payload)
	count, err := r.readVecCount()
	if err != nil {
		return fmt.Errorf("table section: count: %w", err)
	}
	if count > 1 {
		return fmt.Errorf("table section: %d tables: %w", count, ErrTooManyTables)
	}
	for i := uint32(0); i < count; i++ {
		elemType, err := r.readByte()
		if err != nil {
			return fmt.Errorf("table %d: element type: %w", i, err)
		}
		if elemType != 0x70 {
			return fmt.Errorf("table %d: element type 0x%02X: %w", i, elemType, ErrReferenceType)
		}
		lr, err := readLimitsRaw(r)
		if err != nil {
			return fmt.Errorf("table %d: limits: %w", i, err)
		}
		if lr.flags != 0x00 && lr.flags != 0x01 {
			return fmt.Errorf("table %d: limits flags 0x%02X: %w", i, lr.flags, ErrInvalidLimits)
		}
		if lr.min > lim.MaxTableEntries {
			return fmt.Errorf("table %d: min %d exceeds MaxTableEntries %d: %w", i, lr.min, lim.MaxTableEntries, ErrTableTooLarge)
		}
		if lr.hasMax && lr.max < lr.min {
			return fmt.Errorf("table %d: max %d < min %d: %w", i, lr.max, lr.min, ErrInvalidLimits)
		}
		m.table = tableInfo{min: lr.min, max: lr.max, hasMax: lr.hasMax}
		m.hasTable = true
	}
	if r.remaining() != 0 {
		return fmt.Errorf("table section: %w", ErrSectionSizeMismatch)
	}
	return nil
}

// parseMemories implements rule 7.
func parseMemories(payload []byte, m *moduleInfo, lim Limits) error {
	r := newReader(payload)
	count, err := r.readVecCount()
	if err != nil {
		return fmt.Errorf("memory section: count: %w", err)
	}
	if count > 1 {
		return fmt.Errorf("memory section: %d memories: %w", count, ErrTooManyMemories)
	}
	for i := uint32(0); i < count; i++ {
		lr, err := readLimitsRaw(r)
		if err != nil {
			return fmt.Errorf("memory %d: limits: %w", i, err)
		}
		if lr.flags == 0x02 || lr.flags == 0x03 {
			return fmt.Errorf("memory %d: shared (flags 0x%02X): %w", i, lr.flags, ErrSharedMemory)
		}
		if lr.flags != 0x00 && lr.flags != 0x01 {
			return fmt.Errorf("memory %d: limits flags 0x%02X: %w", i, lr.flags, ErrInvalidLimits)
		}
		if lr.min > lim.MaxMemoryPages {
			return fmt.Errorf("memory %d: min %d exceeds MaxMemoryPages %d: %w", i, lr.min, lim.MaxMemoryPages, ErrMemoryTooLarge)
		}
		if lr.hasMax && lr.max < lr.min {
			return fmt.Errorf("memory %d: max %d < min %d: %w", i, lr.max, lr.min, ErrInvalidLimits)
		}
		m.memory = memInfo{min: lr.min, max: lr.max, hasMax: lr.hasMax}
		m.hasMem = true
	}
	if r.remaining() != 0 {
		return fmt.Errorf("memory section: %w", ErrSectionSizeMismatch)
	}
	return nil
}

// parseGlobals implements rule 8.
func parseGlobals(payload []byte, m *moduleInfo, lim Limits) error {
	r := newReader(payload)
	count, err := r.readVecCount()
	if err != nil {
		return fmt.Errorf("global section: count: %w", err)
	}
	if count > lim.MaxGlobals {
		return fmt.Errorf("global section: %d globals exceeds MaxGlobals %d: %w", count, lim.MaxGlobals, ErrTooManyGlobals)
	}
	for i := uint32(0); i < count; i++ {
		vt, err := r.readByte()
		if err != nil {
			return fmt.Errorf("global %d: valtype: %w", i, err)
		}
		if err := checkValType(vt); err != nil {
			return fmt.Errorf("global %d: valtype 0x%02X: %w", i, vt, err)
		}
		mut, err := r.readByte()
		if err != nil {
			return fmt.Errorf("global %d: mutability: %w", i, err)
		}
		if mut != 0x00 && mut != 0x01 {
			return fmt.Errorf("global %d: mutability 0x%02X: %w", i, mut, ErrInvalidMutability)
		}
		var ok bool
		if vt == 0x7F {
			_, ok, err = readConstI32(r)
		} else {
			_, ok, err = readConstI64(r)
		}
		if err != nil {
			return fmt.Errorf("global %d: initializer: %w", i, err)
		}
		if !ok {
			return fmt.Errorf("global %d: initializer: %w", i, ErrNonConstantInitializer)
		}
		m.globals = append(m.globals, globalInfo{valType: vt, mutable: mut == 0x01})
	}
	if r.remaining() != 0 {
		return fmt.Errorf("global section: %w", ErrSectionSizeMismatch)
	}
	return nil
}

// parseExports implements rule 9.
func parseExports(payload []byte, m *moduleInfo) error {
	r := newReader(payload)
	count, err := r.readVecCount()
	if err != nil {
		return fmt.Errorf("export section: count: %w", err)
	}
	for i := uint32(0); i < count; i++ {
		name, err := r.readName()
		if err != nil {
			return fmt.Errorf("export %d: name: %w", i, err)
		}
		kind, err := r.readByte()
		if err != nil {
			return fmt.Errorf("export %d: kind: %w", i, err)
		}
		idx, err := r.readVarU32()
		if err != nil {
			return fmt.Errorf("export %d: index: %w", i, err)
		}

		if name == "_start" || strings.HasPrefix(name, "__zenon_") {
			return fmt.Errorf("export %d %q: %w", i, name, ErrReservedExport)
		}

		switch kind {
		case 0x00: // function
			total := m.imports + uint32(len(m.funcs))
			if idx >= total {
				return fmt.Errorf("export %d %q: function index %d: %w", i, name, idx, ErrFunctionIndexOutOfBounds)
			}
			if name == "run" {
				if idx < m.imports {
					return fmt.Errorf("export %d %q: %w", i, name, ErrRunImported)
				}
				sig := m.types[m.funcs[idx-m.imports]]
				if !sigEqual(sig, runSig) {
					return fmt.Errorf("export %d %q: %w", i, name, ErrRunSignature)
				}
				m.sawRunExport = true
			}
		case 0x01: // table
			if !m.hasTable || idx != 0 {
				return fmt.Errorf("export %d %q: table index %d: %w", i, name, idx, ErrTableIndexOutOfBounds)
			}
		case 0x02: // memory
			if !m.hasMem || idx != 0 {
				return fmt.Errorf("export %d %q: memory index %d: %w", i, name, idx, ErrMemoryIndexOutOfBounds)
			}
			if name == "memory" {
				m.sawMemoryExport = true
			}
		case 0x03: // global
			if idx >= uint32(len(m.globals)) {
				return fmt.Errorf("export %d %q: global index %d: %w", i, name, idx, ErrGlobalIndexOutOfBounds)
			}
			if m.globals[idx].mutable {
				return fmt.Errorf("export %d %q: %w", i, name, ErrMutableGlobalExport)
			}
		default:
			return fmt.Errorf("export %d %q: kind 0x%02X: %w", i, name, kind, ErrMalformedSection)
		}
	}
	if r.remaining() != 0 {
		return fmt.Errorf("export section: %w", ErrSectionSizeMismatch)
	}
	return nil
}

// parseElements implements rules 10 and, for the active-segment bounds check, 12.
func parseElements(payload []byte, m *moduleInfo, lim Limits) error {
	r := newReader(payload)
	count, err := r.readVecCount()
	if err != nil {
		return fmt.Errorf("element section: count: %w", err)
	}
	var totalEntries uint64
	for i := uint32(0); i < count; i++ {
		flags, err := r.readByte()
		if err != nil {
			return fmt.Errorf("element %d: flags: %w", i, err)
		}
		if flags >= 0x01 && flags <= 0x07 {
			return fmt.Errorf("element %d: flags 0x%02X: %w", i, flags, ErrBulkMemory)
		}
		if flags != 0x00 {
			return fmt.Errorf("element %d: flags 0x%02X: %w", i, flags, ErrMalformedSection)
		}
		offVal, ok, err := readConstI32(r)
		if err != nil {
			return fmt.Errorf("element %d: offset: %w", i, err)
		}
		if !ok {
			return fmt.Errorf("element %d: offset: %w", i, ErrNonConstantSegmentOffset)
		}
		idxCount, err := r.readVecCount()
		if err != nil {
			return fmt.Errorf("element %d: function index count: %w", i, err)
		}
		totalEntries += uint64(idxCount)
		if totalEntries > lim.MaxTotalElementEntries {
			return fmt.Errorf("element section: total entries %d exceeds MaxTotalElementEntries %d: %w", totalEntries, lim.MaxTotalElementEntries, ErrTotalElementEntriesExceeded)
		}
		total := m.imports + uint32(len(m.funcs))
		numFuncs := uint32(0)
		for j := uint32(0); j < idxCount; j++ {
			fi, err := r.readVarU32()
			if err != nil {
				return fmt.Errorf("element %d func %d: %w", i, j, err)
			}
			if fi >= total {
				return fmt.Errorf("element %d func %d: index %d: %w", i, j, fi, ErrFunctionIndexOutOfBounds)
			}
			numFuncs++
		}
		// A segment with no table declared has no table 0 to reference. Rule 9 only requires
		// a memory and a run export, not a table export, so this is the primary check that
		// rejects a table-less module with an element segment.
		if !m.hasTable {
			return fmt.Errorf("element %d: no table declared: %w", i, ErrTableIndexOutOfBounds)
		}
		if uint64(uint32(offVal))+uint64(numFuncs) > uint64(m.table.min) {
			return fmt.Errorf("element %d: %w", i, ErrElementSegmentOutOfBounds)
		}
	}
	if r.remaining() != 0 {
		return fmt.Errorf("element section: %w", ErrSectionSizeMismatch)
	}
	return nil
}

// parseData implements rules 11 and, for the active-segment bounds check, 12.
func parseData(payload []byte, m *moduleInfo, lim Limits) error {
	r := newReader(payload)
	count, err := r.readVecCount()
	if err != nil {
		return fmt.Errorf("data section: count: %w", err)
	}
	if count > lim.MaxDataSegments {
		return fmt.Errorf("data section: %d segments exceeds MaxDataSegments %d: %w", count, lim.MaxDataSegments, ErrTooManyDataSegments)
	}
	for i := uint32(0); i < count; i++ {
		flags, err := r.readByte()
		if err != nil {
			return fmt.Errorf("data %d: flags: %w", i, err)
		}
		if flags == 0x01 || flags == 0x02 {
			return fmt.Errorf("data %d: flags 0x%02X: %w", i, flags, ErrBulkMemory)
		}
		if flags != 0x00 {
			return fmt.Errorf("data %d: flags 0x%02X: %w", i, flags, ErrMalformedSection)
		}
		offVal, ok, err := readConstI32(r)
		if err != nil {
			return fmt.Errorf("data %d: offset: %w", i, err)
		}
		if !ok {
			return fmt.Errorf("data %d: offset: %w", i, ErrNonConstantSegmentOffset)
		}
		n, err := r.readVarU32()
		if err != nil {
			return fmt.Errorf("data %d: length: %w", i, err)
		}
		if uint64(n) > uint64(r.remaining()) {
			return fmt.Errorf("data %d: %w", i, ErrSectionSizeMismatch)
		}
		bs, err := r.readBytes(int(n))
		if err != nil {
			return fmt.Errorf("data %d: bytes: %w", i, err)
		}
		// A segment with no memory declared has no memory 0 to reference. Unreachable once
		// rule 9's memory-export requirement has fired first, kept as defence in depth (see
		// the equivalent comment in parseElements).
		if !m.hasMem {
			return fmt.Errorf("data %d: no memory declared: %w", i, ErrMemoryIndexOutOfBounds)
		}
		if uint64(uint32(offVal))+uint64(len(bs)) > uint64(m.memory.min)*65536 {
			return fmt.Errorf("data %d: %w", i, ErrDataSegmentOutOfBounds)
		}
	}
	if r.remaining() != 0 {
		return fmt.Errorf("data section: %w", ErrSectionSizeMismatch)
	}
	return nil
}

// parseCode implements rule 13.
func parseCode(payload []byte, m *moduleInfo, lim Limits) error {
	r := newReader(payload)
	count, err := r.readVecCount()
	if err != nil {
		return fmt.Errorf("code section: count: %w", err)
	}
	if count != uint32(len(m.funcs)) {
		return fmt.Errorf("code section: %d bodies, function section has %d: %w", count, len(m.funcs), ErrFunctionCodeMismatch)
	}

	numTables, numMemories := uint32(0), uint32(0)
	if m.hasTable {
		numTables = 1
	}
	if m.hasMem {
		numMemories = 1
	}
	numFuncs := m.imports + uint32(len(m.funcs))

	var totalLocals uint64
	for i := uint32(0); i < count; i++ {
		bodyLen, err := r.readVarU32()
		if err != nil {
			return fmt.Errorf("code %d: body length: %w", i, err)
		}
		if uint64(bodyLen) > uint64(r.remaining()) {
			return fmt.Errorf("code %d: %w", i, ErrSectionSizeMismatch)
		}
		bodyBytes, err := r.readBytes(int(bodyLen))
		if err != nil {
			return fmt.Errorf("code %d: %w", i, err)
		}

		br := newReader(bodyBytes)
		sig := m.types[m.funcs[i]]
		numLocals, err := parseLocalsAndCheck(br, uint32(len(sig.params)), lim.MaxLocalsPerFunction)
		if err != nil {
			return fmt.Errorf("code %d: locals: %w", i, err)
		}
		totalLocals += uint64(numLocals)
		if totalLocals > lim.MaxTotalLocals {
			return fmt.Errorf("code %d: total locals %d exceeds MaxTotalLocals %d: %w", i, totalLocals, lim.MaxTotalLocals, ErrTotalLocalsExceeded)
		}

		ctx := bodyCtx{
			numTypes:    uint32(len(m.types)),
			numFuncs:    numFuncs,
			numTables:   numTables,
			numMemories: numMemories,
			numGlobals:  uint32(len(m.globals)),
			numLocals:   numLocals,
		}
		if err := validateBody(bodyBytes[br.pos:], ctx); err != nil {
			return fmt.Errorf("code %d: %w", i, err)
		}
	}
	if r.remaining() != 0 {
		return fmt.Errorf("code section: %w", ErrSectionSizeMismatch)
	}
	return nil
}
