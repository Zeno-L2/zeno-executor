package module

import (
	"errors"
	"fmt"
)

// This file implements Instrument, the deterministic, append-only rewriter that turns a
// module module.Validate has already accepted into a metered one: gas accounting, a
// stack-depth limiter and a memory-growth limiter, injected as new globals, new helper
// functions and a per-function-body wrapper, with every existing section's payload bytes
// (beyond a grown count prefix) copied through unmodified. Instrument does not call Validate
// itself -- a validated input is the caller's contract -- but it never panics on any input: it
// decodes with the same classifyOpcode discipline binary.go's validateBody uses and returns an
// error instead of desynchronizing or slicing blindly.

// maxStackDepth mirrors plugins/wasm.MaxStackDepth, which this package cannot import.
const maxStackDepth = 512

// maxInstrumentedCodeSize mirrors plugins/wasm.MaxInstrumentedCodeSize, which this package
// cannot import.
const maxInstrumentedCodeSize = 1 << 20

// ErrInstrumentedTooLarge is returned when the instrumented module exceeds
// maxInstrumentedCodeSize. Nothing is hashed in that case.
var ErrInstrumentedTooLarge = errors.New("module: instrumented module too large")

// ErrNotValidated is returned for a precondition Instrument itself relies on and can check for
// free while parsing (no memory section, a memory count other than 1, a declared memory
// minimum above lim.MaxMemoryPages, a code/function section count mismatch, a function type
// with more than one result, or a decode desync this package's total decoder detects), and
// generally for any input shape Instrument cannot safely proceed on without re-implementing
// Validate's full rule set.
var ErrNotValidated = errors.New("module: input is not a validated module")

// Instrument rewrites a validated module into its instrumented form: gas metering, a
// stack-depth limiter and a memory-growth limiter, injected as new types, globals, functions
// and exports, plus a rewritten body per original function. It is pure and byte-deterministic:
// the same (original, lim) always produces byte-identical output, the input slice is never
// modified, and the output never aliases the input's backing array.
func Instrument(original []byte, lim Limits) ([]byte, error) {
	rest, err := parsePreamble(original)
	if err != nil {
		return nil, err
	}
	secs, err := splitSections(rest)
	if err != nil {
		return nil, err
	}

	var (
		typesPayload []byte
		types        []funcSig

		haveImports    bool
		importsPayload []byte
		importCount    uint32

		funcsPayload []byte
		funcs        []uint32

		haveTable    bool
		tablePayload []byte

		haveMem bool
		memMin  uint32

		globalsPayload []byte
		globalCount    uint32

		exportsPayload []byte
		exportCount    uint32

		haveElem    bool
		elemPayload []byte

		haveCode    bool
		codePayload []byte
		codeCount   uint32

		haveData    bool
		dataPayload []byte
	)

	for _, s := range secs {
		switch s.id {
		case 1:
			sigs, err := parseTypesLoose(s.payload)
			if err != nil {
				return nil, fmt.Errorf("type section: %w", err)
			}
			for i, sig := range sigs {
				if len(sig.results) > 1 {
					return nil, fmt.Errorf("type %d: %d results: %w", i, len(sig.results), ErrNotValidated)
				}
			}
			types = sigs
			typesPayload = s.payload
		case 2:
			haveImports = true
			importsPayload = s.payload
			n, _, err := splitVecCount(s.payload)
			if err != nil {
				return nil, fmt.Errorf("import section: count: %w", err)
			}
			if err := checkImportsAllFunctions(s.payload); err != nil {
				return nil, fmt.Errorf("import section: %w", err)
			}
			importCount = n
		case 3:
			idxs, err := parseFuncsLoose(s.payload)
			if err != nil {
				return nil, fmt.Errorf("function section: %w", err)
			}
			funcs = idxs
			funcsPayload = s.payload
		case 4:
			haveTable = true
			tablePayload = s.payload
		case 5:
			min, err := parseMemoryLoose(s.payload)
			if err != nil {
				return nil, fmt.Errorf("memory section: %w", err)
			}
			memMin = min
			haveMem = true
		case 6:
			globalsPayload = s.payload
			n, _, err := splitVecCount(s.payload)
			if err != nil {
				return nil, fmt.Errorf("global section: count: %w", err)
			}
			globalCount = n
		case 7:
			exportsPayload = s.payload
			n, _, err := splitVecCount(s.payload)
			if err != nil {
				return nil, fmt.Errorf("export section: count: %w", err)
			}
			exportCount = n
		case 9:
			haveElem = true
			elemPayload = s.payload
		case 10:
			haveCode = true
			codePayload = s.payload
			n, _, err := splitVecCount(s.payload)
			if err != nil {
				return nil, fmt.Errorf("code section: count: %w", err)
			}
			codeCount = n
		case 11:
			haveData = true
			dataPayload = s.payload
		}
	}

	if !haveMem {
		return nil, fmt.Errorf("no memory section: %w", ErrNotValidated)
	}
	if memMin > lim.MaxMemoryPages {
		return nil, fmt.Errorf("memory min %d exceeds MaxMemoryPages %d: %w", memMin, lim.MaxMemoryPages, ErrNotValidated)
	}
	if codeCount != uint32(len(funcs)) {
		return nil, fmt.Errorf("code section: %d bodies, function section has %d: %w", codeCount, len(funcs), ErrNotValidated)
	}

	T := uint32(len(types))
	G := globalCount
	I := importCount
	F := uint32(len(funcs))
	base := I + F

	out := make([]byte, 0, len(original)+4096)
	out = append(out, 0x00, 'a', 's', 'm', 0x01, 0x00, 0x00, 0x00)

	// Section 1: types. Always emitted (the 4 appended types are unconditional).
	{
		var restTypes []byte
		if typesPayload != nil {
			_, rest, err := splitVecCount(typesPayload)
			if err != nil {
				return nil, fmt.Errorf("type section: %w", err)
			}
			restTypes = rest
		}
		payload := appendVarU32(nil, T+4)
		payload = append(payload, restTypes...)
		payload = append(payload, appendedTypesBytes()...)
		out = writeSection(out, 1, payload)
	}

	// Section 2: imports, verbatim, omitted if absent.
	if haveImports {
		out = writeSection(out, 2, importsPayload)
	}

	// Section 3: functions. Always emitted (the 6 appended functions are unconditional).
	{
		var restFuncs []byte
		if funcsPayload != nil {
			_, rest, err := splitVecCount(funcsPayload)
			if err != nil {
				return nil, fmt.Errorf("function section: %w", err)
			}
			restFuncs = rest
		}
		payload := appendVarU32(nil, F+6)
		payload = append(payload, restFuncs...)
		payload = append(payload, appendedFuncsBytes(T)...)
		out = writeSection(out, 3, payload)
	}

	// Section 4: table, verbatim, omitted if absent.
	if haveTable {
		out = writeSection(out, 4, tablePayload)
	}

	// Section 5: memory, rewritten: count 1, flags 0x01, min = declared min, max =
	// lim.MaxMemoryPages.
	{
		payload := []byte{0x01, 0x01}
		payload = appendVarU32(payload, memMin)
		payload = appendVarU32(payload, lim.MaxMemoryPages)
		out = writeSection(out, 5, payload)
	}

	// Section 6: globals. Always emitted, even if the original had none.
	{
		var restGlobals []byte
		if globalsPayload != nil {
			_, rest, err := splitVecCount(globalsPayload)
			if err != nil {
				return nil, fmt.Errorf("global section: %w", err)
			}
			restGlobals = rest
		}
		payload := appendVarU32(nil, G+2)
		payload = append(payload, restGlobals...)
		payload = append(payload, appendedGlobalsBytes()...)
		out = writeSection(out, 6, payload)
	}

	// Section 7: exports. Always emitted.
	{
		var restExports []byte
		if exportsPayload != nil {
			_, rest, err := splitVecCount(exportsPayload)
			if err != nil {
				return nil, fmt.Errorf("export section: %w", err)
			}
			restExports = rest
		}
		payload := appendVarU32(nil, exportCount+2)
		payload = append(payload, restExports...)
		payload = append(payload, appendedExportsBytes(base)...)
		out = writeSection(out, 7, payload)
	}

	// Section 9: elements, verbatim, omitted if absent.
	if haveElem {
		out = writeSection(out, 9, elemPayload)
	}

	// Section 10: code. Always emitted: each original body rewritten, then the 6 helper
	// bodies appended.
	{
		var codeBodies []byte
		if haveCode {
			cr := newReader(codePayload)
			cnt, err := cr.readVecCount()
			if err != nil {
				return nil, fmt.Errorf("code section: count: %w", err)
			}
			for i := uint32(0); i < cnt; i++ {
				bodyLen, err := cr.readVarU32()
				if err != nil {
					return nil, fmt.Errorf("code %d: body length: %w", i, err)
				}
				if uint64(bodyLen) > uint64(cr.remaining()) {
					return nil, fmt.Errorf("code %d: %w", i, ErrSectionSizeMismatch)
				}
				bodyBytes, err := cr.readBytes(int(bodyLen))
				if err != nil {
					return nil, fmt.Errorf("code %d: %w", i, err)
				}
				if uint64(funcs[i]) >= uint64(len(types)) {
					return nil, fmt.Errorf("code %d: type index %d: %w", i, funcs[i], ErrNotValidated)
				}
				sig := types[funcs[i]]
				rewritten, err := rewriteBody(bodyBytes, sig, base)
				if err != nil {
					return nil, fmt.Errorf("code %d: %w", i, err)
				}
				codeBodies = appendVarU32(codeBodies, uint32(len(rewritten)))
				codeBodies = append(codeBodies, rewritten...)
			}
			if cr.remaining() != 0 {
				return nil, fmt.Errorf("code section: %w", ErrSectionSizeMismatch)
			}
		}
		for _, h := range buildHelperBodies(base, G+0, G+1, lim) {
			codeBodies = appendVarU32(codeBodies, uint32(len(h)))
			codeBodies = append(codeBodies, h...)
		}
		payload := appendVarU32(nil, F+6)
		payload = append(payload, codeBodies...)
		out = writeSection(out, 10, payload)
	}

	// Section 11: data, verbatim, omitted if absent.
	if haveData {
		out = writeSection(out, 11, dataPayload)
	}

	if len(out) > maxInstrumentedCodeSize {
		return nil, fmt.Errorf("instrumented module %d bytes exceeds %d: %w", len(out), maxInstrumentedCodeSize, ErrInstrumentedTooLarge)
	}
	return out, nil
}

// writeSection appends one section (id, recomputed length prefix, payload) to dst.
func writeSection(dst []byte, id byte, payload []byte) []byte {
	dst = append(dst, id)
	dst = appendVarU32(dst, uint32(len(payload)))
	return append(dst, payload...)
}

// splitVecCount reads payload's leading vector count with the same fits-in-remaining-bytes
// discipline as reader.readVecCount, and returns it along with the unconsumed remainder: the
// vector's own entries, which Instrument copies through unmodified wherever a section's count
// alone needs to grow.
func splitVecCount(payload []byte) (count uint32, rest []byte, err error) {
	r := newReader(payload)
	count, err = r.readVecCount()
	if err != nil {
		return 0, nil, err
	}
	return count, payload[r.pos:], nil
}

// checkImportsAllFunctions walks payload's import vector and returns ErrNotValidated if any
// entry's descriptor kind is not 0x00 (function import). Instrument derives the injected
// globals' indices as G+0/G+1 and the injected functions' indices from base = I+F, which are
// only correct if the import section contributes exclusively to the function index space (no
// imported table, memory or global shifts a different index space out from under that
// arithmetic). A validated module already guarantees this, but Instrument checks it itself
// instead of trusting that guarantee silently, so a mis-indexed ledger fails closed rather than
// compiling and running against the wrong global.
func checkImportsAllFunctions(payload []byte) error {
	r := newReader(payload)
	count, err := r.readVecCount()
	if err != nil {
		return err
	}
	for i := uint32(0); i < count; i++ {
		if _, err := r.readName(); err != nil { // module name
			return err
		}
		if _, err := r.readName(); err != nil { // import name
			return err
		}
		desc, err := r.readByte()
		if err != nil {
			return err
		}
		if desc != 0x00 {
			return fmt.Errorf("import %d: kind 0x%02X is not a function import: %w", i, desc, ErrNotValidated)
		}
		if _, err := r.readVarU32(); err != nil { // type index
			return err
		}
	}
	if r.remaining() != 0 {
		return ErrSectionSizeMismatch
	}
	return nil
}

// parseTypesLoose decodes a type section payload into per-type
// param/result valtypes, tolerating any form byte: Instrument does not
// re-run the type section's form == 0x60 check (a validated input is
// the caller's contract), it only needs each type's shape to look up a
// function's block-wrapper result type and to check the one
// precondition (more than one result) that is cheap to catch here.
func parseTypesLoose(payload []byte) ([]funcSig, error) {
	r := newReader(payload)
	count, err := r.readVecCount()
	if err != nil {
		return nil, err
	}
	var sigs []funcSig
	for i := uint32(0); i < count; i++ {
		if _, err := r.readByte(); err != nil { // form
			return nil, err
		}
		params, err := readValTypeVec(r)
		if err != nil {
			return nil, err
		}
		results, err := readValTypeVec(r)
		if err != nil {
			return nil, err
		}
		sigs = append(sigs, funcSig{params: params, results: results})
	}
	if r.remaining() != 0 {
		return nil, ErrSectionSizeMismatch
	}
	return sigs, nil
}

// parseFuncsLoose decodes a function section payload into its per-function type indices.
func parseFuncsLoose(payload []byte) ([]uint32, error) {
	r := newReader(payload)
	count, err := r.readVecCount()
	if err != nil {
		return nil, err
	}
	var idxs []uint32
	for i := uint32(0); i < count; i++ {
		idx, err := r.readVarU32()
		if err != nil {
			return nil, err
		}
		idxs = append(idxs, idx)
	}
	if r.remaining() != 0 {
		return nil, ErrSectionSizeMismatch
	}
	return idxs, nil
}

// parseMemoryLoose decodes a memory section payload, requiring exactly one memory (a validated
// module always declares exactly one), and returns its declared minimum. The maximum is
// not returned: the output memory's maximum is always lim.MaxMemoryPages, never the declared
// one.
func parseMemoryLoose(payload []byte) (min uint32, err error) {
	r := newReader(payload)
	count, err := r.readVecCount()
	if err != nil {
		return 0, err
	}
	if count != 1 {
		return 0, fmt.Errorf("%d memories: %w", count, ErrNotValidated)
	}
	lr, err := readLimitsRaw(r)
	if err != nil {
		return 0, err
	}
	if r.remaining() != 0 {
		return 0, ErrSectionSizeMismatch
	}
	return lr.min, nil
}

// appendExportName appends a length-prefixed name, the same encoding readName decodes. Named
// distinctly from fixtures_test.go's identical test-only appendName to avoid a same-package
// redeclaration.
func appendExportName(dst []byte, s string) []byte {
	dst = appendVarU32(dst, uint32(len(s)))
	return append(dst, s...)
}

// encodeFuncType appends one func type entry (form 0x60, params, results).
func encodeFuncType(dst []byte, params, results []byte) []byte {
	dst = append(dst, 0x60)
	dst = appendVarU32(dst, uint32(len(params)))
	dst = append(dst, params...)
	dst = appendVarU32(dst, uint32(len(results)))
	dst = append(dst, results...)
	return dst
}

// appendedTypesBytes returns the 4 types this pass always appends, in the fixed order T+0..T+3.
func appendedTypesBytes() []byte {
	var b []byte
	b = encodeFuncType(b, []byte{0x7E}, nil)          // T+0: (i64) -> ()
	b = encodeFuncType(b, nil, nil)                   // T+1: () -> ()
	b = encodeFuncType(b, []byte{0x7F}, []byte{0x7F}) // T+2: (i32) -> (i32)
	b = encodeFuncType(b, nil, []byte{0x7E})          // T+3: () -> (i64)
	return b
}

// appendedFuncsBytes returns the 6 appended functions' type indices, in the fixed order
// base+0..base+5.
func appendedFuncsBytes(T uint32) []byte {
	var b []byte
	b = appendVarU32(b, T+0) // __zenon_charge
	b = appendVarU32(b, T+1) // __zenon_enter
	b = appendVarU32(b, T+1) // __zenon_exit
	b = appendVarU32(b, T+2) // __zenon_grow
	b = appendVarU32(b, T+3) // __zenon_gas_get
	b = appendVarU32(b, T+0) // __zenon_gas_set
	return b
}

// appendedGlobalsBytes returns the 2 appended globals: __zenon_gas (mut i64, init 0) and
// __zenon_depth (mut i32, init 0). Neither is exported.
func appendedGlobalsBytes() []byte {
	b := []byte{0x7E, 0x01, 0x42, 0x00, 0x0B}   // __zenon_gas: mut i64, init i64.const 0; end
	b = append(b, 0x7F, 0x01, 0x41, 0x00, 0x0B) // __zenon_depth: mut i32, init i32.const 0; end
	return b
}

// appendedExportsBytes returns the 2 appended function exports.
func appendedExportsBytes(base uint32) []byte {
	var b []byte
	b = appendExportName(b, "__zenon_gas_get")
	b = append(b, 0x00)
	b = appendVarU32(b, base+4)
	b = appendExportName(b, "__zenon_gas_set")
	b = append(b, 0x00)
	b = appendVarU32(b, base+5)
	return b
}

// instrRecord is one decoded instruction of a function body's instruction stream: its opcode
// and its byte range [start, end) within instrBytes.
type instrRecord struct {
	op         byte
	start, end int
}

// decodeBody splits a function body into its locals declaration (returned verbatim) and its
// instruction stream, decoding the stream once into a slice of instrRecord with the same
// classifyOpcode discipline validateBody uses (binary.go:396): the opcode is classified before
// any immediate is read, so an opcode outside the accepted set is rejected instead of
// desynchronizing the decoder. The final end -- the one that takes a depth seeded at 1 to 0 --
// is located by decoding, not by index; a stream that runs out before reaching depth 0, or
// that leaves bytes after that end, is ErrNotValidated. A zero-length instruction stream hits
// the former case immediately.
func decodeBody(body []byte) (localsBytes, instrBytes []byte, instrs []instrRecord, finalEnd int, err error) {
	lr := newReader(body)
	localsCount, err := lr.readVecCount()
	if err != nil {
		return nil, nil, nil, 0, err
	}
	for i := uint32(0); i < localsCount; i++ {
		if _, err := lr.readVarU32(); err != nil {
			return nil, nil, nil, 0, err
		}
		if _, err := lr.readByte(); err != nil {
			return nil, nil, nil, 0, err
		}
	}
	localsBytes = body[:lr.pos]
	instrBytes = body[lr.pos:]

	r := newReader(instrBytes)
	depth := 1
	for {
		if r.atEnd() {
			return nil, nil, nil, 0, fmt.Errorf("instruction stream ends before its implicit block closes: %w", ErrNotValidated)
		}
		start := r.pos
		op, err := r.readByte()
		if err != nil {
			return nil, nil, nil, 0, err
		}
		if err := classifyOpcode(r, op); err != nil {
			return nil, nil, nil, 0, fmt.Errorf("opcode 0x%02X: %w", op, err)
		}
		switch {
		case op == 0x02 || op == 0x03 || op == 0x04: // block, loop, if
			if _, err := r.readByte(); err != nil { // block type
				return nil, nil, nil, 0, err
			}
			depth++
		case op == 0x05: // else: neutral
		case op == 0x0B: // end
			depth--
		case op == 0x0C || op == 0x0D: // br, br_if
			if _, err := r.readVarU32(); err != nil {
				return nil, nil, nil, 0, err
			}
		case op == 0x0E: // br_table
			n, err := r.readVecCount()
			if err != nil {
				return nil, nil, nil, 0, err
			}
			for i := uint32(0); i < n; i++ {
				if _, err := r.readVarU32(); err != nil {
					return nil, nil, nil, 0, err
				}
			}
			if _, err := r.readVarU32(); err != nil { // default target
				return nil, nil, nil, 0, err
			}
		case op == 0x10: // call
			if _, err := r.readVarU32(); err != nil {
				return nil, nil, nil, 0, err
			}
		case op == 0x11: // call_indirect
			if _, err := r.readVarU32(); err != nil { // type index
				return nil, nil, nil, 0, err
			}
			if _, err := r.readVarU32(); err != nil { // table index
				return nil, nil, nil, 0, err
			}
		case op >= 0x20 && op <= 0x22: // local.get, local.set, local.tee
			if _, err := r.readVarU32(); err != nil {
				return nil, nil, nil, 0, err
			}
		case op == 0x23 || op == 0x24: // global.get, global.set
			if _, err := r.readVarU32(); err != nil {
				return nil, nil, nil, 0, err
			}
		case op >= 0x28 && op <= 0x3E: // memarg loads/stores
			if _, err := r.readVarU32(); err != nil { // align
				return nil, nil, nil, 0, err
			}
			if _, err := r.readVarU32(); err != nil { // offset
				return nil, nil, nil, 0, err
			}
		case op == 0x3F || op == 0x40: // memory.size, memory.grow
			if _, err := r.readByte(); err != nil { // reserved memory index byte
				return nil, nil, nil, 0, err
			}
		case op == 0x41: // i32.const
			if _, err := r.readVarS32(); err != nil {
				return nil, nil, nil, 0, err
			}
		case op == 0x42: // i64.const
			if _, err := r.readVarS64(); err != nil {
				return nil, nil, nil, 0, err
			}
		default:
			// no immediate
		}
		instrs = append(instrs, instrRecord{op: op, start: start, end: r.pos})
		if op == 0x0B && depth == 0 {
			finalEnd = len(instrs) - 1
			break
		}
	}
	if r.remaining() != 0 {
		return nil, nil, nil, 0, fmt.Errorf("trailing bytes after the final end: %w", ErrNotValidated)
	}
	return localsBytes, instrBytes, instrs, finalEnd, nil
}

// instrCost returns rec's metered cost: OpcodeCost for every opcode except br_table, which
// contributes BrTableCost(N) in its place, never both. Every opcode reaching here
// has already passed classifyOpcode during decodeBody, so OpcodeCost always succeeds; the
// error return is defence in depth against that invariant, never a case decodeBody can
// actually produce.
func instrCost(instrBytes []byte, rec instrRecord) (uint64, error) {
	if rec.op == 0x0E { // br_table
		n, _, err := readVarU32(instrBytes, rec.start+1)
		if err != nil {
			return 0, err
		}
		return BrTableCost(int(n)), nil
	}
	cost, ok := OpcodeCost(Opcode(rec.op))
	if !ok {
		return 0, fmt.Errorf("opcode 0x%02X: %w", rec.op, ErrUnmeteredOpcode)
	}
	return cost, nil
}

// segmentLeaderTotals computes, over the whole of instrs (the final end included), each
// segment's total cost, indexed by its leader's position in instrs; every non-leader index is
// 0. A leader is the first instruction, or any instruction whose predecessor satisfies
// TerminatesSegment; a segment runs from a leader up to and including the terminator that ends
// it (or the end of instrs). See gastable.go's terminators doc comment for the normative rule.
func segmentLeaderTotals(instrBytes []byte, instrs []instrRecord) ([]uint64, error) {
	totals := make([]uint64, len(instrs))
	i := 0
	for i < len(instrs) {
		j := i
		var total uint64
		for {
			c, err := instrCost(instrBytes, instrs[j])
			if err != nil {
				return nil, err
			}
			total += c
			if TerminatesSegment(Opcode(instrs[j].op)) || j == len(instrs)-1 {
				break
			}
			j++
		}
		totals[i] = total
		i = j + 1
	}
	return totals, nil
}

// rewriteBody rewrites one original function body into its instrumented form: a call to
// __zenon_enter, a wrapper block (whose type mirrors sig's result, or void), the original
// instructions with segment charges, memory.grow and return rewritten in place, the final
// end's own segment charge if it is itself a leader with a non-zero total (load-bearing: see the
// emission site below for why), the wrapper's end, a call to __zenon_exit, and the function's own
// end. base is I+F, the index of the first entity this pass appends.
func rewriteBody(body []byte, sig funcSig, base uint32) ([]byte, error) {
	localsBytes, instrBytes, instrs, finalEnd, err := decodeBody(body)
	if err != nil {
		return nil, err
	}
	segTotal, err := segmentLeaderTotals(instrBytes, instrs)
	if err != nil {
		return nil, err
	}

	out := make([]byte, 0, len(body)+32)
	out = append(out, localsBytes...)

	out = append(out, 0x10)
	out = appendVarU32(out, base+1) // call __zenon_enter

	out = append(out, 0x02) // block <bt>
	if len(sig.results) == 0 {
		out = append(out, 0x40)
	} else {
		out = append(out, sig.results[0])
	}

	for k := 0; k < finalEnd; k++ {
		rec := instrs[k]
		if segTotal[k] != 0 {
			out = append(out, 0x42)
			out = appendVarS64(out, int64(segTotal[k]))
			out = append(out, 0x10)
			out = appendVarU32(out, base+0) // call __zenon_charge
		}
		switch rec.op {
		case 0x40: // memory.grow -> call __zenon_grow
			out = append(out, 0x10)
			out = appendVarU32(out, base+3)
		case 0x0F: // return: call __zenon_exit before the byte itself
			out = append(out, 0x10)
			out = appendVarU32(out, base+2)
			out = append(out, instrBytes[rec.start:rec.end]...)
		default:
			out = append(out, instrBytes[rec.start:rec.end]...)
		}
	}

	// The final end's own segment charge, emitted inside the wrapper block so fall-through
	// pays it while a branch to the outermost label (which lands after the wrapper's end)
	// correctly skips it. The final end byte itself is never re-emitted: the wrapper's two
	// ends take its place.
	if segTotal[finalEnd] != 0 {
		out = append(out, 0x42)
		out = appendVarS64(out, int64(segTotal[finalEnd]))
		out = append(out, 0x10)
		out = appendVarU32(out, base+0)
	}

	out = append(out, 0x0B) // end (closes the wrapper block)
	out = append(out, 0x10)
	out = appendVarU32(out, base+2) // call __zenon_exit
	out = append(out, 0x0B)         // end (closes the function)

	return out, nil
}

// buildHelperBodies returns the 6 injected functions' bodies, each preceded by its own empty
// (0x00) locals vector, in the fixed order base+0..base+5.
func buildHelperBodies(base, gasGlobal, depthGlobal uint32, lim Limits) [][]byte {
	return [][]byte{
		buildCharge(gasGlobal),
		buildEnter(depthGlobal),
		buildExit(depthGlobal),
		buildGrow(base, lim.MaxMemoryPages),
		buildGasGet(gasGlobal),
		buildGasSet(gasGlobal),
	}
}

// buildCharge builds __zenon_charge(param i64 cost)'s body: the sole out-of-gas marker is
// gas = -1 set before the trap, so a conforming execution can never leave the counter negative.
func buildCharge(gasGlobal uint32) []byte {
	b := []byte{0x00} // empty locals
	b = append(b, 0x23)
	b = appendVarU32(b, gasGlobal) // global.get <gas>
	b = append(b, 0x20, 0x00)      // local.get 0
	b = append(b, 0x53)            // i64.lt_s
	b = append(b, 0x04, 0x40)      // if
	b = append(b, 0x42, 0x7F)      //   i64.const -1
	b = append(b, 0x24)
	b = appendVarU32(b, gasGlobal) //   global.set <gas>
	b = append(b, 0x00)            //   unreachable
	b = append(b, 0x0B)            // end
	b = append(b, 0x23)
	b = appendVarU32(b, gasGlobal) // global.get <gas>
	b = append(b, 0x20, 0x00)      // local.get 0
	b = append(b, 0x7D)            // i64.sub
	b = append(b, 0x24)
	b = appendVarU32(b, gasGlobal) // global.set <gas>
	b = append(b, 0x0B)            // end
	return b
}

// buildEnter builds __zenon_enter()'s body: admits maxStackDepth frames and traps on the next
// one.
func buildEnter(depthGlobal uint32) []byte {
	b := []byte{0x00}
	b = append(b, 0x23)
	b = appendVarU32(b, depthGlobal) // global.get <depth>
	b = append(b, 0x41, 0x01)        // i32.const 1
	b = append(b, 0x6A)              // i32.add
	b = append(b, 0x24)
	b = appendVarU32(b, depthGlobal) // global.set <depth>
	b = append(b, 0x23)
	b = appendVarU32(b, depthGlobal) // global.get <depth>
	b = append(b, 0x41)
	b = appendVarS32(b, maxStackDepth) // i32.const <maxStackDepth>
	b = append(b, 0x4A)                // i32.gt_s
	b = append(b, 0x04, 0x40)          // if
	b = append(b, 0x00)                //   unreachable
	b = append(b, 0x0B)                // end
	b = append(b, 0x0B)                // end
	return b
}

// buildExit builds __zenon_exit()'s body.
func buildExit(depthGlobal uint32) []byte {
	b := []byte{0x00}
	b = append(b, 0x23)
	b = appendVarU32(b, depthGlobal) // global.get <depth>
	b = append(b, 0x41, 0x01)        // i32.const 1
	b = append(b, 0x6B)              // i32.sub
	b = append(b, 0x24)
	b = appendVarU32(b, depthGlobal) // global.set <depth>
	b = append(b, 0x0B)              // end
	return b
}

// buildGrow builds __zenon_grow(param i32 delta) -> i32's body. The unsigned framing makes an
// i32 wrap impossible, and memory.size <= MaxMemoryPages is guaranteed by the normalized
// memory maximum, so the subtraction cannot underflow. Over-limit growth returns -1, grows
// nothing, and pays only the base cost the calling segment already charged.
func buildGrow(base, maxMemoryPages uint32) []byte {
	b := []byte{0x00}
	b = append(b, 0x20, 0x00) // local.get 0
	b = append(b, 0x41)
	b = appendVarS32(b, int32(maxMemoryPages)) // i32.const <MaxMemoryPages>
	b = append(b, 0x3F, 0x00)                  // memory.size
	b = append(b, 0x6B)                        // i32.sub
	b = append(b, 0x4B)                        // i32.gt_u
	b = append(b, 0x04, 0x40)                  // if
	b = append(b, 0x41, 0x7F)                  //   i32.const -1
	b = append(b, 0x0F)                        //   return
	b = append(b, 0x0B)                        // end
	b = append(b, 0x20, 0x00)                  // local.get 0
	b = append(b, 0xAD)                        // i64.extend_i32_u
	b = append(b, 0x42)
	b = appendVarS64(b, int64(MemoryGrowPageCost(1))) // i64.const <MemoryGrowPageCost(1)>
	b = append(b, 0x7E)                               // i64.mul
	b = append(b, 0x10)
	b = appendVarU32(b, base+0) // call __zenon_charge
	b = append(b, 0x20, 0x00)   // local.get 0
	b = append(b, 0x40, 0x00)   // memory.grow
	b = append(b, 0x0B)         // end
	return b
}

// buildGasGet builds __zenon_gas_get() -> i64's body.
func buildGasGet(gasGlobal uint32) []byte {
	b := []byte{0x00}
	b = append(b, 0x23)
	b = appendVarU32(b, gasGlobal) // global.get <gas>
	b = append(b, 0x0B)            // end
	return b
}

// buildGasSet builds __zenon_gas_set(param i64)'s body.
func buildGasSet(gasGlobal uint32) []byte {
	b := []byte{0x00}
	b = append(b, 0x20, 0x00) // local.get 0
	b = append(b, 0x24)
	b = appendVarU32(b, gasGlobal) // global.set <gas>
	b = append(b, 0x0B)            // end
	return b
}
