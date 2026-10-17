package module

import (
	"bytes"
	"errors"
	"fmt"
)

// This file owns the byte-level shape of the format: the section
// splitter, the low-level vector/name reader every rule builds on, and
// the per-instruction immediate decoder (the "skipper" parseCode uses
// to classify and consume one instruction at a time). It never
// pre-sizes a slice from a declared count: every vector in the format
// is parsed one entry at a time and appended, and a count that cannot
// fit the bytes remaining is rejected first.

// parsePreamble checks the \0asm magic and the 01 00 00 00 version, returning the bytes that
// follow.
func parsePreamble(wasm []byte) ([]byte, error) {
	if len(wasm) < 4 || !bytes.Equal(wasm[:4], []byte{0x00, 'a', 's', 'm'}) {
		return nil, fmt.Errorf("preamble: %w", ErrBadMagic)
	}
	if len(wasm) < 8 || !bytes.Equal(wasm[4:8], []byte{0x01, 0x00, 0x00, 0x00}) {
		return nil, fmt.Errorf("preamble: %w", ErrBadVersion)
	}
	return wasm[8:], nil
}

// wasmSection is one non-custom section's id and payload, as located by splitSections.
type wasmSection struct {
	id      byte
	payload []byte
}

// splitSections walks the section stream after the preamble, validating id ordering,
// duplication and range, and each section's own length, without parsing any section's
// content. Custom sections (id 0) are rejected outright with ErrCustomSection: wazero's
// decoder parses a custom section literally named "name" unconditionally, at decode time,
// reading declared counts (function/local name-map sizes, the module-name byte length) with no
// bound against the section's own length or the bytes remaining — a 66-byte module can make it
// attempt a 96 GB allocation, regardless of any engine config knob. Skipping a custom section
// here and stripping it later does not help, because the unstripped bytes still reach the
// engine at validation's own compile step; rejecting the whole class, not just sections named
// "name", is the simplest rule that closes the hole.
func splitSections(b []byte) ([]wasmSection, error) {
	var secs []wasmSection
	pos := 0
	prevID := -1
	for pos < len(b) {
		id := b[pos]
		length, n, err := readVarU32(b, pos+1)
		if err != nil {
			if errors.Is(err, ErrTruncated) {
				return nil, fmt.Errorf("byte 0x%02X after last section: %w", id, ErrTrailingBytes)
			}
			return nil, fmt.Errorf("section id %d: length: %w", id, err)
		}
		pos = pos + 1 + n
		if uint64(pos)+uint64(length) > uint64(len(b)) {
			return nil, fmt.Errorf("section id %d: %w", id, ErrTruncated)
		}
		payload := b[pos : pos+int(length)]
		pos += int(length)

		if id == 0 {
			return nil, fmt.Errorf("section id 0: %w", ErrCustomSection)
		}
		if id > 11 {
			return nil, fmt.Errorf("section id %d: %w", id, ErrUnknownSection)
		}
		if prevID >= 0 {
			if int(id) == prevID {
				return nil, fmt.Errorf("section id %d duplicated: %w", id, ErrDuplicateSection)
			}
			if int(id) < prevID {
				return nil, fmt.Errorf("section id %d after section id %d: %w", id, prevID, ErrSectionOrder)
			}
		}
		prevID = int(id)
		secs = append(secs, wasmSection{id: id, payload: payload})
	}
	return secs, nil
}

// reader walks one section's (or one function body's) payload, tracking how many bytes
// remain so every vector count and byte-string length can be checked against it before any
// allocation.
type reader struct {
	b   []byte
	pos int
}

func newReader(b []byte) *reader { return &reader{b: b} }

// remaining reports how many bytes are left to read.
func (r *reader) remaining() int { return len(r.b) - r.pos }

// atEnd reports whether every byte has been consumed.
func (r *reader) atEnd() bool { return r.pos >= len(r.b) }

func (r *reader) readByte() (byte, error) {
	if r.pos >= len(r.b) {
		return 0, ErrTruncated
	}
	v := r.b[r.pos]
	r.pos++
	return v, nil
}

func (r *reader) readVarU32() (uint32, error) {
	v, n, err := readVarU32(r.b, r.pos)
	if err != nil {
		return 0, err
	}
	r.pos += n
	return v, nil
}

func (r *reader) readVarS32() (int32, error) {
	v, n, err := readVarS32(r.b, r.pos)
	if err != nil {
		return 0, err
	}
	r.pos += n
	return v, nil
}

func (r *reader) readVarS64() (int64, error) {
	v, n, err := readVarS64(r.b, r.pos)
	if err != nil {
		return 0, err
	}
	r.pos += n
	return v, nil
}

// readBytes reads exactly n raw bytes. n itself must already have been checked against
// remaining() by the caller (readVecCount or an explicit byte-string length check).
func (r *reader) readBytes(n int) ([]byte, error) {
	if n < 0 || n > r.remaining() {
		return nil, ErrSectionSizeMismatch
	}
	v := r.b[r.pos : r.pos+n]
	r.pos += n
	return v, nil
}

// readVecCount reads a vector's declared entry count and rejects one that cannot fit: every
// vector entry occupies at least one byte, so count > remaining ⇒ ErrSectionSizeMismatch,
// before a single entry is parsed or any slice is allocated.
func (r *reader) readVecCount() (uint32, error) {
	count, err := r.readVarU32()
	if err != nil {
		return 0, err
	}
	if uint64(count) > uint64(r.remaining()) {
		return 0, ErrSectionSizeMismatch
	}
	return count, nil
}

// readName reads a length-prefixed name (a module, import or export name): the same
// fits-in-remaining-bytes guard as readVecCount, applied to a byte-string length.
func (r *reader) readName() (string, error) {
	n, err := r.readVarU32()
	if err != nil {
		return "", err
	}
	if uint64(n) > uint64(r.remaining()) {
		return "", ErrSectionSizeMismatch
	}
	bs, err := r.readBytes(int(n))
	if err != nil {
		return "", err
	}
	return string(bs), nil
}

// checkValType classifies a single valtype byte (the type section's
// check, reused by globals, locals and block types).
func checkValType(b byte) error {
	switch b {
	case 0x7F, 0x7E:
		return nil
	case 0x7D, 0x7C:
		return ErrFloatType
	case 0x7B:
		return ErrSIMD
	case 0x70, 0x6F:
		return ErrReferenceType
	default:
		return ErrInvalidValType
	}
}

// readValTypeVec reads a vec(valtype), appending one entry at a time (never pre-sized from
// the declared count).
func readValTypeVec(r *reader) ([]byte, error) {
	count, err := r.readVecCount()
	if err != nil {
		return nil, err
	}
	var vals []byte
	for i := uint32(0); i < count; i++ {
		b, err := r.readByte()
		if err != nil {
			return nil, err
		}
		if err := checkValType(b); err != nil {
			return nil, err
		}
		vals = append(vals, b)
	}
	return vals, nil
}

// limitsRaw is a decoded limits pair before its flags byte has been validated by the
// caller: tables and memories interpret the same encoding with different accepted flag
// values and different cap-exceeded reasons.
type limitsRaw struct {
	flags  byte
	min    uint32
	max    uint32
	hasMax bool
}

func readLimitsRaw(r *reader) (limitsRaw, error) {
	flags, err := r.readByte()
	if err != nil {
		return limitsRaw{}, err
	}
	min, err := r.readVarU32()
	if err != nil {
		return limitsRaw{}, err
	}
	lr := limitsRaw{flags: flags, min: min}
	if flags&0x01 != 0 {
		max, err := r.readVarU32()
		if err != nil {
			return limitsRaw{}, err
		}
		lr.max = max
		lr.hasMax = true
	}
	return lr, nil
}

// readConstI32 reads a const-expression initializer and reports ok=false if it is not
// exactly the literal i32.const <vars32> end shape (any other opcode, or a mismatched end).
func readConstI32(r *reader) (v int32, ok bool, err error) {
	op, err := r.readByte()
	if err != nil {
		return 0, false, err
	}
	if op != 0x41 {
		return 0, false, nil
	}
	v, err = r.readVarS32()
	if err != nil {
		return 0, false, err
	}
	end, err := r.readByte()
	if err != nil {
		return 0, false, err
	}
	if end != 0x0B {
		return 0, false, nil
	}
	return v, true, nil
}

// readConstI64 is readConstI32's i64.const counterpart, used only for global initializers
// (element/data segment offsets are always i32).
func readConstI64(r *reader) (v int64, ok bool, err error) {
	op, err := r.readByte()
	if err != nil {
		return 0, false, err
	}
	if op != 0x42 {
		return 0, false, nil
	}
	v, err = r.readVarS64()
	if err != nil {
		return 0, false, err
	}
	end, err := r.readByte()
	if err != nil {
		return 0, false, err
	}
	if end != 0x0B {
		return 0, false, nil
	}
	return v, true, nil
}

// checkBlockType classifies a block/loop/if instruction's one-byte blocktype immediate.
func checkBlockType(b byte) error {
	switch b {
	case 0x40, 0x7F, 0x7E:
		return nil
	case 0x7D, 0x7C:
		return ErrFloatType
	case 0x7B:
		return ErrSIMD
	case 0x70, 0x6F:
		return ErrReferenceType
	}
	if b >= 0x40 && b < 0x80 {
		return ErrInvalidValType
	}
	return ErrBlockTypeIndex
}

// isFloatOpcode reports whether op is one of the f32.*/f64.* instructions rejected as
// ErrFloatInstruction.
func isFloatOpcode(op byte) bool {
	switch {
	case op == 0x2A || op == 0x2B || op == 0x38 || op == 0x39 || op == 0x43 || op == 0x44:
		return true
	case op >= 0x5B && op <= 0x66:
		return true
	case op >= 0x8B && op <= 0xA6:
		return true
	case op >= 0xA8 && op <= 0xAB:
		return true
	case op >= 0xAE && op <= 0xBF:
		return true
	}
	return false
}

// classifyOpcode reports the reason op (and, for 0xFC, its sub-opcode) is rejected, or nil
// if it is accepted. It runs before any other immediate is read: an opcode we do not accept
// has no known immediate shape, so it is rejected on the opcode byte (and, for the 0xFC
// prefix, the sub-opcode varu32) alone. The 0xFC sub-opcode's own decode errors (ErrTruncated,
// ErrNonCanonicalLEB128, ErrIntegerTooLarge) propagate from here before classification.
func classifyOpcode(r *reader, op byte) error {
	switch {
	case op >= 0x06 && op <= 0x08:
		return ErrExceptionHandling
	case op == 0x1C:
		return ErrTypedSelect
	case isFloatOpcode(op):
		return ErrFloatInstruction
	case op >= 0xC0 && op <= 0xC4:
		return ErrSignExtension
	case op >= 0xD0 && op <= 0xD2:
		return ErrReferenceType
	case op == 0xFC:
		sub, err := r.readVarU32()
		if err != nil {
			return err
		}
		if sub <= 7 {
			return ErrSaturatingConversion
		}
		return ErrBulkMemory
	case op == 0xFD:
		return ErrSIMD
	case op == 0xFE:
		return ErrAtomics
	}
	if _, ok := OpcodeCost(Opcode(op)); !ok {
		return ErrUnmeteredOpcode
	}
	return nil
}

// bodyCtx is the index-space bounds a function body's instructions are checked against.
// Bounds are taken from the original module's index spaces (required because the
// instrumentation pass appends a gas global and helper functions at the next free indices, so
// an unchecked out-of-range reference here would resolve onto the gas counter
// post-instrumentation).
type bodyCtx struct {
	numTypes    uint32
	numFuncs    uint32
	numTables   uint32
	numMemories uint32
	numGlobals  uint32
	numLocals   uint32
}

// validateBody decodes body one instruction at a time with a control stack seeded with one
// frame for the implicit function block: block/loop/if push, end pops, else is neutral. It
// requires the body to end with the end that pops the last frame exactly at the declared
// body length, else ErrMalformedFunctionBody.
func validateBody(body []byte, ctx bodyCtx) error {
	r := newReader(body)
	depth := 1
	for depth > 0 {
		if r.atEnd() {
			return ErrMalformedFunctionBody
		}
		op, err := r.readByte()
		if err != nil {
			return err
		}
		if err := classifyOpcode(r, op); err != nil {
			return fmt.Errorf("opcode 0x%02X: %w", op, err)
		}
		switch {
		case op == 0x02 || op == 0x03 || op == 0x04: // block, loop, if
			bt, err := r.readByte()
			if err != nil {
				return err
			}
			if err := checkBlockType(bt); err != nil {
				return fmt.Errorf("block type 0x%02X: %w", bt, err)
			}
			depth++
		case op == 0x05: // else: neutral
		case op == 0x0B: // end
			depth--
		case op == 0x0C || op == 0x0D: // br, br_if
			label, err := r.readVarU32()
			if err != nil {
				return err
			}
			if label >= uint32(depth) {
				return fmt.Errorf("label %d: %w", label, ErrLabelOutOfBounds)
			}
		case op == 0x0E: // br_table
			n, err := r.readVecCount()
			if err != nil {
				return err
			}
			for i := uint32(0); i < n; i++ {
				lbl, err := r.readVarU32()
				if err != nil {
					return err
				}
				if lbl >= uint32(depth) {
					return fmt.Errorf("br_table target %d: %w", lbl, ErrLabelOutOfBounds)
				}
			}
			def, err := r.readVarU32()
			if err != nil {
				return err
			}
			if def >= uint32(depth) {
				return fmt.Errorf("br_table default: %w", ErrLabelOutOfBounds)
			}
		case op == 0x10: // call
			fi, err := r.readVarU32()
			if err != nil {
				return err
			}
			if fi >= ctx.numFuncs {
				return fmt.Errorf("call funcidx %d: %w", fi, ErrFunctionIndexOutOfBounds)
			}
		case op == 0x11: // call_indirect
			ti, err := r.readVarU32()
			if err != nil {
				return err
			}
			if ti >= ctx.numTypes {
				return fmt.Errorf("call_indirect typeidx %d: %w", ti, ErrTypeIndexOutOfBounds)
			}
			tbi, err := r.readVarU32()
			if err != nil {
				return err
			}
			if tbi >= ctx.numTables {
				return fmt.Errorf("call_indirect tableidx %d: %w", tbi, ErrTableIndexOutOfBounds)
			}
		case op >= 0x20 && op <= 0x22: // local.get, local.set, local.tee
			li, err := r.readVarU32()
			if err != nil {
				return err
			}
			if li >= ctx.numLocals {
				return fmt.Errorf("local index %d: %w", li, ErrLocalIndexOutOfBounds)
			}
		case op == 0x23 || op == 0x24: // global.get, global.set
			gi, err := r.readVarU32()
			if err != nil {
				return err
			}
			if gi >= ctx.numGlobals {
				return fmt.Errorf("global index %d: %w", gi, ErrGlobalIndexOutOfBounds)
			}
		case op >= 0x28 && op <= 0x3E: // memarg loads/stores
			if _, err := r.readVarU32(); err != nil { // align
				return err
			}
			if _, err := r.readVarU32(); err != nil { // offset
				return err
			}
			if ctx.numMemories == 0 {
				return fmt.Errorf("memarg: %w", ErrMemoryIndexOutOfBounds)
			}
		case op == 0x3F || op == 0x40: // memory.size, memory.grow
			reserved, err := r.readByte()
			if err != nil {
				return err
			}
			if reserved != 0 || ctx.numMemories == 0 {
				return fmt.Errorf("memory index byte 0x%02X: %w", reserved, ErrMemoryIndexOutOfBounds)
			}
		case op == 0x41: // i32.const
			if _, err := r.readVarS32(); err != nil {
				return err
			}
		case op == 0x42: // i64.const
			if _, err := r.readVarS64(); err != nil {
				return err
			}
		default:
			// no immediate
		}
	}
	if r.remaining() != 0 {
		return ErrMalformedFunctionBody
	}
	return nil
}

// parseLocalsAndCheck reads a function body's vec((count:varu32, valtype)) locals
// declaration, summing params plus declared locals in uint64 and rejecting the moment the
// running total crosses lim.MaxLocalsPerFunction, so an absurd declared count cannot overflow
// or allocate. The cap is checked against paramCount alone before the locals vector is read,
// so a function with an empty (or absent) locals declaration still has its params compared
// against the cap.
func parseLocalsAndCheck(r *reader, paramCount uint32, maxLocals uint32) (uint32, error) {
	count, err := r.readVecCount()
	if err != nil {
		return 0, err
	}
	total := uint64(paramCount)
	if total > uint64(maxLocals) {
		return 0, ErrTooManyLocals
	}
	for i := uint32(0); i < count; i++ {
		n, err := r.readVarU32()
		if err != nil {
			return 0, err
		}
		vt, err := r.readByte()
		if err != nil {
			return 0, err
		}
		if err := checkValType(vt); err != nil {
			return 0, err
		}
		total += uint64(n)
		if total > uint64(maxLocals) {
			return 0, ErrTooManyLocals
		}
	}
	return uint32(total), nil
}
