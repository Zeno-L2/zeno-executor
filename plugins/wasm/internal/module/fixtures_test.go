package module

import (
	"bytes"
	"fmt"
)

// The module builder used by every fixture in validate_test.go (and, per the ticket, meant to
// be extended by later tickets rather than replaced). Three mechanisms:
//
//   - (S) Structural fields: newBuilder()'s minimal conforming module, mutated one field at a
//     time. Every count and length in the output is computed by bytes(); every byte with
//     structural meaning is a raw field.
//   - (R) Raw sections and order override: raws entries are emitted at their id's position
//     (replace: true suppresses the generated section of that id, replace: false emits an
//     extra section); order lists non-custom ids in emission order (nil = ascending).
//   - (P) Post-assembly byte patches: patchAt (an absolute offset, preamble only), patchFirst
//     (the sole occurrence of a byte sequence) and appendRaw (trailing bytes), applied by
//     bytes() in patches-slice order after assembly.
type funcType struct {
	form            byte
	params, results []byte
}

type limits struct {
	flags    byte
	min, max uint32 // max emitted only when flags&1 != 0
}

type importEntry struct {
	module, name string
	desc         byte
	typeIdx      uint32
}

type tableEntry struct {
	elemType byte
	lim      limits
}

type globalEntry struct {
	valType, mut byte
	init         []byte // raw const-expr bytes
}

type exportEntry struct {
	name  string
	kind  byte
	index uint32
}

type elemSeg struct {
	flags  byte
	offset []byte
	funcs  []uint32
}

type dataSeg struct {
	flags  byte
	offset []byte
	bytes  []byte
}

type localDecl struct {
	count   uint32
	valType byte
}

type body struct {
	locals []localDecl
	code   []byte
}

// customSection is emitted right after the real section named afterID has been emitted;
// afterID 0 means before every known section, 0xFF means after all of them.
type customSection struct {
	name    string
	payload []byte
	afterID byte
}

// rawSection is emitted at id's position in the emission order.
type rawSection struct {
	id      byte
	payload []byte
	replace bool
}

// patch is one post-assembly mutation: patchAt uses off, patchFirst uses old/new, appendRaw
// uses only new with off left at its zero value's sentinel (see bytes()).
type patch struct {
	off      int
	old, new []byte
}

const appendSentinel = -1

type builder struct {
	types    []funcType
	imports  []importEntry
	funcs    []uint32
	tables   []tableEntry
	memories []limits
	globals  []globalEntry
	exports  []exportEntry
	start    *uint32
	elements []elemSeg
	data     []dataSeg
	bodies   []body
	customs  []customSection
	raws     []rawSection
	order    []byte // emission order of non-custom ids; nil = ascending
	patches  []patch
}

// newBuilder returns the minimal conforming module: one type (i32,i32) -> i64, one function
// using it, one memory {min 1}, exports "memory" and "run", and one body (i64.const 0; end).
func newBuilder() *builder {
	return &builder{
		types:    []funcType{{form: 0x60, params: []byte{0x7F, 0x7F}, results: []byte{0x7E}}},
		funcs:    []uint32{0},
		memories: []limits{{flags: 0x00, min: 1}},
		exports: []exportEntry{
			{name: "memory", kind: 0x02, index: 0},
			{name: "run", kind: 0x00, index: 0},
		},
		bodies: []body{{code: []byte{0x42, 0x00, 0x0B}}}, // i64.const 0; end
	}
}

// patchAt overwrites len(new) bytes at the absolute offset off (preamble only).
func (b *builder) patchAt(off int, new ...byte) *builder {
	b.patches = append(b.patches, patch{off: off, new: new})
	return b
}

// patchFirst replaces the sole occurrence of old with new; bytes() panics if old does not
// occur exactly once in the assembled module.
func (b *builder) patchFirst(old, new []byte) *builder {
	b.patches = append(b.patches, patch{off: appendSentinel, old: old, new: new})
	return b
}

// appendRaw appends raw trailing bytes after the whole assembled module.
func (b *builder) appendRaw(bs ...byte) *builder {
	b.patches = append(b.patches, patch{off: appendSentinel, new: bs})
	return b
}

func appendName(dst []byte, s string) []byte {
	dst = appendVarU32(dst, uint32(len(s)))
	return append(dst, s...)
}

func appendLimitsBytes(dst []byte, l limits) []byte {
	dst = append(dst, l.flags)
	dst = appendVarU32(dst, l.min)
	if l.flags&0x01 != 0 {
		dst = appendVarU32(dst, l.max)
	}
	return dst
}

// sectionBody returns the encoded payload for a known section id and whether it should be
// emitted at all (an empty vector omits its section entirely, matching a real toolchain).
func (b *builder) sectionBody(id byte) ([]byte, bool) {
	switch id {
	case 1:
		if len(b.types) == 0 {
			return nil, false
		}
		buf := appendVarU32(nil, uint32(len(b.types)))
		for _, t := range b.types {
			buf = append(buf, t.form)
			buf = appendVarU32(buf, uint32(len(t.params)))
			buf = append(buf, t.params...)
			buf = appendVarU32(buf, uint32(len(t.results)))
			buf = append(buf, t.results...)
		}
		return buf, true
	case 2:
		if len(b.imports) == 0 {
			return nil, false
		}
		buf := appendVarU32(nil, uint32(len(b.imports)))
		for _, im := range b.imports {
			buf = appendName(buf, im.module)
			buf = appendName(buf, im.name)
			buf = append(buf, im.desc)
			buf = appendVarU32(buf, im.typeIdx)
		}
		return buf, true
	case 3:
		if len(b.funcs) == 0 {
			return nil, false
		}
		buf := appendVarU32(nil, uint32(len(b.funcs)))
		for _, f := range b.funcs {
			buf = appendVarU32(buf, f)
		}
		return buf, true
	case 4:
		if len(b.tables) == 0 {
			return nil, false
		}
		buf := appendVarU32(nil, uint32(len(b.tables)))
		for _, t := range b.tables {
			buf = append(buf, t.elemType)
			buf = appendLimitsBytes(buf, t.lim)
		}
		return buf, true
	case 5:
		if len(b.memories) == 0 {
			return nil, false
		}
		buf := appendVarU32(nil, uint32(len(b.memories)))
		for _, mlim := range b.memories {
			buf = appendLimitsBytes(buf, mlim)
		}
		return buf, true
	case 6:
		if len(b.globals) == 0 {
			return nil, false
		}
		buf := appendVarU32(nil, uint32(len(b.globals)))
		for _, g := range b.globals {
			buf = append(buf, g.valType, g.mut)
			buf = append(buf, g.init...)
		}
		return buf, true
	case 7:
		if len(b.exports) == 0 {
			return nil, false
		}
		buf := appendVarU32(nil, uint32(len(b.exports)))
		for _, e := range b.exports {
			buf = appendName(buf, e.name)
			buf = append(buf, e.kind)
			buf = appendVarU32(buf, e.index)
		}
		return buf, true
	case 8:
		if b.start == nil {
			return nil, false
		}
		return appendVarU32(nil, *b.start), true
	case 9:
		if len(b.elements) == 0 {
			return nil, false
		}
		buf := appendVarU32(nil, uint32(len(b.elements)))
		for _, el := range b.elements {
			buf = append(buf, el.flags)
			buf = append(buf, el.offset...)
			buf = appendVarU32(buf, uint32(len(el.funcs)))
			for _, fi := range el.funcs {
				buf = appendVarU32(buf, fi)
			}
		}
		return buf, true
	case 10:
		if len(b.bodies) == 0 {
			return nil, false
		}
		buf := appendVarU32(nil, uint32(len(b.bodies)))
		for _, bd := range b.bodies {
			bodyBuf := appendVarU32(nil, uint32(len(bd.locals)))
			for _, ld := range bd.locals {
				bodyBuf = appendVarU32(bodyBuf, ld.count)
				bodyBuf = append(bodyBuf, ld.valType)
			}
			bodyBuf = append(bodyBuf, bd.code...)
			buf = appendVarU32(buf, uint32(len(bodyBuf)))
			buf = append(buf, bodyBuf...)
		}
		return buf, true
	case 11:
		if len(b.data) == 0 {
			return nil, false
		}
		buf := appendVarU32(nil, uint32(len(b.data)))
		for _, d := range b.data {
			buf = append(buf, d.flags)
			buf = append(buf, d.offset...)
			buf = appendVarU32(buf, uint32(len(d.bytes)))
			buf = append(buf, d.bytes...)
		}
		return buf, true
	}
	return nil, false
}

// bytes assembles the module: preamble, then each id in emission order (structural body,
// any raw sections at that id, then any customs pinned to that id), then patches in slice
// order.
func (b *builder) bytes() []byte {
	out := []byte{0x00, 'a', 's', 'm', 0x01, 0x00, 0x00, 0x00}

	order := b.order
	if order == nil {
		order = []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11}
	}
	seen := make(map[byte]bool, len(order))
	effectiveOrder := append([]byte{}, order...)
	for _, id := range effectiveOrder {
		seen[id] = true
	}
	for _, raw := range b.raws {
		if !seen[raw.id] {
			effectiveOrder = append(effectiveOrder, raw.id)
			seen[raw.id] = true
		}
	}

	emitCustoms := func(afterID byte) {
		for _, c := range b.customs {
			if c.afterID != afterID {
				continue
			}
			payload := appendName(nil, c.name)
			payload = append(payload, c.payload...)
			out = append(out, 0x00)
			out = appendVarU32(out, uint32(len(payload)))
			out = append(out, payload...)
		}
	}

	emitCustoms(0)
	for _, id := range effectiveOrder {
		body, present := b.sectionBody(id)
		for _, raw := range b.raws {
			if raw.id == id && raw.replace {
				body, present = raw.payload, true
			}
		}
		if present {
			out = append(out, id)
			out = appendVarU32(out, uint32(len(body)))
			out = append(out, body...)
		}
		for _, raw := range b.raws {
			if raw.id == id && !raw.replace {
				out = append(out, id)
				out = appendVarU32(out, uint32(len(raw.payload)))
				out = append(out, raw.payload...)
			}
		}
		emitCustoms(id)
	}
	emitCustoms(0xFF)

	for _, p := range b.patches {
		switch {
		case len(p.old) > 0:
			if n := bytes.Count(out, p.old); n != 1 {
				panic(fmt.Sprintf("patchFirst: %x occurs %d times, want exactly 1", p.old, n))
			}
			idx := bytes.Index(out, p.old)
			next := make([]byte, 0, len(out)-len(p.old)+len(p.new))
			next = append(next, out[:idx]...)
			next = append(next, p.new...)
			next = append(next, out[idx+len(p.old):]...)
			out = next
		case p.off == appendSentinel:
			out = append(out, p.new...)
		default:
			copy(out[p.off:p.off+len(p.new)], p.new)
		}
	}

	return out
}
