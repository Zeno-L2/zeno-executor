package module

import "math"

// This file implements canonical LEB128 decode and encode for the six shapes the binary
// format needs: every count, length and index is a varu32, and the only signed immediates
// are i32.const (vars32) and i64.const (vars64). Core 1.0 permits padded encodings up to the
// width limit; this package rejects any non-minimal encoding for every one of them (see
// validate.go's package doc comment for the consensus rationale).

// readVarU32 decodes an unsigned LEB128 value at b[pos:], returning the value, the number of
// bytes consumed, and an error. It rejects running off the end (ErrTruncated), an encoding
// using more bytes than a 32-bit value allows or with unused high bits set in the final byte
// (ErrIntegerTooLarge), and any non-minimal encoding (ErrNonCanonicalLEB128).
func readVarU32(b []byte, pos int) (uint32, int, error) {
	var result uint64
	n := 0
	for shift := uint(0); ; shift += 7 {
		if shift >= 35 {
			return 0, 0, ErrIntegerTooLarge
		}
		if pos+n >= len(b) {
			return 0, 0, ErrTruncated
		}
		byt := b[pos+n]
		n++
		result |= uint64(byt&0x7F) << shift
		if byt&0x80 == 0 {
			break
		}
	}
	if result > math.MaxUint32 {
		return 0, 0, ErrIntegerTooLarge
	}
	v := uint32(result)
	if enc := appendVarU32(nil, v); len(enc) != n {
		return 0, 0, ErrNonCanonicalLEB128
	}
	return v, n, nil
}

// readVarS32 decodes a signed LEB128 value at b[pos:] as an i32.const immediate, with the
// same rejections as readVarU32 (ErrTruncated, ErrIntegerTooLarge, ErrNonCanonicalLEB128).
func readVarS32(b []byte, pos int) (int32, int, error) {
	var result int64
	n := 0
	var shift uint
	var last byte
	for {
		if shift >= 35 {
			return 0, 0, ErrIntegerTooLarge
		}
		if pos+n >= len(b) {
			return 0, 0, ErrTruncated
		}
		last = b[pos+n]
		n++
		result |= int64(last&0x7F) << shift
		shift += 7
		if last&0x80 == 0 {
			break
		}
	}
	if shift < 64 && last&0x40 != 0 {
		result |= -1 << shift
	}
	if result < math.MinInt32 || result > math.MaxInt32 {
		return 0, 0, ErrIntegerTooLarge
	}
	v := int32(result)
	if enc := appendVarS32(nil, v); len(enc) != n {
		return 0, 0, ErrNonCanonicalLEB128
	}
	return v, n, nil
}

// readVarS64 decodes a signed LEB128 value at b[pos:] as an i64.const immediate, with the
// same rejections as readVarU32. The final (tenth) byte of a maximal-width encoding carries
// only one meaningful value bit; its other six payload bits must equal that bit's replication
// (0x00 or 0x7F) or the encoding is rejected as ErrIntegerTooLarge before it could otherwise
// silently overflow past the 64-bit accumulator.
func readVarS64(b []byte, pos int) (int64, int, error) {
	var result int64
	n := 0
	var shift uint
	var last byte
	for {
		if shift >= 70 {
			return 0, 0, ErrIntegerTooLarge
		}
		if pos+n >= len(b) {
			return 0, 0, ErrTruncated
		}
		last = b[pos+n]
		n++
		if shift == 63 {
			payload := last & 0x7F
			if payload != 0x00 && payload != 0x7F {
				return 0, 0, ErrIntegerTooLarge
			}
			result |= int64(payload&0x01) << 63
		} else {
			result |= int64(last&0x7F) << shift
		}
		shift += 7
		if last&0x80 == 0 {
			break
		}
	}
	if shift < 64 && last&0x40 != 0 {
		result |= -1 << shift
	}
	v := result
	if enc := appendVarS64(nil, v); len(enc) != n {
		return 0, 0, ErrNonCanonicalLEB128
	}
	return v, n, nil
}

// appendVarU32 appends the shortest unsigned LEB128 encoding of v to dst.
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

// appendVarS32 appends the shortest signed LEB128 encoding of v to dst.
func appendVarS32(dst []byte, v int32) []byte {
	return appendVarS64(dst, int64(v))
}

// appendVarS64 appends the shortest signed LEB128 encoding of v to dst.
func appendVarS64(dst []byte, v int64) []byte {
	for {
		b := byte(v & 0x7F)
		v >>= 7
		signBitSet := b&0x40 != 0
		if (v == 0 && !signBitSet) || (v == -1 && signBitSet) {
			return append(dst, b)
		}
		dst = append(dst, b|0x80)
	}
}
