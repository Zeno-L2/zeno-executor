package module

import (
	"errors"
	"math"
	"testing"
)

func TestVarU32RoundTrip(t *testing.T) {
	values := []uint32{0, 63, 64, 127, 128, math.MaxUint32}
	for _, v := range values {
		enc := appendVarU32(nil, v)
		got, n, err := readVarU32(enc, 0)
		if err != nil {
			t.Fatalf("readVarU32(appendVarU32(%d)): %v", v, err)
		}
		if got != v || n != len(enc) {
			t.Errorf("readVarU32(appendVarU32(%d)) = (%d, %d), want (%d, %d)", v, got, n, v, len(enc))
		}
	}
}

func TestVarS32RoundTrip(t *testing.T) {
	values := []int32{0, 63, 64, 127, 128, -1, math.MinInt32, math.MaxInt32}
	for _, v := range values {
		enc := appendVarS32(nil, v)
		got, n, err := readVarS32(enc, 0)
		if err != nil {
			t.Fatalf("readVarS32(appendVarS32(%d)): %v", v, err)
		}
		if got != v || n != len(enc) {
			t.Errorf("readVarS32(appendVarS32(%d)) = (%d, %d), want (%d, %d)", v, got, n, v, len(enc))
		}
	}
}

func TestVarS64RoundTrip(t *testing.T) {
	values := []int64{0, 63, 64, 127, 128, -1, math.MinInt32, math.MaxInt32, math.MinInt64, math.MaxInt64}
	for _, v := range values {
		enc := appendVarS64(nil, v)
		got, n, err := readVarS64(enc, 0)
		if err != nil {
			t.Fatalf("readVarS64(appendVarS64(%d)): %v", v, err)
		}
		if got != v || n != len(enc) {
			t.Errorf("readVarS64(appendVarS64(%d)) = (%d, %d), want (%d, %d)", v, got, n, v, len(enc))
		}
	}
}

func TestEncodersEmitShortestForm(t *testing.T) {
	if got := appendVarU32(nil, 0); len(got) != 1 {
		t.Errorf("appendVarU32(0) = %d bytes, want 1", len(got))
	}
	if got := appendVarU32(nil, 127); len(got) != 1 {
		t.Errorf("appendVarU32(127) = %d bytes, want 1", len(got))
	}
	if got := appendVarU32(nil, 128); len(got) != 2 {
		t.Errorf("appendVarU32(128) = %d bytes, want 2", len(got))
	}
	if got := appendVarS32(nil, 0); len(got) != 1 {
		t.Errorf("appendVarS32(0) = %d bytes, want 1", len(got))
	}
	if got := appendVarS32(nil, -1); len(got) != 1 {
		t.Errorf("appendVarS32(-1) = %d bytes, want 1", len(got))
	}
	if got := appendVarS64(nil, 0); len(got) != 1 {
		t.Errorf("appendVarS64(0) = %d bytes, want 1", len(got))
	}
}

func TestReadVarU32Truncated(t *testing.T) {
	_, _, err := readVarU32([]byte{0x80}, 0)
	if !errors.Is(err, ErrTruncated) {
		t.Errorf("readVarU32([0x80]) = %v, want ErrTruncated", err)
	}
}

func TestReadVarU32IntegerTooLarge(t *testing.T) {
	// six continuation bytes: more than a varu32 allows.
	_, _, err := readVarU32([]byte{0x80, 0x80, 0x80, 0x80, 0x80, 0x01}, 0)
	if !errors.Is(err, ErrIntegerTooLarge) {
		t.Errorf("readVarU32(6 bytes) = %v, want ErrIntegerTooLarge", err)
	}
}

func TestReadVarU32NonCanonical(t *testing.T) {
	// 0 padded to two bytes instead of the canonical single 0x00.
	_, _, err := readVarU32([]byte{0x80, 0x00}, 0)
	if !errors.Is(err, ErrNonCanonicalLEB128) {
		t.Errorf("readVarU32([0x80,0x00]) = %v, want ErrNonCanonicalLEB128", err)
	}
}

func TestReadVarS32Truncated(t *testing.T) {
	_, _, err := readVarS32([]byte{0x80}, 0)
	if !errors.Is(err, ErrTruncated) {
		t.Errorf("readVarS32([0x80]) = %v, want ErrTruncated", err)
	}
}

func TestReadVarS64Truncated(t *testing.T) {
	_, _, err := readVarS64([]byte{0x80}, 0)
	if !errors.Is(err, ErrTruncated) {
		t.Errorf("readVarS64([0x80]) = %v, want ErrTruncated", err)
	}
}

func TestReadVarS64NonCanonical(t *testing.T) {
	// -1 padded to two bytes instead of the canonical single 0x7F.
	_, _, err := readVarS64([]byte{0xFF, 0x7F}, 0)
	if !errors.Is(err, ErrNonCanonicalLEB128) {
		t.Errorf("readVarS64([0xFF,0x7F]) = %v, want ErrNonCanonicalLEB128", err)
	}
}

func TestReadVarS64IntegerTooLargeFinalByte(t *testing.T) {
	// nine 0x80 continuation bytes then a final byte whose non-sign-consistent payload
	// (0x02) cannot be represented in the remaining single bit.
	enc := []byte{0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x02}
	_, _, err := readVarS64(enc, 0)
	if !errors.Is(err, ErrIntegerTooLarge) {
		t.Errorf("readVarS64(bad final byte) = %v, want ErrIntegerTooLarge", err)
	}
}
