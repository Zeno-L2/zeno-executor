package codec

import (
	"errors"
	"math/big"
	"testing"
)

func TestU256FromBig(t *testing.T) {
	t.Run("zero", func(t *testing.T) {
		got, err := U256FromBig(big.NewInt(0))
		if err != nil {
			t.Fatalf("U256FromBig(0): %v", err)
		}
		if got != (U256{}) {
			t.Errorf("got %x, want zero", got)
		}
	})

	t.Run("one", func(t *testing.T) {
		got, err := U256FromBig(big.NewInt(1))
		if err != nil {
			t.Fatalf("U256FromBig(1): %v", err)
		}
		want := U256{}
		want[31] = 1
		if got != want {
			t.Errorf("got %x, want %x", got, want)
		}
	})

	t.Run("max", func(t *testing.T) {
		max := new(big.Int).Sub(new(big.Int).Lsh(big.NewInt(1), 256), big.NewInt(1))
		got, err := U256FromBig(max)
		if err != nil {
			t.Fatalf("U256FromBig(max): %v", err)
		}
		for _, b := range got {
			if b != 0xFF {
				t.Fatalf("got %x, want all-0xFF", got)
			}
		}
	})

	t.Run("negative rejected", func(t *testing.T) {
		if _, err := U256FromBig(big.NewInt(-1)); err == nil {
			t.Fatalf("expected error for negative value")
		}
	})

	t.Run("too large rejected", func(t *testing.T) {
		tooBig := new(big.Int).Lsh(big.NewInt(1), 256)
		if _, err := U256FromBig(tooBig); err == nil {
			t.Fatalf("expected error for value >= 2^256")
		}
	})
}

func TestReaderBytes(t *testing.T) {
	t.Run("zero length decodes non-nil", func(t *testing.T) {
		r := newReader([]byte{0, 0, 0, 0})
		got, err := r.bytes(^uint32(0))
		if err != nil {
			t.Fatalf("bytes: %v", err)
		}
		if got == nil {
			t.Fatalf("got nil, want non-nil empty slice")
		}
		if len(got) != 0 {
			t.Fatalf("got len %d, want 0", len(got))
		}
	})

	t.Run("truncated length prefix", func(t *testing.T) {
		r := newReader([]byte{0, 0})
		if _, err := r.bytes(^uint32(0)); !errors.Is(err, ErrTruncated) {
			t.Fatalf("got %v, want ErrTruncated", err)
		}
	})

	t.Run("exceeds max", func(t *testing.T) {
		r := newReader([]byte{0, 0, 0, 5, 1, 2, 3, 4, 5})
		if _, err := r.bytes(4); !errors.Is(err, ErrLimitExceeded) {
			t.Fatalf("got %v, want ErrLimitExceeded", err)
		}
	})

	t.Run("exceeds remaining", func(t *testing.T) {
		r := newReader([]byte{0, 0, 0, 5, 1, 2})
		if _, err := r.bytes(^uint32(0)); !errors.Is(err, ErrOverflow) {
			t.Fatalf("got %v, want ErrOverflow", err)
		}
	})

	t.Run("copy, not alias", func(t *testing.T) {
		buf := []byte{0, 0, 0, 1, 0xAA}
		r := newReader(buf)
		got, err := r.bytes(^uint32(0))
		if err != nil {
			t.Fatalf("bytes: %v", err)
		}
		got[0] = 0xBB
		if buf[4] != 0xAA {
			t.Fatalf("mutating returned slice mutated the input buffer")
		}
	})
}

func TestReaderCount(t *testing.T) {
	t.Run("ordinary", func(t *testing.T) {
		r := newReader([]byte{0, 0, 0, 2, 1, 2, 3, 4})
		c, err := r.count(2)
		if err != nil {
			t.Fatalf("count: %v", err)
		}
		if c != 2 {
			t.Fatalf("got %d, want 2", c)
		}
	})

	t.Run("overflow via too few remaining bytes", func(t *testing.T) {
		r := newReader([]byte{0, 0, 0, 5, 1, 2, 3})
		if _, err := r.count(4); !errors.Is(err, ErrOverflow) {
			t.Fatalf("got %v, want ErrOverflow", err)
		}
	})

	t.Run("32-bit product overflow does not wrap", func(t *testing.T) {
		// count * elemMin overflows a 32-bit int (0xFFFFFFFE * 2 wraps to a small number in
		// 32-bit arithmetic) but MUST still be rejected once computed in 64-bit arithmetic.
		r := newReader([]byte{0xFF, 0xFF, 0xFF, 0xFE, 1, 2, 3, 4})
		if _, err := r.count(2); !errors.Is(err, ErrOverflow) {
			t.Fatalf("got %v, want ErrOverflow", err)
		}
	})
}

func TestReaderEnd(t *testing.T) {
	r := newReader([]byte{1, 2, 3})
	if _, err := r.u8(); err != nil {
		t.Fatalf("u8: %v", err)
	}
	if err := r.end(); err == nil {
		t.Fatalf("expected ErrTrailingBytes")
	} else if !errors.Is(err, ErrTrailingBytes) {
		t.Fatalf("got %v, want ErrTrailingBytes", err)
	}

	r2 := newReader([]byte{1})
	if _, err := r2.u8(); err != nil {
		t.Fatalf("u8: %v", err)
	}
	if err := r2.end(); err != nil {
		t.Fatalf("end: %v, want nil", err)
	}
}

func TestWriterReaderRoundTrip(t *testing.T) {
	w := &writer{}
	w.u8(0x11)
	w.u16(0x2233)
	w.u32(0x44556677)
	w.u64(0x8899AABBCCDDEEFF)
	w.raw([]byte{1, 2, 3})

	r := newReader(w.buf)
	if v, err := r.u8(); err != nil || v != 0x11 {
		t.Fatalf("u8 = %v, %v", v, err)
	}
	if v, err := r.u16(); err != nil || v != 0x2233 {
		t.Fatalf("u16 = %v, %v", v, err)
	}
	if v, err := r.u32(); err != nil || v != 0x44556677 {
		t.Fatalf("u32 = %v, %v", v, err)
	}
	if v, err := r.u64(); err != nil || v != 0x8899AABBCCDDEEFF {
		t.Fatalf("u64 = %v, %v", v, err)
	}
	if b, err := r.fixed(3); err != nil || b[0] != 1 || b[1] != 2 || b[2] != 3 {
		t.Fatalf("fixed(3) = %v, %v", b, err)
	}
	if err := r.end(); err != nil {
		t.Fatalf("end: %v", err)
	}
}
