package codec

import (
	"bytes"
	"errors"
	"reflect"
	"testing"
)

// sampleSidecarReceipts returns two receipts at InputIndex 3 and 4, the second a revert.
func sampleSidecarReceipts() []Receipt {
	a := sampleReceipt()
	b := sampleReceipt()
	b.InputIndex = 4
	b.Status = StatusRevert
	return []Receipt{a, b}
}

func TestReceiptSidecarRoundTrip(t *testing.T) {
	receipts := sampleSidecarReceipts()
	enc, err := EncodeReceiptSidecar(receipts)
	if err != nil {
		t.Fatalf("EncodeReceiptSidecar: %v", err)
	}
	got, leaves, err := DecodeReceiptSidecar(enc)
	if err != nil {
		t.Fatalf("DecodeReceiptSidecar: %v", err)
	}
	if !reflect.DeepEqual(got, receipts) {
		t.Fatalf("receipts = %+v, want %+v", got, receipts)
	}
	if len(leaves) != len(receipts) {
		t.Fatalf("leaves = %d, want %d", len(leaves), len(receipts))
	}
	for i, rec := range receipts {
		want, err := rec.Encode()
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(leaves[i], want) {
			t.Fatalf("leaf %d = %x, want %x", i, leaves[i], want)
		}
	}

	empty, err := EncodeReceiptSidecar(nil)
	if err != nil {
		t.Fatalf("encode nil: %v", err)
	}
	if !bytes.Equal(empty, []byte{0, 0, 0, 0}) {
		t.Fatalf("nil sidecar = %x, want 00000000", empty)
	}
	got, leaves, err = DecodeReceiptSidecar(empty)
	if err != nil || len(got) != 0 || len(leaves) != 0 {
		t.Fatalf("decode empty = %d receipts, %d leaves, %v", len(got), len(leaves), err)
	}
}

func TestReceiptSidecarRejects(t *testing.T) {
	enc, err := EncodeReceiptSidecar(sampleSidecarReceipts())
	if err != nil {
		t.Fatal(err)
	}
	oneEnc, err := sampleReceipt().Encode()
	if err != nil {
		t.Fatal(err)
	}

	badVersion := append([]byte(nil), oneEnc...)
	badVersion[0]++ // the first field is the version
	paddedLeaf := &writer{}
	paddedLeaf.u32(1)
	paddedLeaf.bytes(append(append([]byte(nil), oneEnc...), 0))
	unknownVersion := &writer{}
	unknownVersion.u32(1)
	unknownVersion.bytes(badVersion)

	hugeCount := append([]byte(nil), enc...)
	copy(hugeCount, []byte{0xFF, 0xFF, 0xFF, 0xFF})

	cases := []struct {
		name string
		in   []byte
		want error
	}{
		{"trailing byte", append(append([]byte(nil), enc...), 0), ErrTrailingBytes},
		{"truncated", enc[:len(enc)-1], ErrOverflow},
		{"huge count", hugeCount, ErrOverflow},
		{"unknown leaf version", unknownVersion.buf, ErrUnknownVersion},
		{"leaf longer than a receipt", paddedLeaf.buf, ErrLimitExceeded},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, _, err := DecodeReceiptSidecar(tc.in)
			if !errors.Is(err, tc.want) {
				t.Fatalf("err = %v, want %v", err, tc.want)
			}
		})
	}
}

func TestReceiptSidecarOrder(t *testing.T) {
	mk := func(first, second uint64) []Receipt {
		a, b := sampleReceipt(), sampleReceipt()
		a.InputIndex, b.InputIndex = first, second
		return []Receipt{a, b}
	}
	handBuilt := func(rs []Receipt) []byte {
		w := &writer{}
		w.u32(uint32(len(rs)))
		for _, r := range rs {
			e, err := r.Encode()
			if err != nil {
				t.Fatal(err)
			}
			w.bytes(e)
		}
		return w.buf
	}
	cases := []struct {
		name string
		rs   []Receipt
		want error
	}{
		{"duplicate", mk(5, 5), ErrDuplicateKey},
		{"descending", mk(6, 5), ErrNotSorted},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := EncodeReceiptSidecar(tc.rs); !errors.Is(err, tc.want) {
				t.Fatalf("encode err = %v, want %v", err, tc.want)
			}
			if _, _, err := DecodeReceiptSidecar(handBuilt(tc.rs)); !errors.Is(err, tc.want) {
				t.Fatalf("decode err = %v, want %v", err, tc.want)
			}
		})
	}

	bad := sampleReceipt()
	bad.Version = ReceiptVersion + 1
	if _, err := EncodeReceiptSidecar([]Receipt{bad}); !errors.Is(err, ErrUnknownVersion) {
		t.Fatalf("encode wrong version err = %v, want ErrUnknownVersion", err)
	}
}

func TestReceiptEncodedSize(t *testing.T) {
	enc, err := sampleReceipt().Encode()
	if err != nil {
		t.Fatal(err)
	}
	if len(enc) != receiptEncodedSize {
		t.Fatalf("encoded receipt is %d bytes, receiptEncodedSize is %d", len(enc), receiptEncodedSize)
	}
}

// A count that the input could hold at 4 bytes per element but not at the
// size of a whole receipt is rejected before any list is allocated.
func TestReceiptSidecarCountBoundsByReceiptSize(t *testing.T) {
	w := &writer{}
	w.u32(1000)
	w.buf = append(w.buf, make([]byte, 4*1000)...)
	if _, _, err := DecodeReceiptSidecar(w.buf); !errors.Is(err, ErrOverflow) {
		t.Fatalf("err = %v, want ErrOverflow", err)
	}
}
