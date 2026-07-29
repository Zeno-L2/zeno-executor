package codec

import (
	"encoding/binary"
	"fmt"
	"math/big"

	"github.com/zenon-network/go-zenon/common/trie"
	"github.com/zenon-network/go-zenon/common/types"
)

// U256 is a fixed 32-byte big-endian unsigned integer (§27.0). It is a fixed-size array,
// never a *big.Int, precisely so the codec cannot range-check or normalize it — a u256 is
// never range-checked at this layer (§27.0).
type U256 [32]byte

// U256FromBig converts v into its 32-byte big-endian encoding, erroring on a negative value
// or a value that does not fit in 256 bits.
func U256FromBig(v *big.Int) (U256, error) {
	var out U256
	if v.Sign() < 0 {
		return out, fmt.Errorf("codec: u256 from big: negative value")
	}
	if v.BitLen() > 256 {
		return out, fmt.Errorf("codec: u256 from big: value exceeds 256 bits")
	}
	b := v.Bytes()
	copy(out[32-len(b):], b)
	return out, nil
}

// writer is an append-only canonical-encoding buffer over the §27.0 primitives.
type writer struct {
	buf []byte
}

func (w *writer) u8(v uint8)   { w.buf = append(w.buf, v) }
func (w *writer) u16(v uint16) { w.buf = binary.BigEndian.AppendUint16(w.buf, v) }
func (w *writer) u32(v uint32) { w.buf = binary.BigEndian.AppendUint32(w.buf, v) }
func (w *writer) u64(v uint64) { w.buf = binary.BigEndian.AppendUint64(w.buf, v) }

func (w *writer) u256(v U256)             { w.buf = append(w.buf, v[:]...) }
func (w *writer) hash(v types.Hash)       { w.buf = append(w.buf, v[:]...) }
func (w *writer) path(v trie.Path)        { w.buf = append(w.buf, v[:]...) }
func (w *writer) address(v types.Address) { w.buf = append(w.buf, v[:]...) }

// bytes writes the Bytes encoding: a u32 length prefix followed by the raw bytes.
func (w *writer) bytes(b []byte) {
	w.u32(uint32(len(b)))
	w.buf = append(w.buf, b...)
}

// raw appends b with no length prefix.
func (w *writer) raw(b []byte) { w.buf = append(w.buf, b...) }

// reader consumes a canonical encoding from a fixed byte slice, tracking position.
type reader struct {
	buf []byte
	pos int
}

func newReader(b []byte) *reader { return &reader{buf: b} }

// remaining is the number of unconsumed bytes.
func (r *reader) remaining() int { return len(r.buf) - r.pos }

// fixed consumes and returns the next n bytes, or ErrTruncated if fewer remain.
func (r *reader) fixed(n int) ([]byte, error) {
	if r.remaining() < n {
		return nil, ErrTruncated
	}
	out := r.buf[r.pos : r.pos+n]
	r.pos += n
	return out, nil
}

func (r *reader) u8() (uint8, error) {
	b, err := r.fixed(1)
	if err != nil {
		return 0, err
	}
	return b[0], nil
}

func (r *reader) u16() (uint16, error) {
	b, err := r.fixed(2)
	if err != nil {
		return 0, err
	}
	return binary.BigEndian.Uint16(b), nil
}

func (r *reader) u32() (uint32, error) {
	b, err := r.fixed(4)
	if err != nil {
		return 0, err
	}
	return binary.BigEndian.Uint32(b), nil
}

func (r *reader) u64() (uint64, error) {
	b, err := r.fixed(8)
	if err != nil {
		return 0, err
	}
	return binary.BigEndian.Uint64(b), nil
}

func (r *reader) u256() (U256, error) {
	var out U256
	b, err := r.fixed(32)
	if err != nil {
		return out, err
	}
	copy(out[:], b)
	return out, nil
}

func (r *reader) hash() (types.Hash, error) {
	var out types.Hash
	b, err := r.fixed(32)
	if err != nil {
		return out, err
	}
	copy(out[:], b)
	return out, nil
}

func (r *reader) path() (trie.Path, error) {
	var out trie.Path
	b, err := r.fixed(32)
	if err != nil {
		return out, err
	}
	copy(out[:], b)
	return out, nil
}

func (r *reader) address() (types.Address, error) {
	var out types.Address
	b, err := r.fixed(20)
	if err != nil {
		return out, err
	}
	copy(out[:], b)
	return out, nil
}

// bytes reads the Bytes encoding: a u32 length L followed by L raw bytes. It rejects L > max
// with ErrLimitExceeded and L greater than the remaining buffer with ErrOverflow. It returns
// a non-nil, zero-length slice when L == 0, and a copy (never an alias into buf) otherwise.
func (r *reader) bytes(max uint32) ([]byte, error) {
	l, err := r.u32()
	if err != nil {
		return nil, err
	}
	if l > max {
		return nil, ErrLimitExceeded
	}
	if uint64(l) > uint64(r.remaining()) {
		return nil, ErrOverflow
	}
	if l == 0 {
		return []byte{}, nil
	}
	out := make([]byte, l)
	copy(out, r.buf[r.pos:r.pos+int(l)])
	r.pos += int(l)
	return out, nil
}

// count reads a u32 list count and rejects a count whose elements could not possibly fit in
// the remaining buffer, computed in 64-bit arithmetic so a hostile count near 2^32 cannot
// overflow a 32-bit int and wave through. Callers MUST size any resulting slice with
// make(…, 0, n) only after this check, or append incrementally.
func (r *reader) count(elemMin int) (uint32, error) {
	c, err := r.u32()
	if err != nil {
		return 0, err
	}
	if uint64(c)*uint64(elemMin) > uint64(r.remaining()) {
		return 0, ErrOverflow
	}
	return c, nil
}

// end returns ErrTrailingBytes unless the reader has consumed every byte.
func (r *reader) end() error {
	if r.remaining() != 0 {
		return ErrTrailingBytes
	}
	return nil
}
