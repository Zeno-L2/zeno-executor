package ipc

import (
	"errors"
	"fmt"
	"math"

	"github.com/zenon-network/go-zenon/chain/nom"
)

// errHashMismatch marks a momentum or account-block whose stored hash does not match its
// recomputed one.
var errHashMismatch = errors.New("ipc: hash mismatch")

// EncodeMomentum returns dm's wire encoding: u32 len ‖ dm.Momentum.Serialize() ‖ u32 n ‖
// n × (u32 len ‖ AccountBlock.Serialize()). It does not check hashes. Every Serialize call runs
// under recover, because go-zenon's Proto() panics on a nil content entry or a nil descendant.
func EncodeMomentum(dm *nom.DetailedMomentum) (b []byte, err error) {
	defer func() {
		if r := recover(); r != nil {
			b, err = nil, fmt.Errorf("ipc: encode momentum: panic: %v", r)
		}
	}()

	if dm == nil {
		return nil, errors.New("ipc: encode momentum: nil DetailedMomentum")
	}
	if dm.Momentum == nil {
		return nil, errors.New("ipc: encode momentum: nil Momentum")
	}
	momBytes, serr := dm.Momentum.Serialize()
	if serr != nil {
		return nil, fmt.Errorf("ipc: encode momentum: serialize momentum: %w", serr)
	}
	if uint64(len(momBytes)) > math.MaxUint32 {
		return nil, errors.New("ipc: encode momentum: momentum encoding exceeds 2^32-1 bytes")
	}
	if uint64(len(dm.AccountBlocks)) > math.MaxUint32 {
		return nil, errors.New("ipc: encode momentum: too many account blocks")
	}

	w := &pwriter{}
	w.u32(uint32(len(momBytes)))
	w.raw(momBytes)
	w.u32(uint32(len(dm.AccountBlocks)))
	for i, ab := range dm.AccountBlocks {
		if ab == nil {
			return nil, fmt.Errorf("ipc: encode momentum: nil account block at index %d", i)
		}
		abBytes, serr := ab.Serialize()
		if serr != nil {
			return nil, fmt.Errorf("ipc: encode momentum: serialize account block %d: %w", i, serr)
		}
		if uint64(len(abBytes)) > math.MaxUint32 {
			return nil, fmt.Errorf("ipc: encode momentum: account block %d encoding exceeds 2^32-1 bytes", i)
		}
		w.u32(uint32(len(abBytes)))
		w.raw(abBytes)
	}
	return w.buf, nil
}

// splitMomentumFrame validates the structural layout of a Momentum payload without decoding any
// proto content and without materializing a list of block byte-ranges: a u32 len ‖ bytes
// momentum, then a u32 n ‖ n × (u32 len ‖ bytes) list of account blocks. Every length is checked
// against the remaining bytes before it is used to slice, n's implied minimum size (n*4, in
// uint64) is checked before the loop below runs, and a declared block length of 0 is rejected —
// no valid AccountBlock encoding is empty. Trailing bytes are rejected. It returns momBytes, the
// validated block count n, and blockRegion, the sub-slice of b holding exactly those n entries
// header-to-header: nothing about the individual entries is retained here, so a large claimed n
// costs only the walk, never an allocation per entry. DecodeMomentum re-walks blockRegion itself,
// decoding (and only then allocating for) one entry at a time.
func splitMomentumFrame(b []byte) (momBytes []byte, n uint32, blockRegion []byte, err error) {
	r := newPReader(b)

	momLen, err := r.u32()
	if err != nil {
		return nil, 0, nil, errors.New("ipc: decode momentum: truncated momentum length")
	}
	momBytes, err = r.fixed(int(momLen))
	if err != nil {
		return nil, 0, nil, fmt.Errorf("ipc: decode momentum: momentum length %d exceeds remaining bytes", momLen)
	}

	n, err = r.u32()
	if err != nil {
		return nil, 0, nil, errors.New("ipc: decode momentum: truncated account block count")
	}
	if uint64(n)*4 > uint64(r.remaining()) {
		return nil, 0, nil, fmt.Errorf("ipc: decode momentum: account block count %d exceeds remaining bytes", n)
	}

	regionStart := r.pos
	for i := uint32(0); i < n; i++ {
		blen, err := r.u32()
		if err != nil {
			return nil, 0, nil, fmt.Errorf("ipc: decode momentum: truncated account block %d length", i)
		}
		if blen == 0 {
			return nil, 0, nil, fmt.Errorf("ipc: decode momentum: account block %d has zero length", i)
		}
		if _, err := r.fixed(int(blen)); err != nil {
			return nil, 0, nil, fmt.Errorf("ipc: decode momentum: account block %d length %d exceeds remaining bytes", i, blen)
		}
	}
	blockRegion = r.buf[regionStart:r.pos]

	if err := r.end(); err != nil {
		return nil, 0, nil, errors.New("ipc: decode momentum: trailing bytes")
	}
	return momBytes, n, blockRegion, nil
}

// DecodeMomentum decodes and fully verifies b. Structure is checked first (see
// splitMomentumFrame); nom.DeserializeMomentum, nom.DeserializeAccountBlock and the hash checks
// below then run under one recover, because go-zenon panics on malformed proto (DeProtoHash,
// go-zenon/common/types/hash.go:86-93; DeSerializeNonce, go-zenon/chain/nom/account_block.go:74-80).
// A mismatched hash — the momentum's own, any account block's, or, recursively, any descendant
// block's — wraps errHashMismatch. Every error is prefixed "ipc: decode momentum: " and never
// wraps ErrMalformed; ReadMessage adds that.
func DecodeMomentum(b []byte) (dm *nom.DetailedMomentum, err error) {
	momBytes, n, blockRegion, err := splitMomentumFrame(b)
	if err != nil {
		return nil, err
	}

	defer func() {
		if r := recover(); r != nil {
			dm, err = nil, fmt.Errorf("ipc: decode momentum: panic: %v", r)
		}
	}()

	m, derr := nom.DeserializeMomentum(momBytes)
	if derr != nil {
		return nil, fmt.Errorf("ipc: decode momentum: deserialize momentum: %w", derr)
	}

	// Re-walk blockRegion, which splitMomentumFrame already validated header-to-header: decode
	// one entry at a time and append only once it has actually decoded, so a claimed n far
	// larger than what actually decodes never costs more than the entries this loop reaches.
	var blocks []*nom.AccountBlock
	br := newPReader(blockRegion)
	for i := uint32(0); i < n; i++ {
		blen, err := br.u32()
		if err != nil {
			return nil, fmt.Errorf("ipc: decode momentum: re-walk account block %d length: %w", i, err)
		}
		bb, err := br.fixed(int(blen))
		if err != nil {
			return nil, fmt.Errorf("ipc: decode momentum: re-walk account block %d bytes: %w", i, err)
		}
		ab, derr := nom.DeserializeAccountBlock(bb)
		if derr != nil {
			return nil, fmt.Errorf("ipc: decode momentum: deserialize account block %d: %w", i, derr)
		}
		blocks = append(blocks, ab)
	}

	if got := m.ComputeHash(); got != m.Hash {
		return nil, fmt.Errorf("ipc: decode momentum: momentum height %d: %w", m.Height, errHashMismatch)
	}
	for i, ab := range blocks {
		if err := verifyBlockHash(ab); err != nil {
			return nil, fmt.Errorf("ipc: decode momentum: momentum height %d account block %d: %w", m.Height, i, err)
		}
	}

	return &nom.DetailedMomentum{Momentum: m, AccountBlocks: blocks}, nil
}

// verifyBlockHash checks b's own hash and, recursively, every block in DescendantBlocks at any
// depth.
func verifyBlockHash(b *nom.AccountBlock) error {
	if got := b.ComputeHash(); got != b.Hash {
		return fmt.Errorf("block hash %s: %w", b.Hash, errHashMismatch)
	}
	for _, d := range b.DescendantBlocks {
		if err := verifyBlockHash(d); err != nil {
			return err
		}
	}
	return nil
}
