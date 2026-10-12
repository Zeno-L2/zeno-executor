package codec

import (
	"encoding/binary"
	"fmt"

	"github.com/zenon-network/go-zenon/common/crypto"
	"github.com/zenon-network/go-zenon/common/types"
)

// MerkleRoot is the §27.3 construction, distinct from the SMT (§13): leaf
// SHA3-256(0x00 || element), node SHA3-256(0x01 || left || right), a lone trailing node at a
// level promoted unchanged, and bytes32(0) for the empty set. One element hashes to its leaf
// hash (promoted at every level). Element order is the caller's responsibility.
func MerkleRoot(elements [][]byte) types.Hash {
	if len(elements) == 0 {
		return types.Hash{}
	}

	level := make([]types.Hash, len(elements))
	for i, e := range elements {
		level[i] = leafHash(e)
	}

	for len(level) > 1 {
		next := make([]types.Hash, 0, (len(level)+1)/2)
		i := 0
		for ; i+1 < len(level); i += 2 {
			next = append(next, nodeHash(level[i], level[i+1]))
		}
		if i < len(level) {
			next = append(next, level[i])
		}
		level = next
	}
	return level[0]
}

// MerkleProofs returns the §27.3 MerkleProof of every element, in
// element order, over the same tree as MerkleRoot. It builds the tree
// once. A proof is count:u32 BE followed by count steps of side:u8 ||
// sibling:bytes32; side 0 puts the sibling on the left, side 1 on the
// right. A node promoted at a level contributes no step at that level.
func MerkleProofs(elements [][]byte) [][]byte {
	if len(elements) == 0 {
		return nil
	}

	level := make([]types.Hash, len(elements))
	for i, e := range elements {
		level[i] = leafHash(e)
	}
	levels := [][]types.Hash{level}
	for len(level) > 1 {
		next := make([]types.Hash, 0, (len(level)+1)/2)
		i := 0
		for ; i+1 < len(level); i += 2 {
			next = append(next, nodeHash(level[i], level[i+1]))
		}
		if i < len(level) {
			next = append(next, level[i])
		}
		levels = append(levels, next)
		level = next
	}

	proofs := make([][]byte, len(elements))
	for i := range elements {
		var steps []byte
		count := uint32(0)
		idx := i
		for _, lvl := range levels[:len(levels)-1] {
			if idx%2 == 1 {
				steps = append(steps, 0)
				steps = append(steps, lvl[idx-1][:]...)
				count++
			} else if idx+1 < len(lvl) {
				steps = append(steps, 1)
				steps = append(steps, lvl[idx+1][:]...)
				count++
			}
			idx /= 2
		}
		proof := binary.BigEndian.AppendUint32(make([]byte, 0, 4+len(steps)), count)
		proofs[i] = append(proof, steps...)
	}
	return proofs
}

// VerifyMerkleProof reports whether proof places element under root. It
// hashes element as a leaf, so a leaf hash passed as the element does
// not verify. The zero root of the empty set admits no proof.
func VerifyMerkleProof(root types.Hash, element, proof []byte) error {
	const stepSize = 33
	if len(proof) < 4 {
		return fmt.Errorf("codec: verify merkle proof: %w", ErrTruncated)
	}
	count := uint64(binary.BigEndian.Uint32(proof))
	if count > MaxMerkleProofDepth {
		return fmt.Errorf("codec: verify merkle proof: %w", ErrLimitExceeded)
	}
	want := 4 + stepSize*count
	if uint64(len(proof)) < want {
		return fmt.Errorf("codec: verify merkle proof: %w", ErrOverflow)
	}
	if uint64(len(proof)) > want {
		return fmt.Errorf("codec: verify merkle proof: %w", ErrTrailingBytes)
	}

	h := leafHash(element)
	for off := 4; off < len(proof); off += stepSize {
		var sibling types.Hash
		copy(sibling[:], proof[off+1:off+stepSize])
		switch proof[off] {
		case 0:
			h = nodeHash(sibling, h)
		case 1:
			h = nodeHash(h, sibling)
		default:
			return fmt.Errorf("codec: verify merkle proof: %w", ErrInvalidProofSide)
		}
	}
	if h != root {
		return fmt.Errorf("codec: verify merkle proof: %w", ErrProofMismatch)
	}
	return nil
}

func leafHash(element []byte) types.Hash {
	return hashOf(crypto.Hash([]byte{0x00}, element))
}

func nodeHash(left, right types.Hash) types.Hash {
	return hashOf(crypto.Hash([]byte{0x01}, left[:], right[:]))
}

func hashOf(b []byte) types.Hash {
	var out types.Hash
	copy(out[:], b)
	return out
}
