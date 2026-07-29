package codec

import (
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
