package codec

import (
	"testing"

	"github.com/zenon-network/go-zenon/common/crypto"
	"github.com/zenon-network/go-zenon/common/types"
)

func refLeafHash(e []byte) types.Hash {
	var out types.Hash
	copy(out[:], crypto.Hash(append([]byte{0x00}, e...)))
	return out
}

func refNodeHash(l, r types.Hash) types.Hash {
	var out types.Hash
	copy(out[:], crypto.Hash(append(append([]byte{0x01}, l[:]...), r[:]...)))
	return out
}

func TestMerkleRoot(t *testing.T) {
	elem := func(i byte) []byte { return []byte{i} }

	t.Run("n=0 is zero hash", func(t *testing.T) {
		if got := MerkleRoot(nil); got != (types.Hash{}) {
			t.Errorf("got %x, want zero", got)
		}
	})

	t.Run("n=1 is the leaf hash", func(t *testing.T) {
		got := MerkleRoot([][]byte{elem(0)})
		want := refLeafHash(elem(0))
		if got != want {
			t.Errorf("got %x, want %x", got, want)
		}
	})

	t.Run("n=2", func(t *testing.T) {
		got := MerkleRoot([][]byte{elem(0), elem(1)})
		want := refNodeHash(refLeafHash(elem(0)), refLeafHash(elem(1)))
		if got != want {
			t.Errorf("got %x, want %x", got, want)
		}
	})

	t.Run("n=3 promotes the odd trailing node", func(t *testing.T) {
		got := MerkleRoot([][]byte{elem(0), elem(1), elem(2)})
		l0, l1, l2 := refLeafHash(elem(0)), refLeafHash(elem(1)), refLeafHash(elem(2))
		want := refNodeHash(refNodeHash(l0, l1), l2)
		if got != want {
			t.Errorf("got %x, want %x", got, want)
		}
	})

	t.Run("n=4", func(t *testing.T) {
		got := MerkleRoot([][]byte{elem(0), elem(1), elem(2), elem(3)})
		l0, l1, l2, l3 := refLeafHash(elem(0)), refLeafHash(elem(1)), refLeafHash(elem(2)), refLeafHash(elem(3))
		want := refNodeHash(refNodeHash(l0, l1), refNodeHash(l2, l3))
		if got != want {
			t.Errorf("got %x, want %x", got, want)
		}
	})

	t.Run("n=5 promotes the odd trailing node at the top level", func(t *testing.T) {
		got := MerkleRoot([][]byte{elem(0), elem(1), elem(2), elem(3), elem(4)})
		l0, l1, l2, l3, l4 := refLeafHash(elem(0)), refLeafHash(elem(1)), refLeafHash(elem(2)), refLeafHash(elem(3)), refLeafHash(elem(4))
		left := refNodeHash(refNodeHash(l0, l1), refNodeHash(l2, l3))
		want := refNodeHash(left, l4)
		if got != want {
			t.Errorf("got %x, want %x", got, want)
		}
	})
}
