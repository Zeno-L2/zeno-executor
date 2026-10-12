package codec

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
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

func distinctElements(n int) [][]byte {
	out := make([][]byte, n)
	for i := range out {
		out[i] = []byte(fmt.Sprintf("element-%d", i))
	}
	return out
}

// proofOf builds a proof from raw steps, each side || sibling.
func proofOf(count uint32, steps ...[]byte) []byte {
	out := binary.BigEndian.AppendUint32(nil, count)
	for _, s := range steps {
		out = append(out, s...)
	}
	return out
}

func step(side byte, fill byte) []byte {
	return append([]byte{side}, bytes.Repeat([]byte{fill}, 32)...)
}

func TestMerkleProofs(t *testing.T) {
	t.Run("every proof verifies for lengths 1 to 33", func(t *testing.T) {
		for n := 1; n <= 33; n++ {
			elements := distinctElements(n)
			root := MerkleRoot(elements)
			proofs := MerkleProofs(elements)
			if len(proofs) != n {
				t.Fatalf("n=%d: got %d proofs", n, len(proofs))
			}
			for i, p := range proofs {
				if err := VerifyMerkleProof(root, elements[i], p); err != nil {
					t.Fatalf("n=%d i=%d: %v", n, i, err)
				}
				if n > 1 {
					if err := VerifyMerkleProof(root, elements[(i+1)%n], p); !errors.Is(err, ErrProofMismatch) {
						t.Fatalf("n=%d i=%d: other element: got %v, want ErrProofMismatch", n, i, err)
					}
				}
			}
		}
	})

	t.Run("step counts", func(t *testing.T) {
		cases := []struct {
			n    int
			want []uint32
		}{
			{1, []uint32{0}},
			{3, []uint32{2, 2, 1}},
			{5, []uint32{3, 3, 3, 3, 1}},
		}
		for _, c := range cases {
			proofs := MerkleProofs(distinctElements(c.n))
			for i, p := range proofs {
				if got := binary.BigEndian.Uint32(p); got != c.want[i] {
					t.Errorf("n=%d i=%d: count %d, want %d", c.n, i, got, c.want[i])
				}
				if len(p) != 4+33*int(c.want[i]) {
					t.Errorf("n=%d i=%d: length %d", c.n, i, len(p))
				}
			}
		}
	})

	t.Run("one element has the empty proof", func(t *testing.T) {
		proofs := MerkleProofs(distinctElements(1))
		if len(proofs) != 1 || !bytes.Equal(proofs[0], []byte{0, 0, 0, 0}) {
			t.Fatalf("got %x", proofs)
		}
	})

	t.Run("a 2112 element tree gives proofs of at most 12 steps that verify", func(t *testing.T) {
		elements := distinctElements(2112)
		root := MerkleRoot(elements)
		for i, p := range MerkleProofs(elements) {
			if got := binary.BigEndian.Uint32(p); got > 12 {
				t.Fatalf("i=%d: %d steps", i, got)
			}
			if err := VerifyMerkleProof(root, elements[i], p); err != nil {
				t.Fatalf("i=%d: %v", i, err)
			}
		}
	})

	t.Run("no elements gives no proofs", func(t *testing.T) {
		if got := MerkleProofs(nil); len(got) != 0 {
			t.Fatalf("got %d proofs", len(got))
		}
	})
}

func TestVerifyMerkleProof(t *testing.T) {
	elements := distinctElements(5)
	root := MerkleRoot(elements)
	proofs := MerkleProofs(elements)
	valid := proofs[0]

	t.Run("malformed", func(t *testing.T) {
		cases := []struct {
			name  string
			proof []byte
			want  error
		}{
			{"nil", nil, ErrTruncated},
			{"three bytes", []byte{0, 0, 0}, ErrTruncated},
			{"last byte missing", valid[:len(valid)-1], ErrOverflow},
			{"trailing byte", append(append([]byte{}, valid...), 0), ErrTrailingBytes},
			{"count 5 with one step", proofOf(5, step(0, 1)), ErrOverflow},
			{"count 1 with two steps", proofOf(1, step(0, 1), step(0, 2)), ErrTrailingBytes},
			{"side 2", proofOf(1, step(2, 1)), ErrInvalidProofSide},
			{"count 33 with 33 steps", proofOf(33, bytes.Repeat(step(0, 1), 33)), ErrLimitExceeded},
			{"count 0xFFFFFFFF with no steps", proofOf(0xFFFFFFFF), ErrLimitExceeded},
		}
		for _, c := range cases {
			t.Run(c.name, func(t *testing.T) {
				if err := VerifyMerkleProof(root, elements[0], c.proof); !errors.Is(err, c.want) {
					t.Fatalf("got %v, want %v", err, c.want)
				}
			})
		}
	})

	t.Run("well formed but wrong", func(t *testing.T) {
		flippedSide := append([]byte{}, valid...)
		flippedSide[4] ^= 0x01
		flippedSibling := append([]byte{}, valid...)
		flippedSibling[5] ^= 0x01
		var manySteps []byte
		for i := 0; i < MaxMerkleProofDepth; i++ {
			manySteps = append(manySteps, step(byte(i%2), byte(i))...)
		}
		leaf := leafHash(elements[0])

		cases := []struct {
			name    string
			root    types.Hash
			element []byte
			proof   []byte
		}{
			{"flipped side", root, elements[0], flippedSide},
			{"flipped sibling byte", root, elements[0], flippedSibling},
			{"another element's proof", root, elements[0], proofs[1]},
			{"different element", root, []byte("other"), valid},
			{"leaf hash as element", root, leaf[:], valid},
			{"zero root", types.Hash{}, elements[0], valid},
			{"32 arbitrary steps", root, elements[0], proofOf(MaxMerkleProofDepth, manySteps)},
		}
		for _, c := range cases {
			t.Run(c.name, func(t *testing.T) {
				if err := VerifyMerkleProof(c.root, c.element, c.proof); !errors.Is(err, ErrProofMismatch) {
					t.Fatalf("got %v, want ErrProofMismatch", err)
				}
			})
		}
	})
}
