package state

import (
	"testing"

	"github.com/zenon-network/go-zenon/common/trie"

	"github.com/digitalSloth/go-executor/domain"
)

// TestDoubleHashTrap proves a proof this package produces is only valid through the
// path-native verifier: the key-hashing VerifyProof re-derives a path by hashing whatever it
// is given, so handing it an already-derived path as a "key" double-hashes and must reject.
func TestDoubleHashTrap(t *testing.T) {
	s := openTestStore(t)

	var path trie.Path
	path[0] = 0xAA
	value := []byte("v")

	root, err := s.Apply([]domain.StateDiffEntry{{Path: path, Value: value}})
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}

	present, gotValue, proof, err := s.Prove(path)
	if err != nil {
		t.Fatalf("Prove: %v", err)
	}
	if !present {
		t.Fatalf("Prove: expected present")
	}

	ok, err := trie.VerifyProofByPath(root, path, gotValue, proof)
	if err != nil || !ok {
		t.Fatalf("VerifyProofByPath(path) = %v, %v; want accepted", ok, err)
	}

	ok, err = trie.VerifyProof(root, path[:], value, proof)
	if err == nil && ok {
		t.Fatalf("VerifyProof(path-as-key) accepted — expected the double-hash rejection")
	}
}
