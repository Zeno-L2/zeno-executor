package wasm

import (
	"bytes"
	"encoding/hex"
	"testing"

	"github.com/zenon-network/go-zenon/common/crypto"
	"github.com/zenon-network/go-zenon/common/types"

	"github.com/Zeno-L2/zeno-executor/domain"
)

func TestReservedExactMatch(t *testing.T) {
	for _, k := range reservedKeys {
		if !reserved(k) {
			t.Errorf("reserved(%x) = false, want true", k)
		}
	}

	rejected := [][]byte{
		{0x00},
		{0x00, 0x06},
		{0x00, 0x01, 0x00},
		nil,
		{},
		{0x01},
	}
	for _, k := range rejected {
		if reserved(k) {
			t.Errorf("reserved(%x) = true, want false", k)
		}
	}
}

func TestDeriveVector(t *testing.T) {
	var contractID types.Hash
	for i := range contractID {
		contractID[i] = byte(i)
	}
	localKey := []byte{0x00, 0x01}

	got := derive(contractID, localKey)
	want := domain.Path(hashOf(crypto.Hash(contractID[:], localKey)))

	if got != want {
		t.Fatalf("derive mismatch: got %x, want %x", got, want)
	}

	// Distinct lengths must derive to distinct paths (length-sensitive derivation).
	a := derive(contractID, []byte{0x00})
	b := derive(contractID, []byte{0x00, 0x01})
	c := derive(contractID, []byte{0x00, 0x01, 0x00})
	if a == b || b == c || a == c {
		t.Fatalf("derive is not length-sensitive: a=%x b=%x c=%x", a, b, c)
	}
}

// TestCodeHashNISTVectors pins CodeHash to real SHA3-256, not Keccak-256 or any other hash
// family — the two published vectors are the standard NIST SHA3-256 empty-input and "abc"
// digests, independent of this codebase.
func TestCodeHashNISTVectors(t *testing.T) {
	cases := []struct {
		name string
		in   []byte
		want string
	}{
		{"empty", []byte{}, "a7ffc6f8bf1ed76651c14756a061d662f580ff4de43b49fa82d80a4b80f8434a"},
		{"abc", []byte("abc"), "3a985da74fe225b2045c172d6bd390bd855f086e3e9d525b46bfe24511431532"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			want, err := hex.DecodeString(c.want)
			if err != nil {
				t.Fatalf("bad test vector: %v", err)
			}
			got := CodeHash(c.in)
			if !bytes.Equal(got[:], want) {
				t.Errorf("CodeHash(%s) = %x, want %s", c.name, got, c.want)
			}
		})
	}
}
