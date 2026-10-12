package batch

import (
	"encoding/binary"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/zenon-network/go-zenon/common/crypto"
	"github.com/zenon-network/go-zenon/common/trie"

	"github.com/digitalSloth/go-executor/domain"
	"github.com/digitalSloth/go-executor/internal/state"
)

// be8 is the big-endian 8-byte encoding of i, the index every leaf and probe path in this
// benchmark derives from.
func be8(i int) []byte {
	return binary.BigEndian.AppendUint64(nil, uint64(i))
}

// BenchmarkAbsentKeyWitness times the state side of an absent-key storage_len/storage_read
// witness (Accessor.Len plus Store.Prove) at three leaf counts, reporting the proof size that
// growing state imposes on every absent-key input regardless of the plasma_limit it is charged.
func BenchmarkAbsentKeyWitness(b *testing.B) {
	for _, n := range []int{1000, 100000, 1000000} {
		store, err := state.Open(filepath.Join(b.TempDir(), "state.wal"))
		if err != nil {
			b.Fatalf("state.Open: %v", err)
		}

		leaves := make([]domain.StateDiffEntry, n)
		for i := 0; i < n; i++ {
			value := crypto.Hash(be8(i))
			leaves[i] = domain.StateDiffEntry{
				Path:  trie.Path(hashOf(crypto.Hash([]byte("leaf"), be8(i)))),
				Value: value,
			}
		}
		if _, err := store.Apply(leaves); err != nil {
			b.Fatalf("Apply: %v", err)
		}

		b.Run(fmt.Sprintf("leaves=%d", n), func(b *testing.B) {
			b.ReportAllocs()
			acc := store.Accessor()
			paths := make([]trie.Path, b.N)
			for i := 0; i < b.N; i++ {
				paths[i] = trie.Path(hashOf(crypto.Hash([]byte("absent"), be8(i))))
			}

			b.ResetTimer()
			var total int
			for i := 0; i < b.N; i++ {
				p := paths[i]
				_, present, err := acc.Len(p)
				if err != nil {
					b.Fatal(err)
				}
				if present {
					b.Fatal("Accessor.Len: got present, want absent")
				}
				present, _, proof, err := store.Prove(p)
				if err != nil {
					b.Fatal(err)
				}
				if present {
					b.Fatal("Store.Prove: got present, want absent")
				}
				total += len(proof)
			}
			b.ReportMetric(float64(total)/float64(b.N), "proof-B/op")
		})
		_ = store.Close()
	}
}

// BenchmarkWithdrawals times the rebuild of a maximal batch's
// withdrawals: 64 inputs with 33 outbox messages each.
func BenchmarkWithdrawals(b *testing.B) {
	bundle, root := bigBundle(b)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := Withdrawals(bundle, root); err != nil {
			b.Fatal(err)
		}
	}
}
