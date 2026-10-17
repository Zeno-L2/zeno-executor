package state

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/zenon-network/go-zenon/common/crypto"
	"github.com/zenon-network/go-zenon/common/trie"
	"github.com/zenon-network/go-zenon/common/types"

	"github.com/digitalSloth/go-executor/domain"
)

func be8(i int) []byte {
	return binary.BigEndian.AppendUint64(nil, uint64(i))
}

// liveHeapMB returns the live heap after a forced collection, in MiB.
func liveHeapMB() float64 {
	runtime.GC()
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return float64(m.HeapAlloc) / (1 << 20)
}

// BenchmarkOpenSnapshot times OpenSnapshot, which decodes the snapshot
// and builds the tree from its leaves, at three leaf counts. It reports
// the live heap of the whole Store and of the tree alone (Store heap
// minus the bare leaf map).
func BenchmarkOpenSnapshot(b *testing.B) {
	for _, n := range []int{1000, 100000, 1000000} {
		leaves := make(map[trie.Path][]byte, n)
		for i := 0; i < n; i++ {
			var p trie.Path
			copy(p[:], crypto.Hash([]byte("leaf"), be8(i)))
			leaves[p] = crypto.Hash(be8(i))
		}
		dir := b.TempDir()
		name, err := (&Store{leaves: leaves}).SaveSnapshot(dir, SnapshotHeader{})
		if err != nil {
			b.Fatalf("SaveSnapshot: %v", err)
		}
		leaves = nil

		heapBefore := liveHeapMB()
		body, err := os.ReadFile(filepath.Join(dir, name+snapshotExt))
		if err != nil {
			b.Fatalf("read snapshot: %v", err)
		}
		_, leafmap, err := decodeSnapshot(body)
		if err != nil {
			b.Fatalf("decodeSnapshot: %v", err)
		}
		body = nil
		leafmapMB := liveHeapMB() - heapBefore
		runtime.KeepAlive(leafmap)
		leafmap = nil

		b.Run(fmt.Sprintf("leaves=%d", n), func(b *testing.B) {
			var storeMB float64
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				before := liveHeapMB()
				b.StartTimer()
				s, err := OpenSnapshot(dir, name, types.Hash{})
				if err != nil {
					b.Fatalf("OpenSnapshot: %v", err)
				}
				b.StopTimer()
				storeMB = liveHeapMB() - before
				runtime.KeepAlive(s)
				b.StartTimer()
			}
			b.ReportMetric(storeMB, "heap-MB")
			b.ReportMetric(storeMB-leafmapMB, "tree-heap-MB")
		})
	}
}

// BenchmarkAccessor times Store.Accessor alone, which shares the leaf map rather than copying
// it, at three leaf counts. The Store is built without its tree, since Accessor reads only
// s.leaves; building a 10⁶-leaf tree per count would add minutes and measure nothing here.
func BenchmarkAccessor(b *testing.B) {
	for _, n := range []int{1000, 100000, 1000000} {
		leaves := make(map[trie.Path][]byte, n)
		for i := 0; i < n; i++ {
			var p trie.Path
			copy(p[:], crypto.Hash([]byte("leaf"), be8(i)))
			leaves[p] = crypto.Hash(be8(i))
		}
		s := &Store{leaves: leaves}

		b.Run(fmt.Sprintf("leaves=%d", n), func(b *testing.B) {
			b.ReportAllocs()
			var acc *Accessor
			for i := 0; i < b.N; i++ {
				acc = s.Accessor()
			}
			runtime.KeepAlive(acc)
		})
	}
}

// BenchmarkAccessorApply times one driver round trip (Store.Accessor, a Read, Witnesses and
// Release, followed by a one-entry Store.Apply) at three leaf counts. Apply writes no file.
// B/op is the signal to read: at a working set this large, a full-map copy (about 128 MiB/op at
// 10⁶ leaves) would dominate it, while sharing the map keeps B/op within a small constant
// factor of its 10³ value, since Store.Apply still runs CompactTree.Insert and Root, which are
// O(log n).
func BenchmarkAccessorApply(b *testing.B) {
	for _, n := range []int{1000, 100000, 1000000} {
		s := OpenEmpty()
		paths := make([]trie.Path, n)
		diff := make([]domain.StateDiffEntry, n)
		for i := 0; i < n; i++ {
			var p trie.Path
			copy(p[:], crypto.Hash([]byte("leaf"), be8(i)))
			paths[i] = p
			diff[i] = domain.StateDiffEntry{Path: p, Value: crypto.Hash(be8(i))}
		}
		if _, err := s.Apply(diff); err != nil {
			b.Fatalf("Apply: %v", err)
		}

		b.Run(fmt.Sprintf("leaves=%d", n), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				acc := s.Accessor()
				acc.Read(domain.Path(paths[i%n]))
				acc.Witnesses()
				acc.Release()
				entry := []domain.StateDiffEntry{{Path: paths[i%n], Value: crypto.Hash(be8(n + i))}}
				if _, err := s.Apply(entry); err != nil {
					b.Fatalf("Apply: %v", err)
				}
			}
		})
		s.Close()
	}
}
