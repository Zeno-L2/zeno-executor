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

// seedLog writes one n-leaf record to a fresh log at walPath without building any tree.
func seedLog(b *testing.B, walPath string, n int) {
	b.Helper()
	f, err := os.OpenFile(walPath, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0o644)
	if err != nil {
		b.Fatalf("create log: %v", err)
	}
	diff := make([]domain.StateDiffEntry, n)
	for i := 0; i < n; i++ {
		var p trie.Path
		copy(p[:], crypto.Hash([]byte("leaf"), be8(i)))
		diff[i] = domain.StateDiffEntry{Path: p, Value: crypto.Hash(be8(i))}
	}
	if err := appendRecord(f, diff); err != nil {
		b.Fatalf("appendRecord: %v", err)
	}
	if err := f.Close(); err != nil {
		b.Fatalf("close log: %v", err)
	}
}

// BenchmarkOpen times Open, which builds the tree from the replayed leaves, at three leaf
// counts. It is the proxy for OpenSnapshot and OpenEmpty too, which share newStore. It reports
// the live heap of the whole Store and of the tree alone (Store heap minus the bare leaf map).
func BenchmarkOpen(b *testing.B) {
	for _, n := range []int{1000, 100000, 1000000} {
		walPath := filepath.Join(b.TempDir(), "state.wal")
		seedLog(b, walPath, n)

		f, err := os.Open(walPath)
		if err != nil {
			b.Fatalf("open log: %v", err)
		}
		heapBefore := liveHeapMB()
		leafmap, _, err := replayLog(f)
		if err != nil {
			b.Fatalf("replayLog: %v", err)
		}
		leafmapMB := liveHeapMB() - heapBefore
		runtime.KeepAlive(leafmap)
		leafmap = nil
		f.Close()

		b.Run(fmt.Sprintf("leaves=%d", n), func(b *testing.B) {
			var storeMB float64
			for i := 0; i < b.N; i++ {
				b.StopTimer()
				before := liveHeapMB()
				b.StartTimer()
				s, err := Open(walPath)
				if err != nil {
					b.Fatalf("Open: %v", err)
				}
				b.StopTimer()
				storeMB = liveHeapMB() - before
				runtime.KeepAlive(s)
				s.Close()
				b.StartTimer()
			}
			b.ReportMetric(storeMB, "heap-MB")
			b.ReportMetric(storeMB-leafmapMB, "tree-heap-MB")
		})
	}
}

// BenchmarkAccessor times Store.Accessor alone, which shares the leaf map rather than copying
// it, at three leaf counts. The Store is built without its tree or log, since Accessor reads
// only s.leaves; building a 10⁶-leaf tree per count would add minutes and measure nothing here.
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
// Release, followed by a one-entry Store.Apply) at three leaf counts. Apply fsyncs the log on
// every call, so wall time grows with that fixed cost rather than with leaf count. B/op is the
// signal to read: at a working set this large, a full-map copy (about 128 MiB/op at
// 10⁶ leaves) would dominate it, while sharing the map keeps B/op within a small constant
// factor of its 10³ value, since Store.Apply still runs CompactTree.Insert and Root, which are
// O(log n).
func BenchmarkAccessorApply(b *testing.B) {
	for _, n := range []int{1000, 100000, 1000000} {
		walPath := filepath.Join(b.TempDir(), "state.wal")
		s, err := OpenEmpty(walPath)
		if err != nil {
			b.Fatalf("OpenEmpty: %v", err)
		}
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
