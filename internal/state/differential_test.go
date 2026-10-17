package state

import (
	"bytes"
	"fmt"
	"maps"
	"math/rand"
	"sort"
	"testing"

	"github.com/zenon-network/go-zenon/common/trie"
	"github.com/zenon-network/go-zenon/common/types"

	"github.com/digitalSloth/go-executor/domain"
)

// rootOfLeaves is the full-recompute reference the incremental tree must match.
func rootOfLeaves(leaves map[trie.Path][]byte) (types.Hash, error) {
	paths, values := leavesSlices(leaves)
	return trie.RootOfLeaves(paths, values)
}

func leavesSlices(leaves map[trie.Path][]byte) ([]trie.Path, [][]byte) {
	paths := make([]trie.Path, 0, len(leaves))
	values := make([][]byte, 0, len(leaves))
	for p, v := range leaves {
		paths = append(paths, p)
		values = append(values, v)
	}
	return paths, values
}

type proofResult struct {
	present bool
	value   []byte
	proof   []byte
}

// assertProveMatchesRef checks s.Prove against trie.ProveByPath over ref for every target and
// returns the store's results.
func assertProveMatchesRef(t *testing.T, s *Store, ref map[trie.Path][]byte, targets []trie.Path) map[trie.Path]proofResult {
	t.Helper()
	paths, values := leavesSlices(ref)
	out := make(map[trie.Path]proofResult, len(targets))
	for _, target := range targets {
		wantPresent, wantValue, wantProof, err := trie.ProveByPath(paths, values, target)
		if err != nil {
			t.Fatalf("trie.ProveByPath: %v", err)
		}
		present, value, proof, err := s.Prove(target)
		if err != nil {
			t.Fatalf("Prove: %v", err)
		}
		if present != wantPresent || !bytes.Equal(value, wantValue) || !bytes.Equal(proof, wantProof) {
			t.Fatalf("Prove(%x): present=%v value=%x proof=%x; want present=%v value=%x proof=%x",
				target, present, value, proof, wantPresent, wantValue, wantProof)
		}
		out[target] = proofResult{present, value, proof}
	}
	return out
}

// assertLeavesInSync checks the Store's leaf map, which Accessor and snapshots read, against the
// reference set and the expected root, since Root and Prove read the tree instead.
func assertLeavesInSync(t *testing.T, s *Store, ref map[trie.Path][]byte, wantRoot types.Hash) {
	t.Helper()
	if !maps.EqualFunc(s.leaves, ref, bytes.Equal) {
		t.Fatalf("leaf map diverged from reference")
	}
	got, err := rootOfLeaves(s.leaves)
	if err != nil {
		t.Fatalf("rootOfLeaves(s.leaves): %v", err)
	}
	if got != wantRoot {
		t.Fatalf("root of s.leaves = %x, want %x", got, wantRoot)
	}
}

func sortedKeys(ref map[trie.Path][]byte) []trie.Path {
	keys := make([]trie.Path, 0, len(ref))
	for k := range ref {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool { return bytes.Compare(keys[i][:], keys[j][:]) < 0 })
	return keys
}

// TestStoreMatchesFullRecompute drives random diffs through the Store and checks every root
// and proof against a full recomputation over a mirrored leaf map, including across a reopen
// from a snapshot.
func TestStoreMatchesFullRecompute(t *testing.T) {
	for _, n := range []int{0, 1, 64, 512} {
		n := n
		t.Run(fmt.Sprintf("leaves=%d", n), func(t *testing.T) {
			rng := rand.New(rand.NewSource(int64(n) + 1))
			s := OpenEmpty()
			defer func() { _ = s.Close() }()

			ref := map[trie.Path][]byte{}
			written := map[trie.Path]bool{}
			var deleted []trie.Path

			fresh := func() trie.Path {
				for {
					var p trie.Path
					rng.Read(p[:])
					if !written[p] {
						return p
					}
				}
			}
			randValue := func() []byte {
				v := make([]byte, 1+rng.Intn(16))
				rng.Read(v)
				return v
			}
			mirror := func(diff []domain.StateDiffEntry) {
				for _, e := range diff {
					written[e.Path] = true
					if e.Value == nil {
						delete(ref, e.Path)
						continue
					}
					ref[e.Path] = e.Value
				}
			}
			check := func(diff []domain.StateDiffEntry, root types.Hash) []trie.Path {
				t.Helper()
				want, err := rootOfLeaves(ref)
				if err != nil {
					t.Fatalf("rootOfLeaves: %v", err)
				}
				got, err := s.Root()
				if err != nil {
					t.Fatalf("Root: %v", err)
				}
				if root != want || got != want {
					t.Fatalf("root: Apply=%x Root=%x want=%x", root, got, want)
				}
				assertLeavesInSync(t, s, ref, want)
				targets := make([]trie.Path, 0, len(diff)+2)
				for _, e := range diff {
					targets = append(targets, e.Path)
				}
				if live := sortedKeys(ref); len(live) > 0 {
					targets = append(targets, live[rng.Intn(len(live))])
				}
				targets = append(targets, fresh())
				assertProveMatchesRef(t, s, ref, targets)
				return targets
			}

			step := func(i int) []domain.StateDiffEntry {
				used := map[trie.Path]bool{}
				var diff []domain.StateDiffEntry
				add := func(p trie.Path, v []byte) {
					if used[p] {
						return
					}
					used[p] = true
					diff = append(diff, domain.StateDiffEntry{Path: p, Value: v})
				}
				live := sortedKeys(ref)
				pickLive := func() (trie.Path, bool) {
					if len(live) == 0 {
						return trie.Path{}, false
					}
					return live[rng.Intn(len(live))], true
				}

				add(fresh(), randValue())    // insert a fresh path
				if p, ok := pickLive(); ok { // update a live path
					add(p, randValue())
				}
				if i%2 == 0 { // present-empty, to a live path on even steps, fresh on odd
					if p, ok := pickLive(); ok {
						add(p, []byte{})
					}
				} else {
					add(fresh(), []byte{})
				}
				if p, ok := pickLive(); ok { // delete a live path
					add(p, nil)
				}
				add(fresh(), nil)     // delete a never-written path
				if len(deleted) > 0 { // re-insert a previously deleted path
					add(deleted[rng.Intn(len(deleted))], randValue())
				}

				sort.Slice(diff, func(a, b int) bool { return bytes.Compare(diff[a].Path[:], diff[b].Path[:]) < 0 })
				return diff
			}
			apply := func(diff []domain.StateDiffEntry, st *Store) types.Hash {
				t.Helper()
				root, err := st.Apply(diff)
				if err != nil {
					t.Fatalf("Apply: %v", err)
				}
				for _, e := range diff {
					if e.Value == nil && ref[e.Path] != nil {
						deleted = append(deleted, e.Path)
					}
				}
				mirror(diff)
				return root
			}

			// Seed with one Apply of n entries; every 8th value is present-empty.
			var seed []domain.StateDiffEntry
			for i := 0; i < n; i++ {
				v := randValue()
				if i%8 == 7 {
					v = []byte{}
				}
				seed = append(seed, domain.StateDiffEntry{Path: fresh(), Value: v})
				written[seed[i].Path] = true
			}
			root, err := s.Apply(seed)
			if err != nil {
				t.Fatalf("seed Apply: %v", err)
			}
			mirror(seed)
			check(seed[:min(len(seed), 4)], root)

			var lastDiff []domain.StateDiffEntry
			var lastTargets []trie.Path
			for i := 0; i < 12; i++ {
				lastDiff = step(i)
				root := apply(lastDiff, s)
				lastTargets = check(lastDiff, root)
			}

			// Reopen from a snapshot: the rebuilt tree must reproduce
			// the root and every proof byte for byte.
			wantRoot, _ := s.Root()
			wantProofs := assertProveMatchesRef(t, s, ref, lastTargets)
			dir := t.TempDir()
			name, err := s.SaveSnapshot(dir, SnapshotHeader{})
			if err != nil {
				t.Fatalf("SaveSnapshot: %v", err)
			}
			s, err = OpenSnapshot(dir, name, types.Hash{})
			if err != nil {
				t.Fatalf("OpenSnapshot: %v", err)
			}
			if got, _ := s.Root(); got != wantRoot {
				t.Fatalf("reopened root = %x, want %x", got, wantRoot)
			}
			for target, want := range wantProofs {
				present, value, proof, err := s.Prove(target)
				if err != nil {
					t.Fatalf("Prove after reopen: %v", err)
				}
				if present != want.present || !bytes.Equal(value, want.value) || !bytes.Equal(proof, want.proof) {
					t.Fatalf("reopened Prove(%x) differs from before the reopen", target)
				}
			}

			assertLeavesInSync(t, s, ref, wantRoot)

			// One more step on the reopened store.
			diff := step(12)
			check(diff, apply(diff, s))
		})
	}
}
