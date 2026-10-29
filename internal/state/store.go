// Package state is the executor's L2 leaf cache: an in-memory map of
// derived SMT paths to values, backed only by the shared common/trie
// path-native API. Its only durable image is a snapshot. It is a
// cache of derivable state, never the source of truth: genesis plus
// the canonical input stream can always reconstruct it from scratch.
package state

import (
	"maps"
	"sync"

	"github.com/zenon-network/go-zenon/common/trie"
	"github.com/zenon-network/go-zenon/common/types"

	"github.com/Zeno-L2/zeno-executor/domain"
)

// Store is the executor's current L2 leaf set, held in memory. It
// opens empty or from a snapshot, and SaveSnapshot writes its durable
// image.
type Store struct {
	mu     sync.RWMutex
	leaves map[trie.Path][]byte
	tree   *trie.CompactTree // mirrors leaves; mutated only under mu.Lock with leaves
	// gen numbers the map leaves currently refers to; Apply starts a new one when it clones.
	gen uint64
	// live counts the unreleased accessors sharing generation gen's map.
	live int
}

// newStore builds the tree once from leaves (O(N log N), once per
// open).
func newStore(leaves map[trie.Path][]byte) *Store {
	tree := trie.NewCompactTree()
	for p, v := range leaves {
		tree.Insert(p, v)
	}
	return &Store{leaves: leaves, tree: tree}
}

// Close releases nothing and always returns nil, because the store
// holds only memory.
func (s *Store) Close() error {
	return nil
}

// Root returns the current root over every leaf, in O(log N) from the incremental tree.
func (s *Store) Root() (types.Hash, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.tree.Root(), nil
}

// Prove returns a path-native inclusion or non-inclusion proof for path against the store's
// current state.
func (s *Store) Prove(path trie.Path) (present bool, value []byte, proof []byte, err error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	present, value, proof = s.tree.ProveByPath(path)
	return present, value, proof, nil
}

// Accessor returns a witnessing, read-only view over the store's current leaf set, for one
// input's Apply call. The runtime supplies one Accessor per input; every Read/Len against it
// — including one observing an absent path — is recorded as a witness automatically, so the
// domain plugin never builds a witness itself. The accessor shares the store's leaf map rather
// than copying it; Apply clones the map before mutating it for as long as any accessor sharing
// it remains unreleased, so the accessor's reads never see a later Apply's writes. Release ends
// the accessor's hold on its generation once the caller is done reading.
func (s *Store) Accessor() *Accessor {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.live++
	return &Accessor{store: s, gen: s.gen, leaves: s.leaves, witnesses: make(map[trie.Path]Witness)}
}

// Apply applies diff per the canonical L2 StateDiff applier. A nil
// Value deletes its key, a well-formed no-op when the key is already
// absent. Any other Value, including a non-nil zero-length slice, sets
// a present leaf, so present-empty stays distinct from absence. The
// returned root equals a fresh RootOfLeaves over the post-state leaf
// set. Apply cannot fail: its error is always nil, as for Root and
// Prove. Each entry's Value is retained by reference in both the leaf
// set and the tree, so the caller must not mutate it after the call.
// The caller (the STF driver) is responsible for only calling Apply
// once an input has succeeded; Apply itself applies unconditionally.
func (s *Store) Apply(diff []domain.StateDiffEntry) (types.Hash, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.live > 0 {
		s.leaves = maps.Clone(s.leaves)
		s.gen++
		s.live = 0
	}
	for _, entry := range diff {
		if entry.Value == nil {
			delete(s.leaves, entry.Path)
			s.tree.Delete(entry.Path)
			continue
		}
		s.leaves[entry.Path] = entry.Value
		s.tree.Insert(entry.Path, entry.Value)
	}
	return s.tree.Root(), nil
}
