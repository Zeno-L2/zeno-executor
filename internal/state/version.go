package state

import (
	"github.com/zenon-network/go-zenon/common/trie"
	"github.com/zenon-network/go-zenon/common/types"

	"github.com/digitalSloth/go-executor/domain"
)

// Version is one immutable state: a copy of a CompactTree struct.
// CompactTree never mutates a node in place, so a Version shares
// structure with the versions derived from it and never changes when
// they do. The zero Version is the empty state. A Version keeps every
// value slice it is given, so the caller never writes to one
// afterwards.
type Version struct {
	tree trie.CompactTree
}

// Apply returns the state after diff, under the canonical applier
// Store.Apply uses: a nil Value deletes its path, and any other Value,
// including a non-nil zero-length one, sets a present leaf. v itself is
// unchanged.
func (v Version) Apply(diff []domain.StateDiffEntry) Version {
	next := v
	for _, e := range diff {
		if e.Value == nil {
			next.tree.Delete(e.Path)
		} else {
			next.tree.Insert(e.Path, e.Value)
		}
	}
	return next
}

// Root returns the version's root.
func (v Version) Root() types.Hash {
	return v.tree.Root()
}

// Prove returns a path-native inclusion or non-inclusion proof for
// path against the version. The returned value aliases the stored
// slice and must not be written to.
func (v Version) Prove(path trie.Path) (present bool, value []byte, proof []byte) {
	return v.tree.ProveByPath(path)
}

// Version returns the store's current state as a Version. It shares
// the store's trie nodes, and later Apply calls on the store never
// change it.
func (s *Store) Version() Version {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return Version{tree: *s.tree}
}
