// Package state is the executor's L2 leaf cache: a persistent, crash-recoverable map of
// derived SMT paths to values, backed only by the shared common/trie path-native API. It is
// a cache of derivable state, never the source of truth — genesis plus the canonical input
// stream can always reconstruct it from scratch.
package state

import (
	"fmt"
	"io"
	"maps"
	"os"
	"sync"

	"github.com/zenon-network/go-zenon/common/trie"
	"github.com/zenon-network/go-zenon/common/types"

	"github.com/digitalSloth/go-executor/domain"
)

// Store is the executor's current L2 leaf set, durably logged so it survives a crash.
type Store struct {
	mu     sync.RWMutex
	leaves map[trie.Path][]byte
	tree   *trie.CompactTree // mirrors leaves; mutated only under mu.Lock with leaves
	log    *os.File
	// gen numbers the map leaves currently refers to; Apply starts a new one when it clones.
	gen uint64
	// live counts the unreleased accessors sharing generation gen's map.
	live int
}

// newStore builds the tree once from leaves (O(N log N), once per process), then wraps both with log.
func newStore(leaves map[trie.Path][]byte, log *os.File) *Store {
	tree := trie.NewCompactTree()
	for p, v := range leaves {
		tree.Insert(p, v)
	}
	return &Store{leaves: leaves, tree: tree, log: log}
}

// Open loads path's write-ahead log, creating it if absent, replaying every well-formed
// record into memory. A torn trailing record — the signature of a crash mid-append — is
// discarded rather than treated as corruption; any other malformed record is a hard error.
func Open(path string) (*Store, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, fmt.Errorf("state: open %s: %w", path, err)
	}

	leaves, validLen, err := replayLog(f)
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("state: replay %s: %w", path, err)
	}
	if err := f.Truncate(validLen); err != nil {
		f.Close()
		return nil, fmt.Errorf("state: truncate torn tail of %s: %w", path, err)
	}
	if _, err := f.Seek(validLen, io.SeekStart); err != nil {
		f.Close()
		return nil, fmt.Errorf("state: seek %s: %w", path, err)
	}

	return newStore(leaves, f), nil
}

// Close releases the underlying log file. The Store is unusable afterward.
func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.log.Close()
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

// Apply applies diff per the canonical L2 StateDiff applier (SPEC §13.5.1): a nil Value
// deletes its key — a well-formed no-op if the key is already absent — and any other Value,
// including a non-nil zero-length slice, sets a present leaf, distinguishing present-empty
// from absence. The diff is logged before any entry is applied, so a failed append leaves the
// Store unchanged. The returned root equals a fresh RootOfLeaves over the post-state leaf set.
// Each entry's Value is retained by reference in both the leaf set and the tree, so the caller
// must not mutate it after the call. The caller (the STF driver) is responsible for only
// calling Apply once an input has succeeded; Apply itself applies unconditionally.
func (s *Store) Apply(diff []domain.StateDiffEntry) (types.Hash, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := appendRecord(s.log, diff); err != nil {
		return types.Hash{}, fmt.Errorf("state: persist diff: %w", err)
	}
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
