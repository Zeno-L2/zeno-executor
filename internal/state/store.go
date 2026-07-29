// Package state is the executor's L2 leaf cache: a persistent, crash-recoverable map of
// derived SMT paths to values, backed only by the shared common/trie path-native API. It is
// a cache of derivable state, never the source of truth — genesis plus the canonical input
// stream can always reconstruct it from scratch.
package state

import (
	"fmt"
	"io"
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
	log    *os.File
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

	return &Store{leaves: leaves, log: f}, nil
}

// Close releases the underlying log file. The Store is unusable afterward.
func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.log.Close()
}

// Root computes the current root over every leaf, using only the shared path-native API.
func (s *Store) Root() (types.Hash, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return rootOfLeaves(s.leaves)
}

// Prove returns a path-native inclusion or non-inclusion proof for path against the store's
// current state.
func (s *Store) Prove(path trie.Path) (present bool, value []byte, proof []byte, err error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	paths, values := leavesSlices(s.leaves)
	return trie.ProveByPath(paths, values, path)
}

// Accessor returns a witnessing, read-only view over the store's current leaf set, for one
// input's Apply call. The runtime supplies one Accessor per input; every Read/Len against it
// — including one observing an absent path — is recorded as a witness automatically, so the
// domain plugin never builds a witness itself.
func (s *Store) Accessor() *Accessor {
	s.mu.RLock()
	defer s.mu.RUnlock()
	snapshot := make(map[trie.Path][]byte, len(s.leaves))
	for k, v := range s.leaves {
		snapshot[k] = v
	}
	return &Accessor{leaves: snapshot, witnesses: make(map[trie.Path]Witness)}
}

// Apply computes the post-state leaf set for diff per the canonical L2 StateDiff applier
// (SPEC §13.5.1): a nil Value deletes its key — a well-formed no-op if the key is already
// absent — and any other Value, including a non-nil zero-length slice, sets a present leaf,
// distinguishing present-empty from absence. The resulting root is a fresh recomputation
// over the complete leaf set, then durably logged before being adopted. The caller (the STF
// driver) is responsible for only calling Apply once an input has succeeded; Apply itself
// applies unconditionally.
func (s *Store) Apply(diff []domain.StateDiffEntry) (types.Hash, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	next := make(map[trie.Path][]byte, len(s.leaves)+len(diff))
	for k, v := range s.leaves {
		next[k] = v
	}
	for _, entry := range diff {
		if entry.Value == nil {
			delete(next, entry.Path)
			continue
		}
		next[entry.Path] = entry.Value
	}

	root, err := rootOfLeaves(next)
	if err != nil {
		return types.Hash{}, fmt.Errorf("state: compute root: %w", err)
	}

	if err := appendRecord(s.log, diff); err != nil {
		return types.Hash{}, fmt.Errorf("state: persist diff: %w", err)
	}

	s.leaves = next
	return root, nil
}

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
