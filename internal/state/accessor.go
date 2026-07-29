package state

import (
	"bytes"
	"sort"
	"sync"

	"github.com/zenon-network/go-zenon/common/trie"

	"github.com/digitalSloth/go-executor/domain"
)

// Witness is one observation a domain plugin's Apply made during a single input: the path it
// read (via Read or Len) and what it found there, including an absent observation.
type Witness struct {
	Path    trie.Path
	Present bool
	Value   []byte
}

// Accessor is the witnessing, read-only domain.StateAccess implementation supplied to a
// domain plugin's Apply for one input. It is a snapshot: reads never see writes made by the
// same or a later input, since a domain's staged StateDiff is only adopted by Store.Apply
// after the input succeeds.
type Accessor struct {
	mu        sync.Mutex
	leaves    map[trie.Path][]byte
	witnesses map[trie.Path]Witness
}

// Read returns the value stored at path against the accessor's snapshot, recording the
// observation as a witness.
func (a *Accessor) Read(path domain.Path) ([]byte, bool, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	value, present := a.leaves[trie.Path(path)]
	a.record(trie.Path(path), present, value)
	return value, present, nil
}

// Len returns the length of the value stored at path against the accessor's snapshot,
// recording the observation as a witness.
func (a *Accessor) Len(path domain.Path) (int, bool, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	value, present := a.leaves[trie.Path(path)]
	a.record(trie.Path(path), present, value)
	if !present {
		return 0, false, nil
	}
	return len(value), true, nil
}

// record stores the first observation of path; later observations within the same input are
// necessarily identical, since the snapshot is immutable for the accessor's lifetime.
func (a *Accessor) record(path trie.Path, present bool, value []byte) {
	if _, seen := a.witnesses[path]; seen {
		return
	}
	a.witnesses[path] = Witness{Path: path, Present: present, Value: value}
}

// Witnesses returns every path observed during this accessor's lifetime, ascending by path
// and with no duplicates.
func (a *Accessor) Witnesses() []Witness {
	a.mu.Lock()
	defer a.mu.Unlock()
	out := make([]Witness, 0, len(a.witnesses))
	for _, w := range a.witnesses {
		out = append(out, w)
	}
	sort.Slice(out, func(i, j int) bool {
		return bytes.Compare(out[i].Path[:], out[j].Path[:]) < 0
	})
	return out
}
