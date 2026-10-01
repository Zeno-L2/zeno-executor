package state

import (
	"bytes"
	"errors"
	"sort"
	"sync"

	"github.com/zenon-network/go-zenon/common/trie"

	"github.com/digitalSloth/go-executor/domain"
)

// ErrAccessorReleased is returned by Read and Len on an accessor after Release.
var ErrAccessorReleased = errors.New("state: read through a released accessor")

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
// after the input succeeds. It shares the store's leaf map until Release; afterwards Read and
// Len return ErrAccessorReleased and Witnesses keeps working.
type Accessor struct {
	mu        sync.Mutex
	store     *Store
	gen       uint64
	released  bool
	leaves    map[trie.Path][]byte
	witnesses map[trie.Path]Witness
}

// Read returns the value stored at path against the accessor's snapshot, recording the
// observation as a witness.
func (a *Accessor) Read(path domain.Path) ([]byte, bool, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.released {
		return nil, false, ErrAccessorReleased
	}
	value, present := a.leaves[trie.Path(path)]
	a.record(trie.Path(path), present, value)
	return value, present, nil
}

// Len returns the length of the value stored at path against the accessor's snapshot,
// recording the observation as a witness.
func (a *Accessor) Len(path domain.Path) (int, bool, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.released {
		return 0, false, ErrAccessorReleased
	}
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

// Release ends the accessor's use of the store's leaf map. Read and Len return
// ErrAccessorReleased afterwards; Witnesses keeps working. Release is idempotent.
func (a *Accessor) Release() {
	a.mu.Lock()
	if a.released {
		a.mu.Unlock()
		return
	}
	a.released = true
	a.mu.Unlock()

	a.store.mu.Lock()
	defer a.store.mu.Unlock()
	if a.gen == a.store.gen {
		a.store.live--
	}
}
