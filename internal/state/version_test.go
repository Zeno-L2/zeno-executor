package state

import (
	"bytes"
	"testing"

	"github.com/zenon-network/go-zenon/common/trie"

	"github.com/Zeno-L2/zeno-executor/domain"
)

func TestVersionMatchesStore(t *testing.T) {
	store := OpenEmpty()
	t.Cleanup(func() { _ = store.Close() })

	pA, pB, pC, pD := trie.Path{0x10}, trie.Path{0x20}, trie.Path{0x30}, trie.Path{0x40}
	diffs := [][]domain.StateDiffEntry{
		{{Path: pA, Value: []byte("1")}, {Path: pB, Value: []byte{}}},
		{{Path: pA, Value: []byte("2")}, {Path: pB, Value: nil}, {Path: pC, Value: nil}},
		{{Path: pB, Value: []byte("3")}, {Path: pC, Value: []byte{}}},
		{{Path: pA, Value: nil}},
	}

	v := Version{}
	for i, d := range diffs {
		v = v.Apply(d)
		root, err := store.Apply(d)
		if err != nil {
			t.Fatalf("step %d: store apply: %v", i, err)
		}
		if v.Root() != root {
			t.Fatalf("step %d: version root %s, store root %s", i, v.Root(), root)
		}
		for _, p := range []trie.Path{pA, pB, pC, pD} {
			vp, vv, vproof := v.Prove(p)
			sp, sv, sproof, err := store.Prove(p)
			if err != nil {
				t.Fatalf("step %d: store prove: %v", i, err)
			}
			if vp != sp || !bytes.Equal(vv, sv) || !bytes.Equal(vproof, sproof) {
				t.Fatalf("step %d: path %x: version and store proofs differ", i, p)
			}
			if vp && (vv == nil) != (sv == nil) {
				t.Fatalf("step %d: path %x: nil-ness of a present value differs", i, p)
			}
		}
	}
}

func TestStoreVersion(t *testing.T) {
	store := OpenEmpty()
	t.Cleanup(func() { _ = store.Close() })

	pA, pB, pC, pD := trie.Path{0x10}, trie.Path{0x20}, trie.Path{0x30}, trie.Path{0x40}
	if _, err := store.Apply([]domain.StateDiffEntry{{Path: pA, Value: []byte("1")}, {Path: pB, Value: []byte{}}}); err != nil {
		t.Fatalf("first apply: %v", err)
	}
	v := store.Version()
	root, err := store.Root()
	if err != nil {
		t.Fatalf("Root: %v", err)
	}
	if v.Root() != root {
		t.Fatalf("version root %s, store root %s", v.Root(), root)
	}

	type proof struct {
		present bool
		value   []byte
		proof   []byte
	}
	paths := []trie.Path{pA, pB, pC, pD}
	record := func() []proof {
		out := make([]proof, len(paths))
		for i, p := range paths {
			out[i].present, out[i].value, out[i].proof = v.Prove(p)
		}
		return out
	}
	before := record()

	second := []domain.StateDiffEntry{{Path: pA, Value: []byte("2")}, {Path: pB, Value: nil}, {Path: pC, Value: []byte("3")}}
	newRoot, err := store.Apply(second)
	if err != nil {
		t.Fatalf("second apply: %v", err)
	}
	if v.Root() != root {
		t.Fatalf("version root changed to %s after a store apply, want %s", v.Root(), root)
	}
	after := record()
	for i := range paths {
		if before[i].present != after[i].present || !bytes.Equal(before[i].value, after[i].value) || !bytes.Equal(before[i].proof, after[i].proof) {
			t.Fatalf("path %x: version's proof changed after a store apply", paths[i])
		}
	}
	if !after[1].present || after[1].value == nil {
		t.Fatalf("path %x: want present-empty, got present=%v nil=%v", pB, after[1].present, after[1].value == nil)
	}
	if got := v.Apply(second).Root(); got != newRoot {
		t.Fatalf("version apply root %s, store root %s", got, newRoot)
	}
}
