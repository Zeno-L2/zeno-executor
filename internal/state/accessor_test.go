package state

import (
	"errors"
	"reflect"
	"sync"
	"testing"

	"github.com/Zeno-L2/zeno-executor/domain"
)

// TestAccessorUnreleasedReadsPreStateAcrossApply covers the probeAcc shape
// (plugins/wasm/determinism_test.go:191-224): an accessor taken before an
// Apply must keep reading its generation's values after that Apply, which
// clones the map because the accessor is unreleased.
func TestAccessorUnreleasedReadsPreStateAcrossApply(t *testing.T) {
	p, q, r := snapPath(1), snapPath(2), snapPath(3)
	s := newSnapStore(t, domain.StateDiffEntry{Path: p, Value: []byte("v1")}, domain.StateDiffEntry{Path: q, Value: []byte("v2")})

	acc1 := s.Accessor()
	acc2 := s.Accessor()

	if _, err := s.Apply([]domain.StateDiffEntry{
		{Path: p, Value: []byte("v3")},
		{Path: q, Value: nil},
		{Path: r, Value: []byte("v4")},
	}); err != nil {
		t.Fatalf("Apply: %v", err)
	}

	for _, acc := range []*Accessor{acc1, acc2} {
		value, present, err := acc.Read(p)
		if err != nil || !present || string(value) != "v1" {
			t.Fatalf("Read(p) = %q, %v, %v, want v1, true, nil", value, present, err)
		}
		_, present, err = acc.Read(q)
		if err != nil || !present {
			t.Fatalf("Read(q) = present %v, err %v, want true, nil", present, err)
		}
		_, present, err = acc.Read(r)
		if err != nil || present {
			t.Fatalf("Read(r) = present %v, err %v, want false, nil", present, err)
		}
		n, present, err := acc.Len(p)
		if err != nil || !present || n != 2 {
			t.Fatalf("Len(p) = %d, %v, %v, want 2, true, nil", n, present, err)
		}
	}
}

// TestAccessorUnreleasedWitnessesAfterApply covers the refDerive shape
// (plugins/wasm/determinism_test.go:191,233; vectors_test.go:415,493):
// Witnesses must reflect only the reads made before a later Apply.
func TestAccessorUnreleasedWitnessesAfterApply(t *testing.T) {
	p, absent := snapPath(1), snapPath(2)
	s := newSnapStore(t, domain.StateDiffEntry{Path: p, Value: []byte("v1")})

	acc := s.Accessor()
	if _, _, err := acc.Read(p); err != nil {
		t.Fatalf("Read(p): %v", err)
	}
	if _, _, err := acc.Read(absent); err != nil {
		t.Fatalf("Read(absent): %v", err)
	}
	before := acc.Witnesses()

	if _, err := s.Apply([]domain.StateDiffEntry{
		{Path: p, Value: []byte("v2")},
		{Path: absent, Value: []byte("v3")},
	}); err != nil {
		t.Fatalf("Apply: %v", err)
	}

	after := acc.Witnesses()
	if !reflect.DeepEqual(before, after) {
		t.Fatalf("Witnesses() changed after Apply: before %+v, after %+v", before, after)
	}
}

// TestAccessorStaleReleaseKeepsCurrentGenerationIsolated proves a Release
// against an already-superseded generation does not decrement the current
// generation's live count, which would let the next Apply mutate a2's map
// in place.
func TestAccessorStaleReleaseKeepsCurrentGenerationIsolated(t *testing.T) {
	p := snapPath(1)
	s := newSnapStore(t, domain.StateDiffEntry{Path: p, Value: []byte("v1")})

	a1 := s.Accessor()
	if _, err := s.Apply([]domain.StateDiffEntry{{Path: p, Value: []byte("v2")}}); err != nil {
		t.Fatalf("Apply 1: %v", err)
	}
	genAfterFirstApply := s.gen

	a2 := s.Accessor()
	a1.Release()

	if _, err := s.Apply([]domain.StateDiffEntry{{Path: p, Value: []byte("v3")}}); err != nil {
		t.Fatalf("Apply 2: %v", err)
	}
	if s.gen == genAfterFirstApply {
		t.Fatalf("second Apply did not change s.gen: still %d", s.gen)
	}

	value, present, err := a2.Read(p)
	if err != nil || !present || string(value) != "v2" {
		t.Fatalf("a2.Read(p) = %q, %v, %v, want v2, true, nil", value, present, err)
	}
}

// TestAccessorDoubleReleaseKeepsSiblingIsolated proves Release is idempotent:
// releasing a1 twice must not decrement live twice, which would let the
// following Apply mutate a2's still-live generation in place.
func TestAccessorDoubleReleaseKeepsSiblingIsolated(t *testing.T) {
	p := snapPath(1)
	s := newSnapStore(t, domain.StateDiffEntry{Path: p, Value: []byte("v1")})

	a1 := s.Accessor()
	a2 := s.Accessor()
	genBefore := s.gen

	a1.Release()
	a1.Release()

	if _, err := s.Apply([]domain.StateDiffEntry{{Path: p, Value: []byte("v2")}}); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if s.gen == genBefore {
		t.Fatalf("Apply did not change s.gen: still %d", s.gen)
	}

	value, present, err := a2.Read(p)
	if err != nil || !present || string(value) != "v1" {
		t.Fatalf("a2.Read(p) = %q, %v, %v, want v1, true, nil", value, present, err)
	}
}

// TestAccessorDriverSequenceDoesNotClone replays the driver's per-input
// sequence (Accessor, Read, Witnesses, Release, then Apply) with every
// accessor released before its Apply. The no-clone guarantee this proves
// for the real driver rests on computeRecord's defer acc.Release() running
// before finishApply's Store.Apply call (internal/batch/driver.go) - this
// test only establishes that the sequence itself, replayed by hand, never
// clones. A future reviewer must check that defer placement directly.
func TestAccessorDriverSequenceDoesNotClone(t *testing.T) {
	p := snapPath(1)
	s := newSnapStore(t, domain.StateDiffEntry{Path: p, Value: []byte("v0")})

	gen := s.gen
	ptr := reflect.ValueOf(s.leaves).UnsafePointer()

	for i := 0; i < 3; i++ {
		acc := s.Accessor()
		if _, _, err := acc.Read(p); err != nil {
			t.Fatalf("round %d: Read: %v", i, err)
		}
		acc.Witnesses()
		acc.Release()

		if _, err := s.Apply([]domain.StateDiffEntry{{Path: p, Value: []byte{byte(i)}}}); err != nil {
			t.Fatalf("round %d: Apply: %v", i, err)
		}

		if s.gen != gen {
			t.Fatalf("round %d: s.gen changed from %d to %d, want unchanged (no clone)", i, gen, s.gen)
		}
		if got := reflect.ValueOf(s.leaves).UnsafePointer(); got != ptr {
			t.Fatalf("round %d: s.leaves pointer changed, want unchanged (no clone)", i)
		}
		if s.live != 0 {
			t.Fatalf("round %d: s.live = %d, want 0", i, s.live)
		}
	}
}

// TestAccessorReadAfterReleaseFails proves Read and Len return
// ErrAccessorReleased once Release has run, and that Witnesses still
// returns exactly what was recorded beforehand.
func TestAccessorReadAfterReleaseFails(t *testing.T) {
	p := snapPath(1)
	s := newSnapStore(t, domain.StateDiffEntry{Path: p, Value: []byte("v1")})

	acc := s.Accessor()
	if _, _, err := acc.Read(p); err != nil {
		t.Fatalf("Read(p): %v", err)
	}
	acc.Release()

	if _, _, err := acc.Read(p); !errors.Is(err, ErrAccessorReleased) {
		t.Fatalf("Read after Release: err = %v, want ErrAccessorReleased", err)
	}
	if _, _, err := acc.Len(p); !errors.Is(err, ErrAccessorReleased) {
		t.Fatalf("Len after Release: err = %v, want ErrAccessorReleased", err)
	}

	witnesses := acc.Witnesses()
	if len(witnesses) != 1 || witnesses[0].Path != p {
		t.Fatalf("Witnesses() after Release = %+v, want exactly one witness for p", witnesses)
	}
}

// TestAccessorConcurrentReadDuringApply runs one goroutine reading an
// unreleased accessor while the test goroutine applies overwrites to the
// same paths. Every read must see the accessor's pre-state value. Run with
// -race, this also detects any write landing in the accessor's shared map.
func TestAccessorConcurrentReadDuringApply(t *testing.T) {
	p := snapPath(1)
	s := newSnapStore(t, domain.StateDiffEntry{Path: p, Value: []byte("v0")})

	acc := s.Accessor()

	done := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-done:
				return
			default:
			}
			value, present, err := acc.Read(p)
			if err != nil || !present || string(value) != "v0" {
				t.Errorf("Read(p) = %q, %v, %v, want v0, true, nil", value, present, err)
				return
			}
			n, present, err := acc.Len(p)
			if err != nil || !present || n != len("v0") {
				t.Errorf("Len(p) = %d, %v, %v, want %d, true, nil", n, present, err, len("v0"))
				return
			}
		}
	}()

	for i := 0; i < 10; i++ {
		if _, err := s.Apply([]domain.StateDiffEntry{{Path: p, Value: []byte{byte(i)}}}); err != nil {
			t.Fatalf("Apply %d: %v", i, err)
		}
	}
	close(done)
	wg.Wait()
}
