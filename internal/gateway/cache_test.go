package gateway

import (
	"context"
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/zenon-network/go-zenon/common/types"

	"github.com/Zeno-L2/zeno-executor/domain"
	"github.com/Zeno-L2/zeno-executor/internal/codec"
	"github.com/Zeno-L2/zeno-executor/internal/recovery"
	"github.com/Zeno-L2/zeno-executor/internal/state"
)

func mkfile(t *testing.T, path string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
}

// finishesWithin runs fn on a goroutine and fails the test when fn has
// not returned after d, so a call that blocks fails fast with a named
// message instead of hanging to the test timeout. fn must not call t.
func finishesWithin(t *testing.T, d time.Duration, what string, fn func()) {
	t.Helper()
	done := make(chan struct{})
	go func() {
		defer close(done)
		fn()
	}()
	select {
	case <-done:
	case <-time.After(d):
		t.Fatalf("%s did not return within %s: the read deadline is not reaching the read", what, d)
	}
}

func exists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

func TestDiscardCache(t *testing.T) {
	storage := t.TempDir()
	for _, f := range []string{"1/snapshots/a", "1/index/b", "1/bundles/c", "1/receipts/d", "1/state/wal", "2/snapshots/e", "keep.txt"} {
		mkfile(t, filepath.Join(storage, f))
	}
	if err := discardCache(storage, 1); err != nil {
		t.Fatalf("discardCache: %v", err)
	}
	for _, gone := range []string{"1/snapshots", "1/index"} {
		if exists(filepath.Join(storage, gone)) {
			t.Errorf("%s still exists", gone)
		}
	}
	for _, kept := range []string{"1", "1/bundles/c", "1/receipts/d", "1/state/wal", "2/snapshots/e", "keep.txt"} {
		if !exists(filepath.Join(storage, kept)) {
			t.Errorf("%s was removed", kept)
		}
	}
	if err := discardCache(storage, 1); err != nil {
		t.Fatalf("second discardCache: %v", err)
	}
}

// acceptFinal makes the fake accept batches 0 to n with finality
// heights 20 + id.
func acceptFinal(s *fakeSettlement, cs []codec.BatchCommitment, n int) {
	for i := 0; i <= n; i++ {
		s.accept(cs[i], uint64(20+i))
	}
}

func sidecarPath(dir string, c codec.BatchCommitment) string {
	return recovery.ReceiptsPath(layoutOf(dir).Receipts, c.DAHash)
}

func bundlePathOf(dir string, c codec.BatchCommitment) string {
	return recovery.BundlePath(layoutOf(dir).Bundles, c.DAHash)
}

func metaOf(id uint64) []byte { return binary.BigEndian.AppendUint64(nil, id) }

// batch2Leaves is history A's state after batch 2.
func batch2Leaves(t *testing.T, cs []codec.BatchCommitment) []domain.StateDiffEntry {
	t.Helper()
	leaves := []domain.StateDiffEntry{put(pA, "a2"), put(pB, "b2")}
	if root := (state.Version{}).Apply(leaves).Root(); root != cs[2].PostStateRoot {
		t.Fatalf("batch 2 leaves have root %s, want %s", root, cs[2].PostStateRoot)
	}
	return leaves
}

func snapNames(t *testing.T, dir string) []string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, e := range entries {
		names = append(names, e.Name())
	}
	return names
}

func TestOpenChainRestart(t *testing.T) {
	pristine, cs := buildA(t)
	exec := t.TempDir()
	copyDir(t, pristine, exec)
	storage := t.TempDir()
	s := newFakeSettlement()
	s.height = 30
	for i, at := range []uint64{20, 21, 22, 40, 41} {
		s.accept(cs[i], at)
	}

	a, alog := openTestChain(t, storage, exec, s)
	if alog.count("booted at genesis") != 1 {
		t.Fatalf("want a genesis boot line:\n%s", alog.dump())
	}
	if err := a.Poll(context.Background()); err != nil {
		t.Fatalf("Poll: %v", err)
	}
	requireTags(t, a.Head(), 4, 4, 2, StatusOK)
	if alog.count("saved the finalized state at batch 2") != 1 {
		t.Fatalf("want one save line:\n%s", alog.dump())
	}
	snaps := cachePathsFor(storage, 1).snapshots
	infos, err := state.ListSnapshots(snaps)
	if err != nil || len(infos) != 1 {
		t.Fatalf("ListSnapshots = %v, %v; want one snapshot", infos, err)
	}
	if names := snapNames(t, snaps); len(names) != 1 || names[0] != infos[0].Name+".snap" {
		t.Fatalf("snapshots/ = %v, want %s.snap", names, infos[0].Name)
	}
	h := infos[0].Header
	if binary.BigEndian.Uint64(h.Meta) != 2 || len(h.Meta) != 8 || h.InputSeq != cs[2].LastInputSeq || h.STFSpecHash != fixtureSTF {
		t.Fatalf("header = %+v", h)
	}
	closeChain(t, a)

	b, blog := openTestChain(t, storage, exec, s)
	if blog.count("booted from the snapshot of finalized batch 2") != 1 || blog.count("discarding") != 0 {
		t.Fatalf("boot lines:\n%s", blog.dump())
	}
	if f := b.Head().Finalized; f == nil || f.BatchID != 2 {
		t.Fatalf("finalized before poll = %+v, want batch 2", f)
	}
	requireVersionRoot(t, b, 2, cs[2].PostStateRoot)
	if err := b.Poll(context.Background()); err != nil {
		t.Fatalf("Poll: %v", err)
	}
	requireSameVersions(t, a, b, 2, 3, 4)

	c, _ := openTestChain(t, t.TempDir(), exec, s)
	if err := c.Poll(context.Background()); err != nil {
		t.Fatalf("Poll: %v", err)
	}
	requireSameVersions(t, b, c, 2, 3, 4)
}

func TestOpenChainDiscards(t *testing.T) {
	pristine, cs := buildA(t)
	exec := t.TempDir()
	copyDir(t, pristine, exec)

	cases := []struct {
		name, reason string
		leaves       func() []domain.StateDiffEntry
		stf          types.Hash
		meta         []byte
		noEntries    bool
		noRecord     bool
		repoll       bool
	}{
		{name: "different leaf set", reason: "is not batch 2's postStateRoot", repoll: true,
			leaves: func() []domain.StateDiffEntry { return []domain.StateDiffEntry{put(pD, "wrong")} }},
		{name: "no batch id", reason: "carries no batch id", meta: make([]byte, 7)},
		{name: "entry not visible", reason: "entry is not visible", noEntries: true},
		{name: "stfSpecHash changed", reason: "is tagged stfSpecHash", stf: types.Hash{0x01}},
		{name: "no domain record", reason: "domain record is not visible", noRecord: true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			storage := t.TempDir()
			leaves := batch2Leaves(t, cs)
			if c.leaves != nil {
				leaves = c.leaves()
			}
			stf, meta := fixtureSTF, metaOf(2)
			if c.stf != (types.Hash{}) {
				stf = c.stf
			}
			if c.meta != nil {
				meta = c.meta
			}
			snaps := cachePathsFor(storage, 1).snapshots
			snapshotAt(t, snaps, leaves, stf, meta, cs[2].LastInputSeq)

			s := newFakeSettlement()
			s.height = 30
			if !c.noEntries {
				acceptFinal(s, cs, 2)
			}
			if c.noRecord {
				s.record = nil
			}
			chain, log := openTestChain(t, storage, exec, s)
			if log.count("discarding", c.reason) != 1 || log.count("discarding") != 1 {
				t.Fatalf("want one discard line with %q:\n%s", c.reason, log.dump())
			}
			if chain.Head().Finalized != nil {
				t.Fatalf("finalized = %+v, want nil", chain.Head().Finalized)
			}
			if names := snapNames(t, snaps); len(names) != 0 {
				t.Fatalf("snapshots/ = %v, want empty", names)
			}
			if !c.repoll {
				return
			}
			if err := chain.Poll(context.Background()); err != nil {
				t.Fatalf("Poll: %v", err)
			}
			requireTags(t, chain.Head(), 4, 2, 2, StatusOK)
			requireVersionRoot(t, chain, 2, cs[2].PostStateRoot)
			infos, err := state.ListSnapshots(snaps)
			if err != nil || len(infos) != 1 || binary.BigEndian.Uint64(infos[0].Header.Meta) != 2 {
				t.Fatalf("after the poll: snapshots %v, %v; want one tagged 2", infos, err)
			}
			store, err := state.OpenSnapshot(snaps, infos[0].Name, fixtureSTF)
			if err != nil {
				t.Fatal(err)
			}
			defer store.Close()
			if root, _ := store.Root(); root != cs[2].PostStateRoot {
				t.Fatalf("snapshot root %s, want %s", root, cs[2].PostStateRoot)
			}
		})
	}
}

func TestOpenChainKeeps(t *testing.T) {
	pristine, cs := buildA(t)
	exec := t.TempDir()
	copyDir(t, pristine, exec)

	// setup writes the correct snapshot and returns its path.
	setup := func(t *testing.T) (storage, snapFile string, s *fakeSettlement) {
		t.Helper()
		storage = t.TempDir()
		snaps := cachePathsFor(storage, 1).snapshots
		snapshotAt(t, snaps, batch2Leaves(t, cs), fixtureSTF, metaOf(2), cs[2].LastInputSeq)
		s = newFakeSettlement()
		s.height = 30
		acceptFinal(s, cs, 2)
		return storage, filepath.Join(snaps, snapNames(t, snaps)[0]), s
	}
	domainCfg := DomainConfig{DomainID: 1, Source: "LOCAL", ExecutorStoragePath: exec}

	t.Run("settlement error", func(t *testing.T) {
		storage, snap, s := setup(t)
		s.heightErr = errors.New("node down")
		sink := &logSink{}
		_, err := OpenChain(context.Background(), storage, domainCfg, s, &fakeSenders{}, sink.logf)
		if err == nil || !strings.Contains(err.Error(), "settlement") {
			t.Fatalf("err = %v, want a settlement error", err)
		}
		if sink.count("discarding") != 0 || !exists(snap) {
			t.Fatalf("the cache was touched:\n%s", sink.dump())
		}
	})

	t.Run("settlement timeout", func(t *testing.T) {
		storage, snap, s := setup(t)
		hs := newHangingSettlement(t, s)
		hs.hangHeight.Store(true)
		sink := &logSink{}
		var err error
		finishesWithin(t, 2*time.Second, "openFinal", func() {
			_, _, err = openFinal(context.Background(), storage, 1, hs, 20*time.Millisecond, sink.logf)
		})
		if err == nil || !strings.Contains(err.Error(), "settlement") || !errors.Is(err, errNoAnswer) {
			t.Fatalf("err = %v, want a settlement errNoAnswer", err)
		}
		if sink.count("discarding") != 0 || !exists(snap) {
			t.Fatalf("the cache was touched:\n%s", sink.dump())
		}
	})

	t.Run("cancelled boot", func(t *testing.T) {
		storage, snap, s := setup(t)
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		sink := &logSink{}
		_, err := OpenChain(ctx, storage, domainCfg, s, &fakeSenders{}, sink.logf)
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("err = %v, want context.Canceled", err)
		}
		if sink.count("discarding") != 0 || !exists(snap) {
			t.Fatalf("the cache was touched:\n%s", sink.dump())
		}
	})
}

// clock is a test-owned time source.
type clock struct{ t time.Time }

func (c *clock) now() time.Time { return c.t }

func TestChainSnapshotRule(t *testing.T) {
	pristine, cs := buildA(t)
	fresh := func(t *testing.T) string {
		dir := t.TempDir()
		copyDir(t, pristine, dir)
		return dir
	}
	saves := func(r *rig) int { return r.log.count("saved the finalized state") }
	metaIn := func(t *testing.T, dir string) uint64 {
		t.Helper()
		infos, err := state.ListSnapshots(dir)
		if err != nil || len(infos) != 1 {
			t.Fatalf("ListSnapshots(%s) = %v, %v; want one snapshot", dir, infos, err)
		}
		if names := snapNames(t, dir); len(names) != 1 {
			t.Fatalf("snapshots/ = %v, want one entry", names)
		}
		return binary.BigEndian.Uint64(infos[0].Header.Meta)
	}

	t.Run("one poll", func(t *testing.T) {
		s := newFakeSettlement()
		s.height = 30
		acceptFinal(s, cs, 4)
		r := newTestChain(t, fresh(t), s)
		mkfile(t, filepath.Join(r.snaps, "old.snap"))
		mkfile(t, filepath.Join(r.snaps, ".tmp-x"))
		requireTags(t, doPoll(t, r), 4, 4, 4, StatusOK)
		if saves(r) != 1 || r.log.count("saved the finalized state at batch 4") != 1 {
			t.Fatalf("want one save at batch 4:\n%s", r.log.dump())
		}
		if got := metaIn(t, r.snaps); got != 4 {
			t.Fatalf("snapshot tagged %d, want 4", got)
		}
	})

	t.Run("finalization and catch-up in different polls", func(t *testing.T) {
		s := newFakeSettlement()
		s.height = 10
		acceptFinal(s, cs, 2)
		r := newTestChain(t, fresh(t), s)
		doPoll(t, r)
		if saves(r) != 0 {
			t.Fatalf("saved before anything finalized:\n%s", r.log.dump())
		}
		s.height = 30
		s.hidden[1] = true
		requireTags(t, doPoll(t, r), 4, 2, 0, StatusOK)
		if saves(r) != 0 {
			t.Fatalf("saved while finalize was held:\n%s", r.log.dump())
		}
		s.hidden[1] = false
		requireTags(t, doPoll(t, r), 4, 2, 2, StatusOK)
		if saves(r) != 1 || r.log.count("batch 2") == 0 {
			t.Fatalf("want one save at batch 2:\n%s", r.log.dump())
		}
		doPoll(t, r)
		if saves(r) != 1 {
			t.Fatalf("saves = %d, want 1", saves(r))
		}
	})

	t.Run("blocked on a missing sidecar", func(t *testing.T) {
		dir := fresh(t)
		if err := os.Remove(sidecarPath(dir, cs[3])); err != nil {
			t.Fatal(err)
		}
		s := newFakeSettlement()
		s.height = 30
		acceptFinal(s, cs, 3)
		r := newTestChain(t, dir, s)
		requireTags(t, doPoll(t, r), 2, 2, 2, StatusSyncing)
		if saves(r) != 1 || r.log.count("saved the finalized state at batch 2") != 1 {
			t.Fatalf("want one save at batch 2:\n%s", r.log.dump())
		}
		doPoll(t, r)
		if saves(r) != 1 {
			t.Fatalf("saves = %d, want 1", saves(r))
		}
	})

	t.Run("inconsistent", func(t *testing.T) {
		dir := fresh(t)
		flipLastByte(t, bundlePathOf(dir, cs[3]))
		s := newFakeSettlement()
		s.height = 30
		acceptFinal(s, cs, 3)
		r := newTestChain(t, dir, s)
		requireTags(t, doPoll(t, r), 2, 2, 2, StatusInconsistent)
		doPoll(t, r)
		if saves(r) != 0 {
			t.Fatalf("saved an inconsistent domain:\n%s", r.log.dump())
		}
	})

	t.Run("throttle", func(t *testing.T) {
		if snapshotInterval != 10*time.Minute {
			t.Fatalf("snapshotInterval = %s", snapshotInterval)
		}
		s := newFakeSettlement()
		acceptFinal(s, cs, 4)
		r := newTestChain(t, fresh(t), s)
		clk := &clock{t: time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC)}
		t0 := clk.t
		r.c.now = clk.now
		step := func(height uint64, at time.Time) {
			t.Helper()
			s.height = height
			clk.t = at
			if err := r.c.Poll(context.Background()); err != nil {
				t.Fatalf("Poll: %v", err)
			}
		}
		step(20, t0)
		requireTags(t, r.c.Head(), 4, 4, 0, StatusOK)
		if saves(r) != 1 || r.log.count("at batch 0") != 1 {
			t.Fatalf("want one save at batch 0:\n%s", r.log.dump())
		}
		step(21, t0.Add(time.Minute))
		requireTags(t, r.c.Head(), 4, 4, 1, StatusOK)
		if saves(r) != 1 {
			t.Fatalf("saves = %d, want 1", saves(r))
		}
		step(22, t0.Add(10*time.Minute))
		requireTags(t, r.c.Head(), 4, 4, 2, StatusOK)
		if saves(r) != 2 || r.log.count("at batch 2") != 1 || metaIn(t, r.snaps) != 2 {
			t.Fatalf("want a second save at batch 2:\n%s", r.log.dump())
		}
		step(23, t0.Add(10*time.Minute))
		requireTags(t, r.c.Head(), 4, 4, 3, StatusOK)
		if saves(r) != 2 || metaIn(t, r.snaps) != 2 {
			t.Fatalf("saves = %d, want 2 and a snapshot tagged 2", saves(r))
		}
		step(23, t0.Add(20*time.Minute))
		if saves(r) != 3 || r.log.count("at batch 3") != 1 || metaIn(t, r.snaps) != 3 {
			t.Fatalf("want a third save at batch 3:\n%s", r.log.dump())
		}
	})
}
