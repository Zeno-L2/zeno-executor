package recovery

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/zenon-network/go-zenon/chain/nom"
	"github.com/zenon-network/go-zenon/common/types"

	"github.com/digitalSloth/go-executor/domain"
	"github.com/digitalSloth/go-executor/internal/state"
)

// buildTree commits every height 1..n one at a time on the salt-0 chain and closes the store,
// leaving a tree a Boot can load.
func buildTree(t *testing.T, depth, n uint64) Config {
	t.Helper()
	cfg := testConfig(t, depth)
	c := newCommitter(t, cfg, 0)
	c.commitEach(1, 1, n)
	c.store.Close()
	return cfg
}

func mustBoot(t *testing.T, cfg Config) Ready {
	t.Helper()
	rdy, err := Boot(cfg)
	if err != nil {
		t.Fatalf("Boot: %v", err)
	}
	t.Cleanup(func() { rdy.Store.Close() })
	return rdy
}

func mustHalt(t *testing.T, cfg Config) error {
	t.Helper()
	rdy, err := Boot(cfg)
	if err == nil {
		rdy.Store.Close()
		t.Fatalf("Boot succeeded, want ErrHalted")
	}
	if !errors.Is(err, ErrHalted) {
		t.Fatalf("Boot error = %v, want one wrapping ErrHalted", err)
	}
	return err
}

func mustBeUnavailable(t *testing.T, cfg Config) error {
	t.Helper()
	rdy, err := Boot(cfg)
	if err == nil {
		rdy.Store.Close()
		t.Fatalf("Boot succeeded, want ErrSourceUnavailable")
	}
	if !errors.Is(err, ErrSourceUnavailable) {
		t.Fatalf("Boot error = %v, want one wrapping ErrSourceUnavailable", err)
	}
	if errors.Is(err, ErrHalted) {
		t.Fatalf("Boot error = %v, wraps ErrHalted", err)
	}
	return err
}

func storeRoot(t *testing.T, s *state.Store) types.Hash {
	t.Helper()
	r, err := s.Root()
	if err != nil {
		t.Fatal(err)
	}
	return r
}

// writeSnapshotAt hand-writes a snapshot whose embedded position is pos and whose leaf set is
// the single leaf named by leaf, the way a crash orphan or a substituted file looks on disk.
func writeSnapshotAt(t *testing.T, cfg Config, pos Position, leaf uint64, stf types.Hash) string {
	t.Helper()
	s, err := state.OpenEmpty(filepath.Join(t.TempDir(), "wal"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if _, err := s.Apply([]domain.StateDiffEntry{{Path: leafPath(leaf), Value: []byte("other")}}); err != nil {
		t.Fatal(err)
	}
	meta, err := json.Marshal(pos)
	if err != nil {
		t.Fatal(err)
	}
	name, err := s.SaveSnapshot(cfg.Layout.Snapshots, state.SnapshotHeader{InputSeq: pos.LastInputSeq, STFSpecHash: stf, Meta: meta})
	if err != nil {
		t.Fatal(err)
	}
	return name
}

func TestBootFresh(t *testing.T) {
	cfg := testConfig(t, 4)
	cfg.Domain = fakeDomain{genesis: domain.StateInit{
		Paths:  []domain.Path{leafPath(1), leafPath(2)},
		Values: [][]byte{[]byte("a"), []byte("b")},
	}}
	rdy := mustBoot(t, cfg)

	if !rdy.Fresh || rdy.Position != (Position{}) || rdy.HaveAnchor || rdy.VerifyAnchor {
		t.Fatalf("Ready = %+v, want fresh, zero position, no anchor", rdy)
	}
	want, err := state.OpenEmpty(filepath.Join(t.TempDir(), "wal"))
	if err != nil {
		t.Fatal(err)
	}
	defer want.Close()
	if _, err := want.Apply([]domain.StateDiffEntry{{Path: leafPath(1), Value: []byte("a")}, {Path: leafPath(2), Value: []byte("b")}}); err != nil {
		t.Fatal(err)
	}
	if storeRoot(t, rdy.Store) != storeRoot(t, want) {
		t.Fatalf("genesis leaves did not land in the store")
	}

	t.Run("mismatched StateInit", func(t *testing.T) {
		bad := testConfig(t, 4)
		bad.Domain = fakeDomain{genesis: domain.StateInit{Paths: []domain.Path{leafPath(1)}}}
		if rdy, err := Boot(bad); err == nil {
			rdy.Store.Close()
			t.Fatalf("Boot accepted a StateInit with unequal Paths and Values")
		}
	})
}

func TestBootAfterCommit(t *testing.T) {
	cfg := buildTree(t, 4, 3)
	cfg.Momenta = newChainSource(6, 0)
	want, _, _ := ReadCursor(cfg.Layout)

	rdy := mustBoot(t, cfg)
	if rdy.Fresh || rdy.Position != want {
		t.Fatalf("Position = %+v (fresh %v), want %+v", rdy.Position, rdy.Fresh, want)
	}
	if storeRoot(t, rdy.Store) != want.PostStateRoot {
		t.Fatalf("loaded store root differs from the cursor's postStateRoot")
	}
}

func TestBootRejectsAnotherIdentity(t *testing.T) {
	cfg := buildTree(t, 4, 2)
	cfg.Momenta = newChainSource(6, 0)
	mustBoot(t, cfg)
	cfg.ExecutorID = "someone-else"
	mustHalt(t, cfg)
}

func TestBootRejectsMalformedCursor(t *testing.T) {
	allZero, err := json.Marshal(Position{})
	if err != nil {
		t.Fatal(err)
	}
	for name, body := range map[string]string{
		"null":            "null",
		"empty object":    "{}",
		"all zero fields": string(allZero),
	} {
		t.Run(name, func(t *testing.T) {
			cfg := buildTree(t, 4, 3)
			cfg.Momenta = newChainSource(6, 0)

			batchesBefore, err := ListBatchRecords(cfg.Layout)
			if err != nil {
				t.Fatal(err)
			}
			snapsBefore := snapshotHeights(t, cfg.Layout)
			windowBefore := windowHeights(t, cfg.Layout)

			if err := os.WriteFile(cfg.Layout.Cursor, []byte(body), 0o644); err != nil {
				t.Fatal(err)
			}

			err = mustHalt(t, cfg)
			if !strings.Contains(err.Error(), "cursor") {
				t.Fatalf("error %q does not name the cursor", err)
			}

			batchesAfter, err := ListBatchRecords(cfg.Layout)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(batchesBefore, batchesAfter) {
				t.Fatalf("ListBatchRecords changed: %v -> %v", batchesBefore, batchesAfter)
			}
			if got := snapshotHeights(t, cfg.Layout); !reflect.DeepEqual(got, snapsBefore) {
				t.Fatalf("ListSnapshots changed: %v -> %v", snapsBefore, got)
			}
			if got := windowHeights(t, cfg.Layout); !reflect.DeepEqual(got, windowBefore) {
				t.Fatalf("ReadMomentumWindow changed: %v -> %v", windowBefore, got)
			}
		})
	}
}

func TestBootSnapshotSubstitution(t *testing.T) {
	t.Run("corrupted file", func(t *testing.T) {
		cfg := buildTree(t, 4, 3)
		cfg.Momenta = newChainSource(6, 0)
		snaps, _ := listPositioned(cfg.Layout)
		file := filepath.Join(cfg.Layout.Snapshots, snaps[len(snaps)-1].info.Name+snapExt)
		body, _ := os.ReadFile(file)
		body[len(body)-1] ^= 0xFF
		if err := os.WriteFile(file, body, 0o644); err != nil {
			t.Fatal(err)
		}
		mustHalt(t, cfg)
	})

	t.Run("valid file with another leaf set", func(t *testing.T) {
		cfg := buildTree(t, 4, 3)
		cfg.Momenta = newChainSource(6, 0)
		pos, _, _ := ReadCursor(cfg.Layout)
		snaps, _ := listPositioned(cfg.Layout)
		if err := os.Remove(filepath.Join(cfg.Layout.Snapshots, snaps[len(snaps)-1].info.Name+snapExt)); err != nil {
			t.Fatal(err)
		}
		writeSnapshotAt(t, cfg, pos, 0x99, testSTF)
		err := mustHalt(t, cfg)
		if !strings.Contains(err.Error(), "rooted differently") {
			t.Fatalf("error %q does not name the root mismatch", err)
		}
	})

	t.Run("snapshot tagged for another stfSpecHash", func(t *testing.T) {
		cfg := buildTree(t, 4, 3)
		cfg.Momenta = newChainSource(6, 0)
		cfg.STFSpecHash = types.Hash{0xBB}
		err := mustHalt(t, cfg)
		if !strings.Contains(err.Error(), "stfSpecHash") {
			t.Fatalf("error %q does not name the stfSpecHash tag", err)
		}
	})
}

func TestReconcileBranches(t *testing.T) {
	rootA, rootB := types.Hash{0xA1}, types.Hash{0xB2}

	t.Run("no anchor", func(t *testing.T) {
		cfg := testConfig(t, 4)
		verify, err := Reconcile(cfg, Anchor{}, false, Position{LastInputSeq: 3}, false)
		if err != nil || verify {
			t.Fatalf("= %v, %v", verify, err)
		}
	})

	t.Run("fresh position with an anchor at index 0 is local-absent", func(t *testing.T) {
		cfg := testConfig(t, 4)
		verify, err := Reconcile(cfg, Anchor{LastInputSeq: 0, PostStateRoot: rootA}, true, Position{}, true)
		if err != nil || !verify {
			t.Fatalf("= %v, %v; want verify without comparing roots", verify, err)
		}
	})

	t.Run("anchor at the local position", func(t *testing.T) {
		cfg := testConfig(t, 4)
		pos := Position{LastInputSeq: 3, PostStateRoot: rootA}
		if verify, err := Reconcile(cfg, Anchor{LastInputSeq: 3, PostStateRoot: rootA}, true, pos, false); err != nil || verify {
			t.Fatalf("matching = %v, %v", verify, err)
		}
		if _, err := Reconcile(cfg, Anchor{LastInputSeq: 3, PostStateRoot: rootB}, true, pos, false); !errors.Is(err, ErrHalted) {
			t.Fatalf("differing root = %v, want ErrHalted", err)
		}
	})

	t.Run("anchor above the local position", func(t *testing.T) {
		cfg := testConfig(t, 4)
		verify, err := Reconcile(cfg, Anchor{LastInputSeq: 9, PostStateRoot: rootB}, true, Position{LastInputSeq: 3, PostStateRoot: rootA}, false)
		if err != nil || !verify {
			t.Fatalf("= %v, %v", verify, err)
		}
	})

	t.Run("anchor below the local position", func(t *testing.T) {
		pos := Position{LastInputSeq: 20, PostStateRoot: rootB}
		setup := func(t *testing.T) (Config, BatchRecord) {
			cfg := testConfig(t, 4)
			rec := BatchRecord{BatchID: 1, Status: StatusBuilt, FirstInputSeq: 4, LastInputSeq: 9, DAHash: bundleFor(t, cfg, 6, rootA)}
			for _, r := range []BatchRecord{
				{BatchID: 0, Status: StatusBuilt, FirstInputSeq: 0, LastInputSeq: 3},
				rec,
				{BatchID: 2, Status: StatusBuilt, FirstInputSeq: 12, LastInputSeq: 20},
			} {
				if err := writeJSON(batchPath(cfg.Layout, r.BatchID), r); err != nil {
					t.Fatal(err)
				}
			}
			return cfg, rec
		}

		t.Run("strictly inside a record, matching root", func(t *testing.T) {
			cfg, _ := setup(t)
			if verify, err := Reconcile(cfg, Anchor{LastInputSeq: 6, PostStateRoot: rootA}, true, pos, false); err != nil || verify {
				t.Fatalf("= %v, %v", verify, err)
			}
		})
		t.Run("strictly inside a record, differing root", func(t *testing.T) {
			cfg, _ := setup(t)
			if _, err := Reconcile(cfg, Anchor{LastInputSeq: 6, PostStateRoot: rootB}, true, pos, false); !errors.Is(err, ErrHalted) {
				t.Fatalf("= %v, want ErrHalted", err)
			}
		})
		t.Run("between two records' ranges", func(t *testing.T) {
			cfg, _ := setup(t)
			if _, err := Reconcile(cfg, Anchor{LastInputSeq: 10, PostStateRoot: rootA}, true, pos, false); !errors.Is(err, ErrHalted) {
				t.Fatalf("= %v, want ErrHalted", err)
			}
		})
		t.Run("containing record's bundle deleted", func(t *testing.T) {
			cfg, rec := setup(t)
			if err := os.Remove(cfg.Bundles.path(rec.DAHash)); err != nil {
				t.Fatal(err)
			}
			if _, err := Reconcile(cfg, Anchor{LastInputSeq: 6, PostStateRoot: rootA}, true, pos, false); !errors.Is(err, ErrHalted) {
				t.Fatalf("= %v, want ErrHalted", err)
			}
		})
	})
}

func TestBootAnchorReadFailure(t *testing.T) {
	cfg := buildTree(t, 4, 2)
	cfg.Momenta = newChainSource(6, 0)
	anchors := &fakeAnchors{err: errors.New("down")}
	cfg.Anchors = anchors
	mustBeUnavailable(t, cfg)
	if anchors.calls != cfg.Retry.SourceAttempts {
		t.Fatalf("anchor read attempted %d times, want %d", anchors.calls, cfg.Retry.SourceAttempts)
	}

	anchors = &fakeAnchors{err: ErrHalted}
	cfg.Anchors = anchors
	mustHalt(t, cfg)
	if anchors.calls != 1 {
		t.Fatalf("a halting anchor error was retried: %d calls", anchors.calls)
	}
}

func TestBootAwaitDA(t *testing.T) {
	cfg := buildTree(t, 4, 2)
	cfg.Momenta = newChainSource(6, 0)
	recs, _ := ListBatchRecords(cfg.Layout)
	bundlePath := cfg.Bundles.path(recs[1].DAHash)
	saved, err := os.ReadFile(bundlePath)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(bundlePath); err != nil {
		t.Fatal(err)
	}

	err = mustHalt(t, cfg)
	if !strings.Contains(err.Error(), "AWAIT_DA") {
		t.Fatalf("error %q does not name AWAIT_DA", err)
	}

	if err := os.WriteFile(bundlePath, saved, 0o644); err != nil {
		t.Fatal(err)
	}
	mustBoot(t, cfg)
}

func TestBootContinuity(t *testing.T) {
	const depth = 4

	t.Run("unchanged chain", func(t *testing.T) {
		cfg := buildTree(t, depth, 8)
		cfg.Momenta = newChainSource(12, 0)
		want, _, _ := ReadCursor(cfg.Layout)
		if rdy := mustBoot(t, cfg); rdy.Position != want {
			t.Fatalf("Position = %+v, want %+v", rdy.Position, want)
		}
	})

	t.Run("rewritten inside the window rolls back", func(t *testing.T) {
		cfg := buildTree(t, depth, 8) // window 4..8, snapshots at 3, 7, 8
		src := newChainSource(12, 0)
		src.rewrite(6, 1)
		cfg.Momenta = src
		rdy := mustBoot(t, cfg)
		if rdy.Position.MomentumHeight != 3 {
			t.Fatalf("rolled back to height %d, want the newest snapshot at or below the fork parent 5, which is 3", rdy.Position.MomentumHeight)
		}
		if cur, _, _ := ReadCursor(cfg.Layout); cur != rdy.Position {
			t.Fatalf("cursor %+v does not match the rolled-back position", cur)
		}
	})

	t.Run("rewritten below the whole window halts", func(t *testing.T) {
		cfg := buildTree(t, depth, 8)
		src := newChainSource(12, 0)
		src.rewrite(1, 1)
		cfg.Momenta = src
		mustHalt(t, cfg)
	})

	t.Run("frontier below the window top", func(t *testing.T) {
		cfg := buildTree(t, depth, 8)
		src := newChainSource(6, 0)
		cfg.Momenta = src
		before, _, _ := ReadCursor(cfg.Layout)
		err := mustBeUnavailable(t, cfg)
		if !strings.Contains(err.Error(), "frontier 6") || !strings.Contains(err.Error(), "top height 8") {
			t.Fatalf("error %q does not name the frontier and the window's top", err)
		}
		if after, _, _ := ReadCursor(cfg.Layout); after != before {
			t.Fatalf("cursor moved on an unavailable source")
		}
	})

	t.Run("ranged read returns nothing", func(t *testing.T) {
		cfg := buildTree(t, depth, 8)
		src := newChainSource(12, 0)
		src.dropReads = true
		cfg.Momenta = src
		before, _, _ := ReadCursor(cfg.Layout)
		err := mustBeUnavailable(t, cfg)
		if !strings.Contains(err.Error(), "frontier 12") {
			t.Fatalf("error %q does not name the lag", err)
		}
		if after, _, _ := ReadCursor(cfg.Layout); after != before {
			t.Fatalf("cursor moved on an unavailable source")
		}
	})
}

func TestBootPrunesAboveLoadedPosition(t *testing.T) {
	strays := func(t *testing.T, cfg Config) {
		t.Helper()
		writeSnapshotAt(t, cfg, Position{LastInputSeq: 9, MomentumHeight: 9, NextBatchID: 10}, 0x55, testSTF)
		if err := writeJSON(batchPath(cfg.Layout, 9), BatchRecord{BatchID: 9, Status: StatusBuilt}); err != nil {
			t.Fatal(err)
		}
		var w []MomentumRef
		for h := uint64(1); h <= 9; h++ {
			w = append(w, MomentumRef{Height: h, Hash: canonHash(h, 0)})
		}
		if err := writeMomentumWindow(cfg.Layout, w); err != nil {
			t.Fatal(err)
		}
	}

	t.Run("existing cursor", func(t *testing.T) {
		cfg := buildTree(t, 4, 3)
		strays(t, cfg)
		cfg.Momenta = newChainSource(9, 0)
		mustBoot(t, cfg)

		if got := snapshotHeights(t, cfg.Layout); !reflect.DeepEqual(got, []uint64{3}) {
			t.Fatalf("snapshots = %v, want only the cursor's", got)
		}
		recs, _ := ListBatchRecords(cfg.Layout)
		if len(recs) != 3 {
			t.Fatalf("%d batch records survive, want 3", len(recs))
		}
		if got := windowHeights(t, cfg.Layout); !reflect.DeepEqual(got, seq(1, 3)) {
			t.Fatalf("window = %v", got)
		}
	})

	t.Run("fresh position", func(t *testing.T) {
		cfg := testConfig(t, 4)
		strays(t, cfg)
		mustBoot(t, cfg)

		recs, _ := ListBatchRecords(cfg.Layout)
		if len(recs) != 0 || len(snapshotHeights(t, cfg.Layout)) != 0 || len(windowHeights(t, cfg.Layout)) != 0 {
			t.Fatalf("artifacts survive a fresh boot")
		}
	})
}

func TestTwoRollbacksInSequence(t *testing.T) {
	cfg := buildTree(t, 4, 8) // salt 0: snapshots at 3, 7, 8

	first, err := Rollback(cfg, 5)
	if err != nil {
		t.Fatalf("first Rollback: %v", err)
	}
	if first.Position.MomentumHeight != 3 {
		t.Fatalf("first rollback landed at %d, want 3", first.Position.MomentumHeight)
	}
	if got := snapshotHeights(t, cfg.Layout); !reflect.DeepEqual(got, []uint64{3}) {
		t.Fatalf("snapshots after the first rollback = %v, want the retracted branch's deleted", got)
	}

	// A second chain whose momenta carry twice the inputs.
	c := committerOn(t, cfg, first.Store, 1, 2)
	for h := uint64(4); h <= 8; h++ {
		c.commit(h, c.ring(4, h))
	}

	second, err := Rollback(cfg, 7)
	if err != nil {
		t.Fatalf("second Rollback: %v", err)
	}
	defer second.Store.Close()
	if second.Position.MomentumHeight != 7 || second.Position.LastInputSeq != 14 ||
		second.Position.MomentumHash != canonHash(7, 1) {
		t.Fatalf("second rollback landed at %+v, want the second chain's position at height 7", second.Position)
	}
	if cur, _, _ := ReadCursor(cfg.Layout); cur != second.Position {
		t.Fatalf("cursor %+v does not match the rolled-back position", cur)
	}

	none, err := Rollback(cfg, 0)
	if err != nil {
		t.Fatalf("Rollback to genesis: %v", err)
	}
	none.Store.Close()
	if !none.Fresh {
		t.Fatalf("Rollback with no qualifying snapshot is not Fresh")
	}
	if _, ok, _ := ReadCursor(cfg.Layout); ok {
		t.Fatalf("cursor survives a rollback to genesis")
	}
	cfg.Momenta = newChainSource(9, 1)
	if rdy := mustBoot(t, cfg); !rdy.Fresh {
		t.Fatalf("Boot after a rollback to genesis is not Fresh")
	}
}

func TestRollbackLeavesAnchoredWindow(t *testing.T) {
	const depth = 4
	newTree := func(t *testing.T) Config {
		cfg := buildTree(t, depth, 12) // window 8..12, snapshots {7, 11, 12}
		if got := windowHeights(t, cfg.Layout); !reflect.DeepEqual(got, seq(8, 12)) {
			t.Fatalf("window = %v", got)
		}
		if got := snapshotHeights(t, cfg.Layout); !reflect.DeepEqual(got, []uint64{7, 11, 12}) {
			t.Fatalf("snapshots = %v", got)
		}
		return cfg
	}

	t.Run("a: target inside the window keeps the survivors", func(t *testing.T) {
		cfg := newTree(t)
		rdy, err := Rollback(cfg, 11)
		if err != nil {
			t.Fatal(err)
		}
		defer rdy.Store.Close()
		if got := windowHeights(t, cfg.Layout); !reflect.DeepEqual(got, seq(8, 11)) {
			t.Fatalf("window = %v, want [8..11]", got)
		}
		w, _ := ReadMomentumWindow(cfg.Layout)
		if last := w[len(w)-1]; last != (MomentumRef{Height: rdy.Position.MomentumHeight, Hash: rdy.Position.MomentumHash}) {
			t.Fatalf("window's last entry %+v is not the new cursor's pair", last)
		}
	})

	t.Run("b: target below the window writes the single entry", func(t *testing.T) {
		cfg := newTree(t)
		rdy, err := Rollback(cfg, 9)
		if err != nil {
			t.Fatal(err)
		}
		rdy.Store.Close()
		w, _ := ReadMomentumWindow(cfg.Layout)
		if want := []MomentumRef{{Height: 7, Hash: canonHash(7, 0)}}; !reflect.DeepEqual(w, want) {
			t.Fatalf("window = %v, want %v", w, want)
		}
		if w[0].Hash != rdy.Position.MomentumHash {
			t.Fatalf("window hash is not the rolled-back momentum's")
		}

		// The crash-and-reboot case, from (b)'s on-disk state.
		forked := newChainSource(12, 0)
		forked.rewrite(7, 1)
		cfg.Momenta = forked
		mustHalt(t, cfg) // c

		if err := writeMomentumWindow(cfg.Layout, nil); err != nil {
			t.Fatal(err)
		}
		mustHalt(t, cfg) // e: the cursor supplies the entry

		cfg.Momenta = newChainSource(12, 0)
		if got := mustBoot(t, cfg); got.Position.MomentumHeight != 7 { // d
			t.Fatalf("Boot resumed at %d, want 7", got.Position.MomentumHeight)
		}
	})
}

func TestCrashOrphanIsNotARollbackTarget(t *testing.T) {
	cfg := buildTree(t, 4, 8) // window 4..8, snapshots {3, 7, 8}
	writeSnapshotAt(t, cfg, Position{LastInputSeq: 12, MomentumHeight: 12, NextBatchID: 13}, 0x66, testSTF)
	if err := writeJSON(batchPath(cfg.Layout, 12), BatchRecord{BatchID: 12, Status: StatusBuilt}); err != nil {
		t.Fatal(err)
	}
	src := newChainSource(12, 0)
	src.rewrite(6, 1)
	cfg.Momenta = src

	rdy := mustBoot(t, cfg)
	if rdy.Position.MomentumHeight != 3 {
		t.Fatalf("rolled back to %d, want 3", rdy.Position.MomentumHeight)
	}
	if got := snapshotHeights(t, cfg.Layout); !reflect.DeepEqual(got, []uint64{3}) {
		t.Fatalf("snapshots = %v, want the orphan gone", got)
	}
	recs, _ := ListBatchRecords(cfg.Layout)
	for _, r := range recs {
		if r.BatchID >= rdy.Position.NextBatchID {
			t.Fatalf("batch record %d above the rolled-back position survives", r.BatchID)
		}
	}
	w, _ := ReadMomentumWindow(cfg.Layout)
	if want := []MomentumRef{{Height: 3, Hash: canonHash(3, 0)}}; !reflect.DeepEqual(w, want) {
		t.Fatalf("window = %v, want %v", w, want)
	}
}

func TestInterruptedCommitWindowIsStillChecked(t *testing.T) {
	cfg := buildTree(t, 4, 12) // window 8..12, cursor at 12

	// A later commit's artifacts, written without renaming the cursor.
	writeSnapshotAt(t, cfg, Position{LastInputSeq: 17, MomentumHeight: 17, NextBatchID: 18}, 0x77, testSTF)
	if err := writeJSON(batchPath(cfg.Layout, 17), BatchRecord{BatchID: 17, Status: StatusBuilt}); err != nil {
		t.Fatal(err)
	}
	var ring []MomentumRef
	for h := uint64(13); h <= 17; h++ {
		ring = append(ring, MomentumRef{Height: h, Hash: canonHash(h, 0)})
	}
	if err := writeMomentumWindow(cfg.Layout, ring); err != nil {
		t.Fatal(err)
	}

	forked := newChainSource(17, 0)
	forked.rewrite(12, 1)
	cfg.Momenta = forked
	mustHalt(t, cfg)
	if got := windowHeights(t, cfg.Layout); len(got) != 0 {
		t.Fatalf("window = %v, want empty after the boot prune", got)
	}
	if cur, _, _ := ReadCursor(cfg.Layout); cur.MomentumHeight != 12 {
		t.Fatalf("cursor = %+v, want it still at 12", cur)
	}

	cfg.Momenta = newChainSource(17, 0)
	if rdy := mustBoot(t, cfg); rdy.Position.MomentumHeight != 12 {
		t.Fatalf("Boot resumed at %d, want the cursor's 12", rdy.Position.MomentumHeight)
	}
}

// flakyMomenta fails its first failures FrontierHeight calls, then serves inner.
type flakyMomenta struct {
	inner    *chainSource
	failures int
	calls    int
}

func (f *flakyMomenta) FrontierHeight() (uint64, error) {
	f.calls++
	if f.calls <= f.failures {
		return 0, errors.New("znnd down")
	}
	return f.inner.FrontierHeight()
}

func (f *flakyMomenta) DetailedMomentumsByHeight(h, n uint64) ([]*nom.DetailedMomentum, error) {
	return f.inner.DetailedMomentumsByHeight(h, n)
}

func TestForkParentUnavailableSource(t *testing.T) {
	const depth = 4
	pos := func(cfg Config) Position {
		p, _, _ := ReadCursor(cfg.Layout)
		return p
	}

	t.Run("a transient failure inside the budget is retried", func(t *testing.T) {
		cfg := buildTree(t, depth, 8)
		cfg.Momenta = &flakyMomenta{inner: newChainSource(12, 0), failures: cfg.Retry.SourceAttempts - 1}
		if h, ok, err := ForkParent(cfg, pos(cfg)); err != nil || !ok || h != 8 {
			t.Fatalf("ForkParent = %d, %v, %v; want 8, true, nil", h, ok, err)
		}
	})

	t.Run("failures past the budget are unavailable", func(t *testing.T) {
		cfg := buildTree(t, depth, 8)
		src := &flakyMomenta{inner: newChainSource(12, 0), failures: cfg.Retry.SourceAttempts}
		cfg.Momenta = src
		_, ok, err := ForkParent(cfg, pos(cfg))
		if ok || !errors.Is(err, ErrSourceUnavailable) || errors.Is(err, ErrHalted) {
			t.Fatalf("ForkParent = ok %v, err %v; want ErrSourceUnavailable, not ErrHalted", ok, err)
		}
		if src.calls != cfg.Retry.SourceAttempts {
			t.Fatalf("source read %d times, want %d", src.calls, cfg.Retry.SourceAttempts)
		}
	})

	t.Run("a halting source error is not retried", func(t *testing.T) {
		cfg := buildTree(t, depth, 8)
		src := newChainSource(12, 0)
		src.readErr = ErrHalted
		cfg.Momenta = src
		if _, _, err := ForkParent(cfg, pos(cfg)); !errors.Is(err, ErrHalted) {
			t.Fatalf("ForkParent = %v, want ErrHalted", err)
		}
		if src.calls != 1 {
			t.Fatalf("a halting error was retried: %d calls", src.calls)
		}
	})

	t.Run("a ranged read missing a middle height is unavailable, not forked", func(t *testing.T) {
		cfg := buildTree(t, depth, 8) // window 4..8
		src := newChainSource(12, 0)
		delete(src.hashes, 6)
		cfg.Momenta = src
		_, ok, err := ForkParent(cfg, pos(cfg))
		if ok || !errors.Is(err, ErrSourceUnavailable) || errors.Is(err, ErrHalted) || !strings.Contains(err.Error(), "height 6") {
			t.Fatalf("ForkParent = ok %v, err %v; want ErrSourceUnavailable naming height 6, not ErrHalted", ok, err)
		}
	})

	t.Run("a position at momentum height 0 is a caller error", func(t *testing.T) {
		cfg := testConfig(t, depth)
		cfg.Momenta = newChainSource(4, 0)
		_, ok, err := ForkParent(cfg, Position{})
		if ok || err == nil || errors.Is(err, ErrHalted) {
			t.Fatalf("ForkParent = ok %v, err %v; want a plain error", ok, err)
		}
	})
}

func TestRollbackWindowAnchoring(t *testing.T) {
	const depth = 4

	t.Run("survivors ending at the cursor's height with another hash are replaced by the cursor's entry", func(t *testing.T) {
		cfg := buildTree(t, depth, 8) // window 4..8, snapshots {3, 7, 8}
		bad := []MomentumRef{{Height: 6, Hash: canonHash(6, 0)}, {Height: 7, Hash: canonHash(7, 9)}}
		if err := writeMomentumWindow(cfg.Layout, bad); err != nil {
			t.Fatal(err)
		}
		rdy, err := Rollback(cfg, 7)
		if err != nil {
			t.Fatal(err)
		}
		rdy.Store.Close()
		w, _ := ReadMomentumWindow(cfg.Layout)
		if want := []MomentumRef{{Height: 7, Hash: canonHash(7, 0)}}; !reflect.DeepEqual(w, want) {
			t.Fatalf("window = %v, want %v", w, want)
		}
	})

	t.Run("a rollback to the cursor's own height keeps the full window", func(t *testing.T) {
		cfg := buildTree(t, depth, 8)
		rdy, err := Rollback(cfg, 8)
		if err != nil {
			t.Fatal(err)
		}
		defer rdy.Store.Close()
		if got := windowHeights(t, cfg.Layout); !reflect.DeepEqual(got, seq(4, 8)) {
			t.Fatalf("window = %v, want 4..8 unchanged", got)
		}
		if rdy.Position.MomentumHeight != 8 {
			t.Fatalf("Position = %+v", rdy.Position)
		}
	})

	t.Run("snapshots tagged with another stfSpecHash are not targets", func(t *testing.T) {
		cfg := buildTree(t, depth, 8)
		cfg.STFSpecHash = types.Hash{0xBB}
		rdy, err := Rollback(cfg, 8)
		if err != nil {
			t.Fatal(err)
		}
		rdy.Store.Close()
		if !rdy.Fresh {
			t.Fatalf("Rollback chose a snapshot it cannot load: %+v", rdy.Position)
		}
		if _, ok, _ := ReadCursor(cfg.Layout); ok {
			t.Fatalf("cursor survives a rollback to genesis")
		}
	})
}
