package recovery

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/zenon-network/go-zenon/chain/nom"
	"github.com/zenon-network/go-zenon/common/crypto"
	"github.com/zenon-network/go-zenon/common/trie"
	"github.com/zenon-network/go-zenon/common/types"

	"github.com/digitalSloth/go-executor/domain"
	"github.com/digitalSloth/go-executor/internal/codec"
	"github.com/digitalSloth/go-executor/internal/state"
)

// --- fixtures ---

var testSTF = types.Hash{0xAA}

// fakeDomain is a domain.Domain whose only behaviour is its genesis.
type fakeDomain struct{ genesis domain.StateInit }

func (d fakeDomain) Genesis() domain.StateInit { return d.genesis }
func (d fakeDomain) DecodeInput(domain.InputContext, []byte) (domain.Input, error) {
	return nil, errors.New("fakeDomain: DecodeInput is not scripted")
}
func (d fakeDomain) Apply(domain.StateAccess, domain.Input) (domain.Effects, error) {
	return domain.Effects{}, errors.New("fakeDomain: Apply is not scripted")
}

// chainSource is a MomentumSource serving a height-to-hash map, with a settable frontier.
type chainSource struct {
	hashes   map[uint64]types.Hash
	frontier uint64
	// dropReads makes DetailedMomentumsByHeight return nothing.
	dropReads bool
	// readErr, when set, fails every read.
	readErr error
	calls   int
}

func (s *chainSource) FrontierHeight() (uint64, error) {
	s.calls++
	if s.readErr != nil {
		return 0, s.readErr
	}
	return s.frontier, nil
}

func (s *chainSource) DetailedMomentumsByHeight(height, count uint64) ([]*nom.DetailedMomentum, error) {
	if s.readErr != nil {
		return nil, s.readErr
	}
	if s.dropReads {
		return nil, nil
	}
	var out []*nom.DetailedMomentum
	for h := height; h < height+count; h++ {
		hash, ok := s.hashes[h]
		if !ok {
			break
		}
		out = append(out, &nom.DetailedMomentum{Momentum: &nom.Momentum{Height: h, Hash: hash}})
	}
	return out, nil
}

// canonHash is momentum h's hash on the chain with the given salt.
func canonHash(h uint64, salt byte) types.Hash {
	var out types.Hash
	binary.BigEndian.PutUint64(out[:8], h)
	out[31] = salt
	return out
}

// newChainSource serves heights 1..top on the salt chain, frontier at top.
func newChainSource(top uint64, salt byte) *chainSource {
	s := &chainSource{hashes: map[uint64]types.Hash{}, frontier: top}
	for h := uint64(1); h <= top; h++ {
		s.hashes[h] = canonHash(h, salt)
	}
	return s
}

// rewrite replaces every hash from height `from` up to the frontier with the salt chain.
func (s *chainSource) rewrite(from uint64, salt byte) {
	for h := from; h <= s.frontier; h++ {
		s.hashes[h] = canonHash(h, salt)
	}
}

type fakeAnchors struct {
	anchor Anchor
	have   bool
	err    error
	calls  int
}

func (a *fakeAnchors) FinalizedAnchor(uint32) (Anchor, bool, error) {
	a.calls++
	return a.anchor, a.have, a.err
}

// testConfig builds a Config over a fresh tree with the shrunken retry schedule tests need.
func testConfig(t *testing.T, depth uint64) Config {
	t.Helper()
	l, err := NewLayout(t.TempDir(), 1)
	if err != nil {
		t.Fatalf("NewLayout: %v", err)
	}
	bundles, err := NewBundleStore(l.Bundles)
	if err != nil {
		t.Fatalf("NewBundleStore: %v", err)
	}
	return Config{
		Layout:            l,
		DomainID:          1,
		ExecutorID:        "exec-1",
		Role:              "EXECUTOR",
		STFSpecHash:       testSTF,
		ConfirmationDepth: depth,
		Domain:            fakeDomain{},
		Momenta:           newChainSource(0, 0),
		Anchors:           &fakeAnchors{},
		Bundles:           bundles,
		Retry:             RetryPolicy{AwaitDAAttempts: 2, AwaitDAInterval: 1, SourceAttempts: 2, SourceInterval: 1},
	}
}

func leafPath(b uint64) trie.Path {
	var p trie.Path
	binary.BigEndian.PutUint64(p[:8], b)
	return p
}

// committer drives Commit the way the loop does: one store, a leaf per momentum, and a ring
// that starts empty after restart.
type committer struct {
	t     *testing.T
	cfg   Config
	store *state.Store
	salt  byte
	// seqMul scales the input sequence a commit at height h records, so a second chain can carry
	// more inputs per momentum than the first.
	seqMul uint64
}

func newCommitter(t *testing.T, cfg Config, salt byte) *committer {
	t.Helper()
	store, err := state.OpenEmpty(cfg.Layout.StateWAL)
	if err != nil {
		t.Fatalf("OpenEmpty: %v", err)
	}
	t.Cleanup(func() { store.Close() })
	return &committer{t: t, cfg: cfg, store: store, salt: salt, seqMul: 1}
}

// committerOn is a committer over an already-open store, such as one a Rollback returned.
func committerOn(t *testing.T, cfg Config, store *state.Store, salt byte, seqMul uint64) *committer {
	t.Helper()
	t.Cleanup(func() { store.Close() })
	return &committer{t: t, cfg: cfg, store: store, salt: salt, seqMul: seqMul}
}

// ring is the follower's window when it has consumed from..to: at most D+1 trailing entries.
func (c *committer) ring(from, to uint64) []MomentumRef {
	var out []MomentumRef
	for h := from; h <= to; h++ {
		out = append(out, MomentumRef{Height: h, Hash: canonHash(h, c.salt)})
	}
	if max := int(c.cfg.ConfirmationDepth) + 1; len(out) > max {
		out = out[len(out)-max:]
	}
	return out
}

// position is the position committed at momentum height h; input sequence and batch id both
// track the height so every field strictly increases across commits.
func (c *committer) position(h uint64) Position {
	root, err := c.store.Root()
	if err != nil {
		c.t.Fatalf("Root: %v", err)
	}
	return Position{
		LastInputSeq: h * c.seqMul, PostStateRoot: root,
		MomentumHeight: h, MomentumHash: canonHash(h, c.salt),
		NextOrdinal: h + 1, NextBatchID: h + 1,
	}
}

// record is the batch record for height h, with a stored bundle that carries the store's
// current root at the record's last input.
func (c *committer) record(h uint64) BatchRecord {
	root, err := c.store.Root()
	if err != nil {
		c.t.Fatalf("Root: %v", err)
	}
	seq := h * c.seqMul
	return BatchRecord{
		BatchID: h, Status: StatusBuilt, FirstInputSeq: seq, LastInputSeq: seq,
		PostStateRoot: root, DAHash: bundleFor(c.t, c.cfg, seq, root),
	}
}

// commit applies a leaf and commits position h with the given window.
func (c *committer) commit(h uint64, window []MomentumRef) Position {
	c.t.Helper()
	if _, err := c.store.Apply([]domain.StateDiffEntry{{Path: leafPath(h), Value: []byte{byte(h)}}}); err != nil {
		c.t.Fatalf("Apply: %v", err)
	}
	pos := c.position(h)
	if err := Commit(c.cfg, c.store, pos, []BatchRecord{c.record(h)}, window); err != nil {
		c.t.Fatalf("Commit %d: %v", h, err)
	}
	return pos
}

// commitEach commits every height in [from, to] with a ring that has been consumed
// continuously from `ringStart`.
func (c *committer) commitEach(ringStart, from, to uint64) {
	for h := from; h <= to; h++ {
		c.commit(h, c.ring(ringStart, h))
	}
}

func snapshotHeights(t *testing.T, l Layout) []uint64 {
	t.Helper()
	snaps, err := listPositioned(l)
	if err != nil {
		t.Fatalf("listPositioned: %v", err)
	}
	var out []uint64
	for _, s := range snaps {
		out = append(out, s.pos.MomentumHeight)
	}
	return out
}

func windowHeights(t *testing.T, l Layout) []uint64 {
	t.Helper()
	w, err := ReadMomentumWindow(l)
	if err != nil {
		t.Fatalf("ReadMomentumWindow: %v", err)
	}
	var out []uint64
	for _, e := range w {
		out = append(out, e.Height)
	}
	return out
}

func seq(from, to uint64) []uint64 {
	var out []uint64
	for h := from; h <= to; h++ {
		out = append(out, h)
	}
	return out
}

// --- tests 8-14 ---

func TestCursorRoundTrip(t *testing.T) {
	cfg := testConfig(t, 4)
	if _, ok, err := ReadCursor(cfg.Layout); err != nil || ok {
		t.Fatalf("ReadCursor on an empty tree = ok %v, err %v", ok, err)
	}
	want := Position{LastInputSeq: 5, PostStateRoot: types.Hash{1}, MomentumHeight: 9, MomentumHash: types.Hash{2}, NextOrdinal: 11, NextBatchID: 3}
	if err := writeJSON(cfg.Layout.Cursor, want); err != nil {
		t.Fatal(err)
	}
	got, ok, err := ReadCursor(cfg.Layout)
	if err != nil || !ok || got != want {
		t.Fatalf("ReadCursor = %+v, %v, %v; want %+v", got, ok, err, want)
	}
}

func TestBundleStore(t *testing.T) {
	cfg := testConfig(t, 4)
	data := []byte("bundle bytes")
	var daHash types.Hash
	copy(daHash[:], crypto.Hash(data))

	if ok, err := cfg.Bundles.Has(daHash); err != nil || ok {
		t.Fatalf("Has before Put = %v, %v", ok, err)
	}
	if _, err := cfg.Bundles.Get(daHash); !errors.Is(err, ErrBundleMissing) {
		t.Fatalf("Get of an unknown hash = %v, want ErrBundleMissing", err)
	}
	if err := cfg.Bundles.Put(daHash, data); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := cfg.Bundles.Put(daHash, data); err != nil {
		t.Fatalf("second Put: %v", err)
	}
	if ok, err := cfg.Bundles.Has(daHash); err != nil || !ok {
		t.Fatalf("Has after Put = %v, %v", ok, err)
	}
	got, err := cfg.Bundles.Get(daHash)
	if err != nil || string(got) != string(data) {
		t.Fatalf("Get = %q, %v", got, err)
	}

	if err := os.WriteFile(cfg.Bundles.path(daHash), []byte("altered"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := cfg.Bundles.Get(daHash); err == nil || errors.Is(err, ErrBundleMissing) {
		t.Fatalf("Get of altered bytes = %v, want a non-ErrBundleMissing error", err)
	}
}

func TestBundleStorePutRejectsMismatchedHash(t *testing.T) {
	cfg := testConfig(t, 4)
	var daHash types.Hash
	copy(daHash[:], crypto.Hash([]byte("bundle bytes")))

	if err := cfg.Bundles.Put(daHash, []byte("different bytes")); err == nil {
		t.Fatalf("Put accepted bytes that do not hash to daHash")
	}
	if ok, err := cfg.Bundles.Has(daHash); err != nil || ok {
		t.Fatalf("Has after a rejected Put = %v, %v, want false", ok, err)
	}
	if _, err := cfg.Bundles.Get(daHash); !errors.Is(err, ErrBundleMissing) {
		t.Fatalf("Get after a rejected Put = %v, want ErrBundleMissing", err)
	}
}

func TestListBatchRecordsSkipsContentIDMismatch(t *testing.T) {
	cfg := testConfig(t, 4)
	for _, id := range []uint64{1, 2, 3} {
		if err := writeJSON(batchPath(cfg.Layout, id), BatchRecord{BatchID: id, Status: StatusBuilt}); err != nil {
			t.Fatal(err)
		}
	}
	// File named for id 2 decodes a record claiming a different BatchID: a partial overwrite or a
	// stray file must not shadow the record its name promises.
	if err := writeJSON(batchPath(cfg.Layout, 2), BatchRecord{BatchID: 99, Status: StatusBuilt}); err != nil {
		t.Fatal(err)
	}
	recs, err := ListBatchRecords(cfg.Layout)
	if err != nil {
		t.Fatal(err)
	}
	var got []uint64
	for _, r := range recs {
		got = append(got, r.BatchID)
	}
	if want := []uint64{1, 3}; !reflect.DeepEqual(got, want) {
		t.Fatalf("ids = %v, want %v (record at file 2 skipped)", got, want)
	}
}

func TestListBatchRecordsNumericOrder(t *testing.T) {
	cfg := testConfig(t, 4)
	for _, id := range []uint64{12, 3, 0, 11, 2, 10, 1, 9, 100} {
		if err := writeJSON(batchPath(cfg.Layout, id), BatchRecord{BatchID: id, Status: StatusBuilt}); err != nil {
			t.Fatal(err)
		}
	}
	// A leftover temporary is skipped.
	if err := os.WriteFile(filepath.Join(cfg.Layout.Batches, ".tmp-123"), []byte("{"), 0o644); err != nil {
		t.Fatal(err)
	}
	recs, err := ListBatchRecords(cfg.Layout)
	if err != nil {
		t.Fatal(err)
	}
	var got []uint64
	for _, r := range recs {
		got = append(got, r.BatchID)
	}
	if want := []uint64{0, 1, 2, 3, 9, 10, 11, 12, 100}; !reflect.DeepEqual(got, want) {
		t.Fatalf("ids = %v, want %v", got, want)
	}
}

func TestCommitWritesEverything(t *testing.T) {
	cfg := testConfig(t, 4)
	c := newCommitter(t, cfg, 0)
	pos := c.commit(3, c.ring(1, 3))

	if got, ok, err := ReadCursor(cfg.Layout); err != nil || !ok || got != pos {
		t.Fatalf("cursor = %+v, %v, %v", got, ok, err)
	}
	if recs, err := ListBatchRecords(cfg.Layout); err != nil || len(recs) != 1 || recs[0].BatchID != 3 {
		t.Fatalf("records = %v, %v", recs, err)
	}
	if got := windowHeights(t, cfg.Layout); !reflect.DeepEqual(got, seq(1, 3)) {
		t.Fatalf("window = %v", got)
	}
	snaps, err := listPositioned(cfg.Layout)
	if err != nil || len(snaps) != 1 || snaps[0].pos != pos || snaps[0].info.Header.InputSeq != pos.LastInputSeq ||
		snaps[0].info.Header.STFSpecHash != testSTF {
		t.Fatalf("snapshots = %+v, %v", snaps, err)
	}
	if fi, err := os.Stat(cfg.Layout.StateWAL); err != nil || fi.Size() != 0 {
		t.Fatalf("state log after Commit: %v, %v", fi, err)
	}
}

func TestRetentionLadder(t *testing.T) {
	t.Run("D=4 one commit per momentum", func(t *testing.T) {
		cfg := testConfig(t, 4)
		c := newCommitter(t, cfg, 0)
		want := map[uint64][]uint64{
			8:  {3, 7, 8},
			9:  {3, 7, 9},
			10: {3, 7, 10},
			11: {7, 11},
			12: {7, 11, 12},
		}
		for h := uint64(1); h <= 12; h++ {
			c.commit(h, c.ring(1, h))
			got := snapshotHeights(t, cfg.Layout)
			if len(got) > 3 {
				t.Fatalf("Hc=%d: %d snapshots survive: %v", h, len(got), got)
			}
			if got[len(got)-1] != h {
				t.Fatalf("Hc=%d: cursor snapshot missing from %v", h, got)
			}
			if w, ok := want[h]; ok && !reflect.DeepEqual(got, w) {
				t.Fatalf("Hc=%d: snapshots %v, want %v", h, got, w)
			}
		}
	})

	t.Run("commit at least every D momenta", func(t *testing.T) {
		const depth = 4
		cfg := testConfig(t, depth)
		c := newCommitter(t, cfg, 0)
		for h := uint64(3); h <= 40; h += 3 {
			c.commit(h, c.ring(1, h))
			got := snapshotHeights(t, cfg.Layout)
			if len(got) > 3 {
				t.Fatalf("Hc=%d: %d snapshots survive: %v", h, len(got), got)
			}
			if got[len(got)-1] != h {
				t.Fatalf("Hc=%d: cursor snapshot missing from %v", h, got)
			}
			if h > 2*depth && got[0] > h-depth {
				t.Fatalf("Hc=%d: no floor at or below %d in %v", h, h-depth, got)
			}
		}
	})
}

func TestCommitWindowReconciliation(t *testing.T) {
	const depth = 4

	t.Run("a: restart merges", func(t *testing.T) {
		cfg := testConfig(t, depth)
		c := newCommitter(t, cfg, 0)
		c.commit(3, c.ring(1, 3))
		c.commit(5, c.ring(4, 5)) // fresh ring one above the committed height
		if got := windowHeights(t, cfg.Layout); !reflect.DeepEqual(got, seq(1, 5)) {
			t.Fatalf("after first restart: %v", got)
		}
		c.commit(9, c.ring(6, 9))
		if got := windowHeights(t, cfg.Layout); !reflect.DeepEqual(got, seq(5, 9)) {
			t.Fatalf("after second restart: %v, want %v (D+1 entries)", got, seq(5, 9))
		}
	})

	t.Run("b: gap replaces", func(t *testing.T) {
		cfg := testConfig(t, depth)
		c := newCommitter(t, cfg, 0)
		c.commit(9, c.ring(5, 9))
		c.commit(19, c.ring(15, 19))
		if got := windowHeights(t, cfg.Layout); !reflect.DeepEqual(got, seq(15, 19)) {
			t.Fatalf("window = %v, want %v", got, seq(15, 19))
		}
	})

	t.Run("c: overlap merges without duplicates", func(t *testing.T) {
		cfg := testConfig(t, depth)
		c := newCommitter(t, cfg, 0)
		c.commit(9, c.ring(5, 9))
		c.commit(11, c.ring(8, 11))
		if got := windowHeights(t, cfg.Layout); !reflect.DeepEqual(got, seq(7, 11)) {
			t.Fatalf("window = %v, want %v", got, seq(7, 11))
		}
	})

	t.Run("d: empty recorded takes the argument, empty argument leaves it", func(t *testing.T) {
		cfg := testConfig(t, depth)
		c := newCommitter(t, cfg, 0)
		c.commit(6, c.ring(4, 6))
		if got := windowHeights(t, cfg.Layout); !reflect.DeepEqual(got, seq(4, 6)) {
			t.Fatalf("empty recorded window did not take the argument: %v", got)
		}
		c.commit(8, nil)
		if got := windowHeights(t, cfg.Layout); !reflect.DeepEqual(got, seq(4, 6)) {
			t.Fatalf("an empty argument changed the recorded window: %v", got)
		}

		// The recorded window now ends below the cursor, so ForkParent falls back to the
		// cursor's own entry.
		pos, _, _ := ReadCursor(cfg.Layout)
		agree := newChainSource(8, 0)
		cfg.Momenta = agree
		if h, ok, err := ForkParent(cfg, pos); err != nil || !ok || h != 8 {
			t.Fatalf("ForkParent on an agreeing chain = %d, %v, %v", h, ok, err)
		}
		forked := newChainSource(8, 0)
		forked.rewrite(8, 1)
		cfg.Momenta = forked
		if _, ok, err := ForkParent(cfg, pos); err != nil || ok {
			t.Fatalf("ForkParent on a chain forked at the cursor = ok %v, err %v; want ok false", ok, err)
		}
	})

	t.Run("e: invalid windows write nothing", func(t *testing.T) {
		cfg := testConfig(t, depth)
		c := newCommitter(t, cfg, 0)
		c.commit(6, c.ring(4, 6))

		snapshot := func() any {
			cur, _, _ := ReadCursor(cfg.Layout)
			recs, _ := ListBatchRecords(cfg.Layout)
			w, _ := ReadMomentumWindow(cfg.Layout)
			return []any{cur, recs, w, snapshotHeights(t, cfg.Layout)}
		}
		before := snapshot()

		if _, err := c.store.Apply([]domain.StateDiffEntry{{Path: leafPath(7), Value: []byte{7}}}); err != nil {
			t.Fatal(err)
		}
		pos := c.position(7)
		gapped := []MomentumRef{{Height: 5, Hash: canonHash(5, 0)}, {Height: 7, Hash: canonHash(7, 0)}}
		wrongEnd := c.ring(5, 7)
		wrongEnd[len(wrongEnd)-1].Hash = types.Hash{0xEE}
		belowPos := c.ring(4, 6)
		for name, w := range map[string][]MomentumRef{"gapped": gapped, "hash differs": wrongEnd, "ends below pos": belowPos} {
			if err := Commit(cfg, c.store, pos, []BatchRecord{c.record(7)}, w); err == nil {
				t.Fatalf("%s: Commit succeeded", name)
			}
			if after := snapshot(); !reflect.DeepEqual(before, after) {
				t.Fatalf("%s: tree changed: %v -> %v", name, before, after)
			}
		}
	})

	t.Run("f: one-entry recorded window", func(t *testing.T) {
		cfg := testConfig(t, depth)
		c := newCommitter(t, cfg, 0)
		c.commit(7, []MomentumRef{{Height: 7, Hash: canonHash(7, 0)}})

		c.commit(9, c.ring(8, 9)) // untrimmed fresh ring: merges
		if got := windowHeights(t, cfg.Layout); !reflect.DeepEqual(got, seq(7, 9)) {
			t.Fatalf("merge result = %v, want %v", got, seq(7, 9))
		}

		cfg2 := testConfig(t, depth)
		c2 := newCommitter(t, cfg2, 0)
		c2.commit(7, []MomentumRef{{Height: 7, Hash: canonHash(7, 0)}})
		c2.commit(14, c2.ring(8, 14)) // trimmed ring, starts above top+1: replaces
		if got := windowHeights(t, cfg2.Layout); !reflect.DeepEqual(got, seq(10, 14)) {
			t.Fatalf("replace result = %v, want %v", got, seq(10, 14))
		}
	})
}

func TestCommitRejectsMalformedPosition(t *testing.T) {
	cfg := testConfig(t, 4)
	store, err := state.OpenEmpty(cfg.Layout.StateWAL)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	for name, pos := range map[string]Position{
		"zero position":          {},
		"zero momentum hash":     {MomentumHeight: 1, LastInputSeq: 1, PostStateRoot: types.Hash{1}},
		"zero root, nonzero seq": {MomentumHeight: 1, MomentumHash: types.Hash{1}, LastInputSeq: 1},
	} {
		t.Run(name, func(t *testing.T) {
			if err := Commit(cfg, store, pos, nil, nil); err == nil {
				t.Fatalf("Commit accepted %+v", pos)
			}
			if _, ok, err := ReadCursor(cfg.Layout); err != nil || ok {
				t.Fatalf("cursor after rejected Commit: ok %v, err %v, want no cursor written", ok, err)
			}
		})
	}
}

// TestWriteFileAtomicSyncsDirectory exercises writeFileAtomic's rename-then-sync-directory path
// directly: a write must land at path and the directory sync it performs after the rename must
// not itself fail on an ordinary directory.
func TestWriteFileAtomicSyncsDirectory(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "target")
	if err := writeFileAtomic(path, []byte("first")); err != nil {
		t.Fatalf("writeFileAtomic: %v", err)
	}
	if got, err := os.ReadFile(path); err != nil || string(got) != "first" {
		t.Fatalf("read back = %q, %v", got, err)
	}
	// A second write exercises the rename-over-existing-file path the same directory sync guards.
	if err := writeFileAtomic(path, []byte("second")); err != nil {
		t.Fatalf("second writeFileAtomic: %v", err)
	}
	if got, err := os.ReadFile(path); err != nil || string(got) != "second" {
		t.Fatalf("read back after overwrite = %q, %v", got, err)
	}
	if err := syncDir(dir); err != nil {
		t.Fatalf("syncDir on the directory writeFileAtomic just synced: %v", err)
	}
}

func TestSyncDirMissingDirectory(t *testing.T) {
	if err := syncDir(filepath.Join(t.TempDir(), "does-not-exist")); err == nil {
		t.Fatalf("syncDir on a missing directory succeeded")
	}
}

func TestPruneAbove(t *testing.T) {
	cfg := testConfig(t, 4)
	c := newCommitter(t, cfg, 0)
	c.commitEach(1, 1, 5) // leaves snapshots at 3 and 5

	// Snapshots are written by hand for the two shapes no commit sequence produces.
	writeSnap := func(pos Position, leaf byte) {
		t.Helper()
		s, err := state.OpenEmpty(filepath.Join(t.TempDir(), "wal"))
		if err != nil {
			t.Fatal(err)
		}
		defer s.Close()
		if _, err := s.Apply([]domain.StateDiffEntry{{Path: leafPath(uint64(leaf)), Value: []byte{leaf}}}); err != nil {
			t.Fatal(err)
		}
		meta, _ := jsonBytes(pos)
		if _, err := s.SaveSnapshot(cfg.Layout.Snapshots, state.SnapshotHeader{InputSeq: pos.LastInputSeq, STFSpecHash: testSTF, Meta: meta}); err != nil {
			t.Fatal(err)
		}
	}
	base := Position{LastInputSeq: 3, MomentumHeight: 3, NextBatchID: 4}
	writeSnap(Position{LastInputSeq: 3, MomentumHeight: 9, NextBatchID: 4}, 0x71) // same seq, higher height
	writeSnap(Position{LastInputSeq: 8, MomentumHeight: 3, NextBatchID: 4}, 0x72) // same height, higher seq

	if err := PruneAbove(cfg.Layout, base); err != nil {
		t.Fatalf("PruneAbove: %v", err)
	}
	recs, _ := ListBatchRecords(cfg.Layout)
	var ids []uint64
	for _, r := range recs {
		ids = append(ids, r.BatchID)
	}
	if want := []uint64{1, 2, 3}; !reflect.DeepEqual(ids, want) {
		t.Fatalf("records after prune = %v, want %v", ids, want)
	}
	if got := windowHeights(t, cfg.Layout); !reflect.DeepEqual(got, seq(1, 3)) {
		t.Fatalf("window after prune = %v", got)
	}
	if got := snapshotHeights(t, cfg.Layout); !reflect.DeepEqual(got, []uint64{3}) {
		t.Fatalf("snapshot heights after prune = %v, want only the one at height 3", got)
	}

	if err := PruneAbove(cfg.Layout, Position{}); err != nil {
		t.Fatal(err)
	}
	recs, _ = ListBatchRecords(cfg.Layout)
	if len(recs) != 0 || len(windowHeights(t, cfg.Layout)) != 0 || len(snapshotHeights(t, cfg.Layout)) != 0 {
		t.Fatalf("zero-position prune left artifacts behind")
	}
}

// bundleFor stores a one-record bundle whose execution result carries postRoot at index, and
// returns its DAHash.
func bundleFor(t *testing.T, cfg Config, index uint64, postRoot types.Hash) types.Hash {
	t.Helper()
	res, err := codec.ExecutionResult{Version: codec.ExecutionResultVersion, GlobalInputIndex: index, PostStateRoot: postRoot}.Encode()
	if err != nil {
		t.Fatalf("encode result: %v", err)
	}
	bundle, err := codec.EncodeBundle([]codec.BundleInputRecord{{GlobalInputIndex: index, ExecutionResult: res}})
	if err != nil {
		t.Fatalf("encode bundle: %v", err)
	}
	var daHash types.Hash
	copy(daHash[:], crypto.Hash(bundle))
	if err := cfg.Bundles.Put(daHash, bundle); err != nil {
		t.Fatal(err)
	}
	return daHash
}

func jsonBytes(v any) ([]byte, error) { return json.Marshal(v) }
