package gateway

import (
	"bytes"
	"context"
	"errors"
	"io/fs"
	"math"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/zenon-network/go-zenon/common/trie"
	"github.com/zenon-network/go-zenon/common/types"
	"github.com/zenon-network/go-zenon/vm/embedded/definition"

	"github.com/Zeno-L2/zeno-executor/domain"
	"github.com/Zeno-L2/zeno-executor/internal/codec"
	"github.com/Zeno-L2/zeno-executor/internal/recovery"
	"github.com/Zeno-L2/zeno-executor/internal/state"
)

// buildA builds history A into a fresh directory.
func buildA(t *testing.T) (string, []codec.BatchCommitment) {
	t.Helper()
	dir := t.TempDir()
	return dir, buildHistory(t, dir, nil, historyA())
}

// tagID returns the batch id a tag holds, or -1 for a nil tag.
func tagID(b *BatchHead) int64 {
	if b == nil {
		return -1
	}
	return int64(b.BatchID)
}

func doPoll(t *testing.T, r *rig) Head {
	t.Helper()
	if err := r.c.Poll(context.Background()); err != nil {
		t.Fatalf("Poll: %v", err)
	}
	return r.c.Head()
}

// requireTags requires the pending, latest and finalized batch ids
// (-1 for nil) and the status.
func requireTags(t *testing.T, h Head, pending, latest, finalized int64, status Status) {
	t.Helper()
	if tagID(h.Pending) != pending || tagID(h.Latest) != latest || tagID(h.Finalized) != finalized || h.Status != status {
		t.Fatalf("head = pending %d latest %d finalized %d %s, want pending %d latest %d finalized %d %s",
			tagID(h.Pending), tagID(h.Latest), tagID(h.Finalized), h.Status, pending, latest, finalized, status)
	}
}

func requireVersionRoot(t *testing.T, c *Chain, id uint64, want types.Hash) {
	t.Helper()
	v, ok := c.VersionAt(id)
	if !ok {
		t.Fatalf("VersionAt(%d) is not held", id)
	}
	if v.Root() != want {
		t.Fatalf("VersionAt(%d) root %s, want %s", id, v.Root(), want)
	}
}

func requireNoVersion(t *testing.T, c *Chain, ids ...uint64) {
	t.Helper()
	for _, id := range ids {
		if _, ok := c.VersionAt(id); ok {
			t.Fatalf("VersionAt(%d) is held, want it dropped", id)
		}
	}
}

type proofSnap struct {
	present bool
	value   []byte
	proof   []byte
}

func snapPath(v state.Version, p trie.Path) proofSnap {
	present, value, proof := v.Prove(p)
	return proofSnap{present: present, value: bytes.Clone(value), proof: bytes.Clone(proof)}
}

type versionSnap struct {
	root  types.Hash
	paths []proofSnap
}

func snapVersion(v state.Version) versionSnap {
	s := versionSnap{root: v.Root()}
	for _, p := range allPaths {
		s.paths = append(s.paths, snapPath(v, p))
	}
	return s
}

func requireSnap(t *testing.T, name string, v state.Version, want versionSnap) {
	t.Helper()
	got := snapVersion(v)
	if got.root != want.root {
		t.Fatalf("%s: root changed", name)
	}
	for i, g := range got.paths {
		w := want.paths[i]
		if g.present != w.present || !bytes.Equal(g.value, w.value) || !bytes.Equal(g.proof, w.proof) || (g.value == nil) != (w.value == nil) {
			t.Fatalf("%s: path %d changed", name, i)
		}
	}
}

func TestVersionPinning(t *testing.T) {
	d1 := []codec.StateDiffEntry{
		{Key: pA, Value: []byte("one")},
		{Key: pB, Value: []byte{}},
		{Key: pC, Value: []byte("c")},
	}
	d2 := []codec.StateDiffEntry{
		{Key: pA, Value: []byte("two")},
		{Key: pB, Value: nil},
		{Key: pC, Value: nil},
		{Key: pC, Value: []byte("again")},
		{Key: pD, Value: []byte("d")},
	}
	v1 := state.Version{}.Apply(stateDiff(d1))
	s1 := snapVersion(v1)
	v2 := v1.Apply(stateDiff(d2))
	s2 := snapVersion(v2)
	_ = v2.Apply(stateDiff([]codec.StateDiffEntry{{Key: pB, Value: []byte("back")}}))

	for _, d := range [][]codec.StateDiffEntry{d1, d2} {
		for _, e := range d {
			for i := range e.Value {
				e.Value[i] = 0xEE
			}
		}
	}
	requireSnap(t, "v1", v1, s1)
	requireSnap(t, "v2", v2, s2)
	present, value, _ := v1.Prove(pB)
	if !present || value == nil || len(value) != 0 {
		t.Fatalf("v1 pB: present %v, value %v, want present-empty", present, value)
	}
}

// requireProofs verifies every fixture path against version id's root.
func requireProofs(t *testing.T, c *Chain, id uint64) (presence map[trie.Path][]byte) {
	t.Helper()
	v, ok := c.VersionAt(id)
	if !ok {
		t.Fatalf("VersionAt(%d) is not held", id)
	}
	root := v.Root()
	presence = make(map[trie.Path][]byte)
	for _, p := range allPaths {
		present, value, proof := v.Prove(p)
		var good bool
		var err error
		if present {
			good, err = trie.VerifyProofByPath(root, p, value, proof)
			presence[p] = value
		} else {
			good, err = trie.VerifyAbsenceByPath(root, p, proof)
		}
		if err != nil || !good {
			t.Fatalf("version %d path %x: proof does not verify (present %v): %v", id, p[0], present, err)
		}
	}
	return presence
}

func fileSize(t *testing.T, path string) int64 {
	t.Helper()
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat %s: %v", path, err)
	}
	return info.Size()
}

func finalRoot(t *testing.T, r *rig) types.Hash {
	t.Helper()
	root, err := r.final.Root()
	if err != nil {
		t.Fatalf("Final.Root: %v", err)
	}
	return root
}

func TestChainFollowsTags(t *testing.T) {
	dir, cs := buildA(t)
	s := newFakeSettlement()
	s.height = 10
	s.accept(cs[0], 20)
	s.accept(cs[1], 21)
	r := newTestChain(t, dir, s)

	requireTags(t, doPoll(t, r), 4, 1, -1, StatusOK)
	for i, c := range cs {
		requireVersionRoot(t, r.c, uint64(i), c.PostStateRoot)
		pres := requireProofs(t, r.c, uint64(i))
		switch i {
		case 0:
			if v, ok := pres[pB]; !ok || v == nil || len(v) != 0 {
				t.Fatalf("version 0 pB = %v, want present-empty", v)
			}
		case 1:
			if _, ok := pres[pB]; ok {
				t.Fatal("version 1 pB is present, want absent")
			}
		case 4:
			if v, ok := pres[pA]; !ok || v == nil || len(v) != 0 {
				t.Fatalf("version 4 pA = %v, want present-empty", v)
			}
		}
	}

	s.height = 20
	requireTags(t, doPoll(t, r), 4, 1, 0, StatusOK)
	if got := finalRoot(t, r); got != cs[0].PostStateRoot {
		t.Fatalf("Final root %s, want batch 0's %s", got, cs[0].PostStateRoot)
	}

	s.height = 30
	s.accept(cs[2], 22)
	s.accept(cs[3], 23)
	s.accept(cs[4], 24)
	requireTags(t, doPoll(t, r), 4, 4, 4, StatusOK)
	requireNoVersion(t, r.c, 3)
	if got := finalRoot(t, r); got != cs[4].PostStateRoot {
		t.Fatalf("Final root %s, want batch 4's %s", got, cs[4].PostStateRoot)
	}
}

func TestChainConcurrentReads(t *testing.T) {
	dir, cs := buildA(t)
	s := newFakeSettlement()
	s.height = 10
	s.accept(cs[0], 20)
	s.accept(cs[1], 21)
	r := newTestChain(t, dir, s)

	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			select {
			case <-stop:
				return
			default:
			}
			_ = r.c.Head()
			for id := uint64(0); id <= 4; id++ {
				if v, ok := r.c.VersionAt(id); ok {
					_ = v.Root()
					_, _, _ = v.Prove(pA)
				}
			}
		}
	}()

	doPoll(t, r)
	s.height = 20
	doPoll(t, r)
	s.height = 30
	s.accept(cs[2], 22)
	s.accept(cs[3], 23)
	s.accept(cs[4], 24)
	doPoll(t, r)
	close(stop)
	<-done
}

func layoutOf(dir string) recovery.Layout { return recovery.ResolveLayout(dir, 1) }

func readFile(t *testing.T, path string) []byte {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	return b
}

func writeFile(t *testing.T, path string, b []byte) {
	t.Helper()
	if err := os.WriteFile(path, b, 0o644); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}

// copyBatch copies batch c's record, bundle and sidecar from the
// executor directory from to the one at to.
func copyBatch(t *testing.T, from, to string, c codec.BatchCommitment) {
	t.Helper()
	copyRecord(t, from, to, c.BatchID)
	f, d := layoutOf(from), layoutOf(to)
	writeFile(t, recovery.BundlePath(d.Bundles, c.DAHash), readFile(t, recovery.BundlePath(f.Bundles, c.DAHash)))
	writeFile(t, recovery.ReceiptsPath(d.Receipts, c.DAHash), readFile(t, recovery.ReceiptsPath(f.Receipts, c.DAHash)))
}

func copyRecord(t *testing.T, from, to string, id uint64) {
	t.Helper()
	writeFile(t, recordPath(layoutOf(to), id), readFile(t, recordPath(layoutOf(from), id)))
}

func removeRecord(t *testing.T, dir string, id uint64) {
	t.Helper()
	if err := os.Remove(recordPath(layoutOf(dir), id)); err != nil {
		t.Fatalf("remove record %d: %v", id, err)
	}
}

// rewriteSidecar decodes the sidecar at path, applies edit to its
// receipts, writes it back and returns the new receiptRoot.
func rewriteSidecar(t *testing.T, path string, edit func([]codec.Receipt)) types.Hash {
	t.Helper()
	receipts, _, err := codec.DecodeReceiptSidecar(readFile(t, path))
	if err != nil {
		t.Fatalf("decode sidecar: %v", err)
	}
	edit(receipts)
	enc, err := codec.EncodeReceiptSidecar(receipts)
	if err != nil {
		t.Fatalf("encode sidecar: %v", err)
	}
	writeFile(t, path, enc)
	_, leaves, err := codec.DecodeReceiptSidecar(enc)
	if err != nil {
		t.Fatalf("decode rewritten sidecar: %v", err)
	}
	return codec.MerkleRoot(leaves)
}

func flipLastByte(t *testing.T, path string) {
	t.Helper()
	b := readFile(t, path)
	b[len(b)-1] ^= 0xFF
	writeFile(t, path, b)
}

// requireFrozenAt1 holds after a failed check on batch 2: INCONSISTENT,
// latest at batch 1, batch 2 not held, and an ALARM line for batch 2
// that contains wantSub.
func requireFrozenAt1(t *testing.T, r *rig, cs []codec.BatchCommitment, wantSub string) {
	t.Helper()
	h := r.c.Head()
	if h.Status != StatusInconsistent || tagID(h.Latest) != 1 {
		t.Fatalf("head = latest %d %s, want latest 1 INCONSISTENT\n%s", tagID(h.Latest), h.Status, r.log.dump())
	}
	requireVersionRoot(t, r.c, 1, cs[1].PostStateRoot)
	requireNoVersion(t, r.c, 2)
	if r.log.count("ALARM", "batch 2", wantSub) != 1 {
		t.Fatalf("no ALARM line for batch 2 containing %q:\n%s", wantSub, r.log.dump())
	}
}

func TestChainTamper(t *testing.T) {
	pristine, cs := buildA(t)
	wrong := types.Hash{0xAB}

	type env struct {
		l recovery.Layout
		s *fakeSettlement
	}
	bundle2 := func(e env) string { return recovery.BundlePath(e.l.Bundles, cs[2].DAHash) }
	sidecar2 := func(e env) string { return recovery.ReceiptsPath(e.l.Receipts, cs[2].DAHash) }
	entry := func(fn func(*definition.BatchEntry)) func(*testing.T, env) {
		return func(t *testing.T, e env) { e.s.edit(2, fn) }
	}
	sidecarEdit := func(edit func([]codec.Receipt)) func(*testing.T, env) {
		return func(t *testing.T, e env) {
			root := rewriteSidecar(t, sidecar2(e), edit)
			e.s.edit(2, func(be *definition.BatchEntry) { be.ReceiptRoot = root })
		}
	}

	cases := []struct {
		name    string
		wantSub string
		apply   func(*testing.T, env)
	}{
		{"1 bundle byte", "check 1", func(t *testing.T, e env) { flipLastByte(t, bundle2(e)) }},
		{"2 sidecar byte", "check 2", func(t *testing.T, e env) { flipLastByte(t, sidecar2(e)) }},
		{"3 receipt input index", "inputIndex", sidecarEdit(func(r []codec.Receipt) { r[0].InputIndex = 3 })},
		{"4 receipt event root", "check 4", sidecarEdit(func(r []codec.Receipt) { r[1].EventRoot = wrong })},
		{"5 post-state root", "check 3", entry(func(be *definition.BatchEntry) { be.PostStateRoot = wrong })},
		{"6 first input seq", "firstInputSeq", entry(func(be *definition.BatchEntry) { be.FirstInputSeq = 5 })},
		{"7 last input seq", "lastInputSeq", entry(func(be *definition.BatchEntry) { be.LastInputSeq = 5 })},
		{"8 input root", "inputRoot", entry(func(be *definition.BatchEntry) { be.InputRoot = wrong })},
		{"9 event root", "is not the bundle's event root", entry(func(be *definition.BatchEntry) { be.EventRoot = wrong })},
		{"10 outbox root", "is not the bundle's outbox root", entry(func(be *definition.BatchEntry) { be.OutboxRoot = wrong })},
		{"11 outbox count", "outboxCount", sidecarEdit(func(r []codec.Receipt) { r[0].OutboxCount = 0 })},
		{"14 event count", "eventCount", sidecarEdit(func(r []codec.Receipt) { r[0].EventCount++ })},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			copyDir(t, pristine, dir)
			e := env{l: layoutOf(dir), s: newFakeSettlement()}
			e.s.height = 10
			for i := 0; i < 3; i++ {
				e.s.accept(cs[i], 100)
			}
			origBundle, origSidecar, origEntry := readFile(t, bundle2(e)), readFile(t, sidecar2(e)), e.s.entries[2]
			tc.apply(t, e)

			r := newTestChain(t, dir, e.s)
			doPoll(t, r)
			requireFrozenAt1(t, r, cs, tc.wantSub)

			writeFile(t, bundle2(e), origBundle)
			writeFile(t, sidecar2(e), origSidecar)
			e.s.entries[2] = origEntry
			doPoll(t, r)
			requireFrozenAt1(t, r, cs, tc.wantSub)
		})
	}

	t.Run("12 promotion", func(t *testing.T) {
		dir := t.TempDir()
		copyDir(t, pristine, dir)
		s := newFakeSettlement()
		s.height = 10
		s.accept(cs[0], 100)
		s.accept(cs[1], 100)
		r := newTestChain(t, dir, s)
		requireTags(t, doPoll(t, r), 4, 1, -1, StatusOK)

		s.accept(cs[2], 100)
		s.edit(2, func(be *definition.BatchEntry) { be.InputRoot = wrong })
		doPoll(t, r)
		requireFrozenAt1(t, r, cs, "inputRoot")
		if r.log.count("retracted") == 0 {
			t.Fatalf("no retraction logged:\n%s", r.log.dump())
		}
	})

	t.Run("13 pending", func(t *testing.T) {
		dir := t.TempDir()
		copyDir(t, pristine, dir)
		flipLastByte(t, recovery.ReceiptsPath(layoutOf(dir).Receipts, cs[3].DAHash))
		s := newFakeSettlement()
		s.height = 10
		s.accept(cs[0], 100)
		s.accept(cs[1], 100)
		r := newTestChain(t, dir, s)
		h := doPoll(t, r)
		requireTags(t, h, 2, 1, -1, StatusInconsistent)
		if r.log.count("ALARM", "batch 3", "check 2") != 1 {
			t.Fatalf("no ALARM line for batch 3:\n%s", r.log.dump())
		}
	})
}

func TestChainNonEmptyGenesis(t *testing.T) {
	const alarm = "domain 1 is INCONSISTENT at batch 0: the domain's genesis state is not empty; the gateway serves only domains with an empty genesis; it stops advancing"
	build := func(t *testing.T) (string, []codec.BatchCommitment) {
		dir := t.TempDir()
		return dir, buildHistory(t, dir, []domain.StateDiffEntry{put(pD, "g")}, historyA()[:1])
	}

	t.Run("pending", func(t *testing.T) {
		dir, _ := build(t)
		r := newTestChain(t, dir, newFakeSettlement())
		h := doPoll(t, r)
		requireTags(t, h, -1, -1, -1, StatusInconsistent)
		requireNoVersion(t, r.c, 0)
		if r.log.count(alarm) != 1 || r.log.count("ALARM") != 1 {
			t.Fatalf("want exactly one genesis alarm:\n%s", r.log.dump())
		}
	})

	t.Run("accepted", func(t *testing.T) {
		dir, cs := build(t)
		s := newFakeSettlement()
		s.height = 10
		s.accept(cs[0], 20)
		r := newTestChain(t, dir, s)
		h := doPoll(t, r)
		requireTags(t, h, -1, -1, -1, StatusInconsistent)
		requireNoVersion(t, r.c, 0)
		if r.log.count(alarm) != 1 || r.log.count("ALARM") != 1 {
			t.Fatalf("want exactly one genesis alarm:\n%s", r.log.dump())
		}
	})
}

func TestChainRetraction(t *testing.T) {
	pristine, cs := buildA(t)
	bdir := t.TempDir()
	csB := buildHistory(t, bdir, nil, historyB())
	dir := t.TempDir()
	copyDir(t, pristine, dir)
	s := newFakeSettlement()
	s.height = 10
	s.accept(cs[0], 100)
	s.accept(cs[1], 100)
	r := newTestChain(t, dir, s)

	requireTags(t, doPoll(t, r), 4, 1, -1, StatusOK)
	removeRecord(t, dir, 3)
	requireTags(t, doPoll(t, r), 2, 1, -1, StatusOK)
	requireNoVersion(t, r.c, 3, 4)
	if r.log.count("retracted") != 1 {
		t.Fatalf("want one retraction line:\n%s", r.log.dump())
	}

	copyDir(t, pristine, dir)
	requireTags(t, doPoll(t, r), 4, 1, -1, StatusOK)

	copyBatch(t, bdir, dir, csB[3])
	removeRecord(t, dir, 4)
	requireTags(t, doPoll(t, r), 3, 1, -1, StatusOK)
	requireVersionRoot(t, r.c, 3, csB[3].PostStateRoot)
	requireNoVersion(t, r.c, 4)
}

func TestChainStaleBaseRecord(t *testing.T) {
	dir, cs := buildA(t)
	bdir := t.TempDir()
	csB := buildHistory(t, bdir, nil, historyB())
	copyBatch(t, bdir, dir, csB[4])
	s := newFakeSettlement()
	s.height = 10
	s.accept(cs[0], 100)
	s.accept(cs[1], 100)
	r := newTestChain(t, dir, s)

	requireTags(t, doPoll(t, r), 3, 1, -1, StatusOK)
	requireVersionRoot(t, r.c, 3, cs[3].PostStateRoot)
	requireNoVersion(t, r.c, 4)
	if n := r.log.count("ALARM") + r.log.count("WARN"); n != 0 {
		t.Fatalf("first poll logged %d alarm or warning lines:\n%s", n, r.log.dump())
	}

	for i := 0; i < 2; i++ {
		requireTags(t, doPoll(t, r), 3, 1, -1, StatusOK)
	}
	if r.log.count("WARN", "batch 4", csB[4].PreStateRoot.String(), cs[3].PostStateRoot.String()) != 1 || r.log.count("WARN") != 1 {
		t.Fatalf("want one WARN naming batch 4 and both roots:\n%s", r.log.dump())
	}
	if r.log.count("ALARM") != 0 {
		t.Fatalf("unexpected ALARM:\n%s", r.log.dump())
	}

	copyBatch(t, bdir, dir, csB[3])
	requireTags(t, doPoll(t, r), 4, 1, -1, StatusOK)
	requireVersionRoot(t, r.c, 4, csB[4].PostStateRoot)
}

func TestChainIgnoresRecordOfAcceptedBatch(t *testing.T) {
	dir, cs := buildA(t)
	bdir := t.TempDir()
	csB := buildHistory(t, bdir, nil, historyB())
	copyBatch(t, bdir, dir, csB[3])
	removeRecord(t, dir, 4)
	s := newFakeSettlement()
	s.height = 10
	for i := 0; i < 3; i++ {
		s.accept(cs[i], 100)
	}
	r := newTestChain(t, dir, s)

	requireTags(t, doPoll(t, r), 3, 2, -1, StatusOK)
	requireVersionRoot(t, r.c, 3, csB[3].PostStateRoot)

	s.accept(cs[3], 100)
	requireTags(t, doPoll(t, r), 3, 3, -1, StatusOK)
	requireVersionRoot(t, r.c, 3, cs[3].PostStateRoot)

	fresh := newTestChain(t, dir, s)
	requireTags(t, doPoll(t, fresh), 3, 3, -1, StatusOK)
	requireVersionRoot(t, fresh.c, 3, cs[3].PostStateRoot)
}

func TestChainExecutorDirectoryRebuilt(t *testing.T) {
	pristine, cs := buildA(t)
	live := t.TempDir()
	copyDir(t, pristine, live)
	s := newFakeSettlement()
	s.height = 10
	for i := 0; i < 3; i++ {
		s.accept(cs[i], 100)
	}
	r := newTestChain(t, live, s)

	var statuses []Status
	step := func() Head {
		h := doPoll(t, r)
		statuses = append(statuses, h.Status)
		return h
	}
	requireTags(t, step(), 4, 2, -1, StatusOK)
	if err := os.RemoveAll(filepath.Join(live, "1")); err != nil {
		t.Fatal(err)
	}
	requireTags(t, step(), 2, 2, -1, StatusOK)
	if _, err := os.Stat(filepath.Join(live, "1")); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("the engine recreated the directory: %v", err)
	}
	s.accept(cs[3], 100)
	requireTags(t, step(), 2, 2, -1, StatusSyncing)
	copyDir(t, pristine, live)
	requireTags(t, step(), 4, 3, -1, StatusOK)

	want := []Status{StatusOK, StatusOK, StatusSyncing, StatusOK}
	for i := range want {
		if statuses[i] != want[i] {
			t.Fatalf("statuses = %v, want %v", statuses, want)
		}
	}
	fresh := newTestChain(t, live, s)
	doPoll(t, fresh)
	requireSameVersions(t, r.c, fresh.c, 3, 4)
}

func TestChainMissingSidecar(t *testing.T) {
	pristine, cs := buildA(t)
	acceptTo := func(s *fakeSettlement, n int) {
		for i := 0; i <= n; i++ {
			s.accept(cs[i], 100)
		}
	}

	t.Run("sidecar", func(t *testing.T) {
		dir := t.TempDir()
		copyDir(t, pristine, dir)
		path := recovery.ReceiptsPath(layoutOf(dir).Receipts, cs[2].DAHash)
		saved := readFile(t, path)
		if err := os.Remove(path); err != nil {
			t.Fatal(err)
		}
		s := newFakeSettlement()
		s.height = 10
		acceptTo(s, 2)
		r := newTestChain(t, dir, s)
		doPoll(t, r)
		requireTags(t, doPoll(t, r), 1, 1, -1, StatusSyncing)
		if r.log.count("batch 2", "re-synced") != 1 {
			t.Fatalf("want one re-sync line:\n%s", r.log.dump())
		}
		writeFile(t, path, saved)
		requireTags(t, doPoll(t, r), 4, 2, -1, StatusOK)
	})

	if os.Geteuid() == 0 {
		t.Skip("file modes do not deny root")
	}

	chmodBack := func(t *testing.T, path string) {
		t.Helper()
		t.Cleanup(func() { _ = os.Chmod(path, 0o644) })
	}

	t.Run("accepted, permission denied", func(t *testing.T) {
		dir := t.TempDir()
		copyDir(t, pristine, dir)
		path := recovery.BundlePath(layoutOf(dir).Bundles, cs[3].DAHash)
		chmodBack(t, path)
		if err := os.Chmod(path, 0); err != nil {
			t.Fatal(err)
		}
		s := newFakeSettlement()
		s.height = 10
		acceptTo(s, 3)
		r := newTestChain(t, dir, s)
		doPoll(t, r)
		requireTags(t, doPoll(t, r), 2, 2, -1, StatusSyncing)
		if r.log.count("re-synced") != 1 {
			t.Fatalf("want one re-sync line:\n%s", r.log.dump())
		}
		if err := os.Chmod(path, 0o644); err != nil {
			t.Fatal(err)
		}
		requireTags(t, doPoll(t, r), 4, 3, -1, StatusOK)
	})

	for _, which := range []string{"record", "bundle"} {
		t.Run("pending, permission denied, "+which, func(t *testing.T) {
			dir := t.TempDir()
			copyDir(t, pristine, dir)
			path := recordPath(layoutOf(dir), 3)
			if which == "bundle" {
				path = recovery.BundlePath(layoutOf(dir).Bundles, cs[3].DAHash)
			}
			chmodBack(t, path)
			if err := os.Chmod(path, 0); err != nil {
				t.Fatal(err)
			}
			s := newFakeSettlement()
			s.height = 10
			acceptTo(s, 1)
			r := newTestChain(t, dir, s)
			doPoll(t, r)
			requireTags(t, doPoll(t, r), 2, 1, -1, StatusSyncing)
			if r.log.count("re-synced") != 1 {
				t.Fatalf("want one re-sync line:\n%s", r.log.dump())
			}
			if err := os.Chmod(path, 0o644); err != nil {
				t.Fatal(err)
			}
			requireTags(t, doPoll(t, r), 4, 1, -1, StatusOK)
		})
	}
}

func TestChainChangedEntry(t *testing.T) {
	start := func(t *testing.T) (*rig, *fakeSettlement) {
		dir, cs := buildA(t)
		s := newFakeSettlement()
		s.height = 10
		s.accept(cs[0], 20)
		s.accept(cs[1], 21)
		r := newTestChain(t, dir, s)
		doPoll(t, r)
		return r, s
	}

	t.Run("changed entry at finalization", func(t *testing.T) {
		r, s := start(t)
		s.edit(1, func(be *definition.BatchEntry) { be.PostStateRoot = types.Hash{0xAB} })
		s.height = 21
		h := doPoll(t, r)
		if h.Status != StatusInconsistent || tagID(h.Finalized) != 0 {
			t.Fatalf("head = finalized %d %s", tagID(h.Finalized), h.Status)
		}
		if r.log.count("ALARM", "batch 1", "changed after the gateway verified it") != 1 {
			t.Fatalf("no changed-entry alarm:\n%s", r.log.dump())
		}
	})

	t.Run("hidden entry", func(t *testing.T) {
		r, s := start(t)
		s.hidden[1] = true
		s.height = 21
		requireTags(t, doPoll(t, r), 4, 1, 0, StatusOK)
		s.hidden[1] = false
		requireTags(t, doPoll(t, r), 4, 1, 1, StatusOK)
	})

	t.Run("hidden entry during catch-up", func(t *testing.T) {
		dir, cs := buildA(t)
		s := newFakeSettlement()
		s.height = 10
		s.accept(cs[0], 20)
		s.accept(cs[1], 21)
		r := newTestChain(t, dir, s)
		doPoll(t, r)
		s.accept(cs[2], 100)
		s.accept(cs[3], 100)
		s.hidden[1] = true
		s.height = 21
		before := s.reads[1]
		requireTags(t, doPoll(t, r), 4, 3, 0, StatusOK)
		if got := s.reads[1] - before; got != 1 {
			t.Fatalf("entry 1 was read %d times in one poll, want 1", got)
		}
	})

	t.Run("re-read skipped at the same height", func(t *testing.T) {
		dir, cs := buildA(t)
		s := newFakeSettlement()
		s.height = 10
		s.accept(cs[0], 10)
		s.accept(cs[1], 21)
		r := newTestChain(t, dir, s)
		requireTags(t, doPoll(t, r), 4, 1, 0, StatusOK)
		if s.reads[0] != 1 || s.reads[1] != 1 {
			t.Fatalf("reads = %v, want one read each", s.reads)
		}
		s.height = 21
		requireTags(t, doPoll(t, r), 4, 1, 1, StatusOK)
		if s.reads[0] != 1 || s.reads[1] != 2 {
			t.Fatalf("reads = %v, want batch 0 once and batch 1 twice", s.reads)
		}
	})

	t.Run("finality from the re-read entry", func(t *testing.T) {
		r, s := start(t)
		s.edit(1, func(be *definition.BatchEntry) { be.FinalizeAtHeight = 25 })
		s.height = 21
		requireTags(t, doPoll(t, r), 4, 1, 0, StatusOK)
		s.height = 24
		requireTags(t, doPoll(t, r), 4, 1, 0, StatusOK)
		if s.reads[1] != 2 {
			t.Fatalf("reads[1] = %d, want 2", s.reads[1])
		}
		s.height = 25
		requireTags(t, doPoll(t, r), 4, 1, 1, StatusOK)
	})
}

func TestChainCommitmentBinding(t *testing.T) {
	cases := []struct {
		field string
		edit  func(*codec.BatchCommitment)
	}{
		{"domainId", func(cm *codec.BatchCommitment) { cm.DomainID = 2 }},
		{"batchId", func(cm *codec.BatchCommitment) { cm.BatchID = 9 }},
		{"daHash", func(cm *codec.BatchCommitment) { cm.DAHash = types.Hash{0xAB} }},
		{"preStateRoot", func(cm *codec.BatchCommitment) { cm.PreStateRoot = types.Hash{0xAB} }},
		{"postStateRoot", func(cm *codec.BatchCommitment) { cm.PostStateRoot = types.Hash{0xAB} }},
		{"firstInputSeq", func(cm *codec.BatchCommitment) { cm.FirstInputSeq++ }},
		{"lastInputSeq", func(cm *codec.BatchCommitment) { cm.LastInputSeq++ }},
	}
	for _, tc := range cases {
		t.Run(tc.field, func(t *testing.T) {
			dir, _ := buildA(t)
			l := layoutOf(dir)
			rec, ok, err := recovery.ReadBatchRecord(l, 0)
			if err != nil || !ok {
				t.Fatalf("read record: ok %v err %v", ok, err)
			}
			cm, err := codec.DecodeBatchCommitment(rec.Commitment)
			if err != nil {
				t.Fatal(err)
			}
			tc.edit(&cm)
			if rec.Commitment, err = cm.Encode(); err != nil {
				t.Fatal(err)
			}
			writeRecord(t, l, rec)

			r := newTestChain(t, dir, newFakeSettlement())
			h := doPoll(t, r)
			requireTags(t, h, -1, -1, -1, StatusInconsistent)
			if r.log.count("ALARM", "batch 0", "batch record "+tc.field+" does not match its commitment") != 1 {
				t.Fatalf("no %s binding alarm:\n%s", tc.field, r.log.dump())
			}
		})
	}
}

func TestChainPendingCommitmentRoots(t *testing.T) {
	cases := []struct {
		field string
		edit  func(*codec.BatchCommitment)
	}{
		{"inputRoot", func(cm *codec.BatchCommitment) { cm.InputRoot = types.Hash{0xAB} }},
		{"receiptRoot", func(cm *codec.BatchCommitment) { cm.ReceiptRoot = types.Hash{0xAB} }},
		{"eventRoot", func(cm *codec.BatchCommitment) { cm.EventRoot = types.Hash{0xAB} }},
		{"outboxRoot", func(cm *codec.BatchCommitment) { cm.OutboxRoot = types.Hash{0xAB} }},
	}
	for _, tc := range cases {
		t.Run(tc.field, func(t *testing.T) {
			dir, _ := buildA(t)
			l := layoutOf(dir)
			rec, ok, err := recovery.ReadBatchRecord(l, 0)
			if err != nil || !ok {
				t.Fatalf("read record: ok %v err %v", ok, err)
			}
			cm, err := codec.DecodeBatchCommitment(rec.Commitment)
			if err != nil {
				t.Fatal(err)
			}
			tc.edit(&cm)
			if rec.Commitment, err = cm.Encode(); err != nil {
				t.Fatal(err)
			}
			writeRecord(t, l, rec)

			r := newTestChain(t, dir, newFakeSettlement())
			requireTags(t, doPoll(t, r), -1, -1, -1, StatusInconsistent)
			if r.log.count("ALARM", "batch 0") != 1 {
				t.Fatalf("no alarm for a pending %s mismatch:\n%s", tc.field, r.log.dump())
			}
		})
	}
}

func TestChainPendingBatchesFromRecords(t *testing.T) {
	dir, _ := buildA(t)
	r := newTestChain(t, dir, newFakeSettlement())
	requireTags(t, doPoll(t, r), 4, -1, -1, StatusOK)
}

// readingDomain wraps a domain so the input at index reads n distinct
// absent keys before it applies, which records one witness per key.
type readingDomain struct {
	domain.Domain
	index uint64
	n     int
}

func (d readingDomain) Apply(acc domain.StateAccess, in domain.Input) (domain.Effects, error) {
	if in.(uint64) == d.index {
		for i := 0; i < d.n; i++ {
			p := trie.Path{0x80, byte(i >> 16), byte(i >> 8), byte(i)}
			if _, _, err := acc.Len(p); err != nil {
				return domain.Effects{}, err
			}
		}
	}
	return d.Domain.Apply(acc, in)
}

// TestChainVerifiesBundleOver64MiB checks that a valid bundle larger
// than 64 MiB loads and verifies through the production ceiling.
func TestChainVerifiesBundleOver64MiB(t *testing.T) {
	dir := t.TempDir()
	batches := [][]fixtureInput{{{index: 1, effects: success(put(pA, "a1"))}}}
	cs := buildHistoryWith(t, dir, nil, batches, func(d domain.Domain) domain.Domain {
		return readingDomain{Domain: d, index: 1, n: 520000}
	})
	if size := fileSize(t, recovery.BundlePath(layoutOf(dir).Bundles, cs[0].DAHash)); size <= 64<<20 || size > maxBundleBytes {
		t.Fatalf("bundle is %d bytes, want above 64 MiB and within the ceiling", size)
	}
	s := newFakeSettlement()
	s.height = 10
	s.accept(cs[0], 20)
	r := newTestChain(t, dir, s)
	requireTags(t, doPoll(t, r), 0, 0, -1, StatusOK)
	requireVersionRoot(t, r.c, 0, cs[0].PostStateRoot)
}

func TestCheckBatchHeldCost(t *testing.T) {
	if maxHeldBytes != 256<<20 || heldEntryCost != 1<<10 || readTimeout != 30*time.Second {
		t.Fatalf("constants = %d, %d, %s", maxHeldBytes, heldEntryCost, readTimeout)
	}
	pristine, cs := buildA(t)
	s := newFakeSettlement()
	s.height = 10
	s.accept(cs[0], 100)
	s.accept(cs[1], 100)
	r := newTestChain(t, pristine, s)
	doPoll(t, r)
	base, ok := r.c.VersionAt(1)
	if !ok {
		t.Fatal("VersionAt(1) is not held")
	}

	src := NewLocalSource(pristine, 1)
	bundle, err := src.Bundle(cs[2].DAHash)
	if err != nil {
		t.Fatal(err)
	}
	sidecar, err := src.Receipts(cs[2].DAHash)
	if err != nil {
		t.Fatal(err)
	}
	records, err := codec.DecodeBundle(bundle)
	if err != nil {
		t.Fatal(err)
	}
	var want int64
	for _, rec := range records {
		er, err := codec.DecodeExecutionResult(rec.ExecutionResult)
		if err != nil {
			t.Fatal(err)
		}
		want += int64(len(rec.InputData) + len(rec.ExecutionResult) + len(er.StateDiff)*heldEntryCost)
	}

	ck, err := checkBatch(base, commitmentExpect(cs[2]), bundle, sidecar, math.MaxInt64)
	if err != nil || ck.held != want {
		t.Fatalf("held = %d, %v; want %d", ck.held, err, want)
	}
	if _, err := checkBatch(base, commitmentExpect(cs[2]), bundle, sidecar, want); err != nil {
		t.Fatalf("limit equal to the cost: %v", err)
	}
	if _, err := checkBatch(base, commitmentExpect(cs[2]), bundle, sidecar, want-1); !errors.Is(err, errOverBudget) {
		t.Fatalf("limit below the cost: %v, want errOverBudget", err)
	}
	flipped := bytes.Clone(bundle)
	flipped[len(flipped)-1] ^= 0xFF
	_, err = checkBatch(base, commitmentExpect(cs[2]), flipped, sidecar, 0)
	if err == nil || errors.Is(err, errOverBudget) || !strings.Contains(err.Error(), "check 1") {
		t.Fatalf("tampered bundle at limit 0: %v, want the check 1 failure", err)
	}
}

func TestChainPendingHeldBudget(t *testing.T) {
	pristine, cs := buildA(t)
	engine := func(t *testing.T, dir string, budget int64) (*rig, *fakeSettlement) {
		t.Helper()
		s := newFakeSettlement()
		s.height = 10
		s.accept(cs[0], 100)
		s.accept(cs[1], 100)
		r := newTestChain(t, dir, s)
		if budget != 0 {
			r.c.heldBudget = budget
		}
		return r, s
	}
	shared := t.TempDir()
	copyDir(t, pristine, shared)

	a, _ := engine(t, shared, 0)
	requireTags(t, doPoll(t, a), 4, 1, -1, StatusOK)
	h2, h3, h4 := a.c.batches[2].held, a.c.batches[3].held, a.c.batches[4].held
	sum := h2 + h3 + h4

	b, _ := engine(t, shared, sum)
	requireTags(t, doPoll(t, b), 4, 1, -1, StatusOK)

	dir := t.TempDir()
	copyDir(t, pristine, dir)
	c, cs2 := engine(t, dir, sum-1)
	requireTags(t, doPoll(t, c), 3, 1, -1, StatusOK)
	if _, ok := c.c.VersionAt(4); ok {
		t.Fatal("batch 4 is held past the budget")
	}
	if c.log.count("ALARM") != 0 {
		t.Fatalf("a refusal raised an alarm:\n%s", c.log.dump())
	}
	if os.Geteuid() != 0 {
		path := bundlePathOf(dir, cs[4])
		t.Cleanup(func() { _ = os.Chmod(path, 0o644) })
		if err := os.Chmod(path, 0); err != nil {
			t.Fatal(err)
		}
		requireTags(t, doPoll(t, c), 3, 1, -1, StatusOK)
		if c.log.count("re-synced") != 0 {
			t.Fatalf("the refused bundle was read again:\n%s", c.log.dump())
		}
		if err := os.Chmod(path, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	cs2.accept(cs[2], 100)
	requireTags(t, doPoll(t, c), 4, 2, -1, StatusOK)

	d, ds := engine(t, shared, 1)
	requireTags(t, doPoll(t, d), 1, 1, -1, StatusOK)
	for i := 2; i <= 4; i++ {
		ds.accept(cs[i], 100)
	}
	requireTags(t, doPoll(t, d), 4, 4, -1, StatusOK)
}

func TestWithDeadlineEndedContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := withDeadline(ctx, time.Minute, func() (int, error) {
		t.Error("read was started")
		return 0, nil
	})
	if !errors.Is(err, errNoAnswer) || !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want errNoAnswer wrapping context.Canceled", err)
	}
}

func TestWithDeadlineRecoversPanic(t *testing.T) {
	_, err := withDeadline(context.Background(), time.Second, func() (int, error) {
		panic("boom")
	})
	if err == nil || !strings.Contains(err.Error(), "the read panicked: boom") {
		t.Fatalf("err = %v, want the read panicked: boom", err)
	}
	if !strings.Contains(err.Error(), "TestWithDeadlineRecoversPanic") {
		t.Fatalf("err = %v, want the panicking frame", err)
	}
}

func TestWithDeadlineRecoveredPanicIsDeterministic(t *testing.T) {
	read := func() (int, error) { panic("boom") }
	_, a := withDeadline(context.Background(), time.Second, read)
	_, b := withDeadline(context.Background(), time.Second, read)
	if a == nil || b == nil || a.Error() != b.Error() {
		t.Fatalf("errors differ: %v and %v", a, b)
	}
	if len(a.Error()) > 512 || strings.Contains(a.Error(), "goroutine") {
		t.Fatalf("err = %q, want a short text without a goroutine id", a)
	}
}

func TestWithDeadlineRecoveredNilDereferenceNamesTheRead(t *testing.T) {
	var p *struct{ n int }
	_, err := withDeadline(context.Background(), time.Second, func() (int, error) {
		return p.n, nil
	})
	if err == nil || !strings.Contains(err.Error(), "the read panicked") || !strings.Contains(err.Error(), "chain_test.go:") {
		t.Fatalf("err = %v, want the panicking frame in chain_test.go, past the runtime frames", err)
	}
	if strings.Contains(err.Error(), "runtime.") {
		t.Fatalf("err = %v, want no runtime frame", err)
	}
}

func TestChainCachesDomainRecord(t *testing.T) {
	dir, cs := buildA(t)
	setup := func(t *testing.T) (*rig, *fakeSettlement, *clock) {
		s := newFakeSettlement()
		s.height = 10
		s.accept(cs[0], 20)
		r := newTestChain(t, dir, s)
		clk := &clock{t: time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC)}
		r.c.now = clk.now
		return r, s, clk
	}
	setRecord := func(s *fakeSettlement, h types.Hash) {
		s.mu.Lock()
		defer s.mu.Unlock()
		s.record = &definition.DomainRecord{StfSpecHash: h}
	}

	t.Run("refresh interval", func(t *testing.T) {
		r, s, clk := setup(t)
		setRecord(s, types.Hash{0xA1})
		doPoll(t, r)
		if rec := r.c.domainRecord(); rec == nil || rec.StfSpecHash != (types.Hash{0xA1}) {
			t.Fatalf("record = %+v, want 0xA1", rec)
		}
		setRecord(s, types.Hash{0xA2})
		doPoll(t, r)
		if rec := r.c.domainRecord(); rec.StfSpecHash != (types.Hash{0xA1}) {
			t.Fatalf("record = %+v before the interval, want 0xA1", rec)
		}
		clk.t = clk.t.Add(recordInterval)
		doPoll(t, r)
		if rec := r.c.domainRecord(); rec.StfSpecHash != (types.Hash{0xA2}) {
			t.Fatalf("record = %+v after the interval, want 0xA2", rec)
		}
	})

	t.Run("invisible record", func(t *testing.T) {
		r, s, _ := setup(t)
		s.record = nil
		doPoll(t, r)
		if rec := r.c.domainRecord(); rec != nil {
			t.Fatalf("record = %+v, want nil", rec)
		}
		setRecord(s, types.Hash{0xA1})
		doPoll(t, r)
		if r.c.domainRecord() == nil {
			t.Fatal("record still nil after it became visible")
		}
	})

	t.Run("read error", func(t *testing.T) {
		r, s, _ := setup(t)
		s.recordErr = errors.New("record boom")
		err := r.c.Poll(context.Background())
		if err == nil || !strings.Contains(err.Error(), "settlement") || !strings.Contains(err.Error(), "record boom") {
			t.Fatalf("Poll err = %v, want a settlement error naming record boom", err)
		}
		if tagID(r.c.Head().Latest) != 0 {
			t.Fatalf("latest = %d, want 0", tagID(r.c.Head().Latest))
		}
		s.recordErr = nil
		doPoll(t, r)
		if r.c.domainRecord() == nil {
			t.Fatal("record nil after the read recovered")
		}
	})
}

// vanishingRecord serves batch 1's record once and reports it gone
// after, as a record the executor is rewriting.
type vanishingRecord struct {
	*LocalSource
	calls atomic.Int32
}

func (s *vanishingRecord) Record(id uint64) (recovery.BatchRecord, bool, error) {
	if id == 1 && s.calls.Add(1) > 1 {
		return recovery.BatchRecord{}, false, nil
	}
	return s.LocalSource.Record(id)
}

func TestFailPendingRereadsTheRecord(t *testing.T) {
	setup := func(t *testing.T) (*rig, string) {
		dir, cs := buildA(t)
		flipLastByte(t, sidecarPath(dir, cs[1]))
		s := newFakeSettlement()
		s.height = 10
		return newTestChain(t, dir, s), dir
	}

	r, dir := setup(t)
	r.c.cfg.Source = &vanishingRecord{LocalSource: NewLocalSource(dir, 1)}
	h := doPoll(t, r)
	if h.Status == StatusInconsistent || tagID(h.Pending) != 0 {
		t.Fatalf("head = pending %d %s, want pending 0 and not INCONSISTENT\n%s", tagID(h.Pending), h.Status, r.log.dump())
	}
	if r.log.count("ALARM") != 0 {
		t.Fatalf("a rewritten record raised an alarm:\n%s", r.log.dump())
	}

	r, _ = setup(t)
	h = doPoll(t, r)
	if h.Status != StatusInconsistent || h.InconsistentAt == nil || *h.InconsistentAt != 1 {
		t.Fatalf("head = %s at %v, want INCONSISTENT at 1\n%s", h.Status, h.InconsistentAt, r.log.dump())
	}
}
