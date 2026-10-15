package gateway

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/zenon-network/go-zenon/common/types"
	"github.com/zenon-network/go-zenon/vm/embedded/definition"

	"github.com/digitalSloth/go-executor/domain"
	"github.com/digitalSloth/go-executor/internal/codec"
	"github.com/digitalSloth/go-executor/internal/recovery"
)

func addrBytes(a types.Address) []byte { return a[:] }

func hb(b byte) types.Hash { return types.Hash{b} }

func openTestIndex(t *testing.T) *Index {
	t.Helper()
	x, err := openIndex(t.TempDir(), 1, &fakeSenders{})
	if err != nil {
		t.Fatalf("openIndex: %v", err)
	}
	t.Cleanup(func() { _ = x.Close() })
	return x
}

// nb is an empty batch n with the given daHash and receiptRoot.
func nb(n uint64, d, r types.Hash) indexBatch {
	return indexBatch{domainID: 1, id: n, want: expect{batchID: n, daHash: d, receiptRoot: r}}
}

func mustAdd(t *testing.T, x *Index, b indexBatch, accepted bool) {
	t.Helper()
	if err := x.add(context.Background(), b, accepted); err != nil {
		t.Fatalf("add batch %d: %v", b.id, err)
	}
}

func requireConflict(t *testing.T, err error) {
	t.Helper()
	if !errors.Is(err, errIndexConflict) {
		t.Fatalf("err = %v, want errIndexConflict", err)
	}
}

func has(t *testing.T, x *Index, key []byte) bool {
	t.Helper()
	ok, err := x.db.Has(key, nil)
	if err != nil {
		t.Fatal(err)
	}
	return ok
}

func u64At(t *testing.T, x *Index, key []byte) (uint64, bool) {
	t.Helper()
	v, ok, err := x.live().getU64(key)
	if err != nil {
		t.Fatal(err)
	}
	return v, ok
}

func markerOf(t *testing.T, x *Index) (uint64, bool) {
	t.Helper()
	return u64At(t, x, []byte(keyMarker))
}

func finalizedOf(t *testing.T, x *Index) (uint64, bool) {
	t.Helper()
	return u64At(t, x, []byte(keyFinalized))
}

func manifestOf(t *testing.T, x *Index, id uint64) (manifestValue, bool) {
	t.Helper()
	m, ok, err := x.live().manifestAt(id)
	if err != nil {
		t.Fatal(err)
	}
	return m, ok
}

func requireManifest(t *testing.T, x *Index, id uint64, d, r types.Hash, accepted bool) {
	t.Helper()
	m, ok := manifestOf(t, x, id)
	if !ok || m.DAHash != d || m.ReceiptRoot != r || m.Accepted != accepted {
		t.Fatalf("manifest %d = %+v (present %v), want daHash %s receiptRoot %s accepted %v", id, m, ok, d, r, accepted)
	}
}

func requireMarker(t *testing.T, x *Index, want uint64) {
	t.Helper()
	if m, ok := markerOf(t, x); !ok || m != want {
		t.Fatalf("marker = %d (present %v), want %d", m, ok, want)
	}
}

func requireDumpUnchanged(t *testing.T, x *Index, before map[string]string) {
	t.Helper()
	after := indexDump(t, x)
	if len(after) != len(before) {
		t.Fatalf("the index changed: %d keys, was %d", len(after), len(before))
	}
	for k, v := range before {
		if after[k] != v {
			t.Fatalf("the index changed at key %q", k)
		}
	}
}

func TestIndexManifestRules(t *testing.T) {
	x := openTestIndex(t)
	A, B, C := hb(0xA), hb(0xB), hb(0xC)
	R0, R1, R2 := hb(0x10), hb(0x11), hb(0x12)

	mustAdd(t, x, nb(0, A, R0), true)
	mustAdd(t, x, nb(1, B, R1), false)
	mustAdd(t, x, nb(2, C, R2), false)
	requireMarker(t, x, 2)
	requireManifest(t, x, 0, A, R0, true)
	requireManifest(t, x, 1, B, R1, false)
	requireManifest(t, x, 2, C, R2, false)
	for i, d := range []types.Hash{A, B, C} {
		if !has(t, x, ikey(pfxBatch, be64(uint64(i)))) {
			t.Fatalf("b/%d is missing", i)
		}
		if id, ok := u64At(t, x, ikey(pfxDAHash, d[:])); !ok || id != uint64(i) {
			t.Fatalf("B/%s = %d (present %v), want %d", d, id, ok, i)
		}
	}

	mustAdd(t, x, nb(1, B, R1), true)
	requireManifest(t, x, 1, B, R1, true)
	mustAdd(t, x, nb(1, B, R1), false)
	requireManifest(t, x, 1, B, R1, true)

	C2, R2x := hb(0xC2), hb(0x1F)
	mustAdd(t, x, nb(2, C2, R2), false)
	requireManifest(t, x, 2, C2, R2, false)
	requireMarker(t, x, 2)
	if has(t, x, ikey(pfxDAHash, C[:])) {
		t.Fatal("B/<C> survived the replacement")
	}
	mustAdd(t, x, nb(2, C2, R2x), true)
	requireManifest(t, x, 2, C2, R2x, true)

	before := indexDump(t, x)
	requireConflict(t, x.add(context.Background(), nb(1, B, hb(0x1E)), true))
	requireConflict(t, x.add(context.Background(), nb(1, hb(0xB2), R1), true))
	requireDumpUnchanged(t, x, before)

	if err := x.add(context.Background(), nb(4, hb(0xD), R0), false); err == nil || !strings.Contains(err.Error(), "does not follow the last indexed batch") {
		t.Fatalf("add 4 = %v, want the does-not-follow error", err)
	}
}

func TestIndexMarkers(t *testing.T) {
	x := openTestIndex(t)
	A, B, C := hb(0xA), hb(0xB), hb(0xC)
	R0, R1, R2 := hb(0x10), hb(0x11), hb(0x12)
	mustAdd(t, x, nb(0, A, R0), true)
	mustAdd(t, x, nb(1, B, R1), true)
	mustAdd(t, x, nb(2, C, R2), true)

	if err := x.finalize(0); err != nil {
		t.Fatal(err)
	}
	if _, ok := manifestOf(t, x, 0); ok {
		t.Fatal("manifest 0 survived finalize")
	}
	if f, ok := finalizedOf(t, x); !ok || f != 0 {
		t.Fatalf("F = %d (present %v), want 0", f, ok)
	}
	if !has(t, x, ikey(pfxBatch, be64(0))) {
		t.Fatal("b/0 was deleted")
	}
	if err := x.finalize(0); err != nil {
		t.Fatal(err)
	}
	if f, _ := finalizedOf(t, x); f != 0 {
		t.Fatalf("F = %d after a second finalize, want 0", f)
	}

	before := indexDump(t, x)
	mustAdd(t, x, nb(0, A, R0), false)
	requireDumpUnchanged(t, x, before)
	requireConflict(t, x.add(context.Background(), nb(0, A, hb(0x1F)), true))
	requireConflict(t, x.add(context.Background(), nb(0, hb(0xA2), R0), true))
	requireConflict(t, x.retract(0))
	requireDumpUnchanged(t, x, before)

	if err := x.retract(1); err != nil {
		t.Fatal(err)
	}
	requireMarker(t, x, 0)
	for _, id := range []uint64{1, 2} {
		if _, ok := manifestOf(t, x, id); ok {
			t.Fatalf("manifest %d survived the retraction", id)
		}
		if has(t, x, ikey(pfxBatch, be64(id))) {
			t.Fatalf("b/%d survived the retraction", id)
		}
	}
	if !has(t, x, ikey(pfxBatch, be64(0))) {
		t.Fatal("b/0 was deleted")
	}
	if f, _ := finalizedOf(t, x); f != 0 {
		t.Fatalf("F = %d, want 0", f)
	}

	mustAdd(t, x, nb(1, B, R1), false)
	requireManifest(t, x, 1, B, R1, false)
	requireMarker(t, x, 1)
}

func TestIndexFirstWriterWins(t *testing.T) {
	x := openTestIndex(t)
	H, H2, X := hb(0x51), hb(0x52), hb(0xD1)
	b0 := nb(0, hb(0xA), hb(0x10))
	b0.inputs = []batchInput{synthInput(t, 1, H, 4, codec.StatusSuccess, X, nil)}
	mustAdd(t, x, b0, true)
	if err := x.finalize(0); err != nil {
		t.Fatal(err)
	}
	if g, ok := u64At(t, x, ikey(pfxSendHash, H[:])); !ok || g != 1 {
		t.Fatalf("s/H = %d (present %v), want 1", g, ok)
	}
	if g, ok := u64At(t, x, ikey(pfxDeploy, X[:])); !ok || g != 1 {
		t.Fatalf("f/X = %d (present %v), want 1", g, ok)
	}

	b1 := nb(1, hb(0xB), hb(0x11))
	b1.inputs = []batchInput{
		synthInput(t, 2, H, 4, codec.StatusSuccess, X, nil),
		synthInput(t, 3, H2, 0, codec.StatusSuccess, types.Hash{}, nil),
		synthInput(t, 4, H2, 0, codec.StatusSuccess, types.Hash{}, nil),
	}
	mustAdd(t, x, b1, false)
	if g, _ := u64At(t, x, ikey(pfxSendHash, H[:])); g != 1 {
		t.Fatalf("s/H = %d, want 1", g)
	}
	if g, _ := u64At(t, x, ikey(pfxDeploy, X[:])); g != 1 {
		t.Fatalf("f/X = %d, want 1", g)
	}
	if g, _ := u64At(t, x, ikey(pfxSendHash, H2[:])); g != 3 {
		t.Fatalf("s/H2 = %d, want 3", g)
	}
	v := x.live()
	in2, ok, err := v.inputAt(2)
	if err != nil || !ok || in2.SendHash != H {
		t.Fatalf("n/2 = %+v, %v, %v; want SendHash H", in2, ok, err)
	}

	m, _ := manifestOf(t, x, 1)
	count := func(key []byte) int {
		n := 0
		for _, k := range m.Keys {
			if string(k) == string(key) {
				n++
			}
		}
		return n
	}
	if count(ikey(pfxSendHash, H2[:])) != 1 || count(ikey(pfxSendHash, H[:])) != 0 || count(ikey(pfxDeploy, X[:])) != 0 {
		t.Fatal("manifest 1 lists the wrong first-writer keys")
	}

	if err := x.retract(1); err != nil {
		t.Fatal(err)
	}
	if g, _ := u64At(t, x, ikey(pfxSendHash, H[:])); g != 1 {
		t.Fatalf("s/H = %d after the retraction, want 1", g)
	}
	if !has(t, x, ikey(pfxDeploy, X[:])) || !has(t, x, ikey(pfxInput, be64(1))) {
		t.Fatal("f/X or n/1 was deleted")
	}
	for _, k := range [][]byte{ikey(pfxSendHash, H2[:]), ikey(pfxInput, be64(2)), ikey(pfxInput, be64(3)), ikey(pfxInput, be64(4))} {
		if has(t, x, k) {
			t.Fatalf("key %q survived the retraction", k)
		}
	}
	if !has(t, x, ikey(pfxSender, H[:])) || !has(t, x, ikey(pfxSender, H2[:])) {
		t.Fatal("S/ entries were deleted")
	}
}

func TestIndexScan(t *testing.T) {
	x := openTestIndex(t)
	b := nb(0, hb(0xA), hb(0x10))
	for i, g := range []uint64{1, 2, 3, 5} {
		b.inputs = append(b.inputs, synthInput(t, g, hb(byte(0x60+i)), 0, codec.StatusSuccess, types.Hash{}, nil))
	}
	mustAdd(t, x, b, true)
	v, err := x.view()
	if err != nil {
		t.Fatal(err)
	}
	defer v.Release()

	collect := func(lo, hi uint64, before []byte, max int) (gs []uint64, stop []byte, visited int) {
		stop, visited, err := v.scanDesc([]byte(pfxInput), lo, hi, before, max, func(suffix, _ []byte) (bool, error) {
			gs = append(gs, binary.BigEndian.Uint64(suffix))
			return true, nil
		})
		if err != nil {
			t.Fatal(err)
		}
		return gs, stop, visited
	}
	same := func(got []uint64, want ...uint64) {
		t.Helper()
		if len(got) != len(want) {
			t.Fatalf("got %v, want %v", got, want)
		}
		for i := range got {
			if got[i] != want[i] {
				t.Fatalf("got %v, want %v", got, want)
			}
		}
	}

	gs, stop, visited := collect(0, math.MaxUint64, nil, 10)
	same(gs, 5, 3, 2, 1)
	if stop != nil || visited != 4 {
		t.Fatalf("stop %v visited %d, want nil and 4", stop, visited)
	}
	gs, stop, visited = collect(0, math.MaxUint64, nil, 2)
	same(gs, 5, 3)
	if string(stop) != string(be64(3)) || visited != 2 {
		t.Fatalf("stop %x visited %d, want be64(3) and 2", stop, visited)
	}
	gs, _, _ = collect(0, math.MaxUint64, be64(3), 10)
	same(gs, 2, 1)
	gs, _, _ = collect(2, 3, nil, 10)
	same(gs, 3, 2)

	var n int
	stop, visited, err = v.scanDesc([]byte(pfxInput), 0, math.MaxUint64, nil, 10, func(_, _ []byte) (bool, error) {
		n++
		return false, nil
	})
	if err != nil || n != 1 || stop != nil || visited != 1 {
		t.Fatalf("early stop: n %d stop %v visited %d err %v", n, stop, visited, err)
	}
}

func TestIndexViewIsConsistent(t *testing.T) {
	x := openTestIndex(t)
	mustAdd(t, x, nb(0, hb(0xA), hb(0x10)), true)
	b1 := nb(1, hb(0xB), hb(0x11))
	b1.inputs = []batchInput{synthInput(t, 2, hb(0x51), 0, codec.StatusSuccess, types.Hash{}, [][]byte{[]byte("ev")})}
	mustAdd(t, x, b1, false)

	old, err := x.view()
	if err != nil {
		t.Fatal(err)
	}
	defer old.Release()
	if err := x.retract(1); err != nil {
		t.Fatal(err)
	}
	if _, ok, err := old.inputAt(2); err != nil || !ok {
		t.Fatalf("old view inputAt(2) = %v, %v; want present", ok, err)
	}
	evs, err := old.eventsOf(2, 1)
	if err != nil || len(evs) != 1 || string(evs[0]) != "ev" {
		t.Fatalf("old view eventsOf = %q, %v", evs, err)
	}
	if m, ok, err := old.marker(); err != nil || !ok || m != 1 {
		t.Fatalf("old view marker = %d, %v, %v; want 1", m, ok, err)
	}

	fresh, err := x.view()
	if err != nil {
		t.Fatal(err)
	}
	defer fresh.Release()
	if _, ok, _ := fresh.inputAt(2); ok {
		t.Fatal("new view still holds input 2")
	}
	if m, _, _ := fresh.marker(); m != 0 {
		t.Fatalf("new view marker = %d, want 0", m)
	}
}

func TestOpenIndexRefusesSymlinkedIndex(t *testing.T) {
	storage := t.TempDir()
	target := t.TempDir()
	if err := os.WriteFile(filepath.Join(target, "keep"), []byte("k"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(storage, "1"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(target, filepath.Join(storage, "1", "index")); err != nil {
		t.Fatal(err)
	}
	if _, err := openIndex(storage, 1, nil); err == nil || !strings.Contains(err.Error(), "symlink") {
		t.Fatalf("openIndex = %v, want a symlink refusal", err)
	}
	if b, err := os.ReadFile(filepath.Join(target, "keep")); err != nil || string(b) != "k" {
		t.Fatalf("keep = %q, %v", b, err)
	}
	if entries, _ := os.ReadDir(target); len(entries) != 1 {
		t.Fatalf("the link target was written to: %v", entries)
	}
}

// indexInputs maps history A's global input indexes to their batch.
var indexInputs = map[uint64]uint64{1: 0, 2: 0, 3: 1, 4: 2, 6: 2, 7: 3, 8: 4}

func acceptAt(s *fakeSettlement, cs []codec.BatchCommitment, finalizeAt uint64, ids ...int) {
	for _, i := range ids {
		s.accept(cs[i], finalizeAt)
	}
}

// requireManifestsCoverKeys requires every indexed key to be in
// exactly one manifest and every manifest key to exist.
func requireManifestsCoverKeys(t *testing.T, x *Index) {
	t.Helper()
	listed := make(map[string]int)
	for k, v := range indexEntries(t, x, func(k string) bool { return !strings.HasPrefix(k, pfxManifest) }) {
		_ = v
		var m manifestValue
		raw, err := x.db.Get([]byte(k), nil)
		if err != nil {
			t.Fatal(err)
		}
		if err := json.Unmarshal(raw, &m); err != nil {
			t.Fatal(err)
		}
		for _, key := range m.Keys {
			listed[string(key)]++
			if !has(t, x, key) {
				t.Fatalf("manifest key %q does not exist", key)
			}
		}
	}
	for k := range indexKeys(t, x) {
		if listed[k] != 1 {
			t.Fatalf("key %q is listed in %d manifests, want 1", k, listed[k])
		}
	}
}

func TestIndexBatchKeys(t *testing.T) {
	dir, cs := buildA(t)
	s := newFakeSettlement()
	s.height = 10
	acceptAt(s, cs, 100, 0, 1)
	r := newIndexedChain(t, dir, s)
	requireTags(t, doPoll(t, r), 4, 1, -1, StatusOK)
	x := r.idx

	requireMarker(t, x, 4)
	if _, ok := finalizedOf(t, x); ok {
		t.Fatal("F is set")
	}
	for i := uint64(0); i <= 4; i++ {
		requireManifest(t, x, i, cs[i].DAHash, cs[i].ReceiptRoot, i <= 1)
	}
	if r.senders.totalCalls() != 7 {
		t.Fatalf("sender reads = %d, want 7", r.senders.totalCalls())
	}

	zero := types.Hash{}
	for _, g := range []uint64{1, 2, 3, 4, 6, 7, 8} {
		b := indexInputs[g]
		h := hashOfIndex(g)
		if got, ok := u64At(t, x, ikey(pfxSendHash, h[:])); !ok || got != g {
			t.Fatalf("s/%d = %d (present %v)", g, got, ok)
		}
		in, ok, err := x.live().inputAt(g)
		if err != nil || !ok {
			t.Fatalf("n/%d: %v, %v", g, ok, err)
		}
		_, leaves, err := codec.DecodeReceiptSidecar(readFile(t, sidecarPath(dir, cs[b])))
		if err != nil {
			t.Fatal(err)
		}
		pos := 0
		if g == 2 || g == 6 {
			pos = 1
		}
		if in.BatchID != b || in.SendHash != h || in.Sender != senderOf(h) || string(in.Receipt) != string(leaves[pos]) {
			t.Fatalf("n/%d = batch %d sender %v, want batch %d sender %v and the sidecar leaf", g, in.BatchID, in.Sender, b, senderOf(h))
		}
		if !has(t, x, ikey(pfxKind, []byte{0}, be64(g))) || !has(t, x, ikey(pfxSenderIdx, addrBytes(senderOf(h)), be64(g))) {
			t.Fatalf("k/ or a/ for input %d is missing", g)
		}
		wantC := g != 2 && g != 4
		if has(t, x, ikey(pfxContract, hb(0xC0).Bytes(), be64(g))) != wantC {
			t.Fatalf("c/<C0> for input %d: present %v, want %v", g, !wantC, wantC)
		}
		if has(t, x, ikey(pfxContract, zero[:], be64(g))) {
			t.Fatalf("c/ for a zero contract id exists for input %d", g)
		}
	}

	ev, err := codec.Event{ContractID: hb(0xC0), InputIndex: 1, EventIndex: 0, Data: []byte("evt")}.Encode()
	if err != nil {
		t.Fatal(err)
	}
	v, err := x.view()
	if err != nil {
		t.Fatal(err)
	}
	defer v.Release()
	evs, err := v.eventsOf(1, 1)
	if err != nil || len(evs) != 1 || string(evs[0]) != string(ev) {
		t.Fatalf("eventsOf(1, 1) = %x, %v; want %x", evs, err, ev)
	}
	if !has(t, x, ikey(pfxEventCtr, hb(0xC0).Bytes(), be64(1), be32(0))) {
		t.Fatal("ec/ for input 1 is missing")
	}
	outs, err := v.outboxOf(4, 1)
	if err != nil || len(outs) != 1 {
		t.Fatalf("outboxOf(4, 1) = %x, %v", outs, err)
	}
	msg, err := codec.DecodeOutboxMessage(outs[0])
	if err != nil || msg.InputIndex != 4 {
		t.Fatalf("outbox message = %+v, %v", msg, err)
	}
	if !has(t, x, ikey(pfxOutboxTo, addrBytes(types.Address{1}), be64(4), be32(0))) {
		t.Fatal("oa/ for input 4 is missing")
	}
	oid := codec.OutboxID(1, 2, 4, 0)
	if val, ok, err := v.get(ikey(pfxOutboxID, oid[:])); err != nil || !ok || string(val) != string(append(be64(4), be32(0)...)) {
		t.Fatalf("x/ = %x, %v, %v", val, ok, err)
	}

	bv, ok, err := v.batchAt(2)
	if err != nil || !ok {
		t.Fatalf("b/2: %v, %v", ok, err)
	}
	if len(bv.Inputs) != 2 || bv.Inputs[0] != 4 || bv.Inputs[1] != 6 || bv.FirstInputSeq != cs[2].FirstInputSeq || bv.LastInputSeq != cs[2].LastInputSeq ||
		bv.PreStateRoot != cs[2].PreStateRoot || bv.PostStateRoot != cs[2].PostStateRoot || bv.InputRoot != cs[2].InputRoot ||
		bv.ReceiptRoot != cs[2].ReceiptRoot || bv.EventRoot != cs[2].EventRoot || bv.OutboxRoot != cs[2].OutboxRoot || bv.DAHash != cs[2].DAHash {
		t.Fatalf("b/2 = %+v, want cs[2]'s fields and inputs [4 6]", bv)
	}
	for i := uint64(0); i <= 4; i++ {
		if id, ok, err := v.batchByDAHash(cs[i].DAHash); err != nil || !ok || id != i {
			t.Fatalf("B/ for batch %d = %d, %v, %v", i, id, ok, err)
		}
	}
	requireManifestsCoverKeys(t, x)
}

func TestIndexDeployKeys(t *testing.T) {
	dir := t.TempDir()
	cs := buildHistory(t, dir, nil, historyC())
	s := newFakeSettlement()
	s.height = 10
	acceptAt(s, cs, 100, 0)
	r := newIndexedChain(t, dir, s)
	doPoll(t, r)
	x := r.idx

	X, Y, H, H2 := hb(0xD1), hb(0xD2), hb(0xE1), hb(0xE3)
	for _, g := range []uint64{1, 2, 3, 5} {
		if !has(t, x, ikey(pfxContract, X[:], be64(g))) {
			t.Fatalf("c/<X> for input %d is missing", g)
		}
	}
	if !has(t, x, ikey(pfxContract, Y[:], be64(4))) {
		t.Fatal("c/<Y> for input 4 is missing")
	}
	if g, ok := u64At(t, x, ikey(pfxDeploy, X[:])); !ok || g != 2 {
		t.Fatalf("f/<X> = %d (present %v), want 2", g, ok)
	}
	sender := senderOf(hashOfIndex(2))
	if val, err := x.db.Get(ikey(pfxDeployer, addrBytes(sender), be64(2)), nil); err != nil || string(val) != string(X[:]) {
		t.Fatalf("d/ = %x, %v; want X", val, err)
	}
	if !has(t, x, ikey(pfxCodeHash, H[:], be64(2))) || !has(t, x, ikey(pfxCodeHash, H2[:], be64(5))) {
		t.Fatal("h/ keys are missing")
	}
	v, err := x.view()
	if err != nil {
		t.Fatal(err)
	}
	defer v.Release()
	if g, ok, err := v.deployOf(X); err != nil || !ok || g != 2 {
		t.Fatalf("deployOf(X) = %d, %v, %v; want 2", g, ok, err)
	}
}

func TestIndexOwnerAndChunkKeys(t *testing.T) {
	x1 := hb(0xC1)
	h := hb(0xE1)
	ok := func(c types.Hash) domain.Effects {
		return domain.Effects{Status: codec.StatusSuccess, ContractID: c}
	}
	fail := func(c types.Hash) domain.Effects {
		return domain.Effects{Status: codec.StatusUnauthorized, ContractID: c}
	}
	chunk := func(i uint32) codec.InputArgs {
		return codec.DeployChunkArgs{CodeHash: h, ChunkIndex: i, ChunkHash: hb(byte(i))}
	}
	transfer := codec.TransferOwnershipArgs{ContractID: x1, NewOwner: types.Address{7}}
	dir := t.TempDir()
	cs := buildHistory(t, dir, nil, [][]fixtureInput{
		{
			{index: 1, args: chunk(0), chunk: []byte("chunk-one"), effects: ok(types.Hash{})},
			{index: 2, args: chunk(1), chunk: []byte("c2"), effects: domain.Effects{Status: codec.StatusValidationFailed}},
			{index: 3, args: codec.DeployFinalizeArgs{CodeHash: h}, effects: ok(x1)},
			{index: 4, args: transfer, effects: ok(x1)},
		},
		{
			{index: 5, args: transfer, effects: fail(x1)},
			{index: 6, args: codec.UpgradeArgs{ContractID: x1, NewCodeHash: hb(0xE2)}, effects: ok(x1)},
			{index: 7, args: codec.DeployFinalizeArgs{CodeHash: h}, effects: fail(x1)},
			{index: 8, args: chunk(2), effects: ok(types.Hash{})},
			{index: 9, args: transfer, effects: ok(x1)},
		},
	})
	exec := t.TempDir()
	copyDir(t, dir, exec)
	s := newFakeSettlement()
	s.height = 10
	acceptAt(s, cs, 100, 0)
	r := newIndexedChain(t, exec, s)
	x := r.idx
	if has(t, x, []byte(keyMetaSchema)) {
		t.Fatal("a new index holds meta/schema before its first write")
	}
	requireTags(t, doPoll(t, r), 1, 0, -1, StatusOK)

	owner := func(g uint64) bool { return has(t, x, ikey(pfxOwnerSet, x1[:], be64(g))) }
	for g, want := range map[uint64]bool{3: true, 4: true, 5: false, 6: false, 7: false, 9: true} {
		if owner(g) != want {
			t.Fatalf("w/<X> for input %d present = %v, want %v", g, !want, want)
		}
	}
	chunkOf := func(g uint64) (string, bool) {
		v, err := x.db.Get(ikey(pfxChunk, be64(g)), nil)
		if err != nil {
			return "", false
		}
		return string(v), true
	}
	for g, want := range map[uint64]string{1: "chunk-one", 2: "c2", 8: ""} {
		if got, ok := chunkOf(g); !ok || got != want {
			t.Fatalf("D/ for input %d = %q (present %v), want %q", g, got, ok, want)
		}
	}
	for _, g := range []uint64{3, 4, 5, 6, 7, 9} {
		if _, ok := chunkOf(g); ok {
			t.Fatalf("D/ for input %d, which is not a chunk", g)
		}
	}
	requireManifestsCoverKeys(t, x)
	if v, ok := u64At(t, x, []byte(keyMetaSchema)); !ok || v != 1 {
		t.Fatalf("meta/schema = %d (present %v), want 1", v, ok)
	}
	for id := uint64(0); id <= 1; id++ {
		m, found := manifestOf(t, x, id)
		if !found {
			t.Fatalf("manifest %d is missing", id)
		}
		for _, k := range m.Keys {
			if string(k) == keyMetaSchema {
				t.Fatalf("manifest %d lists meta/schema", id)
			}
		}
	}

	removeRecord(t, exec, 1)
	requireTags(t, doPoll(t, r), 0, 0, -1, StatusOK)
	if owner(9) || !owner(3) || !owner(4) {
		t.Fatalf("after the retraction: w/ for inputs 3, 4, 9 present = %v, %v, %v", owner(3), owner(4), owner(9))
	}
	if _, ok := chunkOf(8); ok {
		t.Fatal("D/ of the retracted batch survived")
	}
	if _, ok := chunkOf(1); !ok {
		t.Fatal("D/ of a kept batch was removed")
	}
	if v, ok := u64At(t, x, []byte(keyMetaSchema)); !ok || v != 1 {
		t.Fatalf("meta/schema after the retraction = %d (present %v), want 1", v, ok)
	}
	requireManifestsCoverKeys(t, x)
}

func TestIndexRetraction(t *testing.T) {
	pristine, cs := buildA(t)
	exec := t.TempDir()
	copyDir(t, pristine, exec)
	s := newFakeSettlement()
	s.height = 10
	acceptAt(s, cs, 100, 0, 1)
	r := newIndexedChain(t, exec, s)
	requireTags(t, doPoll(t, r), 4, 1, -1, StatusOK)
	x := r.idx
	before := indexKeys(t, x)
	removed := make(map[string]bool)
	for id := uint64(2); id <= 4; id++ {
		m, ok := manifestOf(t, x, id)
		if !ok {
			t.Fatalf("manifest %d is missing", id)
		}
		for _, k := range m.Keys {
			removed[string(k)] = true
		}
	}

	removeRecord(t, exec, 2)
	requireTags(t, doPoll(t, r), 1, 1, -1, StatusOK)
	requireMarker(t, x, 1)
	for id := uint64(2); id <= 4; id++ {
		if _, ok := manifestOf(t, x, id); ok {
			t.Fatalf("manifest %d survived", id)
		}
	}
	after := indexKeys(t, x)
	for k := range before {
		if removed[k] == after[k] {
			t.Fatalf("key %q: removed %v but present %v", k, removed[k], after[k])
		}
	}
	for _, g := range []uint64{4, 6, 7, 8} {
		h := hashOfIndex(g)
		if !has(t, x, ikey(pfxSender, h[:])) {
			t.Fatalf("S/ for input %d was deleted", g)
		}
	}
	v, err := x.view()
	if err != nil {
		t.Fatal(err)
	}
	h8 := hashOfIndex(8)
	if _, ok, err := v.inputBySendHash(h8); err != nil || ok {
		t.Fatalf("inputBySendHash(8) = %v, %v; want a miss", ok, err)
	}
	v.Release()

	copyDir(t, pristine, exec)
	requireTags(t, doPoll(t, r), 4, 1, -1, StatusOK)
	again := indexKeys(t, x)
	if len(again) != len(before) {
		t.Fatalf("keys after the rebuild = %d, want %d", len(again), len(before))
	}
	for k := range before {
		if !again[k] {
			t.Fatalf("key %q is missing after the rebuild", k)
		}
	}
	if n := r.senders.totalCalls(); n != 7 {
		t.Fatalf("sender reads = %d, want 7", n)
	}
}

func TestIndexAcceptedEntryDiffers(t *testing.T) {
	dir, cs := buildA(t)
	bdir := t.TempDir()
	csB := buildHistory(t, bdir, nil, historyB())
	copyBatch(t, bdir, dir, csB[3])
	removeRecord(t, dir, 4)
	s := newFakeSettlement()
	s.height = 10
	acceptAt(s, cs, 100, 0, 1, 2)
	r := newIndexedChain(t, dir, s)

	requireTags(t, doPoll(t, r), 3, 2, -1, StatusOK)
	requireManifest(t, r.idx, 3, csB[3].DAHash, csB[3].ReceiptRoot, false)

	s.accept(cs[3], 100)
	requireTags(t, doPoll(t, r), 3, 3, -1, StatusOK)
	requireManifest(t, r.idx, 3, cs[3].DAHash, cs[3].ReceiptRoot, true)
	_, leaves, err := codec.DecodeReceiptSidecar(readFile(t, sidecarPath(dir, cs[3])))
	if err != nil {
		t.Fatal(err)
	}
	in, ok, err := r.idx.live().inputAt(7)
	if err != nil || !ok || string(in.Receipt) != string(leaves[0]) {
		t.Fatalf("n/7 = %+v, %v, %v; want cs[3]'s leaf", in, ok, err)
	}
	if n := r.senders.totalCalls(); n != 6 {
		t.Fatalf("sender reads = %d, want 6", n)
	}
}

// receiptOf decodes input g's indexed receipt.
func receiptOf(t *testing.T, x *Index, g uint64) codec.Receipt {
	t.Helper()
	in, ok, err := x.live().inputAt(g)
	if err != nil || !ok {
		t.Fatalf("n/%d: %v, %v", g, ok, err)
	}
	r, err := codec.DecodeReceipt(in.Receipt)
	if err != nil {
		t.Fatal(err)
	}
	return r
}

// rewriteBatch4Gas bumps the first receipt of batch 4's sidecar in
// dir and returns the new receiptRoot and the original gas used.
func rewriteBatch4Gas(t *testing.T, dir string, cs []codec.BatchCommitment) (types.Hash, uint64) {
	t.Helper()
	var orig uint64
	root := rewriteSidecar(t, sidecarPath(dir, cs[4]), func(rs []codec.Receipt) {
		orig = rs[0].GasUsed
		rs[0].GasUsed++
	})
	return root, orig
}

func TestIndexReceiptsChanged(t *testing.T) {
	t.Run("in run", func(t *testing.T) {
		exec, cs := buildA(t)
		s := newFakeSettlement()
		s.height = 10
		acceptAt(s, cs, 100, 0, 1, 2, 3)
		r := newIndexedChain(t, exec, s)
		doPoll(t, r)
		requireManifest(t, r.idx, 4, cs[4].DAHash, cs[4].ReceiptRoot, false)
		r2, orig := rewriteBatch4Gas(t, exec, cs)
		s.accept(cs[4], 100)
		s.edit(4, func(e *definition.BatchEntry) { e.ReceiptRoot = r2 })

		requireTags(t, doPoll(t, r), 4, 4, -1, StatusOK)
		requireManifest(t, r.idx, 4, cs[4].DAHash, r2, true)
		if got := receiptOf(t, r.idx, 8).GasUsed; got != orig+1 {
			t.Fatalf("n/8 gas = %d, want %d", got, orig+1)
		}
		if n := r.senders.totalCalls(); n != 7 {
			t.Fatalf("sender reads = %d, want 7", n)
		}
	})
	t.Run("restart", func(t *testing.T) {
		pristine, cs := buildA(t)
		exec := t.TempDir()
		copyDir(t, pristine, exec)
		storage := t.TempDir()
		s := newFakeSettlement()
		s.height = 30
		for i, at := range []uint64{20, 21, 22, 40} {
			s.accept(cs[i], at)
		}
		senders := &fakeSenders{}
		a, _ := openTestChainWith(t, storage, exec, s, senders)
		if err := a.Poll(context.Background()); err != nil {
			t.Fatal(err)
		}
		closeChain(t, a)
		r2, orig := rewriteBatch4Gas(t, exec, cs)
		rewriteRecordRoot(t, exec, 4, r2)
		s.accept(cs[4], 100)
		s.edit(4, func(e *definition.BatchEntry) { e.ReceiptRoot = r2 })

		b, blog := openTestChainWith(t, storage, exec, s, senders)
		if blog.count("batch 4 and every indexed batch above it removed at boot", "Settlement accepted") != 1 {
			t.Fatalf("boot lines:\n%s", blog.dump())
		}
		if err := b.Poll(context.Background()); err != nil {
			t.Fatal(err)
		}
		requireTags(t, b.Head(), 4, 4, 2, StatusOK)
		requireManifest(t, b.cfg.Index, 4, cs[4].DAHash, r2, true)
		if got := receiptOf(t, b.cfg.Index, 8).GasUsed; got != orig+1 {
			t.Fatalf("n/8 gas = %d, want %d", got, orig+1)
		}
		if n := senders.totalCalls(); n != 7 {
			t.Fatalf("sender reads = %d, want 7", n)
		}
	})
}

// A held pending batch whose record's receiptRoot changes is retracted
// and indexed again, as the boot walk does.
func TestIndexPendingReceiptRootChanged(t *testing.T) {
	exec, cs := buildA(t)
	s := newFakeSettlement()
	s.height = 10
	acceptAt(s, cs, 100, 0, 1, 2, 3)
	r := newIndexedChain(t, exec, s)
	requireTags(t, doPoll(t, r), 4, 3, -1, StatusOK)
	requireManifest(t, r.idx, 4, cs[4].DAHash, cs[4].ReceiptRoot, false)

	r2, orig := rewriteBatch4Gas(t, exec, cs)
	rewriteRecordRoot(t, exec, 4, r2)
	requireTags(t, doPoll(t, r), 4, 3, -1, StatusOK)
	if r.log.count("pending batch 4 retracted") != 1 {
		t.Fatalf("want one retraction line:\n%s", r.log.dump())
	}
	requireManifest(t, r.idx, 4, cs[4].DAHash, r2, false)
	if got := receiptOf(t, r.idx, 8).GasUsed; got != orig+1 {
		t.Fatalf("n/8 gas = %d, want %d", got, orig+1)
	}
	if n := r.senders.totalCalls(); n != 7 {
		t.Fatalf("sender reads = %d, want 7", n)
	}
}

func TestIndexSenders(t *testing.T) {
	t.Run("fails then resumes", func(t *testing.T) {
		exec, _ := buildA(t)
		s := newFakeSettlement()
		s.height = 10
		r := newIndexedChain(t, exec, s)
		clk := &clock{t: time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC)}
		r.c.now = clk.now
		h1, h2 := hashOfIndex(1), hashOfIndex(2)
		first := true
		r.senders.fail = func(h types.Hash) error {
			if h == h2 && first {
				first = false
				return errors.New("node down")
			}
			return nil
		}
		err := r.c.Poll(context.Background())
		if err == nil || !strings.Contains(err.Error(), "read the L1 sender") {
			t.Fatalf("Poll = %v, want a sender read error", err)
		}
		if p := r.c.Head().Pending; p != nil {
			t.Fatalf("pending = %+v, want none", p)
		}
		if !has(t, r.idx, ikey(pfxSender, h1[:])) {
			t.Fatal("S/ for input 1 was not stored")
		}
		clk.t = clk.t.Add(senderRetryInterval)
		requireTags(t, doPoll(t, r), 4, -1, -1, StatusOK)
		if r.senders.callCount(h1) != 1 || r.senders.callCount(h2) != 2 {
			t.Fatalf("calls: input 1 %d, input 2 %d; want 1 and 2", r.senders.callCount(h1), r.senders.callCount(h2))
		}
	})
	t.Run("a hung read ends at the deadline", func(t *testing.T) {
		exec, _ := buildA(t)
		s := newFakeSettlement()
		s.height = 10
		r := newIndexedChain(t, exec, s)
		r.idx.timeout = 20 * time.Millisecond
		release := make(chan struct{})
		t.Cleanup(func() { close(release) })
		r.senders.fail = func(types.Hash) error {
			<-release
			return nil
		}
		finishesWithin(t, 2*time.Second, "Poll", func() {
			if err := r.c.Poll(context.Background()); !errors.Is(err, errNoAnswer) {
				t.Errorf("Poll = %v, want errNoAnswer", err)
			}
		})
	})
	t.Run("an accepted batch naming a foreign send is inconsistent", func(t *testing.T) {
		exec, cs := buildA(t)
		s := newFakeSettlement()
		s.height = 10
		acceptAt(s, cs, 100, 0)
		r := newIndexedChain(t, exec, s)
		r.senders.fail = func(h types.Hash) error {
			if h == hashOfIndex(1) {
				return fmt.Errorf("foreign: %w", errNotDomainInput)
			}
			return nil
		}
		h := doPoll(t, r)
		if h.Status != StatusInconsistent || r.log.count("ALARM", "batch 0", "not an input of this domain") != 1 {
			t.Fatalf("status %s, log:\n%s", h.Status, r.log.dump())
		}
		if r.log.count("ALARM", "differs from the one the index holds") != 0 {
			t.Fatalf("the alarm names a conflict the index does not hold:\n%s", r.log.dump())
		}
	})
	t.Run("a pending batch naming a foreign send stays transient", func(t *testing.T) {
		exec, _ := buildA(t)
		s := newFakeSettlement()
		s.height = 10
		r := newIndexedChain(t, exec, s)
		r.senders.fail = func(types.Hash) error { return fmt.Errorf("foreign: %w", errNotDomainInput) }
		err := r.c.Poll(context.Background())
		if err == nil || errors.Is(err, errIndexConflict) || r.c.Head().Status == StatusInconsistent {
			t.Fatalf("Poll = %v, status %s; want a plain error and no INCONSISTENT", err, r.c.Head().Status)
		}
	})
	t.Run("a pending batch naming a foreign send is read once until its record changes", func(t *testing.T) {
		exec, _ := buildA(t)
		s := newFakeSettlement()
		s.height = 10
		r := newIndexedChain(t, exec, s)
		r.senders.fail = func(types.Hash) error { return fmt.Errorf("foreign: %w", errNotDomainInput) }
		if err := r.c.Poll(context.Background()); !errors.Is(err, errNotDomainInput) {
			t.Fatalf("first Poll = %v, want errNotDomainInput", err)
		}
		reads := r.senders.totalCalls()
		if reads != 1 {
			t.Fatalf("sender reads after the first Poll = %d, want 1", reads)
		}
		for i := 0; i < 3; i++ {
			requireTags(t, doPoll(t, r), -1, -1, -1, StatusOK)
		}
		if n := r.senders.totalCalls(); n != reads {
			t.Fatalf("sender reads after four polls = %d, want %d", n, reads)
		}
		rewriteRecordRoot(t, exec, 0, hashOfIndex(99))
		doPoll(t, r)
		if h := r.c.Head(); h.Status != StatusInconsistent {
			t.Fatalf("status %s after the record changed, want INCONSISTENT from the re-check", h.Status)
		}
	})
	t.Run("a pending foreign batch is checked again when only its daHash changes", func(t *testing.T) {
		exec, _ := buildA(t)
		s := newFakeSettlement()
		s.height = 10
		r := newIndexedChain(t, exec, s)
		r.senders.fail = func(types.Hash) error { return fmt.Errorf("foreign: %w", errNotDomainInput) }
		if err := r.c.Poll(context.Background()); !errors.Is(err, errNotDomainInput) {
			t.Fatalf("first Poll = %v, want errNotDomainInput", err)
		}
		requireTags(t, doPoll(t, r), -1, -1, -1, StatusOK)
		reads := r.senders.totalCalls()

		l := layoutOf(exec)
		rec, ok, err := recovery.ReadBatchRecord(l, 0)
		if err != nil || !ok {
			t.Fatalf("read record 0: ok %v err %v", ok, err)
		}
		other := hashOfIndex(98)
		writeFile(t, recovery.BundlePath(l.Bundles, other), readFile(t, recovery.BundlePath(l.Bundles, rec.DAHash)))
		writeFile(t, recovery.ReceiptsPath(l.Receipts, other), readFile(t, recovery.ReceiptsPath(l.Receipts, rec.DAHash)))
		rec.DAHash = other
		writeRecord(t, l, rec)
		doPoll(t, r)
		if h := r.c.Head(); h.Status != StatusInconsistent {
			t.Fatalf("status %s after the daHash changed, want INCONSISTENT from the re-check", h.Status)
		}
		if n := r.senders.totalCalls(); n != reads {
			t.Fatalf("sender reads = %d, want %d: the bundle fails its daHash before any sender read", n, reads)
		}
	})
	t.Run("a lagging node keeps an accepted batch retrying", func(t *testing.T) {
		exec, cs := buildA(t)
		s := newFakeSettlement()
		s.height = 10
		acceptAt(s, cs, 100, 0)
		r := newIndexedChain(t, exec, s)
		r.senders.fail = func(types.Hash) error { return errors.New("the znnd node has no confirmed send block") }
		for i := 1; i <= 3; i++ {
			if err := r.c.Poll(context.Background()); err == nil {
				t.Fatalf("Poll %d succeeded, want the node error", i)
			}
			if n := r.senders.totalCalls(); n != i {
				t.Fatalf("sender reads after %d polls = %d, want %d", i, n, i)
			}
		}
		r.senders.fail = nil
		requireTags(t, doPoll(t, r), 4, 0, -1, StatusOK)
	})
	missing := func(types.Hash) error { return errors.New("the znnd node has no confirmed send block") }
	t.Run("a pending batch with an unknown send is checked once per interval", func(t *testing.T) {
		exec, _ := buildA(t)
		s := newFakeSettlement()
		s.height = 10
		r := newIndexedChain(t, exec, s)
		clk := &clock{t: time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC)}
		r.c.now = clk.now
		r.senders.fail = missing
		if err := r.c.Poll(context.Background()); err == nil {
			t.Fatal("first Poll succeeded, want the node error")
		}
		reads := r.senders.totalCalls()
		if reads != 1 {
			t.Fatalf("sender reads after the first Poll = %d, want 1", reads)
		}
		for i := 0; i < 5; i++ {
			clk.t = clk.t.Add(senderRetryInterval / 10)
			requireTags(t, doPoll(t, r), -1, -1, -1, StatusOK)
		}
		if n := r.senders.totalCalls(); n != reads {
			t.Fatalf("sender reads inside the interval = %d, want %d", n, reads)
		}
		clk.t = clk.t.Add(senderRetryInterval)
		if err := r.c.Poll(context.Background()); err == nil {
			t.Fatal("Poll after the interval succeeded, want the node error")
		}
		if n := r.senders.totalCalls(); n != reads+1 {
			t.Fatalf("sender reads after the interval = %d, want %d", n, reads+1)
		}
		r.senders.fail = nil
		clk.t = clk.t.Add(senderRetryInterval)
		requireTags(t, doPoll(t, r), 4, -1, -1, StatusOK)
	})
	t.Run("a pending batch with an unknown send is checked again when its record changes", func(t *testing.T) {
		exec, _ := buildA(t)
		s := newFakeSettlement()
		s.height = 10
		r := newIndexedChain(t, exec, s)
		clk := &clock{t: time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC)}
		r.c.now = clk.now
		r.senders.fail = missing
		if err := r.c.Poll(context.Background()); err == nil {
			t.Fatal("first Poll succeeded, want the node error")
		}
		requireTags(t, doPoll(t, r), -1, -1, -1, StatusOK)

		l := layoutOf(exec)
		rec, ok, err := recovery.ReadBatchRecord(l, 0)
		if err != nil || !ok {
			t.Fatalf("read record 0: ok %v err %v", ok, err)
		}
		other := hashOfIndex(97)
		writeFile(t, recovery.BundlePath(l.Bundles, other), readFile(t, recovery.BundlePath(l.Bundles, rec.DAHash)))
		writeFile(t, recovery.ReceiptsPath(l.Receipts, other), readFile(t, recovery.ReceiptsPath(l.Receipts, rec.DAHash)))
		rec.DAHash = other
		writeRecord(t, l, rec)
		doPoll(t, r)
		if h := r.c.Head(); h.Status != StatusInconsistent {
			t.Fatalf("status %s after the daHash changed, want INCONSISTENT from the re-check", h.Status)
		}
	})
	t.Run("a pending batch with an unknown send is checked again when its receiptRoot changes", func(t *testing.T) {
		exec, _ := buildA(t)
		s := newFakeSettlement()
		s.height = 10
		r := newIndexedChain(t, exec, s)
		clk := &clock{t: time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC)}
		r.c.now = clk.now
		r.senders.fail = missing
		if err := r.c.Poll(context.Background()); err == nil {
			t.Fatal("first Poll succeeded, want the node error")
		}
		requireTags(t, doPoll(t, r), -1, -1, -1, StatusOK)
		rewriteRecordRoot(t, exec, 0, hashOfIndex(96))
		doPoll(t, r)
		if h := r.c.Head(); h.Status != StatusInconsistent {
			t.Fatalf("status %s after the receiptRoot changed, want INCONSISTENT from the re-check", h.Status)
		}
	})
	t.Run("an accepted batch retries every poll inside the interval", func(t *testing.T) {
		exec, cs := buildA(t)
		s := newFakeSettlement()
		s.height = 10
		acceptAt(s, cs, 100, 0)
		r := newIndexedChain(t, exec, s)
		clk := &clock{t: time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC)}
		r.c.now = clk.now
		r.senders.fail = missing
		for i := 1; i <= 3; i++ {
			if err := r.c.Poll(context.Background()); err == nil {
				t.Fatalf("Poll %d succeeded, want the node error", i)
			}
			if n := r.senders.totalCalls(); n != i {
				t.Fatalf("sender reads after %d polls = %d, want %d", i, n, i)
			}
		}
	})
}

// keepSetup builds history A in an executor directory and runs a
// chain over it that finalizes batches 0 to 2, accepts 3 and holds 4
// pending, then closes it. It returns what a restart needs.
type keepSetup struct {
	exec, storage string
	cs            []codec.BatchCommitment
	s             *fakeSettlement
	senders       *fakeSenders
	a             *Chain
}

func newKeepSetup(t *testing.T) *keepSetup {
	t.Helper()
	pristine, cs := buildA(t)
	k := &keepSetup{exec: t.TempDir(), storage: t.TempDir(), cs: cs, s: newFakeSettlement(), senders: &fakeSenders{}}
	copyDir(t, pristine, k.exec)
	k.s.height = 30
	for i, at := range []uint64{20, 21, 22, 40} {
		k.s.accept(cs[i], at)
	}
	k.a, _ = openTestChainWith(t, k.storage, k.exec, k.s, k.senders)
	if err := k.a.Poll(context.Background()); err != nil {
		t.Fatalf("Poll: %v", err)
	}
	requireTags(t, k.a.Head(), 4, 3, 2, StatusOK)
	closeChain(t, k.a)
	if n := k.senders.totalCalls(); n != 7 {
		t.Fatalf("sender reads = %d, want 7", n)
	}
	return k
}

func (k *keepSetup) reopen(t *testing.T) (*Chain, *logSink) {
	t.Helper()
	return openTestChainWith(t, k.storage, k.exec, k.s, k.senders)
}

func TestIndexRestartKeeps(t *testing.T) {
	k := newKeepSetup(t)
	b, blog := k.reopen(t)
	if blog.count("discarding") != 0 || blog.count("removed at boot") != 0 {
		t.Fatalf("boot lines:\n%s", blog.dump())
	}
	x := b.cfg.Index
	requireMarker(t, x, 4)
	if f, ok := finalizedOf(t, x); !ok || f != 2 {
		t.Fatalf("F = %d (present %v), want 2", f, ok)
	}
	requireManifest(t, x, 3, k.cs[3].DAHash, k.cs[3].ReceiptRoot, true)
	requireManifest(t, x, 4, k.cs[4].DAHash, k.cs[4].ReceiptRoot, false)
	for id := uint64(0); id <= 2; id++ {
		if _, ok := manifestOf(t, x, id); ok {
			t.Fatalf("manifest %d survived finalize", id)
		}
	}
	if err := b.Poll(context.Background()); err != nil {
		t.Fatal(err)
	}
	requireSameVersions(t, k.a, b, 2, 3, 4)
	if n := k.senders.totalCalls(); n != 7 {
		t.Fatalf("sender reads after the restart = %d, want 7", n)
	}

	c, _ := openTestChain(t, t.TempDir(), k.exec, k.s)
	if err := c.Poll(context.Background()); err != nil {
		t.Fatal(err)
	}
	bd, cd := indexDump(t, x), indexDump(t, c.cfg.Index)
	if len(bd) != len(cd) {
		t.Fatalf("index sizes differ: %d and %d", len(bd), len(cd))
	}
	for key, v := range bd {
		if cd[key] != v {
			t.Fatalf("index differs from a fresh chain's at key %q", key)
		}
	}
}

func TestIndexBootReconcile(t *testing.T) {
	t.Run("pending record removed", func(t *testing.T) {
		k := newKeepSetup(t)
		removeRecord(t, k.exec, 4)
		b, blog := k.reopen(t)
		if blog.count("batch 4 and every indexed batch above it removed at boot") != 1 {
			t.Fatalf("boot lines:\n%s", blog.dump())
		}
		requireMarker(t, b.cfg.Index, 3)
		if has(t, b.cfg.Index, ikey(pfxInput, be64(8))) {
			t.Fatal("n/8 survived")
		}
		h8 := hashOfIndex(8)
		if !has(t, b.cfg.Index, ikey(pfxSender, h8[:])) {
			t.Fatal("S/ for input 8 was deleted")
		}
		if g, ok := u64At(t, b.cfg.Index, []byte(keyMetaSchema)); !ok || g != 1 {
			t.Fatalf("meta/schema = %d (present %v), want 1", g, ok)
		}
		requireTags(t, doPollChain(t, b), 3, 3, 2, StatusOK)
		closeChain(t, b)
		if _, again := k.reopen(t); again.count("discarding") != 0 {
			t.Fatalf("the next boot discarded the cache:\n%s", again.dump())
		}
	})

	// accepted sets up a chain that accepted 0 to 4 with nothing
	// finalized, so no snapshot exists.
	accepted := func(t *testing.T) *keepSetup {
		t.Helper()
		pristine, cs := buildA(t)
		k := &keepSetup{exec: t.TempDir(), storage: t.TempDir(), cs: cs, s: newFakeSettlement(), senders: &fakeSenders{}}
		copyDir(t, pristine, k.exec)
		k.s.height = 30
		acceptAt(k.s, cs, 100, 0, 1, 2, 3, 4)
		a, _ := k.reopen(t)
		if err := a.Poll(context.Background()); err != nil {
			t.Fatal(err)
		}
		requireTags(t, a.Head(), 4, 4, -1, StatusOK)
		closeChain(t, a)
		k.s.hidden[3], k.s.hidden[4] = true, true
		k.s.next = 3
		return k
	}
	t.Run("accepted entry not visible", func(t *testing.T) {
		k := accepted(t)
		b, blog := k.reopen(t)
		if b.Head().Status == StatusInconsistent || blog.count("removed at boot") != 0 {
			t.Fatalf("boot lines:\n%s", blog.dump())
		}
		requireManifest(t, b.cfg.Index, 3, k.cs[3].DAHash, k.cs[3].ReceiptRoot, true)
		requireManifest(t, b.cfg.Index, 4, k.cs[4].DAHash, k.cs[4].ReceiptRoot, true)
		requireTags(t, doPollChain(t, b), 4, 2, -1, StatusOK)
		k.s.hidden = map[uint64]bool{}
		k.s.next = 5
		requireTags(t, doPollChain(t, b), 4, 4, -1, StatusOK)
		if n := k.senders.totalCalls(); n != 7 {
			t.Fatalf("sender reads = %d, want 7", n)
		}
	})
	t.Run("accepted entry changed", func(t *testing.T) {
		k := accepted(t)
		k.s.hidden = map[uint64]bool{}
		k.s.next = 5
		k.s.edit(3, func(e *definition.BatchEntry) { e.DaHash = types.Hash{0xBB} })
		b, blog := k.reopen(t)
		if b.Head().Status != StatusInconsistent || blog.count("ALARM", "batch 3") != 1 {
			t.Fatalf("status %s, boot lines:\n%s", b.Head().Status, blog.dump())
		}
		requireManifest(t, b.cfg.Index, 3, k.cs[3].DAHash, k.cs[3].ReceiptRoot, true)
		requireManifest(t, b.cfg.Index, 4, k.cs[4].DAHash, k.cs[4].ReceiptRoot, true)
	})
	t.Run("pending entry differs", func(t *testing.T) {
		pristine, cs := buildA(t)
		k := &keepSetup{exec: t.TempDir(), storage: t.TempDir(), cs: cs, s: newFakeSettlement(), senders: &fakeSenders{}}
		copyDir(t, pristine, k.exec)
		k.s.height = 10
		acceptAt(k.s, cs, 100, 0, 1, 2)
		a, _ := k.reopen(t)
		if err := a.Poll(context.Background()); err != nil {
			t.Fatal(err)
		}
		requireTags(t, a.Head(), 4, 2, -1, StatusOK)
		closeChain(t, a)
		before := k.senders.totalCalls()

		bdir := t.TempDir()
		csB := buildHistory(t, bdir, nil, historyB())
		copyBatch(t, bdir, k.exec, csB[3])
		removeRecord(t, k.exec, 4)
		k.s.accept(csB[3], 100)
		b, blog := k.reopen(t)
		if blog.count("batch 3 and every indexed batch above it removed at boot") != 1 {
			t.Fatalf("boot lines:\n%s", blog.dump())
		}
		h := doPollChain(t, b)
		if tagID(h.Latest) != 3 || h.Latest.PostStateRoot != csB[3].PostStateRoot {
			t.Fatalf("latest = %+v, want batch 3 at csB[3]'s root", h.Latest)
		}
		requireManifest(t, b.cfg.Index, 3, csB[3].DAHash, csB[3].ReceiptRoot, true)
		if n := k.senders.totalCalls(); n != before {
			t.Fatalf("sender reads = %d, want %d", n, before)
		}
	})
}

func doPollChain(t *testing.T, c *Chain) Head {
	t.Helper()
	if err := c.Poll(context.Background()); err != nil {
		t.Fatalf("Poll: %v", err)
	}
	return c.Head()
}

func TestIndexBootDiscards(t *testing.T) {
	cases := []struct {
		name, text string
		change     func(t *testing.T, k *keepSetup)
	}{
		{"index removed", "index", func(t *testing.T, k *keepSetup) {
			if err := os.RemoveAll(cachePathsFor(k.storage, 1).index); err != nil {
				t.Fatal(err)
			}
		}},
		{"index corrupted", "cannot be opened", func(t *testing.T, k *keepSetup) {
			dir := cachePathsFor(k.storage, 1).index
			if err := os.WriteFile(filepath.Join(dir, "CURRENT"), []byte("garbage"), 0o600); err != nil {
				t.Fatal(err)
			}
			if err := os.Remove(filepath.Join(dir, "CURRENT.bak")); err != nil && !errors.Is(err, fs.ErrNotExist) {
				t.Fatal(err)
			}
		}},
		{"finalized below the snapshot", "finalized batch is 1", func(t *testing.T, k *keepSetup) {
			x, err := openIndex(k.storage, 1, nil)
			if err != nil {
				t.Fatal(err)
			}
			if err := x.db.Put([]byte(keyFinalized), be64(1), syncWrite); err != nil {
				t.Fatal(err)
			}
			if err := x.Close(); err != nil {
				t.Fatal(err)
			}
		}},
		{"manifest gap", "batch 3 has no manifest", func(t *testing.T, k *keepSetup) {
			x, err := openIndex(k.storage, 1, nil)
			if err != nil {
				t.Fatal(err)
			}
			if err := x.db.Delete(ikey(pfxManifest, be64(3)), syncWrite); err != nil {
				t.Fatal(err)
			}
			if err := x.Close(); err != nil {
				t.Fatal(err)
			}
		}},
		{"schema missing", "the index schema is missing", func(t *testing.T, k *keepSetup) {
			x, err := openIndex(k.storage, 1, nil)
			if err != nil {
				t.Fatal(err)
			}
			if err := x.db.Delete([]byte(keyMetaSchema), syncWrite); err != nil {
				t.Fatal(err)
			}
			if err := x.Close(); err != nil {
				t.Fatal(err)
			}
		}},
		{"schema of another version", "the index schema is 2", func(t *testing.T, k *keepSetup) {
			x, err := openIndex(k.storage, 1, nil)
			if err != nil {
				t.Fatal(err)
			}
			if err := x.db.Put([]byte(keyMetaSchema), be64(2), syncWrite); err != nil {
				t.Fatal(err)
			}
			if err := x.Close(); err != nil {
				t.Fatal(err)
			}
		}},
		{"schema damaged", "value is 7 bytes, not 8", func(t *testing.T, k *keepSetup) {
			x, err := openIndex(k.storage, 1, nil)
			if err != nil {
				t.Fatal(err)
			}
			if err := x.db.Put([]byte(keyMetaSchema), make([]byte, 7), syncWrite); err != nil {
				t.Fatal(err)
			}
			if err := x.Close(); err != nil {
				t.Fatal(err)
			}
		}},
		{"finalized batch differs from Settlement", "finalized batch 2", func(t *testing.T, k *keepSetup) {
			k.s.edit(2, func(e *definition.BatchEntry) { e.DaHash = types.Hash{0xBB} })
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			k := newKeepSetup(t)
			tc.change(t, k)
			b, blog := k.reopen(t)
			if blog.count("discarding") != 1 || blog.count("discarding", tc.text) != 1 {
				t.Fatalf("want one discard line with %q:\n%s", tc.text, blog.dump())
			}
			if b.Head().Finalized != nil {
				t.Fatalf("finalized before polling = %+v, want none", b.Head().Finalized)
			}
			if tc.name == "finalized batch differs from Settlement" {
				return
			}
			requireTags(t, doPollChain(t, b), 4, 3, 2, StatusOK)
			c, _ := openTestChain(t, t.TempDir(), k.exec, k.s)
			doPollChain(t, c)
			bd, cd := indexDump(t, b.cfg.Index), indexDump(t, c.cfg.Index)
			if len(bd) != len(cd) {
				t.Fatalf("index sizes differ: %d and %d", len(bd), len(cd))
			}
			for key, v := range bd {
				if cd[key] != v {
					t.Fatalf("index differs from a fresh chain's at key %q", key)
				}
			}
			closeChain(t, b)
			if _, again := k.reopen(t); again.count("discarding") != 0 {
				t.Fatalf("the rebuilt cache was discarded again:\n%s", again.dump())
			}
		})
	}
}
