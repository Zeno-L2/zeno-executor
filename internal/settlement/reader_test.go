package settlement

import (
	"errors"
	"math/big"
	"reflect"
	"testing"

	"github.com/zenon-network/go-zenon/common/trie"
	"github.com/zenon-network/go-zenon/common/types"
	"github.com/zenon-network/go-zenon/vm/embedded/definition"

	"github.com/digitalSloth/go-executor/internal/recovery"
	"github.com/digitalSloth/go-executor/internal/znnd"
	"github.com/digitalSloth/go-executor/internal/znnd/znndtest"
)

func newReader(t *testing.T, frontierOffset uint64) (*znndtest.Fixture, *znndtest.Server, *Reader) {
	t.Helper()
	fx := znndtest.Load(t)
	srv := znndtest.NewServer(t, fx, fx.ReadHeight+frontierOffset)
	return fx, srv, NewReader(znnd.NewClient(srv.URL))
}

func mustHalt(t *testing.T, name string, err error) {
	t.Helper()
	if !errors.Is(err, recovery.ErrHalted) {
		t.Fatalf("%s: want an error wrapping ErrHalted, got %v", name, err)
	}
}

func mustPlain(t *testing.T, name string, err error) {
	t.Helper()
	if err == nil {
		t.Fatalf("%s: want an error, got nil", name)
	}
	if errors.Is(err, recovery.ErrHalted) {
		t.Fatalf("%s: error wraps ErrHalted: %v", name, err)
	}
}

func viewAtRead(t *testing.T, offset uint64) (*znndtest.Fixture, *znndtest.Server, *View) {
	t.Helper()
	fx, srv, r := newReader(t, offset)
	v, err := r.At(fx.ReadHeight)
	if err != nil {
		t.Fatalf("At(%d): %v", fx.ReadHeight, err)
	}
	return fx, srv, v
}

func TestViewTypedReads(t *testing.T) {
	fx, _, v := viewAtRead(t, 10)
	exp := fx.Expected
	d, empty := fx.DomainID, fx.EmptyDomainID

	rec, ok, err := v.DomainRecord(d)
	if err != nil || !ok || !reflect.DeepEqual(*rec, exp.DomainRecord) {
		t.Fatalf("DomainRecord(%d) = %+v, %v, %v; want %+v", d, rec, ok, err, exp.DomainRecord)
	}
	cur, err := v.InputSequenceCursor(d)
	if err != nil || !reflect.DeepEqual(*cur, exp.Cursor) {
		t.Fatalf("InputSequenceCursor = %+v, %v; want %+v", cur, err, exp.Cursor)
	}
	for i := range exp.BatchEntries {
		e, ok, err := v.BatchEntry(d, uint64(i))
		if err != nil || !ok || !reflect.DeepEqual(*e, exp.BatchEntries[i]) {
			t.Fatalf("BatchEntry(%d, %d) = %+v, %v, %v; want %+v", d, i, e, ok, err, exp.BatchEntries[i])
		}
	}
	n, err := v.DomainInputCounter(d)
	if err != nil || n != exp.DomainInputCounter {
		t.Fatalf("DomainInputCounter = %d, %v; want %d", n, err, exp.DomainInputCounter)
	}
	o, ok, err := v.DomainInputOrdinal(d, exp.Ordinal.Ordinal)
	if err != nil || !ok || !reflect.DeepEqual(*o, exp.Ordinal) {
		t.Fatalf("DomainInputOrdinal = %+v, %v, %v; want %+v", o, ok, err, exp.Ordinal)
	}
	ac, ok, err := v.AssetConfig(d, fx.AssetID)
	if err != nil || !ok {
		t.Fatalf("AssetConfig = %+v, %v, %v", ac, ok, err)
	}
	if ac.DomainId != exp.AssetConfig.DomainId || ac.AssetId != exp.AssetConfig.AssetId || ac.MaxBatchWithdrawal.Cmp(exp.AssetConfig.MaxBatchWithdrawal) != 0 {
		t.Fatalf("AssetConfig = %+v, want %+v", ac, exp.AssetConfig)
	}
	gp, err := v.GlobalPauseState()
	if err != nil || gp != exp.GlobalPause {
		t.Fatalf("GlobalPauseState = %+v, %v; want %+v", gp, err, exp.GlobalPause)
	}
	dp, err := v.DomainPauseState(d)
	if err != nil || dp != exp.DomainPause {
		t.Fatalf("DomainPauseState = %+v, %v; want %+v", dp, err, exp.DomainPause)
	}
	m, ok, err := v.ExecutorSetMember(fx.PillarName)
	if err != nil || !ok || !reflect.DeepEqual(*m, exp.ExecutorSetMember) {
		t.Fatalf("ExecutorSetMember = %+v, %v, %v; want %+v", m, ok, err, exp.ExecutorSetMember)
	}

	if _, ok, err := v.DomainRecord(empty); err != nil || ok {
		t.Fatalf("DomainRecord(%d) = %v, %v; want absent", empty, ok, err)
	}
	if _, ok, err := v.BatchEntry(d, 2); err != nil || ok {
		t.Fatalf("BatchEntry(%d, 2) = %v, %v; want absent", d, ok, err)
	}
	if _, ok, err := v.DomainInputOrdinal(d, 0); err != nil || ok {
		t.Fatalf("DomainInputOrdinal(%d, 0) = %v, %v; want absent", d, ok, err)
	}
	if cur, err := v.InputSequenceCursor(empty); err != nil || cur.NextBatchId != 0 || cur.RecordsSweptThrough != 0 || cur.DomainId != empty {
		t.Fatalf("InputSequenceCursor(%d) = %+v, %v; want zero", empty, cur, err)
	}
	if done, err := v.OutboxProcessed(fx.ProcessedOutboxID); err != nil || !done {
		t.Fatalf("OutboxProcessed(processed) = %v, %v; want true", done, err)
	}
	if done, err := v.OutboxProcessed(fx.UnprocessedOutboxID); err != nil || done {
		t.Fatalf("OutboxProcessed(unprocessed) = %v, %v; want false", done, err)
	}
}

func TestViewDetectsTampering(t *testing.T) {
	fx, srv, v := viewAtRead(t, 10)
	present := func() error { _, _, err := v.DomainRecord(fx.DomainID); return err }
	absent := func() error { _, _, err := v.DomainRecord(fx.EmptyDomainID); return err }

	cases := []struct {
		name   string
		tamper func(p *znndtest.Proof)
		read   func() error
	}{
		{"flipped proof byte", func(p *znndtest.Proof) { p.Proof[len(p.Proof)-1] ^= 0x01 }, present},
		{"flipped value byte", func(p *znndtest.Proof) { p.Value[len(p.Value)-1] ^= 0x01 }, present},
		{"changed root", func(p *znndtest.Proof) { p.Root[0] ^= 0x01 }, present},
		{"value cleared on a present key", func(p *znndtest.Proof) { p.Value = nil }, present},
		{"value set on an absent key", func(p *znndtest.Proof) { p.Value = []byte{0x01, 0x02} }, absent},
	}
	for _, tc := range cases {
		srv.TamperProof(tc.tamper)
		mustHalt(t, tc.name, tc.read())
	}

	srv.TamperProof(nil)
	rec, ok, err := v.DomainRecord(fx.DomainID)
	if err != nil || !ok || !reflect.DeepEqual(*rec, fx.Expected.DomainRecord) {
		t.Fatalf("DomainRecord after clearing the tamper = %+v, %v, %v; want %+v", rec, ok, err, fx.Expected.DomainRecord)
	}
}

func TestViewPlainErrors(t *testing.T) {
	fx, srv, r := newReader(t, 10)
	v, err := r.At(fx.ReadHeight)
	if err != nil {
		t.Fatalf("At: %v", err)
	}

	srv.SetDown(true)
	_, err = r.At(fx.ReadHeight)
	mustPlain(t, "At while down", err)
	_, _, err = v.DomainRecord(fx.DomainID)
	mustPlain(t, "read while down", err)
	srv.SetDown(false)

	_, err = r.At(1)
	mustPlain(t, "At before the spork", err)

	_, err = r.ConfirmedHeight(fx.ReadHeight + 10)
	mustPlain(t, "ConfirmedHeight at the frontier", err)
	_, err = r.ConfirmedHeight(fx.ReadHeight + 11)
	mustPlain(t, "ConfirmedHeight above the frontier", err)
	if h, err := r.ConfirmedHeight(10); err != nil || h != fx.ReadHeight {
		t.Fatalf("ConfirmedHeight(10) = %d, %v; want %d", h, err, fx.ReadHeight)
	}
}

func TestCheckNode(t *testing.T) {
	fx, srv, r := newReader(t, 3)
	if err := r.CheckNode(3); err != nil {
		t.Fatalf("CheckNode: %v", err)
	}

	srv.SetDown(true)
	mustPlain(t, "CheckNode while down", r.CheckNode(3))
	srv.SetDown(false)

	srv.TamperProof(func(p *znndtest.Proof) { p.Root[0] ^= 0x01 })
	mustHalt(t, "CheckNode with a tampered root", r.CheckNode(3))
	srv.TamperProof(nil)

	_, _, preSpork := newReader(t, 0)
	mustPlain(t, "CheckNode before the spork", preSpork.CheckNode(fx.ReadHeight-1))
}

// TestPillarActive serves proofs from its own tree, which holds Pillar contract records. It
// builds the View directly at the read height with that tree's root, so no momentum is needed.
func TestPillarActive(t *testing.T) {
	fx := znndtest.Load(t)
	raw := func(name string) []byte {
		return append(append(append([]byte{0x03}, types.PillarContract.Bytes()...), 0x04), definition.GetPillarInfoKey(name)...)
	}
	pillar := func(name string, revoked int64) []byte {
		data, err := definition.ABIPillars.PackVariable("pillarInfo",
			name, types.Address{0x01}, types.Address{0x02}, types.Address{0x03}, big.NewInt(1), int64(1), revoked, uint8(0), uint8(0), uint8(0))
		if err != nil {
			t.Fatalf("PackVariable: %v", err)
		}
		return data
	}
	leaves := map[string][]byte{
		"active":  pillar("active", 0),
		"revoked": pillar("revoked", 5),
		"garbage": {0xde, 0xad},
	}
	pathOf := func(name string) trie.Path { return trie.Path(types.NewHash(raw(name))) }
	tree := trie.NewCompactTree()
	for name, v := range leaves {
		tree.Insert(pathOf(name), v)
	}
	root := tree.Root()

	synth := *fx
	synth.Proofs = nil
	for _, name := range []string{"active", "revoked", "garbage", "absent"} {
		_, value, proof := tree.ProveByPath(pathOf(name))
		synth.Proofs = append(synth.Proofs, znndtest.FixtureProof{
			Height: fx.ReadHeight,
			Key:    raw(name),
			Result: znndtest.Proof{Value: value, Proof: proof, Root: root},
		})
	}
	if present, _, _ := tree.ProveByPath(pathOf("absent")); present {
		t.Fatal("the absent name is present in the synthetic tree")
	}

	srv := znndtest.NewServer(t, &synth, fx.ReadHeight+10)
	v := &View{Height: fx.ReadHeight, c: znnd.NewClient(srv.URL), root: root}

	if ok, err := v.PillarActive("active"); err != nil || !ok {
		t.Fatalf("PillarActive(active) = %v, %v; want true", ok, err)
	}
	if ok, err := v.PillarActive("revoked"); err != nil || ok {
		t.Fatalf("PillarActive(revoked) = %v, %v; want false", ok, err)
	}
	if ok, err := v.PillarActive("absent"); err != nil || ok {
		t.Fatalf("PillarActive(absent) = %v, %v; want false with no error", ok, err)
	}
	_, err := v.PillarActive("garbage")
	mustHalt(t, "PillarActive(garbage)", err)
}

// TestReleaseQueueMeta serves proofs from its own tree, as
// TestPillarActive does.
func TestReleaseQueueMeta(t *testing.T) {
	fx := znndtest.Load(t)
	raw := func(domainID uint32) []byte {
		return append(append(append([]byte{0x03}, types.SettlementContract.Bytes()...), 0x04), definition.ReleaseQueueMetaKey(domainID)...)
	}
	meta, err := definition.ABISettlement.PackVariable(definition.ReleaseQueueMetaVariableName, uint64(2), uint64(5))
	if err != nil {
		t.Fatalf("PackVariable: %v", err)
	}
	const present, absent, garbage = 1, 2, 3
	leaves := map[uint32][]byte{present: meta, garbage: {0xde, 0xad}}
	pathOf := func(d uint32) trie.Path { return trie.Path(types.NewHash(raw(d))) }
	tree := trie.NewCompactTree()
	for d, v := range leaves {
		tree.Insert(pathOf(d), v)
	}
	root := tree.Root()

	synth := *fx
	synth.Proofs = nil
	for _, d := range []uint32{present, absent, garbage} {
		_, value, proof := tree.ProveByPath(pathOf(d))
		synth.Proofs = append(synth.Proofs, znndtest.FixtureProof{
			Height: fx.ReadHeight,
			Key:    raw(d),
			Result: znndtest.Proof{Value: value, Proof: proof, Root: root},
		})
	}
	srv := znndtest.NewServer(t, &synth, fx.ReadHeight+10)
	v := &View{Height: fx.ReadHeight, c: znnd.NewClient(srv.URL), root: root}

	m, err := v.ReleaseQueueMeta(present)
	if err != nil || m.DomainId != present || m.Head != 2 || m.Tail != 5 {
		t.Fatalf("ReleaseQueueMeta(present) = %+v, %v; want domain 1, head 2, tail 5", m, err)
	}
	m, err = v.ReleaseQueueMeta(absent)
	if err != nil || m.DomainId != absent || m.Head != 0 || m.Tail != 0 {
		t.Fatalf("ReleaseQueueMeta(absent) = %+v, %v; want zeros with domain 2", m, err)
	}
	_, err = v.ReleaseQueueMeta(garbage)
	mustHalt(t, "ReleaseQueueMeta(garbage)", err)
}

func TestViewMomentum(t *testing.T) {
	fx, srv, v := viewAtRead(t, 10)
	ms, err := znnd.NewClient(srv.URL).DetailedMomentumsByHeight(fx.ReadHeight, 1)
	if err != nil || len(ms) != 1 || ms[0].Momentum == nil {
		t.Fatalf("DetailedMomentumsByHeight(%d) = %v, %v", fx.ReadHeight, ms, err)
	}
	want := ms[0].Momentum.TimestampUnix
	if want == 0 {
		t.Fatal("the fixture momentum has a zero time, so the test would not tell a missing time from a read one")
	}
	if h, ts := v.Momentum(); h != fx.ReadHeight || ts != want {
		t.Fatalf("Momentum() = %d, %d; want %d, %d", h, ts, fx.ReadHeight, want)
	}
}
