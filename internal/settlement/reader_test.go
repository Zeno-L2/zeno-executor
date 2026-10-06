package settlement

import (
	"errors"
	"reflect"
	"testing"

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
