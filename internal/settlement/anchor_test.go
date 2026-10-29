package settlement

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/zenon-network/go-zenon/common/types"
	"github.com/zenon-network/go-zenon/vm/embedded/definition"

	"github.com/Zeno-L2/zeno-executor/internal/codec"
	"github.com/Zeno-L2/zeno-executor/internal/recovery"
	"github.com/Zeno-L2/zeno-executor/internal/znnd"
	"github.com/Zeno-L2/zeno-executor/internal/znnd/znndtest"
)

type fakeBatches struct {
	cur     definition.InputSequenceCursor
	entries map[uint64]*definition.BatchEntry
	curErr  error
	// ordinals and ordErr feed DomainInputOrdinal.
	ordinals map[uint64]*definition.DomainInputOrdinal
	ordErr   error
	entryErr error // BatchEntry's error
	// entryReads counts BatchEntry calls.
	entryReads int
}

func (f *fakeBatches) InputSequenceCursor(uint32) (*definition.InputSequenceCursor, error) {
	if f.curErr != nil {
		return nil, f.curErr
	}
	c := f.cur
	return &c, nil
}

func (f *fakeBatches) DomainInputOrdinal(_ uint32, o uint64) (*definition.DomainInputOrdinal, bool, error) {
	d, ok := f.ordinals[o]
	return d, ok, f.ordErr
}

func (f *fakeBatches) BatchEntry(_ uint32, b uint64) (*definition.BatchEntry, bool, error) {
	f.entryReads++
	e, ok := f.entries[b]
	return e, ok, f.entryErr
}

const anchorHeight = 100

// batch builds an entry that is finalized at anchorHeight when final is true.
func batch(id uint64, final bool) *definition.BatchEntry {
	e := &definition.BatchEntry{
		PostStateRoot: types.Hash{byte(id + 1)},
		LastInputSeq:  id * 10,
	}
	e.BatchId = id
	if final {
		e.FinalizeAtHeight = anchorHeight
	} else {
		e.FinalizeAtHeight = anchorHeight + 1
	}
	return e
}

func TestSettledWalk(t *testing.T) {
	cases := []struct {
		name    string
		next    uint64
		swept   uint64
		final   []bool // finality by batch id; its length is the number of stored entries
		wantOK  bool
		wantIdx uint64
	}{
		{name: "no batches"},
		{name: "batch 0 unfinalized", next: 1, final: []bool{false}},
		{name: "0 and 1 finalized, 2 not", next: 3, final: []bool{true, true, false}, wantOK: true, wantIdx: 1},
		{name: "1 finalized, 0 not", next: 2, final: []bool{false, true}},
		{name: "0 finalized, 1 not, 2 finalized", next: 3, final: []bool{true, false, true}, wantOK: true, wantIdx: 0},
		{name: "swept through 2, 2 and 3 unfinalized", next: 4, swept: 2, final: []bool{true, true, false, false}, wantOK: true, wantIdx: 1},
		{name: "swept through 2, 2 finalized", next: 4, swept: 2, final: []bool{true, true, true, false}, wantOK: true, wantIdx: 2},
	}
	for _, tc := range cases {
		f := &fakeBatches{
			cur:     definition.InputSequenceCursor{DomainId: 1, NextBatchId: tc.next, RecordsSweptThrough: tc.swept},
			entries: map[uint64]*definition.BatchEntry{},
		}
		for i, fin := range tc.final {
			f.entries[uint64(i)] = batch(uint64(i), fin)
		}
		got, err := settled(f, anchorHeight, 1)
		if err != nil {
			t.Fatalf("%s: %v", tc.name, err)
		}
		if got.HaveAnchor != tc.wantOK {
			t.Fatalf("%s: HaveAnchor = %v, want %v", tc.name, got.HaveAnchor, tc.wantOK)
		}
		if got.NextBatchID != tc.next {
			t.Fatalf("%s: NextBatchID = %d, want %d", tc.name, got.NextBatchID, tc.next)
		}
		if got.HaveAnchor {
			want := batch(tc.wantIdx, true)
			if got.Anchor.PostStateRoot != want.PostStateRoot || got.Anchor.LastInputSeq != want.LastInputSeq {
				t.Fatalf("%s: anchor = %+v, want batch %d", tc.name, got.Anchor, tc.wantIdx)
			}
		}
	}
}

func TestSettledErrors(t *testing.T) {
	missing := &fakeBatches{
		cur:     definition.InputSequenceCursor{DomainId: 1, NextBatchId: 2},
		entries: map[uint64]*definition.BatchEntry{0: batch(0, true)},
	}
	if _, err := settled(missing, anchorHeight, 1); !errors.Is(err, recovery.ErrHalted) {
		t.Fatalf("missing entry: err = %v, want ErrHalted", err)
	}

	sweptMissing := &fakeBatches{
		cur:     definition.InputSequenceCursor{DomainId: 1, NextBatchId: 3, RecordsSweptThrough: 2},
		entries: map[uint64]*definition.BatchEntry{2: batch(2, false)},
	}
	if _, err := settled(sweptMissing, anchorHeight, 1); !errors.Is(err, recovery.ErrHalted) {
		t.Fatalf("missing swept entry: err = %v, want ErrHalted", err)
	}

	boom := errors.New("cursor read failed")
	if _, err := settled(&fakeBatches{curErr: boom}, anchorHeight, 1); err != boom {
		t.Fatalf("cursor error = %v, want it returned unchanged", err)
	}
}

func TestAnchorSourceGolden(t *testing.T) {
	fx := znndtest.Load(t)
	_, srv, r := newReader(t, 10)
	src := NewAnchorSource(r, 10)

	got, err := src.Settled(fx.DomainID)
	if err != nil || !got.HaveAnchor {
		t.Fatalf("Settled(%d) = %+v, %v", fx.DomainID, got, err)
	}
	want := fx.Expected.BatchEntries[0]
	if got.Anchor.PostStateRoot != want.PostStateRoot || got.Anchor.LastInputSeq != want.LastInputSeq {
		t.Fatalf("anchor = %+v, want batch 0 (%s, %d)", got.Anchor, want.PostStateRoot, want.LastInputSeq)
	}
	if n, err := src.NextBatchID(fx.DomainID); err != nil || n != got.NextBatchID || n == 0 {
		t.Fatalf("NextBatchID = %d, %v, want Settled's %d", n, err, got.NextBatchID)
	}

	if s, err := src.Settled(fx.EmptyDomainID); err != nil || s.HaveAnchor {
		t.Fatalf("Settled(%d) = %+v, %v; want no anchor", fx.EmptyDomainID, s, err)
	}

	acc, err := src.AcceptedEntries(fx.DomainID, 0, 64)
	if err != nil {
		t.Fatalf("AcceptedEntries: %v", err)
	}
	if acc.NextBatchID != got.NextBatchID || uint64(len(acc.Entries)) != acc.NextBatchID {
		t.Fatalf("AcceptedEntries: nextBatchId %d with %d entries, want %d entries below %d", acc.NextBatchID, len(acc.Entries), got.NextBatchID, got.NextBatchID)
	}
	for _, e := range acc.Entries {
		var w *definition.BatchEntry
		for i := range fx.Expected.BatchEntries {
			if x := &fx.Expected.BatchEntries[i]; x.DomainId == fx.DomainID && x.BatchId == e.BatchId {
				w = x
			}
		}
		if w == nil {
			t.Fatalf("no expected entry for batch %d", e.BatchId)
		}
		if e.ProtocolVersion != w.ProtocolVersion || e.FirstInputSeq != w.FirstInputSeq || e.LastInputSeq != w.LastInputSeq ||
			e.PreStateRoot != w.PreStateRoot || e.PostStateRoot != w.PostStateRoot || e.InputRoot != w.InputRoot ||
			e.ReceiptRoot != w.ReceiptRoot || e.EventRoot != w.EventRoot || e.OutboxRoot != w.OutboxRoot ||
			e.DaHash != w.DaHash || e.DaMode != w.DaMode || !bytes.Equal(e.AssetFlowSummary, w.AssetFlowSummary) ||
			!bytes.Equal(e.ProofData, w.ProofData) {
			t.Fatalf("batch %d = %+v, want %+v", e.BatchId, e, w)
		}
	}

	srv.SetDown(true)
	_, err = src.Settled(fx.DomainID)
	mustPlain(t, "down", err)
	srv.SetDown(false)

	srv.TamperProof(func(p *znndtest.Proof) { p.Root[0] ^= 0x01 })
	_, err = src.Settled(fx.DomainID)
	mustHalt(t, "tampered root", err)
}

func TestAcceptedEntry(t *testing.T) {
	f := &fakeBatches{
		cur:     definition.InputSequenceCursor{DomainId: 1, NextBatchId: 3},
		entries: map[uint64]*definition.BatchEntry{0: batch(0, true), 1: batch(1, true)},
	}

	if e, err := acceptedEntry(f, anchorHeight, 1, 1); err != nil || e != f.entries[1] {
		t.Fatalf("below nextBatchId with an entry: %v, %v, want the entry", e, err)
	}
	if _, err := acceptedEntry(f, anchorHeight, 1, 2); !errors.Is(err, recovery.ErrHalted) {
		t.Fatalf("below nextBatchId with no entry: err = %v, want ErrHalted", err)
	}
	for _, id := range []uint64{3, 4} {
		if _, err := acceptedEntry(f, anchorHeight, 1, id); err == nil || errors.Is(err, recovery.ErrHalted) {
			t.Fatalf("batch %d at or above nextBatchId: err = %v, want a plain error", id, err)
		}
	}
	boom := errors.New("cursor read failed")
	if _, err := acceptedEntry(&fakeBatches{curErr: boom}, anchorHeight, 1, 0); err != boom {
		t.Fatalf("cursor error = %v, want it returned unchanged", err)
	}
}

func TestAcceptedEntries(t *testing.T) {
	const next = 100
	full := func() *fakeBatches {
		f := &fakeBatches{
			cur:     definition.InputSequenceCursor{DomainId: 1, NextBatchId: next},
			entries: map[uint64]*definition.BatchEntry{},
		}
		for i := uint64(0); i < next; i++ {
			f.entries[i] = batch(i, true)
		}
		return f
	}

	cases := []struct {
		name      string
		from      uint64
		wantCount int
		wantReads int
	}{
		{"from 0 stops at max", 0, 64, 64},
		{"from 98 stops at nextBatchId", 98, 2, 2},
		{"from nextBatchId", next, 0, 0},
		{"from above nextBatchId", 150, 0, 0},
	}
	for _, tc := range cases {
		f := full()
		got, err := acceptedEntries(f, anchorHeight, 1, tc.from, 64)
		if err != nil {
			t.Fatalf("%s: %v", tc.name, err)
		}
		if got.Height != anchorHeight || got.NextBatchID != next {
			t.Fatalf("%s: height %d nextBatchId %d, want %d and %d", tc.name, got.Height, got.NextBatchID, anchorHeight, next)
		}
		if len(got.Entries) != tc.wantCount || f.entryReads != tc.wantReads {
			t.Fatalf("%s: %d entries after %d reads, want %d after %d", tc.name, len(got.Entries), f.entryReads, tc.wantCount, tc.wantReads)
		}
		for i, e := range got.Entries {
			if e.BatchId != tc.from+uint64(i) {
				t.Fatalf("%s: entry %d has batchId %d, want %d", tc.name, i, e.BatchId, tc.from+uint64(i))
			}
		}
	}

	t.Run("honours a smaller max", func(t *testing.T) {
		f := full()
		got, err := acceptedEntries(f, anchorHeight, 1, 10, 3)
		if err != nil || len(got.Entries) != 3 || f.entryReads != 3 {
			t.Fatalf("%d entries after %d reads, %v, want 3 after 3", len(got.Entries), f.entryReads, err)
		}
	})

	t.Run("missing entry halts", func(t *testing.T) {
		f := full()
		delete(f.entries, 5)
		if _, err := acceptedEntries(f, anchorHeight, 1, 3, 64); !errors.Is(err, recovery.ErrHalted) {
			t.Fatalf("err = %v, want ErrHalted", err)
		}
	})

	t.Run("read errors are returned unchanged", func(t *testing.T) {
		boom := errors.New("read failed")
		if _, err := acceptedEntries(&fakeBatches{curErr: boom}, anchorHeight, 1, 0, 64); err != boom {
			t.Fatalf("cursor error = %v, want it unchanged", err)
		}
		f := full()
		f.entryErr = boom
		if _, err := acceptedEntries(f, anchorHeight, 1, 0, 64); err != boom {
			t.Fatalf("entry error = %v, want it unchanged", err)
		}
	})
}

func matchingPair() (*definition.BatchEntry, codec.BatchCommitment) {
	c := codec.BatchCommitment{
		Versions:         codec.Versions{ProtocolVersion: codec.ProtocolVersion},
		DomainID:         1,
		BatchID:          2,
		FirstInputSeq:    3,
		LastInputSeq:     4,
		PreStateRoot:     types.Hash{1},
		PostStateRoot:    types.Hash{2},
		InputRoot:        types.Hash{3},
		ReceiptRoot:      types.Hash{4},
		EventRoot:        types.Hash{5},
		OutboxRoot:       types.Hash{6},
		DAHash:           types.Hash{7},
		DAMode:           1,
		AssetFlowSummary: codec.AssetFlowSummary{},
		ProofData:        []byte{},
	}
	flow, _ := c.AssetFlowSummary.Encode()
	e := &definition.BatchEntry{
		ProtocolVersion:  c.Versions.ProtocolVersion,
		FirstInputSeq:    3,
		LastInputSeq:     4,
		PreStateRoot:     types.Hash{1},
		PostStateRoot:    types.Hash{2},
		InputRoot:        types.Hash{3},
		ReceiptRoot:      types.Hash{4},
		EventRoot:        types.Hash{5},
		OutboxRoot:       types.Hash{6},
		DaHash:           types.Hash{7},
		DaMode:           1,
		AssetFlowSummary: flow,
		ProofData:        []byte{},
	}
	e.DomainId, e.BatchId = 1, 2
	return e, c
}

func TestEntryMatches(t *testing.T) {
	e, c := matchingPair()
	if err := EntryMatches(e, c); err != nil {
		t.Fatalf("equal pair: %v", err)
	}

	skipped := map[string]func(e *definition.BatchEntry){
		"submittedAtHeight": func(e *definition.BatchEntry) { e.SubmittedAtHeight = 9 },
		"executorPillar":    func(e *definition.BatchEntry) { e.ExecutorPillar = "p" },
		"finalizeAtHeight":  func(e *definition.BatchEntry) { e.FinalizeAtHeight = 9 },
		"state":             func(e *definition.BatchEntry) { e.State = 2 },
	}
	for name, mutate := range skipped {
		e, c := matchingPair()
		mutate(e)
		if err := EntryMatches(e, c); err != nil {
			t.Errorf("skipped field %s: %v", name, err)
		}
	}

	compared := map[string]func(e *definition.BatchEntry){
		"protocolVersion": func(e *definition.BatchEntry) { e.ProtocolVersion++ },
		"firstInputSeq":   func(e *definition.BatchEntry) { e.FirstInputSeq++ },
		"lastInputSeq":    func(e *definition.BatchEntry) { e.LastInputSeq++ },
		"preStateRoot":    func(e *definition.BatchEntry) { e.PreStateRoot[0] ^= 1 },
		"postStateRoot":   func(e *definition.BatchEntry) { e.PostStateRoot[0] ^= 1 },
		"inputRoot":       func(e *definition.BatchEntry) { e.InputRoot[0] ^= 1 },
		"receiptRoot":     func(e *definition.BatchEntry) { e.ReceiptRoot[0] ^= 1 },
		"eventRoot":       func(e *definition.BatchEntry) { e.EventRoot[0] ^= 1 },
		"outboxRoot":      func(e *definition.BatchEntry) { e.OutboxRoot[0] ^= 1 },
		"daHash":          func(e *definition.BatchEntry) { e.DaHash[0] ^= 1 },
		"daMode":          func(e *definition.BatchEntry) { e.DaMode++ },
		"assetFlow":       func(e *definition.BatchEntry) { e.AssetFlowSummary = []byte{0xFF} },
		"proofData":       func(e *definition.BatchEntry) { e.ProofData = []byte{1} },
	}
	for name, mutate := range compared {
		e, c := matchingPair()
		mutate(e)
		err := EntryMatches(e, c)
		if err == nil {
			t.Errorf("compared field %s: want a mismatch", name)
		} else if errors.Is(err, recovery.ErrHalted) {
			t.Errorf("compared field %s: err wraps ErrHalted", name)
		}
	}
}

func TestTip(t *testing.T) {
	ord := func(i uint64) *definition.DomainInputOrdinal {
		return &definition.DomainInputOrdinal{GlobalInputIndex: i}
	}

	t.Run("nextBatchId 0 has no previous entry", func(t *testing.T) {
		f := &fakeBatches{
			cur:      definition.InputSequenceCursor{ConsumedInputCount: 4},
			ordinals: map[uint64]*definition.DomainInputOrdinal{4: ord(90)},
		}
		got, err := tip(f, anchorHeight, 1)
		if err != nil {
			t.Fatal(err)
		}
		if got.Prev != nil || got.NextBatchID != 0 || got.Height != anchorHeight || !got.HaveInput || got.FirstInput != 90 {
			t.Fatalf("tip = %+v, want a nil Prev, nextBatchId 0, the input at ordinal 4 (90) and height %d", got, anchorHeight)
		}
	})

	t.Run("returns the entry below nextBatchId", func(t *testing.T) {
		f := &fakeBatches{
			cur:     definition.InputSequenceCursor{NextBatchId: 3},
			entries: map[uint64]*definition.BatchEntry{1: batch(1, true), 2: batch(2, false)},
		}
		got, err := tip(f, anchorHeight, 1)
		if err != nil {
			t.Fatal(err)
		}
		if got.Prev != f.entries[2] || got.NextBatchID != 3 {
			t.Fatalf("tip = %+v, want entry 2 and nextBatchId 3", got)
		}
		if got.HaveInput {
			t.Fatalf("tip = %+v, want no input", got)
		}
	})

	t.Run("a missing entry is a halt", func(t *testing.T) {
		f := &fakeBatches{
			cur:     definition.InputSequenceCursor{NextBatchId: 3},
			entries: map[uint64]*definition.BatchEntry{1: batch(1, true)},
		}
		if _, err := tip(f, anchorHeight, 1); !errors.Is(err, recovery.ErrHalted) {
			t.Fatalf("err = %v, want ErrHalted", err)
		}
	})

	t.Run("read errors are returned unchanged", func(t *testing.T) {
		boom := errors.New("read failed")
		if _, err := tip(&fakeBatches{curErr: boom}, anchorHeight, 1); err != boom {
			t.Fatalf("cursor error = %v, want it returned unchanged", err)
		}
		f := &fakeBatches{cur: definition.InputSequenceCursor{NextBatchId: 1}, entryErr: boom}
		if _, err := tip(f, anchorHeight, 1); err != boom {
			t.Fatalf("entry error = %v, want it returned unchanged", err)
		}
		if _, err := tip(&fakeBatches{ordErr: boom}, anchorHeight, 1); err != boom {
			t.Fatalf("ordinal error = %v, want it returned unchanged", err)
		}
	})
}

// TestTipReadsAtTheConfirmedHeight records the height of every proof read Tip makes through a
// forwarding proxy, so a read at any other height (the frontier, for one) fails the test.
func TestTipReadsAtTheConfirmedHeight(t *testing.T) {
	fx := znndtest.Load(t)
	srv := znndtest.NewServer(t, fx, fx.ReadHeight+10)

	var mu sync.Mutex
	var heights []uint64
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("proxy read: %v", err)
			return
		}
		var req struct {
			Method string
			Params []json.RawMessage
		}
		if json.Unmarshal(body, &req) == nil && req.Method == "ledger.getProof" && len(req.Params) > 0 {
			var h uint64
			if json.Unmarshal(req.Params[0], &h) == nil {
				mu.Lock()
				heights = append(heights, h)
				mu.Unlock()
			}
		}
		resp, err := http.Post(srv.URL, "application/json", bytes.NewReader(body))
		if err != nil {
			t.Errorf("proxy forward: %v", err)
			return
		}
		defer resp.Body.Close()
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.Copy(w, resp.Body)
	}))
	defer proxy.Close()

	src := NewAnchorSource(NewReader(znnd.NewClient(proxy.URL)), 10)
	_, _ = src.Tip(fx.DomainID)

	mu.Lock()
	defer mu.Unlock()
	if len(heights) < 3 {
		t.Fatalf("getProof heights %v, want the cursor, entry and ordinal reads", heights)
	}
	for _, h := range heights {
		if h != fx.ReadHeight {
			t.Fatalf("getProof heights %v, want every read at the confirmed height %d", heights, fx.ReadHeight)
		}
	}
}

// fakeGates is a gateReader that records what it is asked for. DomainRecord is not part of
// gateReader: it shows that a domain record's status is never read.
type fakeGates struct {
	global, domain definition.PauseState
	member         *definition.ExecutorSetMember
	active         bool
	errs           map[string]error // keyed by method name

	domainAsked uint32
	pillarAsked []string
}

func (f *fakeGates) GlobalPauseState() (definition.PauseState, error) {
	return f.global, f.errs["global"]
}

func (f *fakeGates) DomainPauseState(domainID uint32) (definition.PauseState, error) {
	f.domainAsked = domainID
	return f.domain, f.errs["domain"]
}

func (f *fakeGates) ExecutorSetMember(pillarName string) (*definition.ExecutorSetMember, bool, error) {
	f.pillarAsked = append(f.pillarAsked, pillarName)
	return f.member, f.member != nil, f.errs["member"]
}

func (f *fakeGates) PillarActive(name string) (bool, error) {
	f.pillarAsked = append(f.pillarAsked, name)
	return f.active, f.errs["pillar"]
}

func (f *fakeGates) DomainRecord(uint32) (*definition.DomainRecord, bool, error) {
	return &definition.DomainRecord{Status: definition.DomainStatusPaused}, true, nil
}

func TestGates(t *testing.T) {
	member := &definition.ExecutorSetMember{PillarName: "p", ExecutorAddress: types.Address{0x42}}

	t.Run("all clear passes values through", func(t *testing.T) {
		f := &fakeGates{member: member, active: true}
		g, err := gates(f, 77, 3, "p")
		if err != nil {
			t.Fatal(err)
		}
		if g.Height != 77 || g.GlobalPause || g.DomainPause || g.Member != member || !g.PillarActive {
			t.Fatalf("gates = %+v", g)
		}
		if f.domainAsked != 3 || len(f.pillarAsked) != 2 || f.pillarAsked[0] != "p" || f.pillarAsked[1] != "p" {
			t.Fatalf("asked domain %d, pillars %v; want domain 3 and pillar p twice", f.domainAsked, f.pillarAsked)
		}
	})
	t.Run("global submit pause sets the global flag only", func(t *testing.T) {
		g, err := gates(&fakeGates{global: definition.PauseState{SubmitPaused: true}}, 1, 3, "p")
		if err != nil || !g.GlobalPause || g.DomainPause {
			t.Fatalf("gates = %+v, %v", g, err)
		}
	})
	t.Run("domain submit pause sets the domain flag only", func(t *testing.T) {
		f := &fakeGates{domain: definition.PauseState{SubmitPaused: true}}
		g, err := gates(f, 1, 3, "p")
		if err != nil || g.GlobalPause || !g.DomainPause || f.domainAsked != 3 {
			t.Fatalf("gates = %+v, %v, domain asked %d", g, err, f.domainAsked)
		}
	})
	t.Run("relay pause is not a submit pause", func(t *testing.T) {
		f := &fakeGates{
			global: definition.PauseState{RelayPaused: true},
			domain: definition.PauseState{RelayPaused: true},
		}
		g, err := gates(f, 1, 3, "p")
		if err != nil || g.GlobalPause || g.DomainPause {
			t.Fatalf("gates = %+v, %v", g, err)
		}
	})
	t.Run("an absent member is nil", func(t *testing.T) {
		g, err := gates(&fakeGates{active: true}, 1, 3, "p")
		if err != nil || g.Member != nil {
			t.Fatalf("gates = %+v, %v", g, err)
		}
	})
	t.Run("read errors return unchanged", func(t *testing.T) {
		for _, m := range []string{"global", "domain", "member", "pillar"} {
			boom := errors.New("boom " + m)
			_, err := gates(&fakeGates{errs: map[string]error{m: boom}}, 1, 3, "p")
			if err != boom {
				t.Fatalf("%s error = %v, want it returned unchanged", m, err)
			}
		}
	})
}

// TestGatesReadAtTheConfirmedHeight records the height of every proof read Gates makes. The
// fixture holds no Pillar contract proof, so the pillar read, which is last, fails after its
// height is recorded.
func TestGatesReadAtTheConfirmedHeight(t *testing.T) {
	fx := znndtest.Load(t)
	srv := znndtest.NewServer(t, fx, fx.ReadHeight+10)

	var mu sync.Mutex
	var heights []uint64
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("proxy read: %v", err)
			return
		}
		var req struct {
			Method string
			Params []json.RawMessage
		}
		if json.Unmarshal(body, &req) == nil && req.Method == "ledger.getProof" && len(req.Params) > 0 {
			var h uint64
			if json.Unmarshal(req.Params[0], &h) == nil {
				mu.Lock()
				heights = append(heights, h)
				mu.Unlock()
			}
		}
		resp, err := http.Post(srv.URL, "application/json", bytes.NewReader(body))
		if err != nil {
			t.Errorf("proxy forward: %v", err)
			return
		}
		defer resp.Body.Close()
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.Copy(w, resp.Body)
	}))
	defer proxy.Close()

	src := NewAnchorSource(NewReader(znnd.NewClient(proxy.URL)), 10)
	_, _ = src.Gates(fx.DomainID, fx.PillarName)

	mu.Lock()
	defer mu.Unlock()
	if len(heights) < 4 {
		t.Fatalf("getProof heights %v, want the two pauses, the member and the pillar reads", heights)
	}
	for _, h := range heights {
		if h != fx.ReadHeight {
			t.Fatalf("getProof heights %v, want every read at the confirmed height %d", heights, fx.ReadHeight)
		}
	}
}

// fakeRelay is a relayReader. Its cursor and entry reads come from the
// embedded fakeBatches; flags lists the processed ids.
type fakeRelay struct {
	fakeBatches
	global, domain definition.PauseState
	meta           definition.ReleaseQueueMeta
	flags          map[types.Hash]bool
	errs           map[string]error // keyed by method name
	cursorReads    int
	metaReads      int
	flagReads      int
}

func (f *fakeRelay) InputSequenceCursor(d uint32) (*definition.InputSequenceCursor, error) {
	f.cursorReads++
	return f.fakeBatches.InputSequenceCursor(d)
}

func (f *fakeRelay) GlobalPauseState() (definition.PauseState, error) {
	return f.global, f.errs["global"]
}

func (f *fakeRelay) DomainPauseState(uint32) (definition.PauseState, error) {
	return f.domain, f.errs["domain"]
}

func (f *fakeRelay) ReleaseQueueMeta(d uint32) (*definition.ReleaseQueueMeta, error) {
	f.metaReads++
	m := f.meta
	m.DomainId = d
	return &m, f.errs["meta"]
}

func (f *fakeRelay) OutboxProcessed(id types.Hash) (bool, error) {
	f.flagReads++
	return f.flags[id], f.errs["flag"]
}

func TestRelay(t *testing.T) {
	ids := []types.Hash{{0x01}, {0x02}, {0x03}}

	t.Run("pauses and queue length", func(t *testing.T) {
		rs, err := relay(&fakeRelay{meta: definition.ReleaseQueueMeta{Head: 2, Tail: 7}}, anchorHeight, 3, nil)
		if err != nil || rs.Height != anchorHeight || rs.Paused || rs.QueueLen != 5 {
			t.Fatalf("relay = %+v, %v; want height %d, not paused, queue 5", rs, err, anchorHeight)
		}
		for _, f := range []*fakeRelay{
			{global: definition.PauseState{RelayPaused: true}},
			{domain: definition.PauseState{RelayPaused: true}},
		} {
			if rs, err := relay(f, anchorHeight, 3, nil); err != nil || !rs.Paused {
				t.Fatalf("relay = %+v, %v; want paused", rs, err)
			}
		}
		for _, f := range []*fakeRelay{
			{global: definition.PauseState{SubmitPaused: true}},
			{domain: definition.PauseState{SubmitPaused: true}},
		} {
			if rs, err := relay(f, anchorHeight, 3, nil); err != nil || rs.Paused {
				t.Fatalf("relay = %+v, %v; want a submit pause to leave relay unpaused", rs, err)
			}
		}
	})

	t.Run("a queue head above its tail halts", func(t *testing.T) {
		_, err := relay(&fakeRelay{meta: definition.ReleaseQueueMeta{Head: 4, Tail: 3}}, anchorHeight, 3, nil)
		mustHalt(t, "relay", err)
	})

	t.Run("a nil query reads no cursor, entry or flag", func(t *testing.T) {
		f := &fakeRelay{fakeBatches: fakeBatches{cur: definition.InputSequenceCursor{NextBatchId: 5}}, flags: map[types.Hash]bool{ids[0]: true}}
		rs, err := relay(f, anchorHeight, 3, nil)
		if err != nil || rs.Entry != nil || rs.Processed != 0 {
			t.Fatalf("relay = %+v, %v", rs, err)
		}
		if f.cursorReads != 0 || f.entryReads != 0 || f.flagReads != 0 {
			t.Fatalf("reads: cursor %d, entry %d, flag %d; want none", f.cursorReads, f.entryReads, f.flagReads)
		}
	})

	t.Run("the entry and the flags", func(t *testing.T) {
		e := batch(2, true)
		f := &fakeRelay{
			fakeBatches: fakeBatches{cur: definition.InputSequenceCursor{NextBatchId: 3}, entries: map[uint64]*definition.BatchEntry{2: e}},
			flags:       map[types.Hash]bool{ids[0]: true, ids[1]: true, ids[2]: false},
		}
		rs, err := relay(f, anchorHeight, 3, &RelayQuery{BatchID: 2, IDs: ids})
		if err != nil || rs.Entry != e || rs.Processed != 2 {
			t.Fatalf("relay = %+v, %v; want the entry and 2 processed", rs, err)
		}
		if f.flagReads != 3 {
			t.Fatalf("%d flag reads, want 3 (stopping at the first false)", f.flagReads)
		}
	})

	t.Run("flags stop at the first that is not set", func(t *testing.T) {
		f := &fakeRelay{flags: map[types.Hash]bool{ids[1]: true, ids[2]: true}}
		rs, err := relay(f, anchorHeight, 3, &RelayQuery{BatchID: 0, IDs: ids})
		if err != nil || rs.Processed != 0 || f.flagReads != 1 {
			t.Fatalf("relay = %+v, %v with %d flag reads; want 0 processed after 1 read", rs, err, f.flagReads)
		}
	})

	t.Run("a batch not below nextBatchId has no entry but still reads the flags", func(t *testing.T) {
		f := &fakeRelay{
			fakeBatches: fakeBatches{cur: definition.InputSequenceCursor{NextBatchId: 2}},
			flags:       map[types.Hash]bool{ids[0]: true},
		}
		rs, err := relay(f, anchorHeight, 3, &RelayQuery{BatchID: 2, IDs: ids[:1]})
		if err != nil || rs.Entry != nil || rs.Processed != 1 || f.entryReads != 0 {
			t.Fatalf("relay = %+v, %v with %d entry reads; want no entry and 1 processed", rs, err, f.entryReads)
		}
	})

	t.Run("a missing entry below nextBatchId halts", func(t *testing.T) {
		f := &fakeRelay{fakeBatches: fakeBatches{cur: definition.InputSequenceCursor{NextBatchId: 3}}}
		_, err := relay(f, anchorHeight, 3, &RelayQuery{BatchID: 1, IDs: ids})
		mustHalt(t, "relay", err)
	})

	t.Run("read errors return unchanged", func(t *testing.T) {
		for _, m := range []string{"global", "domain", "meta", "flag"} {
			boom := errors.New("boom " + m)
			f := &fakeRelay{errs: map[string]error{m: boom}}
			if _, err := relay(f, anchorHeight, 3, &RelayQuery{IDs: ids}); err != boom {
				t.Fatalf("%s error = %v, want it returned unchanged", m, err)
			}
		}
		boom := errors.New("boom cursor")
		if _, err := relay(&fakeRelay{fakeBatches: fakeBatches{curErr: boom}}, anchorHeight, 3, &RelayQuery{IDs: ids}); err != boom {
			t.Fatalf("cursor error = %v, want it returned unchanged", err)
		}
		boom = errors.New("boom entry")
		f := &fakeRelay{fakeBatches: fakeBatches{cur: definition.InputSequenceCursor{NextBatchId: 3}, entryErr: boom}}
		if _, err := relay(f, anchorHeight, 3, &RelayQuery{BatchID: 1, IDs: ids}); err != boom {
			t.Fatalf("entry error = %v, want it returned unchanged", err)
		}
	})
}
