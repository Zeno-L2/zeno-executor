package gateway

import (
	"bytes"
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"testing"

	"github.com/zenon-network/go-zenon/common/types"
	"github.com/zenon-network/go-zenon/vm/embedded/definition"

	"github.com/Zeno-L2/zeno-executor/domain"
	"github.com/Zeno-L2/zeno-executor/internal/codec"
)

// historyE is two batches of events and withdrawals: inputs 1 and 3
// each emit 64 events with two topics, and inputs 2 and 4 each refund
// one withdrawal, to different recipients.
func historyE() [][]fixtureInput {
	events := func(g uint64, c types.Hash) domain.Effects {
		e := domain.Effects{Status: codec.StatusSuccess, ContractID: c}
		for j := 0; j < codec.MaxEventsPerExecution; j++ {
			e.Events = append(e.Events, codec.Event{
				ContractID: c, InputIndex: g, EventIndex: uint32(j),
				Topics: []types.Hash{{0x70, byte(j % 2)}, {0x71, byte(j % 3)}},
				Data:   []byte{byte(j)},
			})
		}
		return e
	}
	to2 := refund(4)
	to2.OutboxMessages[0].Target = codec.L1Withdrawal{RecipientL1: types.Address{2}, Asset: fixtureAsset, Amount: fixtureDeposit}
	return [][]fixtureInput{
		{{index: 1, effects: events(1, types.Hash{0xC0})}, {index: 2, effects: refund(2)}},
		{{index: 3, effects: events(3, types.Hash{0xC1})}, {index: 4, effects: to2}},
	}
}

// historyL is one batch of four CallL2 inputs, each carrying a payload
// of MaxCallPayload bytes.
func historyL() [][]fixtureInput {
	var b []fixtureInput
	for g := uint64(1); g <= 4; g++ {
		b = append(b, fixtureInput{index: g, args: codec.CallL2Args{
			TargetContract: types.Hash{0xC0}, DepositAsset: fixtureAsset, DepositAmount: fixtureDeposit,
			Payload: bytes.Repeat([]byte{byte(g)}, codec.MaxCallPayload),
		}})
	}
	return [][]fixtureInput{b}
}

// listRig is readRig over the history h.
func listRig(t *testing.T, h [][]fixtureInput) (string, []codec.BatchCommitment, *fakeSettlement, *rig, *Server, *logSink) {
	t.Helper()
	dir := t.TempDir()
	cs := buildHistory(t, dir, nil, h)
	s := newFakeSettlement()
	s.height = 10
	r := newIndexedChain(t, dir, s)
	srv, sink, _ := newTestServer(t, nil, r.c)
	return dir, cs, s, r, srv, sink
}

// listJSON is a decoded list result.
type listJSON struct {
	DomainID   uint32
	BatchID    uint64
	Verified   bool
	Items      []json.RawMessage
	NextCursor *string
}

const noBatch = `{"domainId":1,"batchId":null,"verified":false,"items":[],"nextCursor":null}`

// list calls method with the filter object and decodes the result.
func list(t *testing.T, srv *Server, method, filter string) listJSON {
	t.Helper()
	var l listJSON
	rpcOK(t, srv, method, "["+filter+"]", &l)
	return l
}

// gidx returns the globalInputIndex of each input item, or the
// inputIndex of each event or outbox item.
func gidx(t *testing.T, items []json.RawMessage) []uint64 {
	t.Helper()
	out := []uint64{}
	for _, it := range items {
		var v struct {
			G *uint64 `json:"globalInputIndex"`
			I uint64  `json:"inputIndex"`
		}
		if err := json.Unmarshal(it, &v); err != nil {
			t.Fatal(err)
		}
		if v.G != nil {
			out = append(out, *v.G)
		} else {
			out = append(out, v.I)
		}
	}
	return out
}

// evpos returns each event's inputIndex and eventIndex.
func evpos(t *testing.T, items []json.RawMessage) [][2]uint64 {
	t.Helper()
	out := [][2]uint64{}
	for _, it := range items {
		var v struct {
			I uint64 `json:"inputIndex"`
			E uint64 `json:"eventIndex"`
		}
		if err := json.Unmarshal(it, &v); err != nil {
			t.Fatal(err)
		}
		out = append(out, [2]uint64{v.I, v.E})
	}
	return out
}

// cur is the hex of a cursor at input g and, with j, entry j.
func cur(g uint64, j ...uint32) string {
	b := be64(g)
	if len(j) > 0 {
		b = append(b, be32(j[0])...)
	}
	return fmt.Sprintf("%x", b)
}

// requireList requires the inputs (or entries' input indexes) of l
// and its nextCursor, "" for null.
func requireList(t *testing.T, what string, l listJSON, gs []uint64, next string) {
	t.Helper()
	got := "<null>"
	if l.NextCursor != nil {
		got = *l.NextCursor
	}
	want := next
	if next == "" {
		want = "<null>"
	}
	if g := gidx(t, l.Items); !slices.Equal(g, gs) || got != want {
		t.Fatalf("%s: items %v cursor %s, want %v cursor %s", what, g, got, gs, want)
	}
}

// listBody posts one request per filter for method in a single body
// and returns the responses in order.
func listBody(t *testing.T, srv *Server, method string, filters ...string) []rpcResp {
	t.Helper()
	reqs := make([]string, len(filters))
	for i, f := range filters {
		reqs[i] = fmt.Sprintf(`{"jsonrpc":"2.0","method":%q,"params":[%s],"id":%d}`, method, f, i+1)
	}
	rec := post(t, srv, "["+strings.Join(reqs, ",")+"]")
	var rs []rpcResp
	if err := json.Unmarshal(rec.Body.Bytes(), &rs); err != nil {
		t.Fatalf("%v: %s", err, trunc(rec.Body.String()))
	}
	if len(rs) != len(filters) {
		t.Fatalf("%d responses for %d requests", len(rs), len(filters))
	}
	return rs
}

// listOf decodes the result of response r.
func listOf(t *testing.T, r rpcResp) listJSON {
	t.Helper()
	if r.Error != nil {
		t.Fatalf("error %d %s", r.Error.Code, r.Error.Message)
	}
	var l listJSON
	if err := json.Unmarshal(r.Result, &l); err != nil {
		t.Fatal(err)
	}
	return l
}

func requireBodyErr(t *testing.T, r rpcResp, code int, msg string) {
	t.Helper()
	if r.Error == nil || r.Error.Code != code || r.Error.Message != msg {
		t.Fatalf("response = %+v %s, want error %d %q", r.Error, trunc(string(r.Result)), code, msg)
	}
}

const scanMsg = "request body exceeds its index scan budget; send this request alone"

func hashHex(h types.Hash) string { return h.String() }

func TestGetInputsByContract(t *testing.T) {
	_, cs, s, r, srv, _ := listRig(t, historyC())
	acceptAt(s, cs, 100, 0)
	requireTags(t, doPoll(t, r), 1, 0, -1, StatusOK)
	x := types.Hash{0xD1}
	f := fmt.Sprintf(`{"domainId":1,"contractId":%q`, hashHex(x))

	l := list(t, srv, "zeno_getInputs", f+`,"toBatch":"pending"}`)
	requireList(t, "pending", l, []uint64{5, 3, 2}, "")
	if l.BatchID != 1 || l.Verified {
		t.Fatalf("pending result batch %d verified %v", l.BatchID, l.Verified)
	}
	for i, want := range []struct {
		batch    uint64
		verified bool
		kind     uint8
	}{{1, false, 5}, {0, true, 0}, {0, true, 4}} {
		var it inputItem
		if err := json.Unmarshal(l.Items[i], &it); err != nil {
			t.Fatal(err)
		}
		if it.BatchID != want.batch || it.Verified != want.verified || it.InputKind != want.kind {
			t.Fatalf("item %d = %+v, want batch %d verified %v kind %d", i, it, want.batch, want.verified, want.kind)
		}
	}

	for _, p := range []string{f + `}`, f + `,"toBatch":null}`} {
		l = list(t, srv, "zeno_getInputs", p)
		requireList(t, p, l, []uint64{3, 2}, "")
		if l.BatchID != 0 || !l.Verified {
			t.Fatalf("%s: result batch %d verified %v", p, l.BatchID, l.Verified)
		}
	}

	var it inputItem
	if err := json.Unmarshal(list(t, srv, "zeno_getInputs", f+`}`).Items[0], &it); err != nil {
		t.Fatal(err)
	}
	v, err := r.idx.view()
	if err != nil {
		t.Fatal(err)
	}
	defer v.Release()
	in, _, err := v.inputAt(3)
	if err != nil {
		t.Fatal(err)
	}
	rc, err := codec.DecodeReceipt(in.Receipt)
	if err != nil {
		t.Fatal(err)
	}
	want := inputItem{
		GlobalInputIndex: 3, BatchID: 0, Verified: true, SendHash: hashOfIndex(3), Sender: senderOf(hashOfIndex(3)),
		Receipt: receiptSummary{"REVERT", x, rc.GasUsed, 1},
	}
	if it != want {
		t.Fatalf("item 3 = %+v, want %+v", it, want)
	}

	for _, c := range []types.Hash{{0xD2}, {0xD9}, {}} {
		requireList(t, "unknown contract", list(t, srv, "zeno_getInputs", fmt.Sprintf(`{"domainId":1,"contractId":%q,"toBatch":"pending"}`, hashHex(c))), []uint64{}, "")
	}
}

func TestGetInputsFilters(t *testing.T) {
	_, cs, s, r, srv, _ := listRig(t, historyC())
	acceptAt(s, cs, 100, 0)
	doPoll(t, r)
	x := hashHex(types.Hash{0xD1})
	const m = "zeno_getInputs"
	pend := func(extra string) listJSON {
		return list(t, srv, m, `{"domainId":1,"toBatch":"pending"`+extra+`}`)
	}

	requireList(t, "all", pend(""), []uint64{5, 4, 3, 2, 1}, "")
	requireList(t, "fromBatch 1", pend(`,"fromBatch":1`), []uint64{5, 4}, "")
	requireList(t, "toBatch 0", list(t, srv, m, `{"domainId":1,"toBatch":0}`), []uint64{3, 2, 1}, "")
	requireRPCErr(t, srv, m, `[{"domainId":1,"fromBatch":1,"toBatch":0}]`, -32602, "invalid params: fromBatch 1 is above toBatch 0")
	requireRPCErr(t, srv, m, `[{"domainId":1,"toBatch":2}]`, -32004, "batch 2 not found")
	l := pend(`,"fromBatch":2`)
	requireList(t, "fromBatch 2", l, []uint64{}, "")
	if l.BatchID != 1 || l.Verified {
		t.Fatalf("fromBatch 2: batch %d verified %v", l.BatchID, l.Verified)
	}
	l = list(t, srv, m, `{"domainId":1,"fromBatch":1,"toBatch":"latest"}`)
	requireList(t, "latest below fromBatch", l, []uint64{}, "")
	if l.BatchID != 0 || !l.Verified {
		t.Fatalf("latest below fromBatch: batch %d verified %v", l.BatchID, l.Verified)
	}
	requireList(t, "fromBatch 2 with cursor", pend(`,"fromBatch":2,"cursor":"ffffffffffffffff"`), []uint64{}, "")

	requireList(t, "contract, kind 0", pend(`,"contractId":"`+x+`","inputKind":0`), []uint64{3}, "")
	requireList(t, "contract, sender", pend(`,"contractId":"`+x+`","sender":"`+senderOf(hashOfIndex(2)).String()+`"`), []uint64{2}, "")
	requireList(t, "contract, kind and sender", pend(`,"contractId":"`+x+`","inputKind":4,"sender":"`+senderOf(hashOfIndex(3)).String()+`"`), []uint64{}, "")
	s4 := senderOf(hashOfIndex(4)).String()
	requireList(t, "sender", pend(`,"sender":"`+s4+`"`), []uint64{4}, "")
	requireList(t, "sender, kind", pend(`,"sender":"`+s4+`","inputKind":4`), []uint64{}, "")
	requireList(t, "kind 0", pend(`,"inputKind":0`), []uint64{4, 3, 1}, "")
	requireList(t, "kind 4", pend(`,"inputKind":4`), []uint64{2}, "")
	requireList(t, "kind 9", pend(`,"inputKind":9`), []uint64{}, "")

	requireList(t, "limit 2", pend(`,"limit":2`), []uint64{5, 4}, cur(4))
	requireList(t, "limit 2 page 2", pend(`,"limit":2,"cursor":"`+cur(4)+`"`), []uint64{3, 2}, cur(2))
	requireList(t, "limit 2 page 3", pend(`,"limit":2,"cursor":"`+cur(2)+`"`), []uint64{1}, "")
	requireList(t, "limit 5", pend(`,"limit":5`), []uint64{5, 4, 3, 2, 1}, cur(1))
	requireList(t, "after limit 5", pend(`,"limit":5,"cursor":"`+cur(1)+`"`), []uint64{}, "")
	a, b := callRPC(t, srv, m, `[{"domainId":1}]`), callRPC(t, srv, m, `[{"domainId":1}]`)
	if !bytes.Equal(a.Result, b.Result) {
		t.Fatalf("results differ:\n%s\n%s", a.Result, b.Result)
	}

	requireList(t, "cursor above", pend(`,"cursor":"ffffffffffffffff"`), []uint64{5, 4, 3, 2, 1}, "")
	requireList(t, "cursor zero", pend(`,"cursor":"0000000000000000"`), []uint64{}, "")
	requireList(t, "contract below its range", pend(`,"contractId":"`+x+`","cursor":"`+cur(2)+`"`), []uint64{}, "")
}

func TestListPagingAcrossChanges(t *testing.T) {
	const m = "zeno_getInputs"
	t.Run("new batches", func(t *testing.T) {
		_, cs, s, r, srv, _ := listRig(t, historyC())
		acceptAt(s, cs, 100, 0)
		doPoll(t, r)
		l := list(t, srv, m, `{"domainId":1,"limit":2}`)
		requireList(t, "page 1", l, []uint64{3, 2}, cur(2))
		if l.BatchID != 0 {
			t.Fatalf("page 1 result batch %d, want 0", l.BatchID)
		}
		s.accept(cs[1], 100)
		doPoll(t, r)
		l = list(t, srv, m, `{"domainId":1,"limit":2,"cursor":"`+cur(2)+`"}`)
		requireList(t, "page 2", l, []uint64{1}, "")
		if l.BatchID != 1 {
			t.Fatalf("page 2 result batch %d, want 1", l.BatchID)
		}
	})
	t.Run("retraction", func(t *testing.T) {
		dir, cs, s, r, srv, _ := readRig(t)
		s.accept(cs[0], 100)
		s.accept(cs[1], 100)
		doPoll(t, r)
		requireList(t, "page 1", list(t, srv, m, `{"domainId":1,"toBatch":"pending","limit":4}`), []uint64{8, 7, 6, 4}, cur(4))
		removeRecord(t, dir, 2)
		requireTags(t, doPoll(t, r), 1, 1, -1, StatusOK)
		l := list(t, srv, m, `{"domainId":1,"toBatch":"pending","cursor":"`+cur(4)+`"}`)
		requireList(t, "after retraction", l, []uint64{3, 2, 1}, "")
		if l.BatchID != 1 {
			t.Fatalf("result batch %d, want 1", l.BatchID)
		}
		requireRPCErr(t, srv, m, `[{"domainId":1,"toBatch":4,"cursor":"`+cur(4)+`"}]`, -32004, "batch 4 not found")
	})
}

func TestListIdentityAndStatus(t *testing.T) {
	const m = "zeno_getInputs"
	t.Run("identity", func(t *testing.T) {
		_, cs, s, r, srv, _ := readRig(t)
		s.accept(cs[0], 100)
		s.accept(cs[1], 100)
		doPoll(t, r)
		key := ikey(pfxBatch, be64(4))
		raw, err := r.idx.db.Get(key, nil)
		if err != nil {
			t.Fatal(err)
		}
		var bv batchValue
		if err := json.Unmarshal(raw, &bv); err != nil {
			t.Fatal(err)
		}
		bv.DAHash = types.Hash{0xBB}
		if raw, err = json.Marshal(bv); err != nil {
			t.Fatal(err)
		}
		if err := r.idx.db.Put(key, raw, nil); err != nil {
			t.Fatal(err)
		}
		for _, mm := range []string{"zeno_getInputs", "zeno_getEvents", "zeno_getOutbox"} {
			requireRPCErr(t, srv, mm, `[{"domainId":1,"toBatch":"pending"}]`, -32004, "batch 4 is no longer available")
		}
		requireList(t, "toBatch 3", list(t, srv, m, `{"domainId":1,"toBatch":3}`), []uint64{7, 6, 4, 3, 2, 1}, "")
		if err := r.idx.retract(3); err != nil {
			t.Fatal(err)
		}
		requireRPCErr(t, srv, m, `[{"domainId":1,"toBatch":3}]`, -32004, "batch 3 is no longer available")
		requireList(t, "toBatch 1", list(t, srv, m, `{"domainId":1,"toBatch":1}`), []uint64{3, 2, 1}, "")
	})
	t.Run("status", func(t *testing.T) {
		_, cs, s, r, srv, _ := readRig(t)
		s.accept(cs[0], 20)
		s.accept(cs[1], 21)
		doPoll(t, r)
		s.edit(1, func(be *definition.BatchEntry) { be.PostStateRoot = types.Hash{0xAB} })
		s.height = 21
		requireTags(t, doPoll(t, r), 4, 1, 0, StatusInconsistent)
		const msg = "the domain is INCONSISTENT from batch 1"
		requireRPCErr(t, srv, m, `[{"domainId":1}]`, -32003, msg)
		requireRPCErr(t, srv, m, `[{"domainId":1,"toBatch":"pending"}]`, -32003, msg)
		l := list(t, srv, m, `{"domainId":1,"toBatch":"finalized"}`)
		requireList(t, "finalized", l, []uint64{2, 1}, "")
		if l.BatchID != 0 || !l.Verified {
			t.Fatalf("finalized: batch %d verified %v", l.BatchID, l.Verified)
		}
	})
	t.Run("no batch at the tag", func(t *testing.T) {
		_, _, _, r, srv, _ := listRig(t, historyC())
		requireTags(t, doPoll(t, r), 1, -1, -1, StatusOK)
		for _, mm := range []string{"zeno_getInputs", "zeno_getEvents", "zeno_getOutbox"} {
			if got := callRPC(t, srv, mm, `[{"domainId":1}]`).Result; string(got) != noBatch {
				t.Fatalf("%s = %s", mm, got)
			}
		}
		if got := callRPC(t, srv, m, `[{"domainId":1,"toBatch":"finalized","fromBatch":0}]`).Result; string(got) != noBatch {
			t.Fatalf("finalized = %s", got)
		}
		l := list(t, srv, m, `{"domainId":1,"toBatch":"pending"}`)
		requireList(t, "pending", l, []uint64{5, 4, 3, 2, 1}, "")
		if l.BatchID != 1 || l.Verified {
			t.Fatalf("pending: batch %d verified %v", l.BatchID, l.Verified)
		}
		srv.scanKeys = 5
		rs := listBody(t, srv, m, `{"domainId":1}`, `{"domainId":1,"toBatch":"pending"}`)
		if string(rs[0].Result) != noBatch {
			t.Fatalf("response 1 = %s", rs[0].Result)
		}
		requireList(t, "response 2", listOf(t, rs[1]), []uint64{5, 4, 3, 2, 1}, cur(1))
	})
	t.Run("damage", func(t *testing.T) {
		_, cs, s, r, srv, sink := listRig(t, historyC())
		acceptAt(s, cs, 100, 0)
		doPoll(t, r)
		if err := r.idx.db.Delete(ikey(pfxInput, be64(3)), nil); err != nil {
			t.Fatal(err)
		}
		p := fmt.Sprintf(`[{"domainId":1,"contractId":%q}]`, hashHex(types.Hash{0xD1}))
		for range 2 {
			requireRPCErr(t, srv, m, p, -32603, "internal error")
		}
		if n := sink.count("JSON-RPC zeno_getInputs: internal error", "the index is damaged"); n != 1 {
			t.Fatalf("damage log lines = %d, want 1:\n%s", n, sink.dump())
		}
	})
}

func TestGetEvents(t *testing.T) {
	_, cs, s, r, srv, _ := listRig(t, historyE())
	acceptAt(s, cs, 100, 0, 1)
	requireTags(t, doPoll(t, r), 1, 1, -1, StatusOK)
	const m = "zeno_getEvents"
	pos := func(l listJSON) [][2]uint64 { return evpos(t, l.Items) }
	run := func(from, to uint64, input uint64) [][2]uint64 {
		var out [][2]uint64
		for e := int(to); e >= int(from); e-- {
			out = append(out, [2]uint64{input, uint64(e)})
		}
		return out
	}
	t70 := func(i byte) string { return hashHex(types.Hash{0x70, i}) }
	t71 := func(i byte) string { return hashHex(types.Hash{0x71, i}) }

	l := list(t, srv, m, `{"domainId":1}`)
	if want := append(run(0, 63, 3), run(28, 63, 1)...); !slices.Equal(pos(l), want) || l.NextCursor == nil || *l.NextCursor != cur(1, 28) {
		t.Fatalf("page 1 = %v cursor %v", pos(l), l.NextCursor)
	}
	var ev struct {
		BatchID    uint64
		Verified   bool
		ContractID string
		InputIndex uint64
		EventIndex uint32
		Topics     []string
		Data       string
	}
	for _, it := range l.Items {
		if err := json.Unmarshal(it, &ev); err != nil {
			t.Fatal(err)
		}
		if ev.InputIndex == 3 && ev.EventIndex == 5 {
			break
		}
	}
	if ev.BatchID != 1 || !ev.Verified || ev.ContractID != hashHex(types.Hash{0xC1}) || ev.Data != "05" ||
		!slices.Equal(ev.Topics, []string{t70(1), t71(2)}) {
		t.Fatalf("event (3, 5) = %+v", ev)
	}
	l = list(t, srv, m, `{"domainId":1,"cursor":"`+cur(1, 28)+`"}`)
	if !slices.Equal(pos(l), run(0, 27, 1)) || l.NextCursor != nil {
		t.Fatalf("page 2 = %v cursor %v", pos(l), l.NextCursor)
	}

	c := func(h types.Hash) string { return `,"contractId":"` + hashHex(h) + `"` }
	if got := pos(list(t, srv, m, `{"domainId":1`+c(types.Hash{0xC0})+`}`)); !slices.Equal(got, run(0, 63, 1)) {
		t.Fatalf("C0 = %v", got)
	}
	if got := pos(list(t, srv, m, `{"domainId":1`+c(types.Hash{0xC1})+`}`)); !slices.Equal(got, run(0, 63, 3)) {
		t.Fatalf("C1 = %v", got)
	}
	if got := list(t, srv, m, `{"domainId":1`+c(types.Hash{})+`}`); len(got.Items) != 0 || got.NextCursor != nil {
		t.Fatalf("zero contract = %+v", got)
	}

	topics := func(tp string) listJSON { return list(t, srv, m, `{"domainId":1,"limit":100,"topics":`+tp+`}`) }
	l = topics(`["` + t70(0) + `"]`)
	if len(l.Items) != 64 {
		t.Fatalf("T70_0 = %d items", len(l.Items))
	}
	for _, p := range pos(l) {
		if p[1]%2 != 0 {
			t.Fatalf("odd event %v", p)
		}
	}
	if got := len(topics(`[null,"` + t71(0) + `"]`).Items); got != 44 {
		t.Fatalf("[null, T71_0] = %d items", got)
	}
	l = topics(`["` + t70(1) + `","` + t71(0) + `"]`)
	if got := pos(l); len(got) != 22 || got[0] != [2]uint64{3, 63} || got[1] != [2]uint64{3, 57} || got[5] != [2]uint64{3, 33} {
		t.Fatalf("[T70_1, T71_0] = %v", got)
	}
	if got := topics(`[null,null,"` + t70(0) + `"]`); len(got.Items) != 0 {
		t.Fatalf("third topic matched %d events", len(got.Items))
	}
	if got := topics(`[]`); len(got.Items) != 100 {
		t.Fatalf("[] = %d items", len(got.Items))
	}

	if got := pos(list(t, srv, m, `{"domainId":1,"fromBatch":1}`)); !slices.Equal(got, run(0, 63, 3)) {
		t.Fatalf("fromBatch 1 = %v", got)
	}
	if got := pos(list(t, srv, m, `{"domainId":1,"toBatch":0}`)); !slices.Equal(got, run(0, 63, 1)) {
		t.Fatalf("toBatch 0 = %v", got)
	}
	l = list(t, srv, m, `{"domainId":1,"limit":1}`)
	if got := pos(l); !slices.Equal(got, [][2]uint64{{3, 63}}) || l.NextCursor == nil || *l.NextCursor != cur(3, 63) {
		t.Fatalf("limit 1 = %v cursor %v", got, l.NextCursor)
	}
}

func TestGetOutbox(t *testing.T) {
	_, cs, s, r, srv, _ := listRig(t, historyE())
	acceptAt(s, cs, 100, 0, 1)
	doPoll(t, r)
	const m = "zeno_getOutbox"
	l := list(t, srv, m, `{"domainId":1}`)
	requireList(t, "all", l, []uint64{4, 2}, "")
	type msg struct {
		BatchID        uint64
		OutboxID       string
		Kind           string
		Recipient      *string
		Amount         string
		TargetContract *string
	}
	var m4, m2 msg
	if err := json.Unmarshal(l.Items[0], &m4); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(l.Items[1], &m2); err != nil {
		t.Fatal(err)
	}
	if m4.BatchID != 1 || m4.OutboxID != codec.OutboxID(1, 1, 4, 0).String() || m4.Kind != "L1_WITHDRAWAL" ||
		m4.Recipient == nil || *m4.Recipient != (types.Address{2}).String() || m4.Amount != "5" || m4.TargetContract != nil {
		t.Fatalf("input 4's message = %+v", m4)
	}
	if m2.BatchID != 0 || m2.OutboxID != codec.OutboxID(1, 0, 2, 0).String() || m2.Recipient == nil || *m2.Recipient != (types.Address{1}).String() {
		t.Fatalf("input 2's message = %+v", m2)
	}
	for a, want := range map[byte][]uint64{1: {2}, 2: {4}, 3: {}} {
		requireList(t, "recipient", list(t, srv, m, fmt.Sprintf(`{"domainId":1,"recipient":%q}`, types.Address{a}.String())), want, "")
	}
	requireList(t, "limit 1", list(t, srv, m, `{"domainId":1,"limit":1}`), []uint64{4}, cur(4, 0))
	requireList(t, "limit 1 page 2", list(t, srv, m, `{"domainId":1,"limit":1,"cursor":"`+cur(4, 0)+`"}`), []uint64{2}, cur(2, 0))
}

func TestListScanBudget(t *testing.T) {
	_, cs, s, r, srv, _ := listRig(t, historyC())
	acceptAt(s, cs, 100, 0, 1)
	doPoll(t, r)
	const m = "zeno_getInputs"
	if maxScanKeys != 10000 || srv.scanKeys != maxScanKeys {
		t.Fatalf("maxScanKeys %d, srv.scanKeys %d", maxScanKeys, srv.scanKeys)
	}
	srv.scanKeys = 7
	rs := listBody(t, srv, m, `{"domainId":1}`, `{"domainId":1,"limit":2,"toBatch":"pending"}`, `{"domainId":1}`)
	requireList(t, "response 1", listOf(t, rs[0]), []uint64{5, 4, 3, 2, 1}, "")
	requireList(t, "response 2", listOf(t, rs[1]), []uint64{5, 4}, cur(4))
	requireBodyErr(t, rs[2], -32600, scanMsg)

	rs = listBody(t, srv, m, `{"domainId":1,"cursor":"`+cur(4)+`"}`)
	requireList(t, "new body", listOf(t, rs[0]), []uint64{3, 2, 1}, "")

	srv.scanKeys = 3
	requireList(t, "lone", list(t, srv, m, `{"domainId":1}`), []uint64{5, 4, 3}, cur(3))

	srv.scanKeys = 2
	x := hashHex(types.Hash{0xD1})
	f := `{"domainId":1,"contractId":"` + x + `","inputKind":4`
	requireList(t, "keys without a match", list(t, srv, m, f+`}`), []uint64{}, cur(3))
	requireList(t, "resumed", list(t, srv, m, f+`,"cursor":"`+cur(3)+`"}`), []uint64{2}, "")

	// A request that reads no key answers normally once the budget
	// is used up, and one that must read a key does not.
	srv.scanKeys = 5
	rs = listBody(t, srv, m,
		`{"domainId":1}`,
		`{"domainId":1,"fromBatch":2}`,
		`{"domainId":1,"contractId":"`+hashHex(types.Hash{0xD9})+`"}`,
		`{"domainId":1,"fromBatch":3,"toBatch":1}`,
		`{"domainId":1,"toBatch":9}`,
		`{"domainId":1}`)
	requireList(t, "exhausting request", listOf(t, rs[0]), []uint64{5, 4, 3, 2, 1}, cur(1))
	l := listOf(t, rs[1])
	requireList(t, "empty range", l, []uint64{}, "")
	if l.BatchID != 1 {
		t.Fatalf("empty range: batch %d, want 1", l.BatchID)
	}
	requireList(t, "unknown contract", listOf(t, rs[2]), []uint64{}, "")
	requireBodyErr(t, rs[3], -32602, "invalid params: fromBatch 3 is above toBatch 1")
	requireBodyErr(t, rs[4], -32004, "batch 9 not found")
	requireBodyErr(t, rs[5], -32600, scanMsg)

	srv.scanKeys = 5
	rs = listBody(t, srv, m, `{"domainId":1}`, `{"domainId":1,"fromBatch":2}`)
	requireList(t, "response 2 of 2", listOf(t, rs[1]), []uint64{}, "")
}

func TestListByteBudget(t *testing.T) {
	_, cs, s, r, srv, _ := listRig(t, historyL())
	acceptAt(s, cs, 100, 0)
	doPoll(t, r)
	const m = "zeno_getInputs"
	v, err := r.idx.view()
	if err != nil {
		t.Fatal(err)
	}
	defer v.Release()
	var L int
	for g := uint64(1); g <= 4; g++ {
		b, ok, err := v.get(ikey(pfxInput, be64(g)))
		if err != nil || !ok {
			t.Fatalf("input %d: %v %v", g, ok, err)
		}
		if g == 1 {
			L = len(b)
		} else if len(b) != L {
			t.Fatalf("input %d is %d bytes, input 1 is %d", g, len(b), L)
		}
	}
	if L <= codec.MaxCallPayload {
		t.Fatalf("L = %d", L)
	}
	if maxScanBytes != 32<<20 || srv.scanBytes != maxScanBytes {
		t.Fatalf("maxScanBytes %d, srv.scanBytes %d", maxScanBytes, srv.scanBytes)
	}

	srv.scanBytes = 2 * L
	requireList(t, "page 1", list(t, srv, m, `{"domainId":1}`), []uint64{4, 3}, cur(3))
	requireList(t, "page 2", list(t, srv, m, `{"domainId":1,"cursor":"`+cur(3)+`"}`), []uint64{2, 1}, cur(1))
	requireList(t, "page 3", list(t, srv, m, `{"domainId":1,"cursor":"`+cur(1)+`"}`), []uint64{}, "")

	srv.scanBytes = 3 * L
	rs := listBody(t, srv, m, `{"domainId":1}`, `{"domainId":1}`)
	requireList(t, "shared 1", listOf(t, rs[0]), []uint64{4, 3, 2}, cur(2))
	requireBodyErr(t, rs[1], -32600, scanMsg)

	srv.scanBytes = 1
	requireList(t, "above 1", list(t, srv, m, `{"domainId":1}`), []uint64{4}, cur(4))
	requireList(t, "above 2", list(t, srv, m, `{"domainId":1,"cursor":"`+cur(4)+`"}`), []uint64{3}, cur(3))

	f := `{"domainId":1,"sender":"` + senderOf(hashOfIndex(2)).String() + `","inputKind":9`
	requireList(t, "non-matching", list(t, srv, m, f+`}`), []uint64{}, cur(2))
	requireList(t, "non-matching resumed", list(t, srv, m, f+`,"cursor":"`+cur(2)+`"}`), []uint64{}, "")
}

// An events scan under a contract charges its ec/ point read and
// reads an input's n/ value once per input.
func TestListEventByteBudget(t *testing.T) {
	_, cs, s, r, srv, _ := listRig(t, historyE())
	acceptAt(s, cs, 100, 0, 1)
	doPoll(t, r)
	v, err := r.idx.view()
	if err != nil {
		t.Fatal(err)
	}
	defer v.Release()
	e0, ok, err := v.get(ikey(pfxEvent, be64(3), be32(0)))
	if err != nil || !ok {
		t.Fatalf("event (3, 0): %v %v", ok, err)
	}
	if e1, _, _ := v.get(ikey(pfxEvent, be64(3), be32(63))); len(e1) != len(e0) {
		t.Fatalf("event lengths %d and %d differ", len(e0), len(e1))
	}
	n3, ok, err := v.get(ikey(pfxInput, be64(3)))
	if err != nil || !ok {
		t.Fatalf("input 3: %v %v", ok, err)
	}
	E, N := len(e0), len(n3)

	srv.scanBytes = 2*E + N + 1
	l := list(t, srv, "zeno_getEvents", `{"domainId":1,"contractId":"`+hashHex(types.Hash{0xC1})+`"}`)
	if got := evpos(t, l.Items); !slices.Equal(got, [][2]uint64{{3, 63}, {3, 62}, {3, 61}}) || l.NextCursor == nil || *l.NextCursor != cur(3, 61) {
		t.Fatalf("events = %v cursor %v, want three events and %s", got, l.NextCursor, cur(3, 61))
	}
}

// A cursor only narrows a scan: one above the range's end reads no
// input above toBatch, and one below its start reads none below
// fromBatch or a contract's deploy.
func TestListForgedCursor(t *testing.T) {
	_, cs, s, r, srv, _ := listRig(t, historyC())
	acceptAt(s, cs, 100, 0, 1)
	doPoll(t, r)
	const m = "zeno_getInputs"
	requireList(t, "above toBatch", list(t, srv, m, `{"domainId":1,"toBatch":0,"cursor":"`+cur(1000)+`"}`), []uint64{3, 2, 1}, "")
	x := hashHex(types.Hash{0xD1})
	requireList(t, "contract above toBatch", list(t, srv, m, `{"domainId":1,"toBatch":0,"contractId":"`+x+`","cursor":"`+cur(1000)+`"}`), []uint64{3, 2}, "")
	requireList(t, "below fromBatch", list(t, srv, m, `{"domainId":1,"fromBatch":1,"cursor":"`+cur(2)+`"}`), []uint64{}, "")
	requireList(t, "below the deploy", list(t, srv, m, `{"domainId":1,"contractId":"`+x+`","cursor":"`+cur(2)+`"}`), []uint64{}, "")
}

// An empty page uses none of a body's key or byte budget.
func TestListEmptyPageBudget(t *testing.T) {
	_, cs, s, r, srv, _ := listRig(t, historyC())
	acceptAt(s, cs, 100, 0, 1)
	doPoll(t, r)
	const m = "zeno_getInputs"
	v, err := r.idx.view()
	if err != nil {
		t.Fatal(err)
	}
	defer v.Release()
	var total int
	for g := uint64(1); g <= 5; g++ {
		b, ok, err := v.get(ikey(pfxInput, be64(g)))
		if err != nil || !ok {
			t.Fatalf("input %d: %v %v", g, ok, err)
		}
		total += len(b)
	}
	empty := `{"domainId":1,"fromBatch":2}`

	srv.scanKeys = 6
	rs := listBody(t, srv, m, empty, `{"domainId":1}`)
	requireList(t, "empty page", listOf(t, rs[0]), []uint64{}, "")
	requireList(t, "keys after", listOf(t, rs[1]), []uint64{5, 4, 3, 2, 1}, "")

	srv.scanKeys, srv.scanBytes = maxScanKeys, total+1
	rs = listBody(t, srv, m, empty, `{"domainId":1}`)
	requireList(t, "empty page", listOf(t, rs[0]), []uint64{}, "")
	requireList(t, "bytes after", listOf(t, rs[1]), []uint64{5, 4, 3, 2, 1}, "")
}

// An outbox scan under a recipient charges its oa/ value, its o/
// point read and its input's n/ value.
func TestListOutboxByteBudget(t *testing.T) {
	_, cs, s, r, srv, _ := listRig(t, historyE())
	acceptAt(s, cs, 100, 0, 1)
	doPoll(t, r)
	v, err := r.idx.view()
	if err != nil {
		t.Fatal(err)
	}
	defer v.Release()
	to := types.Address{2}
	var total int
	for _, k := range [][]byte{
		ikey(pfxOutboxTo, to[:], be64(4), be32(0)),
		ikey(pfxOutbox, be64(4), be32(0)),
		ikey(pfxInput, be64(4)),
	} {
		b, ok, err := v.get(k)
		if err != nil || !ok {
			t.Fatalf("key %x: %v %v", k, ok, err)
		}
		total += len(b)
	}

	f := fmt.Sprintf(`{"domainId":1,"recipient":%q}`, to.String())
	srv.scanBytes = total
	requireList(t, "budget used exactly", list(t, srv, "zeno_getOutbox", f), []uint64{4}, cur(4, 0))
	srv.scanBytes = total + 1
	requireList(t, "one byte left", list(t, srv, "zeno_getOutbox", f), []uint64{4}, "")
}

func TestListResponseBudget(t *testing.T) {
	_, cs, s, r, srv, _ := listRig(t, historyE())
	acceptAt(s, cs, 100, 0, 1)
	doPoll(t, r)
	p := []json.RawMessage{json.RawMessage(`{"domainId":1,"limit":2}`)}
	call := func(left int) json.RawMessage {
		t.Helper()
		res, err := srv.getEvents(p, &budget{left: left, scan: maxScanKeys, scanBytes: maxScanBytes})
		if err != nil {
			t.Fatal(err)
		}
		return res.(json.RawMessage)
	}
	decode := func(raw json.RawMessage) listJSON {
		var l listJSON
		if err := json.Unmarshal(raw, &l); err != nil {
			t.Fatal(err)
		}
		return l
	}
	r2 := call(maxResponseBytes)
	l := decode(r2)
	if got := evpos(t, l.Items); !slices.Equal(got, [][2]uint64{{3, 63}, {3, 62}}) || *l.NextCursor != cur(3, 62) {
		t.Fatalf("R2 = %v cursor %v", got, l.NextCursor)
	}
	if got := call(len(r2)); !bytes.Equal(got, r2) {
		t.Fatalf("left len(R2) gave different bytes")
	}
	got := call(len(r2) - 1)
	l = decode(got)
	if pos := evpos(t, l.Items); !slices.Equal(pos, [][2]uint64{{3, 63}}) || *l.NextCursor != cur(3, 63) || len(got) > len(r2)-1 {
		t.Fatalf("left len(R2)-1: %v cursor %v length %d", pos, l.NextCursor, len(got))
	}
	if got := call(10); len(got) <= 10 || len(decode(got).Items) != 1 {
		t.Fatalf("left 10: %d bytes, %d items", len(got), len(decode(got).Items))
	}

	srv.methods["test_leave"] = func(_ *Server, _ []json.RawMessage, b *budget) (any, error) {
		return stringOf(b.left - 100), nil
	}
	body := `[{"jsonrpc":"2.0","method":"test_leave","params":[],"id":1},{"jsonrpc":"2.0","method":"zeno_getEvents","params":[{"domainId":1}],"id":2}]`
	var rs []rpcResp
	if err := json.Unmarshal(post(t, srv, body).Body.Bytes(), &rs); err != nil || len(rs) != 2 {
		t.Fatalf("%v %d", err, len(rs))
	}
	requireBodyErr(t, rs[1], -32600, "batch response exceeds 16 MiB; send this request alone")
}

func TestListParams(t *testing.T) {
	srv, _, _ := newTestServer(t, nil, genesisChain(t))
	const filterMsg = "invalid params: filter must be an object with members "
	members := map[string]string{
		"zeno_getInputs": inputsMembers,
		"zeno_getEvents": eventsMembers,
		"zeno_getOutbox": outboxMembers,
	}
	check := func(method, params string, code int, msg string) {
		t.Helper()
		r := callRPC(t, srv, method, params)
		if code == 0 {
			if r.Error != nil || string(r.Result) != noBatch {
				t.Fatalf("%s %s: %+v %s, want noBatch", method, params, r.Error, r.Result)
			}
			return
		}
		if r.Error == nil || r.Error.Code != code || r.Error.Message != msg {
			t.Fatalf("%s %s: %+v, want %d %q", method, params, r.Error, code, msg)
		}
	}
	for m, names := range members {
		check(m, `[]`, -32602, "invalid params: want 1 to 1 parameters, got 0")
		check(m, `[{"domainId":1},null]`, -32602, "invalid params: want 1 to 1 parameters, got 2")
		for _, p := range []string{`[null]`, `[1]`, `["x"]`, `[[]]`} {
			check(m, p, -32602, filterMsg+names)
		}
		for _, p := range []string{`[{}]`, `[{"domainId":null}]`} {
			check(m, p, -32602, "invalid params: domainId must be an integer from 0 to 4294967295")
		}
		check(m, `[{"domainId":9}]`, -32001, "unknown domain 9")
		for _, mem := range []string{"fromBatch", "toBatch", "limit", "cursor"} {
			check(m, `[{"domainId":1,"`+mem+`":null}]`, 0, "")
		}
		for _, p := range []string{`"fromBatch":-1`, `"fromBatch":"0"`, `"fromBatch":1.5`} {
			check(m, `[{"domainId":1,`+p+`}]`, -32602, "invalid params: fromBatch must be a batch id")
		}
		for _, p := range []string{`"toBatch":"newest"`, `"toBatch":-1`, `"toBatch":true`} {
			check(m, `[{"domainId":1,`+p+`}]`, -32602, `invalid params: toBatch must be "pending", "latest", "finalized" or a batch id`)
		}
		for _, p := range []string{`"limit":0`, `"limit":101`, `"limit":1.5`, `"limit":"5"`} {
			check(m, `[{"domainId":1,`+p+`}]`, -32602, "invalid params: limit must be an integer from 1 to 100")
		}
		for _, p := range []string{`"toBatch":"pending"`, `"toBatch":"finalized"`, `"limit":1`, `"limit":100`, `"fromBatch":5`} {
			check(m, `[{"domainId":1,`+p+`}]`, 0, "")
		}
		for _, p := range []string{`{"DOMAINID":1,"Limit":5}`, `{"domainId":1,"DomainId":2}`, `{"domainId":1,"domainId":2}`, `{"domainId":1,"limit":5,"limit":5}`, `{"domainId":1,"domainId, fromBatch":0}`, `{"domainId":1,"":0}`} {
			check(m, `[`+p+`]`, -32602, filterMsg+names)
		}
		check(m, `[{"domainId":1,"limit":5,"fromBatch":null}]`, 0, "")
		last := names[strings.LastIndex(names, ", ")+2:]
		for _, p := range []string{`{"domainId":1,"LIMIT":5,"fromBatch":0}`, `{"domainId":1,"limit":5,"FROMBATCH":0}`, `{"domainId":1,"` + strings.ToUpper(last) + `":null,"limit":5}`, `{"domainId":1,"` + last + `":null,"` + last + `":null}`} {
			check(m, `[`+p+`]`, -32602, filterMsg+names)
		}
		check(m, `[{"domainId":1,"toBatch":0}]`, -32004, "batch 0 not found")
		check(m, `[{"domainId":1,"fromBatch":1,"toBatch":0}]`, -32602, "invalid params: fromBatch 1 is above toBatch 0")
	}
	const curMsg = "invalid params: cursor must be a nextCursor of this method"
	h16, h24 := strings.Repeat("0", 16), strings.Repeat("0", 24)
	for _, p := range []string{`"00"`, `"` + strings.Repeat("0", 18) + `"`, `"` + h24 + `"`, `"` + strings.Repeat("z", 16) + `"`, `5`} {
		check("zeno_getInputs", `[{"domainId":1,"cursor":`+p+`}]`, -32602, curMsg)
	}
	check("zeno_getInputs", `[{"domainId":1,"cursor":"`+h16+`"}]`, 0, "")
	for _, p := range []string{`"` + h16 + `"`, `5`} {
		check("zeno_getEvents", `[{"domainId":1,"cursor":`+p+`}]`, -32602, curMsg)
		check("zeno_getOutbox", `[{"domainId":1,"cursor":`+p+`}]`, -32602, curMsg)
	}
	check("zeno_getEvents", `[{"domainId":1,"cursor":"`+h24+`"}]`, 0, "")
	check("zeno_getOutbox", `[{"domainId":1,"cursor":"`+h24+`"}]`, 0, "")

	for _, p := range []string{`"00"`, `5`} {
		check("zeno_getInputs", `[{"domainId":1,"contractId":`+p+`}]`, -32602, "invalid params: contractId must be 64 hex characters")
		check("zeno_getEvents", `[{"domainId":1,"contractId":`+p+`}]`, -32602, "invalid params: contractId must be 64 hex characters")
	}
	for _, p := range []string{`"z1bad"`, `5`, `"` + strings.Repeat("0", 64) + `"`} {
		check("zeno_getInputs", `[{"domainId":1,"sender":`+p+`}]`, -32602, "invalid params: sender must be an L1 address")
	}
	for _, p := range []string{`256`, `-1`, `"0"`} {
		check("zeno_getInputs", `[{"domainId":1,"inputKind":`+p+`}]`, -32602, "invalid params: inputKind must be an integer from 0 to 255")
	}
	for _, p := range []string{`"inputKind":255`, `"inputKind":null`, `"contractId":null`, `"sender":null`} {
		check("zeno_getInputs", `[{"domainId":1,`+p+`}]`, 0, "")
	}
	check("zeno_getInputs", `[{"domainId":1,"topics":[]}]`, -32602, filterMsg+inputsMembers)

	const topicsMsg = "invalid params: topics must be an array of at most 4 entries, each 64 hex characters or null"
	for _, p := range []string{`5`, `{}`, `["00"]`, `[null,null,null,null,null]`} {
		check("zeno_getEvents", `[{"domainId":1,"topics":`+p+`}]`, -32602, topicsMsg)
	}
	for _, p := range []string{`[]`, `[null,null,null,null]`, `null`} {
		check("zeno_getEvents", `[{"domainId":1,"topics":`+p+`}]`, 0, "")
	}
	check("zeno_getEvents", `[{"domainId":1,"sender":"x"}]`, -32602, filterMsg+eventsMembers)

	check("zeno_getOutbox", `[{"domainId":1,"recipient":"z1bad"}]`, -32602, "invalid params: recipient must be an L1 address")
	check("zeno_getOutbox", `[{"domainId":1,"recipient":null}]`, 0, "")
	check("zeno_getOutbox", fmt.Sprintf(`[{"domainId":1,"recipient":%q}]`, types.Address{1}.String()), 0, "")
	check("zeno_getOutbox", `[{"domainId":1,"contractId":"x"}]`, -32602, filterMsg+outboxMembers)
}
