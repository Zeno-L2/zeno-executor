package gateway

import (
	"bytes"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/zenon-network/go-zenon/common/types"

	"github.com/digitalSloth/go-executor/domain"
	"github.com/digitalSloth/go-executor/internal/codec"
)

// subRig is an indexed engine over history A in its own copy of the
// executor directory, with a server for it. pristine keeps the
// directory as built.
func subRig(t *testing.T) (pristine, exec string, cs []codec.BatchCommitment, s *fakeSettlement, r *rig, srv *Server) {
	t.Helper()
	pristine, cs = buildA(t)
	exec = t.TempDir()
	copyDir(t, pristine, exec)
	s = newFakeSettlement()
	s.height = 10
	r = newIndexedChain(t, exec, s)
	srv, _, _ = newTestServer(t, nil, r.c)
	return pristine, exec, cs, s, r, srv
}

// onlyConn returns the server's single open connection.
func onlyConn(t *testing.T, s *Server) *wsConn {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		s.wsMu.Lock()
		for c := range s.conns {
			s.wsMu.Unlock()
			return c
		}
		s.wsMu.Unlock()
		if time.Now().After(deadline) {
			t.Fatal("no connection within 5 s")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// subCount returns the subscriptions s holds.
func subCount(s *Server) int {
	s.wsMu.Lock()
	defer s.wsMu.Unlock()
	return len(s.subs)
}

func TestSubscribeParams(t *testing.T) {
	s, _, _ := newTestServer(t, nil, genesisChain(t))
	s.methods["test_leave2"] = func(_ *Server, _ []json.RawMessage, b *budget) (any, error) {
		return stringOf(b.left - (len(`{"jsonrpc":"2.0","id":1,"result":`) + 4)), nil
	}
	_, addr := serveHTTP(t, s, nil)
	c := newWSClient(t, addr)

	bad := func(method, params string, code int, msg string) {
		t.Helper()
		r := c.call(method, params)
		if r.Error == nil || r.Error.Code != code || (msg != "" && r.Error.Message != msg) {
			t.Fatalf("%s %s: %+v %s, want error %d %q", method, params, r.Error, r.Result, code, msg)
		}
	}
	const kindMsg = `invalid params: kind must be "heads", "receipt" or "events"`
	bad("zeno_subscribe", `[null,1]`, -32602, kindMsg)
	bad("zeno_subscribe", `["nope",1]`, -32602, kindMsg)
	bad("zeno_subscribe", `[5,1]`, -32602, kindMsg)
	bad("zeno_subscribe", `["heads"]`, -32602, "")
	bad("zeno_subscribe", `["heads",1,null]`, -32602, "invalid params: want 2 parameters, got 3")
	bad("zeno_subscribe", `["receipt","`+hashOfIndex(1).String()+`","latest"]`, -32602, "invalid params: want 2 parameters, got 3")
	bad("zeno_subscribe", `["heads",9]`, -32001, "unknown domain 9")
	bad("zeno_subscribe", `["receipt","abcd"]`, -32602, "invalid params: sendHash must be 64 hex characters")
	bad("zeno_subscribe", `["events",{"domainId":1,"fromBatch":1}]`, -32602, "invalid params: filter must be an object with members "+eventsSubMembers)
	bad("zeno_subscribe", `["events",{"domainId":1,"limit":1}]`, -32602, "")
	bad("zeno_subscribe", `["events",{"domainId":1,"cursor":null}]`, -32602, "")
	bad("zeno_subscribe", `["events",{}]`, -32602, "invalid params: domainId must be an integer from 0 to 4294967295")
	bad("zeno_subscribe", `["events",{"domainId":null}]`, -32602, "")
	bad("zeno_subscribe", `["events",{"domainId":1,"contractId":"x"}]`, -32602, "invalid params: contractId must be 64 hex characters")
	bad("zeno_subscribe", `["events",{"domainId":1,"topics":"x"}]`, -32602, "")
	bad("zeno_subscribe", `["events",{"domainId":1},"finalized"]`, -32602, `invalid params: tag must be "latest" or "pending"`)
	bad("zeno_subscribe", `["events",{"domainId":1},5]`, -32602, `invalid params: tag must be "latest" or "pending"`)
	if n := subCount(s); n != 0 {
		t.Fatalf("%d subscriptions after the failures, want 0", n)
	}

	pendingOf := func(id string) bool {
		s.wsMu.Lock()
		defer s.wsMu.Unlock()
		for sub := range s.subs {
			if sub.id == id {
				return sub.pending
			}
		}
		t.Fatalf("no subscription %q", id)
		return false
	}
	if pendingOf(c.subscribeOK(`["events",{"domainId":1},null]`)) {
		t.Fatal("a null tag subscribed to pending")
	}
	if pendingOf(c.subscribeOK(`["events",{"domainId":1},"latest"]`)) {
		t.Fatal(`the tag "latest" subscribed to pending`)
	}
	if !pendingOf(c.subscribeOK(`["events",{"domainId":1},"pending"]`)) {
		t.Fatal(`the tag "pending" did not subscribe to pending`)
	}

	// unsubscribe
	id := c.subscribeOK(`["heads",1]`)
	if r := c.call("zeno_unsubscribe", fmt.Sprintf("[%q]", id)); string(r.Result) != "true" {
		t.Fatalf("first unsubscribe = %s %+v", r.Result, r.Error)
	}
	if r := c.call("zeno_unsubscribe", fmt.Sprintf("[%q]", id)); string(r.Result) != "false" {
		t.Fatalf("second unsubscribe = %s %+v", r.Result, r.Error)
	}
	bad("zeno_unsubscribe", `[5]`, -32602, "invalid params: id must be a subscription id")
	bad("zeno_unsubscribe", `[]`, -32602, "")
	if r := c.call("zeno_unsubscribe", `["zz"]`); string(r.Result) != "false" {
		t.Fatalf("unknown id = %s %+v", r.Result, r.Error)
	}

	// Over HTTP both methods are WebSocket only.
	for m, msg := range map[string]string{
		"zeno_subscribe":   "zeno_subscribe is served over WebSocket only",
		"zeno_unsubscribe": "zeno_unsubscribe is served over WebSocket only",
	} {
		requireRPCErr(t, s, m, `["heads",1]`, -32601, msg)
	}

	// The 17th subscription of a connection.
	c2 := newWSClient(t, addr)
	var ids []string
	for i := 0; i < maxSubscriptions; i++ {
		ids = append(ids, c2.subscribeOK(`["heads",1]`))
	}
	r17 := c2.call("zeno_subscribe", `["heads",1]`)
	if r17.Error == nil || r17.Error.Code != -32600 || r17.Error.Message != "connection has 16 subscriptions; unsubscribe one first" {
		t.Fatalf("17th subscription: %+v %s", r17.Error, r17.Result)
	}
	if r := c2.call("zeno_unsubscribe", fmt.Sprintf("[%q]", ids[0])); string(r.Result) != "true" {
		t.Fatalf("unsubscribe = %s", r.Result)
	}
	c2.subscribeOK(`["heads",1]`)

	// A subscribe whose response the cap replaces leaves no
	// subscription.
	c3 := newWSClient(t, addr)
	before := subCount(s)
	c3.send(`[{"jsonrpc":"2.0","method":"test_leave2","params":[],"id":1},{"jsonrpc":"2.0","method":"zeno_subscribe","params":["heads",1],"id":2}]`)
	var resps []rpcResp
	if err := json.Unmarshal(c3.next(), &resps); err != nil || len(resps) != 2 {
		t.Fatalf("batch response: %v (%d items)", err, len(resps))
	}
	if e := resps[1].Error; resps[0].Error != nil || e == nil || e.Code != -32600 || e.Message != "batch response exceeds 16 MiB; send this request alone" {
		t.Fatalf("batch = %+v / %+v, want the overflow error for the subscribe", resps[0].Error, resps[1].Error)
	}
	if n := subCount(s); n != before {
		t.Fatalf("%d subscriptions, want %d: the overflowed subscribe stayed", n, before)
	}
}

// requireHeadNote requires the next note of c to be subscription id's
// head note, equal to zeno_getHead's result.
func requireHeadNote(t *testing.T, c *wsClient, srv *Server, id string) {
	t.Helper()
	n := c.note()
	want := callRPC(t, srv, "zeno_getHead", "[1]")
	if n.Params.Subscription != id || !bytes.Equal(n.Params.Result, want.Result) {
		t.Fatalf("head note = %s %s, want %s %s", n.Params.Subscription, n.Params.Result, id, want.Result)
	}
}

func TestSubscribeHeads(t *testing.T) {
	_, exec, cs, s, r, srv := subRig(t)
	_, addr := serveHTTP(t, srv, nil)
	c := newWSClient(t, addr)
	id := c.subscribeOK(`["heads",1]`)

	requireTags(t, doPoll(t, r), 4, -1, -1, StatusOK)
	requireHeadNote(t, c, srv, id)

	s.accept(cs[0], 20)
	s.accept(cs[1], 21)
	requireTags(t, doPoll(t, r), 4, 1, -1, StatusOK)
	requireHeadNote(t, c, srv, id)

	doPoll(t, r)
	c.none(200 * time.Millisecond)

	s.height = 21
	requireTags(t, doPoll(t, r), 4, 1, 1, StatusOK)
	requireHeadNote(t, c, srv, id)

	removeRecord(t, exec, 3)
	requireTags(t, doPoll(t, r), 2, 1, 1, StatusOK)
	requireHeadNote(t, c, srv, id)
	c.none(200 * time.Millisecond)
}

// requireReceiptNote requires the next note of c to be subscription
// id's receipt note with the given fields.
func requireReceiptNote(t *testing.T, c *wsClient, id string, batch uint64, da, send types.Hash, tag string, verified, removed bool) {
	t.Helper()
	n := c.note()
	var got receiptNote
	if err := json.Unmarshal(n.Params.Result, &got); err != nil {
		t.Fatal(err)
	}
	want := receiptNote{DomainID: 1, BatchID: batch, DAHash: da, SendHash: send, Tag: tag, Verified: verified, Removed: removed}
	if n.Params.Subscription != id || got != want {
		t.Fatalf("receipt note = %s %+v, want %s %+v", n.Params.Subscription, got, id, want)
	}
}

func TestSubscribeReceipt(t *testing.T) {
	t.Run("pending, latest, finalized", func(t *testing.T) {
		_, _, cs, s, r, srv := subRig(t)
		_, addr := serveHTTP(t, srv, nil)
		c := newWSClient(t, addr)
		send := hashOfIndex(1)
		id := c.subscribeOK(fmt.Sprintf(`["receipt","%s"]`, send))

		doPoll(t, r)
		requireReceiptNote(t, c, id, 0, cs[0].DAHash, send, "pending", false, false)
		s.accept(cs[0], 20)
		doPoll(t, r)
		requireReceiptNote(t, c, id, 0, cs[0].DAHash, send, "latest", true, false)
		s.height = 20
		doPoll(t, r)
		requireReceiptNote(t, c, id, 0, cs[0].DAHash, send, "finalized", true, false)
		c.none(200 * time.Millisecond)
	})
	t.Run("retraction", func(t *testing.T) {
		pristine, exec, cs, _, r, srv := subRig(t)
		_, addr := serveHTTP(t, srv, nil)
		c := newWSClient(t, addr)
		send := hashOfIndex(7)
		id := c.subscribeOK(fmt.Sprintf(`["receipt","%s"]`, send))

		doPoll(t, r)
		requireReceiptNote(t, c, id, 3, cs[3].DAHash, send, "pending", false, false)
		removeRecord(t, exec, 3)
		doPoll(t, r)
		requireReceiptNote(t, c, id, 3, cs[3].DAHash, send, "pending", false, true)
		copyDir(t, pristine, exec)
		doPoll(t, r)
		requireReceiptNote(t, c, id, 3, cs[3].DAHash, send, "pending", false, false)
		c.none(200 * time.Millisecond)
	})
}

// historyN is one batch of three inputs that emit 64, 64 and 22
// events of one contract, 150 in all, with no topics.
func historyN() [][]fixtureInput {
	events := func(g uint64, n int) domain.Effects {
		e := success()
		for j := 0; j < n; j++ {
			e.Events = append(e.Events, codec.Event{ContractID: types.Hash{0xC0}, InputIndex: g, EventIndex: uint32(j), Data: []byte{byte(j)}})
		}
		return e
	}
	return [][]fixtureInput{{
		{index: 1, effects: events(1, 64)},
		{index: 2, effects: events(2, 64)},
		{index: 3, effects: events(3, 22)},
	}}
}

// readEventsNote returns the next note of c, which must be an events
// note, and its subscription id.
func readEventsNote(t *testing.T, c *wsClient) (string, eventsNote) {
	t.Helper()
	n := c.note()
	var got eventsNote
	if err := json.Unmarshal(n.Params.Result, &got); err != nil {
		t.Fatal(err)
	}
	return n.Params.Subscription, got
}

// requireEventsNote requires the next note of c to be an events note
// with the given fields and returns its subscription id.
func requireEventsNote(t *testing.T, c *wsClient, batch uint64, da types.Hash, verified, removed bool, count int) string {
	t.Helper()
	id, got := readEventsNote(t, c)
	want := eventsNote{DomainID: 1, BatchID: batch, DAHash: da, Verified: verified, Removed: removed, Count: count}
	if got != want {
		t.Fatalf("events note = %+v, want %+v", got, want)
	}
	return id
}

// requirePaged requires zeno_getEvents for batch with filter, which
// the subscription uses too, to return exactly count items.
func requirePaged(t *testing.T, srv *Server, batch uint64, filter string, count int) {
	t.Helper()
	l := list(t, srv, "zeno_getEvents", fmt.Sprintf(`{%s"fromBatch":%d,"toBatch":%d}`, filter, batch, batch))
	if len(l.Items) != count || l.NextCursor != nil {
		t.Fatalf("zeno_getEvents for batch %d: %d items, next cursor %v, want %d items", batch, len(l.Items), l.NextCursor, count)
	}
}

func TestSubscribeEvents(t *testing.T) {
	dir, cs, s, r, srv, _ := listRig(t, historyE())
	_, addr := serveHTTP(t, srv, nil)
	c := newWSClient(t, addr)
	c0 := types.Hash{0xC0}
	topic := types.Hash{0x70, 0}
	fa := `"domainId":1,`
	fb := fmt.Sprintf(`"domainId":1,"contractId":"%s",`, c0)
	fc := fmt.Sprintf(`"domainId":1,"contractId":"%s","topics":["%s",null],`, c0, topic)
	idA := c.subscribeOK(`["events",{"domainId":1}]`)
	idB := c.subscribeOK(fmt.Sprintf(`["events",{"domainId":1,"contractId":"%s"},"pending"]`, c0))
	idC := c.subscribeOK(fmt.Sprintf(`["events",{"domainId":1,"contractId":"%s","topics":["%s",null]}]`, c0, topic))

	// Both batches are added pending; only the pending subscription
	// for contract C0 matches, in batch 0.
	doPoll(t, r)
	if id := requireEventsNote(t, c, 0, cs[0].DAHash, false, false, 64); id != idB {
		t.Fatalf("pending note went to %s, want %s", id, idB)
	}
	c.none(150 * time.Millisecond)

	// A retraction tells every pending subscription, matches or not.
	removeRecord(t, dir, 1)
	doPoll(t, r)
	if id := requireEventsNote(t, c, 1, cs[1].DAHash, false, true, 0); id != idB {
		t.Fatalf("removed note went to %s, want %s", id, idB)
	}
	c.none(150 * time.Millisecond)

	// Batch 0 is accepted: every subscription with a match is told,
	// with its own count.
	s.accept(cs[0], 20)
	doPoll(t, r)
	want := map[string]int{idA: 64, idB: 64, idC: 32}
	for i := 0; i < 3; i++ {
		id, n := readEventsNote(t, c)
		if n != (eventsNote{DomainID: 1, BatchID: 0, DAHash: cs[0].DAHash, Verified: true, Count: want[id]}) || want[id] == 0 {
			t.Fatalf("note of %s = %+v, want counts %v", id, n, want)
		}
		delete(want, id)
	}
	c.none(150 * time.Millisecond)
	requirePaged(t, srv, 0, fa, 64)
	requirePaged(t, srv, 0, fb, 64)
	requirePaged(t, srv, 0, fc, 32)

	// Batch 1 is accepted with no pending stage: only the subscription
	// without a contract filter has a match.
	s.accept(cs[1], 21)
	doPoll(t, r)
	if id := requireEventsNote(t, c, 1, cs[1].DAHash, true, false, 64); id != idA {
		t.Fatalf("batch 1 note went to %s, want %s", id, idA)
	}
	c.none(150 * time.Millisecond)
	requirePaged(t, srv, 1, fa, 64)
	requirePaged(t, srv, 1, fb, 0)
}

func TestSubscribeEventsOneNote(t *testing.T) {
	_, cs, s, r, srv, _ := listRig(t, historyN())
	_, addr := serveHTTP(t, srv, nil)
	c := newWSClient(t, addr)
	c.subscribeOK(`["events",{"domainId":1}]`)
	s.accept(cs[0], 20)
	doPoll(t, r)
	requireEventsNote(t, c, 0, cs[0].DAHash, true, false, 150)
	c.none(200 * time.Millisecond)
}

func TestSubscribeOrdering(t *testing.T) {
	t.Run("a response precedes the first note", func(t *testing.T) {
		_, _, cs, s, r, srv := subRig(t)
		_, addr := serveHTTP(t, srv, nil)
		c := newWSClient(t, addr)
		conn := onlyConn(t, srv)

		// The response waits on wmu, so the subscription exists but is
		// not active when the poll runs: that poll's change is not
		// reported.
		conn.wmu.Lock()
		c.send(`{"jsonrpc":"2.0","method":"zeno_subscribe","params":["heads",1],"id":1}`)
		deadline := time.Now().Add(5 * time.Second)
		for subCount(srv) == 0 {
			if time.Now().After(deadline) {
				t.Fatal("the subscription was not created within 5 s")
			}
			time.Sleep(5 * time.Millisecond)
		}
		doPoll(t, r)
		conn.wmu.Unlock()
		var resp rpcResp
		if err := json.Unmarshal(c.next(), &resp); err != nil || resp.ID == nil || resp.Error != nil {
			t.Fatalf("the first message is not the subscribe response: %v %+v", err, resp)
		}
		c.none(200 * time.Millisecond)

		s.accept(cs[0], 20)
		doPoll(t, r)
		c.note()
	})
	t.Run("no note after the unsubscribe response", func(t *testing.T) {
		_, _, cs, s, r, srv := subRig(t)
		_, addr := serveHTTP(t, srv, nil)
		c := newWSClient(t, addr)
		id := c.subscribeOK(`["heads",1]`)
		conn := onlyConn(t, srv)

		conn.wmu.Lock()
		s.accept(cs[0], 20)
		doPoll(t, r)
		c.send(fmt.Sprintf(`{"jsonrpc":"2.0","method":"zeno_unsubscribe","params":[%q],"id":1}`, id))
		deadline := time.Now().Add(5 * time.Second)
		for subCount(srv) != 0 {
			if time.Now().After(deadline) {
				t.Fatal("the subscription was not removed within 5 s")
			}
			time.Sleep(5 * time.Millisecond)
		}
		conn.wmu.Unlock()
		var resp rpcResp
		if err := json.Unmarshal(c.next(), &resp); err != nil || string(resp.Result) != "true" {
			t.Fatalf("the first message is not the true response: %v %+v %s", err, resp, resp.Result)
		}
		c.none(200 * time.Millisecond)
	})
}
