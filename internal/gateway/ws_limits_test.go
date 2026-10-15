package gateway

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"
)

func TestWSLimitDefaults(t *testing.T) {
	s, _, _ := newTestServer(t, nil, genesisChain(t))
	for name, c := range map[string][2]int{
		"connections":     {s.wsConns, 64},
		"queue":           {s.wsQueueLen, 256},
		"ws slots":        {cap(s.wsSem), 16},
		"shared slots":    {cap(s.sem), 32},
		"outbox reads":    {cap(s.outboxSem), 4},
		"subscriptions":   {maxSubscriptions, 16},
		"proofs per body": {maxBodyOutboxProofs, 8},
	} {
		if c[0] != c[1] {
			t.Fatalf("%s = %d, want %d", name, c[0], c[1])
		}
	}
	if cap(s.wsSem) >= cap(s.sem) {
		t.Fatal("the WebSocket slots leave the HTTP requests no slot")
	}
}

func TestWSOutboxProofBodyBudget(t *testing.T) {
	_, cs, s, r, srv, _ := listRig(t, historyO())
	s.accept(cs[0], 20)
	s.accept(cs[1], 21)
	doPoll(t, r)
	_, addr := serveHTTP(t, srv, nil)
	c := newWSClient(t, addr)

	unknown := fmt.Sprintf("%064x", 0xAA)
	var reqs []string
	for i := 1; i <= maxBodyOutboxProofs+1; i++ {
		reqs = append(reqs, fmt.Sprintf(`{"jsonrpc":"2.0","method":"zeno_getOutboxProof","params":["%s"],"id":%d}`, unknown, i))
	}
	c.send("[" + strings.Join(reqs, ",") + "]")
	var rs []rpcResp
	if err := json.Unmarshal(c.next(), &rs); err != nil || len(rs) != maxBodyOutboxProofs+1 {
		t.Fatalf("batch response: %v (%d items)", err, len(rs))
	}
	for i, resp := range rs[:maxBodyOutboxProofs] {
		if resp.Error == nil || resp.Error.Code != -32004 {
			t.Fatalf("request %d: %+v, want -32004", i+1, resp.Error)
		}
	}
	if e := rs[maxBodyOutboxProofs].Error; e == nil || e.Code != -32600 || e.Message != outboxBudgetMsg {
		t.Fatalf("9th request: %+v, want -32600 %q", e, outboxBudgetMsg)
	}

	// The budget belongs to one body: the next message has 8 again.
	if r := c.call("zeno_getOutboxProof", `["`+unknown+`"]`); r.Error == nil || r.Error.Code != -32004 {
		t.Fatalf("next body: %+v", r.Error)
	}
	// Null and extra params follow the HTTP rules over WebSocket.
	c.send(`{"jsonrpc":"2.0","method":"zeno_getOutboxProof","params":null,"id":7}`)
	var n rpcResp
	if err := json.Unmarshal(c.next(), &n); err != nil || n.Error == nil || n.Error.Code != -32602 {
		t.Fatalf("null params: %+v %v", n.Error, err)
	}
	if r := c.call("zeno_getOutboxProof", `["`+unknown+`",null]`); r.Error == nil || r.Error.Code != -32602 {
		t.Fatalf("extra param: %+v", r.Error)
	}
}
