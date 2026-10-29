package gateway

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"reflect"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/zenon-network/go-zenon/common/trie"
	"github.com/zenon-network/go-zenon/common/types"
	"github.com/zenon-network/go-zenon/vm/embedded/definition"

	"github.com/Zeno-L2/zeno-executor/domain"
	"github.com/Zeno-L2/zeno-executor/internal/codec"
)

// tagJSON and headJSON decode zeno_getHead's result.
type tagJSON struct {
	BatchID       uint64 `json:"batchId"`
	LastInputSeq  uint64 `json:"lastInputSeq"`
	PostStateRoot string `json:"postStateRoot"`
	Verified      bool   `json:"verified"`
}

type headJSON struct {
	DomainID       uint32   `json:"domainId"`
	Status         string   `json:"status"`
	InconsistentAt *uint64  `json:"inconsistentAt"`
	Pending        *tagJSON `json:"pending"`
	Latest         *tagJSON `json:"latest"`
	Finalized      *tagJSON `json:"finalized"`
}

func getHead(t *testing.T, s *Server) headJSON {
	t.Helper()
	r := callRPC(t, s, "zeno_getHead", "[1]")
	if r.Error != nil {
		t.Fatalf("zeno_getHead: %+v", r.Error)
	}
	var h headJSON
	if err := json.Unmarshal(r.Result, &h); err != nil {
		t.Fatal(err)
	}
	return h
}

// requireTag requires tag to be batch id with the given verified flag,
// or nil when id is -1.
func requireTag(t *testing.T, name string, tag *tagJSON, id int64, verified bool) {
	t.Helper()
	if id < 0 {
		if tag != nil {
			t.Fatalf("%s = %+v, want null", name, *tag)
		}
		return
	}
	if tag == nil || int64(tag.BatchID) != id || tag.Verified != verified {
		t.Fatalf("%s = %+v, want batch %d verified %v", name, tag, id, verified)
	}
}

func TestChainInfo(t *testing.T) {
	dir, cs := buildA(t)
	s := newFakeSettlement()
	s.height = 10
	s.accept(cs[0], 20)
	s.accept(cs[1], 21)
	r := newTestChain(t, dir, s)
	srv, _, _ := newTestServer(t, nil, r.c)

	rec := post(t, srv, `{"jsonrpc":"2.0","method":"zeno_chainInfo","params":[1],"id":1}`)
	requireBody(t, rec, okResp("1", `{"domainId":1,"networkName":"test","chain_id":null,"runtimeKind":null,"stfSpecHash":null,"status":"SYNCING","inconsistentAt":null,"profiles":[],"views":[]}`))

	doPoll(t, r)
	var info struct {
		RuntimeKind string `json:"runtimeKind"`
		StfSpecHash string `json:"stfSpecHash"`
		Status      string `json:"status"`
	}
	res := callRPC(t, srv, "zeno_chainInfo", "[1]")
	if err := json.Unmarshal(res.Result, &info); err != nil {
		t.Fatal(err)
	}
	if info.RuntimeKind != "WASM" || info.StfSpecHash != fixtureSTF.String() || info.Status != "OK" {
		t.Fatalf("info = %+v", info)
	}

	dir2, cs2 := buildA(t)
	s2 := newFakeSettlement()
	s2.height = 10
	s2.accept(cs2[0], 20)
	s2.record = &definition.DomainRecord{RuntimeKind: 7, StfSpecHash: fixtureSTF}
	r2 := newTestChain(t, dir2, s2)
	doPoll(t, r2)
	srv2, _, _ := newTestServer(t, nil, r2.c)
	res = callRPC(t, srv2, "zeno_chainInfo", "[1]")
	if err := json.Unmarshal(res.Result, &info); err != nil || info.RuntimeKind != "7" {
		t.Fatalf("runtimeKind = %q, %v", info.RuntimeKind, err)
	}
}

func TestGetHead(t *testing.T) {
	dir, cs := buildA(t)
	s := newFakeSettlement()
	s.height = 10
	s.accept(cs[0], 20)
	s.accept(cs[1], 21)
	r := newTestChain(t, dir, s)
	srv, _, _ := newTestServer(t, nil, r.c)

	doPoll(t, r)
	h := getHead(t, srv)
	requireTag(t, "pending", h.Pending, 4, false)
	requireTag(t, "latest", h.Latest, 1, true)
	requireTag(t, "finalized", h.Finalized, -1, false)
	if h.Pending.LastInputSeq != cs[4].LastInputSeq || h.Pending.PostStateRoot != cs[4].PostStateRoot.String() {
		t.Fatalf("pending = %+v, want batch 4's values", *h.Pending)
	}
	if h.Status != "OK" || h.InconsistentAt != nil || h.DomainID != 1 {
		t.Fatalf("head = %+v", h)
	}

	s.height = 21
	doPoll(t, r)
	h = getHead(t, srv)
	requireTag(t, "finalized", h.Finalized, 1, true)

	for i := 2; i <= 4; i++ {
		s.accept(cs[i], 100)
	}
	doPoll(t, r)
	h = getHead(t, srv)
	requireTag(t, "pending", h.Pending, 4, true)
	requireTag(t, "latest", h.Latest, 4, true)
}

func TestHeadReportsInconsistent(t *testing.T) {
	t.Run("check failure", func(t *testing.T) {
		dir, cs := buildA(t)
		flipLastByte(t, bundlePathOf(dir, cs[2]))
		s := newFakeSettlement()
		s.height = 10
		acceptFinal(s, cs, 2)
		r := newTestChain(t, dir, s)
		srv, _, _ := newTestServer(t, nil, r.c)
		doPoll(t, r)

		h := getHead(t, srv)
		if h.Status != "INCONSISTENT" || h.InconsistentAt == nil || *h.InconsistentAt != 2 {
			t.Fatalf("head = %+v", h)
		}
		requireTag(t, "latest", h.Latest, 1, true)
		var info struct {
			Status         string  `json:"status"`
			InconsistentAt *uint64 `json:"inconsistentAt"`
		}
		if err := json.Unmarshal(callRPC(t, srv, "zeno_chainInfo", "[1]").Result, &info); err != nil {
			t.Fatal(err)
		}
		if info.Status != "INCONSISTENT" || info.InconsistentAt == nil || *info.InconsistentAt != 2 {
			t.Fatalf("info = %+v", info)
		}
	})

	t.Run("finalize re-read", func(t *testing.T) {
		dir, cs := buildA(t)
		s := newFakeSettlement()
		s.height = 10
		s.accept(cs[0], 20)
		s.accept(cs[1], 21)
		r := newTestChain(t, dir, s)
		srv, _, _ := newTestServer(t, nil, r.c)
		doPoll(t, r)
		s.edit(1, func(be *definition.BatchEntry) { be.PostStateRoot = types.Hash{0xAB} })
		s.height = 21
		doPoll(t, r)

		h := getHead(t, srv)
		if h.Status != "INCONSISTENT" || h.InconsistentAt == nil || *h.InconsistentAt != 1 {
			t.Fatalf("head = %+v", h)
		}
		requireTag(t, "latest", h.Latest, 1, false)
		requireTag(t, "finalized", h.Finalized, 0, true)
		requireTag(t, "pending", h.Pending, 4, false)
	})
}

func TestHeadReportsStall(t *testing.T) {
	dir, cs := buildA(t)
	s := newFakeSettlement()
	s.height = 10
	for i := 0; i <= 4; i++ {
		s.accept(cs[i], 100)
	}
	if err := os.Remove(bundlePathOf(dir, cs[3])); err != nil {
		t.Fatal(err)
	}
	r := newTestChain(t, dir, s)
	srv, _, _ := newTestServer(t, nil, r.c)
	doPoll(t, r)

	h := getHead(t, srv)
	if h.Status != "SYNCING" || h.InconsistentAt != nil {
		t.Fatalf("head = %+v", h)
	}
	requireTag(t, "latest", h.Latest, 2, true)
}

func TestDomainParams(t *testing.T) {
	srv, _, _ := newTestServer(t, nil, genesisChain(t))
	for _, method := range []string{"zeno_getHead", "zeno_chainInfo"} {
		for _, p := range []string{`[]`, `[1,2]`, `["1"]`, `[-1]`, `[4294967296]`, `[1.5]`, `[1e0]`, `[null]`, `[true]`} {
			if r := callRPC(t, srv, method, p); r.Error == nil || r.Error.Code != -32602 {
				t.Fatalf("%s %s: error %+v, want -32602", method, p, r.Error)
			}
		}
		if r := callRPC(t, srv, method, `[9]`); r.Error == nil || r.Error.Code != -32001 || r.Error.Message != "unknown domain 9" {
			t.Fatalf("%s [9]: error %+v", method, r.Error)
		}
	}
}

func TestReadAndSimulate(t *testing.T) {
	srv, _, _ := newTestServer(t, nil, genesisChain(t))
	want := func(method, params string, code int, msg string) {
		t.Helper()
		r := callRPC(t, srv, method, params)
		if r.Error == nil || r.Error.Code != code || !strings.Contains(r.Error.Message, msg) {
			t.Fatalf("%s %s: %+v, want %d %q", method, params, r.Error, code, msg)
		}
	}
	want("zeno_read", `[1,"contract",{},"latest"]`, -32005, "the domain has no such view")
	want("zeno_read", `[1,"x"]`, -32005, "the domain has no such view")
	want("zeno_read", `[9,"x"]`, -32001, "unknown domain 9")
	want("zeno_read", `[1]`, -32602, "want 2 to 4 parameters, got 1")
	want("zeno_read", `[1,"x",{},"latest",5]`, -32602, "want 2 to 4 parameters, got 5")
	want("zeno_read", `[1,5]`, -32602, "view must be a string")
	want("zeno_read", `[1,null]`, -32602, "view must be a string")
	want("zeno_simulate", `[1,{}]`, -32006, "simulation is disabled")
	want("zeno_simulate", `[1]`, -32602, "want 2 to 3 parameters, got 1")
	want("zeno_simulate", `[9,{}]`, -32001, "unknown domain 9")
	want("zeno_simulate", `[]`, -32602, "want 2 to 3 parameters, got 0")
}

func TestUnservedMethods(t *testing.T) {
	srv, _, _ := newTestServer(t, nil, genesisChain(t))
	for m, msg := range map[string]string{
		"zeno_subscribe":   "zeno_subscribe is served over WebSocket only",
		"zeno_unsubscribe": "zeno_unsubscribe is served over WebSocket only",
	} {
		if r := callRPC(t, srv, m, `[1]`); r.Error == nil || r.Error.Code != -32601 || r.Error.Message != msg {
			t.Fatalf("%s: %+v, want -32601 %q", m, r.Error, msg)
		}
	}
}

// readRig returns history A's directory and commitments, a fake
// Settlement at height 10, an indexed engine over them and a server
// for it. Nothing is accepted or polled.
func readRig(t *testing.T) (string, []codec.BatchCommitment, *fakeSettlement, *rig, *Server, *logSink) {
	t.Helper()
	dir, cs := buildA(t)
	s := newFakeSettlement()
	s.height = 10
	r := newIndexedChain(t, dir, s)
	srv, sink, _ := newTestServer(t, nil, r.c)
	return dir, cs, s, r, srv, sink
}

// rpcErr calls method and returns its error, failing the test when
// the call succeeded.
func rpcErr(t *testing.T, srv *Server, method, params string) *rpcError {
	t.Helper()
	r := callRPC(t, srv, method, params)
	if r.Error == nil {
		t.Fatalf("%s %s: result %s, want an error", method, params, trunc(string(r.Result)))
	}
	return r.Error
}

// rpcOK calls method and decodes its result into out, failing the
// test on an error.
func rpcOK(t *testing.T, srv *Server, method, params string, out any) {
	t.Helper()
	r := callRPC(t, srv, method, params)
	if r.Error != nil {
		t.Fatalf("%s %s: %+v", method, params, *r.Error)
	}
	if err := json.Unmarshal(r.Result, out); err != nil {
		t.Fatalf("%s %s: %v: %s", method, params, err, trunc(string(r.Result)))
	}
}

// requireRPCErr requires the call to fail with code and, when msg is
// not empty, exactly that message.
func requireRPCErr(t *testing.T, srv *Server, method, params string, code int, msg string) {
	t.Helper()
	e := rpcErr(t, srv, method, params)
	if e.Code != code || (msg != "" && e.Message != msg) {
		t.Fatalf("%s %s: error %d %q, want %d %q", method, params, e.Code, e.Message, code, msg)
	}
}

// pathHex is p as the 64 hex characters a parameter carries.
func pathHex(p trie.Path) string { return types.Hash(p).String() }

// batchJSON decodes zeno_getBatch's result.
type batchJSON struct {
	DomainID      uint32 `json:"domainId"`
	BatchID       uint64 `json:"batchId"`
	Verified      bool   `json:"verified"`
	FirstInputSeq uint64 `json:"firstInputSeq"`
	LastInputSeq  uint64 `json:"lastInputSeq"`
	PreStateRoot  string `json:"preStateRoot"`
	PostStateRoot string `json:"postStateRoot"`
	InputRoot     string `json:"inputRoot"`
	ReceiptRoot   string `json:"receiptRoot"`
	EventRoot     string `json:"eventRoot"`
	OutboxRoot    string `json:"outboxRoot"`
	DAHash        string `json:"daHash"`
	Inputs        []struct {
		GlobalInputIndex uint64 `json:"globalInputIndex"`
		SendHash         string `json:"sendHash"`
		Status           string `json:"status"`
	} `json:"inputs"`
}

func TestGetBatch(t *testing.T) {
	_, cs, s, r, srv, _ := readRig(t)
	s.accept(cs[0], 20)
	s.accept(cs[1], 21)
	doPoll(t, r)

	var b batchJSON
	rpcOK(t, srv, "zeno_getBatch", `[1,2]`, &b)
	c := cs[2]
	if b.DomainID != 1 || b.BatchID != 2 || b.Verified || b.FirstInputSeq != c.FirstInputSeq || b.LastInputSeq != c.LastInputSeq ||
		b.PreStateRoot != c.PreStateRoot.String() || b.PostStateRoot != c.PostStateRoot.String() ||
		b.InputRoot != c.InputRoot.String() || b.ReceiptRoot != c.ReceiptRoot.String() ||
		b.EventRoot != c.EventRoot.String() || b.OutboxRoot != c.OutboxRoot.String() || b.DAHash != c.DAHash.String() {
		t.Fatalf("batch 2 = %+v, want %+v", b, c)
	}
	if len(b.Inputs) != 2 || b.Inputs[0].GlobalInputIndex != 4 || b.Inputs[0].SendHash != hashOfIndex(4).String() || b.Inputs[0].Status != "RUNTIME_FAULT" ||
		b.Inputs[1].GlobalInputIndex != 6 || b.Inputs[1].SendHash != hashOfIndex(6).String() || b.Inputs[1].Status != "SUCCESS" {
		t.Fatalf("batch 2 inputs = %+v", b.Inputs)
	}

	for _, p := range []string{`[1]`, `[1,"latest"]`, `[1,null]`, `[1,1]`} {
		var l batchJSON
		rpcOK(t, srv, "zeno_getBatch", p, &l)
		if l.BatchID != 1 || !l.Verified || l.DAHash != cs[1].DAHash.String() {
			t.Fatalf("%s = batch %d verified %v, want batch 1 verified", p, l.BatchID, l.Verified)
		}
	}
	var p batchJSON
	rpcOK(t, srv, "zeno_getBatch", `[1,"pending"]`, &p)
	if p.BatchID != 4 || p.Verified || p.DAHash != cs[4].DAHash.String() {
		t.Fatalf("pending = batch %d verified %v", p.BatchID, p.Verified)
	}
	var top batchJSON
	rpcOK(t, srv, "zeno_getBatch", `[1,4]`, &top)
	if top.BatchID != 4 || top.Verified || top.DAHash != cs[4].DAHash.String() {
		t.Fatalf("batch 4 by id = %+v, want the pending batch", top)
	}
	requireRPCErr(t, srv, "zeno_getBatch", `[1,5]`, -32004, "batch 5 not found")
	requireRPCErr(t, srv, "zeno_getBatch", `[1,9]`, -32004, "batch 9 not found")
	requireRPCErr(t, srv, "zeno_getBatch", `[1,"finalized"]`, -32004, "no batch is finalized yet")

	s.height = 21
	doPoll(t, r)
	var f batchJSON
	rpcOK(t, srv, "zeno_getBatch", `[1,0]`, &f)
	if f.BatchID != 0 || !f.Verified || f.PreStateRoot != cs[0].PreStateRoot.String() {
		t.Fatalf("batch 0 = %+v, want cs[0] verified", f)
	}

	gsrv, _, _ := newTestServer(t, nil, genesisChain(t))
	for _, p := range []string{`[1]`, `[1,"latest"]`} {
		requireRPCErr(t, gsrv, "zeno_getBatch", p, -32004, "no batch is latest yet")
	}
	requireRPCErr(t, gsrv, "zeno_getBatch", `[1,0]`, -32004, "batch 0 not found")
}

// stateJSON decodes zeno_getStateAt's result.
type stateJSON struct {
	BatchID       uint64  `json:"batchId"`
	Verified      bool    `json:"verified"`
	PostStateRoot string  `json:"postStateRoot"`
	Path          string  `json:"path"`
	Present       bool    `json:"present"`
	Value         *string `json:"value"`
}

// proofsJSON decodes zeno_getProof's result.
type proofsJSON struct {
	BatchID       uint64 `json:"batchId"`
	Verified      bool   `json:"verified"`
	PostStateRoot string `json:"postStateRoot"`
	Proofs        []struct {
		Path    string  `json:"path"`
		Present bool    `json:"present"`
		Value   *string `json:"value"`
		Proof   string  `json:"proof"`
	} `json:"proofs"`
}

func requireState(t *testing.T, got stateJSON, batch uint64, verified bool, root types.Hash, present bool, value *string) {
	t.Helper()
	if got.BatchID != batch || got.Verified != verified || got.PostStateRoot != root.String() || got.Present != present ||
		(got.Value == nil) != (value == nil) || (value != nil && *got.Value != *value) {
		val := "null"
		if got.Value != nil {
			val = fmt.Sprintf("%q", *got.Value)
		}
		t.Fatalf("state = %+v value %s, want batch %d verified %v root %s present %v", got, val, batch, verified, root, present)
	}
}

func TestStateAndProofs(t *testing.T) {
	dir, cs := buildA(t)
	s := newFakeSettlement()
	s.height = 10
	s.accept(cs[0], 20)
	s.accept(cs[1], 21)
	r := newTestChain(t, dir, s)
	srv, _, _ := newTestServer(t, nil, r.c)
	requireTags(t, doPoll(t, r), 4, 1, -1, StatusOK)

	str := func(v string) *string { return &v }
	state := func(path trie.Path, tag string) stateJSON {
		var out stateJSON
		rpcOK(t, srv, "zeno_getStateAt", fmt.Sprintf(`[1,"%s",%s]`, pathHex(path), tag), &out)
		return out
	}
	root4 := cs[4].PostStateRoot
	requireState(t, state(pA, `"pending"`), 4, false, root4, true, str(""))
	requireState(t, state(pB, `"pending"`), 4, false, root4, true, str("6232"))
	requireState(t, state(pC, `"pending"`), 4, false, root4, false, nil)
	if got := state(pA, `"pending"`); got.Path != pathHex(pA) {
		t.Fatalf("path = %s, want %s", got.Path, pathHex(pA))
	}

	root1 := cs[1].PostStateRoot
	requireState(t, state(pB, `1`), 1, true, root1, false, nil)
	requireState(t, state(pC, `1`), 1, true, root1, true, str("6331"))
	var left stateJSON
	rpcOK(t, srv, "zeno_getStateAt", fmt.Sprintf(`[1,"%s"]`, pathHex(pC)), &left)
	requireState(t, left, 1, true, root1, true, str("6331"))
	var leftProof proofsJSON
	rpcOK(t, srv, "zeno_getProof", fmt.Sprintf(`[1,["%s"]]`, pathHex(pC)), &leftProof)
	if leftProof.BatchID != 1 || leftProof.PostStateRoot != root1.String() {
		t.Fatalf("proof with the tag left out = batch %d root %s", leftProof.BatchID, leftProof.PostStateRoot)
	}

	var pr proofsJSON
	rpcOK(t, srv, "zeno_getProof", fmt.Sprintf(`[1,["%s","%s","%s","%s"],"pending"]`, pathHex(pA), pathHex(pB), pathHex(pC), pathHex(pD)), &pr)
	if pr.BatchID != 4 || pr.Verified || pr.PostStateRoot != root4.String() || len(pr.Proofs) != 4 {
		t.Fatalf("proofs = %+v", pr)
	}
	proof := func(i int) []byte {
		b, err := hex.DecodeString(pr.Proofs[i].Proof)
		if err != nil {
			t.Fatal(err)
		}
		return b
	}
	for i, want := range []string{pathHex(pA), pathHex(pB), pathHex(pC), pathHex(pD)} {
		if pr.Proofs[i].Path != want {
			t.Fatalf("proof %d is for %s, want %s", i, pr.Proofs[i].Path, want)
		}
	}
	// A proof of the wrong kind is an error, not false: either way it
	// does not verify.
	verifies := func(ok bool, err error) bool { return ok && err == nil }
	if !verifies(trie.VerifyProofByPath(root4, pA, []byte{}, proof(0))) {
		t.Fatal("the present-empty path's proof does not verify as presence")
	}
	if verifies(trie.VerifyAbsenceByPath(root4, pA, proof(0))) {
		t.Fatal("the present-empty path's proof verifies as absence")
	}
	if !verifies(trie.VerifyProofByPath(root4, pB, []byte("b2"), proof(1))) {
		t.Fatal("pB's proof does not verify")
	}
	if verifies(trie.VerifyProofByPath(root4, pB, []byte("b3"), proof(1))) {
		t.Fatal("pB's proof verifies a wrong value")
	}
	if !verifies(trie.VerifyAbsenceByPath(root4, pC, proof(2))) {
		t.Fatal("pC's proof does not verify as absence")
	}
	if verifies(trie.VerifyAbsenceByPath(cs[1].PostStateRoot, pC, proof(2))) {
		t.Fatal("pC's proof verifies against another batch's root")
	}
	if pr.Proofs[0].Value == nil || *pr.Proofs[0].Value != "" || pr.Proofs[2].Value != nil || pr.Proofs[2].Present {
		t.Fatalf("proof values: pA %v, pC %v present %v", pr.Proofs[0].Value, pr.Proofs[2].Value, pr.Proofs[2].Present)
	}

	paths := func(n int) string {
		return "[" + strings.TrimSuffix(strings.Repeat(`"`+pathHex(pA)+`",`, n), ",") + "]"
	}
	var many proofsJSON
	rpcOK(t, srv, "zeno_getProof", fmt.Sprintf(`[1,%s,"pending"]`, paths(64)), &many)
	if len(many.Proofs) != 64 {
		t.Fatalf("64 duplicate paths gave %d proofs", len(many.Proofs))
	}
	requireRPCErr(t, srv, "zeno_getProof", fmt.Sprintf(`[1,%s,"pending"]`, paths(65)), -32602, "invalid params: paths must be an array of 1 to 64 paths")

	s.height = 21
	requireTags(t, doPoll(t, r), 4, 1, 1, StatusOK)
	requireRPCErr(t, srv, "zeno_getStateAt", fmt.Sprintf(`[1,"%s",0]`, pathHex(pA)), -32002,
		"batch 0's state is no longer kept; state is kept from the finalized batch 1")
	requireRPCErr(t, srv, "zeno_getProof", fmt.Sprintf(`[1,["%s"],0]`, pathHex(pA)), -32002,
		"batch 0's state is no longer kept; state is kept from the finalized batch 1")
	requireState(t, state(pC, `"finalized"`), 1, true, root1, true, str("6331"))
}

func TestDataParams(t *testing.T) {
	srv, _, _ := newTestServer(t, nil, genesisChain(t))
	const (
		domainMsg = "invalid params: domainId must be an integer from 0 to 4294967295"
		tagMsg    = `invalid params: tag must be "pending", "latest", "finalized" or a batch id`
		pathsMsg  = "invalid params: paths must be an array of 1 to 64 paths"
		noLatest  = "no batch is latest yet"
	)
	h64 := strings.Repeat("ab", 32)
	arr := func(n int) string {
		return "[" + strings.TrimSuffix(strings.Repeat(`"`+h64+`",`, n), ",") + "]"
	}
	rows := []struct {
		method, params string
		code           int
		msg            string
	}{
		{"zeno_getBatch", `[]`, -32602, "invalid params: want 1 to 2 parameters, got 0"},
		{"zeno_getBatch", `[1,"latest",3]`, -32602, "invalid params: want 1 to 2 parameters, got 3"},
		{"zeno_getBatch", `[1,null,null]`, -32602, "invalid params: want 1 to 2 parameters, got 3"},
		{"zeno_getBatch", `[null]`, -32602, domainMsg},
		{"zeno_getBatch", `[1,"newest"]`, -32602, tagMsg},
		{"zeno_getBatch", `[1,-1]`, -32602, tagMsg},
		{"zeno_getBatch", `[1,1.5]`, -32602, tagMsg},
		{"zeno_getBatch", `[1,1e0]`, -32602, tagMsg},
		{"zeno_getBatch", `[1,"1"]`, -32602, tagMsg},
		{"zeno_getBatch", `[1,true]`, -32602, tagMsg},
		{"zeno_getBatch", `[1,18446744073709551616]`, -32602, tagMsg},
		{"zeno_getBatch", `[1,null]`, -32004, noLatest},
		{"zeno_getBatch", `{"domainId":1}`, -32602, "invalid params: params must be an array"},
		{"zeno_getBatch", `[9]`, -32001, ""},
		{"zeno_getReceipt", `[]`, -32602, ""},
		{"zeno_getReceipt", `[null]`, -32602, "invalid params: sendHash must be 64 hex characters"},
		{"zeno_getReceipt", `[5]`, -32602, "invalid params: sendHash must be 64 hex characters"},
		{"zeno_getReceipt", `["00"]`, -32602, "invalid params: sendHash must be 64 hex characters"},
		{"zeno_getReceipt", `["` + h64[:63] + `"]`, -32602, "invalid params: sendHash must be 64 hex characters"},
		{"zeno_getReceipt", `["0x` + h64 + `"]`, -32602, "invalid params: sendHash must be 64 hex characters"},
		{"zeno_getReceipt", `["` + h64 + `",null]`, -32602, ""},
		{"zeno_getStateAt", `[1]`, -32602, ""},
		{"zeno_getStateAt", `[null,"` + h64 + `"]`, -32602, domainMsg},
		{"zeno_getStateAt", `[1,null]`, -32602, "invalid params: path must be 64 hex characters"},
		{"zeno_getStateAt", `[1,"` + strings.Repeat("zz", 32) + `"]`, -32602, "invalid params: path must be 64 hex characters"},
		{"zeno_getStateAt", `[1,"` + h64 + `","x"]`, -32602, tagMsg},
		{"zeno_getStateAt", `[1,"` + h64 + `",null]`, -32004, noLatest},
		{"zeno_getStateAt", `[1,"` + h64 + `"]`, -32004, noLatest},
		{"zeno_getProof", `[null,["` + h64 + `"]]`, -32602, domainMsg},
		{"zeno_getProof", `[1,null]`, -32602, pathsMsg},
		{"zeno_getProof", `[1,"` + h64 + `"]`, -32602, pathsMsg},
		{"zeno_getProof", `[1,[]]`, -32602, pathsMsg},
		{"zeno_getProof", `[1,` + arr(65) + `]`, -32602, pathsMsg},
		{"zeno_getProof", `[1,[null]]`, -32602, "invalid params: paths[0] must be 64 hex characters"},
		{"zeno_getProof", `[1,["` + h64 + `",5]]`, -32602, "invalid params: paths[1] must be 64 hex characters"},
		{"zeno_getProof", `[1,["` + h64 + `"],null]`, -32004, noLatest},
		{"zeno_getProof", `[1,` + arr(64) + `]`, -32004, noLatest},
		{"zeno_getProof", `[1,["` + h64 + `"],"x"]`, -32602, tagMsg},
	}
	for _, m := range []string{"zeno_getBundle", "zeno_getReceiptSidecar"} {
		rows = append(rows, []struct {
			method, params string
			code           int
			msg            string
		}{
			{m, `[]`, -32602, "invalid params: want 1 to 1 parameters, got 0"},
			{m, `[null]`, -32602, "invalid params: daHash must be 64 hex characters"},
			{m, `["xyz"]`, -32602, "invalid params: daHash must be 64 hex characters"},
			{m, `["` + h64 + `",null]`, -32602, "invalid params: want 1 to 1 parameters, got 2"},
		}...)
	}
	for _, row := range rows {
		requireRPCErr(t, srv, row.method, row.params, row.code, row.msg)
	}
}

// receiptRes decodes zeno_getReceipt's result.
type receiptRes struct {
	DomainID         uint32 `json:"domainId"`
	BatchID          uint64 `json:"batchId"`
	Verified         bool   `json:"verified"`
	Tag              string `json:"tag"`
	GlobalInputIndex uint64 `json:"globalInputIndex"`
	SendHash         string `json:"sendHash"`
	Sender           string `json:"sender"`
	Receipt          struct {
		InputIndex  uint64 `json:"inputIndex"`
		InputHash   string `json:"inputHash"`
		BatchID     uint64 `json:"batchId"`
		Status      string `json:"status"`
		AbortCode   int32  `json:"abortCode"`
		EventCount  uint32 `json:"eventCount"`
		OutboxCount uint32 `json:"outboxCount"`
	} `json:"receipt"`
	ReturnData string `json:"returnData"`
	Events     []struct {
		ContractID string   `json:"contractId"`
		InputIndex uint64   `json:"inputIndex"`
		EventIndex uint32   `json:"eventIndex"`
		Topics     []string `json:"topics"`
		Data       string   `json:"data"`
	} `json:"events"`
	Outbox []struct {
		OutboxID       string  `json:"outboxId"`
		Kind           string  `json:"kind"`
		Recipient      *string `json:"recipient"`
		Amount         *string `json:"amount"`
		TargetContract *string `json:"targetContract"`
		Payload        string  `json:"payload"`
	} `json:"outbox"`
	Input json.RawMessage `json:"input"`
}

func getReceipt(t *testing.T, srv *Server, send string) receiptRes {
	t.Helper()
	var out receiptRes
	rpcOK(t, srv, "zeno_getReceipt", fmt.Sprintf(`["%s"]`, send), &out)
	return out
}

func TestGetReceiptTags(t *testing.T) {
	_, cs, s, r, srv, _ := readRig(t)
	requireTags(t, doPoll(t, r), 4, -1, -1, StatusOK)
	h7 := hashOfIndex(7)
	want := func(batch uint64, tag string, verified bool) {
		t.Helper()
		got := getReceipt(t, srv, h7.String())
		if got.DomainID != 1 || got.BatchID != batch || got.GlobalInputIndex != 7 || got.Tag != tag || got.Verified != verified ||
			got.Sender != senderOf(h7).String() || got.SendHash != h7.String() || got.Receipt.Status != "SUCCESS" {
			t.Fatalf("receipt = %+v, want batch %d tag %s verified %v", got, batch, tag, verified)
		}
	}
	want(3, "pending", false)

	acceptFinal(s, cs, 3)
	requireTags(t, doPoll(t, r), 4, 3, -1, StatusOK)
	want(3, "latest", true)

	s.height = 23
	requireTags(t, doPoll(t, r), 4, 3, 3, StatusOK)
	want(3, "finalized", true)
}

func TestGetReceiptContents(t *testing.T) {
	dir, cs, s, r, srv, _ := readRig(t)
	acceptFinal(s, cs, 4)
	doPoll(t, r)

	h1 := hashOfIndex(1)
	got := getReceipt(t, srv, h1.String())
	receipts, _, err := codec.DecodeReceiptSidecar(readFile(t, sidecarPath(dir, cs[0])))
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Events) != 1 {
		t.Fatalf("events = %+v, want one", got.Events)
	}
	ev := got.Events[0]
	if ev.ContractID != (types.Hash{0xC0}).String() || ev.InputIndex != 1 || ev.EventIndex != 0 || ev.Data != "657674" || len(ev.Topics) != 0 || ev.Topics == nil {
		t.Fatalf("event = %+v", ev)
	}
	if got.Outbox == nil || len(got.Outbox) != 0 || string(got.Input) != "null" {
		t.Fatalf("outbox %v, input %s; want [] and null", got.Outbox, got.Input)
	}
	if got.Receipt.InputIndex != receipts[0].InputIndex || got.Receipt.InputHash != receipts[0].InputHash.String() || got.Receipt.BatchID != 0 {
		t.Fatalf("receipt = %+v, want leaf 0 of batch 0's sidecar", got.Receipt)
	}
	if upper := getReceipt(t, srv, strings.ToUpper(h1.String())); fmt.Sprint(upper) != fmt.Sprint(got) {
		t.Fatalf("upper-case lookup differs: %+v", upper)
	}

	rv := getReceipt(t, srv, hashOfIndex(2).String())
	if rv.Receipt.Status != "REVERT" || rv.Receipt.AbortCode != 7 || rv.ReturnData != "6e6f" {
		t.Fatalf("revert = %+v", rv)
	}
	if rv.Events == nil || len(rv.Events) != 0 || rv.Outbox == nil || len(rv.Outbox) != 0 {
		t.Fatalf("revert events %v and outbox %v, want [] and []", rv.Events, rv.Outbox)
	}

	wd := getReceipt(t, srv, hashOfIndex(4).String())
	if wd.Receipt.Status != "RUNTIME_FAULT" || len(wd.Outbox) != 1 {
		t.Fatalf("withdrawal = %+v", wd)
	}
	o := wd.Outbox[0]
	if o.OutboxID != codec.OutboxID(1, 2, 4, 0).String() || o.Kind != "L1_WITHDRAWAL" || o.Recipient == nil || *o.Recipient != (types.Address{1}).String() ||
		o.Amount == nil || *o.Amount != "5" || o.TargetContract != nil || o.Payload != "" {
		t.Fatalf("outbox = %+v", o)
	}

	requireRPCErr(t, srv, "zeno_getReceipt", fmt.Sprintf(`["%s"]`, types.Hash{0x99}), -32004, fmt.Sprintf("send %s not found", types.Hash{0x99}))
}

func TestGetReceiptSkipsInvisible(t *testing.T) {
	dir, cs, _, a, _, _ := readRig(t)
	doPoll(t, a)

	hidden := newTestChain(t, dir, newFakeSettlement())
	hidden.c.cfg.Index = a.idx
	bdir := t.TempDir()
	copyDir(t, dir, bdir)
	s2 := newFakeSettlement()
	s2.height = 10
	acceptAt(s2, cs, 100, 0, 1, 2, 3, 4)
	b := newIndexedChain(t, bdir, s2)
	b.c.cfg.DomainID = 2
	requireTags(t, doPoll(t, b), 4, 4, -1, StatusOK)

	h8 := hashOfIndex(8).String()
	only, _, _ := newTestServer(t, nil, hidden.c)
	requireRPCErr(t, only, "zeno_getReceipt", fmt.Sprintf(`["%s"]`, h8), -32004, "")

	both, _, _ := newTestServer(t, nil, hidden.c, b.c)
	if got := getReceipt(t, both, h8); got.DomainID != 2 || got.BatchID != 4 || got.Tag != "latest" {
		t.Fatalf("receipt = %+v, want domain 2 batch 4 latest", got)
	}
	var raw map[string]json.RawMessage
	rpcOK(t, both, "zeno_getBundle", fmt.Sprintf(`["%s"]`, cs[4].DAHash), &raw)
	if string(raw["domainId"]) != "2" {
		t.Fatalf("bundle domainId = %s, want 2", raw["domainId"])
	}

	first, _, _ := newTestServer(t, nil, a.c, b.c)
	if got := getReceipt(t, first, h8); got.DomainID != 1 || got.Tag != "pending" {
		t.Fatalf("receipt = %+v, want domain 1 pending (configuration order)", got)
	}
}

// rawFile calls method for daHash and returns the raw result text and
// its members.
func rawFile(t *testing.T, srv *Server, method string, daHash types.Hash) (string, map[string]json.RawMessage) {
	t.Helper()
	r := callRPC(t, srv, method, fmt.Sprintf(`["%s"]`, daHash))
	if r.Error != nil {
		t.Fatalf("%s: %+v", method, *r.Error)
	}
	var m map[string]json.RawMessage
	if err := json.Unmarshal(r.Result, &m); err != nil {
		t.Fatal(err)
	}
	return string(r.Result), m
}

func requireRawFile(t *testing.T, text string, m map[string]json.RawMessage, key string, c codec.BatchCommitment, verified bool, want []byte) {
	t.Helper()
	if len(m) != 5 {
		t.Fatalf("members = %d, want exactly 5: %v", len(m), keysOf(m))
	}
	head := fmt.Sprintf(`{"domainId":1,"batchId":%d,"verified":%t,"daHash":"%s","%s":"`, c.BatchID, verified, c.DAHash, key)
	if !strings.HasPrefix(text, head) {
		t.Fatalf("result starts %s, want %s", trunc(text), head)
	}
	var enc string
	if err := json.Unmarshal(m[key], &enc); err != nil {
		t.Fatal(err)
	}
	if enc != hex.EncodeToString(want) {
		t.Fatalf("%s differs from the file on disk", key)
	}
}

func keysOf(m map[string]json.RawMessage) []string {
	var ks []string
	for k := range m {
		ks = append(ks, k)
	}
	return ks
}

func TestRawFiles(t *testing.T) {
	dir, cs, s, r, srv, sink := readRig(t)
	s.accept(cs[0], 20)
	s.accept(cs[1], 21)
	doPoll(t, r)

	text, m := rawFile(t, srv, "zeno_getBundle", cs[2].DAHash)
	requireRawFile(t, text, m, "bundle", cs[2], false, readFile(t, bundlePathOf(dir, cs[2])))
	text, m = rawFile(t, srv, "zeno_getReceiptSidecar", cs[2].DAHash)
	requireRawFile(t, text, m, "sidecar", cs[2], false, readFile(t, sidecarPath(dir, cs[2])))
	text, m = rawFile(t, srv, "zeno_getBundle", cs[1].DAHash)
	requireRawFile(t, text, m, "bundle", cs[1], true, readFile(t, bundlePathOf(dir, cs[1])))
	text, m = rawFile(t, srv, "zeno_getReceiptSidecar", cs[1].DAHash)
	requireRawFile(t, text, m, "sidecar", cs[1], true, readFile(t, sidecarPath(dir, cs[1])))
	// The upper-case parameter resolves to the same batch.
	if got := callRPC(t, srv, "zeno_getBundle", fmt.Sprintf(`["%s"]`, strings.ToUpper(cs[1].DAHash.String()))); got.Error != nil || string(got.Result) != rawOf(t, srv, "zeno_getBundle", cs[1].DAHash) {
		t.Fatalf("upper-case daHash: %+v", got.Error)
	}

	missing := types.Hash{0x99}
	requireRPCErr(t, srv, "zeno_getBundle", fmt.Sprintf(`["%s"]`, missing), -32004, fmt.Sprintf("daHash %s not found", missing))
	requireRPCErr(t, srv, "zeno_getReceiptSidecar", fmt.Sprintf(`["%s"]`, missing), -32004, fmt.Sprintf("daHash %s not found", missing))

	if err := os.Remove(sidecarPath(dir, cs[2])); err != nil {
		t.Fatal(err)
	}
	requireRPCErr(t, srv, "zeno_getReceiptSidecar", fmt.Sprintf(`["%s"]`, cs[2].DAHash), -32004, fmt.Sprintf("receipt sidecar %s not found", cs[2].DAHash))
	if err := os.Remove(bundlePathOf(dir, cs[0])); err != nil {
		t.Fatal(err)
	}
	requireRPCErr(t, srv, "zeno_getBundle", fmt.Sprintf(`["%s"]`, cs[0].DAHash), -32004, fmt.Sprintf("bundle %s not found", cs[0].DAHash))
	if n := sink.count("internal error"); n != 0 {
		t.Fatalf("a missing file logged %d internal errors:\n%s", n, sink.dump())
	}

	fifo := bundlePathOf(dir, cs[1])
	if err := os.Remove(fifo); err != nil {
		t.Fatal(err)
	}
	if err := syscall.Mkfifo(fifo, 0o644); err != nil {
		t.Fatal(err)
	}
	requireRPCErr(t, srv, "zeno_getBundle", fmt.Sprintf(`["%s"]`, cs[1].DAHash), -32603, "internal error")
	if n := sink.count("JSON-RPC zeno_getBundle: internal error", "not a regular file"); n != 1 {
		t.Fatalf("FIFO log lines = %d, want 1:\n%s", n, sink.dump())
	}

	flipLastByte(t, bundlePathOf(dir, cs[3]))
	requireRPCErr(t, srv, "zeno_getBundle", fmt.Sprintf(`["%s"]`, cs[3].DAHash), -32603, "internal error")
	if n := sink.count("JSON-RPC zeno_getBundle: internal error", "no longer hashes to its daHash", cs[3].DAHash.String()); n != 1 {
		t.Fatalf("changed bundle log lines = %d, want 1:\n%s", n, sink.dump())
	}
}

// rawOf returns the raw result text of a successful call.
func rawOf(t *testing.T, srv *Server, method string, daHash types.Hash) string {
	t.Helper()
	text, _ := rawFile(t, srv, method, daHash)
	return text
}

func TestRawSidecarServeChecks(t *testing.T) {
	dir, cs, s, r, srv, sink := readRig(t)
	acceptFinal(s, cs, 1)
	doPoll(t, r)
	call := func(c codec.BatchCommitment) {
		t.Helper()
		requireRPCErr(t, srv, "zeno_getReceiptSidecar", fmt.Sprintf(`["%s"]`, c.DAHash), -32603, "internal error")
	}

	// A sidecar that decodes but holds other receipts.
	rewriteSidecar(t, sidecarPath(dir, cs[0]), func(rs []codec.Receipt) { rs[0].GasUsed++ })
	call(cs[0])
	if n := sink.count("JSON-RPC zeno_getReceiptSidecar: internal error", "no longer matches receiptRoot", cs[0].ReceiptRoot.String()); n != 1 {
		t.Fatalf("log lines = %d, want 1:\n%s", n, sink.dump())
	}
	// A sidecar that does not decode.
	writeFile(t, sidecarPath(dir, cs[1]), []byte("junk"))
	call(cs[1])
	// A sidecar above the size limit is a read error, not -32600.
	writeFile(t, sidecarPath(dir, cs[1]), make([]byte, maxSidecarSize+1))
	call(cs[1])
	if n := sink.count("JSON-RPC zeno_getReceiptSidecar: internal error", "byte limit"); n != 1 {
		t.Fatalf("size error log lines = %d, want 1:\n%s", n, sink.dump())
	}
}

func TestBundleTooLarge(t *testing.T) {
	dir, cs, s, r, srv, _ := readRig(t)
	s.accept(cs[0], 20)
	s.accept(cs[1], 21)
	doPoll(t, r)

	for _, tc := range []struct {
		c        codec.BatchCommitment
		verified bool
	}{{cs[2], false}, {cs[1], true}} {
		c := tc.c
		n := len(readFile(t, bundlePathOf(dir, c)))
		p := fmt.Sprintf(`{"domainId":1,"batchId":%d,"verified":%t,"daHash":"%s","bundle":"`, c.BatchID, tc.verified, c.DAHash)
		params := []json.RawMessage{json.RawMessage(fmt.Sprintf(`"%s"`, c.DAHash))}

		got, err := srv.getBundle(params, &budget{left: len(p) + 2 + 2*n})
		raw, ok := got.(json.RawMessage)
		if err != nil || !ok || len(raw) != len(p)+2+2*n || !strings.HasPrefix(string(raw), p) || !strings.HasSuffix(string(raw), `"}`) || !json.Valid(raw) {
			t.Fatalf("batch %d at the exact ceiling: %T len %d, %v", c.BatchID, got, len(raw), err)
		}

		for _, left := range []int{len(p) + 2 + 2*n - 1, len(p) + 2 + 2*n - 2} {
			ceiling := (left - len(p) - 2) / 2
			_, err = srv.getBundle(params, &budget{left: left})
			want := fmt.Sprintf("bundle %s is larger than this response can carry (%d bytes); fetch GET /bundles/<daHash>", c.DAHash, ceiling)
			var re *rpcError
			if !errors.As(err, &re) || re.Code != -32600 || re.Message != want {
				t.Fatalf("batch %d at left %d: %v, want -32600 %q", c.BatchID, left, err, want)
			}
			b, _ := json.Marshal(re)
			var back rpcError
			if json.Unmarshal(b, &back) != nil || back.Message != want {
				t.Fatalf("marshalled error = %s, want message %q", b, want)
			}
		}
		_, err = srv.getBundle(params, &budget{left: len(p) + 1})
		var re *rpcError
		if !errors.As(err, &re) || re.Code != -32600 || !strings.Contains(re.Message, "(0 bytes)") {
			t.Fatalf("batch %d with no room: %v, want a 0 byte ceiling", c.BatchID, err)
		}
	}
}

func TestRawFilesCountTowardResponseBudget(t *testing.T) {
	_, cs, s, r, srv, _ := readRig(t)
	acceptFinal(s, cs, 1)
	doPoll(t, r)
	srv.methods["test_fill"] = func(_ *Server, _ []json.RawMessage, b *budget) (any, error) { return stringOf(b.left), nil }
	body := fmt.Sprintf(`[{"jsonrpc":"2.0","method":"zeno_getBundle","params":["%s"],"id":1},{"jsonrpc":"2.0","method":"test_fill","id":2}]`, cs[1].DAHash)
	rec := post(t, srv, body)
	if !strings.Contains(rec.Body.String()[:200], `"bundle":"`) {
		t.Fatalf("the first response is not a bundle: %s", trunc(rec.Body.String()))
	}
	if rec.Code != 200 || rec.Body.Len() != maxResponseBytes {
		t.Fatalf("batch body = %d bytes (status %d), want exactly %d: the bundle response counts against the budget", rec.Body.Len(), rec.Code, maxResponseBytes)
	}
}

func TestReadsIdentityRace(t *testing.T) {
	_, cs, s, r, srv, _ := readRig(t)
	s.accept(cs[0], 100)
	s.accept(cs[1], 100)
	requireTags(t, doPoll(t, r), 4, 1, -1, StatusOK)

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
	requireRPCErr(t, srv, "zeno_getBatch", `[1,"pending"]`, -32004, "batch 4 is no longer available")
	requireRPCErr(t, srv, "zeno_getReceipt", fmt.Sprintf(`["%s"]`, hashOfIndex(8)), -32004, fmt.Sprintf("send %s not found", hashOfIndex(8)))
	requireRPCErr(t, srv, "zeno_getBundle", fmt.Sprintf(`["%s"]`, cs[4].DAHash), -32004, "")
	requireRPCErr(t, srv, "zeno_getReceiptSidecar", fmt.Sprintf(`["%s"]`, cs[4].DAHash), -32004, "")
	bv.DAHash = cs[4].DAHash
	bv.ReceiptRoot = types.Hash{0xBC}
	if raw, err = json.Marshal(bv); err != nil {
		t.Fatal(err)
	}
	if err := r.idx.db.Put(key, raw, nil); err != nil {
		t.Fatal(err)
	}
	requireRPCErr(t, srv, "zeno_getBatch", `[1,"pending"]`, -32004, "batch 4 is no longer available")

	if err := r.idx.retract(3); err != nil {
		t.Fatal(err)
	}
	requireRPCErr(t, srv, "zeno_getBatch", `[1,3]`, -32004, "batch 3 is no longer available")
	requireRPCErr(t, srv, "zeno_getReceipt", fmt.Sprintf(`["%s"]`, hashOfIndex(7)), -32004, "")
	var b batchJSON
	rpcOK(t, srv, "zeno_getBatch", `[1,1]`, &b)
	if b.BatchID != 1 || b.DAHash != cs[1].DAHash.String() {
		t.Fatalf("batch 1 = %+v", b)
	}

	t.Run("replaced and finalized after the view", func(t *testing.T) {
		dir, cs := buildA(t)
		bdir := t.TempDir()
		csB := buildHistory(t, bdir, nil, historyB())
		copyBatch(t, bdir, dir, csB[3])
		removeRecord(t, dir, 4)
		s := newFakeSettlement()
		s.height = 10
		acceptAt(s, cs, 20, 0, 1, 2)
		r := newIndexedChain(t, dir, s)
		srv, _, _ := newTestServer(t, nil, r.c)
		requireTags(t, doPoll(t, r), 3, 2, -1, StatusOK)

		v, err := r.idx.view()
		if err != nil {
			t.Fatal(err)
		}
		defer v.Release()
		if held, ok, err := v.batchAt(3); err != nil || !ok || held.DAHash != csB[3].DAHash {
			t.Fatalf("stale view batch 3 = %+v, %v, %v; want csB[3]", held, ok, err)
		}

		s.accept(cs[3], 20)
		s.height = 20
		requireTags(t, doPoll(t, r), 3, 3, 3, StatusOK)

		tr := r.c.readTag(tag{id: 3})
		_, err = heldBatch(v, tr)
		var re *rpcError
		if !errors.As(err, &re) || re.Code != -32004 || re.Message != "batch 3 is no longer available" {
			t.Fatalf("stale view heldBatch = %v, want -32004 batch 3 is no longer available", err)
		}
		if res, err := srv.receiptFrom(r.c, v, hashOfIndex(7)); res != nil || err != nil {
			t.Fatalf("receiptFrom over the stale view = %+v, %v; want nil, nil", res, err)
		}
		if _, _, ok, err := batchFrom(r.c, v, csB[3].DAHash); ok || err != nil {
			t.Fatalf("batchFrom over the stale view = ok %v, %v; want not ok", ok, err)
		}

		fresh, err := r.idx.view()
		if err != nil {
			t.Fatal(err)
		}
		defer fresh.Release()
		if held, err := heldBatch(fresh, tr); err != nil || held.DAHash != cs[3].DAHash {
			t.Fatalf("fresh view heldBatch = %+v, %v; want cs[3]", held, err)
		}
		if res, err := srv.receiptFrom(r.c, fresh, hashOfIndex(7)); err != nil || res == nil || res.Tag != "finalized" {
			t.Fatalf("receiptFrom over a fresh view = %+v, %v", res, err)
		}

		requireRPCErr(t, srv, "zeno_getBundle", fmt.Sprintf(`["%s"]`, csB[3].DAHash), -32004, fmt.Sprintf("daHash %s not found", csB[3].DAHash))
		var b batchJSON
		rpcOK(t, srv, "zeno_getBatch", `[1,3]`, &b)
		if b.DAHash != cs[3].DAHash.String() || !b.Verified {
			t.Fatalf("batch 3 = %+v, want cs[3] verified", b)
		}
	})
}

func TestReadsIndexDamage(t *testing.T) {
	_, cs, s, r, srv, sink := readRig(t)
	s.accept(cs[0], 20)
	s.accept(cs[1], 21)
	doPoll(t, r)
	if err := r.idx.db.Delete(ikey(pfxInput, be64(6)), nil); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		requireRPCErr(t, srv, "zeno_getBatch", `[1,2]`, -32603, "internal error")
	}
	if n := sink.count("JSON-RPC zeno_getBatch: internal error", "the index is damaged"); n != 1 {
		t.Fatalf("damage log lines = %d, want 1:\n%s", n, sink.dump())
	}
}

func TestReadsDamageAtOrBelowFinalized(t *testing.T) {
	_, cs, s, r, srv, sink := readRig(t)
	s.accept(cs[0], 20)
	s.accept(cs[1], 21)
	s.height = 21
	requireTags(t, doPoll(t, r), 4, 1, 1, StatusOK)
	if err := r.idx.db.Delete(ikey(pfxBatch, be64(0)), nil); err != nil {
		t.Fatal(err)
	}
	requireRPCErr(t, srv, "zeno_getBatch", `[1,0]`, -32603, "internal error")
	if n := sink.count("JSON-RPC zeno_getBatch: internal error", "the index is damaged"); n != 1 {
		t.Fatalf("damage log lines = %d, want 1:\n%s", n, sink.dump())
	}
}

func TestReadsInconsistent(t *testing.T) {
	dir, cs, s, r, srv, _ := readRig(t)
	s.accept(cs[0], 20)
	s.accept(cs[1], 21)
	doPoll(t, r)
	s.edit(1, func(be *definition.BatchEntry) { be.PostStateRoot = types.Hash{0xAB} })
	s.height = 21
	requireTags(t, doPoll(t, r), 4, 1, 0, StatusInconsistent)

	const msg = "the domain is INCONSISTENT from batch 1"
	for _, p := range []string{`[1]`, `[1,"latest"]`, `[1,"pending"]`, `[1,1]`, `[1,3]`, `[1,9]`} {
		requireRPCErr(t, srv, "zeno_getBatch", p, -32003, msg)
	}
	for _, p := range []string{`[1,"finalized"]`, `[1,0]`} {
		var b batchJSON
		rpcOK(t, srv, "zeno_getBatch", p, &b)
		if b.BatchID != 0 || !b.Verified {
			t.Fatalf("%s = batch %d verified %v, want batch 0 verified", p, b.BatchID, b.Verified)
		}
	}
	requireRPCErr(t, srv, "zeno_getStateAt", fmt.Sprintf(`[1,"%s","latest"]`, pathHex(pA)), -32003, msg)
	requireRPCErr(t, srv, "zeno_getStateAt", fmt.Sprintf(`[1,"%s",1]`, pathHex(pA)), -32003, msg)
	requireRPCErr(t, srv, "zeno_getProof", fmt.Sprintf(`[1,["%s"],"pending"]`, pathHex(pA)), -32003, msg)
	requireRPCErr(t, srv, "zeno_getReceipt", fmt.Sprintf(`["%s"]`, hashOfIndex(3)), -32003, msg)
	requireRPCErr(t, srv, "zeno_getBundle", fmt.Sprintf(`["%s"]`, cs[1].DAHash), -32003, msg)
	requireRPCErr(t, srv, "zeno_getReceiptSidecar", fmt.Sprintf(`["%s"]`, cs[1].DAHash), -32003, msg)

	var st stateJSON
	rpcOK(t, srv, "zeno_getStateAt", fmt.Sprintf(`[1,"%s","finalized"]`, pathHex(pA)), &st)
	if st.BatchID != 0 || !st.Verified {
		t.Fatalf("finalized state = %+v", st)
	}
	if got := getReceipt(t, srv, hashOfIndex(1).String()); got.Tag != "finalized" || !got.Verified {
		t.Fatalf("receipt = %+v, want finalized verified", got)
	}
	text, m := rawFile(t, srv, "zeno_getBundle", cs[0].DAHash)
	requireRawFile(t, text, m, "bundle", cs[0], true, readFile(t, bundlePathOf(dir, cs[0])))
}

// A caller that alternates daHashes whose files fail the serve-time
// check cannot make each request log a line: the lines differ only in
// the daHash, so they are one kind.
func TestRawFilesLogOncePerKind(t *testing.T) {
	dir, cs, s, r, srv, sink := readRig(t)
	acceptFinal(s, cs, 1)
	doPoll(t, r)
	flipLastByte(t, bundlePathOf(dir, cs[0]))
	flipLastByte(t, bundlePathOf(dir, cs[1]))
	rewriteSidecar(t, sidecarPath(dir, cs[0]), func(rs []codec.Receipt) { rs[0].GasUsed++ })
	rewriteSidecar(t, sidecarPath(dir, cs[1]), func(rs []codec.Receipt) { rs[0].GasUsed++ })

	for i := 0; i < 6; i++ {
		c := cs[i%2]
		requireRPCErr(t, srv, "zeno_getBundle", fmt.Sprintf(`["%s"]`, c.DAHash), -32603, "internal error")
		requireRPCErr(t, srv, "zeno_getReceiptSidecar", fmt.Sprintf(`["%s"]`, c.DAHash), -32603, "internal error")
	}
	if n := sink.count("JSON-RPC zeno_getBundle: internal error"); n != 1 {
		t.Fatalf("bundle log lines = %d, want 1:\n%s", n, sink.dump())
	}
	if n := sink.count("JSON-RPC zeno_getReceiptSidecar: internal error"); n != 1 {
		t.Fatalf("sidecar log lines = %d, want 1:\n%s", n, sink.dump())
	}
}

// proofBody returns a batch of n zeno_getProof requests of paths
// paths each.
func proofBody(n, paths int) string {
	ps := make([]string, paths)
	for i := range ps {
		ps[i] = fmt.Sprintf(`"%s"`, pathHex(pA))
	}
	reqs := make([]string, n)
	for i := range reqs {
		reqs[i] = fmt.Sprintf(`{"jsonrpc":"2.0","method":"zeno_getProof","params":[1,[%s]],"id":%d}`, strings.Join(ps, ","), i+1)
	}
	return "[" + strings.Join(reqs, ",") + "]"
}

// proofCodes posts body and returns the error code of each response,
// 0 for a result.
func proofCodes(t *testing.T, srv *Server, body string) ([]int, string) {
	t.Helper()
	rec := post(t, srv, body)
	var rs []rpcResp
	if err := json.Unmarshal(rec.Body.Bytes(), &rs); err != nil {
		t.Fatalf("%v: %s", err, trunc(rec.Body.String()))
	}
	codes := make([]int, len(rs))
	msg := ""
	for i, r := range rs {
		if r.Error != nil {
			codes[i], msg = r.Error.Code, r.Error.Message
		}
	}
	return codes, msg
}

func TestProofPathBodyBudget(t *testing.T) {
	_, cs, s, r, srv, _ := readRig(t)
	acceptFinal(s, cs, 1)
	doPoll(t, r)

	codes, msg := proofCodes(t, srv, proofBody(5, 64))
	if fmt.Sprint(codes) != "[0 0 0 0 -32600]" || msg != "request body exceeds its proof path budget" {
		t.Fatalf("5 requests of 64 paths: codes %v, last message %q", codes, msg)
	}
	// 4 * 64 paths use the whole budget, so one more path is refused.
	one := `,{"jsonrpc":"2.0","method":"zeno_getProof","params":[1,["` + pathHex(pA) + `"]],"id":9}]`
	codes, _ = proofCodes(t, srv, strings.TrimSuffix(proofBody(4, 64), "]")+one)
	if fmt.Sprint(codes) != "[0 0 0 0 -32600]" {
		t.Fatalf("4 requests of 64 paths then 1 path: codes %v", codes)
	}
	codes, _ = proofCodes(t, srv, proofBody(100, 1))
	for i, c := range codes {
		if c != 0 {
			t.Fatalf("100 requests of 1 path: response %d has error %d", i, c)
		}
	}
	if len(codes) != 100 {
		t.Fatalf("100 requests of 1 path: %d responses", len(codes))
	}
	// A request over the per-request cap still answers -32602.
	requireRPCErr(t, srv, "zeno_getProof", fmt.Sprintf(`[1,[%s]]`, strings.TrimSuffix(strings.Repeat(`"`+pathHex(pA)+`",`, 65), ",")), -32602, "")
}

// historyO is two batches that emit outbox messages: inputs 1 and 2
// each emit two L2 deliveries in batch 0, and input 3 emits one in
// batch 1.
func historyO() [][]fixtureInput {
	deliver := func(g uint64, n int) domain.Effects {
		e := success()
		for j := 0; j < n; j++ {
			e.OutboxMessages = append(e.OutboxMessages, codec.OutboxMessage{
				SourceContract: types.Hash{0xC0},
				InputIndex:     g,
				OutboxIndex:    uint32(j),
				Target:         codec.L2Delivery{TargetContract: types.Hash{0xC1}},
				Payload:        []byte{byte(g), byte(j)},
			})
		}
		return e
	}
	return [][]fixtureInput{
		{{index: 1, effects: deliver(1, 2)}, {index: 2, effects: deliver(2, 2)}},
		{{index: 3, effects: deliver(3, 1)}},
	}
}

// batch0Outbox is the ids of the four outbox messages of historyO's
// batch 0, in order.
func batch0Outbox() []types.Hash {
	return []types.Hash{codec.OutboxID(1, 0, 1, 0), codec.OutboxID(1, 0, 1, 1), codec.OutboxID(1, 0, 2, 0), codec.OutboxID(1, 0, 2, 1)}
}

// outboxProofJSON decodes zeno_getOutboxProof's result.
type outboxProofJSON struct {
	DomainID        uint32         `json:"domainId"`
	BatchID         uint64         `json:"batchId"`
	Verified        bool           `json:"verified"`
	Tag             string         `json:"tag"`
	OutboxID        string         `json:"outboxId"`
	OutboxRoot      string         `json:"outboxRoot"`
	Message         string         `json:"message"`
	InclusionProof  string         `json:"inclusionProof"`
	Decoded         map[string]any `json:"decoded"`
	ProcessedOutbox bool           `json:"processedOutbox"`
}

func outboxParam(id types.Hash) string { return fmt.Sprintf(`["%s"]`, id) }

func outboxProof(t *testing.T, srv *Server, id types.Hash) outboxProofJSON {
	t.Helper()
	var p outboxProofJSON
	rpcOK(t, srv, "zeno_getOutboxProof", outboxParam(id), &p)
	return p
}

func TestOutboxProof(t *testing.T) {
	_, cs, s, r, srv, _ := listRig(t, historyO())
	s.accept(cs[0], 20)
	doPoll(t, r)

	var b batchJSON
	rpcOK(t, srv, "zeno_getBatch", `[1,0]`, &b)
	l := list(t, srv, "zeno_getOutbox", `{"domainId":1,"toBatch":0}`)
	if len(l.Items) != 4 {
		t.Fatalf("zeno_getOutbox lists %d messages, want 4", len(l.Items))
	}
	listed := map[string]map[string]any{}
	for _, it := range l.Items {
		var m map[string]any
		if err := json.Unmarshal(it, &m); err != nil {
			t.Fatal(err)
		}
		delete(m, "batchId")
		delete(m, "verified")
		listed[m["outboxId"].(string)] = m
	}

	root, err := types.HexToHash(b.OutboxRoot)
	if err != nil {
		t.Fatal(err)
	}
	for i, id := range batch0Outbox() {
		p := outboxProof(t, srv, id)
		if p.DomainID != 1 || p.BatchID != 0 || !p.Verified || p.Tag != "latest" || p.OutboxID != id.String() || p.OutboxRoot != b.OutboxRoot || p.ProcessedOutbox {
			t.Fatalf("message %d: %+v", i, p)
		}
		msg, err := hex.DecodeString(p.Message)
		if err != nil {
			t.Fatal(err)
		}
		proof, err := hex.DecodeString(p.InclusionProof)
		if err != nil {
			t.Fatal(err)
		}
		if err := codec.VerifyMerkleProof(root, msg, proof); err != nil {
			t.Fatalf("message %d: proof does not verify: %v", i, err)
		}
		if err := codec.VerifyMerkleProof(root, append([]byte{0}, msg...), proof); err == nil {
			t.Fatalf("message %d: the proof verifies another element", i)
		}
		m, err := codec.DecodeOutboxMessage(msg)
		if err != nil {
			t.Fatal(err)
		}
		if enc, err := m.Encode(); err != nil || !bytes.Equal(enc, msg) {
			t.Fatalf("message %d is not canonical: %v", i, err)
		}
		if !reflect.DeepEqual(p.Decoded, listed[id.String()]) {
			t.Fatalf("message %d: decoded %v, zeno_getOutbox item %v", i, p.Decoded, listed[id.String()])
		}
	}
}

func TestOutboxProcessed(t *testing.T) {
	t.Run("read for verified batches only", func(t *testing.T) {
		_, cs, s, r, srv, _ := listRig(t, historyO())
		s.accept(cs[0], 20)
		doPoll(t, r)
		ids := batch0Outbox()
		s.mu.Lock()
		s.processed = map[types.Hash]bool{ids[0]: true}
		s.mu.Unlock()
		if p := outboxProof(t, srv, ids[0]); !p.ProcessedOutbox {
			t.Fatalf("registered message: %+v", p)
		}
		if p := outboxProof(t, srv, ids[1]); p.ProcessedOutbox {
			t.Fatalf("unregistered message: %+v", p)
		}
		s.mu.Lock()
		calls := s.processedCalls
		s.mu.Unlock()
		if calls != 2 {
			t.Fatalf("processedOutbox reads = %d, want 2", calls)
		}
		p := outboxProof(t, srv, codec.OutboxID(1, 1, 3, 0))
		if p.BatchID != 1 || p.Verified || p.Tag != "pending" || p.ProcessedOutbox {
			t.Fatalf("pending message: %+v", p)
		}
		s.mu.Lock()
		calls = s.processedCalls
		s.mu.Unlock()
		if calls != 2 {
			t.Fatalf("processedOutbox reads after a pending proof = %d, want 2", calls)
		}
	})
	t.Run("unavailable before the first poll", func(t *testing.T) {
		exec := t.TempDir()
		cs := buildHistory(t, exec, nil, historyO())
		storage := t.TempDir()
		s := newFakeSettlement()
		s.height = 30
		s.accept(cs[0], 20)
		s.accept(cs[1], 40)
		a, _ := openTestChainWith(t, storage, exec, s, &fakeSenders{})
		if err := a.Poll(context.Background()); err != nil {
			t.Fatalf("Poll: %v", err)
		}
		requireTags(t, a.Head(), 1, 1, 0, StatusOK)
		closeChain(t, a)

		b, _ := openTestChainWith(t, storage, exec, s, &fakeSenders{})
		srv, _, _ := newTestServer(t, nil, b)
		requireRPCErr(t, srv, "zeno_getOutboxProof", outboxParam(batch0Outbox()[0]), -32002, "processedOutbox is not available until the gateway's first poll reads Settlement; retry later")
		s.mu.Lock()
		calls := s.processedCalls
		s.mu.Unlock()
		if calls != 0 {
			t.Fatalf("processedOutbox reads = %d, want 0", calls)
		}
	})
}

func TestOutboxProofErrors(t *testing.T) {
	_, cs, s, r, srv, _ := listRig(t, historyO())
	s.accept(cs[0], 20)
	s.accept(cs[1], 21)
	doPoll(t, r)

	unknown := types.Hash{0xAA}
	requireRPCErr(t, srv, "zeno_getOutboxProof", outboxParam(unknown), -32004, "outbox message "+unknown.String()+" not found")
	const badID = "invalid params: outboxId must be 64 hex characters"
	requireRPCErr(t, srv, "zeno_getOutboxProof", `[null]`, -32602, badID)
	requireRPCErr(t, srv, "zeno_getOutboxProof", `["abcd"]`, -32602, badID)
	requireRPCErr(t, srv, "zeno_getOutboxProof", `[]`, -32602, "")

	var reqs []string
	for i := 1; i <= maxBodyOutboxProofs+1; i++ {
		reqs = append(reqs, fmt.Sprintf(`{"jsonrpc":"2.0","method":"zeno_getOutboxProof","params":["%s"],"id":%d}`, unknown, i))
	}
	rec := post(t, srv, "["+strings.Join(reqs, ",")+"]")
	var rs []rpcResp
	if err := json.Unmarshal(rec.Body.Bytes(), &rs); err != nil || len(rs) != maxBodyOutboxProofs+1 {
		t.Fatalf("batch response: %v (%d items)", err, len(rs))
	}
	for i, resp := range rs[:maxBodyOutboxProofs] {
		if resp.Error == nil || resp.Error.Code != -32004 {
			t.Fatalf("request %d: %+v, want -32004", i+1, resp.Error)
		}
	}
	if e := rs[maxBodyOutboxProofs].Error; e == nil || e.Code != -32600 || e.Message != outboxBudgetMsg {
		t.Fatalf("request %d: %+v, want -32600 %q", maxBodyOutboxProofs+1, e, outboxBudgetMsg)
	}
	requireRPCErr(t, srv, "zeno_getOutboxProof", outboxParam(unknown), -32004, "")

	s.edit(1, func(be *definition.BatchEntry) { be.PostStateRoot = types.Hash{0xAB} })
	s.height = 21
	requireTags(t, doPoll(t, r), 1, 1, 0, StatusInconsistent)
	requireRPCErr(t, srv, "zeno_getOutboxProof", outboxParam(codec.OutboxID(1, 1, 3, 0)), -32003, "the domain is INCONSISTENT from batch 1")
	if p := outboxProof(t, srv, batch0Outbox()[0]); !p.Verified || p.BatchID != 0 {
		t.Fatalf("batch 0 below the inconsistency: %+v", p)
	}
}

// waitFor polls cond every 5 ms until it holds or 5 s pass.
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("%s did not happen within 5 s", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestOutboxSettlementFailure(t *testing.T) {
	_, cs, s, r, srv, sink := listRig(t, historyO())
	s.accept(cs[0], 20)
	doPoll(t, r)
	ids := batch0Outbox()
	calls := func() int {
		s.mu.Lock()
		defer s.mu.Unlock()
		return s.processedCalls
	}

	s.mu.Lock()
	s.processedErr = errors.New(`Post "http://user:secret@node.example:1/rpc": connection refused`)
	s.mu.Unlock()
	requireRPCErr(t, srv, "zeno_getOutboxProof", outboxParam(ids[0]), -32603, "internal error")
	if n := sink.count("JSON-RPC zeno_getOutboxProof: internal error", "read processedOutbox"); n != 1 {
		t.Fatalf("log lines = %d, want 1:\n%s", n, sink.dump())
	}
	if strings.Contains(sink.dump(), "secret") {
		t.Fatalf("the log shows the endpoint's userinfo:\n%s", sink.dump())
	}

	hang := make(chan struct{})
	s.mu.Lock()
	s.processedErr = nil
	s.processedHang = hang
	s.mu.Unlock()
	srv.outboxReadTimeout = 100 * time.Millisecond
	base := calls()

	// One body of three proofs shares one deadline: the first read
	// times out and the others answer without a node call.
	var reqs []string
	for i := 0; i < 3; i++ {
		reqs = append(reqs, fmt.Sprintf(`{"jsonrpc":"2.0","method":"zeno_getOutboxProof","params":["%s"],"id":%d}`, ids[i], i+1))
	}
	start := time.Now()
	rec := post(t, srv, "["+strings.Join(reqs, ",")+"]")
	if d := time.Since(start); d > 300*time.Millisecond {
		t.Fatalf("the body took %s, want under 300ms", d)
	}
	var rs []rpcResp
	if err := json.Unmarshal(rec.Body.Bytes(), &rs); err != nil || len(rs) != 3 {
		t.Fatalf("batch response: %v (%d items)", err, len(rs))
	}
	for i, resp := range rs {
		if resp.Error == nil || resp.Error.Code != -32603 {
			t.Fatalf("request %d: %+v, want -32603", i+1, resp.Error)
		}
	}
	if n := calls() - base; n != 1 {
		t.Fatalf("node calls = %d, want 1", n)
	}
	if n := len(srv.outboxSem); n != 1 {
		t.Fatalf("held outbox slots = %d, want 1: the abandoned read still counts", n)
	}

	// Three more abandoned reads fill the cap; a fourth is refused.
	for i := 1; i <= 3; i++ {
		requireRPCErr(t, srv, "zeno_getOutboxProof", outboxParam(ids[i]), -32603, "internal error")
	}
	if n, held := calls()-base, len(srv.outboxSem); n != 4 || held != maxOutboxReads {
		t.Fatalf("node calls = %d, held slots = %d, want 4 and %d", n, held, maxOutboxReads)
	}
	requireRPCErr(t, srv, "zeno_getOutboxProof", outboxParam(ids[0]), -32002, outboxBusyMsg)
	if n := calls() - base; n != 4 {
		t.Fatalf("node calls after the refused read = %d, want 4", n)
	}

	close(hang)
	waitFor(t, "the abandoned reads returning", func() bool { return len(srv.outboxSem) == 0 })
	if p := outboxProof(t, srv, ids[0]); !p.Verified {
		t.Fatalf("proof after the hang: %+v", p)
	}

	// A client that hangs up frees its request slot at once, and the
	// hang-up is not logged.
	hang2 := make(chan struct{})
	s.mu.Lock()
	s.processedHang = hang2
	s.mu.Unlock()
	srv.outboxReadTimeout = 5 * time.Second
	_, addr := serveHTTP(t, srv, nil)
	logged := sink.count("getOutboxProof")
	before := calls()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() {
		defer close(done)
		body := fmt.Sprintf(`{"jsonrpc":"2.0","method":"zeno_getOutboxProof","params":["%s"],"id":1}`, ids[0])
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://"+addr+"/", strings.NewReader(body))
		if err != nil {
			return
		}
		req.Header.Set("Content-Type", "application/json")
		if resp, err := http.DefaultClient.Do(req); err == nil {
			_ = resp.Body.Close()
		}
	}()
	waitFor(t, "the node call starting", func() bool { return calls() > before })
	cancel()
	<-done
	waitFor(t, "the request slot freeing", func() bool { return len(srv.sem) == 0 })
	if n := len(srv.outboxSem); n != 1 {
		t.Fatalf("held outbox slots = %d, want 1 until the node answers", n)
	}
	time.Sleep(50 * time.Millisecond)
	if n := sink.count("getOutboxProof"); n != logged {
		t.Fatalf("the hang-up was logged:\n%s", sink.dump())
	}
	close(hang2)
	waitFor(t, "the read returning", func() bool { return len(srv.outboxSem) == 0 })
}

func TestOutboxIndexDamage(t *testing.T) {
	_, cs, s, r, srv, _ := listRig(t, historyO())
	s.accept(cs[0], 20)
	doPoll(t, r)
	if err := r.idx.db.Delete(ikey(pfxOutbox, be64(2), be32(0)), nil); err != nil {
		t.Fatal(err)
	}
	ids := batch0Outbox()
	// The deleted message is missing; the others no longer prove
	// against the batch's outboxRoot.
	requireRPCErr(t, srv, "zeno_getOutboxProof", outboxParam(ids[2]), -32603, "internal error")
	requireRPCErr(t, srv, "zeno_getOutboxProof", outboxParam(ids[0]), -32603, "internal error")
}
