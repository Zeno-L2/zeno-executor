package gateway

import (
	"github.com/zenon-network/go-zenon/common/types"

	"encoding/json"
	"fmt"
	"os"
	"testing"
)

type sdkFixtureCall struct {
	Name     string          `json:"name"`
	Method   string          `json:"method"`
	Params   json.RawMessage `json:"params"`
	Response json.RawMessage `json:"response"`
}

// TestDumpGatewaySdkFixture records real gateway responses for the
// TypeScript client. It runs only when GATEWAY_SDK_FIXTURE_OUT names an
// output file. Each entry holds the request and the response body the
// server wrote, so the client tests replay exact wire bytes.
func TestDumpGatewaySdkFixture(t *testing.T) {
	out := os.Getenv("GATEWAY_SDK_FIXTURE_OUT")
	if out == "" {
		t.Skip("GATEWAY_SDK_FIXTURE_OUT not set")
	}

	var calls []sdkFixtureCall
	record := func(srv *Server, name, method, params string) json.RawMessage {
		t.Helper()
		rec := post(t, srv, fmt.Sprintf(`{"jsonrpc":"2.0","method":%q,"params":%s,"id":1}`, method, params))
		if rec.Code != 200 {
			t.Fatalf("%s: status %d", name, rec.Code)
		}
		body := json.RawMessage(append([]byte(nil), rec.Body.Bytes()...))
		calls = append(calls, sdkFixtureCall{Name: name, Method: method, Params: json.RawMessage(params), Response: body})
		return body
	}
	result := func(body json.RawMessage, into any) {
		t.Helper()
		var r struct {
			Result json.RawMessage `json:"result"`
		}
		if err := json.Unmarshal(body, &r); err != nil || r.Result == nil {
			t.Fatalf("no result in %s", body)
		}
		if err := json.Unmarshal(r.Result, into); err != nil {
			t.Fatal(err)
		}
	}

	// A chain built by the real WASM plugin: deploys, an upgrade and
	// ownership changes, served with the WASM reader.
	w := buildWasmChain(t)
	srv := w.srv
	record(srv, "chainInfo", "zeno_chainInfo", `[1]`)
	record(srv, "getHead", "zeno_getHead", `[1]`)
	record(srv, "getBatch latest", "zeno_getBatch", `[1]`)
	record(srv, "getBatch by id", "zeno_getBatch", `[1,0]`)
	record(srv, "getReceipt success", "zeno_getReceipt", fmt.Sprintf(`["%s"]`, hashOfIndex(4)))
	record(srv, "getReceipt unauthorized", "zeno_getReceipt", fmt.Sprintf(`["%s"]`, hashOfIndex(11)))
	record(srv, "getInputs by contract", "zeno_getInputs", fmt.Sprintf(`[{"domainId":1,"contractId":"%s"}]`, w.c))
	record(srv, "getEvents", "zeno_getEvents", `[{"domainId":1}]`)
	record(srv, "getOutbox empty", "zeno_getOutbox", `[{"domainId":1}]`)
	record(srv, "getBundle", "zeno_getBundle", fmt.Sprintf(`["%s"]`, w.cs[0].DAHash))
	record(srv, "getReceiptSidecar", "zeno_getReceiptSidecar", fmt.Sprintf(`["%s"]`, w.cs[0].DAHash))

	contract := record(srv, "read contract", "zeno_read", fmt.Sprintf(`[1,"contract",{"contractId":"%s"}]`, w.c))
	record(srv, "read contract at batch 0", "zeno_read", fmt.Sprintf(`[1,"contract",{"contractId":"%s"},0]`, w.c))
	record(srv, "read contract absent", "zeno_read", `[1,"contract",{"contractId":"`+types0().String()+`"}]`)
	record(srv, "read storage", "zeno_read", fmt.Sprintf(`[1,"storage",{"contractId":"%s","key":"00"}]`, w.c))
	record(srv, "read contractsByDeployer", "zeno_read", fmt.Sprintf(`[1,"contractsByDeployer",{"address":"%s"}]`, w.d))
	record(srv, "read module", "zeno_read", fmt.Sprintf(`[1,"module",{"sourceCodeHash":"%s"}]`, w.codeA))

	var read readJSON
	result(contract, &read)
	if len(read.Proofs) == 0 {
		t.Fatal("the contract read carries no proofs")
	}
	path := read.Proofs[0].Path
	record(srv, "getStateAt", "zeno_getStateAt", fmt.Sprintf(`[1,"%s"]`, path))
	record(srv, "getProof", "zeno_getProof", fmt.Sprintf(`[1,["%s"]]`, path))

	record(srv, "error unknown domain", "zeno_getHead", `[99]`)
	record(srv, "error receipt not found", "zeno_getReceipt", fmt.Sprintf(`["%s"]`, hashOfIndex(200)))
	record(srv, "error unknown view", "zeno_read", `[1,"nope",{}]`)
	record(srv, "error invalid params", "zeno_getHead", `[]`)
	record(srv, "error method not found", "zeno_nope", `[]`)
	record(srv, "error simulation disabled", "zeno_simulate", `[1,{}]`)
	record(srv, "error tag unavailable", "zeno_getBatch", `[1,"finalized"]`)

	// A chain whose batches emit outbox messages.
	_, cs, s, r, osrv, _ := listRig(t, historyO())
	s.accept(cs[0], 20)
	s.accept(cs[1], 21)
	doPoll(t, r)
	record(osrv, "outbox getBatch", "zeno_getBatch", `[1,0]`)
	record(osrv, "outbox getOutbox", "zeno_getOutbox", `[{"domainId":1,"toBatch":0}]`)
	for i, id := range batch0Outbox() {
		record(osrv, fmt.Sprintf("outbox getOutboxProof %d", i), "zeno_getOutboxProof", outboxParam(id))
	}
	record(osrv, "error outbox not found", "zeno_getOutboxProof", `["`+types0().String()+`"]`)

	raw, err := json.MarshalIndent(map[string]any{"calls": calls}, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(out, append(raw, '\n'), 0o644); err != nil {
		t.Fatal(err)
	}
}

// types0 is the zero hash.
func types0() (h types.Hash) { return }
