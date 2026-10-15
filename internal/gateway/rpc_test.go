package gateway

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

const (
	parseErrBody = `{"jsonrpc":"2.0","id":null,"error":{"code":-32700,"message":"parse error"}}`
	invalidReq   = `{"jsonrpc":"2.0","id":null,"error":{"code":-32600,"message":"invalid request"}}`
)

// testMethods are the methods newTestServer installs.
type testMethods struct {
	count    atomic.Int64
	big      atomic.Int64
	fillLeft atomic.Int64
	mu       sync.Mutex
	failWith error
}

// bigResult is the 9 MiB JSON string test_big returns.
var bigResult = func() json.RawMessage {
	b := make([]byte, 9<<20)
	for i := range b {
		b[i] = 'b'
	}
	b[0], b[len(b)-1] = '"', '"'
	return b
}()

// stringOf returns a JSON string of exactly n bytes.
func stringOf(n int) json.RawMessage {
	b := make([]byte, n)
	for i := range b {
		b[i] = 'a'
	}
	b[0], b[n-1] = '"', '"'
	return b
}

// newTestServer returns a server for chains with the test methods
// installed.
func newTestServer(t *testing.T, origins []string, chains ...*Chain) (*Server, *logSink, *testMethods) {
	t.Helper()
	sink := &logSink{}
	s := NewServer(Config{NetworkName: "test", ZnndEndpoint: "http://user:secret@node.example:1/rpc", AllowedOrigins: origins}, chains, nil, sink.logf)
	tm := &testMethods{}
	s.methods["test_echo"] = func(_ *Server, p []json.RawMessage, _ *budget) (any, error) { return p, nil }
	s.methods["test_count"] = func(*Server, []json.RawMessage, *budget) (any, error) {
		tm.count.Add(1)
		return true, nil
	}
	s.methods["test_fill"] = func(_ *Server, _ []json.RawMessage, b *budget) (any, error) {
		tm.fillLeft.Store(int64(b.left))
		return stringOf(b.left), nil
	}
	s.methods["test_over"] = func(_ *Server, _ []json.RawMessage, b *budget) (any, error) {
		return stringOf(b.left + 1), nil
	}
	s.methods["test_big"] = func(*Server, []json.RawMessage, *budget) (any, error) {
		tm.big.Add(1)
		return bigResult, nil
	}
	s.methods["test_fail"] = func(*Server, []json.RawMessage, *budget) (any, error) {
		tm.mu.Lock()
		defer tm.mu.Unlock()
		return nil, tm.failWith
	}
	s.methods["test_panic"] = func(*Server, []json.RawMessage, *budget) (any, error) { panic("boom") }
	return s, sink, tm
}

// genesisChain returns a chain that has not polled.
func genesisChain(t *testing.T) *Chain {
	t.Helper()
	return newTestChain(t, t.TempDir(), newFakeSettlement()).c
}

// post sends POST / with a JSON content type plus the header pairs.
func post(t *testing.T, s *Server, body string, hdr ...string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	for i := 0; i+1 < len(hdr); i += 2 {
		req.Header.Add(hdr[i], hdr[i+1])
	}
	rec := httptest.NewRecorder()
	s.ServeHTTP(rec, req)
	return rec
}

// rpcResp is one decoded response.
type rpcResp struct {
	ID     json.RawMessage `json:"id"`
	Result json.RawMessage `json:"result"`
	Error  *rpcError       `json:"error"`
}

// callRPC posts a request for method with params and decodes the
// response.
func callRPC(t *testing.T, s *Server, method, params string) rpcResp {
	t.Helper()
	rec := post(t, s, fmt.Sprintf(`{"jsonrpc":"2.0","method":%q,"params":%s,"id":1}`, method, params))
	if rec.Code != 200 {
		t.Fatalf("%s %s: status %d", method, params, rec.Code)
	}
	var r rpcResp
	if err := json.Unmarshal(rec.Body.Bytes(), &r); err != nil {
		t.Fatalf("%s %s: %v: %s", method, params, err, rec.Body.String())
	}
	return r
}

func requireBody(t *testing.T, rec *httptest.ResponseRecorder, want string) {
	t.Helper()
	if got := rec.Body.String(); got != want {
		t.Fatalf("body = %s\nwant   %s", trunc(got), trunc(want))
	}
}

func trunc(s string) string {
	if len(s) > 300 {
		return s[:300] + "..."
	}
	return s
}

func okResp(id, result string) string {
	return `{"jsonrpc":"2.0","id":` + id + `,"result":` + result + `}`
}

func errResp(id string, code int, msg string) string {
	return fmt.Sprintf(`{"jsonrpc":"2.0","id":%s,"error":{"code":%d,"message":%q}}`, id, code, msg)
}

func TestRPCEnvelope(t *testing.T) {
	s, _, tm := newTestServer(t, nil, genesisChain(t))
	cases := []struct{ name, body, want string }{
		{"result", `{"jsonrpc":"2.0","method":"test_echo","params":[1],"id":7}`, okResp("7", "[1]")},
		{"string id", `{"jsonrpc":"2.0","method":"test_echo","params":[1],"id":"a"}`, okResp(`"a"`, "[1]")},
		{"null id", `{"jsonrpc":"2.0","method":"test_echo","params":[1],"id":null}`, okResp("null", "[1]")},
		{"fraction id", `{"jsonrpc":"2.0","method":"test_echo","params":[1],"id":1.5}`, okResp("1.5", "[1]")},
		{"batch with notification",
			`[{"jsonrpc":"2.0","method":"test_echo","id":1},{"jsonrpc":"2.0","method":"test_count"},{"jsonrpc":"2.0","method":"test_echo","id":2}]`,
			"[" + okResp("1", "null") + "," + okResp("2", "null") + "]"},
		{"not json", `nope`, parseErrBody},
		{"empty", ``, parseErrBody},
		{"trailing bytes", `{"jsonrpc":"2.0","method":"test_echo","id":1} x`, parseErrBody},
		{"scalar", `5`, invalidReq},
		{"empty batch", `[]`, invalidReq},
		{"batch of scalar", `[1]`, `[` + invalidReq + `]`},
		{"version", `{"jsonrpc":"1.0","method":"test_echo","id":1}`, errResp("1", -32600, "invalid request")},
		{"method type", `{"jsonrpc":"2.0","method":5,"id":"a"}`, errResp(`"a"`, -32600, "invalid request")},
		{"object id", `{"jsonrpc":"2.0","method":"test_echo","id":{}}`, invalidReq},
		{"bool id", `{"jsonrpc":"2.0","method":"test_echo","id":true}`, invalidReq},
		{"unknown method", `{"jsonrpc":"2.0","method":"nope","id":1}`, errResp("1", -32601, "method not found")},
		{"invalid notification", `{"jsonrpc":"1.0","method":"test_echo"}`, invalidReq},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			rec := post(t, s, c.body)
			if rec.Code != 200 {
				t.Fatalf("status = %d", rec.Code)
			}
			requireBody(t, rec, c.want)
			if ct := rec.Header().Get("Content-Type"); ct != "application/json" {
				t.Fatalf("Content-Type = %q", ct)
			}
			if rec.Header().Get("X-Content-Type-Options") != "nosniff" {
				t.Fatal("no nosniff header")
			}
		})
	}

	rec := post(t, s, `{"jsonrpc":"2.0","method":"test_count"}`)
	if rec.Code != http.StatusNoContent || rec.Body.Len() != 0 || rec.Header().Get("Content-Type") != "" {
		t.Fatalf("notification: status %d body %q Content-Type %q", rec.Code, rec.Body, rec.Header().Get("Content-Type"))
	}
	rec = post(t, s, `[{"jsonrpc":"2.0","method":"test_count"},{"jsonrpc":"2.0","method":"test_count"}]`)
	if rec.Code != http.StatusNoContent || rec.Body.Len() != 0 {
		t.Fatalf("notification batch: status %d body %q", rec.Code, rec.Body)
	}
	if tm.count.Load() != 0 {
		t.Fatalf("a notification ran test_count %d times", tm.count.Load())
	}
}

func TestRPCMembers(t *testing.T) {
	s, _, _ := newTestServer(t, nil, genesisChain(t))
	nums := func(n int) string {
		return "[" + strings.TrimSuffix(strings.Repeat("1,", n), ",") + "]"
	}
	cases := []struct{ name, body, want string }{
		{"object params", `{"jsonrpc":"2.0","method":"test_echo","params":{"a":1},"id":1}`, errResp("1", -32602, "invalid params: params must be an array")},
		{"null params", `{"jsonrpc":"2.0","method":"test_echo","params":null,"id":1}`, errResp("1", -32602, "invalid params: params must be an array")},
		{"16 params", `{"jsonrpc":"2.0","method":"test_echo","params":` + nums(16) + `,"id":1}`, okResp("1", nums(16))},
		{"17 params", `{"jsonrpc":"2.0","method":"test_echo","params":` + nums(17) + `,"id":1}`, errResp("1", -32602, "invalid params: more than 16 parameters")},
		{"last member wins", `{"jsonrpc":"2.0","method":"nope","method":"test_echo","params":[1],"id":1}`, okResp("1", "[1]")},
		{"member case", `{"jsonrpc":"2.0","Method":"test_echo","id":1}`, errResp("1", -32600, "invalid request")},
		{"unknown member", `{"jsonrpc":"2.0","method":"test_echo","x":[1,2,3],"params":[1],"id":1}`, okResp("1", "[1]")},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) { requireBody(t, post(t, s, c.body), c.want) })
	}
}

func TestRPCBatchLimit(t *testing.T) {
	s, _, tm := newTestServer(t, nil, genesisChain(t))
	req := `{"jsonrpc":"2.0","method":"test_count","id":1}`
	batch := func(n int) string {
		return "[" + strings.TrimSuffix(strings.Repeat(req+",", n), ",") + "]"
	}
	rec := post(t, s, batch(100))
	var rs []rpcResp
	if err := json.Unmarshal(rec.Body.Bytes(), &rs); err != nil || len(rs) != 100 {
		t.Fatalf("100 requests: %d responses, %v", len(rs), err)
	}
	requireBody(t, post(t, s, batch(101)), errResp("null", -32600, "batch exceeds 100 requests"))
	if tm.count.Load() != 100 {
		t.Fatalf("test_count ran %d times, want 100", tm.count.Load())
	}
}

func TestRPCBatchMemoryBound(t *testing.T) {
	s, _, _ := newTestServer(t, nil, genesisChain(t))
	elem := `{"id":1}`
	n := (maxRequestBytes - 2 + 1) / (len(elem) + 1)
	batch := "[" + strings.TrimSuffix(strings.Repeat(elem+",", n), ",") + "]"
	if len(batch) > maxRequestBytes || n < 100000 {
		t.Fatalf("batch body: %d bytes, %d elements", len(batch), n)
	}
	requireBody(t, post(t, s, batch), errResp("null", -32600, "batch exceeds 100 requests"))
	if a := testing.AllocsPerRun(3, func() { _, _ = s.handle(context.Background(), nil, []byte(batch), io.Discard) }); a >= 2000 {
		t.Fatalf("a batch of %d elements took %v allocations", n, a)
	}

	head, tail := `{"jsonrpc":"2.0","method":"test_echo","params":[`, `],"id":1}`
	m := (maxRequestBytes - len(head) - len(tail) + 1) / 2
	params := head + strings.TrimSuffix(strings.Repeat("1,", m), ",") + tail
	if len(params) > maxRequestBytes || m < 100000 {
		t.Fatalf("params body: %d bytes, %d params", len(params), m)
	}
	requireBody(t, post(t, s, params), errResp("1", -32602, "invalid params: more than 16 parameters"))
	if a := testing.AllocsPerRun(3, func() { _, _ = s.handle(context.Background(), nil, []byte(params), io.Discard) }); a >= 2000 {
		t.Fatalf("%d parameters took %v allocations", m, a)
	}
}

func TestRPCInternalErrors(t *testing.T) {
	s, sink, tm := newTestServer(t, nil, genesisChain(t))
	tm.failWith = errors.New("dial http://user:secret@node.example:1/rpc: refused")
	want := errResp("1", -32603, "internal error")
	requireBody(t, post(t, s, `{"jsonrpc":"2.0","method":"test_fail","id":1}`), want)
	if sink.count("JSON-RPC test_fail: internal error", "http://node.example:1") != 1 || sink.count("secret") != 0 {
		t.Fatalf("log:\n%s", sink.dump())
	}
	requireBody(t, post(t, s, `{"jsonrpc":"2.0","method":"test_fail","id":1}`), want)
	if sink.count("test_fail") != 1 {
		t.Fatalf("a repeated error logged again:\n%s", sink.dump())
	}
	tm.mu.Lock()
	tm.failWith = errors.New("another failure")
	tm.mu.Unlock()
	post(t, s, `{"jsonrpc":"2.0","method":"test_fail","id":1}`)
	if sink.count("test_fail") != 2 {
		t.Fatalf("a changed error did not log:\n%s", sink.dump())
	}

	requireBody(t, post(t, s, `{"jsonrpc":"2.0","method":"test_panic","id":1}`), want)
	if sink.count("the method panicked: boom", "rpc_test.go:") != 1 {
		t.Fatalf("panic log:\n%s", sink.dump())
	}
}

func TestRPCRequestCap(t *testing.T) {
	s, _, tm := newTestServer(t, nil, genesisChain(t))
	req := `{"jsonrpc":"2.0","method":"test_count","id":1}`
	pad := func(n int) string { return req + strings.Repeat(" ", n-len(req)) }

	if rec := post(t, s, pad(maxRequestBytes)); rec.Code != 200 || tm.count.Load() != 1 {
		t.Fatalf("exact cap: status %d, %d calls", rec.Code, tm.count.Load())
	}
	if rec := post(t, s, pad(maxRequestBytes+1)); rec.Code != http.StatusRequestEntityTooLarge || tm.count.Load() != 1 {
		t.Fatalf("one byte over: status %d, %d calls", rec.Code, tm.count.Load())
	}
	chunked := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(pad(maxRequestBytes+1)))
	chunked.Header.Set("Content-Type", "application/json")
	chunked.ContentLength = -1
	rec := httptest.NewRecorder()
	s.ServeHTTP(rec, chunked)
	if rec.Code != http.StatusRequestEntityTooLarge || tm.count.Load() != 1 {
		t.Fatalf("chunked over the cap: status %d, %d calls", rec.Code, tm.count.Load())
	}

	for i := 0; i < cap(s.sem); i++ {
		s.sem <- struct{}{}
	}
	body := &readRecorder{}
	big := httptest.NewRequest(http.MethodPost, "/", body)
	big.Header.Set("Content-Type", "application/json")
	big.ContentLength = 100 << 20
	rec = httptest.NewRecorder()
	s.ServeHTTP(rec, big)
	if rec.Code != http.StatusRequestEntityTooLarge || body.reads.Load() != 0 {
		t.Fatalf("declared length: status %d, %d reads", rec.Code, body.reads.Load())
	}
}

// readRecorder counts the reads of a body.
type readRecorder struct{ reads atomic.Int64 }

func (r *readRecorder) Read([]byte) (int, error) {
	r.reads.Add(1)
	return 0, io.EOF
}

func TestRPCResponseCap(t *testing.T) {
	s, _, tm := newTestServer(t, nil, genesisChain(t))

	t.Run("exact fit alone", func(t *testing.T) {
		rec := post(t, s, `{"jsonrpc":"2.0","method":"test_fill","id":1}`)
		prefix := `{"jsonrpc":"2.0","id":1,"result":`
		if got := tm.fillLeft.Load(); got != int64(maxResponseBytes-len(prefix)-1) {
			t.Fatalf("left = %d", got)
		}
		if rec.Body.Len() != maxResponseBytes {
			t.Fatalf("body = %d bytes, want %d", rec.Body.Len(), maxResponseBytes)
		}
	})

	t.Run("batch after a full response", func(t *testing.T) {
		rec := post(t, s, `[{"jsonrpc":"2.0","method":"test_fill","id":1},{"jsonrpc":"2.0","method":"test_count","id":2}]`)
		overflow := errResp("2", -32600, "batch response exceeds 16 MiB; send this request alone")
		if rec.Body.Len() != maxResponseBytes+len(overflow)+1 || !strings.HasSuffix(rec.Body.String(), ","+overflow+"]") {
			t.Fatalf("body = %d bytes, want %d", rec.Body.Len(), maxResponseBytes+len(overflow)+1)
		}
		if tm.count.Load() != 0 {
			t.Fatal("test_count ran with no room left")
		}
	})

	t.Run("batch overflow in the middle", func(t *testing.T) {
		rec := post(t, s, `[{"jsonrpc":"2.0","method":"test_big","id":1},{"jsonrpc":"2.0","method":"test_big","id":2},{"jsonrpc":"2.0","method":"test_echo","id":3}]`)
		var rs []rpcResp
		if err := json.Unmarshal(rec.Body.Bytes(), &rs); err != nil || len(rs) != 3 {
			t.Fatalf("%d responses, %v", len(rs), err)
		}
		if rs[0].Error != nil || rs[2].Error != nil || rs[1].Error == nil || rs[1].Error.Code != -32600 {
			t.Fatalf("responses: %+v %+v %+v", rs[0].Error, rs[1].Error, rs[2].Error)
		}
		if tm.big.Load() != 2 {
			t.Fatalf("test_big ran %d times, want 2", tm.big.Load())
		}
	})

	t.Run("lone overflow", func(t *testing.T) {
		requireBody(t, post(t, s, `{"jsonrpc":"2.0","method":"test_over","id":1}`), errResp("1", -32600, "response exceeds 16 MiB"))
	})
}

func TestRPCInFlightCap(t *testing.T) {
	s, _, tm := newTestServer(t, nil, genesisChain(t))
	for i := 0; i < cap(s.sem); i++ {
		s.sem <- struct{}{}
	}
	if rec := post(t, s, `{"jsonrpc":"2.0","method":"test_count","id":1}`); rec.Code != http.StatusServiceUnavailable || tm.count.Load() != 0 {
		t.Fatalf("full: status %d, %d calls", rec.Code, tm.count.Load())
	}
	<-s.sem
	if rec := post(t, s, `{"jsonrpc":"2.0","method":"test_count","id":1}`); rec.Code != 200 {
		t.Fatalf("one free: status %d", rec.Code)
	}
}

func TestNewHTTPServerTimeouts(t *testing.T) {
	s, _, _ := newTestServer(t, nil)
	hs := NewHTTPServer(s)
	if hs.ReadHeaderTimeout != 5*time.Second || hs.ReadTimeout != 30*time.Second || hs.WriteTimeout != 30*time.Second ||
		hs.IdleTimeout != 120*time.Second || hs.Handler != http.Handler(s) {
		t.Fatalf("server = %+v", hs)
	}
	if s.bodyReadTimeout != 5*time.Second {
		t.Fatalf("bodyReadTimeout = %s", s.bodyReadTimeout)
	}
}

func TestRPCHeaderCap(t *testing.T) {
	s, _, _ := newTestServer(t, nil, genesisChain(t))
	ts := httptest.NewUnstartedServer(s)
	ts.Config = NewHTTPServer(s)
	ts.Start()
	t.Cleanup(ts.Close)

	const reqBody = `{"jsonrpc":"2.0","method":"test_count","id":1}`
	do := func(t *testing.T, pad int) int {
		t.Helper()
		req, err := http.NewRequest("POST", ts.URL, strings.NewReader(reqBody))
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("Content-Type", "application/json")
		if pad > 0 {
			req.Header.Set("X-Pad", strings.Repeat("a", pad))
		}
		req.Close = true
		resp, err := ts.Client().Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		_, _ = io.Copy(io.Discard, resp.Body)
		return resp.StatusCode
	}

	if code := do(t, 0); code != 200 {
		t.Fatalf("normal headers: status %d, want 200", code)
	}
	if code := do(t, 32<<10); code != http.StatusRequestHeaderFieldsTooLarge {
		t.Fatalf("32 KiB of headers: status %d, want 431", code)
	}
	if len(s.sem) != 0 {
		t.Fatalf("%d slots held after the oversized request", len(s.sem))
	}
	if code := do(t, 0); code != 200 {
		t.Fatalf("request after the oversized one: status %d, want 200", code)
	}
}

func TestRPCBodyReadDeadline(t *testing.T) {
	s, _, tm := newTestServer(t, nil, genesisChain(t))
	s.bodyReadTimeout = 500 * time.Millisecond
	ts := httptest.NewUnstartedServer(s)
	ts.Config = NewHTTPServer(s)
	ts.Start()
	t.Cleanup(ts.Close)

	dial := func(t *testing.T) (*bufio.Reader, func(string)) {
		t.Helper()
		conn, err := net.Dial("tcp", ts.Listener.Addr().String())
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = conn.Close() })
		_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
		return bufio.NewReader(conn), func(m string) {
			if _, err := io.WriteString(conn, m); err != nil {
				t.Fatal(err)
			}
		}
	}
	const reqBody = `{"jsonrpc":"2.0","method":"test_count","id":1}`
	full := "POST / HTTP/1.1\r\nHost: gw\r\nContent-Type: application/json\r\nContent-Length: " + fmt.Sprint(len(reqBody)) + "\r\n\r\n" + reqBody

	t.Run("stalled client", func(t *testing.T) {
		for i := 0; i < cap(s.sem)-1; i++ {
			s.sem <- struct{}{}
		}
		defer func() {
			for i := 0; i < cap(s.sem)-1; i++ {
				<-s.sem
			}
		}()
		t0 := time.Now()
		br, send := dial(t)
		send("POST / HTTP/1.1\r\nHost: gw\r\nContent-Type: application/json\r\nContent-Length: 64\r\n\r\n")
		resp, err := http.ReadResponse(br, nil)
		if err != nil {
			t.Fatal(err)
		}
		_ = resp.Body.Close()
		if resp.StatusCode != http.StatusRequestTimeout || !resp.Close {
			t.Fatalf("status %d close %v, want 408 with Connection: close", resp.StatusCode, resp.Close)
		}
		if d := time.Since(t0); d < 400*time.Millisecond {
			t.Fatalf("408 after %s, want at least 400ms", d)
		}
		deadline := time.Now().Add(2 * time.Second)
		for len(s.sem) != cap(s.sem)-1 {
			if time.Now().After(deadline) {
				t.Fatalf("slot not returned: %d held", len(s.sem))
			}
			time.Sleep(10 * time.Millisecond)
		}
		before := tm.count.Load()
		res, err := ts.Client().Post(ts.URL, "application/json", strings.NewReader(reqBody))
		if err != nil {
			t.Fatal(err)
		}
		_ = res.Body.Close()
		if res.StatusCode != 200 || tm.count.Load() != before+1 {
			t.Fatalf("status %d, %d calls", res.StatusCode, tm.count.Load()-before)
		}
	})

	t.Run("idle keep-alive", func(t *testing.T) {
		before := tm.count.Load()
		br, send := dial(t)
		for i := 0; i < 2; i++ {
			send(full)
			resp, err := http.ReadResponse(br, nil)
			if err != nil {
				t.Fatalf("request %d: %v", i, err)
			}
			_, _ = io.Copy(io.Discard, resp.Body)
			_ = resp.Body.Close()
			if resp.StatusCode != 200 || resp.Close {
				t.Fatalf("request %d: status %d close %v", i, resp.StatusCode, resp.Close)
			}
			if i == 0 {
				time.Sleep(3 * s.bodyReadTimeout)
			}
		}
		if tm.count.Load() != before+2 {
			t.Fatalf("%d calls, want 2", tm.count.Load()-before)
		}
	})
}

func TestRPCOrigins(t *testing.T) {
	s, _, tm := newTestServer(t, []string{"https://app.example"}, genesisChain(t))
	const call = `{"jsonrpc":"2.0","method":"test_count","id":1}`

	rec := post(t, s, call)
	if rec.Code != 200 || rec.Header().Get("Access-Control-Allow-Origin") != "" || rec.Header().Get("Vary") != "Origin" {
		t.Fatalf("no Origin: status %d headers %v", rec.Code, rec.Header())
	}
	rec = post(t, s, call, "Origin", "https://app.example")
	if rec.Code != 200 || rec.Header().Get("Access-Control-Allow-Origin") != "https://app.example" {
		t.Fatalf("allowed Origin: status %d headers %v", rec.Code, rec.Header())
	}
	calls := tm.count.Load()
	for name, hdr := range map[string][]string{
		"other":  {"Origin", "https://evil.example"},
		"null":   {"Origin", "null"},
		"empty":  {"Origin", ""},
		"two":    {"Origin", "https://app.example", "Origin", "https://app.example"},
		"case":   {"Origin", "HTTPS://APP.EXAMPLE"},
		"prefix": {"Origin", "https://app.example.evil.test"},
		"slash":  {"Origin", "https://app.example/"},
	} {
		rec := post(t, s, call, hdr...)
		if rec.Code != http.StatusForbidden || rec.Header().Get("Access-Control-Allow-Origin") != "" {
			t.Fatalf("%s: status %d headers %v", name, rec.Code, rec.Header())
		}
	}
	if tm.count.Load() != calls {
		t.Fatal("a refused Origin ran a method")
	}

	options := func(s *Server, origin string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(http.MethodOptions, "/", nil)
		if origin != "" {
			req.Header.Set("Origin", origin)
		}
		rec := httptest.NewRecorder()
		s.ServeHTTP(rec, req)
		return rec
	}
	rec = options(s, "https://app.example")
	h := rec.Header()
	if rec.Code != http.StatusNoContent || h.Get("Access-Control-Allow-Origin") != "https://app.example" ||
		h.Get("Access-Control-Allow-Methods") != "POST, OPTIONS" || h.Get("Access-Control-Allow-Headers") != "Content-Type" {
		t.Fatalf("preflight: status %d headers %v", rec.Code, h)
	}
	if rec = options(s, "https://evil.example"); rec.Code != http.StatusForbidden {
		t.Fatalf("preflight from a refused Origin: status %d", rec.Code)
	}
	if rec = options(s, ""); rec.Code != http.StatusNoContent || rec.Header().Get("Allow") != "POST, OPTIONS" {
		t.Fatalf("preflight with no Origin: status %d headers %v", rec.Code, rec.Header())
	}

	none, _, _ := newTestServer(t, nil, genesisChain(t))
	if rec := post(t, none, call, "Origin", "https://app.example"); rec.Code != http.StatusForbidden {
		t.Fatalf("no allowed origins: status %d", rec.Code)
	}
}

func TestRPCRoutes(t *testing.T) {
	s, _, _ := newTestServer(t, nil, genesisChain(t))
	const call = `{"jsonrpc":"2.0","method":"test_count","id":1}`
	do := func(method, path, ct string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(method, path, strings.NewReader(call))
		if ct != "" {
			req.Header.Set("Content-Type", ct)
		}
		rec := httptest.NewRecorder()
		s.ServeHTTP(rec, req)
		return rec
	}
	if rec := do(http.MethodGet, "/", ""); rec.Code != http.StatusMethodNotAllowed || rec.Header().Get("Allow") != "POST, OPTIONS" {
		t.Fatalf("GET /: status %d headers %v", rec.Code, rec.Header())
	}
	for _, c := range []struct{ method, path string }{{"POST", "/x"}, {"GET", "/bundles/00"}} {
		if rec := do(c.method, c.path, "application/json"); rec.Code != http.StatusNotFound {
			t.Fatalf("%s %s: status %d", c.method, c.path, rec.Code)
		}
	}
	for _, ct := range []string{"", "text/plain"} {
		if rec := do(http.MethodPost, "/", ct); rec.Code != http.StatusUnsupportedMediaType {
			t.Fatalf("Content-Type %q: status %d", ct, rec.Code)
		}
	}
	if rec := do(http.MethodPost, "/", "application/json; charset=utf-8"); rec.Code != 200 {
		t.Fatalf("charset: status %d", rec.Code)
	}
}

func TestRPCShutdownDrains(t *testing.T) {
	s, _, _ := newTestServer(t, nil, genesisChain(t))
	s.sem <- struct{}{}
	done := make(chan struct{})
	go func() {
		s.Shutdown(NewHTTPServer(s))
		close(done)
	}()
	select {
	case <-done:
		t.Fatal("Shutdown returned with a request in flight")
	case <-time.After(100 * time.Millisecond):
	}
	<-s.sem
	finishesWithin(t, 2*time.Second, "Shutdown", func() { <-done })
	if rec := post(t, s, `{"jsonrpc":"2.0","method":"test_count","id":1}`); rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("POST after Shutdown: status %d", rec.Code)
	}
}

func TestRPCSlotsReturned(t *testing.T) {
	s, _, _ := newTestServer(t, nil, genesisChain(t))
	if cap(s.sem) != 32 {
		t.Fatalf("in-flight cap = %d, want 32", cap(s.sem))
	}
	over := `{"jsonrpc":"2.0","method":"test_count","id":1}` + strings.Repeat(" ", maxRequestBytes)
	for name, body := range map[string]string{
		"result":       `{"jsonrpc":"2.0","method":"test_count","id":1}`,
		"parse error":  `{`,
		"panic":        `{"jsonrpc":"2.0","method":"test_panic","id":1}`,
		"notification": `{"jsonrpc":"2.0","method":"test_count"}`,
		"body too big": over,
	} {
		post(t, s, body)
		if n := len(s.sem); n != 0 {
			t.Errorf("%s: %d slots held after the response", name, n)
		}
	}
}

// errReader fails every read with err.
type errReader struct{ err error }

func (r errReader) Read([]byte) (int, error) { return 0, r.err }

func TestRPCBodyErrors(t *testing.T) {
	s, _, tm := newTestServer(t, nil, genesisChain(t))
	serve := func(err error) *httptest.ResponseRecorder {
		req := httptest.NewRequest(http.MethodPost, "/", errReader{err})
		req.Header.Set("Content-Type", "application/json")
		rec := httptest.NewRecorder()
		s.ServeHTTP(rec, req)
		return rec
	}
	if rec := serve(os.ErrDeadlineExceeded); rec.Code != http.StatusRequestTimeout || rec.Header().Get("Connection") != "close" {
		t.Fatalf("read deadline: status %d headers %v, want 408 with Connection: close", rec.Code, rec.Header())
	}
	if rec := serve(errors.New("reset")); rec.Code != http.StatusBadRequest {
		t.Fatalf("other read error: status %d, want 400", rec.Code)
	}
	if tm.count.Load() != 0 || len(s.sem) != 0 {
		t.Fatalf("%d calls, %d slots held", tm.count.Load(), len(s.sem))
	}
}

// failWriter accepts one write, then fails.
type failWriter struct{ writes int }

func (w *failWriter) Write(p []byte) (int, error) {
	w.writes++
	if w.writes > 1 {
		return 0, errors.New("closed")
	}
	return len(p), nil
}

func TestRPCWriteErrorStopsBatch(t *testing.T) {
	s, _, tm := newTestServer(t, nil, genesisChain(t))
	one := `{"jsonrpc":"2.0","method":"test_count","id":1}`
	if _, err := s.handle(context.Background(), nil, []byte("["+one+","+one+","+one+"]"), &failWriter{}); err == nil {
		t.Fatal("no write error returned")
	}
	if n := tm.count.Load(); n != 1 {
		t.Fatalf("%d methods ran, want 1: none after the writer failed", n)
	}
}

func TestRPCShutdownDeliversInFlight(t *testing.T) {
	s, _, _ := newTestServer(t, nil, genesisChain(t))
	s.bodyReadTimeout = 200 * time.Millisecond
	started, release := make(chan struct{}), make(chan struct{})
	s.methods["test_block"] = func(*Server, []json.RawMessage, *budget) (any, error) {
		close(started)
		<-release
		return "done", nil
	}
	hs := NewHTTPServer(s)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = hs.Serve(ln) }()
	type result struct {
		code int
		body string
		err  error
	}
	got := make(chan result, 1)
	go func() {
		res, err := http.Post("http://"+ln.Addr().String(), "application/json", strings.NewReader(`{"jsonrpc":"2.0","method":"test_block","id":1}`))
		if err != nil {
			got <- result{err: err}
			return
		}
		defer res.Body.Close()
		b, _ := io.ReadAll(res.Body)
		got <- result{res.StatusCode, string(b), nil}
	}()
	<-started
	down := make(chan struct{})
	go func() {
		s.Shutdown(hs)
		close(down)
	}()
	time.Sleep(500 * time.Millisecond)
	select {
	case <-down:
		t.Fatal("Shutdown returned while a request was in flight")
	default:
	}
	close(release)
	select {
	case r := <-got:
		if r.err != nil || r.code != 200 || r.body != okResp("1", `"done"`) {
			t.Fatalf("in-flight response: %+v", r)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("no response")
	}
	finishesWithin(t, 5*time.Second, "Shutdown", func() { <-down })
}

// A handler that outlasts the body read timeout keeps its request
// context: the read deadline is cleared once the body is read.
func TestRPCBodyDeadlineCleared(t *testing.T) {
	s, _, _ := newTestServer(t, nil, genesisChain(t))
	s.bodyReadTimeout = 200 * time.Millisecond
	s.methods["test_sleep"] = func(*Server, []json.RawMessage, *budget) (any, error) {
		time.Sleep(3 * s.bodyReadTimeout)
		return true, nil
	}
	errs := make(chan error, 1)
	hs := NewHTTPServer(s)
	hs.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s.ServeHTTP(w, r)
		errs <- r.Context().Err()
	})
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = hs.Serve(ln) }()
	t.Cleanup(func() { _ = hs.Close() })

	resp, err := http.Post("http://"+ln.Addr().String()+"/", "application/json",
		strings.NewReader(`{"jsonrpc":"2.0","method":"test_sleep","id":1}`))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil || resp.StatusCode != 200 || string(body) != okResp("1", "true") {
		t.Fatalf("status %d, body %q, err %v", resp.StatusCode, body, err)
	}
	select {
	case err := <-errs:
		if err != nil {
			t.Fatalf("the request context ended while the handler ran: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("handler did not return")
	}
}

// A POST with no body keeps its request context across a handler that
// outlasts the body read timeout, and its connection serves the next
// request.
func TestRPCBodylessPostDeadlineCleared(t *testing.T) {
	s, _, _ := newTestServer(t, nil, genesisChain(t))
	s.bodyReadTimeout = 200 * time.Millisecond
	errs := make(chan error, 2)
	hs := NewHTTPServer(s)
	hs.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s.ServeHTTP(w, r)
		time.Sleep(3 * s.bodyReadTimeout)
		errs <- r.Context().Err()
	})
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = hs.Serve(ln) }()
	t.Cleanup(func() { _ = hs.Close() })

	conn, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	br := bufio.NewReader(conn)
	for i := 0; i < 2; i++ {
		_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
		if _, err := io.WriteString(conn, "POST / HTTP/1.1\r\nHost: x\r\nContent-Type: application/json\r\nContent-Length: 0\r\n\r\n"); err != nil {
			t.Fatalf("request %d: %v", i, err)
		}
		resp, err := http.ReadResponse(br, nil)
		if err != nil {
			t.Fatalf("request %d: %v", i, err)
		}
		_, _ = io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		if resp.StatusCode != 200 {
			t.Fatalf("request %d: status %d", i, resp.StatusCode)
		}
		select {
		case err := <-errs:
			if err != nil {
				t.Fatalf("request %d: the request context ended while the handler ran: %v", i, err)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("handler did not return")
		}
	}
}

func TestLogKind(t *testing.T) {
	same := [][2]string{
		{`read: leaf width 7 for contract 12`, `read: leaf width 4096 for contract 9`},
		{`read: sender z1qqaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa failed`, `read: sender z1qpbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb failed`},
		{`read: key "\x00\x01ab" damaged`, `read: key "\xff\x7fab" damaged`},
		{`read: hash aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa failed`, `read: hash bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb failed`},
	}
	for _, p := range same {
		if a, b := logKind(p[0]), logKind(p[1]); a != b {
			t.Errorf("kinds differ: %q vs %q", a, b)
		}
	}
	if logKind(`read: leaf width 7`) == logKind(`read: chunk count 7`) {
		t.Error("different texts share a kind")
	}
}
