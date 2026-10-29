package zenoexec

import (
	"encoding/binary"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/zenon-network/go-zenon/common/types"

	"github.com/Zeno-L2/zeno-executor/plugins/wasm/internal/keys"
)

const testPort = 8547

var testVersion = VersionInfo{Version: "1.2.3", Revision: "abc", Modified: true}

func newTestHandler(t *testing.T) http.Handler {
	t.Helper()
	h, err := NewHandler(ServeConfig{Network: "testnet", ChildCommand: childCommand(), Port: testPort, Version: testVersion})
	if err != nil {
		t.Fatalf("NewHandler: %v", err)
	}
	return h
}

// do sends one request with Host set to the allowed loopback host.
func do(h http.Handler, method, path, contentType, origin, host, body string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	req.Host = host
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	if origin != "" {
		req.Header.Set("Origin", origin)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

var okHost = fmt.Sprintf("127.0.0.1:%d", testPort)

func TestServeOrigins(t *testing.T) {
	h := newTestHandler(t)
	body := `{"commands":[{"op":"nope"}]}`

	rec := do(h, "POST", "/v1/run", "application/json", "http://localhost:5173", okHost, body)
	if rec.Code != 200 || rec.Header().Get("Access-Control-Allow-Origin") != "http://localhost:5173" {
		t.Fatalf("localhost origin: %d %v", rec.Code, rec.Header())
	}
	if !strings.Contains(rec.Body.String(), `"bad_command"`) {
		t.Fatalf("body = %s", rec.Body)
	}

	rec = do(h, "POST", "/v1/run", "application/json", "https://example.com", okHost, body)
	if rec.Code != 403 || rec.Header().Get("Access-Control-Allow-Origin") != "" {
		t.Fatalf("foreign origin: %d %v", rec.Code, rec.Header())
	}

	rec = do(h, "POST", "/v1/run", "application/json", "", okHost, body)
	if rec.Code != 200 {
		t.Fatalf("no origin: %d", rec.Code)
	}

	rec = do(h, "POST", "/v1/run", "application/json", "", "evil.example:8547", body)
	if rec.Code != 403 {
		t.Fatalf("foreign host: %d", rec.Code)
	}
}

func TestServeAllowOrigin(t *testing.T) {
	h, err := NewHandler(ServeConfig{Network: "testnet", ChildCommand: childCommand(), Port: testPort, AllowOrigins: []string{"https://console.example"}})
	if err != nil {
		t.Fatal(err)
	}
	rec := do(h, "OPTIONS", "/v1/run", "", "https://console.example", okHost, "")
	if rec.Code != 204 || rec.Header().Get("Access-Control-Allow-Origin") != "https://console.example" {
		t.Fatalf("allowed origin preflight: %d %v", rec.Code, rec.Header())
	}
}

func TestServeLimits(t *testing.T) {
	h := newTestHandler(t)

	var cmds []string
	for i := 0; i < 65; i++ {
		cmds = append(cmds, `{"op":"nope"}`)
	}
	rec := do(h, "POST", "/v1/run", "application/json", "", okHost, `{"commands":[`+strings.Join(cmds, ",")+`]}`)
	if rec.Code != 400 {
		t.Fatalf("65 commands: %d", rec.Code)
	}
	for _, body := range []string{`{}`, `{"commands":null}`, `{"commands":[],"x":1}`, `[]`} {
		if rec := do(h, "POST", "/v1/run", "application/json", "", okHost, body); rec.Code != 400 {
			t.Fatalf("body %s: %d", body, rec.Code)
		}
	}

	big := `{"commands":[],"pad":"` + strings.Repeat("a", 2<<20) + `"}`
	if rec := do(h, "POST", "/v1/run", "application/json", "", okHost, big); rec.Code != 413 {
		t.Fatalf("big body: %d", rec.Code)
	}

	// An unknown length passes the header check, so the slot is taken
	// and the body reader refuses the body.
	req := httptest.NewRequest("POST", "/v1/run", strings.NewReader(big))
	req.Host = okHost
	req.ContentLength = -1
	req.Header.Set("Content-Type", "application/json")
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != 413 {
		t.Fatalf("big body of unknown length: %d", rec.Code)
	}
	if len(sem) != 0 {
		t.Fatalf("len(sem) = %d after the refused body, want 0", len(sem))
	}

	if rec := do(h, "POST", "/v1/run", "text/plain", "", okHost, `{"commands":[]}`); rec.Code != 415 {
		t.Fatalf("text/plain: %d", rec.Code)
	}
	if rec := do(h, "GET", "/v1/run", "", "", okHost, ""); rec.Code != 405 {
		t.Fatalf("GET run: %d", rec.Code)
	}
	if rec := do(h, "GET", "/nope", "", "", okHost, ""); rec.Code != 404 {
		t.Fatalf("unknown path: %d", rec.Code)
	}
	if len(sem) != 0 {
		t.Fatalf("len(sem) = %d after rejected requests, want 0", len(sem))
	}
}

func TestServePreflightAndVersion(t *testing.T) {
	h := newTestHandler(t)
	rec := do(h, "OPTIONS", "/v1/run", "", "http://localhost:5173", okHost, "")
	if rec.Code != 204 || rec.Header().Get("Access-Control-Allow-Methods") != "POST, OPTIONS" ||
		rec.Header().Get("Access-Control-Allow-Headers") != "Content-Type" {
		t.Fatalf("preflight: %d %v", rec.Code, rec.Header())
	}
	if rec := do(h, "OPTIONS", "/v1/run", "", "https://example.com", okHost, ""); rec.Code != 403 {
		t.Fatalf("foreign preflight: %d", rec.Code)
	}
	rec = do(h, "GET", "/v1/version", "", "", okHost, "")
	if rec.Code != 200 || strings.TrimSpace(rec.Body.String()) != `{"version":"1.2.3","revision":"abc","modified":true}` {
		t.Fatalf("version: %d %s", rec.Code, rec.Body)
	}
}

func TestNewHandlerRejectsBadOrigins(t *testing.T) {
	for _, o := range []string{"ftp://x", "http://localhost:5173/", "http://", "localhost", "http://u@x", "http://x?q=1"} {
		if _, err := NewHandler(ServeConfig{Network: "testnet", ChildCommand: childCommand(), Port: testPort, AllowOrigins: []string{o}}); err == nil {
			t.Errorf("NewHandler accepted %q", o)
		}
	}
}

func TestCheckListen(t *testing.T) {
	for _, a := range []string{"127.0.0.1:0", "[::1]:0", "127.0.0.1:8547"} {
		if err := CheckListen(a); err != nil {
			t.Errorf("CheckListen(%q): %v", a, err)
		}
	}
	for _, a := range []string{"127.0.0.2:8547", "0.0.0.0:8547", "localhost:8547", ":8547", "nonsense"} {
		if err := CheckListen(a); err == nil {
			t.Errorf("CheckListen(%q) accepted", a)
		}
	}
}

func TestServeTimeoutGives504(t *testing.T) {
	old := requestTimeout
	requestTimeout = time.Nanosecond
	t.Cleanup(func() { requestTimeout = old })

	h := newTestHandler(t)
	body := `{"commands":[` + deployLine(noopModule()) + `]}`
	rec := do(h, "POST", "/v1/run", "application/json", "", okHost, body)
	if rec.Code != 504 {
		t.Fatalf("status = %d, want 504", rec.Code)
	}
	deadline := time.Now().Add(10 * time.Second)
	for len(sem) != 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if len(sem) != 0 {
		t.Fatalf("len(sem) = %d, want the slot released after the session closed", len(sem))
	}
}

func TestServeSemaphore(t *testing.T) {
	h := newTestHandler(t)
	for i := 0; i < cap(sem); i++ {
		select {
		case sem <- struct{}{}:
		default:
			t.Fatalf("len(sem) = %d before the test, want a free slot", len(sem))
		}
	}
	body := `{"commands":[{"op":"nope"}]}`
	rec := do(h, "POST", "/v1/run", "application/json", "", okHost, body)
	for i := 0; i < cap(sem); i++ {
		<-sem
	}
	if rec.Code != 503 {
		t.Fatalf("full semaphore: %d, want 503", rec.Code)
	}

	rec = do(h, "POST", "/v1/run", "application/json", "", okHost, body)
	if rec.Code != 200 {
		t.Fatalf("drained semaphore: %d, want 200", rec.Code)
	}
	if len(sem) != 0 {
		t.Fatalf("len(sem) = %d, want 0", len(sem))
	}
}

// firstContractID is the id of the contract a fresh session deploys
// first: the finalize is input 3.
func firstContractID() types.Hash {
	var idx [8]byte
	binary.BigEndian.PutUint64(idx[:], 3)
	chain := keys.ChainID("testnet")
	return sha3(senderAddr[:], idx[:], chain[:])
}

func TestServeBoundsRunawayCalls(t *testing.T) {
	h := newTestHandler(t)
	call := fmt.Sprintf(`{"op":"call","sender":%q,"contractId":%q,"payload":"","plasmaLimit":1000000000}`, senderHex, firstContractID())
	body := `{"commands":[` + deployLine(loopModule()) + `,` + call + `]}`

	start := time.Now()
	rec := do(h, "POST", "/v1/run", "application/json", "", okHost, body)
	if rec.Code != 200 || !strings.Contains(rec.Body.String(), `"bad_command"`) {
		t.Fatalf("runaway call: %d %s", rec.Code, rec.Body)
	}
	if len(sem) != 0 {
		t.Fatalf("len(sem) = %d, want 0", len(sem))
	}
	if d := time.Since(start); d > requestTimeout {
		t.Fatalf("request took %v, want under %v", d, requestTimeout)
	}
}

func TestServeGasBudgetIs200M(t *testing.T) {
	h := newTestHandler(t)
	call := fmt.Sprintf(`{"op":"call","sender":%q,"contractId":%q,"payload":"","plasmaLimit":200000001}`, senderHex, firstContractID())
	body := `{"commands":[` + deployLine(noopModule()) + `,` + call + `]}`
	rec := do(h, "POST", "/v1/run", "application/json", "", okHost, body)
	if rec.Code != 200 || !strings.Contains(rec.Body.String(), `"kind":"bad_command"`) || !strings.Contains(rec.Body.String(), "exceeds the 200000000 gas left") || !strings.Contains(rec.Body.String(), "lower plasmaLimit") {
		t.Fatalf("call over 200M: %d %s", rec.Code, rec.Body)
	}
	if len(sem) != 0 {
		t.Fatalf("len(sem) = %d, want 0", len(sem))
	}
}

func TestServeChildCrashGives500(t *testing.T) {
	h, err := NewHandler(ServeConfig{Network: "testnet", ChildCommand: []string{os.Args[0], "__crash"}, Port: testPort})
	if err != nil {
		t.Fatal(err)
	}
	rec := do(h, "POST", "/v1/run", "application/json", "", okHost, `{"commands":[{"op":"nope"}]}`)
	if rec.Code != 500 {
		t.Fatalf("status = %d, want 500", rec.Code)
	}
	waitSlotsFree(t, time.Second)
}

func TestNewHandlerRequiresChildCommand(t *testing.T) {
	if _, err := NewHandler(ServeConfig{Network: "testnet", Port: testPort}); err == nil {
		t.Fatal("NewHandler accepted an empty ChildCommand")
	}
}

func TestServeKeepsStateAcrossCommands(t *testing.T) {
	h := newTestHandler(t)
	set := fmt.Sprintf(`{"op":"setStorage","contractId":%q,"localKey":"6b","value":"0102"}`, strings.Repeat("07", 32))
	get := fmt.Sprintf(`{"op":"getStorage","contractId":%q,"localKey":"6b"}`, strings.Repeat("07", 32))
	rec := do(h, "POST", "/v1/run", "application/json", "", okHost, "{\"commands\":[\n"+set+",\n"+get+"]}")
	want := `{"results":[{"ok":true},{"ok":true,"value":"0102"}]}`
	if rec.Code != 200 || strings.TrimSpace(rec.Body.String()) != want {
		t.Fatalf("%d %s, want %s", rec.Code, rec.Body, want)
	}
}

// waitSlotsFree waits for every session slot to come back.
func waitSlotsFree(t *testing.T, within time.Duration) {
	t.Helper()
	deadline := time.Now().Add(within)
	for len(sem) != 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if len(sem) != 0 {
		t.Fatalf("len(sem) = %d, want 0 within %v", len(sem), within)
	}
}

// hangBody is a request body the hanging child never answers.
const hangBody = `{"commands":[{"op":"nope"}]}`

// newHangHandler returns a handler whose session child sleeps until it is killed.
func newHangHandler(t *testing.T) http.Handler {
	t.Helper()
	h, err := NewHandler(ServeConfig{Network: "testnet", ChildCommand: []string{os.Args[0], "__hang"}, Port: testPort})
	if err != nil {
		t.Fatalf("NewHandler: %v", err)
	}
	return h
}

// childPID polls for the first registered session child. The channel carries its PID, or is
// closed after 4 s with no child observed.
func childPID() <-chan int {
	pids := make(chan int, 1)
	go func() {
		for i := 0; i < 400; i++ {
			childMu.Lock()
			for c := range children {
				pids <- c.Process.Pid
				childMu.Unlock()
				return
			}
			childMu.Unlock()
			time.Sleep(10 * time.Millisecond)
		}
		close(pids)
	}()
	return pids
}

// receivePID returns the PID childPID reported, and fails when no child was observed.
func receivePID(t *testing.T, pids <-chan int) int {
	t.Helper()
	pid, ok := <-pids
	if !ok {
		t.Fatal("no session child was observed")
	}
	return pid
}

// requireChildExited fails unless the child with the given PID is gone. It first waits for the
// request slots to free: a slot is released only after the child has been reaped, so a killed
// child is no longer a zombie by then.
func requireChildExited(t *testing.T, pid int) {
	t.Helper()
	waitSlotsFree(t, 5*time.Second)
	if err := syscall.Kill(pid, 0); err == nil {
		t.Fatalf("session child %d still exists", pid)
	}
}

func TestServeKillsSlowChild(t *testing.T) {
	old := requestTimeout
	requestTimeout = 2 * time.Second
	t.Cleanup(func() { requestTimeout = old })

	h := newHangHandler(t)
	pids := childPID()

	start := time.Now()
	rec := do(h, "POST", "/v1/run", "application/json", "", okHost, hangBody)
	if rec.Code != 504 {
		t.Fatalf("status = %d, want 504", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "request timed out") {
		t.Fatalf("body = %q, want it to contain %q", rec.Body, "request timed out")
	}
	if d := time.Since(start); d > requestTimeout+2*time.Second {
		t.Fatalf("request took %v, want under %v", d, requestTimeout+2*time.Second)
	}

	requireChildExited(t, receivePID(t, pids))
	childMu.Lock()
	n := len(children)
	childMu.Unlock()
	if n != 0 {
		t.Fatalf("%d children still registered", n)
	}
}

// TestServeRemovesRequestTempDir checks that the per-request temporary
// directory is gone once the slot is free, after a success, a 504 and
// a 500.
func TestServeRemovesRequestTempDir(t *testing.T) {
	tmp := t.TempDir()
	t.Setenv("TMPDIR", tmp)
	left := func(when string) {
		t.Helper()
		waitSlotsFree(t, 5*time.Second)
		if ents, _ := os.ReadDir(tmp); len(ents) != 0 {
			t.Fatalf("%s: %d entries left in the temporary directory", when, len(ents))
		}
	}

	h := newTestHandler(t)
	rec := do(h, "POST", "/v1/run", "application/json", "", okHost, `{"commands":[`+deployLine(noopModule())+`]}`)
	if rec.Code != 200 {
		t.Fatalf("success: status = %d, want 200", rec.Code)
	}
	left("after a success")

	crash, err := NewHandler(ServeConfig{Network: "testnet", ChildCommand: []string{os.Args[0], "__crash"}, Port: testPort})
	if err != nil {
		t.Fatal(err)
	}
	if rec := do(crash, "POST", "/v1/run", "application/json", "", okHost, `{"commands":[{"op":"nope"}]}`); rec.Code != 500 {
		t.Fatalf("crash: status = %d, want 500", rec.Code)
	}
	left("after a 500")

	old := requestTimeout
	requestTimeout = time.Second
	t.Cleanup(func() { requestTimeout = old })
	hang := newHangHandler(t)
	pids := childPID()
	rec = do(hang, "POST", "/v1/run", "application/json", "", okHost, hangBody)
	if rec.Code != 504 {
		t.Fatalf("slow: status = %d, want 504", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "request timed out") {
		t.Fatalf("slow: body = %q, want it to contain %q", rec.Body, "request timed out")
	}
	requireChildExited(t, receivePID(t, pids))
	left("after a 504")
}

func TestStopSessionsRemovesRequestTempDir(t *testing.T) {
	tmp := t.TempDir()
	t.Setenv("TMPDIR", tmp)
	t.Cleanup(func() {
		childMu.Lock()
		stopping = false
		childMu.Unlock()
	})

	h := newTestHandler(t)
	hang := newHangHandler(t)
	pids := childPID()
	recs := make(chan *httptest.ResponseRecorder, 1)
	go func() { recs <- do(hang, "POST", "/v1/run", "application/json", "", okHost, hangBody) }()
	pid := receivePID(t, pids)

	StopSessions()
	if ents, _ := os.ReadDir(tmp); len(ents) != 0 {
		t.Fatalf("%d entries left in the temporary directory", len(ents))
	}
	rec := <-recs
	if rec.Code != 500 || !strings.Contains(rec.Body.String(), "session failed") {
		t.Fatalf("status = %d, body = %q, want 500 session failed", rec.Code, rec.Body)
	}
	requireChildExited(t, pid)
	if rec := do(h, "POST", "/v1/run", "application/json", "", okHost, `{"commands":[{"op":"nope"}]}`); rec.Code != 500 {
		t.Fatalf("after StopSessions: status = %d, want 500", rec.Code)
	}
}
