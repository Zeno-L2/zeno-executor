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

	"github.com/digitalSloth/go-executor/plugins/wasm"
	"github.com/digitalSloth/go-executor/plugins/wasm/internal/keys"
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

// manyLocalsModule is a valid module whose run calls, in a loop, a
// function that declares 8192 i64 locals. The gas table charges the
// call a flat amount, so the loop burns wall time far faster than gas.
func manyLocalsModule() []byte {
	out := append([]byte{}, wasmHeader...)
	out = append(out, section(1, []byte{0x02, 0x60, 0x00, 0x00, 0x60, 0x02, 0x7F, 0x7F, 0x01, 0x7E})...)
	out = append(out, section(3, []byte{0x02, 0x00, 0x01})...)
	out = append(out, section(5, []byte{0x01, 0x00, 0x01})...)
	exportSec := []byte{0x02}
	exportSec = appendName(exportSec, "memory")
	exportSec = append(exportSec, 0x02, 0x00)
	exportSec = appendName(exportSec, "run")
	exportSec = append(exportSec, 0x00, 0x01)
	out = append(out, section(7, exportSec)...)
	f := []byte{0x01, 0x80, 0x40, 0x7E, 0x0B}
	run := []byte{0x00, 0x03, 0x40, 0x10, 0x00, 0x0C, 0x00, 0x0B, 0x42, 0x00, 0x0B}
	code := []byte{0x02}
	code = appendVarU32(code, uint32(len(f)))
	code = append(code, f...)
	code = appendVarU32(code, uint32(len(run)))
	code = append(code, run...)
	out = append(out, section(10, code)...)
	return out
}

func TestServeKillsSlowChild(t *testing.T) {
	old := requestTimeout
	requestTimeout = 2 * time.Second
	t.Cleanup(func() { requestTimeout = old })

	raw := manyLocalsModule()
	if _, err := wasm.CheckModule(raw, nil); err != nil {
		t.Fatalf("the many-locals module must validate: %v", err)
	}
	h := newTestHandler(t)
	call := fmt.Sprintf(`{"op":"call","sender":%q,"contractId":%q,"payload":"","plasmaLimit":10000000}`, senderHex, firstContractID())
	body := `{"commands":[` + deployLine(raw) + `,` + call + `]}`

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

	start := time.Now()
	rec := do(h, "POST", "/v1/run", "application/json", "", okHost, body)
	if rec.Code != 504 {
		t.Fatalf("status = %d, want 504", rec.Code)
	}
	if d := time.Since(start); d > requestTimeout+2*time.Second {
		t.Fatalf("request took %v, want under %v", d, requestTimeout+2*time.Second)
	}
	waitSlotsFree(t, 2*time.Second)

	pid, ok := <-pids
	if !ok {
		t.Fatal("no session child was observed")
	}
	if err := syscall.Kill(pid, 0); err == nil {
		t.Fatalf("session child %d still exists", pid)
	}
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
	call := fmt.Sprintf(`{"op":"call","sender":%q,"contractId":%q,"payload":"","plasmaLimit":10000000}`, senderHex, firstContractID())
	body := `{"commands":[` + deployLine(manyLocalsModule()) + `,` + call + `]}`
	if rec := do(h, "POST", "/v1/run", "application/json", "", okHost, body); rec.Code != 504 {
		t.Fatalf("slow: status = %d, want 504", rec.Code)
	}
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
	call := fmt.Sprintf(`{"op":"call","sender":%q,"contractId":%q,"payload":"","plasmaLimit":10000000}`, senderHex, firstContractID())
	body := `{"commands":[` + deployLine(manyLocalsModule()) + `,` + call + `]}`
	codes := make(chan int, 1)
	go func() { codes <- do(h, "POST", "/v1/run", "application/json", "", okHost, body).Code }()
	for i := 0; ; i++ {
		childMu.Lock()
		n := len(children)
		childMu.Unlock()
		if n == 1 {
			break
		}
		if i == 400 {
			t.Fatal("no session child was observed")
		}
		time.Sleep(10 * time.Millisecond)
	}

	StopSessions()
	if ents, _ := os.ReadDir(tmp); len(ents) != 0 {
		t.Fatalf("%d entries left in the temporary directory", len(ents))
	}
	if code := <-codes; code != 500 {
		t.Fatalf("status = %d, want 500", code)
	}
	waitSlotsFree(t, time.Second)
	if rec := do(h, "POST", "/v1/run", "application/json", "", okHost, `{"commands":[{"op":"nope"}]}`); rec.Code != 500 {
		t.Fatalf("after StopSessions: status = %d, want 500", rec.Code)
	}
}
