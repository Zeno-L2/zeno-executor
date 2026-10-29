package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/syndtr/goleveldb/leveldb"
	"github.com/zenon-network/go-zenon/common/types"

	"github.com/Zeno-L2/zeno-executor/internal/gateway"
	"github.com/Zeno-L2/zeno-executor/internal/lease"
)

func TestExitStatus(t *testing.T) {
	if got := exitStatus(nil); got != 0 {
		t.Fatalf("exitStatus(nil) = %d, want 0", got)
	}
	if got := exitStatus(fmt.Errorf("%w: x", gateway.ErrConfig)); got != 78 {
		t.Fatalf("exitStatus(config error) = %d, want 78", got)
	}
	if got := exitStatus(errors.New("x")); got != 1 {
		t.Fatalf("exitStatus(other error) = %d, want 1", got)
	}
}

// writeConfig writes a gateway configuration with extra top-level
// fields and returns its path.
func writeConfig(t *testing.T, storagePath string, extra map[string]any) string {
	t.Helper()
	cfg := map[string]any{
		"znndEndpoint":      "http://127.0.0.1:1",
		"networkName":       "test",
		"confirmationDepth": 2,
		"storagePath":       storagePath,
		"listenAddress":     "127.0.0.1:0",
		"domains": []map[string]any{
			{"domainId": 1, "source": "LOCAL", "executorStoragePath": filepath.Join(t.TempDir(), "exec")},
		},
		"simulation": map[string]any{"enabled": false},
	}
	for k, v := range extra {
		cfg[k] = v
	}
	b, err := json.Marshal(cfg)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "gateway.json")
	if err := os.WriteFile(path, b, 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestRunConfigErrors(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "missing.json")
	unknown := writeConfig(t, filepath.Join(t.TempDir(), "gw"), map[string]any{"bogus": 1})
	for name, args := range map[string][]string{
		"missing file":  {"-config", missing},
		"unknown flag":  {"-bogus"},
		"unknown field": {"-config", unknown},
	} {
		if got := run(args); got != 78 {
			t.Errorf("%s: run = %d, want 78", name, got)
		}
	}
}

func TestRunRefusesSymlinkedStorage(t *testing.T) {
	real := t.TempDir()
	link := filepath.Join(t.TempDir(), "gw")
	if err := os.Symlink(real, link); err != nil {
		t.Fatal(err)
	}
	if got := run([]string{"-config", writeConfig(t, link, nil)}); got != 1 {
		t.Fatalf("run = %d, want 1", got)
	}
	if _, err := os.Stat(filepath.Join(real, "gateway.lock")); err == nil {
		t.Fatal("run created gateway.lock under a symlinked storagePath")
	}
}

func TestRunLockHeld(t *testing.T) {
	storage := filepath.Join(t.TempDir(), "gw")
	if err := os.MkdirAll(storage, 0o700); err != nil {
		t.Fatal(err)
	}
	l, err := lease.Acquire(filepath.Join(storage, "gateway.lock"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = l.Release() })
	res := make(chan int, 1)
	go func() { res <- run([]string{"-config", writeConfig(t, storage, nil)}) }()
	select {
	case got := <-res:
		if got != 1 {
			t.Fatalf("run = %d, want 1", got)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("run did not return within 10s: it ignored the held gateway.lock")
	}
}

func TestRunRefusesSymlinkedLock(t *testing.T) {
	storage := filepath.Join(t.TempDir(), "gw")
	if err := os.MkdirAll(storage, 0o700); err != nil {
		t.Fatal(err)
	}
	victim := filepath.Join(t.TempDir(), "victim")
	if err := os.WriteFile(victim, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(victim, filepath.Join(storage, "gateway.lock")); err != nil {
		t.Fatal(err)
	}
	if got := run([]string{"-config", writeConfig(t, storage, nil)}); got != 1 {
		t.Fatalf("run = %d, want 1", got)
	}
	if b, err := os.ReadFile(victim); err != nil || string(b) != "x" {
		t.Fatalf("victim = %q, %v; want untouched", b, err)
	}
}

// A link repointed after PrepareStorage must not redirect the lock or
// the chain cache: run keeps using the resolved path.
func TestRunUsesResolvedStoragePath(t *testing.T) {
	treeA, treeB, holder := t.TempDir(), t.TempDir(), t.TempDir()
	link := filepath.Join(holder, "link")
	if err := os.Symlink(treeA, link); err != nil {
		t.Fatal(err)
	}
	configured := filepath.Join(link, "gw")
	resolvedA, err := filepath.EvalSymlinks(treeA)
	if err != nil {
		t.Fatal(err)
	}
	resolvedGW := filepath.Join(resolvedA, "gw")

	afterPrepare = func() {
		if err := os.Remove(link); err != nil {
			t.Error(err)
		}
		if err := os.Symlink(treeB, link); err != nil {
			t.Error(err)
		}
		if err := os.MkdirAll(configured, 0o700); err != nil {
			t.Error(err)
		}
	}
	waitForNode = func(context.Context, string, func() error, func(string, ...any)) bool { return true }
	t.Cleanup(func() {
		afterPrepare = func() {}
		waitForNode = gateway.WaitForNode
	})

	res := make(chan int, 1)
	go func() { res <- run([]string{"-config", writeConfig(t, configured, nil)}) }()

	// The chain boots once its cache directory exists under the
	// resolved path.
	deadline := time.Now().Add(10 * time.Second)
	for {
		if _, err := os.Stat(filepath.Join(resolvedGW, "gateway.lock")); err == nil {
			if entries, _ := os.ReadDir(resolvedGW); len(entries) >= 2 {
				break
			}
		}
		if time.Now().After(deadline) {
			_ = syscall.Kill(os.Getpid(), syscall.SIGTERM)
			t.Fatal("run never created the lock and the chain cache under the resolved path")
		}
		time.Sleep(20 * time.Millisecond)
	}
	if err := syscall.Kill(os.Getpid(), syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	select {
	case got := <-res:
		if got != 0 {
			t.Fatalf("run = %d, want 0", got)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("run did not stop on SIGTERM")
	}
	if entries, _ := os.ReadDir(configured); len(entries) != 0 {
		t.Fatalf("the repointed tree was touched: %v", entries)
	}
}

// run closes every chain's index when it returns, which releases the
// database lock.
func TestRunClosesIndexes(t *testing.T) {
	tree, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	storage := filepath.Join(tree, "gw")
	indexDir := filepath.Join(storage, "1", "index")
	waitForNode = func(context.Context, string, func() error, func(string, ...any)) bool { return true }
	t.Cleanup(func() { waitForNode = gateway.WaitForNode })

	res := make(chan int, 1)
	go func() { res <- run([]string{"-config", writeConfig(t, storage, nil)}) }()

	deadline := time.Now().Add(10 * time.Second)
	for {
		if _, err := os.Stat(filepath.Join(indexDir, "CURRENT")); err == nil {
			break
		}
		if time.Now().After(deadline) {
			_ = syscall.Kill(os.Getpid(), syscall.SIGTERM)
			t.Fatal("run never opened the index")
		}
		time.Sleep(20 * time.Millisecond)
	}
	if err := syscall.Kill(os.Getpid(), syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	select {
	case got := <-res:
		if got != 0 {
			t.Fatalf("run = %d, want 0", got)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("run did not stop on SIGTERM")
	}

	db, err := leveldb.OpenFile(indexDir, nil)
	if err != nil {
		t.Fatalf("the index is still open after run returned: %v", err)
	}
	_ = db.Close()
}

// stubNode makes run boot its domains without a znnd node.
func stubNode(t *testing.T) {
	t.Helper()
	waitForNode = func(context.Context, string, func() error, func(string, ...any)) bool { return true }
	t.Cleanup(func() { waitForNode = gateway.WaitForNode })
}

// requireIndexReopens requires the domain 1 index under storage to
// open, which it does only once run has closed it.
func requireIndexReopens(t *testing.T, storage string) {
	t.Helper()
	db, err := leveldb.OpenFile(filepath.Join(storage, "1", "index"), nil)
	if err != nil {
		t.Fatalf("the index is still open after run returned: %v", err)
	}
	_ = db.Close()
}

func rpcPost(t *testing.T, addr, body string, hdr ...string) (int, string) {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, "http://"+addr+"/", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	for i := 0; i+1 < len(hdr); i += 2 {
		req.Header.Set(hdr[i], hdr[i+1])
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(b)
}

func TestRunServesJSONRPC(t *testing.T) {
	tree, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	storage := filepath.Join(tree, "gw")
	stubNode(t)
	addrs := make(chan string, 1)
	onListen = func(ln net.Listener) { addrs <- ln.Addr().String() }
	t.Cleanup(func() { onListen = func(net.Listener) {} })

	res := make(chan int, 1)
	go func() { res <- run([]string{"-config", writeConfig(t, storage, nil)}) }()
	var addr string
	select {
	case addr = <-addrs:
	case <-time.After(10 * time.Second):
		_ = syscall.Kill(os.Getpid(), syscall.SIGTERM)
		t.Fatal("run never listened")
	}

	const call = `{"jsonrpc":"2.0","method":"zeno_chainInfo","params":[1],"id":1}`
	code, body := rpcPost(t, addr, call)
	if code != 200 || !strings.Contains(body, `"networkName":"test"`) || !strings.Contains(body, `"status":"SYNCING"`) {
		t.Fatalf("zeno_chainInfo: status %d body %s", code, body)
	}
	if code, _ := rpcPost(t, addr, call, "Origin", "https://evil.example"); code != http.StatusForbidden {
		t.Fatalf("disallowed Origin: status %d, want 403", code)
	}

	if err := syscall.Kill(os.Getpid(), syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	select {
	case got := <-res:
		if got != 0 {
			t.Fatalf("run = %d, want 0", got)
		}
	case <-time.After(20 * time.Second):
		t.Fatal("run did not stop on SIGTERM")
	}
	if conn, err := net.Dial("tcp", addr); err == nil {
		_ = conn.Close()
		t.Fatal("the listener still accepts connections after run returned")
	}
	requireIndexReopens(t, storage)
}

// A request that holds a slot when SIGTERM arrives is answered before
// the indexes close.
func TestRunDrainsBeforeClosingIndexes(t *testing.T) {
	tree, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	storage := filepath.Join(tree, "gw")
	stubNode(t)
	addrs := make(chan string, 1)
	onListen = func(ln net.Listener) { addrs <- ln.Addr().String() }
	t.Cleanup(func() { onListen = func(net.Listener) {} })

	res := make(chan int, 1)
	go func() { res <- run([]string{"-config", writeConfig(t, storage, nil)}) }()
	var addr string
	select {
	case addr = <-addrs:
	case <-time.After(10 * time.Second):
		_ = syscall.Kill(os.Getpid(), syscall.SIGTERM)
		t.Fatal("run never listened")
	}

	body := `{"jsonrpc":"2.0","method":"zeno_getReceipt","params":["` + strings.Repeat("0", 64) + `"],"id":1}`
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(10 * time.Second))
	head := fmt.Sprintf("POST / HTTP/1.1\r\nHost: gw\r\nContent-Type: application/json\r\nContent-Length: %d\r\n\r\n", len(body))
	if _, err := io.WriteString(conn, head+body[:1]); err != nil {
		t.Fatal(err)
	}
	time.Sleep(200 * time.Millisecond)

	if err := syscall.Kill(os.Getpid(), syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	// The listener closes only inside api.Shutdown, which then waits
	// for the request that holds the slot.
	for deadline := time.Now().Add(3 * time.Second); ; time.Sleep(10 * time.Millisecond) {
		c, err := net.Dial("tcp", addr)
		if err != nil {
			break
		}
		_ = c.Close()
		if time.Now().After(deadline) {
			t.Fatal("the listener still accepts connections after SIGTERM")
		}
	}

	if _, err := io.WriteString(conn, body[1:]); err != nil {
		t.Fatal(err)
	}
	resp, err := http.ReadResponse(bufio.NewReader(conn), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var out struct {
		Error *struct {
			Code int `json:"code"`
		} `json:"error"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != 200 || out.Error == nil || out.Error.Code != -32004 {
		t.Fatalf("status %d, error %+v, want 200 and -32004", resp.StatusCode, out.Error)
	}

	select {
	case got := <-res:
		if got != 0 {
			t.Fatalf("run = %d, want 0", got)
		}
	case <-time.After(20 * time.Second):
		t.Fatal("run did not stop on SIGTERM")
	}
	requireIndexReopens(t, storage)
}

func TestRunListenFails(t *testing.T) {
	tree, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	storage := filepath.Join(tree, "gw")
	stubNode(t)
	held, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer held.Close()

	cfg := writeConfig(t, storage, map[string]any{"listenAddress": held.Addr().String()})
	res := make(chan int, 1)
	go func() { res <- run([]string{"-config", cfg}) }()
	select {
	case got := <-res:
		if got != 1 {
			t.Fatalf("run = %d, want 1", got)
		}
	case <-time.After(10 * time.Second):
		_ = syscall.Kill(os.Getpid(), syscall.SIGTERM)
		t.Fatal("run did not return when the listener could not bind")
	}
	requireIndexReopens(t, storage)
}

func TestRunServeFails(t *testing.T) {
	tree, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	storage := filepath.Join(tree, "gw")
	stubNode(t)
	onListen = func(ln net.Listener) { _ = ln.Close() }
	t.Cleanup(func() { onListen = func(net.Listener) {} })

	res := make(chan int, 1)
	go func() { res <- run([]string{"-config", writeConfig(t, storage, nil)}) }()
	select {
	case got := <-res:
		if got != 1 {
			t.Fatalf("run = %d, want 1", got)
		}
	case <-time.After(20 * time.Second):
		_ = syscall.Kill(os.Getpid(), syscall.SIGTERM)
		t.Fatal("run did not return when the server stopped")
	}
	requireIndexReopens(t, storage)
}

// logBuffer is a log destination safe for concurrent use.
type logBuffer struct {
	mu sync.Mutex
	b  strings.Builder
}

func (l *logBuffer) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.Write(p)
}

func (l *logBuffer) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.b.String()
}

// run with simulation enabled seals descriptors, turns the gateway
// non-dumpable, launches a simulator through the real host and stops it
// on shutdown.
func TestRunStartsSimulators(t *testing.T) {
	if runtime.GOOS == "linux" && os.Geteuid() == 0 {
		t.Skip("as root on Linux StartSimulation moves the test process into a cgroup leaf and needs simulation.uid; the pool tests cover that path")
	}
	bin := filepath.Join(t.TempDir(), "worker")
	if err := os.Chmod(filepath.Dir(bin), 0o755); err != nil {
		t.Fatal(err)
	}
	if out, err := exec.Command("go", "build", "-o", bin, "github.com/Zeno-L2/zeno-executor/internal/testbin/worker").CombinedOutput(); err != nil {
		t.Fatalf("build the testbin worker: %v\n%s", err, out)
	}
	tree, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	storage := filepath.Join(tree, "gw")
	stubNode(t)
	addrs := make(chan string, 1)
	onListen = func(ln net.Listener) { addrs <- ln.Addr().String() }
	t.Cleanup(func() { onListen = func(net.Listener) {} })
	logs := &logBuffer{}
	log.SetOutput(logs)
	t.Cleanup(func() { log.SetOutput(os.Stderr) })

	cfg := writeConfig(t, storage, map[string]any{
		"simulation": map[string]any{"enabled": true, "maxConcurrent": 1},
		"domains": []map[string]any{
			{"domainId": 1, "source": "LOCAL", "executorStoragePath": filepath.Join(tree, "exec"), "workerBinary": bin},
		},
	})
	res := make(chan int, 1)
	go func() { res <- run([]string{"-config", cfg}) }()
	var addr string
	select {
	case addr = <-addrs:
	case <-time.After(10 * time.Second):
		_ = syscall.Kill(os.Getpid(), syscall.SIGTERM)
		t.Fatal("run never listened")
	}
	deadline := time.Now().Add(5 * time.Second)
	for !strings.Contains(logs.String(), "gateway: simulation enforcement:") {
		if time.Now().After(deadline) {
			_ = syscall.Kill(os.Getpid(), syscall.SIGTERM)
			t.Fatalf("no enforcement line within 5 s:\n%s", logs.String())
		}
		time.Sleep(20 * time.Millisecond)
	}
	// A start that fails logs "did not start" within the 5 s start
	// limit, so a quiet 6 s means a simulator reached Ready.
	time.Sleep(6 * time.Second)
	if strings.Contains(logs.String(), "did not start") {
		_ = syscall.Kill(os.Getpid(), syscall.SIGTERM)
		t.Fatalf("a simulator did not start:\n%s", logs.String())
	}

	input := fmt.Sprintf(`{"kind":"CallL2","sender":"%s","targetContract":"%s","plasmaLimit":1}`, types.Address{1}, types.Hash{2})
	code, body := rpcPost(t, addr, `{"jsonrpc":"2.0","method":"zeno_simulate","params":[1,`+input+`],"id":1}`)
	if code != 200 || strings.Contains(body, "simulation is disabled") || !strings.Contains(body, `"error"`) {
		t.Fatalf("zeno_simulate: status %d body %s", code, body)
	}
	if runtime.GOOS == "linux" {
		fi, err := os.Stat("/proc/self/environ")
		if err != nil {
			t.Fatal(err)
		}
		if st, ok := fi.Sys().(*syscall.Stat_t); !ok || int(st.Uid) == os.Geteuid() {
			t.Fatalf("/proc/self/environ is owned by the gateway's own uid: the process is dumpable")
		}
	} else {
		t.Logf("the non-dumpable check runs on Linux only; SetNonDumpable is a no-op on %s", runtime.GOOS)
	}

	if err := syscall.Kill(os.Getpid(), syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	select {
	case got := <-res:
		if got != 0 {
			t.Fatalf("run = %d, want 0", got)
		}
	case <-time.After(20 * time.Second):
		t.Fatal("run did not stop on SIGTERM")
	}
}
