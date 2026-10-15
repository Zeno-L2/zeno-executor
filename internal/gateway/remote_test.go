package gateway

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/zenon-network/go-zenon/common/trie"
	"github.com/zenon-network/go-zenon/common/types"

	"github.com/digitalSloth/go-executor/internal/codec"
)

// honestUpstream is a LOCAL gateway over history A with every batch
// accepted, some finalized, served over HTTP.
type honestUpstream struct {
	dir    string
	cs     []codec.BatchCommitment
	settle *fakeSettlement
	c      *Chain
	srv    *Server
	url    string
}

func newHonestUpstream(t *testing.T) *honestUpstream {
	t.Helper()
	dir, cs := buildA(t)
	s := newFakeSettlement()
	s.height = 22
	acceptFinal(s, cs, 4)
	c, _ := openTestChain(t, t.TempDir(), dir, s)
	if err := c.Poll(context.Background()); err != nil {
		t.Fatalf("upstream Poll: %v", err)
	}
	srv, _, _ := newTestServer(t, nil, c)
	_, addr := serveHTTP(t, srv, nil)
	return &honestUpstream{dir: dir, cs: cs, settle: s, c: c, srv: srv, url: "http://" + addr}
}

// maxBundleLen is the size of the largest fixture bundle.
func (u *honestUpstream) maxBundleLen(t *testing.T) int64 {
	t.Helper()
	var m int64
	for _, c := range u.cs {
		if n := int64(len(readFile(t, bundlePathOf(u.dir, c)))); n > m {
			m = n
		}
	}
	return m
}

// remoteSettle is a Settlement that accepts every batch of u, with the
// same finality heights.
func (u *honestUpstream) remoteSettle(n int) *fakeSettlement {
	s := newFakeSettlement()
	s.height = 22
	acceptFinal(s, u.cs, n)
	return s
}

// openRemote boots a REMOTE chain for domain 1.
func openRemote(t *testing.T, storage string, s Settlement, upstreams ...string) (*Chain, *RemoteSource, *logSink) {
	t.Helper()
	sink := &logSink{}
	c, err := OpenChain(context.Background(), storage, DomainConfig{DomainID: 1, Source: "REMOTE", Upstreams: upstreams}, s, &fakeSenders{}, sink.logf)
	if err != nil {
		t.Fatalf("OpenChain: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c, c.cfg.Source.(*RemoteSource), sink
}

func pollFor(c *Chain, d time.Duration) error {
	ctx, cancel := context.WithTimeout(context.Background(), d)
	defer cancel()
	return c.Poll(ctx)
}

func requireHead(t *testing.T, c *Chain, latest, finalized int64, status Status) {
	t.Helper()
	h := c.Head()
	if tagID(h.Latest) != latest || tagID(h.Finalized) != finalized || h.Status != status {
		t.Fatalf("head = latest %d finalized %d %s, want latest %d finalized %d %s", tagID(h.Latest), tagID(h.Finalized), h.Status, latest, finalized, status)
	}
}

// testUpstream is an httptest server that counts its requests.
type testUpstream struct {
	*httptest.Server
	reqs atomic.Int64
	mu   sync.Mutex
	acc  []string // Accept-Encoding values seen
}

func (u *testUpstream) acceptEncodings() []string {
	u.mu.Lock()
	defer u.mu.Unlock()
	return append([]string(nil), u.acc...)
}

// newUpstream serves h and counts every request.
func newUpstream(t *testing.T, h func(w http.ResponseWriter, r *http.Request)) *testUpstream {
	t.Helper()
	u := &testUpstream{}
	u.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		u.reqs.Add(1)
		u.mu.Lock()
		u.acc = append(u.acc, r.Header.Get("Accept-Encoding"))
		u.mu.Unlock()
		h(w, r)
	}))
	t.Cleanup(func() {
		u.Server.CloseClientConnections()
		u.Server.Close()
	})
	return u
}

// realGet fetches path from the honest gateway at base.
func realGet(base, path string) (int, []byte) {
	resp, err := http.Get(base + path)
	if err != nil {
		return 502, nil
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, b
}

// proxy serves what the honest gateway at base serves, after edit
// changes the body of a /bundles or /receipts response.
func proxy(t *testing.T, base string, edit func(path string, body []byte) []byte) *testUpstream {
	t.Helper()
	return newUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		code, body := realGet(base, r.URL.Path)
		if edit != nil && code == 200 {
			body = edit(r.URL.Path, body)
		}
		w.WriteHeader(code)
		_, _ = w.Write(body)
	})
}

func flipLast(b []byte) []byte {
	b = append([]byte(nil), b...)
	b[len(b)-1] ^= 0xFF
	return b
}

func isBundle(path string) bool { return strings.HasPrefix(path, "/bundles/") }

// resetConn hijacks the connection and closes it with a reset.
func resetConn(w http.ResponseWriter) {
	conn, _, err := w.(http.Hijacker).Hijack()
	if err != nil {
		return
	}
	if tc, ok := conn.(*net.TCPConn); ok {
		_ = tc.SetLinger(0)
	}
	conn.Close()
}

// zeroReader yields endless zero bytes.
type zeroReader struct{}

func (zeroReader) Read(p []byte) (int, error) {
	clear(p)
	return len(p), nil
}

func noTmp(t *testing.T, storage string) {
	t.Helper()
	for _, d := range []string{"bundles", "receipts"} {
		es, err := os.ReadDir(filepath.Join(storage, "1", d))
		if err != nil {
			t.Fatal(err)
		}
		for _, e := range es {
			if strings.HasPrefix(e.Name(), ".tmp-") {
				t.Fatalf("%s/%s remains", d, e.Name())
			}
		}
	}
}

func TestRemoteFetchLimitPinsBundleCeiling(t *testing.T) {
	if maxFetchBytes != maxBundleBytes {
		t.Fatalf("maxFetchBytes = %d, want maxBundleBytes %d", maxFetchBytes, maxBundleBytes)
	}
}

func rpcBody(method, params string) string {
	return fmt.Sprintf(`{"jsonrpc":"2.0","method":%q,"params":%s,"id":1}`, method, params)
}

func TestRemoteMatchesLocal(t *testing.T) {
	up := newHonestUpstream(t)
	storage := t.TempDir()
	rs := up.remoteSettle(4)
	rc, _, _ := openRemote(t, storage, rs, up.url)
	if err := rc.Poll(context.Background()); err != nil {
		t.Fatalf("REMOTE Poll: %v", err)
	}
	remote, _, _ := newTestServer(t, nil, rc)
	lh, rh := up.c.Head(), rc.Head()
	if tagID(lh.Latest) != 4 || tagID(rh.Latest) != 4 || tagID(lh.Finalized) != 2 || tagID(rh.Finalized) != 2 || rh.Status != StatusOK {
		t.Fatalf("heads: local %+v, remote %+v", lh, rh)
	}

	var calls []string
	add := func(method, params string) { calls = append(calls, rpcBody(method, params)) }
	add("zeno_getBatch", `[1,"latest"]`)
	add("zeno_getBatch", `[1,"finalized"]`)
	var sends []string
	for id := 0; id < 5; id++ {
		add("zeno_getBatch", fmt.Sprintf(`[1,%d]`, id))
		var b batchJSON
		rpcOK(t, up.srv, "zeno_getBatch", fmt.Sprintf(`[1,%d]`, id), &b)
		for _, in := range b.Inputs {
			sends = append(sends, in.SendHash)
		}
	}
	if len(sends) == 0 {
		t.Fatal("history A has no sends")
	}
	for _, h := range sends {
		add("zeno_getReceipt", fmt.Sprintf(`["%s"]`, h))
	}
	for _, tg := range []string{"latest", "finalized"} {
		for _, p := range []trie.Path{pA, pB, pC, pD} {
			add("zeno_getStateAt", fmt.Sprintf(`[1,"%s","%s"]`, pathHex(p), tg))
		}
		add("zeno_getProof", fmt.Sprintf(`[1,["%s","%s","%s"],"%s"]`, pathHex(pA), pathHex(pB), pathHex(pC), tg))
	}
	for _, m := range []string{"zeno_getInputs", "zeno_getEvents", "zeno_getOutbox"} {
		add(m, `[{"domainId":1}]`)
	}
	for _, c := range up.cs {
		add("zeno_getBundle", fmt.Sprintf(`["%s"]`, c.DAHash))
		add("zeno_getReceiptSidecar", fmt.Sprintf(`["%s"]`, c.DAHash))
	}
	add("zeno_chainInfo", `[1]`)
	for _, body := range calls {
		l, r := post(t, up.srv, body).Body.String(), post(t, remote, body).Body.String()
		if l != r {
			t.Fatalf("%s\nlocal:  %s\nremote: %s", body, trunc(l), trunc(r))
		}
	}
	for _, c := range up.cs {
		for _, prefix := range []string{"/bundles/", "/receipts/"} {
			l, r := getFile(t, up.srv, "GET", prefix+c.DAHash.String()), getFile(t, remote, "GET", prefix+c.DAHash.String())
			if r.Code != 200 || !bytes.Equal(l.Body.Bytes(), r.Body.Bytes()) {
				t.Fatalf("GET %s%s: remote status %d, equal %v", prefix, c.DAHash, r.Code, bytes.Equal(l.Body.Bytes(), r.Body.Bytes()))
			}
		}
	}

	head := func(s *Server) map[string]json.RawMessage {
		var m map[string]json.RawMessage
		rpcOK(t, s, "zeno_getHead", `[1]`, &m)
		return m
	}
	lm, rm := head(up.srv), head(remote)
	if string(rm["pending"]) != "null" {
		t.Fatalf("REMOTE pending = %s, want null", rm["pending"])
	}
	delete(lm, "pending")
	delete(rm, "pending")
	lb, _ := json.Marshal(lm)
	rb, _ := json.Marshal(rm)
	if !bytes.Equal(lb, rb) {
		t.Fatalf("zeno_getHead differs:\nlocal:  %s\nremote: %s", lb, rb)
	}
}

func TestRemoteRefusesPending(t *testing.T) {
	up := newHonestUpstream(t)
	rc, _, _ := openRemote(t, t.TempDir(), up.remoteSettle(4), up.url)
	if err := rc.Poll(context.Background()); err != nil {
		t.Fatal(err)
	}
	srv, _, _ := newTestServer(t, nil, rc)
	for method, params := range map[string]string{
		"zeno_getBatch":   `[1,"pending"]`,
		"zeno_getStateAt": fmt.Sprintf(`[1,"%s","pending"]`, pathHex(pA)),
		"zeno_getProof":   fmt.Sprintf(`[1,["%s"],"pending"]`, pathHex(pA)),
		"zeno_getInputs":  `[{"domainId":1,"toBatch":"pending"}]`,
	} {
		requireRPCErr(t, srv, method, params, -32002, noPendingMsg)
	}
	_, addr := serveHTTP(t, srv, nil)
	ws := newWSClient(t, addr)
	r := ws.call("zeno_subscribe", `["events",{"domainId":1},"pending"]`)
	if r.Error == nil || r.Error.Code != -32002 || r.Error.Message != noPendingMsg {
		t.Fatalf("subscribe: %s, %v", r.Result, r.Error)
	}
}

func TestRemoteRestart(t *testing.T) {
	up := newHonestUpstream(t)
	storage := t.TempDir()
	rs := up.remoteSettle(4)
	var down atomic.Bool
	gate := proxyGate(t, up.url, &down)
	rc, _, _ := openRemote(t, storage, rs, gate.URL)
	if err := rc.Poll(context.Background()); err != nil {
		t.Fatal(err)
	}
	requireHead(t, rc, 4, 2, StatusOK)
	closeChain(t, rc)

	down.Store(true)
	bundles, receipts := filepath.Join(storage, "1", "bundles"), filepath.Join(storage, "1", "receipts")
	mkfile(t, filepath.Join(bundles, ".tmp-x"))
	mkfile(t, filepath.Join(receipts, ".tmp-y"))
	rc2, _, _ := openRemote(t, storage, rs, gate.URL)
	for _, p := range []string{filepath.Join(bundles, ".tmp-x"), filepath.Join(receipts, ".tmp-y")} {
		if _, err := os.Stat(p); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("%s survived OpenChain: %v", p, err)
		}
	}
	if err := rc2.Poll(context.Background()); err != nil {
		t.Fatalf("Poll with no upstream: %v", err)
	}
	requireHead(t, rc2, 4, 2, StatusOK)
	if n := gate.reqs.Load(); n != 5*2 {
		t.Fatalf("the upstream saw %d requests, want the 10 of the first sync", n)
	}
}

// proxyGate answers 503 while down is set, else what the honest
// gateway at base serves.
func proxyGate(t *testing.T, base string, down *atomic.Bool) *testUpstream {
	t.Helper()
	return newUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		if down.Load() {
			http.Error(w, "down", http.StatusServiceUnavailable)
			return
		}
		code, body := realGet(base, r.URL.Path)
		w.WriteHeader(code)
		_, _ = w.Write(body)
	})
}

func TestRemoteRotRefetches(t *testing.T) {
	up := newHonestUpstream(t)
	storage := t.TempDir()
	rs := up.remoteSettle(4)
	var down atomic.Bool
	gate := proxyGate(t, up.url, &down)
	rc, _, _ := openRemote(t, storage, rs, gate.URL)
	if err := rc.Poll(context.Background()); err != nil {
		t.Fatal(err)
	}
	closeChain(t, rc)

	path := filepath.Join(storage, "1", "bundles", filepath.Base(bundlePathOf(up.dir, up.cs[3])))
	want := readFile(t, path)
	flipLastByte(t, path)
	down.Store(true)
	rc2, _, sink := openRemote(t, storage, rs, gate.URL)
	if err := rc2.Poll(context.Background()); err != nil {
		t.Fatalf("Poll: %v", err)
	}
	h := rc2.Head()
	if h.Status != StatusSyncing || tagID(h.Latest) != 2 {
		t.Fatalf("head = latest %d %s, want latest 2 SYNCING\n%s", tagID(h.Latest), h.Status, sink.dump())
	}
	if sink.count("ALARM") != 0 {
		t.Fatalf("a rotted file raised an alarm:\n%s", sink.dump())
	}
	down.Store(false)
	if err := rc2.Poll(context.Background()); err != nil {
		t.Fatal(err)
	}
	requireHead(t, rc2, 4, 2, StatusOK)
	if !bytes.Equal(readFile(t, path), want) {
		t.Fatal("the bundle was not fetched again")
	}
}

func TestRemoteTamperingUpstreamsStayInRotation(t *testing.T) {
	up := newHonestUpstream(t)
	storage := t.TempDir()
	badSidecars := proxy(t, up.url, func(p string, b []byte) []byte {
		if isBundle(p) {
			return b
		}
		b = append([]byte(nil), b...)
		b[len(b)/2] ^= 0xFF
		return b
	})
	badBundles := proxy(t, up.url, func(p string, b []byte) []byte {
		if isBundle(p) {
			return flipLast(b)
		}
		return b
	})
	rc, _, sink := openRemote(t, storage, up.remoteSettle(4), badSidecars.URL, badBundles.URL)
	if err := rc.Poll(context.Background()); err != nil {
		t.Fatal(err)
	}
	requireHead(t, rc, 4, 2, StatusOK)
	if n := sink.count("upstreams[0]", "receipt sidecar"); n != 1 {
		t.Fatalf("skip lines for upstreams[0] sidecar = %d, want 1:\n%s", n, sink.dump())
	}
	if n := sink.count("upstreams[1]", "for bundle"); n != 1 {
		t.Fatalf("skip lines for upstreams[1] bundle = %d, want 1:\n%s", n, sink.dump())
	}
	if n := sink.count("skipped upstreams"); n != 2 {
		t.Fatalf("skip lines = %d, want 2:\n%s", n, sink.dump())
	}
	if sink.count("ALARM") != 0 {
		t.Fatal("the domain went INCONSISTENT")
	}
}

func TestRemoteHostileUpstreams(t *testing.T) {
	up := newHonestUpstream(t)
	storage := t.TempDir()
	limit := up.maxBundleLen(t)

	chunked := newUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		if !isBundle(r.URL.Path) {
			code, body := realGet(up.url, r.URL.Path)
			w.WriteHeader(code)
			_, _ = w.Write(body)
			return
		}
		w.WriteHeader(200)
		w.(http.Flusher).Flush()
		_, _ = io.CopyN(w, zeroReader{}, limit+1)
	})
	declared := newUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		conn, buf, err := w.(http.Hijacker).Hijack()
		if err != nil {
			return
		}
		defer conn.Close()
		fmt.Fprintf(buf, "HTTP/1.1 200 OK\r\nContent-Length: %d\r\n\r\n", int64(maxFetchBytes)+1)
		_ = buf.Flush()
	})
	rc, rsrc, sink := openRemote(t, storage, up.remoteSettle(0), chunked.URL, declared.URL, up.url)
	rsrc.maxBundle = limit
	if err := rc.Poll(context.Background()); err != nil {
		t.Fatal(err)
	}
	requireHead(t, rc, 0, 0, StatusOK)
	if sink.count("upstreams[0]", "the body passed the") != 1 || sink.count("upstreams[1]", "Content-Length") != 1 {
		t.Fatalf("skip lines:\n%s", sink.dump())
	}
	noTmp(t, storage)
}

func TestRemoteRefusesRedirectsAndPartialStatuses(t *testing.T) {
	up := newHonestUpstream(t)
	storage := t.TempDir()
	third := newUpstream(t, func(w http.ResponseWriter, r *http.Request) { http.Error(w, "no", 500) })
	redirect := newUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, third.URL+r.URL.Path, http.StatusFound)
	})
	partial := newUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusPartialContent)
		_, _ = w.Write([]byte("x"))
	})
	notModified := newUpstream(t, func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusNotModified) })
	rc, _, sink := openRemote(t, storage, up.remoteSettle(0), redirect.URL, partial.URL, notModified.URL, up.url)
	if err := rc.Poll(context.Background()); err != nil {
		t.Fatal(err)
	}
	requireHead(t, rc, 0, 0, StatusOK)
	if n := third.reqs.Load(); n != 0 {
		t.Fatalf("the redirect target saw %d requests", n)
	}
	for i, status := range []string{"HTTP status 302", "HTTP status 206", "HTTP status 304"} {
		if sink.count(fmt.Sprintf("upstreams[%d]", i), status) != 1 {
			t.Fatalf("no skip line for upstreams[%d] with %q:\n%s", i, status, sink.dump())
		}
	}
}

func TestRemoteRefusesLargeHeadersAndCompression(t *testing.T) {
	up := newHonestUpstream(t)
	big := newUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Big", strings.Repeat("a", 70<<10))
		w.WriteHeader(200)
	})
	honest := proxy(t, up.url, nil)
	rc, _, sink := openRemote(t, t.TempDir(), up.remoteSettle(0), big.URL, honest.URL)
	if err := rc.Poll(context.Background()); err != nil {
		t.Fatal(err)
	}
	requireHead(t, rc, 0, 0, StatusOK)
	if sink.count("upstreams[0]", "exceeded") != 1 {
		t.Fatalf("the 70 KiB header was not skipped:\n%s", sink.dump())
	}
	for _, v := range honest.acceptEncodings() {
		if v != "" {
			t.Fatalf("a request carried Accept-Encoding %q", v)
		}
	}
	if honest.reqs.Load() == 0 {
		t.Fatal("the honest upstream saw no request")
	}
}

func TestRemoteStalledUpstreams(t *testing.T) {
	up := newHonestUpstream(t)
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })
	block := func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-r.Context().Done():
		case <-release:
		}
	}
	for name, h := range map[string]func(w http.ResponseWriter, r *http.Request){
		"no headers": block,
		"no body": func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Length", "1000")
			w.WriteHeader(200)
			w.(http.Flusher).Flush()
			block(w, r)
		},
	} {
		t.Run(name, func(t *testing.T) {
			storage := t.TempDir()
			stall := newUpstream(t, h)
			rc, rsrc, sink := openRemote(t, storage, up.remoteSettle(4), stall.URL, up.url)
			rsrc.stallWait = 200 * time.Millisecond
			if err := pollFor(rc, 5*time.Second); err != nil {
				t.Fatalf("Poll: %v", err)
			}
			requireHead(t, rc, 4, 2, StatusOK)
			if sink.count("upstreams[0]", "no bytes arrived for 200ms") == 0 {
				t.Fatalf("no stall skip line:\n%s", sink.dump())
			}
			noTmp(t, storage)
		})
	}
}

// hijackTrickle answers 200 for a large body and writes one byte every
// step, until total has passed since the request, then resets the
// connection when reset is true.
func hijackTrickle(w http.ResponseWriter, r *http.Request, step, total time.Duration, reset bool) {
	conn, buf, err := w.(http.Hijacker).Hijack()
	if err != nil {
		return
	}
	defer conn.Close()
	fmt.Fprintf(buf, "HTTP/1.1 200 OK\r\nContent-Length: %d\r\n\r\n", 1<<20)
	_ = buf.Flush()
	start := time.Now()
	for time.Since(start) < total {
		if _, err := conn.Write([]byte{0}); err != nil {
			return
		}
		time.Sleep(step)
	}
	if reset {
		if tc, ok := conn.(*net.TCPConn); ok {
			_ = tc.SetLinger(0)
		}
	}
}

// slowHonest proxies the honest gateway and delays only its first
// response by delay.
func slowHonest(t *testing.T, base string, delay time.Duration) *testUpstream {
	t.Helper()
	var first atomic.Bool
	return newUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		if first.CompareAndSwap(false, true) {
			time.Sleep(delay)
		}
		code, body := realGet(base, r.URL.Path)
		w.WriteHeader(code)
		_, _ = w.Write(body)
	})
}

func requireDeadline(t *testing.T, err error) {
	t.Helper()
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Poll error = %v, want a deadline error", err)
	}
}

func TestRemoteRotationAfterSlowFailures(t *testing.T) {
	up := newHonestUpstream(t)
	slow := func() *testUpstream {
		return newUpstream(t, func(w http.ResponseWriter, r *http.Request) {
			time.Sleep(900 * time.Millisecond)
			http.Error(w, "busy", http.StatusServiceUnavailable)
		})
	}
	s0, s1, s2 := slow(), slow(), slow()
	h := slowHonest(t, up.url, 500*time.Millisecond)
	rc, rsrc, sink := openRemote(t, t.TempDir(), up.remoteSettle(4), s0.URL, s1.URL, s2.URL, h.URL)
	rsrc.stallWait = time.Second
	requireDeadline(t, pollFor(rc, 3*time.Second))
	if rsrc.first != 3 {
		t.Fatalf("first = %d after the cut poll, want 3", rsrc.first)
	}
	if sink.count("did not finish within") != 0 {
		t.Fatalf("a cut that started late logged the cut-fetch line:\n%s", sink.dump())
	}
	if err := pollFor(rc, 10*time.Second); err != nil {
		t.Fatalf("second Poll: %v", err)
	}
	requireHead(t, rc, 4, 2, StatusOK)
	for i, s := range []*testUpstream{s0, s1, s2} {
		if n := s.reqs.Load(); n != 1 {
			t.Fatalf("slow upstream %d received %d requests, want 1", i, n)
		}
	}
}

func TestRemoteRotationAfterHostileTrickle(t *testing.T) {
	up := newHonestUpstream(t)
	s := newUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		hijackTrickle(w, r, 200*time.Millisecond, 2700*time.Millisecond, true)
	})
	h := slowHonest(t, up.url, 500*time.Millisecond)
	rc, rsrc, sink := openRemote(t, t.TempDir(), up.remoteSettle(4), s.URL, h.URL)
	rsrc.stallWait = time.Second
	requireDeadline(t, pollFor(rc, 3*time.Second))
	if rsrc.first != 1 {
		t.Fatalf("first = %d after the cut poll, want 1", rsrc.first)
	}
	if sink.count("did not finish within") != 0 {
		t.Fatalf("the cut-fetch line was logged:\n%s", sink.dump())
	}
	if err := pollFor(rc, 10*time.Second); err != nil {
		t.Fatalf("second Poll: %v", err)
	}
	requireHead(t, rc, 4, 2, StatusOK)
	if n := s.reqs.Load(); n != 1 {
		t.Fatalf("the trickling upstream received %d requests, want 1", n)
	}
}

func TestRemoteRotationMovesPastFailures(t *testing.T) {
	newS := func() *testUpstream {
		return newUpstream(t, func(w http.ResponseWriter, r *http.Request) {
			time.Sleep(500 * time.Millisecond)
			http.Error(w, "busy", http.StatusServiceUnavailable)
		})
	}
	t.Run("one poll", func(t *testing.T) {
		up := newHonestUpstream(t)
		s := newS()
		rc, rsrc, _ := openRemote(t, t.TempDir(), up.remoteSettle(4), s.URL, up.url)
		rsrc.stallWait = time.Second
		if err := pollFor(rc, 10*time.Second); err != nil {
			t.Fatal(err)
		}
		requireHead(t, rc, 4, 2, StatusOK)
		if n := s.reqs.Load(); n != 1 {
			t.Fatalf("the failing upstream received %d requests, want 1", n)
		}
	})
	t.Run("after a cache fault", func(t *testing.T) {
		if os.Geteuid() == 0 {
			t.Skip("root ignores directory modes")
		}
		up := newHonestUpstream(t)
		s := newS()
		storage := t.TempDir()
		rc, rsrc, _ := openRemote(t, storage, up.remoteSettle(4), s.URL, up.url)
		rsrc.stallWait = time.Second
		bundles := filepath.Join(storage, "1", "bundles")
		if err := os.Chmod(bundles, 0o500); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = os.Chmod(bundles, 0o700) })
		var cf *cacheFault
		if err := pollFor(rc, 10*time.Second); !errors.As(err, &cf) {
			t.Fatalf("Poll error = %v, want a cache fault", err)
		}
		if err := os.Chmod(bundles, 0o700); err != nil {
			t.Fatal(err)
		}
		if err := pollFor(rc, 10*time.Second); err != nil {
			t.Fatal(err)
		}
		requireHead(t, rc, 4, 2, StatusOK)
		if n := s.reqs.Load(); n != 1 {
			t.Fatalf("the failing upstream received %d requests across both polls, want 1", n)
		}
	})
}

func TestRemoteTricklerIsSkippedNextPoll(t *testing.T) {
	up := newHonestUpstream(t)
	trickle := newUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		hijackTrickle(w, r, 100*time.Millisecond, 5*time.Second, false)
	})
	rc, rsrc, _ := openRemote(t, t.TempDir(), up.remoteSettle(4), trickle.URL, up.url)
	rsrc.stallWait = 500 * time.Millisecond
	requireDeadline(t, pollFor(rc, 300*time.Millisecond))
	before := trickle.reqs.Load()
	_ = pollFor(rc, 300*time.Millisecond)
	if rc.Head().Latest == nil {
		t.Fatal("the second poll moved nothing")
	}
	if n := trickle.reqs.Load(); n != before {
		t.Fatalf("the trickler received %d requests during the second poll", n-before)
	}
}

func TestRemoteLogsOncePerReason(t *testing.T) {
	up := newHonestUpstream(t)
	t.Run("tampering", func(t *testing.T) {
		bad := proxy(t, up.url, func(p string, b []byte) []byte { return flipLast(b) })
		rc, _, sink := openRemote(t, t.TempDir(), up.remoteSettle(4), bad.URL)
		for i := 0; i < 2; i++ {
			if err := rc.Poll(context.Background()); err != nil {
				t.Fatal(err)
			}
		}
		if rc.Head().Status != StatusSyncing || rc.Head().Latest != nil {
			t.Fatalf("head = %+v", rc.Head())
		}
		if n := sink.count("no upstream served the batch's files"); n != 1 {
			t.Fatalf("all-upstreams-fail lines = %d, want 1:\n%s", n, sink.dump())
		}
		if n := sink.count("skipped upstreams[0]"); n != 1 {
			t.Fatalf("skip lines = %d, want 1:\n%s", n, sink.dump())
		}
	})
	t.Run("userinfo", func(t *testing.T) {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		port := l.Addr().(*net.TCPAddr).Port
		l.Close()
		rc, _, sink := openRemote(t, t.TempDir(), up.remoteSettle(4), fmt.Sprintf("http://user:secret@127.0.0.1:%d", port))
		for i := 0; i < 2; i++ {
			if err := rc.Poll(context.Background()); err != nil {
				t.Fatal(err)
			}
		}
		if sink.count("skipped upstreams[0]") == 0 {
			t.Fatalf("no skip line:\n%s", sink.dump())
		}
		if sink.count("secret") != 0 || sink.count("user:") != 0 {
			t.Fatalf("a log line holds the userinfo:\n%s", sink.dump())
		}
	})
	t.Run("ports are masked", func(t *testing.T) {
		reset := newUpstream(t, func(w http.ResponseWriter, r *http.Request) { resetConn(w) })
		rc, _, sink := openRemote(t, t.TempDir(), up.remoteSettle(4), reset.URL)
		for i := 0; i < 3; i++ {
			if err := rc.Poll(context.Background()); err != nil {
				t.Fatal(err)
			}
		}
		if n := sink.count("skipped upstreams[0]"); n != 1 {
			t.Fatalf("skip lines = %d across three polls, want 1:\n%s", n, sink.dump())
		}
	})
}

func TestRemoteCacheFaultIsNotAnUpstreamFault(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root ignores directory modes")
	}
	up := newHonestUpstream(t)
	storage := t.TempDir()
	rc, _, sink := openRemote(t, storage, up.remoteSettle(4), up.url)
	bundles := filepath.Join(storage, "1", "bundles")
	if err := os.Chmod(bundles, 0o500); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(bundles, 0o700) })
	var texts []string
	for i := 0; i < 2; i++ {
		err := rc.Poll(context.Background())
		var cf *cacheFault
		if !errors.As(err, &cf) {
			t.Fatalf("Poll %d error = %v, want a cache fault", i, err)
		}
		texts = append(texts, err.Error())
	}
	if texts[0] != texts[1] || strings.Contains(texts[0], ".tmp-") {
		t.Fatalf("error texts %q", texts)
	}
	if sink.count("skipped upstreams") != 0 || sink.count("no upstream served") != 0 {
		t.Fatalf("a cache fault was logged as an upstream fault:\n%s", sink.dump())
	}
	if err := os.Chmod(bundles, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := rc.Poll(context.Background()); err != nil {
		t.Fatal(err)
	}
	requireHead(t, rc, 4, 2, StatusOK)
}

// slowBundles proxies the honest gateway and, for the bundles whose
// daHash is in slow, sends half the body, waits 500 ms and sends the
// rest.
type slowBundles struct {
	*testUpstream
	mu   sync.Mutex
	slow map[string]bool
}

func (s *slowBundles) setSlow(h types.Hash, v bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.slow[h.String()] = v
}

func TestRemoteCutFetchLine(t *testing.T) {
	up := newHonestUpstream(t)
	sb := &slowBundles{slow: map[string]bool{}}
	sb.testUpstream = newUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		code, body := realGet(up.url, r.URL.Path)
		sb.mu.Lock()
		slow := sb.slow[strings.TrimPrefix(r.URL.Path, "/bundles/")]
		sb.mu.Unlock()
		if code != 200 || !slow || len(body) < 2 {
			w.WriteHeader(code)
			_, _ = w.Write(body)
			return
		}
		w.Header().Set("Content-Length", fmt.Sprint(len(body)))
		w.WriteHeader(200)
		_, _ = w.Write(body[:len(body)/2])
		w.(http.Flusher).Flush()
		select {
		case <-time.After(500 * time.Millisecond):
		case <-r.Context().Done():
			return
		}
		_, _ = w.Write(body[len(body)/2:])
	})
	storage := t.TempDir()
	rs := newFakeSettlement()
	rs.height = 10
	rs.accept(up.cs[0], 20)
	rs.accept(up.cs[1], 21)
	u := strings.Replace(sb.URL, "http://", "http://user:secret@", 1)
	rc, _, sink := openRemote(t, storage, rs, u)
	sb.setSlow(up.cs[1].DAHash, true)
	lineFor := func(id int) int {
		return sink.count(fmt.Sprintf("gateway: domain 1 batch %d: the bundle fetch from http://127.0.0.1:", id), " did not finish within the 30s poll slice; a bundle this large needs a faster upstream")
	}

	// 1: batch 0 is accepted, the cut on batch 1 is not the poll's
	// first download attempt.
	requireDeadline(t, pollFor(rc, 300*time.Millisecond))
	requireHead(t, rc, 0, -1, StatusSyncing)
	if lineFor(1) != 0 {
		t.Fatalf("a cut after an accepted batch logged the line:\n%s", sink.dump())
	}
	// 2: a cancelled poll is not a slice cut.
	ctx, cancel := context.WithCancel(context.Background())
	time.AfterFunc(300*time.Millisecond, cancel)
	if err := rc.Poll(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled Poll error = %v", err)
	}
	if lineFor(1) != 0 {
		t.Fatalf("a cancelled poll logged the line:\n%s", sink.dump())
	}
	// 3: the line is logged once per batch.
	for i := 0; i < 3; i++ {
		requireDeadline(t, pollFor(rc, 300*time.Millisecond))
		requireHead(t, rc, 0, -1, StatusSyncing)
	}
	if lineFor(1) != 1 {
		t.Fatalf("cut-fetch lines for batch 1 = %d, want 1:\n%s", lineFor(1), sink.dump())
	}
	if sink.count("secret") != 0 || sink.count("user:") != 0 {
		t.Fatalf("a log line holds the userinfo:\n%s", sink.dump())
	}
	noTmp(t, storage)
	// 4: batch 1 downloads once it is not slowed.
	sb.setSlow(up.cs[1].DAHash, false)
	if err := pollFor(rc, 5*time.Second); err != nil {
		t.Fatal(err)
	}
	requireHead(t, rc, 1, -1, StatusOK)
	// 5: the next batch logs its own line.
	rs.accept(up.cs[2], 22)
	sb.setSlow(up.cs[2].DAHash, true)
	requireDeadline(t, pollFor(rc, 300*time.Millisecond))
	if lineFor(2) != 1 || lineFor(1) != 1 {
		t.Fatalf("cut-fetch lines: batch 1 %d, batch 2 %d, want 1 and 1:\n%s", lineFor(1), lineFor(2), sink.dump())
	}
}

func TestRemoteTriesEachUpstreamOncePerFile(t *testing.T) {
	up := newHonestUpstream(t)
	bad := func() *testUpstream {
		return newUpstream(t, func(w http.ResponseWriter, r *http.Request) { http.Error(w, "no", http.StatusInternalServerError) })
	}
	b0, b1, b2 := bad(), bad(), bad()
	good := proxy(t, up.url, nil)
	rc, rsrc, _ := openRemote(t, t.TempDir(), up.remoteSettle(4), b0.URL, b1.URL, b2.URL, good.URL)
	if err := rc.Poll(context.Background()); err != nil {
		t.Fatal(err)
	}
	requireHead(t, rc, 4, 2, StatusOK)
	// Batch 0 tries 0, 1, 2 and 3; every later file starts at the
	// upstream that served the last one.
	for i, b := range []*testUpstream{b0, b1, b2} {
		if n := b.reqs.Load(); n != 1 {
			t.Fatalf("failing upstream %d received %d requests, want 1", i, n)
		}
	}
	if rsrc.first != 3 {
		t.Fatalf("first = %d, want 3", rsrc.first)
	}
}

func TestRemoteSizeCapBoundary(t *testing.T) {
	up := newHonestUpstream(t)
	size := int64(len(readFile(t, bundlePathOf(up.dir, up.cs[0]))))
	honest := proxy(t, up.url, nil)

	t.Run("at the cap", func(t *testing.T) {
		rc, rsrc, _ := openRemote(t, t.TempDir(), up.remoteSettle(0), honest.URL)
		rsrc.maxBundle = size
		if err := rc.Poll(context.Background()); err != nil {
			t.Fatal(err)
		}
		requireHead(t, rc, 0, 0, StatusOK)
	})
	t.Run("one byte above", func(t *testing.T) {
		storage := t.TempDir()
		rc, rsrc, sink := openRemote(t, storage, up.remoteSettle(0), honest.URL)
		rsrc.maxBundle = size - 1
		if err := rc.Poll(context.Background()); err != nil {
			t.Fatal(err)
		}
		if rc.Head().Latest != nil || sink.count("Content-Length") != 1 {
			t.Fatalf("head %+v, log:\n%s", rc.Head(), sink.dump())
		}
		noTmp(t, storage)
	})
	t.Run("chunked body one byte above", func(t *testing.T) {
		storage := t.TempDir()
		chunked := newUpstream(t, func(w http.ResponseWriter, r *http.Request) {
			code, body := realGet(up.url, r.URL.Path)
			w.WriteHeader(code)
			w.(http.Flusher).Flush()
			_, _ = w.Write(body)
		})
		rc, rsrc, sink := openRemote(t, storage, up.remoteSettle(0), chunked.URL)
		rsrc.maxBundle = size - 1
		if err := rc.Poll(context.Background()); err != nil {
			t.Fatal(err)
		}
		if rc.Head().Latest != nil || sink.count("the body passed the") != 1 {
			t.Fatalf("head %+v, log:\n%s", rc.Head(), sink.dump())
		}
		noTmp(t, storage)
	})
}

func TestRemoteRefusesAnotherBatchsSidecar(t *testing.T) {
	up := newHonestUpstream(t)
	storage := t.TempDir()
	swap := proxy(t, up.url, func(p string, b []byte) []byte {
		if isBundle(p) {
			return b
		}
		_, other := realGet(up.url, "/receipts/"+up.cs[1].DAHash.String())
		return other
	})
	rc, _, sink := openRemote(t, storage, up.remoteSettle(0), swap.URL)
	if err := rc.Poll(context.Background()); err != nil {
		t.Fatal(err)
	}
	if rc.Head().Latest != nil || sink.count("is not receiptRoot") != 1 {
		t.Fatalf("head %+v, log:\n%s", rc.Head(), sink.dump())
	}
	// The verified bundle stays, the unverified sidecar does not.
	es, err := os.ReadDir(filepath.Join(storage, "1", "receipts"))
	if err != nil || len(es) != 0 {
		t.Fatalf("receipts/ holds %d entries, err %v", len(es), err)
	}
}

func TestRemoteRottedSidecarRefetches(t *testing.T) {
	up := newHonestUpstream(t)
	storage := t.TempDir()
	rs := up.remoteSettle(4)
	var down atomic.Bool
	gate := proxyGate(t, up.url, &down)
	rc, _, _ := openRemote(t, storage, rs, gate.URL)
	if err := rc.Poll(context.Background()); err != nil {
		t.Fatal(err)
	}
	closeChain(t, rc)

	path := filepath.Join(storage, "1", "receipts", filepath.Base(sidecarPath(up.dir, up.cs[3])))
	want := readFile(t, path)
	flipLastByte(t, path)
	down.Store(true)
	rc2, _, sink := openRemote(t, storage, rs, gate.URL)
	if err := rc2.Poll(context.Background()); err != nil {
		t.Fatalf("Poll: %v", err)
	}
	if h := rc2.Head(); h.Status != StatusSyncing || tagID(h.Latest) != 2 {
		t.Fatalf("head = latest %d %s, want latest 2 SYNCING\n%s", tagID(h.Latest), h.Status, sink.dump())
	}
	down.Store(false)
	if err := rc2.Poll(context.Background()); err != nil {
		t.Fatal(err)
	}
	requireHead(t, rc2, 4, 2, StatusOK)
	if !bytes.Equal(readFile(t, path), want) {
		t.Fatal("the sidecar was not fetched again")
	}
}

func TestRemoteSkipLinesAreKeyedByKind(t *testing.T) {
	up := newHonestUpstream(t)
	always500 := newUpstream(t, func(w http.ResponseWriter, r *http.Request) { http.Error(w, "no", http.StatusInternalServerError) })
	bundlesOnly := newUpstream(t, func(w http.ResponseWriter, r *http.Request) {
		if !isBundle(r.URL.Path) {
			http.Error(w, "no", http.StatusInternalServerError)
			return
		}
		code, body := realGet(up.url, r.URL.Path)
		w.WriteHeader(code)
		_, _ = w.Write(body)
	})
	rc, _, sink := openRemote(t, t.TempDir(), up.remoteSettle(0), always500.URL, bundlesOnly.URL)
	for i := 0; i < 2; i++ {
		if err := rc.Poll(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	// upstreams[0] fails the bundle, then the sidecar, with the same
	// reason: one line for each kind, none repeated by the second poll.
	if n := sink.count("skipped upstreams[0]", "for bundle"); n != 1 {
		t.Fatalf("bundle skip lines for upstreams[0] = %d, want 1:\n%s", n, sink.dump())
	}
	if n := sink.count("skipped upstreams[0]", "for receipt sidecar"); n != 1 {
		t.Fatalf("sidecar skip lines for upstreams[0] = %d, want 1:\n%s", n, sink.dump())
	}
}
