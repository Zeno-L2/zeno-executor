package gateway

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"mime"
	"mime/multipart"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/zenon-network/go-zenon/common/types"

	"github.com/digitalSloth/go-executor/internal/codec"
)

// getFile sends method to path on s, with the header pairs.
func getFile(t *testing.T, s *Server, method, path string, hdr ...string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(method, path, nil)
	for i := 0; i+1 < len(hdr); i += 2 {
		req.Header.Add(hdr[i], hdr[i+1])
	}
	rec := httptest.NewRecorder()
	s.ServeHTTP(rec, req)
	return rec
}

func requireStatus(t *testing.T, rec *httptest.ResponseRecorder, want int) {
	t.Helper()
	if rec.Code != want {
		t.Fatalf("status %d, want %d: %s", rec.Code, want, trunc(rec.Body.String()))
	}
}

func TestFileRoutesServe(t *testing.T) {
	dir, cs, s, r, _, _ := readRig(t)
	srv, _, _ := newTestServer(t, nil, r.c)
	s.accept(cs[0], 20)
	s.accept(cs[1], 21)
	doPoll(t, r)

	bundle := readFile(t, bundlePathOf(dir, cs[1]))
	sidecar := readFile(t, sidecarPath(dir, cs[1]))
	etag := `"` + cs[1].DAHash.String() + `"`
	url := "/bundles/" + cs[1].DAHash.String()

	rec := getFile(t, srv, "GET", url)
	requireStatus(t, rec, 200)
	h := rec.Header()
	if !bytes.Equal(rec.Body.Bytes(), bundle) || h.Get("Content-Type") != "application/octet-stream" ||
		h.Get("X-Content-Type-Options") != "nosniff" || h.Get("Accept-Ranges") != "bytes" || h.Get("ETag") != etag {
		t.Fatalf("GET bundle: headers %v, body equal %v", h, bytes.Equal(rec.Body.Bytes(), bundle))
	}
	head := getFile(t, srv, "HEAD", url)
	requireStatus(t, head, 200)
	if head.Body.Len() != 0 || head.Header().Get("Content-Length") != fmt.Sprint(len(bundle)) ||
		head.Header().Get("ETag") != etag || head.Header().Get("Accept-Ranges") != "bytes" ||
		head.Header().Get("Content-Type") != "application/octet-stream" {
		t.Fatalf("HEAD: headers %v, body %d bytes", head.Header(), head.Body.Len())
	}
	rec = getFile(t, srv, "GET", "/receipts/"+strings.ToUpper(cs[1].DAHash.String()))
	requireStatus(t, rec, 200)
	if !bytes.Equal(rec.Body.Bytes(), sidecar) {
		t.Fatal("GET receipts: body differs from the sidecar")
	}

	rec = getFile(t, srv, "GET", url, "Range", "bytes=10-19")
	requireStatus(t, rec, 206)
	if got, want := rec.Header().Get("Content-Range"), fmt.Sprintf("bytes 10-19/%d", len(bundle)); got != want {
		t.Fatalf("Content-Range = %q, want %q", got, want)
	}
	if !bytes.Equal(rec.Body.Bytes(), bundle[10:20]) {
		t.Fatal("range body differs")
	}
	rec = getFile(t, srv, "GET", url, "Range", "bytes=10-19", "If-Range", etag)
	requireStatus(t, rec, 206)
	rec = getFile(t, srv, "GET", url, "Range", "bytes=10-19", "If-Range", `"other"`)
	requireStatus(t, rec, 200)
	if !bytes.Equal(rec.Body.Bytes(), bundle) {
		t.Fatal("If-Range mismatch: body is not the whole file")
	}

	// A pending batch (a record, no Settlement entry) is served.
	rec = getFile(t, srv, "GET", "/bundles/"+cs[2].DAHash.String())
	requireStatus(t, rec, 200)
	if !bytes.Equal(rec.Body.Bytes(), readFile(t, bundlePathOf(dir, cs[2]))) {
		t.Fatal("pending bundle differs")
	}
	// A request with an Upgrade header is a file request.
	rec = getFile(t, srv, "GET", url, "Connection", "Upgrade", "Upgrade", "websocket", "Sec-WebSocket-Version", "13", "Sec-WebSocket-Key", "dGhlIHNhbXBsZSBub25jZQ==")
	requireStatus(t, rec, 200)
}

func TestFileRoutesRefusals(t *testing.T) {
	dir, cs, s, r, _, _ := readRig(t)
	srv, sink, _ := newTestServer(t, []string{"https://ok.example"}, r.c)
	s.accept(cs[0], 20)
	s.accept(cs[1], 21)
	doPoll(t, r)
	h1 := cs[1].DAHash.String()

	requireStatus(t, getFile(t, srv, "GET", "/bundles/"+types.Hash{0x99}.String()), 404)
	for _, p := range []string{"/bundles/00", "/bundles/" + h1 + "/x", "/bundles/", "/bundles/" + strings.Repeat("zz", 32)} {
		requireStatus(t, getFile(t, srv, "GET", p), 404)
	}
	rec := getFile(t, srv, "POST", "/bundles/"+h1)
	requireStatus(t, rec, 405)
	if rec.Header().Get("Allow") != "GET, HEAD, OPTIONS" {
		t.Fatalf("Allow = %q", rec.Header().Get("Allow"))
	}

	fill(srv.fileSem)
	requireStatus(t, getFile(t, srv, "GET", "/bundles/"+h1), 503)
	for len(srv.fileSem) > 0 {
		<-srv.fileSem
	}

	rec = getFile(t, srv, "GET", "/bundles/"+h1, "Origin", "https://bad.example")
	requireStatus(t, rec, 403)
	if rec.Header().Get("Vary") != "Origin" {
		t.Fatalf("Vary = %q", rec.Header().Get("Vary"))
	}
	rec = getFile(t, srv, "GET", "/bundles/"+h1, "Origin", "https://ok.example")
	requireStatus(t, rec, 200)
	if rec.Header().Get("Access-Control-Allow-Origin") != "https://ok.example" ||
		rec.Header().Get("Access-Control-Expose-Headers") != "Accept-Ranges, Content-Range, ETag" {
		t.Fatalf("CORS headers %v", rec.Header())
	}
	rec = getFile(t, srv, "OPTIONS", "/receipts/"+h1, "Origin", "https://ok.example")
	requireStatus(t, rec, 204)
	if rec.Header().Get("Access-Control-Allow-Methods") != "GET, HEAD, OPTIONS" ||
		rec.Header().Get("Access-Control-Allow-Headers") != "Range" || rec.Header().Get("Allow") != "GET, HEAD, OPTIONS" {
		t.Fatalf("preflight headers %v", rec.Header())
	}

	// A sidecar that no longer matches receiptRoot.
	rewriteSidecar(t, sidecarPath(dir, cs[0]), func(rs []codec.Receipt) { rs[0].GasUsed++ })
	for i := 0; i < 2; i++ {
		requireStatus(t, getFile(t, srv, "GET", "/receipts/"+cs[0].DAHash.String()), 500)
	}
	if n := sink.count("JSON-RPC GET /receipts: internal error", "no longer matches receiptRoot", cs[0].ReceiptRoot.String()); n != 1 {
		t.Fatalf("sidecar log lines = %d, want 1:\n%s", n, sink.dump())
	}

	// A bundle replaced by a FIFO is an error that never blocks.
	fifo := bundlePathOf(dir, cs[1])
	if err := os.Remove(fifo); err != nil {
		t.Fatal(err)
	}
	if err := syscall.Mkfifo(fifo, 0o644); err != nil {
		t.Fatal(err)
	}
	finishesWithin(t, 5*time.Second, "GET of a FIFO bundle", func() {
		requireStatus(t, getFile(t, srv, "GET", "/bundles/"+h1), 500)
	})
	if n := sink.count("JSON-RPC GET /bundles: internal error", "not a regular file"); n != 1 {
		t.Fatalf("FIFO log lines = %d, want 1:\n%s", n, sink.dump())
	}
	if err := os.Remove(sidecarPath(dir, cs[1])); err != nil {
		t.Fatal(err)
	}
	requireStatus(t, getFile(t, srv, "GET", "/receipts/"+h1), 404)

	// An INCONSISTENT domain answers 503 for the batch it froze at.
	r.c.markInconsistent(1, errors.New("test"))
	before := len(sink.lines)
	rec = getFile(t, srv, "GET", "/bundles/"+h1)
	requireStatus(t, rec, 503)
	if !strings.Contains(rec.Body.String(), "INCONSISTENT") {
		t.Fatalf("body %q does not name INCONSISTENT", rec.Body.String())
	}
	if len(sink.lines) != before {
		t.Fatalf("the 503 logged:\n%s", sink.dump())
	}
}

// genReaderAt generates a deterministic stream.
type genReaderAt struct{}

func genByte(off int64) byte { return byte(off*7 + off>>9) }

func (genReaderAt) ReadAt(p []byte, off int64) (int, error) {
	for i := range p {
		p[i] = genByte(off + int64(i))
	}
	return len(p), nil
}

const genSize = 32 << 20

// timeoutRig serves sendFile of genSize generated bytes on a server
// with 200 ms read and write timeouts, and returns its URL and a
// pointer to the handler's request-context check.
func timeoutRig(t *testing.T, tweak func(*Server)) (string, *atomic.Int32) {
	t.Helper()
	srv, _, _ := newTestServer(t, nil, genesisChain(t))
	if tweak != nil {
		tweak(srv)
	}
	var ctxOK atomic.Int32 // 1 ok, 2 cancelled
	ts := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Some net/http versions leave the server's ReadTimeout set
		// on a GET's connection; setting it here makes every version
		// expire the background read 200 ms in unless sendFile clears
		// the deadline.
		_ = http.NewResponseController(w).SetReadDeadline(time.Now().Add(200 * time.Millisecond))
		srv.sendFile(w, r, types.Hash{1}, io.NewSectionReader(genReaderAt{}, 0, genSize))
		if r.Context().Err() == nil {
			ctxOK.Store(1)
		} else {
			ctxOK.Store(2)
		}
	}))
	ts.Config.ReadTimeout = 200 * time.Millisecond
	ts.Config.WriteTimeout = 200 * time.Millisecond
	ts.Start()
	t.Cleanup(ts.Close)
	return ts.URL, &ctxOK
}

// slowRead reads the response in 256 KiB steps with a 10 ms pause. It
// returns the bytes read, whether the pattern held, and the read error.
func slowRead(t *testing.T, url string) (int64, bool, error) {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	buf := make([]byte, 256<<10)
	var n int64
	for {
		m, err := io.ReadFull(resp.Body, buf)
		for i := 0; i < m; i++ {
			if buf[i] != genByte(n+int64(i)) {
				return n, false, nil
			}
		}
		n += int64(m)
		if err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) && n == genSize {
				err = nil
			}
			return n, true, err
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestFileTransferOutlivesServerTimeouts(t *testing.T) {
	url, ctxOK := timeoutRig(t, nil)
	n, ok, err := slowRead(t, url)
	if err != nil || n != genSize || !ok {
		t.Fatalf("read %d bytes, pattern ok %v, err %v; want %d", n, ok, err, genSize)
	}
	time.Sleep(50 * time.Millisecond)
	if ctxOK.Load() != 1 {
		t.Fatalf("request context check = %d, want the context alive after sendFile", ctxOK.Load())
	}
}

func TestFileTransferMaxCutsResponse(t *testing.T) {
	url, _ := timeoutRig(t, func(s *Server) { s.fileTransferMax = 300 * time.Millisecond })
	n, _, err := slowRead(t, url)
	if n >= genSize && err == nil {
		t.Fatalf("the response ran past fileTransferMax: %d bytes", n)
	}
}

func TestFileShutdownDrains(t *testing.T) {
	s, _, _ := newTestServer(t, nil, genesisChain(t))
	s.fileSem <- struct{}{}
	done := make(chan struct{})
	go func() {
		s.Shutdown(NewHTTPServer(s))
		close(done)
	}()
	select {
	case <-done:
		t.Fatal("Shutdown returned with a file response in flight")
	case <-time.After(100 * time.Millisecond):
	}
	<-s.fileSem
	finishesWithin(t, 2*time.Second, "Shutdown", func() { <-done })
	if rec := getFile(t, s, "GET", "/bundles/"+types.Hash{1}.String()); rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("GET after Shutdown: status %d", rec.Code)
	}
}

func TestFileRangeAndConditionalRequests(t *testing.T) {
	dir, cs, s, r, _, _ := readRig(t)
	srv, _, _ := newTestServer(t, nil, r.c)
	s.accept(cs[0], 20)
	s.accept(cs[1], 21)
	doPoll(t, r)
	bundle := readFile(t, bundlePathOf(dir, cs[1]))
	sidecar := readFile(t, sidecarPath(dir, cs[1]))
	n := len(bundle)
	url := "/bundles/" + cs[1].DAHash.String()
	etag := `"` + cs[1].DAHash.String() + `"`

	// An end past the file is clamped to its last byte.
	rec := getFile(t, srv, "GET", url, "Range", "bytes=10-99999999")
	requireStatus(t, rec, 206)
	if got, want := rec.Header().Get("Content-Range"), fmt.Sprintf("bytes 10-%d/%d", n-1, n); got != want {
		t.Fatalf("Content-Range = %q, want %q", got, want)
	}
	if !bytes.Equal(rec.Body.Bytes(), bundle[10:]) {
		t.Fatal("clamped range body differs")
	}
	// A suffix range.
	rec = getFile(t, srv, "GET", url, "Range", "bytes=-5")
	requireStatus(t, rec, 206)
	if !bytes.Equal(rec.Body.Bytes(), bundle[n-5:]) {
		t.Fatal("suffix range body differs")
	}
	// A start past the file is unsatisfiable.
	rec = getFile(t, srv, "GET", url, "Range", fmt.Sprintf("bytes=%d-", n))
	requireStatus(t, rec, 416)
	if got, want := rec.Header().Get("Content-Range"), fmt.Sprintf("bytes */%d", n); got != want {
		t.Fatalf("416 Content-Range = %q, want %q", got, want)
	}
	// Several ranges are one multipart/byteranges body.
	rec = getFile(t, srv, "GET", url, "Range", "bytes=0-3,10-13")
	requireStatus(t, rec, 206)
	mt, params, err := mime.ParseMediaType(rec.Header().Get("Content-Type"))
	if err != nil || mt != "multipart/byteranges" {
		t.Fatalf("Content-Type = %q, %v", rec.Header().Get("Content-Type"), err)
	}
	mr := multipart.NewReader(rec.Body, params["boundary"])
	for i, want := range [][]byte{bundle[0:4], bundle[10:14]} {
		p, err := mr.NextPart()
		if err != nil {
			t.Fatalf("part %d: %v", i, err)
		}
		got, _ := io.ReadAll(p)
		if !bytes.Equal(got, want) {
			t.Fatalf("part %d differs", i)
		}
	}
	// A conditional GET on the ETag is 304 with no body.
	rec = getFile(t, srv, "GET", url, "If-None-Match", etag)
	requireStatus(t, rec, 304)
	if rec.Body.Len() != 0 {
		t.Fatalf("304 carried %d bytes", rec.Body.Len())
	}
	// HEAD with a range answers the range's headers and no body.
	rec = getFile(t, srv, "HEAD", url, "Range", "bytes=10-19")
	requireStatus(t, rec, 206)
	if rec.Body.Len() != 0 || rec.Header().Get("Content-Length") != "10" || rec.Header().Get("ETag") != etag {
		t.Fatalf("HEAD range: headers %v, body %d bytes", rec.Header(), rec.Body.Len())
	}
	// The sidecar route serves ranges and HEAD too.
	rec = getFile(t, srv, "GET", "/receipts/"+cs[1].DAHash.String(), "Range", "bytes=2-9")
	requireStatus(t, rec, 206)
	if !bytes.Equal(rec.Body.Bytes(), sidecar[2:10]) {
		t.Fatal("sidecar range body differs")
	}
	rec = getFile(t, srv, "HEAD", "/receipts/"+cs[1].DAHash.String())
	requireStatus(t, rec, 200)
	if rec.Header().Get("Content-Length") != fmt.Sprint(len(sidecar)) || rec.Body.Len() != 0 {
		t.Fatalf("HEAD receipts: headers %v", rec.Header())
	}
}

func TestFileRoutesOriginHeadAndPreflight(t *testing.T) {
	_, cs, s, r, _, _ := readRig(t)
	srv, _, _ := newTestServer(t, []string{"https://ok.example"}, r.c)
	s.accept(cs[0], 20)
	doPoll(t, r)
	url := "/bundles/" + cs[0].DAHash.String()

	rec := getFile(t, srv, "HEAD", url, "Origin", "https://ok.example", "Range", "bytes=0-3")
	requireStatus(t, rec, 206)
	if rec.Header().Get("Access-Control-Allow-Origin") != "https://ok.example" {
		t.Fatalf("HEAD headers %v", rec.Header())
	}
	// Preflight from a disallowed origin is refused.
	rec = getFile(t, srv, "OPTIONS", url, "Origin", "https://bad.example", "Access-Control-Request-Method", "GET")
	requireStatus(t, rec, 403)
	if rec.Header().Get("Access-Control-Allow-Origin") != "" {
		t.Fatalf("refused preflight allowed an origin: %v", rec.Header())
	}
	// OPTIONS without an Origin carries no CORS headers.
	rec = getFile(t, srv, "OPTIONS", url)
	requireStatus(t, rec, 204)
	if rec.Header().Get("Allow") != "GET, HEAD, OPTIONS" || rec.Header().Get("Access-Control-Allow-Origin") != "" ||
		rec.Header().Get("Access-Control-Allow-Methods") != "" {
		t.Fatalf("OPTIONS headers %v", rec.Header())
	}
	// A disallowed origin on HEAD is refused before any file read.
	requireStatus(t, getFile(t, srv, "HEAD", url, "Origin", "https://bad.example"), 403)
}

func TestFileTransferSlots(t *testing.T) {
	_, cs, s, r, _, _ := readRig(t)
	srv, _, _ := newTestServer(t, nil, r.c)
	s.accept(cs[0], 20)
	doPoll(t, r)
	url := "/bundles/" + cs[0].DAHash.String()

	if cap(srv.fileSem) != 8 {
		t.Fatalf("file transfer slots = %d, want 8", cap(srv.fileSem))
	}
	// Seven slots held: the eighth request is served, then the slot
	// is free again.
	for i := 0; i < 7; i++ {
		srv.fileSem <- struct{}{}
	}
	requireStatus(t, getFile(t, srv, "GET", url), 200)
	if len(srv.fileSem) != 7 {
		t.Fatalf("slots held after a served request = %d, want 7", len(srv.fileSem))
	}
	srv.fileSem <- struct{}{}
	requireStatus(t, getFile(t, srv, "GET", url), 503)
	requireStatus(t, getFile(t, srv, "HEAD", url), 503)
	if len(srv.fileSem) != 8 {
		t.Fatalf("a refused request changed the slots: %d held", len(srv.fileSem))
	}
	for len(srv.fileSem) > 0 {
		<-srv.fileSem
	}
	// Every outcome releases its slot.
	requireStatus(t, getFile(t, srv, "HEAD", url), 200)
	requireStatus(t, getFile(t, srv, "GET", "/bundles/"+types.Hash{0x99}.String()), 404)
	requireStatus(t, getFile(t, srv, "GET", url, "Range", "bytes=99999999-"), 416)
	if len(srv.fileSem) != 0 {
		t.Fatalf("slots held after finished requests = %d", len(srv.fileSem))
	}
}

// panicReceipts is a Source whose Receipts panics.
type panicReceipts struct{ Source }

func (panicReceipts) Receipts(types.Hash) ([]byte, error) { panic("receipts read") }

func TestFileSlotReleasedOnPanic(t *testing.T) {
	_, cs, s, r, _, _ := readRig(t)
	srv, _, _ := newTestServer(t, nil, r.c)
	s.accept(cs[0], 20)
	doPoll(t, r)
	r.c.cfg.Source = panicReceipts{r.c.cfg.Source}
	func() {
		defer func() {
			if recover() == nil {
				t.Fatal("the read did not panic")
			}
		}()
		getFile(t, srv, "GET", "/receipts/"+cs[0].DAHash.String())
	}()
	if len(srv.fileSem) != 0 {
		t.Fatalf("a panicking transfer held %d slots", len(srv.fileSem))
	}
}

func TestFileSymlinkAndDirectoryBundlesAre500(t *testing.T) {
	dir, cs, s, r, _, _ := readRig(t)
	srv, sink, _ := newTestServer(t, nil, r.c)
	s.accept(cs[0], 20)
	s.accept(cs[1], 21)
	doPoll(t, r)

	// A symlink to a real file is not followed.
	p0 := bundlePathOf(dir, cs[0])
	target := p0 + ".real"
	if err := os.Rename(p0, target); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(target, p0); err != nil {
		t.Fatal(err)
	}
	requireStatus(t, getFile(t, srv, "GET", "/bundles/"+cs[0].DAHash.String()), 500)
	requireStatus(t, getFile(t, srv, "HEAD", "/bundles/"+cs[0].DAHash.String()), 500)

	// A directory in the file's place.
	p1 := bundlePathOf(dir, cs[1])
	if err := os.Remove(p1); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(p1, 0o700); err != nil {
		t.Fatal(err)
	}
	requireStatus(t, getFile(t, srv, "GET", "/bundles/"+cs[1].DAHash.String()), 500)
	if sink.count("JSON-RPC GET /bundles: internal error") == 0 {
		t.Fatalf("no internal error line:\n%s", sink.dump())
	}
	if len(srv.fileSem) != 0 {
		t.Fatalf("slots held = %d", len(srv.fileSem))
	}
}

func TestFileSlotReleasedOnClientAbort(t *testing.T) {
	dir, cs, s, r, _, _ := readRig(t)
	srv, _, _ := newTestServer(t, nil, r.c)
	s.accept(cs[0], 20)
	doPoll(t, r)
	// A sparse file larger than the socket buffers, so the server's
	// write blocks until the client goes away.
	p := bundlePathOf(dir, cs[0])
	if err := os.Truncate(p, 64<<20); err != nil {
		t.Fatal(err)
	}
	_, addr := serveHTTP(t, srv, nil)
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Fprintf(conn, "GET /bundles/%s HTTP/1.1\r\nHost: x\r\n\r\n", cs[0].DAHash)
	buf := make([]byte, 1024)
	if _, err := io.ReadFull(conn, buf); err != nil {
		t.Fatal(err)
	}
	if len(srv.fileSem) != 1 {
		t.Fatalf("slots held mid-transfer = %d, want 1", len(srv.fileSem))
	}
	conn.Close()
	deadline := time.Now().Add(5 * time.Second)
	for len(srv.fileSem) != 0 {
		if time.Now().After(deadline) {
			t.Fatal("the slot was not released after the client left")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestFileWriteDeadlineExtendsPerWrite(t *testing.T) {
	url, _ := timeoutRig(t, func(s *Server) { s.fileWriteWait = 300 * time.Millisecond })
	start := time.Now()
	n, ok, err := slowRead(t, url)
	if err != nil || n != genSize || !ok {
		t.Fatalf("read %d bytes, pattern ok %v, err %v; want %d", n, ok, err, genSize)
	}
	if time.Since(start) < 600*time.Millisecond {
		t.Fatalf("the transfer took %s, too short to outlast one write window", time.Since(start))
	}
}

func TestFileWriteDeadlineStopsStalledClient(t *testing.T) {
	url, _ := timeoutRig(t, func(s *Server) { s.fileWriteWait = 300 * time.Millisecond })
	resp, err := http.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	time.Sleep(1500 * time.Millisecond)
	n, err := io.Copy(io.Discard, resp.Body)
	if err == nil && n >= genSize {
		t.Fatalf("a client that stalled past the write window still received all %d bytes", n)
	}
}

func TestFileRequestWithBodyIsRefused(t *testing.T) {
	srv, _, _ := newTestServer(t, nil, genesisChain(t))
	_, addr := serveHTTP(t, srv, nil)
	for _, method := range []string{"GET", "HEAD"} {
		conn, err := net.Dial("tcp", addr)
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Close()
		fmt.Fprintf(conn, "%s /bundles/%s HTTP/1.1\r\nHost: x\r\nContent-Length: 1\r\n\r\n", method, types.Hash{1})
		_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
		resp, err := http.ReadResponse(bufio.NewReader(conn), nil)
		if err != nil {
			t.Fatalf("%s with an unsent body: %v", method, err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusBadRequest || !resp.Close {
			t.Fatalf("%s with a body: status %d, close %v; want 400 and close", method, resp.StatusCode, resp.Close)
		}
		if len(srv.fileSem) != 0 {
			t.Fatalf("%s with a body held %d slots", method, len(srv.fileSem))
		}
	}
}

func TestFileReadDeadlineBoundedByTransferMax(t *testing.T) {
	srv, _, _ := newTestServer(t, nil, genesisChain(t))
	srv.fileTransferMax = 500 * time.Millisecond
	done := make(chan struct{})
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer close(done)
		srv.sendFile(w, r, types.Hash{1}, bytes.NewReader(make([]byte, 1<<20)))
	}))
	t.Cleanup(ts.Close)
	conn, err := net.Dial("tcp", ts.Listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	// The body is announced and never sent, so net/http's discard of
	// it on the first write waits on the client.
	fmt.Fprint(conn, "GET / HTTP/1.1\r\nHost: x\r\nContent-Length: 1\r\n\r\n")
	go io.Copy(io.Discard, conn)
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("the handler outlived fileTransferMax waiting on the request body")
	}
}

// TestFileBodyRequestsDoNotHoldSlots sends eight requests that
// announce a body and never send it, on real connections, then an
// honest request.
func TestFileBodyRequestsDoNotHoldSlots(t *testing.T) {
	_, cs, s, r, _, _ := readRig(t)
	srv, _, _ := newTestServer(t, nil, r.c)
	s.accept(cs[0], 20)
	doPoll(t, r)
	_, addr := serveHTTP(t, srv, nil)
	path := "/bundles/" + cs[0].DAHash.String()

	read := func(conn net.Conn, method string) *http.Response {
		t.Helper()
		_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
		resp, err := http.ReadResponse(bufio.NewReader(conn), &http.Request{Method: method})
		if err != nil {
			t.Fatalf("%s: %v", method, err)
		}
		return resp
	}
	dial := func() net.Conn {
		t.Helper()
		conn, err := net.Dial("tcp", addr)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { conn.Close() })
		return conn
	}

	variants := []struct {
		name, method, framing string
	}{
		{"GET content-length", "GET", "Content-Length: 1\r\n"},
		{"HEAD content-length", "HEAD", "Content-Length: 1\r\n"},
		{"GET chunked", "GET", "Transfer-Encoding: chunked\r\n"},
	}
	for _, v := range variants {
		t.Run(v.name, func(t *testing.T) {
			var conns []net.Conn
			for i := 0; i < 8; i++ {
				conn := dial()
				conns = append(conns, conn)
				fmt.Fprintf(conn, "%s %s HTTP/1.1\r\nHost: x\r\n%s\r\n", v.method, path, v.framing)
			}
			for i, conn := range conns {
				resp := read(conn, v.method)
				resp.Body.Close()
				if resp.StatusCode != http.StatusBadRequest {
					t.Fatalf("request %d: status %d, want 400", i, resp.StatusCode)
				}
			}
			if len(srv.fileSem) != 0 {
				t.Fatalf("%d slots held", len(srv.fileSem))
			}
			conn := dial()
			fmt.Fprintf(conn, "GET %s HTTP/1.1\r\nHost: x\r\n\r\n", path)
			resp := read(conn, "GET")
			defer resp.Body.Close()
			if resp.StatusCode != http.StatusOK {
				t.Fatalf("honest request: status %d, want 200", resp.StatusCode)
			}
		})
	}

	t.Run("content-length zero", func(t *testing.T) {
		for _, method := range []string{"GET", "HEAD"} {
			conn := dial()
			fmt.Fprintf(conn, "%s %s HTTP/1.1\r\nHost: x\r\nContent-Length: 0\r\n\r\n", method, path)
			resp := read(conn, method)
			resp.Body.Close()
			if resp.StatusCode != http.StatusOK {
				t.Fatalf("%s with Content-Length 0: status %d, want 200", method, resp.StatusCode)
			}
		}
	})
}
