package gateway

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

// wsItem is one message a wsClient read, or the error that ended its
// reads.
type wsItem struct {
	msg []byte
	err error
}

// wsClient reads a WebSocket connection on its own goroutine, so a
// test can wait for a message with a deadline without breaking the
// connection.
type wsClient struct {
	t    *testing.T
	conn *websocket.Conn
	in   chan wsItem
}

// serveHTTP serves s on a loopback listener with the gateway's HTTP
// server, after tweak changes it, and returns the server and the
// address. The cleanup closes every WebSocket connection first.
func serveHTTP(t *testing.T, s *Server, tweak func(*http.Server)) (*http.Server, string) {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	hs := NewHTTPServer(s)
	if tweak != nil {
		tweak(hs)
	}
	go func() { _ = hs.Serve(l) }()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		s.closeWS(ctx)
		_ = hs.Close()
	})
	return hs, l.Addr().String()
}

// dialWS dials the gateway at addr with the extra request headers.
func dialWS(addr string, hdr http.Header) (*websocket.Conn, *http.Response, error) {
	return websocket.DefaultDialer.Dial("ws://"+addr+"/", hdr)
}

// newWSClient dials addr and starts reading.
func newWSClient(t *testing.T, addr string) *wsClient {
	t.Helper()
	conn, _, err := dialWS(addr, nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	c := &wsClient{t: t, conn: conn, in: make(chan wsItem, 4096)}
	go func() {
		for {
			_, msg, err := conn.ReadMessage()
			c.in <- wsItem{msg, err}
			if err != nil {
				return
			}
		}
	}()
	t.Cleanup(func() { _ = conn.Close() })
	return c
}

// recv returns the next item, or false when none arrives within d.
func (c *wsClient) recv(d time.Duration) (wsItem, bool) {
	select {
	case it := <-c.in:
		return it, true
	case <-time.After(d):
		return wsItem{}, false
	}
}

// next returns the next message, failing the test on an error or when
// none arrives within 5 s.
func (c *wsClient) next() []byte {
	c.t.Helper()
	it, ok := c.recv(5 * time.Second)
	if !ok {
		c.t.Fatal("no message within 5 s")
	}
	if it.err != nil {
		c.t.Fatalf("read: %v", it.err)
	}
	return it.msg
}

// none requires that no item arrives within d.
func (c *wsClient) none(d time.Duration) {
	c.t.Helper()
	if it, ok := c.recv(d); ok {
		c.t.Fatalf("unexpected item: %s %v", trunc(string(it.msg)), it.err)
	}
}

// send writes one text message.
func (c *wsClient) send(msg string) {
	c.t.Helper()
	if err := c.conn.WriteMessage(websocket.TextMessage, []byte(msg)); err != nil {
		c.t.Fatalf("write: %v", err)
	}
}

// call sends one request with id 1 and returns its response, which
// must be the next message.
func (c *wsClient) call(method, params string) rpcResp {
	c.t.Helper()
	c.send(fmt.Sprintf(`{"jsonrpc":"2.0","method":%q,"params":%s,"id":1}`, method, params))
	var r rpcResp
	msg := c.next()
	if err := json.Unmarshal(msg, &r); err != nil || r.ID == nil {
		c.t.Fatalf("%s %s: not a response: %s", method, params, trunc(string(msg)))
	}
	return r
}

// subscribeOK subscribes and returns the subscription id.
func (c *wsClient) subscribeOK(params string) string {
	c.t.Helper()
	r := c.call("zeno_subscribe", params)
	var id string
	if r.Error != nil || json.Unmarshal(r.Result, &id) != nil {
		c.t.Fatalf("zeno_subscribe %s: %+v %s", params, r.Error, r.Result)
	}
	return id
}

// noteMsg is one decoded notification.
type noteMsg struct {
	Method string `json:"method"`
	Params struct {
		Subscription string          `json:"subscription"`
		Result       json.RawMessage `json:"result"`
	} `json:"params"`
}

// note returns the next message, which must be a notification.
func (c *wsClient) note() noteMsg {
	c.t.Helper()
	msg := c.next()
	var n noteMsg
	if err := json.Unmarshal(msg, &n); err != nil || n.Method != "zeno_subscription" {
		c.t.Fatalf("not a notification: %s", trunc(string(msg)))
	}
	return n
}

// requireClose requires the next item to be a close error with code.
func (c *wsClient) requireClose(code int) {
	c.t.Helper()
	it, ok := c.recv(5 * time.Second)
	if !ok {
		c.t.Fatal("no close within 5 s")
	}
	var ce *websocket.CloseError
	if !errors.As(it.err, &ce) || ce.Code != code {
		c.t.Fatalf("item %s %v, want close %d", trunc(string(it.msg)), it.err, code)
	}
}

// echoReq is a test_echo request with id n.
func echoReq(n int) string {
	return fmt.Sprintf(`{"jsonrpc":"2.0","method":"test_echo","params":[%d],"id":%d}`, n, n)
}

func TestWSRequests(t *testing.T) {
	s, _, _ := newTestServer(t, nil, genesisChain(t))
	_, addr := serveHTTP(t, s, nil)
	c := newWSClient(t, addr)

	viaHandle := func(body string) string {
		var buf bytes.Buffer
		if _, err := s.handle(context.Background(), nil, []byte(body), &buf); err != nil {
			t.Fatal(err)
		}
		return buf.String()
	}
	single := echoReq(1)
	c.send(single)
	if got, want := string(c.next()), viaHandle(single); got != want {
		t.Fatalf("single = %s, want %s", got, want)
	}
	batch := "[" + echoReq(1) + "," + echoReq(2) + `,{"jsonrpc":"2.0","method":"nope","id":3}]`
	c.send(batch)
	if got, want := string(c.next()), viaHandle(batch); got != want {
		t.Fatalf("batch = %s, want %s", got, want)
	}

	c.send(`{"jsonrpc":"2.0","method":"test_count"}`)
	c.send(echoReq(5))
	if got := string(c.next()); got != okResp("5", "[5]") {
		t.Fatalf("after a notification: %s", got)
	}

	var over []string
	for i := 0; i < 101; i++ {
		over = append(over, echoReq(i))
	}
	c.send("[" + strings.Join(over, ",") + "]")
	if got := string(c.next()); got != errResp("null", -32600, "batch exceeds 100 requests") {
		t.Fatalf("101-request batch: %s", got)
	}
}

func TestWSOrigin(t *testing.T) {
	s, _, _ := newTestServer(t, []string{"https://ok.example"}, genesisChain(t))
	_, addr := serveHTTP(t, s, nil)
	for _, origin := range []string{"", "https://ok.example"} {
		hdr := http.Header{}
		if origin != "" {
			hdr.Set("Origin", origin)
		}
		conn, _, err := dialWS(addr, hdr)
		if err != nil {
			t.Fatalf("origin %q: %v", origin, err)
		}
		_ = conn.Close()
	}
	for _, origin := range []string{"https://evil.example", "null"} {
		hdr := http.Header{}
		hdr.Set("Origin", origin)
		conn, resp, err := dialWS(addr, hdr)
		if err == nil {
			_ = conn.Close()
			t.Fatalf("origin %q upgraded", origin)
		}
		if resp == nil || resp.StatusCode != http.StatusForbidden {
			t.Fatalf("origin %q: response %+v, want 403", origin, resp)
		}
	}
}

func TestWSBounds(t *testing.T) {
	t.Run("message size", func(t *testing.T) {
		s, _, _ := newTestServer(t, nil, genesisChain(t))
		_, addr := serveHTTP(t, s, nil)
		c := newWSClient(t, addr)
		// Only a frame header declaring 1 MiB + 1 bytes goes out: a
		// final text frame (0x81), the masked 64-bit length (0xFF),
		// the length and a mask key. The server closes at the header,
		// so nothing is left unread.
		hdr := []byte{0x81, 0xFF}
		hdr = binary.BigEndian.AppendUint64(hdr, maxRequestBytes+1)
		hdr = append(hdr, 1, 2, 3, 4)
		if _, err := c.conn.NetConn().Write(hdr); err != nil {
			t.Fatal(err)
		}
		c.requireClose(websocket.CloseMessageTooBig)
	})
	t.Run("connections", func(t *testing.T) {
		s, _, _ := newTestServer(t, nil, genesisChain(t))
		s.wsConns = 1
		_, addr := serveHTTP(t, s, nil)
		newWSClient(t, addr)
		conn, resp, err := dialWS(addr, nil)
		if err == nil {
			_ = conn.Close()
			t.Fatal("a second connection upgraded")
		}
		if resp == nil || resp.StatusCode != http.StatusServiceUnavailable {
			t.Fatalf("second connection: %+v, want 503", resp)
		}
	})
}

func TestEnqueueFull(t *testing.T) {
	c := &wsConn{
		queue:  make(chan note, 2),
		quit:   make(chan struct{}),
		cancel: func() {},
	}
	sub := &subscription{conn: c}
	c.enqueue(note{sub, []byte("1")})
	c.enqueue(note{sub, []byte("2")})
	select {
	case <-c.quit:
		t.Fatal("quit closed before the queue was full")
	default:
	}
	c.enqueue(note{sub, []byte("3")})
	select {
	case <-c.quit:
	default:
		t.Fatal("a full queue did not end the connection")
	}
	if c.code != websocket.ClosePolicyViolation || c.text != "notification queue full" {
		t.Fatalf("close = %d %q", c.code, c.text)
	}
	done := make(chan struct{})
	go func() {
		c.enqueue(note{sub, []byte("4")})
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("enqueue on an ended connection blocked")
	}
	if len(c.queue) != 2 {
		t.Fatalf("queue holds %d notes, want 2", len(c.queue))
	}
}

func TestWSTimeouts(t *testing.T) {
	t.Run("idle past the HTTP timeouts", func(t *testing.T) {
		s, _, _ := newTestServer(t, nil, genesisChain(t))
		_, addr := serveHTTP(t, s, func(hs *http.Server) {
			hs.ReadTimeout = 200 * time.Millisecond
			hs.WriteTimeout = 200 * time.Millisecond
		})
		c := newWSClient(t, addr)
		time.Sleep(time.Second)
		c.send(echoReq(1))
		if got := string(c.next()); got != okResp("1", "[1]") {
			t.Fatalf("answer after 1 s: %s", got)
		}
	})
	t.Run("pings keep an idle connection open", func(t *testing.T) {
		s, _, _ := newTestServer(t, nil, genesisChain(t))
		s.wsPingInterval = 50 * time.Millisecond
		s.wsPongWait = 200 * time.Millisecond
		_, addr := serveHTTP(t, s, nil)
		// The client's reader answers each ping with a pong, which
		// extends the server's read deadline.
		c := newWSClient(t, addr)
		time.Sleep(time.Second)
		c.send(echoReq(1))
		if got := string(c.next()); got != okResp("1", "[1]") {
			t.Fatalf("answer after 1 s idle: %s", got)
		}
		s.wsMu.Lock()
		n := len(s.conns)
		s.wsMu.Unlock()
		if n != 1 {
			t.Fatalf("%d connections after 1 s idle, want 1", n)
		}
	})
	t.Run("missing pong", func(t *testing.T) {
		s, _, _ := newTestServer(t, nil, genesisChain(t))
		s.wsPingInterval = 50 * time.Millisecond
		s.wsPongWait = 200 * time.Millisecond
		_, addr := serveHTTP(t, s, nil)
		// The client never reads, so it never answers a ping.
		conn, _, err := dialWS(addr, nil)
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Close()
		deadline := time.Now().Add(time.Second)
		for {
			s.wsMu.Lock()
			n := len(s.conns)
			s.wsMu.Unlock()
			if n == 0 {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("%d connections after 1 s, want 0", n)
			}
			time.Sleep(10 * time.Millisecond)
		}
	})
}

// postEcho posts one test_echo request to the gateway at addr and
// returns the response body.
func postEcho(t *testing.T, addr string, n int) string {
	t.Helper()
	resp, err := http.Post("http://"+addr+"/", "application/json", strings.NewReader(echoReq(n)))
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != 200 {
		t.Fatalf("POST status %d: %s", resp.StatusCode, b)
	}
	return string(b)
}

func fill(sem chan struct{}) {
	for i := 0; i < cap(sem); i++ {
		sem <- struct{}{}
	}
}

func TestWSShutdown(t *testing.T) {
	shutdown := func(s *Server, hs *http.Server) chan struct{} {
		done := make(chan struct{})
		go func() {
			s.Shutdown(hs)
			close(done)
		}()
		return done
	}
	wait := func(t *testing.T, done chan struct{}) {
		t.Helper()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Fatal("Shutdown did not return within 5 s")
		}
	}

	t.Run("closes connections with 1001", func(t *testing.T) {
		s, _, _ := newTestServer(t, nil, genesisChain(t))
		hs, addr := serveHTTP(t, s, nil)
		c := newWSClient(t, addr)
		wait(t, shutdown(s, hs))
		c.requireClose(websocket.CloseGoingAway)
	})

	t.Run("wsSem full leaves HTTP its slots", func(t *testing.T) {
		s, _, _ := newTestServer(t, nil, genesisChain(t))
		_, addr := serveHTTP(t, s, nil)
		c := newWSClient(t, addr)
		fill(s.wsSem)
		c.send(echoReq(1))
		c.none(100 * time.Millisecond)
		if got := postEcho(t, addr, 2); got != okResp("2", "[2]") {
			t.Fatalf("HTTP answer = %s", got)
		}
		<-s.wsSem
		if got := string(c.next()); got != okResp("1", "[1]") {
			t.Fatalf("WebSocket answer = %s", got)
		}
	})

	t.Run("sem full holds a WebSocket request", func(t *testing.T) {
		s, _, _ := newTestServer(t, nil, genesisChain(t))
		_, addr := serveHTTP(t, s, nil)
		c := newWSClient(t, addr)
		fill(s.sem)
		c.send(echoReq(1))
		c.none(100 * time.Millisecond)
		<-s.sem
		if got := string(c.next()); got != okResp("1", "[1]") {
			t.Fatalf("WebSocket answer = %s", got)
		}
	})

	t.Run("a client that stopped reading does not stall the close", func(t *testing.T) {
		s, _, _ := newTestServer(t, nil, genesisChain(t))
		s.wsWriteWait = 30 * time.Second
		_, addr := serveHTTP(t, s, nil)
		conn, _, err := dialWS(addr, nil)
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Close()
		req := `{"jsonrpc":"2.0","method":"zeno_subscribe","params":["heads",1],"id":1}`
		if err := conn.WriteMessage(websocket.TextMessage, []byte(req)); err != nil {
			t.Fatal(err)
		}
		if _, _, err := conn.ReadMessage(); err != nil {
			t.Fatal(err)
		}
		// The client reads nothing more, so the writer blocks once
		// the socket buffers fill.
		big := make([]byte, 1<<20)
		s.wsMu.Lock()
		for c := range s.conns {
			for _, sub := range c.subs {
				for i := 0; i < 64; i++ {
					c.enqueue(note{sub, big})
				}
			}
		}
		s.wsMu.Unlock()
		time.Sleep(500 * time.Millisecond)
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		start := time.Now()
		s.closeWS(ctx)
		if d := time.Since(start); d > 4*time.Second {
			t.Fatalf("closeWS took %v, want under 4 s", d)
		}
		s.wsMu.Lock()
		n := len(s.conns)
		s.wsMu.Unlock()
		if n != 0 {
			t.Fatalf("%d connections after closeWS, want 0", n)
		}
	})
	t.Run("a waiting request leaves on shutdown", func(t *testing.T) {
		s, _, _ := newTestServer(t, nil, genesisChain(t))
		hs, addr := serveHTTP(t, s, nil)
		c := newWSClient(t, addr)
		fill(s.wsSem)
		c.send(echoReq(1))
		c.none(100 * time.Millisecond)
		done := shutdown(s, hs)
		c.requireClose(websocket.CloseGoingAway)
		wait(t, done)
	})
}
