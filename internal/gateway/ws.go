package gateway

import (
	"bytes"
	"context"
	"net/http"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

// note is one queued notification and the subscription it is for.
type note struct {
	sub *subscription
	msg []byte
}

// wsConn is one WebSocket connection. Its reader runs in serveWS and
// its writer in writeLoop.
type wsConn struct {
	ws     *websocket.Conn
	ctx    context.Context
	cancel context.CancelFunc
	// wmu serializes data frames: responses from the reader and notes
	// from the writer. It is taken before Server.wsMu, never after.
	wmu   sync.Mutex
	queue chan note
	// quit closes once, when the connection ends. code and text, set
	// before it closes, name the close frame to send; 0 sends none.
	quit chan struct{}
	once sync.Once
	code int
	text string
	// done closes when writeLoop returns.
	done chan struct{}
	// subs and nextID are guarded by Server.wsMu.
	subs   map[string]*subscription
	nextID uint64
}

// serveWS upgrades r and serves JSON-RPC on the connection until it
// ends.
func (s *Server) serveWS(w http.ResponseWriter, r *http.Request) {
	s.wsMu.Lock()
	switch {
	case s.wsClosing:
		s.wsMu.Unlock()
		http.Error(w, "the gateway is shutting down", http.StatusServiceUnavailable)
		return
	case s.wsOpen >= s.wsConns:
		s.wsMu.Unlock()
		http.Error(w, "too many WebSocket connections", http.StatusServiceUnavailable)
		return
	}
	s.wsOpen++
	s.wsWG.Add(1)
	s.wsMu.Unlock()
	defer s.wsWG.Done()
	defer func() {
		s.wsMu.Lock()
		s.wsOpen--
		s.wsMu.Unlock()
	}()

	// CheckOrigin is set because the default rejects every cross-origin
	// upgrade, allowed origins included.
	u := websocket.Upgrader{HandshakeTimeout: s.wsWriteWait, CheckOrigin: s.checkOrigin}
	ws, err := u.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	// Upgrade clears the http.Server's read and write deadlines, so
	// from here only the deadlines this file sets apply.
	ws.SetReadLimit(maxRequestBytes)
	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()
	c := &wsConn{
		ws:     ws,
		ctx:    ctx,
		cancel: cancel,
		queue:  make(chan note, s.wsQueueLen),
		quit:   make(chan struct{}),
		done:   make(chan struct{}),
		subs:   make(map[string]*subscription),
	}
	ws.SetPongHandler(func(string) error {
		return ws.SetReadDeadline(time.Now().Add(s.wsPongWait))
	})

	s.wsMu.Lock()
	s.conns[c] = true
	closing := s.wsClosing
	s.wsMu.Unlock()
	go s.writeLoop(c)
	if closing {
		c.end(websocket.CloseGoingAway, "the gateway is shutting down")
	}

	take := func(sem chan struct{}) bool {
		select {
		case sem <- struct{}{}:
			return true
		case <-c.ctx.Done():
			return false
		}
	}
	for {
		_ = ws.SetReadDeadline(time.Now().Add(s.wsPongWait))
		_, msg, err := ws.ReadMessage()
		if err != nil {
			break
		}
		if !take(s.wsSem) {
			break
		}
		if !take(s.sem) {
			<-s.wsSem
			break
		}
		var buf bytes.Buffer
		n, _ := s.handle(c.ctx, c, msg, &buf)
		if n > 0 {
			c.wmu.Lock()
			err = c.send(buf.Bytes(), s.wsWriteWait)
			c.wmu.Unlock()
		}
		<-s.sem
		<-s.wsSem
		if err != nil {
			break
		}
		s.activate(c)
	}

	c.end(0, "")
	<-c.done
	s.wsMu.Lock()
	for _, sub := range c.subs {
		delete(s.subs, sub)
	}
	delete(s.conns, c)
	s.wsMu.Unlock()
}

// writeLoop writes c's queued notes and pings until c ends, then sends
// c's close frame and closes the connection.
func (s *Server) writeLoop(c *wsConn) {
	defer close(c.done)
	tick := time.NewTicker(s.wsPingInterval)
	defer tick.Stop()
	for {
		// An ended connection writes no further note, even when its
		// queue holds some.
		select {
		case <-c.quit:
			c.closeOut()
			return
		default:
		}
		select {
		case <-c.quit:
			c.closeOut()
			return
		case n := <-c.queue:
			c.wmu.Lock()
			s.wsMu.Lock()
			live := s.subs[n.sub]
			s.wsMu.Unlock()
			var err error
			if live {
				err = c.send(n.msg, s.wsWriteWait)
			}
			c.wmu.Unlock()
			if err != nil {
				c.end(0, "")
			}
		case <-tick.C:
			err := c.ws.WriteControl(websocket.PingMessage, nil, time.Now().Add(s.wsWriteWait))
			if err != nil {
				c.end(0, "")
			}
		}
	}
}

// closeOut sends c's close frame, when it names one, and closes the
// connection, which unblocks the reader.
func (c *wsConn) closeOut() {
	if c.code != 0 {
		msg := websocket.FormatCloseMessage(c.code, c.text)
		_ = c.ws.WriteControl(websocket.CloseMessage, msg, time.Now().Add(wsCloseWait))
	}
	_ = c.ws.Close()
}

// send writes msg as one text message within wait. The caller holds
// c.wmu.
func (c *wsConn) send(msg []byte, wait time.Duration) error {
	_ = c.ws.SetWriteDeadline(time.Now().Add(wait))
	return c.ws.WriteMessage(websocket.TextMessage, msg)
}

// end ends c once: it records the close frame, cancels c's context
// and closes quit. It never blocks.
func (c *wsConn) end(code int, text string) {
	c.once.Do(func() {
		c.code, c.text = code, text
		c.cancel()
		close(c.quit)
	})
}

// enqueue queues n for c without blocking, and ends c with 1008 when
// its queue is full. The caller holds Server.wsMu.
func (c *wsConn) enqueue(n note) {
	select {
	case <-c.quit:
		return
	default:
	}
	select {
	case c.queue <- n:
	default:
		c.end(websocket.ClosePolicyViolation, "notification queue full")
	}
}

// closeWS refuses new connections, ends every open one with 1001 and
// waits for their handlers until ctx ends. It closes the sockets of
// connections that outlast two close-frame waits.
func (s *Server) closeWS(ctx context.Context) {
	s.wsMu.Lock()
	s.wsClosing = true
	for c := range s.conns {
		c.end(websocket.CloseGoingAway, "the gateway is shutting down")
	}
	s.wsMu.Unlock()
	done := make(chan struct{})
	go func() {
		s.wsWG.Wait()
		close(done)
	}()
	// A writer stuck on a client that stopped reading holds its
	// connection open for the whole write deadline, so a connection
	// that outlasts the close frame writes is closed from here.
	grace := time.NewTimer(2 * wsCloseWait)
	defer grace.Stop()
	select {
	case <-done:
		return
	case <-ctx.Done():
		return
	case <-grace.C:
	}
	s.wsMu.Lock()
	for c := range s.conns {
		_ = c.ws.Close()
	}
	s.wsMu.Unlock()
	select {
	case <-done:
	case <-ctx.Done():
	}
}
