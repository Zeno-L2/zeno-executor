package gateway

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"os"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"

	"github.com/digitalSloth/go-executor/domain"
)

// The JSON-RPC server's bounds (EXECUTOR §21.7).
const (
	// maxRequestBytes caps one HTTP request body.
	maxRequestBytes = 1 << 20
	// maxBatchRequests caps the requests in one JSON-RPC batch.
	maxBatchRequests = 100
	// maxParams caps the positional parameters of one request.
	maxParams = 16
	// maxResponseBytes caps the responses to one request body.
	maxResponseBytes = 16 << 20
	// maxConcurrentRequests caps the POST requests served at once.
	maxConcurrentRequests = 32
	// maxProofPaths caps the paths of one zeno_getProof request.
	maxProofPaths = 64
	// maxBodyProofPaths caps the zeno_getProof paths of one request
	// body.
	maxBodyProofPaths = 256
	// proofBudgetMsg is the error of a zeno_getProof request that
	// would pass maxBodyProofPaths.
	proofBudgetMsg = "request body exceeds its proof path budget"
	// maxPageItems caps the items of one list page.
	maxPageItems = 100
	// maxScanKeys caps the index keys the list requests of one request
	// body visit together.
	maxScanKeys = 10000
	// maxScanBytes caps the index value bytes the list requests of one
	// request body read together.
	maxScanBytes = 32 << 20
	// scanBudgetMsg is the error of a list request that must scan an
	// index after its body has used maxScanKeys or maxScanBytes.
	scanBudgetMsg = "request body exceeds its index scan budget; send this request alone"
	// maxLoggedKinds bounds the internal error lines remembered to
	// suppress repeats.
	maxLoggedKinds = 64
	// maxBodyOutboxProofs caps the zeno_getOutboxProof requests of one
	// request body.
	maxBodyOutboxProofs = 8
	// outboxBudgetMsg is the error of a zeno_getOutboxProof request
	// past maxBodyOutboxProofs.
	outboxBudgetMsg = "request body exceeds its outbox proof budget; send this request alone"
	// outboxReadTimeout bounds the processedOutbox reads of one request
	// body together.
	outboxReadTimeout = 10 * time.Second
	// maxOutboxReads caps the processedOutbox reads running at once,
	// counting each until the node answers, abandoned ones included.
	maxOutboxReads = 4
	// outboxBusyMsg is the error of a processedOutbox read refused by
	// maxOutboxReads.
	outboxBusyMsg = "too many processedOutbox reads in flight; retry later"
	// maxBodySimulations caps the zeno_simulate requests of one request
	// body.
	maxBodySimulations = 2
	// simBudgetMsg is the error of a zeno_simulate request past
	// maxBodySimulations.
	simBudgetMsg = "request body exceeds its simulation budget; send this request alone"
	// maxWSConns caps the open WebSocket connections.
	maxWSConns = 64
	// maxWSInFlight caps the WebSocket requests served at once, so
	// HTTP keeps the rest of maxConcurrentRequests.
	maxWSInFlight = 16
	// maxSubscriptions caps the subscriptions of one connection.
	maxSubscriptions = 16
	// wsQueueLen caps the notes one connection holds unwritten.
	wsQueueLen = 256
	// wsWriteWait bounds each WebSocket write and the upgrade response.
	wsWriteWait = 10 * time.Second
	// wsPingInterval is the wait between two pings on a connection.
	wsPingInterval = 30 * time.Second
	// wsPongWait is how long a connection's reader waits for a message
	// or a pong.
	wsPongWait = 60 * time.Second
	// wsCloseWait bounds the write of a close frame.
	wsCloseWait = time.Second

	// maxFileTransfers caps the GET /bundles and /receipts responses
	// served at once.
	maxFileTransfers = 8
	// fileWriteWait bounds each write of a file response.
	fileWriteWait = 30 * time.Second
	// fileTransferMax bounds a whole file response.
	fileTransferMax = 10 * time.Minute

	httpReadHeaderTimeout = 5 * time.Second
	httpReadTimeout       = 30 * time.Second
	httpWriteTimeout      = 30 * time.Second
	httpIdleTimeout       = 120 * time.Second
	// httpMaxHeaderBytes caps the request line and headers of one
	// request, which are read before a request can take a slot.
	httpMaxHeaderBytes = 16 << 10
	// httpBodyReadTimeout bounds the body read of a POST that holds
	// an in-flight slot, so a slow client frees its slot within it.
	httpBodyReadTimeout = 5 * time.Second
	// httpShutdownTimeout bounds the wait for requests in flight at
	// shutdown.
	httpShutdownTimeout = 10 * time.Second
)

// JSON-RPC error codes: JSON-RPC 2.0's and EXECUTOR §21.3's.
const (
	codeParseError     = -32700
	codeInvalidRequest = -32600
	codeMethodNotFound = -32601
	codeInvalidParams  = -32602
	codeInternal       = -32603
	codeUnknownDomain  = -32001
	codeNotAvailable   = -32002
	codeInconsistent   = -32003
	codeNotFound       = -32004
	codeNoSuchView     = -32005
	codeSimulation     = -32006
)

// rpcError is a JSON-RPC error object. A method returns one for an
// error the client may see; any other error is internal.
type rpcError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

func (e *rpcError) Error() string { return e.Message }

// budget is what one request may still use of its body's bounds, with
// the context and connection it runs under: left is the most bytes its
// result's JSON may take, scan the index keys the body may still visit
// and scanBytes the index value bytes it may still read, which the list
// methods reduce.
type budget struct {
	left      int
	scan      int
	scanBytes int
	// proofs is the number of state paths the body may still prove
	// through zeno_read.
	proofs int
	// ctx is the request's context: the HTTP request's, or the
	// WebSocket connection's. one always sets it.
	ctx context.Context
	// conn is the WebSocket connection the request came on; nil over
	// HTTP.
	conn *wsConn
	// outboxBy is the deadline of the body's processedOutbox reads;
	// zero until the body's first read sets it.
	outboxBy time.Time
	// sub is the subscription a zeno_subscribe request created; nil
	// otherwise.
	sub *subscription
}

// Server serves JSON-RPC 2.0 over HTTP (EXECUTOR §21.3) for the
// configured domains. It is safe for concurrent use.
type Server struct {
	networkName string
	// endpoint is the znnd endpoint, redacted from logged errors.
	endpoint string
	chains   map[uint32]*Chain
	// order holds the chains in configuration order, for the methods
	// that search every domain.
	order   []*Chain
	origins map[string]bool
	methods map[string]method
	// sem holds one slot per POST being served.
	sem chan struct{}
	// bodyReadTimeout bounds a POST's body read once it holds a slot.
	bodyReadTimeout time.Duration
	// fileSem holds one slot per file response being served.
	fileSem chan struct{}
	// fileWriteWait bounds each write of a file response, and
	// fileTransferMax the whole response.
	fileWriteWait, fileTransferMax time.Duration
	// scanKeys is the index keys one request body may visit.
	scanKeys int
	// scanBytes is the index value bytes one request body may read.
	scanBytes int
	// outboxReadTimeout bounds the processedOutbox reads of one request
	// body together.
	outboxReadTimeout time.Duration
	// outboxSem holds one slot per processedOutbox read running.
	outboxSem chan struct{}
	// wsSem holds one slot per WebSocket request being served.
	wsSem chan struct{}
	// wsConns, wsQueueLen, wsWriteWait, wsPingInterval and wsPongWait
	// are the WebSocket bounds.
	wsConns, wsQueueLen                     int
	wsWriteWait, wsPingInterval, wsPongWait time.Duration
	logf                                    func(format string, args ...any)
	// readers holds each runtime kind's read adapter.
	readers map[uint8]domain.Reader
	// sim is cfg.Simulation with defaults filled; simulation is on only
	// when sim.Enabled is non-nil and true.
	sim SimulationConfig
	// simBinary maps a domain id to its workerBinary.
	simBinary map[uint32]string
	// simTiming holds the pools' waits; tests shorten them.
	simTiming simTiming
	// simCapsSem holds one slot per simulation caps read running.
	simCapsSem chan struct{}
	// sims maps a domain id to its pool. startSimulation writes it
	// before the server serves; it is read-only afterwards.
	sims map[uint32]*simPool
	// simCtx ends every pool; simCancel is nil until startSimulation
	// starts one.
	simCtx    context.Context
	simCancel context.CancelFunc

	// wsMu guards conns, wsOpen, wsClosing, subs, each wsConn's subs and
	// nextID, and each subscription's active.
	wsMu      sync.Mutex
	conns     map[*wsConn]bool
	wsOpen    int
	wsClosing bool
	subs      map[*subscription]bool
	// wsWG counts the WebSocket handlers.
	wsWG sync.WaitGroup

	logMu sync.Mutex
	// loggedKinds holds the kind of each internal error line logged,
	// as logKind computes it.
	loggedKinds map[string]bool
}

// NewServer returns the JSON-RPC server for chains, with cfg's
// networkName and allowedOrigins, redacting cfg's znndEndpoint from
// the errors it logs through logf. readers maps a domain record's
// runtime kind to the adapter that decodes its inputs and serves its
// views.
func NewServer(cfg Config, chains []*Chain, readers map[uint8]domain.Reader, logf func(format string, args ...any)) *Server {
	s := &Server{
		networkName:     cfg.NetworkName,
		endpoint:        cfg.ZnndEndpoint,
		chains:          make(map[uint32]*Chain, len(chains)),
		origins:         make(map[string]bool, len(cfg.AllowedOrigins)),
		methods:         methodTable(),
		sem:             make(chan struct{}, maxConcurrentRequests),
		bodyReadTimeout: httpBodyReadTimeout,
		fileSem:         make(chan struct{}, maxFileTransfers),
		fileWriteWait:   fileWriteWait,
		fileTransferMax: fileTransferMax,
		scanKeys:        maxScanKeys,
		scanBytes:       maxScanBytes,
		logf:            logf,
		readers:         readers,
		sim:             cfg.Simulation.withDefaults(),
		simBinary:       make(map[uint32]string, len(cfg.Domains)),
		simTiming:       simTiming{queueWait: simQueueWait, restartBase: simRestartBase},
		simCapsSem:      make(chan struct{}, maxSimCapsReads),

		outboxReadTimeout: outboxReadTimeout,
		outboxSem:         make(chan struct{}, maxOutboxReads),
		wsSem:             make(chan struct{}, maxWSInFlight),
		wsConns:           maxWSConns,
		wsQueueLen:        wsQueueLen,
		wsWriteWait:       wsWriteWait,
		wsPingInterval:    wsPingInterval,
		wsPongWait:        wsPongWait,
		conns:             make(map[*wsConn]bool),
		subs:              make(map[*subscription]bool),
	}
	for _, c := range chains {
		s.chains[c.cfg.DomainID] = c
		s.order = append(s.order, c)
		c.notify.Store(s)
	}
	for _, d := range cfg.Domains {
		s.simBinary[d.DomainID] = d.WorkerBinary
	}
	for _, o := range cfg.AllowedOrigins {
		s.origins[o] = true
	}
	return s
}

// NewHTTPServer returns the HTTP server for s, with the read, write
// and idle timeouts and the header cap of a public endpoint.
func NewHTTPServer(s *Server) *http.Server {
	return &http.Server{
		Handler:           s,
		ReadHeaderTimeout: httpReadHeaderTimeout,
		ReadTimeout:       httpReadTimeout,
		WriteTimeout:      httpWriteTimeout,
		IdleTimeout:       httpIdleTimeout,
		MaxHeaderBytes:    httpMaxHeaderBytes,
	}
}

// checkOrigin reports whether r may be served: it carries no Origin
// header, or exactly one whose value is an allowed origin.
func (s *Server) checkOrigin(r *http.Request) bool {
	vs := r.Header.Values("Origin")
	return len(vs) == 0 || (len(vs) == 1 && s.origins[vs[0]])
}

// ServeHTTP serves JSON-RPC on POST / and CORS preflight on
// OPTIONS /, WebSocket upgrades on GET /, and the file routes under
// /bundles/ and /receipts/, under the bounds above.
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		if strings.HasPrefix(r.URL.Path, bundlesPrefix) || strings.HasPrefix(r.URL.Path, receiptsPrefix) {
			s.serveFile(w, r)
			return
		}
		http.NotFound(w, r)
		return
	}
	w.Header().Add("Vary", "Origin")
	if !s.checkOrigin(r) {
		http.Error(w, "origin not allowed", http.StatusForbidden)
		return
	}
	origin := r.Header.Get("Origin")
	if origin != "" {
		w.Header().Set("Access-Control-Allow-Origin", origin)
	}
	switch r.Method {
	case http.MethodOptions:
		w.Header().Set("Allow", "POST, OPTIONS")
		if origin != "" {
			w.Header().Set("Access-Control-Allow-Methods", "POST, OPTIONS")
			w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
		}
		w.WriteHeader(http.StatusNoContent)
		return
	case http.MethodPost:
	case http.MethodGet:
		if websocket.IsWebSocketUpgrade(r) {
			s.serveWS(w, r)
			return
		}
		w.Header().Set("Allow", "POST, OPTIONS")
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	default:
		w.Header().Set("Allow", "POST, OPTIONS")
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	mt, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil || mt != "application/json" {
		http.Error(w, "Content-Type must be application/json", http.StatusUnsupportedMediaType)
		return
	}
	if r.ContentLength > maxRequestBytes {
		http.Error(w, "request body exceeds 1 MiB", http.StatusRequestEntityTooLarge)
		return
	}
	select {
	case s.sem <- struct{}{}:
		defer func() { <-s.sem }()
	default:
		http.Error(w, "too many requests in flight", http.StatusServiceUnavailable)
		return
	}

	// The error is ignored: a recorder does not support deadlines, and
	// on a real connection any fault shows in the body read.
	rc := http.NewResponseController(w)
	_ = rc.SetReadDeadline(time.Now().Add(s.bodyReadTimeout))
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxRequestBytes))
	if err != nil {
		var tooLarge *http.MaxBytesError
		switch {
		case errors.As(err, &tooLarge):
			http.Error(w, "request body exceeds 1 MiB", http.StatusRequestEntityTooLarge)
		case errors.Is(err, os.ErrDeadlineExceeded):
			w.Header().Set("Connection", "close")
			http.Error(w, "request body read timed out", http.StatusRequestTimeout)
		default:
			http.Error(w, "request body could not be read", http.StatusBadRequest)
		}
		return
	}
	// Clear the deadline. net/http clears it when a body reaches EOF,
	// but not for a POST with no body, and a deadline left set would
	// make its background read, which watches for a hang-up while the
	// handler runs, cancel the connection's context.
	_ = rc.SetReadDeadline(time.Time{})

	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	bw := bufio.NewWriterSize(w, 64<<10)
	n, err := s.handle(r.Context(), nil, body, bw)
	if err != nil {
		return
	}
	if n == 0 {
		w.Header().Del("Content-Type")
		w.WriteHeader(http.StatusNoContent)
		return
	}
	_ = bw.Flush()
}

// sink writes one body's responses in order and keeps the response
// cap.
type sink struct {
	w     io.Writer
	batch bool
	count int // responses written
	// proofPaths counts the zeno_getProof paths this body has used.
	proofPaths int
	// scan is the index keys this body may still visit.
	scan int
	// scanBytes is the index value bytes this body may still read.
	scanBytes int
	// outboxProofs counts the zeno_getOutboxProof requests this body
	// has made.
	outboxProofs int
	// simulations counts the zeno_simulate requests this body has
	// made, whatever their outcome.
	simulations int
	// outboxBy is the deadline of this body's processedOutbox reads;
	// zero until the first.
	outboxBy time.Time
	// ctx and conn are the context and WebSocket connection the body
	// runs under; conn is nil over HTTP.
	ctx  context.Context
	conn *wsConn
	n    int   // bytes written
	err  error // the first write error; nothing is written after it
}

// framing returns the bytes a batch adds around its next response: the
// "[" or "," before it, and the "]" kept in reserve.
func (k *sink) framing() int {
	if k.batch {
		return 2
	}
	return 0
}

// room returns the result bytes that fit after a result prefix of
// prefix bytes and the closing brace.
func (k *sink) room(prefix int) int {
	return maxResponseBytes - k.n - k.framing() - prefix - 1
}

func (k *sink) put(p []byte) {
	if k.err != nil {
		return
	}
	n, err := k.w.Write(p)
	k.n += n
	k.err = err
}

// write writes one response made of parts. The parts are written
// separately, so a large result is not copied.
func (k *sink) write(parts ...[]byte) {
	if k.err != nil {
		return
	}
	if k.batch {
		if k.count == 0 {
			k.put([]byte("["))
		} else {
			k.put([]byte(","))
		}
	}
	k.count++
	for _, p := range parts {
		k.put(p)
	}
}

// end closes a batch that wrote anything.
func (k *sink) end() {
	if k.batch && k.count > 0 {
		k.put([]byte("]"))
	}
}

var nullID = json.RawMessage("null")

// fail writes an error response for id.
func (k *sink) fail(id json.RawMessage, code int, msg string) {
	e, _ := json.Marshal(rpcError{Code: code, Message: msg})
	k.write([]byte(`{"jsonrpc":"2.0","id":`), id, []byte(`,"error":`), e, []byte("}"))
}

// handle runs one JSON-RPC request body and writes its responses to
// w. It returns the bytes written, 0 when the body held only
// notifications, and the first write error.
func (s *Server) handle(ctx context.Context, conn *wsConn, body []byte, w io.Writer) (int, error) {
	k := &sink{w: w, scan: s.scanKeys, scanBytes: s.scanBytes, ctx: ctx, conn: conn}
	if !json.Valid(body) {
		k.fail(nullID, codeParseError, "parse error")
		return k.n, k.err
	}
	if bytes.HasPrefix(bytes.TrimLeft(body, " \t\r\n"), []byte("[")) {
		elems, over, err := splitArray(body, maxBatchRequests)
		switch {
		case err != nil:
			k.fail(nullID, codeParseError, "parse error")
		case over:
			k.fail(nullID, codeInvalidRequest, "batch exceeds 100 requests")
		case len(elems) == 0:
			k.fail(nullID, codeInvalidRequest, "invalid request")
		default:
			k.batch = true
			for _, e := range elems {
				if k.err != nil {
					break
				}
				s.one(e, k)
			}
			k.end()
		}
		return k.n, k.err
	}
	s.one(body, k)
	return k.n, k.err
}

// splitArray returns the elements of raw, a valid JSON array,
// decoding at most max of them. over is true, with no elements, when
// raw holds more than max.
func splitArray(raw []byte, max int) (elems []json.RawMessage, over bool, err error) {
	dec := json.NewDecoder(bytes.NewReader(raw))
	if _, err := dec.Token(); err != nil { // the '['
		return nil, false, err
	}
	for dec.More() {
		if len(elems) == max {
			return nil, true, nil
		}
		var e json.RawMessage
		if err := dec.Decode(&e); err != nil {
			return nil, false, err
		}
		elems = append(elems, e)
	}
	return elems, false, nil
}

// startsWith reports whether raw begins with c.
func startsWith(raw json.RawMessage, c byte) bool {
	return len(raw) > 0 && raw[0] == c
}

// one validates and runs one request object and writes its response
// to k, or nothing for a notification.
func (s *Server) one(raw json.RawMessage, k *sink) {
	// The members are read one by one rather than into a map, which
	// would hold an entry for every member of a large object.
	dec := json.NewDecoder(bytes.NewReader(raw))
	if t, err := dec.Token(); err != nil || t != json.Delim('{') {
		k.fail(nullID, codeInvalidRequest, "invalid request")
		return
	}
	var version, id, rawName, params, skip json.RawMessage
	var hasID, hasParams bool
	for dec.More() {
		t, err := dec.Token()
		key, ok := t.(string)
		if err != nil || !ok {
			k.fail(nullID, codeInvalidRequest, "invalid request")
			return
		}
		dst := &skip
		switch key {
		case "jsonrpc":
			dst = &version
		case "id":
			dst, hasID = &id, true
		case "method":
			dst = &rawName
		case "params":
			dst, hasParams = &params, true
		}
		if err := dec.Decode(dst); err != nil {
			k.fail(nullID, codeInvalidRequest, "invalid request")
			return
		}
	}

	rid := nullID
	if hasID {
		if !startsWith(id, '"') && !startsWith(id, '-') && !(len(id) > 0 && id[0] >= '0' && id[0] <= '9') && string(id) != "null" {
			k.fail(nullID, codeInvalidRequest, "invalid request")
			return
		}
		rid = id
	}
	var v, name string
	if !startsWith(version, '"') || json.Unmarshal(version, &v) != nil || v != "2.0" ||
		!startsWith(rawName, '"') || json.Unmarshal(rawName, &name) != nil {
		k.fail(rid, codeInvalidRequest, "invalid request")
		return
	}
	if !hasID {
		return
	}

	var args []json.RawMessage
	if hasParams {
		if !startsWith(params, '[') {
			k.fail(rid, codeInvalidParams, "invalid params: params must be an array")
			return
		}
		a, over, err := splitArray(params, maxParams)
		switch {
		case err != nil:
			k.fail(rid, codeInvalidRequest, "invalid request")
			return
		case over:
			k.fail(rid, codeInvalidParams, fmt.Sprintf("invalid params: more than %d parameters", maxParams))
			return
		}
		args = a
	}
	fn, ok := s.methods[name]
	if !ok {
		k.fail(rid, codeMethodNotFound, "method not found")
		return
	}
	prefix := append(append([]byte(`{"jsonrpc":"2.0","id":`), rid...), `,"result":`...)
	left := k.room(len(prefix))
	if left <= 0 {
		k.overflow(rid)
		return
	}
	if name == "zeno_getProof" {
		n := proofPathCount(args)
		if k.proofPaths+n > maxBodyProofPaths {
			k.fail(rid, codeInvalidRequest, proofBudgetMsg)
			return
		}
		k.proofPaths += n
	}
	if name == "zeno_getOutboxProof" {
		if k.outboxProofs == maxBodyOutboxProofs {
			k.fail(rid, codeInvalidRequest, outboxBudgetMsg)
			return
		}
		k.outboxProofs++
	}
	if name == "zeno_simulate" {
		if k.simulations == maxBodySimulations {
			k.fail(rid, codeInvalidRequest, simBudgetMsg)
			return
		}
		k.simulations++
	}
	b := &budget{left: left, scan: k.scan, scanBytes: k.scanBytes, proofs: maxBodyProofPaths - k.proofPaths, ctx: k.ctx, conn: k.conn, outboxBy: k.outboxBy}
	res, err := s.call(fn, args, b)
	k.scan, k.scanBytes, k.outboxBy = b.scan, b.scanBytes, b.outboxBy
	k.proofPaths = maxBodyProofPaths - b.proofs
	if err != nil {
		var re *rpcError
		if errors.As(err, &re) {
			k.fail(rid, re.Code, re.Message)
			return
		}
		s.logInternal(name, err)
		k.fail(rid, codeInternal, "internal error")
		return
	}
	if len(res) > left {
		if b.sub != nil {
			s.dropSub(b.sub)
		}
		k.overflow(rid)
		return
	}
	k.write(prefix, res, []byte("}"))
}

// proofPathCount returns the number of paths a zeno_getProof request
// with params names, 0 when they are not an array of 1 to
// maxProofPaths elements.
func proofPathCount(params []json.RawMessage) int {
	if len(params) < 2 || !startsWith(params[1], '[') {
		return 0
	}
	elems, over, err := splitArray(params[1], maxProofPaths)
	if err != nil || over {
		return 0
	}
	return len(elems)
}

// overflow writes the error that replaces a response the cap leaves no
// room for.
func (k *sink) overflow(id json.RawMessage) {
	if k.batch {
		k.fail(id, codeInvalidRequest, "batch response exceeds 16 MiB; send this request alone")
		return
	}
	k.fail(id, codeInvalidRequest, "response exceeds 16 MiB")
}

// call runs fn and encodes its result. A panic in fn returns as an
// error naming the panic and where it happened.
func (s *Server) call(fn method, params []json.RawMessage, b *budget) (res []byte, err error) {
	defer func() {
		if p := recover(); p != nil {
			res, err = nil, fmt.Errorf("the method panicked: %v (at %s)", p, panicSite())
		}
	}()
	v, err := fn(s, params, b)
	if err != nil {
		return nil, err
	}
	if raw, ok := v.(json.RawMessage); ok {
		return raw, nil
	}
	return json.Marshal(v)
}

var (
	hexRun     = regexp.MustCompile(`[0-9a-fA-F]{64}`)
	addrRun    = regexp.MustCompile(`z1[0-9a-z]+`)
	escapeRun  = regexp.MustCompile(`\\x[0-9a-fA-F]{2}`)
	decimalRun = regexp.MustCompile(`[0-9]+`)
)

// logKind normalises line so that lines differing only in 64-digit hex
// values, bech32 addresses, byte escapes or decimal numbers, which a
// caller or executor chooses, share one kind.
func logKind(line string) string {
	line = hexRun.ReplaceAllString(line, "<hex>")
	line = addrRun.ReplaceAllString(line, "<addr>")
	line = escapeRun.ReplaceAllString(line, "<esc>")
	return decimalRun.ReplaceAllString(line, "<n>")
}

// logInternal logs an internal error of method name with the znnd
// endpoint redacted, once per kind of line, as computed by logKind.
func (s *Server) logInternal(name string, err error) {
	line := fmt.Sprintf("gateway: JSON-RPC %s: internal error: %q", name, RedactEndpointErr(s.endpoint, err))
	kind := logKind(line)
	s.logMu.Lock()
	defer s.logMu.Unlock()
	if s.loggedKinds[kind] {
		return
	}
	if len(s.loggedKinds) >= maxLoggedKinds {
		clear(s.loggedKinds)
	}
	if s.loggedKinds == nil {
		s.loggedKinds = make(map[string]bool)
	}
	s.loggedKinds[kind] = true
	s.logf("%s", line)
}

// Shutdown first closes every WebSocket connection with 1001 and waits
// for their handlers. It then stops hs from accepting, waits up to
// httpShutdownTimeout for the requests in flight, then closes every
// connection. It returns once no request is reading a domain, and then
// stops every simulator; every later POST and file request answers 503.
func (s *Server) Shutdown(hs *http.Server) {
	ctx, cancel := context.WithTimeout(context.Background(), httpShutdownTimeout)
	defer cancel()
	s.closeWS(ctx)
	if err := hs.Shutdown(ctx); err != nil {
		_ = hs.Close()
	}
	for i := 0; i < cap(s.sem); i++ {
		s.sem <- struct{}{}
	}
	for i := 0; i < cap(s.fileSem); i++ {
		s.fileSem <- struct{}{}
	}
	s.stopSimulation()
}
