package zenoexec

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"net"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"strconv"
	"time"
)

const (
	maxRequestBytes = 2 << 20
	maxCommands     = 64
	bodyReadTimeout = 10 * time.Second
	// ServeGasBudget is a request session's gas budget: a call or deposit
	// whose plasmaLimit exceeds the gas left is refused, and each receipt's
	// gasUsed draws the gas left down. The child process bounds the wall
	// time; the budget refuses an oversized limit up front.
	ServeGasBudget = 200_000_000
)

// requestTimeout bounds one /v1/run request, session included. It is a
// variable so tests can shorten it.
var requestTimeout = 10 * time.Second

// sem holds one token per running session.
var sem = make(chan struct{}, 4)

var defaultOrigin = regexp.MustCompile(`^http://(localhost|127\.0\.0\.1)(:[0-9]+)?$`)

// ServeConfig configures NewHandler.
type ServeConfig struct {
	Network string
	// ChildCommand is the argv of the process that runs one request's
	// commands as "zeno-exec run" does, one result line per command
	// line.
	ChildCommand []string
	Port         int      // the bound port, for the Host check
	AllowOrigins []string // added to the defaults
	Version      VersionInfo
}

// VersionInfo is the output of "version" and GET /v1/version.
type VersionInfo struct {
	Version  string `json:"version"`
	Revision string `json:"revision"`
	Modified bool   `json:"modified"`
}

// CheckListen accepts an address whose host is exactly 127.0.0.1 or
// ::1.
func CheckListen(addr string) error {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return err
	}
	if host != "127.0.0.1" && host != "::1" {
		return fmt.Errorf("listen host %q must be 127.0.0.1 or ::1", host)
	}
	return nil
}

// CheckOrigin accepts an origin of the form scheme://host[:port] with
// an http or https scheme and nothing else.
func CheckOrigin(s string) error {
	u, err := url.Parse(s)
	if err != nil {
		return fmt.Errorf("origin %q: %w", s, err)
	}
	if (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" || u.User != nil ||
		u.Path != "" || u.RawQuery != "" || u.Fragment != "" || u.Opaque != "" ||
		u.Scheme+"://"+u.Host != s {
		return fmt.Errorf("origin %q must be http or https, scheme://host[:port], with no path, query or fragment", s)
	}
	return nil
}

// NewHandler returns the handler behind "serve". It refuses a malformed
// AllowOrigins entry.
func NewHandler(cfg ServeConfig) (http.Handler, error) {
	if len(cfg.ChildCommand) == 0 {
		return nil, errors.New("ChildCommand is required")
	}
	allowed := map[string]bool{}
	for _, o := range cfg.AllowOrigins {
		if err := CheckOrigin(o); err != nil {
			return nil, err
		}
		allowed[o] = true
	}
	port := strconv.Itoa(cfg.Port)
	hosts := map[string]bool{
		"127.0.0.1:" + port: true,
		"localhost:" + port: true,
		"[::1]:" + port:     true,
	}
	h := &handler{cfg: cfg, allowed: allowed, hosts: hosts}
	return h, nil
}

type handler struct {
	cfg     ServeConfig
	allowed map[string]bool
	hosts   map[string]bool
}

func (h *handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Add("Vary", "Origin")
	if !h.hosts[r.Host] {
		http.Error(w, "host not allowed", http.StatusForbidden)
		return
	}
	if vs := r.Header.Values("Origin"); len(vs) > 0 {
		if len(vs) != 1 || !(defaultOrigin.MatchString(vs[0]) || h.allowed[vs[0]]) {
			http.Error(w, "origin not allowed", http.StatusForbidden)
			return
		}
		w.Header().Set("Access-Control-Allow-Origin", vs[0])
	}

	switch r.URL.Path {
	case "/v1/version":
		switch r.Method {
		case http.MethodOptions:
			w.Header().Set("Access-Control-Allow-Methods", "GET, OPTIONS")
			w.WriteHeader(http.StatusNoContent)
		case http.MethodGet:
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(h.cfg.Version)
		default:
			w.Header().Set("Allow", "GET, OPTIONS")
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		}
	case "/v1/run":
		switch r.Method {
		case http.MethodOptions:
			w.Header().Set("Access-Control-Allow-Methods", "POST, OPTIONS")
			w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
			w.WriteHeader(http.StatusNoContent)
		case http.MethodPost:
			h.run(w, r)
		default:
			w.Header().Set("Allow", "POST, OPTIONS")
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		}
	default:
		http.NotFound(w, r)
	}
}

// run serves POST /v1/run. It takes a session slot before the body read
// and releases it on every return before the session goroutine starts;
// from then on only that goroutine releases it.
func (h *handler) run(w http.ResponseWriter, r *http.Request) {
	mt, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil || mt != "application/json" {
		http.Error(w, "Content-Type must be application/json", http.StatusUnsupportedMediaType)
		return
	}
	if r.ContentLength > maxRequestBytes {
		http.Error(w, "request body exceeds 2 MiB", http.StatusRequestEntityTooLarge)
		return
	}
	select {
	case sem <- struct{}{}:
	default:
		http.Error(w, "too many sessions in flight", http.StatusServiceUnavailable)
		return
	}
	release := func() { <-sem }

	// The error is ignored: a recorder does not support deadlines, and
	// on a real connection any fault shows in the body read.
	rc := http.NewResponseController(w)
	_ = rc.SetReadDeadline(time.Now().Add(bodyReadTimeout))
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxRequestBytes))
	if err != nil {
		release()
		var tooLarge *http.MaxBytesError
		switch {
		case errors.As(err, &tooLarge):
			http.Error(w, "request body exceeds 2 MiB", http.StatusRequestEntityTooLarge)
		case errors.Is(err, os.ErrDeadlineExceeded):
			w.Header().Set("Connection", "close")
			http.Error(w, "request body read timed out", http.StatusRequestTimeout)
		default:
			http.Error(w, "request body could not be read", http.StatusBadRequest)
		}
		return
	}
	_ = rc.SetReadDeadline(time.Time{})

	var req map[string]json.RawMessage
	var cmds []json.RawMessage
	if json.Unmarshal(body, &req) != nil || len(req) != 1 || json.Unmarshal(req["commands"], &cmds) != nil || cmds == nil {
		release()
		http.Error(w, `body must be {"commands":[...]}`, http.StatusBadRequest)
		return
	}
	if len(cmds) > maxCommands {
		release()
		http.Error(w, fmt.Sprintf("more than %d commands", maxCommands), http.StatusBadRequest)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), requestTimeout)
	defer cancel()
	type outcome struct {
		results []json.RawMessage
		err     error
	}
	done := make(chan outcome, 1)
	go func() {
		defer release()
		results, err := runChild(ctx, h.cfg.ChildCommand, cmds)
		done <- outcome{results, err}
	}()

	select {
	case <-ctx.Done():
		http.Error(w, "request timed out", http.StatusGatewayTimeout)
	case out := <-done:
		switch {
		case ctx.Err() != nil:
			http.Error(w, "request timed out", http.StatusGatewayTimeout)
		case out.err != nil:
			http.Error(w, "session failed", http.StatusInternalServerError)
		default:
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{"results": out.results})
		}
	}
}
