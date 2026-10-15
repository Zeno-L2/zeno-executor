package gateway

import (
	"bytes"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"os"
	"strings"
	"syscall"
	"time"

	"github.com/zenon-network/go-zenon/common/types"

	"github.com/digitalSloth/go-executor/internal/codec"
	"github.com/digitalSloth/go-executor/internal/recovery"
)

const (
	bundlesPrefix  = "/bundles/"
	receiptsPrefix = "/receipts/"
	fileAllow      = "GET, HEAD, OPTIONS"
)

// parseFileHash returns the daHash of a file route path: prefix plus
// exactly 64 hex characters.
func parseFileHash(path, prefix string) (types.Hash, bool) {
	var h types.Hash
	rest, ok := strings.CutPrefix(path, prefix)
	if !ok || len(rest) != 2*len(h) {
		return h, false
	}
	b, err := hex.DecodeString(rest)
	if err != nil {
		return h, false
	}
	copy(h[:], b)
	return h, true
}

// serveFile serves GET and HEAD for /bundles/<daHash> and
// /receipts/<daHash>, the files of the batches zeno_getBundle and
// zeno_getReceiptSidecar serve, with byte ranges.
func (s *Server) serveFile(w http.ResponseWriter, r *http.Request) {
	prefix, route := bundlesPrefix, "GET /bundles"
	bundle := true
	if strings.HasPrefix(r.URL.Path, receiptsPrefix) {
		prefix, route, bundle = receiptsPrefix, "GET /receipts", false
	}
	h, ok := parseFileHash(r.URL.Path, prefix)
	if !ok {
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
		w.Header().Set("Access-Control-Expose-Headers", "Accept-Ranges, Content-Range, ETag")
	}
	switch r.Method {
	case http.MethodOptions:
		w.Header().Set("Allow", fileAllow)
		if origin != "" {
			w.Header().Set("Access-Control-Allow-Methods", fileAllow)
			w.Header().Set("Access-Control-Allow-Headers", "Range")
		}
		w.WriteHeader(http.StatusNoContent)
		return
	case http.MethodGet, http.MethodHead:
	default:
		w.Header().Set("Allow", fileAllow)
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	// net/http discards an unread request body when the first response
	// bytes go out, and that read would wait on the client while the
	// file slot is held.
	if r.ContentLength != 0 || len(r.TransferEncoding) > 0 {
		w.Header().Set("Connection", "close")
		http.Error(w, "a file request carries no body", http.StatusBadRequest)
		return
	}
	select {
	case s.fileSem <- struct{}{}:
		defer func() { <-s.fileSem }()
	default:
		http.Error(w, "too many file transfers in flight", http.StatusServiceUnavailable)
		return
	}

	c, _, bv, err := s.findDAHash(h)
	if err != nil {
		var re *rpcError
		switch {
		case errors.As(err, &re) && re.Code == codeNotFound:
			http.NotFound(w, r)
		case errors.As(err, &re):
			http.Error(w, re.Message, http.StatusServiceUnavailable)
		default:
			s.fileFailure(w, route, err)
		}
		return
	}

	if bundle {
		f, size, err := openRegular(c.cfg.Source.bundlePath(h), maxBundleBytes)
		if err != nil {
			s.fileReadFailure(w, r, route, err)
			return
		}
		defer f.Close()
		s.sendFile(w, r, h, io.NewSectionReader(f, 0, size))
		return
	}
	data, err := c.cfg.Source.Receipts(h)
	if err != nil {
		s.fileReadFailure(w, r, route, err)
		return
	}
	_, leaves, err := codec.DecodeReceiptSidecar(data)
	if err != nil || codec.MerkleRoot(leaves) != bv.ReceiptRoot {
		s.fileFailure(w, route, fmt.Errorf("receipt sidecar %s no longer matches receiptRoot %s", h, bv.ReceiptRoot))
		return
	}
	s.sendFile(w, r, h, bytes.NewReader(data))
}

// fileReadFailure answers a failed file read: 404 for a missing file,
// else 500.
func (s *Server) fileReadFailure(w http.ResponseWriter, r *http.Request, route string, err error) {
	if errors.Is(err, fs.ErrNotExist) {
		http.NotFound(w, r)
		return
	}
	s.fileFailure(w, route, err)
}

// fileFailure answers 500 and logs err.
func (s *Server) fileFailure(w http.ResponseWriter, route string, err error) {
	s.logInternal(route, err)
	http.Error(w, "internal error", http.StatusInternalServerError)
}

// sendFile writes content as the response to r, with ranges, HEAD and
// the conditional headers served against daHash's ETag. Each write
// extends the connection's write deadline, up to fileTransferMax from
// the start.
func (s *Server) sendFile(w http.ResponseWriter, r *http.Request, daHash types.Hash, content io.ReadSeeker) {
	// The errors are ignored: a recorder does not support deadlines,
	// and on a real connection any fault shows in the writes. The read
	// deadline moves to the end of the transfer so net/http's
	// background read, which the server's ReadTimeout would expire
	// mid-transfer, does not cancel the request's context, and no read
	// outlives the transfer cap.
	stop := time.Now().Add(s.fileTransferMax)
	rc := http.NewResponseController(w)
	_ = rc.SetReadDeadline(stop)
	hd := w.Header()
	hd.Set("Content-Type", "application/octet-stream")
	hd.Set("X-Content-Type-Options", "nosniff")
	hd.Set("ETag", `"`+hex.EncodeToString(daHash[:])+`"`)
	dw := &deadlineWriter{ResponseWriter: w, rc: rc, wait: s.fileWriteWait, stop: stop}
	dw.extend()
	http.ServeContent(dw, r, "", time.Time{}, content)
}

// deadlineWriter extends the write deadline before every write. It
// does not implement io.ReaderFrom, so io.Copy writes in pieces
// through Write.
type deadlineWriter struct {
	http.ResponseWriter
	rc   *http.ResponseController
	wait time.Duration
	stop time.Time
}

// extend sets the write deadline to wait from now, or stop when that
// is earlier.
func (d *deadlineWriter) extend() {
	t := time.Now().Add(d.wait)
	if t.After(d.stop) {
		t = d.stop
	}
	_ = d.rc.SetWriteDeadline(t)
}

func (d *deadlineWriter) Write(p []byte) (int, error) {
	d.extend()
	return d.ResponseWriter.Write(p)
}

// openRegular opens the regular file at path without following a
// symlink or blocking on a FIFO, and returns it with its size. A file
// above limit is refused with an error wrapping recovery.ErrTooLarge.
func openRegular(path string, limit int64) (*os.File, int64, error) {
	f, err := os.OpenFile(path, os.O_RDONLY|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0)
	if err != nil {
		return nil, 0, err
	}
	info, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, 0, err
	}
	if !info.Mode().IsRegular() {
		f.Close()
		return nil, 0, fmt.Errorf("%s is not a regular file", info.Name())
	}
	if info.Size() > limit {
		f.Close()
		return nil, 0, fmt.Errorf("%s is %w: %d bytes, above the %d byte limit", info.Name(), recovery.ErrTooLarge, info.Size(), limit)
	}
	return f, info.Size(), nil
}
