package gateway

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/zenon-network/go-zenon/common/types"
	"golang.org/x/crypto/sha3"

	"github.com/Zeno-L2/zeno-executor/internal/codec"
	"github.com/Zeno-L2/zeno-executor/internal/recovery"
)

const (
	// maxFetchBytes is the most bytes one bundle fetch reads: the
	// same ceiling as a LOCAL bundle read.
	maxFetchBytes = maxBundleBytes
	// fetchStallWait cancels an upstream attempt that makes no
	// progress for this long.
	fetchStallWait = 10 * time.Second
	// maxResponseHeader caps an upstream response's headers.
	maxResponseHeader = 64 << 10
	// tmpPrefix starts the name of a download in progress.
	tmpPrefix = ".tmp-"
)

var (
	errNoUpstream = errors.New("no upstream served the batch's files")
	errStalled    = errors.New("no bytes arrived")
	// portRun matches the port of a host:port in an error's text.
	portRun = regexp.MustCompile(`:[0-9]+`)
)

// RemoteSource serves a domain's batch files from the gateway's own
// verified copies, which fetch downloads from the configured
// upstreams. The embedded LocalSource reads the copies.
type RemoteSource struct {
	*LocalSource // reads the cache: <storagePath>/<domainId>/bundles and receipts
	domainID     uint32
	root         string
	bundles      string
	receipts     string
	raw          []string // upstreams as configured, for redaction
	upstreams    []*url.URL
	client       *http.Client
	logf         func(format string, args ...any)
	maxBundle    int64         // maxFetchBytes; tests lower it
	stallWait    time.Duration // fetchStallWait; tests lower it
	// first is the upstream the next file tries first. Only Poll
	// calls fetch, so first and skipLogged need no lock.
	first int
	// skipLogged maps an upstream and file kind to the masked reason
	// of the last skip line logged for them: at most 2*maxUpstreams
	// entries.
	skipLogged map[skipKey]string
}

// skipKey names one upstream's skip-line entry for one file kind.
type skipKey struct {
	upstream int
	kind     string // "bundle" or "receipt sidecar"
}

// fetchCut is a fetch whose context ended while it downloaded kind
// from upstreams[upstream].
type fetchCut struct {
	kind     string // "bundle" or "receipt sidecar"
	upstream int
	first    bool  // the cut attempt was the poll's first download attempt
	err      error // the context's error
}

func (e *fetchCut) Error() string { return e.err.Error() }
func (e *fetchCut) Unwrap() error { return e.err }

// cacheFault is a disk error in the gateway's own cache files.
type cacheFault struct {
	op, dir string // the operation and the cache directory it ran in
	err     error
}

func (e *cacheFault) Error() string {
	errno := e.err
	var pe *fs.PathError
	var le *os.LinkError
	switch {
	case errors.As(e.err, &pe):
		errno = pe.Err
	case errors.As(e.err, &le):
		errno = le.Err
	}
	return fmt.Sprintf("gateway cache: %s in %s: %v", e.op, e.dir, errno)
}

func (e *cacheFault) Unwrap() error { return e.err }

// progressReader resets the attempt's stall timer on every read that
// returns bytes, and records the last read error other than io.EOF.
type progressReader struct {
	r    io.Reader
	t    *time.Timer
	wait time.Duration
	err  error
}

func (p *progressReader) Read(b []byte) (int, error) {
	n, err := p.r.Read(b)
	if n > 0 {
		p.t.Reset(p.wait)
	}
	if err != nil && err != io.EOF {
		p.err = err
	}
	return n, err
}

// NewRemoteSource returns the source for domainID that keeps its
// verified files under storagePath and fetches them from upstreams,
// which are base URLs. A nil logf selects log.Printf.
func NewRemoteSource(storagePath string, domainID uint32, upstreams []string, logf func(format string, args ...any)) (*RemoteSource, error) {
	if logf == nil {
		logf = log.Printf
	}
	local := NewLocalSource(storagePath, domainID)
	root, bundles, receipts := local.layout.Root, local.layout.Bundles, local.layout.Receipts
	if err := guardCache(root); err != nil {
		return nil, fmt.Errorf("gateway: domain %d: %w", domainID, err)
	}
	for _, dir := range []string{bundles, receipts} {
		if err := os.MkdirAll(dir, 0o700); err != nil {
			return nil, fmt.Errorf("gateway: domain %d: %w", domainID, err)
		}
	}
	for _, dir := range []string{root, bundles, receipts} {
		if err := secureLeaf(dir, euid()); err != nil {
			return nil, fmt.Errorf("gateway: domain %d: %w", domainID, err)
		}
	}
	for _, dir := range []string{bundles, receipts} {
		entries, err := os.ReadDir(dir)
		if err != nil {
			return nil, fmt.Errorf("gateway: domain %d: %w", domainID, err)
		}
		for _, e := range entries {
			if strings.HasPrefix(e.Name(), tmpPrefix) {
				if err := os.Remove(filepath.Join(dir, e.Name())); err != nil && !errors.Is(err, fs.ErrNotExist) {
					return nil, fmt.Errorf("gateway: domain %d: %w", domainID, err)
				}
			}
		}
	}
	urls := make([]*url.URL, len(upstreams))
	for i, raw := range upstreams {
		u, err := url.Parse(raw)
		if err != nil {
			return nil, fmt.Errorf("%w: upstream %s does not parse", ErrConfig, redactEndpoint(raw))
		}
		urls[i] = u
	}
	tr := http.DefaultTransport.(*http.Transport).Clone()
	tr.DisableCompression = true
	tr.MaxResponseHeaderBytes = maxResponseHeader
	return &RemoteSource{
		LocalSource: local,
		domainID:    domainID,
		root:        root,
		bundles:     bundles,
		receipts:    receipts,
		raw:         append([]string(nil), upstreams...),
		upstreams:   urls,
		client: &http.Client{
			Transport:     tr,
			CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
		},
		logf:       logf,
		maxBundle:  maxFetchBytes,
		stallWait:  fetchStallWait,
		skipLogged: make(map[skipKey]string),
	}, nil
}

// Record reports no batch record: a REMOTE domain has no pending
// batches.
func (s *RemoteSource) Record(uint64) (recovery.BatchRecord, bool, error) {
	return recovery.BatchRecord{}, false, nil
}

// drop removes the cache entry at path; a missing entry is fine.
func (s *RemoteSource) drop(dir, path string) error {
	if err := os.Remove(path); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return &cacheFault{op: "remove", dir: dir, err: err}
	}
	return nil
}

// bundleCached reports whether the cache holds daHash's bundle. A
// cached file that fails the check is removed.
func (s *RemoteSource) bundleCached(daHash types.Hash) (bool, error) {
	path := recovery.BundlePath(s.bundles, daHash)
	f, _, err := openRegular(path, s.maxBundle)
	if err == nil {
		h := sha3.New256()
		_, err = io.Copy(h, f)
		f.Close()
		if err == nil && bytes.Equal(h.Sum(nil), daHash[:]) {
			return true, nil
		}
	}
	if errors.Is(err, fs.ErrNotExist) {
		return false, nil
	}
	return false, s.drop(s.bundles, path)
}

// sidecarCached reports whether the cache holds a sidecar for daHash
// with root receiptRoot. A cached file that fails the check is
// removed.
func (s *RemoteSource) sidecarCached(daHash, receiptRoot types.Hash) (bool, error) {
	path := recovery.ReceiptsPath(s.receipts, daHash)
	data, err := recovery.ReadRegularFile(path, maxSidecarSize)
	if err == nil {
		_, leaves, derr := codec.DecodeReceiptSidecar(data)
		if derr == nil && codec.MerkleRoot(leaves) == receiptRoot {
			return true, nil
		}
	}
	if errors.Is(err, fs.ErrNotExist) {
		return false, nil
	}
	return false, s.drop(s.receipts, path)
}

// fetch makes the cache hold daHash's bundle and a sidecar with root
// receiptRoot. firstInPoll is true when no batch was accepted earlier
// in this Poll.
func (s *RemoteSource) fetch(ctx context.Context, batchID uint64, daHash, receiptRoot types.Hash, firstInPoll bool) error {
	if err := guardCache(s.root); err != nil {
		return err
	}
	haveBundle, err := s.bundleCached(daHash)
	if err != nil {
		return err
	}
	haveSidecar, err := s.sidecarCached(daHash, receiptRoot)
	if err != nil {
		return err
	}
	files := []struct {
		kind, seg, dir, final string
		have                  bool
		limit                 int64
		verify                func(tmp string, sum []byte) error
	}{
		{"bundle", "bundles", s.bundles, recovery.BundlePath(s.bundles, daHash), haveBundle, s.maxBundle,
			func(_ string, sum []byte) error {
				if !bytes.Equal(sum, daHash[:]) {
					return errors.New("the bytes do not hash to daHash")
				}
				return nil
			}},
		{"receipt sidecar", "receipts", s.receipts, recovery.ReceiptsPath(s.receipts, daHash), haveSidecar, maxSidecarSize,
			func(tmp string, _ []byte) error {
				data, err := recovery.ReadRegularFile(tmp, maxSidecarSize)
				if err != nil {
					return &cacheFault{op: "read", dir: s.receipts, err: err}
				}
				_, leaves, err := codec.DecodeReceiptSidecar(data)
				if err != nil {
					return fmt.Errorf("the sidecar does not decode: %w", err)
				}
				if root := codec.MerkleRoot(leaves); root != receiptRoot {
					return fmt.Errorf("the sidecar root %s is not receiptRoot %s", root, receiptRoot)
				}
				return nil
			}},
	}
	n := len(s.upstreams)
	attempted := false
	for _, f := range files {
		if f.have {
			continue
		}
		start := s.first
		served := false
		for k := 0; k < n; k++ {
			i := (start + k) % n
			firstAttempt := firstInPoll && !attempted
			attempted = true
			err := s.download(ctx, i, f.seg, daHash, f.dir, f.final, f.limit, f.verify)
			if err == nil {
				s.first = i
				delete(s.skipLogged, skipKey{i, f.kind})
				served = true
				break
			}
			if ctx.Err() != nil {
				if firstAttempt {
					s.first = (i + 1) % n
				} else {
					s.first = i
				}
				return &fetchCut{kind: f.kind, upstream: i, first: firstAttempt, err: ctx.Err()}
			}
			var cf *cacheFault
			if errors.As(err, &cf) {
				return err
			}
			s.first = (i + 1) % n
			s.logSkip(batchID, i, f.kind, daHash, err)
		}
		if !served {
			return fmt.Errorf("%w: %s %s", errNoUpstream, f.kind, daHash)
		}
	}
	return nil
}

// logSkip logs the skip of upstreams[i] for a file of kind, once until
// its reason, with hashes and ports masked, changes.
func (s *RemoteSource) logSkip(batchID uint64, i int, kind string, daHash types.Hash, err error) {
	reason := RedactEndpointErr(s.raw[i], err)
	key := portRun.ReplaceAllString(hexRun.ReplaceAllString(reason, "<hex>"), ":<port>")
	sk := skipKey{i, kind}
	if s.skipLogged[sk] == key {
		return
	}
	s.skipLogged[sk] = key
	s.logf("gateway: domain %d: batch %d: skipped upstreams[%d] %s for %s %s: %q", s.domainID, batchID, i, redactEndpoint(s.raw[i]), kind, daHash, reason)
}

// stalledOr returns the stall error in place of err when the attempt's
// own stall timer, not the poll's context, ended the attempt.
func (s *RemoteSource) stalledOr(ctx, actx context.Context, err error) error {
	if ctx.Err() == nil && context.Cause(actx) == errStalled {
		return fmt.Errorf("%w for %s", errStalled, s.stallWait)
	}
	return err
}

// download fetches seg/<daHash> from upstreams[i] into dir, checks it
// with verify (given the file's path and its SHA3-256) and moves it to
// final. It returns a *cacheFault for a disk error in dir and any
// other error for a fault of the upstream.
func (s *RemoteSource) download(ctx context.Context, i int, seg string, daHash types.Hash, dir, final string, limit int64, verify func(tmp string, sum []byte) error) (err error) {
	actx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	stall := time.AfterFunc(s.stallWait, func() { cancel(errStalled) })
	defer stall.Stop()

	u := s.upstreams[i].JoinPath(seg, hex.EncodeToString(daHash[:]))
	req, err := http.NewRequestWithContext(actx, http.MethodGet, u.String(), nil)
	if err != nil {
		return err
	}
	resp, err := s.client.Do(req)
	if err != nil {
		return s.stalledOr(ctx, actx, err)
	}
	defer resp.Body.Close()
	stall.Reset(s.stallWait)
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("HTTP status %d", resp.StatusCode)
	}
	if resp.ContentLength > limit {
		return fmt.Errorf("Content-Length %d is above the %d byte limit", resp.ContentLength, limit)
	}

	f, err := os.CreateTemp(dir, tmpPrefix+"*")
	if err != nil {
		return &cacheFault{op: "create", dir: dir, err: err}
	}
	tmp := f.Name()
	defer func() {
		if err != nil {
			f.Close()
			os.Remove(tmp)
		}
	}()
	if err := f.Chmod(0o600); err != nil {
		return &cacheFault{op: "chmod", dir: dir, err: err}
	}
	h := sha3.New256()
	pr := &progressReader{r: resp.Body, t: stall, wait: s.stallWait}
	n, err := io.Copy(io.MultiWriter(f, h), io.LimitReader(pr, limit+1))
	stall.Stop()
	if err != nil {
		if pr.err == nil {
			return &cacheFault{op: "write", dir: dir, err: err}
		}
		return s.stalledOr(ctx, actx, pr.err)
	}
	if n > limit {
		return fmt.Errorf("the body passed the %d byte limit", limit)
	}
	if err := f.Sync(); err != nil {
		return &cacheFault{op: "sync", dir: dir, err: err}
	}
	if err := f.Close(); err != nil {
		return &cacheFault{op: "close", dir: dir, err: err}
	}
	if err := verify(tmp, h.Sum(nil)); err != nil {
		return err
	}
	if err := os.Rename(tmp, final); err != nil {
		return &cacheFault{op: "rename", dir: dir, err: err}
	}
	return syncDirectory(dir)
}

// syncDirectory fsyncs dir, which makes a rename inside it durable.
func syncDirectory(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return &cacheFault{op: "sync directory", dir: dir, err: err}
	}
	if err := d.Sync(); err != nil {
		d.Close()
		return &cacheFault{op: "sync directory", dir: dir, err: err}
	}
	if err := d.Close(); err != nil {
		return &cacheFault{op: "sync directory", dir: dir, err: err}
	}
	return nil
}
