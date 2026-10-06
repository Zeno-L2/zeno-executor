package supervisor

import (
	"context"
	"errors"
	"fmt"
	"log"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/digitalSloth/go-executor/internal/codec"
	"github.com/digitalSloth/go-executor/internal/ipc"
	"github.com/digitalSloth/go-executor/internal/lease"
	"github.com/digitalSloth/go-executor/internal/workerproc"
)

// serveDrainTimeout bounds how long runOnce waits for serve to return after a worker's exit is
// already known. A descendant the worker leaves behind can hold the channel's worker end open
// past the process-group and cgroup kill (it may already have left both), which would otherwise
// leave serve blocked in Read and runOnce, and so Run, waiting forever.
const serveDrainTimeout = time.Second

// State is a worker slot's supervision state.
type State string

const (
	StateRunning     State = "RUNNING"
	StateBackoff     State = "BACKOFF"
	StateHalted      State = "HALTED"
	StateConfigError State = "CONFIG_ERROR"
	StateStopped     State = "STOPPED" // before Run starts the worker, and after shutdown
)

// WorkerStatus is a snapshot of one configured worker's supervision state.
type WorkerStatus struct {
	DomainID      uint32
	State         State
	PID           int    // 0 when no process is running
	Restarts      int    // start attempts after the first, successful or not
	LastExit      string // see exitString and the runOnce kill paths
	ServedHeight  uint64
	LastCommitted *codec.BatchCommitment // a copy; nil until the first valid Committed
}

// Supervisor starts and supervises one worker process per configured domain.
type Supervisor struct {
	cfg    Config
	key    []byte // held for signing later; never logged, formatted or serialised
	src    Sources
	policy Policy // WithDefaults applied in New

	mu    sync.Mutex // guards every slot's state, pid, restarts, lastExit
	slots []*slot    // config order
}

// slot is one configured worker's supervision state.
type slot struct {
	w        WorkerConfig
	st       *workerState // one per slot for the Supervisor's lifetime; highWater, servedHeight, lastCommitted carry across runs
	state    State
	pid      int
	restarts int
	lastExit string
}

// New constructs a Supervisor for cfg. cfg must come from LoadConfig; New does not re-validate
// it.
func New(cfg Config, key []byte, src Sources, policy Policy) *Supervisor {
	slots := make([]*slot, len(cfg.Workers))
	for i, w := range cfg.Workers {
		slots[i] = &slot{w: w, st: &workerState{}, state: StateStopped}
	}
	return &Supervisor{cfg: cfg, key: key, src: src, policy: policy.WithDefaults(), slots: slots}
}

// Run acquires the executor's single-instance lease, prepares every worker's storage directory
// and, once Sources.Ready reports true, starts one supervision goroutine per configured worker.
// It returns nil without starting a worker if Ready returns false. It returns once ctx is done
// and every worker has stopped. Call it at most once.
func (s *Supervisor) Run(ctx context.Context) error {
	if err := workerproc.SealInheritedFDs(); err != nil {
		return fmt.Errorf("supervisor: %w", err)
	}
	if err := workerproc.SetNonDumpable(); err != nil {
		return fmt.Errorf("supervisor: %w", err)
	}

	if err := os.MkdirAll(s.cfg.StoragePath, 0o755); err != nil {
		return fmt.Errorf("supervisor: %w", err)
	}
	supDir := filepath.Join(s.cfg.StoragePath, "supervisor")
	if err := os.MkdirAll(supDir, 0o700); err != nil {
		return fmt.Errorf("supervisor: %w", err)
	}

	lockPath := filepath.Join(supDir, s.cfg.ExecutorAddress.String()+".lock")
	held, err := lease.Acquire(lockPath)
	if err != nil {
		return fmt.Errorf("supervisor: %w", err)
	}
	defer held.Release()

	host := workerproc.NewHost()
	limits, uids, reason := host.Enforcement()
	line := fmt.Sprintf("supervisor: enforcement: limits %s, uids %s", onOff(limits), onOff(uids))
	if reason != "" {
		line += ": " + reason
	}
	log.Print(line)

	if uids {
		if err := checkWorkerAccess(s.cfg); err != nil {
			return fmt.Errorf("supervisor: %w", err)
		}
	}

	for _, sl := range s.slots {
		dir := filepath.Join(s.cfg.StoragePath, strconv.FormatUint(uint64(sl.w.DomainID), 10))
		if err := host.PrepareDir(dir, workerUID(sl.w.DomainID)); err != nil {
			return fmt.Errorf("supervisor: %w", err)
		}
	}

	if s.src.Ready != nil && !s.src.Ready(ctx) {
		return nil
	}

	var wg sync.WaitGroup
	for _, sl := range s.slots {
		wg.Add(1)
		go func(sl *slot) {
			defer wg.Done()
			s.supervise(ctx, host, sl)
		}(sl)
	}

	<-ctx.Done()
	wg.Wait()
	return nil
}

// Status reports every configured worker's current state, in configuration order. It is safe to
// call at any time, including while Run is starting up or shutting down.
func (s *Supervisor) Status() []WorkerStatus {
	s.mu.Lock()
	defer s.mu.Unlock()

	out := make([]WorkerStatus, len(s.slots))
	for i, sl := range s.slots {
		sl.st.mu.Lock()
		var lastCommitted *codec.BatchCommitment
		if sl.st.lastCommitted != nil {
			cc := *sl.st.lastCommitted
			lastCommitted = &cc
		}
		served := sl.st.servedHeight
		sl.st.mu.Unlock()

		out[i] = WorkerStatus{
			DomainID:      sl.w.DomainID,
			State:         sl.state,
			PID:           sl.pid,
			Restarts:      sl.restarts,
			LastExit:      sl.lastExit,
			ServedHeight:  served,
			LastCommitted: lastCommitted,
		}
	}
	return out
}

// supervise runs sl's worker to completion, restarting it under s.policy until ctx ends or the
// worker reaches a terminal state (HALTED or CONFIG_ERROR).
func (s *Supervisor) supervise(ctx context.Context, host *workerproc.Host, sl *slot) {
	var backoff Backoff
	first := true

	for {
		if ctx.Err() != nil {
			s.mu.Lock()
			sl.state = StateStopped
			sl.pid = 0
			s.mu.Unlock()
			return
		}

		if !first {
			s.mu.Lock()
			sl.restarts++
			s.mu.Unlock()
		}
		first = false

		end, shutdown := s.runOnce(ctx, host, sl)

		// A dead worker never reports a live-looking PID, whatever state it lands in.
		s.mu.Lock()
		sl.pid = 0
		lastExit := sl.lastExit
		s.mu.Unlock()

		if shutdown {
			s.mu.Lock()
			sl.state = StateStopped
			s.mu.Unlock()
			return
		}

		action, wait, next := s.policy.Next(backoff, end)
		backoff = next

		switch action {
		case ActionRestart:
			s.mu.Lock()
			sl.state = StateBackoff
			s.mu.Unlock()
			log.Printf("supervisor: domain %d worker ended (%s); restarting in %s", sl.w.DomainID, lastExit, wait)

			t := time.NewTimer(wait)
			select {
			case <-ctx.Done():
				t.Stop()
				s.mu.Lock()
				sl.state = StateStopped
				s.mu.Unlock()
				return
			case <-t.C:
			}

		case ActionHalt:
			s.mu.Lock()
			sl.state = StateHalted
			s.mu.Unlock()
			log.Printf("supervisor: domain %d HALTED: worker halted (%s)", sl.w.DomainID, lastExit)
			return

		case ActionConfigError:
			s.mu.Lock()
			sl.state = StateConfigError
			s.mu.Unlock()
			log.Printf("supervisor: domain %d CONFIG_ERROR: worker rejected its configuration (%s)", sl.w.DomainID, lastExit)
			return

		case ActionCrashLoop:
			s.mu.Lock()
			sl.state = StateHalted
			s.mu.Unlock()
			log.Printf("supervisor: domain %d HALTED: crash loop after %d failures without progress (last: %s)", sl.w.DomainID, backoff.Failures, lastExit)
			return
		}
	}
}

// runOnce starts and supervises one attempt at running sl's worker, to completion. shutdown is
// true when the attempt ended because ctx was done, not because the worker exited on its own.
func (s *Supervisor) runOnce(ctx context.Context, host *workerproc.Host, sl *slot) (end RunEnd, shutdown bool) {
	w := sl.w
	st := sl.st
	limitsOn, uidsOn, _ := host.Enforcement()

	supEnd, workerEnd, err := workerproc.Socketpair()
	var p *workerproc.Process
	if err == nil {
		m := w.MemoryLimit
		p, err = host.Start(workerproc.Spec{
			Path:   w.Binary,
			Name:   "worker-" + strconv.FormatUint(uint64(w.DomainID), 10),
			UID:    workerUID(w.DomainID),
			Env:    []string{"GOMEMLIMIT=" + strconv.FormatInt(m-m/10, 10)},
			Conn:   workerEnd,
			Limits: workerproc.Limits{MemoryBytes: w.MemoryLimit, CPUs: w.CPULimit},
		})
	}
	if workerEnd != nil {
		workerEnd.Close()
	}
	if err != nil {
		if supEnd != nil {
			supEnd.Close()
		}
		s.mu.Lock()
		sl.lastExit = "start: " + err.Error()
		s.mu.Unlock()
		return RunEnd{StartFailed: true}, false
	}

	st.mu.Lock()
	st.lastFrame = time.Now()
	st.progressed = false
	st.mu.Unlock()

	s.mu.Lock()
	sl.state = StateRunning
	sl.pid = p.PID()
	s.mu.Unlock()

	logUID := workerUID(w.DomainID)
	if !uidsOn {
		logUID = os.Getuid()
	}
	log.Printf("supervisor: domain %d worker started: pid %d, uid %d, limits %s", w.DomainID, p.PID(), logUID, onOff(limitsOn))

	// The write error is ignored: a worker that never gets a usable Config exits during its own
	// validation, and its exit status classifies the run.
	_ = ipc.WriteMessage(supEnd, s.cfg.workerConfig(w))

	serveErr := make(chan error, 1)
	go func() { serveErr <- serve(supEnd, w, s.cfg.ExecutorAddress, s.src, st) }()

	exitCh := make(chan workerproc.Exit, 1)
	go func() { exitCh <- p.Wait() }()

	started := time.Now()
	killReason := ""

	serveErrCh := serveErr // set to nil once its one value has been received
	serveArrived := false

	liveness := time.NewTimer(s.policy.Liveness)
	defer liveness.Stop()

	var exit workerproc.Exit

loop:
	for {
		select {
		case err := <-serveErrCh:
			serveArrived = true
			serveErrCh = nil
			if err != nil && errors.Is(err, ErrMalformed) && killReason == "" {
				log.Printf("supervisor: domain %d %v", w.DomainID, err)
				killReason = "malformed message: " + err.Error()
				_ = p.Signal(syscall.SIGKILL)
			}
			// A nil error, or one arriving after a kill reason is already set, changes nothing:
			// keep waiting for the exit. Liveness still applies.

		case <-liveness.C:
			st.mu.Lock()
			since := time.Since(st.lastFrame)
			st.mu.Unlock()
			if since >= s.policy.Liveness {
				if killReason == "" {
					killReason = "liveness timeout"
					_ = p.Signal(syscall.SIGKILL)
				}
				// A kill reason from elsewhere already covers this run; do not reset a timer
				// that has nothing left to wait for.
			} else if killReason == "" {
				liveness.Reset(s.policy.Liveness - since)
			}

		case exit = <-exitCh:
			break loop

		case <-ctx.Done():
			_ = p.Signal(syscall.SIGTERM)
			grace := time.NewTimer(s.policy.StopGrace)
			select {
			case exit = <-exitCh:
				grace.Stop()
			case <-grace.C:
				_ = p.Signal(syscall.SIGKILL)
				exit = <-exitCh
			}
			shutdown = true
			break loop
		}
	}

	// serve always sees EOF here, because no other process the supervisor itself started holds
	// the worker end. A descendant the worker left behind (outside its process group, or on a
	// host with limits off) can still hold it open; bound the wait and force the read closed
	// rather than block Run forever.
	if !serveArrived {
		select {
		case <-serveErr:
		case <-time.After(serveDrainTimeout):
			_ = syscall.Shutdown(int(supEnd.Fd()), syscall.SHUT_RDWR)
			<-serveErr
		}
	}
	supEnd.Close()

	// killReason explains LastExit only when the kill it describes is what actually ended the
	// run: a worker that reaches its own exit (3, 75, ...) in the race against a liveness or
	// malformed-frame SIGKILL is still classified, and reported, by that real exit.
	lastExit := ""
	if killReason != "" && exit.Signal == syscall.SIGKILL {
		lastExit = killReason
	}
	if lastExit == "" {
		lastExit = exitString(exit)
	}
	s.mu.Lock()
	sl.lastExit = lastExit
	s.mu.Unlock()

	st.mu.Lock()
	progressed := st.progressed
	st.mu.Unlock()

	return RunEnd{Exit: exit, Ran: time.Since(started), Progressed: progressed}, shutdown
}

// exitString describes how a worker run ended, for LastExit. A killReason that actually landed
// (the process died by SIGKILL), and the "start: ..." text runOnce sets on a start failure,
// override it.
func exitString(e workerproc.Exit) string {
	switch {
	case e.OOMKilled:
		return "memory limit"
	case e.Signal != 0:
		return "signal " + e.Signal.String()
	default:
		return fmt.Sprintf("exit status %d", e.Code)
	}
}

// workerUID is the uid a worker for domainID runs under when the host separates worker uids.
func workerUID(domainID uint32) int {
	return 10000 + int(domainID)
}

func onOff(b bool) string {
	if b {
		return "on"
	}
	return "off"
}

// checkWorkerAccess runs the uid-separation checks that must hold before any worker starts: it
// runs only when the host separates worker uids. Every error wraps ErrConfig and names the
// offending item.
func checkWorkerAccess(cfg Config) error {
	for i, w := range cfg.Workers {
		if w.DomainID > math.MaxInt32-10000 {
			return fmt.Errorf("%w: workers[%d].domainId %d is too large for a worker uid", ErrConfig, i, w.DomainID)
		}
	}

	if err := checkReachable(cfg.StoragePath); err != nil {
		return err
	}
	for _, w := range cfg.Workers {
		if err := checkReachable(w.Binary); err != nil {
			return err
		}
	}

	if cfg.SubmissionKeySource.Kind == "FILE" {
		if err := checkKeyNotWorkerReadable(cfg); err != nil {
			return err
		}
	}

	return nil
}

// checkReachable rejects path unless every directory a worker uid must traverse to reach it
// grants other-execute. It checks both path as configured, the form a worker's own exec or
// MkdirAll call actually walks, and its filepath.EvalSymlinks resolution, the form its final
// component's real permissions and ownership belong to: a symlink sitting in an unreachable
// directory would otherwise pass by resolving to a reachable target, and then fail for the
// worker at exec or open with no check having caught it first.
func checkReachable(path string) error {
	if err := checkPrefixChain(path); err != nil {
		return err
	}
	resolved, err := filepath.EvalSymlinks(path)
	if err != nil {
		return fmt.Errorf("%w: %s: %w", ErrConfig, path, err)
	}
	if resolved != path {
		if err := checkPrefixChain(resolved); err != nil {
			return err
		}
	}
	return nil
}

// checkPrefixChain os.Stats path and every parent directory up to "/", failing at the first
// component that does not grant other-execute.
func checkPrefixChain(path string) error {
	for {
		fi, err := os.Stat(path)
		if err != nil {
			return fmt.Errorf("%w: %s: %w", ErrConfig, path, err)
		}
		if fi.Mode().Perm()&0o001 == 0 {
			return fmt.Errorf("%w: %s is not reachable by worker users (no other-execute bit)", ErrConfig, path)
		}
		parent := filepath.Dir(path)
		if parent == path {
			return nil
		}
		path = parent
	}
}

// checkKeyNotWorkerReadable rejects a FILE submission key source that a worker could read
// despite passing LoadKey's mode-only check: one owned by a configured worker uid, and one
// whose path a domain's own PrepareDir call would later hand to that worker by lying inside its
// storage directory. It does not require the key's owner to equal the supervisor's own euid,
// since a legitimate bind mount can present a different owner.
func checkKeyNotWorkerReadable(cfg Config) error {
	path := cfg.SubmissionKeySource.Path
	resolved, err := filepath.EvalSymlinks(path)
	if err != nil {
		return fmt.Errorf("%w: submissionKeySource path %q: %w", ErrConfig, path, err)
	}
	// LoadConfig rejects a relative FILE path, but New does not re-validate its cfg; make the
	// containment comparison below robust on its own, since a relative resolved path would
	// never match the always-absolute domainDir it is compared against.
	if !filepath.IsAbs(resolved) {
		abs, err := filepath.Abs(resolved)
		if err != nil {
			return fmt.Errorf("%w: submissionKeySource path %q: %w", ErrConfig, path, err)
		}
		resolved = abs
	}

	fi, err := os.Stat(resolved)
	if err != nil {
		return fmt.Errorf("%w: submissionKeySource path %q: %w", ErrConfig, path, err)
	}
	if st, ok := fi.Sys().(*syscall.Stat_t); ok {
		for _, w := range cfg.Workers {
			if int(st.Uid) == workerUID(w.DomainID) {
				return fmt.Errorf("%w: submissionKeySource path %q is owned by domain %d's worker uid", ErrConfig, path, w.DomainID)
			}
		}
	}

	for _, w := range cfg.Workers {
		domainDir := filepath.Join(cfg.StoragePath, strconv.FormatUint(uint64(w.DomainID), 10))
		resolvedDomainDir, err := filepath.EvalSymlinks(domainDir)
		if err != nil {
			resolvedDomainDir = domainDir // not created yet; compare against the configured path
		}
		if resolved == resolvedDomainDir || strings.HasPrefix(resolved, resolvedDomainDir+string(filepath.Separator)) {
			return fmt.Errorf("%w: submissionKeySource path %q is inside domain %d's storage directory", ErrConfig, path, w.DomainID)
		}
	}

	return nil
}
