package supervisor

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/zenon-network/go-zenon/chain/nom"
	"github.com/zenon-network/go-zenon/common/types"

	"github.com/digitalSloth/go-executor/internal/codec"
	"github.com/digitalSloth/go-executor/internal/ipc/ipctest"
	"github.com/digitalSloth/go-executor/internal/lease"
	"github.com/digitalSloth/go-executor/internal/recovery"
	"github.com/digitalSloth/go-executor/internal/workerproc"
)

// --- test worker binary ---

var workerBinary string

func TestMain(m *testing.M) {
	dir, err := os.MkdirTemp("", "supervisor-testbin-")
	if err != nil {
		fmt.Fprintln(os.Stderr, "MkdirTemp:", err)
		os.Exit(1)
	}
	if err := os.Chmod(dir, 0o755); err != nil {
		fmt.Fprintln(os.Stderr, "Chmod:", err)
		os.Exit(1)
	}

	workerBinary = filepath.Join(dir, "worker")
	build := exec.Command("go", "build", "-o", workerBinary, "github.com/digitalSloth/go-executor/internal/testbin/worker")
	build.Stdout = os.Stderr
	build.Stderr = os.Stderr
	if err := build.Run(); err != nil {
		fmt.Fprintln(os.Stderr, "build test worker:", err)
		os.RemoveAll(dir)
		os.Exit(1)
	}

	code := m.Run()
	os.RemoveAll(dir)
	os.Exit(code)
}

// openTempDir returns a 0755 temp directory whose parent is also 0755, since t.TempDir's parent
// is 0700 and a uid-separated worker must be able to reach it.
func openTempDir(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	if err := os.Chmod(dir, 0o755); err != nil {
		t.Fatalf("Chmod %s: %v", dir, err)
	}
	if err := os.Chmod(filepath.Dir(dir), 0o755); err != nil {
		t.Fatalf("Chmod %s: %v", filepath.Dir(dir), err)
	}
	return dir
}

func copyBinary(t *testing.T, src, dst string) {
	t.Helper()
	data, err := os.ReadFile(src)
	if err != nil {
		t.Fatalf("ReadFile %s: %v", src, err)
	}
	if err := os.WriteFile(dst, data, 0o755); err != nil {
		t.Fatalf("WriteFile %s: %v", dst, err)
	}
}

// --- chain building ---

func addr(b byte) types.Address {
	var a types.Address
	a[0] = b
	return a
}

func blockHash(h uint64, domainID byte, salt byte) types.Hash {
	var out types.Hash
	binary.BigEndian.PutUint64(out[:8], h)
	out[8], out[9] = domainID, salt
	return out
}

func inputData(t *testing.T, h uint64, domainID byte, cmd string) []byte {
	t.Helper()
	in := codec.Input{
		SourceAccountBlockHash: blockHash(h, domainID, 0),
		Args:                   codec.CallL2Args{TargetContract: types.Hash{domainID}, Payload: []byte(cmd)},
	}
	data, err := in.Encode()
	if err != nil {
		t.Fatalf("encode input: %v", err)
	}
	return data
}

// buildChain builds n momenta, one domain-1 and one domain-2 block per height, whose payload
// commands cmd(height, domainID) instructs the test worker.
func buildChain(t *testing.T, n uint64, cmd func(h uint64, domainID byte) string) []*nom.DetailedMomentum {
	t.Helper()
	perHeight := make([][]ipctest.Block, n)
	for i := uint64(0); i < n; i++ {
		h := i + 1
		perHeight[i] = []ipctest.Block{
			{Address: addr(1), Data: inputData(t, h, 1, cmd(h, 1))},
			{Address: addr(2), Data: inputData(t, h, 2, cmd(h, 2))},
		}
	}
	return ipctest.Chain(perHeight)
}

// newFrontier returns an atomic frontier height, safe for concurrent reads from every worker's
// serve goroutine.
func newFrontier(initial uint64) *atomic.Uint64 {
	f := &atomic.Uint64{}
	f.Store(initial)
	return f
}

// chainSources answers every request from chain and frontier; anchors always report not found.
func chainSources(chain []*nom.DetailedMomentum, frontier *atomic.Uint64) Sources {
	return Sources{
		Momenta: fakeMomenta{
			frontier: func() (uint64, error) { return frontier.Load(), nil },
			byHeight: func(h, count uint64) ([]*nom.DetailedMomentum, error) {
				if h == 0 || h > uint64(len(chain)) {
					return nil, nil
				}
				end := h - 1 + count
				if end > uint64(len(chain)) {
					end = uint64(len(chain))
				}
				return chain[h-1 : end], nil
			},
		},
		Anchors: fakeAnchors{anchor: func(uint32) (recovery.Anchor, bool, error) { return recovery.Anchor{}, false, nil }},
	}
}

// --- configuration ---

func supervisorConfig(storage string, workers ...WorkerConfig) Config {
	return Config{
		ExecutorAddress:   types.Address{0x42},
		NetworkName:       "test",
		ConfirmationDepth: 2,
		StoragePath:       storage,
		Workers:           workers,
	}
}

func workerConfig(id uint32, binary string, memoryLimit int64, cpuLimit float64) WorkerConfig {
	return WorkerConfig{
		DomainID:    id,
		Binary:      binary,
		Role:        "EXECUTOR",
		STFSpecHash: types.Hash{0xAA},
		MemoryLimit: memoryLimit,
		CPULimit:    cpuLimit,
	}
}

// shrunkPolicy is the restart schedule every test but the liveness one uses.
var shrunkPolicy = Policy{BackoffMin: 10 * time.Millisecond, BackoffMax: 50 * time.Millisecond, CrashLoopLimit: 3}

// --- harness ---

// startSupervisor runs cfg under a fresh Supervisor until t's cleanup cancels it. The goroutine
// sends Run's result on done and then closes it, so a later receive (by the test, by cleanup, or
// both) returns at once instead of blocking.
func startSupervisor(t *testing.T, cfg Config, src Sources, policy Policy) (*Supervisor, context.CancelFunc, <-chan error) {
	t.Helper()
	sup := New(cfg, []byte("test-key"), src, policy)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- sup.Run(ctx)
		close(done)
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(60 * time.Second):
			t.Fatalf("supervisor did not shut down in time")
		}
	})
	return sup, cancel, done
}

// waitFor polls cond every 10ms until it reports true or timeout elapses, failing with what
// naming what did not happen.
func waitFor(t *testing.T, timeout time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func statusFor(sup *Supervisor, id uint32) WorkerStatus {
	for _, st := range sup.Status() {
		if st.DomainID == id {
			return st
		}
	}
	return WorkerStatus{}
}

// layoutFor builds l by hand, as store.go's own field derivation does, rather than calling
// recovery.NewLayout: that would MkdirAll root-owned subdirectories a uid-separated worker
// cannot write.
func layoutFor(storage string, id uint32) recovery.Layout {
	root := filepath.Join(storage, strconv.FormatUint(uint64(id), 10))
	return recovery.Layout{
		Root:    root,
		Cursor:  filepath.Join(root, "cursor"),
		Batches: filepath.Join(root, "batches"),
	}
}

func readCursor(storage string, id uint32) (recovery.Position, bool) {
	pos, ok, err := recovery.ReadCursor(layoutFor(storage, id))
	if err != nil {
		return recovery.Position{}, false
	}
	return pos, ok
}

// --- tests ---

func killMidBatchChain(t *testing.T) []*nom.DetailedMomentum {
	t.Helper()
	return buildChain(t, 12, func(h uint64, d byte) string {
		if d == 1 && h >= 5 && h <= 10 {
			return "sleep:200"
		}
		return ""
	})
}

func TestKillMidBatchRestartsOnlyThatWorker(t *testing.T) {
	policy := Policy{BackoffMin: 10 * time.Millisecond, BackoffMax: 50 * time.Millisecond}

	storage := openTempDir(t)
	chain := killMidBatchChain(t)
	frontier := newFrontier(6)
	src := chainSources(chain, frontier)
	cfg := supervisorConfig(storage, workerConfig(1, workerBinary, 1<<30, 1), workerConfig(2, workerBinary, 1<<30, 1))
	sup, cancel, done := startSupervisor(t, cfg, src, policy)

	waitFor(t, 30*time.Second, "both cursors at height 4 with worker 1's LastCommitted caught up", func() bool {
		p1, ok1 := readCursor(storage, 1)
		p2, ok2 := readCursor(storage, 2)
		if !ok1 || !ok2 || p1.MomentumHeight != 4 || p2.MomentumHeight != 4 {
			return false
		}
		lc := statusFor(sup, 1).LastCommitted
		return lc != nil && lc.LastInputSeq == p1.LastInputSeq
	})
	p1, _ := readCursor(storage, 1)
	s1 := p1.LastInputSeq
	pid1 := statusFor(sup, 1).PID
	pid2 := statusFor(sup, 2).PID

	frontier.Store(12)
	waitFor(t, 30*time.Second, "worker 1 served height >= 10", func() bool {
		return statusFor(sup, 1).ServedHeight >= 10
	})
	if lc := statusFor(sup, 1).LastCommitted; lc == nil || lc.LastInputSeq != s1 {
		t.Fatalf("worker 1 LastCommitted.LastInputSeq changed to %+v before the kill, want unchanged %d", lc, s1)
	}
	if err := syscall.Kill(pid1, syscall.SIGKILL); err != nil {
		t.Fatalf("Kill(%d): %v", pid1, err)
	}

	select {
	case err := <-done:
		t.Fatalf("done fired early: %v", err)
	default:
	}

	waitFor(t, 30*time.Second, "both cursors at height 10", func() bool {
		p1, ok1 := readCursor(storage, 1)
		p2, ok2 := readCursor(storage, 2)
		return ok1 && ok2 && p1.MomentumHeight == 10 && p2.MomentumHeight == 10
	})

	st1 := statusFor(sup, 1)
	if st1.PID == 0 || st1.PID == pid1 {
		t.Fatalf("worker 1 PID = %d, want a new PID (was %d)", st1.PID, pid1)
	}
	if st1.Restarts != 1 {
		t.Fatalf("worker 1 Restarts = %d, want 1", st1.Restarts)
	}
	if st1.LastExit != "signal killed" {
		t.Fatalf("worker 1 LastExit = %q, want %q", st1.LastExit, "signal killed")
	}
	st2 := statusFor(sup, 2)
	if st2.PID != pid2 {
		t.Fatalf("worker 2 PID = %d, want unchanged %d", st2.PID, pid2)
	}
	if st2.Restarts != 0 {
		t.Fatalf("worker 2 Restarts = %d, want 0", st2.Restarts)
	}

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run = %v, want nil", err)
		}
	case <-time.After(60 * time.Second):
		t.Fatalf("Run did not return")
	}

	// Reference run: fresh storage, a fresh frontier, the same two-stage schedule, no kill. It
	// runs only now, after the first supervisor has fully returned, since the two runs would
	// otherwise share cgroup leaf names.
	refStorage := openTempDir(t)
	refChain := killMidBatchChain(t)
	refFrontier := newFrontier(6)
	refSrc := chainSources(refChain, refFrontier)
	refCfg := supervisorConfig(refStorage, workerConfig(1, workerBinary, 1<<30, 1), workerConfig(2, workerBinary, 1<<30, 1))
	refSup, refCancel, refDone := startSupervisor(t, refCfg, refSrc, policy)
	_ = refSup

	waitFor(t, 30*time.Second, "reference: both cursors at height 4", func() bool {
		p1, ok1 := readCursor(refStorage, 1)
		p2, ok2 := readCursor(refStorage, 2)
		return ok1 && ok2 && p1.MomentumHeight == 4 && p2.MomentumHeight == 4
	})
	refFrontier.Store(12)
	waitFor(t, 30*time.Second, "reference: worker 1 served height >= 10", func() bool {
		return statusFor(refSup, 1).ServedHeight >= 10
	})
	waitFor(t, 30*time.Second, "reference: both cursors at height 10", func() bool {
		p1, ok1 := readCursor(refStorage, 1)
		p2, ok2 := readCursor(refStorage, 2)
		return ok1 && ok2 && p1.MomentumHeight == 10 && p2.MomentumHeight == 10
	})

	refCancel()
	select {
	case err := <-refDone:
		if err != nil {
			t.Fatalf("reference Run = %v, want nil", err)
		}
	case <-time.After(60 * time.Second):
		t.Fatalf("reference Run did not return")
	}

	gotCursor, gotOK := readCursor(storage, 1)
	wantCursor, wantOK := readCursor(refStorage, 1)
	if !gotOK || !wantOK || gotCursor != wantCursor {
		t.Fatalf("worker 1 cursor = %+v (ok=%v), reference cursor = %+v (ok=%v)", gotCursor, gotOK, wantCursor, wantOK)
	}
	gotRecords, err := recovery.ListBatchRecords(layoutFor(storage, 1))
	if err != nil {
		t.Fatalf("ListBatchRecords: %v", err)
	}
	wantRecords, err := recovery.ListBatchRecords(layoutFor(refStorage, 1))
	if err != nil {
		t.Fatalf("ListBatchRecords (reference): %v", err)
	}
	if !reflect.DeepEqual(gotRecords, wantRecords) {
		t.Fatalf("worker 1 batch records = %+v, reference = %+v", gotRecords, wantRecords)
	}
}

func TestMemoryLimitRestartsOnlyThatWorker(t *testing.T) {
	limits, _, reason := workerproc.NewHost().Enforcement()
	if !limits {
		t.Skipf("limits off: %s", reason)
	}

	storage := openTempDir(t)
	chain := buildChain(t, 4, func(h uint64, d byte) string {
		if d == 1 && h == 1 {
			return "alloc:256"
		}
		return ""
	})
	frontier := newFrontier(4)
	src := chainSources(chain, frontier)
	cfg := supervisorConfig(storage,
		workerConfig(1, workerBinary, 128<<20, 1),
		workerConfig(2, workerBinary, 256<<20, 1),
	)
	sup, _, _ := startSupervisor(t, cfg, src, shrunkPolicy)

	waitFor(t, 30*time.Second, "worker 2 RUNNING", func() bool { return statusFor(sup, 2).State == StateRunning })
	pid2 := statusFor(sup, 2).PID

	waitFor(t, 30*time.Second, "worker 1 HALTED", func() bool { return statusFor(sup, 1).State == StateHalted })
	st1 := statusFor(sup, 1)
	if st1.LastExit != "memory limit" {
		t.Fatalf("worker 1 LastExit = %q, want %q", st1.LastExit, "memory limit")
	}
	if st1.Restarts < 1 {
		t.Fatalf("worker 1 Restarts = %d, want >= 1", st1.Restarts)
	}

	waitFor(t, 30*time.Second, "worker 2 cursor reaches height 2", func() bool {
		p, ok := readCursor(storage, 2)
		return ok && p.MomentumHeight >= 2
	})
	st2 := statusFor(sup, 2)
	if st2.PID != pid2 {
		t.Fatalf("worker 2 PID = %d, want unchanged %d", st2.PID, pid2)
	}
	if st2.Restarts != 0 {
		t.Fatalf("worker 2 Restarts = %d, want 0", st2.Restarts)
	}
}

func TestLivenessTimeoutKillsHungWorker(t *testing.T) {
	storage := openTempDir(t)
	chain := buildChain(t, 4, func(h uint64, d byte) string {
		if d == 1 && h == 1 {
			return "sleep:5000"
		}
		return ""
	})
	frontier := newFrontier(4)
	src := chainSources(chain, frontier)
	cfg := supervisorConfig(storage, workerConfig(1, workerBinary, 1<<30, 1))
	policy := Policy{Liveness: 2 * time.Second, BackoffMin: 10 * time.Millisecond, BackoffMax: 50 * time.Millisecond}
	sup, _, _ := startSupervisor(t, cfg, src, policy)

	waitFor(t, 30*time.Second, "liveness timeout", func() bool {
		st := statusFor(sup, 1)
		return st.LastExit == "liveness timeout" && st.Restarts >= 1
	})
}

func TestHaltedWorkerIsNotRestarted(t *testing.T) {
	storage := openTempDir(t)
	chain := buildChain(t, 4, func(uint64, byte) string { return "" })
	frontier := newFrontier(4)
	src := Sources{
		Momenta: fakeMomenta{
			frontier: func() (uint64, error) { return frontier.Load(), nil },
			byHeight: func(h, count uint64) ([]*nom.DetailedMomentum, error) {
				if h == 0 || h > uint64(len(chain)) {
					return nil, nil
				}
				end := h - 1 + count
				if end > uint64(len(chain)) {
					end = uint64(len(chain))
				}
				return chain[h-1 : end], nil
			},
		},
		Anchors: fakeAnchors{anchor: func(domainID uint32) (recovery.Anchor, bool, error) {
			if domainID == 1 {
				return recovery.Anchor{}, false, fmt.Errorf("%w: test", recovery.ErrHalted)
			}
			return recovery.Anchor{}, false, nil
		}},
	}
	watcher := workerConfig(3, workerBinary, 1<<30, 1)
	watcher.Role = "WATCHER"
	cfg := supervisorConfig(storage, workerConfig(1, workerBinary, 1<<30, 1), workerConfig(2, workerBinary, 1<<30, 1), watcher)
	sup, _, _ := startSupervisor(t, cfg, src, shrunkPolicy)

	waitFor(t, 30*time.Second, "worker 1 HALTED", func() bool { return statusFor(sup, 1).State == StateHalted })
	// The one exception to "never a fixed sleep": proving an absence, over several times the
	// shrunk BackoffMax, that a HALTED worker is never restarted.
	time.Sleep(5 * 50 * time.Millisecond)

	st1 := statusFor(sup, 1)
	if st1.State != StateHalted || st1.Restarts != 0 || st1.PID != 0 || st1.LastExit != "exit status 3" {
		t.Fatalf("worker 1 status = %+v, want HALTED, 0 restarts, PID 0, exit status 3", st1)
	}

	waitFor(t, 30*time.Second, "worker 2 cursor reaches height 2", func() bool {
		p, ok := readCursor(storage, 2)
		return ok && p.MomentumHeight >= 2
	})
	st2 := statusFor(sup, 2)
	if st2.State != StateRunning || st2.Restarts != 0 {
		t.Fatalf("worker 2 status = %+v, want RUNNING, 0 restarts", st2)
	}

	waitFor(t, 30*time.Second, "worker 3 CONFIG_ERROR", func() bool { return statusFor(sup, 3).State == StateConfigError })
	st3 := statusFor(sup, 3)
	if st3.Restarts != 0 || st3.LastExit != "exit status 78" {
		t.Fatalf("worker 3 status = %+v, want 0 restarts, exit status 78", st3)
	}
}

func TestHeldDomainLockNeverHalts(t *testing.T) {
	storage := openTempDir(t)
	chain := buildChain(t, 4, func(uint64, byte) string { return "" })
	frontier := newFrontier(4)
	src := chainSources(chain, frontier)
	cfg := supervisorConfig(storage, workerConfig(1, workerBinary, 1<<30, 1))

	domainDir := filepath.Join(storage, "1")
	if err := os.MkdirAll(domainDir, 0o755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	held, err := lease.Acquire(filepath.Join(domainDir, "worker.lock"))
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}

	sup, _, _ := startSupervisor(t, cfg, src, shrunkPolicy)

	waitFor(t, 30*time.Second, "restarts > 3", func() bool {
		st := statusFor(sup, 1)
		if st.State == StateHalted {
			t.Fatalf("worker 1 State = HALTED while the domain lock is held")
		}
		return st.Restarts > 3
	})
	if st := statusFor(sup, 1); st.LastExit != "exit status 75" {
		t.Fatalf("LastExit = %q, want %q", st.LastExit, "exit status 75")
	}

	if err := held.Release(); err != nil {
		t.Fatalf("Release: %v", err)
	}

	waitFor(t, 30*time.Second, "cursor reaches height 2 and worker 1 RUNNING", func() bool {
		p, ok := readCursor(storage, 1)
		return ok && p.MomentumHeight >= 2 && statusFor(sup, 1).State == StateRunning
	})
}

func TestStartFailureCountsTowardCrashLoop(t *testing.T) {
	dir := openTempDir(t)
	bin := filepath.Join(dir, "worker")
	copyBinary(t, workerBinary, bin)

	storage := openTempDir(t)
	chain := buildChain(t, 4, func(uint64, byte) string { return "" })
	frontier := newFrontier(4)
	src := chainSources(chain, frontier)
	cfg := supervisorConfig(storage, workerConfig(1, bin, 1<<30, 1))
	sup, _, _ := startSupervisor(t, cfg, src, shrunkPolicy)

	waitFor(t, 30*time.Second, "worker 1 RUNNING", func() bool { return statusFor(sup, 1).State == StateRunning })
	pid := statusFor(sup, 1).PID

	if err := os.Remove(bin); err != nil {
		t.Fatalf("Remove: %v", err)
	}
	if err := syscall.Kill(pid, syscall.SIGKILL); err != nil {
		t.Fatalf("Kill(%d): %v", pid, err)
	}

	waitFor(t, 30*time.Second, "worker 1 HALTED", func() bool { return statusFor(sup, 1).State == StateHalted })
	if st := statusFor(sup, 1); !strings.HasPrefix(st.LastExit, "start: ") {
		t.Fatalf("LastExit = %q, want prefix %q", st.LastExit, "start: ")
	}
}

func TestSecondSupervisorFailsOnLease(t *testing.T) {
	storage := openTempDir(t)
	chain := buildChain(t, 4, func(uint64, byte) string { return "" })
	frontier := newFrontier(4)
	src := chainSources(chain, frontier)
	cfg := supervisorConfig(storage, workerConfig(1, workerBinary, 1<<30, 1))

	supA, _, _ := startSupervisor(t, cfg, src, shrunkPolicy)
	waitFor(t, 30*time.Second, "worker 1 RUNNING", func() bool { return statusFor(supA, 1).State == StateRunning })
	pidA := statusFor(supA, 1).PID

	supB := New(cfg, []byte("test-key"), src, shrunkPolicy)
	ctxB, cancelB := context.WithCancel(context.Background())
	defer cancelB()

	errB := make(chan error, 1)
	go func() { errB <- supB.Run(ctxB) }()

	select {
	case err := <-errB:
		if err == nil {
			t.Fatalf("B.Run = nil, want an error")
		}
		if errors.Is(err, ErrConfig) {
			t.Fatalf("B.Run = %v, wraps ErrConfig, want it not to", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatalf("B.Run did not return in time")
	}

	for _, st := range supB.Status() {
		if st.State != StateStopped || st.PID != 0 || st.Restarts != 0 {
			t.Fatalf("B status = %+v, want STOPPED, PID 0, Restarts 0", st)
		}
	}
	if got := statusFor(supA, 1).PID; got != pidA {
		t.Fatalf("A's PID = %d, want unchanged %d", got, pidA)
	}
}

func TestShutdownStopsWorkers(t *testing.T) {
	storage := openTempDir(t)
	chain := buildChain(t, 4, func(uint64, byte) string { return "" })
	frontier := newFrontier(4)
	src := chainSources(chain, frontier)
	cfg := supervisorConfig(storage, workerConfig(1, workerBinary, 1<<30, 1), workerConfig(2, workerBinary, 1<<30, 1))
	sup, cancel, done := startSupervisor(t, cfg, src, shrunkPolicy)

	waitFor(t, 30*time.Second, "both workers RUNNING", func() bool {
		return statusFor(sup, 1).State == StateRunning && statusFor(sup, 2).State == StateRunning
	})
	pid1 := statusFor(sup, 1).PID
	pid2 := statusFor(sup, 2).PID

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run = %v, want nil", err)
		}
	case <-time.After(30 * time.Second):
		t.Fatalf("Run did not return within StopGrace")
	}

	for _, pid := range []int{pid1, pid2} {
		if err := syscall.Kill(pid, 0); !errors.Is(err, syscall.ESRCH) {
			t.Fatalf("Kill(%d, 0) = %v, want ESRCH", pid, err)
		}
	}
	for _, st := range sup.Status() {
		if st.State != StateStopped || st.PID != 0 {
			t.Fatalf("status = %+v, want STOPPED, PID 0", st)
		}
	}
}

func TestWorkerAccessChecks(t *testing.T) {
	t.Run("domainId too large", func(t *testing.T) {
		cfg := Config{StoragePath: "/", Workers: []WorkerConfig{{DomainID: math.MaxInt32 - 10000 + 1, Binary: "/bin/sh"}}}
		err := checkWorkerAccess(cfg)
		if !errors.Is(err, ErrConfig) || !strings.Contains(err.Error(), "domainId") {
			t.Fatalf("err = %v, want ErrConfig naming domainId", err)
		}
	})

	t.Run("storagePath not reachable", func(t *testing.T) {
		dir := openTempDir(t)
		if err := os.Chmod(dir, 0o700); err != nil {
			t.Fatalf("Chmod: %v", err)
		}
		cfg := Config{StoragePath: dir, Workers: []WorkerConfig{{DomainID: 1, Binary: "/bin/sh"}}}
		err := checkWorkerAccess(cfg)
		if !errors.Is(err, ErrConfig) || !strings.Contains(err.Error(), dir) {
			t.Fatalf("err = %v, want ErrConfig naming %s", err, dir)
		}
	})

	t.Run("binary not reachable", func(t *testing.T) {
		dir := openTempDir(t)
		bin := filepath.Join(dir, "worker")
		if err := os.WriteFile(bin, []byte("x"), 0o750); err != nil {
			t.Fatalf("WriteFile: %v", err)
		}
		cfg := Config{StoragePath: "/", Workers: []WorkerConfig{{DomainID: 1, Binary: bin}}}
		err := checkWorkerAccess(cfg)
		if !errors.Is(err, ErrConfig) || !strings.Contains(err.Error(), bin) {
			t.Fatalf("err = %v, want ErrConfig naming %s", err, bin)
		}
	})

	t.Run("FILE key inside a domain directory", func(t *testing.T) {
		// This row is the only one where checkReachable must succeed the whole way up, not just
		// fail at StoragePath's own leaf, so it needs a storage root whose every ancestor grants
		// other-execute. t.TempDir() nests under macOS's private, 0700 TMPDIR; /tmp does not.
		dir, err := os.MkdirTemp("/tmp", "supervisor-access-test-")
		if err != nil {
			t.Fatalf("MkdirTemp: %v", err)
		}
		t.Cleanup(func() { os.RemoveAll(dir) })
		if err := os.Chmod(dir, 0o755); err != nil {
			t.Fatalf("Chmod: %v", err)
		}
		domainDir := filepath.Join(dir, "1")
		if err := os.MkdirAll(domainDir, 0o755); err != nil {
			t.Fatalf("MkdirAll: %v", err)
		}
		keyPath := filepath.Join(domainDir, "key")
		if err := os.WriteFile(keyPath, []byte("k"), 0o400); err != nil {
			t.Fatalf("WriteFile: %v", err)
		}
		cfg := Config{
			StoragePath:         dir,
			SubmissionKeySource: KeySource{Kind: "FILE", Path: keyPath},
			Workers:             []WorkerConfig{{DomainID: 1, Binary: "/bin/sh"}},
		}
		accessErr := checkWorkerAccess(cfg)
		if !errors.Is(accessErr, ErrConfig) || !strings.Contains(accessErr.Error(), keyPath) {
			t.Fatalf("err = %v, want ErrConfig naming %s", accessErr, keyPath)
		}
	})

	t.Run("FILE key owned by a worker uid", func(t *testing.T) {
		if os.Geteuid() != 0 {
			t.Skip("requires root to chown a file to a worker uid")
		}
		dir, err := os.MkdirTemp("/tmp", "supervisor-access-test-")
		if err != nil {
			t.Fatalf("MkdirTemp: %v", err)
		}
		t.Cleanup(func() { os.RemoveAll(dir) })
		if err := os.Chmod(dir, 0o755); err != nil {
			t.Fatalf("Chmod: %v", err)
		}
		keyPath := filepath.Join(dir, "key")
		if err := os.WriteFile(keyPath, []byte("k"), 0o400); err != nil {
			t.Fatalf("WriteFile: %v", err)
		}
		if err := os.Chown(keyPath, workerUID(1), 0); err != nil {
			t.Fatalf("Chown: %v", err)
		}
		cfg := Config{
			StoragePath:         dir,
			SubmissionKeySource: KeySource{Kind: "FILE", Path: keyPath},
			Workers:             []WorkerConfig{{DomainID: 1, Binary: "/bin/sh"}},
		}
		accessErr := checkWorkerAccess(cfg)
		if !errors.Is(accessErr, ErrConfig) || !strings.Contains(accessErr.Error(), keyPath) {
			t.Fatalf("err = %v, want ErrConfig naming %s", accessErr, keyPath)
		}
	})
}
