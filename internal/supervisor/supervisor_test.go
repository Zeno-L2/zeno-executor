package supervisor

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/zenon-network/go-zenon/chain/nom"
	"github.com/zenon-network/go-zenon/common/types"

	"github.com/Zeno-L2/zeno-executor/internal/codec"
	"github.com/Zeno-L2/zeno-executor/internal/ipc/ipctest"
	"github.com/Zeno-L2/zeno-executor/internal/lease"
	"github.com/Zeno-L2/zeno-executor/internal/ledger"
	"github.com/Zeno-L2/zeno-executor/internal/ledger/ledgertest"
	"github.com/Zeno-L2/zeno-executor/internal/recovery"
	"github.com/Zeno-L2/zeno-executor/internal/workerproc"
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
	build := exec.Command("go", "build", "-o", workerBinary, "github.com/Zeno-L2/zeno-executor/internal/testbin/worker")
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

// buildChain builds n momenta, one domain-1 and one domain-2 block per height, whose payload
// commands cmd(height, domainID) instructs the test worker.
func buildChain(t *testing.T, n uint64, cmd func(h uint64, domainID byte) string) []*nom.DetailedMomentum {
	t.Helper()
	perHeight := make([][]ipctest.Block, n)
	for i := uint64(0); i < n; i++ {
		h := i + 1
		perHeight[i] = []ipctest.Block{
			{Address: addr(1), Data: ledgertest.SendData(1, []byte(cmd(h, 1)))},
			{Address: addr(2), Data: ledgertest.SendData(2, []byte(cmd(h, 2)))},
			{Address: types.SettlementContract},
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
		Assignments: fakeAssignments{assignment: func(d uint32, h uint64, hash types.Hash, n uint64) ([]ledger.AssignmentEntry, error) {
			return ledgertest.Assign(chain, d, h, hash, n)
		}},
	}
}

// --- configuration ---

func supervisorConfig(storage string, workers ...WorkerConfig) Config {
	return Config{
		ExecutorAddress:   types.Address{0x42},
		NetworkName:       "test",
		ConfirmationDepth: 2,
		MaxPendingBatches: defaultMaxPendingBatches,
		StoragePath:       storage,
		Workers:           workers,
	}
}

func workerConfig(id uint32, binary string, memoryLimit int64, cpuLimit float64) WorkerConfig {
	return WorkerConfig{
		DomainID:    id,
		Binary:      binary,
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
	sup := New(cfg, nil, src, policy)
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
	src := chainSources(chain, frontier)
	src.Anchors = fakeAnchors{anchor: func(domainID uint32) (recovery.Anchor, bool, error) {
		if domainID == 1 {
			return recovery.Anchor{}, false, fmt.Errorf("%w: test", recovery.ErrHalted)
		}
		return recovery.Anchor{}, false, nil
	}}
	badHash := workerConfig(3, workerBinary, 1<<30, 1)
	badHash.STFSpecHash = types.Hash{}
	cfg := supervisorConfig(storage, workerConfig(1, workerBinary, 1<<30, 1), workerConfig(2, workerBinary, 1<<30, 1), badHash)
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

	supB := New(cfg, nil, src, shrunkPolicy)
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

func TestReadyGatesWorkerStart(t *testing.T) {
	chain := buildChain(t, 4, func(uint64, byte) string { return "" })
	frontier := newFrontier(4)

	t.Run("blocking", func(t *testing.T) {
		storage := openTempDir(t)
		src := chainSources(chain, frontier)
		called := make(chan struct{})
		release := make(chan struct{})
		var once sync.Once
		releaseOnce := func() { once.Do(func() { close(release) }) }
		defer releaseOnce()
		src.Ready = func(ctx context.Context) bool {
			close(called)
			select {
			case <-release:
				return true
			case <-ctx.Done():
				return false
			}
		}
		cfg := supervisorConfig(storage, workerConfig(1, workerBinary, 1<<30, 1))

		sup, _, _ := startSupervisor(t, cfg, src, shrunkPolicy)
		select {
		case <-called:
		case <-time.After(30 * time.Second):
			t.Fatalf("Ready was not called")
		}

		// The lease is held during the wait, so a second supervisor fails fast.
		other := New(cfg, nil, chainSources(chain, frontier), shrunkPolicy)
		errOther := make(chan error, 1)
		go func() { errOther <- other.Run(context.Background()) }()
		select {
		case err := <-errOther:
			if err == nil || errors.Is(err, ErrConfig) {
				t.Fatalf("second Run = %v, want a non-ErrConfig error", err)
			}
		case <-time.After(10 * time.Second):
			t.Fatalf("second Run did not return in time")
		}

		if st := statusFor(sup, 1); st.State != StateStopped || st.PID != 0 {
			t.Fatalf("worker 1 during the wait = %+v, want STOPPED, PID 0", st)
		}
		releaseOnce()
		waitFor(t, 30*time.Second, "worker 1 RUNNING", func() bool { return statusFor(sup, 1).State == StateRunning })
	})

	t.Run("shutdown", func(t *testing.T) {
		storage := openTempDir(t)
		src := chainSources(chain, frontier)
		src.Ready = func(context.Context) bool { return false }
		cfg := supervisorConfig(storage, workerConfig(1, workerBinary, 1<<30, 1), workerConfig(2, workerBinary, 1<<30, 1))

		sup, _, done := startSupervisor(t, cfg, src, shrunkPolicy)
		select {
		case err := <-done:
			if err != nil {
				t.Fatalf("Run = %v, want nil", err)
			}
		case <-time.After(10 * time.Second):
			t.Fatalf("Run did not return in time")
		}
		for _, st := range sup.Status() {
			if st.State != StateStopped || st.PID != 0 || st.Restarts != 0 {
				t.Fatalf("status = %+v, want STOPPED, PID 0, Restarts 0", st)
			}
		}
	})
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

// --- commitment hand-off ---

func slotFor(sup *Supervisor, id uint32) *slot {
	for _, sl := range sup.slots {
		if sl.w.DomainID == id {
			return sl
		}
	}
	return nil
}

func heldCommitments(sup *Supervisor, id uint32) map[uint64]codec.BatchCommitment {
	st := slotFor(sup, id).st
	st.mu.Lock()
	defer st.mu.Unlock()
	out := make(map[uint64]codec.BatchCommitment, len(st.commitments))
	for k, v := range st.commitments {
		out[k] = v
	}
	return out
}

// firstPhaseCommitments runs a supervisor over fresh storage until every domain's cursor is at
// height 4 with a held batch ending at the cursor's LastInputSeq, copies each domain's held
// commitments, and shuts the supervisor down.
func firstPhaseCommitments(t *testing.T, ids ...uint32) (string, []*nom.DetailedMomentum, *atomic.Uint64, map[uint32]map[uint64]codec.BatchCommitment) {
	t.Helper()
	storage := openTempDir(t)
	chain := buildChain(t, 12, func(uint64, byte) string { return "" })
	frontier := newFrontier(6)
	src := chainSources(chain, frontier)
	var workers []WorkerConfig
	for _, id := range ids {
		workers = append(workers, workerConfig(id, workerBinary, 1<<30, 1))
	}
	sup, cancel, done := startSupervisor(t, supervisorConfig(storage, workers...), src, shrunkPolicy)

	waitFor(t, 30*time.Second, "every cursor at height 4 with its last batch held", func() bool {
		for _, id := range ids {
			pos, ok := readCursor(storage, id)
			if !ok || pos.MomentumHeight != 4 {
				return false
			}
			held := false
			for _, c := range heldCommitments(sup, id) {
				held = held || c.LastInputSeq == pos.LastInputSeq
			}
			if !held {
				return false
			}
		}
		return true
	})
	copies := map[uint32]map[uint64]codec.BatchCommitment{}
	for _, id := range ids {
		copies[id] = heldCommitments(sup, id)
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("first Run = %v, want nil", err)
		}
	case <-time.After(60 * time.Second):
		t.Fatalf("first Run did not return")
	}
	return storage, chain, frontier, copies
}

func TestSupervisorRestartRecoversCommitments(t *testing.T) {
	storage, chain, frontier, copies := firstPhaseCommitments(t, 1)

	src := chainSources(chain, frontier)
	sup, _, _ := startSupervisor(t, supervisorConfig(storage, workerConfig(1, workerBinary, 1<<30, 1)), src, shrunkPolicy)

	waitFor(t, 30*time.Second, "the held commitments refilled from the worker's boot report", func() bool {
		return reflect.DeepEqual(heldCommitments(sup, 1), copies[1])
	})
	st := slotFor(sup, 1).st
	st.mu.Lock()
	progressed := st.progressed
	st.mu.Unlock()
	if progressed {
		t.Fatalf("progressed = true after a boot re-report, want false")
	}
}

func TestConsistencyFaultHaltsOnlyThatDomain(t *testing.T) {
	storage, chain, frontier, copies := firstPhaseCommitments(t, 1, 2)

	src := chainSources(chain, frontier)
	cfg := supervisorConfig(storage, workerConfig(1, workerBinary, 1<<30, 1), workerConfig(2, workerBinary, 1<<30, 1))
	sup := New(cfg, nil, src, shrunkPolicy)

	flipped := map[uint64]codec.BatchCommitment{}
	for k, v := range copies[1] {
		flipped[k] = v
	}
	b0, ok := flipped[0]
	if !ok {
		t.Fatalf("domain 1 holds no batch 0: %v", copies[1])
	}
	b0.DAHash[0] ^= 0xff
	flipped[0] = b0
	st1 := slotFor(sup, 1).st
	st1.mu.Lock()
	st1.commitments = flipped
	st1.mu.Unlock()

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

	waitFor(t, 30*time.Second, "domain 1 HALTED", func() bool { return statusFor(sup, 1).State == StateHalted })
	// Proves an absence over several times the shrunk BackoffMax: a HALTED domain is never
	// restarted.
	time.Sleep(5 * 50 * time.Millisecond)

	s1 := statusFor(sup, 1)
	if s1.State != StateHalted || s1.Restarts != 0 ||
		!strings.Contains(s1.LastExit, "consistency fault") || !strings.Contains(s1.LastExit, "batch 0") {
		t.Fatalf("domain 1 status = %+v, want HALTED, 0 restarts and a consistency fault on batch 0", s1)
	}
	if got := heldCommitments(sup, 1)[0]; !reflect.DeepEqual(got, b0) {
		t.Fatalf("held batch 0 = %+v, want the flipped one %+v", got, b0)
	}
	s2 := statusFor(sup, 2)
	if s2.State != StateRunning || s2.Restarts != 0 {
		t.Fatalf("domain 2 status = %+v, want RUNNING with 0 restarts", s2)
	}
}

// --- supervisor-originated halts ---

var haltErr = fmt.Errorf("%w: domain 1 test halt", ErrInconsistent)

func TestHaltDomainRecordsOnce(t *testing.T) {
	cfg := supervisorConfig(openTempDir(t), workerConfig(1, workerBinary, 1<<30, 1))
	st := New(cfg, nil, Sources{}, Policy{}).slots[0].st

	if !haltDomain(st, haltErr) {
		t.Fatalf("haltDomain = false, want true")
	}
	if st.fault != haltErr.Error() {
		t.Fatalf("fault = %q, want %q", st.fault, haltErr.Error())
	}
	select {
	case <-st.halt:
	default:
		t.Fatalf("halt is open after a recorded fault")
	}
	if haltDomain(st, fmt.Errorf("%w: second", ErrInconsistent)) {
		t.Fatalf("second haltDomain = true, want false")
	}
	if st.fault != haltErr.Error() {
		t.Fatalf("fault = %q after a second call, want %q", st.fault, haltErr.Error())
	}

	st2 := New(cfg, nil, Sources{}, Policy{}).slots[0].st
	st2.fault = "serve fault"
	if haltDomain(st2, haltErr) {
		t.Fatalf("haltDomain over a serve fault = true, want false")
	}
	if st2.fault != "serve fault" {
		t.Fatalf("fault = %q, want the serve fault kept", st2.fault)
	}
	select {
	case <-st2.halt:
		t.Fatalf("halt closed without a recorded fault")
	default:
	}
}

func TestHaltDomainHaltsRunningDomain(t *testing.T) {
	storage := openTempDir(t)
	chain := buildChain(t, 4, func(uint64, byte) string { return "" })
	frontier := newFrontier(4)
	src := chainSources(chain, frontier)
	cfg := supervisorConfig(storage, workerConfig(1, workerBinary, 1<<30, 1), workerConfig(2, workerBinary, 1<<30, 1))
	sup, _, _ := startSupervisor(t, cfg, src, shrunkPolicy)

	waitFor(t, 30*time.Second, "both cursors at height 2 and both RUNNING", func() bool {
		for _, id := range []uint32{1, 2} {
			p, ok := readCursor(storage, id)
			if !ok || p.MomentumHeight < 2 || statusFor(sup, id).State != StateRunning {
				return false
			}
		}
		return true
	})
	if !haltDomain(slotFor(sup, 1).st, haltErr) {
		t.Fatalf("haltDomain = false, want true")
	}
	waitFor(t, 30*time.Second, "domain 1 HALTED", func() bool { return statusFor(sup, 1).State == StateHalted })
	// Proves an absence over several times the shrunk BackoffMax: a HALTED domain is never
	// restarted.
	time.Sleep(5 * 50 * time.Millisecond)

	s1 := statusFor(sup, 1)
	if s1.State != StateHalted || s1.Restarts != 0 || s1.PID != 0 || s1.LastExit != haltErr.Error() {
		t.Fatalf("domain 1 status = %+v, want HALTED, 0 restarts, PID 0, LastExit %q", s1, haltErr.Error())
	}
	s2 := statusFor(sup, 2)
	if s2.State != StateRunning || s2.Restarts != 0 {
		t.Fatalf("domain 2 status = %+v, want RUNNING with 0 restarts", s2)
	}
}

func TestHaltDomainDuringBackoff(t *testing.T) {
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
	t.Cleanup(func() { _ = held.Release() })

	sup, _, _ := startSupervisor(t, cfg, src, Policy{BackoffMin: time.Minute, BackoffMax: time.Minute})
	waitFor(t, 30*time.Second, "domain 1 BACKOFF", func() bool { return statusFor(sup, 1).State == StateBackoff })

	if !haltDomain(slotFor(sup, 1).st, haltErr) {
		t.Fatalf("haltDomain = false, want true")
	}
	waitFor(t, 10*time.Second, "domain 1 HALTED", func() bool { return statusFor(sup, 1).State == StateHalted })
	if s := statusFor(sup, 1); s.Restarts != 0 || s.LastExit != haltErr.Error() {
		t.Fatalf("domain 1 status = %+v, want 0 restarts and LastExit %q", s, haltErr.Error())
	}
}

func TestFaultAfterRunHalts(t *testing.T) {
	const preset = "supervisor: consistency fault: test"
	cases := []struct {
		name   string
		policy Policy
	}{
		{"crash loop", Policy{BackoffMin: 10 * time.Millisecond, BackoffMax: 50 * time.Millisecond, CrashLoopLimit: 1}},
		{"restart", Policy{BackoffMin: time.Minute, BackoffMax: time.Minute}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			bin := filepath.Join(openTempDir(t), "worker")
			if err := os.WriteFile(bin, []byte("not an executable\n"), 0o755); err != nil {
				t.Fatalf("WriteFile: %v", err)
			}
			storage := openTempDir(t)
			chain := buildChain(t, 4, func(uint64, byte) string { return "" })
			src := chainSources(chain, newFrontier(4))
			sup := New(supervisorConfig(storage, workerConfig(1, bin, 1<<30, 1)), nil, src, tc.policy)
			st := slotFor(sup, 1).st
			st.mu.Lock()
			st.fault = preset
			st.mu.Unlock()

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

			waitFor(t, 10*time.Second, "domain 1 HALTED", func() bool { return statusFor(sup, 1).State == StateHalted })
			if s := statusFor(sup, 1); s.Restarts != 0 || s.LastExit != preset {
				t.Fatalf("domain 1 status = %+v, want 0 restarts and LastExit %q", s, preset)
			}
		})
	}
}

func TestStatusReportsSubmitError(t *testing.T) {
	cfg := supervisorConfig(t.TempDir(), workerConfig(1, "/bin/true", 1<<30, 1), workerConfig(2, "/bin/true", 1<<30, 1))
	sup := New(cfg, nil, Sources{}, shrunkPolicy)
	st := sup.slots[1].st
	st.mu.Lock()
	st.submitError = "stalled"
	st.rejectError = "rejected"
	st.mu.Unlock()

	got := sup.Status()
	if got[0].SubmitError != "" || got[1].SubmitError != "stalled" {
		t.Fatalf("SubmitError = %q, %q; want none for domain 1 and %q for domain 2", got[0].SubmitError, got[1].SubmitError, "stalled")
	}
	if got[0].RejectError != "" || got[1].RejectError != "rejected" {
		t.Fatalf("RejectError = %q, %q; want none for domain 1 and %q for domain 2", got[0].RejectError, got[1].RejectError, "rejected")
	}
}

func TestSubmitterStatus(t *testing.T) {
	cfg := supervisorConfig(t.TempDir(), workerConfig(1, "/bin/true", 1<<30, 1))
	sup := New(cfg, nil, Sources{}, shrunkPolicy)

	if got := sup.SubmitterStatus(); got.Last != nil || got.Error != "" {
		t.Fatalf("SubmitterStatus before any send = %+v, want empty", got)
	}

	r := SendRecord{DomainID: 1, BatchID: 4, Hash: types.Hash{0x01}}
	if err := sup.sub.log.Sent(r); err != nil {
		t.Fatal(err)
	}
	got := sup.SubmitterStatus()
	if got.Last == nil || *got.Last != r {
		t.Fatalf("Last = %+v, want %+v", got.Last, r)
	}
	got.Last.BatchID = 99
	if again := sup.SubmitterStatus(); again.Last.BatchID != 4 {
		t.Fatalf("Last changed through the returned copy: %+v", again.Last)
	}

	sup.sub.mu.Lock()
	sup.sub.plasmaText = "no plasma"
	sup.sub.mu.Unlock()
	if got := sup.SubmitterStatus().Error; got != "no plasma" {
		t.Fatalf("Error = %q, want the plasma text", got)
	}
	sup.sub.mu.Lock()
	sup.sub.alertText = "unconfirmed"
	sup.sub.mu.Unlock()
	if got := sup.SubmitterStatus().Error; got != "unconfirmed" {
		t.Fatalf("Error = %q, want the alert text to win over the plasma text", got)
	}
	sup.sub.mu.Lock()
	sup.sub.logText = "disk full"
	sup.sub.mu.Unlock()
	if got := sup.SubmitterStatus().Error; got != "disk full" {
		t.Fatalf("Error = %q, want the log text to win over the alert text", got)
	}
}

// --- submission log wiring ---

// logRun is a supervisor whose Ready counts its calls and returns false, so Run returns once it
// has opened the submission log.
type logRun struct {
	sup   *Supervisor
	dir   string
	path  string
	ready *int32
}

func newLogRun(t *testing.T, withNode bool) logRun {
	t.Helper()
	storage := t.TempDir()
	cfg := supervisorConfig(storage, workerConfig(1, "/bin/true", 1<<30, 1))
	var ready int32
	src := Sources{Ready: func(context.Context) bool { atomic.AddInt32(&ready, 1); return false }}
	if withNode {
		src.Node = newSendNodeFake()
	}
	dir := filepath.Join(storage, "supervisor")
	return logRun{
		sup:   New(cfg, testKeyPair(t), src, shrunkPolicy),
		dir:   dir,
		path:  filepath.Join(dir, cfg.ExecutorAddress.String()+".submissions.jsonl"),
		ready: &ready,
	}
}

func (r logRun) writeLog(t *testing.T, content string) {
	t.Helper()
	if err := os.MkdirAll(r.dir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(r.path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
}

func TestRunOpensTheSubmissionLog(t *testing.T) {
	b2, b3 := logRec(1, 2, 0x02), logRec(1, 3, 0x03)

	t.Run("an existing log", func(t *testing.T) {
		r := newLogRun(t, true)
		r.writeLog(t, logLines(b2, logResolved(b2, OutcomeAccepted), b3))
		if err := r.sup.Run(context.Background()); err != nil {
			t.Fatalf("Run = %v", err)
		}
		if last := r.sup.SubmitterStatus().Last; last == nil || *last != b3 {
			t.Fatalf("Last = %+v, want %+v", last, b3)
		}
		if r.sup.sub.pending == nil || r.sup.sub.pending.unreadable != nil {
			t.Fatalf("pending = %+v, want the readable send", r.sup.sub.pending)
		}
		st := r.sup.slots[0].st
		st.mu.Lock()
		next := st.chainNext
		st.mu.Unlock()
		if next != 3 {
			t.Fatalf("chainNext = %d, want 3", next)
		}
	})

	t.Run("no file beforehand", func(t *testing.T) {
		r := newLogRun(t, true)
		if err := r.sup.Run(context.Background()); err != nil {
			t.Fatalf("Run = %v", err)
		}
		fi, err := os.Stat(r.path)
		if err != nil || fi.Size() != 0 || fi.Mode().Perm() != 0o600 {
			t.Fatalf("log = %v, %v; want an empty file with mode 0600", fi, err)
		}
	})

	t.Run("an unreadable last send", func(t *testing.T) {
		r := newLogRun(t, true)
		h := types.Hash{0x0f}
		r.writeLog(t, fmt.Sprintf(`{"version":2,"domainId":1,"batchId":3,"hash":"%s"}`+"\n", h))
		if err := r.sup.Run(context.Background()); err != nil {
			t.Fatalf("Run = %v", err)
		}
		if last := r.sup.SubmitterStatus().Last; last == nil || last.Hash != h {
			t.Fatalf("Last = %+v, want hash %s", last, h)
		}
		if p := r.sup.sub.pending; p == nil || p.unreadable == nil || !strings.Contains(p.unreadable.Error(), "log-format version 2") {
			t.Fatalf("pending = %+v, want an unreadable send", p)
		}
	})
}

func TestRunFailsOnADamagedSubmissionLog(t *testing.T) {
	r := newLogRun(t, true)
	r.writeLog(t, "not json\n")
	err := r.sup.Run(context.Background())
	if err == nil || !strings.Contains(err.Error(), r.path) || !strings.Contains(err.Error(), "line 1") {
		t.Fatalf("Run = %v, want an error naming %s and line 1", err, r.path)
	}
	if !strings.HasPrefix(err.Error(), "supervisor: ") {
		t.Fatalf("Run = %v, want the supervisor prefix", err)
	}
	if n := atomic.LoadInt32(r.ready); n != 0 {
		t.Fatalf("Ready was called %d times", n)
	}
	if st := r.sup.Status()[0].State; st != StateStopped {
		t.Fatalf("State = %s, want %s", st, StateStopped)
	}
}

func TestRunWithoutNodeOpensNoLog(t *testing.T) {
	r := newLogRun(t, false)
	if err := r.sup.Run(context.Background()); err != nil {
		t.Fatalf("Run = %v", err)
	}
	if _, err := os.Stat(r.path); !os.IsNotExist(err) {
		t.Fatalf("log file: %v, want none", err)
	}
}

func TestRunWithTheLeaseHeldOpensNoLog(t *testing.T) {
	r := newLogRun(t, true)
	if err := os.MkdirAll(r.dir, 0o700); err != nil {
		t.Fatal(err)
	}
	held, err := lease.Acquire(filepath.Join(r.dir, r.sup.cfg.ExecutorAddress.String()+".lock"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { held.Release() })

	err = r.sup.Run(context.Background())
	if err == nil || !strings.Contains(err.Error(), "is held by another process") {
		t.Fatalf("Run = %v, want the lease error", err)
	}
	if n := atomic.LoadInt32(r.ready); n != 0 {
		t.Fatalf("Ready was called %d times", n)
	}
	if _, err := os.Stat(r.path); !os.IsNotExist(err) {
		t.Fatalf("log file: %v, want none", err)
	}
}

// --- withdrawal store ---

func withdrawalDir(storage string, executor types.Address, id uint32) string {
	return filepath.Join(storage, "supervisor", executor.String()+".withdrawals", strconv.FormatUint(uint64(id), 10))
}

func TestRunFailsOnADamagedWithdrawalFile(t *testing.T) {
	storage := openTempDir(t)
	chain := buildChain(t, 12, func(uint64, byte) string { return "" })
	cfg := supervisorConfig(storage, workerConfig(1, workerBinary, 1<<30, 1))
	dir := withdrawalDir(storage, cfg.ExecutorAddress, 1)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "7.json")
	if err := os.WriteFile(path, []byte("{"), 0o600); err != nil {
		t.Fatal(err)
	}

	sup := New(cfg, nil, chainSources(chain, newFrontier(6)), shrunkPolicy)
	err := sup.Run(context.Background())
	if err == nil || !strings.Contains(err.Error(), path) {
		t.Fatalf("Run = %v, want an error naming %s", err, path)
	}
	if pid := statusFor(sup, 1).PID; pid != 0 {
		t.Fatalf("domain 1 PID = %d, want no worker", pid)
	}
}

func TestWithdrawalStoreFailureKillsTheWorker(t *testing.T) {
	storage := openTempDir(t)
	chain := buildChain(t, 12, func(uint64, byte) string { return "" })
	cfg := supervisorConfig(storage, workerConfig(1, workerBinary, 1<<30, 1), workerConfig(2, workerBinary, 1<<30, 1))
	sup, _, _ := startSupervisor(t, cfg, chainSources(chain, newFrontier(6)), shrunkPolicy)

	waitFor(t, 30*time.Second, "domain 1 LastCommitted", func() bool { return statusFor(sup, 1).LastCommitted != nil })
	pid := statusFor(sup, 1).PID
	if err := os.RemoveAll(withdrawalDir(storage, cfg.ExecutorAddress, 1)); err != nil {
		t.Fatal(err)
	}
	if err := syscall.Kill(pid, syscall.SIGKILL); err != nil {
		t.Fatalf("Kill(%d): %v", pid, err)
	}

	waitFor(t, 30*time.Second, "domain 1 HALTED", func() bool { return statusFor(sup, 1).State == StateHalted })
	if s1 := statusFor(sup, 1); !strings.Contains(s1.LastExit, "withdrawal store") {
		t.Fatalf("domain 1 LastExit = %q, want the withdrawal store failure", s1.LastExit)
	}
	if s2 := statusFor(sup, 2); s2.State != StateRunning {
		t.Fatalf("domain 2 state = %s, want RUNNING", s2.State)
	}
}

func TestWithdrawalStoreSurvivesRestarts(t *testing.T) {
	storage, chain, frontier, copies := firstPhaseCommitments(t, 1)

	cfg := supervisorConfig(storage, workerConfig(1, workerBinary, 1<<30, 1))
	s, err := openWithdrawalStore(withdrawalDir(storage, cfg.ExecutorAddress, 1), 1)
	if err != nil {
		t.Fatalf("open the store after shutdown: %v", err)
	}
	var want []BatchRef
	for id := range copies[1] {
		want = append(want, BatchRef{BatchID: id, Verified: true})
	}
	slices.SortFunc(want, func(a, b BatchRef) int { return cmp.Compare(a.BatchID, b.BatchID) })
	if got := s.Batches(); len(want) == 0 || !slices.Equal(got, want) {
		t.Fatalf("Batches = %v, want every held batch %v, verified", got, want)
	}

	sup, _, _ := startSupervisor(t, cfg, chainSources(chain, frontier), shrunkPolicy)
	waitFor(t, 30*time.Second, "the held set refilled", func() bool {
		return reflect.DeepEqual(heldCommitments(sup, 1), copies[1])
	})
	st := slotFor(sup, 1).st
	st.mu.Lock()
	defer st.mu.Unlock()
	if st.fault != "" {
		t.Fatalf("fault = %q, want none", st.fault)
	}
}

func TestNewSetsAFreshRetrySchedule(t *testing.T) {
	cfg := supervisorConfig(t.TempDir(), workerConfig(1, workerBinary, 1<<30, 1), workerConfig(2, workerBinary, 1<<30, 1))
	sup := New(cfg, nil, Sources{}, shrunkPolicy)
	want := verifyRetry{base: time.Second, max: time.Minute, timeout: 2 * time.Second, delay: time.Second}
	for _, sl := range sup.slots {
		if sl.st.verify != want {
			t.Fatalf("domain %d verify = %+v, want %+v", sl.w.DomainID, sl.st.verify, want)
		}
	}
}
