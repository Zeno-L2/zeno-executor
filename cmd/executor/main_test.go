package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/zenon-network/go-zenon/common/types"

	"github.com/Zeno-L2/zeno-executor/internal/ipc"
	"github.com/Zeno-L2/zeno-executor/internal/recovery"
	"github.com/Zeno-L2/zeno-executor/internal/supervisor"
	"github.com/Zeno-L2/zeno-executor/internal/znnd/znndtest"
)

// validHash is a well-formed stfSpecHash: 63 zeros then "1" (internal/supervisor/config_test.go:36).
var validHash = strings.Repeat("0", 63) + "1"

// probeAttempt mirrors the probe's attempt JSON tags.
type probeAttempt struct {
	Errno int    `json:"errno"`
	Err   string `json:"err"`
}

// probeFD mirrors the probe's fdEntry JSON tags.
type probeFD struct {
	FD   int    `json:"fd"`
	Dev  uint64 `json:"dev"`
	Ino  uint64 `json:"ino"`
	Link string `json:"link"`
}

// probeReport mirrors the probe's report JSON tags; the test cannot import package main of the
// probe.
type probeReport struct {
	FDs         []probeFD               `json:"fds"`
	Frames      [][]byte                `json:"frames"`
	FrameErr    string                  `json:"frameErr"`
	Env         []string                `json:"env"`
	Args        []string                `json:"args"`
	UID         int                     `json:"uid"`
	PPID        int                     `json:"ppid"`
	Attempts    map[string]probeAttempt `json:"attempts"`
	CapPrm      string                  `json:"capPrm"`
	CapEff      string                  `json:"capEff"`
	CapAmb      string                  `json:"capAmb"`
	PtraceScope string                  `json:"ptraceScope"`
}

var executorBin, probeBin, probeKeyPath string

func TestMain(m *testing.M) {
	binDir, err := os.MkdirTemp("", "executor-testbin-")
	if err != nil {
		fmt.Fprintln(os.Stderr, "MkdirTemp:", err)
		os.Exit(1)
	}
	if err := os.Chmod(binDir, 0o755); err != nil {
		fmt.Fprintln(os.Stderr, "Chmod:", err)
		os.RemoveAll(binDir)
		os.Exit(1)
	}

	keyDir, err := os.MkdirTemp("", "executor-key-")
	if err != nil {
		fmt.Fprintln(os.Stderr, "MkdirTemp:", err)
		os.RemoveAll(binDir)
		os.Exit(1)
	}
	// 0755, not the default 0700: the uid-separated subtests must fail on the key file's own
	// mode, not on a worker uid being unable to traverse into the directory first. The Docker
	// deployment's key directory is traversable too, so this is what it actually relies on.
	if err := os.Chmod(keyDir, 0o755); err != nil {
		fmt.Fprintln(os.Stderr, "Chmod:", err)
		os.RemoveAll(binDir)
		os.RemoveAll(keyDir)
		os.Exit(1)
	}
	probeKeyPath = filepath.Join(keyDir, "key")

	executorBin = filepath.Join(binDir, "executor")
	buildExecutor := exec.Command("go", "build", "-o", executorBin, "github.com/Zeno-L2/zeno-executor/cmd/executor")
	buildExecutor.Stdout = os.Stderr
	buildExecutor.Stderr = os.Stderr
	if err := buildExecutor.Run(); err != nil {
		fmt.Fprintln(os.Stderr, "build executor:", err)
		os.RemoveAll(binDir)
		os.RemoveAll(keyDir)
		os.Exit(1)
	}

	probeBin = filepath.Join(binDir, "keyprobe")
	buildProbe := exec.Command("go", "build", "-ldflags", "-X main.keyPath="+probeKeyPath, "-o", probeBin, "github.com/Zeno-L2/zeno-executor/internal/testbin/keyprobe")
	buildProbe.Stdout = os.Stderr
	buildProbe.Stderr = os.Stderr
	if err := buildProbe.Run(); err != nil {
		fmt.Fprintln(os.Stderr, "build keyprobe:", err)
		os.RemoveAll(binDir)
		os.RemoveAll(keyDir)
		os.Exit(1)
	}

	code := m.Run()
	os.RemoveAll(binDir)
	os.RemoveAll(keyDir)
	os.Exit(code)
}

// openTempDir returns a 0755 temp directory whose parent is also 0755, since t.TempDir's parent
// is 0700 and a uid-separated worker must be able to reach it (internal/supervisor/supervisor_test.go:60-72).
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

func newMarker(t *testing.T) string {
	t.Helper()
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		t.Fatalf("rand.Read: %v", err)
	}
	return hex.EncodeToString(b)
}

func TestWorkerCannotReachKey(t *testing.T) {
	t.Run("FILE", func(t *testing.T) {
		marker := newMarker(t)
		if err := os.WriteFile(probeKeyPath, []byte(marker), 0o400); err != nil {
			t.Fatalf("WriteFile: %v", err)
		}
		t.Cleanup(func() { os.Remove(probeKeyPath) })

		// inherited is owned by this subtest, not by runWithProbe: the linux and uid separated
		// children below share one open descriptor, freshly planted into the supervisor's
		// ExtraFiles on each call. Closing it per-child would leave the later children with a
		// closed *os.File (Fd() == -1), which os/exec silently turns into "close fd 4 in the
		// child" instead of planting anything — the descriptor-leak check would then test
		// nothing.
		inherited, err := os.Open(probeKeyPath)
		if err != nil {
			t.Fatalf("Open: %v", err)
		}
		t.Cleanup(func() { inherited.Close() })

		keySource := map[string]any{"kind": "FILE", "path": probeKeyPath}

		t.Run("portable", func(t *testing.T) {
			runWithProbe(t, marker, keySource, inherited, nil)
		})

		t.Run("linux", func(t *testing.T) {
			requireLinux(t)
			run := runWithProbe(t, marker, keySource, inherited, nil)
			assertProcDenied(t, run.rep)
			t.Logf("ptrace_scope = %q", run.rep.PtraceScope)
			if os.Geteuid() != 0 {
				// Without uid separation the probe shares the supervisor's user: keyFile must
				// succeed, proving the attempt is live rather than vacuously passing.
				if got := run.rep.Attempts["keyFile"]; got.Errno != 0 {
					t.Errorf("keyFile without uid separation: errno %d (%s), want success", got.Errno, got.Err)
				}
			}
		})

		t.Run("uid separated", func(t *testing.T) {
			requireUIDSeparation(t)
			run := runWithProbe(t, marker, keySource, inherited, nil)
			if run.rep.UID != 10001 {
				t.Errorf("UID = %d, want 10001", run.rep.UID)
			}
			if got := run.rep.Attempts["keyFile"]; got.Errno != int(syscall.EACCES) {
				t.Errorf("keyFile = errno %d (%s), want EACCES", got.Errno, got.Err)
			}
			assertNoCapabilities(t, run.rep)
		})
	})

	t.Run("ENV", func(t *testing.T) {
		marker := newMarker(t)
		inherited, err := os.CreateTemp(t.TempDir(), "inherited")
		if err != nil {
			t.Fatalf("CreateTemp: %v", err)
		}
		// See the FILE subtest above: inherited is owned here, not by runWithProbe, so every
		// child below gets a freshly planted descriptor.
		t.Cleanup(func() { inherited.Close() })

		keySource := map[string]any{"kind": "ENV", "name": "EXECUTOR_KEY"}
		extraEnv := []string{"EXECUTOR_KEY=" + marker}

		t.Run("portable", func(t *testing.T) {
			runWithProbe(t, marker, keySource, inherited, extraEnv)
		})

		t.Run("linux", func(t *testing.T) {
			requireLinux(t)
			run := runWithProbe(t, marker, keySource, inherited, extraEnv)
			assertProcDenied(t, run.rep)
			t.Logf("ptrace_scope = %q", run.rep.PtraceScope)
		})

		t.Run("uid separated", func(t *testing.T) {
			requireUIDSeparation(t)
			run := runWithProbe(t, marker, keySource, inherited, extraEnv)
			if run.rep.UID != 10001 {
				t.Errorf("UID = %d, want 10001", run.rep.UID)
			}
			// The environ denial must guard a key still present in the supervisor's own
			// environment after LoadKey's os.Unsetenv, not an already-cleared variable.
			found := false
			for _, line := range strings.Split(string(run.environ), "\x00") {
				if strings.Contains(line, marker) {
					found = true
					break
				}
			}
			if !found {
				t.Errorf("marker not found in supervisor environ; environ denial would be vacuous")
			}
			assertNoCapabilities(t, run.rep)
		})
	})
}

// probeRun is one runWithProbe result.
type probeRun struct {
	rep        probeReport
	supervisor int    // the supervisor's pid
	environ    []byte // the supervisor's /proc/<pid>/environ, read by the test; nil unless Linux as root
}

// runWithProbe starts the executor supervisor with the probe as its one worker, over keySource,
// with inherited placed at the probe's inheritedFD, and returns its report.
func runWithProbe(t *testing.T, marker string, keySource map[string]any, inherited *os.File, extraEnv []string) probeRun {
	t.Helper()

	inheritedDev, inheritedIno, err := statDevIno(inherited)
	if err != nil {
		t.Fatalf("Stat inherited: %v", err)
	}

	storage := openTempDir(t)

	seed, err := hex.DecodeString(marker)
	if err != nil {
		t.Fatalf("DecodeString: %v", err)
	}
	executorAddress := types.PubKeyToAddress(ed25519.NewKeyFromSeed(seed).Public().(ed25519.PublicKey))

	fx := znndtest.Load(t)
	node := znndtest.NewServer(t, fx, fx.ReadHeight+2)

	cfg := map[string]any{
		"executorAddress":     executorAddress.String(),
		"networkName":         "test",
		"pillarName":          "test",
		"znndEndpoint":        node.URL,
		"confirmationDepth":   float64(2),
		"storagePath":         storage,
		"submissionKeySource": keySource,
		"workers": []any{
			map[string]any{
				"domainId":    float64(1),
				"binary":      probeBin,
				"stfSpecHash": validHash,
				"memoryLimit": float64(268435456),
				"cpuLimit":    1.0,
			},
		},
	}
	b, err := json.Marshal(cfg)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	cfgPath := filepath.Join(t.TempDir(), "executor.json")
	if err := os.WriteFile(cfgPath, b, 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	cmd := exec.Command(executorBin, "-config", cfgPath)
	cmd.Env = append(os.Environ(), extraEnv...)
	// The nil entry closes the supervisor's own descriptor 3 and puts inherited at 4, matching
	// the probe's inheritedFD: workerproc.Start always places the worker channel at descriptor 3
	// regardless of sealing, which would silently absorb a leak placed there.
	cmd.ExtraFiles = []*os.File{nil, inherited}

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatalf("StdoutPipe: %v", err)
	}
	var stderrBuf bytes.Buffer
	cmd.Stderr = &stderrBuf

	if err := cmd.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}

	waited := false
	t.Cleanup(func() {
		if !waited && cmd.Process != nil {
			cmd.Process.Kill()
			cmd.Wait()
		}
	})

	lineCh := make(chan string, 1)
	doneCh := make(chan struct{})
	var stdoutBuf bytes.Buffer
	go func() {
		defer close(doneCh)
		scanner := bufio.NewScanner(stdout)
		scanner.Buffer(make([]byte, 0, 64*1024), 1<<20)
		found := false
		for scanner.Scan() {
			line := scanner.Text()
			if !found && strings.HasPrefix(line, "KEYPROBE ") {
				found = true
				lineCh <- line
				continue
			}
			stdoutBuf.WriteString(line)
			stdoutBuf.WriteByte('\n')
		}
	}()

	var line string
	select {
	case line = <-lineCh:
	case <-doneCh:
		cmd.Process.Kill()
		cmd.Wait()
		waited = true
		t.Fatalf("keyprobe: stdout closed before a KEYPROBE line; stderr:\n%s", stderrBuf.String())
	case <-time.After(60 * time.Second):
		cmd.Process.Kill()
		<-doneCh
		cmd.Wait()
		waited = true
		t.Fatalf("keyprobe: no KEYPROBE line within 60s; stderr:\n%s", stderrBuf.String())
	}

	var rep probeReport
	if err := json.Unmarshal([]byte(strings.TrimPrefix(line, "KEYPROBE ")), &rep); err != nil {
		cmd.Process.Kill()
		<-doneCh
		cmd.Wait()
		waited = true
		t.Fatalf("unmarshal KEYPROBE line: %v; line:\n%s", err, line)
	}

	var environ []byte
	if runtime.GOOS == "linux" && os.Geteuid() == 0 {
		environ, err = os.ReadFile(fmt.Sprintf("/proc/%d/environ", cmd.Process.Pid))
		if err != nil {
			cmd.Process.Kill()
			<-doneCh
			cmd.Wait()
			waited = true
			t.Fatalf("read supervisor environ: %v", err)
		}
	}

	if err := cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatalf("Signal SIGTERM: %v", err)
	}

	waitErr := make(chan error, 1)
	go func() { waitErr <- cmd.Wait() }()
	select {
	case err := <-waitErr:
		waited = true
		if err != nil {
			t.Fatalf("supervisor exit: %v; stderr:\n%s", err, stderrBuf.String())
		}
	case <-time.After(60 * time.Second):
		cmd.Process.Kill()
		<-waitErr
		waited = true
		t.Fatalf("supervisor did not exit within 60s after SIGTERM; stderr:\n%s", stderrBuf.String())
	}
	<-doneCh

	supervisorPID := cmd.Process.Pid

	if rep.FrameErr != "" {
		t.Errorf("FrameErr = %q, want \"\"", rep.FrameErr)
	}
	if len(rep.Frames) != 1 {
		t.Fatalf("len(Frames) = %d, want 1", len(rep.Frames))
	}
	got, err := ipc.ReadMessage(bytes.NewReader(rep.Frames[0]), ipc.Supervisor)
	if err != nil {
		t.Fatalf("ReadMessage: %v", err)
	}
	gotCfg, ok := got.(ipc.WorkerConfig)
	if !ok {
		t.Fatalf("frame type = %T, want ipc.WorkerConfig", got)
	}
	wantCfg := ipc.WorkerConfig{
		DomainID:          1,
		ExecutorAddress:   executorAddress,
		STFSpecHash:       types.HexToHashPanic(validHash),
		ConfirmationDepth: 2,
		NetworkName:       "test",
		StoragePath:       storage,
		MaxPendingBatches: 8,
	}
	if gotCfg != wantCfg {
		t.Errorf("Config frame = %+v, want %+v", gotCfg, wantCfg)
	}

	for _, e := range rep.Env {
		if strings.Contains(e, marker) {
			t.Errorf("marker found in worker Env: %q", e)
		}
	}
	for _, a := range rep.Args {
		if strings.Contains(a, marker) {
			t.Errorf("marker found in worker Args: %q", a)
		}
	}
	if bytes.Contains(rep.Frames[0], []byte(marker)) {
		t.Errorf("marker found in Config frame")
	}
	if strings.Contains(stdoutBuf.String(), marker) {
		t.Errorf("marker found in supervisor stdout")
	}
	if strings.Contains(stderrBuf.String(), marker) {
		t.Errorf("marker found in supervisor stderr")
	}

	wantEnv := []string{"GOMEMLIMIT=241591911"}
	if len(rep.Env) != len(wantEnv) || rep.Env[0] != wantEnv[0] {
		t.Errorf("Env = %v, want %v", rep.Env, wantEnv)
	}
	wantArgs := []string{probeBin}
	if len(rep.Args) != len(wantArgs) || rep.Args[0] != wantArgs[0] {
		t.Errorf("Args = %v, want %v", rep.Args, wantArgs)
	}
	// Three checks, none of them a literal count or position beyond the first four: a probe this
	// size also opens a descriptor or two of its own during init, independent of anything it does
	// and never passed to a child, so a check that keyed off a fixed count or a fixed number above
	// 3 would be unreliable. Instead, a real leak is identified by what it is, not where it lands.
	//
	// 1. The first four descriptors are exactly stdin, stdout, stderr, the supervisor channel.
	wantFDs := []int{0, 1, 2, 3}
	if len(rep.FDs) < len(wantFDs) {
		t.Errorf("FDs = %v, want at least %v", fdNumbers(rep.FDs), wantFDs)
	} else {
		for i, fd := range wantFDs {
			if rep.FDs[i].FD != fd {
				t.Errorf("FDs = %v, want %v as its first %d entries", fdNumbers(rep.FDs), wantFDs, len(wantFDs))
				break
			}
		}
	}
	// 2. No descriptor the probe holds is the same open file as inherited, identified by (dev,
	// ino) rather than by fd number, so a leak is caught wherever it lands.
	for _, e := range rep.FDs {
		if e.Dev == inheritedDev && e.Ino == inheritedIno {
			t.Errorf("fd %d in FDs = %v matches inherited's (dev %d, ino %d): the supervisor's descriptor leaked into the worker", e.FD, fdNumbers(rep.FDs), inheritedDev, inheritedIno)
		}
	}
	// 3. On Linux, every descriptor at 4 or above must resolve to a known Go-runtime entry: an
	// unidentified one is exactly what a future unsealed descriptor (a raw syscall.Open, a
	// leaked socket) would look like, and nothing else may pass silently.
	if runtime.GOOS == "linux" {
		for _, e := range rep.FDs {
			if e.FD < 4 {
				continue
			}
			if e.Link != "anon_inode:[eventpoll]" && e.Link != "anon_inode:[eventfd]" {
				t.Errorf("fd %d in FDs = %v resolves to %q, want a known Go-runtime descriptor", e.FD, fdNumbers(rep.FDs), e.Link)
			}
		}
	}
	if rep.PPID != supervisorPID {
		t.Errorf("PPID = %d, want supervisor pid %d", rep.PPID, supervisorPID)
	}

	return probeRun{rep: rep, supervisor: supervisorPID, environ: environ}
}

func requireLinux(t *testing.T) {
	t.Helper()
	if runtime.GOOS != "linux" {
		t.Skip("the /proc and ptrace attempts run only on Linux")
	}
}

func requireUIDSeparation(t *testing.T) {
	t.Helper()
	if runtime.GOOS != "linux" || os.Geteuid() != 0 {
		t.Skip("supervisor is not root; uid separation is off")
	}
}

// assertProcDenied checks the four /proc and ptrace attempts that hold for any supervisor user:
// a non-dumpable process's /proc/<pid> entries belong to root, and ptrace refuses a non-dumpable
// target without CAP_SYS_PTRACE.
func assertProcDenied(t *testing.T, rep probeReport) {
	t.Helper()
	for _, name := range []string{"environ", "mem", "fd"} {
		got, ok := rep.Attempts[name]
		if !ok {
			t.Errorf("Attempts[%q] missing", name)
			continue
		}
		if got.Errno != int(syscall.EACCES) {
			t.Errorf("Attempts[%q] = errno %d (%s), want EACCES", name, got.Errno, got.Err)
		}
	}
	got, ok := rep.Attempts["ptrace"]
	if !ok {
		t.Errorf("Attempts[\"ptrace\"] missing")
		return
	}
	if got.Errno != int(syscall.EPERM) {
		t.Errorf("Attempts[\"ptrace\"] = errno %d (%s), want EPERM", got.Errno, got.Err)
	}
}

// assertNoCapabilities checks the worker holds no permitted, effective or ambient capability:
// the uid switch actually dropped privilege, rather than leaving one that could let a worker
// bypass the ptrace and mem denials regardless of uid or the non-dumpable flag.
func assertNoCapabilities(t *testing.T, rep probeReport) {
	t.Helper()
	for name, hex := range map[string]string{"CapPrm": rep.CapPrm, "CapEff": rep.CapEff, "CapAmb": rep.CapAmb} {
		v, err := strconv.ParseUint(hex, 16, 64)
		if err != nil {
			t.Errorf("%s = %q: %v", name, hex, err)
			continue
		}
		if v != 0 {
			t.Errorf("%s = %q, want an all-zero capability set", name, hex)
		}
	}
}

// statDevIno returns the (dev, ino) pair identifying f's open file, for the fd-identity check in
// runWithProbe.
func statDevIno(f *os.File) (dev, ino uint64, err error) {
	fi, err := f.Stat()
	if err != nil {
		return 0, 0, err
	}
	st, ok := fi.Sys().(*syscall.Stat_t)
	if !ok {
		return 0, 0, fmt.Errorf("unsupported stat type %T", fi.Sys())
	}
	return uint64(st.Dev), uint64(st.Ino), nil
}

// fdNumbers extracts just the descriptor numbers from fds, for a compact failure message.
func fdNumbers(fds []probeFD) []int {
	out := make([]int, len(fds))
	for i, e := range fds {
		out[i] = e.FD
	}
	return out
}

func TestWaitForNode(t *testing.T) {
	p := supervisor.Policy{BackoffMin: time.Millisecond, BackoffMax: 2 * time.Millisecond}
	const endpoint = "http://node.example:35997"

	var lines []string
	logf := func(format string, args ...any) { lines = append(lines, fmt.Sprintf(format, args...)) }

	calls := 0
	check := func() error {
		calls++
		switch calls {
		case 1:
			return errors.New("down\nforged")
		case 2:
			return fmt.Errorf("%w: root mismatch", recovery.ErrHalted)
		}
		return nil
	}
	if !waitForNode(context.Background(), endpoint, check, p, logf) {
		t.Fatalf("waitForNode = false, want true")
	}
	if calls != 3 || len(lines) != 2 {
		t.Fatalf("calls = %d, lines = %d; want 3 and 2", calls, len(lines))
	}
	if !strings.Contains(lines[0], "is unusable") || !strings.Contains(lines[0], `down\nforged`) ||
		strings.Contains(lines[0], "\n") || strings.Contains(lines[0], "returned inconsistent data") {
		t.Fatalf("first line = %q", lines[0])
	}
	if !strings.Contains(lines[1], "returned inconsistent data") || strings.Contains(lines[1], "is unusable") {
		t.Fatalf("second line = %q", lines[1])
	}
	for _, l := range lines {
		if !strings.Contains(l, endpoint) {
			t.Fatalf("line %q lacks the endpoint", l)
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	failing := func() error { return errors.New("down") }
	if waitForNode(ctx, endpoint, failing, p, func(string, ...any) {}) {
		t.Fatalf("waitForNode with a cancelled context = true, want false")
	}

	// A signal ends the wait while a check is still in flight.
	ctx, cancel = context.WithCancel(context.Background())
	stuck := make(chan struct{})
	t.Cleanup(func() { close(stuck) })
	done := make(chan bool, 1)
	go func() {
		done <- waitForNode(ctx, endpoint, func() error { <-stuck; return nil }, p, func(string, ...any) {})
	}()
	cancel()
	select {
	case ok := <-done:
		if ok {
			t.Fatalf("waitForNode with an in-flight check = true, want false")
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("waitForNode did not return while a check was in flight")
	}
}

func TestBootRejectsMismatchedKey(t *testing.T) {
	seed := newMarker(t)
	cfg := map[string]any{
		"executorAddress":   types.Address{0x01}.String(),
		"networkName":       "test",
		"pillarName":        "test",
		"znndEndpoint":      "http://127.0.0.1:1",
		"confirmationDepth": float64(2),
		"storagePath":       openTempDir(t),
		"submissionKeySource": map[string]any{
			"kind": "ENV",
			"name": "EXECUTOR_KEY",
		},
		"workers": []any{
			map[string]any{
				"domainId":    float64(1),
				"binary":      probeBin,
				"stfSpecHash": validHash,
				"memoryLimit": float64(268435456),
				"cpuLimit":    1.0,
			},
		},
	}
	b, err := json.Marshal(cfg)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	cfgPath := filepath.Join(t.TempDir(), "executor.json")
	if err := os.WriteFile(cfgPath, b, 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	cmd := exec.Command(executorBin, "-config", cfgPath)
	cmd.Env = append(os.Environ(), "EXECUTOR_KEY="+seed)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	err = cmd.Run()
	var ee *exec.ExitError
	if !errors.As(err, &ee) || ee.ExitCode() != 78 {
		t.Fatalf("Run = %v, want exit status 78; stderr:\n%s", err, stderr.String())
	}
	if strings.Contains(stderr.String(), seed) {
		t.Errorf("seed found in supervisor stderr")
	}
}
