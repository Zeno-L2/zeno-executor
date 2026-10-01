package workerproc

import (
	"encoding/json"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"syscall"
	"testing"
	"time"
)

// TestMain dispatches to a helper mode before running any test when WORKERPROC_HELPER is
// set, so a test can exec this same binary as a worker. Otherwise it seals inherited
// descriptors, as the supervisor does at start, before the suite runs.
func TestMain(m *testing.M) {
	if mode := os.Getenv("WORKERPROC_HELPER"); mode != "" {
		os.Exit(runHelper(mode))
	}

	if err := SealInheritedFDs(); err != nil {
		os.Stderr.WriteString(err.Error() + "\n")
		os.Exit(1)
	}

	os.Exit(m.Run())
}

// helperReport is what the "report" helper mode writes to descriptor 3.
type helperReport struct {
	FDs  []int
	Env  []string
	Pid  int
	Pgid int
	Sid  int
}

// forkReport is what the "fork-and-exit" helper mode writes to descriptor 3.
type forkReport struct {
	ChildPid int
}

func runHelper(mode string) int {
	switch mode {
	case "report":
		return runReportHelper()
	case "exit":
		code, err := strconv.Atoi(os.Getenv("WORKERPROC_ARG"))
		if err != nil {
			return 1
		}
		return code
	case "alloc":
		return runAllocHelper()
	case "read":
		return runReadHelper()
	case "selfcheck-nondumpable":
		return runSelfcheckNonDumpableHelper()
	case "fork-and-exit":
		return runForkAndExitHelper()
	case "sleep":
		return runSleepHelper()
	default:
		return 1
	}
}

func runReportHelper() int {
	var fds []int
	var stat syscall.Stat_t
	for fd := 0; fd < 1024; fd++ {
		if syscall.Fstat(fd, &stat) == nil {
			fds = append(fds, fd)
		}
	}

	sid, err := getsid()
	if err != nil {
		return 1
	}

	report := helperReport{
		FDs:  fds,
		Env:  os.Environ(),
		Pid:  os.Getpid(),
		Pgid: syscall.Getpgrp(),
		Sid:  sid,
	}

	fd3 := os.NewFile(3, "workerproc-worker")
	enc := json.NewEncoder(fd3)
	if err := enc.Encode(report); err != nil {
		return 1
	}

	if _, err := io.Copy(io.Discard, fd3); err != nil {
		return 1
	}
	return 0
}

func runAllocHelper() int {
	n, err := strconv.Atoi(os.Getenv("WORKERPROC_ARG"))
	if err != nil {
		return 1
	}

	var slices [][]byte
	for i := 0; i < n; i++ {
		buf := make([]byte, 1<<20)
		for j := 0; j < len(buf); j += 4096 {
			buf[j] = 1
		}
		slices = append(slices, buf)
	}
	runtime.KeepAlive(slices)
	return 0
}

func runReadHelper() int {
	_, err := os.ReadFile(os.Getenv("WORKERPROC_ARG"))
	if err == nil {
		return 0
	}
	if errors.Is(err, syscall.EACCES) {
		return 13
	}
	return 1
}

// runSelfcheckNonDumpableHelper calls SetNonDumpable on itself, then starts a grandchild at
// its own uid (a zero Host sets no Credential, so the grandchild inherits it) that tries to
// read this process's /proc/<pid>/environ. It returns the grandchild's exit code, so a
// same-uid caller can tell whether SetNonDumpable alone, with no uid separation, still blocks
// a same-uid process from reading it.
func runSelfcheckNonDumpableHelper() int {
	if err := SetNonDumpable(); err != nil {
		return 2
	}

	path, err := os.Executable()
	if err != nil {
		return 2
	}

	h := &Host{}
	supervisorEnd, workerEnd, err := Socketpair()
	if err != nil {
		return 2
	}

	spec := Spec{
		Path: path,
		Env:  []string{"WORKERPROC_HELPER=read", "WORKERPROC_ARG=/proc/" + strconv.Itoa(os.Getpid()) + "/environ"},
		Conn: workerEnd,
	}
	p, err := h.Start(spec)
	workerEnd.Close()
	if err != nil {
		supervisorEnd.Close()
		return 2
	}
	defer supervisorEnd.Close()

	return p.Wait().Code
}

// runForkAndExitHelper starts a grandchild that inherits its own stdout, stderr and
// descriptor 3, reports the grandchild's pid over descriptor 3, and exits immediately,
// leaving the grandchild running. This is the case Wait's WaitDelay exists for: with stdout
// and stderr piped through a non-*os.File writer (stdioRelay), a descendant that inherits
// those descriptors can otherwise hold cmd.Wait open long after this helper itself has
// exited. Handing the grandchild descriptor 3 too, not just stdout and stderr, gives the
// test a way to prove the descendant is actually gone afterward that does not depend on it
// being reaped: a process releases every descriptor on exit regardless of zombie state, so
// EOF on the other end of that descriptor is reliable even under an init-less pid 1 that
// never reaps it, unlike kill(pid, 0), which still succeeds against a zombie.
func runForkAndExitHelper() int {
	path, err := os.Executable()
	if err != nil {
		return 2
	}

	fd3 := os.NewFile(3, "workerproc-worker")

	child := exec.Command(path)
	child.Env = []string{"WORKERPROC_HELPER=sleep", "WORKERPROC_ARG=30"}
	child.Stdout = os.Stdout
	child.Stderr = os.Stderr
	child.ExtraFiles = []*os.File{fd3}
	if err := child.Start(); err != nil {
		return 2
	}

	if err := json.NewEncoder(fd3).Encode(forkReport{ChildPid: child.Process.Pid}); err != nil {
		return 2
	}
	return 0
}

// runSleepHelper sleeps for WORKERPROC_ARG seconds, so a caller can start it as a grandchild
// that outlives its own parent.
func runSleepHelper() int {
	n, err := strconv.Atoi(os.Getenv("WORKERPROC_ARG"))
	if err != nil {
		return 1
	}
	time.Sleep(time.Duration(n) * time.Second)
	return 0
}

// getsid returns the caller's own session id. syscall.Getsid exists on darwin but not in the
// linux build of package syscall, so this goes through the raw syscall directly; SYS_GETSID
// is defined for both darwin and every Linux arch this package supports.
func getsid() (int, error) {
	r1, _, errno := syscall.Syscall(syscall.SYS_GETSID, 0, 0, 0)
	if errno != 0 {
		return 0, errno
	}
	return int(r1), nil
}

// startHelper starts the worker binary (this test binary, re-exec'd via a helper mode) and
// returns the started process and the supervisor end of its channel.
func startHelper(t *testing.T, h *Host, s Spec) (*Process, *os.File) {
	t.Helper()

	supervisorEnd, workerEnd, err := Socketpair()
	if err != nil {
		t.Fatalf("Socketpair: %v", err)
	}

	s.Conn = workerEnd
	p, err := h.Start(s)
	workerEnd.Close()
	if err != nil {
		supervisorEnd.Close()
		t.Fatalf("Start: %v", err)
	}

	t.Cleanup(func() { supervisorEnd.Close() })
	return p, supervisorEnd
}

func helperSpec(mode, arg string, env []string) Spec {
	path, err := os.Executable()
	if err != nil {
		panic(err)
	}

	fullEnv := append([]string{"WORKERPROC_HELPER=" + mode}, env...)
	if arg != "" {
		fullEnv = append(fullEnv, "WORKERPROC_ARG="+arg)
	}

	return Spec{Path: path, Env: fullEnv}
}

func readReport(t *testing.T, f *os.File) helperReport {
	t.Helper()

	var report helperReport
	if err := json.NewDecoder(f).Decode(&report); err != nil {
		t.Fatalf("decode report: %v", err)
	}
	return report
}

func TestStartReportsEnvFDsAndProcessGroup(t *testing.T) {
	h := &Host{}
	spec := helperSpec("report", "", []string{"A=1"})
	p, conn := startHelper(t, h, spec)

	report := readReport(t, conn)
	if len(report.Env) != len(spec.Env) {
		t.Fatalf("Env: got %v, want %v", report.Env, spec.Env)
	}
	for i, want := range spec.Env {
		if report.Env[i] != want {
			t.Fatalf("Env[%d]: got %q, want %q", i, report.Env[i], want)
		}
	}
	if want := []int{0, 1, 2, 3}; !equalInts(report.FDs, want) {
		t.Fatalf("FDs: got %v, want %v", report.FDs, want)
	}
	if report.Pid != p.PID() {
		t.Fatalf("Pid: got %d, want %d", report.Pid, p.PID())
	}
	testPgid := syscall.Getpgrp()
	if report.Pgid != report.Pid {
		t.Fatalf("Pgid: got %d, want %d (its own pid)", report.Pgid, report.Pid)
	}
	if report.Pgid == testPgid {
		t.Fatalf("Pgid: got %d, want different from the test's %d", report.Pgid, testPgid)
	}

	testSid, err := getsid()
	if err != nil {
		t.Fatalf("getsid: %v", err)
	}
	if report.Sid != report.Pid {
		t.Fatalf("Sid: got %d, want %d (its own pid, a session leader)", report.Sid, report.Pid)
	}
	if report.Sid == testSid {
		t.Fatalf("Sid: got %d, want different from the test's %d", report.Sid, testSid)
	}

	conn.Close()
	exit := p.Wait()
	if exit != (Exit{Code: 0}) {
		t.Fatalf("Wait: got %+v, want Exit{Code: 0}", exit)
	}
}

func equalInts(got, want []int) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range got {
		if got[i] != want[i] {
			return false
		}
	}
	return true
}

func TestSealInheritedFDsClosesUnexpectedDescriptors(t *testing.T) {
	h := &Host{}

	f, err := os.CreateTemp(t.TempDir(), "sealed")
	if err != nil {
		t.Fatalf("CreateTemp: %v", err)
	}
	t.Cleanup(func() { f.Close() })

	nfd, err := syscall.Dup(int(f.Fd()))
	if err != nil {
		t.Fatalf("Dup: %v", err)
	}
	t.Cleanup(func() { syscall.Close(nfd) })
	if nfd <= 3 {
		t.Fatalf("Dup: got fd %d, want > 3", nfd)
	}

	// Control: before sealing, a helper inherits the unsealed descriptor too.
	p1, conn1 := startHelper(t, h, helperSpec("report", "", nil))
	report1 := readReport(t, conn1)
	conn1.Close()
	p1.Wait()
	if !containsInt(report1.FDs, nfd) {
		t.Fatalf("control FDs: got %v, want to contain %d", report1.FDs, nfd)
	}

	if err := SealInheritedFDs(); err != nil {
		t.Fatalf("SealInheritedFDs: %v", err)
	}

	p2, conn2 := startHelper(t, h, helperSpec("report", "", nil))
	report2 := readReport(t, conn2)
	conn2.Close()
	p2.Wait()
	if want := []int{0, 1, 2, 3}; !equalInts(report2.FDs, want) {
		t.Fatalf("sealed FDs: got %v, want %v", report2.FDs, want)
	}
}

func containsInt(s []int, v int) bool {
	for _, x := range s {
		if x == v {
			return true
		}
	}
	return false
}

func TestWaitReportsExitCode(t *testing.T) {
	h := &Host{}
	p, conn := startHelper(t, h, helperSpec("exit", "7", nil))
	conn.Close()

	exit := p.Wait()
	if exit != (Exit{Code: 7}) {
		t.Fatalf("Wait: got %+v, want Exit{Code: 7}", exit)
	}
}

func TestWaitReportsSignal(t *testing.T) {
	h := &Host{}
	p, conn := startHelper(t, h, helperSpec("report", "", nil))
	readReport(t, conn)

	if err := p.Signal(syscall.SIGKILL); err != nil {
		t.Fatalf("Signal: %v", err)
	}

	exit := p.Wait()
	if exit.Code != -1 || exit.Signal != syscall.SIGKILL {
		t.Fatalf("Wait: got %+v, want Exit{Code: -1, Signal: SIGKILL}", exit)
	}
}

// TestWaitReturnsPromptlyWhenADescendantHoldsStdio covers the case WaitDelay exists for: a
// worker that forks a child inheriting its stdout and stderr, then exits, must not block
// Wait until that child exits too, and the leftover-kill after Wait must reach the child.
func TestWaitReturnsPromptlyWhenADescendantHoldsStdio(t *testing.T) {
	h := &Host{}
	p, conn := startHelper(t, h, helperSpec("fork-and-exit", "", nil))

	var report forkReport
	if err := json.NewDecoder(conn).Decode(&report); err != nil {
		t.Fatalf("decode fork report: %v", err)
	}
	// conn stays open: the grandchild also holds descriptor 3, and the EOF check below
	// needs conn open to observe it closing.

	done := make(chan Exit, 1)
	go func() { done <- p.Wait() }()

	select {
	case exit := <-done:
		if exit != (Exit{Code: 0}) {
			t.Fatalf("Wait: got %+v, want Exit{Code: 0}", exit)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Wait did not return within 5s; a descendant holding stdout/stderr blocked it")
	}

	// The grandchild (pid report.ChildPid) inherited descriptor 3 too. A process releases
	// every descriptor it holds when it exits, whether or not anything ever reaps it, so EOF
	// here proves Wait's leftover kill actually reached the grandchild. This does not depend
	// on the grandchild being reaped: under docker/compose.test.yml's init-less pid 1, a
	// killed grandchild becomes a zombie that a bare kill(pid, 0) would still see as alive.
	eof := make(chan error, 1)
	go func() {
		_, err := io.Copy(io.Discard, conn)
		eof <- err
	}()

	select {
	case err := <-eof:
		if err != nil {
			t.Fatalf("read after Wait: got %v, want EOF", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("descendant (pid %d) still holds descriptor 3 open 2s after Wait's leftover kill", report.ChildPid)
	}
}

func TestStartWithNilConnFails(t *testing.T) {
	h := &Host{}
	path, err := os.Executable()
	if err != nil {
		t.Fatalf("Executable: %v", err)
	}

	p, err := h.Start(Spec{Path: path, Env: []string{"WORKERPROC_HELPER=exit", "WORKERPROC_ARG=0"}})
	if err == nil {
		t.Fatal("Start: got nil error for a nil Conn, want an error")
	}
	if p != nil {
		t.Fatalf("Start: got non-nil Process %+v, want nil", p)
	}
}

func TestStartWithMissingExecutableFails(t *testing.T) {
	h := &Host{}

	supervisorEnd, workerEnd, err := Socketpair()
	if err != nil {
		t.Fatalf("Socketpair: %v", err)
	}
	defer supervisorEnd.Close()

	p, err := h.Start(Spec{Path: filepath.Join(t.TempDir(), "does-not-exist"), Conn: workerEnd})
	workerEnd.Close()
	if err == nil {
		t.Fatal("Start: got nil error for a missing executable, want an error")
	}
	if p != nil {
		t.Fatalf("Start: got non-nil Process %+v, want nil", p)
	}
}

func TestNonLinuxEnforcementOff(t *testing.T) {
	if runtime.GOOS == "linux" {
		t.Skip("covered by workerproc_linux_test.go")
	}

	h := NewHost()
	limits, uids, reason := h.Enforcement()
	if limits || uids || reason != "platform has no per-process limits" {
		t.Fatalf("Enforcement: got (%v, %v, %q), want (false, false, %q)", limits, uids, reason, "platform has no per-process limits")
	}

	dir := t.TempDir() + "/a/b/c"
	if err := h.PrepareDir(dir, 0); err != nil {
		t.Fatalf("PrepareDir: %v", err)
	}
	if info, err := os.Stat(dir); err != nil || !info.IsDir() {
		t.Fatalf("PrepareDir: %s was not created as a directory", dir)
	}
}
