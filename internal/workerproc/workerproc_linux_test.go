//go:build linux

package workerproc

import (
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
)

// enforcedHost returns a Host with both limits and uids on, skipping the test otherwise, and
// a copy of this test binary in a 0755 temp directory a non-root worker can execute.
func enforcedHost(t *testing.T) (*Host, string) {
	t.Helper()

	h := NewHost()
	limits, uids, reason := h.Enforcement()
	if !limits || !uids {
		t.Skipf("enforcement off: %s", reason)
	}

	dir, err := os.MkdirTemp("", "workerproc-")
	if err != nil {
		t.Fatalf("MkdirTemp: %v", err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	if err := os.Chmod(dir, 0o755); err != nil {
		t.Fatalf("Chmod: %v", err)
	}

	src, err := os.Executable()
	if err != nil {
		t.Fatalf("Executable: %v", err)
	}
	copied := filepath.Join(dir, "workerproc.test")
	copyExecutable(t, src, copied)

	return h, copied
}

func copyExecutable(t *testing.T, src, dst string) {
	t.Helper()

	in, err := os.Open(src)
	if err != nil {
		t.Fatalf("open %s: %v", src, err)
	}
	defer in.Close()

	out, err := os.OpenFile(dst, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o755)
	if err != nil {
		t.Fatalf("create %s: %v", dst, err)
	}
	defer out.Close()

	if _, err := io.Copy(out, in); err != nil {
		t.Fatalf("copy %s to %s: %v", src, dst, err)
	}
	if err := out.Chmod(0o755); err != nil {
		t.Fatalf("chmod %s: %v", dst, err)
	}
}

// linuxHelperSpec builds a Spec for the copied test binary at path, running as uid 10001
// with a unique cgroup leaf name and the given limits (256 MiB, 1 CPU when limits is zero).
func linuxHelperSpec(path, name, mode, arg string, limits Limits, env []string) Spec {
	if limits == (Limits{}) {
		limits = Limits{MemoryBytes: 256 << 20, CPUs: 1}
	}

	fullEnv := append([]string{"WORKERPROC_HELPER=" + mode}, env...)
	if arg != "" {
		fullEnv = append(fullEnv, "WORKERPROC_ARG="+arg)
	}

	return Spec{Path: path, Name: name, UID: 10001, Env: fullEnv, Limits: limits}
}

func startLinuxHelper(t *testing.T, h *Host, s Spec) (*Process, *os.File) {
	t.Helper()

	t.Cleanup(func() { os.Remove(filepath.Join(h.cgroup, s.Name)) })

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

func TestLinuxOOMKillsOverLimit(t *testing.T) {
	h, path := enforcedHost(t)
	spec := linuxHelperSpec(path, "workerproc-test-oom", "alloc", "128", Limits{MemoryBytes: 64 << 20, CPUs: 1}, nil)
	p, conn := startLinuxHelper(t, h, spec)
	conn.Close()

	exit := p.Wait()
	if exit.Signal != syscall.SIGKILL {
		t.Fatalf("Wait: got signal %v, want SIGKILL", exit.Signal)
	}
	if !exit.OOMKilled {
		t.Fatalf("Wait: got OOMKilled false, want true")
	}
}

func TestLinuxUnderLimitExitsCleanAndLimitsAreWritten(t *testing.T) {
	h, path := enforcedHost(t)
	spec := linuxHelperSpec(path, "workerproc-test-under-limit", "alloc", "16", Limits{MemoryBytes: 64 << 20, CPUs: 0.5}, nil)
	p, conn := startLinuxHelper(t, h, spec)
	conn.Close()

	exit := p.Wait()
	if exit != (Exit{Code: 0}) {
		t.Fatalf("Wait: got %+v, want Exit{Code: 0}", exit)
	}

	leaf := filepath.Join(h.cgroup, spec.Name)
	memMax := readTrimmed(t, filepath.Join(leaf, "memory.max"))
	if memMax != "67108864" {
		t.Fatalf("memory.max: got %q, want %q", memMax, "67108864")
	}
	cpuMax := readTrimmed(t, filepath.Join(leaf, "cpu.max"))
	if cpuMax != "50000 100000" {
		t.Fatalf("cpu.max: got %q, want %q", cpuMax, "50000 100000")
	}
}

func readTrimmed(t *testing.T, path string) string {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile %s: %v", path, err)
	}
	return strings.TrimSpace(string(data))
}

func TestLinuxUidSeparationDeniesRootOnlyFiles(t *testing.T) {
	h, path := enforcedHost(t)

	if err := SetNonDumpable(); err != nil {
		t.Fatalf("SetNonDumpable: %v", err)
	}
	r1, _, errno := syscall.RawSyscall(syscall.SYS_PRCTL, syscall.PR_GET_DUMPABLE, 0, 0)
	if errno != 0 {
		t.Fatalf("PR_GET_DUMPABLE: %v", errno)
	}
	if r1 != 0 {
		t.Fatalf("PR_GET_DUMPABLE: got %d, want 0", r1)
	}

	dir, err := os.MkdirTemp("", "workerproc-uid-")
	if err != nil {
		t.Fatalf("MkdirTemp: %v", err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	if err := os.Chmod(dir, 0o755); err != nil {
		t.Fatalf("Chmod: %v", err)
	}

	private := filepath.Join(dir, "private")
	if err := os.WriteFile(private, []byte("secret"), 0o400); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	control := filepath.Join(dir, "control")
	if err := os.WriteFile(control, []byte("public"), 0o444); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	if got := runLinuxRead(t, h, path, "workerproc-test-uid-private", private); got != 13 {
		t.Fatalf("read of 0400 file: got exit %d, want 13", got)
	}
	if got := runLinuxRead(t, h, path, "workerproc-test-uid-control", control); got != 0 {
		t.Fatalf("read of 0444 file: got exit %d, want 0", got)
	}

	environ := "/proc/" + strconv.Itoa(os.Getpid()) + "/environ"
	if got := runLinuxRead(t, h, path, "workerproc-test-uid-environ", environ); got != 13 {
		t.Fatalf("read of parent's environ: got exit %d, want 13", got)
	}

	// The uid-separated spawns above each ran a pre-exec setuid on a clone of this process's
	// address space, which resets the dumpable flag to fs.suid_dumpable. Confirm this process
	// is still non-dumpable, so the flag is not silently undone by spawning a worker. Only
	// SUID_DUMP_USER (1) makes the process dumpable again; SUID_DUMP_DISABLE (0) and
	// SUID_DUMP_ROOT (2, common with apport) both still deny a same-uid, non-root ptrace.
	r2, _, errno := syscall.RawSyscall(syscall.SYS_PRCTL, syscall.PR_GET_DUMPABLE, 0, 0)
	if errno != 0 {
		t.Fatalf("PR_GET_DUMPABLE after spawns: %v", errno)
	}
	if r2 == 1 {
		t.Fatalf("PR_GET_DUMPABLE after spawns: got %d, want not 1 (dumpable)", r2)
	}
}

func runLinuxRead(t *testing.T, h *Host, path, name, target string) int {
	t.Helper()

	spec := linuxHelperSpec(path, name, "read", target, Limits{}, nil)
	p, conn := startLinuxHelper(t, h, spec)
	conn.Close()
	return p.Wait().Code
}

// TestLinuxSetNonDumpableBlocksSameUidAccess covers the case root cannot show: with uids
// off, worker and supervisor share a uid, and SetNonDumpable is the only barrier. A helper at
// uid 10001 calls SetNonDumpable and starts a grandchild, also at uid 10001, that tries to
// read the helper's own /proc/<pid>/environ; the grandchild must be denied.
func TestLinuxSetNonDumpableBlocksSameUidAccess(t *testing.T) {
	h, path := enforcedHost(t)

	spec := linuxHelperSpec(path, "workerproc-test-nondumpable", "selfcheck-nondumpable", "", Limits{}, nil)
	p, conn := startLinuxHelper(t, h, spec)
	conn.Close()

	if got := p.Wait().Code; got != 13 {
		t.Fatalf("grandchild read of helper's environ: got exit %d, want 13", got)
	}
}

func TestLinuxPrepareDirHandsTreeToUidWithoutFollowingSymlinks(t *testing.T) {
	h, _ := enforcedHost(t)

	outside, err := os.MkdirTemp("", "workerproc-outside-")
	if err != nil {
		t.Fatalf("MkdirTemp: %v", err)
	}
	t.Cleanup(func() { os.RemoveAll(outside) })
	rootOwned := filepath.Join(outside, "root-owned")
	if err := os.WriteFile(rootOwned, []byte("x"), 0o600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	d, err := os.MkdirTemp("", "workerproc-prepare-")
	if err != nil {
		t.Fatalf("MkdirTemp: %v", err)
	}
	t.Cleanup(func() { os.RemoveAll(d) })

	a := filepath.Join(d, "a")
	if err := os.Mkdir(a, 0o755); err != nil {
		t.Fatalf("Mkdir: %v", err)
	}
	f := filepath.Join(a, "f")
	if err := os.WriteFile(f, []byte("x"), 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	link := filepath.Join(d, "link")
	if err := os.Symlink(rootOwned, link); err != nil {
		t.Fatalf("Symlink: %v", err)
	}

	if err := h.PrepareDir(d, 10001); err != nil {
		t.Fatalf("PrepareDir: %v", err)
	}

	for _, p := range []string{d, a, f, link} {
		var st syscall.Stat_t
		if err := syscall.Lstat(p, &st); err != nil {
			t.Fatalf("Lstat %s: %v", p, err)
		}
		if st.Uid != 10001 || st.Gid != 10001 {
			t.Fatalf("Lstat %s: got uid %d gid %d, want 10001, 10001", p, st.Uid, st.Gid)
		}
	}

	var rootSt syscall.Stat_t
	if err := syscall.Lstat(rootOwned, &rootSt); err != nil {
		t.Fatalf("Lstat %s: %v", rootOwned, err)
	}
	if rootSt.Uid != 0 {
		t.Fatalf("Lstat %s: got uid %d, want 0 (link target untouched)", rootOwned, rootSt.Uid)
	}
}

// TestLinuxCgroupLeafIsFreshOnEveryStart proves a leaf reused across two Start/Wait cycles
// does not carry the first run's cgroup.kill into the second: on a kernel where writing
// cgroup.kill to a leaf keeps killing whatever is cloned into it afterward, a stale leaf would
// SIGKILL the second run at fork instead of letting it reach its own exit code.
func TestLinuxCgroupLeafIsFreshOnEveryStart(t *testing.T) {
	h, path := enforcedHost(t)
	const name = "workerproc-test-fresh-leaf"

	for i := 0; i < 2; i++ {
		spec := linuxHelperSpec(path, name, "exit", "7", Limits{}, nil)
		p, conn := startLinuxHelper(t, h, spec)
		conn.Close()

		exit := p.Wait()
		if exit != (Exit{Code: 7}) {
			t.Fatalf("run %d: Wait = %+v, want Exit{Code: 7}", i, exit)
		}
	}
}

func TestLinuxConfigureGuardsRejectBeforeTouchingCgroupTree(t *testing.T) {
	h, path := enforcedHost(t)

	const baseName = "a" // the valid spec's cgroup leaf name, unchanged except in the Name cases

	run := func(t *testing.T, mutate func(*Spec)) {
		t.Helper()

		s := linuxHelperSpec(path, baseName, "exit", "0", Limits{}, nil)
		mutate(&s)

		supervisorEnd, workerEnd, err := Socketpair()
		if err != nil {
			t.Fatalf("Socketpair: %v", err)
		}
		defer supervisorEnd.Close()
		s.Conn = workerEnd

		p, err := h.Start(s)
		workerEnd.Close()
		if err == nil {
			t.Fatal("Start: got nil error, want an error")
		}
		if p != nil {
			t.Fatalf("Start: got non-nil Process %+v, want nil", p)
		}
	}

	// assertNoLeaf stats a real candidate path under h.cgroup, not an arbitrary unused name,
	// so the check actually proves the rejected call created nothing.
	assertNoLeaf := func(t *testing.T, name string) {
		t.Helper()
		if _, err := os.Stat(filepath.Join(h.cgroup, name)); !os.IsNotExist(err) {
			t.Fatalf("Stat %s: got err %v, want ErrNotExist", name, err)
		}
	}

	t.Run("UID zero", func(t *testing.T) {
		run(t, func(s *Spec) { s.UID = 0 })
		assertNoLeaf(t, baseName)
	})
	t.Run("Name empty", func(t *testing.T) {
		run(t, func(s *Spec) { s.Name = "" })

		// Confirms the rejected call never touched the delegated root's own limit files.
		rootMax := readTrimmed(t, filepath.Join(h.cgroup, "memory.max"))
		if rootMax != "max" {
			t.Fatalf("root memory.max: got %q, want %q", rootMax, "max")
		}
	})
	t.Run("Name path traversal", func(t *testing.T) {
		run(t, func(s *Spec) { s.Name = "../x" })
		assertNoLeaf(t, filepath.Join("..", "x"))
	})
	t.Run("Name nested path", func(t *testing.T) {
		run(t, func(s *Spec) { s.Name = "a/b" })
	})
	t.Run("Name supervisor", func(t *testing.T) {
		// "supervisor" is the pre-existing leaf NewHost moved ancestor pids into, so its
		// existence proves nothing about this rejection; only the rejection itself matters.
		run(t, func(s *Spec) { s.Name = "supervisor" })
	})
	t.Run("MemoryBytes zero", func(t *testing.T) {
		run(t, func(s *Spec) { s.Limits.MemoryBytes = 0 })
		assertNoLeaf(t, baseName)
	})
	t.Run("CPUs zero", func(t *testing.T) {
		run(t, func(s *Spec) { s.Limits.CPUs = 0 })
		assertNoLeaf(t, baseName)
	})
}
