//go:build linux

package workerproc

import (
	"errors"
	"fmt"
	"math"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
)

const fdDir = "/proc/self/fd"

// NewHost probes this machine's ability to separate worker uids and enforce cgroup v2
// limits. It never fails; whatever it cannot enforce is recorded in Enforcement's reason.
func NewHost() *Host {
	h := &Host{}

	if os.Geteuid() != 0 {
		h.reason = "not running as root"
		return h
	}
	h.uids = true

	root, err := delegateCgroup()
	if err != nil {
		if errors.Is(err, syscall.EROFS) {
			h.reason = "cgroup filesystem is read-only (container not privileged)"
		} else {
			h.reason = err.Error()
		}
		return h
	}

	h.limits = true
	h.cgroup = root
	return h
}

// delegateCgroup finds this process's own cgroup v2 leaf, verifies it delegates the memory
// and cpu controllers to a set of pids that are all this process or its ancestors, moves
// them into a "supervisor" child leaf, and enables the controllers for that leaf's
// children. It returns the absolute path of the delegated root, whose children become
// worker leaves.
func delegateCgroup() (string, error) {
	data, err := os.ReadFile("/proc/self/cgroup")
	if err != nil {
		return "", fmt.Errorf("cgroup read /proc/self/cgroup: %w", err)
	}
	lines := strings.Split(strings.TrimRight(string(data), "\n"), "\n")
	if len(lines) != 1 || !strings.HasPrefix(lines[0], "0::") {
		return "", fmt.Errorf("cgroup v2 unified hierarchy not in use")
	}
	p := strings.TrimPrefix(lines[0], "0::")

	if path.Base(p) == "supervisor" {
		p = path.Dir(p)
	}
	root := filepath.Join("/sys/fs/cgroup", p)

	controllers, err := os.ReadFile(filepath.Join(root, "cgroup.controllers"))
	if err != nil {
		return "", fmt.Errorf("cgroup cgroup.controllers: %w", err)
	}
	fields := strings.Fields(string(controllers))
	if !hasField(fields, "memory") || !hasField(fields, "cpu") {
		return "", fmt.Errorf("cgroup cgroup.controllers: memory and cpu controllers not both delegated")
	}

	ancestors, err := ancestorPids()
	if err != nil {
		return "", fmt.Errorf("cgroup ancestor pids: %w", err)
	}

	procsPath := filepath.Join(root, "cgroup.procs")
	procs, err := os.ReadFile(procsPath)
	if err != nil {
		return "", fmt.Errorf("cgroup cgroup.procs: %w", err)
	}
	pids, err := parsePids(procs)
	if err != nil {
		return "", fmt.Errorf("cgroup cgroup.procs: %w", err)
	}
	for _, pid := range pids {
		if !ancestors[pid] {
			return "", fmt.Errorf("cgroup cgroup.procs: pid %d is not this process or an ancestor", pid)
		}
	}

	supervisorLeaf := filepath.Join(root, "supervisor")
	if err := os.Mkdir(supervisorLeaf, 0o755); err != nil && !errors.Is(err, os.ErrExist) {
		return "", fmt.Errorf("cgroup mkdir supervisor: %w", err)
	}

	supervisorProcs := filepath.Join(supervisorLeaf, "cgroup.procs")
	for _, pid := range pids {
		if err := writeCgroupFile(supervisorProcs, strconv.Itoa(pid)); err != nil && !errors.Is(err, syscall.ESRCH) {
			return "", fmt.Errorf("cgroup move pids: %w", err)
		}
	}

	if err := writeCgroupFile(filepath.Join(root, "cgroup.subtree_control"), "+memory +cpu"); err != nil {
		return "", fmt.Errorf("cgroup subtree_control: %w", err)
	}

	return root, nil
}

func hasField(fields []string, want string) bool {
	for _, f := range fields {
		if f == want {
			return true
		}
	}
	return false
}

func parsePids(data []byte) ([]int, error) {
	var pids []int
	for _, line := range strings.Split(strings.TrimRight(string(data), "\n"), "\n") {
		if line == "" {
			continue
		}
		pid, err := strconv.Atoi(line)
		if err != nil {
			return nil, fmt.Errorf("parse pid %q: %w", line, err)
		}
		pids = append(pids, pid)
	}
	return pids, nil
}

// ancestorPids returns the set of this process's pid and every ancestor up to and
// including pid 1.
func ancestorPids() (map[int]bool, error) {
	set := map[int]bool{os.Getpid(): true}
	pid := os.Getpid()
	for {
		ppid, err := readPPID(pid)
		if err != nil {
			return nil, err
		}
		if ppid == 0 {
			return set, nil
		}
		set[ppid] = true
		pid = ppid
	}
}

// readPPID reads the parent pid of pid from /proc/<pid>/stat, splitting the text after its
// last ')' into whitespace fields; the ppid is the second field, after the one-letter state.
func readPPID(pid int) (int, error) {
	data, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid))
	if err != nil {
		return 0, err
	}
	idx := strings.LastIndexByte(string(data), ')')
	if idx < 0 || idx+2 > len(data) {
		return 0, fmt.Errorf("malformed /proc/%d/stat", pid)
	}
	fields := strings.Fields(string(data[idx+2:]))
	if len(fields) < 2 {
		return 0, fmt.Errorf("malformed /proc/%d/stat", pid)
	}
	return strconv.Atoi(fields[1])
}

// writeCgroupFile writes value to the already-existing cgroup control file at path. It uses
// O_WRONLY without O_CREATE, unlike os.WriteFile, because a cgroup control file is never
// created by writing it.
func writeCgroupFile(path, value string) error {
	f, err := os.OpenFile(path, os.O_WRONLY, 0)
	if err != nil {
		return err
	}
	defer f.Close()
	_, err = f.WriteString(value)
	return err
}

// readOOMKill reads the oom_kill count from leaf's memory.events, matching the whitespace
// field that is exactly "oom_kill" and not, for example, "oom_group_kill".
func readOOMKill(leaf string) (uint64, error) {
	data, err := os.ReadFile(filepath.Join(leaf, "memory.events"))
	if err != nil {
		return 0, err
	}
	for _, line := range strings.Split(string(data), "\n") {
		fields := strings.Fields(line)
		if len(fields) == 2 && fields[0] == "oom_kill" {
			return strconv.ParseUint(fields[1], 10, 64)
		}
	}
	return 0, fmt.Errorf("no oom_kill field in memory.events")
}

// PrepareDir creates dir and every missing parent, then, only when h separates uids and
// dir's current owner differs from uid, hands the whole tree to uid.
//
// The caller must ensure no process running as uid modifies the tree during the call. The
// handover itself is post-order (every entry under a directory before the directory itself,
// dir last of all), so no directory is handed to uid until nothing under it remains to
// discover: a directory this call has not yet chowned is still root-owned, and Lchown never
// follows a final-component symlink, so a process already running as uid cannot redirect a
// later step by swapping a directory this call has not reached yet for a symlink.
func (h *Host) PrepareDir(dir string, uid int) error {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("workerproc: prepare dir %s: %w", dir, err)
	}
	if !h.uids {
		return nil
	}

	info, err := os.Lstat(dir)
	if err != nil {
		return fmt.Errorf("workerproc: prepare dir %s: %w", dir, err)
	}
	st, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return fmt.Errorf("workerproc: prepare dir %s: unsupported stat type", dir)
	}
	if int(st.Uid) == uid {
		return nil
	}

	if err := chownTreePostOrder(dir, uid); err != nil {
		return fmt.Errorf("workerproc: prepare dir %s: %w", dir, err)
	}

	if err := os.Lchown(dir, uid, uid); err != nil {
		return fmt.Errorf("workerproc: prepare dir %s: %w", dir, err)
	}
	return nil
}

// chownTreePostOrder hands every entry strictly under dir to uid, a directory only after
// every entry under it, so os.ReadDir never lists a directory that a uid-owned process could
// already write into. dir itself is left untouched; the caller chowns it last.
func chownTreePostOrder(dir string, uid int) error {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return err
	}

	for _, entry := range entries {
		p := filepath.Join(dir, entry.Name())
		if entry.IsDir() {
			if err := chownTreePostOrder(p, uid); err != nil {
				return err
			}
		}
		if err := os.Lchown(p, uid, uid); err != nil {
			return err
		}
	}
	return nil
}

// SetNonDumpable prevents this process's memory from being ptraced or core-dumped by
// another process with the same uid.
func SetNonDumpable() error {
	_, _, errno := syscall.RawSyscall(syscall.SYS_PRCTL, syscall.PR_SET_DUMPABLE, 0, 0)
	if errno != 0 {
		return fmt.Errorf("workerproc: set non-dumpable: %w", errno)
	}
	return nil
}

// configure applies h's enforcement to cmd and p before it starts: the worker's uid, and,
// when limits are on, its cgroup v2 leaf and its memory and CPU caps.
func (h *Host) configure(cmd *exec.Cmd, s Spec, p *Process) (release func(), err error) {
	release = func() {}

	if h.uids {
		if s.UID <= 0 {
			return release, fmt.Errorf("workerproc: configure: UID must be positive when uids are separated, got %d", s.UID)
		}
		cmd.SysProcAttr.Credential = &syscall.Credential{Uid: uint32(s.UID), Gid: uint32(s.UID), Groups: []uint32{}}
	}

	if !h.limits {
		return release, nil
	}

	if s.Name == "" || s.Name == "." || s.Name == ".." || s.Name == "supervisor" || filepath.Base(s.Name) != s.Name {
		return release, fmt.Errorf("workerproc: configure: invalid cgroup leaf name %q", s.Name)
	}
	if s.Limits.MemoryBytes <= 0 {
		return release, fmt.Errorf("workerproc: configure: MemoryBytes must be positive, got %d", s.Limits.MemoryBytes)
	}
	if s.Limits.CPUs <= 0 {
		return release, fmt.Errorf("workerproc: configure: CPUs must be positive, got %v", s.Limits.CPUs)
	}

	// A leaf is removed and recreated on every start, rather than reused empty: on some
	// kernels (observed on Docker Desktop's linuxkit kernel), once cgroup.kill has been
	// written to a leaf, every later process cloned into it via UseCgroupFD is SIGKILLed at
	// fork, even though the leaf reports empty in the meantime. killLeafCgroup writes
	// cgroup.kill to the leaf after every exit, so a reused leaf would kill every worker
	// started in it after the first. EBUSY means the kernel refused the removal because a
	// member is still there, the same "still has members from a previous run" case an empty
	// leaf's occupancy would have caught.
	leaf := filepath.Join(h.cgroup, s.Name)
	switch err := os.Remove(leaf); {
	case err == nil, errors.Is(err, os.ErrNotExist):
	case errors.Is(err, syscall.EBUSY):
		return release, fmt.Errorf("workerproc: configure: leaf %q still has members from a previous run", s.Name)
	default:
		return release, fmt.Errorf("workerproc: configure: remove leaf: %w", err)
	}
	if err := os.Mkdir(leaf, 0o755); err != nil {
		return release, fmt.Errorf("workerproc: configure: mkdir leaf: %w", err)
	}

	if err := writeCgroupFile(filepath.Join(leaf, "memory.max"), strconv.FormatInt(s.Limits.MemoryBytes, 10)); err != nil {
		return release, fmt.Errorf("workerproc: configure: memory.max: %w", err)
	}

	if _, err := os.Stat(filepath.Join(leaf, "memory.swap.max")); err == nil {
		if err := writeCgroupFile(filepath.Join(leaf, "memory.swap.max"), "0"); err != nil {
			return release, fmt.Errorf("workerproc: configure: memory.swap.max: %w", err)
		}
	}

	if err := writeCgroupFile(filepath.Join(leaf, "memory.oom.group"), "1"); err != nil {
		return release, fmt.Errorf("workerproc: configure: memory.oom.group: %w", err)
	}

	quota := int64(math.Round(s.Limits.CPUs * 100000))
	if err := writeCgroupFile(filepath.Join(leaf, "cpu.max"), fmt.Sprintf("%d 100000", quota)); err != nil {
		return release, fmt.Errorf("workerproc: configure: cpu.max: %w", err)
	}

	oomBase, err := readOOMKill(leaf)
	if err != nil {
		return release, fmt.Errorf("workerproc: configure: memory.events: %w", err)
	}
	p.leaf = leaf
	p.oomBase = oomBase

	f, err := os.Open(leaf)
	if err != nil {
		return release, fmt.Errorf("workerproc: configure: open leaf: %w", err)
	}
	cmd.SysProcAttr.UseCgroupFD = true
	cmd.SysProcAttr.CgroupFD = int(f.Fd())

	return func() { f.Close() }, nil
}

// killLeafCgroup kills every process left in p's cgroup leaf, including a descendant the
// worker moved out of its own process group, and reports whether it did anything. It returns
// false when limits are off or the kernel predates cgroup.kill (Linux 5.14), so the caller
// falls back to a process-group SIGKILL.
func (p *Process) killLeafCgroup() bool {
	if p.leaf == "" {
		return false
	}
	return writeCgroupFile(filepath.Join(p.leaf, "cgroup.kill"), "1") == nil
}

// oomKilled reports whether p's cgroup leaf recorded a new oom_kill since it started. A read
// error, including one on a platform with limits off, reports false.
func (p *Process) oomKilled() bool {
	if p.leaf == "" {
		return false
	}
	count, err := readOOMKill(p.leaf)
	if err != nil {
		return false
	}
	return count > p.oomBase
}
