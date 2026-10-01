// Package workerproc starts and supervises one isolated worker process. It gives each
// worker an explicit environment, descriptors 0 to 3 only, its own session, and, where the
// host allows it, a dedicated uid and cgroup v2 memory and CPU limits.
package workerproc

import (
	"fmt"
	"io"
	"os"
	"os/exec"
	"strconv"
	"syscall"
	"time"
)

// waitDelay bounds how long Wait waits for the stdout/stderr relay goroutines to see EOF
// after the worker itself has exited. Without it, a descendant that inherited the worker's
// stdout or stderr (any child it forked before exiting) can hold those pipes open and block
// Wait forever, which would also stop the post-Wait leftover kill below from ever running.
const waitDelay = time.Second

// Limits caps a worker's resource use. Both fields must be positive when a Host enforces
// limits.
type Limits struct {
	MemoryBytes int64
	CPUs        float64
}

// Spec describes the worker process to start.
type Spec struct {
	Path   string   // worker executable
	Name   string   // cgroup leaf name, "worker-<domainId>"
	UID    int      // used only when the host separates uids
	Env    []string // the complete environment; never inherited
	Conn   *os.File // the worker's channel end; becomes descriptor 3
	Limits Limits
}

// Exit describes how a worker process ended.
type Exit struct {
	Code      int            // -1 when killed by a signal
	Signal    syscall.Signal // 0 unless killed by a signal
	OOMKilled bool           // the worker's cgroup recorded a new oom_kill
}

// Host records what this machine lets workerproc enforce. The zero Host enforces nothing.
type Host struct {
	limits, uids bool
	reason       string // "" when both are on; otherwise why the first one is off
	cgroup       string // absolute delegated cgroup dir whose children are worker leaves; "" unless limits
}

// Enforcement reports what h enforces and, when something is off, why.
func (h *Host) Enforcement() (limits, uids bool, reason string) {
	return h.limits, h.uids, h.reason
}

// Process is a started worker process.
type Process struct {
	cmd     *exec.Cmd
	leaf    string // cgroup leaf dir; "" when limits are off
	oomBase uint64 // memory.events oom_kill before start
}

// stdioRelay wraps a supervisor output stream so os/exec sees a plain io.Writer, not an
// *os.File. exec.Cmd hands an *os.File child straight through as the child's fd; wrapping it
// forces exec to open an internal pipe and copy through it in a goroutine instead, so the
// worker never holds a descriptor onto the supervisor's own stdout or stderr, such as the
// operator's controlling terminal.
type stdioRelay struct {
	io.Writer
}

// Start launches the worker described by s. The caller closes its own copy of s.Conn after
// Start returns; Start never closes it.
func (h *Host) Start(s Spec) (*Process, error) {
	if s.Conn == nil {
		return nil, fmt.Errorf("workerproc: start: no channel end given")
	}

	env := s.Env
	if env == nil {
		env = []string{}
	}

	cmd := &exec.Cmd{
		Path:        s.Path,
		Args:        []string{s.Path},
		Env:         env,
		Dir:         "/",
		Stdout:      stdioRelay{os.Stdout},
		Stderr:      stdioRelay{os.Stderr},
		ExtraFiles:  []*os.File{s.Conn},
		SysProcAttr: &syscall.SysProcAttr{Setsid: true},
		WaitDelay:   waitDelay,
	}

	p := &Process{}
	release, err := h.configure(cmd, s, p)
	if err != nil {
		return nil, err
	}

	startErr := cmd.Start()
	release()
	if startErr != nil {
		return nil, fmt.Errorf("workerproc: start: %w", startErr)
	}

	p.cmd = cmd
	return p, nil
}

// PID returns the worker's process ID.
func (p *Process) PID() int {
	return p.cmd.Process.Pid
}

// Signal sends sig to the worker.
func (p *Process) Signal(sig os.Signal) error {
	return p.cmd.Process.Signal(sig)
}

// Wait blocks until the worker exits and reports how it ended. Call it exactly once.
//
// The worker's exit alone does not make Wait return: cmd.Wait also waits for the
// stdout/stderr relay goroutines to see EOF, which needs every holder of those pipes'
// write ends to close them, including a child the worker forked and left running.
// WaitDelay bounds that wait to waitDelay, so Wait returns promptly regardless.
//
// Afterward, Wait kills any descendant the worker leaves behind: Setsid makes the worker its
// own session and process group leader, so a plain SIGKILL to its pid never reaches a child
// it moved out of that group. On Linux with limits on, it instead kills the whole cgroup
// leaf, which catches a descendant even if it also left the process group.
func (p *Process) Wait() Exit {
	_ = p.cmd.Wait()

	exit := Exit{Code: p.cmd.ProcessState.ExitCode()}
	if ws, ok := p.cmd.ProcessState.Sys().(syscall.WaitStatus); ok && ws.Signaled() {
		exit.Code = -1
		exit.Signal = ws.Signal()
	}
	exit.OOMKilled = p.oomKilled()

	if !p.killLeafCgroup() {
		_ = syscall.Kill(-p.cmd.Process.Pid, syscall.SIGKILL)
	}

	return exit
}

// Socketpair creates a private AF_UNIX, SOCK_STREAM pair for a supervisor-worker channel.
// Both descriptors are marked close-on-exec and left blocking.
func Socketpair() (supervisorEnd, workerEnd *os.File, err error) {
	syscall.ForkLock.RLock()
	defer syscall.ForkLock.RUnlock()

	fds, err := syscall.Socketpair(syscall.AF_UNIX, syscall.SOCK_STREAM, 0)
	if err != nil {
		return nil, nil, fmt.Errorf("workerproc: socketpair: %w", err)
	}
	syscall.CloseOnExec(fds[0])
	syscall.CloseOnExec(fds[1])

	return os.NewFile(uintptr(fds[0]), "workerproc-supervisor"), os.NewFile(uintptr(fds[1]), "workerproc-worker"), nil
}

// SealInheritedFDs marks every open descriptor at or above 3 close-on-exec, so only
// descriptors an exec.Cmd explicitly passes reach a child.
func SealInheritedFDs() error {
	entries, err := os.ReadDir(fdDir)
	if err != nil {
		return fmt.Errorf("workerproc: seal inherited descriptors: %w", err)
	}

	for _, entry := range entries {
		fd, err := strconv.Atoi(entry.Name())
		if err != nil {
			continue
		}
		if fd >= 3 {
			syscall.CloseOnExec(fd)
		}
	}

	return nil
}
