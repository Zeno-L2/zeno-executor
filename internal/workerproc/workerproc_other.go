//go:build !linux

package workerproc

import (
	"os"
	"os/exec"
)

const fdDir = "/dev/fd"

// NewHost reports that this platform enforces neither uids nor cgroup limits.
func NewHost() *Host {
	return &Host{reason: "platform has no per-process limits"}
}

// PrepareDir creates dir and every missing parent. uid is ignored; this platform does not
// separate worker uids.
func (h *Host) PrepareDir(dir string, uid int) error {
	return os.MkdirAll(dir, 0o755)
}

// SetNonDumpable is a no-op on this platform.
func SetNonDumpable() error {
	return nil
}

func (h *Host) configure(cmd *exec.Cmd, s Spec, p *Process) (release func(), err error) {
	return func() {}, nil
}

func (p *Process) oomKilled() bool {
	return false
}

// killLeafCgroup always reports false: this platform has no cgroups, so Wait's caller falls
// back to a process-group SIGKILL for every worker.
func (p *Process) killLeafCgroup() bool {
	return false
}
