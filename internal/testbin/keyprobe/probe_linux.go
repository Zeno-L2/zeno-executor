//go:build linux

package main

import (
	"fmt"
	"os"
	"runtime"
	"strconv"
	"strings"
	"syscall"
)

// devIno extracts the (dev, ino) pair Fstat reported, for the fd-identity check: whether a
// descriptor is the same open file as one the test planted, regardless of which number it landed
// at.
func devIno(stat *syscall.Stat_t) (dev, ino uint64) {
	return stat.Dev, stat.Ino
}

// fdLink resolves fd through /proc/self/fd, without opening anything: Readlink reads the entry's
// target directly.
func fdLink(fd int) string {
	link, _ := os.Readlink(fmt.Sprintf("/proc/self/fd/%d", fd))
	return link
}

// selfCaps reports this process's permitted, effective and ambient capability sets from
// /proc/self/status, and the system's Yama ptrace_scope. Capabilities prove the uid switch
// actually dropped privilege; ptrace_scope explains which layer answered a ptrace or mem denial,
// since Yama can refuse those independently of the non-dumpable flag.
func selfCaps() (capPrm, capEff, capAmb, ptraceScope string) {
	if data, err := os.ReadFile("/proc/self/status"); err == nil {
		for _, line := range strings.Split(string(data), "\n") {
			switch {
			case strings.HasPrefix(line, "CapPrm:"):
				capPrm = strings.TrimSpace(strings.TrimPrefix(line, "CapPrm:"))
			case strings.HasPrefix(line, "CapEff:"):
				capEff = strings.TrimSpace(strings.TrimPrefix(line, "CapEff:"))
			case strings.HasPrefix(line, "CapAmb:"):
				capAmb = strings.TrimSpace(strings.TrimPrefix(line, "CapAmb:"))
			}
		}
	}
	if data, err := os.ReadFile("/proc/sys/kernel/yama/ptrace_scope"); err == nil {
		ptraceScope = strings.TrimSpace(string(data))
	}
	return
}

// probe runs the five direct attempts against ppid, the probe's own parent: opening the
// submission key by path, reading and opening two entries under /proc/<ppid>, reopening the
// inherited descriptor through /proc, and attaching with ptrace.
func probe(ppid int) map[string]attempt {
	out := make(map[string]attempt, 5)

	if keyPath != "" {
		out["keyFile"] = attemptOf(tryOpen(keyPath))
	}

	procDir := "/proc/" + strconv.Itoa(ppid)
	out["environ"] = attemptOf(tryReadFile(procDir + "/environ"))
	out["mem"] = attemptOf(tryOpen(procDir + "/mem"))
	out["fd"] = attemptOf(tryOpen(procDir + "/fd/" + strconv.Itoa(inheritedFD)))
	out["ptrace"] = attemptOf(tryPtrace(ppid))

	return out
}

func tryOpen(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	return f.Close()
}

func tryReadFile(path string) error {
	_, err := os.ReadFile(path)
	return err
}

// tryPtrace attempts PTRACE_ATTACH on ppid. On success it waits for the stop and detaches, so a
// live attach never leaves ppid stopped; the attach result, not the wait or detach result, is
// what gets recorded.
func tryPtrace(ppid int) error {
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()

	attachErr := syscall.PtraceAttach(ppid)
	if attachErr == nil {
		var ws syscall.WaitStatus
		_, _ = syscall.Wait4(ppid, &ws, syscall.WALL, nil)
		_ = syscall.PtraceDetach(ppid)
	}
	return attachErr
}
