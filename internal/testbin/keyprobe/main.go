// Command keyprobe is a domain worker stand-in for the key-isolation test: it reports every
// descriptor it inherited, the frames it received before the supervisor's Config, its own
// environment and arguments, and, on Linux, five direct attempts against its parent process.
// It then exits halted so the supervisor never restarts it.
package main

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"syscall"

	"github.com/digitalSloth/go-executor/internal/ipc"
)

// keyPath is set with -ldflags "-X main.keyPath=<path>"; "" skips the keyFile attempt.
var keyPath string

// inheritedFD is the supervisor descriptor at which cmd/executor's test places the file it
// passes down; the fd attempt tries to reopen it through /proc.
const inheritedFD = 4

type attempt struct {
	Errno int    `json:"errno"` // 0 on success; -1 when the error carries no syscall.Errno
	Err   string `json:"err"`   // "" on success
}

// fdEntry identifies one open descriptor the scan found: its number, the (dev, ino) pair its
// Fstat reported, and, on Linux, what os.Readlink("/proc/self/fd/<n>") resolves it to. Readlink
// opens no descriptor, so recording it does not disturb the scan it runs alongside.
type fdEntry struct {
	FD   int    `json:"fd"`
	Dev  uint64 `json:"dev"`
	Ino  uint64 `json:"ino"`
	Link string `json:"link"` // "" off Linux
}

type report struct {
	FDs         []fdEntry          `json:"fds"`      // descriptors 0-1023 that syscall.Fstat accepts
	Frames      [][]byte           `json:"frames"`   // each frame read on descriptor 3, header included
	FrameErr    string             `json:"frameErr"` // "" unless reading stopped before a Config frame
	Env         []string           `json:"env"`
	Args        []string           `json:"args"`
	UID         int                `json:"uid"`
	PPID        int                `json:"ppid"`
	Attempts    map[string]attempt `json:"attempts"`    // nil off Linux; keys keyFile, environ, mem, fd, ptrace
	CapPrm      string             `json:"capPrm"`      // /proc/self/status CapPrm, hex; "" off Linux
	CapEff      string             `json:"capEff"`      // /proc/self/status CapEff, hex; "" off Linux
	CapAmb      string             `json:"capAmb"`      // /proc/self/status CapAmb, hex; "" off Linux
	PtraceScope string             `json:"ptraceScope"` // /proc/sys/kernel/yama/ptrace_scope; "" off Linux or unreadable
}

func main() {
	var fds []fdEntry
	var stat syscall.Stat_t
	for fd := 0; fd < 1024; fd++ {
		if syscall.Fstat(fd, &stat) == nil {
			dev, ino := devIno(&stat)
			fds = append(fds, fdEntry{FD: fd, Dev: dev, Ino: ino, Link: fdLink(fd)})
		}
	}

	frames, ferr := readUntilConfig(os.NewFile(3, "supervisor"))
	rep := report{FDs: fds, Frames: frames}
	if ferr != nil {
		rep.FrameErr = ferr.Error()
	}

	rep.Env = os.Environ()
	rep.Args = os.Args
	rep.UID = os.Getuid()
	ppid := os.Getppid()
	rep.PPID = ppid
	rep.Attempts = probe(ppid)
	rep.CapPrm, rep.CapEff, rep.CapAmb, rep.PtraceScope = selfCaps()

	out, err := json.Marshal(rep)
	if err != nil {
		out = []byte(`{"marshalError":"` + err.Error() + `"}`)
	}
	line := append([]byte("KEYPROBE "), out...)
	line = append(line, '\n')
	os.Stdout.Write(line)

	os.Exit(ipc.ExitHalted)
}

// readUntilConfig reads frames off r: a 4-byte big-endian length, rejecting 0 or above
// ipc.MaxFrame before allocating, then the body. It appends header plus body to the returned
// slice and stops after the first frame whose type byte is ipc.TypeConfig. Any failure, short
// read, EOF or a rejected length, stops reading and is returned as the error, with every frame
// read so far.
func readUntilConfig(r io.Reader) ([][]byte, error) {
	var frames [][]byte
	for {
		var hdr [4]byte
		if _, err := io.ReadFull(r, hdr[:]); err != nil {
			return frames, err
		}
		length := binary.BigEndian.Uint32(hdr[:])
		if length == 0 || length > ipc.MaxFrame {
			return frames, fmt.Errorf("keyprobe: frame length %d out of range", length)
		}
		body := make([]byte, length)
		if _, err := io.ReadFull(r, body); err != nil {
			return frames, err
		}

		frame := make([]byte, 0, len(hdr)+len(body))
		frame = append(frame, hdr[:]...)
		frame = append(frame, body...)
		frames = append(frames, frame)

		if len(body) > 0 && body[0] == byte(ipc.TypeConfig) {
			return frames, nil
		}
	}
}

// attemptOf turns err into an attempt: {0, ""} for nil, the syscall.Errno it wraps when one is
// present, or -1 for anything else.
func attemptOf(err error) attempt {
	if err == nil {
		return attempt{}
	}
	var errno syscall.Errno
	if errors.As(err, &errno) {
		return attempt{Errno: int(errno), Err: err.Error()}
	}
	return attempt{Errno: -1, Err: err.Error()}
}
