package zenoexec

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	osexec "os/exec"
	"sync"
	"time"
)

// maxChildOutput bounds the result bytes read from one session child.
const maxChildOutput = 64 << 20

// children holds the running session children, so StopSessions can
// kill them when the server exits. running counts the runChild calls
// in progress, and stopping makes runChild refuse to start a child.
var (
	childMu  sync.Mutex
	children = map[*osexec.Cmd]struct{}{}
	stopping bool
	running  sync.WaitGroup
)

// StopSessions kills every running session child, refuses new ones and
// returns once each runChild has removed its temporary directory.
func StopSessions() {
	childMu.Lock()
	stopping = true
	for c := range children {
		_ = c.Process.Kill()
	}
	childMu.Unlock()
	running.Wait()
}

// runChild runs the commands, one per line, in one child process
// started from argv, and returns one result per command. The child's
// state is the session: every command of the request runs in the same
// process. The child is killed when ctx ends, and runChild returns only
// after the child has exited and its temporary directory is removed.
func runChild(ctx context.Context, argv []string, cmds []json.RawMessage) ([]json.RawMessage, error) {
	var in bytes.Buffer
	for _, c := range cmds {
		if err := json.Compact(&in, c); err != nil {
			return nil, err
		}
		in.WriteByte('\n')
	}

	childMu.Lock()
	if stopping {
		childMu.Unlock()
		return nil, errors.New("server is stopping")
	}
	running.Add(1)
	childMu.Unlock()
	defer running.Done()

	dir, err := os.MkdirTemp("", "zeno-exec-req-")
	if err != nil {
		return nil, err
	}
	defer os.RemoveAll(dir)

	var out limitedBuffer
	cmd := osexec.CommandContext(ctx, argv[0], argv[1:]...)
	cmd.Env = append(os.Environ(), "TMPDIR="+dir)
	cmd.Stdin = &in
	cmd.Stdout = &out
	cmd.WaitDelay = time.Second
	if err := cmd.Start(); err != nil {
		return nil, err
	}
	childMu.Lock()
	children[cmd] = struct{}{}
	if stopping {
		_ = cmd.Process.Kill()
	}
	childMu.Unlock()
	err = cmd.Wait()
	childMu.Lock()
	delete(children, cmd)
	childMu.Unlock()
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	if err != nil {
		return nil, fmt.Errorf("session child: %w", err)
	}

	var results []json.RawMessage
	sc := bufio.NewScanner(&out.buf)
	sc.Buffer(make([]byte, 0, 64<<10), maxChildOutput)
	for sc.Scan() {
		line := append([]byte(nil), sc.Bytes()...)
		if !json.Valid(line) {
			return nil, errors.New("session child wrote a result that is not JSON")
		}
		results = append(results, line)
	}
	if sc.Err() != nil || len(results) != len(cmds) {
		return nil, fmt.Errorf("session child wrote %d results for %d commands", len(results), len(cmds))
	}
	return results, nil
}

// limitedBuffer collects at most maxChildOutput bytes and refuses more.
type limitedBuffer struct{ buf bytes.Buffer }

func (b *limitedBuffer) Write(p []byte) (int, error) {
	if b.buf.Len()+len(p) > maxChildOutput {
		return 0, errors.New("session child output too large")
	}
	return b.buf.Write(p)
}
