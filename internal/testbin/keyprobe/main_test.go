package main

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"syscall"
	"testing"

	"github.com/digitalSloth/go-executor/internal/ipc"
)

// frame builds a length-prefixed frame: a 4-byte big-endian length followed by a type byte and
// body.
func frame(typ byte, body []byte) []byte {
	full := append([]byte{typ}, body...)
	hdr := make([]byte, 4)
	binary.BigEndian.PutUint32(hdr, uint32(len(full)))
	return append(hdr, full...)
}

func TestReadUntilConfig_StopsAtConfigFrame(t *testing.T) {
	momentum := frame(0x03, []byte("m"))
	config := frame(0x01, []byte("c"))
	trailer := frame(0x03, []byte("never read"))

	var buf bytes.Buffer
	buf.Write(momentum)
	buf.Write(config)
	buf.Write(trailer)

	frames, err := readUntilConfig(&buf)
	if err != nil {
		t.Fatalf("readUntilConfig: %v", err)
	}
	if len(frames) != 2 {
		t.Fatalf("len(frames) = %d, want 2", len(frames))
	}
	if !bytes.Equal(frames[0], momentum) {
		t.Errorf("frames[0] = %x, want %x", frames[0], momentum)
	}
	if !bytes.Equal(frames[1], config) {
		t.Errorf("frames[1] = %x, want %x", frames[1], config)
	}
	// The trailer must never be consumed: only unread bytes remain in buf.
	if buf.Len() != len(trailer) {
		t.Errorf("remaining unread bytes = %d, want %d (trailer left untouched)", buf.Len(), len(trailer))
	}
}

func TestReadUntilConfig_ZeroLength(t *testing.T) {
	hdr := make([]byte, 4)
	binary.BigEndian.PutUint32(hdr, 0)
	r := bytes.NewReader(hdr)

	frames, err := readUntilConfig(r)
	if err == nil {
		t.Fatal("readUntilConfig: want error for zero-length frame, got nil")
	}
	if len(frames) != 0 {
		t.Errorf("frames = %v, want none", frames)
	}
}

func TestReadUntilConfig_OversizedLength(t *testing.T) {
	hdr := make([]byte, 4)
	binary.BigEndian.PutUint32(hdr, uint32(ipc.MaxFrame)+1)
	r := bytes.NewReader(hdr)

	frames, err := readUntilConfig(r)
	if err == nil {
		t.Fatal("readUntilConfig: want error for oversized frame length, got nil")
	}
	if len(frames) != 0 {
		t.Errorf("frames = %v, want none", frames)
	}
	// The body must never be allocated or read: the reader has nothing left but the header it
	// already consumed.
	if r.Len() != 0 {
		t.Errorf("reader has %d unread bytes left; body should not have been attempted", r.Len())
	}
}

func TestReadUntilConfig_EOFBeforeConfig(t *testing.T) {
	momentum := frame(0x03, []byte("m"))
	r := bytes.NewReader(momentum)

	frames, err := readUntilConfig(r)
	if err == nil {
		t.Fatal("readUntilConfig: want error for EOF before a Config frame, got nil")
	}
	if !errors.Is(err, io.EOF) && !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Errorf("err = %v, want io.EOF or io.ErrUnexpectedEOF", err)
	}
	if len(frames) != 1 {
		t.Fatalf("frames = %v, want the one frame read before EOF", frames)
	}
	if !bytes.Equal(frames[0], momentum) {
		t.Errorf("frames[0] = %x, want %x", frames[0], momentum)
	}
}

func TestReadUntilConfig_TruncatedBody(t *testing.T) {
	hdr := make([]byte, 4)
	binary.BigEndian.PutUint32(hdr, 10)
	var buf bytes.Buffer
	buf.Write(hdr)
	buf.Write([]byte{0x03, 'a', 'b'}) // 3 bytes of a promised 10-byte body

	frames, err := readUntilConfig(&buf)
	if err == nil {
		t.Fatal("readUntilConfig: want error for a truncated body, got nil")
	}
	if len(frames) != 0 {
		t.Errorf("frames = %v, want none", frames)
	}
}

func TestReadUntilConfig_ImmediateEOF(t *testing.T) {
	frames, err := readUntilConfig(bytes.NewReader(nil))
	if err == nil {
		t.Fatal("readUntilConfig: want error for immediate EOF, got nil")
	}
	if !errors.Is(err, io.EOF) {
		t.Errorf("err = %v, want io.EOF", err)
	}
	if len(frames) != 0 {
		t.Errorf("frames = %v, want none", frames)
	}
}

func TestAttemptOf(t *testing.T) {
	if got := attemptOf(nil); got.Errno != 0 || got.Err != "" {
		t.Errorf("attemptOf(nil) = %+v, want zero value", got)
	}

	errnoErr := &pathErrnoErr{errno: syscall.EACCES}
	got := attemptOf(errnoErr)
	if got.Errno != int(syscall.EACCES) {
		t.Errorf("attemptOf(EACCES) Errno = %d, want %d", got.Errno, int(syscall.EACCES))
	}
	if got.Err == "" {
		t.Error("attemptOf(EACCES) Err is empty, want the wrapped error's message")
	}

	other := errors.New("boom")
	got = attemptOf(other)
	if got.Errno != -1 {
		t.Errorf("attemptOf(plain error) Errno = %d, want -1", got.Errno)
	}
	if got.Err != "boom" {
		t.Errorf("attemptOf(plain error) Err = %q, want %q", got.Err, "boom")
	}
}

// pathErrnoErr wraps a syscall.Errno the way *os.PathError does, so errors.As finds it.
type pathErrnoErr struct{ errno syscall.Errno }

func (e *pathErrnoErr) Error() string { return "path: " + e.errno.Error() }
func (e *pathErrnoErr) Unwrap() error { return e.errno }
