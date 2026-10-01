package ipc

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"testing"
)

func header(length uint32) []byte {
	b := make([]byte, 4)
	binary.BigEndian.PutUint32(b, length)
	return b
}

func TestReadFrameHeaderErrors(t *testing.T) {
	t.Run("zero bytes is EOF", func(t *testing.T) {
		_, _, err := readFrame(bytes.NewReader(nil), MaxFrame)
		if !errors.Is(err, io.EOF) {
			t.Fatalf("err = %v, want io.EOF", err)
		}
	})
	t.Run("short header is unexpected EOF", func(t *testing.T) {
		_, _, err := readFrame(bytes.NewReader([]byte{0x00, 0x01}), MaxFrame)
		if !errors.Is(err, io.ErrUnexpectedEOF) {
			t.Fatalf("err = %v, want io.ErrUnexpectedEOF", err)
		}
	})
	t.Run("full header, short body is unexpected EOF", func(t *testing.T) {
		buf := append(header(5), 0x01, 0x02)
		_, _, err := readFrame(bytes.NewReader(buf), MaxFrame)
		if !errors.Is(err, io.ErrUnexpectedEOF) {
			t.Fatalf("err = %v, want io.ErrUnexpectedEOF", err)
		}
	})
}

func TestReadFrameLengthBounds(t *testing.T) {
	for _, max := range []uint32{MaxFrame, MaxWorkerFrame} {
		t.Run("", func(t *testing.T) {
			_, _, err := readFrame(bytes.NewReader(header(0)), max)
			if !errors.Is(err, ErrMalformed) || errors.Is(err, io.ErrUnexpectedEOF) {
				t.Fatalf("length 0: err = %v, want ErrMalformed, not ErrUnexpectedEOF", err)
			}
			_, _, err = readFrame(bytes.NewReader(header(max+1)), max)
			if !errors.Is(err, ErrMalformed) || errors.Is(err, io.ErrUnexpectedEOF) {
				t.Fatalf("length max+1: err = %v, want ErrMalformed, not ErrUnexpectedEOF", err)
			}
		})
	}
}

func TestReadFrameAcceptsExactCap(t *testing.T) {
	payload := make([]byte, MaxWorkerFrame-1)
	buf := append(header(MaxWorkerFrame), append([]byte{0x81}, payload...)...)
	typ, got, err := readFrame(bytes.NewReader(buf), MaxWorkerFrame)
	if err != nil {
		t.Fatalf("readFrame: %v", err)
	}
	if typ != 0x81 || len(got) != len(payload) {
		t.Fatalf("typ = %#x, len(got) = %d", typ, len(got))
	}
}

func TestAssembleFrameCap(t *testing.T) {
	ok := make([]byte, MaxWorkerFrame-1)
	if _, err := assembleFrame(TypeCommitted, ok, MaxWorkerFrame); err != nil {
		t.Fatalf("assembleFrame at cap-1: %v", err)
	}
	tooBig := make([]byte, MaxWorkerFrame)
	if _, err := assembleFrame(TypeCommitted, tooBig, MaxWorkerFrame); !errors.Is(err, ErrMalformed) {
		t.Fatalf("assembleFrame at cap: err = %v, want ErrMalformed", err)
	}
}
