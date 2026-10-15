package simipc

import (
	"encoding/binary"
	"fmt"
	"io"
)

// readFrame reads one frame's header and body. Zero bytes read at the
// header returns io.EOF; 1 to 3 bytes returns io.ErrUnexpectedEOF. A
// length field of 0 or above max is rejected with an error wrapping
// ErrMalformed before anything further is allocated or read. A short
// body returns io.ErrUnexpectedEOF. The type byte and payload are
// returned unvalidated; type and payload checks belong to the message
// readers.
func readFrame(r io.Reader, max uint32) (Type, []byte, error) {
	var hdr [4]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			return 0, nil, err
		}
		return 0, nil, fmt.Errorf("simipc: read frame header: %w", err)
	}
	length := binary.BigEndian.Uint32(hdr[:])
	if length == 0 {
		return 0, nil, fmt.Errorf("simipc: frame length is zero: %w", ErrMalformed)
	}
	if length > max {
		return 0, nil, fmt.Errorf("simipc: frame length %d exceeds cap %d: %w", length, max, ErrMalformed)
	}
	body := make([]byte, length)
	if _, err := io.ReadFull(r, body); err != nil {
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			return 0, nil, io.ErrUnexpectedEOF
		}
		return 0, nil, fmt.Errorf("simipc: read frame body: %w", err)
	}
	return Type(body[0]), body[1:], nil
}

// assembleFrame builds a complete frame (header, type, payload),
// rejecting 1+len(payload) > max: the same length field readFrame
// compares, never the 4-byte header.
func assembleFrame(t Type, payload []byte, max uint32) ([]byte, error) {
	if uint64(1+len(payload)) > uint64(max) {
		return nil, fmt.Errorf("simipc: frame length %d exceeds cap %d: %w", 1+len(payload), max, ErrMalformed)
	}
	length := uint32(1 + len(payload))
	out := make([]byte, 0, 4+length)
	out = binary.BigEndian.AppendUint32(out, length)
	out = append(out, byte(t))
	out = append(out, payload...)
	return out, nil
}
