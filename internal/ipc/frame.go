// Package ipc is the private channel between the supervisor and one domain worker: a stream of
// frames, u32 length ‖ u8 type ‖ payload, big-endian, where length counts the type byte and payload.
//
// Session rules. The worker sends one request and reads its whole reply before it sends anything
// else. It sends Committed, Retract, Recovered, Withdrawal and
// WithdrawalsEnd frames only between requests, and none gets a reply.
// At boot and after every rollback it sends one Retract, then zero or
// more Recovered frames, each followed by its hand-off, strictly
// ascending by batchId and each below that Retract's FromBatchID,
// before any Committed. The supervisor treats any other order of these
// frames as malformed. A Retract discards every batchId at or above
// FromBatchID. A Committed or Recovered for a batchId the supervisor
// holds must equal the held commitment unless a Retract discarded it
// first.
//   - FrontierRequest is answered by Frontier or by SourceError.
//   - MomentaRequest is answered by (Momentum* RangeEnd) or by SourceError. SourceError is valid only
//     as the first reply frame, so the supervisor must validate and encode a whole range before it
//     sends any of it. RangeEnd.Count equals the number of Momentum frames sent, which is at most the
//     requested count.
//   - AnchorRequest is answered by Anchor or by SourceError.
//   - AssignmentRequest is answered by Assignment or by SourceError. Assignment entry i carries
//     ordinal NextOrdinal+i.
//   - CapRequest is answered by Cap or by SourceError. Cap carries one amount per requested
//     asset, in request order.
//   - CursorRequest is answered by Cursor or by SourceError.
//   - AcceptedRequest is answered by Accepted or by SourceError. Accepted carries at most
//     MaxAcceptedEntries entries, for consecutive batchIds from FromBatchID, each below its
//     NextBatchID, and carries none only when FromBatchID is at or above NextBatchID.
//
// A Committed or Recovered below the highest nextBatchId the supervisor has read is not held.
//
// Hand-off. After each Committed or Recovered for batch B, the worker
// sends B's hand-off before any other frame: one Withdrawal per kind 1
// outbox message of B, each with BatchID B, then WithdrawalsEnd{B}.
//   - B's Withdrawal frames are strictly ascending by the message's
//     (inputIndex, outboxIndex): each frame's pair is greater than the
//     previous frame's, so no message is sent twice.
//   - A batch with no kind 1 message sends WithdrawalsEnd{B} alone.
//     Kind 0 messages are never sent.
//   - A hand-off carries at most MaxHandOffWithdrawals Withdrawal
//     frames, and each Proof places its Message under B's OutboxRoot.
//   - The worker sends the full hand-off after every Committed and
//     every Recovered, whatever nextBatchId it last read.
//   - Withdrawal and WithdrawalsEnd frames are sent only inside a
//     hand-off and get no reply. The supervisor treats either frame
//     outside a hand-off, a wrong BatchID, any other frame inside a
//     hand-off, or more than MaxHandOffWithdrawals Withdrawal frames as
//     malformed. A Withdrawal not after the previous one is a
//     consistency fault.
//
// Any other reply sequence is a channel failure, and so is a Momentum the worker cannot decode or
// whose hashes do not verify: the worker exits 1, a counted failure. Encode does not verify
// momentum hashes, so a bad momentum from the supervisor's source reaches the worker and ends there.
package ipc

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

const (
	MaxFrame       = 64 << 20 // cap on the length field of frames the worker reads (S→W)
	MaxWorkerFrame = 64 << 10 // cap on the length field of frames the supervisor reads (W→S)
)

// ErrMalformed marks a frame or payload that breaks the protocol. EOF and short reads never wrap it.
var ErrMalformed = errors.New("ipc: malformed frame")

// readFrame reads one frame's header and body. Zero bytes read at the header returns io.EOF; 1 to
// 3 bytes returns io.ErrUnexpectedEOF. A length field of 0 or above max is rejected with an error
// wrapping ErrMalformed before anything further is allocated or read. A short body returns
// io.ErrUnexpectedEOF. The type byte and payload are returned unvalidated; type and payload checks
// belong to ReadMessage.
func readFrame(r io.Reader, max uint32) (Type, []byte, error) {
	var hdr [4]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			return 0, nil, err
		}
		return 0, nil, fmt.Errorf("ipc: read frame header: %w", err)
	}
	length := binary.BigEndian.Uint32(hdr[:])
	if length == 0 {
		return 0, nil, fmt.Errorf("ipc: frame length is zero: %w", ErrMalformed)
	}
	if length > max {
		return 0, nil, fmt.Errorf("ipc: frame length %d exceeds cap %d: %w", length, max, ErrMalformed)
	}
	body := make([]byte, length)
	if _, err := io.ReadFull(r, body); err != nil {
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			return 0, nil, io.ErrUnexpectedEOF
		}
		return 0, nil, fmt.Errorf("ipc: read frame body: %w", err)
	}
	return Type(body[0]), body[1:], nil
}

// assembleFrame builds a complete frame (header, type, payload), rejecting 1+len(payload) > max:
// the same length field readFrame compares, never the 4-byte header.
func assembleFrame(t Type, payload []byte, max uint32) ([]byte, error) {
	if uint64(1+len(payload)) > uint64(max) {
		return nil, fmt.Errorf("ipc: frame length %d exceeds cap %d: %w", 1+len(payload), max, ErrMalformed)
	}
	length := uint32(1 + len(payload))
	out := make([]byte, 0, 4+length)
	out = binary.BigEndian.AppendUint32(out, length)
	out = append(out, byte(t))
	out = append(out, payload...)
	return out, nil
}
