package codec

import (
	"fmt"

	"github.com/zenon-network/go-zenon/common/types"
)

// Event is one execution log entry (§27.3). Events are execution metadata, never state.
type Event struct {
	ContractID types.Hash
	InputIndex uint64
	EventIndex uint32
	Topics     []types.Hash
	Data       []byte
}

// Encode returns the canonical encoding of e.
func (e Event) Encode() ([]byte, error) {
	w := &writer{}
	if err := encodeEventBody(w, e); err != nil {
		return nil, fmt.Errorf("codec: encode event: %w", err)
	}
	return w.buf, nil
}

// DecodeEvent decodes a standalone canonical Event. It applies no index-consistency check —
// that rule lives at the enclosing blob boundary (ContractEffects / ExecutionResult).
func DecodeEvent(b []byte) (Event, error) {
	r := newReader(b)
	e, err := decodeEventBody(r)
	if err != nil {
		return e, fmt.Errorf("codec: decode event: %w", err)
	}
	if err := r.end(); err != nil {
		return e, fmt.Errorf("codec: decode event: %w", err)
	}
	return e, nil
}

func encodeEventBody(w *writer, e Event) error {
	if len(e.Topics) > MaxTopicsPerEvent {
		return ErrLimitExceeded
	}
	if len(e.Data) > MaxEventDataSize {
		return ErrLimitExceeded
	}
	w.hash(e.ContractID)
	w.u64(e.InputIndex)
	w.u32(e.EventIndex)
	w.u32(uint32(len(e.Topics)))
	for _, topic := range e.Topics {
		w.hash(topic)
	}
	w.bytes(e.Data)
	return nil
}

func decodeEventBody(r *reader) (Event, error) {
	var e Event
	var err error

	if e.ContractID, err = r.hash(); err != nil {
		return e, err
	}
	if e.InputIndex, err = r.u64(); err != nil {
		return e, err
	}
	if e.EventIndex, err = r.u32(); err != nil {
		return e, err
	}

	count, err := r.count(32)
	if err != nil {
		return e, err
	}
	if count > MaxTopicsPerEvent {
		return e, ErrLimitExceeded
	}
	topics := make([]types.Hash, 0, count)
	for i := uint32(0); i < count; i++ {
		h, err := r.hash()
		if err != nil {
			return e, err
		}
		topics = append(topics, h)
	}
	e.Topics = topics

	if e.Data, err = r.bytes(MaxEventDataSize); err != nil {
		return e, err
	}
	return e, nil
}

// writeEventList encodes a List<Event> and enforces the §27.3 index rules shared by
// ContractEffects and ExecutionResult: count ≤ MaxEventsPerExecution, ascending EventIndex
// from 0 with no gaps, and InputIndex == globalInputIndex.
func writeEventList(w *writer, events []Event, globalInputIndex uint64) error {
	if len(events) > MaxEventsPerExecution {
		return ErrLimitExceeded
	}
	w.u32(uint32(len(events)))
	for i, e := range events {
		if e.EventIndex != uint32(i) {
			return ErrIndexGap
		}
		if e.InputIndex != globalInputIndex {
			return ErrIndexMismatch
		}
		if err := encodeEventBody(w, e); err != nil {
			return err
		}
	}
	return nil
}

// minEventEncodedSize is the smallest possible encoded Event: contract_id(32) +
// input_index(8) + event_index(4) + topics count(4) + data length(4).
const minEventEncodedSize = 32 + 8 + 4 + 4 + 4

func readEventList(r *reader, globalInputIndex uint64) ([]Event, error) {
	count, err := r.count(minEventEncodedSize)
	if err != nil {
		return nil, err
	}
	if count > MaxEventsPerExecution {
		return nil, ErrLimitExceeded
	}
	events := make([]Event, 0, count)
	for i := uint32(0); i < count; i++ {
		e, err := decodeEventBody(r)
		if err != nil {
			return nil, err
		}
		if e.EventIndex != i {
			return nil, ErrIndexGap
		}
		if e.InputIndex != globalInputIndex {
			return nil, ErrIndexMismatch
		}
		events = append(events, e)
	}
	return events, nil
}
