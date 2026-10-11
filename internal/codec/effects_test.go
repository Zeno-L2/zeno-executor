package codec

import (
	"bytes"
	"encoding/binary"
	"errors"
	"testing"

	"github.com/zenon-network/go-zenon/common/crypto"
	"github.com/zenon-network/go-zenon/common/types"
)

func fillHash(b byte) types.Hash {
	var h types.Hash
	for i := range h {
		h[i] = b
	}
	return h
}

func fillPath(b byte) (p [32]byte) {
	for i := range p {
		p[i] = b
	}
	return p
}

func sampleFrame() ExecutionInputFrame {
	return ExecutionInputFrame{
		ContextVersion: RuntimeProfileVersion,
		ChainID:        fillHash(0x01),
		Caller:         fillHash(0x02),
		ContractID:     fillHash(0x03),
		CodeHash:       fillHash(0x04),
		MomentumHeight: 100,
		MomentumTime:   200,
		InputIndex:     7,
		DepositAsset:   AssetIDFromZTS(types.ZnnTokenStandard),
		DepositAmount:  U256{31: 42},
		Payload:        []byte("payload"),
	}
}

func TestExecutionInputFrameRoundTrip(t *testing.T) {
	f := sampleFrame()
	b, err := f.Encode()
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	got, err := DecodeExecutionInputFrame(b)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if got.ContextVersion != f.ContextVersion || got.ChainID != f.ChainID || got.Caller != f.Caller ||
		got.ContractID != f.ContractID || got.CodeHash != f.CodeHash || got.MomentumHeight != f.MomentumHeight ||
		got.MomentumTime != f.MomentumTime || got.InputIndex != f.InputIndex || got.DepositAsset != f.DepositAsset ||
		got.DepositAmount != f.DepositAmount || !bytes.Equal(got.Payload, f.Payload) {
		t.Errorf("got %+v, want %+v", got, f)
	}
}

func TestExecutionInputFrameVersionRejectedFirst(t *testing.T) {
	f := sampleFrame()
	f.ContextVersion = RuntimeProfileVersion + 1
	b, err := f.Encode()
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	// Corrupt the remainder of the buffer with garbage; the version check must fire before
	// any other byte is interpreted.
	for i := 2; i < len(b); i++ {
		b[i] = 0xFF
	}
	if _, err := DecodeExecutionInputFrame(b); !errors.Is(err, ErrUnknownVersion) {
		t.Fatalf("got %v, want ErrUnknownVersion", err)
	}
}

func TestExecutionInputFrameTrailingBytes(t *testing.T) {
	f := sampleFrame()
	b, err := f.Encode()
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	b = append(b, 0x00)
	if _, err := DecodeExecutionInputFrame(b); !errors.Is(err, ErrTrailingBytes) {
		t.Fatalf("got %v, want ErrTrailingBytes", err)
	}
}

func sampleEvent(inputIndex uint64, eventIndex uint32) Event {
	return Event{
		ContractID: fillHash(0x10),
		InputIndex: inputIndex,
		EventIndex: eventIndex,
		Topics:     []types.Hash{fillHash(0x20), fillHash(0x21)},
		Data:       []byte("event-data"),
	}
}

func TestEventRoundTrip(t *testing.T) {
	e := sampleEvent(5, 0)
	b, err := e.Encode()
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	got, err := DecodeEvent(b)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if got.ContractID != e.ContractID || got.InputIndex != e.InputIndex || got.EventIndex != e.EventIndex ||
		!bytes.Equal(got.Data, e.Data) || len(got.Topics) != len(e.Topics) {
		t.Errorf("got %+v, want %+v", got, e)
	}
}

func TestEventOverCapRejected(t *testing.T) {
	t.Run("topics", func(t *testing.T) {
		e := sampleEvent(0, 0)
		e.Topics = make([]types.Hash, MaxTopicsPerEvent+1)
		if _, err := e.Encode(); !errors.Is(err, ErrLimitExceeded) {
			t.Fatalf("got %v, want ErrLimitExceeded", err)
		}
	})
	t.Run("data", func(t *testing.T) {
		e := sampleEvent(0, 0)
		e.Data = make([]byte, MaxEventDataSize+1)
		if _, err := e.Encode(); !errors.Is(err, ErrLimitExceeded) {
			t.Fatalf("got %v, want ErrLimitExceeded", err)
		}
	})
}

func TestEventTrailingBytes(t *testing.T) {
	e := sampleEvent(0, 0)
	b, err := e.Encode()
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	b = append(b, 0x00)
	if _, err := DecodeEvent(b); !errors.Is(err, ErrTrailingBytes) {
		t.Fatalf("got %v, want ErrTrailingBytes", err)
	}
}

func sampleL2Delivery(inputIndex uint64, outboxIndex uint32) OutboxMessage {
	return OutboxMessage{
		SourceContract: fillHash(0x30),
		InputIndex:     inputIndex,
		OutboxIndex:    outboxIndex,
		Target:         L2Delivery{TargetContract: fillHash(0x40)},
		Payload:        []byte("msg"),
	}
}

func sampleL1Withdrawal(inputIndex uint64, outboxIndex uint32) OutboxMessage {
	return OutboxMessage{
		SourceContract: fillHash(0x31),
		InputIndex:     inputIndex,
		OutboxIndex:    outboxIndex,
		Target: L1Withdrawal{
			RecipientL1: types.Address{1, 2, 3},
			Asset:       AssetIDFromZTS(types.ZnnTokenStandard),
			Amount:      U256{31: 9},
		},
		Payload: nil,
	}
}

func TestOutboxMessageRoundTrip(t *testing.T) {
	for _, m := range []OutboxMessage{sampleL2Delivery(1, 0), sampleL1Withdrawal(2, 1)} {
		b, err := m.Encode()
		if err != nil {
			t.Fatalf("Encode: %v", err)
		}
		got, err := DecodeOutboxMessage(b)
		if err != nil {
			t.Fatalf("Decode: %v", err)
		}
		if got.SourceContract != m.SourceContract || got.InputIndex != m.InputIndex ||
			got.OutboxIndex != m.OutboxIndex || got.Target.Kind() != m.Target.Kind() {
			t.Errorf("got %+v, want %+v", got, m)
		}
	}
}

func TestOutboxMessageInvalidKind(t *testing.T) {
	m := sampleL2Delivery(0, 0)
	b, err := m.Encode()
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	b[44] = 2 // kind byte: source_contract(32) + input_index(8) + outbox_index(4) = offset 44
	if _, err := DecodeOutboxMessage(b); !errors.Is(err, ErrInvalidKind) {
		t.Fatalf("got %v, want ErrInvalidKind", err)
	}
}

func TestOutboxMessageWithdrawalPayloadRejected(t *testing.T) {
	m := sampleL1Withdrawal(0, 0)
	m.Payload = []byte{0x01}
	if _, err := m.Encode(); !errors.Is(err, ErrNonEmptyWithdrawalPayload) {
		t.Fatalf("Encode: got %v, want ErrNonEmptyWithdrawalPayload", err)
	}

	// Craft the decode-side rejection directly, since Encode already refuses it.
	good := sampleL1Withdrawal(0, 0)
	b, err := good.Encode()
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	b = append(b[:len(b)-4], 0, 0, 0, 1, 0xFF) // rewrite the zero-length payload as one byte
	if _, err := DecodeOutboxMessage(b); !errors.Is(err, ErrNonEmptyWithdrawalPayload) {
		t.Fatalf("Decode: got %v, want ErrNonEmptyWithdrawalPayload", err)
	}
}

func TestOutboxMessageNilTarget(t *testing.T) {
	m := OutboxMessage{SourceContract: fillHash(0x01)}
	if _, err := m.Encode(); !errors.Is(err, ErrInvalidKind) {
		t.Fatalf("got %v, want ErrInvalidKind", err)
	}
}

func TestOutboxIDDeterministic(t *testing.T) {
	a := OutboxID(1, 2, 3, 4)
	b := OutboxID(1, 2, 3, 4)
	if a != b {
		t.Fatalf("OutboxID not deterministic: %x != %x", a, b)
	}
	c := OutboxID(1, 2, 3, 5)
	if a == c {
		t.Fatalf("OutboxID collided across different outbox_index")
	}
}

// TestOutboxIDPreimageFormat independently re-derives the §17.3 preimage bytes
// (domainId:u32 || batchId:u64 || inputIndex:u64 || outboxIndex:u32) with plain
// encoding/binary calls, never via the codec's writer, to pin the field widths and
// order (domainId as u32).
func TestOutboxIDPreimageFormat(t *testing.T) {
	domainID := uint32(7)
	batchID := uint64(1234)
	inputIndex := uint64(5678)
	outboxIndex := uint32(9)

	var preimage []byte
	b4 := make([]byte, 4)
	b8 := make([]byte, 8)
	binary.BigEndian.PutUint32(b4, domainID)
	preimage = append(preimage, b4...)
	binary.BigEndian.PutUint64(b8, batchID)
	preimage = append(preimage, b8...)
	binary.BigEndian.PutUint64(b8, inputIndex)
	preimage = append(preimage, b8...)
	binary.BigEndian.PutUint32(b4, outboxIndex)
	preimage = append(preimage, b4...)

	want := crypto.Hash(preimage)
	got := OutboxID(domainID, batchID, inputIndex, outboxIndex)
	if !bytes.Equal(got[:], want) {
		t.Fatalf("OutboxID preimage mismatch: got %x, want SHA3-256(%x) = %x", got, preimage, want)
	}
}

func sampleContractEffects(globalInputIndex uint64) ContractEffects {
	return ContractEffects{
		Version:          ExecutionResultVersion,
		GlobalInputIndex: globalInputIndex,
		StateDiff: []LocalStateDiffEntry{
			{LocalKey: []byte("k2"), Value: []byte("v2")},
			{LocalKey: []byte("k1"), Value: nil},
		},
		Events:         []Event{sampleEvent(globalInputIndex, 0)},
		Outbox:         []OutboxMessage{sampleL2Delivery(globalInputIndex, 0)},
		ReturnData:     []byte("return"),
		ClaimedDeposit: U256{31: 5},
	}
}

func TestContractEffectsRoundTrip(t *testing.T) {
	c := sampleContractEffects(3)
	b, err := c.Encode()
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	got, err := DecodeContractEffects(b)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if got.Version != c.Version || got.GlobalInputIndex != c.GlobalInputIndex ||
		len(got.StateDiff) != len(c.StateDiff) || !bytes.Equal(got.ReturnData, c.ReturnData) ||
		got.ClaimedDeposit != c.ClaimedDeposit {
		t.Errorf("got %+v, want %+v", got, c)
	}
	if got.StateDiff[0].Value == nil || got.StateDiff[1].Value != nil {
		t.Errorf("local StateDiff order/nil-ness not preserved: %+v", got.StateDiff)
	}
}

func TestContractEffectsVersionMismatchReadsNoFurtherField(t *testing.T) {
	c := sampleContractEffects(0)
	c.Version = ExecutionResultVersion + 1
	// Encode manually since c.Encode() would itself reject the bad version.
	w := &writer{}
	w.u16(c.Version)
	w.raw([]byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF}) // garbage globalInputIndex
	if _, err := DecodeContractEffects(w.buf); !errors.Is(err, ErrUnknownVersion) {
		t.Fatalf("got %v, want ErrUnknownVersion", err)
	}
}

func TestContractEffectsTruncatedClaimedDeposit(t *testing.T) {
	c := sampleContractEffects(0)
	b, err := c.Encode()
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	truncated := b[:len(b)-1]
	if _, err := DecodeContractEffects(truncated); !errors.Is(err, ErrTruncated) {
		t.Fatalf("got %v, want ErrTruncated", err)
	}
}

func TestStateValuePresentEmptyVsEmptyVsReserved(t *testing.T) {
	t.Run("present-empty", func(t *testing.T) {
		w := &writer{}
		if err := writeStateValue(w, []byte{}); err != nil {
			t.Fatalf("writeStateValue: %v", err)
		}
		r := newReader(w.buf)
		v, err := readStateValue(r)
		if err != nil {
			t.Fatalf("readStateValue: %v", err)
		}
		if v == nil || len(v) != 0 {
			t.Fatalf("got %v, want non-nil empty", v)
		}
	})

	t.Run("EMPTY sentinel decodes nil", func(t *testing.T) {
		w := &writer{}
		if err := writeStateValue(w, nil); err != nil {
			t.Fatalf("writeStateValue: %v", err)
		}
		r := newReader(w.buf)
		v, err := readStateValue(r)
		if err != nil {
			t.Fatalf("readStateValue: %v", err)
		}
		if v != nil {
			t.Fatalf("got %v, want nil", v)
		}
	})

	t.Run("reserved length rejected", func(t *testing.T) {
		w := &writer{}
		w.u32(MaxStateValueSize + 1)
		r := newReader(w.buf)
		if _, err := readStateValue(r); !errors.Is(err, ErrReservedLength) {
			t.Fatalf("got %v, want ErrReservedLength", err)
		}
	})

	t.Run("reserved length just below sentinel rejected", func(t *testing.T) {
		w := &writer{}
		w.u32(0xFFFFFFFE)
		r := newReader(w.buf)
		if _, err := readStateValue(r); !errors.Is(err, ErrReservedLength) {
			t.Fatalf("got %v, want ErrReservedLength", err)
		}
	})

	t.Run("encode rejects over-limit value", func(t *testing.T) {
		w := &writer{}
		if err := writeStateValue(w, make([]byte, MaxStateValueSize+1)); !errors.Is(err, ErrReservedLength) {
			t.Fatalf("got %v, want ErrReservedLength", err)
		}
	})
}

func TestDerivedStateDiffUnsortedAndDuplicate(t *testing.T) {
	pathOf := func(b byte) (p [32]byte) {
		return fillPath(b)
	}

	t.Run("unsorted rejected", func(t *testing.T) {
		entries := []StateDiffEntry{{Key: pathOf(2), Value: []byte{1}}, {Key: pathOf(1), Value: []byte{2}}}
		w := &writer{}
		if err := writeDerivedStateDiffList(w, entries); !errors.Is(err, ErrNotSorted) {
			t.Fatalf("got %v, want ErrNotSorted", err)
		}
	})

	t.Run("duplicate rejected", func(t *testing.T) {
		entries := []StateDiffEntry{{Key: pathOf(1), Value: []byte{1}}, {Key: pathOf(1), Value: []byte{2}}}
		w := &writer{}
		if err := writeDerivedStateDiffList(w, entries); !errors.Is(err, ErrDuplicateKey) {
			t.Fatalf("got %v, want ErrDuplicateKey", err)
		}
	})

	t.Run("decode side rejects unsorted", func(t *testing.T) {
		w := &writer{}
		w.u32(2)
		_ = encodeStateDiffEntry(w, StateDiffEntry{Key: pathOf(2), Value: []byte{}})
		_ = encodeStateDiffEntry(w, StateDiffEntry{Key: pathOf(1), Value: []byte{}})
		r := newReader(w.buf)
		if _, err := readDerivedStateDiffList(r); !errors.Is(err, ErrNotSorted) {
			t.Fatalf("got %v, want ErrNotSorted", err)
		}
	})
}

func sampleExecutionResult(globalInputIndex uint64) ExecutionResult {
	return ExecutionResult{
		Version:          ExecutionResultVersion,
		GlobalInputIndex: globalInputIndex,
		PreStateRoot:     fillHash(0x50),
		PostStateRoot:    fillHash(0x51),
		StateDiff: []StateDiffEntry{
			{Key: fillPath(0x01), Value: []byte("a")},
			{Key: fillPath(0x02), Value: nil},
		},
		Events:         []Event{sampleEvent(globalInputIndex, 0)},
		Outbox:         []OutboxMessage{sampleL2Delivery(globalInputIndex, 0)},
		ReturnData:     []byte("ret"),
		ClaimedDeposit: U256{31: 3},
	}
}

func TestExecutionResultRoundTrip(t *testing.T) {
	e := sampleExecutionResult(9)
	b, err := e.Encode()
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	got, err := DecodeExecutionResult(b)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if got.Version != e.Version || got.GlobalInputIndex != e.GlobalInputIndex ||
		got.PreStateRoot != e.PreStateRoot || got.PostStateRoot != e.PostStateRoot ||
		len(got.StateDiff) != len(e.StateDiff) || got.ClaimedDeposit != e.ClaimedDeposit {
		t.Errorf("got %+v, want %+v", got, e)
	}
}

func TestEventAndOutboxIndexMismatch(t *testing.T) {
	t.Run("event index gap", func(t *testing.T) {
		e := sampleExecutionResult(1)
		e.Events = []Event{sampleEvent(1, 1)} // must start at 0
		if _, err := e.Encode(); !errors.Is(err, ErrIndexGap) {
			t.Fatalf("got %v, want ErrIndexGap", err)
		}
	})

	t.Run("event input index mismatch", func(t *testing.T) {
		e := sampleExecutionResult(1)
		e.Events = []Event{sampleEvent(2, 0)} // disagrees with GlobalInputIndex
		if _, err := e.Encode(); !errors.Is(err, ErrIndexMismatch) {
			t.Fatalf("got %v, want ErrIndexMismatch", err)
		}
	})

	t.Run("outbox index gap", func(t *testing.T) {
		e := sampleExecutionResult(1)
		e.Outbox = []OutboxMessage{sampleL2Delivery(1, 1)}
		if _, err := e.Encode(); !errors.Is(err, ErrIndexGap) {
			t.Fatalf("got %v, want ErrIndexGap", err)
		}
	})

	t.Run("outbox input index mismatch", func(t *testing.T) {
		e := sampleExecutionResult(1)
		e.Outbox = []OutboxMessage{sampleL2Delivery(2, 0)}
		if _, err := e.Encode(); !errors.Is(err, ErrIndexMismatch) {
			t.Fatalf("got %v, want ErrIndexMismatch", err)
		}
	})

	t.Run("decode side rejects event input index mismatch", func(t *testing.T) {
		b := rawExecutionResult(1, []Event{sampleEvent(2, 0)}, nil)
		if _, err := DecodeExecutionResult(b); !errors.Is(err, ErrIndexMismatch) {
			t.Fatalf("got %v, want ErrIndexMismatch", err)
		}
	})

	t.Run("decode side rejects event index gap", func(t *testing.T) {
		b := rawExecutionResult(1, []Event{sampleEvent(1, 1)}, nil)
		if _, err := DecodeExecutionResult(b); !errors.Is(err, ErrIndexGap) {
			t.Fatalf("got %v, want ErrIndexGap", err)
		}
	})

	t.Run("decode side rejects outbox input index mismatch", func(t *testing.T) {
		b := rawExecutionResult(1, nil, []OutboxMessage{sampleL2Delivery(2, 0)})
		if _, err := DecodeExecutionResult(b); !errors.Is(err, ErrIndexMismatch) {
			t.Fatalf("got %v, want ErrIndexMismatch", err)
		}
	})

	t.Run("decode side rejects outbox index gap", func(t *testing.T) {
		b := rawExecutionResult(1, nil, []OutboxMessage{sampleL2Delivery(1, 1)})
		if _, err := DecodeExecutionResult(b); !errors.Is(err, ErrIndexGap) {
			t.Fatalf("got %v, want ErrIndexGap", err)
		}
	})
}

// rawExecutionResult hand-builds a canonical ExecutionResult encoding with the given events
// and outbox messages written verbatim via encodeEventBody/encodeOutboxMessageBody, bypassing
// writeEventList/writeOutboxList's index checks — Encode refuses to emit such bytes, so this
// is the only way to construct them for a decode-side rejection test.
func rawExecutionResult(globalInputIndex uint64, events []Event, outbox []OutboxMessage) []byte {
	w := &writer{}
	w.u16(ExecutionResultVersion)
	w.u64(globalInputIndex)
	w.hash(fillHash(0x50))
	w.hash(fillHash(0x51))
	w.u32(0) // empty derived state diff list

	w.u32(uint32(len(events)))
	for _, e := range events {
		if err := encodeEventBody(w, e); err != nil {
			panic(err)
		}
	}

	w.u32(uint32(len(outbox)))
	for _, m := range outbox {
		if err := encodeOutboxMessageBody(w, m); err != nil {
			panic(err)
		}
	}

	w.bytes([]byte("ret"))
	w.u256(U256{31: 3})
	return w.buf
}

func TestEventListOverCapRejected(t *testing.T) {
	e := sampleExecutionResult(0)
	e.Events = make([]Event, MaxEventsPerExecution+1)
	for i := range e.Events {
		e.Events[i] = sampleEvent(0, uint32(i))
	}

	t.Run("encode", func(t *testing.T) {
		if _, err := e.Encode(); !errors.Is(err, ErrLimitExceeded) {
			t.Fatalf("got %v, want ErrLimitExceeded", err)
		}
	})

	t.Run("decode", func(t *testing.T) {
		b := rawExecutionResult(0, e.Events, nil)
		if _, err := DecodeExecutionResult(b); !errors.Is(err, ErrLimitExceeded) {
			t.Fatalf("got %v, want ErrLimitExceeded", err)
		}
	})
}

func TestOutboxListOverCapRejected(t *testing.T) {
	e := sampleExecutionResult(0)
	e.Outbox = make([]OutboxMessage, MaxOutboxPerResult+1)
	for i := range e.Outbox {
		e.Outbox[i] = sampleL2Delivery(0, uint32(i))
	}

	t.Run("encode", func(t *testing.T) {
		if _, err := e.Encode(); !errors.Is(err, ErrLimitExceeded) {
			t.Fatalf("got %v, want ErrLimitExceeded", err)
		}
	})

	t.Run("decode", func(t *testing.T) {
		b := rawExecutionResult(0, nil, e.Outbox)
		if _, err := DecodeExecutionResult(b); !errors.Is(err, ErrLimitExceeded) {
			t.Fatalf("got %v, want ErrLimitExceeded", err)
		}
	})
}

// TestExecutionResultOutboxAtMaxPerResultAccepted freezes the §27.4 exemption: an
// ExecutionResult carrying exactly MaxOutboxPerResult messages — the contract's own
// MaxOutboxPerExecution plus the runtime's refund slot — round-trips.
func TestExecutionResultOutboxAtMaxPerResultAccepted(t *testing.T) {
	e := sampleExecutionResult(0)
	e.Outbox = make([]OutboxMessage, MaxOutboxPerResult)
	for i := range e.Outbox {
		e.Outbox[i] = sampleL2Delivery(0, uint32(i))
	}
	b, err := e.Encode()
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	got, err := DecodeExecutionResult(b)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if len(got.Outbox) != MaxOutboxPerResult {
		t.Errorf("got %d outbox messages, want %d", len(got.Outbox), MaxOutboxPerResult)
	}
}

// rawContractEffects hand-builds a canonical ContractEffects encoding with the given outbox
// messages written verbatim via encodeOutboxMessageBody, bypassing writeOutboxList's count
// check — Encode refuses to emit such bytes, so this is the only way to construct them for a
// decode-side rejection test.
func rawContractEffects(globalInputIndex uint64, outbox []OutboxMessage) []byte {
	w := &writer{}
	w.u16(ExecutionResultVersion)
	w.u64(globalInputIndex)
	w.u32(0) // empty local state diff list
	w.u32(0) // empty event list

	w.u32(uint32(len(outbox)))
	for _, m := range outbox {
		if err := encodeOutboxMessageBody(w, m); err != nil {
			panic(err)
		}
	}

	w.bytes([]byte("ret"))
	w.u256(U256{31: 3})
	return w.buf
}

func TestContractEffectsOutboxOverCapRejected(t *testing.T) {
	c := sampleContractEffects(0)
	c.Outbox = make([]OutboxMessage, MaxOutboxPerExecution+1)
	for i := range c.Outbox {
		c.Outbox[i] = sampleL2Delivery(0, uint32(i))
	}

	t.Run("encode", func(t *testing.T) {
		if _, err := c.Encode(); !errors.Is(err, ErrLimitExceeded) {
			t.Fatalf("got %v, want ErrLimitExceeded", err)
		}
	})

	t.Run("decode", func(t *testing.T) {
		b := rawContractEffects(0, c.Outbox)
		if _, err := DecodeContractEffects(b); !errors.Is(err, ErrLimitExceeded) {
			t.Fatalf("got %v, want ErrLimitExceeded", err)
		}
	})
}

func TestOutboxMessagePayloadOverCapRejected(t *testing.T) {
	m := sampleL2Delivery(0, 0)
	m.Payload = make([]byte, MaxOutboxPayloadSize+1)
	if _, err := m.Encode(); !errors.Is(err, ErrLimitExceeded) {
		t.Fatalf("got %v, want ErrLimitExceeded", err)
	}
}

func TestExecutionResultTrailingBytes(t *testing.T) {
	e := sampleExecutionResult(0)
	b, err := e.Encode()
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	b = append(b, 0x00)
	if _, err := DecodeExecutionResult(b); !errors.Is(err, ErrTrailingBytes) {
		t.Fatalf("got %v, want ErrTrailingBytes", err)
	}
}

// TestRootsPresentPinned constructs a 122-byte blob that decodes both as a valid
// ExecutionResult and, via a different four-field split of the same 80 bytes between
// preStateRoot/postStateRoot and the four zero list-count prefixes, as a valid
// ContractEffects. DecodeContractEffects on it MUST return exactly ErrRootsPresent.
func TestRootsPresentPinned(t *testing.T) {
	// ExecutionResult reading:
	//   version(2) globalInputIndex(8) preStateRoot(32) postStateRoot(32)
	//   stateDiffCount(4)=0 eventCount(4)=0 outboxCount(4)=0 returnDataLen(4)=0
	//   claimedDeposit(32)
	// Total = 2+8+32+32+4+4+4+4+32 = 122 bytes.
	//
	// The 80 bytes spanning preStateRoot||postStateRoot||four-zero-prefixes are chosen so a
	// ContractEffects reading of the SAME bytes sees:
	//   stateDiffCount(4)=1, one local entry {keyLen(4)=4,key(4),valueLen(4)=0} = 16 bytes,
	//   eventCount(4)=0, outboxCount(4)=0, returnDataLen(4)=52 (covers the remaining bytes).
	// 4 (count=1) + 16 (entry) + 4 (eventCount) + 4 (outboxCount) + 4 (returnDataLen) = 32
	// bytes, leaving 80-32 = 48 bytes to be absorbed as part of returnData's 52 declared
	// bytes together with the trailing claimed_deposit's leading 4 bytes... instead, we
	// build the 80 bytes directly rather than reason about the split twice.
	buf := &writer{}
	buf.u16(ExecutionResultVersion) // executionresult_version / version, shared field
	buf.u64(42)                     // globalInputIndex, shared field

	// The 80-byte region: for the ExecutionResult reading this is preStateRoot(32) +
	// postStateRoot(32) + four zero u32 prefixes (16), i.e. an empty StateDiff/Events/
	// Outbox/ReturnData. For the ContractEffects reading, the SAME 80 bytes are
	// stateDiffCount(4)=1 + one entry (keyLen=4,key=4 bytes,valueLen=0 => 12 bytes, so
	// count+entry=16) + eventCount(4)=0 + outboxCount(4)=0 + returnDataLen(4)=52, followed
	// by 52 bytes of return-data payload — 16+4+4+4+52 = 80.
	region := &writer{}
	region.u32(1)                              // ContractEffects: stateDiffCount = 1
	region.u32(4)                              // local key length = 4
	region.raw([]byte{0xAA, 0xBB, 0xCC, 0xDD}) // local key bytes
	region.u32(0)                              // value length = 0 (present-empty)
	region.u32(0)                              // ContractEffects: eventCount = 0
	region.u32(0)                              // ContractEffects: outboxCount = 0
	region.u32(52)                             // ContractEffects: returnDataLen = 52
	if len(region.buf) != 28 {
		t.Fatalf("region header is %d bytes, want 28", len(region.buf))
	}
	region.raw(make([]byte, 52)) // the 52 bytes of returnData payload
	if len(region.buf) != 80 {
		t.Fatalf("region is %d bytes, want 80", len(region.buf))
	}

	buf.raw(region.buf)       // preStateRoot || postStateRoot || four zero prefixes (ER view)
	buf.raw(make([]byte, 32)) // claimed_deposit = 0, shared tail field

	if len(buf.buf) != 122 {
		t.Fatalf("constructed blob is %d bytes, want 122", len(buf.buf))
	}

	// Sanity: it is a valid ExecutionResult.
	if _, err := DecodeExecutionResult(buf.buf); err != nil {
		t.Fatalf("sanity DecodeExecutionResult: %v", err)
	}

	// Pinned assertion: DecodeContractEffects rejects it with exactly ErrRootsPresent.
	if _, err := DecodeContractEffects(buf.buf); !errors.Is(err, ErrRootsPresent) {
		t.Fatalf("got %v, want ErrRootsPresent", err)
	}

	// An ordinary ExecutionResult fed to DecodeContractEffects (this exact blob, since it IS
	// an ordinary ExecutionResult) must be rejected — any error is acceptable there; this
	// asserts the stronger, pinned case above. Now check the reverse: a genuinely ordinary
	// ExecutionResult encoding (with a non-degenerate shape) still gets rejected by
	// DecodeContractEffects with some error.
	ordinary := sampleExecutionResult(0)
	ob, err := ordinary.Encode()
	if err != nil {
		t.Fatalf("Encode ordinary: %v", err)
	}
	if _, err := DecodeContractEffects(ob); err == nil {
		t.Fatalf("expected DecodeContractEffects to reject an ordinary ExecutionResult encoding")
	}

	// ContractEffects.Encode on the struct decoded from the crafted blob's ContractEffects
	// reading must also return ErrRootsPresent.
	r := newReader(buf.buf)
	crafted, err := decodeContractEffectsBody(r)
	if err != nil {
		t.Fatalf("decodeContractEffectsBody on crafted blob: %v", err)
	}
	if err := r.end(); err != nil {
		t.Fatalf("crafted blob ContractEffects reading did not consume exactly: %v", err)
	}
	if _, err := crafted.Encode(); !errors.Is(err, ErrRootsPresent) {
		t.Fatalf("ContractEffects.Encode on crafted struct: got %v, want ErrRootsPresent", err)
	}
}

func TestExecutionResultReturnDataBound(t *testing.T) {
	withReturnData := func(n int) ExecutionResult {
		return ExecutionResult{Version: ExecutionResultVersion, GlobalInputIndex: 42, ReturnData: bytes.Repeat([]byte{0xAB}, n)}
	}

	t.Run("encode refuses MaxReturnData+1", func(t *testing.T) {
		if _, err := withReturnData(MaxReturnData + 1).Encode(); !errors.Is(err, ErrLimitExceeded) {
			t.Fatalf("got %v, want ErrLimitExceeded", err)
		}
	})

	t.Run("decode rejects MaxReturnData+1", func(t *testing.T) {
		enc, err := withReturnData(MaxReturnData).Encode()
		if err != nil {
			t.Fatalf("Encode: %v", err)
		}
		lenAt := len(enc) - 32 - 4 - MaxReturnData
		over := append([]byte(nil), enc[:len(enc)-32]...)
		binary.BigEndian.PutUint32(over[lenAt:], MaxReturnData+1)
		over = append(over, 0xAB)
		over = append(over, enc[len(enc)-32:]...)
		if _, err := DecodeExecutionResult(over); !errors.Is(err, ErrLimitExceeded) {
			t.Fatalf("got %v, want ErrLimitExceeded", err)
		}
	})

	t.Run("MaxReturnData round-trips", func(t *testing.T) {
		in := withReturnData(MaxReturnData)
		enc, err := in.Encode()
		if err != nil {
			t.Fatalf("Encode: %v", err)
		}
		out, err := DecodeExecutionResult(enc)
		if err != nil {
			t.Fatalf("Decode: %v", err)
		}
		if !bytes.Equal(out.ReturnData, in.ReturnData) {
			t.Fatalf("ReturnData = %d bytes, want %d", len(out.ReturnData), len(in.ReturnData))
		}
	})
}

// TestRootsPresentProbeUnbounded builds a blob whose 9-field ExecutionResult reading carries
// MaxReturnData+1 bytes of ReturnData. DecodeExecutionResult rejects it, yet both
// ContractEffects entry points still treat it as roots-present.
func TestRootsPresentProbeUnbounded(t *testing.T) {
	const m = MaxReturnData + 1
	buf := &writer{}
	buf.u16(ExecutionResultVersion)
	buf.u64(42)
	// 64-byte region: ContractEffects sees three zero counts and returnDataLen = 64 + m.
	buf.raw(make([]byte, 12))
	buf.u32(64 + m)
	buf.raw(make([]byte, 48))
	// ExecutionResult sees the region as the two roots, then its own counts and ReturnData.
	buf.u32(0)
	buf.u32(0)
	buf.u32(0)
	buf.u32(m)
	buf.raw(make([]byte, m))
	buf.raw(make([]byte, 32))

	if _, err := DecodeExecutionResult(buf.buf); !errors.Is(err, ErrLimitExceeded) {
		t.Fatalf("DecodeExecutionResult: got %v, want ErrLimitExceeded", err)
	}
	if _, err := DecodeContractEffects(buf.buf); !errors.Is(err, ErrRootsPresent) {
		t.Fatalf("DecodeContractEffects: got %v, want ErrRootsPresent", err)
	}

	eff := ContractEffects{
		Version:          ExecutionResultVersion,
		GlobalInputIndex: 42,
		ReturnData:       buf.buf[10+16 : len(buf.buf)-32],
	}
	if _, err := eff.Encode(); !errors.Is(err, ErrRootsPresent) {
		t.Fatalf("ContractEffects.Encode: got %v, want ErrRootsPresent", err)
	}
}
