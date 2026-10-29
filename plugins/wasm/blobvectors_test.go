package wasm

import (
	"bytes"
	"encoding/binary"
	"testing"

	"github.com/zenon-network/go-zenon/common/types"

	"github.com/Zeno-L2/zeno-executor/internal/codec"
)

// minimalEvent is an Event with no topics and no data, the smallest encodable event.
func minimalEvent(idx uint64, i uint32) codec.Event {
	return codec.Event{ContractID: cid2, InputIndex: idx, EventIndex: i}
}

// minimalDelivery is an L2Delivery OutboxMessage with an empty payload, the smallest
// encodable delivery.
func minimalDelivery(idx uint64, i uint32) codec.OutboxMessage {
	return codec.OutboxMessage{SourceContract: cid2, InputIndex: idx, OutboxIndex: i, Target: codec.L2Delivery{TargetContract: repeatedHash(0x55)}}
}

// blobVectors returns the malformed and boundary ContractEffects blob vectorSpecs
// (CV-ATTR-*, CV-DECODE-*, CV-LIMIT-*), drawing each index from nextIndex in code order.
func blobVectors(t *testing.T, nextIndex func() uint64) []vectorSpec {
	t.Helper()

	var specs []vectorSpec

	// --- malformed and boundary ContractEffects blobs: CV-ATTR-*, CV-DECODE-*, CV-LIMIT-*.

	insertAt := func(b []byte, i int, ins []byte) []byte {
		return append(append(append([]byte{}, b[:i]...), ins...), b[i:]...)
	}
	encodedEvent := func(e codec.Event) []byte {
		b, err := e.Encode()
		if err != nil {
			t.Fatalf("Event.Encode: %v", err)
		}
		return b
	}
	encodedMessage := func(m codec.OutboxMessage) []byte {
		b, err := m.Encode()
		if err != nil {
			t.Fatalf("OutboxMessage.Encode: %v", err)
		}
		return b
	}
	events64 := func(idx uint64) []codec.Event {
		events := make([]codec.Event, codec.MaxEventsPerExecution)
		for i := range events {
			events[i] = minimalEvent(idx, uint32(i))
		}
		return events
	}
	topicEvent := func(idx uint64) codec.Event {
		e := minimalEvent(idx, 0)
		e.Topics = []types.Hash{repeatedHash(0xA1), repeatedHash(0xA2), repeatedHash(0xA3), repeatedHash(0xA4)}
		return e
	}
	dataEvent := func(idx uint64) codec.Event {
		e := minimalEvent(idx, 0)
		e.Data = bytes.Repeat([]byte{0xAB}, codec.MaxEventDataSize)
		return e
	}
	payloadDelivery := func(idx uint64) codec.OutboxMessage {
		m := minimalDelivery(idx, 0)
		m.Payload = bytes.Repeat([]byte{0xAB}, codec.MaxOutboxPayloadSize)
		return m
	}
	foreignSource := func(m codec.OutboxMessage, source types.Hash) codec.OutboxMessage {
		m.SourceContract = source
		return m
	}
	maxKeyWrite := codec.LocalStateDiffEntry{LocalKey: bytes.Repeat([]byte("k"), codec.MaxStateKeySize), Value: []byte{0x01}}

	specs = append(specs, orderVector(nextIndex(), "CV-ATTR-1",
		"an event whose contract_id is another contract: RUNTIME_FAULT at the metered gas",
		codec.StatusRuntimeFault, callPlasmaLimit, nil, nil,
		effectsBlob(t, func(idx uint64, eff *codec.ContractEffects) {
			eff.Events = []codec.Event{{ContractID: repeatedHash(0x44), InputIndex: idx, EventIndex: 0, Data: []byte("evt")}}
		})))
	specs = append(specs, orderVector(nextIndex(), "CV-ATTR-2",
		"an L2 delivery whose source_contract is another, non-zero contract: RUNTIME_FAULT at the metered gas",
		codec.StatusRuntimeFault, callPlasmaLimit, nil, nil,
		effectsBlob(t, func(idx uint64, eff *codec.ContractEffects) {
			eff.Outbox = []codec.OutboxMessage{foreignSource(minimalDelivery(idx, 0), repeatedHash(0x44))}
		})))
	specs = append(specs, orderVector(nextIndex(), "CV-ATTR-3",
		"a capped withdrawal whose source_contract is bytes32(0), the runtime's refund marker, which a contract cannot emit: RUNTIME_FAULT at the metered gas",
		codec.StatusRuntimeFault, callPlasmaLimit, nil, capsOfOne(),
		effectsBlob(t, func(idx uint64, eff *codec.ContractEffects) {
			eff.Outbox = []codec.OutboxMessage{foreignSource(withdrawal(idx, 0, testAsset(), 1), types.Hash{})}
		})))
	specs = append(specs, orderVector(nextIndex(), "CV-DECODE-1",
		"a blob whose executionresult_version is 2: an unknown version is rejected, so RUNTIME_FAULT at the metered gas",
		codec.StatusRuntimeFault, callPlasmaLimit, nil, nil,
		func(idx uint64) []byte {
			eff := codec.ContractEffects{Version: codec.ExecutionResultVersion, GlobalInputIndex: idx, ReturnData: []byte{0x01}}
			return corruptBlob(t, eff, codec.ErrUnknownVersion, func(b []byte) []byte {
				b[0], b[1] = 0x00, 0x02
				return b
			})
		}))
	specs = append(specs, orderVector(nextIndex(), "CV-DECODE-2",
		"an event whose input_index is not the executing input's: the decoder rejects it (§27.1a), so RUNTIME_FAULT at the metered gas",
		codec.StatusRuntimeFault, callPlasmaLimit, nil, nil,
		func(idx uint64) []byte {
			eff := codec.ContractEffects{Version: codec.ExecutionResultVersion, GlobalInputIndex: idx, Events: []codec.Event{{ContractID: cid2, InputIndex: idx, EventIndex: 0, Data: []byte("evt")}}}
			return corruptBlob(t, eff, codec.ErrIndexMismatch, func(b []byte) []byte {
				binary.BigEndian.PutUint64(b[50:58], idx+1)
				return b
			})
		}))
	specs = append(specs, orderVector(nextIndex(), "CV-DECODE-3",
		"an outbox message whose input_index is not the executing input's: the decoder rejects it (§27.1a), so RUNTIME_FAULT at the metered gas",
		codec.StatusRuntimeFault, callPlasmaLimit, nil, nil,
		func(idx uint64) []byte {
			eff := codec.ContractEffects{Version: codec.ExecutionResultVersion, GlobalInputIndex: idx, Outbox: []codec.OutboxMessage{minimalDelivery(idx, 0)}}
			return corruptBlob(t, eff, codec.ErrIndexMismatch, func(b []byte) []byte {
				binary.BigEndian.PutUint64(b[54:62], idx+1)
				return b
			})
		}))
	specs = append(specs, orderVector(nextIndex(), "CV-DECODE-4",
		"one trailing byte after claimed_deposit: RUNTIME_FAULT at the metered gas",
		codec.StatusRuntimeFault, callPlasmaLimit, nil, nil,
		func(idx uint64) []byte {
			eff := codec.ContractEffects{Version: codec.ExecutionResultVersion, GlobalInputIndex: idx, ReturnData: []byte{0x01}}
			return corruptBlob(t, eff, codec.ErrTrailingBytes, func(b []byte) []byte {
				return append(b, 0x00)
			})
		}))
	specs = append(specs, orderVector(nextIndex(), "CV-DECODE-5",
		"a blob that ends after ReturnData, omitting claimed_deposit: the field never defaults to 0, so RUNTIME_FAULT at the metered gas",
		codec.StatusRuntimeFault, callPlasmaLimit, nil, nil,
		func(idx uint64) []byte {
			eff := codec.ContractEffects{Version: codec.ExecutionResultVersion, GlobalInputIndex: idx, ReturnData: []byte{0x01}}
			return corruptBlob(t, eff, codec.ErrTruncated, func(b []byte) []byte {
				return b[:len(b)-32]
			})
		}))
	specs = append(specs, orderVector(nextIndex(), "CV-DECODE-6",
		"a ReturnData length prefix one above the bytes present: the prefix is never repaired, so RUNTIME_FAULT at the metered gas",
		codec.StatusRuntimeFault, callPlasmaLimit, nil, nil,
		func(idx uint64) []byte {
			eff := codec.ContractEffects{Version: codec.ExecutionResultVersion, GlobalInputIndex: idx, ReturnData: []byte{0x01, 0x02}}
			return corruptBlob(t, eff, codec.ErrTruncated, func(b []byte) []byte {
				binary.BigEndian.PutUint32(b[22:26], 3)
				return b
			})
		}))
	specs = append(specs, orderVector(nextIndex(), "CV-DECODE-7",
		"an outbox message of kind 2: only kinds 0 and 1 exist, so RUNTIME_FAULT at the metered gas",
		codec.StatusRuntimeFault, callPlasmaLimit, nil, nil,
		func(idx uint64) []byte {
			eff := codec.ContractEffects{Version: codec.ExecutionResultVersion, GlobalInputIndex: idx, Outbox: []codec.OutboxMessage{minimalDelivery(idx, 0)}}
			return corruptBlob(t, eff, codec.ErrInvalidKind, func(b []byte) []byte {
				b[66] = 2
				return b
			})
		}))
	specs = append(specs, orderVector(nextIndex(), "CV-DECODE-8",
		"a zero-amount withdrawal naming a non-canonical AssetID whose only non-zero byte in [0:22] is byte 21, the last byte that must be zero: the decoder rejects it, so RUNTIME_FAULT at the metered gas",
		codec.StatusRuntimeFault, callPlasmaLimit, nil, capsOfOne(),
		func(idx uint64) []byte {
			eff := codec.ContractEffects{Version: codec.ExecutionResultVersion, GlobalInputIndex: idx, Outbox: []codec.OutboxMessage{withdrawal(idx, 0, testAsset(), 0)}}
			return corruptBlob(t, eff, codec.ErrInvalidAssetID, func(b []byte) []byte {
				b[108] = 0x01
				return b
			})
		}))

	{
		idx := nextIndex()
		mod := buildWasmModule(runModuleSpec(nil, 1, []byte{0x00}, returnBlobBody(blobOffset, 0), nil))
		specs = append(specs, vectorSpec{
			ID:         "CV-DECODE-9",
			Purpose:    "run returns a zero-length blob: an empty blob does not decode as a ContractEffects, so RUNTIME_FAULT at the metered gas",
			Shape:      "call",
			BatchID:    2,
			Ctx:        stdCtx(idx),
			Args:       codec.CallL2Args{TargetContract: cid2, PlasmaLimit: callPlasmaLimit},
			ContractID: cid2,
			Module:     mod,
			WantStatus: codec.StatusRuntimeFault,
		})
	}

	specs = append(specs, orderVector(nextIndex(), "CV-DECODE-10",
		"two events with event_index 0 and 2: the gap is rejected, so RUNTIME_FAULT at the metered gas",
		codec.StatusRuntimeFault, callPlasmaLimit, nil, nil,
		func(idx uint64) []byte {
			eff := codec.ContractEffects{Version: codec.ExecutionResultVersion, GlobalInputIndex: idx, Events: []codec.Event{minimalEvent(idx, 0), minimalEvent(idx, 1)}}
			return corruptBlob(t, eff, codec.ErrIndexGap, func(b []byte) []byte {
				binary.BigEndian.PutUint32(b[110:114], 2)
				return b
			})
		}))
	specs = append(specs, orderVector(nextIndex(), "CV-DECODE-11",
		"two outbox messages with outbox_index 0 and 2: the gap is rejected, so RUNTIME_FAULT at the metered gas",
		codec.StatusRuntimeFault, callPlasmaLimit, nil, nil,
		func(idx uint64) []byte {
			eff := codec.ContractEffects{Version: codec.ExecutionResultVersion, GlobalInputIndex: idx, Outbox: []codec.OutboxMessage{minimalDelivery(idx, 0), minimalDelivery(idx, 1)}}
			return corruptBlob(t, eff, codec.ErrIndexGap, func(b []byte) []byte {
				binary.BigEndian.PutUint32(b[143:147], 2)
				return b
			})
		}))
	specs = append(specs, orderVector(nextIndex(), "CV-LIMIT-1",
		"a StateDiff local key of MaxStateKeySize + 1 bytes: RUNTIME_FAULT at the metered gas",
		codec.StatusRuntimeFault, callPlasmaLimit, seeded(), nil,
		func(idx uint64) []byte {
			eff := codec.ContractEffects{Version: codec.ExecutionResultVersion, GlobalInputIndex: idx, StateDiff: []codec.LocalStateDiffEntry{maxKeyWrite}}
			return corruptBlob(t, eff, codec.ErrLimitExceeded, func(b []byte) []byte {
				b = insertAt(b, 50, []byte{'k'})
				binary.BigEndian.PutUint32(b[14:18], codec.MaxStateKeySize+1)
				return b
			})
		}))
	specs = append(specs, orderVector(nextIndex(), "CV-LIMIT-2",
		"a StateDiff local key of exactly MaxStateKeySize bytes is accepted",
		codec.StatusSuccess, callPlasmaLimit, seeded(), nil,
		effectsBlob(t, func(_ uint64, eff *codec.ContractEffects) {
			eff.StateDiff = []codec.LocalStateDiffEntry{maxKeyWrite}
		})))
	specs = append(specs, orderVector(nextIndex(), "CV-LIMIT-3",
		"a StateDiff value of MaxStateValueSize + 1 bytes, every byte present: RUNTIME_FAULT at the metered gas",
		codec.StatusRuntimeFault, callPlasmaLimit, seeded(), nil,
		func(idx uint64) []byte {
			eff := codec.ContractEffects{Version: codec.ExecutionResultVersion, GlobalInputIndex: idx, StateDiff: []codec.LocalStateDiffEntry{{LocalKey: []byte("v"), Value: bytes.Repeat([]byte{0xAB}, codec.MaxStateValueSize)}}}
			return corruptBlob(t, eff, codec.ErrReservedLength, func(b []byte) []byte {
				b = insertAt(b, len(b)-44, []byte{0xAB})
				binary.BigEndian.PutUint32(b[19:23], codec.MaxStateValueSize+1)
				return b
			})
		}))
	specs = append(specs, orderVector(nextIndex(), "CV-LIMIT-4",
		"MaxEventsPerExecution + 1 events: RUNTIME_FAULT at the metered gas",
		codec.StatusRuntimeFault, callPlasmaLimit, nil, nil,
		func(idx uint64) []byte {
			eff := codec.ContractEffects{Version: codec.ExecutionResultVersion, GlobalInputIndex: idx, Events: events64(idx)}
			return corruptBlob(t, eff, codec.ErrLimitExceeded, func(b []byte) []byte {
				b = insertAt(b, len(b)-40, encodedEvent(minimalEvent(idx, codec.MaxEventsPerExecution)))
				binary.BigEndian.PutUint32(b[14:18], codec.MaxEventsPerExecution+1)
				return b
			})
		}))
	specs = append(specs, orderVector(nextIndex(), "CV-LIMIT-5",
		"exactly MaxEventsPerExecution events are accepted",
		codec.StatusSuccess, callPlasmaLimit, nil, nil,
		effectsBlob(t, func(idx uint64, eff *codec.ContractEffects) {
			eff.Events = events64(idx)
		})))
	specs = append(specs, orderVector(nextIndex(), "CV-LIMIT-6",
		"an event with MaxTopicsPerEvent + 1 topics: RUNTIME_FAULT at the metered gas",
		codec.StatusRuntimeFault, callPlasmaLimit, nil, nil,
		func(idx uint64) []byte {
			eff := codec.ContractEffects{Version: codec.ExecutionResultVersion, GlobalInputIndex: idx, Events: []codec.Event{topicEvent(idx)}}
			return corruptBlob(t, eff, codec.ErrLimitExceeded, func(b []byte) []byte {
				fifth := repeatedHash(0xA5)
				b = insertAt(b, len(b)-44, fifth[:])
				binary.BigEndian.PutUint32(b[62:66], codec.MaxTopicsPerEvent+1)
				return b
			})
		}))
	specs = append(specs, orderVector(nextIndex(), "CV-LIMIT-7",
		"an event with exactly MaxTopicsPerEvent topics is accepted",
		codec.StatusSuccess, callPlasmaLimit, nil, nil,
		effectsBlob(t, func(idx uint64, eff *codec.ContractEffects) {
			eff.Events = []codec.Event{topicEvent(idx)}
		})))
	specs = append(specs, orderVector(nextIndex(), "CV-LIMIT-8",
		"an event with MaxEventDataSize + 1 data bytes: RUNTIME_FAULT at the metered gas",
		codec.StatusRuntimeFault, callPlasmaLimit, nil, nil,
		func(idx uint64) []byte {
			eff := codec.ContractEffects{Version: codec.ExecutionResultVersion, GlobalInputIndex: idx, Events: []codec.Event{dataEvent(idx)}}
			return corruptBlob(t, eff, codec.ErrLimitExceeded, func(b []byte) []byte {
				b = insertAt(b, len(b)-40, []byte{0xAB})
				binary.BigEndian.PutUint32(b[66:70], codec.MaxEventDataSize+1)
				return b
			})
		}))
	specs = append(specs, orderVector(nextIndex(), "CV-LIMIT-9",
		"an event with exactly MaxEventDataSize data bytes is accepted",
		codec.StatusSuccess, callPlasmaLimit, nil, nil,
		effectsBlob(t, func(idx uint64, eff *codec.ContractEffects) {
			eff.Events = []codec.Event{dataEvent(idx)}
		})))
	specs = append(specs, orderVector(nextIndex(), "CV-LIMIT-10",
		"an L2 delivery with a payload of MaxOutboxPayloadSize + 1 bytes: RUNTIME_FAULT at the metered gas",
		codec.StatusRuntimeFault, callPlasmaLimit, nil, nil,
		func(idx uint64) []byte {
			eff := codec.ContractEffects{Version: codec.ExecutionResultVersion, GlobalInputIndex: idx, Outbox: []codec.OutboxMessage{payloadDelivery(idx)}}
			return corruptBlob(t, eff, codec.ErrLimitExceeded, func(b []byte) []byte {
				b = insertAt(b, len(b)-36, []byte{0xAB})
				binary.BigEndian.PutUint32(b[99:103], codec.MaxOutboxPayloadSize+1)
				return b
			})
		}))
	specs = append(specs, orderVector(nextIndex(), "CV-LIMIT-11",
		"an L2 delivery with a payload of exactly MaxOutboxPayloadSize bytes is accepted",
		codec.StatusSuccess, callPlasmaLimit, nil, nil,
		effectsBlob(t, func(idx uint64, eff *codec.ContractEffects) {
			eff.Outbox = []codec.OutboxMessage{payloadDelivery(idx)}
		})))
	specs = append(specs, orderVector(nextIndex(), "CV-LIMIT-12",
		"a blob with MaxOutboxPerExecution + 1 messages: the ExecutionResult's refund slot does not widen a contract's own bound, so RUNTIME_FAULT at the metered gas",
		codec.StatusRuntimeFault, callPlasmaLimit, nil, nil,
		func(idx uint64) []byte {
			outbox := make([]codec.OutboxMessage, codec.MaxOutboxPerExecution)
			for i := range outbox {
				outbox[i] = minimalDelivery(idx, uint32(i))
			}
			eff := codec.ContractEffects{Version: codec.ExecutionResultVersion, GlobalInputIndex: idx, Outbox: outbox}
			return corruptBlob(t, eff, codec.ErrLimitExceeded, func(b []byte) []byte {
				b = insertAt(b, len(b)-36, encodedMessage(minimalDelivery(idx, codec.MaxOutboxPerExecution)))
				binary.BigEndian.PutUint32(b[18:22], codec.MaxOutboxPerExecution+1)
				return b
			})
		}))
	specs = append(specs, orderVector(nextIndex(), "CV-LIMIT-13",
		"a StateDiff value of exactly MaxStateValueSize bytes is accepted",
		codec.StatusSuccess, callPlasmaLimit, seeded(), nil,
		effectsBlob(t, func(_ uint64, eff *codec.ContractEffects) {
			eff.StateDiff = []codec.LocalStateDiffEntry{{LocalKey: []byte("v"), Value: bytes.Repeat([]byte{0xAB}, codec.MaxStateValueSize)}}
		})))
	specs = append(specs, orderVector(nextIndex(), "CV-DECODE-12",
		"a blob whose executionresult_version is 0: only version 1 is known, so RUNTIME_FAULT at the metered gas",
		codec.StatusRuntimeFault, callPlasmaLimit, nil, nil,
		func(idx uint64) []byte {
			eff := codec.ContractEffects{Version: codec.ExecutionResultVersion, GlobalInputIndex: idx, ReturnData: []byte{0x01}}
			return corruptBlob(t, eff, codec.ErrUnknownVersion, func(b []byte) []byte {
				b[0], b[1] = 0x00, 0x00
				return b
			})
		}))
	specs = append(specs, orderVector(nextIndex(), "CV-DECODE-13",
		"a lone event with event_index 1: event indices start at 0, so RUNTIME_FAULT at the metered gas",
		codec.StatusRuntimeFault, callPlasmaLimit, nil, nil,
		func(idx uint64) []byte {
			eff := codec.ContractEffects{Version: codec.ExecutionResultVersion, GlobalInputIndex: idx, Events: []codec.Event{minimalEvent(idx, 0)}}
			return corruptBlob(t, eff, codec.ErrIndexGap, func(b []byte) []byte {
				binary.BigEndian.PutUint32(b[58:62], 1)
				return b
			})
		}))
	specs = append(specs, orderVector(nextIndex(), "CV-DECODE-14",
		"a lone outbox message with outbox_index 1: outbox indices start at 0, so RUNTIME_FAULT at the metered gas",
		codec.StatusRuntimeFault, callPlasmaLimit, nil, nil,
		func(idx uint64) []byte {
			eff := codec.ContractEffects{Version: codec.ExecutionResultVersion, GlobalInputIndex: idx, Outbox: []codec.OutboxMessage{minimalDelivery(idx, 0)}}
			return corruptBlob(t, eff, codec.ErrIndexGap, func(b []byte) []byte {
				binary.BigEndian.PutUint32(b[62:66], 1)
				return b
			})
		}))
	specs = append(specs, orderVector(nextIndex(), "CV-DECODE-15",
		"an outbox message of kind 2 followed by an L1_WITHDRAWAL body: only kinds 0 and 1 exist, and kind 2 is never read as a withdrawal, so RUNTIME_FAULT at the metered gas",
		codec.StatusRuntimeFault, callPlasmaLimit, nil, capsOfOne(),
		func(idx uint64) []byte {
			eff := codec.ContractEffects{Version: codec.ExecutionResultVersion, GlobalInputIndex: idx, Outbox: []codec.OutboxMessage{withdrawal(idx, 0, testAsset(), 0)}}
			return corruptBlob(t, eff, codec.ErrInvalidKind, func(b []byte) []byte {
				b[66] = 2
				return b
			})
		}))
	specs = append(specs, orderVector(nextIndex(), "CV-DECODE-16",
		"an Events count prefix of 2 with one event present: the count is never treated as an upper bound, so RUNTIME_FAULT at the metered gas",
		codec.StatusRuntimeFault, callPlasmaLimit, nil, nil,
		func(idx uint64) []byte {
			eff := codec.ContractEffects{Version: codec.ExecutionResultVersion, GlobalInputIndex: idx, Events: []codec.Event{minimalEvent(idx, 0)}}
			return corruptBlob(t, eff, codec.ErrOverflow, func(b []byte) []byte {
				binary.BigEndian.PutUint32(b[14:18], 2)
				return b
			})
		}))

	return specs
}

// blobOrderVectors returns the CV-DECODE-17 to -19 vectorSpecs, drawing each index from
// nextIndex in code order.
func blobOrderVectors(t *testing.T, nextIndex func() uint64) []vectorSpec {
	t.Helper()

	var specs []vectorSpec

	specs = append(specs, orderVector(nextIndex(), "CV-DECODE-17",
		"a zero-amount withdrawal naming a non-canonical AssetID whose only non-zero byte in [0:22] is byte 0, the first byte that must be zero: the decoder rejects it, so RUNTIME_FAULT at the metered gas",
		codec.StatusRuntimeFault, callPlasmaLimit, nil, capsOfOne(),
		func(idx uint64) []byte {
			eff := codec.ContractEffects{Version: codec.ExecutionResultVersion, GlobalInputIndex: idx, Outbox: []codec.OutboxMessage{withdrawal(idx, 0, testAsset(), 0)}}
			return corruptBlob(t, eff, codec.ErrInvalidAssetID, func(b []byte) []byte {
				b[87] = 0x01
				return b
			})
		}))
	specs = append(specs, orderVector(nextIndex(), "CV-DECODE-18",
		"three events with event_index 0, 2 and 1: the list is a gap-free permutation but not ascending, so the decoder rejects it, so RUNTIME_FAULT at the metered gas",
		codec.StatusRuntimeFault, callPlasmaLimit, nil, nil,
		func(idx uint64) []byte {
			eff := codec.ContractEffects{Version: codec.ExecutionResultVersion, GlobalInputIndex: idx, Events: []codec.Event{minimalEvent(idx, 0), minimalEvent(idx, 1), minimalEvent(idx, 2)}}
			return corruptBlob(t, eff, codec.ErrIndexGap, func(b []byte) []byte {
				binary.BigEndian.PutUint32(b[110:114], 2)
				binary.BigEndian.PutUint32(b[162:166], 1)
				return b
			})
		}))
	specs = append(specs, orderVector(nextIndex(), "CV-DECODE-19",
		"three outbox messages with outbox_index 0, 2 and 1: the list is a gap-free permutation but not ascending, so the decoder rejects it, so RUNTIME_FAULT at the metered gas",
		codec.StatusRuntimeFault, callPlasmaLimit, nil, nil,
		func(idx uint64) []byte {
			eff := codec.ContractEffects{Version: codec.ExecutionResultVersion, GlobalInputIndex: idx, Outbox: []codec.OutboxMessage{minimalDelivery(idx, 0), minimalDelivery(idx, 1), minimalDelivery(idx, 2)}}
			return corruptBlob(t, eff, codec.ErrIndexGap, func(b []byte) []byte {
				binary.BigEndian.PutUint32(b[143:147], 2)
				binary.BigEndian.PutUint32(b[224:228], 1)
				return b
			})
		}))

	return specs
}
