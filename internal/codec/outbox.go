package codec

import (
	"fmt"

	"github.com/zenon-network/go-zenon/common/crypto"
	"github.com/zenon-network/go-zenon/common/types"
)

// OutboxTarget is the kind-specific branch of an OutboxMessage. Modeling it as an interface
// makes an inconsistent kind/branch pair unrepresentable — there is no "inactive field must
// be zero" check to enforce.
type OutboxTarget interface {
	// Kind returns the §27.4 kind ordinal this target encodes as.
	Kind() uint8
}

// L2Delivery is the kind==0 branch: an L2/cross-domain/cross-chain message delivery.
type L2Delivery struct {
	TargetContract types.Hash
}

// Kind returns KindL2Delivery.
func (L2Delivery) Kind() uint8 { return KindL2Delivery }

// L1Withdrawal is the kind==1 branch: an L1/foreign withdrawal (peg-out) authorization. A
// runtime-emitted deposit refund is an ordinary L1Withdrawal with SourceContract zero — no
// special case exists in the codec.
type L1Withdrawal struct {
	RecipientL1 types.Address
	Asset       AssetID
	Amount      U256
}

// Kind returns KindL1Withdrawal.
func (L1Withdrawal) Kind() uint8 { return KindL1Withdrawal }

// OutboxMessage is one asynchronous message destined for L1 or another domain (§27.4).
type OutboxMessage struct {
	SourceContract types.Hash
	InputIndex     uint64
	OutboxIndex    uint32
	Target         OutboxTarget
	Payload        []byte
}

// Encode returns the canonical encoding of m.
func (m OutboxMessage) Encode() ([]byte, error) {
	w := &writer{}
	if err := encodeOutboxMessageBody(w, m); err != nil {
		return nil, fmt.Errorf("codec: encode outbox message: %w", err)
	}
	return w.buf, nil
}

// DecodeOutboxMessage decodes a standalone canonical OutboxMessage. It applies no
// index-consistency check — that rule lives at the enclosing blob boundary.
func DecodeOutboxMessage(b []byte) (OutboxMessage, error) {
	r := newReader(b)
	m, err := decodeOutboxMessageBody(r)
	if err != nil {
		return m, fmt.Errorf("codec: decode outbox message: %w", err)
	}
	if err := r.end(); err != nil {
		return m, fmt.Errorf("codec: decode outbox message: %w", err)
	}
	return m, nil
}

func encodeOutboxMessageBody(w *writer, m OutboxMessage) error {
	if m.Target == nil {
		return ErrInvalidKind
	}
	kind := m.Target.Kind()
	if kind != KindL2Delivery && kind != KindL1Withdrawal {
		return ErrInvalidKind
	}
	if kind == KindL1Withdrawal && len(m.Payload) != 0 {
		return ErrNonEmptyWithdrawalPayload
	}
	if len(m.Payload) > MaxOutboxPayloadSize {
		return ErrLimitExceeded
	}

	w.hash(m.SourceContract)
	w.u64(m.InputIndex)
	w.u32(m.OutboxIndex)
	w.u8(kind)

	switch target := m.Target.(type) {
	case L2Delivery:
		w.hash(target.TargetContract)
	case L1Withdrawal:
		if err := target.Asset.Validate(); err != nil {
			return err
		}
		w.address(target.RecipientL1)
		w.raw(target.Asset[:])
		w.u256(target.Amount)
	default:
		return ErrInvalidKind
	}

	w.bytes(m.Payload)
	return nil
}

func decodeOutboxMessageBody(r *reader) (OutboxMessage, error) {
	var m OutboxMessage
	var err error

	if m.SourceContract, err = r.hash(); err != nil {
		return m, err
	}
	if m.InputIndex, err = r.u64(); err != nil {
		return m, err
	}
	if m.OutboxIndex, err = r.u32(); err != nil {
		return m, err
	}
	kind, err := r.u8()
	if err != nil {
		return m, err
	}

	switch kind {
	case KindL2Delivery:
		target, err := r.hash()
		if err != nil {
			return m, err
		}
		m.Target = L2Delivery{TargetContract: target}
	case KindL1Withdrawal:
		recipient, err := r.address()
		if err != nil {
			return m, err
		}
		asset, err := r.assetID()
		if err != nil {
			return m, err
		}
		amount, err := r.u256()
		if err != nil {
			return m, err
		}
		m.Target = L1Withdrawal{RecipientL1: recipient, Asset: asset, Amount: amount}
	default:
		return m, ErrInvalidKind
	}

	payload, err := r.bytes(MaxOutboxPayloadSize)
	if err != nil {
		return m, err
	}
	if kind == KindL1Withdrawal && len(payload) != 0 {
		return m, ErrNonEmptyWithdrawalPayload
	}
	m.Payload = payload
	return m, nil
}

// minOutboxEncodedSize is the smallest possible encoded OutboxMessage: source_contract(32) +
// input_index(8) + outbox_index(4) + kind(1) + the smallest branch (L2Delivery, 32) +
// payload length(4).
const minOutboxEncodedSize = 32 + 8 + 4 + 1 + 32 + 4

// writeOutboxList encodes a List<OutboxMessage> and enforces the §27.4 index rules shared by
// ContractEffects and ExecutionResult: count ≤ max (the caller's bound: MaxOutboxPerExecution
// for a ContractEffects, MaxOutboxPerResult for an ExecutionResult), ascending OutboxIndex
// from 0 with no gaps, and InputIndex == globalInputIndex.
func writeOutboxList(w *writer, messages []OutboxMessage, globalInputIndex uint64, max int) error {
	if len(messages) > max {
		return ErrLimitExceeded
	}
	w.u32(uint32(len(messages)))
	for i, m := range messages {
		if m.OutboxIndex != uint32(i) {
			return ErrIndexGap
		}
		if m.InputIndex != globalInputIndex {
			return ErrIndexMismatch
		}
		if err := encodeOutboxMessageBody(w, m); err != nil {
			return err
		}
	}
	return nil
}

func readOutboxList(r *reader, globalInputIndex uint64, max int) ([]OutboxMessage, error) {
	count, err := r.count(minOutboxEncodedSize)
	if err != nil {
		return nil, err
	}
	if count > uint32(max) {
		return nil, ErrLimitExceeded
	}
	messages := make([]OutboxMessage, 0, count)
	for i := uint32(0); i < count; i++ {
		m, err := decodeOutboxMessageBody(r)
		if err != nil {
			return nil, err
		}
		if m.OutboxIndex != i {
			return nil, ErrIndexGap
		}
		if m.InputIndex != globalInputIndex {
			return nil, ErrIndexMismatch
		}
		messages = append(messages, m)
	}
	return messages, nil
}

// OutboxID computes the replay-protection identifier of an outbox message (§17.3):
// SHA3-256(domainId:u32 || batchId:u64 || inputIndex:u64 || outboxIndex:u32).
func OutboxID(domainID uint32, batchID, inputIndex uint64, outboxIndex uint32) types.Hash {
	w := &writer{}
	w.u32(domainID)
	w.u64(batchID)
	w.u64(inputIndex)
	w.u32(outboxIndex)
	return hashOf(crypto.Hash(w.buf))
}
