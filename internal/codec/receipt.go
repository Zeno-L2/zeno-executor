package codec

import (
	"fmt"

	"github.com/zenon-network/go-zenon/common/crypto"
	"github.com/zenon-network/go-zenon/common/types"
)

// Status is the §15 receipt status ordinal (exhaustive; ordinals fixed).
type Status uint8

const (
	StatusSuccess              Status = 0
	StatusRevert               Status = 1
	StatusOutOfGas             Status = 2
	StatusRuntimeFault         Status = 3
	StatusValidationFailed     Status = 4
	StatusResultTooLarge       Status = 5
	StatusUnauthorized         Status = 6
	StatusUnsupportedOperation Status = 7
)

// String returns the §15 status name, or "UNKNOWN" for an out-of-range ordinal.
func (s Status) String() string {
	switch s {
	case StatusSuccess:
		return "SUCCESS"
	case StatusRevert:
		return "REVERT"
	case StatusOutOfGas:
		return "OUT_OF_GAS"
	case StatusRuntimeFault:
		return "RUNTIME_FAULT"
	case StatusValidationFailed:
		return "VALIDATION_FAILED"
	case StatusResultTooLarge:
		return "RESULT_TOO_LARGE"
	case StatusUnauthorized:
		return "UNAUTHORIZED"
	case StatusUnsupportedOperation:
		return "UNSUPPORTED_OPERATION"
	default:
		return "UNKNOWN"
	}
}

// Receipt is the per-input receipt (§27.7, §15).
type Receipt struct {
	Version       uint16
	InputIndex    uint64
	InputHash     types.Hash
	BatchID       uint64
	ContractID    types.Hash
	CodeHash      types.Hash
	Status        Status
	GasUsed       uint64
	AbortCode     int32
	StateDiffHash types.Hash
	ReturnHash    types.Hash
	EventRoot     types.Hash
	EventCount    uint32
	OutboxRoot    types.Hash
	OutboxCount   uint32
}

// Encode returns the canonical encoding of r.
func (rec Receipt) Encode() ([]byte, error) {
	if rec.Version != ReceiptVersion {
		return nil, fmt.Errorf("codec: encode receipt: %w", ErrUnknownVersion)
	}
	if rec.Status > StatusUnsupportedOperation {
		return nil, fmt.Errorf("codec: encode receipt: %w", ErrInvalidStatus)
	}

	w := &writer{}
	w.u16(rec.Version)
	w.u64(rec.InputIndex)
	w.hash(rec.InputHash)
	w.u64(rec.BatchID)
	w.hash(rec.ContractID)
	w.hash(rec.CodeHash)
	w.u8(uint8(rec.Status))
	w.u64(rec.GasUsed)
	w.i32(rec.AbortCode)
	w.hash(rec.StateDiffHash)
	w.hash(rec.ReturnHash)
	w.hash(rec.EventRoot)
	w.u32(rec.EventCount)
	w.hash(rec.OutboxRoot)
	w.u32(rec.OutboxCount)
	return w.buf, nil
}

// DecodeReceipt decodes a canonical Receipt. It rejects receipt_version != ReceiptVersion
// first (ErrUnknownVersion), then status > 7 (ErrInvalidStatus).
func DecodeReceipt(b []byte) (Receipt, error) {
	r := newReader(b)
	var rec Receipt
	var err error

	v, err := r.u16()
	if err != nil {
		return rec, fmt.Errorf("codec: decode receipt: %w", err)
	}
	if v != ReceiptVersion {
		return rec, fmt.Errorf("codec: decode receipt: %w", ErrUnknownVersion)
	}
	rec.Version = v

	if rec.InputIndex, err = r.u64(); err != nil {
		return rec, fmt.Errorf("codec: decode receipt: %w", err)
	}
	if rec.InputHash, err = r.hash(); err != nil {
		return rec, fmt.Errorf("codec: decode receipt: %w", err)
	}
	if rec.BatchID, err = r.u64(); err != nil {
		return rec, fmt.Errorf("codec: decode receipt: %w", err)
	}
	if rec.ContractID, err = r.hash(); err != nil {
		return rec, fmt.Errorf("codec: decode receipt: %w", err)
	}
	if rec.CodeHash, err = r.hash(); err != nil {
		return rec, fmt.Errorf("codec: decode receipt: %w", err)
	}
	statusByte, err := r.u8()
	if err != nil {
		return rec, fmt.Errorf("codec: decode receipt: %w", err)
	}
	if statusByte > uint8(StatusUnsupportedOperation) {
		return rec, fmt.Errorf("codec: decode receipt: %w", ErrInvalidStatus)
	}
	rec.Status = Status(statusByte)

	if rec.GasUsed, err = r.u64(); err != nil {
		return rec, fmt.Errorf("codec: decode receipt: %w", err)
	}
	if rec.AbortCode, err = r.i32(); err != nil {
		return rec, fmt.Errorf("codec: decode receipt: %w", err)
	}
	if rec.StateDiffHash, err = r.hash(); err != nil {
		return rec, fmt.Errorf("codec: decode receipt: %w", err)
	}
	if rec.ReturnHash, err = r.hash(); err != nil {
		return rec, fmt.Errorf("codec: decode receipt: %w", err)
	}
	if rec.EventRoot, err = r.hash(); err != nil {
		return rec, fmt.Errorf("codec: decode receipt: %w", err)
	}
	if rec.EventCount, err = r.u32(); err != nil {
		return rec, fmt.Errorf("codec: decode receipt: %w", err)
	}
	if rec.OutboxRoot, err = r.hash(); err != nil {
		return rec, fmt.Errorf("codec: decode receipt: %w", err)
	}
	if rec.OutboxCount, err = r.u32(); err != nil {
		return rec, fmt.Errorf("codec: decode receipt: %w", err)
	}

	if err := r.end(); err != nil {
		return rec, fmt.Errorf("codec: decode receipt: %w", err)
	}
	return rec, nil
}

// StateDiffHash is the §15 receipt preimage rule: SHA3-256 over the canonical
// List<StateDiffEntry> encoding (§27.1a, §27.2), and bytes32(0) when the list is empty.
func StateDiffHash(entries []StateDiffEntry) (types.Hash, error) {
	if len(entries) == 0 {
		return types.Hash{}, nil
	}
	w := &writer{}
	if err := writeDerivedStateDiffList(w, entries); err != nil {
		return types.Hash{}, fmt.Errorf("codec: state diff hash: %w", err)
	}
	return hashOf(crypto.Hash(w.buf)), nil
}

// ReturnHash is the §15 receipt preimage rule: SHA3-256(ReturnData) over the raw bytes with
// no length prefix and no framing, and bytes32(0) when ReturnData is empty.
func ReturnHash(data []byte) types.Hash {
	if len(data) == 0 {
		return types.Hash{}
	}
	return hashOf(crypto.Hash(data))
}
