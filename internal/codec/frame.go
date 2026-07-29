package codec

import (
	"fmt"

	"github.com/zenon-network/go-zenon/common/types"
)

// ExecutionInputFrame is the contiguous input frame the runtime writes at memory offset 0
// before invoking a contract's run entry point (§27.1).
type ExecutionInputFrame struct {
	ContextVersion uint16
	ChainID        types.Hash
	Caller         types.Hash
	ContractID     types.Hash
	CodeHash       types.Hash
	MomentumHeight uint64
	MomentumTime   uint64
	InputIndex     uint64
	DepositAsset   AssetID
	DepositAmount  U256
	Payload        []byte
}

// Encode returns the canonical encoding of f.
func (f ExecutionInputFrame) Encode() ([]byte, error) {
	if err := f.DepositAsset.Validate(); err != nil {
		return nil, fmt.Errorf("codec: encode frame: %w", err)
	}
	w := &writer{}
	w.u16(f.ContextVersion)
	w.hash(f.ChainID)
	w.hash(f.Caller)
	w.hash(f.ContractID)
	w.hash(f.CodeHash)
	w.u64(f.MomentumHeight)
	w.u64(f.MomentumTime)
	w.u64(f.InputIndex)
	w.raw(f.DepositAsset[:])
	w.u256(f.DepositAmount)
	w.bytes(f.Payload)
	return w.buf, nil
}

// DecodeExecutionInputFrame decodes a canonical ExecutionInputFrame. It rejects
// context_version != RuntimeProfileVersion with ErrUnknownVersion before reading any other
// field (§24).
func DecodeExecutionInputFrame(b []byte) (ExecutionInputFrame, error) {
	r := newReader(b)
	var f ExecutionInputFrame

	v, err := r.u16()
	if err != nil {
		return f, fmt.Errorf("codec: decode frame: %w", err)
	}
	if v != RuntimeProfileVersion {
		return f, fmt.Errorf("codec: decode frame: %w", ErrUnknownVersion)
	}
	f.ContextVersion = v

	if f.ChainID, err = r.hash(); err != nil {
		return f, fmt.Errorf("codec: decode frame: %w", err)
	}
	if f.Caller, err = r.hash(); err != nil {
		return f, fmt.Errorf("codec: decode frame: %w", err)
	}
	if f.ContractID, err = r.hash(); err != nil {
		return f, fmt.Errorf("codec: decode frame: %w", err)
	}
	if f.CodeHash, err = r.hash(); err != nil {
		return f, fmt.Errorf("codec: decode frame: %w", err)
	}
	if f.MomentumHeight, err = r.u64(); err != nil {
		return f, fmt.Errorf("codec: decode frame: %w", err)
	}
	if f.MomentumTime, err = r.u64(); err != nil {
		return f, fmt.Errorf("codec: decode frame: %w", err)
	}
	if f.InputIndex, err = r.u64(); err != nil {
		return f, fmt.Errorf("codec: decode frame: %w", err)
	}
	if f.DepositAsset, err = r.assetID(); err != nil {
		return f, fmt.Errorf("codec: decode frame: %w", err)
	}
	if f.DepositAmount, err = r.u256(); err != nil {
		return f, fmt.Errorf("codec: decode frame: %w", err)
	}
	if f.Payload, err = r.bytes(^uint32(0)); err != nil {
		return f, fmt.Errorf("codec: decode frame: %w", err)
	}
	if err := r.end(); err != nil {
		return f, fmt.Errorf("codec: decode frame: %w", err)
	}
	return f, nil
}
