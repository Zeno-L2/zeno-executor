package wasm

import (
	"encoding/binary"
	"fmt"
	"math"

	"github.com/zenon-network/go-zenon/common/types"

	"github.com/Zeno-L2/zeno-executor/domain"
	"github.com/Zeno-L2/zeno-executor/internal/codec"
)

// quotaLeafLen is the width of the quota leaf: purchased:u64 || used:u64.
const quotaLeafLen = 16

// decodeQuota splits the quota leaf into purchased and used bytes. An absent leaf is 0/0; a
// present leaf of any other width than 16 is an error, which callers treat as a halt.
func decodeQuota(leaf []byte, present bool) (purchased, used uint64, err error) {
	if !present {
		return 0, 0, nil
	}
	if len(leaf) != quotaLeafLen {
		return 0, 0, fmt.Errorf("wasm: quota leaf has width %d, want %d", len(leaf), quotaLeafLen)
	}
	return binary.BigEndian.Uint64(leaf[:8]), binary.BigEndian.Uint64(leaf[8:]), nil
}

// encodeQuota is the 16-byte quota leaf: purchased || used, both big-endian.
func encodeQuota(purchased, used uint64) []byte {
	leaf := make([]byte, quotaLeafLen)
	binary.BigEndian.PutUint64(leaf[:8], purchased)
	binary.BigEndian.PutUint64(leaf[8:], used)
	return leaf
}

// leafSize is the quota cost of one present leaf: StateLeafOverhead plus its value length.
func leafSize(length int) int64 {
	return StateLeafOverhead + int64(length)
}

// chargeQuota is step 6a. It sums the net change in counted leaf bytes
// across the contract's derived entries and applies it to the quota
// leaf. It returns the diff with the updated quota entry appended, or
// ok == false when the change exceeds the purchased quota. A net change
// of zero reads no quota leaf and adds no entry. An error means halt.
func (s settlement) chargeQuota(diff []domain.StateDiffEntry) ([]domain.StateDiffEntry, bool, error) {
	var delta int64
	for _, e := range diff {
		if e.Value != nil {
			delta += leafSize(len(e.Value))
		}
		length, present, err := s.State.Len(e.Path)
		if err != nil {
			return nil, false, fmt.Errorf("wasm: charge quota: read leaf length: %w", err)
		}
		if present {
			delta -= leafSize(length)
		}
	}
	if delta == 0 {
		return diff, true, nil
	}

	quotaPath := derive(s.ContractID, keyQuota)
	leaf, present, err := s.State.Read(quotaPath)
	if err != nil {
		return nil, false, fmt.Errorf("wasm: charge quota: read quota: %w", err)
	}
	purchased, used, err := decodeQuota(leaf, present)
	if err != nil {
		return nil, false, err
	}

	var newUsed uint64
	if delta > 0 {
		grow := uint64(delta)
		if grow > math.MaxUint64-used || used+grow > purchased {
			return nil, false, nil
		}
		newUsed = used + grow
	} else {
		shrink := uint64(-delta)
		if shrink > used {
			return nil, false, fmt.Errorf("wasm: charge quota: freed %d bytes exceeds used %d", shrink, used)
		}
		newUsed = used - shrink
	}
	return append(diff, domain.StateDiffEntry{Path: quotaPath, Value: encodeQuota(purchased, newUsed)}), true, nil
}

// applyBuyStorage serves input_kind 7: it adds args.Bytes to the target
// contract's purchased quota.
func (p *Plugin) applyBuyStorage(state domain.StateAccess, args codec.BuyStorageArgs) (domain.Effects, error) {
	// Unconditional, in this order, before any branch: the witness set is the same on every
	// outcome.
	codeHashLeaf, codeHashPresent, err := state.Read(derive(args.ContractID, keyCodeHash))
	if err != nil {
		return domain.Effects{}, fmt.Errorf("wasm: apply buy storage: read code hash: %w", err)
	}
	quotaLeaf, quotaPresent, err := state.Read(derive(args.ContractID, keyQuota))
	if err != nil {
		return domain.Effects{}, fmt.Errorf("wasm: apply buy storage: read quota: %w", err)
	}

	if codeHashPresent && len(codeHashLeaf) != len(types.Hash{}) {
		return domain.Effects{}, fmt.Errorf("wasm: apply buy storage: code hash leaf has width %d, want %d", len(codeHashLeaf), len(types.Hash{}))
	}
	purchased, used, err := decodeQuota(quotaLeaf, quotaPresent)
	if err != nil {
		return domain.Effects{}, fmt.Errorf("wasm: apply buy storage: %w", err)
	}

	if !codeHashPresent {
		return domain.Effects{Status: codec.StatusValidationFailed, ContractID: args.ContractID}, nil
	}
	var current types.Hash
	copy(current[:], codeHashLeaf)

	if purchased > math.MaxUint64-args.Bytes {
		return domain.Effects{Status: codec.StatusValidationFailed, ContractID: args.ContractID, CodeHash: current}, nil
	}

	return domain.Effects{
		Status:     codec.StatusSuccess,
		GasUsed:    0,
		ContractID: args.ContractID,
		CodeHash:   current,
		StateDiff:  []domain.StateDiffEntry{{Path: derive(args.ContractID, keyQuota), Value: encodeQuota(purchased+args.Bytes, used)}},
	}, nil
}
