package wasm

import (
	"bytes"
	"fmt"
	"sort"

	"github.com/zenon-network/go-zenon/common/types"

	"github.com/digitalSloth/go-executor/domain"
	"github.com/digitalSloth/go-executor/internal/codec"
	"github.com/digitalSloth/go-executor/plugins/wasm/internal/engine"
	"github.com/digitalSloth/go-executor/plugins/wasm/internal/module"
)

// settlement is everything the ordered validation steps, the gas settlement and the deposit
// settlement need beyond the engine's Outcome. One value is built per kind-0/1 input.
type settlement struct {
	ContractID       types.Hash
	CodeHash         types.Hash
	GlobalInputIndex uint64
	L1Sender         types.Address
	DepositAsset     codec.AssetID
	DepositAmount    codec.U256
	GasBudget        uint64 // plasma_limit, clamped to MaxInt64 (D6, D24)
}

// settle maps a completed Outcome onto the §15 status, the gas_used that status defines, the
// §7.5 abort code where the status is REVERT, and the Effects the runtime commits. Its error
// return means halt.
func (s settlement) settle(out engine.Outcome) (domain.Effects, error) {
	switch out.Kind {
	case engine.OutOfGas:
		return s.fail(codec.StatusOutOfGas, s.GasBudget), nil
	case engine.Trapped:
		return s.fail(codec.StatusRuntimeFault, out.GasUsed), nil
	case engine.Aborted:
		eff := s.fail(codec.StatusRevert, out.GasUsed)
		eff.AbortCode = out.AbortCode
		return eff, nil
	case engine.Returned:
		if out.Ret < 0 {
			eff := s.fail(codec.StatusRevert, out.GasUsed)
			eff.AbortCode = int32(-out.Ret)
			return eff, nil
		}
		return s.validate(out)
	default:
		return domain.Effects{}, fmt.Errorf("wasm: settle: unknown outcome kind %d", out.Kind)
	}
}

// validate runs the ordered validation steps over a returned blob (Outcome.Ret >= 0).
func (s settlement) validate(out engine.Outcome) (domain.Effects, error) {
	metered := out.GasUsed
	remaining := s.GasBudget - metered

	// Step 1: decode the returned blob.
	eff, err := codec.DecodeContractEffects(out.ResultBlob)
	if err != nil {
		return s.fail(codec.StatusRuntimeFault, metered), nil
	}

	// Step 2: the blob must claim the input index it was executed under.
	if eff.GlobalInputIndex != s.GlobalInputIndex {
		return s.fail(codec.StatusRuntimeFault, metered), nil
	}

	// Step 2a (D27): every event and outbox message must be attributed to the executing
	// contract.
	for _, e := range eff.Events {
		if e.ContractID != s.ContractID {
			return s.fail(codec.StatusRuntimeFault, metered), nil
		}
	}
	for _, m := range eff.Outbox {
		if m.SourceContract != s.ContractID || m.SourceContract == (types.Hash{}) {
			return s.fail(codec.StatusRuntimeFault, metered), nil
		}
	}

	// Step 3: reserved-key and duplicate-derived-key scan, in blob order.
	derived := make([]domain.StateDiffEntry, 0, len(eff.StateDiff))
	seen := make(map[domain.Path]bool, len(eff.StateDiff))
	for _, e := range eff.StateDiff {
		if reserved(e.LocalKey) {
			return s.fail(codec.StatusUnauthorized, metered), nil
		}
		path := derive(s.ContractID, e.LocalKey)
		if seen[path] {
			return s.fail(codec.StatusRuntimeFault, metered), nil
		}
		seen[path] = true
		derived = append(derived, domain.StateDiffEntry{Path: path, Value: e.Value})
	}

	// Step 4: claim bounds.
	if bytes.Compare(eff.ClaimedDeposit[:], s.DepositAmount[:]) > 0 {
		return s.fail(codec.StatusRuntimeFault, metered), nil
	}

	// Step 5: outbox capacity for the refund.
	remainder := subU256(s.DepositAmount, eff.ClaimedDeposit)
	if remainder != (codec.U256{}) && len(eff.Outbox) >= codec.MaxOutboxPerExecution {
		return s.fail(codec.StatusRuntimeFault, metered), nil
	}

	// Step 6: the §10.4 post-execution effect charge.
	charge := effectCharge(eff)
	if remaining < charge {
		return s.fail(codec.StatusOutOfGas, s.GasBudget), nil
	}
	gasUsed := metered + charge

	// Step 7: return-data size.
	if len(eff.ReturnData) > MaxReturnData {
		return s.fail(codec.StatusResultTooLarge, gasUsed), nil
	}

	// Step 8: final-message-set size.
	derived = sortStateDiff(derived)

	outbox := append([]codec.OutboxMessage(nil), eff.Outbox...)
	if remainder != (codec.U256{}) {
		outbox = append(outbox, s.refund(eff.ClaimedDeposit, uint32(len(eff.Outbox)))...)
	}

	result := codec.ExecutionResult{
		Version:          codec.ExecutionResultVersion,
		GlobalInputIndex: s.GlobalInputIndex,
		StateDiff:        toCodecStateDiff(derived),
		Events:           eff.Events,
		Outbox:           outbox,
		ReturnData:       eff.ReturnData,
		ClaimedDeposit:   eff.ClaimedDeposit,
	}
	encoded, err := result.Encode()
	if err != nil {
		return domain.Effects{}, err
	}
	if len(encoded) > MaxExecutionResult {
		return s.fail(codec.StatusResultTooLarge, gasUsed), nil
	}

	// Step 9: success.
	return domain.Effects{
		Status:         codec.StatusSuccess,
		GasUsed:        gasUsed,
		ContractID:     s.ContractID,
		CodeHash:       s.CodeHash,
		ClaimedDeposit: eff.ClaimedDeposit,
		StateDiff:      derived,
		Events:         eff.Events,
		OutboxMessages: outbox,
		ReturnData:     eff.ReturnData,
	}, nil
}

// sortStateDiff orders entries ascending by derived path in place, as §27.2's canonical StateDiff
// ordering requires, and returns them.
func sortStateDiff(entries []domain.StateDiffEntry) []domain.StateDiffEntry {
	sort.Slice(entries, func(i, j int) bool {
		return bytes.Compare(entries[i].Path[:], entries[j].Path[:]) < 0
	})
	return entries
}

// toCodecStateDiff projects derived state-diff entries onto codec.StateDiffEntry, preserving
// order.
func toCodecStateDiff(entries []domain.StateDiffEntry) []codec.StateDiffEntry {
	out := make([]codec.StateDiffEntry, len(entries))
	for i, e := range entries {
		out[i] = codec.StateDiffEntry{Key: e.Path, Value: e.Value}
	}
	return out
}

// fail assembles the discard-all-but-refund shape: empty StateDiff, no events, no contract
// outbox, zero ClaimedDeposit, empty ReturnData, and the full-deposit refund (if any) at
// outbox_index 0 (§14.3, §15, §18.5).
func (s settlement) fail(status codec.Status, gasUsed uint64) domain.Effects {
	return domain.Effects{
		Status:         status,
		GasUsed:        gasUsed,
		ContractID:     s.ContractID,
		CodeHash:       s.CodeHash,
		ClaimedDeposit: codec.U256{},
		StateDiff:      nil,
		Events:         nil,
		OutboxMessages: s.refund(codec.U256{}, 0),
		ReturnData:     nil,
	}
}

// effectCharge is the §10.4 post-execution charge over the contract's declared effects: one per
// StateDiff entry by value bytes (a deletion charges base only), one per event by data bytes,
// one per contract outbox message by payload bytes, and one for ReturnData, charged
// unconditionally including when it is empty. The runtime-emitted refund is not charged (D16).
func effectCharge(eff codec.ContractEffects) uint64 {
	var total uint64
	for _, e := range eff.StateDiff {
		total += module.EffectCost(module.Effect{Kind: module.EffectStateDiff, Bytes: len(e.Value)})
	}
	for _, e := range eff.Events {
		total += module.EffectCost(module.Effect{Kind: module.EffectEvent, Bytes: len(e.Data)})
	}
	for _, m := range eff.Outbox {
		total += module.EffectCost(module.Effect{Kind: module.EffectOutboxMessage, Bytes: len(m.Payload)})
	}
	total += module.EffectCost(module.Effect{Kind: module.EffectReturnData, Bytes: len(eff.ReturnData)})
	return total
}
