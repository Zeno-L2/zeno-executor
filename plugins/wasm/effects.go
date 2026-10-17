package wasm

import (
	"bytes"
	"fmt"
	"math/big"
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
	GasBudget        uint64 // the input's plasma_limit, one budget unit per unit
	State            domain.StateAccess
	Caps             domain.CapLookup // the input's withdrawal-cap lookup, read by step 5a
}

// settle maps a completed Outcome onto the receipt status, the gas_used
// that status defines, the abort code where the status is REVERT, and
// the Effects the runtime commits. Its error return means halt.
func (s settlement) settle(out engine.Outcome) (domain.Effects, error) {
	switch out.Kind {
	case engine.OutOfGas:
		return s.fail(codec.StatusOutOfGas, s.GasBudget), nil
	case engine.Trapped:
		return s.fail(codec.StatusRuntimeFault, out.GasUsed), nil
	case engine.Aborted:
		eff := s.fail(codec.StatusRevert, out.GasUsed)
		eff.AbortCode = out.AbortCode
		eff.ReturnData = out.RevertData
		return eff, nil
	case engine.AbortTooLarge:
		return s.fail(codec.StatusResultTooLarge, out.GasUsed), nil
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

	// Step 0: bound the blob before anything reads it. Decoding and the per-entry key
	// derivation in step 3 are both proportional to the blob's length, so the length is
	// checked ahead of every check on its contents: an over-long blob is RESULT_TOO_LARGE
	// whether or not it is also malformed.
	if len(out.ResultBlob) > MaxResultBlob {
		return s.fail(codec.StatusResultTooLarge, metered), nil
	}

	// Step 1: decode the returned blob.
	eff, err := codec.DecodeContractEffects(out.ResultBlob)
	if err != nil {
		return s.fail(codec.StatusRuntimeFault, metered), nil
	}

	// Step 2: the blob must claim the input index it was executed under.
	if eff.GlobalInputIndex != s.GlobalInputIndex {
		return s.fail(codec.StatusRuntimeFault, metered), nil
	}

	// Step 2a: every event and outbox message must be attributed to the
	// executing contract.
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

	// Step 5: outbox capacity for the refund. The refund occupies the
	// 33rd slot, so the bound is the ExecutionResult's
	// MaxOutboxPerResult, not the contract's MaxOutboxPerExecution.
	// Step 1's decode already bounds the contract's own messages at
	// MaxOutboxPerExecution, so this cannot fail today; it is kept
	// because it mirrors the MaxOutboxPerResult >=
	// MaxOutboxPerExecution + 1 invariant that result.Encode()'s own
	// bound depends on below. If that invariant ever breaks, a
	// contract-triggered Encode failure at the end of settle becomes
	// this step's RUNTIME_FAULT instead of an uncontrolled halt.
	remainder := subU256(s.DepositAmount, eff.ClaimedDeposit)
	if remainder != (codec.U256{}) && len(eff.Outbox)+1 > codec.MaxOutboxPerResult {
		return s.fail(codec.StatusRuntimeFault, metered), nil
	}

	// Step 5a: the per-asset withdrawal cap applies to this input's
	// contract withdrawals, summed per asset. The caps are the input's
	// Caps answers, read in one call after the footprint check
	// (Settlement's MaxBatchWithdrawal at the momentum confirming the
	// receive, 0 for an asset with no AssetConfig). The deposit amount
	// is not counted, so step 5a never limits a deposit's size; the
	// deposit asset counts toward the asset footprint because every
	// deposit adds a depositCredit to the batch's AssetFlowSummary.
	totals := map[codec.AssetID]*big.Int{}
	for _, m := range eff.Outbox {
		w, ok := m.Target.(codec.L1Withdrawal)
		if !ok {
			continue
		}
		total, ok := totals[w.Asset]
		if !ok {
			total = new(big.Int)
			totals[w.Asset] = total
		}
		total.Add(total, new(big.Int).SetBytes(w.Amount[:]))
	}
	footprint := len(totals)
	if _, ok := totals[s.DepositAsset]; s.DepositAmount != (codec.U256{}) && !ok {
		footprint++
	}
	if footprint > codec.MaxAssetsPerBatch {
		return s.fail(codec.StatusRuntimeFault, metered), nil
	}
	assets := make([]codec.AssetID, 0, len(totals))
	for a := range totals {
		assets = append(assets, a)
	}
	sort.Slice(assets, func(i, j int) bool { return bytes.Compare(assets[i][:], assets[j][:]) < 0 })
	if len(assets) > 0 {
		if s.Caps == nil {
			return domain.Effects{}, fmt.Errorf("wasm: step 5a: input %d withdraws with no cap lookup", s.GlobalInputIndex)
		}
		caps, err := s.Caps.MaxBatchWithdrawals(assets)
		if err != nil {
			return domain.Effects{}, fmt.Errorf("wasm: step 5a: withdrawal caps: %w", err)
		}
		for i, a := range assets {
			if totals[a].Cmp(new(big.Int).SetBytes(caps[i][:])) > 0 {
				return s.fail(codec.StatusRuntimeFault, metered), nil
			}
		}
	}

	// Step 6: the post-execution effect charge.
	charge := effectCharge(eff)
	if remaining < charge {
		return s.fail(codec.StatusOutOfGas, s.GasBudget), nil
	}
	gasUsed := metered + charge

	// Step 6a: charge the net growth of the contract's leaves against
	// its storage quota, and add the updated quota leaf to the diff.
	derived, quotaOK, err := s.chargeQuota(derived)
	if err != nil {
		return domain.Effects{}, err
	}
	if !quotaOK {
		return s.fail(codec.StatusStorageQuotaExceeded, gasUsed), nil
	}

	// Step 7: return-data size.
	if len(eff.ReturnData) > codec.MaxReturnData {
		return s.fail(codec.StatusResultTooLarge, gasUsed), nil
	}

	// Assemble the canonical state diff and the final message set.
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
	// Step 8: final-message-set size.
	if len(encoded) > MaxExecutionResult {
		return s.fail(codec.StatusResultTooLarge, gasUsed), nil
	}

	// Every step passed.
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

// sortStateDiff orders entries ascending by derived path in place, as
// the canonical StateDiff ordering requires, and returns them.
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

// fail assembles the discard-all-but-refund shape: empty StateDiff, no
// events, no contract outbox, zero ClaimedDeposit, empty ReturnData,
// and the full-deposit refund (if any) at outbox_index 0.
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

// effectCharge is the post-execution charge over the contract's
// declared effects: one per StateDiff entry by value bytes (a deletion
// charges base only), one per event by data bytes, one per contract
// outbox message by payload bytes, and one for ReturnData, charged
// unconditionally including when it is empty. The runtime-emitted
// refund is not charged.
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
