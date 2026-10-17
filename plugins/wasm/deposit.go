package wasm

import (
	"github.com/zenon-network/go-zenon/common/types"

	"github.com/digitalSloth/go-executor/internal/codec"
)

// refund is the deposit settlement, run on every path: the remainder is
// deposit_amount − claimed (the caller passes a zero claim on any
// failure), a zero remainder emits no message, and otherwise exactly
// one runtime-emitted L1 withdrawal is placed at outboxIndex, the
// number of retained contract messages.
func (s settlement) refund(claimed codec.U256, outboxIndex uint32) []codec.OutboxMessage {
	remainder := subU256(s.DepositAmount, claimed)
	if remainder == (codec.U256{}) {
		return nil
	}
	return []codec.OutboxMessage{{
		SourceContract: types.Hash{},
		InputIndex:     s.GlobalInputIndex,
		OutboxIndex:    outboxIndex,
		Target:         codec.L1Withdrawal{RecipientL1: s.L1Sender, Asset: s.DepositAsset, Amount: remainder},
		Payload:        nil,
	}}
}

// subU256 subtracts b from a over the big-endian encoding with a borrow loop. Precondition
// b <= a, guaranteed by validate's claim-bounds step.
func subU256(a, b codec.U256) codec.U256 {
	var out codec.U256
	borrow := 0
	for i := len(a) - 1; i >= 0; i-- {
		d := int(a[i]) - int(b[i]) - borrow
		if d < 0 {
			d += 256
			borrow = 1
		} else {
			borrow = 0
		}
		out[i] = byte(d)
	}
	return out
}
