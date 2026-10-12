package batch

import (
	"fmt"

	"github.com/zenon-network/go-zenon/common/types"

	"github.com/digitalSloth/go-executor/internal/codec"
)

// Withdrawal is one kind 1 outbox message of a batch with its inclusion
// proof.
type Withdrawal struct {
	// Message is the canonical OutboxMessage encoding: the outbox leaf.
	Message []byte
	// Proof is the §27.3 MerkleProof of Message under the batch's
	// OutboxRoot.
	Proof []byte
}

// Withdrawals rebuilds a batch's full outbox leaf list from its stored
// bundle, checks that the leaves are strictly ascending by (InputIndex,
// OutboxIndex) and that their Merkle root equals outboxRoot, and
// returns every kind 1 leaf with its proof, in leaf order. Kind 0
// leaves sit in the tree but are never returned. A batch with no kind 1
// leaf returns nil.
func Withdrawals(bundle []byte, outboxRoot types.Hash) ([]Withdrawal, error) {
	records, err := codec.DecodeBundle(bundle)
	if err != nil {
		return nil, fmt.Errorf("batch: withdrawals: %w", err)
	}

	var (
		leaves      [][]byte
		withdrawals []int
		havePrev    bool
		prevInput   uint64
		prevOutbox  uint32
	)
	for _, rec := range records {
		res, err := codec.DecodeExecutionResult(rec.ExecutionResult)
		if err != nil {
			return nil, fmt.Errorf("batch: withdrawals: input %d: %w", rec.GlobalInputIndex, err)
		}
		for _, m := range res.Outbox {
			if havePrev && (m.InputIndex < prevInput || (m.InputIndex == prevInput && m.OutboxIndex <= prevOutbox)) {
				return nil, fmt.Errorf("batch: withdrawals: outbox message (%d, %d) is not after (%d, %d)",
					m.InputIndex, m.OutboxIndex, prevInput, prevOutbox)
			}
			havePrev, prevInput, prevOutbox = true, m.InputIndex, m.OutboxIndex

			enc, err := m.Encode()
			if err != nil {
				return nil, fmt.Errorf("batch: withdrawals: encode outbox message (%d, %d): %w", m.InputIndex, m.OutboxIndex, err)
			}
			if m.Target.Kind() == codec.KindL1Withdrawal {
				withdrawals = append(withdrawals, len(leaves))
			}
			leaves = append(leaves, enc)
		}
	}

	if got := codec.MerkleRoot(leaves); got != outboxRoot {
		return nil, fmt.Errorf("batch: withdrawals: outbox root %s does not match the commitment's %s", got, outboxRoot)
	}
	if len(withdrawals) == 0 {
		return nil, nil
	}

	proofs := codec.MerkleProofs(leaves)
	out := make([]Withdrawal, len(withdrawals))
	for i, pos := range withdrawals {
		out[i] = Withdrawal{Message: leaves[pos], Proof: proofs[pos]}
	}
	return out, nil
}
