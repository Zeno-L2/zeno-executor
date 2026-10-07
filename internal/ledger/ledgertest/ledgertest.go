// Package ledgertest builds Settlement input sends, answers Settlement's assignment for
// them over an in-memory chain and answers withdrawal caps, for tests outside internal/ledger.
package ledgertest

import (
	"fmt"

	"github.com/zenon-network/go-zenon/chain/nom"
	"github.com/zenon-network/go-zenon/common/types"
	"github.com/zenon-network/go-zenon/vm/embedded/definition"

	"github.com/digitalSloth/go-executor/internal/codec"
	"github.com/digitalSloth/go-executor/internal/ledger"
	"github.com/digitalSloth/go-executor/internal/recovery"
)

// SendData returns the ABI data of a Settlement CallL2 call for domainID carrying payload,
// with a zero target contract and a zero plasma limit.
func SendData(domainID uint32, payload []byte) []byte {
	return definition.ABISettlement.PackMethodPanic(definition.SettlementCallL2MethodName, domainID, types.Hash{}, uint64(0), payload)
}

// Assign stands in for Settlement's assignment over moms, which ascend from height 1. Every
// top-level block whose Data unpacks as a CallL2 call is an accepted send, received in the
// momentum that holds it. Global indices count every domain's sends and ordinals count
// domainID's, both from 0, in moms order and then AccountBlocks order.
func Assign(moms []*nom.DetailedMomentum, domainID uint32, height uint64, hash types.Hash, nextOrdinal uint64) ([]ledger.AssignmentEntry, error) {
	var (
		entries []ledger.AssignmentEntry
		global  uint64
		ordinal uint64
		found   bool
	)
	for _, dm := range moms {
		h := dm.Momentum.Height
		if h > height {
			break
		}
		if h == height {
			if dm.Momentum.Hash != hash {
				break
			}
			found = true
			if nextOrdinal != ordinal {
				return nil, fmt.Errorf("%w: ledgertest: next ordinal %d, Settlement's is %d at height %d", recovery.ErrHalted, nextOrdinal, ordinal, height)
			}
		}
		for _, b := range dm.AccountBlocks {
			var p struct {
				DomainId       uint32
				TargetContract types.Hash
				PlasmaLimit    uint64
				Payload        []byte
			}
			if err := definition.ABISettlement.UnpackMethod(&p, definition.SettlementCallL2MethodName, b.Data); err != nil {
				continue
			}
			if p.DomainId == domainID {
				if h == height {
					entries = append(entries, ledger.AssignmentEntry{
						Ordinal:                ordinal,
						GlobalInputIndex:       global,
						SourceAccountBlockHash: b.Hash,
						SendMomentumHeight:     height,
					})
				}
				ordinal++
			}
			global++
		}
	}
	if !found {
		return nil, fmt.Errorf("ledgertest: no momentum at height %d with hash %s", height, hash)
	}
	return entries, nil
}

// Caps is a ledger.CapSource that answers its own value for every domain, momentum and asset.
type Caps codec.U256

func (c Caps) MaxBatchWithdrawals(_ uint32, _ uint64, _ types.Hash, assets []codec.AssetID) ([]codec.U256, error) {
	out := make([]codec.U256, len(assets))
	for i := range out {
		out[i] = codec.U256(c)
	}
	return out, nil
}
