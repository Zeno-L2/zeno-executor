package ledger

import (
	"errors"
	"fmt"

	"github.com/zenon-network/go-zenon/common/types"

	"github.com/digitalSloth/go-executor/internal/codec"
)

// CapSource returns, for each asset in assets and in that order, the
// MaxBatchWithdrawal of domainID's AssetConfig in Settlement state at
// the end of the momentum (height, hash), or zero for an asset with no
// AssetConfig. assets holds 1 to codec.MaxAssetsPerBatch entries. An
// error wrapping recovery.ErrHalted is a consistency fault; any other
// error is an outage.
type CapSource interface {
	MaxBatchWithdrawals(domainID uint32, height uint64, hash types.Hash, assets []codec.AssetID) ([]codec.U256, error)
}

// ErrCapRead reports that a withdrawal-cap read failed. The cause stays wrapped: a cause
// wrapping recovery.ErrHalted is a fault, anything else an outage.
var ErrCapRead = errors.New("ledger: withdrawal cap read")

// boundCaps is the domain.CapLookup shared by every input one confirming momentum emitted:
// src bound to domainID and that momentum. It memoises answers, never errors. It is not safe
// for concurrent use; the runtime loop calls it from one goroutine. It sends every asset it has
// not memoised in one source call.
type boundCaps struct {
	src      CapSource
	domainID uint32
	height   uint64
	hash     types.Hash
	memo     map[codec.AssetID]codec.U256
}

func (c *boundCaps) MaxBatchWithdrawals(assets []codec.AssetID) ([]codec.U256, error) {
	var miss []codec.AssetID
	seen := map[codec.AssetID]bool{}
	for _, a := range assets {
		if _, ok := c.memo[a]; ok || seen[a] {
			continue
		}
		seen[a] = true
		miss = append(miss, a)
	}
	if len(miss) > 0 {
		vals, err := c.src.MaxBatchWithdrawals(c.domainID, c.height, c.hash, miss)
		if err != nil {
			return nil, fmt.Errorf("%w: domain %d at height %d: %w", ErrCapRead, c.domainID, c.height, err)
		}
		if c.memo == nil {
			c.memo = map[codec.AssetID]codec.U256{}
		}
		for i, a := range miss {
			c.memo[a] = vals[i]
		}
	}
	out := make([]codec.U256, len(assets))
	for i, a := range assets {
		out[i] = c.memo[a]
	}
	return out, nil
}
