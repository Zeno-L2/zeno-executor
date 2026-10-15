package settlement

import (
	"errors"
	"fmt"

	"github.com/zenon-network/go-zenon/chain/nom"
	"github.com/zenon-network/go-zenon/common/types"

	"github.com/digitalSloth/go-executor/internal/codec"
	"github.com/digitalSloth/go-executor/internal/recovery"
)

// MaxBatchWithdrawal proves the domain's AssetConfig for asset against the state root of the
// momentum at height, which must hash to hash. supervisor.CapCache caches its answers per asset.
func (r *Reader) MaxBatchWithdrawal(domainID uint32, height uint64, hash types.Hash, asset codec.AssetID) (codec.U256, error) {
	ms, err := r.c.DetailedMomentumsByHeight(height, 1)
	if err != nil {
		return codec.U256{}, err
	}
	m, err := checkedMomentum(ms, height)
	if err != nil {
		return codec.U256{}, err
	}
	if m.Hash != hash {
		return codec.U256{}, fmt.Errorf("settlement: node's momentum at height %d is %s, not %s", height, m.Hash, hash)
	}
	if m.Version < nom.StateRootMomentumVersion {
		return codec.U256{}, fmt.Errorf("settlement: StateRootSpork is not active at height %d (momentum version %d)", height, m.Version)
	}
	// Build the view from this momentum: a second fetch could return a different momentum.
	v := &View{Height: height, c: r.c, root: m.StateRoot, time: m.TimestampUnix}

	cfg, ok, err := v.AssetConfig(domainID, types.Hash(asset))
	if err != nil {
		if errors.Is(err, recovery.ErrHalted) {
			if uerr := r.unchanged(height, hash); uerr != nil {
				return codec.U256{}, uerr
			}
		}
		return codec.U256{}, err
	}
	if !ok {
		return codec.U256{}, nil
	}
	amount, err := codec.U256FromBig(cfg.MaxBatchWithdrawal)
	if err != nil {
		return codec.U256{}, halted(height, "domain %d asset %x MaxBatchWithdrawal does not fit 256 bits", domainID, asset)
	}
	return amount, nil
}
