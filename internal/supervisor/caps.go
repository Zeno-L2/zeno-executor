package supervisor

import (
	"sync"

	"github.com/zenon-network/go-zenon/common/types"

	"github.com/digitalSloth/go-executor/internal/codec"
)

// capCacheSize bounds CapCache: one worker's worst-case replay after a restart is
// commitInputInterval (1024) inputs, each naming at most MaxAssetsPerBatch (16) assets.
const capCacheSize = 1 << 14

type capKey struct {
	domainID uint32
	asset    codec.AssetID
	height   uint64
	hash     types.Hash
}

// AssetCapSource proves one asset's withdrawal cap at a momentum: the per-asset read CapCache
// caches. *settlement.Reader implements it.
type AssetCapSource interface {
	MaxBatchWithdrawal(domainID uint32, height uint64, hash types.Hash, asset codec.AssetID) (codec.U256, error)
}

// CapCache is a ledger.CapSource that answers each asset from a fixed-size cache in front of src. A
// cached answer is proven state at one momentum hash, so it never goes stale. It caches
// answers only, never errors, and evicts the oldest entry when full. It is safe for
// concurrent use by every worker's serve goroutine.
type CapCache struct {
	src   AssetCapSource
	mu    sync.Mutex
	vals  map[capKey]codec.U256
	order []capKey // insertion ring; order[next] is the oldest once full
	next  int
}

func NewCapCache(src AssetCapSource) *CapCache {
	return &CapCache{src: src, vals: map[capKey]codec.U256{}}
}

// MaxBatchWithdrawals implements ledger.CapSource. It answers each asset in order through
// MaxBatchWithdrawal, so each miss is one proof read, and returns the first error unchanged.
func (c *CapCache) MaxBatchWithdrawals(domainID uint32, height uint64, hash types.Hash, assets []codec.AssetID) ([]codec.U256, error) {
	out := make([]codec.U256, len(assets))
	for i, a := range assets {
		v, err := c.MaxBatchWithdrawal(domainID, height, hash, a)
		if err != nil {
			return nil, err
		}
		out[i] = v
	}
	return out, nil
}

func (c *CapCache) MaxBatchWithdrawal(domainID uint32, height uint64, hash types.Hash, asset codec.AssetID) (codec.U256, error) {
	k := capKey{domainID: domainID, asset: asset, height: height, hash: hash}
	c.mu.Lock()
	if v, ok := c.vals[k]; ok {
		c.mu.Unlock()
		return v, nil
	}
	c.mu.Unlock()

	v, err := c.src.MaxBatchWithdrawal(domainID, height, hash, asset)
	if err != nil {
		return codec.U256{}, err
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if have, ok := c.vals[k]; ok {
		return have, nil
	}
	if len(c.order) < capCacheSize {
		c.order = append(c.order, k)
	} else {
		delete(c.vals, c.order[c.next])
		c.order[c.next] = k
		c.next = (c.next + 1) % capCacheSize
	}
	c.vals[k] = v
	return v, nil
}
