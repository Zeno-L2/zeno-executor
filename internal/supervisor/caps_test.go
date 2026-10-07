package supervisor

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/zenon-network/go-zenon/common/types"

	"github.com/digitalSloth/go-executor/internal/codec"
)

// countingCaps answers a value derived from the key and counts its calls. failFirst, when set,
// fails the first call only.
type countingCaps struct {
	calls     atomic.Int32
	failFirst bool
}

var errCapSource = errors.New("fake: node down")

func (c *countingCaps) MaxBatchWithdrawal(domainID uint32, height uint64, hash types.Hash, asset codec.AssetID) (codec.U256, error) {
	if n := c.calls.Add(1); c.failFirst && n == 1 {
		return codec.U256{}, errCapSource
	}
	return codec.U256{28: byte(domainID), 29: hash[0], 30: asset[0], 31: byte(height)}, nil
}

func wantCapValue(domainID uint32, height uint64, hash types.Hash, asset codec.AssetID) codec.U256 {
	return codec.U256{28: byte(domainID), 29: hash[0], 30: asset[0], 31: byte(height)}
}

func TestCapCacheHit(t *testing.T) {
	src := &countingCaps{}
	c := NewCapCache(src)
	h, a := types.Hash{1}, codec.AssetID{2}
	for i := 0; i < 2; i++ {
		got, err := c.MaxBatchWithdrawal(1, 5, h, a)
		if err != nil || got != wantCapValue(1, 5, h, a) {
			t.Fatalf("call %d = %x, %v", i, got, err)
		}
	}
	if n := src.calls.Load(); n != 1 {
		t.Fatalf("source calls = %d, want 1", n)
	}
}

func TestCapCacheKey(t *testing.T) {
	src := &countingCaps{}
	c := NewCapCache(src)
	type key struct {
		domainID uint32
		height   uint64
		hash     types.Hash
		asset    codec.AssetID
	}
	base := key{1, 5, types.Hash{1}, codec.AssetID{2}}
	keys := []key{base, base, base, base, base}
	keys[0].domainID = 2
	keys[1].asset = codec.AssetID{3}
	keys[2].height = 6
	keys[3].hash = types.Hash{4}
	keys = append(keys, base)
	for _, k := range keys {
		got, err := c.MaxBatchWithdrawal(k.domainID, k.height, k.hash, k.asset)
		if err != nil || got != wantCapValue(k.domainID, k.height, k.hash, k.asset) {
			t.Fatalf("key %+v = %x, %v", k, got, err)
		}
	}
	if n := src.calls.Load(); n != 5 {
		t.Fatalf("source calls = %d, want 5", n)
	}
}

func TestCapCacheBatch(t *testing.T) {
	a, b := codec.AssetID{2}, codec.AssetID{3}
	h := types.Hash{1}
	check := func(t *testing.T, got []codec.U256, err error) {
		t.Helper()
		if err != nil || len(got) != 3 {
			t.Fatalf("batch = %x, %v", got, err)
		}
		for i, asset := range []codec.AssetID{a, b, a} {
			if got[i] != wantCapValue(1, 5, h, asset) {
				t.Fatalf("entry %d = %x, want %x", i, got[i], wantCapValue(1, 5, h, asset))
			}
		}
	}

	src := &countingCaps{}
	c := NewCapCache(src)
	got, err := c.MaxBatchWithdrawals(1, 5, h, []codec.AssetID{a, b, a})
	check(t, got, err)
	if n := src.calls.Load(); n != 2 {
		t.Fatalf("source calls = %d, want 2", n)
	}

	src = &countingCaps{failFirst: true}
	c = NewCapCache(src)
	if _, err := c.MaxBatchWithdrawals(1, 5, h, []codec.AssetID{a, b, a}); !errors.Is(err, errCapSource) {
		t.Fatalf("batch 1 = %v, want the source error", err)
	}
	got, err = c.MaxBatchWithdrawals(1, 5, h, []codec.AssetID{a, b, a})
	check(t, got, err)
	if n := src.calls.Load(); n != 3 {
		t.Fatalf("source calls = %d, want 3", n)
	}
}

func TestCapCacheErrorsNotCached(t *testing.T) {
	src := &countingCaps{failFirst: true}
	c := NewCapCache(src)
	h, a := types.Hash{1}, codec.AssetID{2}
	if _, err := c.MaxBatchWithdrawal(1, 5, h, a); !errors.Is(err, errCapSource) {
		t.Fatalf("call 1 = %v, want the source error", err)
	}
	for i := 2; i <= 3; i++ {
		if got, err := c.MaxBatchWithdrawal(1, 5, h, a); err != nil || got != wantCapValue(1, 5, h, a) {
			t.Fatalf("call %d = %x, %v", i, got, err)
		}
	}
	if n := src.calls.Load(); n != 2 {
		t.Fatalf("source calls = %d, want 2", n)
	}
}

func TestCapCacheEvictsOldest(t *testing.T) {
	src := &countingCaps{}
	c := NewCapCache(src)
	h, a := types.Hash{1}, codec.AssetID{2}
	for height := uint64(1); height <= capCacheSize+1; height++ {
		if _, err := c.MaxBatchWithdrawal(1, height, h, a); err != nil {
			t.Fatalf("height %d: %v", height, err)
		}
	}
	before := src.calls.Load()
	if _, err := c.MaxBatchWithdrawal(1, 2, h, a); err != nil || src.calls.Load() != before {
		t.Fatalf("height 2: err %v, source calls %d -> %d; want a hit", err, before, src.calls.Load())
	}
	if _, err := c.MaxBatchWithdrawal(1, 1, h, a); err != nil || src.calls.Load() != before+1 {
		t.Fatalf("height 1: err %v, source calls %d -> %d; want one miss", err, before, src.calls.Load())
	}
}

func TestCapCacheConcurrent(t *testing.T) {
	c := NewCapCache(&countingCaps{})
	h, a := types.Hash{1}, codec.AssetID{2}
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				height := uint64(i%4 + 1)
				got, err := c.MaxBatchWithdrawal(1, height, h, a)
				if err != nil || got != wantCapValue(1, height, h, a) {
					t.Errorf("height %d = %x, %v", height, got, err)
					return
				}
			}
		}()
	}
	wg.Wait()
}
