package ledger

import (
	"errors"
	"reflect"
	"testing"

	"github.com/zenon-network/go-zenon/common/types"

	"github.com/digitalSloth/go-executor/internal/codec"
)

type funcCaps struct {
	fn    func(call int, assets []codec.AssetID) ([]codec.U256, error)
	calls []capCall
}

func (c *funcCaps) MaxBatchWithdrawals(domainID uint32, height uint64, hash types.Hash, assets []codec.AssetID) ([]codec.U256, error) {
	c.calls = append(c.calls, capCall{domainID: domainID, height: height, hash: hash, assets: append([]codec.AssetID(nil), assets...)})
	return c.fn(len(c.calls), assets)
}

func TestBoundCapsMemo(t *testing.T) {
	a, b, d := codec.AssetID{1}, codec.AssetID{2}, codec.AssetID{3}
	h := testHash(9)
	fake := &funcCaps{fn: func(_ int, assets []codec.AssetID) ([]codec.U256, error) {
		out := make([]codec.U256, len(assets))
		for i, asset := range assets {
			out[i] = codec.U256{31: asset[0] * 10}
		}
		return out, nil
	}}
	c := &boundCaps{src: fake, domainID: 3, height: 7, hash: h}

	for _, tc := range []struct {
		assets []codec.AssetID
		want   []byte
	}{
		{[]codec.AssetID{a, b, a}, []byte{10, 20, 10}},
		{[]codec.AssetID{b, d}, []byte{20, 30}},
		{[]codec.AssetID{d, a}, []byte{30, 10}},
		{nil, nil},
	} {
		got, err := c.MaxBatchWithdrawals(tc.assets)
		if err != nil || len(got) != len(tc.want) {
			t.Fatalf("assets %v = %x, %v; want %v", tc.assets, got, err, tc.want)
		}
		for i, w := range tc.want {
			if got[i] != (codec.U256{31: w}) {
				t.Fatalf("assets %v: entry %d = %x, want %d", tc.assets, i, got[i], w)
			}
		}
	}
	want := []capCall{{3, 7, h, []codec.AssetID{a, b}}, {3, 7, h, []codec.AssetID{d}}}
	if !reflect.DeepEqual(fake.calls, want) {
		t.Fatalf("source calls = %+v, want %+v", fake.calls, want)
	}
}

func TestBoundCapsErrorNotMemoised(t *testing.T) {
	errFake := errors.New("fake: node down")
	fake := &funcCaps{fn: func(call int, assets []codec.AssetID) ([]codec.U256, error) {
		if call == 1 {
			return nil, errFake
		}
		return []codec.U256{{31: 4}}, nil
	}}
	c := &boundCaps{src: fake, domainID: 3, height: 7, hash: testHash(9)}
	a := []codec.AssetID{{1}}

	if _, err := c.MaxBatchWithdrawals(a); !errors.Is(err, ErrCapRead) || !errors.Is(err, errFake) {
		t.Fatalf("call 1 error = %v, want ErrCapRead wrapping errFake", err)
	}
	for i := 2; i <= 3; i++ {
		if got, err := c.MaxBatchWithdrawals(a); err != nil || len(got) != 1 || got[0] != (codec.U256{31: 4}) {
			t.Fatalf("call %d = %x, %v; want [4]", i, got, err)
		}
	}
	if len(fake.calls) != 2 {
		t.Fatalf("source calls = %d, want 2", len(fake.calls))
	}
}
