package codec

import (
	"errors"
	"testing"

	"github.com/zenon-network/go-zenon/common/types"
)

func TestAssetIDRoundTrip(t *testing.T) {
	cases := []types.ZenonTokenStandard{
		types.ZnnTokenStandard,
		types.QsrTokenStandard,
		types.ZeroTokenStandard,
	}
	for _, zts := range cases {
		a := AssetIDFromZTS(zts)
		if err := a.Validate(); err != nil {
			t.Fatalf("Validate(%x): %v", a, err)
		}
		got, err := a.ZTS()
		if err != nil {
			t.Fatalf("ZTS(): %v", err)
		}
		if got != zts {
			t.Errorf("got %x, want %x", got, zts)
		}
	}
}

func TestAssetIDRejectsNonCanonical(t *testing.T) {
	t.Run("non-zero high byte", func(t *testing.T) {
		a := AssetIDFromZTS(types.ZnnTokenStandard)
		a[0] = 0x01
		if err := a.Validate(); !errors.Is(err, ErrInvalidAssetID) {
			t.Fatalf("got %v, want ErrInvalidAssetID", err)
		}
		if _, err := a.ZTS(); !errors.Is(err, ErrInvalidAssetID) {
			t.Fatalf("ZTS() got %v, want ErrInvalidAssetID", err)
		}
	})

	t.Run("zts bytes at wrong alignment", func(t *testing.T) {
		var a AssetID
		copy(a[0:10], types.ZnnTokenStandard[:])
		if err := a.Validate(); !errors.Is(err, ErrInvalidAssetID) {
			t.Fatalf("got %v, want ErrInvalidAssetID", err)
		}
	})
}
