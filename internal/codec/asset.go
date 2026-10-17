package codec

import (
	"fmt"

	"github.com/zenon-network/go-zenon/common/types"
)

// AssetID is the canonical 32-byte encoding of a 10-byte ZTS
// identifier: bytes [0:22] are zero, and bytes [22:32] are the ZTS
// identifier in its native byte order. The all-zero AssetID is valid:
// it is the "no deposit" sentinel and maps to types.ZeroTokenStandard.
type AssetID [32]byte

// AssetIDFromZTS encodes zts into the canonical AssetID alignment: the ZTS bytes at [22:32],
// zero elsewhere.
func AssetIDFromZTS(zts types.ZenonTokenStandard) AssetID {
	var a AssetID
	copy(a[32-types.ZenonTokenStandardSize:], zts[:])
	return a
}

// ZTS decodes a into a ZenonTokenStandard, returning ErrInvalidAssetID if a is not the
// canonical alignment.
func (a AssetID) ZTS() (types.ZenonTokenStandard, error) {
	if err := a.Validate(); err != nil {
		return types.ZenonTokenStandard{}, err
	}
	var zts types.ZenonTokenStandard
	copy(zts[:], a[32-types.ZenonTokenStandardSize:])
	return zts, nil
}

// Validate returns ErrInvalidAssetID unless bytes [0:22] are all zero.
func (a AssetID) Validate() error {
	for _, b := range a[:32-types.ZenonTokenStandardSize] {
		if b != 0 {
			return fmt.Errorf("codec: asset id: %w", ErrInvalidAssetID)
		}
	}
	return nil
}

// assetID reads a 32-byte AssetID and validates its canonical alignment.
func (r *reader) assetID() (AssetID, error) {
	var a AssetID
	b, err := r.fixed(32)
	if err != nil {
		return a, err
	}
	copy(a[:], b)
	if err := a.Validate(); err != nil {
		return a, err
	}
	return a, nil
}
