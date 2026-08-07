// Package keys holds the single shared implementation of the SMT path derivation used by
// plugins/wasm and plugins/wasm/internal/engine, so the consensus-critical hash has one
// definition.
package keys

import (
	"github.com/zenon-network/go-zenon/common/crypto"
	"github.com/zenon-network/go-zenon/common/types"

	"github.com/digitalSloth/go-executor/domain"
)

// Derive computes the SMT path a local key (reserved or contract-defined) resolves to under
// contractID: SHA3-256(contract_id || local_key).
func Derive(contractID types.Hash, localKey []byte) domain.Path {
	return domain.Path(hashOf(crypto.Hash(contractID[:], localKey)))
}

// hashOf copies a SHA3-256 digest into a fixed-size types.Hash.
func hashOf(b []byte) types.Hash {
	var out types.Hash
	copy(out[:], b)
	return out
}
