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

// Reserved local keys (§13.3): exactly these five two-byte strings, matched exactly and
// never by prefix.
var (
	// CodeHash is the post-instrumentation CodeHash (§11.4); its presence means "deployed".
	CodeHash = []byte{0x00, 0x01}
	// PreCodeHash is the pre-instrumentation code_hash, locating the assembled module in the
	// local code store.
	PreCodeHash = []byte{0x00, 0x02}
	// UpgradePolicy is 0 = IMMUTABLE, 1 = OWNER_UPGRADEABLE.
	UpgradePolicy = []byte{0x00, 0x03}
	// Owner is the caller-domain owner identity; absent unless the policy is
	// OWNER_UPGRADEABLE.
	Owner = []byte{0x00, 0x04}
	// Quota is the storage quota leaf: purchased:u64 || used:u64, both big-endian (§13.8).
	Quota = []byte{0x00, 0x05}
)

// Caller is the L1 caller identity: SHA3-256("L1_CALLER:" || raw 20 address bytes).
func Caller(addr types.Address) types.Hash {
	return hashOf(crypto.Hash([]byte("L1_CALLER:"), addr[:]))
}

// ChainID is the domain chain id: SHA3-256("ZENON_WASM_L2:" || network name).
func ChainID(networkName string) types.Hash {
	return hashOf(crypto.Hash([]byte("ZENON_WASM_L2:"), []byte(networkName)))
}
