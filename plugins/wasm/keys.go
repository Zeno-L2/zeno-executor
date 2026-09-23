package wasm

import (
	"bytes"

	"github.com/zenon-network/go-zenon/common/crypto"
	"github.com/zenon-network/go-zenon/common/types"

	"github.com/digitalSloth/go-executor/domain"
	"github.com/digitalSloth/go-executor/plugins/wasm/internal/keys"
)

// Reserved local keys (§13.3): exactly these four two-byte strings, matched exactly and
// never by prefix, so a contract keying its own state by a zero-padded big-endian integer
// never collides with the reserved space.
var (
	// keyCodeHash is the post-instrumentation CodeHash (§11.4); its presence means "deployed".
	keyCodeHash = []byte{0x00, 0x01}
	// keyPreCodeHash is the pre-instrumentation code_hash, locating the assembled module in the
	// local code store.
	keyPreCodeHash = []byte{0x00, 0x02}
	// keyUpgradePolicy is 0 = IMMUTABLE, 1 = OWNER_UPGRADEABLE.
	keyUpgradePolicy = []byte{0x00, 0x03}
	// keyOwner is the caller-domain owner identity; absent unless the policy is
	// OWNER_UPGRADEABLE.
	keyOwner = []byte{0x00, 0x04}
)

var reservedKeys = [][]byte{keyCodeHash, keyPreCodeHash, keyUpgradePolicy, keyOwner}

// reserved reports whether localKey is one of the four protocol-reserved keys, matched
// exactly. A StateDiff entry from a contract whose local_key is reserved is UNAUTHORIZED
// (SPEC.md:694); contract reads of a reserved key need no special case.
func reserved(localKey []byte) bool {
	for _, k := range reservedKeys {
		if bytes.Equal(localKey, k) {
			return true
		}
	}
	return false
}

// derive computes the SMT path a local key (reserved or contract-defined) resolves to under
// contractID: SHA3-256(contract_id || local_key).
func derive(contractID types.Hash, localKey []byte) domain.Path {
	return keys.Derive(contractID, localKey)
}

// CodeHash is the identity of an instrumented module: SHA3-256(instrumented) (§11.4). It is a
// function of the instrumented bytes, not the pre-instrumentation source: Instrument is not
// injective, so distinct validated sources that normalize to the same instrumented output share
// a CodeHash. Callers must not rely on CodeHash alone to distinguish different
// pre-instrumentation inputs.
func CodeHash(instrumented []byte) types.Hash {
	return hashOf(crypto.Hash(instrumented))
}

// hashOf copies a SHA3-256 digest into a fixed-size types.Hash.
func hashOf(b []byte) types.Hash {
	var out types.Hash
	copy(out[:], b)
	return out
}
