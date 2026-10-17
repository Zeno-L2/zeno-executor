package wasm

import (
	"bytes"

	"github.com/zenon-network/go-zenon/common/crypto"
	"github.com/zenon-network/go-zenon/common/types"

	"github.com/digitalSloth/go-executor/domain"
	"github.com/digitalSloth/go-executor/plugins/wasm/internal/keys"
)

// Reserved local keys: exactly these five two-byte strings, matched
// exactly and never by prefix, so a contract keying its own state by a
// zero-padded big-endian integer never collides with the reserved
// space.
var (
	keyCodeHash      = keys.CodeHash
	keyPreCodeHash   = keys.PreCodeHash
	keyUpgradePolicy = keys.UpgradePolicy
	keyOwner         = keys.Owner
	keyQuota         = keys.Quota
)

var reservedKeys = [][]byte{keyCodeHash, keyPreCodeHash, keyUpgradePolicy, keyOwner, keyQuota}

// reserved reports whether localKey is one of the five
// protocol-reserved keys, matched exactly. A StateDiff entry from a
// contract whose local_key is reserved is UNAUTHORIZED; contract reads
// of a reserved key need no special case.
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

// CodeHash is the identity of an instrumented module:
// SHA3-256(instrumented). It is a function of the instrumented bytes,
// not the pre-instrumentation source: Instrument is not injective, so
// distinct validated sources that normalize to the same instrumented
// output share a CodeHash. Callers must not rely on CodeHash alone to
// distinguish different pre-instrumentation inputs.
func CodeHash(instrumented []byte) types.Hash {
	return hashOf(crypto.Hash(instrumented))
}

// hashOf copies a SHA3-256 digest into a fixed-size types.Hash.
func hashOf(b []byte) types.Hash {
	var out types.Hash
	copy(out[:], b)
	return out
}
