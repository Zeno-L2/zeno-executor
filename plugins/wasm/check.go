package wasm

import (
	"github.com/zenon-network/go-zenon/common/crypto"
	"github.com/zenon-network/go-zenon/common/types"

	"github.com/digitalSloth/go-executor/internal/codec"
	"github.com/digitalSloth/go-executor/plugins/wasm/internal/module"
)

// ModuleCheck is what CheckModule reports about an accepted module.
type ModuleCheck struct {
	// CodeHash is CodeHash of the raw module, the code_hash a deploy
	// declares.
	CodeHash types.Hash
	// InstrumentedCodeHash is CodeHash of the instrumented module, the
	// hash a finalize stores.
	InstrumentedCodeHash types.Hash
	// Size is the raw module's length in bytes.
	Size int
	// InstrumentedSize is the instrumented module's length in bytes.
	InstrumentedSize int
	// InitialPages is the declared minimum of the module's memory.
	InitialPages uint32
	// MetadataHash is the SHA3-256 of the metadata, zero when the
	// metadata is nil.
	MetadataHash types.Hash
}

// CheckModule runs the module checks a deploy finalize runs, without a
// plugin or any state. A non-nil metadata is decoded after the module
// derivation succeeds, in finalize's order, and its decode error is
// returned.
func CheckModule(raw []byte, metadata []byte) (ModuleCheck, error) {
	instrumented, instrumentedHash, err := deriveModuleErr(raw)
	if err != nil {
		return ModuleCheck{}, err
	}

	var metadataHash types.Hash
	if metadata != nil {
		if _, err := codec.DecodeContractMetadata(metadata); err != nil {
			return ModuleCheck{}, err
		}
		metadataHash = hashOf(crypto.Hash(metadata))
	}

	pages, err := module.InitialPages(raw)
	if err != nil {
		return ModuleCheck{}, err
	}

	return ModuleCheck{
		CodeHash:             CodeHash(raw),
		InstrumentedCodeHash: instrumentedHash,
		Size:                 len(raw),
		InstrumentedSize:     len(instrumented),
		InitialPages:         pages,
		MetadataHash:         metadataHash,
	}, nil
}
