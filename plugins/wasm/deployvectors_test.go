package wasm

import (
	"bytes"
	"errors"
	"math"
	"testing"

	"github.com/zenon-network/go-zenon/common/crypto"
	"github.com/zenon-network/go-zenon/common/types"

	"github.com/Zeno-L2/zeno-executor/domain"
	"github.com/Zeno-L2/zeno-executor/internal/codec"
	"github.com/Zeno-L2/zeno-executor/plugins/wasm/internal/module"
)

// This file holds the deploy/upgrade and validator rejection
// vectorSpecs, the fixture bodies they need and the
// fixture-to-validator-rule map. All fixtures go through
// buildWasmModule/runModuleSpec (fixtures_test.go); no ad-hoc WASM
// bytes, no new builder.

// deployModule is the module CV-DEPLOY-1, CV-DEPLOY-2 and EXEC-DEPLOY-1 share: three
// independent code stores, not one sequenced deployment.
var deployModule = buildWasmModule(runModuleSpec(nil, 1, []byte{0x00}, []byte{0x42, 0x00, 0x0B}, nil))

// upgradeModule is the new code all three upgrade vectors name.
var upgradeModule = buildWasmModule(runModuleSpec(nil, 1, []byte{0x00}, []byte{0x42, 0x01, 0x0B}, nil))

// The three upgrade targets and their seeded old code hashes.
var (
	upCid       = repeatedHash(0x66) // EXEC-UPGRADE-1's target
	oldCodeHash = repeatedHash(0x9A) // the keyCodeHash leaf EXEC-UPGRADE-1 seeds for upCid

	undeployedCid = repeatedHash(0x67) // CV-UPGRADE-UNDEPLOYED-1's target: no leaves at all
	unauthCid     = repeatedHash(0x68) // CV-UPGRADE-UNAUTH-1's target
	unauthOldHash = repeatedHash(0x9B) // the keyCodeHash leaf CV-UPGRADE-UNAUTH-1 seeds

	upOwnerHash = callerHash(repeatedAddr(0x01)) // EXEC-UPGRADE-1's seeded keyOwner leaf

	quotaCid      = repeatedHash(0x69) // the BuyStorage vectors' deployed target
	quotaCodeHash = repeatedHash(0x9C) // the keyCodeHash leaf the BuyStorage vectors seed for quotaCid
)

// errAssemblyHash is the rejection cause of a vector whose staged bytes do not hash to its
// input's code hash.
var errAssemblyHash = errors.New("assembled bytes do not hash to code_hash")

// rejectionReasons binds each vector that a deploy-path check rejects to that check's error:
// the validator rule, the instrumentation ceiling, the metadata rule or the assembly hash. A
// validation_rejection vector missing from it fails the suite. An errAssemblyHash entry is the
// only thing that lets a vector stage bytes that do not hash to its input's code hash.
var rejectionReasons = map[string]error{
	"CV-REJECT-WASI-1":     module.ErrDisallowedImport,
	"CV-MEM-MIN-1":         module.ErrMemoryTooLarge,
	"CV-REJECT-FLOAT-1":    module.ErrFloatInstruction,
	"CV-REJECT-ATOMIC-1":   module.ErrAtomics,
	"CV-REJECT-SIMD-1":     module.ErrSIMD,
	"CV-REJECT-SIGNEXT-1":  module.ErrSignExtension,
	"CV-REJECT-SATCONV-1":  module.ErrSaturatingConversion,
	"CV-DEPLOY-META-1":     codec.ErrUnknownVersion,
	"CV-DEPLOY-META-2":     codec.ErrTruncated,
	"CV-DEPLOY-META-3":     codec.ErrTrailingBytes,
	"CV-DEPLOY-META-4":     codec.ErrInvalidPolicy,
	"CV-DEPLOY-META-5":     codec.ErrInvalidOwner,
	"CV-DEPLOY-META-6":     codec.ErrInvalidOwner,
	"CV-DEPLOY-HASH-1":     errAssemblyHash,
	"CV-UPGRADE-HASH-1":    errAssemblyHash,
	"CV-UPGRADE-ORDER-1":   module.ErrFloatInstruction,
	"CV-UPGRADE-INVALID-1": module.ErrFloatInstruction,

	"CV-STRUCT-TYPES-1":       module.ErrTooManyTypes,
	"CV-STRUCT-TYPEPARAMS-1":  module.ErrTooManyParams,
	"CV-STRUCT-PARAMS-1":      module.ErrTotalParamsExceeded,
	"CV-STRUCT-FUNCS-1":       module.ErrTooManyFunctions,
	"CV-STRUCT-TABLE-1":       module.ErrTableTooLarge,
	"CV-STRUCT-GLOBALS-1":     module.ErrTooManyGlobals,
	"CV-STRUCT-ELEMS-1":       module.ErrTotalElementEntriesExceeded,
	"CV-STRUCT-DATA-1":        module.ErrTooManyDataSegments,
	"CV-STRUCT-LOCALS-1":      module.ErrTooManyLocals,
	"CV-STRUCT-TOTALLOCALS-1": module.ErrTotalLocalsExceeded,
	"CV-FIT-DATA-1":           module.ErrDataSegmentOutOfBounds,
	"CV-FIT-ELEM-1":           module.ErrElementSegmentOutOfBounds,
	"CV-IMPORT-MODULE-1":      module.ErrDisallowedImport,
	"CV-IMPORT-NAME-1":        module.ErrDisallowedImport,
	"CV-IMPORT-DUP-1":         module.ErrDisallowedImport,
	"CV-IMPORT-SIG-1":         module.ErrImportSignature,
	"CV-IMPORT-KIND-1":        module.ErrDisallowedImport,
	"CV-SECTION-START-1":      module.ErrStartSection,
	"CV-SECTION-CUSTOM-1":     module.ErrCustomSection,
	"CV-SECTION-CUSTOM-2":     module.ErrCustomSection,
	"CV-EXPORT-MUTGLOBAL-1":   module.ErrMutableGlobalExport,
	"CV-EXPORT-START-1":       module.ErrReservedExport,
	"CV-EXPORT-ZENON-1":       module.ErrReservedExport,
	"CV-EXPORT-NORUN-1":       module.ErrMissingExport,
	"CV-EXPORT-NOMEMORY-1":    module.ErrMissingExport,
	"CV-EXPORT-RUNSIG-1":      module.ErrRunSignature,
	"CV-EXPORT-RUNIMPORT-1":   module.ErrRunImported,
	"CV-INSTR-SIZE-1":         module.ErrInstrumentedTooLarge,
	"CV-LEB-PADDED-1":         module.ErrNonCanonicalLEB128,
}

// wantValidateErr returns the module.Validate error vector id's module
// must produce: its rejectionReasons entry, or nil when it has none or
// when that entry is errAssemblyHash.
func wantValidateErr(id string) error {
	if err := rejectionReasons[id]; err != errAssemblyHash {
		return err
	}
	return nil
}

// deployVectors returns every deploy-shape and validation_rejection-shape vectorSpec, in the
// file's table order.
func deployVectors(t *testing.T) []vectorSpec {
	t.Helper()

	n := 0
	// The index block is [2000, 3000), append-only.
	nextIndex := func() uint64 {
		idx := uint64(2000 + n)
		n++
		return idx
	}

	var specs []vectorSpec

	// --- CV-DEPLOY-1/CV-DEPLOY-2/EXEC-DEPLOY-1: single-chunk sizing and finalize.

	{
		idx := nextIndex()
		specs = append(specs, vectorSpec{
			ID:      "CV-DEPLOY-1",
			Purpose: "single-chunk deployment start: chunk_count 1 for a module within MaxDeployChunkData",
			Shape:   "deploy",
			BatchID: 4,
			Ctx:     stdCtx(idx),
			Args: codec.DeployStartArgs{
				CodeHash:     CodeHash(deployModule),
				MetadataHash: hashOf(crypto.Hash(immutableMetadata)),
				TotalSize:    uint64(len(deployModule)),
				ChunkCount:   1,
				Metadata:     immutableMetadata,
			},
			Module:     deployModule,
			WantStatus: codec.StatusSuccess,
		})
	}

	{
		idx := nextIndex()
		ctx := stdCtx(idx)
		ctx.SourceData = deployModule
		specs = append(specs, vectorSpec{
			ID:      "CV-DEPLOY-2",
			Purpose: "the single deploy chunk carrying the whole module at chunk_index 0",
			Shape:   "deploy",
			BatchID: 4,
			Ctx:     ctx,
			Args: codec.DeployChunkArgs{
				CodeHash:   CodeHash(deployModule),
				ChunkIndex: 0,
				ChunkHash:  CodeHash(deployModule),
			},
			Module:     deployModule,
			WantStatus: codec.StatusSuccess,
		})
	}

	{
		idx := nextIndex()
		specs = append(specs, vectorSpec{
			ID:              "EXEC-DEPLOY-1",
			Purpose:         "deploy finalize: the three reserved leaves are written and ReturnData is code_hash then CodeHash",
			Shape:           "deploy",
			BatchID:         4,
			Ctx:             stdCtx(idx),
			Args:            codec.DeployFinalizeArgs{CodeHash: CodeHash(deployModule)},
			Module:          deployModule,
			PendingMetadata: immutableMetadata,
			WantStatus:      codec.StatusSuccess,
		})
	}

	// --- Upgrade: SUCCESS, undeployed target, unauthorized.

	{
		idx := nextIndex()
		specs = append(specs, vectorSpec{
			ID:         "EXEC-UPGRADE-1",
			Purpose:    "an authorized upgrade rewrites both code-hash leaves and returns the old then the new CodeHash",
			Shape:      "deploy",
			BatchID:    4,
			Ctx:        stdCtx(idx),
			ContractID: upCid,
			Args:       codec.UpgradeArgs{ContractID: upCid, NewCodeHash: CodeHash(upgradeModule)},
			Module:     upgradeModule,
			ExtraPreState: map[domain.Path][]byte{
				derive(upCid, keyCodeHash):      oldCodeHash[:],
				derive(upCid, keyUpgradePolicy): {policyOwnerUpgradeable},
				derive(upCid, keyOwner):         upOwnerHash[:],
			},
			PendingMetadata: []byte{},
			WantStatus:      codec.StatusSuccess,
		})
	}

	{
		idx := nextIndex()
		specs = append(specs, vectorSpec{
			ID:              "CV-UPGRADE-UNDEPLOYED-1",
			Purpose:         "an upgrade naming a contract_id with no code-hash leaf is VALIDATION_FAILED, not UNAUTHORIZED",
			Shape:           "deploy",
			BatchID:         4,
			Ctx:             stdCtx(idx),
			ContractID:      undeployedCid,
			Args:            codec.UpgradeArgs{ContractID: undeployedCid, NewCodeHash: CodeHash(upgradeModule)},
			Module:          upgradeModule,
			ExtraPreState:   nil,
			PendingMetadata: []byte{},
			WantStatus:      codec.StatusValidationFailed,
		})
	}

	{
		idx := nextIndex()
		specs = append(specs, vectorSpec{
			ID:         "CV-UPGRADE-UNAUTH-1",
			Purpose:    "an upgrade of a deployed contract with no upgrade-policy leaf is UNAUTHORIZED and its receipt carries the unchanged CodeHash",
			Shape:      "deploy",
			BatchID:    4,
			Ctx:        stdCtx(idx),
			ContractID: unauthCid,
			Args:       codec.UpgradeArgs{ContractID: unauthCid, NewCodeHash: CodeHash(upgradeModule)},
			Module:     upgradeModule,
			ExtraPreState: map[domain.Path][]byte{
				derive(unauthCid, keyCodeHash): unauthOldHash[:],
			},
			PendingMetadata: []byte{},
			WantStatus:      codec.StatusUnauthorized,
		})
	}

	// --- validator rejections.

	{
		idx := nextIndex()
		fixture := buildWasmModule(wasmModuleSpec{
			Types:       []wasmFuncType{{Params: []byte{0x7F, 0x7F}, Results: []byte{0x7E}}},
			Imports:     []wasmImport{{Module: "wasi_snapshot_preview1", Name: "fd_write", TypeIdx: 0}},
			Funcs:       []wasmFunc{{TypeIdx: 0, Locals: []byte{0x00}, Body: []byte{0x42, 0x00, 0x0B}}},
			MemoryPages: 1,
			Exports: []wasmExport{
				{Name: "memory", Kind: 0x02, Index: 0},
				{Name: "run", Kind: 0x00, Index: 1}, // run follows the one import
			},
		})
		specs = append(specs, vectorSpec{
			ID:              "CV-REJECT-WASI-1",
			Purpose:         "a deploy of a module importing from wasi_snapshot_preview1",
			Shape:           "validation_rejection",
			BatchID:         4,
			Ctx:             stdCtx(idx),
			Args:            codec.DeployFinalizeArgs{CodeHash: CodeHash(fixture)},
			Module:          fixture,
			PendingMetadata: immutableMetadata,
			WantStatus:      codec.StatusValidationFailed,
		})
	}

	{
		idx := nextIndex()
		fixture := buildWasmModule(runModuleSpec(nil, 1, []byte{0x00}, []byte{0xA0, 0x42, 0x00, 0x0B}, nil))
		specs = append(specs, vectorSpec{
			ID:              "CV-REJECT-FLOAT-1",
			Purpose:         "a deploy of a module using a float instruction",
			Shape:           "validation_rejection",
			BatchID:         4,
			Ctx:             stdCtx(idx),
			Args:            codec.DeployFinalizeArgs{CodeHash: CodeHash(fixture)},
			Module:          fixture,
			PendingMetadata: immutableMetadata,
			WantStatus:      codec.StatusValidationFailed,
		})
	}

	{
		idx := nextIndex()
		fixture := buildWasmModule(runModuleSpec(nil, 1, []byte{0x00}, []byte{0xFE, 0x10, 0x42, 0x00, 0x0B}, nil))
		specs = append(specs, vectorSpec{
			ID:              "CV-REJECT-ATOMIC-1",
			Purpose:         "a deploy of a module using an atomic instruction",
			Shape:           "validation_rejection",
			BatchID:         4,
			Ctx:             stdCtx(idx),
			Args:            codec.DeployFinalizeArgs{CodeHash: CodeHash(fixture)},
			Module:          fixture,
			PendingMetadata: immutableMetadata,
			WantStatus:      codec.StatusValidationFailed,
		})
	}

	{
		idx := nextIndex()
		fixture := buildWasmModule(runModuleSpec(nil, 1, []byte{0x00}, []byte{0xFD, 0x0C, 0x42, 0x00, 0x0B}, nil))
		specs = append(specs, vectorSpec{
			ID:              "CV-REJECT-SIMD-1",
			Purpose:         "a deploy of a module using a v128 instruction",
			Shape:           "validation_rejection",
			BatchID:         4,
			Ctx:             stdCtx(idx),
			Args:            codec.DeployFinalizeArgs{CodeHash: CodeHash(fixture)},
			Module:          fixture,
			PendingMetadata: immutableMetadata,
			WantStatus:      codec.StatusValidationFailed,
		})
	}

	{
		idx := nextIndex()
		fixture := buildWasmModule(runModuleSpec(nil, 1, []byte{0x00}, []byte{0xC0, 0x42, 0x00, 0x0B}, nil))
		specs = append(specs, vectorSpec{
			ID:              "CV-REJECT-SIGNEXT-1",
			Purpose:         "a deploy of a module using a sign-extension instruction",
			Shape:           "validation_rejection",
			BatchID:         4,
			Ctx:             stdCtx(idx),
			Args:            codec.DeployFinalizeArgs{CodeHash: CodeHash(fixture)},
			Module:          fixture,
			PendingMetadata: immutableMetadata,
			WantStatus:      codec.StatusValidationFailed,
		})
	}

	{
		idx := nextIndex()
		fixture := buildWasmModule(runModuleSpec(nil, 1, []byte{0x00}, []byte{0xFC, 0x00, 0x42, 0x00, 0x0B}, nil))
		specs = append(specs, vectorSpec{
			ID:              "CV-REJECT-SATCONV-1",
			Purpose:         "a deploy of a module using a saturating float-to-int conversion",
			Shape:           "validation_rejection",
			BatchID:         4,
			Ctx:             stdCtx(idx),
			Args:            codec.DeployFinalizeArgs{CodeHash: CodeHash(fixture)},
			Module:          fixture,
			PendingMetadata: immutableMetadata,
			WantStatus:      codec.StatusValidationFailed,
		})
	}

	// --- Contract metadata, upgrade policy and ownership: a chained
	// sequence of vectors sharing ownedCid's reserved leaves. Each
	// vector's spec.ExtraPreState is the reserved leaves the vector
	// before it in the chain leaves, computed inline
	// (generateDeployVector gives every vectorSpec its own fresh store;
	// nothing here shares a store across vectors).

	testnetChainID := hashOf(crypto.Hash([]byte("ZENON_WASM_L2:"), []byte("testnet")))
	deployInstrumented, err := module.Instrument(deployModule, moduleLimits())
	if err != nil {
		t.Fatalf("Instrument(deployModule): %v", err)
	}
	deployInstrumentedHash := CodeHash(deployInstrumented)
	upgradeInstrumented, err := module.Instrument(upgradeModule, moduleLimits())
	if err != nil {
		t.Fatalf("Instrument(upgradeModule): %v", err)
	}
	upgradeInstrumentedHash := CodeHash(upgradeInstrumented)

	ownerMetadata := codec.ContractMetadata{Version: codec.ContractMetadataVersion, UpgradePolicy: codec.PolicyOwnerUpgradeable, Owner: repeatedAddr(0x01)}.Encode()
	owner1Hash := callerHash(repeatedAddr(0x01))
	owner2Hash := callerHash(repeatedAddr(0x02))

	var ownedCid types.Hash

	{
		idx := nextIndex()
		ownedCid = contractID(repeatedAddr(0x01), idx, testnetChainID)
		specs = append(specs, vectorSpec{
			ID:              "EXEC-DEPLOY-2",
			Purpose:         "an OWNER_UPGRADEABLE deploy writes the policy and owner leaves alongside the two code-hash leaves",
			Shape:           "deploy",
			BatchID:         4,
			Ctx:             stdCtx(idx),
			ContractID:      ownedCid,
			Args:            codec.DeployFinalizeArgs{CodeHash: CodeHash(deployModule)},
			Module:          deployModule,
			PendingMetadata: ownerMetadata,
			WantStatus:      codec.StatusSuccess,
		})
	}

	// EXEC-DEPLOY-2's post-state: the four reserved leaves applyDeployFinalize writes for an
	// OWNER_UPGRADEABLE deploy.
	deployModuleHash := CodeHash(deployModule)
	deploy2PostState := map[domain.Path][]byte{
		derive(ownedCid, keyCodeHash):      deployInstrumentedHash[:],
		derive(ownedCid, keyPreCodeHash):   deployModuleHash[:],
		derive(ownedCid, keyUpgradePolicy): {policyOwnerUpgradeable},
		derive(ownedCid, keyOwner):         owner1Hash[:],
	}

	{
		idx := nextIndex()
		specs = append(specs, vectorSpec{
			ID:              "EXEC-UPGRADE-2",
			Purpose:         "an authorized upgrade of an OWNER_UPGRADEABLE contract leaves its policy and owner leaves untouched",
			Shape:           "deploy",
			BatchID:         4,
			Ctx:             stdCtx(idx),
			ContractID:      ownedCid,
			Args:            codec.UpgradeArgs{ContractID: ownedCid, NewCodeHash: CodeHash(upgradeModule)},
			Module:          upgradeModule,
			ExtraPreState:   deploy2PostState,
			PendingMetadata: []byte{},
			WantStatus:      codec.StatusSuccess,
		})
	}

	// EXEC-UPGRADE-2's post-state: keyCodeHash/keyPreCodeHash rewritten to the new module;
	// policy and owner unchanged.
	upgradeModuleHash := CodeHash(upgradeModule)
	upgrade2PostState := map[domain.Path][]byte{
		derive(ownedCid, keyCodeHash):      upgradeInstrumentedHash[:],
		derive(ownedCid, keyPreCodeHash):   upgradeModuleHash[:],
		derive(ownedCid, keyUpgradePolicy): {policyOwnerUpgradeable},
		derive(ownedCid, keyOwner):         owner1Hash[:],
	}

	{
		idx := nextIndex()
		ctx := stdCtx(idx)
		specs = append(specs, vectorSpec{
			ID:            "EXEC-TRANSFER-1",
			Purpose:       "an owner's transfer rewrites only the owner leaf",
			Shape:         "ownership",
			BatchID:       4,
			Ctx:           ctx,
			ContractID:    ownedCid,
			Args:          codec.TransferOwnershipArgs{ContractID: ownedCid, NewOwner: repeatedAddr(0x02)},
			ExtraPreState: upgrade2PostState,
			WantStatus:    codec.StatusSuccess,
		})
	}

	// EXEC-TRANSFER-1's post-state: keyOwner rewritten to repeatedAddr(0x02)'s caller hash.
	transfer1PostState := map[domain.Path][]byte{
		derive(ownedCid, keyCodeHash):      upgradeInstrumentedHash[:],
		derive(ownedCid, keyPreCodeHash):   upgradeModuleHash[:],
		derive(ownedCid, keyUpgradePolicy): {policyOwnerUpgradeable},
		derive(ownedCid, keyOwner):         owner2Hash[:],
	}

	{
		idx := nextIndex()
		ctx := stdCtx(idx)
		ctx.L1Sender = repeatedAddr(0x02)
		specs = append(specs, vectorSpec{
			ID:            "EXEC-TRANSFER-2",
			Purpose:       "a renounce (zero new_owner) sets the policy to IMMUTABLE and deletes the owner leaf",
			Shape:         "ownership",
			BatchID:       4,
			Ctx:           ctx,
			ContractID:    ownedCid,
			Args:          codec.TransferOwnershipArgs{ContractID: ownedCid, NewOwner: types.Address{}},
			ExtraPreState: transfer1PostState,
			WantStatus:    codec.StatusSuccess,
		})
	}

	// EXEC-TRANSFER-2's post-state: policy IMMUTABLE, owner leaf absent.
	transfer2PostState := map[domain.Path][]byte{
		derive(ownedCid, keyCodeHash):      upgradeInstrumentedHash[:],
		derive(ownedCid, keyPreCodeHash):   upgradeModuleHash[:],
		derive(ownedCid, keyUpgradePolicy): {policyImmutable},
	}

	{
		idx := nextIndex()
		ctx := stdCtx(idx)
		ctx.L1Sender = repeatedAddr(0x02)
		specs = append(specs, vectorSpec{
			ID:              "CV-UPGRADE-RENOUNCED-1",
			Purpose:         "an upgrade against a renounced (IMMUTABLE, no owner leaf) contract is UNAUTHORIZED",
			Shape:           "deploy",
			BatchID:         4,
			Ctx:             ctx,
			ContractID:      ownedCid,
			Args:            codec.UpgradeArgs{ContractID: ownedCid, NewCodeHash: CodeHash(deployModule)},
			Module:          deployModule,
			ExtraPreState:   transfer2PostState,
			PendingMetadata: []byte{},
			WantStatus:      codec.StatusUnauthorized,
		})
	}

	{
		idx := nextIndex()
		ctx := stdCtx(idx)
		ctx.L1Sender = repeatedAddr(0x03)
		specs = append(specs, vectorSpec{
			ID:            "CV-TRANSFER-UNAUTH-1",
			Purpose:       "a transfer by a non-owner is UNAUTHORIZED and its receipt carries the unchanged CodeHash",
			Shape:         "ownership",
			BatchID:       4,
			Ctx:           ctx,
			ContractID:    ownedCid,
			Args:          codec.TransferOwnershipArgs{ContractID: ownedCid, NewOwner: repeatedAddr(0x04)},
			ExtraPreState: upgrade2PostState,
			WantStatus:    codec.StatusUnauthorized,
		})
	}

	{
		idx := nextIndex()
		specs = append(specs, vectorSpec{
			ID:         "CV-TRANSFER-UNDEPLOYED-1",
			Purpose:    "a transfer naming a contract_id with no code-hash leaf is VALIDATION_FAILED",
			Shape:      "ownership",
			BatchID:    4,
			Ctx:        stdCtx(idx),
			ContractID: undeployedCid,
			Args:       codec.TransferOwnershipArgs{ContractID: undeployedCid, NewOwner: repeatedAddr(0x02)},
			WantStatus: codec.StatusValidationFailed,
		})
	}

	// --- CV-DEPLOY-META-1..6: each malformed pending-metadata shape CV-META-2 also rejects, in
	// the same order.
	deployMetaCases := []struct {
		id       string
		metadata []byte
	}{
		{"CV-DEPLOY-META-1", codec.ContractMetadata{Version: 2, UpgradePolicy: codec.PolicyImmutable}.Encode()},
		{"CV-DEPLOY-META-2", immutableMetadata[:22]},
		{"CV-DEPLOY-META-3", append(append([]byte{}, immutableMetadata...), 0x00)},
		{"CV-DEPLOY-META-4", codec.ContractMetadata{Version: codec.ContractMetadataVersion, UpgradePolicy: 2}.Encode()},
		{"CV-DEPLOY-META-5", codec.ContractMetadata{Version: codec.ContractMetadataVersion, UpgradePolicy: codec.PolicyImmutable, Owner: repeatedAddr(0x01)}.Encode()},
		{"CV-DEPLOY-META-6", codec.ContractMetadata{Version: codec.ContractMetadataVersion, UpgradePolicy: codec.PolicyOwnerUpgradeable}.Encode()},
	}
	for _, tc := range deployMetaCases {
		idx := nextIndex()
		specs = append(specs, vectorSpec{
			ID:              tc.id,
			Purpose:         "a deploy finalize against malformed pending metadata is VALIDATION_FAILED",
			Shape:           "validation_rejection",
			BatchID:         4,
			Ctx:             stdCtx(idx),
			Args:            codec.DeployFinalizeArgs{CodeHash: CodeHash(deployModule)},
			Module:          deployModule,
			PendingMetadata: tc.metadata,
			WantStatus:      codec.StatusValidationFailed,
		})
	}

	// --- CV-QUOTA-6..8: BuyStorage (kind 7), one vector per outcome.
	{
		idx := nextIndex()
		v8 := []byte{8, 8, 8, 8, 8, 8, 8, 8}
		pre := seedQuota(quotaCid, map[domain.Path][]byte{derive(quotaCid, []byte("q0")): v8}, 1000)
		pre[derive(quotaCid, keyCodeHash)] = quotaCodeHash[:]
		specs = append(specs, vectorSpec{
			ID:            "CV-QUOTA-6",
			Purpose:       "BuyStorage adds bytes to purchased and leaves used unchanged",
			Shape:         "buy_storage",
			BatchID:       4,
			Ctx:           stdCtx(idx),
			ContractID:    quotaCid,
			Args:          codec.BuyStorageArgs{ContractID: quotaCid, Bytes: 4096},
			ExtraPreState: pre,
			WantStatus:    codec.StatusSuccess,
		})
	}

	{
		idx := nextIndex()
		specs = append(specs, vectorSpec{
			ID:         "CV-QUOTA-7",
			Purpose:    "BuyStorage naming a contract_id with no code-hash leaf is VALIDATION_FAILED",
			Shape:      "buy_storage",
			BatchID:    4,
			Ctx:        stdCtx(idx),
			ContractID: undeployedCid,
			Args:       codec.BuyStorageArgs{ContractID: undeployedCid, Bytes: 4096},
			WantStatus: codec.StatusValidationFailed,
		})
	}

	{
		idx := nextIndex()
		specs = append(specs, vectorSpec{
			ID:         "CV-QUOTA-8",
			Purpose:    "BuyStorage whose purchased + bytes is 2^64 is VALIDATION_FAILED, and its receipt carries the unchanged CodeHash",
			Shape:      "buy_storage",
			BatchID:    4,
			Ctx:        stdCtx(idx),
			ContractID: quotaCid,
			Args:       codec.BuyStorageArgs{ContractID: quotaCid, Bytes: 4096},
			ExtraPreState: map[domain.Path][]byte{
				derive(quotaCid, keyCodeHash): quotaCodeHash[:],
				derive(quotaCid, keyQuota):    encodeQuota(math.MaxUint64-4095, 0),
			},
			WantStatus: codec.StatusValidationFailed,
		})
	}

	invalidModule := buildWasmModule(runModuleSpec(nil, 1, []byte{0x00}, []byte{0xA0, 0x42, 0x00, 0x0B}, nil))
	pattern := make([]byte, 16384)
	for i := range pattern {
		pattern[i] = byte(i % 251)
	}
	twoChunkModule := buildWasmModule(runModuleSpec(nil, 1, []byte{0x00}, []byte{0x42, 0x00, 0x0B}, []dataSeg{{offset: 0, bytes: pattern}}))
	upgradeModuleMaxSpec := runModuleSpec(nil, 1, []byte{0x00}, []byte{0x42, 0x01, 0x0B}, nil)
	upgradeModuleMaxSpec.MemoryMax = MaxMemoryPages
	upgradeModuleMax := buildWasmModule(upgradeModuleMaxSpec)

	{
		idx := nextIndex()
		specs = append(specs, vectorSpec{
			ID:              "CV-DEPLOY-3",
			Purpose:         "a deploy finalize of a module longer than MaxDeployChunkData: its two chunks assemble in ascending chunk_index and the deploy succeeds",
			Shape:           "deploy",
			BatchID:         4,
			Ctx:             stdCtx(idx),
			Args:            codec.DeployFinalizeArgs{CodeHash: CodeHash(twoChunkModule)},
			Module:          twoChunkModule,
			PendingMetadata: immutableMetadata,
			WantStatus:      codec.StatusSuccess,
		})
	}

	{
		idx := nextIndex()
		specs = append(specs, vectorSpec{
			ID:              "CV-DEPLOY-HASH-1",
			Purpose:         "a deploy finalize whose staged chunks pass every per-chunk check but assemble to bytes that do not hash to code_hash is VALIDATION_FAILED and creates no contract",
			Shape:           "validation_rejection",
			BatchID:         4,
			Ctx:             stdCtx(idx),
			Args:            codec.DeployFinalizeArgs{CodeHash: CodeHash(upgradeModule)},
			Module:          deployModule,
			PendingMetadata: immutableMetadata,
			WantStatus:      codec.StatusValidationFailed,
		})
	}

	upgradeRow := func(id, purpose string, mod []byte, newCodeHash types.Hash, sender *types.Address, want codec.Status) {
		idx := nextIndex()
		ctx := stdCtx(idx)
		if sender != nil {
			ctx.L1Sender = *sender
		}
		specs = append(specs, vectorSpec{
			ID:              id,
			Purpose:         purpose,
			Shape:           "deploy",
			BatchID:         4,
			Ctx:             ctx,
			ContractID:      ownedCid,
			Args:            codec.UpgradeArgs{ContractID: ownedCid, NewCodeHash: newCodeHash},
			Module:          mod,
			ExtraPreState:   upgrade2PostState,
			PendingMetadata: []byte{},
			WantStatus:      want,
		})
	}
	stranger := repeatedAddr(0x03)

	upgradeRow("CV-UPGRADE-UNAUTH-2", "an upgrade of an OWNER_UPGRADEABLE contract by a caller other than its owner is UNAUTHORIZED and its receipt carries the unchanged CodeHash",
		deployModule, CodeHash(deployModule), &stranger, codec.StatusUnauthorized)
	upgradeRow("CV-UPGRADE-ORDER-1", "an upgrade by a non-owner whose new module also fails validation is UNAUTHORIZED, not VALIDATION_FAILED: authorization precedes the §11.3 pipeline",
		invalidModule, CodeHash(invalidModule), &stranger, codec.StatusUnauthorized)
	upgradeRow("CV-UPGRADE-INVALID-1", "an authorized upgrade whose new module fails validation is VALIDATION_FAILED, with no code change and the unchanged CodeHash in its receipt",
		invalidModule, CodeHash(invalidModule), nil, codec.StatusValidationFailed)
	upgradeRow("CV-UPGRADE-HASH-1", "an authorized upgrade whose staged chunks assemble to bytes that do not hash to new_code_hash is VALIDATION_FAILED, with no code change and the unchanged CodeHash in its receipt",
		deployModule, repeatedHash(0x9D), nil, codec.StatusValidationFailed)
	upgradeRow("CV-UPGRADE-SAMEHASH-1", "an authorized upgrade that re-stages the contract's current module is VALIDATION_FAILED: an upgrade must produce a new CodeHash",
		upgradeModule, CodeHash(upgradeModule), nil, codec.StatusValidationFailed)
	upgradeRow("CV-UPGRADE-SAMEHASH-2", "an authorized upgrade to a different module that instruments to the contract's current CodeHash is VALIDATION_FAILED: the comparison is on CodeHash, not code_hash",
		upgradeModuleMax, CodeHash(upgradeModuleMax), nil, codec.StatusValidationFailed)

	{
		idx := nextIndex()
		fixture := buildWasmModule(runModuleSpec(nil, MaxMemoryPages+1, []byte{0x00}, []byte{0x42, 0x00, 0x0B}, nil))
		specs = append(specs, vectorSpec{
			ID:              "CV-MEM-MIN-1",
			Purpose:         "a deploy of a module declaring 65 initial pages, one above `MaxMemory`, is `VALIDATION_FAILED`",
			Shape:           "validation_rejection",
			BatchID:         4,
			Ctx:             stdCtx(idx),
			Args:            codec.DeployFinalizeArgs{CodeHash: CodeHash(fixture)},
			Module:          fixture,
			PendingMetadata: immutableMetadata,
			WantStatus:      codec.StatusValidationFailed,
		})
	}

	specs = append(specs, deployValidationVectors(t, nextIndex)...)

	{
		idx := nextIndex()
		specs = append(specs, vectorSpec{
			ID:             "CV-DEPOSIT-UNDEPLOYED-1",
			Purpose:        "a `Deposit` of 1000 naming a `target_contract` with no code-hash leaf is `VALIDATION_FAILED` at `gas_used` 0 with a zero receipt `code_hash`; the full deposit is refunded as the only outbox message, and the witness set is exactly the target's absent `0x0001` and `0x0002` leaves",
			Shape:          "undeployed_target",
			BatchID:        4,
			Ctx:            stdCtx(idx),
			ContractID:     undeployedCid,
			Args:           codec.DepositArgs{TargetContract: undeployedCid, PlasmaLimit: callPlasmaLimit, DepositAsset: testAsset(), DepositAmount: u256(1000)},
			WithdrawalCaps: capsOfOne(),
			WantStatus:     codec.StatusValidationFailed,
		})
	}

	{
		idx := nextIndex()
		specs = append(specs, vectorSpec{
			ID:             "CV-CALL-UNDEPLOYED-1",
			Purpose:        "a payable `CallL2` with a payload and a deposit of 1000 naming a `target_contract` with no code-hash leaf has the same outcome as a `Deposit`: `VALIDATION_FAILED` at `gas_used` 0, the full deposit refunded, and a witness set of exactly the target's absent `0x0001` and `0x0002` leaves",
			Shape:          "undeployed_target",
			BatchID:        4,
			Ctx:            stdCtx(idx),
			ContractID:     undeployedCid,
			Args:           codec.CallL2Args{TargetContract: undeployedCid, PlasmaLimit: callPlasmaLimit, DepositAsset: testAsset(), DepositAmount: u256(1000), Payload: []byte{0xCA, 0xFE}},
			WithdrawalCaps: capsOfOne(),
			WantStatus:     codec.StatusValidationFailed,
		})
	}

	return specs
}

// instrSizeReturns is the number of `return`s in CV-INSTR-SIZE-1's run body. Each instruments to
// 7 bytes, so the module stays under MaxCodeSize and the instrumented module exceeds
// MaxInstrumentedCodeSize.
const instrSizeReturns = 151_000

// deployValidationVectors returns the structural-cap, segment-fit, import, section, export,
// instrumented-size and padded-LEB128 vectors, drawing their indices from nextIndex. It fails
// unless each rejection vector's module, with its one defect undone, validates and instruments.
func deployValidationVectors(t *testing.T, nextIndex func() uint64) []vectorSpec {
	t.Helper()

	const i64 = 0x7E
	var specs []vectorSpec

	base := func() wasmModuleSpec {
		return runModuleSpec(nil, 1, []byte{0x00}, []byte{0x42, 0x00, 0x0B}, nil)
	}
	withImports := func(names ...string) wasmModuleSpec {
		return runModuleSpec(names, 1, []byte{0x00}, []byte{0x42, 0x00, 0x0B}, nil)
	}
	void := wasmFuncType{}
	filler := wasmFunc{TypeIdx: 1, Locals: []byte{0x00}, Body: []byte{0x0B}}
	i32s := func(n int) []byte { return bytes.Repeat([]byte{0x7F}, n) }
	voidTypes := func(n int) []wasmFuncType { return make([]wasmFuncType, n) }
	fillers := func(n int) []wasmFunc {
		out := make([]wasmFunc, n)
		for i := range out {
			out[i] = filler
		}
		return out
	}
	heavy := func(n uint32) wasmFunc {
		return wasmFunc{TypeIdx: 1, Locals: localsDecl(n, i64), Body: []byte{0x0B}}
	}

	// reject appends a validation_rejection row for bad after checking that good, bad with its
	// one defect undone, validates and instruments.
	reject := func(id, purpose string, bad, good wasmModuleSpec) {
		goodRaw := buildWasmModule(good)
		if err := module.Validate(goodRaw, moduleLimits()); err != nil {
			t.Fatalf("%s: module with its defect undone: Validate: %v", id, err)
		}
		if _, err := module.Instrument(goodRaw, moduleLimits()); err != nil {
			t.Fatalf("%s: module with its defect undone: Instrument: %v", id, err)
		}
		fixture := buildWasmModule(bad)
		specs = append(specs, vectorSpec{
			ID:              id,
			Purpose:         purpose,
			Shape:           "validation_rejection",
			BatchID:         4,
			Ctx:             stdCtx(nextIndex()),
			Args:            codec.DeployFinalizeArgs{CodeHash: CodeHash(fixture)},
			Module:          fixture,
			PendingMetadata: immutableMetadata,
			WantStatus:      codec.StatusValidationFailed,
		})
	}

	{
		bad, good := base(), base()
		bad.Types = append(bad.Types, voidTypes(1024)...)
		good.Types = append(good.Types, voidTypes(1023)...)
		reject("CV-STRUCT-TYPES-1", "a deploy of a module declaring `MaxTypeCount` + 1 (1,025) function types is `VALIDATION_FAILED`", bad, good)
	}
	{
		bad, good := base(), base()
		bad.Types = append(bad.Types, wasmFuncType{Params: i32s(8193)})
		good.Types = append(good.Types, wasmFuncType{Params: i32s(8192)})
		reject("CV-STRUCT-TYPEPARAMS-1", "a deploy of a module declaring an unused function type with `MaxLocalsPerFunction` + 1 (8,193) parameters is `VALIDATION_FAILED`, though the type section's total stays under `MaxTotalParams`", bad, good)
	}
	{
		bad, good := base(), base()
		for i := 0; i < 7; i++ {
			bad.Types = append(bad.Types, wasmFuncType{Params: i32s(8192)})
			good.Types = append(good.Types, wasmFuncType{Params: i32s(8192)})
		}
		bad.Types = append(bad.Types, wasmFuncType{Params: i32s(8191)})
		reject("CV-STRUCT-PARAMS-1", "a deploy of a module whose function types declare `MaxTotalParams` + 1 (65,537) parameters in total, none above `MaxLocalsPerFunction`, is `VALIDATION_FAILED`", bad, good)
	}
	{
		bad, good := base(), base()
		bad.Types = append(bad.Types, void)
		bad.Funcs = append(bad.Funcs, fillers(8192)...)
		good.Types = append(good.Types, void)
		good.Funcs = append(good.Funcs, fillers(8191)...)
		reject("CV-STRUCT-FUNCS-1", "a deploy of a module defining `MaxFunctions` + 1 (8,193) functions is `VALIDATION_FAILED`", bad, good)
	}
	{
		bad, good := base(), base()
		bad.TableMin = MaxTableEntries + 1
		good.TableMin = MaxTableEntries
		reject("CV-STRUCT-TABLE-1", "a deploy of a module whose table declares a minimum of `MaxTableEntries` + 1 (10,001) entries is `VALIDATION_FAILED`", bad, good)
	}
	{
		bad, good := base(), base()
		bad.Globals = MaxGlobals + 1
		good.Globals = MaxGlobals
		reject("CV-STRUCT-GLOBALS-1", "a deploy of a module declaring `MaxGlobals` + 1 (1,025) globals is `VALIDATION_FAILED`", bad, good)
	}
	{
		elems := func(second int) wasmModuleSpec {
			s := base()
			s.TableMin = MaxTableEntries
			s.Elements = []wasmElem{{Offset: 0, Funcs: make([]uint32, 10000)}, {Offset: 0, Funcs: make([]uint32, second)}}
			return s
		}
		reject("CV-STRUCT-ELEMS-1", "a deploy of a module whose two element segments hold `MaxTotalElementEntries` + 1 (16,385) entries in total, each within the table, is `VALIDATION_FAILED`", elems(6385), elems(6384))
	}
	{
		bad, good := base(), base()
		bad.Data = make([]dataSeg, MaxDataSegments+1)
		good.Data = make([]dataSeg, MaxDataSegments)
		reject("CV-STRUCT-DATA-1", "a deploy of a module declaring `MaxDataSegments` + 1 (1,025) data segments is `VALIDATION_FAILED`", bad, good)
	}
	{
		locals := func(n uint32) wasmModuleSpec {
			return runModuleSpec(nil, 1, localsDecl(n, i64), []byte{0x42, 0x00, 0x0B}, nil)
		}
		reject("CV-STRUCT-LOCALS-1", "a deploy of a module whose `run` has 2 parameters and 8,191 declared locals, `MaxLocalsPerFunction` + 1 in total, is `VALIDATION_FAILED`: parameters count toward the cap", locals(8191), locals(8190))
	}
	{
		bad, good := base(), base()
		bad.Types = append(bad.Types, void)
		good.Types = append(good.Types, void)
		for i := 0; i < 127; i++ {
			bad.Funcs = append(bad.Funcs, heavy(8192))
			good.Funcs = append(good.Funcs, heavy(8192))
		}
		bad.Funcs = append(bad.Funcs, heavy(8191))
		reject("CV-STRUCT-TOTALLOCALS-1", "a deploy of a module whose parameters and declared locals sum to `MaxTotalLocals` + 1 across its functions, with the declared locals alone one below the cap, is `VALIDATION_FAILED`: parameters count toward the cap", bad, good)
	}

	{
		atCap := base()
		atCap.Types = append(atCap.Types, void)
		for i := 0; i < 126; i++ {
			atCap.Types = append(atCap.Types, wasmFuncType{Params: i32s(65)})
		}
		for i := 0; i < 896; i++ {
			atCap.Types = append(atCap.Types, wasmFuncType{Params: i32s(64)})
		}
		atCap.Funcs = append(atCap.Funcs, fillers(MaxFunctions-1)...)
		atCap.Funcs[1] = heavy(MaxLocalsPerFunction)
		atCap.Globals = MaxGlobals
		atCap.TableMin = MaxTableEntries
		atCap.Elements = []wasmElem{
			{Offset: 0, Funcs: make([]uint32, MaxTableEntries)},
			{Offset: 0, Funcs: make([]uint32, MaxTotalElementEntries-MaxTableEntries)},
		}
		atCap.Data = append(make([]dataSeg, MaxDataSegments-1), dataSeg{offset: 65535, bytes: []byte{0xAA}})
		fixture := buildWasmModule(atCap)
		specs = append(specs, vectorSpec{
			ID:              "CV-STRUCT-ATCAP-1",
			Purpose:         "a deploy of a module exactly at `MaxTypeCount`, `MaxTotalParams`, `MaxFunctions`, `MaxLocalsPerFunction`, `MaxGlobals`, `MaxDataSegments`, `MaxTableEntries` and `MaxTotalElementEntries`, with an element segment ending exactly at the table minimum and a data segment ending exactly at the end of initial memory, succeeds",
			Shape:           "deploy",
			BatchID:         4,
			Ctx:             stdCtx(nextIndex()),
			Args:            codec.DeployFinalizeArgs{CodeHash: CodeHash(fixture)},
			Module:          fixture,
			PendingMetadata: immutableMetadata,
			WantStatus:      codec.StatusSuccess,
		})
	}

	{
		bad, good := base(), base()
		bad.Data = []dataSeg{{offset: 65535, bytes: []byte{0xAA, 0xBB}}}
		good.Data = []dataSeg{{offset: 65534, bytes: []byte{0xAA, 0xBB}}}
		reject("CV-FIT-DATA-1", "a deploy of a module whose active data segment at offset 65,535 holds 2 bytes, one past its 1-page initial memory, is `VALIDATION_FAILED` at deploy, not at its first call", bad, good)
	}
	{
		elem := func(offset uint32) wasmModuleSpec {
			s := base()
			s.TableMin = 2
			s.Elements = []wasmElem{{Offset: offset, Funcs: []uint32{0, 0}}}
			return s
		}
		reject("CV-FIT-ELEM-1", "a deploy of a module whose active element segment at offset 1 holds 2 entries, one past its table minimum of 2, is `VALIDATION_FAILED` at deploy, not at its first call", elem(1), elem(0))
	}

	{
		bad := withImports("storage_len")
		bad.Imports[0].Module = "zenon"
		reject("CV-IMPORT-MODULE-1", "a deploy of a module importing `storage_len`, with its signature, from module `zenon` rather than `env` is `VALIDATION_FAILED`", bad, withImports("storage_len"))
	}
	{
		bad := withImports("storage_len")
		bad.Imports[0].Name = "storage_write"
		reject("CV-IMPORT-NAME-1", "a deploy of a module importing `env.storage_write`, a name outside the three host functions, is `VALIDATION_FAILED`", bad, withImports("storage_len"))
	}
	reject("CV-IMPORT-DUP-1", "a deploy of a module importing `env.storage_len` twice is `VALIDATION_FAILED`", withImports("storage_len", "storage_len"), withImports("storage_len"))
	{
		bad := withImports("abort")
		bad.Types[1] = wasmFuncType{Params: []byte{0x7F}}
		reject("CV-IMPORT-SIG-1", "a deploy of a module importing `env.abort` with the one-parameter signature `(i32) -> ()` is `VALIDATION_FAILED`", bad, withImports("abort"))
	}
	{
		bad := withImports("storage_len")
		bad.Imports[0].Desc = []byte{0x03, 0x7F, 0x00}
		bad.Exports[1].Index = 0
		reject("CV-IMPORT-KIND-1", "a deploy of a module importing `env.storage_len` as an `i32` global rather than a function is `VALIDATION_FAILED`", bad, withImports("storage_len"))
	}

	{
		one := uint32(1)
		bad, good := base(), base()
		bad.Types = append(bad.Types, void)
		bad.Funcs = append(bad.Funcs, filler)
		bad.Start = &one
		good.Types = append(good.Types, void)
		good.Funcs = append(good.Funcs, filler)
		reject("CV-SECTION-START-1", "a deploy of a module with a start section naming a `() -> ()` function is `VALIDATION_FAILED`", bad, good)
	}
	{
		bad := base()
		bad.Custom = []wasmCustom{{Name: "name", Payload: []byte{0x00, 0x02, 0x01, 0x6D}}}
		reject("CV-SECTION-CUSTOM-1", "a deploy of a module whose only defect is a trailing `name` custom section is `VALIDATION_FAILED`", bad, base())
	}
	{
		bad := base()
		bad.Custom = []wasmCustom{{Name: "zeno"}}
		reject("CV-SECTION-CUSTOM-2", "a deploy of a module whose only defect is a trailing empty custom section named `zeno` is `VALIDATION_FAILED`", bad, base())
	}

	{
		bad, good := base(), base()
		bad.Globals, good.Globals = 1, 1
		bad.Exports = append(bad.Exports, wasmExport{Name: "g", Kind: 0x03, Index: 0})
		reject("CV-EXPORT-MUTGLOBAL-1", "a deploy of a module exporting a mutable `i32` global is `VALIDATION_FAILED`", bad, good)
	}
	{
		bad, good := base(), base()
		bad.Exports = append(bad.Exports, wasmExport{Name: "_start", Kind: 0x00, Index: 0})
		good.Exports = append(good.Exports, wasmExport{Name: "start", Kind: 0x00, Index: 0})
		reject("CV-EXPORT-START-1", "a deploy of a module that also exports `run` under the name `_start` is `VALIDATION_FAILED`", bad, good)
	}
	{
		bad, good := base(), base()
		bad.Exports = append(bad.Exports, wasmExport{Name: "__zenon_gas", Kind: 0x00, Index: 0})
		good.Exports = append(good.Exports, wasmExport{Name: "zenon_gas", Kind: 0x00, Index: 0})
		reject("CV-EXPORT-ZENON-1", "a deploy of a module that also exports `run` under the name `__zenon_gas`, which the instrumenter never injects, is `VALIDATION_FAILED`: every name beginning `__zenon_` is reserved", bad, good)
	}
	{
		bad := base()
		bad.Exports = []wasmExport{{Name: "memory", Kind: 0x02, Index: 0}}
		reject("CV-EXPORT-NORUN-1", "a deploy of a module exporting `memory` but no `run` is `VALIDATION_FAILED`", bad, base())
	}
	{
		bad := base()
		bad.Exports = []wasmExport{{Name: "run", Kind: 0x00, Index: 0}}
		reject("CV-EXPORT-NOMEMORY-1", "a deploy of a module that declares a memory but exports only `run` is `VALIDATION_FAILED`", bad, base())
	}
	{
		bad := runModuleSpec(nil, 1, []byte{0x00}, []byte{0x41, 0x00, 0x0B}, nil)
		bad.Types[0].Results = []byte{0x7F}
		reject("CV-EXPORT-RUNSIG-1", "a deploy of a module whose `run` is `(i32, i32) -> i32` is `VALIDATION_FAILED`", bad, base())
	}
	{
		bad := withImports("storage_len")
		bad.Exports[1].Index = 0
		reject("CV-EXPORT-RUNIMPORT-1", "a deploy of a module whose `run` export names its `env.storage_len` import, which has `run`'s signature, is `VALIDATION_FAILED`: `run` must be a function the module defines", bad, withImports("storage_len"))
	}

	{
		returns := func(n int) wasmModuleSpec {
			body := append([]byte{0x42, 0x00}, bytes.Repeat([]byte{0x0F}, n)...)
			return runModuleSpec(nil, 1, []byte{0x00}, append(body, 0x0B), nil)
		}
		reject("CV-INSTR-SIZE-1", "a deploy of a module under `MaxCodeSize` whose `run` is `i64.const 0` then 151,000 `return`s: each `return` instruments to 7 bytes, so the instrumented module exceeds `MaxInstrumentedCodeSize` (1 MiB) and the deploy is `VALIDATION_FAILED`", returns(instrSizeReturns), returns(1000))
	}
	{
		bad := runModuleSpec(nil, 1, []byte{0x80, 0x00}, []byte{0x42, 0x00, 0x0B}, nil)
		reject("CV-LEB-PADDED-1", "a deploy of a module whose only defect is `run`'s locals vector count 0 encoded in two bytes (`80 00`), within Core 1.0's five-byte limit, is `VALIDATION_FAILED`: every LEB128 integer must use its shortest encoding", bad, base())
	}

	return specs
}
