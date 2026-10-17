package wasm

import (
	"math"
	"testing"

	"github.com/zenon-network/go-zenon/common/crypto"
	"github.com/zenon-network/go-zenon/common/types"

	"github.com/digitalSloth/go-executor/domain"
	"github.com/digitalSloth/go-executor/internal/codec"
	"github.com/digitalSloth/go-executor/plugins/wasm/internal/module"
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

// rejectionReasons binds each validator rejection vector to the
// validator rule its module exercises. A validation_rejection vector
// missing from this map fails the suite.
var rejectionReasons = map[string]error{
	"CV-REJECT-WASI-1":    module.ErrDisallowedImport,
	"CV-REJECT-FLOAT-1":   module.ErrFloatInstruction,
	"CV-REJECT-ATOMIC-1":  module.ErrAtomics,
	"CV-REJECT-SIMD-1":    module.ErrSIMD,
	"CV-REJECT-SIGNEXT-1": module.ErrSignExtension,
	"CV-REJECT-SATCONV-1": module.ErrSaturatingConversion,
	"CV-DEPLOY-META-1":    codec.ErrUnknownVersion,
	"CV-DEPLOY-META-2":    codec.ErrTruncated,
	"CV-DEPLOY-META-3":    codec.ErrTrailingBytes,
	"CV-DEPLOY-META-4":    codec.ErrInvalidPolicy,
	"CV-DEPLOY-META-5":    codec.ErrInvalidOwner,
	"CV-DEPLOY-META-6":    codec.ErrInvalidOwner,
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

	return specs
}
