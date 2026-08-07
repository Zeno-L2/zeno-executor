package wasm

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"

	"github.com/zenon-network/go-zenon/common/crypto"
	"github.com/zenon-network/go-zenon/common/types"

	"github.com/digitalSloth/go-executor/domain"
	"github.com/digitalSloth/go-executor/internal/codec"
	"github.com/digitalSloth/go-executor/plugins/wasm/internal/engine"
	"github.com/digitalSloth/go-executor/plugins/wasm/internal/module"
)

// Upgrade policy byte values stored under the 0x0003 reserved key (D2).
const (
	policyImmutable        byte = 0
	policyOwnerUpgradeable byte = 1
)

// pendingRecordSize is the fixed width of encodePendingRecord's output:
// metadata_hash(32) + total_size(8) + chunk_count(4) + deployer_l1_address(20).
const pendingRecordSize = 64

// pendingRecord is the decoded kind-2 record DecodeInput wrote to the local store. Only
// TotalSize and ChunkCount are used; MetadataHash has no consumer (§12's policy declaration has
// no transport, D28) and Deployer is decoded for round-trip fidelity with encodePendingRecord
// and is deliberately read by nothing on the consensus path (see contractID).
type pendingRecord struct {
	MetadataHash types.Hash
	TotalSize    uint64
	ChunkCount   uint32
	Deployer     types.Address
}

// decodePendingRecord is encodePendingRecord's inverse. ok is false for any length other than
// pendingRecordSize; the caller turns that into a halt (D30).
func decodePendingRecord(b []byte) (pendingRecord, bool) {
	if len(b) != pendingRecordSize {
		return pendingRecord{}, false
	}
	var rec pendingRecord
	copy(rec.MetadataHash[:], b[0:32])
	rec.TotalSize = binary.BigEndian.Uint64(b[32:40])
	rec.ChunkCount = binary.BigEndian.Uint32(b[40:44])
	copy(rec.Deployer[:], b[44:64])
	return rec, true
}

// prepareModule runs §11.3's finalize steps 1-6 plus step 7's CodeHash derivation against a
// pre-instrumentation code_hash, and memoizes both derived module tiers on success. It returns
// only the derived CodeHash: nothing else it computes is consensus-visible, and in particular
// the pending record's recorded deployer must never leave this function (see contractID).
//
// Call-site contract, mandatory and in this order:
//
//	codeHash, ok, err := p.prepareModule(h)
//	if err != nil {
//		return domain.Effects{}, err   // halt: this node cannot see what other nodes can
//	}
//	if !ok {
//		… VALIDATION_FAILED …          // every node with the same chunks agrees
//	}
//
// err MUST be tested before ok. When err != nil the value of ok is undefined and meaningless;
// testing ok first would silently commit a halt as a status. The shape is codeStore.get's and
// domain.StateAccess.Read's (value, ok, err), which every existing call site in call.go already
// handles error-first.
func (p *Plugin) prepareModule(preCodeHash types.Hash) (types.Hash, bool, error) {
	// 1. Pending record.
	recBytes, ok, err := p.store.get(entryPendingRecord, preCodeHash[:])
	if err != nil {
		return types.Hash{}, false, fmt.Errorf("wasm: prepare module: pending record for code hash %x: %w", preCodeHash, err)
	}
	if !ok {
		return types.Hash{}, false, fmt.Errorf("wasm: prepare module: no pending record for code hash %x; local recovery required", preCodeHash)
	}
	record, ok := decodePendingRecord(recBytes)
	if !ok {
		return types.Hash{}, false, fmt.Errorf("wasm: prepare module: pending record for code hash %x has unexpected length %d; local recovery required", preCodeHash, len(recBytes))
	}

	// 2. Record bounds, before any chunk lookup.
	if record.ChunkCount < 1 || record.ChunkCount > MaxChunkCount {
		return types.Hash{}, false, nil
	}
	if record.TotalSize < 1 || record.TotalSize > MaxCodeSize {
		return types.Hash{}, false, nil
	}
	wantChunkCount := uint32((record.TotalSize + MaxDeployChunkData - 1) / MaxDeployChunkData)
	if record.ChunkCount != wantChunkCount {
		return types.Hash{}, false, nil
	}

	// 3. Chunks.
	assembled := make([]byte, 0, record.TotalSize)
	for i := uint32(0); i < record.ChunkCount; i++ {
		chunk, ok, err := p.store.get(entryDeployChunk, deployChunkKey(preCodeHash, i))
		if err != nil {
			return types.Hash{}, false, fmt.Errorf("wasm: prepare module: chunk %d for code hash %x: %w", i, preCodeHash, err)
		}
		if !ok {
			return types.Hash{}, false, fmt.Errorf("wasm: prepare module: missing chunk %d for code hash %x; local recovery required", i, preCodeHash)
		}
		assembled = append(assembled, chunk...)
	}

	// 4. Assembly verification.
	if uint64(len(assembled)) != record.TotalSize || CodeHash(assembled) != preCodeHash {
		return types.Hash{}, false, nil
	}

	// 5. Derive.
	instrumented, codeHash, ok := p.deriveModule(assembled)
	if !ok {
		return types.Hash{}, false, nil
	}

	// 6. Memoize.
	if err := p.store.put(entryAssembledModule, preCodeHash[:], assembled); err != nil {
		return types.Hash{}, false, fmt.Errorf("wasm: prepare module: memoize assembled module for code hash %x: %w", preCodeHash, err)
	}
	if err := p.store.put(entryInstrumentedModule, codeHash[:], instrumented); err != nil {
		return types.Hash{}, false, fmt.Errorf("wasm: prepare module: memoize instrumented module for code hash %x: %w", codeHash, err)
	}

	return codeHash, true, nil
}

// deriveModule runs finalize steps 5-7 over assembled bytes: validation, the engine compile of
// the original, instrumentation, the engine compile of the instrumented output, and the CodeHash
// derivation. It has no error return because none of these steps can ever be a halt (D22, D31):
// every one of them is a pure, deterministic function of the assembled bytes under the pinned
// engine, so a rejection here is a rejection on every node. ok == false is VALIDATION_FAILED.
func (p *Plugin) deriveModule(assembled []byte) (instrumented []byte, codeHash types.Hash, ok bool) {
	defer func() {
		if r := recover(); r != nil {
			instrumented = nil
			codeHash = types.Hash{}
			ok = false
		}
	}()

	if err := module.Validate(assembled, moduleLimits()); err != nil {
		return nil, types.Hash{}, false
	}
	if err := engine.Compile(context.Background(), assembled); err != nil {
		return nil, types.Hash{}, false
	}
	inst, err := module.Instrument(assembled, moduleLimits())
	if err != nil {
		return nil, types.Hash{}, false
	}
	if err := engine.Compile(context.Background(), inst); err != nil {
		return nil, types.Hash{}, false
	}
	return inst, CodeHash(inst), true
}

// applyDeployFinalize serves input_kind 4 (§11.3). It takes no StateAccess: a finalize reads no
// state, so its witness set is empty on every path.
func (p *Plugin) applyDeployFinalize(ctx domain.InputContext, args codec.DeployFinalizeArgs) (domain.Effects, error) {
	codeHash, ok, err := p.prepareModule(args.CodeHash)
	if err != nil {
		return domain.Effects{}, fmt.Errorf("wasm: apply deploy finalize: %w", err)
	}
	if !ok {
		return domain.Effects{Status: codec.StatusValidationFailed}, nil
	}

	// The deployer comes from this finalize input's own L1Sender, never from the pending
	// record: §11.3 requires the finalize caller to be the recorded deployer and Settlement
	// verifies that on-chain, so the two values agree under every honest scenario, but only
	// ctx.L1Sender is consensus-sourced. Reading the record's deployer would let a stale local
	// entry (code_hash is reusable across attempts) derive a different ContractID than honest
	// nodes and silently commit a divergent state/receipt root.
	id := contractID(ctx.L1Sender, ctx.GlobalInputIndex, p.chainID)

	diff := sortStateDiff([]domain.StateDiffEntry{
		{Path: derive(id, keyCodeHash), Value: codeHash[:]},
		{Path: derive(id, keyPreCodeHash), Value: args.CodeHash[:]},
		{Path: derive(id, keyUpgradePolicy), Value: []byte{policyImmutable}},
	})

	returnData := append(append([]byte{}, args.CodeHash[:]...), codeHash[:]...)

	return domain.Effects{
		Status:     codec.StatusSuccess,
		GasUsed:    0,
		ContractID: id,
		CodeHash:   codeHash,
		StateDiff:  diff,
		ReturnData: returnData,
	}, nil
}

// applyUpgrade serves input_kind 5 (§12).
func (p *Plugin) applyUpgrade(state domain.StateAccess, ctx domain.InputContext, args codec.UpgradeArgs) (domain.Effects, error) {
	// D20: unconditional, in this fixed order, before any branch — the witness set is committed
	// in DAHash and must not depend on which authorization branch is taken.
	codeHashLeaf, codeHashPresent, err := state.Read(derive(args.ContractID, keyCodeHash))
	if err != nil {
		return domain.Effects{}, fmt.Errorf("wasm: apply upgrade: read code hash: %w", err)
	}
	// keyPreCodeHash's value is never used on this path; it is read only to keep D20's witness
	// set a pure, branch-independent function of state.
	_, _, err = state.Read(derive(args.ContractID, keyPreCodeHash))
	if err != nil {
		return domain.Effects{}, fmt.Errorf("wasm: apply upgrade: read pre code hash: %w", err)
	}
	policyLeaf, policyPresent, err := state.Read(derive(args.ContractID, keyUpgradePolicy))
	if err != nil {
		return domain.Effects{}, fmt.Errorf("wasm: apply upgrade: read upgrade policy: %w", err)
	}
	ownerLeaf, ownerPresent, err := state.Read(derive(args.ContractID, keyOwner))
	if err != nil {
		return domain.Effects{}, fmt.Errorf("wasm: apply upgrade: read owner: %w", err)
	}

	if codeHashPresent && len(codeHashLeaf) != len(types.Hash{}) {
		return domain.Effects{}, fmt.Errorf("wasm: apply upgrade: code hash leaf has width %d, want %d", len(codeHashLeaf), len(types.Hash{}))
	}

	if !codeHashPresent {
		return domain.Effects{Status: codec.StatusValidationFailed, ContractID: args.ContractID}, nil
	}
	var oldCodeHash types.Hash
	copy(oldCodeHash[:], codeHashLeaf)

	caller := callerHash(ctx.L1Sender)
	authorized := policyPresent && len(policyLeaf) == 1 && policyLeaf[0] == policyOwnerUpgradeable &&
		ownerPresent && bytes.Equal(ownerLeaf, caller[:])
	if !authorized {
		return domain.Effects{Status: codec.StatusUnauthorized, ContractID: args.ContractID, CodeHash: oldCodeHash}, nil
	}

	codeHash, ok, err := p.prepareModule(args.NewCodeHash)
	if err != nil {
		return domain.Effects{}, fmt.Errorf("wasm: apply upgrade: %w", err)
	}
	if !ok {
		return domain.Effects{Status: codec.StatusValidationFailed, ContractID: args.ContractID, CodeHash: oldCodeHash}, nil
	}

	diff := sortStateDiff([]domain.StateDiffEntry{
		{Path: derive(args.ContractID, keyCodeHash), Value: codeHash[:]},
		{Path: derive(args.ContractID, keyPreCodeHash), Value: args.NewCodeHash[:]},
	})

	returnData := append(append([]byte{}, oldCodeHash[:]...), codeHash[:]...)

	return domain.Effects{
		Status:     codec.StatusSuccess,
		GasUsed:    0,
		ContractID: args.ContractID,
		CodeHash:   codeHash,
		StateDiff:  diff,
		ReturnData: returnData,
	}, nil
}

// contractID is §12's derivation (SPEC.md:640):
// SHA3-256(deployer_l1_address(20) ‖ globalInputIndex(8B BE) ‖ chain_id(32)), over the raw
// 20-byte L1 address (D4).
func contractID(deployer types.Address, globalInputIndex uint64, chainID types.Hash) types.Hash {
	var idxBuf [8]byte
	binary.BigEndian.PutUint64(idxBuf[:], globalInputIndex)
	return hashOf(crypto.Hash(deployer[:], idxBuf[:], chainID[:]))
}
