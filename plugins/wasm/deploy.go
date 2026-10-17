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

// Upgrade policy byte values stored under the 0x0003 reserved key.
const (
	policyImmutable        = codec.PolicyImmutable
	policyOwnerUpgradeable = codec.PolicyOwnerUpgradeable
)

// pendingRecordPrefixSize is the fixed width of encodePendingRecord's prefix, before the raw
// metadata bytes: metadata_hash(32) + total_size(8) + chunk_count(4).
const pendingRecordPrefixSize = 44

// pendingRecord is the decoded kind-2 record DecodeInput wrote to the
// local store. MetadataHash is decoded for round-trip fidelity with
// encodePendingRecord. The deployer is not stored: the record's key
// carries it. Metadata feeds finalize's metadata validation.
type pendingRecord struct {
	MetadataHash types.Hash
	TotalSize    uint64
	ChunkCount   uint32
	Metadata     []byte
}

// decodePendingRecord is encodePendingRecord's inverse. ok is false
// only when b is shorter than pendingRecordPrefixSize; the caller turns
// that into a halt. Metadata is a copy of b[pendingRecordPrefixSize:].
func decodePendingRecord(b []byte) (pendingRecord, bool) {
	if len(b) < pendingRecordPrefixSize {
		return pendingRecord{}, false
	}
	var rec pendingRecord
	copy(rec.MetadataHash[:], b[0:32])
	rec.TotalSize = binary.BigEndian.Uint64(b[32:40])
	rec.ChunkCount = binary.BigEndian.Uint32(b[40:44])
	rec.Metadata = append([]byte(nil), b[pendingRecordPrefixSize:]...)
	return rec, true
}

// prepareModule runs the finalize steps numbered in its body against a
// pre-instrumentation code_hash, and memoizes both derived module tiers
// on success. It reads the pending record and chunks keyed by
// (deployer, preCodeHash), so it only ever sees the given deployer's
// own staged deployment. It returns the derived CodeHash and the
// pending record's metadata for the caller's metadata validation.
//
// Call-site contract, mandatory and in this order:
//
//	codeHash, metadata, ok, err := p.prepareModule(deployer, h)
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
func (p *Plugin) prepareModule(deployer types.Address, preCodeHash types.Hash) (codeHash types.Hash, metadata []byte, ok bool, err error) {
	// 1. Pending record.
	recBytes, ok, err := p.store.get(entryPendingRecord, pendingRecordKey(deployer, preCodeHash))
	if err != nil {
		return types.Hash{}, nil, false, fmt.Errorf("wasm: prepare module: pending record for code hash %x: %w", preCodeHash, err)
	}
	if !ok {
		return types.Hash{}, nil, false, fmt.Errorf("wasm: prepare module: no pending record for code hash %x; local recovery required", preCodeHash)
	}
	record, ok := decodePendingRecord(recBytes)
	if !ok {
		return types.Hash{}, nil, false, fmt.Errorf("wasm: prepare module: pending record for code hash %x has unexpected length %d; local recovery required", preCodeHash, len(recBytes))
	}

	// 2. Record bounds, before any chunk lookup.
	if record.ChunkCount < 1 || record.ChunkCount > MaxChunkCount {
		return types.Hash{}, nil, false, nil
	}
	if record.TotalSize < 1 || record.TotalSize > MaxCodeSize {
		return types.Hash{}, nil, false, nil
	}
	wantChunkCount := uint32((record.TotalSize + MaxDeployChunkData - 1) / MaxDeployChunkData)
	if record.ChunkCount != wantChunkCount {
		return types.Hash{}, nil, false, nil
	}

	// 3. Chunks.
	assembled := make([]byte, 0, record.TotalSize)
	for i := uint32(0); i < record.ChunkCount; i++ {
		chunk, ok, err := p.store.get(entryDeployChunk, deployChunkKey(deployer, preCodeHash, i))
		if err != nil {
			return types.Hash{}, nil, false, fmt.Errorf("wasm: prepare module: chunk %d for code hash %x: %w", i, preCodeHash, err)
		}
		if !ok {
			return types.Hash{}, nil, false, fmt.Errorf("wasm: prepare module: missing chunk %d for code hash %x; local recovery required", i, preCodeHash)
		}
		assembled = append(assembled, chunk...)
	}

	// 4. Assembly verification.
	if uint64(len(assembled)) != record.TotalSize || CodeHash(assembled) != preCodeHash {
		return types.Hash{}, nil, false, nil
	}

	// 5. Derive.
	instrumented, derivedHash, ok := p.deriveModule(assembled)
	if !ok {
		return types.Hash{}, nil, false, nil
	}

	// 6. Memoize.
	if err := p.store.put(entryAssembledModule, preCodeHash[:], assembled); err != nil {
		return types.Hash{}, nil, false, fmt.Errorf("wasm: prepare module: memoize assembled module for code hash %x: %w", preCodeHash, err)
	}
	if err := p.store.put(entryInstrumentedModule, derivedHash[:], instrumented); err != nil {
		return types.Hash{}, nil, false, fmt.Errorf("wasm: prepare module: memoize instrumented module for code hash %x: %w", derivedHash, err)
	}

	return derivedHash, record.Metadata, true, nil
}

// deriveModule is prepareModule's step 5 (Derive): validation, the
// engine compile of the original, instrumentation, the engine compile
// of the instrumented output, and the CodeHash derivation. It has no
// error return because none of these steps can ever be a halt: every
// one of them is a pure, deterministic function of the assembled bytes
// under the pinned engine, so a rejection here is a rejection on every
// node. ok == false is VALIDATION_FAILED.
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

// applyDeployFinalize serves input_kind 4. It takes no StateAccess: a
// finalize reads no state, so its witness set is empty on every path.
func (p *Plugin) applyDeployFinalize(ctx domain.InputContext, args codec.DeployFinalizeArgs) (domain.Effects, error) {
	codeHash, metadata, ok, err := p.prepareModule(ctx.L1Sender, args.CodeHash)
	if err != nil {
		return domain.Effects{}, fmt.Errorf("wasm: apply deploy finalize: %w", err)
	}

	if !ok {
		return domain.Effects{Status: codec.StatusValidationFailed}, nil
	}

	meta, err := codec.DecodeContractMetadata(metadata)
	if err != nil {
		return domain.Effects{Status: codec.StatusValidationFailed}, nil
	}

	// The deployer comes from this finalize input's own L1Sender, the same value that keys the
	// pending record and chunks prepareModule read, so only ctx.L1Sender, which is
	// consensus-sourced, feeds the ContractID.
	id := contractID(ctx.L1Sender, ctx.GlobalInputIndex, p.chainID)

	entries := []domain.StateDiffEntry{
		{Path: derive(id, keyCodeHash), Value: codeHash[:]},
		{Path: derive(id, keyPreCodeHash), Value: args.CodeHash[:]},
		{Path: derive(id, keyUpgradePolicy), Value: []byte{meta.UpgradePolicy}},
	}
	if meta.UpgradePolicy == policyOwnerUpgradeable {
		owner := callerHash(meta.Owner)
		entries = append(entries, domain.StateDiffEntry{Path: derive(id, keyOwner), Value: owner[:]})
	}
	diff := sortStateDiff(entries)

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

// applyUpgrade serves input_kind 5.
func (p *Plugin) applyUpgrade(state domain.StateAccess, ctx domain.InputContext, args codec.UpgradeArgs) (domain.Effects, error) {
	// Unconditional, in this fixed order, before any branch — the witness set is committed in
	// DAHash and must not depend on which authorization branch is taken.
	codeHashLeaf, codeHashPresent, err := state.Read(derive(args.ContractID, keyCodeHash))
	if err != nil {
		return domain.Effects{}, fmt.Errorf("wasm: apply upgrade: read code hash: %w", err)
	}
	// keyPreCodeHash's value is never used on this path; it is read only to keep the witness
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

	if !ownerAuthorized(policyLeaf, policyPresent, ownerLeaf, ownerPresent, callerHash(ctx.L1Sender)) {
		return domain.Effects{Status: codec.StatusUnauthorized, ContractID: args.ContractID, CodeHash: oldCodeHash}, nil
	}

	codeHash, _, ok, err := p.prepareModule(ctx.L1Sender, args.NewCodeHash)
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

// ownerAuthorized reports whether the caller is the contract's current owner: the policy leaf
// must be present, exactly one byte wide and OWNER_UPGRADEABLE, and the owner leaf must be
// present and equal to caller. A policy leaf of any other width is unauthorized, for both
// upgrade and transfer.
func ownerAuthorized(policyLeaf []byte, policyPresent bool, ownerLeaf []byte, ownerPresent bool, caller types.Hash) bool {
	return policyPresent && len(policyLeaf) == 1 && policyLeaf[0] == policyOwnerUpgradeable &&
		ownerPresent && bytes.Equal(ownerLeaf, caller[:])
}

// applyTransferOwnership serves input_kind 6.
func (p *Plugin) applyTransferOwnership(state domain.StateAccess, ctx domain.InputContext, args codec.TransferOwnershipArgs) (domain.Effects, error) {
	// Unconditional, in this order, before any branch: the runtime writes these reserved keys
	// itself, so the step-3 reserved-key scan is not involved and this read order alone fixes
	// the witness set.
	codeHashLeaf, codeHashPresent, err := state.Read(derive(args.ContractID, keyCodeHash))
	if err != nil {
		return domain.Effects{}, fmt.Errorf("wasm: apply transfer ownership: read code hash: %w", err)
	}
	policyLeaf, policyPresent, err := state.Read(derive(args.ContractID, keyUpgradePolicy))
	if err != nil {
		return domain.Effects{}, fmt.Errorf("wasm: apply transfer ownership: read upgrade policy: %w", err)
	}
	ownerLeaf, ownerPresent, err := state.Read(derive(args.ContractID, keyOwner))
	if err != nil {
		return domain.Effects{}, fmt.Errorf("wasm: apply transfer ownership: read owner: %w", err)
	}

	if codeHashPresent && len(codeHashLeaf) != len(types.Hash{}) {
		return domain.Effects{}, fmt.Errorf("wasm: apply transfer ownership: code hash leaf has width %d, want %d", len(codeHashLeaf), len(types.Hash{}))
	}

	if !codeHashPresent {
		return domain.Effects{Status: codec.StatusValidationFailed, ContractID: args.ContractID}, nil
	}
	var current types.Hash
	copy(current[:], codeHashLeaf)

	if !ownerAuthorized(policyLeaf, policyPresent, ownerLeaf, ownerPresent, callerHash(ctx.L1Sender)) {
		return domain.Effects{Status: codec.StatusUnauthorized, ContractID: args.ContractID, CodeHash: current}, nil
	}

	var diff []domain.StateDiffEntry
	if args.NewOwner != (types.Address{}) {
		newOwner := callerHash(args.NewOwner)
		diff = []domain.StateDiffEntry{{Path: derive(args.ContractID, keyOwner), Value: newOwner[:]}}
	} else {
		diff = sortStateDiff([]domain.StateDiffEntry{
			{Path: derive(args.ContractID, keyUpgradePolicy), Value: []byte{policyImmutable}},
			{Path: derive(args.ContractID, keyOwner), Value: nil},
		})
	}

	return domain.Effects{
		Status:     codec.StatusSuccess,
		GasUsed:    0,
		ContractID: args.ContractID,
		CodeHash:   current,
		StateDiff:  diff,
	}, nil
}

// contractID derives a contract's id: SHA3-256(deployer_l1_address(20)
// ‖ globalInputIndex(8B BE) ‖ chain_id(32)), over the raw 20-byte L1
// address.
func contractID(deployer types.Address, globalInputIndex uint64, chainID types.Hash) types.Hash {
	var idxBuf [8]byte
	binary.BigEndian.PutUint64(idxBuf[:], globalInputIndex)
	return hashOf(crypto.Hash(deployer[:], idxBuf[:], chainID[:]))
}
