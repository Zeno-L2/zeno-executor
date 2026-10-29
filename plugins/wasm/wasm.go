// Package wasm is the WASM execution domain plugin: the domain.Domain implementation that
// interprets deterministic WebAssembly contracts. All WASM-specific logic lives here; the
// generic executor runtime never sees below this seam.
package wasm

import (
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/zenon-network/go-zenon/common/crypto"
	"github.com/zenon-network/go-zenon/common/types"

	"github.com/Zeno-L2/zeno-executor/domain"
	"github.com/Zeno-L2/zeno-executor/internal/codec"
	"github.com/Zeno-L2/zeno-executor/plugins/wasm/internal/engine"
	"github.com/Zeno-L2/zeno-executor/plugins/wasm/internal/keys"
)

// Config is the plugin's construction-time configuration.
type Config struct {
	// NetworkName selects the fixed per-network chainID ("mainnet" |
	// "testnet" | a named devnet); chainID = SHA3-256("ZENON_WASM_L2:"
	// ‖ name), computed once at construction. chain_id is not
	// per-input.
	NetworkName string
	// CodeStorePath is the directory for the local, non-consensus code
	// store.
	CodeStorePath string
	// FetchModule, when set, selects simulate mode: an in-memory code
	// tier whose assembled-module misses call it.
	FetchModule func(sourceCodeHash types.Hash) (module []byte, found bool, err error)
}

// Plugin is the WASM domain.Domain implementation.
type Plugin struct {
	cfg     Config
	chainID types.Hash
	store   codeTier
	engine  *engine.Engine
}

// New returns a WASM domain plugin, erroring on an empty NetworkName or an unusable
// CodeStorePath. With FetchModule set it keeps its code in memory and
// refuses a CodeStorePath.
func New(cfg Config) (*Plugin, error) {
	if cfg.NetworkName == "" {
		return nil, fmt.Errorf("wasm: NetworkName is required")
	}
	var store codeTier
	if cfg.FetchModule != nil {
		if cfg.CodeStorePath != "" {
			return nil, fmt.Errorf("wasm: CodeStorePath and FetchModule are mutually exclusive")
		}
		store = newMemTier(cfg.FetchModule, simModuleCacheBytes, simModuleCacheEntries)
	} else {
		disk, err := newCodeStore(cfg.CodeStorePath)
		if err != nil {
			return nil, fmt.Errorf("wasm: %w", err)
		}
		store = disk
	}
	eng, err := engine.New()
	if err != nil {
		return nil, fmt.Errorf("wasm: %w", err)
	}
	return &Plugin{
		cfg:     cfg,
		chainID: keys.ChainID(cfg.NetworkName),
		store:   store,
		engine:  eng,
	}, nil
}

// Close releases the resources the plugin's engine owns. The Plugin is unusable afterward.
func (p *Plugin) Close() error {
	return p.engine.Close()
}

// Genesis returns the empty initial state; the WASM domain seeds no leaves before its first
// input.
func (p *Plugin) Genesis() domain.StateInit {
	return domain.StateInit{}
}

// DecodeInput turns a canonical payload into a typed Input.
//
// Its error return means halt: the canonical encoding is produced by
// our own input pipeline from an account block whose structure
// Settlement's method ABI already constrained, so a malformed encoding,
// a kind-3 chunk-hash or chunk-size mismatch, or a SourceData/kind
// mismatch is an internal fault, not a consensus outcome. An input_kind
// outside the implemented set {0..7} is instead returned as
// Input{Unsupported: true} with a nil error, so Apply can emit the
// deterministic UNSUPPORTED_OPERATION status.
func (p *Plugin) DecodeInput(ctx domain.InputContext, payload []byte) (domain.Input, error) {
	in, err := codec.DecodeInput(payload)
	if err != nil {
		if errors.Is(err, codec.ErrInvalidKind) {
			return Input{Ctx: ctx, Unsupported: true}, nil
		}
		return nil, fmt.Errorf("wasm: decode input: %w", err)
	}

	kind := in.Args.Kind()
	if kind == codec.DeployChunk {
		if len(ctx.SourceData) == 0 {
			return nil, fmt.Errorf("wasm: decode input: kind 3 requires non-empty SourceData")
		}
	} else if len(ctx.SourceData) != 0 {
		return nil, fmt.Errorf("wasm: decode input: kind %d must not carry SourceData", kind)
	}

	switch args := in.Args.(type) {
	case codec.DeployChunkArgs:
		if hashOf(crypto.Hash(ctx.SourceData)) != args.ChunkHash {
			return nil, fmt.Errorf("wasm: decode input: chunk_hash mismatch for code_hash %x chunk %d", args.CodeHash, args.ChunkIndex)
		}
		if len(ctx.SourceData) > MaxDeployChunkData {
			return nil, fmt.Errorf("wasm: decode input: chunk for code_hash %x chunk %d exceeds MaxDeployChunkData", args.CodeHash, args.ChunkIndex)
		}
		if err := p.store.put(entryDeployChunk, deployChunkKey(ctx.L1Sender, args.CodeHash, args.ChunkIndex), ctx.SourceData); err != nil {
			return nil, fmt.Errorf("wasm: decode input: %w", err)
		}
	case codec.DeployStartArgs:
		// A code_hash is reusable across deployment attempts (expiry
		// frees it, and only a duplicate live record is rejected). The
		// pending record and the chunk entries are keyed by (deployer,
		// code_hash), so a start clears only its own deployer's earlier
		// attempt and never reaches another deployer's. Each chunk's
		// bytes are verified only against that input's own chunk_hash,
		// never against code_hash as a set, so a surviving stale chunk
		// would assemble into a mix that passes every per-chunk check
		// and commits a status where a node with a complete view
		// commits another. Deleting first restores the property the
		// finalize path relies on: a missing part of the prefix is
		// absence, and absence is a halt. The loop bound is a constant,
		// the deletes are unconditional, and absence is not an error,
		// so this is a pure function of the input.
		//
		// This relies on DecodeInput and Apply being interleaved per input (decode i, apply i,
		// decode i+1, ...), never batch-decoded ahead of Apply: a later DeployStart for the same
		// deployer and code_hash deletes the chunks an already-decoded, not-yet-applied DeployFinalize for an
		// earlier attempt still needs to read.
		if err := clearDeployStaging(p.store, ctx.L1Sender, args.CodeHash); err != nil {
			return nil, fmt.Errorf("wasm: decode input: %w", err)
		}
		record := encodePendingRecord(args.MetadataHash, args.TotalSize, args.ChunkCount, args.Metadata)
		if err := p.store.put(entryPendingRecord, pendingRecordKey(ctx.L1Sender, args.CodeHash), record); err != nil {
			return nil, fmt.Errorf("wasm: decode input: %w", err)
		}
	case codec.DeployFinalizeArgs:
		if err := p.markConsumed(ctx, args.CodeHash); err != nil {
			return nil, fmt.Errorf("wasm: decode input: %w", err)
		}
	case codec.UpgradeArgs:
		if err := p.markConsumed(ctx, args.NewCodeHash); err != nil {
			return nil, fmt.Errorf("wasm: decode input: %w", err)
		}
	}

	return Input{Ctx: ctx, Args: in.Args}, nil
}

// markConsumed records that the input at ctx.GlobalInputIndex consumed
// the staging of (ctx.L1Sender, codeHash). Settlement consumes the L1
// record at every admitted finalize or upgrade whatever Apply returns,
// so the mark does not depend on Apply's status. A simulate-mode store
// keeps no staging, so it keeps no mark.
func (p *Plugin) markConsumed(ctx domain.InputContext, codeHash types.Hash) error {
	s, ok := p.store.(*codeStore)
	if !ok {
		return nil
	}
	return s.put(entryConsumedDeploy, pendingRecordKey(ctx.L1Sender, codeHash), encodeConsumedDeploy(ctx.GlobalInputIndex, ctx.L1Sender, codeHash))
}

// Prune implements domain.Pruner. It deletes the chunks and pending
// record of every (deployer, code_hash) whose finalize or upgrade sits
// at or below throughSeq, then the mark itself. The mark goes last and
// absence counts as success, so a Prune that fails midway is repeated
// by the next one.
//
// The staging is dead because the runtime never applies an input at or
// below throughSeq again, and no later input reads it without a new
// start, which deletes the mark first. Assembled and instrumented
// modules stay: calls read them.
func (p *Plugin) Prune(throughSeq uint64) error {
	s, ok := p.store.(*codeStore)
	if !ok {
		return nil
	}
	marks, err := s.list(entryConsumedDeploy)
	if err != nil {
		return fmt.Errorf("wasm: prune: %w", err)
	}
	for _, m := range marks {
		seq, deployer, codeHash, ok := decodeConsumedDeploy(m)
		if !ok || seq > throughSeq {
			continue
		}
		if err := clearDeployStaging(s, deployer, codeHash); err != nil {
			return fmt.Errorf("wasm: prune: %w", err)
		}
	}
	return nil
}

// Rewind implements domain.Pruner. It deletes the mark of every
// finalize or upgrade above lastInputSeq, or every mark when fresh.
// After a restore, every remaining mark was written by a decode on the
// restored lineage: PruneAbove keeps only snapshots on that lineage,
// and every retracted input's index is above the restored position. A
// mark therefore means its pair's last start or finalize on the lineage
// was that finalize, so the pair's staging belongs to the consumed
// attempt. Chunks and records stay, because a canonical finalize above
// the position may still read them.
func (p *Plugin) Rewind(lastInputSeq uint64, fresh bool) error {
	s, ok := p.store.(*codeStore)
	if !ok {
		return nil
	}
	marks, err := s.list(entryConsumedDeploy)
	if err != nil {
		return fmt.Errorf("wasm: rewind: %w", err)
	}
	var refs []entryRef
	for _, m := range marks {
		seq, deployer, codeHash, ok := decodeConsumedDeploy(m)
		if !ok || !(fresh || seq > lastInputSeq) {
			continue
		}
		refs = append(refs, entryRef{entryConsumedDeploy, pendingRecordKey(deployer, codeHash)})
	}
	if err := s.deleteMany(refs); err != nil {
		return fmt.Errorf("wasm: rewind: %w", err)
	}
	return nil
}

// clearDeployStaging deletes every chunk, the pending record and the consumed mark of the
// (deployer, codeHash) pair. The chunks and record are unlinked and synced before the mark, so a
// power loss never leaves them without a mark. A new start owns the pair again, so a consumed-deploy
// entry left by an earlier finalize must not let Prune reach it. It syncs each affected directory
// once rather than once per entry.
func clearDeployStaging(store codeTier, deployer types.Address, codeHash types.Hash) error {
	refs := make([]entryRef, 0, MaxChunkCount+1)
	for i := uint32(0); i < MaxChunkCount; i++ {
		refs = append(refs, entryRef{entryDeployChunk, deployChunkKey(deployer, codeHash, i)})
	}
	refs = append(refs, entryRef{entryPendingRecord, pendingRecordKey(deployer, codeHash)})
	if err := store.deleteMany(refs); err != nil {
		return err
	}
	return store.delete(entryConsumedDeploy, pendingRecordKey(deployer, codeHash))
}

var _ domain.Pruner = (*Plugin)(nil)

// Apply serves all eight input kinds: CallL2 and Deposit (0/1), the chunked-deployment
// receipts and finalize (2-4), the upgrade (5), the ownership transfer (6), and the storage purchase (7).
func (p *Plugin) Apply(state domain.StateAccess, in domain.Input) (domain.Effects, error) {
	input, ok := in.(Input)
	if !ok {
		return domain.Effects{}, fmt.Errorf("wasm: apply: unexpected input type %T", in)
	}
	if input.Unsupported {
		// Receipt fields: gasUsed 0, zero contractId/codeHash, no
		// outbox message. deposit_asset/deposit_amount live in the tail
		// that could not be decoded.
		return domain.Effects{Status: codec.StatusUnsupportedOperation}, nil
	}

	switch args := input.Args.(type) {
	case codec.DeployStartArgs, codec.DeployChunkArgs:
		// Kinds 2/3: DecodeInput already wrote the pending record and
		// the hash-verified chunk bytes to the local store, so the
		// receipt is all that is left.
		return domain.Effects{Status: codec.StatusSuccess}, nil
	case codec.DeployFinalizeArgs:
		return p.applyDeployFinalize(input.Ctx, args)
	case codec.UpgradeArgs:
		return p.applyUpgrade(state, input.Ctx, args)
	case codec.TransferOwnershipArgs:
		return p.applyTransferOwnership(state, input.Ctx, args)
	case codec.BuyStorageArgs:
		return p.applyBuyStorage(state, args)
	}

	callArgs, ok := toCallArgs(input.Args)
	if !ok {
		return domain.Effects{}, fmt.Errorf("wasm: apply: unexpected input kind %d", input.Args.Kind())
	}
	return p.applyCall(state, input.Ctx, callArgs)
}

// pendingRecordKey is the (deployer, code_hash) code-store key for a pending deploy record.
func pendingRecordKey(deployer types.Address, codeHash types.Hash) []byte {
	key := make([]byte, 0, len(deployer)+len(codeHash))
	key = append(key, deployer[:]...)
	return append(key, codeHash[:]...)
}

// encodeConsumedDeploy is seq(8, BE) ‖ deployer(20) ‖ code_hash(32),
// the value of the consumed-deploy entry keyed by
// pendingRecordKey(deployer, codeHash). The value carries the pair
// because the store's listing returns values only.
func encodeConsumedDeploy(seq uint64, deployer types.Address, codeHash types.Hash) []byte {
	buf := make([]byte, 0, 8+len(deployer)+len(codeHash))
	buf = binary.BigEndian.AppendUint64(buf, seq)
	buf = append(buf, deployer[:]...)
	return append(buf, codeHash[:]...)
}

// decodeConsumedDeploy is the inverse of encodeConsumedDeploy. ok is
// false unless len(b) == 60.
func decodeConsumedDeploy(b []byte) (seq uint64, deployer types.Address, codeHash types.Hash, ok bool) {
	if len(b) != 8+len(deployer)+len(codeHash) {
		return 0, deployer, codeHash, false
	}
	seq = binary.BigEndian.Uint64(b)
	copy(deployer[:], b[8:])
	copy(codeHash[:], b[8+len(deployer):])
	return seq, deployer, codeHash, true
}

// deployChunkKey is the composite (deployer, code_hash, chunk_index) code-store key for one
// deploy chunk.
func deployChunkKey(deployer types.Address, codeHash types.Hash, chunkIndex uint32) []byte {
	key := make([]byte, len(deployer)+len(codeHash)+4)
	copy(key, deployer[:])
	copy(key[len(deployer):], codeHash[:])
	binary.BigEndian.PutUint32(key[len(deployer)+len(codeHash):], chunkIndex)
	return key
}

// encodePendingRecord is the local, non-consensus encoding of a chunked
// deployment's pending record: the fixed-width prefix metadata_hash(32)
// ‖ total_size(8, BE) ‖ chunk_count(4, BE), followed by the raw
// metadata bytes with no length prefix. Any encoding is legitimate here
// (the record never crosses the consensus boundary) but it must be a
// pure function of the kind-2 input alone.
func encodePendingRecord(metadataHash types.Hash, totalSize uint64, chunkCount uint32, metadata []byte) []byte {
	buf := make([]byte, 0, len(metadataHash)+8+4+len(metadata))
	buf = append(buf, metadataHash[:]...)
	buf = binary.BigEndian.AppendUint64(buf, totalSize)
	buf = binary.BigEndian.AppendUint32(buf, chunkCount)
	buf = append(buf, metadata...)
	return buf
}
