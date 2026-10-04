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

	"github.com/digitalSloth/go-executor/domain"
	"github.com/digitalSloth/go-executor/internal/codec"
	"github.com/digitalSloth/go-executor/plugins/wasm/internal/engine"
)

// Config is the plugin's construction-time configuration.
type Config struct {
	// NetworkName selects the fixed per-network chainID ("mainnet" | "testnet" | a named
	// devnet); chainID = SHA3-256("ZENON_WASM_L2:" ‖ name), computed once at construction
	// (§9.1). chain_id is not per-input.
	NetworkName string
	// CodeStorePath is the directory for the local, non-consensus code store (§4).
	CodeStorePath string
}

// Plugin is the WASM domain.Domain implementation.
type Plugin struct {
	cfg     Config
	chainID types.Hash
	store   *codeStore
	engine  *engine.Engine
}

// New returns a WASM domain plugin, erroring on an empty NetworkName or an unusable
// CodeStorePath.
func New(cfg Config) (*Plugin, error) {
	if cfg.NetworkName == "" {
		return nil, fmt.Errorf("wasm: NetworkName is required")
	}
	store, err := newCodeStore(cfg.CodeStorePath)
	if err != nil {
		return nil, fmt.Errorf("wasm: %w", err)
	}
	eng, err := engine.New()
	if err != nil {
		return nil, fmt.Errorf("wasm: %w", err)
	}
	return &Plugin{
		cfg:     cfg,
		chainID: hashOf(crypto.Hash([]byte("ZENON_WASM_L2:"), []byte(cfg.NetworkName))),
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

// DecodeInput turns a canonical §27.5 payload into a typed Input.
//
// Its error return means halt: the canonical encoding is produced by our own input pipeline
// from an account block whose structure Settlement's method ABI already constrained, so a
// malformed encoding, a kind-3 chunk-hash or chunk-size mismatch, or a SourceData/kind
// mismatch is an internal fault, not a consensus outcome. An input_kind outside the Phase-1
// set {0..7} is instead returned as Input{Unsupported: true} with a nil error, so Apply can
// emit the deterministic UNSUPPORTED_OPERATION status (§15).
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
		// A code_hash is reusable across deployment attempts (§11.5 expiry frees it, and §11.1
		// rejects only a duplicate live record). The pending record and the chunk entries are
		// keyed by (deployer, code_hash), so a start clears only its own deployer's earlier
		// attempt and never reaches another deployer's. Each chunk's bytes are verified only
		// against that input's own chunk_hash, never against code_hash as a set, so a surviving
		// stale chunk would assemble into a mix that passes every per-chunk check and commits a
		// status where a node with a complete view commits another. Deleting first restores the
		// property the finalize path relies on: a missing part of the prefix is absence, and
		// absence is a halt. The loop bound is a constant, the deletes are unconditional, and
		// absence is not an error, so this is a pure function of the input.
		//
		// This relies on DecodeInput and Apply being interleaved per input (decode i, apply i,
		// decode i+1, ...), never batch-decoded ahead of Apply: a later DeployStart for the same
		// deployer and code_hash deletes the chunks an already-decoded, not-yet-applied DeployFinalize for an
		// earlier attempt still needs to read.
		for i := uint32(0); i < MaxChunkCount; i++ {
			if err := p.store.delete(entryDeployChunk, deployChunkKey(ctx.L1Sender, args.CodeHash, i)); err != nil {
				return nil, fmt.Errorf("wasm: decode input: %w", err)
			}
		}
		if err := p.store.delete(entryPendingRecord, pendingRecordKey(ctx.L1Sender, args.CodeHash)); err != nil {
			return nil, fmt.Errorf("wasm: decode input: %w", err)
		}
		record := encodePendingRecord(args.MetadataHash, args.TotalSize, args.ChunkCount, args.Metadata)
		if err := p.store.put(entryPendingRecord, pendingRecordKey(ctx.L1Sender, args.CodeHash), record); err != nil {
			return nil, fmt.Errorf("wasm: decode input: %w", err)
		}
	}

	return Input{Ctx: ctx, Args: in.Args}, nil
}

// Apply serves all eight input kinds: CallL2 and Deposit (0/1), the chunked-deployment
// receipts and finalize (2-4), the upgrade (5), the ownership transfer (6), and the storage purchase (7).
func (p *Plugin) Apply(state domain.StateAccess, in domain.Input) (domain.Effects, error) {
	input, ok := in.(Input)
	if !ok {
		return domain.Effects{}, fmt.Errorf("wasm: apply: unexpected input type %T", in)
	}
	if input.Unsupported {
		// §15 receipt side: gasUsed 0, zero contractId/codeHash, no outbox message —
		// deposit_asset/deposit_amount live in the tail that could not be decoded.
		return domain.Effects{Status: codec.StatusUnsupportedOperation}, nil
	}

	switch args := input.Args.(type) {
	case codec.DeployStartArgs, codec.DeployChunkArgs:
		// Kinds 2/3 (§11.1, §11.2): DecodeInput already wrote the pending record and the
		// hash-verified chunk bytes to the local store, so the receipt is all that is left.
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

// deployChunkKey is the composite (deployer, code_hash, chunk_index) code-store key for one
// deploy chunk.
func deployChunkKey(deployer types.Address, codeHash types.Hash, chunkIndex uint32) []byte {
	key := make([]byte, len(deployer)+len(codeHash)+4)
	copy(key, deployer[:])
	copy(key[len(deployer):], codeHash[:])
	binary.BigEndian.PutUint32(key[len(deployer)+len(codeHash):], chunkIndex)
	return key
}

// encodePendingRecord is the local, non-consensus encoding of a chunked deployment's pending
// record: the fixed-width prefix metadata_hash(32) ‖ total_size(8, BE) ‖ chunk_count(4, BE),
// followed by the raw metadata bytes with no length prefix. Any
// encoding is legitimate here — the record never crosses the consensus boundary — but it must
// be a pure function of the kind-2 input alone (§4).
func encodePendingRecord(metadataHash types.Hash, totalSize uint64, chunkCount uint32, metadata []byte) []byte {
	buf := make([]byte, 0, len(metadataHash)+8+4+len(metadata))
	buf = append(buf, metadataHash[:]...)
	buf = binary.BigEndian.AppendUint64(buf, totalSize)
	buf = binary.BigEndian.AppendUint32(buf, chunkCount)
	buf = append(buf, metadata...)
	return buf
}
