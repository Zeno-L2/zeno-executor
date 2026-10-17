package codec

import (
	"errors"
	"fmt"

	"github.com/zenon-network/go-zenon/common/types"
)

// InputKind is the input type ordinal. Ordinals are fixed and MUST NOT
// be reused.
type InputKind uint8

const (
	CallL2            InputKind = 0
	Deposit           InputKind = 1
	DeployStart       InputKind = 2
	DeployChunk       InputKind = 3
	DeployFinalize    InputKind = 4
	Upgrade           InputKind = 5
	TransferOwnership InputKind = 6
	BuyStorage        InputKind = 7
)

// InputArgs is the kind-specific tail of a canonical input encoding.
type InputArgs interface {
	// Kind returns the input_kind ordinal this tail encodes as.
	Kind() InputKind
}

// CallL2Args is the input_kind = 0 tail: a call into target_contract carrying an optional
// deposit and an application payload.
type CallL2Args struct {
	TargetContract types.Hash
	PlasmaLimit    uint64
	DepositAsset   AssetID
	DepositAmount  U256
	Payload        []byte
}

// Kind returns CallL2.
func (CallL2Args) Kind() InputKind { return CallL2 }

// DepositArgs is the input_kind = 1 tail: value delivered to target_contract with no
// application payload.
type DepositArgs struct {
	TargetContract types.Hash
	PlasmaLimit    uint64
	DepositAsset   AssetID
	DepositAmount  U256
}

// Kind returns Deposit.
func (DepositArgs) Kind() InputKind { return Deposit }

// DeployStartArgs is the input_kind = 2 tail.
type DeployStartArgs struct {
	CodeHash     types.Hash
	MetadataHash types.Hash
	TotalSize    uint64
	ChunkCount   uint32
	Metadata     []byte // ContractMetadata encoding, carried verbatim; validated by the runtime at finalize
}

// Kind returns DeployStart.
func (DeployStartArgs) Kind() InputKind { return DeployStart }

// DeployChunkArgs is the input_kind = 3 tail. chunk_data is committed
// by chunk_hash, not inlined.
type DeployChunkArgs struct {
	CodeHash   types.Hash
	ChunkIndex uint32
	ChunkHash  types.Hash
}

// Kind returns DeployChunk.
func (DeployChunkArgs) Kind() InputKind { return DeployChunk }

// DeployFinalizeArgs is the input_kind = 4 tail.
type DeployFinalizeArgs struct {
	CodeHash types.Hash
}

// Kind returns DeployFinalize.
func (DeployFinalizeArgs) Kind() InputKind { return DeployFinalize }

// UpgradeArgs is the input_kind = 5 tail. NewCodeHash is the
// pre-instrumentation code_hash of the replacement bytecode.
type UpgradeArgs struct {
	ContractID  types.Hash
	NewCodeHash types.Hash
}

// Kind returns Upgrade.
func (UpgradeArgs) Kind() InputKind { return Upgrade }

// TransferOwnershipArgs is the input_kind = 6 tail.
type TransferOwnershipArgs struct {
	ContractID types.Hash
	NewOwner   types.Address
}

// Kind returns TransferOwnership.
func (TransferOwnershipArgs) Kind() InputKind { return TransferOwnership }

// BuyStorageArgs is the input_kind = 7 tail: bytes of additional
// storage quota purchased for contract_id. bytes is non-zero.
type BuyStorageArgs struct {
	ContractID types.Hash
	Bytes      uint64
}

// Kind returns BuyStorage.
func (BuyStorageArgs) Kind() InputKind { return BuyStorage }

// Input is the canonical input encoding's source: input_kind:u8 ||
// source_hash:bytes32 || <kind-specific tail>. This is byte-identical
// to the inputHash preimage and the inputRoot leaf.
type Input struct {
	SourceAccountBlockHash types.Hash
	Args                   InputArgs
}

// Encode returns the canonical input encoding.
func (in Input) Encode() ([]byte, error) {
	if in.Args == nil {
		return nil, fmt.Errorf("codec: encode input: %w", ErrInvalidKind)
	}

	w := &writer{}
	w.u8(uint8(in.Args.Kind()))
	w.hash(in.SourceAccountBlockHash)

	switch args := in.Args.(type) {
	case CallL2Args:
		if err := args.DepositAsset.Validate(); err != nil {
			return nil, fmt.Errorf("codec: encode input: %w", err)
		}
		if len(args.Payload) > MaxCallPayload {
			return nil, fmt.Errorf("codec: encode input: %w", ErrLimitExceeded)
		}
		w.hash(args.TargetContract)
		w.u64(args.PlasmaLimit)
		w.raw(args.DepositAsset[:])
		w.u256(args.DepositAmount)
		w.bytes(args.Payload)
	case DepositArgs:
		if err := args.DepositAsset.Validate(); err != nil {
			return nil, fmt.Errorf("codec: encode input: %w", err)
		}
		w.hash(args.TargetContract)
		w.u64(args.PlasmaLimit)
		w.raw(args.DepositAsset[:])
		w.u256(args.DepositAmount)
	case DeployStartArgs:
		w.hash(args.CodeHash)
		w.hash(args.MetadataHash)
		w.u64(args.TotalSize)
		w.u32(args.ChunkCount)
		w.bytes(args.Metadata)
	case DeployChunkArgs:
		w.hash(args.CodeHash)
		w.u32(args.ChunkIndex)
		w.hash(args.ChunkHash)
	case DeployFinalizeArgs:
		w.hash(args.CodeHash)
	case UpgradeArgs:
		w.hash(args.ContractID)
		w.hash(args.NewCodeHash)
	case TransferOwnershipArgs:
		w.hash(args.ContractID)
		w.address(args.NewOwner)
	case BuyStorageArgs:
		if args.Bytes == 0 {
			return nil, fmt.Errorf("codec: encode input: %w", ErrZeroStorageBytes)
		}
		w.hash(args.ContractID)
		w.u64(args.Bytes)
	default:
		return nil, fmt.Errorf("codec: encode input: %w", ErrInvalidKind)
	}

	return w.buf, nil
}

// DecodeInput decodes a canonical Input encoding.
func DecodeInput(b []byte) (Input, error) {
	r := newReader(b)
	var in Input

	kindByte, err := r.u8()
	if err != nil {
		return in, fmt.Errorf("codec: decode input: %w", err)
	}
	if in.SourceAccountBlockHash, err = r.hash(); err != nil {
		return in, fmt.Errorf("codec: decode input: %w", err)
	}

	switch InputKind(kindByte) {
	case CallL2:
		target, err := r.hash()
		if err != nil {
			return in, fmt.Errorf("codec: decode input: %w", err)
		}
		plasmaLimit, err := r.u64()
		if err != nil {
			return in, fmt.Errorf("codec: decode input: %w", err)
		}
		asset, err := r.assetID()
		if err != nil {
			return in, fmt.Errorf("codec: decode input: %w", err)
		}
		amount, err := r.u256()
		if err != nil {
			return in, fmt.Errorf("codec: decode input: %w", err)
		}
		payload, err := r.bytes(MaxCallPayload)
		if err != nil {
			return in, fmt.Errorf("codec: decode input: %w", err)
		}
		in.Args = CallL2Args{TargetContract: target, PlasmaLimit: plasmaLimit, DepositAsset: asset, DepositAmount: amount, Payload: payload}
	case Deposit:
		target, err := r.hash()
		if err != nil {
			return in, fmt.Errorf("codec: decode input: %w", err)
		}
		plasmaLimit, err := r.u64()
		if err != nil {
			return in, fmt.Errorf("codec: decode input: %w", err)
		}
		asset, err := r.assetID()
		if err != nil {
			return in, fmt.Errorf("codec: decode input: %w", err)
		}
		amount, err := r.u256()
		if err != nil {
			return in, fmt.Errorf("codec: decode input: %w", err)
		}
		in.Args = DepositArgs{TargetContract: target, PlasmaLimit: plasmaLimit, DepositAsset: asset, DepositAmount: amount}
	case DeployStart:
		codeHash, err := r.hash()
		if err != nil {
			return in, fmt.Errorf("codec: decode input: %w", err)
		}
		metadataHash, err := r.hash()
		if err != nil {
			return in, fmt.Errorf("codec: decode input: %w", err)
		}
		totalSize, err := r.u64()
		if err != nil {
			return in, fmt.Errorf("codec: decode input: %w", err)
		}
		chunkCount, err := r.u32()
		if err != nil {
			return in, fmt.Errorf("codec: decode input: %w", err)
		}
		metadata, err := r.bytes(^uint32(0))
		if err != nil {
			return in, fmt.Errorf("codec: decode input: %w", err)
		}
		in.Args = DeployStartArgs{CodeHash: codeHash, MetadataHash: metadataHash, TotalSize: totalSize, ChunkCount: chunkCount, Metadata: metadata}
	case DeployChunk:
		codeHash, err := r.hash()
		if err != nil {
			return in, fmt.Errorf("codec: decode input: %w", err)
		}
		chunkIndex, err := r.u32()
		if err != nil {
			return in, fmt.Errorf("codec: decode input: %w", err)
		}
		chunkHash, err := r.hash()
		if err != nil {
			return in, fmt.Errorf("codec: decode input: %w", err)
		}
		in.Args = DeployChunkArgs{CodeHash: codeHash, ChunkIndex: chunkIndex, ChunkHash: chunkHash}
	case DeployFinalize:
		codeHash, err := r.hash()
		if err != nil {
			return in, fmt.Errorf("codec: decode input: %w", err)
		}
		in.Args = DeployFinalizeArgs{CodeHash: codeHash}
	case Upgrade:
		contractID, err := r.hash()
		if err != nil {
			return in, fmt.Errorf("codec: decode input: %w", err)
		}
		newCodeHash, err := r.hash()
		if err != nil {
			return in, fmt.Errorf("codec: decode input: %w", err)
		}
		in.Args = UpgradeArgs{ContractID: contractID, NewCodeHash: newCodeHash}
	case TransferOwnership:
		contractID, err := r.hash()
		if err != nil {
			return in, fmt.Errorf("codec: decode input: %w", err)
		}
		newOwner, err := r.address()
		if err != nil {
			return in, fmt.Errorf("codec: decode input: %w", err)
		}
		in.Args = TransferOwnershipArgs{ContractID: contractID, NewOwner: newOwner}
	case BuyStorage:
		contractID, err := r.hash()
		if err != nil {
			return in, fmt.Errorf("codec: decode input: %w", err)
		}
		storageBytes, err := r.u64()
		if err != nil {
			return in, fmt.Errorf("codec: decode input: %w", err)
		}
		if storageBytes == 0 {
			return in, fmt.Errorf("codec: decode input: %w", ErrZeroStorageBytes)
		}
		in.Args = BuyStorageArgs{ContractID: contractID, Bytes: storageBytes}
	default:
		return in, fmt.Errorf("codec: decode input: %w", ErrInvalidKind)
	}

	if err := r.end(); err != nil {
		return in, fmt.Errorf("codec: decode input: %w", err)
	}
	return in, nil
}

// CanonicalInput returns the canonical input encoding for a raw
// payload: the payload itself when it decodes as one of the eight
// defined kinds, and the input_kind || source_account_block_hash prefix
// alone for any other kind, which defines no tail. This is the
// inputHash preimage, the inputRoot leaf, and the bundle record's
// inputData.
//
// CanonicalInput reaches the prefix branch only through errors.Is(err, ErrInvalidKind), which
// DecodeInput can report only after it has already consumed the 1-byte kind and the 32-byte
// source hash; every other decode error is returned unchanged.
func CanonicalInput(payload []byte) ([]byte, error) {
	if _, err := DecodeInput(payload); err != nil {
		if errors.Is(err, ErrInvalidKind) {
			return payload[:33:33], nil
		}
		return nil, err
	}
	return payload, nil
}
