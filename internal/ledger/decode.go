package ledger

import (
	"bytes"
	"errors"
	"fmt"

	"github.com/zenon-network/go-zenon/chain/nom"
	"github.com/zenon-network/go-zenon/common/types"
	"github.com/zenon-network/go-zenon/vm/embedded/definition"

	"github.com/Zeno-L2/zeno-executor/domain"
	"github.com/Zeno-L2/zeno-executor/internal/codec"
)

// ErrAssignmentFault marks a consistency fault in Settlement's
// assignment: an assignment that cannot be reconciled with the momentum
// content it names. Examples are a send that is not in the momentum
// content, a send whose method does not decode as an input method, and
// a send that names another domain.
var ErrAssignmentFault = errors.New("ledger: assignment consistency fault")

// ABI argument structs for the eight input methods. The go-zenon ABI maps each argument to
// the field named by capitalising its first letter, so the field names are load-bearing.
type callL2Params struct {
	DomainId       uint32
	TargetContract types.Hash
	PlasmaLimit    uint64
	Payload        []byte
}

type depositParams struct {
	DomainId       uint32
	TargetContract types.Hash
	PlasmaLimit    uint64
}

type deployStartParams struct {
	DomainId     uint32
	CodeHash     types.Hash
	MetadataHash types.Hash
	TotalSize    uint64
	ChunkCount   uint32
	Metadata     []byte
}

type deployChunkParams struct {
	DomainId   uint32
	CodeHash   types.Hash
	ChunkIndex uint32
	ChunkHash  types.Hash
	ChunkData  []byte
}

type deployFinalizeParams struct {
	DomainId uint32
	CodeHash types.Hash
}

type upgradeParams struct {
	DomainId    uint32
	ContractId  types.Hash
	NewCodeHash types.Hash
}

type transferOwnershipParams struct {
	DomainId   uint32
	ContractId types.Hash
	NewOwner   types.Address
}

type buyStorageParams struct {
	DomainId   uint32
	ContractId types.Hash
	Bytes      uint64
}

// DecodeAssignedInput builds the canonical input for one entry of Settlement's assignment:
// the send hashed sourceHash, found in dm's content, assigned globalInputIndex.
//
// Precondition, not checked: dm came through ipc.DecodeMomentum, so the momentum hash and
// every block hash are verified and no entry is nil. The hash check is what ties the send's
// ToAddress and BlockType, which this function does not inspect, to the momentum content.
//
// Every error wraps ErrAssignmentFault, and the returned Input is then the zero value.
func DecodeAssignedInput(domainID uint32, dm *nom.DetailedMomentum, globalInputIndex uint64, sourceHash types.Hash) (Input, error) {
	fault := func(err error, format string, args ...any) (Input, error) {
		prefix := fmt.Sprintf("%%w: send %s at momentum %d (global index %d): ", sourceHash, dm.Momentum.Height, globalInputIndex)
		if err != nil {
			return Input{}, fmt.Errorf(prefix+format+": %w", append(append([]any{ErrAssignmentFault}, args...), err)...)
		}
		return Input{}, fmt.Errorf(prefix+format, append([]any{ErrAssignmentFault}, args...)...)
	}

	b, ok := findSend(dm, sourceHash)
	if !ok {
		return fault(nil, "not in momentum content")
	}
	method, err := definition.ABISettlement.MethodById(b.Data)
	if err != nil {
		return fault(err, "method lookup")
	}

	var (
		paramDomain uint32
		args        codec.InputArgs
		sourceData  []byte
	)
	unpack := func(p any) error {
		return definition.ABISettlement.UnpackMethod(p, method.Name, b.Data)
	}

	switch method.Name {
	case definition.SettlementCallL2MethodName:
		var p callL2Params
		if err := unpack(&p); err != nil {
			return fault(err, "unpack %s", method.Name)
		}
		asset, amount, err := sendValue(b)
		if err != nil {
			return fault(err, "deposit value")
		}
		paramDomain = p.DomainId
		args = codec.CallL2Args{TargetContract: p.TargetContract, PlasmaLimit: p.PlasmaLimit, DepositAsset: asset, DepositAmount: amount, Payload: p.Payload}
	case definition.SettlementDepositMethodName:
		var p depositParams
		if err := unpack(&p); err != nil {
			return fault(err, "unpack %s", method.Name)
		}
		asset, amount, err := sendValue(b)
		if err != nil {
			return fault(err, "deposit value")
		}
		paramDomain = p.DomainId
		args = codec.DepositArgs{TargetContract: p.TargetContract, PlasmaLimit: p.PlasmaLimit, DepositAsset: asset, DepositAmount: amount}
	case definition.SettlementDeployContractStartMethodName:
		var p deployStartParams
		if err := unpack(&p); err != nil {
			return fault(err, "unpack %s", method.Name)
		}
		paramDomain = p.DomainId
		args = codec.DeployStartArgs{CodeHash: p.CodeHash, MetadataHash: p.MetadataHash, TotalSize: p.TotalSize, ChunkCount: p.ChunkCount, Metadata: p.Metadata}
	case definition.SettlementDeployContractChunkMethodName:
		var p deployChunkParams
		if err := unpack(&p); err != nil {
			return fault(err, "unpack %s", method.Name)
		}
		paramDomain = p.DomainId
		args = codec.DeployChunkArgs{CodeHash: p.CodeHash, ChunkIndex: p.ChunkIndex, ChunkHash: p.ChunkHash}
		sourceData = bytes.Clone(p.ChunkData)
	case definition.SettlementDeployContractFinalizeMethodName:
		var p deployFinalizeParams
		if err := unpack(&p); err != nil {
			return fault(err, "unpack %s", method.Name)
		}
		paramDomain = p.DomainId
		args = codec.DeployFinalizeArgs{CodeHash: p.CodeHash}
	case definition.SettlementUpgradeContractMethodName:
		var p upgradeParams
		if err := unpack(&p); err != nil {
			return fault(err, "unpack %s", method.Name)
		}
		paramDomain = p.DomainId
		args = codec.UpgradeArgs{ContractID: p.ContractId, NewCodeHash: p.NewCodeHash}
	case definition.SettlementTransferContractOwnershipMethodName:
		var p transferOwnershipParams
		if err := unpack(&p); err != nil {
			return fault(err, "unpack %s", method.Name)
		}
		paramDomain = p.DomainId
		args = codec.TransferOwnershipArgs{ContractID: p.ContractId, NewOwner: p.NewOwner}
	case definition.SettlementBuyStorageMethodName:
		var p buyStorageParams
		if err := unpack(&p); err != nil {
			return fault(err, "unpack %s", method.Name)
		}
		paramDomain = p.DomainId
		args = codec.BuyStorageArgs{ContractID: p.ContractId, Bytes: p.Bytes}
	default:
		return fault(nil, "method %s is not an input method", method.Name)
	}

	if paramDomain != domainID {
		return fault(nil, "names domain %d, not %d", paramDomain, domainID)
	}
	payload, err := codec.Input{SourceAccountBlockHash: sourceHash, Args: args}.Encode()
	if err != nil {
		return fault(err, "encode")
	}
	return Input{
		Payload: payload,
		Ctx: domain.InputContext{
			GlobalInputIndex: globalInputIndex,
			MomentumHeight:   dm.Momentum.Height,
			MomentumTime:     dm.Momentum.TimestampUnix,
			MomentumHash:     dm.Momentum.Hash,
			L1Sender:         b.Address,
			SourceData:       sourceData,
		},
	}, nil
}

// findSend returns the account block hashed sourceHash. The block must be listed in the
// momentum content and present in dm.AccountBlocks with a matching header; either lookup
// failing reports not found. Descendant blocks are not searched: momentum content lists
// top-level blocks only.
func findSend(dm *nom.DetailedMomentum, sourceHash types.Hash) (*nom.AccountBlock, bool) {
	var entry *types.AccountHeader
	for _, h := range dm.Momentum.Content {
		if h.Hash == sourceHash {
			entry = h
			break
		}
	}
	if entry == nil {
		return nil, false
	}
	for _, b := range dm.AccountBlocks {
		if b.Header() == *entry {
			return b, true
		}
	}
	return nil, false
}

// sendValue returns the deposit asset and amount a send carries. A send with no value maps
// to the zero AssetID and zero amount, whatever token standard it names.
func sendValue(b *nom.AccountBlock) (codec.AssetID, codec.U256, error) {
	if b.Amount == nil || b.Amount.Sign() == 0 {
		return codec.AssetID{}, codec.U256{}, nil
	}
	amount, err := codec.U256FromBig(b.Amount)
	if err != nil {
		return codec.AssetID{}, codec.U256{}, err
	}
	return codec.AssetIDFromZTS(b.TokenStandard), amount, nil
}
