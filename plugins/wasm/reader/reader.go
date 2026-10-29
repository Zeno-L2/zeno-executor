// Package reader is the WASM domain's gateway read adapter. It
// decodes inputs and reads state through domain.ReadContext. It never
// executes contract code and loads no engine.
package reader

import (
	"encoding/hex"
	"fmt"
	"math/big"

	"github.com/zenon-network/go-zenon/common/types"

	"github.com/Zeno-L2/zeno-executor/domain"
	"github.com/Zeno-L2/zeno-executor/internal/codec"
	"github.com/Zeno-L2/zeno-executor/plugins/wasm/internal/keys"
)

// profileName is the one runtime profile the WASM domain serves.
const profileName = "contracts/1"

// Reader is the WASM domain's domain.Reader.
type Reader struct {
	chainID types.Hash
}

var _ domain.Reader = (*Reader)(nil)
var _ domain.CodeSource = (*Reader)(nil)

// New returns the Reader of the network networkName.
func New(networkName string) *Reader {
	return &Reader{chainID: keys.ChainID(networkName)}
}

// ChainID returns the domain's chain id as a types.Hash.
func (r *Reader) ChainID() any { return r.chainID }

// Profiles returns the runtime profiles the domain serves.
func (r *Reader) Profiles() []domain.Profile {
	return []domain.Profile{{
		Name: profileName,
		Features: map[string]bool{
			"upgrade":           true,
			"ownershipTransfer": true,
			"storageQuota":      true,
			"payableCalls":      true,
			"deposits":          true,
			"nativeAddresses":   false,
		},
		InterfaceEncodings: []string{"zeno-selector-v1"},
	}}
}

// hexBytes encodes as a lowercase hex JSON string.
type hexBytes []byte

func (b hexBytes) MarshalText() ([]byte, error) { return []byte(hex.EncodeToString(b)), nil }

// inputHead starts every decoded input.
type inputHead struct {
	Kind                   string     `json:"kind"`
	InputKind              uint8      `json:"inputKind"`
	SourceAccountBlockHash types.Hash `json:"sourceAccountBlockHash"`
}

type callInput struct {
	inputHead
	TargetContract types.Hash `json:"targetContract"`
	PlasmaLimit    uint64     `json:"plasmaLimit"`
	DepositAsset   hexBytes   `json:"depositAsset"`
	DepositAmount  string     `json:"depositAmount"`
	Payload        hexBytes   `json:"payload"`
}

type depositInput struct {
	inputHead
	TargetContract types.Hash `json:"targetContract"`
	PlasmaLimit    uint64     `json:"plasmaLimit"`
	DepositAsset   hexBytes   `json:"depositAsset"`
	DepositAmount  string     `json:"depositAmount"`
}

type deployStartInput struct {
	inputHead
	CodeHash     types.Hash `json:"codeHash"`
	MetadataHash types.Hash `json:"metadataHash"`
	TotalSize    uint64     `json:"totalSize"`
	ChunkCount   uint32     `json:"chunkCount"`
	Metadata     hexBytes   `json:"metadata"`
}

type deployChunkInput struct {
	inputHead
	CodeHash   types.Hash `json:"codeHash"`
	ChunkIndex uint32     `json:"chunkIndex"`
	ChunkHash  types.Hash `json:"chunkHash"`
}

type deployFinalizeInput struct {
	inputHead
	CodeHash types.Hash `json:"codeHash"`
}

type upgradeInput struct {
	inputHead
	ContractID  types.Hash `json:"contractId"`
	NewCodeHash types.Hash `json:"newCodeHash"`
}

type transferInput struct {
	inputHead
	ContractID types.Hash    `json:"contractId"`
	NewOwner   types.Address `json:"newOwner"`
}

type buyStorageInput struct {
	inputHead
	ContractID types.Hash `json:"contractId"`
	Bytes      uint64     `json:"bytes"`
}

// DescribeInput decodes a canonical input into its JSON object. Any
// decode error, including an unknown kind, is returned.
func (r *Reader) DescribeInput(inputData []byte) (any, error) {
	in, err := codec.DecodeInput(inputData)
	if err != nil {
		return nil, err
	}
	head := func(name string) inputHead {
		return inputHead{name, uint8(in.Args.Kind()), in.SourceAccountBlockHash}
	}
	switch a := in.Args.(type) {
	case codec.CallL2Args:
		return callInput{head("CallL2"), a.TargetContract, a.PlasmaLimit, a.DepositAsset[:], amountOf(a.DepositAmount), a.Payload}, nil
	case codec.DepositArgs:
		return depositInput{head("Deposit"), a.TargetContract, a.PlasmaLimit, a.DepositAsset[:], amountOf(a.DepositAmount)}, nil
	case codec.DeployStartArgs:
		return deployStartInput{head("DeployContractStart"), a.CodeHash, a.MetadataHash, a.TotalSize, a.ChunkCount, a.Metadata}, nil
	case codec.DeployChunkArgs:
		return deployChunkInput{head("DeployContractChunk"), a.CodeHash, a.ChunkIndex, a.ChunkHash}, nil
	case codec.DeployFinalizeArgs:
		return deployFinalizeInput{head("DeployContractFinalize"), a.CodeHash}, nil
	case codec.UpgradeArgs:
		return upgradeInput{head("UpgradeContract"), a.ContractID, a.NewCodeHash}, nil
	case codec.TransferOwnershipArgs:
		return transferInput{head("TransferContractOwnership"), a.ContractID, a.NewOwner}, nil
	case codec.BuyStorageArgs:
		return buyStorageInput{head("BuyStorage"), a.ContractID, a.Bytes}, nil
	}
	return nil, fmt.Errorf("input kind %d has no description", in.Args.Kind())
}

// amountOf returns a as a decimal string.
func amountOf(a codec.U256) string { return new(big.Int).SetBytes(a[:]).String() }
