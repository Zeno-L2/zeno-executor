package domain

import (
	"errors"

	"github.com/zenon-network/go-zenon/common/types"

	"github.com/Zeno-L2/zeno-executor/internal/codec"
)

// Reader is a domain's gateway read adapter. It decodes and reads
// gateway state; it never executes.
type Reader interface {
	ChainID() any // JSON-encodable chain_id
	DescribeInput(inputData []byte) (any, error)
	Profiles() []Profile
	Views() []View
}

// Profile is one runtime profile a domain serves.
type Profile struct {
	Name               string          `json:"name"`
	Features           map[string]bool `json:"features"`
	InterfaceEncodings []string        `json:"interfaceEncodings,omitempty"`
}

// ParamType is the type of a view parameter.
type ParamType string

const (
	ParamHash    ParamType = "bytes32" // args value: types.Hash
	ParamAddress ParamType = "address" // args value: types.Address
	ParamBytes   ParamType = "bytes"   // args value: []byte
)

// Param is one parameter of a view.
type Param struct {
	Name     string    `json:"name"`
	Type     ParamType `json:"type"`
	MaxLen   int       `json:"maxLength,omitempty"` // ParamBytes only
	Optional bool      `json:"optional"`
}

// View is one named read. args holds each given param under its
// name; a left-out optional param has no entry.
type View struct {
	Name    string                                                 `json:"name"`
	Profile string                                                 `json:"profile,omitempty"`
	Params  []Param                                                `json:"params"`
	Result  []string                                               `json:"result"`
	Read    func(rc ReadContext, args map[string]any) (any, error) `json:"-"`
}

// IndexedInput is one input the gateway indexed.
type IndexedInput struct {
	GlobalInputIndex uint64
	BatchID          uint64
	SendHash         types.Hash
	Sender           types.Address
	InputData        []byte
	Receipt          codec.Receipt
}

// ReadContext reads gateway state at one batch: the state version
// the response names, and indexed inputs at or below that batch.
// Visitors run newest first, over inputs whose global index is
// below before, and stop when fn returns false.
type ReadContext interface {
	Read(path Path) (value []byte, present bool, err error)
	InputsBySender(sender types.Address, before uint64, fn func(IndexedInput) (bool, error)) error
	Deploys(deployer types.Address, before uint64, fn func(IndexedInput) (bool, error)) error
	LastOwnerInput(contract types.Hash) (IndexedInput, bool, error)
	FirstCodeInput(codeHash types.Hash) (IndexedInput, bool, error)
	ChunkData(globalInputIndex uint64) ([]byte, error)
}

// CodeSource is implemented by a Reader that can serve a contract's
// as-deployed module to the domain's simulators.
type CodeSource interface {
	// Code returns the as-deployed module whose hash is sourceCodeHash
	// at rc's batch, or an error wrapping ErrNotFound when no
	// successful deploy or upgrade at or below that batch has it.
	Code(rc ReadContext, sourceCodeHash types.Hash) ([]byte, error)
}

var (
	// ErrInvalidParams marks a view's rejection of its arguments.
	ErrInvalidParams = errors.New("invalid params")
	// ErrNotFound marks a view whose subject the batch does not hold.
	ErrNotFound = errors.New("not found")
)
