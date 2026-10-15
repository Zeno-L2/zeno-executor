package reader

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"

	"github.com/zenon-network/go-zenon/common/crypto"
	"github.com/zenon-network/go-zenon/common/types"
	"github.com/zenon-network/go-zenon/vm/constants"

	"github.com/digitalSloth/go-executor/domain"
	"github.com/digitalSloth/go-executor/internal/codec"
	"github.com/digitalSloth/go-executor/plugins/wasm/internal/keys"
)

// maxDeploysPage is the most contracts one contractsByDeployer page
// holds.
const maxDeploysPage = 100

// Views returns the views the domain serves, in order.
func (r *Reader) Views() []domain.View {
	return []domain.View{
		{
			Name:    "contract",
			Profile: profileName,
			Params:  []domain.Param{{Name: "contractId", Type: domain.ParamHash}},
			Result:  []string{"codeHash", "sourceCodeHash", "upgradePolicy", "owner", "ownerCaller", "quota", "nativeAddress"},
			Read:    contractView,
		},
		{
			Name:    "storage",
			Profile: profileName,
			Params: []domain.Param{
				{Name: "contractId", Type: domain.ParamHash},
				{Name: "key", Type: domain.ParamBytes, MaxLen: codec.MaxStateKeySize},
			},
			Result: []string{"present", "value"},
			Read:   storageView,
		},
		{
			Name:    "contractsByDeployer",
			Profile: profileName,
			Params: []domain.Param{
				{Name: "address", Type: domain.ParamAddress},
				{Name: "cursor", Type: domain.ParamBytes, MaxLen: 8, Optional: true},
			},
			Result: []string{"contracts", "nextCursor"},
			Read:   deployerView,
		},
		{
			Name:    "module",
			Profile: profileName,
			Params:  []domain.Param{{Name: "sourceCodeHash", Type: domain.ParamHash}},
			Result:  []string{"sourceCodeHash", "module"},
			Read:    moduleView,
		},
	}
}

// quotaJSON is a contract's storage quota in bytes.
type quotaJSON struct {
	Purchased uint64 `json:"purchased"`
	Used      uint64 `json:"used"`
}

// contractJSON is the result of the contract view. A field is null
// when its leaf is absent.
type contractJSON struct {
	CodeHash       *types.Hash    `json:"codeHash"`
	SourceCodeHash *types.Hash    `json:"sourceCodeHash"`
	UpgradePolicy  *string        `json:"upgradePolicy"`
	Owner          *types.Address `json:"owner"`
	OwnerCaller    *types.Hash    `json:"ownerCaller"`
	Quota          *quotaJSON     `json:"quota"`
	NativeAddress  any            `json:"nativeAddress"`
}

// leaf is one reserved leaf as the state holds it.
type leaf struct {
	value   []byte
	present bool
}

// hashLeaf returns the leaf's hash, nil when absent.
func hashLeaf(l leaf, name string, contract types.Hash) (*types.Hash, error) {
	if !l.present {
		return nil, nil
	}
	if len(l.value) != types.HashSize {
		return nil, fmt.Errorf("contract %s: the %s leaf is %d bytes, not %d", contract, name, len(l.value), types.HashSize)
	}
	h, err := types.BytesToHash(l.value)
	if err != nil {
		return nil, fmt.Errorf("contract %s: the %s leaf: %w", contract, name, err)
	}
	return &h, nil
}

func contractView(rc domain.ReadContext, args map[string]any) (any, error) {
	id := args["contractId"].(types.Hash)
	var leaves [5]leaf
	for i, k := range [][]byte{keys.CodeHash, keys.PreCodeHash, keys.UpgradePolicy, keys.Owner, keys.Quota} {
		v, ok, err := rc.Read(keys.Derive(id, k))
		if err != nil {
			return nil, err
		}
		leaves[i] = leaf{v, ok}
	}
	codeL, preL, policyL, ownerL, quotaL := leaves[0], leaves[1], leaves[2], leaves[3], leaves[4]

	var res contractJSON
	var err error
	if res.CodeHash, err = hashLeaf(codeL, "CodeHash", id); err != nil {
		return nil, err
	}
	if res.SourceCodeHash, err = hashLeaf(preL, "PreCodeHash", id); err != nil {
		return nil, err
	}
	if policyL.present {
		if len(policyL.value) != 1 || policyL.value[0] > codec.PolicyOwnerUpgradeable {
			return nil, fmt.Errorf("contract %s: the UpgradePolicy leaf is not 0 or 1", id)
		}
		name := "IMMUTABLE"
		if policyL.value[0] == codec.PolicyOwnerUpgradeable {
			name = "OWNER_UPGRADEABLE"
		}
		res.UpgradePolicy = &name
	}
	if res.OwnerCaller, err = hashLeaf(ownerL, "Owner", id); err != nil {
		return nil, err
	}
	if quotaL.present && len(quotaL.value) != 16 {
		return nil, fmt.Errorf("contract %s: the Quota leaf is %d bytes, not 16", id, len(quotaL.value))
	}
	if res.OwnerCaller != nil {
		owner, err := ownerOf(rc, id, *res.OwnerCaller)
		if err != nil {
			return nil, err
		}
		res.Owner = &owner
	}
	if res.CodeHash != nil {
		q := quotaJSON{}
		if quotaL.present {
			q = quotaJSON{binary.BigEndian.Uint64(quotaL.value[:8]), binary.BigEndian.Uint64(quotaL.value[8:])}
		}
		res.Quota = &q
	}
	return res, nil
}

// storageJSON is the result of the storage view.
type storageJSON struct {
	Present bool      `json:"present"`
	Value   *hexBytes `json:"value"`
}

func storageView(rc domain.ReadContext, args map[string]any) (any, error) {
	id := args["contractId"].(types.Hash)
	v, ok, err := rc.Read(keys.Derive(id, args["key"].([]byte)))
	if err != nil {
		return nil, err
	}
	res := storageJSON{Present: ok}
	if ok {
		hb := hexBytes(v)
		res.Value = &hb
	}
	return res, nil
}

// deployedJSON is one contract of the contractsByDeployer view.
type deployedJSON struct {
	ContractID       types.Hash `json:"contractId"`
	SendHash         types.Hash `json:"sendHash"`
	GlobalInputIndex uint64     `json:"globalInputIndex"`
	BatchID          uint64     `json:"batchId"`
}

// deployerJSON is the result of the contractsByDeployer view.
type deployerJSON struct {
	Contracts  []deployedJSON `json:"contracts"`
	NextCursor *hexBytes      `json:"nextCursor"`
}

func deployerView(rc domain.ReadContext, args map[string]any) (any, error) {
	addr := args["address"].(types.Address)
	before := uint64(math.MaxUint64)
	if c, ok := args["cursor"]; ok {
		cur := c.([]byte)
		if len(cur) != 8 {
			return nil, fmt.Errorf("%w: cursor must be a nextCursor of this view", domain.ErrInvalidParams)
		}
		before = binary.BigEndian.Uint64(cur)
	}
	res := deployerJSON{Contracts: []deployedJSON{}}
	err := rc.Deploys(addr, before, func(in domain.IndexedInput) (bool, error) {
		res.Contracts = append(res.Contracts, deployedJSON{in.Receipt.ContractID, in.SendHash, in.GlobalInputIndex, in.BatchID})
		return len(res.Contracts) < maxDeploysPage, nil
	})
	if err != nil {
		return nil, err
	}
	if len(res.Contracts) == maxDeploysPage {
		next := hexBytes(binary.BigEndian.AppendUint64(nil, res.Contracts[maxDeploysPage-1].GlobalInputIndex))
		res.NextCursor = &next
	}
	return res, nil
}

// moduleJSON is the result of the module view.
type moduleJSON struct {
	SourceCodeHash types.Hash `json:"sourceCodeHash"`
	Module         hexBytes   `json:"module"`
}

func moduleView(rc domain.ReadContext, args map[string]any) (any, error) {
	h := args["sourceCodeHash"].(types.Hash)
	first, ok, err := rc.FirstCodeInput(h)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, fmt.Errorf("%w: module %s", domain.ErrNotFound, h)
	}
	start, chunks, err := stagedDeploy(rc, first, h)
	if err != nil {
		return nil, err
	}
	if start.ChunkCount < 1 || start.ChunkCount > constants.MaxChunkCount {
		return nil, fmt.Errorf("module %s: chunk_count %d is outside 1 to %d", h, start.ChunkCount, constants.MaxChunkCount)
	}
	if start.TotalSize < 1 || start.TotalSize > constants.MaxCodeSize {
		return nil, fmt.Errorf("module %s: total_size %d is outside 1 to %d", h, start.TotalSize, constants.MaxCodeSize)
	}
	module := make([]byte, 0, start.TotalSize)
	for i := uint32(0); i < start.ChunkCount; i++ {
		g, ok := chunks[i]
		if !ok {
			return nil, fmt.Errorf("module %s: chunk %d of %d is missing", h, i, start.ChunkCount)
		}
		data, err := rc.ChunkData(g)
		if err != nil {
			return nil, err
		}
		if uint64(len(module)+len(data)) > start.TotalSize {
			return nil, fmt.Errorf("module %s: the chunks are longer than total_size %d", h, start.TotalSize)
		}
		module = append(module, data...)
	}
	if uint64(len(module)) != start.TotalSize {
		return nil, fmt.Errorf("module %s: the chunks hold %d bytes, not total_size %d", h, len(module), start.TotalSize)
	}
	if !bytes.Equal(crypto.Hash(module), h[:]) {
		return nil, fmt.Errorf("module %s: the chunks do not hash to the code hash", h)
	}
	return moduleJSON{h, module}, nil
}

// Code returns the as-deployed module whose hash is sourceCodeHash at
// rc's batch, as the module view does.
func (r *Reader) Code(rc domain.ReadContext, sourceCodeHash types.Hash) ([]byte, error) {
	res, err := moduleView(rc, map[string]any{"sourceCodeHash": sourceCodeHash})
	if err != nil {
		return nil, err
	}
	return res.(moduleJSON).Module, nil
}

// stagedDeploy scans in.Sender's inputs below in, newest first, to the
// newest DeployContractStart for codeHash. chunks maps each chunk index
// below start.ChunkCount to the global index of the newest
// DeployContractChunk for codeHash above that start.
func stagedDeploy(rc domain.ReadContext, in domain.IndexedInput, codeHash types.Hash) (start codec.DeployStartArgs, chunks map[uint32]uint64, err error) {
	chunks = make(map[uint32]uint64)
	found := false
	err = rc.InputsBySender(in.Sender, in.GlobalInputIndex, func(x domain.IndexedInput) (bool, error) {
		dec, err := codec.DecodeInput(x.InputData)
		if err != nil {
			return true, nil
		}
		switch a := dec.Args.(type) {
		case codec.DeployChunkArgs:
			if _, seen := chunks[a.ChunkIndex]; a.CodeHash == codeHash && !seen {
				chunks[a.ChunkIndex] = x.GlobalInputIndex
			}
		case codec.DeployStartArgs:
			if a.CodeHash == codeHash {
				start, found = a, true
				return false, nil
			}
		}
		return true, nil
	})
	if err != nil {
		return start, nil, err
	}
	if !found {
		return start, nil, fmt.Errorf("no DeployContractStart for code hash %s below input %d of sender %s", codeHash, in.GlobalInputIndex, in.Sender)
	}
	for i := range chunks {
		if i >= start.ChunkCount {
			delete(chunks, i)
		}
	}
	return start, chunks, nil
}

// ownerOf returns the owner address of contract, checked against the
// owner leaf's caller hash ownerCaller. The address comes from the
// newest finalize or ownership transfer of the contract.
func ownerOf(rc domain.ReadContext, contract, ownerCaller types.Hash) (types.Address, error) {
	in, ok, err := rc.LastOwnerInput(contract)
	if err != nil {
		return types.Address{}, fmt.Errorf("owner of contract %s: %w", contract, err)
	}
	if !ok {
		return types.Address{}, fmt.Errorf("owner of contract %s: no finalize or ownership transfer is indexed", contract)
	}
	dec, err := codec.DecodeInput(in.InputData)
	if err != nil {
		return types.Address{}, fmt.Errorf("owner of contract %s: input %d: %w", contract, in.GlobalInputIndex, err)
	}
	var owner types.Address
	switch a := dec.Args.(type) {
	case codec.TransferOwnershipArgs:
		owner = a.NewOwner
	case codec.DeployFinalizeArgs:
		start, _, err := stagedDeploy(rc, in, a.CodeHash)
		if err != nil {
			return types.Address{}, fmt.Errorf("owner of contract %s: %w", contract, err)
		}
		meta, err := codec.DecodeContractMetadata(start.Metadata)
		if err != nil {
			return types.Address{}, fmt.Errorf("owner of contract %s: %w", contract, err)
		}
		owner = meta.Owner
	default:
		return types.Address{}, fmt.Errorf("owner of contract %s: input %d is of kind %d", contract, in.GlobalInputIndex, dec.Args.Kind())
	}
	if keys.Caller(owner) != ownerCaller {
		return types.Address{}, fmt.Errorf("owner of contract %s: the derived owner does not match the owner leaf", contract)
	}
	return owner, nil
}
