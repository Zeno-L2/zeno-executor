package ledger

import (
	"encoding/hex"
	"encoding/json"
	"errors"
	"math/big"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/zenon-network/go-zenon/chain/nom"
	"github.com/zenon-network/go-zenon/common/types"
	"github.com/zenon-network/go-zenon/vm/embedded/definition"

	"github.com/digitalSloth/go-executor/domain"
	"github.com/digitalSloth/go-executor/internal/codec"
)

type decodeVectorFile struct {
	Vectors []struct {
		ID                string `json:"id"`
		ExpectedInputRoot string `json:"expected_input_root"`
		Inputs            []struct {
			EncodedHex string `json:"encoded_hex"`
		} `json:"inputs"`
	} `json:"vectors"`
}

var chunkBytes = []byte("chunk-bytes")

func loadInputRootVector(t *testing.T) (leaves [][]byte, root string) {
	t.Helper()
	data, err := os.ReadFile(filepath.Join("..", "codec", "testdata", "serialization-conformance-v1.json"))
	if err != nil {
		t.Fatalf("read vectors: %v", err)
	}
	var vf decodeVectorFile
	if err := json.Unmarshal(data, &vf); err != nil {
		t.Fatalf("parse vectors: %v", err)
	}
	for _, v := range vf.Vectors {
		if v.ID != "CV-INPUTROOT-1" {
			continue
		}
		for _, in := range v.Inputs {
			b, err := hex.DecodeString(in.EncodedHex)
			if err != nil {
				t.Fatalf("bad leaf hex: %v", err)
			}
			leaves = append(leaves, b)
		}
		return leaves, v.ExpectedInputRoot
	}
	t.Fatalf("vector CV-INPUTROOT-1 not found")
	return nil, ""
}

func pack(t *testing.T, method string, args ...any) []byte {
	t.Helper()
	data, err := definition.ABISettlement.PackMethod(method, args...)
	if err != nil {
		t.Fatalf("pack %s: %v", method, err)
	}
	return data
}

// sendFor builds the user send that carries in to domain 1, with the value fields the
// method needs.
func sendFor(t *testing.T, i int, in codec.Input) *nom.AccountBlock {
	t.Helper()
	b := &nom.AccountBlock{
		Address:       testAddr(byte(0x40 + i)),
		Height:        uint64(i + 1),
		Hash:          in.SourceAccountBlockHash,
		ToAddress:     types.SettlementContract,
		BlockType:     nom.BlockTypeUserSend,
		Amount:        big.NewInt(0),
		TokenStandard: types.ZnnTokenStandard,
	}
	switch a := in.Args.(type) {
	case codec.CallL2Args:
		zts, err := a.DepositAsset.ZTS()
		if err != nil {
			t.Fatalf("deposit asset: %v", err)
		}
		b.TokenStandard = zts
		b.Amount = new(big.Int).SetBytes(a.DepositAmount[:])
		b.Data = pack(t, definition.SettlementCallL2MethodName, uint32(1), a.TargetContract, a.PlasmaLimit, a.Payload)
	case codec.DepositArgs:
		zts, err := a.DepositAsset.ZTS()
		if err != nil {
			t.Fatalf("deposit asset: %v", err)
		}
		b.TokenStandard = zts
		b.Amount = new(big.Int).SetBytes(a.DepositAmount[:])
		b.Data = pack(t, definition.SettlementDepositMethodName, uint32(1), a.TargetContract, a.PlasmaLimit)
	case codec.DeployStartArgs:
		b.Data = pack(t, definition.SettlementDeployContractStartMethodName, uint32(1), a.CodeHash, a.MetadataHash, a.TotalSize, a.ChunkCount, a.Metadata)
	case codec.DeployChunkArgs:
		b.Data = pack(t, definition.SettlementDeployContractChunkMethodName, uint32(1), a.CodeHash, a.ChunkIndex, a.ChunkHash, chunkBytes)
	case codec.DeployFinalizeArgs:
		b.Amount = big.NewInt(5)
		b.Data = pack(t, definition.SettlementDeployContractFinalizeMethodName, uint32(1), a.CodeHash)
	case codec.UpgradeArgs:
		b.Amount = big.NewInt(5)
		b.Data = pack(t, definition.SettlementUpgradeContractMethodName, uint32(1), a.ContractID, a.NewCodeHash)
	case codec.TransferOwnershipArgs:
		b.Data = pack(t, definition.SettlementTransferContractOwnershipMethodName, uint32(1), a.ContractID, a.NewOwner)
	case codec.BuyStorageArgs:
		b.Amount = big.NewInt(5)
		b.Data = pack(t, definition.SettlementBuyStorageMethodName, uint32(1), a.ContractID, a.Bytes)
	default:
		t.Fatalf("unexpected args %T", in.Args)
	}
	return b
}

func TestDecodeAssignedInputVectors(t *testing.T) {
	leaves, root := loadInputRootVector(t)
	if len(leaves) < 8 {
		t.Fatalf("vector has %d leaves, want at least 8", len(leaves))
	}

	// First pass: build every send and the momentum that holds them all.
	inputs := make([]codec.Input, len(leaves))
	sends := make([]*nom.AccountBlock, len(leaves))
	for i, leaf := range leaves {
		in, err := codec.DecodeInput(leaf)
		if err != nil {
			t.Fatalf("leaf %d: decode: %v", i, err)
		}
		inputs[i] = in
		sends[i] = sendFor(t, i, in)
	}
	dm := newMomentum(7, types.Hash{}, sends)

	// Second pass: decode each leaf from that momentum.
	payloads := make([][]byte, len(leaves))
	for i, in := range inputs {
		got, err := DecodeAssignedInput(1, dm, 100+uint64(i), in.SourceAccountBlockHash)
		if err != nil {
			t.Fatalf("leaf %d: %v", i, err)
		}
		if !reflect.DeepEqual(got.Payload, leaves[i]) {
			t.Fatalf("leaf %d: payload %x, want %x", i, got.Payload, leaves[i])
		}
		want := domain.InputContext{
			GlobalInputIndex: 100 + uint64(i),
			MomentumHeight:   7,
			MomentumTime:     dm.Momentum.TimestampUnix,
			MomentumHash:     momHash(7),
			L1Sender:         testAddr(byte(0x40 + i)),
		}
		if in.Args.Kind() == codec.DeployChunk {
			want.SourceData = chunkBytes
		}
		if !reflect.DeepEqual(got.Ctx, want) {
			t.Fatalf("leaf %d: ctx %+v, want %+v", i, got.Ctx, want)
		}
		payloads[i] = got.Payload
	}
	if got := codec.MerkleRoot(payloads); hex.EncodeToString(got[:]) != root {
		t.Fatalf("input root %x, want %s", got, root)
	}
}

func TestDecodeAssignedInputZeroValueCallL2(t *testing.T) {
	target := testHash(0x33)
	b := &nom.AccountBlock{
		Address:       testAddr(0x41),
		Height:        1,
		Hash:          testHash(0x77),
		ToAddress:     types.SettlementContract,
		BlockType:     nom.BlockTypeUserSend,
		Amount:        big.NewInt(0),
		TokenStandard: types.ZnnTokenStandard,
		Data:          pack(t, definition.SettlementCallL2MethodName, uint32(1), target, uint64(9), []byte("p")),
	}
	dm := newMomentum(3, types.Hash{}, []*nom.AccountBlock{b})
	got, err := DecodeAssignedInput(1, dm, 0, b.Hash)
	if err != nil {
		t.Fatal(err)
	}
	want, err := codec.Input{SourceAccountBlockHash: b.Hash, Args: codec.CallL2Args{
		TargetContract: target, PlasmaLimit: 9, DepositAsset: codec.AssetID{}, DepositAmount: codec.U256{}, Payload: []byte("p"),
	}}.Encode()
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got.Payload, want) {
		t.Fatalf("payload %x, want %x", got.Payload, want)
	}
}

func TestDecodeAssignedInputFaults(t *testing.T) {
	target := testHash(0x33)
	callL2 := pack(t, definition.SettlementCallL2MethodName, uint32(1), target, uint64(9), []byte("p"))
	submit := pack(t, definition.SettlementSubmitBatchMethodName, []byte("c"))

	mk := func(data []byte) *nom.AccountBlock {
		return &nom.AccountBlock{
			Address: testAddr(0x41), Height: 1, Hash: testHash(0x77),
			ToAddress: types.SettlementContract, BlockType: nom.BlockTypeUserSend,
			Amount: big.NewInt(0), Data: data,
		}
	}
	withData := func(data []byte) func(*nom.AccountBlock) *nom.DetailedMomentum {
		return func(b *nom.AccountBlock) *nom.DetailedMomentum {
			b.Data = data
			return newMomentum(3, types.Hash{}, []*nom.AccountBlock{b})
		}
	}

	cases := []struct {
		name  string
		build func(b *nom.AccountBlock) *nom.DetailedMomentum
	}{
		{"hash in neither content nor blocks", func(b *nom.AccountBlock) *nom.DetailedMomentum {
			return newMomentum(3, types.Hash{}, nil)
		}},
		{"header removed from content", func(b *nom.AccountBlock) *nom.DetailedMomentum {
			b.Data = callL2
			dm := newMomentum(3, types.Hash{}, []*nom.AccountBlock{b})
			dm.Momentum.Content = nil
			return dm
		}},
		{"block removed from account blocks", func(b *nom.AccountBlock) *nom.DetailedMomentum {
			b.Data = callL2
			dm := newMomentum(3, types.Hash{}, []*nom.AccountBlock{b})
			dm.AccountBlocks = nil
			return dm
		}},
		{"nil data", withData(nil)},
		{"unknown selector", withData(append([]byte{0xde, 0xad, 0xbe, 0xef}, make([]byte, 32)...))},
		{"non-input method", withData(submit)},
		{"selector only", withData(callL2[:4])},
		{"truncated arguments", withData(callL2[:len(callL2)-32])},
		{"other domain", withData(pack(t, definition.SettlementDepositMethodName, uint32(2), target, uint64(9)))},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			b := mk(nil)
			b.Amount = big.NewInt(1)
			dm := tc.build(b)
			got, err := DecodeAssignedInput(1, dm, 5, b.Hash)
			if !errors.Is(err, ErrAssignmentFault) {
				t.Fatalf("err = %v, want ErrAssignmentFault", err)
			}
			if errors.Is(err, ErrSourceRead) {
				t.Fatalf("err = %v, must not be ErrSourceRead", err)
			}
			if !reflect.DeepEqual(got, Input{}) {
				t.Fatalf("input = %+v, want zero", got)
			}
		})
	}
}
