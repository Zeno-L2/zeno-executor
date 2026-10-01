// Package ipctest builds hash-valid momentum chains for internal/ipc tests: chains that pass
// both DecodeMomentum and the follower's continuity checks (internal/ledger/follower.go:337-461).
package ipctest

import (
	"encoding/binary"
	"math/big"

	"github.com/zenon-network/go-zenon/chain/nom"
	"github.com/zenon-network/go-zenon/common/types"
)

// fixedToAddress and fixedTokenStandard are every block's ToAddress and TokenStandard: fixed,
// non-zero, and irrelevant to the tests this package serves.
var (
	fixedToAddress     = types.Address{0xEE}
	fixedTokenStandard = types.ZenonTokenStandard{0x01}
)

// Block is one caller-supplied account block: its address and payload.
type Block struct {
	Address types.Address
	Data    []byte
}

// heightHash derives a deterministic, non-zero 32-byte value from tag and h, used for every
// hashed field this package fixes to a height-derived rather than a literal-fixed value.
func heightHash(tag byte, h uint64) types.Hash {
	var out types.Hash
	out[0] = tag
	binary.BigEndian.PutUint64(out[1:9], h)
	return out
}

func nonceFor(h uint64) [8]byte {
	var out [8]byte
	binary.BigEndian.PutUint64(out[:], h)
	return out
}

// addrChain tracks one address's own account-block chain across the whole result.
type addrChain struct {
	height   uint64
	prevHash types.Hash
}

// Chain returns momenta 1..len(perHeight); perHeight[i] holds the blocks of height i+1, in
// order. See the package doc for the exact fields it fixes.
func Chain(perHeight [][]Block) []*nom.DetailedMomentum {
	out := make([]*nom.DetailedMomentum, 0, len(perHeight))
	chains := make(map[types.Address]*addrChain)
	var prevMomHash types.Hash

	for i, blocks := range perHeight {
		h := uint64(i + 1)

		abs := make([]*nom.AccountBlock, 0, len(blocks))
		for _, blk := range blocks {
			st := chains[blk.Address]
			if st == nil {
				st = &addrChain{}
				chains[blk.Address] = st
			}
			st.height++
			nonce := nonceFor(h)
			ab := &nom.AccountBlock{
				Version:              1,
				ChainIdentifier:      1,
				BlockType:            nom.BlockTypeUserSend,
				PreviousHash:         st.prevHash,
				Height:               st.height,
				MomentumAcknowledged: types.HashHeight{Hash: prevMomHash, Height: h - 1},
				Address:              blk.Address,
				ToAddress:            fixedToAddress,
				Amount:               big.NewInt(int64(h)),
				TokenStandard:        fixedTokenStandard,
				Data:                 blk.Data,
				FusedPlasma:          h * 10,
				Nonce:                nom.Nonce{Data: nonce},
			}
			ab.Hash = ab.ComputeHash()
			st.prevHash = ab.Hash
			abs = append(abs, ab)
		}

		m := &nom.Momentum{
			Version:         nom.StateRootMomentumVersion,
			ChainIdentifier: 1,
			PreviousHash:    prevMomHash,
			Height:          h,
			TimestampUnix:   1000 + h,
			Data:            binary.BigEndian.AppendUint64(nil, h),
			Content:         nom.NewMomentumContent(abs),
			ChangesHash:     heightHash(0xC1, h),
			NextFusionPrice: 1000,
			NextWorkPrice:   2000,
		}
		if h%2 == 1 {
			m.StateRoot = heightHash(0xD1, h)
		}
		m.Hash = m.ComputeHash()
		prevMomHash = m.Hash

		out = append(out, &nom.DetailedMomentum{Momentum: m, AccountBlocks: abs})
	}
	return out
}

// sealBlock recomputes b's hash after recomputing every descendant's hash, deepest first.
func sealBlock(b *nom.AccountBlock) {
	for _, d := range b.DescendantBlocks {
		sealBlock(d)
	}
	b.Hash = b.ComputeHash()
}

// Seal recomputes, in place, every descendant hash (deepest first), every account-block hash,
// dm.Momentum.Content and dm.Momentum.Hash. It does not relink PreviousHash; it is for tests
// that edit one momentum.
func Seal(dm *nom.DetailedMomentum) {
	for _, b := range dm.AccountBlocks {
		sealBlock(b)
	}
	dm.Momentum.Content = nom.NewMomentumContent(dm.AccountBlocks)
	dm.Momentum.Hash = dm.Momentum.ComputeHash()
}
