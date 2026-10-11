package supervisor

import (
	"errors"
	"fmt"
	"math"
	"math/big"

	"github.com/zenon-network/go-zenon/chain/nom"
	"github.com/zenon-network/go-zenon/common/types"
	"github.com/zenon-network/go-zenon/dp"
	"github.com/zenon-network/go-zenon/wallet"

	"github.com/digitalSloth/go-executor/internal/znnd"
)

// blockNode is what buildSend reads from the node. *znnd.Client implements it.
type blockNode interface {
	FrontierAccountBlock(addr types.Address) (*nom.AccountBlock, error)
	FrontierMomentum() (*nom.Momentum, error)
	RequiredPlasma(from, to types.Address, data []byte) (znnd.PlasmaRequirement, error)
}

var _ blockNode = (*znnd.Client)(nil)

// errInsufficientPlasma marks a send the executor address cannot pay for with fused plasma
// yet. It is retryable: the caller warns the operator and tries again at a later momentum,
// and never halts.
var errInsufficientPlasma = errors.New("supervisor: insufficient plasma")

// buildSend builds and signs a user send block from kp.Address to the Settlement contract
// carrying data, at the next height of kp.Address's account chain as the node reports it now.
func buildSend(node blockNode, kp *wallet.KeyPair, data []byte) (*nom.AccountBlock, error) {
	// The account frontier is read before the momentum so the momentum is at least as new as
	// the one the previous block acknowledges; the node rejects a block that acknowledges an
	// older momentum than its predecessor.
	prev, err := node.FrontierAccountBlock(kp.Address)
	if err != nil {
		return nil, err
	}
	m, err := node.FrontierMomentum()
	if err != nil {
		return nil, err
	}
	req, err := node.RequiredPlasma(kp.Address, types.SettlementContract, data)
	if err != nil {
		return nil, err
	}

	// The momentum version stands in for the RPC's spork check: the header carries no spork
	// flag, and consensus prices from the version alone.
	fused := req.BasePlasma
	if m.Version >= nom.DynamicPlasmaMomentumVersion {
		// Rounded up so the fused plasma meets the floor at this momentum's fusion price.
		v := new(big.Int).Mul(new(big.Int).SetUint64(req.BasePlasma), new(big.Int).SetUint64(m.NextFusionPrice))
		v.Add(v, new(big.Int).SetUint64(dp.PriceScaleFactor-1))
		v.Div(v, new(big.Int).SetUint64(dp.PriceScaleFactor))
		if !v.IsUint64() {
			return nil, fmt.Errorf("supervisor: fused plasma for base plasma %d at fusion price %d exceeds %d", req.BasePlasma, m.NextFusionPrice, uint64(math.MaxUint64))
		}
		fused = v.Uint64()
	}
	if req.AvailablePlasma < fused {
		return nil, fmt.Errorf("%w: executor address %s has %d plasma available, the send needs %d", errInsufficientPlasma, kp.Address, req.AvailablePlasma, fused)
	}

	b := &nom.AccountBlock{
		Version:              1,
		ChainIdentifier:      m.ChainIdentifier,
		BlockType:            nom.BlockTypeUserSend,
		Height:               1,
		PreviousHash:         types.ZeroHash,
		MomentumAcknowledged: m.Identifier(),
		Address:              kp.Address,
		ToAddress:            types.SettlementContract,
		Amount:               big.NewInt(0),
		Data:                 data,
		FusedPlasma:          fused,
	}
	if prev != nil {
		b.Height = prev.Height + 1
		b.PreviousHash = prev.Hash
	}
	b.Hash = b.ComputeHash()
	b.PublicKey = kp.Public
	b.Signature = kp.Sign(b.Hash.Bytes())
	return b, nil
}
