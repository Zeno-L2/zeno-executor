package gateway

import (
	"errors"
	"fmt"

	"github.com/zenon-network/go-zenon/chain/nom"
	"github.com/zenon-network/go-zenon/common/types"
	"github.com/zenon-network/go-zenon/vm/embedded/definition"

	"github.com/digitalSloth/go-executor/internal/znnd"
)

// SenderSource reads the L1 sender of the send block hashed sendHash,
// an input of domain domainID (EXECUTOR §21.6). The index wraps every
// call in a deadline.
type SenderSource interface {
	Sender(domainID uint32, sendHash types.Hash) (types.Address, error)
}

// errNotDomainInput marks a send block that is genuine but is not an
// input of the domain: it is not a user send to the Settlement
// contract, does not call an input method, or names another domain.
// The answer never depends on the node, so it is permanent.
var errNotDomainInput = errors.New("the send is not an input of this domain")

// ZnndSenders reads senders from a znnd node. It trusts a block only
// after recomputing its hash, which covers the sender's address, the
// destination, the block type and the call data.
type ZnndSenders struct {
	status func(types.Hash) (znnd.SendStatus, error)
}

// NewZnndSenders returns the sender source over c.
func NewZnndSenders(c *znnd.Client) ZnndSenders {
	return ZnndSenders{status: c.SendStatus}
}

// inputMethods names the Settlement methods whose sends are domain
// inputs. Each takes domainId as its first argument.
var inputMethods = map[string]bool{
	definition.SettlementCallL2MethodName:                    true,
	definition.SettlementDepositMethodName:                   true,
	definition.SettlementDeployContractStartMethodName:       true,
	definition.SettlementDeployContractChunkMethodName:       true,
	definition.SettlementDeployContractFinalizeMethodName:    true,
	definition.SettlementUpgradeContractMethodName:           true,
	definition.SettlementTransferContractOwnershipMethodName: true,
	definition.SettlementBuyStorageMethodName:                true,
}

// Sender returns the address that sent the block hashed sendHash. A
// block that is not an input of domain domainID returns an error
// wrapping errNotDomainInput.
func (z ZnndSenders) Sender(domainID uint32, sendHash types.Hash) (types.Address, error) {
	st, err := z.status(sendHash)
	if err != nil {
		return types.Address{}, err
	}
	b := st.Block
	if b == nil {
		return types.Address{}, fmt.Errorf("gateway: the znnd node has no confirmed send block %s", sendHash)
	}
	// ComputeHash dereferences every descendant, so a nil entry must be
	// rejected first.
	for _, d := range b.DescendantBlocks {
		if d == nil {
			return types.Address{}, fmt.Errorf("gateway: the znnd node returned send block %s with a nil descendant", sendHash)
		}
	}
	if got := b.ComputeHash(); got != sendHash {
		return types.Address{}, fmt.Errorf("gateway: the znnd node returned a block hashing to %s for send %s", got, sendHash)
	}
	if b.BlockType != nom.BlockTypeUserSend || b.ToAddress != types.SettlementContract {
		return types.Address{}, fmt.Errorf("gateway: send %s is not a user send to the Settlement contract: %w", sendHash, errNotDomainInput)
	}
	m, err := definition.ABISettlement.MethodById(b.Data)
	if err != nil || !inputMethods[m.Name] {
		return types.Address{}, fmt.Errorf("gateway: send %s does not call a Settlement input method: %w", sendHash, errNotDomainInput)
	}
	var p struct{ DomainId uint32 }
	if err := definition.ABISettlement.UnpackMethod(&p, m.Name, b.Data); err != nil {
		return types.Address{}, fmt.Errorf("gateway: send %s: its %s arguments do not decode: %v: %w", sendHash, m.Name, err, errNotDomainInput)
	}
	if p.DomainId != domainID {
		return types.Address{}, fmt.Errorf("gateway: send %s names domain %d, not %d: %w", sendHash, p.DomainId, domainID, errNotDomainInput)
	}
	return b.Address, nil
}
