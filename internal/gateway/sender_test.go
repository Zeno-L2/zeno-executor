package gateway

import (
	"errors"
	"math/big"
	"strings"
	"testing"

	"github.com/zenon-network/go-zenon/chain/nom"
	"github.com/zenon-network/go-zenon/common/types"
	"github.com/zenon-network/go-zenon/vm/embedded/definition"

	"github.com/digitalSloth/go-executor/internal/znnd"
)

func packSettlement(t *testing.T, method string, args ...any) []byte {
	t.Helper()
	data, err := definition.ABISettlement.PackMethod(method, args...)
	if err != nil {
		t.Fatalf("pack %s: %v", method, err)
	}
	return data
}

// userSend is a user send to the Settlement contract that calls CallL2
// for domain 1.
func userSend(t *testing.T) *nom.AccountBlock {
	t.Helper()
	return &nom.AccountBlock{
		Address:       types.Address{7},
		Height:        1,
		ToAddress:     types.SettlementContract,
		BlockType:     nom.BlockTypeUserSend,
		Amount:        big.NewInt(0),
		TokenStandard: types.ZnnTokenStandard,
		Data:          packSettlement(t, definition.SettlementCallL2MethodName, uint32(1), types.Hash{0xC0}, uint64(0), []byte("p")),
	}
}

func TestZnndSenders(t *testing.T) {
	serve := func(b *nom.AccountBlock) ZnndSenders {
		return ZnndSenders{status: func(types.Hash) (znnd.SendStatus, error) {
			return znnd.SendStatus{Block: b}, nil
		}}
	}
	refused := func(t *testing.T, b *nom.AccountBlock, domainID uint32, want string) {
		t.Helper()
		_, err := serve(b).Sender(domainID, b.ComputeHash())
		if err == nil || !strings.Contains(err.Error(), want) {
			t.Fatalf("err = %v, want %q", err, want)
		}
		if !errors.Is(err, errNotDomainInput) {
			t.Fatalf("err = %v, want errNotDomainInput", err)
		}
	}

	t.Run("accepted", func(t *testing.T) {
		b := userSend(t)
		got, err := serve(b).Sender(1, b.ComputeHash())
		if err != nil || got != (types.Address{7}) {
			t.Fatalf("Sender = %v, %v; want %v", got, err, types.Address{7})
		}
	})
	t.Run("hash mismatch", func(t *testing.T) {
		_, err := serve(userSend(t)).Sender(1, types.Hash{1})
		if err == nil || !strings.Contains(err.Error(), "hashing to") || errors.Is(err, errNotDomainInput) {
			t.Fatalf("err = %v, want a transient hashing to error", err)
		}
	})
	t.Run("no block", func(t *testing.T) {
		_, err := serve(nil).Sender(1, types.Hash{1})
		if err == nil || !strings.Contains(err.Error(), "no confirmed send block") || errors.Is(err, errNotDomainInput) {
			t.Fatalf("err = %v, want a transient no confirmed send block error", err)
		}
	})
	t.Run("node error", func(t *testing.T) {
		boom := errors.New("boom")
		z := ZnndSenders{status: func(types.Hash) (znnd.SendStatus, error) { return znnd.SendStatus{}, boom }}
		if _, err := z.Sender(1, types.Hash{1}); !errors.Is(err, boom) || errors.Is(err, errNotDomainInput) {
			t.Fatalf("err = %v, want the node error", err)
		}
	})
	t.Run("nil descendant", func(t *testing.T) {
		b := userSend(t)
		b.DescendantBlocks = []*nom.AccountBlock{nil}
		_, err := serve(b).Sender(1, types.Hash{1})
		if err == nil || !strings.Contains(err.Error(), "nil descendant") {
			t.Fatalf("err = %v, want nil descendant", err)
		}
	})
	t.Run("another domain", func(t *testing.T) {
		refused(t, userSend(t), 2, "names domain 1, not 2")
	})
	t.Run("another destination", func(t *testing.T) {
		b := userSend(t)
		b.ToAddress = types.Address{9}
		refused(t, b, 1, "not a user send to the Settlement contract")
	})
	t.Run("not a user send", func(t *testing.T) {
		b := userSend(t)
		b.BlockType = nom.BlockTypeContractSend
		refused(t, b, 1, "not a user send to the Settlement contract")
	})
	t.Run("not an input method", func(t *testing.T) {
		b := userSend(t)
		b.Data = packSettlement(t, definition.SettlementSubmitBatchMethodName, []byte("c"))
		refused(t, b, 1, "does not call a Settlement input method")
	})
	t.Run("undecodable arguments", func(t *testing.T) {
		b := userSend(t)
		b.Data = append(append([]byte(nil), b.Data[:4]...), 1, 2, 3)
		refused(t, b, 1, "arguments do not decode")
	})
}
