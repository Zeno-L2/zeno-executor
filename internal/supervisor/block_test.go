package supervisor

import (
	"bytes"
	"crypto/ed25519"
	"encoding/json"
	"errors"
	"math"
	"strings"
	"testing"

	"github.com/zenon-network/go-zenon/chain/nom"
	"github.com/zenon-network/go-zenon/common/types"
	"github.com/zenon-network/go-zenon/wallet"

	"github.com/Zeno-L2/zeno-executor/internal/recovery"
	"github.com/Zeno-L2/zeno-executor/internal/znnd"
)

type fakeBlockNode struct {
	frontier    *nom.AccountBlock
	momentum    *nom.Momentum
	req         znnd.PlasmaRequirement
	frontierErr error
	momentumErr error
	reqErr      error
	calls       []string
	gotFrom     types.Address
	gotTo       types.Address
	gotData     []byte
}

func (f *fakeBlockNode) FrontierAccountBlock(types.Address) (*nom.AccountBlock, error) {
	f.calls = append(f.calls, "account")
	return f.frontier, f.frontierErr
}

func (f *fakeBlockNode) FrontierMomentum() (*nom.Momentum, error) {
	f.calls = append(f.calls, "momentum")
	return f.momentum, f.momentumErr
}

func (f *fakeBlockNode) RequiredPlasma(from, to types.Address, data []byte) (znnd.PlasmaRequirement, error) {
	f.calls = append(f.calls, "plasma")
	f.gotFrom, f.gotTo, f.gotData = from, to, data
	return f.req, f.reqErr
}

func testKeyPair(t *testing.T) *wallet.KeyPair {
	t.Helper()
	priv := ed25519.NewKeyFromSeed([]byte(strings.Repeat("k", ed25519.SeedSize)))
	pub := priv.Public().(ed25519.PublicKey)
	return &wallet.KeyPair{Public: pub, Private: priv, Address: types.PubKeyToAddress(pub)}
}

func testMomentum(version, price uint64) *nom.Momentum {
	return &nom.Momentum{
		Version:         version,
		ChainIdentifier: 3,
		Height:          42,
		Hash:            types.Hash{0x42},
		NextFusionPrice: price,
	}
}

func TestBuildSendFields(t *testing.T) {
	kp := testKeyPair(t)
	data := []byte{0x01, 0x02, 0x03}
	prevBlock := &nom.AccountBlock{Height: 7, Hash: types.Hash{0x07}}

	for name, prev := range map[string]*nom.AccountBlock{"no frontier": nil, "frontier": prevBlock} {
		t.Run(name, func(t *testing.T) {
			m := testMomentum(1, 0)
			node := &fakeBlockNode{frontier: prev, momentum: m, req: znnd.PlasmaRequirement{AvailablePlasma: 100000, BasePlasma: 21000}}
			b, err := buildSend(node, kp, data)
			if err != nil {
				t.Fatalf("buildSend: %v", err)
			}

			wantHeight, wantPrev := uint64(1), types.ZeroHash
			if prev != nil {
				wantHeight, wantPrev = prev.Height+1, prev.Hash
			}
			if b.Version != 1 || b.ChainIdentifier != 3 || b.BlockType != nom.BlockTypeUserSend {
				t.Errorf("version/chain/type = %d/%d/%d", b.Version, b.ChainIdentifier, b.BlockType)
			}
			if b.Height != wantHeight || b.PreviousHash != wantPrev {
				t.Errorf("height/previous = %d/%s, want %d/%s", b.Height, b.PreviousHash, wantHeight, wantPrev)
			}
			if b.MomentumAcknowledged != m.Identifier() {
				t.Errorf("MomentumAcknowledged = %v, want %v", b.MomentumAcknowledged, m.Identifier())
			}
			if b.Address != kp.Address || b.ToAddress != types.SettlementContract {
				t.Errorf("address/to = %s/%s", b.Address, b.ToAddress)
			}
			if b.Amount == nil || b.Amount.Sign() != 0 {
				t.Errorf("Amount = %v, want non-nil zero", b.Amount)
			}
			if string(b.Data) != string(data) || b.FusedPlasma != 21000 {
				t.Errorf("data/fused = %x/%d", b.Data, b.FusedPlasma)
			}
			if b.TokenStandard != types.ZeroTokenStandard || b.Difficulty != 0 || b.Nonce != (nom.Nonce{}) {
				t.Errorf("token/difficulty/nonce are not zero")
			}
			if b.ComputeHash() != b.Hash {
				t.Errorf("Hash does not match ComputeHash")
			}
			if ok, err := wallet.VerifySignature(b.PublicKey, b.Hash.Bytes(), b.Signature); !ok || err != nil {
				t.Errorf("VerifySignature = %v, %v", ok, err)
			}
			if types.PubKeyToAddress(b.PublicKey) != kp.Address {
				t.Errorf("PublicKey does not hash to the signer address")
			}

			raw, err := json.Marshal(b)
			if err != nil {
				t.Fatalf("Marshal: %v", err)
			}
			var back nom.AccountBlock
			if err := json.Unmarshal(raw, &back); err != nil {
				t.Fatalf("Unmarshal: %v", err)
			}
			if back.ComputeHash() != b.Hash {
				t.Errorf("JSON round trip changes the hash")
			}

			if got := strings.Join(node.calls, ","); got != "account,momentum,plasma" {
				t.Errorf("read order = %s", got)
			}
			if node.gotFrom != kp.Address || node.gotTo != types.SettlementContract || !bytes.Equal(node.gotData, data) {
				t.Errorf("RequiredPlasma query = %s/%s/%x, want %s/%s/%x", node.gotFrom, node.gotTo, node.gotData, kp.Address, types.SettlementContract, data)
			}
		})
	}
}

func TestBuildSendFollowsFrontier(t *testing.T) {
	kp := testKeyPair(t)
	node := &fakeBlockNode{momentum: testMomentum(1, 0), req: znnd.PlasmaRequirement{AvailablePlasma: 100000, BasePlasma: 21000}}

	first, err := buildSend(node, kp, nil)
	if err != nil {
		t.Fatalf("buildSend: %v", err)
	}
	node.frontier = first
	second, err := buildSend(node, kp, nil)
	if err != nil {
		t.Fatalf("buildSend: %v", err)
	}
	if second.Height != first.Height+1 || second.PreviousHash != first.Hash {
		t.Errorf("second = %d/%s, want %d/%s", second.Height, second.PreviousHash, first.Height+1, first.Hash)
	}
}

func TestBuildSendFusedPlasma(t *testing.T) {
	kp := testKeyPair(t)
	cases := []struct {
		name           string
		version, price uint64
		base           uint64
		want           uint64
		wantErr        bool
	}{
		{name: "version 1 uses base", version: 1, price: 5000, base: 21000, want: 21000},
		{name: "version 2 is dynamic", version: 2, price: 1500, base: 21000, want: 31500},
		{name: "version 3 at the scale factor", version: 3, price: 1000, base: 21000, want: 21000},
		{name: "exact multiple", version: 3, price: 1500, base: 21000, want: 31500},
		{name: "rounds up", version: 3, price: 1001, base: 21001, want: 21023},
		{name: "overflow", version: 3, price: 2000, base: math.MaxUint64, wantErr: true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			node := &fakeBlockNode{momentum: testMomentum(c.version, c.price), req: znnd.PlasmaRequirement{AvailablePlasma: math.MaxUint64, BasePlasma: c.base}}
			b, err := buildSend(node, kp, nil)
			if c.wantErr {
				if err == nil || errors.Is(err, errInsufficientPlasma) {
					t.Fatalf("buildSend = %v, want a plain error", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("buildSend: %v", err)
			}
			if b.FusedPlasma != c.want {
				t.Errorf("FusedPlasma = %d, want %d", b.FusedPlasma, c.want)
			}
		})
	}
}

func TestBuildSendPlasmaAndErrors(t *testing.T) {
	kp := testKeyPair(t)
	newNode := func(available uint64) *fakeBlockNode {
		return &fakeBlockNode{momentum: testMomentum(1, 0), req: znnd.PlasmaRequirement{AvailablePlasma: available, BasePlasma: 21000}}
	}

	t.Run("one below", func(t *testing.T) {
		_, err := buildSend(newNode(20999), kp, nil)
		if !errors.Is(err, errInsufficientPlasma) {
			t.Fatalf("buildSend = %v, want errInsufficientPlasma", err)
		}
		for _, want := range []string{kp.Address.String(), "20999", "21000"} {
			if !strings.Contains(err.Error(), want) {
				t.Errorf("error %q lacks %q", err, want)
			}
		}
		if errors.Is(err, recovery.ErrHalted) {
			t.Errorf("error wraps ErrHalted")
		}
	})

	t.Run("equal", func(t *testing.T) {
		if _, err := buildSend(newNode(21000), kp, nil); err != nil {
			t.Fatalf("buildSend: %v", err)
		}
	})

	boom := errors.New("boom")
	for name, mutate := range map[string]func(*fakeBlockNode){
		"account frontier": func(f *fakeBlockNode) { f.frontierErr = boom },
		"momentum":         func(f *fakeBlockNode) { f.momentumErr = boom },
		"plasma":           func(f *fakeBlockNode) { f.reqErr = boom },
	} {
		t.Run(name+" error", func(t *testing.T) {
			node := newNode(100000)
			mutate(node)
			b, err := buildSend(node, kp, nil)
			if b != nil || !errors.Is(err, boom) {
				t.Fatalf("buildSend = %v, %v; want nil, boom", b, err)
			}
			if errors.Is(err, recovery.ErrHalted) {
				t.Errorf("error wraps ErrHalted")
			}
		})
	}
}
