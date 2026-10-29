package settlement

import (
	"strings"
	"testing"

	"github.com/zenon-network/go-zenon/common/types"

	"github.com/Zeno-L2/zeno-executor/internal/codec"
	"github.com/Zeno-L2/zeno-executor/internal/znnd/znndtest"
)

func wantCap(t *testing.T, fx *znndtest.Fixture) codec.U256 {
	t.Helper()
	want, err := codec.U256FromBig(fx.Expected.AssetConfig.MaxBatchWithdrawal)
	if err != nil {
		t.Fatalf("U256FromBig: %v", err)
	}
	return want
}

func TestMaxBatchWithdrawal(t *testing.T) {
	fx := znndtest.Load(t)
	_, r := newAssignmentReader(t, fx)
	h := fx.ReadHeight
	want := wantCap(t, fx)

	got, err := r.MaxBatchWithdrawal(fx.DomainID, h, hashOf(t, fx, h), codec.AssetID(fx.AssetID))
	if err != nil || got != want {
		t.Fatalf("configured asset = %x, %v; want %x", got, err, want)
	}
	got, err = r.MaxBatchWithdrawal(fx.DomainID, h, hashOf(t, fx, h), codec.AssetID(fx.UnconfiguredAssetID))
	if err != nil || got != (codec.U256{}) {
		t.Fatalf("unconfigured asset = %x, %v; want zero and nil", got, err)
	}
}

func TestMaxBatchWithdrawalNodeFaults(t *testing.T) {
	fx := znndtest.Load(t)
	h := fx.ReadHeight
	asset := codec.AssetID(fx.AssetID)

	t.Run("hash mismatch", func(t *testing.T) {
		_, r := newAssignmentReader(t, fx)
		_, err := r.MaxBatchWithdrawal(fx.DomainID, h, types.Hash{0x01}, asset)
		mustPlain(t, "hash mismatch", err)
	})
	t.Run("node down", func(t *testing.T) {
		srv, r := newAssignmentReader(t, fx)
		srv.SetDown(true)
		_, err := r.MaxBatchWithdrawal(fx.DomainID, h, hashOf(t, fx, h), asset)
		mustPlain(t, "node down", err)
	})
	t.Run("pre-state-root momentum", func(t *testing.T) {
		_, r := newAssignmentReader(t, fx)
		ph := fx.Assignment.PreStateRootEmptyHeight
		_, err := r.MaxBatchWithdrawal(fx.DomainID, ph, hashOf(t, fx, ph), asset)
		mustPlain(t, "pre-state-root momentum", err)
		if !strings.Contains(err.Error(), "StateRootSpork is not active") {
			t.Fatalf("error %q lacks the StateRootSpork text", err)
		}
	})
	t.Run("bad proof at an unchanged momentum", func(t *testing.T) {
		srv, r := newAssignmentReader(t, fx)
		srv.TamperProof(func(p *znndtest.Proof) { p.Root[0] ^= 0x01 })
		_, err := r.MaxBatchWithdrawal(fx.DomainID, h, hashOf(t, fx, h), asset)
		mustHalt(t, "bad proof", err)
	})
}

func TestMaxBatchWithdrawalReorgMidRead(t *testing.T) {
	fx := znndtest.Load(t)
	h := fx.ReadHeight
	hash := hashOf(t, fx, h)

	alt := fx.DetailedMomentum(t, h)
	alt.Momentum.StateRoot = types.Hash{0x03}
	alt.Momentum.Hash = alt.Momentum.ComputeHash()

	srv, r := newAssignmentReader(t, fx)
	srv.TamperProof(func(p *znndtest.Proof) {
		srv.SetMomentum(alt)
		p.Root = alt.Momentum.StateRoot
	})
	_, err := r.MaxBatchWithdrawal(fx.DomainID, h, hash, codec.AssetID(fx.AssetID))
	mustPlain(t, "reorg mid-read", err)
	if !strings.Contains(err.Error(), "changed during the read") {
		t.Fatalf("error %q lacks %q", err, "changed during the read")
	}
}
