package settlement

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/zenon-network/go-zenon/chain/nom"
	"github.com/zenon-network/go-zenon/common/trie"
	"github.com/zenon-network/go-zenon/common/types"

	"github.com/digitalSloth/go-executor/internal/ledger"
	"github.com/digitalSloth/go-executor/internal/znnd"
	"github.com/digitalSloth/go-executor/internal/znnd/znndtest"
)

func newAssignmentReader(t *testing.T, fx *znndtest.Fixture) (*znndtest.Server, *Reader) {
	t.Helper()
	srv := znndtest.NewServer(t, fx, fx.Assignment.ReceiveHeight+10)
	return srv, NewReader(znnd.NewClient(srv.URL))
}

func hashOf(t *testing.T, fx *znndtest.Fixture, height uint64) types.Hash {
	t.Helper()
	return fx.DetailedMomentum(t, height).Momentum.Hash
}

func wantEntries(fx *znndtest.Fixture) []ledger.AssignmentEntry {
	var out []ledger.AssignmentEntry
	for _, e := range fx.Assignment.Entries {
		out = append(out, ledger.AssignmentEntry(e))
	}
	return out
}

func mustHaltWith(t *testing.T, name string, err error, text string) {
	t.Helper()
	mustHalt(t, name, err)
	if !strings.Contains(err.Error(), text) {
		t.Fatalf("%s: error %q lacks %q", name, err, text)
	}
}

func TestAssignmentAcceptance(t *testing.T) {
	fx := znndtest.Load(t)
	_, r := newAssignmentReader(t, fx)
	a := fx.Assignment
	c0 := fx.Expected.DomainInputCounter

	got, err := r.Assignment(fx.DomainID, a.ReceiveHeight, hashOf(t, fx, a.ReceiveHeight), c0)
	if err != nil {
		t.Fatalf("Assignment: %v", err)
	}
	if want := wantEntries(fx); !reflect.DeepEqual(got, want) {
		t.Fatalf("Assignment = %+v, want %+v", got, want)
	}
	if len(got) != 2 {
		t.Fatalf("got %d entries, want 2", len(got))
	}
	if got[1].GlobalInputIndex <= got[0].GlobalInputIndex {
		t.Fatalf("global input indexes %d, %d do not strictly increase", got[0].GlobalInputIndex, got[1].GlobalInputIndex)
	}
	for _, e := range got {
		if e.SourceAccountBlockHash == a.RejectedSendHash {
			t.Fatalf("entry %+v carries the rejected send", e)
		}
		if e.SendMomentumHeight != a.SendHeight {
			t.Fatalf("entry %+v has send height %d, want %d", e, e.SendMomentumHeight, a.SendHeight)
		}
	}
}

func TestAssignmentNoSettlementBlock(t *testing.T) {
	fx := znndtest.Load(t)
	_, r := newAssignmentReader(t, fx)
	for _, h := range []uint64{fx.Assignment.EmptyHeight, fx.Assignment.PreStateRootEmptyHeight} {
		got, err := r.Assignment(fx.DomainID, h, hashOf(t, fx, h), 0)
		if err != nil || got != nil {
			t.Fatalf("Assignment at %d = %v, %v; want nil, nil", h, got, err)
		}
	}
}

func TestAssignmentPreStateRootSettlement(t *testing.T) {
	fx := znndtest.Load(t)
	_, r := newAssignmentReader(t, fx)
	h := fx.Assignment.PreStateRootSettlementHeight
	_, err := r.Assignment(fx.DomainID, h, hashOf(t, fx, h), 0)
	mustHaltWith(t, "pre-state-root Settlement momentum", err, "StateRootSpork must be active before OffchainExecutionSpork")
}

func TestAssignmentOtherHash(t *testing.T) {
	fx := znndtest.Load(t)
	_, r := newAssignmentReader(t, fx)
	other := types.Hash{0x01}
	_, err := r.Assignment(fx.DomainID, fx.Assignment.ReceiveHeight, other, fx.Expected.DomainInputCounter-1)
	mustPlain(t, "another hash", err)
	if want := fmt.Sprintf("not %s", other); !strings.Contains(err.Error(), want) {
		t.Fatalf("error %q lacks %q", err, want)
	}
}

func TestAssignmentCursor(t *testing.T) {
	fx := znndtest.Load(t)
	_, r := newAssignmentReader(t, fx)
	rh := fx.Assignment.ReceiveHeight
	hash := hashOf(t, fx, rh)
	c0 := fx.Expected.DomainInputCounter
	count := c0 + uint64(len(fx.Assignment.Entries))

	_, err := r.Assignment(fx.DomainID, rh, hash, c0-1)
	mustHaltWith(t, "behind", err, "is behind Settlement")
	for _, next := range []uint64{c0 + 1, count} {
		_, err = r.Assignment(fx.DomainID, rh, hash, next)
		mustHaltWith(t, "ahead", err, "is ahead of Settlement")
	}
	_, err = r.Assignment(fx.DomainID, rh, hash, count+1)
	mustHaltWith(t, "below", err, "is below the worker's next ordinal")

	got, err := r.Assignment(fx.EmptyDomainID, rh, hash, 0)
	if err != nil || got != nil {
		t.Fatalf("Assignment(empty domain) = %v, %v; want nil, nil", got, err)
	}
}

func TestAssignmentNodeFaults(t *testing.T) {
	fx := znndtest.Load(t)
	rh := fx.Assignment.ReceiveHeight
	hash := hashOf(t, fx, rh)
	c0 := fx.Expected.DomainInputCounter

	t.Run("down", func(t *testing.T) {
		srv, r := newAssignmentReader(t, fx)
		srv.SetDown(true)
		_, err := r.Assignment(fx.DomainID, rh, hash, c0)
		mustPlain(t, "down", err)
	})

	t.Run("tampered proof", func(t *testing.T) {
		srv, r := newAssignmentReader(t, fx)
		srv.TamperProof(func(p *znndtest.Proof) { p.Root[0] ^= 0xff })
		_, err := r.Assignment(fx.DomainID, rh, hash, c0)
		mustHalt(t, "tampered proof", err)
	})

	t.Run("no account blocks", func(t *testing.T) {
		cp := *fx
		cp.AccountBlocks = nil
		_, r := newAssignmentReader(t, &cp)
		_, err := r.Assignment(fx.DomainID, rh, hash, c0)
		mustPlain(t, "no account blocks", err)
	})

	t.Run("send confirmed in a momentum that does not list it", func(t *testing.T) {
		cp := *fx
		cp.AccountBlocks = nil
		for _, raw := range fx.AccountBlocks {
			var b map[string]any
			if err := json.Unmarshal(raw, &b); err != nil {
				t.Fatal(err)
			}
			if b["hash"] == fx.Assignment.Entries[0].SourceAccountBlockHash.String() {
				b["confirmationDetail"].(map[string]any)["momentumHeight"] = fx.Assignment.SendHeight - 1
			}
			out, err := json.Marshal(b)
			if err != nil {
				t.Fatal(err)
			}
			cp.AccountBlocks = append(cp.AccountBlocks, out)
		}
		_, r := newAssignmentReader(t, &cp)
		_, err := r.Assignment(fx.DomainID, rh, hash, c0)
		mustPlain(t, "wrong send height", err)
		if !strings.Contains(err.Error(), "does not list send") {
			t.Fatalf("error %q lacks %q", err, "does not list send")
		}
	})
}

func TestAssignmentSendHeightAboveRead(t *testing.T) {
	fx := znndtest.Load(t)
	rh := fx.Assignment.ReceiveHeight
	hash := hashOf(t, fx, rh)
	cp := *fx
	cp.AccountBlocks = nil
	for _, raw := range fx.AccountBlocks {
		var b map[string]any
		if err := json.Unmarshal(raw, &b); err != nil {
			t.Fatal(err)
		}
		if b["hash"] == fx.Assignment.Entries[0].SourceAccountBlockHash.String() {
			b["confirmationDetail"].(map[string]any)["momentumHeight"] = rh + 1
		}
		out, err := json.Marshal(b)
		if err != nil {
			t.Fatal(err)
		}
		cp.AccountBlocks = append(cp.AccountBlocks, out)
	}
	_, r := newAssignmentReader(t, &cp)
	_, err := r.Assignment(fx.DomainID, rh, hash, fx.Expected.DomainInputCounter)
	mustPlain(t, "send height above the read height", err)
	if !strings.Contains(err.Error(), "outside [1,") {
		t.Fatalf("error %q lacks %q", err, "outside [1,")
	}
}

func TestCheckedMomentumAndSettlementReceives(t *testing.T) {
	fx := znndtest.Load(t)
	rh := fx.Assignment.ReceiveHeight
	hash := hashOf(t, fx, rh)

	dm := fx.DetailedMomentum(t, rh)
	m, ran, received, err := settlementReceives([]*nom.DetailedMomentum{dm}, rh, hash)
	if err != nil || !ran || m.Hash != hash {
		t.Fatalf("settlementReceives = %v, %v, %v; want the momentum and ran", m, ran, err)
	}
	sends := []types.Hash{fx.Assignment.RejectedSendHash}
	for _, e := range fx.Assignment.Entries {
		sends = append(sends, e.SourceAccountBlockHash)
	}
	for _, s := range sends {
		if !received[s] {
			t.Fatalf("received set lacks send %s", s)
		}
	}

	settlementIdx := func(dm *nom.DetailedMomentum) []int {
		var idx []int
		for i, b := range dm.AccountBlocks {
			if b.Address == types.SettlementContract {
				idx = append(idx, i)
			}
		}
		return idx
	}
	cases := []struct {
		name   string
		mutate func(dm *nom.DetailedMomentum) ([]*nom.DetailedMomentum, uint64, types.Hash)
	}{
		{"empty list", func(*nom.DetailedMomentum) ([]*nom.DetailedMomentum, uint64, types.Hash) { return nil, rh, hash }},
		{"height mismatch", func(dm *nom.DetailedMomentum) ([]*nom.DetailedMomentum, uint64, types.Hash) {
			return []*nom.DetailedMomentum{dm}, rh + 1, hash
		}},
		{"nil content entry", func(dm *nom.DetailedMomentum) ([]*nom.DetailedMomentum, uint64, types.Hash) {
			dm.Momentum.Content = append([]*types.AccountHeader{nil}, dm.Momentum.Content...)
			return []*nom.DetailedMomentum{dm}, rh, hash
		}},
		{"hash does not recompute", func(dm *nom.DetailedMomentum) ([]*nom.DetailedMomentum, uint64, types.Hash) {
			dm.Momentum.Hash = types.Hash{0x02}
			return []*nom.DetailedMomentum{dm}, rh, types.Hash{0x02}
		}},
		{"Settlement block removed", func(dm *nom.DetailedMomentum) ([]*nom.DetailedMomentum, uint64, types.Hash) {
			i := settlementIdx(dm)[0]
			dm.AccountBlocks = append(dm.AccountBlocks[:i:i], dm.AccountBlocks[i+1:]...)
			return []*nom.DetailedMomentum{dm}, rh, hash
		}},
		{"receive FromBlockHash changed", func(dm *nom.DetailedMomentum) ([]*nom.DetailedMomentum, uint64, types.Hash) {
			for _, i := range settlementIdx(dm) {
				if b := dm.AccountBlocks[i]; b.BlockType == nom.BlockTypeContractReceive {
					b.FromBlockHash = types.Hash{0x04}
					break
				}
			}
			return []*nom.DetailedMomentum{dm}, rh, hash
		}},
		{"nil descendant", func(dm *nom.DetailedMomentum) ([]*nom.DetailedMomentum, uint64, types.Hash) {
			b := dm.AccountBlocks[settlementIdx(dm)[0]]
			b.DescendantBlocks = append(b.DescendantBlocks, nil)
			return []*nom.DetailedMomentum{dm}, rh, hash
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ms, h, hh := tc.mutate(fx.DetailedMomentum(t, rh))
			_, _, _, err := settlementReceives(ms, h, hh)
			mustPlain(t, tc.name, err)
		})
	}
}

func TestAssignmentReorgMidRead(t *testing.T) {
	fx := znndtest.Load(t)
	rh := fx.Assignment.ReceiveHeight
	hash := hashOf(t, fx, rh)
	c0 := fx.Expected.DomainInputCounter

	alt := fx.DetailedMomentum(t, rh)
	alt.Momentum.StateRoot = types.Hash{0x03}
	alt.Momentum.Hash = alt.Momentum.ComputeHash()

	// Proof reads: 1 is the counter, 2 and 3 the served ordinals, 4 ordinal c0-1. The last run
	// swaps the momentum on read 4 and leaves the roots alone, so only the final re-check sees it.
	for _, tc := range []struct {
		name     string
		n        int32
		rewrite  bool
		wantText string
	}{
		{"counter read", 1, true, "changed during the read"},
		{"ordinal read", 2, true, "changed during the read"},
		{"previous ordinal read", 4, true, "changed during the read"},
		{"final re-check", 4, false, "changed during the read"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			srv, r := newAssignmentReader(t, fx)
			var calls atomic.Int32
			srv.TamperProof(func(p *znndtest.Proof) {
				k := calls.Add(1)
				if k == tc.n {
					srv.SetMomentum(alt)
				}
				if tc.rewrite && k >= tc.n {
					p.Root = alt.Momentum.StateRoot
				}
			})
			_, err := r.Assignment(fx.DomainID, rh, hash, c0)
			mustPlain(t, tc.name, err)
			if !strings.Contains(err.Error(), tc.wantText) {
				t.Fatalf("error %q lacks %q", err, tc.wantText)
			}
		})
	}
}

// A cursor whose previous ordinal is absent from the proven state, as when a batch swept it
// before the receive momentum, is not ahead of Settlement. The fixture has no such ordinal, so
// the test serves proofs from its own tree under a momentum with the matching state root.
func TestAssignmentSweptPreviousOrdinal(t *testing.T) {
	fx := znndtest.Load(t)
	rh := fx.Assignment.ReceiveHeight
	c0 := fx.Expected.DomainInputCounter

	var atR []znndtest.FixtureProof
	for _, p := range fx.Proofs {
		if p.Height == rh {
			atR = append(atR, p)
		}
	}
	// Proofs at R: counter 1, counter 2, then ordinals c0-1 .. count-1.
	if uint64(len(atR)) != 2+1+uint64(len(fx.Assignment.Entries)) {
		t.Fatalf("fixture holds %d proofs at height %d, want %d", len(atR), rh, 3+len(fx.Assignment.Entries))
	}
	swept := atR[2].Key
	tree := trie.NewCompactTree()
	pathOf := func(key []byte) trie.Path { return trie.Path(types.NewHash(key)) }
	for i, p := range atR {
		if i != 2 && len(p.Result.Value) > 0 {
			tree.Insert(pathOf(p.Key), p.Result.Value)
		}
	}
	root := tree.Root()

	alt := fx.DetailedMomentum(t, rh)
	alt.Momentum.StateRoot = root
	alt.Momentum.Hash = alt.Momentum.ComputeHash()

	synth := *fx
	synth.Proofs = nil
	for _, p := range atR {
		_, value, proof := tree.ProveByPath(pathOf(p.Key))
		synth.Proofs = append(synth.Proofs, znndtest.FixtureProof{
			Height: rh,
			Key:    p.Key,
			Result: znndtest.Proof{Value: value, Proof: proof, Root: root},
		})
	}
	if present, _, _ := tree.ProveByPath(pathOf(swept)); present {
		t.Fatal("swept ordinal is present in the synthetic tree")
	}

	srv := znndtest.NewServer(t, &synth, rh+10)
	srv.SetMomentum(alt)
	r := NewReader(znnd.NewClient(srv.URL))

	got, err := r.Assignment(fx.DomainID, rh, alt.Momentum.Hash, c0)
	if err != nil {
		t.Fatalf("Assignment with a swept previous ordinal: %v", err)
	}
	if want := wantEntries(fx); !reflect.DeepEqual(got, want) {
		t.Fatalf("Assignment = %+v, want %+v", got, want)
	}
}
