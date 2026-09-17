package batch

import (
	"encoding/binary"
	"errors"
	"testing"

	"github.com/zenon-network/go-zenon/chain/nom"
	"github.com/zenon-network/go-zenon/common/types"

	"github.com/digitalSloth/go-executor/domain"
	"github.com/digitalSloth/go-executor/internal/codec"
	"github.com/digitalSloth/go-executor/internal/ledger"
)

// This file drives Driver.NextBatch through a real *ledger.Follower (as opposed to the
// fakeInputSource used elsewhere), so the driver's source-error classification is exercised
// against ledger's actual *ReorgError type and its actual latched-vs-unlatched error behaviour,
// not a fake that only mimics the contract.

// followerSource is a minimal ledger.MomentumSource: a slice of momenta plus a settable
// frontier, in the style of internal/ledger/follower_test.go's fakeSource.
type followerSource struct {
	frontier uint64
	moms     []*nom.DetailedMomentum
	readErr  error
}

func (s *followerSource) FrontierHeight() (uint64, error) { return s.frontier, nil }

func (s *followerSource) DetailedMomentumsByHeight(height, count uint64) ([]*nom.DetailedMomentum, error) {
	if s.readErr != nil {
		err := s.readErr
		s.readErr = nil
		return nil, err
	}
	var out []*nom.DetailedMomentum
	for _, dm := range s.moms {
		if dm.Momentum.Height < height {
			continue
		}
		if uint64(len(out)) >= count {
			break
		}
		out = append(out, dm)
	}
	return out, nil
}

// acceptAllFilter derives one canonical kind-0 CallL2 input, for the configured domain, from
// every account-block it sees.
type acceptAllFilter struct {
	domainID uint32
	target   types.Hash
}

func (f acceptAllFilter) Accept(m *nom.Momentum, block *nom.AccountBlock) (*ledger.Envelope, error) {
	src := types.Hash{}
	src[0] = block.Address[0]
	in := codec.Input{
		SourceAccountBlockHash: src,
		Args:                   codec.CallL2Args{TargetContract: f.target},
	}
	payload, err := in.Encode()
	if err != nil {
		return nil, err
	}
	return &ledger.Envelope{DomainID: f.domainID, Payload: payload}, nil
}

func followerHash(b byte, height uint64) types.Hash {
	var h types.Hash
	h[0] = b
	binary.BigEndian.PutUint64(h[24:], height)
	return h
}

func followerBlock(address byte, height uint64) *nom.AccountBlock {
	return &nom.AccountBlock{
		Address: types.Address{address},
		Height:  1,
		Hash:    followerHash(address, height),
	}
}

// followerMomentum builds one valid, self-consistent momentum at height with the given
// account-blocks and previous-hash linkage.
func followerMomentum(height uint64, prev types.Hash, blocks []*nom.AccountBlock) *nom.DetailedMomentum {
	return &nom.DetailedMomentum{
		Momentum: &nom.Momentum{
			Height:        height,
			Hash:          followerHash(0xAA, height),
			PreviousHash:  prev,
			TimestampUnix: 1000 + height,
			Content:       nom.NewMomentumContent(blocks),
		},
		AccountBlocks: blocks,
	}
}

func newTestFollower(t *testing.T, src *followerSource, domainID uint32, target types.Hash) *ledger.Follower {
	t.Helper()
	f, err := ledger.New(ledger.Config{
		DomainID:          domainID,
		ConfirmationDepth: 1,
		StartHeight:       1,
		Source:            src,
		Filter:            acceptAllFilter{domainID: domainID, target: target},
	})
	if err != nil {
		t.Fatalf("ledger.New: %v", err)
	}
	return f
}

func successDomain(contractID types.Hash, n int) *fakeDomain {
	steps := make([]fakeStep, n)
	for i := range steps {
		steps[i] = fakeStep{effects: domain.Effects{Status: codec.StatusSuccess, ContractID: contractID}}
	}
	return &fakeDomain{steps: steps}
}

// TestDriverRealFollowerReorgMidBatch drives NextBatch through a real ledger.Follower whose
// underlying MomentumSource reports momentum 4's PreviousHash mismatching momentum 3's Hash
// partway through — the actual *ledger.ReorgError construction path (follower.go:310-313), not
// a scripted fake error.
func TestDriverRealFollowerReorgMidBatch(t *testing.T) {
	contractID := hashN(2)
	target := contractID

	// Momenta 1-3 form a valid chain; momentum 4 is confirmed (frontier 5, depth 1 -> ceiling
	// 4) but names a PreviousHash that does not match momentum 3's Hash — the real trigger for
	// processMomentum's mid-window pendingReorg branch (follower.go:310-313). All four are
	// returned from a single DetailedMomentumsByHeight call, so momenta 1-3 are buffered before
	// momentum 4's mismatch is discovered.
	m1 := followerMomentum(1, types.Hash{}, []*nom.AccountBlock{followerBlock(1, 1)})
	m2 := followerMomentum(2, followerHash(0xAA, 1), []*nom.AccountBlock{followerBlock(2, 2)})
	m3 := followerMomentum(3, followerHash(0xAA, 2), []*nom.AccountBlock{followerBlock(3, 3)})
	m4 := followerMomentum(4, followerHash(0xBB, 99), []*nom.AccountBlock{followerBlock(4, 4)})
	src := &followerSource{frontier: 5, moms: []*nom.DetailedMomentum{m1, m2, m3, m4}}
	follower := newTestFollower(t, src, 7, target)

	// Three successful steps: the driver consumes momenta 1-3's inputs before its fourth
	// Inputs.Next() call surfaces the reorg buffered alongside them.
	dom := successDomain(contractID, 3)
	cfg := testConfig(t, dom, follower, newFakeBundleStore())
	cfg.DomainID = 7
	d, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	_, ok, err := d.NextBatch()
	if ok {
		t.Fatalf("ok = true, want false (the reorg must discard the 3 already-accumulated records, not close a batch)")
	}
	var re *ledger.ReorgError
	if !errors.As(err, &re) {
		t.Fatalf("errors.As(err, *ledger.ReorgError) = false, err = %v", err)
	}
	if re.Height != 4 {
		t.Fatalf("ReorgError.Height = %d, want 4 (the mismatched momentum)", re.Height)
	}
}

// TestDriverRealFollowerPermanentErrorClosesThenRepresents drives NextBatch through a real
// ledger.Follower whose source momentum stream is malformed (a height gap), triggering the
// Follower's own latched-permanent-error path (follower.go:303-306), not a scripted fake.
func TestDriverRealFollowerPermanentErrorClosesThenRepresents(t *testing.T) {
	contractID := hashN(2)
	target := contractID

	// Height 1 is valid; height 2 is replaced by a momentum claiming height 5, which
	// processMomentum rejects as a height mismatch and latches permanently.
	m1 := followerMomentum(1, types.Hash{}, []*nom.AccountBlock{followerBlock(1, 1)})
	bad := followerMomentum(5, followerHash(0xAA, 1), []*nom.AccountBlock{followerBlock(2, 2)})
	src := &followerSource{frontier: 5, moms: []*nom.DetailedMomentum{m1, bad}}
	follower := newTestFollower(t, src, 7, target)

	dom := successDomain(contractID, 1)
	cfg := testConfig(t, dom, follower, newFakeBundleStore())
	cfg.DomainID = 7
	d, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	got, ok, err := d.NextBatch()
	if err != nil || !ok {
		t.Fatalf("NextBatch: ok=%v err=%v, want a closed batch of 1 with a nil error", ok, err)
	}
	if len(got.Receipts) != 1 {
		t.Fatalf("len(Receipts) = %d, want 1", len(got.Receipts))
	}

	_, ok, err = d.NextBatch()
	if ok {
		t.Fatalf("second NextBatch: ok = true, want false")
	}
	if err == nil {
		t.Fatalf("second NextBatch err = nil, want the latched height-mismatch error to re-present")
	}
	var re *ledger.ReorgError
	if errors.As(err, &re) {
		t.Fatalf("second NextBatch err classified as *ledger.ReorgError, want a permanent halt-with-nil-batch-error, not a reorg")
	}
}
