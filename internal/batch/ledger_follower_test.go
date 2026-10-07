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
	"github.com/digitalSloth/go-executor/internal/ledger/ledgertest"
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

// Assignment answers Settlement's assignment over the source's momenta.
func (s *followerSource) Assignment(domainID uint32, height uint64, hash types.Hash, nextOrdinal uint64) ([]ledger.AssignmentEntry, error) {
	return ledgertest.Assign(s.moms, domainID, height, hash, nextOrdinal)
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
		Data:    ledgertest.SendData(7, nil),
	}
}

// followerMomentum builds one valid, self-consistent momentum at height with the given
// account-blocks and previous-hash linkage.
func followerMomentum(height uint64, prev types.Hash, sends []*nom.AccountBlock) *nom.DetailedMomentum {
	blocks := append(append([]*nom.AccountBlock(nil), sends...),
		&nom.AccountBlock{Address: types.SettlementContract, Height: height, Hash: followerHash(0xEE, height)})
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

func newTestFollower(t *testing.T, src *followerSource, domainID uint32) *ledger.Follower {
	t.Helper()
	f, err := ledger.New(ledger.Config{
		DomainID:          domainID,
		ConfirmationDepth: 1,
		StartHeight:       1,
		Source:            src,
		Assignments:       src,
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
	follower := newTestFollower(t, src, 7)

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

	// Height 1 is valid; height 2 is replaced by a momentum claiming height 5, which
	// processMomentum rejects as a height mismatch and latches permanently.
	m1 := followerMomentum(1, types.Hash{}, []*nom.AccountBlock{followerBlock(1, 1)})
	bad := followerMomentum(5, followerHash(0xAA, 1), []*nom.AccountBlock{followerBlock(2, 2)})
	src := &followerSource{frontier: 5, moms: []*nom.DetailedMomentum{m1, bad}}
	follower := newTestFollower(t, src, 7)

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
