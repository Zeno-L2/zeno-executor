package runtime

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/zenon-network/go-zenon/common/types"

	"github.com/digitalSloth/go-executor/domain"
	"github.com/digitalSloth/go-executor/internal/batch"
	"github.com/digitalSloth/go-executor/internal/codec"
	"github.com/digitalSloth/go-executor/internal/ledger"
	"github.com/digitalSloth/go-executor/internal/recovery"
	"github.com/digitalSloth/go-executor/internal/state"
)

// pollInterval is how long the loop waits when the follower has nothing further: it advances
// only as the confirmation frontier moves.
const pollInterval = time.Second

// commitInputInterval is how many inputs commitBoundary lets through before it forces a commit
// boundary. Sixteen full-cap batches; a crash re-derives at most that many inputs plus one
// momentum's remainder. A package-level variable so this package's tests can lower it.
var commitInputInterval = 1024

// commitBoundary wraps the follower as the driver's input source and reports the source drained
// at the first momentum boundary after commitInputInterval inputs, which closes the open batch
// there and makes it committable. Without it a saturated domain, and every initial catch-up from
// height 1, would run for thousands of momenta before the follower's buffer happened to be empty
// at a batch boundary, holding every BatchRecord since boot in memory and losing all of it to a
// crash. A momentum boundary is where the follower reports a Checkpoint, and firing there is what
// keeps the closed batch a safe boundary: nothing is buffered, and the driver carries nothing,
// because a carry closes the batch before the wrapper is asked again.
//
// A fire can land on a call where the driver has accumulated nothing: the driver closes a batch
// at MaxBatchInputs without asking the source again, so when the last input of a batch is also
// the interval's last and the last buffered input of its momentum, the next NextBatch asks the
// wrapper first and gets ok == false with nothing accumulated. Nothing is lost — that batch
// closed at a safe boundary and the commit step already took it — and the cost is one
// pollInterval before the loop asks again.
type commitBoundary struct {
	follower  *ledger.Follower
	delivered int
}

func (s *commitBoundary) Next() (ledger.Input, bool, error) {
	if s.delivered >= commitInputInterval {
		if _, ok := s.follower.Checkpoint(); ok {
			s.delivered = 0
			return ledger.Input{}, false, nil
		}
	}
	in, ok, err := s.follower.Next()
	if ok {
		s.delivered++
	}
	return in, ok, err
}

// loop is the execution loop's state. pos and fresh mirror the last durable position: they come
// from Ready at boot, from every successful Commit, and from every Rollback's Ready.
type loop struct {
	cfg      Config
	rcfg     recovery.Config
	executor types.Address
	dom      domain.Domain
	bundles  *recovery.BundleStore

	store    *state.Store
	follower *ledger.Follower
	driver   *batch.Driver

	pos   recovery.Position
	fresh bool
	// pending holds the commitments of batches built since the last commit.
	pending []codec.BatchCommitment

	anchor     recovery.Anchor
	haveAnchor bool
	verify     bool
}

func halt(err error) error {
	if err == nil || errors.Is(err, recovery.ErrHalted) {
		return err
	}
	return fmt.Errorf("%w: %w", recovery.ErrHalted, err)
}

// sleep waits d, and reports false when ctx ended first.
func sleep(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

// build wires the follower, commit boundary and driver at a ready position.
func (l *loop) build(rdy recovery.Ready) error {
	startHeight, startOrdinal := uint64(1), uint64(0)
	if !rdy.Fresh {
		startHeight, startOrdinal = rdy.Position.MomentumHeight+1, rdy.Position.NextOrdinal
	}
	follower, err := ledger.New(ledger.Config{
		DomainID:          l.cfg.DomainID,
		ConfirmationDepth: l.cfg.ConfirmationDepth,
		StartHeight:       startHeight,
		StartOrdinal:      startOrdinal,
		MaxBufferedInputs: commitInputInterval,
		Source:            l.cfg.Momenta,
		Assignments:       l.cfg.Assignments,
		Caps:              l.cfg.Caps,
	})
	if err != nil {
		return err
	}
	driver, err := batch.New(batch.Config{
		DomainID:     l.cfg.DomainID,
		ExecutorID:   l.executor,
		Domain:       l.dom,
		Store:        rdy.Store,
		Inputs:       &commitBoundary{follower: follower},
		Bundles:      l.bundles,
		FirstBatchID: rdy.Position.NextBatchID,
	})
	if err != nil {
		return err
	}
	l.store, l.follower, l.driver = rdy.Store, follower, driver
	l.pos, l.fresh = rdy.Position, rdy.Fresh
	l.pending = nil
	return nil
}

func (l *loop) run(ctx context.Context, rdy recovery.Ready) error {
	l.anchor, l.haveAnchor, l.verify = rdy.Anchor, rdy.HaveAnchor, rdy.VerifyAnchor
	if err := l.build(rdy); err != nil {
		rdy.Store.Close()
		return halt(err)
	}
	defer func() { l.store.Close() }()

	policy := l.cfg.Retry.WithDefaults()
	sourceFailures := 0
	for {
		if ctx.Err() != nil {
			return nil
		}

		b, ok, err := l.driver.NextBatch()
		var reorg *ledger.ReorgError
		switch {
		case errors.As(err, &reorg):
			if err := l.reorg(); err != nil {
				return err
			}
			sourceFailures = 0
			continue
		// An assignment halt also matches ledger.ErrSourceRead, so this case precedes that one.
		case errors.Is(err, recovery.ErrHalted):
			return err
		// The driver may have applied inputs it now discards, so a cap outage is never retried
		// here: the run ends and a restart re-derives from the last commit.
		case errors.Is(err, ledger.ErrCapRead):
			return fmt.Errorf("%w: %w", recovery.ErrSourceUnavailable, err)
		case errors.Is(err, ledger.ErrSourceRead):
			sourceFailures++
			if sourceFailures >= policy.SourceAttempts {
				return fmt.Errorf("%w: momentum source failed %d times in a row: %w", recovery.ErrSourceUnavailable, sourceFailures, err)
			}
			if !sleep(ctx, policy.SourceInterval) {
				return nil
			}
			continue
		case err != nil:
			return halt(err)
		}
		sourceFailures = 0

		if ok {
			if l.verify {
				if err := l.checkAnchor(b); err != nil {
					return err
				}
			}
			l.pending = append(l.pending, b.Commitment)
		}

		if err := l.commitStep(); err != nil {
			return err
		}

		if !ok && !sleep(ctx, pollInterval) {
			return nil
		}
	}
}

// commitStep commits the pending records when the follower is at a safe boundary: it holds no
// input it has read but not returned, and the driver holds no carried record — a carried input
// has already been emitted, so the last emitted index would exceed the last pending record's
// LastInputSeq. Otherwise the records stay pending and the next boundary commits them; nothing
// is lost, since an uncommitted batch's inputs are re-derived after a crash.
func (l *loop) commitStep() error {
	if len(l.pending) == 0 {
		return nil
	}
	last := l.pending[len(l.pending)-1]
	cp, ok := l.follower.Checkpoint()
	if !ok {
		return nil
	}
	if emitted, have := l.follower.LastEmittedIndex(); !have || emitted != last.LastInputSeq {
		return nil
	}

	pos := recovery.Position{
		LastInputSeq:   last.LastInputSeq,
		PostStateRoot:  last.PostStateRoot,
		MomentumHeight: cp.Height,
		MomentumHash:   cp.Hash,
		NextOrdinal:    cp.NextOrdinal,
		NextBatchID:    last.BatchID + 1,
	}
	recent := l.follower.RecentMomenta()
	window := make([]recovery.MomentumRef, len(recent))
	for i, m := range recent {
		window[i] = recovery.MomentumRef{Height: m.Height, Hash: m.Hash}
	}
	records := make([]recovery.BatchRecord, len(l.pending))
	for i, c := range l.pending {
		records[i] = recovery.BatchRecord{
			BatchID:       c.BatchID,
			Status:        recovery.StatusBuilt,
			FirstInputSeq: c.FirstInputSeq,
			LastInputSeq:  c.LastInputSeq,
			PreStateRoot:  c.PreStateRoot,
			PostStateRoot: c.PostStateRoot,
			DAHash:        c.DAHash,
		}
	}
	// Every recovery.Commit failure is transient. That covers its write failures and its
	// pre-write checks (config, malformed position, window contiguity, window end against the
	// position, store.go:485-497). It returns unhalted, and a restart re-derives from the last
	// durable position. A check that fails the same way on every attempt ends in the
	// supervisor's crash-loop halt.
	if err := recovery.Commit(l.rcfg, l.store, pos, records, window); err != nil {
		return err
	}
	committed := l.pending
	l.pos, l.fresh, l.pending = pos, false, nil
	if l.cfg.OnCommit != nil {
		if err := l.cfg.OnCommit(committed); err != nil {
			return fmt.Errorf("runtime: report committed batches: %w", err)
		}
	}
	return nil
}

// checkAnchor compares the finalized anchor's root against the root this run derived at the
// anchor's input index, once the batch containing that index is built. The bundle carries a
// PostStateRoot per input, so the comparison works at any index inside a batch and needs no
// forced batch boundary at the anchor.
func (l *loop) checkAnchor(b batch.Batch) error {
	seq := l.anchor.LastInputSeq
	switch {
	case seq > b.Commitment.LastInputSeq:
		return nil
	case seq < b.Commitment.FirstInputSeq:
		// The anchor's index is below this batch and was never one of this domain's inputs in
		// the derived stream, so the finalized batch and this derivation disagree about the
		// canonical stream itself.
		return halt(fmt.Errorf("finalized anchor's input %d is not in the derived stream (next batch starts at %d)", seq, b.Commitment.FirstInputSeq))
	}
	inputs, err := codec.DecodeBundle(b.Bundle)
	if err != nil {
		return halt(fmt.Errorf("decode bundle of batch %d: %w", b.Commitment.BatchID, err))
	}
	for _, in := range inputs {
		if in.GlobalInputIndex != seq {
			continue
		}
		result, err := codec.DecodeExecutionResult(in.ExecutionResult)
		if err != nil {
			return halt(fmt.Errorf("decode execution result at input %d: %w", seq, err))
		}
		if result.PostStateRoot != l.anchor.PostStateRoot {
			return halt(fmt.Errorf("derived root %s at input %d differs from the finalized root %s", result.PostStateRoot, seq, l.anchor.PostStateRoot))
		}
		l.verify = false
		return nil
	}
	return halt(fmt.Errorf("batch %d has no record at the finalized anchor's input %d", b.Commitment.BatchID, seq))
}

// reorg handles a *ledger.ReorgError: it drops the uncommitted records, the driver and the
// store, rolls back to the fork parent of the committed position — or to genesis when nothing
// has been committed — re-runs Reconcile against the rolled-back position, and rebuilds.
//
// The ForkParent search usually returns the committed position's own height, because the
// follower's own last consumed height is above the last commit whenever any momentum has been
// consumed since. The rollback there retracts nothing: it reloads the store this method just
// discarded, from the cursor's own snapshot, and leaves the recorded window as it stands.
//
// The follower built afterwards starts at the rolled-back Position, so its next expected
// ordinal rolls back with the cursor.
func (l *loop) reorg() error {
	log.Printf("runtime: reorg detected at committed momentum height %d, rolling back", l.pos.MomentumHeight)
	l.pending = nil
	l.store.Close()

	var (
		rdy recovery.Ready
		err error
	)
	if l.fresh {
		rdy, err = recovery.Rollback(l.rcfg, 0)
	} else {
		height, ok, ferr := recovery.ForkParent(l.rcfg, l.pos)
		if ferr != nil {
			if errors.Is(ferr, recovery.ErrSourceUnavailable) {
				return ferr
			}
			return halt(ferr)
		}
		if !ok {
			return halt(fmt.Errorf("reorg below every momentum height this executor has a record of (committed height %d)", l.pos.MomentumHeight))
		}
		rdy, err = recovery.Rollback(l.rcfg, height)
	}
	if err != nil {
		return halt(err)
	}

	verify, err := recovery.Reconcile(l.rcfg, l.anchor, l.haveAnchor, rdy.Position, rdy.Fresh)
	if err != nil {
		rdy.Store.Close()
		return halt(err)
	}
	l.verify = verify
	if err := l.build(rdy); err != nil {
		rdy.Store.Close()
		return halt(err)
	}
	return nil
}
