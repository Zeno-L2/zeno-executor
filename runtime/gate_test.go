package runtime

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/digitalSloth/go-executor/internal/recovery"
	"github.com/zenon-network/go-zenon/chain/nom"
	"github.com/zenon-network/go-zenon/vm/embedded/definition"
)

// fakeCursor is a CursorSource whose answer a test changes while the run polls it.
type fakeCursor struct {
	mu    sync.Mutex
	next  uint64
	err   error
	calls int

	// entries is indexed by batchId. perCall caps the entries one AcceptedEntries call returns
	// (0 means 64). onAccepted, when set, runs with the requested batchId before each answer.
	entries       []*definition.BatchEntry
	perCall       int
	acceptedErr   error
	acceptedCalls int
	onAccepted    func(from uint64)
}

func (f *fakeCursor) AcceptedEntries(_ uint32, from uint64) (uint64, []*definition.BatchEntry, error) {
	f.mu.Lock()
	hook := f.onAccepted
	f.mu.Unlock()
	if hook != nil {
		hook(from)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.acceptedCalls++
	if f.acceptedErr != nil {
		return 0, nil, f.acceptedErr
	}
	limit := f.perCall
	if limit == 0 {
		limit = 64
	}
	var out []*definition.BatchEntry
	for b := from; b < f.next && b < uint64(len(f.entries)) && len(out) < limit; b++ {
		out = append(out, f.entries[b])
	}
	return f.next, out, nil
}

func (f *fakeCursor) NextBatchID(uint32) (uint64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	return f.next, f.err
}

func (f *fakeCursor) set(next uint64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.next = next
}

func (f *fakeCursor) readCalls() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

func nextBatchIDOf(t *testing.T, cfg Config) uint64 {
	t.Helper()
	pos, ok := cursorAt(t, cfg)
	if !ok {
		return 0
	}
	return pos.NextBatchID
}

func waitNextBatchID(t *testing.T, cfg Config, want uint64) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		if nextBatchIDOf(t, cfg) == want {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("NextBatchID = %d, want %d", nextBatchIDOf(t, cfg), want)
}

// holdsNextBatchID fails unless NextBatchID stays at want across two poll intervals.
func holdsNextBatchID(t *testing.T, cfg Config, want uint64) {
	t.Helper()
	time.Sleep(2*pollInterval + 200*time.Millisecond)
	if got := nextBatchIDOf(t, cfg); got != want {
		t.Fatalf("NextBatchID = %d, want it held at %d", got, want)
	}
}

func lowerCommitInterval(t *testing.T, n int) {
	t.Helper()
	old := commitInputInterval
	commitInputInterval = n
	t.Cleanup(func() { commitInputInterval = old })
}

func TestGateStopsAtMaxPendingBatchesAndResumes(t *testing.T) {
	lowerCommitInterval(t, 2)
	src := &chainSource{frontier: 62, moms: buildChain(t, 60, 0)}
	cur := &fakeCursor{}
	cfg := validConfig(t, src)
	cfg.MaxPendingBatches = 2
	cfg.Cursor = cur
	l := startRun(t, cfg, src, scriptDomain{})

	waitNextBatchID(t, cfg, 2)
	holdsNextBatchID(t, cfg, 2)
	before, _ := cursorAt(t, cfg)

	// The chain accepts two batches: the run seals two more and stops again. A stop that is
	// never cleared leaves the position where it was.
	cur.set(2)
	waitNextBatchID(t, cfg, 4)
	holdsNextBatchID(t, cfg, 4)
	after, _ := cursorAt(t, cfg)
	if after.LastInputSeq <= before.LastInputSeq {
		t.Fatalf("LastInputSeq %d did not advance past %d", after.LastInputSeq, before.LastInputSeq)
	}

	records, err := recovery.ListBatchRecords(layoutOf(t, cfg))
	if err != nil {
		t.Fatal(err)
	}
	var next uint64
	for _, r := range records {
		if r.FirstInputSeq != next || r.LastInputSeq < r.FirstInputSeq {
			t.Fatalf("record %d spans %d..%d, want it to start at %d", r.BatchID, r.FirstInputSeq, r.LastInputSeq, next)
		}
		next = r.LastInputSeq + 2
	}
	if err := l.stop(); err != nil {
		t.Fatalf("Run: %v", err)
	}
}

func TestGateCommitsPendingBatchesBeforeStopping(t *testing.T) {
	// One momentum holds 200 domain inputs, so the driver closes three full batches and a
	// remainder before the follower reaches a checkpoint.
	src := &chainSource{frontier: 6, moms: buildShape(1, 4, func(uint64) byte { return 0 }, func(uint64) int { return 200 })}
	cfg := validConfig(t, src)
	cfg.MaxPendingBatches = 1
	cfg.Cursor = &fakeCursor{}
	l := startRun(t, cfg, src, scriptDomain{})

	deadline := time.Now().Add(15 * time.Second)
	for nextBatchIDOf(t, cfg) < 2 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	first, _ := cursorAt(t, cfg)
	if first.NextBatchID < 2 {
		t.Fatalf("first commit = %+v, want the batches built after the trip committed together", first)
	}
	holdsNextBatchID(t, cfg, first.NextBatchID)
	if err := l.stop(); err != nil {
		t.Fatalf("Run: %v", err)
	}
}

// risingSource raises the frontier by one momentum on every ranged
// read, so the follower delivers more inputs between the gate
// tripping and the next buffer checkpoint.
type risingSource struct {
	*chainSource
}

func (s risingSource) DetailedMomentumsByHeight(height, count uint64) ([]*nom.DetailedMomentum, error) {
	out, err := s.chainSource.DetailedMomentumsByHeight(height, count)
	s.chainSource.mu.Lock()
	s.chainSource.frontier++
	s.chainSource.mu.Unlock()
	return out, err
}

// TestGateStopsTheFollowerAtATrip pins that a trip with pending
// batches stops the follower at the next checkpoint. Without the
// stop, the follower keeps delivering the rising frontier's momenta
// and the driver seals past the first commit.
func TestGateStopsTheFollowerAtATrip(t *testing.T) {
	chain := &chainSource{frontier: 3, moms: buildShape(1, 8, func(uint64) byte { return 0 }, func(uint64) int { return 200 })}
	src := risingSource{chain}
	cfg := validConfig(t, src)
	cfg.MaxPendingBatches = 1
	cfg.Cursor = &fakeCursor{}
	l := startRun(t, cfg, chain, scriptDomain{})

	deadline := time.Now().Add(15 * time.Second)
	for nextBatchIDOf(t, cfg) < 2 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	first, _ := cursorAt(t, cfg)
	if first.NextBatchID != 4 {
		t.Fatalf("first commit NextBatchID = %d, want 4 (three full batches and a remainder)", first.NextBatchID)
	}
	holdsNextBatchID(t, cfg, first.NextBatchID)
	if err := l.stop(); err != nil {
		t.Fatalf("Run: %v", err)
	}
}

func TestGateResumesAfterAPendingTrip(t *testing.T) {
	lowerCommitInterval(t, 2)
	// Momentum 1 holds 200 inputs, so the trip finds pending batches and forces a commit at the
	// checkpoint. Every later momentum holds one input.
	src := &chainSource{frontier: 22, moms: buildShape(1, 20, func(uint64) byte { return 0 }, func(h uint64) int {
		if h == 1 {
			return 200
		}
		return 1
	})}
	cur := &fakeCursor{}
	cfg := validConfig(t, src)
	cfg.MaxPendingBatches = 1
	cfg.Cursor = cur
	l := startRun(t, cfg, src, scriptDomain{})

	deadline := time.Now().Add(15 * time.Second)
	for nextBatchIDOf(t, cfg) < 2 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	first, _ := cursorAt(t, cfg)
	holdsNextBatchID(t, cfg, first.NextBatchID)

	// The chain accepts everything committed so far: the run resumes and seals one more batch.
	cur.set(first.NextBatchID)
	waitNextBatchID(t, cfg, first.NextBatchID+1)
	if err := l.stop(); err != nil {
		t.Fatalf("Run: %v", err)
	}
}

func TestGateCursorErrors(t *testing.T) {
	lowerCommitInterval(t, 2)
	run := func(t *testing.T, cursorErr error) error {
		src := &chainSource{frontier: 22, moms: buildChain(t, 20, 0)}
		cfg := validConfig(t, src)
		cfg.MaxPendingBatches = 1
		cfg.Cursor = &fakeCursor{err: cursorErr}
		return startRun(t, cfg, src, scriptDomain{}).exit()
	}

	t.Run("halted", func(t *testing.T) {
		if err := run(t, errors.Join(recovery.ErrHalted, errors.New("bad"))); !errors.Is(err, recovery.ErrHalted) {
			t.Fatalf("Run = %v, want ErrHalted", err)
		}
	})

	t.Run("unavailable", func(t *testing.T) {
		if err := run(t, errors.New("node down")); !errors.Is(err, recovery.ErrSourceUnavailable) || errors.Is(err, recovery.ErrHalted) {
			t.Fatalf("Run = %v, want ErrSourceUnavailable", err)
		}
	})
}

func TestLongResyncIgnoresMaxPendingBatches(t *testing.T) {
	lowerCommitInterval(t, 8)

	natSrc := &chainSource{frontier: 62, moms: buildShape(1, 60, zeroSalt, varied)}
	natCfg := validConfig(t, natSrc)
	natCfg.MaxPendingBatches = 1 << 20
	var natLog commitLog
	natLog.attach(&natCfg)
	natRun := startRun(t, natCfg, natSrc, scriptDomain{})
	natRun.waitFor("the cursor at height 60", natRun.cursorHeight(60))
	if err := natRun.stop(); err != nil {
		t.Fatalf("natural run: %v", err)
	}
	nat := natLog.flat()
	n := uint64(len(nat))
	if n < 12 {
		t.Fatalf("natural run made %d batches, want at least 12", n)
	}

	// Heights 61..63 hold 6 domain inputs, fewer than the interval, so they form one batch above
	// the chain.
	src := &chainSource{frontier: 65, moms: buildShape(1, 63, zeroSalt, varied)}
	cfg, cur, log := resyncConfig(t, src, nat, 4)
	cfg.MaxPendingBatches = 1
	l := startRun(t, cfg, src, scriptDomain{})

	waitNextBatchID(t, cfg, n+1)
	holdsNextBatchID(t, cfg, n+1)
	if err := l.stop(); err != nil {
		t.Fatalf("Run: %v", err)
	}

	got := log.flat()
	if uint64(len(got)) != n+1 {
		t.Fatalf("%d commitments, want %d", len(got), n+1)
	}
	assertCommitments(t, got, nat)
	if want := nat[n-1].LastInputSeq + 2; got[n].FirstInputSeq != want {
		t.Fatalf("batch %d starts at %d, want the first domain input at height 61 (%d)", n, got[n].FirstInputSeq, want)
	}
	cur.mu.Lock()
	calls := cur.acceptedCalls
	cur.mu.Unlock()
	if uint64(calls) < n/4 {
		t.Fatalf("AcceptedEntries called %d times, want at least %d", calls, n/4)
	}
}
