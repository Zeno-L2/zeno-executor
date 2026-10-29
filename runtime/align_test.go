package runtime

import (
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/zenon-network/go-zenon/chain/nom"
	"github.com/zenon-network/go-zenon/vm/embedded/definition"

	"github.com/Zeno-L2/zeno-executor/internal/batch"
	"github.com/Zeno-L2/zeno-executor/internal/codec"
	"github.com/Zeno-L2/zeno-executor/internal/ledger"
	"github.com/Zeno-L2/zeno-executor/internal/recovery"
)

// entryOf is the accepted entry that equals c on every field the worker compares.
func entryOf(c codec.BatchCommitment) *definition.BatchEntry {
	flow, err := c.AssetFlowSummary.Encode()
	if err != nil {
		panic(err)
	}
	e := &definition.BatchEntry{
		ProtocolVersion:  c.Versions.ProtocolVersion,
		FirstInputSeq:    c.FirstInputSeq,
		LastInputSeq:     c.LastInputSeq,
		PreStateRoot:     c.PreStateRoot,
		PostStateRoot:    c.PostStateRoot,
		InputRoot:        c.InputRoot,
		ReceiptRoot:      c.ReceiptRoot,
		EventRoot:        c.EventRoot,
		OutboxRoot:       c.OutboxRoot,
		DaHash:           c.DAHash,
		DaMode:           c.DAMode,
		AssetFlowSummary: flow,
		ProofData:        c.ProofData,
		State:            1,
	}
	e.DomainId, e.BatchId = c.DomainID, c.BatchID
	return e
}

func entriesOf(cs []codec.BatchCommitment) []*definition.BatchEntry {
	out := make([]*definition.BatchEntry, len(cs))
	for i, c := range cs {
		out[i] = entryOf(c)
	}
	return out
}

// commitLog records every OnCommit call.
type commitLog struct {
	mu    sync.Mutex
	calls [][]codec.BatchCommitment
}

func (c *commitLog) attach(cfg *Config) {
	cfg.OnCommit = func(cs []codec.BatchCommitment, _ Withdrawals) error {
		c.mu.Lock()
		defer c.mu.Unlock()
		c.calls = append(c.calls, append([]codec.BatchCommitment(nil), cs...))
		return nil
	}
}

func (c *commitLog) snapshot() [][]codec.BatchCommitment {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([][]codec.BatchCommitment(nil), c.calls...)
}

func (c *commitLog) flat() []codec.BatchCommitment {
	var out []codec.BatchCommitment
	for _, call := range c.snapshot() {
		out = append(out, call...)
	}
	return out
}

// waitCalls waits until n OnCommit calls have been made.
func (c *commitLog) waitCalls(t *testing.T, n int) {
	t.Helper()
	deadline := time.Now().Add(25 * time.Second)
	for time.Now().Before(deadline) {
		if len(c.snapshot()) >= n {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("%d OnCommit calls, want %d", len(c.snapshot()), n)
}

func callIDs(call []codec.BatchCommitment) []uint64 {
	ids := make([]uint64, len(call))
	for i, c := range call {
		ids[i] = c.BatchID
	}
	return ids
}

func zeroSalt(uint64) byte { return 0 }

// varied gives momentum h between 1 and 3 domain inputs.
func varied(h uint64) int { return 1 + int(h%3) }

func variedChain(to uint64) []*nom.DetailedMomentum { return buildShape(1, to, zeroSalt, varied) }

// naturalRun builds the chain once with the follower confirming one ceiling at a time, and
// returns every commitment it made.
func naturalRun(t *testing.T, chain []*nom.DetailedMomentum, ceilings ...uint64) []codec.BatchCommitment {
	t.Helper()
	src := &chainSource{}
	cfg := validConfig(t, src)
	var log commitLog
	log.attach(&cfg)
	l := startRun(t, cfg, src, scriptDomain{})
	for _, c := range ceilings {
		l.step(chain, c+cfg.ConfirmationDepth)
	}
	if err := l.stop(); err != nil {
		t.Fatalf("natural run: %v", err)
	}
	return log.flat()
}

func ceilingsTo(n uint64, stride uint64) []uint64 {
	var out []uint64
	for h := stride; h <= n; h += stride {
		out = append(out, h)
	}
	return out
}

var (
	perMomentumOnce sync.Once
	perMomentumNat  []codec.BatchCommitment
	pairedOnce      sync.Once
	pairedNat       []codec.BatchCommitment
)

// naturalPerMomentum is the history of 12 momenta confirmed one at a time: one batch each.
func naturalPerMomentum(t *testing.T) []codec.BatchCommitment {
	t.Helper()
	perMomentumOnce.Do(func() { perMomentumNat = naturalRun(t, variedChain(15), ceilingsTo(12, 1)...) })
	if len(perMomentumNat) != 12 {
		t.Fatalf("natural run made %d batches, want 12", len(perMomentumNat))
	}
	return perMomentumNat
}

// naturalPaired is the history of 4 momenta confirmed two at a time: batch 0 is heights 1..2 and
// batch 1 is heights 3..4.
func naturalPaired(t *testing.T) []codec.BatchCommitment {
	t.Helper()
	pairedOnce.Do(func() { pairedNat = naturalRun(t, variedChain(15), ceilingsTo(4, 2)...) })
	if len(pairedNat) != 2 {
		t.Fatalf("natural run made %d batches, want 2", len(pairedNat))
	}
	return pairedNat
}

// resyncConfig is a fresh domain directory whose chain has accepted nat.
func resyncConfig(t *testing.T, src *chainSource, nat []codec.BatchCommitment, perCall int) (Config, *fakeCursor, *commitLog) {
	t.Helper()
	cfg := validConfig(t, src)
	cur := &fakeCursor{next: uint64(len(nat)), entries: entriesOf(nat), perCall: perCall}
	cfg.Cursor = cur
	var log commitLog
	log.attach(&cfg)
	return cfg, cur, &log
}

func assertCommitments(t *testing.T, got, want []codec.BatchCommitment) {
	t.Helper()
	if len(got) < len(want) {
		t.Fatalf("%d commitments, want at least %d", len(got), len(want))
	}
	for i := range want {
		if !reflect.DeepEqual(got[i], want[i]) {
			t.Fatalf("commitment %d = %+v, want %+v", i, got[i], want[i])
		}
	}
}

func TestResyncReproducesAcceptedBatches(t *testing.T) {
	nat := naturalPerMomentum(t)
	chain := variedChain(15)

	// Control: without accepted entries the same chain, confirmed at once, builds different batches.
	csrc := &chainSource{frontier: 17, moms: chain}
	ccfg := validConfig(t, csrc)
	var clog commitLog
	clog.attach(&ccfg)
	cl := startRun(t, ccfg, csrc, scriptDomain{})
	waitNextBatchID(t, ccfg, 1)
	if err := cl.stop(); err != nil {
		t.Fatalf("control run: %v", err)
	}
	control := clog.flat()
	if len(control) == 0 || control[0].LastInputSeq == nat[0].LastInputSeq {
		t.Fatalf("control batch 0 ends at %v, the natural batch 0 ends at %d: the test cannot tell alignment from none", control, nat[0].LastInputSeq)
	}

	src := &chainSource{frontier: 17, moms: chain}
	cfg, cur, log := resyncConfig(t, src, nat, 2)
	l := startRun(t, cfg, src, scriptDomain{})
	waitNextBatchID(t, cfg, uint64(len(nat))+1)
	if err := l.stop(); err != nil {
		t.Fatalf("Run: %v", err)
	}

	got := log.flat()
	if len(got) != len(nat)+1 {
		t.Fatalf("%d commitments, want %d", len(got), len(nat)+1)
	}
	assertCommitments(t, got, nat)
	tail := got[len(nat)]
	if want := nat[len(nat)-1].LastInputSeq + 2; tail.FirstInputSeq != want {
		t.Fatalf("batch %d starts at %d, want %d (the first input after the accepted batches)", len(nat), tail.FirstInputSeq, want)
	}
	if tail.LastInputSeq != control[0].LastInputSeq {
		t.Fatalf("batch %d ends at %d, want the last available input %d", len(nat), tail.LastInputSeq, control[0].LastInputSeq)
	}
	if cur.acceptedCalls < len(nat)/2 {
		t.Fatalf("AcceptedEntries called %d times, want at least %d", cur.acceptedCalls, len(nat)/2)
	}
}

func TestAlignedCommitAtBufferEnd(t *testing.T) {
	nat := naturalPerMomentum(t)
	src := &chainSource{frontier: 14, moms: variedChain(15)}
	cfg, cur, _ := resyncConfig(t, src, nat, 0)
	l := startRun(t, cfg, src, scriptDomain{})

	waitNextBatchID(t, cfg, uint64(len(nat)))
	pos, ok := cursorAt(t, cfg)
	if !ok || pos.LastInputSeq != nat[len(nat)-1].LastInputSeq {
		t.Fatalf("cursor = %+v, want it at the last accepted batch's input %d", pos, nat[len(nat)-1].LastInputSeq)
	}

	// At the tip the run idles: one fetch for the window and one at the crossing, then none.
	holdsNextBatchID(t, cfg, uint64(len(nat)))
	cur.mu.Lock()
	calls := cur.acceptedCalls
	cur.mu.Unlock()
	if calls != 2 {
		t.Fatalf("AcceptedEntries called %d times, want 2", calls)
	}
	if err := l.stop(); err != nil {
		t.Fatalf("Run: %v", err)
	}
}

func TestAlignedBatchMismatchHalts(t *testing.T) {
	nat := naturalPerMomentum(t)
	src := &chainSource{frontier: 14, moms: variedChain(15)}
	cfg, cur, _ := resyncConfig(t, src, nat, 0)
	cur.entries[2].PostStateRoot[0] ^= 1
	l := startRun(t, cfg, src, scriptDomain{})

	err := l.exit()
	if !errors.Is(err, recovery.ErrHalted) {
		t.Fatalf("Run = %v, want ErrHalted", err)
	}
	for _, want := range []string{"batch 2", "postStateRoot", layoutOf(t, cfg).Root} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("Run = %v, want it to name %q", err, want)
		}
	}
	records, lerr := recovery.ListBatchRecords(layoutOf(t, cfg))
	if lerr != nil {
		t.Fatal(lerr)
	}
	for _, r := range records {
		if r.BatchID >= 2 {
			t.Fatalf("record for batch %d exists, want none from the mismatched batch", r.BatchID)
		}
	}
}

func TestAlignedBoundaryUnreachableHalts(t *testing.T) {
	nat := naturalPerMomentum(t)
	src := &chainSource{frontier: 14, moms: variedChain(15)}
	cfg, cur, _ := resyncConfig(t, src, nat, 0)
	// The input right after batch 1's last is another domain's, so no batch of this domain ends there.
	cur.entries[1].LastInputSeq = nat[1].LastInputSeq + 1
	l := startRun(t, cfg, src, scriptDomain{})

	err := l.exit()
	if !errors.Is(err, recovery.ErrHalted) || !errors.Is(err, batch.ErrMisaligned) {
		t.Fatalf("Run = %v, want ErrHalted wrapping ErrMisaligned", err)
	}
}

func TestAlignedBatchWaitsForItsLastInput(t *testing.T) {
	nat := naturalPaired(t)
	chain := variedChain(15)
	src := &chainSource{frontier: 5, moms: chain}
	cfg, _, log := resyncConfig(t, src, nat, 0)
	l := startRun(t, cfg, src, scriptDomain{})

	waitNextBatchID(t, cfg, 1)
	holdsNextBatchID(t, cfg, 1)
	select {
	case <-l.done:
		t.Fatalf("Run returned while batch 1 waited for its last input: %v", l.err)
	default:
	}

	src.set(chain, 6)
	waitNextBatchID(t, cfg, 2)
	if err := l.stop(); err != nil {
		t.Fatalf("Run: %v", err)
	}
	assertCommitments(t, log.flat(), nat)
	if got := log.flat(); len(got) != 2 {
		t.Fatalf("%d commitments, want 2", len(got))
	}
}

func TestAlignmentFollowsARisingNextBatchID(t *testing.T) {
	nat := naturalPerMomentum(t)
	src := &chainSource{frontier: 14, moms: variedChain(15)}
	cfg, cur, log := resyncConfig(t, src, nat, 0)
	cur.set(2)
	cur.onAccepted = func(from uint64) {
		if from == 2 {
			cur.set(uint64(len(nat)))
		}
	}
	l := startRun(t, cfg, src, scriptDomain{})

	waitNextBatchID(t, cfg, uint64(len(nat)))
	if err := l.stop(); err != nil {
		t.Fatalf("Run: %v", err)
	}
	got := log.flat()
	if len(got) != len(nat) {
		t.Fatalf("%d commitments, want %d", len(got), len(nat))
	}
	assertCommitments(t, got, nat)
}

func TestAcceptedSourceErrors(t *testing.T) {
	run := func(t *testing.T, configure func(cur *fakeCursor)) error {
		src := &chainSource{frontier: 7, moms: buildChain(t, 5, 0)}
		cfg := validConfig(t, src)
		cur := &fakeCursor{}
		configure(cur)
		cfg.Cursor = cur
		return startRun(t, cfg, src, scriptDomain{}).exit()
	}

	t.Run("halted", func(t *testing.T) {
		err := run(t, func(cur *fakeCursor) { cur.acceptedErr = fmt.Errorf("%w: bad", recovery.ErrHalted) })
		if !errors.Is(err, recovery.ErrHalted) {
			t.Fatalf("Run = %v, want ErrHalted", err)
		}
	})

	t.Run("unavailable", func(t *testing.T) {
		err := run(t, func(cur *fakeCursor) { cur.acceptedErr = errors.New("node down") })
		if !errors.Is(err, recovery.ErrSourceUnavailable) || errors.Is(err, recovery.ErrHalted) {
			t.Fatalf("Run = %v, want ErrSourceUnavailable", err)
		}
	})

	t.Run("no entry below nextBatchId", func(t *testing.T) {
		err := run(t, func(cur *fakeCursor) { cur.next = 5 })
		if !errors.Is(err, recovery.ErrSourceUnavailable) || errors.Is(err, recovery.ErrHalted) {
			t.Fatalf("Run = %v, want ErrSourceUnavailable", err)
		}
	})
}

func TestCommitBoundaryAlignSuppressesFires(t *testing.T) {
	lowerCommitInterval(t, 3)

	next := func(align bool) string {
		f, err := ledger.New(ledger.Config{
			DomainID: 1, ConfirmationDepth: 2, StartHeight: 1,
			Source: streamSource{perRead: 1}, Assignments: streamSource{perRead: 1},
		})
		if err != nil {
			t.Fatal(err)
		}
		b := &commitBoundary{follower: f}
		for i := 0; i < 3; i++ {
			if _, ok, err := b.Next(); err != nil || !ok {
				t.Fatalf("Next %d: ok=%v err=%v", i, ok, err)
			}
		}
		b.align = align
		in, ok, err := b.Next()
		if err != nil {
			t.Fatalf("Next: %v", err)
		}
		if !ok {
			return "drained"
		}
		return fmt.Sprint(in.Ctx.GlobalInputIndex)
	}

	if got := next(false); got != "drained" {
		t.Fatalf("unaligned Next at a checkpoint past the interval = %s, want drained", got)
	}
	if got := next(true); got != "6" {
		t.Fatalf("aligned Next at the same checkpoint = %s, want input 6", got)
	}
}

// evenOdd is the chain of one domain input per momentum, 41 momenta.
func evenOddChain() []*nom.DetailedMomentum { return buildShape(1, 41, zeroSalt, dense) }

// naturalEvenOdd builds batch 0 from height 1, then restarts on the same directory and catches up
// over heights 2..41 with the commit interval at 8: batches 1..5 end at ordinals 8, 16, 24, 32, 40.
func naturalEvenOdd(t *testing.T) []codec.BatchCommitment {
	t.Helper()
	chain := evenOddChain()
	src := &chainSource{}
	cfg := validConfig(t, src)
	var log commitLog
	log.attach(&cfg)

	l := startRun(t, cfg, src, scriptDomain{})
	l.step(chain, 3)
	waitNextBatchID(t, cfg, 1)
	if err := l.stop(); err != nil {
		t.Fatalf("natural run 1: %v", err)
	}

	src.set(chain, 43)
	l = startRun(t, cfg, src, scriptDomain{})
	waitNextBatchID(t, cfg, 6)
	if err := l.stop(); err != nil {
		t.Fatalf("natural run 2: %v", err)
	}
	got := log.flat()
	if len(got) != 6 {
		t.Fatalf("natural history has %d batches, want 6", len(got))
	}
	return got
}

func TestAlignedResyncCommitsAtEachMomentumEnd(t *testing.T) {
	lowerCommitInterval(t, 8)
	nat := naturalEvenOdd(t)

	src := &chainSource{frontier: 43, moms: evenOddChain()}
	cfg, _, log := resyncConfig(t, src, nat, 0)
	l := startRun(t, cfg, src, scriptDomain{})
	waitNextBatchID(t, cfg, uint64(len(nat)))
	log.waitCalls(t, len(nat))
	if err := l.stop(); err != nil {
		t.Fatalf("Run: %v", err)
	}

	calls := log.snapshot()
	if len(calls) != len(nat) {
		t.Fatalf("%d OnCommit calls, want %d", len(calls), len(nat))
	}
	for i, call := range calls {
		if len(call) != 1 || call[0].BatchID != uint64(i) {
			t.Fatalf("call %d carries batches %v, want exactly batch %d", i, callIDs(call), i)
		}
		if !reflect.DeepEqual(call[0], nat[i]) {
			t.Fatalf("call %d commitment = %+v, want %+v", i, call[0], nat[i])
		}
	}
}

func TestAlignedResyncResumesFromItsLastCommit(t *testing.T) {
	lowerCommitInterval(t, 8)
	nat := naturalEvenOdd(t)
	chain := evenOddChain()

	src := &chainSource{frontier: 22, moms: chain}
	cfg, _, _ := resyncConfig(t, src, nat, 0)
	l := startRun(t, cfg, src, scriptDomain{})
	waitNextBatchID(t, cfg, 3)
	holdsNextBatchID(t, cfg, 3)
	if err := l.stop(); err != nil {
		t.Fatalf("Run: %v", err)
	}

	src.set(chain, 43)
	cfg2 := cfg
	cfg2.Cursor = &fakeCursor{next: uint64(len(nat)), entries: entriesOf(nat)}
	var log commitLog
	log.attach(&cfg2)
	var recoveredNext []uint64
	var mu sync.Mutex
	cfg2.OnRecover = func(next uint64, _ []codec.BatchCommitment, _ Withdrawals) error {
		mu.Lock()
		defer mu.Unlock()
		recoveredNext = append(recoveredNext, next)
		return nil
	}
	l = startRun(t, cfg2, src, scriptDomain{})
	waitNextBatchID(t, cfg2, uint64(len(nat)))
	log.waitCalls(t, 1)
	if err := l.stop(); err != nil {
		t.Fatalf("restarted Run: %v", err)
	}

	mu.Lock()
	if len(recoveredNext) == 0 || recoveredNext[0] != 3 {
		t.Fatalf("OnRecover nextBatchID = %v, want it to start at 3", recoveredNext)
	}
	mu.Unlock()
	if !src.requestedFrom(18) || src.requestedFrom(1) {
		t.Fatalf("restart read heights %v, want a read from 18 and none from 1", src.requests)
	}
	got := log.flat()
	if len(got) == 0 || got[0].BatchID != 3 {
		t.Fatalf("first reported batch = %v, want batch 3", callIDs(got))
	}
	assertCommitments(t, got, nat[3:])
}

func TestReadAheadLimitClearsAtTheCrossing(t *testing.T) {
	chain := buildShape(1, 131, zeroSalt, dense)
	nsrc := &chainSource{}
	ncfg := validConfig(t, nsrc)
	var nlog commitLog
	nlog.attach(&ncfg)
	nl := startRun(t, ncfg, nsrc, scriptDomain{})
	nl.step(chain, 3)
	waitNextBatchID(t, ncfg, 1)
	if err := nl.stop(); err != nil {
		t.Fatalf("natural run: %v", err)
	}
	nat := nlog.flat()
	if len(nat) != 1 {
		t.Fatalf("natural run made %d batches, want 1", len(nat))
	}

	src := &chainSource{frontier: 133, moms: chain}
	cfg, _, log := resyncConfig(t, src, nat, 0)
	l := startRun(t, cfg, src, scriptDomain{})
	waitNextBatchID(t, cfg, 4)
	log.waitCalls(t, 2)
	if err := l.stop(); err != nil {
		t.Fatalf("Run: %v", err)
	}

	calls := log.snapshot()
	if len(calls) != 2 || !reflect.DeepEqual(callIDs(calls[0]), []uint64{0}) || !reflect.DeepEqual(callIDs(calls[1]), []uint64{1, 2, 3}) {
		t.Fatalf("OnCommit calls carry batches %v, want [0] then [1 2 3]", [][]uint64{callIDs(calls[0]), callIDs(calls[len(calls)-1])})
	}
	if !reflect.DeepEqual(calls[0][0], nat[0]) {
		t.Fatalf("batch 0 = %+v, want %+v", calls[0][0], nat[0])
	}
}

// hundred is the chain of 100 domain inputs at every one of 6 heights.
func hundredChain(salt byte) []*nom.DetailedMomentum {
	return buildShape(1, 6, func(uint64) byte { return salt }, func(uint64) int { return 100 })
}

// naturalHundred confirms all 6 heights at once with the commit interval at 200: each refill buffers
// two momenta, three batches close at 64 inputs and the fire at the checkpoint closes the 8 left.
func naturalHundred(t *testing.T) []codec.BatchCommitment {
	t.Helper()
	src := &chainSource{frontier: 8, moms: hundredChain(0)}
	cfg := validConfig(t, src)
	var log commitLog
	log.attach(&cfg)
	l := startRun(t, cfg, src, scriptDomain{})
	waitNextBatchID(t, cfg, 12)
	log.waitCalls(t, 3)
	if err := l.stop(); err != nil {
		t.Fatalf("natural run: %v", err)
	}
	calls := log.snapshot()
	want := [][]uint64{{0, 1, 2, 3}, {4, 5, 6, 7}, {8, 9, 10, 11}}
	var got [][]uint64
	for _, c := range calls {
		got = append(got, callIDs(c))
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("natural OnCommit calls carry %v, want %v", got, want)
	}
	return log.flat()
}

func TestAlignedResyncCommitsBatchesEndingInsideMomenta(t *testing.T) {
	lowerCommitInterval(t, 200)
	nat := naturalHundred(t)

	src := &chainSource{frontier: 8, moms: hundredChain(0)}
	cfg, cur, log := resyncConfig(t, src, nat, 0)
	cfg.MaxPendingBatches = 1
	l := startRun(t, cfg, src, scriptDomain{})
	waitNextBatchID(t, cfg, uint64(len(nat)))
	log.waitCalls(t, 3)
	if err := l.stop(); err != nil {
		t.Fatalf("Run: %v", err)
	}

	var got [][]uint64
	for _, c := range log.snapshot() {
		got = append(got, callIDs(c))
	}
	if want := [][]uint64{{0, 1, 2, 3}, {4, 5, 6, 7}, {8, 9, 10, 11}}; !reflect.DeepEqual(got, want) {
		t.Fatalf("OnCommit calls carry %v, want %v", got, want)
	}
	assertCommitments(t, log.flat(), nat)
	if n := cur.readCalls(); n != 0 {
		t.Fatalf("the gate read the cursor %d times, want 0: an aligned re-sync takes nextBatchId from the accepted-entry reply", n)
	}
}

func TestAlignedReorgRefetches(t *testing.T) {
	lowerCommitInterval(t, 200)
	nat := naturalHundred(t)

	src := &chainSource{frontier: 3, moms: hundredChain(0)}
	cfg, _, log := resyncConfig(t, src, nat, 0)
	l := startRun(t, cfg, src, scriptDomain{})
	l.waitFor("the first ranged read", func() bool { return src.returnedTo() >= 1 })
	l.settle()
	if _, ok := cursorAt(t, cfg); ok {
		t.Fatalf("a cursor exists before any commit")
	}

	src.set(buildShape(1, 6, saltFrom(1, 1), func(uint64) int { return 100 }), 8)
	waitNextBatchID(t, cfg, uint64(len(nat)))
	log.waitCalls(t, 3)
	if err := l.stop(); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if !src.requestedFrom(1) {
		t.Fatalf("no read from height 1 after the fork, want a rollback to genesis")
	}
	got := log.flat()
	if len(got) != len(nat) {
		t.Fatalf("%d commitments, want %d", len(got), len(nat))
	}
	assertCommitments(t, got, nat)
}

func TestFreshDomainFetchesAcceptedEntriesOncePerBoot(t *testing.T) {
	chain := variedChain(15)
	src := &chainSource{}
	cfg := validConfig(t, src)
	cur := &fakeCursor{}
	cfg.Cursor = cur
	l := startRun(t, cfg, src, scriptDomain{})
	for h := uint64(1); h <= 4; h++ {
		l.step(chain, h+2)
	}
	if err := l.stop(); err != nil {
		t.Fatalf("Run: %v", err)
	}
	cur.mu.Lock()
	defer cur.mu.Unlock()
	if cur.acceptedCalls != 1 {
		t.Fatalf("AcceptedEntries called %d times over four batches above an empty chain, want 1", cur.acceptedCalls)
	}
	if cur.calls != 0 {
		t.Fatalf("NextBatchID called %d times, want 0", cur.calls)
	}
}

// TestAlignedCrossingWithUncommittedBatches ends the accepted history on a batch that ends inside a
// momentum, so the crossing happens with accepted batches still pending.
func TestAlignedCrossingWithUncommittedBatches(t *testing.T) {
	lowerCommitInterval(t, 200)
	nat := naturalHundred(t)

	src := &chainSource{frontier: 8, moms: hundredChain(0)}
	cfg, cur, log := resyncConfig(t, src, nat, 0)
	cur.next = 3
	l := startRun(t, cfg, src, scriptDomain{})
	l.waitFor("batch 12", func() bool { return nextBatchIDOf(t, cfg) == uint64(len(nat)) })
	log.waitCalls(t, 3)
	if err := l.stop(); err != nil {
		t.Fatalf("Run: %v", err)
	}
	got := log.flat()
	if len(got) != len(nat) {
		t.Fatalf("%d commitments, want %d", len(got), len(nat))
	}
	assertCommitments(t, got, nat)
}
