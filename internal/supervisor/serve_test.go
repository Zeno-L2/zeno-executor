package supervisor

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/zenon-network/go-zenon/chain/nom"
	"github.com/zenon-network/go-zenon/common/types"
	"github.com/zenon-network/go-zenon/vm/embedded/definition"

	"github.com/digitalSloth/go-executor/internal/batch"
	"github.com/digitalSloth/go-executor/internal/codec"
	"github.com/digitalSloth/go-executor/internal/ipc"
	"github.com/digitalSloth/go-executor/internal/ipc/ipctest"
	"github.com/digitalSloth/go-executor/internal/ledger"
	"github.com/digitalSloth/go-executor/internal/recovery"
	"github.com/digitalSloth/go-executor/internal/settlement"
	"github.com/digitalSloth/go-executor/internal/workerproc"
	"github.com/digitalSloth/go-executor/internal/znnd"
)

// --- fake sources: structs of funcs, one goroutine at a time per internal/ipc/client_test.go:16-33 ---

type fakeMomenta struct {
	frontier func() (uint64, error)
	byHeight func(height, count uint64) ([]*nom.DetailedMomentum, error)
}

func (f fakeMomenta) FrontierHeight() (uint64, error) { return f.frontier() }
func (f fakeMomenta) DetailedMomentumsByHeight(height, count uint64) ([]*nom.DetailedMomentum, error) {
	return f.byHeight(height, count)
}

// fakeAnchors serves Settled from anchor and next, NextBatchID from next, AcceptedEntry from
// entry, AcceptedEntries from accepted, Tip from tip and Gates from gates. A nil next reads as
// nextBatchId 0. A nil accepted builds its reply from next and entry. A nil entry, tip or gates
// reads as an unavailable source.
type fakeAnchors struct {
	anchor func(domainID uint32) (recovery.Anchor, bool, error)
	next   func(domainID uint32) (uint64, error)
	entry  func(domainID uint32, batchID uint64) (*definition.BatchEntry, error)
	// accepted, when set, answers AcceptedEntries.
	accepted func(domainID uint32, from uint64, max int) (settlement.Accepted, error)
	tip      func(domainID uint32) (settlement.Tip, error)
	gates    func(domainID uint32, pillarName string) (settlement.Gates, error)
	// relay, when set, answers Relay; a nil relay reads as an idle domain.
	relay func(domainID uint32, q *settlement.RelayQuery) (settlement.RelayState, error)
}

func (f fakeAnchors) Relay(domainID uint32, q *settlement.RelayQuery) (settlement.RelayState, error) {
	if f.relay == nil {
		return settlement.RelayState{}, nil
	}
	return f.relay(domainID, q)
}

func (f fakeAnchors) Gates(domainID uint32, pillarName string) (settlement.Gates, error) {
	if f.gates == nil {
		return settlement.Gates{}, errors.New("fake: no gates source")
	}
	return f.gates(domainID, pillarName)
}

func (f fakeAnchors) Tip(domainID uint32) (settlement.Tip, error) {
	if f.tip == nil {
		return settlement.Tip{}, errors.New("fake: no tip source")
	}
	return f.tip(domainID)
}

func (f fakeAnchors) Settled(domainID uint32) (settlement.Settled, error) {
	a, ok, err := f.anchor(domainID)
	if err != nil {
		return settlement.Settled{}, err
	}
	n, err := f.NextBatchID(domainID)
	if err != nil {
		return settlement.Settled{}, err
	}
	return settlement.Settled{Anchor: a, HaveAnchor: ok, NextBatchID: n}, nil
}

func (f fakeAnchors) NextBatchID(domainID uint32) (uint64, error) {
	if f.next == nil {
		return 0, nil
	}
	return f.next(domainID)
}

func (f fakeAnchors) AcceptedEntries(domainID uint32, from uint64, max int) (settlement.Accepted, error) {
	if f.accepted != nil {
		return f.accepted(domainID, from, max)
	}
	n, err := f.NextBatchID(domainID)
	if err != nil {
		return settlement.Accepted{}, err
	}
	out := settlement.Accepted{NextBatchID: n}
	for b := from; b < n && len(out.Entries) < max; b++ {
		e, err := f.AcceptedEntry(domainID, b)
		if err != nil {
			return settlement.Accepted{}, err
		}
		out.Entries = append(out.Entries, e)
	}
	return out, nil
}

func (f fakeAnchors) AcceptedEntry(domainID uint32, batchID uint64) (*definition.BatchEntry, error) {
	if f.entry == nil {
		return nil, errors.New("fake: no accepted entry source")
	}
	return f.entry(domainID, batchID)
}

type fakeAssignments struct {
	assignment func(domainID uint32, height uint64, hash types.Hash, nextOrdinal uint64) ([]ledger.AssignmentEntry, error)
}

func (f fakeAssignments) Assignment(domainID uint32, height uint64, hash types.Hash, nextOrdinal uint64) ([]ledger.AssignmentEntry, error) {
	return f.assignment(domainID, height, hash, nextOrdinal)
}

type fakeCaps struct {
	cap func(domainID uint32, height uint64, hash types.Hash, assets []codec.AssetID) ([]codec.U256, error)
}

func (f fakeCaps) MaxBatchWithdrawals(domainID uint32, height uint64, hash types.Hash, assets []codec.AssetID) ([]codec.U256, error) {
	return f.cap(domainID, height, hash, assets)
}

// --- harness ---

func newChannel(t *testing.T) (sEnd, wEnd io.ReadWriteCloser) {
	t.Helper()
	s, w, err := workerproc.Socketpair()
	if err != nil {
		t.Fatalf("Socketpair: %v", err)
	}
	t.Cleanup(func() {
		s.Close()
		w.Close()
	})
	return s, w
}

// runServe starts serve on its own goroutine and reports its result on the returned channel.
func runServe(t *testing.T, conn io.ReadWriter, w WorkerConfig, executor types.Address, src Sources, st *workerState) <-chan error {
	t.Helper()
	done := make(chan error, 1)
	go func() { done <- serve(conn, w, executor, src, st) }()
	return done
}

// waitServe waits for serve to return, with a timeout so a hung serve fails the test instead of
// hanging it.
func waitServe(t *testing.T, done <-chan error) error {
	t.Helper()
	select {
	case err := <-done:
		return err
	case <-time.After(5 * time.Second):
		t.Fatalf("serve did not return in time")
		return nil
	}
}

func rawFrame(typ byte, payload []byte) []byte {
	length := uint32(1 + len(payload))
	buf := make([]byte, 0, 4+length)
	buf = binary.BigEndian.AppendUint32(buf, length)
	buf = append(buf, typ)
	buf = append(buf, payload...)
	return buf
}

func rawU64Pair(a, b uint64) []byte {
	out := make([]byte, 16)
	binary.BigEndian.PutUint64(out[0:8], a)
	binary.BigEndian.PutUint64(out[8:16], b)
	return out
}

func lastFrameOf(st *workerState) time.Time {
	st.mu.Lock()
	defer st.mu.Unlock()
	return st.lastFrame
}

func servedHeightOf(st *workerState) uint64 {
	st.mu.Lock()
	defer st.mu.Unlock()
	return st.servedHeight
}

// commitmentFor builds a valid codec.BatchCommitment (internal/ipc/messages_test.go:15-43's
// validCommitment, with DomainID, ExecutorID and SubmittedAtHeight set for this package's tests).
func commitmentFor(domainID uint32, executor types.Address, batchID, lastInputSeq uint64) codec.BatchCommitment {
	return codec.BatchCommitment{
		Versions: codec.Versions{
			ProtocolVersion:        codec.ProtocolVersion,
			RuntimeProfileVersion:  codec.RuntimeProfileVersion,
			MeteringVersion:        codec.MeteringVersion,
			SerializationVersion:   codec.SerializationVersion,
			StateDiffVersion:       codec.StateDiffVersion,
			ExecutionResultVersion: codec.ExecutionResultVersion,
			ReceiptVersion:         codec.ReceiptVersion,
			OutboxVersion:          codec.OutboxVersion,
			DAModeVersion:          codec.DAModeVersion,
		},
		DomainID:          domainID,
		BatchID:           batchID,
		FirstInputSeq:     1,
		LastInputSeq:      lastInputSeq,
		PreStateRoot:      types.Hash{0x01},
		PostStateRoot:     types.Hash{0x02},
		InputRoot:         types.Hash{0x03},
		ReceiptRoot:       types.Hash{0x04},
		EventRoot:         types.Hash{0x05},
		OutboxRoot:        types.Hash{0x06},
		DAHash:            types.Hash{0x07},
		DAMode:            1,
		ExecutorID:        executor,
		SubmittedAtHeight: 0,
		// codec.DecodeBatchCommitment always produces a non-nil, empty AssetFlowSummary and
		// ProofData for an empty encoding; matching that shape here keeps a round-tripped
		// Committed frame reflect.DeepEqual to this fixture.
		AssetFlowSummary: codec.AssetFlowSummary{},
		ProofData:        []byte{},
	}
}

// syncRoundTrip sends a FrontierRequest and reads its reply, giving a Committed frame just sent
// (which gets no reply) a happens-before point against serve's recording goroutine.
func syncRoundTrip(t *testing.T, wEnd io.ReadWriter) {
	t.Helper()
	if err := ipc.WriteMessage(wEnd, ipc.FrontierRequest{}); err != nil {
		t.Fatalf("WriteMessage FrontierRequest: %v", err)
	}
	if _, err := ipc.ReadMessage(wEnd, ipc.Supervisor); err != nil {
		t.Fatalf("ReadMessage: %v", err)
	}
}

// --- Frontier ---

func TestServeFrontier(t *testing.T) {
	sEnd, wEnd := newChannel(t)
	st := &workerState{maxHeld: heldCap(defaultMaxPendingBatches), withdrawals: testStore(t, 1)}
	src := Sources{Momenta: fakeMomenta{frontier: func() (uint64, error) { return 42, nil }}}
	done := runServe(t, sEnd, WorkerConfig{DomainID: 1}, types.Address{}, src, st)

	before := time.Now()
	if err := ipc.WriteMessage(wEnd, ipc.FrontierRequest{}); err != nil {
		t.Fatalf("WriteMessage: %v", err)
	}
	m, err := ipc.ReadMessage(wEnd, ipc.Supervisor)
	if err != nil {
		t.Fatalf("ReadMessage: %v", err)
	}
	f, ok := m.(ipc.Frontier)
	if !ok || f.Height != 42 {
		t.Fatalf("reply = %#v, want Frontier{42}", m)
	}
	if lf := lastFrameOf(st); lf.Before(before) {
		t.Fatalf("lastFrame = %v, want at or after %v", lf, before)
	}

	wEnd.Close()
	if err := waitServe(t, done); err != nil {
		t.Fatalf("serve = %v, want nil", err)
	}
}

// --- Anchor ---

func TestServeAnchor(t *testing.T) {
	t.Run("found", func(t *testing.T) {
		sEnd, wEnd := newChannel(t)
		st := &workerState{maxHeld: heldCap(defaultMaxPendingBatches), withdrawals: testStore(t, 1)}
		var gotDomain uint32
		src := Sources{Anchors: fakeAnchors{anchor: func(domainID uint32) (recovery.Anchor, bool, error) {
			gotDomain = domainID
			return recovery.Anchor{PostStateRoot: types.Hash{0x11}, LastInputSeq: 7}, true, nil
		}}}
		done := runServe(t, sEnd, WorkerConfig{DomainID: 9}, types.Address{}, src, st)

		if err := ipc.WriteMessage(wEnd, ipc.AnchorRequest{}); err != nil {
			t.Fatalf("WriteMessage: %v", err)
		}
		m, err := ipc.ReadMessage(wEnd, ipc.Supervisor)
		if err != nil {
			t.Fatalf("ReadMessage: %v", err)
		}
		want := ipc.Anchor{Found: true, PostStateRoot: types.Hash{0x11}, LastInputSeq: 7}
		if m != want {
			t.Fatalf("reply = %#v, want %#v", m, want)
		}
		if gotDomain != 9 {
			t.Fatalf("domainID passed to source = %d, want 9", gotDomain)
		}

		wEnd.Close()
		if err := waitServe(t, done); err != nil {
			t.Fatalf("serve = %v, want nil", err)
		}
	})

	t.Run("not found", func(t *testing.T) {
		sEnd, wEnd := newChannel(t)
		st := &workerState{maxHeld: heldCap(defaultMaxPendingBatches), withdrawals: testStore(t, 1)}
		src := Sources{Anchors: fakeAnchors{anchor: func(domainID uint32) (recovery.Anchor, bool, error) {
			// A non-zero anchor from the source with found == false; the reply must be the
			// zero anchor, since ipc.Encode rejects a not-found anchor with a non-zero root
			// or seq.
			return recovery.Anchor{PostStateRoot: types.Hash{0x22}, LastInputSeq: 3}, false, nil
		}}}
		done := runServe(t, sEnd, WorkerConfig{DomainID: 1}, types.Address{}, src, st)

		if err := ipc.WriteMessage(wEnd, ipc.AnchorRequest{}); err != nil {
			t.Fatalf("WriteMessage: %v", err)
		}
		m, err := ipc.ReadMessage(wEnd, ipc.Supervisor)
		if err != nil {
			t.Fatalf("ReadMessage: %v", err)
		}
		if m != (ipc.Anchor{}) {
			t.Fatalf("reply = %#v, want the zero anchor", m)
		}

		wEnd.Close()
		if err := waitServe(t, done); err != nil {
			t.Fatalf("serve = %v, want nil", err)
		}
	})
}

func TestServeAnchorPrunesBelowNextBatchID(t *testing.T) {
	sEnd, wEnd := newChannel(t)
	st := &workerState{maxHeld: heldCap(defaultMaxPendingBatches), withdrawals: testStore(t, 1)}
	w := WorkerConfig{DomainID: 1}
	exec := types.Address{0x09}
	src := Sources{Anchors: fakeAnchors{anchor: func(uint32) (recovery.Anchor, bool, error) {
		return recovery.Anchor{LastInputSeq: 6}, true, nil
	}, next: func(uint32) (uint64, error) { return 3, nil }}}
	done := runServe(t, sEnd, w, exec, src, st)

	if err := ipc.WriteMessage(wEnd, ipc.Retract{FromBatchID: 5}); err != nil {
		t.Fatalf("WriteMessage: %v", err)
	}
	for id := uint64(1); id <= 4; id++ {
		writeHandOff(t, wEnd, ipc.Recovered{Commitment: commitmentFor(1, exec, id, id*3)})
	}
	if err := ipc.WriteMessage(wEnd, ipc.AnchorRequest{}); err != nil {
		t.Fatalf("WriteMessage: %v", err)
	}
	if _, err := ipc.ReadMessage(wEnd, ipc.Supervisor); err != nil {
		t.Fatalf("ReadMessage: %v", err)
	}

	st.mu.Lock()
	got := len(st.commitments)
	_, ok3 := st.commitments[3]
	_, ok2 := st.commitments[2]
	st.mu.Unlock()
	if got != 2 || !ok3 || ok2 {
		t.Fatalf("held %d commitments (batch 2 held: %v, batch 3 held: %v), want batches 3 and 4 only", got, ok2, ok3)
	}

	wEnd.Close()
	if err := waitServe(t, done); err != nil {
		t.Fatalf("serve = %v, want nil", err)
	}
}

// --- Momenta ---

func fiveMomentumChain() []*nom.DetailedMomentum {
	return ipctest.Chain([][]ipctest.Block{
		{{Address: types.Address{0x01}, Data: []byte("h1")}},
		{{Address: types.Address{0x01}, Data: []byte("h2")}},
		{{Address: types.Address{0x01}, Data: []byte("h3")}},
		{{Address: types.Address{0x01}, Data: []byte("h4")}},
		{{Address: types.Address{0x01}, Data: []byte("h5")}},
	})
}

func requestMomenta(t *testing.T, wEnd io.ReadWriter, height, count uint64) ([]*nom.DetailedMomentum, uint32) {
	t.Helper()
	if err := ipc.WriteMessage(wEnd, ipc.MomentaRequest{Height: height, Count: count}); err != nil {
		t.Fatalf("WriteMessage: %v", err)
	}
	var got []*nom.DetailedMomentum
	for {
		m, err := ipc.ReadMessage(wEnd, ipc.Supervisor)
		if err != nil {
			t.Fatalf("ReadMessage: %v", err)
		}
		switch v := m.(type) {
		case ipc.Momentum:
			got = append(got, v.Detailed)
		case ipc.RangeEnd:
			return got, v.Count
		default:
			t.Fatalf("unexpected reply %#v", m)
		}
	}
}

func TestServeMomentaRange(t *testing.T) {
	sEnd, wEnd := newChannel(t)
	st := &workerState{maxHeld: heldCap(defaultMaxPendingBatches), withdrawals: testStore(t, 1)}
	chain := fiveMomentumChain()
	src := Sources{Momenta: fakeMomenta{byHeight: func(height, count uint64) ([]*nom.DetailedMomentum, error) {
		start := int(height) - 1
		if start < 0 || start >= len(chain) {
			return nil, nil
		}
		end := start + int(count)
		if end > len(chain) {
			end = len(chain)
		}
		return chain[start:end], nil
	}}}
	done := runServe(t, sEnd, WorkerConfig{DomainID: 1}, types.Address{}, src, st)

	got, count := requestMomenta(t, wEnd, 2, 3)
	if count != 3 || len(got) != 3 {
		t.Fatalf("got %d momenta, RangeEnd %d, want 3 and 3", len(got), count)
	}
	for i, dm := range got {
		if dm.Momentum.Height != uint64(i+2) {
			t.Fatalf("momentum %d height = %d, want %d", i, dm.Momentum.Height, i+2)
		}
	}
	if sh := servedHeightOf(st); sh != 4 {
		t.Fatalf("servedHeight = %d, want 4", sh)
	}

	// A later, lower-height request (a reorg rollback) leaves servedHeight where it was.
	if _, count := requestMomenta(t, wEnd, 1, 1); count != 1 {
		t.Fatalf("RangeEnd = %d, want 1", count)
	}
	if sh := servedHeightOf(st); sh != 4 {
		t.Fatalf("servedHeight after a lower request = %d, want unchanged 4", sh)
	}

	wEnd.Close()
	if err := waitServe(t, done); err != nil {
		t.Fatalf("serve = %v, want nil", err)
	}
}

// limitedMomenta serves fiveMomentumChain and fails any read above limit as an over-limit
// response. serve calls it from another goroutine, so mu guards every field.
type limitedMomenta struct {
	mu     sync.Mutex
	chain  []*nom.DetailedMomentum
	limit  uint64
	counts []uint64
}

func newLimitedMomenta(limit uint64) *limitedMomenta {
	return &limitedMomenta{chain: fiveMomentumChain(), limit: limit}
}

func (l *limitedMomenta) byHeight(height, count uint64) ([]*nom.DetailedMomentum, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.counts = append(l.counts, count)
	if count > l.limit {
		return nil, fmt.Errorf("znnd: ledger.getDetailedMomentumsByHeight: %w", znnd.ErrResponseTooLarge)
	}
	start := int(height) - 1
	if start < 0 || start >= len(l.chain) {
		return nil, nil
	}
	end := min(start+int(count), len(l.chain))
	return l.chain[start:end], nil
}

func (l *limitedMomenta) setLimit(n uint64) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.limit = n
}

func (l *limitedMomenta) take() []uint64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	c := l.counts
	l.counts = nil
	return c
}

func checkMomentaRead(t *testing.T, step string, lm *limitedMomenta, wEnd io.ReadWriter, height, count uint64, wantHeights []uint64, wantCounts []uint64) {
	t.Helper()
	got, end := requestMomenta(t, wEnd, height, count)
	if int(end) != len(wantHeights) || len(got) != len(wantHeights) {
		t.Fatalf("%s: got %d momenta, RangeEnd %d, want %d", step, len(got), end, len(wantHeights))
	}
	for i, dm := range got {
		if dm.Momentum.Height != wantHeights[i] {
			t.Fatalf("%s: momentum %d height = %d, want %d", step, i, dm.Momentum.Height, wantHeights[i])
		}
	}
	if c := lm.take(); !reflect.DeepEqual(c, wantCounts) {
		t.Fatalf("%s: counts = %v, want %v", step, c, wantCounts)
	}
}

func TestServeMomentaHalvesOversizedRange(t *testing.T) {
	t.Run("halves until it fits", func(t *testing.T) {
		sEnd, wEnd := newChannel(t)
		st := &workerState{maxHeld: heldCap(defaultMaxPendingBatches), withdrawals: testStore(t, 1)}
		lm := newLimitedMomenta(2)
		src := Sources{Momenta: fakeMomenta{byHeight: lm.byHeight}}
		done := runServe(t, sEnd, WorkerConfig{DomainID: 1}, types.Address{}, src, st)

		checkMomentaRead(t, "read", lm, wEnd, 1, 5, []uint64{1, 2}, []uint64{5, 2})
		if sh := servedHeightOf(st); sh != 2 {
			t.Fatalf("servedHeight = %d, want 2", sh)
		}
		wEnd.Close()
		if err := waitServe(t, done); err != nil {
			t.Fatalf("serve = %v, want nil", err)
		}
	})

	t.Run("too large at count 1", func(t *testing.T) {
		sEnd, wEnd := newChannel(t)
		st := &workerState{maxHeld: heldCap(defaultMaxPendingBatches), withdrawals: testStore(t, 1)}
		lm := newLimitedMomenta(0)
		src := Sources{Momenta: fakeMomenta{byHeight: lm.byHeight}}
		done := runServe(t, sEnd, WorkerConfig{DomainID: 1}, types.Address{}, src, st)

		if err := ipc.WriteMessage(wEnd, ipc.MomentaRequest{Height: 1, Count: 5}); err != nil {
			t.Fatalf("WriteMessage: %v", err)
		}
		m, err := ipc.ReadMessage(wEnd, ipc.Supervisor)
		if err != nil {
			t.Fatalf("ReadMessage: %v", err)
		}
		se, ok := m.(ipc.SourceError)
		if !ok || se.Kind != ipc.SourceUnavailable {
			t.Fatalf("reply = %#v, want a SourceError of kind unavailable", m)
		}
		if c := lm.take(); !reflect.DeepEqual(c, []uint64{5, 2, 1}) {
			t.Fatalf("counts = %v, want [5 2 1]", c)
		}
		wEnd.Close()
		if err := waitServe(t, done); err != nil {
			t.Fatalf("serve = %v, want nil", err)
		}
	})
}

func TestServeMomentaCountGrowsBack(t *testing.T) {
	sEnd, wEnd := newChannel(t)
	st := &workerState{maxHeld: heldCap(defaultMaxPendingBatches), withdrawals: testStore(t, 1)}
	lm := newLimitedMomenta(1)
	src := Sources{Momenta: fakeMomenta{byHeight: lm.byHeight}}
	done := runServe(t, sEnd, WorkerConfig{DomainID: 1}, types.Address{}, src, st)

	checkMomentaRead(t, "step 1", lm, wEnd, 1, 5, []uint64{1}, []uint64{5, 2, 1})

	lm.setLimit(5)
	checkMomentaRead(t, "step 2", lm, wEnd, 3, 1, []uint64{3}, []uint64{1})
	checkMomentaRead(t, "step 3", lm, wEnd, 1, 5, []uint64{1, 2}, []uint64{2})

	wEnd.Close()
	if err := waitServe(t, done); err != nil {
		t.Fatalf("serve = %v, want nil", err)
	}

	// A restarted worker reuses the same workerState.
	sEnd2, wEnd2 := newChannel(t)
	done2 := runServe(t, sEnd2, WorkerConfig{DomainID: 1}, types.Address{}, src, st)
	checkMomentaRead(t, "step 4", lm, wEnd2, 1, 5, []uint64{1, 2, 3, 4}, []uint64{4})
	for i := 0; i < 2; i++ {
		checkMomentaRead(t, "step 5", lm, wEnd2, 1, 5, []uint64{1, 2, 3, 4, 5}, []uint64{5})
	}
	wEnd2.Close()
	if err := waitServe(t, done2); err != nil {
		t.Fatalf("serve = %v, want nil", err)
	}
}

func TestServeMomentaTrimsToCount(t *testing.T) {
	sEnd, wEnd := newChannel(t)
	st := &workerState{maxHeld: heldCap(defaultMaxPendingBatches), withdrawals: testStore(t, 1)}
	chain := fiveMomentumChain()
	src := Sources{Momenta: fakeMomenta{byHeight: func(height, count uint64) ([]*nom.DetailedMomentum, error) {
		return chain, nil // the source ignores count and always returns all five
	}}}
	done := runServe(t, sEnd, WorkerConfig{DomainID: 1}, types.Address{}, src, st)

	got, count := requestMomenta(t, wEnd, 1, 2)
	if count != 2 || len(got) != 2 {
		t.Fatalf("got %d momenta, RangeEnd %d, want 2 and 2", len(got), count)
	}

	wEnd.Close()
	if err := waitServe(t, done); err != nil {
		t.Fatalf("serve = %v, want nil", err)
	}
}

func TestServeMomentaZero(t *testing.T) {
	sEnd, wEnd := newChannel(t)
	st := &workerState{maxHeld: heldCap(defaultMaxPendingBatches), withdrawals: testStore(t, 1)}
	src := Sources{Momenta: fakeMomenta{byHeight: func(height, count uint64) ([]*nom.DetailedMomentum, error) {
		return nil, nil
	}}}
	done := runServe(t, sEnd, WorkerConfig{DomainID: 1}, types.Address{}, src, st)

	got, count := requestMomenta(t, wEnd, 1, 5)
	if count != 0 || len(got) != 0 {
		t.Fatalf("got %d momenta, RangeEnd %d, want 0 and 0", len(got), count)
	}
	if sh := servedHeightOf(st); sh != 0 {
		t.Fatalf("servedHeight = %d, want 0", sh)
	}

	wEnd.Close()
	if err := waitServe(t, done); err != nil {
		t.Fatalf("serve = %v, want nil", err)
	}
}

// --- source-error mapping ---

func TestServeSourceErrorMapping(t *testing.T) {
	cases := []struct {
		name string
		err  error
		kind ipc.SourceErrorKind
	}{
		{"wraps ErrHalted", fmt.Errorf("source: %w", recovery.ErrHalted), ipc.SourceHalted},
		{"plain error", errors.New("upstream unavailable"), ipc.SourceUnavailable},
	}

	for _, tc := range cases {
		t.Run(tc.name+"/frontier", func(t *testing.T) {
			sEnd, wEnd := newChannel(t)
			st := &workerState{maxHeld: heldCap(defaultMaxPendingBatches), withdrawals: testStore(t, 1)}
			src := Sources{Momenta: fakeMomenta{frontier: func() (uint64, error) { return 0, tc.err }}}
			done := runServe(t, sEnd, WorkerConfig{DomainID: 1}, types.Address{}, src, st)

			if err := ipc.WriteMessage(wEnd, ipc.FrontierRequest{}); err != nil {
				t.Fatalf("WriteMessage: %v", err)
			}
			m, err := ipc.ReadMessage(wEnd, ipc.Supervisor)
			if err != nil {
				t.Fatalf("ReadMessage: %v", err)
			}
			se, ok := m.(ipc.SourceError)
			if !ok || se.Kind != tc.kind {
				t.Fatalf("reply = %#v, want SourceError kind %d", m, tc.kind)
			}

			wEnd.Close()
			waitServe(t, done)
		})

		t.Run(tc.name+"/anchor", func(t *testing.T) {
			sEnd, wEnd := newChannel(t)
			st := &workerState{maxHeld: heldCap(defaultMaxPendingBatches), withdrawals: testStore(t, 1)}
			src := Sources{Anchors: fakeAnchors{anchor: func(uint32) (recovery.Anchor, bool, error) {
				return recovery.Anchor{}, false, tc.err
			}}}
			done := runServe(t, sEnd, WorkerConfig{DomainID: 1}, types.Address{}, src, st)

			if err := ipc.WriteMessage(wEnd, ipc.AnchorRequest{}); err != nil {
				t.Fatalf("WriteMessage: %v", err)
			}
			m, err := ipc.ReadMessage(wEnd, ipc.Supervisor)
			if err != nil {
				t.Fatalf("ReadMessage: %v", err)
			}
			se, ok := m.(ipc.SourceError)
			if !ok || se.Kind != tc.kind {
				t.Fatalf("reply = %#v, want SourceError kind %d", m, tc.kind)
			}

			wEnd.Close()
			waitServe(t, done)
		})

		t.Run(tc.name+"/momenta", func(t *testing.T) {
			sEnd, wEnd := newChannel(t)
			st := &workerState{maxHeld: heldCap(defaultMaxPendingBatches), withdrawals: testStore(t, 1)}
			src := Sources{Momenta: fakeMomenta{byHeight: func(uint64, uint64) ([]*nom.DetailedMomentum, error) {
				return nil, tc.err
			}}}
			done := runServe(t, sEnd, WorkerConfig{DomainID: 1}, types.Address{}, src, st)

			if err := ipc.WriteMessage(wEnd, ipc.MomentaRequest{Height: 1, Count: 1}); err != nil {
				t.Fatalf("WriteMessage: %v", err)
			}
			m, err := ipc.ReadMessage(wEnd, ipc.Supervisor)
			if err != nil {
				t.Fatalf("ReadMessage: %v", err)
			}
			se, ok := m.(ipc.SourceError)
			if !ok || se.Kind != tc.kind {
				t.Fatalf("reply = %#v, want SourceError kind %d", m, tc.kind)
			}

			wEnd.Close()
			waitServe(t, done)
		})
	}
}

func TestServeSourceErrorMessageCut(t *testing.T) {
	sEnd, wEnd := newChannel(t)
	st := &workerState{maxHeld: heldCap(defaultMaxPendingBatches), withdrawals: testStore(t, 1)}
	long := strings.Repeat("x", 5000)
	src := Sources{Momenta: fakeMomenta{frontier: func() (uint64, error) { return 0, errors.New(long) }}}
	done := runServe(t, sEnd, WorkerConfig{DomainID: 1}, types.Address{}, src, st)

	if err := ipc.WriteMessage(wEnd, ipc.FrontierRequest{}); err != nil {
		t.Fatalf("WriteMessage: %v", err)
	}
	m, err := ipc.ReadMessage(wEnd, ipc.Supervisor)
	if err != nil {
		t.Fatalf("ReadMessage: %v", err)
	}
	se, ok := m.(ipc.SourceError)
	if !ok {
		t.Fatalf("reply = %#v, want SourceError", m)
	}
	if len(se.Message) != ipc.MaxSourceErrorLen {
		t.Fatalf("message length = %d, want %d", len(se.Message), ipc.MaxSourceErrorLen)
	}

	wEnd.Close()
	waitServe(t, done)
}

// --- momenta the channel cannot carry ---

func TestServeMomentaNilEntry(t *testing.T) {
	chain := fiveMomentumChain()

	t.Run("nil DetailedMomentum", func(t *testing.T) {
		sEnd, wEnd := newChannel(t)
		st := &workerState{maxHeld: heldCap(defaultMaxPendingBatches), withdrawals: testStore(t, 1)}
		ms := []*nom.DetailedMomentum{chain[0], nil, chain[1]}
		src := Sources{Momenta: fakeMomenta{byHeight: func(uint64, uint64) ([]*nom.DetailedMomentum, error) {
			return ms, nil
		}}}
		done := runServe(t, sEnd, WorkerConfig{DomainID: 1}, types.Address{}, src, st)

		if err := ipc.WriteMessage(wEnd, ipc.MomentaRequest{Height: 1, Count: 3}); err != nil {
			t.Fatalf("WriteMessage: %v", err)
		}
		m, err := ipc.ReadMessage(wEnd, ipc.Supervisor)
		if err != nil {
			t.Fatalf("ReadMessage: %v", err)
		}
		se, ok := m.(ipc.SourceError)
		if !ok || se.Kind != ipc.SourceHalted {
			t.Fatalf("reply = %#v, want SourceError halted", m)
		}
		if sh := servedHeightOf(st); sh != 0 {
			t.Fatalf("servedHeight = %d, want unchanged 0", sh)
		}

		wEnd.Close()
		waitServe(t, done)
	})

	t.Run("nil account block", func(t *testing.T) {
		sEnd, wEnd := newChannel(t)
		st := &workerState{maxHeld: heldCap(defaultMaxPendingBatches), withdrawals: testStore(t, 1)}
		bad := *chain[1]
		bad.AccountBlocks = []*nom.AccountBlock{nil}
		ms := []*nom.DetailedMomentum{chain[0], &bad}
		src := Sources{Momenta: fakeMomenta{byHeight: func(uint64, uint64) ([]*nom.DetailedMomentum, error) {
			return ms, nil
		}}}
		done := runServe(t, sEnd, WorkerConfig{DomainID: 1}, types.Address{}, src, st)

		if err := ipc.WriteMessage(wEnd, ipc.MomentaRequest{Height: 1, Count: 2}); err != nil {
			t.Fatalf("WriteMessage: %v", err)
		}
		m, err := ipc.ReadMessage(wEnd, ipc.Supervisor)
		if err != nil {
			t.Fatalf("ReadMessage: %v", err)
		}
		se, ok := m.(ipc.SourceError)
		if !ok || se.Kind != ipc.SourceHalted {
			t.Fatalf("reply = %#v, want SourceError halted", m)
		}
		if sh := servedHeightOf(st); sh != 0 {
			t.Fatalf("servedHeight = %d, want unchanged 0", sh)
		}

		wEnd.Close()
		waitServe(t, done)
	})
}

// --- Committed ---

func TestServeCommittedValid(t *testing.T) {
	sEnd, wEnd := newChannel(t)
	st := &workerState{maxHeld: heldCap(defaultMaxPendingBatches), withdrawals: testStore(t, 1)}
	w := WorkerConfig{DomainID: 1}
	executor := types.Address{0x09}
	// syncRoundTrip below needs a working FrontierRequest round trip to serve as its sync point.
	src := Sources{Momenta: fakeMomenta{frontier: func() (uint64, error) { return 0, nil }}}
	done := runServe(t, sEnd, w, executor, src, st)

	c1 := commitmentFor(w.DomainID, executor, 1, 5)
	if err := ipc.WriteMessage(wEnd, ipc.Retract{FromBatchID: 0}); err != nil {
		t.Fatalf("WriteMessage: %v", err)
	}
	writeHandOff(t, wEnd, ipc.Committed{Commitment: c1})
	// A Committed frame gets no reply; sync against serve's recording goroutine before reading
	// shared state.
	syncRoundTrip(t, wEnd)

	st.mu.Lock()
	if st.lastCommitted == nil || !reflect.DeepEqual(*st.lastCommitted, c1) {
		t.Fatalf("lastCommitted = %#v, want %#v", st.lastCommitted, c1)
	}
	if !st.progressed {
		t.Fatalf("progressed = false, want true")
	}
	if st.highWater != 5 {
		t.Fatalf("highWater = %d, want 5", st.highWater)
	}
	st.progressed = false
	st.mu.Unlock()

	// A second Committed with a lower LastInputSeq updates lastCommitted but is not progress.
	c2 := commitmentFor(w.DomainID, executor, 2, 2)
	writeHandOff(t, wEnd, ipc.Committed{Commitment: c2})
	syncRoundTrip(t, wEnd)

	st.mu.Lock()
	if st.lastCommitted == nil || !reflect.DeepEqual(*st.lastCommitted, c2) {
		t.Fatalf("lastCommitted = %#v, want %#v", st.lastCommitted, c2)
	}
	if st.progressed {
		t.Fatalf("progressed = true, want false")
	}
	if st.highWater != 5 {
		t.Fatalf("highWater = %d, want unchanged 5", st.highWater)
	}
	st.progressed = false
	st.mu.Unlock()

	// A third Committed with LastInputSeq exactly at highWater updates lastCommitted but is
	// still not progress: only a strictly higher LastInputSeq counts.
	c3 := commitmentFor(w.DomainID, executor, 3, 5)
	writeHandOff(t, wEnd, ipc.Committed{Commitment: c3})
	syncRoundTrip(t, wEnd)

	st.mu.Lock()
	if st.lastCommitted == nil || !reflect.DeepEqual(*st.lastCommitted, c3) {
		t.Fatalf("lastCommitted = %#v, want %#v", st.lastCommitted, c3)
	}
	if st.progressed {
		t.Fatalf("progressed = true, want false (LastInputSeq equals highWater, not above it)")
	}
	if st.highWater != 5 {
		t.Fatalf("highWater = %d, want unchanged 5", st.highWater)
	}
	if len(st.commitments) != 3 {
		t.Fatalf("commitments = %d entries, want 3", len(st.commitments))
	}
	for _, c := range []codec.BatchCommitment{c1, c2, c3} {
		if got, ok := st.commitments[c.BatchID]; !ok || !reflect.DeepEqual(got, c) {
			t.Fatalf("commitments[%d] = %#v, %v, want %#v", c.BatchID, got, ok, c)
		}
	}
	st.mu.Unlock()

	wEnd.Close()
	if err := waitServe(t, done); err != nil {
		t.Fatalf("serve = %v, want nil", err)
	}
}

// recoveryHarness runs serve on a fresh channel with a working FrontierRequest, which
// syncRoundTrip needs as its sync point.
type recoveryHarness struct {
	t    *testing.T
	wEnd io.ReadWriteCloser
	st   *workerState
	done <-chan error
	w    WorkerConfig
	exec types.Address
}

func newRecoveryHarness(t *testing.T) *recoveryHarness {
	t.Helper()
	sEnd, wEnd := newChannel(t)
	h := &recoveryHarness{t: t, wEnd: wEnd, st: &workerState{maxHeld: heldCap(defaultMaxPendingBatches), withdrawals: testStore(t, 1)}, w: WorkerConfig{DomainID: 1}, exec: types.Address{0x09}}
	src := Sources{Momenta: fakeMomenta{frontier: func() (uint64, error) { return 0, nil }}}
	h.done = runServe(t, sEnd, h.w, h.exec, src, h.st)
	// The session's opening Retract, which discards nothing these tests hold.
	h.send(ipc.Retract{FromBatchID: math.MaxUint64})
	return h
}

func (h *recoveryHarness) send(m ipc.Message) {
	h.t.Helper()
	switch m.(type) {
	case ipc.Committed, ipc.Recovered:
		writeHandOff(h.t, h.wEnd, m)
		return
	}
	if err := ipc.WriteMessage(h.wEnd, m); err != nil {
		h.t.Fatalf("WriteMessage: %v", err)
	}
}

// sendSync sends m and waits until serve has processed it.
func (h *recoveryHarness) sendSync(m ipc.Message) {
	h.t.Helper()
	h.send(m)
	syncRoundTrip(h.t, h.wEnd)
}

func (h *recoveryHarness) commitment(batchID, lastInputSeq uint64) codec.BatchCommitment {
	return commitmentFor(h.w.DomainID, h.exec, batchID, lastInputSeq)
}

func (h *recoveryHarness) finish() {
	h.t.Helper()
	h.wEnd.Close()
	if err := waitServe(h.t, h.done); err != nil {
		h.t.Fatalf("serve = %v, want nil", err)
	}
}

func TestServeRecovered(t *testing.T) {
	h := newRecoveryHarness(t)
	c1 := h.commitment(1, 5)
	h.sendSync(ipc.Committed{Commitment: c1})
	h.st.mu.Lock()
	h.st.progressed = false
	h.st.mu.Unlock()

	c2 := h.commitment(2, 9)
	h.send(ipc.Retract{FromBatchID: 3})
	h.sendSync(ipc.Recovered{Commitment: c2})

	h.st.mu.Lock()
	defer h.st.mu.Unlock()
	if got, ok := h.st.commitments[2]; !ok || !reflect.DeepEqual(got, c2) {
		t.Fatalf("commitments[2] = %#v, %v, want %#v", got, ok, c2)
	}
	if h.st.highWater != 9 {
		t.Fatalf("highWater = %d, want 9", h.st.highWater)
	}
	if h.st.progressed {
		t.Fatalf("progressed = true, want false")
	}
	if h.st.lastCommitted == nil || !reflect.DeepEqual(*h.st.lastCommitted, c1) {
		t.Fatalf("lastCommitted = %#v, want %#v", h.st.lastCommitted, c1)
	}
}

func TestServeRetract(t *testing.T) {
	h := newRecoveryHarness(t)
	for id := uint64(1); id <= 4; id++ {
		h.send(ipc.Committed{Commitment: h.commitment(id, id*2)})
	}
	syncRoundTrip(t, h.wEnd)
	h.st.mu.Lock()
	h.st.progressed = false
	h.st.mu.Unlock()

	h.sendSync(ipc.Retract{FromBatchID: 3})

	h.st.mu.Lock()
	defer h.st.mu.Unlock()
	if len(h.st.commitments) != 2 {
		t.Fatalf("commitments = %v, want batches 1 and 2 only", h.st.commitments)
	}
	for _, id := range []uint64{1, 2} {
		if _, ok := h.st.commitments[id]; !ok {
			t.Fatalf("commitments lost batch %d", id)
		}
	}
	if h.st.highWater != 8 || h.st.progressed {
		t.Fatalf("highWater = %d, progressed = %v, want 8, false", h.st.highWater, h.st.progressed)
	}
}

type heldSnapshot struct {
	commitments   map[uint64]codec.BatchCommitment
	highWater     uint64
	progressed    bool
	lastCommitted *codec.BatchCommitment
	fault         string
}

func snapshotHeld(st *workerState) heldSnapshot {
	st.mu.Lock()
	defer st.mu.Unlock()
	s := heldSnapshot{commitments: map[uint64]codec.BatchCommitment{}, highWater: st.highWater,
		progressed: st.progressed, fault: st.fault}
	for k, v := range st.commitments {
		s.commitments[k] = v
	}
	if st.lastCommitted != nil {
		lc := *st.lastCommitted
		s.lastCommitted = &lc
	}
	return s
}

func TestServeHeldCommitment(t *testing.T) {
	frames := []struct {
		name string
		msg  func(codec.BatchCommitment) ipc.Message
	}{
		{"Recovered", func(c codec.BatchCommitment) ipc.Message { return ipc.Recovered{Commitment: c} }},
		{"Committed", func(c codec.BatchCommitment) ipc.Message { return ipc.Committed{Commitment: c} }},
	}
	for _, f := range frames {
		t.Run("equal "+f.name+" is a no-op", func(t *testing.T) {
			h := newRecoveryHarness(t)
			c := h.commitment(1, 5)
			h.sendSync(ipc.Committed{Commitment: c})
			h.st.mu.Lock()
			h.st.progressed = false
			h.st.mu.Unlock()
			before := snapshotHeld(h.st)

			h.send(ipc.Retract{FromBatchID: c.BatchID + 1})
			h.sendSync(f.msg(c))

			if after := snapshotHeld(h.st); !reflect.DeepEqual(before, after) {
				t.Fatalf("state changed: before %#v, after %#v", before, after)
			}
			h.finish()
		})

		t.Run("different "+f.name+" is a consistency fault", func(t *testing.T) {
			h := newRecoveryHarness(t)
			c := h.commitment(7, 5)
			h.sendSync(ipc.Committed{Commitment: c})
			before := snapshotHeld(h.st)

			other := c
			other.DAHash = types.Hash{0xEE}
			h.send(ipc.Retract{FromBatchID: c.BatchID + 1})
			h.send(f.msg(other))

			err := waitServe(t, h.done)
			if !errors.Is(err, ErrInconsistent) || errors.Is(err, ErrMalformed) {
				t.Fatalf("serve = %v, want ErrInconsistent and not ErrMalformed", err)
			}
			h.st.mu.Lock()
			defer h.st.mu.Unlock()
			if !strings.Contains(h.st.fault, "batch 7") {
				t.Fatalf("fault = %q, want it to name batch 7", h.st.fault)
			}
			if got := h.st.commitments[7]; !reflect.DeepEqual(got, c) {
				t.Fatalf("held commitment = %#v, want unchanged %#v", got, c)
			}
			if h.st.highWater != before.highWater || h.st.progressed != before.progressed ||
				!reflect.DeepEqual(h.st.lastCommitted, before.lastCommitted) {
				t.Fatalf("progress state changed by a faulting frame")
			}
		})
	}

	t.Run("retract then a different Committed stores it", func(t *testing.T) {
		h := newRecoveryHarness(t)
		c := h.commitment(4, 5)
		h.sendSync(ipc.Committed{Commitment: c})

		other := c
		other.DAHash = types.Hash{0xEE}
		h.send(ipc.Retract{FromBatchID: 4})
		h.sendSync(ipc.Committed{Commitment: other})

		h.st.mu.Lock()
		got := h.st.commitments[4]
		fault := h.st.fault
		h.st.mu.Unlock()
		if !reflect.DeepEqual(got, other) || fault != "" {
			t.Fatalf("commitments[4] = %#v, fault = %q, want the new commitment and no fault", got, fault)
		}
		h.finish()
	})
}

func TestServeHeldConflictKeepsFirstFault(t *testing.T) {
	h := newRecoveryHarness(t)
	c := h.commitment(7, 5)
	h.sendSync(ipc.Committed{Commitment: c})
	const preset = "supervisor: consistency fault: preset"
	h.st.mu.Lock()
	h.st.fault = preset
	h.st.mu.Unlock()

	other := c
	other.DAHash = types.Hash{0xEE}
	h.send(ipc.Committed{Commitment: other})

	err := waitServe(t, h.done)
	if !errors.Is(err, ErrInconsistent) || !strings.Contains(err.Error(), "batch 7") {
		t.Fatalf("serve = %v, want ErrInconsistent naming batch 7", err)
	}
	h.st.mu.Lock()
	defer h.st.mu.Unlock()
	if h.st.fault != preset {
		t.Fatalf("fault = %q, want the first fault %q", h.st.fault, preset)
	}
}

func TestServeFrameOrder(t *testing.T) {
	exec := types.Address{0x09}
	C := func(id uint64) ipc.Message { return ipc.Committed{Commitment: commitmentFor(1, exec, id, id)} }
	R := func(id uint64) ipc.Message { return ipc.Recovered{Commitment: commitmentFor(1, exec, id, id)} }
	Rt := func(from uint64) ipc.Message { return ipc.Retract{FromBatchID: from} }
	E := func(id uint64) ipc.Message { return ipc.WithdrawalsEnd{BatchID: id} }
	wc := commitmentFor(1, exec, 0, 1)
	wd := withdrawalBatch(t, &wc, []codec.OutboxMessage{l1msg(1, 0, assetN(1), 5, types.Hash{0x01})})[0]
	W := func(id uint64) ipc.Message {
		w := wd
		w.BatchID = id
		return w
	}

	violations := []struct {
		name   string
		frames []ipc.Message
		text   string
	}{
		{"Committed first", []ipc.Message{C(1)}, "Committed batch 1 arrived before the session's first Retract"},
		{"Recovered first", []ipc.Message{R(1)}, "Recovered batch 1 arrived before the session's first Retract"},
		{"Recovered after Committed", []ipc.Message{Rt(5), C(1), E(1), R(2)}, "follows a Committed with no Retract between"},
		{"Recovered equal to from", []ipc.Message{Rt(5), R(5)}, "is at or above the Retract from batch 5"},
		{"Recovered above from", []ipc.Message{Rt(5), R(6)}, "is at or above the Retract from batch 5"},
		{"Recovered repeated", []ipc.Message{Rt(5), R(2), E(2), R(2)}, "is not above the previous Recovered batch"},
		{"Recovered descending", []ipc.Message{Rt(5), R(3), E(3), R(2)}, "is not above the previous Recovered batch"},
		{"order before field check", []ipc.Message{ipc.Committed{Commitment: commitmentFor(1, types.Address{0xFF}, 1, 1)}}, "Committed batch 1 arrived before the session's first Retract"},
		{"later Retract sets from", []ipc.Message{Rt(5), Rt(2), R(3)}, "is at or above the Retract from batch 2"},
		{"Withdrawal first", []ipc.Message{Rt(5), W(1)}, "arrived outside a hand-off"},
		{"WithdrawalsEnd first", []ipc.Message{Rt(5), E(1)}, "arrived outside a hand-off"},
		{"Withdrawal for another batch", []ipc.Message{Rt(5), C(1), W(2)}, "arrived inside batch 1's hand-off"},
		{"WithdrawalsEnd for another batch", []ipc.Message{Rt(5), C(1), E(2)}, "arrived inside batch 1's hand-off"},
		{"request inside a hand-off", []ipc.Message{Rt(5), C(1), ipc.FrontierRequest{}}, "arrived inside batch 1's hand-off"},
		{"Retract inside a hand-off", []ipc.Message{Rt(5), C(1), Rt(5)}, "arrived inside batch 1's hand-off"},
		{"Committed inside a hand-off", []ipc.Message{Rt(5), C(1), C(2)}, "arrived inside batch 1's hand-off"},
	}
	for _, tc := range violations {
		t.Run(tc.name, func(t *testing.T) {
			sEnd, wEnd := newChannel(t)
			st := &workerState{maxHeld: heldCap(defaultMaxPendingBatches), withdrawals: testStore(t, 1)}
			done := runServe(t, sEnd, WorkerConfig{DomainID: 1}, exec, Sources{}, st)
			for _, m := range tc.frames {
				if err := ipc.WriteMessage(wEnd, m); err != nil {
					t.Fatalf("WriteMessage: %v", err)
				}
			}
			wEnd.Close()
			err := waitServe(t, done)
			if !errors.Is(err, ErrMalformed) || errors.Is(err, ErrInconsistent) || !strings.Contains(err.Error(), tc.text) {
				t.Fatalf("serve = %v, want ErrMalformed containing %q", err, tc.text)
			}
			st.mu.Lock()
			defer st.mu.Unlock()
			if st.fault != "" {
				t.Fatalf("fault = %q, want none", st.fault)
			}
			if len(tc.frames) == 1 && len(st.commitments) != 0 {
				t.Fatalf("held %d commitments, want none", len(st.commitments))
			}
		})
	}

	c3 := commitmentFor(1, exec, 3, 3)
	c3.DAHash = types.Hash{0xAB}
	honest := []struct {
		name   string
		frames []ipc.Message
		held   []uint64
	}{
		{"boot then rollback", []ipc.Message{Rt(3), R(1), E(1), R(2), E(2), C(3), E(3), C(4), E(4), Rt(3), R(1), E(1), R(2), E(2), ipc.Committed{Commitment: c3}, E(3)}, []uint64{1, 2, 3}},
		{"Retract zero then Committed", []ipc.Message{Rt(0), C(0), E(0)}, []uint64{0}},
		{"Retract resets ascending", []ipc.Message{Rt(5), R(3), E(3), Rt(5), R(1), E(1)}, []uint64{1, 3}},
	}
	for _, tc := range honest {
		t.Run(tc.name, func(t *testing.T) {
			sEnd, wEnd := newChannel(t)
			st := &workerState{maxHeld: heldCap(defaultMaxPendingBatches), withdrawals: testStore(t, 1)}
			done := runServe(t, sEnd, WorkerConfig{DomainID: 1}, exec, Sources{}, st)
			for _, m := range tc.frames {
				if err := ipc.WriteMessage(wEnd, m); err != nil {
					t.Fatalf("WriteMessage: %v", err)
				}
			}
			wEnd.Close()
			if err := waitServe(t, done); err != nil {
				t.Fatalf("serve = %v, want nil", err)
			}
			st.mu.Lock()
			defer st.mu.Unlock()
			if st.fault != "" {
				t.Fatalf("fault = %q, want none", st.fault)
			}
			if len(st.commitments) != len(tc.held) {
				t.Fatalf("held %d commitments, want batches %v", len(st.commitments), tc.held)
			}
			for _, id := range tc.held {
				if _, ok := st.commitments[id]; !ok {
					t.Fatalf("batch %d not held, want batches %v", id, tc.held)
				}
			}
		})
	}
}

func TestServeHeldCommitmentCap(t *testing.T) {
	const limit = 8
	t.Run("an honest worker at the cap is not killed", func(t *testing.T) {
		h := newRecoveryHarness(t)
		h.st.maxHeld = limit
		for id := uint64(1); id <= limit; id++ {
			h.send(ipc.Committed{Commitment: h.commitment(id, id)})
		}
		// A re-report of a held batch is not a new entry.
		h.send(ipc.Retract{FromBatchID: limit + 1})
		h.send(ipc.Recovered{Commitment: h.commitment(limit, limit)})
		syncRoundTrip(t, h.wEnd)
		h.st.mu.Lock()
		held := len(h.st.commitments)
		h.st.mu.Unlock()
		if held != limit {
			t.Fatalf("held = %d, want %d", held, limit)
		}
		h.finish()
	})

	t.Run("a new batchId over the cap is malformed", func(t *testing.T) {
		h := newRecoveryHarness(t)
		h.st.maxHeld = limit
		for id := uint64(1); id <= limit; id++ {
			h.send(ipc.Recovered{Commitment: h.commitment(id, id)})
		}
		h.send(ipc.Committed{Commitment: h.commitment(limit+1, limit+1)})
		err := waitServe(t, h.done)
		if !errors.Is(err, ErrMalformed) || errors.Is(err, ErrInconsistent) || !strings.Contains(err.Error(), "would exceed the cap") {
			t.Fatalf("serve = %v, want ErrMalformed naming the cap", err)
		}
		h.st.mu.Lock()
		defer h.st.mu.Unlock()
		if len(h.st.commitments) != limit || h.st.fault != "" {
			t.Fatalf("held = %d, fault = %q, want %d and no fault", len(h.st.commitments), h.st.fault, limit)
		}
	})

	t.Run("a retract frees room under the cap", func(t *testing.T) {
		h := newRecoveryHarness(t)
		h.st.maxHeld = limit
		for id := uint64(1); id <= limit; id++ {
			h.send(ipc.Committed{Commitment: h.commitment(id, id)})
		}
		h.send(ipc.Retract{FromBatchID: 5})
		h.sendSync(ipc.Committed{Commitment: h.commitment(limit+1, limit+1)})
		h.finish()
	})
}

// --- EOF, short read ---

func TestServeEOFAndShortRead(t *testing.T) {
	t.Run("EOF", func(t *testing.T) {
		sEnd, wEnd := newChannel(t)
		st := &workerState{maxHeld: heldCap(defaultMaxPendingBatches), withdrawals: testStore(t, 1)}
		done := runServe(t, sEnd, WorkerConfig{}, types.Address{}, Sources{}, st)
		wEnd.Close()
		if err := waitServe(t, done); err != nil {
			t.Fatalf("serve = %v, want nil", err)
		}
	})

	t.Run("short read", func(t *testing.T) {
		sEnd, wEnd := newChannel(t)
		st := &workerState{maxHeld: heldCap(defaultMaxPendingBatches), withdrawals: testStore(t, 1)}
		done := runServe(t, sEnd, WorkerConfig{}, types.Address{}, Sources{}, st)
		if _, err := wEnd.Write([]byte{0x00, 0x00}); err != nil {
			t.Fatalf("Write: %v", err)
		}
		wEnd.Close()
		if err := waitServe(t, done); err != nil {
			t.Fatalf("serve = %v, want nil", err)
		}
	})
}

// --- malformed frames ---

func TestServeMalformed(t *testing.T) {
	w := WorkerConfig{DomainID: 1}
	executor := types.Address{0x09}

	cases := []struct {
		name  string
		frame []byte
		close bool // close the connection after writing, instead of leaving it open
	}{
		{"length prefix over MaxWorkerFrame", func() []byte {
			var hdr [4]byte
			binary.BigEndian.PutUint32(hdr[:], ipc.MaxWorkerFrame+1)
			return hdr[:]
		}(), false},
		{"unknown type", rawFrame(0x90, nil), true},
		{"S->W type sent by worker", rawFrame(0x02, nil), true},
		{"FrontierRequest with a trailing byte", rawFrame(0x81, []byte{0x00}), true},
		{"MomentaRequest count 0", rawFrame(0x82, rawU64Pair(1, 0)), true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			sEnd, wEnd := newChannel(t)
			st := &workerState{maxHeld: heldCap(defaultMaxPendingBatches), withdrawals: testStore(t, 1)}
			done := runServe(t, sEnd, w, executor, Sources{}, st)

			if _, err := wEnd.Write(tc.frame); err != nil {
				t.Fatalf("Write: %v", err)
			}
			if tc.close {
				wEnd.Close()
			}

			err := waitServe(t, done)
			if !errors.Is(err, ErrMalformed) {
				t.Fatalf("serve = %v, want wrapping ErrMalformed", err)
			}
		})
	}

	frameCases := []struct {
		name string
		c    codec.BatchCommitment
	}{
		{"wrong domain", commitmentFor(w.DomainID+1, executor, 1, 1)},
		{"wrong executor", commitmentFor(w.DomainID, types.Address{0xFF}, 1, 1)},
		{"submittedAtHeight 1", func() codec.BatchCommitment {
			c := commitmentFor(w.DomainID, executor, 1, 1)
			c.SubmittedAtHeight = 1
			return c
		}()},
	}
	frames := []struct {
		name string
		msg  func(codec.BatchCommitment) ipc.Message
	}{
		{"Committed", func(c codec.BatchCommitment) ipc.Message { return ipc.Committed{Commitment: c} }},
		{"Recovered", func(c codec.BatchCommitment) ipc.Message { return ipc.Recovered{Commitment: c} }},
	}
	for _, f := range frames {
		for _, tc := range frameCases {
			t.Run(f.name+" "+tc.name, func(t *testing.T) {
				malformedCommitmentFrame(t, w, executor, f.msg(tc.c))
			})
		}
	}
}

func malformedCommitmentFrame(t *testing.T, w WorkerConfig, executor types.Address, m ipc.Message) {
	t.Helper()
	sEnd, wEnd := newChannel(t)
	st := &workerState{maxHeld: heldCap(defaultMaxPendingBatches), withdrawals: testStore(t, 1)}
	done := runServe(t, sEnd, w, executor, Sources{}, st)

	// The session's opening Retract, so the frame reaches checkCommitment.
	if err := ipc.WriteMessage(wEnd, ipc.Retract{FromBatchID: math.MaxUint64}); err != nil {
		t.Fatalf("WriteMessage: %v", err)
	}
	if err := ipc.WriteMessage(wEnd, m); err != nil {
		t.Fatalf("WriteMessage: %v", err)
	}

	err := waitServe(t, done)
	if !errors.Is(err, ErrMalformed) || !strings.Contains(err.Error(), "commitment ") {
		t.Fatalf("serve = %v, want ErrMalformed from a commitment field check", err)
	}
}

// --- reply write failures ---

// failAfterWriter is an io.ReadWriter whose Write starts returning an error from its failFrom'th
// call onward (1-indexed); calls before that succeed and are recorded. Its Read side replays a
// fixed byte sequence, so a serve that wrongly keeps going after a write failure reads whatever
// comes next.
type failAfterWriter struct {
	io.Reader
	failFrom int
	calls    int
}

func (w *failAfterWriter) Write(p []byte) (int, error) {
	w.calls++
	if w.calls >= w.failFrom {
		return 0, io.ErrClosedPipe
	}
	return len(p), nil
}

// requestThenBadFrame is one encoded request frame followed by an unknown-type frame: if serve
// kept serving after a write failure instead of returning, it would read this next and return an
// error wrapping ErrMalformed instead of nil.
func requestThenBadFrame(t *testing.T, m ipc.Message) io.Reader {
	t.Helper()
	b, err := ipc.Encode(m)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	var buf bytes.Buffer
	buf.Write(b)
	buf.Write(rawFrame(0x90, nil))
	return &buf
}

// TestServeWriteFailure proves serve stops on the first failed reply write, returns nil (not
// ErrMalformed) and never records a result the worker didn't actually receive, rather than
// silently ignoring the failure and reading on.
func TestServeWriteFailure(t *testing.T) {
	t.Run("Frontier reply", func(t *testing.T) {
		conn := &failAfterWriter{Reader: requestThenBadFrame(t, ipc.FrontierRequest{}), failFrom: 1}
		st := &workerState{maxHeld: heldCap(defaultMaxPendingBatches), withdrawals: testStore(t, 1)}
		src := Sources{Momenta: fakeMomenta{frontier: func() (uint64, error) { return 42, nil }}}
		if err := serve(conn, WorkerConfig{DomainID: 1}, types.Address{}, src, st); err != nil {
			t.Fatalf("serve = %v, want nil", err)
		}
	})

	t.Run("Anchor reply", func(t *testing.T) {
		conn := &failAfterWriter{Reader: requestThenBadFrame(t, ipc.AnchorRequest{}), failFrom: 1}
		st := &workerState{maxHeld: heldCap(defaultMaxPendingBatches), withdrawals: testStore(t, 1)}
		src := Sources{Anchors: fakeAnchors{anchor: func(uint32) (recovery.Anchor, bool, error) {
			return recovery.Anchor{}, false, nil
		}}}
		if err := serve(conn, WorkerConfig{DomainID: 1}, types.Address{}, src, st); err != nil {
			t.Fatalf("serve = %v, want nil", err)
		}
	})

	t.Run("Momenta source-error reply", func(t *testing.T) {
		conn := &failAfterWriter{Reader: requestThenBadFrame(t, ipc.MomentaRequest{Height: 1, Count: 1}), failFrom: 1}
		st := &workerState{maxHeld: heldCap(defaultMaxPendingBatches), withdrawals: testStore(t, 1)}
		src := Sources{Momenta: fakeMomenta{byHeight: func(uint64, uint64) ([]*nom.DetailedMomentum, error) {
			return nil, errors.New("upstream unavailable")
		}}}
		if err := serve(conn, WorkerConfig{DomainID: 1}, types.Address{}, src, st); err != nil {
			t.Fatalf("serve = %v, want nil", err)
		}
		if sh := servedHeightOf(st); sh != 0 {
			t.Fatalf("servedHeight = %d, want 0", sh)
		}
	})

	t.Run("first Momentum frame of a 3-momentum range", func(t *testing.T) {
		chain := fiveMomentumChain()[:3]
		conn := &failAfterWriter{Reader: requestThenBadFrame(t, ipc.MomentaRequest{Height: 1, Count: 3}), failFrom: 1}
		st := &workerState{maxHeld: heldCap(defaultMaxPendingBatches), withdrawals: testStore(t, 1)}
		src := Sources{Momenta: fakeMomenta{byHeight: func(uint64, uint64) ([]*nom.DetailedMomentum, error) {
			return chain, nil
		}}}
		if err := serve(conn, WorkerConfig{DomainID: 1}, types.Address{}, src, st); err != nil {
			t.Fatalf("serve = %v, want nil", err)
		}
		if sh := servedHeightOf(st); sh != 0 {
			t.Fatalf("servedHeight = %d, want 0", sh)
		}
	})

	t.Run("RangeEnd of a 3-momentum range", func(t *testing.T) {
		chain := fiveMomentumChain()[:3]
		conn := &failAfterWriter{Reader: requestThenBadFrame(t, ipc.MomentaRequest{Height: 1, Count: 3}), failFrom: 4}
		st := &workerState{maxHeld: heldCap(defaultMaxPendingBatches), withdrawals: testStore(t, 1)}
		src := Sources{Momenta: fakeMomenta{byHeight: func(uint64, uint64) ([]*nom.DetailedMomentum, error) {
			return chain, nil
		}}}
		if err := serve(conn, WorkerConfig{DomainID: 1}, types.Address{}, src, st); err != nil {
			t.Fatalf("serve = %v, want nil", err)
		}
		if sh := servedHeightOf(st); sh != 0 {
			t.Fatalf("servedHeight = %d, want 0 (RangeEnd itself failed to write)", sh)
		}
	})
}

// --- against the real client (internal/ipc.Client) ---

func TestServeAssignment(t *testing.T) {
	entries := []ledger.AssignmentEntry{
		{Ordinal: 3, GlobalInputIndex: 10, SourceAccountBlockHash: types.Hash{0x01}, SendMomentumHeight: 7},
		{Ordinal: 4, GlobalInputIndex: 12, SourceAccountBlockHash: types.Hash{0x02}, SendMomentumHeight: 8},
	}
	type call struct {
		domainID    uint32
		height      uint64
		hash        types.Hash
		nextOrdinal uint64
	}
	run := func(t *testing.T, reply func() ([]ledger.AssignmentEntry, error)) (*ipc.Client, *call) {
		t.Helper()
		sEnd, wEnd := newChannel(t)
		w := WorkerConfig{DomainID: 9, Role: "EXECUTOR", STFSpecHash: types.Hash{0x01}, MemoryLimit: 1 << 30, CPULimit: 1}
		cfg := Config{ExecutorAddress: types.Address{0x09}, NetworkName: "devnet", ConfirmationDepth: 10, StoragePath: "/var/lib/executor"}
		if err := ipc.WriteMessage(sEnd, cfg.workerConfig(w)); err != nil {
			t.Fatalf("WriteMessage config: %v", err)
		}
		got := new(call)
		src := Sources{Assignments: fakeAssignments{assignment: func(d uint32, h uint64, hash types.Hash, n uint64) ([]ledger.AssignmentEntry, error) {
			*got = call{d, h, hash, n}
			return reply()
		}}}
		done := runServe(t, sEnd, w, types.Address{}, src, &workerState{maxHeld: heldCap(defaultMaxPendingBatches), withdrawals: testStore(t, 1)})
		client := ipc.NewClient(wEnd, nil)
		if _, err := client.ReadConfig(); err != nil {
			t.Fatalf("ReadConfig: %v", err)
		}
		t.Cleanup(func() {
			wEnd.Close()
			if err := waitServe(t, done); err != nil {
				t.Errorf("serve = %v, want nil", err)
			}
		})
		return client, got
	}

	t.Run("entries", func(t *testing.T) {
		client, got := run(t, func() ([]ledger.AssignmentEntry, error) { return entries, nil })
		out, err := client.Assignment(9, 20, types.Hash{0x0A}, 3)
		if err != nil {
			t.Fatalf("Assignment: %v", err)
		}
		if want := (call{9, 20, types.Hash{0x0A}, 3}); *got != want {
			t.Fatalf("source saw %+v, want %+v", *got, want)
		}
		if !reflect.DeepEqual(out, entries) {
			t.Fatalf("entries = %+v, want %+v", out, entries)
		}
	})

	t.Run("plain error", func(t *testing.T) {
		client, _ := run(t, func() ([]ledger.AssignmentEntry, error) { return nil, errors.New("node down") })
		_, err := client.Assignment(9, 20, types.Hash{}, 0)
		if err == nil || errors.Is(err, recovery.ErrHalted) || errors.Is(err, ipc.ErrChannel) {
			t.Fatalf("err = %v, want a plain error", err)
		}
	})

	t.Run("halt", func(t *testing.T) {
		client, _ := run(t, func() ([]ledger.AssignmentEntry, error) {
			return nil, fmt.Errorf("%w: bad", recovery.ErrHalted)
		})
		if _, err := client.Assignment(9, 20, types.Hash{}, 0); !errors.Is(err, recovery.ErrHalted) {
			t.Fatalf("err = %v, want ErrHalted", err)
		}
	})

	t.Run("unencodable entry", func(t *testing.T) {
		client, _ := run(t, func() ([]ledger.AssignmentEntry, error) {
			return []ledger.AssignmentEntry{{Ordinal: 0}}, nil
		})
		_, err := client.Assignment(9, 20, types.Hash{}, 0)
		if !errors.Is(err, recovery.ErrHalted) || !strings.Contains(err.Error(), "cannot be sent") {
			t.Fatalf("err = %v, want ErrHalted containing %q", err, "cannot be sent")
		}
	})
}

func TestServeCap(t *testing.T) {
	type call struct {
		domainID uint32
		height   uint64
		hash     types.Hash
		assets   []codec.AssetID
	}
	asset := codec.AssetIDFromZTS(types.ZnnTokenStandard)
	asset2 := codec.AssetIDFromZTS(types.QsrTokenStandard)
	run := func(t *testing.T, reply func() ([]codec.U256, error)) (*ipc.Client, *[]call) {
		t.Helper()
		sEnd, wEnd := newChannel(t)
		w := WorkerConfig{DomainID: 9, Role: "EXECUTOR", STFSpecHash: types.Hash{0x01}, MemoryLimit: 1 << 30, CPULimit: 1}
		cfg := Config{ExecutorAddress: types.Address{0x09}, NetworkName: "devnet", ConfirmationDepth: 10, StoragePath: "/var/lib/executor"}
		if err := ipc.WriteMessage(sEnd, cfg.workerConfig(w)); err != nil {
			t.Fatalf("WriteMessage config: %v", err)
		}
		got := new([]call)
		src := Sources{Caps: fakeCaps{cap: func(d uint32, h uint64, hash types.Hash, a []codec.AssetID) ([]codec.U256, error) {
			*got = append(*got, call{d, h, hash, a})
			return reply()
		}}}
		done := runServe(t, sEnd, w, types.Address{}, src, &workerState{maxHeld: heldCap(defaultMaxPendingBatches), withdrawals: testStore(t, 1)})
		client := ipc.NewClient(wEnd, nil)
		if _, err := client.ReadConfig(); err != nil {
			t.Fatalf("ReadConfig: %v", err)
		}
		t.Cleanup(func() {
			wEnd.Close()
			if err := waitServe(t, done); err != nil {
				t.Errorf("serve = %v, want nil", err)
			}
		})
		return client, got
	}

	t.Run("amounts", func(t *testing.T) {
		client, got := run(t, func() ([]codec.U256, error) { return []codec.U256{{31: 7}, {31: 8}}, nil })
		out, err := client.MaxBatchWithdrawals(9, 20, types.Hash{0x0A}, []codec.AssetID{asset, asset2})
		if want := []codec.U256{{31: 7}, {31: 8}}; err != nil || !reflect.DeepEqual(out, want) {
			t.Fatalf("MaxBatchWithdrawals = %x, %v; want %x", out, err, want)
		}
		if want := []call{{9, 20, types.Hash{0x0A}, []codec.AssetID{asset, asset2}}}; !reflect.DeepEqual(*got, want) {
			t.Fatalf("source saw %+v, want %+v", *got, want)
		}
	})

	t.Run("plain error", func(t *testing.T) {
		client, _ := run(t, func() ([]codec.U256, error) { return nil, errors.New("node down") })
		_, err := client.MaxBatchWithdrawals(9, 20, types.Hash{}, []codec.AssetID{asset})
		if err == nil || errors.Is(err, recovery.ErrHalted) || errors.Is(err, ipc.ErrChannel) {
			t.Fatalf("err = %v, want a plain error", err)
		}
	})

	t.Run("halt", func(t *testing.T) {
		client, _ := run(t, func() ([]codec.U256, error) {
			return nil, fmt.Errorf("%w: bad", recovery.ErrHalted)
		})
		if _, err := client.MaxBatchWithdrawals(9, 20, types.Hash{}, []codec.AssetID{asset}); !errors.Is(err, recovery.ErrHalted) {
			t.Fatalf("err = %v, want ErrHalted", err)
		}
	})
}

func TestServeAgainstRealClient(t *testing.T) {
	sEnd, wEnd := newChannel(t)

	executor := types.Address{0x09}
	cfg := Config{ExecutorAddress: executor, NetworkName: "devnet", ConfirmationDepth: 10, StoragePath: "/var/lib/executor"}
	w := WorkerConfig{DomainID: 1, Role: "EXECUTOR", STFSpecHash: types.Hash{0x01}, MemoryLimit: 1 << 30, CPULimit: 1}
	wireCfg := cfg.workerConfig(w)
	if err := ipc.WriteMessage(sEnd, wireCfg); err != nil {
		t.Fatalf("WriteMessage config: %v", err)
	}

	chain := fiveMomentumChain()
	frontierHeight := uint64(5)
	anchor := recovery.Anchor{PostStateRoot: types.Hash{0x02}, LastInputSeq: 3}

	var momentaCalls int
	src := Sources{
		Momenta: fakeMomenta{
			frontier: func() (uint64, error) { return frontierHeight, nil },
			byHeight: func(height, count uint64) ([]*nom.DetailedMomentum, error) {
				momentaCalls++
				switch momentaCalls {
				case 1:
					return chain, nil
				case 2:
					return []*nom.DetailedMomentum{chain[0], nil, chain[1]}, nil
				case 3:
					return nil, errors.New("upstream unavailable")
				default:
					return chain, nil
				}
			},
		},
		Anchors: fakeAnchors{anchor: func(uint32) (recovery.Anchor, bool, error) { return anchor, true, nil }},
	}

	st := &workerState{maxHeld: heldCap(defaultMaxPendingBatches), withdrawals: testStore(t, 1)}
	done := runServe(t, sEnd, w, executor, src, st)

	client := ipc.NewClient(wEnd, nil)
	gotCfg, err := client.ReadConfig()
	if err != nil {
		t.Fatalf("ReadConfig: %v", err)
	}
	if gotCfg != wireCfg {
		t.Fatalf("ReadConfig = %+v, want %+v", gotCfg, wireCfg)
	}

	if h, err := client.FrontierHeight(); err != nil || h != frontierHeight {
		t.Fatalf("FrontierHeight = %d, %v, want %d, nil", h, err, frontierHeight)
	}

	ms, err := client.DetailedMomentumsByHeight(1, uint64(len(chain)))
	if err != nil {
		t.Fatalf("DetailedMomentumsByHeight: %v", err)
	}
	if len(ms) != len(chain) {
		t.Fatalf("got %d momenta, want %d", len(ms), len(chain))
	}

	a, ok, err := client.FinalizedAnchor(w.DomainID)
	if err != nil || !ok || a != anchor {
		t.Fatalf("FinalizedAnchor = %+v, %v, %v", a, ok, err)
	}

	// Hand-off frames follow the Retract, sit between two Recovered
	// frames and follow the Committed. The supervisor verifies and
	// stores each batch's withdrawal.
	msgs := make([][]byte, 3)
	proofs := make([][]byte, 3)
	cms := make([]codec.BatchCommitment, 3)
	for id := range cms {
		msg, err := codec.OutboxMessage{
			SourceContract: types.Hash{0x04},
			InputIndex:     uint64(id),
			Target:         codec.L1Withdrawal{RecipientL1: types.Address{0x01}, Asset: codec.AssetIDFromZTS(types.ZnnTokenStandard), Amount: codec.U256{31: 9}},
		}.Encode()
		if err != nil {
			t.Fatalf("Encode: %v", err)
		}
		msgs[id], proofs[id] = msg, codec.MerkleProofs([][]byte{msg})[0]
		cms[id] = commitmentFor(w.DomainID, executor, uint64(id), uint64(id))
		cms[id].FirstInputSeq = uint64(id)
		cms[id].OutboxRoot = codec.MerkleRoot([][]byte{msg})
		cms[id].AssetFlowSummary = codec.AssetFlowSummary{{Asset: codec.AssetIDFromZTS(types.ZnnTokenStandard), WithdrawalDebit: codec.U256{31: 9}}}
	}
	oneWithdrawal := func(c codec.BatchCommitment) ([]batch.Withdrawal, error) {
		return []batch.Withdrawal{{Message: msgs[c.BatchID], Proof: proofs[c.BatchID]}}, nil
	}
	if err := client.ReportRecovered(2, cms[:2], oneWithdrawal); err != nil {
		t.Fatalf("ReportRecovered: %v", err)
	}
	if err := client.ReportCommitted(cms[2:], oneWithdrawal); err != nil {
		t.Fatalf("ReportCommitted: %v", err)
	}
	if client.Broken() != nil {
		t.Fatalf("Broken() = %v, want nil", client.Broken())
	}
	if _, err := client.FrontierHeight(); err != nil {
		t.Fatalf("FrontierHeight after ReportCommitted: %v", err)
	}

	st.mu.Lock()
	committed := st.lastCommitted
	held := len(st.commitments)
	_, has0 := st.commitments[0]
	_, has1 := st.commitments[1]
	_, has2 := st.commitments[2]
	st.mu.Unlock()
	if committed == nil || committed.BatchID != 2 {
		t.Fatalf("lastCommitted = %+v, want batch 2", committed)
	}
	if held != 3 || !has0 || !has1 || !has2 {
		t.Fatalf("held commitments = %d (0: %v, 1: %v, 2: %v), want batches 0, 1 and 2", held, has0, has1, has2)
	}
	for id := uint64(0); id < 3; id++ {
		b := mustBatch(t, st.withdrawals, id)
		if !b.Verified || len(b.Withdrawals) != 1 || !bytes.Equal(b.Withdrawals[0].Message, msgs[id]) || !bytes.Equal(b.Withdrawals[0].Proof, proofs[id]) {
			t.Fatalf("stored batch %d = %+v, want the verified hand-off", id, b)
		}
	}

	// A nil momentum entry in the range: an error wrapping recovery.ErrHalted, the client stays
	// healthy, and the connection recovers cleanly for the next call.
	if _, err := client.DetailedMomentumsByHeight(1, 3); !errors.Is(err, recovery.ErrHalted) {
		t.Fatalf("DetailedMomentumsByHeight (nil entry) = %v, want wrapping recovery.ErrHalted", err)
	}
	if client.Broken() != nil {
		t.Fatalf("Broken() = %v, want nil", client.Broken())
	}
	if _, err := client.FrontierHeight(); err != nil {
		t.Fatalf("FrontierHeight after a halted source error: %v", err)
	}

	// A plain source error: no ErrHalted, the client stays healthy, and the connection recovers.
	if _, err := client.DetailedMomentumsByHeight(1, 3); err == nil || errors.Is(err, recovery.ErrHalted) {
		t.Fatalf("DetailedMomentumsByHeight (plain error) = %v, want a plain error", err)
	}
	if client.Broken() != nil {
		t.Fatalf("Broken() = %v, want nil", client.Broken())
	}
	if _, err := client.FrontierHeight(); err != nil {
		t.Fatalf("FrontierHeight after an unavailable source error: %v", err)
	}

	if client.Broken() != nil {
		t.Fatalf("Broken() = %v, want nil throughout", client.Broken())
	}

	wEnd.Close()
	if err := waitServe(t, done); err != nil {
		t.Fatalf("serve = %v, want nil", err)
	}
}

func TestServeMomentaRememberedCount(t *testing.T) {
	t.Run("a failure at count 1 is remembered", func(t *testing.T) {
		sEnd, wEnd := newChannel(t)
		st := &workerState{maxHeld: heldCap(defaultMaxPendingBatches), withdrawals: testStore(t, 1)}
		lm := newLimitedMomenta(0)
		src := Sources{Momenta: fakeMomenta{byHeight: lm.byHeight}}
		done := runServe(t, sEnd, WorkerConfig{DomainID: 1}, types.Address{}, src, st)

		for i, want := range [][]uint64{{5, 2, 1}, {1}} {
			if err := ipc.WriteMessage(wEnd, ipc.MomentaRequest{Height: 1, Count: 5}); err != nil {
				t.Fatalf("WriteMessage: %v", err)
			}
			m, err := ipc.ReadMessage(wEnd, ipc.Supervisor)
			if err != nil {
				t.Fatalf("ReadMessage: %v", err)
			}
			if se, ok := m.(ipc.SourceError); !ok || se.Kind != ipc.SourceUnavailable {
				t.Fatalf("read %d: reply = %#v, want a SourceError of kind unavailable", i, m)
			}
			if c := lm.take(); !reflect.DeepEqual(c, want) {
				t.Fatalf("read %d: counts = %v, want %v", i, c, want)
			}
		}
		wEnd.Close()
		if err := waitServe(t, done); err != nil {
			t.Fatalf("serve = %v, want nil", err)
		}
	})

	t.Run("another error is not retried and leaves the count", func(t *testing.T) {
		sEnd, wEnd := newChannel(t)
		st := &workerState{maxHeld: heldCap(defaultMaxPendingBatches), withdrawals: testStore(t, 1)}
		var mu sync.Mutex
		var counts []uint64
		src := Sources{Momenta: fakeMomenta{byHeight: func(height, count uint64) ([]*nom.DetailedMomentum, error) {
			mu.Lock()
			defer mu.Unlock()
			counts = append(counts, count)
			return nil, errors.New("znnd: connection refused")
		}}}
		done := runServe(t, sEnd, WorkerConfig{DomainID: 1}, types.Address{}, src, st)

		if err := ipc.WriteMessage(wEnd, ipc.MomentaRequest{Height: 1, Count: 5}); err != nil {
			t.Fatalf("WriteMessage: %v", err)
		}
		m, err := ipc.ReadMessage(wEnd, ipc.Supervisor)
		if err != nil {
			t.Fatalf("ReadMessage: %v", err)
		}
		if _, ok := m.(ipc.SourceError); !ok {
			t.Fatalf("reply = %#v, want a SourceError", m)
		}
		wEnd.Close()
		if err := waitServe(t, done); err != nil {
			t.Fatalf("serve = %v, want nil", err)
		}
		mu.Lock()
		defer mu.Unlock()
		if !reflect.DeepEqual(counts, []uint64{5}) {
			t.Fatalf("counts = %v, want [5]", counts)
		}
		st.mu.Lock()
		defer st.mu.Unlock()
		if st.momentaCount != 0 {
			t.Fatalf("momentaCount = %d, want 0", st.momentaCount)
		}
	})

	t.Run("each worker keeps its own count", func(t *testing.T) {
		lm := newLimitedMomenta(2)
		src := Sources{Momenta: fakeMomenta{byHeight: lm.byHeight}}
		for i := 0; i < 2; i++ {
			sEnd, wEnd := newChannel(t)
			st := &workerState{maxHeld: heldCap(defaultMaxPendingBatches), withdrawals: testStore(t, 1)}
			done := runServe(t, sEnd, WorkerConfig{DomainID: 1}, types.Address{}, src, st)
			checkMomentaRead(t, "read", lm, wEnd, 1, 5, []uint64{1, 2}, []uint64{5, 2})
			wEnd.Close()
			if err := waitServe(t, done); err != nil {
				t.Fatalf("serve = %v, want nil", err)
			}
		}
	})
}

func TestRetractFlagsUnresolvedSend(t *testing.T) {
	newState := func() *workerState {
		cfg := Config{MaxPendingBatches: defaultMaxPendingBatches, Workers: []WorkerConfig{{DomainID: 1}}}
		return New(cfg, nil, Sources{}, Policy{}).slots[0].st
	}

	// The unresolved send is batch 5; a Retract from f covers every batchId at or above f.
	cases := []struct {
		name string
		from uint64
		want bool
	}{
		{"from above the batch", 6, false},
		{"from the batch", 5, true},
		{"from below the batch", 2, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			st := newState()
			st.unresolved = &unresolvedSend{batchID: 5}
			retract(tc.from, st)
			if st.unresolved.retracted != tc.want {
				t.Fatalf("retracted = %v after Retract from %d, want %v", st.unresolved.retracted, tc.from, tc.want)
			}
		})
	}

	t.Run("no unresolved send", func(t *testing.T) {
		st := newState()
		retract(0, st)
		if st.unresolved != nil {
			t.Fatal("Retract created an unresolved send")
		}
	})
}
