package supervisor

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/zenon-network/go-zenon/chain/nom"
	"github.com/zenon-network/go-zenon/common/types"

	"github.com/digitalSloth/go-executor/internal/codec"
	"github.com/digitalSloth/go-executor/internal/ipc"
	"github.com/digitalSloth/go-executor/internal/ipc/ipctest"
	"github.com/digitalSloth/go-executor/internal/recovery"
	"github.com/digitalSloth/go-executor/internal/workerproc"
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

type fakeAnchors struct {
	anchor func(domainID uint32) (recovery.Anchor, bool, error)
}

func (f fakeAnchors) FinalizedAnchor(domainID uint32) (recovery.Anchor, bool, error) {
	return f.anchor(domainID)
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
func commitmentFor(domainID uint32, executor types.Address, lastInputSeq uint64) codec.BatchCommitment {
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
		BatchID:           1,
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
	st := &workerState{}
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
		st := &workerState{}
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
		st := &workerState{}
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
	st := &workerState{}
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

func TestServeMomentaTrimsToCount(t *testing.T) {
	sEnd, wEnd := newChannel(t)
	st := &workerState{}
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
	st := &workerState{}
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
			st := &workerState{}
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
			st := &workerState{}
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
			st := &workerState{}
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
	st := &workerState{}
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
		st := &workerState{}
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
		st := &workerState{}
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
	st := &workerState{}
	w := WorkerConfig{DomainID: 1}
	executor := types.Address{0x09}
	// syncRoundTrip below needs a working FrontierRequest round trip to serve as its sync point.
	src := Sources{Momenta: fakeMomenta{frontier: func() (uint64, error) { return 0, nil }}}
	done := runServe(t, sEnd, w, executor, src, st)

	c1 := commitmentFor(w.DomainID, executor, 5)
	if err := ipc.WriteMessage(wEnd, ipc.Committed{Commitment: c1}); err != nil {
		t.Fatalf("WriteMessage: %v", err)
	}
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
	c2 := commitmentFor(w.DomainID, executor, 2)
	if err := ipc.WriteMessage(wEnd, ipc.Committed{Commitment: c2}); err != nil {
		t.Fatalf("WriteMessage: %v", err)
	}
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
	c3 := commitmentFor(w.DomainID, executor, 5)
	if err := ipc.WriteMessage(wEnd, ipc.Committed{Commitment: c3}); err != nil {
		t.Fatalf("WriteMessage: %v", err)
	}
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
	st.mu.Unlock()

	wEnd.Close()
	if err := waitServe(t, done); err != nil {
		t.Fatalf("serve = %v, want nil", err)
	}
}

// --- EOF, short read ---

func TestServeEOFAndShortRead(t *testing.T) {
	t.Run("EOF", func(t *testing.T) {
		sEnd, wEnd := newChannel(t)
		st := &workerState{}
		done := runServe(t, sEnd, WorkerConfig{}, types.Address{}, Sources{}, st)
		wEnd.Close()
		if err := waitServe(t, done); err != nil {
			t.Fatalf("serve = %v, want nil", err)
		}
	})

	t.Run("short read", func(t *testing.T) {
		sEnd, wEnd := newChannel(t)
		st := &workerState{}
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
			st := &workerState{}
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

	committedCases := []struct {
		name string
		c    codec.BatchCommitment
	}{
		{"wrong domain", commitmentFor(w.DomainID+1, executor, 1)},
		{"wrong executor", commitmentFor(w.DomainID, types.Address{0xFF}, 1)},
		{"submittedAtHeight 1", func() codec.BatchCommitment {
			c := commitmentFor(w.DomainID, executor, 1)
			c.SubmittedAtHeight = 1
			return c
		}()},
	}
	for _, tc := range committedCases {
		t.Run("Committed "+tc.name, func(t *testing.T) {
			sEnd, wEnd := newChannel(t)
			st := &workerState{}
			done := runServe(t, sEnd, w, executor, Sources{}, st)

			if err := ipc.WriteMessage(wEnd, ipc.Committed{Commitment: tc.c}); err != nil {
				t.Fatalf("WriteMessage: %v", err)
			}

			err := waitServe(t, done)
			if !errors.Is(err, ErrMalformed) {
				t.Fatalf("serve = %v, want wrapping ErrMalformed", err)
			}
		})
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
		st := &workerState{}
		src := Sources{Momenta: fakeMomenta{frontier: func() (uint64, error) { return 42, nil }}}
		if err := serve(conn, WorkerConfig{DomainID: 1}, types.Address{}, src, st); err != nil {
			t.Fatalf("serve = %v, want nil", err)
		}
	})

	t.Run("Anchor reply", func(t *testing.T) {
		conn := &failAfterWriter{Reader: requestThenBadFrame(t, ipc.AnchorRequest{}), failFrom: 1}
		st := &workerState{}
		src := Sources{Anchors: fakeAnchors{anchor: func(uint32) (recovery.Anchor, bool, error) {
			return recovery.Anchor{}, false, nil
		}}}
		if err := serve(conn, WorkerConfig{DomainID: 1}, types.Address{}, src, st); err != nil {
			t.Fatalf("serve = %v, want nil", err)
		}
	})

	t.Run("Momenta source-error reply", func(t *testing.T) {
		conn := &failAfterWriter{Reader: requestThenBadFrame(t, ipc.MomentaRequest{Height: 1, Count: 1}), failFrom: 1}
		st := &workerState{}
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
		st := &workerState{}
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
		st := &workerState{}
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

	st := &workerState{}
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

	if err := client.ReportCommitted([]codec.BatchCommitment{commitmentFor(w.DomainID, executor, 1)}); err != nil {
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
	st.mu.Unlock()
	if committed == nil {
		t.Fatalf("lastCommitted not recorded")
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
