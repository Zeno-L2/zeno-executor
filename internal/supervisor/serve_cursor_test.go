package supervisor

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"log"
	"math"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/zenon-network/go-zenon/common/types"
	"github.com/zenon-network/go-zenon/vm/embedded/definition"

	"github.com/digitalSloth/go-executor/internal/codec"
	"github.com/digitalSloth/go-executor/internal/ipc"
	"github.com/digitalSloth/go-executor/internal/recovery"
	"github.com/digitalSloth/go-executor/internal/settlement"
)

// chainFake is a Settlement stand-in whose cursor and entries tests change between frames.
type chainFake struct {
	mu       sync.Mutex
	next     uint64
	nextErr  error
	entries  map[uint64]*definition.BatchEntry
	entryErr error
	reads    int
	// entryBlock, when set, holds every AcceptedEntry read after it is
	// counted until the channel is closed.
	entryBlock chan struct{}
	// acceptedMax records the max of every AcceptedEntries read.
	acceptedMax []int
}

func (c *chainFake) sources() Sources {
	return Sources{
		Momenta: fakeMomenta{frontier: func() (uint64, error) { return 1, nil }},
		Anchors: fakeAnchors{
			anchor: func(uint32) (recovery.Anchor, bool, error) { return recovery.Anchor{}, false, nil },
			next: func(uint32) (uint64, error) {
				c.mu.Lock()
				defer c.mu.Unlock()
				return c.next, c.nextErr
			},
			accepted: func(_ uint32, from uint64, max int) (settlement.Accepted, error) {
				c.mu.Lock()
				defer c.mu.Unlock()
				c.acceptedMax = append(c.acceptedMax, max)
				if c.nextErr != nil {
					return settlement.Accepted{}, c.nextErr
				}
				out := settlement.Accepted{NextBatchID: c.next}
				for id := from; id < c.next && len(out.Entries) < max; id++ {
					if e, ok := c.entries[id]; ok {
						out.Entries = append(out.Entries, e)
					}
				}
				return out, nil
			},
			entry: func(_ uint32, id uint64) (*definition.BatchEntry, error) {
				c.mu.Lock()
				c.reads++
				block := c.entryBlock
				c.mu.Unlock()
				if block != nil {
					<-block
				}
				c.mu.Lock()
				defer c.mu.Unlock()
				if c.entryErr != nil {
					return nil, c.entryErr
				}
				e, ok := c.entries[id]
				if !ok {
					return nil, fmt.Errorf("fake: no entry %d", id)
				}
				return e, nil
			},
		},
	}
}

func (c *chainFake) set(f func(c *chainFake)) {
	c.mu.Lock()
	defer c.mu.Unlock()
	f(c)
}

func (c *chainFake) readCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.reads
}

// entryFor is the chain entry that equals cm on every compared field.
func entryFor(t *testing.T, cm codec.BatchCommitment) *definition.BatchEntry {
	t.Helper()
	flow, err := cm.AssetFlowSummary.Encode()
	if err != nil {
		t.Fatalf("Encode asset flow summary: %v", err)
	}
	e := &definition.BatchEntry{
		ProtocolVersion:  cm.Versions.ProtocolVersion,
		FirstInputSeq:    cm.FirstInputSeq,
		LastInputSeq:     cm.LastInputSeq,
		PreStateRoot:     cm.PreStateRoot,
		PostStateRoot:    cm.PostStateRoot,
		InputRoot:        cm.InputRoot,
		ReceiptRoot:      cm.ReceiptRoot,
		EventRoot:        cm.EventRoot,
		OutboxRoot:       cm.OutboxRoot,
		DaHash:           cm.DAHash,
		DaMode:           cm.DAMode,
		AssetFlowSummary: flow,
		ProofData:        cm.ProofData,
		ExecutorPillar:   "p",
		State:            1,
	}
	e.DomainId, e.BatchId = cm.DomainID, cm.BatchID
	return e
}

type cursorHarness struct {
	t     *testing.T
	chain *chainFake
	wEnd  io.ReadWriteCloser
	st    *workerState
	w     WorkerConfig
	exec  types.Address
	done  <-chan error
}

func newCursorHarness(t *testing.T) *cursorHarness {
	t.Helper()
	sEnd, wEnd := newChannel(t)
	h := &cursorHarness{
		t:     t,
		chain: &chainFake{entries: map[uint64]*definition.BatchEntry{}},
		wEnd:  wEnd,
		st:    &workerState{maxHeld: heldCap(defaultMaxPendingBatches), endpoint: "http://znnd.test", withdrawals: testStore(t, 1)},
		w:     WorkerConfig{DomainID: 1},
		exec:  types.Address{0x09},
	}
	// Closing sEnd when serve returns turns a serve failure into an EOF on the worker end, so a
	// blocked read fails the test instead of hanging it.
	done := make(chan error, 1)
	go func() {
		err := serve(sEnd, h.w, h.exec, h.chain.sources(), h.st)
		sEnd.Close()
		done <- err
	}()
	h.done = done
	// The session's opening Retract, which discards nothing these tests hold.
	h.send(ipc.Retract{FromBatchID: math.MaxUint64})
	return h
}

func (h *cursorHarness) commitment(id, lastSeq uint64) codec.BatchCommitment {
	return commitmentFor(h.w.DomainID, h.exec, id, lastSeq)
}

func (h *cursorHarness) send(m ipc.Message) {
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

// cursor sends a CursorRequest and returns the Cursor reply's NextBatchID.
func (h *cursorHarness) cursor() uint64 {
	h.t.Helper()
	h.send(ipc.CursorRequest{})
	m, err := ipc.ReadMessage(h.wEnd, ipc.Supervisor)
	if err != nil {
		h.t.Fatalf("ReadMessage: %v", err)
	}
	c, ok := m.(ipc.Cursor)
	if !ok {
		h.t.Fatalf("reply = %#v, want Cursor", m)
	}
	return c.NextBatchID
}

// waitReads waits until the fake has served n entry reads, for frames that get no reply.
func (h *cursorHarness) waitReads(n int) {
	h.t.Helper()
	waitFor(h.t, 5*time.Second, fmt.Sprintf("%d entry reads", n), func() bool { return h.chain.readCount() >= n })
}

func (h *cursorHarness) sync() { syncRoundTrip(h.t, h.wEnd) }

func (h *cursorHarness) held() map[uint64]bool {
	h.st.mu.Lock()
	defer h.st.mu.Unlock()
	out := map[uint64]bool{}
	for id := range h.st.commitments {
		out[id] = true
	}
	return out
}

func (h *cursorHarness) finish() {
	h.t.Helper()
	h.wEnd.Close()
	if err := waitServe(h.t, h.done); err != nil {
		h.t.Fatalf("serve = %v, want nil", err)
	}
}

func (h *cursorHarness) wantFault(what string) {
	h.t.Helper()
	err := waitServe(h.t, h.done)
	if !errors.Is(err, ErrInconsistent) {
		h.t.Fatalf("%s: serve = %v, want ErrInconsistent", what, err)
	}
	h.st.mu.Lock()
	defer h.st.mu.Unlock()
	if h.st.fault == "" {
		h.t.Fatalf("%s: st.fault is empty", what)
	}
}

// q4Mutations changes one compared field of an entry each.
var q4Mutations = map[string]func(e *definition.BatchEntry){
	"protocolVersion": func(e *definition.BatchEntry) { e.ProtocolVersion++ },
	"firstInputSeq":   func(e *definition.BatchEntry) { e.FirstInputSeq++ },
	"lastInputSeq":    func(e *definition.BatchEntry) { e.LastInputSeq++ },
	"preStateRoot":    func(e *definition.BatchEntry) { e.PreStateRoot[0] ^= 1 },
	"postStateRoot":   func(e *definition.BatchEntry) { e.PostStateRoot[0] ^= 1 },
	"inputRoot":       func(e *definition.BatchEntry) { e.InputRoot[0] ^= 1 },
	"receiptRoot":     func(e *definition.BatchEntry) { e.ReceiptRoot[0] ^= 1 },
	"eventRoot":       func(e *definition.BatchEntry) { e.EventRoot[0] ^= 1 },
	"outboxRoot":      func(e *definition.BatchEntry) { e.OutboxRoot[0] ^= 1 },
	"daHash":          func(e *definition.BatchEntry) { e.DaHash[0] ^= 1 },
	"daMode":          func(e *definition.BatchEntry) { e.DaMode++ },
	"assetFlow":       func(e *definition.BatchEntry) { e.AssetFlowSummary = append([]byte{}, 0xFF) },
	"proofData":       func(e *definition.BatchEntry) { e.ProofData = []byte{1} },
}

func TestServeCursorReplyAndPruning(t *testing.T) {
	h := newCursorHarness(t)
	for id := uint64(0); id < 4; id++ {
		h.send(ipc.Recovered{Commitment: h.commitment(id, id*3+3)})
	}
	h.chain.set(func(c *chainFake) { c.next = 2 })
	if got := h.cursor(); got != 2 {
		t.Fatalf("Cursor = %d, want 2", got)
	}
	if held := h.held(); len(held) != 2 || !held[2] || !held[3] {
		t.Fatalf("held = %v, want batches 2 and 3", held)
	}

	// A Committed below the mark is not held but still counts as progress.
	h.st.mu.Lock()
	h.st.progressed = false
	hw := h.st.highWater
	h.st.mu.Unlock()
	h.send(ipc.Committed{Commitment: h.commitment(0, hw+10)})
	h.sync()
	h.st.mu.Lock()
	if h.st.lastCommitted == nil || h.st.lastCommitted.LastInputSeq != hw+10 || h.st.highWater != hw+10 || !h.st.progressed {
		t.Errorf("lastCommitted %v, highWater %d, progressed %v: want a Committed below the mark to count as progress", h.st.lastCommitted, h.st.highWater, h.st.progressed)
	}
	h.st.mu.Unlock()
	if held := h.held(); len(held) != 2 {
		t.Fatalf("held = %v, want the Committed below the mark ignored", held)
	}

	// A conflicting Recovered below the mark changes nothing and is not a fault.
	other := h.commitment(0, 99)
	other.DAHash = types.Hash{0xEE}
	h.send(ipc.Retract{FromBatchID: 4})
	h.send(ipc.Recovered{Commitment: other})
	h.sync()
	h.st.mu.Lock()
	if h.st.highWater != hw+10 || h.st.fault != "" || len(h.st.commitments) != 2 {
		t.Errorf("a Recovered below the mark changed state: highWater %d, fault %q, held %d", h.st.highWater, h.st.fault, len(h.st.commitments))
	}
	h.st.mu.Unlock()
	h.finish()
}

func TestServeCursorSourceError(t *testing.T) {
	h := newCursorHarness(t)
	h.chain.set(func(c *chainFake) { c.nextErr = fmt.Errorf("%w: bad", recovery.ErrHalted) })
	h.send(ipc.CursorRequest{})
	m, err := ipc.ReadMessage(h.wEnd, ipc.Supervisor)
	if err != nil {
		t.Fatalf("ReadMessage: %v", err)
	}
	if se, ok := m.(ipc.SourceError); !ok || se.Kind != ipc.SourceHalted {
		t.Fatalf("reply = %#v, want a halted SourceError", m)
	}
	h.finish()
}

func TestServeAnchorRequestObservesNext(t *testing.T) {
	h := newCursorHarness(t)
	for id := uint64(0); id < 3; id++ {
		h.send(ipc.Recovered{Commitment: h.commitment(id, id+1)})
	}
	h.chain.set(func(c *chainFake) { c.next = 2 })
	h.send(ipc.AnchorRequest{})
	if _, err := ipc.ReadMessage(h.wEnd, ipc.Supervisor); err != nil {
		t.Fatalf("ReadMessage: %v", err)
	}
	if held := h.held(); len(held) != 1 || !held[2] {
		t.Fatalf("held = %v, want batch 2 only", held)
	}
	h.finish()
}

func TestServeTipComparison(t *testing.T) {
	for _, frame := range []string{"Committed", "Recovered"} {
		send := func(h *cursorHarness, c codec.BatchCommitment) {
			if frame == "Committed" {
				h.send(ipc.Committed{Commitment: c})
			} else {
				h.send(ipc.Retract{FromBatchID: c.BatchID + 1})
				h.send(ipc.Recovered{Commitment: c})
			}
		}

		t.Run(frame+" equal entry passes and is read once", func(t *testing.T) {
			h := newCursorHarness(t)
			c := h.commitment(2, 6)
			h.chain.set(func(ch *chainFake) { ch.next = 3; ch.entries[2] = entryFor(t, c) })
			h.cursor()
			send(h, c)
			send(h, c)
			h.sync()
			if n := h.chain.readCount(); n != 1 {
				t.Fatalf("entry reads = %d, want 1", n)
			}
			h.finish()
		})

		for name, mutate := range q4Mutations {
			t.Run(frame+" differing "+name+" faults", func(t *testing.T) {
				h := newCursorHarness(t)
				c := h.commitment(2, 6)
				e := entryFor(t, c)
				mutate(e)
				h.chain.set(func(ch *chainFake) { ch.next = 3; ch.entries[2] = e })
				h.cursor()
				send(h, c)
				h.wantFault(name)
			})
		}

		t.Run(frame+" skipped fields do not fault", func(t *testing.T) {
			h := newCursorHarness(t)
			c := h.commitment(2, 6)
			e := entryFor(t, c)
			e.SubmittedAtHeight, e.FinalizeAtHeight, e.ExecutorPillar, e.State = 77, 88, "other", 9
			h.chain.set(func(ch *chainFake) { ch.next = 3; ch.entries[2] = e })
			h.cursor()
			send(h, c)
			h.sync()
			h.finish()
		})

		t.Run(frame+" reads again after the cursor rises", func(t *testing.T) {
			h := newCursorHarness(t)
			c2, c4 := h.commitment(2, 6), h.commitment(4, 12)
			h.chain.set(func(ch *chainFake) { ch.next = 3; ch.entries[2] = entryFor(t, c2); ch.entries[4] = entryFor(t, c4) })
			h.cursor()
			send(h, c2)
			h.sync()
			h.chain.set(func(ch *chainFake) { ch.next = 5 })
			h.cursor()
			send(h, c4)
			send(h, c4)
			h.sync()
			if n := h.chain.readCount(); n != 2 {
				t.Fatalf("entry reads = %d, want 2", n)
			}
			h.finish()
		})

		t.Run(frame+" makes no read while the mark is 0", func(t *testing.T) {
			h := newCursorHarness(t)
			send(h, h.commitment(0, 3))
			h.sync()
			if n := h.chain.readCount(); n != 0 {
				t.Fatalf("entry reads = %d, want 0", n)
			}
			h.finish()
		})

		t.Run(frame+" unreadable accepted entry faults", func(t *testing.T) {
			h := newCursorHarness(t)
			h.chain.set(func(ch *chainFake) { ch.next = 3; ch.entryErr = fmt.Errorf("%w: gone", recovery.ErrHalted) })
			h.cursor()
			send(h, h.commitment(2, 6))
			h.wantFault("halted read")
		})
	}
}

func TestServeBootComparison(t *testing.T) {
	h := newCursorHarness(t)
	c := h.commitment(2, 6)
	e := entryFor(t, c)
	e.OutboxRoot[0] ^= 1
	h.chain.set(func(ch *chainFake) { ch.next = 3; ch.entries[2] = e })
	h.send(ipc.AnchorRequest{})
	if _, err := ipc.ReadMessage(h.wEnd, ipc.Supervisor); err != nil {
		t.Fatalf("ReadMessage: %v", err)
	}
	h.send(ipc.Retract{FromBatchID: 3})
	h.send(ipc.Recovered{Commitment: c})
	h.wantFault("boot path")
}

func TestServeEntryMismatchKeepsFirstFault(t *testing.T) {
	h := newCursorHarness(t)
	c := h.commitment(2, 6)
	e := entryFor(t, c)
	e.OutboxRoot[0] ^= 1
	h.chain.set(func(ch *chainFake) { ch.next = 3; ch.entries[2] = e })
	h.cursor()
	const preset = "supervisor: consistency fault: preset"
	h.st.mu.Lock()
	h.st.fault = preset
	h.st.mu.Unlock()
	h.send(ipc.Committed{Commitment: c})

	err := waitServe(t, h.done)
	if !errors.Is(err, ErrInconsistent) || !strings.Contains(err.Error(), "differs from Settlement's accepted entry") {
		t.Fatalf("serve = %v, want ErrInconsistent naming the accepted entry", err)
	}
	h.st.mu.Lock()
	defer h.st.mu.Unlock()
	if h.st.fault != preset {
		t.Fatalf("fault = %q, want the first fault %q", h.st.fault, preset)
	}
}

func TestServeOwedComparison(t *testing.T) {
	t.Run("an outage leaves the comparison to the next request frame", func(t *testing.T) {
		h := newCursorHarness(t)
		c := h.commitment(2, 6)
		e := entryFor(t, c)
		e.DaMode++
		h.chain.set(func(ch *chainFake) { ch.next = 3; ch.entryErr = errors.New("node down"); ch.entries[2] = e })
		h.cursor()
		h.send(ipc.Committed{Commitment: c})
		// The tip check and the hand-off each make one failing read.
		h.waitReads(2)
		// The next request frame retries the read.
		h.chain.set(func(ch *chainFake) { ch.entryErr = nil })
		h.send(ipc.FrontierRequest{})
		h.wantFault("owed comparison")
		if n := h.chain.readCount(); n != 3 {
			t.Fatalf("entry reads = %d, want 3", n)
		}
	})

	t.Run("an owed comparison that passes clears", func(t *testing.T) {
		h := newCursorHarness(t)
		c := h.commitment(2, 6)
		h.chain.set(func(ch *chainFake) {
			ch.next = 3
			ch.entryErr = errors.New("node down")
			ch.entries[2] = entryFor(t, c)
		})
		h.cursor()
		h.send(ipc.Recovered{Commitment: c})
		h.waitReads(2)
		h.chain.set(func(ch *chainFake) { ch.entryErr = nil })
		h.sync()
		if v, ok := h.st.withdrawals.Lookup(2); !ok || !v {
			t.Fatalf("batch 2 stored (verified, ok) = (%v, %v), want a verified file", v, ok)
		}
		h.sync()
		if n := h.chain.readCount(); n != 3 {
			t.Fatalf("entry reads = %d, want 3", n)
		}
		h.finish()
	})
}

// lockedBuffer is a log destination that serve's goroutine and the test both touch.
type lockedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func captureLog(t *testing.T) *lockedBuffer {
	t.Helper()
	b := &lockedBuffer{}
	log.SetOutput(b)
	t.Cleanup(func() { log.SetOutput(io.Discard) })
	return b
}

func TestServeLaggingRead(t *testing.T) {
	t.Run("the mark does not fall and the read is logged", func(t *testing.T) {
		out := captureLog(t)
		h := newCursorHarness(t)
		for id := uint64(0); id < 6; id++ {
			h.send(ipc.Recovered{Commitment: h.commitment(id, id+1)})
		}
		h.chain.set(func(c *chainFake) { c.next = 5 })
		if got := h.cursor(); got != 5 {
			t.Fatalf("Cursor = %d, want 5", got)
		}
		if held := h.held(); len(held) != 1 || !held[5] {
			t.Fatalf("held = %v, want batch 5 only", held)
		}
		h.chain.set(func(c *chainFake) { c.next = 3 })
		if got := h.cursor(); got != 5 {
			t.Fatalf("lagging Cursor = %d, want the observed 5", got)
		}
		lines := strings.Count(out.String(), "WARN")
		want := "supervisor: domain 1 WARN: znnd node at http://znnd.test reported nextBatchId 3, below the observed 5; keeping the observed value"
		if lines != 1 || !strings.Contains(out.String(), want) {
			t.Fatalf("log = %q, want one line %q", out.String(), want)
		}

		// A Recovered below the mark is not held.
		h.send(ipc.Retract{FromBatchID: 6})
		h.send(ipc.Recovered{Commitment: h.commitment(3, 4)})
		h.sync()
		if held := h.held(); held[3] {
			t.Fatalf("held = %v, want batch 3 ignored", held)
		}
		h.finish()
	})

	t.Run("a comparison still owed survives a lagging read", func(t *testing.T) {
		captureLog(t)
		h := newCursorHarness(t)
		c := h.commitment(4, 8)
		h.chain.set(func(ch *chainFake) { ch.next = 5; ch.entryErr = errors.New("node down") })
		h.cursor()
		h.send(ipc.Recovered{Commitment: c})
		h.waitReads(2)
		h.chain.set(func(ch *chainFake) { ch.next = 3 })
		// The lagging CursorRequest's own retry also fails.
		if got := h.cursor(); got != 5 {
			t.Fatalf("Cursor = %d, want 5", got)
		}
		e := entryFor(t, c)
		e.PostStateRoot[0] ^= 1
		h.chain.set(func(ch *chainFake) { ch.entryErr = nil; ch.entries[4] = e })
		reads := h.chain.readCount()
		h.send(ipc.FrontierRequest{})
		h.wantFault("owed after a lagging read")
		if n := h.chain.readCount(); n != reads+1 {
			t.Fatalf("entry reads = %d, want %d", n, reads+1)
		}
	})
}

func TestHeldCap(t *testing.T) {
	for _, tc := range []struct {
		pending uint64
		want    int
	}{{1, 2049}, {8, 2056}, {64, 2112}} {
		if got := heldCap(tc.pending); got != tc.want {
			t.Fatalf("heldCap(%d) = %d, want %d (pending batches plus two worker buffers)", tc.pending, got, tc.want)
		}
	}
}

func TestLaggingReadLogOmitsEndpointCredentials(t *testing.T) {
	out := captureLog(t)
	cfg := Config{
		ZnndEndpoint:      "https://user:s3cret@znnd.test:35997/rpc/path?apikey=tok3n",
		MaxPendingBatches: defaultMaxPendingBatches,
		Workers:           []WorkerConfig{{DomainID: 1}},
	}
	st := New(cfg, nil, Sources{}, Policy{}).slots[0].st
	observeNext(1, 5, st)
	observeNext(1, 3, st)

	got := out.String()
	if !strings.Contains(got, "znnd node at https://znnd.test:35997 reported") {
		t.Fatalf("log = %q, want the scheme and host", got)
	}
	for _, secret := range []string{"user", "s3cret", "apikey", "tok3n", "/rpc/path"} {
		if strings.Contains(got, secret) {
			t.Fatalf("log = %q, must not contain %q", got, secret)
		}
	}
}

// accepted sends an AcceptedRequest and returns the reply.
func (h *cursorHarness) accepted(from uint64) ipc.Message {
	h.t.Helper()
	h.send(ipc.AcceptedRequest{FromBatchID: from})
	m, err := ipc.ReadMessage(h.wEnd, ipc.Supervisor)
	if err != nil {
		h.t.Fatalf("ReadMessage: %v", err)
	}
	return m
}

func (h *cursorHarness) entriesTo(n uint64) {
	h.chain.set(func(c *chainFake) {
		for id := uint64(0); id < n; id++ {
			c.entries[id] = entryFor(h.t, h.commitment(id, id+1))
		}
	})
}

func TestServeAcceptedReplyAndPruning(t *testing.T) {
	h := newCursorHarness(t)
	const next = 70
	h.entriesTo(next)
	h.chain.set(func(c *chainFake) { c.next = next })
	for id := uint64(0); id < 4; id++ {
		h.send(ipc.Recovered{Commitment: h.commitment(id+next-2, id+next)})
	}

	m := h.accepted(0)
	a, ok := m.(ipc.Accepted)
	if !ok {
		t.Fatalf("reply = %#v, want Accepted", m)
	}
	if a.NextBatchID != next || len(a.Entries) != 64 {
		t.Fatalf("Accepted: nextBatchId %d with %d entries, want %d with 64", a.NextBatchID, len(a.Entries), next)
	}
	for i, e := range a.Entries {
		if e.BatchId != uint64(i) {
			t.Fatalf("entry %d has batchId %d", i, e.BatchId)
		}
	}
	h.chain.mu.Lock()
	maxs := append([]int(nil), h.chain.acceptedMax...)
	h.chain.mu.Unlock()
	if len(maxs) != 1 || maxs[0] != 64 {
		t.Fatalf("max passed to the source = %v, want [64]", maxs)
	}
	if held := h.held(); len(held) != 2 || !held[next] || !held[next+1] {
		t.Fatalf("held = %v, want batches %d and %d (those below the read pruned)", held, next, next+1)
	}

	m = h.accepted(next)
	if a, ok := m.(ipc.Accepted); !ok || a.NextBatchID != next || len(a.Entries) != 0 {
		t.Fatalf("reply at nextBatchId = %#v, want an empty Accepted", m)
	}
	h.finish()
}

func TestServeAcceptedLaggingRead(t *testing.T) {
	out := captureLog(t)
	h := newCursorHarness(t)
	h.entriesTo(8)
	h.chain.set(func(c *chainFake) { c.next = 10 })
	if got := h.cursor(); got != 10 {
		t.Fatalf("Cursor = %d, want 10", got)
	}
	h.chain.set(func(c *chainFake) { c.next = 8 })

	m := h.accepted(8)
	if se, ok := m.(ipc.SourceError); !ok || se.Kind != ipc.SourceUnavailable {
		t.Fatalf("from 8 = %#v, want an unavailable SourceError", m)
	}
	m = h.accepted(5)
	a, ok := m.(ipc.Accepted)
	if !ok || a.NextBatchID != 10 || len(a.Entries) != 3 || a.Entries[0].BatchId != 5 || a.Entries[2].BatchId != 7 {
		t.Fatalf("from 5 = %#v, want Accepted{NextBatchID: 10} with entries 5..7", m)
	}
	if n := strings.Count(out.String(), "WARN"); n != 2 {
		t.Fatalf("log has %d WARN lines, want 2 (one per lagging read): %q", n, out.String())
	}
	h.finish()
}

func TestServeAcceptedSourceError(t *testing.T) {
	t.Run("a halted source replies SourceHalted", func(t *testing.T) {
		h := newCursorHarness(t)
		h.chain.set(func(c *chainFake) { c.nextErr = fmt.Errorf("%w: bad", recovery.ErrHalted) })
		if se, ok := h.accepted(0).(ipc.SourceError); !ok || se.Kind != ipc.SourceHalted {
			t.Fatalf("reply is not a halted SourceError")
		}
		h.finish()
	})

	t.Run("a plain error replies unavailable", func(t *testing.T) {
		h := newCursorHarness(t)
		h.chain.set(func(c *chainFake) { c.nextErr = errors.New("node down") })
		if se, ok := h.accepted(0).(ipc.SourceError); !ok || se.Kind != ipc.SourceUnavailable {
			t.Fatalf("reply is not an unavailable SourceError")
		}
		h.finish()
	})

	t.Run("an owed comparison settles before the reply", func(t *testing.T) {
		h := newCursorHarness(t)
		c := h.commitment(2, 6)
		e := entryFor(t, c)
		e.DaMode++
		h.chain.set(func(ch *chainFake) { ch.next = 3; ch.entryErr = errors.New("node down"); ch.entries[2] = e })
		h.cursor()
		h.send(ipc.Committed{Commitment: c})
		h.waitReads(2)
		h.chain.set(func(ch *chainFake) { ch.entryErr = nil })
		h.send(ipc.AcceptedRequest{FromBatchID: 0})
		h.wantFault("owed comparison before an accepted reply")
		if m, err := ipc.ReadMessage(h.wEnd, ipc.Supervisor); err == nil {
			t.Fatalf("read %#v after the fault, want no reply", m)
		}
	})
}
