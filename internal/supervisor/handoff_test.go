package supervisor

import (
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/zenon-network/go-zenon/common/types"

	"github.com/Zeno-L2/zeno-executor/internal/codec"
	"github.com/Zeno-L2/zeno-executor/internal/ipc"
	"github.com/Zeno-L2/zeno-executor/internal/recovery"
)

// --- helpers ---

var testSource = types.Hash{0x01}

// twoMessages is a batch of two withdrawals in different assets.
func twoMessages(id uint64) []codec.OutboxMessage {
	return []codec.OutboxMessage{
		l1msg(1, 0, assetN(1), 5+id, testSource),
		l1msg(2, 0, assetN(2), 7+id, testSource),
	}
}

// threeMessages is a batch of three withdrawals in two assets.
func threeMessages(id uint64) []codec.OutboxMessage {
	return []codec.OutboxMessage{
		l1msg(1, 0, assetN(1), 5+id, testSource),
		l1msg(2, 0, assetN(1), 6+id, testSource),
		l1msg(2, 1, assetN(2), 7+id, testSource),
	}
}

// batch builds batch id's commitment, covering input sequences 1 to
// last, and its withdrawals.
func (h *cursorHarness) batch(id, last uint64, msgs []codec.OutboxMessage) (codec.BatchCommitment, []ipc.Withdrawal) {
	h.t.Helper()
	c := h.commitment(id, last)
	return c, withdrawalBatch(h.t, &c, msgs)
}

// handOff sends report and its withdrawals in one write.
func (h *cursorHarness) handOff(report ipc.Message, ws ...ipc.Withdrawal) {
	h.t.Helper()
	writeHandOff(h.t, h.wEnd, report, ws...)
}

// syncWithin sends a FrontierRequest and fails the test when no reply
// arrives within d.
func (h *cursorHarness) syncWithin(d time.Duration) {
	h.t.Helper()
	h.send(ipc.FrontierRequest{})
	got := make(chan error, 1)
	go func() {
		_, err := ipc.ReadMessage(h.wEnd, ipc.Supervisor)
		got <- err
	}()
	select {
	case err := <-got:
		if err != nil {
			h.t.Fatalf("ReadMessage: %v", err)
		}
	case <-time.After(d):
		h.t.Fatalf("no reply within %s", d)
	}
}

// progress reads the progress fields a hand-off may change.
func (h *cursorHarness) progress() (last *codec.BatchCommitment, high uint64, progressed bool) {
	h.st.mu.Lock()
	defer h.st.mu.Unlock()
	return h.st.lastCommitted, h.st.highWater, h.st.progressed
}

func (h *cursorHarness) wantNoProgress() {
	h.t.Helper()
	if last, high, progressed := h.progress(); last != nil || high != 0 || progressed {
		h.t.Fatalf("lastCommitted = %v, highWater = %d, progressed = %v, want none", last, high, progressed)
	}
}

func (h *cursorHarness) wantStored(id uint64, verified bool) StoredBatch {
	h.t.Helper()
	b := mustBatch(h.t, h.st.withdrawals, id)
	if b.Verified != verified {
		h.t.Fatalf("batch %d verified = %v, want %v", id, b.Verified, verified)
	}
	return b
}

func (h *cursorHarness) wantNoFile(id uint64) {
	h.t.Helper()
	if _, ok := h.st.withdrawals.Lookup(id); ok {
		h.t.Fatalf("batch %d has a file, want none", id)
	}
}

func (h *cursorHarness) wantReads(n int) {
	h.t.Helper()
	if got := h.chain.readCount(); got != n {
		h.t.Fatalf("entry reads = %d, want %d", got, n)
	}
}

func unverifiedOf(c codec.BatchCommitment, ws []ipc.Withdrawal) StoredBatch {
	return StoredBatch{DomainID: c.DomainID, BatchID: c.BatchID, Withdrawals: storedOf(ws)}
}

func wantStoredEqual(t *testing.T, got StoredBatch, ws []ipc.Withdrawal) {
	t.Helper()
	if len(got.Withdrawals) != len(ws) {
		t.Fatalf("stored %d withdrawals, want %d", len(got.Withdrawals), len(ws))
	}
	for i, w := range ws {
		if string(got.Withdrawals[i].Message) != string(w.Message) || string(got.Withdrawals[i].Proof) != string(w.Proof) {
			t.Fatalf("stored withdrawal %d differs from the frame sent", i)
		}
	}
}

func countOf(buf *lockedBuffer, sub string) int { return strings.Count(buf.String(), sub) }

// --- verify ---

func TestHandOffVerifiesAboveTheMark(t *testing.T) {
	h := newCursorHarness(t)
	c, ws := h.batch(1, 2, []codec.OutboxMessage{
		kind0msg(1, 0),
		l1msg(1, 1, assetN(1), 5, testSource),
		l1msg(2, 0, assetN(2), 0, testSource),
		l1msg(2, 1, assetN(1), 3, types.Hash{}),
	})
	if len(ws) != 3 || len(c.AssetFlowSummary) != 1 || c.AssetFlowSummary[0].WithdrawalDebit != u256Of(8) {
		t.Fatalf("fixture: %d withdrawals, summary %v", len(ws), c.AssetFlowSummary)
	}
	h.handOff(ipc.Committed{Commitment: c}, ws...)
	h.sync()

	b := h.wantStored(1, true)
	if b.OutboxRoot != c.OutboxRoot {
		t.Fatalf("stored root = %v, want %v", b.OutboxRoot, c.OutboxRoot)
	}
	wantStoredEqual(t, b, ws)
	if held := h.held(); len(held) != 1 || !held[1] {
		t.Fatalf("held = %v, want batch 1", held)
	}
	if last, _, progressed := h.progress(); last == nil || last.BatchID != 1 || !progressed {
		t.Fatalf("lastCommitted = %v, progressed = %v, want batch 1 and progress", last, progressed)
	}
	h.finish()
}

func TestHandOffFaultsAboveTheMark(t *testing.T) {
	other := func(h *cursorHarness) []ipc.Withdrawal {
		_, ws := h.batch(9, 3, []codec.OutboxMessage{l1msg(1, 0, assetN(1), 1, types.Hash{0x07}), l1msg(2, 0, assetN(1), 2, types.Hash{0x07})})
		return ws
	}
	cases := []struct {
		name string
		prep func(h *cursorHarness, c *codec.BatchCommitment, ws []ipc.Withdrawal) []ipc.Withdrawal
	}{
		{"a flipped proof sibling byte", func(h *cursorHarness, c *codec.BatchCommitment, ws []ipc.Withdrawal) []ipc.Withdrawal {
			ws[1].Proof[len(ws[1].Proof)-1] ^= 1
			return ws
		}},
		{"a valid proof from another tree", func(h *cursorHarness, c *codec.BatchCommitment, ws []ipc.Withdrawal) []ipc.Withdrawal {
			ws[0].Proof = other(h)[0].Proof
			return ws
		}},
		{"an inputIndex outside the range", func(h *cursorHarness, c *codec.BatchCommitment, ws []ipc.Withdrawal) []ipc.Withdrawal {
			c.LastInputSeq = 1
			return ws
		}},
		{"a repeated frame", func(h *cursorHarness, c *codec.BatchCommitment, ws []ipc.Withdrawal) []ipc.Withdrawal {
			return []ipc.Withdrawal{ws[0], ws[0], ws[1], ws[2]}
		}},
		{"two frames reversed", func(h *cursorHarness, c *codec.BatchCommitment, ws []ipc.Withdrawal) []ipc.Withdrawal {
			return []ipc.Withdrawal{ws[1], ws[0], ws[2]}
		}},
		{"a non-zero withdrawal left out", func(h *cursorHarness, c *codec.BatchCommitment, ws []ipc.Withdrawal) []ipc.Withdrawal {
			return []ipc.Withdrawal{ws[0], ws[2]}
		}},
		{"a summary debit for an asset with no withdrawal", func(h *cursorHarness, c *codec.BatchCommitment, ws []ipc.Withdrawal) []ipc.Withdrawal {
			c.AssetFlowSummary = append(c.AssetFlowSummary, codec.AssetFlowEntry{Asset: assetN(9), WithdrawalDebit: u256Of(1)})
			return ws
		}},
		{"an empty hand-off against a non-zero debit", func(h *cursorHarness, c *codec.BatchCommitment, ws []ipc.Withdrawal) []ipc.Withdrawal {
			return nil
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			captureLog(t)
			h := newCursorHarness(t)
			c, ws := h.batch(1, 3, threeMessages(0))
			ws = tc.prep(h, &c, ws)
			h.handOff(ipc.Committed{Commitment: c}, ws...)
			h.wantFault(tc.name)
			h.wantNoFile(1)
			if held := h.held(); len(held) != 0 {
				t.Fatalf("held = %v, want none", held)
			}
			h.wantNoProgress()
		})
	}
}

func TestHandOffEOFStoresNothing(t *testing.T) {
	h := newCursorHarness(t)
	c, ws := h.batch(1, 3, threeMessages(0))
	if err := ipc.WriteMessage(h.wEnd, ipc.Committed{Commitment: c}); err != nil {
		t.Fatal(err)
	}
	for _, w := range ws[:2] {
		if err := ipc.WriteMessage(h.wEnd, w); err != nil {
			t.Fatal(err)
		}
	}
	h.finish()
	h.wantNoFile(1)
	if held := h.held(); len(held) != 0 {
		t.Fatalf("held = %v, want none", held)
	}
	h.wantNoProgress()
}

func TestHandOffMalformed(t *testing.T) {
	t.Run("more withdrawals than the limit", func(t *testing.T) {
		h := newCursorHarness(t)
		msgs := make([]codec.OutboxMessage, ipc.MaxHandOffWithdrawals+1)
		for i := range msgs {
			msgs[i] = l1msg(uint64(1+i/33), uint32(i%33), assetN(1), 1, testSource)
		}
		c, ws := h.batch(1, 1+uint64(len(msgs))/33, msgs)
		if err := ipc.WriteMessage(h.wEnd, ipc.Committed{Commitment: c}); err != nil {
			t.Fatal(err)
		}
		for _, w := range ws {
			if err := ipc.WriteMessage(h.wEnd, w); err != nil {
				t.Fatal(err)
			}
		}
		err := waitServe(t, h.done)
		if !errors.Is(err, ErrMalformed) || errors.Is(err, ErrInconsistent) || !strings.Contains(err.Error(), fmt.Sprint(ipc.MaxHandOffWithdrawals)) {
			t.Fatalf("serve = %v, want ErrMalformed naming the limit", err)
		}
		h.wantNoFile(1)
		h.st.mu.Lock()
		defer h.st.mu.Unlock()
		if h.st.fault != "" {
			t.Fatalf("fault = %q, want none", h.st.fault)
		}
	})

	t.Run("a kind 0 message in a Withdrawal frame", func(t *testing.T) {
		h := newCursorHarness(t)
		c, _ := h.batch(1, 3, threeMessages(0))
		if err := ipc.WriteMessage(h.wEnd, ipc.Committed{Commitment: c}); err != nil {
			t.Fatal(err)
		}
		msg, err := kind0msg(1, 0).Encode()
		if err != nil {
			t.Fatal(err)
		}
		payload := binary.BigEndian.AppendUint64(nil, 1)
		payload = append(payload, msg...)
		payload = append(payload, 0, 0, 0, 0)
		if _, err := h.wEnd.Write(rawFrame(0x8B, payload)); err != nil {
			t.Fatal(err)
		}
		if err := waitServe(t, h.done); !errors.Is(err, ErrMalformed) {
			t.Fatalf("serve = %v, want ErrMalformed", err)
		}
		h.wantNoFile(1)
	})
}

// --- compare ---

func TestHandOffComparesWithAVerifiedFile(t *testing.T) {
	setup := func(t *testing.T) (*cursorHarness, codec.BatchCommitment, []ipc.Withdrawal) {
		captureLog(t)
		h := newCursorHarness(t)
		c, ws := h.batch(1, 3, threeMessages(0))
		h.handOff(ipc.Committed{Commitment: c}, ws...)
		h.sync()
		h.chain.set(func(ch *chainFake) { ch.next = 3 })
		h.cursor()
		h.send(ipc.Retract{FromBatchID: 2})
		return h, c, ws
	}

	t.Run("an identical re-send passes without a read", func(t *testing.T) {
		h, c, ws := setup(t)
		h.handOff(ipc.Recovered{Commitment: c}, ws...)
		h.sync()
		h.wantReads(0)
		h.wantStored(1, true)
		h.finish()
	})

	cases := []struct {
		name string
		edit func(ws []ipc.Withdrawal) []ipc.Withdrawal
	}{
		{"a changed proof byte", func(ws []ipc.Withdrawal) []ipc.Withdrawal {
			ws[0].Proof[len(ws[0].Proof)-1] ^= 1
			return ws
		}},
		{"a changed message", func(ws []ipc.Withdrawal) []ipc.Withdrawal {
			m, err := l1msg(1, 0, assetN(1), 99, testSource).Encode()
			if err != nil {
				t.Fatal(err)
			}
			ws[0].Message = m
			return ws
		}},
		{"a missing last withdrawal", func(ws []ipc.Withdrawal) []ipc.Withdrawal { return ws[:2] }},
		{"an extra withdrawal", func(ws []ipc.Withdrawal) []ipc.Withdrawal {
			m, err := l1msg(3, 0, assetN(1), 1, testSource).Encode()
			if err != nil {
				t.Fatal(err)
			}
			return append(ws, ipc.Withdrawal{BatchID: 1, Message: m, Proof: ws[0].Proof})
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h, c, ws := setup(t)
			h.handOff(ipc.Recovered{Commitment: c}, tc.edit(ws)...)
			h.wantFault(tc.name)
		})
	}
}

func TestHandOffVerifiesAgainstSettlementBelowTheMark(t *testing.T) {
	// A batch below nextBatchId - 1 with no file is verified against
	// Settlement's accepted entry.
	setup := func(t *testing.T) (*cursorHarness, codec.BatchCommitment, []ipc.Withdrawal) {
		captureLog(t)
		h := newCursorHarness(t)
		c, ws := h.batch(1, 3, threeMessages(0))
		h.chain.set(func(ch *chainFake) { ch.next = 4; ch.entries[1] = entryFor(t, c) })
		h.cursor()
		return h, c, ws
	}

	t.Run("a matching entry stores the batch verified and holds nothing", func(t *testing.T) {
		h, c, ws := setup(t)
		h.handOff(ipc.Recovered{Commitment: c}, ws...)
		h.sync()
		wantStoredEqual(t, h.wantStored(1, true), ws)
		h.wantReads(1)
		if held := h.held(); len(held) != 0 {
			t.Fatalf("held = %v, want none", held)
		}
		h.finish()
	})

	cases := []struct {
		name string
		edit func(h *cursorHarness, c codec.BatchCommitment)
	}{
		{"another tree's root", func(h *cursorHarness, c codec.BatchCommitment) {
			h.chain.set(func(ch *chainFake) { ch.entries[1].OutboxRoot = types.Hash{0x77} })
		}},
		{"a different debit", func(h *cursorHarness, c codec.BatchCommitment) {
			c.AssetFlowSummary[0].WithdrawalDebit = u256Of(1)
			h.chain.set(func(ch *chainFake) { ch.entries[1] = entryFor(h.t, c) })
		}},
		{"a range excluding an inputIndex", func(h *cursorHarness, c codec.BatchCommitment) {
			h.chain.set(func(ch *chainFake) { ch.entries[1].LastInputSeq = 1 })
		}},
		{"a halted read", func(h *cursorHarness, c codec.BatchCommitment) {
			h.chain.set(func(ch *chainFake) { ch.entryErr = fmt.Errorf("%w: gone", recovery.ErrHalted) })
		}},
		{"an undecodable asset flow summary", func(h *cursorHarness, c codec.BatchCommitment) {
			h.chain.set(func(ch *chainFake) { ch.entries[1].AssetFlowSummary = []byte{0xFF} })
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h, c, ws := setup(t)
			tc.edit(h, c)
			h.handOff(ipc.Recovered{Commitment: c}, ws...)
			h.wantFault(tc.name)
			h.wantNoFile(1)
		})
	}
}

// --- unverified ---

// unverifiedHarness stores batch 1 unverified through an outage on
// Settlement's read, with the retry held back. The entry for batch 1 is
// in the fake but unreadable.
func unverifiedHarness(t *testing.T) (*cursorHarness, codec.BatchCommitment, []ipc.Withdrawal, *lockedBuffer) {
	t.Helper()
	logs := captureLog(t)
	h := newCursorHarness(t)
	holdRetries(h.st, true)
	c, ws := h.batch(1, 3, twoMessages(0))
	h.chain.set(func(ch *chainFake) {
		ch.next = 4
		ch.entryErr = errors.New("node down")
		ch.entries[1] = entryFor(t, c)
	})
	h.cursor()
	h.handOff(ipc.Recovered{Commitment: c}, ws...)
	h.sync()
	return h, c, ws, logs
}

func TestHandOffStoresUnverifiedDuringAnOutage(t *testing.T) {
	h, c, ws, logs := unverifiedHarness(t)
	want := "supervisor: domain 1 WARN: batch 1 withdrawals stored unverified: Settlement read: node down; the supervisor checks them once Settlement answers"
	if n := countOf(logs, want); n != 1 {
		t.Fatalf("log has the warning %d times, want once:\n%s", n, logs.String())
	}
	b := h.wantStored(1, false)
	if b.OutboxRoot != (types.Hash{}) {
		t.Fatalf("stored root = %v, want zero", b.OutboxRoot)
	}
	wantStoredEqual(t, b, ws)
	if refs := h.st.withdrawals.Batches(); len(refs) != 1 || refs[0].Verified {
		t.Fatalf("Batches = %v, want batch 1 unverified", refs)
	}
	if held := h.held(); len(held) != 0 {
		t.Fatalf("held = %v, want none", held)
	}
	h.wantNoProgress()

	// An identical re-send compares with the file without a read and
	// changes no progress.
	reads := h.chain.readCount()
	h.send(ipc.Retract{FromBatchID: 4})
	h.handOff(ipc.Recovered{Commitment: c}, ws...)
	h.sync()
	h.wantReads(reads)
	h.wantNoProgress()

	// A changed proof is a fault.
	changed := append([]ipc.Withdrawal{}, ws...)
	changed[0].Proof = append([]byte{}, ws[0].Proof...)
	changed[0].Proof[len(changed[0].Proof)-1] ^= 1
	h.send(ipc.Retract{FromBatchID: 4})
	h.handOff(ipc.Recovered{Commitment: c}, changed...)
	h.wantFault("changed proof against an unverified file")
}

func TestRetryVerifiesAnUnverifiedFile(t *testing.T) {
	h, c, _, _ := unverifiedHarness(t)
	reads := h.chain.readCount()
	holdRetries(h.st, false)
	h.chain.set(func(ch *chainFake) { ch.entryErr = nil })
	h.sync()
	b := h.wantStored(1, true)
	if b.OutboxRoot != c.OutboxRoot {
		t.Fatalf("verified root = %v, want the entry's %v", b.OutboxRoot, c.OutboxRoot)
	}
	h.wantReads(reads + 1)
	h.sync()
	h.wantReads(reads + 1)
	h.finish()
}

func TestRetryFindsAMismatch(t *testing.T) {
	cases := []struct {
		name string
		edit func(t *testing.T, ch *chainFake, c codec.BatchCommitment)
	}{
		{"another tree's root", func(t *testing.T, ch *chainFake, c codec.BatchCommitment) {
			ch.entries[1].OutboxRoot = types.Hash{0x77}
		}},
		{"a different debit", func(t *testing.T, ch *chainFake, c codec.BatchCommitment) {
			c.AssetFlowSummary[0].WithdrawalDebit = u256Of(1)
			ch.entries[1] = entryFor(t, c)
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h, c, _, _ := unverifiedHarness(t)
			holdRetries(h.st, false)
			h.chain.set(func(ch *chainFake) { ch.entryErr = nil; tc.edit(t, ch, c) })
			h.send(ipc.FrontierRequest{})
			h.wantFault(tc.name)
			h.st.mu.Lock()
			if !strings.Contains(h.st.fault, "batch 1") {
				t.Fatalf("fault = %q, want it to name batch 1", h.st.fault)
			}
			h.st.mu.Unlock()
			h.wantStored(1, false)
		})
	}
}

// retryHarness stores unverified batches 1 and 2 directly, with both
// entries readable.
func retryHarness(t *testing.T) (*cursorHarness, *lockedBuffer) {
	t.Helper()
	logs := captureLog(t)
	h := newCursorHarness(t)
	for id := uint64(1); id <= 2; id++ {
		c, ws := h.batch(id, 3, twoMessages(id))
		if err := h.st.withdrawals.Put(unverifiedOf(c, ws)); err != nil {
			t.Fatal(err)
		}
		e := entryFor(t, c)
		h.chain.set(func(ch *chainFake) { ch.entries[id] = e })
	}
	return h, logs
}

func (h *cursorHarness) wantVerified(id uint64, want bool) {
	h.t.Helper()
	if v, ok := h.st.withdrawals.Lookup(id); !ok || v != want {
		h.t.Fatalf("batch %d (verified, ok) = (%v, %v), want verified %v", id, v, ok, want)
	}
}

func TestRetryBoundsAndErrors(t *testing.T) {
	t.Run("a zero schedule verifies one batch per request frame", func(t *testing.T) {
		h, _ := retryHarness(t)
		h.sync()
		h.wantVerified(1, true)
		h.wantVerified(2, false)
		h.sync()
		h.wantVerified(2, true)
	})

	t.Run("a long wait stops the second frame", func(t *testing.T) {
		h, _ := retryHarness(t)
		h.st.mu.Lock()
		h.st.verify = verifyRetry{base: time.Hour, delay: time.Hour}
		h.st.mu.Unlock()
		h.sync()
		h.wantVerified(1, true)
		h.sync()
		h.wantVerified(2, false)
	})

	t.Run("a read error logs once and leaves no fault", func(t *testing.T) {
		h, logs := retryHarness(t)
		h.chain.set(func(ch *chainFake) { ch.entryErr = errors.New("node down") })
		h.sync()
		h.sync()
		if n := countOf(logs, "WARN: batch 1 stored withdrawals still unverified: node down"); n != 1 {
			t.Fatalf("log has the warning %d times, want once:\n%s", n, logs.String())
		}
		h.wantVerified(1, false)
		h.st.mu.Lock()
		defer h.st.mu.Unlock()
		if h.st.fault != "" {
			t.Fatalf("fault = %q, want none", h.st.fault)
		}
	})

	t.Run("a halted read faults", func(t *testing.T) {
		h, _ := retryHarness(t)
		h.chain.set(func(ch *chainFake) { ch.entryErr = fmt.Errorf("%w: gone", recovery.ErrHalted) })
		h.send(ipc.FrontierRequest{})
		h.wantFault("halted retry read")
	})

	t.Run("a removed store directory keeps the batch unverified", func(t *testing.T) {
		h, logs := retryHarness(t)
		if err := os.RemoveAll(h.st.withdrawals.dir); err != nil {
			t.Fatal(err)
		}
		h.sync()
		h.sync()
		h.wantVerified(1, false)
		if n := countOf(logs, "stored withdrawals still unverified"); n != 1 {
			t.Fatalf("log has the warning %d times, want once:\n%s", n, logs.String())
		}
	})
}

func TestStaleMarkCannotVerifyOrDelete(t *testing.T) {
	// Batch 2 has an unverified file while the observed mark is 0, as
	// after a restart against a lagging node.
	setup := func(t *testing.T) (*cursorHarness, codec.BatchCommitment, []ipc.Withdrawal) {
		captureLog(t)
		h := newCursorHarness(t)
		holdRetries(h.st, true)
		c, ws := h.batch(2, 3, twoMessages(2))
		if err := h.st.withdrawals.Put(unverifiedOf(c, ws)); err != nil {
			t.Fatal(err)
		}
		return h, c, ws
	}

	t.Run("a Retract keeps the file", func(t *testing.T) {
		h, _, _ := setup(t)
		h.send(ipc.Retract{FromBatchID: 0})
		h.sync()
		h.wantVerified(2, false)
	})

	t.Run("a Recovered compares and holds nothing", func(t *testing.T) {
		h, c, ws := setup(t)
		h.send(ipc.Retract{FromBatchID: 3})
		h.handOff(ipc.Recovered{Commitment: c}, ws...)
		h.sync()
		h.wantVerified(2, false)
		if held := h.held(); len(held) != 0 {
			t.Fatalf("held = %v, want none", held)
		}
		h.wantReads(0)
		h.finish()
	})

	t.Run("a Recovered with a changed proof faults", func(t *testing.T) {
		h, c, ws := setup(t)
		ws[0].Proof = append([]byte{}, ws[0].Proof...)
		ws[0].Proof[len(ws[0].Proof)-1] ^= 1
		h.send(ipc.Retract{FromBatchID: 3})
		h.handOff(ipc.Recovered{Commitment: c}, ws...)
		h.wantFault("changed proof")
	})

	t.Run("a Committed changes no progress", func(t *testing.T) {
		h, c, ws := setup(t)
		h.send(ipc.Retract{FromBatchID: 0})
		h.handOff(ipc.Committed{Commitment: c}, ws...)
		h.sync()
		h.wantVerified(2, false)
		h.wantNoProgress()
		if held := h.held(); len(held) != 0 {
			t.Fatalf("held = %v, want none", held)
		}
		h.finish()
	})
}

// --- hung Settlement ---

// hungHarness stores unverified batch 2 with its entry readable, but
// every read blocks until the returned release is called.
func hungHarness(t *testing.T) (*cursorHarness, *lockedBuffer, func()) {
	t.Helper()
	logs := captureLog(t)
	h := newCursorHarness(t)
	c, ws := h.batch(2, 3, twoMessages(2))
	if err := h.st.withdrawals.Put(unverifiedOf(c, ws)); err != nil {
		t.Fatal(err)
	}
	block := make(chan struct{})
	h.chain.set(func(ch *chainFake) { ch.entries[2] = entryFor(t, c); ch.entryBlock = block })
	h.st.mu.Lock()
	h.st.verify = verifyRetry{base: 10 * time.Millisecond, max: 40 * time.Millisecond, timeout: 50 * time.Millisecond, delay: 10 * time.Millisecond}
	h.st.mu.Unlock()
	var once sync.Once
	release := func() { once.Do(func() { close(block) }) }
	t.Cleanup(release)
	return h, logs, release
}

func (h *cursorHarness) schedule() verifyRetry {
	h.st.mu.Lock()
	defer h.st.mu.Unlock()
	return h.st.verify
}

func (h *cursorHarness) dueNow() {
	h.st.mu.Lock()
	h.st.verify.after = time.Time{}
	h.st.mu.Unlock()
}

// waitPending waits, with a bound, until the abandoned read has
// finished and its result is waiting in the schedule's channel.
func (h *cursorHarness) waitPending() {
	h.t.Helper()
	waitFor(h.t, 5*time.Second, "the abandoned read's result", func() bool {
		h.st.mu.Lock()
		defer h.st.mu.Unlock()
		return len(h.st.verify.pending) == 1
	})
}

func TestRetryBoundsAHungSettlement(t *testing.T) {
	h, logs, release := hungHarness(t)

	h.syncWithin(500 * time.Millisecond)
	h.waitReads(1)
	if v := h.schedule(); v.delay != 20*time.Millisecond || v.pending == nil {
		t.Fatalf("after frame 1: delay = %s, pending set = %v, want 20ms and set", v.delay, v.pending != nil)
	}
	if n := countOf(logs, "stored withdrawals still unverified"); n != 1 {
		t.Fatalf("log has the warning %d times, want once:\n%s", n, logs.String())
	}

	for i, want := range []time.Duration{40 * time.Millisecond, 40 * time.Millisecond} {
		h.dueNow()
		h.syncWithin(500 * time.Millisecond)
		h.wantReads(1)
		if v := h.schedule(); v.delay != want {
			t.Fatalf("after frame %d: delay = %s, want %s", i+2, v.delay, want)
		}
	}

	release()
	h.waitPending()
	h.dueNow()
	h.syncWithin(500 * time.Millisecond)
	h.wantVerified(2, true)
	h.wantReads(1)
	v := h.schedule()
	if v.delay != 10*time.Millisecond || v.failing || v.pending != nil {
		t.Fatalf("after frame 4: delay = %s, failing = %v, pending set = %v, want 10ms, false, unset", v.delay, v.failing, v.pending != nil)
	}
	h.st.mu.Lock()
	defer h.st.mu.Unlock()
	if h.st.fault != "" {
		t.Fatalf("fault = %q, want none", h.st.fault)
	}
}

func TestRetryScheduleSurvivesAWorkerRestart(t *testing.T) {
	h, _, release := hungHarness(t)

	h.syncWithin(500 * time.Millisecond)
	h.waitReads(1)
	if v := h.schedule(); v.delay != 20*time.Millisecond || v.pending == nil {
		t.Fatalf("after frame 1: delay = %s, pending set = %v, want 20ms and set", v.delay, v.pending != nil)
	}
	h.finish()

	sEnd, wEnd := newChannel(t)
	done := runServe(t, sEnd, h.w, h.exec, h.chain.sources(), h.st)
	h.wEnd = wEnd
	h.dueNow()
	h.syncWithin(500 * time.Millisecond)
	h.wantReads(1)
	if v := h.schedule(); v.delay != 40*time.Millisecond {
		t.Fatalf("new session: delay = %s, want 40ms", v.delay)
	}

	release()
	h.waitPending()
	h.dueNow()
	h.syncWithin(500 * time.Millisecond)
	h.wantVerified(2, true)

	wEnd.Close()
	if err := waitServe(t, done); err != nil {
		t.Fatalf("serve = %v, want nil", err)
	}
}

// --- re-sync, Retract, store failure ---

func TestHandOffResyncFromAnEmptyStore(t *testing.T) {
	captureLog(t)
	h := newCursorHarness(t)
	type sent struct {
		c  codec.BatchCommitment
		ws []ipc.Withdrawal
	}
	var all []sent
	for id := uint64(0); id <= 5; id++ {
		c, ws := h.batch(id, 3, twoMessages(id))
		all = append(all, sent{c, ws})
		if id < 5 {
			e := entryFor(t, c)
			h.chain.set(func(ch *chainFake) { ch.entries[id] = e })
		}
	}
	h.chain.set(func(ch *chainFake) { ch.next = 5 })
	h.cursor()

	h.send(ipc.Retract{FromBatchID: 5})
	for _, s := range all[:5] {
		h.handOff(ipc.Recovered{Commitment: s.c}, s.ws...)
	}
	h.handOff(ipc.Committed{Commitment: all[5].c}, all[5].ws...)
	h.sync()

	refs := h.st.withdrawals.Batches()
	if len(refs) != 6 {
		t.Fatalf("Batches = %v, want batches 0 to 5", refs)
	}
	for i, r := range refs {
		if r.BatchID != uint64(i) || !r.Verified {
			t.Fatalf("Batches = %v, want batches 0 to 5, all verified", refs)
		}
		b := mustBatch(t, h.st.withdrawals, r.BatchID)
		if b.OutboxRoot != all[i].c.OutboxRoot {
			t.Fatalf("batch %d root = %v, want %v", i, b.OutboxRoot, all[i].c.OutboxRoot)
		}
		wantStoredEqual(t, b, all[i].ws)
	}
	if held := h.held(); len(held) != 1 || !held[5] {
		t.Fatalf("held = %v, want batch 5 only", held)
	}
	h.wantReads(5)

	h.send(ipc.Retract{FromBatchID: 6})
	for _, s := range all {
		h.handOff(ipc.Recovered{Commitment: s.c}, s.ws...)
	}
	h.sync()
	h.wantReads(5)
	h.finish()
}

func TestRetractDropsVerifiedFilesAboveTheMark(t *testing.T) {
	captureLog(t)
	h := newCursorHarness(t)
	for id := uint64(0); id <= 5; id++ {
		b := sampleBatch(t, 1, id)
		if err := h.st.withdrawals.Put(b); err != nil {
			t.Fatal(err)
		}
	}
	h.chain.set(func(ch *chainFake) { ch.next = 3 })
	h.cursor()

	ids := func() []uint64 {
		var out []uint64
		for _, r := range h.st.withdrawals.Batches() {
			out = append(out, r.BatchID)
		}
		return out
	}
	h.send(ipc.Retract{FromBatchID: 4})
	h.sync()
	if got := fmt.Sprint(ids()); got != "[0 1 2 3]" {
		t.Fatalf("after Retract 4: %s, want [0 1 2 3]", got)
	}
	h.send(ipc.Retract{FromBatchID: 2})
	h.sync()
	if got := fmt.Sprint(ids()); got != "[0 1 2]" {
		t.Fatalf("after Retract 2: %s, want [0 1 2]", got)
	}
	h.finish()
}

func TestHandOffStoreFailure(t *testing.T) {
	t.Run("a hand-off write", func(t *testing.T) {
		h := newCursorHarness(t)
		h.sync()
		if err := os.RemoveAll(h.st.withdrawals.dir); err != nil {
			t.Fatal(err)
		}
		c, ws := h.batch(1, 3, twoMessages(1))
		h.handOff(ipc.Committed{Commitment: c}, ws...)
		err := waitServe(t, h.done)
		if !errors.Is(err, ErrWithdrawalStore) || errors.Is(err, ErrMalformed) || errors.Is(err, ErrInconsistent) {
			t.Fatalf("serve = %v, want only ErrWithdrawalStore", err)
		}
		h.st.mu.Lock()
		defer h.st.mu.Unlock()
		if h.st.fault != "" || len(h.st.commitments) != 0 {
			t.Fatalf("fault = %q, held %d, want none", h.st.fault, len(h.st.commitments))
		}
	})

	t.Run("a Retract delete", func(t *testing.T) {
		h := newCursorHarness(t)
		if err := h.st.withdrawals.Put(sampleBatch(t, 1, 3)); err != nil {
			t.Fatal(err)
		}
		h.sync()
		if err := os.RemoveAll(h.st.withdrawals.dir); err != nil {
			t.Fatal(err)
		}
		h.send(ipc.Retract{FromBatchID: 0})
		if err := waitServe(t, h.done); !errors.Is(err, ErrWithdrawalStore) {
			t.Fatalf("serve = %v, want ErrWithdrawalStore", err)
		}
	})
}
