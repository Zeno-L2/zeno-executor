package supervisor

import (
	"encoding/binary"
	"errors"
	"math/big"
	"net/url"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/zenon-network/go-zenon/common/types"

	"github.com/Zeno-L2/zeno-executor/internal/codec"
	"github.com/Zeno-L2/zeno-executor/internal/ipc"
)

// referenceFor builds the reference a batch of msgs verifies against.
func referenceFor(t *testing.T, msgs []codec.OutboxMessage, first, last uint64) (reference, []StoredWithdrawal) {
	t.Helper()
	c := commitmentFor(1, types.Address{0x09}, 1, last)
	ws := withdrawalBatch(t, &c, msgs)
	return reference{root: c.OutboxRoot, first: first, last: last, summary: c.AssetFlowSummary}, storedOf(ws)
}

func TestHandOffFaultsOnPlacementAlone(t *testing.T) {
	// Each case keeps every proof and every per-asset sum valid, so only
	// the strictly ascending (inputIndex, outboxIndex) order can reject it.
	cases := []struct {
		name string
		msgs []codec.OutboxMessage
		send func(ws []ipc.Withdrawal) []ipc.Withdrawal
	}{
		{
			"outboxIndex descending within one input",
			[]codec.OutboxMessage{l1msg(1, 0, assetN(1), 5, testSource), l1msg(2, 0, assetN(1), 6, testSource), l1msg(2, 1, assetN(1), 7, testSource)},
			func(ws []ipc.Withdrawal) []ipc.Withdrawal { return []ipc.Withdrawal{ws[0], ws[2], ws[1]} },
		},
		{
			"a zero-amount withdrawal repeated",
			[]codec.OutboxMessage{l1msg(1, 0, assetN(1), 5, testSource), l1msg(2, 0, assetN(1), 0, testSource)},
			func(ws []ipc.Withdrawal) []ipc.Withdrawal { return []ipc.Withdrawal{ws[0], ws[1], ws[1]} },
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			captureLog(t)
			h := newCursorHarness(t)
			c, ws := h.batch(1, 3, tc.msgs)
			h.handOff(ipc.Committed{Commitment: c}, tc.send(ws)...)
			h.wantFault(tc.name)
			h.wantNoFile(1)
			h.wantNoProgress()
		})
	}
}

func TestHandOffAddRejectsAKind0Message(t *testing.T) {
	msg, err := kind0msg(1, 0).Encode()
	if err != nil {
		t.Fatal(err)
	}
	h := &handOff{c: codec.BatchCommitment{BatchID: 1}}
	err = h.add(ipc.Withdrawal{BatchID: 1, Message: msg, Proof: make([]byte, 4)}, &workerState{})
	if !errors.Is(err, ErrMalformed) || errors.Is(err, ErrInconsistent) {
		t.Fatalf("add = %v, want ErrMalformed", err)
	}
	if len(h.got) != 0 {
		t.Fatalf("got %d withdrawals, want none", len(h.got))
	}
}

func TestVerifyBatch(t *testing.T) {
	a := assetN(1)
	big256 := func(shift uint) codec.U256 {
		u, err := codec.U256FromBig(new(big.Int).Lsh(big.NewInt(1), shift))
		if err != nil {
			t.Fatal(err)
		}
		return u
	}
	withAmount := func(input uint64, amount codec.U256) codec.OutboxMessage {
		m := l1msg(input, 0, a, 0, testSource)
		l1 := m.Target.(codec.L1Withdrawal)
		l1.Amount = amount
		m.Target = l1
		return m
	}

	t.Run("an amount above 64 bits sums in full", func(t *testing.T) {
		ref, ws := referenceFor(t, []codec.OutboxMessage{withAmount(1, big256(64)), withAmount(2, big256(200))}, 1, 2)
		if err := verifyBatch(ref, ws); err != nil {
			t.Fatalf("verifyBatch = %v, want nil", err)
		}
	})

	t.Run("a refund counts in the totals", func(t *testing.T) {
		msgs := []codec.OutboxMessage{l1msg(1, 0, a, 5, testSource), l1msg(2, 0, a, 3, types.Hash{})}
		ref, ws := referenceFor(t, msgs, 1, 2)
		if err := verifyBatch(ref, ws); err != nil {
			t.Fatalf("verifyBatch = %v, want nil", err)
		}
		ref.summary[0].WithdrawalDebit = u256Of(5)
		if err := verifyBatch(ref, ws); err == nil {
			t.Fatalf("verifyBatch with the refund left out of the debit = nil, want an error")
		}
	})

	t.Run("a zero-amount withdrawal in an asset with no summary entry passes", func(t *testing.T) {
		msgs := []codec.OutboxMessage{l1msg(1, 0, a, 5, testSource), l1msg(2, 0, assetN(2), 0, testSource)}
		ref, ws := referenceFor(t, msgs, 1, 2)
		if len(ref.summary) != 1 {
			t.Fatalf("summary has %d entries, want 1", len(ref.summary))
		}
		if err := verifyBatch(ref, ws); err != nil {
			t.Fatalf("verifyBatch = %v, want nil", err)
		}
	})

	t.Run("an inputIndex below the range", func(t *testing.T) {
		msgs := []codec.OutboxMessage{l1msg(1, 0, a, 5, testSource), l1msg(2, 0, a, 6, testSource)}
		ref, ws := referenceFor(t, msgs, 2, 2)
		if err := verifyBatch(ref, ws); err == nil {
			t.Fatalf("verifyBatch = nil, want an error for inputIndex 1 below first 2")
		}
	})

	t.Run("an inputIndex at each end of the range", func(t *testing.T) {
		msgs := []codec.OutboxMessage{l1msg(1, 0, a, 5, testSource), l1msg(2, 0, a, 6, testSource)}
		ref, ws := referenceFor(t, msgs, 1, 2)
		if err := verifyBatch(ref, ws); err != nil {
			t.Fatalf("verifyBatch = %v, want nil", err)
		}
	})

	t.Run("a kind 0 message", func(t *testing.T) {
		ref, ws := referenceFor(t, []codec.OutboxMessage{kind0msg(1, 0), l1msg(2, 0, a, 6, testSource)}, 1, 2)
		k0, err := kind0msg(1, 0).Encode()
		if err != nil {
			t.Fatal(err)
		}
		proofs := codec.MerkleProofs([][]byte{k0, ws[0].Message})
		if err := verifyBatch(ref, []StoredWithdrawal{{Message: k0, Proof: proofs[0]}, ws[0]}); err == nil {
			t.Fatalf("verifyBatch = nil, want an error for a kind 0 message")
		}
	})
}

func TestRetryFailurePushesTheNextRetryOut(t *testing.T) {
	h, logs := retryHarness(t)
	h.chain.set(func(ch *chainFake) { ch.entryErr = errors.New("node down") })
	h.st.mu.Lock()
	h.st.verify = verifyRetry{base: time.Hour, max: time.Hour, delay: time.Hour}
	h.st.mu.Unlock()

	before := time.Now()
	h.sync()
	reads := h.chain.readCount()
	if reads != 1 {
		t.Fatalf("entry reads = %d, want 1", reads)
	}
	if v := h.schedule(); v.after.Before(before.Add(time.Hour)) {
		t.Fatalf("next retry at %v, want at least an hour after %v", v.after, before)
	}
	h.sync()
	h.sync()
	h.wantReads(reads)
	if n := countOf(logs, "stored withdrawals still unverified"); n != 1 {
		t.Fatalf("log has the warning %d times, want once:\n%s", n, logs.String())
	}
}

func TestHandOffWarningsRedactTheEndpoint(t *testing.T) {
	const raw = "https://u:p@host/path?token=x"
	urlErr := &url.Error{Op: "Post", URL: raw, Err: errors.New("connection refused")}
	check := func(t *testing.T, logs *lockedBuffer, warning string) {
		t.Helper()
		if countOf(logs, warning) != 1 {
			t.Fatalf("log has no %q warning:\n%s", warning, logs.String())
		}
		for _, bad := range []string{"u:p", "/path", "token=x"} {
			if countOf(logs, bad) != 0 {
				t.Fatalf("the log carries %q:\n%s", bad, logs.String())
			}
		}
		if countOf(logs, "https://host") == 0 {
			t.Fatalf("the log has no redacted endpoint:\n%s", logs.String())
		}
	}

	t.Run("hand-off stored unverified", func(t *testing.T) {
		logs := captureLog(t)
		h := newCursorHarness(t)
		h.st.rawURL = raw
		holdRetries(h.st, true)
		c, ws := h.batch(1, 3, twoMessages(0))
		h.chain.set(func(ch *chainFake) {
			ch.next = 4
			ch.entryErr = urlErr
		})
		h.cursor()
		h.handOff(ipc.Recovered{Commitment: c}, ws...)
		h.sync()
		check(t, logs, "withdrawals stored unverified")
	})

	t.Run("retry failure", func(t *testing.T) {
		h, logs := retryHarness(t)
		h.st.rawURL = raw
		h.chain.set(func(ch *chainFake) { ch.entryErr = urlErr })
		h.sync()
		check(t, logs, "stored withdrawals still unverified")
	})
}

func TestLowestUnverified(t *testing.T) {
	h := newCursorHarness(t)
	if _, ok := h.st.withdrawals.lowestUnverified(); ok {
		t.Fatal("lowestUnverified on an empty store = found, want none")
	}
	for _, id := range []uint64{3, 1, 2} {
		c, ws := h.batch(id, 3, twoMessages(id))
		w := unverifiedOf(c, ws)
		w.Verified = id == 1
		if err := h.st.withdrawals.Put(w); err != nil {
			t.Fatal(err)
		}
	}
	if id, ok := h.st.withdrawals.lowestUnverified(); !ok || id != 2 {
		t.Fatalf("lowestUnverified = (%d, %v), want (2, true)", id, ok)
	}
}

func TestRetryIgnoresAFinishedReadForAnotherBatch(t *testing.T) {
	h, _, release := hungHarness(t)

	h.syncWithin(500 * time.Millisecond)
	h.waitReads(1)
	release()
	h.waitPending()

	// A lower unverified batch appears; the finished read is for batch 2.
	c1, ws1 := h.batch(1, 3, twoMessages(1))
	if err := h.st.withdrawals.Put(unverifiedOf(c1, ws1)); err != nil {
		t.Fatal(err)
	}
	h.chain.set(func(ch *chainFake) { ch.entries[1] = entryFor(t, c1) })

	h.dueNow()
	h.syncWithin(500 * time.Millisecond)
	h.wantVerified(1, true)
	h.wantVerified(2, false)
	h.wantReads(2)
	h.st.mu.Lock()
	defer h.st.mu.Unlock()
	if h.st.fault != "" {
		t.Fatalf("fault = %q, want none", h.st.fault)
	}
}

func TestWithdrawalStoreFailedWriteKeepsThePreviousFile(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("directory permissions do not bind root")
	}
	s := testStore(t, 1)
	b := sampleBatch(t, 1, 7)
	if err := s.Put(b); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(s.dir, "7.json")
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(s.dir, 0o500); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.Chmod(s.dir, 0o700) })

	changed := b
	changed.Withdrawals = b.Withdrawals[:1]
	if err := s.Put(changed); err == nil {
		t.Fatalf("Put into a read-only directory = nil, want an error")
	}
	after, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(after) != string(before) {
		t.Fatalf("the file changed after a failed Put")
	}
}

func TestWithdrawalStoreDamageInUnverifiedFiles(t *testing.T) {
	proof := make([]byte, 4)
	binary.BigEndian.PutUint32(proof, 0)
	unverifiedWith := func(msgs ...codec.OutboxMessage) StoredBatch {
		b := StoredBatch{DomainID: 1, BatchID: 7}
		for _, m := range msgs {
			enc, err := m.Encode()
			if err != nil {
				t.Fatal(err)
			}
			b.Withdrawals = append(b.Withdrawals, StoredWithdrawal{Message: enc, Proof: proof})
		}
		return b
	}
	over := make([]codec.OutboxMessage, ipc.MaxHandOffWithdrawals+1)
	for i := range over {
		over[i] = l1msg(uint64(i+1), 0, assetN(1), 1, testSource)
	}

	// Each batch is well formed except for the one fault named.
	for name, b := range map[string]StoredBatch{
		"a kind 0 message":                         unverifiedWith(kind0msg(1, 0)),
		"more withdrawals than the hand-off limit": unverifiedWith(over...),
	} {
		t.Run(name, func(t *testing.T) {
			if err := checkStoredBatch(b, 1, 7); err == nil {
				t.Fatalf("checkStoredBatch = nil, want an error")
			}
		})
	}
	t.Run("the same shape within the limit is accepted", func(t *testing.T) {
		if err := checkStoredBatch(unverifiedWith(over[:ipc.MaxHandOffWithdrawals]...), 1, 7); err != nil {
			t.Fatalf("checkStoredBatch = %v, want nil", err)
		}
	})
}

func TestLowestVerified(t *testing.T) {
	h := newCursorHarness(t)
	if _, ok := h.st.withdrawals.lowestVerified(10); ok {
		t.Fatal("lowestVerified on an empty store = found, want none")
	}
	for _, id := range []uint64{3, 1, 2} {
		c, ws := h.batch(id, 3, twoMessages(id))
		w := unverifiedOf(c, ws)
		w.Verified = id != 1
		if err := h.st.withdrawals.Put(w); err != nil {
			t.Fatal(err)
		}
	}
	for _, tc := range []struct {
		limit uint64
		id    uint64
		ok    bool
	}{{10, 2, true}, {3, 2, true}, {2, 0, false}, {0, 0, false}} {
		if id, ok := h.st.withdrawals.lowestVerified(tc.limit); ok != tc.ok || id != tc.id {
			t.Fatalf("lowestVerified(%d) = (%d, %v), want (%d, %v)", tc.limit, id, ok, tc.id, tc.ok)
		}
	}
}

func TestCompareWithRemovedBatch(t *testing.T) {
	h := newCursorHarness(t)
	c, ws := h.batch(1, 3, twoMessages(1))
	if err := h.st.withdrawals.Put(unverifiedOf(c, ws)); err != nil {
		t.Fatal(err)
	}
	ho := &handOff{c: c}
	if got, found, err := ho.compareWith(h.st); err != nil || !found || got != ho || got.mode != compareHandOff {
		t.Fatalf("compareWith = %v, %v, %v; want the compare hand-off", got, found, err)
	}
	if err := h.st.withdrawals.Remove(1); err != nil {
		t.Fatal(err)
	}
	ho = &handOff{c: c}
	if got, found, err := ho.compareWith(h.st); err != nil || found || got != nil {
		t.Fatalf("compareWith after Remove = %v, %v, %v; want not found and no error", got, found, err)
	}
}
