package supervisor

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/zenon-network/go-zenon/chain/nom"
	"github.com/zenon-network/go-zenon/common/types"
	"github.com/zenon-network/go-zenon/vm/embedded/definition"

	"github.com/digitalSloth/go-executor/internal/codec"
	"github.com/digitalSloth/go-executor/internal/recovery"
	"github.com/digitalSloth/go-executor/internal/settlement"
	"github.com/digitalSloth/go-executor/internal/znnd"
)

var relayRecipient = types.Address{0x0A}

// relayMsg is a kind 1 outbox message.
func relayMsg(input uint64, outbox uint32, amount byte, to types.Address) codec.OutboxMessage {
	return codec.OutboxMessage{
		SourceContract: types.Hash{0x04},
		InputIndex:     input,
		OutboxIndex:    outbox,
		Target:         codec.L1Withdrawal{RecipientL1: to, Asset: assetN(1), Amount: u256Of(uint64(amount))},
	}
}

// storeRelayBatch stores domain d's batch id as a verified file holding
// msgs, whose leaves root on the commitment h.commitment builds, and
// returns it.
func (h *submitHarness) storeRelayBatch(d uint32, id uint64, msgs ...codec.OutboxMessage) StoredBatch {
	h.t.Helper()
	c := h.commitment(d, id, 10*(id+1))
	ws := withdrawalBatch(h.t, &c, msgs)
	b := StoredBatch{DomainID: d, BatchID: id, Verified: true, OutboxRoot: c.OutboxRoot}
	for _, w := range ws {
		b.Withdrawals = append(b.Withdrawals, StoredWithdrawal{Message: w.Message, Proof: w.Proof})
	}
	if err := h.st(d).withdrawals.Put(b); err != nil {
		h.t.Fatal(err)
	}
	return b
}

// finalized makes Settlement's entry for domain d's batch id carry root
// and finalize at height at.
func (h *submitHarness) finalized(d uint32, id uint64, root types.Hash, at uint64) {
	h.entries[entryKey{d, id}] = &definition.BatchEntry{OutboxRoot: root, FinalizeAtHeight: at}
}

// newRelayHarness returns a harness whose domain d has accepted batch 0
// of msgs, finalized, stored and not yet relayed.
func newRelayHarness(t *testing.T, d uint32, msgs ...codec.OutboxMessage) (*submitHarness, StoredBatch) {
	t.Helper()
	h := newSubmitHarness(t, d)
	h.setTip(d, 1)
	h.setMark(d, 1)
	b := h.storeRelayBatch(d, 0, msgs...)
	h.finalized(d, 0, b.OutboxRoot, 100)
	return h, b
}

func twoMsgs() []codec.OutboxMessage {
	return []codec.OutboxMessage{relayMsg(0, 0, 5, relayRecipient), relayMsg(1, 0, 6, relayRecipient)}
}

func relayIDOf(d uint32, batch uint64, m codec.OutboxMessage) types.Hash {
	return codec.OutboxID(d, batch, m.InputIndex, m.OutboxIndex)
}

func packRelay(t *testing.T, d uint32, batch uint64, w StoredWithdrawal) []byte {
	t.Helper()
	data, err := definition.ABISettlement.PackMethod(definition.SettlementRelayMessageMethodName, d, batch, w.Message, w.Proof)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func packRelease(t *testing.T, d uint32) []byte {
	t.Helper()
	data, err := definition.ABISettlement.PackMethod(definition.SettlementReleaseWithdrawalsMethodName, d)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

// receiveSend makes the node report b received at height 90.
func (h *submitHarness) receiveSend(b *nom.AccountBlock) {
	h.node.statuses[b.Hash] = znnd.SendStatus{Block: b, Received: true, ReceiveHeight: 90}
}

// lastMethod returns the method of the latest record.
func (h *submitHarness) lastMethod() string {
	r, _ := h.last()
	return r.Method
}

func TestRelayAndReleaseCycle(t *testing.T) {
	h, b := newRelayHarness(t, 1, twoMsgs()...)
	m := twoMsgs()
	id0, id1 := relayIDOf(1, 0, m[0]), relayIDOf(1, 0, m[1])

	h.step()
	h.wantPublished(1)
	b0 := h.lastBlock()
	if want := packRelay(t, 1, 0, b.Withdrawals[0]); !bytes.Equal(b0.Data, want) {
		t.Fatalf("Data = %x, want the packed RelayMessage of withdrawal 0 %x", b0.Data, want)
	}
	if r, _ := h.last(); r.Method != definition.SettlementRelayMessageMethodName || r.DomainID != 1 || r.BatchID != 0 || r.OutboxID != id0 || r.Outcome != "" {
		t.Fatalf("record = %+v, want an unresolved relay of withdrawal 0", r)
	}
	if h.unresolved(1) != nil {
		t.Fatal("a relay marked the worker's batch unresolved")
	}

	h.receiveSend(b0)
	h.processed[id0] = true
	h.queue[1] = 1
	h.step()
	if r, _ := h.last(); r.Outcome != OutcomeAccepted || !h.logHas("registered") {
		t.Fatalf("record = %+v, want accepted and logged registered", r)
	}
	h.wantPublished(1)
	h.step()
	h.wantPublished(2)
	b1 := h.lastBlock()
	if want := packRelay(t, 1, 0, b.Withdrawals[1]); !bytes.Equal(b1.Data, want) {
		t.Fatalf("Data = %x, want the relay of withdrawal 1 %x, not a release", b1.Data, want)
	}

	h.receiveSend(b1)
	h.processed[id1] = true
	h.queue[1] = 2
	h.step()
	if r, _ := h.last(); r.Outcome != OutcomeAccepted {
		t.Fatalf("record = %+v, want accepted", r)
	}
	h.step()
	h.wantPublished(3)
	rel := h.lastBlock()
	if !bytes.Equal(rel.Data, packRelease(t, 1)) {
		t.Fatalf("Data = %x, want the packed ReleaseWithdrawals", rel.Data)
	}
	if refs := h.st(1).withdrawals.Batches(); len(refs) != 0 {
		t.Fatalf("stored batches = %v, want the file removed", refs)
	}
	if r, _ := h.last(); r.Method != definition.SettlementReleaseWithdrawalsMethodName || r.BatchID != 0 || r.OutboxID != (types.Hash{}) {
		t.Fatalf("record = %+v, want a release record", r)
	}

	h.receiveSend(rel)
	h.setTipHeight(1, 89)
	h.step()
	if r, _ := h.last(); r.Outcome != "" {
		t.Fatalf("outcome = %q at view height 89, want pending", r.Outcome)
	}
	h.setTipHeight(1, 100)
	h.step()
	if r, _ := h.last(); r.Outcome != OutcomeReceived {
		t.Fatalf("outcome = %q, want received", r.Outcome)
	}

	h.queue[1] = 0
	h.step()
	h.wantPublished(3)

	t.Run("a full queue is released ahead of a ready relay", func(t *testing.T) {
		h2, _ := newRelayHarness(t, 1, twoMsgs()...)
		h2.queue[1] = 16
		h2.step()
		h2.wantPublished(1)
		if !bytes.Equal(h2.lastBlock().Data, packRelease(t, 1)) {
			t.Fatalf("Data = %x, want the packed ReleaseWithdrawals", h2.lastBlock().Data)
		}
	})

	t.Run("a queue below 16 with no stored batch is released", func(t *testing.T) {
		h2 := newSubmitHarness(t, 1)
		h2.queue[1] = 1
		h2.step()
		h2.wantPublished(1)
		if h2.lastMethod() != definition.SettlementReleaseWithdrawalsMethodName {
			t.Fatalf("method = %q, want ReleaseWithdrawals", h2.lastMethod())
		}
	})
}

func TestRelayGates(t *testing.T) {
	t.Run("relay pause", func(t *testing.T) {
		h, _ := newRelayHarness(t, 1, twoMsgs()...)
		h.relayPaused[1] = true
		h.queue[1] = 16
		h.step()
		h.wantPublished(0)
		if !h.logHas("relay pause is set") {
			t.Fatalf("log = %q, want the pause warning", h.out.String())
		}
	})

	t.Run("entry not finalized", func(t *testing.T) {
		h, b := newRelayHarness(t, 1, twoMsgs()...)
		h.finalized(1, 0, b.OutboxRoot, 101)
		h.step()
		h.wantPublished(0)
	})

	t.Run("view below the batch", func(t *testing.T) {
		h, _ := newRelayHarness(t, 1, twoMsgs()...)
		h.setTip(1, 0)
		h.step()
		h.wantPublished(0)
		if f := h.fault(1); f != "" {
			t.Fatalf("fault = %q, want none", f)
		}
	})

	t.Run("unverified batch", func(t *testing.T) {
		h, b := newRelayHarness(t, 1, twoMsgs()...)
		b.Verified, b.OutboxRoot = false, types.Hash{}
		if err := h.st(1).withdrawals.Put(b); err != nil {
			t.Fatal(err)
		}
		h.step()
		h.wantPublished(0)
		if refs := h.st(1).withdrawals.Batches(); len(refs) != 1 {
			t.Fatalf("stored batches = %v, want the unverified file kept", refs)
		}
	})

	t.Run("halted domain", func(t *testing.T) {
		for _, queue := range []uint64{0, 16} {
			h, _ := newRelayHarness(t, 1, twoMsgs()...)
			h.queue[1] = queue
			st := h.st(1)
			st.mu.Lock()
			st.fault = "halted"
			st.mu.Unlock()
			h.step()
			h.wantPublished(0)
		}
	})
}

func TestRelaySkips(t *testing.T) {
	t.Run("a processed withdrawal is skipped", func(t *testing.T) {
		h, b := newRelayHarness(t, 1, twoMsgs()...)
		h.processed[relayIDOf(1, 0, twoMsgs()[0])] = true
		h.step()
		h.wantPublished(1)
		if want := packRelay(t, 1, 0, b.Withdrawals[1]); !bytes.Equal(h.lastBlock().Data, want) {
			t.Fatalf("Data = %x, want the relay of withdrawal 1 %x", h.lastBlock().Data, want)
		}
	})

	t.Run("zero-amount and embedded recipients are never relayed", func(t *testing.T) {
		msgs := []codec.OutboxMessage{
			relayMsg(0, 0, 0, relayRecipient),
			relayMsg(1, 0, 5, types.PlasmaContract),
			relayMsg(2, 0, 7, relayRecipient),
		}
		h, b := newRelayHarness(t, 1, msgs...)
		h.step()
		h.wantPublished(1)
		if want := packRelay(t, 1, 0, b.Withdrawals[2]); !bytes.Equal(h.lastBlock().Data, want) {
			t.Fatalf("Data = %x, want the relay of withdrawal 2 %x", h.lastBlock().Data, want)
		}
		h.receiveSend(h.lastBlock())
		h.processed[relayIDOf(1, 0, msgs[2])] = true
		h.step()
		h.step()
		h.wantPublished(1)
		if got := strings.Count(h.out.String(), "its amount is zero"); got != 1 {
			t.Fatalf("%d zero-amount lines, want 1", got)
		}
		if got := strings.Count(h.out.String(), "is an embedded contract"); got != 1 {
			t.Fatalf("%d embedded-recipient lines, want 1", got)
		}
		if refs := h.st(1).withdrawals.Batches(); len(refs) != 0 {
			t.Fatalf("stored batches = %v, want the file removed once the relayable withdrawal is registered", refs)
		}
	})

	t.Run("a batch with nothing relayable is removed", func(t *testing.T) {
		h, _ := newRelayHarness(t, 1, relayMsg(0, 0, 0, relayRecipient))
		h.step()
		h.wantPublished(0)
		if refs := h.st(1).withdrawals.Batches(); len(refs) != 0 {
			t.Fatalf("stored batches = %v, want the file removed", refs)
		}
	})
}

func TestRelayRootMismatch(t *testing.T) {
	h, _ := newRelayHarness(t, 1, twoMsgs()...)
	h.finalized(1, 0, types.Hash{0xee}, 0)
	h.step()
	h.wantPublished(0)
	if f := h.fault(1); !strings.Contains(f, "outbox root") {
		t.Fatalf("fault = %q, want the outbox root mismatch", f)
	}
	if refs := h.st(1).withdrawals.Batches(); len(refs) != 1 {
		t.Fatalf("stored batches = %v, want the file kept", refs)
	}
}

// publishAndReject publishes the next send and has Settlement
// receive it without processing the withdrawal, then resolves it.
func publishAndReject(h *submitHarness) {
	h.t.Helper()
	n := len(h.node.published)
	h.step()
	h.wantPublished(n + 1)
	h.receiveSend(h.lastBlock())
	h.step()
}

func TestRelayRejectionAndBackoff(t *testing.T) {
	m := twoMsgs()

	t.Run("a rejected relay waits out the backoff", func(t *testing.T) {
		h, b := newRelayHarness(t, 1, m...)
		publishAndReject(h)
		if r, _ := h.last(); r.Outcome != OutcomeRejected {
			t.Fatalf("outcome = %q, want rejected", r.Outcome)
		}
		h.step()
		h.wantPublished(1)
		h.advance(10 * time.Second)
		h.step()
		h.wantPublished(2)
		if want := packRelay(t, 1, 0, b.Withdrawals[0]); !bytes.Equal(h.lastBlock().Data, want) {
			t.Fatalf("Data = %x, want the same withdrawal again", h.lastBlock().Data)
		}
	})

	t.Run("five rejections raise the relay status and a third party clears it", func(t *testing.T) {
		h, b := newRelayHarness(t, 1, m...)
		for k := 0; k < 5; k++ {
			publishAndReject(h)
			h.advance(80 * time.Second)
		}
		if got := h.sup.Status()[0].RelayError; !strings.Contains(got, "5 consecutive") {
			t.Fatalf("RelayError = %q, want 5 consecutive rejections", got)
		}
		h.processed[relayIDOf(1, 0, m[0])] = true
		h.step()
		h.wantPublished(6)
		if want := packRelay(t, 1, 0, b.Withdrawals[1]); !bytes.Equal(h.lastBlock().Data, want) {
			t.Fatalf("Data = %x, want the relay of withdrawal 1", h.lastBlock().Data)
		}
		if got := h.sup.Status()[0].RelayError; got != "" {
			t.Fatalf("RelayError = %q, want it cleared", got)
		}
	})

	t.Run("a dropped relay backs off without counting", func(t *testing.T) {
		h, _ := newRelayHarness(t, 1, m...)
		h.node.publish = func(*nom.AccountBlock) error { return errors.New("node refused") }
		h.step()
		h.wantPublished(1)
		h.step()
		if r, _ := h.last(); r.Outcome != OutcomeDropped {
			t.Fatalf("outcome = %q, want dropped", r.Outcome)
		}
		if rr := h.sub.relayRetry[1]; rr == nil || rr.rejections != 0 || rr.delay != 10*time.Second {
			t.Fatalf("relayRetry = %+v, want 10s with no rejections", rr)
		}
		h.step()
		h.wantPublished(1)
		h.advance(10 * time.Second)
		h.step()
		h.wantPublished(2)
	})

	t.Run("a relay that cannot be built backs off without counting", func(t *testing.T) {
		h, _ := newRelayHarness(t, 1, m...)
		h.node.req.AvailablePlasma = 0
		h.step()
		h.wantPublished(0)
		calls := h.node.plasmaCalls
		if calls == 0 {
			t.Fatal("the build did not run")
		}
		h.step()
		if h.node.plasmaCalls != calls {
			t.Fatal("RequiredPlasma was called before the backoff ended")
		}
		if rr := h.sub.relayRetry[1]; rr == nil || rr.rejections != 0 {
			t.Fatalf("relayRetry = %+v, want a backoff with no rejections", rr)
		}
	})

	t.Run("a release that cannot be built backs off", func(t *testing.T) {
		h := newSubmitHarness(t, 1)
		h.queue[1] = 16
		h.node.req.AvailablePlasma = 0
		h.step()
		h.wantPublished(0)
		calls := h.node.plasmaCalls
		if calls == 0 {
			t.Fatal("the build did not run")
		}
		h.step()
		if h.node.plasmaCalls != calls {
			t.Fatal("RequiredPlasma was called before the backoff ended")
		}
		if rr := h.sub.releaseRetry[1]; rr == nil || rr.delay != 10*time.Second {
			t.Fatalf("releaseRetry = %+v, want a 10s backoff", rr)
		}
		h.node.req.AvailablePlasma = 100000
		h.advance(10 * time.Second)
		h.step()
		h.wantPublished(1)
		if h.sub.releaseRetry[1] != nil {
			t.Fatal("the release backoff outlived a built release")
		}
	})
}

func TestSubmitterAlternates(t *testing.T) {
	methods := func(h *submitHarness, n int, accept func(h *submitHarness, b *nom.AccountBlock)) []string {
		t.Helper()
		var out []string
		for k := 0; k < n; k++ {
			h.step()
			h.wantPublished(k + 1)
			out = append(out, h.lastMethod())
			accept(h, h.lastBlock())
			h.step()
			if r, _ := h.last(); r.Outcome == "" {
				t.Fatalf("send %d was not resolved", k)
			}
		}
		return out
	}

	t.Run("batch, relay, batch, release", func(t *testing.T) {
		h := newSubmitHarness(t, 1, 2)
		c0, c1 := h.hold(1, 0), h.hold(1, 1)
		h.setTip(2, 1)
		h.setMark(2, 1)
		msg := relayMsg(0, 0, 5, relayRecipient)
		b := h.storeRelayBatch(2, 0, msg)
		h.finalized(2, 0, b.OutboxRoot, 100)

		accept := func(h *submitHarness, blk *nom.AccountBlock) {
			switch h.lastMethod() {
			case definition.SettlementSubmitBatchMethodName:
				c := c0
				if blk.Data != nil && h.tips[1].NextBatchID == 1 {
					c = c1
				}
				h.accept(c, blk)
			case definition.SettlementRelayMessageMethodName:
				h.receiveSend(blk)
				h.processed[relayIDOf(2, 0, msg)] = true
				h.queue[2] = 1
			default:
				h.receiveSend(blk)
			}
		}
		got := methods(h, 4, accept)
		want := []string{
			definition.SettlementSubmitBatchMethodName,
			definition.SettlementRelayMessageMethodName,
			definition.SettlementSubmitBatchMethodName,
			definition.SettlementReleaseWithdrawalsMethodName,
		}
		if fmt.Sprint(got) != fmt.Sprint(want) {
			t.Fatalf("methods = %v, want %v", got, want)
		}
	})

	t.Run("two batch sends follow each other with no withdrawal ready", func(t *testing.T) {
		h := newSubmitHarness(t, 1)
		c0, c1 := h.hold(1, 0), h.hold(1, 1)
		got := methods(h, 2, func(h *submitHarness, blk *nom.AccountBlock) {
			c := c0
			if h.tips[1].NextBatchID == 1 {
				c = c1
			}
			h.accept(c, blk)
		})
		for _, m := range got {
			if m != definition.SettlementSubmitBatchMethodName {
				t.Fatalf("methods = %v, want two SubmitBatch", got)
			}
		}
	})

	t.Run("two relays follow each other with no batch ready", func(t *testing.T) {
		msgs := twoMsgs()
		h, _ := newRelayHarness(t, 1, msgs...)
		k := 0
		got := methods(h, 2, func(h *submitHarness, blk *nom.AccountBlock) {
			h.receiveSend(blk)
			h.processed[relayIDOf(1, 0, msgs[k])] = true
			k++
		})
		for _, m := range got {
			if m != definition.SettlementRelayMessageMethodName {
				t.Fatalf("methods = %v, want two RelayMessage", got)
			}
		}
	})
}

func TestRelayRestart(t *testing.T) {
	msgs := twoMsgs()

	setup := func(t *testing.T, path string, harness func() *submitHarness) (*submitHarness, *fileLog) {
		t.Helper()
		h := harness()
		lg, err := openSendLog(path)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { lg.Close() })
		h.sub.resume(lg, lg.accepted, lg.unreadable)
		return h, lg
	}

	t.Run("a pending relay is followed, not sent again", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "x.submissions.jsonl")
		fresh := func() *submitHarness {
			h, _ := newRelayHarness(t, 1, msgs...)
			return h
		}
		h1, lg1 := setup(t, path, fresh)
		h1.step()
		h1.wantPublished(1)
		b := h1.lastBlock()
		lg1.Close()

		h2, _ := setup(t, path, fresh)
		h2.sup.src.Node = h1.node
		h2.node = h1.node
		h2.step()
		h1.wantPublished(1)
		if h1.node.statusCount(b.Hash) == 0 {
			t.Fatal("the restored relay was not checked")
		}
		h2.receiveSend(b)
		h2.processed[relayIDOf(1, 0, msgs[0])] = true
		h2.step()
		if r, _ := h2.last(); r.Outcome != OutcomeAccepted {
			t.Fatalf("record = %+v, want accepted", r)
		}
		h2.step()
		h2.step()
		sent := 0
		for _, p := range h1.node.published {
			if bytes.Equal(p.Data, b.Data) {
				sent++
			}
		}
		if sent != 1 {
			t.Fatalf("the relay of withdrawal 0 was published %d times, want 1", sent)
		}
	})

	t.Run("a pending release is followed to received", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "x.submissions.jsonl")
		fresh := func() *submitHarness {
			h := newSubmitHarness(t, 1)
			h.queue[1] = 16
			return h
		}
		h1, lg1 := setup(t, path, fresh)
		h1.step()
		h1.wantPublished(1)
		b := h1.lastBlock()
		lg1.Close()

		h2, _ := setup(t, path, fresh)
		h2.sup.src.Node = h1.node
		h2.node = h1.node
		h2.step()
		h1.wantPublished(1)
		if h1.node.statusCount(b.Hash) == 0 {
			t.Fatal("the restored release was not checked")
		}
		h2.receiveSend(b)
		h2.step()
		if r, _ := h2.last(); r.Outcome != OutcomeReceived || r.Method != definition.SettlementReleaseWithdrawalsMethodName {
			t.Fatalf("record = %+v, want a received release", r)
		}
	})

	t.Run("a relay never published is dropped, then sent once", func(t *testing.T) {
		h, b := newRelayHarness(t, 1, msgs...)
		data := packRelay(t, 1, 0, b.Withdrawals[0])
		r := SendRecord{
			DomainID: 1, Method: definition.SettlementRelayMessageMethodName, BatchID: 0,
			Height: 5, Hash: types.Hash{0xb5}, DataHash: types.NewHash(data), OutboxID: relayIDOf(1, 0, msgs[0]),
		}
		lg := &memLog{}
		lg.Sent(r)
		h.node.put(&nom.AccountBlock{Height: 4, Hash: types.Hash{0x04}, Data: []byte{1}})
		h.sub.resume(lg, nil, nil)
		h.step()
		h.wantPublished(0)
		if got, _ := h.last(); got.Outcome != OutcomeDropped {
			t.Fatalf("record = %+v, want dropped", got)
		}
		h.step()
		h.wantPublished(0)
		h.advance(10 * time.Second)
		h.step()
		h.wantPublished(1)
		if !bytes.Equal(h.lastBlock().Data, data) {
			t.Fatalf("Data = %x, want the relay of withdrawal 0", h.lastBlock().Data)
		}
		h.step()
		h.wantPublished(1)
	})
}

func TestResolveRelayAndReleaseOutcomes(t *testing.T) {
	relayFixture := func(t *testing.T) *resolveFixture {
		f := newResolveFixture(t)
		f.data = packRelay(t, 1, 3, StoredWithdrawal{Message: []byte{1}, Proof: []byte{2}})
		f.blk.Data = f.data
		f.r = SendRecord{
			DomainID: 1, Method: definition.SettlementRelayMessageMethodName, BatchID: 3,
			Height: 5, Hash: f.blk.Hash, DataHash: types.NewHash(f.data), OutboxID: types.Hash{0x0d},
		}
		f.received()
		return f
	}
	releaseFixture := func(t *testing.T) *resolveFixture {
		f := newResolveFixture(t)
		f.data = packRelease(t, 1)
		f.blk.Data = f.data
		f.r = SendRecord{
			DomainID: 1, Method: definition.SettlementReleaseWithdrawalsMethodName,
			Height: 5, Hash: f.blk.Hash, DataHash: types.NewHash(f.data),
		}
		f.received()
		return f
	}

	t.Run("a relay is pending while the view is below the receive", func(t *testing.T) {
		f := relayFixture(t)
		f.relay = settlement.RelayState{Height: 89, Processed: 1}
		f.mustResolve(sendCheck{})
	})

	t.Run("a processed id is accepted", func(t *testing.T) {
		f := relayFixture(t)
		f.relay = settlement.RelayState{Height: 100, Processed: 1}
		f.mustResolve(sendCheck{Outcome: OutcomeAccepted})
	})

	t.Run("an id that is not processed is rejected", func(t *testing.T) {
		f := relayFixture(t)
		f.relay = settlement.RelayState{Height: 100}
		f.mustResolve(sendCheck{Outcome: OutcomeRejected})
	})

	t.Run("a missing entry does not change the outcome", func(t *testing.T) {
		f := relayFixture(t)
		f.relay = settlement.RelayState{Height: 90, Processed: 1}
		f.mustResolve(sendCheck{Outcome: OutcomeAccepted})
		f.relay = settlement.RelayState{Height: 90}
		f.mustResolve(sendCheck{Outcome: OutcomeRejected})
	})

	t.Run("a release is pending below the receive and received at it", func(t *testing.T) {
		f := releaseFixture(t)
		f.relay = settlement.RelayState{Height: 89}
		f.mustResolve(sendCheck{})
		f.relay = settlement.RelayState{Height: 90}
		f.mustResolve(sendCheck{Outcome: OutcomeReceived})
	})

	t.Run("a halted read is a fault naming the send", func(t *testing.T) {
		for _, f := range []*resolveFixture{relayFixture(t), releaseFixture(t)} {
			f.relayErr = fmt.Errorf("proof failed: %w", recovery.ErrHalted)
			got, err := f.resolve()
			if err != nil || !errors.Is(got.Fault, ErrInconsistent) || !strings.Contains(got.Fault.Error(), sendLabel(f.r)) {
				t.Fatalf("resolveSend = %+v, %v; want a consistency fault naming %q", got, err, sendLabel(f.r))
			}
		}
	})

	t.Run("a plain read error is returned", func(t *testing.T) {
		for _, f := range []*resolveFixture{relayFixture(t), releaseFixture(t)} {
			f.relayErr = errors.New("node down")
			if _, err := f.resolve(); err == nil || err.Error() != "node down" {
				t.Fatalf("resolveSend err = %v, want the read error", err)
			}
		}
	})
}

func TestRelayWindow(t *testing.T) {
	var msgs []codec.OutboxMessage
	for i := uint64(0); i < 66; i++ {
		msgs = append(msgs, relayMsg(i, 0, 5, relayRecipient))
	}
	h, b := newRelayHarness(t, 1, msgs...)
	for i := 0; i <= 64; i++ {
		h.processed[relayIDOf(1, 0, msgs[i])] = true
	}
	h.step()
	h.wantPublished(0)
	h.step()
	h.wantPublished(1)
	if want := packRelay(t, 1, 0, b.Withdrawals[65]); !bytes.Equal(h.lastBlock().Data, want) {
		t.Fatalf("Data = %x, want the relay of withdrawal 65", h.lastBlock().Data)
	}
}

func TestRelayShutdownAndResumeMarks(t *testing.T) {
	msgs := twoMsgs()

	t.Run("a context that ends during the relay read sends and records nothing", func(t *testing.T) {
		h, _ := newRelayHarness(t, 1, msgs...)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		fa := h.sup.src.Anchors.(fakeAnchors)
		read := fa.relay
		fa.relay = func(d uint32, q *settlement.RelayQuery) (settlement.RelayState, error) {
			rs, err := read(d, q)
			cancel()
			return rs, err
		}
		h.sup.src.Anchors = fa
		h.sub.step(ctx)
		h.wantPublished(0)
		if _, ok := h.last(); ok {
			t.Fatal("a send was recorded")
		}
	})

	t.Run("resuming a relay or a release leaves the batch send unmarked", func(t *testing.T) {
		for _, r := range []SendRecord{
			{DomainID: 1, Method: definition.SettlementRelayMessageMethodName, BatchID: 0, Height: 5, Hash: types.Hash{0xb5}, DataHash: types.Hash{0x01}, OutboxID: types.Hash{0x0d}},
			{DomainID: 1, Method: definition.SettlementReleaseWithdrawalsMethodName, Height: 5, Hash: types.Hash{0xb6}, DataHash: types.Hash{0x02}},
		} {
			h := newSubmitHarness(t, 1)
			lg := &memLog{}
			lg.Sent(r)
			h.sub.resume(lg, nil, nil)
			if h.sub.pending == nil || h.sub.pending.r != r {
				t.Fatalf("%s: pending = %+v, want the record followed", r.Method, h.sub.pending)
			}
			if u := h.unresolved(1); u != nil {
				t.Fatalf("%s: unresolved = %+v, want nil", r.Method, u)
			}
		}
	})
}

func TestReleaseDroppedWaitsBeforeSendingAgain(t *testing.T) {
	h := newSubmitHarness(t, 1)
	h.queue[1] = 1
	attempts := 0
	h.node.publish = func(*nom.AccountBlock) error {
		attempts++
		return errors.New("node refused")
	}

	h.step()
	if attempts != 1 {
		t.Fatalf("attempts = %d, want 1", attempts)
	}
	h.step()
	if r, _ := h.last(); r.Method != definition.SettlementReleaseWithdrawalsMethodName || r.Outcome != OutcomeDropped {
		t.Fatalf("record = %+v, want a dropped release", r)
	}
	if !h.logHas("sending again in") {
		t.Fatalf("the dropped release was not logged with its wait:\n%s", h.out.String())
	}
	calls := h.node.plasmaCalls
	h.step()
	h.step()
	if attempts != 1 || h.node.plasmaCalls != calls {
		t.Fatalf("attempts = %d, plasma calls %d -> %d; want no work before the wait ends", attempts, calls, h.node.plasmaCalls)
	}

	h.advance(10 * time.Second)
	h.step()
	if attempts != 2 {
		t.Fatalf("attempts = %d after the wait, want 2", attempts)
	}
}

func TestPickSkipsTheSecondPassAfterALogFailure(t *testing.T) {
	h := newSubmitHarness(t, 1, 2)
	h.hold(1, 0)
	h.setTip(2, 1)
	h.setMark(2, 1)
	b := h.storeRelayBatch(2, 0, twoMsgs()...)
	h.finalized(2, 0, b.OutboxRoot, 100)
	h.sub.log = &failingLog{sentErr: errors.New("disk full")}

	h.step()
	h.wantPublished(0)
	if h.node.plasmaCalls != 1 || h.relayCalls != 0 {
		t.Fatalf("plasma calls = %d, relay reads = %d; want 1 and 0, the withdrawal pass must not run after the log failed", h.node.plasmaCalls, h.relayCalls)
	}
}
