//go:build devnet

package devnet

import (
	"fmt"
	"math/big"
	"sort"

	"github.com/zenon-network/go-zenon/chain/nom"
	"github.com/zenon-network/go-zenon/common/types"
	"github.com/zenon-network/go-zenon/vm/embedded/definition"

	"github.com/Zeno-L2/zeno-executor/internal/codec"
	"github.com/Zeno-L2/zeno-executor/internal/supervisor"
)

// withdrawalPhase clears domain 1's relay pause and checks that the
// running executor registers each deposit's refund exactly once,
// releases it, and that Settlement pays it to the depositor.
func (d *devnet) withdrawalPhase(storage string, last uint64) {
	t := d.t
	t.Helper()
	batches := last + 1

	from := len(sendLog(t, storage))
	if uint64(from) != 2*batches {
		t.Fatalf("the submission log holds %d records before the relay pause clears, want %d", from, 2*batches)
	}
	if n := withdrawalFiles(t, storage); uint64(n) != batches {
		t.Fatalf("the withdrawals store holds %d batch files before the relay pause clears, want %d", n, batches)
	}
	frontier, err := d.c.FrontierAccountBlock(types.SettlementContract)
	if err != nil || frontier == nil {
		t.Fatalf("frontier account block of %s: %v, error %v", types.SettlementContract, frontier, err)
	}
	settlementFrom := frontier.Height

	d.setRelayPause(false)
	d.waitFor("every refund registered and released", momenta(withdrawalDelay+16*(confirmationDepth+2)), func() (bool, string, error) {
		accepted := 0
		for _, r := range sendLog(t, storage)[from:] {
			if r.Method == definition.SettlementRelayMessageMethodName && r.Outcome == supervisor.OutcomeAccepted {
				accepted++
			}
		}
		files := withdrawalFiles(t, storage)
		if accepted != 3 || files != 0 {
			return false, fmt.Sprintf("%d relays accepted, %d withdrawals files", accepted, files), nil
		}
		h, err := d.r.ConfirmedHeight(confirmationDepth)
		if err != nil {
			return false, "", err
		}
		v, err := d.r.At(h)
		if err != nil {
			return false, "", err
		}
		q, err := v.ReleaseQueueMeta(domainID)
		if err != nil {
			return false, "", err
		}
		return q.Head == 3 && q.Tail == 3, fmt.Sprintf("height %d: release queue head %d tail %d", h, q.Head, q.Tail), nil
	})
	t.Logf("phase 5: every refund registered and released at momentum %d", d.height())

	// A stray send would reach the log in this window.
	d.waitHeight(d.height() + 2*(confirmationDepth+2))

	r := sendLog(t, storage)[from:]
	var sends []supervisor.SendRecord
	resolved := map[types.Hash]supervisor.SendRecord{}
	for _, rec := range r {
		if rec.Method != definition.SettlementRelayMessageMethodName && rec.Method != definition.SettlementReleaseWithdrawalsMethodName {
			t.Fatalf("submission log record %+v has an unexpected method", rec)
		}
		if rec.Outcome == "" {
			sends = append(sends, rec)
			continue
		}
		if _, dup := resolved[rec.Hash]; dup {
			t.Fatalf("send %s has more than one outcome record in %+v", rec.Hash, r)
		}
		resolved[rec.Hash] = rec
	}
	if len(r) != 2*len(sends) {
		t.Fatalf("the log holds %d records for %d sends: %+v", len(r), len(sends), r)
	}
	var relays []supervisor.SendRecord
	releases := 0
	seenBatch := map[uint64]bool{}
	seenOutbox := map[types.Hash]bool{}
	for _, s := range sends {
		out, ok := resolved[s.Hash]
		if !ok {
			t.Fatalf("send %+v has no outcome record in %+v", s, r)
		}
		if s.Method == definition.SettlementRelayMessageMethodName {
			if out.Outcome != supervisor.OutcomeAccepted {
				t.Fatalf("relay %+v has outcome %q, want %q", s, out.Outcome, supervisor.OutcomeAccepted)
			}
			if seenBatch[s.BatchID] || seenOutbox[s.OutboxID] {
				t.Fatalf("relay %+v repeats a batch or an outbox id of an earlier relay in %+v", s, r)
			}
			seenBatch[s.BatchID] = true
			seenOutbox[s.OutboxID] = true
			relays = append(relays, s)
			continue
		}
		if out.Outcome != supervisor.OutcomeReceived {
			t.Fatalf("release %+v has outcome %q, want %q", s, out.Outcome, supervisor.OutcomeReceived)
		}
		releases++
	}
	if len(relays) != 3 || !seenBatch[0] || !seenBatch[1] || !seenBatch[last] {
		t.Fatalf("the log holds %d relay sends for batches %v, want one each for batches 0, 1 and %d: %+v", len(relays), seenBatch, last, r)
	}
	if releases < 1 {
		t.Fatalf("the log holds no ReleaseWithdrawals send: %+v", r)
	}
	blocks := map[types.Hash]*nom.AccountBlock{}
	for i, s := range sends {
		if want := 2 + batches + uint64(i); s.Height != want {
			t.Fatalf("send %d is %+v, want account height %d", i, s, want)
		}
		b, err := d.c.AccountBlockAtHeight(executorAddress, s.Height)
		if err != nil || b == nil {
			t.Fatalf("account block of %s at height %d: %v, error %v", executorAddress, s.Height, b, err)
		}
		if b.Hash != s.Hash {
			t.Fatalf("account block at height %d has hash %s, the send record has %s", s.Height, b.Hash, s.Hash)
		}
		blocks[s.Hash] = b
	}
	if h, want := d.executorHeight(), 1+batches+uint64(len(sends)); h != want {
		t.Fatalf("executor address is at account height %d, want %d for %d sends", h, want, len(sends))
	}

	ch, err := d.r.ConfirmedHeight(confirmationDepth)
	if err != nil {
		t.Fatalf("confirmed height: %v", err)
	}
	v, err := d.r.At(ch)
	if err != nil {
		t.Fatalf("view at height %d: %v", ch, err)
	}
	var relayed []*big.Int
	for _, s := range relays {
		b := blocks[s.Hash]
		if b.ToAddress != types.SettlementContract {
			t.Fatalf("relay at height %d goes to %s, want %s", s.Height, b.ToAddress, types.SettlementContract)
		}
		var call struct {
			DomainId       uint32
			BatchId        uint64
			Message        []byte
			InclusionProof []byte
		}
		if err := definition.ABISettlement.UnpackMethod(&call, definition.SettlementRelayMessageMethodName, b.Data); err != nil {
			t.Fatalf("unpack the relay of batch %d: %v", s.BatchID, err)
		}
		if call.DomainId != domainID || call.BatchId != s.BatchID {
			t.Fatalf("relay at height %d names domain %d batch %d, want domain %d batch %d", s.Height, call.DomainId, call.BatchId, domainID, s.BatchID)
		}
		m, err := codec.DecodeOutboxMessage(call.Message)
		if err != nil {
			t.Fatalf("decode the outbox message of batch %d: %v", s.BatchID, err)
		}
		w, ok := m.Target.(codec.L1Withdrawal)
		if !ok {
			t.Fatalf("outbox message of batch %d targets %T, want an L1 withdrawal", s.BatchID, m.Target)
		}
		amount := new(big.Int).SetBytes(w.Amount[:])
		if m.SourceContract != types.ZeroHash || w.RecipientL1 != adminAddress ||
			w.Asset != codec.AssetIDFromZTS(types.ZnnTokenStandard) || amount.Sign() <= 0 {
			t.Fatalf("outbox message of batch %d is %+v with withdrawal %+v, want a positive ZNN refund to %s with a zero source contract", s.BatchID, m, w, adminAddress)
		}
		if id := codec.OutboxID(domainID, call.BatchId, m.InputIndex, m.OutboxIndex); id != s.OutboxID {
			t.Fatalf("outbox id of batch %d is %s, the send record has %s", s.BatchID, id, s.OutboxID)
		}
		processed, err := v.OutboxProcessed(s.OutboxID)
		if err != nil || !processed {
			t.Fatalf("outbox %s of batch %d: processed %v, error %v", s.OutboxID, s.BatchID, processed, err)
		}
		relayed = append(relayed, amount)
	}

	top, err := d.c.FrontierAccountBlock(types.SettlementContract)
	if err != nil || top == nil {
		t.Fatalf("frontier account block of %s: %v, error %v", types.SettlementContract, top, err)
	}
	var paid []*big.Int
	for height := settlementFrom + 1; height <= top.Height; height++ {
		b, err := d.c.AccountBlockAtHeight(types.SettlementContract, height)
		if err != nil || b == nil {
			t.Fatalf("account block of %s at height %d: %v, error %v", types.SettlementContract, height, b, err)
		}
		if b.BlockType != nom.BlockTypeContractSend || b.ToAddress != adminAddress {
			continue
		}
		if b.TokenStandard != types.ZnnTokenStandard {
			t.Fatalf("Settlement send at height %d to %s carries token %s, want %s", height, adminAddress, b.TokenStandard, types.ZnnTokenStandard)
		}
		if _, ok, err := d.c.ConfirmationHeight(b.Hash); err != nil || !ok {
			t.Fatalf("Settlement send %s at height %d is not in a momentum: ok %v, error %v", b.Hash, height, ok, err)
		}
		paid = append(paid, b.Amount)
	}
	if len(paid) != 3 {
		t.Fatalf("Settlement made %d sends to %s in account heights (%d, %d], want 3", len(paid), adminAddress, settlementFrom, top.Height)
	}
	byValue := func(s []*big.Int) {
		sort.Slice(s, func(i, j int) bool { return s[i].Cmp(s[j]) < 0 })
	}
	byValue(paid)
	byValue(relayed)
	for i := range paid {
		if paid[i].Cmp(relayed[i]) != 0 {
			t.Fatalf("payout amounts %v differ from the relayed refund amounts %v", paid, relayed)
		}
	}
}
