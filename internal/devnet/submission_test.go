//go:build devnet

package devnet

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/zenon-network/go-zenon/vm/embedded/definition"

	"github.com/digitalSloth/go-executor/internal/codec"
	"github.com/digitalSloth/go-executor/internal/recovery"
	"github.com/digitalSloth/go-executor/internal/settlement"
	"github.com/digitalSloth/go-executor/internal/supervisor"
)

// TestSubmissionOnDevnet runs the executor binaries against the devnet and checks that
// Settlement accepts the batches they submit, across a restart with an unresolved send, a submit
// pause with a paused domain, and a re-sync from an empty domain directory, and, once Settlement's
// relay pause is cleared, the registration, release and payout of each deposit's refund.
func TestSubmissionOnDevnet(t *testing.T) {
	t.Logf("phase 1: setup")
	d := newDevnet(t)
	executorBin, workerBin, gatewayBin := buildBinaries(t, d.dir)
	configPath, storage := writeConfig(t, d.dir, workerBin)
	d.setup()
	d.setRelayPause(true)

	t.Logf("phase 2: the first batch across a restart")
	d.deposit(1)
	d.startExecutor("executor-1", executorBin, configPath)
	d.waitFor("batch 0 sent", momenta(20), func() (bool, string, error) {
		log := sendLog(t, storage)
		if len(log) > 1 {
			t.Fatalf("batch 0 resolved before the restart: the test must stop the executor while the send is unresolved")
		}
		if len(log) == 0 {
			return false, "no send record", nil
		}
		s, err := d.c.SendStatus(log[0].Hash)
		if err != nil {
			return false, "", err
		}
		return log[0].BatchID == 0 && log[0].Outcome == "" && s.Block != nil, fmt.Sprintf("record %+v, send in a momentum %v", log[0], s.Block != nil), nil
	})
	d.exec.stop()
	if log := sendLog(t, storage); len(log) != 1 || log[0].Outcome != "" {
		t.Fatalf("after the stop the submission log holds %+v, want exactly the unresolved send of batch 0", log)
	}

	e2 := d.startExecutor("executor-2", executorBin, configPath)
	local := d.startGateway("gateway-local", gatewayBin, writeGatewayConfig(t, d.dir, "gateway-local", map[string]any{
		"domainId": domainID, "source": "LOCAL", "executorStoragePath": storage, "workerBinary": workerBin,
	}))
	d.waitFor("batch 0 outcome", momenta(20), func() (bool, string, error) {
		log := sendLog(t, storage)
		return len(log) >= 2, fmt.Sprintf("%d submission log records", len(log)), nil
	})
	r := sendLog(t, storage)
	if r[1].Hash != r[0].Hash || r[1].Outcome != supervisor.OutcomeAccepted {
		t.Fatalf("second record is %+v, want the accepted outcome of %s", r[1], r[0].Hash)
	}
	want := fmt.Sprintf("batch 0: following send %s at account height %d from the submission log", r[0].Hash, r[0].Height)
	if !e2.logHas(want) {
		t.Fatalf("executor-2 did not log %q\n%s", want, e2.tail(50))
	}
	if r[0].Height != 2 {
		t.Fatalf("batch 0 was sent at account height %d, want 2", r[0].Height)
	}
	if b, err := d.c.FrontierAccountBlock(executorAddress); err != nil || b == nil || b.Height != 2 || b.Hash != r[0].Hash {
		t.Fatalf("frontier account block of the executor address is %v (error %v), want height 2 and hash %s", b, err, r[0].Hash)
	}
	v := d.checkAccepted(0, r[0])
	if active, err := v.PillarActive("no-such-pillar"); err != nil || active {
		t.Fatalf("PillarActive(no-such-pillar) is %v with error %v, want false with none", active, err)
	}
	g, err := settlement.NewAnchorSource(d.r, confirmationDepth).Gates(domainID, pillarName)
	if err != nil {
		t.Fatalf("gates: %v", err)
	}
	if g.GlobalPause || g.DomainPause || g.Member == nil || g.Member.ExecutorAddress != executorAddress ||
		g.Member.Status != definition.ExecutorStatusActive || !g.PillarActive {
		t.Fatalf("gates are %+v, want no pause, an active member naming %s and an active pillar", g, executorAddress)
	}

	t.Logf("phase 3: the submit pause and a PAUSED domain")
	d.setDomainPause(true)
	d.deposit(2)
	call := d.callL2(3)
	d.setDomainStatus(definition.DomainStatusPaused)
	d.waitFor("gate warning for batch 1", momenta(15), func() (bool, string, error) {
		return e2.logHas("domain 1 batch 1 not sent") && e2.logHas("submit pause for domain 1 is set"), "no gate warning in the executor log", nil
	})
	d.waitHeight(d.height() + 3)
	if h := d.executorHeight(); h != 2 {
		t.Fatalf("executor address is at account height %d while the submit pause holds, want 2", h)
	}
	if log := sendLog(t, storage); len(log) != 2 {
		t.Fatalf("submission log holds %d records while the submit pause holds, want 2: %+v", len(log), log)
	}
	pend := d.waitReceipt(local, call, "pending", momenta(15))
	callBatch := pend.BatchID
	if callBatch != 1 && callBatch != 2 {
		t.Fatalf("the CallL2 is in batch %d, want batch 1 or 2", callBatch)
	}
	t.Logf("phase 3: the CallL2 is in batch %d", callBatch)
	d.setDomainPause(false)
	d.waitFor("every batch of phase 3 resolved", momenta(20*callBatch), func() (bool, string, error) {
		log := sendLog(t, storage)
		return uint64(len(log)) >= 2+2*callBatch, fmt.Sprintf("%d submission log records", len(log)), nil
	})
	r = sendLog(t, storage)
	wantLog := expectedLog(callBatch)
	if err := checkLog(r[:len(wantLog)], wantLog); err != nil {
		t.Fatal(err)
	}
	for b := uint64(1); b <= callBatch; b++ {
		v = d.checkAccepted(b, r[2*b])
	}
	if rec, ok, err := v.DomainRecord(domainID); err != nil || !ok || rec.Status != definition.DomainStatusPaused {
		t.Fatalf("domain record is %+v (present %v, error %v), want status PAUSED", rec, ok, err)
	}
	d.waitReceipt(local, call, "latest", momenta(confirmationDepth+10))
	d.setDomainStatus(definition.DomainStatusActive)

	t.Logf("phase 4: an aligned re-sync")
	e2.stop()
	domainDir := filepath.Join(storage, "1")
	if err := os.RemoveAll(domainDir); err != nil {
		t.Fatalf("remove %s: %v", domainDir, err)
	}
	if _, err := os.Stat(domainDir); !os.IsNotExist(err) {
		t.Fatalf("%s still exists after removal (stat error %v)", domainDir, err)
	}
	last := callBatch + 1
	d.startExecutor("executor-3", executorBin, configPath)
	d.deposit(4)
	d.waitFor(fmt.Sprintf("batch %d outcome", last), momenta(40), func() (bool, string, error) {
		log := sendLog(t, storage)
		return uint64(len(log)) >= 2*(last+1), fmt.Sprintf("%d submission log records", len(log)), nil
	})
	r = sendLog(t, storage)
	if err := checkLog(r[:2*(last+1)], expectedLog(last)); err != nil {
		t.Fatal(err)
	}
	v = d.checkAccepted(last, r[2*last])
	// Every state root in this run is the zero hash, so acceptance of
	// the last batch proves input continuity only. The rebuilt records
	// must also match Settlement's accepted entries.
	layout, err := recovery.NewLayout(storage, domainID)
	if err != nil {
		t.Fatalf("open the domain layout: %v", err)
	}
	records, err := recovery.ListBatchRecords(layout)
	if err != nil {
		t.Fatalf("list the rebuilt batch records: %v", err)
	}
	if uint64(len(records)) < last+1 {
		t.Fatalf("rebuilt batch records are %+v, want records for batches 0 to %d", records, last)
	}
	for id := uint64(0); id <= last; id++ {
		rec := records[id]
		if rec.BatchID != id {
			t.Fatalf("rebuilt record %d has batch id %d", id, rec.BatchID)
		}
		entry, ok, err := v.BatchEntry(domainID, id)
		if err != nil || !ok {
			t.Fatalf("batch entry %d of domain %d: present %v, error %v", id, domainID, ok, err)
		}
		commitment, err := codec.DecodeBatchCommitment(rec.Commitment)
		if err != nil {
			t.Fatalf("decode the rebuilt commitment of batch %d: %v", id, err)
		}
		if err := settlement.EntryMatches(entry, commitment); err != nil {
			t.Fatalf("rebuilt batch %d differs from Settlement's accepted entry: %v", id, err)
		}
	}
	if h := d.executorHeight(); h != 2+last {
		t.Fatalf("executor address is at account height %d, want %d", h, 2+last)
	}
	if err := checkLog(sendLog(t, storage), expectedLog(last)); err != nil {
		t.Fatal(err)
	}
	d.waitReceipt(local, call, "finalized", momenta(withdrawalDelay+confirmationDepth+10))
	d.waitHeadOK(local, momenta(5))
	remote := d.startGateway("gateway-remote", gatewayBin, writeGatewayConfig(t, d.dir, "gateway-remote", map[string]any{
		"domainId": domainID, "source": "REMOTE", "upstreams": []string{local.url}, "workerBinary": workerBin,
	}))
	t.Logf("phase 5: withdrawals")
	d.withdrawalPhase(storage, last)
	d.compareGateways(local, remote, last)
	remote.stop()
	local.stop()
	d.exec.stop()
}
