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
// pause with a paused domain, and a re-sync from an empty domain directory.
func TestSubmissionOnDevnet(t *testing.T) {
	t.Logf("phase 1: setup")
	d := newDevnet(t)
	executorBin, workerBin := buildBinaries(t, d.dir)
	configPath, storage := writeConfig(t, d.dir, workerBin)
	d.setup()

	// Phase 2: the first batch across a restart.
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

	// Phase 3: the submit pause and a PAUSED domain.
	t.Logf("phase 3: the submit pause and a PAUSED domain")
	d.setDomainPause(true)
	d.deposit(2)
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
	d.setDomainPause(false)
	d.waitFor("batch 1 outcome", momenta(20), func() (bool, string, error) {
		log := sendLog(t, storage)
		return len(log) >= 4, fmt.Sprintf("%d submission log records", len(log)), nil
	})
	r = sendLog(t, storage)
	if r[2].BatchID != 1 || r[2].Height != 3 || r[2].Outcome != "" {
		t.Fatalf("third record is %+v, want the unresolved send of batch 1 at account height 3", r[2])
	}
	if r[3].Hash != r[2].Hash || r[3].Outcome != supervisor.OutcomeAccepted {
		t.Fatalf("fourth record is %+v, want the accepted outcome of %s", r[3], r[2].Hash)
	}
	v = d.checkAccepted(1, r[2])
	if rec, ok, err := v.DomainRecord(domainID); err != nil || !ok || rec.Status != definition.DomainStatusPaused {
		t.Fatalf("domain record is %+v (present %v, error %v), want status PAUSED", rec, ok, err)
	}
	d.setDomainStatus(definition.DomainStatusActive)

	// Phase 4: an aligned re-sync.
	t.Logf("phase 4: an aligned re-sync")
	e2.stop()
	domainDir := filepath.Join(storage, "1")
	if err := os.RemoveAll(domainDir); err != nil {
		t.Fatalf("remove %s: %v", domainDir, err)
	}
	if _, err := os.Stat(domainDir); !os.IsNotExist(err) {
		t.Fatalf("%s still exists after removal (stat error %v)", domainDir, err)
	}
	d.startExecutor("executor-3", executorBin, configPath)
	d.deposit(3)
	d.waitFor("batch 2 outcome", momenta(40), func() (bool, string, error) {
		log := sendLog(t, storage)
		return len(log) >= 6, fmt.Sprintf("%d submission log records", len(log)), nil
	})
	r = sendLog(t, storage)
	if r[4].BatchID != 2 || r[4].Height != 4 {
		t.Fatalf("fifth record is %+v, want batch 2 at account height 4", r[4])
	}
	if r[5].Hash != r[4].Hash || r[5].Outcome != supervisor.OutcomeAccepted {
		t.Fatalf("sixth record is %+v, want the accepted outcome of %s", r[5], r[4].Hash)
	}
	v = d.checkAccepted(2, r[4])
	// Every state root in this run is the zero hash, so acceptance of batch 2 proves input
	// continuity only. The rebuilt records must also match Settlement's accepted entries.
	layout, err := recovery.NewLayout(storage, domainID)
	if err != nil {
		t.Fatalf("open the domain layout: %v", err)
	}
	records, err := recovery.ListBatchRecords(layout)
	if err != nil {
		t.Fatalf("list the rebuilt batch records: %v", err)
	}
	if len(records) < 3 {
		t.Fatalf("rebuilt batch records are %+v, want records for batches 0, 1 and 2", records)
	}
	for id := uint64(0); id < 3; id++ {
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
	if h := d.executorHeight(); h != 4 {
		t.Fatalf("executor address is at account height %d, want 4", h)
	}
	wantLog := []struct {
		batch   uint64
		height  uint64
		outcome supervisor.Outcome
	}{{0, 2, ""}, {0, 2, supervisor.OutcomeAccepted}, {1, 3, ""}, {1, 3, supervisor.OutcomeAccepted}, {2, 4, ""}, {2, 4, supervisor.OutcomeAccepted}}
	r = sendLog(t, storage)
	if len(r) != len(wantLog) {
		t.Fatalf("submission log holds %d records, want %d: %+v", len(r), len(wantLog), r)
	}
	for i, w := range wantLog {
		if r[i].BatchID != w.batch || r[i].Height != w.height || r[i].Outcome != w.outcome {
			t.Fatalf("record %d is %+v, want batch %d at account height %d with outcome %q", i, r[i], w.batch, w.height, w.outcome)
		}
		if i%2 == 1 && r[i].Hash != r[i-1].Hash {
			t.Fatalf("record %d has hash %s, want the hash %s of its send record", i, r[i].Hash, r[i-1].Hash)
		}
	}
	d.exec.stop()
}
