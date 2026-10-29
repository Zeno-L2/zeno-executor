package worker

import (
	"context"
	"encoding/binary"
	"errors"
	"net"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/zenon-network/go-zenon/chain/nom"
	"github.com/zenon-network/go-zenon/common/trie"
	"github.com/zenon-network/go-zenon/common/types"
	"github.com/zenon-network/go-zenon/vm/embedded/definition"

	"github.com/Zeno-L2/zeno-executor/domain"
	"github.com/Zeno-L2/zeno-executor/internal/codec"
	"github.com/Zeno-L2/zeno-executor/internal/ipc"
	"github.com/Zeno-L2/zeno-executor/internal/ipc/ipctest"
	"github.com/Zeno-L2/zeno-executor/internal/ledger"
	"github.com/Zeno-L2/zeno-executor/internal/ledger/ledgertest"
	"github.com/Zeno-L2/zeno-executor/internal/recovery"
	"github.com/Zeno-L2/zeno-executor/runtime"
)

func init() {
	retry = recovery.RetryPolicy{SourceAttempts: 2, SourceInterval: time.Millisecond, AwaitDAAttempts: 1, AwaitDAInterval: time.Millisecond}
}

// --- fixtures ---

// payloadDomain is scriptDomain-like (runtime/runtime_test.go:31-49), but writes the input's
// payload as its leaf value, so every state root depends on the payload bytes.
type payloadDomain struct{}

type payloadInput struct {
	idx     uint64
	payload []byte
}

func (payloadDomain) Genesis() domain.StateInit { return domain.StateInit{} }

func (payloadDomain) DecodeInput(ctx domain.InputContext, payload []byte) (domain.Input, error) {
	return payloadInput{idx: ctx.GlobalInputIndex, payload: append([]byte(nil), payload...)}, nil
}

func (payloadDomain) Apply(_ domain.StateAccess, in domain.Input) (domain.Effects, error) {
	p := in.(payloadInput)
	var path trie.Path
	binary.BigEndian.PutUint64(path[:8], p.idx)
	return domain.Effects{
		Status:    codec.StatusSuccess,
		StateDiff: []domain.StateDiffEntry{{Path: path, Value: p.payload}},
	}, nil
}

func testNewPlugin(ipc.WorkerConfig) (domain.Domain, error) { return payloadDomain{}, nil }

func addr(b byte) types.Address {
	var a types.Address
	a[0] = b
	return a
}

// buildChain builds n momenta; d1At(h) domain-1 blocks and one domain-2 block per height.
func buildChain(t *testing.T, n uint64, d1At func(h uint64) int) []*nom.DetailedMomentum {
	t.Helper()
	perHeight := make([][]ipctest.Block, n)
	for i := uint64(0); i < n; i++ {
		h := i + 1
		var blocks []ipctest.Block
		for j := 0; j < d1At(h); j++ {
			blocks = append(blocks, ipctest.Block{Address: addr(1), Data: ledgertest.SendData(1, nil)})
		}
		blocks = append(blocks, ipctest.Block{Address: addr(2), Data: ledgertest.SendData(2, nil)})
		blocks = append(blocks, ipctest.Block{Address: types.SettlementContract})
		perHeight[i] = blocks
	}
	return ipctest.Chain(perHeight)
}

func dense(uint64) int { return 1 }

func validWorkerConfig(storagePath string) ipc.WorkerConfig {
	return ipc.WorkerConfig{
		DomainID:          1,
		ExecutorAddress:   types.Address{0x42},
		STFSpecHash:       types.Hash{0xAA},
		ConfirmationDepth: 2,
		NetworkName:       "test",
		StoragePath:       storagePath,
		MaxPendingBatches: 8,
	}
}

// frontierFor is the staged frontier this suite drives runs with: 6 while there is no cursor or
// its MomentumHeight is below 4; 8 while it is below 6; 12 from then on.
func frontierFor(pos recovery.Position, ok bool) uint64 {
	switch {
	case !ok || pos.MomentumHeight < 4:
		return 6
	case pos.MomentumHeight < 6:
		return 8
	default:
		return 12
	}
}

func fixedFrontier(h uint64) func(recovery.Position, bool) uint64 {
	return func(recovery.Position, bool) uint64 { return h }
}

// --- fake supervisor ---

// fakeSupervisor drives the worker side of a net.Pipe as the supervisor would, answering every
// request type. A test reads its recordings only after run has returned, the worker end is
// closed and the fake goroutine has exited.
type fakeSupervisor struct {
	t        *testing.T
	conn     net.Conn
	cfg      ipc.WorkerConfig
	chain    []*nom.DetailedMomentum
	frontier func(recovery.Position, bool) uint64
	anchor   func() ipc.Message
	// assign, when set, rewrites each assignment reply's entries before it is sent.
	assign func(req ipc.AssignmentRequest, entries []ledger.AssignmentEntry) []ledger.AssignmentEntry

	mu        sync.Mutex
	requests  []ipc.MomentaRequest
	committed []codec.BatchCommitment
	retracts  []uint64
	// committedAtRetract[i] is len(committed) when retracts[i] arrived.
	committedAtRetract []int
	recovered          []codec.BatchCommitment
	// reports holds every Committed, Recovered, Withdrawal and
	// WithdrawalsEnd frame in arrival order.
	reports []ipc.Message

	layout recovery.Layout
	done   chan struct{}
}

func newFake(t *testing.T, conn net.Conn, cfg ipc.WorkerConfig, chain []*nom.DetailedMomentum, frontier func(recovery.Position, bool) uint64) *fakeSupervisor {
	t.Helper()
	layout, err := recovery.NewLayout(cfg.StoragePath, cfg.DomainID)
	if err != nil {
		t.Fatalf("fake: layout: %v", err)
	}
	return &fakeSupervisor{
		t: t, conn: conn, cfg: cfg, chain: chain, frontier: frontier,
		anchor: func() ipc.Message { return ipc.Anchor{Found: false} },
		layout: layout, done: make(chan struct{}),
	}
}

// run sends cfg and then answers requests until the connection breaks or an unexpected message
// arrives. It always closes done on return.
func (f *fakeSupervisor) run() {
	defer close(f.done)
	if err := ipc.WriteMessage(f.conn, f.cfg); err != nil {
		return
	}
	for {
		m, err := ipc.ReadMessage(f.conn, ipc.Worker)
		if err != nil {
			return
		}
		switch v := m.(type) {
		case ipc.FrontierRequest:
			pos, ok, _ := recovery.ReadCursor(f.layout)
			if err := ipc.WriteMessage(f.conn, ipc.Frontier{Height: f.frontier(pos, ok)}); err != nil {
				return
			}
		case ipc.MomentaRequest:
			f.mu.Lock()
			f.requests = append(f.requests, v)
			f.mu.Unlock()
			start := v.Height - 1
			end := start + v.Count
			if end > uint64(len(f.chain)) {
				end = uint64(len(f.chain))
			}
			var n uint32
			for i := start; i < end; i++ {
				if err := ipc.WriteMessage(f.conn, ipc.Momentum{Detailed: f.chain[i]}); err != nil {
					return
				}
				n++
			}
			if err := ipc.WriteMessage(f.conn, ipc.RangeEnd{Count: n}); err != nil {
				return
			}
		case ipc.AssignmentRequest:
			entries, err := ledgertest.Assign(f.chain, f.cfg.DomainID, v.Height, v.Hash, v.NextOrdinal)
			if f.assign != nil {
				entries = f.assign(v, entries)
			}
			var reply ipc.Message = ipc.Assignment{Entries: entries}
			if err != nil {
				kind := ipc.SourceUnavailable
				if errors.Is(err, recovery.ErrHalted) {
					kind = ipc.SourceHalted
				}
				reply = ipc.SourceError{Kind: kind, Message: err.Error()}
			}
			if err := ipc.WriteMessage(f.conn, reply); err != nil {
				return
			}
		case ipc.AnchorRequest:
			if err := ipc.WriteMessage(f.conn, f.anchor()); err != nil {
				return
			}
		case ipc.AcceptedRequest:
			if err := ipc.WriteMessage(f.conn, ipc.Accepted{}); err != nil {
				return
			}
		case ipc.Committed:
			f.mu.Lock()
			f.committed = append(f.committed, v.Commitment)
			f.reports = append(f.reports, v)
			f.mu.Unlock()
		case ipc.Retract:
			f.mu.Lock()
			f.retracts = append(f.retracts, v.FromBatchID)
			f.committedAtRetract = append(f.committedAtRetract, len(f.committed))
			f.mu.Unlock()
		case ipc.Recovered:
			f.mu.Lock()
			f.recovered = append(f.recovered, v.Commitment)
			f.reports = append(f.reports, v)
			f.mu.Unlock()
		case ipc.Withdrawal, ipc.WithdrawalsEnd:
			f.mu.Lock()
			f.reports = append(f.reports, v)
			f.mu.Unlock()
		default:
			return
		}
	}
}

func (f *fakeSupervisor) momentaRequests() []ipc.MomentaRequest {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]ipc.MomentaRequest(nil), f.requests...)
}

func (f *fakeSupervisor) committedCommitments() []codec.BatchCommitment {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]codec.BatchCommitment(nil), f.committed...)
}

// --- reference in-process source ---

// memSource is an in-process ledger.MomentumSource over a fixed chain, whose frontier is
// derived from the caller's own on-disk cursor via frontierFn.
type memSource struct {
	chain      []*nom.DetailedMomentum
	layout     recovery.Layout
	frontierFn func(recovery.Position, bool) uint64
}

func (s memSource) FrontierHeight() (uint64, error) {
	pos, ok, err := recovery.ReadCursor(s.layout)
	if err != nil {
		return 0, err
	}
	return s.frontierFn(pos, ok), nil
}

func (s memSource) DetailedMomentumsByHeight(height, count uint64) ([]*nom.DetailedMomentum, error) {
	if height == 0 {
		return nil, nil
	}
	start := height - 1
	end := start + count
	if end > uint64(len(s.chain)) {
		end = uint64(len(s.chain))
	}
	if start >= end {
		return nil, nil
	}
	return s.chain[start:end], nil
}

func (s memSource) Assignment(domainID uint32, height uint64, hash types.Hash, nextOrdinal uint64) ([]ledger.AssignmentEntry, error) {
	return ledgertest.Assign(s.chain, domainID, height, hash, nextOrdinal)
}

type noAnchor struct{}

func (noAnchor) FinalizedAnchor(uint32) (recovery.Anchor, bool, error) {
	return recovery.Anchor{}, false, nil
}

// noCursor reports a nextBatchId of 0 for every domain.
type noCursor struct{}

func (noCursor) NextBatchID(uint32) (uint64, error) { return 0, nil }

func (noCursor) AcceptedEntries(uint32, uint64) (uint64, []*definition.BatchEntry, error) {
	return 0, nil, nil
}

// --- polling helpers ---

func waitForHeight(t *testing.T, layout recovery.Layout, height uint64, timeout time.Duration) recovery.Position {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if pos, ok, err := recovery.ReadCursor(layout); err == nil && ok && pos.MomentumHeight >= height {
			return pos
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("cursor did not reach height %d in time", height)
	return recovery.Position{}
}

// --- status tests ---

func TestStatuses(t *testing.T) {
	t.Run("anchor source error halted", func(t *testing.T) {
		storagePath := t.TempDir()
		cfg := validWorkerConfig(storagePath)
		a, b := net.Pipe()
		defer a.Close()
		fake := newFake(t, b, cfg, nil, fixedFrontier(0))
		fake.anchor = func() ipc.Message { return ipc.SourceError{Kind: ipc.SourceHalted, Message: "down"} }
		go fake.run()

		status := run(context.Background(), a, testNewPlugin)
		if status != ipc.ExitHalted {
			t.Fatalf("status = %d, want %d", status, ipc.ExitHalted)
		}
	})

	t.Run("anchor source error unavailable", func(t *testing.T) {
		storagePath := t.TempDir()
		cfg := validWorkerConfig(storagePath)
		a, b := net.Pipe()
		defer a.Close()
		fake := newFake(t, b, cfg, nil, fixedFrontier(0))
		fake.anchor = func() ipc.Message { return ipc.SourceError{Kind: ipc.SourceUnavailable, Message: "down"} }
		go fake.run()

		status := run(context.Background(), a, testNewPlugin)
		if status != ipc.ExitTempFail {
			t.Fatalf("status = %d, want %d", status, ipc.ExitTempFail)
		}
	})

	t.Run("truncated config", func(t *testing.T) {
		a, b := net.Pipe()
		defer a.Close()
		go func() {
			// length=5 (type + 4 bytes): a Config payload truncated well short of its fixed
			// prefix (domainId(4) + address(20) + hash(32) + depth(8) = 64 bytes).
			b.Write([]byte{0, 0, 0, 5, 0x01, 0, 0, 0, 0})
			b.Close()
		}()
		status := run(context.Background(), a, testNewPlugin)
		if status != ipc.ExitConfig {
			t.Fatalf("status = %d, want %d", status, ipc.ExitConfig)
		}
	})

	t.Run("frontier as first frame", func(t *testing.T) {
		a, b := net.Pipe()
		defer a.Close()
		go func() {
			ipc.WriteMessage(b, ipc.Frontier{Height: 1})
			b.Close()
		}()
		status := run(context.Background(), a, testNewPlugin)
		if status != ipc.ExitConfig {
			t.Fatalf("status = %d, want %d", status, ipc.ExitConfig)
		}
	})

	t.Run("confirmation depth 0", func(t *testing.T) {
		storagePath := t.TempDir()
		cfg := validWorkerConfig(storagePath)
		cfg.ConfirmationDepth = 0
		a, b := net.Pipe()
		defer a.Close()
		go func() {
			ipc.WriteMessage(b, cfg)
			b.Close()
		}()
		status := run(context.Background(), a, testNewPlugin)
		if status != ipc.ExitConfig {
			t.Fatalf("status = %d, want %d", status, ipc.ExitConfig)
		}
	})

	t.Run("newPlugin error", func(t *testing.T) {
		storagePath := t.TempDir()
		cfg := validWorkerConfig(storagePath)
		a, b := net.Pipe()
		defer a.Close()
		go func() {
			ipc.WriteMessage(b, cfg)
			b.Close()
		}()
		failingPlugin := func(ipc.WorkerConfig) (domain.Domain, error) { return nil, errors.New("boom") }
		status := run(context.Background(), a, failingPlugin)
		if status != ipc.ExitConfig {
			t.Fatalf("status = %d, want %d", status, ipc.ExitConfig)
		}
	})

	t.Run("fake closes on first request", func(t *testing.T) {
		storagePath := t.TempDir()
		cfg := validWorkerConfig(storagePath)
		a, b := net.Pipe()
		defer a.Close()
		go func() {
			ipc.WriteMessage(b, cfg)
			ipc.ReadMessage(b, ipc.Worker) // the first request (AnchorRequest, at FIND_RECOVERY_ANCHOR)
			b.Close()
		}()
		status := run(context.Background(), a, testNewPlugin)
		if status != 1 {
			t.Fatalf("status = %d, want 1", status)
		}
	})
}

// A first assignment entry off the worker's next ordinal is a consistency fault: the worker
// halts and the channel stays healthy.
func TestAssignmentFirstOrdinalMismatchHalts(t *testing.T) {
	shift := func(entries []ledger.AssignmentEntry, delta int) []ledger.AssignmentEntry {
		out := append([]ledger.AssignmentEntry(nil), entries...)
		for i := range out {
			out[i].Ordinal = uint64(int64(out[i].Ordinal) + int64(delta))
		}
		return out
	}
	cases := []struct {
		name   string
		assign func(ipc.AssignmentRequest, []ledger.AssignmentEntry) []ledger.AssignmentEntry
	}{
		{"ahead", func(_ ipc.AssignmentRequest, e []ledger.AssignmentEntry) []ledger.AssignmentEntry {
			return shift(e, 1)
		}},
		{"behind", func(r ipc.AssignmentRequest, e []ledger.AssignmentEntry) []ledger.AssignmentEntry {
			if r.NextOrdinal == 0 {
				return e
			}
			return shift(e, -1)
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := validWorkerConfig(t.TempDir())
			a, b := net.Pipe()
			defer a.Close()
			fake := newFake(t, b, cfg, buildChain(t, 12, dense), fixedFrontier(12))
			fake.assign = tc.assign
			go fake.run()

			statusCh := make(chan int, 1)
			go func() { statusCh <- run(context.Background(), a, testNewPlugin) }()
			select {
			case status := <-statusCh:
				if status != ipc.ExitHalted {
					t.Fatalf("status = %d, want %d", status, ipc.ExitHalted)
				}
			case <-time.After(15 * time.Second):
				t.Fatalf("run did not return")
			}
			select {
			case <-fake.done:
				t.Fatalf("the channel closed: the fault was reported as a channel failure")
			default:
			}
			a.Close()
			<-fake.done
		})
	}
}

// --- exitStatus table ---

func TestExitStatus(t *testing.T) {
	cases := []struct {
		name   string
		runErr error
		broken error
		want   int
	}{
		{"config", runtime.ErrConfig, nil, ipc.ExitConfig},
		{"halted", recovery.ErrHalted, nil, ipc.ExitHalted},
		{"halted and broken wins halted", errors.Join(recovery.ErrHalted, ipc.ErrChannel), ipc.ErrChannel, ipc.ExitHalted},
		{"broken with nil runErr", nil, ipc.ErrChannel, 1},
		{"source unavailable with broken wins 1", recovery.ErrSourceUnavailable, ipc.ErrChannel, 1},
		{"source unavailable", recovery.ErrSourceUnavailable, nil, ipc.ExitTempFail},
		{"locked", runtime.ErrLocked, nil, ipc.ExitTempFail},
		{"plain error", errors.New("boom"), nil, 1},
		{"clean", nil, nil, 0},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := exitStatus(c.runErr, c.broken); got != c.want {
				t.Fatalf("exitStatus(%v, %v) = %d, want %d", c.runErr, c.broken, got, c.want)
			}
		})
	}
}

// --- staged frontier / commit batching ---

func TestStagedFrontierCommitsThreeBatches(t *testing.T) {
	storagePath := t.TempDir()
	cfg := validWorkerConfig(storagePath)
	chain := buildChain(t, 12, dense)

	a, b := net.Pipe()
	defer a.Close()
	fake := newFake(t, b, cfg, chain, frontierFor)
	go fake.run()

	ctx, cancel := context.WithCancel(context.Background())
	statusCh := make(chan int, 1)
	go func() { statusCh <- run(ctx, a, testNewPlugin) }()

	layout, err := recovery.NewLayout(storagePath, cfg.DomainID)
	if err != nil {
		t.Fatalf("layout: %v", err)
	}
	waitForHeight(t, layout, 10, 15*time.Second)
	cancel()

	select {
	case status := <-statusCh:
		if status != 0 {
			t.Fatalf("status = %d, want 0", status)
		}
	case <-time.After(15 * time.Second):
		t.Fatalf("run did not return")
	}
	a.Close()
	<-fake.done

	records, err := recovery.ListBatchRecords(layout)
	if err != nil {
		t.Fatalf("ListBatchRecords: %v", err)
	}
	if len(records) != 3 {
		t.Fatalf("%d batch records, want 3", len(records))
	}

	fake.mu.Lock()
	defer fake.mu.Unlock()
	if len(fake.retracts) == 0 || fake.retracts[0] != 0 || fake.committedAtRetract[0] != 0 {
		t.Fatalf("retracts = %v, committed before the first retract = %v, want Retract{0} before any Committed",
			fake.retracts, fake.committedAtRetract)
	}

	// Each report for batch B is followed by its Withdrawal frames, then
	// WithdrawalsEnd{B}, before the next report.
	var open *uint64
	ends := 0
	for i, m := range fake.reports {
		switch v := m.(type) {
		case ipc.Committed:
			if open != nil {
				t.Fatalf("frame %d: Committed %d while batch %d has no WithdrawalsEnd", i, v.Commitment.BatchID, *open)
			}
			id := v.Commitment.BatchID
			open = &id
		case ipc.Recovered:
			if open != nil {
				t.Fatalf("frame %d: Recovered %d while batch %d has no WithdrawalsEnd", i, v.Commitment.BatchID, *open)
			}
			id := v.Commitment.BatchID
			open = &id
		case ipc.Withdrawal:
			if open == nil || v.BatchID != *open {
				t.Fatalf("frame %d: Withdrawal for batch %d outside its hand-off (open %v)", i, v.BatchID, open)
			}
		case ipc.WithdrawalsEnd:
			if open == nil || v.BatchID != *open {
				t.Fatalf("frame %d: WithdrawalsEnd %d outside its hand-off (open %v)", i, v.BatchID, open)
			}
			open = nil
			ends++
		}
	}
	if open != nil {
		t.Fatalf("batch %d has no WithdrawalsEnd", *open)
	}
	if ends < 3 {
		t.Fatalf("%d WithdrawalsEnd frames, want at least 3", ends)
	}
}

// --- equivalence: split worker run vs. one in-process runtime.Run ---

func TestEquivalence(t *testing.T) {
	// 12 heights, blocks for domains {1} and {2} at every height, and two domain-1 blocks at
	// heights 5 and 9.
	d1At := func(h uint64) int {
		if h == 5 || h == 9 {
			return 2
		}
		return 1
	}
	chain := buildChain(t, 12, d1At)

	workerStorage := t.TempDir()
	cfg := validWorkerConfig(workerStorage)
	layout, err := recovery.NewLayout(workerStorage, cfg.DomainID)
	if err != nil {
		t.Fatalf("layout: %v", err)
	}

	// Run A: frontier fixed at 6. Wait until the cursor reaches height 4, then cancel.
	aConn, aFakeConn := net.Pipe()
	fakeA := newFake(t, aFakeConn, cfg, chain, fixedFrontier(6))
	go fakeA.run()
	ctxA, cancelA := context.WithCancel(context.Background())
	statusA := make(chan int, 1)
	go func() { statusA <- run(ctxA, aConn, testNewPlugin) }()
	waitForHeight(t, layout, 4, 15*time.Second)
	cancelA()
	if got := <-statusA; got != 0 {
		t.Fatalf("run A status = %d, want 0", got)
	}
	aConn.Close()
	<-fakeA.done

	// Run B: same storage, a fresh net.Pipe and fake, staged frontier. Wait until the cursor
	// reaches height 10, then cancel.
	bConn, bFakeConn := net.Pipe()
	fakeB := newFake(t, bFakeConn, cfg, chain, frontierFor)
	go fakeB.run()
	ctxB, cancelB := context.WithCancel(context.Background())
	statusB := make(chan int, 1)
	go func() { statusB <- run(ctxB, bConn, testNewPlugin) }()
	waitForHeight(t, layout, 10, 15*time.Second)
	cancelB()
	if got := <-statusB; got != 0 {
		t.Fatalf("run B status = %d, want 0", got)
	}
	bConn.Close()
	<-fakeB.done

	// The ForkParent window read at boot: MomentaRequest{Height: 2, Count: 3}, before any
	// MomentaRequest at height 5.
	reqs := fakeB.momentaRequests()
	forkParentIdx, height5Idx := -1, -1
	for i, r := range reqs {
		if forkParentIdx == -1 && r.Height == 2 && r.Count == 3 {
			forkParentIdx = i
		}
		if height5Idx == -1 && r.Height == 5 {
			height5Idx = i
		}
	}
	if forkParentIdx == -1 {
		t.Fatalf("fake B never received MomentaRequest{Height: 2, Count: 3}; requests = %+v", reqs)
	}
	if height5Idx != -1 && height5Idx < forkParentIdx {
		t.Fatalf("MomentaRequest at height 5 (index %d) arrived before the ForkParent window read (index %d)", height5Idx, forkParentIdx)
	}

	// Reference: runtime.Run in-process, separate storage, over an in-memory source on the same
	// chain, frontier = frontierFor of the reference's own cursor.
	refStorage := t.TempDir()
	refLayout, err := recovery.NewLayout(refStorage, cfg.DomainID)
	if err != nil {
		t.Fatalf("layout: %v", err)
	}
	refSrc := memSource{chain: chain, layout: refLayout, frontierFn: frontierFor}
	refCfg := runtime.Config{
		DomainID:          cfg.DomainID,
		ExecutorID:        cfg.ExecutorAddress.String(),
		StoragePath:       refStorage,
		STFSpecHash:       cfg.STFSpecHash,
		ConfirmationDepth: cfg.ConfirmationDepth,
		Momenta:           refSrc,
		Assignments:       refSrc,
		Caps:              ledgertest.Caps{},
		Anchors:           noAnchor{},
		MaxPendingBatches: 1 << 30,
		Cursor:            noCursor{},
		Retry:             retry,
	}
	rt := runtime.New(refCfg)
	rt.Register(payloadDomain{})
	ctxRef, cancelRef := context.WithCancel(context.Background())
	refErr := make(chan error, 1)
	go func() { refErr <- rt.Run(ctxRef) }()
	waitForHeight(t, refLayout, 10, 15*time.Second)
	cancelRef()
	if err := <-refErr; err != nil {
		t.Fatalf("reference Run: %v", err)
	}

	// Equal cursor positions and exactly three matching batch records.
	gotPos, gotOK, err := recovery.ReadCursor(layout)
	if err != nil || !gotOK {
		t.Fatalf("worker cursor: ok=%v err=%v", gotOK, err)
	}
	wantPos, wantOK, err := recovery.ReadCursor(refLayout)
	if err != nil || !wantOK {
		t.Fatalf("reference cursor: ok=%v err=%v", wantOK, err)
	}
	if gotPos != wantPos {
		t.Fatalf("worker cursor = %+v, reference cursor = %+v", gotPos, wantPos)
	}

	gotRecords, err := recovery.ListBatchRecords(layout)
	if err != nil {
		t.Fatalf("ListBatchRecords: %v", err)
	}
	wantRecords, err := recovery.ListBatchRecords(refLayout)
	if err != nil {
		t.Fatalf("ListBatchRecords: %v", err)
	}
	if len(gotRecords) != 3 {
		t.Fatalf("%d worker batch records, want 3", len(gotRecords))
	}
	if !reflect.DeepEqual(gotRecords, wantRecords) {
		t.Fatalf("worker records = %+v, reference records = %+v", gotRecords, wantRecords)
	}

	// The Committed frames from A and B, concatenated, match those records one to one.
	committed := append(fakeA.committedCommitments(), fakeB.committedCommitments()...)
	if len(committed) != len(gotRecords) {
		t.Fatalf("%d committed frames, want %d", len(committed), len(gotRecords))
	}
	for i, c := range committed {
		r := gotRecords[i]
		if c.BatchID != r.BatchID || c.FirstInputSeq != r.FirstInputSeq || c.LastInputSeq != r.LastInputSeq ||
			c.PreStateRoot != r.PreStateRoot || c.PostStateRoot != r.PostStateRoot || c.DAHash != r.DAHash {
			t.Fatalf("commitment %d = %+v, want to match record %+v", i, c, r)
		}
		if c.DomainID != 1 {
			t.Fatalf("commitment %d DomainID = %d, want 1", i, c.DomainID)
		}
		if c.ExecutorID != (types.Address{0x42}) {
			t.Fatalf("commitment %d ExecutorID = %v, want {0x42}", i, c.ExecutorID)
		}
	}
}
