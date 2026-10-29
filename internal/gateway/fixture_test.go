package gateway

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"math/big"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/zenon-network/go-zenon/common/trie"
	"github.com/zenon-network/go-zenon/common/types"
	"github.com/zenon-network/go-zenon/vm/embedded/definition"

	"github.com/Zeno-L2/zeno-executor/domain"
	"github.com/Zeno-L2/zeno-executor/internal/batch"
	"github.com/Zeno-L2/zeno-executor/internal/codec"
	"github.com/Zeno-L2/zeno-executor/internal/ledger"
	"github.com/Zeno-L2/zeno-executor/internal/recovery"
	"github.com/Zeno-L2/zeno-executor/internal/state"
)

var (
	pA = trie.Path{0x10}
	pB = trie.Path{0x20}
	pC = trie.Path{0x30}
	pD = trie.Path{0x40}

	allPaths = []trie.Path{pA, pB, pC, pD}

	fixtureAsset   = codec.AssetID{31: 1}
	fixtureDeposit = codec.U256{31: 5}
)

// fixtureInput is one input of a built history and the Effects the
// scripted domain returns for it.
type fixtureInput struct {
	index   uint64
	effects domain.Effects
	// args is the input's kind-specific tail; nil keeps a CallL2 to
	// {0xC0}.
	args codec.InputArgs
	// chunk is the input's source data, a deploy chunk's bytes.
	chunk []byte
}

// scriptedDomain returns the Effects registered for each input index.
type scriptedDomain struct {
	effects map[uint64]domain.Effects
}

func (d *scriptedDomain) Genesis() domain.StateInit { return domain.StateInit{} }

func (d *scriptedDomain) DecodeInput(ctx domain.InputContext, _ []byte) (domain.Input, error) {
	return ctx.GlobalInputIndex, nil
}

func (d *scriptedDomain) Apply(_ domain.StateAccess, in domain.Input) (domain.Effects, error) {
	return d.effects[in.(uint64)], nil
}

// sliceSource serves a fixed input list.
type sliceSource struct {
	inputs []ledger.Input
	pos    int
}

func (s *sliceSource) Next() (ledger.Input, bool, error) {
	if s.pos >= len(s.inputs) {
		return ledger.Input{}, false, nil
	}
	in := s.inputs[s.pos]
	s.pos++
	return in, true, nil
}

func hashOfIndex(i uint64) types.Hash {
	var h types.Hash
	h[0] = byte(i)
	h[1] = 0xEE
	return h
}

func inputPayload(t *testing.T, index uint64, args codec.InputArgs) []byte {
	t.Helper()
	if args == nil {
		args = codec.CallL2Args{
			TargetContract: types.Hash{0xC0},
			DepositAsset:   fixtureAsset,
			DepositAmount:  fixtureDeposit,
		}
	}
	in := codec.Input{SourceAccountBlockHash: hashOfIndex(index), Args: args}
	b, err := in.Encode()
	if err != nil {
		t.Fatalf("encode input: %v", err)
	}
	return b
}

// buildHistory builds batches, one inner slice per batch, into
// storage/1 with the batch driver, the recovery bundle and receipt
// stores and a state store holding genesis, writes one batch record
// per batch, and returns each batch's commitment.
func buildHistory(t *testing.T, storage string, genesis []domain.StateDiffEntry, batches [][]fixtureInput) []codec.BatchCommitment {
	t.Helper()
	return buildHistoryWith(t, storage, genesis, batches, func(d domain.Domain) domain.Domain { return d })
}

// buildHistoryWith is buildHistory over the scripted domain as wrapped
// by wrap.
func buildHistoryWith(t *testing.T, storage string, genesis []domain.StateDiffEntry, batches [][]fixtureInput, wrap func(domain.Domain) domain.Domain) []codec.BatchCommitment {
	t.Helper()
	dom := &scriptedDomain{effects: make(map[uint64]domain.Effects)}
	ledgerBatches := make([][]ledger.Input, len(batches))
	for i, b := range batches {
		for _, in := range b {
			dom.effects[in.index] = in.effects
			ledgerBatches[i] = append(ledgerBatches[i], ledger.Input{
				Ctx:     domain.InputContext{GlobalInputIndex: in.index, SourceData: in.chunk},
				Payload: inputPayload(t, in.index, in.args),
			})
		}
	}
	return buildFrom(t, storage, genesis, wrap(dom), ledgerBatches)
}

// buildFrom builds batches, one inner slice per batch, of inputs over
// dom as buildHistory does.
func buildFrom(t *testing.T, storage string, genesis []domain.StateDiffEntry, dom domain.Domain, batches [][]ledger.Input) []codec.BatchCommitment {
	t.Helper()
	l, err := recovery.NewLayout(storage, 1)
	if err != nil {
		t.Fatalf("NewLayout: %v", err)
	}
	bundles, err := recovery.NewBundleStore(l.Bundles)
	if err != nil {
		t.Fatalf("NewBundleStore: %v", err)
	}
	receipts, err := recovery.NewReceiptStore(l.Receipts)
	if err != nil {
		t.Fatalf("NewReceiptStore: %v", err)
	}
	store := state.OpenEmpty()
	t.Cleanup(func() { _ = store.Close() })
	if len(genesis) > 0 {
		if _, err := store.Apply(genesis); err != nil {
			t.Fatalf("apply genesis: %v", err)
		}
	}

	var inputs []ledger.Input
	for _, b := range batches {
		inputs = append(inputs, b...)
	}
	d, err := batch.New(batch.Config{
		DomainID:     1,
		ExecutorID:   types.Address{9},
		Domain:       dom,
		Store:        store,
		Inputs:       &sliceSource{inputs: inputs},
		Bundles:      bundles,
		Receipts:     receipts,
		FirstBatchID: 0,
	})
	if err != nil {
		t.Fatalf("batch.New: %v", err)
	}

	var commitments []codec.BatchCommitment
	for i, b := range batches {
		built, ok, err := d.NextBatchTo(b[len(b)-1].Ctx.GlobalInputIndex)
		if err != nil || !ok {
			t.Fatalf("batch %d: NextBatchTo: ok=%v err=%v", i, ok, err)
		}
		c := built.Commitment
		enc, err := c.Encode()
		if err != nil {
			t.Fatalf("batch %d: encode commitment: %v", i, err)
		}
		rec := recovery.BatchRecord{
			BatchID:       c.BatchID,
			Status:        recovery.StatusBuilt,
			FirstInputSeq: c.FirstInputSeq,
			LastInputSeq:  c.LastInputSeq,
			PreStateRoot:  c.PreStateRoot,
			PostStateRoot: c.PostStateRoot,
			DAHash:        c.DAHash,
			Commitment:    enc,
		}
		writeRecord(t, l, rec)
		commitments = append(commitments, c)
	}
	return commitments
}

func recordPath(l recovery.Layout, id uint64) string {
	return filepath.Join(l.Batches, fmt.Sprintf("%020d.json", id))
}

func writeRecord(t *testing.T, l recovery.Layout, rec recovery.BatchRecord) {
	t.Helper()
	data, err := json.MarshalIndent(rec, "", "  ")
	if err != nil {
		t.Fatalf("marshal record: %v", err)
	}
	path := recordPath(l, rec.BatchID)
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatalf("write record: %v", err)
	}
	if err := os.Chmod(path, 0o644); err != nil {
		t.Fatalf("chmod record: %v", err)
	}
}

func success(diff ...domain.StateDiffEntry) domain.Effects {
	return domain.Effects{
		Status:     codec.StatusSuccess,
		ContractID: types.Hash{0xC0},
		StateDiff:  diff,
	}
}

func put(p trie.Path, v string) domain.StateDiffEntry {
	return domain.StateDiffEntry{Path: p, Value: []byte(v)}
}

func del(p trie.Path) domain.StateDiffEntry { return domain.StateDiffEntry{Path: p} }

func putEmpty(p trie.Path) domain.StateDiffEntry {
	return domain.StateDiffEntry{Path: p, Value: []byte{}}
}

func refund(index uint64) domain.Effects {
	return domain.Effects{
		Status: codec.StatusRuntimeFault,
		OutboxMessages: []codec.OutboxMessage{{
			InputIndex: index,
			Target: codec.L1Withdrawal{
				RecipientL1: types.Address{1},
				Asset:       fixtureAsset,
				Amount:      fixtureDeposit,
			},
		}},
	}
}

// history builds the fixture history over the given batch 3 and 4
// effects. Batch 2 holds inputs 4 and 6, so batch indices have a gap.
func history(d3, d4 domain.StateDiffEntry) [][]fixtureInput {
	one := success(put(pA, "a1"), putEmpty(pB))
	one.Events = []codec.Event{{ContractID: types.Hash{0xC0}, InputIndex: 1, EventIndex: 0, Data: []byte("evt")}}
	two := domain.Effects{Status: codec.StatusRevert, ReturnData: []byte("no"), AbortCode: 7}
	return [][]fixtureInput{
		{{index: 1, effects: one}, {index: 2, effects: two}},
		{{index: 3, effects: success(put(pA, "a2"), del(pB), put(pC, "c1"))}},
		{{index: 4, effects: refund(4)}, {index: 6, effects: success(put(pB, "b2"), del(pC))}},
		{{index: 7, effects: success(d3)}},
		{{index: 8, effects: success(d4)}},
	}
}

func historyA() [][]fixtureInput { return history(put(pD, "d3"), putEmpty(pA)) }
func historyB() [][]fixtureInput { return history(put(pD, "other"), putEmpty(pA)) }

// fixtureSTF is the stfSpecHash the fake's domain record carries.
var fixtureSTF = types.Hash{0x5F}

// fakeSettlement is a Settlement over a map of entries. Its reads,
// accept and edit hold mu, so a read the engine abandoned never races
// a test step.
type fakeSettlement struct {
	mu           sync.Mutex
	height, next uint64
	entries      map[uint64]definition.BatchEntry
	hidden       map[uint64]bool
	reads        map[uint64]int
	// record is the domain record; nil makes it invisible.
	record *definition.DomainRecord
	// recordErr is returned by DomainRecord when set.
	recordErr error
	// heightErr is returned by ConfirmedHeight when set.
	heightErr error
	// processed holds the outbox ids OutboxProcessed reports true, and
	// processedErr is its error when set. processedCalls counts its
	// calls. A non-nil processedHang makes each call wait for it to
	// close, outside mu.
	processed      map[types.Hash]bool
	processedErr   error
	processedCalls int
	processedHang  chan struct{}
	// momentumTime and momentumHash are the unix time and hash
	// Momentum reports. caps holds the MaxBatchWithdrawal AssetConfig
	// reports per asset; an asset with no entry has no config. capsErr
	// is AssetConfig's error when set. capsCalls counts its calls. A
	// non-nil capsHang makes each call wait for it to close, outside
	// mu.
	momentumTime uint64
	momentumHash types.Hash
	caps         map[types.Hash]*big.Int
	capsErr      error
	capsCalls    int
	capsHang     chan struct{}
}

func newFakeSettlement() *fakeSettlement {
	return &fakeSettlement{
		entries: make(map[uint64]definition.BatchEntry),
		hidden:  make(map[uint64]bool),
		reads:   make(map[uint64]int),
		record:  &definition.DomainRecord{StfSpecHash: fixtureSTF},
	}
}

func (f *fakeSettlement) ConfirmedHeight() (uint64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.heightErr != nil {
		return 0, f.heightErr
	}
	return f.height, nil
}

func (f *fakeSettlement) At(uint64) (SettlementView, error) { return fakeView{f}, nil }

type fakeView struct{ f *fakeSettlement }

func (v fakeView) InputSequenceCursor(domainID uint32) (*definition.InputSequenceCursor, error) {
	v.f.mu.Lock()
	defer v.f.mu.Unlock()
	return &definition.InputSequenceCursor{DomainId: domainID, NextBatchId: v.f.next}, nil
}

func (v fakeView) DomainRecord(uint32) (*definition.DomainRecord, bool, error) {
	v.f.mu.Lock()
	defer v.f.mu.Unlock()
	if v.f.recordErr != nil {
		return nil, false, v.f.recordErr
	}
	if v.f.record == nil {
		return nil, false, nil
	}
	rec := *v.f.record
	return &rec, true, nil
}

func (v fakeView) OutboxProcessed(id types.Hash) (bool, error) {
	v.f.mu.Lock()
	v.f.processedCalls++
	ok, err, hang := v.f.processed[id], v.f.processedErr, v.f.processedHang
	v.f.mu.Unlock()
	if hang != nil {
		<-hang
	}
	return ok, err
}

func (v fakeView) Momentum() (uint64, uint64, types.Hash) {
	v.f.mu.Lock()
	defer v.f.mu.Unlock()
	return v.f.height, v.f.momentumTime, v.f.momentumHash
}

func (v fakeView) AssetConfig(_ uint32, asset types.Hash) (*definition.AssetConfig, bool, error) {
	v.f.mu.Lock()
	v.f.capsCalls++
	max, ok, err, hang := v.f.caps[asset], false, v.f.capsErr, v.f.capsHang
	if max != nil {
		ok = true
	}
	v.f.mu.Unlock()
	if hang != nil {
		<-hang
	}
	if err != nil || !ok {
		return nil, false, err
	}
	return &definition.AssetConfig{MaxBatchWithdrawal: new(big.Int).Set(max)}, true, nil
}

func (v fakeView) BatchEntry(_ uint32, id uint64) (*definition.BatchEntry, bool, error) {
	v.f.mu.Lock()
	defer v.f.mu.Unlock()
	v.f.reads[id]++
	e, ok := v.f.entries[id]
	if !ok || v.f.hidden[id] {
		return nil, false, nil
	}
	return &e, true, nil
}

// accept stores the entry for c and moves nextBatchId past it.
func (f *fakeSettlement) accept(c codec.BatchCommitment, finalizeAt uint64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.entries[c.BatchID] = definition.BatchEntry{
		BatchEntryKey:    definition.BatchEntryKey{DomainId: 1, BatchId: c.BatchID},
		FirstInputSeq:    c.FirstInputSeq,
		LastInputSeq:     c.LastInputSeq,
		PreStateRoot:     c.PreStateRoot,
		PostStateRoot:    c.PostStateRoot,
		InputRoot:        c.InputRoot,
		ReceiptRoot:      c.ReceiptRoot,
		EventRoot:        c.EventRoot,
		OutboxRoot:       c.OutboxRoot,
		DaHash:           c.DAHash,
		FinalizeAtHeight: finalizeAt,
	}
	f.next = c.BatchID + 1
}

// edit changes the stored entry for id.
func (f *fakeSettlement) edit(id uint64, fn func(*definition.BatchEntry)) {
	f.mu.Lock()
	defer f.mu.Unlock()
	e := f.entries[id]
	fn(&e)
	f.entries[id] = e
}

// logSink collects the engine's log lines.
type logSink struct {
	mu    sync.Mutex
	lines []string
}

func (s *logSink) logf(format string, args ...any) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.lines = append(s.lines, fmt.Sprintf(format, args...))
}

// count returns the number of lines containing every substring.
func (s *logSink) count(subs ...string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for _, l := range s.lines {
		ok := true
		for _, sub := range subs {
			if !strings.Contains(l, sub) {
				ok = false
			}
		}
		if ok {
			n++
		}
	}
	return n
}

func (s *logSink) dump() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return strings.Join(s.lines, "\n")
}

// rig is an engine under test with the handles tests inspect.
type rig struct {
	c       *Chain
	final   *state.Store
	snaps   string
	log     *logSink
	idx     *Index
	senders *fakeSenders
}

// newTestChain returns an engine over the executor directory storage.
func newTestChain(t *testing.T, storage string, s *fakeSettlement) *rig {
	t.Helper()
	final := state.OpenEmpty()
	t.Cleanup(func() { _ = final.Close() })
	sink := &logSink{}
	// The production layout, <storagePath>/<id>/snapshots, so the cache
	// guard checks a storage path the test owns.
	snaps := cachePathsFor(t.TempDir(), 1).snapshots
	c := NewChain(ChainConfig{
		DomainID:   1,
		Source:     NewLocalSource(storage, 1),
		Settlement: s,
		Final:      final,
		Snapshots:  snaps,
		Logf:       sink.logf,
	})
	return &rig{c: c, final: final, snaps: snaps, log: sink}
}

// openTestChain boots an engine through OpenChain for domain 1.
func openTestChain(t *testing.T, storagePath, execDir string, s Settlement) (*Chain, *logSink) {
	t.Helper()
	return openTestChainWith(t, storagePath, execDir, s, &fakeSenders{})
}

// openTestChainWith is openTestChain over senders. It closes the
// engine when the test ends, and closing it twice is harmless.
func openTestChainWith(t *testing.T, storagePath, execDir string, s Settlement, senders SenderSource) (*Chain, *logSink) {
	t.Helper()
	sink := &logSink{}
	c, err := OpenChain(context.Background(), storagePath, DomainConfig{DomainID: 1, Source: "LOCAL", ExecutorStoragePath: execDir}, s, senders, sink.logf)
	if err != nil {
		t.Fatalf("OpenChain: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c, sink
}

// closeChain closes c and fails the test on an error, so a restart
// test can reopen the same storage path.
func closeChain(t *testing.T, c *Chain) {
	t.Helper()
	if err := c.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

// snapshotAt writes one snapshot of leaves into dir.
func snapshotAt(t *testing.T, dir string, leaves []domain.StateDiffEntry, stf types.Hash, meta []byte, inputSeq uint64) {
	t.Helper()
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	store := state.OpenEmpty()
	defer store.Close()
	if _, err := store.Apply(leaves); err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if _, err := store.SaveSnapshot(dir, state.SnapshotHeader{InputSeq: inputSeq, STFSpecHash: stf, Meta: meta}); err != nil {
		t.Fatalf("SaveSnapshot: %v", err)
	}
}

// hangingSettlement wraps a fakeSettlement and hangs chosen reads until
// the test ends.
type hangingSettlement struct {
	f          *fakeSettlement
	hangHeight atomic.Bool
	hangView   atomic.Bool
	calls      atomic.Int64
	entryDelay time.Duration
	release    chan struct{}
	// hangEntry is a batch id whose BatchEntry hangs, or -1.
	hangEntry  atomic.Int64
	entryHangs atomic.Int64
	// onEntryHang runs when a hanging BatchEntry starts; a test sets it
	// before any read starts.
	onEntryHang func()
}

func newHangingSettlement(t *testing.T, f *fakeSettlement) *hangingSettlement {
	t.Helper()
	h := &hangingSettlement{f: f, release: make(chan struct{})}
	h.hangEntry.Store(-1)
	t.Cleanup(func() { close(h.release) })
	return h
}

func (h *hangingSettlement) ConfirmedHeight() (uint64, error) {
	h.calls.Add(1)
	if h.hangHeight.Load() {
		<-h.release
		return 0, errors.New("released")
	}
	return h.f.ConfirmedHeight()
}

func (h *hangingSettlement) At(height uint64) (SettlementView, error) {
	v, err := h.f.At(height)
	if err != nil {
		return nil, err
	}
	return hangingView{h: h, view: v}, nil
}

type hangingView struct {
	h    *hangingSettlement
	view SettlementView
}

func (v hangingView) InputSequenceCursor(domainID uint32) (*definition.InputSequenceCursor, error) {
	if v.h.hangView.Load() {
		<-v.h.release
		return nil, errors.New("released")
	}
	return v.view.InputSequenceCursor(domainID)
}

func (v hangingView) BatchEntry(domainID uint32, id uint64) (*definition.BatchEntry, bool, error) {
	if int64(id) == v.h.hangEntry.Load() {
		v.h.entryHangs.Add(1)
		if v.h.onEntryHang != nil {
			v.h.onEntryHang()
		}
		<-v.h.release
		return nil, false, errors.New("released")
	}
	time.Sleep(v.h.entryDelay)
	return v.view.BatchEntry(domainID, id)
}

func (v hangingView) DomainRecord(domainID uint32) (*definition.DomainRecord, bool, error) {
	return v.view.DomainRecord(domainID)
}

func (v hangingView) OutboxProcessed(id types.Hash) (bool, error) {
	return v.view.OutboxProcessed(id)
}

func (v hangingView) Momentum() (uint64, uint64, types.Hash) { return v.view.Momentum() }

func (v hangingView) AssetConfig(domainID uint32, asset types.Hash) (*definition.AssetConfig, bool, error) {
	return v.view.AssetConfig(domainID, asset)
}

// copyDir copies the tree src to dst with file modes.
func copyDir(t *testing.T, src, dst string) {
	t.Helper()
	err := filepath.WalkDir(src, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(src, path)
		if err != nil {
			return err
		}
		target := filepath.Join(dst, rel)
		info, err := d.Info()
		if err != nil {
			return err
		}
		if d.IsDir() {
			return os.MkdirAll(target, info.Mode().Perm())
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		return os.WriteFile(target, data, info.Mode().Perm())
	})
	if err != nil {
		t.Fatalf("copy %s to %s: %v", src, dst, err)
	}
}

// requireSameVersions requires equal heads and, for each id, equal
// roots and byte-equal proofs for every fixture path.
func requireSameVersions(t *testing.T, a, b *Chain, ids ...uint64) {
	t.Helper()
	if ha, hb := a.Head(), b.Head(); fmt.Sprintf("%+v", derefHead(ha)) != fmt.Sprintf("%+v", derefHead(hb)) {
		t.Fatalf("heads differ: %+v vs %+v", derefHead(ha), derefHead(hb))
	}
	for _, id := range ids {
		va, oka := a.VersionAt(id)
		vb, okb := b.VersionAt(id)
		if !oka || !okb {
			t.Fatalf("version %d: held by a %v, by b %v", id, oka, okb)
		}
		if va.Root() != vb.Root() {
			t.Fatalf("version %d: roots %s and %s differ", id, va.Root(), vb.Root())
		}
		for _, p := range allPaths {
			pa, xa, fa := va.Prove(p)
			pb, xb, fb := vb.Prove(p)
			if pa != pb || string(xa) != string(xb) || string(fa) != string(fb) {
				t.Fatalf("version %d path %x: proofs differ", id, p[0])
			}
		}
	}
}

type headValues struct {
	Pending, Latest, Finalized BatchHead
	Has                        [3]bool
	Status                     Status
}

func derefHead(h Head) headValues {
	var v headValues
	for i, p := range []*BatchHead{h.Pending, h.Latest, h.Finalized} {
		if p != nil {
			v.Has[i] = true
			switch i {
			case 0:
				v.Pending = *p
			case 1:
				v.Latest = *p
			case 2:
				v.Finalized = *p
			}
		}
	}
	v.Status = h.Status
	return v
}

// fakeSenders is a SenderSource that counts its calls.
type fakeSenders struct {
	mu    sync.Mutex
	calls map[types.Hash]int
	total int
	// fail, when set and returning an error, fails the call.
	fail func(types.Hash) error
}

func (f *fakeSenders) Sender(_ uint32, h types.Hash) (types.Address, error) {
	f.mu.Lock()
	if f.calls == nil {
		f.calls = make(map[types.Hash]int)
	}
	f.calls[h]++
	f.total++
	fail := f.fail
	f.mu.Unlock()
	if fail != nil {
		if err := fail(h); err != nil {
			return types.Address{}, err
		}
	}
	return senderOf(h), nil
}

func (f *fakeSenders) callCount(h types.Hash) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls[h]
}

func (f *fakeSenders) totalCalls() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.total
}

// senderOf is the sender fakeSenders returns for send hash h.
func senderOf(h types.Hash) types.Address { return types.Address{0xA0, h[0]} }

// newIndexedChain is newTestChain with an index in a fresh directory.
func newIndexedChain(t *testing.T, exec string, s *fakeSettlement) *rig {
	t.Helper()
	senders := &fakeSenders{}
	r := newIndexedChainWith(t, exec, s, senders)
	r.senders = senders
	return r
}

// newIndexedChainWith is newIndexedChain over senders; the rig's
// senders field is left nil.
func newIndexedChainWith(t *testing.T, exec string, s *fakeSettlement, senders SenderSource) *rig {
	t.Helper()
	r := newTestChain(t, exec, s)
	idx, err := openIndex(t.TempDir(), 1, senders)
	if err != nil {
		t.Fatalf("openIndex: %v", err)
	}
	t.Cleanup(func() { _ = idx.Close() })
	r.idx = idx
	r.c.cfg.Index = idx
	return r
}

// indexEntries returns every key of x and its value, hex-encoded,
// except the keys skip names.
func indexEntries(t *testing.T, x *Index, skip func(key string) bool) map[string]string {
	t.Helper()
	it := x.db.NewIterator(nil, nil)
	defer it.Release()
	out := make(map[string]string)
	for it.Next() {
		k := string(it.Key())
		if !skip(k) {
			out[k] = hex.EncodeToString(it.Value())
		}
	}
	if err := it.Error(); err != nil {
		t.Fatalf("iterate the index: %v", err)
	}
	return out
}

// indexKeys returns every key except t, F, meta/schema, m/... and
// S/....
func indexKeys(t *testing.T, x *Index) map[string]bool {
	t.Helper()
	out := make(map[string]bool)
	for k := range indexEntries(t, x, func(k string) bool {
		return k == keyMarker || k == keyFinalized || k == keyMetaSchema || strings.HasPrefix(k, pfxManifest) || strings.HasPrefix(k, pfxSender)
	}) {
		out[k] = true
	}
	return out
}

// indexDump returns every key and value except S/....
func indexDump(t *testing.T, x *Index) map[string]string {
	t.Helper()
	return indexEntries(t, x, func(k string) bool { return strings.HasPrefix(k, pfxSender) })
}

// synthInput is one synthetic input g of kind with the given send
// hash, receipt status and contract id, and the given events.
func synthInput(t *testing.T, g uint64, sendHash types.Hash, kind byte, status codec.Status, contract types.Hash, events [][]byte) batchInput {
	t.Helper()
	r := codec.Receipt{
		Version:    codec.ReceiptVersion,
		InputIndex: g,
		Status:     status,
		ContractID: contract,
		EventCount: uint32(len(events)),
	}
	leaf, err := r.Encode()
	if err != nil {
		t.Fatalf("encode receipt: %v", err)
	}
	evs := make([]codec.Event, len(events))
	for i := range evs {
		evs[i] = codec.Event{InputIndex: g, EventIndex: uint32(i)}
	}
	return batchInput{
		gidx:      g,
		inputData: append([]byte{kind}, sendHash[:]...),
		receipt:   r,
		leaf:      leaf,
		result:    codec.ExecutionResult{Version: 1, GlobalInputIndex: g, Events: evs},
		events:    events,
	}
}

// historyC is two batches of contract activity: a deploy, calls and
// an upgrade. No input writes state.
func historyC() [][]fixtureInput {
	x, y := types.Hash{0xD1}, types.Hash{0xD2}
	h, h2 := types.Hash{0xE1}, types.Hash{0xE3}
	call := func(c types.Hash) codec.InputArgs {
		return codec.CallL2Args{TargetContract: c, DepositAsset: fixtureAsset, DepositAmount: fixtureDeposit}
	}
	return [][]fixtureInput{
		{
			{index: 1, args: call(x), effects: domain.Effects{Status: codec.StatusValidationFailed, ContractID: x}},
			{index: 2, args: codec.DeployFinalizeArgs{CodeHash: h}, effects: domain.Effects{Status: codec.StatusSuccess, ContractID: x, CodeHash: types.Hash{0xE2}}},
			{index: 3, args: call(x), effects: domain.Effects{Status: codec.StatusRevert, ContractID: x, ReturnData: []byte("r"), AbortCode: 1}},
		},
		{
			{index: 4, args: call(y), effects: domain.Effects{Status: codec.StatusSuccess, ContractID: y}},
			{index: 5, args: codec.UpgradeArgs{ContractID: x, NewCodeHash: h2}, effects: domain.Effects{Status: codec.StatusSuccess, ContractID: x, CodeHash: types.Hash{0xE4}}},
		},
	}
}

// historyOverlap returns history A's first two batches followed by a
// batch of inputs 4 and 6 (e), and the same inputs split as {1, 2},
// {3}, {4}, {6, 7} (z). z's batch 3 starts at input 6, where e's
// batch 2 ends, on the same state.
func historyOverlap() (e, z [][]fixtureInput) {
	a := historyA()
	four := fixtureInput{index: 4, effects: refund(4)}
	six := fixtureInput{index: 6, effects: success()}
	seven := fixtureInput{index: 7, effects: success()}
	e = [][]fixtureInput{a[0], a[1], {four, six}}
	z = [][]fixtureInput{a[0], a[1], {four}, {six, seven}}
	return e, z
}

// rewriteRecordRoot sets the receiptRoot in record id's commitment in
// dir.
func rewriteRecordRoot(t *testing.T, dir string, id uint64, root types.Hash) {
	t.Helper()
	l := layoutOf(dir)
	rec, ok, err := recovery.ReadBatchRecord(l, id)
	if err != nil || !ok {
		t.Fatalf("read record %d: ok %v err %v", id, ok, err)
	}
	cm, err := codec.DecodeBatchCommitment(rec.Commitment)
	if err != nil {
		t.Fatal(err)
	}
	cm.ReceiptRoot = root
	if rec.Commitment, err = cm.Encode(); err != nil {
		t.Fatal(err)
	}
	writeRecord(t, l, rec)
}
