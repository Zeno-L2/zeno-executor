package runtime

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/zenon-network/go-zenon/chain/nom"
	"github.com/zenon-network/go-zenon/common/trie"
	"github.com/zenon-network/go-zenon/common/types"

	"github.com/digitalSloth/go-executor/domain"
	"github.com/digitalSloth/go-executor/internal/codec"
	"github.com/digitalSloth/go-executor/internal/ledger"
	"github.com/digitalSloth/go-executor/internal/recovery"
)

// --- fixtures ---

// scriptDomain is a domain.Domain whose every input succeeds and writes one leaf keyed by the
// input's global index.
type scriptDomain struct{}

type scriptInput struct{ idx uint64 }

func (scriptDomain) Genesis() domain.StateInit { return domain.StateInit{} }

func (scriptDomain) DecodeInput(ctx domain.InputContext, payload []byte) (domain.Input, error) {
	return scriptInput{idx: ctx.GlobalInputIndex}, nil
}

func (scriptDomain) Apply(_ domain.StateAccess, in domain.Input) (domain.Effects, error) {
	idx := in.(scriptInput).idx
	var p trie.Path
	binary.BigEndian.PutUint64(p[:8], idx)
	return domain.Effects{
		Status:    codec.StatusSuccess,
		StateDiff: []domain.StateDiffEntry{{Path: p, Value: []byte{byte(idx), 1}}},
	}, nil
}

// chainSource serves prebuilt momenta with a settable frontier. Every field is guarded by mu:
// the runtime reads the source from its own goroutine while a test swaps the chain.
type chainSource struct {
	mu       sync.Mutex
	frontier uint64
	moms     []*nom.DetailedMomentum

	requests        []uint64 // start heights of DetailedMomentumsByHeight calls since the last set
	frontierCalls   int
	highestReturned uint64
}

// set replaces the chain and the frontier, and clears the request history.
func (s *chainSource) set(moms []*nom.DetailedMomentum, frontier uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.moms, s.frontier = moms, frontier
	s.requests, s.highestReturned = nil, 0
}

func (s *chainSource) FrontierHeight() (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.frontierCalls++
	return s.frontier, nil
}

func (s *chainSource) DetailedMomentumsByHeight(height, count uint64) ([]*nom.DetailedMomentum, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.requests = append(s.requests, height)
	var out []*nom.DetailedMomentum
	for _, dm := range s.moms {
		if dm.Momentum.Height < height || uint64(len(out)) >= count {
			continue
		}
		out = append(out, dm)
		if dm.Momentum.Height > s.highestReturned {
			s.highestReturned = dm.Momentum.Height
		}
	}
	return out, nil
}

// requestedFrom reports whether a ranged read starting at height has happened since the last set.
func (s *chainSource) requestedFrom(height uint64) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, h := range s.requests {
		if h == height {
			return true
		}
	}
	return false
}

func (s *chainSource) frontierReads() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.frontierCalls
}

func momHash(h uint64, salt byte) types.Hash {
	var out types.Hash
	binary.BigEndian.PutUint64(out[:8], h)
	out[31] = salt
	return out
}

// buildChain builds momenta 1..n; every momentum holds one block for domain 1 (address byte 1)
// and one for domain 2, so domain 1's indices are the even numbers.
func buildChain(t *testing.T, n uint64, salt byte) []*nom.DetailedMomentum {
	t.Helper()
	return buildChainFrom(t, n, func(uint64) byte { return salt })
}

// buildChainFrom is buildChain with a per-height momentum hash salt, so a test can fork a chain at
// a chosen height.
func buildChainFrom(t *testing.T, n uint64, saltAt func(height uint64) byte) []*nom.DetailedMomentum {
	t.Helper()
	return buildShape(1, n, saltAt, func(uint64) int { return 1 })
}

// buildShape builds momenta from..to. Momentum h holds d1At(h) blocks for domain 1 and one block
// for domain 2, and its hash carries saltAt(h). Two chains that share a salt at a height and
// below it agree on those momenta's hashes; d1At sets how many inputs the domain sees.
func buildShape(from, to uint64, saltAt func(height uint64) byte, d1At func(height uint64) int) []*nom.DetailedMomentum {
	var out []*nom.DetailedMomentum
	prev := types.Hash{}
	if from > 1 {
		prev = momHash(from-1, saltAt(from-1))
	}
	for h := from; h <= to; h++ {
		var blocks []*nom.AccountBlock
		add := func(domainID byte, j int) {
			in := codec.Input{
				SourceAccountBlockHash: blockHash(h, domainID, j),
				Args:                   codec.CallL2Args{TargetContract: types.Hash{domainID}},
			}
			data, err := in.Encode()
			if err != nil {
				panic(fmt.Sprintf("encode input: %v", err))
			}
			blocks = append(blocks, &nom.AccountBlock{Address: types.Address{domainID}, Height: h, Hash: blockHash(h, domainID, j), Data: data})
		}
		for j := 0; j < d1At(h); j++ {
			add(1, j)
		}
		add(2, 0)
		hash := momHash(h, saltAt(h))
		out = append(out, &nom.DetailedMomentum{
			Momentum: &nom.Momentum{
				Height: h, Hash: hash, PreviousHash: prev,
				TimestampUnix: 1000 + h, Content: nom.NewMomentumContent(blocks),
			},
			AccountBlocks: blocks,
		})
		prev = hash
	}
	return out
}

func blockHash(h uint64, domainID byte, j int) types.Hash {
	var out types.Hash
	binary.BigEndian.PutUint64(out[:8], h)
	out[8], out[9] = domainID, byte(j)
	return out
}

// byAddress routes a block to the domain named by its address's first byte.
type byAddress struct{}

func (byAddress) Accept(_ *nom.Momentum, block *nom.AccountBlock) (*ledger.Envelope, error) {
	return &ledger.Envelope{DomainID: uint32(block.Address[0]), Payload: block.Data}, nil
}

type noAnchor struct{}

func (noAnchor) FinalizedAnchor(uint32) (recovery.Anchor, bool, error) {
	return recovery.Anchor{}, false, nil
}

func validConfig(t *testing.T, src ledger.MomentumSource) Config {
	t.Helper()
	return Config{
		DomainID:          1,
		ExecutorID:        types.Address{0x42}.String(),
		StoragePath:       t.TempDir(),
		Role:              "EXECUTOR",
		STFSpecHash:       types.Hash{0xAA},
		ConfirmationDepth: 2,
		Momenta:           src,
		Filter:            byAddress{},
		Anchors:           noAnchor{},
		Retry:             recovery.RetryPolicy{SourceAttempts: 2, SourceInterval: time.Millisecond, AwaitDAAttempts: 1, AwaitDAInterval: time.Millisecond},
	}
}

// runUntil runs the runtime until done reports true, then cancels and returns Run's error.
func runUntil(t *testing.T, cfg Config, done func() bool) error {
	t.Helper()
	rt := New(cfg)
	rt.Register(scriptDomain{})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	result := make(chan error, 1)
	go func() { result <- rt.Run(ctx) }()

	deadline := time.After(15 * time.Second)
	tick := time.NewTicker(10 * time.Millisecond)
	defer tick.Stop()
	for {
		select {
		case err := <-result:
			return err
		case <-deadline:
			cancel()
			t.Fatalf("condition not reached in time")
		case <-tick.C:
			if done() {
				cancel()
				return <-result
			}
		}
	}
}

func cursorAt(t *testing.T, cfg Config) (recovery.Position, bool) {
	t.Helper()
	l, err := recovery.NewLayout(cfg.StoragePath, cfg.DomainID)
	if err != nil {
		t.Fatal(err)
	}
	pos, ok, err := recovery.ReadCursor(l)
	if err != nil {
		t.Fatal(err)
	}
	return pos, ok
}

// --- tests ---

func TestConfigValidation(t *testing.T) {
	src := &chainSource{}
	cases := map[string]func(*Config){
		"empty executor id":        func(c *Config) { c.ExecutorID = "" },
		"executor id not address":  func(c *Config) { c.ExecutorID = "not-an-address" },
		"empty storage path":       func(c *Config) { c.StoragePath = "" },
		"role other than executor": func(c *Config) { c.Role = "WATCHER" },
		"zero stf spec hash":       func(c *Config) { c.STFSpecHash = types.Hash{} },
		"zero confirmation depth":  func(c *Config) { c.ConfirmationDepth = 0 },
		"depth above the maximum":  func(c *Config) { c.ConfirmationDepth = recovery.MaxConfirmationDepth + 1 },
		"nil momentum source":      func(c *Config) { c.Momenta = nil },
		"nil filter":               func(c *Config) { c.Filter = nil },
		"nil anchor source":        func(c *Config) { c.Anchors = nil },
	}
	if err := validConfig(t, src).validate(); err != nil {
		t.Fatalf("valid config rejected: %v", err)
	}
	for name, mutate := range cases {
		cfg := validConfig(t, src)
		mutate(&cfg)
		if err := cfg.validate(); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}

func TestRunCommitsAndRecovers(t *testing.T) {
	src := &chainSource{frontier: 12, moms: buildChain(t, 10, 0)}
	cfg := validConfig(t, src)

	err := runUntil(t, cfg, func() bool {
		pos, ok := cursorAt(t, cfg)
		return ok && pos.LastInputSeq == 18
	})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	pos, _ := cursorAt(t, cfg)
	if pos.MomentumHeight != 10 || pos.NextGlobalIndex != 20 {
		t.Fatalf("cursor = %+v", pos)
	}
	layout, _ := recovery.NewLayout(cfg.StoragePath, cfg.DomainID)
	records, err := recovery.ListBatchRecords(layout)
	if err != nil || len(records) == 0 {
		t.Fatalf("batch records = %v, %v", records, err)
	}
	bundles, _ := recovery.NewBundleStore(layout.Bundles)
	var next uint64
	for _, r := range records {
		if r.FirstInputSeq != next {
			t.Fatalf("record %d starts at %d, want %d", r.BatchID, r.FirstInputSeq, next)
		}
		next = r.LastInputSeq + 2
		if ok, err := bundles.Has(r.DAHash); err != nil || !ok {
			t.Fatalf("bundle of record %d: %v, %v", r.BatchID, ok, err)
		}
	}

	// A fresh boot lands on the cursor's root.
	rdy, err := recovery.Boot(recovery.Config{
		Layout: layout, DomainID: cfg.DomainID, ExecutorID: cfg.ExecutorID, Role: cfg.Role,
		STFSpecHash: cfg.STFSpecHash, ConfirmationDepth: cfg.ConfirmationDepth, Domain: scriptDomain{},
		Momenta: src, Anchors: noAnchor{}, Bundles: bundles, Retry: cfg.Retry,
	})
	if err != nil {
		t.Fatalf("Boot: %v", err)
	}
	defer rdy.Store.Close()
	if root, _ := rdy.Store.Root(); root != pos.PostStateRoot {
		t.Fatalf("boot root %s differs from the cursor's %s", root, pos.PostStateRoot)
	}

	// A second run over the same tree resumes without re-deriving anything.
	src.set(buildChain(t, 12, 0), 14)
	err = runUntil(t, cfg, func() bool {
		pos, _ := cursorAt(t, cfg)
		return pos.LastInputSeq == 22
	})
	if err != nil {
		t.Fatalf("second Run: %v", err)
	}
	all, _ := recovery.ListBatchRecords(layout)
	next = 0
	for _, r := range all {
		if r.FirstInputSeq != next {
			t.Fatalf("after resume, record %d starts at %d, want %d", r.BatchID, r.FirstInputSeq, next)
		}
		next = r.LastInputSeq + 2
	}
}

func TestRunHaltsOnUnwiredSource(t *testing.T) {
	cfg := validConfig(t, failingSource{})
	cfg.StoragePath = filepath.Join(t.TempDir(), "x")
	rt := New(cfg)
	rt.Register(scriptDomain{})
	err := rt.Run(context.Background())
	if !errors.Is(err, recovery.ErrHalted) {
		t.Fatalf("Run = %v, want ErrHalted", err)
	}
}

type failingSource struct{}

func (failingSource) FrontierHeight() (uint64, error) {
	return 0, errors.New("znnd down")
}

func (failingSource) DetailedMomentumsByHeight(uint64, uint64) ([]*nom.DetailedMomentum, error) {
	return nil, errors.New("znnd down")
}

func TestRunRecoversFromReorgAboveCommit(t *testing.T) {
	src := &chainSource{frontier: 12, moms: buildChain(t, 10, 0)}
	cfg := validConfig(t, src)
	rt := New(cfg)
	rt.Register(scriptDomain{})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	result := make(chan error, 1)
	go func() { result <- rt.Run(ctx) }()

	waitFor := func(cond func(recovery.Position) bool) recovery.Position {
		t.Helper()
		for deadline := time.Now().Add(15 * time.Second); time.Now().Before(deadline); time.Sleep(10 * time.Millisecond) {
			if pos, ok := cursorAt(t, cfg); ok && cond(pos) {
				return pos
			}
		}
		t.Fatalf("condition not reached in time")
		return recovery.Position{}
	}
	waitFor(func(p recovery.Position) bool { return p.LastInputSeq == 18 })

	// Heights 10 and up now sit on another branch; 9 and below are unchanged.
	src.set(buildChainFrom(t, 12, func(h uint64) byte {
		if h >= 10 {
			return 1
		}
		return 0
	}), 14)
	final := waitFor(func(p recovery.Position) bool { return p.MomentumHeight == 12 })
	cancel()
	if err := <-result; err != nil {
		t.Fatalf("Run: %v", err)
	}

	if final.LastInputSeq != 22 || final.MomentumHash != momHash(12, 1) {
		t.Fatalf("final cursor = %+v", final)
	}
	layout, _ := recovery.NewLayout(cfg.StoragePath, cfg.DomainID)
	records, _ := recovery.ListBatchRecords(layout)
	var next uint64
	for _, r := range records {
		if r.FirstInputSeq != next {
			t.Fatalf("record %d starts at %d, want %d", r.BatchID, r.FirstInputSeq, next)
		}
		next = r.LastInputSeq + 2
	}
}

// --- run helpers ---

// live is a running Run with a source the test can steer.
type live struct {
	t      *testing.T
	cfg    Config
	src    *chainSource
	cancel context.CancelFunc
	done   chan struct{}
	err    error
}

// startRun starts Run over cfg with dom registered. The run is cancelled and awaited when the
// test ends.
func startRun(t *testing.T, cfg Config, src *chainSource, dom domain.Domain) *live {
	t.Helper()
	rt := New(cfg)
	rt.Register(dom)
	ctx, cancel := context.WithCancel(context.Background())
	l := &live{t: t, cfg: cfg, src: src, cancel: cancel, done: make(chan struct{})}
	go func() {
		l.err = rt.Run(ctx)
		close(l.done)
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case <-l.done:
		case <-time.After(20 * time.Second):
			t.Errorf("Run did not stop after cancel")
		}
	})
	return l
}

// stop cancels the run and returns Run's error.
func (l *live) stop() error {
	l.t.Helper()
	l.cancel()
	select {
	case <-l.done:
	case <-time.After(20 * time.Second):
		l.t.Fatalf("Run did not stop after cancel")
	}
	return l.err
}

// exit waits for Run to return on its own.
func (l *live) exit() error {
	l.t.Helper()
	select {
	case <-l.done:
		return l.err
	case <-time.After(20 * time.Second):
		l.t.Fatalf("Run did not return")
		return nil
	}
}

// poll waits for cond, and reports false when Run returned first.
func (l *live) poll(desc string, cond func() bool) bool {
	l.t.Helper()
	deadline := time.Now().Add(25 * time.Second)
	for time.Now().Before(deadline) {
		select {
		case <-l.done:
			return false
		default:
		}
		if cond() {
			return true
		}
		time.Sleep(5 * time.Millisecond)
	}
	l.t.Fatalf("condition not reached in time: %s", desc)
	return false
}

// waitFor is poll that fails the test when Run returns first.
func (l *live) waitFor(desc string, cond func() bool) {
	l.t.Helper()
	if !l.poll(desc, cond) {
		l.t.Fatalf("Run returned while waiting for %s: %v", desc, l.err)
	}
}

func (l *live) cursorHeight(h uint64) func() bool {
	return func() bool {
		pos, ok := cursorAt(l.t, l.cfg)
		return ok && pos.MomentumHeight == h
	}
}

// advance points the source at chain with the given frontier and waits for the cursor to reach
// the new confirmation ceiling. It returns Run's error when Run returned first.
func (l *live) advance(chain []*nom.DetailedMomentum, frontier uint64) error {
	l.t.Helper()
	l.src.set(chain, frontier)
	if !l.poll(fmt.Sprintf("cursor at height %d", frontier-l.cfg.ConfirmationDepth), l.cursorHeight(frontier-l.cfg.ConfirmationDepth)) {
		return l.err
	}
	return nil
}

func (l *live) step(chain []*nom.DetailedMomentum, frontier uint64) {
	l.t.Helper()
	if err := l.advance(chain, frontier); err != nil {
		l.t.Fatalf("Run returned at frontier %d: %v", frontier, err)
	}
}

// settle waits until the loop has started one more source poll, which means it finished
// processing whatever the previous poll read.
func (l *live) settle() {
	l.t.Helper()
	base := l.src.frontierReads()
	l.waitFor("a further source poll", func() bool { return l.src.frontierReads() > base })
}

func (s *chainSource) returnedTo() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.highestReturned
}

func saltFrom(fork uint64, salt byte) func(uint64) byte {
	return func(h uint64) byte {
		if h >= fork {
			return salt
		}
		return 0
	}
}

func dense(uint64) int { return 1 }

func layoutOf(t *testing.T, cfg Config) recovery.Layout {
	t.Helper()
	l, err := recovery.NewLayout(cfg.StoragePath, cfg.DomainID)
	if err != nil {
		t.Fatal(err)
	}
	return l
}

// derivedInputs returns every input's global index across the records on disk, in record order,
// and fails when a bundle holds an index outside its record's range or out of order.
func derivedInputs(t *testing.T, cfg Config) []uint64 {
	t.Helper()
	layout := layoutOf(t, cfg)
	records, err := recovery.ListBatchRecords(layout)
	if err != nil {
		t.Fatal(err)
	}
	bundles, err := recovery.NewBundleStore(layout.Bundles)
	if err != nil {
		t.Fatal(err)
	}
	var out []uint64
	for _, r := range records {
		raw, err := bundles.Get(r.DAHash)
		if err != nil {
			t.Fatalf("bundle of record %d: %v", r.BatchID, err)
		}
		inputs, err := codec.DecodeBundle(raw)
		if err != nil {
			t.Fatalf("decode bundle of record %d: %v", r.BatchID, err)
		}
		for _, in := range inputs {
			idx := in.GlobalInputIndex
			if idx < r.FirstInputSeq || idx > r.LastInputSeq {
				t.Fatalf("record %d spans [%d, %d] but its bundle holds index %d", r.BatchID, r.FirstInputSeq, r.LastInputSeq, idx)
			}
			if len(out) > 0 && idx <= out[len(out)-1] {
				t.Fatalf("index %d follows %d: input indices are not strictly ascending", idx, out[len(out)-1])
			}
			out = append(out, idx)
		}
	}
	return out
}

// assertRecordsContiguous checks the batch records on disk: ids run 0..n-1 and ranges ascend
// without overlap.
func assertRecordsContiguous(t *testing.T, cfg Config) {
	t.Helper()
	records, err := recovery.ListBatchRecords(layoutOf(t, cfg))
	if err != nil {
		t.Fatal(err)
	}
	for i, r := range records {
		if r.BatchID != uint64(i) {
			t.Fatalf("record %d has batch id %d", i, r.BatchID)
		}
		if r.FirstInputSeq > r.LastInputSeq {
			t.Fatalf("record %d spans [%d, %d]", r.BatchID, r.FirstInputSeq, r.LastInputSeq)
		}
		if i > 0 && r.FirstInputSeq <= records[i-1].LastInputSeq {
			t.Fatalf("record %d starts at %d, inside record %d ending at %d", r.BatchID, r.FirstInputSeq, records[i-1].BatchID, records[i-1].LastInputSeq)
		}
	}
}

// rootAtIndex returns the post-state root the bundles on disk recorded at input index seq.
func rootAtIndex(t *testing.T, cfg Config, seq uint64) types.Hash {
	t.Helper()
	layout := layoutOf(t, cfg)
	records, err := recovery.ListBatchRecords(layout)
	if err != nil {
		t.Fatal(err)
	}
	bundles, err := recovery.NewBundleStore(layout.Bundles)
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range records {
		raw, err := bundles.Get(r.DAHash)
		if err != nil {
			t.Fatal(err)
		}
		inputs, err := codec.DecodeBundle(raw)
		if err != nil {
			t.Fatal(err)
		}
		for _, in := range inputs {
			if in.GlobalInputIndex == seq {
				res, err := codec.DecodeExecutionResult(in.ExecutionResult)
				if err != nil {
					t.Fatal(err)
				}
				return res.PostStateRoot
			}
		}
	}
	t.Fatalf("no record holds input index %d", seq)
	return types.Hash{}
}

// reference runs a fresh executor over chain alone, in its own storage, until the cursor reaches
// the confirmation ceiling, and returns its config, cursor and derived input indices.
func reference(t *testing.T, chain []*nom.DetailedMomentum, frontier uint64) (Config, recovery.Position, []uint64) {
	t.Helper()
	src := &chainSource{}
	src.set(chain, frontier)
	cfg := validConfig(t, src)
	ceiling := frontier - cfg.ConfirmationDepth
	err := runUntil(t, cfg, func() bool {
		pos, ok := cursorAt(t, cfg)
		return ok && pos.MomentumHeight == ceiling
	})
	if err != nil {
		t.Fatalf("reference run: %v", err)
	}
	pos, _ := cursorAt(t, cfg)
	return cfg, pos, derivedInputs(t, cfg)
}

// assertMatchesReference checks that the run in cfg ended where a fresh run over the final chain
// ends: the same cursor, the same root, the same derived input indices, contiguous records.
func assertMatchesReference(t *testing.T, cfg Config, want recovery.Position, wantInputs []uint64) {
	t.Helper()
	got, ok := cursorAt(t, cfg)
	if !ok {
		t.Fatalf("no cursor")
	}
	// Batch boundaries depend on when the source drained, so NextBatchID is not comparable.
	got.NextBatchID, want.NextBatchID = 0, 0
	if got != want {
		t.Fatalf("cursor = %+v, a fresh run over the final chain ends at %+v", got, want)
	}
	if inputs := derivedInputs(t, cfg); !reflect.DeepEqual(inputs, wantInputs) {
		t.Fatalf("derived input indices = %v, want %v", inputs, wantInputs)
	}
	assertRecordsContiguous(t, cfg)
}

type fixedAnchor struct {
	anchor recovery.Anchor
	ok     bool
}

func (f fixedAnchor) FinalizedAnchor(uint32) (recovery.Anchor, bool, error) {
	return f.anchor, f.ok, nil
}

func windowOf(t *testing.T, cfg Config) []recovery.MomentumRef {
	t.Helper()
	w, err := recovery.ReadMomentumWindow(layoutOf(t, cfg))
	if err != nil {
		t.Fatal(err)
	}
	return w
}

func refs(h0, h1 uint64, salt byte) []recovery.MomentumRef {
	var out []recovery.MomentumRef
	for h := h0; h <= h1; h++ {
		out = append(out, recovery.MomentumRef{Height: h, Hash: momHash(h, salt)})
	}
	return out
}

// --- crash recovery against a finalized anchor ---

// crashedAt4 runs the domain over a ten-momentum chain up to a cursor at momentum 4 and cancels,
// leaving the layout a crash would leave. It returns the config, the source and the full chain.
func crashedAt4(t *testing.T) (Config, *chainSource, []*nom.DetailedMomentum) {
	t.Helper()
	chain := buildChain(t, 10, 0)
	src := &chainSource{}
	src.set(chain, 6)
	cfg := validConfig(t, src)
	err := runUntil(t, cfg, func() bool {
		pos, ok := cursorAt(t, cfg)
		return ok && pos.MomentumHeight == 4
	})
	if err != nil {
		t.Fatalf("first run: %v", err)
	}
	if pos, _ := cursorAt(t, cfg); pos.LastInputSeq != 6 {
		t.Fatalf("crash cursor = %+v", pos)
	}
	return cfg, src, chain
}

func TestCrashRecoveryAgainstFinalizedAnchor(t *testing.T) {
	t.Parallel()
	chain := buildChain(t, 10, 0)
	_, refPos, refInputs := reference(t, chain, 12)
	if want := []uint64{0, 2, 4, 6, 8, 10, 12, 14, 16, 18}; !reflect.DeepEqual(refInputs, want) {
		t.Fatalf("reference inputs = %v, want %v", refInputs, want)
	}

	// The anchor sits strictly below the crash cursor (input 6) and carries the root the run's
	// own bundle recorded at that index.
	t.Run("anchor below the crash position", func(t *testing.T) {
		t.Parallel()
		cfg, src, chain := crashedAt4(t)
		cfg.Anchors = fixedAnchor{recovery.Anchor{LastInputSeq: 4, PostStateRoot: rootAtIndex(t, cfg, 4)}, true}
		src.set(chain, 12)
		err := runUntil(t, cfg, func() bool {
			pos, ok := cursorAt(t, cfg)
			return ok && pos.MomentumHeight == 10
		})
		if err != nil {
			t.Fatalf("second run: %v", err)
		}
		assertMatchesReference(t, cfg, refPos, refInputs)
	})

	t.Run("anchor at the crash position", func(t *testing.T) {
		t.Parallel()
		cfg, src, chain := crashedAt4(t)
		pos, _ := cursorAt(t, cfg)
		cfg.Anchors = fixedAnchor{recovery.Anchor{LastInputSeq: pos.LastInputSeq, PostStateRoot: pos.PostStateRoot}, true}
		src.set(chain, 12)
		err := runUntil(t, cfg, func() bool {
			p, ok := cursorAt(t, cfg)
			return ok && p.MomentumHeight == 10
		})
		if err != nil {
			t.Fatalf("second run: %v", err)
		}
		assertMatchesReference(t, cfg, refPos, refInputs)
	})

	t.Run("anchor below the crash position with another root halts", func(t *testing.T) {
		t.Parallel()
		cfg, src, chain := crashedAt4(t)
		wrong := rootAtIndex(t, cfg, 4)
		wrong[0] ^= 0xff
		cfg.Anchors = fixedAnchor{recovery.Anchor{LastInputSeq: 4, PostStateRoot: wrong}, true}
		src.set(chain, 12)
		err := runUntil(t, cfg, func() bool { return false })
		if !errors.Is(err, recovery.ErrHalted) {
			t.Fatalf("Run = %v, want ErrHalted", err)
		}
		if pos, _ := cursorAt(t, cfg); pos.MomentumHeight != 4 {
			t.Fatalf("cursor moved to %+v after a failed reconcile", pos)
		}
	})
}

// --- reorg within the window ---

func TestReorgWithinWindowRederivesOnTheNewChain(t *testing.T) {
	t.Parallel()
	// Momenta 9 and up are replaced by momenta holding two inputs for the domain instead of one,
	// so the post-reorg index stream differs from the pre-reorg one.
	a := buildShape(1, 12, saltFrom(1<<62, 0), dense)
	b := buildShape(1, 16, saltFrom(9, 1), func(h uint64) int {
		if h >= 9 {
			return 2
		}
		return 1
	})
	refCfg, refPos, refInputs := reference(t, b, 18)

	run := func(t *testing.T, cfg Config, src *chainSource) *live {
		l := startRun(t, cfg, src, scriptDomain{})
		l.step(a, 4)
		l.step(a, 8)
		l.step(a, 12)
		return l
	}

	t.Run("no anchor", func(t *testing.T) {
		t.Parallel()
		src := &chainSource{}
		cfg := validConfig(t, src)
		l := run(t, cfg, src)
		pre, _ := cursorAt(t, cfg)
		l.step(b, 18)
		if err := l.stop(); err != nil {
			t.Fatalf("Run: %v", err)
		}
		if got, _ := cursorAt(t, cfg); got.PostStateRoot == pre.PostStateRoot {
			t.Fatalf("root did not change across the reorg")
		}
		assertMatchesReference(t, cfg, refPos, refInputs)
	})

	// The anchor lies beyond everything the pre-reorg chain reached and inside a batch of the
	// post-reorg chain, so the check can only fire on the re-derived stream.
	anchorAhead := func(t *testing.T, mutate func(*types.Hash)) (Config, *chainSource) {
		x := refInputs[len(refInputs)-4]
		if x <= 18 {
			t.Fatalf("anchor index %d is not beyond the pre-reorg stream", x)
		}
		root := rootAtIndex(t, refCfg, x)
		mutate(&root)
		src := &chainSource{}
		cfg := validConfig(t, src)
		cfg.Anchors = fixedAnchor{recovery.Anchor{LastInputSeq: x, PostStateRoot: root}, true}
		return cfg, src
	}

	t.Run("anchor ahead of the fork with the correct root", func(t *testing.T) {
		t.Parallel()
		cfg, src := anchorAhead(t, func(*types.Hash) {})
		l := run(t, cfg, src)
		l.step(b, 18)
		if err := l.stop(); err != nil {
			t.Fatalf("Run: %v", err)
		}
		assertMatchesReference(t, cfg, refPos, refInputs)
	})

	t.Run("anchor ahead of the fork with another root halts on the re-derived stream", func(t *testing.T) {
		t.Parallel()
		cfg, src := anchorAhead(t, func(h *types.Hash) { h[0] ^= 0xff })
		l := run(t, cfg, src)
		if pre, _ := cursorAt(t, cfg); pre.MomentumHeight != 10 {
			t.Fatalf("pre-reorg cursor = %+v", pre)
		}
		src.set(b, 18)
		err := l.exit()
		if !errors.Is(err, recovery.ErrHalted) || !strings.Contains(err.Error(), "differs from the finalized root") {
			t.Fatalf("Run = %v, want an ErrHalted root mismatch", err)
		}
	})
}

// --- reorg shapes ---

func TestReorgDeeperThanTheWindowHalts(t *testing.T) {
	t.Parallel()
	a := buildShape(1, 12, saltFrom(1<<62, 0), dense)
	b := buildShape(1, 14, saltFrom(5, 1), dense)
	src := &chainSource{}
	cfg := validConfig(t, src)
	l := startRun(t, cfg, src, scriptDomain{})
	l.step(a, 4)
	l.step(a, 8)
	l.step(a, 12)
	if w := windowOf(t, cfg); !reflect.DeepEqual(w, refs(8, 10, 0)) {
		t.Fatalf("window = %v", w)
	}

	src.set(b, 16)
	err := l.exit()
	if !errors.Is(err, recovery.ErrHalted) || !strings.Contains(err.Error(), "below every") {
		t.Fatalf("Run = %v, want an ErrHalted naming a fork below every recorded height", err)
	}
	if pos, _ := cursorAt(t, cfg); pos.MomentumHeight != 10 {
		t.Fatalf("cursor = %+v, a halted reorg must not move it", pos)
	}
}

func TestReorgBeforeTheFirstCommitRollsBackToGenesis(t *testing.T) {
	t.Parallel()
	// Momenta 1-6 hold nothing for the domain, so the follower consumes them without any commit.
	a := buildShape(1, 6, saltFrom(1<<62, 0), func(uint64) int { return 0 })
	b := buildShape(1, 10, saltFrom(4, 1), func(h uint64) int {
		if h >= 4 {
			return 1
		}
		return 0
	})
	_, refPos, refInputs := reference(t, b, 12)

	src := &chainSource{}
	src.set(a, 8)
	cfg := validConfig(t, src)
	l := startRun(t, cfg, src, scriptDomain{})
	l.waitFor("momentum 6 consumed", func() bool { return src.returnedTo() >= 6 })
	l.settle()
	if _, ok := cursorAt(t, cfg); ok {
		t.Fatalf("a cursor exists before any input was derived")
	}
	if w := windowOf(t, cfg); len(w) != 0 {
		t.Fatalf("window = %v before the first commit", w)
	}

	l.step(b, 12)
	if err := l.stop(); err != nil {
		t.Fatalf("Run: %v", err)
	}
	assertMatchesReference(t, cfg, refPos, refInputs)
}

// rolledBelowWindow drives a run whose first reorg lands below every recorded window entry: two
// commits (cursors 4 and 10, window 8-10), then a fork at 9 that selects the snapshot at 4. The
// post-fork chain holds nothing for the domain, so nothing commits afterwards and the recorded
// window stays the single cursor entry.
func rolledBelowWindow(t *testing.T) *live {
	t.Helper()
	a := buildShape(1, 10, saltFrom(1<<62, 0), func(h uint64) int {
		if h <= 4 || h == 10 {
			return 1
		}
		return 0
	})
	b := buildShape(1, 12, saltFrom(9, 1), func(h uint64) int {
		if h <= 4 {
			return 1
		}
		return 0
	})
	src := &chainSource{}
	cfg := validConfig(t, src)
	l := startRun(t, cfg, src, scriptDomain{})
	l.step(a, 6)
	l.step(a, 12)
	if w := windowOf(t, cfg); !reflect.DeepEqual(w, refs(8, 10, 0)) {
		t.Fatalf("window before the reorg = %v", w)
	}

	src.set(b, 14)
	l.waitFor("re-derivation from momentum 5", func() bool { return src.requestedFrom(5) })
	l.settle()
	pos, _ := cursorAt(t, cfg)
	if pos.MomentumHeight != 4 || pos.MomentumHash != momHash(4, 0) {
		t.Fatalf("cursor after the rollback = %+v, want the snapshot at momentum 4", pos)
	}
	if w := windowOf(t, cfg); !reflect.DeepEqual(w, refs(4, 4, 0)) {
		t.Fatalf("window after the rollback = %v, want the single cursor entry", w)
	}
	return l
}

func TestSecondReorgAfterSingleEntryWindow(t *testing.T) {
	t.Parallel()

	t.Run("below the single entry halts", func(t *testing.T) {
		t.Parallel()
		l := rolledBelowWindow(t)
		c := buildShape(1, 14, saltFrom(4, 2), dense)
		l.src.set(c, 16)
		err := l.exit()
		if !errors.Is(err, recovery.ErrHalted) || !strings.Contains(err.Error(), "below every") {
			t.Fatalf("Run = %v, want an ErrHalted naming a fork below every recorded height", err)
		}
		if pos, _ := cursorAt(t, l.cfg); pos.MomentumHeight != 4 {
			t.Fatalf("cursor = %+v, a halted reorg must not move it", pos)
		}
	})

	t.Run("at or above the single entry rolls back and completes", func(t *testing.T) {
		t.Parallel()
		l := rolledBelowWindow(t)
		c := buildShape(1, 14, saltFrom(5, 2), dense)
		_, refPos, refInputs := reference(t, c, 16)
		l.step(c, 16)
		if err := l.stop(); err != nil {
			t.Fatalf("Run: %v", err)
		}
		assertMatchesReference(t, l.cfg, refPos, refInputs)
	})
}

func TestReorgAboveTheLastCommitKeepsTheFullWindow(t *testing.T) {
	t.Parallel()
	// Momenta 11 and 12 hold nothing for the domain: the follower consumes past the last commit
	// (momentum 10) without committing again.
	inputsThrough10 := func(h uint64) int {
		if h <= 10 {
			return 1
		}
		return 0
	}
	x := buildShape(1, 12, saltFrom(1<<62, 0), inputsThrough10)
	b := buildShape(1, 14, saltFrom(11, 1), inputsThrough10)
	c := buildShape(1, 16, saltFrom(10, 2), dense)
	_, refPos, refInputs := reference(t, c, 18)

	src := &chainSource{}
	cfg := validConfig(t, src)
	l := startRun(t, cfg, src, scriptDomain{})
	l.step(x, 4)
	l.step(x, 8)
	l.step(x, 12)
	before, _ := cursorAt(t, cfg)

	src.set(x, 14)
	l.waitFor("momentum 12 consumed", func() bool { return src.returnedTo() >= 12 })
	l.settle()

	// Only momenta strictly above the committed height are rewritten.
	src.set(b, 16)
	l.waitFor("re-derivation from momentum 11", func() bool { return src.requestedFrom(11) })
	l.settle()
	if after, _ := cursorAt(t, cfg); after != before {
		t.Fatalf("cursor = %+v after a reorg above it, want %+v", after, before)
	}
	if w := windowOf(t, cfg); !reflect.DeepEqual(w, refs(8, 10, 0)) {
		t.Fatalf("window = %v, want the full three-entry window 8-10", w)
	}

	// A second reorg forking inside that window, before any further commit.
	l.step(c, 18)
	if err := l.stop(); err != nil {
		t.Fatalf("Run: %v", err)
	}
	assertMatchesReference(t, cfg, refPos, refInputs)
}

// --- anchor verification while running ---

func TestAnchorVerificationDuringTheRun(t *testing.T) {
	t.Parallel()
	chain := buildChain(t, 10, 0)
	refCfg, _, _ := reference(t, chain, 12)
	rootOf := func(seq uint64) types.Hash { return rootAtIndex(t, refCfg, seq) }
	flip := func(h types.Hash) types.Hash { h[0] ^= 0xff; return h }

	// Frontiers 5, 11 and 12 build the batches [0, 2, 4] and [6 .. 16] and then [18].
	steps := []uint64{5, 11, 12}
	drive := func(t *testing.T, cfg Config, src *chainSource, chain []*nom.DetailedMomentum) error {
		l := startRun(t, cfg, src, scriptDomain{})
		for _, f := range steps {
			if err := l.advance(chain, f); err != nil {
				return err
			}
		}
		return l.stop()
	}
	run := func(t *testing.T, chain []*nom.DetailedMomentum, a recovery.Anchor) (Config, error) {
		src := &chainSource{}
		cfg := validConfig(t, src)
		cfg.Anchors = fixedAnchor{a, true}
		return cfg, drive(t, cfg, src, chain)
	}

	proceeds := map[string]uint64{
		"index strictly inside a later batch": 10,
		"last input of a batch":               4,
		"first input of a batch":              6,
		"index beyond the derived stream":     500,
	}
	for name, seq := range proceeds {
		name, seq := name, seq
		t.Run("correct root, "+name, func(t *testing.T) {
			t.Parallel()
			root := types.Hash{0x01}
			if seq < 20 {
				root = rootOf(seq)
			}
			cfg, err := run(t, chain, recovery.Anchor{LastInputSeq: seq, PostStateRoot: root})
			if err != nil {
				t.Fatalf("Run: %v", err)
			}
			if pos, _ := cursorAt(t, cfg); pos.MomentumHeight != 10 {
				t.Fatalf("cursor = %+v", pos)
			}
			if seq == 10 {
				records, _ := recovery.ListBatchRecords(layoutOf(t, cfg))
				inside := false
				for _, r := range records {
					inside = inside || (r.FirstInputSeq < seq && seq < r.LastInputSeq)
				}
				if !inside {
					t.Fatalf("no batch holds index %d strictly inside: %+v", seq, records)
				}
			}
		})
	}

	halts := map[string]struct {
		chain  []*nom.DetailedMomentum
		anchor recovery.Anchor
		text   string
	}{
		"wrong root inside a later batch":    {chain, recovery.Anchor{LastInputSeq: 10, PostStateRoot: flip(rootOf(10))}, "differs from the finalized root"},
		"wrong root at a batch's last input": {chain, recovery.Anchor{LastInputSeq: 4, PostStateRoot: flip(rootOf(4))}, "differs from the finalized root"},
		"index the domain never consumed":    {chain, recovery.Anchor{LastInputSeq: 9, PostStateRoot: rootOf(8)}, "no record at the finalized anchor"},
		"index below the first derived input": {
			buildShape(1, 10, saltFrom(1<<62, 0), func(h uint64) int {
				if h == 1 {
					return 0
				}
				return 1
			}),
			recovery.Anchor{LastInputSeq: 0, PostStateRoot: types.Hash{0x01}},
			"not in the derived stream",
		},
	}
	for name, c := range halts {
		name, c := name, c
		t.Run(name+" halts", func(t *testing.T) {
			t.Parallel()
			_, err := run(t, c.chain, c.anchor)
			if !errors.Is(err, recovery.ErrHalted) || !strings.Contains(err.Error(), c.text) {
				t.Fatalf("Run = %v, want ErrHalted containing %q", err, c.text)
			}
		})
	}
}

// --- momentum source failures ---

// flakySource fails FrontierHeight according to fail, called with the 1-based call number.
type flakySource struct {
	ledger.MomentumSource
	mu    sync.Mutex
	calls int
	fail  func(call int) error
}

func (f *flakySource) FrontierHeight() (uint64, error) {
	f.mu.Lock()
	f.calls++
	n := f.calls
	f.mu.Unlock()
	if err := f.fail(n); err != nil {
		return 0, err
	}
	return f.MomentumSource.FrontierHeight()
}

func (f *flakySource) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

func TestSourceRetryBudget(t *testing.T) {
	t.Parallel()
	const interval = 60 * time.Millisecond
	down := errors.New("znnd down")
	newRun := func(attempts int, interval time.Duration, fail func(int) error) (Config, *flakySource) {
		inner := &chainSource{}
		inner.set(buildChain(t, 6, 0), 8)
		src := &flakySource{MomentumSource: inner, fail: fail}
		cfg := validConfig(t, src)
		cfg.Retry = recovery.RetryPolicy{SourceAttempts: attempts, SourceInterval: interval}
		return cfg, src
	}
	reached := func(cfg Config) func() bool {
		return func() bool {
			pos, ok := cursorAt(t, cfg)
			return ok && pos.MomentumHeight == 6
		}
	}

	t.Run("consecutive failures equal to the budget halt", func(t *testing.T) {
		t.Parallel()
		cfg, src := newRun(3, interval, func(int) error { return down })
		start := time.Now()
		err := runUntil(t, cfg, func() bool { return false })
		if !errors.Is(err, recovery.ErrHalted) || !errors.Is(err, down) {
			t.Fatalf("Run = %v, want ErrHalted wrapping the source error", err)
		}
		if n := src.callCount(); n != 3 {
			t.Fatalf("source called %d times, want 3", n)
		}
		if elapsed := time.Since(start); elapsed < 2*interval {
			t.Fatalf("Run returned after %v, want at least the two waits between three attempts (%v)", elapsed, 2*interval)
		}
	})

	t.Run("failures short of the budget recover", func(t *testing.T) {
		t.Parallel()
		cfg, _ := newRun(3, interval, func(n int) error {
			if n <= 2 {
				return down
			}
			return nil
		})
		if err := runUntil(t, cfg, reached(cfg)); err != nil {
			t.Fatalf("Run: %v", err)
		}
	})

	t.Run("a success resets the budget", func(t *testing.T) {
		t.Parallel()
		cfg, _ := newRun(2, interval, func(n int) error {
			if n%2 == 1 {
				return down
			}
			return nil
		})
		if err := runUntil(t, cfg, reached(cfg)); err != nil {
			t.Fatalf("Run: %v", err)
		}
	})

	t.Run("a halting source error returns without waiting", func(t *testing.T) {
		t.Parallel()
		cfg, src := newRun(5, 3*time.Second, func(int) error { return fmt.Errorf("credentials revoked: %w", recovery.ErrHalted) })
		start := time.Now()
		err := runUntil(t, cfg, func() bool { return false })
		if !errors.Is(err, recovery.ErrHalted) {
			t.Fatalf("Run = %v, want ErrHalted", err)
		}
		if n := src.callCount(); n != 1 {
			t.Fatalf("source called %d times, want 1", n)
		}
		if elapsed := time.Since(start); elapsed >= time.Second {
			t.Fatalf("Run returned after %v: the halting error consumed retry budget", elapsed)
		}
	})
}

// --- commit interval ---

// streamSource is a source with no end: its frontier is always far ahead of the follower, and it
// generates the momenta a read asks for, at most perRead of them when perRead is set.
type streamSource struct{ perRead uint64 }

func (streamSource) FrontierHeight() (uint64, error) { return 1 << 40, nil }

func (s streamSource) DetailedMomentumsByHeight(height, count uint64) ([]*nom.DetailedMomentum, error) {
	if s.perRead > 0 && count > s.perRead {
		count = s.perRead
	}
	return buildShape(height, height+count-1, saltFrom(1<<62, 0), dense), nil
}

// gateDomain is scriptDomain that blocks the apply of input number stops[i] until the test lets
// it continue, which freezes the loop at a known point.
type gateDomain struct {
	scriptDomain
	applied atomic.Int64
	stops   []int64
	hit     chan int64
	proceed chan struct{}
	abort   chan struct{}
}

func (g *gateDomain) Apply(a domain.StateAccess, in domain.Input) (domain.Effects, error) {
	n := g.applied.Add(1)
	for _, s := range g.stops {
		if s != n {
			continue
		}
		select {
		case g.hit <- n:
		case <-g.abort:
		}
		select {
		case <-g.proceed:
		case <-g.abort:
		}
	}
	return g.scriptDomain.Apply(a, in)
}

// TestCommitIntervalWithOneMomentumPerRead runs against a source that never drains and returns
// one momentum per read, so the follower's buffer empties at every momentum boundary.
func TestCommitIntervalWithOneMomentumPerRead(t *testing.T) {
	commitIntervalBoundsUncommitted(t, streamSource{perRead: 1})
}

// TestCommitIntervalWithFullRangedReads runs against a source that never drains and answers each
// ranged read in full, as znnd does up to its 1024-momentum cap. The follower buffers a whole
// read, so a commit boundary exists only where a read ends.
func TestCommitIntervalWithFullRangedReads(t *testing.T) {
	commitIntervalBoundsUncommitted(t, streamSource{})
}

// commitIntervalBoundsUncommitted asserts that, with commitInputInterval lowered, a commit lands
// within the interval plus one momentum of every point the run reaches, and that the cursor
// advances repeatedly while the source never drains.
func commitIntervalBoundsUncommitted(t *testing.T, source streamSource) {
	const interval = 5
	old := commitInputInterval
	commitInputInterval = interval
	t.Cleanup(func() { commitInputInterval = old })

	g := &gateDomain{
		stops:   []int64{30, 60, 90},
		hit:     make(chan int64),
		proceed: make(chan struct{}),
		abort:   make(chan struct{}),
	}

	cfg := validConfig(t, source)
	l := startRun(t, cfg, nil, g)
	// Registered after startRun so it runs first: a blocked apply must release before the
	// run's own cleanup waits for Run to return.
	t.Cleanup(func() {
		l.cancel()
		close(g.abort)
	})

	var last uint64
	for i, stop := range g.stops {
		select {
		case <-g.hit:
		case <-l.done:
			t.Fatalf("Run returned before input %d: %v", stop, l.err)
		case <-time.After(20 * time.Second):
			t.Fatalf("input %d was never applied", stop)
		}
		// Inputs 1..stop-1 are applied. Each momentum holds one input for the domain, so the
		// input numbered k has global index 2(k-1).
		pos, ok := cursorAt(t, cfg)
		if !ok {
			t.Fatalf("stop %d: no commit after %d inputs with a commit interval of %d", stop, stop-1, interval)
		}
		committed := int64(pos.LastInputSeq/2) + 1
		if uncommitted := stop - 1 - committed; uncommitted > interval+1 {
			t.Fatalf("stop %d: %d applied inputs are uncommitted, more than the interval %d plus one momentum", stop, uncommitted, interval)
		}
		if i > 0 && pos.LastInputSeq <= last {
			t.Fatalf("stop %d: cursor at input %d did not advance past %d", stop, pos.LastInputSeq, last)
		}
		last = pos.LastInputSeq
		g.proceed <- struct{}{}
	}
	if err := l.stop(); err != nil {
		t.Fatalf("Run: %v", err)
	}
}

// --- carried inputs ---

// capDomain is scriptDomain whose every input also withdraws more than half of the per-batch
// withdrawal cap, so no batch holds two inputs: the driver admits one, then closes the batch and
// carries the next input it has already read into the batch after.
type capDomain struct{ scriptDomain }

func (capDomain) Apply(a domain.StateAccess, in domain.Input) (domain.Effects, error) {
	eff, err := scriptDomain{}.Apply(a, in)
	if err != nil {
		return eff, err
	}
	eff.ContractID = types.Hash{0x07}
	eff.OutboxMessages = []codec.OutboxMessage{{
		SourceContract: types.Hash{0x07},
		InputIndex:     in.(scriptInput).idx,
		Target: codec.L1Withdrawal{
			RecipientL1: types.Address{0x09},
			Asset:       codec.AssetIDFromZTS(types.ZenonTokenStandard{0x01}),
			Amount:      codec.U256{0x60},
		},
	}}
	return eff, nil
}

// pausingSource blocks its pauseAt-th FrontierHeight call until the test releases it.
type pausingSource struct {
	*chainSource
	pauseAt int
	mu      sync.Mutex
	calls   int
	hit     chan struct{}
	release chan struct{}
}

func (p *pausingSource) FrontierHeight() (uint64, error) {
	p.mu.Lock()
	p.calls++
	n := p.calls
	p.mu.Unlock()
	if n == p.pauseAt {
		close(p.hit)
		<-p.release
	}
	return p.chainSource.FrontierHeight()
}

// TestCommitWaitsForCarriedInput freezes the loop right after the batch that carries the
// follower's last input: the follower is at a momentum boundary with nothing buffered, but the
// carried input has been emitted and belongs to no built batch. Committing there would resume
// past it.
func TestCommitWaitsForCarriedInput(t *testing.T) {
	chain := buildChain(t, 6, 0)
	_, refPos, refInputs := reference(t, chain, 8)

	inner := &chainSource{}
	inner.set(chain, 8)
	src := &pausingSource{chainSource: inner, pauseAt: 2, hit: make(chan struct{}), release: make(chan struct{})}
	cfg := validConfig(t, src)
	l := startRun(t, cfg, inner, capDomain{})
	// Registered after startRun so it runs first and frees the loop if the test fails early.
	var once sync.Once
	releaseOnce := func() { once.Do(func() { close(src.release) }) }
	t.Cleanup(releaseOnce)

	select {
	case <-src.hit:
	case <-l.done:
		t.Fatalf("Run returned before the follower drained: %v", l.err)
	case <-time.After(20 * time.Second):
		t.Fatalf("the follower never asked the source a second time")
	}
	if pos, ok := cursorAt(t, cfg); ok {
		t.Fatalf("cursor %+v committed while the last input was carried and unbuilt", pos)
	}
	releaseOnce()

	l.waitFor("cursor at the ceiling", l.cursorHeight(6))
	if err := l.stop(); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if want := []uint64{0, 2, 4, 6, 8, 10}; !reflect.DeepEqual(refInputs, want) {
		t.Fatalf("reference inputs = %v", refInputs)
	}
	got, _ := cursorAt(t, cfg)
	got.NextBatchID, refPos.NextBatchID = 0, 0
	if got != refPos {
		t.Fatalf("cursor = %+v, want %+v", got, refPos)
	}
	if inputs := derivedInputs(t, cfg); !reflect.DeepEqual(inputs, refInputs) {
		t.Fatalf("derived inputs = %v, want %v", inputs, refInputs)
	}
	records, _ := recovery.ListBatchRecords(layoutOf(t, cfg))
	if len(records) != 6 {
		t.Fatalf("%d batch records, want one per input (6)", len(records))
	}
}

// TestCommitBoundaryClosesAtAMomentumBoundary checks the wrapper alone, over a follower that
// buffers one momentum per read: it delivers commitInputInterval inputs, then reports the source
// drained once, then delivers again.
func TestCommitBoundaryClosesAtAMomentumBoundary(t *testing.T) {
	old := commitInputInterval
	commitInputInterval = 3
	t.Cleanup(func() { commitInputInterval = old })

	f, err := ledger.New(ledger.Config{
		DomainID: 1, ConfirmationDepth: 2, StartHeight: 1,
		Source: streamSource{perRead: 1}, Filter: byAddress{},
	})
	if err != nil {
		t.Fatal(err)
	}
	b := &commitBoundary{follower: f}

	var got []string
	for i := 0; i < 9; i++ {
		in, ok, err := b.Next()
		if err != nil {
			t.Fatalf("Next: %v", err)
		}
		if ok {
			got = append(got, fmt.Sprint(in.Ctx.GlobalInputIndex))
		} else {
			got = append(got, "drained")
		}
	}
	want := []string{"0", "2", "4", "drained", "6", "8", "10", "drained", "12"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("sequence = %v, want %v", got, want)
	}
}
