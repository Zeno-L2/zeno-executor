package runtime

import (
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"testing"

	"github.com/zenon-network/go-zenon/common/crypto"
	"github.com/zenon-network/go-zenon/common/types"

	"github.com/Zeno-L2/zeno-executor/domain"
	"github.com/Zeno-L2/zeno-executor/internal/codec"
	"github.com/Zeno-L2/zeno-executor/internal/recovery"
	"github.com/Zeno-L2/zeno-executor/plugins/wasm"
)

// --- fixtures ---

type rewindCall struct {
	seq   uint64
	fresh bool
}

// pruneLog is a mutex-guarded record of every Prune and Rewind argument.
type pruneLog struct {
	mu      sync.Mutex
	prunes  []uint64
	rewinds []rewindCall
}

func (g *pruneLog) logPrune(through uint64) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.prunes = append(g.prunes, through)
}

func (g *pruneLog) logRewind(seq uint64, fresh bool) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.rewinds = append(g.rewinds, rewindCall{seq, fresh})
}

func (g *pruneLog) pruneCalls() []uint64 {
	g.mu.Lock()
	defer g.mu.Unlock()
	return append([]uint64(nil), g.prunes...)
}

func (g *pruneLog) rewindCalls() []rewindCall {
	g.mu.Lock()
	defer g.mu.Unlock()
	return append([]rewindCall(nil), g.rewinds...)
}

// prunedAtLeast reports whether some Prune call had an argument of at least seq.
func (g *pruneLog) prunedAtLeast(seq uint64) func() bool {
	return func() bool {
		for _, p := range g.pruneCalls() {
			if p >= seq {
				return true
			}
		}
		return false
	}
}

// pruneDomain is scriptDomain that implements domain.Pruner by logging.
type pruneDomain struct {
	scriptDomain
	pruneLog
}

func (d *pruneDomain) Rewind(seq uint64, fresh bool) error {
	d.logRewind(seq, fresh)
	return nil
}

func (d *pruneDomain) Prune(through uint64) error {
	d.logPrune(through)
	return nil
}

// scripted is one wasm input the test injects at a momentum.
type scripted struct {
	args       codec.InputArgs
	sourceData []byte
}

// wasmScript runs a real wasm.Plugin under the runtime. An input whose MomentumHash keys the
// script decodes as that scripted wasm payload from deployer; every other input decodes and
// applies as scriptDomain does.
type wasmScript struct {
	*wasm.Plugin
	pruneLog
	scripts   map[types.Hash]scripted
	storePath string
}

var scriptDeployer = types.Address{0xD0}

func newWasmScript(t *testing.T, scripts map[types.Hash]scripted) *wasmScript {
	t.Helper()
	path := t.TempDir()
	p, err := wasm.New(wasm.Config{NetworkName: "testnet", CodeStorePath: path})
	if err != nil {
		t.Fatalf("wasm.New: %v", err)
	}
	t.Cleanup(func() { p.Close() })
	return &wasmScript{Plugin: p, scripts: scripts, storePath: path}
}

func (w *wasmScript) DecodeInput(ctx domain.InputContext, payload []byte) (domain.Input, error) {
	s, ok := w.scripts[ctx.MomentumHash]
	if !ok {
		return scriptDomain{}.DecodeInput(ctx, payload)
	}
	enc, err := codec.Input{Args: s.args}.Encode()
	if err != nil {
		return nil, err
	}
	ctx.L1Sender, ctx.SourceData = scriptDeployer, s.sourceData
	return w.Plugin.DecodeInput(ctx, enc)
}

func (w *wasmScript) Apply(state domain.StateAccess, in domain.Input) (domain.Effects, error) {
	if si, ok := in.(scriptInput); ok {
		return scriptDomain{}.Apply(state, si)
	}
	return w.Plugin.Apply(state, in)
}

func (w *wasmScript) Rewind(seq uint64, fresh bool) error {
	w.logRewind(seq, fresh)
	return w.Plugin.Rewind(seq, fresh)
}

func (w *wasmScript) Prune(through uint64) error {
	if err := w.Plugin.Prune(through); err != nil {
		return err
	}
	w.logPrune(through)
	return nil
}

// farAnchor is a finalized anchor far above every input, so throughSeq is the oldest
// snapshot's LastInputSeq.
var farAnchor = fixedAnchor{recovery.Anchor{LastInputSeq: 1 << 40}, true}

// deployModule is the module of conformance vector EXEC-DEPLOY-1.
func deployModule(t *testing.T) []byte {
	t.Helper()
	data, err := os.ReadFile(filepath.Join("..", "plugins", "wasm", "testdata", "execution-conformance-v1.json"))
	if err != nil {
		t.Fatalf("read fixture: %v", err)
	}
	var f struct {
		Vectors []struct {
			ID            string `json:"id"`
			ModuleWasmHex string `json:"module_wasm_hex"`
		} `json:"vectors"`
	}
	if err := json.Unmarshal(data, &f); err != nil {
		t.Fatalf("parse fixture: %v", err)
	}
	for _, v := range f.Vectors {
		if v.ID == "EXEC-DEPLOY-1" {
			mb, err := hex.DecodeString(v.ModuleWasmHex)
			if err != nil {
				t.Fatalf("decode module_wasm_hex: %v", err)
			}
			return mb
		}
	}
	t.Fatal("EXEC-DEPLOY-1 not found in plugins/wasm/testdata/execution-conformance-v1.json")
	return nil
}

// deployScripts scripts the start at momentum startMom, the single chunk at chunkMom and the
// finalize at finalizeMom, all hashed with salt.
func deployScripts(t *testing.T, salt byte, startMom, chunkMom, finalizeMom uint64) map[types.Hash]scripted {
	t.Helper()
	module := deployModule(t)
	codeHash := wasm.CodeHash(module)
	metadata := codec.ContractMetadata{Version: codec.ContractMetadataVersion, UpgradePolicy: codec.PolicyImmutable}.Encode()
	var metadataHash types.Hash
	copy(metadataHash[:], crypto.Hash(metadata))
	return map[types.Hash]scripted{
		momHash(startMom, salt): {args: codec.DeployStartArgs{
			CodeHash: codeHash, MetadataHash: metadataHash, TotalSize: uint64(len(module)), ChunkCount: 1, Metadata: metadata,
		}},
		momHash(chunkMom, salt): {
			args:       codec.DeployChunkArgs{CodeHash: codeHash, ChunkIndex: 0, ChunkHash: wasm.CodeHash(module)},
			sourceData: module,
		},
		momHash(finalizeMom, salt): {args: codec.DeployFinalizeArgs{CodeHash: codeHash}},
	}
}

// contractID is the id a finalize at input index idx gives the deployed contract.
func contractID(deployer types.Address, idx uint64) types.Hash {
	chainID := crypto.Hash([]byte("ZENON_WASM_L2:"), []byte("testnet"))
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], idx)
	var id types.Hash
	copy(id[:], crypto.Hash(deployer[:], buf[:], chainID))
	return id
}

// storeEntries lists the files in one directory of the plugin's code store.
func (w *wasmScript) storeEntries(t *testing.T, dir string) []string {
	t.Helper()
	entries, err := os.ReadDir(filepath.Join(w.storePath, dir))
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, e := range entries {
		names = append(names, e.Name())
	}
	return names
}

// resultsByIndex returns every input's encoded ExecutionResult across the bundles on disk.
func resultsByIndex(t *testing.T, cfg Config) map[uint64][]byte {
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
	out := map[uint64][]byte{}
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
			out[in.GlobalInputIndex] = in.ExecutionResult
		}
	}
	return out
}

// --- tests ---

func TestPrunerCalls(t *testing.T) {
	t.Parallel()
	chain := buildChain(t, 10, 0)
	refCfg, _, _ := reference(t, chain, 12)
	anchor := recovery.Anchor{LastInputSeq: 6, PostStateRoot: rootAtIndex(t, refCfg, 6)}

	src := &chainSource{}
	cfg := validConfig(t, src)
	cfg.Anchors = fixedAnchor{anchor, true}
	dom := &pruneDomain{}
	l := startRun(t, cfg, src, dom)
	l.step(chain, 12)
	if err := l.stop(); err != nil {
		t.Fatalf("Run: %v", err)
	}
	oldest, ok, err := recovery.OldestSnapshotSeq(layoutOf(t, cfg))
	if err != nil || !ok {
		t.Fatalf("OldestSnapshotSeq = ok %v, err %v", ok, err)
	}
	prunes := dom.pruneCalls()
	if len(prunes) == 0 {
		t.Fatal("Prune was never called")
	}
	if got, want := prunes[len(prunes)-1], min(anchor.LastInputSeq, oldest); got != want {
		t.Fatalf("last Prune(%d), want min(anchor %d, oldest snapshot %d) = %d", got, anchor.LastInputSeq, oldest, want)
	}
	if got, want := dom.rewindCalls(), []rewindCall{{0, true}}; !reflect.DeepEqual(got, want) {
		t.Fatalf("Rewind calls = %v, want %v", got, want)
	}

	cursor, _ := cursorAt(t, cfg)
	dom2 := &pruneDomain{}
	l2 := startRun(t, cfg, src, dom2)
	l2.waitFor("a Rewind call", func() bool { return len(dom2.rewindCalls()) > 0 })
	if err := l2.stop(); err != nil {
		t.Fatalf("second Run: %v", err)
	}
	if got, want := dom2.rewindCalls()[0], (rewindCall{cursor.LastInputSeq, false}); got != want {
		t.Fatalf("first Rewind of the second run = %v, want %v", got, want)
	}

	src3 := &chainSource{}
	cfg3 := validConfig(t, src3)
	dom3 := &pruneDomain{}
	l3 := startRun(t, cfg3, src3, dom3)
	l3.step(chain, 12)
	if err := l3.stop(); err != nil {
		t.Fatalf("third Run: %v", err)
	}
	if got := dom3.pruneCalls(); len(got) != 0 {
		t.Fatalf("Prune calls without an anchor = %v, want none", got)
	}
	if got, want := dom3.rewindCalls(), []rewindCall{{0, true}}; !reflect.DeepEqual(got, want) {
		t.Fatalf("Rewind calls without an anchor = %v, want %v", got, want)
	}
}

func TestPruneThenRollbackReplaysDeployment(t *testing.T) {
	t.Parallel()
	a := buildShape(1, 12, saltFrom(1<<62, 0), dense)
	b := buildShape(1, 16, saltFrom(9, 1), dense)
	scripts := deployScripts(t, 0, 1, 2, 3)
	callScripts := map[types.Hash]scripted{momHash(12, 1): {args: codec.CallL2Args{
		TargetContract: contractID(scriptDeployer, 4), PlasmaLimit: 10_000_000,
	}}}
	for k, v := range scripts {
		callScripts[k] = v
	}

	src := &chainSource{}
	cfg := validConfig(t, src)
	cfg.Anchors = farAnchor
	ws := newWasmScript(t, callScripts)
	l := startRun(t, cfg, src, ws)
	l.step(a, 4)
	l.step(a, 8)
	l.step(a, 12)
	l.waitFor("a Prune of the finalize's index", ws.prunedAtLeast(4))
	for _, dir := range []string{"deploy-chunk", "pending-record", "consumed-deploy"} {
		if names := ws.storeEntries(t, dir); len(names) != 0 {
			t.Fatalf("%s holds %v after the prune", dir, names)
		}
	}
	if names := ws.storeEntries(t, "instrumented"); len(names) != 1 {
		t.Fatalf("instrumented holds %v, want the deployed module", names)
	}

	l.step(b, 18)
	if err := l.stop(); err != nil {
		t.Fatalf("Run: %v", err)
	}

	refSrc := &chainSource{}
	refCfg := validConfig(t, refSrc)
	refCfg.Anchors = farAnchor
	ref := startRun(t, refCfg, refSrc, newWasmScript(t, callScripts))
	ref.step(b, 18)
	if err := ref.stop(); err != nil {
		t.Fatalf("reference Run: %v", err)
	}
	refPos, _ := cursorAt(t, refCfg)
	assertMatchesReference(t, cfg, refPos, derivedInputs(t, refCfg))
	if got, want := resultsByIndex(t, cfg), resultsByIndex(t, refCfg); !reflect.DeepEqual(got, want) {
		t.Fatalf("execution results after the rollback differ from a fresh run over the final chain")
	}
}

func TestRewindKeepsLiveDeploymentAfterReorg(t *testing.T) {
	t.Parallel()
	a := buildShape(1, 8, saltFrom(1<<62, 0), dense)
	b := buildShape(1, 24, saltFrom(5, 1), func(h uint64) int {
		if h == 5 {
			return 0
		}
		return 1
	})
	scripts := deployScripts(t, 0, 1, 2, 5)
	scripts[momHash(21, 1)] = scripts[momHash(5, 0)]

	src := &chainSource{}
	cfg := validConfig(t, src)
	cfg.Anchors = farAnchor
	ws := newWasmScript(t, scripts)
	l := startRun(t, cfg, src, ws)
	l.step(a, 4)
	l.step(a, 8)
	for f := uint64(10); f <= 20; f += 2 {
		l.step(b, f)
	}
	l.waitFor("a Prune past the retracted finalize", ws.prunedAtLeast(8))
	l.step(b, 24)
	if err := l.stop(); err != nil {
		t.Fatalf("Run: %v", err)
	}

	results := resultsByIndex(t, cfg)
	var finalizeResult *codec.ExecutionResult
	var last uint64
	for idx, raw := range results {
		res, err := codec.DecodeExecutionResult(raw)
		if err != nil {
			t.Fatal(err)
		}
		if len(res.ReturnData) == 64 && (finalizeResult == nil || idx > last) {
			finalizeResult, last = &res, idx
		}
	}
	if finalizeResult == nil {
		t.Fatal("no input returned the 64 bytes of a successful finalize")
	}
	if finalizeResult.GlobalInputIndex != 39 {
		t.Fatalf("the successful finalize is input %d, want the momentum 21 finalize at input 39", finalizeResult.GlobalInputIndex)
	}
}
