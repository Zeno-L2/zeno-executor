package engine

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sync"

	"github.com/tetratelabs/wazero"
	"github.com/tetratelabs/wazero/api"

	"github.com/zenon-network/go-zenon/common/types"

	"github.com/digitalSloth/go-executor/plugins/wasm/internal/module"
)

// Node-local defaults. None of these is consensus-visible: they change only how often the
// engine recompiles, never what an input computes.
const (
	// defaultCacheEntries bounds the compiled-module cache by entry count.
	defaultCacheEntries = 64
	// defaultCacheBytes bounds it by summed len(instrumented). An interpreter CompiledModule
	// costs roughly 36-38x its instrumented source, so 8 MiB of source is ~300 MiB of heap.
	defaultCacheBytes = 8 << 20
	// defaultCompilesPerRebuild is how many compiles the engine performs before tearing down
	// and rebuilding its runtime. wazero interns every declared function type of every compiled
	// module into the Store for the runtime's lifetime, ~117 bytes each, and never evicts one,
	// so the Store is only reclaimable by discarding the runtime. MaxTypeCount holds a module's
	// worst-case contribution to ~240 KiB, so 512 compiles is ~125 MiB. Rebuilding discards the
	// warm cache, which costs at most defaultCacheEntries recompiles per rebuild, i.e. <= 12.5%
	// extra compilation amortized.
	defaultCompilesPerRebuild = 512
)

// cacheEntry is one compiled-module cache entry: the compiled artifact and the byte-budget
// unit it costs, len(instrumented).
type cacheEntry struct {
	compiled wazero.CompiledModule
	size     int
}

// Engine runs instrumented modules against the pinned wazero interpreter: a compiled-module
// cache keyed by CodeHash that is bounded in both entries and bytes and evicted least-recently
// used, a reclaimable wazero Store rebuilt periodically to bound its permanent per-type
// retention, the three-function env host ABI, one gas ledger shared by injected and host
// charges, seeded net of the gas table's initial-memory charge, and a deterministic Outcome
// classification.
type Engine struct {
	mu    sync.Mutex
	rt    wazero.Runtime
	env   api.Module
	cache map[types.Hash]cacheEntry
	order []types.Hash // least- to most-recently used
	bytes int          // sum of cache[h].size

	compiles int // compiles since the last runtime rebuild

	// Tuning, seeded from the defaults above by New. Fields rather than bare constants so tests
	// can exercise eviction and rebuild without building megabytes of fixtures.
	maxCacheEntries    int
	maxCacheBytes      int
	compilesPerRebuild int
}

// newRuntime builds a fresh wazero runtime with 04c's runtimeConfig (shared with deploy-time
// validation) and instantiates the env host module on it. On any error it closes the runtime
// and returns the error.
func newRuntime(ctx context.Context) (wazero.Runtime, api.Module, error) {
	rt := wazero.NewRuntimeWithConfig(ctx, runtimeConfig())

	env, err := instantiateHostModule(ctx, rt)
	if err != nil {
		_ = rt.Close(ctx)
		return nil, nil, fmt.Errorf("engine: instantiate host module: %w", err)
	}
	return rt, env, nil
}

// New builds an Engine: the pinned runtime, one instantiation of the env host module, and an
// empty cache seeded with the node-local defaults.
func New() (*Engine, error) {
	rt, env, err := newRuntime(context.Background())
	if err != nil {
		return nil, err
	}

	return &Engine{
		rt:                 rt,
		env:                env,
		cache:              make(map[types.Hash]cacheEntry),
		maxCacheEntries:    defaultCacheEntries,
		maxCacheBytes:      defaultCacheBytes,
		compilesPerRebuild: defaultCompilesPerRebuild,
	}, nil
}

// Close releases every resource the Engine owns.
func (e *Engine) Close() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.rt.Close(context.Background())
}

// rebuild tears down the current runtime and host module and replaces them with a fresh pair,
// resetting every cache and rebuild counter -- including on a newRuntime error, so no entry
// ever outlives the runtime that compiled it. Reclamation is by reachability, not by an
// explicit close cascade: runtime.CloseWithExitCode nils the Store's typeIDs map but does not
// close individual CompiledModules, and the interpreter engine's own Close is a no-op, so what
// actually frees the compiled artifacts is this function dropping e.rt and e.cache, which makes
// the interpreter engine and every decoded wasm.Module unreachable to the GC. A newRuntime
// failure is returned as a Go error (halt), leaving e.rt pointing at the closed runtime; that
// is safe because wazero's failIfClosed guards both CompileModule and InstantiateModule, so
// every subsequent Run errors cleanly rather than instantiating against a dead artifact.
//
// May only be called from Run, before instantiation.
func (e *Engine) rebuild(ctx context.Context) error {
	_ = e.rt.Close(ctx)
	e.cache = make(map[types.Hash]cacheEntry)
	e.order = nil
	e.bytes = 0
	e.compiles = 0

	rt, env, err := newRuntime(ctx)
	if err != nil {
		return err
	}
	e.rt = rt
	e.env = env
	return nil
}

// Run executes instrumented against frame, seeded with gasBudget gas, and returns a
// deterministic Outcome.
//
// Precondition: codeHash must equal SHA3-256(instrumented). On a cache hit, instrumented is
// ignored entirely and never re-verified against codeHash, so a caller passing a mismatched
// pair silently executes the cached code; re-hashing per call would defeat the cache. 04f's
// wiring is the trust boundary and should be reviewed against it.
//
// gasBudget is the whole plasma_limit. Run applies the pre-execution initial-memory charge
// itself, because it is the only code positioned to know the module's declared page count; a
// caller must not subtract that charge a second time.
//
// Run takes no context.Context: it builds its own context.Background()-derived one. A
// caller-supplied cancellable context would make execution wall-clock dependent, i.e.
// non-deterministic.
//
// Run holds the Engine's lock for its whole body: one input runs at a time, which is also what
// makes cache eviction and runtime rebuilding safe, since no instance derived from an evicted
// CompiledModule, and no instance at all, can be live between Run calls.
func (e *Engine) Run(hc HostContext, codeHash types.Hash, instrumented, frame []byte, gasBudget uint64) (Outcome, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	ctx := withHostContext(context.Background(), hc)

	budget := gasBudget
	if budget > math.MaxInt64 {
		budget = math.MaxInt64 // §10.1
	}

	if e.compiles >= e.compilesPerRebuild {
		if err := e.rebuild(ctx); err != nil {
			return Outcome{}, err
		}
	}

	compiled, err := e.compiledModule(ctx, codeHash, instrumented)
	if err != nil {
		return Outcome{}, err
	}

	// The declared memory is read from the compiled module, before instantiation: validation
	// (rule 9) guarantees the export, but Run never invokes the validator, so this is the guard
	// for a caller that reaches Run by a path that skips it. Reading it here, rather than from
	// the instantiated module below, also lets the initial-memory charge refuse an unaffordable
	// budget before wazero pays the allocation and zeroing it prices, not after.
	memDef, ok := compiled.ExportedMemories()["memory"]
	if !ok {
		return Outcome{}, fmt.Errorf("engine: module has no memory export")
	}

	// Initial-memory charge (§10/§15): applied before anything else, including instantiation.
	// Min() is exactly the page count wazero is about to allocate and zero -- nothing has run
	// yet, so no memory.grow can have moved it.
	memCost := module.InitialMemoryCost(memDef.Min())
	if budget < memCost {
		return Outcome{Kind: OutOfGas, GasUsed: budget}, nil
	}

	mod, err := e.rt.InstantiateModule(ctx, compiled, wazero.NewModuleConfig().WithName("").WithStartFunctions())
	if err != nil {
		return Outcome{}, fmt.Errorf("engine: instantiate: %w", err)
	}
	defer func() { _ = mod.Close(ctx) }()

	// mod.Memory() on a memory-less module returns a typed-nil *MemoryInstance, so
	// mod.Memory() != nil would be true and the next method call would nil-deref. ExportedMemory
	// returns a genuinely nil interface for a memory-less module. The compiled-module check
	// above already guarantees the export exists structurally; this is belt-and-suspenders
	// against the instantiated module somehow disagreeing with the compiled one.
	mem := mod.ExportedMemory("memory")
	if mem == nil {
		return Outcome{}, fmt.Errorf("engine: module has no memory export")
	}

	if err := gasSet(ctx, mod, int64(budget-memCost)); err != nil {
		return Outcome{}, err
	}

	if !mem.Write(0, frame) {
		// The frame does not fit declared memory (§27.1 => RUNTIME_FAULT). Nothing ran, so the
		// only charge paid is the initial-memory one -- exactly §15's "metered".
		return Outcome{Kind: Trapped, GasUsed: memCost}, nil
	}

	run := mod.ExportedFunction("run")
	if run == nil {
		return Outcome{}, fmt.Errorf("engine: module has no run export")
	}
	// len(frame) cannot wrap int32: a frame larger than the instance's memory (<= 4 MiB) already
	// returned above, at the Write guard.
	results, callErr := run.Call(ctx, api.EncodeI32(0), api.EncodeI32(int32(len(frame))))

	var fault *internalFault
	if errors.As(callErr, &fault) {
		return Outcome{}, fault.err
	}

	gas, err := gasGet(ctx, mod)
	if err != nil {
		return Outcome{}, err
	}

	var gasUsed uint64
	if gas < 0 {
		gasUsed = budget
	} else {
		// Includes memCost: the ledger was seeded with budget - memCost.
		gasUsed = budget - uint64(gas)
	}

	out := Outcome{Kind: classify(callErr, gas), GasUsed: gasUsed}
	switch out.Kind {
	case Aborted:
		var abortErr *abortError
		errors.As(callErr, &abortErr)
		out.AbortCode = abortErr.code
	case Returned:
		if len(results) != 1 {
			return Outcome{}, fmt.Errorf("engine: run returned %d results, want 1", len(results))
		}
		ret := int64(results[0])
		out.Ret = ret
		if ret >= 0 {
			resultPtr := uint32(uint64(ret) >> 32)
			resultLen := uint32(uint64(ret))
			// Read already bounds-checks exactly [resultPtr, resultPtr+resultLen) <= len(Buffer),
			// including the resultLen == 0 edge, and returns a subslice without allocating.
			view, ok := mem.Read(resultPtr, resultLen)
			if !ok {
				out.Kind = Trapped
				out.Ret = 0
				out.ResultBlob = nil
				break
			}
			// A copy of the whole view -- no truncation at any length (R1): ResultBlob is the
			// whole ContractEffects blob, not just ReturnData.
			out.ResultBlob = append([]byte(nil), view...)
		}
	}

	return out, nil
}

// compiledModule returns codeHash's compiled module, compiling and caching it on a miss.
func (e *Engine) compiledModule(ctx context.Context, codeHash types.Hash, instrumented []byte) (wazero.CompiledModule, error) {
	if entry, ok := e.cache[codeHash]; ok {
		e.touch(codeHash)
		return entry.compiled, nil
	}

	compiled, err := e.rt.CompileModule(ctx, instrumented)
	if err != nil {
		return nil, fmt.Errorf("engine: compile: %w", err)
	}
	e.compiles++

	size := len(instrumented)
	e.cache[codeHash] = cacheEntry{compiled: compiled, size: size}
	e.bytes += size
	e.order = append(e.order, codeHash)

	// The len(e.order) > 1 term is load-bearing, not cosmetic: it makes "the entry just
	// inserted is never evicted" structural, so a maxCacheBytes smaller than one entry degrades
	// to a cache that holds exactly one module rather than to Run instantiating a closed
	// CompiledModule.
	for len(e.order) > 1 && (len(e.order) > e.maxCacheEntries || e.bytes > e.maxCacheBytes) {
		evict := e.order[0]
		e.order = e.order[1:]
		if entry, ok := e.cache[evict]; ok {
			_ = entry.compiled.Close(ctx)
			e.bytes -= entry.size
			delete(e.cache, evict)
		}
	}

	return compiled, nil
}

// touch moves h to the back of e.order (most recently used).
func (e *Engine) touch(h types.Hash) {
	for i, o := range e.order {
		if o == h {
			e.order = append(e.order[:i], e.order[i+1:]...)
			break
		}
	}
	e.order = append(e.order, h)
}
