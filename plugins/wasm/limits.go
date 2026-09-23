package wasm

import "github.com/digitalSloth/go-executor/plugins/wasm/internal/module"

// Size and structural limits the codec package deliberately omits (§27.0: the codec enforces
// only the bounds named inside its own grammar blocks). These are the plugin's own bounds,
// enforced by the validator, the instrumentation ceiling, and the effects pipeline.
const (
	// MaxReturnData bounds ContractEffects.ReturnData (§14.3).
	MaxReturnData = 4 << 10
	// MaxExecutionResult bounds the final assembled ExecutionResult.
	MaxExecutionResult = 32 << 10
	// MaxResultBlob bounds the ContractEffects blob a contract returns, checked before the
	// blob is decoded. It keeps the decode, and the per-entry SMT key derivation that follows
	// it, proportional to a consensus constant rather than to the contract's declared memory.
	MaxResultBlob = 64 << 10
	// MaxInputGas is the ceiling on a CallL2 or Deposit plasma_limit, and so on the gas budget
	// one input can authorize. Settlement rejects a larger plasma_limit at send validation, so
	// a budget above this is one no admitted input carries.
	MaxInputGas = 1_000_000_000
	// MaxCodeSize bounds a pre-instrumentation assembled module.
	MaxCodeSize = 256 << 10
	// MaxChunkCount bounds a chunked deployment's declared chunk_count.
	MaxChunkCount = 18
	// MaxDeployChunkData bounds one deploy chunk's payload; it fits within codec's
	// MaxStateValueSize, so a chunk stored as an SMT leaf would fit one leaf.
	MaxDeployChunkData = 15 << 10
	// MaxMemoryPages bounds a module's declared and instrumented-limit memory, one WASM page
	// being 65536 bytes (64 pages = 4 MiB).
	MaxMemoryPages = 64
	// MaxStackDepth is the injected call-stack depth ceiling (§7.4), fixed and identical across
	// implementations.
	MaxStackDepth = 512
	// MaxInstrumentedCodeSize is the post-instrumentation size ceiling (§21).
	MaxInstrumentedCodeSize = 1 << 20
	// MaxLocalsPerFunction bounds params + declared locals per function (§25.1).
	MaxLocalsPerFunction = 8192
	// MaxTotalLocals bounds the sum of params + declared locals across every function in a
	// module (§25.1). A function can be declared in as few as ~7 bytes regardless of its local
	// count (a one-entry locals vector with a single run-length count), so MaxCodeSize alone
	// does not bound functions x locals: a 256 KiB module could otherwise pack tens of
	// thousands of functions each sitting at MaxLocalsPerFunction. Measured compile cost is
	// roughly 55 bytes of engine heap per local (453 MB for 1,000 functions x 8,192 locals),
	// so this cap keeps worst-case deploy-time compile heap under ~55 MB regardless of how the
	// module's functions and locals are shaped, while still comfortably admitting realistic
	// modules (thousands of functions with everyday local counts, or a much smaller number of
	// functions each at MaxLocalsPerFunction).
	MaxTotalLocals = 1 << 20
	// MaxTableEntries bounds a declared table minimum (§25.1).
	MaxTableEntries = 10000
	// MaxTotalParams bounds the sum of every declared function type's parameter count across a
	// module's type section. wazero's decode-time cache key for a function type is built with
	// O(n^2) string concatenation over its params, for every declared type whether or not any
	// function references it, so MaxLocalsPerFunction alone (checked against a function's
	// locals, not an unused type's declared params) does not bound it. Measured worst case at
	// MaxLocalsPerFunction-sized types (8 types x 8,192 params) is roughly 125ms and 808 MB of
	// compile-time allocation churn, so this cap keeps that worst case bounded regardless of
	// how the type section's params are shaped, while still comfortably admitting realistic
	// modules.
	MaxTotalParams = 1 << 16
	// MaxGlobals bounds a module's declared global count (§25.1). ~36ns of instantiation work
	// each (one heap-allocated GlobalInstance per global), forcing that cost on every
	// instantiation, i.e. every call, forever.
	MaxGlobals = 1024
	// MaxDataSegments bounds the data section's segment count (§25.1). ~16ns each (one copy per
	// active data segment applied at every instantiation).
	MaxDataSegments = 1024
	// MaxFunctions bounds the function section's count, i.e. locally defined functions (§25.1).
	// ~11ns each (one []function entry built at every instantiation).
	MaxFunctions = 8192
	// MaxTotalElementEntries bounds the sum of every element segment's function-index count
	// (§25.1). MaxTableEntries caps one table's declared minimum, not how many segments write into
	// it, so it does not bound this dimension. ~2.5ns per entry (one table write at every
	// instantiation).
	MaxTotalElementEntries = 16384
	// MaxTypeCount bounds the type section's declared function-type count (§25.1). The engine
	// interns every declared type into its Store for the runtime's lifetime and never evicts
	// one, at ~117 bytes per distinct type, so this is what bounds a module's permanent
	// contribution to node heap: ~240 KiB at this cap (the interning key is ~3 bytes per
	// parameter and MaxTotalParams already caps the section's parameters), against ~800 KiB for
	// the worst uncapped shape.
	MaxTypeCount = 1024
)

// moduleLimits is the plugin's structural caps in the shape internal/module consumes.
func moduleLimits() module.Limits {
	return module.Limits{
		MaxCodeSize:            MaxCodeSize,
		MaxMemoryPages:         MaxMemoryPages,
		MaxLocalsPerFunction:   MaxLocalsPerFunction,
		MaxTotalLocals:         MaxTotalLocals,
		MaxTableEntries:        MaxTableEntries,
		MaxTotalParams:         MaxTotalParams,
		MaxGlobals:             MaxGlobals,
		MaxDataSegments:        MaxDataSegments,
		MaxFunctions:           MaxFunctions,
		MaxTotalElementEntries: MaxTotalElementEntries,
		MaxTypeCount:           MaxTypeCount,
	}
}
