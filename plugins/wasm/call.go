package wasm

import (
	"fmt"

	"github.com/zenon-network/go-zenon/common/types"

	"github.com/digitalSloth/go-executor/domain"
	"github.com/digitalSloth/go-executor/internal/codec"
	"github.com/digitalSloth/go-executor/plugins/wasm/internal/engine"
	"github.com/digitalSloth/go-executor/plugins/wasm/internal/keys"
	"github.com/digitalSloth/go-executor/plugins/wasm/internal/module"
)

// callArgs is the kind-0/1 intersection the execution pipeline consumes; a Deposit is a CallL2
// with an empty frame payload (§27.5).
type callArgs struct {
	TargetContract types.Hash
	PlasmaLimit    uint64
	DepositAsset   codec.AssetID
	DepositAmount  codec.U256
	Payload        []byte
}

// toCallArgs projects a kind-0 or kind-1 input onto callArgs, reporting false for every other
// kind. It is where §27.5's "for Deposit the frame payload is empty" rule lives.
func toCallArgs(args codec.InputArgs) (callArgs, bool) {
	switch a := args.(type) {
	case codec.CallL2Args:
		return callArgs{
			TargetContract: a.TargetContract,
			PlasmaLimit:    a.PlasmaLimit,
			DepositAsset:   a.DepositAsset,
			DepositAmount:  a.DepositAmount,
			Payload:        a.Payload,
		}, true
	case codec.DepositArgs:
		return callArgs{
			TargetContract: a.TargetContract,
			PlasmaLimit:    a.PlasmaLimit,
			DepositAsset:   a.DepositAsset,
			DepositAmount:  a.DepositAmount,
		}, true
	default:
		return callArgs{}, false
	}
}

// applyCall runs the §18.2/§27.1 execution pipeline for a kind-0/1 input: two unconditional
// reserved reads, deployed-target detection, module loading, frame assembly and encoding,
// engine execution, and settlement.
func (p *Plugin) applyCall(state domain.StateAccess, ctx domain.InputContext, args callArgs) (domain.Effects, error) {
	// §20: unconditional, in this order, before any code-store lookup.
	codeHashLeaf, codeHashPresent, err := state.Read(derive(args.TargetContract, keyCodeHash))
	if err != nil {
		return domain.Effects{}, fmt.Errorf("wasm: apply call: read code hash: %w", err)
	}
	preCodeHashLeaf, preCodeHashPresent, err := state.Read(derive(args.TargetContract, keyPreCodeHash))
	if err != nil {
		return domain.Effects{}, fmt.Errorf("wasm: apply call: read pre code hash: %w", err)
	}
	if codeHashPresent && len(codeHashLeaf) != len(types.Hash{}) {
		return domain.Effects{}, fmt.Errorf("wasm: apply call: code hash leaf has width %d, want %d", len(codeHashLeaf), len(types.Hash{}))
	}
	if preCodeHashPresent && len(preCodeHashLeaf) != len(types.Hash{}) {
		return domain.Effects{}, fmt.Errorf("wasm: apply call: pre code hash leaf has width %d, want %d", len(preCodeHashLeaf), len(types.Hash{}))
	}

	// Settlement rejects a plasma_limit above MaxInputGas at send validation, so an admitted
	// input never carries one. A larger budget here means this node's view differs from the
	// network's, which is a halt rather than a committed status.
	if args.PlasmaLimit > MaxInputGas {
		return domain.Effects{}, fmt.Errorf("wasm: apply call: plasma_limit %d exceeds MaxInputGas %d", args.PlasmaLimit, uint64(MaxInputGas))
	}
	gasBudget := args.PlasmaLimit
	s := settlement{
		ContractID:       args.TargetContract,
		GlobalInputIndex: ctx.GlobalInputIndex,
		L1Sender:         ctx.L1Sender,
		DepositAsset:     args.DepositAsset,
		DepositAmount:    args.DepositAmount,
		GasBudget:        gasBudget,
		State:            state,
		Caps:             ctx.Caps,
	}

	// §18.2: no code hash ⇒ no deployed target, full refund, gas_used = 0.
	if !codeHashPresent {
		return s.fail(codec.StatusValidationFailed, 0), nil
	}
	var codeHash types.Hash
	copy(codeHash[:], codeHashLeaf)
	s.CodeHash = codeHash

	var preCodeHash types.Hash
	copy(preCodeHash[:], preCodeHashLeaf)

	instrumented, err := p.loadInstrumented(codeHash, preCodeHash, preCodeHashPresent)
	if err != nil {
		return domain.Effects{}, fmt.Errorf("wasm: apply call: %w", err)
	}

	frame := buildFrame(p.chainID, ctx, args, s.CodeHash)
	encodedFrame, err := frame.Encode()
	if err != nil {
		return domain.Effects{}, fmt.Errorf("wasm: apply call: encode frame: %w", err)
	}

	out, err := p.engine.Run(engine.HostContext{State: state, ContractID: args.TargetContract}, s.CodeHash, instrumented, encodedFrame, s.GasBudget)
	if err != nil {
		return domain.Effects{}, fmt.Errorf("wasm: apply call: run: %w", err)
	}

	return s.settle(out)
}

// buildFrame assembles the §27.1 input frame a kind-0/1 invocation presents to the contract.
// Construction cannot fail — the only encodable error is the deposit asset's, raised by
// Encode — so it returns no error.
func buildFrame(chainID types.Hash, ctx domain.InputContext, args callArgs, codeHash types.Hash) codec.ExecutionInputFrame {
	return codec.ExecutionInputFrame{
		ContextVersion: codec.RuntimeProfileVersion,
		ChainID:        chainID,
		Caller:         callerHash(ctx.L1Sender),
		ContractID:     args.TargetContract,
		CodeHash:       codeHash,
		MomentumHeight: ctx.MomentumHeight,
		MomentumTime:   ctx.MomentumTime,
		InputIndex:     ctx.GlobalInputIndex,
		DepositAsset:   args.DepositAsset,
		DepositAmount:  args.DepositAmount,
		Payload:        args.Payload,
	}
}

// callerHash is the §9.2/§27.0/§14.1 L1 caller identity: SHA3-256("L1_CALLER:" || raw 20 address bytes).
func callerHash(addr types.Address) types.Hash {
	return keys.Caller(addr)
}

// loadInstrumented returns the instrumented module for codeHash, re-deriving it from the
// assembled module when the memoized tier is absent or fails hash verification.
func (p *Plugin) loadInstrumented(codeHash, preCodeHash types.Hash, prePresent bool) ([]byte, error) {
	if instrumented, ok, err := p.store.get(entryInstrumentedModule, codeHash[:]); err != nil {
		return nil, fmt.Errorf("wasm: load instrumented: %w", err)
	} else if ok && CodeHash(instrumented) == codeHash {
		return instrumented, nil
	}

	if !prePresent {
		return nil, fmt.Errorf("wasm: load instrumented: no pre code hash for code hash %x", codeHash)
	}
	assembled, ok, err := p.store.get(entryAssembledModule, preCodeHash[:])
	if err != nil {
		return nil, fmt.Errorf("wasm: load instrumented: %w", err)
	}
	if !ok || CodeHash(assembled) != preCodeHash {
		return nil, fmt.Errorf("wasm: load instrumented: assembled module for pre code hash %x is unavailable locally; recovery required", preCodeHash)
	}

	instrumented, err := module.Instrument(assembled, moduleLimits())
	if err != nil {
		return nil, fmt.Errorf("wasm: load instrumented: instrument: %w", err)
	}
	if CodeHash(instrumented) != codeHash {
		return nil, fmt.Errorf("wasm: load instrumented: re-instrumented module hash mismatch for code hash %x", codeHash)
	}
	if err := p.store.put(entryInstrumentedModule, codeHash[:], instrumented); err != nil {
		return nil, fmt.Errorf("wasm: load instrumented: %w", err)
	}
	return instrumented, nil
}
