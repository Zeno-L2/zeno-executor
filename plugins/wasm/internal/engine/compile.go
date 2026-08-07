package engine

import (
	"context"
	"errors"
	"fmt"

	// Bumping this pinned version can change which modules type-check, i.e. change
	// VALIDATION_FAILED vs SUCCESS, so a bump is a consensus change and requires the full
	// conformance suite to be re-run on both CI architectures.
	"github.com/tetratelabs/wazero"
	"github.com/tetratelabs/wazero/api"
)

// ErrEngineRejected is the single generic reason for a rejection by the pinned engine's own
// validator. Our named-reason taxonomy covers our rules only; the engine's is not ours to
// promise.
var ErrEngineRejected = errors.New("engine: module rejected")

// maxMemoryPages mirrors plugins/wasm.MaxMemoryPages, which this package cannot import.
const maxMemoryPages = 64

// runtimeConfig is the pinned engine configuration. Deploy-time validation (here) and
// run-time execution must use this same constructor: any divergence between the two is a
// consensus divergence.
// WithDebugInfoEnabled(false) and WithCustomSections(false) are defense-in-depth hardening for
// the DWARF/debug-info surface: they stop wazero from capturing CustomSections and running the
// Go dwarf parser over unvalidated deploy bytes. They are NOT the fix for the custom "name"
// section resource-exhaustion primitive (internal/module's Validate rejects every custom
// section outright for that reason): decoder.go's name-section handling sits outside the
// storeCustomSections/dwarfEnabled guard these knobs control, so neither knob reaches it.
func runtimeConfig() wazero.RuntimeConfig {
	return wazero.NewRuntimeConfigInterpreter().
		WithCoreFeatures(api.CoreFeaturesV1).
		WithMemoryLimitPages(maxMemoryPages).
		WithDebugInfoEnabled(false).
		WithCustomSections(false)
}

// Compile compiles wasm with the pinned interpreter configuration and discards the artifact,
// obtaining the engine's Core 1.0 structural and type checking. It resolves no imports, so a
// module importing the three env host functions compiles without a host module present.
func Compile(ctx context.Context, wasm []byte) error {
	r := wazero.NewRuntimeWithConfig(ctx, runtimeConfig())
	defer func() { _ = r.Close(ctx) }()

	mod, err := r.CompileModule(ctx, wasm)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrEngineRejected, err)
	}
	defer func() { _ = mod.Close(ctx) }()

	return nil
}
