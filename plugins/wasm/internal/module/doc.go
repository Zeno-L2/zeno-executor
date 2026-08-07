// Package module is the engine-independent half of the WASM runtime: binary parsing,
// validation, the gas cost model, and deterministic instrumentation. It must never import
// wazero — that dependency belongs to internal/engine alone.
package module
