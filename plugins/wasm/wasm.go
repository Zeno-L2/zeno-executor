// Package wasm is the WASM execution domain plugin. It is a stub until the deterministic
// WebAssembly interpreter lands; it registers cleanly with the runtime but performs no
// execution.
package wasm

import (
	"errors"

	"github.com/digitalSloth/go-executor/domain"
)

// Plugin is the WASM domain.Domain implementation.
type Plugin struct{}

// New returns a WASM domain plugin.
func New() *Plugin {
	return &Plugin{}
}

// Genesis returns the empty initial state.
func (p *Plugin) Genesis() domain.StateInit {
	return domain.StateInit{}
}

// DecodeInput is not yet implemented.
func (p *Plugin) DecodeInput(payload []byte) (domain.Input, error) {
	return nil, errors.New("wasm: DecodeInput not implemented")
}

// Apply is not yet implemented.
func (p *Plugin) Apply(state domain.StateAccess, in domain.Input) (domain.Effects, error) {
	return domain.Effects{}, errors.New("wasm: Apply not implemented")
}
