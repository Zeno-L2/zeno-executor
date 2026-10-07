// Command worker-wasm is the WASM domain worker. The supervisor starts it with its channel on descriptor 3.
package main

import (
	"path/filepath"

	"github.com/digitalSloth/go-executor/domain"
	"github.com/digitalSloth/go-executor/plugins/wasm"
	"github.com/digitalSloth/go-executor/worker"
)

func main() {
	worker.Main(func(c worker.Config) (domain.Domain, error) {
		return wasm.New(wasm.Config{NetworkName: c.NetworkName, CodeStorePath: filepath.Join(c.DomainDir(), "wasm-code")})
	})
}
