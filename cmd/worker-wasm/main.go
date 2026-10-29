// Command worker-wasm is the WASM domain worker. The supervisor starts it with its channel on descriptor 3.
package main

import (
	"path/filepath"

	"github.com/Zeno-L2/zeno-executor/domain"
	"github.com/Zeno-L2/zeno-executor/plugins/wasm"
	"github.com/Zeno-L2/zeno-executor/worker"
)

func main() {
	worker.Main(func(c worker.Config) (domain.Domain, error) {
		return wasm.New(wasm.Config{NetworkName: c.NetworkName, CodeStorePath: filepath.Join(c.DomainDir(), "wasm-code")})
	}, func(c worker.SimConfig, code worker.CodeFetcher) (domain.Domain, error) {
		return wasm.New(wasm.Config{NetworkName: c.NetworkName, FetchModule: code})
	})
}
