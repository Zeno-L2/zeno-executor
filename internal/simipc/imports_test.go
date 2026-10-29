package simipc

import (
	"os/exec"
	"strings"
	"testing"
)

func TestImportDenyList(t *testing.T) {
	const mod = "github.com/Zeno-L2/zeno-executor/"
	cmd := exec.Command("go", "list", "-deps", mod+"internal/simipc")
	var stderr strings.Builder
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("go list -deps: %v\n%s", err, stderr.String())
	}
	denied := map[string]bool{"github.com/zenon-network/go-zenon/wallet": true}
	for _, p := range []string{"internal/supervisor", "internal/ipc", "internal/batch", "runtime", "worker", "plugins/wasm", "plugins/wasm/internal/engine", "plugins/wasm/internal/module"} {
		denied[mod+p] = true
	}
	const wazero = "github.com/tetratelabs/wazero"
	sawCodec := false
	for _, line := range strings.Split(strings.TrimSpace(string(out)), "\n") {
		if line == mod+"internal/codec" {
			sawCodec = true
		}
		if denied[line] || line == wazero || strings.HasPrefix(line, wazero+"/") {
			t.Errorf("internal/simipc depends on %s", line)
		}
	}
	if !sawCodec {
		t.Errorf("go list -deps did not list internal/codec:\n%s", out)
	}
}
