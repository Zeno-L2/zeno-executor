package supervisor

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/zenon-network/go-zenon/common/types"

	"github.com/digitalSloth/go-executor/internal/ipc"
)

func writeFile(t *testing.T, path, content string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
}

// validBinary returns a fresh, executable regular file.
func validBinary(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "worker")
	writeFile(t, path, "#!/bin/sh\n")
	if err := os.Chmod(path, 0o755); err != nil {
		t.Fatalf("Chmod: %v", err)
	}
	return path
}

// validHash is a well-formed stfSpecHash: 63 zeros then "1".
var validHash = strings.Repeat("0", 63) + "1"

// baseConfig builds the documented example configuration as a generic map, so a test can mutate
// a field to any JSON value, including one Config's static typing could not hold.
func baseConfig(storage, binary string) map[string]any {
	return map[string]any{
		"executorAddress":   types.Address{0x01}.String(),
		"networkName":       "devnet",
		"confirmationDepth": float64(10),
		"storagePath":       storage,
		"submissionKeySource": map[string]any{
			"kind": "FILE",
			"path": "/run/executor-key/key",
		},
		"workers": []any{
			map[string]any{
				"domainId":    float64(1),
				"binary":      binary,
				"role":        "EXECUTOR",
				"stfSpecHash": validHash,
				"memoryLimit": float64(1 << 30),
				"cpuLimit":    1.0,
			},
		},
	}
}

func worker0(m map[string]any) map[string]any {
	return m["workers"].([]any)[0].(map[string]any)
}

func mustJSON(t *testing.T, v any) string {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	return string(b)
}

func writeConfig(t *testing.T, m map[string]any) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "executor.json")
	writeFile(t, path, mustJSON(t, m))
	return path
}

func TestLoadConfigValid(t *testing.T) {
	storage := t.TempDir()
	binary := validBinary(t)
	path := writeConfig(t, baseConfig(storage, binary))

	cfg, err := LoadConfig(path)
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}

	wantAddr := types.Address{0x01}
	if cfg.ExecutorAddress != wantAddr {
		t.Errorf("ExecutorAddress = %v, want %v", cfg.ExecutorAddress, wantAddr)
	}
	if cfg.NetworkName != "devnet" {
		t.Errorf("NetworkName = %q, want devnet", cfg.NetworkName)
	}
	if cfg.ConfirmationDepth != 10 {
		t.Errorf("ConfirmationDepth = %d, want 10", cfg.ConfirmationDepth)
	}
	if cfg.StoragePath != storage {
		t.Errorf("StoragePath = %q, want %q", cfg.StoragePath, storage)
	}
	wantKey := KeySource{Kind: "FILE", Path: "/run/executor-key/key"}
	if cfg.SubmissionKeySource != wantKey {
		t.Errorf("SubmissionKeySource = %+v, want %+v", cfg.SubmissionKeySource, wantKey)
	}
	if len(cfg.Workers) != 1 {
		t.Fatalf("Workers = %d, want 1", len(cfg.Workers))
	}
	w := cfg.Workers[0]
	wantHash := types.HexToHashPanic(validHash)
	if w.DomainID != 1 {
		t.Errorf("DomainID = %d, want 1", w.DomainID)
	}
	if w.Binary != binary {
		t.Errorf("Binary = %q, want %q", w.Binary, binary)
	}
	if w.Role != "EXECUTOR" {
		t.Errorf("Role = %q, want EXECUTOR", w.Role)
	}
	if w.STFSpecHash != wantHash {
		t.Errorf("STFSpecHash = %v, want %v", w.STFSpecHash, wantHash)
	}
	if w.MemoryLimit != 1<<30 {
		t.Errorf("MemoryLimit = %d, want %d", w.MemoryLimit, int64(1<<30))
	}
	if w.CPULimit != 1.0 {
		t.Errorf("CPULimit = %v, want 1.0", w.CPULimit)
	}
}

// TestLoadConfigCPULimitBoundaries confirms the cpuLimit range check accepts both ends of
// [0.01, 1024] exactly, not just values well inside it.
func TestLoadConfigCPULimitBoundaries(t *testing.T) {
	storage := t.TempDir()
	binary := validBinary(t)

	for _, cpuLimit := range []float64{0.01, 1024.0} {
		t.Run(fmt.Sprintf("%v", cpuLimit), func(t *testing.T) {
			m := baseConfig(storage, binary)
			worker0(m)["cpuLimit"] = cpuLimit
			path := writeConfig(t, m)

			cfg, err := LoadConfig(path)
			if err != nil {
				t.Fatalf("LoadConfig: %v", err)
			}
			if cfg.Workers[0].CPULimit != cpuLimit {
				t.Fatalf("CPULimit = %v, want %v", cfg.Workers[0].CPULimit, cpuLimit)
			}
		})
	}
}

// TestLoadConfigBinaryMustBeAbsolute uses a relative path that resolves, from the test's working
// directory, to a real executable file: that way the rejection can only come from the
// absolute-path check itself, never from os.Stat failing to find the file. A relative path built
// from a name that does not exist would fail at os.Stat regardless of whether the absolute-path
// check ran at all.
func TestLoadConfigBinaryMustBeAbsolute(t *testing.T) {
	storage := t.TempDir()
	binary := validBinary(t)

	cwd, err := os.Getwd()
	if err != nil {
		t.Fatalf("Getwd: %v", err)
	}
	rel, err := filepath.Rel(cwd, binary)
	if err != nil {
		t.Fatalf("Rel: %v", err)
	}
	if filepath.IsAbs(rel) {
		t.Fatalf("filepath.Rel produced an absolute path: %q", rel)
	}
	if _, err := os.Stat(rel); err != nil {
		t.Fatalf("relative binary path does not resolve to a real file: %v", err)
	}

	m := baseConfig(storage, binary)
	worker0(m)["binary"] = rel
	path := writeConfig(t, m)

	_, err = LoadConfig(path)
	if !errors.Is(err, ErrConfig) {
		t.Fatalf("LoadConfig error = %v, want wrapping ErrConfig", err)
	}
	if !strings.Contains(err.Error(), "not absolute") {
		t.Fatalf("LoadConfig error = %v, want it to say not absolute", err)
	}
}

func TestConfigWorkerConfig(t *testing.T) {
	cfg := Config{
		ExecutorAddress:   types.Address{0x01},
		NetworkName:       "devnet",
		ConfirmationDepth: 10,
		StoragePath:       "/var/lib/executor",
	}
	w := WorkerConfig{
		DomainID:    7,
		Binary:      "/usr/local/bin/worker",
		Role:        "EXECUTOR",
		STFSpecHash: types.Hash{0xAA},
		MemoryLimit: 1 << 30,
		CPULimit:    1.0,
	}
	got := cfg.workerConfig(w)
	want := ipc.WorkerConfig{
		DomainID:          7,
		ExecutorAddress:   types.Address{0x01},
		STFSpecHash:       types.Hash{0xAA},
		ConfirmationDepth: 10,
		Role:              "EXECUTOR",
		NetworkName:       "devnet",
		StoragePath:       "/var/lib/executor",
	}
	if got != want {
		t.Fatalf("workerConfig = %+v, want %+v", got, want)
	}
}

func TestLoadConfigInvalid(t *testing.T) {
	storage := t.TempDir()
	binary := validBinary(t)

	otherDir := t.TempDir()
	nonExecBinary := filepath.Join(otherDir, "nonexec")
	writeFile(t, nonExecBinary, "")
	if err := os.Chmod(nonExecBinary, 0o644); err != nil {
		t.Fatalf("Chmod: %v", err)
	}

	missingDir := t.TempDir()

	base := func() map[string]any { return baseConfig(storage, binary) }

	cases := []struct {
		name    string
		path    string // set: use this literal path, writing nothing
		content string // set: write this raw content
		mutate  func(m map[string]any)
	}{
		{name: "missing file", path: filepath.Join(missingDir, "does-not-exist.json")},
		{name: "malformed JSON", content: `{"executorAddress": `},
		{name: "trailing second value", content: mustJSON(t, base()) + "\n" + mustJSON(t, map[string]any{"x": 1})},
		{name: "unknown field top level", mutate: func(m map[string]any) { m["znndEndpoint"] = "x" }},
		{name: "unknown field worker", mutate: func(m map[string]any) { worker0(m)["maxBatchInputs"] = float64(1) }},
		{name: "unknown field submissionKeySource", mutate: func(m map[string]any) {
			m["submissionKeySource"].(map[string]any)["extra"] = "x"
		}},
		{name: "executorAddress missing", mutate: func(m map[string]any) { delete(m, "executorAddress") }},
		{name: "executorAddress non-bech32", mutate: func(m map[string]any) { m["executorAddress"] = "not-an-address" }},
		{name: "networkName empty", mutate: func(m map[string]any) { m["networkName"] = "" }},
		{name: "networkName 257 bytes", mutate: func(m map[string]any) { m["networkName"] = strings.Repeat("a", 257) }},
		{name: "confirmationDepth 0", mutate: func(m map[string]any) { m["confirmationDepth"] = float64(0) }},
		{name: "confirmationDepth 1024", mutate: func(m map[string]any) { m["confirmationDepth"] = float64(1024) }},
		{name: "storagePath relative", mutate: func(m map[string]any) { m["storagePath"] = "relative/path" }},
		{name: "submissionKeySource FILE path relative", mutate: func(m map[string]any) {
			m["submissionKeySource"].(map[string]any)["path"] = "relative/key"
		}},
		{name: "storagePath 4097 bytes absolute", mutate: func(m map[string]any) {
			m["storagePath"] = "/" + strings.Repeat("a", 4096)
		}},
		{name: "workers empty", mutate: func(m map[string]any) { m["workers"] = []any{} }},
		{name: "duplicate domainId", mutate: func(m map[string]any) {
			dup := map[string]any{}
			for k, v := range worker0(m) {
				dup[k] = v
			}
			m["workers"] = append(m["workers"].([]any), dup)
		}},
		{name: "binary missing", mutate: func(m map[string]any) {
			worker0(m)["binary"] = filepath.Join(missingDir, "does-not-exist-bin")
		}},
		{name: "binary a directory", mutate: func(m map[string]any) { worker0(m)["binary"] = storage }},
		{name: "binary mode 0644", mutate: func(m map[string]any) { worker0(m)["binary"] = nonExecBinary }},
		{name: "role WATCHER", mutate: func(m map[string]any) { worker0(m)["role"] = "WATCHER" }},
		{name: "stfSpecHash missing", mutate: func(m map[string]any) { delete(worker0(m), "stfSpecHash") }},
		{name: "stfSpecHash 63 hex", mutate: func(m map[string]any) {
			worker0(m)["stfSpecHash"] = strings.Repeat("0", 63)
		}},
		{name: "domainId 2^32", mutate: func(m map[string]any) { worker0(m)["domainId"] = float64(4294967296) }},
		{name: "memoryLimit 0", mutate: func(m map[string]any) { worker0(m)["memoryLimit"] = float64(0) }},
		{name: "memoryLimit -1", mutate: func(m map[string]any) { worker0(m)["memoryLimit"] = float64(-1) }},
		{name: "memoryLimit 1.5", mutate: func(m map[string]any) { worker0(m)["memoryLimit"] = 1.5 }},
		{name: "cpuLimit 0", mutate: func(m map[string]any) { worker0(m)["cpuLimit"] = 0.0 }},
		{name: "cpuLimit -0.5", mutate: func(m map[string]any) { worker0(m)["cpuLimit"] = -0.5 }},
		{name: "cpuLimit just below min 0.009", mutate: func(m map[string]any) { worker0(m)["cpuLimit"] = 0.009 }},
		{name: "cpuLimit just above max 1024.1", mutate: func(m map[string]any) { worker0(m)["cpuLimit"] = 1024.1 }},
		{name: "cpuLimit far above max 2048", mutate: func(m map[string]any) { worker0(m)["cpuLimit"] = 2048.0 }},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var path string
			switch {
			case tc.path != "":
				path = tc.path
			case tc.content != "":
				path = filepath.Join(t.TempDir(), "cfg.json")
				writeFile(t, path, tc.content)
			default:
				m := base()
				tc.mutate(m)
				path = writeConfig(t, m)
			}

			_, err := LoadConfig(path)
			if err == nil {
				t.Fatalf("LoadConfig: want an error")
			}
			if !errors.Is(err, ErrConfig) {
				t.Fatalf("LoadConfig error = %v, want wrapping ErrConfig", err)
			}
		})
	}
}
