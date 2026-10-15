package gateway

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"
)

func writeCfg(t *testing.T, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "gateway.json")
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func cfgJSON(endpoint, network string, depth int, storage string, domains string) string {
	return `{"znndEndpoint":"` + endpoint + `","networkName":"` + network + `","confirmationDepth":` + strconv.Itoa(depth) +
		`,"storagePath":"` + storage + `","domains":` + domains + `,"simulation":{"enabled":false}}`
}

func TestLoadConfig(t *testing.T) {
	tmp, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	gw, exec := filepath.Join(tmp, "gw"), filepath.Join(tmp, "exec")
	if err := os.MkdirAll(exec, 0o700); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(tmp, "link")
	if err := os.Symlink(exec, link); err != nil {
		t.Fatal(err)
	}
	execSym := filepath.Join(tmp, "execsym")
	if err := os.Symlink(exec, execSym); err != nil {
		t.Fatal(err)
	}
	domains := func(execPath string) string {
		return `[{"domainId":1,"source":"LOCAL","executorStoragePath":"` + execPath + `"}]`
	}
	const ep = "http://127.0.0.1:35997"

	cfg, err := LoadConfig(writeCfg(t, cfgJSON(ep, "test", 2, gw, domains(exec))))
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	want := Config{ZnndEndpoint: ep, NetworkName: "test", ConfirmationDepth: 2, StoragePath: gw,
		Domains: []DomainConfig{{DomainID: 1, Source: "LOCAL", ExecutorStoragePath: exec}}}
	if len(cfg.Domains) != 1 || !reflect.DeepEqual(cfg.Domains[0], want.Domains[0]) || cfg.ZnndEndpoint != want.ZnndEndpoint ||
		cfg.NetworkName != want.NetworkName || cfg.ConfirmationDepth != want.ConfirmationDepth || cfg.StoragePath != want.StoragePath {
		t.Fatalf("config = %+v, want %+v", cfg, want)
	}

	if cfg.ListenAddress != "127.0.0.1:35999" || len(cfg.AllowedOrigins) != 0 {
		t.Fatalf("defaults: listenAddress %q, allowedOrigins %q", cfg.ListenAddress, cfg.AllowedOrigins)
	}
	withNet := func(extra string) string {
		return strings.Replace(cfgJSON(ep, "test", 2, gw, domains(exec)), `{"znndEndpoint"`, `{`+extra+`,"znndEndpoint"`, 1)
	}
	for _, addr := range []string{"0.0.0.0:0", "[::1]:35999", "127.0.0.1:65535"} {
		c, err := LoadConfig(writeCfg(t, withNet(`"listenAddress":"`+addr+`"`)))
		if err != nil || c.ListenAddress != addr {
			t.Fatalf("listenAddress %q: %+v, %v", addr, c.ListenAddress, err)
		}
	}
	okOrigins := []string{"https://app.example", "http://localhost:5173", "http://[::1]:8080", "https://app.example:8443"}
	oc, err := LoadConfig(writeCfg(t, withNet(`"allowedOrigins":["https://app.example","http://localhost:5173","http://[::1]:8080","https://app.example:8443"]`)))
	if err != nil || strings.Join(oc.AllowedOrigins, ",") != strings.Join(okOrigins, ",") {
		t.Fatalf("allowedOrigins: %q, %v", oc.AllowedOrigins, err)
	}
	const hint = "use 127.0.0.1:35999 or [::1]:35999"
	for addr, subs := range map[string][]string{
		":35999":          {"has no host", hint},
		"localhost:35999": {"not an IP address", hint},
		"[fe80::1%en0]:1": {"not an IP address"},
		"127.0.0.1":       {"not host:port"},
		"127.0.0.1:65536": {"port"},
		"127.0.0.1:x":     {"port"},
	} {
		_, err := LoadConfig(writeCfg(t, withNet(`"listenAddress":"`+addr+`"`)))
		if !errors.Is(err, ErrConfig) {
			t.Fatalf("listenAddress %q: err = %v, want ErrConfig", addr, err)
		}
		for _, sub := range subs {
			if !strings.Contains(err.Error(), sub) {
				t.Fatalf("listenAddress %q: err = %q, want %q", addr, err, sub)
			}
		}
	}
	for _, o := range []string{"*", "null", "", "https://app.example/", "https://App.example", "HTTPS://app.example",
		"https://app.example:443", "http://app.example:80", "https://app.example:0443", "https://app.example:",
		"https://app.example:0", "https://user@app.example", "https://app.example?q", "https://app.example#f",
		"ftp://app.example", "https://"} {
		_, err := LoadConfig(writeCfg(t, withNet(`"allowedOrigins":["`+o+`"]`)))
		if !errors.Is(err, ErrConfig) || !strings.Contains(err.Error(), "allowedOrigins[0]") {
			t.Fatalf("allowedOrigin %q: err = %v", o, err)
		}
	}

	valid := cfgJSON(ep, "test", 2, gw, domains(exec))
	cases := []struct {
		name, body, sub string
	}{
		{"not json", "nope", ""},
		{"trailing value", valid + `{}`, "unexpected value"},
		{"unknown top-level field", strings.Replace(valid, `{"znndEndpoint"`, `{"bogus":1,"znndEndpoint"`, 1), "bogus"},
		{"unknown domain field", strings.Replace(valid, `"domainId":1`, `"mirror":[],"domainId":1`, 1), "mirror"},
		{"endpoint scheme", cfgJSON("ftp://user:secret@node", "test", 2, gw, domains(exec)), "znndEndpoint"},
		{"empty network", cfgJSON(ep, "", 2, gw, domains(exec)), "networkName"},
		{"depth 0", cfgJSON(ep, "test", 0, gw, domains(exec)), "confirmationDepth 0"},
		{"depth 1024", cfgJSON(ep, "test", 1024, gw, domains(exec)), "confirmationDepth 1024"},
		{"relative storage", cfgJSON(ep, "test", 2, "gw", domains(exec)), "storagePath"},
		{"no domains", cfgJSON(ep, "test", 2, gw, `[]`), "domains is empty"},
		{"duplicate domain", cfgJSON(ep, "test", 2, gw, `[{"domainId":1,"source":"LOCAL","executorStoragePath":"`+exec+`"},{"domainId":1,"source":"LOCAL","executorStoragePath":"`+exec+`"}]`), "already used"},
		{"remote source", cfgJSON(ep, "test", 2, gw, `[{"domainId":1,"source":"REMOTE","executorStoragePath":"`+exec+`"}]`), "executorStoragePath"},
		{"lower-case source", cfgJSON(ep, "test", 2, gw, `[{"domainId":1,"source":"remote","upstreams":["http://127.0.0.1:1"]}]`), "is not LOCAL or REMOTE"},
		{"local with upstreams", cfgJSON(ep, "test", 2, gw, `[{"domainId":1,"source":"LOCAL","executorStoragePath":"`+exec+`","upstreams":["http://127.0.0.1:1"]}]`), "upstreams is for REMOTE domains only"},
		{"remote without upstreams", cfgJSON(ep, "test", 2, gw, `[{"domainId":1,"source":"REMOTE"}]`), "upstreams must hold 1 to 8 URLs"},
		{"remote with nine upstreams", cfgJSON(ep, "test", 2, gw, `[{"domainId":1,"source":"REMOTE","upstreams":[`+strings.TrimSuffix(strings.Repeat(`"http://u:secret@127.0.0.1:1",`, 9), ",")+`]}]`), "upstreams must hold 1 to 8 URLs"},
		{"upstream with a query", cfgJSON(ep, "test", 2, gw, `[{"domainId":1,"source":"REMOTE","upstreams":["http://u:secret@127.0.0.1:1/x?a=b"]}]`), "upstreams[0] http://127.0.0.1:1 is not an absolute"},
		{"upstream with an empty query", cfgJSON(ep, "test", 2, gw, `[{"domainId":1,"source":"REMOTE","upstreams":["http://u:secret@127.0.0.1:1/x?"]}]`), "upstreams[0]"},
		{"upstream with a fragment", cfgJSON(ep, "test", 2, gw, `[{"domainId":1,"source":"REMOTE","upstreams":["http://127.0.0.1:1","http://u:secret@127.0.0.1:1/x#f"]}]`), "upstreams[1]"},
		{"upstream with another scheme", cfgJSON(ep, "test", 2, gw, `[{"domainId":1,"source":"REMOTE","upstreams":["ftp://u:secret@127.0.0.1:1"]}]`), "upstreams[0]"},
		{"upstream without a host", cfgJSON(ep, "test", 2, gw, `[{"domainId":1,"source":"REMOTE","upstreams":["http://"]}]`), "upstreams[0]"},
		{"absent executor path", cfgJSON(ep, "test", 2, gw, `[{"domainId":1,"source":"LOCAL"}]`), "executorStoragePath"},
		{"relative executor path", cfgJSON(ep, "test", 2, gw, domains("exec")), "executorStoragePath"},
		{"equal paths", cfgJSON(ep, "test", 2, exec, domains(exec)), "overlaps"},
		{"storage inside executor", cfgJSON(ep, "test", 2, filepath.Join(exec, "gw"), domains(exec)), "overlaps"},
		{"storage contains executor", cfgJSON(ep, "test", 2, tmp, domains(exec)), "overlaps"},
		{"storage is root", cfgJSON(ep, "test", 2, "/", domains(exec)), "overlaps"},
		{"storage is a symlink to executor", cfgJSON(ep, "test", 2, link, domains(exec)), "overlaps"},
		{"storage under a symlinked parent", cfgJSON(ep, "test", 2, filepath.Join(link, "gw"), domains(exec)), "overlaps"},
		{"executor is a symlink to storage", cfgJSON(ep, "test", 2, exec, domains(execSym)), "overlaps"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, err := LoadConfig(writeCfg(t, c.body))
			if !errors.Is(err, ErrConfig) {
				t.Fatalf("err = %v, want ErrConfig", err)
			}
			if !strings.Contains(err.Error(), c.sub) {
				t.Fatalf("err = %q, want it to contain %q", err, c.sub)
			}
			if strings.Contains(err.Error(), "secret") {
				t.Fatalf("err = %q leaks the endpoint userinfo", err)
			}
		})
	}

	remote := `[{"domainId":1,"source":"REMOTE","upstreams":["http://u:secret@127.0.0.1:1/gw","https://example.org"]}]`
	rc, err := LoadConfig(writeCfg(t, cfgJSON(ep, "test", 2, gw, remote)))
	if err != nil {
		t.Fatalf("REMOTE config: %v", err)
	}
	if got := rc.Domains[0]; got.Source != "REMOTE" || got.ExecutorStoragePath != "" || len(got.Upstreams) != 2 || got.Upstreams[0] != "http://u:secret@127.0.0.1:1/gw" {
		t.Fatalf("REMOTE domain = %+v", got)
	}

	if _, err := LoadConfig(filepath.Join(tmp, "missing.json")); !errors.Is(err, ErrConfig) {
		t.Fatalf("missing file: err = %v, want ErrConfig", err)
	}
}

func TestLoadConfigEightUpstreams(t *testing.T) {
	gw := filepath.Join(t.TempDir(), "gw")
	ups := strings.TrimSuffix(strings.Repeat(`"http://127.0.0.1:1/gw","https://example.org",`, 4), ",")
	body := cfgJSON("http://127.0.0.1:35997", "test", 2, gw, `[{"domainId":1,"source":"REMOTE","upstreams":[`+ups+`]}]`)
	cfg, err := LoadConfig(writeCfg(t, body))
	if err != nil || len(cfg.Domains[0].Upstreams) != 8 {
		t.Fatalf("LoadConfig with 8 upstreams: %+v, %v", cfg.Domains, err)
	}
}

// simBinary writes an executable file under dir and returns its path.
func simBinary(t *testing.T, dir string) string {
	t.Helper()
	path := filepath.Join(dir, "worker")
	if err := os.WriteFile(path, []byte("#!/bin/sh\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestLoadConfigSimulation(t *testing.T) {
	tmp, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	gw, exec := filepath.Join(tmp, "gw"), filepath.Join(tmp, "exec")
	if err := os.MkdirAll(exec, 0o700); err != nil {
		t.Fatal(err)
	}
	bin := simBinary(t, tmp)
	plain := filepath.Join(tmp, "plain")
	if err := os.WriteFile(plain, []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	const ep = "http://127.0.0.1:35997"
	domain := func(workerBinary string) string {
		wb := ""
		if workerBinary != "" {
			wb = `,"workerBinary":"` + workerBinary + `"`
		}
		return `[{"domainId":1,"source":"LOCAL","executorStoragePath":"` + exec + `"` + wb + `}]`
	}
	body := func(sim, domains string) string {
		return strings.Replace(cfgJSON(ep, "test", 2, gw, domains), `"simulation":{"enabled":false}`, `"simulation":`+sim, 1)
	}

	cfg, err := LoadConfig(writeCfg(t, body(`{}`, domain(bin))))
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	if got := cfg.Simulation; got.Enabled == nil || !*got.Enabled || got.MaxGas != 100_000_000 || got.TimeoutMs != 2_000 ||
		got.MaxConcurrent != 2 || got.UID != nil {
		t.Fatalf("defaults = %+v", got)
	}
	if cfg.Domains[0].WorkerBinary != bin {
		t.Fatalf("workerBinary = %q", cfg.Domains[0].WorkerBinary)
	}
	ok, err := LoadConfig(writeCfg(t, body(`{"maxGas":1000000000,"timeoutMs":5000,"maxConcurrent":8}`, domain(bin))))
	if err != nil || ok.Simulation.MaxGas != 1_000_000_000 || ok.Simulation.TimeoutMs != 5_000 || ok.Simulation.MaxConcurrent != 8 {
		t.Fatalf("upper bounds: %+v, %v", ok.Simulation, err)
	}
	ok, err = LoadConfig(writeCfg(t, body(`{"maxGas":1,"timeoutMs":1,"maxConcurrent":1}`, domain(bin))))
	if err != nil || ok.Simulation.MaxGas != 1 || ok.Simulation.TimeoutMs != 1 || ok.Simulation.MaxConcurrent != 1 {
		t.Fatalf("lower bounds: %+v, %v", ok.Simulation, err)
	}
	off, err := LoadConfig(writeCfg(t, body(`{"enabled":false}`, domain(""))))
	if err != nil || off.Simulation.Enabled == nil || *off.Simulation.Enabled {
		t.Fatalf("disabled without workerBinary: %+v, %v", off.Simulation, err)
	}

	cases := []struct{ name, body, sub string }{
		{"maxGas too high", body(`{"maxGas":1000000001}`, domain(bin)), "simulation.maxGas 1000000001 is outside [1, 1000000000]"},
		{"timeout too high", body(`{"timeoutMs":5001}`, domain(bin)), "simulation.timeoutMs 5001 is outside [1, 5000]"},
		{"concurrency too high", body(`{"maxConcurrent":9}`, domain(bin)), "simulation.maxConcurrent 9 is outside [1, 8]"},
		{"concurrency negative", body(`{"maxConcurrent":-1}`, domain(bin)), "simulation.maxConcurrent -1 is outside [1, 8]"},
		{"unknown member", body(`{"bogus":1}`, domain(bin)), "bogus"},
		{"binary required", body(`{}`, domain("")), "domains[0].workerBinary is required when simulation is enabled"},
		{"binary required when enabled", body(`{"enabled":true}`, domain("")), "domains[0].workerBinary is required"},
		{"relative binary", body(`{}`, domain("worker")), "domains[0].workerBinary \"worker\" is not absolute"},
		{"relative binary when disabled", body(`{"enabled":false}`, domain("worker")), "not absolute"},
		{"missing binary", body(`{}`, domain(filepath.Join(tmp, "nope"))), "domains[0].workerBinary"},
		{"directory binary", body(`{}`, domain(exec)), "is not a regular file"},
		{"not executable binary", body(`{"enabled":false}`, domain(plain)), "is not executable"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, err := LoadConfig(writeCfg(t, c.body))
			if !errors.Is(err, ErrConfig) || !strings.Contains(err.Error(), c.sub) {
				t.Fatalf("err = %v, want ErrConfig containing %q", err, c.sub)
			}
		})
	}
}

func TestLoadConfigSimulationUID(t *testing.T) {
	tmp, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	exec := filepath.Join(tmp, "exec")
	if err := os.MkdirAll(exec, 0o700); err != nil {
		t.Fatal(err)
	}
	const ep = "http://127.0.0.1:35997"
	domains := `[{"domainId":1,"source":"LOCAL","executorStoragePath":"` + exec + `"}]`
	load := func(storage string, uid int, enabled bool) error {
		b := strings.Replace(cfgJSON(ep, "test", 2, storage, domains), `"simulation":{"enabled":false}`,
			fmt.Sprintf(`"simulation":{"enabled":%t,"uid":%d}`, enabled, uid), 1)
		_, err := LoadConfig(writeCfg(t, b))
		return err
	}
	want := func(err error, sub string) {
		t.Helper()
		if !errors.Is(err, ErrConfig) || !strings.Contains(err.Error(), sub) {
			t.Fatalf("err = %v, want ErrConfig containing %q", err, sub)
		}
	}
	missing := filepath.Join(tmp, "gw")
	for _, enabled := range []bool{true, false} {
		want(load(missing, 0, enabled), "simulation.uid 0 is root; choose a dedicated uid from 1 to 9999")
	}
	want(load(missing, -1, false), "simulation.uid -1 is outside [1, 9999]")
	want(load(missing, 10000, false), "simulation.uid 10000 is outside [1, 9999]")

	t.Run("existing path", func(t *testing.T) {
		store := filepath.Join(tmp, "existing")
		if err := os.MkdirAll(store, 0o700); err != nil {
			t.Fatal(err)
		}
		owner := os.Geteuid()
		if owner == 0 {
			owner = 4321
			if err := os.Chown(store, owner, -1); err != nil {
				t.Fatal(err)
			}
		}
		if owner < 1 || owner > maxSimUID {
			t.Skipf("the test's uid %d is outside 1 to 9999 and the test is not root, so no refusable owner uid exists", owner)
		}
		want(load(store, owner, false), fmt.Sprintf("simulation.uid %d owns storagePath", owner))
		if err := load(store, owner+1, false); err != nil && strings.Contains(err.Error(), "owns storagePath") {
			t.Fatalf("a different uid is refused as the owner: %v", err)
		}
	})
	t.Run("missing path", func(t *testing.T) {
		setEuid(t, 4321)
		want(load(missing, 4321, false), "simulation.uid 4321 is the gateway's own uid, which will own storagePath")
		if err := load(missing, 4322, false); err != nil {
			t.Fatalf("uid 4322: %v", err)
		}
	})
}

// TestExampleConfigLoads decodes the compose example configuration,
// checks its container paths, then loads it with temp paths in their
// place.
func TestExampleConfigLoads(t *testing.T) {
	raw, err := os.ReadFile("../../docker/gateway.json")
	if err != nil {
		t.Fatal(err)
	}
	var m map[string]any
	if err := json.Unmarshal(raw, &m); err != nil {
		t.Fatal(err)
	}
	if m["storagePath"] != "/var/lib/gateway" {
		t.Fatalf("storagePath = %v", m["storagePath"])
	}
	if m["listenAddress"] != "0.0.0.0:35999" {
		t.Fatalf("listenAddress = %v", m["listenAddress"])
	}
	doms, ok := m["domains"].([]any)
	if !ok || len(doms) == 0 {
		t.Fatalf("domains = %v", m["domains"])
	}
	dom, ok := doms[0].(map[string]any)
	if !ok {
		t.Fatalf("domain 0 = %v", doms[0])
	}
	if dom["executorStoragePath"] != "/var/lib/executor" {
		t.Fatalf("executorStoragePath = %v", dom["executorStoragePath"])
	}
	if dom["workerBinary"] != "/usr/local/bin/worker-wasm" {
		t.Fatalf("workerBinary = %v", dom["workerBinary"])
	}
	sim, ok := m["simulation"].(map[string]any)
	if !ok {
		t.Fatalf("simulation = %v", m["simulation"])
	}
	if _, has := sim["uid"]; has {
		t.Fatal("simulation sets uid")
	}

	tmp, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	exec := filepath.Join(tmp, "exec")
	if err := os.MkdirAll(exec, 0o700); err != nil {
		t.Fatal(err)
	}
	m["storagePath"] = filepath.Join(tmp, "gw")
	dom["executorStoragePath"] = exec
	dom["workerBinary"] = simBinary(t, tmp)
	out, err := json.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	cfg, err := LoadConfig(writeCfg(t, string(out)))
	if err != nil {
		t.Fatalf("LoadConfig: %v", err)
	}
	if cfg.Simulation.MaxConcurrent != 1 || cfg.Simulation.UID != nil ||
		cfg.Simulation.Enabled == nil || !*cfg.Simulation.Enabled {
		t.Fatalf("simulation = %+v", cfg.Simulation)
	}
	if cfg.NetworkName != "devnet" {
		t.Fatalf("networkName = %q", cfg.NetworkName)
	}
	if len(cfg.Domains) != 1 || cfg.Domains[0].DomainID != 1 || cfg.Domains[0].Source != "LOCAL" {
		t.Fatalf("domains = %+v", cfg.Domains)
	}
}
