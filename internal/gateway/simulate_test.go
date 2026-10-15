package gateway

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/zenon-network/go-zenon/common/crypto"
	"github.com/zenon-network/go-zenon/common/trie"
	"github.com/zenon-network/go-zenon/common/types"

	"github.com/digitalSloth/go-executor/domain"
	"github.com/digitalSloth/go-executor/internal/codec"
	"github.com/digitalSloth/go-executor/internal/ledger"
	"github.com/digitalSloth/go-executor/internal/recovery"
	"github.com/digitalSloth/go-executor/internal/simipc"
	"github.com/digitalSloth/go-executor/internal/state"
	"github.com/digitalSloth/go-executor/internal/workerproc"
	"github.com/digitalSloth/go-executor/plugins/wasm"
	wasmreader "github.com/digitalSloth/go-executor/plugins/wasm/reader"
)

var (
	simBinOnce sync.Once
	simBinDir  string
	simBinErr  error
	simBinTest string
	simBinWasm string
)

// TestMain seals inherited descriptors, as the gateway does before it
// starts a simulator, and removes the child binaries the suite built.
func TestMain(m *testing.M) {
	if err := workerproc.SealInheritedFDs(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	code := m.Run()
	if simBinDir != "" {
		os.RemoveAll(simBinDir)
	}
	os.Exit(code)
}

// simBinaries builds the testbin worker and cmd/worker-wasm once, into
// a 0755 directory, and returns the testbin worker's path. A build
// error fails the calling test.
func simBinaries(t *testing.T) string {
	t.Helper()
	simBinOnce.Do(func() {
		dir, err := os.MkdirTemp("", "gateway-simbin-")
		if err != nil {
			simBinErr = err
			return
		}
		simBinDir = dir
		if err := os.Chmod(dir, 0o755); err != nil {
			simBinErr = err
			return
		}
		simBinTest = filepath.Join(dir, "worker")
		simBinWasm = filepath.Join(dir, "worker-wasm")
		out, err := exec.Command("go", "build", "-o", dir+string(filepath.Separator),
			"github.com/digitalSloth/go-executor/internal/testbin/worker",
			"github.com/digitalSloth/go-executor/cmd/worker-wasm").CombinedOutput()
		if err != nil {
			simBinErr = fmt.Errorf("build the simulator binaries: %v\n%s", err, out)
		}
	})
	if simBinErr != nil {
		t.Fatal(simBinErr)
	}
	return simBinTest
}

// simWasmBinary returns the cmd/worker-wasm binary simBinaries built.
func simWasmBinary(t *testing.T) string {
	t.Helper()
	simBinaries(t)
	return simBinWasm
}

// simRecorder starts simulators on a zero Host and records every
// attempt.
type simRecorder struct {
	mu       sync.Mutex
	fail     bool
	attempts int
	specs    []workerproc.Spec
	procs    []*workerproc.Process
}

func (r *simRecorder) start(spec workerproc.Spec) (*workerproc.Process, error) {
	r.mu.Lock()
	r.attempts++
	r.specs = append(r.specs, spec)
	fail := r.fail
	r.mu.Unlock()
	if fail {
		return nil, errors.New("the recorder refuses to start a process")
	}
	p, err := (&workerproc.Host{}).Start(spec)
	if err == nil {
		r.mu.Lock()
		r.procs = append(r.procs, p)
		r.mu.Unlock()
	}
	return p, err
}

func (r *simRecorder) started() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.procs)
}

func (r *simRecorder) tried() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.attempts
}

func (r *simRecorder) pids() []int {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]int, len(r.procs))
	for i, p := range r.procs {
		out[i] = p.PID()
	}
	return out
}

func (r *simRecorder) recorded() []workerproc.Spec {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]workerproc.Spec(nil), r.specs...)
}

// startSims starts srv's simulator pools over the child binary bin with
// cfg, after adjust changed the server. cfg is not validated.
func startSims(t *testing.T, srv *Server, bin string, cfg SimulationConfig, adjust func(*Server)) *simRecorder {
	t.Helper()
	rec := &simRecorder{}
	startSimsWith(t, srv, bin, cfg, adjust, rec)
	return rec
}

// startAs runs srv.startSimulation as a gateway whose effective uid is
// uid, so the result does not depend on the user running the tests.
func startAs(uid int, srv *Server, enf enforcer, start func(workerproc.Spec) (*workerproc.Process, error)) error {
	restore := euid
	euid = func() int { return uid }
	defer func() { euid = restore }()
	return srv.startSimulation(enf, start)
}

func startSimsWith(t *testing.T, srv *Server, bin string, cfg SimulationConfig, adjust func(*Server), rec *simRecorder) {
	t.Helper()
	on := true
	cfg.Enabled = &on
	srv.sim = cfg.withDefaults()
	for id := range srv.chains {
		srv.simBinary[id] = bin
	}
	if adjust != nil {
		adjust(srv)
	}
	if err := startAs(1000, srv, &workerproc.Host{}, rec.start); err != nil {
		t.Fatalf("startSimulation: %v", err)
	}
	t.Cleanup(srv.stopSimulation)
}

// startDeadSims starts pools in which no process can start.
func startDeadSims(t *testing.T, srv *Server, cfg SimulationConfig, adjust func(*Server)) *simRecorder {
	t.Helper()
	rec := &simRecorder{fail: true}
	startSimsWith(t, srv, "/dead", cfg, adjust, rec)
	return rec
}

// pipeSim returns a simulator whose conn is one end of a pipe, and the
// other end, for tests that drive serve without a process.
func pipeSim(t *testing.T) (*simulator, net.Conn) {
	t.Helper()
	gw, sim := net.Pipe()
	t.Cleanup(func() { gw.Close(); sim.Close() })
	exited := make(chan struct{})
	close(exited)
	return &simulator{conn: gw, exited: exited}, sim
}

// fakeEnforcer stands in for a host; it touches no cgroup.
type fakeEnforcer struct {
	limits, uids bool
	reason       string
}

func (f fakeEnforcer) Enforcement() (bool, bool, string) { return f.limits, f.uids, f.reason }

// fdCount returns the number of open descriptors of the test process.
func fdCount(t *testing.T) int {
	t.Helper()
	entries, err := os.ReadDir("/dev/fd")
	if err != nil {
		t.Fatal(err)
	}
	return len(entries)
}

// simRig is a chain with a Settlement view and a server over it.
type simRig struct {
	srv  *Server
	sink *logSink
	cs   []codec.BatchCommitment
	set  *fakeSettlement
	r    *rig
}

const simMomentumTime = 1_700_000_000

// simChain returns history A with batch 0 accepted and finalized,
// batches 1 and 2 accepted and batches 3 and 4 pending. The fake view
// reads at height 10 and momentum time simMomentumTime. Latest is
// batch 2 (last input 6) and pending batch 4 (last input 8).
func simChain(t *testing.T) *simRig {
	t.Helper()
	dir, cs := buildA(t)
	s := newFakeSettlement()
	s.height = 10
	s.momentumTime = simMomentumTime
	s.accept(cs[0], 5)
	s.accept(cs[1], 20)
	s.accept(cs[2], 21)
	r := newIndexedChain(t, dir, s)
	srv, sink, _ := newTestServer(t, nil, r.c)
	requireTags(t, doPoll(t, r), 4, 2, 0, StatusOK)
	return &simRig{srv: srv, sink: sink, cs: cs, set: s, r: r}
}

// emptyChain returns a chain of one batch whose effects write nothing,
// accepted and finalized, so the tag's state is empty.
func emptyChain(t *testing.T) *simRig {
	t.Helper()
	dir := t.TempDir()
	cs := buildHistory(t, dir, nil, [][]fixtureInput{{{index: 1, effects: success()}}})
	s := newFakeSettlement()
	s.height = 10
	s.momentumTime = simMomentumTime
	s.accept(cs[0], 5)
	r := newIndexedChain(t, dir, s)
	srv, sink, _ := newTestServer(t, nil, r.c)
	requireTags(t, doPoll(t, r), 0, 0, 0, StatusOK)
	return &simRig{srv: srv, sink: sink, cs: cs, set: s, r: r}
}

var (
	simSender = types.Address{0x11}
	simTarget = types.Hash{0xC0}
)

// simCallJSON is a CallL2 input whose payload is cmd.
func simCallJSON(cmd string, plasma uint64) string {
	return fmt.Sprintf(`{"kind":"CallL2","sender":"%s","targetContract":"%s","payload":"%x","plasmaLimit":%d}`, simSender, simTarget, cmd, plasma)
}

// simOut decodes zeno_simulate's result.
type simOut struct {
	DomainID       uint32 `json:"domainId"`
	BatchID        uint64 `json:"batchId"`
	PostStateRoot  string `json:"postStateRoot"`
	Verified       bool   `json:"verified"`
	Simulated      bool   `json:"simulated"`
	InputIndex     uint64 `json:"inputIndex"`
	MomentumHeight uint64 `json:"momentumHeight"`
	MomentumTime   uint64 `json:"momentumTime"`
	Status         string `json:"status"`
	GasUsed        uint64 `json:"gasUsed"`
	AbortCode      int32  `json:"abortCode"`
	ContractID     string `json:"contractId"`
	CodeHash       string `json:"codeHash"`
	ReturnData     string `json:"returnData"`
	Events         []struct {
		ContractID string   `json:"contractId"`
		InputIndex uint64   `json:"inputIndex"`
		EventIndex uint32   `json:"eventIndex"`
		Topics     []string `json:"topics"`
		Data       string   `json:"data"`
	} `json:"events"`
	Outbox []struct {
		OutboxID       *string `json:"outboxId"`
		SourceContract string  `json:"sourceContract"`
		InputIndex     uint64  `json:"inputIndex"`
		OutboxIndex    uint32  `json:"outboxIndex"`
		Kind           string  `json:"kind"`
		TargetContract *string `json:"targetContract"`
		Payload        string  `json:"payload"`
	} `json:"outbox"`
	StateDiff []struct {
		Path  string  `json:"path"`
		Value *string `json:"value"`
	} `json:"stateDiff"`
}

// simParams is the params array of a zeno_simulate request on domain 1.
func simParams(input, tag string) string {
	if tag == "" {
		return fmt.Sprintf(`[1,%s]`, input)
	}
	return fmt.Sprintf(`[1,%s,%s]`, input, tag)
}

// simulateOK runs a simulation and fails the test on an error.
func simulateOK(t *testing.T, srv *Server, input, tag string) simOut {
	t.Helper()
	var out simOut
	rpcOK(t, srv, "zeno_simulate", simParams(input, tag), &out)
	return out
}

// requireSimErr requires a simulation to fail with -32006 and exactly
// msg.
func requireSimErr(t *testing.T, srv *Server, input, msg string) {
	t.Helper()
	requireRPCErr(t, srv, "zeno_simulate", simParams(input, ""), -32006, msg)
}

// eventually polls cond every 20 ms until it holds or d passes.
func eventually(t *testing.T, d time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("%s did not happen within %s", what, d)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// echoEventually retries an echo simulation until one succeeds, which
// waits out a replacement.
func echoEventually(t *testing.T, srv *Server) simOut {
	t.Helper()
	var out simOut
	eventually(t, 10*time.Second, "an echo simulation", func() bool {
		r := callRPC(t, srv, "zeno_simulate", simParams(simCallJSON("", 1000), ""))
		if r.Error != nil {
			return false
		}
		if err := json.Unmarshal(r.Result, &out); err != nil {
			t.Fatal(err)
		}
		return true
	})
	return out
}

func TestSimulateDisabled(t *testing.T) {
	srv, _, _ := newTestServer(t, nil, genesisChain(t))
	requireSimErr(t, srv, `{}`, "simulation is disabled")
	srv.stopSimulation()
}

func TestSimulateBodyBudget(t *testing.T) {
	r := simChain(t)
	startDeadSims(t, r.srv, SimulationConfig{MaxConcurrent: 1}, func(s *Server) { s.simTiming.queueWait = 50 * time.Millisecond })
	one := fmt.Sprintf(`{"jsonrpc":"2.0","method":"zeno_simulate","params":[1,%s],"id":%%d}`, simCallJSON("", 1000))
	body := "[" + fmt.Sprintf(one, 1) + "," + fmt.Sprintf(one, 2) + "," + fmt.Sprintf(one, 3) + "]"
	rec := post(t, r.srv, body)
	var resps []rpcResp
	if err := json.Unmarshal(rec.Body.Bytes(), &resps); err != nil || len(resps) != 3 {
		t.Fatalf("body = %s: %v", trunc(rec.Body.String()), err)
	}
	for i, want := range []struct {
		code int
		msg  string
	}{{-32006, "no simulator is free; retry later"}, {-32006, "no simulator is free; retry later"}, {-32600, simBudgetMsg}} {
		if resps[i].Error == nil || resps[i].Error.Code != want.code || resps[i].Error.Message != want.msg {
			t.Fatalf("response %d = %+v, want %d %q", i, resps[i].Error, want.code, want.msg)
		}
	}
}

func TestSimulateMaxGas(t *testing.T) {
	bin := simBinaries(t)
	over := `plasmaLimit 1001 exceeds simulation.maxGas 1000`
	t.Run("dead pool", func(t *testing.T) {
		r := simChain(t)
		startDeadSims(t, r.srv, SimulationConfig{MaxGas: 1000, MaxConcurrent: 1}, nil)
		requireSimErr(t, r.srv, simCallJSON("", 1001), over)
	})
	t.Run("live pool", func(t *testing.T) {
		r := simChain(t)
		startSims(t, r.srv, bin, SimulationConfig{MaxGas: 1000, MaxConcurrent: 1}, nil)
		requireSimErr(t, r.srv, simCallJSON("", 1001), over)
		if out := simulateOK(t, r.srv, simCallJSON("", 1000), ""); out.Status != "SUCCESS" {
			t.Fatalf("status = %s", out.Status)
		}
		if out := simulateOK(t, r.srv, simCallJSON("", 0), ""); out.Status != "SUCCESS" {
			t.Fatalf("plasmaLimit 0: status = %s", out.Status)
		}
	})
}

func TestSimulateSpec(t *testing.T) {
	bin := simBinaries(t)
	r := simChain(t)
	rec := startSims(t, r.srv, bin, SimulationConfig{MaxConcurrent: 2}, nil)
	eventually(t, 5*time.Second, "two simulator starts", func() bool { return rec.started() == 2 })
	names := map[string]bool{}
	for _, sp := range rec.recorded() {
		names[sp.Name] = true
		if sp.Path != bin || len(sp.Env) != 0 || sp.UID != 0 || sp.Limits != (workerproc.Limits{MemoryBytes: 2 << 30, CPUs: 1}) {
			t.Fatalf("spec = %+v", sp)
		}
	}
	if !names["sim-1-0"] || !names["sim-1-1"] || len(names) != 2 {
		t.Fatalf("names = %v", names)
	}

	seven := 7
	u, err := simUID(true, nil)
	if !errors.Is(err, ErrConfig) || err == nil || !strings.Contains(err.Error(), "simulation.uid is required when the gateway runs as root on Linux") || u != 0 {
		t.Fatalf("simUID(true, nil) = %d, %v", u, err)
	}
	for _, c := range []struct {
		uids bool
		uid  *int
		want int
	}{{true, &seven, 7}, {false, &seven, 0}, {false, nil, 0}} {
		if got, err := simUID(c.uids, c.uid); err != nil || got != c.want {
			t.Fatalf("simUID(%v, %v) = %d, %v; want %d", c.uids, c.uid, got, err, c.want)
		}
	}
	if got := stoppedErr(workerproc.Exit{OOMKilled: true}); got.Code != -32006 || got.Message != "the simulator ran out of memory" {
		t.Fatalf("stoppedErr(OOM) = %+v", got)
	}
	if got := stoppedErr(workerproc.Exit{Code: -1, Signal: syscall.SIGKILL}); got.Code != -32006 || got.Message != "the simulator stopped during the simulation; retry" {
		t.Fatalf("stoppedErr(SIGKILL) = %+v", got)
	}
	for e, want := range map[workerproc.Exit]string{
		{Code: 2}:                           "exit status 2",
		{Code: -1, Signal: syscall.SIGKILL}: "signal killed",
		{Code: -1, Signal: syscall.SIGKILL, OOMKilled: true}: "killed for memory (OOM)",
	} {
		if got := exitText(e); got != want {
			t.Fatalf("exitText(%+v) = %q, want %q", e, got, want)
		}
	}
}

func TestSimulateCgroupRefusal(t *testing.T) {
	seven := 7
	reason := "cgroup filesystem is read-only (container not privileged)"
	newRig := func(t *testing.T, uid *int, concurrent int) (*simRig, *simRecorder) {
		r := simChain(t)
		on := true
		r.srv.sim = SimulationConfig{Enabled: &on, UID: uid, MaxConcurrent: concurrent}.withDefaults()
		r.srv.simBinary[1] = "/bin/echo"
		return r, &simRecorder{fail: true}
	}

	t.Run("limits off as root", func(t *testing.T) {
		r, rec := newRig(t, &seven, 1)
		err := startAs(0, r.srv, fakeEnforcer{uids: true, reason: reason}, rec.start)
		want := "simulation needs cgroup v2 memory and cpu limits when the gateway runs as root, but they are off: " + reason +
			"; delegate both controllers to the gateway's cgroup or set simulation.enabled to false"
		if err == nil || err.Error() != want || errors.Is(err, ErrConfig) {
			t.Fatalf("err = %v, want %q not wrapping ErrConfig", err, want)
		}
		if len(rec.recorded()) != 0 || r.srv.sims != nil || r.srv.simCancel != nil {
			t.Fatalf("a pool exists after the refusal")
		}
		r.srv.stopSimulation()
		requireRPCErr(t, r.srv, "zeno_simulate", simParams(`{}`, ""), -32006, "simulation is disabled")
	})
	t.Run("limits on as root", func(t *testing.T) {
		r, rec := newRig(t, &seven, 1)
		rec.fail = false
		bin := simBinaries(t)
		r.srv.simBinary[1] = bin
		if err := startAs(0, r.srv, fakeEnforcer{limits: true, uids: true}, rec.start); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(r.srv.stopSimulation)
		eventually(t, 5*time.Second, "one simulator start", func() bool { return len(rec.recorded()) >= 1 })
		sp := rec.recorded()[0]
		if sp.UID != 7 || sp.Limits != (workerproc.Limits{MemoryBytes: 2 << 30, CPUs: 1}) {
			t.Fatalf("spec = %+v", sp)
		}
	})
	t.Run("root without uid separation", func(t *testing.T) {
		r, rec := newRig(t, nil, 1)
		err := startAs(0, r.srv, fakeEnforcer{limits: true, reason: "uids are not separated on this host"}, rec.start)
		want := "simulation needs a host that separates uids when the gateway runs as root, and this host does not; run the gateway as an unprivileged user or set simulation.enabled to false"
		if err == nil || err.Error() != want || errors.Is(err, ErrConfig) {
			t.Fatalf("err = %v, want %q not wrapping ErrConfig", err, want)
		}
		if len(rec.recorded()) != 0 || r.srv.sims != nil || r.srv.simCancel != nil {
			t.Fatalf("a pool exists after the refusal")
		}
		r.srv.stopSimulation()
		requireRPCErr(t, r.srv, "zeno_simulate", simParams(`{}`, ""), -32006, "simulation is disabled")
	})
	t.Run("unprivileged without uid separation", func(t *testing.T) {
		r, rec := newRig(t, nil, 1)
		rec.fail = false
		r.srv.simBinary[1] = simBinaries(t)
		if err := startAs(1000, r.srv, fakeEnforcer{limits: true}, rec.start); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(r.srv.stopSimulation)
		eventually(t, 5*time.Second, "one simulator start", func() bool { return len(rec.recorded()) >= 1 })
		if sp := rec.recorded()[0]; sp.UID != 0 {
			t.Fatalf("spec = %+v, want uid 0", sp)
		}
	})
	t.Run("non-root without uid separation or limits", func(t *testing.T) {
		r, rec := newRig(t, nil, 1)
		rec.fail = false
		r.srv.simBinary[1] = simBinaries(t)
		if err := startAs(501, r.srv, fakeEnforcer{}, rec.start); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(r.srv.stopSimulation)
		eventually(t, 5*time.Second, "one simulator start", func() bool { return len(rec.recorded()) >= 1 })
		if sp := rec.recorded()[0]; sp.UID != 0 {
			t.Fatalf("spec = %+v, want uid 0", sp)
		}
	})
	t.Run("uid first", func(t *testing.T) {
		r, rec := newRig(t, nil, 1)
		err := r.srv.startSimulation(fakeEnforcer{uids: true, reason: reason}, rec.start)
		if !errors.Is(err, ErrConfig) || !strings.Contains(err.Error(), "simulation.uid is required") {
			t.Fatalf("err = %v", err)
		}
	})
	t.Run("missing binary", func(t *testing.T) {
		r := simChain(t)
		second := newTestChain(t, t.TempDir(), newFakeSettlement()).c
		second.cfg.DomainID = 2
		srv, _, _ := newTestServer(t, nil, r.r.c, second)
		on := true
		srv.sim = SimulationConfig{Enabled: &on, MaxConcurrent: 1}.withDefaults()
		srv.simBinary[1] = "/bin/echo"
		rec := &simRecorder{}
		err := startAs(1000, srv, fakeEnforcer{limits: true}, rec.start)
		if !errors.Is(err, ErrConfig) || !strings.Contains(err.Error(), "workerBinary") || len(rec.recorded()) != 0 || srv.sims != nil {
			t.Fatalf("err = %v, specs %d", err, len(rec.recorded()))
		}
	})
	t.Run("simulation off", func(t *testing.T) {
		r := simChain(t)
		rec := &simRecorder{}
		if err := r.srv.startSimulation(fakeEnforcer{uids: true, reason: reason}, rec.start); err != nil || r.srv.sims != nil {
			t.Fatalf("err = %v, sims = %v", err, r.srv.sims)
		}
	})
}

func TestSimulateInputs(t *testing.T) {
	bin := simBinaries(t)
	r := simChain(t)
	startSims(t, r.srv, bin, SimulationConfig{MaxConcurrent: 1}, nil)
	pathArg := func(p trie.Path) string { return "read:" + pathHex(p) }

	echoWant, err := codec.Input{Args: codec.CallL2Args{TargetContract: simTarget, PlasmaLimit: 1000, Payload: []byte{}}}.Encode()
	if err != nil {
		t.Fatal(err)
	}
	out := simulateOK(t, r.srv, simCallJSON("", 1000), `"latest"`)
	if out.DomainID != 1 || out.BatchID != 2 || out.PostStateRoot != r.cs[2].PostStateRoot.String() || out.Verified || !out.Simulated ||
		out.InputIndex != 9 || out.MomentumHeight != 10 || out.MomentumTime != simMomentumTime || out.Status != "SUCCESS" ||
		out.ContractID != simTarget.String() || out.ReturnData != fmt.Sprintf("%x", "echo") {
		t.Fatalf("result = %+v", out)
	}
	if len(out.StateDiff) != 1 || out.StateDiff[0].Value == nil || *out.StateDiff[0].Value != fmt.Sprintf("%x", echoWant) {
		t.Fatalf("stateDiff = %+v, want the input %x", out.StateDiff, echoWant)
	}
	wantData := fmt.Sprintf("%x%x%x%x", be64(9), be64(10), be64(simMomentumTime), simSender[:])
	if len(out.Events) != 1 || out.Events[0].Data != wantData || out.Events[0].InputIndex != 9 || out.Events[0].Topics == nil {
		t.Fatalf("events = %+v, want data %s", out.Events, wantData)
	}
	if len(out.Outbox) != 1 || out.Outbox[0].OutboxID != nil || out.Outbox[0].Kind != "L2_DELIVERY" || out.Outbox[0].Payload != fmt.Sprintf("%x", "echo") ||
		out.Outbox[0].TargetContract == nil || *out.Outbox[0].TargetContract != simTarget.String() {
		t.Fatalf("outbox = %+v", out.Outbox)
	}

	for _, tag := range []string{"", `null`, `"pending"`, `2`, `1`} {
		if got := simulateOK(t, r.srv, simCallJSON("", 1), tag); got.InputIndex != 9 {
			t.Fatalf("tag %q: inputIndex = %d, want 9 whatever the tag", tag, got.InputIndex)
		}
	}
	if got := simulateOK(t, r.srv, simCallJSON("", 1), `"pending"`); got.BatchID != 4 || got.PostStateRoot != r.cs[4].PostStateRoot.String() {
		t.Fatalf("pending: batch %d root %s", got.BatchID, got.PostStateRoot)
	}

	dep := fmt.Sprintf(`{"kind":"Deposit","sender":"%s","targetContract":"%s","deposit":{"asset":"%s","amount":"5"},"plasmaLimit":77}`, simSender, simTarget, types.Hash(fixtureAsset))
	depWant, err := codec.Input{Args: codec.DepositArgs{TargetContract: simTarget, PlasmaLimit: 77, DepositAsset: fixtureAsset, DepositAmount: codec.U256{31: 5}}}.Encode()
	if err != nil {
		t.Fatal(err)
	}
	d := simulateOK(t, r.srv, dep, `"latest"`)
	if d.Status != "SUCCESS" || len(d.StateDiff) != 1 || *d.StateDiff[0].Value != fmt.Sprintf("%x", depWant) || d.InputIndex != 9 {
		t.Fatalf("deposit result = %+v, want the input %x", d, depWant)
	}

	value := func(path trie.Path, tag string) (present bool, v string) {
		t.Helper()
		o := simulateOK(t, r.srv, simCallJSON(pathArg(path), 1000), tag)
		if len(o.StateDiff) != 1 || o.StateDiff[0].Path != pathHex(path) {
			t.Fatalf("read of %x at %s: diff %+v", path[:2], tag, o.StateDiff)
		}
		if o.StateDiff[0].Value == nil {
			return false, ""
		}
		return true, *o.StateDiff[0].Value
	}
	if p, v := value(pA, `"latest"`); !p || v != fmt.Sprintf("%x", "a2") {
		t.Fatalf("pA at latest = %v %q", p, v)
	}
	if p, v := value(pA, `"finalized"`); !p || v != fmt.Sprintf("%x", "a1") {
		t.Fatalf("pA at finalized = %v %q", p, v)
	}
	if p, v := value(pB, `"finalized"`); !p || v != "" {
		t.Fatalf("pB at finalized = %v %q, want present and empty", p, v)
	}
	if p, _ := value(pC, `"latest"`); p {
		t.Fatalf("pC at latest is present")
	}
}

func TestSimulateInputErrors(t *testing.T) {
	r := simChain(t)
	startDeadSims(t, r.srv, SimulationConfig{MaxConcurrent: 1}, func(s *Server) { s.simTiming.queueWait = 50 * time.Millisecond })
	obj := "invalid params: input must be an object with members kind, sender, targetContract, payload, deposit, plasmaLimit"
	kindMsg := `invalid params: input.kind must be "CallL2" or "Deposit"`
	send, tgt := simSender.String(), simTarget.String()
	call := func(extra string) string {
		return fmt.Sprintf(`{"kind":"CallL2","sender":"%s","targetContract":"%s","plasmaLimit":1%s}`, send, tgt, extra)
	}
	badAmount := "invalid params: input.deposit.amount must be a decimal string below 2^256"
	asset := types.Hash(fixtureAsset).String()
	dep := func(amount string) string {
		return call(fmt.Sprintf(`,"deposit":{"asset":"%s","amount":%s}`, asset, amount))
	}
	var noncanon types.Hash
	noncanon[0] = 1
	cases := []struct{ name, input, msg string }{
		{"null", `null`, obj},
		{"array", `[]`, obj},
		{"string", `"x"`, obj},
		{"unknown member", call(`,"extra":1`), obj},
		{"repeated member", `{"kind":"CallL2","kind":"CallL2"}`, obj},
		{"member in another case", `{"Kind":"CallL2"}`, obj},
		{"kind missing", `{}`, "invalid params: input.kind is required"},
		{"kind null", `{"kind":null}`, "invalid params: input.kind is required"},
		{"kind lower case", strings.Replace(call(""), `"CallL2"`, `"callL2"`, 1), kindMsg},
		{"kind integer 0", strings.Replace(call(""), `"CallL2"`, `0`, 1), kindMsg},
		{"kind integer 2", strings.Replace(call(""), `"CallL2"`, `2`, 1), kindMsg},
		{"kind integer 6", strings.Replace(call(""), `"CallL2"`, `6`, 1), kindMsg},
		{"kind integer 7", strings.Replace(call(""), `"CallL2"`, `7`, 1), kindMsg},
		{"kind deploy", strings.Replace(call(""), `"CallL2"`, `"DeployContractStart"`, 1), kindMsg},
		{"kind transfer", strings.Replace(call(""), `"CallL2"`, `"TransferContractOwnership"`, 1), kindMsg},
		{"kind buy storage", strings.Replace(call(""), `"CallL2"`, `"BuyStorage"`, 1), kindMsg},
		{"sender missing", `{"kind":"CallL2"}`, "invalid params: input.sender is required"},
		{"sender bad", strings.Replace(call(""), send, "z1bad", 1), "invalid params: input.sender must be an L1 address"},
		{"target missing", fmt.Sprintf(`{"kind":"CallL2","sender":"%s"}`, send), "invalid params: input.targetContract is required"},
		{"target bad", strings.Replace(call(""), tgt, "00", 1), "invalid params: input.targetContract must be 64 hex characters"},
		{"payload odd hex", call(`,"payload":"abc"`), "invalid params: input.payload must be hex of at most 8192 bytes"},
		{"payload too long", call(`,"payload":"` + strings.Repeat("00", 8193) + `"`), "invalid params: input.payload must be hex of at most 8192 bytes"},
		{"payload on deposit", strings.Replace(call(`,"payload":""`), `"CallL2"`, `"Deposit"`, 1), "invalid params: input.payload is for CallL2 only"},
		{"deposit not an object", call(`,"deposit":5`), "invalid params: input.deposit must be an object with members asset, amount"},
		{"deposit without amount", call(fmt.Sprintf(`,"deposit":{"asset":"%s"}`, asset)), "invalid params: input.deposit must be an object with members asset, amount"},
		{"deposit extra member", call(fmt.Sprintf(`,"deposit":{"asset":"%s","amount":"1","x":1}`, asset)), "invalid params: input.deposit must be an object with members asset, amount"},
		{"deposit asset bad", call(`,"deposit":{"asset":"1","amount":"1"}`), "invalid params: input.deposit.asset must be 64 hex characters"},
		{"amount 007", dep(`"007"`), badAmount},
		{"amount negative", dep(`"-1"`), badAmount},
		{"amount empty", dep(`""`), badAmount},
		{"amount exponent", dep(`"1e3"`), badAmount},
		{"amount number", dep(`5`), badAmount},
		{"amount 2^256", dep(`"115792089237316195423570985008687907853269984665640564039457584007913129639936"`), badAmount},
		{"asset not canonical", call(fmt.Sprintf(`,"deposit":{"asset":"%s","amount":"1"}`, noncanon)), "invalid params: input does not encode:"},
		{"plasmaLimit missing", fmt.Sprintf(`{"kind":"CallL2","sender":"%s","targetContract":"%s"}`, send, tgt), "invalid params: input.plasmaLimit is required"},
		{"plasmaLimit negative", strings.Replace(call(""), `"plasmaLimit":1`, `"plasmaLimit":-1`, 1), "invalid params: input.plasmaLimit must be an integer from 0 to 18446744073709551615"},
		{"plasmaLimit string", strings.Replace(call(""), `"plasmaLimit":1`, `"plasmaLimit":"1"`, 1), "invalid params: input.plasmaLimit must be an integer from 0 to 18446744073709551615"},
		{"plasmaLimit fraction", strings.Replace(call(""), `"plasmaLimit":1`, `"plasmaLimit":1.5`, 1), "invalid params: input.plasmaLimit must be an integer from 0 to 18446744073709551615"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			e := rpcErr(t, r.srv, "zeno_simulate", simParams(c.input, ""))
			if e.Code != -32602 || !strings.HasPrefix(e.Message, c.msg) {
				t.Fatalf("error = %d %q, want -32602 %q", e.Code, e.Message, c.msg)
			}
		})
	}
	requireRPCErr(t, r.srv, "zeno_simulate", simParams(call(""), `"bogus"`), -32602, "")
	requireRPCErr(t, r.srv, "zeno_simulate", simParams(call(""), `"finalized"`), -32006, "no simulator is free; retry later")
	requireRPCErr(t, r.srv, "zeno_simulate", simParams(call(""), `9`), -32004, "")
}

// simAsync runs a simulation on its own goroutine.
func simAsync(srv *Server, input string) <-chan rpcResp {
	out := make(chan rpcResp, 1)
	go func() {
		body := fmt.Sprintf(`{"jsonrpc":"2.0","method":"zeno_simulate","params":%s,"id":1}`, simParams(input, ""))
		req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		rec := httptest.NewRecorder()
		srv.ServeHTTP(rec, req)
		var r rpcResp
		_ = json.Unmarshal(rec.Body.Bytes(), &r)
		out <- r
	}()
	return out
}

func waitResp(t *testing.T, c <-chan rpcResp, d time.Duration) rpcResp {
	t.Helper()
	select {
	case r := <-c:
		return r
	case <-time.After(d):
		t.Fatalf("no response within %s", d)
		return rpcResp{}
	}
}

const stoppedMsg = "the simulator stopped during the simulation; retry"

func TestSimulateKilled(t *testing.T) {
	bin := simBinaries(t)
	r := simChain(t)
	rec := startSims(t, r.srv, bin, SimulationConfig{MaxConcurrent: 1}, nil)
	echoEventually(t, r.srv)
	first := rec.pids()
	if len(first) != 1 {
		t.Fatalf("started %d simulators, want 1", len(first))
	}
	res := simAsync(r.srv, simCallJSON("sleep:10000", 1000))
	time.Sleep(300 * time.Millisecond)
	if err := syscall.Kill(first[0], syscall.SIGKILL); err != nil {
		t.Fatal(err)
	}
	got := waitResp(t, res, 5*time.Second)
	if got.Error == nil || got.Error.Code != -32006 || got.Error.Message != stoppedMsg {
		t.Fatalf("response = %+v, want -32006 %q", got.Error, stoppedMsg)
	}
	if out := echoEventually(t, r.srv); out.Status != "SUCCESS" {
		t.Fatalf("status = %s", out.Status)
	}
	if pids := rec.pids(); len(pids) != 2 || pids[1] == first[0] {
		t.Fatalf("pids = %v, want a replacement process", pids)
	}
}

func TestSimulateTimeout(t *testing.T) {
	bin := simBinaries(t)
	r := simChain(t)
	rec := startSims(t, r.srv, bin, SimulationConfig{MaxConcurrent: 1, TimeoutMs: 200}, nil)
	echoEventually(t, r.srv)
	start := time.Now()
	requireSimErr(t, r.srv, simCallJSON("sleep:2000", 1000), "simulation exceeded timeoutMs (200 ms)")
	if d := time.Since(start); d > 2*time.Second {
		t.Fatalf("the timeout answered after %s", d)
	}
	echoEventually(t, r.srv)
	if rec.started() != 2 {
		t.Fatalf("started %d simulators, want a replacement (2)", rec.started())
	}
	if r.sink.count("was replaced") != 0 {
		t.Fatalf("a timeout logged a replacement line:\n%s", r.sink.dump())
	}
}

func TestSimulateFailedKeepsProcess(t *testing.T) {
	bin := simBinaries(t)
	r := simChain(t)
	rec := startSims(t, r.srv, bin, SimulationConfig{MaxConcurrent: 1}, nil)
	echoEventually(t, r.srv)
	requireSimErr(t, r.srv, simCallJSON("fail", 1000), "the simulator could not run the input: testbin simulator: fail")
	if out := simulateOK(t, r.srv, simCallJSON("", 1000), ""); out.Status != "SUCCESS" || rec.started() != 1 {
		t.Fatalf("status %s after fail, %d starts", out.Status, rec.started())
	}
	rpcRec := post(t, r.srv, fmt.Sprintf(`{"jsonrpc":"2.0","method":"zeno_simulate","params":%s,"id":1}`, simParams(simCallJSON("badutf8", 1000), "")))
	if !utf8.Valid(rpcRec.Body.Bytes()) {
		t.Fatalf("the response body is not valid UTF-8: %q", rpcRec.Body.String())
	}
	var resp rpcResp
	if err := json.Unmarshal(rpcRec.Body.Bytes(), &resp); err != nil || resp.Error == nil || resp.Error.Code != -32006 ||
		!strings.HasPrefix(resp.Error.Message, "the simulator could not run the input: ") || !strings.HasSuffix(resp.Error.Message, "\uFFFD") {
		t.Fatalf("badutf8 response = %s", rpcRec.Body.String())
	}
	if simulateOK(t, r.srv, simCallJSON("", 1000), "").Status != "SUCCESS" || rec.started() != 1 {
		t.Fatalf("the simulator was replaced after Failed (%d starts)", rec.started())
	}
	if dump := r.sink.dump(); strings.Contains(dump, "testbin simulator: fail") || strings.Contains(dump, "bad text") {
		t.Fatalf("a Failed text reached the log:\n%s", dump)
	}
}

// codeReader is a reader that serves code through fn.
type codeReader struct {
	domain.Reader
	fn func(rc domain.ReadContext, h types.Hash) ([]byte, error)
}

func (c codeReader) Code(rc domain.ReadContext, h types.Hash) ([]byte, error) { return c.fn(rc, h) }

func TestSimulateProtocol(t *testing.T) {
	bin := simBinaries(t)
	r := simChain(t)
	r.srv.readers = map[uint8]domain.Reader{0: codeReader{fn: func(domain.ReadContext, types.Hash) ([]byte, error) {
		return nil, domain.ErrNotFound
	}}}
	rec := startSims(t, r.srv, bin, SimulationConfig{MaxConcurrent: 1}, nil)
	echoEventually(t, r.srv)
	asset := types.Hash{0xAA}.String()
	for i, cmd := range []string{"code2:" + asset, "caps2:" + asset} {
		requireSimErr(t, r.srv, simCallJSON(cmd, 1000), stoppedMsg)
		echoEventually(t, r.srv)
		if rec.started() != i+2 {
			t.Fatalf("after %q: %d starts, want %d", cmd, rec.started(), i+2)
		}
	}
	// A single request of each kind is served.
	if out := simulateOK(t, r.srv, simCallJSON("code:"+asset, 1000), ""); out.ReturnData != fmt.Sprintf("%x", "notfound") {
		t.Fatalf("code: returned %q", out.ReturnData)
	}
}

func TestSimulateCaps(t *testing.T) {
	bin := simBinaries(t)
	asset, other := types.Hash{0xA1}, types.Hash{0xA2}
	cap5 := new(big.Int).SetUint64(5)
	setup := func(t *testing.T, cfg SimulationConfig, adjust func(*Server)) (*simRig, *simRecorder) {
		r := simChain(t)
		r.set.caps = map[types.Hash]*big.Int{asset: cap5}
		return r, startSims(t, r.srv, bin, cfg, adjust)
	}

	t.Run("amounts", func(t *testing.T) {
		r, _ := setup(t, SimulationConfig{MaxConcurrent: 1}, nil)
		want := make([]byte, 32)
		want[31] = 5
		if out := simulateOK(t, r.srv, simCallJSON("caps:"+asset.String(), 1000), ""); out.ReturnData != fmt.Sprintf("%x", want) {
			t.Fatalf("cap = %s", out.ReturnData)
		}
		if out := simulateOK(t, r.srv, simCallJSON("caps:"+other.String(), 1000), ""); out.ReturnData != fmt.Sprintf("%x", make([]byte, 32)) {
			t.Fatalf("cap of an asset with no config = %s", out.ReturnData)
		}
	})
	t.Run("node error", func(t *testing.T) {
		r, rec := setup(t, SimulationConfig{MaxConcurrent: 1}, nil)
		r.set.capsErr = &url.Error{Op: "Post", URL: r.srv.endpoint, Err: errors.New("connection refused")}
		requireSimErr(t, r.srv, simCallJSON("caps:"+asset.String(), 1000), "the simulator could not run the input: withdrawal caps are unavailable")
		if r.sink.count("AssetConfig") != 1 || strings.Contains(r.sink.dump(), "secret") {
			t.Fatalf("want one log line naming AssetConfig and no secret:\n%s", r.sink.dump())
		}
		r.set.capsErr = nil
		simulateOK(t, r.srv, simCallJSON("", 1000), "")
		if rec.started() != 1 {
			t.Fatalf("%d starts, want the simulator kept", rec.started())
		}
	})
	t.Run("hanging node", func(t *testing.T) {
		r, rec := setup(t, SimulationConfig{MaxConcurrent: 1, TimeoutMs: 1000}, nil)
		r.set.capsHang = make(chan struct{})
		t.Cleanup(func() { close(r.set.capsHang) })
		start := time.Now()
		requireSimErr(t, r.srv, simCallJSON("caps:"+asset.String(), 1000), "the simulator could not run the input: withdrawal caps are unavailable")
		if d := time.Since(start); d > 2*time.Second {
			t.Fatalf("the hanging read answered after %s", d)
		}
		simulateOK(t, r.srv, simCallJSON("", 1000), "")
		if rec.started() != 1 || r.sink.count("was replaced") != 0 {
			t.Fatalf("%d starts; log:\n%s", rec.started(), r.sink.dump())
		}
	})
	t.Run("abandoned reads keep their slot", func(t *testing.T) {
		r, _ := setup(t, SimulationConfig{MaxConcurrent: 1, TimeoutMs: 300}, func(s *Server) { s.simCapsSem = make(chan struct{}, 1) })
		r.set.capsHang = make(chan struct{})
		t.Cleanup(func() { close(r.set.capsHang) })
		for i := 0; i < 2; i++ {
			requireSimErr(t, r.srv, simCallJSON("caps:"+asset.String(), 1000), "the simulator could not run the input: withdrawal caps are unavailable")
		}
		r.set.mu.Lock()
		calls := r.set.capsCalls
		r.set.mu.Unlock()
		if calls != 1 {
			t.Fatalf("capsCalls = %d, want 1: the first read still holds the only slot", calls)
		}
	})
}

// simRequest is a Simulate whose input is an echo.
func simRequest(t *testing.T) simipc.Simulate {
	t.Helper()
	data, err := codec.Input{Args: codec.CallL2Args{TargetContract: simTarget, PlasmaLimit: 1, Payload: []byte{}}}.Encode()
	if err != nil {
		t.Fatal(err)
	}
	return simipc.Simulate{GlobalInputIndex: 9, MomentumHeight: 10, MomentumTime: simMomentumTime, L1Sender: simSender, InputData: data}
}

// pipePool returns a pool whose serve runs on a pipe, and the server's
// log.
func pipePool(t *testing.T, timeout time.Duration) (*simPool, *logSink) {
	t.Helper()
	srv, sink, _ := newTestServer(t, nil, genesisChain(t))
	return &simPool{s: srv, domainID: 1, timeout: timeout, ctx: context.Background()}, sink
}

type serveResult struct {
	out     simOutcome
	healthy bool
	cause   error
}

func serveAsync(p *simPool, m *simulator, j *simJob) <-chan serveResult {
	c := make(chan serveResult, 1)
	go func() {
		out, healthy, cause := p.serve(m, j)
		c <- serveResult{out, healthy, cause}
	}()
	return c
}

func TestSimulateHostileFrames(t *testing.T) {
	p, _ := pipePool(t, 30*time.Second)
	m, end := pipeSim(t)
	j := &simJob{req: simRequest(t), done: make(chan simOutcome, 1)}
	res := serveAsync(p, m, j)
	if _, err := simipc.ReadFromGateway(end); err != nil {
		t.Fatal(err)
	}
	if _, err := end.Write(binary.BigEndian.AppendUint32(nil, simipc.MaxSimulatorFrame+1)); err != nil {
		t.Fatal(err)
	}
	select {
	case got := <-res:
		if got.healthy || got.out.err == nil || got.out.err.Message != stoppedMsg || !errors.Is(got.cause, simipc.ErrMalformed) {
			t.Fatalf("serve = %+v", got)
		}
	case <-time.After(time.Second):
		t.Fatal("serve waited for the body of an over-cap frame")
	}
}

type panicConn struct{ net.Conn }

func (panicConn) Write([]byte) (int, error) { panic("write panicked") }

func TestSimulateServePanic(t *testing.T) {
	p, sink := pipePool(t, 30*time.Second)
	m, _ := pipeSim(t)
	m.conn = panicConn{m.conn}
	j := &simJob{req: simRequest(t), done: make(chan simOutcome, 1)}
	out, healthy, cause := p.serve(m, j)
	if healthy || out.err == nil || out.err.Code != -32603 || out.err.Message != "internal error" || cause == nil || !strings.Contains(cause.Error(), "serve panicked") {
		t.Fatalf("serve = %+v, healthy %v, cause %v", out, healthy, cause)
	}
	if sink.count("serve panicked") != 1 {
		t.Fatalf("log:\n%s", sink.dump())
	}
}

func TestSimulateCapsDeadline(t *testing.T) {
	assets := make([]codec.AssetID, 16)
	for i := range assets {
		assets[i] = codec.AssetID{31: byte(i + 1)}
	}
	run := func(t *testing.T, timeout time.Duration) (time.Duration, int) {
		p, _ := pipePool(t, timeout)
		set := newFakeSettlement()
		set.capsHang = make(chan struct{})
		t.Cleanup(func() { close(set.capsHang) })
		m, end := pipeSim(t)
		j := &simJob{req: simRequest(t), view: fakeView{set}, done: make(chan simOutcome, 1)}
		res := serveAsync(p, m, j)
		if _, err := simipc.ReadFromGateway(end); err != nil {
			t.Fatal(err)
		}
		start := time.Now()
		if err := simipc.WriteMessage(end, simipc.CapRequest{Assets: assets}); err != nil {
			t.Fatal(err)
		}
		reply, err := simipc.ReadFromGateway(end)
		if err != nil {
			t.Fatal(err)
		}
		took := time.Since(start)
		if _, ok := reply.(simipc.Unavailable); !ok {
			t.Fatalf("reply = %T, want Unavailable", reply)
		}
		if err := simipc.WriteMessage(end, simipc.Failed{Message: "done"}); err != nil {
			t.Fatal(err)
		}
		if got := <-res; !got.healthy {
			t.Fatalf("serve = %+v, want a healthy simulator", got)
		}
		// A read started after the caps deadline runs on its own
		// goroutine, so the count is read after it had time to start.
		time.Sleep(150 * time.Millisecond)
		set.mu.Lock()
		defer set.mu.Unlock()
		return took, set.capsCalls
	}
	t.Run("300 ms", func(t *testing.T) {
		took, calls := run(t, 300*time.Millisecond)
		if took >= 300*time.Millisecond || calls != 1 {
			t.Fatalf("Unavailable after %s with %d calls, want before 300 ms and 1 call", took, calls)
		}
	})
	t.Run("50 ms", func(t *testing.T) {
		took, calls := run(t, 50*time.Millisecond)
		if took >= 50*time.Millisecond || calls != 0 {
			t.Fatalf("Unavailable after %s with %d calls, want at once and none", took, calls)
		}
	})
}

func TestSimulateCapsOrder(t *testing.T) {
	p, _ := pipePool(t, 5*time.Second)
	set := newFakeSettlement()
	a, b, none := types.Hash{0xA1}, types.Hash{0xA2}, types.Hash{0xA3}
	set.caps = map[types.Hash]*big.Int{a: big.NewInt(5), b: big.NewInt(7)}
	m, end := pipeSim(t)
	j := &simJob{req: simRequest(t), view: fakeView{set}, done: make(chan simOutcome, 1)}
	res := serveAsync(p, m, j)
	if _, err := simipc.ReadFromGateway(end); err != nil {
		t.Fatal(err)
	}
	if err := simipc.WriteMessage(end, simipc.CapRequest{Assets: []codec.AssetID{codec.AssetID(b), codec.AssetID(none), codec.AssetID(a)}}); err != nil {
		t.Fatal(err)
	}
	reply, err := simipc.ReadFromGateway(end)
	if err != nil {
		t.Fatal(err)
	}
	caps, ok := reply.(simipc.Caps)
	if !ok || len(caps.Amounts) != 3 || caps.Amounts[0] != (codec.U256{31: 7}) || caps.Amounts[1] != (codec.U256{}) || caps.Amounts[2] != (codec.U256{31: 5}) {
		t.Fatalf("reply = %+v, want the caps 7, 0, 5 in request order", reply)
	}
	if err := simipc.WriteMessage(end, simipc.Failed{Message: "done"}); err != nil {
		t.Fatal(err)
	}
	if got := <-res; !got.healthy {
		t.Fatalf("serve = %+v", got)
	}
}

func TestSimulateLaunchCleanup(t *testing.T) {
	bin := simBinaries(t)
	t.Run("fileConn fails", func(t *testing.T) {
		srv, _, _ := newTestServer(t, nil, genesisChain(t))
		rec := &simRecorder{}
		p := &simPool{
			s: srv, domainID: 1, spec: workerproc.Spec{Path: bin}, timeout: time.Second, start: rec.start,
			fileConn: func(*os.File) (net.Conn, error) { return nil, errors.New("fileConn refused") },
			jobs:     make(chan *simJob), ctx: context.Background(),
		}
		before := fdCount(t)
		done := make(chan error, 1)
		go func() { _, err := p.launch(0); done <- err }()
		select {
		case err := <-done:
			if err == nil || !strings.Contains(err.Error(), "fileConn refused") {
				t.Fatalf("launch error = %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("launch did not return")
		}
		pids := rec.pids()
		if len(pids) != 1 {
			t.Fatalf("started %d processes, want 1", len(pids))
		}
		if err := syscall.Kill(pids[0], 0); !errors.Is(err, syscall.ESRCH) {
			t.Fatalf("kill(pid, 0) = %v, want ESRCH: the child was not reaped", err)
		}
		if after := fdCount(t); after != before {
			t.Fatalf("%d descriptors before, %d after", before, after)
		}
	})
	t.Run("a panic during launch is an error and reaps the child", func(t *testing.T) {
		srv, _, _ := newTestServer(t, nil, genesisChain(t))
		rec := &simRecorder{}
		p := &simPool{
			s: srv, domainID: 1, spec: workerproc.Spec{Path: bin}, timeout: time.Second, start: rec.start,
			fileConn: func(*os.File) (net.Conn, error) { panic("fileConn exploded") },
			jobs:     make(chan *simJob), ctx: context.Background(),
		}
		before := fdCount(t)
		done := make(chan error, 1)
		go func() {
			defer func() {
				if r := recover(); r != nil {
					done <- fmt.Errorf("launch let a panic escape: %v", r)
				}
			}()
			_, err := p.launch(0)
			done <- err
		}()
		select {
		case err := <-done:
			if err == nil || !strings.Contains(err.Error(), "the launch panicked: fileConn exploded") {
				t.Fatalf("launch error = %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("launch did not return")
		}
		pids := rec.pids()
		if len(pids) != 1 {
			t.Fatalf("started %d processes, want 1", len(pids))
		}
		if err := syscall.Kill(pids[0], 0); !errors.Is(err, syscall.ESRCH) {
			t.Fatalf("kill(pid, 0) = %v, want ESRCH: the child was not reaped", err)
		}
		if after := fdCount(t); after != before {
			t.Fatalf("%d descriptors before, %d after", before, after)
		}
	})
	t.Run("dead pool", func(t *testing.T) {
		r := simChain(t)
		before := fdCount(t)
		rec := startDeadSims(t, r.srv, SimulationConfig{MaxConcurrent: 1}, func(s *Server) { s.simTiming.restartBase = 10 * time.Millisecond })
		eventually(t, 10*time.Second, "five start attempts", func() bool { return rec.tried() >= 5 })
		done := make(chan struct{})
		go func() { r.srv.stopSimulation(); close(done) }()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Fatal("stopSimulation did not return")
		}
		if after := fdCount(t); after != before {
			t.Fatalf("%d descriptors before the pool, %d after", before, after)
		}
	})
}

func TestSimulateCrashBackoff(t *testing.T) {
	bin := simBinaries(t)
	t.Run("a crash or an exit is replaced at once", func(t *testing.T) {
		for _, cmd := range []string{"panic", "exit:3"} {
			r := simChain(t)
			rec := startSims(t, r.srv, bin, SimulationConfig{MaxConcurrent: 1}, nil)
			echoEventually(t, r.srv)
			requireSimErr(t, r.srv, simCallJSON(cmd, 1000), stoppedMsg)
			eventually(t, 5*time.Second, "a replacement after "+cmd, func() bool { return rec.started() == 2 })
			echoEventually(t, r.srv)
		}
	})
	t.Run("repeated crashes back off", func(t *testing.T) {
		r := simChain(t)
		rec := startSims(t, r.srv, bin, SimulationConfig{MaxConcurrent: 1}, func(s *Server) {
			s.simTiming = simTiming{queueWait: 100 * time.Millisecond, restartBase: 2 * time.Second}
		})
		echoEventually(t, r.srv)
		requireSimErr(t, r.srv, simCallJSON("panic", 1000), stoppedMsg)
		echoEventually(t, r.srv)
		requireSimErr(t, r.srv, simCallJSON("panic", 1000), stoppedMsg)
		second := time.Now()
		requireSimErr(t, r.srv, simCallJSON("", 1000), "no simulator is free; retry later")
		var succeeded time.Time
		eventually(t, 5*time.Second, "an echo after the backoff", func() bool {
			if r := callRPC(t, r.srv, "zeno_simulate", simParams(simCallJSON("", 1000), "")); r.Error == nil {
				succeeded = time.Now()
				return true
			}
			time.Sleep(80 * time.Millisecond)
			return false
		})
		if d := succeeded.Sub(second); d < 1900*time.Millisecond {
			t.Fatalf("the replacement answered %s after the second crash, want at least 2 s", d)
		}
		if rec.started() != 3 {
			t.Fatalf("%d starts, want 3", rec.started())
		}
	})
	t.Run("a Failed answer and a caps failure cost no strike", func(t *testing.T) {
		r := simChain(t)
		rec := startSims(t, r.srv, bin, SimulationConfig{MaxConcurrent: 1}, func(s *Server) {
			s.simTiming = simTiming{queueWait: 100 * time.Millisecond, restartBase: 2 * time.Second}
		})
		echoEventually(t, r.srv)
		requireSimErr(t, r.srv, simCallJSON("fail", 1000), "the simulator could not run the input: testbin simulator: fail")
		r.set.capsErr = errors.New("node down")
		requireSimErr(t, r.srv, simCallJSON("caps:"+types.Hash{0xA1}.String(), 1000), "the simulator could not run the input: withdrawal caps are unavailable")
		r.set.capsErr = nil
		// The first crash after two answers that cost nothing is the
		// slot's first strike, so its replacement starts at once.
		requireSimErr(t, r.srv, simCallJSON("panic", 1000), stoppedMsg)
		start := time.Now()
		eventually(t, 1500*time.Millisecond, "an immediate replacement", func() bool {
			return callRPC(t, r.srv, "zeno_simulate", simParams(simCallJSON("", 1000), "")).Error == nil
		})
		if rec.started() != 2 {
			t.Fatalf("%d starts, want 2 (%s after the crash)", rec.started(), time.Since(start))
		}
	})
	t.Run("relaunchDelay", func(t *testing.T) {
		want := []time.Duration{0, time.Second, 2 * time.Second, 4 * time.Second, 8 * time.Second, 16 * time.Second, 30 * time.Second, 30 * time.Second}
		for i, w := range want {
			if got := relaunchDelay(i+1, simRestartBase, simRestartMax); got != w {
				t.Fatalf("relaunchDelay(%d) = %s, want %s", i+1, got, w)
			}
		}
		if got := relaunchDelay(1000, simRestartBase, simRestartMax); got != simRestartMax {
			t.Fatalf("relaunchDelay(1000) = %s", got)
		}
	})
}

func TestSimulateBusy(t *testing.T) {
	bin := simBinaries(t)
	r := simChain(t)
	startSims(t, r.srv, bin, SimulationConfig{MaxConcurrent: 1, TimeoutMs: 5000}, func(s *Server) { s.simTiming.queueWait = 300 * time.Millisecond })
	echoEventually(t, r.srv)
	slow := simAsync(r.srv, simCallJSON("sleep:1500", 1000))
	time.Sleep(200 * time.Millisecond)
	start := time.Now()
	requireSimErr(t, r.srv, simCallJSON("", 1000), "no simulator is free; retry later")
	if d := time.Since(start); d > 2*time.Second {
		t.Fatalf("the busy answer took %s", d)
	}

	ctx, cancel := context.WithCancel(context.Background())
	got := make(chan rpcResp, 1)
	go func() {
		body := fmt.Sprintf(`{"jsonrpc":"2.0","method":"zeno_simulate","params":%s,"id":1}`, simParams(simCallJSON("", 1000), ""))
		req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(body)).WithContext(ctx)
		req.Header.Set("Content-Type", "application/json")
		rec := httptest.NewRecorder()
		r.srv.ServeHTTP(rec, req)
		var resp rpcResp
		_ = json.Unmarshal(rec.Body.Bytes(), &resp)
		got <- resp
	}()
	time.Sleep(50 * time.Millisecond)
	cancel()
	if resp := waitResp(t, got, 2*time.Second); resp.Error == nil || resp.Error.Message != "the request ended before a simulator was free" {
		t.Fatalf("cancelled request = %+v", resp.Error)
	}
	if resp := waitResp(t, slow, 5*time.Second); resp.Error != nil {
		t.Fatalf("the slow simulation failed: %+v", resp.Error)
	}
}

func TestSimulateShutdown(t *testing.T) {
	bin := simBinaries(t)
	r := simChain(t)
	rec := startSims(t, r.srv, bin, SimulationConfig{MaxConcurrent: 2}, nil)
	eventually(t, 5*time.Second, "two simulators", func() bool { return rec.started() == 2 })
	simulateOK(t, r.srv, simCallJSON("", 1000), "")
	done := make(chan struct{})
	go func() { r.srv.Shutdown(&http.Server{}); close(done) }()
	select {
	case <-done:
	case <-time.After(15 * time.Second):
		t.Fatal("Shutdown did not return")
	}
	for _, pid := range rec.pids() {
		if err := syscall.Kill(pid, 0); !errors.Is(err, syscall.ESRCH) {
			t.Fatalf("kill(%d, 0) = %v, want ESRCH", pid, err)
		}
	}
	r.srv.stopSimulation()
}

func TestSimulateCodeBudget(t *testing.T) {
	bin := simBinaries(t)
	module := []byte{1, 2, 3, 4}
	var code types.Hash
	copy(code[:], crypto.Hash(module))
	x, y := types.Address{0xB1}, types.Address{0xB2}
	contract := types.Hash{0xC7}
	meta := codec.ContractMetadata{Version: codec.ContractMetadataVersion, UpgradePolicy: codec.PolicyOwnerUpgradeable, Owner: x}.Encode()
	var mh types.Hash
	copy(mh[:], crypto.Hash(meta))
	batch := []fixtureInput{
		{index: 1, args: codec.DeployStartArgs{CodeHash: code, MetadataHash: mh, TotalSize: 4, ChunkCount: 1, Metadata: meta}, effects: domain.Effects{Status: codec.StatusSuccess}},
		{index: 2, args: codec.DeployChunkArgs{CodeHash: code, ChunkIndex: 0, ChunkHash: code}, chunk: module, effects: domain.Effects{Status: codec.StatusSuccess}},
		{index: 3, args: codec.DeployFinalizeArgs{CodeHash: code}, effects: domain.Effects{Status: codec.StatusSuccess, ContractID: contract}},
	}
	senders := senderMap{hashOfIndex(1): x, hashOfIndex(2): x, hashOfIndex(3): x}
	for g := uint64(4); g < 24; g++ {
		batch = append(batch, fixtureInput{index: g, effects: success()})
		senders[hashOfIndex(g)] = y
	}
	dir := t.TempDir()
	cs := buildHistory(t, dir, nil, [][]fixtureInput{batch})
	s := newFakeSettlement()
	s.height = 10
	s.momentumTime = simMomentumTime
	s.accept(cs[0], 5)
	r := newIndexedChainWith(t, dir, s, senders)
	srv, sink, _ := newTestServer(t, nil, r.c)
	requireTags(t, doPoll(t, r), 0, 0, 0, StatusOK)
	srv.readers = map[uint8]domain.Reader{0: wasmreader.New("test")}
	rec := startSims(t, srv, bin, SimulationConfig{MaxConcurrent: 1}, nil)

	want := fmt.Sprintf("%x", module)
	if out := simulateOK(t, srv, simCallJSON("code:"+code.String(), 1000), ""); out.ReturnData != want {
		t.Fatalf("module = %s, want %s", out.ReturnData, want)
	}
	if out := simulateOK(t, srv, simCallJSON("code:"+types.Hash{0xEE}.String(), 1000), ""); out.ReturnData != fmt.Sprintf("%x", "notfound") {
		t.Fatalf("an unknown hash returned %s", out.ReturnData)
	}

	// The smallest index key budget at which a simulation alone gets
	// the module.
	fetches := func() bool {
		r := callRPC(t, srv, "zeno_simulate", simParams(simCallJSON("code:"+code.String(), 1000), ""))
		return r.Error == nil
	}
	budget := 0
	for k := 1; k <= 50 && budget == 0; k++ {
		srv.scanKeys = k
		if fetches() {
			budget = k
		}
	}
	if budget == 0 || budget >= 20 {
		t.Fatalf("no small key budget serves the module (found %d)", budget)
	}
	srv.scanKeys = budget
	if budget > 1 {
		srv.scanKeys = budget - 1
		if fetches() {
			t.Fatalf("a budget of %d serves the module, below the smallest %d", budget-1, budget)
		}
		srv.scanKeys = budget
	}

	list := `{"jsonrpc":"2.0","method":"zeno_getInputs","params":[{"domainId":1,"toBatch":"pending"}],"id":%d}`
	sim := fmt.Sprintf(`{"jsonrpc":"2.0","method":"zeno_simulate","params":%s,"id":%%d}`, simParams(simCallJSON("code:"+code.String(), 1000), ""))
	two := func(a, b string) []rpcResp {
		rec := post(t, srv, "["+a+","+b+"]")
		var resps []rpcResp
		if err := json.Unmarshal(rec.Body.Bytes(), &resps); err != nil || len(resps) != 2 {
			t.Fatalf("body = %s: %v", trunc(rec.Body.String()), err)
		}
		return resps
	}
	if resps := two(fmt.Sprintf(list, 1), fmt.Sprintf(list, 2)); resps[1].Error == nil || resps[1].Error.Message != scanBudgetMsg {
		t.Fatalf("the body's key budget is not spent by the first list request: %+v", resps[1])
	}
	resps := two(fmt.Sprintf(list, 1), fmt.Sprintf(sim, 2))
	var out simOut
	if resps[1].Error != nil || json.Unmarshal(resps[1].Result, &out) != nil || out.ReturnData != want {
		t.Fatalf("the simulation after a list request that spent the body's budget = %+v, want the module", resps[1])
	}

	t.Run("a panicking code source", func(t *testing.T) {
		before := rec.started()
		srv.scanKeys = maxScanKeys
		srv.readers = map[uint8]domain.Reader{0: codeReader{fn: func(domain.ReadContext, types.Hash) ([]byte, error) { panic("code source panicked") }}}
		e := rpcErr(t, srv, "zeno_simulate", simParams(simCallJSON("code:"+code.String(), 1000), ""))
		if e.Code != -32006 || !strings.HasPrefix(e.Message, "the simulator could not run the input: ") {
			t.Fatalf("error = %+v", e)
		}
		if sink.count("the code fetch panicked") != 1 {
			t.Fatalf("log:\n%s", sink.dump())
		}
		simulateOK(t, srv, simCallJSON("", 1000), "")
		if rec.started() != before {
			t.Fatalf("the simulator was replaced after a code source panic")
		}
	})
}

func TestSimulateReadBudget(t *testing.T) {
	bin := simBinaries(t)
	r := emptyChain(t)
	rec := startSims(t, r.srv, bin, SimulationConfig{MaxConcurrent: 1, TimeoutMs: 60_000}, nil)
	echoEventually(t, r.srv)

	if out := simulateOK(t, r.srv, simCallJSON("reads:65536", 1000), ""); out.Status != "SUCCESS" {
		t.Fatalf("reads:65536 status = %s, want SUCCESS", out.Status)
	}
	requireSimErr(t, r.srv, simCallJSON("reads:65537", 1000), simReadsMsg)
	if out := simulateOK(t, r.srv, simCallJSON("", 1000), ""); out.Status != "SUCCESS" {
		t.Fatalf("echo after the budget status = %s", out.Status)
	}
	if rec.started() != 1 || r.sink.count("was replaced") != 0 {
		t.Fatalf("%d starts after the budget, log:\n%s", rec.started(), r.sink.dump())
	}
	// 200,000 reads exceed the budget, so only a simulator that fetches
	// each path once can finish this input.
	if out := simulateOK(t, r.srv, simCallJSON("same:200000", 1000), ""); out.Status != "SUCCESS" {
		t.Fatalf("same:200000 status = %s, want SUCCESS", out.Status)
	}
}

func TestSimulateHostileReads(t *testing.T) {
	// start runs a simulation over a pipe up to its request phase and
	// returns the end that plays the simulator.
	start := func(t *testing.T, view SettlementView) (net.Conn, *simJob, <-chan serveResult) {
		p, _ := pipePool(t, 30*time.Second)
		m, end := pipeSim(t)
		j := &simJob{req: simRequest(t), view: view, done: make(chan simOutcome, 1)}
		res := serveAsync(p, m, j)
		if _, err := simipc.ReadFromGateway(end); err != nil {
			t.Fatal(err)
		}
		return end, j, res
	}
	ask := func(t *testing.T, end net.Conn, n int, want func(i int) bool) {
		t.Helper()
		var path domain.Path
		for i := 1; i <= n; i++ {
			if err := simipc.WriteMessage(end, simipc.ReadRequest{Path: path}); err != nil {
				t.Fatal(err)
			}
			reply, err := simipc.ReadFromGateway(end)
			if err != nil {
				t.Fatal(err)
			}
			v, isValue := reply.(simipc.Value)
			if isValue && v.Present {
				t.Fatalf("reply %d is a present value, want an absent one", i)
			}
			_, isUnavailable := reply.(simipc.Unavailable)
			if (want(i) && !isValue) || (!want(i) && !isUnavailable) {
				t.Fatalf("reply %d = %T", i, reply)
			}
		}
	}
	answered := func(i int) bool { return i <= simipc.MaxStateReads }
	resultAt := func(t *testing.T, index uint64) simipc.Message {
		data, err := codec.ExecutionResult{Version: codec.ExecutionResultVersion, GlobalInputIndex: index}.Encode()
		if err != nil {
			t.Fatal(err)
		}
		return simipc.Result{ExecutionResult: data}
	}
	result := func(t *testing.T) simipc.Message { return resultAt(t, 9) }
	finish := func(t *testing.T, res <-chan serveResult) serveResult {
		select {
		case got := <-res:
			return got
		case <-time.After(10 * time.Second):
			t.Fatal("serve did not return")
			return serveResult{}
		}
	}
	overBudget := func(t *testing.T, got serveResult) {
		t.Helper()
		if got.out.err == nil || got.out.err.Code != codeSimulation || got.out.err.Message != simReadsMsg || !got.healthy || got.cause != nil {
			t.Fatalf("serve = %+v, want -32006 %q from a healthy simulator", got, simReadsMsg)
		}
	}

	t.Run("result after the budget", func(t *testing.T) {
		end, _, res := start(t, nil)
		ask(t, end, simipc.MaxStateReads+2, answered)
		if err := simipc.WriteMessage(end, simipc.CodeRequest{}); err != nil {
			t.Fatal(err)
		}
		reply, err := simipc.ReadFromGateway(end)
		if err != nil {
			t.Fatal(err)
		}
		if _, ok := reply.(simipc.Unavailable); !ok {
			t.Fatalf("CodeRequest reply = %T, want Unavailable for a nil code source", reply)
		}
		if err := simipc.WriteMessage(end, result(t)); err != nil {
			t.Fatal(err)
		}
		overBudget(t, finish(t, res))
	})
	t.Run("invalid result after the budget", func(t *testing.T) {
		end, _, res := start(t, nil)
		ask(t, end, simipc.MaxStateReads+1, answered)
		if err := simipc.WriteMessage(end, resultAt(t, 8)); err != nil {
			t.Fatal(err)
		}
		got := finish(t, res)
		if got.healthy || got.cause == nil {
			t.Fatalf("serve = %+v, want an unhealthy simulator with a cause", got)
		}
	})
	t.Run("failed after the budget", func(t *testing.T) {
		end, _, res := start(t, nil)
		ask(t, end, simipc.MaxStateReads+1, answered)
		if err := simipc.WriteMessage(end, simipc.Failed{Message: "out of reads"}); err != nil {
			t.Fatal(err)
		}
		overBudget(t, finish(t, res))
	})
	t.Run("exactly the budget", func(t *testing.T) {
		end, j, res := start(t, nil)
		ask(t, end, simipc.MaxStateReads, answered)
		if err := simipc.WriteMessage(end, result(t)); err != nil {
			t.Fatal(err)
		}
		got := finish(t, res)
		if got.out.err != nil || !got.healthy || got.cause != nil || j.reads != simipc.MaxStateReads {
			t.Fatalf("serve = %+v with %d reads, want a result at the budget", got, j.reads)
		}
	})
	t.Run("code and caps requests are not reads", func(t *testing.T) {
		end, j, res := start(t, fakeView{newFakeSettlement()})
		for _, req := range []simipc.Message{simipc.CodeRequest{}, simipc.CapRequest{Assets: []codec.AssetID{{31: 1}}}} {
			if err := simipc.WriteMessage(end, req); err != nil {
				t.Fatal(err)
			}
			if _, err := simipc.ReadFromGateway(end); err != nil {
				t.Fatal(err)
			}
		}
		ask(t, end, simipc.MaxStateReads, answered)
		if err := simipc.WriteMessage(end, result(t)); err != nil {
			t.Fatal(err)
		}
		got := finish(t, res)
		if got.out.err != nil || !got.healthy || got.cause != nil || j.reads != simipc.MaxStateReads {
			t.Fatalf("serve = %+v with %d reads, want a result at the budget", got, j.reads)
		}
	})
}

// simModuleCall is a CallL2 simulation input for target c from X.
func simModuleCall(x types.Address, c types.Hash) string {
	return fmt.Sprintf(`{"kind":"CallL2","sender":"%s","targetContract":"%s","plasmaLimit":10000000}`, x, c)
}

func TestSimulateEqualsBatch(t *testing.T) {
	d, x := types.Address{0xD1}, types.Address{0xA1}
	c1, c2 := contractIDOf(d, 3), contractIDOf(d, 6)

	execMod := loadModule(t, "EXEC-009")
	abortMod := loadModule(t, "CV-ABORT-1")
	// The plugin faults a result blob whose input index or contract
	// differs from the executing call's, so the module's fixed blob is
	// rewritten to call 1's index and contract, keeping every width.
	index := binary.BigEndian.AppendUint64(nil, 1006)
	contract := bytes.Repeat([]byte{0x11}, 32)
	if n := bytes.Count(execMod, index); n != 3 {
		t.Fatalf("EXEC-009's blob names input 1006 %d times, want 3", n)
	}
	if n := bytes.Count(execMod, contract); n != 2 {
		t.Fatalf("EXEC-009's blob names contract 0x11 x 32 %d times, want 2", n)
	}
	execMod = bytes.ReplaceAll(execMod, index, binary.BigEndian.AppendUint64(nil, 8))
	execMod = bytes.ReplaceAll(execMod, contract, c1[:])

	plugin, err := wasm.New(wasm.Config{NetworkName: "test", CodeStorePath: t.TempDir()})
	if err != nil {
		t.Fatalf("wasm.New: %v", err)
	}
	t.Cleanup(func() { _ = plugin.Close() })

	senders := senderMap{}
	var batches [][]ledger.Input
	var g uint64
	add := func(b int, sender types.Address, args codec.InputArgs, chunk []byte) {
		g++
		data, err := codec.Input{SourceAccountBlockHash: hashOfIndex(g), Args: args}.Encode()
		if err != nil {
			t.Fatal(err)
		}
		senders[hashOfIndex(g)] = sender
		for len(batches) <= b {
			batches = append(batches, nil)
		}
		batches[b] = append(batches[b], ledger.Input{
			Ctx:     domain.InputContext{GlobalInputIndex: g, L1Sender: sender, SourceData: chunk, MomentumHeight: 10, MomentumTime: simMomentumTime},
			Payload: data,
		})
	}
	deploy := func(mod []byte) {
		code := wasm.CodeHash(mod)
		m := codec.ContractMetadata{Version: codec.ContractMetadataVersion, UpgradePolicy: codec.PolicyImmutable}.Encode()
		var mh, ch types.Hash
		copy(mh[:], crypto.Hash(m))
		copy(ch[:], crypto.Hash(mod))
		add(0, d, codec.DeployStartArgs{CodeHash: code, MetadataHash: mh, TotalSize: uint64(len(mod)), ChunkCount: 1, Metadata: m}, nil)
		add(0, d, codec.DeployChunkArgs{CodeHash: code, ChunkIndex: 0, ChunkHash: ch}, mod)
		add(0, d, codec.DeployFinalizeArgs{CodeHash: code}, nil)
	}
	deploy(execMod)
	deploy(abortMod)
	add(0, d, codec.BuyStorageArgs{ContractID: c1, Bytes: 65_536}, nil)
	add(1, x, codec.CallL2Args{TargetContract: c1, PlasmaLimit: 10_000_000}, nil)
	add(2, x, codec.CallL2Args{TargetContract: c2, PlasmaLimit: 10_000_000}, nil)

	dir := t.TempDir()
	cs := buildFrom(t, dir, nil, plugin, batches)
	rec1 := readFile(t, recordPath(layoutOf(dir), 1))
	removeRecord(t, dir, 1)
	removeRecord(t, dir, 2)

	s := newFakeSettlement()
	s.height = 10
	s.momentumTime = simMomentumTime
	s.accept(cs[0], 100)
	r := newIndexedChainWith(t, dir, s, senders)
	requireTags(t, doPoll(t, r), 0, 0, -1, StatusOK)
	srv, _ := newReaderServer(t, r.c)
	startSims(t, srv, simWasmBinary(t), SimulationConfig{MaxConcurrent: 1, TimeoutMs: 5_000}, func(s *Server) { s.simTiming.queueWait = 10 * time.Second })

	compare := func(t *testing.T, out simOut, g uint64, k int) {
		t.Helper()
		l := layoutOf(dir)
		receipts, _, err := codec.DecodeReceiptSidecar(readFile(t, recovery.ReceiptsPath(l.Receipts, cs[k].DAHash)))
		if err != nil || len(receipts) != 1 {
			t.Fatalf("batch %d sidecar: %d receipts, err %v", k, len(receipts), err)
		}
		rc := receipts[0]
		records, err := codec.DecodeBundle(readFile(t, recovery.BundlePath(l.Bundles, cs[k].DAHash)))
		if err != nil || len(records) != 1 {
			t.Fatalf("batch %d bundle: %d records, err %v", k, len(records), err)
		}
		want, err := codec.DecodeExecutionResult(records[0].ExecutionResult)
		if err != nil {
			t.Fatal(err)
		}

		if out.Status != rc.Status.String() || out.GasUsed != rc.GasUsed || out.AbortCode != rc.AbortCode ||
			out.ContractID != rc.ContractID.String() || out.CodeHash != rc.CodeHash.String() || out.InputIndex != rc.InputIndex || out.InputIndex != g {
			t.Errorf("preview %+v differs from receipt %+v", out, rc)
		}
		expected, err := simResultOf(r.c, tagRead{}, &simJob{}, simOutcome{exec: want})
		if err != nil {
			t.Fatal(err)
		}
		raw, err := json.Marshal(expected)
		if err != nil {
			t.Fatal(err)
		}
		var exp simOut
		if err := json.Unmarshal(raw, &exp); err != nil {
			t.Fatal(err)
		}
		if out.ReturnData != exp.ReturnData || !reflect.DeepEqual(out.Events, exp.Events) ||
			!reflect.DeepEqual(out.Outbox, exp.Outbox) || !reflect.DeepEqual(out.StateDiff, exp.StateDiff) {
			t.Errorf("preview effects differ from the bundle record:\npreview %+v\nbatch   %+v", out, exp)
		}
		if out.BatchID != uint64(k-1) || out.PostStateRoot != cs[k-1].PostStateRoot.String() || out.MomentumHeight != 10 ||
			out.MomentumTime != simMomentumTime || out.Verified || !out.Simulated {
			t.Errorf("preview context = %+v", out)
		}
		for i, o := range out.Outbox {
			if o.OutboxID != nil {
				t.Errorf("outbox %d has an outboxId", i)
			}
		}
		switch k {
		case 1:
			if rc.Status != codec.StatusSuccess || len(want.StateDiff) != 2 || len(want.Events) != 1 || len(want.Outbox) != 1 || len(want.ReturnData) == 0 {
				t.Errorf("batch 1 outcome = %v with %d diff, %d events, %d outbox, %d return bytes", rc.Status, len(want.StateDiff), len(want.Events), len(want.Outbox), len(want.ReturnData))
			}
		case 2:
			wantRet := make([]byte, 40)
			for i := range wantRet {
				wantRet[i] = byte(i)
			}
			if rc.Status != codec.StatusRevert || rc.AbortCode != 7 || !bytes.Equal(want.ReturnData, wantRet) {
				t.Errorf("batch 2 outcome = %v abort %d, %d return bytes", rc.Status, rc.AbortCode, len(want.ReturnData))
			}
		}
	}

	compare(t, simulateOK(t, srv, simModuleCall(x, c1), `"latest"`), 8, 1)

	writeFile(t, recordPath(layoutOf(dir), 1), rec1)
	s.accept(cs[1], 100)
	requireTags(t, doPoll(t, r), 1, 1, -1, StatusOK)
	compare(t, simulateOK(t, srv, simModuleCall(x, c2), `"latest"`), 9, 2)
}

// BenchmarkSimulateProve measures the gateway's cost of one state read:
// a proof of a path that is absent from, or a 16 KiB value that is in,
// a state of 1,024 leaves. The absent case cycles through 2,048
// distinct absent paths, because the proof's cost depends on where a
// path diverges from the tree.
func BenchmarkSimulateProve(b *testing.B) {
	pathOf := func(i uint64) domain.Path {
		var p domain.Path
		copy(p[:], crypto.Hash(binary.BigEndian.AppendUint64(nil, i)))
		return p
	}
	var diff []domain.StateDiffEntry
	for i := uint64(0); i < 1024; i++ {
		diff = append(diff, domain.StateDiffEntry{Path: pathOf(i), Value: bytes.Repeat([]byte{byte(i)}, 32)})
	}
	large := pathOf(1 << 20)
	diff = append(diff, domain.StateDiffEntry{Path: large, Value: bytes.Repeat([]byte{0xAB}, 16_384)})
	v := state.Version{}.Apply(diff)
	absent := make([]domain.Path, 2048)
	for i := range absent {
		absent[i] = pathOf(1<<21 + uint64(i))
	}
	for _, c := range []struct {
		name  string
		paths []domain.Path
	}{{"absent", absent}, {"present16KiB", []domain.Path{large}}} {
		b.Run(c.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				v.Prove(c.paths[i%len(c.paths)])
			}
		})
	}
}
