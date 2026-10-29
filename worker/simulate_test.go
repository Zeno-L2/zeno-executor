package worker

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"runtime/debug"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/zenon-network/go-zenon/common/types"

	"github.com/Zeno-L2/zeno-executor/domain"
	"github.com/Zeno-L2/zeno-executor/internal/codec"
	"github.com/Zeno-L2/zeno-executor/internal/ipc"
	"github.com/Zeno-L2/zeno-executor/internal/simipc"
)

// simDomain is the simulate-mode test domain. A CallL2 payload is a
// command: read:<path>, reread:<n>:<path>, caps:<asset>[,<asset>...],
// code:<hash>, fail, revert-diff, exact-claim, or anything else, which
// echoes the context. revert-diff returns REVERT effects with a state
// diff, and exact-claim returns SUCCESS effects claiming a deposit of
// 100.
type simDomain struct {
	fetch       CodeFetcher
	decodeCalls atomic.Int32
}

type simInput struct {
	ctx     domain.InputContext
	payload string
}

func (*simDomain) Genesis() domain.StateInit { return domain.StateInit{} }

func (d *simDomain) DecodeInput(ctx domain.InputContext, payload []byte) (domain.Input, error) {
	d.decodeCalls.Add(1)
	in, err := codec.DecodeInput(payload)
	if err != nil {
		return nil, err
	}
	args, _ := in.Args.(codec.CallL2Args)
	return simInput{ctx: ctx, payload: string(args.Payload)}, nil
}

func hexBytes(s string) []byte {
	b, err := hex.DecodeString(s)
	if err != nil {
		panic(err)
	}
	return b
}

func (d *simDomain) Apply(state domain.StateAccess, in domain.Input) (domain.Effects, error) {
	si := in.(simInput)
	cmd, arg, _ := strings.Cut(si.payload, ":")
	switch cmd {
	case "read":
		var path domain.Path
		copy(path[:], hexBytes(arg))
		v, present, err := state.Read(path)
		if err != nil {
			return domain.Effects{}, err
		}
		_ = present
		return domain.Effects{StateDiff: []domain.StateDiffEntry{{Path: path, Value: v}}}, nil
	case "reread":
		count, hexPath, _ := strings.Cut(arg, ":")
		n, err := strconv.Atoi(count)
		if err != nil {
			return domain.Effects{}, err
		}
		var path domain.Path
		copy(path[:], hexBytes(hexPath))
		first, firstPresent, err := state.Read(path)
		if err != nil {
			return domain.Effects{}, err
		}
		for i := 1; i < n; i++ {
			v, present, err := state.Read(path)
			if err != nil {
				return domain.Effects{}, err
			}
			if present != firstPresent || !bytes.Equal(v, first) || (v == nil) != (first == nil) {
				return domain.Effects{}, fmt.Errorf("read %d of %x differs from the first", i+1, path[:])
			}
		}
		return domain.Effects{StateDiff: []domain.StateDiffEntry{{Path: path, Value: first}}}, nil
	case "caps":
		var assets []codec.AssetID
		for _, a := range strings.Split(arg, ",") {
			var id codec.AssetID
			copy(id[:], hexBytes(a))
			assets = append(assets, id)
		}
		amounts, err := si.ctx.Caps.MaxBatchWithdrawals(assets)
		if err != nil {
			return domain.Effects{}, err
		}
		var out []byte
		for _, a := range amounts {
			out = append(out, a[:]...)
		}
		return domain.Effects{ReturnData: out}, nil
	case "code":
		var h types.Hash
		copy(h[:], hexBytes(arg))
		module, found, err := d.fetch(h)
		if err != nil {
			return domain.Effects{}, err
		}
		return domain.Effects{ReturnData: append([]byte{byte(map[bool]int{false: 0, true: 1}[found])}, module...)}, nil
	case "fail":
		return domain.Effects{}, errors.New("apply failed on request")
	case "revert-diff":
		return domain.Effects{Status: codec.StatusRevert, StateDiff: []domain.StateDiffEntry{{Path: domain.Path{1}, Value: []byte("v")}}}, nil
	case "exact-claim":
		return domain.Effects{Status: codec.StatusSuccess, ClaimedDeposit: codec.U256{31: 100}}, nil
	}

	idx := si.ctx.GlobalInputIndex
	data := binary.BigEndian.AppendUint64(nil, idx)
	data = binary.BigEndian.AppendUint64(data, si.ctx.MomentumHeight)
	data = binary.BigEndian.AppendUint64(data, si.ctx.MomentumTime)
	data = append(data, si.ctx.MomentumHash[:]...)
	data = append(data, si.ctx.L1Sender[:]...)
	return domain.Effects{
		Status:  codec.StatusSuccess,
		GasUsed: 77,
		StateDiff: []domain.StateDiffEntry{
			{Path: domain.Path{1}, Value: nil},
			{Path: domain.Path{2}, Value: []byte{}},
			{Path: domain.Path{3}, Value: []byte("v")},
		},
		Events:         []codec.Event{{ContractID: types.Hash{9}, InputIndex: idx, Data: data}},
		OutboxMessages: []codec.OutboxMessage{{SourceContract: types.Hash{9}, InputIndex: idx, Target: codec.L2Delivery{TargetContract: types.Hash{8}}, Payload: []byte("m")}},
		ReturnData:     []byte("ret"),
	}, nil
}

func newTestSim(out **simDomain) NewSimulator {
	return func(_ SimConfig, code CodeFetcher) (domain.Domain, error) {
		d := &simDomain{fetch: code}
		if out != nil {
			*out = d
		}
		return d, nil
	}
}

func callInput(t *testing.T, payload string) []byte {
	t.Helper()
	b, err := codec.Input{Args: codec.CallL2Args{TargetContract: types.Hash{1}, PlasmaLimit: 1, Payload: []byte(payload)}}.Encode()
	if err != nil {
		t.Fatalf("encode input: %v", err)
	}
	return b
}

// restoreMemoryLimit restores the process-wide memory limit after a
// test that runs simulate.
func restoreMemoryLimit(t *testing.T) {
	t.Helper()
	old := debug.SetMemoryLimit(-1)
	t.Cleanup(func() { debug.SetMemoryLimit(old) })
}

// gateway is the fake gateway end of a simulate-mode channel.
type gateway struct {
	t      *testing.T
	conn   net.Conn
	done   chan int
	cancel context.CancelFunc
	// respond answers one request; nil answers every request with
	// Unavailable.
	respond func(simipc.Message) simipc.Message
	// requests records each request the simulator sent.
	requests []simipc.Message
}

func startGateway(t *testing.T, newSim NewSimulator) *gateway {
	t.Helper()
	restoreMemoryLimit(t)
	a, b := net.Pipe()
	ctx, cancel := context.WithCancel(context.Background())
	g := &gateway{t: t, conn: a, done: make(chan int, 1), cancel: cancel}
	go func() {
		code := simulate(ctx, bufio.NewReader(b), b, newSim)
		b.Close()
		g.done <- code
	}()
	t.Cleanup(func() {
		cancel()
		a.Close()
	})
	return g
}

func (g *gateway) send(m simipc.Message) {
	g.t.Helper()
	if err := simipc.WriteMessage(g.conn, m); err != nil {
		g.t.Fatalf("write %T: %v", m, err)
	}
}

func (g *gateway) recv() simipc.Message {
	g.t.Helper()
	m, err := simipc.ReadFromSimulator(g.conn)
	if err != nil {
		g.t.Fatalf("read: %v", err)
	}
	return m
}

// start sends a Config and expects Ready.
func (g *gateway) start(cfg simipc.Config) {
	g.t.Helper()
	g.send(cfg)
	if _, ok := g.recv().(simipc.Ready); !ok {
		g.t.Fatal("first reply is not Ready")
	}
}

func defaultConfig() simipc.Config { return simipc.Config{DomainID: 3, NetworkName: "testnet"} }

// simulate sends one Simulate and answers requests until a Result or
// Failed arrives.
func (g *gateway) simulate(s simipc.Simulate) simipc.Message {
	g.t.Helper()
	g.send(s)
	for {
		m := g.recv()
		switch m.(type) {
		case simipc.Result, simipc.Failed:
			return m
		}
		g.requests = append(g.requests, m)
		var reply simipc.Message = simipc.Unavailable{}
		if g.respond != nil {
			reply = g.respond(m)
		}
		g.send(reply)
	}
}

func (g *gateway) wait() int {
	g.t.Helper()
	select {
	case code := <-g.done:
		return code
	case <-time.After(10 * time.Second):
		g.t.Fatal("simulate did not return")
		return -1
	}
}

// expectEOF fails unless the simulator wrote nothing further.
func (g *gateway) expectEOF() {
	g.t.Helper()
	if m, err := simipc.ReadFromSimulator(g.conn); err != io.EOF {
		g.t.Fatalf("read after exit: %v, %v; want io.EOF", m, err)
	}
}

func (g *gateway) simInput(t *testing.T, payload string) simipc.Simulate {
	return simipc.Simulate{GlobalInputIndex: 1, MomentumHeight: 2, MomentumTime: 3, InputData: callInput(t, payload)}
}

func mustResult(t *testing.T, m simipc.Message) (simipc.Result, codec.ExecutionResult) {
	t.Helper()
	res, ok := m.(simipc.Result)
	if !ok {
		t.Fatalf("got %T %+v, want a Result", m, m)
	}
	er, err := codec.DecodeExecutionResult(res.ExecutionResult)
	if err != nil {
		t.Fatalf("decode execution result: %v", err)
	}
	return res, er
}

func TestSimulateMemoryLimit(t *testing.T) {
	for _, tc := range []struct {
		name  string
		limit uint64
		want  int64
	}{{"2 GiB", 2 << 30, 2 << 30}, {"default", 0, simipc.DefaultMemoryLimit}} {
		t.Run(tc.name, func(t *testing.T) {
			g := startGateway(t, newTestSim(nil))
			cfg := defaultConfig()
			cfg.MemoryLimit = tc.limit
			g.start(cfg)
			if got := debug.SetMemoryLimit(-1); got != tc.want {
				t.Fatalf("memory limit = %d, want %d", got, tc.want)
			}
		})
	}
}

func hexOf(b []byte) string { return hex.EncodeToString(b) }

func TestSimulateReads(t *testing.T) {
	g := startGateway(t, newTestSim(nil))
	g.start(defaultConfig())
	g.respond = func(m simipc.Message) simipc.Message {
		switch m.(simipc.ReadRequest).Path[0] {
		case 1:
			return simipc.Value{Present: true, Value: []byte{1, 2}}
		case 2:
			return simipc.Value{Present: true, Value: []byte{}}
		}
		return simipc.Value{}
	}
	for _, tc := range []struct {
		first   byte
		present bool
		value   string
	}{{0, false, ""}, {1, true, "\x01\x02"}, {2, true, ""}} {
		path := make([]byte, 32)
		path[0] = tc.first
		_, er := mustResult(t, g.simulate(g.simInput(t, "read:"+hexOf(path))))
		if len(er.StateDiff) != 1 {
			t.Fatalf("diff has %d entries", len(er.StateDiff))
		}
		v := er.StateDiff[0].Value
		if (v != nil) != tc.present || string(v) != tc.value {
			t.Errorf("path %d: value %q (nil %v), want %q present %v", tc.first, v, v == nil, tc.value, tc.present)
		}
	}
}

func TestSimulateReadCache(t *testing.T) {
	g := startGateway(t, newTestSim(nil))
	g.start(defaultConfig())
	answer := byte(0)
	g.respond = func(m simipc.Message) simipc.Message {
		switch m.(simipc.ReadRequest).Path[0] {
		case 1:
			if answer != 0 {
				return simipc.Value{Present: true, Value: []byte{answer}}
			}
			return simipc.Value{Present: true, Value: []byte{1, 2}}
		case 2:
			return simipc.Value{Present: true, Value: []byte{}}
		}
		return simipc.Value{}
	}
	pathHex := func(first byte) string {
		path := make([]byte, 32)
		path[0] = first
		return hexOf(path)
	}
	reread := func(n int, first byte) codec.ExecutionResult {
		t.Helper()
		_, er := mustResult(t, g.simulate(g.simInput(t, fmt.Sprintf("reread:%d:%s", n, pathHex(first)))))
		if len(er.StateDiff) != 1 {
			t.Fatalf("diff has %d entries", len(er.StateDiff))
		}
		return er
	}
	for _, tc := range []struct {
		first   byte
		present bool
		value   string
	}{{0, false, ""}, {1, true, "\x01\x02"}, {2, true, ""}} {
		before := len(g.requests)
		v := reread(3, tc.first).StateDiff[0].Value
		if got := len(g.requests) - before; got != 1 {
			t.Errorf("path %d: %d requests for 3 reads, want 1", tc.first, got)
		}
		if (v != nil) != tc.present || string(v) != tc.value {
			t.Errorf("path %d: value %q (nil %v), want %q present %v", tc.first, v, v == nil, tc.value, tc.present)
		}
	}

	before := len(g.requests)
	reread(200_000, 1)
	if got := len(g.requests) - before; got != 1 {
		t.Errorf("200000 reads sent %d requests, want 1", got)
	}

	before = len(g.requests)
	reread(2, 1)
	answer = 3
	v := reread(2, 1).StateDiff[0].Value
	if got := len(g.requests) - before; got != 2 {
		t.Errorf("two simulations sent %d requests, want 2", got)
	}
	if string(v) != "\x03" {
		t.Errorf("second simulation read %q, want the changed value 03", v)
	}

	t.Run("broken channel", func(t *testing.T) {
		var p domain.Path
		p[0] = 1
		e := errors.New("channel broke")
		c := &simChannel{broken: e, values: map[domain.Path]simipc.Value{p: {Present: true, Value: []byte{1}}}}
		if _, _, err := c.Read(p); err != e {
			t.Errorf("Read error = %v, want %v", err, e)
		}
	})
}

func TestSimulateCaps(t *testing.T) {
	var a, b codec.AssetID
	a[0], b[0] = 0xA1, 0xB2
	g := startGateway(t, newTestSim(nil))
	g.start(defaultConfig())
	g.respond = func(m simipc.Message) simipc.Message {
		return simipc.Caps{Amounts: []codec.U256{{31: 100}, {31: 200}}}
	}
	res, er := mustResult(t, g.simulate(g.simInput(t, "caps:"+hexOf(a[:])+","+hexOf(b[:]))))
	_ = res
	if len(g.requests) != 1 {
		t.Fatalf("%d requests", len(g.requests))
	}
	req := g.requests[0].(simipc.CapRequest)
	if len(req.Assets) != 2 || req.Assets[0] != a || req.Assets[1] != b {
		t.Errorf("assets = %x, want %x then %x", req.Assets, a, b)
	}
	want := append(append([]byte{}, make([]byte, 31)...), 100)
	want = append(want, make([]byte, 31)...)
	want = append(want, 200)
	if !bytes.Equal(er.ReturnData, want) {
		t.Errorf("return data = %x, want %x", er.ReturnData, want)
	}
}

func TestSimulateCapsCountMismatch(t *testing.T) {
	g := startGateway(t, newTestSim(nil))
	g.start(defaultConfig())
	g.send(simipc.Simulate{InputData: callInput(t, "caps:"+hexOf(make([]byte, 32))+","+hexOf(append(make([]byte, 31), 1))), GlobalInputIndex: 1})
	if _, ok := g.recv().(simipc.CapRequest); !ok {
		t.Fatal("no CapRequest")
	}
	g.send(simipc.Caps{Amounts: []codec.U256{{31: 100}}})
	if code := g.wait(); code != 1 {
		t.Fatalf("exit %d, want 1", code)
	}
	g.expectEOF()
}

func TestSimulateWrongReply(t *testing.T) {
	g := startGateway(t, newTestSim(nil))
	g.start(defaultConfig())
	g.send(simipc.Simulate{InputData: callInput(t, "read:"+hexOf(make([]byte, 32))), GlobalInputIndex: 1})
	if _, ok := g.recv().(simipc.ReadRequest); !ok {
		t.Fatal("no ReadRequest")
	}
	g.send(simipc.Code{Found: false})
	if code := g.wait(); code != 1 {
		t.Fatalf("exit %d, want 1", code)
	}
	g.expectEOF()
}

func TestSimulateCode(t *testing.T) {
	g := startGateway(t, newTestSim(nil))
	g.start(defaultConfig())
	g.respond = func(m simipc.Message) simipc.Message {
		if req := m.(simipc.CodeRequest); req.SourceCodeHash[0] == 1 {
			return simipc.Code{Found: true, Module: []byte("mod")}
		}
		return simipc.Code{}
	}
	h := make([]byte, 32)
	h[0] = 1
	_, er := mustResult(t, g.simulate(g.simInput(t, "code:"+hexOf(h))))
	if string(er.ReturnData) != "\x01mod" {
		t.Errorf("return data = %q", er.ReturnData)
	}
	_, er = mustResult(t, g.simulate(g.simInput(t, "code:"+hexOf(make([]byte, 32)))))
	if string(er.ReturnData) != "\x00" {
		t.Errorf("return data = %q", er.ReturnData)
	}
}

func TestSimulateFailures(t *testing.T) {
	var d *simDomain
	g := startGateway(t, newTestSim(&d))
	g.start(defaultConfig())
	path := hexOf(make([]byte, 32))

	expectFailed := func(m simipc.Message, contains string) {
		t.Helper()
		f, ok := m.(simipc.Failed)
		if !ok || !strings.Contains(f.Message, contains) {
			t.Fatalf("got %T %+v, want Failed containing %q", m, m, contains)
		}
	}

	expectFailed(g.simulate(g.simInput(t, "read:"+path)), "unavailable")
	mustResult(t, g.simulate(g.simInput(t, "echo")))

	expectFailed(g.simulate(g.simInput(t, "fail")), "apply failed on request")
	mustResult(t, g.simulate(g.simInput(t, "echo")))

	expectFailed(g.simulate(simipc.Simulate{InputData: []byte{0}}), "the input does not decode")

	before := d.decodeCalls.Load()
	start, err := codec.Input{Args: codec.DeployStartArgs{CodeHash: types.Hash{2}, MetadataHash: types.Hash{3}, TotalSize: 100, ChunkCount: 1}}.Encode()
	if err != nil {
		t.Fatal(err)
	}
	f := g.simulate(simipc.Simulate{InputData: start}).(simipc.Failed)
	if f.Message != "simulate mode runs only CallL2 and Deposit inputs" {
		t.Errorf("message = %q", f.Message)
	}
	if got := d.decodeCalls.Load(); got != before {
		t.Errorf("plugin DecodeInput called %d times for a refused kind", got-before)
	}

	m := g.simulate(g.simInput(t, "revert-diff"))
	expectFailed(m, "the executor refuses the plugin's effects: ")
	expectFailed(m, "non-empty StateDiff/Events")

	m = g.simulate(g.simInput(t, "exact-claim"))
	expectFailed(m, "the executor refuses the plugin's effects: ")
	expectFailed(m, "ClaimedDeposit exceeds the input's deposit_amount")

	claim, err := codec.Input{Args: codec.CallL2Args{TargetContract: types.Hash{1}, PlasmaLimit: 1, Payload: []byte("exact-claim"), DepositAsset: codec.AssetID{31: 1}, DepositAmount: codec.U256{31: 100}}}.Encode()
	if err != nil {
		t.Fatal(err)
	}
	_, er := mustResult(t, g.simulate(simipc.Simulate{GlobalInputIndex: 1, InputData: claim}))
	if er.ClaimedDeposit != (codec.U256{31: 100}) {
		t.Errorf("ClaimedDeposit = %v, want 100", er.ClaimedDeposit)
	}
}

func TestSimulateResult(t *testing.T) {
	g := startGateway(t, newTestSim(nil))
	g.start(defaultConfig())
	var sender types.Address
	for i := range sender {
		sender[i] = 0x33
	}
	res, er := mustResult(t, g.simulate(simipc.Simulate{
		GlobalInputIndex: 5, MomentumHeight: 11, MomentumTime: 22,
		MomentumHash: types.Hash{0x44, 0x45}, L1Sender: sender,
		InputData: callInput(t, "echo"),
	}))
	if res.Status != codec.StatusSuccess || res.GasUsed != 77 {
		t.Errorf("status %v gas %d", res.Status, res.GasUsed)
	}
	if er.PreStateRoot != (types.Hash{}) || er.PostStateRoot != (types.Hash{}) {
		t.Error("roots are not zero")
	}
	if er.GlobalInputIndex != 5 {
		t.Errorf("global input index = %d", er.GlobalInputIndex)
	}
	if len(er.Events) != 1 {
		t.Fatalf("%d events", len(er.Events))
	}
	wantData := binary.BigEndian.AppendUint64(nil, 5)
	wantData = binary.BigEndian.AppendUint64(wantData, 11)
	wantData = binary.BigEndian.AppendUint64(wantData, 22)
	wantData = append(wantData, 0x44, 0x45)
	wantData = append(wantData, make([]byte, 30)...)
	wantData = append(wantData, sender[:]...)
	if !bytes.Equal(er.Events[0].Data, wantData) {
		t.Errorf("echo event = %x, want %x", er.Events[0].Data, wantData)
	}
	if len(er.StateDiff) != 3 {
		t.Fatalf("%d diff entries", len(er.StateDiff))
	}
	if er.StateDiff[0].Key[0] != 1 || er.StateDiff[0].Value != nil {
		t.Errorf("deletion entry = %+v", er.StateDiff[0])
	}
	if er.StateDiff[1].Key[0] != 2 || er.StateDiff[1].Value == nil || len(er.StateDiff[1].Value) != 0 {
		t.Errorf("present-empty entry = %+v", er.StateDiff[1])
	}
	if er.StateDiff[2].Key[0] != 3 || string(er.StateDiff[2].Value) != "v" {
		t.Errorf("value entry = %+v", er.StateDiff[2])
	}
	if len(er.Outbox) != 1 || string(er.ReturnData) != "ret" {
		t.Errorf("outbox %d, return data %q", len(er.Outbox), er.ReturnData)
	}
}

func encoded(t *testing.T, m simipc.Message) []byte {
	t.Helper()
	var buf bytes.Buffer
	if err := simipc.WriteMessage(&buf, m); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

func TestSimulateEnds(t *testing.T) {
	t.Run("eof between simulations", func(t *testing.T) {
		g := startGateway(t, newTestSim(nil))
		g.start(defaultConfig())
		mustResult(t, g.simulate(g.simInput(t, "echo")))
		g.conn.Close()
		if code := g.wait(); code != 0 {
			t.Fatalf("exit %d, want 0", code)
		}
	})
	t.Run("context cancelled while idle", func(t *testing.T) {
		g := startGateway(t, newTestSim(nil))
		g.start(defaultConfig())
		g.cancel()
		if code := g.wait(); code != 0 {
			t.Fatalf("exit %d, want 0", code)
		}
	})
	t.Run("eof awaiting a value", func(t *testing.T) {
		g := startGateway(t, newTestSim(nil))
		g.start(defaultConfig())
		g.send(g.simInput(t, "read:"+hexOf(make([]byte, 32))))
		if _, ok := g.recv().(simipc.ReadRequest); !ok {
			t.Fatal("no ReadRequest")
		}
		g.conn.Close()
		if code := g.wait(); code != 1 {
			t.Fatalf("exit %d, want 1", code)
		}
	})
}

func TestSimulateMalformed(t *testing.T) {
	t.Run("value between simulations", func(t *testing.T) {
		g := startGateway(t, newTestSim(nil))
		g.start(defaultConfig())
		g.send(simipc.Value{})
		if code := g.wait(); code != 1 {
			t.Fatalf("exit %d, want 1", code)
		}
	})
	t.Run("simulate with a trailing byte", func(t *testing.T) {
		g := startGateway(t, newTestSim(nil))
		g.start(defaultConfig())
		f := encoded(t, g.simInput(t, "echo"))
		binary.BigEndian.PutUint32(f, binary.BigEndian.Uint32(f)+1)
		if _, err := g.conn.Write(append(f, 0)); err != nil {
			t.Fatal(err)
		}
		if code := g.wait(); code != 1 {
			t.Fatalf("exit %d, want 1", code)
		}
	})
	t.Run("config with memory limit 1", func(t *testing.T) {
		g := startGateway(t, newTestSim(nil))
		f := encoded(t, simipc.Config{NetworkName: "x"})
		binary.BigEndian.PutUint64(f[9:17], 1)
		if _, err := g.conn.Write(f); err != nil {
			t.Fatal(err)
		}
		if code := g.wait(); code != 1 {
			t.Fatalf("exit %d, want 1", code)
		}
		g.expectEOF()
	})
	t.Run("first frame is not a config", func(t *testing.T) {
		g := startGateway(t, newTestSim(nil))
		g.send(simipc.Unavailable{})
		if code := g.wait(); code != 1 {
			t.Fatalf("exit %d, want 1", code)
		}
	})
}

func TestSimulateNoSimulator(t *testing.T) {
	t.Run("nil newSim", func(t *testing.T) {
		g := startGateway(t, nil)
		g.send(defaultConfig())
		if code := g.wait(); code != ipc.ExitConfig {
			t.Fatalf("exit %d, want %d", code, ipc.ExitConfig)
		}
		g.expectEOF()
	})
	t.Run("failing newSim", func(t *testing.T) {
		g := startGateway(t, func(SimConfig, CodeFetcher) (domain.Domain, error) { return nil, errors.New("no") })
		g.send(defaultConfig())
		if code := g.wait(); code != ipc.ExitConfig {
			t.Fatalf("exit %d, want %d", code, ipc.ExitConfig)
		}
		g.expectEOF()
	})
	t.Run("newSim calls its fetcher", func(t *testing.T) {
		var got error
		g := startGateway(t, func(_ SimConfig, code CodeFetcher) (domain.Domain, error) {
			_, _, got = code(types.Hash{1})
			return nil, got
		})
		g.send(defaultConfig())
		if code := g.wait(); code != ipc.ExitConfig {
			t.Fatalf("exit %d, want %d", code, ipc.ExitConfig)
		}
		g.expectEOF()
		if got == nil || got.Error() != "code fetch outside a simulation" {
			t.Fatalf("fetch error = %v", got)
		}
	})
}

func TestDispatch(t *testing.T) {
	run := func(t *testing.T, first func(net.Conn)) (code int, plugins, sims int) {
		t.Helper()
		restoreMemoryLimit(t)
		a, b := net.Pipe()
		t.Cleanup(func() { a.Close() })
		newPlugin := func(ipc.WorkerConfig) (domain.Domain, error) {
			plugins++
			return nil, errors.New("stop")
		}
		newSim := func(SimConfig, CodeFetcher) (domain.Domain, error) {
			sims++
			return nil, errors.New("stop")
		}
		done := make(chan int, 1)
		go func() {
			c := dispatch(context.Background(), b, newPlugin, newSim)
			b.Close()
			done <- c
		}()
		first(a)
		select {
		case code = <-done:
		case <-time.After(10 * time.Second):
			t.Fatal("dispatch did not return")
		}
		return code, plugins, sims
	}

	t.Run("executor config", func(t *testing.T) {
		code, plugins, sims := run(t, func(c net.Conn) {
			if err := ipc.WriteMessage(c, validWorkerConfig(t.TempDir())); err != nil {
				t.Error(err)
			}
		})
		if code != ipc.ExitConfig || plugins != 1 || sims != 0 {
			t.Fatalf("exit %d, newPlugin %d, newSim %d", code, plugins, sims)
		}
	})
	t.Run("simulator config", func(t *testing.T) {
		code, plugins, sims := run(t, func(c net.Conn) {
			if err := simipc.WriteMessage(c, defaultConfig()); err != nil {
				t.Error(err)
			}
		})
		if code != ipc.ExitConfig || plugins != 0 || sims != 1 {
			t.Fatalf("exit %d, newPlugin %d, newSim %d", code, plugins, sims)
		}
	})
	t.Run("empty stream", func(t *testing.T) {
		code, plugins, sims := run(t, func(c net.Conn) { c.Close() })
		if code != ipc.ExitConfig || plugins != 0 || sims != 0 {
			t.Fatalf("exit %d, newPlugin %d, newSim %d", code, plugins, sims)
		}
	})
	t.Run("short stream", func(t *testing.T) {
		code, plugins, sims := run(t, func(c net.Conn) {
			c.Write([]byte{0, 0, 0})
			c.Close()
		})
		if code != ipc.ExitConfig || plugins != 0 || sims != 0 {
			t.Fatalf("exit %d, newPlugin %d, newSim %d", code, plugins, sims)
		}
	})
}
