package worker

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"os/signal"
	"runtime/debug"
	"syscall"

	"github.com/zenon-network/go-zenon/common/types"

	"github.com/digitalSloth/go-executor/domain"
	"github.com/digitalSloth/go-executor/internal/batch"
	"github.com/digitalSloth/go-executor/internal/codec"
	"github.com/digitalSloth/go-executor/internal/ipc"
	"github.com/digitalSloth/go-executor/internal/simipc"
)

// simulate serves the gateway's simulations over conn until the gateway
// closes it, returning the process exit status. r wraps conn and may
// already hold the Config frame's bytes. It holds no persistent state:
// each Simulate runs one input against state, code and caps the gateway
// answers over the channel.
func simulate(ctx context.Context, r *bufio.Reader, conn io.ReadWriteCloser, newSim NewSimulator) int {
	// Descriptor 3 is blocking, so closing it cannot interrupt an idle
	// read. A signal ends the process at once instead: a simulator
	// holds nothing to flush.
	signal.Reset(syscall.SIGINT, syscall.SIGTERM)
	stop := context.AfterFunc(ctx, func() { conn.Close() })
	defer stop()
	log.SetPrefix("simulator: ")

	msg, err := simipc.ReadFromGateway(r)
	if err != nil {
		log.Printf("read config: %v", err)
		return 1
	}
	cfg, ok := msg.(simipc.Config)
	if !ok {
		log.Printf("read config: first frame is type 0x%02x", uint8(msg.Type()))
		return 1
	}
	log.SetPrefix(fmt.Sprintf("simulator %d: ", cfg.DomainID))
	limit := cfg.MemoryLimit
	if limit == 0 {
		limit = simipc.DefaultMemoryLimit
	}
	debug.SetMemoryLimit(int64(limit))

	if newSim == nil {
		log.Printf("this worker binary has no simulate mode")
		return ipc.ExitConfig
	}
	var cur *simChannel
	fetch := func(h types.Hash) ([]byte, bool, error) {
		if cur == nil {
			return nil, false, errors.New("code fetch outside a simulation")
		}
		return cur.code(h)
	}
	plugin, err := newSim(cfg, fetch)
	if err != nil {
		log.Printf("new simulator: %v", err)
		return ipc.ExitConfig
	}
	if err := simipc.WriteMessage(conn, simipc.Ready{}); err != nil {
		log.Printf("write ready: %v", err)
		return 1
	}

	for {
		msg, err := simipc.ReadFromGateway(r)
		if err == io.EOF || (err != nil && ctx.Err() != nil) {
			return 0
		}
		if err != nil {
			log.Printf("read: %v", err)
			return 1
		}
		sim, ok := msg.(simipc.Simulate)
		if !ok {
			log.Printf("unexpected frame type 0x%02x between simulations", uint8(msg.Type()))
			return 1
		}

		ch := &simChannel{r: r, w: conn, values: map[domain.Path]simipc.Value{}}
		cur = ch
		out := runSimulation(plugin, ch, sim)
		cur = nil
		if ch.broken != nil {
			log.Printf("channel: %v", ch.broken)
			return 1
		}
		err = simipc.WriteMessage(conn, out)
		if _, isResult := out.(simipc.Result); isResult && errors.Is(err, simipc.ErrMalformed) {
			err = simipc.WriteMessage(conn, simipc.Failed{Message: "the result is refused: " + err.Error()})
		}
		if err != nil {
			log.Printf("write: %v", err)
			return 1
		}
	}
}

// runSimulation runs one input and returns the Result or Failed to
// send. A broken channel is left on ch, and the returned message is
// then not sent.
func runSimulation(plugin domain.Domain, ch *simChannel, sim simipc.Simulate) simipc.Message {
	in, err := codec.DecodeInput(sim.InputData)
	if err != nil {
		return simipc.Failed{Message: fmt.Sprintf("the input does not decode: %v", err)}
	}
	if k := in.Args.Kind(); k != codec.CallL2 && k != codec.Deposit {
		return simipc.Failed{Message: "simulate mode runs only CallL2 and Deposit inputs"}
	}
	decoded, err := plugin.DecodeInput(domain.InputContext{
		GlobalInputIndex: sim.GlobalInputIndex,
		MomentumHeight:   sim.MomentumHeight,
		MomentumTime:     sim.MomentumTime,
		MomentumHash:     sim.MomentumHash,
		L1Sender:         sim.L1Sender,
		Caps:             simCaps{ch},
	}, sim.InputData)
	if err != nil {
		return simipc.Failed{Message: err.Error()}
	}
	eff, err := plugin.Apply(ch, decoded)
	if err != nil {
		return simipc.Failed{Message: err.Error()}
	}

	// The batch driver halts on effects CheckEffects refuses, so a
	// simulation never answers them as a result.
	var deposit codec.U256
	switch a := in.Args.(type) {
	case codec.CallL2Args:
		deposit = a.DepositAmount
	case codec.DepositArgs:
		deposit = a.DepositAmount
	}
	if err := batch.CheckEffects(eff, deposit); err != nil {
		return simipc.Failed{Message: "the executor refuses the plugin's effects: " + err.Error()}
	}

	diff := make([]codec.StateDiffEntry, len(eff.StateDiff))
	for i, e := range eff.StateDiff {
		diff[i] = codec.StateDiffEntry{Key: e.Path, Value: e.Value}
	}
	encoded, err := codec.ExecutionResult{
		Version:          codec.ExecutionResultVersion,
		GlobalInputIndex: sim.GlobalInputIndex,
		StateDiff:        diff,
		Events:           eff.Events,
		Outbox:           eff.OutboxMessages,
		ReturnData:       eff.ReturnData,
		ClaimedDeposit:   eff.ClaimedDeposit,
	}.Encode()
	if err != nil {
		return simipc.Failed{Message: fmt.Sprintf("encode the execution result: %v", err)}
	}
	return simipc.Result{
		Status:          eff.Status,
		GasUsed:         eff.GasUsed,
		ContractID:      eff.ContractID,
		CodeHash:        eff.CodeHash,
		AbortCode:       eff.AbortCode,
		ExecutionResult: encoded,
	}
}

// simChannel is one simulation's accessor set: it asks the gateway for
// state, code and caps and reads one reply per request. The first
// channel failure is latched in broken, and every later call returns it
// without I/O. Each Value read is kept for the simulation, so a path is
// fetched at most once.
type simChannel struct {
	r      *bufio.Reader
	w      io.Writer
	broken error

	// values holds each path's Value for this simulation. It holds at
	// most simipc.MaxStateReads entries, because the gateway answers no
	// more reads with a Value.
	values map[domain.Path]simipc.Value
}

// ask sends req and returns the reply, latching any failure.
func (c *simChannel) ask(req simipc.Message) (simipc.Message, error) {
	if c.broken != nil {
		return nil, c.broken
	}
	if err := simipc.WriteMessage(c.w, req); err != nil {
		c.broken = fmt.Errorf("write request: %w", err)
		return nil, c.broken
	}
	reply, err := simipc.ReadFromGateway(c.r)
	if err != nil {
		c.broken = fmt.Errorf("read reply: %w", err)
		return nil, c.broken
	}
	return reply, nil
}

func (c *simChannel) wrongReply(reply simipc.Message) error {
	c.broken = fmt.Errorf("unexpected reply type 0x%02x", uint8(reply.Type()))
	return c.broken
}

// Read implements domain.StateAccess.
func (c *simChannel) Read(path domain.Path) ([]byte, bool, error) {
	if c.broken != nil {
		return nil, false, c.broken
	}
	v, ok := c.values[path]
	if !ok {
		reply, err := c.ask(simipc.ReadRequest{Path: path})
		if err != nil {
			return nil, false, err
		}
		switch r := reply.(type) {
		case simipc.Value:
			v = r
			c.values[path] = v
		case simipc.Unavailable:
			return nil, false, fmt.Errorf("state read of %x is unavailable", path[:])
		default:
			return nil, false, c.wrongReply(reply)
		}
	}
	if !v.Present {
		return nil, false, nil
	}
	return v.Value, true, nil
}

// Len implements domain.StateAccess.
func (c *simChannel) Len(path domain.Path) (int, bool, error) {
	v, present, err := c.Read(path)
	return len(v), present, err
}

// code asks for the as-deployed module with hash h.
func (c *simChannel) code(h types.Hash) ([]byte, bool, error) {
	reply, err := c.ask(simipc.CodeRequest{SourceCodeHash: h})
	if err != nil {
		return nil, false, err
	}
	switch v := reply.(type) {
	case simipc.Code:
		return v.Module, v.Found, nil
	case simipc.Unavailable:
		return nil, false, fmt.Errorf("code %x is unavailable", h[:])
	default:
		return nil, false, c.wrongReply(reply)
	}
}

// simCaps is the domain.CapLookup of one simulation.
type simCaps struct{ c *simChannel }

// MaxBatchWithdrawals asks the gateway for one cap per asset, in assets
// order.
func (s simCaps) MaxBatchWithdrawals(assets []codec.AssetID) ([]codec.U256, error) {
	reply, err := s.c.ask(simipc.CapRequest{Assets: assets})
	if err != nil {
		return nil, err
	}
	switch v := reply.(type) {
	case simipc.Caps:
		if len(v.Amounts) != len(assets) {
			s.c.broken = fmt.Errorf("caps reply holds %d amounts for %d assets", len(v.Amounts), len(assets))
			return nil, s.c.broken
		}
		return v.Amounts, nil
	case simipc.Unavailable:
		return nil, errors.New("withdrawal caps are unavailable")
	default:
		return nil, s.c.wrongReply(reply)
	}
}
