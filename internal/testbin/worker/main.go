// Command worker is a domain worker for the supervisor's process
// tests and the gateway's simulator tests, not a real domain. As an
// executor worker it runs testDomain; in simulate mode it runs
// simDomain.
package main

import (
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/zenon-network/go-zenon/common/types"

	"github.com/digitalSloth/go-executor/domain"
	"github.com/digitalSloth/go-executor/internal/codec"
	"github.com/digitalSloth/go-executor/worker"
)

// testDomain is a minimal, deterministic domain.Domain whose Apply can be told, through its
// input payload, to allocate memory or sleep. Both are deliberate test side effects: the leaf
// value Apply writes is still a pure function of the input.
type testDomain struct{}

type testInput struct {
	idx uint64
	raw []byte
	cmd string
}

// retained holds every slice an "alloc:" command allocates, so it survives past the Apply call
// that allocated it, for a memory-limit test to observe.
var retained [][]byte

func (testDomain) Genesis() domain.StateInit { return domain.StateInit{} }

func (testDomain) DecodeInput(ctx domain.InputContext, payload []byte) (domain.Input, error) {
	in, err := codec.DecodeInput(payload)
	if err != nil {
		return nil, err
	}
	args, ok := in.Args.(codec.CallL2Args)
	if !ok {
		return nil, fmt.Errorf("testbin worker: input kind %T is not CallL2Args", in.Args)
	}
	return testInput{idx: ctx.GlobalInputIndex, raw: append([]byte(nil), payload...), cmd: string(args.Payload)}, nil
}

func (testDomain) Apply(_ domain.StateAccess, in domain.Input) (domain.Effects, error) {
	ti := in.(testInput)
	if err := applyCmd(ti.cmd); err != nil {
		return domain.Effects{}, err
	}

	var path domain.Path
	binary.BigEndian.PutUint64(path[:8], ti.idx)
	return domain.Effects{
		Status:    codec.StatusSuccess,
		StateDiff: []domain.StateDiffEntry{{Path: path, Value: ti.raw}},
	}, nil
}

// applyCmd acts on one of two prefixes; any other cmd, including the empty string, does
// nothing.
func applyCmd(cmd string) error {
	switch {
	case strings.HasPrefix(cmd, "alloc:"):
		n, err := strconv.Atoi(strings.TrimPrefix(cmd, "alloc:"))
		if err != nil {
			return fmt.Errorf("testbin worker: bad alloc amount %q: %w", cmd, err)
		}
		for i := 0; i < n; i++ {
			b := make([]byte, 1<<20)
			for j := 0; j < len(b); j += 4096 {
				b[j] = 1
			}
			retained = append(retained, b)
		}
	case strings.HasPrefix(cmd, "sleep:"):
		ms, err := strconv.Atoi(strings.TrimPrefix(cmd, "sleep:"))
		if err != nil {
			return fmt.Errorf("testbin worker: bad sleep duration %q: %w", cmd, err)
		}
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
	return nil
}

// simDomain is the simulate-mode domain of the gateway's simulator
// tests: a CallL2 payload is a command, and an input without one
// echoes the input and its context.
type simDomain struct{ fetch worker.CodeFetcher }

type simInput struct {
	ctx    domain.InputContext
	raw    []byte
	target types.Hash
	cmd    string
}

func (simDomain) Genesis() domain.StateInit { return domain.StateInit{} }

func (simDomain) DecodeInput(ctx domain.InputContext, payload []byte) (domain.Input, error) {
	in, err := codec.DecodeInput(payload)
	if err != nil {
		return nil, err
	}
	si := simInput{ctx: ctx, raw: append([]byte(nil), payload...)}
	switch a := in.Args.(type) {
	case codec.CallL2Args:
		si.target, si.cmd = a.TargetContract, string(a.Payload)
	case codec.DepositArgs:
		si.target = a.TargetContract
	default:
		return nil, fmt.Errorf("testbin simulator: input kind %T is not CallL2Args or DepositArgs", in.Args)
	}
	return si, nil
}

// errSimFail is the error of the "fail" command.
var errSimFail = errors.New("testbin simulator: fail")

func (d simDomain) Apply(state domain.StateAccess, in domain.Input) (domain.Effects, error) {
	si := in.(simInput)
	cmd, arg, _ := strings.Cut(si.cmd, ":")
	switch cmd {
	case "sleep":
		ms, err := strconv.Atoi(arg)
		if err != nil {
			return domain.Effects{}, fmt.Errorf("testbin simulator: bad sleep duration %q: %w", arg, err)
		}
		time.Sleep(time.Duration(ms) * time.Millisecond)
	case "exit":
		code, err := strconv.Atoi(arg)
		if err != nil {
			return domain.Effects{}, fmt.Errorf("testbin simulator: bad exit code %q: %w", arg, err)
		}
		os.Exit(code)
	case "panic":
		panic("testbin simulator: panic")
	case "fail":
		return domain.Effects{}, errSimFail
	case "badutf8":
		return domain.Effects{}, errors.New("testbin simulator: bad text \xff\xfe")
	case "read":
		path, err := simPath(arg)
		if err != nil {
			return domain.Effects{}, err
		}
		v, _, err := state.Read(path)
		if err != nil {
			return domain.Effects{}, err
		}
		return domain.Effects{Status: codec.StatusSuccess, StateDiff: []domain.StateDiffEntry{{Path: path, Value: v}}}, nil
	case "reads", "same":
		n, err := strconv.Atoi(arg)
		if err != nil {
			return domain.Effects{}, fmt.Errorf("testbin simulator: bad read count %q: %w", arg, err)
		}
		for i := 0; i < n; i++ {
			var path domain.Path
			if cmd == "reads" {
				binary.BigEndian.PutUint64(path[:8], uint64(i))
			}
			if _, _, err := state.Read(path); err != nil {
				return domain.Effects{}, err
			}
		}
	case "caps", "caps2":
		asset, err := simPath(arg)
		if err != nil {
			return domain.Effects{}, err
		}
		var amounts []codec.U256
		for i := 0; i < len(cmd)-3; i++ {
			amounts, err = si.ctx.Caps.MaxBatchWithdrawals([]codec.AssetID{codec.AssetID(asset)})
			if err != nil {
				return domain.Effects{}, err
			}
		}
		return domain.Effects{Status: codec.StatusSuccess, ReturnData: amounts[0][:]}, nil
	case "code", "code2":
		h, err := simPath(arg)
		if err != nil {
			return domain.Effects{}, err
		}
		var module []byte
		found := false
		for i := 0; i < len(cmd)-3; i++ {
			module, found, err = d.fetch(types.Hash(h))
			if err != nil {
				return domain.Effects{}, err
			}
		}
		if !found {
			module = []byte("notfound")
		}
		return domain.Effects{Status: codec.StatusSuccess, ReturnData: module}, nil
	}
	return simEcho(si), nil
}

// simPath decodes 64 hex characters.
func simPath(s string) (domain.Path, error) {
	var p domain.Path
	b, err := hex.DecodeString(s)
	if err != nil || len(b) != len(p) {
		return p, fmt.Errorf("testbin simulator: %q is not 64 hex characters", s)
	}
	copy(p[:], b)
	return p, nil
}

// simEcho returns the effects of a simulation with no command: the
// input and its context come back in the diff, an event and an outbox
// message.
func simEcho(si simInput) domain.Effects {
	g := si.ctx.GlobalInputIndex
	data := binary.BigEndian.AppendUint64(nil, g)
	data = binary.BigEndian.AppendUint64(data, si.ctx.MomentumHeight)
	data = binary.BigEndian.AppendUint64(data, si.ctx.MomentumTime)
	data = append(data, si.ctx.L1Sender[:]...)
	return domain.Effects{
		Status:         codec.StatusSuccess,
		ContractID:     si.target,
		StateDiff:      []domain.StateDiffEntry{{Path: domain.Path{0x01}, Value: si.raw}},
		Events:         []codec.Event{{ContractID: si.target, InputIndex: g, EventIndex: 0, Data: data}},
		OutboxMessages: []codec.OutboxMessage{{SourceContract: si.target, InputIndex: g, OutboxIndex: 0, Target: codec.L2Delivery{TargetContract: si.target}, Payload: []byte("echo")}},
		ReturnData:     []byte("echo"),
	}
}

func main() {
	worker.Main(
		func(worker.Config) (domain.Domain, error) { return testDomain{}, nil },
		func(_ worker.SimConfig, code worker.CodeFetcher) (domain.Domain, error) {
			return simDomain{fetch: code}, nil
		},
	)
}
