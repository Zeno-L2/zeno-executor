// Command worker is a domain worker for the supervisor's process tests, not a real domain.
package main

import (
	"encoding/binary"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/zenon-network/go-zenon/chain/nom"

	"github.com/digitalSloth/go-executor/domain"
	"github.com/digitalSloth/go-executor/internal/codec"
	"github.com/digitalSloth/go-executor/internal/ledger"
	"github.com/digitalSloth/go-executor/worker"
)

// byAddress routes a block to the domain named by its address's first byte.
type byAddress struct{}

func (byAddress) Accept(_ *nom.Momentum, block *nom.AccountBlock) (*ledger.Envelope, error) {
	return &ledger.Envelope{DomainID: uint32(block.Address[0]), Payload: block.Data}, nil
}

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

func main() {
	worker.Main(func(worker.Config) (domain.Domain, error) { return testDomain{}, nil }, byAddress{})
}
