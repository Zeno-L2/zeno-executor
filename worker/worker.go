// Package worker is the domain-agnostic worker process entry point: it wraps the unchanged
// runtime.Run over a supervisor channel on descriptor 3 and translates the result into a process
// exit status the supervisor's crash-loop logic can act on. When the
// channel's first frame is an internal/simipc configuration, the worker
// runs in simulate mode instead: one input per request against state,
// code and caps read over the channel.
package worker

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/zenon-network/go-zenon/common/types"

	"github.com/digitalSloth/go-executor/domain"
	"github.com/digitalSloth/go-executor/internal/ipc"
	"github.com/digitalSloth/go-executor/internal/recovery"
	"github.com/digitalSloth/go-executor/internal/simipc"
	"github.com/digitalSloth/go-executor/runtime"
)

// Config is the worker's boot configuration, read from the supervisor channel.
type Config = ipc.WorkerConfig

// NewPlugin constructs the domain plugin this worker runs, from the configuration the
// supervisor sent.
type NewPlugin func(Config) (domain.Domain, error)

// SimConfig is a simulate-mode worker's boot configuration, read from
// the gateway channel.
type SimConfig = simipc.Config

// CodeFetcher returns the as-deployed module whose hash is
// sourceCodeHash, asking the gateway. found is false when the gateway
// holds no such module.
type CodeFetcher func(sourceCodeHash types.Hash) (module []byte, found bool, err error)

// NewSimulator constructs the domain a simulate-mode worker runs, from
// the configuration the gateway sent and a fetcher for as-deployed
// modules.
type NewSimulator func(cfg SimConfig, code CodeFetcher) (domain.Domain, error)

// retry is the runtime's source retry schedule; the zero value is production. A package
// variable so this package's tests can shorten it, as runtime/loop.go:24-27 does for
// commitInputInterval.
var retry recovery.RetryPolicy

// Main is the worker binary's entry point: it drives dispatch over the
// supervisor channel on descriptor 3 and exits with the resulting
// status. newSim may be nil, for a binary with no simulate mode.
func Main(newPlugin NewPlugin, newSim NewSimulator) {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	code := dispatch(ctx, os.NewFile(3, "supervisor"), newPlugin, newSim)
	stop() // a deferred call never runs past os.Exit
	os.Exit(code)
}

// dispatch picks the worker's mode from the channel's first frame: a
// frame whose type byte is simipc.TypeConfig runs simulate, anything
// else (including a stream too short to hold a type byte) runs the
// executor worker over the same bytes.
func dispatch(ctx context.Context, conn io.ReadWriteCloser, newPlugin NewPlugin, newSim NewSimulator) int {
	br := bufio.NewReader(conn)
	if head, err := br.Peek(5); err == nil && simipc.Type(head[4]) == simipc.TypeConfig {
		return simulate(ctx, br, conn, newSim)
	}
	return run(ctx, struct {
		io.Reader
		io.Writer
	}{br, conn}, newPlugin)
}

// run reads the worker's configuration from conn, constructs and registers the domain plugin,
// and drives runtime.Run to completion, returning the process exit status.
func run(ctx context.Context, conn io.ReadWriter, newPlugin NewPlugin) int {
	log.SetPrefix("worker: ")
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	client := ipc.NewClient(conn, cancel)

	cfg, err := client.ReadConfig()
	if err != nil {
		log.Printf("read config: %v", err)
		return ipc.ExitConfig
	}
	log.SetPrefix(fmt.Sprintf("worker %d: ", cfg.DomainID))

	plugin, err := newPlugin(cfg)
	if err != nil {
		log.Printf("new plugin: %v", err)
		return ipc.ExitConfig
	}

	rt := runtime.New(runtime.Config{
		DomainID:          cfg.DomainID,
		ExecutorID:        cfg.ExecutorAddress.String(),
		StoragePath:       cfg.StoragePath,
		Role:              cfg.Role,
		STFSpecHash:       cfg.STFSpecHash,
		ConfirmationDepth: cfg.ConfirmationDepth,
		MaxPendingBatches: cfg.MaxPendingBatches,
		Momenta:           client,
		Assignments:       client,
		Caps:              client,
		Anchors:           client,
		Cursor:            client,
		Retry:             retry,
		OnCommit:          client.ReportCommitted,
		OnRecover:         client.ReportRecovered,
	})
	rt.Register(plugin)
	err = rt.Run(ctx)

	status := exitStatus(err, client.Broken())
	if status != 0 {
		if broken := client.Broken(); broken != nil {
			log.Printf("exit %d: %v (channel: %v)", status, err, broken)
		} else {
			log.Printf("exit %d: %v", status, err)
		}
	}
	return status
}

// exitStatus maps runtime.Run's result and the channel's broken state to a process exit
// status. The first matching row wins; see the package's spec for the full table and why the
// broken row sits above ExitTempFail.
func exitStatus(runErr, broken error) int {
	switch {
	case errors.Is(runErr, runtime.ErrConfig):
		return ipc.ExitConfig
	case errors.Is(runErr, recovery.ErrHalted):
		return ipc.ExitHalted
	case broken != nil:
		return 1
	case errors.Is(runErr, recovery.ErrSourceUnavailable), errors.Is(runErr, runtime.ErrLocked):
		return ipc.ExitTempFail
	case runErr != nil:
		return 1
	default:
		return 0
	}
}
