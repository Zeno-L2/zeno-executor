// Command executor is the executor supervisor, which starts and supervises one worker process
// per configured domain.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/zenon-network/go-zenon/chain/nom"

	"github.com/digitalSloth/go-executor/internal/ipc"
	"github.com/digitalSloth/go-executor/internal/recovery"
	"github.com/digitalSloth/go-executor/internal/supervisor"
)

// unwiredMomentumSource stands in for a znnd JSON-RPC client until one exists. Each method's
// error wraps recovery.ErrHalted: the failure is fixed at compile time, so the bounded source
// retry must not sleep through its budget on it.
type unwiredMomentumSource struct{}

func (unwiredMomentumSource) FrontierHeight() (uint64, error) {
	return 0, fmt.Errorf("%w: znnd momentum source is not wired", recovery.ErrHalted)
}

func (unwiredMomentumSource) DetailedMomentumsByHeight(height, count uint64) ([]*nom.DetailedMomentum, error) {
	return nil, fmt.Errorf("%w: znnd momentum source is not wired", recovery.ErrHalted)
}

// unwiredAnchorSource stands in for the Settlement storage reader until one exists.
type unwiredAnchorSource struct{}

func (unwiredAnchorSource) FinalizedAnchor(domainID uint32) (recovery.Anchor, bool, error) {
	return recovery.Anchor{}, false, fmt.Errorf("%w: settlement anchor read is not wired", recovery.ErrHalted)
}

func main() {
	os.Exit(run())
}

// run loads the supervisor's configuration and key, then runs it until ctx ends or it fails to
// start. It returns the process exit status.
func run() int {
	path := flag.String("config", "", "path to the supervisor configuration file")
	flag.Parse()

	cfg, err := supervisor.LoadConfig(*path)
	if err != nil {
		log.Printf("executor: %v", err)
		return exitStatus(err)
	}

	key, err := supervisor.LoadKey(cfg.SubmissionKeySource)
	if err != nil {
		log.Printf("executor: %v", err)
		return exitStatus(err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	sup := supervisor.New(cfg, key, supervisor.Sources{Momenta: unwiredMomentumSource{}, Anchors: unwiredAnchorSource{}}, supervisor.Policy{})
	err = sup.Run(ctx)
	if err != nil {
		log.Printf("executor: %v", err)
	}
	return exitStatus(err)
}

// exitStatus maps Run's result to the process exit status: ipc.ExitConfig for an error wrapping
// supervisor.ErrConfig, 1 for any other error, 0 for nil.
func exitStatus(err error) int {
	switch {
	case err == nil:
		return 0
	case errors.Is(err, supervisor.ErrConfig):
		return ipc.ExitConfig
	default:
		return 1
	}
}
