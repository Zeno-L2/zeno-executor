// Command executor is the executor supervisor, which starts and supervises one worker process
// per configured domain.
package main

import (
	"context"
	"errors"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/digitalSloth/go-executor/internal/ipc"
	"github.com/digitalSloth/go-executor/internal/recovery"
	"github.com/digitalSloth/go-executor/internal/settlement"
	"github.com/digitalSloth/go-executor/internal/supervisor"
	"github.com/digitalSloth/go-executor/internal/znnd"
)

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

	client := znnd.NewClient(cfg.ZnndEndpoint)
	reader := settlement.NewReader(client)
	ready := func(ctx context.Context) bool {
		check := func() error { return reader.CheckNode(cfg.ConfirmationDepth) }
		return waitForNode(ctx, cfg.ZnndEndpoint, check, supervisor.Policy{}.WithDefaults(), log.Printf)
	}
	sources := supervisor.Sources{
		Momenta:     client,
		Anchors:     settlement.NewAnchorSource(reader, cfg.ConfirmationDepth),
		Assignments: reader,
		Caps:        supervisor.NewCapCache(reader),
		Ready:       ready,
	}
	sup := supervisor.New(cfg, key, sources, supervisor.Policy{})
	err = sup.Run(ctx)
	if err != nil {
		log.Printf("executor: %v", err)
	}
	return exitStatus(err)
}

// waitForNode runs check until it returns nil (true) or ctx ends (false), waiting
// p.BackoffMin after the first failure and doubling up to p.BackoffMax. It logs
// each failure through logf. It retries every error, including one wrapping
// recovery.ErrHalted: before any worker starts, an inconsistent node is a reason
// to wait, not to stop. check runs on its own goroutine, so a signal ends the
// wait without waiting for an in-flight request.
func waitForNode(ctx context.Context, endpoint string, check func() error, p supervisor.Policy, logf func(format string, args ...any)) bool {
	delay := p.BackoffMin
	for {
		res := make(chan error, 1)
		go func() { res <- check() }()
		var err error
		select {
		case err = <-res:
		case <-ctx.Done():
			return false
		}
		if err == nil {
			return true
		}

		// The error text comes from the node, so %q escapes any control characters in it.
		if errors.Is(err, recovery.ErrHalted) {
			logf("executor: znnd node at %s returned inconsistent data: %q; retrying in %s", endpoint, err.Error(), delay)
		} else {
			logf("executor: znnd node at %s is unusable: %q; retrying in %s", endpoint, err.Error(), delay)
		}

		timer := time.NewTimer(delay)
		select {
		case <-timer.C:
		case <-ctx.Done():
			timer.Stop()
			return false
		}
		if delay > p.BackoffMax/2 {
			delay = p.BackoffMax
		} else {
			delay *= 2
		}
	}
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
