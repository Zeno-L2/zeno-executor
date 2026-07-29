// Command executor runs the off-chain executor binary for one execution domain.
package main

import (
	"context"
	"flag"
	"log"
	"os/signal"
	"syscall"

	"github.com/digitalSloth/go-executor/plugins/wasm"
	"github.com/digitalSloth/go-executor/runtime"
)

func main() {
	executorID := flag.String("executor-id", "", "bonded executor identity; keys the single-instance lease")
	storagePath := flag.String("storage-path", "./data", "root directory for on-disk state")
	domainID := flag.Uint("domain-id", 0, "execution domain served by this process")
	flag.Parse()

	if *executorID == "" {
		log.Fatal("executor: -executor-id is required")
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	exec := runtime.New(runtime.Config{
		DomainID:    uint32(*domainID),
		ExecutorID:  *executorID,
		StoragePath: *storagePath,
	})
	exec.Register(wasm.New())

	if err := exec.Run(ctx); err != nil {
		log.Fatal(err)
	}
}
