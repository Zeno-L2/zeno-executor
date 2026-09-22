// Command executor runs the off-chain executor binary for one execution domain.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os/signal"
	"path/filepath"
	"syscall"

	"github.com/zenon-network/go-zenon/chain/nom"
	"github.com/zenon-network/go-zenon/common/types"

	"github.com/digitalSloth/go-executor/internal/ledger"
	"github.com/digitalSloth/go-executor/internal/recovery"
	"github.com/digitalSloth/go-executor/plugins/wasm"
	"github.com/digitalSloth/go-executor/runtime"
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
	executorID := flag.String("executor-id", "", "bonded executor identity; keys the single-instance lease")
	storagePath := flag.String("storage-path", "./data", "root directory for on-disk state")
	domainID := flag.Uint("domain-id", 0, "execution domain served by this process")
	network := flag.String("network", "", "network name (\"mainnet\" | \"testnet\" | a named devnet); fixes the WASM domain's chainID")
	role := flag.String("role", "", "process role (\"EXECUTOR\"); only EXECUTOR is implemented")
	stfSpecHash := flag.String("stf-spec-hash", "", "hex hash identifying the state-transition function this build implements; tags every snapshot")
	confirmationDepth := flag.Uint("confirmation-depth", 0, "momenta behind the frontier treated as confirmed")
	flag.Parse()

	if *executorID == "" {
		log.Fatal("executor: -executor-id is required")
	}
	if *network == "" {
		log.Fatal("executor: -network is required")
	}
	if *role == "" {
		log.Fatal("executor: -role is required")
	}
	if *stfSpecHash == "" {
		log.Fatal("executor: -stf-spec-hash is required")
	}
	specHash, err := types.HexToHash(*stfSpecHash)
	if err != nil {
		log.Fatalf("executor: -stf-spec-hash: %v", err)
	}
	if *confirmationDepth == 0 {
		log.Fatal("executor: -confirmation-depth is required")
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	exec := runtime.New(runtime.Config{
		DomainID:    uint32(*domainID),
		ExecutorID:  *executorID,
		StoragePath: *storagePath,

		Role:              *role,
		STFSpecHash:       specHash,
		ConfirmationDepth: uint64(*confirmationDepth),
		Momenta:           unwiredMomentumSource{},
		Filter:            ledger.UnwiredFilter{},
		Anchors:           unwiredAnchorSource{},
	})

	plugin, err := wasm.New(wasm.Config{
		NetworkName:   *network,
		CodeStorePath: filepath.Join(*storagePath, "wasm-code"),
	})
	if err != nil {
		log.Fatal(err)
	}
	exec.Register(plugin)

	if err := exec.Run(ctx); err != nil {
		log.Fatal(err)
	}
}
