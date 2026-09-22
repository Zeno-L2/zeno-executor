// Package runtime is the generic, domain-agnostic executor process: it owns the registered
// domain plugins and the single-instance lease, and drives them through Run. Everything
// except the state-transition function is written once here; a domain contributes only
// what domain.Domain requires.
package runtime

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"

	"github.com/zenon-network/go-zenon/common/types"

	"github.com/digitalSloth/go-executor/domain"
	"github.com/digitalSloth/go-executor/internal/lease"
	"github.com/digitalSloth/go-executor/internal/ledger"
	"github.com/digitalSloth/go-executor/internal/recovery"
)

// roleExecutor is the only EXECUTOR §17 role this runtime implements.
const roleExecutor = "EXECUTOR"

// Config is the executor's boot configuration. Every field is validated before the run loop
// starts; the runtime halts on any missing or malformed value rather than defaulting
// silently.
type Config struct {
	// DomainID is the execution domain this process serves.
	DomainID uint32
	// ExecutorID is this process's bonded identity; the single-instance lease is keyed by
	// it, so two processes sharing an ExecutorID can never run concurrently.
	ExecutorID string
	// StoragePath is the root directory for this process's on-disk state.
	StoragePath string
	// Role is EXECUTOR | WATCHER | RELAY (EXECUTOR §17). Only EXECUTOR is implemented; the
	// runtime refuses to start under any other, rather than running an executor's loop under
	// another role's name.
	Role string
	// STFSpecHash identifies the state-transition function this build implements. It tags every
	// snapshot, and a snapshot tagged with another value is never loaded (§16).
	STFSpecHash types.Hash
	// ConfirmationDepth is how many momenta behind the frontier are treated as confirmed
	// (SPEC §4.3). It has no default: §17 makes every field required and validated, and the
	// depth is the parameter that bounds how deep a reorg may be before it is an L1 fault.
	ConfirmationDepth uint64

	// Wired by the binary, not by the operator's configuration file.
	Momenta ledger.MomentumSource
	// Filter must be stateless across ledger.Follower instances: Run hands the same instance to
	// the follower it builds after every rollback.
	Filter  ledger.SettlementFilter
	Anchors recovery.AnchorSource
	// Retry is the bounded-retry schedule; the zero value is the production one.
	Retry recovery.RetryPolicy
}

func (c Config) validate() error {
	if c.ExecutorID == "" {
		return fmt.Errorf("runtime: executorId is required")
	}
	if _, err := types.ParseAddress(c.ExecutorID); err != nil {
		return fmt.Errorf("runtime: executorId %q is not an address: %w", c.ExecutorID, err)
	}
	if c.StoragePath == "" {
		return fmt.Errorf("runtime: storagePath is required")
	}
	if c.Role != roleExecutor {
		return fmt.Errorf("runtime: role %q is not implemented; only %s is", c.Role, roleExecutor)
	}
	if c.STFSpecHash.IsZero() {
		return fmt.Errorf("runtime: stfSpecHash is required")
	}
	if c.ConfirmationDepth < 1 || c.ConfirmationDepth > recovery.MaxConfirmationDepth {
		return fmt.Errorf("runtime: confirmationDepth %d is outside [1, %d] (recovery.MaxConfirmationDepth)", c.ConfirmationDepth, recovery.MaxConfirmationDepth)
	}
	if c.Momenta == nil {
		return fmt.Errorf("runtime: momentum source is required")
	}
	if c.Filter == nil {
		return fmt.Errorf("runtime: settlement filter is required")
	}
	if c.Anchors == nil {
		return fmt.Errorf("runtime: anchor source is required")
	}
	return nil
}

// Runtime hosts one or more registered domain plugins and drives them through Run. Domains
// are registered statically at compile time; there is no dynamic plugin loading.
type Runtime struct {
	cfg     Config
	domains map[uint32]domain.Domain
}

// New creates a Runtime for the given configuration. Register the domains this process
// serves before calling Run.
func New(cfg Config) *Runtime {
	return &Runtime{
		cfg:     cfg,
		domains: make(map[uint32]domain.Domain),
	}
}

// Register binds a plugin to the runtime's configured domain.
func (r *Runtime) Register(d domain.Domain) {
	r.domains[r.cfg.DomainID] = d
}

// Run acquires the single-instance lease for cfg.ExecutorID, boots the recovery machine, and
// then drives the execution loop until ctx is cancelled, shutting down cleanly. A non-holder of
// the lease returns an error before ever entering the execution loop. Every consistency fault
// returns an error wrapping recovery.ErrHalted.
func (r *Runtime) Run(ctx context.Context) error {
	if err := r.cfg.validate(); err != nil {
		return err
	}

	d, ok := r.domains[r.cfg.DomainID]
	if !ok {
		return fmt.Errorf("runtime: no domain registered for domainId %d", r.cfg.DomainID)
	}

	if err := os.MkdirAll(r.cfg.StoragePath, 0o755); err != nil {
		return fmt.Errorf("runtime: create storage path %s: %w", r.cfg.StoragePath, err)
	}

	leasePath := filepath.Join(r.cfg.StoragePath, r.cfg.ExecutorID+".lock")
	l, err := lease.Acquire(leasePath)
	if err != nil {
		return fmt.Errorf("runtime: %w", err)
	}
	defer l.Release()

	layout, err := recovery.NewLayout(r.cfg.StoragePath, r.cfg.DomainID)
	if err != nil {
		return fmt.Errorf("runtime: %w", err)
	}
	bundles, err := recovery.NewBundleStore(layout.Bundles)
	if err != nil {
		return fmt.Errorf("runtime: %w", err)
	}
	executor, err := types.ParseAddress(r.cfg.ExecutorID)
	if err != nil {
		return fmt.Errorf("runtime: executorId %q is not an address: %w", r.cfg.ExecutorID, err)
	}

	rcfg := recovery.Config{
		Layout:            layout,
		DomainID:          r.cfg.DomainID,
		ExecutorID:        r.cfg.ExecutorID,
		Role:              r.cfg.Role,
		STFSpecHash:       r.cfg.STFSpecHash,
		ConfirmationDepth: r.cfg.ConfirmationDepth,
		Domain:            d,
		Momenta:           r.cfg.Momenta,
		Anchors:           r.cfg.Anchors,
		Bundles:           bundles,
		Retry:             r.cfg.Retry,
	}
	ready, err := recovery.Boot(rcfg)
	if err != nil {
		return fmt.Errorf("runtime: boot: %w", err)
	}

	log.Printf("runtime: executor %q registered domain %d (%T), ready at momentum height %d (fresh: %t)",
		r.cfg.ExecutorID, r.cfg.DomainID, d, ready.Position.MomentumHeight, ready.Fresh)

	lp := &loop{cfg: r.cfg, rcfg: rcfg, executor: executor, dom: d, bundles: bundles}
	err = lp.run(ctx, ready)
	if err != nil && !errors.Is(err, recovery.ErrHalted) {
		err = fmt.Errorf("%w: %w", recovery.ErrHalted, err)
	}

	log.Printf("runtime: shutting down")
	return err
}
