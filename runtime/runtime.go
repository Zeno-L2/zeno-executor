// Package runtime is the generic, domain-agnostic executor process: it owns the registered
// domain plugins and the per-domain lock, and drives them through Run. Everything
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
	"syscall"

	"github.com/zenon-network/go-zenon/common/types"

	"github.com/digitalSloth/go-executor/domain"
	"github.com/digitalSloth/go-executor/internal/codec"
	"github.com/digitalSloth/go-executor/internal/lease"
	"github.com/digitalSloth/go-executor/internal/ledger"
	"github.com/digitalSloth/go-executor/internal/recovery"
)

var (
	// ErrConfig marks an invalid Config: a missing or malformed field, rejected before boot.
	ErrConfig = errors.New("runtime: invalid configuration")
	// ErrLocked marks a domain whose worker.lock is already held by another process.
	ErrLocked = errors.New("runtime: domain lock held by another process")
)

// roleExecutor is the only EXECUTOR §17 role this runtime implements.
const roleExecutor = "EXECUTOR"

// Config is the executor's boot configuration. Every field is validated before the run loop
// starts. Run rejects any missing or malformed value with an error wrapping ErrConfig rather
// than defaulting silently.
type Config struct {
	// DomainID is the execution domain this process serves.
	DomainID uint32
	// ExecutorID is the bonded identity recorded in meta.json and in every batch commitment.
	// It keys no lock.
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
	// Assignments answers Settlement's input assignment for DomainID. An error wrapping
	// recovery.ErrHalted halts Run; any other error is retried as a source outage.
	Assignments ledger.AssignmentSource
	// Caps answers Settlement's withdrawal caps for DomainID. An error wrapping
	// recovery.ErrHalted halts Run; any other error ends Run at once with
	// recovery.ErrSourceUnavailable, and a restart re-derives from the last commit.
	Caps    ledger.CapSource
	Anchors recovery.AnchorSource
	// Retry is the bounded-retry schedule; the zero value is the production one.
	Retry recovery.RetryPolicy
	// OnCommit, when set, is called on the loop goroutine after every successful
	// recovery.Commit, with that commit's batch commitments in batch order. The cursor already
	// covers them. An error ends Run with that error, unhalted, and the batches stay committed.
	OnCommit func([]codec.BatchCommitment) error
}

func (c Config) validate() error {
	if c.ExecutorID == "" {
		return fmt.Errorf("%w: executorId is required", ErrConfig)
	}
	if _, err := types.ParseAddress(c.ExecutorID); err != nil {
		return fmt.Errorf("%w: executorId %q is not an address: %w", ErrConfig, c.ExecutorID, err)
	}
	if c.StoragePath == "" {
		return fmt.Errorf("%w: storagePath is required", ErrConfig)
	}
	if c.Role != roleExecutor {
		return fmt.Errorf("%w: role %q is not implemented; only %s is", ErrConfig, c.Role, roleExecutor)
	}
	if c.STFSpecHash.IsZero() {
		return fmt.Errorf("%w: stfSpecHash is required", ErrConfig)
	}
	if c.ConfirmationDepth < 1 || c.ConfirmationDepth > recovery.MaxConfirmationDepth {
		return fmt.Errorf("%w: confirmationDepth %d is outside [1, %d] (recovery.MaxConfirmationDepth)", ErrConfig, c.ConfirmationDepth, recovery.MaxConfirmationDepth)
	}
	if c.Momenta == nil {
		return fmt.Errorf("%w: momentum source is required", ErrConfig)
	}
	if c.Assignments == nil {
		return fmt.Errorf("%w: assignment source is required", ErrConfig)
	}
	if c.Caps == nil {
		return fmt.Errorf("%w: withdrawal cap source is required", ErrConfig)
	}
	if c.Anchors == nil {
		return fmt.Errorf("%w: anchor source is required", ErrConfig)
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

// Run locks <StoragePath>/<DomainID>/worker.lock for its lifetime, boots the recovery machine,
// and then drives the execution loop until ctx is cancelled, shutting down cleanly. A lock
// already held by another process returns an error wrapping ErrLocked before boot.
// Configuration errors wrap ErrConfig. Consistency faults wrap recovery.ErrHalted. A source
// still unavailable after its retry budget wraps recovery.ErrSourceUnavailable, and so does a
// withdrawal-cap outage. Any other error, such as a failed batch commit or an OnCommit error, is
// transient.
func (r *Runtime) Run(ctx context.Context) error {
	if err := r.cfg.validate(); err != nil {
		return err
	}

	d, ok := r.domains[r.cfg.DomainID]
	if !ok {
		return fmt.Errorf("%w: no domain registered for domainId %d", ErrConfig, r.cfg.DomainID)
	}

	if err := os.MkdirAll(r.cfg.StoragePath, 0o755); err != nil {
		return fmt.Errorf("runtime: create storage path %s: %w", r.cfg.StoragePath, err)
	}

	layout, err := recovery.NewLayout(r.cfg.StoragePath, r.cfg.DomainID)
	if err != nil {
		return fmt.Errorf("runtime: %w", err)
	}

	lock, err := lease.Acquire(filepath.Join(layout.Root, "worker.lock"))
	if err != nil {
		if errors.Is(err, syscall.EWOULDBLOCK) {
			return fmt.Errorf("%w: %w", ErrLocked, err)
		}
		return fmt.Errorf("runtime: %w", err)
	}
	defer lock.Release()

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

	log.Printf("runtime: shutting down")
	return err
}
