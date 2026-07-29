// Package runtime is the generic, domain-agnostic executor process: it owns the registered
// domain plugins and the single-instance lease, and drives them through Run. Everything
// except the state-transition function is written once here; a domain contributes only
// what domain.Domain requires.
package runtime

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"

	"github.com/digitalSloth/go-executor/domain"
	"github.com/digitalSloth/go-executor/internal/lease"
)

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
}

func (c Config) validate() error {
	if c.ExecutorID == "" {
		return fmt.Errorf("runtime: executorId is required")
	}
	if c.StoragePath == "" {
		return fmt.Errorf("runtime: storagePath is required")
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

// Run acquires the single-instance lease for cfg.ExecutorID, then blocks until ctx is
// cancelled, shutting down cleanly. A non-holder of the lease returns an error before ever
// entering the execution loop.
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

	log.Printf("runtime: executor %q registered domain %d (%T), ready", r.cfg.ExecutorID, r.cfg.DomainID, d)

	<-ctx.Done()

	log.Printf("runtime: shutting down")
	return nil
}
