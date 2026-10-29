// Package supervisor is the supervisor: configuration, the submission key, the restart policy,
// worker channel serving, and spawning and supervising one worker process per configured
// domain.
package supervisor

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"

	"github.com/zenon-network/go-zenon/common/types"

	"github.com/Zeno-L2/zeno-executor/internal/ipc"
	"github.com/Zeno-L2/zeno-executor/internal/recovery"
)

// ErrConfig marks an invalid supervisor configuration file or submission key: a missing,
// malformed or out-of-range field, rejected before any worker starts.
var ErrConfig = errors.New("supervisor: invalid configuration")

// defaultMaxPendingBatches is the maxPendingBatches an absent field takes.
const defaultMaxPendingBatches = 8

// maxPendingBatchesMax is the largest accepted maxPendingBatches.
const maxPendingBatchesMax = 64

// cpuLimitMin is the smallest cpuLimit the kernel's cpu.max accepts: workerproc_linux.go writes
// a fixed 100000us period, and the kernel rejects a quota under 1000us, i.e. under 0.01 CPUs.
const cpuLimitMin = 0.01

// cpuLimitMax bounds cpuLimit well below where round(cpuLimit*100000) could overflow the int64
// conversion in workerproc_linux.go's cpu.max write, and well above any host's real core count.
const cpuLimitMax = 1024.0

// Config is the supervisor's boot configuration, decoded from one JSON
// file with DisallowUnknownFields: a binary requires every field it
// consumes and rejects any field it does not, so no setting is silently
// ignored.
type Config struct {
	ExecutorAddress     types.Address  `json:"executorAddress"`
	NetworkName         string         `json:"networkName"`
	PillarName          string         `json:"pillarName"`
	ZnndEndpoint        string         `json:"znndEndpoint"`
	ConfirmationDepth   uint64         `json:"confirmationDepth"`
	StoragePath         string         `json:"storagePath"`
	SubmissionKeySource KeySource      `json:"submissionKeySource"`
	MaxPendingBatches   uint64         `json:"maxPendingBatches"`
	Workers             []WorkerConfig `json:"workers"`
}

// KeySource names where the submission key comes from. See LoadKey.
type KeySource struct {
	Kind string `json:"kind"`           // FILE | ENV | KMS
	Path string `json:"path,omitempty"` // FILE
	Name string `json:"name,omitempty"` // ENV: the variable holding the key
}

// WorkerConfig is one configured domain worker.
type WorkerConfig struct {
	DomainID    uint32     `json:"domainId"`
	Binary      string     `json:"binary"`
	STFSpecHash types.Hash `json:"stfSpecHash"`
	MemoryLimit int64      `json:"memoryLimit"` // bytes
	CPULimit    float64    `json:"cpuLimit"`    // CPUs
}

// LoadConfig reads, strictly decodes and validates the configuration at path. Every error,
// including a failure to open or decode the file, wraps ErrConfig and names the field as JSON
// spells it (and, for a worker field, its index), in the style of runtime.Config.validate
// (runtime/runtime.go:72-101).
//
// encoding/json's field matching is case-insensitive and, for a JSON object with a duplicate
// key, silently keeps the value of the last occurrence: neither Decode nor this function reports
// that as an error.
func LoadConfig(path string) (Config, error) {
	f, err := os.Open(path)
	if err != nil {
		return Config{}, fmt.Errorf("%w: %w", ErrConfig, err)
	}
	defer f.Close()

	dec := json.NewDecoder(f)
	dec.DisallowUnknownFields()
	cfg := Config{MaxPendingBatches: defaultMaxPendingBatches}
	if err := dec.Decode(&cfg); err != nil {
		return Config{}, fmt.Errorf("%w: %w", ErrConfig, err)
	}
	// A second Decode must hit end of file: anything else is a trailing value after the one
	// configuration object the file may hold.
	var trailing json.RawMessage
	if err := dec.Decode(&trailing); err != io.EOF {
		if err == nil {
			return Config{}, fmt.Errorf("%w: unexpected value after the configuration object", ErrConfig)
		}
		return Config{}, fmt.Errorf("%w: %w", ErrConfig, err)
	}

	if cfg.ExecutorAddress.IsZero() {
		return Config{}, fmt.Errorf("%w: executorAddress is required", ErrConfig)
	}
	if cfg.NetworkName == "" {
		return Config{}, fmt.Errorf("%w: networkName is required", ErrConfig)
	}
	if cfg.PillarName == "" {
		return Config{}, fmt.Errorf("%w: pillarName is required", ErrConfig)
	}
	if u, err := url.Parse(cfg.ZnndEndpoint); err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return Config{}, fmt.Errorf("%w: znndEndpoint %q is not an absolute http or https URL", ErrConfig, cfg.ZnndEndpoint)
	}
	if cfg.ConfirmationDepth < 1 || cfg.ConfirmationDepth > recovery.MaxConfirmationDepth {
		return Config{}, fmt.Errorf("%w: confirmationDepth %d is outside [1, %d]", ErrConfig, cfg.ConfirmationDepth, recovery.MaxConfirmationDepth)
	}
	if cfg.MaxPendingBatches < 1 || cfg.MaxPendingBatches > maxPendingBatchesMax {
		return Config{}, fmt.Errorf("%w: maxPendingBatches %d is outside [1, %d]", ErrConfig, cfg.MaxPendingBatches, maxPendingBatchesMax)
	}
	if !filepath.IsAbs(cfg.StoragePath) {
		return Config{}, fmt.Errorf("%w: storagePath %q is not absolute", ErrConfig, cfg.StoragePath)
	}
	if cfg.SubmissionKeySource.Kind == "FILE" && cfg.SubmissionKeySource.Path != "" && !filepath.IsAbs(cfg.SubmissionKeySource.Path) {
		// checkWorkerAccess's domain-directory containment check compares this path, resolved,
		// against always-absolute domain directories; a relative path would never match and so
		// would never be caught as living inside one.
		return Config{}, fmt.Errorf("%w: submissionKeySource path %q is not absolute", ErrConfig, cfg.SubmissionKeySource.Path)
	}
	if len(cfg.Workers) == 0 {
		return Config{}, fmt.Errorf("%w: workers is empty", ErrConfig)
	}

	seen := make(map[uint32]bool, len(cfg.Workers))
	for i, w := range cfg.Workers {
		if seen[w.DomainID] {
			return Config{}, fmt.Errorf("%w: workers[%d].domainId %d is already used by an earlier entry", ErrConfig, i, w.DomainID)
		}
		seen[w.DomainID] = true

		if !filepath.IsAbs(w.Binary) {
			return Config{}, fmt.Errorf("%w: workers[%d].binary %q is not absolute", ErrConfig, i, w.Binary)
		}
		fi, err := os.Stat(w.Binary)
		if err != nil {
			return Config{}, fmt.Errorf("%w: workers[%d].binary: %w", ErrConfig, i, err)
		}
		if !fi.Mode().IsRegular() {
			return Config{}, fmt.Errorf("%w: workers[%d].binary %q is not a regular file", ErrConfig, i, w.Binary)
		}
		if fi.Mode().Perm()&0o111 == 0 {
			return Config{}, fmt.Errorf("%w: workers[%d].binary %q is not executable", ErrConfig, i, w.Binary)
		}
		if w.STFSpecHash.IsZero() {
			return Config{}, fmt.Errorf("%w: workers[%d].stfSpecHash is required", ErrConfig, i)
		}
		if w.MemoryLimit <= 0 {
			return Config{}, fmt.Errorf("%w: workers[%d].memoryLimit %d must be positive", ErrConfig, i, w.MemoryLimit)
		}
		if w.CPULimit < cpuLimitMin || w.CPULimit > cpuLimitMax {
			return Config{}, fmt.Errorf("%w: workers[%d].cpuLimit %v is outside [%v, %v]", ErrConfig, i, w.CPULimit, cpuLimitMin, cpuLimitMax)
		}
	}

	// Every worker's channel Config frame must fit the wire caps (internal/ipc/messages.go:44-46),
	// checked last and separately so every structural field error above is reported first.
	for i, w := range cfg.Workers {
		if _, err := ipc.Encode(cfg.workerConfig(w)); err != nil {
			return Config{}, fmt.Errorf("%w: workers[%d] cannot be sent as a channel Config frame: %w", ErrConfig, i, err)
		}
	}

	return cfg, nil
}

// workerConfig is the channel Config frame for w: w's DomainID and
// STFSpecHash, plus c's ExecutorAddress, NetworkName,
// ConfirmationDepth, StoragePath and MaxPendingBatches. The supervisor
// sends this to the worker over its channel before serving any request.
func (c Config) workerConfig(w WorkerConfig) ipc.WorkerConfig {
	return ipc.WorkerConfig{
		DomainID:          w.DomainID,
		ExecutorAddress:   c.ExecutorAddress,
		STFSpecHash:       w.STFSpecHash,
		ConfirmationDepth: c.ConfirmationDepth,
		NetworkName:       c.NetworkName,
		StoragePath:       c.StoragePath,
		MaxPendingBatches: c.MaxPendingBatches,
	}
}
