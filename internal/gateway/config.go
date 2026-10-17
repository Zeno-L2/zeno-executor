package gateway

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"

	"github.com/digitalSloth/go-executor/internal/recovery"
	"github.com/zenon-network/go-zenon/vm/constants"
)

// ErrConfig marks an invalid gateway configuration file: a missing,
// malformed or out-of-range field, rejected before any domain boots.
var ErrConfig = errors.New("gateway: invalid configuration")

// defaultListenAddress is the JSON-RPC listen address when the
// configuration leaves listenAddress empty.
const defaultListenAddress = "127.0.0.1:35999"

// Config is the gateway's boot configuration, decoded from one JSON
// file with DisallowUnknownFields, so no setting is silently ignored.
type Config struct {
	ZnndEndpoint      string           `json:"znndEndpoint"`
	NetworkName       string           `json:"networkName"`
	ConfirmationDepth uint64           `json:"confirmationDepth"`
	StoragePath       string           `json:"storagePath"`
	ListenAddress     string           `json:"listenAddress"`
	AllowedOrigins    []string         `json:"allowedOrigins"`
	Domains           []DomainConfig   `json:"domains"`
	Simulation        SimulationConfig `json:"simulation"`
}

// SimulationConfig is the simulation object of the configuration.
type SimulationConfig struct {
	Enabled       *bool  `json:"enabled"`       // nil: true after LoadConfig, off in NewServer
	MaxGas        uint64 `json:"maxGas"`        // 0: defaultSimMaxGas
	TimeoutMs     uint64 `json:"timeoutMs"`     // 0: defaultSimTimeoutMs
	MaxConcurrent int    `json:"maxConcurrent"` // 0: defaultSimConcurrent
	UID           *int   `json:"uid"`           // nil: none
}

// The simulation bounds and defaults. A zero maxGas, timeoutMs or
// maxConcurrent means the default.
const (
	defaultSimMaxGas     = 100_000_000
	defaultSimTimeoutMs  = 2_000
	maxSimTimeoutMs      = 5_000
	defaultSimConcurrent = 2
	maxSimConcurrent     = 8
	maxSimUID            = 9_999
)

// withDefaults fills a zero MaxGas, TimeoutMs and MaxConcurrent.
func (c SimulationConfig) withDefaults() SimulationConfig {
	if c.MaxGas == 0 {
		c.MaxGas = defaultSimMaxGas
	}
	if c.TimeoutMs == 0 {
		c.TimeoutMs = defaultSimTimeoutMs
	}
	if c.MaxConcurrent == 0 {
		c.MaxConcurrent = defaultSimConcurrent
	}
	return c
}

// DomainConfig is one served domain.
type DomainConfig struct {
	DomainID            uint32   `json:"domainId"`
	Source              string   `json:"source"`
	ExecutorStoragePath string   `json:"executorStoragePath"`
	Upstreams           []string `json:"upstreams"`
	WorkerBinary        string   `json:"workerBinary"`
}

// maxUpstreams bounds a REMOTE domain's upstreams, and with them the
// fetch attempts per file.
const maxUpstreams = 8

// LoadConfig reads, strictly decodes and validates the configuration
// at path. Every error wraps ErrConfig and names the field as JSON
// spells it.
//
// encoding/json's field matching is case-insensitive and, for a JSON
// object with a duplicate key, silently keeps the value of the last
// occurrence: neither Decode nor this function reports that as an
// error.
func LoadConfig(path string) (Config, error) {
	f, err := os.Open(path)
	if err != nil {
		return Config{}, fmt.Errorf("%w: %w", ErrConfig, err)
	}
	defer f.Close()

	dec := json.NewDecoder(f)
	dec.DisallowUnknownFields()
	var cfg Config
	if err := dec.Decode(&cfg); err != nil {
		return Config{}, fmt.Errorf("%w: %w", ErrConfig, err)
	}
	// A second Decode must hit end of file: anything else is a
	// trailing value after the one configuration object the file may
	// hold.
	var trailing json.RawMessage
	if err := dec.Decode(&trailing); err != io.EOF {
		if err == nil {
			return Config{}, fmt.Errorf("%w: unexpected value after the configuration object", ErrConfig)
		}
		return Config{}, fmt.Errorf("%w: %w", ErrConfig, err)
	}

	if u, err := url.Parse(cfg.ZnndEndpoint); err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return Config{}, fmt.Errorf("%w: znndEndpoint is not an absolute http or https URL", ErrConfig)
	}
	if cfg.NetworkName == "" {
		return Config{}, fmt.Errorf("%w: networkName is required", ErrConfig)
	}
	if cfg.ConfirmationDepth < 1 || cfg.ConfirmationDepth > recovery.MaxConfirmationDepth {
		return Config{}, fmt.Errorf("%w: confirmationDepth %d is outside [1, %d]", ErrConfig, cfg.ConfirmationDepth, recovery.MaxConfirmationDepth)
	}
	if !filepath.IsAbs(cfg.StoragePath) {
		return Config{}, fmt.Errorf("%w: storagePath %q is not absolute", ErrConfig, cfg.StoragePath)
	}
	if cfg.ListenAddress == "" {
		cfg.ListenAddress = defaultListenAddress
	}
	host, port, err := net.SplitHostPort(cfg.ListenAddress)
	if err != nil {
		return Config{}, fmt.Errorf("%w: listenAddress %q is not host:port", ErrConfig, cfg.ListenAddress)
	}
	if host == "" {
		return Config{}, fmt.Errorf("%w: listenAddress %q has no host; use 127.0.0.1:35999 or [::1]:35999", ErrConfig, cfg.ListenAddress)
	}
	if net.ParseIP(host) == nil {
		return Config{}, fmt.Errorf("%w: listenAddress %q: host %q is not an IP address; use 127.0.0.1:35999 or [::1]:35999", ErrConfig, cfg.ListenAddress, host)
	}
	if _, err := strconv.ParseUint(port, 10, 16); err != nil {
		return Config{}, fmt.Errorf("%w: listenAddress %q: port %q is not a number from 0 to 65535", ErrConfig, cfg.ListenAddress, port)
	}
	for i, o := range cfg.AllowedOrigins {
		if !validOrigin(o) {
			return Config{}, fmt.Errorf("%w: allowedOrigins[%d] %q is not a browser origin: want lowercase http or https scheme://host[:port] with no default port, path, query or fragment", ErrConfig, i, o)
		}
	}
	if len(cfg.Domains) == 0 {
		return Config{}, fmt.Errorf("%w: domains is empty", ErrConfig)
	}
	seen := make(map[uint32]bool, len(cfg.Domains))
	for i, d := range cfg.Domains {
		if seen[d.DomainID] {
			return Config{}, fmt.Errorf("%w: domains[%d].domainId %d is already used by an earlier entry", ErrConfig, i, d.DomainID)
		}
		seen[d.DomainID] = true
		switch d.Source {
		case "LOCAL":
			if len(d.Upstreams) != 0 {
				return Config{}, fmt.Errorf("%w: domains[%d].upstreams is for REMOTE domains only", ErrConfig, i)
			}
			if !filepath.IsAbs(d.ExecutorStoragePath) {
				return Config{}, fmt.Errorf("%w: domains[%d].executorStoragePath %q is not absolute", ErrConfig, i, d.ExecutorStoragePath)
			}
			if overlaps(cfg.StoragePath, d.ExecutorStoragePath) {
				return Config{}, fmt.Errorf("%w: domains[%d].executorStoragePath %q overlaps storagePath %q: neither may equal or contain the other", ErrConfig, i, d.ExecutorStoragePath, cfg.StoragePath)
			}
		case "REMOTE":
			if d.ExecutorStoragePath != "" {
				return Config{}, fmt.Errorf("%w: domains[%d].executorStoragePath is for LOCAL domains only", ErrConfig, i)
			}
			if len(d.Upstreams) < 1 || len(d.Upstreams) > maxUpstreams {
				return Config{}, fmt.Errorf("%w: domains[%d].upstreams must hold 1 to %d URLs", ErrConfig, i, maxUpstreams)
			}
			for j, raw := range d.Upstreams {
				u, err := url.Parse(raw)
				if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" ||
					u.Opaque != "" || u.RawQuery != "" || u.Fragment != "" || u.ForceQuery {
					return Config{}, fmt.Errorf("%w: domains[%d].upstreams[%d] %s is not an absolute http or https base URL without a query or fragment", ErrConfig, i, j, redactEndpoint(raw))
				}
			}
		default:
			return Config{}, fmt.Errorf("%w: domains[%d].source %q is not LOCAL or REMOTE", ErrConfig, i, d.Source)
		}
	}
	if cfg.Simulation.Enabled == nil {
		on := true
		cfg.Simulation.Enabled = &on
	}
	if err := checkSimulation(cfg); err != nil {
		return Config{}, err
	}
	cfg.Simulation = cfg.Simulation.withDefaults()
	return cfg, nil
}

// checkSimulation validates the simulation object and every domain's
// workerBinary. It runs after the other checks and expects Enabled to
// be set.
func checkSimulation(cfg Config) error {
	sim := cfg.Simulation
	if sim.MaxGas > constants.MaxInputGas {
		return fmt.Errorf("%w: simulation.maxGas %d is outside [1, %d]", ErrConfig, sim.MaxGas, constants.MaxInputGas)
	}
	if sim.TimeoutMs > maxSimTimeoutMs {
		return fmt.Errorf("%w: simulation.timeoutMs %d is outside [1, %d]", ErrConfig, sim.TimeoutMs, maxSimTimeoutMs)
	}
	if sim.MaxConcurrent < 0 || sim.MaxConcurrent > maxSimConcurrent {
		return fmt.Errorf("%w: simulation.maxConcurrent %d is outside [1, %d]", ErrConfig, sim.MaxConcurrent, maxSimConcurrent)
	}
	if sim.UID != nil {
		if err := checkSimUID(*sim.UID, cfg.StoragePath); err != nil {
			return err
		}
	}
	for i, d := range cfg.Domains {
		switch {
		case d.WorkerBinary != "":
			if err := checkWorkerBinary(i, d.WorkerBinary); err != nil {
				return err
			}
		case *sim.Enabled:
			return fmt.Errorf("%w: domains[%d].workerBinary is required when simulation is enabled", ErrConfig, i)
		}
	}
	return nil
}

// checkSimUID refuses a simulation.uid that is out of range, is root,
// or is the uid that owns (or will own) storagePath.
func checkSimUID(uid int, storagePath string) error {
	if uid == 0 {
		return fmt.Errorf("%w: simulation.uid 0 is root; choose a dedicated uid from 1 to %d", ErrConfig, maxSimUID)
	}
	if uid < 0 || uid > maxSimUID {
		return fmt.Errorf("%w: simulation.uid %d is outside [1, %d]", ErrConfig, uid, maxSimUID)
	}
	fi, err := os.Stat(resolve(storagePath))
	switch {
	case errors.Is(err, fs.ErrNotExist):
		// PrepareStorage creates the path as the gateway's own uid.
		if uid == euid() {
			return fmt.Errorf("%w: simulation.uid %d is the gateway's own uid, which will own storagePath %s", ErrConfig, uid, storagePath)
		}
	case err != nil:
		return fmt.Errorf("%w: simulation.uid: storagePath %s: %w", ErrConfig, storagePath, err)
	default:
		if st, ok := fi.Sys().(*syscall.Stat_t); ok && int(st.Uid) == uid {
			return fmt.Errorf("%w: simulation.uid %d owns storagePath %s; simulators must not run as the gateway's storage owner", ErrConfig, uid, storagePath)
		}
	}
	return nil
}

// checkWorkerBinary applies the checks a supervisor worker binary
// passes: absolute, present, a regular file and executable.
func checkWorkerBinary(i int, path string) error {
	if !filepath.IsAbs(path) {
		return fmt.Errorf("%w: domains[%d].workerBinary %q is not absolute", ErrConfig, i, path)
	}
	fi, err := os.Stat(path)
	if err != nil {
		return fmt.Errorf("%w: domains[%d].workerBinary: %w", ErrConfig, i, err)
	}
	if !fi.Mode().IsRegular() {
		return fmt.Errorf("%w: domains[%d].workerBinary %q is not a regular file", ErrConfig, i, path)
	}
	if fi.Mode().Perm()&0o111 == 0 {
		return fmt.Errorf("%w: domains[%d].workerBinary %q is not executable", ErrConfig, i, path)
	}
	return nil
}

// validOrigin reports whether o is a browser origin as an Origin
// header carries it, so an exact comparison can match it.
func validOrigin(o string) bool {
	u, err := url.Parse(o)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Hostname() == "" {
		return false
	}
	if o != u.Scheme+"://"+u.Host || u.Host != strings.ToLower(u.Host) || strings.HasSuffix(u.Host, ":") {
		return false
	}
	if p := u.Port(); p != "" {
		n, err := strconv.Atoi(p)
		if err != nil || n < 1 || n > 65535 || strconv.Itoa(n) != p {
			return false
		}
		if (u.Scheme == "http" && n == 80) || (u.Scheme == "https" && n == 443) {
			return false
		}
	}
	return true
}

// overlaps reports whether a and b are the same path or one contains
// the other. It compares their cleaned forms and their forms with
// symlinks resolved, then their file identities, because a discard
// removes directories under storagePath.
func overlaps(a, b string) bool {
	for _, x := range []string{filepath.Clean(a), resolve(a)} {
		for _, y := range []string{filepath.Clean(b), resolve(b)} {
			if within(x, y) || within(y, x) {
				return true
			}
		}
	}
	return sameTree(a, b)
}

// resolve returns path with the symlinks in its deepest existing
// ancestor resolved and the rest appended unchanged, so a path that
// does not exist yet still compares through a symlinked parent.
func resolve(path string) string {
	clean := filepath.Clean(path)
	rest := ""
	for cur := clean; ; {
		if r, err := filepath.EvalSymlinks(cur); err == nil {
			return filepath.Join(r, rest)
		}
		parent := filepath.Dir(cur)
		if parent == cur {
			return clean
		}
		rest = filepath.Join(filepath.Base(cur), rest)
		cur = parent
	}
}

// within reports whether path is dir or lies under it. Both are clean
// absolute paths.
func within(path, dir string) bool {
	sep := string(filepath.Separator)
	return strings.HasPrefix(strings.TrimSuffix(path, sep)+sep, strings.TrimSuffix(dir, sep)+sep)
}
