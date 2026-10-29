package wasm

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/zenon-network/go-zenon/common/crypto"
	"github.com/zenon-network/go-zenon/common/types"
)

// entryKind partitions the code store's flat key space into its five
// entry shapes. Kind and key together identify a stored value.
type entryKind uint8

const (
	// entryDeployChunk holds one chunk's raw bytes, keyed by (deployer, code_hash, chunk_index).
	// Written by DecodeInput on an input_kind = 3; read by Apply on kind 4/5.
	entryDeployChunk entryKind = iota
	// entryPendingRecord holds a chunked deployment's {metadata_hash, total_size, chunk_count},
	// keyed by (deployer, code_hash). Written by DecodeInput on kind 2; read by Apply
	// on kind 4/5.
	entryPendingRecord
	// entryAssembledModule holds the concatenated, pre-instrumentation module bytes, keyed by
	// code_hash. Written by Apply on kind 4/5; read by Apply on kinds 0/1.
	entryAssembledModule
	// entryInstrumentedModule holds the post-instrumentation module bytes, keyed by CodeHash.
	// Written by Apply on kind 4/5 and, on re-derivation, by Apply on kinds 0/1; read by Apply
	// on kinds 0/1.
	entryInstrumentedModule
	// entryConsumedDeploy marks a (deployer, code_hash) pair whose staged chunks and pending
	// record a finalize or upgrade has consumed, keyed by (deployer, code_hash). The value
	// is encodeConsumedDeploy's. Written by DecodeInput on kind 4/5 and deleted by the next
	// kind 2 of the pair; read by Prune and Rewind.
	entryConsumedDeploy
)

func (k entryKind) dirName() string {
	switch k {
	case entryDeployChunk:
		return "deploy-chunk"
	case entryPendingRecord:
		return "pending-record"
	case entryAssembledModule:
		return "assembled"
	case entryInstrumentedModule:
		return "instrumented"
	case entryConsumedDeploy:
		return "consumed-deploy"
	default:
		panic(fmt.Sprintf("wasm: code store: unknown entry kind %d", k))
	}
}

// codeTier is the store the plugin reads and memoizes into: the disk
// codeStore of an executor worker, or the in-memory memTier of a
// simulate-mode worker.
type codeTier interface {
	get(kind entryKind, key []byte) (value []byte, ok bool, err error)
	put(kind entryKind, key, value []byte) error
	delete(kind entryKind, key []byte) error
	deleteMany(refs []entryRef) error
}

// entryRef names one code-store entry.
type entryRef struct {
	kind entryKind
	key  []byte
}

var (
	_ codeTier = (*codeStore)(nil)
	_ codeTier = (*memTier)(nil)
)

var allEntryKinds = []entryKind{entryDeployChunk, entryPendingRecord, entryAssembledModule, entryInstrumentedModule, entryConsumedDeploy}

// codeStore is the local, non-consensus, checksummed disk tier that
// Apply and DecodeInput read and memoize into. There is no in-memory
// byte tier: the OS page cache is sufficient at this scale. Every entry
// carries a SHA3-256 checksum of its own bytes, and a checksum mismatch
// (corruption, or a write that never completed) is treated exactly as
// absence; the caller's obligation to turn "absent" into a halt lives
// above this type, not in it.
type codeStore struct {
	root string
}

// newCodeStore opens (creating if necessary) the disk-tier store rooted at root.
func newCodeStore(root string) (*codeStore, error) {
	if root == "" {
		return nil, fmt.Errorf("wasm: code store: path is required")
	}
	for _, k := range allEntryKinds {
		if err := os.MkdirAll(filepath.Join(root, k.dirName()), 0o755); err != nil {
			return nil, fmt.Errorf("wasm: code store: %w", err)
		}
	}
	// Sync every kind directory (flushing any unlink or rename an earlier process left
	// unsynced), then the root and its parent so the created directories themselves persist.
	dirs := make([]string, 0, len(allEntryKinds)+2)
	for _, k := range allEntryKinds {
		dirs = append(dirs, filepath.Join(root, k.dirName()))
	}
	dirs = append(dirs, root, filepath.Dir(root))
	for _, d := range dirs {
		if err := syncDir(d); err != nil {
			return nil, fmt.Errorf("wasm: code store: %w", err)
		}
	}
	return &codeStore{root: root}, nil
}

// path derives the on-disk location for (kind, key): a fixed-width digest of key under kind's
// directory, so an arbitrary-length composite key (e.g. code_hash ‖ chunk_index) never
// produces an invalid or colliding filename.
func (s *codeStore) path(kind entryKind, key []byte) string {
	digest := crypto.Hash(key)
	return filepath.Join(s.root, kind.dirName(), hex.EncodeToString(digest))
}

// put writes value under (kind, key), appending a SHA3-256 checksum of value and committing
// via a temp-file-then-rename so a process interrupted mid-write leaves the prior entry (or
// absence) intact, never a partially-written file at the final path.
func (s *codeStore) put(kind entryKind, key, value []byte) error {
	checksum := crypto.Hash(value)
	buf := make([]byte, 0, len(value)+len(checksum))
	buf = append(buf, value...)
	buf = append(buf, checksum...)

	dst := s.path(kind, key)
	tmp, err := os.CreateTemp(filepath.Dir(dst), ".tmp-*")
	if err != nil {
		return fmt.Errorf("wasm: code store: put: %w", err)
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName) // no-op once the rename below succeeds

	if _, err := tmp.Write(buf); err != nil {
		tmp.Close()
		return fmt.Errorf("wasm: code store: put: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return fmt.Errorf("wasm: code store: put: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("wasm: code store: put: %w", err)
	}
	if err := os.Rename(tmpName, dst); err != nil {
		return fmt.Errorf("wasm: code store: put: %w", err)
	}
	if err := syncDir(filepath.Dir(dst)); err != nil {
		return fmt.Errorf("wasm: code store: put: %w", err)
	}
	return nil
}

// syncDir fsyncs dir itself, which is what makes a rename or removal inside it durable. It is
// a variable so a test can record the calls.
var syncDir = func(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}

// delete removes the entry at (kind, key) and makes the removal durable.
func (s *codeStore) delete(kind entryKind, key []byte) error {
	return s.deleteMany([]entryRef{{kind, key}})
}

// deleteMany removes every entry in refs, treating absence as success: the caller's contract is
// "these entries must not exist afterwards", and a missing file already satisfies it. Each
// affected directory is synced once after all removals, including a directory whose entries were
// all absent, so an unlink an earlier attempt left unsynced becomes durable. A non-nil error
// means a genuine I/O fault, which the caller turns into a halt.
func (s *codeStore) deleteMany(refs []entryRef) error {
	var dirs []string
	seen := make(map[string]bool)
	for _, r := range refs {
		path := s.path(r.kind, r.key)
		if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("wasm: code store: delete: %w", err)
		}
		if dir := filepath.Dir(path); !seen[dir] {
			seen[dir] = true
			dirs = append(dirs, dir)
		}
	}
	for _, dir := range dirs {
		if err := syncDir(dir); err != nil {
			return fmt.Errorf("wasm: code store: delete: %w", err)
		}
	}
	return nil
}

// get reads the entry at (kind, key). ok is false, with a nil error, for both a missing file
// and a checksum mismatch — the two are indistinguishable by design, since a mismatch can only
// mean corruption or an incomplete write, either of which is exactly "I do not have this
// entry." A non-nil error means a genuine I/O fault distinct from absence.
func (s *codeStore) get(kind entryKind, key []byte) (value []byte, ok bool, err error) {
	return readEntry(s.path(kind, key))
}

// list returns the value of every entry of kind whose checksum
// verifies, in no fixed order. It skips put's ".tmp-" files and treats
// a file removed during the listing as absent.
func (s *codeStore) list(kind entryKind) ([][]byte, error) {
	dir := filepath.Join(s.root, kind.dirName())
	names, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("wasm: code store: list: %w", err)
	}
	var values [][]byte
	for _, n := range names {
		if strings.HasPrefix(n.Name(), ".tmp-") {
			continue
		}
		value, ok, err := readEntry(filepath.Join(dir, n.Name()))
		if err != nil {
			return nil, err
		}
		if ok {
			values = append(values, value)
		}
	}
	return values, nil
}

// readEntry reads and verifies the entry file at path, with get's absence rules.
func readEntry(path string) (value []byte, ok bool, err error) {
	b, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, false, nil
		}
		return nil, false, fmt.Errorf("wasm: code store: get: %w", err)
	}

	const checksumSize = 32 // SHA3-256
	if len(b) < checksumSize {
		return nil, false, nil
	}
	value, checksum := b[:len(b)-checksumSize], b[len(b)-checksumSize:]
	if !bytes.Equal(checksum, crypto.Hash(value)) {
		return nil, false, nil
	}
	return value, true, nil
}

const (
	// simModuleCacheBytes bounds the instrumented modules a
	// simulate-mode plugin keeps.
	simModuleCacheBytes = 32 << 20
	// simModuleCacheEntries bounds their count, which keeps the linear
	// scan of memTier.order short and the per-entry map and slice
	// overhead small beside the cached bytes.
	simModuleCacheEntries = 1024
)

// memTier is the in-memory code tier of a simulate-mode plugin. It
// holds only instrumented modules, in an LRU bounded by bytes and
// entries. An assembled-module miss asks fetch and keeps nothing; every
// other entry kind, and every delete, is refused, because a simulation
// never writes the chunk or pending-record tiers. A memTier is not safe
// for concurrent use: a simulator runs one Apply at a time.
type memTier struct {
	fetch      func(types.Hash) ([]byte, bool, error)
	maxBytes   int // simModuleCacheBytes outside tests
	maxEntries int // simModuleCacheEntries outside tests
	mods       map[types.Hash][]byte
	order      []types.Hash // least to most recently used
	bytes      int          // sum of len over mods
}

// newMemTier returns an empty memTier that fetches assembled modules
// through fetch.
func newMemTier(fetch func(types.Hash) ([]byte, bool, error), maxBytes, maxEntries int) *memTier {
	return &memTier{fetch: fetch, maxBytes: maxBytes, maxEntries: maxEntries, mods: map[types.Hash][]byte{}}
}

func memTierKey(key []byte) (types.Hash, error) {
	var h types.Hash
	if len(key) != len(h) {
		return h, fmt.Errorf("wasm: code store: key is %d bytes, want %d", len(key), len(h))
	}
	copy(h[:], key)
	return h, nil
}

func refusedInSimulate(op string, kind entryKind) error {
	return fmt.Errorf("wasm: code store: %s of %s entries is not available in simulate mode", op, kind.dirName())
}

// touch moves h to the most recently used end of order.
func (m *memTier) touch(h types.Hash) {
	m.drop(h)
	m.order = append(m.order, h)
}

// drop removes h from order.
func (m *memTier) drop(h types.Hash) {
	for i, o := range m.order {
		if o == h {
			m.order = append(m.order[:i], m.order[i+1:]...)
			return
		}
	}
}

func (m *memTier) get(kind entryKind, key []byte) (value []byte, ok bool, err error) {
	if kind != entryInstrumentedModule && kind != entryAssembledModule {
		return nil, false, refusedInSimulate("get", kind)
	}
	h, err := memTierKey(key)
	if err != nil {
		return nil, false, err
	}
	if kind == entryAssembledModule {
		module, found, err := m.fetch(h)
		if err != nil {
			return nil, false, fmt.Errorf("wasm: code store: fetch: %w", err)
		}
		if !found {
			return nil, false, nil
		}
		return module, true, nil
	}
	value, ok = m.mods[h]
	if !ok {
		return nil, false, nil
	}
	m.touch(h)
	return value, true, nil
}

func (m *memTier) put(kind entryKind, key, value []byte) error {
	if kind != entryInstrumentedModule {
		return refusedInSimulate("put", kind)
	}
	h, err := memTierKey(key)
	if err != nil {
		return err
	}
	if len(value) > m.maxBytes {
		return fmt.Errorf("wasm: code store: put: module of %d bytes exceeds the %d byte cache", len(value), m.maxBytes)
	}
	if old, ok := m.mods[h]; ok {
		m.bytes -= len(old)
	}
	m.mods[h] = value
	m.bytes += len(value)
	m.touch(h)
	for m.bytes > m.maxBytes || len(m.mods) > m.maxEntries {
		oldest := m.order[0]
		m.order = m.order[1:]
		m.bytes -= len(m.mods[oldest])
		delete(m.mods, oldest)
	}
	return nil
}

func (m *memTier) delete(kind entryKind, key []byte) error {
	return refusedInSimulate("delete", kind)
}

func (m *memTier) deleteMany(refs []entryRef) error {
	if len(refs) == 0 {
		return nil
	}
	return refusedInSimulate("delete", refs[0].kind)
}
