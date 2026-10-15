package gateway

import (
	"fmt"

	"github.com/zenon-network/go-zenon/common/types"

	"github.com/digitalSloth/go-executor/internal/codec"
	"github.com/digitalSloth/go-executor/internal/recovery"
)

// The size limits of the files LocalSource reads. A worker owns these
// directories, so a read never trusts a file's size.
const (
	// maxBundleBytes is a memory ceiling on one bundle read, not a
	// protocol bound: no rule bounds the bytes of a bundle, and a
	// witness is recorded for every distinct key an input reads. A
	// bundle above the ceiling is never loaded, so the domain stalls
	// at SYNCING with the size error.
	maxBundleBytes = 1 << 30
	// maxSidecarSize is the receipt sidecar of the most records a
	// batch holds: a 4-byte count and, per receipt, a 4-byte length
	// and the 263-byte receipt.
	maxSidecarSize = 4 + codec.MaxBatchInputs*(4+263)
)

// Source is where a domain's batch files come from: an executor's
// read interface (LOCAL) or the gateway's verified copies (REMOTE).
type Source interface {
	Record(batchID uint64) (recovery.BatchRecord, bool, error)
	Bundle(daHash types.Hash) ([]byte, error)
	Receipts(daHash types.Hash) ([]byte, error)
	bundleWithin(daHash types.Hash, ceiling int64) ([]byte, error)
	bundlePath(daHash types.Hash) string
}

// LocalSource reads one executor domain directory's read interface
// (EXECUTOR §16): batches/, bundles/ and receipts/ only. It never
// creates, writes or changes the mode of anything.
type LocalSource struct {
	layout recovery.Layout
}

// NewLocalSource returns the source for domainID under
// executorStoragePath.
func NewLocalSource(executorStoragePath string, domainID uint32) *LocalSource {
	return &LocalSource{layout: recovery.ResolveLayout(executorStoragePath, domainID)}
}

// Record returns the batch record for batchID; ok is false when there
// is none.
func (s *LocalSource) Record(batchID uint64) (recovery.BatchRecord, bool, error) {
	return recovery.ReadBatchRecord(s.layout, batchID)
}

// Bundle returns the bundle file named by daHash, unverified.
func (s *LocalSource) Bundle(daHash types.Hash) ([]byte, error) {
	return s.bundleWithin(daHash, maxBundleBytes)
}

// bundleWithin is Bundle with the ceiling as a parameter.
func (s *LocalSource) bundleWithin(daHash types.Hash, ceiling int64) ([]byte, error) {
	data, err := recovery.ReadRegularFile(recovery.BundlePath(s.layout.Bundles, daHash), ceiling)
	if err != nil {
		return nil, fmt.Errorf("gateway: read bundle %s: %w", daHash, err)
	}
	return data, nil
}

// Receipts returns the receipt sidecar named by daHash, unverified.
func (s *LocalSource) Receipts(daHash types.Hash) ([]byte, error) {
	data, err := recovery.ReadRegularFile(recovery.ReceiptsPath(s.layout.Receipts, daHash), maxSidecarSize)
	if err != nil {
		return nil, fmt.Errorf("gateway: read receipt sidecar %s: %w", daHash, err)
	}
	return data, nil
}

// bundlePath returns the path of the bundle file named by daHash.
func (s *LocalSource) bundlePath(daHash types.Hash) string {
	return recovery.BundlePath(s.layout.Bundles, daHash)
}
