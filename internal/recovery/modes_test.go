package recovery

import (
	"fmt"
	"os"
	"path/filepath"
	"syscall"
	"testing"

	"github.com/zenon-network/go-zenon/common/crypto"
	"github.com/zenon-network/go-zenon/common/types"

	"github.com/digitalSloth/go-executor/domain"
	"github.com/digitalSloth/go-executor/internal/codec"
	"github.com/digitalSloth/go-executor/internal/state"
)

// writeArtifacts writes every file a worker boot and one commit write under storage/1, through
// the package's own writers, and returns the layout.
func writeArtifacts(storage string) (Layout, error) {
	l, err := NewLayout(storage, 1)
	if err != nil {
		return Layout{}, fmt.Errorf("new layout: %w", err)
	}
	if err := OpenMeta(l, Meta{DomainID: 1, ExecutorID: "exec-1"}); err != nil {
		return l, fmt.Errorf("open meta: %w", err)
	}

	bundle := []byte("bundle bytes")
	var daHash types.Hash
	copy(daHash[:], crypto.Hash(bundle))
	bundles, err := NewBundleStore(l.Bundles)
	if err != nil {
		return l, fmt.Errorf("bundle store: %w", err)
	}
	if err := bundles.Put(daHash, bundle); err != nil {
		return l, fmt.Errorf("put bundle: %w", err)
	}

	rec := codec.Receipt{Version: codec.ReceiptVersion, InputIndex: 1}
	leaf, err := rec.Encode()
	if err != nil {
		return l, fmt.Errorf("encode receipt: %w", err)
	}
	sidecar, err := codec.EncodeReceiptSidecar([]codec.Receipt{rec})
	if err != nil {
		return l, fmt.Errorf("encode sidecar: %w", err)
	}
	receipts, err := NewReceiptStore(l.Receipts)
	if err != nil {
		return l, fmt.Errorf("receipt store: %w", err)
	}
	if err := receipts.Put(daHash, codec.MerkleRoot([][]byte{leaf}), sidecar); err != nil {
		return l, fmt.Errorf("put receipts: %w", err)
	}

	store := state.OpenEmpty()
	defer store.Close()
	root, err := store.Apply([]domain.StateDiffEntry{{Path: leafPath(1), Value: []byte{1}}})
	if err != nil {
		return l, fmt.Errorf("apply: %w", err)
	}
	pos := Position{
		LastInputSeq: 1, PostStateRoot: root, MomentumHeight: 1,
		MomentumHash: canonHash(1, 0), NextOrdinal: 2, NextBatchID: 1,
	}
	records := []BatchRecord{{
		BatchID: 0, Status: StatusBuilt, FirstInputSeq: 1, LastInputSeq: 1,
		PostStateRoot: root, DAHash: daHash,
	}}
	window := []MomentumRef{{Height: 1, Hash: canonHash(1, 0)}}
	cfg := Config{Layout: l, STFSpecHash: testSTF, ConfirmationDepth: 4}
	if err := Commit(cfg, store, pos, records, window); err != nil {
		return l, fmt.Errorf("commit: %w", err)
	}
	return l, nil
}

func requireMode(t *testing.T, path string, want os.FileMode) {
	t.Helper()
	fi, err := os.Stat(path)
	if err != nil {
		t.Errorf("stat %s: %v", path, err)
		return
	}
	if got := fi.Mode().Perm(); got != want {
		t.Errorf("%s mode = %o, want %o", path, got, want)
	}
}

// requireFilesMode checks that dir holds at least one regular file and that every one has mode
// want.
func requireFilesMode(t *testing.T, dir string, want os.FileMode) {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Errorf("read %s: %v", dir, err)
		return
	}
	files := 0
	for _, e := range entries {
		if !e.Type().IsRegular() {
			continue
		}
		files++
		requireMode(t, filepath.Join(dir, e.Name()), want)
	}
	if files == 0 {
		t.Errorf("%s holds no regular file", dir)
	}
}

func checkModes(t *testing.T, l Layout) {
	t.Helper()
	requireMode(t, l.Root, 0o711)
	for _, dir := range []string{l.Bundles, l.Receipts, l.Batches} {
		requireMode(t, dir, 0o755)
		requireFilesMode(t, dir, 0o644)
	}
	for _, path := range []string{l.Cursor, l.Meta, l.Momenta} {
		requireMode(t, path, 0o600)
	}
	requireFilesMode(t, l.Snapshots, 0o600)
}

func TestModesIgnoreUmask(t *testing.T) {
	old := syscall.Umask(0o077)
	defer syscall.Umask(old)

	l, err := writeArtifacts(t.TempDir())
	if err != nil {
		t.Fatalf("writeArtifacts: %v", err)
	}
	checkModes(t, l)
}

func TestModesCorrectedOnReopen(t *testing.T) {
	storage := t.TempDir()
	l, err := writeArtifacts(storage)
	if err != nil {
		t.Fatalf("writeArtifacts: %v", err)
	}
	chmods := map[string]os.FileMode{
		l.Root: 0o755, l.Bundles: 0o700, l.Receipts: 0o700, l.Batches: 0o700,
	}
	for path, mode := range chmods {
		if err := os.Chmod(path, mode); err != nil {
			t.Fatal(err)
		}
	}

	if _, err := NewLayout(storage, 1); err != nil {
		t.Fatalf("NewLayout: %v", err)
	}
	if _, err := NewBundleStore(l.Bundles); err != nil {
		t.Fatalf("NewBundleStore: %v", err)
	}
	if _, err := NewReceiptStore(l.Receipts); err != nil {
		t.Fatalf("NewReceiptStore: %v", err)
	}
	requireMode(t, l.Root, 0o711)
	for _, dir := range []string{l.Bundles, l.Receipts, l.Batches} {
		requireMode(t, dir, 0o755)
	}
}
