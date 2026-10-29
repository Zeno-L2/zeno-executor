package recovery

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"syscall"

	"github.com/zenon-network/go-zenon/common/crypto"
	"github.com/zenon-network/go-zenon/common/types"

	"github.com/Zeno-L2/zeno-executor/internal/codec"
	"github.com/Zeno-L2/zeno-executor/internal/state"
)

const (
	batchExt  = ".json"
	batchName = 20 // digits in a zero-padded batch id
	bundleExt = ".bundle"
	snapExt   = ".snap"

	receiptExt = ".receipts"
)

// File and directory modes. bundles/, receipts/ and batches/ are the
// gateway's read interface: their directories are readDirMode and their
// files readFileMode, inside a domain directory other users can
// traverse but not list. Every other file this package writes is
// privateFileMode. Each mode is set by an explicit chmod, never by the
// umask.
const (
	domainDirMode   os.FileMode = 0o711
	readDirMode     os.FileMode = 0o755
	readFileMode    os.FileMode = 0o644
	privateFileMode os.FileMode = 0o600
)

// Layout is the per-domain on-disk tree, rooted at
// <storagePath>/<domainId>. Momenta is the path of
// momentum_hashes/window.json; every other field that names a directory
// (Snapshots, Bundles, Receipts, Batches) is that directory.
type Layout struct {
	Root, Snapshots, Bundles, Receipts, Batches, Cursor, Momenta, Meta string
}

// ResolveLayout returns the tree for domainID under storagePath
// without creating, reading or changing anything, for a reader of the
// read interface such as the gateway.
func ResolveLayout(storagePath string, domainID uint32) Layout {
	root := filepath.Join(storagePath, strconv.FormatUint(uint64(domainID), 10))
	return Layout{
		Root:      root,
		Snapshots: filepath.Join(root, "snapshots"),
		Bundles:   filepath.Join(root, "bundles"),
		Receipts:  filepath.Join(root, "receipts"),
		Batches:   filepath.Join(root, "batches"),
		Cursor:    filepath.Join(root, "cursor"),
		Momenta:   filepath.Join(root, "momentum_hashes", "window.json"),
		Meta:      filepath.Join(root, "meta.json"),
	}
}

// NewLayout resolves the tree for domainID under storagePath and creates every directory it
// names. It sets the domain directory to 0711 and batches/ to 0755 on every call, whatever the
// umask.
func NewLayout(storagePath string, domainID uint32) (Layout, error) {
	l := ResolveLayout(storagePath, domainID)
	for _, dir := range []string{l.Snapshots, l.Bundles, l.Receipts, l.Batches, filepath.Dir(l.Momenta)} {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return Layout{}, fmt.Errorf("recovery: create %s: %w", dir, err)
		}
	}
	if err := os.Chmod(l.Root, domainDirMode); err != nil {
		return Layout{}, fmt.Errorf("recovery: chmod %s: %w", l.Root, err)
	}
	if err := os.Chmod(l.Batches, readDirMode); err != nil {
		return Layout{}, fmt.Errorf("recovery: chmod %s: %w", l.Batches, err)
	}
	return l, nil
}

// Meta is meta.json: the identity this tree was created under.
type Meta struct {
	DomainID   uint32 `json:"domainId"`
	ExecutorID string `json:"executorId"`
}

// OpenMeta creates meta.json from want on first boot. On every later
// boot it reads the file back and rejects a tree created for a
// different domainId or executorId: a storage path reused under
// another identity is a consistency fault, not a fresh start. It never
// rewrites an existing file.
func OpenMeta(l Layout, want Meta) error {
	var got Meta
	found, err := readJSON(l.Meta, &got)
	if err != nil {
		return fmt.Errorf("recovery: read meta: %w", err)
	}
	if !found {
		if err := writeJSON(l.Meta, want, privateFileMode); err != nil {
			return fmt.Errorf("recovery: write meta: %w", err)
		}
		return nil
	}
	if got.DomainID != want.DomainID {
		return fmt.Errorf("recovery: storage tree belongs to domain %d, configured for %d", got.DomainID, want.DomainID)
	}
	if got.ExecutorID != want.ExecutorID {
		return fmt.Errorf("recovery: storage tree belongs to executor %q, configured for %q", got.ExecutorID, want.ExecutorID)
	}
	return nil
}

// Position is one durably committed executor position: the input sequence the state includes,
// the root that state hashes to, and the ledger point to resume from. It is written atomically
// as the cursor and recorded verbatim in the snapshot whose leaf set it describes, so a
// rollback restores the cursor, the resume point and the reorg-detection window from the
// snapshot alone.
type Position struct {
	LastInputSeq  uint64     `json:"lastInputSeq"`
	PostStateRoot types.Hash `json:"postStateRoot"`
	// MomentumHeight is the last momentum fully consumed; the next read starts above it.
	MomentumHeight uint64 `json:"momentumHeight"`
	// MomentumHash is that momentum's hash. It is what lets a position anchor the
	// reorg-detection window on its own: the continuity check uses the recorded window only when
	// its last entry is this pair, and this pair alone otherwise. So a boot whose recorded window
	// is empty under a live cursor, or which a crash left unanchored, still has an exact entry to
	// check against, and a Rollback can write a one-entry window without reading the source.
	MomentumHash types.Hash `json:"momentumHash"`
	// NextOrdinal is the domain ordinal Settlement assigns to this domain's first input above
	// MomentumHeight: the ordinal the next assignment must start at. It is not derivable from
	// LastInputSeq: inputs of other domains consume global indices this domain never sees.
	NextOrdinal uint64 `json:"nextOrdinal"`
	NextBatchID uint64 `json:"nextBatchId"`
}

// ReadCursor returns the committed position. ok is false when no position has been committed.
//
// A zero post-state root is a valid value (an empty domain state hashes to the zero root), so a
// zero value cannot mark a torn file. Commit always writes the postStateRoot field, so a cursor
// file without it, or with it null, is rejected.
func ReadCursor(l Layout) (Position, bool, error) {
	data, err := os.ReadFile(l.Cursor)
	if errors.Is(err, os.ErrNotExist) {
		return Position{}, false, nil
	}
	if err != nil {
		return Position{}, false, fmt.Errorf("recovery: read cursor: %w", err)
	}
	var pos Position
	if err := json.Unmarshal(data, &pos); err != nil {
		return Position{}, false, fmt.Errorf("recovery: read cursor: decode %s: %w", filepath.Base(l.Cursor), err)
	}
	var present struct {
		PostStateRoot *json.RawMessage `json:"postStateRoot"`
	}
	if err := json.Unmarshal(data, &present); err != nil || present.PostStateRoot == nil {
		return Position{}, false, fmt.Errorf("recovery: read cursor: %s has no postStateRoot field", filepath.Base(l.Cursor))
	}
	return pos, true, nil
}

// validatePosition rejects a Position that cannot be a committed one: JSON decodes null, {} and
// any object with omitted fields into the zero Position without error, so a torn or truncated
// cursor file reads back as a value indistinguishable from genesis unless checked here. It is
// run before pos drives any destructive call, and before Commit writes pos as the cursor, so a
// malformed value is never mistaken for a valid position on either side of the file. A zero
// post-state root is legitimate at any input sequence: an empty domain state has the zero hash
// as its root, so it is not checked here.
func validatePosition(pos Position) error {
	if pos.MomentumHeight == 0 {
		return errors.New("momentum height is 0")
	}
	if pos.MomentumHash == (types.Hash{}) {
		return errors.New("momentum hash is zero")
	}
	return nil
}

// StatusBuilt marks a batch this executor built and committed locally.
// It is the only status a BatchRecord carries, and the record has no
// txHash or submittedAtHeight field.
const StatusBuilt = "BUILT"

// BatchRecord is one committed batch's metadata.
type BatchRecord struct {
	BatchID       uint64     `json:"batchId"`
	Status        string     `json:"status"`
	FirstInputSeq uint64     `json:"firstInputSeq"`
	LastInputSeq  uint64     `json:"lastInputSeq"`
	PreStateRoot  types.Hash `json:"preStateRoot"`
	PostStateRoot types.Hash `json:"postStateRoot"`
	DAHash        types.Hash `json:"daHash"`
	// Commitment is codec.BatchCommitment.Encode() of the batch. It is omitted when empty, so
	// adding it needs no format version or migration.
	Commitment []byte `json:"commitment,omitempty"`
}

func batchPath(l Layout, id uint64) string {
	return filepath.Join(l.Batches, fmt.Sprintf("%0*d%s", batchName, id, batchExt))
}

// parseBatchFile returns the batch id a directory entry name encodes, and false for any name
// that is not a zero-padded batch id.
func parseBatchFile(name string) (uint64, bool) {
	digits, ok := strings.CutSuffix(name, batchExt)
	if !ok || len(digits) != batchName {
		return 0, false
	}
	id, err := strconv.ParseUint(digits, 10, 64)
	if err != nil {
		return 0, false
	}
	return id, true
}

// ReadBatchRecord returns the batch record for id. ok is false when no
// record file exists for id, and when the file's batchId differs from
// id, which ListBatchRecords skips the same way.
func ReadBatchRecord(l Layout, id uint64) (BatchRecord, bool, error) {
	var rec BatchRecord
	found, err := readRecordJSON(batchPath(l, id), &rec)
	if err != nil {
		return BatchRecord{}, false, fmt.Errorf("recovery: read batch record %d: %w", id, err)
	}
	if !found || rec.BatchID != id {
		return BatchRecord{}, false, nil
	}
	return rec, true, nil
}

// ListBatchRecords returns every committed batch record, ascending by batchId. A file whose
// name does not parse as a zero-padded batch id is skipped: a crash between the atomic-write
// helper's create and its rename leaves a temporary in the same directory. A file whose decoded
// BatchID does not match the id its name encodes is also skipped, so a partial overwrite or a
// file dropped into the directory cannot shadow the record its name promises.
func ListBatchRecords(l Layout) ([]BatchRecord, error) {
	entries, err := os.ReadDir(l.Batches)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("recovery: list batch records: %w", err)
	}
	type named struct {
		id   uint64
		file string
	}
	var files []named
	for _, e := range entries {
		if id, ok := parseBatchFile(e.Name()); ok && !e.IsDir() {
			files = append(files, named{id, e.Name()})
		}
	}
	sort.Slice(files, func(i, j int) bool { return files[i].id < files[j].id })

	out := make([]BatchRecord, 0, len(files))
	for _, f := range files {
		var rec BatchRecord
		if _, err := readJSON(filepath.Join(l.Batches, f.file), &rec); err != nil {
			return nil, fmt.Errorf("recovery: read batch record %s: %w", f.file, err)
		}
		if rec.BatchID != f.id {
			continue
		}
		out = append(out, rec)
	}
	return out, nil
}

// MomentumRef is one height-to-hash entry of the reorg-detection window. It is the JSON shape of
// window.json; ledger.ConsumedMomentum is the same pair in the follower's own vocabulary, and
// the caller converts between them.
type MomentumRef struct {
	Height uint64     `json:"height"`
	Hash   types.Hash `json:"hash"`
}

// ReadMomentumWindow returns the recorded window, ascending by height and contiguous. It is the
// file's contents, not the window a continuity check runs against: that one is anchored on the
// cursor, which ForkParent does.
func ReadMomentumWindow(l Layout) ([]MomentumRef, error) {
	var w []MomentumRef
	if _, err := readJSON(l.Momenta, &w); err != nil {
		return nil, fmt.Errorf("recovery: read momentum window: %w", err)
	}
	if err := checkContiguous(w); err != nil {
		return nil, fmt.Errorf("recovery: momentum window: %w", err)
	}
	return w, nil
}

func writeMomentumWindow(l Layout, w []MomentumRef) error {
	if w == nil {
		w = []MomentumRef{}
	}
	if err := writeJSON(l.Momenta, w, privateFileMode); err != nil {
		return fmt.Errorf("recovery: write momentum window: %w", err)
	}
	return nil
}

func checkContiguous(w []MomentumRef) error {
	for i := 1; i < len(w); i++ {
		if w[i].Height != w[i-1].Height+1 {
			return fmt.Errorf("entries %d and %d (heights %d, %d) are not ascending and contiguous", i-1, i, w[i-1].Height, w[i].Height)
		}
	}
	return nil
}

// PruneAbove deletes every batch record, snapshot and window entry above pos — the artifacts a
// crash before a cursor write, or a rollback off a retracted branch, leaves behind. A snapshot
// is above pos when its embedded Position has a higher MomentumHeight or a higher LastInputSeq.
// A committed snapshot above pos exceeds it on both fields: every commit records at least one
// batch and a batch never has zero inputs, so LastInputSeq strictly increases across commits
// exactly as MomentumHeight does. Both comparisons are kept so the rule is total over whatever
// is on disk, since ListSnapshots returns files rather than a commit history, and a file that
// exceeds pos on one field only is not a valid rollback target under any reading. A fresh
// (zero) position deletes everything, with no special case: momentum heights are 1-based, so
// every window entry and every snapshot is above height 0, and every BatchID is at or above 0.
// Boot and Rollback both call it before the store is opened and before any snapshot is
// selected, so nothing above the position can be loaded or chosen later.
func PruneAbove(l Layout, pos Position) error {
	entries, err := os.ReadDir(l.Batches)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("recovery: prune batch records: %w", err)
	}
	for _, e := range entries {
		if id, ok := parseBatchFile(e.Name()); ok && id >= pos.NextBatchID {
			if err := os.Remove(filepath.Join(l.Batches, e.Name())); err != nil {
				return fmt.Errorf("recovery: prune batch record %s: %w", e.Name(), err)
			}
		}
	}

	snaps, err := listPositioned(l)
	if err != nil {
		return err
	}
	for _, s := range snaps {
		if s.pos.MomentumHeight > pos.MomentumHeight || s.pos.LastInputSeq > pos.LastInputSeq {
			if err := removeSnapshot(l, s.info.Name); err != nil {
				return err
			}
		}
	}

	window, err := ReadMomentumWindow(l)
	if err != nil {
		return err
	}
	kept := window[:0:0]
	for _, e := range window {
		if e.Height <= pos.MomentumHeight {
			kept = append(kept, e)
		}
	}
	if len(kept) != len(window) {
		return writeMomentumWindow(l, kept)
	}
	return nil
}

// positioned is a snapshot together with the Position its header's Meta carries.
type positioned struct {
	info state.SnapshotInfo
	pos  Position
}

// listPositioned returns every listed snapshot whose Meta decodes as a Position, oldest first
// by newer's ordering. A snapshot whose Meta is not a Position was not written by Commit and is
// left alone by every caller.
func listPositioned(l Layout) ([]positioned, error) {
	infos, err := state.ListSnapshots(l.Snapshots)
	if err != nil {
		return nil, fmt.Errorf("recovery: %w", err)
	}
	out := make([]positioned, 0, len(infos))
	for _, info := range infos {
		var pos Position
		if err := json.Unmarshal(info.Header.Meta, &pos); err != nil {
			continue
		}
		out = append(out, positioned{info: info, pos: pos})
	}
	sort.Slice(out, func(i, j int) bool { return newer(out[j], out[i]) })
	return out, nil
}

// OldestSnapshotSeq returns the smallest LastInputSeq among the stored
// snapshots, which bounds every position Rollback can restore other
// than genesis. ok is false when no snapshot is stored.
func OldestSnapshotSeq(l Layout) (seq uint64, ok bool, err error) {
	snaps, err := listPositioned(l)
	if err != nil {
		return 0, false, err
	}
	for _, s := range snaps {
		if !ok || s.pos.LastInputSeq < seq {
			seq, ok = s.pos.LastInputSeq, true
		}
	}
	return seq, ok, nil
}

// newer reports whether a is newer than b: it orders by MomentumHeight, then LastInputSeq,
// then NextBatchID, then name, so the order is total over whatever is on disk.
func newer(a, b positioned) bool {
	switch {
	case a.pos.MomentumHeight != b.pos.MomentumHeight:
		return a.pos.MomentumHeight > b.pos.MomentumHeight
	case a.pos.LastInputSeq != b.pos.LastInputSeq:
		return a.pos.LastInputSeq > b.pos.LastInputSeq
	case a.pos.NextBatchID != b.pos.NextBatchID:
		return a.pos.NextBatchID > b.pos.NextBatchID
	}
	return a.info.Name > b.info.Name
}

func removeSnapshot(l Layout, name string) error {
	if err := os.Remove(filepath.Join(l.Snapshots, name+snapExt)); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("recovery: remove snapshot %s: %w", name, err)
	}
	return nil
}

// BundleStore persists DA bundles by DAHash, one file each, and
// satisfies batch.BundleStore. Bundles are never deleted: they are the
// only durable copy of a superseded CodeHash and of an un-finalized
// batch's own DA payload, and a bundle orphaned by a discarded batch is
// retained by the same rule.
type BundleStore struct {
	dir string
}

// NewBundleStore returns a BundleStore over dir, creating it when absent and setting it to
// 0755.
func NewBundleStore(dir string) (*BundleStore, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("recovery: create bundle dir %s: %w", dir, err)
	}
	if err := os.Chmod(dir, readDirMode); err != nil {
		return nil, fmt.Errorf("recovery: chmod bundle dir %s: %w", dir, err)
	}
	return &BundleStore{dir: dir}, nil
}

func (b *BundleStore) path(daHash types.Hash) string {
	return BundlePath(b.dir, daHash)
}

// BundlePath is the file holding the bundle for daHash in the bundle
// directory dir.
func BundlePath(dir string, daHash types.Hash) string {
	return filepath.Join(dir, hex.EncodeToString(daHash[:])+bundleExt)
}

// Put writes bundle under daHash atomically. It is idempotent: the name is the content's own
// hash, so a rewrite stores the same bytes. It rejects bundle bytes that do not hash to daHash,
// so Has can never bless a bundle this process wrote with a name that does not match its content.
func (b *BundleStore) Put(daHash types.Hash, bundle []byte) error {
	if !bytes.Equal(crypto.Hash(bundle), daHash[:]) {
		return fmt.Errorf("recovery: put bundle %s: bytes do not hash to daHash", daHash)
	}
	if err := writeFileAtomic(b.path(daHash), bundle, readFileMode); err != nil {
		return fmt.Errorf("recovery: put bundle %s: %w", daHash, err)
	}
	return nil
}

// Get returns the stored bundle, verifying that its bytes still hash to daHash. A missing
// bundle is ErrBundleMissing, which the AWAIT_DA path classifies; corrupted bytes are a plain
// error, which halts.
func (b *BundleStore) Get(daHash types.Hash) ([]byte, error) {
	data, err := os.ReadFile(b.path(daHash))
	if errors.Is(err, os.ErrNotExist) {
		return nil, fmt.Errorf("%w: %s", ErrBundleMissing, daHash)
	}
	if err != nil {
		return nil, fmt.Errorf("recovery: read bundle %s: %w", daHash, err)
	}
	if !bytes.Equal(crypto.Hash(data), daHash[:]) {
		return nil, fmt.Errorf("recovery: bundle %s: stored bytes do not hash to the file name", daHash)
	}
	return data, nil
}

// Has reports whether a bundle is stored for daHash, without reading it. Boot checks presence
// over every un-finalized batch, so it must not re-hash the whole history.
func (b *BundleStore) Has(daHash types.Hash) (bool, error) {
	_, err := os.Stat(b.path(daHash))
	if err == nil {
		return true, nil
	}
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	return false, fmt.Errorf("recovery: stat bundle %s: %w", daHash, err)
}

// ReceiptStore persists receipt sidecars by DAHash, one file each, and
// satisfies batch.ReceiptStore. Sidecars are retained forever, as
// bundles are.
type ReceiptStore struct {
	dir string
}

// NewReceiptStore returns a ReceiptStore over dir, creating it when absent and setting it to
// 0755.
func NewReceiptStore(dir string) (*ReceiptStore, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("recovery: create receipts dir %s: %w", dir, err)
	}
	if err := os.Chmod(dir, readDirMode); err != nil {
		return nil, fmt.Errorf("recovery: chmod receipts dir %s: %w", dir, err)
	}
	return &ReceiptStore{dir: dir}, nil
}

func (s *ReceiptStore) path(daHash types.Hash) string {
	return ReceiptsPath(s.dir, daHash)
}

// ReceiptsPath is the file holding the receipt sidecar for daHash in
// the receipts directory dir.
func ReceiptsPath(dir string, daHash types.Hash) string {
	return filepath.Join(dir, hex.EncodeToString(daHash[:])+receiptExt)
}

// Put writes sidecar under daHash atomically, replacing any sidecar already stored there. It
// rejects a sidecar that does not decode, or whose leaves' MerkleRoot is not receiptRoot, and
// then writes nothing.
func (s *ReceiptStore) Put(daHash, receiptRoot types.Hash, sidecar []byte) error {
	_, leaves, err := codec.DecodeReceiptSidecar(sidecar)
	if err != nil {
		return fmt.Errorf("recovery: put receipts %s: %w", daHash, err)
	}
	got := codec.MerkleRoot(leaves)
	if got != receiptRoot {
		return fmt.Errorf("recovery: put receipts %s: sidecar root %s is not receiptRoot %s", daHash, got, receiptRoot)
	}
	if err := writeFileAtomic(s.path(daHash), sidecar, readFileMode); err != nil {
		return fmt.Errorf("recovery: put receipts %s: %w", daHash, err)
	}
	return nil
}

// Commit durably records pos: it snapshots store — tagged with pos.LastInputSeq, the configured
// stfSpecHash, and pos itself as the header's Meta, which is what lets a rollback rebuild the
// cursor and its window from a snapshot alone — writes a record for every batch built since the
// previous commit, reconciles window into the recorded momentum window, and renames the new
// cursor over the old, the single step that makes everything before it durable. It then prunes
// snapshots to the retention ladder.
//
// A non-empty window argument must end at the position being committed: its last entry's Height
// must equal pos.MomentumHeight and its Hash must equal pos.MomentumHash. That is what keeps the
// recorded window anchored on the cursor, which the continuity check and PruneAbove both rely on,
// and it catches a caller that committed a position and a checkpoint taken at different moments.
// For every commit the runtime makes, the recorded window's last entry is therefore the cursor's
// own pair after the commit.
//
// window is reconciled by height, never blindly substituted: it comes from a Follower, whose
// ring starts empty at every boot and every rollback, so substituting would cut the recorded
// window to one entry on the first commit after a restart and shrink reorg tolerance to one
// entry exactly when a restart makes a while-down reorg most likely. Let top be the recorded
// window's highest height:
//
//   - window is empty                           -> the recorded window is unchanged;
//   - the recorded window is empty              -> window becomes the recorded window;
//   - window[0].Height <= top                   -> merge: drop window's entries at or below top,
//     append the rest, trim the front to ConfirmationDepth+1;
//   - window[0].Height == top+1                 -> merge, which appends all of window;
//   - window[0].Height > top+1                  -> replace the recorded window with window.
//
// The empty-recorded case is a fresh tree, and also a tree whose previous commit was interrupted
// between the window write and the cursor rename and whose window the next boot's PruneAbove then
// emptied. Taking the argument as-is is right in both, because the argument ends at the position
// being committed.
//
// The replace case is ordinary, not a fault: more than ConfirmationDepth+1 momenta pass between
// two commits on any domain whose inputs are sparse, and on every initial catch-up. Keeping both
// halves would leave a hole in a window whose contiguity the continuity check's single ranged
// read and its "no entry matches means the fork is below everything recorded" argument both rest
// on. Replacing never shortens the recorded window: a Follower built after a boot or a rollback
// starts at cursor+1, which is one above the recorded top, so a gap can only arise once its ring
// has been trimmed, and window therefore holds ConfirmationDepth+1 entries whenever this case is
// taken. A one-entry recorded window left by a rollback is merged when the fresh ring has not yet
// been trimmed and replaced otherwise; neither shortens it.
//
// A window argument that is not itself ascending and contiguous, or a non-empty one that does not
// end at pos, is an error, returned before anything at all is written. An empty window argument
// is a path only this package's tests take, since the runtime always commits the follower's own
// ring; it leaves the recorded window as it was, and the continuity check's cursor fallback
// absorbs the unanchored result.
func Commit(cfg Config, store *state.Store, pos Position, records []BatchRecord, window []MomentumRef) error {
	if err := cfg.validate(0); err != nil {
		return err
	}
	if err := validatePosition(pos); err != nil {
		return fmt.Errorf("recovery: commit position is malformed: %w", err)
	}
	if err := checkContiguous(window); err != nil {
		return fmt.Errorf("recovery: commit window: %w", err)
	}
	if n := len(window); n > 0 && (window[n-1].Height != pos.MomentumHeight || window[n-1].Hash != pos.MomentumHash) {
		return fmt.Errorf("recovery: commit window ends at height %d hash %s, position is height %d hash %s",
			window[n-1].Height, window[n-1].Hash, pos.MomentumHeight, pos.MomentumHash)
	}
	recorded, err := ReadMomentumWindow(cfg.Layout)
	if err != nil {
		return err
	}

	meta, err := json.Marshal(pos)
	if err != nil {
		return fmt.Errorf("recovery: encode position: %w", err)
	}
	snapName, err := store.SaveSnapshot(cfg.Layout.Snapshots, state.SnapshotHeader{
		InputSeq:    pos.LastInputSeq,
		STFSpecHash: cfg.STFSpecHash,
		Meta:        meta,
	})
	if err != nil {
		return fmt.Errorf("recovery: %w", err)
	}

	for _, rec := range records {
		if err := writeJSON(batchPath(cfg.Layout, rec.BatchID), rec, readFileMode); err != nil {
			return fmt.Errorf("recovery: write batch record %d: %w", rec.BatchID, err)
		}
	}

	if len(window) > 0 {
		if err := writeMomentumWindow(cfg.Layout, mergeWindow(recorded, window, cfg.ConfirmationDepth)); err != nil {
			return err
		}
	}

	if err := writeJSON(cfg.Layout.Cursor, pos, privateFileMode); err != nil {
		return fmt.Errorf("recovery: write cursor: %w", err)
	}

	if err := retainSnapshots(cfg, pos, snapName); err != nil {
		return err
	}
	return nil
}

// mergeWindow applies Commit's reconciliation rule to a non-empty window argument.
func mergeWindow(recorded, window []MomentumRef, depth uint64) []MomentumRef {
	var out []MomentumRef
	switch {
	case len(recorded) == 0:
		out = append(out, window...)
	case window[0].Height > recorded[len(recorded)-1].Height+1:
		out = append(out, window...)
	default:
		top := recorded[len(recorded)-1].Height
		out = append(out, recorded...)
		for _, e := range window {
			if e.Height > top {
				out = append(out, e)
			}
		}
	}
	if max := depth + 1; uint64(len(out)) > max {
		out = out[uint64(len(out))-max:]
	}
	return out
}

// retainSnapshots keeps exactly the cursor's snapshot, the floor and
// the rung, and deletes every other snapshot written by Commit.
func retainSnapshots(cfg Config, committed Position, cursorName string) error {
	snaps, err := listPositioned(cfg.Layout)
	if err != nil {
		return err
	}
	depth := cfg.ConfirmationDepth
	var floorBound uint64
	if committed.MomentumHeight > depth {
		floorBound = committed.MomentumHeight - depth
	}
	rungBand := committed.MomentumHeight / depth

	keep := map[string]bool{cursorName: true}
	var floor, rung *positioned
	for i := range snaps { // ascending: the last match is the newest
		s := &snaps[i]
		if s.info.Name == cursorName {
			continue
		}
		if s.pos.MomentumHeight <= floorBound {
			floor = s
		}
		if s.pos.MomentumHeight/depth < rungBand {
			rung = s
		}
	}
	for _, s := range []*positioned{floor, rung} {
		if s != nil {
			keep[s.info.Name] = true
		}
	}
	for _, s := range snaps {
		if !keep[s.info.Name] {
			if err := removeSnapshot(cfg.Layout, s.info.Name); err != nil {
				return err
			}
		}
	}
	return nil
}

func readJSON(path string, v any) (bool, error) {
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if err := json.Unmarshal(data, v); err != nil {
		return false, fmt.Errorf("decode %s: %w", filepath.Base(path), err)
	}
	return true, nil
}

// MaxBatchRecordSize bounds a batch record file read through
// ReadBatchRecord. A record is indented JSON of a few hundred bytes
// plus the hex of its commitment; 64 KiB is far above that.
const MaxBatchRecordSize = 64 << 10

// readRecordJSON is readJSON for one batch record: the read is
// ReadRegularFile with MaxBatchRecordSize.
func readRecordJSON(path string, v any) (bool, error) {
	data, err := ReadRegularFile(path, MaxBatchRecordSize)
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if err := json.Unmarshal(data, v); err != nil {
		return false, fmt.Errorf("decode %s: %w", filepath.Base(path), err)
	}
	return true, nil
}

// ErrTooLarge is wrapped by ReadRegularFile's errors for a file above
// its size limit.
var ErrTooLarge = errors.New("too large")

// ReadRegularFile returns the contents of the regular file at path,
// which must hold at most maxSize bytes. It never follows a symlink
// at path and never blocks on a FIFO or device, and it checks the
// size of the open file before it allocates one buffer of that size,
// so a file written by another user cannot exhaust memory or stall
// the caller. A missing file reports an error satisfying
// errors.Is(err, os.ErrNotExist), and a file above maxSize one
// satisfying errors.Is(err, ErrTooLarge); every other rejection is a
// plain error.
func ReadRegularFile(path string, maxSize int64) ([]byte, error) {
	f, err := os.OpenFile(path, os.O_RDONLY|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() {
		return nil, fmt.Errorf("%s is not a regular file", filepath.Base(path))
	}
	if info.Size() > maxSize {
		return nil, fmt.Errorf("%s is %w: %d bytes, above the %d byte limit", filepath.Base(path), ErrTooLarge, info.Size(), maxSize)
	}
	r := io.LimitReader(f, maxSize+1)
	// One spare byte lets the read at end of file return without
	// growing the buffer; it grows only for a file that grew after
	// Stat, up to the limit.
	data := make([]byte, 0, info.Size()+1)
	for {
		n, err := r.Read(data[len(data):cap(data)])
		data = data[:len(data)+n]
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, err
		}
		if int64(len(data)) > maxSize {
			break
		}
		if len(data) == cap(data) {
			data = append(data, 0)[:len(data)]
		}
	}
	if int64(len(data)) > maxSize {
		return nil, fmt.Errorf("%s is %w: it grew past the %d byte limit", filepath.Base(path), ErrTooLarge, maxSize)
	}
	return data, nil
}

func writeJSON(path string, v any, perm os.FileMode) error {
	data, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	return writeFileAtomic(path, data, perm)
}

// writeFileAtomic writes data to a temporary file in path's directory, syncs and closes it, then
// renames it over path and syncs the containing directory, so the rename itself is durable and
// not merely ordered after the file's own content. The file gets mode perm, set on the
// temporary file before its contents are synced, so the rename publishes the final mode whatever
// the umask.
func writeFileAtomic(path string, data []byte, perm os.FileMode) error {
	dir := filepath.Dir(path)
	f, err := os.CreateTemp(dir, ".tmp-*")
	if err != nil {
		return err
	}
	tmp := f.Name()
	if err := f.Chmod(perm); err != nil {
		f.Close()
		os.Remove(tmp)
		return err
	}
	if _, err := f.Write(data); err != nil {
		f.Close()
		os.Remove(tmp)
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		os.Remove(tmp)
		return err
	}
	if err := f.Close(); err != nil {
		os.Remove(tmp)
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		os.Remove(tmp)
		return err
	}
	return syncDir(dir)
}

// syncDir fsyncs dir itself, which is what makes a rename inside it durable.
func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}
