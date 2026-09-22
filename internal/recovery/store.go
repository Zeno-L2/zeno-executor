package recovery

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/zenon-network/go-zenon/common/crypto"
	"github.com/zenon-network/go-zenon/common/types"

	"github.com/digitalSloth/go-executor/internal/state"
)

const (
	batchExt  = ".json"
	batchName = 20 // digits in a zero-padded batch id
	bundleExt = ".bundle"
	snapExt   = ".snap"
)

// Layout is the per-domain on-disk tree (EXECUTOR §16), rooted at <storagePath>/<domainId>.
// Momenta is the path of momentum_hashes/window.json; every other field that names a
// directory (Snapshots, Bundles, Batches) is that directory.
type Layout struct {
	Root, StateWAL, Snapshots, Bundles, Batches, Cursor, Momenta, Meta string
}

// NewLayout resolves the tree for domainID under storagePath and creates every directory it
// names.
func NewLayout(storagePath string, domainID uint32) (Layout, error) {
	root := filepath.Join(storagePath, strconv.FormatUint(uint64(domainID), 10))
	l := Layout{
		Root:      root,
		StateWAL:  filepath.Join(root, "state", "wal"),
		Snapshots: filepath.Join(root, "snapshots"),
		Bundles:   filepath.Join(root, "bundles"),
		Batches:   filepath.Join(root, "batches"),
		Cursor:    filepath.Join(root, "cursor"),
		Momenta:   filepath.Join(root, "momentum_hashes", "window.json"),
		Meta:      filepath.Join(root, "meta.json"),
	}
	for _, dir := range []string{filepath.Dir(l.StateWAL), l.Snapshots, l.Bundles, l.Batches, filepath.Dir(l.Momenta)} {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return Layout{}, fmt.Errorf("recovery: create %s: %w", dir, err)
		}
	}
	return l, nil
}

// Meta is meta.json: the identity this tree was created under.
type Meta struct {
	DomainID    uint32     `json:"domainId"`
	ExecutorID  string     `json:"executorId"`
	STFSpecHash types.Hash `json:"stfSpecHash"`
	Role        string     `json:"role"`
	CreatedAt   time.Time  `json:"createdAt"`
	LastBootAt  time.Time  `json:"lastBootAt"`
}

// OpenMeta reads meta.json, creating it from want on first boot, and rejects a tree created for
// a different domainId or executorId — a storage path reused under another identity is a
// consistency fault, not a fresh start. A differing stfSpecHash is recorded rather than
// rejected: §16 makes the snapshot's own tag the load guard.
func OpenMeta(l Layout, want Meta) (Meta, error) {
	now := time.Now().UTC()
	if want.CreatedAt.IsZero() {
		want.CreatedAt = now
	}
	if want.LastBootAt.IsZero() {
		want.LastBootAt = now
	}

	var got Meta
	found, err := readJSON(l.Meta, &got)
	if err != nil {
		return Meta{}, fmt.Errorf("recovery: read meta: %w", err)
	}
	if !found {
		if err := writeJSON(l.Meta, want); err != nil {
			return Meta{}, fmt.Errorf("recovery: write meta: %w", err)
		}
		return want, nil
	}

	if got.DomainID != want.DomainID {
		return Meta{}, fmt.Errorf("recovery: storage tree belongs to domain %d, configured for %d", got.DomainID, want.DomainID)
	}
	if got.ExecutorID != want.ExecutorID {
		return Meta{}, fmt.Errorf("recovery: storage tree belongs to executor %q, configured for %q", got.ExecutorID, want.ExecutorID)
	}
	got.STFSpecHash = want.STFSpecHash
	got.Role = want.Role
	got.LastBootAt = want.LastBootAt
	if err := writeJSON(l.Meta, got); err != nil {
		return Meta{}, fmt.Errorf("recovery: write meta: %w", err)
	}
	return got, nil
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
	// NextGlobalIndex is the globalInputIndex the first accepted block above MomentumHeight
	// takes. It is not derivable from LastInputSeq: accepted blocks of other domains consume
	// indices this domain never sees.
	NextGlobalIndex uint64 `json:"nextGlobalIndex"`
	NextBatchID     uint64 `json:"nextBatchId"`
}

// ReadCursor returns the committed position. ok is false when no position has been committed.
func ReadCursor(l Layout) (Position, bool, error) {
	var pos Position
	found, err := readJSON(l.Cursor, &pos)
	if err != nil {
		return Position{}, false, fmt.Errorf("recovery: read cursor: %w", err)
	}
	return pos, found, nil
}

// validatePosition rejects a Position that cannot be a committed one: JSON decodes null, {} and
// any object with omitted fields into the zero Position without error, so a torn or truncated
// cursor file reads back as a value indistinguishable from genesis unless checked here. It is
// run before pos drives any destructive call, and before Commit writes pos as the cursor, so a
// malformed value is never mistaken for a valid position on either side of the file.
func validatePosition(pos Position) error {
	if pos.MomentumHeight == 0 {
		return errors.New("momentum height is 0")
	}
	if pos.MomentumHash == (types.Hash{}) {
		return errors.New("momentum hash is zero")
	}
	if pos.LastInputSeq != 0 && pos.PostStateRoot == (types.Hash{}) {
		return errors.New("post-state root is zero for a non-zero input sequence")
	}
	return nil
}

// StatusBuilt marks a batch this executor built and committed locally. It is the only status
// Phase 1 records: nothing submits yet, so the record carries no txHash and no
// submittedAtHeight. Both are omitted JSON fields, so the submitting ticket adds them and
// widens Status without a format migration.
const StatusBuilt = "BUILT"

// BatchRecord is one committed batch's metadata (EXECUTOR §16).
type BatchRecord struct {
	BatchID       uint64     `json:"batchId"`
	Status        string     `json:"status"`
	FirstInputSeq uint64     `json:"firstInputSeq"`
	LastInputSeq  uint64     `json:"lastInputSeq"`
	PreStateRoot  types.Hash `json:"preStateRoot"`
	PostStateRoot types.Hash `json:"postStateRoot"`
	DAHash        types.Hash `json:"daHash"`
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
	if err := writeJSON(l.Momenta, w); err != nil {
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

// BundleStore persists DA bundles by DAHash, one file each, and satisfies batch.BundleStore.
// Bundles are never deleted: they are the only durable copy of a superseded CodeHash (SPEC §12)
// and of an un-finalized batch's own DA payload, and a bundle orphaned by a discarded batch is
// retained by the same rule.
type BundleStore struct {
	dir string
}

// NewBundleStore returns a BundleStore over dir, creating it when absent.
func NewBundleStore(dir string) (*BundleStore, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("recovery: create bundle dir %s: %w", dir, err)
	}
	return &BundleStore{dir: dir}, nil
}

func (b *BundleStore) path(daHash types.Hash) string {
	return filepath.Join(b.dir, hex.EncodeToString(daHash[:])+bundleExt)
}

// Put writes bundle under daHash atomically. It is idempotent: the name is the content's own
// hash, so a rewrite stores the same bytes. It rejects bundle bytes that do not hash to daHash,
// so Has can never bless a bundle this process wrote with a name that does not match its content.
func (b *BundleStore) Put(daHash types.Hash, bundle []byte) error {
	if !bytes.Equal(crypto.Hash(bundle), daHash[:]) {
		return fmt.Errorf("recovery: put bundle %s: bytes do not hash to daHash", daHash)
	}
	if err := writeFileAtomic(b.path(daHash), bundle); err != nil {
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

// Commit durably records pos: it snapshots store — tagged with pos.LastInputSeq, the configured
// stfSpecHash, and pos itself as the header's Meta, which is what lets a rollback rebuild the
// cursor and its window from a snapshot alone — writes a record for every batch built since the
// previous commit, reconciles window into the recorded momentum window, and renames the new
// cursor over the old, the single step that makes everything before it durable. It then prunes
// snapshots to the retention ladder and truncates the store's log.
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
		if err := writeJSON(batchPath(cfg.Layout, rec.BatchID), rec); err != nil {
			return fmt.Errorf("recovery: write batch record %d: %w", rec.BatchID, err)
		}
	}

	if len(window) > 0 {
		if err := writeMomentumWindow(cfg.Layout, mergeWindow(recorded, window, cfg.ConfirmationDepth)); err != nil {
			return err
		}
	}

	if err := writeJSON(cfg.Layout.Cursor, pos); err != nil {
		return fmt.Errorf("recovery: write cursor: %w", err)
	}

	if err := retainSnapshots(cfg, pos, snapName); err != nil {
		return err
	}
	if err := store.TruncateLog(); err != nil {
		return fmt.Errorf("recovery: %w", err)
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

// retainSnapshots keeps exactly the cursor's snapshot, the floor and the rung, and deletes every
// other snapshot written by Commit (see the retention ladder in the package's design).
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

func writeJSON(path string, v any) error {
	data, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	return writeFileAtomic(path, data)
}

// writeFileAtomic writes data to a temporary file in path's directory, syncs and closes it, then
// renames it over path and syncs the containing directory, so the rename itself is durable and
// not merely ordered after the file's own content.
func writeFileAtomic(path string, data []byte) error {
	dir := filepath.Dir(path)
	f, err := os.CreateTemp(dir, ".tmp-*")
	if err != nil {
		return err
	}
	tmp := f.Name()
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
