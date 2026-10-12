package supervisor

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/zenon-network/go-zenon/common/types"

	"github.com/digitalSloth/go-executor/internal/codec"
	"github.com/digitalSloth/go-executor/internal/ipc"
)

// withdrawalStoreVersion is the file-format version this build writes
// and reads.
const withdrawalStoreVersion = 1

// StoredWithdrawal is one L1 withdrawal in the arguments RelayMessage
// takes.
type StoredWithdrawal struct {
	Message []byte `json:"message"` // canonical kind 1 OutboxMessage
	Proof   []byte `json:"proof"`   // MerkleProof of Message
}

// StoredBatch is one batch's hand-off. Verified means every Proof
// verified against OutboxRoot and the totals matched an asset flow
// summary; only a verified batch may be relayed. An unverified batch
// belongs to a batch Settlement accepted, has a zero OutboxRoot, and
// waits for its check against Settlement's entry.
type StoredBatch struct {
	DomainID    uint32             `json:"domainId"`
	BatchID     uint64             `json:"batchId"`
	Verified    bool               `json:"verified"`
	OutboxRoot  types.Hash         `json:"outboxRoot"`
	Withdrawals []StoredWithdrawal `json:"withdrawals"` // strictly ascending by (inputIndex, outboxIndex)
}

// BatchRef names one stored batch and whether it is verified.
type BatchRef struct {
	BatchID  uint64
	Verified bool
}

// storedFile is one store file: the version, then the batch's fields.
type storedFile struct {
	Version uint32 `json:"version"`
	StoredBatch
}

// withdrawalStore is one domain's hand-offs, one file per batch. Safe
// for concurrent use.
type withdrawalStore struct {
	dir      string
	domainID uint32
	mu       sync.Mutex      // guards ids and serialises every file operation
	ids      map[uint64]bool // batchId with a file -> its Verified flag
}

const tmpPrefix = ".tmp-"

// openWithdrawalStore opens, creating when absent, the store in dir. It
// reads and checks every batch file, and removes the temporary file a
// crashed write leaves behind. A file that fails its check is an error
// naming it.
func openWithdrawalStore(dir string, domainID uint32) (*withdrawalStore, error) {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, err
	}
	parent := filepath.Dir(dir)
	if err := syncDir(parent); err != nil {
		return nil, err
	}
	if err := syncDir(filepath.Dir(parent)); err != nil {
		return nil, err
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	s := &withdrawalStore{dir: dir, domainID: domainID, ids: make(map[uint64]bool)}
	removed := false
	for _, e := range entries {
		name := e.Name()
		if strings.HasPrefix(name, tmpPrefix) {
			if err := os.Remove(filepath.Join(dir, name)); err != nil {
				return nil, err
			}
			removed = true
			continue
		}
		id, ok := batchFileID(name)
		if !ok || e.IsDir() {
			log.Printf("supervisor: WARN: withdrawal store %s: ignoring %s", dir, name)
			continue
		}
		b, err := s.read(id)
		if err != nil {
			return nil, err
		}
		s.ids[id] = b.Verified
	}
	if removed {
		if err := syncDir(dir); err != nil {
			return nil, err
		}
	}
	return s, nil
}

// batchFileID returns the batchId a canonical batch file name encodes.
func batchFileID(name string) (uint64, bool) {
	digits, ok := strings.CutSuffix(name, ".json")
	if !ok {
		return 0, false
	}
	id, err := strconv.ParseUint(digits, 10, 64)
	if err != nil || strconv.FormatUint(id, 10) != digits {
		return 0, false
	}
	return id, true
}

func (s *withdrawalStore) path(batchID uint64) string {
	return filepath.Join(s.dir, strconv.FormatUint(batchID, 10)+".json")
}

// read loads, decodes and checks batchID's file. The caller holds s.mu
// or owns s.
func (s *withdrawalStore) read(batchID uint64) (StoredBatch, error) {
	path := s.path(batchID)
	b, err := func() (StoredBatch, error) {
		data, err := os.ReadFile(path)
		if err != nil {
			return StoredBatch{}, err
		}
		var f storedFile
		if err := json.Unmarshal(data, &f); err != nil {
			return StoredBatch{}, err
		}
		if f.Version != withdrawalStoreVersion {
			return StoredBatch{}, fmt.Errorf("version %d, this build reads version %d", f.Version, withdrawalStoreVersion)
		}
		if err := checkStoredBatch(f.StoredBatch, s.domainID, batchID); err != nil {
			return StoredBatch{}, err
		}
		return f.StoredBatch, nil
	}()
	if err != nil {
		return StoredBatch{}, fmt.Errorf("withdrawal store file %s is damaged: %w", path, err)
	}
	return b, nil
}

// Put writes b's file, replacing any existing one, and records its
// flag. It does not re-run checkStoredBatch.
func (s *withdrawalStore) Put(b StoredBatch) error {
	if b.DomainID != s.domainID {
		return fmt.Errorf("withdrawal store: batch %d is for domain %d, the store is domain %d", b.BatchID, b.DomainID, s.domainID)
	}
	data, err := json.Marshal(storedFile{Version: withdrawalStoreVersion, StoredBatch: b})
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := writeFileAtomic(s.path(b.BatchID), data); err != nil {
		return err
	}
	s.ids[b.BatchID] = b.Verified
	return nil
}

// Batch returns batchID's stored batch, or false when the store has
// none.
func (s *withdrawalStore) Batch(batchID uint64) (StoredBatch, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.ids[batchID]; !ok {
		return StoredBatch{}, false, nil
	}
	b, err := s.read(batchID)
	if err != nil {
		return StoredBatch{}, false, err
	}
	return b, true, nil
}

// Lookup reports whether batchID has a file and whether it is verified,
// without reading it.
func (s *withdrawalStore) Lookup(batchID uint64) (verified, ok bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	verified, ok = s.ids[batchID]
	return verified, ok
}

// Batches lists every stored batch, ascending by BatchID.
func (s *withdrawalStore) Batches() []BatchRef {
	s.mu.Lock()
	refs := make([]BatchRef, 0, len(s.ids))
	for id, v := range s.ids {
		refs = append(refs, BatchRef{BatchID: id, Verified: v})
	}
	s.mu.Unlock()
	sort.Slice(refs, func(i, j int) bool { return refs[i].BatchID < refs[j].BatchID })
	return refs
}

// lowestUnverified returns the lowest BatchID whose file is unverified.
func (s *withdrawalStore) lowestUnverified() (uint64, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var id uint64
	found := false
	for batchID, verified := range s.ids {
		if !verified && (!found || batchID < id) {
			id, found = batchID, true
		}
	}
	return id, found
}

// lowestVerified returns the lowest verified BatchID below limit.
func (s *withdrawalStore) lowestVerified(limit uint64) (uint64, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var id uint64
	found := false
	for batchID, verified := range s.ids {
		if verified && batchID < limit && (!found || batchID < id) {
			id, found = batchID, true
		}
	}
	return id, found
}

// Remove deletes batchID's file. An absent file counts as removed.
func (s *withdrawalStore) Remove(batchID uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := os.Remove(s.path(batchID)); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return err
	}
	if err := syncDir(s.dir); err != nil {
		return err
	}
	delete(s.ids, batchID)
	return nil
}

// checkStoredBatch checks a decoded file against the batch it should
// hold. A verified batch's proofs must verify against its root. An
// unverified batch must have a zero root and well-formed proofs.
func checkStoredBatch(b StoredBatch, domainID uint32, batchID uint64) error {
	if b.DomainID != domainID {
		return fmt.Errorf("domainId %d, want %d", b.DomainID, domainID)
	}
	if b.BatchID != batchID {
		return fmt.Errorf("batchId %d, want %d", b.BatchID, batchID)
	}
	if len(b.Withdrawals) > ipc.MaxHandOffWithdrawals {
		return fmt.Errorf("%d withdrawals, maximum %d", len(b.Withdrawals), ipc.MaxHandOffWithdrawals)
	}
	if !b.Verified && b.OutboxRoot != (types.Hash{}) {
		return errors.New("unverified batch has a non-zero outbox root")
	}
	var prevInput uint64
	var prevOutbox uint32
	for i, w := range b.Withdrawals {
		m, err := codec.DecodeOutboxMessage(w.Message)
		if err != nil {
			return fmt.Errorf("withdrawal %d: %w", i, err)
		}
		if _, ok := m.Target.(codec.L1Withdrawal); !ok {
			return fmt.Errorf("withdrawal %d is not a kind 1 outbox message", i)
		}
		if i > 0 && !after(m.InputIndex, m.OutboxIndex, prevInput, prevOutbox) {
			return fmt.Errorf("withdrawal %d (%d, %d) is not after (%d, %d)", i, m.InputIndex, m.OutboxIndex, prevInput, prevOutbox)
		}
		prevInput, prevOutbox = m.InputIndex, m.OutboxIndex
		if b.Verified {
			if err := codec.VerifyMerkleProof(b.OutboxRoot, w.Message, w.Proof); err != nil {
				return fmt.Errorf("withdrawal %d proof: %w", i, err)
			}
		} else if err := ipc.CheckProof(w.Proof); err != nil {
			return fmt.Errorf("withdrawal %d proof: %w", i, err)
		}
	}
	return nil
}

// after reports whether (input, outbox) is strictly greater than
// (prevInput, prevOutbox), comparing the input index first.
func after(input uint64, outbox uint32, prevInput uint64, prevOutbox uint32) bool {
	return input > prevInput || input == prevInput && outbox > prevOutbox
}

// writeFileAtomic writes data to a temporary file in path's directory,
// syncs and closes it, then renames it over path and syncs the
// containing directory, so the rename itself is durable and not merely
// ordered after the file's own content.
func writeFileAtomic(path string, data []byte) error {
	dir := filepath.Dir(path)
	f, err := os.CreateTemp(dir, tmpPrefix+"*")
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
