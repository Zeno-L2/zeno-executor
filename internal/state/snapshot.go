package state

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/zenon-network/go-zenon/common/crypto"
	"github.com/zenon-network/go-zenon/common/trie"
	"github.com/zenon-network/go-zenon/common/types"
)

const (
	snapshotMagic = "ZNSNAP01"
	snapshotExt   = ".snap"
)

// SnapshotHeader is the tag block a snapshot file carries (EXECUTOR §16). This package
// interprets only STFSpecHash, which guards loading; InputSeq and Meta are recorded verbatim
// and handed back unchanged, for the recovery layer's own bookkeeping.
type SnapshotHeader struct {
	// InputSeq is the last globalInputIndex whose effects the snapshotted leaf set includes.
	InputSeq    uint64
	STFSpecHash types.Hash
	// Meta is caller-defined bytes stored with the snapshot and returned by ListSnapshots.
	Meta []byte
}

// SnapshotInfo is one snapshot file's name and header.
type SnapshotInfo struct {
	Name   string
	Header SnapshotHeader
}

// SaveSnapshot writes the store's complete leaf set to dir, tagged with hdr, under a name that
// is the hex SHA3-256 of the file's own bytes, and returns that name. The write is atomic — a
// temporary file is synced and renamed over — so a crash never leaves a snapshot a later boot
// could load in part. Leaves are written ascending by path, so the same leaf set always
// produces the same file.
func (s *Store) SaveSnapshot(dir string, hdr SnapshotHeader) (string, error) {
	s.mu.RLock()
	body := encodeSnapshot(hdr, s.leaves)
	s.mu.RUnlock()

	name := hex.EncodeToString(crypto.Hash(body))
	if err := writeFileAtomic(filepath.Join(dir, name+snapshotExt), body); err != nil {
		return "", fmt.Errorf("state: save snapshot: %w", err)
	}
	return name, nil
}

// ListSnapshots returns the header of every well-formed snapshot in dir, ascending by
// InputSeq. A file whose bytes do not hash to its name, or which is truncated, is skipped
// rather than reported: a snapshot is a cache of derivable state, so an unreadable one is not
// itself a fault. dir not existing yields no snapshots and no error.
func ListSnapshots(dir string) ([]SnapshotInfo, error) {
	entries, err := os.ReadDir(dir)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("state: list snapshots in %s: %w", dir, err)
	}

	var out []SnapshotInfo
	for _, e := range entries {
		fileName := e.Name()
		if e.IsDir() || !strings.HasSuffix(fileName, snapshotExt) {
			continue
		}
		name := strings.TrimSuffix(fileName, snapshotExt)
		body, err := os.ReadFile(filepath.Join(dir, fileName))
		if err != nil {
			return nil, fmt.Errorf("state: read snapshot %s: %w", fileName, err)
		}
		if hex.EncodeToString(crypto.Hash(body)) != name {
			continue
		}
		hdr, _, err := decodeSnapshot(body)
		if err != nil {
			continue
		}
		out = append(out, SnapshotInfo{Name: name, Header: hdr})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Header.InputSeq != out[j].Header.InputSeq {
			return out[i].Header.InputSeq < out[j].Header.InputSeq
		}
		return out[i].Name < out[j].Name
	})
	return out, nil
}

// OpenSnapshot opens walPath seeded from the snapshot dir/name, discarding any existing WAL
// content: the snapshot is the durable state and the log holds only diffs applied after it,
// which a boot always discards. It fails when the file's bytes do not hash to name, and when
// the snapshot's STFSpecHash differs from stfSpecHash — a snapshot produced by a different STF
// is never loaded (§16).
// The open sets the log to mode 0600, whatever its mode was.
func OpenSnapshot(walPath, dir, name string, stfSpecHash types.Hash) (*Store, error) {
	body, err := os.ReadFile(filepath.Join(dir, name+snapshotExt))
	if err != nil {
		return nil, fmt.Errorf("state: read snapshot %s: %w", name, err)
	}
	if hex.EncodeToString(crypto.Hash(body)) != name {
		return nil, fmt.Errorf("state: snapshot %s: bytes do not hash to the file name", name)
	}
	hdr, leaves, err := decodeSnapshot(body)
	if err != nil {
		return nil, fmt.Errorf("state: snapshot %s: %w", name, err)
	}
	if hdr.STFSpecHash != stfSpecHash {
		return nil, fmt.Errorf("state: snapshot %s is tagged stfSpecHash %s, want %s", name, hdr.STFSpecHash, stfSpecHash)
	}
	return openFresh(walPath, leaves)
}

// OpenEmpty opens walPath with an empty leaf set, discarding any existing WAL content. It is
// the genesis path: a first boot seeds the domain's StateInit into a store whose log starts
// empty.
// The open sets the log to mode 0600, whatever its mode was.
func OpenEmpty(walPath string) (*Store, error) {
	return openFresh(walPath, make(map[trie.Path][]byte))
}

// TruncateLog discards every record in the store's log, leaving the in-memory leaf set
// untouched: it truncates the file to zero length and seeks back to the start. Both halves
// matter — Apply writes at the file's current offset, so truncating without seeking would
// leave the next record beyond a hole of zero bytes, which replay reads as a zero-length
// record and rejects.
//
// The log's only reader is Open, which replays it from the start, so a caller that has a
// durable image of this same leaf set elsewhere — and will reopen from that image rather than
// from the log — loses nothing by discarding it, and bounds a file that otherwise grows for the
// life of the process.
func (s *Store) TruncateLog() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.log.Truncate(0); err != nil {
		return fmt.Errorf("state: truncate log: %w", err)
	}
	if _, err := s.log.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("state: seek log: %w", err)
	}
	if err := s.log.Sync(); err != nil {
		return fmt.Errorf("state: sync log: %w", err)
	}
	return nil
}

func openFresh(walPath string, leaves map[trie.Path][]byte) (*Store, error) {
	f, err := os.OpenFile(walPath, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0o600)
	if err != nil {
		return nil, fmt.Errorf("state: open %s: %w", walPath, err)
	}
	if err := f.Chmod(0o600); err != nil {
		f.Close()
		return nil, fmt.Errorf("state: chmod %s: %w", walPath, err)
	}
	return newStore(leaves, f), nil
}

func encodeSnapshot(hdr SnapshotHeader, leaves map[trie.Path][]byte) []byte {
	paths := make([]trie.Path, 0, len(leaves))
	size := len(snapshotMagic) + 8 + 32 + 4 + len(hdr.Meta) + 4
	for p, v := range leaves {
		paths = append(paths, p)
		size += 32 + 4 + len(v)
	}
	sort.Slice(paths, func(i, j int) bool { return bytes.Compare(paths[i][:], paths[j][:]) < 0 })

	buf := make([]byte, 0, size)
	buf = append(buf, snapshotMagic...)
	buf = binary.BigEndian.AppendUint64(buf, hdr.InputSeq)
	buf = append(buf, hdr.STFSpecHash[:]...)
	buf = binary.BigEndian.AppendUint32(buf, uint32(len(hdr.Meta)))
	buf = append(buf, hdr.Meta...)
	buf = binary.BigEndian.AppendUint32(buf, uint32(len(paths)))
	for _, p := range paths {
		v := leaves[p]
		buf = append(buf, p[:]...)
		buf = binary.BigEndian.AppendUint32(buf, uint32(len(v)))
		buf = append(buf, v...)
	}
	return buf
}

func decodeSnapshot(b []byte) (SnapshotHeader, map[trie.Path][]byte, error) {
	var hdr SnapshotHeader
	r := bytes.NewReader(b)

	magic := make([]byte, len(snapshotMagic))
	if _, err := io.ReadFull(r, magic); err != nil || string(magic) != snapshotMagic {
		return hdr, nil, errors.New("bad magic")
	}
	var fixed [8 + 32 + 4]byte
	if _, err := io.ReadFull(r, fixed[:]); err != nil {
		return hdr, nil, errors.New("truncated header")
	}
	hdr.InputSeq = binary.BigEndian.Uint64(fixed[:8])
	copy(hdr.STFSpecHash[:], fixed[8:40])
	metaLen := binary.BigEndian.Uint32(fixed[40:44])
	if int64(metaLen) > int64(r.Len()) {
		return hdr, nil, errors.New("truncated meta")
	}
	hdr.Meta = make([]byte, metaLen)
	if _, err := io.ReadFull(r, hdr.Meta); err != nil {
		return hdr, nil, errors.New("truncated meta")
	}

	var count [4]byte
	if _, err := io.ReadFull(r, count[:]); err != nil {
		return hdr, nil, errors.New("truncated leaf count")
	}
	n := binary.BigEndian.Uint32(count[:])
	// Every leaf takes at least 36 bytes, so a count the remaining bytes cannot hold is
	// rejected before it sizes an allocation.
	if int64(n)*36 > int64(r.Len()) {
		return hdr, nil, errors.New("leaf count exceeds file size")
	}

	leaves := make(map[trie.Path][]byte, n)
	var prev trie.Path
	for i := uint32(0); i < n; i++ {
		var entry [32 + 4]byte
		if _, err := io.ReadFull(r, entry[:]); err != nil {
			return hdr, nil, fmt.Errorf("truncated leaf %d", i)
		}
		var p trie.Path
		copy(p[:], entry[:32])
		if i > 0 && bytes.Compare(prev[:], p[:]) >= 0 {
			return hdr, nil, fmt.Errorf("leaf %d is not in ascending path order", i)
		}
		prev = p
		valueLen := binary.BigEndian.Uint32(entry[32:])
		if int64(valueLen) > int64(r.Len()) {
			return hdr, nil, fmt.Errorf("truncated leaf %d value", i)
		}
		v := make([]byte, valueLen)
		if _, err := io.ReadFull(r, v); err != nil {
			return hdr, nil, fmt.Errorf("truncated leaf %d value", i)
		}
		leaves[p] = v
	}
	if r.Len() != 0 {
		return hdr, nil, fmt.Errorf("%d trailing bytes", r.Len())
	}
	return hdr, leaves, nil
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
