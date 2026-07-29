package state

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"

	"github.com/zenon-network/go-zenon/common/trie"

	"github.com/digitalSloth/go-executor/domain"
)

// deleteSentinel marks a StateDiffEntry as a deletion on disk (SPEC §27.2's EMPTY sentinel),
// distinct from a present, zero-length value (encoded as valueLen == 0).
const deleteSentinel = 0xFFFFFFFF

// Each Apply call is logged as one record:
//
//	recordLen  : u32 (big-endian) — byte length of everything that follows
//	numEntries : u32
//	numEntries * { path : 32 bytes ; valueLen : u32 (deleteSentinel => delete) ; value : valueLen bytes }
//
// recordLen lets replay detect a torn trailing record — fewer bytes remain in the file than
// recordLen promises — which is the ordinary signature of a crash mid-append, not corruption.
func appendRecord(f *os.File, diff []domain.StateDiffEntry) error {
	payload := encodeRecord(diff)

	header := make([]byte, 4)
	binary.BigEndian.PutUint32(header, uint32(len(payload)))

	if _, err := f.Write(header); err != nil {
		return fmt.Errorf("write record header: %w", err)
	}
	if _, err := f.Write(payload); err != nil {
		return fmt.Errorf("write record payload: %w", err)
	}
	return f.Sync()
}

func encodeRecord(diff []domain.StateDiffEntry) []byte {
	buf := make([]byte, 4, 4+len(diff)*(32+4))
	binary.BigEndian.PutUint32(buf, uint32(len(diff)))

	for _, entry := range diff {
		buf = append(buf, entry.Path[:]...)
		if entry.Value == nil {
			buf = binary.BigEndian.AppendUint32(buf, deleteSentinel)
			continue
		}
		buf = binary.BigEndian.AppendUint32(buf, uint32(len(entry.Value)))
		buf = append(buf, entry.Value...)
	}
	return buf
}

// replayLog reads every well-formed record from the start of f, applying each in order to
// build the leaf set. It returns that leaf set and the byte offset immediately following the
// last complete record — the point a torn trailing record (if any) should be truncated from.
func replayLog(f *os.File) (map[trie.Path][]byte, int64, error) {
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return nil, 0, fmt.Errorf("seek start: %w", err)
	}

	leaves := make(map[trie.Path][]byte)
	var offset int64

	header := make([]byte, 4)
	for {
		_, err := io.ReadFull(f, header)
		if err == io.EOF {
			break // clean end: no more records.
		}
		if err == io.ErrUnexpectedEOF {
			break // torn header: a crash left fewer than 4 bytes trailing.
		}
		if err != nil {
			return nil, 0, fmt.Errorf("read record header at %d: %w", offset, err)
		}

		recordLen := binary.BigEndian.Uint32(header)
		payload := make([]byte, recordLen)
		if _, err := io.ReadFull(f, payload); err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				break // torn payload: crash occurred mid-append.
			}
			return nil, 0, fmt.Errorf("read record payload at %d: %w", offset, err)
		}

		diff, err := decodeRecord(payload)
		if err != nil {
			return nil, 0, fmt.Errorf("decode record at %d: %w", offset, err)
		}
		for _, entry := range diff {
			if entry.Value == nil {
				delete(leaves, entry.Path)
				continue
			}
			leaves[entry.Path] = entry.Value
		}

		offset += int64(4 + len(payload))
	}

	return leaves, offset, nil
}

func decodeRecord(payload []byte) ([]domain.StateDiffEntry, error) {
	if len(payload) < 4 {
		return nil, fmt.Errorf("record too short for entry count: %d bytes", len(payload))
	}
	numEntries := binary.BigEndian.Uint32(payload)
	pos := 4

	entries := make([]domain.StateDiffEntry, 0, numEntries)
	for i := uint32(0); i < numEntries; i++ {
		if pos+32+4 > len(payload) {
			return nil, fmt.Errorf("record truncated at entry %d", i)
		}
		var path trie.Path
		copy(path[:], payload[pos:pos+32])
		pos += 32

		valueLen := binary.BigEndian.Uint32(payload[pos : pos+4])
		pos += 4

		if valueLen == deleteSentinel {
			entries = append(entries, domain.StateDiffEntry{Path: path, Value: nil})
			continue
		}
		if pos+int(valueLen) > len(payload) {
			return nil, fmt.Errorf("record truncated at entry %d value", i)
		}
		value := make([]byte, valueLen)
		copy(value, payload[pos:pos+int(valueLen)])
		pos += int(valueLen)

		entries = append(entries, domain.StateDiffEntry{Path: path, Value: value})
	}

	if pos != len(payload) {
		return nil, fmt.Errorf("record has %d trailing bytes", len(payload)-pos)
	}
	return entries, nil
}
