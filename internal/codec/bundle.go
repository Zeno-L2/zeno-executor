package codec

import (
	"bytes"
	"fmt"

	"github.com/zenon-network/go-zenon/common/trie"
)

// WitnessEntry is one SMT witness in a DA bundle record: an inclusion
// proof for a read or written present key, or a non-inclusion proof
// for an absent observation.
type WitnessEntry struct {
	DerivedKey trie.Path
	// Value is the observed leaf value in the state value form: nil
	// means absent (the EMPTY sentinel), a non-nil zero-length slice
	// means present-empty.
	Value []byte
	Proof []byte
}

// BundleInputRecord is one input's entry in the canonical DA bundle.
type BundleInputRecord struct {
	GlobalInputIndex uint64
	InputData        []byte
	Witnesses        []WitnessEntry
	ExecutionResult  []byte
	// DeployChunkData is the kind-3 chunk's raw bytes. It is encoded,
	// as a trailing Bytes field, only for a record whose InputData
	// names input_kind 3; it MUST be nil for every other kind.
	DeployChunkData []byte
}

func encodeBundleInputRecordBody(w *writer, rec BundleInputRecord) error {
	for i := 1; i < len(rec.Witnesses); i++ {
		switch bytes.Compare(rec.Witnesses[i].DerivedKey[:], rec.Witnesses[i-1].DerivedKey[:]) {
		case 0:
			return ErrDuplicateKey
		case -1:
			return ErrNotSorted
		}
	}

	w.u64(rec.GlobalInputIndex)
	w.bytes(rec.InputData)
	w.u32(uint32(len(rec.Witnesses)))
	for _, we := range rec.Witnesses {
		w.path(we.DerivedKey)
		if err := writeStateValue(w, we.Value); err != nil {
			return err
		}
		w.bytes(we.Proof)
	}
	w.bytes(rec.ExecutionResult)

	isKind3 := len(rec.InputData) > 0 && rec.InputData[0] == 3
	if rec.DeployChunkData != nil && !isKind3 {
		return ErrInvalidKind
	}
	if isKind3 {
		w.bytes(rec.DeployChunkData)
	}
	return nil
}

// minWitnessEntryEncodedSize is the smallest possible encoded WitnessEntry: derivedKey(32) +
// value length(4) + proof length(4).
const minWitnessEntryEncodedSize = 32 + 4 + 4

func decodeBundleInputRecordBody(r *reader) (BundleInputRecord, error) {
	var rec BundleInputRecord
	var err error

	if rec.GlobalInputIndex, err = r.u64(); err != nil {
		return rec, err
	}
	if rec.InputData, err = r.bytes(^uint32(0)); err != nil {
		return rec, err
	}

	count, err := r.count(minWitnessEntryEncodedSize)
	if err != nil {
		return rec, err
	}
	witnesses := make([]WitnessEntry, 0, count)
	for i := uint32(0); i < count; i++ {
		key, err := r.path()
		if err != nil {
			return rec, err
		}
		value, err := readStateValue(r)
		if err != nil {
			return rec, err
		}
		proof, err := r.bytes(^uint32(0))
		if err != nil {
			return rec, err
		}
		if i > 0 {
			switch bytes.Compare(key[:], witnesses[i-1].DerivedKey[:]) {
			case 0:
				return rec, ErrDuplicateKey
			case -1:
				return rec, ErrNotSorted
			}
		}
		witnesses = append(witnesses, WitnessEntry{DerivedKey: key, Value: value, Proof: proof})
	}
	rec.Witnesses = witnesses

	if rec.ExecutionResult, err = r.bytes(^uint32(0)); err != nil {
		return rec, err
	}

	if len(rec.InputData) > 0 && rec.InputData[0] == 3 {
		if rec.DeployChunkData, err = r.bytes(^uint32(0)); err != nil {
			return rec, err
		}
	}
	return rec, nil
}

// EncodeBundle returns the canonical DA bundle encoding: the
// concatenation, with no separators and no outer framing, of each
// record's encoding in the given slice order. Records MUST be strictly
// ascending by GlobalInputIndex with no duplicates.
func EncodeBundle(records []BundleInputRecord) ([]byte, error) {
	for i := 1; i < len(records); i++ {
		if records[i].GlobalInputIndex == records[i-1].GlobalInputIndex {
			return nil, fmt.Errorf("codec: encode bundle: %w", ErrDuplicateKey)
		}
		if records[i].GlobalInputIndex < records[i-1].GlobalInputIndex {
			return nil, fmt.Errorf("codec: encode bundle: %w", ErrNotSorted)
		}
	}

	w := &writer{}
	for _, rec := range records {
		if err := encodeBundleInputRecordBody(w, rec); err != nil {
			return nil, fmt.Errorf("codec: encode bundle: %w", err)
		}
	}
	return w.buf, nil
}

// DecodeBundle decodes a canonical DA bundle. It reads self-delimiting records until the
// buffer is exhausted; a partial trailing record is ErrTruncated.
func DecodeBundle(b []byte) ([]BundleInputRecord, error) {
	r := newReader(b)
	var records []BundleInputRecord

	for r.remaining() > 0 {
		rec, err := decodeBundleInputRecordBody(r)
		if err != nil {
			return nil, fmt.Errorf("codec: decode bundle: %w", err)
		}
		if len(records) > 0 {
			prev := records[len(records)-1].GlobalInputIndex
			if rec.GlobalInputIndex == prev {
				return nil, fmt.Errorf("codec: decode bundle: %w", ErrDuplicateKey)
			}
			if rec.GlobalInputIndex < prev {
				return nil, fmt.Errorf("codec: decode bundle: %w", ErrNotSorted)
			}
		}
		records = append(records, rec)
	}
	return records, nil
}
