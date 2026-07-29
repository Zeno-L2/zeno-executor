package codec

import (
	"bytes"
	"fmt"

	"github.com/zenon-network/go-zenon/common/crypto"
	"github.com/zenon-network/go-zenon/common/trie"
	"github.com/zenon-network/go-zenon/common/types"
)

// WitnessEntry is one SMT witness in a DA bundle record (§20): an inclusion proof for a read
// or written present key, or a non-inclusion proof for an absent observation.
type WitnessEntry struct {
	DerivedKey trie.Path
	Proof      []byte
}

// BundleInputRecord is one input's entry in the canonical DA bundle (§20).
type BundleInputRecord struct {
	GlobalInputIndex uint64
	InputData        []byte
	Witnesses        []WitnessEntry
	ExecutionResult  []byte
}

// Encode returns the canonical encoding of r. Witnesses MUST be strictly ascending by
// DerivedKey with no duplicates.
func (r BundleInputRecord) Encode() ([]byte, error) {
	w := &writer{}
	if err := encodeBundleInputRecordBody(w, r); err != nil {
		return nil, fmt.Errorf("codec: encode bundle input record: %w", err)
	}
	return w.buf, nil
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
		w.bytes(we.Proof)
	}
	w.bytes(rec.ExecutionResult)
	return nil
}

// minWitnessEntryEncodedSize is the smallest possible encoded WitnessEntry: derivedKey(32) +
// proof length(4).
const minWitnessEntryEncodedSize = 32 + 4

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
		witnesses = append(witnesses, WitnessEntry{DerivedKey: key, Proof: proof})
	}
	rec.Witnesses = witnesses

	if rec.ExecutionResult, err = r.bytes(^uint32(0)); err != nil {
		return rec, err
	}
	return rec, nil
}

// EncodeBundle returns the canonical DA bundle encoding: the concatenation, with no
// separators and no outer framing, of each record's encoding in the given slice order.
// Records MUST be strictly ascending by GlobalInputIndex with no duplicates (§20).
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

// BundleHash returns DAHash = SHA3-256(EncodeBundle(records)).
func BundleHash(records []BundleInputRecord) (types.Hash, error) {
	b, err := EncodeBundle(records)
	if err != nil {
		return types.Hash{}, fmt.Errorf("codec: bundle hash: %w", err)
	}
	return hashOf(crypto.Hash(b)), nil
}
