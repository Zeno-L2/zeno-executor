package codec

import (
	"bytes"
	"fmt"
)

// AssetFlowEntry is one per-asset flow record in a batch commitment.
type AssetFlowEntry struct {
	Asset           AssetID
	DepositCredit   U256
	WithdrawalDebit U256
}

// AssetFlowSummary is a bounded, ascending-by-asset list of
// AssetFlowEntry.
type AssetFlowSummary []AssetFlowEntry

// Encode returns the canonical encoding of s.
func (s AssetFlowSummary) Encode() ([]byte, error) {
	w := &writer{}
	if err := encodeAssetFlowSummaryBody(w, s); err != nil {
		return nil, fmt.Errorf("codec: encode asset flow summary: %w", err)
	}
	return w.buf, nil
}

// DecodeAssetFlowSummary decodes a canonical AssetFlowSummary.
func DecodeAssetFlowSummary(b []byte) (AssetFlowSummary, error) {
	r := newReader(b)
	s, err := decodeAssetFlowSummaryBody(r)
	if err != nil {
		return nil, fmt.Errorf("codec: decode asset flow summary: %w", err)
	}
	if err := r.end(); err != nil {
		return nil, fmt.Errorf("codec: decode asset flow summary: %w", err)
	}
	return s, nil
}

func encodeAssetFlowSummaryBody(w *writer, s AssetFlowSummary) error {
	if len(s) > MaxAssetsPerBatch {
		return ErrLimitExceeded
	}
	for i, e := range s {
		if err := e.Asset.Validate(); err != nil {
			return err
		}
		if e.DepositCredit == (U256{}) && e.WithdrawalDebit == (U256{}) {
			return ErrEmptyAssetFlowEntry
		}
		if i > 0 {
			switch bytes.Compare(e.Asset[:], s[i-1].Asset[:]) {
			case 0:
				return ErrDuplicateKey
			case -1:
				return ErrNotSorted
			}
		}
	}

	w.u32(uint32(len(s)))
	for _, e := range s {
		w.raw(e.Asset[:])
		w.u256(e.DepositCredit)
		w.u256(e.WithdrawalDebit)
	}
	return nil
}

// minAssetFlowEntryEncodedSize is the encoded size of one AssetFlowEntry: asset_id(32) +
// deposit_credit(32) + withdrawal_debit(32).
const minAssetFlowEntryEncodedSize = 32 + 32 + 32

func decodeAssetFlowSummaryBody(r *reader) (AssetFlowSummary, error) {
	count, err := r.count(minAssetFlowEntryEncodedSize)
	if err != nil {
		return nil, err
	}
	if count > MaxAssetsPerBatch {
		return nil, ErrLimitExceeded
	}

	out := make(AssetFlowSummary, 0, count)
	for i := uint32(0); i < count; i++ {
		asset, err := r.assetID()
		if err != nil {
			return nil, err
		}
		credit, err := r.u256()
		if err != nil {
			return nil, err
		}
		debit, err := r.u256()
		if err != nil {
			return nil, err
		}
		if credit == (U256{}) && debit == (U256{}) {
			return nil, ErrEmptyAssetFlowEntry
		}
		if i > 0 {
			switch bytes.Compare(asset[:], out[i-1].Asset[:]) {
			case 0:
				return nil, ErrDuplicateKey
			case -1:
				return nil, ErrNotSorted
			}
		}
		out = append(out, AssetFlowEntry{Asset: asset, DepositCredit: credit, WithdrawalDebit: debit})
	}
	return out, nil
}
