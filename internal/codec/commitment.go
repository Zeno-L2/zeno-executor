package codec

import (
	"fmt"

	"github.com/zenon-network/go-zenon/common/types"
)

// Versions is the nine-field version block a batch commitment carries
// first. No constructor: every field is the corresponding package
// constant; tests set fields directly to exercise mismatches.
type Versions struct {
	ProtocolVersion        uint16
	RuntimeProfileVersion  uint16
	MeteringVersion        uint16
	SerializationVersion   uint16
	StateDiffVersion       uint16
	ExecutionResultVersion uint16
	ReceiptVersion         uint16
	OutboxVersion          uint16
	DAModeVersion          uint16
}

// BatchCommitment is the on-chain-committed summary of one batch.
type BatchCommitment struct {
	Versions          Versions
	DomainID          uint32
	BatchID           uint64
	FirstInputSeq     uint64
	LastInputSeq      uint64
	PreStateRoot      types.Hash
	PostStateRoot     types.Hash
	InputRoot         types.Hash
	ReceiptRoot       types.Hash
	EventRoot         types.Hash
	OutboxRoot        types.Hash
	DAHash            types.Hash
	DAMode            uint8
	ExecutorID        types.Address
	SubmittedAtHeight uint64
	AssetFlowSummary  AssetFlowSummary
	ProofData         []byte
}

// Encode returns the canonical encoding of c. ProofData must be the
// zero-length encoding; a non-empty ProofData is rejected with
// ErrNonEmptyProofData. Each of the nine Versions fields must equal
// its package constant, checked in declaration order, first mismatch
// wins, so Encode never emits bytes DecodeBatchCommitment would
// reject.
func (c BatchCommitment) Encode() ([]byte, error) {
	versionFields := []struct {
		want uint16
		got  uint16
	}{
		{ProtocolVersion, c.Versions.ProtocolVersion},
		{RuntimeProfileVersion, c.Versions.RuntimeProfileVersion},
		{MeteringVersion, c.Versions.MeteringVersion},
		{SerializationVersion, c.Versions.SerializationVersion},
		{StateDiffVersion, c.Versions.StateDiffVersion},
		{ExecutionResultVersion, c.Versions.ExecutionResultVersion},
		{ReceiptVersion, c.Versions.ReceiptVersion},
		{OutboxVersion, c.Versions.OutboxVersion},
		{DAModeVersion, c.Versions.DAModeVersion},
	}
	for _, field := range versionFields {
		if field.got != field.want {
			return nil, fmt.Errorf("codec: encode batch commitment: %w", ErrUnknownVersion)
		}
	}

	if len(c.ProofData) != 0 {
		return nil, fmt.Errorf("codec: encode batch commitment: %w", ErrNonEmptyProofData)
	}

	w := &writer{}
	w.u16(c.Versions.ProtocolVersion)
	w.u16(c.Versions.RuntimeProfileVersion)
	w.u16(c.Versions.MeteringVersion)
	w.u16(c.Versions.SerializationVersion)
	w.u16(c.Versions.StateDiffVersion)
	w.u16(c.Versions.ExecutionResultVersion)
	w.u16(c.Versions.ReceiptVersion)
	w.u16(c.Versions.OutboxVersion)
	w.u16(c.Versions.DAModeVersion)
	w.u32(c.DomainID)
	w.u64(c.BatchID)
	w.u64(c.FirstInputSeq)
	w.u64(c.LastInputSeq)
	w.hash(c.PreStateRoot)
	w.hash(c.PostStateRoot)
	w.hash(c.InputRoot)
	w.hash(c.ReceiptRoot)
	w.hash(c.EventRoot)
	w.hash(c.OutboxRoot)
	w.hash(c.DAHash)
	w.u8(c.DAMode)
	w.address(c.ExecutorID)
	w.u64(c.SubmittedAtHeight)
	if err := encodeAssetFlowSummaryBody(w, c.AssetFlowSummary); err != nil {
		return nil, fmt.Errorf("codec: encode batch commitment: %w", err)
	}
	w.bytes(c.ProofData)
	return w.buf, nil
}

// DecodeBatchCommitment decodes a canonical BatchCommitment.
// protocol_version is read and checked first, before any other byte is
// interpreted; each remaining version field is then checked in turn,
// rejecting a mismatch with ErrUnknownVersion before the body is read.
// proofData must be the zero-length encoding (ErrNonEmptyProofData
// otherwise), and trailing bytes after it are rejected.
func DecodeBatchCommitment(b []byte) (BatchCommitment, error) {
	r := newReader(b)
	var c BatchCommitment

	expected := []struct {
		want uint16
		dst  *uint16
	}{
		{ProtocolVersion, &c.Versions.ProtocolVersion},
		{RuntimeProfileVersion, &c.Versions.RuntimeProfileVersion},
		{MeteringVersion, &c.Versions.MeteringVersion},
		{SerializationVersion, &c.Versions.SerializationVersion},
		{StateDiffVersion, &c.Versions.StateDiffVersion},
		{ExecutionResultVersion, &c.Versions.ExecutionResultVersion},
		{ReceiptVersion, &c.Versions.ReceiptVersion},
		{OutboxVersion, &c.Versions.OutboxVersion},
		{DAModeVersion, &c.Versions.DAModeVersion},
	}
	for _, field := range expected {
		v, err := r.u16()
		if err != nil {
			return c, fmt.Errorf("codec: decode batch commitment: %w", err)
		}
		if v != field.want {
			return c, fmt.Errorf("codec: decode batch commitment: %w", ErrUnknownVersion)
		}
		*field.dst = v
	}

	var err error
	if c.DomainID, err = r.u32(); err != nil {
		return c, fmt.Errorf("codec: decode batch commitment: %w", err)
	}
	if c.BatchID, err = r.u64(); err != nil {
		return c, fmt.Errorf("codec: decode batch commitment: %w", err)
	}
	if c.FirstInputSeq, err = r.u64(); err != nil {
		return c, fmt.Errorf("codec: decode batch commitment: %w", err)
	}
	if c.LastInputSeq, err = r.u64(); err != nil {
		return c, fmt.Errorf("codec: decode batch commitment: %w", err)
	}
	if c.PreStateRoot, err = r.hash(); err != nil {
		return c, fmt.Errorf("codec: decode batch commitment: %w", err)
	}
	if c.PostStateRoot, err = r.hash(); err != nil {
		return c, fmt.Errorf("codec: decode batch commitment: %w", err)
	}
	if c.InputRoot, err = r.hash(); err != nil {
		return c, fmt.Errorf("codec: decode batch commitment: %w", err)
	}
	if c.ReceiptRoot, err = r.hash(); err != nil {
		return c, fmt.Errorf("codec: decode batch commitment: %w", err)
	}
	if c.EventRoot, err = r.hash(); err != nil {
		return c, fmt.Errorf("codec: decode batch commitment: %w", err)
	}
	if c.OutboxRoot, err = r.hash(); err != nil {
		return c, fmt.Errorf("codec: decode batch commitment: %w", err)
	}
	if c.DAHash, err = r.hash(); err != nil {
		return c, fmt.Errorf("codec: decode batch commitment: %w", err)
	}
	if c.DAMode, err = r.u8(); err != nil {
		return c, fmt.Errorf("codec: decode batch commitment: %w", err)
	}
	if c.ExecutorID, err = r.address(); err != nil {
		return c, fmt.Errorf("codec: decode batch commitment: %w", err)
	}
	if c.SubmittedAtHeight, err = r.u64(); err != nil {
		return c, fmt.Errorf("codec: decode batch commitment: %w", err)
	}

	afs, err := decodeAssetFlowSummaryBody(r)
	if err != nil {
		return c, fmt.Errorf("codec: decode batch commitment: %w", err)
	}
	c.AssetFlowSummary = afs

	proofData, err := r.bytes(^uint32(0))
	if err != nil {
		return c, fmt.Errorf("codec: decode batch commitment: %w", err)
	}
	if len(proofData) != 0 {
		return c, fmt.Errorf("codec: decode batch commitment: %w", ErrNonEmptyProofData)
	}
	c.ProofData = proofData

	if err := r.end(); err != nil {
		return c, fmt.Errorf("codec: decode batch commitment: %w", err)
	}
	return c, nil
}
