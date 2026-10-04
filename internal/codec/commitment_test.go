package codec

import (
	"errors"
	"testing"

	"github.com/zenon-network/go-zenon/common/types"
)

func sampleAssetFlowSummary() AssetFlowSummary {
	a1 := AssetIDFromZTS(types.QsrTokenStandard)
	a2 := AssetIDFromZTS(types.ZnnTokenStandard)
	// QSR sorts before ZNN by byte comparison of their canonical AssetID encodings.
	if bytesGreater(a1[:], a2[:]) {
		a1, a2 = a2, a1
	}
	return AssetFlowSummary{
		{Asset: a1, DepositCredit: U256{31: 10}, WithdrawalDebit: U256{31: 1}},
		{Asset: a2, DepositCredit: U256{31: 0}, WithdrawalDebit: U256{31: 2}},
	}
}

func bytesGreater(a, b []byte) bool {
	for i := range a {
		if a[i] != b[i] {
			return a[i] > b[i]
		}
	}
	return false
}

func TestAssetFlowSummaryRoundTrip(t *testing.T) {
	s := sampleAssetFlowSummary()
	b, err := s.Encode()
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	got, err := DecodeAssetFlowSummary(b)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if len(got) != len(s) {
		t.Fatalf("got %d entries, want %d", len(got), len(s))
	}
	for i := range s {
		if got[i] != s[i] {
			t.Errorf("entry %d: got %+v, want %+v", i, got[i], s[i])
		}
	}
}

func TestAssetFlowSummaryUnsortedDuplicateBothZero(t *testing.T) {
	s := sampleAssetFlowSummary()

	t.Run("unsorted", func(t *testing.T) {
		bad := AssetFlowSummary{s[1], s[0]}
		if _, err := bad.Encode(); !errors.Is(err, ErrNotSorted) {
			t.Fatalf("got %v, want ErrNotSorted", err)
		}
	})

	t.Run("duplicate", func(t *testing.T) {
		bad := AssetFlowSummary{s[0], s[0]}
		if _, err := bad.Encode(); !errors.Is(err, ErrDuplicateKey) && !errors.Is(err, ErrNotSorted) {
			t.Fatalf("got %v, want ErrDuplicateKey or ErrNotSorted", err)
		}
	})

	t.Run("both zero", func(t *testing.T) {
		bad := AssetFlowSummary{{Asset: s[0].Asset, DepositCredit: U256{}, WithdrawalDebit: U256{}}}
		if _, err := bad.Encode(); !errors.Is(err, ErrEmptyAssetFlowEntry) {
			t.Fatalf("got %v, want ErrEmptyAssetFlowEntry", err)
		}
	})
}

func TestAssetFlowSummaryOverCap(t *testing.T) {
	var s AssetFlowSummary
	for i := 0; i < MaxAssetsPerBatch+1; i++ {
		var a AssetID
		a[31] = byte(i)
		a[30] = byte(i >> 8)
		s = append(s, AssetFlowEntry{Asset: a, DepositCredit: U256{31: 1}})
	}
	if _, err := s.Encode(); !errors.Is(err, ErrLimitExceeded) {
		t.Fatalf("got %v, want ErrLimitExceeded", err)
	}
}

func sampleReceipt() Receipt {
	return Receipt{
		Version:       ReceiptVersion,
		InputIndex:    3,
		InputHash:     fillHash(0x01),
		BatchID:       7,
		ContractID:    fillHash(0x02),
		CodeHash:      fillHash(0x03),
		Status:        StatusSuccess,
		GasUsed:       12345,
		AbortCode:     -7,
		StateDiffHash: fillHash(0x04),
		ReturnHash:    fillHash(0x05),
		EventRoot:     fillHash(0x06),
		EventCount:    2,
		OutboxRoot:    fillHash(0x07),
		OutboxCount:   1,
	}
}

func TestReceiptRoundTrip(t *testing.T) {
	rec := sampleReceipt()
	b, err := rec.Encode()
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	got, err := DecodeReceipt(b)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if got != rec {
		t.Errorf("got %+v, want %+v", got, rec)
	}
}

func TestReceiptUnknownVersion(t *testing.T) {
	rec := sampleReceipt()
	rec.Version = ReceiptVersion + 1
	w := &writer{}
	w.u16(rec.Version)
	w.raw(make([]byte, 8)) // garbage
	if _, err := DecodeReceipt(w.buf); !errors.Is(err, ErrUnknownVersion) {
		t.Fatalf("got %v, want ErrUnknownVersion", err)
	}
}

func TestReceiptUnknownStatus(t *testing.T) {
	rec := sampleReceipt()
	b, err := rec.Encode()
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	// status byte offset: version(2)+input_index(8)+input_hash(32)+batch_id(8)+contract_id(32)+code_hash(32) = 114
	b[114] = 9
	if _, err := DecodeReceipt(b); !errors.Is(err, ErrInvalidStatus) {
		t.Fatalf("got %v, want ErrInvalidStatus", err)
	}
}

func TestReceiptStorageQuotaExceededRoundTrip(t *testing.T) {
	rec := sampleReceipt()
	rec.Status = StatusStorageQuotaExceeded
	b, err := rec.Encode()
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	got, err := DecodeReceipt(b)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if got != rec {
		t.Errorf("got %+v, want %+v", got, rec)
	}
	if got.Status.String() != "STORAGE_QUOTA_EXCEEDED" {
		t.Errorf("String: got %q", got.Status.String())
	}
}

func TestReceiptAbortCodeRoundTrip(t *testing.T) {
	for _, abortCode := range []int32{42, -42, 0} {
		rec := sampleReceipt()
		rec.AbortCode = abortCode
		b, err := rec.Encode()
		if err != nil {
			t.Fatalf("Encode(%d): %v", abortCode, err)
		}
		got, err := DecodeReceipt(b)
		if err != nil {
			t.Fatalf("Decode(%d): %v", abortCode, err)
		}
		if got != rec {
			t.Errorf("abort_code %d: got %+v, want %+v", abortCode, got, rec)
		}
	}
}

func TestReceiptVersion1AcceptedVersion2Rejected(t *testing.T) {
	rec := sampleReceipt()
	b, err := rec.Encode()
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	if _, err := DecodeReceipt(b); err != nil {
		t.Fatalf("version 1: %v", err)
	}
	b[1] = 2 // receipt_version = 2, big-endian u16 low byte
	if _, err := DecodeReceipt(b); !errors.Is(err, ErrUnknownVersion) {
		t.Fatalf("got %v, want ErrUnknownVersion", err)
	}
}

func TestReceiptTruncated(t *testing.T) {
	rec := sampleReceipt()
	b, err := rec.Encode()
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	if _, err := DecodeReceipt(b[:len(b)-1]); !errors.Is(err, ErrTruncated) {
		t.Fatalf("got %v, want ErrTruncated", err)
	}
}

func sampleCommitment() BatchCommitment {
	return BatchCommitment{
		Versions: Versions{
			ProtocolVersion:        ProtocolVersion,
			RuntimeProfileVersion:  RuntimeProfileVersion,
			MeteringVersion:        MeteringVersion,
			SerializationVersion:   SerializationVersion,
			StateDiffVersion:       StateDiffVersion,
			ExecutionResultVersion: ExecutionResultVersion,
			ReceiptVersion:         ReceiptVersion,
			OutboxVersion:          OutboxVersion,
			DAModeVersion:          DAModeVersion,
		},
		DomainID:          1,
		BatchID:           2,
		FirstInputSeq:     3,
		LastInputSeq:      4,
		PreStateRoot:      fillHash(0x10),
		PostStateRoot:     fillHash(0x11),
		InputRoot:         fillHash(0x12),
		ReceiptRoot:       fillHash(0x13),
		EventRoot:         fillHash(0x14),
		OutboxRoot:        fillHash(0x15),
		DAHash:            fillHash(0x16),
		DAMode:            0,
		ExecutorID:        types.Address{1, 2, 3, 4},
		SubmittedAtHeight: 500,
		AssetFlowSummary:  sampleAssetFlowSummary(),
		ProofData:         nil,
	}
}

func TestBatchCommitmentRoundTrip(t *testing.T) {
	c := sampleCommitment()
	b, err := c.Encode()
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	got, err := DecodeBatchCommitment(b)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if got.DomainID != c.DomainID || got.BatchID != c.BatchID || got.ExecutorID != c.ExecutorID ||
		len(got.AssetFlowSummary) != len(c.AssetFlowSummary) {
		t.Errorf("got %+v, want %+v", got, c)
	}
	if got.Versions != c.Versions {
		t.Errorf("versions: got %+v, want %+v", got.Versions, c.Versions)
	}
}

func TestBatchCommitmentUnknownProtocolVersionReadsNoFurtherField(t *testing.T) {
	c := sampleCommitment()
	w := &writer{}
	w.u16(ProtocolVersion + 1)
	w.raw([]byte{0xFF, 0xFF}) // truncated remainder: proves nothing else was parsed
	_ = c
	if _, err := DecodeBatchCommitment(w.buf); !errors.Is(err, ErrUnknownVersion) {
		t.Fatalf("got %v, want ErrUnknownVersion", err)
	}
}

func TestBatchCommitmentEachVersionFieldMismatched(t *testing.T) {
	base := sampleCommitment()

	mutate := func(mutator func(*Versions)) BatchCommitment {
		c := base
		mutator(&c.Versions)
		return c
	}

	cases := []struct {
		name string
		c    BatchCommitment
	}{
		{"runtime_profile_version", mutate(func(v *Versions) { v.RuntimeProfileVersion++ })},
		{"metering_version", mutate(func(v *Versions) { v.MeteringVersion++ })},
		{"serialization_version", mutate(func(v *Versions) { v.SerializationVersion++ })},
		{"statediff_version", mutate(func(v *Versions) { v.StateDiffVersion++ })},
		{"executionresult_version", mutate(func(v *Versions) { v.ExecutionResultVersion++ })},
		{"receipt_version", mutate(func(v *Versions) { v.ReceiptVersion++ })},
		{"outbox_version", mutate(func(v *Versions) { v.OutboxVersion++ })},
		{"damode_version", mutate(func(v *Versions) { v.DAModeVersion++ })},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			b := encodeVersionsRaw(tc.c.Versions)
			if _, err := DecodeBatchCommitment(b); !errors.Is(err, ErrUnknownVersion) {
				t.Fatalf("got %v, want ErrUnknownVersion", err)
			}
		})
	}
}

func TestBatchCommitmentEncodeRejectsVersionMismatch(t *testing.T) {
	base := sampleCommitment()

	mutate := func(mutator func(*Versions)) BatchCommitment {
		c := base
		mutator(&c.Versions)
		return c
	}

	cases := []struct {
		name string
		c    BatchCommitment
	}{
		{"protocol_version", mutate(func(v *Versions) { v.ProtocolVersion++ })},
		{"runtime_profile_version", mutate(func(v *Versions) { v.RuntimeProfileVersion++ })},
		{"metering_version", mutate(func(v *Versions) { v.MeteringVersion++ })},
		{"serialization_version", mutate(func(v *Versions) { v.SerializationVersion++ })},
		{"statediff_version", mutate(func(v *Versions) { v.StateDiffVersion++ })},
		{"executionresult_version", mutate(func(v *Versions) { v.ExecutionResultVersion++ })},
		{"receipt_version", mutate(func(v *Versions) { v.ReceiptVersion++ })},
		{"outbox_version", mutate(func(v *Versions) { v.OutboxVersion++ })},
		{"damode_version", mutate(func(v *Versions) { v.DAModeVersion++ })},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := tc.c.Encode(); !errors.Is(err, ErrUnknownVersion) {
				t.Fatalf("got %v, want ErrUnknownVersion", err)
			}
		})
	}
}

// encodeVersionsRaw encodes only the nine version fields, so a mismatch on any but the first
// is exercised without needing the rest of the commitment to be well-formed.
func encodeVersionsRaw(v Versions) []byte {
	w := &writer{}
	w.u16(v.ProtocolVersion)
	w.u16(v.RuntimeProfileVersion)
	w.u16(v.MeteringVersion)
	w.u16(v.SerializationVersion)
	w.u16(v.StateDiffVersion)
	w.u16(v.ExecutionResultVersion)
	w.u16(v.ReceiptVersion)
	w.u16(v.OutboxVersion)
	w.u16(v.DAModeVersion)
	return w.buf
}

func TestBatchCommitmentNonEmptyProofData(t *testing.T) {
	c := sampleCommitment()
	c.ProofData = []byte{0x01}
	if _, err := c.Encode(); !errors.Is(err, ErrNonEmptyProofData) {
		t.Fatalf("Encode: got %v, want ErrNonEmptyProofData", err)
	}

	good := sampleCommitment()
	b, err := good.Encode()
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	b = append(b[:len(b)-4], 0, 0, 0, 1, 0xAA) // rewrite the zero-length proofData as one byte
	if _, err := DecodeBatchCommitment(b); !errors.Is(err, ErrNonEmptyProofData) {
		t.Fatalf("Decode: got %v, want ErrNonEmptyProofData", err)
	}
}

func TestBatchCommitmentTrailingBytes(t *testing.T) {
	c := sampleCommitment()
	b, err := c.Encode()
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	b = append(b, 0x00)
	if _, err := DecodeBatchCommitment(b); !errors.Is(err, ErrTrailingBytes) {
		t.Fatalf("got %v, want ErrTrailingBytes", err)
	}
}
