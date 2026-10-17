package codec

import (
	"bytes"
	"errors"
	"testing"

	"github.com/zenon-network/go-zenon/common/trie"
)

func sampleBundleRecords(t testing.TB) []BundleInputRecord {
	t.Helper()

	// A small leaf set, used only to produce a real non-inclusion proof via the path-native
	// trie API (scripts/check-trie-imports.sh requires path-native calls only).
	present := fillPath(0x01)
	absent := fillPath(0x02)
	paths := []trie.Path{present}
	values := [][]byte{[]byte("v")}

	_, _, presentProof, err := trie.ProveByPath(paths, values, present)
	if err != nil {
		t.Fatalf("ProveByPath(present): %v", err)
	}
	_, _, absentProof, err := trie.ProveByPath(paths, values, absent)
	if err != nil {
		t.Fatalf("ProveByPath(absent): %v", err)
	}

	rec0 := BundleInputRecord{
		GlobalInputIndex: 0,
		InputData:        []byte("input-0"),
		Witnesses: []WitnessEntry{
			{DerivedKey: present, Value: []byte("v"), Proof: presentProof},
			{DerivedKey: absent, Value: nil, Proof: absentProof},
		},
		ExecutionResult: []byte("result-0"),
	}
	rec1 := BundleInputRecord{
		GlobalInputIndex: 1,
		InputData:        []byte("input-1"),
		Witnesses:        nil,
		ExecutionResult:  []byte("result-1"),
	}
	return []BundleInputRecord{rec0, rec1}
}

func TestBundleRoundTrip(t *testing.T) {
	records := sampleBundleRecords(t)
	b, err := EncodeBundle(records)
	if err != nil {
		t.Fatalf("EncodeBundle: %v", err)
	}
	got, err := DecodeBundle(b)
	if err != nil {
		t.Fatalf("DecodeBundle: %v", err)
	}
	if len(got) != len(records) {
		t.Fatalf("got %d records, want %d", len(got), len(records))
	}
	for i, rec := range records {
		if got[i].GlobalInputIndex != rec.GlobalInputIndex ||
			!bytes.Equal(got[i].InputData, rec.InputData) ||
			!bytes.Equal(got[i].ExecutionResult, rec.ExecutionResult) ||
			len(got[i].Witnesses) != len(rec.Witnesses) {
			t.Errorf("record %d: got %+v, want %+v", i, got[i], rec)
		}
		for j := range rec.Witnesses {
			if got[i].Witnesses[j].DerivedKey != rec.Witnesses[j].DerivedKey ||
				!bytes.Equal(got[i].Witnesses[j].Value, rec.Witnesses[j].Value) ||
				(got[i].Witnesses[j].Value == nil) != (rec.Witnesses[j].Value == nil) ||
				!bytes.Equal(got[i].Witnesses[j].Proof, rec.Witnesses[j].Proof) {
				t.Errorf("record %d witness %d: got %+v, want %+v", i, j, got[i].Witnesses[j], rec.Witnesses[j])
			}
		}
	}

	rebuilt, err := EncodeBundle(got)
	if err != nil {
		t.Fatalf("re-EncodeBundle: %v", err)
	}
	if !bytes.Equal(rebuilt, b) {
		t.Errorf("round trip mismatch: got %x, want %x", rebuilt, b)
	}
}

func TestBundleOutOfOrderRecords(t *testing.T) {
	records := sampleBundleRecords(t)
	reversed := []BundleInputRecord{records[1], records[0]}
	if _, err := EncodeBundle(reversed); !errors.Is(err, ErrNotSorted) {
		t.Fatalf("got %v, want ErrNotSorted", err)
	}
}

func TestBundleDuplicateAndOutOfOrderWitnesses(t *testing.T) {
	records := sampleBundleRecords(t)

	t.Run("duplicate witness", func(t *testing.T) {
		rec := records[0]
		rec.Witnesses = []WitnessEntry{rec.Witnesses[0], rec.Witnesses[0]}
		if _, err := EncodeBundle([]BundleInputRecord{rec}); !errors.Is(err, ErrDuplicateKey) {
			t.Fatalf("got %v, want ErrDuplicateKey", err)
		}
	})

	t.Run("out of order witnesses", func(t *testing.T) {
		rec := records[0]
		rec.Witnesses = []WitnessEntry{rec.Witnesses[1], rec.Witnesses[0]}
		if bytes.Compare(rec.Witnesses[0].DerivedKey[:], rec.Witnesses[1].DerivedKey[:]) < 0 {
			t.Skip("witnesses already ascending after swap; construction assumption violated")
		}
		if _, err := EncodeBundle([]BundleInputRecord{rec}); !errors.Is(err, ErrNotSorted) {
			t.Fatalf("got %v, want ErrNotSorted", err)
		}
	})
}

func TestBundleTruncatedTrailingRecord(t *testing.T) {
	records := sampleBundleRecords(t)
	b, err := EncodeBundle(records)
	if err != nil {
		t.Fatalf("EncodeBundle: %v", err)
	}
	// Append a partial trailing record: three bytes is not enough for even the fixed-width
	// globalInputIndex (u64) field the next record must start with.
	torn := append(append([]byte{}, b...), 0x00, 0x00, 0x00)
	if _, err := DecodeBundle(torn); !errors.Is(err, ErrTruncated) {
		t.Fatalf("got %v, want ErrTruncated", err)
	}
}

func TestBundleEmptyRecordSetRoundTrips(t *testing.T) {
	b, err := EncodeBundle(nil)
	if err != nil {
		t.Fatalf("EncodeBundle(nil): %v", err)
	}
	if len(b) != 0 {
		t.Fatalf("EncodeBundle(nil) = %x, want empty", b)
	}
	got, err := DecodeBundle(b)
	if err != nil {
		t.Fatalf("DecodeBundle(empty): %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("DecodeBundle(empty) = %+v, want no records", got)
	}
}

// TestBundleKind3DeployChunkData covers the kind-3-scoped
// DeployChunkData field: it round-trips for a kind-3 record, is
// rejected on a non-kind-3 record, does not panic on an empty
// InputData, and leaves a kind-4 record's encoding unchanged.
func TestBundleKind3DeployChunkData(t *testing.T) {
	t.Run("Kind3RoundTrips", func(t *testing.T) {
		rec := BundleInputRecord{
			GlobalInputIndex: 0,
			InputData:        []byte{3, 0xAA, 0xBB},
			ExecutionResult:  []byte("result-0"),
			DeployChunkData:  []byte("chunk-bytes"),
		}
		b, err := EncodeBundle([]BundleInputRecord{rec})
		if err != nil {
			t.Fatalf("EncodeBundle: %v", err)
		}
		got, err := DecodeBundle(b)
		if err != nil {
			t.Fatalf("DecodeBundle: %v", err)
		}
		if len(got) != 1 {
			t.Fatalf("got %d records, want 1", len(got))
		}
		if !bytes.Equal(got[0].DeployChunkData, rec.DeployChunkData) {
			t.Errorf("DeployChunkData = %q, want %q", got[0].DeployChunkData, rec.DeployChunkData)
		}
		rebuilt, err := EncodeBundle(got)
		if err != nil {
			t.Fatalf("re-EncodeBundle: %v", err)
		}
		if !bytes.Equal(rebuilt, b) {
			t.Errorf("round trip mismatch: got %x, want %x", rebuilt, b)
		}
	})

	t.Run("NonKind3WithDeployChunkDataRejected", func(t *testing.T) {
		rec := BundleInputRecord{
			GlobalInputIndex: 0,
			InputData:        []byte{0, 0xAA, 0xBB},
			ExecutionResult:  []byte("result-0"),
			DeployChunkData:  []byte("chunk-bytes"),
		}
		if _, err := EncodeBundle([]BundleInputRecord{rec}); !errors.Is(err, ErrInvalidKind) {
			t.Fatalf("got %v, want ErrInvalidKind", err)
		}
	})

	t.Run("EmptyInputDataDoesNotPanic", func(t *testing.T) {
		rec := BundleInputRecord{
			GlobalInputIndex: 0,
			InputData:        []byte{},
			ExecutionResult:  []byte("result-0"),
		}
		b, err := EncodeBundle([]BundleInputRecord{rec})
		if err != nil {
			t.Fatalf("EncodeBundle: %v", err)
		}
		if _, err := DecodeBundle(b); err != nil {
			t.Fatalf("DecodeBundle: %v", err)
		}
	})

	t.Run("Kind4EncodingUnchanged", func(t *testing.T) {
		rec := BundleInputRecord{
			GlobalInputIndex: 0,
			InputData:        []byte{4, 0xAA, 0xBB},
			ExecutionResult:  []byte("result-0"),
		}
		b, err := EncodeBundle([]BundleInputRecord{rec})
		if err != nil {
			t.Fatalf("EncodeBundle: %v", err)
		}
		got, err := DecodeBundle(b)
		if err != nil {
			t.Fatalf("DecodeBundle: %v", err)
		}
		if len(got) != 1 || got[0].DeployChunkData != nil {
			t.Fatalf("got %+v, want a single record with nil DeployChunkData", got)
		}
		rebuilt, err := EncodeBundle(got)
		if err != nil {
			t.Fatalf("re-EncodeBundle: %v", err)
		}
		if !bytes.Equal(rebuilt, b) {
			t.Errorf("round trip mismatch: got %x, want %x", rebuilt, b)
		}
	})
}

func TestBundleWitnessValuePresentEmptyAndAbsent(t *testing.T) {
	records := sampleBundleRecords(t)
	present := records[0].Witnesses[0]
	absent := records[0].Witnesses[1]

	present.Value = []byte{}
	rec := BundleInputRecord{
		GlobalInputIndex: 0,
		InputData:        []byte("input-0"),
		Witnesses:        []WitnessEntry{present},
		ExecutionResult:  []byte("result-0"),
	}
	b, err := EncodeBundle([]BundleInputRecord{rec})
	if err != nil {
		t.Fatalf("EncodeBundle: %v", err)
	}
	got, err := DecodeBundle(b)
	if err != nil {
		t.Fatalf("DecodeBundle: %v", err)
	}
	if got[0].Witnesses[0].Value == nil || len(got[0].Witnesses[0].Value) != 0 {
		t.Fatalf("present-empty: got %+v, want non-nil zero-length", got[0].Witnesses[0].Value)
	}

	absent.Value = nil
	rec = BundleInputRecord{
		GlobalInputIndex: 0,
		InputData:        []byte("input-0"),
		Witnesses:        []WitnessEntry{absent},
		ExecutionResult:  []byte("result-0"),
	}
	b, err = EncodeBundle([]BundleInputRecord{rec})
	if err != nil {
		t.Fatalf("EncodeBundle: %v", err)
	}
	got, err = DecodeBundle(b)
	if err != nil {
		t.Fatalf("DecodeBundle: %v", err)
	}
	if got[0].Witnesses[0].Value != nil {
		t.Fatalf("absent: got %+v, want nil", got[0].Witnesses[0].Value)
	}
}
