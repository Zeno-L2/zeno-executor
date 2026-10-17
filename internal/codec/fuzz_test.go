package codec

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

// roundTripper decodes b with a top-level Decode… and, on success, re-encodes the result.
// decoded reports whether the decode phase itself succeeded; encoded/err report the
// separate re-encode phase, which the canonical round-trip property requires to always
// succeed and reproduce the input whenever decoded is true.
type roundTripper struct {
	name      string
	roundTrip func(b []byte) (decoded bool, encoded []byte, err error)
}

// codecFamilies covers every top-level structure this package decodes, used by both the
// fuzz target and the mutation test below.
var codecFamilies = []roundTripper{
	{"frame", func(b []byte) (bool, []byte, error) {
		v, err := DecodeExecutionInputFrame(b)
		if err != nil {
			return false, nil, nil
		}
		enc, err := v.Encode()
		return true, enc, err
	}},
	{"event", func(b []byte) (bool, []byte, error) {
		v, err := DecodeEvent(b)
		if err != nil {
			return false, nil, nil
		}
		enc, err := v.Encode()
		return true, enc, err
	}},
	{"outbox", func(b []byte) (bool, []byte, error) {
		v, err := DecodeOutboxMessage(b)
		if err != nil {
			return false, nil, nil
		}
		enc, err := v.Encode()
		return true, enc, err
	}},
	{"contract_effects", func(b []byte) (bool, []byte, error) {
		v, err := DecodeContractEffects(b)
		if err != nil {
			return false, nil, nil
		}
		enc, err := v.Encode()
		return true, enc, err
	}},
	{"execution_result", func(b []byte) (bool, []byte, error) {
		v, err := DecodeExecutionResult(b)
		if err != nil {
			return false, nil, nil
		}
		enc, err := v.Encode()
		return true, enc, err
	}},
	{"input", func(b []byte) (bool, []byte, error) {
		v, err := DecodeInput(b)
		if err != nil {
			return false, nil, nil
		}
		enc, err := v.Encode()
		return true, enc, err
	}},
	{"asset_flow_summary", func(b []byte) (bool, []byte, error) {
		v, err := DecodeAssetFlowSummary(b)
		if err != nil {
			return false, nil, nil
		}
		enc, err := v.Encode()
		return true, enc, err
	}},
	{"receipt", func(b []byte) (bool, []byte, error) {
		v, err := DecodeReceipt(b)
		if err != nil {
			return false, nil, nil
		}
		enc, err := v.Encode()
		return true, enc, err
	}},
	{"batch_commitment", func(b []byte) (bool, []byte, error) {
		v, err := DecodeBatchCommitment(b)
		if err != nil {
			return false, nil, nil
		}
		enc, err := v.Encode()
		return true, enc, err
	}},
	{"bundle", func(b []byte) (bool, []byte, error) {
		records, err := DecodeBundle(b)
		if err != nil {
			return false, nil, nil
		}
		enc, err := EncodeBundle(records)
		return true, enc, err
	}},
	{"receipt_sidecar", func(b []byte) (bool, []byte, error) {
		receipts, _, err := DecodeReceiptSidecar(b)
		if err != nil {
			return false, nil, nil
		}
		enc, err := EncodeReceiptSidecar(receipts)
		return true, enc, err
	}},
}

// sampleEncodings returns one representative valid encoding per family, used both as fuzz
// seeds and as the mutation test's base case. It takes testing.TB so it works from both
// *testing.T (the mutation test) and *testing.F (the fuzz target's seed corpus).
func sampleEncodings(t testing.TB) map[string][]byte {
	t.Helper()
	out := make(map[string][]byte)

	frame := sampleFrame()
	b, err := frame.Encode()
	if err != nil {
		t.Fatal(err)
	}
	out["frame"] = b

	event := sampleEvent(0, 0)
	b, err = event.Encode()
	if err != nil {
		t.Fatal(err)
	}
	out["event"] = b

	outbox := sampleL2Delivery(0, 0)
	b, err = outbox.Encode()
	if err != nil {
		t.Fatal(err)
	}
	out["outbox"] = b

	ce := sampleContractEffects(0)
	b, err = ce.Encode()
	if err != nil {
		t.Fatal(err)
	}
	out["contract_effects"] = b

	er := sampleExecutionResult(0)
	b, err = er.Encode()
	if err != nil {
		t.Fatal(err)
	}
	out["execution_result"] = b

	in := sampleInputs()[0]
	b, err = in.Encode()
	if err != nil {
		t.Fatal(err)
	}
	out["input"] = b

	afs := sampleAssetFlowSummary()
	b, err = afs.Encode()
	if err != nil {
		t.Fatal(err)
	}
	out["asset_flow_summary"] = b

	rec := sampleReceipt()
	b, err = rec.Encode()
	if err != nil {
		t.Fatal(err)
	}
	out["receipt"] = b

	commit := sampleCommitment()
	b, err = commit.Encode()
	if err != nil {
		t.Fatal(err)
	}
	out["batch_commitment"] = b

	bundleRecords := sampleBundleRecords(t)
	b, err = EncodeBundle(bundleRecords)
	if err != nil {
		t.Fatal(err)
	}
	out["bundle"] = b

	b, err = EncodeReceiptSidecar(sampleSidecarReceipts())
	if err != nil {
		t.Fatal(err)
	}
	out["receipt_sidecar"] = b

	return out
}

// FuzzCanonicalDecode proves the property that makes the format canonical: any byte string
// the decoder accepts re-encodes to exactly itself — exactly one encoding, no silent repair.
func FuzzCanonicalDecode(f *testing.F) {
	familyIndex := make(map[string]uint8, len(codecFamilies))
	for i, fam := range codecFamilies {
		familyIndex[fam.name] = uint8(i)
	}

	samples := sampleEncodings(f)
	for name, enc := range samples {
		f.Add(familyIndex[name], enc)
	}

	// Additional seeds beyond one-per-family: every unit-test encoding
	// and vector encoding, paired with the selector for its own
	// family.
	addSeed := func(name string, enc []byte) { f.Add(familyIndex[name], enc) }

	if b, err := sampleL1Withdrawal(0, 0).Encode(); err == nil {
		addSeed("outbox", b)
	}
	for _, in := range sampleInputs() {
		if b, err := in.Encode(); err == nil {
			addSeed("input", b)
		}
	}
	if b, err := sampleExecutionResult(5).Encode(); err == nil {
		addSeed("execution_result", b)
	}
	if b, err := sampleEvent(3, 1).Encode(); err == nil {
		addSeed("event", b)
	}

	vf, err := os.ReadFile(filepath.Join("testdata", "serialization-conformance-v1.json"))
	if err == nil {
		var parsed vectorFile
		if json.Unmarshal(vf, &parsed) == nil {
			for _, vec := range parsed.Vectors {
				if vec.EncodedHex == "" {
					continue
				}
				enc, err := hex.DecodeString(vec.EncodedHex)
				if err != nil {
					continue
				}
				switch vec.Structure {
				case "BatchCommitment":
					addSeed("batch_commitment", enc)
				case "OutboxMessage":
					addSeed("outbox", enc)
				}
			}
		}
	}

	f.Fuzz(func(t *testing.T, sel uint8, data []byte) {
		fam := codecFamilies[int(sel)%len(codecFamilies)]
		decoded, encoded, err := fam.roundTrip(data)
		if !decoded {
			// Decode rejection is a valid outcome and is never a failure.
			return
		}
		if err != nil {
			t.Fatalf("%s: decode accepted %x but re-encode failed: %v", fam.name, data, err)
		}
		if !bytes.Equal(encoded, data) {
			t.Fatalf("%s: decode accepted %x but re-encoded to %x — not canonical", fam.name, data, encoded)
		}
	})
}

// TestSingleByteMutationIsNeverSilentlyRepaired flips every byte of a valid encoding of each
// structure family and requires the decoder either to reject it or to round-trip it back to
// the mutated bytes — never to the original.
func TestSingleByteMutationIsNeverSilentlyRepaired(t *testing.T) {
	samples := sampleEncodings(t)

	for _, fam := range codecFamilies {
		fam := fam
		t.Run(fam.name, func(t *testing.T) {
			base, ok := samples[fam.name]
			if !ok {
				t.Fatalf("no sample encoding for family %q", fam.name)
			}
			for i := range base {
				mutated := append([]byte{}, base...)
				mutated[i] ^= 0xFF

				decoded, encoded, err := fam.roundTrip(mutated)
				if !decoded {
					continue // decode rejection is fine
				}
				if err != nil {
					t.Fatalf("byte %d: decode accepted mutated input but re-encode failed: %v", i, err)
				}
				if !bytes.Equal(encoded, mutated) {
					t.Fatalf("byte %d: decode accepted mutated input but re-encoded to a different value (silent repair):\n mutated  %x\n got      %x", i, mutated, encoded)
				}
				if bytes.Equal(encoded, base) {
					t.Fatalf("byte %d: decode of mutated input re-encoded back to the ORIGINAL bytes (silent repair)", i)
				}
			}
		})
	}
}
