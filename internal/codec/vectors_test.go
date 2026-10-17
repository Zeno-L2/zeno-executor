package codec

// Conformance test against
// internal/codec/testdata/serialization-conformance-v1.json, the
// serialization, commitment and bundle vector suite this package
// authors. This file is the normative companion artifact's consumer;
// CV-COMMIT-1's encoded_hex is the byte-level contract Settlement must
// reproduce.

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/zenon-network/go-zenon/common/crypto"
	"github.com/zenon-network/go-zenon/common/trie"
	"github.com/zenon-network/go-zenon/common/types"
)

type vectorFile struct {
	Vectors []vectorRaw `json:"vectors"`
}

type vectorRaw struct {
	ID                  string            `json:"id"`
	Structure           string            `json:"structure"`
	Expect              string            `json:"expect"`
	EncodedHex          string            `json:"encoded_hex"`
	TrailingBytesHex    string            `json:"trailing_bytes_hex"`
	RejectReason        string            `json:"reject_reason"`
	Inputs              []inputVectorRaw  `json:"inputs"`
	ExpectedInputRoot   string            `json:"expected_input_root"`
	ProofsHex           []string          `json:"proofs_hex"`
	PrefixRoot          string            `json:"prefix_root"`
	PrefixProofsHex     []string          `json:"prefix_proofs_hex"`
	ZtsBech32           string            `json:"zts_bech32"`
	ZtsHex              string            `json:"zts_hex"`
	AssetIDHex          string            `json:"asset_id_hex"`
	RejectedAssetIDsHex []string          `json:"rejected_asset_ids_hex"`
	PreStateLeaves      map[string]string `json:"pre_state_leaves"`
	PreStateRoot        string            `json:"pre_state_root"`
	Records             []bundleRecordRaw `json:"records"`
	BundleHex           string            `json:"bundle_hex"`
	ExpectedDAHash      string            `json:"expected_da_hash"`
	Rejected            []metaRejectRaw   `json:"rejected"`
	Fields              *metaFieldsRaw    `json:"fields"`
}

type metaRejectRaw struct {
	EncodedHex   string `json:"encoded_hex"`
	RejectReason string `json:"reject_reason"`
}

type metaFieldsRaw struct {
	MetadataVersion uint16 `json:"metadata_version"`
	UpgradePolicy   uint8  `json:"upgrade_policy"`
	OwnerHex        string `json:"owner_hex"`
}

type inputVectorRaw struct {
	Kind         string `json:"kind"`
	EncodedHex   string `json:"encoded_hex"`
	InputHashHex string `json:"input_hash_hex"`
}

type bundleRecordRaw struct {
	GlobalInputIndex   uint64   `json:"global_input_index"`
	InputHex           string   `json:"input_hex"`
	ExecutionResultHex string   `json:"execution_result_hex"`
	ObservedKeys       []string `json:"observed_keys"`
	DeployChunkDataHex string   `json:"deploy_chunk_data_hex"`
}

func loadConformanceVectors(t *testing.T) vectorFile {
	t.Helper()
	data, err := os.ReadFile(filepath.Join("testdata", "serialization-conformance-v1.json"))
	if err != nil {
		t.Fatalf("read vectors: %v", err)
	}
	var vf vectorFile
	if err := json.Unmarshal(data, &vf); err != nil {
		t.Fatalf("parse vectors: %v", err)
	}
	return vf
}

func mustHexBytes(t *testing.T, s string) []byte {
	t.Helper()
	b, err := hex.DecodeString(s)
	if err != nil {
		t.Fatalf("bad hex %q: %v", s, err)
	}
	return b
}

func rejectSentinel(reason string) error {
	switch reason {
	case "non_empty_proof_data":
		return ErrNonEmptyProofData
	case "trailing_bytes":
		return ErrTrailingBytes
	case "non_empty_withdrawal_payload":
		return ErrNonEmptyWithdrawalPayload
	case "invalid_asset_id":
		return ErrInvalidAssetID
	case "unknown_version":
		return ErrUnknownVersion
	case "truncated":
		return ErrTruncated
	case "invalid_upgrade_policy":
		return ErrInvalidPolicy
	case "invalid_owner":
		return ErrInvalidOwner
	default:
		return nil
	}
}

func TestConformanceVectors(t *testing.T) {
	vf := loadConformanceVectors(t)

	for _, vec := range vf.Vectors {
		vec := vec
		t.Run(vec.ID, func(t *testing.T) {
			switch vec.ID {
			case "CV-COMMIT-1":
				testCommitAccept(t, vec)
			case "CV-COMMIT-2":
				testGenericReject(t, vec, func(b []byte) error { _, err := DecodeBatchCommitment(b); return err })
			case "CV-INPUTROOT-1":
				testInputRoot(t, vec)
			case "CV-ASSET-1":
				testAssetAccept(t, vec)
			case "CV-ASSET-2":
				testAssetReject(t, vec)
			case "CV-OUTBOX-1":
				testGenericReject(t, vec, func(b []byte) error { _, err := DecodeOutboxMessage(b); return err })
			case "CV-BUNDLE-1":
				testBundle(t, vec)
			case "CV-BUNDLE-2":
				testBundle2(t, vec)
			case "CV-META-1":
				testMeta1(t, vec)
			case "CV-META-2":
				testMeta2(t, vec)
			default:
				t.Fatalf("unhandled vector id %q", vec.ID)
			}
		})
	}
}

func testCommitAccept(t *testing.T, vec vectorRaw) {
	t.Helper()
	encoded := mustHexBytes(t, vec.EncodedHex)
	c, err := DecodeBatchCommitment(encoded)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	got, err := c.Encode()
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	if !bytes.Equal(got, encoded) {
		t.Fatalf("Encode did not reproduce encoded_hex:\n got  %x\n want %x", got, encoded)
	}

	if vec.TrailingBytesHex != "" {
		trailing := mustHexBytes(t, vec.TrailingBytesHex)
		if _, err := DecodeBatchCommitment(trailing); !errors.Is(err, ErrTrailingBytes) {
			t.Fatalf("trailing_bytes_hex: got %v, want ErrTrailingBytes", err)
		}
	}
}

func testGenericReject(t *testing.T, vec vectorRaw, decode func([]byte) error) {
	t.Helper()
	if vec.Expect != "reject" {
		t.Fatalf("vector %s: expect field is %q, want reject", vec.ID, vec.Expect)
	}
	want := rejectSentinel(vec.RejectReason)
	if want == nil {
		t.Fatalf("vector %s: unmapped reject_reason %q", vec.ID, vec.RejectReason)
	}
	encoded := mustHexBytes(t, vec.EncodedHex)
	if err := decode(encoded); !errors.Is(err, want) {
		t.Fatalf("got %v, want %v", err, want)
	}
}

func testInputRoot(t *testing.T, vec vectorRaw) {
	t.Helper()
	elements := make([][]byte, len(vec.Inputs))
	for i, iv := range vec.Inputs {
		encoded := mustHexBytes(t, iv.EncodedHex)
		in, err := DecodeInput(encoded)
		if err != nil {
			t.Fatalf("input %d (%s): Decode: %v", i, iv.Kind, err)
		}
		gotEncoded, err := in.Encode()
		if err != nil {
			t.Fatalf("input %d (%s): Encode: %v", i, iv.Kind, err)
		}
		if !bytes.Equal(gotEncoded, encoded) {
			t.Fatalf("input %d (%s): Encode did not reproduce encoded_hex", i, iv.Kind)
		}
		gotHash := hashOf(crypto.Hash(encoded))
		if hex.EncodeToString(gotHash[:]) != iv.InputHashHex {
			t.Fatalf("input %d (%s): input_hash_hex mismatch: got %x, want %s", i, iv.Kind, gotHash, iv.InputHashHex)
		}
		elements[i] = encoded
	}

	got := MerkleRoot(elements)
	want := refMerkleRoot(elements)
	if got != want {
		t.Fatalf("MerkleRoot mismatch: got %x, want %x (independent Merkle recomputation)", got, want)
	}
	if hex.EncodeToString(got[:]) != vec.ExpectedInputRoot {
		t.Fatalf("expected_input_root mismatch: got %x, want %s", got, vec.ExpectedInputRoot)
	}

	checkFrozenProofs(t, "proofs_hex", elements, vec.ExpectedInputRoot, vec.ProofsHex)

	prefix := elements[:len(vec.PrefixProofsHex)]
	if gotPrefix := MerkleRoot(prefix); hex.EncodeToString(gotPrefix[:]) != vec.PrefixRoot {
		t.Fatalf("prefix_root mismatch: got %x, want %s", gotPrefix, vec.PrefixRoot)
	}
	checkFrozenProofs(t, "prefix_proofs_hex", prefix, vec.PrefixRoot, vec.PrefixProofsHex)
}

// checkFrozenProofs checks that MerkleProofs reproduces the frozen
// proof of every element, that each frozen proof verifies against
// rootHex, and that changing its last (sibling) byte yields
// ErrProofMismatch.
func checkFrozenProofs(t *testing.T, name string, elements [][]byte, rootHex string, frozen []string) {
	t.Helper()
	if len(frozen) == 0 || len(frozen) != len(elements) {
		t.Fatalf("%s: %d proofs for %d elements", name, len(frozen), len(elements))
	}
	rootBytes, err := hex.DecodeString(rootHex)
	if err != nil || len(rootBytes) != len(types.Hash{}) {
		t.Fatalf("%s: bad root %q", name, rootHex)
	}
	root := hashOf(rootBytes)

	built := MerkleProofs(elements)
	if len(built) != len(elements) {
		t.Fatalf("%s: MerkleProofs returned %d proofs for %d elements", name, len(built), len(elements))
	}
	for i, element := range elements {
		if got := hex.EncodeToString(built[i]); got != frozen[i] {
			t.Fatalf("%s[%d]: MerkleProofs mismatch: got %s, want %s", name, i, got, frozen[i])
		}
		proof, err := hex.DecodeString(frozen[i])
		if err != nil {
			t.Fatalf("%s[%d]: %v", name, i, err)
		}
		if err := VerifyMerkleProof(root, element, proof); err != nil {
			t.Fatalf("%s[%d]: VerifyMerkleProof: %v", name, i, err)
		}
		proof[len(proof)-1] ^= 0x01
		if err := VerifyMerkleProof(root, element, proof); !errors.Is(err, ErrProofMismatch) {
			t.Fatalf("%s[%d]: tampered proof: got %v, want ErrProofMismatch", name, i, err)
		}
	}
}

func testAssetAccept(t *testing.T, vec vectorRaw) {
	t.Helper()
	zts, err := types.ParseZTS(vec.ZtsBech32)
	if err != nil {
		t.Fatalf("ParseZTS(%q): %v", vec.ZtsBech32, err)
	}
	if hex.EncodeToString(zts[:]) != vec.ZtsHex {
		t.Fatalf("zts_hex mismatch: got %x, want %s", zts, vec.ZtsHex)
	}

	assetID := AssetIDFromZTS(zts)
	if hex.EncodeToString(assetID[:]) != vec.AssetIDHex {
		t.Fatalf("asset_id_hex mismatch: got %x, want %s", assetID, vec.AssetIDHex)
	}

	gotZTS, err := assetID.ZTS()
	if err != nil {
		t.Fatalf("ZTS(): %v", err)
	}
	if gotZTS != zts {
		t.Fatalf("round trip mismatch: got %x, want %x", gotZTS, zts)
	}
}

func testAssetReject(t *testing.T, vec vectorRaw) {
	t.Helper()
	for _, hexStr := range vec.RejectedAssetIDsHex {
		var a AssetID
		copy(a[:], mustHexBytes(t, hexStr))
		if err := a.Validate(); !errors.Is(err, ErrInvalidAssetID) {
			t.Fatalf("Validate(%s): got %v, want ErrInvalidAssetID", hexStr, err)
		}
	}
}

func testBundle(t *testing.T, vec vectorRaw) {
	t.Helper()

	// Step 1: rebuild the leaf slices from pre_state_leaves and assert RootOfLeaves matches
	// pre_state_root and record 1's decoded ExecutionResult.PreStateRoot.
	basePaths, baseValues := leavesFromMap(t, vec.PreStateLeaves)
	baseRoot, err := trie.RootOfLeaves(basePaths, baseValues)
	if err != nil {
		t.Fatalf("RootOfLeaves(pre_state_leaves): %v", err)
	}
	wantPreRoot := mustHexBytes(t, vec.PreStateRoot)
	if !bytes.Equal(baseRoot[:], wantPreRoot) {
		t.Fatalf("pre_state_root mismatch: got %x, want %x", baseRoot, wantPreRoot)
	}

	if len(vec.Records) != 2 {
		t.Fatalf("expected 2 records, got %d", len(vec.Records))
	}

	rec1ER, err := DecodeExecutionResult(mustHexBytes(t, vec.Records[0].ExecutionResultHex))
	if err != nil {
		t.Fatalf("record 1: DecodeExecutionResult: %v", err)
	}
	if rec1ER.PreStateRoot != baseRoot {
		t.Fatalf("record 1 PreStateRoot mismatch: got %x, want %x", rec1ER.PreStateRoot, baseRoot)
	}

	bundleBytes := mustHexBytes(t, vec.BundleHex)
	bundleRecords, err := DecodeBundle(bundleBytes)
	if err != nil {
		t.Fatalf("DecodeBundle: %v", err)
	}
	if len(bundleRecords) != 2 {
		t.Fatalf("expected 2 bundle records, got %d", len(bundleRecords))
	}

	// Step 2: for every record-1 observed key, prove against the base leaf set and compare
	// to the corresponding WitnessEntry.Proof decoded from bundle_hex.
	checkWitnesses(t, "record 1", vec.Records[0].ObservedKeys, bundleRecords[0].Witnesses,
		basePaths, baseValues, rec1ER.PreStateRoot, vec.PreStateLeaves)

	// Step 3: replay record 1's decoded StateDiff onto the base leaf set to produce record
	// 2's leaf set, and assert the root equals both record 1's PostStateRoot and record 2's
	// PreStateRoot.
	rec2Paths, rec2Values := replayStateDiff(basePaths, baseValues, rec1ER.StateDiff)
	rec2Root, err := trie.RootOfLeaves(rec2Paths, rec2Values)
	if err != nil {
		t.Fatalf("RootOfLeaves(record 2 leaves): %v", err)
	}
	if rec2Root != rec1ER.PostStateRoot {
		t.Fatalf("record 1 PostStateRoot mismatch: got %x, want %x", rec1ER.PostStateRoot, rec2Root)
	}

	rec2ER, err := DecodeExecutionResult(mustHexBytes(t, vec.Records[1].ExecutionResultHex))
	if err != nil {
		t.Fatalf("record 2: DecodeExecutionResult: %v", err)
	}
	if rec2ER.PreStateRoot != rec2Root {
		t.Fatalf("record 2 PreStateRoot mismatch: got %x, want %x", rec2ER.PreStateRoot, rec2Root)
	}

	// Step 4: repeat step 2 for record 2 against record 2's derived leaf set.
	rec2Leaves := leavesMapFromSlices(rec2Paths, rec2Values)
	checkWitnesses(t, "record 2", vec.Records[1].ObservedKeys, bundleRecords[1].Witnesses,
		rec2Paths, rec2Values, rec2ER.PreStateRoot, rec2Leaves)

	// Step 5: replay record 2's StateDiff onto record 2's leaf set and assert the root
	// equals record 2's PostStateRoot.
	rec3Paths, rec3Values := replayStateDiff(rec2Paths, rec2Values, rec2ER.StateDiff)
	rec3Root, err := trie.RootOfLeaves(rec3Paths, rec3Values)
	if err != nil {
		t.Fatalf("RootOfLeaves(post record 2 leaves): %v", err)
	}
	if rec3Root != rec2ER.PostStateRoot {
		t.Fatalf("record 2 PostStateRoot mismatch: got %x, want %x", rec2ER.PostStateRoot, rec3Root)
	}

	// Step 6: DecodeBundle round-trips byte-for-byte, and
	// crypto.Hash(bundle_hex), recomputed inline, equals
	// expected_da_hash.
	rebuilt, err := EncodeBundle(bundleRecords)
	if err != nil {
		t.Fatalf("EncodeBundle: %v", err)
	}
	if !bytes.Equal(rebuilt, bundleBytes) {
		t.Fatalf("bundle round trip mismatch:\n got  %x\n want %x", rebuilt, bundleBytes)
	}
	gotDAHash := hashOf(crypto.Hash(bundleBytes))
	wantDAHash := mustHexBytes(t, vec.ExpectedDAHash)
	if !bytes.Equal(gotDAHash[:], wantDAHash) {
		t.Fatalf("expected_da_hash mismatch: got %x, want %x", gotDAHash, wantDAHash)
	}
}

// testBundle2 checks CV-BUNDLE-2: a kind-3 record carrying deployChunkData followed by a
// non-kind-3 record, both with real canonical inputs and a real encoded empty SUCCESS
// ExecutionResult, no witnesses.
func testBundle2(t *testing.T, vec vectorRaw) {
	t.Helper()

	if len(vec.Records) != 2 {
		t.Fatalf("expected 2 records, got %d", len(vec.Records))
	}

	var records []BundleInputRecord
	for i, r := range vec.Records {
		rec := BundleInputRecord{
			GlobalInputIndex: r.GlobalInputIndex,
			InputData:        mustHexBytes(t, r.InputHex),
			ExecutionResult:  mustHexBytes(t, r.ExecutionResultHex),
		}
		if r.DeployChunkDataHex != "" {
			rec.DeployChunkData = mustHexBytes(t, r.DeployChunkDataHex)
		}
		records = append(records, rec)
		if i == 0 && rec.DeployChunkData == nil {
			t.Fatalf("record 0: want a kind-3 record carrying DeployChunkData")
		}
	}

	bundleBytes := mustHexBytes(t, vec.BundleHex)
	got, err := EncodeBundle(records)
	if err != nil {
		t.Fatalf("EncodeBundle: %v", err)
	}
	if !bytes.Equal(got, bundleBytes) {
		t.Fatalf("EncodeBundle mismatch:\n got  %x\n want %x", got, bundleBytes)
	}

	decoded, err := DecodeBundle(bundleBytes)
	if err != nil {
		t.Fatalf("DecodeBundle: %v", err)
	}
	if len(decoded) != 2 {
		t.Fatalf("DecodeBundle: got %d records, want 2", len(decoded))
	}
	if decoded[1].DeployChunkData != nil {
		t.Fatalf("record 1 (kind-4): DeployChunkData = %x, want nil", decoded[1].DeployChunkData)
	}

	gotDAHash := hashOf(crypto.Hash(bundleBytes))
	wantDAHash := mustHexBytes(t, vec.ExpectedDAHash)
	if !bytes.Equal(gotDAHash[:], wantDAHash) {
		t.Fatalf("expected_da_hash mismatch: got %x, want %x", gotDAHash, wantDAHash)
	}
}

// testMeta1 checks CV-META-1: decode, compare the fields, and re-encode to reproduce
// encoded_hex.
func testMeta1(t *testing.T, vec vectorRaw) {
	t.Helper()
	encoded := mustHexBytes(t, vec.EncodedHex)
	m, err := DecodeContractMetadata(encoded)
	if err != nil {
		t.Fatalf("DecodeContractMetadata: %v", err)
	}

	if vec.Fields == nil {
		t.Fatalf("vector %s: fields is absent", vec.ID)
	}
	if m.Version != vec.Fields.MetadataVersion {
		t.Fatalf("Version = %d, want fields.metadata_version %d", m.Version, vec.Fields.MetadataVersion)
	}
	if m.UpgradePolicy != vec.Fields.UpgradePolicy {
		t.Fatalf("UpgradePolicy = %d, want fields.upgrade_policy %d", m.UpgradePolicy, vec.Fields.UpgradePolicy)
	}
	if hex.EncodeToString(m.Owner[:]) != vec.Fields.OwnerHex {
		t.Fatalf("Owner = %s, want fields.owner_hex %s", hex.EncodeToString(m.Owner[:]), vec.Fields.OwnerHex)
	}

	got := m.Encode()
	if !bytes.Equal(got, encoded) {
		t.Fatalf("Encode did not reproduce encoded_hex:\n got  %x\n want %x", got, encoded)
	}
}

// testMeta2 checks CV-META-2: every rejected shape fails DecodeContractMetadata with its
// mapped sentinel.
func testMeta2(t *testing.T, vec vectorRaw) {
	t.Helper()
	for _, r := range vec.Rejected {
		want := rejectSentinel(r.RejectReason)
		if want == nil {
			t.Fatalf("unmapped reject_reason %q", r.RejectReason)
		}
		encoded := mustHexBytes(t, r.EncodedHex)
		if _, err := DecodeContractMetadata(encoded); !errors.Is(err, want) {
			t.Fatalf("%s: got %v, want %v", r.RejectReason, err, want)
		}
	}
}

// leavesFromMap parses a pre_state_leaves object (derived-path hex -> value hex, "" ==
// present-empty) into parallel path/value slices.
func leavesFromMap(t *testing.T, m map[string]string) ([]trie.Path, [][]byte) {
	t.Helper()
	paths := make([]trie.Path, 0, len(m))
	values := make([][]byte, 0, len(m))
	for pathHex, valueHex := range m {
		var p trie.Path
		copy(p[:], mustHexBytes(t, pathHex))
		paths = append(paths, p)
		if valueHex == "" {
			values = append(values, []byte{})
		} else {
			values = append(values, mustHexBytes(t, valueHex))
		}
	}
	return paths, values
}

// leavesMapFromSlices builds a hex-keyed map from parallel path/value slices, for feeding
// checkWitnesses' presence lookup.
func leavesMapFromSlices(paths []trie.Path, values [][]byte) map[string]string {
	m := make(map[string]string, len(paths))
	for i, p := range paths {
		m[hex.EncodeToString(p[:])] = hex.EncodeToString(values[i])
	}
	return m
}

// replayStateDiff applies diff onto the leaf set given by paths/values
// (nil deletes, any other value sets), returning the resulting
// parallel slices.
func replayStateDiff(paths []trie.Path, values [][]byte, diff []StateDiffEntry) ([]trie.Path, [][]byte) {
	leaves := make(map[trie.Path][]byte, len(paths)+len(diff))
	for i, p := range paths {
		leaves[p] = values[i]
	}
	for _, e := range diff {
		if e.Value == nil {
			delete(leaves, e.Key)
			continue
		}
		leaves[e.Key] = e.Value
	}
	outPaths := make([]trie.Path, 0, len(leaves))
	outValues := make([][]byte, 0, len(leaves))
	for p, v := range leaves {
		outPaths = append(outPaths, p)
		outValues = append(outValues, v)
	}
	return outPaths, outValues
}

// checkWitnesses proves every observed key in order against the given leaf set (the same
// call Store.Prove makes), asserting the proof matches the corresponding WitnessEntry decoded
// from the bundle and that presence matches membership in presentLeaves. For an absent key,
// it additionally asserts VerifyAbsenceByPath accepts the proof against preStateRoot.
func checkWitnesses(t *testing.T, label string, observedKeys []string, witnesses []WitnessEntry,
	paths []trie.Path, values [][]byte, preStateRoot types.Hash, presentLeaves map[string]string) {
	t.Helper()

	if len(observedKeys) != len(witnesses) {
		t.Fatalf("%s: observed_keys has %d entries, witnesses has %d", label, len(observedKeys), len(witnesses))
	}

	for i, keyHex := range observedKeys {
		var key trie.Path
		copy(key[:], mustHexBytes(t, keyHex))

		if witnesses[i].DerivedKey != key {
			t.Fatalf("%s: witness %d key mismatch: got %x, want %x", label, i, witnesses[i].DerivedKey, key)
		}

		present, value, proof, err := trie.ProveByPath(paths, values, key)
		if err != nil {
			t.Fatalf("%s: ProveByPath(%s): %v", label, keyHex, err)
		}
		if !bytes.Equal(proof, witnesses[i].Proof) {
			t.Fatalf("%s: witness %d proof mismatch:\n got  %x\n want %x", label, i, witnesses[i].Proof, proof)
		}

		_, wantPresent := presentLeaves[keyHex]
		if present != wantPresent {
			t.Fatalf("%s: witness %d present = %v, want %v", label, i, present, wantPresent)
		}
		if present {
			if !bytes.Equal(witnesses[i].Value, value) {
				t.Fatalf("%s: witness %d value mismatch: got %x, want %x", label, i, witnesses[i].Value, value)
			}
		} else if witnesses[i].Value != nil {
			t.Fatalf("%s: witness %d value = %x, want nil (absent)", label, i, witnesses[i].Value)
		}

		if !present {
			ok, err := trie.VerifyAbsenceByPath(preStateRoot, key, proof)
			if err != nil || !ok {
				t.Fatalf("%s: VerifyAbsenceByPath(%s) = %v, %v; want (true, nil)", label, keyHex, ok, err)
			}
		}
	}
}
