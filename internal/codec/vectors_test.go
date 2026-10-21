package codec

// Conformance test against
// internal/codec/testdata/serialization-conformance-v1.json, the
// serialization, commitment and bundle vector suite this package
// authors. This file is the normative companion artifact's consumer;
// CV-COMMIT-1's encoded_hex is the byte-level contract Settlement must
// reproduce.

import (
	"bytes"
	"encoding/binary"
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
	Case         string `json:"case"`
	Root         string `json:"root"`
	ElementHex   string `json:"element_hex"`
	ProofHex     string `json:"proof_hex"`
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
	case "overflow":
		return ErrOverflow
	case "not_sorted":
		return ErrNotSorted
	case "duplicate_key":
		return ErrDuplicateKey
	case "index_mismatch":
		return ErrIndexMismatch
	case "limit_exceeded":
		return ErrLimitExceeded
	case "invalid_proof_side":
		return ErrInvalidProofSide
	case "proof_mismatch":
		return ErrProofMismatch
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
			case "CV-RESULT-1":
				testResultAccept(t, vec)
			case "CV-RESULT-2":
				testResultReject(t, vec, vectorByID(t, vf, "CV-RESULT-1"))
			case "CV-RESULT-OUTBOX-1":
				testResultOutboxAccept(t, vec)
			case "CV-RESULT-OUTBOX-2":
				testResultOutboxReject(t, vec, vectorByID(t, vf, "CV-RESULT-OUTBOX-1"))
			case "CV-MERKLE-1":
				testMerkleReject(t, vec, vectorByID(t, vf, "CV-INPUTROOT-1"))
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

// vectorByID returns the vector with the given id, failing when there is none.
func vectorByID(t *testing.T, vf vectorFile, id string) vectorRaw {
	t.Helper()
	for _, v := range vf.Vectors {
		if v.ID == id {
			return v
		}
	}
	t.Fatalf("no vector with id %q", id)
	return vectorRaw{}
}

// encodeResultUnchecked writes e's nine fields in ExecutionResult order
// without the list-order, index and count checks of
// encodeExecutionResultBody, so a test can build a non-canonical encoding.
func encodeResultUnchecked(t *testing.T, e ExecutionResult) []byte {
	t.Helper()
	w := &writer{}
	w.u16(e.Version)
	w.u64(e.GlobalInputIndex)
	w.hash(e.PreStateRoot)
	w.hash(e.PostStateRoot)
	w.u32(uint32(len(e.StateDiff)))
	for _, s := range e.StateDiff {
		if err := encodeStateDiffEntry(w, s); err != nil {
			t.Fatalf("encode state diff entry: %v", err)
		}
	}
	w.u32(uint32(len(e.Events)))
	for _, ev := range e.Events {
		if err := encodeEventBody(w, ev); err != nil {
			t.Fatalf("encode event: %v", err)
		}
	}
	w.u32(uint32(len(e.Outbox)))
	for _, m := range e.Outbox {
		if err := encodeOutboxMessageBody(w, m); err != nil {
			t.Fatalf("encode outbox message: %v", err)
		}
	}
	w.bytes(e.ReturnData)
	w.u256(e.ClaimedDeposit)
	return w.buf
}

// testResultAccept checks CV-RESULT-1: decode, re-encode, and the
// unchecked writer, all reproducing encoded_hex.
func testResultAccept(t *testing.T, vec vectorRaw) {
	t.Helper()
	if vec.Expect != "accept" {
		t.Fatalf("vector %s: expect field is %q, want accept", vec.ID, vec.Expect)
	}
	encoded := mustHexBytes(t, vec.EncodedHex)
	r, err := DecodeExecutionResult(encoded)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	got, err := r.Encode()
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	if !bytes.Equal(got, encoded) {
		t.Fatalf("Encode did not reproduce encoded_hex:\n got  %x\n want %x", got, encoded)
	}
	if got := encodeResultUnchecked(t, r); !bytes.Equal(got, encoded) {
		t.Fatalf("encodeResultUnchecked did not reproduce encoded_hex:\n got  %x\n want %x", got, encoded)
	}
}

// resultRejectCases is the case order of CV-RESULT-2.
var resultRejectCases = []string{
	"trailing_byte",
	"roots_after_claimed_deposit",
	"state_diff_descending",
	"state_diff_duplicate_key",
	"event_input_index",
	"outbox_input_index",
}

// testResultReject checks CV-RESULT-2: each rejected encoding is
// CV-RESULT-1 with exactly its named defect, and fails to decode with
// its reject_reason.
func testResultReject(t *testing.T, vec, base vectorRaw) {
	t.Helper()
	if vec.Expect != "reject" {
		t.Fatalf("vector %s: expect field is %q, want reject", vec.ID, vec.Expect)
	}
	baseBytes := mustHexBytes(t, base.EncodedHex)
	b, err := DecodeExecutionResult(baseBytes)
	if err != nil {
		t.Fatalf("decode %s: %v", base.ID, err)
	}
	if len(b.StateDiff) != 2 || len(b.Events) < 1 || len(b.Outbox) < 1 {
		t.Fatalf("%s: want 2 state diff entries and at least one event and outbox message, got %d, %d, %d",
			base.ID, len(b.StateDiff), len(b.Events), len(b.Outbox))
	}
	if len(vec.Rejected) != len(resultRejectCases) {
		t.Fatalf("%s: %d rejected entries, want %d", vec.ID, len(vec.Rejected), len(resultRejectCases))
	}
	for i, rej := range vec.Rejected {
		if rej.Case != resultRejectCases[i] {
			t.Fatalf("rejected[%d]: case %q, want %q", i, rej.Case, resultRejectCases[i])
		}
		var want []byte
		mutated := b
		switch rej.Case {
		case "trailing_byte":
			want = append(append([]byte{}, baseBytes...), 0x00)
		case "roots_after_claimed_deposit":
			want = append(append(append([]byte{}, baseBytes[0:10]...), baseBytes[74:]...), baseBytes[10:74]...)
			if bytes.Equal(want, baseBytes) {
				t.Fatalf("%s: moved roots equal the base", rej.Case)
			}
		case "state_diff_descending":
			mutated.StateDiff = append([]StateDiffEntry{}, b.StateDiff...)
			mutated.StateDiff[0], mutated.StateDiff[1] = b.StateDiff[1], b.StateDiff[0]
			want = encodeResultUnchecked(t, mutated)
		case "state_diff_duplicate_key":
			mutated.StateDiff = append([]StateDiffEntry{}, b.StateDiff...)
			mutated.StateDiff[1].Key = b.StateDiff[0].Key
			want = encodeResultUnchecked(t, mutated)
		case "event_input_index":
			mutated.Events = append([]Event{}, b.Events...)
			mutated.Events[0].InputIndex = 8
			want = encodeResultUnchecked(t, mutated)
		case "outbox_input_index":
			mutated.Outbox = append([]OutboxMessage{}, b.Outbox...)
			mutated.Outbox[0].InputIndex = 8
			want = encodeResultUnchecked(t, mutated)
		}
		if got := mustHexBytes(t, rej.EncodedHex); !bytes.Equal(got, want) {
			t.Fatalf("%s: encoded_hex is not %s with only its defect:\n got  %x\n want %x", rej.Case, base.ID, got, want)
		}
		sentinel := rejectSentinel(rej.RejectReason)
		if sentinel == nil {
			t.Fatalf("%s: unmapped reject_reason %q", rej.Case, rej.RejectReason)
		}
		if _, err := DecodeExecutionResult(want); !errors.Is(err, sentinel) {
			t.Fatalf("%s: got %v, want %v", rej.Case, err, sentinel)
		}
	}
}

// testResultOutboxAccept checks CV-RESULT-OUTBOX-1: 33 outbox messages,
// the last a deposit refund, decode and re-encode.
func testResultOutboxAccept(t *testing.T, vec vectorRaw) {
	t.Helper()
	if vec.Expect != "accept" {
		t.Fatalf("vector %s: expect field is %q, want accept", vec.ID, vec.Expect)
	}
	encoded := mustHexBytes(t, vec.EncodedHex)
	r, err := DecodeExecutionResult(encoded)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	got, err := r.Encode()
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	if !bytes.Equal(got, encoded) {
		t.Fatalf("Encode did not reproduce encoded_hex:\n got  %x\n want %x", got, encoded)
	}
	if len(r.Outbox) != MaxOutboxPerExecution+1 {
		t.Fatalf("outbox has %d messages, want %d", len(r.Outbox), MaxOutboxPerExecution+1)
	}
	for i, m := range r.Outbox[:MaxOutboxPerExecution] {
		if m.SourceContract == (types.Hash{}) {
			t.Fatalf("outbox[%d]: zero SourceContract", i)
		}
	}
	refund := r.Outbox[MaxOutboxPerExecution]
	if refund.SourceContract != (types.Hash{}) {
		t.Fatalf("outbox[%d]: SourceContract %x, want zero", MaxOutboxPerExecution, refund.SourceContract)
	}
	if _, ok := refund.Target.(L1Withdrawal); !ok {
		t.Fatalf("outbox[%d]: target %T, want L1Withdrawal", MaxOutboxPerExecution, refund.Target)
	}
	if len(refund.Payload) != 0 {
		t.Fatalf("outbox[%d]: payload has %d bytes, want 0", MaxOutboxPerExecution, len(refund.Payload))
	}
}

// testResultOutboxReject checks CV-RESULT-OUTBOX-2: CV-RESULT-OUTBOX-1
// with one more contract message fails with ErrLimitExceeded.
func testResultOutboxReject(t *testing.T, vec, base vectorRaw) {
	t.Helper()
	if vec.Expect != "reject" {
		t.Fatalf("vector %s: expect field is %q, want reject", vec.ID, vec.Expect)
	}
	sentinel := rejectSentinel(vec.RejectReason)
	if sentinel != ErrLimitExceeded {
		t.Fatalf("vector %s: reject_reason %q maps to %v, want ErrLimitExceeded", vec.ID, vec.RejectReason, sentinel)
	}
	b, err := DecodeExecutionResult(mustHexBytes(t, base.EncodedHex))
	if err != nil {
		t.Fatalf("decode %s: %v", base.ID, err)
	}
	if len(b.Outbox) != MaxOutboxPerExecution+1 {
		t.Fatalf("%s: outbox has %d messages, want %d", base.ID, len(b.Outbox), MaxOutboxPerExecution+1)
	}
	outbox := append([]OutboxMessage{}, b.Outbox[:MaxOutboxPerExecution]...)
	extra := b.Outbox[MaxOutboxPerExecution-1]
	extra.OutboxIndex = MaxOutboxPerExecution
	refund := b.Outbox[MaxOutboxPerExecution]
	refund.OutboxIndex = MaxOutboxPerExecution + 1
	b.Outbox = append(outbox, extra, refund)

	want := encodeResultUnchecked(t, b)
	if got := mustHexBytes(t, vec.EncodedHex); !bytes.Equal(got, want) {
		t.Fatalf("encoded_hex is not %s with one more contract message:\n got  %x\n want %x", base.ID, got, want)
	}
	if _, err := DecodeExecutionResult(want); !errors.Is(err, sentinel) {
		t.Fatalf("got %v, want %v", err, sentinel)
	}
}

// refFold folds proof's steps over node with refNodeHash, with no
// depth bound, and fails unless the proof is a whole number of steps
// matching its step_count, each with side 0 or 1.
func refFold(t *testing.T, node types.Hash, proof []byte) types.Hash {
	t.Helper()
	if len(proof) < 4 {
		t.Fatalf("proof has %d bytes, want at least 4", len(proof))
	}
	count := int(binary.BigEndian.Uint32(proof))
	if len(proof) != 4+33*count {
		t.Fatalf("proof has %d bytes for step_count %d, want %d", len(proof), count, 4+33*count)
	}
	for off := 4; off < len(proof); off += 33 {
		sibling := hashOf(proof[off+1 : off+33])
		switch proof[off] {
		case 0:
			node = refNodeHash(sibling, node)
		case 1:
			node = refNodeHash(node, sibling)
		default:
			t.Fatalf("proof step at byte %d: side %d, want 0 or 1", off, proof[off])
		}
	}
	return node
}

// merkleRejectCases is the case order of CV-MERKLE-1.
var merkleRejectCases = []string{
	"side_2",
	"side_2_from_left",
	"step_count_above_depth",
	"trailing_byte",
	"leaf_hash_as_element",
	"empty_set_root",
}

// testMerkleReject checks CV-MERKLE-1: each rejected proof is built
// from CV-INPUTROOT-1 with exactly its named defect, and
// VerifyMerkleProof fails with its reject_reason.
func testMerkleReject(t *testing.T, vec, base vectorRaw) {
	t.Helper()
	if vec.Expect != "reject" {
		t.Fatalf("vector %s: expect field is %q, want reject", vec.ID, vec.Expect)
	}
	if len(base.Inputs) < 2 || len(base.ProofsHex) < 2 {
		t.Fatalf("%s: want at least 2 inputs and proofs", base.ID)
	}
	rootHex := base.ExpectedInputRoot
	e := mustHexBytes(t, base.Inputs[0].EncodedHex)
	p := mustHexBytes(t, base.ProofsHex[0])
	e1 := mustHexBytes(t, base.Inputs[1].EncodedHex)
	p1 := mustHexBytes(t, base.ProofsHex[1])
	root := hashOf(mustHexBytes(t, rootHex))

	withSide2 := func(proof []byte) []byte {
		out := append([]byte{}, proof...)
		out[4] = 0x02
		return out
	}
	d := binary.BigEndian.AppendUint32(nil, MaxMerkleProofDepth+1)
	sibling := fillHash(0x33)
	for i := 0; i < MaxMerkleProofDepth+1; i++ {
		d = append(append(d, 0x01), sibling[:]...)
	}
	rd := refFold(t, refLeafHash(e), d)
	leaf := refLeafHash(e)

	if len(vec.Rejected) != len(merkleRejectCases) {
		t.Fatalf("%s: %d rejected entries, want %d", vec.ID, len(vec.Rejected), len(merkleRejectCases))
	}
	for i, rej := range vec.Rejected {
		if rej.Case != merkleRejectCases[i] {
			t.Fatalf("rejected[%d]: case %q, want %q", i, rej.Case, merkleRejectCases[i])
		}
		var wantRoot string
		var wantElement, wantProof []byte
		wantRoot = rootHex
		switch rej.Case {
		case "side_2":
			if p[4] != 0x01 {
				t.Fatalf("%s: step 0 side is %d, want 1 (a right sibling)", rej.Case, p[4])
			}
			wantElement, wantProof = e, withSide2(p)
		case "side_2_from_left":
			if p1[4] != 0x00 {
				t.Fatalf("%s: step 0 side is %d, want 0 (a left sibling)", rej.Case, p1[4])
			}
			wantElement, wantProof = e1, withSide2(p1)
		case "step_count_above_depth":
			wantRoot = hex.EncodeToString(rd[:])
			wantElement, wantProof = e, d
		case "trailing_byte":
			wantElement, wantProof = e, append(append([]byte{}, p...), 0x00)
		case "leaf_hash_as_element":
			if refFold(t, leaf, p) != root {
				t.Fatalf("%s: the leaf hash does not fold to the root over the first proof", rej.Case)
			}
			wantElement, wantProof = leaf[:], p
		case "empty_set_root":
			wantRoot = hex.EncodeToString(make([]byte, 32))
			wantElement, wantProof = e, p
		}
		if rej.Root != wantRoot {
			t.Fatalf("%s: root %s, want %s", rej.Case, rej.Root, wantRoot)
		}
		if rej.ElementHex != hex.EncodeToString(wantElement) {
			t.Fatalf("%s: element_hex %s, want %x", rej.Case, rej.ElementHex, wantElement)
		}
		if rej.ProofHex != hex.EncodeToString(wantProof) {
			t.Fatalf("%s: proof_hex %s, want %x", rej.Case, rej.ProofHex, wantProof)
		}
		sentinel := rejectSentinel(rej.RejectReason)
		if sentinel == nil {
			t.Fatalf("%s: unmapped reject_reason %q", rej.Case, rej.RejectReason)
		}
		got := VerifyMerkleProof(hashOf(mustHexBytes(t, rej.Root)), mustHexBytes(t, rej.ElementHex), mustHexBytes(t, rej.ProofHex))
		if !errors.Is(got, sentinel) {
			t.Fatalf("%s: got %v, want %v", rej.Case, got, sentinel)
		}
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
