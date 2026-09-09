package state

// Conformance test against go-zenon's smt-v1-test-vectors.json (SMT-001…SMT-014): proves
// this package's Store/Accessor reproduce the pinned go-zenon dependency's roots and proof
// bytes byte-for-byte through the path-native API alone.
//
// KEY DISTINCTION: vector "key" fields are already the 32-byte derived SMT path, not a raw
// application key — they are fed to Store.Apply/Prove directly, with no hashing.

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/zenon-network/go-zenon/common/trie"
	"github.com/zenon-network/go-zenon/common/types"

	"github.com/digitalSloth/go-executor/domain"
)

type smtVectorFile struct {
	Vectors []smtVector `json:"vectors"`
}

type smtVector struct {
	ID             string            `json:"id"`
	ResultingState map[string]string `json:"resulting_state"`
	ExpectedRoot   string            `json:"expected_root"`
	ExpLeafHashes  map[string]string `json:"expected_leaf_hashes"`
	InclusionProof *smtProofVector   `json:"expected_inclusion_proof"`
	NonIncProof    *smtProofVector   `json:"expected_non_inclusion_proof"`
	AssertDistinct bool              `json:"assert_distinct"`
	RootAbsent     string            `json:"root_absent"`
	RootPresentEmp string            `json:"root_present_empty"`
	Steps          []smtStep         `json:"steps"`
}

type smtProofVector struct {
	Key        string `json:"key"`
	Present    bool   `json:"present"`
	Serialized string `json:"serialized"`
}

type smtStep struct {
	AfterKey        string `json:"after_insert_key"`
	IncrementalRoot string `json:"incremental_root"`
}

// vectorsFilePath locates the smt-v1-test-vectors.json shipped inside the pinned go-zenon
// module dependency, so this test always exercises whatever version go.mod currently points
// at rather than a copy that could drift out of sync.
func vectorsFilePath(t *testing.T) string {
	t.Helper()
	out, err := exec.Command("go", "list", "-m", "-f", "{{.Dir}}", "github.com/zenon-network/go-zenon").Output()
	if err != nil {
		t.Fatalf("locate go-zenon module dir: %v", err)
	}
	dir := strings.TrimSpace(string(out))
	return filepath.Join(dir, "common", "trie", "testdata", "smt-v1-test-vectors.json")
}

func loadVectors(t *testing.T) smtVectorFile {
	t.Helper()
	data, err := os.ReadFile(vectorsFilePath(t))
	if err != nil {
		t.Fatalf("read vectors: %v", err)
	}
	var vf smtVectorFile
	if err := json.Unmarshal(data, &vf); err != nil {
		t.Fatalf("parse vectors: %v", err)
	}
	return vf
}

func mustPath(t *testing.T, hexStr string) trie.Path {
	t.Helper()
	b, err := hex.DecodeString(hexStr)
	if err != nil {
		t.Fatalf("bad hex %q: %v", hexStr, err)
	}
	if len(b) != types.HashSize {
		t.Fatalf("expected %d-byte path, got %d bytes from %q", types.HashSize, len(b), hexStr)
	}
	var p trie.Path
	copy(p[:], b)
	return p
}

func mustHash(t *testing.T, hexStr string) types.Hash {
	t.Helper()
	return types.Hash(mustPath(t, hexStr))
}

func mustValue(t *testing.T, hexStr string) []byte {
	t.Helper()
	if hexStr == "" {
		return []byte{}
	}
	b, err := hex.DecodeString(hexStr)
	if err != nil {
		t.Fatalf("bad hex %q: %v", hexStr, err)
	}
	return b
}

func openTestStore(t *testing.T) *Store {
	t.Helper()
	s, err := Open(filepath.Join(t.TempDir(), "state.wal"))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func TestSMTConformanceVectors(t *testing.T) {
	vf := loadVectors(t)

	for _, vec := range vf.Vectors {
		vec := vec
		if vec.ExpectedRoot == "" {
			// CV-PATH-2 (a coverage declaration over the SMT-* vectors, gated by go-zenon's
			// own conformance suite) and CV-APPLY-2 (a diff_steps sequence, not a single
			// resulting_state) carry no independent leaf data in this shape; skip them here.
			continue
		}
		t.Run(vec.ID, func(t *testing.T) {
			s := openTestStore(t)

			diff := make([]domain.StateDiffEntry, 0, len(vec.ResultingState))
			for pathHex, valHex := range vec.ResultingState {
				diff = append(diff, domain.StateDiffEntry{
					Path:  mustPath(t, pathHex),
					Value: mustValue(t, valHex),
				})
			}

			gotRoot, err := s.Apply(diff)
			if err != nil {
				t.Fatalf("Apply: %v", err)
			}
			wantRoot := mustHash(t, vec.ExpectedRoot)
			if gotRoot != wantRoot {
				t.Errorf("root = %x, want %x", gotRoot, wantRoot)
			}

			for pathHex, lhHex := range vec.ExpLeafHashes {
				path := mustPath(t, pathHex)
				value := mustValue(t, vec.ResultingState[pathHex])
				wantLH := mustHash(t, lhHex)
				if gotLH := trie.LeafHash(types.Hash(path), value); gotLH != wantLH {
					t.Errorf("LeafHash(%s) = %x, want %x", pathHex, gotLH, wantLH)
				}
			}

			checkProof := func(name string, p *smtProofVector) {
				if p == nil {
					return
				}
				target := mustPath(t, p.Key)
				present, _, proof, err := s.Prove(target)
				if err != nil {
					t.Fatalf("%s: Prove: %v", name, err)
				}
				if present != p.Present {
					t.Errorf("%s: present = %v, want %v", name, present, p.Present)
				}
				want := mustValue(t, p.Serialized)
				if !bytes.Equal(proof, want) {
					t.Errorf("%s: proof bytes mismatch\n  got  %x\n  want %x", name, proof, want)
				}
			}
			checkProof("inclusion", vec.InclusionProof)
			checkProof("non-inclusion", vec.NonIncProof)

			if vec.AssertDistinct {
				rootAbsent := mustHash(t, vec.RootAbsent)
				rootPresEmp := mustHash(t, vec.RootPresentEmp)
				if rootAbsent == rootPresEmp {
					t.Errorf("present-empty root == absent root, expected distinct")
				}
				if gotRoot != rootPresEmp {
					t.Errorf("computed root = %x, want root_present_empty = %x", gotRoot, rootPresEmp)
				}
			}

			if len(vec.Steps) > 0 {
				stepStore := openTestStore(t)
				for i, step := range vec.Steps {
					key := mustPath(t, step.AfterKey)
					value := mustValue(t, vec.ResultingState[step.AfterKey])
					gotStepRoot, err := stepStore.Apply([]domain.StateDiffEntry{{Path: key, Value: value}})
					if err != nil {
						t.Fatalf("step %d: Apply: %v", i, err)
					}
					wantStepRoot := mustHash(t, step.IncrementalRoot)
					if gotStepRoot != wantStepRoot {
						t.Errorf("step %d (after %s): incremental root = %x, want %x",
							i, step.AfterKey, gotStepRoot, wantStepRoot)
					}
				}
			}
		})
	}
}

// testPath returns a distinct 32-byte SMT path seeded from b.
func testPath(b byte) trie.Path {
	var p trie.Path
	p[0] = b
	return p
}

// TestApplyMixedPresentEmptyAndDelete applies a present-empty write and a delete in one diff.
//
// The two are different operations with different results: a present-empty write leaves the
// path in the tree holding a zero-length value, a delete takes the path out of the tree
// entirely, and the applier keeps them apart within a single diff. The expected post-root is
// computed independently through the shared path-native core over the leaf set the diff
// implies, so the applier is checked against the primitive the L1 side commits with rather
// than against a constant.
func TestApplyMixedPresentEmptyAndDelete(t *testing.T) {
	emptied := testPath(0xA1)
	deleted := testPath(0xB2)
	untouched := testPath(0xC3)

	s := openTestStore(t)
	if _, err := s.Apply([]domain.StateDiffEntry{
		{Path: emptied, Value: []byte("a")},
		{Path: deleted, Value: []byte("b")},
		{Path: untouched, Value: []byte("c")},
	}); err != nil {
		t.Fatalf("seed Apply: %v", err)
	}

	got, err := s.Apply([]domain.StateDiffEntry{
		{Path: emptied, Value: []byte{}},
		{Path: deleted, Value: nil},
	})
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}

	want, err := trie.RootOfLeaves(
		[]trie.Path{emptied, untouched},
		[][]byte{{}, []byte("c")},
	)
	if err != nil {
		t.Fatalf("RootOfLeaves: %v", err)
	}
	if got != want {
		t.Errorf("post-root = %x, want %x", got, want)
	}

	present, value, _, err := s.Prove(emptied)
	if err != nil {
		t.Fatalf("Prove(emptied): %v", err)
	}
	if !present {
		t.Errorf("emptied: present = false, want true")
	}
	if len(value) != 0 {
		t.Errorf("emptied: value = %x, want zero-length", value)
	}

	present, _, _, err = s.Prove(deleted)
	if err != nil {
		t.Fatalf("Prove(deleted): %v", err)
	}
	if present {
		t.Errorf("deleted: present = true, want false")
	}

	// A present-empty leaf and an absent one give different roots: the same leaf set with
	// emptied removed rather than emptied to zero length hashes differently.
	absentRoot, err := trie.RootOfLeaves(
		[]trie.Path{untouched},
		[][]byte{[]byte("c")},
	)
	if err != nil {
		t.Fatalf("RootOfLeaves(absent): %v", err)
	}
	if got == absentRoot {
		t.Errorf("present-empty root == absent root, expected distinct")
	}
}
