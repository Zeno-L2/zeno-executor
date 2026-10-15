package recovery

import (
	"os"
	"testing"

	"github.com/zenon-network/go-zenon/common/types"

	"github.com/digitalSloth/go-executor/internal/codec"
)

func sidecarFor(t *testing.T, inputIndex uint64) (sidecar []byte, root types.Hash) {
	t.Helper()
	rec := codec.Receipt{Version: codec.ReceiptVersion, InputIndex: inputIndex}
	leaf, err := rec.Encode()
	if err != nil {
		t.Fatalf("encode receipt: %v", err)
	}
	sidecar, err = codec.EncodeReceiptSidecar([]codec.Receipt{rec})
	if err != nil {
		t.Fatalf("encode sidecar: %v", err)
	}
	return sidecar, codec.MerkleRoot([][]byte{leaf})
}

func TestReceiptStorePutRejections(t *testing.T) {
	s, err := NewReceiptStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewReceiptStore: %v", err)
	}
	daHash := types.Hash{1}
	good, root := sidecarFor(t, 1)
	if err := s.Put(daHash, root, good); err != nil {
		t.Fatalf("Put: %v", err)
	}

	other, otherRoot := sidecarFor(t, 2)
	if err := s.Put(daHash, root, other); err == nil {
		t.Fatalf("Put accepted a sidecar whose leaves do not hash to receiptRoot")
	}
	if err := s.Put(daHash, otherRoot, []byte{0xFF, 0xFF}); err == nil {
		t.Fatalf("Put accepted a sidecar that does not decode")
	}
	got, err := os.ReadFile(s.path(daHash))
	if err != nil {
		t.Fatalf("read sidecar: %v", err)
	}
	if string(got) != string(good) {
		t.Fatalf("a rejected Put changed the stored sidecar")
	}
	if ents, _ := os.ReadDir(s.dir); len(ents) != 1 {
		t.Fatalf("receipts dir holds %d entries after rejected puts, want 1", len(ents))
	}

	if err := s.Put(daHash, otherRoot, other); err != nil {
		t.Fatalf("overwrite Put: %v", err)
	}
	got, _ = os.ReadFile(s.path(daHash))
	if string(got) != string(other) {
		t.Fatalf("Put did not replace the stored sidecar")
	}
}
