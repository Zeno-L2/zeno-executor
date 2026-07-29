package state

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/zenon-network/go-zenon/common/trie"

	"github.com/digitalSloth/go-executor/domain"
)

// TestCrashRecoveryTornWrite simulates a kill mid-write: after two committed applies, a
// third record is left torn (a crash partway through the append). Reopening must discard
// exactly that torn tail and recover to the last fully committed root, leaving the log usable
// for further applies.
func TestCrashRecoveryTornWrite(t *testing.T) {
	walPath := filepath.Join(t.TempDir(), "state.wal")

	var pathA, pathB, pathC trie.Path
	pathA[0], pathB[0], pathC[0] = 0x01, 0x02, 0x03

	s, err := Open(walPath)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if _, err := s.Apply([]domain.StateDiffEntry{{Path: pathA, Value: []byte("a")}}); err != nil {
		t.Fatalf("Apply 1: %v", err)
	}
	lastGoodRoot, err := s.Apply([]domain.StateDiffEntry{{Path: pathB, Value: []byte("b")}})
	if err != nil {
		t.Fatalf("Apply 2: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Simulate a crash mid-append: hand-append a record header promising more payload bytes
	// than actually follow, exactly what a torn write leaves behind.
	f, err := os.OpenFile(walPath, os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		t.Fatalf("reopen for torn write: %v", err)
	}
	torn := encodeRecord([]domain.StateDiffEntry{{Path: pathC, Value: []byte("c")}})
	header := make([]byte, 4)
	header[3] = byte(len(torn)) // recordLen claims the full payload...
	if _, err := f.Write(header); err != nil {
		t.Fatalf("write torn header: %v", err)
	}
	if _, err := f.Write(torn[:len(torn)/2]); err != nil { // ...but only half of it is written.
		t.Fatalf("write torn payload: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("close torn write: %v", err)
	}

	reopened, err := Open(walPath)
	if err != nil {
		t.Fatalf("Open after torn write: %v", err)
	}
	defer reopened.Close()

	gotRoot, err := reopened.Root()
	if err != nil {
		t.Fatalf("Root: %v", err)
	}
	if gotRoot != lastGoodRoot {
		t.Fatalf("root after recovery = %x, want last committed root %x", gotRoot, lastGoodRoot)
	}

	// The log must still be usable: a fresh apply should succeed and persist.
	if _, err := reopened.Apply([]domain.StateDiffEntry{{Path: pathC, Value: []byte("c")}}); err != nil {
		t.Fatalf("Apply after recovery: %v", err)
	}
}
