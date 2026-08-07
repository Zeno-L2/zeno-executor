package wasm

import (
	"os"
	"path/filepath"
	"testing"
)

func TestCodeStorePutGetRoundTrip(t *testing.T) {
	store, err := newCodeStore(t.TempDir())
	if err != nil {
		t.Fatalf("newCodeStore: %v", err)
	}

	key := []byte("code-hash-and-chunk-index")
	value := []byte("chunk bytes")

	if err := store.put(entryDeployChunk, key, value); err != nil {
		t.Fatalf("put: %v", err)
	}

	got, ok, err := store.get(entryDeployChunk, key)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if !ok {
		t.Fatal("get: ok = false, want true")
	}
	if string(got) != string(value) {
		t.Fatalf("get: value = %q, want %q", got, value)
	}
}

func TestCodeStoreGetMissingIsAbsent(t *testing.T) {
	store, err := newCodeStore(t.TempDir())
	if err != nil {
		t.Fatalf("newCodeStore: %v", err)
	}

	_, ok, err := store.get(entryAssembledModule, []byte("never-written"))
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if ok {
		t.Fatal("get: ok = true for a never-written key, want false")
	}
}

func TestCodeStoreCorruptChecksumReadsAsAbsent(t *testing.T) {
	root := t.TempDir()
	store, err := newCodeStore(root)
	if err != nil {
		t.Fatalf("newCodeStore: %v", err)
	}

	key := []byte("k")
	value := []byte("v")
	if err := store.put(entryPendingRecord, key, value); err != nil {
		t.Fatalf("put: %v", err)
	}

	path := store.path(entryPendingRecord, key)
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read committed file: %v", err)
	}
	b[0] ^= 0xFF // flip a byte inside the payload, invalidating the trailing checksum
	if err := os.WriteFile(path, b, 0o644); err != nil {
		t.Fatalf("corrupt committed file: %v", err)
	}

	_, ok, err := store.get(entryPendingRecord, key)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if ok {
		t.Fatal("get: ok = true for a corrupt checksum, want false")
	}
}

func TestCodeStoreAtomicRenameSurvivesInterruptedWrite(t *testing.T) {
	root := t.TempDir()
	store, err := newCodeStore(root)
	if err != nil {
		t.Fatalf("newCodeStore: %v", err)
	}

	key := []byte("k")
	original := []byte("original value")
	if err := store.put(entryInstrumentedModule, key, original); err != nil {
		t.Fatalf("put: %v", err)
	}

	// Simulate a write interrupted before its rename: a stray temp file sits alongside the
	// committed entry. get must still return the last successfully committed value, never the
	// stray temp bytes, and the directory scan (a fresh newCodeStore) must not choke on it.
	dir := filepath.Join(root, entryInstrumentedModule.dirName())
	if err := os.WriteFile(filepath.Join(dir, ".tmp-stray"), []byte("half-written"), 0o644); err != nil {
		t.Fatalf("write stray temp file: %v", err)
	}

	reopened, err := newCodeStore(root)
	if err != nil {
		t.Fatalf("newCodeStore (reopen): %v", err)
	}
	got, ok, err := reopened.get(entryInstrumentedModule, key)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if !ok {
		t.Fatal("get: ok = false, want true (committed entry must survive a stray temp file)")
	}
	if string(got) != string(original) {
		t.Fatalf("get: value = %q, want %q", got, original)
	}
}

func TestNewCodeStoreRejectsEmptyPath(t *testing.T) {
	if _, err := newCodeStore(""); err == nil {
		t.Fatal("newCodeStore(\"\"): got nil error, want an error")
	}
}

func TestCodeStorePutOverwritesExistingEntry(t *testing.T) {
	store, err := newCodeStore(t.TempDir())
	if err != nil {
		t.Fatalf("newCodeStore: %v", err)
	}

	key := []byte("k")
	if err := store.put(entryDeployChunk, key, []byte("first value")); err != nil {
		t.Fatalf("put (first): %v", err)
	}
	if err := store.put(entryDeployChunk, key, []byte("second value")); err != nil {
		t.Fatalf("put (second): %v", err)
	}

	got, ok, err := store.get(entryDeployChunk, key)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if !ok {
		t.Fatal("get: ok = false, want true")
	}
	if string(got) != "second value" {
		t.Fatalf("get: value = %q, want %q (overwrite must replace, not append)", got, "second value")
	}
}

func TestCodeStorePutEmptyValueRoundTrips(t *testing.T) {
	store, err := newCodeStore(t.TempDir())
	if err != nil {
		t.Fatalf("newCodeStore: %v", err)
	}

	key := []byte("k")
	if err := store.put(entryAssembledModule, key, []byte{}); err != nil {
		t.Fatalf("put: %v", err)
	}

	got, ok, err := store.get(entryAssembledModule, key)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if !ok {
		t.Fatal("get: ok = false, want true for a present-but-empty value")
	}
	if len(got) != 0 {
		t.Fatalf("get: value = %q, want empty", got)
	}
}

func TestCodeStoreGetTooShortFileIsAbsent(t *testing.T) {
	root := t.TempDir()
	store, err := newCodeStore(root)
	if err != nil {
		t.Fatalf("newCodeStore: %v", err)
	}

	key := []byte("k")
	if err := store.put(entryPendingRecord, key, []byte("v")); err != nil {
		t.Fatalf("put: %v", err)
	}

	// Truncate the committed file below the 32-byte checksum size, simulating a write that was
	// interrupted before it ever wrote a full checksum.
	path := store.path(entryPendingRecord, key)
	if err := os.WriteFile(path, []byte{0x01, 0x02, 0x03}, 0o644); err != nil {
		t.Fatalf("truncate committed file: %v", err)
	}

	_, ok, err := store.get(entryPendingRecord, key)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if ok {
		t.Fatal("get: ok = true for a too-short file, want false")
	}
}
