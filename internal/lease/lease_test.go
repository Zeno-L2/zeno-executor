package lease

import (
	"path/filepath"
	"testing"
)

func TestAcquireExclusive(t *testing.T) {
	path := filepath.Join(t.TempDir(), "executor.lock")

	first, err := Acquire(path)
	if err != nil {
		t.Fatalf("first Acquire: %v", err)
	}
	defer first.Release()

	if _, err := Acquire(path); err == nil {
		t.Fatal("second Acquire on the same path succeeded; want an error")
	}
}

func TestAcquireAfterRelease(t *testing.T) {
	path := filepath.Join(t.TempDir(), "executor.lock")

	first, err := Acquire(path)
	if err != nil {
		t.Fatalf("first Acquire: %v", err)
	}
	if err := first.Release(); err != nil {
		t.Fatalf("Release: %v", err)
	}

	second, err := Acquire(path)
	if err != nil {
		t.Fatalf("Acquire after release: %v", err)
	}
	defer second.Release()
}
