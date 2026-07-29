// Package lease provides an exclusive, advisory file lock guaranteeing that only one
// process holds a given executor identity at a time. A process that fails to acquire the
// lease for its configured executorId MUST exit before entering the execution loop, so two
// copies of the same identity can never equivocate.
package lease

import (
	"fmt"
	"os"
	"syscall"
)

// Lease is a held, exclusive lock on a single lock file.
type Lease struct {
	file *os.File
}

// Acquire takes an exclusive, non-blocking lock on path, creating it if necessary. It
// returns an error if another process already holds the lock.
func Acquire(path string) (*Lease, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, fmt.Errorf("lease: open %s: %w", path, err)
	}

	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		f.Close()
		return nil, fmt.Errorf("lease: %s is held by another process: %w", path, err)
	}

	return &Lease{file: f}, nil
}

// Release drops the lock and closes the underlying file. The lease is unusable afterward.
func (l *Lease) Release() error {
	defer l.file.Close()
	return syscall.Flock(int(l.file.Fd()), syscall.LOCK_UN)
}
