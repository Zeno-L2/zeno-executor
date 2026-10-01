package supervisor

import (
	"fmt"
	"io"
	"os"
	"syscall"
)

// maxKeyFileBytes caps how much of a FILE key source LoadKey reads. A submission key is at most
// a few hundred bytes; anything near this size is already a misconfiguration, not a key.
const maxKeyFileBytes = 1 << 20 // 1 MiB

// LoadKey reads the submission key named by src. Every error wraps ErrConfig and names the path
// or the variable, never the key bytes.
func LoadKey(src KeySource) ([]byte, error) {
	switch src.Kind {
	case "FILE":
		return loadFileKey(src)
	case "ENV":
		return loadEnvKey(src)
	case "KMS":
		return nil, fmt.Errorf("%w: submissionKeySource kind KMS is not implemented", ErrConfig)
	default:
		return nil, fmt.Errorf("%w: submissionKeySource kind %q is not recognised", ErrConfig, src.Kind)
	}
}

func loadFileKey(src KeySource) ([]byte, error) {
	if src.Path == "" {
		return nil, fmt.Errorf("%w: submissionKeySource kind FILE requires path", ErrConfig)
	}
	if src.Name != "" {
		return nil, fmt.Errorf("%w: submissionKeySource kind FILE does not accept name", ErrConfig)
	}

	// O_NONBLOCK keeps a FIFO at path with no writer from hanging this open indefinitely; it has
	// no effect on a regular file's later reads.
	f, err := os.OpenFile(src.Path, os.O_RDONLY|syscall.O_NONBLOCK, 0)
	if err != nil {
		return nil, fmt.Errorf("%w: submissionKeySource path %q: %w", ErrConfig, src.Path, err)
	}
	defer f.Close()

	// The mode checked is the mode of the file just opened, so this behaves the same as root.
	fi, err := f.Stat()
	if err != nil {
		return nil, fmt.Errorf("%w: submissionKeySource path %q: %w", ErrConfig, src.Path, err)
	}
	if !fi.Mode().IsRegular() {
		return nil, fmt.Errorf("%w: submissionKeySource path %q is not a regular file", ErrConfig, src.Path)
	}
	if fi.Mode().Perm()&0o077 != 0 {
		return nil, fmt.Errorf("%w: submissionKeySource path %q must not be readable or writable by group or other", ErrConfig, src.Path)
	}
	// A hard link to this file elsewhere, for example inside a worker's own storage directory,
	// would let a later PrepareDir's Lchown of that link hand this file's shared inode to a
	// worker uid without checkWorkerAccess's path-based checks ever seeing this name. Stat_t
	// comes from the descriptor already open above, not a fresh path lookup, so this cannot be
	// raced by relinking between the two.
	if st, ok := fi.Sys().(*syscall.Stat_t); ok && st.Nlink > 1 {
		return nil, fmt.Errorf("%w: submissionKeySource path %q has %d hard links; a submission key file must have exactly one", ErrConfig, src.Path, st.Nlink)
	}

	// Read one byte past the cap so an over-size file is rejected rather than silently
	// truncated.
	b, err := io.ReadAll(io.LimitReader(f, maxKeyFileBytes+1))
	if err != nil {
		return nil, fmt.Errorf("%w: submissionKeySource path %q: %w", ErrConfig, src.Path, err)
	}
	if len(b) > maxKeyFileBytes {
		return nil, fmt.Errorf("%w: submissionKeySource path %q exceeds %d bytes", ErrConfig, src.Path, maxKeyFileBytes)
	}
	if len(b) == 0 {
		return nil, fmt.Errorf("%w: submissionKeySource path %q is empty", ErrConfig, src.Path)
	}
	return b, nil
}

func loadEnvKey(src KeySource) ([]byte, error) {
	if src.Name == "" {
		return nil, fmt.Errorf("%w: submissionKeySource kind ENV requires name", ErrConfig)
	}
	if src.Path != "" {
		return nil, fmt.Errorf("%w: submissionKeySource kind ENV does not accept path", ErrConfig)
	}

	v, ok := os.LookupEnv(src.Name)
	if !ok || v == "" {
		return nil, fmt.Errorf("%w: submissionKeySource variable %s is not set", ErrConfig, src.Name)
	}
	os.Unsetenv(src.Name)
	return []byte(v), nil
}
