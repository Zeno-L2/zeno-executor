package supervisor

import (
	"crypto/ed25519"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/zenon-network/go-zenon/common/types"
	"github.com/zenon-network/go-zenon/wallet"
)

const keyContent = "s3cr3t-key-material-do-not-log-me"

func writeKeyFile(t *testing.T, mode os.FileMode) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "key")
	writeFile(t, path, keyContent)
	if err := os.Chmod(path, mode); err != nil {
		t.Fatalf("Chmod: %v", err)
	}
	return path
}

func TestLoadKeyFileAccepted(t *testing.T) {
	for _, mode := range []os.FileMode{0o400, 0o600} {
		t.Run(mode.String(), func(t *testing.T) {
			path := writeKeyFile(t, mode)
			got, err := LoadKey(KeySource{Kind: "FILE", Path: path})
			if err != nil {
				t.Fatalf("LoadKey: %v", err)
			}
			if string(got) != keyContent {
				t.Fatalf("LoadKey = %q, want %q", got, keyContent)
			}
		})
	}
}

func TestLoadKeyFileRejected(t *testing.T) {
	dir := t.TempDir()

	emptyPath := filepath.Join(dir, "empty")
	writeFile(t, emptyPath, "")
	if err := os.Chmod(emptyPath, 0o600); err != nil {
		t.Fatalf("Chmod: %v", err)
	}

	cases := []struct {
		name string
		src  KeySource
	}{
		{"mode 0440", KeySource{Kind: "FILE", Path: writeKeyFile(t, 0o440)}},
		{"mode 0404", KeySource{Kind: "FILE", Path: writeKeyFile(t, 0o404)}},
		{"mode 0640", KeySource{Kind: "FILE", Path: writeKeyFile(t, 0o640)}},
		{"missing", KeySource{Kind: "FILE", Path: filepath.Join(dir, "does-not-exist")}},
		{"empty", KeySource{Kind: "FILE", Path: emptyPath}},
		{"a directory", KeySource{Kind: "FILE", Path: dir}},
		{"with name set", KeySource{Kind: "FILE", Path: writeKeyFile(t, 0o600), Name: "X"}},
		{"hard linked", func() KeySource {
			target := writeKeyFile(t, 0o400)
			link := target + "-link"
			if err := os.Link(target, link); err != nil {
				t.Fatalf("Link: %v", err)
			}
			return KeySource{Kind: "FILE", Path: target}
		}()},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := LoadKey(tc.src)
			if err == nil {
				t.Fatalf("LoadKey: want an error")
			}
			if !errors.Is(err, ErrConfig) {
				t.Fatalf("LoadKey error = %v, want wrapping ErrConfig", err)
			}
			if strings.Contains(err.Error(), keyContent) {
				t.Fatalf("error text contains the key: %v", err)
			}
		})
	}
}

// TestLoadKeyFileFIFONoWriter confirms O_NONBLOCK does its job: a FIFO with no writer must not
// hang LoadKey. The open returns immediately, and the file is then rejected as not a regular
// file (a FIFO can never pass the IsRegular check), so this also doubles as the "not a regular
// file" rejection for a FIFO specifically.
func TestLoadKeyFileFIFONoWriter(t *testing.T) {
	path := filepath.Join(t.TempDir(), "fifo")
	if err := syscall.Mkfifo(path, 0o600); err != nil {
		t.Fatalf("Mkfifo: %v", err)
	}

	done := make(chan struct{})
	var err error
	go func() {
		_, err = LoadKey(KeySource{Kind: "FILE", Path: path})
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatalf("LoadKey did not return: a FIFO with no writer blocked the open")
	}

	if !errors.Is(err, ErrConfig) {
		t.Fatalf("LoadKey error = %v, want wrapping ErrConfig", err)
	}
	if !strings.Contains(err.Error(), "not a regular file") {
		t.Fatalf("LoadKey error = %v, want it to name the FIFO as not a regular file", err)
	}
}

// TestLoadKeyFileSizeCap writes a file exactly at maxKeyFileBytes (accepted) and one byte over it
// (rejected, with the error naming the cap), proving the cap is enforced rather than the read
// being silently truncated or left unbounded.
func TestLoadKeyFileSizeCap(t *testing.T) {
	t.Run("exactly at the cap", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "key")
		writeFile(t, path, strings.Repeat("k", maxKeyFileBytes))
		if err := os.Chmod(path, 0o600); err != nil {
			t.Fatalf("Chmod: %v", err)
		}
		got, err := LoadKey(KeySource{Kind: "FILE", Path: path})
		if err != nil {
			t.Fatalf("LoadKey: %v", err)
		}
		if len(got) != maxKeyFileBytes {
			t.Fatalf("len(LoadKey) = %d, want %d", len(got), maxKeyFileBytes)
		}
	})

	t.Run("one byte over the cap", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "key")
		writeFile(t, path, strings.Repeat("k", maxKeyFileBytes+1))
		if err := os.Chmod(path, 0o600); err != nil {
			t.Fatalf("Chmod: %v", err)
		}
		_, err := LoadKey(KeySource{Kind: "FILE", Path: path})
		if !errors.Is(err, ErrConfig) {
			t.Fatalf("LoadKey error = %v, want wrapping ErrConfig", err)
		}
		if !strings.Contains(err.Error(), "exceeds") {
			t.Fatalf("LoadKey error = %v, want it to mention the cap", err)
		}
	})
}

func TestLoadKeyEnv(t *testing.T) {
	t.Run("accepted", func(t *testing.T) {
		t.Setenv("SUPERVISOR_TEST_KEY", keyContent)
		got, err := LoadKey(KeySource{Kind: "ENV", Name: "SUPERVISOR_TEST_KEY"})
		if err != nil {
			t.Fatalf("LoadKey: %v", err)
		}
		if string(got) != keyContent {
			t.Fatalf("LoadKey = %q, want %q", got, keyContent)
		}
		if v, ok := os.LookupEnv("SUPERVISOR_TEST_KEY"); ok {
			t.Fatalf("variable still set: %q", v)
		}
		for _, e := range os.Environ() {
			if strings.HasPrefix(e, "SUPERVISOR_TEST_KEY=") {
				t.Fatalf("os.Environ() still carries the variable: %q", e)
			}
		}
	})

	t.Run("rejected", func(t *testing.T) {
		t.Setenv("SUPERVISOR_TEST_EMPTY", "")
		cases := []struct {
			name string
			src  KeySource
		}{
			{"unset", KeySource{Kind: "ENV", Name: "SUPERVISOR_TEST_UNSET"}},
			{"empty", KeySource{Kind: "ENV", Name: "SUPERVISOR_TEST_EMPTY"}},
			{"empty name", KeySource{Kind: "ENV", Name: ""}},
		}
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				_, err := LoadKey(tc.src)
				if err == nil {
					t.Fatalf("LoadKey: want an error")
				}
				if !errors.Is(err, ErrConfig) {
					t.Fatalf("LoadKey error = %v, want wrapping ErrConfig", err)
				}
			})
		}
	})
}

// TestLoadKeyEnvPathSetIsRejected uses a variable that is set and non-empty, so the only way
// LoadKey can still return an error is the path-set rejection itself (an unset-variable case
// would fail even with that check removed, so it cannot prove this rejection fires). It also
// confirms the rejection runs before os.Unsetenv: the variable must still be set afterwards.
func TestLoadKeyEnvPathSetIsRejected(t *testing.T) {
	t.Setenv("SUPERVISOR_TEST_PATH_SET", keyContent)

	_, err := LoadKey(KeySource{Kind: "ENV", Name: "SUPERVISOR_TEST_PATH_SET", Path: "/x"})
	if err == nil {
		t.Fatalf("LoadKey: want an error")
	}
	if !errors.Is(err, ErrConfig) {
		t.Fatalf("LoadKey error = %v, want wrapping ErrConfig", err)
	}
	if !strings.Contains(err.Error(), "path") {
		t.Fatalf("LoadKey error = %v, want it to mention path", err)
	}
	if v, ok := os.LookupEnv("SUPERVISOR_TEST_PATH_SET"); !ok || v != keyContent {
		t.Fatalf("variable = %q, ok=%v, want unchanged and still set", v, ok)
	}
}

func TestLoadKeyKMSAndUnknown(t *testing.T) {
	for _, kind := range []string{"KMS", "", "VAULT"} {
		t.Run(kind, func(t *testing.T) {
			_, err := LoadKey(KeySource{Kind: kind})
			if err == nil {
				t.Fatalf("LoadKey: want an error")
			}
			if !errors.Is(err, ErrConfig) {
				t.Fatalf("LoadKey error = %v, want wrapping ErrConfig", err)
			}
		})
	}
}

// testSeed is a fixed 32-byte ed25519 seed in hex.
var testSeed = strings.Repeat("ab", 32)

func seedAddress(t *testing.T, seedHex string) types.Address {
	t.Helper()
	seed, err := hex.DecodeString(seedHex)
	if err != nil {
		t.Fatalf("DecodeString: %v", err)
	}
	return types.PubKeyToAddress(ed25519.NewKeyFromSeed(seed).Public().(ed25519.PublicKey))
}

func TestLoadSigner(t *testing.T) {
	want := seedAddress(t, testSeed)
	msg := []byte("message")

	check := func(t *testing.T, kp *wallet.KeyPair, err error) {
		t.Helper()
		if err != nil {
			t.Fatalf("LoadSigner: %v", err)
		}
		if kp.Address != want {
			t.Fatalf("Address = %s, want %s", kp.Address, want)
		}
		if ok, err := wallet.VerifySignature(kp.Public, msg, kp.Sign(msg)); !ok || err != nil {
			t.Fatalf("VerifySignature = %v, %v", ok, err)
		}
	}

	t.Run("FILE", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "key")
		writeFile(t, path, testSeed+"\n")
		if err := os.Chmod(path, 0o600); err != nil {
			t.Fatalf("Chmod: %v", err)
		}
		kp, err := LoadSigner(KeySource{Kind: "FILE", Path: path}, want)
		check(t, kp, err)
	})

	t.Run("ENV", func(t *testing.T) {
		t.Setenv("TEST_SIGNER_KEY", "  "+testSeed+"  ")
		kp, err := LoadSigner(KeySource{Kind: "ENV", Name: "TEST_SIGNER_KEY"}, want)
		check(t, kp, err)
	})

	t.Run("upper case", func(t *testing.T) {
		t.Setenv("TEST_SIGNER_KEY", strings.ToUpper(strings.Repeat("ab", 32)))
		kp, err := LoadSigner(KeySource{Kind: "ENV", Name: "TEST_SIGNER_KEY"}, want)
		check(t, kp, err)
	})
}

func TestLoadSignerRejected(t *testing.T) {
	executor := seedAddress(t, testSeed)
	otherSeed := strings.Repeat("cd", 32)
	cases := map[string]string{
		"63 characters":     testSeed[:63],
		"65 characters":     testSeed + "a",
		"0x prefix":         "0x" + testSeed,
		"non-hex character": testSeed[:63] + "g",
		"whitespace only":   "   ",
		"another address":   otherSeed,
		"inner whitespace":  testSeed[:30] + " " + testSeed[31:],
	}
	for name, key := range cases {
		t.Run(name, func(t *testing.T) {
			t.Setenv("TEST_SIGNER_KEY", key)
			_, err := LoadSigner(KeySource{Kind: "ENV", Name: "TEST_SIGNER_KEY"}, executor)
			if !errors.Is(err, ErrConfig) {
				t.Fatalf("LoadSigner = %v, want ErrConfig", err)
			}
			msg := err.Error()
			if trimmed := strings.TrimSpace(key); (trimmed != "" && strings.Contains(msg, trimmed)) || strings.Contains(msg, "invalid byte") {
				t.Fatalf("error holds key material: %q", msg)
			}
		})
	}
}

func TestSignerFromKeyClearsRaw(t *testing.T) {
	executor := seedAddress(t, testSeed)
	src := KeySource{Kind: "ENV", Name: "K"}
	cases := map[string]string{
		"success":           testSeed,
		"another address":   strings.Repeat("cd", 32),
		"non-hex character": testSeed[:63] + "g",
	}
	for name, key := range cases {
		t.Run(name, func(t *testing.T) {
			raw := []byte(key)
			signerFromKey(raw, src, executor)
			for i, c := range raw {
				if c != 0 {
					t.Fatalf("raw[%d] = %#x, want 0", i, c)
				}
			}
		})
	}
}
