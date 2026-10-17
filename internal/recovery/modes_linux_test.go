//go:build linux

package recovery

import (
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"

	"github.com/digitalSloth/go-executor/internal/workerproc"
)

// TestMain runs the helper modes of TestModesUnderUIDSeparation when this binary is exec'd as a
// worker or gateway stand-in, and the suite otherwise.
func TestMain(m *testing.M) {
	if mode := os.Getenv("RECOVERY_MODES_HELPER"); mode != "" {
		os.Exit(runModesHelper(mode, os.Getenv("RECOVERY_MODES_DIR")))
	}
	os.Exit(m.Run())
}

func runModesHelper(mode, storage string) int {
	switch mode {
	case "write":
		if _, err := writeArtifacts(storage); err != nil {
			fmt.Fprintln(os.Stderr, "write:", err)
			return 1
		}
		return 0
	case "read":
		return readAsGateway(storage)
	}
	fmt.Fprintln(os.Stderr, "unknown helper mode", mode)
	return 1
}

// readAsGateway checks what a user other than the domain's owner can and cannot read. It builds
// paths by hand: NewLayout would create directories and chmod them.
func readAsGateway(storage string) int {
	root := filepath.Join(storage, "1")
	failed := 0
	fail := func(format string, args ...any) {
		fmt.Fprintf(os.Stderr, format+"\n", args...)
		failed++
	}

	for _, name := range []string{"bundles", "receipts", "batches"} {
		dir := filepath.Join(root, name)
		entries, err := os.ReadDir(dir)
		if err != nil {
			fail("list %s: %v", dir, err)
			continue
		}
		files := 0
		for _, e := range entries {
			if !e.Type().IsRegular() {
				continue
			}
			files++
			if _, err := os.ReadFile(filepath.Join(dir, e.Name())); err != nil {
				fail("read %s/%s: %v", dir, e.Name(), err)
			}
		}
		if files == 0 {
			fail("%s lists no file", dir)
		}
	}

	if _, err := os.ReadDir(root); !errors.Is(err, fs.ErrPermission) {
		fail("list %s: err = %v, want permission denied", root, err)
	}
	for _, rel := range []string{"cursor", "meta.json", "momentum_hashes/window.json"} {
		path := filepath.Join(root, rel)
		if _, err := os.ReadFile(path); !errors.Is(err, fs.ErrPermission) {
			fail("read %s: err = %v, want permission denied", path, err)
		}
	}
	if failed > 0 {
		return 1
	}
	return 0
}

func TestModesUnderUIDSeparation(t *testing.T) {
	if os.Geteuid() != 0 {
		t.Skip("uid separation needs root")
	}
	h := workerproc.NewHost()
	if _, uids, reason := h.Enforcement(); !uids {
		t.Skipf("uid separation off: %s", reason)
	}

	base, err := os.MkdirTemp("", "recovery-modes-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(base) })
	if err := os.Chmod(base, 0o755); err != nil {
		t.Fatal(err)
	}
	storage := filepath.Join(base, "storage")
	if err := os.Mkdir(storage, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(storage, 0o755); err != nil {
		t.Fatal(err)
	}
	self, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	bin := filepath.Join(base, "recovery.test")
	copyTestBinary(t, self, bin)

	// 022 is the production umask. Under it NewLayout's unchmodded
	// momentum_hashes/ is 0755, so only the file mode can deny the
	// second user momentum_hashes/window.json.
	old := syscall.Umask(0o022)
	defer syscall.Umask(old)

	if err := h.PrepareDir(filepath.Join(storage, "1"), 10001); err != nil {
		t.Fatalf("PrepareDir: %v", err)
	}
	for _, step := range []struct {
		mode string
		uid  uint32
	}{{"write", 10001}, {"read", 10002}} {
		cmd := exec.Command(bin)
		cmd.Env = append(os.Environ(), "RECOVERY_MODES_HELPER="+step.mode, "RECOVERY_MODES_DIR="+storage)
		cmd.SysProcAttr = &syscall.SysProcAttr{Credential: &syscall.Credential{Uid: step.uid, Gid: step.uid}}
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("helper %s as uid %d: %v\n%s", step.mode, step.uid, err, out)
		}
	}
}

func copyTestBinary(t *testing.T, src, dst string) {
	t.Helper()
	in, err := os.Open(src)
	if err != nil {
		t.Fatal(err)
	}
	defer in.Close()
	out, err := os.OpenFile(dst, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o755)
	if err != nil {
		t.Fatal(err)
	}
	defer out.Close()
	if _, err := io.Copy(out, in); err != nil {
		t.Fatal(err)
	}
	if err := out.Chmod(0o755); err != nil {
		t.Fatal(err)
	}
}
