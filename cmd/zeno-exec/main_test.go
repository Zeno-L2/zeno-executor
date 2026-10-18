package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/digitalSloth/go-executor/plugins/wasm"
)

// validModule is a module exporting memory and run, a function of type
// (i32,i32)->i64 returning 0, with one memory page.
var validModule = []byte{
	0x00, 'a', 's', 'm', 0x01, 0x00, 0x00, 0x00,
	// type: (i32,i32)->i64
	0x01, 0x07, 0x01, 0x60, 0x02, 0x7F, 0x7F, 0x01, 0x7E,
	// function: one function of type 0
	0x03, 0x02, 0x01, 0x00,
	// memory: min 1 page
	0x05, 0x03, 0x01, 0x00, 0x01,
	// export: memory, run
	0x07, 0x10, 0x02, 0x06, 'm', 'e', 'm', 'o', 'r', 'y', 0x02, 0x00, 0x03, 'r', 'u', 'n', 0x00, 0x00,
	// code: no locals, i64.const 0, end
	0x0A, 0x06, 0x01, 0x04, 0x00, 0x42, 0x00, 0x0B,
}

func exec(t *testing.T, stdin string, args ...string) (int, string, string) {
	t.Helper()
	var out, errb bytes.Buffer
	code := run(args, strings.NewReader(stdin), &out, &errb)
	return code, out.String(), errb.String()
}

func writeModule(t *testing.T, b []byte) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "m.wasm")
	if err := os.WriteFile(p, b, 0o600); err != nil {
		t.Fatal(err)
	}
	return p
}

func TestVersion(t *testing.T) {
	code, out, _ := exec(t, "", "version")
	if code != 0 || !strings.Contains(out, `"version":"devel"`) {
		t.Fatalf("version: %d %s", code, out)
	}
}

func TestValidate(t *testing.T) {
	code, out, _ := exec(t, "", "validate", "--metadata", "immutable", writeModule(t, validModule))
	if code != 0 {
		t.Fatalf("validate: %d %s", code, out)
	}
	var m map[string]any
	if err := json.Unmarshal([]byte(out), &m); err != nil {
		t.Fatal(err)
	}
	if m["ok"] != true || m["initialPages"] != float64(1) || m["metadata"] == nil || m["metadataHash"] == nil {
		t.Fatalf("validate result = %s", out)
	}

	code, out, _ = exec(t, "", "validate", writeModule(t, validModule))
	if code != 0 || strings.Contains(out, `"metadata"`) {
		t.Fatalf("validate without metadata: %d %s", code, out)
	}

	custom := append(append([]byte{}, validModule...), 0x00, 0x02, 0x01, 'x')
	code, out, _ = exec(t, "", "validate", writeModule(t, custom))
	if code != 1 || !strings.Contains(out, `"kind":"custom_section"`) {
		t.Fatalf("custom section: %d %s", code, out)
	}
}

func TestValidateBoundedRead(t *testing.T) {
	code, out, _ := exec(t, "", "validate", writeModule(t, make([]byte, wasm.MaxCodeSize+1)))
	if code != 1 || !strings.Contains(out, `"kind":"module_too_large"`) {
		t.Fatalf("oversized file: %d %s", code, out)
	}
	code, out, errs := exec(t, "", "validate", t.TempDir())
	if code != 2 || out != "" || !strings.Contains(errs, "usage:") {
		t.Fatalf("directory: %d %q %q", code, out, errs)
	}
}

func TestValidateReadStopsAtLimit(t *testing.T) {
	size := wasm.MaxCodeSize + 4096
	_, out, _ := exec(t, "", "validate", writeModule(t, make([]byte, size)))
	want := fmt.Sprintf("%d bytes exceeds", wasm.MaxCodeSize+1)
	if !strings.Contains(out, want) {
		t.Fatalf("oversized file: %s, want it to report %q", out, want)
	}
}

func TestValidateRefusesDevice(t *testing.T) {
	if _, err := os.Stat("/dev/zero"); err != nil {
		t.Skip("no /dev/zero")
	}
	code, out, errs := exec(t, "", "validate", "/dev/zero")
	if code != 2 || out != "" || !strings.Contains(errs, "not a regular file") {
		t.Fatalf("/dev/zero: %d %q %q", code, out, errs)
	}
}

func TestValidateRefusesFIFO(t *testing.T) {
	p := filepath.Join(t.TempDir(), "f")
	if err := syscall.Mkfifo(p, 0o600); err != nil {
		t.Skipf("mkfifo: %v", err)
	}
	done := make(chan struct{})
	var code int
	var errs string
	go func() {
		code, _, errs = exec(t, "", "validate", p)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("validate blocked on a FIFO")
	}
	if code != 2 || !strings.Contains(errs, "not a regular file") {
		t.Fatalf("fifo: %d %q", code, errs)
	}
}

func TestUsageErrors(t *testing.T) {
	good := writeModule(t, validModule)
	zero := "owner:" + strings.Repeat("0", 40)
	cases := map[string][]string{
		"NoArgs":           {},
		"UnknownCommand":   {"bogus"},
		"MissingPath":      {"validate", filepath.Join(t.TempDir(), "absent.wasm")},
		"NoPositional":     {"validate"},
		"BadMetadata":      {"validate", "--metadata", "bogus", good},
		"ZeroOwner":        {"validate", "--metadata", zero, good},
		"UnknownFlag":      {"run", "--nope"},
		"EmptyNetwork":     {"run", "--network", ""},
		"NonLoopbackServe": {"serve", "--listen", "0.0.0.0:8547"},
		"BadOrigin":        {"serve", "--allow-origin", "ftp://x"},
		"VersionExtra":     {"version", "extra"},
	}
	for name, args := range cases {
		t.Run(name, func(t *testing.T) {
			code, out, errs := exec(t, "", args...)
			if code != 2 || out != "" || !strings.Contains(errs, "usage:") {
				t.Fatalf("code %d, stdout %q, stderr %q", code, out, errs)
			}
		})
	}
}

func TestHelp(t *testing.T) {
	code, out, errs := exec(t, "", "run", "-h")
	if code != 0 || out != "" || !strings.Contains(errs, "usage:") {
		t.Fatalf("help: %d %q %q", code, out, errs)
	}
}

func TestRunLines(t *testing.T) {
	in := "nonsense\n" + `{"op":"getStorage","contractId":"` + strings.Repeat("07", 32) + `","localKey":"6b"}` + "\n"
	code, out, _ := exec(t, in, "run")
	lines := strings.Split(strings.TrimSpace(out), "\n")
	if code != 0 || len(lines) != 2 {
		t.Fatalf("run: %d %q", code, out)
	}
	if !strings.Contains(lines[0], `"bad_command"`) || lines[1] != `{"ok":true,"value":null}` {
		t.Fatalf("run lines = %q", lines)
	}
}

func TestRunLineTooLong(t *testing.T) {
	code, out, _ := exec(t, strings.Repeat("a", 4<<20+10)+"\n", "run")
	if code != 1 || !strings.Contains(out, `"line exceeds 4 MiB"`) {
		t.Fatalf("long line: %d %q", code, out[:min(len(out), 200)])
	}
}
