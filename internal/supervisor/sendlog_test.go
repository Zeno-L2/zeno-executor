package supervisor

import (
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/zenon-network/go-zenon/common/types"
	"github.com/zenon-network/go-zenon/vm/embedded/definition"
)

// logRec returns a readable record for domain d and batch b whose hashes derive from tag.
func logRec(d uint32, b uint64, tag byte) SendRecord {
	return SendRecord{
		DomainID: d,
		Method:   definition.SettlementSubmitBatchMethodName,
		BatchID:  b,
		Height:   7,
		Hash:     types.Hash{tag},
		DataHash: types.Hash{tag, 0x01},
	}
}

// logRecLine returns r as the log writes it, one line without its newline.
func logRecLine(r SendRecord) string {
	s := fmt.Sprintf(`{"version":1,"domainId":%d,"method":%q,"batchId":%d,"accountHeight":%d,"hash":"%s","dataHash":"%s"`,
		r.DomainID, r.Method, r.BatchID, r.Height, r.Hash, r.DataHash)
	if r.Outcome != "" {
		s += fmt.Sprintf(`,"outcome":%q`, string(r.Outcome))
	}
	return s + "}"
}

func logResolved(r SendRecord, o Outcome) SendRecord {
	r.Outcome = o
	return r
}

func logLines(rs ...SendRecord) string {
	var sb strings.Builder
	for _, r := range rs {
		sb.WriteString(logRecLine(r) + "\n")
	}
	return sb.String()
}

func writeLog(t *testing.T, path, content string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
}

func readLog(t *testing.T, path string) string {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

func newLogPath(t *testing.T) string {
	return filepath.Join(t.TempDir(), "x.submissions.jsonl")
}

func mustOpenLog(t *testing.T, path string) *fileLog {
	t.Helper()
	l, err := openSendLog(path)
	if err != nil {
		t.Fatalf("openSendLog: %v", err)
	}
	t.Cleanup(func() { l.Close() })
	return l
}

func TestSendLogRoundTrip(t *testing.T) {
	path := newLogPath(t)
	l := mustOpenLog(t, path)
	fi, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if fi.Mode().Perm() != 0o600 {
		t.Fatalf("mode = %v, want 0600", fi.Mode().Perm())
	}

	r := logRec(1, 3, 0xaa)
	if err := l.Sent(r); err != nil {
		t.Fatal(err)
	}
	if got, ok := l.Last(); !ok || got != r {
		t.Fatalf("Last = %+v, %v; want %+v", got, ok, r)
	}
	acc := logResolved(r, OutcomeAccepted)
	if err := l.Resolved(acc); err != nil {
		t.Fatal(err)
	}
	if got, _ := l.Last(); got != acc {
		t.Fatalf("Last = %+v, want %+v", got, acc)
	}

	want := fmt.Sprintf(`{"version":1,"domainId":1,"method":%q,"batchId":3,"accountHeight":7,"hash":"%s","dataHash":"%s"}`, r.Method, r.Hash, r.DataHash) + "\n" +
		fmt.Sprintf(`{"version":1,"domainId":1,"method":%q,"batchId":3,"accountHeight":7,"hash":"%s","dataHash":"%s","outcome":"accepted"}`, r.Method, r.Hash, r.DataHash) + "\n"
	if got := readLog(t, path); got != want {
		t.Fatalf("file = %q, want %q", got, want)
	}

	l.Close()
	l2 := mustOpenLog(t, path)
	if got, _ := l2.Last(); got != acc {
		t.Fatalf("reopened Last = %+v, want %+v", got, acc)
	}
	if l2.unreadable != nil {
		t.Fatalf("unreadable = %v, want nil", l2.unreadable)
	}
}

func TestSendLogReplay(t *testing.T) {
	a := logRec(1, 3, 0xaa)

	t.Run("sent only", func(t *testing.T) {
		path := newLogPath(t)
		l := mustOpenLog(t, path)
		if err := l.Sent(a); err != nil {
			t.Fatal(err)
		}
		l.Close()
		if got, ok := mustOpenLog(t, path).Last(); !ok || got != a {
			t.Fatalf("Last = %+v, %v; want %+v unresolved", got, ok, a)
		}
	})

	t.Run("abandoned", func(t *testing.T) {
		path := newLogPath(t)
		l := mustOpenLog(t, path)
		ab := logResolved(a, OutcomeAbandoned)
		if err := l.Sent(a); err != nil {
			t.Fatal(err)
		}
		if err := l.Resolved(ab); err != nil {
			t.Fatal(err)
		}
		l.Close()
		if got, _ := mustOpenLog(t, path).Last(); got != ab {
			t.Fatalf("Last = %+v, want %+v", got, ab)
		}
	})

	t.Run("an outcome with another hash is ignored", func(t *testing.T) {
		path := newLogPath(t)
		l := mustOpenLog(t, path)
		other := logResolved(logRec(1, 4, 0xbb), OutcomeAccepted)
		if err := l.Sent(a); err != nil {
			t.Fatal(err)
		}
		if err := l.Resolved(other); err != nil {
			t.Fatal(err)
		}
		l.Close()
		if got, _ := mustOpenLog(t, path).Last(); got != a {
			t.Fatalf("Last = %+v, want %+v unresolved", got, a)
		}
		if got, want := readLog(t, path), logLines(a, other); got != want {
			t.Fatalf("file = %q, want both lines %q", got, want)
		}
	})
}

func TestSendLogAccepted(t *testing.T) {
	d1b3, d1b4 := logRec(1, 3, 0x10), logRec(1, 4, 0x11)
	d2b0, d2b1 := logRec(2, 0, 0x20), logRec(2, 1, 0x21)
	acc := logResolved(d2b1, OutcomeAccepted)
	acc.Hash = types.Hash{0x99}
	cur := logLines(d1b3, logResolved(d1b3, OutcomeAccepted),
		d2b0, logResolved(d2b0, OutcomeRejected),
		d1b4, logResolved(d1b4, OutcomeAccepted),
		d2b1, acc)

	t.Run("latest per domain", func(t *testing.T) {
		path := newLogPath(t)
		writeLog(t, path, cur)
		if got, want := mustOpenLog(t, path).accepted, map[uint32]uint64{1: 4}; !reflect.DeepEqual(got, want) {
			t.Fatalf("accepted = %v, want %v", got, want)
		}
	})

	t.Run("the rotated file is replayed first", func(t *testing.T) {
		path := newLogPath(t)
		writeLog(t, path, cur)
		d2b7, d1b9 := logRec(2, 7, 0x30), logRec(1, 9, 0x31)
		writeLog(t, path+".1", logLines(d2b7, logResolved(d2b7, OutcomeAccepted), d1b9, logResolved(d1b9, OutcomeAccepted)))
		if got, want := mustOpenLog(t, path).accepted, map[uint32]uint64{1: 4, 2: 7}; !reflect.DeepEqual(got, want) {
			t.Fatalf("accepted = %v, want %v", got, want)
		}
	})

	t.Run("none", func(t *testing.T) {
		path := newLogPath(t)
		writeLog(t, path, logLines(d1b3, logResolved(d1b3, OutcomeRejected)))
		if got := mustOpenLog(t, path).accepted; len(got) != 0 {
			t.Fatalf("accepted = %v, want none", got)
		}
	})
}

func TestSendLogTornTail(t *testing.T) {
	first, second := logRec(1, 1, 0x01), logRec(1, 2, 0x02)
	head := logLines(first)
	nul := func(n int) string { return strings.Repeat("\x00", n) }
	secondLine := logRecLine(second)
	cases := map[string]string{
		"a prefix of a line":       `{"version":1,"domainId":1,"meth`,
		"a record with no newline": secondLine,
		"a tail of NUL bytes":      nul(40),
		"a line with a NUL start":  nul(20) + secondLine[20:] + "\n",
	}
	for name, tail := range cases {
		t.Run(name, func(t *testing.T) {
			out := captureLog(t)
			path := newLogPath(t)
			writeLog(t, path, head+tail)
			l := mustOpenLog(t, path)
			if got := readLog(t, path); got != head {
				t.Fatalf("file = %q, want only the first line", got)
			}
			if n := strings.Count(out.String(), "incomplete last line"); n != 1 {
				t.Fatalf("log has %d incomplete-line warnings, want 1: %q", n, out.String())
			}
			if got, _ := l.Last(); got != first {
				t.Fatalf("Last = %+v, want %+v", got, first)
			}
			if err := l.Sent(second); err != nil {
				t.Fatal(err)
			}
			l.Close()
			if got, _ := mustOpenLog(t, path).Last(); got != second {
				t.Fatalf("reopened Last = %+v, want %+v", got, second)
			}
		})
	}

	t.Run("only a torn line", func(t *testing.T) {
		path := newLogPath(t)
		writeLog(t, path, nul(30)+"\n")
		l := mustOpenLog(t, path)
		if l.size != 0 {
			t.Fatalf("size = %d, want 0", l.size)
		}
		if _, ok := l.Last(); ok {
			t.Fatal("Last is ok on an empty log")
		}
	})
}

func TestSendLogDamagedLine(t *testing.T) {
	r := logRec(1, 1, 0x01)
	good := logRecLine(r)
	bad := map[string]string{
		"not json":     "not json",
		"empty":        "",
		"two records":  good + good,
		"no version":   strings.Replace(good, `"version":1,`, "", 1),
		"version zero": strings.Replace(good, `"version":1`, `"version":0`, 1),
		"wrong type":   strings.Replace(good, `"domainId":1`, `"domainId":"x"`, 1),
		"zero hash":    strings.Replace(good, r.Hash.String(), types.ZeroHash.String(), 1),
		"a NUL byte":   strings.Replace(good, `"batchId":1`, "\x00\"batchId\":1", 1),
	}
	for name, line := range bad {
		t.Run(name, func(t *testing.T) {
			path := newLogPath(t)
			content := good + "\n" + line + "\n" + good + "\n"
			writeLog(t, path, content)
			_, err := openSendLog(path)
			if err == nil || !strings.Contains(err.Error(), path) || !strings.Contains(err.Error(), "line 2") {
				t.Fatalf("err = %v, want one naming %s and line 2", err, path)
			}
			if got := readLog(t, path); got != content {
				t.Fatal("the file changed")
			}
		})
	}

	t.Run("a damaged last line with a newline is not torn", func(t *testing.T) {
		path := newLogPath(t)
		writeLog(t, path, good+"\n"+good+"\nnot json\n")
		if _, err := openSendLog(path); err == nil || !strings.Contains(err.Error(), "line 3") {
			t.Fatalf("err = %v, want line 3", err)
		}
	})

	t.Run("the rotated file", func(t *testing.T) {
		path := newLogPath(t)
		writeLog(t, path+".1", "not json\n")
		_, err := openSendLog(path)
		if err == nil || !strings.Contains(err.Error(), path+".1") || !strings.Contains(err.Error(), "line 1") {
			t.Fatalf("err = %v, want one naming %s.1 and line 1", err, path)
		}
		if strings.Contains(err.Error(), "forgets any send") {
			t.Fatalf("err = %v: the rotated file holds no pending send", err)
		}
		if _, statErr := os.Stat(path); !os.IsNotExist(statErr) {
			t.Fatalf("the current file exists after a failed open: %v", statErr)
		}
	})
}

func TestSendLogRotation(t *testing.T) {
	path := newLogPath(t)
	l := mustOpenLog(t, path)
	l.limit = 1
	writeLog(t, path+".1", "old\n")

	a, b := logRec(1, 1, 0x01), logRec(1, 2, 0x02)
	accA := logResolved(a, OutcomeAccepted)
	for _, err := range []error{l.Sent(a), l.Resolved(accA), l.Sent(b)} {
		if err != nil {
			t.Fatal(err)
		}
	}
	if got, want := readLog(t, path+".1"), logLines(a, accA); got != want {
		t.Fatalf(".1 = %q, want %q", got, want)
	}
	if got, want := readLog(t, path), logLines(b); got != want {
		t.Fatalf("file = %q, want %q", got, want)
	}
	l.Close()

	l2 := mustOpenLog(t, path)
	if got, _ := l2.Last(); got != b {
		t.Fatalf("Last = %+v, want %+v unresolved", got, b)
	}
	l2.Close()

	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	l3 := mustOpenLog(t, path)
	if got, _ := l3.Last(); got != accA {
		t.Fatalf("Last = %+v, want %+v", got, accA)
	}
	if fi, err := os.Stat(path); err != nil || fi.Size() != 0 || fi.Mode().Perm() != 0o600 {
		t.Fatalf("current file = %v, %v; want empty with mode 0600", fi, err)
	}
}

func TestSendLogFailureIsSticky(t *testing.T) {
	t.Run("write", func(t *testing.T) {
		path := newLogPath(t)
		l := mustOpenLog(t, path)
		c := logRec(1, 1, 0x03)
		if err := l.Sent(c); err != nil {
			t.Fatal(err)
		}
		before := readLog(t, path)
		l.f.Close()

		err := l.Sent(logRec(1, 2, 0x01))
		if err == nil || !strings.Contains(err.Error(), path) {
			t.Fatalf("Sent err = %v, want one naming %s", err, path)
		}
		if got, _ := l.Last(); got != c {
			t.Fatalf("Last = %+v after a failed Sent, want %+v", got, c)
		}

		f, ferr := os.OpenFile(path, os.O_RDWR|os.O_APPEND, 0o600)
		if ferr != nil {
			t.Fatal(ferr)
		}
		defer f.Close()
		l.f = f
		if err2 := l.Sent(logRec(1, 3, 0x02)); err2 == nil || err2.Error() != err.Error() {
			t.Fatalf("later Sent err = %v, want %v", err2, err)
		}
		if got := readLog(t, path); got != before {
			t.Fatalf("file = %q, want unchanged %q", got, before)
		}

		acc := logResolved(c, OutcomeAccepted)
		if err3 := l.Resolved(acc); err3 == nil {
			t.Fatal("Resolved returned no error")
		}
		if got, _ := l.Last(); got != acc {
			t.Fatalf("Last = %+v, want the outcome recorded in memory: %+v", got, acc)
		}
		if got := readLog(t, path); got != before {
			t.Fatal("Resolved wrote after a failure")
		}
	})

	t.Run("rotation", func(t *testing.T) {
		path := newLogPath(t)
		l := mustOpenLog(t, path)
		a := logRec(1, 1, 0x01)
		if err := l.Sent(a); err != nil {
			t.Fatal(err)
		}
		if err := l.Resolved(logResolved(a, OutcomeAccepted)); err != nil {
			t.Fatal(err)
		}
		before := readLog(t, path)
		l.limit = 1
		if err := os.MkdirAll(path+".1", 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(path+".1", "keep"), []byte("x"), 0o600); err != nil {
			t.Fatal(err)
		}

		err := l.Sent(logRec(1, 2, 0x02))
		if err == nil || !strings.Contains(err.Error(), "rotating") {
			t.Fatalf("Sent err = %v, want a rotation error", err)
		}
		if got := readLog(t, path); got != before {
			t.Fatal("the current file changed")
		}
		if err := l.Sent(logRec(1, 3, 0x03)); err == nil {
			t.Fatal("a later Sent succeeded")
		}
	})
}

func TestSendLogUnreadableRecords(t *testing.T) {
	prefix := logLines(logRec(1, 3, 0x01), logResolved(logRec(1, 3, 0x01), OutcomeAccepted))
	h := types.Hash{0x02}
	u := fmt.Sprintf(`{"version":2,"domainId":2,"batchId":3,"hash":"%s","extra":1}`, h)
	open := func(t *testing.T, tail string) (*fileLog, string) {
		t.Helper()
		path := newLogPath(t)
		writeLog(t, path, prefix+tail)
		return mustOpenLog(t, path), path
	}
	want1 := map[uint32]uint64{1: 3}
	env := SendRecord{DomainID: 2, BatchID: 3, Hash: h}

	t.Run("another version", func(t *testing.T) {
		l, path := open(t, u+"\n")
		if got, _ := l.Last(); got != env {
			t.Fatalf("Last = %+v, want %+v", got, env)
		}
		for _, s := range []string{path, "line 3", "log-format version 2 is not 1"} {
			if l.unreadable == nil || !strings.Contains(l.unreadable.Error(), s) {
				t.Fatalf("unreadable = %v, want %q", l.unreadable, s)
			}
		}
		if !reflect.DeepEqual(l.accepted, want1) {
			t.Fatalf("accepted = %v, want %v", l.accepted, want1)
		}
	})

	good := logRec(2, 3, 0x02)
	base := logRecLine(good)
	fields := []struct{ name, from, to, why string }{
		{"method", `"method":` + fmt.Sprintf("%q", good.Method), `"method":"Other"`, "method"},
		{"height", `"accountHeight":7`, `"accountHeight":0`, "account height 0"},
		{"data hash", `,"dataHash":"` + good.DataHash.String() + `"`, "", "no data hash"},
		{"bad type", `"accountHeight":7`, `"accountHeight":"x"`, "accountHeight"},
	}
	for _, f := range fields {
		t.Run(f.name, func(t *testing.T) {
			line := strings.Replace(base, f.from, f.to, 1)
			if line == base {
				t.Fatal("the test line was not changed")
			}
			l, _ := open(t, line+"\n")
			if got, _ := l.Last(); got != env {
				t.Fatalf("Last = %+v, want %+v", got, env)
			}
			if l.unreadable == nil || !strings.Contains(l.unreadable.Error(), f.why) {
				t.Fatalf("unreadable = %v, want %q", l.unreadable, f.why)
			}
		})
	}

	t.Run("an unreadable send then its unreadable outcome", func(t *testing.T) {
		l, _ := open(t, u+"\n"+strings.Replace(u, "}", `,"outcome":"accepted"}`, 1)+"\n")
		if got, _ := l.Last(); got.Outcome != OutcomeAccepted || l.unreadable != nil {
			t.Fatalf("Last = %+v, unreadable = %v; want accepted and nil", got, l.unreadable)
		}
		if !reflect.DeepEqual(l.accepted, want1) {
			t.Fatalf("accepted = %v, want %v", l.accepted, want1)
		}
	})

	t.Run("an unreadable outcome line does not seed", func(t *testing.T) {
		acc := strings.Replace(logRecLine(logResolved(good, OutcomeAccepted)), `"method":`+fmt.Sprintf("%q", good.Method), `"method":"Other"`, 1)
		l, _ := open(t, base+"\n"+acc+"\n")
		if got, _ := l.Last(); got.Outcome != OutcomeAccepted || l.unreadable != nil {
			t.Fatalf("Last = %+v, unreadable = %v; want accepted and nil", got, l.unreadable)
		}
		if !reflect.DeepEqual(l.accepted, want1) {
			t.Fatalf("accepted = %v, want %v", l.accepted, want1)
		}
	})

	t.Run("a later readable send clears the reason", func(t *testing.T) {
		later := logRec(2, 4, 0x05)
		l, _ := open(t, u+"\n"+logRecLine(later)+"\n")
		if got, _ := l.Last(); got != later || l.unreadable != nil {
			t.Fatalf("Last = %+v, unreadable = %v; want %+v and nil", got, l.unreadable, later)
		}
	})

	t.Run("unknown fields are ignored", func(t *testing.T) {
		l, _ := open(t, strings.Replace(base, "}", `,"commitment":"00ff"}`, 1)+"\n")
		if got, _ := l.Last(); got != good || l.unreadable != nil {
			t.Fatalf("Last = %+v, unreadable = %v; want %+v and nil", got, l.unreadable, good)
		}
	})

	t.Run("the abandoned line resolves an unreadable send", func(t *testing.T) {
		l, path := open(t, u+"\n")
		ab := SendRecord{DomainID: 2, BatchID: 3, Hash: h, Outcome: OutcomeAbandoned}
		if err := l.Resolved(ab); err != nil {
			t.Fatal(err)
		}
		l.Close()
		l2 := mustOpenLog(t, path)
		if got, _ := l2.Last(); got.Outcome != OutcomeAbandoned || l2.unreadable != nil {
			t.Fatalf("Last = %+v, unreadable = %v; want abandoned and nil", got, l2.unreadable)
		}
	})
}

func TestSendLogRotationBoundary(t *testing.T) {
	a, b, c := logRec(1, 1, 0x01), logRec(1, 2, 0x02), logRec(1, 3, 0x03)

	t.Run("a log exactly at the limit rotates", func(t *testing.T) {
		path := newLogPath(t)
		l := mustOpenLog(t, path)
		if err := l.Sent(a); err != nil {
			t.Fatal(err)
		}
		l.limit = l.size
		if err := l.Sent(b); err != nil {
			t.Fatal(err)
		}
		if got, want := readLog(t, path+".1"), logLines(a); got != want {
			t.Fatalf(".1 = %q, want %q", got, want)
		}
	})

	t.Run("a log below the limit does not rotate", func(t *testing.T) {
		path := newLogPath(t)
		l := mustOpenLog(t, path)
		if err := l.Sent(a); err != nil {
			t.Fatal(err)
		}
		l.limit = l.size + 1
		if err := l.Sent(b); err != nil {
			t.Fatal(err)
		}
		if _, err := os.Stat(path + ".1"); !os.IsNotExist(err) {
			t.Fatalf("rotated below the limit: %v", err)
		}
	})

	t.Run("the new file has mode 0600 and counts only its own bytes", func(t *testing.T) {
		path := newLogPath(t)
		l := mustOpenLog(t, path)
		if err := l.Sent(a); err != nil {
			t.Fatal(err)
		}
		l.limit = l.size
		if err := l.Sent(b); err != nil {
			t.Fatal(err)
		}
		fi, err := os.Stat(path)
		if err != nil || fi.Mode().Perm() != 0o600 {
			t.Fatalf("rotated file = %v, %v; want mode 0600", fi, err)
		}
		if want := int64(len(logLines(b))); l.size != want || fi.Size() != want {
			t.Fatalf("size = %d, file size = %d, want %d", l.size, fi.Size(), want)
		}
		// b's line is as long as a's, so a size that kept the old bytes would rotate again.
		l.limit = l.size + 1
		if err := l.Sent(c); err != nil {
			t.Fatal(err)
		}
		if got, want := readLog(t, path+".1"), logLines(a); got != want {
			t.Fatalf(".1 = %q, want %q", got, want)
		}
	})

	t.Run("a reopened log counts the bytes already in the file", func(t *testing.T) {
		path := newLogPath(t)
		writeLog(t, path, logLines(a))
		l := mustOpenLog(t, path)
		l.limit = 1
		if err := l.Sent(b); err != nil {
			t.Fatal(err)
		}
		if got, want := readLog(t, path+".1"), logLines(a); got != want {
			t.Fatalf(".1 = %q, want %q", got, want)
		}
		if got, want := readLog(t, path), logLines(b); got != want {
			t.Fatalf("file = %q, want %q", got, want)
		}
	})

	t.Run("a rotation failure stays after its cause is gone", func(t *testing.T) {
		path := newLogPath(t)
		l := mustOpenLog(t, path)
		if err := l.Sent(a); err != nil {
			t.Fatal(err)
		}
		l.limit = 1
		if err := os.MkdirAll(filepath.Join(path+".1", "keep"), 0o700); err != nil {
			t.Fatal(err)
		}
		first := l.Sent(b)
		if first == nil {
			t.Fatal("Sent succeeded with a directory at .1")
		}
		if err := os.RemoveAll(path + ".1"); err != nil {
			t.Fatal(err)
		}
		if err := l.Sent(c); err == nil || err.Error() != first.Error() {
			t.Fatalf("Sent err = %v, want the first error %v", err, first)
		}
		if got, want := readLog(t, path), logLines(a); got != want {
			t.Fatalf("file = %q, want only a", got)
		}
	})
}

func TestSendLogRotatedFileTail(t *testing.T) {
	a := logRec(1, 1, 0x01)
	acc := logResolved(a, OutcomeAccepted)
	path := newLogPath(t)
	writeLog(t, path+".1", logLines(a, acc)+`{"version":1,"domainId":1,"meth`)
	l := mustOpenLog(t, path)
	if want := map[uint32]uint64{1: 1}; !reflect.DeepEqual(l.accepted, want) {
		t.Fatalf("accepted = %v, want %v", l.accepted, want)
	}
}

// A directory that cannot be read cannot be fsynced, so these tests cover the directory sync
// failure paths. They need a user that the mode bits bind.
func TestSendLogDirectorySyncFailure(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("the mode bits do not bind root")
	}
	a, b := logRec(1, 1, 0x01), logRec(1, 2, 0x02)

	t.Run("open", func(t *testing.T) {
		dir := t.TempDir()
		if err := os.Chmod(dir, 0o300); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { os.Chmod(dir, 0o700) })
		path := filepath.Join(dir, "x.submissions.jsonl")
		if _, err := openSendLog(path); err == nil || !strings.Contains(err.Error(), path) {
			t.Fatalf("openSendLog err = %v, want one naming %s", err, path)
		}
	})

	t.Run("rotation", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "x.submissions.jsonl")
		l := mustOpenLog(t, path)
		if err := l.Sent(a); err != nil {
			t.Fatal(err)
		}
		l.limit = 1
		if err := os.Chmod(dir, 0o300); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { os.Chmod(dir, 0o700) })
		err := l.Sent(b)
		if err == nil || !strings.Contains(err.Error(), "rotating") {
			t.Fatalf("Sent err = %v, want a rotation error", err)
		}
		if err2 := l.Sent(logRec(1, 3, 0x03)); err2 == nil || err2.Error() != err.Error() {
			t.Fatalf("later Sent err = %v, want %v", err2, err)
		}
		if last, _ := l.Last(); last != a {
			t.Fatalf("Last = %+v, want %+v", last, a)
		}
	})
}
