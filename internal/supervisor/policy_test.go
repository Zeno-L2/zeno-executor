package supervisor

import (
	"math"
	"syscall"
	"testing"
	"time"

	"github.com/Zeno-L2/zeno-executor/internal/ipc"
	"github.com/Zeno-L2/zeno-executor/internal/workerproc"
)

func TestPolicyWithDefaults(t *testing.T) {
	got := Policy{}.WithDefaults()
	want := Policy{
		BackoffMin:     time.Second,
		BackoffMax:     60 * time.Second,
		CrashLoopLimit: 5,
		StableAfter:    10 * time.Minute,
		Liveness:       10 * time.Minute,
		StopGrace:      30 * time.Second,
	}
	if got != want {
		t.Fatalf("WithDefaults() = %+v, want %+v", got, want)
	}

	set := Policy{BackoffMin: 5 * time.Millisecond}
	got = set.WithDefaults()
	if got.BackoffMin != 5*time.Millisecond {
		t.Fatalf("WithDefaults() kept BackoffMin = %v, want 5ms", got.BackoffMin)
	}
	if got.BackoffMax != want.BackoffMax || got.CrashLoopLimit != want.CrashLoopLimit {
		t.Fatalf("WithDefaults() changed an unset field: %+v", got)
	}
}

func exitEnd(code int) RunEnd { return RunEnd{Exit: workerproc.Exit{Code: code}} }

func TestPolicyNextExitOneCrashLoop(t *testing.T) {
	p := Policy{}
	b := Backoff{}
	wantWaits := []time.Duration{time.Second, 2 * time.Second, 4 * time.Second, 8 * time.Second}

	for i, want := range wantWaits {
		action, wait, next := p.Next(b, exitEnd(1))
		if action != ActionRestart {
			t.Fatalf("run %d: action = %v, want ActionRestart", i, action)
		}
		if wait != want {
			t.Fatalf("run %d: wait = %v, want %v", i, wait, want)
		}
		if next.Failures != i+1 {
			t.Fatalf("run %d: Failures = %d, want %d", i, next.Failures, i+1)
		}
		b = next
	}

	action, wait, _ := p.Next(b, exitEnd(1))
	if action != ActionCrashLoop {
		t.Fatalf("5th run: action = %v, want ActionCrashLoop", action)
	}
	if wait != 0 {
		t.Fatalf("5th run: wait = %v, want 0", wait)
	}
}

func TestPolicyNextDelayCaps(t *testing.T) {
	p := Policy{CrashLoopLimit: 1000}
	b := Backoff{}
	want := []time.Duration{
		time.Second, 2 * time.Second, 4 * time.Second, 8 * time.Second,
		16 * time.Second, 32 * time.Second, 60 * time.Second, 60 * time.Second,
	}
	for i, w := range want {
		action, wait, next := p.Next(b, exitEnd(1))
		if action != ActionRestart {
			t.Fatalf("run %d: action = %v", i, action)
		}
		if wait != w {
			t.Fatalf("run %d: wait = %v, want %v", i, wait, w)
		}
		b = next
	}
}

// TestPolicyNextProgressResets exercises the progress reset
// (Failures = 0, Delay = BackoffMin) against the exit-75
// (ipc.ExitTempFail) path, the only outcome whose next Backoff carries
// b.Failures straight through without the "+1" a counted failure always
// adds; see TestPolicyNextProgressResetThenCounts for a counted exit's
// result (Failures == 1) after the same reset.
func TestPolicyNextProgressResets(t *testing.T) {
	p := Policy{StableAfter: 10 * time.Minute}
	b := Backoff{Failures: 3, Delay: 32 * time.Second}

	t.Run("Progressed", func(t *testing.T) {
		end := RunEnd{Exit: workerproc.Exit{Code: ipc.ExitTempFail}, Progressed: true}
		_, wait, next := p.Next(b, end)
		if next.Failures != 0 {
			t.Fatalf("Failures = %d, want 0", next.Failures)
		}
		if wait != p.WithDefaults().BackoffMin {
			t.Fatalf("wait = %v, want BackoffMin", wait)
		}
	})

	t.Run("Ran == StableAfter", func(t *testing.T) {
		end := RunEnd{Exit: workerproc.Exit{Code: ipc.ExitTempFail}, Ran: p.StableAfter}
		_, wait, next := p.Next(b, end)
		if next.Failures != 0 {
			t.Fatalf("Failures = %d, want 0", next.Failures)
		}
		if wait != p.WithDefaults().BackoffMin {
			t.Fatalf("wait = %v, want BackoffMin", wait)
		}
	})

	t.Run("Ran == StableAfter - 1ns", func(t *testing.T) {
		end := RunEnd{Exit: workerproc.Exit{Code: ipc.ExitTempFail}, Ran: p.StableAfter - time.Nanosecond}
		_, _, next := p.Next(b, end)
		if next.Failures != b.Failures {
			t.Fatalf("Failures = %d, want %d (not reset)", next.Failures, b.Failures)
		}
	})
}

// TestPolicyNextProgressResetThenCounts shows that "progress resets
// Failures to 0" is not the whole story: a counted-failure exit (here,
// 1) after a progress reset still adds the counted-failure +1, landing
// at Failures == 1, not 0.
func TestPolicyNextProgressResetThenCounts(t *testing.T) {
	p := Policy{StableAfter: 10 * time.Minute}
	b := Backoff{Failures: 3, Delay: 32 * time.Second}
	end := RunEnd{Exit: workerproc.Exit{Code: 1}, Progressed: true}

	_, wait, next := p.Next(b, end)
	if next.Failures != 1 {
		t.Fatalf("Failures = %d, want 1 (reset to 0, then the counted-failure +1)", next.Failures)
	}
	if wait != p.WithDefaults().BackoffMin {
		t.Fatalf("wait = %v, want BackoffMin", wait)
	}
}

func TestPolicyNextHaltAndConfigError(t *testing.T) {
	for _, failures := range []int{0, 3, 100} {
		for _, delay := range []time.Duration{0, time.Second, 45 * time.Second} {
			b := Backoff{Failures: failures, Delay: delay}

			action, wait, _ := Policy{}.Next(b, exitEnd(ipc.ExitHalted))
			if action != ActionHalt || wait != 0 {
				t.Fatalf("halted: action=%v wait=%v, want ActionHalt/0", action, wait)
			}

			action, wait, _ = Policy{}.Next(b, exitEnd(ipc.ExitConfig))
			if action != ActionConfigError || wait != 0 {
				t.Fatalf("config: action=%v wait=%v, want ActionConfigError/0", action, wait)
			}
		}
	}
}

// TestPolicyNextInconsistentHalts: a consistency fault halts whatever exit the worker reached.
func TestPolicyNextInconsistentHalts(t *testing.T) {
	exits := map[string]workerproc.Exit{
		"temp fail": {Code: ipc.ExitTempFail},
		"exit 0":    {Code: 0},
		"sigkill":   {Code: -1, Signal: syscall.SIGKILL},
	}
	for name, exit := range exits {
		for _, progressed := range []bool{false, true} {
			end := RunEnd{Exit: exit, Progressed: progressed, Inconsistent: true}
			action, wait, _ := Policy{}.Next(Backoff{Failures: 2, Delay: time.Second}, end)
			if action != ActionHalt || wait != 0 {
				t.Fatalf("%s progressed=%v: action=%v wait=%v, want ActionHalt/0", name, progressed, action, wait)
			}
		}
	}
}

// TestPolicyNextExit75NeverCounts sends 100 consecutive exit-75 ends from each of two starting
// failure counts. Every one restarts, the wait grows and caps, and Failures never changes:
// exit 75 (ipc.ExitTempFail) is never a counted failure.
func TestPolicyNextExit75NeverCounts(t *testing.T) {
	p := Policy{}
	for _, start := range []int{0, p.WithDefaults().CrashLoopLimit - 1} {
		b := Backoff{Failures: start}
		prevWait := time.Duration(0)
		for i := 0; i < 100; i++ {
			action, wait, next := p.Next(b, exitEnd(ipc.ExitTempFail))
			if action != ActionRestart {
				t.Fatalf("start=%d run %d: action = %v, want ActionRestart", start, i, action)
			}
			if next.Failures != start {
				t.Fatalf("start=%d run %d: Failures = %d, want %d", start, i, next.Failures, start)
			}
			if wait < prevWait {
				t.Fatalf("start=%d run %d: wait %v less than previous %v", start, i, wait, prevWait)
			}
			if wait > p.WithDefaults().BackoffMax {
				t.Fatalf("start=%d run %d: wait %v exceeds BackoffMax", start, i, wait)
			}
			prevWait = wait
			b = next
		}
	}
}

// TestPolicyNextCountedFailures exercises every end that increments Failures by exactly one from
// a fresh Backoff, including a StartFailed run whose Progressed is true and ignored.
func TestPolicyNextCountedFailures(t *testing.T) {
	p := Policy{}
	cases := []struct {
		name string
		end  RunEnd
	}{
		{"exit 0", exitEnd(0)},
		{"exit 2", exitEnd(2)},
		{"signal SIGKILL", RunEnd{Exit: workerproc.Exit{Code: -1, Signal: syscall.SIGKILL}}},
		{"OOMKilled", RunEnd{Exit: workerproc.Exit{Code: -1, Signal: syscall.SIGKILL, OOMKilled: true}}},
		{"StartFailed with Progressed true", RunEnd{StartFailed: true, Progressed: true}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			action, _, next := p.Next(Backoff{}, tc.end)
			if action != ActionRestart {
				t.Fatalf("action = %v, want ActionRestart", action)
			}
			if next.Failures != 1 {
				t.Fatalf("Failures = %d, want 1", next.Failures)
			}
		})
	}
}

// TestPolicyNextStartFailedIgnoresProgress starts each case from a nonzero Failures count, so a
// reset-then-increment (StartFailed correctly ignores Progressed and Ran) and a
// no-reset-then-increment land on different values: TestPolicyNextCountedFailures's StartFailed
// row starts from Backoff{}, where both paths give the same Failures and cannot tell them apart.
func TestPolicyNextStartFailedIgnoresProgress(t *testing.T) {
	end := RunEnd{StartFailed: true, Progressed: true, Ran: 10 * time.Minute, Exit: workerproc.Exit{Code: ipc.ExitTempFail}}

	t.Run("Failures 3 restarts at 4", func(t *testing.T) {
		action, wait, next := Policy{}.Next(Backoff{Failures: 3, Delay: 8 * time.Second}, end)
		if action != ActionRestart {
			t.Fatalf("action = %v, want ActionRestart", action)
		}
		if wait != 8*time.Second {
			t.Fatalf("wait = %v, want 8s", wait)
		}
		want := Backoff{Failures: 4, Delay: 16 * time.Second}
		if next != want {
			t.Fatalf("next = %+v, want %+v", next, want)
		}
	})

	t.Run("Failures 4 crash loops at 5", func(t *testing.T) {
		action, _, _ := Policy{}.Next(Backoff{Failures: 4}, end)
		if action != ActionCrashLoop {
			t.Fatalf("action = %v, want ActionCrashLoop", action)
		}
	})
}

// TestPolicyNextDelayClamps exercises the two Backoff.Delay clamps Next applies before touching
// Failures: the upper clamp (a stale Delay above BackoffMax is lowered before use), and the
// progress-reset delay using min(BackoffMin, BackoffMax) rather than BackoffMin unconditionally,
// which matters when a caller sets BackoffMin above BackoffMax.
func TestPolicyNextDelayClamps(t *testing.T) {
	t.Run("stale Delay above BackoffMax is lowered", func(t *testing.T) {
		_, wait, _ := Policy{}.Next(Backoff{Delay: 5 * time.Minute}, exitEnd(1))
		if wait != 60*time.Second {
			t.Fatalf("wait = %v, want 60s (BackoffMax)", wait)
		}
	})

	t.Run("progress reset uses min(BackoffMin, BackoffMax)", func(t *testing.T) {
		p := Policy{BackoffMin: 2 * time.Minute, BackoffMax: time.Minute}
		_, wait, _ := p.Next(Backoff{}, RunEnd{Exit: workerproc.Exit{Code: 1}, Progressed: true})
		if wait != time.Minute {
			t.Fatalf("wait = %v, want 1m (min(BackoffMin, BackoffMax))", wait)
		}
	})
}

// TestPolicyNextNoOverflowAtExtremeBackoffMax confirms double(d, p) cannot overflow time.Duration
// even when BackoffMax is near its max value and Failures grows across many runs: wait is always
// non-negative and never exceeds BackoffMax.
func TestPolicyNextNoOverflowAtExtremeBackoffMax(t *testing.T) {
	p := Policy{BackoffMin: time.Second, BackoffMax: math.MaxInt64 - 1, CrashLoopLimit: 1000000}
	b := Backoff{}
	for i := 0; i < 200; i++ {
		action, wait, next := p.Next(b, exitEnd(1))
		if action != ActionRestart {
			t.Fatalf("run %d: action = %v, want ActionRestart", i, action)
		}
		if wait < 0 {
			t.Fatalf("run %d: wait = %v, want non-negative (overflow)", i, wait)
		}
		if wait > p.BackoffMax {
			t.Fatalf("run %d: wait = %v, exceeds BackoffMax %v", i, wait, p.BackoffMax)
		}
		if next.Delay < 0 {
			t.Fatalf("run %d: next.Delay = %v, want non-negative (overflow)", i, next.Delay)
		}
		b = next
	}
}

// TestPolicyNextSupervisionShape exercises a short, millisecond-scale policy shape a caller
// driving an actual supervision loop would use, rather than the second-scale default.
func TestPolicyNextSupervisionShape(t *testing.T) {
	p := Policy{BackoffMin: 10 * time.Millisecond, BackoffMax: 50 * time.Millisecond, CrashLoopLimit: 3}
	b := Backoff{}

	action, wait, next := p.Next(b, exitEnd(1))
	if action != ActionRestart || wait != 10*time.Millisecond {
		t.Fatalf("run 1: action=%v wait=%v", action, wait)
	}
	b = next

	action, wait, next = p.Next(b, exitEnd(1))
	if action != ActionRestart || wait != 20*time.Millisecond {
		t.Fatalf("run 2: action=%v wait=%v", action, wait)
	}
	b = next

	action, wait, _ = p.Next(b, exitEnd(1))
	if action != ActionCrashLoop || wait != 0 {
		t.Fatalf("run 3: action=%v wait=%v, want ActionCrashLoop/0", action, wait)
	}
}
