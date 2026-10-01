package supervisor

import (
	"time"

	"github.com/digitalSloth/go-executor/internal/ipc"
	"github.com/digitalSloth/go-executor/internal/workerproc"
)

// Policy is the restart schedule. A field <= 0 takes its default, so the zero Policy is the
// production one and a test can shrink it.
type Policy struct {
	BackoffMin     time.Duration // default 1s
	BackoffMax     time.Duration // default 60s
	CrashLoopLimit int           // default 5
	StableAfter    time.Duration // default 10m
	Liveness       time.Duration // default 10m; read by the supervision loop, not by Next
	StopGrace      time.Duration // default 30s; read by the supervision loop, not by Next
}

// WithDefaults returns p with every field <= 0 replaced by its default, following
// recovery.RetryPolicy.WithDefaults (internal/recovery/boot.go:78-93).
func (p Policy) WithDefaults() Policy {
	if p.BackoffMin <= 0 {
		p.BackoffMin = time.Second
	}
	if p.BackoffMax <= 0 {
		p.BackoffMax = 60 * time.Second
	}
	if p.CrashLoopLimit <= 0 {
		p.CrashLoopLimit = 5
	}
	if p.StableAfter <= 0 {
		p.StableAfter = 10 * time.Minute
	}
	if p.Liveness <= 0 {
		p.Liveness = 10 * time.Minute
	}
	if p.StopGrace <= 0 {
		p.StopGrace = 30 * time.Second
	}
	return p
}

// Action is what the supervision goroutine does after one worker run ends.
type Action int

const (
	ActionRestart     Action = iota + 1 // BACKOFF: wait, then start again
	ActionHalt                          // exit 3: HALTED, never restarted
	ActionConfigError                   // exit 78: CONFIG_ERROR, never restarted
	ActionCrashLoop                     // CrashLoopLimit counted failures without progress: HALTED, "crash loop"
)

// RunEnd is how one worker run ended. The caller never builds one for a run ended by shutdown.
type RunEnd struct {
	StartFailed bool // Host.Start failed; Exit, Ran and Progressed are ignored
	Exit        workerproc.Exit
	Ran         time.Duration // start to exit
	Progressed  bool          // workerState.progressed when the run ended
}

// Backoff is the restart state carried from one run to the next. The zero value is a fresh
// worker.
type Backoff struct {
	Failures int           // consecutive counted failures without progress
	Delay    time.Duration // wait before the next restart; clamped to [BackoffMin, BackoffMax]
}

// Next is pure: no clock, no I/O. wait is non-zero only for ActionRestart.
func (p Policy) Next(b Backoff, end RunEnd) (action Action, wait time.Duration, next Backoff) {
	p = p.WithDefaults()

	d := b.Delay
	if d < p.BackoffMin {
		d = p.BackoffMin
	}
	if d > p.BackoffMax {
		d = p.BackoffMax
	}

	// Progress resets the failure count and the delay: a run that committed past its high-water
	// mark, or simply stayed up long enough, earns the worker a fresh backoff schedule.
	if !end.StartFailed && (end.Progressed || end.Ran >= p.StableAfter) {
		b.Failures = 0
		d = min(p.BackoffMin, p.BackoffMax)
	}

	if !end.StartFailed {
		switch end.Exit.Code {
		case ipc.ExitHalted:
			return ActionHalt, 0, Backoff{Failures: b.Failures, Delay: d}
		case ipc.ExitConfig:
			return ActionConfigError, 0, Backoff{Failures: b.Failures, Delay: d}
		case ipc.ExitTempFail:
			return ActionRestart, d, Backoff{Failures: b.Failures, Delay: double(d, p)}
		}
	}

	// Every other end (a start failure, exit 0, 1, 2, any other code, any signal, including a
	// signal death reported as Code -1) is a counted failure.
	f := b.Failures + 1
	if f >= p.CrashLoopLimit {
		return ActionCrashLoop, 0, Backoff{Failures: f, Delay: d}
	}
	return ActionRestart, d, Backoff{Failures: f, Delay: double(d, p)}
}

// double returns the next restart delay after d: capped at p.BackoffMax rather than doubled past
// it, so the schedule cannot overflow.
func double(d time.Duration, p Policy) time.Duration {
	if d > p.BackoffMax/2 {
		return p.BackoffMax
	}
	return 2 * d
}
