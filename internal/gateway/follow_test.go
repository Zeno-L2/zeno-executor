package gateway

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Zeno-L2/zeno-executor/internal/recovery"
)

// flakySettlement fails ConfirmedHeight on chosen calls.
type flakySettlement struct {
	*fakeSettlement
	calls  atomic.Int64
	err    error
	cancel context.CancelFunc
}

func (f *flakySettlement) ConfirmedHeight() (uint64, error) {
	switch f.calls.Add(1) {
	case 1, 2, 4:
		return 0, f.err
	case 5:
		f.cancel()
		return 0, f.err
	}
	return f.fakeSettlement.ConfirmedHeight()
}

const nodeURL = "http://node.example:35997"

func TestFollowLogsOnceRedacted(t *testing.T) {
	const raw = "http://user:secret@node.example:35997/rpc?key=k"
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	fake := &flakySettlement{
		fakeSettlement: newFakeSettlement(),
		err:            &url.Error{Op: "Post", URL: raw, Err: errors.New("connection refused")},
		cancel:         cancel,
	}
	r := newTestChain(t, t.TempDir(), fake.fakeSettlement)
	r.c.cfg.Settlement = fake

	follow(ctx, []*Chain{r.c}, raw, time.Millisecond, pollSlice, r.log.logf)
	if n := r.log.count("poll failed"); n != 2 {
		t.Fatalf("poll failed lines = %d, want 2:\n%s", n, r.log.dump())
	}
	for _, l := range strings.Split(r.log.dump(), "\n") {
		if !strings.Contains(l, nodeURL) || strings.Contains(l, "secret") || strings.Contains(l, "key=k") {
			t.Fatalf("line %q is not redacted", l)
		}
	}
}

func TestFollowQuotesPollError(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	fake := &flakySettlement{
		fakeSettlement: newFakeSettlement(),
		err:            errors.New("bad\ngateway: domain 9: forged line"),
		cancel:         cancel,
	}
	r := newTestChain(t, t.TempDir(), fake.fakeSettlement)
	r.c.cfg.Settlement = fake

	follow(ctx, []*Chain{r.c}, nodeURL, time.Millisecond, pollSlice, r.log.logf)
	if r.log.count("poll failed") == 0 {
		t.Fatalf("no poll failed line:\n%s", r.log.dump())
	}
	for _, l := range strings.Split(r.log.dump(), "\n") {
		if strings.HasPrefix(l, "gateway: domain 9") || !strings.HasPrefix(l, "gateway: domain ") {
			t.Fatalf("line %q was injected by the error text", l)
		}
	}
}

func TestWaitForNode(t *testing.T) {
	const raw = "http://user:secret@node.example:35997/x"
	checkLines := func(t *testing.T, lines []string) {
		t.Helper()
		for _, l := range lines {
			if !strings.Contains(l, nodeURL) || strings.Contains(l, "secret") {
				t.Fatalf("line %q is not redacted", l)
			}
		}
	}

	t.Run("retries until the check passes", func(t *testing.T) {
		sink := &logSink{}
		var calls atomic.Int64
		check := func() error {
			switch calls.Add(1) {
			case 1:
				return errors.New("down\nforged")
			case 2:
				return fmt.Errorf("%w: root mismatch", recovery.ErrHalted)
			case 3:
				return fmt.Errorf("get %s failed", raw)
			}
			return nil
		}
		if !waitForNode(context.Background(), raw, check, time.Millisecond, 2*time.Millisecond, time.Minute, sink.logf) {
			t.Fatal("waitForNode = false, want true")
		}
		if calls.Load() != 4 || sink.count("znnd node") != 3 {
			t.Fatalf("calls = %d, lines:\n%s", calls.Load(), sink.dump())
		}
		lines := strings.Split(sink.dump(), "\n")
		if !strings.Contains(lines[0], "is unusable") || !strings.Contains(lines[0], `down\nforged`) || strings.Contains(lines[0], "returned inconsistent data") {
			t.Fatalf("first line = %q", lines[0])
		}
		if !strings.Contains(lines[1], "returned inconsistent data") || strings.Contains(lines[1], "is unusable") {
			t.Fatalf("second line = %q", lines[1])
		}
		checkLines(t, lines)
	})

	t.Run("cancelled context", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		failing := func() error { return errors.New("down") }
		if waitForNode(ctx, raw, failing, time.Millisecond, 2*time.Millisecond, time.Minute, func(string, ...any) {}) {
			t.Fatal("waitForNode with a cancelled context = true, want false")
		}
	})

	t.Run("signal during an in-flight check", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		stuck := make(chan struct{})
		t.Cleanup(func() { close(stuck) })
		done := make(chan bool, 1)
		go func() {
			done <- waitForNode(ctx, raw, func() error { <-stuck; return nil }, time.Millisecond, 2*time.Millisecond, time.Minute, func(string, ...any) {})
		}()
		cancel()
		select {
		case ok := <-done:
			if ok {
				t.Fatal("waitForNode with an in-flight check = true, want false")
			}
		case <-time.After(5 * time.Second):
			t.Fatal("waitForNode did not return while a check was in flight")
		}
	})

	t.Run("stuck check", func(t *testing.T) {
		sink := &logSink{}
		stuck := make(chan struct{})
		t.Cleanup(func() { close(stuck) })
		var calls atomic.Int64
		check := func() error {
			if calls.Add(1) == 1 {
				<-stuck
			}
			return nil
		}
		if !waitForNode(context.Background(), raw, check, time.Millisecond, 2*time.Millisecond, 20*time.Millisecond, sink.logf) {
			t.Fatal("waitForNode = false, want true")
		}
		if sink.count("znnd node") == 0 || sink.count("znnd node") != sink.count("is unusable", "did not answer") {
			t.Fatalf("want only is-unusable did-not-answer lines:\n%s", sink.dump())
		}
		checkLines(t, strings.Split(sink.dump(), "\n"))
	})
}

func waitUntil(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// runFollow runs follow on a goroutine and returns a stop function that
// cancels it and requires it to return.
func runFollow(t *testing.T, chains []*Chain, slice time.Duration, sink *logSink) (ctx context.Context, cancel context.CancelFunc, wait func()) {
	t.Helper()
	ctx, cancel = context.WithCancel(context.Background())
	t.Cleanup(cancel)
	done := make(chan struct{})
	go func() {
		follow(ctx, chains, nodeURL, time.Millisecond, slice, sink.logf)
		close(done)
	}()
	return ctx, cancel, func() {
		t.Helper()
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			t.Fatal("follow did not return")
		}
	}
}

func TestFollowHungRead(t *testing.T) {
	pristine, cs := buildA(t)
	mk := func(t *testing.T) (*rig, *fakeSettlement) {
		dir := t.TempDir()
		copyDir(t, pristine, dir)
		f := newFakeSettlement()
		f.height = 10
		for i := 0; i <= 2; i++ {
			f.accept(cs[i], 100)
		}
		return newTestChain(t, dir, f), f
	}
	a, fakeA := mk(t)
	b, fakeB := mk(t)
	ha := newHangingSettlement(t, fakeA)
	a.c.cfg.Settlement = ha
	requireTags(t, doPoll(t, a), 4, 2, -1, StatusOK)
	requireTags(t, doPoll(t, b), 4, 2, -1, StatusOK)
	// The id changes only after the warm-up poll, because a pending
	// record's commitment names domain 1.
	b.c.cfg.DomainID = 2
	a.c.readTimeout = 20 * time.Millisecond

	pollHung := func(t *testing.T) {
		t.Helper()
		var err error
		finishesWithin(t, 2*time.Second, "Poll", func() {
			err = a.c.Poll(context.Background())
		})
		if !errors.Is(err, errNoAnswer) || !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("Poll = %v, want errNoAnswer wrapping DeadlineExceeded", err)
		}
		requireTags(t, a.c.Head(), 4, 2, -1, StatusOK)
	}

	t.Run("height read never answers", func(t *testing.T) {
		ha.hangHeight.Store(true)
		pollHung(t)
	})
	t.Run("view read never answers", func(t *testing.T) {
		ha.hangHeight.Store(false)
		ha.hangView.Store(true)
		pollHung(t)
	})

	sink := &logSink{}
	t.Run("the next turn retries and other domains run", func(t *testing.T) {
		ha.hangView.Store(false)
		ha.hangHeight.Store(true)
		fakeB.accept(cs[3], 100)
		fakeB.accept(cs[4], 100)
		start := ha.calls.Load()
		_, cancel, wait := runFollow(t, []*Chain{a.c, b.c}, pollSlice, sink)
		waitUntil(t, "retries and the other domain", func() bool {
			h := b.c.Head().Latest
			return ha.calls.Load() >= start+3 && h != nil && h.BatchID == 4
		})
		cancel()
		wait()
		requireTags(t, a.c.Head(), 4, 2, -1, StatusOK)
		if sink.count("domain 1: poll failed") != 1 || sink.count("domain 1: poll failed", "did not answer") != 1 {
			t.Fatalf("want one did-not-answer line for domain 1:\n%s", sink.dump())
		}
		if sink.count("domain 2: poll failed") != 0 || sink.count("used its whole") != 0 {
			t.Fatalf("unexpected lines:\n%s", sink.dump())
		}
	})
	t.Run("recovers once the node answers", func(t *testing.T) {
		a.c.readTimeout = 5 * time.Second
		ha.hangHeight.Store(false)
		fakeA.accept(cs[3], 100)
		fakeA.accept(cs[4], 100)
		if err := a.c.Poll(context.Background()); err != nil {
			t.Fatalf("Poll: %v", err)
		}
		requireTags(t, a.c.Head(), 4, 4, -1, StatusOK)
	})
}

func TestFollowSliceEndsMidCatchUp(t *testing.T) {
	pristine, cs := buildA(t)
	dir := t.TempDir()
	copyDir(t, pristine, dir)
	f := newFakeSettlement()
	f.height = 10
	for i := 0; i <= 4; i++ {
		f.accept(cs[i], 100)
	}
	r := newTestChain(t, dir, f)
	hs := newHangingSettlement(t, f)
	hs.entryDelay = 50 * time.Millisecond
	r.c.cfg.Settlement = hs

	_, cancel, wait := runFollow(t, []*Chain{r.c}, 200*time.Millisecond, r.log)
	waitUntil(t, "latest 4", func() bool {
		h := r.c.Head().Latest
		return h != nil && h.BatchID == 4
	})
	cancel()
	wait()
	if r.log.count("poll failed") != 0 || r.log.count("used its whole") > 1 {
		t.Fatalf("unexpected lines:\n%s", r.log.dump())
	}
}

func TestFollowHangAfterPublish(t *testing.T) {
	pristine, cs := buildA(t)
	dir := t.TempDir()
	copyDir(t, pristine, dir)
	f := newFakeSettlement()
	f.height = 10
	f.accept(cs[0], 20)
	f.accept(cs[1], 100)
	r := newTestChain(t, dir, f)
	hs := newHangingSettlement(t, f)
	r.c.cfg.Settlement = hs
	requireTags(t, doPoll(t, r), 4, 1, -1, StatusOK)

	f.height = 20
	f.accept(cs[2], 100)
	hs.hangEntry.Store(0)
	var hangs atomic.Int64
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	hs.onEntryHang = func() {
		switch hangs.Add(1) {
		case 1:
			f.accept(cs[3], 100)
		case 2:
			f.accept(cs[4], 100)
		case 3:
			cancel()
		}
	}
	done := make(chan struct{})
	go func() {
		follow(ctx, []*Chain{r.c}, nodeURL, time.Millisecond, 200*time.Millisecond, r.log.logf)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("follow did not return")
	}
	if hs.entryHangs.Load() != 3 {
		t.Fatalf("entryHangs = %d, want 3", hs.entryHangs.Load())
	}
	requireTags(t, r.c.Head(), 4, 4, -1, StatusSyncing)
	if r.log.count("domain 1: the poll used its whole 200ms slice") != 1 || r.log.count("poll failed") != 0 {
		t.Fatalf("unexpected lines:\n%s", r.log.dump())
	}
}
