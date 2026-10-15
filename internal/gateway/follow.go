package gateway

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/digitalSloth/go-executor/internal/recovery"
)

const (
	// followInterval is the wait between two rounds of polls.
	followInterval = 2 * time.Second
	// pollSlice bounds one engine's Poll within a round, so one
	// domain's catch-up cannot hold up the others.
	pollSlice = 30 * time.Second
	// nodeBackoffMin and nodeBackoffMax bound the wait between two
	// node checks at boot.
	nodeBackoffMin = time.Second
	nodeBackoffMax = 60 * time.Second
)

// Follow polls every engine, one at a time in order, until ctx ends.
// It logs each poll error through logf with the znnd endpoint
// redacted, once until the message changes or a poll succeeds. A
// poll cut off by its slice after publishing a batch logs a fixed
// line in place of its error, deduplicated the same way.
func Follow(ctx context.Context, chains []*Chain, endpoint string, logf func(format string, args ...any)) {
	follow(ctx, chains, endpoint, followInterval, pollSlice, logf)
}

// follow is Follow with the wait between rounds and each poll's slice
// as parameters.
func follow(ctx context.Context, chains []*Chain, endpoint string, interval, slice time.Duration, logf func(format string, args ...any)) {
	last := make([]string, len(chains))
	for {
		for i, c := range chains {
			if ctx.Err() != nil {
				return
			}
			before := c.Head()
			pctx, cancel := context.WithTimeout(ctx, slice)
			err := c.Poll(pctx)
			// The slice is read from pctx, never from err: an HTTP
			// client timeout also satisfies context.DeadlineExceeded.
			sliceEnded := pctx.Err() != nil
			cancel()
			if err == nil {
				last[i] = ""
				continue
			}
			if ctx.Err() != nil {
				return
			}
			var msg string
			if sliceEnded && headChanged(before, c.Head()) {
				msg = fmt.Sprintf("the poll used its whole %s slice; resuming next round", slice)
			} else {
				msg = fmt.Sprintf("poll failed: %q; retrying next round", RedactEndpointErr(endpoint, err))
			}
			if msg != last[i] {
				logf("gateway: domain %d: %s", c.cfg.DomainID, msg)
				last[i] = msg
			}
		}
		timer := time.NewTimer(interval)
		select {
		case <-timer.C:
		case <-ctx.Done():
			timer.Stop()
			return
		}
	}
}

// headChanged reports whether any of the three tags differs between
// two heads.
func headChanged(a, b Head) bool {
	differ := func(x, y *BatchHead) bool {
		if x == nil || y == nil {
			return x != y
		}
		return *x != *y
	}
	return differ(a.Pending, b.Pending) || differ(a.Latest, b.Latest) || differ(a.Finalized, b.Finalized)
}

// WaitForNode runs check until it returns nil (true) or ctx ends
// (false), backing off from one second to one minute and logging each
// failure with the endpoint redacted. A check that does not return
// within 30 s counts as a failure. check runs on its own goroutine,
// so a signal ends the wait without waiting for an in-flight request.
func WaitForNode(ctx context.Context, endpoint string, check func() error, logf func(format string, args ...any)) bool {
	return waitForNode(ctx, endpoint, check, nodeBackoffMin, nodeBackoffMax, readTimeout, logf)
}

// waitForNode is WaitForNode with the backoff bounds and the check
// deadline as parameters.
func waitForNode(ctx context.Context, endpoint string, check func() error, minDelay, maxDelay, timeout time.Duration, logf func(format string, args ...any)) bool {
	delay := minDelay
	for {
		res := make(chan error, 1)
		go func() { res <- check() }()
		cctx, cancel := context.WithTimeout(ctx, timeout)
		var err error
		select {
		case err = <-res:
			cancel()
		case <-cctx.Done():
			if ctx.Err() != nil {
				cancel()
				return false
			}
			err = fmt.Errorf("%w: %w", errNoAnswer, cctx.Err())
			cancel()
		}
		if err == nil {
			return true
		}

		// The error text comes from the node, so %q escapes any
		// control characters in it.
		if errors.Is(err, recovery.ErrHalted) {
			logf("gateway: znnd node at %s returned inconsistent data: %q; retrying in %s", redactEndpoint(endpoint), RedactEndpointErr(endpoint, err), delay)
		} else {
			logf("gateway: znnd node at %s is unusable: %q; retrying in %s", redactEndpoint(endpoint), RedactEndpointErr(endpoint, err), delay)
		}

		timer := time.NewTimer(delay)
		select {
		case <-timer.C:
		case <-ctx.Done():
			timer.Stop()
			return false
		}
		if delay > maxDelay/2 {
			delay = maxDelay
		} else {
			delay *= 2
		}
	}
}

// RedactEndpointErr returns err's text with the znnd endpoint raw, and
// the request URL of a failed call to it, replaced by the endpoint's
// scheme and host, so userinfo, path and query never reach a log. The
// result is cut to maxLoggedErr bytes with a truncation marker, so a
// node cannot flood the log with long error text.
func RedactEndpointErr(raw string, err error) string {
	msg := err.Error()
	red := redactEndpoint(raw)
	var uerr *url.Error
	if errors.As(err, &uerr) && uerr.URL != "" {
		msg = strings.ReplaceAll(msg, uerr.URL, red)
	}
	if raw != "" {
		msg = strings.ReplaceAll(msg, raw, red)
	}
	if len(msg) > maxLoggedErr {
		msg = msg[:maxLoggedErr] + "...(truncated)"
	}
	return msg
}

// maxLoggedErr is the most bytes of an error's text that
// RedactEndpointErr keeps.
const maxLoggedErr = 512

// redactEndpoint returns the scheme and host of a znnd endpoint, or a
// placeholder when it does not parse.
func redactEndpoint(endpoint string) string {
	u, err := url.Parse(endpoint)
	if err != nil || u.Host == "" {
		return "<invalid endpoint>"
	}
	return u.Scheme + "://" + u.Host
}
