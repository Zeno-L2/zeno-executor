package supervisor

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/zenon-network/go-zenon/common/types"

	"github.com/digitalSloth/go-executor/internal/codec"
	"github.com/digitalSloth/go-executor/internal/ipc"
	"github.com/digitalSloth/go-executor/internal/ledger"
	"github.com/digitalSloth/go-executor/internal/recovery"
)

// ErrMalformed marks a worker frame that breaks the protocol: over the frame cap, an unknown or
// wrong-direction type, a bad payload, or a Committed that fails a field check.
var ErrMalformed = errors.New("supervisor: malformed worker message")

// Sources is what serve reads to answer a worker's requests. Run also reads it: it calls Ready
// before starting any worker.
type Sources struct {
	Momenta ledger.MomentumSource
	Anchors recovery.AnchorSource
	Ready   func(ctx context.Context) bool // nil: Run starts workers without waiting
}

// workerState is shared by serve and the worker's supervision goroutine, guarded by mu.
type workerState struct {
	mu            sync.Mutex
	lastFrame     time.Time
	servedHeight  uint64
	lastCommitted *codec.BatchCommitment
	highWater     uint64 // highest LastInputSeq committed; a Committed above it is progress
	progressed    bool   // set by a Committed above highWater; cleared by the supervision goroutine
}

// serve answers conn's requests from src until EOF, a short read or a write failure (nil), or a
// malformed frame (an error wrapping ErrMalformed).
func serve(conn io.ReadWriter, w WorkerConfig, executor types.Address, src Sources, st *workerState) error {
	for {
		m, err := ipc.ReadMessage(conn, ipc.Worker)
		if err != nil {
			if errors.Is(err, ipc.ErrMalformed) {
				return fmt.Errorf("%w: %w", ErrMalformed, err)
			}
			return nil
		}

		st.mu.Lock()
		st.lastFrame = time.Now()
		st.mu.Unlock()

		switch v := m.(type) {
		case ipc.FrontierRequest:
			if !serveFrontier(conn, src) {
				return nil
			}

		case ipc.AnchorRequest:
			if !serveAnchor(conn, w, src) {
				return nil
			}

		case ipc.MomentaRequest:
			if !serveMomenta(conn, v, src, st) {
				return nil
			}

		case ipc.Committed:
			if err := recordCommitted(v.Commitment, w, executor, st); err != nil {
				return err
			}
		}
	}
}

// serveFrontier answers a FrontierRequest. It returns false when the reply write failed, telling
// serve to end the session with nil.
func serveFrontier(conn io.ReadWriter, src Sources) bool {
	height, err := src.Momenta.FrontierHeight()
	var reply ipc.Message
	if err != nil {
		reply = sourceError(err)
	} else {
		reply = ipc.Frontier{Height: height}
	}
	return ipc.WriteMessage(conn, reply) == nil
}

// serveAnchor answers an AnchorRequest for w's own domain. A source anchor reported not found is
// sent as the zero anchor: ipc.Encode rejects a not-found anchor with a non-zero root or seq.
func serveAnchor(conn io.ReadWriter, w WorkerConfig, src Sources) bool {
	anchor, found, err := src.Anchors.FinalizedAnchor(w.DomainID)
	var reply ipc.Message
	switch {
	case err != nil:
		reply = sourceError(err)
	case found:
		reply = ipc.Anchor{Found: true, PostStateRoot: anchor.PostStateRoot, LastInputSeq: anchor.LastInputSeq}
	default:
		reply = ipc.Anchor{}
	}
	return ipc.WriteMessage(conn, reply) == nil
}

// serveMomenta answers a MomentaRequest: it calls Momenta.DetailedMomentumsByHeight once, keeps
// at most v.Count results, encodes every kept entry before writing anything, and only raises
// st.servedHeight once RangeEnd is written. It returns false when a reply write failed.
func serveMomenta(conn io.ReadWriter, v ipc.MomentaRequest, src Sources, st *workerState) bool {
	ms, err := src.Momenta.DetailedMomentumsByHeight(v.Height, v.Count)
	if err != nil {
		return ipc.WriteMessage(conn, sourceError(err)) == nil
	}

	kept := ms
	if uint64(len(kept)) > v.Count {
		kept = kept[:v.Count]
	}

	encoded := make([][]byte, 0, len(kept))
	for i, dm := range kept {
		b, err := ipc.Encode(ipc.Momentum{Detailed: dm})
		if err != nil {
			msg := cut(fmt.Sprintf("supervisor: momentum %d of the range from height %d cannot be sent: %s", i, v.Height, err))
			return ipc.WriteMessage(conn, ipc.SourceError{Kind: ipc.SourceHalted, Message: msg}) == nil
		}
		encoded = append(encoded, b)
	}

	for _, b := range encoded {
		if _, err := conn.Write(b); err != nil {
			return false
		}
	}
	if err := ipc.WriteMessage(conn, ipc.RangeEnd{Count: uint32(len(kept))}); err != nil {
		return false
	}

	if len(kept) > 0 {
		maxHeight := kept[0].Momentum.Height
		for _, dm := range kept[1:] {
			if dm.Momentum.Height > maxHeight {
				maxHeight = dm.Momentum.Height
			}
		}
		st.mu.Lock()
		if maxHeight > st.servedHeight {
			st.servedHeight = maxHeight
		}
		st.mu.Unlock()
	}
	return true
}

// recordCommitted validates a worker's Committed frame and, if valid, records it. c is already
// strictly decoded by ReadMessage (codec.DecodeBatchCommitment, internal/ipc/messages.go:574-580).
func recordCommitted(c codec.BatchCommitment, w WorkerConfig, executor types.Address, st *workerState) error {
	if c.DomainID != w.DomainID {
		return fmt.Errorf("%w: committed domainId %d, want %d", ErrMalformed, c.DomainID, w.DomainID)
	}
	if c.ExecutorID != executor {
		return fmt.Errorf("%w: committed executorId %s, want %s", ErrMalformed, c.ExecutorID, executor)
	}
	if c.SubmittedAtHeight != 0 {
		return fmt.Errorf("%w: committed submittedAtHeight %d, want 0", ErrMalformed, c.SubmittedAtHeight)
	}

	st.mu.Lock()
	defer st.mu.Unlock()
	cc := c
	st.lastCommitted = &cc
	if c.LastInputSeq > st.highWater {
		st.highWater = c.LastInputSeq
		st.progressed = true
	}
	return nil
}

// sourceError maps a source-call error to the SourceError the worker sees: errors.Is(err,
// recovery.ErrHalted) becomes kind halted, anything else kind unavailable. The message is cut to
// ipc.MaxSourceErrorLen bytes, which may split a UTF-8 sequence.
func sourceError(err error) ipc.SourceError {
	kind := ipc.SourceUnavailable
	if errors.Is(err, recovery.ErrHalted) {
		kind = ipc.SourceHalted
	}
	return ipc.SourceError{Kind: kind, Message: cut(err.Error())}
}

func cut(s string) string {
	if len(s) <= ipc.MaxSourceErrorLen {
		return s
	}
	return s[:ipc.MaxSourceErrorLen]
}
