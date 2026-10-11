package ipc

import (
	"errors"
	"fmt"
	"io"

	"github.com/zenon-network/go-zenon/chain/nom"
	"github.com/zenon-network/go-zenon/common/types"
	"github.com/zenon-network/go-zenon/vm/embedded/definition"

	"github.com/digitalSloth/go-executor/internal/codec"
	"github.com/digitalSloth/go-executor/internal/ledger"
	"github.com/digitalSloth/go-executor/internal/recovery"
)

// ErrChannel marks a broken supervisor channel: a write failure, a read failure (EOF, a short
// read, ErrMalformed, including a momentum failing DecodeMomentum), or a reply breaking the
// session rules in the package doc.
var ErrChannel = errors.New("ipc: supervisor channel failed")

// Client is the worker side of the channel: it implements ledger.MomentumSource,
// ledger.AssignmentSource, ledger.CapSource, recovery.AnchorSource and the OnCommit function runtime.Config
// takes. No locking: every call comes from the runtime's single loop goroutine, one request
// and its whole reply at a time.
type Client struct {
	conn     io.ReadWriter
	onBroken func()

	broken error

	haveDomainID bool
	domainID     uint32
}

// NewClient wraps conn. onBroken, when non-nil, is called exactly once, the first time the
// channel breaks.
func NewClient(conn io.ReadWriter, onBroken func()) *Client {
	return &Client{conn: conn, onBroken: onBroken}
}

// Broken returns the first channel failure, wrapped in ErrChannel, or nil while healthy.
func (c *Client) Broken() error { return c.broken }

// fail latches the first channel failure, calls onBroken exactly once, and returns the latched
// error.
func (c *Client) fail(cause error) error {
	if c.broken == nil {
		c.broken = fmt.Errorf("%w: %w", ErrChannel, cause)
		if c.onBroken != nil {
			c.onBroken()
		}
	}
	return c.broken
}

// send encodes and writes m. An Encode error is returned unchanged, without marking the client
// broken; a write failure marks it broken.
func (c *Client) send(m Message) error {
	b, err := Encode(m)
	if err != nil {
		return err
	}
	if _, err := c.conn.Write(b); err != nil {
		return c.fail(err)
	}
	return nil
}

// recv reads one reply frame sent by the supervisor. Any error marks the client broken.
func (c *Client) recv() (Message, error) {
	m, err := ReadMessage(c.conn, Supervisor)
	if err != nil {
		return nil, c.fail(err)
	}
	return m, nil
}

// sourceErr maps a supervisor-sent SourceError to the error runtime.Run sees. v.Message
// originates from the supervisor's upstream source (ultimately a node or RPC error) and is
// logged as part of the worker's exit reason (worker.run), so it is formatted with %q rather
// than %s: an operator log line must not let a hostile or malfunctioning node inject raw
// newlines or control characters into the worker's stderr.
func sourceErr(v SourceError) error {
	if v.Kind == SourceHalted {
		return fmt.Errorf("%w: supervisor: %q", recovery.ErrHalted, v.Message)
	}
	return fmt.Errorf("ipc: supervisor source unavailable: %q", v.Message)
}

// ReadConfig reads the worker's boot configuration. It must be the first frame; any other type
// is a channel failure. It stores DomainID for FinalizedAnchor.
func (c *Client) ReadConfig() (WorkerConfig, error) {
	if c.broken != nil {
		return WorkerConfig{}, c.broken
	}
	m, err := c.recv()
	if err != nil {
		return WorkerConfig{}, err
	}
	cfg, ok := m.(WorkerConfig)
	if !ok {
		return WorkerConfig{}, c.fail(fmt.Errorf("ipc: expected config, got %T", m))
	}
	c.domainID, c.haveDomainID = cfg.DomainID, true
	return cfg, nil
}

// FrontierHeight implements ledger.MomentumSource.
func (c *Client) FrontierHeight() (uint64, error) {
	if c.broken != nil {
		return 0, c.broken
	}
	if err := c.send(FrontierRequest{}); err != nil {
		return 0, err
	}
	m, err := c.recv()
	if err != nil {
		return 0, err
	}
	switch v := m.(type) {
	case Frontier:
		return v.Height, nil
	case SourceError:
		return 0, sourceErr(v)
	default:
		return 0, c.fail(fmt.Errorf("ipc: unexpected reply %T to a frontier request", m))
	}
}

// DetailedMomentumsByHeight implements ledger.MomentumSource. height == 0, count == 0 or
// count > MaxMomentaCount is a plain error: nothing is sent and the client stays healthy.
func (c *Client) DetailedMomentumsByHeight(height, count uint64) ([]*nom.DetailedMomentum, error) {
	if c.broken != nil {
		return nil, c.broken
	}
	if height == 0 {
		return nil, errors.New("ipc: height must be at least 1")
	}
	if count == 0 || count > MaxMomentaCount {
		return nil, fmt.Errorf("ipc: count %d is outside [1, %d]", count, MaxMomentaCount)
	}
	if err := c.send(MomentaRequest{Height: height, Count: count}); err != nil {
		return nil, err
	}
	var out []*nom.DetailedMomentum
	for {
		m, err := c.recv()
		if err != nil {
			return nil, err
		}
		switch v := m.(type) {
		case Momentum:
			if uint64(len(out)) >= count {
				return nil, c.fail(errors.New("ipc: more momentum frames than requested"))
			}
			out = append(out, v.Detailed)
		case RangeEnd:
			if uint64(v.Count) != uint64(len(out)) {
				return nil, c.fail(fmt.Errorf("ipc: range end count %d does not match %d momenta received", v.Count, len(out)))
			}
			return out, nil
		case SourceError:
			if len(out) != 0 {
				return nil, c.fail(errors.New("ipc: source error after one or more momentum frames"))
			}
			return nil, sourceErr(v)
		default:
			return nil, c.fail(fmt.Errorf("ipc: unexpected reply %T in a momenta range", m))
		}
	}
}

// FinalizedAnchor implements recovery.AnchorSource. A domainID other than the one ReadConfig
// returned is a plain error: nothing is sent and the client stays healthy.
func (c *Client) FinalizedAnchor(domainID uint32) (recovery.Anchor, bool, error) {
	if c.broken != nil {
		return recovery.Anchor{}, false, c.broken
	}
	if !c.haveDomainID || domainID != c.domainID {
		return recovery.Anchor{}, false, fmt.Errorf("ipc: domainId %d is not this worker's configured domain", domainID)
	}
	if err := c.send(AnchorRequest{}); err != nil {
		return recovery.Anchor{}, false, err
	}
	m, err := c.recv()
	if err != nil {
		return recovery.Anchor{}, false, err
	}
	switch v := m.(type) {
	case Anchor:
		return recovery.Anchor{PostStateRoot: v.PostStateRoot, LastInputSeq: v.LastInputSeq}, v.Found, nil
	case SourceError:
		return recovery.Anchor{}, false, sourceErr(v)
	default:
		return recovery.Anchor{}, false, c.fail(fmt.Errorf("ipc: unexpected reply %T to an anchor request", m))
	}
}

// NextBatchID implements runtime.CursorSource: it returns the supervisor's observed
// nextBatchId for the domain. A domainID other than the one ReadConfig returned is a plain
// error: nothing is sent and the client stays healthy.
func (c *Client) NextBatchID(domainID uint32) (uint64, error) {
	if c.broken != nil {
		return 0, c.broken
	}
	if !c.haveDomainID || domainID != c.domainID {
		return 0, fmt.Errorf("ipc: domainId %d is not this worker's configured domain", domainID)
	}
	if err := c.send(CursorRequest{}); err != nil {
		return 0, err
	}
	m, err := c.recv()
	if err != nil {
		return 0, err
	}
	switch v := m.(type) {
	case Cursor:
		return v.NextBatchID, nil
	case SourceError:
		return 0, sourceErr(v)
	default:
		return 0, c.fail(fmt.Errorf("ipc: unexpected reply %T to a cursor request", m))
	}
}

// AcceptedEntries implements runtime.CursorSource: it returns the supervisor's observed
// nextBatchId and Settlement's accepted entries for consecutive batchIds from `from`. A domainID
// other than the one ReadConfig returned is a plain error: nothing is sent and the client stays
// healthy. A reply whose entries are not consecutive from `from`, not below its nextBatchId, or
// empty while `from` is below it breaks the channel.
func (c *Client) AcceptedEntries(domainID uint32, from uint64) (uint64, []*definition.BatchEntry, error) {
	if c.broken != nil {
		return 0, nil, c.broken
	}
	if !c.haveDomainID || domainID != c.domainID {
		return 0, nil, fmt.Errorf("ipc: domainId %d is not this worker's configured domain", domainID)
	}
	if err := c.send(AcceptedRequest{FromBatchID: from}); err != nil {
		return 0, nil, err
	}
	m, err := c.recv()
	if err != nil {
		return 0, nil, err
	}
	switch v := m.(type) {
	case Accepted:
		for i, e := range v.Entries {
			if want := from + uint64(i); e.BatchId != want {
				return 0, nil, c.fail(fmt.Errorf("ipc: accepted entry %d has batchId %d, want %d", i, e.BatchId, want))
			}
			if e.BatchId >= v.NextBatchID {
				return 0, nil, c.fail(fmt.Errorf("ipc: accepted entry %d has batchId %d, not below nextBatchId %d", i, e.BatchId, v.NextBatchID))
			}
		}
		if len(v.Entries) == 0 && from < v.NextBatchID {
			return 0, nil, c.fail(fmt.Errorf("ipc: no accepted entries from batch %d below nextBatchId %d", from, v.NextBatchID))
		}
		return v.NextBatchID, v.Entries, nil
	case SourceError:
		return 0, nil, sourceErr(v)
	default:
		return 0, nil, c.fail(fmt.Errorf("ipc: unexpected reply %T to an accepted request", m))
	}
}

// Assignment implements ledger.AssignmentSource. A domainID other than the one ReadConfig
// returned, or height 0, is a plain error: nothing is sent and the client stays healthy.
func (c *Client) Assignment(domainID uint32, height uint64, hash types.Hash, nextOrdinal uint64) ([]ledger.AssignmentEntry, error) {
	if c.broken != nil {
		return nil, c.broken
	}
	if !c.haveDomainID || domainID != c.domainID {
		return nil, fmt.Errorf("ipc: domainId %d is not this worker's configured domain", domainID)
	}
	if height < 1 {
		return nil, errors.New("ipc: assignment height must be at least 1")
	}
	if err := c.send(AssignmentRequest{Height: height, Hash: hash, NextOrdinal: nextOrdinal}); err != nil {
		return nil, err
	}
	m, err := c.recv()
	if err != nil {
		return nil, err
	}
	switch v := m.(type) {
	case Assignment:
		// The first entry's ordinal is the follower's check: a misnumbered start is a
		// consistency fault, not a channel failure.
		for i := 1; i < len(v.Entries); i++ {
			if want := v.Entries[0].Ordinal + uint64(i); v.Entries[i].Ordinal != want {
				return nil, c.fail(fmt.Errorf("ipc: assignment entry %d has ordinal %d, want %d", i, v.Entries[i].Ordinal, want))
			}
		}
		return v.Entries, nil
	case SourceError:
		return nil, sourceErr(v)
	default:
		return nil, c.fail(fmt.Errorf("ipc: unexpected reply %T to an assignment request", m))
	}
}

// MaxBatchWithdrawals implements ledger.CapSource with one CapRequest. A domainID other than
// the one ReadConfig returned, height 0, or an asset count outside [1, codec.MaxAssetsPerBatch]
// is a plain error: nothing is sent and the client stays healthy.
func (c *Client) MaxBatchWithdrawals(domainID uint32, height uint64, hash types.Hash, assets []codec.AssetID) ([]codec.U256, error) {
	if c.broken != nil {
		return nil, c.broken
	}
	if !c.haveDomainID || domainID != c.domainID {
		return nil, fmt.Errorf("ipc: domainId %d is not this worker's configured domain", domainID)
	}
	if height < 1 {
		return nil, errors.New("ipc: cap height must be at least 1")
	}
	if err := c.send(CapRequest{Height: height, Hash: hash, Assets: assets}); err != nil {
		return nil, err
	}
	m, err := c.recv()
	if err != nil {
		return nil, err
	}
	switch v := m.(type) {
	case Cap:
		if len(v.Amounts) != len(assets) {
			return nil, c.fail(fmt.Errorf("ipc: cap reply carries %d amounts for %d assets", len(v.Amounts), len(assets)))
		}
		return v.Amounts, nil
	case SourceError:
		return nil, sourceErr(v)
	default:
		return nil, c.fail(fmt.Errorf("ipc: unexpected reply %T to a cap request", m))
	}
}

// ReportRecovered implements runtime.Config.OnRecover: it writes Retract{FromBatchID:
// nextBatchID}, then one Recovered frame per commitment, in order, and reads no reply.
func (c *Client) ReportRecovered(nextBatchID uint64, cs []codec.BatchCommitment) error {
	if c.broken != nil {
		return c.broken
	}
	if err := c.send(Retract{FromBatchID: nextBatchID}); err != nil {
		return err
	}
	for _, cm := range cs {
		if err := c.send(Recovered{Commitment: cm}); err != nil {
			return err
		}
	}
	return nil
}

// ReportCommitted implements runtime.Config.OnCommit: it writes one Committed frame per
// commitment, in order, and reads no reply. An empty cs writes nothing.
func (c *Client) ReportCommitted(cs []codec.BatchCommitment) error {
	if c.broken != nil {
		return c.broken
	}
	for _, cm := range cs {
		if err := c.send(Committed{Commitment: cm}); err != nil {
			return err
		}
	}
	return nil
}
