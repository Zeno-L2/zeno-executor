package ledger

import (
	"errors"

	"github.com/zenon-network/go-zenon/chain/nom"
)

// Envelope is the {domainId, payload} an accepted Settlement account-block carries.
//
// The Follower stores Payload and SourceData by reference in its buffer, sometimes across an
// entire refill window, and passes them on to Input.Payload / InputContext.SourceData
// unchanged. Once Accept returns an Envelope, the Follower owns both slices: an
// implementation MUST NOT mutate either backing array afterward, and MUST NOT hand out a
// slice that aliases a buffer it reuses or mutates on a later call.
type Envelope struct {
	DomainID uint32
	Payload  []byte
	// SourceData is the domain-defined tail of the source account-block that the payload's
	// canonical encoding does not carry (e.g. a deployment chunk's bytes). It is passed
	// through to domain.InputContext.SourceData unchanged.
	SourceData []byte
}

// SettlementFilter decides whether one account-block is a canonical input (SPEC §4.6).
//
// Only a send block addressed to the Settlement contract MAY yield a non-nil Envelope. A
// receive block MUST yield a nil Envelope. Deriving an input at a receive would attribute the
// input to the contract's own address and to the receiving momentum, which is not where the
// canonical stream places it. Receives are delivered only for completeness of the canonical
// per-block walk; a filter MAY ignore them and is not required to derive anything from them.
//
// The Follower guarantees the delivery contract a stateful filter needs: within one Follower's
// lifetime it calls Accept at most once for any account-block, and it calls Accept on every
// account-block of every confirmed momentum it consumes, in canonical order within a momentum,
// with momenta ascending by height and no gaps. A momentum is never walked twice by one
// Follower: a *ReorgError is raised before any Accept call for the momentum that triggered it,
// and every momentum-processing error latches the follower, so no later call re-reads a momentum
// whose walk had already begun. The guarantee does not span Follower instances — a follower
// resumed at Config.StartHeight re-walks that momentum in full, so a filter whose state outlives
// one Follower MUST satisfy the resume rule on Config.StartGlobalIndex. The containing momentum
// is passed so the filter can evaluate height- and time-dependent acceptance checks (e.g.
// deployment-record expiry) without consulting block.MomentumAcknowledged, which names a
// different momentum.
//
// A nil Envelope with a nil error means the block is not an accepted Settlement input — it
// consumes no global input index. A non-nil error halts the follower permanently: once the
// buffered inputs drain, every later Next returns that same error.
//
// Accept MUST NOT mutate or reuse the backing array of a returned Envelope's Payload or
// SourceData after returning it — see the Envelope doc comment.
type SettlementFilter interface {
	Accept(m *nom.Momentum, block *nom.AccountBlock) (*Envelope, error)
}

// ErrSettlementUnwired reports that no Settlement decoder is wired in.
var ErrSettlementUnwired = errors.New("ledger: settlement input decoding is not wired")

// UnwiredFilter is the placeholder filter used until the Settlement address and ABI exist
// upstream. Accept always fails, so a build without a real decoder halts instead of silently
// deriving an empty input stream.
type UnwiredFilter struct{}

func (UnwiredFilter) Accept(m *nom.Momentum, block *nom.AccountBlock) (*Envelope, error) {
	return nil, ErrSettlementUnwired
}
