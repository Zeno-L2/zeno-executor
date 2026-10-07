package ledger

import "github.com/zenon-network/go-zenon/common/types"

// AssignmentEntry is one input Settlement assigned to a domain: the input at Ordinal in the
// domain's sequence, its global index, the send's hash, and the height of the momentum that
// confirms the send.
type AssignmentEntry struct {
	Ordinal                uint64
	GlobalInputIndex       uint64
	SourceAccountBlockHash types.Hash
	SendMomentumHeight     uint64
}

// AssignmentSource returns the inputs Settlement admitted for domainID at the momentum
// (height, hash), as the entries for ordinals nextOrdinal, nextOrdinal+1, … in order. An error
// wrapping recovery.ErrHalted is a consistency fault; any other error is an outage.
type AssignmentSource interface {
	Assignment(domainID uint32, height uint64, hash types.Hash, nextOrdinal uint64) ([]AssignmentEntry, error)
}
