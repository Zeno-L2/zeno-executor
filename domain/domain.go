// Package domain defines the plugin seam between the generic executor runtime and an
// execution domain: a pure state-transition function plus input decoding and genesis. The
// runtime owns ordering, witness capture, batching, commitments, and Settlement/DA
// communication; a domain never sees any of that.
package domain

import (
	"context"

	"github.com/zenon-network/go-zenon/common/trie"
)

// Path is the 32-byte, already-derived SMT position — the same type the shared path-native
// trie API consumes. Domains compute it as a pure function of their own key layout (e.g.
// sha3(contractID, localKey)); the runtime consumes it as given and never re-hashes it.
type Path = trie.Path

// StateAccess is the runtime-supplied, witnessing state accessor a domain's Apply consumes.
// Every Read and Len call, including one that observes an absent path, is captured as a
// witness by the runtime automatically — a domain never builds a witness itself.
type StateAccess interface {
	// Read returns the value stored at path. present is false if no value has ever been
	// written at path, distinct from a present-but-empty value.
	Read(path Path) (value []byte, present bool, err error)

	// Len returns the length of the value stored at path, with the same present/absent
	// distinction as Read.
	Len(path Path) (length int, present bool, err error)
}

// StateDiffEntry is one write staged by Apply. A nil Value deletes the entry at Path; a
// non-nil, zero-length Value stores a present-but-empty leaf, which is distinct from
// deletion.
type StateDiffEntry struct {
	Path  Path
	Value []byte
}

// Event is a domain-defined log entry emitted during Apply.
type Event struct {
	Topic []byte
	Data  []byte
}

// OutboxMessage is a domain-defined message destined for L1 once its batch finalizes (e.g.
// a withdrawal release or a call into another embedded contract).
type OutboxMessage struct {
	Kind    uint8
	Payload []byte
}

// Effects is the full, pure output of one Apply call: the staged state diff plus everything
// the runtime folds into the input's receipt, the batch's event/outbox roots, and its
// AssetFlowSummary.
type Effects struct {
	StateDiff      []StateDiffEntry
	Events         []Event
	OutboxMessages []OutboxMessage
	ReturnData     []byte
}

// StateInit is a domain's genesis contribution: the leaf set the runtime seeds the SMT
// with before the first input is applied.
type StateInit struct {
	Paths  []Path
	Values [][]byte
}

// Input is an opaque, domain-decoded input. DecodeInput is the only place a domain
// interprets the raw L1 payload; Apply only ever sees the typed result.
type Input interface{}

// Domain is the state-transition-function contract every execution domain implements. It is
// the only code a domain author writes: a pure function plus decoding and genesis.
type Domain interface {
	// Genesis returns the domain's initial state, seeded once before the first input.
	Genesis() StateInit

	// DecodeInput turns an opaque L1 payload into a typed, structurally-valid Input. It
	// MUST reject anything that is not well-formed for this domain.
	DecodeInput(payload []byte) (Input, error)

	// Apply is the pure state-transition function: deterministic, side-effect-free, and a
	// function only of state and in. No clock, no RNG, no network, no environment.
	Apply(state StateAccess, in Input) (Effects, error)
}

// RelayPayload is one datum a Source watches on an external chain, destined to be posted to
// L1 as an ordinary account-block.
type RelayPayload struct {
	Data []byte
}

// Source is implemented only by domains whose inputSource is L1_RELAYED or
// EXTERNAL_OBSERVED — the relay role watches an external chain and posts what the domain's
// STF will need onto L1. An L1_NATIVE domain implements only Domain.
type Source interface {
	Watch(ctx context.Context) (<-chan RelayPayload, error)
}
