// Package domain defines the plugin seam between the generic executor runtime and an
// execution domain: a pure state-transition function plus input decoding and genesis. The
// runtime owns ordering, witness capture, batching, commitments, and Settlement/DA
// communication; a domain never sees any of that.
package domain

import (
	"context"

	"github.com/zenon-network/go-zenon/common/trie"
	"github.com/zenon-network/go-zenon/common/types"

	"github.com/digitalSloth/go-executor/internal/codec"
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

// Effects is the full, pure output of one Apply call: the staged state diff plus everything
// the runtime folds into the input's receipt, the batch's event/outbox roots, and its
// AssetFlowSummary. Every deterministic execution outcome, including every failure status, is
// returned as Effects with a nil error — see Apply's doc comment for the halt/status split.
type Effects struct {
	// Status is the §15 receipt status ordinal.
	Status codec.Status
	// GasUsed is committed in the receiptRoot; its value per status is a domain invariant, not
	// a free choice (§27.7).
	GasUsed uint64
	// ContractID is the receipt's contractId; zero when no contract target was involved.
	ContractID types.Hash
	// CodeHash is the receipt's codeHash; zero when nothing executed.
	CodeHash types.Hash
	// AbortCode is the §7.5 abort code of a REVERT: the code passed to env.abort, or for a run
	// returning r < 0 the value (-r) truncated to i32. It is 0 for every other status. It is
	// in-process only — no wire format carries it today.
	AbortCode int32
	// ClaimedDeposit is zero on any failure status (§14.3).
	ClaimedDeposit codec.U256
	StateDiff      []StateDiffEntry
	Events         []codec.Event
	OutboxMessages []codec.OutboxMessage
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

// InputContext is the per-input context the runtime observes on L1 that the canonical wire
// encoding a domain decodes does not itself carry. It crosses the seam as a plain Go struct,
// in-process only: none of these facts are ever committed or sent over the wire by this
// package.
type InputContext struct {
	GlobalInputIndex uint64
	MomentumHeight   uint64
	MomentumTime     uint64
	// L1Sender is the raw L1 sender address of the source account-block.
	L1Sender types.Address
	// SourceData is domain-defined bytes from the source account-block that the domain's
	// canonical input encoding does not carry (e.g. chunk bytes for a chunked deployment).
	SourceData []byte
}

// Domain is the state-transition-function contract every execution domain implements. It is
// the only code a domain author writes: a pure function plus decoding and genesis.
type Domain interface {
	// Genesis returns the domain's initial state, seeded once before the first input.
	Genesis() StateInit

	// DecodeInput turns an opaque L1 payload into a typed, structurally-valid Input. It MUST
	// reject anything that is not well-formed for this domain. Its error return means halt the
	// binary — an internal or local-availability fault, never a consensus outcome; a
	// deterministic decode-time rejection the domain wants recorded as a receipt (e.g. an
	// unsupported input kind) is instead carried forward as an Input for Apply to turn into a
	// status.
	DecodeInput(ctx InputContext, payload []byte) (Input, error)

	// Apply is the pure state-transition function: deterministic, side-effect-free, and a
	// function only of state and in. No clock, no RNG, no network, no wall clock, no other
	// process environment.
	//
	// Apply's error return means halt the binary, never a consensus outcome: every
	// deterministic execution outcome, including every failure status, MUST be returned as
	// Effects with a nil error, since the runtime discards Effects entirely on a non-nil error
	// and a domain's own failure-path effects (e.g. a refund owed independently of the failure)
	// would be lost with it.
	//
	// Read-and-write local-store carve-out: Apply MAY read a local content-addressed store,
	// provided every entry it reads is verified against a hash obtained from state or from the
	// input, and provided a miss or a verification failure is reported through the error
	// channel (halt) and never as a receipt status or a state difference. Apply MAY write
	// entries to that same store, provided every entry written is a pure deterministic function
	// of state and the input — a content-addressed blob plus its integrity checksum, and
	// nothing else. Such a write is memoization only: it is never observable in the returned
	// Effects, it never changes the receipt or state outcome of any later call, and its only
	// effect is that work already derived need not be re-derived — a missing entry can only
	// halt this node, never alter what any node computes. An entry read back is still
	// hash-verified before use, exactly as the read rule requires. Apply MUST NOT read from, or
	// write to, anything else in the environment.
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
