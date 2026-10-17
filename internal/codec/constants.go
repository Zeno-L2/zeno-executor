package codec

// Version constants. Every version field this package encodes is one
// of these, checked first on decode and rejected with
// ErrUnknownVersion on mismatch.
const (
	ProtocolVersion        uint16 = 1
	RuntimeProfileVersion  uint16 = 1
	MeteringVersion        uint16 = 1
	SerializationVersion   uint16 = 1
	StateDiffVersion       uint16 = 1
	ExecutionResultVersion uint16 = 1
	ReceiptVersion         uint16 = 1
	OutboxVersion          uint16 = 1
	DAModeVersion          uint16 = 1
)

// Limits enforced by the codec. The codec enforces no other size or
// range check.
const (
	MaxStateKeySize       = 32
	MaxStateValueSize     = 16 << 10
	MaxBatchInputs        = 64
	MaxEventsPerExecution = 64
	MaxEventDataSize      = 1 << 10
	MaxTopicsPerEvent     = 4
	MaxOutboxPerExecution = 32
	// MaxOutboxPerResult is the ExecutionResult outbox bound: the
	// contract's own MaxOutboxPerExecution messages plus one slot for
	// the runtime-emitted deposit refund, which sits outside the
	// contract's own cap.
	MaxOutboxPerResult   = MaxOutboxPerExecution + 1
	MaxOutboxPayloadSize = 8 << 10
	MaxAssetsPerBatch    = 16
	MaxCallPayload       = 8 << 10
	// MaxReturnData bounds an ExecutionResult's ReturnData, which on
	// REVERT is the revert data. It does not bound a ContractEffects
	// blob's ReturnData: an oversized one must decode, so that
	// plugins/wasm's step 7 rejects it as RESULT_TOO_LARGE.
	MaxReturnData = 4 << 10
	// MaxMerkleProofDepth bounds the step count of a MerkleProof.
	MaxMerkleProofDepth = 32
)

// OutboxMessage kind ordinals.
const (
	KindL2Delivery   uint8 = 0
	KindL1Withdrawal uint8 = 1
)

// EmptyValueLength is the EMPTY deletion sentinel: a StateDiff value
// length prefix of 0xFFFFFFFF marks deletion rather than a present
// value of that length.
const EmptyValueLength uint32 = 0xFFFFFFFF
