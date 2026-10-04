package codec

// Phase-1 version constants (§24). Every version field this package encodes is one of
// these, checked first on decode and rejected with ErrUnknownVersion on mismatch.
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

// Limits enforced by the codec (§25.1). These are exactly the bounds named inside the §27
// grammar blocks; the codec enforces no other size or range check (§27.0).
const (
	MaxStateKeySize       = 32
	MaxStateValueSize     = 16 << 10
	MaxEventsPerExecution = 64
	MaxEventDataSize      = 1 << 10
	MaxTopicsPerEvent     = 4
	MaxOutboxPerExecution = 32
	// MaxOutboxPerResult is the ExecutionResult outbox bound (§27.4): the contract's own
	// MaxOutboxPerExecution messages plus one slot for the runtime-emitted deposit refund
	// (§18.5), which sits outside the contract's own cap.
	MaxOutboxPerResult   = MaxOutboxPerExecution + 1
	MaxOutboxPayloadSize = 8 << 10
	MaxAssetsPerBatch    = 16
	MaxCallPayload       = 8 << 10
)

// OutboxMessage kind ordinals (§27.4).
const (
	KindL2Delivery   uint8 = 0
	KindL1Withdrawal uint8 = 1
)

// EmptyValueLength is the §27.2 EMPTY deletion sentinel: a StateDiff value length prefix of
// 0xFFFFFFFF marks deletion rather than a present value of that length.
const EmptyValueLength uint32 = 0xFFFFFFFF

// MaxBatchWithdrawal is the per-asset withdrawal cap (§27.9). Step 5a applies it to one
// input's contract withdrawals; internal/batch's builder applies it to the batch aggregate.
// Both read this one symbol, so the two cannot disagree.
//
// Step 5a must read the Periphery value in force at the momentum containing the input; the
// batch builder splits against the value it expects Settlement to apply when it registers the
// batch's withdrawals. 2^255 stands in for both until the Periphery read is wired (§19,
// §27.9).
//
// The u256 invariant: a lone input's withdrawalDebit is at most MaxBatchWithdrawal + deposit
// <= 2^255 + (2^255 - 1) = 2^256 - 1, because go-zenon's TokenMaxSupplyBig is 2^255 - 1
// (vm/constants/embedded.go:75), so internal/batch's breach() never rejects a lone input for
// overflow. A placeholder above 2^255 breaks this invariant.
var MaxBatchWithdrawal = U256{0x80}
