package codec

import "errors"

// Sentinel errors returned by every decoder and encoder in this package. Callers compare
// against these with errors.Is; the conformance vector suite maps each vector's
// reject_reason string to one of these.
var (
	// ErrTruncated is returned when fewer bytes remain than a fixed-width field requires.
	ErrTruncated = errors.New("codec: truncated")
	// ErrTrailingBytes is returned when a top-level decode does not consume the whole input.
	ErrTrailingBytes = errors.New("codec: trailing bytes")
	// ErrUnknownVersion is returned when a version field does not match the expected constant.
	ErrUnknownVersion = errors.New("codec: unknown version")
	// ErrOverflow is returned when a length or count prefix exceeds the remaining bytes.
	ErrOverflow = errors.New("codec: length or count exceeds remaining bytes")
	// ErrLimitExceeded is returned when a list or field exceeds a §25 bound.
	ErrLimitExceeded = errors.New("codec: limit exceeded")
	// ErrNotSorted is returned when a list required to be ascending is not.
	ErrNotSorted = errors.New("codec: not sorted")
	// ErrDuplicateKey is returned when a list required to have distinct keys does not.
	ErrDuplicateKey = errors.New("codec: duplicate key")
	// ErrInvalidAssetID is returned when an AssetID has a non-zero byte in [0:22].
	ErrInvalidAssetID = errors.New("codec: invalid asset id")
	// ErrInvalidKind is returned for an out-of-range kind ordinal or an inconsistent branch.
	ErrInvalidKind = errors.New("codec: invalid kind")
	// ErrInvalidStatus is returned for a status ordinal outside 0…8.
	ErrInvalidStatus = errors.New("codec: invalid status")
	// ErrZeroStorageBytes is returned when a BuyStorage input's bytes is zero (§10.7).
	ErrZeroStorageBytes = errors.New("codec: BuyStorage bytes is zero")
	// ErrNonEmptyWithdrawalPayload is returned when an L1Withdrawal outbox message carries a
	// non-empty payload.
	ErrNonEmptyWithdrawalPayload = errors.New("codec: non-empty withdrawal payload")
	// ErrNonEmptyProofData is returned when a Phase-1 batch commitment's proofData is not the
	// zero-length encoding.
	ErrNonEmptyProofData = errors.New("codec: non-empty proof data")
	// ErrEmptyAssetFlowEntry is returned when an AssetFlowEntry has both amounts zero.
	ErrEmptyAssetFlowEntry = errors.New("codec: empty asset flow entry")
	// ErrIndexGap is returned when an event or outbox index does not run ascending from 0
	// with no gaps.
	ErrIndexGap = errors.New("codec: index gap")
	// ErrIndexMismatch is returned when an event or outbox message's input_index disagrees
	// with the enclosing blob's globalInputIndex.
	ErrIndexMismatch = errors.New("codec: index mismatch")
	// ErrReservedLength is returned for a state value length in [MaxStateValueSize+1,
	// 0xFFFFFFFE], which is neither a valid length nor the deletion sentinel.
	ErrReservedLength = errors.New("codec: reserved length")
	// ErrRootsPresent is returned when bytes claiming to be a ContractEffects blob also parse
	// as a 9-field ExecutionResult (i.e. carry preStateRoot/postStateRoot).
	ErrRootsPresent = errors.New("codec: roots present")
	// ErrInvalidPolicy is returned when a ContractMetadata's upgrade_policy is not one of the
	// §27.12 ordinals.
	ErrInvalidPolicy = errors.New("codec: invalid upgrade policy")
	// ErrInvalidOwner is returned when a ContractMetadata's owner is inconsistent with its
	// upgrade_policy (§27.12).
	ErrInvalidOwner = errors.New("codec: owner inconsistent with upgrade policy")
	// ErrInvalidProofSide is returned when a MerkleProof step's side is
	// not 0 or 1.
	ErrInvalidProofSide = errors.New("codec: invalid merkle proof side")
	// ErrProofMismatch is returned when a well-formed MerkleProof does
	// not reach the root.
	ErrProofMismatch = errors.New("codec: merkle proof does not reach root")
)
