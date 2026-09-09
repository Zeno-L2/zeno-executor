package codec

import (
	"bytes"
	"fmt"

	"github.com/zenon-network/go-zenon/common/trie"
	"github.com/zenon-network/go-zenon/common/types"
)

// LocalStateDiffEntry is one StateDiff entry as a contract returns it in a ContractEffects
// blob (§27.2): the key is a length-prefixed local key, not yet derived. A nil Value means
// deletion (the EMPTY sentinel); a non-nil Value, including zero-length, means present.
type LocalStateDiffEntry struct {
	LocalKey []byte
	Value    []byte
}

// StateDiffEntry is one StateDiff entry in the canonical, derived-key ExecutionResult form
// (§27.2). Key is a trie.Path — an already-derived SMT position — encoded as 32 raw bytes,
// identically to a bytes32.
type StateDiffEntry struct {
	Key   trie.Path
	Value []byte
}

// writeStateValue encodes the §27.2 value form: a nil Value is the EMPTY deletion sentinel
// (length prefix 0xFFFFFFFF, no bytes); any other Value, including zero-length, is a present
// value of that length.
func writeStateValue(w *writer, value []byte) error {
	if value == nil {
		w.u32(EmptyValueLength)
		return nil
	}
	if len(value) > MaxStateValueSize {
		return ErrReservedLength
	}
	w.bytes(value)
	return nil
}

// readStateValue decodes the §27.2 value form: 0xFFFFFFFF decodes to nil (deletion);
// 0x00000000 decodes to a non-nil empty value; any length in [MaxStateValueSize+1,
// 0xFFFFFFFE] is rejected as ErrReservedLength.
func readStateValue(r *reader) ([]byte, error) {
	l, err := r.u32()
	if err != nil {
		return nil, err
	}
	if l == EmptyValueLength {
		return nil, nil
	}
	if l > MaxStateValueSize {
		return nil, ErrReservedLength
	}
	if uint64(l) > uint64(r.remaining()) {
		return nil, ErrOverflow
	}
	if l == 0 {
		return []byte{}, nil
	}
	v := make([]byte, l)
	copy(v, r.buf[r.pos:r.pos+int(l)])
	r.pos += int(l)
	return v, nil
}

func encodeLocalStateDiffEntry(w *writer, e LocalStateDiffEntry) error {
	if len(e.LocalKey) > MaxStateKeySize {
		return ErrLimitExceeded
	}
	w.bytes(e.LocalKey)
	return writeStateValue(w, e.Value)
}

func decodeLocalStateDiffEntry(r *reader) (LocalStateDiffEntry, error) {
	key, err := r.bytes(MaxStateKeySize)
	if err != nil {
		return LocalStateDiffEntry{}, err
	}
	value, err := readStateValue(r)
	if err != nil {
		return LocalStateDiffEntry{}, err
	}
	return LocalStateDiffEntry{LocalKey: key, Value: value}, nil
}

func encodeStateDiffEntry(w *writer, e StateDiffEntry) error {
	w.path(e.Key)
	return writeStateValue(w, e.Value)
}

func decodeStateDiffEntry(r *reader) (StateDiffEntry, error) {
	key, err := r.path()
	if err != nil {
		return StateDiffEntry{}, err
	}
	value, err := readStateValue(r)
	if err != nil {
		return StateDiffEntry{}, err
	}
	return StateDiffEntry{Key: key, Value: value}, nil
}

// writeLocalStateDiffList encodes a List<StateDiffEntry-local> verbatim, in the given order:
// local entries need not be sorted, and duplicate local keys are not rejected (§27.2;
// canonical order is over derived keys, which this package does not compute).
func writeLocalStateDiffList(w *writer, entries []LocalStateDiffEntry) error {
	w.u32(uint32(len(entries)))
	for _, e := range entries {
		if err := encodeLocalStateDiffEntry(w, e); err != nil {
			return err
		}
	}
	return nil
}

// minLocalStateDiffEncodedSize is the smallest possible encoded LocalStateDiffEntry: key
// length(4) + value length(4), with a zero-length key and the EMPTY sentinel.
const minLocalStateDiffEncodedSize = 4 + 4

func readLocalStateDiffList(r *reader) ([]LocalStateDiffEntry, error) {
	count, err := r.count(minLocalStateDiffEncodedSize)
	if err != nil {
		return nil, err
	}
	entries := make([]LocalStateDiffEntry, 0, count)
	for i := uint32(0); i < count; i++ {
		e, err := decodeLocalStateDiffEntry(r)
		if err != nil {
			return nil, err
		}
		entries = append(entries, e)
	}
	return entries, nil
}

// writeDerivedStateDiffList encodes a List<StateDiffEntry>, requiring strictly ascending
// order by derived key with no duplicates (§27.2).
func writeDerivedStateDiffList(w *writer, entries []StateDiffEntry) error {
	for i := 1; i < len(entries); i++ {
		switch bytes.Compare(entries[i].Key[:], entries[i-1].Key[:]) {
		case 0:
			return ErrDuplicateKey
		case -1:
			return ErrNotSorted
		}
	}
	w.u32(uint32(len(entries)))
	for _, e := range entries {
		if err := encodeStateDiffEntry(w, e); err != nil {
			return err
		}
	}
	return nil
}

// minDerivedStateDiffEncodedSize is the smallest possible encoded StateDiffEntry: key(32) +
// value length(4), with the EMPTY sentinel.
const minDerivedStateDiffEncodedSize = 32 + 4

func readDerivedStateDiffList(r *reader) ([]StateDiffEntry, error) {
	count, err := r.count(minDerivedStateDiffEncodedSize)
	if err != nil {
		return nil, err
	}
	entries := make([]StateDiffEntry, 0, count)
	for i := uint32(0); i < count; i++ {
		e, err := decodeStateDiffEntry(r)
		if err != nil {
			return nil, err
		}
		if i > 0 {
			switch bytes.Compare(e.Key[:], entries[i-1].Key[:]) {
			case 0:
				return nil, ErrDuplicateKey
			case -1:
				return nil, ErrNotSorted
			}
		}
		entries = append(entries, e)
	}
	return entries, nil
}

// ContractEffects is the blob a contract's run entry point returns (§27.1a): everything the
// contract computed, before the runtime inserts the state roots it alone may compute.
type ContractEffects struct {
	Version          uint16
	GlobalInputIndex uint64
	StateDiff        []LocalStateDiffEntry
	Events           []Event
	Outbox           []OutboxMessage
	ReturnData       []byte
	ClaimedDeposit   U256
}

// Encode returns the canonical encoding of c. It rejects a struct whose bytes would also
// parse as a 9-field ExecutionResult (i.e. one that would carry roots, §14.2) with
// ErrRootsPresent, so no invalid struct has an encoding.
func (c ContractEffects) Encode() ([]byte, error) {
	w := &writer{}
	if err := encodeContractEffectsBody(w, c); err != nil {
		return nil, fmt.Errorf("codec: encode contract effects: %w", err)
	}
	if _, err := DecodeExecutionResult(w.buf); err == nil {
		return nil, fmt.Errorf("codec: encode contract effects: %w", ErrRootsPresent)
	}
	return w.buf, nil
}

// DecodeContractEffects decodes a canonical ContractEffects blob. After a successful 7-field
// decode, it additionally attempts DecodeExecutionResult on the same bytes and returns
// ErrRootsPresent if that also succeeds — the fail-safe direction §14.2 requires for a blob
// that happens to parse both ways.
func DecodeContractEffects(b []byte) (ContractEffects, error) {
	r := newReader(b)
	c, err := decodeContractEffectsBody(r)
	if err != nil {
		return ContractEffects{}, fmt.Errorf("codec: decode contract effects: %w", err)
	}
	if err := r.end(); err != nil {
		return ContractEffects{}, fmt.Errorf("codec: decode contract effects: %w", err)
	}
	if _, err := DecodeExecutionResult(b); err == nil {
		return ContractEffects{}, fmt.Errorf("codec: decode contract effects: %w", ErrRootsPresent)
	}
	return c, nil
}

func encodeContractEffectsBody(w *writer, c ContractEffects) error {
	if c.Version != ExecutionResultVersion {
		return ErrUnknownVersion
	}
	w.u16(c.Version)
	w.u64(c.GlobalInputIndex)
	if err := writeLocalStateDiffList(w, c.StateDiff); err != nil {
		return err
	}
	if err := writeEventList(w, c.Events, c.GlobalInputIndex); err != nil {
		return err
	}
	if err := writeOutboxList(w, c.Outbox, c.GlobalInputIndex, MaxOutboxPerExecution); err != nil {
		return err
	}
	w.bytes(c.ReturnData)
	w.u256(c.ClaimedDeposit)
	return nil
}

func decodeContractEffectsBody(r *reader) (ContractEffects, error) {
	var c ContractEffects
	var err error

	v, err := r.u16()
	if err != nil {
		return c, err
	}
	if v != ExecutionResultVersion {
		return c, ErrUnknownVersion
	}
	c.Version = v

	if c.GlobalInputIndex, err = r.u64(); err != nil {
		return c, err
	}
	if c.StateDiff, err = readLocalStateDiffList(r); err != nil {
		return c, err
	}
	if c.Events, err = readEventList(r, c.GlobalInputIndex); err != nil {
		return c, err
	}
	if c.Outbox, err = readOutboxList(r, c.GlobalInputIndex, MaxOutboxPerExecution); err != nil {
		return c, err
	}
	if c.ReturnData, err = r.bytes(^uint32(0)); err != nil {
		return c, err
	}
	if c.ClaimedDeposit, err = r.u256(); err != nil {
		return c, err
	}
	return c, nil
}

// ExecutionResult is the canonical, runtime-assembled result of one input's execution
// (§27.1a): ContractEffects plus the pre/post state roots the runtime alone computes.
type ExecutionResult struct {
	Version          uint16
	GlobalInputIndex uint64
	PreStateRoot     types.Hash
	PostStateRoot    types.Hash
	StateDiff        []StateDiffEntry
	Events           []Event
	// Outbox is bounded at MaxOutboxPerResult on decode. The codec places no constraint on
	// which slot (if any) is the runtime-emitted deposit refund, or on that message's
	// SourceContract; attributing the 33rd slot to the refund is the runtime's invariant, not
	// this type's.
	Outbox         []OutboxMessage
	ReturnData     []byte
	ClaimedDeposit U256
}

// Encode returns the canonical encoding of e.
func (e ExecutionResult) Encode() ([]byte, error) {
	w := &writer{}
	if err := encodeExecutionResultBody(w, e); err != nil {
		return nil, fmt.Errorf("codec: encode execution result: %w", err)
	}
	return w.buf, nil
}

// DecodeExecutionResult decodes a canonical ExecutionResult.
func DecodeExecutionResult(b []byte) (ExecutionResult, error) {
	r := newReader(b)
	e, err := decodeExecutionResultBody(r)
	if err != nil {
		return e, fmt.Errorf("codec: decode execution result: %w", err)
	}
	if err := r.end(); err != nil {
		return e, fmt.Errorf("codec: decode execution result: %w", err)
	}
	return e, nil
}

func encodeExecutionResultBody(w *writer, e ExecutionResult) error {
	if e.Version != ExecutionResultVersion {
		return ErrUnknownVersion
	}
	w.u16(e.Version)
	w.u64(e.GlobalInputIndex)
	w.hash(e.PreStateRoot)
	w.hash(e.PostStateRoot)
	if err := writeDerivedStateDiffList(w, e.StateDiff); err != nil {
		return err
	}
	if err := writeEventList(w, e.Events, e.GlobalInputIndex); err != nil {
		return err
	}
	if err := writeOutboxList(w, e.Outbox, e.GlobalInputIndex, MaxOutboxPerResult); err != nil {
		return err
	}
	w.bytes(e.ReturnData)
	w.u256(e.ClaimedDeposit)
	return nil
}

func decodeExecutionResultBody(r *reader) (ExecutionResult, error) {
	var e ExecutionResult
	var err error

	v, err := r.u16()
	if err != nil {
		return e, err
	}
	if v != ExecutionResultVersion {
		return e, ErrUnknownVersion
	}
	e.Version = v

	if e.GlobalInputIndex, err = r.u64(); err != nil {
		return e, err
	}
	if e.PreStateRoot, err = r.hash(); err != nil {
		return e, err
	}
	if e.PostStateRoot, err = r.hash(); err != nil {
		return e, err
	}
	if e.StateDiff, err = readDerivedStateDiffList(r); err != nil {
		return e, err
	}
	if e.Events, err = readEventList(r, e.GlobalInputIndex); err != nil {
		return e, err
	}
	if e.Outbox, err = readOutboxList(r, e.GlobalInputIndex, MaxOutboxPerResult); err != nil {
		return e, err
	}
	if e.ReturnData, err = r.bytes(^uint32(0)); err != nil {
		return e, err
	}
	if e.ClaimedDeposit, err = r.u256(); err != nil {
		return e, err
	}
	return e, nil
}
