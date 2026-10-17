package wasm

import (
	"bytes"
	"encoding/binary"
	"sort"
	"testing"

	"github.com/zenon-network/go-zenon/common/crypto"
	"github.com/zenon-network/go-zenon/common/trie"
	"github.com/zenon-network/go-zenon/common/types"

	"github.com/digitalSloth/go-executor/domain"
	"github.com/digitalSloth/go-executor/internal/codec"
	"github.com/digitalSloth/go-executor/internal/state"
)

// refStateDiffPreimage returns the stateDiffHash preimage: the
// List<StateDiffEntry> encoding of entries, count:u32 BE followed by
// each entry as key:bytes32 ‖ value, where a nil value is the deletion
// sentinel (0xFFFFFFFF, no bytes) and any other value is len:u32 BE ‖
// bytes, including the zero-length present-empty case.
func refStateDiffPreimage(entries []codec.StateDiffEntry) []byte {
	buf := make([]byte, 0, 4+len(entries)*40)
	buf = binary.BigEndian.AppendUint32(buf, uint32(len(entries)))
	for _, e := range entries {
		buf = append(buf, e.Key[:]...)
		if e.Value == nil {
			buf = binary.BigEndian.AppendUint32(buf, codec.EmptyValueLength)
			continue
		}
		buf = binary.BigEndian.AppendUint32(buf, uint32(len(e.Value)))
		buf = append(buf, e.Value...)
	}
	return buf
}

// refStateDiffHash is bytes32(0) for an empty list and
// SHA3-256(refStateDiffPreimage) otherwise.
func refStateDiffHash(entries []codec.StateDiffEntry) types.Hash {
	if len(entries) == 0 {
		return types.Hash{}
	}
	return hashOf(crypto.Hash(refStateDiffPreimage(entries)))
}

// refReturnHash is bytes32(0) for empty return data and
// SHA3-256(returnData), unprefixed, otherwise.
func refReturnHash(returnData []byte) types.Hash {
	if len(returnData) == 0 {
		return types.Hash{}
	}
	return hashOf(crypto.Hash(returnData))
}

// refEventRoot is the root over each event's canonical encoding, in
// list order.
func refEventRoot(t *testing.T, events []codec.Event) types.Hash {
	t.Helper()
	elements := make([][]byte, len(events))
	for i, e := range events {
		b, err := e.Encode()
		if err != nil {
			t.Fatalf("refEventRoot: Event.Encode: %v", err)
		}
		elements[i] = b
	}
	return codec.MerkleRoot(elements)
}

// refOutboxRoot is the root over each message's canonical encoding, in
// list order.
func refOutboxRoot(t *testing.T, messages []codec.OutboxMessage) types.Hash {
	t.Helper()
	elements := make([][]byte, len(messages))
	for i, m := range messages {
		b, err := m.Encode()
		if err != nil {
			t.Fatalf("refOutboxRoot: OutboxMessage.Encode: %v", err)
		}
		elements[i] = b
	}
	return codec.MerkleRoot(elements)
}

// refWitnessKeys is the witness key union: every path the accessor
// observed (Read or Len, present or absent) unioned with every path the
// input wrote, ascending and deduplicated.
func refWitnessKeys(witnesses []state.Witness, diff []domain.StateDiffEntry) []trie.Path {
	set := make(map[trie.Path]struct{}, len(witnesses)+len(diff))
	for _, w := range witnesses {
		set[w.Path] = struct{}{}
	}
	for _, e := range diff {
		set[trie.Path(e.Path)] = struct{}{}
	}
	out := make([]trie.Path, 0, len(set))
	for p := range set {
		out = append(out, p)
	}
	sort.Slice(out, func(i, j int) bool { return bytes.Compare(out[i][:], out[j][:]) < 0 })
	return out
}

// refResult is every value the generator takes from the reference implementation. The four
// hashes and both roots reach the artifact through Receipt, so they are not surfaced twice.
type refResult struct {
	ExecutionResultBlob []byte
	WitnessKeys         []trie.Path
	Receipt             codec.Receipt
	ReceiptBlob         []byte
}

// refDerive assembles the canonical ExecutionResult (inserting the
// pre/post state roots the contract cannot compute), the two receipt
// hashes, the two roots, the witness union and the receipt, from what
// Apply returned for one vector.
func refDerive(t *testing.T, eff domain.Effects, acc *state.Accessor,
	globalInputIndex, batchID uint64, preRoot, postRoot, inputHash types.Hash) refResult {
	t.Helper()

	result := codec.ExecutionResult{
		Version:          codec.ExecutionResultVersion,
		GlobalInputIndex: globalInputIndex,
		PreStateRoot:     preRoot,
		PostStateRoot:    postRoot,
		StateDiff:        toCodecStateDiff(eff.StateDiff),
		Events:           eff.Events,
		Outbox:           eff.OutboxMessages,
		ReturnData:       eff.ReturnData,
		ClaimedDeposit:   eff.ClaimedDeposit,
	}
	encodedResult, err := result.Encode()
	if err != nil {
		t.Fatalf("refDerive: ExecutionResult.Encode: %v", err)
	}

	stateDiffHash := refStateDiffHash(result.StateDiff)
	returnHash := refReturnHash(result.ReturnData)
	eventRoot := refEventRoot(t, result.Events)
	outboxRoot := refOutboxRoot(t, result.Outbox)

	rec := codec.Receipt{
		Version:       codec.ReceiptVersion,
		InputIndex:    globalInputIndex,
		InputHash:     inputHash,
		BatchID:       batchID,
		ContractID:    eff.ContractID,
		CodeHash:      eff.CodeHash,
		Status:        eff.Status,
		GasUsed:       eff.GasUsed,
		AbortCode:     eff.AbortCode,
		StateDiffHash: stateDiffHash,
		ReturnHash:    returnHash,
		EventRoot:     eventRoot,
		EventCount:    uint32(len(eff.Events)),
		OutboxRoot:    outboxRoot,
		OutboxCount:   uint32(len(eff.OutboxMessages)),
	}
	receiptBlob, err := rec.Encode()
	if err != nil {
		t.Fatalf("refDerive: Receipt.Encode: %v", err)
	}

	return refResult{
		ExecutionResultBlob: encodedResult,
		WitnessKeys:         refWitnessKeys(acc.Witnesses(), eff.StateDiff),
		Receipt:             rec,
		ReceiptBlob:         receiptBlob,
	}
}
