package module

import (
	_ "embed"
	"encoding/json"
	"fmt"
	"strconv"
)

//go:embed wasm-gas-table-v1.json
var gasTableJSON []byte

var table = mustParseGasTable(gasTableJSON)

// Opcode is a single-byte WebAssembly Core 1.0 MVP opcode.
type Opcode byte

// HostFn identifies one of the three Phase 1 host imports.
type HostFn uint8

const (
	HostStorageLen HostFn = iota
	HostStorageRead
	HostAbort
)

// EffectKind identifies one post-execution effect charge.
type EffectKind uint8

const (
	EffectStateDiff EffectKind = iota
	EffectEvent
	EffectOutboxMessage
	EffectReturnData
)

// Effect is one charged effect: its kind and its serialized size in bytes
// (value_bytes / data_bytes / payload_bytes / return_bytes respectively).
//
// For EffectEvent, Bytes is the event's data bytes and excludes topics: topics are fixed
// 32 bytes each and are already covered by the base charge up to MaxTopicsPerEvent. Passing
// a full serialized event size instead would fork gas.
type Effect struct {
	Kind  EffectKind
	Bytes int
}

// opcodeEntry is one entry of the artifact's opcodes[] array.
type opcodeEntry struct {
	Mnemonic string `json:"mnemonic"`
	Opcode   string `json:"opcode"`
	Cost     uint64 `json:"cost"`
}

// memoryModel is the artifact's memory_model object.
type memoryModel struct {
	MemoryGrowPerPage uint64 `json:"memory_grow_per_page"`
	MemoryGrowBase    uint64 `json:"memory_grow_base"`
}

// brTableSurcharge is the artifact's surcharges.br_table object.
type brTableSurcharge struct {
	PerTargetEntry uint64 `json:"per_target_entry"`
}

// memoryGrowSurcharge is the artifact's surcharges["memory.grow"] object.
type memoryGrowSurcharge struct {
	PerPage uint64 `json:"per_page"`
}

// surcharges is the artifact's surcharges object.
type surcharges struct {
	BrTable    brTableSurcharge    `json:"br_table"`
	MemoryGrow memoryGrowSurcharge `json:"memory.grow"`
}

// hostFunctionCost is one entry of the artifact's host_functions object.
type hostFunctionCost struct {
	Base       uint64 `json:"base"`
	Per32Bytes uint64 `json:"per_32_bytes"`
}

// hostFunctions is the artifact's host_functions object.
type hostFunctions struct {
	StorageLen  hostFunctionCost `json:"storage_len"`
	StorageRead hostFunctionCost `json:"storage_read"`
	Abort       hostFunctionCost `json:"abort"`
}

// effectCharge is one entry of the artifact's post_execution_effect_charges object.
type effectCharge struct {
	Base       uint64 `json:"base"`
	Per32Bytes uint64 `json:"per_32_bytes"`
}

// postExecutionEffectCharges is the artifact's post_execution_effect_charges object.
type postExecutionEffectCharges struct {
	StateDiffEntry effectCharge `json:"state_diff_entry"`
	Event          effectCharge `json:"event"`
	OutboxMessage  effectCharge `json:"outbox_message"`
	ReturnData     effectCharge `json:"return_data"`
}

// gasTable is the parsed subset of the wasm-gas-table-v1.json artifact this package uses.
type gasTable struct {
	MeteringVersion            uint64                     `json:"metering_version"`
	OpcodeCount                int                        `json:"opcode_count"`
	Opcodes                    []opcodeEntry              `json:"opcodes"`
	MemoryModel                memoryModel                `json:"memory_model"`
	Surcharges                 surcharges                 `json:"surcharges"`
	HostFunctions              hostFunctions              `json:"host_functions"`
	PostExecutionEffectCharges postExecutionEffectCharges `json:"post_execution_effect_charges"`

	opcodeCosts     map[Opcode]uint64
	brTableBaseCost uint64
}

// mustParseGasTable parses the embedded artifact into a gasTable, panicking on any
// malformed or internally inconsistent input: the input is a compiled-in constant, so a
// failure here is a build defect, not a runtime condition.
func mustParseGasTable(raw []byte) gasTable {
	var t gasTable
	if err := json.Unmarshal(raw, &t); err != nil {
		panic(fmt.Sprintf("module: gas table does not unmarshal: %v", err))
	}
	if t.MeteringVersion != 1 {
		panic(fmt.Sprintf("module: gas table metering_version = %d, want 1", t.MeteringVersion))
	}
	if t.OpcodeCount != len(t.Opcodes) {
		panic(fmt.Sprintf("module: gas table opcode_count = %d, len(opcodes) = %d", t.OpcodeCount, len(t.Opcodes)))
	}

	t.opcodeCosts = make(map[Opcode]uint64, len(t.Opcodes))
	brTableFound := false
	for _, e := range t.Opcodes {
		op, err := parseOpcodeHex(e.Opcode)
		if err != nil {
			panic(fmt.Sprintf("module: gas table opcode %q for mnemonic %q: %v", e.Opcode, e.Mnemonic, err))
		}
		if _, dup := t.opcodeCosts[op]; dup {
			panic(fmt.Sprintf("module: gas table opcode 0x%02X duplicated (mnemonic %q)", byte(op), e.Mnemonic))
		}
		t.opcodeCosts[op] = e.Cost
		if e.Mnemonic == "br_table" {
			t.brTableBaseCost = e.Cost
			brTableFound = true
		}
	}
	if !brTableFound {
		panic("module: gas table has no br_table opcode entry")
	}

	return t
}

// parseOpcodeHex parses an artifact opcode string of the form "0x0B" into an Opcode.
func parseOpcodeHex(s string) (Opcode, error) {
	if len(s) != 4 || s[0] != '0' || s[1] != 'x' {
		return 0, fmt.Errorf("not 0x + a 1-byte hex value")
	}
	v, err := strconv.ParseUint(s[2:], 16, 8)
	if err != nil {
		return 0, fmt.Errorf("not 0x + a 1-byte hex value: %w", err)
	}
	return Opcode(v), nil
}

// OpcodeCost returns the metered cost of op, and false if op is not in the metered set. A
// false result is the hook the validator uses for the table-completeness MUST: reject the
// module, never assign an implicit cost.
func OpcodeCost(op Opcode) (uint64, bool) {
	cost, ok := table.opcodeCosts[op]
	return cost, ok
}

// BrTableCost returns the total cost of a br_table with targets labelled targets: the
// br_table base opcode cost plus per_target_entry times (targets + 1), the +1 counting the
// default target. Callers summing a segment use this instead of OpcodeCost(0x0E), never
// both.
func BrTableCost(targets int) uint64 {
	return table.brTableBaseCost + table.Surcharges.BrTable.PerTargetEntry*uint64(targets+1)
}

// MemoryGrowPageCost returns only the per-page surcharge for growing memory by pages pages
// (memory_grow_per_page * pages); the base cost already lives in the opcode table and is
// charged by the enclosing segment. Unlike BrTableCost, this is charged at run time rather
// than folded into a static segment sum, because a grow delta is dynamic.
//
// Call it only for a growth that actually succeeds. A grow that would exceed MaxMemory
// returns -1, grows nothing, and charges the base only -- no per-page charge for the
// rejected pages.
//
// The instrumentation ticket emits the per-page charge inside the instrumented WASM and
// needs the raw per-page constant as an i64.const; it obtains that constant as
// MemoryGrowPageCost(1).
func MemoryGrowPageCost(pages uint32) uint64 {
	return table.MemoryModel.MemoryGrowPerPage * uint64(pages)
}

// InitialMemoryCost returns the cost of a module's declared initial memory, charged at
// instantiation before execution begins. It is the same arithmetic as MemoryGrowPageCost,
// kept as its own named function because the two call sites are distinct consensus charge
// points.
func InitialMemoryCost(pages uint32) uint64 {
	return table.MemoryModel.MemoryGrowPerPage * uint64(pages)
}

// HostCost returns the cost of invoking the named host function fn with a value of
// valueBytes bytes: base + per_32_bytes * ceilDiv32(valueBytes). valueBytes is ignored
// where per_32_bytes is 0 (storage_len, abort). A storage_read of an absent key charges
// base only: callers pass valueBytes = 0, never the -1 ABI sentinel.
func HostCost(fn HostFn, valueBytes int) uint64 {
	var c hostFunctionCost
	switch fn {
	case HostStorageLen:
		c = table.HostFunctions.StorageLen
	case HostStorageRead:
		c = table.HostFunctions.StorageRead
	case HostAbort:
		c = table.HostFunctions.Abort
	default:
		panic(fmt.Sprintf("module: unknown host function %d", fn))
	}
	return c.Base + c.Per32Bytes*ceilDiv32(valueBytes)
}

// EffectCost returns the cost of charging effect e: base + per_32_bytes * ceilDiv32(e.Bytes)
// for its kind. A StateDiff deletion is Effect{EffectStateDiff, 0} and charges base only.
func EffectCost(e Effect) uint64 {
	var c effectCharge
	switch e.Kind {
	case EffectStateDiff:
		c = table.PostExecutionEffectCharges.StateDiffEntry
	case EffectEvent:
		c = table.PostExecutionEffectCharges.Event
	case EffectOutboxMessage:
		c = table.PostExecutionEffectCharges.OutboxMessage
	case EffectReturnData:
		c = table.PostExecutionEffectCharges.ReturnData
	default:
		panic(fmt.Sprintf("module: unknown effect kind %d", e.Kind))
	}
	return c.Base + c.Per32Bytes*ceilDiv32(e.Bytes)
}

// ceilDiv32 returns ceil(n / 32) as a uint64, returning 0 for n <= 0 as defence-in-depth so
// a negative length reaching the arithmetic yields 0 rather than converting to a huge
// uint64. No caller is expected to pass a negative value.
func ceilDiv32(n int) uint64 {
	if n <= 0 {
		return 0
	}
	return uint64((n + 31) / 32)
}

// terminators is the nine-opcode basic-block leader set (normative):
//
// An instruction is a leader iff it is the first instruction of a function body, or it
// immediately follows a segment terminator. A segment runs from a leader up to and
// including the instruction before the next leader. Its cost is the sum of its own
// instructions' table costs, charged once immediately before its first instruction; a
// segment whose total is 0 emits no charge. Two exceptions to "table cost": a br_table
// contributes BrTableCost(N) -- its base plus one entry per target including the default
// -- in place of OpcodeCost(0x0E), never both; and a memory.grow contributes only its base
// opcode cost, its per-page surcharge being charged at run time against the actual growth.
// A terminator's own cost is paid by the segment that reaches it (control can only reach it
// by falling into it), never by the segment after it -- a charge placed before
// end/else/loop/if would be skipped by every taken branch, which is unbounded free compute
// inside a loop. Code after end is a branch landing site, after else is the false arm's
// landing site, after loop is the loop-label landing site. block does not terminate a
// segment (nothing branches to the instruction right after a bare block). call/call_indirect
// do not either (the callee charges its own segments). Segments are computed over the
// original instruction stream, so injected instructions are never metered.
var terminators = map[Opcode]bool{
	0x00: true, // unreachable
	0x03: true, // loop
	0x04: true, // if
	0x05: true, // else
	0x0B: true, // end
	0x0C: true, // br
	0x0D: true, // br_if
	0x0E: true, // br_table
	0x0F: true, // return
}

// TerminatesSegment reports whether op ends the basic-block segment that reaches it. See
// the terminators doc comment for the normative segmentation rule.
func TerminatesSegment(op Opcode) bool {
	return terminators[op]
}
