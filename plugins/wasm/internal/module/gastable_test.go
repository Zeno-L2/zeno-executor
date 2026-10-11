package module

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/digitalSloth/go-executor/internal/codec"
)

// opcodeFor resolves a mnemonic to its opcode byte through the parsed artifact, failing the
// test if the mnemonic is absent.
func opcodeFor(t *testing.T, mnemonic string) Opcode {
	t.Helper()
	for _, e := range table.Opcodes {
		if e.Mnemonic == mnemonic {
			op, err := parseOpcodeHex(e.Opcode)
			if err != nil {
				t.Fatalf("opcodeFor(%q): %v", mnemonic, err)
			}
			return op
		}
	}
	t.Fatalf("opcodeFor(%q): mnemonic not found in artifact", mnemonic)
	return 0
}

func TestArtifactInvariants(t *testing.T) {
	if table.MeteringVersion != 1 {
		t.Errorf("metering_version = %d, want 1", table.MeteringVersion)
	}
	if table.OpcodeCount != 104 {
		t.Errorf("opcode_count = %d, want 104", table.OpcodeCount)
	}
	if len(table.Opcodes) != 104 {
		t.Errorf("len(opcodes) = %d, want 104", len(table.Opcodes))
	}
	if len(table.opcodeCosts) != 104 {
		t.Errorf("len(opcodeCosts) = %d, want 104", len(table.opcodeCosts))
	}
	if table.MeteringVersion != uint64(codec.MeteringVersion) {
		t.Errorf("table.MeteringVersion = %d, codec.MeteringVersion = %d", table.MeteringVersion, codec.MeteringVersion)
	}
}

func TestOpcodeCostFullTableRoundTrip(t *testing.T) {
	for _, e := range table.Opcodes {
		op, err := parseOpcodeHex(e.Opcode)
		if err != nil {
			t.Fatalf("parseOpcodeHex(%q): %v", e.Opcode, err)
		}
		got, ok := OpcodeCost(op)
		if !ok {
			t.Errorf("OpcodeCost(0x%02X) (%s) ok = false, want true", byte(op), e.Mnemonic)
			continue
		}
		if got != e.Cost {
			t.Errorf("OpcodeCost(0x%02X) (%s) = %d, want %d", byte(op), e.Mnemonic, got, e.Cost)
		}
	}

	for b := 0; b < 256; b++ {
		op := Opcode(byte(b))
		_, inTable := table.opcodeCosts[op]
		_, ok := OpcodeCost(op)
		if ok != inTable {
			t.Errorf("OpcodeCost(0x%02X) ok = %v, want %v", byte(b), ok, inTable)
		}
	}
}

func TestWorkedExamples(t *testing.T) {
	if got := HostCost(HostStorageRead, 100); got != 240 {
		t.Errorf("HostCost(HostStorageRead, 100) = %d, want 240", got)
	}
	if got := EffectCost(Effect{EffectStateDiff, 64}); got != 540 {
		t.Errorf("EffectCost({EffectStateDiff, 64}) = %d, want 540", got)
	}
	if got := EffectCost(Effect{EffectStateDiff, 0}); got != 500 {
		t.Errorf("EffectCost({EffectStateDiff, 0}) = %d, want 500", got)
	}
	if got := MemoryGrowPageCost(3); got != 3000 {
		t.Errorf("MemoryGrowPageCost(3) = %d, want 3000", got)
	}
	base, ok := OpcodeCost(opcodeFor(t, "memory.grow"))
	if !ok {
		t.Fatalf("OpcodeCost(memory.grow) ok = false")
	}
	if got := base + MemoryGrowPageCost(3); got != 3005 {
		t.Errorf("memory.grow base + MemoryGrowPageCost(3) = %d, want 3005", got)
	}
	if got := BrTableCost(5); got != 8 {
		t.Errorf("BrTableCost(5) = %d, want 8", got)
	}
	if got := EffectCost(Effect{EffectEvent, 200}); got != 170 {
		t.Errorf("EffectCost({EffectEvent, 200}) = %d, want 170", got)
	}
}

func TestCeilDiv32(t *testing.T) {
	cases := []struct {
		n    int
		want uint64
	}{
		{-1, 0},
		{0, 0},
		{1, 1},
		{31, 1},
		{32, 1},
		{33, 2},
		{64, 2},
		{100, 4},
		{200, 7},
	}
	for _, c := range cases {
		if got := ceilDiv32(c.n); got != c.want {
			t.Errorf("ceilDiv32(%d) = %d, want %d", c.n, got, c.want)
		}
	}
}

func TestHostCostU32(t *testing.T) {
	cases := map[uint32]uint64{
		0:          1,
		1:          11,
		32:         11,
		33:         21,
		1<<31 - 31: 1 + 10*(1<<26),
		1 << 31:    1 + 10*(1<<26),
		0xFFFFFFFF: 1 + 10*134217728,
	}
	for n, want := range cases {
		if got := HostCostU32(HostAbort, n); got != want {
			t.Errorf("HostCostU32(HostAbort, %d) = %d, want %d", n, got, want)
		}
	}
	if got := HostCostU32(HostStorageLen, 0xFFFFFFFF); got != 100 {
		t.Errorf("HostCostU32(HostStorageLen, max) = %d, want 100", got)
	}
}

func TestHostAndEffectCosts(t *testing.T) {
	for _, n := range []int{0, 1, 32, 100} {
		if got := HostCost(HostStorageLen, n); got != 100 {
			t.Errorf("HostCost(HostStorageLen, %d) = %d, want 100", n, got)
		}
	}
	if got := HostCost(HostStorageRead, 0); got != 200 {
		t.Errorf("HostCost(HostStorageRead, 0) = %d, want 200", got)
	}
	if got := HostCost(HostAbort, 0); got != 1 {
		t.Errorf("HostCost(HostAbort, 0) = %d, want 1", got)
	}
	for n, want := range map[int]uint64{1: 11, 32: 11, 33: 21, 4096: 1281, 4097: 1291} {
		if got := HostCost(HostAbort, n); got != want {
			t.Errorf("HostCost(HostAbort, %d) = %d, want %d", n, got, want)
		}
	}

	if got := EffectCost(Effect{EffectOutboxMessage, 0}); got != 200 {
		t.Errorf("EffectCost({EffectOutboxMessage, 0}) = %d, want 200", got)
	}
	if got := EffectCost(Effect{EffectOutboxMessage, 100}); got != 220 {
		t.Errorf("EffectCost({EffectOutboxMessage, 100}) = %d, want 220", got)
	}
	if got := EffectCost(Effect{EffectReturnData, 0}); got != 10 {
		t.Errorf("EffectCost({EffectReturnData, 0}) = %d, want 10", got)
	}
	if got := EffectCost(Effect{EffectReturnData, 100}); got != 18 {
		t.Errorf("EffectCost({EffectReturnData, 100}) = %d, want 18", got)
	}
}

func TestSurchargeEdges(t *testing.T) {
	if got := BrTableCost(0); got != 3 {
		t.Errorf("BrTableCost(0) = %d, want 3", got)
	}
	if got := MemoryGrowPageCost(0); got != 0 {
		t.Errorf("MemoryGrowPageCost(0) = %d, want 0", got)
	}
	if got := InitialMemoryCost(2); got != 2000 {
		t.Errorf("InitialMemoryCost(2) = %d, want 2000", got)
	}

	brTableBase, ok := OpcodeCost(opcodeFor(t, "br_table"))
	if !ok {
		t.Fatalf("OpcodeCost(br_table) ok = false")
	}
	want := brTableBase + table.Surcharges.BrTable.PerTargetEntry
	if got := BrTableCost(0); got != want {
		t.Errorf("BrTableCost(0) = %d, want %d (base %d + per_target_entry %d)", got, want, brTableBase, table.Surcharges.BrTable.PerTargetEntry)
	}
}

func TestCrossFieldConsistency(t *testing.T) {
	memoryGrowBase, ok := OpcodeCost(opcodeFor(t, "memory.grow"))
	if !ok {
		t.Fatalf("OpcodeCost(memory.grow) ok = false")
	}
	if table.MemoryModel.MemoryGrowBase != memoryGrowBase {
		t.Errorf("memory_model.memory_grow_base = %d, opcode-table cost of memory.grow = %d", table.MemoryModel.MemoryGrowBase, memoryGrowBase)
	}
	if table.MemoryModel.MemoryGrowPerPage != table.Surcharges.MemoryGrow.PerPage {
		t.Errorf("memory_model.memory_grow_per_page = %d, surcharges[memory.grow].per_page = %d", table.MemoryModel.MemoryGrowPerPage, table.Surcharges.MemoryGrow.PerPage)
	}
}

func TestLeaderSet(t *testing.T) {
	mnemonics := []string{"unreachable", "loop", "if", "else", "end", "br", "br_if", "br_table", "return"}
	expected := make(map[Opcode]bool, len(mnemonics))
	for _, m := range mnemonics {
		expected[opcodeFor(t, m)] = true
	}
	if len(expected) != 9 {
		t.Fatalf("expected leader set has %d distinct members, want 9", len(expected))
	}

	for b := 0; b < 256; b++ {
		op := Opcode(byte(b))
		if got, want := TerminatesSegment(op), expected[op]; got != want {
			t.Errorf("TerminatesSegment(0x%02X) = %v, want %v", byte(b), got, want)
		}
	}

	for _, m := range []string{"block", "nop", "call", "call_indirect"} {
		if TerminatesSegment(opcodeFor(t, m)) {
			t.Errorf("TerminatesSegment(%s) = true, want false", m)
		}
	}
}

// TestOpcodeCostsAllNonZero asserts every entry in the embedded gas table costs at least 1.
// segmentLeaderTotals gates charge emission on a segment's total being non-zero, and every
// branch/loop landing site is a segment leader, so an unbounded loop is only guaranteed to
// consume gas because no segment can ever total 0. A future 0-cost opcode would create a loop
// landing site with no charge at all: unbounded free compute with no other test catching it.
func TestOpcodeCostsAllNonZero(t *testing.T) {
	for _, e := range table.Opcodes {
		if e.Cost == 0 {
			t.Errorf("opcode %q (%s) has cost 0, want >= 1", e.Opcode, e.Mnemonic)
		}
	}
}

func TestMustParseGasTablePanics(t *testing.T) {
	cases := []struct {
		name string
		json string
	}{
		{
			name: "invalid JSON",
			json: `{`,
		},
		{
			name: "malformed opcode hex",
			json: `{"metering_version":1,"opcode_count":1,"opcodes":[{"mnemonic":"x","opcode":"0xZZ","cost":1}]}`,
		},
		{
			name: "opcode hex wrong length",
			json: `{"metering_version":1,"opcode_count":1,"opcodes":[{"mnemonic":"x","opcode":"0x001","cost":1}]}`,
		},
		{
			name: "duplicate opcode byte",
			json: `{"metering_version":1,"opcode_count":2,"opcodes":[{"mnemonic":"a","opcode":"0x00","cost":1},{"mnemonic":"b","opcode":"0x00","cost":2}]}`,
		},
		{
			name: "opcode_count mismatch",
			json: `{"metering_version":1,"opcode_count":5,"opcodes":[{"mnemonic":"a","opcode":"0x00","cost":1}]}`,
		},
		{
			name: "metering_version not 1",
			json: `{"metering_version":2,"opcode_count":0,"opcodes":[]}`,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			defer func() {
				if r := recover(); r == nil {
					t.Errorf("mustParseGasTable did not panic for %s", c.name)
				}
			}()
			mustParseGasTable([]byte(c.json))
		})
	}
}

func TestHostCostPanicsOnUnknownFn(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("HostCost(unknown fn) did not panic")
		}
	}()
	HostCost(HostFn(99), 0)
}

func TestEffectCostPanicsOnUnknownKind(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("EffectCost(unknown kind) did not panic")
		}
	}()
	EffectCost(Effect{Kind: EffectKind(99), Bytes: 0})
}

// TestArtifactDrift holds the embedded gas table equal to the published artifact vendored in
// testdata. The two are separate copies of one table: the embedded one is what the metering
// code reads, the vendored one is the published artifact as last synced. Changing the table
// means replacing both, and this test fails until they agree.
func TestArtifactDrift(t *testing.T) {
	publishedPath := filepath.Join("testdata", "wasm-gas-table-v1.json")
	published, err := os.ReadFile(publishedPath)
	if err != nil {
		t.Fatalf("read published artifact: %v", err)
	}
	if !bytes.Equal(published, gasTableJSON) {
		t.Errorf("embedded gas table differs from %s", publishedPath)
	}
}
