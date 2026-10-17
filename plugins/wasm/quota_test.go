package wasm

import (
	"bytes"
	"math"
	"reflect"
	"testing"

	"github.com/zenon-network/go-zenon/common/types"

	"github.com/digitalSloth/go-executor/domain"
	"github.com/digitalSloth/go-executor/internal/codec"
	"github.com/digitalSloth/go-executor/internal/state"
)

var quotaTestCID = types.Hash{0x51, 0x52}

func quotaPath(local string) domain.Path { return derive(quotaTestCID, []byte(local)) }

// quotaSettlement is a settlement over state, targeting quotaTestCID.
func quotaSettlement(st domain.StateAccess) settlement {
	return settlement{ContractID: quotaTestCID, State: st}
}

// quotaMapState seeds a mapState with leaves plus a quota leaf. quotaLeaf nil seeds none.
func quotaMapState(leaves map[domain.Path][]byte, quotaLeaf []byte) *mapState {
	values := map[domain.Path][]byte{}
	for p, v := range leaves {
		values[p] = v
	}
	if quotaLeaf != nil {
		values[derive(quotaTestCID, keyQuota)] = quotaLeaf
	}
	return &mapState{values: values}
}

// quotaStore opens a state.Store seeded with values.
func quotaStore(t *testing.T, values map[domain.Path][]byte) *state.Store {
	t.Helper()
	store := state.OpenEmpty()
	t.Cleanup(func() { store.Close() })
	var diff []domain.StateDiffEntry
	for p, v := range values {
		diff = append(diff, domain.StateDiffEntry{Path: p, Value: v})
	}
	if _, err := store.Apply(diff); err != nil {
		t.Fatalf("seed Apply: %v", err)
	}
	return store
}

func witnessedPaths(acc *state.Accessor) map[domain.Path]bool {
	out := map[domain.Path]bool{}
	for _, w := range acc.Witnesses() {
		out[domain.Path(w.Path)] = true
	}
	return out
}

func TestChargeQuota(t *testing.T) {
	v8 := bytes.Repeat([]byte{8}, 8)
	v4 := bytes.Repeat([]byte{4}, 4)
	quotaKeyPath := derive(quotaTestCID, keyQuota)

	cases := []struct {
		name      string
		state     *mapState
		diff      []domain.StateDiffEntry
		wantOK    bool
		wantErr   bool
		wantQuota []byte // appended quota entry value; nil means no entry is appended
		wantReads int    // -1 skips the check
	}{
		{
			name:      "WithinQuota",
			state:     quotaMapState(nil, encodeQuota(100, 0)),
			diff:      []domain.StateDiffEntry{{Path: quotaPath("a"), Value: v8}},
			wantOK:    true,
			wantQuota: encodeQuota(100, 72),
			wantReads: -1,
		},
		{
			name:      "ExactlyAtQuota",
			state:     quotaMapState(nil, encodeQuota(72, 0)),
			diff:      []domain.StateDiffEntry{{Path: quotaPath("a"), Value: v8}},
			wantOK:    true,
			wantQuota: encodeQuota(72, 72),
			wantReads: -1,
		},
		{
			name:      "OverQuota",
			state:     quotaMapState(nil, encodeQuota(71, 0)),
			diff:      []domain.StateDiffEntry{{Path: quotaPath("a"), Value: v8}},
			wantOK:    false,
			wantReads: -1,
		},
		{
			name:      "DeleteAbsentKeyContributesZero",
			state:     quotaMapState(nil, encodeQuota(0, 0)),
			diff:      []domain.StateDiffEntry{{Path: quotaPath("a"), Value: nil}},
			wantOK:    true,
			wantReads: 0,
		},
		{
			name:      "EqualLengthOverwriteContributesZero",
			state:     quotaMapState(map[domain.Path][]byte{quotaPath("a"): v8}, encodeQuota(72, 72)),
			diff:      []domain.StateDiffEntry{{Path: quotaPath("a"), Value: bytes.Repeat([]byte{9}, 8)}},
			wantOK:    true,
			wantReads: 0,
		},
		{
			name:      "PresentEmptyWriteCountsOverhead",
			state:     quotaMapState(nil, encodeQuota(64, 0)),
			diff:      []domain.StateDiffEntry{{Path: quotaPath("a"), Value: []byte{}}},
			wantOK:    true,
			wantQuota: encodeQuota(64, 64),
			wantReads: -1,
		},
		{
			name:      "EmptyDiffReadsNothing",
			state:     quotaMapState(nil, encodeQuota(0, 0)),
			diff:      nil,
			wantOK:    true,
			wantReads: 0,
		},
		{
			name:      "AbsentQuotaLeafGrowthIsExceeded",
			state:     quotaMapState(nil, nil),
			diff:      []domain.StateDiffEntry{{Path: quotaPath("a"), Value: v4}},
			wantOK:    false,
			wantReads: -1,
		},
		{
			name:      "AbsentQuotaLeafShrinkHalts",
			state:     quotaMapState(map[domain.Path][]byte{quotaPath("a"): v8}, nil),
			diff:      []domain.StateDiffEntry{{Path: quotaPath("a"), Value: nil}},
			wantErr:   true,
			wantReads: -1,
		},
		{
			name:      "ShrinkPastUsedHalts",
			state:     quotaMapState(map[domain.Path][]byte{quotaPath("a"): v8}, encodeQuota(100, 71)),
			diff:      []domain.StateDiffEntry{{Path: quotaPath("a"), Value: nil}},
			wantErr:   true,
			wantReads: -1,
		},
		{
			name:      "GrowthPastUint64IsExceededNotWrapped",
			state:     quotaMapState(nil, encodeQuota(math.MaxUint64, math.MaxUint64-10)),
			diff:      []domain.StateDiffEntry{{Path: quotaPath("a"), Value: v8}},
			wantOK:    false,
			wantReads: -1,
		},
		{
			name:      "ShortQuotaLeafHalts",
			state:     quotaMapState(nil, make([]byte, 15)),
			diff:      []domain.StateDiffEntry{{Path: quotaPath("a"), Value: v8}},
			wantErr:   true,
			wantReads: -1,
		},
		{
			name:      "LongQuotaLeafHalts",
			state:     quotaMapState(nil, make([]byte, 17)),
			diff:      []domain.StateDiffEntry{{Path: quotaPath("a"), Value: v8}},
			wantErr:   true,
			wantReads: -1,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, ok, err := quotaSettlement(tc.state).chargeQuota(append([]domain.StateDiffEntry(nil), tc.diff...))
			if tc.wantErr {
				if err == nil {
					t.Fatal("chargeQuota: err = nil, want a halt error")
				}
				return
			}
			if err != nil {
				t.Fatalf("chargeQuota: %v", err)
			}
			if ok != tc.wantOK {
				t.Fatalf("ok = %v, want %v", ok, tc.wantOK)
			}
			if !tc.wantOK {
				return
			}
			want := append([]domain.StateDiffEntry(nil), tc.diff...)
			if tc.wantQuota != nil {
				want = append(want, domain.StateDiffEntry{Path: quotaKeyPath, Value: tc.wantQuota})
			}
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("diff = %+v, want %+v", got, want)
			}
			if tc.wantReads >= 0 && len(tc.state.reads) != tc.wantReads {
				t.Fatalf("State.Read calls = %d, want %d", len(tc.state.reads), tc.wantReads)
			}
		})
	}
}

// A diff netting zero reads no quota leaf: the accessor's witness set lacks the quota path.
func TestChargeQuotaZeroDeltaLeavesQuotaOutOfWitnessSet(t *testing.T) {
	v8 := bytes.Repeat([]byte{8}, 8)
	store := quotaStore(t, map[domain.Path][]byte{
		quotaPath("a"):                 v8,
		derive(quotaTestCID, keyQuota): encodeQuota(72, 72),
	})
	acc := store.Accessor()

	diff := []domain.StateDiffEntry{
		{Path: quotaPath("a"), Value: nil},
		{Path: quotaPath("b"), Value: v8},
	}
	got, ok, err := quotaSettlement(acc).chargeQuota(diff)
	if err != nil || !ok {
		t.Fatalf("chargeQuota: ok = %v, err = %v, want ok", ok, err)
	}
	if !reflect.DeepEqual(got, diff) {
		t.Fatalf("diff = %+v, want it unchanged", got)
	}
	seen := witnessedPaths(acc)
	if seen[derive(quotaTestCID, keyQuota)] {
		t.Fatal("witness set holds the quota path, want it absent")
	}
	if !seen[quotaPath("a")] || !seen[quotaPath("b")] {
		t.Fatalf("witness set = %v, want both diff keys observed through Len", seen)
	}
}

// At a full quota a delete applied to the store frees its bytes, and a later call writes the
// freed size against the post-state without buying more.
func TestChargeQuotaDeleteThenReuse(t *testing.T) {
	v8 := bytes.Repeat([]byte{8}, 8)
	quotaP := derive(quotaTestCID, keyQuota)
	store := quotaStore(t, map[domain.Path][]byte{
		quotaPath("a"): v8,
		quotaP:         encodeQuota(72, 72),
	})

	first := store.Accessor()
	diff, ok, err := quotaSettlement(first).chargeQuota([]domain.StateDiffEntry{{Path: quotaPath("a"), Value: nil}})
	if err != nil || !ok {
		t.Fatalf("delete: ok = %v, err = %v, want ok", ok, err)
	}
	wantQuota := domain.StateDiffEntry{Path: quotaP, Value: encodeQuota(72, 0)}
	if len(diff) != 2 || !reflect.DeepEqual(diff[1], wantQuota) {
		t.Fatalf("delete diff = %+v, want the quota entry %+v appended", diff, wantQuota)
	}
	if _, err := store.Apply(diff); err != nil {
		t.Fatalf("Apply: %v", err)
	}

	second := store.Accessor()
	diff, ok, err = quotaSettlement(second).chargeQuota([]domain.StateDiffEntry{{Path: quotaPath("b"), Value: v8}})
	if err != nil || !ok {
		t.Fatalf("reuse: ok = %v, err = %v, want ok", ok, err)
	}
	wantQuota = domain.StateDiffEntry{Path: quotaP, Value: encodeQuota(72, 72)}
	if len(diff) != 2 || !reflect.DeepEqual(diff[1], wantQuota) {
		t.Fatalf("reuse diff = %+v, want the quota entry %+v appended", diff, wantQuota)
	}
}

func TestApplyBuyStorage(t *testing.T) {
	p := newTestPlugin(t)
	cid := types.Hash{0x61}
	codeHash := types.Hash{0x62}
	codeHashPath := derive(cid, keyCodeHash)
	quotaP := derive(cid, keyQuota)

	cases := []struct {
		name       string
		values     map[domain.Path][]byte
		bytes      uint64
		wantStatus codec.Status
		wantCode   types.Hash
		wantDiff   []domain.StateDiffEntry
	}{
		{
			name:       "SuccessNoQuotaLeaf",
			values:     map[domain.Path][]byte{codeHashPath: codeHash[:]},
			bytes:      4096,
			wantStatus: codec.StatusSuccess,
			wantCode:   codeHash,
			wantDiff:   []domain.StateDiffEntry{{Path: quotaP, Value: encodeQuota(4096, 0)}},
		},
		{
			name:       "SuccessKeepsUsed",
			values:     map[domain.Path][]byte{codeHashPath: codeHash[:], quotaP: encodeQuota(1000, 72)},
			bytes:      4096,
			wantStatus: codec.StatusSuccess,
			wantCode:   codeHash,
			wantDiff:   []domain.StateDiffEntry{{Path: quotaP, Value: encodeQuota(5096, 72)}},
		},
		{
			name:       "ExactlyMaxUint64Succeeds",
			values:     map[domain.Path][]byte{codeHashPath: codeHash[:], quotaP: encodeQuota(math.MaxUint64-4096, 0)},
			bytes:      4096,
			wantStatus: codec.StatusSuccess,
			wantCode:   codeHash,
			wantDiff:   []domain.StateDiffEntry{{Path: quotaP, Value: encodeQuota(math.MaxUint64, 0)}},
		},
		{
			name:       "Undeployed",
			values:     map[domain.Path][]byte{},
			bytes:      4096,
			wantStatus: codec.StatusValidationFailed,
		},
		{
			name:       "OverflowAtTwoToThe64",
			values:     map[domain.Path][]byte{codeHashPath: codeHash[:], quotaP: encodeQuota(math.MaxUint64-4095, 0)},
			bytes:      4096,
			wantStatus: codec.StatusValidationFailed,
			wantCode:   codeHash,
		},
	}

	var witnessSets []map[domain.Path]bool
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			acc := quotaStore(t, tc.values).Accessor()
			eff, err := p.applyBuyStorage(acc, codec.BuyStorageArgs{ContractID: cid, Bytes: tc.bytes})
			if err != nil {
				t.Fatalf("applyBuyStorage: %v", err)
			}
			if eff.Status != tc.wantStatus {
				t.Fatalf("Status = %v, want %v", eff.Status, tc.wantStatus)
			}
			if eff.ContractID != cid || eff.CodeHash != tc.wantCode {
				t.Fatalf("ContractID = %x, CodeHash = %x, want %x and %x", eff.ContractID, eff.CodeHash, cid, tc.wantCode)
			}
			if eff.GasUsed != 0 || len(eff.Events) != 0 || len(eff.OutboxMessages) != 0 {
				t.Fatalf("GasUsed = %d, Events = %d, OutboxMessages = %d, want all zero", eff.GasUsed, len(eff.Events), len(eff.OutboxMessages))
			}
			if !reflect.DeepEqual(eff.StateDiff, tc.wantDiff) {
				t.Fatalf("StateDiff = %+v, want %+v", eff.StateDiff, tc.wantDiff)
			}
			witnessSets = append(witnessSets, witnessedPaths(acc))
		})
	}

	want := map[domain.Path]bool{codeHashPath: true, quotaP: true}
	for i, seen := range witnessSets {
		if !reflect.DeepEqual(seen, want) {
			t.Errorf("case %q: witness set = %v, want exactly the 0x0001 and 0x0005 paths", cases[i].name, seen)
		}
	}
}

func TestApplyBuyStorageHaltsOnBadLeafWidth(t *testing.T) {
	p := newTestPlugin(t)
	cid := types.Hash{0x63}
	codeHash := types.Hash{0x64}

	cases := []struct {
		name   string
		values map[domain.Path][]byte
	}{
		{"ShortQuotaLeaf", map[domain.Path][]byte{derive(cid, keyCodeHash): codeHash[:], derive(cid, keyQuota): make([]byte, 15)}},
		{"LongQuotaLeaf", map[domain.Path][]byte{derive(cid, keyCodeHash): codeHash[:], derive(cid, keyQuota): make([]byte, 17)}},
		{"QuotaLeafOnUndeployedTarget", map[domain.Path][]byte{derive(cid, keyQuota): make([]byte, 8)}},
		{"ShortCodeHashLeaf", map[domain.Path][]byte{derive(cid, keyCodeHash): make([]byte, 31)}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			acc := quotaStore(t, tc.values).Accessor()
			if _, err := p.applyBuyStorage(acc, codec.BuyStorageArgs{ContractID: cid, Bytes: 1}); err == nil {
				t.Fatal("applyBuyStorage: err = nil, want a halt error")
			}
		})
	}
}
