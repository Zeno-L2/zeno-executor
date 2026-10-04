package wasm

import (
	"errors"
	"reflect"
	"testing"

	"github.com/zenon-network/go-zenon/common/crypto"
	"github.com/zenon-network/go-zenon/common/types"

	"github.com/digitalSloth/go-executor/domain"
	"github.com/digitalSloth/go-executor/internal/codec"
	"github.com/digitalSloth/go-executor/plugins/wasm/internal/engine"
	"github.com/digitalSloth/go-executor/plugins/wasm/internal/module"
)

// --- fixture builder, copied from plugins/wasm/internal/engine/engine_test.go's buildModule
// (type/import/function/memory/export/code/data sections); it already supports one active
// data segment. Duplicated here because the builder is internal/engine-private.

var wasmHeader = []byte{0x00, 'a', 's', 'm', 0x01, 0x00, 0x00, 0x00}

func section(id byte, payload []byte) []byte {
	out := []byte{id}
	out = appendVarU32(out, uint32(len(payload)))
	return append(out, payload...)
}

func appendVarU32(dst []byte, v uint32) []byte {
	for {
		b := byte(v & 0x7F)
		v >>= 7
		if v != 0 {
			dst = append(dst, b|0x80)
			continue
		}
		return append(dst, b)
	}
}

func appendVarS64(dst []byte, v int64) []byte {
	for {
		b := byte(v & 0x7F)
		v >>= 7
		signBitSet := b&0x40 != 0
		if (v == 0 && !signBitSet) || (v == -1 && signBitSet) {
			return append(dst, b)
		}
		dst = append(dst, b|0x80)
	}
}

func encodeFuncType(params, results []byte) []byte {
	var b []byte
	b = append(b, 0x60)
	b = appendVarU32(b, uint32(len(params)))
	b = append(b, params...)
	b = appendVarU32(b, uint32(len(results)))
	b = append(b, results...)
	return b
}

func appendName(dst []byte, s string) []byte {
	dst = appendVarU32(dst, uint32(len(s)))
	return append(dst, s...)
}

// dataSeg is one active data segment buildModule may append.
type dataSeg struct {
	offset uint32
	bytes  []byte
}

// buildModule builds a module.Validate-accepted module: a type section carrying run's
// (i32,i32)->i64; a function section (run only); a memory section {min pages}; an export
// section exporting memory and run; the code section (run's body only); and, when data !=
// nil, an active data segment emitted after the code section. No env imports are needed for
// this ticket's fixtures.
func buildModule(data *dataSeg, locals, body []byte, pages uint32) []byte {
	var out []byte
	out = append(out, wasmHeader...)

	runType := encodeFuncType([]byte{0x7F, 0x7F}, []byte{0x7E})
	typeSec := appendVarU32(nil, 1)
	typeSec = append(typeSec, runType...)
	out = append(out, section(1, typeSec)...)

	funcSec := []byte{0x01, 0x00} // run: 1 function, type index 0
	out = append(out, section(3, funcSec)...)

	memSec := appendVarU32([]byte{0x01, 0x00}, pages) // 1 memory, flags 0, min pages
	out = append(out, section(5, memSec)...)

	exportSec := []byte{0x02}
	exportSec = appendName(exportSec, "memory")
	exportSec = append(exportSec, 0x02, 0x00)
	exportSec = appendName(exportSec, "run")
	exportSec = append(exportSec, 0x00, 0x00)
	out = append(out, section(7, exportSec)...)

	fbody := append(append([]byte{}, locals...), body...)
	codeSec := []byte{0x01}
	codeSec = appendVarU32(codeSec, uint32(len(fbody)))
	codeSec = append(codeSec, fbody...)
	out = append(out, section(10, codeSec)...)

	if data != nil {
		dataPayload := []byte{0x01, 0x00, 0x41}
		dataPayload = appendVarU32(dataPayload, data.offset)
		dataPayload = append(dataPayload, 0x0B)
		dataPayload = appendVarU32(dataPayload, uint32(len(data.bytes)))
		dataPayload = append(dataPayload, data.bytes...)
		out = append(out, section(11, dataPayload)...)
	}

	return out
}

// --- mapState: a domain.StateAccess fake over a plain map, recording every path it is asked
// to Read.

type mapState struct {
	values map[domain.Path][]byte
	reads  []domain.Path
	err    error
}

func (m *mapState) Read(p domain.Path) ([]byte, bool, error) {
	m.reads = append(m.reads, p)
	if m.err != nil {
		return nil, false, m.err
	}
	v, ok := m.values[p]
	return v, ok, nil
}

func (m *mapState) Len(p domain.Path) (int, bool, error) {
	if m.err != nil {
		return 0, false, m.err
	}
	v, ok := m.values[p]
	return len(v), ok, nil
}

// --- pure-function tests: callerHash, toCallArgs, buildFrame.

func TestCallerHashFixedVector(t *testing.T) {
	var addr types.Address
	for i := range addr {
		addr[i] = byte(i)
	}

	preimage := append([]byte("L1_CALLER:"), addr[:]...)
	if len(preimage) != 30 {
		t.Fatalf("len(preimage) = %d, want 30", len(preimage))
	}
	want := hashOf(crypto.Hash(preimage))
	got := callerHash(addr)
	if got != want {
		t.Fatalf("callerHash = %x, want %x", got, want)
	}

	bech32Form := hashOf(crypto.Hash([]byte("L1_CALLER:"), []byte(addr.String())))
	if got == bech32Form {
		t.Fatal("callerHash equals the bech32 string form; want the raw 20-byte address form")
	}

	other := addr
	other[5] ^= 0xFF
	if callerHash(addr) == callerHash(other) {
		t.Fatal("callerHash collides for two addresses differing in one byte")
	}
}

func TestToCallArgs(t *testing.T) {
	t.Run("CallL2", func(t *testing.T) {
		in := codec.CallL2Args{
			TargetContract: types.Hash{1},
			PlasmaLimit:    1000,
			DepositAsset:   codec.AssetID{},
			DepositAmount:  u256(5),
			Payload:        []byte("hello"),
		}
		got, ok := toCallArgs(in)
		if !ok {
			t.Fatal("toCallArgs: ok = false, want true")
		}
		want := callArgs{TargetContract: in.TargetContract, PlasmaLimit: in.PlasmaLimit, DepositAsset: in.DepositAsset, DepositAmount: in.DepositAmount, Payload: in.Payload}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("toCallArgs(CallL2) = %+v, want %+v", got, want)
		}
	})

	t.Run("Deposit", func(t *testing.T) {
		in := codec.DepositArgs{
			TargetContract: types.Hash{2},
			PlasmaLimit:    2000,
			DepositAsset:   codec.AssetID{},
			DepositAmount:  u256(7),
		}
		got, ok := toCallArgs(in)
		if !ok {
			t.Fatal("toCallArgs: ok = false, want true")
		}
		if got.Payload != nil {
			t.Fatalf("toCallArgs(Deposit).Payload = %v, want nil (§27.5)", got.Payload)
		}
		want := callArgs{TargetContract: in.TargetContract, PlasmaLimit: in.PlasmaLimit, DepositAsset: in.DepositAsset, DepositAmount: in.DepositAmount, Payload: nil}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("toCallArgs(Deposit) = %+v, want %+v", got, want)
		}
	})

	t.Run("OtherKinds", func(t *testing.T) {
		others := []codec.InputArgs{
			codec.DeployStartArgs{},
			codec.DeployChunkArgs{},
			codec.DeployFinalizeArgs{},
			codec.UpgradeArgs{},
		}
		for _, a := range others {
			if _, ok := toCallArgs(a); ok {
				t.Errorf("toCallArgs(%T): ok = true, want false", a)
			}
		}
	})
}

func TestBuildFrame(t *testing.T) {
	chainID := types.Hash{0x11, 0x22}

	cases := []struct {
		name    string
		payload []byte
	}{
		{"CallL2", []byte("call-payload")},
		{"Deposit", nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx := domain.InputContext{
				GlobalInputIndex: 42,
				MomentumHeight:   100,
				MomentumTime:     200, // distinct from MomentumHeight so a swap is caught
				L1Sender:         types.Address{3, 3, 3},
			}
			args := callArgs{
				TargetContract: types.Hash{4, 4, 4},
				PlasmaLimit:    999, // absent from the frame; not asserted
				DepositAsset:   codec.AssetID{},
				DepositAmount:  u256(55),
				Payload:        tc.payload,
			}
			codeHash := types.Hash{6, 6, 6}

			f := buildFrame(chainID, ctx, args, codeHash)

			if f.ContextVersion != codec.RuntimeProfileVersion {
				t.Errorf("ContextVersion = %d, want %d", f.ContextVersion, codec.RuntimeProfileVersion)
			}
			if f.ChainID != chainID {
				t.Errorf("ChainID = %x, want %x", f.ChainID, chainID)
			}
			if f.Caller != callerHash(ctx.L1Sender) {
				t.Errorf("Caller = %x, want callerHash(L1Sender)", f.Caller)
			}
			if f.ContractID != args.TargetContract {
				t.Errorf("ContractID = %x, want %x", f.ContractID, args.TargetContract)
			}
			if f.CodeHash != codeHash {
				t.Errorf("CodeHash = %x, want %x", f.CodeHash, codeHash)
			}
			if f.MomentumHeight != ctx.MomentumHeight {
				t.Errorf("MomentumHeight = %d, want %d", f.MomentumHeight, ctx.MomentumHeight)
			}
			if f.MomentumTime != ctx.MomentumTime {
				t.Errorf("MomentumTime = %d, want %d", f.MomentumTime, ctx.MomentumTime)
			}
			if f.InputIndex != ctx.GlobalInputIndex {
				t.Errorf("InputIndex = %d, want %d", f.InputIndex, ctx.GlobalInputIndex)
			}
			if f.DepositAsset != args.DepositAsset {
				t.Errorf("DepositAsset = %x, want %x", f.DepositAsset, args.DepositAsset)
			}
			if f.DepositAmount != args.DepositAmount {
				t.Errorf("DepositAmount = %x, want %x", f.DepositAmount, args.DepositAmount)
			}
			if !reflect.DeepEqual(f.Payload, args.Payload) {
				t.Errorf("Payload = %v, want %v", f.Payload, args.Payload)
			}

			encoded, err := f.Encode()
			if err != nil {
				t.Fatalf("Encode: %v", err)
			}
			decoded, err := codec.DecodeExecutionInputFrame(encoded)
			if err != nil {
				t.Fatalf("DecodeExecutionInputFrame: %v", err)
			}
			// Bytes decodes a zero-length payload as a non-nil empty slice; a round-trip
			// comparison normalizes that against a nil payload (kind 1), which is not itself
			// a wire distinction.
			want := f
			if want.Payload == nil {
				want.Payload = []byte{}
			}
			if !reflect.DeepEqual(decoded, want) {
				t.Fatalf("round-trip mismatch: got %+v, want %+v", decoded, want)
			}
		})
	}
}

// --- end-to-end tests through Apply.

func TestApplyUndeployedTarget(t *testing.T) {
	p := newTestPlugin(t)
	target := types.Hash{8, 8, 8}
	state := &mapState{values: map[domain.Path][]byte{}} // no 0x0001 leaf: undeployed

	cases := []struct {
		name string
		args codec.InputArgs
	}{
		{"CallL2", codec.CallL2Args{TargetContract: target, PlasmaLimit: 1000, DepositAsset: codec.AssetID{}, DepositAmount: u256(500)}},
		{"Deposit", codec.DepositArgs{TargetContract: target, PlasmaLimit: 1000, DepositAsset: codec.AssetID{}, DepositAmount: u256(500)}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			eff, err := p.Apply(state, Input{Ctx: domain.InputContext{L1Sender: types.Address{1}}, Args: tc.args})
			if err != nil {
				t.Fatalf("Apply: %v", err)
			}
			if eff.Status != codec.StatusValidationFailed {
				t.Fatalf("Status = %v, want StatusValidationFailed", eff.Status)
			}
			if eff.GasUsed != 0 {
				t.Fatalf("GasUsed = %d, want 0", eff.GasUsed)
			}
			if eff.CodeHash != (types.Hash{}) {
				t.Fatalf("CodeHash = %x, want zero", eff.CodeHash)
			}
			if eff.ContractID != target {
				t.Fatalf("ContractID = %x, want %x", eff.ContractID, target)
			}
			if len(eff.OutboxMessages) != 1 {
				t.Fatalf("OutboxMessages = %+v, want exactly one refund", eff.OutboxMessages)
			}
			refund := eff.OutboxMessages[0]
			if refund.OutboxIndex != 0 {
				t.Fatalf("refund OutboxIndex = %d, want 0", refund.OutboxIndex)
			}
			withdrawal, ok := refund.Target.(codec.L1Withdrawal)
			if !ok {
				t.Fatalf("refund.Target = %T, want codec.L1Withdrawal", refund.Target)
			}
			if withdrawal.Amount != u256(500) {
				t.Fatalf("refund amount = %x, want %x (full deposit_amount)", withdrawal.Amount, u256(500))
			}
		})
	}
}

// successFixture is the shared build for the SUCCESS end-to-end and cold-instrumented-tier
// tests: a module whose active data segment holds a pre-encoded ContractEffects blob at
// offset 1024 and whose run body returns (offset<<32)|len.
type successFixture struct {
	contractID       types.Hash
	globalInputIndex uint64
	raw              []byte
	instrumented     []byte
	codeHash         types.Hash
	preCodeHash      types.Hash
	blob             []byte
	decoded          codec.ContractEffects
}

func buildSuccessFixture(t *testing.T) successFixture {
	t.Helper()

	contractID := types.Hash{7, 7, 7}
	globalInputIndex := uint64(3)

	decoded := codec.ContractEffects{
		Version:          codec.ExecutionResultVersion,
		GlobalInputIndex: globalInputIndex,
		StateDiff:        []codec.LocalStateDiffEntry{{LocalKey: []byte("k"), Value: []byte("v")}},
		Outbox: []codec.OutboxMessage{
			{SourceContract: contractID, InputIndex: globalInputIndex, OutboxIndex: 0, Target: codec.L2Delivery{TargetContract: types.Hash{9}}},
		},
		ReturnData:     []byte("ok"),
		ClaimedDeposit: u256(400),
	}
	blob := encodeEffects(t, decoded)

	offset := uint32(1024)
	body := appendVarS64([]byte{0x42}, (int64(offset)<<32)|int64(len(blob)))
	body = append(body, 0x0B)
	raw := buildModule(&dataSeg{offset: offset, bytes: blob}, []byte{0x00}, body, 1)

	if err := module.Validate(raw, moduleLimits()); err != nil {
		t.Fatalf("Validate: %v", err)
	}
	instrumented, err := module.Instrument(raw, moduleLimits())
	if err != nil {
		t.Fatalf("Instrument: %v", err)
	}

	return successFixture{
		contractID:       contractID,
		globalInputIndex: globalInputIndex,
		raw:              raw,
		instrumented:     instrumented,
		codeHash:         CodeHash(instrumented),
		preCodeHash:      CodeHash(raw),
		blob:             blob,
		decoded:          decoded,
	}
}

func (f successFixture) state() *mapState {
	return &mapState{values: map[domain.Path][]byte{
		derive(f.contractID, keyCodeHash):    f.codeHash[:],
		derive(f.contractID, keyPreCodeHash): f.preCodeHash[:],
		derive(f.contractID, keyQuota):       encodeQuota(1<<20, 0),
	}}
}

func (f successFixture) input(kind string) Input {
	ctx := domain.InputContext{GlobalInputIndex: f.globalInputIndex, MomentumHeight: 5, MomentumTime: 6, L1Sender: types.Address{1, 2, 3}}
	depositAmount := u256(1000)
	if kind == "Deposit" {
		return Input{Ctx: ctx, Args: codec.DepositArgs{TargetContract: f.contractID, PlasmaLimit: 10_000_000, DepositAsset: codec.AssetID{}, DepositAmount: depositAmount}}
	}
	return Input{Ctx: ctx, Args: codec.CallL2Args{TargetContract: f.contractID, PlasmaLimit: 10_000_000, DepositAsset: codec.AssetID{}, DepositAmount: depositAmount, Payload: []byte("payload")}}
}

// Settlement rejects a plasma_limit above MaxInputGas at send validation, so an admitted input
// never carries one. Seeing one means this node's view differs from the network's, which is a
// halt: Apply returns an error rather than a committed status.
func TestApplyPlasmaLimitAboveMaxInputGasHalts(t *testing.T) {
	for _, kind := range []string{"CallL2", "Deposit"} {
		t.Run(kind, func(t *testing.T) {
			f := buildSuccessFixture(t)
			p := newTestPlugin(t)
			if err := p.store.put(entryAssembledModule, f.preCodeHash[:], f.raw); err != nil {
				t.Fatalf("store.put assembled: %v", err)
			}
			if err := p.store.put(entryInstrumentedModule, f.codeHash[:], f.instrumented); err != nil {
				t.Fatalf("store.put instrumented: %v", err)
			}

			in := f.input(kind)
			switch args := in.Args.(type) {
			case codec.CallL2Args:
				args.PlasmaLimit = MaxInputGas + 1
				in.Args = args
			case codec.DepositArgs:
				args.PlasmaLimit = MaxInputGas + 1
				in.Args = args
			default:
				t.Fatalf("unexpected args type %T", in.Args)
			}

			if _, err := p.Apply(f.state(), in); err == nil {
				t.Fatal("Apply returned no error, want a halt")
			}
		})
	}
}

func TestApplySuccessEndToEnd(t *testing.T) {
	for _, kind := range []string{"CallL2", "Deposit"} {
		t.Run(kind, func(t *testing.T) {
			f := buildSuccessFixture(t)
			p := newTestPlugin(t)
			if err := p.store.put(entryAssembledModule, f.preCodeHash[:], f.raw); err != nil {
				t.Fatalf("store.put assembled: %v", err)
			}
			if err := p.store.put(entryInstrumentedModule, f.codeHash[:], f.instrumented); err != nil {
				t.Fatalf("store.put instrumented: %v", err)
			}

			in := f.input(kind)

			// Independent engine run for the expected metered gas, over a fresh state so
			// nothing about Apply's own bookkeeping leaks in.
			callArgs, ok := toCallArgs(in.Args)
			if !ok {
				t.Fatalf("toCallArgs: ok = false for %T", in.Args)
			}
			frame := buildFrame(p.chainID, in.Ctx, callArgs, f.codeHash)
			encodedFrame, err := frame.Encode()
			if err != nil {
				t.Fatalf("frame.Encode: %v", err)
			}
			wantOut, err := p.engine.Run(engine.HostContext{State: f.state(), ContractID: f.contractID}, f.codeHash, f.instrumented, encodedFrame, 10_000_000)
			if err != nil {
				t.Fatalf("engine.Run: %v", err)
			}
			decodedForCharge, err := codec.DecodeContractEffects(wantOut.ResultBlob)
			if err != nil {
				t.Fatalf("DecodeContractEffects: %v", err)
			}
			wantGasUsed := wantOut.GasUsed + effectCharge(decodedForCharge)

			eff, err := p.Apply(f.state(), in)
			if err != nil {
				t.Fatalf("Apply: %v", err)
			}
			if eff.Status != codec.StatusSuccess {
				t.Fatalf("Status = %v, want StatusSuccess", eff.Status)
			}
			if eff.GasUsed != wantGasUsed {
				t.Fatalf("GasUsed = %d, want %d", eff.GasUsed, wantGasUsed)
			}
			wantRefundOutboxIndex := uint32(len(f.decoded.Outbox))
			if len(eff.OutboxMessages) != len(f.decoded.Outbox)+1 {
				t.Fatalf("len(OutboxMessages) = %d, want %d", len(eff.OutboxMessages), len(f.decoded.Outbox)+1)
			}
			refund := eff.OutboxMessages[len(eff.OutboxMessages)-1]
			if refund.OutboxIndex != wantRefundOutboxIndex {
				t.Fatalf("refund OutboxIndex = %d, want %d", refund.OutboxIndex, wantRefundOutboxIndex)
			}
			withdrawal, ok := refund.Target.(codec.L1Withdrawal)
			if !ok {
				t.Fatalf("refund.Target = %T, want codec.L1Withdrawal", refund.Target)
			}
			if withdrawal.Amount != u256(600) { // 1000 deposit - 400 claimed
				t.Fatalf("refund amount = %x, want %x", withdrawal.Amount, u256(600))
			}
		})
	}
}

func TestApplyColdInstrumentedTier(t *testing.T) {
	f := buildSuccessFixture(t)

	warmPlugin := newTestPlugin(t)
	if err := warmPlugin.store.put(entryAssembledModule, f.preCodeHash[:], f.raw); err != nil {
		t.Fatalf("store.put assembled: %v", err)
	}
	if err := warmPlugin.store.put(entryInstrumentedModule, f.codeHash[:], f.instrumented); err != nil {
		t.Fatalf("store.put instrumented: %v", err)
	}
	warmState := f.state()
	warmEff, err := warmPlugin.Apply(warmState, f.input("CallL2"))
	if err != nil {
		t.Fatalf("Apply (warm): %v", err)
	}

	coldPlugin := newTestPlugin(t)
	if err := coldPlugin.store.put(entryAssembledModule, f.preCodeHash[:], f.raw); err != nil {
		t.Fatalf("store.put assembled: %v", err)
	}
	// entryInstrumentedModule is deliberately left absent.
	coldState := f.state()
	coldEff, err := coldPlugin.Apply(coldState, f.input("CallL2"))
	if err != nil {
		t.Fatalf("Apply (cold): %v", err)
	}

	if !reflect.DeepEqual(warmEff, coldEff) {
		t.Fatalf("cold-tier Effects differ from warm-tier Effects:\nwarm=%+v\ncold=%+v", warmEff, coldEff)
	}

	wantReads := []domain.Path{derive(f.contractID, keyCodeHash), derive(f.contractID, keyPreCodeHash)}
	if !reflect.DeepEqual(coldState.reads[:2], wantReads) {
		t.Fatalf("cold-tier reads = %v, want %v as the first two reads", coldState.reads, wantReads)
	}
	if !reflect.DeepEqual(warmState.reads, coldState.reads) {
		t.Fatalf("warm-tier reads = %v, cold-tier reads = %v, want identical witness sets", warmState.reads, coldState.reads)
	}
}

func TestApplyMissingAssembledModuleHalts(t *testing.T) {
	f := buildSuccessFixture(t)
	p := newTestPlugin(t)
	// Neither entryAssembledModule nor entryInstrumentedModule is seeded.
	state := f.state()

	if _, err := p.Apply(state, f.input("CallL2")); err == nil {
		t.Fatal("Apply: got nil error for a missing assembled module, want a halt error")
	}
}

func TestApplyStep1HaltPaths(t *testing.T) {
	target := types.Hash{8, 8, 8}
	args := codec.CallL2Args{TargetContract: target, PlasmaLimit: 1000, DepositAsset: codec.AssetID{}, DepositAmount: u256(0)}
	in := Input{Ctx: domain.InputContext{}, Args: args}

	t.Run("ReadError", func(t *testing.T) {
		p := newTestPlugin(t)
		state := &mapState{values: map[domain.Path][]byte{}, err: errors.New("wasm: test read failure")}
		if _, err := p.Apply(state, in); err == nil {
			t.Fatal("Apply: got nil error for a Read failure, want a halt error")
		}
	})

	t.Run("WrongWidthCodeHash", func(t *testing.T) {
		p := newTestPlugin(t)
		state := &mapState{values: map[domain.Path][]byte{
			derive(target, keyCodeHash): make([]byte, 31),
		}}
		if _, err := p.Apply(state, in); err == nil {
			t.Fatal("Apply: got nil error for a 31-byte 0x0001 leaf, want a halt error")
		}
	})

	t.Run("WrongWidthPreCodeHash", func(t *testing.T) {
		p := newTestPlugin(t)
		state := &mapState{values: map[domain.Path][]byte{
			derive(target, keyCodeHash):    make([]byte, 32),
			derive(target, keyPreCodeHash): make([]byte, 31),
		}}
		if _, err := p.Apply(state, in); err == nil {
			t.Fatal("Apply: got nil error for a 31-byte 0x0002 leaf, want a halt error")
		}
	})
}
