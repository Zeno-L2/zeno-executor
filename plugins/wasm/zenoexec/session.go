package zenoexec

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"os"

	"github.com/zenon-network/go-zenon/common/types"

	"github.com/Zeno-L2/zeno-executor/domain"
	"github.com/Zeno-L2/zeno-executor/internal/batch"
	"github.com/Zeno-L2/zeno-executor/internal/codec"
	"github.com/Zeno-L2/zeno-executor/plugins/wasm"
	"github.com/Zeno-L2/zeno-executor/plugins/wasm/internal/keys"
)

// mapState is a map-backed domain.StateAccess.
type mapState struct {
	values map[domain.Path][]byte
}

func (m *mapState) Read(p domain.Path) ([]byte, bool, error) {
	v, ok := m.values[p]
	return v, ok, nil
}

func (m *mapState) Len(p domain.Path) (int, bool, error) {
	v, ok := m.values[p]
	return len(v), ok, nil
}

// capLookup caps every asset at a fixed amount.
type capLookup struct{ max codec.U256 }

func (c capLookup) MaxBatchWithdrawals(assets []codec.AssetID) ([]codec.U256, error) {
	out := make([]codec.U256, len(assets))
	for i := range out {
		out[i] = c.max
	}
	return out, nil
}

// Session is the wasm plugin over a map-backed state, with a code store
// in a temporary directory.
type Session struct {
	p     *wasm.Plugin
	dir   string
	state *mapState
	index uint64
	caps  domain.CapLookup
	// budgeted limits the plasmaLimit a call or deposit may use to
	// gasLeft, which finished inputs draw down by their gas used.
	budgeted bool
	gasLeft  uint64
}

// NewSession opens a plugin for network over an empty state. The input
// index starts at 1 and every asset is capped at 2^256-1.
func NewSession(network string) (*Session, error) {
	dir, err := os.MkdirTemp("", "zeno-exec-")
	if err != nil {
		return nil, err
	}
	p, err := wasm.New(wasm.Config{NetworkName: network, CodeStorePath: dir})
	if err != nil {
		_ = os.RemoveAll(dir)
		return nil, err
	}
	var max codec.U256
	for i := range max {
		max[i] = 0xFF
	}
	return &Session{
		p:     p,
		dir:   dir,
		state: &mapState{values: map[domain.Path][]byte{}},
		index: 1,
		caps:  capLookup{max: max},
	}, nil
}

// Close closes the plugin and removes the temporary directory.
func (s *Session) Close() error {
	err := s.p.Close()
	if rmErr := os.RemoveAll(s.dir); err == nil {
		err = rmErr
	}
	return err
}

// execError carries the kind and message of a failed command.
type execError struct {
	kind string
	msg  string
}

func (e *execError) Error() string { return e.msg }

// apply encodes, decodes and applies one input, checks its effects, and
// then applies its StateDiff to the state.
func (s *Session) apply(args codec.InputArgs, sender types.Address, sourceData []byte) (domain.Effects, *execError) {
	payload, err := codec.Input{Args: args}.Encode()
	if err != nil {
		return domain.Effects{}, &execError{"bad_command", err.Error()}
	}
	ctx := domain.InputContext{GlobalInputIndex: s.index, L1Sender: sender, SourceData: sourceData, Caps: s.caps}
	s.index++
	in, err := s.p.DecodeInput(ctx, payload)
	if err != nil {
		return domain.Effects{}, &execError{"halt", err.Error()}
	}
	eff, err := s.p.Apply(s.state, in)
	if err != nil {
		return domain.Effects{}, &execError{"halt", err.Error()}
	}
	var deposit codec.U256
	switch a := args.(type) {
	case codec.CallL2Args:
		deposit = a.DepositAmount
	case codec.DepositArgs:
		deposit = a.DepositAmount
	}
	if err := batch.CheckEffects(eff, deposit); err != nil {
		return domain.Effects{}, &execError{"halt", "the executor refuses the plugin's effects: " + err.Error()}
	}
	for _, d := range eff.StateDiff {
		if d.Value == nil {
			delete(s.state.values, d.Path)
		} else {
			s.state.values[d.Path] = d.Value
		}
	}
	return eff, nil
}

// stage sends DeployStart and every DeployChunk for raw, as sender.
func (s *Session) stage(raw, metadata []byte, sender types.Address) (types.Hash, *execError) {
	codeHash := wasm.CodeHash(raw)
	var chunks [][]byte
	for i := 0; i < len(raw); i += wasm.MaxDeployChunkData {
		chunks = append(chunks, raw[i:min(i+wasm.MaxDeployChunkData, len(raw))])
	}
	if _, e := s.apply(codec.DeployStartArgs{
		CodeHash:     codeHash,
		MetadataHash: sha3(metadata),
		TotalSize:    uint64(len(raw)),
		ChunkCount:   uint32(len(chunks)),
		Metadata:     metadata,
	}, sender, nil); e != nil {
		return codeHash, e
	}
	for i, c := range chunks {
		if _, e := s.apply(codec.DeployChunkArgs{CodeHash: codeHash, ChunkIndex: uint32(i), ChunkHash: sha3(c)}, sender, c); e != nil {
			return codeHash, e
		}
	}
	return codeHash, nil
}

// Exec runs one command line and returns its result value.
func (s *Session) Exec(cmd []byte) any {
	c, err := decodeCommand(cmd)
	if err != nil {
		return failure("bad_command", "%s", err.Error())
	}
	res, e := s.run(c)
	if e != nil {
		return failure(e.kind, "%s", e.msg)
	}
	return res
}

func (s *Session) receipt(eff domain.Effects) (receiptResult, *execError) {
	r, err := receiptOf(eff)
	if err != nil {
		return receiptResult{}, &execError{"halt", err.Error()}
	}
	return receiptResult{OK: true, Receipt: r}, nil
}

func (s *Session) run(c command) (any, *execError) {
	switch c.op {
	case "deploy":
		codeHash, e := s.stage(c.module, c.metadata, c.sender)
		if e != nil {
			return nil, e
		}
		eff, e := s.apply(codec.DeployFinalizeArgs{CodeHash: codeHash}, c.sender, nil)
		if e != nil {
			return nil, e
		}
		res, e := s.receipt(eff)
		if e != nil {
			return nil, e
		}
		if eff.Status == codec.StatusSuccess && len(eff.ReturnData) >= 32 {
			id := eff.ContractID
			var ch types.Hash
			copy(ch[:], eff.ReturnData[:32])
			res.ContractID, res.CodeHash = &id, &ch
		}
		return res, nil
	case "upgrade":
		if _, e := s.stage(c.module, c.metadata, c.sender); e != nil {
			return nil, e
		}
		eff, e := s.apply(codec.UpgradeArgs{ContractID: c.contractID, NewCodeHash: wasm.CodeHash(c.module)}, c.sender, nil)
		if e != nil {
			return nil, e
		}
		return s.receipt(eff)
	case "transferOwnership":
		return s.applyReceipt(codec.TransferOwnershipArgs{ContractID: c.contractID, NewOwner: c.newOwner}, c.sender)
	case "buyStorage":
		return s.applyReceipt(codec.BuyStorageArgs{ContractID: c.contractID, Bytes: c.bytes}, c.sender)
	case "call":
		if e := s.reserveGas(c.plasmaLimit); e != nil {
			return nil, e
		}
		return s.applyReceipt(codec.CallL2Args{
			TargetContract: c.contractID,
			PlasmaLimit:    c.plasmaLimit,
			DepositAsset:   c.asset,
			DepositAmount:  c.amount,
			Payload:        c.payload,
		}, c.sender)
	case "deposit":
		if e := s.reserveGas(c.plasmaLimit); e != nil {
			return nil, e
		}
		return s.applyReceipt(codec.DepositArgs{
			TargetContract: c.contractID,
			PlasmaLimit:    c.plasmaLimit,
			DepositAsset:   c.asset,
			DepositAmount:  c.amount,
		}, c.sender)
	case "setStorage":
		return s.setStorage(c)
	default: // getStorage
		v, ok := s.state.values[keys.Derive(c.contractID, c.localKey)]
		res := valueResult{OK: true}
		if ok {
			h := hexBytes(v)
			if h == nil {
				h = hexBytes{}
			}
			res.Value = &h
		}
		return res, nil
	}
}

func (s *Session) applyReceipt(args codec.InputArgs, sender types.Address) (any, *execError) {
	eff, e := s.apply(args, sender, nil)
	if e != nil {
		return nil, e
	}
	if s.budgeted {
		s.gasLeft -= min(eff.GasUsed, s.gasLeft)
	}
	return s.receipt(eff)
}

// reserveGas refuses a plasmaLimit above the session's remaining gas
// budget. A session without a budget accepts any plasmaLimit.
func (s *Session) reserveGas(plasmaLimit uint64) *execError {
	if s.budgeted && plasmaLimit > s.gasLeft {
		return &execError{"bad_command", fmt.Sprintf("plasmaLimit %d exceeds the %d gas left in this session's budget; lower plasmaLimit, or use zeno-exec run for a larger one", plasmaLimit, s.gasLeft)}
	}
	return nil
}

func leafSize(length int) int64 { return wasm.StateLeafOverhead + int64(length) }

// setStorage writes or deletes one contract leaf and moves the quota
// leaf's used counter by the leaf-size delta, raising purchased to at
// least used.
func (s *Session) setStorage(c command) (any, *execError) {
	bad := func(format string, args ...any) (any, *execError) {
		return nil, &execError{"bad_command", fmt.Sprintf("setStorage: "+format, args...)}
	}
	for _, r := range [][]byte{keys.CodeHash, keys.PreCodeHash, keys.UpgradePolicy, keys.Owner, keys.Quota} {
		if bytes.Equal(c.localKey, r) {
			return bad("localKey: is a reserved key")
		}
	}
	if len(c.localKey) > codec.MaxStateKeySize {
		return bad("localKey: longer than %d bytes", codec.MaxStateKeySize)
	}
	if len(c.value) > codec.MaxStateValueSize {
		return bad("value: longer than %d bytes", codec.MaxStateValueSize)
	}

	path := keys.Derive(c.contractID, c.localKey)
	old, present := s.state.values[path]
	var delta int64
	if !c.valueNull {
		delta += leafSize(len(c.value))
	}
	if present {
		delta -= leafSize(len(old))
	}

	var quota []byte
	if delta != 0 {
		quotaPath := keys.Derive(c.contractID, keys.Quota)
		leaf, qPresent := s.state.values[quotaPath]
		var purchased, used uint64
		if qPresent {
			if len(leaf) != 16 {
				return bad("the quota leaf has width %d, want 16", len(leaf))
			}
			purchased = binary.BigEndian.Uint64(leaf[:8])
			used = binary.BigEndian.Uint64(leaf[8:])
		}
		if delta < 0 {
			if uint64(-delta) > used {
				return bad("value: frees %d bytes but the quota leaf counts %d used", -delta, used)
			}
			used -= uint64(-delta)
		} else {
			if uint64(delta) > math.MaxUint64-used {
				return bad("value: overflows the quota leaf")
			}
			used += uint64(delta)
			if used > purchased {
				purchased = used
			}
		}
		quota = make([]byte, 16)
		binary.BigEndian.PutUint64(quota[:8], purchased)
		binary.BigEndian.PutUint64(quota[8:], used)
		s.state.values[quotaPath] = quota
	}
	switch {
	case c.valueNull:
		delete(s.state.values, path)
	default:
		v := c.value
		if v == nil {
			v = []byte{}
		}
		s.state.values[path] = v
	}
	return okResult{OK: true}, nil
}
