package gateway

import (
	"encoding/json"
	"fmt"
	"math/big"
	"regexp"
	"time"

	"github.com/zenon-network/go-zenon/common/trie"
	"github.com/zenon-network/go-zenon/common/types"

	"github.com/digitalSloth/go-executor/domain"
	"github.com/digitalSloth/go-executor/internal/codec"
	"github.com/digitalSloth/go-executor/internal/simipc"
)

// simulateResult is zeno_simulate's result (EXECUTOR §21.8).
type simulateResult struct {
	DomainID       uint32          `json:"domainId"`
	BatchID        uint64          `json:"batchId"`       // the tag's batch, whose state the input ran on
	PostStateRoot  types.Hash      `json:"postStateRoot"` // that batch's root
	Verified       bool            `json:"verified"`      // always false
	Simulated      bool            `json:"simulated"`     // always true
	InputIndex     uint64          `json:"inputIndex"`
	MomentumHeight uint64          `json:"momentumHeight"`
	MomentumTime   uint64          `json:"momentumTime"`
	Status         string          `json:"status"` // codec.Status.String()
	GasUsed        uint64          `json:"gasUsed"`
	AbortCode      int32           `json:"abortCode"`
	ContractID     types.Hash      `json:"contractId"`
	CodeHash       types.Hash      `json:"codeHash"`
	ReturnData     hexBytes        `json:"returnData"` // revert data on REVERT
	Events         []eventJSON     `json:"events"`
	Outbox         []simOutboxJSON `json:"outbox"`
	StateDiff      []stateDiffJSON `json:"stateDiff"`
}

// simOutboxJSON is an outbox message of a simulation. No batch holds
// it, so its outboxId is null.
type simOutboxJSON struct {
	outboxJSON
	OutboxID *types.Hash `json:"outboxId"` // always null: no batch holds the message
}

// stateDiffJSON is one write of a simulation's state diff.
type stateDiffJSON struct {
	Path  types.Hash `json:"path"`
	Value *hexBytes  `json:"value"` // null deletes; "" is present-empty
}

var decimalAmount = regexp.MustCompile(`^(0|[1-9][0-9]*)$`)

// simInputObjectMsg is the error of an input that is not an object of
// the right members.
const simInputObjectMsg = "invalid params: input must be an object with members kind, sender, targetContract, payload, deposit, plasmaLimit"

// simInputOf decodes zeno_simulate's input object into the L1 sender,
// the canonical input encoding and its plasmaLimit.
func simInputOf(raw json.RawMessage) (sender types.Address, data []byte, plasmaLimit uint64, err error) {
	if !startsWith(raw, '{') || !exactMembers(raw, "kind, sender, targetContract, payload, deposit, plasmaLimit") {
		return sender, nil, 0, &rpcError{codeInvalidParams, simInputObjectMsg}
	}
	var m map[string]json.RawMessage
	if json.Unmarshal(raw, &m) != nil {
		return sender, nil, 0, &rpcError{codeInvalidParams, simInputObjectMsg}
	}
	required := func(name string) (json.RawMessage, error) {
		if !given(m[name]) {
			return nil, &rpcError{codeInvalidParams, fmt.Sprintf("invalid params: input.%s is required", name)}
		}
		return m[name], nil
	}

	kindRaw, err := required("kind")
	if err != nil {
		return sender, nil, 0, err
	}
	var kind string
	if !startsWith(kindRaw, '"') || json.Unmarshal(kindRaw, &kind) != nil || (kind != "CallL2" && kind != "Deposit") {
		return sender, nil, 0, &rpcError{codeInvalidParams, `invalid params: input.kind must be "CallL2" or "Deposit"`}
	}
	senderRaw, err := required("sender")
	if err != nil {
		return sender, nil, 0, err
	}
	if sender, err = addressParam(senderRaw, "input.sender"); err != nil {
		return sender, nil, 0, err
	}
	targetRaw, err := required("targetContract")
	if err != nil {
		return sender, nil, 0, err
	}
	target, err := hashParam(targetRaw, "input.targetContract")
	if err != nil {
		return sender, nil, 0, err
	}

	var payload []byte
	if given(m["payload"]) {
		if kind == "Deposit" {
			return sender, nil, 0, &rpcError{codeInvalidParams, "invalid params: input.payload is for CallL2 only"}
		}
		if payload, err = bytesParam(m["payload"], "input.payload", codec.MaxCallPayload); err != nil {
			return sender, nil, 0, err
		}
	}

	var asset codec.AssetID
	var amount codec.U256
	if given(m["deposit"]) {
		if asset, amount, err = depositOf(m["deposit"]); err != nil {
			return sender, nil, 0, err
		}
	}

	limitRaw, err := required("plasmaLimit")
	if err != nil {
		return sender, nil, 0, err
	}
	plasmaLimit, ok := uintOf(limitRaw)
	if !ok {
		return sender, nil, 0, &rpcError{codeInvalidParams, "invalid params: input.plasmaLimit must be an integer from 0 to 18446744073709551615"}
	}

	var args codec.InputArgs = codec.CallL2Args{TargetContract: target, PlasmaLimit: plasmaLimit, DepositAsset: asset, DepositAmount: amount, Payload: payload}
	if kind == "Deposit" {
		args = codec.DepositArgs{TargetContract: target, PlasmaLimit: plasmaLimit, DepositAsset: asset, DepositAmount: amount}
	}
	data, encErr := codec.Input{Args: args}.Encode()
	if encErr != nil {
		return sender, nil, 0, &rpcError{codeInvalidParams, fmt.Sprintf("invalid params: input does not encode: %v", encErr)}
	}
	return sender, data, plasmaLimit, nil
}

// depositOf decodes the deposit member of an input.
func depositOf(raw json.RawMessage) (codec.AssetID, codec.U256, error) {
	badObject := &rpcError{codeInvalidParams, "invalid params: input.deposit must be an object with members asset, amount"}
	var m map[string]json.RawMessage
	if !startsWith(raw, '{') || !exactMembers(raw, "asset, amount") || json.Unmarshal(raw, &m) != nil || !given(m["asset"]) || !given(m["amount"]) {
		return codec.AssetID{}, codec.U256{}, badObject
	}
	asset, err := hashParam(m["asset"], "input.deposit.asset")
	if err != nil {
		return codec.AssetID{}, codec.U256{}, err
	}
	badAmount := &rpcError{codeInvalidParams, "invalid params: input.deposit.amount must be a decimal string below 2^256"}
	var s string
	if !startsWith(m["amount"], '"') || json.Unmarshal(m["amount"], &s) != nil || !decimalAmount.MatchString(s) {
		return codec.AssetID{}, codec.U256{}, badAmount
	}
	n, ok := new(big.Int).SetString(s, 10)
	if !ok || n.BitLen() > 256 {
		return codec.AssetID{}, codec.U256{}, badAmount
	}
	amount, err := codec.U256FromBig(n)
	if err != nil {
		return codec.AssetID{}, codec.U256{}, badAmount
	}
	return codec.AssetID(asset), amount, nil
}

// simReadsMsg is the error of a simulation that read past its state
// read budget.
const simReadsMsg = "simulation exceeded 65,536 state reads"

// simulate serves zeno_simulate(domainId, input[, tag]).
func (s *Server) simulate(params []json.RawMessage, b *budget) (any, error) {
	if err := arity(params, 2, 3); err != nil {
		return nil, err
	}
	c, err := s.domainOf(params[0])
	if err != nil {
		return nil, err
	}
	p := s.sims[c.cfg.DomainID]
	if p == nil {
		return nil, &rpcError{codeSimulation, "simulation is disabled"}
	}
	sender, data, plasmaLimit, err := simInputOf(params[1])
	if err != nil {
		return nil, err
	}
	if plasmaLimit > s.sim.MaxGas {
		return nil, &rpcError{codeSimulation, fmt.Sprintf("plasmaLimit %d exceeds simulation.maxGas %d", plasmaLimit, s.sim.MaxGas)}
	}
	t, err := tagAt(params, 2)
	if err != nil {
		return nil, err
	}
	r, v, bv, err := s.readBatch(c, t)
	if err != nil {
		return nil, err
	}
	defer v.Release()
	if err := stateAt(r); err != nil {
		return nil, err
	}
	view := c.settlementView()
	if view == nil {
		return nil, &rpcError{codeNotAvailable, "simulation is not available until the gateway's first poll reads Settlement; retry later"}
	}

	height, unixTime := view.Momentum()
	code, _ := s.readerOf(c).(domain.CodeSource)
	j := &simJob{
		req: simipc.Simulate{
			GlobalInputIndex: r.head.Pending.LastInputSeq + 1,
			MomentumHeight:   height,
			MomentumTime:     unixTime,
			L1Sender:         sender,
			InputData:        data,
		},
		version: r.version,
		code:    code,
		// The code fetch gets the most a zeno_read module view can
		// have, so a module that view serves reaches the simulator.
		rc: &readContext{
			v:       v,
			b:       &budget{scan: s.scanKeys, scanBytes: s.scanBytes, proofs: maxBodyProofPaths, ctx: b.ctx},
			version: r.version,
			hi:      bv.LastInputSeq,
			seen:    map[trie.Path]bool{},
		},
		view: view,
		done: make(chan simOutcome, 1),
	}

	wait := time.NewTimer(s.simTiming.queueWait)
	defer wait.Stop()
	select {
	case p.jobs <- j:
	case <-wait.C:
		return nil, &rpcError{codeSimulation, "no simulator is free; retry later"}
	case <-b.ctx.Done():
		return nil, &rpcError{codeSimulation, "the request ended before a simulator was free"}
	case <-p.ctx.Done():
		return nil, &rpcError{codeSimulation, "the gateway is shutting down"}
	}

	// The slot always answers, within timeoutMs plus at most one inline
	// code fetch and one reap.
	out := <-j.done
	if out.err != nil {
		return nil, out.err
	}
	return simResultOf(c, r, j, out)
}

// simResultOf builds zeno_simulate's result from a finished simulation.
func simResultOf(c *Chain, r tagRead, j *simJob, out simOutcome) (simulateResult, error) {
	res := simulateResult{
		DomainID:       c.cfg.DomainID,
		BatchID:        r.id,
		PostStateRoot:  r.root,
		Simulated:      true,
		InputIndex:     j.req.GlobalInputIndex,
		MomentumHeight: j.req.MomentumHeight,
		MomentumTime:   j.req.MomentumTime,
		Status:         out.res.Status.String(),
		GasUsed:        out.res.GasUsed,
		AbortCode:      out.res.AbortCode,
		ContractID:     out.res.ContractID,
		CodeHash:       out.res.CodeHash,
		ReturnData:     out.exec.ReturnData,
		Events:         make([]eventJSON, 0, len(out.exec.Events)),
		Outbox:         make([]simOutboxJSON, 0, len(out.exec.Outbox)),
		StateDiff:      make([]stateDiffJSON, 0, len(out.exec.StateDiff)),
	}
	for _, ev := range out.exec.Events {
		res.Events = append(res.Events, eventJSONOf(ev))
	}
	for _, m := range out.exec.Outbox {
		o, err := outboxJSONOf(m)
		if err != nil {
			return simulateResult{}, err
		}
		res.Outbox = append(res.Outbox, simOutboxJSON{outboxJSON: o})
	}
	for _, e := range out.exec.StateDiff {
		d := stateDiffJSON{Path: types.Hash(e.Key)}
		if e.Value != nil {
			v := hexBytes(e.Value)
			d.Value = &v
		}
		res.StateDiff = append(res.StateDiff, d)
	}
	return res, nil
}
