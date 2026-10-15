package gateway

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"math/big"
	"slices"
	"time"

	"github.com/zenon-network/go-zenon/common/crypto"
	"github.com/zenon-network/go-zenon/common/trie"
	"github.com/zenon-network/go-zenon/common/types"

	"github.com/digitalSloth/go-executor/domain"
	"github.com/digitalSloth/go-executor/internal/codec"
	"github.com/digitalSloth/go-executor/internal/recovery"
)

// method is one JSON-RPC method. params are its positional
// parameters and b its share of the request body's budgets. It
// returns the result, which the server encodes, or a json.RawMessage
// the server writes unchanged. An *rpcError reaches the client; any
// other error is internal.
type method func(s *Server, params []json.RawMessage, b *budget) (any, error)

// methodTable returns the methods this build serves.
func methodTable() map[string]method {
	return map[string]method{
		"zeno_chainInfo":         (*Server).chainInfo,
		"zeno_getHead":           (*Server).getHead,
		"zeno_getBatch":          (*Server).getBatch,
		"zeno_getReceipt":        (*Server).getReceipt,
		"zeno_getStateAt":        (*Server).getStateAt,
		"zeno_getProof":          (*Server).getProof,
		"zeno_getInputs":         (*Server).getInputs,
		"zeno_getEvents":         (*Server).getEvents,
		"zeno_getOutbox":         (*Server).getOutbox,
		"zeno_getBundle":         (*Server).getBundle,
		"zeno_getReceiptSidecar": (*Server).getReceiptSidecar,
		"zeno_getOutboxProof":    (*Server).getOutboxProof,
		"zeno_subscribe":         (*Server).subscribe,
		"zeno_unsubscribe":       (*Server).unsubscribe,
		"zeno_read":              (*Server).read,
		"zeno_simulate":          (*Server).simulate,
	}
}

// arity returns an invalid-params error unless params has min to max
// entries.
func arity(params []json.RawMessage, min, max int) error {
	if len(params) < min || len(params) > max {
		return &rpcError{codeInvalidParams, fmt.Sprintf("invalid params: want %d to %d parameters, got %d", min, max, len(params))}
	}
	return nil
}

// domainOf decodes raw as a domain id and returns its chain.
func (s *Server) domainOf(raw json.RawMessage) (*Chain, error) {
	var id uint32
	if string(raw) == "null" || json.Unmarshal(raw, &id) != nil {
		return nil, &rpcError{codeInvalidParams, "invalid params: domainId must be an integer from 0 to 4294967295"}
	}
	c, ok := s.chains[id]
	if !ok {
		return nil, &rpcError{codeUnknownDomain, fmt.Sprintf("unknown domain %d", id)}
	}
	return c, nil
}

// tagHead is one tag of zeno_getHead.
type tagHead struct {
	BatchID       uint64     `json:"batchId"`
	LastInputSeq  uint64     `json:"lastInputSeq"`
	PostStateRoot types.Hash `json:"postStateRoot"`
	Verified      bool       `json:"verified"`
}

// headResult is zeno_getHead's result. A tag no batch has reached is
// null.
type headResult struct {
	DomainID       uint32   `json:"domainId"`
	Status         Status   `json:"status"`
	InconsistentAt *uint64  `json:"inconsistentAt"`
	Pending        *tagHead `json:"pending"`
	Latest         *tagHead `json:"latest"`
	Finalized      *tagHead `json:"finalized"`
}

// chainInfoResult is zeno_chainInfo's result.
type chainInfoResult struct {
	DomainID       uint32           `json:"domainId"`
	NetworkName    string           `json:"networkName"`
	ChainID        any              `json:"chain_id"`
	RuntimeKind    *string          `json:"runtimeKind"`
	StfSpecHash    *types.Hash      `json:"stfSpecHash"`
	Status         Status           `json:"status"`
	InconsistentAt *uint64          `json:"inconsistentAt"`
	Profiles       []domain.Profile `json:"profiles"`
	Views          []domain.View    `json:"views"`
}

// headTag returns b as a tag of h, nil when b is nil.
func headTag(b *BatchHead, h Head) *tagHead {
	if b == nil {
		return nil
	}
	return &tagHead{
		BatchID:       b.BatchID,
		LastInputSeq:  b.LastInputSeq,
		PostStateRoot: b.PostStateRoot,
		Verified:      isVerified(h, b.BatchID),
	}
}

func (s *Server) getHead(params []json.RawMessage, _ *budget) (any, error) {
	if err := arity(params, 1, 1); err != nil {
		return nil, err
	}
	c, err := s.domainOf(params[0])
	if err != nil {
		return nil, err
	}
	return headResultOf(c, c.Head()), nil
}

// headResultOf returns zeno_getHead's result for c at head h.
func headResultOf(c *Chain, h Head) headResult {
	pending := headTag(h.Pending, h)
	if c.remote() {
		pending = nil
	}
	return headResult{
		DomainID:       c.cfg.DomainID,
		Status:         h.Status,
		InconsistentAt: h.InconsistentAt,
		Pending:        pending,
		Latest:         headTag(h.Latest, h),
		Finalized:      headTag(h.Finalized, h),
	}
}

func (s *Server) chainInfo(params []json.RawMessage, _ *budget) (any, error) {
	if err := arity(params, 1, 1); err != nil {
		return nil, err
	}
	c, err := s.domainOf(params[0])
	if err != nil {
		return nil, err
	}
	h := c.Head()
	res := chainInfoResult{
		DomainID:       c.cfg.DomainID,
		NetworkName:    s.networkName,
		Status:         h.Status,
		InconsistentAt: h.InconsistentAt,
		Profiles:       []domain.Profile{},
		Views:          []domain.View{},
	}
	if rd := s.readerOf(c); rd != nil {
		res.ChainID, res.Profiles, res.Views = rd.ChainID(), rd.Profiles(), rd.Views()
	}
	if rec := c.domainRecord(); rec != nil {
		kind := "WASM"
		if rec.RuntimeKind != 0 {
			kind = fmt.Sprint(rec.RuntimeKind)
		}
		res.RuntimeKind = &kind
		res.StfSpecHash = &rec.StfSpecHash
	}
	return res, nil
}

// tag is a parsed tag: name is "pending", "latest" or "finalized",
// or empty for the batch id in id.
type tag struct {
	name string
	id   uint64
}

var errBadTag = &rpcError{codeInvalidParams, `invalid params: tag must be "pending", "latest", "finalized" or a batch id`}

// parseTag decodes raw as a tag.
func parseTag(raw json.RawMessage) (tag, error) {
	switch {
	case startsWith(raw, '"'):
		var name string
		if json.Unmarshal(raw, &name) != nil || (name != "pending" && name != "latest" && name != "finalized") {
			return tag{}, errBadTag
		}
		return tag{name: name}, nil
	case len(raw) > 0 && raw[0] >= '0' && raw[0] <= '9':
		var id uint64
		if json.Unmarshal(raw, &id) != nil {
			return tag{}, errBadTag
		}
		return tag{id: id}, nil
	}
	return tag{}, errBadTag
}

// tagAt returns params[i] as a tag. The tag is optional: a params
// array that ends before i, or an explicit null at i, leaves it out,
// and a left-out tag is "latest". splitArray hands each element over
// without surrounding whitespace, so the null test is exact.
func tagAt(params []json.RawMessage, i int) (tag, error) {
	if i >= len(params) || string(params[i]) == "null" {
		return tag{name: "latest"}, nil
	}
	return parseTag(params[i])
}

// hashParam decodes raw as 32 bytes of hex; name names the parameter
// in the error.
func hashParam(raw json.RawMessage, name string) (types.Hash, error) {
	var h types.Hash
	if !startsWith(raw, '"') || json.Unmarshal(raw, &h) != nil {
		return types.Hash{}, &rpcError{codeInvalidParams, fmt.Sprintf("invalid params: %s must be 64 hex characters", name)}
	}
	return h, nil
}

// pathsParam decodes raw as an array of 1 to maxProofPaths paths,
// decoding at most maxProofPaths of them.
func pathsParam(raw json.RawMessage) ([]trie.Path, error) {
	bad := &rpcError{codeInvalidParams, fmt.Sprintf("invalid params: paths must be an array of 1 to %d paths", maxProofPaths)}
	if !startsWith(raw, '[') {
		return nil, bad
	}
	elems, over, err := splitArray(raw, maxProofPaths)
	if err != nil || over || len(elems) == 0 {
		return nil, bad
	}
	paths := make([]trie.Path, len(elems))
	for i, e := range elems {
		h, err := hashParam(e, fmt.Sprintf("paths[%d]", i))
		if err != nil {
			return nil, err
		}
		paths[i] = trie.Path(h)
	}
	return paths, nil
}

// isVerified reports whether batch id passed the checks against
// Settlement: it is at or below h's latest batch and below the batch
// at which h became INCONSISTENT.
func isVerified(h Head, id uint64) bool {
	return h.Latest != nil && id <= h.Latest.BatchID && (h.InconsistentAt == nil || id < *h.InconsistentAt)
}

// reached names the furthest tag of h that batch id has reached.
func reached(h Head, id uint64) string {
	switch {
	case h.Finalized != nil && id <= h.Finalized.BatchID:
		return "finalized"
	case h.Latest != nil && id <= h.Latest.BatchID:
		return "latest"
	}
	return "pending"
}

// noPendingMsg is the error of a "pending" tag on a REMOTE domain.
const noPendingMsg = `"pending" is not served for a REMOTE domain; use "latest" or "finalized"`

// statusErr returns the error for r, resolved from t, that the domain
// cannot serve, or nil.
func statusErr(r tagRead, t tag) error {
	if r.noPending {
		return &rpcError{codeNotAvailable, noPendingMsg}
	}
	if at := r.head.InconsistentAt; at != nil && (r.ok || t.name == "") && r.id >= *at {
		return &rpcError{codeInconsistent, fmt.Sprintf("the domain is INCONSISTENT from batch %d", *at)}
	}
	if !r.ok {
		if t.name != "" {
			return &rpcError{codeNotFound, fmt.Sprintf("no batch is %s yet", t.name)}
		}
		return &rpcError{codeNotFound, fmt.Sprintf("batch %d not found", t.id)}
	}
	return nil
}

// heldBatch returns the batch r names from v. It returns -32004 when
// v no longer holds the batch r's head checked, or when v predates
// the batch's finalization, and index damage when v's finalized
// marker covers the batch but v lacks it.
func heldBatch(v *indexView, r tagRead) (batchValue, error) {
	gone := &rpcError{codeNotFound, fmt.Sprintf("batch %d is no longer available", r.id)}
	if !r.above {
		f, hasF, err := v.finalized()
		if err != nil {
			return batchValue{}, err
		}
		if !hasF || f < r.id {
			return batchValue{}, gone
		}
	}
	bv, has, err := v.batchAt(r.id)
	if err != nil {
		return batchValue{}, err
	}
	if r.above {
		if !has || bv.DAHash != r.want.daHash || bv.ReceiptRoot != r.want.receiptRoot {
			return batchValue{}, gone
		}
		return bv, nil
	}
	if !has {
		return batchValue{}, damaged(ikey(pfxBatch, be64(r.id)), errors.New("missing at or below the finalized batch"))
	}
	return bv, nil
}

// readBatch resolves t on c, opens one index view and returns the
// resolved batch from it. The caller must Release the view.
func (s *Server) readBatch(c *Chain, t tag) (tagRead, *indexView, batchValue, error) {
	r := c.readTag(t)
	if err := statusErr(r, t); err != nil {
		return r, nil, batchValue{}, err
	}
	v, err := c.cfg.Index.view()
	if err != nil {
		return r, nil, batchValue{}, err
	}
	bv, err := heldBatch(v, r)
	if err != nil {
		v.Release()
		return r, nil, batchValue{}, err
	}
	return r, v, bv, nil
}

// hexBytes encodes as a lowercase hex JSON string.
type hexBytes []byte

func (b hexBytes) MarshalText() ([]byte, error) { return []byte(hex.EncodeToString(b)), nil }

// receiptJSON is one receipt (SPEC §27.7).
type receiptJSON struct {
	InputIndex    uint64     `json:"inputIndex"`
	InputHash     types.Hash `json:"inputHash"`
	BatchID       uint64     `json:"batchId"`
	ContractID    types.Hash `json:"contractId"`
	CodeHash      types.Hash `json:"codeHash"`
	Status        string     `json:"status"`
	GasUsed       uint64     `json:"gasUsed"`
	AbortCode     int32      `json:"abortCode"`
	StateDiffHash types.Hash `json:"stateDiffHash"`
	ReturnHash    types.Hash `json:"returnHash"`
	EventRoot     types.Hash `json:"eventRoot"`
	EventCount    uint32     `json:"eventCount"`
	OutboxRoot    types.Hash `json:"outboxRoot"`
	OutboxCount   uint32     `json:"outboxCount"`
}

// eventJSON is one event (SPEC §27.3).
type eventJSON struct {
	ContractID types.Hash   `json:"contractId"`
	InputIndex uint64       `json:"inputIndex"`
	EventIndex uint32       `json:"eventIndex"`
	Topics     []types.Hash `json:"topics"`
	Data       hexBytes     `json:"data"`
}

// outboxJSON is one outbox message (SPEC §27.4) with its outboxId.
// The target fields of the other kind are null.
type outboxJSON struct {
	OutboxID       types.Hash     `json:"outboxId"`
	SourceContract types.Hash     `json:"sourceContract"`
	InputIndex     uint64         `json:"inputIndex"`
	OutboxIndex    uint32         `json:"outboxIndex"`
	Kind           string         `json:"kind"`
	TargetContract *types.Hash    `json:"targetContract"`
	Recipient      *types.Address `json:"recipient"`
	AssetID        *hexBytes      `json:"assetId"`
	Amount         *string        `json:"amount"`
	Payload        hexBytes       `json:"payload"`
}

func receiptJSONOf(rc codec.Receipt) receiptJSON {
	return receiptJSON{
		InputIndex:    rc.InputIndex,
		InputHash:     rc.InputHash,
		BatchID:       rc.BatchID,
		ContractID:    rc.ContractID,
		CodeHash:      rc.CodeHash,
		Status:        rc.Status.String(),
		GasUsed:       rc.GasUsed,
		AbortCode:     rc.AbortCode,
		StateDiffHash: rc.StateDiffHash,
		ReturnHash:    rc.ReturnHash,
		EventRoot:     rc.EventRoot,
		EventCount:    rc.EventCount,
		OutboxRoot:    rc.OutboxRoot,
		OutboxCount:   rc.OutboxCount,
	}
}

func eventOf(enc []byte) (eventJSON, error) {
	ev, err := codec.DecodeEvent(enc)
	if err != nil {
		return eventJSON{}, err
	}
	return eventJSONOf(ev), nil
}

// eventJSONOf returns ev's JSON form, with nil topics as [].
func eventJSONOf(ev codec.Event) eventJSON {
	topics := ev.Topics
	if topics == nil {
		topics = []types.Hash{}
	}
	return eventJSON{
		ContractID: ev.ContractID,
		InputIndex: ev.InputIndex,
		EventIndex: ev.EventIndex,
		Topics:     topics,
		Data:       ev.Data,
	}
}

func outboxOf(domainID uint32, batchID uint64, enc []byte) (outboxJSON, error) {
	m, err := codec.DecodeOutboxMessage(enc)
	if err != nil {
		return outboxJSON{}, err
	}
	out, err := outboxJSONOf(m)
	if err != nil {
		return outboxJSON{}, err
	}
	out.OutboxID = codec.OutboxID(domainID, batchID, m.InputIndex, m.OutboxIndex)
	return out, nil
}

// outboxJSONOf returns m's JSON form with OutboxID left zero.
func outboxJSONOf(m codec.OutboxMessage) (outboxJSON, error) {
	out := outboxJSON{
		SourceContract: m.SourceContract,
		InputIndex:     m.InputIndex,
		OutboxIndex:    m.OutboxIndex,
		Payload:        m.Payload,
	}
	switch t := m.Target.(type) {
	case codec.L2Delivery:
		out.Kind = "L2_DELIVERY"
		out.TargetContract = &t.TargetContract
	case codec.L1Withdrawal:
		out.Kind = "L1_WITHDRAWAL"
		asset := hexBytes(t.Asset[:])
		amount := new(big.Int).SetBytes(t.Amount[:]).String()
		out.Recipient, out.AssetID, out.Amount = &t.RecipientL1, &asset, &amount
	default:
		return outboxJSON{}, fmt.Errorf("outbox message of unknown kind %d", m.Target.Kind())
	}
	return out, nil
}

// batchResult is zeno_getBatch's result.
type batchResult struct {
	DomainID      uint32      `json:"domainId"`
	BatchID       uint64      `json:"batchId"`
	Verified      bool        `json:"verified"`
	FirstInputSeq uint64      `json:"firstInputSeq"`
	LastInputSeq  uint64      `json:"lastInputSeq"`
	PreStateRoot  types.Hash  `json:"preStateRoot"`
	PostStateRoot types.Hash  `json:"postStateRoot"`
	InputRoot     types.Hash  `json:"inputRoot"`
	ReceiptRoot   types.Hash  `json:"receiptRoot"`
	EventRoot     types.Hash  `json:"eventRoot"`
	OutboxRoot    types.Hash  `json:"outboxRoot"`
	DAHash        types.Hash  `json:"daHash"`
	Inputs        []batchItem `json:"inputs"`
}

// batchItem is one input of zeno_getBatch.
type batchItem struct {
	GlobalInputIndex uint64     `json:"globalInputIndex"`
	SendHash         types.Hash `json:"sendHash"`
	Status           string     `json:"status"`
}

// receiptResult is zeno_getReceipt's result. Input is the domain
// reader's decoded input, or null.
type receiptResult struct {
	DomainID         uint32        `json:"domainId"`
	BatchID          uint64        `json:"batchId"`
	Verified         bool          `json:"verified"`
	Tag              string        `json:"tag"`
	GlobalInputIndex uint64        `json:"globalInputIndex"`
	SendHash         types.Hash    `json:"sendHash"`
	Sender           types.Address `json:"sender"`
	Receipt          receiptJSON   `json:"receipt"`
	ReturnData       hexBytes      `json:"returnData"`
	Events           []eventJSON   `json:"events"`
	Outbox           []outboxJSON  `json:"outbox"`
	Input            any           `json:"input"`
}

// stateResult is zeno_getStateAt's result. Value is null when the
// path is absent and "" when it is present and empty.
type stateResult struct {
	DomainID      uint32     `json:"domainId"`
	BatchID       uint64     `json:"batchId"`
	Verified      bool       `json:"verified"`
	PostStateRoot types.Hash `json:"postStateRoot"`
	Path          types.Hash `json:"path"`
	Present       bool       `json:"present"`
	Value         *hexBytes  `json:"value"`
}

// proofResult is zeno_getProof's result.
type proofResult struct {
	DomainID      uint32      `json:"domainId"`
	BatchID       uint64      `json:"batchId"`
	Verified      bool        `json:"verified"`
	PostStateRoot types.Hash  `json:"postStateRoot"`
	Proofs        []pathProof `json:"proofs"`
}

// pathProof is one path of zeno_getProof: its value as in stateResult
// and its path-native SMT proof against PostStateRoot.
type pathProof struct {
	Path    types.Hash `json:"path"`
	Present bool       `json:"present"`
	Value   *hexBytes  `json:"value"`
	Proof   hexBytes   `json:"proof"`
}

func (s *Server) getBatch(params []json.RawMessage, _ *budget) (any, error) {
	if err := arity(params, 1, 2); err != nil {
		return nil, err
	}
	c, err := s.domainOf(params[0])
	if err != nil {
		return nil, err
	}
	t, err := tagAt(params, 1)
	if err != nil {
		return nil, err
	}
	r, v, bv, err := s.readBatch(c, t)
	if err != nil {
		return nil, err
	}
	defer v.Release()
	items := make([]batchItem, 0, len(bv.Inputs))
	for _, g := range bv.Inputs {
		key := ikey(pfxInput, be64(g))
		in, ok, err := v.inputAt(g)
		if err != nil {
			return nil, err
		}
		if !ok {
			return nil, damaged(key, errors.New("missing inside an indexed batch"))
		}
		rc, err := codec.DecodeReceipt(in.Receipt)
		if err != nil {
			return nil, damaged(key, err)
		}
		items = append(items, batchItem{g, in.SendHash, rc.Status.String()})
	}
	return batchResult{
		DomainID:      c.cfg.DomainID,
		BatchID:       r.id,
		Verified:      isVerified(r.head, r.id),
		FirstInputSeq: bv.FirstInputSeq,
		LastInputSeq:  bv.LastInputSeq,
		PreStateRoot:  bv.PreStateRoot,
		PostStateRoot: bv.PostStateRoot,
		InputRoot:     bv.InputRoot,
		ReceiptRoot:   bv.ReceiptRoot,
		EventRoot:     bv.EventRoot,
		OutboxRoot:    bv.OutboxRoot,
		DAHash:        bv.DAHash,
		Inputs:        items,
	}, nil
}

func (s *Server) getReceipt(params []json.RawMessage, _ *budget) (any, error) {
	if err := arity(params, 1, 1); err != nil {
		return nil, err
	}
	h, err := hashParam(params[0], "sendHash")
	if err != nil {
		return nil, err
	}
	for _, c := range s.order {
		res, err := s.receiptIn(c, h)
		if err != nil {
			return nil, err
		}
		if res != nil {
			return res, nil
		}
	}
	return nil, &rpcError{codeNotFound, fmt.Sprintf("send %s not found", h)}
}

// receiptIn returns the receipt of send h from c, or nil when c's
// index holds no visible input for it.
func (s *Server) receiptIn(c *Chain, h types.Hash) (*receiptResult, error) {
	v, err := c.cfg.Index.view()
	if err != nil {
		return nil, err
	}
	defer v.Release()
	return s.receiptFrom(c, v, h)
}

// receiptFrom is receiptIn over the view v.
func (s *Server) receiptFrom(c *Chain, v *indexView, h types.Hash) (*receiptResult, error) {
	g, ok, err := v.inputBySendHash(h)
	if err != nil || !ok {
		return nil, err
	}
	in, ok, err := v.inputAt(g)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, damaged(ikey(pfxInput, be64(g)), errors.New("missing for an indexed send hash"))
	}
	r, _, ok, err := visibleBatch(c, v, in.BatchID)
	if err != nil || !ok {
		return nil, err
	}
	rc, err := codec.DecodeReceipt(in.Receipt)
	if err != nil {
		return nil, damaged(ikey(pfxInput, be64(g)), err)
	}
	encEvents, err := v.eventsOf(g, rc.EventCount)
	if err != nil {
		return nil, err
	}
	encOutbox, err := v.outboxOf(g, rc.OutboxCount)
	if err != nil {
		return nil, err
	}
	events := make([]eventJSON, 0, len(encEvents))
	for j, enc := range encEvents {
		ev, err := eventOf(enc)
		if err != nil {
			return nil, damaged(ikey(pfxEvent, be64(g), be32(uint32(j))), err)
		}
		events = append(events, ev)
	}
	outbox := make([]outboxJSON, 0, len(encOutbox))
	for j, enc := range encOutbox {
		m, err := outboxOf(c.cfg.DomainID, r.id, enc)
		if err != nil {
			return nil, damaged(ikey(pfxOutbox, be64(g), be32(uint32(j))), err)
		}
		outbox = append(outbox, m)
	}
	return &receiptResult{
		DomainID:         c.cfg.DomainID,
		BatchID:          r.id,
		Verified:         isVerified(r.head, r.id),
		Tag:              reached(r.head, r.id),
		GlobalInputIndex: g,
		SendHash:         in.SendHash,
		Sender:           in.Sender,
		Receipt:          receiptJSONOf(rc),
		ReturnData:       in.ReturnData,
		Events:           events,
		Outbox:           outbox,
		Input:            describe(s.readerOf(c), in.InputData),
	}, nil
}

// stateAt returns nil when the state of the batch r names is kept, and
// the -32002 error otherwise. getStateAt and getProof share it.
func stateAt(r tagRead) error {
	if r.kept {
		return nil
	}
	return &rpcError{codeNotAvailable, fmt.Sprintf("batch %d's state is no longer kept; state is kept from the finalized batch %d", r.id, r.head.Finalized.BatchID)}
}

func (s *Server) getStateAt(params []json.RawMessage, _ *budget) (any, error) {
	if err := arity(params, 2, 3); err != nil {
		return nil, err
	}
	c, err := s.domainOf(params[0])
	if err != nil {
		return nil, err
	}
	path, err := hashParam(params[1], "path")
	if err != nil {
		return nil, err
	}
	t, err := tagAt(params, 2)
	if err != nil {
		return nil, err
	}
	r := c.readTag(t)
	if err := statusErr(r, t); err != nil {
		return nil, err
	}
	if err := stateAt(r); err != nil {
		return nil, err
	}
	present, value, _ := r.version.Prove(trie.Path(path))
	res := stateResult{
		DomainID:      c.cfg.DomainID,
		BatchID:       r.id,
		Verified:      isVerified(r.head, r.id),
		PostStateRoot: r.root,
		Path:          path,
		Present:       present,
	}
	if present {
		hb := hexBytes(value)
		res.Value = &hb
	}
	return res, nil
}

func (s *Server) getProof(params []json.RawMessage, _ *budget) (any, error) {
	if err := arity(params, 2, 3); err != nil {
		return nil, err
	}
	c, err := s.domainOf(params[0])
	if err != nil {
		return nil, err
	}
	paths, err := pathsParam(params[1])
	if err != nil {
		return nil, err
	}
	t, err := tagAt(params, 2)
	if err != nil {
		return nil, err
	}
	r := c.readTag(t)
	if err := statusErr(r, t); err != nil {
		return nil, err
	}
	if err := stateAt(r); err != nil {
		return nil, err
	}
	proofs := make([]pathProof, 0, len(paths))
	for _, p := range paths {
		present, value, proof := r.version.Prove(p)
		pp := pathProof{Path: types.Hash(p), Present: present, Proof: proof}
		if present {
			hb := hexBytes(value)
			pp.Value = &hb
		}
		proofs = append(proofs, pp)
	}
	return proofResult{
		DomainID:      c.cfg.DomainID,
		BatchID:       r.id,
		Verified:      isVerified(r.head, r.id),
		PostStateRoot: r.root,
		Proofs:        proofs,
	}, nil
}

func (s *Server) getBundle(params []json.RawMessage, b *budget) (any, error) {
	if err := arity(params, 1, 1); err != nil {
		return nil, err
	}
	h, err := hashParam(params[0], "daHash")
	if err != nil {
		return nil, err
	}
	c, r, _, err := s.findDAHash(h)
	if err != nil {
		return nil, err
	}
	prefix := rawPrefix(c, r, h, "bundle")
	ceiling := int64(min(maxBundleBytes, max(0, (b.left-len(prefix)-2)/2)))
	data, err := c.cfg.Source.bundleWithin(h, ceiling)
	switch {
	case errors.Is(err, recovery.ErrTooLarge):
		return nil, &rpcError{codeInvalidRequest, fmt.Sprintf("bundle %s is larger than this response can carry (%d bytes); fetch GET /bundles/<daHash>", h, ceiling)}
	case errors.Is(err, fs.ErrNotExist):
		return nil, &rpcError{codeNotFound, fmt.Sprintf("bundle %s not found", h)}
	case err != nil:
		return nil, err
	}
	if !bytes.Equal(crypto.Hash(data), h[:]) {
		return nil, fmt.Errorf("bundle %s in the executor's directory no longer hashes to its daHash", h)
	}
	return rawResult(prefix, data), nil
}

func (s *Server) getReceiptSidecar(params []json.RawMessage, _ *budget) (any, error) {
	if err := arity(params, 1, 1); err != nil {
		return nil, err
	}
	h, err := hashParam(params[0], "daHash")
	if err != nil {
		return nil, err
	}
	c, r, bv, err := s.findDAHash(h)
	if err != nil {
		return nil, err
	}
	data, err := c.cfg.Source.Receipts(h)
	if errors.Is(err, fs.ErrNotExist) {
		return nil, &rpcError{codeNotFound, fmt.Sprintf("receipt sidecar %s not found", h)}
	}
	if err != nil {
		return nil, err
	}
	_, leaves, err := codec.DecodeReceiptSidecar(data)
	if err != nil || codec.MerkleRoot(leaves) != bv.ReceiptRoot {
		return nil, fmt.Errorf("receipt sidecar %s in the executor's directory no longer matches receiptRoot %s", h, bv.ReceiptRoot)
	}
	return rawResult(rawPrefix(c, r, h, "sidecar"), data), nil
}

// findDAHash returns the first chain, in configuration order, whose
// head shows a batch the index holds for daHash h, with the resolved
// batch.
func (s *Server) findDAHash(h types.Hash) (*Chain, tagRead, batchValue, error) {
	for _, c := range s.order {
		r, bv, ok, err := batchWithDAHash(c, h)
		if err != nil {
			return nil, tagRead{}, batchValue{}, err
		}
		if ok {
			return c, r, bv, nil
		}
	}
	return nil, tagRead{}, batchValue{}, &rpcError{codeNotFound, fmt.Sprintf("daHash %s not found", h)}
}

// batchWithDAHash is findDAHash for one chain; ok is false when c
// shows no batch for h.
func batchWithDAHash(c *Chain, h types.Hash) (tagRead, batchValue, bool, error) {
	v, err := c.cfg.Index.view()
	if err != nil {
		return tagRead{}, batchValue{}, false, err
	}
	defer v.Release()
	return batchFrom(c, v, h)
}

// batchFrom is batchWithDAHash over the view v.
func batchFrom(c *Chain, v *indexView, h types.Hash) (tagRead, batchValue, bool, error) {
	id, found, err := v.batchByDAHash(h)
	if err != nil || !found {
		return tagRead{}, batchValue{}, false, err
	}
	return visibleBatch(c, v, id)
}

// visibleBatch resolves batch id on c and returns it from v. ok is
// false when c shows no such batch or v no longer holds it, which a
// search skips; a batch the domain cannot serve as INCONSISTENT is an
// error.
func visibleBatch(c *Chain, v *indexView, id uint64) (r tagRead, bv batchValue, ok bool, err error) {
	t := tag{id: id}
	r = c.readTag(t)
	if err := statusErr(r, t); err != nil {
		if re := (*rpcError)(nil); errors.As(err, &re) && re.Code == codeInconsistent {
			return r, bv, false, err
		}
		return r, bv, false, nil
	}
	bv, err = heldBatch(v, r)
	if err != nil {
		if re := (*rpcError)(nil); errors.As(err, &re) {
			return r, bv, false, nil
		}
		return r, bv, false, err
	}
	return r, bv, true, nil
}

// rawPrefix returns a raw file result's bytes before the file's hex.
func rawPrefix(c *Chain, r tagRead, h types.Hash, key string) []byte {
	return fmt.Appendf(nil, `{"domainId":%d,"batchId":%d,"verified":%t,"daHash":"%s","%s":"`,
		c.cfg.DomainID, r.id, isVerified(r.head, r.id), h, key)
}

// rawResult returns prefix, data as lowercase hex and the closing
// quote and brace, in one allocation.
func rawResult(prefix, data []byte) json.RawMessage {
	out := make([]byte, len(prefix)+2*len(data)+2)
	copy(out, prefix)
	hex.Encode(out[len(prefix):], data)
	copy(out[len(prefix)+2*len(data):], `"}`)
	return out
}

// maxBatchOutbox is the most outbox messages one batch can hold.
const maxBatchOutbox = codec.MaxBatchInputs * codec.MaxOutboxPerResult

// errOutboxBusy reports a processedOutbox read refused because
// maxOutboxReads reads are running. outboxProcessed alone returns it.
var errOutboxBusy = errors.New("processedOutbox reads are at their cap")

// outboxProofResult is zeno_getOutboxProof's result: the RelayMessage
// arguments for one outbox message (SPEC §17.3, §27.3) and its
// processedOutbox state.
type outboxProofResult struct {
	DomainID        uint32     `json:"domainId"`
	BatchID         uint64     `json:"batchId"`
	Verified        bool       `json:"verified"`
	Tag             string     `json:"tag"`
	OutboxID        types.Hash `json:"outboxId"`
	OutboxRoot      types.Hash `json:"outboxRoot"`
	Message         hexBytes   `json:"message"`        // canonical encoding
	InclusionProof  hexBytes   `json:"inclusionProof"` // §27.3 MerkleProof
	Decoded         outboxJSON `json:"decoded"`
	ProcessedOutbox bool       `json:"processedOutbox"`
}

func (s *Server) getOutboxProof(params []json.RawMessage, b *budget) (any, error) {
	if err := arity(params, 1, 1); err != nil {
		return nil, err
	}
	id, err := hashParam(params[0], "outboxId")
	if err != nil {
		return nil, err
	}
	for _, c := range s.order {
		res, err := s.outboxProofIn(c, id, b)
		if err != nil {
			return nil, err
		}
		if res != nil {
			return res, nil
		}
	}
	return nil, &rpcError{codeNotFound, fmt.Sprintf("outbox message %s not found", id)}
}

// outboxProofIn returns the proof of outbox message id from c, or nil
// when c's index holds no visible message for it.
func (s *Server) outboxProofIn(c *Chain, id types.Hash, b *budget) (*outboxProofResult, error) {
	v, err := c.cfg.Index.view()
	if err != nil {
		return nil, err
	}
	defer v.Release()
	g, j, ok, err := v.outboxAt(id)
	if err != nil || !ok {
		return nil, err
	}
	in, ok, err := v.inputAt(g)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, damaged(ikey(pfxInput, be64(g)), errors.New("missing for an indexed outbox message"))
	}
	r, bv, ok, err := visibleBatch(c, v, in.BatchID)
	if err != nil || !ok {
		return nil, err
	}
	if b.scan <= 0 || b.scanBytes <= 0 {
		return nil, &rpcError{codeInvalidRequest, scanBudgetMsg}
	}

	var sufs, encs [][]byte
	stop, visited, err := v.scanDesc([]byte(pfxOutbox), bv.FirstInputSeq, bv.LastInputSeq, nil, maxBatchOutbox+1, func(suffix, value []byte) (bool, error) {
		b.scanBytes -= len(value)
		sufs, encs = append(sufs, suffix), append(encs, value)
		return true, nil
	})
	b.scan -= visited
	if err != nil {
		return nil, err
	}
	if stop != nil {
		return nil, damaged(ikey(pfxOutbox, stop), errors.New("more outbox messages than a batch can hold"))
	}
	slices.Reverse(sufs)
	slices.Reverse(encs)
	want := append(be64(g), be32(j)...)
	idx := slices.IndexFunc(sufs, func(suffix []byte) bool { return bytes.Equal(suffix, want) })
	if idx < 0 {
		return nil, damaged(ikey(pfxOutbox, want), errors.New("missing inside an indexed batch"))
	}
	key := ikey(pfxOutbox, want)
	proof := codec.MerkleProofs(encs)[idx]
	if err := codec.VerifyMerkleProof(bv.OutboxRoot, encs[idx], proof); err != nil {
		return nil, damaged(key, fmt.Errorf("outbox proof of %s does not verify against batch %d's outboxRoot: %w", id, r.id, err))
	}
	decoded, err := outboxOf(c.cfg.DomainID, r.id, encs[idx])
	if err != nil {
		return nil, damaged(key, err)
	}
	if decoded.OutboxID != id {
		return nil, damaged(key, fmt.Errorf("decodes to outbox message %s, not %s", decoded.OutboxID, id))
	}
	processed := false
	if isVerified(r.head, r.id) {
		if processed, err = s.outboxProcessed(c, b, id); err != nil {
			return nil, err
		}
	}
	return &outboxProofResult{
		DomainID:        c.cfg.DomainID,
		BatchID:         r.id,
		Verified:        isVerified(r.head, r.id),
		Tag:             reached(r.head, r.id),
		OutboxID:        id,
		OutboxRoot:      bv.OutboxRoot,
		Message:         encs[idx],
		InclusionProof:  proof,
		Decoded:         decoded,
		ProcessedOutbox: processed,
	}, nil
}

// outboxProcessed reads whether Settlement has registered outbox
// message id, from c's newest Settlement view, by b's outbox deadline
// and within maxOutboxReads.
func (s *Server) outboxProcessed(c *Chain, b *budget, id types.Hash) (bool, error) {
	view := c.settlementView()
	if view == nil {
		return false, &rpcError{codeNotAvailable, "processedOutbox is not available until the gateway's first poll reads Settlement; retry later"}
	}
	if b.outboxBy.IsZero() {
		b.outboxBy = time.Now().Add(s.outboxReadTimeout)
	}
	ctx, cancel := context.WithDeadline(b.ctx, b.outboxBy)
	defer cancel()
	// The slot is taken and released inside the read's goroutine, so it
	// counts the node call until it returns, and withDeadline takes none
	// when ctx has already ended.
	ok, err := withDeadline(ctx, s.outboxReadTimeout, func() (bool, error) {
		select {
		case s.outboxSem <- struct{}{}:
		default:
			return false, errOutboxBusy
		}
		defer func() { <-s.outboxSem }()
		return view.OutboxProcessed(id)
	})
	switch {
	case errors.Is(err, errOutboxBusy):
		return false, &rpcError{codeNotAvailable, outboxBusyMsg}
	case err != nil && b.ctx.Err() != nil:
		return false, &rpcError{codeInternal, "internal error"}
	case err != nil:
		return false, fmt.Errorf("settlement: read processedOutbox of %s: %w", id, err)
	}
	return ok, nil
}
