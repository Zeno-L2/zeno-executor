package gateway

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/syndtr/goleveldb/leveldb/util"
	"github.com/zenon-network/go-zenon/common/trie"
	"github.com/zenon-network/go-zenon/common/types"

	"github.com/digitalSloth/go-executor/domain"
	"github.com/digitalSloth/go-executor/internal/codec"
	"github.com/digitalSloth/go-executor/internal/state"
)

// readResult is zeno_read's result.
type readResult struct {
	DomainID      uint32      `json:"domainId"`
	BatchID       uint64      `json:"batchId"`
	Verified      bool        `json:"verified"`
	PostStateRoot types.Hash  `json:"postStateRoot"`
	View          string      `json:"view"`
	Result        any         `json:"result"`
	Proofs        []pathProof `json:"proofs"` // [] when none
}

// readContext is the domain.ReadContext of one zeno_read request: it
// reads the state version and the inputs of one batch, within the
// request body's budget b.
type readContext struct {
	v       *indexView
	b       *budget
	version state.Version
	hi      uint64 // the batch's LastInputSeq
	proofs  []pathProof
	seen    map[trie.Path]bool
}

var _ domain.ReadContext = (*readContext)(nil)

func proofBudgetErr() error { return &rpcError{codeInvalidRequest, proofBudgetMsg} }

func scanBudgetErr() error { return &rpcError{codeInvalidRequest, scanBudgetMsg} }

// Read returns the value at path, and records its proof the first time
// the request reads path.
func (rc *readContext) Read(path domain.Path) ([]byte, bool, error) {
	present, value, proof := rc.version.Prove(path)
	if !rc.seen[path] {
		if rc.b.proofs <= 0 {
			return nil, false, proofBudgetErr()
		}
		rc.b.proofs--
		rc.seen[path] = true
		pp := pathProof{Path: types.Hash(path), Present: present, Proof: proof}
		if present {
			hb := hexBytes(value)
			pp.Value = &hb
		}
		rc.proofs = append(rc.proofs, pp)
	}
	if !present {
		return nil, false, nil
	}
	out := bytes.Clone(value)
	if out == nil {
		out = []byte{}
	}
	return out, true, nil
}

// indexed returns input g of the index as a domain.IndexedInput.
func (rc *readContext) indexed(g uint64) (domain.IndexedInput, error) {
	in, err := inputOf(rc.v, g, rc.b)
	if err != nil {
		return domain.IndexedInput{}, err
	}
	rec, err := codec.DecodeReceipt(in.Receipt)
	if err != nil {
		return domain.IndexedInput{}, damaged(ikey(pfxInput, be64(g)), err)
	}
	return domain.IndexedInput{
		GlobalInputIndex: g,
		BatchID:          in.BatchID,
		SendHash:         in.SendHash,
		Sender:           in.Sender,
		InputData:        in.InputData,
		Receipt:          rec,
	}, nil
}

// visit calls fn for each input under prefix, newest first, over the
// inputs whose global index is below before and at most the batch's.
func (rc *readContext) visit(prefix []byte, before uint64, fn func(domain.IndexedInput) (bool, error)) error {
	if before == 0 {
		return nil
	}
	if rc.b.scan <= 0 || rc.b.scanBytes <= 0 {
		return scanBudgetErr()
	}
	spent := false
	stop, visited, err := rc.v.scanDesc(prefix, 0, min(rc.hi, before-1), nil, rc.b.scan, func(suffix, _ []byte) (bool, error) {
		g, err := suffixIndex(prefix, suffix, inputCursor)
		if err != nil {
			return false, err
		}
		in, err := rc.indexed(g)
		if err != nil {
			return false, err
		}
		more, err := fn(in)
		if err != nil || !more {
			return false, err
		}
		if rc.b.scanBytes <= 0 {
			spent = true
			return false, nil
		}
		return true, nil
	})
	rc.b.scan -= visited
	if err != nil {
		return err
	}
	if stop != nil || spent {
		return scanBudgetErr()
	}
	return nil
}

// InputsBySender visits sender's inputs.
func (rc *readContext) InputsBySender(sender types.Address, before uint64, fn func(domain.IndexedInput) (bool, error)) error {
	return rc.visit(ikey(pfxSenderIdx, sender[:]), before, fn)
}

// Deploys visits deployer's successful contract deploys.
func (rc *readContext) Deploys(deployer types.Address, before uint64, fn func(domain.IndexedInput) (bool, error)) error {
	return rc.visit(ikey(pfxDeployer, deployer[:]), before, fn)
}

// LastOwnerInput returns the newest successful finalize or ownership
// transfer of contract.
func (rc *readContext) LastOwnerInput(contract types.Hash) (domain.IndexedInput, bool, error) {
	if rc.b.scan <= 0 || rc.b.scanBytes <= 0 {
		return domain.IndexedInput{}, false, scanBudgetErr()
	}
	prefix := ikey(pfxOwnerSet, contract[:])
	var g uint64
	found := false
	_, visited, err := rc.v.scanDesc(prefix, 0, rc.hi, nil, 1, func(suffix, _ []byte) (bool, error) {
		var err error
		g, err = suffixIndex(prefix, suffix, inputCursor)
		found = err == nil
		return false, err
	})
	rc.b.scan -= visited
	if err != nil || !found {
		return domain.IndexedInput{}, false, err
	}
	in, err := rc.indexed(g)
	return in, err == nil, err
}

// FirstCodeInput returns the oldest successful finalize or upgrade
// whose code hash is codeHash.
func (rc *readContext) FirstCodeInput(codeHash types.Hash) (domain.IndexedInput, bool, error) {
	if rc.b.scan <= 0 || rc.b.scanBytes <= 0 {
		return domain.IndexedInput{}, false, scanBudgetErr()
	}
	prefix := ikey(pfxCodeHash, codeHash[:])
	it := rc.v.r.NewIterator(util.BytesPrefix(prefix), nil)
	defer it.Release()
	has := it.First()
	rc.b.scan--
	if !has {
		return domain.IndexedInput{}, false, it.Error()
	}
	g, err := suffixIndex(prefix, it.Key()[len(prefix):], inputCursor)
	if err != nil {
		return domain.IndexedInput{}, false, err
	}
	if g > rc.hi {
		return domain.IndexedInput{}, false, nil
	}
	in, err := rc.indexed(g)
	return in, err == nil, err
}

// ChunkData returns the chunk bytes of the deploy chunk input g.
func (rc *readContext) ChunkData(g uint64) ([]byte, error) {
	if rc.b.scanBytes <= 0 {
		return nil, scanBudgetErr()
	}
	key := ikey(pfxChunk, be64(g))
	value, ok, err := rc.v.get(key)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, damaged(key, errors.New("missing for an indexed chunk"))
	}
	rc.b.scanBytes -= len(value)
	return value, nil
}

// readerOf returns c's reader, nil when its record is not read yet or
// its runtime kind has none.
func (s *Server) readerOf(c *Chain) domain.Reader {
	rec := c.domainRecord()
	if rec == nil {
		return nil
	}
	return s.readers[rec.RuntimeKind]
}

// describe returns rd's decoded input, nil when rd is nil or cannot
// decode it.
func describe(rd domain.Reader, inputData []byte) any {
	if rd == nil {
		return nil
	}
	d, err := rd.DescribeInput(inputData)
	if err != nil {
		return nil
	}
	return d
}

// bytesParam decodes raw as hex of at most maxLen bytes; name names
// the parameter in the error.
func bytesParam(raw json.RawMessage, name string, maxLen int) ([]byte, error) {
	bad := &rpcError{codeInvalidParams, fmt.Sprintf("invalid params: %s must be hex of at most %d bytes", name, maxLen)}
	var s string
	if !startsWith(raw, '"') || json.Unmarshal(raw, &s) != nil || len(s) > 2*maxLen {
		return nil, bad
	}
	b, err := hex.DecodeString(s)
	if err != nil {
		return nil, bad
	}
	return b, nil
}

// viewArgs decodes params[2], the argument object of view vw.
func viewArgs(vw domain.View, params []json.RawMessage) (map[string]any, error) {
	names := make([]string, len(vw.Params))
	for i, p := range vw.Params {
		names[i] = p.Name
	}
	var raw json.RawMessage
	if len(params) > 2 {
		raw = params[2]
	}
	members := map[string]json.RawMessage{}
	if given(raw) {
		bad := &rpcError{codeInvalidParams, "invalid params: params must be an object with members " + strings.Join(names, ", ")}
		if len(names) == 0 {
			bad.Message = "invalid params: params must be an empty object"
		}
		if !startsWith(raw, '{') || !exactMembers(raw, strings.Join(names, ", ")) || json.Unmarshal(raw, &members) != nil {
			return nil, bad
		}
		if len(names) == 0 && len(members) > 0 {
			return nil, bad
		}
	}
	args := make(map[string]any, len(vw.Params))
	for _, p := range vw.Params {
		m := members[p.Name]
		if !given(m) {
			if !p.Optional {
				return nil, &rpcError{codeInvalidParams, fmt.Sprintf("invalid params: %s is required", p.Name)}
			}
			continue
		}
		var err error
		switch p.Type {
		case domain.ParamHash:
			args[p.Name], err = hashParam(m, p.Name)
		case domain.ParamAddress:
			args[p.Name], err = addressParam(m, p.Name)
		case domain.ParamBytes:
			args[p.Name], err = bytesParam(m, p.Name, p.MaxLen)
		}
		if err != nil {
			return nil, err
		}
	}
	return args, nil
}

// read serves zeno_read(domainId, view[, params[, tag]]).
func (s *Server) read(params []json.RawMessage, b *budget) (any, error) {
	if err := arity(params, 2, 4); err != nil {
		return nil, err
	}
	c, err := s.domainOf(params[0])
	if err != nil {
		return nil, err
	}
	var name string
	if !startsWith(params[1], '"') || json.Unmarshal(params[1], &name) != nil {
		return nil, &rpcError{codeInvalidParams, "invalid params: view must be a string"}
	}
	var vw domain.View
	found := false
	if rd := s.readerOf(c); rd != nil {
		for _, x := range rd.Views() {
			if x.Name == name {
				vw, found = x, true
				break
			}
		}
	}
	if !found {
		return nil, &rpcError{codeNoSuchView, "the domain has no such view"}
	}
	args, err := viewArgs(vw, params)
	if err != nil {
		return nil, err
	}
	t, err := tagAt(params, 3)
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
	rc := &readContext{v: v, b: b, version: r.version, hi: bv.LastInputSeq, seen: map[trie.Path]bool{}}
	out, err := vw.Read(rc, args)
	if err != nil {
		var re *rpcError
		switch {
		case errors.As(err, &re):
			return nil, re
		case errors.Is(err, domain.ErrInvalidParams):
			return nil, &rpcError{codeInvalidParams, err.Error()}
		case errors.Is(err, domain.ErrNotFound):
			return nil, &rpcError{codeNotFound, err.Error()}
		}
		return nil, err
	}
	proofs := rc.proofs
	if proofs == nil {
		proofs = []pathProof{}
	}
	return readResult{
		DomainID:      c.cfg.DomainID,
		BatchID:       r.id,
		Verified:      isVerified(r.head, r.id),
		PostStateRoot: r.root,
		View:          name,
		Result:        out,
		Proofs:        proofs,
	}, nil
}
