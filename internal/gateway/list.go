package gateway

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/zenon-network/go-zenon/common/types"

	"github.com/Zeno-L2/zeno-executor/internal/codec"
)

// listFilter holds the members every list filter has, each as raw
// JSON: nil when absent and the bytes null when null.
type listFilter struct {
	DomainID  json.RawMessage `json:"domainId"`
	FromBatch json.RawMessage `json:"fromBatch"`
	ToBatch   json.RawMessage `json:"toBatch"`
	Limit     json.RawMessage `json:"limit"`
	Cursor    json.RawMessage `json:"cursor"`
}

// inputsFilter is zeno_getInputs' filter.
type inputsFilter struct {
	listFilter
	ContractID json.RawMessage `json:"contractId"`
	Sender     json.RawMessage `json:"sender"`
	InputKind  json.RawMessage `json:"inputKind"`
}

// eventsFilter is zeno_getEvents' filter.
type eventsFilter struct {
	listFilter
	ContractID json.RawMessage `json:"contractId"`
	Topics     json.RawMessage `json:"topics"`
}

// outboxFilter is zeno_getOutbox's filter.
type outboxFilter struct {
	listFilter
	Recipient json.RawMessage `json:"recipient"`
}

const (
	inputsMembers = "domainId, fromBatch, toBatch, limit, cursor, contractId, sender, inputKind"
	eventsMembers = "domainId, fromBatch, toBatch, limit, cursor, contractId, topics"
	outboxMembers = "domainId, fromBatch, toBatch, limit, cursor, recipient"
	// inputCursor and entryCursor are cursor lengths in bytes: a
	// global input index, and for events and outbox messages also
	// their index.
	inputCursor = 8
	entryCursor = 12
)

// decodeFilter decodes params, which must hold one filter object with
// only dst's members, spelled exactly and given once, into dst; names
// lists those members.
func decodeFilter(params []json.RawMessage, dst any, names string) error {
	if err := arity(params, 1, 1); err != nil {
		return err
	}
	bad := &rpcError{codeInvalidParams, "invalid params: filter must be an object with members " + names}
	if !startsWith(params[0], '{') {
		return bad
	}
	if !exactMembers(params[0], names) {
		return bad
	}
	dec := json.NewDecoder(bytes.NewReader(params[0]))
	dec.DisallowUnknownFields()
	if dec.Decode(dst) != nil {
		return bad
	}
	return nil
}

// exactMembers reports whether the object raw names only members from
// the comma-separated list names, each spelled exactly and at most
// once. encoding/json matches names without regard to case and keeps
// the last of a repeated member, so the check reads the names itself.
func exactMembers(raw json.RawMessage, names string) bool {
	dec := json.NewDecoder(bytes.NewReader(raw))
	if _, err := dec.Token(); err != nil {
		return false
	}
	seen := map[string]bool{}
	for dec.More() {
		t, err := dec.Token()
		key, ok := t.(string)
		if err != nil || !ok || seen[key] || !slices.Contains(strings.Split(names, ", "), key) {
			return false
		}
		seen[key] = true
		var skip json.RawMessage
		if dec.Decode(&skip) != nil {
			return false
		}
	}
	return true
}

// given reports whether the optional member raw is given: present and
// not null.
func given(raw json.RawMessage) bool {
	return len(raw) > 0 && string(raw) != "null"
}

// uintOf decodes raw as a JSON integer from 0 to 2^64-1.
func uintOf(raw json.RawMessage) (uint64, bool) {
	var u uint64
	if len(raw) == 0 || raw[0] < '0' || raw[0] > '9' || json.Unmarshal(raw, &u) != nil {
		return 0, false
	}
	return u, true
}

// addressParam decodes raw as an L1 address; name names the member in
// the error.
func addressParam(raw json.RawMessage, name string) (types.Address, error) {
	var a types.Address
	if !startsWith(raw, '"') || json.Unmarshal(raw, &a) != nil {
		return types.Address{}, &rpcError{codeInvalidParams, fmt.Sprintf("invalid params: %s must be an L1 address", name)}
	}
	return a, nil
}

// topicsParam decodes raw as an array of at most MaxTopicsPerEvent
// entries, each a hash or null; a nil entry matches any topic.
func topicsParam(raw json.RawMessage) ([]*types.Hash, error) {
	bad := &rpcError{codeInvalidParams, fmt.Sprintf("invalid params: topics must be an array of at most %d entries, each 64 hex characters or null", codec.MaxTopicsPerEvent)}
	if !startsWith(raw, '[') {
		return nil, bad
	}
	elems, over, err := splitArray(raw, codec.MaxTopicsPerEvent)
	if err != nil || over {
		return nil, bad
	}
	topics := make([]*types.Hash, len(elems))
	for i, e := range elems {
		if string(e) == "null" {
			continue
		}
		h, err := hashParam(e, "topic")
		if err != nil {
			return nil, bad
		}
		topics[i] = &h
	}
	return topics, nil
}

// cursorParam decodes raw as a cursor of n bytes.
func cursorParam(raw json.RawMessage, n int) ([]byte, error) {
	if len(raw) == 2*n+2 && raw[0] == '"' && raw[len(raw)-1] == '"' {
		if c, err := hex.DecodeString(string(raw[1 : len(raw)-1])); err == nil {
			return c, nil
		}
	}
	return nil, &rpcError{codeInvalidParams, "invalid params: cursor must be a nextCursor of this method"}
}

// topicsMatch reports whether an event's topics match want by
// position.
func topicsMatch(topics []types.Hash, want []*types.Hash) bool {
	for i, w := range want {
		if w != nil && (i >= len(topics) || topics[i] != *w) {
			return false
		}
	}
	return true
}

// listCall is one list request's shared filter members, parsed.
type listCall struct {
	c      *Chain
	from   uint64
	to     tag
	limit  int
	cursor []byte // nil for the first page
}

// listCallOf parses f's shared members for a method whose cursor is n
// bytes.
func (s *Server) listCallOf(f listFilter, n int) (listCall, error) {
	c, err := s.domainOf(f.DomainID)
	if err != nil {
		return listCall{}, err
	}
	l := listCall{c: c, to: tag{name: "latest"}, limit: maxPageItems}
	if given(f.FromBatch) {
		from, ok := uintOf(f.FromBatch)
		if !ok {
			return listCall{}, &rpcError{codeInvalidParams, "invalid params: fromBatch must be a batch id"}
		}
		l.from = from
	}
	if given(f.ToBatch) {
		t, err := parseTag(f.ToBatch)
		if err != nil {
			return listCall{}, &rpcError{codeInvalidParams, `invalid params: toBatch must be "pending", "latest", "finalized" or a batch id`}
		}
		l.to = t
	}
	if given(f.Limit) {
		lim, ok := uintOf(f.Limit)
		if !ok || lim < 1 || lim > maxPageItems {
			return listCall{}, &rpcError{codeInvalidParams, fmt.Sprintf("invalid params: limit must be an integer from 1 to %d", maxPageItems)}
		}
		l.limit = int(lim)
	}
	if given(f.Cursor) {
		cur, err := cursorParam(f.Cursor, n)
		if err != nil {
			return listCall{}, err
		}
		l.cursor = cur
	}
	if l.to.name == "" && l.from > l.to.id {
		return listCall{}, &rpcError{codeInvalidParams, fmt.Sprintf("invalid params: fromBatch %d is above toBatch %d", l.from, l.to.id)}
	}
	return l, nil
}

// listRange resolves l's toBatch, opens one index view and returns the
// global input range [lo, hi] of fromBatch through toBatch; lo > hi
// when toBatch has not reached fromBatch. v is nil when no batch has
// reached a named toBatch; otherwise the caller must Release v.
func (s *Server) listRange(l listCall) (r tagRead, v *indexView, lo, hi uint64, err error) {
	r, v, bv, err := s.readBatch(l.c, l.to)
	if err != nil {
		// A named tag no batch has reached lists nothing, where the
		// other methods answer not found.
		if l.to.name != "" && !r.ok && !r.noPending {
			return r, nil, 1, 0, nil
		}
		return r, nil, 0, 0, err
	}
	if l.from > r.id {
		return r, v, 1, 0, nil
	}
	lo = bv.FirstInputSeq
	if l.from < r.id {
		from, ok, err := v.batchAt(l.from)
		if err == nil && !ok {
			err = damaged(ikey(pfxBatch, be64(l.from)), errors.New("missing below an indexed batch"))
		}
		if err != nil {
			v.Release()
			return r, nil, 0, 0, err
		}
		lo = from.FirstInputSeq
	}
	return r, v, lo, bv.LastInputSeq, nil
}

// listHead returns a list result's bytes before its first item, with
// batchId null and verified false when no batch is at a named
// toBatch.
func listHead(l listCall, r tagRead) []byte {
	if r.ok {
		return fmt.Appendf(nil, `{"domainId":%d,"batchId":%d,"verified":%t,"items":[`, l.c.cfg.DomainID, r.id, isVerified(r.head, r.id))
	}
	return fmt.Appendf(nil, `{"domainId":%d,"batchId":null,"verified":false,"items":[`, l.c.cfg.DomainID)
}

// emptyPage returns a list result with no items and no next page.
func emptyPage(l listCall, r tagRead) json.RawMessage {
	return append(listHead(l, r), `],"nextCursor":null}`...)
}

// listPage scans the keys under prefix whose global input index lies
// in [lo, hi], below l's cursor, newest first, within b, and returns
// the page of the items item builds; item returns nil to skip a key
// and charges b.scanBytes for each value it reads. n is the method's
// cursor length. A scan that starts after b's body has used its key or
// byte budget is an error; a request that scans nothing never is.
func listPage(v *indexView, b *budget, l listCall, r tagRead, prefix []byte, lo, hi uint64, n int, item func(suffix, value []byte) (any, error)) (json.RawMessage, error) {
	if lo > hi {
		return emptyPage(l, r), nil
	}
	if b.scan <= 0 || b.scanBytes <= 0 {
		return nil, &rpcError{codeInvalidRequest, scanBudgetMsg}
	}
	out := listHead(l, r)
	// end is the most bytes out may reach before the tail, which
	// takes at most len(`],"nextCursor":""}`) and the cursor's hex.
	end := b.left - (18 + 2*n)
	count, cut := 0, false
	var last, spent []byte
	stop, visited, err := v.scanDesc(prefix, lo, hi, l.cursor, b.scan, func(suffix, value []byte) (bool, error) {
		b.scanBytes -= len(value)
		it, err := item(suffix, value)
		if err != nil {
			return false, err
		}
		if it != nil {
			enc, err := json.Marshal(it)
			if err != nil {
				return false, err
			}
			if count > 0 && len(out)+1+len(enc) > end {
				cut = true
				return false, nil
			}
			if count > 0 {
				out = append(out, ',')
			}
			out = append(out, enc...)
			count, last = count+1, suffix
			if count == l.limit {
				cut = true
				return false, nil
			}
		}
		// The key that uses up the byte budget is finished, so a
		// key larger than the budget still moves the cursor.
		if b.scanBytes <= 0 {
			spent = suffix
			return false, nil
		}
		return true, nil
	})
	b.scan -= visited
	if err != nil {
		return nil, err
	}
	next := stop
	switch {
	case cut:
		next = last
	case spent != nil:
		next = spent
	}
	out = append(out, `],"nextCursor":`...)
	if next == nil {
		out = append(out, "null"...)
	} else {
		out = fmt.Appendf(out, `"%x"`, next)
	}
	return append(out, '}'), nil
}

// inputOf returns input g from v and charges its value's length to
// b.scanBytes. An index entry names g, so a missing input is damage.
func inputOf(v *indexView, g uint64, b *budget) (inputValue, error) {
	var in inputValue
	key := ikey(pfxInput, be64(g))
	value, ok, err := v.get(key)
	if err != nil {
		return in, err
	}
	if !ok {
		return in, damaged(key, errors.New("missing for an indexed entry"))
	}
	b.scanBytes -= len(value)
	if err := json.Unmarshal(value, &in); err != nil {
		return in, damaged(key, err)
	}
	return in, nil
}

// suffixIndex returns the global input index at the start of a key's
// suffix, which must be n bytes.
func suffixIndex(prefix, suffix []byte, n int) (uint64, error) {
	if len(suffix) != n {
		return 0, damaged(append(bytes.Clone(prefix), suffix...), errors.New("key has the wrong length"))
	}
	return binary.BigEndian.Uint64(suffix), nil
}

// inputItem is one input of zeno_getInputs. Input is the domain
// reader's decoded input, or null.
type inputItem struct {
	GlobalInputIndex uint64         `json:"globalInputIndex"`
	BatchID          uint64         `json:"batchId"`
	Verified         bool           `json:"verified"`
	SendHash         types.Hash     `json:"sendHash"`
	Sender           types.Address  `json:"sender"`
	InputKind        uint8          `json:"inputKind"`
	Receipt          receiptSummary `json:"receipt"`
	Input            any            `json:"input"`
}

// receiptSummary is the part of a receipt zeno_getInputs returns.
type receiptSummary struct {
	Status     string     `json:"status"`
	ContractID types.Hash `json:"contractId"`
	GasUsed    uint64     `json:"gasUsed"`
	AbortCode  int32      `json:"abortCode"`
}

// eventItem is one event of zeno_getEvents.
type eventItem struct {
	BatchID  uint64 `json:"batchId"`
	Verified bool   `json:"verified"`
	eventJSON
}

// outboxItem is one outbox message of zeno_getOutbox.
type outboxItem struct {
	BatchID  uint64 `json:"batchId"`
	Verified bool   `json:"verified"`
	outboxJSON
}

// inputMemo holds the last input an event or outbox scan read. Keys
// run by global input index descending, so the keys of one input are
// consecutive and its value is read once.
type inputMemo struct {
	v    *indexView
	b    *budget
	in   inputValue
	g    uint64
	read bool
}

// of returns input g, reading it unless it is the last one read.
func (m *inputMemo) of(g uint64) (inputValue, error) {
	if !m.read || g != m.g {
		in, err := inputOf(m.v, g, m.b)
		if err != nil {
			return in, err
		}
		m.in, m.g, m.read = in, g, true
	}
	return m.in, nil
}

// getInputs serves zeno_getInputs(filter).
func (s *Server) getInputs(params []json.RawMessage, b *budget) (any, error) {
	var f inputsFilter
	if err := decodeFilter(params, &f, inputsMembers); err != nil {
		return nil, err
	}
	l, err := s.listCallOf(f.listFilter, inputCursor)
	if err != nil {
		return nil, err
	}
	var contract *types.Hash
	if given(f.ContractID) {
		h, err := hashParam(f.ContractID, "contractId")
		if err != nil {
			return nil, err
		}
		contract = &h
	}
	var sender *types.Address
	if given(f.Sender) {
		a, err := addressParam(f.Sender, "sender")
		if err != nil {
			return nil, err
		}
		sender = &a
	}
	var kind *uint8
	if given(f.InputKind) {
		k, ok := uintOf(f.InputKind)
		if !ok || k > 255 {
			return nil, &rpcError{codeInvalidParams, "invalid params: inputKind must be an integer from 0 to 255"}
		}
		kk := uint8(k)
		kind = &kk
	}
	r, v, lo, hi, err := s.listRange(l)
	if err != nil {
		return nil, err
	}
	if v == nil {
		return emptyPage(l, r), nil
	}
	defer v.Release()
	var prefix []byte
	switch {
	case contract != nil:
		d, ok, err := v.deployOf(*contract)
		if err != nil {
			return nil, err
		}
		if !ok {
			return emptyPage(l, r), nil
		}
		lo = max(lo, d)
		prefix = ikey(pfxContract, contract[:])
	case sender != nil:
		prefix = ikey(pfxSenderIdx, sender[:])
	case kind != nil:
		prefix = ikey(pfxKind, []byte{*kind})
	default:
		prefix = []byte(pfxInput)
	}
	rd := s.readerOf(l.c)
	return listPage(v, b, l, r, prefix, lo, hi, inputCursor, func(suffix, value []byte) (any, error) {
		g, err := suffixIndex(prefix, suffix, inputCursor)
		if err != nil {
			return nil, err
		}
		key := ikey(pfxInput, be64(g))
		var in inputValue
		if string(prefix) == pfxInput {
			if err := json.Unmarshal(value, &in); err != nil {
				return nil, damaged(key, err)
			}
		} else if in, err = inputOf(v, g, b); err != nil {
			return nil, err
		}
		if len(in.InputData) == 0 {
			return nil, damaged(key, errors.New("input data is empty"))
		}
		if (sender != nil && *sender != in.Sender) || (kind != nil && *kind != in.InputData[0]) {
			return nil, nil
		}
		rc, err := codec.DecodeReceipt(in.Receipt)
		if err != nil {
			return nil, damaged(key, err)
		}
		return inputItem{
			GlobalInputIndex: g,
			BatchID:          in.BatchID,
			Verified:         isVerified(r.head, in.BatchID),
			SendHash:         in.SendHash,
			Sender:           in.Sender,
			InputKind:        in.InputData[0],
			Receipt:          receiptSummary{rc.Status.String(), rc.ContractID, rc.GasUsed, rc.AbortCode},
			Input:            describe(rd, in.InputData),
		}, nil
	})
}

// getEvents serves zeno_getEvents(filter).
func (s *Server) getEvents(params []json.RawMessage, b *budget) (any, error) {
	var f eventsFilter
	if err := decodeFilter(params, &f, eventsMembers); err != nil {
		return nil, err
	}
	l, err := s.listCallOf(f.listFilter, entryCursor)
	if err != nil {
		return nil, err
	}
	var contract *types.Hash
	if given(f.ContractID) {
		h, err := hashParam(f.ContractID, "contractId")
		if err != nil {
			return nil, err
		}
		contract = &h
	}
	var topics []*types.Hash
	if given(f.Topics) {
		if topics, err = topicsParam(f.Topics); err != nil {
			return nil, err
		}
	}
	r, v, lo, hi, err := s.listRange(l)
	if err != nil {
		return nil, err
	}
	if v == nil {
		return emptyPage(l, r), nil
	}
	defer v.Release()
	prefix := []byte(pfxEvent)
	if contract != nil {
		prefix = ikey(pfxEventCtr, contract[:])
	}
	memo := &inputMemo{v: v, b: b}
	return listPage(v, b, l, r, prefix, lo, hi, entryCursor, func(suffix, value []byte) (any, error) {
		g, err := suffixIndex(prefix, suffix, entryCursor)
		if err != nil {
			return nil, err
		}
		key := ikey(pfxEvent, suffix)
		enc := value
		if contract != nil {
			var ok bool
			if enc, ok, err = v.get(key); err != nil {
				return nil, err
			}
			if !ok {
				return nil, damaged(key, errors.New("missing for an indexed contract event"))
			}
			b.scanBytes -= len(enc)
		}
		ev, err := eventOf(enc)
		if err != nil {
			return nil, damaged(key, err)
		}
		if !topicsMatch(ev.Topics, topics) {
			return nil, nil
		}
		in, err := memo.of(g)
		if err != nil {
			return nil, err
		}
		return eventItem{in.BatchID, isVerified(r.head, in.BatchID), ev}, nil
	})
}

// getOutbox serves zeno_getOutbox(filter).
func (s *Server) getOutbox(params []json.RawMessage, b *budget) (any, error) {
	var f outboxFilter
	if err := decodeFilter(params, &f, outboxMembers); err != nil {
		return nil, err
	}
	l, err := s.listCallOf(f.listFilter, entryCursor)
	if err != nil {
		return nil, err
	}
	var recipient *types.Address
	if given(f.Recipient) {
		a, err := addressParam(f.Recipient, "recipient")
		if err != nil {
			return nil, err
		}
		recipient = &a
	}
	r, v, lo, hi, err := s.listRange(l)
	if err != nil {
		return nil, err
	}
	if v == nil {
		return emptyPage(l, r), nil
	}
	defer v.Release()
	prefix := []byte(pfxOutbox)
	if recipient != nil {
		prefix = ikey(pfxOutboxTo, recipient[:])
	}
	memo := &inputMemo{v: v, b: b}
	return listPage(v, b, l, r, prefix, lo, hi, entryCursor, func(suffix, value []byte) (any, error) {
		g, err := suffixIndex(prefix, suffix, entryCursor)
		if err != nil {
			return nil, err
		}
		key := ikey(pfxOutbox, suffix)
		enc := value
		if recipient != nil {
			var ok bool
			if enc, ok, err = v.get(key); err != nil {
				return nil, err
			}
			if !ok {
				return nil, damaged(key, errors.New("missing for an indexed recipient"))
			}
			b.scanBytes -= len(enc)
		}
		in, err := memo.of(g)
		if err != nil {
			return nil, err
		}
		m, err := outboxOf(l.c.cfg.DomainID, in.BatchID, enc)
		if err != nil {
			return nil, damaged(key, err)
		}
		return outboxItem{in.BatchID, isVerified(r.head, in.BatchID), m}, nil
	})
}
