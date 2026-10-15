package gateway

import (
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strconv"

	"github.com/zenon-network/go-zenon/common/types"

	"github.com/digitalSloth/go-executor/internal/codec"
)

// batchEventKind is what happened to a batch.
type batchEventKind int

const (
	evPending   batchEventKind = iota // added above latest
	evLatest                          // accepted on Settlement
	evFinalized                       // finalized
	evRemoved                         // retracted
)

// batchEvent is one batch transition the chain reports. sends holds
// each input's send hash in record order.
type batchEvent struct {
	kind  batchEventKind
	want  expect
	sends []types.Hash
}

type subKind int

const (
	subHeads subKind = iota
	subReceipt
	subEvents
)

// subscription is one zeno_subscribe on one connection. Every field
// but active is fixed once it is created.
type subscription struct {
	conn     *wsConn
	id       string
	kind     subKind
	domainID uint32        // heads and events
	sendHash types.Hash    // receipt
	contract *types.Hash   // events; nil matches any contract
	topics   []*types.Hash // events; by position, nil matches any
	pending  bool          // events: also unverified batches
	// active is false until the response that created it is written.
	// Server.wsMu guards it.
	active bool
}

// eventsSubFilter is the filter of an events subscription.
type eventsSubFilter struct {
	DomainID   json.RawMessage `json:"domainId"`
	ContractID json.RawMessage `json:"contractId"`
	Topics     json.RawMessage `json:"topics"`
}

const eventsSubMembers = "domainId, contractId, topics"

// maxBatchEvents is the most events one batch can hold.
const maxBatchEvents = codec.MaxBatchInputs * codec.MaxEventsPerExecution

// receiptNote is the result of a receipt notification.
type receiptNote struct {
	DomainID uint32     `json:"domainId"`
	BatchID  uint64     `json:"batchId"`
	DAHash   types.Hash `json:"daHash"`
	SendHash types.Hash `json:"sendHash"`
	Tag      string     `json:"tag"`
	Verified bool       `json:"verified"`
	Removed  bool       `json:"removed"`
}

// eventsNote is the result of an events notification: the batch and
// the count of its events that match the subscription's filter.
type eventsNote struct {
	DomainID uint32     `json:"domainId"`
	BatchID  uint64     `json:"batchId"`
	DAHash   types.Hash `json:"daHash"`
	Verified bool       `json:"verified"`
	Removed  bool       `json:"removed"`
	Count    int        `json:"count"`
}

func (s *Server) subscribe(params []json.RawMessage, b *budget) (any, error) {
	if b.conn == nil {
		return nil, &rpcError{codeMethodNotFound, "zeno_subscribe is served over WebSocket only"}
	}
	if err := arity(params, 2, 3); err != nil {
		return nil, err
	}
	var kind string
	if !startsWith(params[0], '"') || json.Unmarshal(params[0], &kind) != nil || (kind != "heads" && kind != "receipt" && kind != "events") {
		return nil, &rpcError{codeInvalidParams, `invalid params: kind must be "heads", "receipt" or "events"`}
	}
	sub := &subscription{conn: b.conn}
	switch kind {
	case "heads", "receipt":
		if len(params) == 3 {
			return nil, &rpcError{codeInvalidParams, "invalid params: want 2 parameters, got 3"}
		}
		if kind == "heads" {
			c, err := s.domainOf(params[1])
			if err != nil {
				return nil, err
			}
			sub.kind, sub.domainID = subHeads, c.cfg.DomainID
			break
		}
		h, err := hashParam(params[1], "sendHash")
		if err != nil {
			return nil, err
		}
		sub.kind, sub.sendHash = subReceipt, h
	case "events":
		var f eventsSubFilter
		if err := decodeFilter(params[1:2], &f, eventsSubMembers); err != nil {
			return nil, err
		}
		c, err := s.domainOf(f.DomainID)
		if err != nil {
			return nil, err
		}
		sub.kind, sub.domainID = subEvents, c.cfg.DomainID
		if given(f.ContractID) {
			h, err := hashParam(f.ContractID, "contractId")
			if err != nil {
				return nil, err
			}
			sub.contract = &h
		}
		if given(f.Topics) {
			if sub.topics, err = topicsParam(f.Topics); err != nil {
				return nil, err
			}
		}
		if len(params) == 3 && string(params[2]) != "null" {
			var t string
			if !startsWith(params[2], '"') || json.Unmarshal(params[2], &t) != nil || (t != "latest" && t != "pending") {
				return nil, &rpcError{codeInvalidParams, `invalid params: tag must be "latest" or "pending"`}
			}
			sub.pending = t == "pending"
			if sub.pending && c.remote() {
				return nil, &rpcError{codeNotAvailable, noPendingMsg}
			}
		}
	}

	s.wsMu.Lock()
	defer s.wsMu.Unlock()
	if len(b.conn.subs) >= maxSubscriptions {
		return nil, &rpcError{codeInvalidRequest, fmt.Sprintf("connection has %d subscriptions; unsubscribe one first", maxSubscriptions)}
	}
	b.conn.nextID++
	sub.id = strconv.FormatUint(b.conn.nextID, 16)
	b.conn.subs[sub.id] = sub
	s.subs[sub] = true
	b.sub = sub
	return sub.id, nil
}

func (s *Server) unsubscribe(params []json.RawMessage, b *budget) (any, error) {
	if b.conn == nil {
		return nil, &rpcError{codeMethodNotFound, "zeno_unsubscribe is served over WebSocket only"}
	}
	if err := arity(params, 1, 1); err != nil {
		return nil, err
	}
	var id string
	if !startsWith(params[0], '"') || json.Unmarshal(params[0], &id) != nil {
		return nil, &rpcError{codeInvalidParams, "invalid params: id must be a subscription id"}
	}
	s.wsMu.Lock()
	defer s.wsMu.Unlock()
	sub, ok := b.conn.subs[id]
	if ok {
		delete(b.conn.subs, id)
		delete(s.subs, sub)
	}
	return ok, nil
}

// activate makes c's subscriptions notify; the reader calls it after
// writing the response that created them.
func (s *Server) activate(c *wsConn) {
	s.wsMu.Lock()
	defer s.wsMu.Unlock()
	for _, sub := range c.subs {
		sub.active = true
	}
}

// dropSub removes sub from its connection and the server.
func (s *Server) dropSub(sub *subscription) {
	s.wsMu.Lock()
	defer s.wsMu.Unlock()
	delete(sub.conn.subs, sub.id)
	delete(s.subs, sub)
}

// noteFrame returns the notification frame of subscription id.
func noteFrame(id string, result []byte) []byte {
	out := fmt.Appendf(nil, `{"jsonrpc":"2.0","method":"zeno_subscription","params":{"subscription":%q,"result":`, id)
	out = append(out, result...)
	return append(out, "}}"...)
}

// live reports whether sub is active and still registered. The caller
// holds s.wsMu.
func (s *Server) live(sub *subscription) bool {
	return sub.active && s.subs[sub]
}

// headMoved notifies the heads subscribers of c's domain of h.
func (s *Server) headMoved(c *Chain, h Head) {
	var targets []*subscription
	s.wsMu.Lock()
	for sub := range s.subs {
		if sub.kind == subHeads && sub.active && sub.domainID == c.cfg.DomainID {
			targets = append(targets, sub)
		}
	}
	s.wsMu.Unlock()
	if len(targets) == 0 {
		return
	}
	result, err := json.Marshal(headResultOf(c, h))
	if err != nil {
		s.logInternal("zeno_subscription", err)
		return
	}
	s.wsMu.Lock()
	defer s.wsMu.Unlock()
	for _, sub := range targets {
		if s.live(sub) {
			sub.conn.enqueue(note{sub, noteFrame(sub.id, result)})
		}
	}
}

// batchChanged notifies the receipt and events subscribers ev
// concerns. It runs on c's Poll goroutine and never blocks.
func (s *Server) batchChanged(c *Chain, ev batchEvent) {
	var recs, evs []*subscription
	s.wsMu.Lock()
	for sub := range s.subs {
		if !sub.active {
			continue
		}
		switch {
		case sub.kind == subReceipt:
			recs = append(recs, sub)
		case sub.kind == subEvents && sub.domainID == c.cfg.DomainID && eventsConcerns(sub, ev.kind):
			evs = append(evs, sub)
		}
	}
	s.wsMu.Unlock()
	if len(recs) == 0 && len(evs) == 0 {
		return
	}

	if len(recs) > 0 {
		sent := make(map[types.Hash]bool, len(ev.sends))
		for _, h := range ev.sends {
			sent[h] = true
		}
		recs = slices.DeleteFunc(recs, func(sub *subscription) bool { return !sent[sub.sendHash] })
	}

	counts := make([]int, len(evs))
	if len(evs) > 0 && (ev.kind == evPending || ev.kind == evLatest) {
		if c.cfg.Index == nil {
			evs = nil
		} else if err := s.countEvents(c, ev, evs, counts); err != nil {
			s.logInternal("zeno_subscription", err)
			evs = nil
		}
	}

	type out struct {
		sub *subscription
		msg []byte
	}
	var outs []out
	if len(recs) > 0 {
		r := receiptNote{DomainID: c.cfg.DomainID, BatchID: ev.want.batchID, DAHash: ev.want.daHash}
		switch ev.kind {
		case evPending:
			r.Tag = "pending"
		case evLatest:
			r.Tag, r.Verified = "latest", true
		case evFinalized:
			r.Tag, r.Verified = "finalized", true
		case evRemoved:
			r.Tag, r.Removed = "pending", true
		}
		for _, sub := range recs {
			r.SendHash = sub.sendHash
			if result, err := json.Marshal(r); err == nil {
				outs = append(outs, out{sub, noteFrame(sub.id, result)})
			}
		}
	}
	for i, sub := range evs {
		n := eventsNote{DomainID: c.cfg.DomainID, BatchID: ev.want.batchID, DAHash: ev.want.daHash, Count: counts[i]}
		switch ev.kind {
		case evLatest:
			n.Verified = true
		case evRemoved:
			n.Removed = true
		}
		if ev.kind != evRemoved && n.Count == 0 {
			continue
		}
		if result, err := json.Marshal(n); err == nil {
			outs = append(outs, out{sub, noteFrame(sub.id, result)})
		}
	}

	s.wsMu.Lock()
	defer s.wsMu.Unlock()
	for _, o := range outs {
		if s.live(o.sub) {
			o.sub.conn.enqueue(note{o.sub, o.msg})
		}
	}
}

// eventsConcerns reports whether an events subscription is told of a
// batch event of kind k: every kind but finalized tells pending
// subscribers, and an accepted batch tells the rest.
func eventsConcerns(sub *subscription, k batchEventKind) bool {
	switch k {
	case evLatest:
		return true
	case evPending, evRemoved:
		return sub.pending
	}
	return false
}

// countEvents sets counts[i] to the number of the batch ev names whose
// events match subs[i]'s filter.
func (s *Server) countEvents(c *Chain, ev batchEvent, subs []*subscription, counts []int) error {
	v, err := c.cfg.Index.view()
	if err != nil {
		return err
	}
	defer v.Release()
	events, err := batchEvents(v, ev.want)
	if err != nil {
		return err
	}
	for _, e := range events {
		for i, sub := range subs {
			if (sub.contract == nil || e.ContractID == *sub.contract) && topicsMatch(e.Topics, sub.topics) {
				counts[i]++
			}
		}
	}
	return nil
}

// batchEvents returns the events of the batch want names from v.
func batchEvents(v *indexView, want expect) ([]eventJSON, error) {
	var out []eventJSON
	stop, _, err := v.scanDesc([]byte(pfxEvent), want.firstInputSeq, want.lastInputSeq, nil, maxBatchEvents+1, func(suffix, value []byte) (bool, error) {
		e, err := eventOf(value)
		if err != nil {
			return false, damaged(ikey(pfxEvent, suffix), err)
		}
		out = append(out, e)
		return true, nil
	})
	if err != nil {
		return nil, err
	}
	if stop != nil {
		return nil, damaged(ikey(pfxEvent, stop), errors.New("more events than a batch can hold"))
	}
	return out, nil
}
