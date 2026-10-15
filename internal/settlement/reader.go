// Package settlement reads the Settlement contract's storage from a znnd node and checks every
// value against the state root in a momentum header, so a node cannot feed the executor state
// the header does not commit to.
package settlement

import (
	"bytes"
	"fmt"

	"github.com/zenon-network/go-zenon/chain/nom"
	"github.com/zenon-network/go-zenon/common/trie"
	"github.com/zenon-network/go-zenon/common/types"
	"github.com/zenon-network/go-zenon/vm/embedded/definition"

	"github.com/digitalSloth/go-executor/internal/recovery"
	"github.com/digitalSloth/go-executor/internal/znnd"
)

// Reader opens verified views of Settlement storage at confirmed heights.
type Reader struct {
	c *znnd.Client
}

// NewReader returns a reader over c.
func NewReader(c *znnd.Client) *Reader {
	return &Reader{c: c}
}

// halted marks a consistency fault: data the node returned contradicts the header, or fails to
// verify against it. The executor stops until an operator acts.
func halted(height uint64, format string, args ...any) error {
	return fmt.Errorf("%w: settlement at height %d: %s", recovery.ErrHalted, height, fmt.Sprintf(format, args...))
}

// ConfirmedHeight returns the node's frontier height minus confirmationDepth.
func (r *Reader) ConfirmedHeight(confirmationDepth uint64) (uint64, error) {
	frontier, err := r.c.FrontierHeight()
	if err != nil {
		return 0, err
	}
	if frontier <= confirmationDepth {
		return 0, fmt.Errorf("settlement: frontier height %d is not above confirmation depth %d", frontier, confirmationDepth)
	}
	return frontier - confirmationDepth, nil
}

// View is Settlement storage as of one momentum, verified against that momentum's state root.
type View struct {
	Height uint64

	c    *znnd.Client
	root types.Hash // the momentum header's StateRoot at Height
	time uint64     // the momentum's TimestampUnix
}

// At returns the view at height. It fails when the momentum does not exist or predates the
// StateRootSpork, whose headers carry no state root.
func (r *Reader) At(height uint64) (*View, error) {
	ms, err := r.c.DetailedMomentumsByHeight(height, 1)
	if err != nil {
		return nil, err
	}
	if len(ms) == 0 || ms[0].Momentum == nil || ms[0].Momentum.Height != height {
		return nil, fmt.Errorf("settlement: node returned no momentum at height %d", height)
	}
	m := ms[0].Momentum
	if m.Version < nom.StateRootMomentumVersion {
		return nil, fmt.Errorf("settlement: StateRootSpork is not active at height %d (momentum version %d)", height, m.Version)
	}
	return &View{Height: height, c: r.c, root: m.StateRoot, time: m.TimestampUnix}, nil
}

// Momentum returns the view's height and its momentum's unix time.
func (v *View) Momentum() (height, unixTime uint64) { return v.Height, v.time }

// CheckNode confirms the node serves usable state at the confirmed height: the StateRootSpork
// is active, the node's state root matches the header, and a proven read succeeds. It returns
// the first error unchanged.
func (r *Reader) CheckNode(confirmationDepth uint64) error {
	h, err := r.ConfirmedHeight(confirmationDepth)
	if err != nil {
		return err
	}
	v, err := r.At(h)
	if err != nil {
		return err
	}
	root, err := r.c.StateRoot(h)
	if err != nil {
		return err
	}
	if root != v.root {
		return fmt.Errorf("settlement: node state root %s at height %d differs from the header state root %s", root, h, v.root)
	}
	_, err = v.GlobalPauseState()
	return err
}

// pillarInfoVariableName is go-zenon's ABI variable name for a pillar record, which go-zenon
// does not export.
const pillarInfoVariableName = "pillarInfo"

// read returns the proven value stored under key in Settlement storage, or nil when the key is
// proven absent.
func (v *View) read(key []byte) ([]byte, error) {
	return v.readAt(types.SettlementContract, key)
}

// readAt returns the proven value stored under key in contract's storage, or nil when the key
// is proven absent.
func (v *View) readAt(contract types.Address, key []byte) ([]byte, error) {
	raw := append(append(append([]byte{0x03}, contract.Bytes()...), 0x04), key...)
	p, err := v.c.Proof(v.Height, raw)
	if err != nil {
		return nil, err
	}
	if p.Root != v.root {
		return nil, halted(v.Height, "proof root %s differs from the header state root %s", p.Root, v.root)
	}
	path := trie.Path(types.NewHash(raw))
	var ok bool
	if len(p.Value) == 0 {
		ok, err = trie.VerifyAbsenceByPath(v.root, path, p.Proof)
	} else {
		ok, err = trie.VerifyProofByPath(v.root, path, p.Value, p.Proof)
	}
	if err != nil {
		return nil, halted(v.Height, "proof verification failed: %v", err)
	}
	if !ok {
		return nil, halted(v.Height, "proof does not verify against the header state root")
	}
	if len(p.Value) == 0 {
		return nil, nil
	}
	return p.Value, nil
}

// unpack decodes a proven value into out.
func (v *View) unpack(out any, name string, value []byte) error {
	if err := definition.ABISettlement.UnpackVariable(out, name, value); err != nil {
		return halted(v.Height, "proven %s value does not decode: %v", name, err)
	}
	return nil
}

// DomainRecord returns the domain's record, or false when the domain is not registered.
func (v *View) DomainRecord(domainID uint32) (*definition.DomainRecord, bool, error) {
	value, err := v.read((&definition.DomainRecordKey{DomainId: domainID}).Key())
	if err != nil || value == nil {
		return nil, false, err
	}
	rec := new(definition.DomainRecord)
	if err := v.unpack(rec, definition.DomainRecordVariableName, value); err != nil {
		return nil, false, err
	}
	rec.DomainId = domainID
	return rec, true, nil
}

// InputSequenceCursor returns the domain's cursor, all zero when the domain has none.
func (v *View) InputSequenceCursor(domainID uint32) (*definition.InputSequenceCursor, error) {
	value, err := v.read(definition.InputSequenceCursorKey(domainID))
	if err != nil {
		return nil, err
	}
	cur := &definition.InputSequenceCursor{DomainId: domainID}
	if value == nil {
		return cur, nil
	}
	if err := v.unpack(cur, definition.InputSequenceCursorVariableName, value); err != nil {
		return nil, err
	}
	cur.DomainId = domainID
	return cur, nil
}

// ReleaseQueueMeta returns the domain's release queue cursors, both
// zero when the domain has no queue.
func (v *View) ReleaseQueueMeta(domainID uint32) (*definition.ReleaseQueueMeta, error) {
	value, err := v.read(definition.ReleaseQueueMetaKey(domainID))
	if err != nil {
		return nil, err
	}
	m := &definition.ReleaseQueueMeta{DomainId: domainID}
	if value == nil {
		return m, nil
	}
	if err := v.unpack(m, definition.ReleaseQueueMetaVariableName, value); err != nil {
		return nil, err
	}
	m.DomainId = domainID
	return m, nil
}

// BatchEntry returns the domain's batch entry, or false when no such batch was accepted.
func (v *View) BatchEntry(domainID uint32, batchID uint64) (*definition.BatchEntry, bool, error) {
	value, err := v.read((&definition.BatchEntryKey{DomainId: domainID, BatchId: batchID}).Key())
	if err != nil || value == nil {
		return nil, false, err
	}
	e := new(definition.BatchEntry)
	if err := v.unpack(e, definition.BatchEntryVariableName, value); err != nil {
		return nil, false, err
	}
	e.DomainId = domainID
	e.BatchId = batchID
	return e, true, nil
}

// DomainInputCounter returns how many inputs the domain has received.
func (v *View) DomainInputCounter(domainID uint32) (uint64, error) {
	value, err := v.read(definition.DomainInputCounterKey(domainID))
	if err != nil || value == nil {
		return 0, err
	}
	c := new(definition.DomainInputCounter)
	if err := v.unpack(c, definition.DomainInputCounterVariableName, value); err != nil {
		return 0, err
	}
	return c.Count, nil
}

// DomainInputOrdinal returns the domain's input at ordinal, or false when it is absent.
func (v *View) DomainInputOrdinal(domainID uint32, ordinal uint64) (*definition.DomainInputOrdinal, bool, error) {
	value, err := v.read(definition.DomainInputOrdinalKey(domainID, ordinal))
	if err != nil || value == nil {
		return nil, false, err
	}
	o := new(definition.DomainInputOrdinal)
	if err := v.unpack(o, definition.DomainInputOrdinalVariableName, value); err != nil {
		return nil, false, err
	}
	o.DomainId = domainID
	o.Ordinal = ordinal
	return o, true, nil
}

// AssetConfig returns the domain's config for the asset, or false when none is set.
func (v *View) AssetConfig(domainID uint32, assetID types.Hash) (*definition.AssetConfig, bool, error) {
	value, err := v.read(definition.AssetConfigKey(domainID, assetID))
	if err != nil || value == nil {
		return nil, false, err
	}
	c := new(definition.AssetConfig)
	if err := v.unpack(c, definition.AssetConfigVariableName, value); err != nil {
		return nil, false, err
	}
	c.DomainId = domainID
	c.AssetId = assetID
	return c, true, nil
}

func (v *View) pauseState(key []byte) (definition.PauseState, error) {
	value, err := v.read(key)
	if err != nil || value == nil {
		return definition.PauseState{}, err
	}
	var p definition.PauseState
	if err := v.unpack(&p, definition.PauseStateVariableName, value); err != nil {
		return definition.PauseState{}, err
	}
	return p, nil
}

// GlobalPauseState returns the network-wide pause flags.
func (v *View) GlobalPauseState() (definition.PauseState, error) {
	return v.pauseState(definition.GlobalPauseStateKey())
}

// DomainPauseState returns the domain's pause flags.
func (v *View) DomainPauseState(domainID uint32) (definition.PauseState, error) {
	return v.pauseState(definition.DomainPauseStateKey(domainID))
}

// ExecutorSetMember returns the executor-set member registered for the pillar, or false.
func (v *View) ExecutorSetMember(pillarName string) (*definition.ExecutorSetMember, bool, error) {
	value, err := v.read(definition.ExecutorSetMemberKey(pillarName))
	if err != nil || value == nil {
		return nil, false, err
	}
	m := new(definition.ExecutorSetMember)
	if err := v.unpack(m, definition.ExecutorSetMemberVariableName, value); err != nil {
		return nil, false, err
	}
	return m, true, nil
}

// PillarActive reports whether the pillar is registered and not revoked, from the Pillar
// contract's storage. A value that does not decode is an error wrapping recovery.ErrHalted.
func (v *View) PillarActive(name string) (bool, error) {
	value, err := v.readAt(types.PillarContract, definition.GetPillarInfoKey(name))
	if err != nil || value == nil {
		return false, err
	}
	p := new(definition.PillarInfo)
	if err := definition.ABIPillars.UnpackVariable(p, pillarInfoVariableName, value); err != nil {
		return false, halted(v.Height, "proven %s value does not decode: %v", pillarInfoVariableName, err)
	}
	return p.IsActive(), nil
}

// OutboxProcessed reports whether the outbox message was relayed. Settlement stores the single
// byte 0x01 for a processed id, so the value is not ABI-decoded.
func (v *View) OutboxProcessed(outboxID types.Hash) (bool, error) {
	value, err := v.read(definition.ProcessedOutboxKey(outboxID))
	if err != nil || value == nil {
		return false, err
	}
	if !bytes.Equal(value, []byte{0x01}) {
		return false, halted(v.Height, "processed-outbox value %x is not 0x01", value)
	}
	return true, nil
}
