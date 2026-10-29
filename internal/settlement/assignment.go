package settlement

import (
	"errors"
	"fmt"

	"github.com/zenon-network/go-zenon/chain/nom"
	"github.com/zenon-network/go-zenon/common/types"

	"github.com/Zeno-L2/zeno-executor/internal/ledger"
	"github.com/Zeno-L2/zeno-executor/internal/recovery"
)

// Assignment implements ledger.AssignmentSource.
//
// A momentum whose hash-checked content lists no Settlement block has an empty assignment and
// needs no proof. Otherwise the supervisor proves the domain's received-input counter at the
// momentum only and serves ordinals nextOrdinal .. count-1. Three halts pin nextOrdinal to the
// counter before the momentum, because only the supervisor holds the momentum's proven state:
// the counter below nextOrdinal, a served ordinal whose send Settlement did not receive in this
// momentum, and ordinal nextOrdinal-1 received in this momentum. An absent ordinal
// nextOrdinal-1 passes: sweeping deletes only finalized batches' ordinals, and a batch
// finalizes at least WithdrawalDelayMin >= MomentumsPerEpoch momenta after it is submitted, so
// a swept ordinal was admitted before this momentum.
func (r *Reader) Assignment(domainID uint32, height uint64, hash types.Hash, nextOrdinal uint64) ([]ledger.AssignmentEntry, error) {
	ms, err := r.c.DetailedMomentumsByHeight(height, 1)
	if err != nil {
		return nil, err
	}
	m, ran, received, err := settlementReceives(ms, height, hash)
	if err != nil {
		return nil, err
	}
	if !ran {
		return nil, nil
	}
	// Build the view from this momentum: a second fetch could return a different momentum.
	v := &View{Height: height, c: r.c, root: m.StateRoot, time: m.TimestampUnix}

	// The node answers getProof from its current chain, so a reorg at height after the fetch
	// above surfaces as a halt from a proof read. The halt stands only when the momentum still
	// hashes to hash.
	guard := func(err error) error {
		if err != nil && errors.Is(err, recovery.ErrHalted) {
			if uerr := r.unchanged(height, hash); uerr != nil {
				return uerr
			}
		}
		return err
	}

	count, err := v.DomainInputCounter(domainID)
	if err != nil {
		return nil, guard(err)
	}
	if count < nextOrdinal {
		return nil, halted(height, "domain %d received-input count %d is below the worker's next ordinal %d", domainID, count, nextOrdinal)
	}
	var entries []ledger.AssignmentEntry
	for o := nextOrdinal; o < count; o++ {
		e, ok, err := v.DomainInputOrdinal(domainID, o)
		if err != nil {
			return nil, guard(err)
		}
		if !ok {
			return nil, halted(height, "domain %d ordinal %d is absent below the received-input count %d: the worker's next ordinal %d is behind Settlement", domainID, o, count, nextOrdinal)
		}
		if !received[e.SourceAccountBlockHash] {
			return nil, halted(height, "domain %d ordinal %d was not admitted at this momentum: the worker's next ordinal %d is behind Settlement", domainID, o, nextOrdinal)
		}
		entries = append(entries, ledger.AssignmentEntry{
			Ordinal:                o,
			GlobalInputIndex:       e.GlobalInputIndex,
			SourceAccountBlockHash: e.SourceAccountBlockHash,
		})
	}
	if nextOrdinal > 0 {
		p, ok, err := v.DomainInputOrdinal(domainID, nextOrdinal-1)
		if err != nil {
			return nil, guard(err)
		}
		if ok && received[p.SourceAccountBlockHash] {
			return nil, halted(height, "domain %d ordinal %d was admitted at this momentum: the worker's next ordinal %d is ahead of Settlement", domainID, nextOrdinal-1, nextOrdinal)
		}
	}
	if len(entries) == 0 {
		return nil, nil
	}

	if err := oneInputPerSend(height, domainID, entries); err != nil {
		return nil, err
	}

	for i := range entries {
		e := &entries[i]
		sh, ok, err := r.c.ConfirmationHeight(e.SourceAccountBlockHash)
		if err != nil {
			return nil, err
		}
		if !ok {
			return nil, fmt.Errorf("settlement: node has no confirmed send %s for domain %d ordinal %d", e.SourceAccountBlockHash, domainID, e.Ordinal)
		}
		if sh == 0 || sh > height {
			return nil, fmt.Errorf("settlement: node confirms send %s at height %d, outside [1, %d]", e.SourceAccountBlockHash, sh, height)
		}
		e.SendMomentumHeight = sh
	}

	sendMomenta := make(map[uint64]*nom.Momentum)
	for _, e := range entries {
		sm, ok := sendMomenta[e.SendMomentumHeight]
		if !ok {
			sms, err := r.c.DetailedMomentumsByHeight(e.SendMomentumHeight, 1)
			if err != nil {
				return nil, err
			}
			if sm, err = checkedMomentum(sms, e.SendMomentumHeight); err != nil {
				return nil, err
			}
			sendMomenta[e.SendMomentumHeight] = sm
		}
		listed := false
		for _, h := range sm.Content {
			if h.Hash == e.SourceAccountBlockHash {
				listed = true
				break
			}
		}
		if !listed {
			return nil, fmt.Errorf("settlement: node's momentum at height %d does not list send %s for domain %d ordinal %d", e.SendMomentumHeight, e.SourceAccountBlockHash, domainID, e.Ordinal)
		}
	}

	if err := r.unchanged(height, hash); err != nil {
		return nil, err
	}
	return entries, nil
}

// oneInputPerSend halts when two entries share a source send. With the per-entry check that
// each ordinal's send was received in this momentum, every input then maps to its own Settlement
// receive block, so a momentum carries at most as many domain inputs as Settlement receives.
func oneInputPerSend(height uint64, domainID uint32, entries []ledger.AssignmentEntry) error {
	seen := make(map[types.Hash]uint64, len(entries))
	for _, e := range entries {
		if first, dup := seen[e.SourceAccountBlockHash]; dup {
			return halted(height, "domain %d ordinals %d and %d share send %s", domainID, first, e.Ordinal, e.SourceAccountBlockHash)
		}
		seen[e.SourceAccountBlockHash] = e.Ordinal
	}
	return nil
}

// unchanged re-fetches the momentum at height and returns a plain error unless it passes
// checkedMomentum and hashes to hash.
func (r *Reader) unchanged(height uint64, hash types.Hash) error {
	ms, err := r.c.DetailedMomentumsByHeight(height, 1)
	if err != nil {
		return err
	}
	m, err := checkedMomentum(ms, height)
	if err != nil {
		return err
	}
	if m.Hash != hash {
		return fmt.Errorf("settlement: node's momentum at height %d changed during the read", height)
	}
	return nil
}

// checkedMomentum returns the node's momentum at height after checking that it is that
// height, has no nil content entry, and hashes to its own Hash.
func checkedMomentum(ms []*nom.DetailedMomentum, height uint64) (*nom.Momentum, error) {
	if len(ms) == 0 || ms[0] == nil || ms[0].Momentum == nil || ms[0].Momentum.Height != height {
		return nil, fmt.Errorf("settlement: node returned no momentum at height %d", height)
	}
	m := ms[0].Momentum
	// Hashing a nil content entry can panic, so this runs before ComputeHash.
	for _, h := range m.Content {
		if h == nil {
			return nil, fmt.Errorf("settlement: momentum at height %d has a nil content entry", height)
		}
	}
	if m.ComputeHash() != m.Hash {
		return nil, fmt.Errorf("settlement: momentum at height %d does not hash to %s", height, m.Hash)
	}
	return m, nil
}

// settlementReceives checks the node's answer for the momentum at (height, hash). ran reports
// whether its content lists a SettlementContract block. received holds the FromBlockHash of
// every hash-checked Settlement contract-receive block in it.
func settlementReceives(ms []*nom.DetailedMomentum, height uint64, hash types.Hash) (m *nom.Momentum, ran bool, received map[types.Hash]bool, err error) {
	m, err = checkedMomentum(ms, height)
	if err != nil {
		return nil, false, nil, err
	}
	if m.Hash != hash {
		return nil, false, nil, fmt.Errorf("settlement: node's momentum at height %d is %s, not %s", height, m.Hash, hash)
	}
	var headers []*types.AccountHeader
	for _, h := range m.Content {
		if h.Address == types.SettlementContract {
			headers = append(headers, h)
		}
	}
	if len(headers) == 0 {
		return m, false, nil, nil
	}
	if m.Version < nom.StateRootMomentumVersion {
		return nil, false, nil, halted(height, "momentum version %d predates StateRootSpork but holds %d Settlement account blocks: StateRootSpork must be active before OffchainExecutionSpork", m.Version, len(headers))
	}
	received = make(map[types.Hash]bool)
	for _, h := range headers {
		var b *nom.AccountBlock
		for _, c := range ms[0].AccountBlocks {
			if c != nil && c.Header() == *h {
				b = c
				break
			}
		}
		if b == nil {
			return nil, false, nil, fmt.Errorf("settlement: momentum at height %d lists Settlement block %s that the node did not return", height, h.Hash)
		}
		// DescendantBlocksHash dereferences each entry, so nil entries are rejected first.
		for _, d := range b.DescendantBlocks {
			if d == nil {
				return nil, false, nil, fmt.Errorf("settlement: Settlement block %s at height %d has a nil descendant", h.Hash, height)
			}
		}
		if b.ComputeHash() != b.Hash {
			return nil, false, nil, fmt.Errorf("settlement: Settlement block %s at height %d does not hash to its hash", h.Hash, height)
		}
		if b.BlockType == nom.BlockTypeContractReceive {
			received[b.FromBlockHash] = true
		}
	}
	return m, true, received, nil
}
