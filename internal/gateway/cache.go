package gateway

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/zenon-network/go-zenon/common/types"

	"github.com/digitalSloth/go-executor/internal/state"
)

// snapshotExt is the extension state.SaveSnapshot gives a snapshot
// file.
const snapshotExt = ".snap"

// cachePaths are one domain's gateway cache paths under storagePath.
type cachePaths struct {
	snapshots, index string
}

// cachePathsFor returns the cache paths for domainID under
// storagePath, creating nothing.
func cachePathsFor(storagePath string, domainID uint32) cachePaths {
	root := filepath.Join(storagePath, strconv.FormatUint(uint64(domainID), 10))
	return cachePaths{
		snapshots: filepath.Join(root, "snapshots"),
		index:     filepath.Join(root, "index"),
	}
}

// OpenChain boots one domain's engine. It opens the gateway's
// finalized state from its newest snapshot, checked against
// Settlement, or from genesis, opens and reconciles the domain's
// index, and discards the domain's cache when the snapshot or the
// index cannot be trusted. Its Settlement reads end when ctx does.
func OpenChain(ctx context.Context, storagePath string, d DomainConfig, s Settlement, senders SenderSource, logf func(format string, args ...any)) (*Chain, error) {
	switch d.Source {
	case "LOCAL":
		if sameTree(storagePath, d.ExecutorStoragePath) {
			return nil, fmt.Errorf("gateway: domain %d: storagePath %q and executorStoragePath %q overlap", d.DomainID, storagePath, d.ExecutorStoragePath)
		}
	case "REMOTE":
	default:
		return nil, fmt.Errorf("gateway: domain %d: source %q is not LOCAL or REMOTE", d.DomainID, d.Source)
	}
	final, start, err := openFinal(ctx, storagePath, d.DomainID, s, readTimeout, logf)
	if err != nil {
		return nil, err
	}
	var src Source
	if d.Source == "REMOTE" {
		rs, err := NewRemoteSource(storagePath, d.DomainID, d.Upstreams, logf)
		if err != nil {
			_ = final.Close()
			return nil, err
		}
		src = rs
	} else {
		src = NewLocalSource(d.ExecutorStoragePath, d.DomainID)
	}
	var bad *uint64
	idx, err := openIndex(storagePath, d.DomainID, senders)
	if err == nil {
		bad, err = reconcileIndex(ctx, idx, d.DomainID, start, s, src, readTimeout, logf)
	}
	if errors.Is(err, errIndexDamaged) {
		if idx != nil {
			_ = idx.Close()
		}
		_ = final.Close()
		final, start, idx, err = rebuildCache(ctx, storagePath, d.DomainID, s, senders, err.Error(), logf)
		bad = nil
	}
	if err != nil {
		if idx != nil {
			_ = idx.Close()
		}
		if final != nil {
			_ = final.Close()
		}
		return nil, err
	}
	if start != nil {
		logf("gateway: domain %d: booted from the snapshot of finalized batch %d", d.DomainID, start.BatchID)
	} else {
		logf("gateway: domain %d: booted at genesis", d.DomainID)
	}
	c := NewChain(ChainConfig{
		DomainID:   d.DomainID,
		Source:     src,
		Settlement: s,
		Final:      final,
		Start:      start,
		Snapshots:  cachePathsFor(storagePath, d.DomainID).snapshots,
		Index:      idx,
		Logf:       logf,
	})
	if bad != nil {
		c.markInconsistent(*bad, errors.New("the accepted Settlement entry differs from the batch the gateway indexed"))
	}
	return c, nil
}

// rebuildCache discards the domain's cache for reason and opens an
// empty finalized state and index. A second index damage is an error,
// never another rebuild.
func rebuildCache(ctx context.Context, storagePath string, domainID uint32, s Settlement, senders SenderSource, reason string, logf func(format string, args ...any)) (*state.Store, *BatchHead, *Index, error) {
	logf("gateway: domain %d: discarding the gateway cache (%s); rebuilding from batch 0", domainID, reason)
	if err := guardCache(filepath.Join(storagePath, strconv.FormatUint(uint64(domainID), 10))); err != nil {
		return nil, nil, nil, fmt.Errorf("gateway: domain %d: %w", domainID, err)
	}
	if err := discardCache(storagePath, domainID); err != nil {
		return nil, nil, nil, err
	}
	final, start, err := openFinal(ctx, storagePath, domainID, s, readTimeout, logf)
	if err != nil {
		return nil, nil, nil, err
	}
	idx, err := openIndex(storagePath, domainID, senders)
	if err != nil {
		_ = final.Close()
		return nil, nil, nil, err
	}
	return final, start, idx, nil
}

// Close closes the domain's index, when it has one, and its finalized
// state. Call it once Poll has returned for good.
func (c *Chain) Close() error {
	var idxErr, finalErr error
	if c.cfg.Index != nil {
		if err := c.cfg.Index.Close(); err != nil {
			idxErr = fmt.Errorf("gateway: domain %d: close the index: %w", c.cfg.DomainID, err)
		}
	}
	if err := c.cfg.Final.Close(); err != nil {
		finalErr = fmt.Errorf("gateway: domain %d: close the finalized state: %w", c.cfg.DomainID, err)
	}
	return errors.Join(idxErr, finalErr)
}

// openFinal opens the finalized state for domainID and returns the
// batch it holds, nil at genesis. Each Settlement read it makes
// fails after timeout or when ctx ends.
func openFinal(ctx context.Context, storagePath string, domainID uint32, s Settlement, timeout time.Duration, logf func(format string, args ...any)) (*state.Store, *BatchHead, error) {
	p := cachePathsFor(storagePath, domainID)
	cacheRoot := filepath.Dir(p.snapshots)
	mkdirs := func() error {
		if err := guardCache(cacheRoot); err != nil {
			return fmt.Errorf("gateway: domain %d: %w", domainID, err)
		}
		for _, dir := range []string{p.snapshots} {
			if err := os.MkdirAll(dir, 0o700); err != nil {
				return fmt.Errorf("gateway: domain %d: %w", domainID, err)
			}
		}
		for _, dir := range []string{cacheRoot, p.snapshots} {
			if err := secureLeaf(dir, euid()); err != nil {
				return fmt.Errorf("gateway: domain %d: %w", domainID, err)
			}
		}
		return nil
	}
	discard := func(reason string) (*state.Store, *BatchHead, error) {
		logf("gateway: domain %d: discarding the gateway cache (%s); rebuilding from batch 0", domainID, reason)
		if err := guardCache(cacheRoot); err != nil {
			return nil, nil, fmt.Errorf("gateway: domain %d: %w", domainID, err)
		}
		if err := discardCache(storagePath, domainID); err != nil {
			return nil, nil, err
		}
		if err := mkdirs(); err != nil {
			return nil, nil, err
		}
		return state.OpenEmpty(), nil, nil
	}

	if err := mkdirs(); err != nil {
		return nil, nil, err
	}
	infos, err := state.ListSnapshots(p.snapshots)
	if err != nil {
		return discard(fmt.Sprintf("the snapshot directory cannot be read: %v", err))
	}
	if len(infos) == 0 {
		return state.OpenEmpty(), nil, nil
	}
	info := infos[len(infos)-1]
	if len(info.Header.Meta) != 8 {
		return discard(fmt.Sprintf("snapshot %s carries no batch id", info.Name))
	}
	id := binary.BigEndian.Uint64(info.Header.Meta)

	h, err := withDeadline(ctx, timeout, s.ConfirmedHeight)
	if err != nil {
		return nil, nil, fmt.Errorf("gateway: domain %d: settlement: %w", domainID, err)
	}
	view, err := withDeadline(ctx, timeout, func() (SettlementView, error) { return s.At(h) })
	if err != nil {
		return nil, nil, fmt.Errorf("gateway: domain %d: settlement: %w", domainID, err)
	}
	tv := timedView{ctx: ctx, timeout: timeout, view: view}
	e, ok, err := tv.BatchEntry(domainID, id)
	if err != nil {
		return nil, nil, fmt.Errorf("gateway: domain %d: settlement: %w", domainID, err)
	}
	if !ok {
		return discard(fmt.Sprintf("batch %d's entry is not visible at height %d", id, h))
	}
	rec, ok, err := tv.DomainRecord(domainID)
	if err != nil {
		return nil, nil, fmt.Errorf("gateway: domain %d: settlement: %w", domainID, err)
	}
	if !ok {
		return discard(fmt.Sprintf("the domain record is not visible at height %d", h))
	}

	if err := guardCache(cacheRoot); err != nil {
		return nil, nil, fmt.Errorf("gateway: domain %d: %w", domainID, err)
	}
	store, err := state.OpenSnapshot(p.snapshots, info.Name, rec.StfSpecHash)
	if err != nil {
		return discard(err.Error())
	}
	root, err := store.Root()
	if err != nil {
		_ = store.Close()
		return nil, nil, fmt.Errorf("gateway: domain %d: %w", domainID, err)
	}
	if root != e.PostStateRoot {
		_ = store.Close()
		return discard(fmt.Sprintf("the snapshot's root %s is not batch %d's postStateRoot %s", root, id, e.PostStateRoot))
	}
	return store, &BatchHead{BatchID: id, LastInputSeq: e.LastInputSeq, PostStateRoot: e.PostStateRoot}, nil
}

// discardCache removes the domain's snapshots/ and index/ under
// storagePath, and nothing else: never storagePath, never the domain
// directory, and never a REMOTE gateway's bundles/ and receipts/.
func discardCache(storagePath string, domainID uint32) error {
	p := cachePathsFor(storagePath, domainID)
	for _, dir := range []string{p.snapshots, p.index} {
		if err := os.RemoveAll(dir); err != nil {
			return fmt.Errorf("gateway: discard %s: %w", dir, err)
		}
	}
	return nil
}

// writeSnapshot saves final's leaf set to dir, tagged with head, and
// removes every other entry in dir.
func writeSnapshot(final *state.Store, dir string, head BatchHead, stfSpecHash types.Hash) error {
	if err := guardCache(filepath.Dir(dir)); err != nil {
		return err
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	if err := secureLeaf(dir, euid()); err != nil {
		return err
	}
	name, err := final.SaveSnapshot(dir, state.SnapshotHeader{
		InputSeq:    head.LastInputSeq,
		STFSpecHash: stfSpecHash,
		Meta:        binary.BigEndian.AppendUint64(nil, head.BatchID),
	})
	if err != nil {
		return err
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return err
	}
	for _, e := range entries {
		if e.Name() == name+snapshotExt {
			continue
		}
		if err := os.RemoveAll(filepath.Join(dir, e.Name())); err != nil {
			return err
		}
	}
	return nil
}
