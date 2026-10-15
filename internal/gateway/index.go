package gateway

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"time"

	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/iterator"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/util"
	"github.com/zenon-network/go-zenon/common/types"

	"github.com/digitalSloth/go-executor/internal/codec"
)

// The index's keys. All integers are big-endian: id is a batch id
// (u64), g a global input index (u64) and j an event or outbox index
// (u32). No prefix is a prefix of another.
const (
	keyMarker    = "t"   // the highest indexed batch id
	keyFinalized = "F"   // the index's finalized batch id
	pfxManifest  = "m/"  // id
	pfxBatch     = "b/"  // id
	pfxDAHash    = "B/"  // daHash
	pfxInput     = "n/"  // g
	pfxSendHash  = "s/"  // sendHash, first writer wins
	pfxSender    = "S/"  // sendHash, kept for good
	pfxKind      = "k/"  // kind(1) g
	pfxSenderIdx = "a/"  // sender(20) g
	pfxContract  = "c/"  // contractId(32) g
	pfxDeploy    = "f/"  // contractId, first writer wins
	pfxDeployer  = "d/"  // deployer(20) g
	pfxCodeHash  = "h/"  // codeHash(32) g
	pfxEvent     = "e/"  // g j
	pfxEventCtr  = "ec/" // contractId g j
	pfxOutbox    = "o/"  // g j
	pfxOutboxTo  = "oa/" // recipient(20) g j
	pfxOutboxID  = "x/"  // outboxId
	pfxOwnerSet  = "w/"  // contractId(32) g
	pfxChunk     = "D/"  // g

	// keyMetaSchema holds the index schema, which indexSchema names.
	// Only the marker's batch writes it, so no manifest lists it and
	// no retraction deletes it.
	keyMetaSchema = "meta/schema"
)

// indexSchema is the version of the index's key set. A build that
// changes the keys it stores raises it, and a cache of another
// version is rebuilt.
const indexSchema = 1

// manifestValue is one batch's manifest: the daHash and receiptRoot
// it was indexed from, whether Settlement had accepted it, and every
// key it added.
type manifestValue struct {
	DAHash      types.Hash
	ReceiptRoot types.Hash
	Accepted    bool
	Keys        [][]byte
}

// batchValue is one indexed batch: its commitment fields and the
// global indexes of its inputs in order.
type batchValue struct {
	FirstInputSeq, LastInputSeq uint64
	PreStateRoot, PostStateRoot types.Hash
	InputRoot, ReceiptRoot      types.Hash
	EventRoot, OutboxRoot       types.Hash
	DAHash                      types.Hash
	Inputs                      []uint64
}

// inputValue is one indexed input.
type inputValue struct {
	BatchID    uint64
	SendHash   types.Hash
	Sender     types.Address
	InputData  []byte
	Receipt    []byte // the receipt's canonical encoding
	ReturnData []byte
}

func be64(v uint64) []byte { return binary.BigEndian.AppendUint64(nil, v) }

func be32(v uint32) []byte { return binary.BigEndian.AppendUint32(nil, v) }

// ikey returns prefix followed by parts.
func ikey(prefix string, parts ...[]byte) []byte {
	k := []byte(prefix)
	for _, p := range parts {
		k = append(k, p...)
	}
	return k
}

// Index is one domain's on-disk index (EXECUTOR §21.6). Poll is its
// only writer; views of it are safe from any goroutine.
type Index struct {
	db      *leveldb.DB
	senders SenderSource
	// timeout bounds each sender read; openIndex sets readTimeout.
	timeout time.Duration
}

// errIndexDamaged marks an index the gateway cannot use: it cannot be
// opened, a value does not decode, or its markers or manifests
// disagree with each other or with the snapshot. OpenChain discards
// the domain's cache on it.
var errIndexDamaged = errors.New("the index is damaged")

// errIndexConflict reports a batch that differs from the one the
// index holds for that id as accepted or finalized, or a retraction of
// a batch the index holds as finalized.
var errIndexConflict = errors.New("the batch differs from the one the index holds for it as accepted or finalized")

// openIndex opens the index of domainID under storagePath, creating
// it when missing.
func openIndex(storagePath string, domainID uint32, senders SenderSource) (*Index, error) {
	p := cachePathsFor(storagePath, domainID)
	if err := guardCache(filepath.Dir(p.index)); err != nil {
		return nil, fmt.Errorf("gateway: domain %d: %w", domainID, err)
	}
	if err := os.MkdirAll(p.index, 0o700); err != nil {
		return nil, fmt.Errorf("gateway: domain %d: %w", domainID, err)
	}
	if err := secureLeaf(p.index, euid()); err != nil {
		return nil, fmt.Errorf("gateway: domain %d: %w", domainID, err)
	}
	db, err := leveldb.OpenFile(p.index, nil)
	if err != nil {
		return nil, fmt.Errorf("%w: the index cannot be opened: %v", errIndexDamaged, err)
	}
	return &Index{db: db, senders: senders, timeout: readTimeout}, nil
}

// Close closes the index.
func (x *Index) Close() error { return x.db.Close() }

// kv is the read side that *leveldb.DB and *leveldb.Snapshot share.
type kv interface {
	Get(key []byte, ro *opt.ReadOptions) ([]byte, error)
	Has(key []byte, ro *opt.ReadOptions) (bool, error)
	NewIterator(slice *util.Range, ro *opt.ReadOptions) iterator.Iterator
}

// indexView reads the index through r: a snapshot for readers on
// other goroutines, or the live database for Poll, the only writer.
type indexView struct {
	r    kv
	snap *leveldb.Snapshot // nil for the live view
}

// view returns a read of the index as it stands now; later writes do
// not change it. The caller must call Release.
func (x *Index) view() (*indexView, error) {
	s, err := x.db.GetSnapshot()
	if err != nil {
		return nil, err
	}
	return &indexView{r: s, snap: s}, nil
}

// live returns a view of the live database. Only Poll's goroutine
// uses it.
func (x *Index) live() *indexView { return &indexView{r: x.db} }

// Release releases the view's snapshot, when it has one.
func (v *indexView) Release() {
	if v.snap != nil {
		v.snap.Release()
	}
}

// get returns the value of key; ok is false when it is missing.
func (v *indexView) get(key []byte) ([]byte, bool, error) {
	b, err := v.r.Get(key, nil)
	if errors.Is(err, leveldb.ErrNotFound) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	return b, true, nil
}

func damaged(key []byte, err error) error {
	return fmt.Errorf("%w: key %q: %v", errIndexDamaged, key, err)
}

// getJSON decodes the JSON value of key into out.
func (v *indexView) getJSON(key []byte, out any) (bool, error) {
	b, ok, err := v.get(key)
	if err != nil || !ok {
		return false, err
	}
	if err := json.Unmarshal(b, out); err != nil {
		return false, damaged(key, err)
	}
	return true, nil
}

// getU64 returns the 8-byte value of key.
func (v *indexView) getU64(key []byte) (uint64, bool, error) {
	b, ok, err := v.get(key)
	if err != nil || !ok {
		return 0, false, err
	}
	if len(b) != 8 {
		return 0, false, damaged(key, fmt.Errorf("value is %d bytes, not 8", len(b)))
	}
	return binary.BigEndian.Uint64(b), true, nil
}

func (v *indexView) marker() (uint64, bool, error) {
	return v.getU64([]byte(keyMarker))
}

func (v *indexView) finalized() (uint64, bool, error) {
	return v.getU64([]byte(keyFinalized))
}

func (v *indexView) manifestAt(id uint64) (manifestValue, bool, error) {
	var m manifestValue
	ok, err := v.getJSON(ikey(pfxManifest, be64(id)), &m)
	return m, ok, err
}

func (v *indexView) batchAt(id uint64) (batchValue, bool, error) {
	var b batchValue
	ok, err := v.getJSON(ikey(pfxBatch, be64(id)), &b)
	return b, ok, err
}

func (v *indexView) inputAt(g uint64) (inputValue, bool, error) {
	var in inputValue
	ok, err := v.getJSON(ikey(pfxInput, be64(g)), &in)
	return in, ok, err
}

func (v *indexView) inputBySendHash(h types.Hash) (uint64, bool, error) {
	return v.getU64(ikey(pfxSendHash, h[:]))
}

func (v *indexView) batchByDAHash(h types.Hash) (uint64, bool, error) {
	return v.getU64(ikey(pfxDAHash, h[:]))
}

func (v *indexView) deployOf(contract types.Hash) (uint64, bool, error) {
	return v.getU64(ikey(pfxDeploy, contract[:]))
}

// outboxAt returns the global input index and outbox index of the
// outbox message id names.
func (v *indexView) outboxAt(id types.Hash) (g uint64, j uint32, ok bool, err error) {
	key := ikey(pfxOutboxID, id[:])
	b, ok, err := v.get(key)
	if err != nil || !ok {
		return 0, 0, false, err
	}
	if len(b) != 12 {
		return 0, 0, false, damaged(key, fmt.Errorf("value is %d bytes, not 12", len(b)))
	}
	return binary.BigEndian.Uint64(b), binary.BigEndian.Uint32(b[8:]), true, nil
}

// eventsOf returns input g's first n event encodings in order.
func (v *indexView) eventsOf(g uint64, n uint32) ([][]byte, error) {
	return v.listOf(pfxEvent, g, n)
}

// outboxOf returns input g's first n outbox encodings in order.
func (v *indexView) outboxOf(g uint64, n uint32) ([][]byte, error) {
	return v.listOf(pfxOutbox, g, n)
}

func (v *indexView) listOf(prefix string, g uint64, n uint32) ([][]byte, error) {
	out := make([][]byte, 0, min(n, 1024))
	for j := uint32(0); j < n; j++ {
		key := ikey(prefix, be64(g), be32(j))
		b, ok, err := v.get(key)
		if err != nil {
			return nil, err
		}
		if !ok {
			return nil, damaged(key, errors.New("missing inside an indexed batch"))
		}
		out = append(out, b)
	}
	return out, nil
}

// scanLimit returns the exclusive upper bound of the keys under
// prefix whose next 8 bytes are at most hi.
func scanLimit(prefix []byte, hi uint64) []byte {
	if hi == math.MaxUint64 {
		return util.BytesPrefix(prefix).Limit
	}
	return append(append([]byte(nil), prefix...), be64(hi+1)...)
}

// scanDesc visits the keys under prefix whose next 8 bytes, a global
// input index, lie in [lo, hi], highest first, and below before when
// before is set. It stops when fn returns false or after max keys.
// stop is the suffix after prefix of the last key it visited when it
// stopped at max, else nil; visited counts the keys it passed to fn.
func (v *indexView) scanDesc(prefix []byte, lo, hi uint64, before []byte, max int, fn func(suffix, value []byte) (bool, error)) (stop []byte, visited int, err error) {
	rng := &util.Range{
		Start: append(append([]byte(nil), prefix...), be64(lo)...),
		Limit: scanLimit(prefix, hi),
	}
	if len(before) > 0 {
		b := append(append([]byte(nil), prefix...), before...)
		if string(b) < string(rng.Limit) {
			rng.Limit = b
		}
	}
	it := v.r.NewIterator(rng, nil)
	defer it.Release()
	for ok := it.Last(); ok; ok = it.Prev() {
		if visited >= max {
			break
		}
		suffix := append([]byte(nil), it.Key()[len(prefix):]...)
		value := append([]byte(nil), it.Value()...)
		visited++
		more, err := fn(suffix, value)
		if err != nil {
			return nil, visited, err
		}
		if !more {
			return nil, visited, it.Error()
		}
		if visited == max {
			return suffix, visited, it.Error()
		}
	}
	return nil, visited, it.Error()
}

// indexBatch is one checked batch as the index stores it.
type indexBatch struct {
	domainID     uint32
	id           uint64
	want         expect
	preStateRoot types.Hash
	inputs       []batchInput
}

// sameIdentity reports whether a batch with daHash and receiptRoot is
// the batch b.
func (b indexBatch) sameIdentity(daHash, receiptRoot types.Hash) bool {
	return daHash == b.want.daHash && receiptRoot == b.want.receiptRoot
}

func (b indexBatch) conflict(daHash, receiptRoot types.Hash) error {
	return fmt.Errorf("batch %d holds daHash %s and receiptRoot %s, not %s and %s: %w", b.id, daHash, receiptRoot, b.want.daHash, b.want.receiptRoot, errIndexConflict)
}

var syncWrite = &opt.WriteOptions{Sync: true}

// add indexes batch b, reading any sender it lacks from the sender
// source under ctx. A batch the index already holds with the same
// daHash and receiptRoot is kept, and its manifest is marked accepted
// when accepted is set. A batch that names a send that is not an input
// of the domain returns an error wrapping errNotDomainInput.
func (x *Index) add(ctx context.Context, b indexBatch, accepted bool) error {
	v := x.live()
	m, ok, err := v.marker()
	if err != nil {
		return err
	}
	f, hasF, err := v.finalized()
	if err != nil {
		return err
	}

	if hasF && b.id <= f {
		bv, has, err := v.batchAt(b.id)
		if err != nil {
			return err
		}
		if !has {
			return fmt.Errorf("%w: batch %d is at or below the finalized batch but not indexed", errIndexDamaged, b.id)
		}
		if b.sameIdentity(bv.DAHash, bv.ReceiptRoot) {
			return nil
		}
		return b.conflict(bv.DAHash, bv.ReceiptRoot)
	}

	if ok && b.id <= m {
		mf, has, err := v.manifestAt(b.id)
		if err != nil {
			return err
		}
		if !has {
			return fmt.Errorf("%w: batch %d has no manifest", errIndexDamaged, b.id)
		}
		if b.sameIdentity(mf.DAHash, mf.ReceiptRoot) {
			if accepted && !mf.Accepted {
				return x.accept(b.id)
			}
			return nil
		}
		if mf.Accepted {
			return b.conflict(mf.DAHash, mf.ReceiptRoot)
		}
		if err := x.retract(b.id); err != nil {
			return err
		}
	} else if (ok && b.id != m+1) || (!ok && b.id != 0) {
		return fmt.Errorf("gateway: index: batch %d does not follow the last indexed batch", b.id)
	}
	return x.write(ctx, v, b, accepted)
}

// write writes batch b, which the index does not hold, in one atomic
// leveldb batch.
func (x *Index) write(ctx context.Context, v *indexView, b indexBatch, accepted bool) error {
	senders := make([]types.Address, len(b.inputs))
	hashes := make([]types.Hash, len(b.inputs))
	for i, in := range b.inputs {
		copy(hashes[i][:], in.inputData[1:33])
		s, err := x.sender(ctx, b.domainID, hashes[i])
		if err != nil {
			return fmt.Errorf("read the L1 sender of send %s: %w", hashes[i], err)
		}
		senders[i] = s
	}

	var batch leveldb.Batch
	var keys [][]byte
	wrote := make(map[string]bool)
	put := func(k, val []byte) {
		keys = append(keys, k)
		batch.Put(k, val)
	}
	putFirst := func(k, val []byte) error {
		if wrote[string(k)] {
			return nil
		}
		has, err := v.r.Has(k, nil)
		if err != nil || has {
			return err
		}
		put(k, val)
		wrote[string(k)] = true
		return nil
	}

	gs := make([]uint64, len(b.inputs))
	for i, in := range b.inputs {
		g, sender := in.gidx, senders[i]
		gs[i] = g
		val, err := json.Marshal(inputValue{
			BatchID: b.id, SendHash: hashes[i], Sender: sender,
			InputData: in.inputData, Receipt: in.leaf, ReturnData: in.result.ReturnData,
		})
		if err != nil {
			return err
		}
		put(ikey(pfxInput, be64(g)), val)
		if err := putFirst(ikey(pfxSendHash, hashes[i][:]), be64(g)); err != nil {
			return err
		}
		put(ikey(pfxKind, in.inputData[:1], be64(g)), nil)
		put(ikey(pfxSenderIdx, sender[:], be64(g)), nil)
		if in.receipt.ContractID != (types.Hash{}) {
			put(ikey(pfxContract, in.receipt.ContractID[:], be64(g)), nil)
		}
		for j, enc := range in.events {
			put(ikey(pfxEvent, be64(g), be32(uint32(j))), enc)
			if c := in.result.Events[j].ContractID; c != (types.Hash{}) {
				put(ikey(pfxEventCtr, c[:], be64(g), be32(uint32(j))), nil)
			}
		}
		for j, enc := range in.outbox {
			jb := be32(uint32(j))
			put(ikey(pfxOutbox, be64(g), jb), enc)
			id := codec.OutboxID(b.domainID, b.id, g, uint32(j))
			put(ikey(pfxOutboxID, id[:]), append(be64(g), jb...))
			if w, ok := in.result.Outbox[j].Target.(codec.L1Withdrawal); ok {
				put(ikey(pfxOutboxTo, w.RecipientL1[:], be64(g), jb), nil)
			}
		}
		if in.inputData[0] == byte(codec.DeployChunk) {
			put(ikey(pfxChunk, be64(g)), in.chunk)
		}
		if err := x.deployKeys(put, putFirst, in, sender); err != nil {
			return err
		}
	}

	bv, err := json.Marshal(batchValue{
		FirstInputSeq: b.want.firstInputSeq, LastInputSeq: b.want.lastInputSeq,
		PreStateRoot: b.preStateRoot, PostStateRoot: b.want.postStateRoot,
		InputRoot: b.want.inputRoot, ReceiptRoot: b.want.receiptRoot,
		EventRoot: b.want.eventRoot, OutboxRoot: b.want.outboxRoot,
		DAHash: b.want.daHash, Inputs: gs,
	})
	if err != nil {
		return err
	}
	put(ikey(pfxBatch, be64(b.id)), bv)
	put(ikey(pfxDAHash, b.want.daHash[:]), be64(b.id))
	mfKey := ikey(pfxManifest, be64(b.id))
	mf, err := json.Marshal(manifestValue{DAHash: b.want.daHash, ReceiptRoot: b.want.receiptRoot, Accepted: accepted, Keys: keys})
	if err != nil {
		return err
	}
	batch.Put(mfKey, mf)

	top := b.id
	if m, ok, err := v.marker(); err != nil {
		return err
	} else if ok && m > top {
		top = m
	}
	batch.Put([]byte(keyMarker), be64(top))
	batch.Put([]byte(keyMetaSchema), be64(indexSchema))
	return x.db.Write(&batch, syncWrite)
}

// deployKeys adds the keys of a successful contract deploy or
// upgrade.
func (x *Index) deployKeys(put func(k, v []byte), putFirst func(k, v []byte) error, in batchInput, sender types.Address) error {
	if in.receipt.Status != codec.StatusSuccess {
		return nil
	}
	switch codec.InputKind(in.inputData[0]) {
	case codec.DeployFinalize:
		if err := putFirst(ikey(pfxDeploy, in.receipt.ContractID[:]), be64(in.gidx)); err != nil {
			return err
		}
		put(ikey(pfxDeployer, sender[:], be64(in.gidx)), in.receipt.ContractID[:])
		put(ikey(pfxOwnerSet, in.receipt.ContractID[:], be64(in.gidx)), nil)
		if dec, err := codec.DecodeInput(in.inputData); err == nil {
			if a, ok := dec.Args.(codec.DeployFinalizeArgs); ok {
				put(ikey(pfxCodeHash, a.CodeHash[:], be64(in.gidx)), nil)
			}
		}
	case codec.TransferOwnership:
		put(ikey(pfxOwnerSet, in.receipt.ContractID[:], be64(in.gidx)), nil)
	case codec.Upgrade:
		if dec, err := codec.DecodeInput(in.inputData); err == nil {
			if a, ok := dec.Args.(codec.UpgradeArgs); ok {
				put(ikey(pfxCodeHash, a.NewCodeHash[:], be64(in.gidx)), nil)
			}
		}
	}
	return nil
}

// sender returns the L1 sender of send h, from the index when it is
// stored and otherwise from the sender source, which it then stores.
func (x *Index) sender(ctx context.Context, domainID uint32, h types.Hash) (types.Address, error) {
	key := ikey(pfxSender, h[:])
	b, ok, err := x.live().get(key)
	if err != nil {
		return types.Address{}, err
	}
	if ok {
		if len(b) != types.AddressSize {
			return types.Address{}, damaged(key, fmt.Errorf("value is %d bytes, not %d", len(b), types.AddressSize))
		}
		a, err := types.BytesToAddress(b)
		if err != nil {
			return types.Address{}, damaged(key, err)
		}
		return a, nil
	}
	addr, err := withDeadline(ctx, x.timeout, func() (types.Address, error) {
		return x.senders.Sender(domainID, h)
	})
	if err != nil {
		return types.Address{}, err
	}
	if err := x.db.Put(key, addr[:], &opt.WriteOptions{Sync: false}); err != nil {
		return types.Address{}, err
	}
	return addr, nil
}

// accept marks batch id's manifest accepted.
func (x *Index) accept(id uint64) error {
	mf, ok, err := x.live().manifestAt(id)
	if err != nil || !ok || mf.Accepted {
		return err
	}
	mf.Accepted = true
	b, err := json.Marshal(mf)
	if err != nil {
		return err
	}
	return x.db.Put(ikey(pfxManifest, be64(id)), b, syncWrite)
}

// finalize deletes batch id's manifest and raises the finalized
// marker to id; the batch's entries stay.
func (x *Index) finalize(id uint64) error {
	f, hasF, err := x.live().finalized()
	if err != nil {
		return err
	}
	if hasF && f > id {
		id = f
	}
	var batch leveldb.Batch
	batch.Delete(ikey(pfxManifest, be64(id)))
	batch.Put([]byte(keyFinalized), be64(id))
	return x.db.Write(&batch, syncWrite)
}

// retract deletes the keys and manifests of batch id and every indexed
// batch above it, and sets the marker below id.
func (x *Index) retract(id uint64) error {
	v := x.live()
	if f, hasF, err := v.finalized(); err != nil {
		return err
	} else if hasF && id <= f {
		return fmt.Errorf("batch %d is finalized in the index: %w", id, errIndexConflict)
	}
	t, ok, err := v.marker()
	if err != nil {
		return err
	}
	if !ok || id > t {
		return nil
	}
	var batch leveldb.Batch
	for k := id; k <= t; k++ {
		mf, has, err := v.manifestAt(k)
		if err != nil {
			return err
		}
		if !has {
			continue
		}
		for _, key := range mf.Keys {
			batch.Delete(key)
		}
		batch.Delete(ikey(pfxManifest, be64(k)))
	}
	if id == 0 {
		batch.Delete([]byte(keyMarker))
	} else {
		batch.Put([]byte(keyMarker), be64(id-1))
	}
	return x.db.Write(&batch, syncWrite)
}

// manifestHead is the part of a manifest the boot walk reads.
type manifestHead struct {
	DAHash      types.Hash
	ReceiptRoot types.Hash
	Accepted    bool
}

// reconcileIndex checks the manifests above the index's finalized
// batch against Settlement at the confirmed height and the executor's
// batch records, and the finalized batch against its Settlement entry
// when that is visible. It keeps the manifests that still match and
// retracts from the first one that does not. It returns the batch at
// which an accepted manifest's entry changed, or nil.
func reconcileIndex(ctx context.Context, x *Index, domainID uint32, start *BatchHead, s Settlement, src Source, timeout time.Duration, logf func(format string, args ...any)) (*uint64, error) {
	v := x.live()
	m, ok, err := v.marker()
	if err != nil {
		return nil, err
	}
	if ok {
		got, has, err := v.getU64([]byte(keyMetaSchema))
		if err != nil {
			return nil, err
		}
		switch {
		case !has:
			return nil, fmt.Errorf("%w: the index schema is missing; this build uses %d", errIndexDamaged, indexSchema)
		case got != indexSchema:
			return nil, fmt.Errorf("%w: the index schema is %d; this build uses %d", errIndexDamaged, got, indexSchema)
		}
	}
	f, hasF, err := v.finalized()
	if err != nil {
		return nil, err
	}
	if start != nil {
		switch {
		case !ok:
			return nil, fmt.Errorf("%w: the index holds no batch, but the snapshot holds batch %d", errIndexDamaged, start.BatchID)
		case m < start.BatchID:
			return nil, fmt.Errorf("%w: the index ends at batch %d, below the snapshot's batch %d", errIndexDamaged, m, start.BatchID)
		case !hasF || f < start.BatchID:
			fs := "none"
			if hasF {
				fs = fmt.Sprint(f)
			}
			return nil, fmt.Errorf("%w: the index's finalized batch is %s, below the snapshot's batch %d", errIndexDamaged, fs, start.BatchID)
		}
	}
	if !ok {
		return nil, nil
	}
	first := uint64(0)
	if hasF {
		first = f + 1
	}

	var heads []manifestHead
	if first <= m {
		heads, err = manifestHeads(v, first, m)
		if err != nil {
			return nil, err
		}
	}
	if !hasF && len(heads) == 0 {
		return nil, nil
	}

	h, err := withDeadline(ctx, timeout, s.ConfirmedHeight)
	if err != nil {
		return nil, fmt.Errorf("gateway: domain %d: settlement: %w", domainID, err)
	}
	view, err := withDeadline(ctx, timeout, func() (SettlementView, error) { return s.At(h) })
	if err != nil {
		return nil, fmt.Errorf("gateway: domain %d: settlement: %w", domainID, err)
	}
	tv := timedView{ctx: ctx, timeout: timeout, view: view}

	if hasF {
		bv, has, err := v.batchAt(f)
		if err != nil {
			return nil, err
		}
		if !has {
			return nil, fmt.Errorf("%w: the finalized batch %d is not indexed", errIndexDamaged, f)
		}
		e, vis, err := tv.BatchEntry(domainID, f)
		if err != nil {
			return nil, fmt.Errorf("gateway: domain %d: settlement: %w", domainID, err)
		}
		if vis && (e.DaHash != bv.DAHash || e.ReceiptRoot != bv.ReceiptRoot) {
			return nil, fmt.Errorf("%w: the finalized batch %d is daHash %s and receiptRoot %s in the index, but daHash %s and receiptRoot %s in Settlement", errIndexDamaged, f, bv.DAHash, bv.ReceiptRoot, e.DaHash, e.ReceiptRoot)
		}
	}

	retractFrom := func(k uint64, reason string) (*uint64, error) {
		if err := x.retract(k); err != nil {
			return nil, fmt.Errorf("gateway: domain %d: index: %w", domainID, err)
		}
		logf("gateway: domain %d: index: batch %d and every indexed batch above it removed at boot (%s)", domainID, k, reason)
		return nil, nil
	}
	for i, mh := range heads {
		k := first + uint64(i)
		e, vis, err := tv.BatchEntry(domainID, k)
		if err != nil {
			return nil, fmt.Errorf("gateway: domain %d: settlement: %w", domainID, err)
		}
		switch {
		case vis && e.DaHash == mh.DAHash && e.ReceiptRoot == mh.ReceiptRoot:
			continue
		case vis && mh.Accepted:
			return &k, nil
		case vis:
			return retractFrom(k, fmt.Sprintf("Settlement accepted daHash %s, receiptRoot %s for it", e.DaHash, e.ReceiptRoot))
		case mh.Accepted:
			continue
		}
		rec, has, err := src.Record(k)
		if err != nil {
			return nil, nil
		}
		if has && rec.DAHash == mh.DAHash {
			if cm, err := codec.DecodeBatchCommitment(rec.Commitment); err == nil && cm.ReceiptRoot == mh.ReceiptRoot {
				continue
			}
		}
		return retractFrom(k, "its batch record was removed or changed")
	}
	return nil, nil
}

// manifestHeads reads the manifests of batches first through last,
// which must run without a gap.
func manifestHeads(v *indexView, first, last uint64) ([]manifestHead, error) {
	prefix := []byte(pfxManifest)
	it := v.r.NewIterator(&util.Range{Start: ikey(pfxManifest, be64(first)), Limit: scanLimit(prefix, last)}, nil)
	defer it.Release()
	var heads []manifestHead
	next := first
	for it.Next() {
		key := it.Key()
		if len(key) != len(prefix)+8 || binary.BigEndian.Uint64(key[len(prefix):]) != next {
			break
		}
		var mh manifestHead
		if err := json.Unmarshal(it.Value(), &mh); err != nil {
			return nil, damaged(key, err)
		}
		heads = append(heads, mh)
		next++
	}
	if err := it.Error(); err != nil {
		return nil, err
	}
	if next != last+1 {
		return nil, fmt.Errorf("%w: batch %d has no manifest", errIndexDamaged, next)
	}
	return heads, nil
}
