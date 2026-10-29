package batch

import (
	"bytes"
	"fmt"
	"math/big"
	"sort"

	"github.com/zenon-network/go-zenon/common/crypto"
	"github.com/zenon-network/go-zenon/common/types"

	"github.com/Zeno-L2/zeno-executor/internal/codec"
)

// assetTotals is one asset's running batch contribution: depositCredit
// and withdrawalDebit mirror the committed AssetFlowSummary fields;
// capTotal is the MaxBatchWithdrawal aggregate, which excludes
// runtime-emitted refunds, and capLimit is the highest cap among the
// batch's records holding a contract withdrawal in the asset, which
// bounds capTotal. All four are unbounded math/big values; conversion
// to codec.U256 happens only once a candidate has been admitted
// (breach) or the batch is assembled (assetFlowSummary).
type assetTotals struct {
	depositCredit   *big.Int
	withdrawalDebit *big.Int
	capTotal        *big.Int
	capLimit        *big.Int
}

// batchTotals is the in-progress batch's per-asset running totals,
// keyed by AssetID. An asset with no contribution has no entry.
type batchTotals map[codec.AssetID]*assetTotals

func (bt batchTotals) entry(asset codec.AssetID) *assetTotals {
	t, ok := bt[asset]
	if !ok {
		t = &assetTotals{depositCredit: new(big.Int), withdrawalDebit: new(big.Int), capTotal: new(big.Int), capLimit: new(big.Int)}
		bt[asset] = t
	}
	return t
}

// clone returns a deep copy, so a candidate can be evaluated against a scratch totals set
// without mutating the batch's committed totals until the candidate is admitted.
func (bt batchTotals) clone() batchTotals {
	out := make(batchTotals, len(bt))
	for k, v := range bt {
		out[k] = &assetTotals{
			depositCredit:   new(big.Int).Set(v.depositCredit),
			withdrawalDebit: new(big.Int).Set(v.withdrawalDebit),
			capTotal:        new(big.Int).Set(v.capTotal),
			capLimit:        new(big.Int).Set(v.capLimit),
		}
	}
	return out
}

// apply folds rec's contribution into bt: its own deposit (regardless
// of status) into depositCredit only (MaxBatchWithdrawal bounds
// withdrawals, not inbound deposits) and every codec.L1Withdrawal in
// its committed ExecutionResult into withdrawalDebit (including
// refunds) and, when its SourceContract is non-zero, into capTotal too
// (refunds excluded). It also raises capLimit to the cap rec's Ctx.Caps
// answered for that asset when that is higher.
func (bt batchTotals) apply(rec *applied) {
	if rec.depositAmount != (codec.U256{}) {
		t := bt.entry(rec.depositAsset)
		amt := new(big.Int).SetBytes(rec.depositAmount[:])
		t.depositCredit.Add(t.depositCredit, amt)
	}
	for _, m := range rec.eff.OutboxMessages {
		w, ok := m.Target.(codec.L1Withdrawal)
		if !ok {
			continue
		}
		t := bt.entry(w.Asset)
		amt := new(big.Int).SetBytes(w.Amount[:])
		t.withdrawalDebit.Add(t.withdrawalDebit, amt)
		if m.SourceContract != (types.Hash{}) {
			t.capTotal.Add(t.capTotal, amt)
			c := rec.caps[w.Asset]
			if limit := new(big.Int).SetBytes(c[:]); limit.Cmp(t.capLimit) > 0 {
				t.capLimit = limit
			}
		}
	}
}

// breach reports whether bt exceeds MaxAssetsPerBatch's distinct-asset
// footprint, any asset's capTotal exceeds its capLimit, or any asset's
// depositCredit or withdrawalDebit no longer fits a codec.U256. A
// 256-bit overflow is treated exactly as a cap breach.
func (bt batchTotals) breach() bool {
	if len(bt) > codec.MaxAssetsPerBatch {
		return true
	}
	for _, t := range bt {
		if t.capTotal.Cmp(t.capLimit) > 0 {
			return true
		}
		if _, err := codec.U256FromBig(t.depositCredit); err != nil {
			return true
		}
		if _, err := codec.U256FromBig(t.withdrawalDebit); err != nil {
			return true
		}
	}
	return false
}

// assetFlowSummary converts bt into the committed, ascending-by-asset AssetFlowSummary.
func (bt batchTotals) assetFlowSummary() (codec.AssetFlowSummary, error) {
	assets := make([]codec.AssetID, 0, len(bt))
	for asset := range bt {
		assets = append(assets, asset)
	}
	sort.Slice(assets, func(i, j int) bool { return bytes.Compare(assets[i][:], assets[j][:]) < 0 })

	out := make(codec.AssetFlowSummary, 0, len(assets))
	for _, asset := range assets {
		t := bt[asset]
		credit, err := codec.U256FromBig(t.depositCredit)
		if err != nil {
			return nil, fmt.Errorf("batch: asset flow summary: %w", err)
		}
		debit, err := codec.U256FromBig(t.withdrawalDebit)
		if err != nil {
			return nil, fmt.Errorf("batch: asset flow summary: %w", err)
		}
		if credit == (codec.U256{}) && debit == (codec.U256{}) {
			// apply creates an entry unconditionally for a zero-amount
			// withdrawal (bt.entry(w.Asset) above), so this skip is
			// load-bearing, not defensive: it is what keeps the codec's
			// both-amounts-zero rejection (ErrEmptyAssetFlowEntry)
			// satisfied. Such an asset still counts toward breach()'s
			// MaxAssetsPerBatch footprint.
			continue
		}
		out = append(out, codec.AssetFlowEntry{Asset: asset, DepositCredit: credit, WithdrawalDebit: debit})
	}
	return out, nil
}

// admitCandidate evaluates rec against a scratch clone of totals,
// including rec's own contribution. On a breach with isFirst (this
// batch has nothing accumulated yet) it is ErrUnbatchable, a halt. On a
// breach otherwise it reports (false, nil): the caller closes the batch
// and carries rec. Otherwise it commits the scratch totals into *totals
// and reports (true, nil).
func admitCandidate(totals *batchTotals, rec *applied, isFirst bool) (bool, error) {
	scratch := totals.clone()
	scratch.apply(rec)
	if scratch.breach() {
		if isFirst {
			return false, fmt.Errorf("%w", ErrUnbatchable)
		}
		return false, nil
	}
	*totals = scratch
	return true, nil
}

// assembleReceipt builds one input's receipt from its applied record.
func assembleReceipt(rec applied, batchID uint64) (codec.Receipt, error) {
	sdHash, err := codec.StateDiffHash(toDerivedStateDiff(rec.eff.StateDiff))
	if err != nil {
		return codec.Receipt{}, fmt.Errorf("batch: state diff hash: %w", err)
	}
	retHash := codec.ReturnHash(rec.eff.ReturnData)

	eventElems, err := encodeAll(rec.eff.Events, func(e codec.Event) ([]byte, error) { return e.Encode() })
	if err != nil {
		return codec.Receipt{}, fmt.Errorf("batch: encode event: %w", err)
	}
	outboxElems, err := encodeAll(rec.eff.OutboxMessages, func(m codec.OutboxMessage) ([]byte, error) { return m.Encode() })
	if err != nil {
		return codec.Receipt{}, fmt.Errorf("batch: encode outbox message: %w", err)
	}

	return codec.Receipt{
		Version:       codec.ReceiptVersion,
		InputIndex:    rec.input.Ctx.GlobalInputIndex,
		InputHash:     rec.inputHash,
		BatchID:       batchID,
		ContractID:    rec.eff.ContractID,
		CodeHash:      rec.eff.CodeHash,
		Status:        rec.eff.Status,
		GasUsed:       rec.eff.GasUsed,
		AbortCode:     rec.eff.AbortCode,
		StateDiffHash: sdHash,
		ReturnHash:    retHash,
		EventRoot:     codec.MerkleRoot(eventElems),
		EventCount:    uint32(len(eventElems)),
		OutboxRoot:    codec.MerkleRoot(outboxElems),
		OutboxCount:   uint32(len(outboxElems)),
	}, nil
}

// encodeAll encodes every element of items with encode, in order.
func encodeAll[T any](items []T, encode func(T) ([]byte, error)) ([][]byte, error) {
	out := make([][]byte, len(items))
	for i, item := range items {
		b, err := encode(item)
		if err != nil {
			return nil, err
		}
		out[i] = b
	}
	return out, nil
}

// assemble builds and self-verifies the batch over records, which MUST
// be non-empty. batchID is d.batchID; it advances only once assemble
// succeeds (NextBatch's responsibility). It stores the batch's receipt
// sidecar after its bundle, so both exist before the caller commits a
// record naming them.
func (d *Driver) assemble(records []applied) (Batch, error) {
	if len(records) == 0 {
		return Batch{}, fmt.Errorf("batch: assemble: no records")
	}

	receipts := make([]codec.Receipt, len(records))
	inputElems := make([][]byte, len(records))
	receiptElems := make([][]byte, len(records))
	var allEventElems, allOutboxElems [][]byte
	bundleRecords := make([]codec.BundleInputRecord, len(records))

	totals := batchTotals{}

	for i := range records {
		rec := records[i]

		receipt, err := assembleReceipt(rec, d.batchID)
		if err != nil {
			return Batch{}, err
		}
		receipts[i] = receipt

		inputElems[i] = rec.inputData

		encReceipt, err := receipt.Encode()
		if err != nil {
			return Batch{}, fmt.Errorf("batch: encode receipt: %w", err)
		}
		receiptElems[i] = encReceipt

		eventElems, err := encodeAll(rec.eff.Events, func(e codec.Event) ([]byte, error) { return e.Encode() })
		if err != nil {
			return Batch{}, fmt.Errorf("batch: encode event: %w", err)
		}
		allEventElems = append(allEventElems, eventElems...)

		outboxElems, err := encodeAll(rec.eff.OutboxMessages, func(m codec.OutboxMessage) ([]byte, error) { return m.Encode() })
		if err != nil {
			return Batch{}, fmt.Errorf("batch: encode outbox message: %w", err)
		}
		allOutboxElems = append(allOutboxElems, outboxElems...)

		totals.apply(&rec)

		var chunkData []byte
		if len(rec.inputData) > 0 && rec.inputData[0] == 3 {
			chunkData = rec.input.Ctx.SourceData
		}
		bundleRecords[i] = codec.BundleInputRecord{
			GlobalInputIndex: rec.input.Ctx.GlobalInputIndex,
			InputData:        rec.inputData,
			Witnesses:        rec.witnesses,
			ExecutionResult:  rec.encodedResult,
			DeployChunkData:  chunkData,
		}
	}

	summary, err := totals.assetFlowSummary()
	if err != nil {
		return Batch{}, err
	}

	bundle, err := codec.EncodeBundle(bundleRecords)
	if err != nil {
		return Batch{}, fmt.Errorf("batch: encode bundle: %w", err)
	}
	daHash := hashOf(crypto.Hash(bundle))

	if err := d.verifyBundleRoundTrip(daHash, bundle); err != nil {
		return Batch{}, err
	}

	receiptRoot := codec.MerkleRoot(receiptElems)
	sidecar, err := codec.EncodeReceiptSidecar(receipts)
	if err != nil {
		return Batch{}, fmt.Errorf("batch: encode receipt sidecar: %w", err)
	}
	if err := d.cfg.Receipts.Put(daHash, receiptRoot, sidecar); err != nil {
		return Batch{}, fmt.Errorf("batch: receipt store put: %w", err)
	}

	first, last := records[0], records[len(records)-1]

	commitment := codec.BatchCommitment{
		Versions: codec.Versions{
			ProtocolVersion:        codec.ProtocolVersion,
			RuntimeProfileVersion:  codec.RuntimeProfileVersion,
			MeteringVersion:        codec.MeteringVersion,
			SerializationVersion:   codec.SerializationVersion,
			StateDiffVersion:       codec.StateDiffVersion,
			ExecutionResultVersion: codec.ExecutionResultVersion,
			ReceiptVersion:         codec.ReceiptVersion,
			OutboxVersion:          codec.OutboxVersion,
			DAModeVersion:          codec.DAModeVersion,
		},
		DomainID:          d.cfg.DomainID,
		BatchID:           d.batchID,
		FirstInputSeq:     first.input.Ctx.GlobalInputIndex,
		LastInputSeq:      last.input.Ctx.GlobalInputIndex,
		PreStateRoot:      first.preStateRoot,
		PostStateRoot:     last.postStateRoot,
		InputRoot:         codec.MerkleRoot(inputElems),
		ReceiptRoot:       receiptRoot,
		EventRoot:         codec.MerkleRoot(allEventElems),
		OutboxRoot:        codec.MerkleRoot(allOutboxElems),
		DAHash:            daHash,
		DAMode:            DAMode,
		ExecutorID:        d.cfg.ExecutorID,
		SubmittedAtHeight: 0,
		AssetFlowSummary:  summary,
		ProofData:         nil,
	}

	return Batch{Commitment: commitment, Receipts: receipts, Bundle: bundle}, nil
}

// verifyBundleRoundTrip runs the Put/Get/decode/re-encode/re-hash check that keeps NextBatch
// from ever returning an unverified batch: a decode error, a re-encoded byte difference, or a
// hash difference is ErrDAHashMismatch. A BundleStore I/O failure is a separate, generic halt.
func (d *Driver) verifyBundleRoundTrip(daHash types.Hash, bundle []byte) error {
	if err := d.cfg.Bundles.Put(daHash, bundle); err != nil {
		return fmt.Errorf("batch: bundle store put: %w", err)
	}
	stored, err := d.cfg.Bundles.Get(daHash)
	if err != nil {
		return fmt.Errorf("batch: bundle store get: %w", err)
	}
	decoded, err := codec.DecodeBundle(stored)
	if err != nil {
		return fmt.Errorf("%w: decode: %v", ErrDAHashMismatch, err)
	}
	reencoded, err := codec.EncodeBundle(decoded)
	if err != nil {
		return fmt.Errorf("%w: re-encode: %v", ErrDAHashMismatch, err)
	}
	if !bytes.Equal(reencoded, bundle) {
		return fmt.Errorf("%w: bytes differ", ErrDAHashMismatch)
	}
	if hashOf(crypto.Hash(reencoded)) != daHash {
		return fmt.Errorf("%w: hash differs", ErrDAHashMismatch)
	}
	return nil
}
