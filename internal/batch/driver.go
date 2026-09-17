package batch

import (
	"bytes"
	"errors"
	"fmt"
	"math/big"
	"sort"

	"github.com/zenon-network/go-zenon/common/crypto"
	"github.com/zenon-network/go-zenon/common/trie"
	"github.com/zenon-network/go-zenon/common/types"

	"github.com/digitalSloth/go-executor/domain"
	"github.com/digitalSloth/go-executor/internal/codec"
	"github.com/digitalSloth/go-executor/internal/ledger"
	"github.com/digitalSloth/go-executor/internal/state"
)

// MaxBatchInputs is the §25.1 per-batch input count cap.
const MaxBatchInputs = 64

// DAMode is the §20 DA mode ordinal; 0 is the only Phase-1 value.
const DAMode uint8 = 0

// InputSource is the ordered per-domain input stream. *ledger.Follower satisfies it; ok ==
// false with a nil error means the source has nothing further right now.
type InputSource interface {
	Next() (ledger.Input, bool, error)
}

// BundleStore is the persistence seam the DAHash round-trip reads back through. Ticket 07
// owns the on-disk implementation.
type BundleStore interface {
	Put(daHash types.Hash, bundle []byte) error
	Get(daHash types.Hash) ([]byte, error)
}

// Config is a Driver's construction-time configuration. Every field is validated by New.
type Config struct {
	DomainID     uint32
	ExecutorID   types.Address
	Domain       domain.Domain
	Store        *state.Store
	Inputs       InputSource
	Bundles      BundleStore
	FirstBatchID uint64
}

// Batch is one built, self-verified batch. The commitment carries DAHash and the roots; Bundle
// is the exact bytes DAHash covers and the DA layer publishes.
type Batch struct {
	Commitment codec.BatchCommitment
	Receipts   []codec.Receipt
	Bundle     []byte
}

var (
	// ErrDAHashMismatch reports a batch's DA bundle round-trip (Put, Get, decode, re-encode,
	// re-hash) failed to reproduce the bundle bytes DAHash commits.
	ErrDAHashMismatch = errors.New("batch: DA bundle round-trip mismatch")
	// ErrUnbatchable reports that the first input of a batch alone exceeds a per-batch cap.
	// plugins/wasm's ordered validation step 5a makes this unreachable: it rejects any input
	// whose own withdrawal-plus-deposit footprint would breach either cap, so every input the
	// STF settles as SUCCESS fits within one batch alone. Kept as a structural, fail-closed
	// guard.
	ErrUnbatchable = errors.New("batch: first input of a batch alone exceeds a per-batch cap")
	// ErrEffectsInvariant reports that an input's Effects carried content that violates a
	// committed invariant: on a non-SUCCESS status, §14.3/§15 forbid a non-empty StateDiff,
	// Events or ReturnData, a non-zero ClaimedDeposit, a non-zero AbortCode outside REVERT, or an
	// OutboxMessages set other than empty or exactly one runtime-emitted deposit refund; on
	// SUCCESS, ClaimedDeposit must not exceed the input's own deposit_amount (§18.3).
	ErrEffectsInvariant = errors.New("batch: non-success effects carry committed content")
	// ErrDriverReorged reports that NextBatch was called again on a Driver that already returned
	// a *ledger.ReorgError. The caller must discard this Driver and construct a fresh one over
	// the rolled-back store instead of reusing it.
	ErrDriverReorged = errors.New("batch: driver reused after a *ledger.ReorgError")
)

// Driver runs the per-input execution pipeline against Config.Domain and Config.Store, slices
// Config.Inputs' stream into batches under the §25.1/§27.9 caps, and assembles and
// self-verifies each batch through NextBatch.
//
// Config.Store's exclusive ownership by the Driver, for its entire lifetime, is a precondition:
// the Driver caches the store's current root and refreshes it only from its own Store.Apply
// calls, so a concurrent writer would desync the cache from the store's true root.
type Driver struct {
	cfg Config

	root    types.Hash
	rootSet bool

	batchID uint64

	// carried is the first input of the next batch when a cap closed the previous one: computed
	// through step 7, unapplied, resumed at step 8 by the next NextBatch call.
	carried *applied

	// reorged is set once NextBatch returns a *ledger.ReorgError, and makes every subsequent call
	// fail with ErrDriverReorged instead of silently caching a stale root past an external
	// rollback the Driver cannot observe (see NextBatch's doc comment).
	reorged bool
}

// New returns a Driver over cfg, rejecting a nil Domain, Store, Inputs or Bundles.
func New(cfg Config) (*Driver, error) {
	if cfg.Domain == nil {
		return nil, errors.New("batch: Config.Domain is nil")
	}
	if cfg.Store == nil {
		return nil, errors.New("batch: Config.Store is nil")
	}
	if cfg.Inputs == nil {
		return nil, errors.New("batch: Config.Inputs is nil")
	}
	if cfg.Bundles == nil {
		return nil, errors.New("batch: Config.Bundles is nil")
	}
	return &Driver{cfg: cfg, batchID: cfg.FirstBatchID}, nil
}

// applied is one input's per-input pipeline output, computed through step 7 (witness proofs).
// postStateRoot and encodedResult are set only once steps 9-10 have run; a carried record is the
// same value short of those two steps.
type applied struct {
	input         ledger.Input
	inputData     []byte
	inputHash     types.Hash
	depositAsset  codec.AssetID
	depositAmount codec.U256

	eff domain.Effects

	preStateRoot  types.Hash
	postStateRoot types.Hash

	witnesses []codec.WitnessEntry

	encodedResult []byte
}

// NextBatch executes inputs from Config.Inputs until a batch closes, then assembles and
// self-verifies it (the DAHash round-trip below). Its error contract, by class:
//
//   - ok == false with a nil error: the source yielded nothing and nothing was accumulated.
//
//   - A source error with nothing accumulated: returned wrapped with %w, never reclassified, so
//     errors.As(err, **ledger.ReorgError) and errors.Is(err, ledger.ErrIndexOverflow) both
//     survive to the caller. ledger.Follower.Next documents three distinct classes — an
//     unlatched MomentumSource read error the caller may retry, a one-shot *ReorgError, and a
//     latched permanent error — and EXECUTOR.md §15 gives each a different behaviour ("retry
//     with backoff" vs "roll back cursor+state" vs HALTED). Classification and backoff are the
//     caller's; NextBatch must not collapse them into a halt.
//
//   - A *ledger.ReorgError, whatever is accumulated: discard the accumulated records and return
//     the error wrapped. NextBatch never closes and returns a batch on this class. A reorg means
//     the ledger no longer extends the last momentum the follower consumed, so inputs already
//     popped and applied may come from a retracted momentum; returning them with a nil error
//     would let the caller submit them before its next call tells it about the reorg. Nothing is
//     lost by discarding: the store already carries their writes, and ticket 07's rollback must
//     restore every durable side effect a discarded record produced, not only the cursor and the
//     state trie: Store.Apply appends each diff to the durable WAL before this method returns
//     (internal/state/store.go:117), so a rollback that only rewrites in-memory leaves lands back
//     on the discarded root after a restart replays that log; and Domain.DecodeInput (step 4,
//     called before any admission or cap decision) can mutate a domain plugin's own local,
//     non-consensus store outside the state trie — plugins/wasm's code store records a kind-3
//     input's chunk bytes and a kind-2 input's pending record (plugins/wasm/wasm.go:104,123-134) —
//     which a state-root rollback cannot reach and which ticket 07 must rebuild or re-derive
//     itself.
//
//   - After a *ledger.ReorgError, the caller MUST NOT reuse this Driver instance: it caches the
//     store's current root (currentRoot) and only ever refreshes it from its own Store.Apply
//     calls, so it has no way to observe an external rollback of the store it does not own past
//     that point. Construct a fresh Driver once the store (and every side effect named above) has
//     been rolled back. Every subsequent NextBatch call on this Driver fails with
//     ErrDriverReorged instead of silently committing a stale PreStateRoot.
//
//   - A transient-read or latched-permanent source error with records already accumulated: close
//     the batch with what is accumulated and return it with a nil error, exactly as a clean
//     drain does. Neither class retracts anything already read. The error is not stashed: a read
//     error is not latched and nothing advanced, and a permanent error is latched, so both
//     re-present on the next call. This is what keeps already-executed, already-applied inputs
//     from being dropped between batches.
//
//   - Every non-source error is a halt: a Domain.DecodeInput/Domain.Apply error, an encode error,
//     ErrDAHashMismatch, ErrEffectsInvariant, ErrUnbatchable.
//
// Outside a reorg discard, the store always equals the last returned batch's PostStateRoot: cap
// evaluation (step 8) precedes Store.Apply, so the store never runs ahead of a batch NextBatch
// has not yet returned. A reorg discard is the one case that breaks the invariant — the store is
// then ahead of it by the discarded records' writes — and restoring it is ticket 07's rollback to
// perform, not the Driver's.
func (d *Driver) NextBatch() (Batch, bool, error) {
	if d.reorged {
		return Batch{}, false, ErrDriverReorged
	}

	var records []applied
	totals := batchTotals{}

	if d.carried != nil {
		rec := *d.carried
		d.carried = nil

		// isFirst=true: a breach here is ErrUnbatchable (edge case 5), never a close-and-carry,
		// since this batch has nothing accumulated yet to close.
		if _, err := admitCandidate(&totals, &rec, true); err != nil {
			return Batch{}, false, err
		}

		rec, err := d.finishApply(rec)
		if err != nil {
			return Batch{}, false, err
		}
		records = append(records, rec)
	}

	for len(records) < MaxBatchInputs {
		in, ok, err := d.cfg.Inputs.Next()
		if err != nil {
			var reorgErr *ledger.ReorgError
			if errors.As(err, &reorgErr) {
				d.reorged = true
				return Batch{}, false, fmt.Errorf("batch: %w", err)
			}
			if len(records) == 0 {
				return Batch{}, false, fmt.Errorf("batch: %w", err)
			}
			break
		}
		if !ok {
			if len(records) == 0 {
				return Batch{}, false, nil
			}
			break
		}

		rec, err := d.computeRecord(in)
		if err != nil {
			return Batch{}, false, err
		}

		admitted, err := admitCandidate(&totals, &rec, len(records) == 0)
		if err != nil {
			return Batch{}, false, err
		}
		if !admitted {
			d.carried = &rec
			break
		}

		rec, err = d.finishApply(rec)
		if err != nil {
			return Batch{}, false, err
		}
		records = append(records, rec)
	}

	batch, err := d.assemble(records)
	if err != nil {
		return Batch{}, false, err
	}
	d.batchID++
	return batch, true, nil
}

// currentRoot returns the driver's cached pre-state root, seeded by one Store.Root() call at
// the first input the driver ever executes.
func (d *Driver) currentRoot() (types.Hash, error) {
	if d.rootSet {
		return d.root, nil
	}
	root, err := d.cfg.Store.Root()
	if err != nil {
		return types.Hash{}, fmt.Errorf("batch: root: %w", err)
	}
	d.root = root
	d.rootSet = true
	return root, nil
}

// computeRecord runs steps 1-7 of the per-input pipeline against in: the cached pre-state root,
// the canonical input encoding and its deposit, decode, apply, the non-SUCCESS effects
// invariant, and the witness proofs. Steps 8-10 are driven by NextBatch.
func (d *Driver) computeRecord(in ledger.Input) (applied, error) {
	var rec applied
	rec.input = in

	pre, err := d.currentRoot()
	if err != nil {
		return applied{}, err
	}
	rec.preStateRoot = pre

	inputData, err := codec.CanonicalInput(in.Payload)
	if err != nil {
		return applied{}, fmt.Errorf("batch: canonical input: %w", err)
	}
	rec.inputData = inputData
	rec.inputHash = hashOf(crypto.Hash(inputData))

	ci, err := codec.DecodeInput(inputData)
	switch {
	case err == nil:
		rec.depositAsset, rec.depositAmount = depositOf(ci.Args)
	case errors.Is(err, codec.ErrInvalidKind):
		// An unsupported-kind input carries no deposit; not a halt (edge case per §27.5).
	default:
		return applied{}, fmt.Errorf("batch: decode canonical input: %w", err)
	}

	acc := d.cfg.Store.Accessor()

	dIn, err := d.cfg.Domain.DecodeInput(in.Ctx, in.Payload)
	if err != nil {
		return applied{}, fmt.Errorf("batch: domain decode input: %w", err)
	}

	eff, err := d.cfg.Domain.Apply(acc, dIn)
	if err != nil {
		return applied{}, fmt.Errorf("batch: domain apply: %w", err)
	}
	rec.eff = eff

	if eff.Status != codec.StatusSuccess {
		if err := checkNonSuccessInvariant(eff); err != nil {
			return applied{}, err
		}
	} else if err := checkClaimedDeposit(eff.ClaimedDeposit, rec.depositAmount); err != nil {
		return applied{}, err
	}

	keys := witnessKeys(acc.Witnesses(), eff.StateDiff)
	witnesses := make([]codec.WitnessEntry, 0, len(keys))
	for _, k := range keys {
		_, _, proof, err := d.cfg.Store.Prove(k)
		if err != nil {
			return applied{}, fmt.Errorf("batch: prove: %w", err)
		}
		witnesses = append(witnesses, codec.WitnessEntry{DerivedKey: k, Proof: proof})
	}
	rec.witnesses = witnesses

	return rec, nil
}

// finishApply runs steps 9-10: on SUCCESS, Store.Apply and refresh the cached root; otherwise
// postStateRoot equals preStateRoot and the store is untouched. Either way it assembles and
// encodes the ExecutionResult.
func (d *Driver) finishApply(rec applied) (applied, error) {
	if rec.eff.Status == codec.StatusSuccess {
		post, err := d.cfg.Store.Apply(rec.eff.StateDiff)
		if err != nil {
			return applied{}, fmt.Errorf("batch: store apply: %w", err)
		}
		rec.postStateRoot = post
		d.root = post
		d.rootSet = true
	} else {
		rec.postStateRoot = rec.preStateRoot
	}

	result := codec.ExecutionResult{
		Version:          codec.ExecutionResultVersion,
		GlobalInputIndex: rec.input.Ctx.GlobalInputIndex,
		PreStateRoot:     rec.preStateRoot,
		PostStateRoot:    rec.postStateRoot,
		StateDiff:        toDerivedStateDiff(rec.eff.StateDiff),
		Events:           rec.eff.Events,
		Outbox:           rec.eff.OutboxMessages,
		ReturnData:       rec.eff.ReturnData,
		ClaimedDeposit:   rec.eff.ClaimedDeposit,
	}
	encoded, err := result.Encode()
	if err != nil {
		return applied{}, fmt.Errorf("batch: encode execution result: %w", err)
	}
	rec.encodedResult = encoded
	return rec, nil
}

// checkNonSuccessInvariant enforces edge case 7 / §14.3 / §15: a non-SUCCESS input's Effects
// carry no StateDiff, Events or ReturnData, a zero ClaimedDeposit, AbortCode zero unless the
// status is REVERT, and an OutboxMessages set that is either empty or exactly one message that
// is a codec.L1Withdrawal at OutboxIndex 0 with a zero SourceContract — the runtime-emitted
// deposit refund, the only outbox content a failed input may carry.
func checkNonSuccessInvariant(eff domain.Effects) error {
	if len(eff.StateDiff) != 0 || len(eff.Events) != 0 || len(eff.ReturnData) != 0 {
		return fmt.Errorf("%w: non-empty StateDiff/Events/ReturnData", ErrEffectsInvariant)
	}
	if eff.ClaimedDeposit != (codec.U256{}) {
		return fmt.Errorf("%w: non-zero ClaimedDeposit", ErrEffectsInvariant)
	}
	if eff.AbortCode != 0 && eff.Status != codec.StatusRevert {
		return fmt.Errorf("%w: non-zero AbortCode outside REVERT", ErrEffectsInvariant)
	}
	switch len(eff.OutboxMessages) {
	case 0:
	case 1:
		m := eff.OutboxMessages[0]
		if _, ok := m.Target.(codec.L1Withdrawal); !ok {
			return fmt.Errorf("%w: outbox message is not an L1Withdrawal", ErrEffectsInvariant)
		}
		if m.OutboxIndex != 0 {
			return fmt.Errorf("%w: refund OutboxIndex != 0", ErrEffectsInvariant)
		}
		if m.SourceContract != (types.Hash{}) {
			return fmt.Errorf("%w: refund SourceContract != zero", ErrEffectsInvariant)
		}
	default:
		return fmt.Errorf("%w: more than one outbox message", ErrEffectsInvariant)
	}
	return nil
}

// checkClaimedDeposit enforces, on a SUCCESS effects, that the plugin's own ClaimedDeposit does
// not exceed the input's deposit_amount — the bound plugins/wasm's step 4 already enforces
// (effects.go:107), re-checked here at the plugin seam because ClaimedDeposit is content a
// plugin returns and the driver commits on trust otherwise (§18.3 conservation). Violation is
// ErrEffectsInvariant, the same sentinel checkNonSuccessInvariant uses for a plugin's untrusted
// effects.
func checkClaimedDeposit(claimed, depositAmount codec.U256) error {
	if new(big.Int).SetBytes(claimed[:]).Cmp(new(big.Int).SetBytes(depositAmount[:])) > 0 {
		return fmt.Errorf("%w: ClaimedDeposit exceeds the input's deposit_amount", ErrEffectsInvariant)
	}
	return nil
}

// witnessKeys returns the union of observed paths and diff paths, deduplicated and ascending.
// On a non-SUCCESS input diff is empty (checkNonSuccessInvariant enforces it), so the union
// reduces to the observed paths alone (edge case 9) with no special-casing needed here.
func witnessKeys(observed []state.Witness, diff []domain.StateDiffEntry) []trie.Path {
	set := make(map[trie.Path]bool, len(observed)+len(diff))
	for _, w := range observed {
		set[w.Path] = true
	}
	for _, e := range diff {
		set[e.Path] = true
	}
	keys := make([]trie.Path, 0, len(set))
	for k := range set {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool { return bytes.Compare(keys[i][:], keys[j][:]) < 0 })
	return keys
}

// depositOf projects a decoded input's own deposit onto (asset, amount), zero for a kind with
// no deposit field.
func depositOf(args codec.InputArgs) (codec.AssetID, codec.U256) {
	switch a := args.(type) {
	case codec.CallL2Args:
		return a.DepositAsset, a.DepositAmount
	case codec.DepositArgs:
		return a.DepositAsset, a.DepositAmount
	default:
		return codec.AssetID{}, codec.U256{}
	}
}

// toDerivedStateDiff projects domain.StateDiffEntry onto codec.StateDiffEntry, preserving
// order; the domain plugin is responsible for canonical (ascending, deduplicated) ordering.
func toDerivedStateDiff(entries []domain.StateDiffEntry) []codec.StateDiffEntry {
	out := make([]codec.StateDiffEntry, len(entries))
	for i, e := range entries {
		out[i] = codec.StateDiffEntry{Key: e.Path, Value: e.Value}
	}
	return out
}

// hashOf packs a 32-byte hash's raw bytes into a types.Hash.
func hashOf(b []byte) types.Hash {
	var out types.Hash
	copy(out[:], b)
	return out
}
