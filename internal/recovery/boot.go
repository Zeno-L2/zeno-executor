package recovery

import (
	"errors"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/zenon-network/go-zenon/chain/nom"
	"github.com/zenon-network/go-zenon/common/types"

	"github.com/digitalSloth/go-executor/domain"
	"github.com/digitalSloth/go-executor/internal/codec"
	"github.com/digitalSloth/go-executor/internal/ledger"
	"github.com/digitalSloth/go-executor/internal/state"
)

// MaxConfirmationDepth is the largest ConfirmationDepth this package accepts. The recorded
// momentum window holds ConfirmationDepth+1 entries and the continuity check reads that whole
// height range in one DetailedMomentumsByHeight call, which upstream serves up to 1024 momenta
// (rpcMaxCountSize, internal/ledger/follower.go:18). The five entry points that take a Config —
// Boot, Commit, Rollback, Reconcile and ForkParent — each validate it against this bound, so the
// guarantee does not depend on the caller being runtime. The file readers and writers
// (ReadCursor, ListBatchRecords, ReadMomentumWindow, PruneAbove, NewLayout, NewBundleStore) take
// no Config and have no depth to check.
const MaxConfirmationDepth = 1023

// ErrHalted marks a consistency fault: the executor stops and stays stopped until an operator
// acts (EXECUTOR §15). It is never auto-retried and never recovered from in-process — including
// by the bounded source retry, which returns a source error wrapping ErrHalted immediately
// rather than sleeping through its budget on a failure that cannot become transient.
var ErrHalted = errors.New("recovery: halted")

// ErrBundleMissing reports that no bundle is stored for a DAHash.
var ErrBundleMissing = errors.New("recovery: bundle missing")

// Anchor is the most recent FINALIZED batch for a domain: the state root it committed and the
// last globalInputIndex it consumed (EXECUTOR §14).
type Anchor struct {
	PostStateRoot types.Hash
	LastInputSeq  uint64
}

// AnchorSource reads the recovery anchor from Settlement storage. ok == false means no batch
// for the domain has ever been FINALIZED, in which case the anchor is genesis — the empty SMT
// with nothing consumed. "Nothing consumed" crosses this seam as ok == false and never as a
// sentinel sequence number: its on-chain representation is a Settlement concern (SPEC §32). An
// implementation whose failure is permanent rather than transient wraps ErrHalted, which skips
// the bounded retry.
type AnchorSource interface {
	FinalizedAnchor(domainID uint32) (Anchor, bool, error)
}

// RetryPolicy is the bounded-retry schedule for the two transient conditions the machine
// tolerates. A zero field takes its package default, so the zero RetryPolicy is the production
// schedule and a test can shrink the intervals it would otherwise sleep through. EXECUTOR §14
// tags both schedules [BINARY]: they shift how quickly an operator is paged, never correctness,
// unlike ConfirmationDepth, which has no default at all.
type RetryPolicy struct {
	// AwaitDAAttempts/AwaitDAInterval re-check one bundle this process wrote itself, which is
	// not subject to network latency: a transient miss is a benign race with its own rename
	// completing, not a condition that benefits from waiting longer. Defaults: 5, 200ms.
	AwaitDAAttempts int
	AwaitDAInterval time.Duration
	// SourceAttempts/SourceInterval bound a znnd read failure and a znnd that is behind the
	// recorded momentum window (EXECUTOR §15). Defaults: 5, 1s.
	SourceAttempts int
	SourceInterval time.Duration
}

// WithDefaults returns p with every zero field replaced by its default.
func (p RetryPolicy) WithDefaults() RetryPolicy {
	if p.AwaitDAAttempts <= 0 {
		p.AwaitDAAttempts = 5
	}
	if p.AwaitDAInterval <= 0 {
		p.AwaitDAInterval = 200 * time.Millisecond
	}
	if p.SourceAttempts <= 0 {
		p.SourceAttempts = 5
	}
	if p.SourceInterval <= 0 {
		p.SourceInterval = time.Second
	}
	return p
}

// Config is everything the recovery machine reads and writes. Boot, Commit, Rollback,
// Reconcile and ForkParent each validate it before touching the tree: ConfirmationDepth must be
// in [1, MaxConfirmationDepth], and Domain, Momenta, Anchors and Bundles must be non-nil for
// the entry points that use them.
type Config struct {
	Layout            Layout
	DomainID          uint32
	ExecutorID        string
	Role              string
	STFSpecHash       types.Hash
	ConfirmationDepth uint64
	Domain            domain.Domain
	Momenta           ledger.MomentumSource
	Anchors           AnchorSource
	Bundles           *BundleStore
	Retry             RetryPolicy
}

const (
	needDomain = 1 << iota
	needMomenta
	needAnchors
	needBundles
)

func (c Config) validate(need int) error {
	if c.ConfirmationDepth < 1 || c.ConfirmationDepth > MaxConfirmationDepth {
		return fmt.Errorf("recovery: ConfirmationDepth %d is outside [1, %d]", c.ConfirmationDepth, MaxConfirmationDepth)
	}
	if need&needDomain != 0 && c.Domain == nil {
		return errors.New("recovery: Config.Domain is nil")
	}
	if need&needMomenta != 0 && c.Momenta == nil {
		return errors.New("recovery: Config.Momenta is nil")
	}
	if need&needAnchors != 0 && c.Anchors == nil {
		return errors.New("recovery: Config.Anchors is nil")
	}
	if need&needBundles != 0 && c.Bundles == nil {
		return errors.New("recovery: Config.Bundles is nil")
	}
	return nil
}

// Ready is the reconciled position the execution loop starts from.
type Ready struct {
	// Store is loaded from a snapshot, or empty and seeded with domain genesis.
	Store *state.Store
	// Position is where to resume; Fresh is true when nothing has been committed, in which case
	// Position is zero and reading starts at momentum height 1 with global index 0.
	Position Position
	Fresh    bool
	// Anchor is the finalized anchor this boot read; HaveAnchor is false when no batch for the
	// domain has ever been finalized. The loop keeps both for the life of the run, since a
	// rollback has to be reconciled against the same anchor.
	Anchor     Anchor
	HaveAnchor bool
	// VerifyAnchor is set when Anchor lies ahead of Position: the loop must compare its
	// re-derived root against Anchor.PostStateRoot at exactly Anchor.LastInputSeq before going
	// past it.
	VerifyAnchor bool
}

func halted(format string, args ...any) error {
	return fmt.Errorf("%w: %s", ErrHalted, fmt.Sprintf(format, args...))
}

// asHalt makes err wrap ErrHalted unless it already does.
func asHalt(err error) error {
	if err == nil || errors.Is(err, ErrHalted) {
		return err
	}
	return fmt.Errorf("%w: %w", ErrHalted, err)
}

func enter(state string) { log.Printf("recovery: %s", state) }

// Boot runs BOOT → LOAD_SNAPSHOT → FIND_RECOVERY_ANCHOR → RECONCILE_FINALIZED_ROOT →
// REPLAY_POST_ANCHOR's local preconditions and returns the position to resume from. Every
// consistency fault — a substituted or unloadable snapshot, an anchor-root mismatch, a missing
// bundle for an un-finalized batch, a reorg deeper than the recorded window — returns an error
// wrapping ErrHalted.
func Boot(cfg Config) (Ready, error) {
	if err := cfg.validate(needDomain | needMomenta | needAnchors | needBundles); err != nil {
		return Ready{}, err
	}
	rdy, err := boot(cfg)
	if err != nil {
		if rdy.Store != nil {
			rdy.Store.Close()
		}
		return Ready{}, asHalt(err)
	}
	enter("READY")
	return rdy, nil
}

func boot(cfg Config) (Ready, error) {
	var rdy Ready

	enter("BOOT")
	if _, err := OpenMeta(cfg.Layout, Meta{
		DomainID:    cfg.DomainID,
		ExecutorID:  cfg.ExecutorID,
		STFSpecHash: cfg.STFSpecHash,
		Role:        cfg.Role,
	}); err != nil {
		return rdy, err
	}

	enter("LOAD_SNAPSHOT")
	pos, found, err := ReadCursor(cfg.Layout)
	if err != nil {
		return rdy, err
	}
	if !found {
		if err := PruneAbove(cfg.Layout, Position{}); err != nil {
			return rdy, err
		}
		store, err := openGenesis(cfg)
		if err != nil {
			return rdy, err
		}
		rdy = Ready{Store: store, Fresh: true}
	} else {
		if err := validatePosition(pos); err != nil {
			return rdy, halted("cursor is malformed: %v", err)
		}
		if err := PruneAbove(cfg.Layout, pos); err != nil {
			return rdy, err
		}
		height, ok, err := ForkParent(cfg, pos)
		if err != nil {
			return rdy, err
		}
		if !ok {
			return rdy, halted("reorg deeper than every momentum height this executor still has a record of (window ends at height %d)", pos.MomentumHeight)
		}
		if height == pos.MomentumHeight {
			store, err := loadSnapshot(cfg, pos)
			if err != nil {
				return rdy, err
			}
			rdy = Ready{Store: store, Position: pos}
		} else {
			enter("ROLLBACK")
			rdy, err = Rollback(cfg, height)
			if err != nil {
				return rdy, err
			}
		}
	}

	enter("FIND_RECOVERY_ANCHOR")
	var anchor Anchor
	var haveAnchor bool
	err = withSourceRetry(cfg.Retry, "read finalized anchor", func() error {
		var err error
		anchor, haveAnchor, err = cfg.Anchors.FinalizedAnchor(cfg.DomainID)
		return err
	})
	if err != nil {
		return rdy, err
	}
	rdy.Anchor, rdy.HaveAnchor = anchor, haveAnchor

	enter("RECONCILE_FINALIZED_ROOT")
	verify, err := Reconcile(cfg, anchor, haveAnchor, rdy.Position, rdy.Fresh)
	if err != nil {
		return rdy, err
	}
	rdy.VerifyAnchor = verify

	enter("REPLAY_POST_ANCHOR")
	if err := checkBundles(cfg, anchor, haveAnchor); err != nil {
		return rdy, err
	}
	return rdy, nil
}

// openGenesis returns an empty store seeded with Domain.Genesis().
func openGenesis(cfg Config) (*state.Store, error) {
	init := cfg.Domain.Genesis()
	if len(init.Paths) != len(init.Values) {
		return nil, fmt.Errorf("recovery: genesis StateInit has %d paths and %d values", len(init.Paths), len(init.Values))
	}
	store, err := state.OpenEmpty(cfg.Layout.StateWAL)
	if err != nil {
		return nil, fmt.Errorf("recovery: %w", err)
	}
	if len(init.Paths) == 0 {
		return store, nil
	}
	diff := make([]domain.StateDiffEntry, len(init.Paths))
	for i := range init.Paths {
		diff[i] = domain.StateDiffEntry{Path: init.Paths[i], Value: init.Values[i]}
	}
	if _, err := store.Apply(diff); err != nil {
		store.Close()
		return nil, fmt.Errorf("recovery: seed genesis: %w", err)
	}
	return store, nil
}

// loadSnapshot opens the store for pos from the snapshot whose input sequence is pos's, whose
// stfSpecHash tag is the configured one and whose leaf set hashes to pos.PostStateRoot. It halts
// naming why the candidate failed: missing, tagged for another stfSpecHash, or rooted
// differently. This is the snapshot-substitution and local-corruption gate.
func loadSnapshot(cfg Config, pos Position) (*state.Store, error) {
	infos, err := state.ListSnapshots(cfg.Layout.Snapshots)
	if err != nil {
		return nil, fmt.Errorf("recovery: %w", err)
	}
	var wrongTag, wrongRoot, unloadable int
	found := 0
	for _, info := range infos {
		if info.Header.InputSeq != pos.LastInputSeq {
			continue
		}
		found++
		if info.Header.STFSpecHash != cfg.STFSpecHash {
			wrongTag++
			continue
		}
		store, err := state.OpenSnapshot(cfg.Layout.StateWAL, cfg.Layout.Snapshots, info.Name, cfg.STFSpecHash)
		if err != nil {
			unloadable++
			continue
		}
		root, err := store.Root()
		if err == nil && root == pos.PostStateRoot {
			return store, nil
		}
		store.Close()
		wrongRoot++
	}
	switch {
	case found == 0:
		return nil, halted("no snapshot at input sequence %d: the cursor's durable state image is missing", pos.LastInputSeq)
	case wrongRoot > 0:
		return nil, halted("snapshot at input sequence %d is rooted differently from the cursor's postStateRoot %s", pos.LastInputSeq, pos.PostStateRoot)
	case wrongTag > 0 && unloadable == 0:
		return nil, halted("snapshot at input sequence %d is tagged for another stfSpecHash than %s", pos.LastInputSeq, cfg.STFSpecHash)
	}
	return nil, halted("snapshot at input sequence %d could not be loaded", pos.LastInputSeq)
}

// withSourceRetry runs fn up to policy.SourceAttempts times, SourceInterval apart. An error
// wrapping ErrHalted returns immediately. Exhaustion returns an error wrapping ErrHalted and the
// last failure.
func withSourceRetry(p RetryPolicy, what string, fn func() error) error {
	p = p.WithDefaults()
	for attempt := 1; ; attempt++ {
		err := fn()
		if err == nil {
			return nil
		}
		if errors.Is(err, ErrHalted) {
			return err
		}
		if attempt >= p.SourceAttempts {
			return fmt.Errorf("%w: %s failed %d times: %w", ErrHalted, what, attempt, err)
		}
		time.Sleep(p.SourceInterval)
	}
}

// checkBundles is AWAIT_DA: every batch record above the anchor must have its bundle stored.
func checkBundles(cfg Config, anchor Anchor, haveAnchor bool) error {
	records, err := ListBatchRecords(cfg.Layout)
	if err != nil {
		return err
	}
	p := cfg.Retry.WithDefaults()
	for _, rec := range records {
		if haveAnchor && rec.LastInputSeq <= anchor.LastInputSeq {
			continue
		}
		present, err := cfg.Bundles.Has(rec.DAHash)
		if err != nil {
			return err
		}
		if present {
			continue
		}
		enter("AWAIT_DA")
		for attempt := 0; !present && attempt < p.AwaitDAAttempts; attempt++ {
			time.Sleep(p.AwaitDAInterval)
			if present, err = cfg.Bundles.Has(rec.DAHash); err != nil {
				return err
			}
		}
		if !present {
			return halted("AWAIT_DA: bundle %s of batch %d (inputs %d-%d) is missing after %d re-checks",
				rec.DAHash, rec.BatchID, rec.FirstInputSeq, rec.LastInputSeq, p.AwaitDAAttempts)
		}
	}
	return nil
}

// Reconcile runs RECONCILE_FINALIZED_ROOT for one position and reports whether the caller must
// still verify the anchor's root against re-derived state. It branches on fresh before any
// sequence comparison: a fresh Position is the zero Position, and globalInputIndex starts at 0,
// so its LastInputSeq is indistinguishable by value from a committed position that consumed
// exactly index 0. When the anchor lies below the position it verifies against the local
// BatchRecord whose input range contains the anchor's index, through that record's stored
// bundle. Boot calls it once; the loop calls it again after every rollback, against the
// rolled-back position, which re-arms verification when the rollback landed below the anchor and
// halts when the re-derived stream disagrees with the finalized one at the anchor's index.
func Reconcile(cfg Config, anchor Anchor, haveAnchor bool, pos Position, fresh bool) (verify bool, err error) {
	if err := cfg.validate(needBundles); err != nil {
		return false, err
	}
	switch {
	case !haveAnchor:
		return false, nil
	case fresh:
		return true, nil
	case anchor.LastInputSeq > pos.LastInputSeq:
		return true, nil
	case anchor.LastInputSeq == pos.LastInputSeq:
		if pos.PostStateRoot != anchor.PostStateRoot {
			return false, halted("local root %s at input %d differs from the finalized root %s", pos.PostStateRoot, pos.LastInputSeq, anchor.PostStateRoot)
		}
		return false, nil
	}
	return false, asHalt(verifyAgainstRecord(cfg, anchor))
}

// verifyAgainstRecord checks the anchor against the local batch record whose input range
// contains the anchor's index, through that record's stored bundle.
func verifyAgainstRecord(cfg Config, anchor Anchor) error {
	records, err := ListBatchRecords(cfg.Layout)
	if err != nil {
		return err
	}
	var rec *BatchRecord
	for i := range records {
		if records[i].FirstInputSeq <= anchor.LastInputSeq && anchor.LastInputSeq <= records[i].LastInputSeq {
			rec = &records[i]
			break
		}
	}
	if rec == nil {
		return halted("no local batch record contains the finalized anchor's input %d", anchor.LastInputSeq)
	}
	bundle, err := cfg.Bundles.Get(rec.DAHash)
	if err != nil {
		return halted("bundle of batch %d covering finalized input %d: %v", rec.BatchID, anchor.LastInputSeq, err)
	}
	inputs, err := codec.DecodeBundle(bundle)
	if err != nil {
		return halted("bundle of batch %d does not decode: %v", rec.BatchID, err)
	}
	for _, in := range inputs {
		if in.GlobalInputIndex != anchor.LastInputSeq {
			continue
		}
		result, err := codec.DecodeExecutionResult(in.ExecutionResult)
		if err != nil {
			return halted("execution result at input %d does not decode: %v", in.GlobalInputIndex, err)
		}
		if result.PostStateRoot != anchor.PostStateRoot {
			return halted("local root %s at input %d differs from the finalized root %s", result.PostStateRoot, in.GlobalInputIndex, anchor.PostStateRoot)
		}
		return nil
	}
	return halted("bundle of batch %d has no record at the finalized anchor's input %d", rec.BatchID, anchor.LastInputSeq)
}

// anchoredWindow is the window a continuity check runs against: the recorded window when its
// last entry is exactly {pos.MomentumHeight, pos.MomentumHash}, and that single entry otherwise.
func anchoredWindow(l Layout, pos Position) ([]MomentumRef, error) {
	recorded, err := ReadMomentumWindow(l)
	if err != nil {
		return nil, err
	}
	self := MomentumRef{Height: pos.MomentumHeight, Hash: pos.MomentumHash}
	if n := len(recorded); n > 0 && recorded[n-1] == self {
		return recorded, nil
	}
	return []MomentumRef{self}, nil
}

// ForkParent returns the highest height of pos's momentum window whose hash the source still
// agrees with. height == pos.MomentumHeight means the chain still extends what was consumed; a
// lower height is the fork parent to roll back to; ok == false is a fork below every height this
// executor still has a record of, which is a halt at both call sites.
//
// The window it compares is anchored on pos, not taken from the file alone: the recorded window
// when its last entry is exactly {pos.MomentumHeight, pos.MomentumHash}, and the single entry
// {pos.MomentumHeight, pos.MomentumHash} otherwise. The fallback covers an empty file, which a
// commit interrupted before its cursor rename leaves once the next boot's PruneAbove has cut its
// window back to the cursor's height, and defensively any window that does not end at the
// cursor. pos must be a committed position; pos.MomentumHeight == 0 is a caller bug and returns
// an error, since a fresh position has consumed nothing and its caller rolls back to genesis
// without searching.
//
// A source whose frontier is below the window's top height, or whose ranged read does not reach
// it, is unavailable rather than forked: ForkParent retries under the policy and then returns an
// error wrapping ErrHalted that names the lag, never a fork.
func ForkParent(cfg Config, pos Position) (height uint64, ok bool, err error) {
	if err := cfg.validate(needMomenta); err != nil {
		return 0, false, err
	}
	if pos.MomentumHeight == 0 {
		return 0, false, errors.New("recovery: ForkParent needs a committed position, got momentum height 0")
	}
	window, err := anchoredWindow(cfg.Layout, pos)
	if err != nil {
		return 0, false, err
	}
	top := window[len(window)-1].Height

	var byHeight map[uint64]types.Hash
	err = withSourceRetry(cfg.Retry, "continuity check", func() error {
		frontier, err := cfg.Momenta.FrontierHeight()
		if err != nil {
			return err
		}
		if frontier < top {
			return fmt.Errorf("the node's frontier %d is below the window's top height %d", frontier, top)
		}
		moms, err := cfg.Momenta.DetailedMomentumsByHeight(window[0].Height, uint64(len(window)))
		if err != nil {
			return err
		}
		got := momentumHashes(moms)
		for _, w := range window {
			if _, present := got[w.Height]; !present {
				return fmt.Errorf("ranged read of %d momenta from height %d returned nothing at height %d (frontier %d, window top %d)",
					len(window), window[0].Height, w.Height, frontier, top)
			}
		}
		byHeight = got
		return nil
	})
	if err != nil {
		return 0, false, err
	}

	for i := len(window) - 1; i >= 0; i-- {
		if byHeight[window[i].Height] == window[i].Hash {
			return window[i].Height, true, nil
		}
	}
	return 0, false, nil
}

func momentumHashes(moms []*nom.DetailedMomentum) map[uint64]types.Hash {
	out := make(map[uint64]types.Hash, len(moms))
	for _, dm := range moms {
		if dm != nil && dm.Momentum != nil {
			out[dm.Momentum.Height] = dm.Momentum.Hash
		}
	}
	return out
}

// Rollback restores the executor to the newest committed position at or below forkParent —
// genesis when no snapshot qualifies, in which case it removes the cursor rather than writing
// one — rewriting the cursor, deleting every artifact above it, and leaving the momentum window
// anchored on that cursor: the prune's survivors when their last entry is already
// {Position.MomentumHeight, Position.MomentumHash}, and that single entry written over them
// otherwise. All of that happens before the store is opened. forkParent may be 0, which selects
// genesis: that is how a caller with a fresh position asks to restart from genesis after a reorg.
// forkParent equal to a committed position's own height is ordinary rather than a call the caller
// should skip — it is how the loop reloads a store it discarded on a reorg above the cursor — and
// it retracts nothing. The returned Ready carries Store, Position and Fresh only: it has no
// anchor knowledge, so the caller runs Reconcile against the returned position, then builds a
// fresh Follower and Driver there and re-derives forward.
//
// The window step keeps the prune's survivors only when they end at the new cursor's pair. Every
// survivor is then at or below forkParent, whose hash the source just confirmed, so the
// survivors are its canonical ancestors and checking against them is sound. A prune that leaves
// nothing, or a window that stops short of the cursor, takes the single-entry branch, which is
// the deepest tolerance that can be justified: the next commit starts refilling the window and
// it regains ConfirmationDepth+1 entries once that many momenta have been consumed, so until
// then a second reorg below the entries it holds halts rather than rolling back again.
//
// The configured SettlementFilter must be stateless across Follower instances. The caller
// builds a fresh Follower after this returns and hands it the same filter, which re-walks
// momenta the retired Follower already fed it, on a different chain. Phase 1's UnwiredFilter
// is stateless.
//
// The plugin's local code store is not rolled back: domain.Domain exposes no rollback hook.
// A replayed canonical DeployStartArgs deletes every chunk slot and the pending record for its
// code_hash before rewriting them, so a retracted start's slots are rewritten idempotently, and
// a finalize or upgrade naming a code_hash whose start was retracted is not an input at all,
// because Settlement accepted no matching start on the canonical chain. The known hazard is a
// kind-3 chunk written by DecodeInput above the fork that no canonical DeployStartArgs
// rewrites: it survives the rollback, so prepareModule reads a stale chunk where a node that
// never reorged reads absence, and a local-recovery halt becomes a committed failed status.
// That is a replay-determinism divergence between a reorged executor and a fresh replay. The
// operator remedy is to stop the executor, remove the domain directory and the plugin's
// code-store directory, and re-sync from genesis. The hazard is recorded in
// EXECUTOR §15.
func Rollback(cfg Config, forkParent uint64) (Ready, error) {
	if err := cfg.validate(needDomain); err != nil {
		return Ready{}, err
	}
	snaps, err := listPositioned(cfg.Layout)
	if err != nil {
		return Ready{}, err
	}
	var chosen *positioned
	for i := range snaps { // ascending: the last match is the newest
		s := &snaps[i]
		if s.pos.MomentumHeight <= forkParent && s.info.Header.STFSpecHash == cfg.STFSpecHash {
			chosen = s
		}
	}

	if chosen == nil {
		enter("ROLLBACK to genesis")
		if err := os.Remove(cfg.Layout.Cursor); err != nil && !errors.Is(err, os.ErrNotExist) {
			return Ready{}, fmt.Errorf("recovery: remove cursor: %w", err)
		}
		if err := PruneAbove(cfg.Layout, Position{}); err != nil {
			return Ready{}, err
		}
		store, err := openGenesis(cfg)
		if err != nil {
			return Ready{}, err
		}
		return Ready{Store: store, Fresh: true}, nil
	}

	pos := chosen.pos
	enter(fmt.Sprintf("ROLLBACK to momentum height %d", pos.MomentumHeight))
	if err := writeJSON(cfg.Layout.Cursor, pos); err != nil {
		return Ready{}, fmt.Errorf("recovery: write cursor: %w", err)
	}
	if err := PruneAbove(cfg.Layout, pos); err != nil {
		return Ready{}, err
	}
	survivors, err := ReadMomentumWindow(cfg.Layout)
	if err != nil {
		return Ready{}, err
	}
	self := MomentumRef{Height: pos.MomentumHeight, Hash: pos.MomentumHash}
	if n := len(survivors); n == 0 || survivors[n-1] != self {
		if err := writeMomentumWindow(cfg.Layout, []MomentumRef{self}); err != nil {
			return Ready{}, err
		}
	}
	store, err := loadSnapshot(cfg, pos)
	if err != nil {
		return Ready{}, err
	}
	return Ready{Store: store, Position: pos}, nil
}
