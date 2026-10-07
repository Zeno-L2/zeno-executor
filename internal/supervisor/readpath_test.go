package supervisor

import (
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/zenon-network/go-zenon/common/types"

	"github.com/digitalSloth/go-executor/domain"
	"github.com/digitalSloth/go-executor/internal/codec"
	"github.com/digitalSloth/go-executor/internal/ipc"
	"github.com/digitalSloth/go-executor/internal/ledger"
	"github.com/digitalSloth/go-executor/internal/recovery"
	"github.com/digitalSloth/go-executor/internal/settlement"
	"github.com/digitalSloth/go-executor/internal/znnd"
	"github.com/digitalSloth/go-executor/internal/znnd/znndtest"
)

// emptyDomain is a domain with no genesis state, for a boot that only reads the anchor.
type emptyDomain struct{}

func (emptyDomain) Genesis() domain.StateInit { return domain.StateInit{} }
func (emptyDomain) DecodeInput(domain.InputContext, []byte) (domain.Input, error) {
	return nil, errors.New("emptyDomain decodes nothing")
}
func (emptyDomain) Apply(domain.StateAccess, domain.Input) (domain.Effects, error) {
	return domain.Effects{}, errors.New("emptyDomain applies nothing")
}

// readPathClient serves the L1 read path over a worker channel and returns the worker's IPC
// client.
func readPathClient(t *testing.T, fx *znndtest.Fixture, srv *znndtest.Server, depth uint64) *ipc.Client {
	t.Helper()
	sEnd, wEnd := newChannel(t)

	executor := types.Address{0x09}
	cfg := Config{ExecutorAddress: executor, NetworkName: "devnet", ConfirmationDepth: depth, StoragePath: "/var/lib/executor"}
	w := WorkerConfig{DomainID: fx.DomainID, Role: "EXECUTOR", STFSpecHash: types.Hash{0x01}, MemoryLimit: 1 << 30, CPULimit: 1}
	if err := ipc.WriteMessage(sEnd, cfg.workerConfig(w)); err != nil {
		t.Fatalf("WriteMessage config: %v", err)
	}

	zc := znnd.NewClient(srv.URL)
	src := Sources{Momenta: zc, Anchors: settlement.NewAnchorSource(settlement.NewReader(zc), depth), Assignments: settlement.NewReader(zc), Caps: NewCapCache(settlement.NewReader(zc))}
	done := runServe(t, sEnd, w, executor, src, &workerState{})

	client := ipc.NewClient(wEnd, nil)
	if _, err := client.ReadConfig(); err != nil {
		t.Fatalf("ReadConfig: %v", err)
	}
	t.Cleanup(func() {
		wEnd.Close()
		if err := waitServe(t, done); err != nil {
			t.Errorf("serve = %v, want nil", err)
		}
	})
	return client
}

func bootConfig(t *testing.T, fx *znndtest.Fixture, depth uint64, client *ipc.Client) recovery.Config {
	t.Helper()
	layout, err := recovery.NewLayout(t.TempDir(), fx.DomainID)
	if err != nil {
		t.Fatalf("NewLayout: %v", err)
	}
	bundles, err := recovery.NewBundleStore(layout.Bundles)
	if err != nil {
		t.Fatalf("NewBundleStore: %v", err)
	}
	return recovery.Config{
		Layout:            layout,
		DomainID:          fx.DomainID,
		ExecutorID:        "exec-1",
		Role:              "EXECUTOR",
		STFSpecHash:       types.Hash{0x01},
		ConfirmationDepth: depth,
		Domain:            emptyDomain{},
		Momenta:           client,
		Anchors:           client,
		Bundles:           bundles,
		Retry:             recovery.RetryPolicy{SourceAttempts: 2, SourceInterval: 10 * time.Millisecond},
	}
}

func TestReadPathEndToEnd(t *testing.T) {
	const depth = 10
	fx := znndtest.Load(t)
	srv := znndtest.NewServer(t, fx, fx.ReadHeight+depth)
	client := readPathClient(t, fx, srv, depth)

	want := recovery.Anchor{PostStateRoot: fx.Expected.BatchEntries[0].PostStateRoot, LastInputSeq: fx.Expected.BatchEntries[0].LastInputSeq}

	if h, err := client.FrontierHeight(); err != nil || h != fx.ReadHeight+depth {
		t.Fatalf("FrontierHeight = %d, %v; want %d", h, err, fx.ReadHeight+depth)
	}
	ms, err := client.DetailedMomentumsByHeight(fx.ReadHeight-9, 10)
	if err != nil || len(ms) != 10 {
		t.Fatalf("DetailedMomentumsByHeight = %d momenta, %v; want 10", len(ms), err)
	}

	got, ok, err := client.FinalizedAnchor(fx.DomainID)
	if err != nil || !ok || got != want {
		t.Fatalf("FinalizedAnchor = %+v, %v, %v; want %+v", got, ok, err, want)
	}

	cfg := bootConfig(t, fx, depth, client)
	pos := recovery.Position{LastInputSeq: want.LastInputSeq, PostStateRoot: want.PostStateRoot}
	if _, err := recovery.Reconcile(cfg, got, true, pos, false); err != nil {
		t.Fatalf("Reconcile at the anchor: %v", err)
	}
	pos.PostStateRoot = types.Hash{0xEE}
	if _, err := recovery.Reconcile(cfg, got, true, pos, false); !errors.Is(err, recovery.ErrHalted) {
		t.Fatalf("Reconcile with a differing root = %v, want ErrHalted", err)
	}

	rdy, err := recovery.Boot(cfg)
	if err != nil {
		t.Fatalf("Boot: %v", err)
	}
	defer rdy.Store.Close()
	if !rdy.HaveAnchor || !rdy.VerifyAnchor || rdy.Anchor != want {
		t.Fatalf("Boot = HaveAnchor %v, VerifyAnchor %v, Anchor %+v; want true, true, %+v", rdy.HaveAnchor, rdy.VerifyAnchor, rdy.Anchor, want)
	}
}

func TestAssignmentEndToEnd(t *testing.T) {
	fx := znndtest.Load(t)
	a := fx.Assignment
	srv := znndtest.NewServer(t, fx, a.ReceiveHeight+10)
	client := readPathClient(t, fx, srv, 10)
	hash := fx.DetailedMomentum(t, a.ReceiveHeight).Momentum.Hash
	c0 := fx.Expected.DomainInputCounter

	got, err := client.Assignment(fx.DomainID, a.ReceiveHeight, hash, c0)
	if err != nil {
		t.Fatalf("Assignment: %v", err)
	}
	var want []ledger.AssignmentEntry
	for _, e := range a.Entries {
		want = append(want, ledger.AssignmentEntry(e))
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("Assignment = %+v, want %+v", got, want)
	}

	_, err = client.Assignment(fx.DomainID, a.ReceiveHeight, types.Hash{0x01}, c0-1)
	if err == nil || errors.Is(err, recovery.ErrHalted) || client.Broken() != nil || !strings.Contains(err.Error(), "not "+(types.Hash{0x01}).String()) {
		t.Fatalf("Assignment with another hash = %v, Broken() = %v; want a plain error", err, client.Broken())
	}

	if _, err := client.Assignment(fx.DomainID, a.ReceiveHeight, hash, c0+1); !errors.Is(err, recovery.ErrHalted) {
		t.Fatalf("Assignment with a cursor ahead = %v, want ErrHalted", err)
	}
}

func TestAssignmentPreStateRootHaltsOverTheChannel(t *testing.T) {
	fx := znndtest.Load(t)
	h := fx.Assignment.PreStateRootSettlementHeight
	srv := znndtest.NewServer(t, fx, fx.Assignment.ReceiveHeight+10)
	client := readPathClient(t, fx, srv, 10)
	hash := fx.DetailedMomentum(t, h).Momentum.Hash

	_, err := client.Assignment(fx.DomainID, h, hash, 0)
	if !errors.Is(err, recovery.ErrHalted) || client.Broken() != nil {
		t.Fatalf("Assignment at a pre-state-root Settlement momentum = %v, Broken() = %v; want ErrHalted on a live channel", err, client.Broken())
	}
}

func TestCapEndToEnd(t *testing.T) {
	fx := znndtest.Load(t)
	srv := znndtest.NewServer(t, fx, fx.Assignment.ReceiveHeight+10)
	client := readPathClient(t, fx, srv, 10)
	h := fx.ReadHeight
	hash := fx.DetailedMomentum(t, h).Momentum.Hash
	want, err := codec.U256FromBig(fx.Expected.AssetConfig.MaxBatchWithdrawal)
	if err != nil {
		t.Fatalf("U256FromBig: %v", err)
	}
	configured, unconfigured := codec.AssetID(fx.AssetID), codec.AssetID(fx.UnconfiguredAssetID)

	check := func(label string) {
		t.Helper()
		got, err := client.MaxBatchWithdrawals(fx.DomainID, h, hash, []codec.AssetID{configured, unconfigured})
		if wantAll := []codec.U256{want, {}}; err != nil || !reflect.DeepEqual(got, wantAll) {
			t.Fatalf("%s: caps = %x, %v; want %x", label, got, err, wantAll)
		}
	}
	check("live")

	srv.SetDown(true)
	check("cached while the node is down")
	srv.SetDown(false)

	_, err = client.MaxBatchWithdrawals(fx.DomainID, h, types.Hash{0x01}, []codec.AssetID{configured})
	if err == nil || errors.Is(err, recovery.ErrHalted) || client.Broken() != nil {
		t.Fatalf("another hash = %v, Broken() = %v; want a plain error", err, client.Broken())
	}
}

// TestFollowerEndToEnd drives a ledger.Follower through the worker channel against the recorded
// fixture: the follower reads the receiving momentum and fetches the momentum that holds the
// sends. A genesis boot against this fixture halts by design at PreStateRootSettlementHeight, so
// boot over the same path stays covered by TestReadPathEndToEnd.
func TestFollowerEndToEnd(t *testing.T) {
	fx := znndtest.Load(t)
	a := fx.Assignment
	srv := znndtest.NewServer(t, fx, a.ReceiveHeight+10)
	client := readPathClient(t, fx, srv, 10)
	c0 := fx.Expected.DomainInputCounter

	newFollower := func(startOrdinal uint64) *ledger.Follower {
		t.Helper()
		f, err := ledger.New(ledger.Config{
			DomainID: fx.DomainID, ConfirmationDepth: 10, StartHeight: a.ReceiveHeight,
			StartOrdinal: startOrdinal, Source: client, Assignments: client,
		})
		if err != nil {
			t.Fatalf("ledger.New: %v", err)
		}
		return f
	}

	f := newFollower(c0)
	var got []ledger.Input
	for {
		in, ok, err := f.Next()
		if err != nil {
			t.Fatalf("Next: %v", err)
		}
		if !ok {
			break
		}
		got = append(got, in)
	}

	if len(got) != len(a.Entries) {
		t.Fatalf("drained %d inputs, want %d", len(got), len(a.Entries))
	}
	sendMomentum := fx.DetailedMomentum(t, a.SendHeight)
	for i, e := range a.Entries {
		want, err := ledger.DecodeAssignedInput(fx.DomainID, sendMomentum, e.GlobalInputIndex, e.SourceAccountBlockHash)
		if err != nil {
			t.Fatalf("DecodeAssignedInput %d: %v", i, err)
		}
		if !reflect.DeepEqual(got[i], want) {
			t.Fatalf("input %d = %+v, want %+v", i, got[i], want)
		}
		if i > 0 && got[i].Ctx.GlobalInputIndex <= got[i-1].Ctx.GlobalInputIndex {
			t.Fatalf("input %d: global index %d does not exceed %d", i, got[i].Ctx.GlobalInputIndex, got[i-1].Ctx.GlobalInputIndex)
		}
		if got[i].Ctx.MomentumHeight != a.SendHeight {
			t.Fatalf("input %d: momentum height %d, want %d", i, got[i].Ctx.MomentumHeight, a.SendHeight)
		}
		decoded, err := codec.DecodeInput(got[i].Payload)
		if err != nil {
			t.Fatalf("DecodeInput %d: %v", i, err)
		}
		if decoded.SourceAccountBlockHash == a.RejectedSendHash {
			t.Fatalf("input %d is the rejected send", i)
		}
	}

	if _, ok, err := f.Next(); ok || err != nil {
		t.Fatalf("Next after drain = (ok=%v, err=%v), want (false, nil)", ok, err)
	}
	wantCP := ledger.Checkpoint{
		Height:      a.ReceiveHeight,
		Hash:        fx.DetailedMomentum(t, a.ReceiveHeight).Momentum.Hash,
		NextOrdinal: c0 + uint64(len(a.Entries)),
	}
	if cp, ok := f.Checkpoint(); !ok || cp != wantCP {
		t.Fatalf("Checkpoint = (%+v, %v), want %+v", cp, ok, wantCP)
	}

	// A follower whose next ordinal disagrees with Settlement's cursor halts, and the channel
	// stays usable.
	_, _, err := newFollower(c0 + 1).Next()
	if !errors.Is(err, recovery.ErrHalted) || !errors.Is(err, ledger.ErrSourceRead) || client.Broken() != nil {
		t.Fatalf("Next with a cursor ahead = %v, Broken() = %v; want ErrHalted inside ErrSourceRead on a live channel", err, client.Broken())
	}
}

func TestReadPathNodeDown(t *testing.T) {
	const depth = 10
	fx := znndtest.Load(t)
	srv := znndtest.NewServer(t, fx, fx.ReadHeight+depth)
	client := readPathClient(t, fx, srv, depth)
	srv.SetDown(true)

	if _, _, err := client.FinalizedAnchor(fx.DomainID); err == nil || errors.Is(err, recovery.ErrHalted) {
		t.Fatalf("FinalizedAnchor while down = %v, want a plain error", err)
	}
	if rdy, err := recovery.Boot(bootConfig(t, fx, depth, client)); !errors.Is(err, recovery.ErrSourceUnavailable) {
		if err == nil {
			rdy.Store.Close()
		}
		t.Fatalf("Boot while down = %v, want ErrSourceUnavailable", err)
	}
}

func TestReadPathTamperedRoot(t *testing.T) {
	const depth = 10
	fx := znndtest.Load(t)
	srv := znndtest.NewServer(t, fx, fx.ReadHeight+depth)
	client := readPathClient(t, fx, srv, depth)
	srv.TamperProof(func(p *znndtest.Proof) { p.Root[0] ^= 0x01 })

	if _, _, err := client.FinalizedAnchor(fx.DomainID); !errors.Is(err, recovery.ErrHalted) {
		t.Fatalf("FinalizedAnchor with a tampered root = %v, want ErrHalted", err)
	}
	if rdy, err := recovery.Boot(bootConfig(t, fx, depth, client)); !errors.Is(err, recovery.ErrHalted) {
		if err == nil {
			rdy.Store.Close()
		}
		t.Fatalf("Boot with a tampered root = %v, want ErrHalted", err)
	}
}
