package supervisor

import (
	"errors"
	"testing"
	"time"

	"github.com/zenon-network/go-zenon/common/types"

	"github.com/digitalSloth/go-executor/domain"
	"github.com/digitalSloth/go-executor/internal/ipc"
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
	src := Sources{Momenta: zc, Anchors: settlement.NewAnchorSource(settlement.NewReader(zc), depth)}
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
