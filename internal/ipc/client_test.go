package ipc

import (
	"errors"
	"net"
	"reflect"
	"testing"
	"time"

	"github.com/zenon-network/go-zenon/common/types"
	"github.com/zenon-network/go-zenon/vm/embedded/definition"

	"github.com/digitalSloth/go-executor/internal/codec"
	"github.com/digitalSloth/go-executor/internal/ipc/ipctest"
	"github.com/digitalSloth/go-executor/internal/ledger"
	"github.com/digitalSloth/go-executor/internal/recovery"
)

// recvFrom and sendFrom run on a fake-supervisor goroutine, so they report failures with Errorf,
// never Fatalf: only the test's own goroutine may call Fatal (go vet "tests").
func recvFrom(t *testing.T, conn net.Conn, side Side) Message {
	t.Helper()
	m, err := ReadMessage(conn, side)
	if err != nil {
		t.Errorf("fake: ReadMessage: %v", err)
		return nil
	}
	return m
}

func sendFrom(t *testing.T, conn net.Conn, m Message) {
	t.Helper()
	if err := WriteMessage(conn, m); err != nil {
		t.Errorf("fake: WriteMessage: %v", err)
	}
}

func TestClientReadConfig(t *testing.T) {
	a, b := net.Pipe()
	defer a.Close()
	defer b.Close()
	c := NewClient(a, nil)

	want := WorkerConfig{
		DomainID: 1, ExecutorAddress: types.Address{0x42}, STFSpecHash: types.Hash{0xAA},
		ConfirmationDepth: 2, Role: "EXECUTOR", NetworkName: "test", StoragePath: "/x",
	}
	go sendFrom(t, b, want)

	got, err := c.ReadConfig()
	if err != nil {
		t.Fatalf("ReadConfig: %v", err)
	}
	if got != want {
		t.Fatalf("ReadConfig = %+v, want %+v", got, want)
	}
	if c.Broken() != nil {
		t.Fatalf("Broken() = %v", c.Broken())
	}
}

func TestClientFrontier(t *testing.T) {
	a, b := net.Pipe()
	defer a.Close()
	defer b.Close()
	c := NewClient(a, nil)

	go func() {
		recvFrom(t, b, Worker)
		sendFrom(t, b, Frontier{Height: 42})
	}()

	h, err := c.FrontierHeight()
	if err != nil {
		t.Fatalf("FrontierHeight: %v", err)
	}
	if h != 42 {
		t.Fatalf("height = %d, want 42", h)
	}
}

func TestClientRangedRead(t *testing.T) {
	a, b := net.Pipe()
	defer a.Close()
	defer b.Close()
	c := NewClient(a, nil)

	chain := ipctest.Chain([][]ipctest.Block{
		{{Address: addr(1), Data: []byte("h1")}},
		{{Address: addr(1), Data: []byte("h2")}},
		{{Address: addr(1), Data: []byte("h3")}},
	})

	go func() {
		req, _ := recvFrom(t, b, Worker).(MomentaRequest)
		if req.Height != 1 || req.Count != 3 {
			t.Errorf("request = %+v", req)
		}
		for _, dm := range chain {
			sendFrom(t, b, Momentum{Detailed: dm})
		}
		sendFrom(t, b, RangeEnd{Count: 3})
	}()

	got, err := c.DetailedMomentumsByHeight(1, 3)
	if err != nil {
		t.Fatalf("DetailedMomentumsByHeight: %v", err)
	}
	if len(got) != 3 {
		t.Fatalf("got %d momenta, want 3", len(got))
	}
	for i, dm := range got {
		if dm.Momentum.Hash != chain[i].Momentum.Hash {
			t.Fatalf("momentum %d hash mismatch", i)
		}
	}
}

func TestClientAnchor(t *testing.T) {
	t.Run("found", func(t *testing.T) {
		a, b := net.Pipe()
		defer a.Close()
		defer b.Close()
		c := NewClient(a, nil)
		go sendFrom(t, b, WorkerConfig{DomainID: 1})
		if _, err := c.ReadConfig(); err != nil {
			t.Fatalf("ReadConfig: %v", err)
		}
		go func() {
			recvFrom(t, b, Worker)
			sendFrom(t, b, Anchor{Found: true, PostStateRoot: types.Hash{0x01}, LastInputSeq: 5})
		}()
		anchor, ok, err := c.FinalizedAnchor(1)
		if err != nil {
			t.Fatalf("FinalizedAnchor: %v", err)
		}
		if !ok || anchor.PostStateRoot != (types.Hash{0x01}) || anchor.LastInputSeq != 5 {
			t.Fatalf("anchor = %+v ok=%v", anchor, ok)
		}
	})

	t.Run("not found", func(t *testing.T) {
		a, b := net.Pipe()
		defer a.Close()
		defer b.Close()
		c := NewClient(a, nil)
		go sendFrom(t, b, WorkerConfig{DomainID: 1})
		if _, err := c.ReadConfig(); err != nil {
			t.Fatalf("ReadConfig: %v", err)
		}
		go func() {
			recvFrom(t, b, Worker)
			sendFrom(t, b, Anchor{Found: false})
		}()
		_, ok, err := c.FinalizedAnchor(1)
		if err != nil {
			t.Fatalf("FinalizedAnchor: %v", err)
		}
		if ok {
			t.Fatalf("ok = true, want false")
		}
	})
}

func TestClientAssignment(t *testing.T) {
	c, b := readyClient(t)
	defer b.Close()
	want := []ledger.AssignmentEntry{
		{Ordinal: 3, GlobalInputIndex: 10, SourceAccountBlockHash: types.Hash{0x01}, SendMomentumHeight: 7},
		{Ordinal: 4, GlobalInputIndex: 12, SourceAccountBlockHash: types.Hash{0x02}, SendMomentumHeight: 8},
	}
	got := make(chan Message, 1)
	go func() {
		got <- recvFrom(t, b, Worker)
		sendFrom(t, b, Assignment{Entries: want})
	}()
	entries, err := c.Assignment(1, 9, types.Hash{0x0A}, 3)
	if err != nil {
		t.Fatalf("Assignment: %v", err)
	}
	if req, ok := (<-got).(AssignmentRequest); !ok || req != (AssignmentRequest{Height: 9, Hash: types.Hash{0x0A}, NextOrdinal: 3}) {
		t.Fatalf("supervisor received %+v", req)
	}
	if len(entries) != 2 || entries[0] != want[0] || entries[1] != want[1] {
		t.Fatalf("entries = %+v, want %+v", entries, want)
	}

	if _, err := c.Assignment(2, 9, types.Hash{}, 0); err == nil || c.Broken() != nil {
		t.Fatalf("domain 2: err = %v, Broken() = %v; want a plain error", err, c.Broken())
	}
	if _, err := c.Assignment(1, 0, types.Hash{}, 0); err == nil || c.Broken() != nil {
		t.Fatalf("height 0: err = %v, Broken() = %v; want a plain error", err, c.Broken())
	}

	for _, kind := range []SourceErrorKind{SourceHalted, SourceUnavailable} {
		go func() {
			recvFrom(t, b, Worker)
			sendFrom(t, b, SourceError{Kind: kind, Message: "x"})
		}()
		_, err := c.Assignment(1, 9, types.Hash{}, 0)
		if halted := errors.Is(err, recovery.ErrHalted); err == nil || halted != (kind == SourceHalted) || errors.Is(err, ErrChannel) {
			t.Fatalf("kind %d: err = %v", kind, err)
		}
		if c.Broken() != nil {
			t.Fatalf("Broken() = %v, want nil", c.Broken())
		}
	}
}

func TestClientCap(t *testing.T) {
	c, b := readyClient(t)
	defer b.Close()
	zrc, qsr := codec.AssetIDFromZTS(types.ZnnTokenStandard), codec.AssetIDFromZTS(types.QsrTokenStandard)
	assets := []codec.AssetID{zrc, qsr}
	got := make(chan Message, 1)
	go func() {
		got <- recvFrom(t, b, Worker)
		sendFrom(t, b, Cap{Amounts: []codec.U256{{31: 9}, {31: 10}}})
	}()
	amounts, err := c.MaxBatchWithdrawals(1, 9, types.Hash{0x0A}, assets)
	if want := []codec.U256{{31: 9}, {31: 10}}; err != nil || !reflect.DeepEqual(amounts, want) {
		t.Fatalf("MaxBatchWithdrawals = %x, %v; want %x", amounts, err, want)
	}
	if req, ok := (<-got).(CapRequest); !ok || !reflect.DeepEqual(req, CapRequest{Height: 9, Hash: types.Hash{0x0A}, Assets: assets}) {
		t.Fatalf("supervisor received %+v", req)
	}

	plain := map[string]func() error{
		"domain 2":  func() error { _, err := c.MaxBatchWithdrawals(2, 9, types.Hash{}, assets); return err },
		"height 0":  func() error { _, err := c.MaxBatchWithdrawals(1, 0, types.Hash{}, assets); return err },
		"no assets": func() error { _, err := c.MaxBatchWithdrawals(1, 9, types.Hash{}, nil); return err },
		"17 assets": func() error {
			_, err := c.MaxBatchWithdrawals(1, 9, types.Hash{}, make([]codec.AssetID, codec.MaxAssetsPerBatch+1))
			return err
		},
	}
	for name, call := range plain {
		if err := call(); err == nil || c.Broken() != nil {
			t.Fatalf("%s: err = %v, Broken() = %v; want a plain error", name, err, c.Broken())
		}
	}

	for _, kind := range []SourceErrorKind{SourceHalted, SourceUnavailable} {
		go func() {
			recvFrom(t, b, Worker)
			sendFrom(t, b, SourceError{Kind: kind, Message: "x"})
		}()
		_, err := c.MaxBatchWithdrawals(1, 9, types.Hash{}, []codec.AssetID{zrc})
		if halted := errors.Is(err, recovery.ErrHalted); err == nil || halted != (kind == SourceHalted) || errors.Is(err, ErrChannel) {
			t.Fatalf("kind %d: err = %v", kind, err)
		}
		if c.Broken() != nil {
			t.Fatalf("Broken() = %v, want nil", c.Broken())
		}
	}
}

func TestClientAssignmentFirstOrdinalPassesThrough(t *testing.T) {
	c, b := readyClient(t)
	defer b.Close()
	want := []ledger.AssignmentEntry{{Ordinal: 4, SendMomentumHeight: 1}, {Ordinal: 5, SendMomentumHeight: 1}}
	go func() {
		recvFrom(t, b, Worker)
		sendFrom(t, b, Assignment{Entries: want})
	}()
	entries, err := c.Assignment(1, 9, types.Hash{}, 3)
	if err != nil || len(entries) != 2 || entries[0] != want[0] || entries[1] != want[1] {
		t.Fatalf("entries = %+v, err = %v; want %+v", entries, err, want)
	}
	if c.Broken() != nil {
		t.Fatalf("Broken() = %v, want nil", c.Broken())
	}
}

func readyClient(t *testing.T) (*Client, net.Conn) {
	t.Helper()
	a, b := net.Pipe()
	c := NewClient(a, nil)
	go sendFrom(t, b, WorkerConfig{DomainID: 1})
	if _, err := c.ReadConfig(); err != nil {
		t.Fatalf("ReadConfig: %v", err)
	}
	return c, b
}

func TestSourceErrorMapping(t *testing.T) {
	t.Run("halted", func(t *testing.T) {
		c, b := readyClient(t)
		defer b.Close()
		go func() {
			recvFrom(t, b, Worker)
			sendFrom(t, b, SourceError{Kind: SourceHalted, Message: "down"})
		}()
		_, err := c.FrontierHeight()
		if !errors.Is(err, recovery.ErrHalted) {
			t.Fatalf("err = %v, want ErrHalted", err)
		}
		if c.Broken() != nil {
			t.Fatalf("Broken() = %v, want nil", c.Broken())
		}
	})

	t.Run("unavailable", func(t *testing.T) {
		c, b := readyClient(t)
		defer b.Close()
		go func() {
			recvFrom(t, b, Worker)
			sendFrom(t, b, SourceError{Kind: SourceUnavailable, Message: "down"})
		}()
		_, err := c.FrontierHeight()
		if errors.Is(err, recovery.ErrHalted) || errors.Is(err, ErrChannel) {
			t.Fatalf("err = %v, want neither ErrHalted nor ErrChannel", err)
		}
		if c.Broken() != nil {
			t.Fatalf("Broken() = %v, want nil", c.Broken())
		}
	})
}

func TestFinalizedAnchorWrongDomain(t *testing.T) {
	c, b := readyClient(t)
	defer b.Close()
	_, _, err := c.FinalizedAnchor(2)
	if err == nil {
		t.Fatalf("want an error")
	}
	if c.Broken() != nil {
		t.Fatalf("Broken() = %v, want nil", c.Broken())
	}
}

// breakCase runs one channel-failure scenario against a ready client and asserts it breaks.
type breakCase struct {
	name string
	do   func(t *testing.T, c *Client, b net.Conn)
}

func TestChannelFailures(t *testing.T) {
	cases := []breakCase{
		{"range end count mismatch", func(t *testing.T, c *Client, b net.Conn) {
			go func() {
				recvFrom(t, b, Worker)
				chain := ipctest.Chain([][]ipctest.Block{{{Address: addr(1), Data: []byte("x")}}})
				sendFrom(t, b, Momentum{Detailed: chain[0]})
				sendFrom(t, b, RangeEnd{Count: 2})
			}()
			if _, err := c.DetailedMomentumsByHeight(1, 3); !errors.Is(err, ErrChannel) {
				t.Fatalf("err = %v, want ErrChannel", err)
			}
		}},
		{"more momentum than requested", func(t *testing.T, c *Client, b net.Conn) {
			go func() {
				recvFrom(t, b, Worker)
				chain := ipctest.Chain([][]ipctest.Block{
					{{Address: addr(1), Data: []byte("a")}},
					{{Address: addr(1), Data: []byte("b")}},
				})
				sendFrom(t, b, Momentum{Detailed: chain[0]})
				sendFrom(t, b, Momentum{Detailed: chain[1]})
			}()
			if _, err := c.DetailedMomentumsByHeight(1, 1); !errors.Is(err, ErrChannel) {
				t.Fatalf("err = %v, want ErrChannel", err)
			}
		}},
		{"source error after a momentum", func(t *testing.T, c *Client, b net.Conn) {
			go func() {
				recvFrom(t, b, Worker)
				chain := ipctest.Chain([][]ipctest.Block{{{Address: addr(1), Data: []byte("x")}}})
				sendFrom(t, b, Momentum{Detailed: chain[0]})
				sendFrom(t, b, SourceError{Kind: SourceUnavailable, Message: "x"})
			}()
			if _, err := c.DetailedMomentumsByHeight(1, 3); !errors.Is(err, ErrChannel) {
				t.Fatalf("err = %v, want ErrChannel", err)
			}
		}},
		{"unexpected type", func(t *testing.T, c *Client, b net.Conn) {
			go func() {
				recvFrom(t, b, Worker)
				sendFrom(t, b, Frontier{Height: 1})
			}()
			if _, _, err := c.FinalizedAnchor(1); !errors.Is(err, ErrChannel) {
				t.Fatalf("err = %v, want ErrChannel", err)
			}
		}},
		{"assignment ordinal gap", func(t *testing.T, c *Client, b net.Conn) {
			go func() {
				recvFrom(t, b, Worker)
				sendFrom(t, b, Assignment{Entries: []ledger.AssignmentEntry{
					{Ordinal: 3, SendMomentumHeight: 1},
					{Ordinal: 5, SendMomentumHeight: 1},
				}})
			}()
			if _, err := c.Assignment(1, 9, types.Hash{}, 3); !errors.Is(err, ErrChannel) {
				t.Fatalf("err = %v, want ErrChannel", err)
			}
		}},
		{"assignment answered by a frontier", func(t *testing.T, c *Client, b net.Conn) {
			go func() {
				recvFrom(t, b, Worker)
				sendFrom(t, b, Frontier{Height: 1})
			}()
			if _, err := c.Assignment(1, 9, types.Hash{}, 3); !errors.Is(err, ErrChannel) {
				t.Fatalf("err = %v, want ErrChannel", err)
			}
		}},
		{"cap answered by a frontier", func(t *testing.T, c *Client, b net.Conn) {
			go func() {
				recvFrom(t, b, Worker)
				sendFrom(t, b, Frontier{Height: 1})
			}()
			if _, err := c.MaxBatchWithdrawals(1, 9, types.Hash{}, []codec.AssetID{{}}); !errors.Is(err, ErrChannel) {
				t.Fatalf("err = %v, want ErrChannel", err)
			}
		}},
		{"cap with the wrong amount count", func(t *testing.T, c *Client, b net.Conn) {
			go func() {
				recvFrom(t, b, Worker)
				sendFrom(t, b, Cap{Amounts: []codec.U256{{31: 1}, {31: 2}}})
			}()
			if _, err := c.MaxBatchWithdrawals(1, 9, types.Hash{}, []codec.AssetID{{}}); !errors.Is(err, ErrChannel) {
				t.Fatalf("err = %v, want ErrChannel", err)
			}
		}},
		{"momentum with a bad hash", func(t *testing.T, c *Client, b net.Conn) {
			go func() {
				recvFrom(t, b, Worker)
				chain := ipctest.Chain([][]ipctest.Block{{{Address: addr(1), Data: []byte("x")}}})
				chain[0].Momentum.Hash[0] ^= 0xff
				if err := WriteMessage(b, Momentum{Detailed: chain[0]}); err != nil {
					t.Errorf("WriteMessage: %v", err)
				}
			}()
			if _, err := c.DetailedMomentumsByHeight(1, 3); !errors.Is(err, ErrChannel) {
				t.Fatalf("err = %v, want ErrChannel", err)
			}
		}},
		{"eof", func(t *testing.T, c *Client, b net.Conn) {
			b.Close()
			if _, err := c.FrontierHeight(); !errors.Is(err, ErrChannel) {
				t.Fatalf("err = %v, want ErrChannel", err)
			}
		}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			a, b := net.Pipe()
			defer a.Close()
			defer b.Close()
			var broken int
			c := NewClient(a, func() { broken++ })
			go sendFrom(t, b, WorkerConfig{DomainID: 1})
			if _, err := c.ReadConfig(); err != nil {
				t.Fatalf("ReadConfig: %v", err)
			}

			tc.do(t, c, b)

			if c.Broken() == nil {
				t.Fatalf("Broken() = nil, want set")
			}
			if broken != 1 {
				t.Fatalf("onBroken called %d times, want 1", broken)
			}

			// A further call on each method returns ErrChannel without blocking; the fake end
			// is still open but not reading, so a write would block if it happened.
			done := make(chan struct{})
			go func() {
				defer close(done)
				if _, err := c.FrontierHeight(); !errors.Is(err, ErrChannel) {
					t.Errorf("FrontierHeight after break: err = %v", err)
				}
				if _, err := c.DetailedMomentumsByHeight(1, 1); !errors.Is(err, ErrChannel) {
					t.Errorf("DetailedMomentumsByHeight after break: err = %v", err)
				}
				if _, _, err := c.FinalizedAnchor(1); !errors.Is(err, ErrChannel) {
					t.Errorf("FinalizedAnchor after break: err = %v", err)
				}
				if err := c.ReportCommitted([]codec.BatchCommitment{{}}); !errors.Is(err, ErrChannel) {
					t.Errorf("ReportCommitted after break: err = %v", err)
				}
				if err := c.ReportRecovered(3, nil); !errors.Is(err, ErrChannel) {
					t.Errorf("ReportRecovered after break: err = %v", err)
				}
				if _, err := c.ReadConfig(); !errors.Is(err, ErrChannel) {
					t.Errorf("ReadConfig after break: err = %v", err)
				}
			}()
			select {
			case <-done:
			case <-time.After(2 * time.Second):
				t.Fatalf("a call after break blocked")
			}
			if broken != 1 {
				t.Fatalf("onBroken called %d times after further calls, want 1", broken)
			}
		})
	}
}

func TestReportCommitted(t *testing.T) {
	a, b := net.Pipe()
	defer a.Close()
	defer b.Close()
	c := NewClient(a, nil)

	c1 := validCommitment()
	c1.BatchID = 1
	c2 := validCommitment()
	c2.BatchID = 2

	got := make(chan []Committed, 1)
	go func() {
		var recvd []Committed
		for i := 0; i < 2; i++ {
			m, _ := recvFrom(t, b, Worker).(Committed)
			recvd = append(recvd, m)
		}
		got <- recvd
	}()

	if err := c.ReportCommitted([]codec.BatchCommitment{c1, c2}); err != nil {
		t.Fatalf("ReportCommitted: %v", err)
	}
	recvd := <-got
	if recvd[0].Commitment.BatchID != 1 || recvd[1].Commitment.BatchID != 2 {
		t.Fatalf("received = %+v", recvd)
	}
}

func TestReportRecovered(t *testing.T) {
	c1 := validCommitment()
	c1.BatchID = 4
	c2 := validCommitment()
	c2.BatchID = 5

	cases := []struct {
		name string
		cs   []codec.BatchCommitment
	}{
		{"commitments", []codec.BatchCommitment{c1, c2}},
		{"empty", nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			a, b := net.Pipe()
			defer a.Close()
			defer b.Close()
			c := NewClient(a, nil)

			got := make(chan []Message, 1)
			go func() {
				var recvd []Message
				for i := 0; i < 1+len(tc.cs); i++ {
					recvd = append(recvd, recvFrom(t, b, Worker))
				}
				got <- recvd
			}()

			if err := c.ReportRecovered(4, tc.cs); err != nil {
				t.Fatalf("ReportRecovered: %v", err)
			}
			// Closing the writer ends the reader at EOF if ReportRecovered wrote fewer frames
			// than expected, so a missing frame fails instead of blocking.
			a.Close()
			recvd := <-got
			if r, ok := recvd[0].(Retract); !ok || r.FromBatchID != 4 {
				t.Fatalf("first frame = %#v, want Retract{4}", recvd[0])
			}
			for i, cm := range tc.cs {
				r, ok := recvd[i+1].(Recovered)
				if !ok || r.Commitment.BatchID != cm.BatchID {
					t.Fatalf("frame %d = %#v, want Recovered batch %d", i+1, recvd[i+1], cm.BatchID)
				}
			}
		})
	}
}

func TestClientNextBatchID(t *testing.T) {
	t.Run("reply", func(t *testing.T) {
		c, b := readyClient(t)
		defer b.Close()
		go func() {
			if m := recvFrom(t, b, Worker); m != (CursorRequest{}) {
				t.Errorf("request = %#v, want CursorRequest", m)
			}
			sendFrom(t, b, Cursor{NextBatchID: 9})
		}()
		n, err := c.NextBatchID(1)
		if err != nil || n != 9 {
			t.Fatalf("NextBatchID = %d, %v, want 9, nil", n, err)
		}
	})

	for _, kind := range []struct {
		name   string
		kind   SourceErrorKind
		halted bool
	}{{"halted", SourceHalted, true}, {"unavailable", SourceUnavailable, false}} {
		t.Run(kind.name, func(t *testing.T) {
			c, b := readyClient(t)
			defer b.Close()
			go func() {
				recvFrom(t, b, Worker)
				sendFrom(t, b, SourceError{Kind: kind.kind, Message: "x"})
			}()
			_, err := c.NextBatchID(1)
			if errors.Is(err, recovery.ErrHalted) != kind.halted || errors.Is(err, ErrChannel) {
				t.Fatalf("err = %v, want halted=%v and not a channel failure", err, kind.halted)
			}
			if c.Broken() != nil {
				t.Fatalf("Broken() = %v, want nil", c.Broken())
			}
		})
	}

	t.Run("wrong domain", func(t *testing.T) {
		c, b := readyClient(t)
		defer b.Close()
		if _, err := c.NextBatchID(2); err == nil {
			t.Fatalf("want an error")
		}
		if c.Broken() != nil {
			t.Fatalf("Broken() = %v, want nil", c.Broken())
		}
	})

	t.Run("unexpected reply", func(t *testing.T) {
		c, b := readyClient(t)
		defer b.Close()
		go func() {
			recvFrom(t, b, Worker)
			sendFrom(t, b, Frontier{Height: 1})
		}()
		if _, err := c.NextBatchID(1); !errors.Is(err, ErrChannel) {
			t.Fatalf("err = %v, want ErrChannel", err)
		}
	})
}

func TestClientAcceptedEntries(t *testing.T) {
	t.Run("reply", func(t *testing.T) {
		c, b := readyClient(t)
		defer b.Close()
		go func() {
			if m := recvFrom(t, b, Worker); m != (AcceptedRequest{FromBatchID: 5}) {
				t.Errorf("request = %#v, want AcceptedRequest{5}", m)
			}
			sendFrom(t, b, Accepted{NextBatchID: 8, Entries: []*definition.BatchEntry{acceptedEntry(5, 0), acceptedEntry(6, 3), acceptedEntry(7, 0)}})
		}()
		next, entries, err := c.AcceptedEntries(1, 5)
		if err != nil || next != 8 || len(entries) != 3 || entries[2].BatchId != 7 {
			t.Fatalf("AcceptedEntries = %d, %d entries, %v, want 8, 3 entries ending at batch 7, nil", next, len(entries), err)
		}
	})

	t.Run("empty at nextBatchId", func(t *testing.T) {
		c, b := readyClient(t)
		defer b.Close()
		go func() {
			recvFrom(t, b, Worker)
			sendFrom(t, b, Accepted{NextBatchID: 5})
		}()
		next, entries, err := c.AcceptedEntries(1, 5)
		if err != nil || next != 5 || len(entries) != 0 {
			t.Fatalf("AcceptedEntries = %d, %d entries, %v, want 5, none, nil", next, len(entries), err)
		}
	})

	for _, tc := range []struct {
		name  string
		reply Accepted
	}{
		{"gap in batchIds", Accepted{NextBatchID: 9, Entries: []*definition.BatchEntry{acceptedEntry(5, 0), acceptedEntry(7, 0)}}},
		{"first batchId not from", Accepted{NextBatchID: 9, Entries: []*definition.BatchEntry{acceptedEntry(6, 0)}}},
		{"batchId at nextBatchId", Accepted{NextBatchID: 6, Entries: []*definition.BatchEntry{acceptedEntry(5, 0), acceptedEntry(6, 0)}}},
		{"empty below nextBatchId", Accepted{NextBatchID: 9}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c, b := readyClient(t)
			defer b.Close()
			go func() {
				recvFrom(t, b, Worker)
				sendFrom(t, b, tc.reply)
			}()
			if _, _, err := c.AcceptedEntries(1, 5); !errors.Is(err, ErrChannel) {
				t.Fatalf("err = %v, want ErrChannel", err)
			}
			if c.Broken() == nil {
				t.Fatalf("Broken() = nil, want the channel broken")
			}
		})
	}

	for _, kind := range []struct {
		name   string
		kind   SourceErrorKind
		halted bool
	}{{"halted", SourceHalted, true}, {"unavailable", SourceUnavailable, false}} {
		t.Run(kind.name, func(t *testing.T) {
			c, b := readyClient(t)
			defer b.Close()
			go func() {
				recvFrom(t, b, Worker)
				sendFrom(t, b, SourceError{Kind: kind.kind, Message: "x"})
			}()
			_, _, err := c.AcceptedEntries(1, 0)
			if errors.Is(err, recovery.ErrHalted) != kind.halted || errors.Is(err, ErrChannel) {
				t.Fatalf("err = %v, want halted=%v and not a channel failure", err, kind.halted)
			}
			if c.Broken() != nil {
				t.Fatalf("Broken() = %v, want nil", c.Broken())
			}
		})
	}

	t.Run("wrong domain", func(t *testing.T) {
		c, b := readyClient(t)
		defer b.Close()
		if _, _, err := c.AcceptedEntries(2, 0); err == nil {
			t.Fatalf("want an error")
		}
		if c.Broken() != nil {
			t.Fatalf("Broken() = %v, want nil", c.Broken())
		}
	})

	t.Run("unexpected reply", func(t *testing.T) {
		c, b := readyClient(t)
		defer b.Close()
		go func() {
			recvFrom(t, b, Worker)
			sendFrom(t, b, Frontier{Height: 1})
		}()
		if _, _, err := c.AcceptedEntries(1, 0); !errors.Is(err, ErrChannel) {
			t.Fatalf("err = %v, want ErrChannel", err)
		}
	})
}
