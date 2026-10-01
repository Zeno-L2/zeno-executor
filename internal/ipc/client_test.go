package ipc

import (
	"errors"
	"net"
	"testing"
	"time"

	"github.com/zenon-network/go-zenon/common/types"

	"github.com/digitalSloth/go-executor/internal/codec"
	"github.com/digitalSloth/go-executor/internal/ipc/ipctest"
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
