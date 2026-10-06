package znnd

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"

	"github.com/zenon-network/go-zenon/common/types"

	"github.com/digitalSloth/go-executor/internal/recovery"
	"github.com/digitalSloth/go-executor/internal/znnd/znndtest"
)

func setup(t *testing.T) (*znndtest.Fixture, *znndtest.Server, *Client) {
	t.Helper()
	fx := znndtest.Load(t)
	srv := znndtest.NewServer(t, fx, fx.ReadHeight+10)
	return fx, srv, NewClient(srv.URL)
}

func mustPlain(t *testing.T, name string, err error) {
	t.Helper()
	if err == nil {
		t.Fatalf("%s: want an error, got nil", name)
	}
	if errors.Is(err, recovery.ErrHalted) {
		t.Fatalf("%s: error wraps ErrHalted: %v", name, err)
	}
}

func TestClientReads(t *testing.T) {
	fx, _, c := setup(t)

	h, err := c.FrontierHeight()
	if err != nil {
		t.Fatalf("FrontierHeight: %v", err)
	}
	if h != fx.ReadHeight+10 {
		t.Fatalf("FrontierHeight = %d, want %d", h, fx.ReadHeight+10)
	}

	first := fx.ReadHeight - 4
	ms, err := c.DetailedMomentumsByHeight(first, 5)
	if err != nil {
		t.Fatalf("DetailedMomentumsByHeight: %v", err)
	}
	if len(ms) != 5 {
		t.Fatalf("got %d momenta, want 5", len(ms))
	}
	for i, m := range ms {
		if m.Momentum.Height != first+uint64(i) {
			t.Fatalf("momentum %d has height %d, want %d", i, m.Momentum.Height, first+uint64(i))
		}
		if got := m.Momentum.ComputeHash(); got != m.Momentum.Hash {
			t.Fatalf("momentum %d hash %s does not recompute (%s)", m.Momentum.Height, m.Momentum.Hash, got)
		}
		for _, b := range m.AccountBlocks {
			if got := b.ComputeHash(); got != b.Hash {
				t.Fatalf("block %s in momentum %d does not recompute (%s)", b.Hash, m.Momentum.Height, got)
			}
		}
	}

	key := fx.Proofs[0].Key
	p, err := c.Proof(fx.ReadHeight, key)
	if err != nil {
		t.Fatalf("Proof: %v", err)
	}
	if want := fx.Proofs[0].Result; !reflect.DeepEqual(p, Proof(want)) {
		t.Fatalf("Proof = %+v, want %+v", p, want)
	}

	root, err := c.StateRoot(fx.ReadHeight)
	if err != nil {
		t.Fatalf("StateRoot: %v", err)
	}
	if root != fx.StateRoot {
		t.Fatalf("StateRoot = %s, want %s", root, fx.StateRoot)
	}

	b, err := c.FrontierAccountBlock(fx.ExecutorAddress)
	if err != nil {
		t.Fatalf("FrontierAccountBlock: %v", err)
	}
	if b == nil || b.Address != fx.ExecutorAddress {
		t.Fatalf("FrontierAccountBlock = %+v, want a block of %s", b, fx.ExecutorAddress)
	}
	if got := b.ComputeHash(); got != b.Hash {
		t.Fatalf("account block hash %s does not recompute (%s)", b.Hash, got)
	}

	none, err := c.FrontierAccountBlock(types.Address{0x09})
	if err != nil || none != nil {
		t.Fatalf("FrontierAccountBlock(unknown) = %v, %v; want nil, nil", none, err)
	}
}

func TestClientErrorsArePlain(t *testing.T) {
	fx, srv, c := setup(t)
	key := fx.Proofs[0].Key

	if _, err := c.Proof(fx.ReadHeight-1, key); err == nil {
		t.Fatalf("Proof at another height: want an error")
	} else {
		mustPlain(t, "Proof at another height", err)
	}

	srv.SetDown(true)
	_, err := c.FrontierHeight()
	mustPlain(t, "down FrontierHeight", err)
	_, err = c.Proof(fx.ReadHeight, key)
	mustPlain(t, "down Proof", err)
	srv.SetDown(false)
	if _, err := c.FrontierHeight(); err != nil {
		t.Fatalf("FrontierHeight after recovery: %v", err)
	}
}

func TestClientBespokeServers(t *testing.T) {
	serve := func(t *testing.T, body func(id uint64) string) *Client {
		t.Helper()
		s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			var req rpcRequest
			if err := jsonDecode(r, &req); err != nil {
				t.Errorf("decode request: %v", err)
			}
			fmt.Fprint(w, body(req.ID))
		}))
		t.Cleanup(s.Close)
		return NewClient(s.URL)
	}

	t.Run("different id", func(t *testing.T) {
		c := serve(t, func(id uint64) string { return fmt.Sprintf(`{"jsonrpc":"2.0","id":%d,"result":"0x"}`, id+1) })
		_, err := c.StateRoot(1)
		mustPlain(t, "id mismatch", err)
	})

	t.Run("null result", func(t *testing.T) {
		c := serve(t, func(id uint64) string { return fmt.Sprintf(`{"jsonrpc":"2.0","id":%d,"result":null}`, id) })
		_, err := c.Proof(1, []byte{1})
		mustPlain(t, "null Proof", err)
		_, err = c.StateRoot(1)
		mustPlain(t, "null StateRoot", err)
		_, err = c.DetailedMomentumsByHeight(1, 1)
		mustPlain(t, "null DetailedMomentumsByHeight", err)
		_, err = c.FrontierMomentum()
		mustPlain(t, "null FrontierMomentum", err)
		if b, err := c.FrontierAccountBlock(types.Address{}); b != nil || err != nil {
			t.Fatalf("null FrontierAccountBlock = %v, %v; want nil, nil", b, err)
		}
	})

	t.Run("rpc error", func(t *testing.T) {
		c := serve(t, func(id uint64) string {
			return fmt.Sprintf(`{"jsonrpc":"2.0","id":%d,"error":{"code":-32000,"message":"boom"}}`, id)
		})
		_, err := c.StateRoot(1)
		mustPlain(t, "rpc error", err)
		if got := err.Error(); !strings.Contains(got, "-32000") || !strings.Contains(got, "boom") {
			t.Fatalf("error %q lacks the code or message", got)
		}
	})

	t.Run("undecodable body", func(t *testing.T) {
		c := serve(t, func(uint64) string { return `not json` })
		_, err := c.StateRoot(1)
		mustPlain(t, "undecodable", err)
	})
}

func TestClientClosedServer(t *testing.T) {
	fx := znndtest.Load(t)
	s := httptest.NewServer(http.NotFoundHandler())
	c := NewClient(s.URL)
	s.Close()
	_, err := c.StateRoot(fx.ReadHeight)
	mustPlain(t, "closed server", err)
}

func jsonDecode(r *http.Request, v any) error { return json.NewDecoder(r.Body).Decode(v) }
