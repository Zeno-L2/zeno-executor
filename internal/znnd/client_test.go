package znnd

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"

	"github.com/zenon-network/go-zenon/chain/nom"
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

	t.Run("rpc error message is quoted", func(t *testing.T) {
		c := serve(t, func(id uint64) string {
			return fmt.Sprintf(`{"jsonrpc":"2.0","id":%d,"error":{"code":-32000,"message":"a\nb\u0007c\r\u001b[2Jd"}}`, id)
		})
		_, err := c.StateRoot(1)
		mustPlain(t, "rpc error", err)
		got := err.Error()
		if strings.ContainsAny(got, "\n\r\x07\x1b") {
			t.Fatalf("error %q holds a control character", got)
		}
		if !strings.Contains(got, `a\nb\ac\r\x1b[2Jd`) {
			t.Fatalf("error %q lacks the quoted message", got)
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

func TestConfirmationHeight(t *testing.T) {
	fx, _, c := setup(t)
	a := fx.Assignment

	hashes := []types.Hash{a.RejectedSendHash}
	for _, e := range a.Entries {
		hashes = append(hashes, e.SourceAccountBlockHash)
	}
	for _, hash := range hashes {
		h, ok, err := c.ConfirmationHeight(hash)
		if err != nil || !ok || h != a.SendHeight {
			t.Fatalf("ConfirmationHeight(%s) = %d, %v, %v; want %d, true, nil", hash, h, ok, err, a.SendHeight)
		}
	}

	h, ok, err := c.ConfirmationHeight(types.Hash{0x09})
	if err != nil || ok || h != 0 {
		t.Fatalf("ConfirmationHeight(unknown) = %d, %v, %v; want 0, false, nil", h, ok, err)
	}
}

func TestConfirmationHeightBespoke(t *testing.T) {
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
	want := types.Hash{0x01}
	other := types.Hash{0x02}

	t.Run("another hash", func(t *testing.T) {
		c := serve(t, func(id uint64) string {
			return fmt.Sprintf(`{"jsonrpc":"2.0","id":%d,"result":{"hash":%q,"confirmationDetail":{"momentumHeight":5}}}`, id, other.String())
		})
		_, _, err := c.ConfirmationHeight(want)
		mustPlain(t, "block with another hash", err)
	})

	t.Run("unconfirmed", func(t *testing.T) {
		c := serve(t, func(id uint64) string {
			return fmt.Sprintf(`{"jsonrpc":"2.0","id":%d,"result":{"hash":%q,"confirmationDetail":null}}`, id, want.String())
		})
		h, ok, err := c.ConfirmationHeight(want)
		if h != 0 || ok || err != nil {
			t.Fatalf("unconfirmed block = %d, %v, %v; want 0, false, nil", h, ok, err)
		}
	})
}

// sendBlock is the JSON the node returns for a send block, with the detail fields the caller adds.
func sendBlock(hash types.Hash, data []byte, extra string) string {
	b := &nom.AccountBlock{
		Hash:                 hash,
		Height:               7,
		Amount:               big.NewInt(0),
		Data:                 data,
		MomentumAcknowledged: types.HashHeight{Hash: types.Hash{0x0a}, Height: 33},
	}
	raw, err := json.Marshal(b)
	if err != nil {
		panic(err)
	}
	return string(raw[:len(raw)-1]) + extra + "}"
}

func TestSendStatusBespoke(t *testing.T) {
	serve := func(t *testing.T, result func() string) *Client {
		t.Helper()
		s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			var req rpcRequest
			if err := jsonDecode(r, &req); err != nil {
				t.Errorf("decode request: %v", err)
			}
			fmt.Fprintf(w, `{"jsonrpc":"2.0","id":%d,"result":%s}`, req.ID, result())
		}))
		t.Cleanup(s.Close)
		return NewClient(s.URL)
	}
	want := types.Hash{0x01}
	data := []byte{1, 2, 3}

	t.Run("null", func(t *testing.T) {
		c := serve(t, func() string { return "null" })
		got, err := c.SendStatus(want)
		if err != nil || got.Block != nil || got.Received || got.ReceiveHeight != 0 {
			t.Fatalf("SendStatus(null) = %+v, %v; want the zero status", got, err)
		}
	})

	t.Run("known without a paired block", func(t *testing.T) {
		c := serve(t, func() string {
			return sendBlock(want, data, `,"confirmationDetail":{"momentumHeight":40},"pairedAccountBlock":null`)
		})
		got, err := c.SendStatus(want)
		if err != nil || got.Block == nil || got.Received || got.ReceiveHeight != 0 {
			t.Fatalf("SendStatus = %+v, %v; want a block and no receive", got, err)
		}
		if got.Block.Hash != want || !bytes.Equal(got.Block.Data, data) || got.Block.MomentumAcknowledged.Height != 33 {
			t.Fatalf("Block = %+v; want hash, data and acknowledged momentum 33", got.Block)
		}
	})

	t.Run("paired block unconfirmed", func(t *testing.T) {
		c := serve(t, func() string {
			return sendBlock(want, data, `,"confirmationDetail":{"momentumHeight":40},"pairedAccountBlock":{"confirmationDetail":null}`)
		})
		got, err := c.SendStatus(want)
		if err != nil || got.Block == nil || got.Received {
			t.Fatalf("SendStatus = %+v, %v; want a block and no receive", got, err)
		}
	})

	t.Run("received", func(t *testing.T) {
		c := serve(t, func() string {
			return sendBlock(want, data, `,"confirmationDetail":{"momentumHeight":40},"pairedAccountBlock":{"confirmationDetail":{"momentumHeight":44}}`)
		})
		got, err := c.SendStatus(want)
		if err != nil || got.Block == nil || !got.Received || got.ReceiveHeight != 44 {
			t.Fatalf("SendStatus = %+v, %v; want a block received at 44", got, err)
		}
	})

	t.Run("null confirmationDetail", func(t *testing.T) {
		c := serve(t, func() string {
			return sendBlock(want, data, `,"confirmationDetail":null,"pairedAccountBlock":{"confirmationDetail":{"momentumHeight":44}}`)
		})
		got, err := c.SendStatus(want)
		if err != nil || got.Block != nil || got.Received {
			t.Fatalf("SendStatus = %+v, %v; want the zero status", got, err)
		}
	})

	t.Run("another hash", func(t *testing.T) {
		c := serve(t, func() string {
			return sendBlock(types.Hash{0x02}, data, `,"confirmationDetail":{"momentumHeight":40}`)
		})
		_, err := c.SendStatus(want)
		mustPlain(t, "block with another hash", err)
	})
}

func TestAccountBlockAtHeightBespoke(t *testing.T) {
	addr := types.Address{0x01}
	var params []json.RawMessage
	serve := func(t *testing.T, result string) *Client {
		t.Helper()
		s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			var req rpcRequest
			if err := jsonDecode(r, &req); err != nil {
				t.Errorf("decode request: %v", err)
			}
			if req.Method != "ledger.getAccountBlocksByHeight" {
				t.Errorf("method = %q, want ledger.getAccountBlocksByHeight", req.Method)
			}
			params = params[:0]
			for _, p := range req.Params {
				raw, _ := json.Marshal(p)
				params = append(params, raw)
			}
			fmt.Fprintf(w, `{"jsonrpc":"2.0","id":%d,"result":%s}`, req.ID, result)
		}))
		t.Cleanup(s.Close)
		return NewClient(s.URL)
	}
	block := func(addr types.Address, height uint64) string {
		raw, err := json.Marshal(&nom.AccountBlock{Address: addr, Height: height, Amount: big.NewInt(0)})
		if err != nil {
			panic(err)
		}
		return string(raw)
	}

	t.Run("request and one block", func(t *testing.T) {
		c := serve(t, fmt.Sprintf(`{"list":[%s],"count":9}`, block(addr, 7)))
		b, err := c.AccountBlockAtHeight(addr, 7)
		if err != nil || b == nil || b.Height != 7 || b.Address != addr {
			t.Fatalf("AccountBlockAtHeight = %+v, %v; want height 7", b, err)
		}
		want := []string{fmt.Sprintf("%q", addr.String()), "7", "1"}
		if len(params) != 3 {
			t.Fatalf("params = %s, want %v", params, want)
		}
		for i, w := range want {
			if string(params[i]) != w {
				t.Fatalf("param %d = %s, want %s", i, params[i], w)
			}
		}
	})

	t.Run("empty list", func(t *testing.T) {
		c := serve(t, `{"list":[],"count":0}`)
		b, err := c.AccountBlockAtHeight(addr, 7)
		if err != nil || b != nil {
			t.Fatalf("AccountBlockAtHeight(empty) = %+v, %v; want nil, nil", b, err)
		}
	})

	t.Run("null entry", func(t *testing.T) {
		c := serve(t, `{"list":[null]}`)
		_, err := c.AccountBlockAtHeight(addr, 7)
		mustPlain(t, "null entry", err)
	})

	t.Run("wrong height", func(t *testing.T) {
		c := serve(t, fmt.Sprintf(`{"list":[%s]}`, block(addr, 8)))
		_, err := c.AccountBlockAtHeight(addr, 7)
		mustPlain(t, "wrong height", err)
	})

	t.Run("wrong address", func(t *testing.T) {
		c := serve(t, fmt.Sprintf(`{"list":[%s]}`, block(types.Address{0x02}, 7)))
		_, err := c.AccountBlockAtHeight(addr, 7)
		mustPlain(t, "wrong address", err)
	})
}

func TestClientBodyBound(t *testing.T) {
	fx, srv, c := setup(t)

	srv.PadTo(maxResponseBody)
	root, err := c.StateRoot(fx.ReadHeight)
	if err != nil || root != fx.StateRoot {
		t.Fatalf("StateRoot at the bound = %s, %v; want %s, nil", root, err, fx.StateRoot)
	}

	srv.PadTo(maxResponseBody + 1)
	_, err = c.StateRoot(fx.ReadHeight)
	mustPlain(t, "StateRoot over the bound", err)
	if !errors.Is(err, ErrResponseTooLarge) {
		t.Fatalf("StateRoot over the bound = %v, want ErrResponseTooLarge", err)
	}

	srv.PadTo(0)
	if _, err := c.StateRoot(fx.ReadHeight); err != nil {
		t.Fatalf("StateRoot after padding off: %v", err)
	}
}

func TestClientBodyBoundPerMethod(t *testing.T) {
	fx, srv, c := setup(t)
	srv.PadTo(maxResponseBody + 1)

	calls := map[string]func() error{
		"FrontierMomentum": func() error { _, err := c.FrontierMomentum(); return err },
		"FrontierHeight":   func() error { _, err := c.FrontierHeight(); return err },
		"DetailedMomentumsByHeight": func() error {
			_, err := c.DetailedMomentumsByHeight(fx.ReadHeight, 1)
			return err
		},
		"Proof":     func() error { _, err := c.Proof(fx.ReadHeight, fx.Proofs[0].Key); return err },
		"StateRoot": func() error { _, err := c.StateRoot(fx.ReadHeight); return err },
		"FrontierAccountBlock": func() error {
			_, err := c.FrontierAccountBlock(fx.ExecutorAddress)
			return err
		},
		"ConfirmationHeight": func() error {
			_, _, err := c.ConfirmationHeight(fx.Assignment.Entries[0].SourceAccountBlockHash)
			return err
		},
		"SendStatus": func() error {
			_, err := c.SendStatus(fx.Assignment.Entries[0].SourceAccountBlockHash)
			return err
		},
		"AccountBlockAtHeight": func() error {
			_, err := c.AccountBlockAtHeight(fx.ExecutorAddress, 1)
			return err
		},
		"RequiredPlasma": func() error {
			_, err := c.RequiredPlasma(fx.ExecutorAddress, types.SettlementContract, nil)
			return err
		},
		"PublishRawTransaction": func() error {
			return c.PublishRawTransaction(&nom.AccountBlock{Amount: big.NewInt(0)})
		},
	}
	for name, call := range calls {
		t.Run(name, func(t *testing.T) {
			err := call()
			mustPlain(t, name, err)
			if !errors.Is(err, ErrResponseTooLarge) {
				t.Fatalf("%s = %v, want ErrResponseTooLarge", name, err)
			}
		})
	}
}

// countingBody yields an endless stream of spaces and counts the bytes read from it.
type countingBody struct{ n int }

func (b *countingBody) Read(p []byte) (int, error) {
	for i := range p {
		p[i] = ' '
	}
	b.n += len(p)
	return len(p), nil
}

func (b *countingBody) Close() error { return nil }

type bodyTransport struct{ body *countingBody }

func (t bodyTransport) RoundTrip(*http.Request) (*http.Response, error) {
	return &http.Response{StatusCode: http.StatusOK, Body: t.body, Header: http.Header{}}, nil
}

func TestClientBodyReadStopsAtBound(t *testing.T) {
	body := &countingBody{}
	c := NewClient("http://znnd.invalid")
	c.hc = &http.Client{Transport: bodyTransport{body}}

	_, err := c.FrontierHeight()
	if !errors.Is(err, ErrResponseTooLarge) {
		t.Fatalf("FrontierHeight = %v, want ErrResponseTooLarge", err)
	}
	if body.n != maxResponseBody+1 {
		t.Fatalf("read %d bytes, want exactly the bound plus one byte (%d)", body.n, maxResponseBody+1)
	}
}

func TestRequiredPlasma(t *testing.T) {
	from := types.Address{0x01}
	data := []byte{0xde, 0xad}

	t.Run("request and result", func(t *testing.T) {
		var gotMethod string
		var gotParams []map[string]any
		s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			var req struct {
				ID     uint64           `json:"id"`
				Method string           `json:"method"`
				Params []map[string]any `json:"params"`
			}
			if err := jsonDecode(r, &req); err != nil {
				t.Errorf("decode request: %v", err)
			}
			gotMethod, gotParams = req.Method, req.Params
			fmt.Fprintf(w, `{"jsonrpc":"2.0","id":%d,"result":{"availablePlasma":50000,"basePlasma":21000,"requiredDifficulty":0}}`, req.ID)
		}))
		t.Cleanup(s.Close)

		got, err := NewClient(s.URL).RequiredPlasma(from, types.SettlementContract, data)
		if err != nil {
			t.Fatalf("RequiredPlasma: %v", err)
		}
		if want := (PlasmaRequirement{AvailablePlasma: 50000, BasePlasma: 21000}); got != want {
			t.Fatalf("RequiredPlasma = %+v, want %+v", got, want)
		}
		if gotMethod != "embedded.plasma.getRequiredPoWForAccountBlock" {
			t.Fatalf("method = %q", gotMethod)
		}
		if len(gotParams) != 1 {
			t.Fatalf("params = %v, want one object", gotParams)
		}
		p := gotParams[0]
		if len(p) != 4 || p["address"] != from.String() || p["toAddress"] != types.SettlementContract.String() || p["blockType"] != float64(2) || p["data"] != "3q0=" {
			t.Fatalf("param = %v", p)
		}
	})

	t.Run("null result", func(t *testing.T) {
		s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			var req rpcRequest
			jsonDecode(r, &req)
			fmt.Fprintf(w, `{"jsonrpc":"2.0","id":%d,"result":null}`, req.ID)
		}))
		t.Cleanup(s.Close)
		_, err := NewClient(s.URL).RequiredPlasma(from, types.SettlementContract, data)
		mustPlain(t, "null result", err)
	})
}

func TestPublishRawTransaction(t *testing.T) {
	b := &nom.AccountBlock{
		Version:   1,
		BlockType: nom.BlockTypeUserSend,
		Height:    1,
		Address:   types.Address{0x01},
		ToAddress: types.SettlementContract,
		Amount:    big.NewInt(0),
		Data:      []byte{0x01},
	}
	b.Hash = b.ComputeHash()

	respond := func(t *testing.T, result string, got **nom.AccountBlock) *Client {
		t.Helper()
		s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			var req struct {
				ID     uint64              `json:"id"`
				Method string              `json:"method"`
				Params []*nom.AccountBlock `json:"params"`
			}
			if err := jsonDecode(r, &req); err != nil {
				t.Errorf("decode request: %v", err)
			}
			if req.Method != "ledger.publishRawTransaction" || len(req.Params) != 1 {
				t.Errorf("method %q with %d params", req.Method, len(req.Params))
			} else if got != nil {
				*got = req.Params[0]
			}
			fmt.Fprintf(w, `{"jsonrpc":"2.0","id":%d,%s}`, req.ID, result)
		}))
		t.Cleanup(s.Close)
		return NewClient(s.URL)
	}

	t.Run("null result", func(t *testing.T) {
		var got *nom.AccountBlock
		if err := respond(t, `"result":null`, &got).PublishRawTransaction(b); err != nil {
			t.Fatalf("PublishRawTransaction: %v", err)
		}
		if got == nil || got.ComputeHash() != b.Hash || got.Amount == nil || got.Amount.Sign() != 0 {
			t.Fatalf("server decoded %+v", got)
		}
	})

	t.Run("rpc error", func(t *testing.T) {
		err := respond(t, `"error":{"code":-32000,"message":"rejected"}`, nil).PublishRawTransaction(b)
		mustPlain(t, "rpc error", err)
	})
}
