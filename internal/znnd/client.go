// Package znnd is a JSON-RPC client for the ledger methods of a znnd node. Its errors are
// always plain: a node that answers wrongly may be recovering, so the caller retries, and only
// proof verification against a header the node supplied turns a fault into a halt.
package znnd

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/zenon-network/go-zenon/chain/nom"
	"github.com/zenon-network/go-zenon/common/types"
)

// requestTimeout bounds one RPC round trip, so a hung node surfaces as an error instead of
// blocking a worker request forever.
const requestTimeout = 30 * time.Second

// Client talks to one znnd node. It is safe for concurrent use.
type Client struct {
	endpoint string
	hc       *http.Client
	next     atomic.Uint64
}

// NewClient returns a client for the JSON-RPC HTTP endpoint.
func NewClient(endpoint string) *Client {
	return &Client{endpoint: endpoint, hc: &http.Client{Timeout: requestTimeout}}
}

// Proof is the ledger.getProof result. An empty Value is the node's claim that the key is
// absent.
type Proof struct {
	Value []byte     `json:"value"`
	Proof []byte     `json:"proof"`
	Root  types.Hash `json:"root"`
}

type rpcRequest struct {
	JSONRPC string `json:"jsonrpc"`
	ID      uint64 `json:"id"`
	Method  string `json:"method"`
	Params  []any  `json:"params"`
}

type rpcError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

type rpcResponse struct {
	ID     uint64          `json:"id"`
	Result json.RawMessage `json:"result"`
	Error  *rpcError       `json:"error"`
}

// call performs one request and returns the raw result, which is nil for a JSON null.
func (c *Client) call(method string, params ...any) (json.RawMessage, error) {
	id := c.next.Add(1)
	body, err := json.Marshal(rpcRequest{JSONRPC: "2.0", ID: id, Method: method, Params: params})
	if err != nil {
		return nil, fmt.Errorf("znnd: encode %s request: %w", method, err)
	}
	resp, err := c.hc.Post(c.endpoint, "application/json", bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("znnd: %s: %w", method, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("znnd: %s: HTTP status %d", method, resp.StatusCode)
	}
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("znnd: %s: read response: %w", method, err)
	}
	var r rpcResponse
	if err := json.Unmarshal(raw, &r); err != nil {
		return nil, fmt.Errorf("znnd: %s: decode response: %w", method, err)
	}
	if r.ID != id {
		return nil, fmt.Errorf("znnd: %s: response id %d does not match request id %d", method, r.ID, id)
	}
	if r.Error != nil {
		return nil, fmt.Errorf("znnd: %s: rpc error %d: %s", method, r.Error.Code, r.Error.Message)
	}
	if string(r.Result) == "null" {
		return nil, nil
	}
	return r.Result, nil
}

var errNullResult = errors.New("null result")

// callInto decodes the result of method into out and rejects a null result.
func (c *Client) callInto(out any, method string, params ...any) error {
	raw, err := c.call(method, params...)
	if err != nil {
		return err
	}
	if raw == nil {
		return fmt.Errorf("znnd: %s: %w", method, errNullResult)
	}
	if err := json.Unmarshal(raw, out); err != nil {
		return fmt.Errorf("znnd: %s: decode result: %w", method, err)
	}
	return nil
}

// FrontierMomentum returns the node's frontier momentum header (ledger.getFrontierMomentum).
func (c *Client) FrontierMomentum() (*nom.Momentum, error) {
	m := new(nom.Momentum)
	if err := c.callInto(m, "ledger.getFrontierMomentum"); err != nil {
		return nil, err
	}
	return m, nil
}

// FrontierHeight returns the height of the node's frontier momentum.
func (c *Client) FrontierHeight() (uint64, error) {
	m, err := c.FrontierMomentum()
	if err != nil {
		return 0, err
	}
	return m.Height, nil
}

// DetailedMomentumsByHeight returns up to count momenta with their account blocks, ascending
// from height (ledger.getDetailedMomentumsByHeight).
func (c *Client) DetailedMomentumsByHeight(height, count uint64) ([]*nom.DetailedMomentum, error) {
	// The RPC names the block list "blocks" and nom.DetailedMomentum names it "accountBlocks",
	// so the result decodes into a local shape. Fields beyond these two are ignored.
	var res struct {
		List []struct {
			Momentum *nom.Momentum       `json:"momentum"`
			Blocks   []*nom.AccountBlock `json:"blocks"`
		} `json:"list"`
	}
	if err := c.callInto(&res, "ledger.getDetailedMomentumsByHeight", height, count); err != nil {
		return nil, err
	}
	out := make([]*nom.DetailedMomentum, 0, len(res.List))
	for i, e := range res.List {
		if e.Momentum == nil {
			return nil, fmt.Errorf("znnd: ledger.getDetailedMomentumsByHeight: entry %d has no momentum", i)
		}
		out = append(out, &nom.DetailedMomentum{Momentum: e.Momentum, AccountBlocks: e.Blocks})
	}
	return out, nil
}

// Proof returns the state proof for key at height (ledger.getProof).
func (c *Client) Proof(height uint64, key []byte) (Proof, error) {
	var p Proof
	if err := c.callInto(&p, "ledger.getProof", height, key); err != nil {
		return Proof{}, err
	}
	return p, nil
}

// StateRoot returns the state root at height (ledger.getStateRoot).
func (c *Client) StateRoot(height uint64) (types.Hash, error) {
	var h types.Hash
	if err := c.callInto(&h, "ledger.getStateRoot", height); err != nil {
		return types.Hash{}, err
	}
	return h, nil
}

// FrontierAccountBlock returns the address's frontier account block
// (ledger.getFrontierAccountBlock), or nil, nil when the address has none.
func (c *Client) FrontierAccountBlock(addr types.Address) (*nom.AccountBlock, error) {
	raw, err := c.call("ledger.getFrontierAccountBlock", addr)
	if err != nil || raw == nil {
		return nil, err
	}
	b := new(nom.AccountBlock)
	if err := json.Unmarshal(raw, b); err != nil {
		return nil, fmt.Errorf("znnd: ledger.getFrontierAccountBlock: decode result: %w", err)
	}
	return b, nil
}
