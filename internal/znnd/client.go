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

// maxResponseBody bounds one JSON-RPC response body, so a node cannot make the client buffer an
// unbounded response.
const maxResponseBody = 32 << 20

// ErrResponseTooLarge reports a response body over 32 MiB. Like every client error it is plain:
// the caller retries, or asks for fewer momenta.
var ErrResponseTooLarge = errors.New("znnd: response body exceeds 32 MiB")

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
	raw, err := io.ReadAll(io.LimitReader(resp.Body, maxResponseBody+1))
	if err != nil {
		return nil, fmt.Errorf("znnd: %s: read response: %w", method, err)
	}
	if len(raw) > maxResponseBody {
		return nil, fmt.Errorf("znnd: %s: %w", method, ErrResponseTooLarge)
	}
	var r rpcResponse
	if err := json.Unmarshal(raw, &r); err != nil {
		return nil, fmt.Errorf("znnd: %s: decode response: %w", method, err)
	}
	if r.ID != id {
		return nil, fmt.Errorf("znnd: %s: response id %d does not match request id %d", method, r.ID, id)
	}
	if r.Error != nil {
		return nil, fmt.Errorf("znnd: %s: rpc error %d: %q", method, r.Error.Code, r.Error.Message)
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

// ConfirmationHeight returns the height of the momentum that confirms the account block
// hashed hash (ledger.getAccountBlockByHash). ok is false when the node has no such block or
// reports it unconfirmed.
func (c *Client) ConfirmationHeight(hash types.Hash) (height uint64, ok bool, err error) {
	raw, err := c.call("ledger.getAccountBlockByHash", hash)
	if err != nil || raw == nil {
		return 0, false, err
	}
	var b struct {
		Hash               types.Hash `json:"hash"`
		ConfirmationDetail *struct {
			MomentumHeight uint64 `json:"momentumHeight"`
		} `json:"confirmationDetail"`
	}
	if err := json.Unmarshal(raw, &b); err != nil {
		return 0, false, fmt.Errorf("znnd: ledger.getAccountBlockByHash: decode result: %w", err)
	}
	if b.Hash != hash {
		return 0, false, fmt.Errorf("znnd: ledger.getAccountBlockByHash: node returned block %s for %s", b.Hash, hash)
	}
	if b.ConfirmationDetail == nil {
		return 0, false, nil
	}
	return b.ConfirmationDetail.MomentumHeight, true, nil
}

// SendStatus is what the node's momentum store holds for a send block.
type SendStatus struct {
	Block         *nom.AccountBlock // the send as a momentum holds it; nil when no momentum does
	Received      bool              // its paired receive is in a momentum
	ReceiveHeight uint64            // that momentum's height; 0 unless Received
}

// SendStatus reads ledger.getAccountBlockByHash for hash and its pairedAccountBlock. The zero
// SendStatus means the node has no such block.
func (c *Client) SendStatus(hash types.Hash) (SendStatus, error) {
	raw, err := c.call("ledger.getAccountBlockByHash", hash)
	if err != nil || raw == nil {
		return SendStatus{}, err
	}
	// nom.AccountBlock has a custom UnmarshalJSON, so embedding it in a struct that adds the
	// RPC's detail fields would decode nothing but the block. The result is decoded twice instead.
	b := new(nom.AccountBlock)
	if err := json.Unmarshal(raw, b); err != nil {
		return SendStatus{}, fmt.Errorf("znnd: ledger.getAccountBlockByHash: decode result: %w", err)
	}
	var d struct {
		Hash               types.Hash `json:"hash"`
		ConfirmationDetail *struct {
			MomentumHeight uint64 `json:"momentumHeight"`
		} `json:"confirmationDetail"`
		PairedAccountBlock *struct {
			ConfirmationDetail *struct {
				MomentumHeight uint64 `json:"momentumHeight"`
			} `json:"confirmationDetail"`
		} `json:"pairedAccountBlock"`
	}
	if err := json.Unmarshal(raw, &d); err != nil {
		return SendStatus{}, fmt.Errorf("znnd: ledger.getAccountBlockByHash: decode result: %w", err)
	}
	if d.Hash != hash {
		return SendStatus{}, fmt.Errorf("znnd: ledger.getAccountBlockByHash: node returned block %s for %s", d.Hash, hash)
	}
	if d.ConfirmationDetail == nil {
		return SendStatus{}, nil
	}
	s := SendStatus{Block: b}
	if p := d.PairedAccountBlock; p != nil && p.ConfirmationDetail != nil {
		s.Received = true
		s.ReceiveHeight = p.ConfirmationDetail.MomentumHeight
	}
	return s, nil
}

// AccountBlockAtHeight returns addr's block at height from the node's account chain, unconfirmed
// blocks included (ledger.getAccountBlocksByHeight with count 1), or nil, nil when the node has
// none.
func (c *Client) AccountBlockAtHeight(addr types.Address, height uint64) (*nom.AccountBlock, error) {
	var res struct {
		List []*nom.AccountBlock `json:"list"`
	}
	raw, err := c.call("ledger.getAccountBlocksByHeight", addr, height, 1)
	if err != nil || raw == nil {
		return nil, err
	}
	if err := json.Unmarshal(raw, &res); err != nil {
		return nil, fmt.Errorf("znnd: ledger.getAccountBlocksByHeight: decode result: %w", err)
	}
	if len(res.List) == 0 {
		return nil, nil
	}
	b := res.List[0]
	if b == nil {
		return nil, fmt.Errorf("znnd: ledger.getAccountBlocksByHeight: node returned a null block for %s at height %d", addr, height)
	}
	if b.Height != height || b.Address != addr {
		return nil, fmt.Errorf("znnd: ledger.getAccountBlocksByHeight: node returned height %d of %s for height %d of %s", b.Height, b.Address, height, addr)
	}
	return b, nil
}

// PlasmaRequirement is the part of the embedded.plasma.getRequiredPoWForAccountBlock result
// a fused-plasma send needs.
type PlasmaRequirement struct {
	AvailablePlasma uint64 `json:"availablePlasma"`
	BasePlasma      uint64 `json:"basePlasma"`
}

// RequiredPlasma returns the base plasma a user send from from to to carrying data needs, and
// the plasma from has available (embedded.plasma.getRequiredPoWForAccountBlock).
func (c *Client) RequiredPlasma(from, to types.Address, data []byte) (PlasmaRequirement, error) {
	param := struct {
		Address   types.Address `json:"address"`
		BlockType uint64        `json:"blockType"`
		ToAddress types.Address `json:"toAddress"`
		Data      []byte        `json:"data"`
	}{from, nom.BlockTypeUserSend, to, data}
	var r PlasmaRequirement
	if err := c.callInto(&r, "embedded.plasma.getRequiredPoWForAccountBlock", param); err != nil {
		return PlasmaRequirement{}, err
	}
	return r, nil
}

// PublishRawTransaction hands a signed account block to the node
// (ledger.publishRawTransaction). A nil error means the node inserted it into its own account
// pool; the block is not yet confirmed.
func (c *Client) PublishRawTransaction(b *nom.AccountBlock) error {
	_, err := c.call("ledger.publishRawTransaction", b)
	return err
}
