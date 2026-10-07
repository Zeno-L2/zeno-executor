// Package znndtest serves a recorded znnd ledger over JSON-RPC for tests. The recording holds a
// Settlement scenario captured from a go-zenon chain, with state proofs for the keys the L1
// read path touches, and an input-assignment scenario. The package imports no internal/znnd
// package, so the client under test is checked against the recording and not against itself.
package znndtest

import (
	"bytes"
	_ "embed"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/zenon-network/go-zenon/chain/nom"
	"github.com/zenon-network/go-zenon/common/types"
	"github.com/zenon-network/go-zenon/vm/embedded/definition"
)

//go:embed testdata/readpath-fixture.json
var fixtureJSON []byte

// Fixture is the recording. Momenta are ascending by height and may skip heights between
// momentum 1 and the first kept height.
type Fixture struct {
	ReadHeight           uint64            `json:"readHeight"`
	ExecutorAddress      types.Address     `json:"executorAddress"`
	PillarName           string            `json:"pillarName"`
	DomainID             uint32            `json:"domainId"`
	EmptyDomainID        uint32            `json:"emptyDomainId"`
	AssetID              types.Hash        `json:"assetId"`
	UnconfiguredAssetID  types.Hash        `json:"unconfiguredAssetId"`
	ProcessedOutboxID    types.Hash        `json:"processedOutboxId"`
	UnprocessedOutboxID  types.Hash        `json:"unprocessedOutboxId"`
	StateRoot            types.Hash        `json:"stateRoot"`
	Proofs               []FixtureProof    `json:"proofs"`
	Momenta              []json.RawMessage `json:"momenta"`
	FrontierAccountBlock json.RawMessage   `json:"frontierAccountBlock"`
	AccountBlocks        []json.RawMessage `json:"accountBlocks"` // api.AccountBlock per scenario send
	Assignment           AssignmentFixture `json:"assignment"`
	Expected             Expected          `json:"expected"`
}

// FixtureProof is one recorded getProof result for a raw storage key at a height.
type FixtureProof struct {
	Height uint64 `json:"height"`
	Key    []byte `json:"key"`
	Result Proof  `json:"result"`
}

// AssignmentFixture describes the input-assignment scenario: one momentum (ReceiveHeight)
// where Settlement received two accepted sends and one rejected send, all confirmed in
// SendHeight.
type AssignmentFixture struct {
	PreStateRootEmptyHeight      uint64            `json:"preStateRootEmptyHeight"`
	PreStateRootSettlementHeight uint64            `json:"preStateRootSettlementHeight"`
	SendHeight                   uint64            `json:"sendHeight"`
	ReceiveHeight                uint64            `json:"receiveHeight"`
	EmptyHeight                  uint64            `json:"emptyHeight"`
	RejectedSendHash             types.Hash        `json:"rejectedSendHash"`
	Entries                      []AssignmentEntry `json:"entries"`
}

// AssignmentEntry has the same field names, types and order as ledger.AssignmentEntry, so
// tests convert with ledger.AssignmentEntry(e). znndtest does not import ledger.
type AssignmentEntry struct {
	Ordinal                uint64     `json:"ordinal"`
	GlobalInputIndex       uint64     `json:"globalInputIndex"`
	SourceAccountBlockHash types.Hash `json:"sourceAccountBlockHash"`
	SendMomentumHeight     uint64     `json:"sendMomentumHeight"`
}

// Proof is the getProof result shape.
type Proof struct {
	Value []byte     `json:"value"`
	Proof []byte     `json:"proof"`
	Root  types.Hash `json:"root"`
}

// Expected holds the values the definition getters returned at ReadHeight.
type Expected struct {
	DomainRecord       definition.DomainRecord        `json:"domainRecord"`
	Cursor             definition.InputSequenceCursor `json:"cursor"`
	BatchEntries       []definition.BatchEntry        `json:"batchEntries"`
	DomainInputCounter uint64                         `json:"domainInputCounter"`
	Ordinal            definition.DomainInputOrdinal  `json:"ordinal"`
	AssetConfig        definition.AssetConfig         `json:"assetConfig"`
	GlobalPause        definition.PauseState          `json:"globalPause"`
	DomainPause        definition.PauseState          `json:"domainPause"`
	ExecutorSetMember  definition.ExecutorSetMember   `json:"executorSetMember"`
}

// Load decodes the embedded recording.
func Load(t testing.TB) *Fixture {
	t.Helper()
	fx := new(Fixture)
	if err := json.Unmarshal(fixtureJSON, fx); err != nil {
		t.Fatalf("znndtest: decode fixture: %v", err)
	}
	return fx
}

// DetailedMomentum decodes the fixture entry at height ("momentum" and "blocks") into a fresh
// value on every call, so callers may mutate it; t.Fatal when absent.
func (fx *Fixture) DetailedMomentum(t testing.TB, height uint64) *nom.DetailedMomentum {
	t.Helper()
	for _, raw := range fx.Momenta {
		var e struct {
			Momentum *nom.Momentum       `json:"momentum"`
			Blocks   []*nom.AccountBlock `json:"blocks"`
		}
		if err := json.Unmarshal(raw, &e); err != nil {
			t.Fatalf("znndtest: decode momentum entry: %v", err)
		}
		if e.Momentum != nil && e.Momentum.Height == height {
			return &nom.DetailedMomentum{Momentum: e.Momentum, AccountBlocks: e.Blocks}
		}
	}
	t.Fatalf("znndtest: fixture has no momentum at height %d", height)
	return nil
}

// Server is a fake znnd JSON-RPC endpoint backed by a Fixture.
type Server struct {
	URL string

	fx       *Fixture
	frontier uint64
	byHeight map[uint64]json.RawMessage // height -> detailed momentum entry
	headers  map[uint64]json.RawMessage // height -> momentum member
	blocks   map[types.Hash]json.RawMessage

	mu     sync.Mutex
	down   bool
	tamper func(p *Proof)
	pad    int
}

// NewServer starts a server whose frontier momentum is the fixture's momentum at height
// frontier. It closes when the test ends.
func NewServer(t testing.TB, fx *Fixture, frontier uint64) *Server {
	t.Helper()
	s := &Server{
		fx:       fx,
		frontier: frontier,
		byHeight: make(map[uint64]json.RawMessage),
		headers:  make(map[uint64]json.RawMessage),
		blocks:   make(map[types.Hash]json.RawMessage),
	}
	for _, raw := range fx.AccountBlocks {
		var head struct {
			Hash types.Hash `json:"hash"`
		}
		if err := json.Unmarshal(raw, &head); err != nil {
			t.Fatalf("znndtest: decode account block: %v", err)
		}
		s.blocks[head.Hash] = raw
	}
	for _, raw := range fx.Momenta {
		var entry struct {
			Momentum json.RawMessage `json:"momentum"`
		}
		if err := json.Unmarshal(raw, &entry); err != nil {
			t.Fatalf("znndtest: decode momentum entry: %v", err)
		}
		var head struct {
			Height uint64 `json:"height"`
		}
		if err := json.Unmarshal(entry.Momentum, &head); err != nil {
			t.Fatalf("znndtest: decode momentum header: %v", err)
		}
		s.byHeight[head.Height] = raw
		s.headers[head.Height] = entry.Momentum
	}
	if _, ok := s.byHeight[frontier]; !ok {
		t.Fatalf("znndtest: fixture has no momentum at frontier height %d", frontier)
	}
	hs := httptest.NewServer(http.HandlerFunc(s.handle))
	t.Cleanup(hs.Close)
	s.URL = hs.URL
	return s
}

// PadTo makes every later JSON-RPC response body exactly n bytes, by appending spaces (JSON
// whitespace) to an encoded response shorter than n. Zero turns padding off.
func (s *Server) PadTo(n int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.pad = n
}

// SetDown makes every request fail with HTTP 503 while down is true.
func (s *Server) SetDown(down bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.down = down
}

// SetMomentum makes the server serve dm at dm.Momentum.Height from now on, in place of the
// recorded entry. A TamperProof callback may call it.
func (s *Server) SetMomentum(dm *nom.DetailedMomentum) {
	entry, err := json.Marshal(map[string]any{"momentum": dm.Momentum, "blocks": dm.AccountBlocks})
	if err != nil {
		panic(err)
	}
	head, err := json.Marshal(dm.Momentum)
	if err != nil {
		panic(err)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.byHeight[dm.Momentum.Height] = entry
	s.headers[dm.Momentum.Height] = head
}

// TamperProof applies f to a copy of each getProof result before the server writes it. A nil
// f clears the tamper.
func (s *Server) TamperProof(f func(p *Proof)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.tamper = f
}

type rpcRequest struct {
	ID     json.RawMessage   `json:"id"`
	Method string            `json:"method"`
	Params []json.RawMessage `json:"params"`
}

type rpcError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

type rpcResponse struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id"`
	Result  json.RawMessage `json:"result,omitempty"`
	Error   *rpcError       `json:"error,omitempty"`
}

var errNoSuchVersion = &rpcError{Code: -32000, Message: "no such version"}

func (s *Server) handle(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	down, tamper, pad := s.down, s.tamper, s.pad
	s.mu.Unlock()
	if down {
		http.Error(w, "down", http.StatusServiceUnavailable)
		return
	}
	var req rpcRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	resp := rpcResponse{JSONRPC: "2.0", ID: req.ID}
	result, rerr := s.call(req, tamper)
	if rerr != nil {
		resp.Error = rerr
	} else {
		b, err := json.Marshal(result)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		resp.Result = b
	}
	w.Header().Set("Content-Type", "application/json")
	var buf bytes.Buffer
	_ = json.NewEncoder(&buf).Encode(resp)
	if n := pad - buf.Len(); n > 0 {
		buf.Write(bytes.Repeat([]byte{' '}, n))
	}
	_, _ = w.Write(buf.Bytes())
}

func (s *Server) call(req rpcRequest, tamper func(p *Proof)) (any, *rpcError) {
	bad := func(msg string) (any, *rpcError) { return nil, &rpcError{Code: -32602, Message: msg} }
	switch req.Method {
	case "ledger.getFrontierMomentum":
		s.mu.Lock()
		defer s.mu.Unlock()
		return s.headers[s.frontier], nil

	case "ledger.getDetailedMomentumsByHeight":
		var h, c uint64
		if len(req.Params) != 2 || json.Unmarshal(req.Params[0], &h) != nil || json.Unmarshal(req.Params[1], &c) != nil {
			return bad("invalid params")
		}
		if h == 0 {
			return nil, &rpcError{Code: -32000, Message: "height param is zero"}
		}
		s.mu.Lock()
		defer s.mu.Unlock()
		list := []json.RawMessage{}
		if h <= s.frontier {
			last := s.frontier
			if c > 0 && h+c-1 < last {
				last = h + c - 1
			}
			for n := h; c > 0 && n <= last; n++ {
				entry, ok := s.byHeight[n]
				if !ok {
					return nil, &rpcError{Code: -32000, Message: fmt.Sprintf("fixture has no momentum at height %d", n)}
				}
				list = append(list, entry)
			}
		}
		return map[string]any{"list": list, "count": s.frontier}, nil

	case "ledger.getProof":
		var h uint64
		var key []byte
		if len(req.Params) != 2 || json.Unmarshal(req.Params[0], &h) != nil || json.Unmarshal(req.Params[1], &key) != nil {
			return bad("invalid params")
		}
		recorded := false
		for _, p := range s.fx.Proofs {
			if p.Height == h {
				recorded = true
				break
			}
		}
		if !recorded {
			return nil, errNoSuchVersion
		}
		for _, p := range s.fx.Proofs {
			if p.Height != h || !bytes.Equal(p.Key, key) {
				continue
			}
			out := Proof{Value: bytes.Clone(p.Result.Value), Proof: bytes.Clone(p.Result.Proof), Root: p.Result.Root}
			if tamper != nil {
				tamper(&out)
			}
			return out, nil
		}
		return nil, &rpcError{Code: -32000, Message: "fixture has no proof for key"}

	case "ledger.getStateRoot":
		var h uint64
		if len(req.Params) != 1 || json.Unmarshal(req.Params[0], &h) != nil {
			return bad("invalid params")
		}
		if h != s.fx.ReadHeight {
			return nil, errNoSuchVersion
		}
		return s.fx.StateRoot, nil

	case "ledger.getAccountBlockByHash":
		var hash types.Hash
		if len(req.Params) != 1 || json.Unmarshal(req.Params[0], &hash) != nil {
			return bad("invalid params")
		}
		if raw, ok := s.blocks[hash]; ok {
			return raw, nil
		}
		return nil, nil

	case "ledger.getFrontierAccountBlock":
		var addr types.Address
		if len(req.Params) != 1 || json.Unmarshal(req.Params[0], &addr) != nil {
			return bad("invalid params")
		}
		if addr == s.fx.ExecutorAddress {
			return s.fx.FrontierAccountBlock, nil
		}
		return nil, nil
	}
	return nil, &rpcError{Code: -32601, Message: "method not found"}
}
