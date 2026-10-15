package gateway

import (
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/zenon-network/go-zenon/common/crypto"
	"github.com/zenon-network/go-zenon/common/trie"
	"github.com/zenon-network/go-zenon/common/types"

	"github.com/digitalSloth/go-executor/domain"
	"github.com/digitalSloth/go-executor/internal/codec"
	"github.com/digitalSloth/go-executor/internal/ledger"
	"github.com/digitalSloth/go-executor/plugins/wasm"
	wasmreader "github.com/digitalSloth/go-executor/plugins/wasm/reader"
)

// senderMap is a SenderSource over a fixed send hash to address map.
type senderMap map[types.Hash]types.Address

func (m senderMap) Sender(_ uint32, h types.Hash) (types.Address, error) {
	a, ok := m[h]
	if !ok {
		return types.Address{}, fmt.Errorf("no sender for send %s", h)
	}
	return a, nil
}

// newReaderServer is newTestServer with the WASM reader of network
// "test".
func newReaderServer(t *testing.T, chains ...*Chain) (*Server, *logSink) {
	t.Helper()
	srv, sink, _ := newTestServer(t, nil, chains...)
	srv.readers = map[uint8]domain.Reader{0: wasmreader.New("test")}
	return srv, sink
}

// leafPath is the state path of local key key under contract.
func leafPath(contract types.Hash, key ...byte) types.Hash {
	var h types.Hash
	copy(h[:], crypto.Hash(contract[:], key))
	return h
}

// callerOf is the L1 caller hash of addr.
func callerOf(addr types.Address) types.Hash {
	var h types.Hash
	copy(h[:], crypto.Hash([]byte("L1_CALLER:"), addr[:]))
	return h
}

func testChainID() types.Hash {
	var h types.Hash
	copy(h[:], crypto.Hash([]byte("ZENON_WASM_L2:"), []byte("test")))
	return h
}

// loadModule reads one vector's module bytes from the plugin's
// execution conformance fixture.
func loadModule(t *testing.T, id string) []byte {
	t.Helper()
	data, err := os.ReadFile(filepath.Join("..", "..", "plugins", "wasm", "testdata", "execution-conformance-v1.json"))
	if err != nil {
		t.Fatal(err)
	}
	var f struct {
		Vectors []struct {
			ID            string `json:"id"`
			ModuleWasmHex string `json:"module_wasm_hex"`
		} `json:"vectors"`
	}
	if err := json.Unmarshal(data, &f); err != nil {
		t.Fatal(err)
	}
	for _, v := range f.Vectors {
		if v.ID == id {
			b, err := hex.DecodeString(v.ModuleWasmHex)
			if err != nil {
				t.Fatal(err)
			}
			return b
		}
	}
	t.Fatalf("%s not in the fixture", id)
	return nil
}

// proofJSON is one path of a read result.
type proofJSON struct {
	Path    string  `json:"path"`
	Present bool    `json:"present"`
	Value   *string `json:"value"`
	Proof   string  `json:"proof"`
}

// readJSON is zeno_read's result.
type readJSON struct {
	DomainID      uint32          `json:"domainId"`
	BatchID       uint64          `json:"batchId"`
	Verified      bool            `json:"verified"`
	PostStateRoot string          `json:"postStateRoot"`
	View          string          `json:"view"`
	Result        json.RawMessage `json:"result"`
	Proofs        []proofJSON     `json:"proofs"`
}

// contractRes is the contract view's result.
type contractRes struct {
	CodeHash       *string `json:"codeHash"`
	SourceCodeHash *string `json:"sourceCodeHash"`
	UpgradePolicy  *string `json:"upgradePolicy"`
	Owner          *string `json:"owner"`
	OwnerCaller    *string `json:"ownerCaller"`
	Quota          *struct {
		Purchased uint64 `json:"purchased"`
		Used      uint64 `json:"used"`
	} `json:"quota"`
	NativeAddress *string `json:"nativeAddress"`
}

// readView calls zeno_read and decodes the result.
func readView(t *testing.T, srv *Server, view, args string, tag any) readJSON {
	t.Helper()
	var out readJSON
	rpcOK(t, srv, "zeno_read", fmt.Sprintf(`[1,%q,%s,%v]`, view, args, tagParam(tag)), &out)
	if out.View != view || out.DomainID != 1 {
		t.Fatalf("read %s: result names domain %d view %q", view, out.DomainID, out.View)
	}
	return out
}

func tagParam(tag any) string {
	if s, ok := tag.(string); ok {
		return fmt.Sprintf("%q", s)
	}
	return fmt.Sprint(tag)
}

// checkProofs verifies every proof of r against r.PostStateRoot, and
// returns each path's value, nil when absent.
func checkProofs(t *testing.T, r readJSON) map[string]*string {
	t.Helper()
	root, err := types.HexToHash(r.PostStateRoot)
	if err != nil {
		t.Fatal(err)
	}
	out := make(map[string]*string)
	for _, p := range r.Proofs {
		ph, err := types.HexToHash(p.Path)
		if err != nil {
			t.Fatal(err)
		}
		proof, err := hex.DecodeString(p.Proof)
		if err != nil {
			t.Fatal(err)
		}
		if p.Present {
			if p.Value == nil {
				t.Fatalf("path %s present with a null value", p.Path)
			}
			val, err := hex.DecodeString(*p.Value)
			if err != nil {
				t.Fatal(err)
			}
			if ok, err := trie.VerifyProofByPath(root, trie.Path(ph), val, proof); !ok || err != nil {
				t.Fatalf("path %s: the inclusion proof does not verify: %v", p.Path, err)
			}
		} else {
			if p.Value != nil {
				t.Fatalf("path %s absent with a value", p.Path)
			}
			if ok, err := trie.VerifyAbsenceByPath(root, trie.Path(ph), proof); !ok || err != nil {
				t.Fatalf("path %s: the absence proof does not verify: %v", p.Path, err)
			}
		}
		out[p.Path] = p.Value
	}
	return out
}

func (r readJSON) contract(t *testing.T) contractRes {
	t.Helper()
	var c contractRes
	if err := json.Unmarshal(r.Result, &c); err != nil {
		t.Fatal(err)
	}
	return c
}

func strOf(p *string) string {
	if p == nil {
		return "null"
	}
	return *p
}

// wasmChain is a chain of four accepted batches built by the real
// plugin: a deploy of module A by D, an immutable deploy of A by E, an
// upgrade of D's contract C to module B by its owner O, an ownership
// transfer to N, and N's renounce.
type wasmChain struct {
	srv        *Server
	r          *rig
	sink       *logSink
	cs         []codec.BatchCommitment
	d, e, o, n types.Address
	modA, modB []byte
	codeA      types.Hash
	codeB      types.Hash
	c, ce      types.Hash // D's and E's contracts
	finalizeD  uint64     // the global index of D's finalize
	chunkA     uint64     // the global index of A's chunk 0 as D sent it last
}

func buildWasmChain(t *testing.T) *wasmChain {
	t.Helper()
	w := &wasmChain{d: types.Address{0xD1}, e: types.Address{0xE1}, o: types.Address{0x01}, n: types.Address{0x0E}}
	w.modA, w.modB = loadModule(t, "EXEC-DEPLOY-1"), loadModule(t, "EXEC-UPGRADE-1")
	w.codeA, w.codeB = wasm.CodeHash(w.modA), wasm.CodeHash(w.modB)

	plugin, err := wasm.New(wasm.Config{NetworkName: "test", CodeStorePath: t.TempDir()})
	if err != nil {
		t.Fatalf("wasm.New: %v", err)
	}
	t.Cleanup(func() { _ = plugin.Close() })

	senders := senderMap{}
	var batches [][]ledger.Input
	var g uint64
	add := func(b int, sender types.Address, args codec.InputArgs, chunk []byte) uint64 {
		g++
		data, err := codec.Input{SourceAccountBlockHash: hashOfIndex(g), Args: args}.Encode()
		if err != nil {
			t.Fatal(err)
		}
		senders[hashOfIndex(g)] = sender
		for len(batches) <= b {
			batches = append(batches, nil)
		}
		batches[b] = append(batches[b], ledger.Input{
			Ctx:     domain.InputContext{GlobalInputIndex: g, L1Sender: sender, SourceData: chunk},
			Payload: data,
		})
		return g
	}
	meta := func(policy uint8, owner types.Address) ([]byte, types.Hash) {
		m := codec.ContractMetadata{Version: codec.ContractMetadataVersion, UpgradePolicy: policy, Owner: owner}.Encode()
		var h types.Hash
		copy(h[:], crypto.Hash(m))
		return m, h
	}
	start := func(b int, sender types.Address, code types.Hash, mod []byte, m []byte, mh types.Hash) uint64 {
		return add(b, sender, codec.DeployStartArgs{CodeHash: code, MetadataHash: mh, TotalSize: uint64(len(mod)), ChunkCount: 1, Metadata: m}, nil)
	}
	chunk := func(b int, sender types.Address, code types.Hash, data []byte) uint64 {
		var ch types.Hash
		copy(ch[:], crypto.Hash(data))
		return add(b, sender, codec.DeployChunkArgs{CodeHash: code, ChunkIndex: 0, ChunkHash: ch}, data)
	}

	om, omh := meta(codec.PolicyOwnerUpgradeable, w.o)
	start(0, w.d, w.codeA, w.modA, om, omh)
	chunk(0, w.d, w.codeA, []byte("other bytes"))
	w.chunkA = chunk(0, w.d, w.codeA, w.modA)
	w.finalizeD = add(0, w.d, codec.DeployFinalizeArgs{CodeHash: w.codeA}, nil)
	im, imh := meta(codec.PolicyImmutable, types.Address{})
	start(0, w.e, w.codeA, w.modA, im, imh)
	chunk(0, w.e, w.codeA, w.modA)
	finalizeE := add(0, w.e, codec.DeployFinalizeArgs{CodeHash: w.codeA}, nil)

	var empty types.Hash
	copy(empty[:], crypto.Hash(nil))
	start(1, w.o, w.codeB, w.modB, []byte{}, empty)
	chunk(1, w.o, w.codeB, w.modB)
	add(1, w.o, codec.UpgradeArgs{ContractID: contractIDOf(w.d, w.finalizeD), NewCodeHash: w.codeB}, nil)
	w.c, w.ce = contractIDOf(w.d, w.finalizeD), contractIDOf(w.e, finalizeE)
	add(1, w.d, codec.TransferOwnershipArgs{ContractID: w.c, NewOwner: w.d}, nil)
	add(2, w.o, codec.TransferOwnershipArgs{ContractID: w.c, NewOwner: w.n}, nil)
	add(3, w.n, codec.TransferOwnershipArgs{ContractID: w.c, NewOwner: types.Address{}}, nil)

	dir := t.TempDir()
	w.cs = buildFrom(t, dir, nil, plugin, batches)
	s := newFakeSettlement()
	s.height = 10
	for i, c := range w.cs {
		s.accept(c, uint64(20+i))
	}
	w.r = newIndexedChainWith(t, dir, s, senders)
	if h := doPoll(t, w.r); h.Latest == nil || h.Latest.BatchID != 3 || h.Status != StatusOK {
		t.Fatalf("the chain did not reach batch 3: %+v", derefHead(h))
	}
	w.srv, w.sink = newReaderServer(t, w.r.c)
	return w
}

func contractIDOf(deployer types.Address, g uint64) types.Hash {
	chain := testChainID()
	var h types.Hash
	copy(h[:], crypto.Hash(deployer[:], binary.BigEndian.AppendUint64(nil, g), chain[:]))
	return h
}

func (w *wasmChain) contract(t *testing.T, id types.Hash, batch uint64) (readJSON, contractRes) {
	t.Helper()
	r := readView(t, w.srv, "contract", fmt.Sprintf(`{"contractId":"%s"}`, id), batch)
	return r, r.contract(t)
}

func TestViewsWasm(t *testing.T) {
	w := buildWasmChain(t)
	srv := w.srv
	for g, want := range map[uint64]string{4: "SUCCESS", 7: "SUCCESS", 10: "SUCCESS", 11: "UNAUTHORIZED", 12: "SUCCESS", 13: "SUCCESS"} {
		var in receiptRes
		rpcOK(t, srv, "zeno_getReceipt", fmt.Sprintf(`["%s"]`, hashOfIndex(g)), &in)
		if in.Receipt.Status != want {
			t.Fatalf("input %d: status %s, want %s", g, in.Receipt.Status, want)
		}
	}

	t.Run("contract", func(t *testing.T) {
		chain := func(r readJSON, batch uint64) {
			t.Helper()
			if r.BatchID != batch || !r.Verified || r.PostStateRoot != w.cs[batch].PostStateRoot.String() {
				t.Fatalf("batch %d: result names batch %d verified %v root %s", batch, r.BatchID, r.Verified, r.PostStateRoot)
			}
		}
		for batch, want := range map[uint64]types.Address{0: w.o, 1: w.o, 2: w.n} {
			r, c := w.contract(t, w.c, batch)
			chain(r, batch)
			vals := checkProofs(t, r)
			if len(r.Proofs) != 5 {
				t.Fatalf("batch %d: %d proofs, want 5", batch, len(r.Proofs))
			}
			for i, p := range r.Proofs {
				if p.Path != leafPath(w.c, 0, byte(i+1)).String() || p.Present != (i < 4) {
					t.Fatalf("batch %d: proof %d is %s present %v", batch, i, p.Path, p.Present)
				}
			}
			if c.Owner == nil || *c.Owner != want.String() {
				t.Fatalf("batch %d: owner %s, want %s", batch, strOf(c.Owner), want)
			}
			if got := callerOf(want).String(); strOf(c.OwnerCaller) != got || strOf(vals[leafPath(w.c, 0, 4).String()]) != got {
				t.Fatalf("batch %d: ownerCaller %s, the owner leaf %s, want %s", batch, strOf(c.OwnerCaller), strOf(vals[leafPath(w.c, 0, 4).String()]), got)
			}
			if strOf(c.CodeHash) != strOf(vals[leafPath(w.c, 0, 1).String()]) || strOf(c.SourceCodeHash) != strOf(vals[leafPath(w.c, 0, 2).String()]) {
				t.Fatalf("batch %d: the code hashes do not match their proofs", batch)
			}
			if c.UpgradePolicy == nil || *c.UpgradePolicy != "OWNER_UPGRADEABLE" || c.Quota == nil || c.NativeAddress != nil {
				t.Fatalf("batch %d: %+v", batch, c)
			}
		}
		if _, c := w.contract(t, w.c, 0); strOf(c.SourceCodeHash) != w.codeA.String() {
			t.Fatalf("batch 0 sourceCodeHash %s, want %s", strOf(c.SourceCodeHash), w.codeA)
		}
		if _, c := w.contract(t, w.c, 1); strOf(c.SourceCodeHash) != w.codeB.String() {
			t.Fatalf("batch 1 sourceCodeHash %s, want %s", strOf(c.SourceCodeHash), w.codeB)
		}

		r, c := w.contract(t, w.c, 3)
		chain(r, 3)
		vals := checkProofs(t, r)
		if c.Owner != nil || c.OwnerCaller != nil || c.UpgradePolicy == nil || *c.UpgradePolicy != "IMMUTABLE" {
			t.Fatalf("batch 3: %+v", c)
		}
		if vals[leafPath(w.c, 0, 4).String()] != nil || r.Proofs[3].Present {
			t.Fatal("batch 3: the owner leaf is present")
		}

		r, c = w.contract(t, w.ce, 3)
		checkProofs(t, r)
		if c.Owner != nil || c.OwnerCaller != nil || c.UpgradePolicy == nil || *c.UpgradePolicy != "IMMUTABLE" || c.CodeHash == nil {
			t.Fatalf("E's contract: %+v", c)
		}

		r, c = w.contract(t, types.Hash{0xAB}, 3)
		if got, _ := json.Marshal(c); string(got) != `{"codeHash":null,"sourceCodeHash":null,"upgradePolicy":null,"owner":null,"ownerCaller":null,"quota":null,"nativeAddress":null}` {
			t.Fatalf("undeployed contract: %s", got)
		}
		if len(r.Proofs) != 5 {
			t.Fatalf("undeployed contract: %d proofs", len(r.Proofs))
		}
		for _, p := range r.Proofs {
			if p.Present {
				t.Fatalf("undeployed contract: path %s present", p.Path)
			}
		}
		checkProofs(t, r)
	})

	t.Run("module", func(t *testing.T) {
		mod := func(h types.Hash, batch uint64) string {
			t.Helper()
			r := readView(t, srv, "module", fmt.Sprintf(`{"sourceCodeHash":"%s"}`, h), batch)
			if len(r.Proofs) != 0 {
				t.Fatalf("module returned %d proofs", len(r.Proofs))
			}
			var m struct {
				SourceCodeHash string `json:"sourceCodeHash"`
				Module         string `json:"module"`
			}
			if err := json.Unmarshal(r.Result, &m); err != nil {
				t.Fatal(err)
			}
			if m.SourceCodeHash != h.String() {
				t.Fatalf("module names %s, want %s", m.SourceCodeHash, h)
			}
			return m.Module
		}
		if got := mod(w.codeA, 3); got != hex.EncodeToString(w.modA) {
			t.Fatalf("module A = %s", got)
		}
		if got := mod(w.codeB, 3); got != hex.EncodeToString(w.modB) {
			t.Fatalf("module B = %s", got)
		}
		requireRPCErr(t, srv, "zeno_read", fmt.Sprintf(`[1,"module",{"sourceCodeHash":"%s"},0]`, w.codeB), -32004, "")

		_, c := w.contract(t, w.c, 3)
		source, err := types.HexToHash(strOf(c.SourceCodeHash))
		if err != nil {
			t.Fatal(err)
		}
		if got := mod(source, 3); got != hex.EncodeToString(w.modB) {
			t.Fatalf("module of the contract's sourceCodeHash = %s", got)
		}
		requireRPCErr(t, srv, "zeno_read", fmt.Sprintf(`[1,"module",{"sourceCodeHash":"%s"},3]`, strOf(c.CodeHash)), -32004, "")
	})

	t.Run("contractsByDeployer", func(t *testing.T) {
		for _, args := range []string{
			fmt.Sprintf(`{"address":"%s"}`, w.d),
			fmt.Sprintf(`{"address":"%s","cursor":null}`, w.d),
		} {
			r := readView(t, srv, "contractsByDeployer", args, "latest")
			var res struct {
				Contracts []struct {
					ContractID       string `json:"contractId"`
					SendHash         string `json:"sendHash"`
					GlobalInputIndex uint64 `json:"globalInputIndex"`
					BatchID          uint64 `json:"batchId"`
				} `json:"contracts"`
				NextCursor *string `json:"nextCursor"`
			}
			if err := json.Unmarshal(r.Result, &res); err != nil {
				t.Fatal(err)
			}
			if len(res.Contracts) != 1 || res.Contracts[0].ContractID != w.c.String() || res.Contracts[0].GlobalInputIndex != w.finalizeD ||
				res.Contracts[0].SendHash != hashOfIndex(w.finalizeD).String() || res.Contracts[0].BatchID != 0 || res.NextCursor != nil {
				t.Fatalf("%s: %+v", args, res)
			}
			if len(r.Proofs) != 0 {
				t.Fatalf("%d proofs", len(r.Proofs))
			}
		}
	})

	t.Run("receipt input", func(t *testing.T) {
		var in struct {
			Input map[string]any `json:"input"`
		}
		rpcOK(t, srv, "zeno_getReceipt", fmt.Sprintf(`["%s"]`, hashOfIndex(w.finalizeD)), &in)
		if in.Input["kind"] != "DeployContractFinalize" || in.Input["codeHash"] != w.codeA.String() || in.Input["inputKind"] != float64(4) {
			t.Fatalf("input = %v", in.Input)
		}
		var list struct {
			Items []struct {
				Input map[string]any `json:"input"`
			} `json:"items"`
		}
		rpcOK(t, srv, "zeno_getInputs", `[{"domainId":1,"inputKind":4,"toBatch":0}]`, &list)
		if len(list.Items) != 2 || list.Items[0].Input["kind"] != "DeployContractFinalize" {
			t.Fatalf("items = %+v", list.Items)
		}
	})

	t.Run("budgets", func(t *testing.T) {
		v, err := w.r.idx.view()
		if err != nil {
			t.Fatal(err)
		}
		defer v.Release()
		ver, ok := w.r.c.VersionAt(3)
		if !ok {
			t.Fatal("no version at batch 3")
		}
		bv, _, err := v.batchAt(3)
		if err != nil {
			t.Fatal(err)
		}
		ctx := func(b *budget) *readContext {
			return &readContext{v: v, b: b, version: ver, hi: bv.LastInputSeq, seen: map[trie.Path]bool{}}
		}
		isScanErr := func(err error) bool {
			re, ok := err.(*rpcError)
			return ok && re.Code == -32600 && re.Message == scanBudgetMsg
		}
		spent := ctx(&budget{scan: 0, scanBytes: 1 << 20})
		if _, _, err := spent.LastOwnerInput(w.c); !isScanErr(err) {
			t.Fatalf("LastOwnerInput with no scan keys: %v", err)
		}
		if _, _, err := spent.FirstCodeInput(w.codeA); !isScanErr(err) {
			t.Fatalf("FirstCodeInput with no scan keys: %v", err)
		}
		b := &budget{scan: 10, scanBytes: 1 << 20}
		data, err := ctx(b).ChunkData(w.chunkA)
		if err != nil {
			t.Fatal(err)
		}
		if string(data) != string(w.modA) || b.scanBytes != 1<<20-len(w.modA) {
			t.Fatalf("ChunkData returned %d bytes and left %d scan bytes, want the module and %d", len(data), b.scanBytes, 1<<20-len(w.modA))
		}
		if _, err := ctx(&budget{scan: 10, scanBytes: 0}).ChunkData(w.chunkA); !isScanErr(err) {
			t.Fatalf("ChunkData with no scan bytes: %v", err)
		}
		n := 0
		err = ctx(&budget{scan: 10, scanBytes: 1}).InputsBySender(w.d, math.MaxUint64, func(domain.IndexedInput) (bool, error) {
			n++
			return true, nil
		})
		if !isScanErr(err) || n != 1 {
			t.Fatalf("InputsBySender with 1 scan byte: err %v after %d inputs, want the scan error after 1", err, n)
		}
	})

	t.Run("pinned inputs", func(t *testing.T) {
		v, err := w.r.idx.view()
		if err != nil {
			t.Fatal(err)
		}
		defer v.Release()
		at := func(batch uint64) *readContext {
			ver, ok := w.r.c.VersionAt(batch)
			if !ok {
				t.Fatalf("no version at batch %d", batch)
			}
			bv, _, err := v.batchAt(batch)
			if err != nil {
				t.Fatal(err)
			}
			return &readContext{v: v, b: &budget{scan: 100, scanBytes: 1 << 20}, version: ver, hi: bv.LastInputSeq, seen: map[trie.Path]bool{}}
		}
		visited := func(batch uint64) []uint64 {
			var gs []uint64
			err := at(batch).InputsBySender(w.o, math.MaxUint64, func(in domain.IndexedInput) (bool, error) {
				gs = append(gs, in.GlobalInputIndex)
				return true, nil
			})
			if err != nil {
				t.Fatal(err)
			}
			return gs
		}
		if gs := visited(0); len(gs) != 0 {
			t.Fatalf("batch 0 visited %v, want no input of the owner", gs)
		}
		gs := visited(1)
		if len(gs) != 3 || !slices.IsSortedFunc(gs, func(a, b uint64) int { return int(b) - int(a) }) {
			t.Fatalf("batch 1 visited %v, want the owner's three inputs newest first", gs)
		}
		if all := visited(2); len(all) != 4 {
			t.Fatalf("batch 2 visited %v, want the owner's four inputs", all)
		}
	})
}

func TestViewsScripted(t *testing.T) {
	contract := types.Hash{0xC7}
	code := types.Hash{0xE1}
	x, y := types.Address{0xB1}, types.Address{0xB2}
	meta := codec.ContractMetadata{Version: codec.ContractMetadataVersion, UpgradePolicy: codec.PolicyOwnerUpgradeable, Owner: x}.Encode()
	var mh types.Hash
	copy(mh[:], crypto.Hash(meta))
	owner := callerOf(y)
	kPath, vPath := trie.Path(leafPath(contract, 'k')), trie.Path(leafPath(contract, 'v'))
	diff := []domain.StateDiffEntry{
		{Path: trie.Path(leafPath(contract, 0, 1)), Value: code[:]},
		{Path: trie.Path(leafPath(contract, 0, 4)), Value: owner[:]},
		{Path: kPath, Value: []byte{}},
		{Path: vPath, Value: []byte{0xAB}},
	}
	slices.SortFunc(diff, func(a, b domain.StateDiffEntry) int { return strings.Compare(string(a.Path[:]), string(b.Path[:])) })
	dir := t.TempDir()
	cs := buildHistory(t, dir, nil, [][]fixtureInput{{
		{index: 1, args: codec.DeployStartArgs{CodeHash: code, MetadataHash: mh, TotalSize: 4, ChunkCount: 1, Metadata: meta}, effects: domain.Effects{Status: codec.StatusSuccess}},
		{index: 2, args: codec.DeployFinalizeArgs{CodeHash: code}, effects: domain.Effects{Status: codec.StatusSuccess, ContractID: contract, StateDiff: diff}},
	}})
	s := newFakeSettlement()
	s.height = 10
	s.accept(cs[0], 20)
	r := newIndexedChainWith(t, dir, s, senderMap{hashOfIndex(1): x, hashOfIndex(2): x})
	doPoll(t, r)
	srv, sink := newReaderServer(t, r.c)

	t.Run("failed owner check", func(t *testing.T) {
		requireRPCErr(t, srv, "zeno_read", fmt.Sprintf(`[1,"contract",{"contractId":"%s"}]`, contract), -32603, "internal error")
		if sink.count("owner of contract", contract.String(), "does not match") != 1 {
			t.Fatalf("the log does not name the contract once:\n%s", sink.dump())
		}
		if h := getHead(t, srv); h.Status != "OK" {
			t.Fatalf("status = %s", h.Status)
		}
	})
	t.Run("storage", func(t *testing.T) {
		key := func(k string, present bool, value string) {
			t.Helper()
			res := readView(t, srv, "storage", fmt.Sprintf(`{"contractId":"%s","key":"%x"}`, contract, k), "latest")
			var out struct {
				Present bool    `json:"present"`
				Value   *string `json:"value"`
			}
			if err := json.Unmarshal(res.Result, &out); err != nil {
				t.Fatal(err)
			}
			if out.Present != present || (value == "<null>") != (out.Value == nil) || (out.Value != nil && *out.Value != value) {
				t.Fatalf("key %q: %s", k, res.Result)
			}
			if len(res.Proofs) != 1 || res.Proofs[0].Present != present {
				t.Fatalf("key %q: proofs %+v", k, res.Proofs)
			}
			checkProofs(t, res)
		}
		key("k", true, "")
		key("v", true, "ab")
		key("z", false, "<null>")
		key("", false, "<null>")
	})
}

func TestReadErrors(t *testing.T) {
	_, cs, s, r, _, _ := readRig(t)
	acceptFinal(s, cs, 1)
	s.height = 21
	doPoll(t, r)
	srv, _ := newReaderServer(t, r.c)
	plain, _, _ := newTestServer(t, nil, r.c)
	hash := types.Hash{0x77}

	requireRPCErr(t, plain, "zeno_read", `[1,"contract",{"contractId":"`+hash.String()+`"}]`, -32005, "the domain has no such view")
	requireRPCErr(t, srv, "zeno_read", `[1,"nope",{}]`, -32005, "the domain has no such view")
	requireRPCErr(t, srv, "zeno_read", `[1,"contract"]`, -32602, "invalid params: contractId is required")

	for _, c := range []struct{ name, params, msg string }{
		{"not an object", `[1,"contract",5]`, "invalid params: params must be an object with members contractId"},
		{"an array", `[1,"contract",[]]`, "invalid params: params must be an object with members contractId"},
		{"unknown member", `[1,"contract",{"contractId":"` + hash.String() + `","x":1}]`, "invalid params: params must be an object with members contractId"},
		{"repeated member", `[1,"contract",{"contractId":"` + hash.String() + `","contractId":"` + hash.String() + `"}]`, "invalid params: params must be an object with members contractId"},
		{"wrong case", `[1,"contract",{"ContractId":"` + hash.String() + `"}]`, "invalid params: params must be an object with members contractId"},
		{"missing contractId", `[1,"contract",{}]`, "invalid params: contractId is required"},
		{"null contractId", `[1,"contract",{"contractId":null}]`, "invalid params: contractId is required"},
		{"bad hash", `[1,"contract",{"contractId":"zz"}]`, "invalid params: contractId must be 64 hex characters"},
		{"bad hex", `[1,"storage",{"contractId":"` + hash.String() + `","key":"zz"}]`, "invalid params: key must be hex of at most 32 bytes"},
		{"odd hex", `[1,"storage",{"contractId":"` + hash.String() + `","key":"abc"}]`, "invalid params: key must be hex of at most 32 bytes"},
		{"33-byte key", `[1,"storage",{"contractId":"` + hash.String() + `","key":"` + strings.Repeat("ab", 33) + `"}]`, "invalid params: key must be hex of at most 32 bytes"},
		{"key not a string", `[1,"storage",{"contractId":"` + hash.String() + `","key":5}]`, "invalid params: key must be hex of at most 32 bytes"},
		{"null params", `[1,"contractsByDeployer",null]`, "invalid params: address is required"},
		{"bad address", `[1,"contractsByDeployer",{"address":"x"}]`, "invalid params: address must be an L1 address"},
		{"7-byte cursor", `[1,"contractsByDeployer",{"address":"` + types.Address{1}.String() + `","cursor":"00000000000000"}]`, "invalid params: cursor must be a nextCursor of this view"},
	} {
		if e := rpcErr(t, srv, "zeno_read", c.params); e.Code != -32602 || e.Message != c.msg {
			t.Errorf("%s: error %d %q, want -32602 %q", c.name, e.Code, e.Message, c.msg)
		}
	}
	var ok readJSON
	rpcOK(t, srv, "zeno_read", `[1,"contractsByDeployer",{"address":"`+types.Address{1}.String()+`","cursor":null}]`, &ok)
	rpcOK(t, srv, "zeno_read", `[1,"contractsByDeployer",{"address":"`+types.Address{1}.String()+`","cursor":"0000000000000001"}]`, &ok)

	requireRPCErr(t, srv, "zeno_read", `[1,"module",{"sourceCodeHash":"`+hash.String()+`"}]`, -32004, "not found: module "+hash.String())
	requireRPCErr(t, srv, "zeno_read", `[1,"contract",{"contractId":"`+hash.String()+`"},0]`, -32002, "")
}

func TestReadErrorsRuntimeKind(t *testing.T) {
	dir, cs := buildA(t)
	s := newFakeSettlement()
	s.height = 10
	s.accept(cs[0], 20)
	s.record.RuntimeKind = 1
	r := newIndexedChain(t, dir, s)
	doPoll(t, r)
	srv, _ := newReaderServer(t, r.c)
	requireRPCErr(t, srv, "zeno_read", `[1,"contract",{"contractId":"`+types.Hash{1}.String()+`"}]`, -32005, "the domain has no such view")
}

func TestReadBudgets(t *testing.T) {
	t.Run("proof paths", func(t *testing.T) {
		_, cs, s, r, _, _ := readRig(t)
		acceptFinal(s, cs, 1)
		doPoll(t, r)
		srv, _ := newReaderServer(t, r.c)
		reqs := make([]string, 52)
		for i := range reqs {
			reqs[i] = fmt.Sprintf(`{"jsonrpc":"2.0","method":"zeno_read","params":[1,"contract",{"contractId":"%s"}],"id":%d}`, types.Hash{0x31}, i+1)
		}
		codes, msg := proofCodes(t, srv, "["+strings.Join(reqs, ",")+"]")
		want := make([]int, 52)
		want[51] = -32600
		if !slices.Equal(codes, want) || msg != proofBudgetMsg {
			t.Fatalf("codes %v, last message %q; want 51 results then -32600 %q", codes, msg, proofBudgetMsg)
		}
		// getProof and zeno_read draw on one budget.
		one := fmt.Sprintf(`{"jsonrpc":"2.0","method":"zeno_getProof","params":[1,["%s","%s","%s","%s","%s","%s"]],"id":1}`, pathHex(pA), pathHex(pA), pathHex(pA), pathHex(pA), pathHex(pA), pathHex(pA))
		codes, _ = proofCodes(t, srv, "["+strings.Join(append(reqs[:51:51], one), ",")+"]")
		if codes[51] != -32600 {
			t.Fatalf("a getProof of 6 paths after 51 reads: code %d", codes[51])
		}
	})
	t.Run("scan keys", func(t *testing.T) {
		w := buildWasmChain(t)
		w.srv.scanKeys = 1
		// An owner from a finalize needs the w/ key and the deployer's
		// inputs.
		requireRPCErr(t, w.srv, "zeno_read", fmt.Sprintf(`[1,"contract",{"contractId":"%s"},0]`, w.c), -32600, scanBudgetMsg)
		// An owner from a transfer needs the one w/ key.
		var out readJSON
		rpcOK(t, w.srv, "zeno_read", fmt.Sprintf(`[1,"contract",{"contractId":"%s"},2]`, w.c), &out)
		if c := out.contract(t); c.Owner == nil || *c.Owner != w.n.String() {
			t.Fatalf("owner %s, want %s", strOf(c.Owner), w.n)
		}
	})
}

func TestChainInfoReader(t *testing.T) {
	dir, cs := buildA(t)
	s := newFakeSettlement()
	s.height = 10
	s.accept(cs[0], 20)
	s.accept(cs[1], 21)
	r := newTestChain(t, dir, s)
	doPoll(t, r)
	srv, _ := newReaderServer(t, r.c)

	const profile = `{"name":"contracts/1","features":{"deposits":true,"nativeAddresses":false,"ownershipTransfer":true,"payableCalls":true,"storageQuota":true,"upgrade":true},"interfaceEncodings":["zeno-selector-v1"]}`
	const views = `[` +
		`{"name":"contract","profile":"contracts/1","params":[{"name":"contractId","type":"bytes32","optional":false}],"result":["codeHash","sourceCodeHash","upgradePolicy","owner","ownerCaller","quota","nativeAddress"]},` +
		`{"name":"storage","profile":"contracts/1","params":[{"name":"contractId","type":"bytes32","optional":false},{"name":"key","type":"bytes","maxLength":32,"optional":false}],"result":["present","value"]},` +
		`{"name":"contractsByDeployer","profile":"contracts/1","params":[{"name":"address","type":"address","optional":false},{"name":"cursor","type":"bytes","maxLength":8,"optional":true}],"result":["contracts","nextCursor"]},` +
		`{"name":"module","profile":"contracts/1","params":[{"name":"sourceCodeHash","type":"bytes32","optional":false}],"result":["sourceCodeHash","module"]}]`
	rec := post(t, srv, `{"jsonrpc":"2.0","method":"zeno_chainInfo","params":[1],"id":1}`)
	requireBody(t, rec, okResp("1", fmt.Sprintf(`{"domainId":1,"networkName":"test","chain_id":"%s","runtimeKind":"WASM","stfSpecHash":"%s","status":"OK","inconsistentAt":null,"profiles":[%s],"views":%s}`, testChainID(), fixtureSTF, profile, views)))

	dir2, cs2 := buildA(t)
	s2 := newFakeSettlement()
	s2.height = 10
	s2.accept(cs2[0], 20)
	s2.record.RuntimeKind = 1
	r2 := newTestChain(t, dir2, s2)
	doPoll(t, r2)
	srv2, _ := newReaderServer(t, r2.c)
	rec = post(t, srv2, `{"jsonrpc":"2.0","method":"zeno_chainInfo","params":[1],"id":1}`)
	requireBody(t, rec, okResp("1", fmt.Sprintf(`{"domainId":1,"networkName":"test","chain_id":null,"runtimeKind":"1","stfSpecHash":"%s","status":"OK","inconsistentAt":null,"profiles":[],"views":[]}`, fixtureSTF)))
}
