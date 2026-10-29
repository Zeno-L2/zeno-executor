package reader

import (
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"slices"
	"strings"
	"testing"

	"github.com/zenon-network/go-zenon/common/crypto"
	"github.com/zenon-network/go-zenon/common/types"
	"github.com/zenon-network/go-zenon/vm/constants"

	"github.com/Zeno-L2/zeno-executor/domain"
	"github.com/Zeno-L2/zeno-executor/internal/codec"
	"github.com/Zeno-L2/zeno-executor/plugins/wasm/internal/keys"
)

// fakeContext is a domain.ReadContext over inputs in ascending global
// index order, a leaf map and a chunk map.
type fakeContext struct {
	inputs []domain.IndexedInput
	leaves map[domain.Path][]byte
	chunks map[uint64][]byte
	reads  []domain.Path
	// ownerErr is returned by LastOwnerInput when set.
	ownerErr error
}

func newFake() *fakeContext {
	return &fakeContext{leaves: map[domain.Path][]byte{}, chunks: map[uint64][]byte{}}
}

func h32(b ...[]byte) types.Hash {
	var h types.Hash
	copy(h[:], crypto.Hash(b...))
	return h
}

func (f *fakeContext) Read(p domain.Path) ([]byte, bool, error) {
	f.reads = append(f.reads, p)
	v, ok := f.leaves[p]
	return v, ok, nil
}

func (f *fakeContext) visit(match func(domain.IndexedInput) bool, before uint64, fn func(domain.IndexedInput) (bool, error)) error {
	for i := len(f.inputs) - 1; i >= 0; i-- {
		in := f.inputs[i]
		if in.GlobalInputIndex >= before || !match(in) {
			continue
		}
		if more, err := fn(in); err != nil || !more {
			return err
		}
	}
	return nil
}

func (f *fakeContext) InputsBySender(sender types.Address, before uint64, fn func(domain.IndexedInput) (bool, error)) error {
	return f.visit(func(in domain.IndexedInput) bool { return in.Sender == sender }, before, fn)
}

func (f *fakeContext) Deploys(deployer types.Address, before uint64, fn func(domain.IndexedInput) (bool, error)) error {
	return f.visit(func(in domain.IndexedInput) bool {
		return in.Sender == deployer && in.InputData[0] == byte(codec.DeployFinalize) && in.Receipt.Status == codec.StatusSuccess
	}, before, fn)
}

func (f *fakeContext) LastOwnerInput(contract types.Hash) (domain.IndexedInput, bool, error) {
	if f.ownerErr != nil {
		return domain.IndexedInput{}, false, f.ownerErr
	}
	var out domain.IndexedInput
	found := false
	_ = f.visit(func(in domain.IndexedInput) bool {
		k := in.InputData[0]
		return (k == byte(codec.DeployFinalize) || k == byte(codec.TransferOwnership)) && in.Receipt.Status == codec.StatusSuccess && in.Receipt.ContractID == contract
	}, math.MaxUint64, func(in domain.IndexedInput) (bool, error) {
		out, found = in, true
		return false, nil
	})
	return out, found, nil
}

func (f *fakeContext) FirstCodeInput(h types.Hash) (domain.IndexedInput, bool, error) {
	for _, in := range f.inputs {
		if in.Receipt.Status != codec.StatusSuccess {
			continue
		}
		dec, err := codec.DecodeInput(in.InputData)
		if err != nil {
			continue
		}
		switch a := dec.Args.(type) {
		case codec.DeployFinalizeArgs:
			if a.CodeHash == h {
				return in, true, nil
			}
		case codec.UpgradeArgs:
			if a.NewCodeHash == h {
				return in, true, nil
			}
		}
	}
	return domain.IndexedInput{}, false, nil
}

func (f *fakeContext) ChunkData(g uint64) ([]byte, error) {
	d, ok := f.chunks[g]
	if !ok {
		return nil, fmt.Errorf("no chunk for input %d", g)
	}
	return d, nil
}

// add appends an input and returns its global index.
func (f *fakeContext) add(sender types.Address, args codec.InputArgs, status codec.Status, contract types.Hash) uint64 {
	g := uint64(len(f.inputs)) + 1
	data, err := codec.Input{SourceAccountBlockHash: h32([]byte{byte(g)}), Args: args}.Encode()
	if err != nil {
		panic(err)
	}
	f.inputs = append(f.inputs, domain.IndexedInput{
		GlobalInputIndex: g,
		BatchID:          g / 4,
		SendHash:         h32([]byte{byte(g), 1}),
		Sender:           sender,
		InputData:        data,
		Receipt:          codec.Receipt{Status: status, ContractID: contract},
	})
	return g
}

func (f *fakeContext) start(sender types.Address, code types.Hash, total uint64, count uint32, owner types.Address) uint64 {
	meta := codec.ContractMetadata{Version: codec.ContractMetadataVersion, UpgradePolicy: codec.PolicyOwnerUpgradeable, Owner: owner}.Encode()
	return f.add(sender, codec.DeployStartArgs{CodeHash: code, MetadataHash: h32(meta), TotalSize: total, ChunkCount: count, Metadata: meta}, codec.StatusSuccess, types.Hash{})
}

func (f *fakeContext) chunk(sender types.Address, code types.Hash, idx uint32, data []byte) uint64 {
	g := f.add(sender, codec.DeployChunkArgs{CodeHash: code, ChunkIndex: idx, ChunkHash: h32(data)}, codec.StatusSuccess, types.Hash{})
	f.chunks[g] = data
	return g
}

func (f *fakeContext) finalize(sender types.Address, code, contract types.Hash) uint64 {
	return f.add(sender, codec.DeployFinalizeArgs{CodeHash: code}, codec.StatusSuccess, contract)
}

func (f *fakeContext) transfer(sender types.Address, contract types.Hash, to types.Address) uint64 {
	return f.add(sender, codec.TransferOwnershipArgs{ContractID: contract, NewOwner: to}, codec.StatusSuccess, contract)
}

func (f *fakeContext) setLeaf(contract types.Hash, key, v []byte) {
	f.leaves[keys.Derive(contract, key)] = v
}

func (f *fakeContext) setOwner(contract types.Hash, owner types.Address) {
	c := keys.Caller(owner)
	f.setLeaf(contract, keys.Owner, c[:])
}

func view(t *testing.T, name string) domain.View {
	t.Helper()
	for _, v := range New("test").Views() {
		if v.Name == name {
			return v
		}
	}
	t.Fatalf("no view %s", name)
	return domain.View{}
}

func addr(b byte) types.Address { return types.Address{0xA0, b} }

func contractOf(t *testing.T, f *fakeContext, id types.Hash) (contractJSON, error) {
	t.Helper()
	res, err := view(t, "contract").Read(f, map[string]any{"contractId": id})
	if err != nil {
		return contractJSON{}, err
	}
	return res.(contractJSON), nil
}

func TestDescribeInput(t *testing.T) {
	src := types.Hash{0x51}
	target := types.Hash{0xC0}
	asset := codec.AssetID{31: 7}
	meta := []byte{1, 2, 3}
	owner := addr(9)
	head := func(name string, kind int) string {
		return fmt.Sprintf(`{"kind":%q,"inputKind":%d,"sourceAccountBlockHash":%q`, name, kind, src)
	}
	assetHex := hex.EncodeToString(asset[:])
	cases := []struct {
		args codec.InputArgs
		want string
	}{
		{codec.CallL2Args{TargetContract: target, PlasmaLimit: 5000, DepositAsset: asset, DepositAmount: codec.U256{31: 5}, Payload: []byte{0xAB, 0xCD}},
			head("CallL2", 0) + fmt.Sprintf(`,"targetContract":%q,"plasmaLimit":5000,"depositAsset":%q,"depositAmount":"5","payload":"abcd"}`, target, assetHex)},
		{codec.DepositArgs{TargetContract: target, PlasmaLimit: 7, DepositAsset: asset, DepositAmount: codec.U256{30: 1}},
			head("Deposit", 1) + fmt.Sprintf(`,"targetContract":%q,"plasmaLimit":7,"depositAsset":%q,"depositAmount":"256"}`, target, assetHex)},
		{codec.DeployStartArgs{CodeHash: types.Hash{1}, MetadataHash: types.Hash{2}, TotalSize: 40000, ChunkCount: 3, Metadata: meta},
			head("DeployContractStart", 2) + fmt.Sprintf(`,"codeHash":%q,"metadataHash":%q,"totalSize":40000,"chunkCount":3,"metadata":"010203"}`, types.Hash{1}, types.Hash{2})},
		{codec.DeployChunkArgs{CodeHash: types.Hash{1}, ChunkIndex: 2, ChunkHash: types.Hash{3}},
			head("DeployContractChunk", 3) + fmt.Sprintf(`,"codeHash":%q,"chunkIndex":2,"chunkHash":%q}`, types.Hash{1}, types.Hash{3})},
		{codec.DeployFinalizeArgs{CodeHash: types.Hash{1}},
			head("DeployContractFinalize", 4) + fmt.Sprintf(`,"codeHash":%q}`, types.Hash{1})},
		{codec.UpgradeArgs{ContractID: types.Hash{4}, NewCodeHash: types.Hash{5}},
			head("UpgradeContract", 5) + fmt.Sprintf(`,"contractId":%q,"newCodeHash":%q}`, types.Hash{4}, types.Hash{5})},
		{codec.TransferOwnershipArgs{ContractID: types.Hash{4}, NewOwner: owner},
			head("TransferContractOwnership", 6) + fmt.Sprintf(`,"contractId":%q,"newOwner":%q}`, types.Hash{4}, owner)},
		{codec.BuyStorageArgs{ContractID: types.Hash{4}, Bytes: 1 << 40},
			head("BuyStorage", 7) + fmt.Sprintf(`,"contractId":%q,"bytes":1099511627776}`, types.Hash{4})},
	}
	r := New("test")
	for _, c := range cases {
		data, err := codec.Input{SourceAccountBlockHash: src, Args: c.args}.Encode()
		if err != nil {
			t.Fatal(err)
		}
		got, err := r.DescribeInput(data)
		if err != nil {
			t.Fatalf("kind %d: %v", c.args.Kind(), err)
		}
		enc, err := json.Marshal(got)
		if err != nil {
			t.Fatal(err)
		}
		if string(enc) != c.want {
			t.Errorf("kind %d:\n got %s\nwant %s", c.args.Kind(), enc, c.want)
		}
	}
	bad := append([]byte{8}, src[:]...)
	if _, err := r.DescribeInput(bad); !errors.Is(err, codec.ErrInvalidKind) {
		t.Fatalf("kind 8: err = %v, want ErrInvalidKind", err)
	}
	if _, err := r.DescribeInput([]byte{0}); err == nil {
		t.Fatal("a truncated input decoded")
	}
}

func TestChainIDAndProfiles(t *testing.T) {
	r := New("test")
	want := h32([]byte("ZENON_WASM_L2:"), []byte("test"))
	if r.ChainID() != any(want) {
		t.Fatalf("chain id = %v, want %v", r.ChainID(), want)
	}
	if p := r.Profiles(); len(p) != 1 || p[0].Name != "contracts/1" || p[0].Features["nativeAddresses"] || !p[0].Features["deposits"] {
		t.Fatalf("profiles = %+v", p)
	}
	var names []string
	for _, v := range r.Views() {
		names = append(names, v.Name)
	}
	if strings.Join(names, ",") != "contract,storage,contractsByDeployer,module" {
		t.Fatalf("views = %v", names)
	}
}

func TestContractView(t *testing.T) {
	id := types.Hash{0xC1}
	t.Run("read order", func(t *testing.T) {
		f := newFake()
		if _, err := contractOf(t, f, id); err != nil {
			t.Fatal(err)
		}
		var want []domain.Path
		for _, k := range [][]byte{keys.CodeHash, keys.PreCodeHash, keys.UpgradePolicy, keys.Owner, keys.Quota} {
			want = append(want, keys.Derive(id, k))
		}
		if !slices.Equal(f.reads, want) {
			t.Fatalf("reads = %x, want %x", f.reads, want)
		}
	})
	t.Run("undeployed", func(t *testing.T) {
		res, err := contractOf(t, newFake(), id)
		if err != nil {
			t.Fatal(err)
		}
		enc, _ := json.Marshal(res)
		const want = `{"codeHash":null,"sourceCodeHash":null,"upgradePolicy":null,"owner":null,"ownerCaller":null,"quota":null,"nativeAddress":null}`
		if string(enc) != want {
			t.Fatalf("got %s", enc)
		}
	})
	t.Run("quota", func(t *testing.T) {
		f := newFake()
		code, pre := types.Hash{1}, types.Hash{2}
		f.setLeaf(id, keys.CodeHash, code[:])
		f.setLeaf(id, keys.PreCodeHash, pre[:])
		f.setLeaf(id, keys.UpgradePolicy, []byte{0})
		res, err := contractOf(t, f, id)
		if err != nil {
			t.Fatal(err)
		}
		if res.Quota == nil || *res.Quota != (quotaJSON{}) {
			t.Fatalf("absent quota leaf: %+v", res.Quota)
		}
		q := binary.BigEndian.AppendUint64(binary.BigEndian.AppendUint64(nil, 1<<40), 77)
		f.setLeaf(id, keys.Quota, q)
		res, err = contractOf(t, f, id)
		if err != nil {
			t.Fatal(err)
		}
		enc, _ := json.Marshal(res)
		want := fmt.Sprintf(`{"codeHash":%q,"sourceCodeHash":%q,"upgradePolicy":"IMMUTABLE","owner":null,"ownerCaller":null,"quota":{"purchased":1099511627776,"used":77},"nativeAddress":null}`, code, pre)
		if string(enc) != want {
			t.Fatalf("got %s\nwant %s", enc, want)
		}
	})
	t.Run("wrong width", func(t *testing.T) {
		for _, c := range []struct {
			name string
			key  []byte
			v    []byte
		}{
			{"CodeHash", keys.CodeHash, make([]byte, 31)},
			{"PreCodeHash", keys.PreCodeHash, make([]byte, 33)},
			{"UpgradePolicy", keys.UpgradePolicy, []byte{0, 0}},
			{"UpgradePolicy value", keys.UpgradePolicy, []byte{2}},
			{"Owner", keys.Owner, make([]byte, 20)},
			{"Quota", keys.Quota, make([]byte, 15)},
		} {
			f := newFake()
			f.setLeaf(id, c.key, c.v)
			if _, err := contractOf(t, f, id); err == nil || !strings.Contains(err.Error(), id.String()) {
				t.Errorf("%s: err = %v, want an error naming the contract", c.name, err)
			}
		}
	})
}

func TestOwner(t *testing.T) {
	code := types.Hash{0xAA}
	d, o1, o2, n := addr(1), addr(2), addr(3), addr(4)
	c1, c2 := types.Hash{0xC1}, types.Hash{0xC2}

	t.Run("transfer is newest", func(t *testing.T) {
		f := newFake()
		f.start(d, code, 4, 1, o1)
		f.chunk(d, code, 0, []byte("abcd"))
		f.finalize(d, code, c1)
		f.transfer(o1, c1, n)
		f.setOwner(c1, n)
		got, err := ownerOf(f, c1, keys.Caller(n))
		if err != nil || got != n {
			t.Fatalf("owner = %v, %v; want %v", got, err, n)
		}
	})
	t.Run("two starts before one finalize", func(t *testing.T) {
		f := newFake()
		f.start(d, code, 4, 1, o1)
		f.start(d, code, 4, 1, o2)
		f.chunk(d, code, 0, []byte("abcd"))
		f.finalize(d, code, c1)
		got, err := ownerOf(f, c1, keys.Caller(o2))
		if err != nil || got != o2 {
			t.Fatalf("owner = %v, %v; want %v", got, err, o2)
		}
	})
	t.Run("redeploy of one code hash", func(t *testing.T) {
		f := newFake()
		f.start(d, code, 4, 1, o1)
		f.chunk(d, code, 0, []byte("abcd"))
		f.finalize(d, code, c1)
		f.start(d, code, 4, 1, o2)
		f.chunk(d, code, 0, []byte("abcd"))
		f.finalize(d, code, c2)
		if got, err := ownerOf(f, c1, keys.Caller(o1)); err != nil || got != o1 {
			t.Fatalf("first contract: owner = %v, %v; want %v", got, err, o1)
		}
		if got, err := ownerOf(f, c2, keys.Caller(o2)); err != nil || got != o2 {
			t.Fatalf("second contract: owner = %v, %v; want %v", got, err, o2)
		}
	})
	t.Run("failed caller check", func(t *testing.T) {
		f := newFake()
		f.start(d, code, 4, 1, o1)
		f.finalize(d, code, c1)
		_, err := ownerOf(f, c1, keys.Caller(o2))
		if err == nil || !strings.Contains(err.Error(), c1.String()) {
			t.Fatalf("err = %v, want an error naming the contract", err)
		}
		if errors.Is(err, domain.ErrInvalidParams) || errors.Is(err, domain.ErrNotFound) {
			t.Fatalf("err wraps a sentinel: %v", err)
		}
	})
	t.Run("missing start", func(t *testing.T) {
		f := newFake()
		f.finalize(d, code, c1)
		if _, err := ownerOf(f, c1, keys.Caller(o1)); err == nil || !strings.Contains(err.Error(), "no DeployContractStart") {
			t.Fatalf("err = %v", err)
		}
	})
	t.Run("no owner input", func(t *testing.T) {
		f := newFake()
		if _, err := ownerOf(f, c1, keys.Caller(o1)); err == nil || !strings.Contains(err.Error(), c1.String()) {
			t.Fatalf("err = %v", err)
		}
	})
	t.Run("undecodable metadata", func(t *testing.T) {
		f := newFake()
		f.add(d, codec.DeployStartArgs{CodeHash: code, TotalSize: 4, ChunkCount: 1, Metadata: []byte{9}}, codec.StatusSuccess, types.Hash{})
		f.finalize(d, code, c1)
		if _, err := ownerOf(f, c1, keys.Caller(o1)); err == nil {
			t.Fatal("undecodable metadata gave an owner")
		}
	})
	t.Run("context error passes through", func(t *testing.T) {
		f := newFake()
		f.ownerErr = &ctxErr{"budget"}
		_, err := ownerOf(f, c1, keys.Caller(o1))
		var ce *ctxErr
		if !errors.As(err, &ce) {
			t.Fatalf("err = %v, want the context error", err)
		}
	})
}

type ctxErr struct{ msg string }

func (e *ctxErr) Error() string { return e.msg }

// moduleOf reads the module view for h.
func moduleOf(t *testing.T, f *fakeContext, h types.Hash) ([]byte, error) {
	t.Helper()
	res, err := view(t, "module").Read(f, map[string]any{"sourceCodeHash": h})
	if err != nil {
		return nil, err
	}
	m := res.(moduleJSON)
	if m.SourceCodeHash != h {
		t.Fatalf("sourceCodeHash = %v, want %v", m.SourceCodeHash, h)
	}
	return m.Module, nil
}

func TestModuleView(t *testing.T) {
	d, u := addr(1), addr(2)
	parts := [][]byte{[]byte("alpha-"), []byte("beta-"), []byte("gamma")}
	full := []byte("alpha-beta-gamma")
	code := h32(full)

	stage := func(f *fakeContext, sender types.Address, order ...int) {
		f.start(sender, code, uint64(len(full)), 3, sender)
		for _, i := range order {
			f.chunk(sender, code, uint32(i), parts[i])
		}
	}

	t.Run("out of order", func(t *testing.T) {
		f := newFake()
		stage(f, d, 2, 0, 1)
		f.finalize(d, code, types.Hash{0xC1})
		got, err := moduleOf(t, f, code)
		if err != nil || string(got) != string(full) {
			t.Fatalf("module = %q, %v", got, err)
		}
	})
	t.Run("re-sent index takes the newest", func(t *testing.T) {
		f := newFake()
		f.start(d, code, uint64(len(full)), 3, d)
		f.chunk(d, code, 0, parts[0])
		f.chunk(d, code, 1, []byte("WRONG"))
		f.chunk(d, code, 1, parts[1])
		f.chunk(d, code, 2, parts[2])
		f.finalize(d, code, types.Hash{0xC1})
		got, err := moduleOf(t, f, code)
		if err != nil || string(got) != string(full) {
			t.Fatalf("module = %q, %v", got, err)
		}
	})
	t.Run("chunk before the newest start is ignored", func(t *testing.T) {
		f := newFake()
		f.chunk(d, code, 1, parts[1])
		f.start(d, code, uint64(len(full)), 3, d)
		f.chunk(d, code, 0, parts[0])
		f.chunk(d, code, 2, parts[2])
		f.finalize(d, code, types.Hash{0xC1})
		if _, err := moduleOf(t, f, code); err == nil || !strings.Contains(err.Error(), "chunk 1") {
			t.Fatalf("err = %v, want chunk 1 missing", err)
		}
	})
	t.Run("interleaved staging from one sender", func(t *testing.T) {
		fullB := []byte("bravo-one-bravo-two")
		codeB := h32(fullB)
		f := newFake()
		f.start(d, code, uint64(len(full)), 3, d)
		f.start(d, codeB, uint64(len(fullB)), 2, d)
		f.chunk(d, code, 0, parts[0])
		f.chunk(d, codeB, 0, fullB[:10])
		f.chunk(d, code, 1, parts[1])
		f.chunk(d, codeB, 1, fullB[10:])
		f.chunk(d, code, 2, parts[2])
		f.finalize(d, code, types.Hash{0xC1})
		f.finalize(d, codeB, types.Hash{0xC2})
		if got, err := moduleOf(t, f, code); err != nil || string(got) != string(full) {
			t.Fatalf("module A = %q, %v", got, err)
		}
		if got, err := moduleOf(t, f, codeB); err != nil || string(got) != string(fullB) {
			t.Fatalf("module B = %q, %v", got, err)
		}
	})
	t.Run("chunk at chunk_count is ignored", func(t *testing.T) {
		f := newFake()
		stage(f, d, 0, 1, 2)
		f.chunk(d, code, 3, []byte("extra"))
		f.finalize(d, code, types.Hash{0xC1})
		if got, err := moduleOf(t, f, code); err != nil || string(got) != string(full) {
			t.Fatalf("module = %q, %v", got, err)
		}
	})
	t.Run("an upgrader's chunks", func(t *testing.T) {
		f := newFake()
		stage(f, u, 0, 1, 2)
		f.add(u, codec.UpgradeArgs{ContractID: types.Hash{0xC1}, NewCodeHash: code}, codec.StatusSuccess, types.Hash{0xC1})
		if got, err := moduleOf(t, f, code); err != nil || string(got) != string(full) {
			t.Fatalf("module = %q, %v", got, err)
		}
	})
	t.Run("a failed finalize is not a deploy", func(t *testing.T) {
		f := newFake()
		stage(f, d, 0, 1, 2)
		f.add(d, codec.DeployFinalizeArgs{CodeHash: code}, codec.StatusValidationFailed, types.Hash{0xC1})
		if _, err := moduleOf(t, f, code); !errors.Is(err, domain.ErrNotFound) {
			t.Fatalf("err = %v, want ErrNotFound", err)
		}
	})
	t.Run("failures", func(t *testing.T) {
		missing := newFake()
		stage(missing, d, 0, 2)
		missing.finalize(d, code, types.Hash{0xC1})

		mismatch := newFake()
		mismatch.start(d, code, uint64(len(full)), 3, d)
		mismatch.chunk(d, code, 0, parts[0])
		mismatch.chunk(d, code, 1, parts[1])
		mismatch.chunk(d, code, 2, []byte("GAMMA"))
		mismatch.finalize(d, code, types.Hash{0xC1})

		long := newFake()
		long.start(d, code, 3, 3, d)
		for i := range parts {
			long.chunk(d, code, uint32(i), parts[i])
		}
		long.finalize(d, code, types.Hash{0xC1})

		count := newFake()
		count.start(d, code, 100, constants.MaxChunkCount+1, d)
		count.finalize(d, code, types.Hash{0xC1})

		zero := newFake()
		zero.start(d, code, 100, 0, d)
		zero.finalize(d, code, types.Hash{0xC1})

		size := newFake()
		size.start(d, code, constants.MaxCodeSize+1, 3, d)
		size.finalize(d, code, types.Hash{0xC1})

		nostart := newFake()
		nostart.finalize(d, code, types.Hash{0xC1})

		for name, f := range map[string]*fakeContext{"missing index": missing, "hash mismatch": mismatch, "longer than total_size": long, "chunk_count too high": count, "chunk_count zero": zero, "total_size too high": size, "no start": nostart} {
			_, err := moduleOf(t, f, code)
			if err == nil || !strings.Contains(err.Error(), code.String()) && name != "no start" {
				t.Errorf("%s: err = %v, want an error naming the code hash", name, err)
			}
			if errors.Is(err, domain.ErrNotFound) || errors.Is(err, domain.ErrInvalidParams) {
				t.Errorf("%s: err wraps a sentinel: %v", name, err)
			}
		}
		_, err := moduleOf(t, newFake(), code)
		if !errors.Is(err, domain.ErrNotFound) || !strings.Contains(err.Error(), code.String()) {
			t.Fatalf("not found: err = %v", err)
		}
	})
}

func TestStorageView(t *testing.T) {
	f := newFake()
	id := types.Hash{0xC1}
	f.setLeaf(id, []byte("k"), []byte{})
	f.setLeaf(id, []byte("v"), []byte{0xAB})
	read := func(key string) string {
		res, err := view(t, "storage").Read(f, map[string]any{"contractId": id, "key": []byte(key)})
		if err != nil {
			t.Fatal(err)
		}
		enc, _ := json.Marshal(res)
		return string(enc)
	}
	if got := read("k"); got != `{"present":true,"value":""}` {
		t.Fatalf("present-empty: %s", got)
	}
	if got := read("v"); got != `{"present":true,"value":"ab"}` {
		t.Fatalf("present: %s", got)
	}
	if got := read("zz"); got != `{"present":false,"value":null}` {
		t.Fatalf("absent: %s", got)
	}
}

func TestContractsByDeployer(t *testing.T) {
	d := addr(1)
	f := newFake()
	var gs []uint64
	for i := 0; i < 101; i++ {
		gs = append(gs, f.finalize(d, types.Hash{byte(i)}, types.Hash{0xC0, byte(i)}))
	}
	f.finalize(addr(2), types.Hash{0xFF}, types.Hash{0xCF})
	read := func(args map[string]any) (deployerJSON, error) {
		res, err := view(t, "contractsByDeployer").Read(f, args)
		if err != nil {
			return deployerJSON{}, err
		}
		return res.(deployerJSON), nil
	}
	p1, err := read(map[string]any{"address": d})
	if err != nil {
		t.Fatal(err)
	}
	if len(p1.Contracts) != 100 || p1.Contracts[0].GlobalInputIndex != gs[100] || p1.Contracts[99].GlobalInputIndex != gs[1] {
		t.Fatalf("page 1: %d contracts, first %d, last %d", len(p1.Contracts), p1.Contracts[0].GlobalInputIndex, p1.Contracts[99].GlobalInputIndex)
	}
	if p1.Contracts[0].ContractID != (types.Hash{0xC0, 100}) {
		t.Fatalf("contractId = %v", p1.Contracts[0].ContractID)
	}
	if p1.NextCursor == nil || binary.BigEndian.Uint64(*p1.NextCursor) != gs[1] {
		t.Fatalf("nextCursor = %v", p1.NextCursor)
	}
	p2, err := read(map[string]any{"address": d, "cursor": []byte(*p1.NextCursor)})
	if err != nil {
		t.Fatal(err)
	}
	if len(p2.Contracts) != 1 || p2.Contracts[0].GlobalInputIndex != gs[0] || p2.NextCursor != nil {
		t.Fatalf("page 2 = %+v", p2)
	}
	if _, err := read(map[string]any{"address": d, "cursor": make([]byte, 7)}); !errors.Is(err, domain.ErrInvalidParams) {
		t.Fatalf("7-byte cursor: err = %v, want ErrInvalidParams", err)
	}
	none, err := read(map[string]any{"address": addr(3)})
	if err != nil {
		t.Fatal(err)
	}
	if enc, _ := json.Marshal(none); string(enc) != `{"contracts":[],"nextCursor":null}` {
		t.Fatalf("no deploys: %s", enc)
	}
}

func TestCode(t *testing.T) {
	d := addr(1)
	full := []byte("alpha-beta-gamma")
	code := h32(full)
	f := newFake()
	f.start(d, code, uint64(len(full)), 2, d)
	f.chunk(d, code, 0, full[:6])
	f.chunk(d, code, 1, full[6:])
	f.finalize(d, code, types.Hash{0xC1})

	var src domain.CodeSource = New("testnet")
	got, err := src.Code(f, code)
	if err != nil || string(got) != string(full) {
		t.Fatalf("Code = %q, %v", got, err)
	}
	want, err := moduleOf(t, f, code)
	if err != nil || string(got) != string(want) {
		t.Fatalf("Code differs from the module view: %q vs %q, %v", got, want, err)
	}
	if _, err := src.Code(f, types.Hash{0xFF}); !errors.Is(err, domain.ErrNotFound) {
		t.Fatalf("unknown hash: err = %v, want ErrNotFound", err)
	}
}
