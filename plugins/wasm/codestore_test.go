package wasm

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/zenon-network/go-zenon/common/crypto"
	"github.com/zenon-network/go-zenon/common/types"

	"github.com/Zeno-L2/zeno-executor/domain"
	"github.com/Zeno-L2/zeno-executor/internal/codec"
	"github.com/Zeno-L2/zeno-executor/plugins/wasm/internal/module"
)

func TestCodeStorePutGetRoundTrip(t *testing.T) {
	store, err := newCodeStore(t.TempDir())
	if err != nil {
		t.Fatalf("newCodeStore: %v", err)
	}

	key := []byte("code-hash-and-chunk-index")
	value := []byte("chunk bytes")

	if err := store.put(entryDeployChunk, key, value); err != nil {
		t.Fatalf("put: %v", err)
	}

	got, ok, err := store.get(entryDeployChunk, key)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if !ok {
		t.Fatal("get: ok = false, want true")
	}
	if string(got) != string(value) {
		t.Fatalf("get: value = %q, want %q", got, value)
	}
}

func TestCodeStoreGetMissingIsAbsent(t *testing.T) {
	store, err := newCodeStore(t.TempDir())
	if err != nil {
		t.Fatalf("newCodeStore: %v", err)
	}

	_, ok, err := store.get(entryAssembledModule, []byte("never-written"))
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if ok {
		t.Fatal("get: ok = true for a never-written key, want false")
	}
}

func TestCodeStoreCorruptChecksumReadsAsAbsent(t *testing.T) {
	root := t.TempDir()
	store, err := newCodeStore(root)
	if err != nil {
		t.Fatalf("newCodeStore: %v", err)
	}

	key := []byte("k")
	value := []byte("v")
	if err := store.put(entryPendingRecord, key, value); err != nil {
		t.Fatalf("put: %v", err)
	}

	path := store.path(entryPendingRecord, key)
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read committed file: %v", err)
	}
	b[0] ^= 0xFF // flip a byte inside the payload, invalidating the trailing checksum
	if err := os.WriteFile(path, b, 0o644); err != nil {
		t.Fatalf("corrupt committed file: %v", err)
	}

	_, ok, err := store.get(entryPendingRecord, key)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if ok {
		t.Fatal("get: ok = true for a corrupt checksum, want false")
	}
}

func TestCodeStoreAtomicRenameSurvivesInterruptedWrite(t *testing.T) {
	root := t.TempDir()
	store, err := newCodeStore(root)
	if err != nil {
		t.Fatalf("newCodeStore: %v", err)
	}

	key := []byte("k")
	original := []byte("original value")
	if err := store.put(entryInstrumentedModule, key, original); err != nil {
		t.Fatalf("put: %v", err)
	}

	// Simulate a write interrupted before its rename: a stray temp file sits alongside the
	// committed entry. get must still return the last successfully committed value, never the
	// stray temp bytes, and the directory scan (a fresh newCodeStore) must not choke on it.
	dir := filepath.Join(root, entryInstrumentedModule.dirName())
	if err := os.WriteFile(filepath.Join(dir, ".tmp-stray"), []byte("half-written"), 0o644); err != nil {
		t.Fatalf("write stray temp file: %v", err)
	}

	reopened, err := newCodeStore(root)
	if err != nil {
		t.Fatalf("newCodeStore (reopen): %v", err)
	}
	got, ok, err := reopened.get(entryInstrumentedModule, key)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if !ok {
		t.Fatal("get: ok = false, want true (committed entry must survive a stray temp file)")
	}
	if string(got) != string(original) {
		t.Fatalf("get: value = %q, want %q", got, original)
	}
}

func TestNewCodeStoreRejectsEmptyPath(t *testing.T) {
	if _, err := newCodeStore(""); err == nil {
		t.Fatal("newCodeStore(\"\"): got nil error, want an error")
	}
}

func TestCodeStorePutOverwritesExistingEntry(t *testing.T) {
	store, err := newCodeStore(t.TempDir())
	if err != nil {
		t.Fatalf("newCodeStore: %v", err)
	}

	key := []byte("k")
	if err := store.put(entryDeployChunk, key, []byte("first value")); err != nil {
		t.Fatalf("put (first): %v", err)
	}
	if err := store.put(entryDeployChunk, key, []byte("second value")); err != nil {
		t.Fatalf("put (second): %v", err)
	}

	got, ok, err := store.get(entryDeployChunk, key)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if !ok {
		t.Fatal("get: ok = false, want true")
	}
	if string(got) != "second value" {
		t.Fatalf("get: value = %q, want %q (overwrite must replace, not append)", got, "second value")
	}
}

func TestCodeStorePutEmptyValueRoundTrips(t *testing.T) {
	store, err := newCodeStore(t.TempDir())
	if err != nil {
		t.Fatalf("newCodeStore: %v", err)
	}

	key := []byte("k")
	if err := store.put(entryAssembledModule, key, []byte{}); err != nil {
		t.Fatalf("put: %v", err)
	}

	got, ok, err := store.get(entryAssembledModule, key)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if !ok {
		t.Fatal("get: ok = false, want true for a present-but-empty value")
	}
	if len(got) != 0 {
		t.Fatalf("get: value = %q, want empty", got)
	}
}

func TestCodeStoreGetTooShortFileIsAbsent(t *testing.T) {
	root := t.TempDir()
	store, err := newCodeStore(root)
	if err != nil {
		t.Fatalf("newCodeStore: %v", err)
	}

	key := []byte("k")
	if err := store.put(entryPendingRecord, key, []byte("v")); err != nil {
		t.Fatalf("put: %v", err)
	}

	// Truncate the committed file below the 32-byte checksum size, simulating a write that was
	// interrupted before it ever wrote a full checksum.
	path := store.path(entryPendingRecord, key)
	if err := os.WriteFile(path, []byte{0x01, 0x02, 0x03}, 0o644); err != nil {
		t.Fatalf("truncate committed file: %v", err)
	}

	_, ok, err := store.get(entryPendingRecord, key)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if ok {
		t.Fatal("get: ok = true for a too-short file, want false")
	}
}

func noFetch(types.Hash) ([]byte, bool, error) { return nil, false, nil }

func hashKey(b byte) []byte {
	h := types.Hash{b}
	return h[:]
}

func memTierState(m *memTier) string {
	var names []string
	for _, h := range m.order {
		names = append(names, fmt.Sprintf("%d:%d", h[0], len(m.mods[h])))
	}
	return fmt.Sprintf("%v bytes=%d", names, m.bytes)
}

func TestMemTierLRU(t *testing.T) {
	m := newMemTier(noFetch, 10, 100)
	put := func(b byte, n int) {
		t.Helper()
		if err := m.put(entryInstrumentedModule, hashKey(b), bytes.Repeat([]byte{b}, n)); err != nil {
			t.Fatalf("put %d: %v", b, err)
		}
	}
	put(1, 4)
	put(2, 4)
	if got, want := memTierState(m), "[1:4 2:4] bytes=8"; got != want {
		t.Fatalf("after two puts: %s, want %s", got, want)
	}

	// A hit refreshes: 1 becomes the most recent, so 2 is evicted next.
	if v, ok, err := m.get(entryInstrumentedModule, hashKey(1)); err != nil || !ok || len(v) != 4 {
		t.Fatalf("get 1: %v %v %v", v, ok, err)
	}
	put(3, 4)
	if got, want := memTierState(m), "[1:4 3:4] bytes=8"; got != want {
		t.Fatalf("after eviction by bytes: %s, want %s", got, want)
	}
	if _, ok, _ := m.get(entryInstrumentedModule, hashKey(2)); ok {
		t.Fatal("2 survived eviction")
	}

	// Replacing a key adjusts the byte count and refreshes it.
	put(1, 2)
	if got, want := memTierState(m), "[3:4 1:2] bytes=6"; got != want {
		t.Fatalf("after replace: %s, want %s", got, want)
	}

	// One put can evict several.
	put(4, 9)
	if got, want := memTierState(m), "[4:9] bytes=9"; got != want {
		t.Fatalf("after a large put: %s, want %s", got, want)
	}

	// The entry bound evicts with room to spare in bytes.
	e := newMemTier(noFetch, 100, 2)
	for _, b := range []byte{1, 2, 3} {
		if err := e.put(entryInstrumentedModule, hashKey(b), []byte{b}); err != nil {
			t.Fatal(err)
		}
	}
	if got, want := memTierState(e), "[2:1 3:1] bytes=2"; got != want {
		t.Fatalf("entry bound: %s, want %s", got, want)
	}
}

func TestMemTierErrors(t *testing.T) {
	m := newMemTier(noFetch, 8, 4)
	if err := m.put(entryInstrumentedModule, hashKey(1), make([]byte, 9)); err == nil {
		t.Error("oversize put succeeded")
	}
	if len(m.mods) != 0 || m.bytes != 0 {
		t.Errorf("oversize put stored something: %s", memTierState(m))
	}
	short := make([]byte, 31)
	if err := m.put(entryInstrumentedModule, short, []byte{1}); err == nil {
		t.Error("put with a 31-byte key succeeded")
	}
	if _, _, err := m.get(entryInstrumentedModule, short); err == nil {
		t.Error("get with a 31-byte key succeeded")
	}
	if _, _, err := m.get(entryAssembledModule, short); err == nil {
		t.Error("assembled get with a 31-byte key succeeded")
	}
	for _, k := range []entryKind{entryDeployChunk, entryPendingRecord} {
		if _, _, err := m.get(k, hashKey(1)); err == nil || !strings.Contains(err.Error(), "simulate mode") {
			t.Errorf("get %s: %v", k.dirName(), err)
		}
		if err := m.put(k, hashKey(1), []byte{1}); err == nil || !strings.Contains(err.Error(), "simulate mode") {
			t.Errorf("put %s: %v", k.dirName(), err)
		}
	}
	if err := m.put(entryAssembledModule, hashKey(1), []byte{1}); err == nil || !strings.Contains(err.Error(), "simulate mode") {
		t.Errorf("assembled put: %v", err)
	}
	for _, k := range allEntryKinds {
		if err := m.delete(k, hashKey(1)); err == nil || !strings.Contains(err.Error(), "simulate mode") {
			t.Errorf("delete %s: %v", k.dirName(), err)
		}
	}
	boom := errors.New("boom")
	f := newMemTier(func(types.Hash) ([]byte, bool, error) { return nil, false, boom }, 8, 4)
	if _, _, err := f.get(entryAssembledModule, hashKey(1)); !errors.Is(err, boom) {
		t.Errorf("fetch error: %v", err)
	}
}

func TestMemTierAssembledNotKept(t *testing.T) {
	calls := 0
	m := newMemTier(func(types.Hash) ([]byte, bool, error) { calls++; return []byte{1}, true, nil }, 8, 4)
	for i := 0; i < 2; i++ {
		if v, ok, err := m.get(entryAssembledModule, hashKey(1)); err != nil || !ok || len(v) != 1 {
			t.Fatalf("get: %v %v %v", v, ok, err)
		}
	}
	if calls != 2 {
		t.Errorf("fetch called %d times, want 2", calls)
	}
	if len(m.mods) != 0 || m.bytes != 0 {
		t.Errorf("assembled bytes were kept: %s", memTierState(m))
	}
}

// simPlugin returns a simulate-mode plugin whose fetch serves modules
// by pre code hash and counts its calls.
func simPlugin(t *testing.T, modules map[types.Hash][]byte, calls *int) *Plugin {
	t.Helper()
	p, err := New(Config{NetworkName: "testnet", FetchModule: func(h types.Hash) ([]byte, bool, error) {
		*calls++
		m, ok := modules[h]
		return m, ok, nil
	}})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = p.Close() })
	return p
}

func TestSimulateModePlugin(t *testing.T) {
	f := buildSuccessFixture(t)
	modules := map[types.Hash][]byte{f.preCodeHash: f.raw}

	disk := newTestPlugin(t)
	if err := disk.store.put(entryAssembledModule, f.preCodeHash[:], f.raw); err != nil {
		t.Fatal(err)
	}
	if err := disk.store.put(entryInstrumentedModule, f.codeHash[:], f.instrumented); err != nil {
		t.Fatal(err)
	}
	want, err := disk.Apply(f.state(), f.input("CallL2"))
	if err != nil {
		t.Fatalf("disk Apply: %v", err)
	}

	calls := 0
	p := simPlugin(t, modules, &calls)
	for i := 0; i < 2; i++ {
		got, err := p.Apply(f.state(), f.input("CallL2"))
		if err != nil {
			t.Fatalf("Apply %d: %v", i, err)
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("Apply %d effects differ:\n got %+v\nwant %+v", i, got, want)
		}
	}
	if calls != 1 {
		t.Errorf("fetch called %d times over two Apply calls, want 1", calls)
	}

	tierEmpty := func(p *Plugin) bool {
		m := p.store.(*memTier)
		return len(m.mods) == 0 && m.bytes == 0
	}

	t.Run("wrong bytes", func(t *testing.T) {
		calls := 0
		p := simPlugin(t, map[types.Hash][]byte{f.preCodeHash: append([]byte{}, f.instrumented...)}, &calls)
		if _, err := p.Apply(f.state(), f.input("CallL2")); err == nil {
			t.Fatal("Apply accepted bytes that do not hash to the pre code hash")
		}
		if !tierEmpty(p) {
			t.Error("tier kept a module after a failed hash check")
		}
	})
	t.Run("wrong code hash leaf", func(t *testing.T) {
		calls := 0
		p := simPlugin(t, modules, &calls)
		st := f.state()
		st.values[derive(f.contractID, keyCodeHash)] = bytes.Repeat([]byte{0xEE}, 32)
		if _, err := p.Apply(st, f.input("CallL2")); err == nil {
			t.Fatal("Apply accepted a code hash leaf the module does not instrument to")
		}
		if !tierEmpty(p) {
			t.Error("tier kept a module after a failed code hash check")
		}
	})
	t.Run("not found", func(t *testing.T) {
		calls := 0
		p := simPlugin(t, nil, &calls)
		if _, err := p.Apply(f.state(), f.input("CallL2")); err == nil {
			t.Fatal("Apply succeeded with no module")
		}
	})
	t.Run("fetch error", func(t *testing.T) {
		p, err := New(Config{NetworkName: "testnet", FetchModule: func(types.Hash) ([]byte, bool, error) {
			return nil, false, errors.New("boom")
		}})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = p.Close() })
		if _, err := p.Apply(f.state(), f.input("CallL2")); err == nil {
			t.Fatal("Apply succeeded despite a fetch error")
		}
	})
}

// buildAltFixture is buildSuccessFixture with another contract and
// return data, so its module and code hash differ.
func buildAltFixture(t *testing.T) successFixture {
	t.Helper()
	contractID := types.Hash{8, 8, 8}
	decoded := codec.ContractEffects{
		Version:          codec.ExecutionResultVersion,
		GlobalInputIndex: 3,
		ReturnData:       []byte("alt"),
	}
	blob := encodeEffects(t, decoded)
	offset := uint32(1024)
	body := appendVarS64([]byte{0x42}, (int64(offset)<<32)|int64(len(blob)))
	body = append(body, 0x0B)
	raw := buildModule(&dataSeg{offset: offset, bytes: blob}, []byte{0x00}, body, 1)
	instrumented, err := module.Instrument(raw, moduleLimits())
	if err != nil {
		t.Fatalf("Instrument: %v", err)
	}
	return successFixture{contractID: contractID, globalInputIndex: 3, raw: raw, instrumented: instrumented,
		codeHash: CodeHash(instrumented), preCodeHash: CodeHash(raw), blob: blob, decoded: decoded}
}

func TestSimulateModeEviction(t *testing.T) {
	a, b := buildSuccessFixture(t), buildAltFixture(t)
	fetches := map[types.Hash]int{}
	p, err := New(Config{NetworkName: "testnet", FetchModule: func(h types.Hash) ([]byte, bool, error) {
		fetches[h]++
		switch h {
		case a.preCodeHash:
			return a.raw, true, nil
		case b.preCodeHash:
			return b.raw, true, nil
		}
		return nil, false, nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = p.Close() })
	n := max(len(a.instrumented), len(b.instrumented))
	if len(a.instrumented)+len(b.instrumented) <= n {
		t.Fatal("fixture sizes leave no room to evict")
	}
	p.store = newMemTier(p.cfg.FetchModule, n, simModuleCacheEntries)

	for _, f := range []successFixture{a, b, a} {
		if _, err := p.Apply(f.state(), f.input("CallL2")); err != nil {
			t.Fatalf("Apply: %v", err)
		}
	}
	if fetches[a.preCodeHash] != 2 || fetches[b.preCodeHash] != 1 {
		t.Errorf("fetches a=%d b=%d, want 2 and 1", fetches[a.preCodeHash], fetches[b.preCodeHash])
	}
}

func TestSimulateModeRefusals(t *testing.T) {
	calls := 0
	p := simPlugin(t, nil, &calls)

	if _, err := p.DecodeInput(domain.InputContext{}, encodeInput(t, codec.DeployStartArgs{CodeHash: types.Hash{2}, MetadataHash: types.Hash{3}, TotalSize: 100, ChunkCount: 1})); err == nil {
		t.Error("kind 2 DecodeInput succeeded")
	}
	chunk := []byte("chunk bytes")
	chunkArgs := codec.DeployChunkArgs{CodeHash: types.Hash{2}, ChunkIndex: 0, ChunkHash: hashOf(crypto.Hash(chunk))}
	if _, err := p.DecodeInput(domain.InputContext{SourceData: chunk}, encodeInput(t, chunkArgs)); err == nil {
		t.Error("kind 3 DecodeInput succeeded")
	}
	in, err := p.DecodeInput(domain.InputContext{}, encodeInput(t, codec.DeployFinalizeArgs{CodeHash: types.Hash{2}}))
	if err != nil {
		t.Fatalf("kind 4 DecodeInput: %v", err)
	}
	if _, err := p.Apply(&mapState{}, in); err == nil || !strings.Contains(err.Error(), "simulate mode") {
		t.Errorf("kind 4 Apply: %v", err)
	}

	if _, err := New(Config{NetworkName: "testnet", FetchModule: noFetch, CodeStorePath: t.TempDir()}); err == nil {
		t.Error("New accepted FetchModule with CodeStorePath")
	}
}

func TestCodeStoreList(t *testing.T) {
	store, err := newCodeStore(t.TempDir())
	if err != nil {
		t.Fatalf("newCodeStore: %v", err)
	}
	if got, err := store.list(entryConsumedDeploy); err != nil || len(got) != 0 {
		t.Fatalf("list of an empty kind = %q, err %v, want none", got, err)
	}

	for _, v := range []string{"one", "two", "corrupt"} {
		if err := store.put(entryConsumedDeploy, []byte(v), []byte(v)); err != nil {
			t.Fatalf("put %s: %v", v, err)
		}
	}
	corrupt := store.path(entryConsumedDeploy, []byte("corrupt"))
	b, err := os.ReadFile(corrupt)
	if err != nil {
		t.Fatal(err)
	}
	b[0] ^= 0xFF
	if err := os.WriteFile(corrupt, b, 0o644); err != nil {
		t.Fatal(err)
	}
	tmp := filepath.Join(store.root, entryConsumedDeploy.dirName(), ".tmp-123")
	if err := os.WriteFile(tmp, []byte("partial"), 0o644); err != nil {
		t.Fatal(err)
	}

	got, err := store.list(entryConsumedDeploy)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	var values []string
	for _, v := range got {
		values = append(values, string(v))
	}
	sort.Strings(values)
	if want := []string{"one", "two"}; !reflect.DeepEqual(values, want) {
		t.Fatalf("list = %q, want %q", values, want)
	}
}

func TestCodeStoreSyncsDirectoryOnPutAndDelete(t *testing.T) {
	s, err := newCodeStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	var synced []string
	orig := syncDir
	syncDir = func(dir string) error {
		synced = append(synced, dir)
		return orig(dir)
	}
	t.Cleanup(func() { syncDir = orig })

	want := filepath.Dir(s.path(entryConsumedDeploy, []byte("k")))
	if err := s.put(entryConsumedDeploy, []byte("k"), []byte("v")); err != nil {
		t.Fatal(err)
	}
	if len(synced) != 1 || synced[0] != want {
		t.Fatalf("put synced %v, want [%s]", synced, want)
	}
	if err := s.delete(entryConsumedDeploy, []byte("k")); err != nil {
		t.Fatal(err)
	}
	if len(synced) != 2 || synced[1] != want {
		t.Fatalf("delete synced %v, want a second sync of %s", synced, want)
	}
}

func TestCodeStoreDeleteOfAbsentEntrySyncsDirectory(t *testing.T) {
	s, err := newCodeStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	var synced []string
	orig := syncDir
	syncDir = func(dir string) error {
		synced = append(synced, dir)
		return orig(dir)
	}
	t.Cleanup(func() { syncDir = orig })

	want := filepath.Dir(s.path(entryConsumedDeploy, []byte("absent")))
	if err := s.delete(entryConsumedDeploy, []byte("absent")); err != nil {
		t.Fatal(err)
	}
	if len(synced) != 1 || synced[0] != want {
		t.Fatalf("delete of an absent entry synced %v, want [%s]", synced, want)
	}
}

func TestClearDeployStagingSyncsEachDirectoryOnce(t *testing.T) {
	s, err := newCodeStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	var synced []string
	var markPath string
	var clearing bool
	orig := syncDir
	syncDir = func(dir string) error {
		synced = append(synced, dir)
		if clearing && dir != filepath.Join(s.root, entryConsumedDeploy.dirName()) {
			if _, err := os.Stat(markPath); err != nil {
				t.Errorf("mark already unlinked when %s was synced: %v", dir, err)
			}
		}
		return orig(dir)
	}
	t.Cleanup(func() { syncDir = orig })

	var deployer types.Address
	var codeHash types.Hash
	deployer[0], codeHash[0] = 1, 2
	markPath = s.path(entryConsumedDeploy, pendingRecordKey(deployer, codeHash))
	if err := s.put(entryDeployChunk, deployChunkKey(deployer, codeHash, 0), []byte("c")); err != nil {
		t.Fatal(err)
	}
	if err := s.put(entryConsumedDeploy, pendingRecordKey(deployer, codeHash), []byte("m")); err != nil {
		t.Fatal(err)
	}
	synced = nil
	clearing = true
	if err := clearDeployStaging(s, deployer, codeHash); err != nil {
		t.Fatal(err)
	}
	want := map[string]bool{}
	for _, k := range []entryKind{entryDeployChunk, entryPendingRecord, entryConsumedDeploy} {
		want[filepath.Join(s.root, k.dirName())] = true
	}
	if len(synced) != len(want) {
		t.Fatalf("clear synced %d times (%v), want once per directory (%d)", len(synced), synced, len(want))
	}
	for _, d := range synced {
		if !want[d] {
			t.Fatalf("clear synced unexpected directory %s", d)
		}
		delete(want, d)
	}
	if last := synced[len(synced)-1]; last != filepath.Join(s.root, entryConsumedDeploy.dirName()) {
		t.Fatalf("clear synced %v, want the consumed-deploy directory last", synced)
	}
	if _, ok, _ := s.get(entryDeployChunk, deployChunkKey(deployer, codeHash, 0)); ok {
		t.Fatal("chunk survived the clear")
	}
}

func TestNewCodeStoreSyncsCreatedDirectories(t *testing.T) {
	var synced []string
	orig := syncDir
	syncDir = func(dir string) error {
		synced = append(synced, dir)
		return orig(dir)
	}
	t.Cleanup(func() { syncDir = orig })

	parent := t.TempDir()
	root := filepath.Join(parent, "wasm-code")
	if _, err := newCodeStore(root); err != nil {
		t.Fatal(err)
	}
	got := map[string]bool{}
	for _, d := range synced {
		got[d] = true
	}
	want := []string{root, parent}
	for _, k := range allEntryKinds {
		want = append(want, filepath.Join(root, k.dirName()))
	}
	for _, d := range want {
		if !got[d] {
			t.Errorf("newCodeStore did not sync %s (synced %v)", d, synced)
		}
	}
}

func TestCodeStoreReturnsSyncErrors(t *testing.T) {
	root := t.TempDir()
	s, err := newCodeStore(root)
	if err != nil {
		t.Fatal(err)
	}
	orig := syncDir
	syncErr := errors.New("sync failed")
	syncDir = func(string) error { return syncErr }
	t.Cleanup(func() { syncDir = orig })

	if err := s.put(entryConsumedDeploy, []byte("k"), []byte("v")); !errors.Is(err, syncErr) {
		t.Fatalf("put error = %v, want %v", err, syncErr)
	}
	if err := s.delete(entryConsumedDeploy, []byte("k")); !errors.Is(err, syncErr) {
		t.Fatalf("delete error = %v, want %v", err, syncErr)
	}
	if _, err := newCodeStore(root); !errors.Is(err, syncErr) {
		t.Fatalf("newCodeStore error = %v, want %v", err, syncErr)
	}
}
