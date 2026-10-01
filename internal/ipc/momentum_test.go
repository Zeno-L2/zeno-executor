package ipc

import (
	"bytes"
	"errors"
	"runtime"
	"testing"

	"github.com/zenon-network/go-zenon/chain/nom"
	"github.com/zenon-network/go-zenon/common/types"

	"github.com/digitalSloth/go-executor/internal/ipc/ipctest"
)

func addr(b byte) types.Address {
	var a types.Address
	a[0] = b
	return a
}

func testChain() []*nom.DetailedMomentum {
	return ipctest.Chain([][]ipctest.Block{
		{{Address: addr(1), Data: []byte("h1-a1")}, {Address: addr(2), Data: []byte("h1-a2")}},
		{{Address: addr(1), Data: []byte("h2-a1-1")}, {Address: addr(1), Data: []byte("h2-a1-2")}},
		{{Address: addr(2), Data: []byte("h3-a2")}},
	})
}

func TestChainRoundTrips(t *testing.T) {
	chain := testChain()
	for i, dm := range chain {
		h := uint64(i + 1)
		enc, err := EncodeMomentum(dm)
		if err != nil {
			t.Fatalf("height %d: EncodeMomentum: %v", h, err)
		}
		got, err := DecodeMomentum(enc)
		if err != nil {
			t.Fatalf("height %d: DecodeMomentum: %v", h, err)
		}
		if got.Momentum.Hash != dm.Momentum.Hash || got.Momentum.Height != dm.Momentum.Height {
			t.Fatalf("height %d: momentum hash/height mismatch", h)
		}
		if len(got.AccountBlocks) != len(dm.AccountBlocks) {
			t.Fatalf("height %d: %d account blocks, want %d", h, len(got.AccountBlocks), len(dm.AccountBlocks))
		}
		for j := range dm.AccountBlocks {
			if got.AccountBlocks[j].Hash != dm.AccountBlocks[j].Hash || got.AccountBlocks[j].Height != dm.AccountBlocks[j].Height {
				t.Fatalf("height %d block %d: hash/height mismatch", h, j)
			}
		}
		reenc, err := EncodeMomentum(got)
		if err != nil {
			t.Fatalf("height %d: re-encode: %v", h, err)
		}
		if !bytes.Equal(enc, reenc) {
			t.Fatalf("height %d: re-encoding does not match original", h)
		}

		wantStateRoot := h%2 == 1
		if hasRoot := dm.Momentum.StateRoot != (types.Hash{}); hasRoot != wantStateRoot {
			t.Fatalf("height %d: StateRoot set = %v, want %v", h, hasRoot, wantStateRoot)
		}
	}
}

func TestChainWithDescendantRoundTrips(t *testing.T) {
	chain := ipctest.Chain([][]ipctest.Block{
		{{Address: addr(1), Data: []byte("root")}},
	})
	dm := chain[0]
	descendant := &nom.AccountBlock{
		Version:              1,
		ChainIdentifier:      1,
		BlockType:            nom.BlockTypeUserSend,
		Height:               dm.AccountBlocks[0].Height + 1,
		MomentumAcknowledged: dm.AccountBlocks[0].MomentumAcknowledged,
		Address:              dm.AccountBlocks[0].Address,
		ToAddress:            types.Address{0xEE},
		Data:                 []byte("descendant"),
	}
	dm.AccountBlocks[0].DescendantBlocks = []*nom.AccountBlock{descendant}
	ipctest.Seal(dm)

	enc, err := EncodeMomentum(dm)
	if err != nil {
		t.Fatalf("EncodeMomentum: %v", err)
	}
	got, err := DecodeMomentum(enc)
	if err != nil {
		t.Fatalf("DecodeMomentum: %v", err)
	}
	if len(got.AccountBlocks[0].DescendantBlocks) != 1 {
		t.Fatalf("decoded block has %d descendants, want 1", len(got.AccountBlocks[0].DescendantBlocks))
	}
	if got.AccountBlocks[0].DescendantBlocks[0].Hash != descendant.Hash {
		t.Fatalf("descendant hash mismatch")
	}
}

// TestTamperedMomentumFieldsHashMismatch tampers one momentum field per case, after Sealing (so
// every OTHER field is self-consistent), then flips the stored Hash back to the pre-tamper value
// so the encoded bytes disagree with what ComputeHash now produces.
func TestTamperedMomentumFieldsHashMismatch(t *testing.T) {
	fresh := func() *nom.DetailedMomentum {
		return ipctest.Chain([][]ipctest.Block{{{Address: addr(1), Data: []byte("x")}}})[0]
	}
	fields := map[string]func(*nom.Momentum){
		"Version":         func(m *nom.Momentum) { m.Version++ },
		"ChainIdentifier": func(m *nom.Momentum) { m.ChainIdentifier++ },
		"PreviousHash":    func(m *nom.Momentum) { m.PreviousHash[0] ^= 0xff },
		"Height":          func(m *nom.Momentum) { m.Height += 5 },
		"TimestampUnix":   func(m *nom.Momentum) { m.TimestampUnix++ },
		"Data":            func(m *nom.Momentum) { m.Data = append(append([]byte{}, m.Data...), 0x01) },
		"ChangesHash":     func(m *nom.Momentum) { m.ChangesHash[0] ^= 0xff },
		"NextFusionPrice": func(m *nom.Momentum) { m.NextFusionPrice++ },
		"NextWorkPrice":   func(m *nom.Momentum) { m.NextWorkPrice++ },
		"StateRoot":       func(m *nom.Momentum) { m.StateRoot[0] ^= 0xff; m.StateRoot[31] ^= 0xff },
	}
	for name, mutate := range fields {
		t.Run("momentum/"+name, func(t *testing.T) {
			dm := fresh()
			staleHash := dm.Momentum.Hash
			mutate(dm.Momentum)
			dm.Momentum.Hash = staleHash // stale: no longer matches ComputeHash()
			enc, err := EncodeMomentum(dm)
			if err != nil {
				t.Fatalf("EncodeMomentum: %v", err)
			}
			if _, err := DecodeMomentum(enc); !errors.Is(err, errHashMismatch) {
				t.Fatalf("DecodeMomentum: err = %v, want errHashMismatch", err)
			}
		})
	}

	t.Run("momentum/Content", func(t *testing.T) {
		dm := ipctest.Chain([][]ipctest.Block{
			{{Address: addr(1), Data: []byte("a")}, {Address: addr(2), Data: []byte("b")}},
		})[0]
		staleHash := dm.Momentum.Hash
		dm.Momentum.Content[0].Height++
		dm.Momentum.Hash = staleHash
		enc, err := EncodeMomentum(dm)
		if err != nil {
			t.Fatalf("EncodeMomentum: %v", err)
		}
		if _, err := DecodeMomentum(enc); !errors.Is(err, errHashMismatch) {
			t.Fatalf("DecodeMomentum: err = %v, want errHashMismatch", err)
		}
	})

	t.Run("momentum/Hash", func(t *testing.T) {
		dm := fresh()
		dm.Momentum.Hash[0] ^= 0xff
		enc, err := EncodeMomentum(dm)
		if err != nil {
			t.Fatalf("EncodeMomentum: %v", err)
		}
		if _, err := DecodeMomentum(enc); !errors.Is(err, errHashMismatch) {
			t.Fatalf("DecodeMomentum: err = %v, want errHashMismatch", err)
		}
	})
}

func TestTamperedBlockFieldsHashMismatch(t *testing.T) {
	fresh := func() *nom.DetailedMomentum {
		return ipctest.Chain([][]ipctest.Block{{{Address: addr(1), Data: []byte("x")}}})[0]
	}
	fields := map[string]func(*nom.AccountBlock){
		"Version":              func(b *nom.AccountBlock) { b.Version++ },
		"ChainIdentifier":      func(b *nom.AccountBlock) { b.ChainIdentifier++ },
		"BlockType":            func(b *nom.AccountBlock) { b.BlockType = nom.BlockTypeUserReceive },
		"PreviousHash":         func(b *nom.AccountBlock) { b.PreviousHash[0] ^= 0xff },
		"Height":               func(b *nom.AccountBlock) { b.Height += 3 },
		"MomentumAcknowledged": func(b *nom.AccountBlock) { b.MomentumAcknowledged.Height++ },
		"Address":              func(b *nom.AccountBlock) { b.Address[1] ^= 0xff },
		"ToAddress":            func(b *nom.AccountBlock) { b.ToAddress[0] ^= 0xff },
		"Amount":               func(b *nom.AccountBlock) { b.Amount.Add(b.Amount, b.Amount) },
		"TokenStandard":        func(b *nom.AccountBlock) { b.TokenStandard[0] ^= 0xff },
		"FromBlockHash":        func(b *nom.AccountBlock) { b.FromBlockHash[0] ^= 0xff },
		"Data":                 func(b *nom.AccountBlock) { b.Data = append(append([]byte{}, b.Data...), 0x02) },
		"FusedPlasma":          func(b *nom.AccountBlock) { b.FusedPlasma++ },
		"Difficulty":           func(b *nom.AccountBlock) { b.Difficulty++ },
		"Nonce":                func(b *nom.AccountBlock) { b.Nonce.Data[0] ^= 0xff },
	}
	for name, mutate := range fields {
		t.Run("block/"+name, func(t *testing.T) {
			dm := fresh()
			b := dm.AccountBlocks[0]
			staleHash := b.Hash
			mutate(b)
			b.Hash = staleHash
			enc, err := EncodeMomentum(dm)
			if err != nil {
				t.Fatalf("EncodeMomentum: %v", err)
			}
			if _, err := DecodeMomentum(enc); !errors.Is(err, errHashMismatch) {
				t.Fatalf("DecodeMomentum: err = %v, want errHashMismatch", err)
			}
		})
	}

	t.Run("block/Hash", func(t *testing.T) {
		dm := fresh()
		dm.AccountBlocks[0].Hash[0] ^= 0xff
		enc, err := EncodeMomentum(dm)
		if err != nil {
			t.Fatalf("EncodeMomentum: %v", err)
		}
		if _, err := DecodeMomentum(enc); !errors.Is(err, errHashMismatch) {
			t.Fatalf("DecodeMomentum: err = %v, want errHashMismatch", err)
		}
	})

	// The single-block cases above all tamper AccountBlocks[0]; this one tamper's the second
	// block of a two-block momentum, so a regression that only re-verifies the first block would
	// still be caught.
	t.Run("block/1/Data", func(t *testing.T) {
		dm := testChain()[0] // height 1: two blocks, addresses 1 and 2
		if len(dm.AccountBlocks) != 2 {
			t.Fatalf("fixture has %d account blocks, want 2", len(dm.AccountBlocks))
		}
		b := dm.AccountBlocks[1]
		staleHash := b.Hash
		b.Data = append(append([]byte{}, b.Data...), 0x02)
		b.Hash = staleHash
		enc, err := EncodeMomentum(dm)
		if err != nil {
			t.Fatalf("EncodeMomentum: %v", err)
		}
		if _, err := DecodeMomentum(enc); !errors.Is(err, errHashMismatch) {
			t.Fatalf("DecodeMomentum: err = %v, want errHashMismatch", err)
		}
	})
}

func TestTamperedDescendantFieldsHashMismatch(t *testing.T) {
	build := func() *nom.DetailedMomentum {
		dm := ipctest.Chain([][]ipctest.Block{{{Address: addr(1), Data: []byte("root")}}})[0]
		descendant := &nom.AccountBlock{
			Version:              1,
			ChainIdentifier:      1,
			BlockType:            nom.BlockTypeUserSend,
			Height:               dm.AccountBlocks[0].Height + 1,
			MomentumAcknowledged: dm.AccountBlocks[0].MomentumAcknowledged,
			Address:              dm.AccountBlocks[0].Address,
			ToAddress:            types.Address{0xEE},
			Data:                 []byte("descendant"),
		}
		dm.AccountBlocks[0].DescendantBlocks = []*nom.AccountBlock{descendant}
		ipctest.Seal(dm)
		return dm
	}

	t.Run("descendant/Data", func(t *testing.T) {
		dm := build()
		d := dm.AccountBlocks[0].DescendantBlocks[0]
		stale := d.Hash
		d.Data = append(append([]byte{}, d.Data...), 0x09)
		d.Hash = stale
		// The parent's own hash still covers the (now stale but unchanged) descendant hash, so
		// only the descendant's own verification catches this.
		enc, err := EncodeMomentum(dm)
		if err != nil {
			t.Fatalf("EncodeMomentum: %v", err)
		}
		if _, err := DecodeMomentum(enc); !errors.Is(err, errHashMismatch) {
			t.Fatalf("DecodeMomentum: err = %v, want errHashMismatch", err)
		}
	})

	t.Run("descendant/Hash", func(t *testing.T) {
		dm := build()
		d := dm.AccountBlocks[0].DescendantBlocks[0]
		d.Hash[0] ^= 0xff
		// This also breaks the parent's DescendantBlocksHash, so either check may fire; both are
		// errHashMismatch.
		enc, err := EncodeMomentum(dm)
		if err != nil {
			t.Fatalf("EncodeMomentum: %v", err)
		}
		if _, err := DecodeMomentum(enc); !errors.Is(err, errHashMismatch) {
			t.Fatalf("DecodeMomentum: err = %v, want errHashMismatch", err)
		}
	})
}

func TestDecodeMomentumNeverPanics(t *testing.T) {
	valid, err := EncodeMomentum(testChain()[0])
	if err != nil {
		t.Fatalf("EncodeMomentum: %v", err)
	}

	check := func(name string, b []byte) {
		t.Run(name, func(t *testing.T) {
			defer func() {
				if r := recover(); r != nil {
					t.Fatalf("DecodeMomentum panicked: %v", r)
				}
			}()
			if _, err := DecodeMomentum(b); err == nil {
				t.Fatalf("DecodeMomentum(%s) = nil error, want one", name)
			}
		})
	}

	check("random bytes", []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08})
	for i := 0; i < len(valid); i++ {
		check("prefix", valid[:i])
	}
	check("trailing byte", append(append([]byte{}, valid...), 0x00))

	// A valid outer structure whose momentum bytes are {0x1a, 0x00}: MomentumProto field 3
	// (hash), present but empty, reaching the DeProtoHash length panic.
	outer := []byte{0, 0, 0, 2, 0x1a, 0x00, 0, 0, 0, 0}
	check("empty hash field panics inside DeProtoHash", outer)

	// n = 0xFFFFFFFF with 4 bytes left.
	nearMaxN := []byte{0, 0, 0, 0, 0xFF, 0xFF, 0xFF, 0xFF, 0, 0, 0, 0}
	check("huge n with 4 bytes left", nearMaxN)

	check("zero-length momentum", nil)
}

func TestEncodeMomentumRejectsNilAndPanicking(t *testing.T) {
	if _, err := EncodeMomentum(nil); err == nil {
		t.Fatalf("EncodeMomentum(nil) = nil error")
	}
	if _, err := EncodeMomentum(&nom.DetailedMomentum{}); err == nil {
		t.Fatalf("EncodeMomentum with nil Momentum = nil error")
	}
	dm := testChain()[0]
	dm.AccountBlocks = append(dm.AccountBlocks, nil)
	if _, err := EncodeMomentum(dm); err == nil {
		t.Fatalf("EncodeMomentum with a nil account block = nil error")
	}

	dm2 := testChain()[0]
	dm2.Momentum.Content = append(dm2.Momentum.Content, nil)
	func() {
		defer func() {
			if r := recover(); r != nil {
				t.Fatalf("EncodeMomentum panicked on a nil content entry: %v", r)
			}
		}()
		if _, err := EncodeMomentum(dm2); err == nil {
			t.Fatalf("EncodeMomentum with a nil content entry = nil error")
		}
	}()
}

func FuzzDecodeMomentum(f *testing.F) {
	chain := testChain()
	for _, dm := range chain {
		if b, err := EncodeMomentum(dm); err == nil {
			f.Add(b)
		}
	}
	withDescendant := ipctest.Chain([][]ipctest.Block{{{Address: addr(1), Data: []byte("root")}}})[0]
	descendant := &nom.AccountBlock{
		Version: 1, ChainIdentifier: 1, BlockType: nom.BlockTypeUserSend,
		Height:               withDescendant.AccountBlocks[0].Height + 1,
		MomentumAcknowledged: withDescendant.AccountBlocks[0].MomentumAcknowledged,
		Address:              withDescendant.AccountBlocks[0].Address,
		ToAddress:            types.Address{0xEE},
		Data:                 []byte("descendant"),
	}
	withDescendant.AccountBlocks[0].DescendantBlocks = []*nom.AccountBlock{descendant}
	ipctest.Seal(withDescendant)
	if b, err := EncodeMomentum(withDescendant); err == nil {
		f.Add(b)
	}
	f.Add([]byte{})

	f.Fuzz(func(t *testing.T, b []byte) {
		defer func() {
			if r := recover(); r != nil {
				t.Fatalf("DecodeMomentum panicked: %v", r)
			}
		}()
		dm, err := DecodeMomentum(b)
		if (dm == nil) == (err == nil) {
			t.Fatalf("DecodeMomentum: exactly one of (result, error) must be nil; got dm=%v err=%v", dm, err)
		}
	})
}

// TestSplitMomentumFrameRejectsZeroLengthBlock checks that a declared block length of 0 is
// rejected outright: no valid AccountBlock encoding is empty.
func TestSplitMomentumFrameRejectsZeroLengthBlock(t *testing.T) {
	w := &pwriter{}
	w.u32(0) // momentum bytes: empty; splitMomentumFrame never decodes them
	w.u32(1) // n = 1
	w.u32(0) // block 0's declared length: 0
	if _, _, _, err := splitMomentumFrame(w.buf); err == nil {
		t.Fatalf("splitMomentumFrame accepted a zero-length account block")
	}
}

// TestSplitMomentumFrameZeroLengthBlocksStayCheap is the regression guard for the specific
// zero-length case: a frame that declares n far larger than the entries it actually pays for
// (all of them zero-length, so free apart from their 4-byte headers) must be rejected at the
// very first entry, not walked all the way through first. splitMomentumFrame never allocates a
// per-entry slice at all (see TestDecodeMomentumAdversarialEntriesStayCheap for the general
// case), so this mostly guards against a future regression back to a presized or per-entry
// accumulating structure.
func TestSplitMomentumFrameZeroLengthBlocksStayCheap(t *testing.T) {
	const declared = 1_000_000 // n * 4 bytes of header alone is ~4 MiB
	w := &pwriter{}
	w.u32(0)
	w.u32(declared)
	w.raw(make([]byte, declared*4)) // declared entries, each u32(0): a zero-length block claim
	payload := w.buf

	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	_, _, _, err := splitMomentumFrame(payload)
	runtime.ReadMemStats(&after)

	if err == nil {
		t.Fatalf("splitMomentumFrame accepted %d declared zero-length blocks", declared)
	}
	const wantMax = 1 << 20 // 1 MiB: generous given rejection happens on the very first entry
	if allocated := after.TotalAlloc - before.TotalAlloc; allocated > wantMax {
		t.Fatalf("splitMomentumFrame allocated %d bytes rejecting %d declared zero-length blocks, want well under %d", allocated, declared, wantMax)
	}
}

// TestDecodeMomentumAdversarialEntriesStayCheap is the regression guard for the general
// allocation-amplification case: each declared entry is individually well-formed (a non-zero
// length, one body byte), so splitMomentumFrame's structural walk accepts all of them and does
// not reject on entry 0 the way the all-zero-length case above does. What must still stay cheap
// is decoding: nom.DeserializeAccountBlock on a single 0x00 byte fails immediately (an invalid
// protobuf tag), so DecodeMomentum's second pass over the block region must stop at entry 0
// rather than materializing a byte-range or a decoded value for any of the other 999,999
// declared entries.
func TestDecodeMomentumAdversarialEntriesStayCheap(t *testing.T) {
	const declared = 1_000_000 // n * 5 bytes (u32 len ‖ one body byte) is ~4.8 MiB

	// A structurally valid (if hash-unverified) momentum encoding, so DeserializeMomentum
	// succeeds and DecodeMomentum reaches the block region loop this test targets, rather than
	// panicking earlier on the momentum itself.
	momBytes, err := (&nom.Momentum{Version: nom.StateRootMomentumVersion, Height: 1}).Serialize()
	if err != nil {
		t.Fatalf("serialize placeholder momentum: %v", err)
	}

	w := &pwriter{}
	w.u32(uint32(len(momBytes)))
	w.raw(momBytes)
	w.u32(declared)
	for i := 0; i < declared; i++ {
		w.u32(1) // declared length: 1 byte
		w.u8(0x00)
	}
	payload := w.buf

	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	dm, err := DecodeMomentum(payload)
	runtime.ReadMemStats(&after)

	if err == nil {
		t.Fatalf("DecodeMomentum accepted %d adversarial one-byte account block entries; dm = %+v", declared, dm)
	}
	const wantMax = 1 << 20 // 1 MiB: generous; a correct decode never gets past entry 0
	if allocated := after.TotalAlloc - before.TotalAlloc; allocated > wantMax {
		t.Fatalf("DecodeMomentum allocated %d bytes on %d adversarial one-byte entries, want well under %d", allocated, declared, wantMax)
	}
}
