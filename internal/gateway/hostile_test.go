package gateway

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/zenon-network/go-zenon/common/crypto"
	"github.com/zenon-network/go-zenon/common/types"
	"github.com/zenon-network/go-zenon/vm/constants"

	"github.com/Zeno-L2/zeno-executor/internal/batch"
	"github.com/Zeno-L2/zeno-executor/internal/codec"
	"github.com/Zeno-L2/zeno-executor/internal/recovery"
	"github.com/Zeno-L2/zeno-executor/internal/state"
)

// pollWithin runs one Poll and fails the test if it does not return
// within five seconds.
func pollWithin(t *testing.T, r *rig) error {
	t.Helper()
	done := make(chan error, 1)
	go func() { done <- r.c.Poll(context.Background()) }()
	select {
	case err := <-done:
		return err
	case <-time.After(5 * time.Second):
		t.Fatal("Poll did not return")
		return nil
	}
}

type hostileKind struct {
	name string
	// make creates a hostile file at path; outside is a regular file
	// outside the directory holding the original content.
	make func(t *testing.T, path, outside string)
}

func hostileKinds() []hostileKind {
	return []hostileKind{
		{"sparse file", func(t *testing.T, path, _ string) {
			f, err := os.Create(path)
			if err != nil {
				t.Fatal(err)
			}
			defer f.Close()
			if err := f.Truncate(1 << 40); err != nil {
				t.Skipf("no sparse files: %v", err)
			}
		}},
		{"symlink to /dev/zero", func(t *testing.T, path, _ string) {
			if err := os.Symlink("/dev/zero", path); err != nil {
				t.Fatal(err)
			}
		}},
		{"symlink to a file outside", func(t *testing.T, path, outside string) {
			if err := os.Symlink(outside, path); err != nil {
				t.Fatal(err)
			}
		}},
		{"FIFO", func(t *testing.T, path, _ string) {
			if err := syscall.Mkfifo(path, 0o644); err != nil {
				t.Fatal(err)
			}
		}},
	}
}

func TestChainHostileFiles(t *testing.T) {
	pristine, cs := buildA(t)
	targets := []struct {
		name     string
		accepted bool
		path     func(l recovery.Layout) string
	}{
		{"accepted bundle", true, func(l recovery.Layout) string { return recovery.BundlePath(l.Bundles, cs[0].DAHash) }},
		{"accepted sidecar", true, func(l recovery.Layout) string { return recovery.ReceiptsPath(l.Receipts, cs[0].DAHash) }},
		{"pending record", false, func(l recovery.Layout) string { return recordPath(l, 0) }},
		{"pending bundle", false, func(l recovery.Layout) string { return recovery.BundlePath(l.Bundles, cs[0].DAHash) }},
		{"pending sidecar", false, func(l recovery.Layout) string { return recovery.ReceiptsPath(l.Receipts, cs[0].DAHash) }},
	}
	for _, tg := range targets {
		for _, k := range hostileKinds() {
			t.Run(tg.name+", "+k.name, func(t *testing.T) {
				dir := t.TempDir()
				copyDir(t, pristine, dir)
				path := tg.path(layoutOf(dir))
				outside := filepath.Join(t.TempDir(), "outside")
				writeFile(t, outside, readFile(t, path))
				if err := os.Remove(path); err != nil {
					t.Fatal(err)
				}
				k.make(t, path, outside)
				s := newFakeSettlement()
				s.height = 10
				if tg.accepted {
					s.accept(cs[0], 100)
				}
				r := newTestChain(t, dir, s)
				if err := pollWithin(t, r); err == nil {
					t.Fatal("Poll succeeded over a hostile file")
				}
				if st := r.c.Head().Status; st == StatusInconsistent {
					t.Fatalf("status = %s, want a read failure that keeps the domain healthy", st)
				}
				if r.c.Head().Latest != nil {
					t.Fatalf("latest = %d, want none", tagID(r.c.Head().Latest))
				}
			})
		}
	}

	t.Run("oversize record", func(t *testing.T) {
		dir := t.TempDir()
		copyDir(t, pristine, dir)
		writeFile(t, recordPath(layoutOf(dir), 0), make([]byte, recovery.MaxBatchRecordSize+1))
		r := newTestChain(t, dir, newFakeSettlement())
		if err := pollWithin(t, r); err == nil {
			t.Fatal("Poll succeeded over an oversize record")
		}
		if st := r.c.Head().Status; st == StatusInconsistent {
			t.Fatalf("status = %s", st)
		}
	})

	t.Run("oversize record of a held pending batch", func(t *testing.T) {
		dir := t.TempDir()
		copyDir(t, pristine, dir)
		s := newFakeSettlement()
		s.height = 10
		s.accept(cs[0], 100)
		r := newTestChain(t, dir, s)
		requireTags(t, doPoll(t, r), 4, 0, -1, StatusOK)
		writeFile(t, recordPath(layoutOf(dir), 2), make([]byte, recovery.MaxBatchRecordSize+1))
		if err := pollWithin(t, r); err == nil {
			t.Fatal("Poll succeeded over an oversize record")
		}
	})

	t.Run("FIFO at the next pending record", func(t *testing.T) {
		dir := t.TempDir()
		copyDir(t, pristine, dir)
		if err := syscall.Mkfifo(recordPath(layoutOf(dir), 5), 0o644); err != nil {
			t.Fatal(err)
		}
		s := newFakeSettlement()
		s.height = 10
		s.accept(cs[0], 100)
		r := newTestChain(t, dir, s)
		if err := pollWithin(t, r); err == nil {
			t.Fatal("Poll succeeded over a FIFO")
		}
		requireTags(t, r.c.Head(), 4, 0, -1, StatusOK)
	})
}

func TestReadRegularFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "f")
	writeFile(t, path, []byte("abcd"))
	b, err := recovery.ReadRegularFile(path, 4)
	if err != nil || string(b) != "abcd" {
		t.Fatalf("at the limit: %q, %v", b, err)
	}
	if cap(b) != 5 {
		t.Fatalf("at the limit: cap %d, want one buffer of size+1", cap(b))
	}
	if _, err := recovery.ReadRegularFile(path, 3); !errors.Is(err, recovery.ErrTooLarge) {
		t.Fatalf("above the limit: %v, want ErrTooLarge", err)
	}
	_, err = recovery.ReadRegularFile(filepath.Join(dir, "none"), 4)
	if !os.IsNotExist(err) || errors.Is(err, recovery.ErrTooLarge) {
		t.Fatalf("missing file: %v, want not-exist", err)
	}
	empty := filepath.Join(dir, "empty")
	writeFile(t, empty, nil)
	if b, err := recovery.ReadRegularFile(empty, 4); err != nil || len(b) != 0 {
		t.Fatalf("empty file: %q, %v", b, err)
	}
}

// A file that another process grows or shrinks during the read never
// yields more than maxSize bytes and never costs more than a few
// buffers of maxSize bytes: the read returns a prefix of the file or
// an error satisfying ErrTooLarge.
func TestReadRegularFileChangingSize(t *testing.T) {
	const maxSize = 4 << 10
	path := filepath.Join(t.TempDir(), "f")
	chunk := make([]byte, 512)
	for i := range chunk {
		chunk[i] = 'x'
	}
	var grew int
	for i := 0; i < 5000 && (grew < 20 || i < 200); i++ {
		shrink := i%2 == 1
		if shrink {
			writeFile(t, path, make([]byte, 3000))
		} else {
			writeFile(t, path, chunk[:1])
		}
		done := make(chan struct{})
		go func() {
			defer close(done)
			f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0)
			if err != nil {
				return
			}
			defer f.Close()
			if shrink {
				_ = f.Truncate(100)
				return
			}
			for n := 0; n < 40; n++ {
				if _, err := f.Write(chunk); err != nil {
					return
				}
				if n == 3 {
					// A sparse extension to 8 MiB: a read that did not
					// stop at the limit would allocate far more.
					_ = f.Truncate(8 << 20)
				}
			}
		}()
		var before, after runtime.MemStats
		runtime.ReadMemStats(&before)
		data, err := recovery.ReadRegularFile(path, maxSize)
		runtime.ReadMemStats(&after)
		<-done
		if used := after.TotalAlloc - before.TotalAlloc; used > 32*maxSize {
			t.Fatalf("read %d allocated %d bytes for a %d byte limit", i, used, maxSize)
		}
		switch {
		case err != nil && !errors.Is(err, recovery.ErrTooLarge):
			t.Fatalf("read %d: %v", i, err)
		case err != nil && strings.Contains(err.Error(), "grew past"):
			grew++
		case err == nil && len(data) > maxSize:
			t.Fatalf("read %d returned %d bytes, above the %d byte limit", i, len(data), maxSize)
		}
	}
	if grew == 0 {
		t.Fatal("no read saw the file grow past the limit")
	}
}

func TestChainPendingCap(t *testing.T) {
	const n = maxPendingBatches + 3
	batches := make([][]fixtureInput, n)
	for i := range batches {
		batches[i] = []fixtureInput{{index: uint64(i + 1), effects: success(put(pA, fmt.Sprint("v", i)))}}
	}
	dir := t.TempDir()
	cs := buildHistory(t, dir, nil, batches)
	s := newFakeSettlement()
	s.height = 10
	r := newTestChain(t, dir, s)

	requireTags(t, doPoll(t, r), maxPendingBatches-1, -1, -1, StatusOK)
	requireTags(t, doPoll(t, r), maxPendingBatches-1, -1, -1, StatusOK)
	s.accept(cs[0], 100)
	s.accept(cs[1], 100)
	requireTags(t, doPoll(t, r), maxPendingBatches+1, 1, -1, StatusOK)
}

func TestCheckBatchRecordCount(t *testing.T) {
	records := make([]codec.BundleInputRecord, codec.MaxBatchInputs+1)
	for i := range records {
		records[i] = codec.BundleInputRecord{GlobalInputIndex: uint64(i)}
	}
	bundle, err := codec.EncodeBundle(records)
	if err != nil {
		t.Fatal(err)
	}
	var want expect
	copy(want.daHash[:], crypto.Hash(bundle))
	_, err = checkBatch(state.Version{}, want, bundle, nil, math.MaxInt64)
	if err == nil || !strings.Contains(err.Error(), "above the limit") {
		t.Fatalf("checkBatch = %v, want a record-count failure", err)
	}
}

func TestLocalSourceSizeCaps(t *testing.T) {
	pristine, cs := buildA(t)
	dir := t.TempDir()
	copyDir(t, pristine, dir)
	l := layoutOf(dir)
	src := NewLocalSource(dir, 1)
	cases := []struct {
		name string
		path string
		max  int64
		read func() ([]byte, error)
		// belowOnly skips the file at the limit, whose read would
		// allocate that many bytes.
		belowOnly bool
	}{
		{"bundle", recovery.BundlePath(l.Bundles, cs[0].DAHash), 1 << 30,
			func() ([]byte, error) { return src.Bundle(cs[0].DAHash) }, true},
		{"sidecar", recovery.ReceiptsPath(l.Receipts, cs[0].DAHash), 4 + 64*(4+263),
			func() ([]byte, error) { return src.Receipts(cs[0].DAHash) }, false},
	}
	for _, tc := range cases {
		sizes := []int64{tc.max, tc.max + 1}
		if tc.belowOnly {
			sizes = sizes[1:]
		}
		for _, size := range sizes {
			if err := os.Remove(tc.path); err != nil {
				t.Fatal(err)
			}
			f, err := os.Create(tc.path)
			if err != nil {
				t.Fatal(err)
			}
			if err := f.Truncate(size); err != nil {
				t.Skipf("no sparse files: %v", err)
			}
			f.Close()
			data, err := tc.read()
			if size == tc.max && (err != nil || int64(len(data)) != size) {
				t.Fatalf("%s of %d bytes: %d bytes, %v, want it read whole", tc.name, size, len(data), err)
			}
			if size > tc.max && err == nil {
				t.Fatalf("%s of %d bytes was read, want the %d byte limit", tc.name, size, tc.max)
			}
		}
	}
}

// TestBundleCeilingGate checks the ceiling gate with a lowered
// ceiling, so a file at the limit reads in a few MiB.
func TestBundleCeilingGate(t *testing.T) {
	if maxBundleBytes != 1<<30 {
		t.Fatalf("maxBundleBytes = %d, want %d", int64(maxBundleBytes), int64(1<<30))
	}
	pristine, cs := buildA(t)
	dir := t.TempDir()
	copyDir(t, pristine, dir)
	path := recovery.BundlePath(layoutOf(dir).Bundles, cs[0].DAHash)
	src := NewLocalSource(dir, 1)
	const ceiling = 8 << 20
	for _, size := range []int64{ceiling, ceiling + 1} {
		if err := os.Remove(path); err != nil {
			t.Fatal(err)
		}
		f, err := os.Create(path)
		if err != nil {
			t.Fatal(err)
		}
		if err := f.Truncate(size); err != nil {
			t.Skipf("no sparse files: %v", err)
		}
		f.Close()
		data, err := src.bundleWithin(cs[0].DAHash, ceiling)
		if size == ceiling && (err != nil || int64(len(data)) != size) {
			t.Fatalf("bundle of %d bytes: %d bytes, %v, want it read whole", size, len(data), err)
		}
		if size > ceiling && (err == nil || !strings.Contains(err.Error(), "above the")) {
			t.Fatalf("bundle of %d bytes: %v, want the size error", size, err)
		}
	}
}

func TestBatchRecordSizeCap(t *testing.T) {
	pristine, _ := buildA(t)
	dir := t.TempDir()
	copyDir(t, pristine, dir)
	path := recordPath(layoutOf(dir), 0)
	for _, tc := range []struct {
		size  int64
		limit bool
	}{{64 << 10, false}, {64<<10 + 1, true}} {
		f, err := os.Create(path)
		if err != nil {
			t.Fatal(err)
		}
		if err := f.Truncate(tc.size); err != nil {
			t.Skipf("no sparse files: %v", err)
		}
		f.Close()
		_, _, err = recovery.ReadBatchRecord(layoutOf(dir), 0)
		if err == nil {
			t.Fatalf("a zero-filled record of %d bytes decoded", tc.size)
		}
		if got := strings.Contains(err.Error(), "byte limit"); got != tc.limit {
			t.Fatalf("record of %d bytes: %v, limit rejection = %v, want %v", tc.size, err, got, tc.limit)
		}
	}
}

func TestCheckBatchShortInputData(t *testing.T) {
	data := []byte{0}
	er, err := codec.ExecutionResult{Version: 1, GlobalInputIndex: 1}.Encode()
	if err != nil {
		t.Fatal(err)
	}
	bundle, err := codec.EncodeBundle([]codec.BundleInputRecord{{GlobalInputIndex: 1, InputData: data, ExecutionResult: er}})
	if err != nil {
		t.Fatal(err)
	}
	sidecar, err := codec.EncodeReceiptSidecar([]codec.Receipt{{Version: codec.ReceiptVersion, InputIndex: 1, InputHash: types.NewHash(data), BatchID: 0}})
	if err != nil {
		t.Fatal(err)
	}
	_, leaves, err := codec.DecodeReceiptSidecar(sidecar)
	if err != nil {
		t.Fatal(err)
	}
	want := expect{
		firstInputSeq: 1,
		lastInputSeq:  1,
		daHash:        types.NewHash(bundle),
		inputRoot:     codec.MerkleRoot([][]byte{data}),
		receiptRoot:   codec.MerkleRoot(leaves),
	}
	_, err = checkBatch(state.Version{}, want, bundle, sidecar, math.MaxInt64)
	if err == nil || !strings.Contains(err.Error(), "shorter than its kind and send hash") {
		t.Fatalf("checkBatch = %v, want the short input data error", err)
	}
}

// longest is an input of one kind at its longest canonical encoding.
type longest struct {
	kind byte
	data []byte
	want int
}

// longestInputs returns, for each input kind 0 to 7 and two kinds with
// no tail, the longest canonical input and its expected length.
func longestInputs(t *testing.T) []longest {
	t.Helper()
	h := hashOfIndex(1)
	args := []struct {
		args codec.InputArgs
		want int
	}{
		{codec.CallL2Args{DepositAsset: fixtureAsset, Payload: make([]byte, codec.MaxCallPayload)}, 8333},
		{codec.DepositArgs{DepositAsset: fixtureAsset}, 137},
		{codec.DeployStartArgs{Metadata: make([]byte, constants.MaxContractMetadataSize)}, 369},
		{codec.DeployChunkArgs{}, 101},
		{codec.DeployFinalizeArgs{}, 65},
		{codec.UpgradeArgs{}, 97},
		{codec.TransferOwnershipArgs{}, 85},
		{codec.BuyStorageArgs{Bytes: 1}, 73},
	}
	var out []longest
	for i, a := range args {
		data, err := codec.Input{SourceAccountBlockHash: h, Args: a.args}.Encode()
		if err != nil {
			t.Fatalf("kind %d: encode: %v", i, err)
		}
		out = append(out, longest{kind: byte(i), data: data, want: a.want})
	}
	for _, kind := range []byte{8, 255} {
		raw := append([]byte{kind}, h[:]...)
		raw = append(raw, make([]byte, 10)...)
		data, err := codec.CanonicalInput(raw)
		if err != nil {
			t.Fatalf("kind %d: CanonicalInput: %v", kind, err)
		}
		out = append(out, longest{kind: kind, data: data, want: 33})
	}
	return out
}

// resultOfSize returns an execution result for input 1 whose encoding
// is n bytes: 31 events, 30 of them full and the last sized to fit.
func resultOfSize(t *testing.T, n int) codec.ExecutionResult {
	t.Helper()
	const empty, full, fixed = 122, 1076, 52
	last := n - empty - 30*full - fixed
	if last < 0 || last > codec.MaxEventDataSize {
		t.Fatalf("resultOfSize(%d): last event data of %d bytes is out of range", n, last)
	}
	er := codec.ExecutionResult{Version: 1, GlobalInputIndex: 1}
	for i := 0; i < 31; i++ {
		size := codec.MaxEventDataSize
		if i == 30 {
			size = last
		}
		er.Events = append(er.Events, codec.Event{InputIndex: 1, EventIndex: uint32(i), Data: make([]byte, size)})
	}
	enc, err := er.Encode()
	if err != nil {
		t.Fatalf("resultOfSize(%d): %v", n, err)
	}
	if len(enc) != n {
		t.Fatalf("resultOfSize(%d): encodes to %d bytes", n, len(enc))
	}
	return er
}

// oneRecordBatch returns batch 0 holding one record for input 1 with
// data, chunk and er, as the expected fields, bundle and sidecar.
func oneRecordBatch(t *testing.T, data, chunk []byte, er codec.ExecutionResult) (expect, []byte, []byte) {
	t.Helper()
	enc, err := er.Encode()
	if err != nil {
		t.Fatal(err)
	}
	bundle, err := codec.EncodeBundle([]codec.BundleInputRecord{{GlobalInputIndex: 1, InputData: data, DeployChunkData: chunk, ExecutionResult: enc}})
	if err != nil {
		t.Fatal(err)
	}
	var evs, outs [][]byte
	for _, ev := range er.Events {
		b, err := ev.Encode()
		if err != nil {
			t.Fatal(err)
		}
		evs = append(evs, b)
	}
	for _, m := range er.Outbox {
		b, err := m.Encode()
		if err != nil {
			t.Fatal(err)
		}
		outs = append(outs, b)
	}
	sdh, err := codec.StateDiffHash(er.StateDiff)
	if err != nil {
		t.Fatal(err)
	}
	sidecar, err := codec.EncodeReceiptSidecar([]codec.Receipt{{
		Version:       codec.ReceiptVersion,
		InputIndex:    1,
		InputHash:     types.NewHash(data),
		BatchID:       0,
		StateDiffHash: sdh,
		ReturnHash:    codec.ReturnHash(er.ReturnData),
		EventRoot:     codec.MerkleRoot(evs),
		EventCount:    uint32(len(evs)),
		OutboxRoot:    codec.MerkleRoot(outs),
		OutboxCount:   uint32(len(outs)),
	}})
	if err != nil {
		t.Fatal(err)
	}
	_, leaves, err := codec.DecodeReceiptSidecar(sidecar)
	if err != nil {
		t.Fatal(err)
	}
	want := expect{
		firstInputSeq: 1,
		lastInputSeq:  1,
		daHash:        types.NewHash(bundle),
		inputRoot:     codec.MerkleRoot([][]byte{data}),
		receiptRoot:   codec.MerkleRoot(leaves),
		eventRoot:     codec.MerkleRoot(evs),
		outboxRoot:    codec.MerkleRoot(outs),
		postStateRoot: state.Version{}.Root(),
	}
	return want, bundle, sidecar
}

// writeOneRecordBatch stores the batch oneRecordBatch returned under
// dir as a built batch record and returns its commitment.
func writeOneRecordBatch(t *testing.T, dir string, want expect, bundle, sidecar []byte) codec.BatchCommitment {
	t.Helper()
	l, err := recovery.NewLayout(dir, 1)
	if err != nil {
		t.Fatal(err)
	}
	writeFile(t, recovery.BundlePath(l.Bundles, want.daHash), bundle)
	writeFile(t, recovery.ReceiptsPath(l.Receipts, want.daHash), sidecar)
	c := codec.BatchCommitment{
		Versions: codec.Versions{
			ProtocolVersion:        codec.ProtocolVersion,
			RuntimeProfileVersion:  codec.RuntimeProfileVersion,
			MeteringVersion:        codec.MeteringVersion,
			SerializationVersion:   codec.SerializationVersion,
			StateDiffVersion:       codec.StateDiffVersion,
			ExecutionResultVersion: codec.ExecutionResultVersion,
			ReceiptVersion:         codec.ReceiptVersion,
			OutboxVersion:          codec.OutboxVersion,
			DAModeVersion:          codec.DAModeVersion,
		},
		DomainID:      1,
		BatchID:       0,
		FirstInputSeq: want.firstInputSeq,
		LastInputSeq:  want.lastInputSeq,
		PreStateRoot:  want.postStateRoot,
		PostStateRoot: want.postStateRoot,
		InputRoot:     want.inputRoot,
		ReceiptRoot:   want.receiptRoot,
		EventRoot:     want.eventRoot,
		OutboxRoot:    want.outboxRoot,
		DAHash:        want.daHash,
		DAMode:        batch.DAMode,
		ExecutorID:    types.Address{9},
	}
	enc, err := c.Encode()
	if err != nil {
		t.Fatal(err)
	}
	writeRecord(t, l, recovery.BatchRecord{
		BatchID:       0,
		Status:        recovery.StatusBuilt,
		FirstInputSeq: c.FirstInputSeq,
		LastInputSeq:  c.LastInputSeq,
		PreStateRoot:  c.PreStateRoot,
		PostStateRoot: c.PostStateRoot,
		DAHash:        c.DAHash,
		Commitment:    enc,
	})
	return c
}

func TestMaxInputData(t *testing.T) {
	for _, l := range longestInputs(t) {
		if len(l.data) != l.want || maxInputData(l.kind) != l.want {
			t.Fatalf("kind %d: input is %d bytes, maxInputData %d, want %d", l.kind, len(l.data), maxInputData(l.kind), l.want)
		}
		if got, err := codec.CanonicalInput(l.data); err != nil || !bytes.Equal(got, l.data) {
			t.Fatalf("kind %d: CanonicalInput = %d bytes, %v, want the input unchanged", l.kind, len(got), err)
		}
	}
	over := codec.Input{SourceAccountBlockHash: hashOfIndex(1), Args: codec.CallL2Args{Payload: make([]byte, codec.MaxCallPayload+1)}}
	if _, err := over.Encode(); !errors.Is(err, codec.ErrLimitExceeded) {
		t.Fatalf("Encode over MaxCallPayload = %v, want ErrLimitExceeded", err)
	}
}

func TestMaxExecutionResult(t *testing.T) {
	if constants.MaxExecutionResult != 32<<10 {
		t.Fatalf("MaxExecutionResult = %d, want %d", constants.MaxExecutionResult, 32<<10)
	}
}

func TestCheckBatchInputBound(t *testing.T) {
	empty := codec.ExecutionResult{Version: 1, GlobalInputIndex: 1}
	run := func(data, chunk []byte, er codec.ExecutionResult) error {
		want, bundle, sidecar := oneRecordBatch(t, data, chunk, er)
		_, err := checkBatch(state.Version{}, want, bundle, sidecar, math.MaxInt64)
		return err
	}
	requireFails := func(err error, sub string) {
		t.Helper()
		if err == nil || !strings.Contains(err.Error(), sub) {
			t.Fatalf("checkBatch = %v, want an error containing %q", err, sub)
		}
	}
	for _, l := range longestInputs(t) {
		if err := run(l.data, nil, empty); err != nil {
			t.Fatalf("kind %d at its longest: %v", l.kind, err)
		}
		over := append(bytes.Clone(l.data), 0)
		requireFails(run(over, nil, empty), fmt.Sprintf("input data is %d bytes, longer than the %d bytes", l.want+1, l.want))
		if l.kind != 3 {
			continue
		}
		if err := run(l.data, make([]byte, constants.MaxDeployChunkData), empty); err != nil {
			t.Fatalf("a chunk of MaxDeployChunkData bytes: %v", err)
		}
		requireFails(run(l.data, make([]byte, constants.MaxDeployChunkData+1), empty), "deploy chunk data is 15361 bytes")
	}
	call := longestInputs(t)[0].data
	if err := run(call, nil, resultOfSize(t, constants.MaxExecutionResult)); err != nil {
		t.Fatalf("a result of MaxExecutionResult bytes: %v", err)
	}
	requireFails(run(call, nil, resultOfSize(t, constants.MaxExecutionResult+1)), "execution result is 32769 bytes, longer than MaxExecutionResult (32768 bytes)")
}

// The result cap applies to the raw bytes, before they decode.
func TestCheckBatchResultCapBeforeDecode(t *testing.T) {
	raw := make([]byte, constants.MaxExecutionResult+1)
	if _, err := codec.DecodeExecutionResult(raw); !errors.Is(err, codec.ErrUnknownVersion) {
		t.Fatalf("DecodeExecutionResult(raw) = %v, want ErrUnknownVersion", err)
	}
	data := longestInputs(t)[0].data
	bundle, err := codec.EncodeBundle([]codec.BundleInputRecord{{GlobalInputIndex: 1, InputData: data, ExecutionResult: raw}})
	if err != nil {
		t.Fatal(err)
	}
	sidecar, err := codec.EncodeReceiptSidecar([]codec.Receipt{{Version: codec.ReceiptVersion, InputIndex: 1, InputHash: types.NewHash(data), BatchID: 0}})
	if err != nil {
		t.Fatal(err)
	}
	_, leaves, err := codec.DecodeReceiptSidecar(sidecar)
	if err != nil {
		t.Fatal(err)
	}
	want := expect{
		firstInputSeq: 1,
		lastInputSeq:  1,
		daHash:        types.NewHash(bundle),
		inputRoot:     codec.MerkleRoot([][]byte{data}),
		receiptRoot:   codec.MerkleRoot(leaves),
	}
	_, err = checkBatch(state.Version{}, want, bundle, sidecar, math.MaxInt64)
	if err == nil || !strings.Contains(err.Error(), "execution result is 32769 bytes, longer than MaxExecutionResult (32768 bytes)") {
		t.Fatalf("checkBatch = %v, want the result cap error", err)
	}
}

// The result cap applies to a record of every input kind, not only
// CallL2.
func TestCheckBatchResultCapEveryKind(t *testing.T) {
	for _, l := range longestInputs(t) {
		for _, n := range []int{constants.MaxExecutionResult, constants.MaxExecutionResult + 1} {
			want, bundle, sidecar := oneRecordBatch(t, l.data, nil, resultOfSize(t, n))
			_, err := checkBatch(state.Version{}, want, bundle, sidecar, math.MaxInt64)
			if n == constants.MaxExecutionResult && err != nil {
				t.Fatalf("kind %d, result of %d bytes: %v", l.kind, n, err)
			}
			if n > constants.MaxExecutionResult && (err == nil || !strings.Contains(err.Error(), "execution result is 32769 bytes")) {
				t.Fatalf("kind %d, result of %d bytes: checkBatch = %v, want the result cap error", l.kind, n, err)
			}
		}
	}
}

// A bound error names sizes and limits and never echoes the record's
// bytes, which a hostile executor chooses.
func TestCheckBatchBoundErrorsDoNotEchoBytes(t *testing.T) {
	fill := func(n int, b byte) []byte { return bytes.Repeat([]byte{b}, n) }
	call := longestInputs(t)[0].data
	overInput := append(bytes.Clone(call), fill(8, 0xAB)...)
	cases := []struct {
		name  string
		data  []byte
		chunk []byte
		raw   []byte
		mark  byte
	}{
		{"input data", overInput, nil, nil, 0xAB},
		{"deploy chunk data", longestInputs(t)[3].data, fill(constants.MaxDeployChunkData+1, 0xCD), nil, 0xCD},
		{"execution result", call, nil, fill(constants.MaxExecutionResult+1, 0xEF), 0xEF},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			raw := tc.raw
			if raw == nil {
				enc, err := (codec.ExecutionResult{Version: 1, GlobalInputIndex: 1}).Encode()
				if err != nil {
					t.Fatal(err)
				}
				raw = enc
			}
			bundle, err := codec.EncodeBundle([]codec.BundleInputRecord{{GlobalInputIndex: 1, InputData: tc.data, DeployChunkData: tc.chunk, ExecutionResult: raw}})
			if err != nil {
				t.Fatal(err)
			}
			sidecar, err := codec.EncodeReceiptSidecar([]codec.Receipt{{Version: codec.ReceiptVersion, InputIndex: 1, InputHash: types.NewHash(tc.data), BatchID: 0}})
			if err != nil {
				t.Fatal(err)
			}
			_, leaves, err := codec.DecodeReceiptSidecar(sidecar)
			if err != nil {
				t.Fatal(err)
			}
			want := expect{firstInputSeq: 1, lastInputSeq: 1, daHash: types.NewHash(bundle), inputRoot: codec.MerkleRoot([][]byte{tc.data}), receiptRoot: codec.MerkleRoot(leaves)}
			_, err = checkBatch(state.Version{}, want, bundle, sidecar, math.MaxInt64)
			if err == nil {
				t.Fatal("checkBatch accepted an over-bound record")
			}
			msg := err.Error()
			for _, echo := range []string{fmt.Sprintf("%x", fill(4, tc.mark)), string(fill(4, tc.mark)), fmt.Sprint(fill(2, tc.mark))} {
				if strings.Contains(msg, echo) {
					t.Fatalf("error echoes the record's bytes (%q): %.200s", echo, msg)
				}
			}
			if len(msg) > 300 {
				t.Fatalf("error is %d bytes long: %.200s", len(msg), msg)
			}
		})
	}
}

func TestChainInputBound(t *testing.T) {
	all := longestInputs(t)
	call, chunk := all[0].data, all[3].data
	empty := codec.ExecutionResult{Version: 1, GlobalInputIndex: 1}
	cases := []struct {
		name  string
		data  []byte
		chunk []byte
		er    codec.ExecutionResult
		over  string
	}{
		{"call at the bound", call, nil, empty, ""},
		{"call one byte over", append(bytes.Clone(call), 0), nil, empty, "input data is 8334 bytes"},
		{"chunk at the bound", chunk, make([]byte, constants.MaxDeployChunkData), empty, ""},
		{"chunk one byte over", chunk, make([]byte, constants.MaxDeployChunkData+1), empty, "deploy chunk data is 15361 bytes"},
		{"result at the cap", call, nil, resultOfSize(t, constants.MaxExecutionResult), ""},
		{"result one byte over", call, nil, resultOfSize(t, constants.MaxExecutionResult+1), "execution result is 32769 bytes"},
	}
	for _, mode := range []string{"accepted", "pending"} {
		for _, tc := range cases {
			t.Run(mode+"/"+tc.name, func(t *testing.T) {
				dir := t.TempDir()
				want, bundle, sidecar := oneRecordBatch(t, tc.data, tc.chunk, tc.er)
				c := writeOneRecordBatch(t, dir, want, bundle, sidecar)
				s := newFakeSettlement()
				s.height = 10
				if mode == "accepted" {
					s.accept(c, 100)
				}
				r := newIndexedChain(t, dir, s)
				h := doPoll(t, r)
				stored := has(t, r.idx, ikey(pfxInput, be64(1)))
				if tc.over == "" {
					if mode == "accepted" {
						requireTags(t, h, 0, 0, -1, StatusOK)
					} else {
						requireTags(t, h, 0, -1, -1, StatusOK)
					}
					if !stored {
						t.Fatal("the index lacks input 1")
					}
					return
				}
				requireTags(t, h, -1, -1, -1, StatusInconsistent)
				if h.InconsistentAt == nil || *h.InconsistentAt != 0 {
					t.Fatalf("InconsistentAt = %v, want 0", h.InconsistentAt)
				}
				if n := r.log.count("ALARM", "INCONSISTENT at batch 0", tc.over); n != 1 {
					t.Fatalf("want one alarm line with %q, got %d:\n%s", tc.over, n, r.log.dump())
				}
				if stored {
					t.Fatal("the index holds input 1 of an over-bound batch")
				}
			})
		}
	}
}

// A pending record whose inputs start at or below the batch below it
// stops the pending pass with one warning and leaves the index alone.
func TestChainRefusesOverlappingPendingBatch(t *testing.T) {
	// At 20 to 22 batches 0 to 2 finalize, so the refused batch 3 sits
	// on the finalized batch. At 100 none finalizes, so batch 3 sits on
	// batch 2 held in the chain.
	for _, tc := range []struct {
		name       string
		finalizeAt uint64
		finalized  int64
	}{{"on the finalized batch", 20, 2}, {"on a held batch", 100, -1}} {
		t.Run(tc.name, func(t *testing.T) {
			e, z := historyOverlap()
			edir, zdir := t.TempDir(), t.TempDir()
			es := buildHistory(t, edir, nil, e)
			zs := buildHistory(t, zdir, nil, z)
			if zs[3].PreStateRoot != es[2].PostStateRoot || zs[3].FirstInputSeq != 6 {
				t.Fatalf("fixture: z batch 3 pre-state %s first input %d", zs[3].PreStateRoot, zs[3].FirstInputSeq)
			}
			s := newFakeSettlement()
			s.height = 30
			for i := 0; i < 3; i++ {
				s.accept(es[i], tc.finalizeAt+uint64(i))
			}
			r := newIndexedChain(t, edir, s)
			requireTags(t, doPoll(t, r), 2, 2, tc.finalized, StatusOK)
			before := indexDump(t, r.idx)

			copyBatch(t, zdir, edir, zs[3])
			requireTags(t, doPoll(t, r), 2, 2, tc.finalized, StatusOK)
			requireTags(t, doPoll(t, r), 2, 2, tc.finalized, StatusOK)
			if r.log.count("WARN", "batch 3", "starts at input 6") != 1 {
				t.Fatalf("want one overlap warning:\n%s", r.log.dump())
			}
			requireDumpUnchanged(t, r.idx, before)

			removeRecord(t, edir, 3)
			doPoll(t, r)
			requireDumpUnchanged(t, r.idx, before)
		})
	}
}

func TestChainOverlapWarningIgnoresRewrittenDAHash(t *testing.T) {
	e, z := historyOverlap()
	edir, zdir := t.TempDir(), t.TempDir()
	es := buildHistory(t, edir, nil, e)
	zs := buildHistory(t, zdir, nil, z)
	s := newFakeSettlement()
	s.height = 30
	for i := 0; i < 3; i++ {
		s.accept(es[i], uint64(20+i))
	}
	r := newIndexedChain(t, edir, s)
	doPoll(t, r)
	copyBatch(t, zdir, edir, zs[3])
	l := layoutOf(edir)
	rec, ok, err := recovery.ReadBatchRecord(l, 3)
	if err != nil || !ok {
		t.Fatalf("read record 3: ok %v err %v", ok, err)
	}
	for i := 0; i < 5; i++ {
		rec.DAHash = hashOfIndex(uint64(200 + i))
		writeRecord(t, l, rec)
		doPoll(t, r)
		if len(r.c.overlapWarned) > 1 {
			t.Fatalf("overlapWarned holds %d entries, want at most 1", len(r.c.overlapWarned))
		}
	}
	if n := r.log.count("WARN", "batch 3", "starts at input 6"); n != 1 {
		t.Fatalf("overlap warnings = %d, want 1:\n%s", n, r.log.dump())
	}
	removeRecord(t, edir, 3)
	doPoll(t, r)
	if len(r.c.overlapWarned) > 1 {
		t.Fatalf("overlapWarned holds %d entries", len(r.c.overlapWarned))
	}
}

// The overlap warning memory drops a batch id once the pending pass
// has moved to a lower id after a retraction.
func TestChainOverlapWarningMemoryFollowsPending(t *testing.T) {
	e, z := historyOverlap()
	edir, zdir := t.TempDir(), t.TempDir()
	buildHistory(t, edir, nil, e)
	zs := buildHistory(t, zdir, nil, z)
	s := newFakeSettlement()
	s.height = 10
	r := newIndexedChain(t, edir, s)
	doPoll(t, r)
	copyBatch(t, zdir, edir, zs[3])
	doPoll(t, r)
	if n := r.log.count("WARN", "batch 3", "starts at input 6"); n != 1 || len(r.c.overlapWarned) != 1 {
		t.Fatalf("warnings %d, remembered %d, want 1 and 1:\n%s", n, len(r.c.overlapWarned), r.log.dump())
	}
	removeRecord(t, edir, 2)
	doPoll(t, r)
	if len(r.c.overlapWarned) != 0 {
		t.Fatalf("remembered %d ids after pending moved below batch 3, want 0", len(r.c.overlapWarned))
	}
}
