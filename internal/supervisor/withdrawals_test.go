package supervisor

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"io"
	"math/big"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/zenon-network/go-zenon/common/types"

	"github.com/digitalSloth/go-executor/internal/codec"
	"github.com/digitalSloth/go-executor/internal/ipc"
)

// --- shared helpers ---

// testStore opens a withdrawal store for domainID in a fresh temporary
// directory.
func testStore(t *testing.T, domainID uint32) *withdrawalStore {
	t.Helper()
	s, err := openWithdrawalStore(filepath.Join(t.TempDir(), "withdrawals"), domainID)
	if err != nil {
		t.Fatalf("openWithdrawalStore: %v", err)
	}
	return s
}

func assetN(n byte) codec.AssetID {
	var a codec.AssetID
	a[31] = n
	return a
}

func u256Of(n uint64) codec.U256 {
	var u codec.U256
	binary.BigEndian.PutUint64(u[24:], n)
	return u
}

// l1msg is a kind 1 outbox message. A zero src makes it a refund.
func l1msg(input uint64, outbox uint32, asset codec.AssetID, amount uint64, src types.Hash) codec.OutboxMessage {
	return codec.OutboxMessage{
		SourceContract: src,
		InputIndex:     input,
		OutboxIndex:    outbox,
		Target:         codec.L1Withdrawal{RecipientL1: types.Address{0x0A}, Asset: asset, Amount: u256Of(amount)},
	}
}

// kind0msg is a kind 0 outbox message.
func kind0msg(input uint64, outbox uint32) codec.OutboxMessage {
	return codec.OutboxMessage{
		SourceContract: types.Hash{0x04},
		InputIndex:     input,
		OutboxIndex:    outbox,
		Target:         codec.L2Delivery{TargetContract: types.Hash{0x09}},
		Payload:        make([]byte, 52),
	}
}

// withdrawalBatch encodes msgs as the batch's leaves, in the given
// order, which must be ascending by (InputIndex, OutboxIndex). It sets
// c.OutboxRoot to the leaves' root and c.AssetFlowSummary to the
// per-asset debit sums of the kind 1 messages, ascending by asset with
// zero sums left out. It returns one Withdrawal per kind 1 message,
// carrying its proof.
func withdrawalBatch(t *testing.T, c *codec.BatchCommitment, msgs []codec.OutboxMessage) []ipc.Withdrawal {
	t.Helper()
	leaves := make([][]byte, len(msgs))
	for i, m := range msgs {
		b, err := m.Encode()
		if err != nil {
			t.Fatalf("encode outbox message %d: %v", i, err)
		}
		leaves[i] = b
	}
	c.OutboxRoot = codec.MerkleRoot(leaves)
	proofs := codec.MerkleProofs(leaves)

	sums := map[codec.AssetID]*big.Int{}
	var out []ipc.Withdrawal
	for i, m := range msgs {
		l1, ok := m.Target.(codec.L1Withdrawal)
		if !ok {
			continue
		}
		if sums[l1.Asset] == nil {
			sums[l1.Asset] = new(big.Int)
		}
		sums[l1.Asset].Add(sums[l1.Asset], new(big.Int).SetBytes(l1.Amount[:]))
		out = append(out, ipc.Withdrawal{BatchID: c.BatchID, Message: leaves[i], Proof: proofs[i]})
	}
	assets := make([]codec.AssetID, 0, len(sums))
	for a, s := range sums {
		if s.Sign() != 0 {
			assets = append(assets, a)
		}
	}
	slices.SortFunc(assets, func(a, b codec.AssetID) int { return bytes.Compare(a[:], b[:]) })
	summary := codec.AssetFlowSummary{}
	for _, a := range assets {
		d, err := codec.U256FromBig(sums[a])
		if err != nil {
			t.Fatalf("U256FromBig: %v", err)
		}
		summary = append(summary, codec.AssetFlowEntry{Asset: a, WithdrawalDebit: d})
	}
	c.AssetFlowSummary = summary
	return out
}

// writeHandOff writes report, each withdrawal and the closing
// WithdrawalsEnd to w in one Write, so serve ending at the report frame
// leaves nothing half-delivered.
func writeHandOff(t *testing.T, w io.Writer, report ipc.Message, ws ...ipc.Withdrawal) {
	t.Helper()
	var id uint64
	switch v := report.(type) {
	case ipc.Committed:
		id = v.Commitment.BatchID
	case ipc.Recovered:
		id = v.Commitment.BatchID
	default:
		t.Fatalf("writeHandOff: %T is not a Committed or Recovered", report)
	}
	var buf bytes.Buffer
	frames := []ipc.Message{report}
	for _, wd := range ws {
		frames = append(frames, wd)
	}
	frames = append(frames, ipc.WithdrawalsEnd{BatchID: id})
	for _, m := range frames {
		b, err := ipc.Encode(m)
		if err != nil {
			t.Fatalf("Encode %T: %v", m, err)
		}
		buf.Write(b)
	}
	if _, err := w.Write(buf.Bytes()); err != nil {
		t.Fatalf("write hand-off: %v", err)
	}
}

// holdRetries stops the retry of unverified files when on is true, and
// makes the next request frame retry when it is false.
func holdRetries(st *workerState, on bool) {
	st.mu.Lock()
	defer st.mu.Unlock()
	if on {
		st.verify.after = time.Now().Add(time.Hour)
	} else {
		st.verify.after = time.Time{}
	}
}

func storedOf(ws []ipc.Withdrawal) []StoredWithdrawal {
	out := make([]StoredWithdrawal, len(ws))
	for i, w := range ws {
		out[i] = StoredWithdrawal{Message: w.Message, Proof: w.Proof}
	}
	return out
}

// sampleBatch is a verified batch for domain and id with three
// withdrawals.
func sampleBatch(t *testing.T, domain uint32, id uint64) StoredBatch {
	t.Helper()
	c := commitmentFor(domain, types.Address{0x09}, id, 3)
	ws := withdrawalBatch(t, &c, []codec.OutboxMessage{
		l1msg(1, 0, assetN(1), 5, types.Hash{0x01}),
		l1msg(2, 0, assetN(1), 6, types.Hash{0x01}),
		l1msg(2, 1, assetN(2), 7, types.Hash{0x01}),
	})
	return StoredBatch{DomainID: domain, BatchID: id, Verified: true, OutboxRoot: c.OutboxRoot, Withdrawals: storedOf(ws)}
}

func mustBatch(t *testing.T, s *withdrawalStore, id uint64) StoredBatch {
	t.Helper()
	b, ok, err := s.Batch(id)
	if err != nil || !ok {
		t.Fatalf("Batch(%d) = (_, %v, %v), want a batch", id, ok, err)
	}
	return b
}

// --- store ---

func TestWithdrawalStoreRoundTrip(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "withdrawals")
	s, err := openWithdrawalStore(dir, 1)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	verified := sampleBatch(t, 1, 3)
	empty := StoredBatch{DomainID: 1, BatchID: 5, Verified: true}
	unverified := sampleBatch(t, 1, 4)
	unverified.Verified, unverified.OutboxRoot = false, types.Hash{}
	for _, b := range []StoredBatch{verified, empty, unverified} {
		if err := s.Put(b); err != nil {
			t.Fatalf("Put(%d): %v", b.BatchID, err)
		}
	}

	check := func(s *withdrawalStore) {
		t.Helper()
		got := mustBatch(t, s, 3)
		if !got.Verified || got.OutboxRoot != verified.OutboxRoot || len(got.Withdrawals) != 3 {
			t.Fatalf("batch 3 = %+v", got)
		}
		for i, w := range got.Withdrawals {
			if !bytes.Equal(w.Message, verified.Withdrawals[i].Message) || !bytes.Equal(w.Proof, verified.Withdrawals[i].Proof) {
				t.Fatalf("batch 3 withdrawal %d differs", i)
			}
		}
		if got := mustBatch(t, s, 5); !got.Verified || len(got.Withdrawals) != 0 {
			t.Fatalf("batch 5 = %+v, want verified and empty", got)
		}
		if got := mustBatch(t, s, 4); got.Verified || got.OutboxRoot != (types.Hash{}) || len(got.Withdrawals) != 3 {
			t.Fatalf("batch 4 = %+v, want unverified with a zero root", got)
		}
		want := []BatchRef{{3, true}, {4, false}, {5, true}}
		if got := s.Batches(); !slices.Equal(got, want) {
			t.Fatalf("Batches = %v, want %v", got, want)
		}
		for _, r := range want {
			if v, ok := s.Lookup(r.BatchID); !ok || v != r.Verified {
				t.Fatalf("Lookup(%d) = (%v, %v)", r.BatchID, v, ok)
			}
		}
		if _, ok := s.Lookup(9); ok {
			t.Fatalf("Lookup(9) reports a file")
		}
		if _, ok, err := s.Batch(9); ok || err != nil {
			t.Fatalf("Batch(9) = (_, %v, %v), want not found", ok, err)
		}
	}
	check(s)

	reopened, err := openWithdrawalStore(dir, 1)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	check(reopened)

	fi, err := os.Stat(filepath.Join(dir, "3.json"))
	if err != nil {
		t.Fatal(err)
	}
	if fi.Mode().Perm() != 0o600 {
		t.Fatalf("file mode = %v, want 0600", fi.Mode().Perm())
	}
	di, err := os.Stat(dir)
	if err != nil {
		t.Fatal(err)
	}
	if di.Mode().Perm() != 0o700 {
		t.Fatalf("directory mode = %v, want 0700", di.Mode().Perm())
	}
}

func TestWithdrawalStorePutOverwrites(t *testing.T) {
	s := testStore(t, 1)
	b := sampleBatch(t, 1, 2)
	unverified := b
	unverified.Verified, unverified.OutboxRoot = false, types.Hash{}
	if err := s.Put(unverified); err != nil {
		t.Fatal(err)
	}
	if v, _ := s.Lookup(2); v {
		t.Fatalf("batch 2 is verified after an unverified Put")
	}
	if err := s.Put(b); err != nil {
		t.Fatal(err)
	}
	if got := mustBatch(t, s, 2); !got.Verified || got.OutboxRoot != b.OutboxRoot {
		t.Fatalf("batch 2 = %+v, want the verified rewrite", got)
	}
	if v, ok := s.Lookup(2); !v || !ok {
		t.Fatalf("Lookup(2) = (%v, %v), want verified", v, ok)
	}

	other := sampleBatch(t, 2, 3)
	if err := s.Put(other); err == nil {
		t.Fatalf("Put with another DomainID returned nil")
	}
	if _, ok := s.Lookup(3); ok {
		t.Fatalf("a rejected Put recorded batch 3")
	}
}

func TestWithdrawalStoreRemove(t *testing.T) {
	s := testStore(t, 1)
	if err := s.Put(sampleBatch(t, 1, 2)); err != nil {
		t.Fatal(err)
	}
	if err := s.Remove(2); err != nil {
		t.Fatalf("Remove(2): %v", err)
	}
	if _, err := os.Stat(filepath.Join(s.dir, "2.json")); !os.IsNotExist(err) {
		t.Fatalf("file still present: %v", err)
	}
	if len(s.Batches()) != 0 {
		t.Fatalf("Batches = %v after Remove", s.Batches())
	}
	if _, ok, err := s.Batch(2); ok || err != nil {
		t.Fatalf("Batch(2) = (_, %v, %v), want not found", ok, err)
	}
	if err := s.Remove(7); err != nil {
		t.Fatalf("Remove of an absent id = %v, want nil", err)
	}
}

func TestWithdrawalStoreDamage(t *testing.T) {
	good := sampleBatch(t, 1, 7)
	marshal := func(version uint32, b StoredBatch) []byte {
		data, err := json.Marshal(storedFile{Version: version, StoredBatch: b})
		if err != nil {
			t.Fatal(err)
		}
		return data
	}
	mutate := func(f func(b *StoredBatch)) []byte {
		b := good
		b.Withdrawals = slices.Clone(good.Withdrawals)
		for i := range b.Withdrawals {
			b.Withdrawals[i].Proof = slices.Clone(b.Withdrawals[i].Proof)
		}
		f(&b)
		return marshal(withdrawalStoreVersion, b)
	}
	unverified := func(f func(b *StoredBatch)) []byte {
		return mutate(func(b *StoredBatch) {
			b.Verified, b.OutboxRoot = false, types.Hash{}
			f(b)
		})
	}
	proofWith := func(count uint32, side byte) []byte {
		p := make([]byte, 4+33*int(count))
		binary.BigEndian.PutUint32(p, count)
		for i := 0; i < int(count); i++ {
			p[4+33*i] = side
		}
		return p
	}
	valid := marshal(withdrawalStoreVersion, good)

	cases := []struct {
		name string
		data []byte
	}{
		{"not json", []byte("not json")},
		{"truncated json", valid[:len(valid)/2]},
		{"version 2", marshal(2, good)},
		{"no version", marshal(0, good)},
		{"domainId 2", mutate(func(b *StoredBatch) { b.DomainID = 2 })},
		{"batchId 8", mutate(func(b *StoredBatch) { b.BatchID = 8 })},
		{"undecodable message", mutate(func(b *StoredBatch) { b.Withdrawals[0].Message = []byte{1, 2} })},
		{"kind 0 message", mutate(func(b *StoredBatch) {
			m, err := kind0msg(1, 0).Encode()
			if err != nil {
				t.Fatal(err)
			}
			b.Withdrawals[0].Message = m
		})},
		{"flipped proof byte", mutate(func(b *StoredBatch) { b.Withdrawals[1].Proof[len(b.Withdrawals[1].Proof)-1] ^= 1 })},
		{"entries reversed", mutate(func(b *StoredBatch) {
			b.Withdrawals[0], b.Withdrawals[1] = b.Withdrawals[1], b.Withdrawals[0]
		})},
		{"repeated entry", mutate(func(b *StoredBatch) { b.Withdrawals[1] = b.Withdrawals[0] })},
		{"unverified non-zero root", mutate(func(b *StoredBatch) { b.Verified = false })},
		{"unverified proof count 33", unverified(func(b *StoredBatch) { b.Withdrawals[0].Proof = proofWith(33, 0) })},
		{"unverified side byte 2", unverified(func(b *StoredBatch) { b.Withdrawals[0].Proof = proofWith(1, 2) })},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dir := filepath.Join(t.TempDir(), "withdrawals")
			if err := os.MkdirAll(dir, 0o700); err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(dir, "7.json")
			if err := os.WriteFile(path, tc.data, 0o600); err != nil {
				t.Fatal(err)
			}
			_, err := openWithdrawalStore(dir, 1)
			if err == nil || !strings.Contains(err.Error(), path) || !strings.Contains(err.Error(), "damaged") {
				t.Fatalf("open = %v, want a damaged-file error naming %s", err, path)
			}
		})
	}

	t.Run("damaged after open", func(t *testing.T) {
		s := testStore(t, 1)
		if err := s.Put(good); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(s.dir, "7.json"), []byte("{"), 0o600); err != nil {
			t.Fatal(err)
		}
		if _, _, err := s.Batch(7); err == nil || !strings.Contains(err.Error(), "damaged") {
			t.Fatalf("Batch = %v, want a damaged-file error", err)
		}
	})
}

func TestWithdrawalStoreOpenCleanup(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "withdrawals")
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{".tmp-abc", "notes.txt", "07.json"} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte("x"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	logs := captureLog(t)
	s, err := openWithdrawalStore(dir, 1)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, ".tmp-abc")); !os.IsNotExist(err) {
		t.Fatalf(".tmp-abc still present: %v", err)
	}
	if len(s.Batches()) != 0 {
		t.Fatalf("Batches = %v, want none", s.Batches())
	}
	out := logs.String()
	for _, name := range []string{"notes.txt", "07.json"} {
		if want := "supervisor: WARN: withdrawal store " + dir + ": ignoring " + name; !strings.Contains(out, want) {
			t.Fatalf("log lacks %q:\n%s", want, out)
		}
	}
	if strings.Contains(out, ".tmp-abc") {
		t.Fatalf("log mentions the temporary file:\n%s", out)
	}
	if _, err := os.Stat(filepath.Join(dir, "07.json")); err != nil {
		t.Fatalf("07.json was touched: %v", err)
	}
}

// --- checkTotals ---

func TestCheckTotals(t *testing.T) {
	a, b := assetN(1), assetN(2)
	max := codec.U256{}
	for i := range max {
		max[i] = 0xFF
	}
	maxBig := new(big.Int).SetBytes(max[:])
	sum := func(n int64) *big.Int { return big.NewInt(n) }
	entry := func(asset codec.AssetID, debit codec.U256) codec.AssetFlowEntry {
		return codec.AssetFlowEntry{Asset: asset, WithdrawalDebit: debit}
	}

	cases := []struct {
		name    string
		sums    map[codec.AssetID]*big.Int
		summary codec.AssetFlowSummary
		wantErr bool
	}{
		{"equal", map[codec.AssetID]*big.Int{a: sum(8), b: sum(3)}, codec.AssetFlowSummary{entry(a, u256Of(8)), entry(b, u256Of(3))}, false},
		{"summary asset with no withdrawal", map[codec.AssetID]*big.Int{}, codec.AssetFlowSummary{entry(a, u256Of(8))}, true},
		{"extra non-zero asset", map[codec.AssetID]*big.Int{a: sum(8), b: sum(1)}, codec.AssetFlowSummary{entry(a, u256Of(8))}, true},
		{"extra zero asset", map[codec.AssetID]*big.Int{a: sum(8), b: sum(0)}, codec.AssetFlowSummary{entry(a, u256Of(8))}, false},
		{"wrong sum", map[codec.AssetID]*big.Int{a: sum(7)}, codec.AssetFlowSummary{entry(a, u256Of(8))}, true},
		{"two maximum amounts against a maximum debit", map[codec.AssetID]*big.Int{a: new(big.Int).Mul(maxBig, sum(2))}, codec.AssetFlowSummary{entry(a, max)}, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := checkTotals(tc.sums, tc.summary)
			if (err != nil) != tc.wantErr {
				t.Fatalf("checkTotals = %v, want error %v", err, tc.wantErr)
			}
		})
	}
}
