package zenoexec

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"testing"

	"github.com/zenon-network/go-zenon/common/types"

	"github.com/Zeno-L2/zeno-executor/internal/codec"
	"github.com/Zeno-L2/zeno-executor/plugins/wasm"
	"github.com/Zeno-L2/zeno-executor/plugins/wasm/internal/keys"
)

// --- module fixtures, copied from the wasm package's tests because
// test helpers do not cross packages.

var wasmHeader = []byte{0x00, 'a', 's', 'm', 0x01, 0x00, 0x00, 0x00}

func section(id byte, payload []byte) []byte {
	out := []byte{id}
	out = appendVarU32(out, uint32(len(payload)))
	return append(out, payload...)
}

func appendVarU32(dst []byte, v uint32) []byte {
	for {
		b := byte(v & 0x7F)
		v >>= 7
		if v != 0 {
			dst = append(dst, b|0x80)
			continue
		}
		return append(dst, b)
	}
}

func appendVarS64(dst []byte, v int64) []byte {
	for {
		b := byte(v & 0x7F)
		v >>= 7
		signBitSet := b&0x40 != 0
		if (v == 0 && !signBitSet) || (v == -1 && signBitSet) {
			return append(dst, b)
		}
		dst = append(dst, b|0x80)
	}
}

func appendName(dst []byte, s string) []byte {
	dst = appendVarU32(dst, uint32(len(s)))
	return append(dst, s...)
}

// buildModule builds a module exporting memory and run, a function of
// type (i32,i32)->i64, with an optional active data segment at offset.
func buildModule(data []byte, offset uint32, body []byte, pages uint32) []byte {
	out := append([]byte{}, wasmHeader...)
	runType := []byte{0x60, 0x02, 0x7F, 0x7F, 0x01, 0x7E}
	out = append(out, section(1, append([]byte{0x01}, runType...))...)
	out = append(out, section(3, []byte{0x01, 0x00})...)
	out = append(out, section(5, appendVarU32([]byte{0x01, 0x00}, pages))...)
	exportSec := []byte{0x02}
	exportSec = appendName(exportSec, "memory")
	exportSec = append(exportSec, 0x02, 0x00)
	exportSec = appendName(exportSec, "run")
	exportSec = append(exportSec, 0x00, 0x00)
	out = append(out, section(7, exportSec)...)
	fbody := append([]byte{0x00}, body...)
	codeSec := appendVarU32([]byte{0x01}, uint32(len(fbody)))
	codeSec = append(codeSec, fbody...)
	out = append(out, section(10, codeSec)...)
	if data != nil {
		dp := []byte{0x01, 0x00, 0x41}
		dp = appendVarU32(dp, offset)
		dp = append(dp, 0x0B)
		dp = appendVarU32(dp, uint32(len(data)))
		dp = append(dp, data...)
		out = append(out, section(11, dp)...)
	}
	return out
}

// effectsModule returns a module whose run returns the packed
// (offset, length) of blob, a ContractEffects encoding in its data
// segment.
func effectsModule(blob []byte) []byte {
	const offset = 1024
	body := appendVarS64([]byte{0x42}, (int64(offset)<<32)|int64(len(blob)))
	body = append(body, 0x0B)
	return buildModule(blob, offset, body, 1)
}

// noopModule is a valid module whose run returns 0.
func noopModule() []byte {
	return buildModule(nil, 0, []byte{0x42, 0x00, 0x0B}, 1)
}

func u256Of(v uint64) codec.U256 {
	var out codec.U256
	binary.BigEndian.PutUint64(out[24:], v)
	return out
}

// --- helpers.

var (
	senderAddr = func() types.Address {
		var a types.Address
		for i := 1; i < len(a); i++ {
			a[i] = 0xA1
		}
		return a
	}()
	senderHex    = "00" + strings.Repeat("a1", 19)
	recipientHex = "00" + strings.Repeat("c3", 19)
	zeroHash     = strings.Repeat("0", 64)
)

func testAssetHex() string { return strings.Repeat("00", 22) + strings.Repeat("01", 10) }

func newSession(t *testing.T) *Session {
	t.Helper()
	s, err := NewSession("testnet")
	if err != nil {
		t.Fatalf("NewSession: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

// exec runs one command line and returns the result's JSON.
func exec(t *testing.T, s *Session, line string) string {
	t.Helper()
	b, err := json.Marshal(s.Exec([]byte(line)))
	if err != nil {
		t.Fatalf("marshal result: %v", err)
	}
	return string(b)
}

func decode(t *testing.T, s string) map[string]any {
	t.Helper()
	var m map[string]any
	if err := json.Unmarshal([]byte(s), &m); err != nil {
		t.Fatalf("result %q: %v", s, err)
	}
	return m
}

func status(t *testing.T, res string) string {
	t.Helper()
	m := decode(t, res)
	r, ok := m["receipt"].(map[string]any)
	if !ok {
		t.Fatalf("no receipt in %s", res)
	}
	return r["status"].(string)
}

func errKind(t *testing.T, res string) string {
	t.Helper()
	m := decode(t, res)
	e, ok := m["error"].(map[string]any)
	if !ok {
		t.Fatalf("no error in %s", res)
	}
	return e["kind"].(string)
}

func deployLine(module []byte) string {
	return fmt.Sprintf(`{"op":"deploy","sender":%q,"module":%q,"metadata":"immutable"}`, senderHex, hex.EncodeToString(module))
}

// --- tests.

func TestSessionCallResult(t *testing.T) {
	// The contract id is SHA3-256(sender || u64be(3) || chainID): the
	// finalize is input 3.
	var idx [8]byte
	binary.BigEndian.PutUint64(idx[:], 3)
	chain := keys.ChainID("testnet")
	id := sha3(senderAddr[:], idx[:], chain[:])

	asset := testAsset()
	topic := types.Hash{0x11}
	blob, err := codec.ContractEffects{
		Version:          codec.ExecutionResultVersion,
		GlobalInputIndex: 5,
		StateDiff:        []codec.LocalStateDiffEntry{{LocalKey: []byte("k")}},
		Events:           []codec.Event{{ContractID: id, InputIndex: 5, EventIndex: 0, Topics: []types.Hash{topic}, Data: []byte{0xde, 0xad}}},
		Outbox: []codec.OutboxMessage{{
			SourceContract: id,
			InputIndex:     5,
			OutboxIndex:    0,
			Target:         codec.L1Withdrawal{RecipientL1: mustAddr(t, recipientHex), Asset: asset, Amount: u256Of(7)},
		}},
		ClaimedDeposit: u256Of(400),
	}.Encode()
	if err != nil {
		t.Fatalf("encode effects: %v", err)
	}
	raw := effectsModule(blob)
	chk, err := wasm.CheckModule(raw, nil)
	if err != nil {
		t.Fatalf("CheckModule: %v", err)
	}

	s := newSession(t)
	dep := exec(t, s, deployLine(raw))
	if got := status(t, dep); got != "SUCCESS" {
		t.Fatalf("deploy: %s", dep)
	}
	dm := decode(t, dep)
	if dm["contractId"] != id.String() || dm["codeHash"] != chk.CodeHash.String() {
		t.Fatalf("deploy top-level ids = %v %v, want %s %s", dm["contractId"], dm["codeHash"], id, chk.CodeHash)
	}
	if got := status(t, exec(t, s, fmt.Sprintf(`{"op":"buyStorage","sender":%q,"contractId":%q,"bytes":4096}`, senderHex, id))); got != "SUCCESS" {
		t.Fatalf("buyStorage status = %s", got)
	}
	if got := exec(t, s, fmt.Sprintf(`{"op":"setStorage","contractId":%q,"localKey":"6b","value":%q}`, id, strings.Repeat("ab", 32))); got != `{"ok":true}` {
		t.Fatalf("setStorage = %s", got)
	}

	res := exec(t, s, fmt.Sprintf(`{"op":"call","sender":%q,"contractId":%q,"payload":"","deposit":{"asset":%q,"amount":"1000"}}`, senderHex, id, zeroHash))
	gas := decode(t, res)["receipt"].(map[string]any)["gasUsed"].(float64)

	leaf := keys.Derive(id, []byte("k"))
	quota := keys.Derive(id, keys.Quota)
	quotaLeaf := make([]byte, 16)
	binary.BigEndian.PutUint64(quotaLeaf[:8], 4096)
	diff := []struct {
		path  types.Hash
		value string
	}{
		{types.Hash(leaf), "null"},
		{types.Hash(quota), fmt.Sprintf("%q", hex.EncodeToString(quotaLeaf))},
	}
	sort.Slice(diff, func(i, j int) bool { return bytes.Compare(diff[i].path[:], diff[j].path[:]) < 0 })
	var diffJSON []string
	for _, d := range diff {
		diffJSON = append(diffJSON, fmt.Sprintf(`{"path":"%s","value":%s}`, d.path, d.value))
	}

	want := fmt.Sprintf(`{"ok":true,"receipt":{"status":"SUCCESS","gasUsed":%d,"abortCode":0,"contractId":"%s","codeHash":"%s","returnData":"","events":[{"contractId":"%s","inputIndex":5,"eventIndex":0,"topics":["%s"],"data":"dead"}],"outbox":[`+
		`{"sourceContract":"%s","inputIndex":5,"outboxIndex":0,"kind":"L1_WITHDRAWAL","targetContract":null,"recipient":"%s","assetId":"%s","amount":"7","payload":"","outboxId":null},`+
		`{"sourceContract":"%s","inputIndex":5,"outboxIndex":1,"kind":"L1_WITHDRAWAL","targetContract":null,"recipient":"%s","assetId":"%s","amount":"600","payload":"","outboxId":null}],`+
		`"stateDiff":[%s],"claimedDeposit":"400"}}`,
		uint64(gas), id, chk.InstrumentedCodeHash, id, topic,
		id, mustAddr(t, recipientHex), testAssetHex(),
		zeroHash, senderAddr, zeroHash,
		strings.Join(diffJSON, ","))
	if res != want {
		t.Fatalf("call result\n got %s\nwant %s", res, want)
	}

	if got := exec(t, s, fmt.Sprintf(`{"op":"getStorage","contractId":%q,"localKey":"6b"}`, id)); got != `{"ok":true,"value":null}` {
		t.Fatalf("getStorage after delete = %s", got)
	}
}

func mustAddr(t *testing.T, s string) types.Address {
	t.Helper()
	a, err := ParseAddress(s)
	if err != nil {
		t.Fatalf("ParseAddress(%q): %v", s, err)
	}
	return a
}

func TestSessionHaltKeepsGoing(t *testing.T) {
	s := newSession(t)
	dep := decode(t, exec(t, s, deployLine(noopModule())))
	id := dep["contractId"].(string)

	res := exec(t, s, fmt.Sprintf(`{"op":"call","sender":%q,"contractId":%q,"payload":"","plasmaLimit":1000000001}`, senderHex, id))
	if errKind(t, res) != "halt" {
		t.Fatalf("over-limit call = %s, want halt", res)
	}
	if got := exec(t, s, fmt.Sprintf(`{"op":"getStorage","contractId":%q,"localKey":"6b"}`, id)); got != `{"ok":true,"value":null}` {
		t.Fatalf("getStorage = %s", got)
	}
}

func TestSessionStorageSeeding(t *testing.T) {
	s := newSession(t)
	id := strings.Repeat("07", 32)
	set := func(key, value string) string {
		return exec(t, s, fmt.Sprintf(`{"op":"setStorage","contractId":%q,"localKey":%q,"value":%s}`, id, key, value))
	}

	if got := set("6b", `""`); got != `{"ok":true}` {
		t.Fatalf("setStorage = %s", got)
	}
	if got := exec(t, s, fmt.Sprintf(`{"op":"getStorage","contractId":%q,"localKey":"6b"}`, id)); got != `{"ok":true,"value":""}` {
		t.Fatalf("present-empty getStorage = %s", got)
	}
	h, _ := hex.DecodeString(id)
	var cid types.Hash
	copy(cid[:], h)
	quota := s.state.values[keys.Derive(cid, keys.Quota)]
	want := make([]byte, 16)
	binary.BigEndian.PutUint64(want[:8], wasm.StateLeafOverhead)
	binary.BigEndian.PutUint64(want[8:], wasm.StateLeafOverhead)
	if !bytes.Equal(quota, want) {
		t.Fatalf("quota leaf = %x, want %x", quota, want)
	}

	// A null value on an absent leaf has a zero delta and writes nothing.
	before := len(s.state.values)
	if got := set("6c", "null"); got != `{"ok":true}` || len(s.state.values) != before {
		t.Fatalf("null on absent leaf: %s, leaves %d -> %d", got, before, len(s.state.values))
	}

	// Deleting the seeded leaf frees its bytes.
	if got := set("6b", "null"); got != `{"ok":true}` {
		t.Fatalf("delete = %s", got)
	}
	quota = s.state.values[keys.Derive(cid, keys.Quota)]
	if binary.BigEndian.Uint64(quota[8:]) != 0 {
		t.Fatalf("used after delete = %x, want 0", quota[8:])
	}

	// Rejected writes leave the quota leaf alone.
	for _, key := range []string{"0001", "0005", strings.Repeat("aa", 33)} {
		if got := set(key, `"00"`); errKind(t, got) != "bad_command" {
			t.Fatalf("setStorage key %s = %s, want bad_command", key, got)
		}
	}
	s.state.values[keys.Derive(cid, []byte{0x01})] = []byte{1}
	if got := set("01", "null"); errKind(t, got) != "bad_command" {
		t.Fatalf("delete beyond used = %s, want bad_command", got)
	}
	if _, ok := s.state.values[keys.Derive(cid, []byte{0x01})]; !ok {
		t.Fatal("rejected delete removed the leaf")
	}
}

func TestSessionBadCommands(t *testing.T) {
	s := newSession(t)
	id := strings.Repeat("07", 32)
	cases := map[string]string{
		"UnknownOp":      `{"op":"nope"}`,
		"UnknownMember":  fmt.Sprintf(`{"op":"getStorage","contractId":%q,"localKey":"6b","extra":1}`, id),
		"MissingMember":  fmt.Sprintf(`{"op":"getStorage","contractId":%q}`, id),
		"NullMember":     fmt.Sprintf(`{"op":"getStorage","contractId":%q,"localKey":null}`, id),
		"OddHex":         fmt.Sprintf(`{"op":"getStorage","contractId":%q,"localKey":"6"}`, id),
		"UppercaseHex":   fmt.Sprintf(`{"op":"getStorage","contractId":%q,"localKey":"6B"}`, id),
		"ShortContract":  fmt.Sprintf(`{"op":"getStorage","contractId":%q,"localKey":"6b"}`, id[:63]),
		"LongKey":        fmt.Sprintf(`{"op":"setStorage","contractId":%q,"localKey":%q,"value":"00"}`, id, strings.Repeat("aa", 33)),
		"NotAnObject":    `[1]`,
		"Blank":          ``,
		"BadAmount":      fmt.Sprintf(`{"op":"deposit","sender":%q,"contractId":%q,"asset":%q,"amount":"01"}`, senderHex, id, zeroHash),
		"ZeroBuyStorage": fmt.Sprintf(`{"op":"buyStorage","sender":%q,"contractId":%q,"bytes":0}`, senderHex, id),
	}
	for name, line := range cases {
		t.Run(name, func(t *testing.T) {
			if got := exec(t, s, line); errKind(t, got) != "bad_command" {
				t.Fatalf("%s = %s, want bad_command", line, got)
			}
		})
	}
}

func TestSessionDeployValidationFailed(t *testing.T) {
	custom := append(noopModule(), section(0, appendName(nil, "x"))...)
	s := newSession(t)
	res := exec(t, s, deployLine(custom))
	if got := status(t, res); got != "VALIDATION_FAILED" {
		t.Fatalf("status = %s in %s", got, res)
	}
	m := decode(t, res)
	if _, ok := m["contractId"]; ok {
		t.Fatalf("top-level contractId present: %s", res)
	}
	if _, ok := m["codeHash"]; ok {
		t.Fatalf("top-level codeHash present: %s", res)
	}
}

func TestParseAddress(t *testing.T) {
	bech := senderAddr.String()
	good := map[string]types.Address{
		bech:      senderAddr,
		senderHex: senderAddr,
	}
	for in, want := range good {
		got, err := ParseAddress(in)
		if err != nil || got != want {
			t.Errorf("ParseAddress(%q) = %v, %v", in, got, err)
		}
	}
	for _, in := range []string{senderHex[:39], strings.ToUpper(senderHex), "0x" + senderHex[:38], ""} {
		if _, err := ParseAddress(in); err == nil {
			t.Errorf("ParseAddress(%q) succeeded", in)
		}
	}
}

func testAsset() codec.AssetID {
	var a codec.AssetID
	for i := 22; i < 32; i++ {
		a[i] = 0x01
	}
	return a
}

func TestSessionDeployBoundsReachFinalize(t *testing.T) {
	for name, module := range map[string][]byte{
		"Empty":     {},
		"Oversized": make([]byte, wasm.MaxCodeSize+1),
	} {
		t.Run(name, func(t *testing.T) {
			s := newSession(t)
			res := exec(t, s, deployLine(module))
			if got := status(t, res); got != "VALIDATION_FAILED" {
				t.Fatalf("result = %s, want VALIDATION_FAILED", res)
			}
		})
	}
}

func TestSessionSetStorageZeroDeltaWritesNoQuotaLeaf(t *testing.T) {
	s := newSession(t)
	id := strings.Repeat("08", 32)
	set := func(key, value string) string {
		return exec(t, s, fmt.Sprintf(`{"op":"setStorage","contractId":%q,"localKey":%q,"value":%s}`, id, key, value))
	}

	if got := set("6c", "null"); got != `{"ok":true}` || len(s.state.values) != 0 {
		t.Fatalf("null on absent leaf with no quota leaf: %s, leaves %d, want 0", got, len(s.state.values))
	}

	h, _ := hex.DecodeString(id)
	var cid types.Hash
	copy(cid[:], h)
	quotaPath := keys.Derive(cid, keys.Quota)
	s.state.values[quotaPath] = bytes.Repeat([]byte{0xff}, 16)
	s.state.values[keys.Derive(cid, []byte{0x6d})] = []byte{1}
	if got := set("6d", `"02"`); got != `{"ok":true}` {
		t.Fatalf("same-size overwrite = %s", got)
	}
	if !bytes.Equal(s.state.values[quotaPath], bytes.Repeat([]byte{0xff}, 16)) {
		t.Fatalf("quota leaf = %x after a same-size overwrite, want unchanged", s.state.values[quotaPath])
	}
}

// loopModule is a valid module whose run never returns.
func loopModule() []byte {
	return buildModule(nil, 0, []byte{0x03, 0x40, 0x0C, 0x00, 0x0B, 0x42, 0x00, 0x0B}, 1)
}

func TestSessionGasBudget(t *testing.T) {
	s := newSession(t)
	id := decode(t, exec(t, s, deployLine(loopModule())))["contractId"].(string)
	call := func(limit uint64) string {
		return exec(t, s, fmt.Sprintf(`{"op":"call","sender":%q,"contractId":%q,"payload":"","plasmaLimit":%d}`, senderHex, id, limit))
	}

	s.budgeted, s.gasLeft = true, 1_000_000
	if got := call(1_000_001); errKind(t, got) != "bad_command" {
		t.Fatalf("call over budget = %s, want bad_command", got)
	}
	if got := status(t, call(600_000)); got != "OUT_OF_GAS" {
		t.Fatalf("loop status = %s, want OUT_OF_GAS", got)
	}
	if s.gasLeft != 400_000 {
		t.Fatalf("gasLeft = %d, want 400000", s.gasLeft)
	}
	if got := call(400_001); errKind(t, got) != "bad_command" {
		t.Fatalf("call over the remaining budget = %s, want bad_command", got)
	}
	call(400_000)
	if got := call(1); errKind(t, got) != "bad_command" {
		t.Fatalf("call on an empty budget = %s, want bad_command", got)
	}
}

func TestSessionWithoutBudgetAcceptsLargeLimit(t *testing.T) {
	s := newSession(t)
	id := decode(t, exec(t, s, deployLine(noopModule())))["contractId"].(string)
	got := exec(t, s, fmt.Sprintf(`{"op":"call","sender":%q,"contractId":%q,"payload":"","plasmaLimit":900000000}`, senderHex, id))
	if strings.Contains(got, `"error"`) {
		t.Fatalf("call with a 900M plasmaLimit = %s, want a receipt", got)
	}
}

func TestSessionUpgradeAndTransferOwnership(t *testing.T) {
	newOwnerHex := "00" + strings.Repeat("b2", 19)
	second := buildModule(nil, 0, []byte{0x42, 0x01, 0x0B}, 1)
	s := newSession(t)

	dep := exec(t, s, fmt.Sprintf(`{"op":"deploy","sender":%q,"module":%q,"metadata":{"owner":%q}}`,
		senderHex, hex.EncodeToString(noopModule()), senderHex))
	if got := status(t, dep); got != "SUCCESS" {
		t.Fatalf("deploy: %s", dep)
	}
	id := decode(t, dep)["contractId"].(string)

	transfer := exec(t, s, fmt.Sprintf(`{"op":"transferOwnership","sender":%q,"contractId":%q,"newOwner":%q}`, senderHex, id, newOwnerHex))
	if got := status(t, transfer); got != "SUCCESS" {
		t.Fatalf("transferOwnership: %s", transfer)
	}
	upgrade := func(sender, owner string) string {
		return exec(t, s, fmt.Sprintf(`{"op":"upgrade","sender":%q,"contractId":%q,"module":%q,"metadata":{"owner":%q}}`,
			sender, id, hex.EncodeToString(second), owner))
	}
	if got := status(t, upgrade(senderHex, senderHex)); got != "UNAUTHORIZED" {
		t.Fatalf("upgrade by the old owner status = %s, want UNAUTHORIZED", got)
	}
	res := upgrade(newOwnerHex, newOwnerHex)
	if got := status(t, res); got != "SUCCESS" {
		t.Fatalf("upgrade by the new owner: %s", res)
	}
	chk, err := wasm.CheckModule(second, nil)
	if err != nil {
		t.Fatal(err)
	}
	receipt := decode(t, res)["receipt"].(map[string]any)
	if receipt["codeHash"] != chk.InstrumentedCodeHash.String() {
		t.Fatalf("upgrade codeHash = %v, want %s", receipt["codeHash"], chk.InstrumentedCodeHash)
	}
}

func TestSessionRaisesPurchasedOnExistingQuota(t *testing.T) {
	s := newSession(t)
	cid := types.Hash{7}
	s.state.values[keys.Derive(cid, keys.Quota)] = make([]byte, 16)

	line := fmt.Sprintf(`{"op":"setStorage","contractId":%q,"localKey":"6b","value":"01020304"}`, cid)
	if got := exec(t, s, line); got != `{"ok":true}` {
		t.Fatalf("setStorage = %s", got)
	}
	want := make([]byte, 16)
	binary.BigEndian.PutUint64(want[:8], wasm.StateLeafOverhead+4)
	binary.BigEndian.PutUint64(want[8:], wasm.StateLeafOverhead+4)
	if got := s.state.values[keys.Derive(cid, keys.Quota)]; !bytes.Equal(got, want) {
		t.Fatalf("quota leaf = %x, want %x", got, want)
	}
}
