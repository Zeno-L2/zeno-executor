package zenoexec

import (
	"bufio"
	"bytes"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/big"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"github.com/zenon-network/go-zenon/common/crypto"
	"github.com/zenon-network/go-zenon/common/types"

	"github.com/Zeno-L2/zeno-executor/domain"
	"github.com/Zeno-L2/zeno-executor/internal/codec"
	"github.com/Zeno-L2/zeno-executor/plugins/wasm"
)

// maxLineSize is the longest command line Run reads.
const maxLineSize = 4 << 20

// ErrLineTooLong is returned by Run when a command line exceeds
// maxLineSize.
var ErrLineTooLong = errors.New("zenoexec: line exceeds 4 MiB")

var decimalRE = regexp.MustCompile(`^(0|[1-9][0-9]*)$`)

// --- result values.

// hexBytes encodes as a lowercase hex JSON string.
type hexBytes []byte

func (b hexBytes) MarshalText() ([]byte, error) { return []byte(hex.EncodeToString(b)), nil }

type errorBody struct {
	Kind    string `json:"kind"`
	Message string `json:"message"`
}

type errorResult struct {
	OK    bool      `json:"ok"`
	Error errorBody `json:"error"`
}

func failure(kind, format string, args ...any) errorResult {
	return errorResult{Error: errorBody{Kind: kind, Message: fmt.Sprintf(format, args...)}}
}

type okResult struct {
	OK bool `json:"ok"`
}

type valueResult struct {
	OK    bool      `json:"ok"`
	Value *hexBytes `json:"value"`
}

type receiptResult struct {
	OK         bool        `json:"ok"`
	Receipt    receiptJSON `json:"receipt"`
	ContractID *types.Hash `json:"contractId,omitempty"`
	CodeHash   *types.Hash `json:"codeHash,omitempty"`
}

type receiptJSON struct {
	Status         string          `json:"status"`
	GasUsed        uint64          `json:"gasUsed"`
	AbortCode      int32           `json:"abortCode"`
	ContractID     types.Hash      `json:"contractId"`
	CodeHash       types.Hash      `json:"codeHash"`
	ReturnData     hexBytes        `json:"returnData"`
	Events         []eventJSON     `json:"events"`
	Outbox         []outboxJSON    `json:"outbox"`
	StateDiff      []stateDiffJSON `json:"stateDiff"`
	ClaimedDeposit string          `json:"claimedDeposit"`
}

type eventJSON struct {
	ContractID types.Hash   `json:"contractId"`
	InputIndex uint64       `json:"inputIndex"`
	EventIndex uint32       `json:"eventIndex"`
	Topics     []types.Hash `json:"topics"`
	Data       hexBytes     `json:"data"`
}

type outboxJSON struct {
	SourceContract types.Hash     `json:"sourceContract"`
	InputIndex     uint64         `json:"inputIndex"`
	OutboxIndex    uint32         `json:"outboxIndex"`
	Kind           string         `json:"kind"`
	TargetContract *types.Hash    `json:"targetContract"`
	Recipient      *types.Address `json:"recipient"`
	AssetID        *hexBytes      `json:"assetId"`
	Amount         *string        `json:"amount"`
	Payload        hexBytes       `json:"payload"`
	OutboxID       *types.Hash    `json:"outboxId"` // always null: no batch holds the message
}

type stateDiffJSON struct {
	Path  types.Hash `json:"path"`
	Value *hexBytes  `json:"value"` // null deletes; "" is present-empty
}

// receiptOf builds the JSON form of eff, with every array non-nil.
func receiptOf(eff domain.Effects) (receiptJSON, error) {
	r := receiptJSON{
		Status:         eff.Status.String(),
		GasUsed:        eff.GasUsed,
		AbortCode:      eff.AbortCode,
		ContractID:     eff.ContractID,
		CodeHash:       eff.CodeHash,
		ReturnData:     eff.ReturnData,
		Events:         make([]eventJSON, 0, len(eff.Events)),
		Outbox:         make([]outboxJSON, 0, len(eff.OutboxMessages)),
		StateDiff:      make([]stateDiffJSON, 0, len(eff.StateDiff)),
		ClaimedDeposit: new(big.Int).SetBytes(eff.ClaimedDeposit[:]).String(),
	}
	for _, ev := range eff.Events {
		topics := ev.Topics
		if topics == nil {
			topics = []types.Hash{}
		}
		r.Events = append(r.Events, eventJSON{
			ContractID: ev.ContractID,
			InputIndex: ev.InputIndex,
			EventIndex: ev.EventIndex,
			Topics:     topics,
			Data:       ev.Data,
		})
	}
	for _, m := range eff.OutboxMessages {
		o := outboxJSON{
			SourceContract: m.SourceContract,
			InputIndex:     m.InputIndex,
			OutboxIndex:    m.OutboxIndex,
			Payload:        m.Payload,
		}
		switch t := m.Target.(type) {
		case codec.L2Delivery:
			o.Kind = "L2_DELIVERY"
			o.TargetContract = &t.TargetContract
		case codec.L1Withdrawal:
			o.Kind = "L1_WITHDRAWAL"
			asset := hexBytes(t.Asset[:])
			amount := new(big.Int).SetBytes(t.Amount[:]).String()
			o.Recipient, o.AssetID, o.Amount = &t.RecipientL1, &asset, &amount
		default:
			return receiptJSON{}, fmt.Errorf("outbox message of unknown kind %d", m.Target.Kind())
		}
		r.Outbox = append(r.Outbox, o)
	}
	for _, e := range eff.StateDiff {
		d := stateDiffJSON{Path: types.Hash(e.Path)}
		if e.Value != nil {
			v := hexBytes(e.Value)
			d.Value = &v
		}
		r.StateDiff = append(r.StateDiff, d)
	}
	return r, nil
}

// --- value parsing.

// lowerHex decodes lowercase hex without a prefix. An empty string is
// valid.
func lowerHex(s string) ([]byte, error) {
	if len(s)%2 != 0 {
		return nil, errors.New("odd number of hex digits")
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return nil, errors.New("hex must be lowercase digits 0-9 and a-f, with no 0x prefix")
		}
	}
	return hex.DecodeString(s)
}

// ParseAddress parses a "z1..." bech32 address or exactly 40 lowercase
// hex digits.
func ParseAddress(s string) (types.Address, error) {
	if strings.HasPrefix(s, "z1") {
		return types.ParseAddress(s)
	}
	if len(s) != 2*types.AddressSize {
		return types.Address{}, fmt.Errorf("address must be z1 bech32 or %d lowercase hex digits", 2*types.AddressSize)
	}
	b, err := lowerHex(s)
	if err != nil {
		return types.Address{}, err
	}
	var a types.Address
	copy(a[:], b)
	return a, nil
}

// encodeMetadata is the canonical ContractMetadata encoding of a
// policy and owner.
func encodeMetadata(policy uint8, owner types.Address) []byte {
	return codec.ContractMetadata{Version: codec.ContractMetadataVersion, UpgradePolicy: policy, Owner: owner}.Encode()
}

// ParseMetadataFlag encodes "immutable" or "owner:<address>". The zero
// owner is refused, because finalize rejects it.
func ParseMetadataFlag(s string) ([]byte, error) {
	if s == "immutable" {
		return encodeMetadata(codec.PolicyImmutable, types.Address{}), nil
	}
	rest, ok := strings.CutPrefix(s, "owner:")
	if !ok {
		return nil, fmt.Errorf("metadata must be immutable or owner:<address>, got %q", s)
	}
	owner, err := ParseAddress(rest)
	if err != nil {
		return nil, fmt.Errorf("metadata owner: %w", err)
	}
	if owner.IsZero() {
		return nil, errors.New("metadata owner must not be the zero address")
	}
	return encodeMetadata(codec.PolicyOwnerUpgradeable, owner), nil
}

// --- validate.

type validateOK struct {
	OK                   bool        `json:"ok"`
	CodeHash             types.Hash  `json:"codeHash"`
	InstrumentedCodeHash types.Hash  `json:"instrumentedCodeHash"`
	Size                 int         `json:"size"`
	InstrumentedSize     int         `json:"instrumentedSize"`
	InitialPages         uint32      `json:"initialPages"`
	Metadata             *hexBytes   `json:"metadata,omitempty"`
	MetadataHash         *types.Hash `json:"metadataHash,omitempty"`
}

// Validate runs the deploy-time module checks. ok is false when the
// module or the metadata is refused, and result is then the error
// result.
func Validate(raw, metadata []byte) (result any, ok bool) {
	chk, err := wasm.CheckModule(raw, metadata)
	if err != nil {
		return errorResult{Error: errorBody{Kind: Kind(err), Message: err.Error()}}, false
	}
	res := validateOK{
		OK:                   true,
		CodeHash:             chk.CodeHash,
		InstrumentedCodeHash: chk.InstrumentedCodeHash,
		Size:                 chk.Size,
		InstrumentedSize:     chk.InstrumentedSize,
		InitialPages:         chk.InitialPages,
	}
	if metadata != nil {
		m := hexBytes(metadata)
		res.Metadata, res.MetadataHash = &m, &chk.MetadataHash
	}
	return res, true
}

// --- run.

// Run executes one command per input line in a fresh session, writes
// one result line per command, and returns at end of input. A line over
// 4 MiB writes a bad_command result and returns ErrLineTooLong.
func Run(network string, in io.Reader, out io.Writer) error {
	return RunWithBudget(network, 0, in, out)
}

// RunWithBudget is Run with a gas budget: a call or deposit whose
// plasmaLimit exceeds the gas left is a bad_command, and each receipt's
// gasUsed draws the gas left down. A gasBudget of 0 means no limit.
func RunWithBudget(network string, gasBudget uint64, in io.Reader, out io.Writer) error {
	s, err := NewSession(network)
	if err != nil {
		return err
	}
	defer s.Close()
	if gasBudget > 0 {
		s.budgeted, s.gasLeft = true, gasBudget
	}

	enc := json.NewEncoder(out)
	flush := func() error {
		if f, ok := out.(interface{ Flush() error }); ok {
			return f.Flush()
		}
		return nil
	}
	sc := bufio.NewScanner(in)
	sc.Buffer(make([]byte, 0, 64<<10), maxLineSize)
	for sc.Scan() {
		if err := enc.Encode(s.Exec(sc.Bytes())); err != nil {
			return err
		}
		if err := flush(); err != nil {
			return err
		}
	}
	if err := sc.Err(); err != nil {
		if errors.Is(err, bufio.ErrTooLong) {
			if encErr := enc.Encode(failure("bad_command", "line exceeds 4 MiB")); encErr != nil {
				return encErr
			}
			if flushErr := flush(); flushErr != nil {
				return flushErr
			}
			return ErrLineTooLong
		}
		return err
	}
	return nil
}

// --- command decoding.

// opMembers lists every member of each op, required or optional.
var opMembers = map[string][]string{
	"deploy":            {"sender", "module", "metadata"},
	"upgrade":           {"sender", "contractId", "module", "metadata"},
	"transferOwnership": {"sender", "contractId", "newOwner"},
	"buyStorage":        {"sender", "contractId", "bytes"},
	"call":              {"sender", "contractId", "payload", "deposit", "plasmaLimit"},
	"deposit":           {"sender", "contractId", "asset", "amount", "plasmaLimit"},
	"setStorage":        {"contractId", "localKey", "value"},
	"getStorage":        {"contractId", "localKey"},
}

// defaultPlasmaLimit is the plasmaLimit of a call or deposit that names
// none.
const defaultPlasmaLimit = 10_000_000

// command is one decoded command. Only the members its op lists are
// set.
type command struct {
	op          string
	sender      types.Address
	contractID  types.Hash
	module      []byte
	metadata    []byte // the encoded ContractMetadata; IMMUTABLE when the op names none
	newOwner    types.Address
	bytes       uint64
	payload     []byte
	asset       codec.AssetID
	amount      codec.U256
	plasmaLimit uint64
	localKey    []byte
	value       []byte
	valueNull   bool
}

// fields reads the members of one command, keeping the first error.
type fields struct {
	op  string
	m   map[string]json.RawMessage
	err error
}

func (f *fields) fail(name, format string, args ...any) {
	if f.err == nil {
		f.err = fmt.Errorf("%s: %s: %s", f.op, name, fmt.Sprintf(format, args...))
	}
}

// get returns a member's raw value. A missing or null member is absent;
// a required one is also an error.
func (f *fields) get(name string, required bool) (json.RawMessage, bool) {
	if f.err != nil {
		return nil, false
	}
	raw, ok := f.m[name]
	if !ok || string(bytes.TrimSpace(raw)) == "null" {
		if required {
			f.fail(name, "is required")
		}
		return nil, false
	}
	return raw, true
}

func (f *fields) str(name string, required bool) (string, bool) {
	raw, ok := f.get(name, required)
	if !ok {
		return "", false
	}
	var s string
	if json.Unmarshal(raw, &s) != nil {
		f.fail(name, "must be a string")
		return "", false
	}
	return s, true
}

// hexOf reads a lowercase hex member of wantLen bytes, or of any
// length when wantLen is negative.
func (f *fields) hexOf(name string, required bool, wantLen int) ([]byte, bool) {
	s, ok := f.str(name, required)
	if !ok {
		return nil, false
	}
	b, err := lowerHex(s)
	if err != nil {
		f.fail(name, "%v", err)
		return nil, false
	}
	if wantLen >= 0 && len(b) != wantLen {
		f.fail(name, "must be exactly %d hex digits", 2*wantLen)
		return nil, false
	}
	return b, true
}

func (f *fields) hash(name string) types.Hash {
	var h types.Hash
	if b, ok := f.hexOf(name, true, 32); ok {
		copy(h[:], b)
	}
	return h
}

func (f *fields) address(name string) types.Address {
	var a types.Address
	if s, ok := f.str(name, true); ok {
		var err error
		if a, err = ParseAddress(s); err != nil {
			f.fail(name, "%v", err)
		}
	}
	return a
}

func (f *fields) uint64Of(name string, required bool) (uint64, bool) {
	raw, ok := f.get(name, required)
	if !ok {
		return 0, false
	}
	n, err := strconv.ParseUint(string(raw), 10, 64)
	if err != nil || !decimalRE.Match(raw) {
		f.fail(name, "must be a JSON integer from 0 to 18446744073709551615")
		return 0, false
	}
	return n, true
}

// amountOf decodes a decimal string below 2^256 from raw.
func amountOf(s string) (codec.U256, bool) {
	if !decimalRE.MatchString(s) {
		return codec.U256{}, false
	}
	n, ok := new(big.Int).SetString(s, 10)
	if !ok || n.BitLen() > 256 {
		return codec.U256{}, false
	}
	u, err := codec.U256FromBig(n)
	return u, err == nil
}

func (f *fields) amount(name string) codec.U256 {
	var u codec.U256
	if s, ok := f.str(name, true); ok {
		var good bool
		if u, good = amountOf(s); !good {
			f.fail(name, "must be a decimal string below 2^256")
		}
	}
	return u
}

// metadata reads "immutable" or {"owner": <address>}.
func (f *fields) metadata(required bool) []byte {
	raw, ok := f.get("metadata", required)
	if !ok {
		return nil
	}
	var s string
	if json.Unmarshal(raw, &s) == nil {
		if s != "immutable" {
			f.fail("metadata", `must be "immutable" or an object with member owner`)
			return nil
		}
		return encodeMetadata(codec.PolicyImmutable, types.Address{})
	}
	var obj map[string]json.RawMessage
	if json.Unmarshal(raw, &obj) != nil || len(obj) != 1 {
		f.fail("metadata", `must be "immutable" or an object with member owner`)
		return nil
	}
	sub := &fields{op: f.op, m: obj}
	owner := sub.address("owner")
	if sub.err != nil {
		f.fail("metadata", "%v", strings.TrimPrefix(sub.err.Error(), f.op+": "))
		return nil
	}
	return encodeMetadata(codec.PolicyOwnerUpgradeable, owner)
}

// decodeCommand decodes one command line. Every error is a bad_command.
func decodeCommand(line []byte) (command, error) {
	var m map[string]json.RawMessage
	if t := bytes.TrimSpace(line); len(t) == 0 || t[0] != '{' || json.Unmarshal(t, &m) != nil {
		return command{}, errors.New("a command must be a JSON object")
	}
	var op string
	if raw, ok := m["op"]; !ok || json.Unmarshal(raw, &op) != nil {
		return command{}, errors.New("op: must be a string")
	}
	members, ok := opMembers[op]
	if !ok {
		return command{}, fmt.Errorf("op: unknown op %q", op)
	}
	known := map[string]bool{"op": true}
	for _, name := range members {
		known[name] = true
	}
	var extra []string
	for name := range m {
		if !known[name] {
			extra = append(extra, name)
		}
	}
	if len(extra) > 0 {
		sort.Strings(extra)
		return command{}, fmt.Errorf("%s: %s: unknown member", op, extra[0])
	}

	f := &fields{op: op, m: m}
	c := command{op: op}
	switch op {
	case "deploy":
		c.sender = f.address("sender")
		c.module, _ = f.hexOf("module", true, -1)
		c.metadata = f.metadata(true)
	case "upgrade":
		c.sender = f.address("sender")
		c.contractID = f.hash("contractId")
		c.module, _ = f.hexOf("module", true, -1)
		c.metadata = f.metadata(false)
		if c.metadata == nil {
			c.metadata = encodeMetadata(codec.PolicyImmutable, types.Address{})
		}
	case "transferOwnership":
		c.sender = f.address("sender")
		c.contractID = f.hash("contractId")
		c.newOwner = f.address("newOwner")
	case "buyStorage":
		c.sender = f.address("sender")
		c.contractID = f.hash("contractId")
		c.bytes, _ = f.uint64Of("bytes", true)
	case "call":
		c.sender = f.address("sender")
		c.contractID = f.hash("contractId")
		c.payload, _ = f.hexOf("payload", true, -1)
		if raw, ok := f.get("deposit", false); ok {
			var d map[string]json.RawMessage
			if json.Unmarshal(raw, &d) != nil || len(d) != 2 {
				f.fail("deposit", "must be an object with members asset, amount")
				break
			}
			sub := &fields{op: op, m: d}
			var asset types.Hash
			if b, ok := sub.hexOf("asset", true, 32); ok {
				copy(asset[:], b)
			}
			c.asset = codec.AssetID(asset)
			c.amount = sub.amount("amount")
			if sub.err != nil {
				f.fail("deposit", "%v", strings.TrimPrefix(sub.err.Error(), op+": "))
			}
		}
		c.plasmaLimit = defaultPlasmaLimit
		if n, ok := f.uint64Of("plasmaLimit", false); ok {
			c.plasmaLimit = n
		}
	case "deposit":
		c.sender = f.address("sender")
		c.contractID = f.hash("contractId")
		c.asset = codec.AssetID(f.hash("asset"))
		c.amount = f.amount("amount")
		c.plasmaLimit = defaultPlasmaLimit
		if n, ok := f.uint64Of("plasmaLimit", false); ok {
			c.plasmaLimit = n
		}
	case "setStorage":
		c.contractID = f.hash("contractId")
		c.localKey, _ = f.hexOf("localKey", true, -1)
		if f.err == nil {
			raw, present := m["value"]
			switch {
			case !present:
				f.fail("value", "is required")
			case string(bytes.TrimSpace(raw)) == "null":
				c.valueNull = true
			default:
				c.value, _ = f.hexOf("value", true, -1)
			}
		}
	case "getStorage":
		c.contractID = f.hash("contractId")
		c.localKey, _ = f.hexOf("localKey", true, -1)
	}
	if f.err != nil {
		return command{}, f.err
	}
	return c, nil
}

// sha3 is the SHA3-256 of the concatenated parts as a types.Hash.
func sha3(parts ...[]byte) types.Hash {
	var h types.Hash
	copy(h[:], crypto.Hash(parts...))
	return h
}
