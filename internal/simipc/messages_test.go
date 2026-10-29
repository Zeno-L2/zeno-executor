package simipc

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"reflect"
	"strings"
	"testing"

	"github.com/zenon-network/go-zenon/common/types"

	"github.com/Zeno-L2/zeno-executor/internal/codec"
)

func u256(b byte) codec.U256 { return codec.U256{31: b} }

func gatewayMessages() []Message {
	return []Message{
		Config{DomainID: 7, MemoryLimit: 0, NetworkName: "testnet"},
		Config{DomainID: 7, MemoryLimit: MinMemoryLimit, NetworkName: "testnet"},
		Config{DomainID: 7, MemoryLimit: MaxMemoryLimit, NetworkName: "testnet"},
		Simulate{GlobalInputIndex: 5, MomentumHeight: 11, MomentumTime: 22, MomentumHash: types.Hash{4, 5, 6}, L1Sender: types.Address{1, 2, 3}, InputData: []byte{0, 1, 2}},
		Simulate{InputData: bytes.Repeat([]byte{9}, MaxInputData)},
		Value{Present: true, Value: []byte{1, 2}},
		Value{Present: true, Value: []byte{}},
		Value{Present: false},
		Code{Found: true, Module: []byte{0, 'a', 's', 'm'}},
		Code{Found: false},
		Caps{Amounts: []codec.U256{u256(1), u256(2)}},
		Unavailable{},
	}
}

func simulatorMessages() []Message {
	return []Message{
		Ready{},
		ReadRequest{Path: [32]byte{1, 2, 3}},
		CodeRequest{SourceCodeHash: types.Hash{4, 5}},
		CapRequest{Assets: []codec.AssetID{{1}, {2}}},
		Result{Status: codec.StatusRuntimeFault, GasUsed: 99, ContractID: types.Hash{1}, CodeHash: types.Hash{2}, AbortCode: -7, ExecutionResult: []byte{1, 2, 3}},
		Result{Status: codec.StatusSuccess, ExecutionResult: bytes.Repeat([]byte{1}, 32<<10)},
		Failed{Message: "no"},
		Failed{Message: ""},
	}
}

func TestRoundTrip(t *testing.T) {
	for _, m := range gatewayMessages() {
		var buf bytes.Buffer
		if err := WriteMessage(&buf, m); err != nil {
			t.Fatalf("write %T: %v", m, err)
		}
		got, err := ReadFromGateway(&buf)
		if err != nil {
			t.Fatalf("read %T: %v", m, err)
		}
		if !reflect.DeepEqual(got, m) {
			t.Errorf("%T round trip: got %+v, want %+v", m, got, m)
		}
	}
	for _, m := range simulatorMessages() {
		var buf bytes.Buffer
		if err := WriteMessage(&buf, m); err != nil {
			t.Fatalf("write %T: %v", m, err)
		}
		got, err := ReadFromSimulator(&buf)
		if err != nil {
			t.Fatalf("read %T: %v", m, err)
		}
		if !reflect.DeepEqual(got, m) {
			t.Errorf("%T round trip: got %+v, want %+v", m, got, m)
		}
	}
}

// frame builds a frame by hand, bypassing WriteMessage's checks.
func frame(t Type, payload []byte) []byte {
	out := binary.BigEndian.AppendUint32(nil, uint32(1+len(payload)))
	out = append(out, byte(t))
	return append(out, payload...)
}

func be32(v uint32) []byte { return binary.BigEndian.AppendUint32(nil, v) }
func be64(v uint64) []byte { return binary.BigEndian.AppendUint64(nil, v) }
func be16(v uint16) []byte { return binary.BigEndian.AppendUint16(nil, v) }

func cat(parts ...[]byte) []byte { return bytes.Join(parts, nil) }

// TestSimulateWireLayout pins the field order of a Simulate payload
// against hand-built bytes, so a reorder applied to both encode and
// decode is caught.
func TestSimulateWireLayout(t *testing.T) {
	payload := cat(be64(1), be64(2), be64(3),
		bytes.Repeat([]byte{0xAA}, 32), bytes.Repeat([]byte{0xBB}, 20),
		be32(1), []byte{0})
	m, err := ReadFromGateway(bytes.NewReader(frame(TypeSimulate, payload)))
	if err != nil {
		t.Fatal(err)
	}
	s := m.(Simulate)
	if s.GlobalInputIndex != 1 || s.MomentumHeight != 2 || s.MomentumTime != 3 {
		t.Fatalf("integers = %d %d %d", s.GlobalInputIndex, s.MomentumHeight, s.MomentumTime)
	}
	if s.MomentumHash != types.BytesToHashPanic(bytes.Repeat([]byte{0xAA}, 32)) {
		t.Fatalf("momentum hash = %x", s.MomentumHash[:])
	}
	if !bytes.Equal(s.L1Sender[:], bytes.Repeat([]byte{0xBB}, 20)) {
		t.Fatalf("l1 sender = %x", s.L1Sender[:])
	}
}

func TestDecodeRejects(t *testing.T) {
	name := []byte("net")
	cfg := func(limit uint64, nameLen uint16, n []byte) []byte {
		return cat(be32(1), be64(limit), be16(nameLen), n)
	}
	sim := func(dataLen uint32, data []byte) []byte {
		return cat(be64(1), be64(2), be64(3), make([]byte, 32), make([]byte, 20), be32(dataLen), data)
	}
	res := func(status byte, n uint32, body []byte) []byte {
		return cat([]byte{status}, be64(1), make([]byte, 64), be32(0), be32(n), body)
	}
	cases := []struct {
		name    string
		gateway bool
		t       Type
		payload []byte
	}{
		{"config limit below min", true, TypeConfig, cfg(MinMemoryLimit-1, 3, name)},
		{"config limit above max", true, TypeConfig, cfg(MaxMemoryLimit+1, 3, name)},
		{"config limit 1", true, TypeConfig, cfg(1, 3, name)},
		{"config name empty", true, TypeConfig, cfg(0, 0, nil)},
		{"config name too long", true, TypeConfig, cfg(0, MaxNetworkName+1, bytes.Repeat([]byte("a"), MaxNetworkName+1))},
		{"config without memory limit", true, TypeConfig, cat(be32(1), be16(3), name)},
		{"config trailing", true, TypeConfig, append(cfg(0, 3, name), 0)},
		{"config short name", true, TypeConfig, cfg(0, 4, name)},
		{"simulate data empty", true, TypeSimulate, sim(0, nil)},
		{"simulate data too long", true, TypeSimulate, sim(MaxInputData+1, make([]byte, MaxInputData+1))},
		{"simulate trailing", true, TypeSimulate, append(sim(1, []byte{1}), 0)},
		{"simulate short", true, TypeSimulate, sim(2, []byte{1})},
		{"value present 2", true, TypeValue, cat([]byte{2}, be32(0))},
		{"value absent with bytes", true, TypeValue, cat([]byte{0}, be32(1), []byte{1})},
		{"value too long", true, TypeValue, cat([]byte{1}, be32(codec.MaxStateValueSize+1))},
		{"value trailing", true, TypeValue, cat([]byte{1}, be32(0), []byte{0})},
		{"value short", true, TypeValue, []byte{1, 0}},
		{"code found 2", true, TypeCode, cat([]byte{2}, be32(0))},
		{"code not found with bytes", true, TypeCode, cat([]byte{0}, be32(1), []byte{1})},
		{"code found empty", true, TypeCode, cat([]byte{1}, be32(0))},
		{"code too long", true, TypeCode, cat([]byte{1}, be32(256<<10+1))},
		{"code trailing", true, TypeCode, cat([]byte{1}, be32(1), []byte{1, 1})},
		{"caps zero", true, TypeCaps, be16(0)},
		{"caps too many", true, TypeCaps, cat(be16(codec.MaxAssetsPerBatch+1), make([]byte, 32*(codec.MaxAssetsPerBatch+1)))},
		{"caps short", true, TypeCaps, cat(be16(2), make([]byte, 32))},
		{"caps trailing", true, TypeCaps, cat(be16(1), make([]byte, 33))},
		{"unavailable payload", true, TypeUnavailable, []byte{0}},
		{"ready payload", false, TypeReady, []byte{0}},
		{"read request short", false, TypeReadRequest, make([]byte, 31)},
		{"read request trailing", false, TypeReadRequest, make([]byte, 33)},
		{"code request short", false, TypeCodeRequest, make([]byte, 31)},
		{"code request trailing", false, TypeCodeRequest, make([]byte, 33)},
		{"cap request zero", false, TypeCapRequest, be16(0)},
		{"cap request too many", false, TypeCapRequest, cat(be16(codec.MaxAssetsPerBatch+1), make([]byte, 32*(codec.MaxAssetsPerBatch+1)))},
		{"cap request trailing", false, TypeCapRequest, cat(be16(1), make([]byte, 33))},
		{"result status", false, TypeResult, res(byte(codec.StatusStorageQuotaExceeded)+1, 1, []byte{1})},
		{"result empty", false, TypeResult, res(0, 0, nil)},
		{"result too long", false, TypeResult, res(0, 32<<10+1, nil)},
		{"result trailing", false, TypeResult, append(res(0, 1, []byte{1}), 0)},
		{"result short", false, TypeResult, res(0, 2, []byte{1})},
		{"failed too long", false, TypeFailed, cat(be16(MaxFailedMessage+1), make([]byte, MaxFailedMessage+1))},
		{"failed trailing", false, TypeFailed, cat(be16(1), []byte("ab"))},
		{"failed short", false, TypeFailed, cat(be16(2), []byte("a"))},
	}
	for _, c := range cases {
		read := ReadFromSimulator
		if c.gateway {
			read = ReadFromGateway
		}
		_, err := read(bytes.NewReader(frame(c.t, c.payload)))
		if !errors.Is(err, ErrMalformed) {
			t.Errorf("%s: err = %v, want ErrMalformed", c.name, err)
		}
	}
}

func TestFrameCaps(t *testing.T) {
	// A reader that fails the test when read past the 4 header bytes.
	over := func(n uint32) io.Reader {
		return &headerOnly{t: t, hdr: be32(n)}
	}
	if _, err := ReadFromGateway(over(MaxGatewayFrame + 1)); !errors.Is(err, ErrMalformed) {
		t.Errorf("gateway cap+1: %v", err)
	}
	if _, err := ReadFromSimulator(over(MaxSimulatorFrame + 1)); !errors.Is(err, ErrMalformed) {
		t.Errorf("simulator cap+1: %v", err)
	}
	if _, err := ReadFromGateway(over(0)); !errors.Is(err, ErrMalformed) {
		t.Errorf("gateway zero: %v", err)
	}
	if _, err := ReadFromSimulator(over(0)); !errors.Is(err, ErrMalformed) {
		t.Errorf("simulator zero: %v", err)
	}
	if _, err := ReadFromGateway(bytes.NewReader(nil)); err != io.EOF {
		t.Errorf("empty: %v, want io.EOF", err)
	}
	if _, err := ReadFromGateway(bytes.NewReader([]byte{0, 0})); err != io.ErrUnexpectedEOF {
		t.Errorf("short header: %v, want io.ErrUnexpectedEOF", err)
	}
	if _, err := ReadFromGateway(bytes.NewReader(append(be32(9), 0x41))); err != io.ErrUnexpectedEOF {
		t.Errorf("short body: %v, want io.ErrUnexpectedEOF", err)
	}
}

type headerOnly struct {
	t    *testing.T
	hdr  []byte
	done bool
}

func (h *headerOnly) Read(p []byte) (int, error) {
	if h.done {
		h.t.Error("read past the frame header")
		return 0, io.EOF
	}
	h.done = true
	return copy(p, h.hdr), nil
}

func TestWrongDirection(t *testing.T) {
	if _, err := ReadFromGateway(bytes.NewReader(frame(TypeReady, nil))); !errors.Is(err, ErrMalformed) {
		t.Errorf("gateway reader, 0xC1: %v", err)
	}
	if _, err := ReadFromSimulator(bytes.NewReader(frame(TypeConfig, cat(be32(1), be64(0), be16(1), []byte("a"))))); !errors.Is(err, ErrMalformed) {
		t.Errorf("simulator reader, 0x41: %v", err)
	}
	if _, err := ReadFromGateway(bytes.NewReader(frame(0x40, nil))); !errors.Is(err, ErrMalformed) {
		t.Errorf("gateway reader, 0x40: %v", err)
	}
}

func TestWriteRefuses(t *testing.T) {
	refused := []Message{
		Config{NetworkName: "a", MemoryLimit: MinMemoryLimit - 1},
		Config{NetworkName: "a", MemoryLimit: MaxMemoryLimit + 1},
		Config{},
		Config{NetworkName: strings.Repeat("a", MaxNetworkName+1)},
		Simulate{},
		Simulate{InputData: make([]byte, MaxInputData+1)},
		Value{Present: false, Value: []byte{1}},
		Value{Present: true, Value: make([]byte, codec.MaxStateValueSize+1)},
		Code{Found: true},
		Code{Found: false, Module: []byte{1}},
		Code{Found: true, Module: make([]byte, 256<<10+1)},
		Caps{},
		Caps{Amounts: make([]codec.U256, codec.MaxAssetsPerBatch+1)},
		CapRequest{},
		CapRequest{Assets: make([]codec.AssetID, codec.MaxAssetsPerBatch+1)},
		Result{Status: codec.StatusStorageQuotaExceeded + 1, ExecutionResult: []byte{1}},
		Result{},
		Result{ExecutionResult: make([]byte, 32<<10+1)},
	}
	for _, m := range refused {
		var buf bytes.Buffer
		err := WriteMessage(&buf, m)
		if !errors.Is(err, ErrMalformed) {
			t.Errorf("%T %+v: err = %v, want ErrMalformed", m, m, err)
		}
		if buf.Len() != 0 {
			t.Errorf("%T: wrote %d bytes despite refusing", m, buf.Len())
		}
	}

	var buf bytes.Buffer
	if err := WriteMessage(&buf, Failed{Message: strings.Repeat("x", 2000)}); err != nil {
		t.Fatal(err)
	}
	got, err := ReadFromSimulator(&buf)
	if err != nil {
		t.Fatal(err)
	}
	if n := len(got.(Failed).Message); n != MaxFailedMessage {
		t.Errorf("truncated message is %d bytes, want %d", n, MaxFailedMessage)
	}
}

func TestWriteOneWritePerFrame(t *testing.T) {
	w := &countWriter{}
	if err := WriteMessage(w, Simulate{InputData: []byte{1}}); err != nil {
		t.Fatal(err)
	}
	if w.n != 1 {
		t.Errorf("%d writes, want 1", w.n)
	}
}

type countWriter struct{ n int }

func (c *countWriter) Write(p []byte) (int, error) { c.n++; return len(p), nil }
