// Package simipc is the private channel between the gateway and one
// simulate-mode worker. It carries a stream of length-prefixed, typed
// frames: u32 big-endian length (the type byte plus the payload), u8
// type, payload. The gateway writes types 0x41 to 0x46 and the
// simulator writes types 0xC1 to 0xC6.
//
// Session rules:
//
//  1. The gateway writes Config first. The simulator answers Ready
//     once, then waits.
//  2. Per simulation the gateway writes one Simulate. The simulator
//     sends zero or more requests, each answered by exactly one reply
//     before its next frame, and ends with exactly one Result or
//     Failed.
//  3. ReadRequest is answered by Value or Unavailable, CodeRequest by
//     Code or Unavailable, CapRequest by Caps with the same count or
//     Unavailable.
//  4. A simulation sends at most one CodeRequest and at most one
//     CapRequest. The gateway answers at most MaxStateReads
//     ReadRequests per simulation with Value and every later one with
//     Unavailable. The simulator keeps each Value for the rest of its
//     simulation and never asks for that path again in it.
//  5. Any other frame, a frame over its cap or a malformed payload ends
//     the session, and the simulator exits 1. End of file between
//     simulations makes the simulator exit 0.
//
// The package imports neither internal/ipc nor internal/batch, so the
// gateway can use it without linking the executor.
package simipc

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"

	"github.com/zenon-network/go-zenon/common/trie"
	"github.com/zenon-network/go-zenon/common/types"
	"github.com/zenon-network/go-zenon/vm/constants"

	"github.com/Zeno-L2/zeno-executor/internal/codec"
)

const (
	MaxGatewayFrame   = 1 << 20  // length cap on frames the simulator reads
	MaxSimulatorFrame = 64 << 10 // length cap on frames the gateway reads

	// MaxInputData is the longest canonical CallL2 input: kind 1,
	// source block hash 32, target contract 32, plasma limit 8, deposit
	// asset 32, deposit amount 32, payload length 4, then a full
	// payload.
	MaxInputData     = 1 + 32 + 32 + 8 + 32 + 32 + 4 + codec.MaxCallPayload
	MaxFailedMessage = 1024
	MaxNetworkName   = 256

	// MaxStateReads is the ReadRequests the gateway answers with a
	// Value in one simulation.
	MaxStateReads = 65_536

	DefaultMemoryLimit = 1 << 30 // applied when Config.MemoryLimit is 0
	MinMemoryLimit     = 768 << 20
	MaxMemoryLimit     = 8 << 30
)

// ErrMalformed marks a frame or payload that breaks the protocol. EOF
// and short reads never wrap it.
var ErrMalformed = errors.New("simipc: malformed frame")

// Type is a frame's type byte.
type Type uint8

const (
	TypeConfig      Type = 0x41 // G to S
	TypeSimulate    Type = 0x42
	TypeValue       Type = 0x43
	TypeCode        Type = 0x44
	TypeCaps        Type = 0x45
	TypeUnavailable Type = 0x46
	TypeReady       Type = 0xC1 // S to G
	TypeReadRequest Type = 0xC2
	TypeCodeRequest Type = 0xC3
	TypeCapRequest  Type = 0xC4
	TypeResult      Type = 0xC5
	TypeFailed      Type = 0xC6
)

// Message is implemented by exactly the twelve types below, all with
// value receivers.
type Message interface{ Type() Type }

// Config is the first frame of a session.
type Config struct {
	DomainID    uint32
	MemoryLimit uint64 // bytes; 0 means DefaultMemoryLimit
	NetworkName string
}

// Simulate asks for one simulation.
type Simulate struct {
	GlobalInputIndex, MomentumHeight, MomentumTime uint64
	MomentumHash                                   types.Hash
	L1Sender                                       types.Address
	InputData                                      []byte // canonical input encoding, kind 0 or 1
}

// Value answers a ReadRequest. Present false is an absent path.
type Value struct {
	Present bool
	Value   []byte
}

// Code answers a CodeRequest.
type Code struct {
	Found  bool
	Module []byte
}

// Caps answers a CapRequest, one amount per requested asset.
type Caps struct{ Amounts []codec.U256 }

// Unavailable answers any request the gateway cannot serve.
type Unavailable struct{}

// Ready is the simulator's answer to Config.
type Ready struct{}

// ReadRequest asks for the state value at Path.
type ReadRequest struct{ Path trie.Path }

// CodeRequest asks for the as-deployed module with this hash.
type CodeRequest struct{ SourceCodeHash types.Hash }

// CapRequest asks for the withdrawal cap of each asset.
type CapRequest struct{ Assets []codec.AssetID }

// Result ends a simulation that ran.
type Result struct {
	Status          codec.Status
	GasUsed         uint64
	ContractID      types.Hash
	CodeHash        types.Hash
	AbortCode       int32
	ExecutionResult []byte // canonical codec.ExecutionResult, both roots zero
}

// Failed ends a simulation that could not run.
type Failed struct{ Message string }

func (Config) Type() Type      { return TypeConfig }
func (Simulate) Type() Type    { return TypeSimulate }
func (Value) Type() Type       { return TypeValue }
func (Code) Type() Type        { return TypeCode }
func (Caps) Type() Type        { return TypeCaps }
func (Unavailable) Type() Type { return TypeUnavailable }
func (Ready) Type() Type       { return TypeReady }
func (ReadRequest) Type() Type { return TypeReadRequest }
func (CodeRequest) Type() Type { return TypeCodeRequest }
func (CapRequest) Type() Type  { return TypeCapRequest }
func (Result) Type() Type      { return TypeResult }
func (Failed) Type() Type      { return TypeFailed }

// gatewayType reports whether t is a gateway-to-simulator type.
func gatewayType(t Type) bool { return t >= TypeConfig && t <= TypeUnavailable }

// simulatorType reports whether t is a simulator-to-gateway type.
func simulatorType(t Type) bool { return t >= TypeReady && t <= TypeFailed }

// WriteMessage writes m as one frame in a single Write. It refuses any
// message the readers would reject, wrapping ErrMalformed, except a
// Failed message longer than MaxFailedMessage, which it truncates.
func WriteMessage(w io.Writer, m Message) error {
	if f, ok := m.(Failed); ok && len(f.Message) > MaxFailedMessage {
		m = Failed{Message: f.Message[:MaxFailedMessage]}
	}
	payload, err := encode(m)
	if err != nil {
		return err
	}
	// Decoding the payload applies every rule the readers apply.
	if _, err := decode(m.Type(), payload); err != nil {
		return err
	}
	max := uint32(MaxSimulatorFrame)
	if gatewayType(m.Type()) {
		max = MaxGatewayFrame
	}
	frame, err := assembleFrame(m.Type(), payload, max)
	if err != nil {
		return err
	}
	_, err = w.Write(frame)
	return err
}

// ReadFromGateway reads one frame of types 0x41 to 0x46, capped at
// MaxGatewayFrame.
func ReadFromGateway(r io.Reader) (Message, error) {
	t, payload, err := readFrame(r, MaxGatewayFrame)
	if err != nil {
		return nil, err
	}
	if !gatewayType(t) {
		return nil, fmt.Errorf("simipc: type 0x%02x is not a gateway frame: %w", uint8(t), ErrMalformed)
	}
	return decode(t, payload)
}

// ReadFromSimulator reads one frame of types 0xC1 to 0xC6, capped at
// MaxSimulatorFrame.
func ReadFromSimulator(r io.Reader) (Message, error) {
	t, payload, err := readFrame(r, MaxSimulatorFrame)
	if err != nil {
		return nil, err
	}
	if !simulatorType(t) {
		return nil, fmt.Errorf("simipc: type 0x%02x is not a simulator frame: %w", uint8(t), ErrMalformed)
	}
	return decode(t, payload)
}

func malformed(format string, args ...any) error {
	return fmt.Errorf("simipc: %s: %w", fmt.Sprintf(format, args...), ErrMalformed)
}

func encode(m Message) ([]byte, error) {
	var b []byte
	switch v := m.(type) {
	case Config:
		b = binary.BigEndian.AppendUint32(b, v.DomainID)
		b = binary.BigEndian.AppendUint64(b, v.MemoryLimit)
		b = binary.BigEndian.AppendUint16(b, uint16(len(v.NetworkName)))
		if len(v.NetworkName) > 0xFFFF {
			return nil, malformed("network name of %d bytes", len(v.NetworkName))
		}
		b = append(b, v.NetworkName...)
	case Simulate:
		b = binary.BigEndian.AppendUint64(b, v.GlobalInputIndex)
		b = binary.BigEndian.AppendUint64(b, v.MomentumHeight)
		b = binary.BigEndian.AppendUint64(b, v.MomentumTime)
		b = append(b, v.MomentumHash[:]...)
		b = append(b, v.L1Sender[:]...)
		b = binary.BigEndian.AppendUint32(b, uint32(len(v.InputData)))
		b = append(b, v.InputData...)
	case Value:
		b = append(b, boolByte(v.Present))
		b = binary.BigEndian.AppendUint32(b, uint32(len(v.Value)))
		b = append(b, v.Value...)
	case Code:
		b = append(b, boolByte(v.Found))
		b = binary.BigEndian.AppendUint32(b, uint32(len(v.Module)))
		b = append(b, v.Module...)
	case Caps:
		if len(v.Amounts) > 0xFFFF {
			return nil, malformed("%d amounts", len(v.Amounts))
		}
		b = binary.BigEndian.AppendUint16(b, uint16(len(v.Amounts)))
		for _, a := range v.Amounts {
			b = append(b, a[:]...)
		}
	case Unavailable, Ready:
	case ReadRequest:
		b = append(b, v.Path[:]...)
	case CodeRequest:
		b = append(b, v.SourceCodeHash[:]...)
	case CapRequest:
		if len(v.Assets) > 0xFFFF {
			return nil, malformed("%d assets", len(v.Assets))
		}
		b = binary.BigEndian.AppendUint16(b, uint16(len(v.Assets)))
		for _, a := range v.Assets {
			b = append(b, a[:]...)
		}
	case Result:
		b = append(b, byte(v.Status))
		b = binary.BigEndian.AppendUint64(b, v.GasUsed)
		b = append(b, v.ContractID[:]...)
		b = append(b, v.CodeHash[:]...)
		b = binary.BigEndian.AppendUint32(b, uint32(v.AbortCode))
		b = binary.BigEndian.AppendUint32(b, uint32(len(v.ExecutionResult)))
		b = append(b, v.ExecutionResult...)
	case Failed:
		if len(v.Message) > 0xFFFF {
			return nil, malformed("failed message of %d bytes", len(v.Message))
		}
		b = binary.BigEndian.AppendUint16(b, uint16(len(v.Message)))
		b = append(b, v.Message...)
	default:
		return nil, malformed("unknown message %T", m)
	}
	return b, nil
}

func boolByte(v bool) byte {
	if v {
		return 1
	}
	return 0
}

// payloadReader consumes a payload and remembers the first failure.
type payloadReader struct {
	b   []byte
	err error
}

func (r *payloadReader) take(n int) []byte {
	if r.err != nil {
		return nil
	}
	if n < 0 || n > len(r.b) {
		r.err = malformed("short payload")
		return nil
	}
	out := r.b[:n]
	r.b = r.b[n:]
	return out
}

func (r *payloadReader) u8() uint8 {
	if b := r.take(1); b != nil {
		return b[0]
	}
	return 0
}

func (r *payloadReader) u16() uint16 {
	if b := r.take(2); b != nil {
		return binary.BigEndian.Uint16(b)
	}
	return 0
}

func (r *payloadReader) u32() uint32 {
	if b := r.take(4); b != nil {
		return binary.BigEndian.Uint32(b)
	}
	return 0
}

func (r *payloadReader) u64() uint64 {
	if b := r.take(8); b != nil {
		return binary.BigEndian.Uint64(b)
	}
	return 0
}

func (r *payloadReader) fail(format string, args ...any) {
	if r.err == nil {
		r.err = malformed(format, args...)
	}
}

// flag reads a u8 that must be 0 or 1.
func (r *payloadReader) flag(name string) bool {
	v := r.u8()
	if v > 1 {
		r.fail("%s byte %d is not 0 or 1", name, v)
	}
	return v == 1
}

// blob reads a u32 length and that many bytes, never reading more than
// max. A zero length returns a non-nil empty slice.
func (r *payloadReader) blob(max uint32) []byte {
	n := r.u32()
	if r.err != nil {
		return nil
	}
	if n > max {
		r.fail("length %d exceeds %d", n, max)
		return nil
	}
	out := r.take(int(n))
	if r.err == nil && out == nil {
		out = []byte{}
	}
	return out
}

// count reads a u16 that must lie in 1 to codec.MaxAssetsPerBatch.
func (r *payloadReader) count() int {
	n := r.u16()
	if r.err == nil && (n == 0 || n > codec.MaxAssetsPerBatch) {
		r.fail("count %d is outside 1 to %d", n, codec.MaxAssetsPerBatch)
	}
	return int(n)
}

func (r *payloadReader) done() error {
	if r.err == nil && len(r.b) != 0 {
		r.fail("%d trailing bytes", len(r.b))
	}
	return r.err
}

func decode(t Type, payload []byte) (Message, error) {
	r := &payloadReader{b: payload}
	var m Message
	switch t {
	case TypeConfig:
		var c Config
		c.DomainID = r.u32()
		c.MemoryLimit = r.u64()
		n := r.u16()
		if r.err == nil && c.MemoryLimit != 0 && (c.MemoryLimit < MinMemoryLimit || c.MemoryLimit > MaxMemoryLimit) {
			r.fail("memory limit %d is outside %d to %d", c.MemoryLimit, uint64(MinMemoryLimit), uint64(MaxMemoryLimit))
		}
		if r.err == nil && (n == 0 || n > MaxNetworkName) {
			r.fail("network name length %d is outside 1 to %d", n, MaxNetworkName)
		}
		c.NetworkName = string(r.take(int(n)))
		m = c
	case TypeSimulate:
		var s Simulate
		s.GlobalInputIndex = r.u64()
		s.MomentumHeight = r.u64()
		s.MomentumTime = r.u64()
		copy(s.MomentumHash[:], r.take(len(s.MomentumHash)))
		copy(s.L1Sender[:], r.take(len(s.L1Sender)))
		n := r.u32()
		if r.err == nil && (n == 0 || n > MaxInputData) {
			r.fail("input data length %d is outside 1 to %d", n, MaxInputData)
		}
		s.InputData = r.take(int(n))
		m = s
	case TypeValue:
		var v Value
		v.Present = r.flag("present")
		v.Value = r.blob(codec.MaxStateValueSize)
		if r.err == nil && !v.Present && len(v.Value) > 0 {
			r.fail("absent value with %d bytes", len(v.Value))
		}
		if !v.Present {
			v.Value = nil
		}
		m = v
	case TypeCode:
		var c Code
		c.Found = r.flag("found")
		c.Module = r.blob(constants.MaxCodeSize)
		if r.err == nil && !c.Found && len(c.Module) > 0 {
			r.fail("module not found with %d bytes", len(c.Module))
		}
		if r.err == nil && c.Found && len(c.Module) == 0 {
			r.fail("module found with no bytes")
		}
		if !c.Found {
			c.Module = nil
		}
		m = c
	case TypeCaps:
		var c Caps
		n := r.count()
		for i := 0; i < n && r.err == nil; i++ {
			var a codec.U256
			copy(a[:], r.take(len(a)))
			c.Amounts = append(c.Amounts, a)
		}
		m = c
	case TypeUnavailable:
		m = Unavailable{}
	case TypeReady:
		m = Ready{}
	case TypeReadRequest:
		var q ReadRequest
		copy(q.Path[:], r.take(len(q.Path)))
		m = q
	case TypeCodeRequest:
		var q CodeRequest
		copy(q.SourceCodeHash[:], r.take(len(q.SourceCodeHash)))
		m = q
	case TypeCapRequest:
		var q CapRequest
		n := r.count()
		for i := 0; i < n && r.err == nil; i++ {
			var a codec.AssetID
			copy(a[:], r.take(len(a)))
			q.Assets = append(q.Assets, a)
		}
		m = q
	case TypeResult:
		var res Result
		res.Status = codec.Status(r.u8())
		if r.err == nil && res.Status > codec.StatusStorageQuotaExceeded {
			r.fail("status %d is unknown", res.Status)
		}
		res.GasUsed = r.u64()
		copy(res.ContractID[:], r.take(len(res.ContractID)))
		copy(res.CodeHash[:], r.take(len(res.CodeHash)))
		res.AbortCode = int32(r.u32())
		res.ExecutionResult = r.blob(constants.MaxExecutionResult)
		if r.err == nil && len(res.ExecutionResult) == 0 {
			r.fail("empty execution result")
		}
		m = res
	case TypeFailed:
		n := r.u16()
		if r.err == nil && n > MaxFailedMessage {
			r.fail("message length %d exceeds %d", n, MaxFailedMessage)
		}
		m = Failed{Message: string(r.take(int(n)))}
	default:
		return nil, malformed("unknown type 0x%02x", uint8(t))
	}
	if err := r.done(); err != nil {
		return nil, err
	}
	return m, nil
}
