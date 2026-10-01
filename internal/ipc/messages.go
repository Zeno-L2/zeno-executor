package ipc

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"strconv"

	"github.com/zenon-network/go-zenon/chain/nom"
	"github.com/zenon-network/go-zenon/common/types"

	"github.com/digitalSloth/go-executor/internal/codec"
)

type Type uint8

const (
	TypeConfig          Type = 0x01 // S→W
	TypeFrontier        Type = 0x02 // S→W
	TypeMomentum        Type = 0x03 // S→W
	TypeRangeEnd        Type = 0x04 // S→W
	TypeAnchor          Type = 0x05 // S→W
	TypeSourceError     Type = 0x06 // S→W
	TypeFrontierRequest Type = 0x81 // W→S
	TypeMomentaRequest  Type = 0x82 // W→S
	TypeAnchorRequest   Type = 0x83 // W→S
	TypeCommitted       Type = 0x84 // W→S
)

// Side names the sender of a frame. Supervisor sends 0x01-0x06 and is read with MaxFrame;
// Worker sends 0x81-0x84 and is read with MaxWorkerFrame.
type Side uint8

const (
	Supervisor Side = 1
	Worker     Side = 2
)

const (
	MaxMomentaCount   = 1024 // internal/ledger/follower.go:18 (rpcMaxCountSize, unexported there)
	MaxSourceErrorLen = 4096 // the supervisor truncates a source error's text to this
	maxRoleLen        = 64
	maxNetworkNameLen = 256
	maxStoragePathLen = 4096
)

// Message is implemented by exactly the ten types below, all with value receivers.
type Message interface{ Type() Type }

type WorkerConfig struct {
	DomainID          uint32
	ExecutorAddress   types.Address
	STFSpecHash       types.Hash
	ConfirmationDepth uint64
	Role              string
	NetworkName       string
	StoragePath       string
}

func (WorkerConfig) Type() Type { return TypeConfig }

// DomainDir is <StoragePath>/<DomainID>, the root recovery.NewLayout uses (internal/recovery/store.go:39).
func (c WorkerConfig) DomainDir() string {
	return filepath.Join(c.StoragePath, strconv.FormatUint(uint64(c.DomainID), 10))
}

type Frontier struct{ Height uint64 }
type Momentum struct{ Detailed *nom.DetailedMomentum }
type RangeEnd struct{ Count uint32 }
type Anchor struct {
	Found         bool
	PostStateRoot types.Hash
	LastInputSeq  uint64
}

func (Frontier) Type() Type { return TypeFrontier }
func (Momentum) Type() Type { return TypeMomentum }
func (RangeEnd) Type() Type { return TypeRangeEnd }
func (Anchor) Type() Type   { return TypeAnchor }

type SourceErrorKind uint8

const (
	SourceUnavailable SourceErrorKind = 1
	SourceHalted      SourceErrorKind = 2
)

type SourceError struct {
	Kind    SourceErrorKind
	Message string
}

func (SourceError) Type() Type { return TypeSourceError }

type FrontierRequest struct{}
type MomentaRequest struct{ Height, Count uint64 }
type AnchorRequest struct{}
type Committed struct{ Commitment codec.BatchCommitment }

func (FrontierRequest) Type() Type { return TypeFrontierRequest }
func (MomentaRequest) Type() Type  { return TypeMomentaRequest }
func (AnchorRequest) Type() Type   { return TypeAnchorRequest }
func (Committed) Type() Type       { return TypeCommitted }

// --- payload reader/writer, shared with momentum.go ---

// pwriter is an append-only payload buffer for the message encoders below.
type pwriter struct{ buf []byte }

func (w *pwriter) u8(v uint8)   { w.buf = append(w.buf, v) }
func (w *pwriter) u16(v uint16) { w.buf = binary.BigEndian.AppendUint16(w.buf, v) }
func (w *pwriter) u32(v uint32) { w.buf = binary.BigEndian.AppendUint32(w.buf, v) }
func (w *pwriter) u64(v uint64) { w.buf = binary.BigEndian.AppendUint64(w.buf, v) }
func (w *pwriter) raw(b []byte) { w.buf = append(w.buf, b...) }
func (w *pwriter) str16(s string) {
	w.u16(uint16(len(s)))
	w.buf = append(w.buf, s...)
}

// errPayload marks a structural failure inside a payload reader. It is never returned to a
// caller outside this package: messages.go wraps it (and every other decode failure) in
// ErrMalformed, and momentum.go wraps it in its own "ipc: decode momentum: " prefix.
var errPayload = errors.New("ipc: payload decode error")

// preader consumes a payload from a fixed byte slice, tracking position.
type preader struct {
	buf []byte
	pos int
}

func newPReader(b []byte) *preader { return &preader{buf: b} }

func (r *preader) remaining() int { return len(r.buf) - r.pos }

func (r *preader) fixed(n int) ([]byte, error) {
	if n < 0 || r.remaining() < n {
		return nil, errPayload
	}
	out := r.buf[r.pos : r.pos+n]
	r.pos += n
	return out, nil
}

func (r *preader) u8() (uint8, error) {
	b, err := r.fixed(1)
	if err != nil {
		return 0, err
	}
	return b[0], nil
}

func (r *preader) u16() (uint16, error) {
	b, err := r.fixed(2)
	if err != nil {
		return 0, err
	}
	return binary.BigEndian.Uint16(b), nil
}

func (r *preader) u32() (uint32, error) {
	b, err := r.fixed(4)
	if err != nil {
		return 0, err
	}
	return binary.BigEndian.Uint32(b), nil
}

func (r *preader) u64() (uint64, error) {
	b, err := r.fixed(8)
	if err != nil {
		return 0, err
	}
	return binary.BigEndian.Uint64(b), nil
}

// str16 reads a u16 length then that many bytes, rejecting a length over max or over the
// remaining bytes before slicing.
func (r *preader) str16(max int) (string, error) {
	l, err := r.u16()
	if err != nil {
		return "", err
	}
	if int(l) > max || int(l) > r.remaining() {
		return "", errPayload
	}
	b, err := r.fixed(int(l))
	if err != nil {
		return "", err
	}
	return string(b), nil
}

func (r *preader) end() error {
	if r.remaining() != 0 {
		return errPayload
	}
	return nil
}

// --- Encode / WriteMessage / ReadMessage ---

func sideOfType(t Type) (Side, bool) {
	switch t {
	case TypeConfig, TypeFrontier, TypeMomentum, TypeRangeEnd, TypeAnchor, TypeSourceError:
		return Supervisor, true
	case TypeFrontierRequest, TypeMomentaRequest, TypeAnchorRequest, TypeCommitted:
		return Worker, true
	default:
		return 0, false
	}
}

func capOf(side Side) uint32 {
	if side == Supervisor {
		return MaxFrame
	}
	return MaxWorkerFrame
}

// Encode returns m's complete frame (header, type, payload). It applies every structural and range
// rule ReadMessage applies, and rejects a length field above the cap of m's sending side. The one
// rule it does not apply is momentum hash verification: a Momentum is encoded with EncodeMomentum
// and never decoded here. Encode writes nothing, so an Encode error leaves a channel clean.
func Encode(m Message) ([]byte, error) {
	t := m.Type()
	side, ok := sideOfType(t)
	if !ok {
		return nil, fmt.Errorf("ipc: encode: unknown message type %T", m)
	}

	var payload []byte
	var err error
	switch v := m.(type) {
	case WorkerConfig:
		payload, err = encodeConfig(v)
	case Frontier:
		payload = encodeFrontier(v)
	case Momentum:
		payload, err = EncodeMomentum(v.Detailed)
	case RangeEnd:
		payload = encodeRangeEnd(v)
	case Anchor:
		payload, err = encodeAnchor(v)
	case SourceError:
		payload, err = encodeSourceError(v)
	case FrontierRequest:
		payload = nil
	case MomentaRequest:
		payload, err = encodeMomentaRequest(v)
	case AnchorRequest:
		payload = nil
	case Committed:
		payload, err = v.Commitment.Encode()
	default:
		return nil, fmt.Errorf("ipc: encode: unhandled message type %T", m)
	}
	if err != nil {
		return nil, fmt.Errorf("ipc: encode %T: %w", m, err)
	}
	return assembleFrame(t, payload, capOf(side))
}

// WriteMessage writes Encode(m) with a single Write call.
func WriteMessage(w io.Writer, m Message) error {
	b, err := Encode(m)
	if err != nil {
		return err
	}
	_, err = w.Write(b)
	return err
}

// ReadMessage reads one frame sent by from, with from's cap, and decodes it strictly. It rejects a
// Side other than Supervisor or Worker before reading any byte. It returns message values, never
// pointers. readFrame's I/O errors are returned so errors.Is matches io.EOF and
// io.ErrUnexpectedEOF; every protocol violation wraps ErrMalformed.
func ReadMessage(r io.Reader, from Side) (Message, error) {
	if from != Supervisor && from != Worker {
		return nil, fmt.Errorf("ipc: invalid side %d: %w", from, ErrMalformed)
	}
	t, payload, err := readFrame(r, capOf(from))
	if err != nil {
		return nil, err
	}
	side, ok := sideOfType(t)
	if !ok || side != from {
		return nil, fmt.Errorf("ipc: type %#x is not sent by side %d: %w", byte(t), from, ErrMalformed)
	}
	m, err := decodePayload(t, payload)
	if err != nil {
		return nil, fmt.Errorf("ipc: decode type %#x: %w: %w", byte(t), err, ErrMalformed)
	}
	return m, nil
}

func decodePayload(t Type, payload []byte) (Message, error) {
	switch t {
	case TypeConfig:
		return decodeConfig(payload)
	case TypeFrontier:
		return decodeFrontier(payload)
	case TypeMomentum:
		return decodeMomentumMessage(payload)
	case TypeRangeEnd:
		return decodeRangeEnd(payload)
	case TypeAnchor:
		return decodeAnchor(payload)
	case TypeSourceError:
		return decodeSourceError(payload)
	case TypeFrontierRequest:
		return decodeFrontierRequest(payload)
	case TypeMomentaRequest:
		return decodeMomentaRequest(payload)
	case TypeAnchorRequest:
		return decodeAnchorRequest(payload)
	case TypeCommitted:
		return decodeCommitted(payload)
	default:
		return nil, fmt.Errorf("ipc: unknown type %#x", byte(t))
	}
}

// --- Config ---

func encodeConfig(c WorkerConfig) ([]byte, error) {
	if len(c.Role) > maxRoleLen {
		return nil, fmt.Errorf("role exceeds %d bytes", maxRoleLen)
	}
	if len(c.NetworkName) > maxNetworkNameLen {
		return nil, fmt.Errorf("networkName exceeds %d bytes", maxNetworkNameLen)
	}
	if len(c.StoragePath) > maxStoragePathLen {
		return nil, fmt.Errorf("storagePath exceeds %d bytes", maxStoragePathLen)
	}
	w := &pwriter{}
	w.u32(c.DomainID)
	w.raw(c.ExecutorAddress[:])
	w.raw(c.STFSpecHash[:])
	w.u64(c.ConfirmationDepth)
	w.str16(c.Role)
	w.str16(c.NetworkName)
	w.str16(c.StoragePath)
	return w.buf, nil
}

func decodeConfig(p []byte) (Message, error) {
	r := newPReader(p)
	var c WorkerConfig
	domainID, err := r.u32()
	if err != nil {
		return nil, err
	}
	addr, err := r.fixed(20)
	if err != nil {
		return nil, err
	}
	hash, err := r.fixed(32)
	if err != nil {
		return nil, err
	}
	depth, err := r.u64()
	if err != nil {
		return nil, err
	}
	role, err := r.str16(maxRoleLen)
	if err != nil {
		return nil, err
	}
	network, err := r.str16(maxNetworkNameLen)
	if err != nil {
		return nil, err
	}
	storage, err := r.str16(maxStoragePathLen)
	if err != nil {
		return nil, err
	}
	if err := r.end(); err != nil {
		return nil, err
	}
	c.DomainID = domainID
	copy(c.ExecutorAddress[:], addr)
	copy(c.STFSpecHash[:], hash)
	c.ConfirmationDepth = depth
	c.Role = role
	c.NetworkName = network
	c.StoragePath = storage
	return c, nil
}

// --- Frontier ---

func encodeFrontier(f Frontier) []byte {
	w := &pwriter{}
	w.u64(f.Height)
	return w.buf
}

func decodeFrontier(p []byte) (Message, error) {
	r := newPReader(p)
	h, err := r.u64()
	if err != nil {
		return nil, err
	}
	if err := r.end(); err != nil {
		return nil, err
	}
	return Frontier{Height: h}, nil
}

// --- Momentum ---

func decodeMomentumMessage(p []byte) (Message, error) {
	dm, err := DecodeMomentum(p)
	if err != nil {
		return nil, err
	}
	return Momentum{Detailed: dm}, nil
}

// --- RangeEnd ---

func encodeRangeEnd(r RangeEnd) []byte {
	w := &pwriter{}
	w.u32(r.Count)
	return w.buf
}

func decodeRangeEnd(p []byte) (Message, error) {
	r := newPReader(p)
	c, err := r.u32()
	if err != nil {
		return nil, err
	}
	if err := r.end(); err != nil {
		return nil, err
	}
	return RangeEnd{Count: c}, nil
}

// --- Anchor ---

func encodeAnchor(a Anchor) ([]byte, error) {
	if !a.Found && (a.PostStateRoot != (types.Hash{}) || a.LastInputSeq != 0) {
		return nil, errors.New("anchor not found requires a zero root and a zero seq")
	}
	w := &pwriter{}
	if a.Found {
		w.u8(1)
	} else {
		w.u8(0)
	}
	w.raw(a.PostStateRoot[:])
	w.u64(a.LastInputSeq)
	return w.buf, nil
}

func decodeAnchor(p []byte) (Message, error) {
	r := newPReader(p)
	foundByte, err := r.u8()
	if err != nil {
		return nil, err
	}
	if foundByte > 1 {
		return nil, errPayload
	}
	rootBytes, err := r.fixed(32)
	if err != nil {
		return nil, err
	}
	seq, err := r.u64()
	if err != nil {
		return nil, err
	}
	if err := r.end(); err != nil {
		return nil, err
	}
	var root types.Hash
	copy(root[:], rootBytes)
	found := foundByte == 1
	if !found && (root != (types.Hash{}) || seq != 0) {
		return nil, errPayload
	}
	return Anchor{Found: found, PostStateRoot: root, LastInputSeq: seq}, nil
}

// --- SourceError ---

func encodeSourceError(s SourceError) ([]byte, error) {
	if s.Kind != SourceUnavailable && s.Kind != SourceHalted {
		return nil, fmt.Errorf("invalid source error kind %d", s.Kind)
	}
	if len(s.Message) > MaxSourceErrorLen {
		return nil, fmt.Errorf("message exceeds %d bytes", MaxSourceErrorLen)
	}
	w := &pwriter{}
	w.u8(uint8(s.Kind))
	w.str16(s.Message)
	return w.buf, nil
}

func decodeSourceError(p []byte) (Message, error) {
	r := newPReader(p)
	kind, err := r.u8()
	if err != nil {
		return nil, err
	}
	if kind != uint8(SourceUnavailable) && kind != uint8(SourceHalted) {
		return nil, errPayload
	}
	msg, err := r.str16(MaxSourceErrorLen)
	if err != nil {
		return nil, err
	}
	if err := r.end(); err != nil {
		return nil, err
	}
	return SourceError{Kind: SourceErrorKind(kind), Message: msg}, nil
}

// --- FrontierRequest / AnchorRequest ---

func decodeFrontierRequest(p []byte) (Message, error) {
	if err := newPReader(p).end(); err != nil {
		return nil, err
	}
	return FrontierRequest{}, nil
}

func decodeAnchorRequest(p []byte) (Message, error) {
	if err := newPReader(p).end(); err != nil {
		return nil, err
	}
	return AnchorRequest{}, nil
}

// --- MomentaRequest ---

func encodeMomentaRequest(m MomentaRequest) ([]byte, error) {
	if m.Height < 1 {
		return nil, errors.New("height must be at least 1")
	}
	if m.Count < 1 || m.Count > MaxMomentaCount {
		return nil, fmt.Errorf("count %d out of range [1, %d]", m.Count, MaxMomentaCount)
	}
	w := &pwriter{}
	w.u64(m.Height)
	w.u64(m.Count)
	return w.buf, nil
}

func decodeMomentaRequest(p []byte) (Message, error) {
	r := newPReader(p)
	height, err := r.u64()
	if err != nil {
		return nil, err
	}
	count, err := r.u64()
	if err != nil {
		return nil, err
	}
	if err := r.end(); err != nil {
		return nil, err
	}
	if height < 1 || count < 1 || count > MaxMomentaCount {
		return nil, errPayload
	}
	return MomentaRequest{Height: height, Count: count}, nil
}

// --- Committed ---

func decodeCommitted(p []byte) (Message, error) {
	c, err := codec.DecodeBatchCommitment(p)
	if err != nil {
		return nil, err
	}
	return Committed{Commitment: c}, nil
}
