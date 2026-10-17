package ipc

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"path/filepath"
	"strconv"

	"github.com/zenon-network/go-zenon/chain/nom"
	"github.com/zenon-network/go-zenon/common/types"
	"github.com/zenon-network/go-zenon/vm/embedded/definition"

	"github.com/digitalSloth/go-executor/internal/batch"
	"github.com/digitalSloth/go-executor/internal/codec"
	"github.com/digitalSloth/go-executor/internal/ledger"
)

type Type uint8

const (
	TypeConfig            Type = 0x01 // S→W
	TypeFrontier          Type = 0x02 // S→W
	TypeMomentum          Type = 0x03 // S→W
	TypeRangeEnd          Type = 0x04 // S→W
	TypeAnchor            Type = 0x05 // S→W
	TypeSourceError       Type = 0x06 // S→W
	TypeAssignment        Type = 0x07 // S→W
	TypeCap               Type = 0x08 // S→W
	TypeCursor            Type = 0x09 // S→W
	TypeAccepted          Type = 0x0A // S→W
	TypeFrontierRequest   Type = 0x81 // W→S
	TypeMomentaRequest    Type = 0x82 // W→S
	TypeAnchorRequest     Type = 0x83 // W→S
	TypeCommitted         Type = 0x84 // W→S
	TypeAssignmentRequest Type = 0x85 // W→S
	TypeCapRequest        Type = 0x86 // W→S
	TypeRetract           Type = 0x87 // W→S
	TypeRecovered         Type = 0x88 // W→S
	TypeCursorRequest     Type = 0x89 // W→S
	TypeAcceptedRequest   Type = 0x8A // W→S
	TypeWithdrawal        Type = 0x8B // W→S
	TypeWithdrawalsEnd    Type = 0x8C // W→S
)

// Side names the sender of a frame. Supervisor sends 0x01-0x0A and is read with MaxFrame;
// Worker sends 0x81-0x8C and is read with MaxWorkerFrame.
type Side uint8

const (
	Supervisor Side = 1
	Worker     Side = 2
)

const (
	MaxAcceptedEntries = 64   // entries in one Accepted reply; Settlement caps a batch at 64 inputs
	MaxMomentaCount    = 1024 // internal/ledger/follower.go:18 (rpcMaxCountSize, unexported there)
	MaxSourceErrorLen  = 4096 // the supervisor truncates a source error's text to this
	maxNetworkNameLen  = 256
	maxStoragePathLen  = 4096

	// MaxHandOffWithdrawals bounds the Withdrawal frames of one batch's
	// hand-off: MaxBatchInputs inputs, each with at most
	// MaxOutboxPerResult outbox messages (2,112).
	MaxHandOffWithdrawals = batch.MaxBatchInputs * codec.MaxOutboxPerResult

	// withdrawalMessageLen is the encoded size of every kind 1
	// OutboxMessage: sourceContract 32, inputIndex 8, outboxIndex 4, kind
	// 1, recipient 20, asset 32, amount 32, payload length 4 (the payload
	// is always empty).
	withdrawalMessageLen = 133
)

// Message is implemented by exactly the twenty-two types below, all
// with value receivers.
type Message interface{ Type() Type }

type WorkerConfig struct {
	DomainID          uint32
	ExecutorAddress   types.Address
	STFSpecHash       types.Hash
	ConfirmationDepth uint64
	NetworkName       string
	StoragePath       string
	MaxPendingBatches uint64
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

type AssignmentRequest struct {
	Height      uint64
	Hash        types.Hash
	NextOrdinal uint64
}
type Assignment struct{ Entries []ledger.AssignmentEntry }

func (AssignmentRequest) Type() Type { return TypeAssignmentRequest }
func (Assignment) Type() Type        { return TypeAssignment }

type CapRequest struct {
	Height uint64
	Hash   types.Hash
	Assets []codec.AssetID
}
type Cap struct{ Amounts []codec.U256 }

func (CapRequest) Type() Type { return TypeCapRequest }
func (Cap) Type() Type        { return TypeCap }

// CursorRequest asks for Settlement's nextBatchId for the worker's domain.
type CursorRequest struct{}

// Cursor carries the supervisor's highest observed nextBatchId for the domain.
type Cursor struct{ NextBatchID uint64 }

func (CursorRequest) Type() Type { return TypeCursorRequest }
func (Cursor) Type() Type        { return TypeCursor }

// AcceptedRequest asks for Settlement's accepted entries for the worker's domain, from FromBatchID
// upward.
type AcceptedRequest struct{ FromBatchID uint64 }

// Accepted carries each entry's BatchId and the fields settlement.EntryMatches compares; every
// other BatchEntry field decodes as its zero value.
type Accepted struct {
	NextBatchID uint64
	Entries     []*definition.BatchEntry
}

func (AcceptedRequest) Type() Type { return TypeAcceptedRequest }
func (Accepted) Type() Type        { return TypeAccepted }

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

// Retract tells the supervisor the worker holds no batch at or above FromBatchID.
type Retract struct{ FromBatchID uint64 }

// Recovered re-reports a batch the worker committed earlier and holds above its finalized
// anchor.
type Recovered struct{ Commitment codec.BatchCommitment }

func (FrontierRequest) Type() Type { return TypeFrontierRequest }
func (MomentaRequest) Type() Type  { return TypeMomentaRequest }
func (AnchorRequest) Type() Type   { return TypeAnchorRequest }
func (Committed) Type() Type       { return TypeCommitted }
func (Retract) Type() Type         { return TypeRetract }
func (Recovered) Type() Type       { return TypeRecovered }

// Withdrawal carries one L1 withdrawal of batch BatchID. Message is a
// canonical kind 1 OutboxMessage; its inputIndex and outboxIndex are
// its position in the batch, and a batch's Withdrawal frames are
// strictly ascending in that pair. Proof is its MerkleProof under the
// batch's OutboxRoot.
type Withdrawal struct {
	BatchID uint64
	Message []byte
	Proof   []byte
}

// WithdrawalsEnd closes batch BatchID's hand-off.
type WithdrawalsEnd struct{ BatchID uint64 }

func (Withdrawal) Type() Type     { return TypeWithdrawal }
func (WithdrawalsEnd) Type() Type { return TypeWithdrawalsEnd }

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
	case TypeConfig, TypeFrontier, TypeMomentum, TypeRangeEnd, TypeAnchor, TypeSourceError, TypeAssignment, TypeCap, TypeCursor, TypeAccepted:
		return Supervisor, true
	case TypeFrontierRequest, TypeMomentaRequest, TypeAnchorRequest, TypeCommitted, TypeAssignmentRequest, TypeCapRequest, TypeRetract, TypeRecovered, TypeCursorRequest, TypeAcceptedRequest, TypeWithdrawal, TypeWithdrawalsEnd:
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
	case Retract:
		payload = encodeRetract(v)
	case Recovered:
		payload, err = v.Commitment.Encode()
	case AssignmentRequest:
		payload, err = encodeAssignmentRequest(v)
	case Assignment:
		payload, err = encodeAssignment(v)
	case CapRequest:
		payload, err = encodeCapRequest(v)
	case Cap:
		payload, err = encodeCap(v)
	case CursorRequest:
		payload = nil
	case Cursor:
		payload = encodeCursor(v)
	case AcceptedRequest:
		payload = encodeAcceptedRequest(v)
	case Accepted:
		payload, err = encodeAccepted(v)
	case Withdrawal:
		payload, err = encodeWithdrawal(v)
	case WithdrawalsEnd:
		payload = encodeWithdrawalsEnd(v)
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
	case TypeRetract:
		return decodeRetract(payload)
	case TypeWithdrawal:
		return decodeWithdrawal(payload)
	case TypeWithdrawalsEnd:
		return decodeWithdrawalsEnd(payload)
	case TypeRecovered:
		return decodeRecovered(payload)
	case TypeAssignmentRequest:
		return decodeAssignmentRequest(payload)
	case TypeAssignment:
		return decodeAssignment(payload)
	case TypeCapRequest:
		return decodeCapRequest(payload)
	case TypeCap:
		return decodeCap(payload)
	case TypeCursorRequest:
		return decodeCursorRequest(payload)
	case TypeCursor:
		return decodeCursor(payload)
	case TypeAcceptedRequest:
		return decodeAcceptedRequest(payload)
	case TypeAccepted:
		return decodeAccepted(payload)
	default:
		return nil, fmt.Errorf("ipc: unknown type %#x", byte(t))
	}
}

// --- Config ---

func encodeConfig(c WorkerConfig) ([]byte, error) {
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
	w.str16(c.NetworkName)
	w.str16(c.StoragePath)
	w.u64(c.MaxPendingBatches)
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
	network, err := r.str16(maxNetworkNameLen)
	if err != nil {
		return nil, err
	}
	storage, err := r.str16(maxStoragePathLen)
	if err != nil {
		return nil, err
	}
	maxPending, err := r.u64()
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
	c.NetworkName = network
	c.StoragePath = storage
	c.MaxPendingBatches = maxPending
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

// --- Retract ---

func encodeRetract(m Retract) []byte {
	w := &pwriter{}
	w.u64(m.FromBatchID)
	return w.buf
}

func decodeRetract(p []byte) (Message, error) {
	r := newPReader(p)
	id, err := r.u64()
	if err != nil {
		return nil, err
	}
	if err := r.end(); err != nil {
		return nil, err
	}
	return Retract{FromBatchID: id}, nil
}

// --- Withdrawal, WithdrawalsEnd ---

// checkWithdrawalMessage requires msg to be a canonical kind 1
// OutboxMessage of exactly withdrawalMessageLen bytes. The kind check
// matters: a kind 0 leaf sits in the same tree, so its proof also
// verifies, and a kind 0 message with a 52-byte payload has the same
// length.
func checkWithdrawalMessage(msg []byte) error {
	if len(msg) != withdrawalMessageLen {
		return fmt.Errorf("withdrawal message is %d bytes, want %d", len(msg), withdrawalMessageLen)
	}
	m, err := codec.DecodeOutboxMessage(msg)
	if err != nil {
		return fmt.Errorf("withdrawal message: %w", err)
	}
	if _, ok := m.Target.(codec.L1Withdrawal); !ok {
		return errors.New("withdrawal message is not a kind 1 outbox message")
	}
	return nil
}

// CheckProof requires proof to be a well-formed MerkleProof: a u32 step
// count of at most codec.MaxMerkleProofDepth, then exactly that many
// steps of a side byte (0 or 1) and a 32-byte sibling.
func CheckProof(proof []byte) error {
	if len(proof) < 4 {
		return fmt.Errorf("proof is %d bytes, want at least 4", len(proof))
	}
	count := binary.BigEndian.Uint32(proof)
	if count > codec.MaxMerkleProofDepth {
		return fmt.Errorf("proof has %d steps, maximum %d", count, codec.MaxMerkleProofDepth)
	}
	if want := 4 + 33*int(count); len(proof) != want {
		return fmt.Errorf("proof is %d bytes, want %d for %d steps", len(proof), want, count)
	}
	for i := 0; i < int(count); i++ {
		if side := proof[4+33*i]; side > 1 {
			return fmt.Errorf("proof step %d has side %d", i, side)
		}
	}
	return nil
}

func encodeWithdrawal(m Withdrawal) ([]byte, error) {
	if err := checkWithdrawalMessage(m.Message); err != nil {
		return nil, err
	}
	if err := CheckProof(m.Proof); err != nil {
		return nil, err
	}
	w := &pwriter{}
	w.u64(m.BatchID)
	w.raw(m.Message)
	w.raw(m.Proof)
	return w.buf, nil
}

func decodeWithdrawal(p []byte) (Message, error) {
	r := newPReader(p)
	id, err := r.u64()
	if err != nil {
		return nil, err
	}
	msg, err := r.fixed(withdrawalMessageLen)
	if err != nil {
		return nil, err
	}
	if err := checkWithdrawalMessage(msg); err != nil {
		return nil, err
	}
	proof, err := r.fixed(r.remaining())
	if err != nil {
		return nil, err
	}
	if err := CheckProof(proof); err != nil {
		return nil, err
	}
	return Withdrawal{BatchID: id, Message: append([]byte{}, msg...), Proof: append([]byte{}, proof...)}, nil
}

func encodeWithdrawalsEnd(m WithdrawalsEnd) []byte {
	w := &pwriter{}
	w.u64(m.BatchID)
	return w.buf
}

func decodeWithdrawalsEnd(p []byte) (Message, error) {
	r := newPReader(p)
	id, err := r.u64()
	if err != nil {
		return nil, err
	}
	if err := r.end(); err != nil {
		return nil, err
	}
	return WithdrawalsEnd{BatchID: id}, nil
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

// --- AssignmentRequest ---

func encodeAssignmentRequest(m AssignmentRequest) ([]byte, error) {
	if m.Height < 1 {
		return nil, errors.New("height must be at least 1")
	}
	w := &pwriter{}
	w.u64(m.Height)
	w.raw(m.Hash[:])
	w.u64(m.NextOrdinal)
	return w.buf, nil
}

func decodeAssignmentRequest(p []byte) (Message, error) {
	r := newPReader(p)
	height, err := r.u64()
	if err != nil {
		return nil, err
	}
	hashBytes, err := r.fixed(32)
	if err != nil {
		return nil, err
	}
	next, err := r.u64()
	if err != nil {
		return nil, err
	}
	if err := r.end(); err != nil {
		return nil, err
	}
	if height < 1 {
		return nil, errPayload
	}
	m := AssignmentRequest{Height: height, NextOrdinal: next}
	copy(m.Hash[:], hashBytes)
	return m, nil
}

// --- CapRequest and Cap ---

func encodeCapRequest(m CapRequest) ([]byte, error) {
	if m.Height < 1 {
		return nil, errors.New("height must be at least 1")
	}
	if n := len(m.Assets); n < 1 || n > codec.MaxAssetsPerBatch {
		return nil, fmt.Errorf("%d assets outside [1, %d]", n, codec.MaxAssetsPerBatch)
	}
	w := &pwriter{buf: make([]byte, 0, 8+32+1+len(m.Assets)*32)}
	w.u64(m.Height)
	w.raw(m.Hash[:])
	w.u8(uint8(len(m.Assets)))
	for _, a := range m.Assets {
		w.raw(a[:])
	}
	return w.buf, nil
}

func decodeCapRequest(p []byte) (Message, error) {
	r := newPReader(p)
	height, err := r.u64()
	if err != nil {
		return nil, err
	}
	hashBytes, err := r.fixed(32)
	if err != nil {
		return nil, err
	}
	n, err := r.u8()
	if err != nil {
		return nil, err
	}
	if n < 1 || n > codec.MaxAssetsPerBatch || int(n)*32 != r.remaining() {
		return nil, errPayload
	}
	if height < 1 {
		return nil, errPayload
	}
	m := CapRequest{Height: height, Assets: make([]codec.AssetID, n)}
	copy(m.Hash[:], hashBytes)
	for i := range m.Assets {
		b, err := r.fixed(32)
		if err != nil {
			return nil, err
		}
		copy(m.Assets[i][:], b)
	}
	return m, nil
}

func encodeCap(m Cap) ([]byte, error) {
	if n := len(m.Amounts); n < 1 || n > codec.MaxAssetsPerBatch {
		return nil, fmt.Errorf("%d amounts outside [1, %d]", n, codec.MaxAssetsPerBatch)
	}
	w := &pwriter{buf: make([]byte, 0, 1+len(m.Amounts)*32)}
	w.u8(uint8(len(m.Amounts)))
	for _, a := range m.Amounts {
		w.raw(a[:])
	}
	return w.buf, nil
}

func decodeCap(p []byte) (Message, error) {
	r := newPReader(p)
	n, err := r.u8()
	if err != nil {
		return nil, err
	}
	if n < 1 || n > codec.MaxAssetsPerBatch || int(n)*32 != r.remaining() {
		return nil, errPayload
	}
	m := Cap{Amounts: make([]codec.U256, n)}
	for i := range m.Amounts {
		b, err := r.fixed(32)
		if err != nil {
			return nil, err
		}
		copy(m.Amounts[i][:], b)
	}
	return m, nil
}

// --- Assignment ---

// assignmentEntryLen is the wire size of one entry: three u64 fields and a 32-byte hash.
const assignmentEntryLen = 8 + 8 + 32 + 8

func encodeAssignment(a Assignment) ([]byte, error) {
	if uint64(len(a.Entries)) > math.MaxUint32 {
		return nil, fmt.Errorf("%d entries exceed a u32 count", len(a.Entries))
	}
	w := &pwriter{buf: make([]byte, 0, 4+len(a.Entries)*assignmentEntryLen)}
	w.u32(uint32(len(a.Entries)))
	for i, e := range a.Entries {
		if e.SendMomentumHeight == 0 {
			return nil, fmt.Errorf("entry %d has send momentum height 0", i)
		}
		w.u64(e.Ordinal)
		w.u64(e.GlobalInputIndex)
		w.raw(e.SourceAccountBlockHash[:])
		w.u64(e.SendMomentumHeight)
	}
	return w.buf, nil
}

func decodeAssignment(p []byte) (Message, error) {
	r := newPReader(p)
	n, err := r.u32()
	if err != nil {
		return nil, err
	}
	if uint64(n)*assignmentEntryLen != uint64(r.remaining()) {
		return nil, errPayload
	}
	if n == 0 {
		return Assignment{}, nil
	}
	entries := make([]ledger.AssignmentEntry, n)
	for i := range entries {
		e := &entries[i]
		if e.Ordinal, err = r.u64(); err != nil {
			return nil, err
		}
		if e.GlobalInputIndex, err = r.u64(); err != nil {
			return nil, err
		}
		hashBytes, err := r.fixed(32)
		if err != nil {
			return nil, err
		}
		copy(e.SourceAccountBlockHash[:], hashBytes)
		if e.SendMomentumHeight, err = r.u64(); err != nil {
			return nil, err
		}
		if e.SendMomentumHeight == 0 {
			return nil, errPayload
		}
	}
	return Assignment{Entries: entries}, nil
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

// --- CursorRequest and Cursor ---

func decodeCursorRequest(p []byte) (Message, error) {
	if err := newPReader(p).end(); err != nil {
		return nil, err
	}
	return CursorRequest{}, nil
}

func encodeCursor(m Cursor) []byte {
	w := &pwriter{}
	w.u64(m.NextBatchID)
	return w.buf
}

func decodeCursor(p []byte) (Message, error) {
	r := newPReader(p)
	n, err := r.u64()
	if err != nil {
		return nil, err
	}
	if err := r.end(); err != nil {
		return nil, err
	}
	return Cursor{NextBatchID: n}, nil
}

// --- AcceptedRequest and Accepted ---

func encodeAcceptedRequest(m AcceptedRequest) []byte {
	w := &pwriter{}
	w.u64(m.FromBatchID)
	return w.buf
}

func decodeAcceptedRequest(p []byte) (Message, error) {
	r := newPReader(p)
	from, err := r.u64()
	if err != nil {
		return nil, err
	}
	if err := r.end(); err != nil {
		return nil, err
	}
	return AcceptedRequest{FromBatchID: from}, nil
}

func encodeAccepted(m Accepted) ([]byte, error) {
	if len(m.Entries) > MaxAcceptedEntries {
		return nil, fmt.Errorf("%d entries exceed %d", len(m.Entries), MaxAcceptedEntries)
	}
	w := &pwriter{}
	w.u64(m.NextBatchID)
	w.u32(uint32(len(m.Entries)))
	for i, e := range m.Entries {
		if e == nil {
			return nil, fmt.Errorf("entry %d is nil", i)
		}
		w.u64(e.BatchId)
		w.u16(e.ProtocolVersion)
		w.u64(e.FirstInputSeq)
		w.u64(e.LastInputSeq)
		for _, h := range []types.Hash{e.PreStateRoot, e.PostStateRoot, e.InputRoot, e.ReceiptRoot, e.EventRoot, e.OutboxRoot, e.DaHash} {
			w.raw(h[:])
		}
		w.u8(e.DaMode)
		w.u32(uint32(len(e.AssetFlowSummary)))
		w.raw(e.AssetFlowSummary)
		w.u32(uint32(len(e.ProofData)))
		w.raw(e.ProofData)
	}
	return w.buf, nil
}

func decodeAccepted(p []byte) (Message, error) {
	r := newPReader(p)
	next, err := r.u64()
	if err != nil {
		return nil, err
	}
	count, err := r.u32()
	if err != nil {
		return nil, err
	}
	if count > MaxAcceptedEntries {
		return nil, errPayload
	}
	entries := make([]*definition.BatchEntry, 0, count)
	for i := uint32(0); i < count; i++ {
		e := &definition.BatchEntry{}
		if e.BatchId, err = r.u64(); err != nil {
			return nil, err
		}
		if e.ProtocolVersion, err = r.u16(); err != nil {
			return nil, err
		}
		if e.FirstInputSeq, err = r.u64(); err != nil {
			return nil, err
		}
		if e.LastInputSeq, err = r.u64(); err != nil {
			return nil, err
		}
		for _, h := range []*types.Hash{&e.PreStateRoot, &e.PostStateRoot, &e.InputRoot, &e.ReceiptRoot, &e.EventRoot, &e.OutboxRoot, &e.DaHash} {
			b, err := r.fixed(len(h))
			if err != nil {
				return nil, err
			}
			copy(h[:], b)
		}
		if e.DaMode, err = r.u8(); err != nil {
			return nil, err
		}
		if e.AssetFlowSummary, err = r.lengthPrefixed(); err != nil {
			return nil, err
		}
		if e.ProofData, err = r.lengthPrefixed(); err != nil {
			return nil, err
		}
		entries = append(entries, e)
	}
	if err := r.end(); err != nil {
		return nil, err
	}
	return Accepted{NextBatchID: next, Entries: entries}, nil
}

// lengthPrefixed reads a u32 length and that many bytes, copied out of the payload.
func (r *preader) lengthPrefixed() ([]byte, error) {
	n, err := r.u32()
	if err != nil {
		return nil, err
	}
	b, err := r.fixed(int(n))
	if err != nil {
		return nil, err
	}
	return append([]byte{}, b...), nil
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

// --- Recovered ---

func decodeRecovered(p []byte) (Message, error) {
	c, err := codec.DecodeBatchCommitment(p)
	if err != nil {
		return nil, err
	}
	return Recovered{Commitment: c}, nil
}
