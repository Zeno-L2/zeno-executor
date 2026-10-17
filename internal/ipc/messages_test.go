package ipc

import (
	"bytes"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/zenon-network/go-zenon/common/types"
	"github.com/zenon-network/go-zenon/vm/embedded/definition"

	"github.com/digitalSloth/go-executor/internal/codec"
	"github.com/digitalSloth/go-executor/internal/ipc/ipctest"
	"github.com/digitalSloth/go-executor/internal/ledger"
)

func validCommitment() codec.BatchCommitment {
	return codec.BatchCommitment{
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
		DomainID:          1,
		BatchID:           2,
		FirstInputSeq:     3,
		LastInputSeq:      4,
		PreStateRoot:      types.Hash{0x01},
		PostStateRoot:     types.Hash{0x02},
		InputRoot:         types.Hash{0x03},
		ReceiptRoot:       types.Hash{0x04},
		EventRoot:         types.Hash{0x05},
		OutboxRoot:        types.Hash{0x06},
		DAHash:            types.Hash{0x07},
		DAMode:            1,
		ExecutorID:        types.Address{0x08},
		SubmittedAtHeight: 9,
	}
}

func testMomentum() Momentum {
	dm := ipctest.Chain([][]ipctest.Block{{{Address: addr(1), Data: []byte("only")}}})[0]
	return Momentum{Detailed: dm}
}

type msgCase struct {
	name string
	side Side
	msg  Message
}

// withdrawalMessage returns the encoding of a kind 1 outbox message for
// the given indexes. The fixtures panic on an encode error because
// validTable has no *testing.T.
func withdrawalMessage(input uint64, outbox uint32) []byte {
	enc, err := codec.OutboxMessage{
		SourceContract: types.Hash{0x04},
		InputIndex:     input,
		OutboxIndex:    outbox,
		Target:         codec.L1Withdrawal{RecipientL1: types.Address{0x01}, Asset: codec.AssetIDFromZTS(types.ZnnTokenStandard), Amount: codec.U256{31: 9}},
	}.Encode()
	if err != nil {
		panic(err)
	}
	return enc
}

// withdrawalFixture returns a kind 1 message and its one-step proof
// from a two-leaf tree.
func withdrawalFixture() (msg, proof []byte) {
	msg = withdrawalMessage(3, 0)
	other := withdrawalMessage(3, 1)
	return msg, codec.MerkleProofs([][]byte{msg, other})[0]
}

// proofOf returns a proof with count steps whose side byte is side.
func proofOf(count uint32, side byte) []byte {
	w := &pwriter{}
	w.u32(count)
	for i := uint32(0); i < count; i++ {
		w.u8(side)
		w.raw(make([]byte, 32))
	}
	return w.buf
}

func validTable() []msgCase {
	wMsg, wProof := withdrawalFixture()
	return []msgCase{
		{"config", Supervisor, WorkerConfig{
			DomainID:          7,
			ExecutorAddress:   types.Address{0x42},
			STFSpecHash:       types.Hash{0xAA},
			ConfirmationDepth: 2,
			NetworkName:       "test",
			StoragePath:       "/data",
			MaxPendingBatches: 8,
		}},
		{"frontier", Supervisor, Frontier{Height: 12}},
		{"momentum", Supervisor, testMomentum()},
		{"rangeEnd", Supervisor, RangeEnd{Count: 3}},
		{"anchorFound", Supervisor, Anchor{Found: true, PostStateRoot: types.Hash{0x01}, LastInputSeq: 5}},
		{"anchorNotFound", Supervisor, Anchor{Found: false}},
		{"sourceError", Supervisor, SourceError{Kind: SourceHalted, Message: "bad"}},
		{"frontierRequest", Worker, FrontierRequest{}},
		{"momentaRequest", Worker, MomentaRequest{Height: 1, Count: 10}},
		{"anchorRequest", Worker, AnchorRequest{}},
		{"committed", Worker, Committed{Commitment: validCommitment()}},
		{"retract", Worker, Retract{FromBatchID: 5}},
		{"recovered", Worker, Recovered{Commitment: validCommitment()}},
		{"assignmentRequest", Worker, AssignmentRequest{Height: 9, Hash: types.Hash{0x0A}, NextOrdinal: 3}},
		{"assignment", Supervisor, Assignment{Entries: []ledger.AssignmentEntry{
			{Ordinal: 3, GlobalInputIndex: 10, SourceAccountBlockHash: types.Hash{0x01}, SendMomentumHeight: 7},
			{Ordinal: 4, GlobalInputIndex: 12, SourceAccountBlockHash: types.Hash{0x02}, SendMomentumHeight: 8},
		}}},
		{"assignmentEmpty", Supervisor, Assignment{}},
		{"capRequest", Worker, CapRequest{Height: 9, Hash: types.Hash{1}, Assets: []codec.AssetID{codec.AssetIDFromZTS(types.ZnnTokenStandard), codec.AssetIDFromZTS(types.QsrTokenStandard)}}},
		{"cap", Supervisor, Cap{Amounts: []codec.U256{{31: 7}, {31: 8}}}},
		{"cursorRequest", Worker, CursorRequest{}},
		{"cursor", Supervisor, Cursor{NextBatchID: 12}},
		{"acceptedRequest", Worker, AcceptedRequest{FromBatchID: 6}},
		{"accepted", Supervisor, Accepted{NextBatchID: 9, Entries: []*definition.BatchEntry{acceptedEntry(6, 0), acceptedEntry(7, 100)}}},
		{"acceptedEmpty", Supervisor, Accepted{NextBatchID: 9}},
		{"withdrawal", Worker, Withdrawal{BatchID: 3, Message: wMsg, Proof: wProof}},
		{"withdrawalsEnd", Worker, WithdrawalsEnd{BatchID: 3}},
	}
}

func TestRoundTrip(t *testing.T) {
	for _, c := range validTable() {
		t.Run(c.name, func(t *testing.T) {
			enc, err := Encode(c.msg)
			if err != nil {
				t.Fatalf("Encode: %v", err)
			}
			got, err := ReadMessage(bytes.NewReader(enc), c.side)
			if err != nil {
				t.Fatalf("ReadMessage: %v", err)
			}
			reenc, err := Encode(got)
			if err != nil {
				t.Fatalf("re-Encode: %v", err)
			}
			if !bytes.Equal(enc, reenc) {
				t.Fatalf("round trip mismatch: %x != %x", enc, reenc)
			}
		})
	}
}

func TestShortAndTrailingInput(t *testing.T) {
	for _, c := range validTable() {
		t.Run(c.name, func(t *testing.T) {
			enc, err := Encode(c.msg)
			if err != nil {
				t.Fatalf("Encode: %v", err)
			}
			typ := c.msg.Type()
			payload := enc[5:] // strip the 4-byte length and 1-byte type

			for k := 0; k < len(payload); k++ {
				frame, err := assembleFrame(typ, payload[:k], MaxFrame)
				if err != nil {
					t.Fatalf("assembleFrame: %v", err)
				}
				if _, err := ReadMessage(bytes.NewReader(frame), c.side); !errors.Is(err, ErrMalformed) {
					t.Fatalf("short payload len %d: err = %v, want ErrMalformed", k, err)
				}
			}

			trailing := append(append([]byte{}, payload...), 0x00)
			frame, err := assembleFrame(typ, trailing, MaxFrame)
			if err != nil {
				t.Fatalf("assembleFrame: %v", err)
			}
			if _, err := ReadMessage(bytes.NewReader(frame), c.side); !errors.Is(err, ErrMalformed) {
				t.Fatalf("trailing byte: err = %v, want ErrMalformed", err)
			}
		})
	}
}

func TestStr16Overrun(t *testing.T) {
	// Config with short strings, so a tampered length header for one field cannot be masked by
	// another field's own limit check tripping first.
	cfg := WorkerConfig{
		DomainID:          1,
		ExecutorAddress:   types.Address{0x01},
		STFSpecHash:       types.Hash{0x02},
		ConfirmationDepth: 1,
		NetworkName:       "N",
		StoragePath:       "S",
		MaxPendingBatches: 1,
	}
	enc, err := Encode(cfg)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	payload := enc[5:]
	// Layout: u32 domainId(4) + 20B addr + 32B hash + u64 depth(8) =
	// 64 bytes fixed prefix, then str16 networkName, str16 storagePath.
	const fixed = 4 + 20 + 32 + 8
	networkLenOff := fixed
	storageLenOff := networkLenOff + 2 + len(cfg.NetworkName)

	overrun := func(name string, lenOff int) {
		t.Run("config/"+name, func(t *testing.T) {
			p := append([]byte{}, payload...)
			remaining := len(p) - lenOff - 2
			p[lenOff] = byte((remaining + 1) >> 8)
			p[lenOff+1] = byte(remaining + 1)
			frame, err := assembleFrame(TypeConfig, p, MaxFrame)
			if err != nil {
				t.Fatalf("assembleFrame: %v", err)
			}
			if _, err := ReadMessage(bytes.NewReader(frame), Supervisor); !errors.Is(err, ErrMalformed) {
				t.Fatalf("err = %v, want ErrMalformed", err)
			}
		})
	}
	overrun("networkName", networkLenOff)
	overrun("storagePath", storageLenOff)

	t.Run("sourceError/message", func(t *testing.T) {
		se := SourceError{Kind: SourceUnavailable, Message: "m"}
		enc, err := Encode(se)
		if err != nil {
			t.Fatalf("Encode: %v", err)
		}
		p := append([]byte{}, enc[5:]...)
		// Layout: u8 kind(1), then str16 message at offset 1.
		remaining := len(p) - 1 - 2
		p[1] = byte((remaining + 1) >> 8)
		p[2] = byte(remaining + 1)
		frame, err := assembleFrame(TypeSourceError, p, MaxFrame)
		if err != nil {
			t.Fatalf("assembleFrame: %v", err)
		}
		if _, err := ReadMessage(bytes.NewReader(frame), Supervisor); !errors.Is(err, ErrMalformed) {
			t.Fatalf("err = %v, want ErrMalformed", err)
		}
	})
}

func TestOverLimitStrings(t *testing.T) {
	base := WorkerConfig{
		DomainID:          1,
		ExecutorAddress:   types.Address{0x01},
		STFSpecHash:       types.Hash{0x02},
		ConfirmationDepth: 1,
		NetworkName:       "test",
		StoragePath:       "/data",
	}

	longNetwork := base
	longNetwork.NetworkName = strings.Repeat("n", maxNetworkNameLen+1)
	longStorage := base
	longStorage.StoragePath = strings.Repeat("s", maxStoragePathLen+1)

	for name, cfg := range map[string]WorkerConfig{"networkName": longNetwork, "storagePath": longStorage} {
		t.Run("config/"+name, func(t *testing.T) {
			if _, err := Encode(cfg); err == nil {
				t.Fatalf("Encode: want error")
			}
			// Build the over-limit frame by hand: valid base config's str16 fields spliced with
			// an oversized one, since Encode itself refuses to emit it.
			frame := handEncodeConfig(t, cfg)
			if _, err := ReadMessage(bytes.NewReader(frame), Supervisor); !errors.Is(err, ErrMalformed) {
				t.Fatalf("ReadMessage: err = %v, want ErrMalformed", err)
			}
		})
	}

	longMsg := SourceError{Kind: SourceUnavailable, Message: strings.Repeat("m", MaxSourceErrorLen+1)}
	if _, err := Encode(longMsg); err == nil {
		t.Fatalf("Encode SourceError: want error")
	}
	frame := handEncodeSourceError(t, longMsg)
	if _, err := ReadMessage(bytes.NewReader(frame), Supervisor); !errors.Is(err, ErrMalformed) {
		t.Fatalf("ReadMessage: err = %v, want ErrMalformed", err)
	}
}

// handEncodeConfig builds a Config frame bypassing Encode's length checks, so an over-limit
// string can reach ReadMessage.
func handEncodeConfig(t *testing.T, c WorkerConfig) []byte {
	t.Helper()
	w := &pwriter{}
	w.u32(c.DomainID)
	w.raw(c.ExecutorAddress[:])
	w.raw(c.STFSpecHash[:])
	w.u64(c.ConfirmationDepth)
	w.str16(c.NetworkName)
	w.str16(c.StoragePath)
	w.u64(c.MaxPendingBatches)
	frame, err := assembleFrame(TypeConfig, w.buf, MaxFrame)
	if err != nil {
		t.Fatalf("assembleFrame: %v", err)
	}
	return frame
}

func handEncodeSourceError(t *testing.T, s SourceError) []byte {
	t.Helper()
	w := &pwriter{}
	w.u8(uint8(s.Kind))
	w.str16(s.Message)
	frame, err := assembleFrame(TypeSourceError, w.buf, MaxFrame)
	if err != nil {
		t.Fatalf("assembleFrame: %v", err)
	}
	return frame
}

func TestConfigStringsAtLimitRoundTrip(t *testing.T) {
	cfg := WorkerConfig{
		DomainID:          1,
		ExecutorAddress:   types.Address{0x01},
		STFSpecHash:       types.Hash{0x02},
		ConfirmationDepth: 1,
		NetworkName:       strings.Repeat("n", maxNetworkNameLen),
		StoragePath:       strings.Repeat("s", maxStoragePathLen),
		MaxPendingBatches: 7,
	}
	enc, err := Encode(cfg)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	m, err := ReadMessage(bytes.NewReader(enc), Supervisor)
	if err != nil {
		t.Fatalf("ReadMessage: %v", err)
	}
	if got := m.(WorkerConfig); got != cfg {
		t.Fatalf("decoded config differs from the encoded one: got %+v", got)
	}
}

func TestUnknownTypes(t *testing.T) {
	known := map[byte]bool{
		0x01: true, 0x02: true, 0x03: true, 0x04: true, 0x05: true, 0x06: true, 0x07: true, 0x08: true, 0x09: true,
		0x81: true, 0x82: true, 0x83: true, 0x84: true, 0x85: true, 0x86: true, 0x87: true, 0x88: true, 0x89: true, 0x8A: true, 0x8B: true, 0x8C: true,
	}
	for b := 0; b <= 0xff; b++ {
		if known[byte(b)] {
			continue
		}
		frame, err := assembleFrame(Type(b), []byte{0x00}, MaxFrame)
		if err != nil {
			continue // exceeds MaxWorkerFrame's cap consideration is irrelevant here
		}
		for _, side := range []Side{Supervisor, Worker} {
			if _, err := ReadMessage(bytes.NewReader(frame), side); !errors.Is(err, ErrMalformed) {
				t.Fatalf("type %#x side %d: err = %v, want ErrMalformed", b, side, err)
			}
		}
	}
}

// TestWrongDirection builds each case from a real, well-formed payload out of validTable() and
// reads it with the opposite side, so direction is the only reason a read can fail: a nil or
// otherwise malformed payload would let 8 of the 10 types reject as short before the direction
// check ever runs.
func TestWrongDirection(t *testing.T) {
	for _, c := range validTable() {
		c := c
		wrong := Worker
		if c.side == Worker {
			wrong = Supervisor
		}
		t.Run(c.name, func(t *testing.T) {
			frame, err := Encode(c.msg)
			if err != nil {
				t.Fatalf("Encode: %v", err)
			}
			if _, err := ReadMessage(bytes.NewReader(frame), wrong); !errors.Is(err, ErrMalformed) {
				t.Fatalf("%s read as the wrong side: err = %v, want ErrMalformed", c.name, err)
			}
		})
	}
}

func TestOutOfRange(t *testing.T) {
	t.Run("momentaRequest count 0", func(t *testing.T) {
		if _, err := Encode(MomentaRequest{Height: 1, Count: 0}); err == nil {
			t.Fatalf("Encode: want error")
		}
		frame := handEncodeMomentaRequest(t, 1, 0)
		if _, err := ReadMessage(bytes.NewReader(frame), Worker); !errors.Is(err, ErrMalformed) {
			t.Fatalf("err = %v, want ErrMalformed", err)
		}
	})
	t.Run("momentaRequest count 1025", func(t *testing.T) {
		if _, err := Encode(MomentaRequest{Height: 1, Count: 1025}); err == nil {
			t.Fatalf("Encode: want error")
		}
		frame := handEncodeMomentaRequest(t, 1, 1025)
		if _, err := ReadMessage(bytes.NewReader(frame), Worker); !errors.Is(err, ErrMalformed) {
			t.Fatalf("err = %v, want ErrMalformed", err)
		}
	})
	t.Run("momentaRequest height 0", func(t *testing.T) {
		if _, err := Encode(MomentaRequest{Height: 0, Count: 1}); err == nil {
			t.Fatalf("Encode: want error")
		}
		frame := handEncodeMomentaRequest(t, 0, 1)
		if _, err := ReadMessage(bytes.NewReader(frame), Worker); !errors.Is(err, ErrMalformed) {
			t.Fatalf("err = %v, want ErrMalformed", err)
		}
	})
	t.Run("assignmentRequest height 0", func(t *testing.T) {
		if _, err := Encode(AssignmentRequest{Height: 0}); err == nil {
			t.Fatalf("Encode: want error")
		}
		w := &pwriter{}
		w.u64(0)
		w.raw(make([]byte, 32))
		w.u64(0)
		frame, err := assembleFrame(TypeAssignmentRequest, w.buf, MaxWorkerFrame)
		if err != nil {
			t.Fatalf("assembleFrame: %v", err)
		}
		if _, err := ReadMessage(bytes.NewReader(frame), Worker); !errors.Is(err, ErrMalformed) {
			t.Fatalf("err = %v, want ErrMalformed", err)
		}
	})
	t.Run("capRequest height 0", func(t *testing.T) {
		if _, err := Encode(CapRequest{Height: 0}); err == nil {
			t.Fatalf("Encode: want error")
		}
		w := &pwriter{}
		w.u64(0)
		w.raw(make([]byte, 32))
		w.u8(1)
		w.raw(make([]byte, 32))
		frame, err := assembleFrame(TypeCapRequest, w.buf, MaxWorkerFrame)
		if err != nil {
			t.Fatalf("assembleFrame: %v", err)
		}
		if _, err := ReadMessage(bytes.NewReader(frame), Worker); !errors.Is(err, ErrMalformed) {
			t.Fatalf("err = %v, want ErrMalformed", err)
		}
	})
	for _, n := range []int{0, codec.MaxAssetsPerBatch + 1} {
		t.Run(fmt.Sprintf("capRequest %d assets", n), func(t *testing.T) {
			if _, err := Encode(CapRequest{Height: 1, Assets: make([]codec.AssetID, n)}); err == nil {
				t.Fatalf("Encode: want error")
			}
			w := &pwriter{}
			w.u64(1)
			w.raw(make([]byte, 32))
			w.u8(uint8(n))
			w.raw(make([]byte, 32*n))
			frame, err := assembleFrame(TypeCapRequest, w.buf, MaxWorkerFrame)
			if err != nil {
				t.Fatalf("assembleFrame: %v", err)
			}
			if _, err := ReadMessage(bytes.NewReader(frame), Worker); !errors.Is(err, ErrMalformed) {
				t.Fatalf("err = %v, want ErrMalformed", err)
			}
		})
		t.Run(fmt.Sprintf("cap %d amounts", n), func(t *testing.T) {
			if _, err := Encode(Cap{Amounts: make([]codec.U256, n)}); err == nil {
				t.Fatalf("Encode: want error")
			}
			w := &pwriter{}
			w.u8(uint8(n))
			w.raw(make([]byte, 32*n))
			frame, err := assembleFrame(TypeCap, w.buf, MaxWorkerFrame)
			if err != nil {
				t.Fatalf("assembleFrame: %v", err)
			}
			if _, err := ReadMessage(bytes.NewReader(frame), Supervisor); !errors.Is(err, ErrMalformed) {
				t.Fatalf("err = %v, want ErrMalformed", err)
			}
		})
	}
	t.Run("assignment send height 0", func(t *testing.T) {
		if _, err := Encode(Assignment{Entries: []ledger.AssignmentEntry{{Ordinal: 1}}}); err == nil {
			t.Fatalf("Encode: want error")
		}
		frame := handEncodeAssignment(t, 1, 1)
		if _, err := ReadMessage(bytes.NewReader(frame), Supervisor); !errors.Is(err, ErrMalformed) {
			t.Fatalf("err = %v, want ErrMalformed", err)
		}
	})
	t.Run("assignment count 3 with two entries", func(t *testing.T) {
		frame := handEncodeAssignment(t, 3, 2)
		if _, err := ReadMessage(bytes.NewReader(frame), Supervisor); !errors.Is(err, ErrMalformed) {
			t.Fatalf("err = %v, want ErrMalformed", err)
		}
	})
	t.Run("anchor found 2", func(t *testing.T) {
		w := &pwriter{}
		w.u8(2)
		w.raw(make([]byte, 32))
		w.u64(0)
		frame, err := assembleFrame(TypeAnchor, w.buf, MaxFrame)
		if err != nil {
			t.Fatalf("assembleFrame: %v", err)
		}
		if _, err := ReadMessage(bytes.NewReader(frame), Supervisor); !errors.Is(err, ErrMalformed) {
			t.Fatalf("err = %v, want ErrMalformed", err)
		}
	})
	t.Run("anchor found 0 non-zero root", func(t *testing.T) {
		if _, err := Encode(Anchor{Found: false, PostStateRoot: types.Hash{0x01}}); err == nil {
			t.Fatalf("Encode: want error")
		}
		w := &pwriter{}
		w.u8(0)
		root := make([]byte, 32)
		root[0] = 0x01
		w.raw(root)
		w.u64(0)
		frame, err := assembleFrame(TypeAnchor, w.buf, MaxFrame)
		if err != nil {
			t.Fatalf("assembleFrame: %v", err)
		}
		if _, err := ReadMessage(bytes.NewReader(frame), Supervisor); !errors.Is(err, ErrMalformed) {
			t.Fatalf("err = %v, want ErrMalformed", err)
		}
	})
	t.Run("anchor found 0 non-zero seq", func(t *testing.T) {
		if _, err := Encode(Anchor{Found: false, LastInputSeq: 1}); err == nil {
			t.Fatalf("Encode: want error")
		}
		w := &pwriter{}
		w.u8(0)
		w.raw(make([]byte, 32))
		w.u64(1)
		frame, err := assembleFrame(TypeAnchor, w.buf, MaxFrame)
		if err != nil {
			t.Fatalf("assembleFrame: %v", err)
		}
		if _, err := ReadMessage(bytes.NewReader(frame), Supervisor); !errors.Is(err, ErrMalformed) {
			t.Fatalf("err = %v, want ErrMalformed", err)
		}
	})
	t.Run("sourceError kind 0", func(t *testing.T) {
		if _, err := Encode(SourceError{Kind: 0, Message: "x"}); err == nil {
			t.Fatalf("Encode: want error")
		}
		frame := handEncodeSourceError(t, SourceError{Kind: 0, Message: "x"})
		if _, err := ReadMessage(bytes.NewReader(frame), Supervisor); !errors.Is(err, ErrMalformed) {
			t.Fatalf("err = %v, want ErrMalformed", err)
		}
	})
	t.Run("sourceError kind 3", func(t *testing.T) {
		if _, err := Encode(SourceError{Kind: 3, Message: "x"}); err == nil {
			t.Fatalf("Encode: want error")
		}
		frame := handEncodeSourceError(t, SourceError{Kind: 3, Message: "x"})
		if _, err := ReadMessage(bytes.NewReader(frame), Supervisor); !errors.Is(err, ErrMalformed) {
			t.Fatalf("err = %v, want ErrMalformed", err)
		}
	})
}

func handEncodeMomentaRequest(t *testing.T, height, count uint64) []byte {
	t.Helper()
	w := &pwriter{}
	w.u64(height)
	w.u64(count)
	frame, err := assembleFrame(TypeMomentaRequest, w.buf, MaxWorkerFrame)
	if err != nil {
		t.Fatalf("assembleFrame: %v", err)
	}
	return frame
}

type failReader struct{}

func (failReader) Read([]byte) (int, error) { panic("should not be called") }

func TestReadMessageInvalidSide(t *testing.T) {
	for _, s := range []Side{0, 3} {
		if _, err := ReadMessage(failReader{}, s); err == nil {
			t.Fatalf("side %d: want error", s)
		}
	}
}

func TestEncodeMomentumOverMaxFrame(t *testing.T) {
	dm := ipctest.Chain([][]ipctest.Block{{{Address: addr(1), Data: make([]byte, MaxFrame)}}})[0]
	if _, err := Encode(Momentum{Detailed: dm}); err == nil {
		t.Fatalf("Encode: want error for an over-cap momentum")
	}
}

func TestDomainDir(t *testing.T) {
	c := WorkerConfig{StoragePath: "/s", DomainID: 7}
	if got, want := c.DomainDir(), "/s/7"; got != want {
		t.Fatalf("DomainDir = %q, want %q", got, want)
	}
}

// handEncodeAssignment frames an Assignment payload that claims n entries and carries entries
// entries, each with send momentum height 0 when n is 1 and 5 otherwise.
func handEncodeAssignment(t *testing.T, n uint32, entries int) []byte {
	t.Helper()
	w := &pwriter{}
	w.u32(n)
	for i := 0; i < entries; i++ {
		w.u64(uint64(i))
		w.u64(uint64(i))
		w.raw(make([]byte, 32))
		if n == 1 {
			w.u64(0)
		} else {
			w.u64(5)
		}
	}
	frame, err := assembleFrame(TypeAssignment, w.buf, MaxFrame)
	if err != nil {
		t.Fatalf("assembleFrame: %v", err)
	}
	return frame
}

func TestCursorRequestRejectsPayload(t *testing.T) {
	frame, err := assembleFrame(TypeCursorRequest, []byte{0x00}, MaxWorkerFrame)
	if err != nil {
		t.Fatalf("assembleFrame: %v", err)
	}
	if _, err := ReadMessage(bytes.NewReader(frame), Worker); !errors.Is(err, ErrMalformed) {
		t.Fatalf("non-empty CursorRequest: err = %v, want ErrMalformed", err)
	}
}

func TestConfigCarriesMaxPendingBatches(t *testing.T) {
	enc, err := Encode(WorkerConfig{DomainID: 1, NetworkName: "N", StoragePath: "S", MaxPendingBatches: 64})
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	m, err := ReadMessage(bytes.NewReader(enc), Supervisor)
	if err != nil {
		t.Fatalf("ReadMessage: %v", err)
	}
	if got := m.(WorkerConfig).MaxPendingBatches; got != 64 {
		t.Fatalf("MaxPendingBatches = %d, want 64", got)
	}
}

// acceptedEntry builds an entry with every wire-carried field set to a value derived from id and
// an asset flow summary of summaryLen bytes.
func acceptedEntry(id uint64, summaryLen int) *definition.BatchEntry {
	e := &definition.BatchEntry{
		ProtocolVersion: uint16(10 + id),
		FirstInputSeq:   id * 10,
		LastInputSeq:    id*10 + 4,
		PreStateRoot:    types.Hash{0x11, byte(id)},
		PostStateRoot:   types.Hash{0x12, byte(id)},
		InputRoot:       types.Hash{0x13, byte(id)},
		ReceiptRoot:     types.Hash{0x14, byte(id)},
		EventRoot:       types.Hash{0x15, byte(id)},
		OutboxRoot:      types.Hash{0x16, byte(id)},
		DaHash:          types.Hash{0x17, byte(id)},
		DaMode:          1,
		ProofData:       []byte{},
	}
	e.BatchId = id
	if summaryLen > 0 {
		e.AssetFlowSummary = bytes.Repeat([]byte{byte(id)}, summaryLen)
	}
	return e
}

func readAs(t *testing.T, m Message, side Side) Message {
	t.Helper()
	enc, err := Encode(m)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	got, err := ReadMessage(bytes.NewReader(enc), side)
	if err != nil {
		t.Fatalf("ReadMessage: %v", err)
	}
	return got
}

func TestAcceptedRequestRoundTrip(t *testing.T) {
	got := readAs(t, AcceptedRequest{FromBatchID: 1<<40 + 3}, Worker)
	if got != (AcceptedRequest{FromBatchID: 1<<40 + 3}) {
		t.Fatalf("got %+v, want FromBatchID %d", got, uint64(1<<40+3))
	}
}

func TestAcceptedRoundTripCarriesEveryField(t *testing.T) {
	in := Accepted{NextBatchID: 9, Entries: []*definition.BatchEntry{acceptedEntry(6, 0), acceptedEntry(7, 100)}}
	in.Entries[1].ProofData = []byte{1, 2, 3}
	got := readAs(t, in, Supervisor).(Accepted)

	if got.NextBatchID != 9 || len(got.Entries) != 2 {
		t.Fatalf("got NextBatchID %d with %d entries, want 9 with 2", got.NextBatchID, len(got.Entries))
	}
	for i, e := range got.Entries {
		w := in.Entries[i]
		if e.BatchId != w.BatchId || e.ProtocolVersion != w.ProtocolVersion || e.FirstInputSeq != w.FirstInputSeq ||
			e.LastInputSeq != w.LastInputSeq || e.DaMode != w.DaMode {
			t.Fatalf("entry %d scalars = %+v, want %+v", i, e, w)
		}
		for name, pair := range map[string][2]types.Hash{
			"PreStateRoot": {e.PreStateRoot, w.PreStateRoot}, "PostStateRoot": {e.PostStateRoot, w.PostStateRoot},
			"InputRoot": {e.InputRoot, w.InputRoot}, "ReceiptRoot": {e.ReceiptRoot, w.ReceiptRoot},
			"EventRoot": {e.EventRoot, w.EventRoot}, "OutboxRoot": {e.OutboxRoot, w.OutboxRoot}, "DaHash": {e.DaHash, w.DaHash},
		} {
			if pair[0] != pair[1] {
				t.Fatalf("entry %d %s = %x, want %x", i, name, pair[0], pair[1])
			}
		}
		if !bytes.Equal(e.AssetFlowSummary, w.AssetFlowSummary) || !bytes.Equal(e.ProofData, w.ProofData) {
			t.Fatalf("entry %d byte fields differ: summary %x proof %x", i, e.AssetFlowSummary, e.ProofData)
		}
		if e.DomainId != 0 || e.ExecutorPillar != "" || e.SubmittedAtHeight != 0 || e.FinalizeAtHeight != 0 || e.State != 0 {
			t.Fatalf("entry %d carries a field outside the wire format: %+v", i, e)
		}
	}
	if len(got.Entries[1].AssetFlowSummary) != 100 {
		t.Fatalf("AssetFlowSummary length = %d, want 100", len(got.Entries[1].AssetFlowSummary))
	}
}

func TestAcceptedDecodeCopiesByteFields(t *testing.T) {
	enc, err := Encode(Accepted{NextBatchID: 9, Entries: []*definition.BatchEntry{acceptedEntry(1, 8)}})
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	got, err := ReadMessage(bytes.NewReader(enc), Supervisor)
	if err != nil {
		t.Fatalf("ReadMessage: %v", err)
	}
	e := got.(Accepted).Entries[0]
	want := append([]byte(nil), e.AssetFlowSummary...)
	for i := range enc {
		enc[i] = 0xEE
	}
	if !bytes.Equal(e.AssetFlowSummary, want) {
		t.Fatalf("AssetFlowSummary aliases the frame buffer")
	}
}

func TestAcceptedEntryLimits(t *testing.T) {
	// The limit is 64 entries, one Settlement batch's input cap.
	if MaxAcceptedEntries != 64 {
		t.Fatalf("MaxAcceptedEntries = %d, want 64", MaxAcceptedEntries)
	}
	entries := func(n int) []*definition.BatchEntry {
		out := make([]*definition.BatchEntry, n)
		for i := range out {
			out[i] = acceptedEntry(uint64(i), 0)
		}
		return out
	}

	if _, err := Encode(Accepted{NextBatchID: 100, Entries: entries(64)}); err != nil {
		t.Fatalf("Encode of 64 entries: %v", err)
	}
	if _, err := Encode(Accepted{NextBatchID: 100, Entries: entries(65)}); err == nil {
		t.Fatalf("Encode of 65 entries succeeded")
	}
	if _, err := Encode(Accepted{NextBatchID: 100, Entries: []*definition.BatchEntry{nil}}); err == nil {
		t.Fatalf("Encode of a nil entry succeeded")
	}

	// A frame that claims 65 well-formed entries is malformed because of the count alone.
	one, err := Encode(Accepted{NextBatchID: 100, Entries: entries(1)})
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	entry := one[5+8+4:]
	w := &pwriter{}
	w.u64(100)
	w.u32(65)
	for i := 0; i < 65; i++ {
		w.raw(entry)
	}
	frame, err := assembleFrame(TypeAccepted, w.buf, MaxFrame)
	if err != nil {
		t.Fatalf("assembleFrame: %v", err)
	}
	if _, err := ReadMessage(bytes.NewReader(frame), Supervisor); !errors.Is(err, ErrMalformed) {
		t.Fatalf("count 65: err = %v, want ErrMalformed", err)
	}

	w = &pwriter{}
	w.u64(100)
	w.u32(64)
	for i := 0; i < 64; i++ {
		w.raw(entry)
	}
	frame, err = assembleFrame(TypeAccepted, w.buf, MaxFrame)
	if err != nil {
		t.Fatalf("assembleFrame: %v", err)
	}
	if _, err := ReadMessage(bytes.NewReader(frame), Supervisor); err != nil {
		t.Fatalf("count 64: %v", err)
	}
}

func TestAcceptedRequestRejectsWrongLength(t *testing.T) {
	for _, n := range []int{7, 9} {
		frame, err := assembleFrame(TypeAcceptedRequest, make([]byte, n), MaxWorkerFrame)
		if err != nil {
			t.Fatalf("assembleFrame: %v", err)
		}
		if _, err := ReadMessage(bytes.NewReader(frame), Worker); !errors.Is(err, ErrMalformed) {
			t.Fatalf("%d-byte AcceptedRequest: err = %v, want ErrMalformed", n, err)
		}
	}
}

func TestAcceptedIsReadOnlyFromItsSide(t *testing.T) {
	for _, c := range []struct {
		m    Message
		from Side
		bad  Side
	}{
		{Accepted{NextBatchID: 1}, Supervisor, Worker},
		{AcceptedRequest{FromBatchID: 1}, Worker, Supervisor},
	} {
		enc, err := Encode(c.m)
		if err != nil {
			t.Fatalf("Encode: %v", err)
		}
		if _, err := ReadMessage(bytes.NewReader(enc), c.from); err != nil {
			t.Fatalf("%T from its own side: %v", c.m, err)
		}
		if _, err := ReadMessage(bytes.NewReader(enc), c.bad); !errors.Is(err, ErrMalformed) {
			t.Fatalf("%T from the other side: err = %v, want ErrMalformed", c.m, err)
		}
	}
}

// handEncodeWithdrawal builds a Withdrawal frame bypassing Encode's
// checks.
func handEncodeWithdrawal(t *testing.T, id uint64, msg, proof []byte) []byte {
	t.Helper()
	w := &pwriter{}
	w.u64(id)
	w.raw(msg)
	w.raw(proof)
	frame, err := assembleFrame(TypeWithdrawal, w.buf, MaxWorkerFrame)
	if err != nil {
		t.Fatalf("assembleFrame: %v", err)
	}
	return frame
}

func TestWithdrawalRejects(t *testing.T) {
	msg, proof := withdrawalFixture()

	kind0, err := codec.OutboxMessage{
		SourceContract: types.Hash{0x04},
		Target:         codec.L2Delivery{TargetContract: types.Hash{0x09}},
		Payload:        make([]byte, 52),
	}.Encode()
	if err != nil {
		t.Fatalf("encode kind 0 message: %v", err)
	}
	if len(kind0) != withdrawalMessageLen {
		t.Fatalf("kind 0 fixture is %d bytes, want %d", len(kind0), withdrawalMessageLen)
	}
	undecodable := append([]byte{}, msg...)
	undecodable[44] = 7 // the kind byte

	cases := []struct {
		name  string
		msg   []byte
		proof []byte
	}{
		{"kind 0 message", kind0, proofOf(0, 0)},
		{"undecodable message", undecodable, proofOf(0, 0)},
		{"proof count 33", msg, proofOf(33, 0)},
		{"proof length", msg, append(append([]byte{}, proof...), 0)},
		{"proof side 2", msg, proofOf(1, 2)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := Encode(Withdrawal{BatchID: 1, Message: tc.msg, Proof: tc.proof}); err == nil {
				t.Fatal("Encode accepted it")
			}
			frame := handEncodeWithdrawal(t, 1, tc.msg, tc.proof)
			if _, err := ReadMessage(bytes.NewReader(frame), Worker); !errors.Is(err, ErrMalformed) {
				t.Fatalf("ReadMessage: err = %v, want ErrMalformed", err)
			}
		})
	}
}

func TestWithdrawalProofBounds(t *testing.T) {
	msg, _ := withdrawalFixture()
	for _, count := range []uint32{0, 32} {
		frame, err := Encode(Withdrawal{BatchID: 1, Message: msg, Proof: proofOf(count, 1)})
		if err != nil {
			t.Fatalf("count %d: Encode: %v", count, err)
		}
		if count == 32 {
			if got := len(frame) - 4; got != 1202 {
				t.Fatalf("count 32: length field = %d, want 1202", got)
			}
			if len(frame) != 1206 {
				t.Fatalf("count 32: frame = %d bytes, want 1206", len(frame))
			}
		}
		if _, err := ReadMessage(bytes.NewReader(frame), Worker); err != nil {
			t.Fatalf("count %d: ReadMessage: %v", count, err)
		}
	}
}
