package ipc

import (
	"bytes"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/zenon-network/go-zenon/common/types"

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

func validTable() []msgCase {
	return []msgCase{
		{"config", Supervisor, WorkerConfig{
			DomainID:          7,
			ExecutorAddress:   types.Address{0x42},
			STFSpecHash:       types.Hash{0xAA},
			ConfirmationDepth: 2,
			Role:              "EXECUTOR",
			NetworkName:       "test",
			StoragePath:       "/data",
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
		{"assignmentRequest", Worker, AssignmentRequest{Height: 9, Hash: types.Hash{0x0A}, NextOrdinal: 3}},
		{"assignment", Supervisor, Assignment{Entries: []ledger.AssignmentEntry{
			{Ordinal: 3, GlobalInputIndex: 10, SourceAccountBlockHash: types.Hash{0x01}, SendMomentumHeight: 7},
			{Ordinal: 4, GlobalInputIndex: 12, SourceAccountBlockHash: types.Hash{0x02}, SendMomentumHeight: 8},
		}}},
		{"assignmentEmpty", Supervisor, Assignment{}},
		{"capRequest", Worker, CapRequest{Height: 9, Hash: types.Hash{1}, Assets: []codec.AssetID{codec.AssetIDFromZTS(types.ZnnTokenStandard), codec.AssetIDFromZTS(types.QsrTokenStandard)}}},
		{"cap", Supervisor, Cap{Amounts: []codec.U256{{31: 7}, {31: 8}}}},
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
		Role:              "R",
		NetworkName:       "N",
		StoragePath:       "S",
	}
	enc, err := Encode(cfg)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	payload := enc[5:]
	// Layout: u32 domainId(4) + 20B addr + 32B hash + u64 depth(8) = 64 bytes fixed prefix,
	// then str16 role, str16 networkName, str16 storagePath.
	const fixed = 4 + 20 + 32 + 8
	roleLenOff := fixed
	networkLenOff := fixed + 2 + len(cfg.Role)
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
	overrun("role", roleLenOff)
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
		Role:              "EXECUTOR",
		NetworkName:       "test",
		StoragePath:       "/data",
	}

	longRole := base
	longRole.Role = strings.Repeat("r", maxRoleLen+1)
	longNetwork := base
	longNetwork.NetworkName = strings.Repeat("n", maxNetworkNameLen+1)
	longStorage := base
	longStorage.StoragePath = strings.Repeat("s", maxStoragePathLen+1)

	for name, cfg := range map[string]WorkerConfig{"role": longRole, "networkName": longNetwork, "storagePath": longStorage} {
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
	w.str16(c.Role)
	w.str16(c.NetworkName)
	w.str16(c.StoragePath)
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

func TestUnknownTypes(t *testing.T) {
	known := map[byte]bool{
		0x01: true, 0x02: true, 0x03: true, 0x04: true, 0x05: true, 0x06: true, 0x07: true, 0x08: true,
		0x81: true, 0x82: true, 0x83: true, 0x84: true, 0x85: true, 0x86: true,
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
