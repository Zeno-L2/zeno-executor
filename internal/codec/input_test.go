package codec

import (
	"bytes"
	"errors"
	"testing"

	"github.com/zenon-network/go-zenon/common/crypto"
	"github.com/zenon-network/go-zenon/common/types"
)

func sampleInputs() []Input {
	return []Input{
		{
			SourceAccountBlockHash: fillHash(0x01),
			Args: CallL2Args{
				TargetContract: fillHash(0x02),
				PlasmaLimit:    1000,
				DepositAsset:   AssetIDFromZTS(types.ZnnTokenStandard),
				DepositAmount:  U256{31: 5},
				Payload:        []byte("call"),
			},
		},
		{
			SourceAccountBlockHash: fillHash(0x03),
			Args: DepositArgs{
				TargetContract: fillHash(0x04),
				PlasmaLimit:    2000,
				DepositAsset:   AssetIDFromZTS(types.QsrTokenStandard),
				DepositAmount:  U256{31: 6},
			},
		},
		{
			SourceAccountBlockHash: fillHash(0x05),
			Args: DeployStartArgs{
				CodeHash:     fillHash(0x06),
				MetadataHash: fillHash(0x07),
				TotalSize:    4096,
				ChunkCount:   3,
			},
		},
		{
			SourceAccountBlockHash: fillHash(0x08),
			Args: DeployChunkArgs{
				CodeHash:   fillHash(0x09),
				ChunkIndex: 1,
				ChunkHash:  fillHash(0x0A),
			},
		},
		{
			SourceAccountBlockHash: fillHash(0x0B),
			Args: DeployFinalizeArgs{
				CodeHash: fillHash(0x0C),
			},
		},
		{
			SourceAccountBlockHash: fillHash(0x0D),
			Args: UpgradeArgs{
				ContractID:  fillHash(0x0E),
				NewCodeHash: fillHash(0x0F),
			},
		},
		{
			SourceAccountBlockHash: fillHash(0x10),
			Args: TransferOwnershipArgs{
				ContractID: fillHash(0x11),
				NewOwner:   fillAddress(0x12),
			},
		},
	}
}

func fillAddress(b byte) types.Address {
	var out types.Address
	for i := range out {
		out[i] = b
	}
	return out
}

func TestInputRoundTripAllKinds(t *testing.T) {
	for _, in := range sampleInputs() {
		in := in
		t.Run(argsTypeName(in.Args), func(t *testing.T) {
			b, err := in.Encode()
			if err != nil {
				t.Fatalf("Encode: %v", err)
			}
			got, err := DecodeInput(b)
			if err != nil {
				t.Fatalf("Decode: %v", err)
			}
			gotB, err := got.Encode()
			if err != nil {
				t.Fatalf("re-encode: %v", err)
			}
			if !bytes.Equal(gotB, b) {
				t.Errorf("round trip mismatch: got %x, want %x", gotB, b)
			}
		})
	}
}

func argsTypeName(a InputArgs) string {
	switch a.(type) {
	case CallL2Args:
		return "CallL2"
	case DepositArgs:
		return "Deposit"
	case DeployStartArgs:
		return "DeployStart"
	case DeployChunkArgs:
		return "DeployChunk"
	case DeployFinalizeArgs:
		return "DeployFinalize"
	case UpgradeArgs:
		return "Upgrade"
	case TransferOwnershipArgs:
		return "TransferOwnership"
	default:
		return "unknown"
	}
}

func TestInputHashMatchesIndependentSHA3(t *testing.T) {
	in := sampleInputs()[0]
	encoded, err := in.Encode()
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	got, err := in.Hash()
	if err != nil {
		t.Fatalf("Hash: %v", err)
	}
	want := hashOf(crypto.Hash(encoded))
	if got != want {
		t.Errorf("got %x, want %x", got, want)
	}
}

func TestInputUnknownKind(t *testing.T) {
	b := []byte{0x07} // kind 7 is out of the 0..6 range
	b = append(b, make([]byte, 32)...)
	if _, err := DecodeInput(b); !errors.Is(err, ErrInvalidKind) {
		t.Fatalf("got %v, want ErrInvalidKind", err)
	}
}

func TestInputTruncatedTail(t *testing.T) {
	// DeployFinalizeArgs' tail is a single fixed-width hash, so truncating its last byte
	// hits the fixed-width check (ErrTruncated) rather than a length-prefix overflow check.
	in := sampleInputs()[4]
	b, err := in.Encode()
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	truncated := b[:len(b)-1]
	if _, err := DecodeInput(truncated); !errors.Is(err, ErrTruncated) {
		t.Fatalf("got %v, want ErrTruncated", err)
	}
}

func TestInputTrailingBytes(t *testing.T) {
	in := sampleInputs()[0]
	b, err := in.Encode()
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	b = append(b, 0x00)
	if _, err := DecodeInput(b); !errors.Is(err, ErrTrailingBytes) {
		t.Fatalf("got %v, want ErrTrailingBytes", err)
	}
}

func TestInputCallL2PayloadCap(t *testing.T) {
	in := sampleInputs()[0]
	args := in.Args.(CallL2Args)
	args.Payload = make([]byte, MaxCallPayload+1)
	in.Args = args
	if _, err := in.Encode(); !errors.Is(err, ErrLimitExceeded) {
		t.Fatalf("got %v, want ErrLimitExceeded", err)
	}
}

func TestInputNonCanonicalAssetID(t *testing.T) {
	t.Run("CallL2", func(t *testing.T) {
		in := sampleInputs()[0]
		args := in.Args.(CallL2Args)
		args.DepositAsset[0] = 0x01
		in.Args = args
		if _, err := in.Encode(); !errors.Is(err, ErrInvalidAssetID) {
			t.Fatalf("got %v, want ErrInvalidAssetID", err)
		}
	})

	t.Run("Deposit", func(t *testing.T) {
		in := sampleInputs()[1]
		args := in.Args.(DepositArgs)
		args.DepositAsset[0] = 0x01
		in.Args = args
		if _, err := in.Encode(); !errors.Is(err, ErrInvalidAssetID) {
			t.Fatalf("got %v, want ErrInvalidAssetID", err)
		}
	})
}

func TestInputRootMixedKinds(t *testing.T) {
	inputs := sampleInputs()
	got, err := InputRoot(inputs)
	if err != nil {
		t.Fatalf("InputRoot: %v", err)
	}

	elements := make([][]byte, len(inputs))
	for i, in := range inputs {
		b, err := in.Encode()
		if err != nil {
			t.Fatalf("Encode: %v", err)
		}
		elements[i] = b
	}
	want := refMerkleRoot(elements)
	if got != want {
		t.Errorf("got %x, want %x", got, want)
	}
}

// refMerkleRoot recomputes the §27.3 Merkle construction directly from sha3 calls, never via
// codec.MerkleRoot, so the test does not check the library against itself.
func refMerkleRoot(elements [][]byte) types.Hash {
	if len(elements) == 0 {
		return types.Hash{}
	}
	level := make([]types.Hash, len(elements))
	for i, e := range elements {
		level[i] = refLeafHash(e)
	}
	for len(level) > 1 {
		next := make([]types.Hash, 0, (len(level)+1)/2)
		i := 0
		for ; i+1 < len(level); i += 2 {
			next = append(next, refNodeHash(level[i], level[i+1]))
		}
		if i < len(level) {
			next = append(next, level[i])
		}
		level = next
	}
	return level[0]
}
