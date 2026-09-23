package codec

import (
	"bytes"
	"errors"
	"testing"

	"github.com/zenon-network/go-zenon/common/types"
)

func TestContractMetadataRoundTrip(t *testing.T) {
	m := ContractMetadata{
		Version:       ContractMetadataVersion,
		UpgradePolicy: PolicyOwnerUpgradeable,
		Owner:         fillAddress(0x01),
	}
	encoded := m.Encode()
	got, err := DecodeContractMetadata(encoded)
	if err != nil {
		t.Fatalf("DecodeContractMetadata: %v", err)
	}
	if got != m {
		t.Errorf("got %+v, want %+v", got, m)
	}
}

func TestContractMetadataDecodeErrors(t *testing.T) {
	valid := ContractMetadata{Version: ContractMetadataVersion, UpgradePolicy: PolicyOwnerUpgradeable, Owner: fillAddress(0x01)}.Encode()

	t.Run("unknown version", func(t *testing.T) {
		b := append([]byte{}, valid...)
		b[1] = 0x02 // version = 2
		if _, err := DecodeContractMetadata(b); !errors.Is(err, ErrUnknownVersion) {
			t.Fatalf("got %v, want ErrUnknownVersion", err)
		}
	})

	t.Run("truncated", func(t *testing.T) {
		b := valid[:len(valid)-1]
		if _, err := DecodeContractMetadata(b); !errors.Is(err, ErrTruncated) {
			t.Fatalf("got %v, want ErrTruncated", err)
		}
	})

	t.Run("trailing bytes", func(t *testing.T) {
		b := append(append([]byte{}, valid...), 0x00)
		if _, err := DecodeContractMetadata(b); !errors.Is(err, ErrTrailingBytes) {
			t.Fatalf("got %v, want ErrTrailingBytes", err)
		}
	})

	t.Run("invalid policy", func(t *testing.T) {
		b := ContractMetadata{Version: ContractMetadataVersion, UpgradePolicy: 2, Owner: fillAddress(0x01)}.Encode()
		if _, err := DecodeContractMetadata(b); !errors.Is(err, ErrInvalidPolicy) {
			t.Fatalf("got %v, want ErrInvalidPolicy", err)
		}
	})

	t.Run("immutable with non-zero owner", func(t *testing.T) {
		b := ContractMetadata{Version: ContractMetadataVersion, UpgradePolicy: PolicyImmutable, Owner: fillAddress(0x01)}.Encode()
		if _, err := DecodeContractMetadata(b); !errors.Is(err, ErrInvalidOwner) {
			t.Fatalf("got %v, want ErrInvalidOwner", err)
		}
	})

	t.Run("owner-upgradeable with zero owner", func(t *testing.T) {
		b := ContractMetadata{Version: ContractMetadataVersion, UpgradePolicy: PolicyOwnerUpgradeable, Owner: types.Address{}}.Encode()
		if _, err := DecodeContractMetadata(b); !errors.Is(err, ErrInvalidOwner) {
			t.Fatalf("got %v, want ErrInvalidOwner", err)
		}
	})
}

func TestContractMetadataEncodeNoValidation(t *testing.T) {
	// Encode writes exactly version || policy || owner with no checks, even for a shape that
	// would fail DecodeContractMetadata's validation.
	m := ContractMetadata{Version: ContractMetadataVersion, UpgradePolicy: 2, Owner: fillAddress(0x01)}
	got := m.Encode()
	owner := fillAddress(0x01)
	want := []byte{0x00, 0x01, 0x02}
	want = append(want, owner[:]...)
	if !bytes.Equal(got, want) {
		t.Errorf("got %x, want %x", got, want)
	}
}
