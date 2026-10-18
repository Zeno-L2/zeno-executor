package codec

import (
	"fmt"

	"github.com/zenon-network/go-zenon/common/types"
)

// Contract metadata upgrade policy ordinals.
const (
	PolicyImmutable        uint8 = 0
	PolicyOwnerUpgradeable uint8 = 1
)

// ContractMetadataVersion is the version field ContractMetadata.Encode writes and
// DecodeContractMetadata requires.
const ContractMetadataVersion uint16 = 1

// ContractMetadata is the metadata a deploy carries and a finalize
// validates.
type ContractMetadata struct {
	Version       uint16
	UpgradePolicy uint8
	Owner         types.Address
}

// DecodeContractMetadata decodes a canonical ContractMetadata encoding
// and validates it: an unknown version is rejected before any further
// field is read, since a later version may append fields the current
// one does not know how to skip; the policy/owner consistency rule is
// checked last.
func DecodeContractMetadata(b []byte) (ContractMetadata, error) {
	m, err := decodeContractMetadataBody(newReader(b))
	if err != nil {
		return ContractMetadata{}, fmt.Errorf("codec: decode contract metadata: %w", err)
	}
	return m, nil
}

func decodeContractMetadataBody(r *reader) (ContractMetadata, error) {
	var m ContractMetadata

	version, err := r.u16()
	if err != nil {
		return ContractMetadata{}, err
	}
	if version != ContractMetadataVersion {
		return ContractMetadata{}, ErrUnknownVersion
	}
	m.Version = version

	policy, err := r.u8()
	if err != nil {
		return ContractMetadata{}, err
	}
	owner, err := r.address()
	if err != nil {
		return ContractMetadata{}, err
	}
	if err := r.end(); err != nil {
		return ContractMetadata{}, err
	}

	if policy != PolicyImmutable && policy != PolicyOwnerUpgradeable {
		return ContractMetadata{}, ErrInvalidPolicy
	}
	if (policy == PolicyImmutable) != (owner == (types.Address{})) {
		return ContractMetadata{}, ErrInvalidOwner
	}

	m.UpgradePolicy = policy
	m.Owner = owner
	return m, nil
}

// Encode writes version:u16 || upgrade_policy:u8 || owner:20 with no validation. Tests,
// vector construction and zeno-exec call this; the runtime never encodes metadata.
func (m ContractMetadata) Encode() []byte {
	w := &writer{}
	w.u16(m.Version)
	w.u8(m.UpgradePolicy)
	w.address(m.Owner)
	return w.buf
}
