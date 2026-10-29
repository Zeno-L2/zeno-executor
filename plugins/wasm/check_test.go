package wasm

import (
	"errors"
	"testing"

	"github.com/zenon-network/go-zenon/common/crypto"
	"github.com/zenon-network/go-zenon/common/types"

	"github.com/Zeno-L2/zeno-executor/domain"
	"github.com/Zeno-L2/zeno-executor/internal/codec"
	"github.com/Zeno-L2/zeno-executor/plugins/wasm/internal/module"
)

// finalizeEffects drives a full deploy of raw with metadata through the
// plugin and returns the finalize effects.
func finalizeEffects(t *testing.T, raw, metadata []byte) domain.Effects {
	t.Helper()
	p := newTestPlugin(t)
	sender := types.Address{1}
	codeHash := seedDeployment(t, p, raw, sender, metadata)
	eff, err := p.Apply(nil, Input{Ctx: domain.InputContext{L1Sender: sender}, Args: codec.DeployFinalizeArgs{CodeHash: codeHash}})
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}
	return eff
}

func TestCheckModuleMatchesFinalize(t *testing.T) {
	valid := buildModule(nil, []byte{0x00}, []byte{0x42, 0x00, 0x0B}, 1)
	twoPages := buildModule(nil, []byte{0x00}, []byte{0x42, 0x00, 0x0B}, 2)
	custom := append(append([]byte{}, valid...), section(0, appendName(nil, "x"))...)
	illTyped := buildModule(nil, []byte{0x00}, []byte{0x41, 0x00, 0x0B}, 1)

	cases := []struct {
		name      string
		raw       []byte
		wantPages uint32
		wantErr   error
	}{
		{"Valid", valid, 1, nil},
		{"TwoPages", twoPages, 2, nil},
		{"CustomSection", custom, 0, module.ErrCustomSection},
		{"EngineRefused", illTyped, 0, ErrEngineCompile},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			eff := finalizeEffects(t, tc.raw, immutableMetadata)
			chk, err := CheckModule(tc.raw, immutableMetadata)
			if (err == nil) != (eff.Status == codec.StatusSuccess) {
				t.Fatalf("CheckModule err = %v, finalize status = %v", err, eff.Status)
			}
			if tc.wantErr != nil {
				if !errors.Is(err, tc.wantErr) {
					t.Fatalf("CheckModule err = %v, want %v", err, tc.wantErr)
				}
				if eff.Status != codec.StatusValidationFailed {
					t.Fatalf("finalize status = %v, want StatusValidationFailed", eff.Status)
				}
				return
			}
			if err != nil {
				t.Fatalf("CheckModule: %v", err)
			}
			if string(chk.CodeHash[:]) != string(eff.ReturnData[0:32]) {
				t.Fatalf("CodeHash = %x, want %x", chk.CodeHash, eff.ReturnData[0:32])
			}
			if chk.InstrumentedCodeHash != eff.CodeHash {
				t.Fatalf("InstrumentedCodeHash = %x, want %x", chk.InstrumentedCodeHash, eff.CodeHash)
			}
			if chk.InitialPages != tc.wantPages {
				t.Fatalf("InitialPages = %d, want %d", chk.InitialPages, tc.wantPages)
			}
			if chk.Size != len(tc.raw) || chk.InstrumentedSize <= 0 {
				t.Fatalf("Size = %d, InstrumentedSize = %d", chk.Size, chk.InstrumentedSize)
			}
		})
	}
}

func TestCheckModuleMetadataHash(t *testing.T) {
	raw := buildModule(nil, []byte{0x00}, []byte{0x42, 0x00, 0x0B}, 1)

	chk, err := CheckModule(raw, immutableMetadata)
	if err != nil {
		t.Fatalf("CheckModule: %v", err)
	}
	var want types.Hash
	copy(want[:], crypto.Hash(immutableMetadata))
	if chk.MetadataHash != want {
		t.Fatalf("MetadataHash = %x, want %x", chk.MetadataHash, want)
	}

	chk, err = CheckModule(raw, nil)
	if err != nil {
		t.Fatalf("CheckModule nil metadata: %v", err)
	}
	if chk.MetadataHash != (types.Hash{}) {
		t.Fatalf("MetadataHash = %x, want zero", chk.MetadataHash)
	}
}

func TestCheckModuleRejectsZeroOwnerLikeFinalize(t *testing.T) {
	raw := buildModule(nil, []byte{0x00}, []byte{0x42, 0x00, 0x0B}, 1)
	bad := codec.ContractMetadata{Version: codec.ContractMetadataVersion, UpgradePolicy: codec.PolicyOwnerUpgradeable}.Encode()

	if _, err := CheckModule(raw, bad); !errors.Is(err, codec.ErrInvalidOwner) {
		t.Fatalf("CheckModule err = %v, want codec.ErrInvalidOwner", err)
	}
	if eff := finalizeEffects(t, raw, bad); eff.Status != codec.StatusValidationFailed {
		t.Fatalf("finalize status = %v, want StatusValidationFailed", eff.Status)
	}
}
