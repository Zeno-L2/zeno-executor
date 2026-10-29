//go:build devnet

// Package devnet holds integration tests that run the executor against a live go-zenon devnet.
// scripts/devnet-test.sh runs them.
package devnet

import (
	"bytes"
	"crypto/ed25519"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"math/big"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/zenon-network/go-zenon/chain/nom"
	"github.com/zenon-network/go-zenon/common/types"
	"github.com/zenon-network/go-zenon/dp"
	"github.com/zenon-network/go-zenon/vm/constants"
	"github.com/zenon-network/go-zenon/vm/embedded/definition"
	"github.com/zenon-network/go-zenon/wallet"

	"github.com/Zeno-L2/zeno-executor/internal/codec"
	"github.com/Zeno-L2/zeno-executor/internal/recovery"
	"github.com/Zeno-L2/zeno-executor/internal/settlement"
	"github.com/Zeno-L2/zeno-executor/internal/supervisor"
	"github.com/Zeno-L2/zeno-executor/internal/znnd"
)

const (
	endpoint           = "http://127.0.0.1:35997" // the devnet's public RPC node
	chainIdentifier    = 69
	confirmationDepth  = 3
	domainID           = uint32(1)
	pillarName         = "dev1"
	libp2pHeight       = uint64(10) // go-zenon's docker/devnet/genesis.json enforces offchain-execution at height 0 and switches to libp2p here; a block published before the switch can be lost
	momentum           = 10 * time.Second
	poll               = 500 * time.Millisecond
	depositAmount      = 100  // ZNN base units per deposit
	maxBatchWithdrawal = 1000 // ZNN base units
	inputGas           = uint64(5000)
	withdrawalDelay    = uint64(30) // go-zenon's devnet WithdrawalDelayMin (vm/constants/embedded_devnet.go); domain 1 registers with it

	// Seeds of dev address indexes 1 and 3 (paths m/44'/73404'/1' and /3' of the devnet mnemonic
	// in go-zenon's docker/devnet/README.md). Both are public and for the devnet only.
	adminSeed    = "de28dac8ecd697dfbc195076f6d4d7a4ec49adf8936f6444b44d446c2d725d81"
	executorSeed = "bd2b1f4b42334f923064f466ed5590379498585b2dd4e42152db0e5ddbe2023e"
)

var (
	adminAddress    = types.ParseAddressPanic("z1qq6eg8n43g032hanpsfp02qcdmv7zfj3y2lt5d")
	executorAddress = types.ParseAddressPanic("z1qp3yph55qgresyytz83anynr2f4z39x2z3ej3e")
	stfSpecHash     = types.HexToHashPanic("0000000000000000000000000000000000000000000000000000000000000001")
	depositTarget   = types.Hash{0x07}                                              // no contract lives there, so the domain refunds every deposit
	callTarget      = types.Hash{0x08}                                              // a hash with no contract, the target of the CallL2
	fuseAmount      = new(big.Int).Mul(big.NewInt(10_000), big.NewInt(100_000_000)) // 10,000 QSR
)

// devnet drives the live devnet from the test goroutine only.
type devnet struct {
	t     *testing.T
	c     *znnd.Client
	r     *settlement.Reader
	admin *wallet.KeyPair
	dir   string         // the artifact directory; removed when the test passes
	exec  *executor      // the running executor, checked on every poll; nil when none runs
	gws   []*gatewayProc // the running gateways, checked on every poll
}

// newDevnet checks the environment and returns a driver for the devnet at endpoint.
func newDevnet(t *testing.T) *devnet {
	t.Helper()
	if runtime.GOOS == "linux" && os.Geteuid() == 0 {
		t.Fatal("run the devnet test as a non-root user: as root on Linux the supervisor separates worker uids, which cannot reach the test's 0700 directories")
	}
	admin := keyPair(t, adminSeed, adminAddress)
	keyPair(t, executorSeed, executorAddress)
	if types.NetworkAdmin != adminAddress {
		t.Fatalf("types.NetworkAdmin is %s, not the devnet admin %s: the go-zenon checkout lacks the devnet build (common/types/address_devnet.go)", types.NetworkAdmin, adminAddress)
	}
	if constants.WithdrawalDelayMin != withdrawalDelay {
		t.Fatalf("constants.WithdrawalDelayMin is %d, not the devnet's %d: the go-zenon checkout lacks the devnet withdrawal delay (vm/constants/embedded_devnet.go)", constants.WithdrawalDelayMin, withdrawalDelay)
	}

	c := znnd.NewClient(endpoint)
	d := &devnet{t: t, c: c, r: settlement.NewReader(c), admin: admin}
	var m *nom.Momentum
	deadline := time.Now().Add(2 * time.Minute)
	for {
		var err error
		if m, err = c.FrontierMomentum(); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("no znnd node answers at %s; scripts/devnet-test.sh starts the devnet", endpoint)
		}
		time.Sleep(poll)
	}
	if m.ChainIdentifier != chainIdentifier {
		t.Fatalf("the node at %s has chain identifier %d, not the devnet's %d", endpoint, m.ChainIdentifier, chainIdentifier)
	}

	dir, err := os.MkdirTemp("", "zeno-executor-devnet-")
	if err != nil {
		t.Fatalf("MkdirTemp: %v", err)
	}
	d.dir = dir
	t.Logf("artifacts: %s", dir)
	t.Cleanup(func() {
		if !t.Failed() {
			os.RemoveAll(dir)
		}
	})
	return d
}

// keyPair returns the key pair of the hex seed and fails unless it derives want.
func keyPair(t *testing.T, seedHex string, want types.Address) *wallet.KeyPair {
	t.Helper()
	seed, err := hex.DecodeString(seedHex)
	if err != nil || len(seed) != ed25519.SeedSize {
		t.Fatalf("seed for %s is not %d hex characters", want, 2*ed25519.SeedSize)
	}
	priv := ed25519.NewKeyFromSeed(seed)
	pub := priv.Public().(ed25519.PublicKey)
	addr := types.PubKeyToAddress(pub)
	if addr != want {
		t.Fatalf("seed for %s derives %s", want, addr)
	}
	return &wallet.KeyPair{Public: pub, Private: priv, Address: addr}
}

// momenta is the time allowed for n momenta: twice the nominal time plus 20 momenta.
func momenta(n uint64) time.Duration {
	return time.Duration(n+10) * 2 * momentum
}

// waitFor calls cond until it reports done. A halt error fails at once; any other error is kept
// and retried until timeout.
func (d *devnet) waitFor(what string, timeout time.Duration, cond func() (done bool, last string, err error)) {
	d.t.Helper()
	deadline := time.Now().Add(timeout)
	var last string
	var lastErr error
	for {
		if d.exec != nil {
			d.exec.alive()
		}
		for _, g := range d.gws {
			g.alive()
		}
		done, l, err := cond()
		if err != nil && errors.Is(err, recovery.ErrHalted) {
			d.t.Fatalf("halted while waiting for %s: %v", what, err)
		}
		if err == nil && done {
			return
		}
		if l != "" {
			last = l
		}
		if err != nil {
			lastErr = err
		}
		if time.Now().After(deadline) {
			d.t.Fatalf("timed out after %s waiting for %s; last observed: %s; last error: %v", timeout, what, last, lastErr)
		}
		time.Sleep(poll)
	}
}

// height returns the frontier height and fails the test on a read error.
func (d *devnet) height() uint64 {
	d.t.Helper()
	h, err := d.c.FrontierHeight()
	if err != nil {
		d.t.Fatalf("frontier height: %v", err)
	}
	return h
}

// waitHeight waits for the frontier to reach h.
func (d *devnet) waitHeight(h uint64) {
	d.t.Helper()
	var n uint64
	if f := d.height(); h > f {
		n = h - f
	}
	d.waitFor(fmt.Sprintf("frontier height %d", h), momenta(n), func() (bool, string, error) {
		f, err := d.c.FrontierHeight()
		return err == nil && f >= h, fmt.Sprintf("frontier %d", f), err
	})
}

// send signs and publishes a user send from kp, built as the supervisor builds its sends, and
// returns once a momentum holds it.
func (d *devnet) send(kp *wallet.KeyPair, to types.Address, zts types.ZenonTokenStandard, amount *big.Int, data []byte) types.Hash {
	d.t.Helper()
	prev, err := d.c.FrontierAccountBlock(kp.Address)
	if err != nil {
		d.t.Fatalf("frontier account block of %s: %v", kp.Address, err)
	}
	m, err := d.c.FrontierMomentum()
	if err != nil {
		d.t.Fatalf("frontier momentum: %v", err)
	}
	req, err := d.c.RequiredPlasma(kp.Address, to, data)
	if err != nil {
		d.t.Fatalf("required plasma for %s: %v", kp.Address, err)
	}
	fused := req.BasePlasma
	if m.Version >= nom.DynamicPlasmaMomentumVersion {
		v := new(big.Int).Mul(new(big.Int).SetUint64(req.BasePlasma), new(big.Int).SetUint64(m.NextFusionPrice))
		v.Add(v, new(big.Int).SetUint64(dp.PriceScaleFactor-1))
		v.Div(v, new(big.Int).SetUint64(dp.PriceScaleFactor))
		if !v.IsUint64() {
			d.t.Fatalf("fused plasma for base plasma %d at fusion price %d exceeds %d", req.BasePlasma, m.NextFusionPrice, uint64(math.MaxUint64))
		}
		fused = v.Uint64()
	}
	if req.AvailablePlasma < fused {
		d.t.Fatalf("%s has %d plasma available, the send needs %d", kp.Address, req.AvailablePlasma, fused)
	}

	b := &nom.AccountBlock{
		Version:              1,
		ChainIdentifier:      m.ChainIdentifier,
		BlockType:            nom.BlockTypeUserSend,
		Height:               1,
		PreviousHash:         types.ZeroHash,
		MomentumAcknowledged: m.Identifier(),
		Address:              kp.Address,
		ToAddress:            to,
		TokenStandard:        zts,
		Amount:               amount,
		Data:                 data,
		FusedPlasma:          fused,
	}
	if prev != nil {
		b.Height = prev.Height + 1
		b.PreviousHash = prev.Hash
	}
	b.Hash = b.ComputeHash()
	b.PublicKey = kp.Public
	b.Signature = kp.Sign(b.Hash.Bytes())
	if err := d.c.PublishRawTransaction(b); err != nil {
		d.t.Fatalf("publish send %s from %s: %v", b.Hash, kp.Address, err)
	}
	d.waitFor(fmt.Sprintf("send %s in a momentum", b.Hash), momenta(3), func() (bool, string, error) {
		s, err := d.c.SendStatus(b.Hash)
		return err == nil && s.Block != nil, "send not in a momentum", err
	})
	return b.Hash
}

// received waits for the receive of the send hash and returns the receiving momentum's height.
func (d *devnet) received(hash types.Hash) uint64 {
	d.t.Helper()
	var height uint64
	d.waitFor(fmt.Sprintf("receive of %s", hash), momenta(3), func() (bool, string, error) {
		s, err := d.c.SendStatus(hash)
		if err == nil && s.Received {
			height = s.ReceiveHeight
		}
		return err == nil && s.Received, "send not received", err
	})
	return height
}

// settlementCall sends the admin's call of a Settlement method and returns its receive height.
func (d *devnet) settlementCall(method string, args ...any) uint64 {
	d.t.Helper()
	data, err := definition.ABISettlement.PackMethod(method, args...)
	if err != nil {
		d.t.Fatalf("pack %s: %v", method, err)
	}
	h := d.received(d.send(d.admin, types.SettlementContract, types.ZeroTokenStandard, big.NewInt(0), data))
	d.t.Logf("%s received at momentum %d", method, h)
	return h
}

// view waits for a confirmed view at least at height min in which cond is done, and returns it.
func (d *devnet) view(minHeight uint64, what string, cond func(v *settlement.View) (done bool, last string, err error)) *settlement.View {
	d.t.Helper()
	n := uint64(3)
	if f := d.height(); minHeight+confirmationDepth > f {
		n = minHeight + confirmationDepth - f + 3
	}
	var out *settlement.View
	d.waitFor(what, momenta(n), func() (bool, string, error) {
		h, err := d.r.ConfirmedHeight(confirmationDepth)
		if err != nil {
			return false, "", err
		}
		if h < minHeight {
			return false, fmt.Sprintf("confirmed height %d below %d", h, minHeight), nil
		}
		v, err := d.r.At(h)
		if err != nil {
			return false, "", err
		}
		done, last, err := cond(v)
		if err == nil && done {
			out = v
		}
		return done, fmt.Sprintf("height %d: %s", h, last), err
	})
	return out
}

// domainParams returns the RegisterDomain arguments of the domain with the given status.
func domainParams(status uint8) []any {
	return []any{
		domainID,
		definition.DomainClassExecution,
		definition.RuntimeKindWASM,
		stfSpecHash,
		definition.InputSourceL1Native,
		definition.ExecProfileAttestation,
		definition.ForeignProfileNone,
		[]byte{},
		[]byte{},
		uint8(1),
		definition.ProposerPolicySingle,
		definition.FinalityModelDelay,
		status,
		constants.WithdrawalDelayMin,
		constants.WithdrawalDelayMin,
	}
}

// setup waits for the libp2p switch, then registers domain 1, the executor and the asset
// as the network admin, and fuses plasma to the executor address.
func (d *devnet) setup() {
	d.t.Helper()
	d.t.Logf("waiting for momentum %d, after the libp2p switch", libp2pHeight+1)
	d.waitHeight(libp2pHeight + 1)

	d.view(libp2pHeight+1, "fresh devnet", func(v *settlement.View) (bool, string, error) {
		_, ok, err := v.DomainRecord(domainID)
		return err == nil && !ok, fmt.Sprintf("domain %d registered: %v", domainID, ok), err
	})
	b, err := d.c.FrontierAccountBlock(executorAddress)
	if err != nil {
		d.t.Fatalf("frontier account block of %s: %v", executorAddress, err)
	}
	if b == nil || b.Height != 1 || b.BlockType != nom.BlockTypeGenesisReceive {
		d.t.Fatal("the devnet is not fresh (domain 1 is registered or the executor address has sent since genesis); scripts/devnet-test.sh wipes it")
	}

	d.settlementCall(definition.SettlementRegisterDomainMethodName, domainParams(definition.DomainStatusActive)...)

	assetID := definition.AssetIdFromZTS(types.ZnnTokenStandard)
	registerExecutor := func() uint64 {
		return d.settlementCall(definition.SettlementRegisterExecutorMethodName, pillarName, executorAddress)
	}
	setDAModes := func() uint64 {
		return d.settlementCall(definition.SettlementSetApprovedDAModesMethodName, domainID, []byte{definition.DAModeBestEffort})
	}
	setAsset := func() uint64 {
		return d.settlementCall(definition.SettlementSetAssetConfigMethodName, domainID, assetID, big.NewInt(maxBatchWithdrawal))
	}
	rExec := registerExecutor()
	rDA := setDAModes()
	rAsset := setAsset()

	fuse, err := definition.ABIPlasma.PackMethod(definition.FuseMethodName, executorAddress)
	if err != nil {
		d.t.Fatalf("pack %s: %v", definition.FuseMethodName, err)
	}
	d.received(d.send(d.admin, types.PlasmaContract, types.QsrTokenStandard, fuseAmount, fuse))

	d.waitHeight(rDA + constants.MinSoftDelay + 1)
	setDAModes()
	d.waitHeight(rAsset + constants.MinSoftDelay + 1)
	setAsset()
	d.waitHeight(rExec + constants.MinAdministratorDelay + 1)
	rLast := registerExecutor()

	d.view(rLast, "setup state", func(v *settlement.View) (bool, string, error) {
		rec, domainOK, err := v.DomainRecord(domainID)
		if err != nil {
			return false, "", err
		}
		asset, assetOK, err := v.AssetConfig(domainID, assetID)
		if err != nil {
			return false, "", err
		}
		member, memberOK, err := v.ExecutorSetMember(pillarName)
		if err != nil {
			return false, "", err
		}
		active, err := v.PillarActive(pillarName)
		if err != nil {
			return false, "", err
		}
		req, err := d.c.RequiredPlasma(executorAddress, adminAddress, nil)
		if err != nil {
			return false, "", err
		}
		last := fmt.Sprintf("domain present %v; asset config present %v; executor-set member present %v; pillar active %v; executor plasma %d",
			domainOK, assetOK, memberOK, active, req.AvailablePlasma)
		done := domainOK && rec.Status == definition.DomainStatusActive &&
			assetOK && asset.MaxBatchWithdrawal != nil && asset.MaxBatchWithdrawal.Cmp(big.NewInt(maxBatchWithdrawal)) == 0 &&
			memberOK && member.ExecutorAddress == executorAddress && member.Status == definition.ExecutorStatusActive &&
			active && req.AvailablePlasma > 0
		return done, last, nil
	})
}

// deposit sends one deposit into domain 1 and waits until its input counter is count.
func (d *devnet) deposit(count uint64) uint64 {
	d.t.Helper()
	data, err := definition.ABISettlement.PackMethod(definition.SettlementDepositMethodName, domainID, depositTarget, inputGas)
	if err != nil {
		d.t.Fatalf("pack %s: %v", definition.SettlementDepositMethodName, err)
	}
	h := d.received(d.send(d.admin, types.SettlementContract, types.ZnnTokenStandard, big.NewInt(depositAmount), data))
	d.view(h, fmt.Sprintf("input counter %d", count), func(v *settlement.View) (bool, string, error) {
		n, err := v.DomainInputCounter(domainID)
		return err == nil && n == count, fmt.Sprintf("input counter %d", n), err
	})
	return h
}

// callL2 sends a zero-deposit CallL2 into domain 1, waits until its
// input counter is count and returns the send hash.
func (d *devnet) callL2(count uint64) types.Hash {
	d.t.Helper()
	data, err := definition.ABISettlement.PackMethod(definition.SettlementCallL2MethodName, domainID, callTarget, inputGas, []byte{})
	if err != nil {
		d.t.Fatalf("pack %s: %v", definition.SettlementCallL2MethodName, err)
	}
	send := d.send(d.admin, types.SettlementContract, types.ZeroTokenStandard, big.NewInt(0), data)
	h := d.received(send)
	d.view(h, fmt.Sprintf("input counter %d", count), func(v *settlement.View) (bool, string, error) {
		n, err := v.DomainInputCounter(domainID)
		return err == nil && n == count, fmt.Sprintf("input counter %d", n), err
	})
	return send
}

// setDomainPause sets or clears domain 1's submit pause.
func (d *devnet) setDomainPause(active bool) uint64 {
	d.t.Helper()
	h := d.settlementCall(definition.SettlementPauseMethodName, domainID, false, definition.PauseScopeSubmit, active)
	d.view(h, fmt.Sprintf("submit pause %v", active), func(v *settlement.View) (bool, string, error) {
		p, err := v.DomainPauseState(domainID)
		return err == nil && p.SubmitPaused == active, fmt.Sprintf("submit paused %v", p.SubmitPaused), err
	})
	return h
}

// setRelayPause sets or clears domain 1's relay pause.
func (d *devnet) setRelayPause(active bool) {
	d.t.Helper()
	h := d.settlementCall(definition.SettlementPauseMethodName, domainID, false, definition.PauseScopeRelay, active)
	d.view(h, fmt.Sprintf("relay pause %v", active), func(v *settlement.View) (bool, string, error) {
		p, err := v.DomainPauseState(domainID)
		return err == nil && p.RelayPaused == active, fmt.Sprintf("relay paused %v", p.RelayPaused), err
	})
}

// setDomainStatus re-registers domain 1 with the given status.
func (d *devnet) setDomainStatus(status uint8) uint64 {
	d.t.Helper()
	h := d.settlementCall(definition.SettlementRegisterDomainMethodName, domainParams(status)...)
	d.view(h, fmt.Sprintf("domain status %d", status), func(v *settlement.View) (bool, string, error) {
		rec, ok, err := v.DomainRecord(domainID)
		if err != nil || !ok {
			return false, "domain record absent", err
		}
		return rec.Status == status, fmt.Sprintf("domain status %d", rec.Status), nil
	})
	return h
}

// checkAccepted waits until Settlement has accepted batch b, then checks that its entry equals
// the commitment in the account block of r. It returns the view that holds the entry.
func (d *devnet) checkAccepted(batchID uint64, r supervisor.SendRecord) *settlement.View {
	d.t.Helper()
	v := d.view(0, fmt.Sprintf("batch %d accepted", batchID), func(v *settlement.View) (bool, string, error) {
		cur, err := v.InputSequenceCursor(domainID)
		if err != nil {
			return false, "", err
		}
		return cur.NextBatchId > batchID, fmt.Sprintf("next batch id %d", cur.NextBatchId), nil
	})
	entry, ok, err := v.BatchEntry(domainID, batchID)
	if err != nil || !ok {
		d.t.Fatalf("batch entry %d of domain %d: present %v, error %v", batchID, domainID, ok, err)
	}
	b, err := d.c.AccountBlockAtHeight(executorAddress, r.Height)
	if err != nil || b == nil {
		d.t.Fatalf("account block of %s at height %d: %v, error %v", executorAddress, r.Height, b, err)
	}
	if b.Hash != r.Hash {
		d.t.Fatalf("account block at height %d has hash %s, the send record has %s", r.Height, b.Hash, r.Hash)
	}
	var call struct{ Commitment []byte }
	if err := definition.ABISettlement.UnpackMethod(&call, definition.SettlementSubmitBatchMethodName, b.Data); err != nil {
		d.t.Fatalf("unpack the send at height %d: %v", r.Height, err)
	}
	commitment, err := codec.DecodeBatchCommitment(call.Commitment)
	if err != nil {
		d.t.Fatalf("decode the commitment of the send at height %d: %v", r.Height, err)
	}
	if err := settlement.EntryMatches(entry, commitment); err != nil {
		d.t.Fatalf("batch %d entry differs from the signed commitment: %v", batchID, err)
	}
	if entry.ExecutorPillar != pillarName {
		d.t.Fatalf("batch %d entry names pillar %q, want %q", batchID, entry.ExecutorPillar, pillarName)
	}
	return v
}

// executorHeight returns the height of the executor address's frontier account block: 1, its
// genesis block, before any send.
func (d *devnet) executorHeight() uint64 {
	d.t.Helper()
	b, err := d.c.FrontierAccountBlock(executorAddress)
	if err != nil || b == nil {
		d.t.Fatalf("frontier account block of %s: %v, error %v", executorAddress, b, err)
	}
	return b.Height
}

// executor is one supervisor process.
type executor struct {
	t      *testing.T
	d      *devnet
	name   string
	cmd    *exec.Cmd
	log    string        // its combined stdout and stderr
	exited chan struct{} // closed when cmd.Wait returns
	err    error         // cmd.Wait's result; read only after exited is closed
}

// buildBinaries builds the executor, the wasm worker and the gateway
// without build tags, as a deployment does.
func buildBinaries(t *testing.T, dir string) (executorBin, workerBin, gatewayBin string) {
	t.Helper()
	bin := filepath.Join(dir, "bin") + string(filepath.Separator)
	cmd := exec.Command("go", "build", "-o", bin, "github.com/Zeno-L2/zeno-executor/cmd/executor", "github.com/Zeno-L2/zeno-executor/cmd/worker-wasm", "github.com/Zeno-L2/zeno-executor/cmd/gateway")
	cmd.Env = append(os.Environ(), "GOFLAGS=")
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("build the executor, worker and gateway: %v\n%s", err, out)
	}
	return filepath.Join(bin, "executor"), filepath.Join(bin, "worker-wasm"), filepath.Join(bin, "gateway")
}

// writeConfig writes the executor's key and configuration under dir.
func writeConfig(t *testing.T, dir, workerBin string) (configPath, storagePath string) {
	t.Helper()
	keyPath := filepath.Join(dir, "executor.key")
	if err := os.WriteFile(keyPath, []byte(executorSeed+"\n"), 0o600); err != nil {
		t.Fatalf("write the key: %v", err)
	}
	storagePath = filepath.Join(dir, "storage")
	cfg := map[string]any{
		"executorAddress":     executorAddress.String(),
		"networkName":         "devnet",
		"pillarName":          pillarName,
		"znndEndpoint":        endpoint,
		"confirmationDepth":   confirmationDepth,
		"storagePath":         storagePath,
		"submissionKeySource": map[string]any{"kind": "FILE", "path": keyPath},
		"workers": []any{
			map[string]any{
				"domainId":    domainID,
				"binary":      workerBin,
				"stfSpecHash": stfSpecHash.String(),
				"memoryLimit": 1073741824,
				"cpuLimit":    1.0,
			},
		},
	}
	b, err := json.Marshal(cfg)
	if err != nil {
		t.Fatalf("marshal the configuration: %v", err)
	}
	configPath = filepath.Join(dir, "executor.json")
	if err := os.WriteFile(configPath, b, 0o600); err != nil {
		t.Fatalf("write the configuration: %v", err)
	}
	return configPath, storagePath
}

// startExecutor starts the supervisor binary and registers it as the running executor.
func (d *devnet) startExecutor(name, bin, configPath string) *executor {
	d.t.Helper()
	logPath := filepath.Join(d.dir, name+".log")
	f, err := os.Create(logPath)
	if err != nil {
		d.t.Fatalf("create %s: %v", logPath, err)
	}
	cmd := exec.Command(bin, "-config", configPath)
	cmd.Stdout = f
	cmd.Stderr = f
	if err := cmd.Start(); err != nil {
		f.Close()
		d.t.Fatalf("start executor %s: %v", name, err)
	}
	e := &executor{t: d.t, d: d, name: name, cmd: cmd, log: logPath, exited: make(chan struct{})}
	go func() {
		e.err = cmd.Wait()
		f.Close()
		close(e.exited)
	}()
	d.t.Cleanup(func() {
		select {
		case <-e.exited:
			return
		default:
		}
		cmd.Process.Signal(syscall.SIGTERM)
		select {
		case <-e.exited:
		case <-time.After(60 * time.Second):
			cmd.Process.Kill()
			<-e.exited
		}
	})
	d.exec = e
	return e
}

// alive fails the test when the executor exited, halted, logged an error or was rejected.
func (e *executor) alive() {
	e.t.Helper()
	select {
	case <-e.exited:
		e.t.Fatalf("executor %s exited: %v\n%s", e.name, e.err, e.tail(50))
	default:
	}
	for _, s := range []string{"HALTED", "ERROR:", "rejected by Settlement"} {
		if e.logHas(s) {
			e.t.Fatalf("executor %s logged %q\n%s", e.name, s, e.tail(50))
		}
	}
}

// stop sends SIGTERM and requires a clean exit.
func (e *executor) stop() {
	e.t.Helper()
	if err := e.cmd.Process.Signal(syscall.SIGTERM); err != nil {
		e.t.Fatalf("signal executor %s: %v", e.name, err)
	}
	select {
	case <-e.exited:
	case <-time.After(60 * time.Second):
		e.t.Fatalf("executor %s did not exit within 60s of SIGTERM\n%s", e.name, e.tail(50))
	}
	if e.err != nil {
		e.t.Fatalf("executor %s exited with an error after SIGTERM: %v\n%s", e.name, e.err, e.tail(50))
	}
	e.d.exec = nil
}

// logHas reports whether the executor's log contains s.
func (e *executor) logHas(s string) bool {
	return logHas(e.log, s)
}

// logHas reports whether the file at path contains s.
func logHas(path, s string) bool {
	b, err := os.ReadFile(path)
	return err == nil && bytes.Contains(b, []byte(s))
}

// tail returns the last n lines of the executor's log.
func (e *executor) tail(n int) string {
	return logTail(e.log, n)
}

// logTail returns the last n lines of the file at path.
func logTail(path string, n int) string {
	b, err := os.ReadFile(path)
	if err != nil {
		return fmt.Sprintf("(log unreadable: %v)", err)
	}
	lines := strings.Split(strings.TrimRight(string(b), "\n"), "\n")
	if len(lines) > n {
		lines = lines[len(lines)-n:]
	}
	return strings.Join(lines, "\n")
}

// sendLog reads the executor address's submission log under storagePath. It ignores the bytes
// after the last newline, a line still being written, and returns no records for a missing file.
func sendLog(t *testing.T, storagePath string) []supervisor.SendRecord {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(storagePath, "supervisor", executorAddress.String()+".submissions.jsonl"))
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		t.Fatalf("read the submission log: %v", err)
	}
	i := bytes.LastIndexByte(b, '\n')
	if i < 0 {
		return nil
	}
	b = b[:i]
	var out []supervisor.SendRecord
	for _, line := range bytes.Split(b, []byte("\n")) {
		var r supervisor.SendRecord
		if err := json.Unmarshal(line, &r); err != nil {
			t.Fatalf("decode submission log line %q: %v", line, err)
		}
		out = append(out, r)
	}
	return out
}

// withdrawalFiles returns how many batch files the executor's
// withdrawal store for domain 1 holds under storagePath.
func withdrawalFiles(t *testing.T, storagePath string) int {
	t.Helper()
	dir := filepath.Join(storagePath, "supervisor", executorAddress.String()+".withdrawals", "1")
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read the withdrawals store: %v", err)
	}
	n := 0
	for _, e := range entries {
		if filepath.Ext(e.Name()) == ".json" {
			n++
		}
	}
	return n
}
