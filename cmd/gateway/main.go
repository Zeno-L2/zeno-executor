// Command gateway is the gateway: it follows each configured domain's
// executor directory and Settlement, and keeps each domain's checked
// state. It serves JSON-RPC over HTTP at listenAddress.
package main

import (
	"context"
	"errors"
	"flag"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"sync/atomic"
	"syscall"

	"github.com/zenon-network/go-zenon/vm/embedded/definition"

	"github.com/Zeno-L2/zeno-executor/domain"
	"github.com/Zeno-L2/zeno-executor/internal/gateway"
	"github.com/Zeno-L2/zeno-executor/internal/lease"
	"github.com/Zeno-L2/zeno-executor/internal/settlement"
	"github.com/Zeno-L2/zeno-executor/internal/znnd"
	wasmreader "github.com/Zeno-L2/zeno-executor/plugins/wasm/reader"
)

// exitConfig is the exit status for an invalid configuration:
// sysexits EX_CONFIG, the status the executor uses.
const exitConfig = 78

// afterPrepare runs once PrepareStorage has returned. Tests set it to
// change the filesystem at that point.
var afterPrepare = func() {}

// waitForNode blocks until the znnd node answers. Tests replace it so
// a run can boot its domains without a node.
var waitForNode = gateway.WaitForNode

// onListen runs once the listener is bound. Tests set it to learn the
// address or to close the listener.
var onListen = func(net.Listener) {}

func main() { os.Exit(run(os.Args[1:])) }

// run loads the configuration, waits for the znnd node, boots every
// domain, serves JSON-RPC and follows the domains until a signal. It
// returns the process exit status.
func run(args []string) int {
	fs := flag.NewFlagSet("gateway", flag.ContinueOnError)
	path := fs.String("config", "", "path to the gateway configuration file")
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return exitConfig
	}

	cfg, err := gateway.LoadConfig(*path)
	if err != nil {
		log.Printf("%v", err)
		return exitStatus(err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	storage, err := gateway.PrepareStorage(cfg.StoragePath)
	if err != nil {
		log.Printf("gateway: %v", err)
		return 1
	}
	afterPrepare()
	lockPath := filepath.Join(storage, "gateway.lock")
	if fi, err := os.Lstat(lockPath); err == nil && fi.Mode()&os.ModeSymlink != 0 {
		log.Printf("gateway: %s is a symlink", lockPath)
		return 1
	}
	lock, err := lease.Acquire(lockPath)
	if err != nil {
		log.Printf("gateway: %v", err)
		return 1
	}
	defer lock.Release()

	client := znnd.NewClient(cfg.ZnndEndpoint)
	reader := settlement.NewReader(client)
	senders := gateway.NewZnndSenders(client)
	if !waitForNode(ctx, cfg.ZnndEndpoint, func() error { return reader.CheckNode(cfg.ConfirmationDepth) }, log.Printf) {
		return 0
	}

	s := settlementReader{r: reader, depth: cfg.ConfirmationDepth}
	chains := make([]*gateway.Chain, 0, len(cfg.Domains))
	// The closure reads chains when run returns; Follow has returned by
	// then, so no Poll writes while the indexes close.
	defer func() {
		for _, c := range chains {
			if err := c.Close(); err != nil {
				log.Printf("%v", err)
			}
		}
	}()
	for _, d := range cfg.Domains {
		c, err := gateway.OpenChain(ctx, storage, d, s, senders, log.Printf)
		if err != nil {
			if ctx.Err() != nil {
				return 0
			}
			log.Printf("gateway: domain %d: boot failed: %s", d.DomainID, gateway.RedactEndpointErr(cfg.ZnndEndpoint, err))
			return exitStatus(err)
		}
		chains = append(chains, c)
	}

	ln, err := net.Listen("tcp", cfg.ListenAddress)
	if err != nil {
		log.Printf("gateway: listen on %s: %v", cfg.ListenAddress, err)
		return 1
	}
	log.Printf("gateway: serving JSON-RPC on http://%s", ln.Addr())
	onListen(ln)
	api := gateway.NewServer(cfg, chains, map[uint8]domain.Reader{definition.RuntimeKindWASM: wasmreader.New(cfg.NetworkName)}, log.Printf)
	if err := api.StartSimulation(); err != nil {
		log.Printf("gateway: %v", err)
		ln.Close()
		return exitStatus(err)
	}
	hs := gateway.NewHTTPServer(api)
	fctx, cancel := context.WithCancel(ctx)
	defer cancel()
	var serveFailed atomic.Bool
	go func() {
		if err := hs.Serve(ln); !errors.Is(err, http.ErrServerClosed) {
			log.Printf("gateway: the JSON-RPC server stopped: %v", err)
			serveFailed.Store(true)
			cancel()
		}
	}()

	gateway.Follow(fctx, chains, cfg.ZnndEndpoint, log.Printf)
	// Follow has returned, so no index write is in progress and a second
	// signal may end the process while the server drains.
	stop()
	api.Shutdown(hs)
	if serveFailed.Load() {
		return 1
	}
	return 0
}

// exitStatus maps an error to the process exit status: exitConfig for
// an error wrapping gateway.ErrConfig, 1 for any other error, 0 for
// nil.
func exitStatus(err error) int {
	switch {
	case err == nil:
		return 0
	case errors.Is(err, gateway.ErrConfig):
		return exitConfig
	default:
		return 1
	}
}

// settlementReader adapts settlement.Reader to gateway.Settlement at
// the configured confirmation depth.
type settlementReader struct {
	r     *settlement.Reader
	depth uint64
}

func (s settlementReader) ConfirmedHeight() (uint64, error) {
	return s.r.ConfirmedHeight(s.depth)
}

func (s settlementReader) At(height uint64) (gateway.SettlementView, error) {
	v, err := s.r.At(height)
	if err != nil {
		return nil, err
	}
	return v, nil
}

// *settlement.View must keep satisfying the engine's view.
var _ gateway.SettlementView = (*settlement.View)(nil)
