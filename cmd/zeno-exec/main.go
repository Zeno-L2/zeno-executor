package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/signal"
	"runtime/debug"
	"strconv"
	"syscall"
	"time"

	"github.com/digitalSloth/go-executor/plugins/wasm"
	"github.com/digitalSloth/go-executor/plugins/wasm/zenoexec"
)

// version is set by -ldflags "-X main.version=<v>".
var version = "devel"

const defaultNetwork = "testnet"

func main() { os.Exit(run(os.Args[1:], os.Stdin, os.Stdout, os.Stderr)) }

// originList is a repeatable --allow-origin flag. Set refuses a
// malformed origin, so parsing fails before any listen.
type originList []string

func (o *originList) String() string { return fmt.Sprint([]string(*o)) }

func (o *originList) Set(s string) error {
	if err := zenoexec.CheckOrigin(s); err != nil {
		return err
	}
	*o = append(*o, s)
	return nil
}

// versionInfo reads the VCS stamp of the build; absent settings give ""
// and false.
func versionInfo() zenoexec.VersionInfo {
	v := zenoexec.VersionInfo{Version: version}
	if bi, ok := debug.ReadBuildInfo(); ok {
		for _, s := range bi.Settings {
			switch s.Key {
			case "vcs.revision":
				v.Revision = s.Value
			case "vcs.modified":
				v.Modified = s.Value == "true"
			}
		}
	}
	return v
}

// run executes one subcommand and returns the process exit status.
func run(args []string, stdin io.Reader, stdout, stderr io.Writer) int {
	usageError := func(format string, a ...any) int {
		fmt.Fprintf(stderr, format+"\n", a...)
		fmt.Fprint(stderr, usage)
		return 2
	}
	if len(args) == 0 {
		return usageError("zeno-exec: a subcommand is required")
	}
	name, rest := args[0], args[1:]
	switch name {
	case "-h", "--help", "help":
		fmt.Fprint(stderr, usage)
		return 0
	case "version", "validate", "run", "serve":
	default:
		return usageError("zeno-exec: unknown subcommand %q", name)
	}

	fs := flag.NewFlagSet(name, flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	var metadata, network, listen string
	var gasBudget uint64
	var origins originList
	switch name {
	case "validate":
		fs.StringVar(&metadata, "metadata", "", "")
	case "run":
		fs.StringVar(&network, "network", defaultNetwork, "")
		fs.Uint64Var(&gasBudget, "gas-budget", 0, "")
	case "serve":
		fs.StringVar(&network, "network", defaultNetwork, "")
		fs.StringVar(&listen, "listen", "127.0.0.1:8547", "")
		fs.Var(&origins, "allow-origin", "")
	}
	if err := fs.Parse(rest); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			fmt.Fprint(stderr, usage)
			return 0
		}
		return usageError("zeno-exec %s: %v", name, err)
	}
	pos := fs.Args()
	wantPos := 0
	if name == "validate" {
		wantPos = 1
	}
	if len(pos) != wantPos {
		return usageError("zeno-exec %s: expected %d argument(s), got %d", name, wantPos, len(pos))
	}
	if (name == "run" || name == "serve") && network == "" {
		return usageError("zeno-exec %s: --network must not be empty", name)
	}

	enc := json.NewEncoder(stdout)
	switch name {
	case "version":
		_ = enc.Encode(versionInfo())
		return 0
	case "validate":
		return validate(pos[0], metadata, fs, enc, stderr, usageError)
	case "run":
		if err := zenoexec.RunWithBudget(network, gasBudget, stdin, stdout); err != nil {
			if !errors.Is(err, zenoexec.ErrLineTooLong) {
				fmt.Fprintf(stderr, "zeno-exec run: %v\n", err)
			}
			return 1
		}
		return 0
	default:
		return serve(network, listen, origins, stdout, stderr, usageError)
	}
}

func validate(path, metadataFlag string, fs *flag.FlagSet, enc *json.Encoder, stderr io.Writer, usageError func(string, ...any) int) int {
	var metadata []byte
	given := false
	fs.Visit(func(f *flag.Flag) { given = given || f.Name == "metadata" })
	if given {
		m, err := zenoexec.ParseMetadataFlag(metadataFlag)
		if err != nil {
			return usageError("zeno-exec validate: --metadata: %v", err)
		}
		metadata = m
	}
	raw, err := readModule(path)
	if err != nil {
		return usageError("zeno-exec validate: %v", err)
	}
	result, ok := zenoexec.Validate(raw, metadata)
	_ = enc.Encode(result)
	if !ok {
		return 1
	}
	return 0
}

// readModule reads a regular file, stopping one byte past the largest
// module so that the module checks still report an oversized file.
func readModule(path string) ([]byte, error) {
	fi, err := os.Stat(path)
	if err != nil {
		return nil, err
	}
	if !fi.Mode().IsRegular() {
		return nil, fmt.Errorf("%s is not a regular file", path)
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	return io.ReadAll(io.LimitReader(f, wasm.MaxCodeSize+1))
}

func serve(network, listen string, origins originList, stdout, stderr io.Writer, usageError func(string, ...any) int) int {
	if err := zenoexec.CheckListen(listen); err != nil {
		return usageError("zeno-exec serve: --listen: %v", err)
	}
	ln, err := net.Listen("tcp", listen)
	if err != nil {
		fmt.Fprintf(stderr, "zeno-exec serve: %v\n", err)
		return 1
	}
	defer ln.Close()
	exe, err := os.Executable()
	if err != nil {
		fmt.Fprintf(stderr, "zeno-exec serve: %v\n", err)
		return 1
	}
	h, err := zenoexec.NewHandler(zenoexec.ServeConfig{
		Network: network,
		ChildCommand: []string{exe, "run", "--network", network,
			"--gas-budget", strconv.FormatUint(zenoexec.ServeGasBudget, 10)},
		Port:         ln.Addr().(*net.TCPAddr).Port,
		AllowOrigins: origins,
		Version:      versionInfo(),
	})
	if err != nil {
		return usageError("zeno-exec serve: %v", err)
	}
	_ = json.NewEncoder(stdout).Encode(map[string]string{"listen": ln.Addr().String()})

	srv := &http.Server{Handler: h, ReadHeaderTimeout: 5 * time.Second}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	errc := make(chan error, 1)
	go func() { errc <- srv.Serve(ln) }()
	select {
	case err := <-errc:
		fmt.Fprintf(stderr, "zeno-exec serve: %v\n", err)
		return 1
	case <-ctx.Done():
	}
	shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = srv.Shutdown(shutdown)
	zenoexec.StopSessions()
	return 0
}
