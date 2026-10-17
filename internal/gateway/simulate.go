package gateway

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"sync"
	"syscall"
	"time"

	"github.com/zenon-network/go-zenon/common/types"

	"github.com/digitalSloth/go-executor/domain"
	"github.com/digitalSloth/go-executor/internal/codec"
	"github.com/digitalSloth/go-executor/internal/simipc"
	"github.com/digitalSloth/go-executor/internal/state"
	"github.com/digitalSloth/go-executor/internal/workerproc"
)

// The simulator pools' bounds.
const (
	simMemoryLimit  = simipc.DefaultMemoryLimit // the Config frame's soft limit
	simMemoryMax    = simMemoryLimit + 1<<30    // memory.max as root on Linux
	simCPUs         = 1
	simStartTimeout = 5 * time.Second
	simQueueWait    = time.Second
	simRestartBase  = time.Second
	simRestartMax   = 30 * time.Second
	simStrikeWindow = time.Minute
	// simReplyMargin is how long before a simulation's deadline its
	// caps reads stop, so the reply is written in time.
	simReplyMargin  = 100 * time.Millisecond
	maxSimCapsReads = 4
)

// errCapsBusy reports a caps read refused because maxSimCapsReads
// reads are running.
var errCapsBusy = errors.New("simulation caps reads are at their cap")

// enforcer reports what a host enforces. *workerproc.Host satisfies
// it; tests stand in for a root host whose cgroup setup failed.
type enforcer interface {
	Enforcement() (limits, uids bool, reason string)
}

// simTiming holds the waits a pool uses; tests shorten them.
type simTiming struct {
	queueWait, restartBase time.Duration
}

// simPool is one domain's simulators.
type simPool struct {
	s        *Server // logf, logInternal, endpoint, networkName, simCapsSem
	domainID uint32
	spec     workerproc.Spec // Path, UID, Limits; Name and Conn set per launch
	timeout  time.Duration
	timing   simTiming
	start    func(workerproc.Spec) (*workerproc.Process, error)
	fileConn func(*os.File) (net.Conn, error) // net.FileConn; tests make it fail
	jobs     chan *simJob                     // unbuffered: a send succeeds only when a slot is idle
	ctx      context.Context
	wg       sync.WaitGroup
}

// simulator is one started simulator process and its channel.
type simulator struct {
	proc   *workerproc.Process
	conn   net.Conn      // net.FileConn of the gateway end; nil until set
	exited chan struct{} // closed once proc.Wait returns
	exit   workerproc.Exit
}

// simJob is one simulation handed to a slot.
type simJob struct {
	req     simipc.Simulate
	version state.Version     // the tag's kept state
	code    domain.CodeSource // nil when the domain's reader serves no code
	rc      *readContext      // the tag's batch with a fresh budget, for code
	view    SettlementView    // the newest poll's raw view, for caps
	done    chan simOutcome   // buffered 1

	// reads counts the simulation's ReadRequests; only the slot serving
	// the job touches it.
	reads int
}

// simOutcome is what a slot answers a job with.
type simOutcome struct {
	res  simipc.Result
	exec codec.ExecutionResult // decoded res.ExecutionResult
	err  *rpcError             // set when the simulation produced no result
}

// simEnabled reports whether the configuration turns simulation on.
func (s *Server) simEnabled() bool {
	return s.sim.Enabled != nil && *s.sim.Enabled
}

// StartSimulation starts every domain's simulator pool when simulation
// is enabled. Call it once, before the server serves.
func (s *Server) StartSimulation() error {
	if !s.simEnabled() {
		return nil
	}
	if err := workerproc.SealInheritedFDs(); err != nil {
		return fmt.Errorf("simulation: %w", err)
	}
	if err := workerproc.SetNonDumpable(); err != nil {
		return fmt.Errorf("simulation: %w", err)
	}
	h := workerproc.NewHost()
	return s.startSimulation(h, h.Start)
}

// startSimulation checks the host and the configuration, then starts a
// pool per domain whose simulators start through start.
func (s *Server) startSimulation(enf enforcer, start func(workerproc.Spec) (*workerproc.Process, error)) error {
	if !s.simEnabled() {
		return nil
	}
	limits, uids, reason := enf.Enforcement()
	uid, err := simUID(uids, s.sim.UID)
	if err != nil {
		return err
	}
	if uids && !limits {
		return fmt.Errorf("simulation needs cgroup v2 memory and cpu limits when the gateway runs as root, but they are off: %s; delegate both controllers to the gateway's cgroup or set simulation.enabled to false", reason)
	}
	if !uids && euid() == 0 {
		return errors.New("simulation needs a host that separates uids when the gateway runs as root, and this host does not; run the gateway as an unprivileged user or set simulation.enabled to false")
	}
	for i, c := range s.order {
		if s.simBinary[c.cfg.DomainID] == "" {
			return fmt.Errorf("%w: domains[%d].workerBinary is required when simulation is enabled", ErrConfig, i)
		}
	}
	line := fmt.Sprintf("gateway: simulation enforcement: limits %s, uids %s", onOff(limits), onOff(uids))
	if reason != "" {
		line += ": " + reason
	}
	if !uids && s.sim.UID != nil {
		line += ", simulation.uid ignored"
	}
	s.logf("%s", line)

	s.simCtx, s.simCancel = context.WithCancel(context.Background())
	s.sims = make(map[uint32]*simPool, len(s.order))
	for _, c := range s.order {
		id := c.cfg.DomainID
		p := &simPool{
			s:        s,
			domainID: id,
			spec: workerproc.Spec{
				Path:   s.simBinary[id],
				UID:    uid,
				Limits: workerproc.Limits{MemoryBytes: simMemoryMax, CPUs: simCPUs},
			},
			timeout:  time.Duration(s.sim.TimeoutMs) * time.Millisecond,
			timing:   s.simTiming,
			start:    start,
			fileConn: net.FileConn,
			jobs:     make(chan *simJob),
			ctx:      s.simCtx,
		}
		s.sims[id] = p
		for slot := 0; slot < s.sim.MaxConcurrent; slot++ {
			p.wg.Add(1)
			go p.run(slot)
		}
	}
	return nil
}

func onOff(b bool) string {
	if b {
		return "on"
	}
	return "off"
}

// stopSimulation ends every pool and waits for its slots to return. It
// does nothing when no pool was started.
func (s *Server) stopSimulation() {
	if s.simCancel == nil {
		return
	}
	s.simCancel()
	for _, p := range s.sims {
		p.wg.Wait()
	}
}

// simUID returns the uid simulators run as: uid when the host separates
// uids, which then must be set, and 0 otherwise.
func simUID(uids bool, uid *int) (int, error) {
	if !uids {
		return 0, nil
	}
	if uid == nil {
		return 0, fmt.Errorf("%w: simulation.uid is required when the gateway runs as root on Linux", ErrConfig)
	}
	return *uid, nil
}

// relaunchDelay is the wait before a slot's relaunch after its nth
// strike: none for the first, then base doubling each strike, up to
// max.
func relaunchDelay(strikes int, base, max time.Duration) time.Duration {
	if strikes <= 1 {
		return 0
	}
	shift := strikes - 2
	if shift >= 32 || base<<shift > max || base<<shift <= 0 {
		return max
	}
	return base << shift
}

// run is one slot: it owns one simulator at a time, from its launch to
// its reap, until the pool's context ends.
func (p *simPool) run(slot int) {
	defer p.wg.Done()
	var (
		strikes    int
		lastStrike time.Time
	)
	// strike counts one abnormal end of the slot's simulator and
	// returns the wait before its replacement.
	strike := func() time.Duration {
		now := time.Now()
		if now.Sub(lastStrike) >= simStrikeWindow {
			strikes = 0
		}
		strikes++
		lastStrike = now
		return relaunchDelay(strikes, p.timing.restartBase, simRestartMax)
	}
	// wait sleeps for d and reports whether the pool is still running.
	wait := func(d time.Duration) bool {
		if d <= 0 {
			return p.ctx.Err() == nil
		}
		t := time.NewTimer(d)
		defer t.Stop()
		select {
		case <-t.C:
			return true
		case <-p.ctx.Done():
			return false
		}
	}

	for {
		m, err := p.launch(slot)
		if err != nil {
			if p.ctx.Err() != nil {
				return
			}
			d := strike()
			p.s.logf("gateway: domain %d: simulator %d did not start: %s; next attempt in %s",
				p.domainID, slot, RedactEndpointErr(p.s.endpoint, err), d)
			if !wait(d) {
				return
			}
			continue
		}

		var d time.Duration
	idle:
		for {
			select {
			case j := <-p.jobs:
				out, healthy, cause := p.serve(m, j)
				if healthy {
					j.done <- out
					continue
				}
				p.kill(m)
				if out.err != nil && out.err.Message == stoppedErr(workerproc.Exit{}).Message {
					out.err = stoppedErr(m.exit)
				}
				j.done <- out
				d = strike()
				var ne net.Error
				if !(errors.As(cause, &ne) && ne.Timeout()) {
					p.s.logf("gateway: domain %d: simulator %d was replaced (%s): %s; next start in %s",
						p.domainID, slot, exitText(m.exit), RedactEndpointErr(p.s.endpoint, cause), d)
				}
				break idle
			case <-m.exited:
				p.kill(m)
				d = strike()
				p.s.logf("gateway: domain %d: simulator %d exited while idle (%s); next start in %s",
					p.domainID, slot, exitText(m.exit), d)
				break idle
			case <-p.ctx.Done():
				p.kill(m)
				return
			}
		}
		if !wait(d) {
			return
		}
	}
}

// launch starts a simulator and brings it to Ready. Every error return
// after the process started returns m, so the deferred cleanup kills
// and reaps it.
func (p *simPool) launch(slot int) (m *simulator, err error) {
	gwEnd, wEnd, err := workerproc.Socketpair()
	if err != nil {
		return nil, err
	}
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("the launch panicked: %v (at %s)", r, panicSite())
		}
		if wEnd != nil {
			wEnd.Close()
		}
		if gwEnd != nil {
			gwEnd.Close()
		}
		if err != nil && m != nil {
			p.kill(m)
			err = fmt.Errorf("%w (%s)", err, exitText(m.exit))
			m = nil
		}
	}()
	spec := p.spec
	spec.Name, spec.Conn = fmt.Sprintf("sim-%d-%d", p.domainID, slot), wEnd
	proc, err := p.start(spec)
	wEnd.Close()
	wEnd = nil
	if err != nil {
		return nil, err
	}
	m = &simulator{proc: proc, exited: make(chan struct{})}
	go func() { m.exit = proc.Wait(); close(m.exited) }()
	m.conn, err = p.fileConn(gwEnd)
	gwEnd.Close()
	gwEnd = nil
	if err != nil {
		return m, err
	}

	m.conn.SetDeadline(time.Now().Add(simStartTimeout))
	stop := context.AfterFunc(p.ctx, func() { m.conn.SetDeadline(time.Now()) })
	err = simipc.WriteMessage(m.conn, simipc.Config{DomainID: p.domainID, MemoryLimit: simMemoryLimit, NetworkName: p.s.networkName})
	if err != nil {
		stop()
		return m, err
	}
	msg, err := simipc.ReadFromSimulator(m.conn)
	if err != nil {
		stop()
		return m, err
	}
	if _, ok := msg.(simipc.Ready); !ok {
		stop()
		return m, fmt.Errorf("the simulator answered its Config with frame type 0x%02x, not Ready", uint8(msg.Type()))
	}
	if !stop() {
		return m, p.ctx.Err()
	}
	m.conn.SetDeadline(time.Time{})
	return m, nil
}

// kill ends the simulator and waits until its process is reaped. Only
// the slot that owns m calls it.
func (p *simPool) kill(m *simulator) {
	_ = m.proc.Signal(syscall.SIGKILL)
	if m.conn != nil {
		m.conn.Close()
	}
	<-m.exited
}

// exitText describes how a simulator ended.
func exitText(e workerproc.Exit) string {
	switch {
	case e.OOMKilled:
		return "killed for memory (OOM)"
	case e.Signal != 0:
		return "signal " + e.Signal.String()
	default:
		return fmt.Sprintf("exit status %d", e.Code)
	}
}

// stoppedErr is the -32006 error of a simulator that stopped during a
// simulation: "the simulator ran out of memory" when e.OOMKilled, else
// "the simulator stopped during the simulation; retry".
func stoppedErr(e workerproc.Exit) *rpcError {
	if e.OOMKilled {
		return &rpcError{codeSimulation, "the simulator ran out of memory"}
	}
	return &rpcError{codeSimulation, "the simulator stopped during the simulation; retry"}
}

// serve runs j on m. healthy is false when m must be replaced, and
// cause then says why; out.err then holds the caller's error.
func (p *simPool) serve(m *simulator, j *simJob) (out simOutcome, healthy bool, cause error) {
	defer func() {
		if r := recover(); r != nil {
			out = simOutcome{err: &rpcError{codeInternal, "internal error"}}
			healthy = false
			cause = fmt.Errorf("serve panicked: %v (at %s)", r, panicSite())
			p.s.logInternal("zeno_simulate", cause)
		}
	}()
	// broken ends the simulation for an error of the channel: a
	// deadline reads as the timeout, anything else as a stop.
	broken := func(err error) (simOutcome, bool, error) {
		var ne net.Error
		if errors.As(err, &ne) && ne.Timeout() {
			return simOutcome{err: &rpcError{codeSimulation, fmt.Sprintf("simulation exceeded timeoutMs (%d ms)", p.timeout.Milliseconds())}}, false, err
		}
		return simOutcome{err: stoppedErr(workerproc.Exit{})}, false, err
	}

	deadline := time.Now().Add(p.timeout)
	if err := m.conn.SetDeadline(deadline); err != nil {
		return broken(err)
	}
	if err := simipc.WriteMessage(m.conn, j.req); err != nil {
		return broken(err)
	}
	var codeSeen, capsSeen bool
	for {
		msg, err := simipc.ReadFromSimulator(m.conn)
		if err != nil {
			return broken(err)
		}
		var reply simipc.Message
		switch f := msg.(type) {
		case simipc.ReadRequest:
			reply = p.answerRead(j, f)
		case simipc.CodeRequest:
			if codeSeen {
				return broken(errors.New("the simulator sent a second CodeRequest"))
			}
			codeSeen = true
			reply = p.answerCode(j, f)
		case simipc.CapRequest:
			if capsSeen {
				return broken(errors.New("the simulator sent a second CapRequest"))
			}
			capsSeen = true
			reply = p.answerCaps(j, f, deadline)
		case simipc.Result:
			er, err := codec.DecodeExecutionResult(f.ExecutionResult)
			switch {
			case err != nil:
				return broken(fmt.Errorf("the simulator's execution result does not decode: %w", err))
			case er.GlobalInputIndex != j.req.GlobalInputIndex:
				return broken(fmt.Errorf("the simulator's result is for input %d, not %d", er.GlobalInputIndex, j.req.GlobalInputIndex))
			case er.PreStateRoot != (types.Hash{}) || er.PostStateRoot != (types.Hash{}):
				return broken(errors.New("the simulator's result carries a state root"))
			}
			_ = m.conn.SetDeadline(time.Time{})
			if j.reads > simipc.MaxStateReads {
				return simOutcome{err: &rpcError{codeSimulation, simReadsMsg}}, true, nil
			}
			return simOutcome{res: f, exec: er}, true, nil
		case simipc.Failed:
			_ = m.conn.SetDeadline(time.Time{})
			if j.reads > simipc.MaxStateReads {
				return simOutcome{err: &rpcError{codeSimulation, simReadsMsg}}, true, nil
			}
			return simOutcome{err: &rpcError{codeSimulation, "the simulator could not run the input: " + f.Message}}, true, nil
		default:
			return broken(fmt.Errorf("the simulator sent frame type 0x%02x during a simulation", uint8(msg.Type())))
		}
		err = simipc.WriteMessage(m.conn, reply)
		if errors.Is(err, simipc.ErrMalformed) {
			// The gateway's own reply was refused by the frame rules.
			err = simipc.WriteMessage(m.conn, simipc.Unavailable{})
		}
		if err != nil {
			return broken(err)
		}
	}
}

// answerRead answers one ReadRequest from the tag's kept state, or with
// Unavailable once the simulation has used its simipc.MaxStateReads
// reads.
func (p *simPool) answerRead(j *simJob, req simipc.ReadRequest) simipc.Message {
	j.reads++
	if j.reads > simipc.MaxStateReads {
		return simipc.Unavailable{}
	}
	present, value, _ := j.version.Prove(req.Path)
	if !present {
		value = nil
	}
	return simipc.Value{Present: present, Value: value}
}

// answerCode answers the simulation's CodeRequest. A panic in the
// reader's code source is logged and answered as Unavailable.
func (p *simPool) answerCode(j *simJob, req simipc.CodeRequest) simipc.Message {
	if j.code == nil {
		return simipc.Unavailable{}
	}
	module, err := func() (module []byte, err error) {
		defer func() {
			if r := recover(); r != nil {
				err = fmt.Errorf("the code fetch panicked: %v (at %s)", r, panicSite())
			}
		}()
		return j.code.Code(j.rc, req.SourceCodeHash)
	}()
	var re *rpcError
	switch {
	case err == nil:
		return simipc.Code{Found: true, Module: module}
	case errors.Is(err, domain.ErrNotFound):
		return simipc.Code{Found: false}
	case errors.As(err, &re):
		return simipc.Unavailable{}
	default:
		p.s.logInternal("zeno_simulate", err)
		return simipc.Unavailable{}
	}
}

// answerCaps answers the simulation's CapRequest with one cap per
// asset, read from the newest poll's Settlement view. The reads stop
// simReplyMargin before the simulation's deadline. The first read that
// fails answers Unavailable for the whole request.
func (p *simPool) answerCaps(j *simJob, req simipc.CapRequest, deadline time.Time) simipc.Message {
	capsBy := deadline.Add(-simReplyMargin)
	ctx, cancel := context.WithDeadline(p.ctx, capsBy)
	defer cancel()
	amounts := make([]codec.U256, 0, len(req.Assets))
	for _, asset := range req.Assets {
		amount, err := withDeadline(ctx, time.Until(capsBy), func() (codec.U256, error) {
			select {
			case p.s.simCapsSem <- struct{}{}:
			default:
				return codec.U256{}, errCapsBusy
			}
			defer func() { <-p.s.simCapsSem }()
			cfg, ok, err := j.view.AssetConfig(p.domainID, types.Hash(asset))
			if err != nil || !ok {
				return codec.U256{}, err
			}
			return codec.U256FromBig(cfg.MaxBatchWithdrawal)
		})
		if err != nil {
			if !errors.Is(err, errCapsBusy) && p.ctx.Err() == nil {
				p.s.logInternal("zeno_simulate", fmt.Errorf("settlement: read AssetConfig of %x: %w", asset[:], err))
			}
			return simipc.Unavailable{}
		}
		amounts = append(amounts, amount)
	}
	return simipc.Caps{Amounts: amounts}
}
