package supervisor

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/zenon-network/go-zenon/chain/nom"
	"github.com/zenon-network/go-zenon/common/types"
	"github.com/zenon-network/go-zenon/vm/embedded/definition"

	"github.com/Zeno-L2/zeno-executor/internal/codec"
	"github.com/Zeno-L2/zeno-executor/internal/recovery"
	"github.com/Zeno-L2/zeno-executor/internal/settlement"
	"github.com/Zeno-L2/zeno-executor/internal/znnd"
)

// sendNodeFake is a SendNode over a small account chain. It embeds fakeBlockNode for the reads
// buildSend makes.
type sendNodeFake struct {
	*fakeBlockNode
	blocks   map[uint64]*nom.AccountBlock
	statuses map[types.Hash]znnd.SendStatus

	publish    func(b *nom.AccountBlock) error             // nil: the node accepts the block
	status     func(h types.Hash) (znnd.SendStatus, error) // nil: statuses
	atHeight   func(addr types.Address, h uint64) (*nom.AccountBlock, error)
	onPlasma   func()
	frontierFn func(call int) *nom.AccountBlock // nil: fakeBlockNode's frontier

	published     []*nom.AccountBlock
	statusCalls   []types.Hash
	plasmaCalls   int
	frontierCalls int
}

func newSendNodeFake() *sendNodeFake {
	return &sendNodeFake{
		fakeBlockNode: &fakeBlockNode{
			momentum: testMomentum(1, 0),
			req:      znnd.PlasmaRequirement{AvailablePlasma: 100000, BasePlasma: 21000},
		},
		blocks:   map[uint64]*nom.AccountBlock{},
		statuses: map[types.Hash]znnd.SendStatus{},
	}
}

// put places b on the node's account chain, pool included.
func (n *sendNodeFake) put(b *nom.AccountBlock) {
	n.blocks[b.Height] = b
	if n.frontier == nil || b.Height >= n.frontier.Height {
		n.frontier = b
	}
}

func (n *sendNodeFake) FrontierAccountBlock(a types.Address) (*nom.AccountBlock, error) {
	n.frontierCalls++
	if n.frontierFn != nil {
		return n.frontierFn(n.frontierCalls), nil
	}
	return n.fakeBlockNode.FrontierAccountBlock(a)
}

func (n *sendNodeFake) RequiredPlasma(from, to types.Address, data []byte) (znnd.PlasmaRequirement, error) {
	n.plasmaCalls++
	if n.onPlasma != nil {
		n.onPlasma()
	}
	return n.fakeBlockNode.RequiredPlasma(from, to, data)
}

func (n *sendNodeFake) PublishRawTransaction(b *nom.AccountBlock) error {
	n.published = append(n.published, b)
	if n.publish != nil {
		if err := n.publish(b); err != nil {
			return err
		}
	}
	n.put(b)
	return nil
}

func (n *sendNodeFake) SendStatus(h types.Hash) (znnd.SendStatus, error) {
	n.statusCalls = append(n.statusCalls, h)
	if n.status != nil {
		return n.status(h)
	}
	return n.statuses[h], nil
}

func (n *sendNodeFake) AccountBlockAtHeight(addr types.Address, h uint64) (*nom.AccountBlock, error) {
	if n.atHeight != nil {
		return n.atHeight(addr, h)
	}
	return n.blocks[h], nil
}

func (n *sendNodeFake) statusCount(h types.Hash) int {
	c := 0
	for _, x := range n.statusCalls {
		if x == h {
			c++
		}
	}
	return c
}

// packSubmit returns the Data of a SubmitBatch send for c.
func packSubmit(t *testing.T, c codec.BatchCommitment) []byte {
	t.Helper()
	enc, err := c.Encode()
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	data, err := definition.ABISettlement.PackMethod(definition.SettlementSubmitBatchMethodName, enc)
	if err != nil {
		t.Fatalf("PackMethod: %v", err)
	}
	return data
}

type entryKey struct {
	domain uint32
	batch  uint64
}

// submitHarness drives a Supervisor's submitter step by step against fake sources.
type submitHarness struct {
	t     *testing.T
	sup   *Supervisor
	sub   *submitter
	node  *sendNodeFake
	out   *lockedBuffer
	clock time.Time

	tips     map[uint32]settlement.Tip
	tipErr   map[uint32]error
	tipCalls map[uint32]int
	onTip    func(d uint32) // called before each Tip read
	entries  map[entryKey]*definition.BatchEntry

	gate      settlement.Gates // what a domain's gate read returns unless gateBy or gateErr names it
	gateBy    map[uint32]settlement.Gates
	gateErr   map[uint32]error
	gateCalls []gateCall

	relayPaused map[uint32]bool
	queue       map[uint32]uint64 // each domain's release queue length
	processed   map[types.Hash]bool
	relayErr    map[uint32]error
	relayCalls  int
}

type gateCall struct {
	domain uint32
	pillar string
}

const harnessDepth = 2

var harnessExecutor = types.Address{0x42}

func newSubmitHarness(t *testing.T, domains ...uint32) *submitHarness {
	t.Helper()
	h := &submitHarness{
		t:        t,
		out:      captureLog(t),
		clock:    time.Unix(1000, 0),
		node:     newSendNodeFake(),
		tips:     map[uint32]settlement.Tip{},
		tipErr:   map[uint32]error{},
		tipCalls: map[uint32]int{},
		entries:  map[entryKey]*definition.BatchEntry{},
		gateBy:   map[uint32]settlement.Gates{},
		gateErr:  map[uint32]error{},

		relayPaused: map[uint32]bool{},
		queue:       map[uint32]uint64{},
		processed:   map[types.Hash]bool{},
		relayErr:    map[uint32]error{},
		gate: settlement.Gates{
			Height: 100,
			Member: &definition.ExecutorSetMember{
				PillarName: "pillar-a", ExecutorAddress: harnessExecutor, Status: definition.ExecutorStatusActive,
			},
			PillarActive: true,
		},
	}
	var ws []WorkerConfig
	for _, d := range domains {
		ws = append(ws, workerConfig(d, "/bin/true", 1<<30, 1))
		h.setTip(d, 0)
	}
	cfg := supervisorConfig(t.TempDir(), ws...)
	cfg.ExecutorAddress = harnessExecutor
	cfg.ConfirmationDepth = harnessDepth
	cfg.PillarName = "pillar-a"
	cfg.ZnndEndpoint = "http://user:s3cret@znnd.test:35997/rpc"
	src := Sources{
		Anchors: fakeAnchors{
			tip: func(d uint32) (settlement.Tip, error) {
				h.tipCalls[d]++
				if h.onTip != nil {
					h.onTip(d)
				}
				if err := h.tipErr[d]; err != nil {
					return settlement.Tip{}, err
				}
				return h.tips[d], nil
			},
			entry: func(d uint32, id uint64) (*definition.BatchEntry, error) {
				if e := h.entries[entryKey{d, id}]; e != nil {
					return e, nil
				}
				return nil, errors.New("fake: no such entry")
			},
			gates: func(d uint32, pillar string) (settlement.Gates, error) {
				h.gateCalls = append(h.gateCalls, gateCall{d, pillar})
				if err := h.gateErr[d]; err != nil {
					return settlement.Gates{}, err
				}
				if g, ok := h.gateBy[d]; ok {
					return g, nil
				}
				return h.gate, nil
			},
			relay: func(d uint32, q *settlement.RelayQuery) (settlement.RelayState, error) {
				h.relayCalls++
				if err := h.relayErr[d]; err != nil {
					return settlement.RelayState{}, err
				}
				rs := settlement.RelayState{Height: h.tips[d].Height, Paused: h.relayPaused[d], QueueLen: h.queue[d]}
				if q == nil {
					return rs, nil
				}
				if q.BatchID < h.tips[d].NextBatchID {
					rs.Entry = h.entries[entryKey{d, q.BatchID}]
				}
				for _, id := range q.IDs {
					if !h.processed[id] {
						break
					}
					rs.Processed++
				}
				return rs, nil
			},
		},
		Node: h.node,
	}
	h.sup = New(cfg, testKeyPair(t), src, Policy{BackoffMin: 10 * time.Second, BackoffMax: 80 * time.Second})
	h.sub = h.sup.sub
	for _, d := range domains {
		h.st(d).withdrawals = testStore(t, d)
	}
	h.sub.warnEvery = 0
	h.sub.now = func() time.Time { return h.clock }
	return h
}

func (h *submitHarness) slot(d uint32) *slot {
	for _, sl := range h.sup.slots {
		if sl.w.DomainID == d {
			return sl
		}
	}
	h.t.Fatalf("no slot for domain %d", d)
	return nil
}

func (h *submitHarness) st(d uint32) *workerState { return h.slot(d).st }

// setTip sets domain d's Settlement tip to nextBatchId n, whose previous entry has postStateRoot
// {1} (the root every held commitment starts from).
func (h *submitHarness) setTip(d uint32, n uint64) {
	t := settlement.Tip{Height: 100, NextBatchID: n, FirstInput: 1, HaveInput: true}
	if n > 0 {
		t.Prev = &definition.BatchEntry{PostStateRoot: types.Hash{0x01}}
	}
	h.tips[d] = t
}

func (h *submitHarness) setTipHeight(d uint32, height uint64) {
	t := h.tips[d]
	t.Height = height
	h.tips[d] = t
}

// commitment builds domain d's commitment for batch id, chained from postStateRoot {1}.
func (h *submitHarness) commitment(d uint32, id, last uint64) codec.BatchCommitment {
	c := commitmentFor(d, harnessExecutor, id, last)
	c.PreStateRoot = types.Hash{0x01}
	if id == 0 {
		c.PreStateRoot = types.ZeroHash
	}
	c.PostStateRoot = types.Hash{0x01}
	return c
}

// hold puts domain d's commitment for batch id into its held set and returns it.
func (h *submitHarness) hold(d uint32, id uint64) codec.BatchCommitment {
	c := h.commitment(d, id, 10*(id+1))
	h.holdCommitment(c)
	return c
}

func (h *submitHarness) holdCommitment(c codec.BatchCommitment) {
	st := h.st(c.DomainID)
	st.mu.Lock()
	defer st.mu.Unlock()
	if st.commitments == nil {
		st.commitments = map[uint64]codec.BatchCommitment{}
	}
	st.commitments[c.BatchID] = c
}

func (h *submitHarness) held(d uint32, id uint64) bool {
	st := h.st(d)
	st.mu.Lock()
	defer st.mu.Unlock()
	_, ok := st.commitments[id]
	return ok
}

func (h *submitHarness) setMark(d uint32, m uint64) {
	st := h.st(d)
	st.mu.Lock()
	st.chainNext = m
	st.mu.Unlock()
}

func (h *submitHarness) mark(d uint32) uint64 {
	st := h.st(d)
	st.mu.Lock()
	defer st.mu.Unlock()
	return st.chainNext
}

func (h *submitHarness) fault(d uint32) string {
	st := h.st(d)
	st.mu.Lock()
	defer st.mu.Unlock()
	return st.fault
}

func (h *submitHarness) unresolved(d uint32) *unresolvedSend {
	st := h.st(d)
	st.mu.Lock()
	defer st.mu.Unlock()
	if st.unresolved == nil {
		return nil
	}
	u := *st.unresolved
	return &u
}

func (h *submitHarness) step() { h.sub.step(context.Background()) }

func (h *submitHarness) advance(d time.Duration) { h.clock = h.clock.Add(d) }

func (h *submitHarness) last() (SendRecord, bool) { return h.sub.log.Last() }

func (h *submitHarness) lastBlock() *nom.AccountBlock {
	h.t.Helper()
	if len(h.node.published) == 0 {
		h.t.Fatal("nothing was published")
	}
	return h.node.published[len(h.node.published)-1]
}

func (h *submitHarness) wantPublished(n int) {
	h.t.Helper()
	if got := len(h.node.published); got != n {
		h.t.Fatalf("published %d blocks, want %d", got, n)
	}
}

// receive makes the node report b received at height 90, and Settlement's tip for b's domain
// read nextBatchId next. prev, when set, is the tip's previous entry.
func (h *submitHarness) receive(d uint32, b *nom.AccountBlock, next uint64, prev *definition.BatchEntry) {
	h.node.statuses[b.Hash] = znnd.SendStatus{Block: b, Received: true, ReceiveHeight: 90}
	t := h.tips[d]
	t.NextBatchID = next
	t.Prev = prev
	h.tips[d] = t
}

// accept makes Settlement accept c through the send b: the node shows b received and the tip
// moves to c's next batch, whose previous entry matches c.
func (h *submitHarness) accept(c codec.BatchCommitment, b *nom.AccountBlock) {
	h.receive(c.DomainID, b, c.BatchID+1, entryFor(h.t, c))
}

// reject makes Settlement reject b: the node shows it received while the tip stays at id.
func (h *submitHarness) reject(d uint32, b *nom.AccountBlock, id uint64) {
	h.receive(d, b, id, h.tips[d].Prev)
}

func (h *submitHarness) logHas(s string) bool { return strings.Contains(h.out.String(), s) }

func TestSubmitGuardPublishes(t *testing.T) {
	h := newSubmitHarness(t, 1, 2)
	c := h.hold(1, 0)

	var seen SendRecord
	var seenOK bool
	var seenUnresolved *unresolvedSend
	h.node.publish = func(b *nom.AccountBlock) error {
		seen, seenOK = h.last()
		seenUnresolved = h.unresolved(1)
		return nil
	}
	h.step()

	h.wantPublished(1)
	b := h.lastBlock()
	if want := packSubmit(t, c); !bytes.Equal(b.Data, want) {
		t.Fatalf("Data = %x, want the packed SubmitBatch of the commitment %x", b.Data, want)
	}
	if !seenOK || seen.Outcome != "" || seen.Hash != b.Hash || seen.DataHash != types.NewHash(b.Data) {
		t.Fatalf("record at publish = %+v, %v; want an unresolved record of the block with its data hash", seen, seenOK)
	}
	if seen.DomainID != 1 || seen.BatchID != 0 || seen.Height != b.Height || seen.Method != definition.SettlementSubmitBatchMethodName {
		t.Fatalf("record at publish = %+v, want domain 1 batch 0 at height %d", seen, b.Height)
	}
	if seenUnresolved == nil || seenUnresolved.batchID != 0 {
		t.Fatalf("unresolved at publish = %+v, want batch 0", seenUnresolved)
	}
	if !h.logHas("domain 1 batch 0: sent") {
		t.Fatalf("log = %q, want a sent line", h.out.String())
	}
	if strings.Contains(h.out.String(), "s3cret") {
		t.Fatalf("log = %q, must not contain the endpoint credentials", h.out.String())
	}
}

func TestSubmitBehindPublishesNothing(t *testing.T) {
	t.Run("nothing held", func(t *testing.T) {
		h := newSubmitHarness(t, 1, 2)
		h.step()
		h.wantPublished(0)
		if _, ok := h.last(); ok {
			t.Fatal("a send was recorded")
		}
		if h.fault(1) != "" || h.fault(2) != "" {
			t.Fatal("a domain halted")
		}
	})
	t.Run("next input not yet in Settlement", func(t *testing.T) {
		h := newSubmitHarness(t, 1)
		h.hold(1, 0)
		tip := h.tips[1]
		tip.HaveInput = false
		h.tips[1] = tip
		h.step()
		h.wantPublished(0)
		if h.fault(1) != "" {
			t.Fatal("the domain halted")
		}
	})
}

func TestSubmitContradictionHaltsOnlyThatDomain(t *testing.T) {
	cases := []struct {
		name   string
		break_ func(h *submitHarness)
	}{
		{"first input", func(h *submitHarness) {
			tip := h.tips[1]
			tip.FirstInput = 5
			h.tips[1] = tip
		}},
		{"preStateRoot", func(h *submitHarness) {
			h.setTip(1, 1)
			h.tips[1].Prev.PostStateRoot = types.Hash{0x99}
		}},
		{"preStateRoot at batch 0", func(h *submitHarness) {
			c := h.commitment(1, 0, 10)
			c.PreStateRoot = types.Hash{0x07}
			h.holdCommitment(c)
		}},
		{"settlement read halted", func(h *submitHarness) {
			h.tipErr[1] = fmt.Errorf("%w: bad", recovery.ErrHalted)
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := newSubmitHarness(t, 1, 2)
			h.hold(1, 0)
			if tc.name == "preStateRoot" {
				h.hold(1, 1)
				h.setMark(1, 1)
			}
			h.hold(2, 0)
			tc.break_(h)

			h.step()

			if f := h.fault(1); !strings.Contains(f, ErrInconsistent.Error()) {
				t.Fatalf("domain 1 fault = %q, want a consistency fault", f)
			}
			if h.fault(2) != "" {
				t.Fatalf("domain 2 fault = %q, want none", h.fault(2))
			}
			h.wantPublished(1)
			if r, _ := h.last(); r.DomainID != 2 {
				t.Fatalf("send for domain %d, want domain 2", r.DomainID)
			}
			select {
			case <-h.st(1).halt:
			default:
				t.Fatal("domain 1's halt channel is open")
			}
			if !h.logHas("domain 1 ") {
				t.Fatalf("log = %q, want the halt logged", h.out.String())
			}
		})
	}

	t.Run("a passing tip after a halt publishes nothing for that domain", func(t *testing.T) {
		h := newSubmitHarness(t, 1, 2)
		h.hold(1, 0)
		c2 := h.hold(2, 0)
		tip := h.tips[1]
		tip.FirstInput = 5
		h.tips[1] = tip
		h.step()
		h.wantPublished(1)

		h.setTip(1, 0)
		h.accept(c2, h.lastBlock())
		h.step() // accepted
		h.step()
		h.wantPublished(1)
	})
}

func TestSubmitSkipsHaltedAndConfigErrorSlots(t *testing.T) {
	for _, state := range []State{StateHalted, StateConfigError} {
		t.Run(string(state), func(t *testing.T) {
			h := newSubmitHarness(t, 1)
			h.hold(1, 0)
			sl := h.slot(1)
			h.sup.mu.Lock()
			sl.state = state
			h.sup.mu.Unlock()
			h.step()
			h.wantPublished(0)
			if h.tipCalls[1] != 0 {
				t.Fatalf("Tip read %d times for a %s slot, want 0", h.tipCalls[1], state)
			}
		})
	}
	t.Run("recorded fault", func(t *testing.T) {
		h := newSubmitHarness(t, 1)
		h.hold(1, 0)
		haltDomain(h.st(1), fmt.Errorf("%w: earlier", ErrInconsistent))
		h.step()
		h.wantPublished(0)
	})
}

func TestSubmitOneSendAtATime(t *testing.T) {
	h := newSubmitHarness(t, 1, 2)
	c1 := h.hold(1, 0)
	h.hold(2, 0)

	h.step()
	h.wantPublished(1)
	first := h.lastBlock()
	if r, _ := h.last(); r.DomainID != 1 {
		t.Fatalf("first send for domain %d, want 1", r.DomainID)
	}

	h.step()
	h.step()
	h.wantPublished(1)
	if h.node.statusCount(first.Hash) == 0 {
		t.Fatal("later steps did not check the pending send")
	}

	h.accept(c1, first)
	h.step() // resolves
	if r, _ := h.last(); r.Outcome != OutcomeAccepted {
		t.Fatalf("outcome = %q, want accepted", r.Outcome)
	}
	h.wantPublished(1)

	h.step()
	h.wantPublished(2)
	if r, _ := h.last(); r.DomainID != 2 {
		t.Fatalf("second send for domain %d, want 2", r.DomainID)
	}
}

func TestSubmitMark(t *testing.T) {
	h := newSubmitHarness(t, 1)
	c0 := h.hold(1, 0)
	h.step()
	h.accept(c0, h.lastBlock())
	h.step()

	if got := h.mark(1); got != 1 {
		t.Fatalf("mark = %d, want 1", got)
	}
	if h.held(1, 0) {
		t.Fatal("batch 0 is still held after its acceptance")
	}

	// A lagging tip still names the accepted batch.
	h.holdCommitment(c0)
	h.setTip(1, 0)
	h.step()
	h.wantPublished(1)

	// A tip at the mark sends the batch held there.
	h.setTip(1, 1)
	c1 := h.hold(1, 1)
	h.step()
	h.wantPublished(2)
	if want := packSubmit(t, c1); !bytes.Equal(h.lastBlock().Data, want) {
		t.Fatal("the send at the mark does not carry batch 1")
	}
}

func TestSubmitBackoff(t *testing.T) {
	t.Run("rejection doubles and acceptance clears", func(t *testing.T) {
		h := newSubmitHarness(t, 1)
		c := h.hold(1, 0)

		h.step()
		h.reject(1, h.lastBlock(), 0)
		h.step()
		if r, _ := h.last(); r.Outcome != OutcomeRejected {
			t.Fatalf("outcome = %q, want rejected", r.Outcome)
		}
		rs := h.sub.retry[1]
		if rs == nil || rs.batchID != 0 || rs.delay != 10*time.Second || !rs.until.Equal(h.clock.Add(10*time.Second)) {
			t.Fatalf("retry = %+v, want batch 0 at 10s", rs)
		}

		h.step()
		h.advance(9 * time.Second)
		h.step()
		h.wantPublished(1)

		h.advance(time.Second)
		h.step()
		h.wantPublished(2)
		h.reject(1, h.lastBlock(), 0)
		h.step()
		if rs := h.sub.retry[1]; rs == nil || rs.delay != 20*time.Second {
			t.Fatalf("retry after a second rejection = %+v, want 20s", rs)
		}

		h.advance(20 * time.Second)
		h.step()
		h.wantPublished(3)
		h.accept(c, h.lastBlock())
		h.step()
		if h.sub.retry[1] != nil {
			t.Fatal("the acceptance left a retry entry")
		}
	})

	t.Run("an older batch's backoff does not delay the next batch", func(t *testing.T) {
		h := newSubmitHarness(t, 1)
		h.hold(1, 0)
		h.sub.retry[1] = &retryState{batchID: 0, delay: time.Minute, until: h.clock.Add(time.Hour)}
		h.setTip(1, 1)
		h.hold(1, 1)
		h.setMark(1, 1)
		h.step()
		h.wantPublished(1)
		if h.sub.retry[1] != nil {
			t.Fatal("the stale retry entry was kept")
		}
	})

	t.Run("a failing publish ends dropped and backs off", func(t *testing.T) {
		h := newSubmitHarness(t, 1)
		h.hold(1, 0)
		h.node.publish = func(*nom.AccountBlock) error { return errors.New("node refused") }

		h.step() // attempt 1
		h.wantPublished(1)
		h.step() // checks: dropped
		if r, _ := h.last(); r.Outcome != OutcomeDropped {
			t.Fatalf("outcome = %q, want dropped", r.Outcome)
		}
		if rs := h.sub.retry[1]; rs == nil || rs.delay != 10*time.Second || rs.dropped != 1 {
			t.Fatalf("retry = %+v, want 10s with dropped height 1", rs)
		}
		plasma := h.node.plasmaCalls

		h.advance(5 * time.Second)
		h.step()
		if h.node.plasmaCalls != plasma {
			t.Fatal("RequiredPlasma was called before the backoff ended")
		}

		h.advance(5 * time.Second)
		h.step() // attempt 2
		h.wantPublished(2)
		h.step()
		if rs := h.sub.retry[1]; rs == nil || rs.delay != 20*time.Second {
			t.Fatalf("retry after a second drop = %+v, want 20s", rs)
		}
		if got := strings.Count(h.out.String(), "publishing "); got != 2 {
			t.Fatalf("%d publish-failure lines, want 2 (one per attempt)", got)
		}
	})
}

func TestSubmitAdoption(t *testing.T) {
	// drop publishes batch 0 with a node that refuses it, and resolves the send as dropped.
	drop := func(t *testing.T) (*submitHarness, codec.BatchCommitment, *nom.AccountBlock) {
		h := newSubmitHarness(t, 1)
		c := h.hold(1, 0)
		h.node.publish = func(*nom.AccountBlock) error { return errors.New("node refused") }
		h.step()
		h.step()
		h.node.publish = nil
		if r, _ := h.last(); r.Outcome != OutcomeDropped {
			t.Fatalf("outcome = %q, want dropped", r.Outcome)
		}
		return h, c, h.node.published[0]
	}

	t.Run("a copy that appears with the same data is followed", func(t *testing.T) {
		h, _, dropped := drop(t)
		h.node.put(dropped)
		h.advance(10 * time.Second)
		h.step()
		h.wantPublished(1)
		r, _ := h.last()
		if r.Hash != dropped.Hash || r.Outcome != "" {
			t.Fatalf("record = %+v, want the old send followed", r)
		}
		if !h.logHas("following send") {
			t.Fatalf("log = %q, want a following line", h.out.String())
		}
	})

	t.Run("other data at the height builds", func(t *testing.T) {
		h, _, dropped := drop(t)
		other := *dropped
		other.Data = []byte{0xde, 0xad}
		other.Hash = types.Hash{0x55}
		h.node.put(&other)
		h.advance(10 * time.Second)
		h.step()
		h.wantPublished(2)
		if h.lastBlock().Height != 2 {
			t.Fatalf("height = %d, want 2", h.lastBlock().Height)
		}
	})

	t.Run("a frontier that moves between the reads publishes nothing", func(t *testing.T) {
		h, _, dropped := drop(t)
		other := *dropped
		other.Data = []byte{0xde, 0xad}
		other.Hash = types.Hash{0x55}
		h.node.put(&other)
		moved := other
		moved.Height = 2
		moved.Hash = types.Hash{0x56}
		h.node.blocks[2] = &moved
		h.node.frontierCalls = 0
		h.node.frontierFn = func(call int) *nom.AccountBlock {
			if call == 1 {
				return &other
			}
			return &moved
		}
		h.advance(10 * time.Second)
		h.step()
		h.wantPublished(1)
		if h.unresolved(1) != nil {
			t.Fatal("a send was marked in flight")
		}
	})

	t.Run("a rejected rebuild is not adopted", func(t *testing.T) {
		h, _, _ := drop(t)
		h.advance(10 * time.Second)
		h.step() // rebuilds at the dropped height
		h.wantPublished(2)
		rebuilt := h.lastBlock()
		if rebuilt.Height != 1 {
			t.Fatalf("rebuilt height = %d, want 1", rebuilt.Height)
		}
		h.reject(1, rebuilt, 0)
		h.step()
		if r, _ := h.last(); r.Outcome != OutcomeRejected {
			t.Fatalf("outcome = %q, want rejected", r.Outcome)
		}
		h.advance(20 * time.Second)
		h.step()
		h.wantPublished(3)
		if h.lastBlock().Hash == rebuilt.Hash {
			t.Fatal("the rejected block was published again")
		}
		if r, _ := h.last(); r.Hash == rebuilt.Hash {
			t.Fatal("the rejected block was adopted")
		}
	})
}

func TestSubmitPlasma(t *testing.T) {
	h := newSubmitHarness(t, 1)
	h.hold(1, 0)
	h.node.req.AvailablePlasma = 0

	h.step()
	h.wantPublished(0)
	if st := h.sup.SubmitterStatus(); !strings.Contains(st.Error, "insufficient plasma") {
		t.Fatalf("Error = %q, want insufficient plasma", st.Error)
	}
	if h.fault(1) != "" {
		t.Fatal("insufficient plasma halted the domain")
	}

	h.node.req.AvailablePlasma = 100000
	h.step()
	h.wantPublished(0)
	h.advance(10 * time.Second)
	h.step()
	h.wantPublished(1)
	if st := h.sup.SubmitterStatus(); st.Error != "" {
		t.Fatalf("Error = %q, want it cleared", st.Error)
	}
}

func TestSubmitPlasmaFailureDoesNotStarveOtherDomains(t *testing.T) {
	h := newSubmitHarness(t, 1, 2)
	h.hold(1, 0)
	c2 := h.hold(2, 0)
	// Only the first build (domain 1's) lacks plasma.
	first := true
	h.node.onPlasma = func() {
		h.node.req.AvailablePlasma = 100000
		if first {
			first = false
			h.node.req.AvailablePlasma = 0
		}
	}

	h.step()
	h.wantPublished(1)
	if !bytes.Equal(h.lastBlock().Data, packSubmit(t, c2)) {
		t.Fatal("the block published is not domain 2's")
	}
	if !h.logHas("WARN: submitter: domain 1 batch 0: ") || !h.logHas("insufficient plasma") {
		t.Fatalf("the plasma shortfall log does not name domain 1 batch 0:\n%s", h.out.String())
	}
	if rs := h.sub.retry[1]; rs == nil || rs.batchID != 0 || rs.delay != 10*time.Second {
		t.Fatalf("retry = %+v, want domain 1's batch 0 at 10s", rs)
	}
	if h.fault(1) != "" {
		t.Fatal("insufficient plasma halted the domain")
	}
}

func TestSubmitLogsRedactTheEndpoint(t *testing.T) {
	const secret = "abc123"
	check := func(t *testing.T, h *submitHarness) {
		t.Helper()
		for _, bad := range []string{secret, "user", "/v1/key"} {
			if h.logHas(bad) {
				t.Fatalf("the log carries %q:\n%s", bad, h.out.String())
			}
		}
		if !h.logHas("http://znnd.test:35997") {
			t.Fatalf("the log has no redacted endpoint:\n%s", h.out.String())
		}
	}
	// The error the HTTP client returns: the URL with its password masked.
	urlErr := &url.Error{Op: "Post", URL: "http://user@znnd.test:35997/v1/key?token=" + secret, Err: errors.New("connection refused")}
	setup := func(t *testing.T) *submitHarness {
		h := newSubmitHarness(t, 1)
		h.sup.cfg.ZnndEndpoint = "http://user:pw@znnd.test:35997/v1/key?token=" + secret
		return h
	}

	t.Run("outage", func(t *testing.T) {
		h := setup(t)
		h.tipErr[1] = urlErr
		h.step()
		if !h.logHas("connection refused") {
			t.Fatalf("the outage was not logged:\n%s", h.out.String())
		}
		check(t, h)
	})

	t.Run("publish failure", func(t *testing.T) {
		h := setup(t)
		h.hold(1, 0)
		h.node.publish = func(*nom.AccountBlock) error { return urlErr }
		h.step()
		if !h.logHas("failed") {
			t.Fatalf("the publish failure was not logged:\n%s", h.out.String())
		}
		check(t, h)
	})
}

func TestSubmitHaltDuringBuildPublishesNothing(t *testing.T) {
	h := newSubmitHarness(t, 1)
	h.hold(1, 0)
	h.node.onPlasma = func() {
		haltDomain(h.st(1), fmt.Errorf("%w: domain 1 halted mid-guard", ErrInconsistent))
	}
	h.step()
	h.wantPublished(0)
	if _, ok := h.last(); ok {
		t.Fatal("a send was recorded")
	}
	if h.unresolved(1) != nil {
		t.Fatal("a send was marked in flight")
	}
}

func TestSubmitSlotStateChangeDuringBuildPublishesNothing(t *testing.T) {
	for _, state := range []State{StateHalted, StateConfigError} {
		t.Run(string(state), func(t *testing.T) {
			h := newSubmitHarness(t, 1)
			h.hold(1, 0)
			h.node.onPlasma = func() {
				h.sup.mu.Lock()
				h.slot(1).state = state
				h.sup.mu.Unlock()
			}
			h.step()
			h.wantPublished(0)
			if _, ok := h.last(); ok {
				t.Fatal("a send was recorded")
			}
			if h.unresolved(1) != nil {
				t.Fatal("a send was marked in flight")
			}
		})
	}
}

func TestSubmitUnconfirmedAlert(t *testing.T) {
	h := newSubmitHarness(t, 1)
	c := h.hold(1, 0)
	h.step()
	b := h.lastBlock()
	ack := b.MomentumAcknowledged.Height
	threshold := alertDepths * uint64(harnessDepth)

	h.node.momentum.Height = ack + threshold - 1
	h.step()
	h.step()
	if h.logHas("WARN: send") || h.sup.SubmitterStatus().Error != "" {
		t.Fatalf("below the threshold: log %q, error %q; want neither", h.out.String(), h.sup.SubmitterStatus().Error)
	}

	h.node.momentum.Height = ack + threshold
	h.step()
	if got := strings.Count(h.out.String(), "WARN: send"); got != 1 {
		t.Fatalf("%d alert lines at the threshold, want 1", got)
	}
	if e := h.sup.SubmitterStatus().Error; !strings.Contains(e, "not replaced automatically") {
		t.Fatalf("Error = %q, want the unconfirmed text", e)
	}
	h.step()
	h.step()
	if got := strings.Count(h.out.String(), "WARN: send"); got != 1 {
		t.Fatalf("%d alert lines after more checks, want 1", got)
	}

	h.accept(c, b)
	h.step()
	if e := h.sup.SubmitterStatus().Error; e != "" {
		t.Fatalf("Error = %q after the outcome, want it cleared", e)
	}
}

func TestSubmitStall(t *testing.T) {
	h := newSubmitHarness(t, 1)
	h.hold(1, 3)
	h.setMark(1, 5)
	h.setTip(1, 3)
	threshold := alertDepths * uint64(harnessDepth)

	h.step()
	h.setTipHeight(1, 100+threshold-1)
	h.step()
	if got := h.sup.Status()[0].SubmitError; got != "" {
		t.Fatalf("SubmitError = %q below the threshold, want none", got)
	}

	h.setTipHeight(1, 100+threshold)
	h.step()
	got := h.sup.Status()[0].SubmitError
	if !strings.Contains(got, "restart the executor") || !strings.Contains(got, "move the submission log files aside") {
		t.Fatalf("SubmitError = %q, want the restart remedy and the submission log remedy", got)
	}
	if !h.logHas("domain 1 ERROR:") {
		t.Fatalf("log = %q, want an error line", h.out.String())
	}
	h.wantPublished(0)

	h.setTip(1, 5)
	h.step()
	if got := h.sup.Status()[0].SubmitError; got != "" {
		t.Fatalf("SubmitError = %q once the cursor reaches the mark, want none", got)
	}
	if _, ok := h.sub.below[1]; ok {
		t.Fatal("the stall start was kept")
	}
}

func TestSubmitRetractRule(t *testing.T) {
	t.Run("a retract during publish then acceptance halts", func(t *testing.T) {
		h := newSubmitHarness(t, 1)
		c := h.hold(1, 0)
		h.node.publish = func(*nom.AccountBlock) error {
			retract(0, h.st(1))
			return nil
		}
		h.step()
		if u := h.unresolved(1); u == nil || !u.retracted {
			t.Fatalf("unresolved = %+v, want retracted", u)
		}
		h.accept(c, h.lastBlock())
		h.step()
		if f := h.fault(1); !strings.Contains(f, "retracted") {
			t.Fatalf("fault = %q, want a retracted fault", f)
		}
		if h.unresolved(1) != nil {
			t.Fatal("unresolved was kept")
		}
		if h.mark(1) != 1 {
			t.Fatalf("mark = %d, want 1", h.mark(1))
		}
	})

	t.Run("a retract then rejection is superseded and the re-announce is sent", func(t *testing.T) {
		h := newSubmitHarness(t, 1)
		h.hold(1, 0)
		first := true
		h.node.publish = func(*nom.AccountBlock) error {
			if first {
				first = false
				retract(0, h.st(1))
			}
			return nil
		}
		h.step()
		h.reject(1, h.lastBlock(), 0)
		h.step()
		if r, _ := h.last(); r.Outcome != OutcomeSuperseded {
			t.Fatalf("outcome = %q, want superseded", r.Outcome)
		}
		if h.fault(1) != "" {
			t.Fatal("a retracted rejection halted the domain")
		}

		c2 := h.commitment(1, 0, 99)
		h.holdCommitment(c2)
		h.step()
		h.wantPublished(1)
		if rs := h.sub.retry[1]; rs == nil || rs.batchID != 0 || rs.delay != 10*time.Second {
			t.Fatalf("retry = %+v, want batch 0 at 10s", rs)
		}
		h.advance(10 * time.Second)
		h.step()
		h.wantPublished(2)
		if want := packSubmit(t, c2); !bytes.Equal(h.lastBlock().Data, want) {
			t.Fatal("the next send does not carry the re-announced commitment")
		}
	})

	t.Run("a retract from above the batch leaves the flag clear", func(t *testing.T) {
		h := newSubmitHarness(t, 1)
		h.hold(1, 3)
		h.setMark(1, 3)
		h.setTip(1, 3)
		h.node.publish = func(*nom.AccountBlock) error {
			retract(4, h.st(1))
			return nil
		}
		h.step()
		if u := h.unresolved(1); u == nil || u.retracted {
			t.Fatalf("unresolved = %+v, want set and not retracted", u)
		}
	})

	t.Run("a held entry changed while the plasma is read publishes nothing", func(t *testing.T) {
		h := newSubmitHarness(t, 1)
		h.hold(1, 0)
		h.node.onPlasma = func() { h.holdCommitment(h.commitment(1, 0, 77)) }
		h.step()
		h.wantPublished(0)
		if _, ok := h.last(); ok {
			t.Fatal("a send was recorded")
		}
		if h.unresolved(1) != nil {
			t.Fatal("a send was marked in flight")
		}
	})
}

func TestSubmitAbandoned(t *testing.T) {
	h := newSubmitHarness(t, 1, 2)
	h.hold(1, 0)
	h.hold(2, 0)
	h.step()
	b := h.lastBlock()
	h.node.statuses[b.Hash] = znnd.SendStatus{Block: b, Received: true, ReceiveHeight: 90}
	h.tipErr[1] = fmt.Errorf("%w: bad", recovery.ErrHalted)

	h.step()
	if h.fault(1) == "" {
		t.Fatal("domain 1 did not halt")
	}
	if h.unresolved(1) != nil {
		t.Fatal("unresolved was kept")
	}
	r, _ := h.last()
	if r.Outcome != OutcomeAbandoned || r.Hash != b.Hash {
		t.Fatalf("Last = %+v, want the send abandoned", r)
	}
	if st := h.sup.SubmitterStatus(); st.Last == nil || st.Last.Outcome != OutcomeAbandoned || st.Last.Hash != b.Hash {
		t.Fatalf("SubmitterStatus.Last = %+v, want the send abandoned", st.Last)
	}
	checks := h.node.statusCount(b.Hash)

	h.step()
	h.wantPublished(2)
	if r, _ := h.last(); r.DomainID != 2 {
		t.Fatalf("next send for domain %d, want 2", r.DomainID)
	}
	h.step()
	h.step()
	if got := h.node.statusCount(b.Hash); got != checks {
		t.Fatalf("SendStatus called %d times for the abandoned send, want %d", got, checks)
	}
}

func TestSubmitShutdown(t *testing.T) {
	t.Run("no further slot is visited", func(t *testing.T) {
		h := newSubmitHarness(t, 1, 2, 3)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		for _, d := range []uint32{1, 2, 3} {
			h.hold(d, 0)
			h.tipErr[d] = errors.New("node down")
		}
		h.onTip = func(uint32) { cancel() }
		h.sub.step(ctx)
		total := 0
		for _, n := range h.tipCalls {
			total += n
		}
		if total != 1 {
			t.Fatalf("%d Tip calls after the context ended, want 1", total)
		}
	})

	t.Run("nothing is recorded or published", func(t *testing.T) {
		h := newSubmitHarness(t, 1, 2)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		h.hold(1, 0)
		h.hold(2, 0)
		h.onTip = func(uint32) { cancel() }
		h.sub.step(ctx)
		h.wantPublished(0)
		if _, ok := h.last(); ok {
			t.Fatal("a send was recorded")
		}
		if h.unresolved(1) != nil || h.unresolved(2) != nil {
			t.Fatal("a send was marked in flight")
		}
	})
}

// --- resolveSend ---

type resolveFixture struct {
	t        *testing.T
	node     *sendNodeFake
	tip      settlement.Tip
	tipErr   error
	entries  map[uint64]*definition.BatchEntry
	relay    settlement.RelayState
	relayErr error
	c        codec.BatchCommitment
	data     []byte
	r        SendRecord
	blk      *nom.AccountBlock // the send as the node would store it
}

// newResolveFixture builds a record for domain 1 batch 3 sent at account height 5.
func newResolveFixture(t *testing.T) *resolveFixture {
	f := &resolveFixture{t: t, node: newSendNodeFake(), entries: map[uint64]*definition.BatchEntry{}}
	f.c = commitmentFor(1, harnessExecutor, 3, 40)
	f.data = packSubmit(t, f.c)
	f.blk = &nom.AccountBlock{
		Hash:                 types.Hash{0xb1},
		Height:               5,
		Address:              harnessExecutor,
		ToAddress:            types.SettlementContract,
		Data:                 f.data,
		MomentumAcknowledged: types.HashHeight{Height: 30},
	}
	f.r = SendRecord{
		DomainID: 1, Method: definition.SettlementSubmitBatchMethodName, BatchID: 3,
		Height: 5, Hash: f.blk.Hash, DataHash: types.NewHash(f.data),
	}
	f.tip = settlement.Tip{Height: 100, NextBatchID: 4, Prev: entryFor(t, f.c), FirstInput: 1, HaveInput: true}
	f.node.momentum.Height = 50
	return f
}

func (f *resolveFixture) resolve() (sendCheck, error) {
	anchors := fakeAnchors{
		tip: func(uint32) (settlement.Tip, error) { return f.tip, f.tipErr },
		entry: func(_ uint32, id uint64) (*definition.BatchEntry, error) {
			if e := f.entries[id]; e != nil {
				return e, nil
			}
			return nil, errors.New("fake: no such entry")
		},
		relay: func(uint32, *settlement.RelayQuery) (settlement.RelayState, error) { return f.relay, f.relayErr },
	}
	return resolveSend(f.node, anchors, harnessExecutor, f.r)
}

// received makes the node report the send received at height 90.
func (f *resolveFixture) received() {
	f.node.statuses[f.r.Hash] = znnd.SendStatus{Block: f.blk, Received: true, ReceiveHeight: 90}
}

func (f *resolveFixture) mustResolve(want sendCheck) {
	f.t.Helper()
	got, err := f.resolve()
	if err != nil {
		f.t.Fatalf("resolveSend: %v", err)
	}
	if got.Fault != nil {
		f.t.Fatalf("resolveSend fault: %v", got.Fault)
	}
	if got != want {
		f.t.Fatalf("resolveSend = %+v, want %+v", got, want)
	}
}

func TestResolveSendOutcomes(t *testing.T) {
	t.Run("accepted through the previous entry", func(t *testing.T) {
		f := newResolveFixture(t)
		f.received()
		f.mustResolve(sendCheck{Outcome: OutcomeAccepted})
	})

	t.Run("accepted through the accepted entry", func(t *testing.T) {
		f := newResolveFixture(t)
		f.received()
		f.tip.NextBatchID = 6
		f.tip.Prev = &definition.BatchEntry{}
		f.entries[3] = entryFor(t, f.c)
		f.mustResolve(sendCheck{Outcome: OutcomeAccepted})
	})

	t.Run("a differing entry is rejected with a fault", func(t *testing.T) {
		f := newResolveFixture(t)
		f.received()
		e := entryFor(t, f.c)
		e.PostStateRoot = types.Hash{0x77}
		f.tip.Prev = e
		got, err := f.resolve()
		if err != nil || got.Outcome != OutcomeRejected || !errors.Is(got.Fault, ErrInconsistent) {
			t.Fatalf("resolveSend = %+v, %v; want rejected with a consistency fault", got, err)
		}
	})

	t.Run("nextBatchId equal to the batch is rejected", func(t *testing.T) {
		f := newResolveFixture(t)
		f.received()
		f.tip.NextBatchID = 3
		f.mustResolve(sendCheck{Outcome: OutcomeRejected})
	})

	t.Run("nextBatchId below the batch at the receive height is rejected", func(t *testing.T) {
		f := newResolveFixture(t)
		f.received()
		f.tip.NextBatchID = 2
		f.tip.Height = 90
		f.mustResolve(sendCheck{Outcome: OutcomeRejected})
	})

	t.Run("a view below the receive height is pending", func(t *testing.T) {
		f := newResolveFixture(t)
		f.received()
		f.tip.Height = 89
		f.tip.NextBatchID = 3
		f.mustResolve(sendCheck{})
		f.tip.Height = 90
		f.mustResolve(sendCheck{Outcome: OutcomeRejected})
	})

	t.Run("a halted tip is a fault with no outcome", func(t *testing.T) {
		f := newResolveFixture(t)
		f.received()
		f.tipErr = fmt.Errorf("%w: bad", recovery.ErrHalted)
		got, err := f.resolve()
		if err != nil || got.Outcome != "" || !errors.Is(got.Fault, ErrInconsistent) {
			t.Fatalf("resolveSend = %+v, %v; want a fault with no outcome", got, err)
		}
	})

	t.Run("a plain tip error is returned", func(t *testing.T) {
		f := newResolveFixture(t)
		f.received()
		f.tipErr = errors.New("node down")
		if got, err := f.resolve(); err == nil || got.Outcome != "" {
			t.Fatalf("resolveSend = %+v, %v; want an error", got, err)
		}
	})

	t.Run("data that differs from the record is an error", func(t *testing.T) {
		f := newResolveFixture(t)
		f.received()
		f.r.DataHash = types.Hash{0x09}
		if got, err := f.resolve(); err == nil || got.Outcome != "" || got.Fault != nil {
			t.Fatalf("resolveSend = %+v, %v; want an error and no outcome", got, err)
		}
	})
}

func TestResolveSendUnconfirmedAndDropped(t *testing.T) {
	t.Run("dropped with no frontier", func(t *testing.T) {
		f := newResolveFixture(t)
		f.mustResolve(sendCheck{Outcome: OutcomeDropped})
	})

	t.Run("dropped below the send's height", func(t *testing.T) {
		f := newResolveFixture(t)
		f.node.frontier = &nom.AccountBlock{Height: 4}
		f.mustResolve(sendCheck{Outcome: OutcomeDropped})
	})

	t.Run("in a momentum without a receive", func(t *testing.T) {
		f := newResolveFixture(t)
		f.node.statuses[f.r.Hash] = znnd.SendStatus{Block: f.blk}
		f.mustResolve(sendCheck{Unconfirmed: true, Age: 20})
	})

	t.Run("a lagging momentum has age 0", func(t *testing.T) {
		f := newResolveFixture(t)
		f.node.statuses[f.r.Hash] = znnd.SendStatus{Block: f.blk}
		f.node.momentum.Height = 20
		f.mustResolve(sendCheck{Unconfirmed: true})
	})

	t.Run("at the frontier", func(t *testing.T) {
		f := newResolveFixture(t)
		f.node.frontier = f.blk
		f.mustResolve(sendCheck{Unconfirmed: true, Age: 20})
	})

	t.Run("superseded at the frontier", func(t *testing.T) {
		f := newResolveFixture(t)
		other := *f.blk
		other.Hash = types.Hash{0xb2}
		other.Data = []byte{1}
		f.node.frontier = &other
		f.mustResolve(sendCheck{Outcome: OutcomeSuperseded, Other: other.Hash})
	})

	t.Run("above the frontier through the height read", func(t *testing.T) {
		f := newResolveFixture(t)
		f.node.frontier = &nom.AccountBlock{Height: 8}
		f.node.blocks[5] = f.blk
		f.mustResolve(sendCheck{Unconfirmed: true, Age: 20})

		other := *f.blk
		other.Hash = types.Hash{0xb2}
		other.Data = []byte{1}
		f.node.blocks[5] = &other
		f.mustResolve(sendCheck{Outcome: OutcomeSuperseded, Other: other.Hash})
	})

	t.Run("no block at a height at or below the frontier is an error", func(t *testing.T) {
		f := newResolveFixture(t)
		f.node.frontier = &nom.AccountBlock{Height: 8}
		if got, err := f.resolve(); err == nil || got.Outcome != "" {
			t.Fatalf("resolveSend = %+v, %v; want an error", got, err)
		}
	})

	t.Run("read errors are returned", func(t *testing.T) {
		f := newResolveFixture(t)
		boom := errors.New("node down")
		f.node.status = func(types.Hash) (znnd.SendStatus, error) { return znnd.SendStatus{}, boom }
		if _, err := f.resolve(); err != boom {
			t.Fatalf("err = %v, want the status error unchanged", err)
		}
	})
}

func TestResolveSendCopies(t *testing.T) {
	copyOf := func(f *resolveFixture) *nom.AccountBlock {
		c := *f.blk
		c.Hash = types.Hash{0xc1}
		c.Height = f.r.Height
		return &c
	}

	t.Run("unconfirmed copy", func(t *testing.T) {
		f := newResolveFixture(t)
		cp := copyOf(f)
		f.node.frontier = cp
		f.mustResolve(sendCheck{Unconfirmed: true, Age: 20})
		if f.node.statusCount(cp.Hash) != 1 {
			t.Fatal("the copy's own status was not read")
		}
	})

	t.Run("copy in a momentum without a receive", func(t *testing.T) {
		f := newResolveFixture(t)
		cp := copyOf(f)
		f.node.frontier = cp
		f.node.statuses[cp.Hash] = znnd.SendStatus{Block: cp}
		f.mustResolve(sendCheck{Unconfirmed: true, Age: 20})
	})

	t.Run("received copy", func(t *testing.T) {
		f := newResolveFixture(t)
		cp := copyOf(f)
		f.node.frontier = cp
		f.node.statuses[cp.Hash] = znnd.SendStatus{Block: cp, Received: true, ReceiveHeight: 90}
		f.mustResolve(sendCheck{Outcome: OutcomeAccepted})
	})

	t.Run("the same data to another address", func(t *testing.T) {
		f := newResolveFixture(t)
		cp := copyOf(f)
		cp.ToAddress = types.Address{0x09}
		f.node.frontier = cp
		f.mustResolve(sendCheck{Outcome: OutcomeSuperseded, Other: cp.Hash})
	})

	t.Run("a settlement send with other data", func(t *testing.T) {
		f := newResolveFixture(t)
		cp := copyOf(f)
		cp.Data = []byte{1, 2}
		f.node.frontier = cp
		f.mustResolve(sendCheck{Outcome: OutcomeSuperseded, Other: cp.Hash})
	})
}

// TestSubmitMarkRaiseHoldsTheStateLock runs acceptances while another goroutine reads and
// retracts under the state lock, so the race detector sees a raise made outside that lock.
func TestSubmitMarkRaiseHoldsTheStateLock(t *testing.T) {
	h := newSubmitHarness(t, 1)
	st := h.st(1)

	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			st.mu.Lock()
			_ = st.chainNext
			_ = len(st.commitments)
			st.mu.Unlock()
			retract(1000, st)
		}
	}()

	for id := uint64(0); id < 200; id++ {
		h.setTip(1, id)
		c := h.hold(1, id)
		h.step()
		h.accept(c, h.lastBlock())
		h.step()
	}
	close(stop)
	wg.Wait()

	if got := h.mark(1); got != 200 {
		t.Fatalf("mark = %d, want 200", got)
	}
}

// TestSubmitTipRaiseHoldsTheStateLock reads a Tip that leads the mark on every pass while
// another goroutine reads and retracts under the state lock, so the race detector sees a raise
// made outside that lock.
func TestSubmitTipRaiseHoldsTheStateLock(t *testing.T) {
	h := newSubmitHarness(t, 1)
	st := h.st(1)

	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			st.mu.Lock()
			_ = st.chainNext
			_ = len(st.commitments)
			st.mu.Unlock()
			retract(1000, st)
		}
	}()

	for id := uint64(0); id < 400; id += 2 {
		h.setTip(1, id)
		c := h.hold(1, id)
		h.step()
		h.accept(c, h.lastBlock())
		h.step()
	}
	close(stop)
	wg.Wait()

	if got := h.mark(1); got != 399 {
		t.Fatalf("mark = %d, want 399", got)
	}
}

func (h *submitHarness) setDomainPause(d uint32, paused bool) {
	g := h.gate
	g.DomainPause = paused
	h.gateBy[d] = g
}

// rejectAttempt sends the held batch id of domain d, makes Settlement reject it, resolves the
// send and lets the backoff expire.
func (h *submitHarness) rejectAttempt(d uint32, id uint64) {
	h.t.Helper()
	before := len(h.node.published)
	h.step()
	h.wantPublished(before + 1)
	h.reject(d, h.lastBlock(), id)
	h.step()
	h.advance(80 * time.Second)
}

func (h *submitHarness) rejectError(d uint32) string {
	return h.sup.Status()[h.slotIndex(d)].RejectError
}

func (h *submitHarness) slotIndex(d uint32) int {
	for i, sl := range h.sup.slots {
		if sl.w.DomainID == d {
			return i
		}
	}
	h.t.Fatalf("no slot for domain %d", d)
	return -1
}

func (h *submitHarness) wantRejectError(d uint32, want string) {
	h.t.Helper()
	got := h.rejectError(d)
	if want == "" && got != "" || !strings.Contains(got, want) {
		h.t.Fatalf("RejectError = %q, want %q", got, want)
	}
}

func TestSubmitGates(t *testing.T) {
	notSent := func(h *submitHarness) int { return strings.Count(h.out.String(), "not sent") }

	// blocked asserts that a step publishes nothing, records nothing and halts nothing.
	blocked := func(t *testing.T, h *submitHarness) {
		t.Helper()
		h.step()
		h.wantPublished(0)
		if _, ok := h.last(); ok {
			t.Fatal("a send was recorded")
		}
		if h.unresolved(1) != nil {
			t.Fatal("a send was marked in flight")
		}
		if f := h.fault(1); f != "" {
			t.Fatalf("fault = %q, want none", f)
		}
		if h.sub.retry[1] != nil {
			t.Fatalf("retry = %+v, want none", h.sub.retry[1])
		}
	}

	t.Run("clear gates publish for the configured pillar", func(t *testing.T) {
		h := newSubmitHarness(t, 1)
		h.hold(1, 0)
		h.step()
		h.wantPublished(1)
		if len(h.gateCalls) != 1 || h.gateCalls[0] != (gateCall{1, "pillar-a"}) {
			t.Fatalf("gate calls = %+v, want one for (1, pillar-a)", h.gateCalls)
		}
	})

	t.Run("global pause", func(t *testing.T) {
		h := newSubmitHarness(t, 1)
		h.hold(1, 0)
		h.gate.GlobalPause = true
		blocked(t, h)
		if notSent(h) != 1 || !h.logHas("global submit pause") {
			t.Fatalf("log = %q, want one warning naming the global submit pause", h.out.String())
		}
		h.gate.GlobalPause = false
		h.step()
		h.wantPublished(1)
	})

	t.Run("domain pause blocks only that domain", func(t *testing.T) {
		h := newSubmitHarness(t, 1, 2)
		h.hold(1, 0)
		h.hold(2, 0)
		h.setDomainPause(1, true)
		h.step()
		h.wantPublished(1)
		if r, _ := h.last(); r.DomainID != 2 {
			t.Fatalf("send for domain %d, want domain 2", r.DomainID)
		}
		if !h.logHas("submit pause for domain 1") {
			t.Fatalf("log = %q, want a warning naming domain 1's pause", h.out.String())
		}
	})

	cases := []struct {
		name string
		set  func(g *settlement.Gates)
		log  string
	}{
		{"pillar not active", func(g *settlement.Gates) { g.PillarActive = false }, `"pillar-a" is not an active pillar`},
		{"no member", func(g *settlement.Gates) { g.Member = nil }, `has no executor-set member`},
		{"member names another address", func(g *settlement.Gates) {
			m := *g.Member
			m.ExecutorAddress = types.Address{0x77}
			g.Member = &m
		}, `executor-set member names`},
		{"member not active", func(g *settlement.Gates) {
			m := *g.Member
			m.Status = 1
			g.Member = &m
		}, `has status 1, not active`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := newSubmitHarness(t, 1)
			h.hold(1, 0)
			tc.set(&h.gate)
			blocked(t, h)
			if !h.logHas(tc.log) {
				t.Fatalf("log = %q, want %q", h.out.String(), tc.log)
			}
		})
	}

	t.Run("gate read errors neither halt nor send", func(t *testing.T) {
		for name, err := range map[string]error{
			"halted": fmt.Errorf("%w: bad", recovery.ErrHalted),
			"plain":  errors.New("connection refused"),
		} {
			t.Run(name, func(t *testing.T) {
				h := newSubmitHarness(t, 1)
				h.hold(1, 0)
				h.gateErr[1] = err
				blocked(t, h)
				if !h.logHas("WARN: submitter: znnd node") {
					t.Fatalf("log = %q, want an outage warning", h.out.String())
				}
				delete(h.gateErr, 1)
				h.step()
				h.wantPublished(1)
			})
		}
	})

	t.Run("no gate read while behind or in backoff", func(t *testing.T) {
		h := newSubmitHarness(t, 1)
		h.step()
		if len(h.gateCalls) != 0 {
			t.Fatalf("gate calls = %+v with nothing held, want none", h.gateCalls)
		}
		h.hold(1, 0)
		h.sub.retry[1] = &retryState{batchID: 0, delay: time.Minute, until: h.clock.Add(time.Hour)}
		h.step()
		if len(h.gateCalls) != 0 {
			t.Fatalf("gate calls = %+v in backoff, want none", h.gateCalls)
		}
	})

	t.Run("a contradiction halts even while paused", func(t *testing.T) {
		h := newSubmitHarness(t, 1)
		h.hold(1, 0)
		h.gate.GlobalPause = true
		tip := h.tips[1]
		tip.FirstInput = 5
		h.tips[1] = tip
		h.step()
		if f := h.fault(1); !strings.Contains(f, ErrInconsistent.Error()) {
			t.Fatalf("fault = %q, want a consistency fault", f)
		}
	})

	t.Run("a pause blocks the adoption of a dropped send's copy", func(t *testing.T) {
		h := newSubmitHarness(t, 1)
		h.hold(1, 0)
		h.node.publish = func(*nom.AccountBlock) error { return errors.New("node refused") }
		h.step()
		h.step()
		h.node.publish = nil
		if r, _ := h.last(); r.Outcome != OutcomeDropped {
			t.Fatalf("outcome = %q, want dropped", r.Outcome)
		}
		dropped := h.node.published[0]
		h.node.put(dropped)
		atHeight := 0
		h.node.atHeight = func(types.Address, uint64) (*nom.AccountBlock, error) {
			atHeight++
			return h.node.blocks[dropped.Height], nil
		}
		h.gate.GlobalPause = true
		h.advance(10 * time.Second)
		h.step()
		if r, _ := h.last(); r.Outcome != OutcomeDropped || r.Hash != dropped.Hash {
			t.Fatalf("record = %+v, want the dropped record unchanged", r)
		}
		if atHeight != 0 {
			t.Fatalf("AccountBlockAtHeight called %d times, want 0", atHeight)
		}
		h.wantPublished(1)
	})
}

func TestSubmitRejectionStatus(t *testing.T) {
	t.Run("the 5th rejection raises the status and later ones update it", func(t *testing.T) {
		h := newSubmitHarness(t, 1)
		h.hold(1, 0)
		for i := 0; i < 4; i++ {
			h.rejectAttempt(1, 0)
		}
		h.wantRejectError(1, "")
		if h.logHas("ERROR: Settlement rejected") {
			t.Fatal("an error was logged before the 5th rejection")
		}
		h.rejectAttempt(1, 0)
		h.wantRejectError(1, "batch 0 5 consecutive")
		if got := strings.Count(h.out.String(), "ERROR: Settlement rejected"); got != 1 {
			t.Fatalf("%d error lines after the 5th rejection, want 1", got)
		}
		h.rejectAttempt(1, 0)
		h.wantRejectError(1, "6 consecutive")
		if !h.logHas("domain 1 WARN: Settlement rejected batch 0 6 consecutive") {
			t.Fatalf("log = %q, want a warning with the count", h.out.String())
		}
		if got := strings.Count(h.out.String(), "ERROR: Settlement rejected"); got != 1 {
			t.Fatalf("%d error lines after the 6th rejection, want 1", got)
		}
		if h.fault(1) != "" {
			t.Fatalf("fault = %q, want none", h.fault(1))
		}
	})

	t.Run("an accepted outcome clears the status at once", func(t *testing.T) {
		h := newSubmitHarness(t, 1)
		c := h.hold(1, 0)
		for i := 0; i < 5; i++ {
			h.rejectAttempt(1, 0)
		}
		h.step()
		h.accept(c, h.lastBlock())
		h.step()
		h.wantRejectError(1, "")
		if h.sub.retry[1] != nil {
			t.Fatal("the acceptance left a retry entry")
		}
	})

	t.Run("the count is per batch", func(t *testing.T) {
		h := newSubmitHarness(t, 1)
		c := h.hold(1, 0)
		for i := 0; i < 4; i++ {
			h.rejectAttempt(1, 0)
		}
		h.step()
		h.accept(c, h.lastBlock())
		h.step()
		h.hold(1, 1)
		h.rejectAttempt(1, 1)
		h.wantRejectError(1, "")
	})

	t.Run("a different batchId in Settlement clears the status", func(t *testing.T) {
		h := newSubmitHarness(t, 1)
		h.hold(1, 0)
		for i := 0; i < 5; i++ {
			h.rejectAttempt(1, 0)
		}
		h.wantRejectError(1, "5 consecutive")
		h.setTip(1, 1)
		h.hold(1, 1)
		h.step()
		h.wantPublished(6)
		h.wantRejectError(1, "")
		if got := h.mark(1); got != 1 {
			t.Fatalf("mark = %d, want 1", got)
		}
		h.reject(1, h.lastBlock(), 1)
		h.step()
		h.advance(80 * time.Second)
		for i := 0; i < 3; i++ {
			h.rejectAttempt(1, 1)
		}
		h.wantRejectError(1, "")
	})

	t.Run("a lagging tip neither clears the count nor the status", func(t *testing.T) {
		h := newSubmitHarness(t, 1)
		h.hold(1, 0)
		h.hold(1, 1)
		h.setTip(1, 1)
		for i := 0; i < 5; i++ {
			h.rejectAttempt(1, 1)
		}
		h.wantRejectError(1, "5 consecutive")
		h.setTip(1, 0)
		for i := 0; i < 3; i++ {
			h.step()
			h.advance(80 * time.Second)
		}
		h.wantPublished(5)
		if got := h.mark(1); got != 1 {
			t.Fatalf("mark = %d, want 1", got)
		}
		if h.held(1, 0) {
			t.Fatal("batch 0 is held again after a lagging read")
		}
		h.wantRejectError(1, "5 consecutive")
		h.setTip(1, 1)
		h.rejectAttempt(1, 1)
		h.wantRejectError(1, "6 consecutive")
	})

	t.Run("a dropped send neither counts nor resets", func(t *testing.T) {
		h := newSubmitHarness(t, 1)
		h.hold(1, 0)
		for i := 0; i < 4; i++ {
			h.rejectAttempt(1, 0)
		}
		h.node.publish = func(*nom.AccountBlock) error { return errors.New("node refused") }
		h.step()
		h.step()
		h.node.publish = nil
		if r, _ := h.last(); r.Outcome != OutcomeDropped {
			t.Fatalf("outcome = %q, want dropped", r.Outcome)
		}
		h.wantRejectError(1, "")
		h.advance(80 * time.Second)
		h.rejectAttempt(1, 0)
		h.wantRejectError(1, "5 consecutive")
	})

	t.Run("a superseded send neither counts nor resets", func(t *testing.T) {
		h := newSubmitHarness(t, 1)
		h.hold(1, 0)
		for i := 0; i < 4; i++ {
			h.rejectAttempt(1, 0)
		}
		h.step()
		sent := h.lastBlock()
		other := *sent
		other.Data = []byte{0xde, 0xad}
		other.Hash = types.Hash{0x55}
		h.node.put(&other)
		h.step()
		if r, _ := h.last(); r.Outcome != OutcomeSuperseded {
			t.Fatalf("outcome = %q, want superseded", r.Outcome)
		}
		h.advance(80 * time.Second)
		h.rejectAttempt(1, 0)
		h.wantRejectError(1, "5 consecutive")
	})

	t.Run("a gate block or a pick pass neither counts nor resets", func(t *testing.T) {
		h := newSubmitHarness(t, 1)
		h.hold(1, 0)
		for i := 0; i < 4; i++ {
			h.rejectAttempt(1, 0)
		}
		h.setDomainPause(1, true)
		h.step()
		h.wantPublished(4)
		h.setDomainPause(1, false)
		h.rejectAttempt(1, 0)
		h.wantRejectError(1, "5 consecutive")

		h.setDomainPause(1, true)
		h.step()
		h.wantPublished(5)
		h.wantRejectError(1, "5 consecutive")

		h.setDomainPause(1, false)
		h.rejectAttempt(1, 0)
		h.wantRejectError(1, "6 consecutive")
	})

	t.Run("a new supervisor starts with no status", func(t *testing.T) {
		h := newSubmitHarness(t, 1, 2)
		for _, s := range h.sup.Status() {
			if s.RejectError != "" {
				t.Fatalf("RejectError = %q for domain %d, want none", s.RejectError, s.DomainID)
			}
		}
	})
}

func TestSubmitTipRaisesTheMark(t *testing.T) {
	t.Run("a higher tip raises the mark, prunes below it and keeps the batch it names", func(t *testing.T) {
		h := newSubmitHarness(t, 1)
		for id := uint64(0); id < 4; id++ {
			h.hold(1, id)
		}
		h.setTip(1, 3)
		h.step()
		if got := h.mark(1); got != 3 {
			t.Fatalf("mark = %d, want 3", got)
		}
		for id := uint64(0); id < 3; id++ {
			if h.held(1, id) {
				t.Fatalf("batch %d is still held below the mark", id)
			}
		}
		if !h.held(1, 3) {
			t.Fatal("batch 3 was pruned")
		}
		h.wantPublished(1)

		h.reject(1, h.lastBlock(), 3)
		h.step()
		h.advance(80 * time.Second)
		h.setTip(1, 2)
		h.step()
		if got := h.mark(1); got != 3 {
			t.Fatalf("mark = %d after a lower read, want 3", got)
		}
		h.wantPublished(1)
	})

	t.Run("a lower read is warned once per throttle window", func(t *testing.T) {
		h := newSubmitHarness(t, 1)
		h.sub.warnEvery = time.Minute
		h.setTip(1, 3)
		h.step()
		h.setTip(1, 2)
		for i := 0; i < 10; i++ {
			h.step()
		}
		const line = "reported nextBatchId 2, below the observed 3"
		if got := strings.Count(h.out.String(), line); got != 1 {
			t.Fatalf("%d lines with %q, want 1; log = %q", got, line, h.out.String())
		}
		if !h.logHas("znnd.test:35997") || h.logHas("s3cret") {
			t.Fatalf("log = %q, want the redacted endpoint", h.out.String())
		}
		if got := h.mark(1); got != 3 {
			t.Fatalf("mark = %d, want 3", got)
		}
		h.advance(time.Minute)
		h.step()
		if got := strings.Count(h.out.String(), line); got != 2 {
			t.Fatalf("%d lines after the window, want 2", got)
		}
	})

	t.Run("a failed tip read raises nothing", func(t *testing.T) {
		h := newSubmitHarness(t, 1)
		h.hold(1, 0)
		h.setTip(1, 5)
		h.tipErr[1] = errors.New("node down")
		h.step()
		if got := h.mark(1); got != 0 {
			t.Fatalf("mark = %d, want 0", got)
		}
		if !h.held(1, 0) {
			t.Fatal("batch 0 was pruned by a failed read")
		}
		h.wantPublished(0)
	})

	t.Run("a pending send's resolution does not raise the mark", func(t *testing.T) {
		h := newSubmitHarness(t, 1)
		h.hold(1, 0)
		h.step()
		h.wantPublished(1)
		h.setTip(1, 5)
		h.step()
		if got := h.mark(1); got != 0 {
			t.Fatalf("mark = %d while the send is pending, want 0", got)
		}
		if !h.held(1, 0) {
			t.Fatal("batch 0 was pruned while its send is pending")
		}
	})
}

// --- submission log ---

// failingLog is a memLog whose Sent or Resolved returns an error when one is set.
type failingLog struct {
	memLog
	sentErr     error
	resolvedErr error
}

func (l *failingLog) Sent(r SendRecord) error {
	if l.sentErr != nil {
		return l.sentErr
	}
	return l.memLog.Sent(r)
}

func (l *failingLog) Resolved(r SendRecord) error {
	l.memLog.Resolved(r)
	return l.resolvedErr
}

// restoredSend returns the block at height 5 that carries c with the given Data, and its record.
func restoredSend(c codec.BatchCommitment, tag byte, data []byte) (*nom.AccountBlock, SendRecord) {
	b := &nom.AccountBlock{
		Hash:                 types.Hash{tag},
		Height:               5,
		Address:              harnessExecutor,
		ToAddress:            types.SettlementContract,
		Data:                 data,
		MomentumAcknowledged: types.HashHeight{Height: 30},
	}
	return b, SendRecord{
		DomainID: c.DomainID, Method: definition.SettlementSubmitBatchMethodName, BatchID: c.BatchID,
		Height: b.Height, Hash: b.Hash, DataHash: types.NewHash(data),
	}
}

// restoredHarness sets up domains 1 and 2 with domain 1's batch 0 held, and domain 2's batch 3
// sent as block b at height 5 and unresolved in a memLog.
func restoredHarness(t *testing.T) (*submitHarness, codec.BatchCommitment, *nom.AccountBlock, SendRecord, *memLog) {
	h := newSubmitHarness(t, 1, 2)
	h.hold(1, 0)
	c2 := h.hold(2, 3)
	h.setTip(2, 3)
	b, r := restoredSend(c2, 0xb2, packSubmit(t, c2))
	lg := &memLog{}
	lg.Sent(r)
	return h, c2, b, r, lg
}

func TestResumeFollowsThePendingSend(t *testing.T) {
	h, c2, b, _, lg := restoredHarness(t)
	h.sub.resume(lg, nil, nil)
	if u := h.unresolved(2); u == nil || u.batchID != 3 {
		t.Fatalf("unresolved = %+v, want batch 3", u)
	}
	if !h.logHas("from the submission log") {
		t.Fatalf("log = %q, want the restore line", h.out.String())
	}

	h.node.put(b)
	h.step()
	h.wantPublished(0)
	if h.node.statusCount(b.Hash) == 0 {
		t.Fatal("the restored send was not checked")
	}

	h.accept(c2, b)
	h.step()
	if h.mark(2) != 4 || h.held(2, 3) {
		t.Fatalf("mark = %d, held = %v; want 4 and pruned", h.mark(2), h.held(2, 3))
	}
	if r, _ := h.last(); r.Outcome != OutcomeAccepted {
		t.Fatalf("Last = %+v, want accepted", r)
	}
	if h.unresolved(2) != nil || h.sub.pending != nil {
		t.Fatal("the send was not released")
	}
	if h.logHas("INFO") {
		t.Fatalf("log = %q, want no INFO line", h.out.String())
	}
	h.step()
	h.wantPublished(1)
	if r, _ := h.last(); r.DomainID != 1 {
		t.Fatalf("next send for domain %d, want 1", r.DomainID)
	}
}

func TestResumeRejectedSendCountsOnce(t *testing.T) {
	h, _, b, _, lg := restoredHarness(t)
	h.sub.resume(lg, nil, nil)
	h.reject(2, b, 3)
	h.step()
	if rs := h.sub.retry[2]; rs == nil || rs.rejections != 1 {
		t.Fatalf("retry = %+v, want one rejection", rs)
	}
}

func TestResumeDroppedSendIsAdopted(t *testing.T) {
	setup := func(t *testing.T) (*submitHarness, *nom.AccountBlock) {
		h := newSubmitHarness(t, 2)
		c2 := h.hold(2, 3)
		h.setTip(2, 3)
		b, r := restoredSend(c2, 0xb2, packSubmit(t, c2))
		lg := &memLog{}
		lg.Sent(r)
		h.node.put(&nom.AccountBlock{Height: 4, Hash: types.Hash{0x04}, Data: []byte{1}})
		h.sub.resume(lg, nil, nil)
		h.step()
		h.wantPublished(0)
		if rs := h.sub.retry[2]; rs == nil || rs.dropped != 5 {
			t.Fatalf("retry = %+v, want dropped at height 5", rs)
		}
		return h, b
	}

	t.Run("the send reached the node late", func(t *testing.T) {
		h, b := setup(t)
		h.node.put(b)
		h.advance(time.Minute)
		h.step()
		h.wantPublished(0)
		if r, _ := h.last(); r.Hash != b.Hash || r.Outcome != "" {
			t.Fatalf("Last = %+v, want %s unresolved", r, b.Hash)
		}
		if !h.logHas("already on the node") {
			t.Fatalf("log = %q, want the adoption line", h.out.String())
		}
	})

	t.Run("the node stays at height 4", func(t *testing.T) {
		h, _ := setup(t)
		h.advance(time.Minute)
		h.step()
		h.wantPublished(1)
		if got := h.lastBlock().Height; got != 5 {
			t.Fatalf("published height %d, want 5", got)
		}
	})
}

func TestResumeSkipsResolvedRecords(t *testing.T) {
	for _, o := range []Outcome{OutcomeAbandoned, OutcomeAccepted} {
		t.Run(string(o), func(t *testing.T) {
			h, _, _, r, _ := restoredHarness(t)
			r.Outcome = o
			lg := &memLog{}
			lg.Sent(r)
			h.sub.resume(lg, nil, nil)
			if h.sub.pending != nil || h.unresolved(2) != nil {
				t.Fatal("a resolved record was restored")
			}
			h.step()
			h.wantPublished(1)
			if h.node.statusCount(r.Hash) != 0 {
				t.Fatal("the resolved record was checked")
			}
		})
	}
}

func TestResumeUnconfiguredDomain(t *testing.T) {
	setup := func(t *testing.T) (*submitHarness, codec.BatchCommitment, *nom.AccountBlock) {
		h := newSubmitHarness(t, 1)
		h.hold(1, 0)
		c9 := h.commitment(9, 3, 40)
		h.setTip(9, 3)
		b9, r := restoredSend(c9, 0xb9, packSubmit(t, c9))
		lg := &memLog{}
		lg.Sent(r)
		h.sub.resume(lg, nil, nil)
		if h.sub.pending == nil || h.sub.pending.slot != -1 {
			t.Fatalf("pending = %+v, want slot -1", h.sub.pending)
		}
		if !h.logHas("domain 9 is not configured") {
			t.Fatalf("log = %q, want the restore warning", h.out.String())
		}
		return h, c9, b9
	}
	finish := func(h *submitHarness) {
		h.t.Helper()
		h.step()
		h.wantPublished(1)
		if r, _ := h.last(); r.DomainID != 1 {
			h.t.Fatalf("next send for domain %d, want 1", r.DomainID)
		}
		if h.sub.retry[9] != nil || h.fault(1) != "" {
			h.t.Fatalf("retry[9] = %+v, fault(1) = %q; want no retry state and no halt", h.sub.retry[9], h.fault(1))
		}
	}

	t.Run("accepted", func(t *testing.T) {
		h, c9, b9 := setup(t)
		h.accept(c9, b9)
		h.step()
		h.wantPublished(0)
		if r, _ := h.last(); r.Outcome != OutcomeAccepted {
			t.Fatalf("Last = %+v, want accepted", r)
		}
		if h.sub.pending != nil {
			t.Fatal("the send was not released")
		}
		if !h.logHas("WARN") || !h.logHas("domain 9") || !h.logHas("finished accepted") {
			t.Fatalf("log = %q, want a warning naming domain 9", h.out.String())
		}
		finish(h)
	})

	t.Run("unreachable then dropped", func(t *testing.T) {
		h, _, _ := setup(t)
		h.node.status = func(types.Hash) (znnd.SendStatus, error) { return znnd.SendStatus{}, errors.New("node down") }
		h.step()
		h.step()
		h.wantPublished(0)
		if h.tipCalls[1] != 0 {
			t.Fatalf("%d Tip calls for domain 1 while the node is down", h.tipCalls[1])
		}
		if r, _ := h.last(); r.Outcome != "" {
			t.Fatalf("Last = %+v, want no outcome", r)
		}
		if !h.logHas("retrying") {
			t.Fatalf("log = %q, want the outage line", h.out.String())
		}
		h.node.status = nil
		h.step()
		if r, _ := h.last(); r.Outcome != OutcomeDropped {
			t.Fatalf("Last = %+v, want dropped", r)
		}
		if !h.logHas("finished dropped") {
			t.Fatalf("log = %q, want the dropped warning", h.out.String())
		}
		finish(h)
	})

	t.Run("fault", func(t *testing.T) {
		h, c9, b9 := setup(t)
		h.accept(c9, b9)
		h.tipErr[9] = fmt.Errorf("fake: %w", recovery.ErrHalted)
		h.step()
		if r, _ := h.last(); r.Outcome != OutcomeAbandoned {
			t.Fatalf("Last = %+v, want abandoned", r)
		}
		if !h.logHas("domain 9") || !h.logHas("consistency fault") {
			t.Fatalf("log = %q, want a warning with the fault", h.out.String())
		}
		finish(h)
	})
}

func TestResumeRetractThenAcceptHalts(t *testing.T) {
	h, c2, b, _, lg := restoredHarness(t)
	h.sub.resume(lg, nil, nil)
	retract(3, h.st(2))
	h.accept(c2, b)
	h.step()
	if f := h.fault(2); !strings.Contains(f, "retracted") {
		t.Fatalf("fault = %q, want a retracted fault", f)
	}
}

func TestResumeSeedsTheMark(t *testing.T) {
	setup := func(t *testing.T) (*submitHarness, codec.BatchCommitment) {
		h := newSubmitHarness(t, 1)
		h.sub.resume(&memLog{}, map[uint32]uint64{1: 4, 9: 2}, nil)
		if h.mark(1) != 5 {
			t.Fatalf("mark = %d, want 5", h.mark(1))
		}
		if !h.logHas("observed nextBatchId 5 from the submission log") {
			t.Fatalf("log = %q, want the seed line", h.out.String())
		}
		h.hold(1, 4)
		return h, h.hold(1, 5)
	}
	for _, n := range []uint64{4, 3} {
		t.Run(fmt.Sprintf("tip %d", n), func(t *testing.T) {
			h, _ := setup(t)
			h.setTip(1, n)
			h.step()
			h.wantPublished(0)
			if !h.logHas("below the observed 5") || h.mark(1) != 5 {
				t.Fatalf("mark = %d, log = %q; want the lagging-read warning and mark 5", h.mark(1), h.out.String())
			}
		})
	}
	t.Run("tip 5", func(t *testing.T) {
		h, c5 := setup(t)
		h.setTip(1, 5)
		h.step()
		h.wantPublished(1)
		if !bytes.Equal(h.lastBlock().Data, packSubmit(t, c5)) {
			t.Fatal("the published block does not carry batch 5")
		}
	})
}

func TestLogFailureStopsSending(t *testing.T) {
	check := func(t *testing.T, h *submitHarness) {
		t.Helper()
		got := h.sup.SubmitterStatus().Error
		if !strings.Contains(got, "disk full") || !strings.Contains(got, "nothing is sent until the executor restarts") {
			t.Fatalf("Error = %q, want the log failure", got)
		}
		if n := strings.Count(h.out.String(), "ERROR: submitter"); n != 1 {
			t.Fatalf("%d ERROR lines, want 1", n)
		}
		tips, plasma, published := h.tipCalls[1], h.node.plasmaCalls, len(h.node.published)
		h.step()
		if h.tipCalls[1] != tips || h.node.plasmaCalls != plasma || len(h.node.published) != published {
			t.Fatal("the submitter worked after the log failed")
		}
		if n := strings.Count(h.out.String(), "ERROR: submitter"); n != 1 {
			t.Fatalf("%d ERROR lines after another step, want 1", n)
		}
	}

	t.Run("sent line fails", func(t *testing.T) {
		h := newSubmitHarness(t, 1)
		h.hold(1, 0)
		h.sub.log = &failingLog{sentErr: errors.New("disk full")}
		h.step()
		h.wantPublished(0)
		if h.unresolved(1) != nil {
			t.Fatal("unresolved was kept")
		}
		check(t, h)
	})

	t.Run("outcome line fails", func(t *testing.T) {
		h := newSubmitHarness(t, 1)
		c := h.hold(1, 0)
		h.sub.log = &failingLog{resolvedErr: errors.New("disk full")}
		h.step()
		h.wantPublished(1)
		h.accept(c, h.lastBlock())
		h.step()
		if h.sub.pending != nil || h.mark(1) != 1 {
			t.Fatalf("pending = %+v, mark = %d; want the send released and mark 1", h.sub.pending, h.mark(1))
		}
		if r, _ := h.last(); r.Outcome != OutcomeAccepted {
			t.Fatalf("Last = %+v, want accepted", r)
		}
		check(t, h)
	})
}

func TestSubmissionLogSurvivesRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "x.submissions.jsonl")
	resume := func(h *submitHarness) *fileLog {
		lg, err := openSendLog(path)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { lg.Close() })
		h.sub.resume(lg, lg.accepted, lg.unreadable)
		return lg
	}

	h1 := newSubmitHarness(t, 1)
	c := h1.hold(1, 0)
	lg1 := resume(h1)
	h1.node.publish = func(b *nom.AccountBlock) error {
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		lines := strings.Split(strings.TrimSuffix(string(data), "\n"), "\n")
		r, why, err := decodeSendRecord([]byte(lines[0]))
		if len(lines) != 1 || err != nil || why != nil || r.Hash != b.Hash || r.Outcome != "" {
			t.Errorf("the log at publish time holds %q, want one unresolved line for %s", data, b.Hash)
		}
		return nil
	}
	h1.step()
	h1.wantPublished(1)
	b := h1.lastBlock()
	lg1.Close()

	h2 := newSubmitHarness(t, 1)
	h2.sup.src.Node = h1.node
	h2.node = h1.node
	h2.hold(1, 0)
	resume(h2)
	h2.step()
	h1.wantPublished(1)
	if h1.node.statusCount(b.Hash) == 0 {
		t.Fatal("the restored send was not checked")
	}
	h2.accept(c, b)
	h2.step()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSuffix(string(data), "\n"), "\n")
	if len(lines) != 2 || !strings.Contains(lines[1], `"outcome":"accepted"`) {
		t.Fatalf("log = %q, want the accepted outcome as line 2", data)
	}

	h3 := newSubmitHarness(t, 1)
	resume(h3)
	if h3.mark(1) != 1 {
		t.Fatalf("mark = %d, want 1", h3.mark(1))
	}
}

func TestResumeUndecodableSend(t *testing.T) {
	setup := func(t *testing.T, data []byte) (*submitHarness, codec.BatchCommitment, *nom.AccountBlock) {
		h := newSubmitHarness(t, 1, 2)
		h.hold(1, 0)
		c2 := h.hold(2, 3)
		h.setTip(2, 3)
		bx, r := restoredSend(c2, 0xbd, data)
		lg := &memLog{}
		lg.Sent(r)
		h.sub.resume(lg, nil, nil)
		return h, c2, bx
	}
	otherVersion := func(t *testing.T) []byte {
		enc, err := commitmentFor(2, harnessExecutor, 3, 40).Encode()
		if err != nil {
			t.Fatal(err)
		}
		enc[0] ^= 0xff
		data, err := definition.ABISettlement.PackMethod(definition.SettlementSubmitBatchMethodName, enc)
		if err != nil {
			t.Fatal(err)
		}
		return data
	}

	accepted := func(t *testing.T, data []byte, why string) {
		h, c2, bx := setup(t, data)
		h.accept(c2, bx)
		h.setTipHeight(2, 80)
		h.step()
		if r, _ := h.last(); r.Outcome != "" {
			t.Fatalf("Last = %+v, want no outcome while the receive is pending", r)
		}
		if h.logHas("cannot decode") {
			t.Fatal("the INFO line was logged before the receive")
		}
		h.setTipHeight(2, 100)
		h.step()
		if r, _ := h.last(); r.Outcome != OutcomeAccepted {
			t.Fatalf("Last = %+v, want accepted", r)
		}
		if h.mark(2) != 4 || h.fault(2) != "" || h.sub.pending != nil || h.unresolved(2) != nil {
			t.Fatalf("mark = %d, fault = %q, pending = %+v; want 4, none, released", h.mark(2), h.fault(2), h.sub.pending)
		}
		out := h.out.String()
		if n := strings.Count(out, "cannot decode"); n != 1 {
			t.Fatalf("%d INFO lines, want 1: %q", n, out)
		}
		for _, line := range strings.Split(out, "\n") {
			if strings.Contains(line, "cannot decode") {
				if !strings.Contains(line, "INFO") || !strings.Contains(line, bx.Hash.String()) || !strings.Contains(line, why) {
					t.Fatalf("INFO line = %q, want INFO, %s and %q", line, bx.Hash, why)
				}
			}
		}
		if h.logHas("retrying") {
			t.Fatal("the outage line was logged")
		}
		h.step()
		h.wantPublished(1)
		if r, _ := h.last(); r.DomainID != 1 {
			t.Fatalf("next send for domain %d, want 1", r.DomainID)
		}
	}

	t.Run("accepted", func(t *testing.T) { accepted(t, otherVersion(t), "unknown version") })
	t.Run("accepted with data that does not unpack", func(t *testing.T) {
		accepted(t, []byte{0xde, 0xad, 0xbe, 0xef, 0x01}, "")
	})

	t.Run("rejected", func(t *testing.T) {
		h, _, bx := setup(t, otherVersion(t))
		h.reject(2, bx, 3)
		h.step()
		if r, _ := h.last(); r.Outcome != OutcomeRejected {
			t.Fatalf("Last = %+v, want rejected", r)
		}
		if rs := h.sub.retry[2]; rs == nil || rs.rejections != 1 || h.fault(2) != "" {
			t.Fatalf("retry = %+v, fault = %q; want one rejection and no fault", rs, h.fault(2))
		}
		if h.logHas("cannot decode") {
			t.Fatal("the INFO line was logged for a rejected send")
		}
	})
}

func TestResumeUnreadableRecord(t *testing.T) {
	why := errors.New("log-format version 2 is not 1")

	t.Run("configured domain", func(t *testing.T) {
		h, _, b, _, _ := restoredHarness(t)
		lg := &memLog{}
		lg.Sent(SendRecord{DomainID: 2, BatchID: 3, Hash: b.Hash})
		h.sub.resume(lg, nil, why)
		if u := h.unresolved(2); u == nil || u.batchID != 3 {
			t.Fatalf("unresolved = %+v, want batch 3", u)
		}
		if h.logHas("following send") {
			t.Fatal("an unreadable send was announced")
		}
		h.node.status = func(types.Hash) (znnd.SendStatus, error) { return znnd.SendStatus{}, errors.New("node down") }
		h.step()
		f := h.fault(2)
		for _, s := range []string{"cannot be read by this build", "log-format version 2", b.Hash.String()} {
			if !strings.Contains(f, s) {
				t.Fatalf("fault = %q, want %q", f, s)
			}
		}
		if r, _ := h.last(); r.Outcome != OutcomeAbandoned {
			t.Fatalf("Last = %+v, want abandoned", r)
		}
		if h.sub.pending != nil || h.unresolved(2) != nil || len(h.node.statusCalls) != 0 {
			t.Fatalf("pending = %+v, status calls = %d; want a release and no node read", h.sub.pending, len(h.node.statusCalls))
		}
		h.node.status = nil
		h.step()
		h.wantPublished(1)
		if r, _ := h.last(); r.DomainID != 1 {
			t.Fatalf("next send for domain %d, want 1", r.DomainID)
		}
	})

	t.Run("unconfigured domain", func(t *testing.T) {
		h := newSubmitHarness(t, 1)
		h.hold(1, 0)
		lg := &memLog{}
		lg.Sent(SendRecord{DomainID: 9, BatchID: 3, Hash: types.Hash{0xb9}})
		h.sub.resume(lg, nil, why)
		h.step()
		if r, _ := h.last(); r.Outcome != OutcomeAbandoned {
			t.Fatalf("Last = %+v, want abandoned", r)
		}
		if !h.logHas("domain 9") || !h.logHas("cannot be read by this build") {
			t.Fatalf("log = %q, want a warning naming domain 9", h.out.String())
		}
		if h.fault(1) != "" || h.sub.retry[9] != nil || len(h.node.statusCalls) != 0 {
			t.Fatal("the unconfigured fallback halted, retried or read the node")
		}
		h.step()
		h.wantPublished(1)
		if r, _ := h.last(); r.DomainID != 1 {
			t.Fatalf("next send for domain %d, want 1", r.DomainID)
		}
	})
}

func TestLogFailedReportsOnce(t *testing.T) {
	h := newSubmitHarness(t, 1)
	h.sub.logFailed(errors.New("disk full"))
	h.sub.logFailed(errors.New("second failure"))
	got := h.sup.SubmitterStatus().Error
	if !strings.Contains(got, "disk full") || strings.Contains(got, "second failure") {
		t.Fatalf("Error = %q, want only the first failure", got)
	}
	if n := strings.Count(h.out.String(), "ERROR: submitter"); n != 1 {
		t.Fatalf("%d ERROR lines, want 1", n)
	}
}

// A failed outcome write stops sending on every path that records an outcome.
func TestLogFailureOnResolvePaths(t *testing.T) {
	why := errors.New("log-format version 2 is not 1")
	cases := []struct {
		name     string
		domainID uint32
		domains  []uint32
	}{
		{"abandon on a halted domain", 2, []uint32{1, 2}},
		{"drop for an unconfigured domain", 9, []uint32{1}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := newSubmitHarness(t, tc.domains...)
			h.hold(1, 0)
			lg := &failingLog{resolvedErr: errors.New("disk full")}
			lg.memLog.Sent(SendRecord{DomainID: tc.domainID, BatchID: 3, Hash: types.Hash{0xc1}})
			h.sub.resume(lg, nil, why)
			h.step()
			if got := h.sup.SubmitterStatus().Error; !strings.Contains(got, "disk full") || !strings.Contains(got, "nothing is sent until the executor restarts") {
				t.Fatalf("Error = %q, want the log failure", got)
			}
			if n := strings.Count(h.out.String(), "ERROR: submitter"); n != 1 {
				t.Fatalf("%d ERROR lines, want 1", n)
			}
			h.step()
			h.wantPublished(0)
		})
	}
}
