//go:build devnet

package devnet

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/zenon-network/go-zenon/common/types"

	"github.com/Zeno-L2/zeno-executor/internal/supervisor"
)

// listenMarker precedes the address in the gateway's listen line.
const listenMarker = "gateway: serving JSON-RPC on "

// gatewayClient serves every request the test sends a gateway.
var gatewayClient = &http.Client{Timeout: 30 * time.Second}

// gatewayProc is one gateway process.
type gatewayProc struct {
	t      *testing.T
	d      *devnet
	name   string
	cmd    *exec.Cmd
	log    string        // its combined stdout and stderr
	url    string        // http://127.0.0.1:<port>, from its listen line
	exited chan struct{} // closed when cmd.Wait returns
	err    error         // cmd.Wait's result; read only after exited is closed
}

type rpcError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

type receiptView struct {
	BatchID  uint64 `json:"batchId"`
	Verified bool   `json:"verified"`
	Tag      string `json:"tag"`
	Sender   string `json:"sender"`
	Receipt  struct {
		Status string `json:"status"`
	} `json:"receipt"`
}

type batchView struct {
	DAHash string `json:"daHash"`
	Inputs []struct {
		SendHash string `json:"sendHash"`
	} `json:"inputs"`
}

type headView struct {
	Status string `json:"status"`
	Latest *struct {
		BatchID uint64 `json:"batchId"`
	} `json:"latest"`
	Finalized *struct {
		BatchID uint64 `json:"batchId"`
	} `json:"finalized"`
}

// logWant is one expected submission log record.
type logWant struct {
	batch   uint64
	height  uint64
	outcome supervisor.Outcome
}

// listenURL returns the address in the first complete listen line of
// log.
func listenURL(log string) (url string, ok bool) {
	i := strings.Index(log, listenMarker)
	if i < 0 {
		return "", false
	}
	rest := log[i+len(listenMarker):]
	nl := strings.IndexByte(rest, '\n')
	if nl < 0 {
		return "", false
	}
	return rest[:nl], true
}

// tagRank orders the tags a batch passes through: pending, latest,
// finalized. Any other string ranks -1.
func tagRank(tag string) int {
	switch tag {
	case "pending":
		return 0
	case "latest":
		return 1
	case "finalized":
		return 2
	}
	return -1
}

// checkReceipt compares a receipt with the tag the test waits for. It
// reports done once the receipt holds that tag with the expected
// content, and an error when the tag was skipped or the content
// differs.
func checkReceipt(got receiptView, want string) (done bool, err error) {
	rank, wantRank := tagRank(got.Tag), tagRank(want)
	if wantRank < 0 {
		return false, fmt.Errorf("unknown tag %q to wait for", want)
	}
	if rank < 0 {
		return false, fmt.Errorf("the receipt has unknown tag %q", got.Tag)
	}
	if rank < wantRank {
		return false, nil
	}
	if rank > wantRank {
		return false, fmt.Errorf("the receipt skipped tag `%s`: it is already at `%s`", want, got.Tag)
	}
	if wantVerified := want != "pending"; got.Verified != wantVerified {
		return false, fmt.Errorf("the receipt at tag %s has verified %v, want %v", want, got.Verified, wantVerified)
	}
	if got.Receipt.Status != "VALIDATION_FAILED" {
		return false, fmt.Errorf("the receipt at tag %s has status %q, want VALIDATION_FAILED", want, got.Receipt.Status)
	}
	if got.Sender != adminAddress.String() {
		return false, fmt.Errorf("the receipt at tag %s has sender %q, want %s", want, got.Sender, adminAddress)
	}
	return true, nil
}

// decodeRPC decodes a JSON-RPC response body. It returns the error
// member when there is one, and otherwise unmarshals the result member
// into out.
func decodeRPC(body []byte, out any) (*rpcError, error) {
	var env struct {
		Result json.RawMessage `json:"result"`
		Error  *rpcError       `json:"error"`
	}
	if err := json.Unmarshal(body, &env); err != nil {
		return nil, fmt.Errorf("the response is not JSON: %w", err)
	}
	if env.Error != nil {
		return env.Error, nil
	}
	if env.Result == nil {
		return nil, errors.New("the response holds neither a result nor an error")
	}
	if err := json.Unmarshal(env.Result, out); err != nil {
		return nil, fmt.Errorf("decode the result: %w", err)
	}
	return nil, nil
}

// expectedLog returns the submission log of batches 0 to last: each
// batch b is sent at account height 2+b and then resolved as accepted.
func expectedLog(last uint64) []logWant {
	var out []logWant
	for b := uint64(0); b <= last; b++ {
		out = append(out, logWant{b, 2 + b, ""}, logWant{b, 2 + b, supervisor.OutcomeAccepted})
	}
	return out
}

// checkLog requires r to match want record for record, each outcome
// record naming the hash of the send record before it.
func checkLog(r []supervisor.SendRecord, want []logWant) error {
	if len(r) != len(want) {
		return fmt.Errorf("the submission log holds %d records, want %d: %+v", len(r), len(want), r)
	}
	for i, w := range want {
		if r[i].BatchID != w.batch || r[i].Height != w.height || r[i].Outcome != w.outcome {
			return fmt.Errorf("record %d is %+v, want batch %d at account height %d with outcome %q", i, r[i], w.batch, w.height, w.outcome)
		}
		if i%2 == 1 && r[i].Hash != r[i-1].Hash {
			return fmt.Errorf("record %d has hash %s, want the hash %s of its send record", i, r[i].Hash, r[i-1].Hash)
		}
	}
	return nil
}

// rpcRequest is the request body of one JSON-RPC call. params is a JSON
// array.
func rpcRequest(method, params string) string {
	return fmt.Sprintf(`{"jsonrpc":"2.0","id":1,"method":%q,"params":%s}`, method, params)
}

// compareBodies returns the requests the two gateways must answer with
// equal bytes. batches[i] is batch i's zeno_getBatch result.
func compareBodies(last uint64, batches []batchView) []string {
	var out []string
	add := func(method, params string) { out = append(out, rpcRequest(method, params)) }
	add("zeno_getBatch", `[1,"latest"]`)
	add("zeno_getBatch", `[1,"finalized"]`)
	for id := uint64(0); id <= last; id++ {
		add("zeno_getBatch", fmt.Sprintf(`[1,%d]`, id))
	}
	for id := uint64(0); id <= last; id++ {
		for _, in := range batches[id].Inputs {
			add("zeno_getReceipt", fmt.Sprintf(`["%s"]`, in.SendHash))
		}
	}
	path := types.ZeroHash.String()
	for _, tag := range []string{"latest", "finalized"} {
		add("zeno_getStateAt", fmt.Sprintf(`[1,"%s","%s"]`, path, tag))
		add("zeno_getProof", fmt.Sprintf(`[1,["%s"],"%s"]`, path, tag))
	}
	for _, m := range []string{"zeno_getInputs", "zeno_getEvents", "zeno_getOutbox"} {
		add(m, `[{"domainId":1}]`)
	}
	for id := uint64(0); id <= last; id++ {
		add("zeno_getBundle", fmt.Sprintf(`["%s"]`, batches[id].DAHash))
		add("zeno_getReceiptSidecar", fmt.Sprintf(`["%s"]`, batches[id].DAHash))
	}
	add("zeno_chainInfo", `[1]`)
	return out
}

// headWithoutPending decodes a zeno_getHead result and splits off its
// pending member.
func headWithoutPending(result []byte) (pending json.RawMessage, rest map[string]json.RawMessage, err error) {
	if err := json.Unmarshal(result, &rest); err != nil {
		return nil, nil, fmt.Errorf("decode the head: %w", err)
	}
	pending = rest["pending"]
	delete(rest, "pending")
	return pending, rest, nil
}

// batchOf names the batch of a head's tag for a failure message.
func batchOf(t *struct {
	BatchID uint64 `json:"batchId"`
}) string {
	if t == nil {
		return "none"
	}
	return fmt.Sprintf("batch %d", t.BatchID)
}

// cut shortens b to 512 bytes for a failure message.
func cut(b []byte) string {
	if len(b) > 512 {
		return string(b[:512]) + "..."
	}
	return string(b)
}

// writeGatewayConfig writes the configuration of a gateway named name,
// serving dom on a free loopback port, and returns its path.
func writeGatewayConfig(t *testing.T, dir, name string, dom map[string]any) string {
	t.Helper()
	cfg := map[string]any{
		"znndEndpoint":      endpoint,
		"networkName":       "devnet",
		"confirmationDepth": confirmationDepth,
		"storagePath":       filepath.Join(dir, name+"-storage"),
		"listenAddress":     "127.0.0.1:0",
		"domains":           []any{dom},
		"simulation":        map[string]any{"maxConcurrent": 1},
	}
	b, err := json.Marshal(cfg)
	if err != nil {
		t.Fatalf("marshal the gateway configuration: %v", err)
	}
	path := filepath.Join(dir, name+".json")
	if err := os.WriteFile(path, b, 0o600); err != nil {
		t.Fatalf("write the gateway configuration: %v", err)
	}
	return path
}

// startGateway starts the gateway binary and returns once its log names
// the address it serves.
func (d *devnet) startGateway(name, bin, configPath string) *gatewayProc {
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
		d.t.Fatalf("start gateway %s: %v", name, err)
	}
	g := &gatewayProc{t: d.t, d: d, name: name, cmd: cmd, log: logPath, exited: make(chan struct{})}
	go func() {
		g.err = cmd.Wait()
		f.Close()
		close(g.exited)
	}()
	d.t.Cleanup(func() {
		select {
		case <-g.exited:
			return
		default:
		}
		cmd.Process.Signal(syscall.SIGTERM)
		select {
		case <-g.exited:
		case <-time.After(30 * time.Second):
			cmd.Process.Kill()
			<-g.exited
		}
	})
	d.waitFor(fmt.Sprintf("gateway %s to listen", name), 2*time.Minute, func() (bool, string, error) {
		g.alive()
		b, err := os.ReadFile(logPath)
		if err != nil {
			return false, "", err
		}
		url, ok := listenURL(string(b))
		if ok {
			g.url = url
		}
		return ok, "no listen line", nil
	})
	d.gws = append(d.gws, g)
	return g
}

// alive fails the test when the gateway exited, raised an alarm or
// could not start a simulator.
func (g *gatewayProc) alive() {
	g.t.Helper()
	select {
	case <-g.exited:
		g.t.Fatalf("gateway %s exited: %v\n%s", g.name, g.err, logTail(g.log, 50))
	default:
	}
	for _, s := range []string{"gateway: ALARM:", "did not start"} {
		if logHas(g.log, s) {
			g.t.Fatalf("gateway %s logged %q\n%s", g.name, s, logTail(g.log, 50))
		}
	}
}

// stop sends SIGTERM and requires a clean exit.
func (g *gatewayProc) stop() {
	g.t.Helper()
	if err := g.cmd.Process.Signal(syscall.SIGTERM); err != nil {
		g.t.Fatalf("signal gateway %s: %v", g.name, err)
	}
	select {
	case <-g.exited:
	case <-time.After(30 * time.Second):
		g.t.Fatalf("gateway %s did not exit within 30s of SIGTERM\n%s", g.name, logTail(g.log, 50))
	}
	if g.err != nil {
		g.t.Fatalf("gateway %s exited with an error after SIGTERM: %v\n%s", g.name, g.err, logTail(g.log, 50))
	}
	for i, o := range g.d.gws {
		if o == g {
			g.d.gws = append(g.d.gws[:i], g.d.gws[i+1:]...)
			break
		}
	}
}

// post sends body to the gateway's JSON-RPC endpoint and returns the
// response body.
func (g *gatewayProc) post(body string) []byte {
	g.t.Helper()
	resp, err := gatewayClient.Post(g.url+"/", "application/json", strings.NewReader(body))
	if err != nil {
		g.t.Fatalf("POST to gateway %s: %v", g.name, err)
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		g.t.Fatalf("read the response of gateway %s: %v", g.name, err)
	}
	if resp.StatusCode != http.StatusOK {
		g.t.Fatalf("gateway %s answered status %d: %s", g.name, resp.StatusCode, cut(b))
	}
	return b
}

// call sends one JSON-RPC request and decodes its result into out. It
// returns the error member when the gateway answers one.
func (g *gatewayProc) call(method, params string, out any) *rpcError {
	g.t.Helper()
	rerr, err := decodeRPC(g.post(rpcRequest(method, params)), out)
	if err != nil {
		g.t.Fatalf("gateway %s %s %s: %v", g.name, method, params, err)
	}
	return rerr
}

// get sends a GET request for path and returns the status and body.
func (g *gatewayProc) get(path string) (status int, body []byte) {
	g.t.Helper()
	resp, err := gatewayClient.Get(g.url + path)
	if err != nil {
		g.t.Fatalf("GET %s from gateway %s: %v", path, g.name, err)
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		g.t.Fatalf("read GET %s from gateway %s: %v", path, g.name, err)
	}
	return resp.StatusCode, b
}

// waitReceipt waits until the gateway serves the receipt of send at tag
// want and returns it.
func (d *devnet) waitReceipt(g *gatewayProc, send types.Hash, want string, timeout time.Duration) receiptView {
	d.t.Helper()
	var out receiptView
	d.waitFor(fmt.Sprintf("the receipt of %s at tag %s", send, want), timeout, func() (bool, string, error) {
		var rv receiptView
		if rerr := g.call("zeno_getReceipt", fmt.Sprintf(`["%s"]`, send), &rv); rerr != nil {
			if rerr.Code == -32004 {
				return false, "no receipt yet", nil
			}
			return false, fmt.Sprintf("rpc error %d: %s", rerr.Code, rerr.Message), fmt.Errorf("rpc error %d: %s", rerr.Code, rerr.Message)
		}
		done, err := checkReceipt(rv, want)
		if err != nil {
			d.t.Fatalf("receipt of %s: %v", send, err)
		}
		if done {
			out = rv
		}
		return done, fmt.Sprintf("receipt %+v", rv), nil
	})
	return out
}

// waitHeadOK waits until the gateway reports domain 1 with status OK.
func (d *devnet) waitHeadOK(g *gatewayProc, timeout time.Duration) {
	d.t.Helper()
	d.waitFor(fmt.Sprintf("gateway %s head OK", g.name), timeout, func() (bool, string, error) {
		var hv headView
		if rerr := g.call("zeno_getHead", `[1]`, &hv); rerr != nil {
			return false, fmt.Sprintf("rpc error %d: %s", rerr.Code, rerr.Message), nil
		}
		return hv.Status == "OK", "status " + hv.Status, nil
	})
}

// compareGateways requires the two gateways to answer the same requests
// with the same bytes, and the remote one to serve no pending batch.
func (d *devnet) compareGateways(local, remote *gatewayProc, last uint64) {
	d.t.Helper()
	d.waitFor("both gateways at the last batch", momenta(confirmationDepth+10), func() (bool, string, error) {
		var sum []string
		done := true
		for _, g := range []*gatewayProc{local, remote} {
			var hv headView
			if rerr := g.call("zeno_getHead", `[1]`, &hv); rerr != nil {
				return false, fmt.Sprintf("%s: rpc error %d: %s", g.name, rerr.Code, rerr.Message), nil
			}
			ok := hv.Status == "OK" && hv.Latest != nil && hv.Latest.BatchID == last && hv.Finalized != nil && hv.Finalized.BatchID == last
			done = done && ok
			sum = append(sum, fmt.Sprintf("%s: status %s, latest %s, finalized %s", g.name, hv.Status, batchOf(hv.Latest), batchOf(hv.Finalized)))
		}
		return done, strings.Join(sum, "; "), nil
	})

	batches := make([]batchView, last+1)
	for id := uint64(0); id <= last; id++ {
		if rerr := local.call("zeno_getBatch", fmt.Sprintf(`[1,%d]`, id), &batches[id]); rerr != nil {
			d.t.Fatalf("local zeno_getBatch %d: rpc error %d: %s", id, rerr.Code, rerr.Message)
		}
	}
	for _, body := range compareBodies(last, batches) {
		lb := local.post(body)
		var raw json.RawMessage
		if rerr, err := decodeRPC(lb, &raw); rerr != nil || err != nil {
			d.t.Fatalf("%s\nlocal did not answer a result (error member %+v, decode error %v): %s", body, rerr, err, cut(lb))
		}
		rb := remote.post(body)
		if !bytes.Equal(lb, rb) {
			d.t.Fatalf("%s\nlocal:  %s\nremote: %s", body, cut(lb), cut(rb))
		}
	}
	for _, b := range batches {
		for _, prefix := range []string{"/bundles/", "/receipts/"} {
			ls, lb := local.get(prefix + b.DAHash)
			rs, rb := remote.get(prefix + b.DAHash)
			if ls != http.StatusOK || rs != http.StatusOK || !bytes.Equal(lb, rb) {
				d.t.Fatalf("GET %s%s: local status %d, remote status %d, equal %v", prefix, b.DAHash, ls, rs, bytes.Equal(lb, rb))
			}
		}
	}

	var heads [2][]byte
	for i, g := range []*gatewayProc{local, remote} {
		var raw json.RawMessage
		if rerr := g.call("zeno_getHead", `[1]`, &raw); rerr != nil {
			d.t.Fatalf("%s zeno_getHead: rpc error %d: %s", g.name, rerr.Code, rerr.Message)
		}
		heads[i] = raw
	}
	_, lrest, err := headWithoutPending(heads[0])
	if err != nil {
		d.t.Fatalf("local head: %v", err)
	}
	rpending, rrest, err := headWithoutPending(heads[1])
	if err != nil {
		d.t.Fatalf("remote head: %v", err)
	}
	if string(rpending) != "null" {
		d.t.Fatalf("remote pending = %s, want null", rpending)
	}
	lb, _ := json.Marshal(lrest)
	rb, _ := json.Marshal(rrest)
	if !bytes.Equal(lb, rb) {
		d.t.Fatalf("zeno_getHead differs:\nlocal:  %s\nremote: %s", lb, rb)
	}
	var raw json.RawMessage
	if rerr := remote.call("zeno_getBatch", `[1,"pending"]`, &raw); rerr == nil || rerr.Code != -32002 {
		d.t.Fatalf("remote zeno_getBatch pending answered error %+v with result %s, want -32002", rerr, raw)
	}
}

// TestGatewayHelpers checks the pure helpers of the gateway checks. It
// starts no process and opens no connection.
func TestGatewayHelpers(t *testing.T) {
	admin := adminAddress.String()
	hash := func(c byte) string { return strings.Repeat(string("0123456789abcdef"[c]), 64) }

	t.Run("listen line", func(t *testing.T) {
		in := "2026/10/15 09:41:07 gateway: simulation enforcement: limits off, uids off\n" +
			"2026/10/15 09:41:07 gateway: serving JSON-RPC on http://127.0.0.1:53211\n"
		if u, ok := listenURL(in); !ok || u != "http://127.0.0.1:53211" {
			t.Fatalf("listenURL = %q, %v", u, ok)
		}
		if u, ok := listenURL(strings.TrimSuffix(in, "\n")); ok {
			t.Fatalf("listenURL of a line without its newline = %q, true", u)
		}
		if u, ok := listenURL("2026/10/15 09:41:07 gateway: simulation enforcement: limits off, uids off\n"); ok {
			t.Fatalf("listenURL without the marker = %q, true", u)
		}
	})

	t.Run("receipt tags", func(t *testing.T) {
		mk := func(tag string, verified bool, status, sender string) receiptView {
			var r receiptView
			r.Tag, r.Verified, r.Sender, r.Receipt.Status = tag, verified, sender, status
			return r
		}
		const failed = "VALIDATION_FAILED"
		for _, c := range []struct {
			name    string
			want    string
			got     receiptView
			done    bool
			errPart string
		}{
			{"pending", "pending", mk("pending", false, failed, admin), true, ""},
			{"latest", "latest", mk("latest", true, failed, admin), true, ""},
			{"finalized", "finalized", mk("finalized", true, failed, admin), true, ""},
			{"not yet latest", "latest", mk("pending", false, failed, admin), false, ""},
			{"not yet finalized", "finalized", mk("latest", true, failed, admin), false, ""},
			{"skipped pending", "pending", mk("latest", true, failed, admin), false, "skipped tag `pending`"},
			{"skipped latest", "latest", mk("finalized", true, failed, admin), false, "skipped tag `latest`"},
			{"pending verified", "pending", mk("pending", true, failed, admin), false, "verified"},
			{"latest unverified", "latest", mk("latest", false, failed, admin), false, "verified"},
			{"status", "latest", mk("latest", true, "SUCCESS", admin), false, "status"},
			{"sender", "latest", mk("latest", true, failed, "z1qp3yph55qgresyytz83anynr2f4z39x2z3ej3e"), false, "sender"},
			{"empty tag", "latest", mk("", true, failed, admin), false, "tag"},
		} {
			done, err := checkReceipt(c.got, c.want)
			if done != c.done || (c.errPart == "") != (err == nil) || (err != nil && !strings.Contains(err.Error(), c.errPart)) {
				t.Errorf("%s: checkReceipt = %v, %v; want done %v, error containing %q", c.name, done, err, c.done, c.errPart)
			}
		}
	})

	t.Run("rpc envelope", func(t *testing.T) {
		receipt := `{"inputIndex":4,"inputHash":"` + hash(1) + `","batchId":2,"contractId":"` + hash(2) + `","codeHash":"` + hash(3) +
			`","status":"VALIDATION_FAILED","gasUsed":0,"abortCode":0,"stateDiffHash":"` + hash(4) + `","returnHash":"` + hash(5) +
			`","eventRoot":"` + hash(6) + `","eventCount":0,"outboxRoot":"` + hash(7) + `","outboxCount":0}`
		result := `{"domainId":1,"batchId":2,"verified":true,"tag":"latest","globalInputIndex":4,"sendHash":"` + hash(8) +
			`","sender":"` + admin + `","receipt":` + receipt + `,"returnData":"","events":[],"outbox":[],"input":null}`
		var rv receiptView
		rerr, err := decodeRPC([]byte(`{"jsonrpc":"2.0","id":1,"result":`+result+`}`), &rv)
		if rerr != nil || err != nil {
			t.Fatalf("decodeRPC = %+v, %v", rerr, err)
		}
		if rv.BatchID != 2 || !rv.Verified || rv.Tag != "latest" || rv.Sender != admin || rv.Receipt.Status != "VALIDATION_FAILED" {
			t.Fatalf("receiptView = %+v", rv)
		}
		var raw json.RawMessage
		rerr, err = decodeRPC([]byte(`{"jsonrpc":"2.0","id":1,"error":{"code":-32004,"message":"not found"}}`), &raw)
		if err != nil || rerr == nil || rerr.Code != -32004 {
			t.Fatalf("decodeRPC of an error = %+v, %v", rerr, err)
		}
		for _, bad := range []string{"not json", `{"jsonrpc":"2.0","id":1}`} {
			if rerr, err := decodeRPC([]byte(bad), &raw); err == nil {
				t.Fatalf("decodeRPC(%q) = %+v, nil", bad, rerr)
			}
		}
		var bv batchView
		batch := `{"domainId":1,"batchId":2,"verified":true,"daHash":"` + hash(9) + `","inputs":[{"globalInputIndex":4,"sendHash":"` +
			hash(10) + `","status":"VALIDATION_FAILED"},{"globalInputIndex":5,"sendHash":"` + hash(11) + `","status":"SUCCESS"}]}`
		if rerr, err := decodeRPC([]byte(`{"jsonrpc":"2.0","id":1,"result":`+batch+`}`), &bv); rerr != nil || err != nil {
			t.Fatalf("decodeRPC of a batch = %+v, %v", rerr, err)
		}
		if bv.DAHash != hash(9) || len(bv.Inputs) != 2 || bv.Inputs[0].SendHash != hash(10) || bv.Inputs[1].SendHash != hash(11) {
			t.Fatalf("batchView = %+v", bv)
		}
		var hv headView
		head := `{"domainId":1,"status":"OK","inconsistentAt":null,"pending":null,"latest":{"batchId":3,"verified":true},"finalized":{"batchId":2,"verified":true}}`
		if rerr, err := decodeRPC([]byte(`{"jsonrpc":"2.0","id":1,"result":`+head+`}`), &hv); rerr != nil || err != nil {
			t.Fatalf("decodeRPC of a head = %+v, %v", rerr, err)
		}
		if hv.Status != "OK" || hv.Latest == nil || hv.Latest.BatchID != 3 || hv.Finalized == nil || hv.Finalized.BatchID != 2 {
			t.Fatalf("headView = %+v", hv)
		}
	})

	t.Run("send log", func(t *testing.T) {
		want := expectedLog(1)
		exp := []logWant{{0, 2, ""}, {0, 2, supervisor.OutcomeAccepted}, {1, 3, ""}, {1, 3, supervisor.OutcomeAccepted}}
		if len(want) != len(exp) {
			t.Fatalf("expectedLog(1) = %+v", want)
		}
		for i := range exp {
			if want[i] != exp[i] {
				t.Fatalf("expectedLog(1)[%d] = %+v, want %+v", i, want[i], exp[i])
			}
		}
		for last, n := range map[uint64]int{2: 6, 3: 8} {
			w := expectedLog(last)
			if len(w) != n {
				t.Fatalf("expectedLog(%d) has %d entries, want %d", last, len(w), n)
			}
			for b := uint64(0); b <= last; b++ {
				if w[2*b] != (logWant{b, 2 + b, ""}) {
					t.Fatalf("expectedLog(%d)[%d] = %+v", last, 2*b, w[2*b])
				}
			}
		}
		build := func() []supervisor.SendRecord {
			var r []supervisor.SendRecord
			for i, w := range expectedLog(2) {
				rec := supervisor.SendRecord{BatchID: w.batch, Height: w.height, Outcome: w.outcome}
				rec.Hash[0] = byte(i/2 + 1)
				r = append(r, rec)
			}
			return r
		}
		w := expectedLog(2)
		if err := checkLog(build(), w); err != nil {
			t.Fatalf("checkLog of a matching log: %v", err)
		}
		r := build()
		r[2].Height++
		r[3].Height++
		if checkLog(r, w) == nil {
			t.Error("checkLog accepted a wrong height")
		}
		r = build()
		r[1].Outcome = supervisor.OutcomeReceived
		if checkLog(r, w) == nil {
			t.Error("checkLog accepted a wrong outcome")
		}
		r = build()
		r[3].Hash[1] = 9
		if checkLog(r, w) == nil {
			t.Error("checkLog accepted an outcome with another hash")
		}
		if checkLog(build()[:5], w) == nil {
			t.Error("checkLog accepted a log with a record missing")
		}
	})

	t.Run("comparison bodies", func(t *testing.T) {
		mk := func(da byte, sends ...byte) batchView {
			var b batchView
			b.DAHash = hash(da)
			for _, s := range sends {
				b.Inputs = append(b.Inputs, struct {
					SendHash string `json:"sendHash"`
				}{hash(s)})
			}
			return b
		}
		batches := []batchView{mk(1, 2), mk(3, 4, 5), mk(6, 7)}
		bodies := compareBodies(2, batches)
		if len(bodies) != 23 {
			t.Fatalf("compareBodies returned %d bodies, want 23", len(bodies))
		}
		for _, body := range bodies {
			var m struct {
				JSONRPC string          `json:"jsonrpc"`
				ID      int             `json:"id"`
				Method  string          `json:"method"`
				Params  json.RawMessage `json:"params"`
			}
			if err := json.Unmarshal([]byte(body), &m); err != nil {
				t.Fatalf("body %s is not JSON: %v", body, err)
			}
			var params []json.RawMessage
			if m.JSONRPC != "2.0" || m.ID != 1 || m.Method == "" || json.Unmarshal(m.Params, &params) != nil {
				t.Fatalf("body %s is not a JSON-RPC request with array params", body)
			}
		}
		count := func(s string) int {
			n := 0
			for _, body := range bodies {
				if strings.Contains(body, s) {
					n++
				}
			}
			return n
		}
		for _, b := range batches {
			if n := count(b.DAHash); n != 2 {
				t.Errorf("daHash %s appears in %d bodies, want 2", b.DAHash, n)
			}
			for _, in := range b.Inputs {
				if n := count(in.SendHash); n != 1 {
					t.Errorf("sendHash %s appears in %d bodies, want 1", in.SendHash, n)
				}
			}
		}
		if count(`"zeno_chainInfo"`) != 1 {
			t.Error("the bodies lack zeno_chainInfo")
		}
	})

	t.Run("head without pending", func(t *testing.T) {
		head := func(status, pending string) []byte {
			return []byte(`{"domainId":1,"status":"` + status + `","pending":` + pending + `,"latest":{"batchId":3},"finalized":{"batchId":2}}`)
		}
		marshal := func(b []byte) (json.RawMessage, string) {
			p, rest, err := headWithoutPending(b)
			if err != nil {
				t.Fatal(err)
			}
			out, _ := json.Marshal(rest)
			return p, string(out)
		}
		_, a := marshal(head("OK", `{"batchId":4}`))
		p, b := marshal(head("OK", "null"))
		if a != b {
			t.Fatalf("heads that differ only in pending differ: %s, %s", a, b)
		}
		if string(p) != "null" {
			t.Fatalf("pending = %s, want null", p)
		}
		if _, c := marshal(head("SYNCING", "null")); c == b {
			t.Fatal("heads that differ in status are equal")
		}
		if _, _, err := headWithoutPending([]byte("[]")); err == nil {
			t.Fatal("headWithoutPending accepted a non-object")
		}
	})
}
