package supervisor

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log"
	"os"
	"path/filepath"

	"github.com/zenon-network/go-zenon/common/types"
	"github.com/zenon-network/go-zenon/vm/embedded/definition"
)

// maxSendLogBytes is the size at which the submission log is rotated before its next send.
const maxSendLogBytes = 16 << 20

// sendLogVersion is the log-format version this build writes and reads in full.
const sendLogVersion = 1

// logLine is one line of the submission log. Every log-format version keeps version,
// domainId, batchId, hash and outcome with their version 1 meaning, so replay can place
// any line; a line may carry fields this build does not know, and replay ignores them.
type logLine struct {
	Version uint32 `json:"version"`
	SendRecord
}

// fileLog is a sendLog that appends each record to a file as one JSON line and fsyncs it before
// returning. Only the submitter goroutine calls Sent and Resolved; Last is safe from any goroutine.
type fileLog struct {
	mem        memLog            // the latest send and its outcome, as replayed and written so far
	accepted   map[uint32]uint64 // each domain's batchId in its latest replayed readable "accepted" line; filled by openSendLog only
	unreadable error             // why this build cannot read Last()'s record, when it has no outcome; nil otherwise; set by openSendLog only
	path       string
	f          *os.File // opened O_RDWR|O_APPEND
	size       int64    // bytes in the current file
	limit      int64    // maxSendLogBytes; tests lower it
	err        error    // the first write, sync or rotation error; nil until one
}

// openSendLog replays path+".1", when present, then path, which it opens, creating it with mode
// 0600. It truncates a torn last line of path and fsyncs the file and its directory.
func openSendLog(path string) (*fileLog, error) {
	l := &fileLog{accepted: make(map[uint32]uint64), path: path, limit: maxSendLogBytes}
	fail := func(err error) (*fileLog, error) {
		if l.f != nil {
			l.f.Close()
		}
		return nil, fmt.Errorf("submission log %s: %w", path, err)
	}

	old, err := os.ReadFile(path + ".1")
	switch {
	case errors.Is(err, fs.ErrNotExist):
	case err != nil:
		return fail(err)
	default:
		// Bytes after the last newline are ignored: the file is never written again.
		if err := l.replay(path+".1", old[:bytes.LastIndexByte(old, '\n')+1]); err != nil {
			return nil, err
		}
	}

	l.f, err = os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0o600)
	if err != nil {
		return fail(err)
	}
	data, err := io.ReadAll(l.f)
	if err != nil {
		return fail(err)
	}

	// Bytes after the last newline belong to a write that never returned. A last complete line
	// holding a NUL byte is torn too: a power loss can persist a line's second page, newline
	// included, and not its first, and json.Marshal never writes a raw NUL.
	end := bytes.LastIndexByte(data, '\n') + 1
	if end > 0 {
		s := bytes.LastIndexByte(data[:end-1], '\n') + 1
		if bytes.IndexByte(data[s:end], 0) >= 0 {
			end = s
		}
	}
	if err := l.replay(path, data[:end]); err != nil {
		l.f.Close()
		return nil, err
	}
	if end < len(data) {
		if err := l.f.Truncate(int64(end)); err != nil {
			return fail(err)
		}
		if err := l.f.Sync(); err != nil {
			return fail(err)
		}
		log.Printf("supervisor: WARN: submission log %s: dropped an incomplete last line of %d bytes", path, len(data)-end)
	}
	if err := syncDir(filepath.Dir(path)); err != nil {
		return fail(err)
	}
	l.size = int64(end)
	return l, nil
}

// replay feeds every line of data, which ends at a newline or is empty, into l.mem, l.accepted
// and l.unreadable. name is the file data came from, for errors.
func (l *fileLog) replay(name string, data []byte) error {
	if len(data) == 0 {
		return nil
	}
	lines := bytes.Split(data[:len(data)-1], []byte{'\n'})
	for i, line := range lines {
		r, why, err := decodeSendRecord(line)
		if err != nil {
			remedy := "moving the file aside starts a new log, which forgets any send it has not resolved"
			if name != l.path {
				remedy = "moving the file aside forgets the accepted batches it records, which only seed each domain's observed nextBatchId"
			}
			return fmt.Errorf("submission log %s line %d is damaged: %w; %s", name, i+1, err, remedy)
		}
		if r.Outcome == "" {
			l.mem.Sent(r)
			l.unreadable = nil
			if why != nil {
				l.unreadable = fmt.Errorf("submission log %s line %d: %w", name, i+1, why)
			}
			continue
		}
		l.mem.Resolved(r)
		if last, _ := l.mem.Last(); last == r {
			l.unreadable = nil
			if why == nil && r.Outcome == OutcomeAccepted {
				l.accepted[r.DomainID] = r.BatchID
			}
		}
	}
	return nil
}

// decodeSendRecord decodes one log line, without its newline. err is damage: the line's envelope
// does not decode. Otherwise unreadable is nil and r is the whole record, or unreadable says why
// this build cannot read the line and r holds only the envelope's fields.
func decodeSendRecord(line []byte) (r SendRecord, unreadable error, err error) {
	var env struct {
		Version  uint32     `json:"version"`
		DomainID uint32     `json:"domainId"`
		BatchID  uint64     `json:"batchId"`
		Hash     types.Hash `json:"hash"`
		Outcome  Outcome    `json:"outcome"`
	}
	dec := json.NewDecoder(bytes.NewReader(line))
	if err := dec.Decode(&env); err != nil {
		return SendRecord{}, nil, err
	}
	var trailing json.RawMessage
	if err := dec.Decode(&trailing); err != io.EOF {
		if err == nil {
			return SendRecord{}, nil, errors.New("unexpected value after the record")
		}
		return SendRecord{}, nil, err
	}
	if env.Version == 0 {
		return SendRecord{}, nil, errors.New("no log-format version")
	}
	if env.Hash == types.ZeroHash {
		return SendRecord{}, nil, errors.New("no block hash")
	}
	r = SendRecord{DomainID: env.DomainID, BatchID: env.BatchID, Hash: env.Hash, Outcome: env.Outcome}

	if env.Version != sendLogVersion {
		return r, fmt.Errorf("log-format version %d is not %d", env.Version, sendLogVersion), nil
	}
	var ll logLine
	if err := json.Unmarshal(line, &ll); err != nil {
		return r, err, nil
	}
	switch {
	case ll.Method != definition.SettlementSubmitBatchMethodName:
		return r, fmt.Errorf("method %q is not %s", ll.Method, definition.SettlementSubmitBatchMethodName), nil
	case ll.Height == 0:
		return r, errors.New("account height 0"), nil
	case ll.DataHash == types.ZeroHash:
		return r, errors.New("no data hash"), nil
	}
	return ll.SendRecord, nil, nil
}

// syncDir fsyncs dir itself, which makes a file created or renamed inside it durable.
func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}

// append writes r as one line and fsyncs it. A failure is kept in l.err.
func (l *fileLog) append(r SendRecord) error {
	b, err := json.Marshal(logLine{Version: sendLogVersion, SendRecord: r})
	if err == nil {
		b = append(b, '\n')
		if _, err = l.f.Write(b); err == nil {
			err = l.f.Sync()
		}
	}
	if err != nil {
		l.err = fmt.Errorf("submission log %s: %w", l.path, err)
		return l.err
	}
	l.size += int64(len(b))
	return nil
}

// rotate renames the current file to path+".1", replacing any older one, and starts a new file.
func (l *fileLog) rotate() error {
	var nf *os.File
	err := os.Rename(l.path, l.path+".1")
	if err == nil {
		nf, err = os.OpenFile(l.path, os.O_RDWR|os.O_CREATE|os.O_EXCL|os.O_APPEND, 0o600)
	}
	if err == nil {
		err = syncDir(filepath.Dir(l.path))
	}
	if err != nil {
		if nf != nil {
			nf.Close()
		}
		l.err = fmt.Errorf("submission log %s: rotating: %w", l.path, err)
		return l.err
	}
	l.f.Close()
	l.f, l.size = nf, 0
	return nil
}

// Sent writes r before it is published. It rotates a full log first.
func (l *fileLog) Sent(r SendRecord) error {
	if l.err != nil {
		return l.err
	}
	if l.size >= l.limit {
		if err := l.rotate(); err != nil {
			return err
		}
	}
	if err := l.append(r); err != nil {
		return err
	}
	return l.mem.Sent(r)
}

// Resolved records r's outcome in memory first, so a failed write still shows it in the status.
func (l *fileLog) Resolved(r SendRecord) error {
	l.mem.Resolved(r)
	if l.err != nil {
		return l.err
	}
	return l.append(r)
}

func (l *fileLog) Last() (SendRecord, bool) { return l.mem.Last() }

func (l *fileLog) Close() error { return l.f.Close() }
