package state

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/zenon-network/go-zenon/common/trie"
	"github.com/zenon-network/go-zenon/common/types"

	"github.com/digitalSloth/go-executor/domain"
)

func snapPath(b byte) trie.Path {
	var p trie.Path
	p[0] = b
	return p
}

func newSnapStore(t *testing.T, entries ...domain.StateDiffEntry) *Store {
	t.Helper()
	s := OpenEmpty()
	t.Cleanup(func() { s.Close() })
	if len(entries) > 0 {
		if _, err := s.Apply(entries); err != nil {
			t.Fatalf("Apply: %v", err)
		}
	}
	return s
}

func mustRoot(t *testing.T, s *Store) types.Hash {
	t.Helper()
	r, err := s.Root()
	if err != nil {
		t.Fatalf("Root: %v", err)
	}
	return r
}

func TestSnapshotSaveListOpen(t *testing.T) {
	s := newSnapStore(t,
		domain.StateDiffEntry{Path: snapPath(1), Value: []byte("one")},
		domain.StateDiffEntry{Path: snapPath(2), Value: []byte("two")})
	dir := t.TempDir()
	stf := types.Hash{9}
	hdr := SnapshotHeader{InputSeq: 7, STFSpecHash: stf, Meta: []byte("meta-bytes")}

	name, err := s.SaveSnapshot(dir, hdr)
	if err != nil {
		t.Fatalf("SaveSnapshot: %v", err)
	}
	infos, err := ListSnapshots(dir)
	if err != nil || len(infos) != 1 {
		t.Fatalf("ListSnapshots = %v, %v", infos, err)
	}
	if infos[0].Name != name || infos[0].Header.InputSeq != 7 || infos[0].Header.STFSpecHash != stf ||
		!bytes.Equal(infos[0].Header.Meta, hdr.Meta) {
		t.Fatalf("listed header = %+v", infos[0])
	}

	reopened, err := OpenSnapshot(dir, name, stf)
	if err != nil {
		t.Fatalf("OpenSnapshot: %v", err)
	}
	defer reopened.Close()
	if mustRoot(t, reopened) != mustRoot(t, s) {
		t.Fatalf("reopened root differs from the original")
	}
}

func TestSnapshotPresentEmptyLeaf(t *testing.T) {
	present := newSnapStore(t, domain.StateDiffEntry{Path: snapPath(1), Value: []byte{}})
	absent := newSnapStore(t)
	dir := t.TempDir()
	stf := types.Hash{1}

	name, err := present.SaveSnapshot(dir, SnapshotHeader{STFSpecHash: stf})
	if err != nil {
		t.Fatalf("SaveSnapshot: %v", err)
	}
	reopened, err := OpenSnapshot(dir, name, stf)
	if err != nil {
		t.Fatalf("OpenSnapshot: %v", err)
	}
	defer reopened.Close()

	if mustRoot(t, reopened) != mustRoot(t, present) {
		t.Fatalf("present-empty leaf did not survive the round trip")
	}
	if mustRoot(t, reopened) == mustRoot(t, absent) {
		t.Fatalf("present-empty leaf is indistinguishable from an absent path")
	}
}

func TestSnapshotDeterministicName(t *testing.T) {
	a := newSnapStore(t,
		domain.StateDiffEntry{Path: snapPath(1), Value: []byte("a")},
		domain.StateDiffEntry{Path: snapPath(2), Value: []byte("b")})
	b := newSnapStore(t,
		domain.StateDiffEntry{Path: snapPath(2), Value: []byte("b")},
		domain.StateDiffEntry{Path: snapPath(1), Value: []byte("a")})
	hdr := SnapshotHeader{InputSeq: 3, STFSpecHash: types.Hash{4}}

	n1, err := a.SaveSnapshot(t.TempDir(), hdr)
	if err != nil {
		t.Fatal(err)
	}
	n2, err := b.SaveSnapshot(t.TempDir(), hdr)
	if err != nil {
		t.Fatal(err)
	}
	if n1 != n2 {
		t.Fatalf("names differ: %s vs %s", n1, n2)
	}
}

func TestOpenSnapshotWrongSTFSpecHash(t *testing.T) {
	s := newSnapStore(t, domain.StateDiffEntry{Path: snapPath(1), Value: []byte("a")})
	dir := t.TempDir()
	name, err := s.SaveSnapshot(dir, SnapshotHeader{STFSpecHash: types.Hash{1}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := OpenSnapshot(dir, name, types.Hash{2}); err == nil {
		t.Fatalf("OpenSnapshot with another stfSpecHash succeeded")
	}
	if infos, err := ListSnapshots(dir); err != nil || len(infos) != 1 {
		t.Fatalf("ListSnapshots = %v, %v; want the file still listed", infos, err)
	}
}

func TestSnapshotFlippedByte(t *testing.T) {
	s := newSnapStore(t, domain.StateDiffEntry{Path: snapPath(1), Value: []byte("abc")})
	dir := t.TempDir()
	stf := types.Hash{1}
	name, err := s.SaveSnapshot(dir, SnapshotHeader{STFSpecHash: stf})
	if err != nil {
		t.Fatal(err)
	}
	file := filepath.Join(dir, name+snapshotExt)
	body, err := os.ReadFile(file)
	if err != nil {
		t.Fatal(err)
	}
	body[len(body)-1] ^= 0xFF
	if err := os.WriteFile(file, body, 0o644); err != nil {
		t.Fatal(err)
	}

	if _, err := OpenSnapshot(dir, name, stf); err == nil {
		t.Fatalf("OpenSnapshot of a corrupted file succeeded")
	}
	if infos, err := ListSnapshots(dir); err != nil || len(infos) != 0 {
		t.Fatalf("ListSnapshots = %v, %v; want the corrupted file skipped", infos, err)
	}
}

func TestListSnapshotsMissingDir(t *testing.T) {
	infos, err := ListSnapshots(filepath.Join(t.TempDir(), "absent"))
	if err != nil || len(infos) != 0 {
		t.Fatalf("ListSnapshots = %v, %v", infos, err)
	}
}
