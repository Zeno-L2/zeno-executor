package gateway

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/zenon-network/go-zenon/common/types"
	"github.com/zenon-network/go-zenon/vm/embedded/definition"

	"github.com/Zeno-L2/zeno-executor/internal/recovery"
	"github.com/Zeno-L2/zeno-executor/internal/state"
)

func noLog(string, ...any) {}

func survives(t *testing.T, path, want string) {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil || string(b) != want {
		t.Fatalf("%s = %q, %v; want %q untouched", path, b, err, want)
	}
}

func TestOpenFinalRefusesSymlinkedDomainDir(t *testing.T) {
	storage := t.TempDir()
	other := t.TempDir()
	for _, f := range []string{"snapshots/a", "index/b"} {
		mkfile(t, filepath.Join(other, f))
	}
	if err := os.Symlink(other, filepath.Join(storage, "1")); err != nil {
		t.Fatal(err)
	}
	if _, _, err := openFinal(context.Background(), storage, 1, nil, readTimeout, noLog); err == nil || !strings.Contains(err.Error(), "symlink") {
		t.Fatalf("openFinal = %v, want a symlink refusal", err)
	}
	for _, f := range []string{"snapshots/a", "index/b"} {
		survives(t, filepath.Join(other, f), "x")
	}
}

func TestOpenFinalRefusesSymlinkedSnapshots(t *testing.T) {
	storage := t.TempDir()
	other := t.TempDir()
	mkfile(t, filepath.Join(other, "keep"))
	if err := os.MkdirAll(filepath.Join(storage, "1"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(other, filepath.Join(storage, "1", "snapshots")); err != nil {
		t.Fatal(err)
	}
	if _, _, err := openFinal(context.Background(), storage, 1, nil, readTimeout, noLog); err == nil || !strings.Contains(err.Error(), "symlink") {
		t.Fatalf("openFinal = %v, want a symlink refusal", err)
	}
	survives(t, filepath.Join(other, "keep"), "x")
}

func TestWriteSnapshotRefusesSymlinkedSnapshots(t *testing.T) {
	storage := t.TempDir()
	other := t.TempDir()
	mkfile(t, filepath.Join(other, "keep"))
	if err := os.MkdirAll(filepath.Join(storage, "1"), 0o700); err != nil {
		t.Fatal(err)
	}
	dir := filepath.Join(storage, "1", "snapshots")
	if err := os.Symlink(other, dir); err != nil {
		t.Fatal(err)
	}
	store := state.OpenEmpty()
	defer store.Close()
	if err := writeSnapshot(store, dir, BatchHead{}, types.Hash{}); err == nil || !strings.Contains(err.Error(), "symlink") {
		t.Fatalf("writeSnapshot = %v, want a symlink refusal", err)
	}
	survives(t, filepath.Join(other, "keep"), "x")
}

func TestOpenFinalRefusesForeignOwner(t *testing.T) {
	if os.Geteuid() != 0 {
		t.Skip("changing a directory's owner needs root")
	}
	storage := t.TempDir()
	if err := os.MkdirAll(filepath.Join(storage, "1", "index"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Chown(filepath.Join(storage, "1", "index"), 12345, 12345); err != nil {
		t.Fatal(err)
	}
	if _, _, err := openFinal(context.Background(), storage, 1, nil, readTimeout, noLog); err == nil || !strings.Contains(err.Error(), "owned by uid") {
		t.Fatalf("openFinal = %v, want an ownership refusal", err)
	}
}

func TestOpenFinalRefusesSymlinkedStoragePath(t *testing.T) {
	real := t.TempDir()
	mkfile(t, filepath.Join(real, "1", "snapshots", "a"))
	link := filepath.Join(t.TempDir(), "gw")
	if err := os.Symlink(real, link); err != nil {
		t.Fatal(err)
	}
	if _, _, err := openFinal(context.Background(), link, 1, nil, readTimeout, noLog); err == nil || !strings.Contains(err.Error(), "symlink") {
		t.Fatalf("openFinal = %v, want a symlink refusal", err)
	}
	survives(t, filepath.Join(real, "1", "snapshots", "a"), "x")
}

// The root directory is owned by root, so a non-root process sees a
// directory owned by another uid without needing chown rights.
func TestCheckOwnedDirForeignOwner(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("the root directory is owned by the effective uid")
	}
	if err := checkOwnedDir("/"); err == nil || !strings.Contains(err.Error(), "owned by uid") {
		t.Fatalf("checkOwnedDir(/) = %v, want an ownership refusal", err)
	}
	if _, err := PrepareStorage("/"); err == nil || !strings.Contains(err.Error(), "owned by uid") {
		t.Fatalf("PrepareStorage(/) = %v, want an ownership refusal", err)
	}
}

func TestPrepareStorage(t *testing.T) {
	t.Run("creates and tightens", func(t *testing.T) {
		dir := filepath.Join(t.TempDir(), "gw")
		if err := os.Mkdir(dir, 0o777); err != nil {
			t.Fatal(err)
		}
		if _, err := PrepareStorage(dir); err != nil {
			t.Fatal(err)
		}
		fi, err := os.Stat(dir)
		if err != nil || fi.Mode().Perm() != 0o700 {
			t.Fatalf("mode = %v, %v; want 0700", fi.Mode().Perm(), err)
		}
	})
	t.Run("symlink", func(t *testing.T) {
		real := t.TempDir()
		link := filepath.Join(t.TempDir(), "gw")
		if err := os.Symlink(real, link); err != nil {
			t.Fatal(err)
		}
		if _, err := PrepareStorage(link); err == nil || !strings.Contains(err.Error(), "symlink") {
			t.Fatalf("PrepareStorage = %v, want a symlink refusal", err)
		}
	})
	t.Run("not a directory", func(t *testing.T) {
		f := filepath.Join(t.TempDir(), "gw")
		mkfile(t, f)
		if _, err := PrepareStorage(f); err == nil {
			t.Fatal("PrepareStorage accepted a file")
		}
	})
}

func TestOpenFinalRefusesExecutorMarkers(t *testing.T) {
	for _, m := range []string{"batches", "cursor", "meta.json", "momentum_hashes"} {
		t.Run(m, func(t *testing.T) {
			storage := t.TempDir()
			snap := filepath.Join(storage, "1", "snapshots", "a")
			mkfile(t, snap)
			mkfile(t, filepath.Join(storage, "1", m, "x"))
			if _, _, err := openFinal(context.Background(), storage, 1, nil, readTimeout, noLog); err == nil || !strings.Contains(err.Error(), m) {
				t.Fatalf("openFinal = %v, want a refusal naming %s", err, m)
			}
			survives(t, snap, "x")
		})
	}
}

func TestGuardCacheBundlesAndReceipts(t *testing.T) {
	cache := func(t *testing.T) string {
		root := filepath.Join(t.TempDir(), "1")
		for _, d := range []string{"bundles", "receipts", "snapshots", "index"} {
			if err := os.MkdirAll(filepath.Join(root, d), 0o700); err != nil {
				t.Fatal(err)
			}
		}
		return root
	}
	if err := guardCache(cache(t)); err != nil {
		t.Fatalf("guardCache refused a gateway cache with bundles/ and receipts/: %v", err)
	}
	for _, m := range []string{"batches", "momentum_hashes"} {
		root := cache(t)
		if err := os.Mkdir(filepath.Join(root, m), 0o700); err != nil {
			t.Fatal(err)
		}
		if err := guardCache(root); err == nil || !strings.Contains(err.Error(), m) {
			t.Fatalf("guardCache with %s/ = %v, want a refusal naming it", m, err)
		}
	}
	for _, m := range []string{"cursor", "meta.json"} {
		root := cache(t)
		mkfile(t, filepath.Join(root, m))
		if err := guardCache(root); err == nil || !strings.Contains(err.Error(), m) {
			t.Fatalf("guardCache with %s = %v, want a refusal naming it", m, err)
		}
	}
	storage := t.TempDir()
	if _, err := recovery.NewLayout(storage, 1); err != nil {
		t.Fatal(err)
	}
	if err := guardCache(filepath.Join(storage, "1")); err == nil {
		t.Fatal("guardCache accepted an executor's tree")
	}
	root := cache(t)
	if err := os.Remove(filepath.Join(root, "bundles")); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(t.TempDir(), filepath.Join(root, "bundles")); err != nil {
		t.Fatal(err)
	}
	if err := guardCache(root); err == nil || !strings.Contains(err.Error(), "symlink") {
		t.Fatalf("guardCache with a symlinked bundles/ = %v, want a symlink refusal", err)
	}
	root = cache(t)
	if err := os.Remove(filepath.Join(root, "receipts")); err != nil {
		t.Fatal(err)
	}
	mkfile(t, filepath.Join(root, "receipts"))
	if err := guardCache(root); err == nil || !strings.Contains(err.Error(), "not a directory") {
		t.Fatalf("guardCache with a file at receipts = %v, want a not-a-directory refusal", err)
	}
}

func TestOpenChainRefusesSwappedPath(t *testing.T) {
	exec := t.TempDir()
	mkfile(t, filepath.Join(exec, "1", "snapshots", "a"))
	link := filepath.Join(t.TempDir(), "gw")
	if err := os.Symlink(exec, link); err != nil {
		t.Fatal(err)
	}
	_, err := OpenChain(context.Background(), link, DomainConfig{DomainID: 1, Source: "LOCAL", ExecutorStoragePath: exec}, nil, nil, noLog)
	if err == nil || !strings.Contains(err.Error(), "overlap") {
		t.Fatalf("OpenChain = %v, want an overlap refusal", err)
	}
	survives(t, filepath.Join(exec, "1", "snapshots", "a"), "x")
}

func TestSameTreeCaseInsensitive(t *testing.T) {
	base := t.TempDir()
	lower := filepath.Join(base, "exec")
	if err := os.Mkdir(lower, 0o700); err != nil {
		t.Fatal(err)
	}
	upper := filepath.Join(base, "EXEC")
	if _, err := os.Stat(upper); err != nil {
		t.Skip("the filesystem is case-sensitive")
	}
	if !sameTree(lower, upper) {
		t.Fatal("sameTree missed one directory spelled in two cases")
	}
	if !overlaps(lower, filepath.Join(upper, "sub")) {
		t.Fatal("overlaps missed a directory under the other spelling")
	}
}

func TestSameTreeSeparate(t *testing.T) {
	a, b := t.TempDir(), t.TempDir()
	if sameTree(a, b) || sameTree(filepath.Join(a, "new"), b) {
		t.Fatal("sameTree matched unrelated directories")
	}
	if !sameTree(filepath.Join(a, "new", "deeper"), a) {
		t.Fatal("sameTree missed a path under an existing directory")
	}
}

func TestRedactEndpointErrCapsLength(t *testing.T) {
	long := strings.Repeat("a", 100000)
	got := RedactEndpointErr("http://node:1", errors.New(long))
	if len(got) > maxLoggedErr+len("...(truncated)") || !strings.HasSuffix(got, "(truncated)") {
		t.Fatalf("len = %d, suffix %q; want a capped, marked message", len(got), got[len(got)-12:])
	}
	if short := RedactEndpointErr("http://node:1", errors.New("short")); short != "short" {
		t.Fatalf("short = %q", short)
	}
}

func mkdirMode(t *testing.T, path string, mode os.FileMode) {
	t.Helper()
	if err := os.Mkdir(path, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(path, mode); err != nil {
		t.Fatal(err)
	}
}

// resolvedTemp returns a temp dir whose path holds no symlink.
func resolvedTemp(t *testing.T) string {
	t.Helper()
	dir, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	return dir
}

// setAfterMkdir installs fn as the hook between MkdirAll and
// EvalSymlinks for the rest of the test.
func setAfterMkdir(t *testing.T, fn func()) {
	t.Helper()
	afterMkdir = fn
	t.Cleanup(func() { afterMkdir = func() {} })
}

func TestCheckAncestorsLstat(t *testing.T) {
	t.Run("symlink ancestor refused", func(t *testing.T) {
		real := resolvedTemp(t)
		link := filepath.Join(resolvedTemp(t), "link")
		if err := os.Symlink(real, link); err != nil {
			t.Fatal(err)
		}
		err := checkAncestors(filepath.Join(link, "gw"), os.Geteuid())
		if err == nil || !strings.Contains(err.Error(), "symlink") {
			t.Fatalf("checkAncestors = %v, want a symlink refusal", err)
		}
	})
	t.Run("symlink to a trusted directory refused", func(t *testing.T) {
		link := filepath.Join(resolvedTemp(t), "root")
		if err := os.Symlink("/", link); err != nil {
			t.Fatal(err)
		}
		if err := checkAncestors(filepath.Join(link, "gw"), os.Geteuid()); err == nil {
			t.Fatal("checkAncestors accepted a symlink to /")
		}
	})
	t.Run("file ancestor refused", func(t *testing.T) {
		f := filepath.Join(resolvedTemp(t), "file")
		if err := os.WriteFile(f, nil, 0o600); err != nil {
			t.Fatal(err)
		}
		if err := checkAncestors(filepath.Join(f, "gw"), os.Geteuid()); err == nil {
			t.Fatal("checkAncestors accepted a file")
		}
	})
}

func TestPrepareStorageStepOrder(t *testing.T) {
	t.Run("foreign symlink refused before anything is created", func(t *testing.T) {
		real := resolvedTemp(t)
		link := filepath.Join(resolvedTemp(t), "link")
		if err := os.Symlink(real, link); err != nil {
			t.Fatal(err)
		}
		// The link is owned by this user, so it is foreign to a
		// gateway running as the next uid.
		restore := euid
		euid = func() int { return os.Geteuid() + 1 }
		t.Cleanup(func() { euid = restore })
		_, err := PrepareStorage(filepath.Join(link, "gw"))
		if err == nil || !strings.Contains(err.Error(), "symlink") {
			t.Fatalf("PrepareStorage = %v, want a symlink ownership refusal", err)
		}
		if exists(filepath.Join(real, "gw")) {
			t.Fatal("PrepareStorage created a directory through the refused link")
		}
	})
	t.Run("ancestor swapped for a link to a writable directory refused", func(t *testing.T) {
		base := resolvedTemp(t)
		open := filepath.Join(base, "open")
		mkdirMode(t, open, 0o777)
		parent := filepath.Join(base, "parent")
		setAfterMkdir(t, func() {
			// Swap the parent for a link into the writable directory,
			// which holds a tree of the same shape.
			if err := os.MkdirAll(filepath.Join(open, "gw"), 0o700); err != nil {
				t.Error(err)
			}
			if err := os.RemoveAll(parent); err != nil {
				t.Error(err)
			}
			if err := os.Symlink(open, parent); err != nil {
				t.Error(err)
			}
		})
		got, err := PrepareStorage(filepath.Join(parent, "gw"))
		if err == nil || !strings.Contains(err.Error(), "writable") {
			t.Fatalf("PrepareStorage = %q, %v; want a writable-ancestor refusal", got, err)
		}
	})
	t.Run("returned path is the resolved path after a swap", func(t *testing.T) {
		base := resolvedTemp(t)
		other := filepath.Join(base, "other")
		mkdirMode(t, other, 0o700)
		parent := filepath.Join(base, "parent")
		setAfterMkdir(t, func() {
			if err := os.MkdirAll(filepath.Join(other, "gw"), 0o700); err != nil {
				t.Error(err)
			}
			if err := os.RemoveAll(parent); err != nil {
				t.Error(err)
			}
			if err := os.Symlink(other, parent); err != nil {
				t.Error(err)
			}
		})
		got, err := PrepareStorage(filepath.Join(parent, "gw"))
		if err != nil {
			t.Fatal(err)
		}
		if got != filepath.Join(other, "gw") {
			t.Fatalf("PrepareStorage = %q, want %q", got, filepath.Join(other, "gw"))
		}
	})
}

func TestPrepareStorageAncestors(t *testing.T) {
	t.Run("temp dir accepted", func(t *testing.T) {
		if _, err := PrepareStorage(filepath.Join(t.TempDir(), "gw")); err != nil {
			t.Fatalf("PrepareStorage under t.TempDir() = %v", err)
		}
	})
	t.Run("world-writable parent refused", func(t *testing.T) {
		parent := filepath.Join(t.TempDir(), "open")
		mkdirMode(t, parent, 0o777)
		_, err := PrepareStorage(filepath.Join(parent, "gw"))
		if err == nil || !strings.Contains(err.Error(), "writable") {
			t.Fatalf("PrepareStorage = %v, want a writable-ancestor refusal", err)
		}
	})
	t.Run("group-writable grandparent refused", func(t *testing.T) {
		top := filepath.Join(t.TempDir(), "grp")
		mkdirMode(t, top, 0o770)
		if err := os.MkdirAll(filepath.Join(top, "a"), 0o700); err != nil {
			t.Fatal(err)
		}
		_, err := PrepareStorage(filepath.Join(top, "a", "gw"))
		if err == nil || !strings.Contains(err.Error(), "writable") {
			t.Fatalf("PrepareStorage = %v, want a writable-ancestor refusal", err)
		}
	})
	t.Run("sticky world-writable parent accepted", func(t *testing.T) {
		parent := filepath.Join(t.TempDir(), "sticky")
		mkdirMode(t, parent, 0o777|os.ModeSticky)
		if _, err := PrepareStorage(filepath.Join(parent, "gw")); err != nil {
			t.Fatalf("PrepareStorage = %v", err)
		}
	})
	t.Run("symlinked ancestor owned by this user accepted", func(t *testing.T) {
		real := t.TempDir()
		link := filepath.Join(t.TempDir(), "link")
		if err := os.Symlink(real, link); err != nil {
			t.Fatal(err)
		}
		if _, err := PrepareStorage(filepath.Join(link, "gw")); err != nil {
			t.Fatalf("PrepareStorage = %v", err)
		}
	})
	t.Run("trailing slash cleaned", func(t *testing.T) {
		real := t.TempDir()
		link := filepath.Join(t.TempDir(), "gw")
		if err := os.Symlink(real, link); err != nil {
			t.Fatal(err)
		}
		if _, err := PrepareStorage(link + "/"); err == nil || !strings.Contains(err.Error(), "symlink") {
			t.Fatalf("PrepareStorage(%q) = %v, want a symlink refusal", link+"/", err)
		}
	})
	t.Run("ancestor of another uid refused", func(t *testing.T) {
		if os.Geteuid() == 0 {
			t.Skip("root trusts every ancestor owned by root; see the chown subtest")
		}
		// The temp dir is owned by this user, so it is foreign to
		// a gateway running as the next uid.
		dir := filepath.Join(resolvedTemp(t), "gw")
		mkdirMode(t, dir, 0o700)
		err := checkAncestors(dir, os.Geteuid()+1)
		if err == nil || !strings.Contains(err.Error(), "owned by uid") {
			t.Fatalf("checkAncestors = %v, want an ownership refusal", err)
		}
	})
	t.Run("ancestor chowned to another uid refused", func(t *testing.T) {
		if os.Geteuid() != 0 {
			t.Skip("changing a directory's owner needs root")
		}
		parent := filepath.Join(resolvedTemp(t), "foreign")
		mkdirMode(t, parent, 0o755)
		if err := os.Chown(parent, 12345, 12345); err != nil {
			t.Fatal(err)
		}
		err := checkAncestors(filepath.Join(parent, "gw"), os.Geteuid())
		if err == nil || !strings.Contains(err.Error(), "owned by uid") {
			t.Fatalf("checkAncestors = %v, want an ownership refusal", err)
		}
	})
}

// The cache directory is swapped for a symlink after the Settlement
// reads and before the snapshot opens.
type swapSettlement struct {
	*fakeSettlement
	swap func()
}

func (s swapSettlement) At(h uint64) (SettlementView, error) {
	return swapView{fakeView{s.fakeSettlement}, s.swap}, nil
}

type swapView struct {
	fakeView
	swap func()
}

func (v swapView) DomainRecord(id uint32) (*definition.DomainRecord, bool, error) {
	rec, ok, err := v.fakeView.DomainRecord(id)
	v.swap()
	return rec, ok, err
}

func TestOpenFinalGuardsAgainBeforeOpeningSnapshot(t *testing.T) {
	_, cs := buildA(t)
	storage := t.TempDir()
	snaps := cachePathsFor(storage, 1).snapshots
	snapshotAt(t, snaps, batch2Leaves(t, cs), fixtureSTF, metaOf(2), cs[2].LastInputSeq)
	s := newFakeSettlement()
	s.height = 30
	acceptFinal(s, cs, 2)
	elsewhere := t.TempDir()
	swapped := false
	ss := swapSettlement{s, func() {
		moved := snaps + ".moved"
		if err := os.Rename(snaps, moved); err != nil {
			t.Error(err)
			return
		}
		if err := os.Symlink(elsewhere, snaps); err != nil {
			t.Error(err)
			return
		}
		swapped = true
	}}
	sink := &logSink{}
	_, _, err := openFinal(context.Background(), storage, 1, ss, readTimeout, sink.logf)
	if !swapped {
		t.Fatal("the swap never ran")
	}
	if err == nil || !strings.Contains(err.Error(), "symlink") {
		t.Fatalf("openFinal = %v, want a symlink refusal", err)
	}
	if sink.count("discarding") != 0 {
		t.Fatalf("the snapshot opened before the guard ran:\n%s", sink.dump())
	}
}

func TestPrepareStorageSymlinkComponents(t *testing.T) {
	t.Run("returns the resolved path", func(t *testing.T) {
		real := t.TempDir()
		link := filepath.Join(t.TempDir(), "link")
		if err := os.Symlink(real, link); err != nil {
			t.Fatal(err)
		}
		got, err := PrepareStorage(filepath.Join(link, "gw"))
		if err != nil {
			t.Fatal(err)
		}
		want, err := filepath.EvalSymlinks(real)
		if err != nil {
			t.Fatal(err)
		}
		if got != filepath.Join(want, "gw") {
			t.Fatalf("PrepareStorage = %q, want %q", got, filepath.Join(want, "gw"))
		}
	})
	t.Run("root-owned symlink component accepted", func(t *testing.T) {
		for _, link := range []string{"/tmp", "/var"} {
			if fi, err := os.Lstat(link); err != nil || fi.Mode()&os.ModeSymlink == 0 {
				continue
			}
			dir, err := os.MkdirTemp(link, "gwtest")
			if err != nil {
				t.Skipf("cannot create a directory under %s: %v", link, err)
			}
			t.Cleanup(func() { _ = os.RemoveAll(dir) })
			if _, err := PrepareStorage(filepath.Join(dir, "gw")); err != nil {
				t.Fatalf("PrepareStorage under %s = %v", link, err)
			}
			return
		}
		// No root-owned symlink on this system: a plain temp path
		// must pass the same walk.
		if _, err := PrepareStorage(filepath.Join(t.TempDir(), "gw")); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("symlink of another uid refused", func(t *testing.T) {
		if os.Geteuid() == 0 {
			t.Skip("root trusts every symlink owned by root; see the chown subtest")
		}
		real := t.TempDir()
		link := filepath.Join(t.TempDir(), "link")
		if err := os.Symlink(real, link); err != nil {
			t.Fatal(err)
		}
		// The link is owned by this user, so it is foreign to a
		// gateway running as the next uid.
		err := checkSymlinks(filepath.Join(link, "gw"), os.Geteuid()+1)
		if err == nil || !strings.Contains(err.Error(), "symlink") || !strings.Contains(err.Error(), "owned by uid") {
			t.Fatalf("checkSymlinks = %v, want a symlink ownership refusal", err)
		}
	})
	t.Run("symlink of another uid in a world-writable directory refused", func(t *testing.T) {
		if os.Geteuid() != 0 {
			t.Skip("changing a symlink's owner needs root")
		}
		open := filepath.Join(t.TempDir(), "open")
		mkdirMode(t, open, 0o777)
		real := t.TempDir()
		link := filepath.Join(open, "link")
		if err := os.Symlink(real, link); err != nil {
			t.Fatal(err)
		}
		if err := os.Lchown(link, 12345, 12345); err != nil {
			t.Fatal(err)
		}
		_, err := PrepareStorage(filepath.Join(link, "gw"))
		if err == nil || !strings.Contains(err.Error(), "owned by uid") {
			t.Fatalf("PrepareStorage = %v, want a symlink ownership refusal", err)
		}
		if exists(filepath.Join(real, "gw")) {
			t.Fatal("PrepareStorage created a directory through the refused link")
		}
	})
}

// A link repointed after PrepareStorage must not redirect a gateway
// that opens the chain under the resolved path.
func TestOpenChainIgnoresRepointedLink(t *testing.T) {
	pristine, cs := buildA(t)
	exec := t.TempDir()
	copyDir(t, pristine, exec)
	treeA, treeB, holder := t.TempDir(), t.TempDir(), t.TempDir()
	link := filepath.Join(holder, "link")
	if err := os.Symlink(treeA, link); err != nil {
		t.Fatal(err)
	}
	configured := filepath.Join(link, "gw")
	resolved, err := PrepareStorage(configured)
	if err != nil {
		t.Fatal(err)
	}
	s := newFakeSettlement()
	s.height = 30
	acceptFinal(s, cs, 2)
	seed, _ := openTestChain(t, resolved, exec, s)
	if err := seed.Poll(context.Background()); err != nil {
		t.Fatalf("Poll: %v", err)
	}
	closeChain(t, seed)

	if err := os.Remove(link); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(treeB, link); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(configured, 0o700); err != nil {
		t.Fatal(err)
	}

	c, sink := openTestChain(t, resolved, exec, s)
	if c == nil || sink.count("booted from the snapshot") != 1 {
		t.Fatalf("the chain did not boot from the original tree:\n%s", sink.dump())
	}
	if entries, _ := os.ReadDir(configured); len(entries) != 0 {
		t.Fatalf("the repointed tree was touched: %v", entries)
	}
}

// setEuid makes the gateway believe it runs as uid for the rest of the
// test.
func setEuid(t *testing.T, uid int) {
	t.Helper()
	restore := euid
	euid = func() int { return uid }
	t.Cleanup(func() { euid = restore })
}

// setBeforeOpenLeaf installs fn as the hook run just before secureLeaf
// opens a leaf, for the rest of the test.
func setBeforeOpenLeaf(t *testing.T, fn func(string)) {
	t.Helper()
	beforeOpenLeaf = fn
	t.Cleanup(func() { beforeOpenLeaf = func(string) {} })
}

func TestSecureLeaf(t *testing.T) {
	t.Run("sets mode 0700 on an owned directory", func(t *testing.T) {
		dir := filepath.Join(resolvedTemp(t), "leaf")
		mkdirMode(t, dir, 0o755)
		if err := secureLeaf(dir, os.Geteuid()); err != nil {
			t.Fatal(err)
		}
		if fi, err := os.Stat(dir); err != nil || fi.Mode().Perm() != 0o700 {
			t.Fatalf("mode = %v, %v; want 0700", fi.Mode().Perm(), err)
		}
	})
	t.Run("missing leaf refused", func(t *testing.T) {
		if err := secureLeaf(filepath.Join(resolvedTemp(t), "none"), os.Geteuid()); err == nil {
			t.Fatal("secureLeaf accepted a missing leaf")
		}
	})
	t.Run("symlink refused", func(t *testing.T) {
		base := resolvedTemp(t)
		victim := filepath.Join(base, "victim")
		mkdirMode(t, victim, 0o755)
		link := filepath.Join(base, "leaf")
		if err := os.Symlink(victim, link); err != nil {
			t.Fatal(err)
		}
		err := secureLeaf(link, os.Geteuid())
		if err == nil || !strings.Contains(err.Error(), "symlink") {
			t.Fatalf("secureLeaf = %v, want a symlink refusal", err)
		}
		if fi, err := os.Stat(victim); err != nil || fi.Mode().Perm() != 0o755 {
			t.Fatalf("victim mode = %v, %v; want 0755 untouched", fi.Mode().Perm(), err)
		}
	})
	t.Run("file refused", func(t *testing.T) {
		f := filepath.Join(resolvedTemp(t), "leaf")
		mkfile(t, f)
		if err := secureLeaf(f, os.Geteuid()); err == nil {
			t.Fatal("secureLeaf accepted a file")
		}
	})
	t.Run("foreign owner refused through the euid seam", func(t *testing.T) {
		dir := filepath.Join(resolvedTemp(t), "leaf")
		mkdirMode(t, dir, 0o755)
		err := secureLeaf(dir, os.Geteuid()+1)
		if err == nil || !strings.Contains(err.Error(), "owned by uid") {
			t.Fatalf("secureLeaf = %v, want an ownership refusal", err)
		}
		if fi, err := os.Stat(dir); err != nil || fi.Mode().Perm() != 0o755 {
			t.Fatalf("mode = %v, %v; want 0755 untouched", fi.Mode().Perm(), err)
		}
	})
	t.Run("foreign owner refused after chown", func(t *testing.T) {
		if os.Geteuid() != 0 {
			t.Skip("changing a directory's owner needs root")
		}
		dir := filepath.Join(resolvedTemp(t), "leaf")
		mkdirMode(t, dir, 0o755)
		if err := os.Chown(dir, 12345, 12345); err != nil {
			t.Fatal(err)
		}
		err := secureLeaf(dir, os.Geteuid())
		if err == nil || !strings.Contains(err.Error(), "owned by uid") {
			t.Fatalf("secureLeaf = %v, want an ownership refusal", err)
		}
	})
	t.Run("symlink swapped in after the check is not followed", func(t *testing.T) {
		base := resolvedTemp(t)
		victim := filepath.Join(base, "victim")
		mkdirMode(t, victim, 0o755)
		leaf := filepath.Join(base, "leaf")
		mkdirMode(t, leaf, 0o700)
		setBeforeOpenLeaf(t, func(p string) {
			if err := os.Remove(p); err != nil {
				t.Error(err)
			}
			if err := os.Symlink(victim, p); err != nil {
				t.Error(err)
			}
		})
		if err := secureLeaf(leaf, os.Geteuid()); err == nil {
			t.Fatal("secureLeaf followed a swapped-in symlink")
		}
		if fi, err := os.Stat(victim); err != nil || fi.Mode().Perm() != 0o755 {
			t.Fatalf("victim mode = %v, %v; want 0755 untouched", fi.Mode().Perm(), err)
		}
	})
}

func TestPrepareStorageRefusesSwappedLeaf(t *testing.T) {
	base := resolvedTemp(t)
	victim := filepath.Join(base, "victim")
	mkdirMode(t, victim, 0o755)
	storage := filepath.Join(base, "gw")
	setBeforeOpenLeaf(t, func(p string) {
		if err := os.Remove(p); err != nil {
			t.Error(err)
		}
		if err := os.Symlink(victim, p); err != nil {
			t.Error(err)
		}
	})
	got, err := PrepareStorage(storage)
	if err == nil || !strings.Contains(err.Error(), "symlink") {
		t.Fatalf("PrepareStorage = %q, %v; want a symlink refusal", got, err)
	}
	if fi, err := os.Stat(victim); err != nil || fi.Mode().Perm() != 0o755 {
		t.Fatalf("victim mode = %v, %v; want 0755 untouched", fi.Mode().Perm(), err)
	}
}

func TestPrepareStorageRefusesForeignLeaf(t *testing.T) {
	t.Run("through the euid seam", func(t *testing.T) {
		if os.Geteuid() == 0 {
			t.Skip("a root-owned leaf is the gateway's own")
		}
		storage := filepath.Join(resolvedTemp(t), "gw")
		mkdirMode(t, storage, 0o700)
		setEuid(t, os.Geteuid()+1)
		// The seam also makes the ancestors foreign, so the leaf check
		// is driven directly with the same uid.
		err := secureLeaf(storage, euid())
		if err == nil || !strings.Contains(err.Error(), "owned by uid") {
			t.Fatalf("secureLeaf = %v, want an ownership refusal", err)
		}
	})
	t.Run("after chown", func(t *testing.T) {
		if os.Geteuid() != 0 {
			t.Skip("changing a directory's owner needs root")
		}
		storage := filepath.Join(resolvedTemp(t), "gw")
		mkdirMode(t, storage, 0o700)
		if err := os.Chown(storage, 12345, 12345); err != nil {
			t.Fatal(err)
		}
		if _, err := PrepareStorage(storage); err == nil || !strings.Contains(err.Error(), "owned by uid") {
			t.Fatalf("PrepareStorage = %v, want an ownership refusal", err)
		}
	})
}

func TestOpenFinalRefusesSwappedLeaf(t *testing.T) {
	for _, leaf := range []string{"", "snapshots"} {
		t.Run("leaf "+leaf, func(t *testing.T) {
			storage := resolvedTemp(t)
			victim := filepath.Join(resolvedTemp(t), "victim")
			mkdirMode(t, victim, 0o755)
			want := filepath.Join(storage, "1", leaf)
			setBeforeOpenLeaf(t, func(p string) {
				if p != want {
					return
				}
				if err := os.RemoveAll(p); err != nil {
					t.Error(err)
				}
				if err := os.Symlink(victim, p); err != nil {
					t.Error(err)
				}
			})
			_, _, err := openFinal(context.Background(), storage, 1, nil, readTimeout, noLog)
			if err == nil || !strings.Contains(err.Error(), "symlink") {
				t.Fatalf("openFinal = %v, want a symlink refusal", err)
			}
			if fi, err := os.Stat(victim); err != nil || fi.Mode().Perm() != 0o755 {
				t.Fatalf("victim mode = %v, %v; want 0755 untouched", fi.Mode().Perm(), err)
			}
		})
	}
}

func TestOpenFinalRefusesForeignLeaf(t *testing.T) {
	t.Run("through the euid seam", func(t *testing.T) {
		if os.Geteuid() == 0 {
			t.Skip("a root-owned leaf is the gateway's own")
		}
		storage := resolvedTemp(t)
		if err := os.MkdirAll(filepath.Join(storage, "1"), 0o700); err != nil {
			t.Fatal(err)
		}
		// guardCache still sees the real uid, so the refusal comes from
		// the descriptor check on the first leaf, <id>.
		setEuid(t, os.Geteuid()+1)
		_, _, err := openFinal(context.Background(), storage, 1, nil, readTimeout, noLog)
		if err == nil || !strings.Contains(err.Error(), "owned by uid") || !strings.Contains(err.Error(), filepath.Join(storage, "1")) {
			t.Fatalf("openFinal = %v, want an ownership refusal naming the domain directory", err)
		}
	})
	for _, leaf := range []string{"", "snapshots"} {
		t.Run("chown "+leaf, func(t *testing.T) {
			if os.Geteuid() != 0 {
				t.Skip("changing a directory's owner needs root")
			}
			storage := resolvedTemp(t)
			dir := filepath.Join(storage, "1", leaf)
			if err := os.MkdirAll(dir, 0o700); err != nil {
				t.Fatal(err)
			}
			if err := os.Chown(dir, 12345, 12345); err != nil {
				t.Fatal(err)
			}
			_, _, err := openFinal(context.Background(), storage, 1, nil, readTimeout, noLog)
			if err == nil || !strings.Contains(err.Error(), "owned by uid") {
				t.Fatalf("openFinal = %v, want an ownership refusal", err)
			}
		})
	}
}

func TestOpenFinalRefusesSymlinkedIndex(t *testing.T) {
	storage := t.TempDir()
	other := t.TempDir()
	mkfile(t, filepath.Join(other, "keep"))
	if err := os.MkdirAll(filepath.Join(storage, "1"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(other, filepath.Join(storage, "1", "index")); err != nil {
		t.Fatal(err)
	}
	if _, _, err := openFinal(context.Background(), storage, 1, nil, readTimeout, noLog); err == nil || !strings.Contains(err.Error(), "symlink") {
		t.Fatalf("openFinal = %v, want a symlink refusal", err)
	}
	survives(t, filepath.Join(other, "keep"), "x")
}

func TestOpenIndexSecuresItsLeaf(t *testing.T) {
	t.Run("tightens the mode of an existing directory", func(t *testing.T) {
		storage := resolvedTemp(t)
		dir := cachePathsFor(storage, 1).index
		if err := os.MkdirAll(filepath.Dir(dir), 0o700); err != nil {
			t.Fatal(err)
		}
		mkdirMode(t, dir, 0o755)
		x, err := openIndex(storage, 1, nil)
		if err != nil {
			t.Fatal(err)
		}
		defer x.Close()
		if fi, err := os.Stat(dir); err != nil || fi.Mode().Perm() != 0o700 {
			t.Fatalf("index mode = %v, %v; want 0700", fi.Mode().Perm(), err)
		}
	})
	t.Run("refuses a leaf swapped for a symlink after creation", func(t *testing.T) {
		storage := resolvedTemp(t)
		victim := filepath.Join(resolvedTemp(t), "victim")
		mkdirMode(t, victim, 0o755)
		want := cachePathsFor(storage, 1).index
		setBeforeOpenLeaf(t, func(p string) {
			if p != want {
				return
			}
			if err := os.RemoveAll(p); err != nil {
				t.Error(err)
			}
			if err := os.Symlink(victim, p); err != nil {
				t.Error(err)
			}
		})
		_, err := openIndex(storage, 1, nil)
		if err == nil || !strings.Contains(err.Error(), "symlink") {
			t.Fatalf("openIndex = %v, want a symlink refusal", err)
		}
		if entries, _ := os.ReadDir(victim); len(entries) != 0 {
			t.Fatalf("the link target was written to: %v", entries)
		}
	})
}

func TestOpenIndexRefusesExecutorTree(t *testing.T) {
	storage := resolvedTemp(t)
	root := filepath.Join(storage, "1")
	if err := os.MkdirAll(root, 0o700); err != nil {
		t.Fatal(err)
	}
	mkfile(t, filepath.Join(root, "meta.json"))
	if _, err := openIndex(storage, 1, nil); err == nil || !strings.Contains(err.Error(), "executor") {
		t.Fatalf("openIndex = %v, want an executor tree refusal", err)
	}
	if _, err := os.Lstat(filepath.Join(root, "index")); !os.IsNotExist(err) {
		t.Fatalf("index/ was created in an executor tree: %v", err)
	}
	survives(t, filepath.Join(root, "meta.json"), "x")
}

// A fresh gateway cache holds only snapshots/ and index/, and a
// leftover state/ is neither checked, followed nor removed.
func TestOpenFinalIgnoresStateDirectory(t *testing.T) {
	t.Run("a fresh cache has no state directory", func(t *testing.T) {
		storage := t.TempDir()
		store, _, err := openFinal(context.Background(), storage, 1, nil, readTimeout, noLog)
		if err != nil {
			t.Fatalf("openFinal: %v", err)
		}
		defer store.Close()
		if _, err := os.Lstat(filepath.Join(storage, "1", "state")); !os.IsNotExist(err) {
			t.Fatalf("state/ was created in the cache: %v", err)
		}
	})
	t.Run("a leftover symlinked state directory is left alone", func(t *testing.T) {
		storage := t.TempDir()
		other := t.TempDir()
		mkfile(t, filepath.Join(other, "wal"))
		if err := os.MkdirAll(filepath.Join(storage, "1"), 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink(other, filepath.Join(storage, "1", "state")); err != nil {
			t.Fatal(err)
		}
		store, _, err := openFinal(context.Background(), storage, 1, nil, readTimeout, noLog)
		if err != nil {
			t.Fatalf("openFinal: %v", err)
		}
		defer store.Close()
		survives(t, filepath.Join(other, "wal"), "x")
	})
}
