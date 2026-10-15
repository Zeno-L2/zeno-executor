package gateway

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"syscall"
)

// executorMarkers are the entries only an executor's domain tree
// holds. A gateway cache holds none of them (its bundles/ and
// receipts/ are not markers), so finding one under
// <storagePath>/<id> means the directory is an executor's.
var executorMarkers = []string{"batches", "cursor", "meta.json", "momentum_hashes"}

// afterMkdir runs between MkdirAll and EvalSymlinks. Tests set it to
// change the filesystem at that point.
var afterMkdir = func() {}

// euid returns the effective uid PrepareStorage trusts. Tests replace
// it to stand in for a gateway running as another user.
var euid = os.Geteuid

// PrepareStorage creates storagePath if it is missing and returns its
// symlink-free form, the only path the gateway may use afterwards.
// Before it creates anything it refuses a storagePath that is itself a
// symlink and any symlink component not owned by root or the effective
// uid, see checkSymlinks. It then resolves the path and walks the
// resolved path from the root with Lstat, see checkAncestors. That
// walk is authoritative: the resolved path holds no symlink, and each
// component is owned by root or the effective uid and not writable by
// anyone else, so once it passes only a trusted owner can change the
// path. The storage directory itself must be owned by the effective
// uid, and it is set to mode 0700.
func PrepareStorage(storagePath string) (string, error) {
	storagePath = filepath.Clean(storagePath)
	uid := euid()
	if err := checkSymlinks(storagePath, uid); err != nil {
		return "", err
	}
	if err := os.MkdirAll(storagePath, 0o700); err != nil {
		return "", err
	}
	afterMkdir()
	resolved, err := filepath.EvalSymlinks(storagePath)
	if err != nil {
		return "", fmt.Errorf("gateway: storagePath %s: %w", storagePath, err)
	}
	if err := checkAncestors(resolved, uid); err != nil {
		return "", err
	}
	// The storage directory is opened and chmodded after its
	// ancestors, so the chmod runs only on a path whose every parent
	// is trusted.
	if err := secureLeaf(resolved, uid); err != nil {
		return "", err
	}
	return resolved, nil
}

// beforeOpenLeaf runs in secureLeaf just before it opens the leaf.
// Tests set it to change the filesystem at that point.
var beforeOpenLeaf = func(string) {}

// secureLeaf opens the directory at path without following a link,
// checks its owner on the descriptor and sets its mode to 0700 on the
// descriptor, so nothing swapped in at path afterwards is touched. It
// refuses a symlink, a non-directory, a missing path and an owner
// other than uid. Call it after the directory is created.
func secureLeaf(path string, uid int) error {
	beforeOpenLeaf(path)
	fd, err := syscall.Open(path, syscall.O_RDONLY|syscall.O_DIRECTORY|syscall.O_NOFOLLOW|syscall.O_CLOEXEC, 0)
	switch {
	case errors.Is(err, syscall.ELOOP), errors.Is(err, syscall.ENOTDIR):
		return fmt.Errorf("gateway: %s is a symlink or not a directory", path)
	case err != nil:
		return fmt.Errorf("gateway: %s: %w", path, err)
	}
	f := os.NewFile(uintptr(fd), path)
	defer f.Close()
	fi, err := f.Stat()
	if err != nil {
		return fmt.Errorf("gateway: %s: %w", path, err)
	}
	if st, ok := fi.Sys().(*syscall.Stat_t); ok && int(st.Uid) != uid {
		return fmt.Errorf("gateway: %s is owned by uid %d, not uid %d", path, st.Uid, uid)
	}
	if err := f.Chmod(0o700); err != nil {
		return fmt.Errorf("gateway: %s: %w", path, err)
	}
	return nil
}

// checkSymlinks walks path component by component with Lstat from the
// root and follows each symlink it meets to its target, so chained
// links are checked too. It refuses a symlink owned by neither root
// nor uid, because its owner can repoint it, and refuses path itself
// being a symlink. Components that do not exist end the walk.
func checkSymlinks(path string, uid int) error {
	path = filepath.Clean(path)
	for hops := 0; hops < 255; hops++ {
		link, target, ok, err := firstSymlink(path)
		if err != nil {
			return err
		}
		if !ok {
			return nil
		}
		if link == path {
			return fmt.Errorf("gateway: %s is a symlink", path)
		}
		fi, err := os.Lstat(link)
		if err != nil {
			return err
		}
		if st, ok := fi.Sys().(*syscall.Stat_t); ok && st.Uid != 0 && int(st.Uid) != uid {
			return fmt.Errorf("gateway: symlink %s is owned by uid %d, not root or uid %d", link, st.Uid, uid)
		}
		rest := path[len(link):]
		if !filepath.IsAbs(target) {
			target = filepath.Join(filepath.Dir(link), target)
		}
		path = filepath.Join(target, rest)
	}
	return fmt.Errorf("gateway: too many symlinks in %s", path)
}

// firstSymlink returns the first component of path, from the root,
// that is a symlink, with its target.
func firstSymlink(path string) (link, target string, found bool, err error) {
	cur := string(filepath.Separator)
	for _, part := range strings.Split(path, string(filepath.Separator)) {
		if part == "" {
			continue
		}
		cur = filepath.Join(cur, part)
		fi, err := os.Lstat(cur)
		if errors.Is(err, fs.ErrNotExist) {
			return "", "", false, nil
		}
		if err != nil {
			return "", "", false, err
		}
		if fi.Mode()&fs.ModeSymlink != 0 {
			t, err := os.Readlink(cur)
			if err != nil {
				return "", "", false, err
			}
			return cur, t, true, nil
		}
	}
	return "", "", false, nil
}

// checkAncestors walks the components of the resolved path above its
// last one, from the filesystem root down, with Lstat. It refuses a
// component that is a symlink, is not a directory, is owned by neither
// root nor uid, or is writable by group or others without the sticky
// bit, because such a user could rename the next component and swap
// in a tree of their own. Lstat never follows a link, so a component
// swapped for a link during the walk fails the symlink check.
func checkAncestors(resolved string, uid int) error {
	resolved = filepath.Clean(resolved)
	dirs := []string{}
	for dir := filepath.Dir(resolved); ; dir = filepath.Dir(dir) {
		dirs = append(dirs, dir)
		if filepath.Dir(dir) == dir {
			break
		}
	}
	for i := len(dirs) - 1; i >= 0; i-- {
		dir := dirs[i]
		fi, err := os.Lstat(dir)
		if err != nil {
			return fmt.Errorf("gateway: ancestor %s: %w", dir, err)
		}
		if fi.Mode()&fs.ModeSymlink != 0 {
			return fmt.Errorf("gateway: ancestor %s is a symlink", dir)
		}
		if !fi.IsDir() {
			return fmt.Errorf("gateway: ancestor %s is not a directory", dir)
		}
		if st, ok := fi.Sys().(*syscall.Stat_t); ok && st.Uid != 0 && int(st.Uid) != uid {
			return fmt.Errorf("gateway: ancestor %s is owned by uid %d, not root or uid %d", dir, st.Uid, uid)
		}
		if fi.Mode().Perm()&0o022 != 0 && fi.Mode()&fs.ModeSticky == 0 {
			return fmt.Errorf("gateway: ancestor %s is writable by group or others and is not sticky", dir)
		}
	}
	return nil
}

// checkOwnedDir fails when path is a symlink, is not a directory, or
// is owned by another uid. A path that does not exist passes.
func checkOwnedDir(path string) error {
	fi, err := os.Lstat(path)
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	if fi.Mode()&fs.ModeSymlink != 0 {
		return fmt.Errorf("gateway: %s is a symlink", path)
	}
	if !fi.IsDir() {
		return fmt.Errorf("gateway: %s is not a directory", path)
	}
	if st, ok := fi.Sys().(*syscall.Stat_t); ok && int(st.Uid) != os.Geteuid() {
		return fmt.Errorf("gateway: %s is owned by uid %d, not uid %d", path, st.Uid, os.Geteuid())
	}
	return nil
}

// guardCache checks the cache tree at root (<storagePath>/<id>)
// before the gateway creates, truncates or removes anything in it.
// storagePath, root, state/, snapshots/, index/, bundles/ and
// receipts/ must each be absent or a real directory owned by the
// effective uid, state/wal must not
// be a symlink, and root must hold no executor marker.
func guardCache(root string) error {
	for _, dir := range []string{
		filepath.Dir(root),
		root,
		filepath.Join(root, "state"),
		filepath.Join(root, "snapshots"),
		filepath.Join(root, "index"),
		filepath.Join(root, "bundles"),
		filepath.Join(root, "receipts"),
	} {
		if err := checkOwnedDir(dir); err != nil {
			return err
		}
	}
	wal := filepath.Join(root, "state", "wal")
	if fi, err := os.Lstat(wal); err == nil && fi.Mode()&fs.ModeSymlink != 0 {
		return fmt.Errorf("gateway: %s is a symlink", wal)
	}
	for _, m := range executorMarkers {
		if _, err := os.Lstat(filepath.Join(root, m)); err == nil {
			return fmt.Errorf("gateway: %s holds %s, so it is an executor's domain tree, not a gateway cache", root, m)
		}
	}
	return nil
}

// sameTree reports whether a and b are the same file, or either lies
// inside the other, by file identity: it walks up from each path's
// deepest existing ancestor and compares every ancestor with the
// other path's. Unlike a path comparison it sees through case
// folding, bind mounts and symlinks.
func sameTree(a, b string) bool {
	return containsIdentity(a, b) || containsIdentity(b, a)
}

// containsIdentity reports whether dir is path or one of path's
// ancestors, by os.SameFile.
func containsIdentity(path, dir string) bool {
	want, err := os.Stat(dir)
	if err != nil {
		return false
	}
	for cur := filepath.Clean(path); ; cur = filepath.Dir(cur) {
		if fi, err := os.Stat(cur); err == nil && os.SameFile(fi, want) {
			return true
		}
		if filepath.Dir(cur) == cur {
			return false
		}
	}
}
