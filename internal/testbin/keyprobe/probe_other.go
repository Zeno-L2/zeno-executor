//go:build !linux

package main

import "syscall"

// probe is a no-op off Linux: the five attempts rely on Linux /proc and ptrace semantics.
func probe(ppid int) map[string]attempt {
	return nil
}

// devIno extracts the (dev, ino) pair Fstat reported. syscall.Stat_t's Dev and Ino field types
// vary by OS; the explicit conversion is always valid for the integer types they are.
func devIno(stat *syscall.Stat_t) (dev, ino uint64) {
	return uint64(stat.Dev), uint64(stat.Ino)
}

// fdLink is empty off Linux: there is no portable equivalent of /proc/self/fd here.
func fdLink(fd int) string {
	return ""
}

// selfCaps reports nothing off Linux: capabilities and Yama are Linux-specific.
func selfCaps() (capPrm, capEff, capAmb, ptraceScope string) {
	return "", "", "", ""
}
