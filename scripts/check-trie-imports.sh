#!/usr/bin/env bash
# Fails if the executor's production code references any L1-only trie entry point. The
# executor MUST use only the path-native SMT API (RootOfLeaves, ProveByPath,
# VerifyProofByPath, VerifyAbsenceByPath, and CompactTree) and MUST NOT call the key-hashing
# verifiers or L1 maintenance structures, which would double-hash an already-derived path or fold a
# present-empty leaf to a deletion. Test files are exempt: an adversarial test may deliberately
# call a forbidden function to prove it rejects misuse (mirroring go-zenon's own
# trie_adversarial_test.go), the same way this check itself must reference the banned names.
set -euo pipefail

forbidden='VerifyProof\(|VerifyAbsence\(|Tree\.Update|stagedApplier'

if hits=$(grep -rnE "$forbidden" --include='*.go' --exclude='*_test.go' .); then
  echo "forbidden L1-only trie API referenced:" >&2
  echo "$hits" >&2
  exit 1
else
  echo "ok: no forbidden trie API references"
fi
