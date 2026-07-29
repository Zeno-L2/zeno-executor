#!/usr/bin/env bash
# Fails if the executor references any L1-only trie entry point. The executor MUST use only
# the path-native SMT API (RootOfLeaves, ProveByPath, VerifyProofByPath, VerifyAbsenceByPath)
# and MUST NOT call the key-hashing verifiers or L1 maintenance structures, which would
# double-hash an already-derived path or fold a present-empty leaf to a deletion.
set -euo pipefail

forbidden='VerifyProof\(|VerifyAbsence\(|Tree\.Update|stagedApplier|CompactTree'

if hits=$(grep -rnE "$forbidden" --include='*.go' .); then
  echo "forbidden L1-only trie API referenced:" >&2
  echo "$hits" >&2
  exit 1
else
  echo "ok: no forbidden trie API references"
fi
