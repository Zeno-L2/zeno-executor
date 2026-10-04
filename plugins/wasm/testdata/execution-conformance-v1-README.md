# execution-conformance-v1

## What this is

The repo-owned source copy of the WASM domain's execution conformance artifact, produced by
driving `Plugin.DecodeInput`/`Plugin.Apply` over real modules through the real engine. The copy
in the `zeno` documentation repository under `phase-1/WASM Spec/` was written from this one and
is byte-identical to it **as of this snapshot**. The two are kept in sync by hand and no
automated check enforces the equality, because the two copies now live in different
repositories. The pattern such a check would follow is `../internal/module/gastable_test.go`
`TestArtifactDrift`, which compares the embedded gas table against a vendored copy checked into
this repository's own `testdata/`, not against the zeno documentation tree.

## Provenance

This section is a provenance note for the bytes in this file, not a claim of unconditional
authority: several items below flag open upstream questions whose resolution would change these
vectors.

1. §10.6 fixes both the §10.4 charge/size-check ordering and the per-status `gas_used` table
   (§10.6), so both are normative rather than open.
2. `SPEC.md` §30 `OD-4`: its named remedy is a `len(ResultBlob)`-proportional pre-decode charge,
   i.e. a `metering_version` increment, with the same blast radius — every `gas_used` and
   every `receipt_hex`.
3. §25.1's table now carries the nine structural caps this plugin enforces (§25.1), so the
   cap set is normative rather than open. It re-dates no vector in this file: the `CV-REJECT-*`
   vectors freeze §7.2's prohibited *capabilities* — WASI, floats, threads/atomics, SIMD,
   sign-extension operators, saturating conversions — which the structural caps do not reach.
   Every module here is hand-built and small: at most two declared types, two functions and two
   data segments, each far below `MaxTypeCount = 1024`, `MaxFunctions = 8192` and
   `MaxDataSegments = 1024`; the largest module is ~64 KiB against a 256 KiB `MaxCodeSize`
   (`../limits.go`).
4. §15 states the `state_diff_hash`/`return_hash` preimages and §14.3 states the non-`SUCCESS`
   `ReturnData` discard normatively.
5. The §15 preimage corroboration outcome: the derived rule was checked against the four
   literals §15's own preimages name — EXEC-001's `return_hash`, and EXEC-002's put,
   EXEC-004's delete and EXEC-005's present-empty put `state_diff_hash` — each read out of the
   `receipt_hex` of the superseded upstream EXEC-001…012 artifact,
   which carried no `state_diff_hash`/`return_hash` field of its own. That artifact's other
   state-diff-bearing vectors (EXEC-003, EXEC-006, EXEC-009, EXEC-012) were not checked. The
   check discriminates: a length-prefixed `ReturnData` preimage produces different bytes for
   EXEC-001's `return_hash`, and a count-less concatenation (§20's
   `DAHash` shape) or a §27.3 Merkle root each produce different bytes for all three
   `state_diff_hash` literals — so reproducing the four rules those candidates out on this
   evidence. EXEC-001's zero `state_diff_hash` on an empty `StateDiff` confirms the
   zero-if-empty carve-out.
6. §27.5 states the canonical encoding for an unrecognized `input_kind`: `EXEC-UNSUP-1`'s
   `canonical_input_hex` is that 33-byte prefix — the one vector whose `canonical_input_hex`
   does not come from `codec.Input.Encode` — and it is assembled by the conformance harness;
   no production path produces it. `module_wasm_hex`, `code_hash`, `instrumented_code_hash`,
   `pre_state`, `post_state_root` and `witness_keys` are assembled outside `internal/codec`
   wherever present, and `input_frame_hex` does come from the codec (`../call.go:100`'s
   `frame.Encode()`, where `frame` is the `codec.ExecutionInputFrame` `../call.go:116` builds).
7. The pins the bytes depend on: `github.com/tetratelabs/wazero v1.8.2` (the repository's
   `go.mod:6`, §6) and the `wasm-gas-table-v1` artifact at `metering_version = 1`. A bump to
   either is a consensus change and requires regenerating this file and re-running the suite
   on both CI architectures.

## Schema

Declared as Go structs (`vectorFile`, `vectorJSON`, `vectorInput`, `vectorReceipt`) in
`../vectors_test.go`.

Six vector shapes, distinguished by `shape`:

| Shape | `input_frame_hex` | `module_wasm_hex` | `source_data_hex` | `input` | notes |
|---|---|---|---|---|---|
| `call` (kinds 0/1) | present | present | absent | present | full pipeline |
| `deploy` (kinds 2-5) | absent | present | present iff kind 3 | present | code-store seeding, §15 identity; ReturnData = code_hash ‖ CodeHash (§11.3, §12) on a SUCCESS finalize or upgrade only — a rejected upgrade carries no ReturnData and no state diff, and an UNAUTHORIZED one carries the target's unchanged receipt.code_hash; `pending_metadata_hex` for kinds 4/5 |
| `ownership` (kind 6) | absent | absent | absent | present | frame absent, module absent, source data absent, input present |
| `validation_rejection` | absent | present | absent | present | module never runs; `VALIDATION_FAILED` |
| `buy_storage` (kind 7) | absent | absent | absent | present | frame absent, module absent, source data absent, input present; witness set is the `0x0001` and `0x0005` leaves on every outcome |
| `unsupported_kind` (kind 8) | absent | absent | absent | absent | `canonical_input_hex` is §27.5's 33-byte prefix |

Encoding rules:

- Hex is lowercase, no `0x` prefix.
- Every 32-byte hash field is written in full, including the all-zero value; `omitempty` on a
  fixed-width hex field can therefore only ever drop a field the vector has no value for.
- `pre_state` and `witness_keys` are always marshalled, and the generator always assigns a
  non-nil map/slice, so a shape with neither renders `{}` and `[]`, never `null`.
- `input`'s numeric/string fields that can legitimately be zero or empty for a kind that carries
  them (`plasma_limit`, `total_size`, `chunk_count`, `chunk_index`, `payload_hex`, `bytes`) are pointer
  types, so `omitempty` cannot be confused with a genuinely absent field.
- `contract_effects_blob_hex` absent means the run returned no blob. A module returning a
  zero-length blob is not distinguishable from that and no shape produces one; a future vector
  that needs the distinction adds its own field rather than reinterpreting absence.
- A vector's top-level `code_hash` is SPEC's pre-instrumentation `code_hash`; `instrumented_code_hash`
  and `receipt.code_hash` are §11.4's `CodeHash`, the post-instrumentation identity.
- `receipt.abort_code` is always marshalled, including at `0`, so a reader can see the field on
  every receipt.

## Receipt hash preimages

`state_diff_hash` and `return_hash` follow §15's preimages; every non-`SUCCESS` receipt carries
both as zero per §14.3.

## Regeneration

```
go test ./plugins/wasm -run TestGenerateVectors -update
```

then update the `vectorFileDigest` literal in `../vectors_test.go` with the digest
`TestVectorFileDigest` reports. Never hand-edit the JSON.

## Coverage

| ID | What it freezes |
|---|---|
| `EXEC-001` | pure-return success |
| `EXEC-002` | single-write success with an absent-key `storage_len` observation |
| `EXEC-FAIL-OUTBOX-1` | a failed input carrying a deposit, proving §14.3's discard |
| `EXEC-003` | update an existing key |
| `EXEC-004` | delete an existing key via the EMPTY sentinel |
| `EXEC-005` | write a present-empty value |
| `EXEC-006` | multi-entry StateDiff, sorted by the runtime, not the contract |
| `EXEC-007` | one event, no state change |
| `EXEC-008` | one contract-emitted L1 withdrawal |
| `EXEC-009` | combined put + event + L2 delivery + return data |
| `EXEC-010` | `abort(7)`: a `REVERT` receipt with a non-zero `abort_code` |
| `EXEC-011` | `unreachable`: a non-`REVERT` receipt with `abort_code = 0` |
| `EXEC-012` | absent-key `storage_read`, then write a different key |
| `EXEC-DEP-1` | full claim, no refund |
| `EXEC-DEP-2` | partial claim, refund after the contract's messages |
| `EXEC-DEP-3` | claim above `deposit_amount` |
| `EXEC-DEP-4` | non-zero claim against a zero deposit |
| `CV-OUTBOX-1` | a withdrawal carrying a payload is rejected at the runtime layer |
| `CV-ROOTS-1` | an `ExecutionResult` returned where a `ContractEffects` is expected does not decode as a `ContractEffects` and the input faults |
| `CV-DUPKEY-1` | two StateDiff entries deriving to one key |
| `CV-UNAUTH-1` | a write to a reserved local key |
| `CV-RTL-RETURN-1` | `len(ReturnData) > MaxReturnData` alone |
| `CV-RTL-RESULT-1` | the refund pushes the `ExecutionResult` past `MaxExecutionResult` |
| `CV-OOG-1` | `OUT_OF_GAS`: the pre-execution initial-memory charge is unaffordable, nothing runs |
| `CV-OOG-2` | `OUT_OF_GAS`: an injected segment charge |
| `CV-OOG-3` | `OUT_OF_GAS`: a host-function charge |
| `CV-OOG-4` | `OUT_OF_GAS`: the §10.4 effect charge, step 6 |
| `CV-READ-2` | `storage_read` returns `-2` when the out buffer is too small |
| `CV-OOB-1` | `result_ptr + result_len` outside memory |
| `CV-GROW-1` | `memory.grow` past `MaxMemory` returns `-1`, grows nothing, pays no per-page surcharge |
| `CV-DEPTH-1` | the 513th frame traps |
| `CV-SEG-BRIF-1` | a taken `br_if` skips the rest of its block |
| `CV-SEG-IF-1` | a false `if` skips its arm |
| `CV-SEG-AFTEREND-1` | a branch landing after an `end` |
| `CV-SEG-OUTER-1` | a branch to the function's outermost label |
| `EXEC-DEP-5` | a contract at `MaxOutboxPerExecution` with an unclaimed remainder: `SUCCESS`, `outbox_count = 33`, the refund at `outbox_index = 32` |
| `CV-RTL-BLOB-1` | a well-formed `ContractEffects` blob longer than `MaxResultBlob`: `RESULT_TOO_LARGE`, metered `gas_used` with no effect charge |
| `CV-RTL-BLOB-2` | the same, also malformed, proving step 0 precedes the decode |
| `CV-ASSETS-1` | 17 distinct-asset withdrawals in one input breach the per-input asset footprint: `RUNTIME_FAULT` at the metered gas |
| `CV-ASSETS-2` | 16 distinct-asset withdrawals plus a non-zero deposit in a 17th asset breach the footprint: `RUNTIME_FAULT`, full refund |
| `CV-QUOTA-1` | a write that brings `used` exactly to `purchased` succeeds, and the committed StateDiff carries the updated `0x0005` leaf |
| `CV-QUOTA-2` | net state growth one byte over the remaining quota is `STORAGE_QUOTA_EXCEEDED` at metered plus effect gas, with the full deposit refunded |
| `CV-QUOTA-3` | at a fully used quota, deleting a leaf frees its bytes and a smaller write reuses them without buying more |
| `CV-QUOTA-4` | writes that net to `delta = 0` succeed at a fully used quota, commit no quota entry, and leave `0x0005` out of the witness set |
| `CV-QUOTA-5` | a contract writing the reserved quota key `0x0005` is `UNAUTHORIZED` |
| `CV-DEPLOY-1` | single-chunk deployment start: `chunk_count` 1 for a module within `MaxDeployChunkData` |
| `CV-DEPLOY-2` | the single deploy chunk carrying the whole module at `chunk_index` 0 |
| `EXEC-DEPLOY-1` | deploy finalize: the three reserved leaves are written and `ReturnData` is `code_hash` then `CodeHash` |
| `EXEC-UPGRADE-1` | an authorized upgrade rewrites both code-hash leaves and returns the old then the new `CodeHash` |
| `CV-UPGRADE-UNDEPLOYED-1` | an upgrade naming a `contract_id` with no code-hash leaf is `VALIDATION_FAILED`, not `UNAUTHORIZED` |
| `CV-UPGRADE-UNAUTH-1` | an upgrade of a deployed contract with no upgrade-policy leaf is `UNAUTHORIZED` and its receipt carries the unchanged `CodeHash` |
| `CV-REJECT-WASI-1` | a deploy of a module importing from `wasi_snapshot_preview1` |
| `CV-REJECT-FLOAT-1` | a deploy of a module using a float instruction |
| `CV-REJECT-ATOMIC-1` | a deploy of a module using an atomic instruction |
| `CV-REJECT-SIMD-1` | a deploy of a module using a v128 instruction |
| `CV-REJECT-SIGNEXT-1` | a deploy of a module using a sign-extension instruction |
| `CV-REJECT-SATCONV-1` | a deploy of a module using a saturating float-to-int conversion |
| `EXEC-DEPLOY-2` | an `OWNER_UPGRADEABLE` deploy writes the policy and owner leaves alongside the two code-hash leaves |
| `EXEC-UPGRADE-2` | an authorized upgrade of an `OWNER_UPGRADEABLE` contract leaves its policy and owner leaves untouched |
| `EXEC-TRANSFER-1` | an owner's transfer rewrites only the owner leaf |
| `EXEC-TRANSFER-2` | a renounce (zero `new_owner`) sets the policy to `IMMUTABLE` and deletes the owner leaf |
| `CV-UPGRADE-RENOUNCED-1` | an upgrade against a renounced (`IMMUTABLE`, no owner leaf) contract is `UNAUTHORIZED` |
| `CV-TRANSFER-UNAUTH-1` | a transfer by a non-owner is `UNAUTHORIZED` and its receipt carries the unchanged `CodeHash` |
| `CV-TRANSFER-UNDEPLOYED-1` | a transfer naming a `contract_id` with no code-hash leaf is `VALIDATION_FAILED` |
| `CV-DEPLOY-META-1` | a deploy finalize against pending metadata with an unknown version is `VALIDATION_FAILED` |
| `CV-DEPLOY-META-2` | a deploy finalize against truncated pending metadata is `VALIDATION_FAILED` |
| `CV-DEPLOY-META-3` | a deploy finalize against pending metadata with trailing bytes is `VALIDATION_FAILED` |
| `CV-DEPLOY-META-4` | a deploy finalize against pending metadata with an unlisted `upgrade_policy` is `VALIDATION_FAILED` |
| `CV-DEPLOY-META-5` | a deploy finalize against `IMMUTABLE` pending metadata carrying a non-zero owner is `VALIDATION_FAILED` |
| `CV-DEPLOY-META-6` | a deploy finalize against `OWNER_UPGRADEABLE` pending metadata carrying a zero owner is `VALIDATION_FAILED` |
| `CV-QUOTA-6` | `BuyStorage` adds `bytes` to `purchased` and leaves `used` unchanged |
| `CV-QUOTA-7` | `BuyStorage` naming a `contract_id` with no code-hash leaf is `VALIDATION_FAILED` |
| `CV-QUOTA-8` | `BuyStorage` whose `purchased + bytes` is `2^64` is `VALIDATION_FAILED`, and its receipt carries the unchanged `CodeHash` |
| `EXEC-UNSUP-1` | an unrecognized `input_kind` is refused deterministically as `UNSUPPORTED_OPERATION` |

`CV-DEPLOY-1` and `CV-DEPLOY-2` together are the single-chunk sizing case: the kind-2 record
declaring `chunk_count == 1` and the kind-3 chunk that carries the whole module.
