# execution-conformance-v1

## What this is

The repo-owned source copy of the WASM domain's execution conformance artifact, produced by
driving `Plugin.DecodeInput`/`Plugin.Apply` over real modules through the real engine. The copy
in `zeno-docs` under `wasm-vm/` was written from this one and is byte-identical to it **as of
this snapshot**. The two are kept in sync by hand and no automated check enforces the equality,
because the two copies now live in different repositories. The pattern such a check would follow
is `../internal/module/gastable_test.go` `TestArtifactDrift`, which compares the embedded gas
table against a vendored copy checked into this repository's own `testdata/`, not against
`zeno-docs`.

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
   Every module here is hand-built. The `CV-STRUCT-*` modules sit at or one above a §25.1 cap,
   and `CV-INSTR-SIZE-1`'s module, about 148 KiB, is the largest, against a 256 KiB
   `MaxCodeSize` (`../limits.go`). Every other module has at most two declared types, two
   functions and two data segments.
4. §15 states the `state_diff_hash`/`return_hash` preimages and §14.3 states the non-`SUCCESS`
   `ReturnData` discard normatively, except `REVERT`'s revert data.
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

Seven vector shapes, distinguished by `shape`:

| Shape | `input_frame_hex` | `module_wasm_hex` | `source_data_hex` | `input` | notes |
|---|---|---|---|---|---|
| `call` (kinds 0/1) | present | present | absent | present | full pipeline |
| `deploy` (kinds 2-5) | absent | present | present iff kind 3 | present | code-store seeding, §15 identity; ReturnData = code_hash ‖ CodeHash (§11.3, §12) on a SUCCESS finalize or upgrade only — a rejected upgrade carries no ReturnData and no state diff, and an UNAUTHORIZED one, or a VALIDATION_FAILED one against a deployed contract, carries the target's unchanged receipt.code_hash; `pending_metadata_hex` for kinds 4/5 |
| `ownership` (kind 6) | absent | absent | absent | present | frame absent, module absent, source data absent, input present |
| `validation_rejection` | absent | present | absent | present | kind 4; module never runs; `VALIDATION_FAILED` with exactly one cause: the module's validation, its instrumentation, the pending metadata, or the assembly hash (`code_hash` ≠ `input.code_hash`) |
| `buy_storage` (kind 7) | absent | absent | absent | present | frame absent, module absent, source data absent, input present; witness set is the `0x0001` and `0x0005` leaves on every outcome |
| `undeployed_target` (kinds 0/1) | absent | absent | absent | present | the target has no `0x0001` leaf, so no module or frame exists and nothing runs; `VALIDATION_FAILED` at `gas_used` 0 with a zero `receipt.code_hash`; the full deposit is refunded; witness set is exactly the target's `0x0001` and `0x0002` leaves, both absent |
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
- `contract_effects_blob_hex` absent means the run returned no blob or a zero-length blob.
  `CV-DECODE-9`'s module returns a zero-length blob, which renders the same as no blob; its
  status (`RUNTIME_FAULT`, because an empty blob does not decode) and its `gas_used` pin the
  outcome.
- A vector's top-level `code_hash` is SPEC's pre-instrumentation `code_hash`; `instrumented_code_hash`
  and `receipt.code_hash` are §11.4's `CodeHash`, the post-instrumentation identity.
- `withdrawal_caps` is always marshalled: an object from a 64-hex `AssetID` to its 64-hex big-endian
  u256 `MaxBatchWithdrawal`. An asset absent from it has cap 0 (§27.9). The generator always assigns a
  non-nil map, so a vector with no table renders `{}`.
- `receipt.abort_code` is always marshalled, including at `0`, so a reader can see the field on
  every receipt.

## Receipt hash preimages

`state_diff_hash` and `return_hash` follow §15's preimages; every non-`SUCCESS` receipt carries
both as zero per §14.3, except `REVERT`, whose `return_hash` is `SHA3-256` of its revert data
(zero when the revert data is empty).

## Regeneration

```
go test ./plugins/wasm -run TestGenerateVectors -update
```

then update the `vectorFileDigest` literal in `../vectors_test.go` with the digest
`TestVectorFileDigest` reports. Never hand-edit the JSON.

## Coverage

Vector IDs are stable names; gaps in numbering are intentional.

| ID | What it freezes |
|---|---|
| `EXEC-001` | pure-return success |
| `EXEC-002` | single-write success with an absent-key `storage_len` observation |
| `EXEC-003` | update an existing key |
| `EXEC-004` | delete an existing key via the EMPTY sentinel |
| `EXEC-005` | write a present-empty value |
| `EXEC-006` | multi-entry StateDiff, sorted by the runtime, not the contract |
| `EXEC-007` | one event, no state change |
| `EXEC-008` | one contract-emitted L1 withdrawal |
| `EXEC-009` | combined put + event + L2 delivery + return data |
| `EXEC-010` | `abort(7)` with `data_len` 0: a `REVERT` receipt with a non-zero `abort_code` |
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
| `CV-ASSETS-1` | 17 distinct-asset withdrawals in one input, every asset capped above its total, breach the per-input asset footprint, so only the footprint faults: `RUNTIME_FAULT` at the metered gas |
| `CV-ASSETS-2` | 16 distinct-asset withdrawals plus a non-zero deposit in a 17th asset, every asset capped above its total, breach the footprint, so only the footprint faults: `RUNTIME_FAULT`, full refund |
| `CV-QUOTA-1` | a write that brings `used` exactly to `purchased` succeeds, and the committed StateDiff carries the updated `0x0005` leaf |
| `CV-QUOTA-2` | net state growth one byte over the remaining quota is `STORAGE_QUOTA_EXCEEDED` at metered plus effect gas, with the full deposit refunded |
| `CV-QUOTA-3` | at a fully used quota, deleting a leaf frees its bytes and a smaller write reuses them without buying more |
| `CV-QUOTA-4` | writes that net to `delta = 0` succeed at a fully used quota, commit no quota entry, and leave `0x0005` out of the witness set |
| `CV-QUOTA-5` | a contract writing the reserved quota key `0x0005` is `UNAUTHORIZED` |
| `CV-CAP-1` | two withdrawals in one asset summing exactly to its `MaxBatchWithdrawal` pass step 5a |
| `CV-CAP-2` | two withdrawals in one asset summing to one above its `MaxBatchWithdrawal`: `RUNTIME_FAULT` at the metered gas, full rollback and the full deposit refunded |
| `CV-CAP-3` | a non-zero withdrawal in an asset with no AssetConfig (absent from `withdrawal_caps`, cap 0): `RUNTIME_FAULT`, full rollback and the full deposit refunded |
| `CV-CAP-4` | a zero-amount withdrawal in an asset with no AssetConfig (cap 0) passes step 5a |
| `CV-CAP-5` | five withdrawals in three assets with different caps (300, 100, 200 in ascending asset order): the outbox lists 100-cap asset 25, 300-cap asset 100, 100-cap asset 75, 200-cap asset 200, 300-cap asset 200, so the first-appearance order is neither ascending nor descending, the first three withdrawals repeat an asset, and each asset totals exactly its own `MaxBatchWithdrawal` (100, 300, 200): step 5a passes and the status is `SUCCESS`; the 300-cap and 100-cap assets differ only in the first ZTS byte (byte 22) and the 100-cap and 200-cap assets only in the last (byte 31), so a total keyed by a truncated asset ID merges two assets and faults |
| `CV-CAP-6` | three assets with caps 300, 100, 200 in ascending asset order and six withdrawals 25 (200-cap asset), 25 (200-cap asset), 60 (100-cap asset), 50 (300-cap asset), 60 (100-cap asset), 150 (300-cap asset): the 100-cap asset is the middle asset in ascending and in first-appearance order, its two withdrawals are identical (60) and each is within its cap but their total 120 is over it, the last withdrawal is of another asset and another asset has the largest total (200), and the other two assets are within their own caps: `RUNTIME_FAULT` at the metered gas, full rollback and the full deposit refunded |
| `CV-CAP-7` | a withdrawal over its cap at a `plasma_limit` that leaves nothing for the §10.4 effect charge: `RUNTIME_FAULT` from step 5a, not `OUT_OF_GAS` from step 6 |
| `CV-CAP-8` | two withdrawals of 2^255 in one asset capped at 2^256 - 1: each is within the cap and the exact total 2^256 is over it, though a u256 sum wraps to 0: `RUNTIME_FAULT` at the metered gas from step 5a; with a state write, an event and a deposit: full rollback and the full deposit refunded |
| `CV-CAP-9` | withdrawals of 2^64 + 1 and 2^128 + 1 in one asset capped at 2^129: the exact total is within the cap and step 5a passes |
| `CV-CAP-10` | withdrawals of 2^128 and 2^128 + 2^64 in one asset capped at 2^128 + 2^64: each is within the cap and the exact total is over it, though the total truncated to 64 or 128 bits is within it: `RUNTIME_FAULT` at the metered gas; with a state write, an event and a deposit: full rollback and the full deposit refunded |
| `CV-CAP-11` | withdrawals of 2^192 - 1 and 1 in one asset capped at 2^192 - 1, with a state write, an event and a deposit: the exact total 2^192 carries across the 2^64, 2^128 and 2^192 limb boundaries and is over the cap: `RUNTIME_FAULT` at the metered gas, full rollback and the full deposit refunded |
| `CV-CAP-12` | withdrawals of 600 and 400 in the deposit asset, capped at 1000, with an unclaimed deposit of 1000: the deposit and its refund do not count toward the cap, so step 5a passes and the refund follows the two withdrawals |
| `CV-ASSETS-3` | 16 distinct-asset withdrawals, every asset capped above its total, fill the per-input asset footprint exactly and pass step 5a |
| `CV-ASSETS-4` | 16 distinct-asset withdrawals plus a non-zero deposit in one of those assets: the deposit asset is already in the footprint, which stays at 16, and step 5a passes |
| `CV-ASSETS-5` | 16 capped distinct-asset withdrawals plus a zero-amount withdrawal in a 17th, uncapped asset breach the footprint, since a zero-amount withdrawal still names its asset: `RUNTIME_FAULT` at the metered gas |
| `CV-ORDER-1` | a wrong input index and a reserved-key write: step 2 precedes step 3, so `RUNTIME_FAULT`, not `UNAUTHORIZED` |
| `CV-ORDER-2` | an event attributed to another contract and a reserved-key write: step 2's attribution check precedes step 3, so `RUNTIME_FAULT`, not `UNAUTHORIZED` |
| `CV-ORDER-3` | a duplicated local key, then a reserved key: the step 3 scan runs in blob order and meets the duplicate first, so `RUNTIME_FAULT`, not `UNAUTHORIZED` |
| `CV-ORDER-4` | a reserved key, then a duplicated local key: the step 3 scan runs in blob order and meets the reserved key first, so `UNAUTHORIZED`, not `RUNTIME_FAULT` |
| `CV-ORDER-5` | a reserved-key write followed by one trailing byte: step 1 decodes the whole blob before step 3 scans it, so `RUNTIME_FAULT`, not `UNAUTHORIZED` |
| `CV-ORDER-6` | a reserved-key write and a non-zero claim with no deposit: step 3 precedes step 4, so `UNAUTHORIZED`, not `RUNTIME_FAULT` |
| `CV-ORDER-7` | a reserved-key write and a withdrawal over its cap: step 3 precedes step 5a, so `UNAUTHORIZED`, not `RUNTIME_FAULT` |
| `CV-ORDER-8` | a wrong input index at a `plasma_limit` that leaves nothing for the effect charge: step 2 precedes step 6, so `RUNTIME_FAULT`, not `OUT_OF_GAS` |
| `CV-ORDER-9` | a reserved-key write at a `plasma_limit` that leaves nothing for the effect charge: step 3 precedes step 6, so `UNAUTHORIZED`, not `OUT_OF_GAS` |
| `CV-ORDER-10` | a non-zero claim with no deposit at a `plasma_limit` that leaves nothing for the effect charge: step 4 precedes step 6, so `RUNTIME_FAULT`, not `OUT_OF_GAS` |
| `CV-ORDER-11` | a write one byte over the remaining quota at a `plasma_limit` that leaves nothing for the effect charge: step 6 precedes step 6a, so `OUT_OF_GAS`, not `STORAGE_QUOTA_EXCEEDED` |
| `CV-ORDER-12` | a write one byte over the remaining quota and ReturnData over `MaxReturnData`: step 6a precedes step 7, so `STORAGE_QUOTA_EXCEEDED` at metered plus effect gas, not `RESULT_TOO_LARGE` |
| `CV-ORDER-13` | ReturnData over `MaxReturnData` at a `plasma_limit` that leaves nothing for the effect charge: step 6 precedes step 7, so `OUT_OF_GAS`, not `RESULT_TOO_LARGE` |
| `CV-ORDER-14` | an outbox message whose `source_contract` is another contract, and a reserved-key write: step 2's outbox attribution check precedes step 3, so `RUNTIME_FAULT`, not `UNAUTHORIZED` |
| `CV-ORDER-15` | withdrawals naming 17 distinct capped assets at a `plasma_limit` that leaves nothing for the effect charge: the step 5a footprint check precedes step 6, so `RUNTIME_FAULT`, not `OUT_OF_GAS` |
| `CV-ORDER-16` | withdrawals naming 17 distinct capped assets and a reserved-key write: step 3 precedes the step 5a footprint check, so `UNAUTHORIZED`, not `RUNTIME_FAULT` |
| `CV-ATTR-1` | an event whose `contract_id` is another contract: `RUNTIME_FAULT` at the metered gas |
| `CV-ATTR-2` | an L2 delivery whose `source_contract` is another, non-zero contract: `RUNTIME_FAULT` at the metered gas |
| `CV-ATTR-3` | a capped withdrawal whose `source_contract` is `bytes32(0)`, the runtime's refund marker, which a contract cannot emit: `RUNTIME_FAULT` at the metered gas |
| `CV-DECODE-1` | a blob whose `executionresult_version` is 2: an unknown version is rejected, so `RUNTIME_FAULT` at the metered gas |
| `CV-DECODE-2` | an event whose `input_index` is not the executing input's: the decoder rejects it (§27.1a), so `RUNTIME_FAULT` at the metered gas |
| `CV-DECODE-3` | an outbox message whose `input_index` is not the executing input's: the decoder rejects it (§27.1a), so `RUNTIME_FAULT` at the metered gas |
| `CV-DECODE-4` | one trailing byte after `claimed_deposit`: `RUNTIME_FAULT` at the metered gas |
| `CV-DECODE-5` | a blob that ends after ReturnData, omitting `claimed_deposit`: the field never defaults to 0, so `RUNTIME_FAULT` at the metered gas |
| `CV-DECODE-6` | a ReturnData length prefix one above the bytes present: the prefix is never repaired, so `RUNTIME_FAULT` at the metered gas |
| `CV-DECODE-7` | an outbox message of kind 2: only kinds 0 and 1 exist, so `RUNTIME_FAULT` at the metered gas |
| `CV-DECODE-8` | a zero-amount withdrawal naming a non-canonical AssetID whose only non-zero byte in `[0:22]` is byte 21, the last byte that must be zero: the decoder rejects it, so `RUNTIME_FAULT` at the metered gas |
| `CV-DECODE-9` | `run` returns a zero-length blob: an empty blob does not decode as a ContractEffects, so `RUNTIME_FAULT` at the metered gas |
| `CV-DECODE-10` | two events with `event_index` 0 and 2: the gap is rejected, so `RUNTIME_FAULT` at the metered gas |
| `CV-DECODE-11` | two outbox messages with `outbox_index` 0 and 2: the gap is rejected, so `RUNTIME_FAULT` at the metered gas |
| `CV-LIMIT-1` | a StateDiff local key of `MaxStateKeySize` + 1 bytes: `RUNTIME_FAULT` at the metered gas |
| `CV-LIMIT-2` | a StateDiff local key of exactly `MaxStateKeySize` bytes is accepted |
| `CV-LIMIT-3` | a StateDiff value of `MaxStateValueSize` + 1 bytes, every byte present: `RUNTIME_FAULT` at the metered gas |
| `CV-LIMIT-4` | `MaxEventsPerExecution` + 1 events: `RUNTIME_FAULT` at the metered gas |
| `CV-LIMIT-5` | exactly `MaxEventsPerExecution` events are accepted |
| `CV-LIMIT-6` | an event with `MaxTopicsPerEvent` + 1 topics: `RUNTIME_FAULT` at the metered gas |
| `CV-LIMIT-7` | an event with exactly `MaxTopicsPerEvent` topics is accepted |
| `CV-LIMIT-8` | an event with `MaxEventDataSize` + 1 data bytes: `RUNTIME_FAULT` at the metered gas |
| `CV-LIMIT-9` | an event with exactly `MaxEventDataSize` data bytes is accepted |
| `CV-LIMIT-10` | an L2 delivery with a payload of `MaxOutboxPayloadSize` + 1 bytes: `RUNTIME_FAULT` at the metered gas |
| `CV-LIMIT-11` | an L2 delivery with a payload of exactly `MaxOutboxPayloadSize` bytes is accepted |
| `CV-LIMIT-12` | a blob with `MaxOutboxPerExecution` + 1 messages: the ExecutionResult's refund slot does not widen a contract's own bound, so `RUNTIME_FAULT` at the metered gas |
| `CV-LIMIT-13` | a StateDiff value of exactly `MaxStateValueSize` bytes is accepted |
| `CV-DECODE-12` | a blob whose `executionresult_version` is 0: only version 1 is known, so `RUNTIME_FAULT` at the metered gas |
| `CV-DECODE-13` | a lone event with `event_index` 1: event indices start at 0, so `RUNTIME_FAULT` at the metered gas |
| `CV-DECODE-14` | a lone outbox message with `outbox_index` 1: outbox indices start at 0, so `RUNTIME_FAULT` at the metered gas |
| `CV-DECODE-15` | an outbox message of kind 2 followed by an `L1_WITHDRAWAL` body: only kinds 0 and 1 exist, and kind 2 is never read as a withdrawal, so `RUNTIME_FAULT` at the metered gas |
| `CV-DECODE-16` | an Events count prefix of 2 with one event present: the count is never treated as an upper bound, so `RUNTIME_FAULT` at the metered gas |
| `CV-STATE-1` | deleting a key absent from the pre-state: a well-formed no-op that commits its `EMPTY` entry, leaves the root unchanged and reads no quota leaf |
| `CV-STATE-2` | an `EMPTY` deletion sentinel followed by stray value bytes: `EMPTY` carries no value bytes, so `RUNTIME_FAULT` at the metered gas |
| `CV-STATE-3` | rewriting a key with its current value: the entry is committed, the root is unchanged, and the net quota change is 0, so no quota entry is committed |
| `CV-RESERVED-1` | writes to `0x00`, `0x0000`, `0x0006` and `0x000100`, each near a reserved key but none equal to one: reserved keys match exactly, never by prefix or range, so `SUCCESS` |
| `CV-QUOTA-9` | a write by a contract with no quota leaf: an absent leaf means purchased = used = 0, so `STORAGE_QUOTA_EXCEEDED` at metered plus effect gas |
| `EXEC-DEP-6` | a payable `CallL2` with a payload and a deposit of 1000 claims 400: `SUCCESS`, and the 600 refund follows the contract's message |
| `EXEC-DEP-7` | `abort(7)` with `data_len` 0 on an input carrying a deposit: `REVERT`, and the full deposit is refunded |
| `EXEC-DEP-8` | the pre-execution initial-memory charge is unaffordable on an input carrying a deposit: `OUT_OF_GAS`, and the full deposit is refunded |
| `EXEC-DEP-9` | a reserved-key write on an input carrying a deposit: `UNAUTHORIZED`, and the full deposit is refunded |
| `EXEC-DEP-10` | a blob longer than `MaxResultBlob` on an input carrying a deposit: `RESULT_TOO_LARGE` from step 0, and the full deposit is refunded |
| `CV-DECODE-17` | a zero-amount withdrawal naming a non-canonical AssetID whose only non-zero byte in `[0:22]` is byte 0, the first byte that must be zero: the decoder rejects it, so `RUNTIME_FAULT` at the metered gas |
| `CV-DECODE-18` | three events with `event_index` 0, 2 and 1: the list is a gap-free permutation but not ascending, so the decoder rejects it, so `RUNTIME_FAULT` at the metered gas |
| `CV-DECODE-19` | three outbox messages with `outbox_index` 0, 2 and 1: the list is a gap-free permutation but not ascending, so the decoder rejects it, so `RUNTIME_FAULT` at the metered gas |
| `CV-ABORT-1` | `abort(7)` with 40 bytes of revert data: `REVERT`, `abort_code` 7, `return_hash` = `SHA3-256` of the revert data |
| `CV-ABORT-2` | `abort` with `data_len` 0 and `data_ptr` 0xFFFFFFFF: `REVERT` with empty revert data and a zero `return_hash`; the pointer is never read |
| `CV-ABORT-3` | `data_len` = `MaxReturnData` is accepted: `REVERT` carrying all of it |
| `CV-ABORT-4` | `data_len` = `MaxReturnData` + 1 over a range outside memory: `RESULT_TOO_LARGE`, not `RUNTIME_FAULT`, so memory is not read |
| `CV-ABORT-5` | a revert-data range crossing the end of memory traps: `RUNTIME_FAULT` at the metered gas, including abort's charge |
| `CV-ABORT-6` | abort's data charge is unaffordable though its base is not: `OUT_OF_GAS` |
| `EXEC-DEP-11` | a payable `CallL2` that aborts with revert data: `REVERT`, and the full deposit refund sits beside the revert data |
| `EXEC-UNSUP-1` | an unrecognized `input_kind` is refused deterministically as `UNSUPPORTED_OPERATION` |
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
| `CV-DEPLOY-3` | a deploy finalize of a module longer than `MaxDeployChunkData`: its two chunks assemble in ascending `chunk_index` and the deploy succeeds |
| `CV-DEPLOY-HASH-1` | a deploy finalize whose staged chunks pass every per-chunk check but assemble to bytes that do not hash to `code_hash` is `VALIDATION_FAILED` and creates no contract |
| `CV-UPGRADE-UNAUTH-2` | an upgrade of an `OWNER_UPGRADEABLE` contract by a caller other than its owner is `UNAUTHORIZED` and its receipt carries the unchanged `CodeHash` |
| `CV-UPGRADE-ORDER-1` | an upgrade by a non-owner whose new module also fails validation is `UNAUTHORIZED`, not `VALIDATION_FAILED`: authorization precedes the §11.3 pipeline |
| `CV-UPGRADE-INVALID-1` | an authorized upgrade whose new module fails validation is `VALIDATION_FAILED`, with no code change and the unchanged `CodeHash` in its receipt |
| `CV-UPGRADE-HASH-1` | an authorized upgrade whose staged chunks assemble to bytes that do not hash to `new_code_hash` is `VALIDATION_FAILED`, with no code change and the unchanged `CodeHash` in its receipt |
| `CV-UPGRADE-SAMEHASH-1` | an authorized upgrade that re-stages the contract's current module is `VALIDATION_FAILED`: an upgrade must produce a new `CodeHash` |
| `CV-UPGRADE-SAMEHASH-2` | an authorized upgrade to a different module that instruments to the contract's current `CodeHash` is `VALIDATION_FAILED`: the comparison is on `CodeHash`, not `code_hash` |
| `EXEC-FAIL-OUTBOX-1` | a failed input carrying a deposit, proving §14.3's discard |
| `CV-FRAME-1` | the contract returns the frame's `momentum_hash` as its `ReturnData` and takes its `input_index` from the frame: `SUCCESS`, and `return_hash` commits the momentum hash |
| `CV-LOCALS-1` | a function declaring `MaxLocalsPerFunction` (8,192) `i64` locals pays `ceil(8192 / locals_per_gas)` once at entry: `SUCCESS` at the metered gas |
| `CV-LOCALS-2` | the same call one gas short of that function's entry segment: `OUT_OF_GAS` before its first instruction runs |
| `CV-TRAP-DIV-1` | integer division by zero (`i32.div_u`) traps: `RUNTIME_FAULT` at the metered gas |
| `CV-TRAP-OVF-1` | `i32.div_s` of `-2^31` by `-1` overflows and traps: `RUNTIME_FAULT` at the metered gas |
| `CV-TRAP-MEM-1` | an `i32.load` whose four bytes cross the end of memory traps, though its first three bytes are in bounds: `RUNTIME_FAULT` at the metered gas |
| `CV-TRAP-TABLE-1` | a `call_indirect` past the end of the table traps: `RUNTIME_FAULT` at the metered gas |
| `CV-TRAP-NULL-1` | a `call_indirect` to an uninitialized table element traps: `RUNTIME_FAULT` at the metered gas |
| `CV-TRAP-SIG-1` | a `call_indirect` whose expected type differs from the function's type traps: `RUNTIME_FAULT` at the metered gas |
| `CV-NEG-1` | `run` returns `-7`: `REVERT` with `abort_code` 7, empty revert data and a zero `return_hash` |
| `CV-NEG-2` | `run` returns `-2^63`: `(-r)` truncated to `i32` is 0, so `REVERT` with `abort_code` 0 |
| `CV-FRAME-2` | the input frame is written after the data segments: a segment over bytes 0 to 265 keeps only its bytes past the 258-byte frame, and memory past the segment is zero |
| `CV-FRAME-3` | a module declaring 0 initial pages cannot hold the frame: `RUNTIME_FAULT`, `run` is never called, and `gas_used` is the initial-memory charge, 0 |
| `CV-GROW-2` | a module declaring a memory maximum of 1 page: `memory.grow` by 1 returns `-1` and pays no per-page surcharge |
| `CV-GROW-3` | a module declaring no memory maximum grows from 1 page to exactly `MaxMemory` (64 pages), and one more page returns `-1` |
| `CV-GROW-4` | a module declaring a memory maximum of 65 pages is capped at `MaxMemory`: it grows to 64 pages, and one more page returns `-1` |
| `CV-GROW-5` | a module declaring a memory maximum of 2 pages grows to exactly 2: `memory.grow` returns the old size 1, and `memory.size` is then 2 |
| `CV-READ-3` | `storage_read` of an absent key returns `-1` though its out buffer crosses the end of memory: the absent check precedes the out-buffer check |
| `CV-READ-4` | `storage_read` returns `-2` when `out_len` is below the value's length, though its out buffer crosses the end of memory: that path never checks the buffer |
| `CV-READ-5` | `storage_read` traps when its out buffer crosses the end of memory, though the value's 8 bytes at `out_ptr` would fit: `RUNTIME_FAULT` at the metered gas, including the read charge |
| `CV-READ-6` | `storage_read` into a 16-byte out buffer writes exactly the value's 8 bytes at `out_ptr` and returns 8: the buffer's other 8 bytes keep their contents |
| `CV-READ-7` | `storage_read` traps when its key range crosses the end of memory, though the key is absent: `RUNTIME_FAULT` at the metered gas, because the key-range check precedes the absent check |
| `CV-EVENTS-1` | three distinct events: `SUCCESS`, the result lists them in `event_index` order, and the per-input `event_root` promotes the odd third leaf unchanged |
| `EXEC-DEP-12` | a `Deposit` of 1000 whose contract claims 0 and emits nothing: `SUCCESS`, and the full refund is the only outbox message, at `outbox_index` 0 |
| `CV-MEM-MIN-1` | a deploy of a module declaring 65 initial pages, one above `MaxMemory`, is `VALIDATION_FAILED` |
| `CV-STRUCT-TYPES-1` | a deploy of a module declaring `MaxTypeCount` + 1 (1,025) function types is `VALIDATION_FAILED` |
| `CV-STRUCT-TYPEPARAMS-1` | a deploy of a module declaring an unused function type with `MaxLocalsPerFunction` + 1 (8,193) parameters is `VALIDATION_FAILED`, though the type section's total stays under `MaxTotalParams` |
| `CV-STRUCT-PARAMS-1` | a deploy of a module whose function types declare `MaxTotalParams` + 1 (65,537) parameters in total, none above `MaxLocalsPerFunction`, is `VALIDATION_FAILED` |
| `CV-STRUCT-FUNCS-1` | a deploy of a module defining `MaxFunctions` + 1 (8,193) functions is `VALIDATION_FAILED` |
| `CV-STRUCT-TABLE-1` | a deploy of a module whose table declares a minimum of `MaxTableEntries` + 1 (10,001) entries is `VALIDATION_FAILED` |
| `CV-STRUCT-GLOBALS-1` | a deploy of a module declaring `MaxGlobals` + 1 (1,025) globals is `VALIDATION_FAILED` |
| `CV-STRUCT-ELEMS-1` | a deploy of a module whose two element segments hold `MaxTotalElementEntries` + 1 (16,385) entries in total, each within the table, is `VALIDATION_FAILED` |
| `CV-STRUCT-DATA-1` | a deploy of a module declaring `MaxDataSegments` + 1 (1,025) data segments is `VALIDATION_FAILED` |
| `CV-STRUCT-LOCALS-1` | a deploy of a module whose `run` has 2 parameters and 8,191 declared locals, `MaxLocalsPerFunction` + 1 in total, is `VALIDATION_FAILED`: parameters count toward the cap |
| `CV-STRUCT-TOTALLOCALS-1` | a deploy of a module whose parameters and declared locals sum to `MaxTotalLocals` + 1 across its functions, with the declared locals alone one below the cap, is `VALIDATION_FAILED`: parameters count toward the cap |
| `CV-STRUCT-ATCAP-1` | a deploy of a module exactly at `MaxTypeCount`, `MaxTotalParams`, `MaxFunctions`, `MaxLocalsPerFunction`, `MaxGlobals`, `MaxDataSegments`, `MaxTableEntries` and `MaxTotalElementEntries`, with an element segment ending exactly at the table minimum and a data segment ending exactly at the end of initial memory, succeeds |
| `CV-FIT-DATA-1` | a deploy of a module whose active data segment at offset 65,535 holds 2 bytes, one past its 1-page initial memory, is `VALIDATION_FAILED` at deploy, not at its first call |
| `CV-FIT-ELEM-1` | a deploy of a module whose active element segment at offset 1 holds 2 entries, one past its table minimum of 2, is `VALIDATION_FAILED` at deploy, not at its first call |
| `CV-IMPORT-MODULE-1` | a deploy of a module importing `storage_len`, with its signature, from module `zenon` rather than `env` is `VALIDATION_FAILED` |
| `CV-IMPORT-NAME-1` | a deploy of a module importing `env.storage_write`, a name outside the three host functions, is `VALIDATION_FAILED` |
| `CV-IMPORT-DUP-1` | a deploy of a module importing `env.storage_len` twice is `VALIDATION_FAILED` |
| `CV-IMPORT-SIG-1` | a deploy of a module importing `env.abort` with the one-parameter signature `(i32) -> ()` is `VALIDATION_FAILED` |
| `CV-IMPORT-KIND-1` | a deploy of a module importing `env.storage_len` as an `i32` global rather than a function is `VALIDATION_FAILED` |
| `CV-SECTION-START-1` | a deploy of a module with a start section naming a `() -> ()` function is `VALIDATION_FAILED` |
| `CV-SECTION-CUSTOM-1` | a deploy of a module whose only defect is a trailing `name` custom section is `VALIDATION_FAILED` |
| `CV-SECTION-CUSTOM-2` | a deploy of a module whose only defect is a trailing empty custom section named `zeno` is `VALIDATION_FAILED` |
| `CV-EXPORT-MUTGLOBAL-1` | a deploy of a module exporting a mutable `i32` global is `VALIDATION_FAILED` |
| `CV-EXPORT-START-1` | a deploy of a module that also exports `run` under the name `_start` is `VALIDATION_FAILED` |
| `CV-EXPORT-ZENON-1` | a deploy of a module that also exports `run` under the name `__zenon_gas`, which the instrumenter never injects, is `VALIDATION_FAILED`: every name beginning `__zenon_` is reserved |
| `CV-EXPORT-NORUN-1` | a deploy of a module exporting `memory` but no `run` is `VALIDATION_FAILED` |
| `CV-EXPORT-NOMEMORY-1` | a deploy of a module that declares a memory but exports only `run` is `VALIDATION_FAILED` |
| `CV-EXPORT-RUNSIG-1` | a deploy of a module whose `run` is `(i32, i32) -> i32` is `VALIDATION_FAILED` |
| `CV-EXPORT-RUNIMPORT-1` | a deploy of a module whose `run` export names its `env.storage_len` import, which has `run`'s signature, is `VALIDATION_FAILED`: `run` must be a function the module defines |
| `CV-INSTR-SIZE-1` | a deploy of a module under `MaxCodeSize` whose `run` is `i64.const 0` then 151,000 `return`s: each `return` instruments to 7 bytes, so the instrumented module exceeds `MaxInstrumentedCodeSize` (1 MiB) and the deploy is `VALIDATION_FAILED` |
| `CV-LEB-PADDED-1` | a deploy of a module whose only defect is `run`'s locals vector count 0 encoded in two bytes (`80 00`), within Core 1.0's five-byte limit, is `VALIDATION_FAILED`: every LEB128 integer must use its shortest encoding |
| `CV-DEPOSIT-UNDEPLOYED-1` | a `Deposit` of 1000 naming a `target_contract` with no code-hash leaf is `VALIDATION_FAILED` at `gas_used` 0 with a zero receipt `code_hash`; the full deposit is refunded as the only outbox message, and the witness set is exactly the target's absent `0x0001` and `0x0002` leaves |
| `CV-CALL-UNDEPLOYED-1` | a payable `CallL2` with a payload and a deposit of 1000 naming a `target_contract` with no code-hash leaf has the same outcome as a `Deposit`: `VALIDATION_FAILED` at `gas_used` 0, the full deposit refunded, and a witness set of exactly the target's absent `0x0001` and `0x0002` leaves |

`CV-DEPLOY-1` and `CV-DEPLOY-2` together are the single-chunk sizing case: the kind-2 record
declaring `chunk_count == 1` and the kind-3 chunk that carries the whole module.

A kind-4 or kind-5 vector's pending record is staged by `l1_sender` under `input.code_hash`
(kind 4) or `input.new_code_hash` (kind 5). Its `total_size` is the length of `module_wasm_hex`,
its `chunk_count` is `ceil(total_size / MaxDeployChunkData)` and its metadata is
`pending_metadata_hex`. Chunk `i` is the bytes of `module_wasm_hex` from `i ×
MaxDeployChunkData` up to the next multiple of `MaxDeployChunkData` or the end. The top-level
`code_hash` is `SHA3-256(module_wasm_hex)`. Only `CV-DEPLOY-HASH-1` and `CV-UPGRADE-HASH-1`
stage bytes whose `code_hash` differs from the one their input names.

Two §11.3 assembly checks have no vector, because no input sequence Settlement accepts can fail
them. A length mismatch cannot arise: §11.2 fixes every chunk's size, so the accepted chunks
always total `total_size`. A `total_size` or `chunk_count` out of bounds cannot arise: §11.1
rejects that start at its receive. go-executor still checks both
(`TestApplyDeployFinalizeAssemblyVerification` and `TestApplyDeployFinalizeRecordBounds` in
`../deploy_test.go`).

`MaxCodeSize` has no rejection vector for the same reason: §11.1 rejects a start whose
`total_size` exceeds it, so no finalize assembles a larger module. `MaxTotalLocals` has a vector
one above the cap and none at it: a module at the cap drives about 55 MB of allocation per
engine compile (`../limits.go`), and the generator and the determinism matrix derive every
deploy vector 13 times, compiling its module twice each time. No vector deploys a module
declaring a function type with exactly `MaxLocalsPerFunction` parameters successfully: one such
type costs about an eighth of the 808 MB of compile allocation that `../limits.go` measures for
eight of them, on each of those 26 compiles. §8.1's fourth-import rule has no vector of its
own: a fourth import repeats a name or names a symbol outside the three, which
`CV-IMPORT-DUP-1` and `CV-IMPORT-NAME-1` pin.

`env.abort` takes three parameters: `code`, `data_ptr` and `data_len`. Deploy validation rejects a module that imports the one-parameter form (`CV-IMPORT-SIG-1`). A module with that import that is already in a store fails instantiation with an import type mismatch, and the call path returns that error to the driver, which halts the batch. A network with such state needs a fresh genesis or a devnet wipe.
