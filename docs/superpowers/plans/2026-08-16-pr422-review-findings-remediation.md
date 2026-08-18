# PR #422 Review Findings Remediation Implementation Plan

> **For AI workers:** Required execution skill: use `subagent-driven-development`
> for the three file-disjoint workstreams. Track every step with this checklist.
> Commit and push are intentionally omitted because the active recovery authority
> forbids them.

**Goal:** Close the four confirmed exact-Head PR #422 findings without changing
Storage disk format or Vector product behavior, and retain auditable exact-Head
Oracle evidence.

**Architecture:** Storage rebinding becomes a current/immediate-predecessor
state transition. The Oracle controller executes a Git-object-derived frozen
Kiwi tree and atomically publishes a bounded canonical evidence document before
provenance v4. The existing raw RESP parser gains a shared top-level frame
budget and monotonic absolute deadline.

**Tech stack:** Rust 1.97.1, Python 3, Bash, GitHub Actions, RocksDB migration
manifests, Linux mount namespaces, RESP2/RESP3, Redis 8.8.1 exact commit
`77b6c308396c9700672390a210143a8496fb4b10`.

**Authority:** Inspect, edit source/tests/CI/docs, and run tests. Do not commit,
push, merge, rebase, reset, clean, modify PR metadata, resolve review threads,
or close Issues.

---

## File Responsibilities

- `src/storage/src/storage_migration.rs`: derive and enforce legal predecessor
  Root bindings.
- `src/storage/tests/storage_migration_test.rs`: foreign-Root RED tests and
  legitimate journal-first regression coverage.
- `tests/python/raw_resp_client.py`: frame resource budget and deadline.
- `tests/python/test_vector_error_matrix.py`: scripted-socket RED/GREEN tests.
- `tests/python/test_vector_set_differential.py`: raw transcript and final-state
  record production.
- `tests/compat/redis-8.8.1/vector-required-jobs.yaml`: explicit final-state
  applicability ownership.
- `tools/compat/src/manifest.rs`: strict required-jobs schema support.
- `tools/compat/tests/manifest.rs`: required-jobs schema mutants.
- `scripts/compat/run-vector-differential.sh`: transcript/final-state/log
  validation and exact-Head runner inputs.
- `scripts/compat/oracle_controller.py`: frozen callback tree, evidence
  publication, and provenance v4.
- `scripts/compat/verify-redis-8.8.1.ps1`: pass expected Head/evidence path from
  Windows when applicable.
- `tools/compat/src/oracle.rs`: strict provenance v4 DTO and parser.
- `tools/compat/tests/oracle.rs`: frozen-input, provenance, and publication
  fault tests.
- `tools/compat/tests/ci_contract.rs`: runner/workflow/evidence mutants.
- `.github/workflows/ci.yml`: exact PR Head checkout and dual evidence upload.
- `docs/compatibility/redis-8.8.1.md`: document retained transcript and
  final-state evidence.
- `docs/superpowers/specs/2026-08-16-pr422-review-findings-remediation-design.md`:
  approved design authority.

## Task 1: Storage Foreign-root RED Tests

**Files:**

- Modify: `src/storage/tests/storage_migration_test.rs`

- [ ] **Step 1: Add a reusable dual-Root replacement helper**

  The helper creates Root A and Root B with one V2 instance each, drives both
  through production open, optionally closes the rollback window, replaces
  `A/0` with `B/0`, and returns both pre-repair manifests. It must use existing
  migration test fixtures and release all RocksDB handles before directory
  replacement.

- [ ] **Step 2: Add terminal-phase foreign binding assertions**

  Add tests named:

  ```rust
  committed_resume_rejects_foreign_v2_instance_before_rebinding
  rollback_window_closed_rejects_foreign_v2_instance_before_rebinding
  ```

  Each test asserts that `prepare_or_resume_migration()` fails, A's on-disk
  instance still has B's `root_manifest_id` and `root_manifest_digest`, and the
  production storage open path also rejects the replacement.

- [ ] **Step 3: Observe RED on exact Head**

  Run each exact test with:

  ```powershell
  cargo test -p storage --features test-fault-injection --test storage_migration_test <test-name> -- --exact --nocapture
  ```

  Expected: current code succeeds or rewrites the foreign binding, so the new
  rejection/non-mutation assertion fails. A fixture/setup failure is not an
  acceptable RED and must be corrected before production code changes.

## Task 2: Storage Immediate-predecessor Enforcement

**Files:**

- Modify: `src/storage/src/storage_migration.rs`
- Verify: `src/storage/tests/storage_migration_test.rs`

- [ ] **Step 1: Add predecessor reconstruction**

  Add a migration-private helper that exhaustively maps current phase and
  instance to its legal predecessor transaction. Clone the current Root,
  install the predecessor transaction with existing `set_migration()`, and
  return the resulting digest. `SourceDetected` and invalid phase/instance
  combinations return `InvalidFormat`.

- [ ] **Step 2: Carry in-process predecessor digest**

  In `persist_transition()`, save `manifest.manifest_digest()` before changing
  the transaction. After the new Root is durable, pass the saved digest as the
  only allowed predecessor to `rebind_all_v2_instances()`.

- [ ] **Step 3: Validate before rewrite**

  Change rebinding to require:

  ```rust
  manifest.instance_id() == instance_id
  manifest.root_manifest_id() == root_manifest.manifest_id()
  manifest.root_manifest_digest() == root_manifest.manifest_digest()
      || manifest.root_manifest_digest() == allowed_predecessor_digest
  ```

  Skip writes for current bindings. Reject all other bindings before invoking
  `rebind_root()`.

- [ ] **Step 4: Run targeted GREEN and regression checks**

  Run the two new tests, then:

  ```powershell
  cargo test -p storage --features test-fault-injection --test storage_migration_test committed_resume_repairs_instance_binding_after_journal_first_crash -- --exact --nocapture
  cargo test -p storage --features test-fault-injection --test storage_migration_test closed_resume_repairs_instance_binding_without_restoring_backup -- --exact --nocapture
  cargo test -p storage --features test-fault-injection --test storage_migration_test migration_retries_after_ -- --nocapture
  ```

## Task 3: Raw RESP Budget RED Tests

**Files:**

- Modify: `tests/python/test_vector_error_matrix.py`

- [ ] **Step 1: Extend `ScriptedSocket` without changing existing behavior**

  Record `settimeout()` values and support a fake monotonic clock for the
  slow-progress test. Existing byte-chunk and binary-nested tests must remain
  unchanged semantically.

- [ ] **Step 2: Add seven focused reader tests**

  Add unmarked unit tests for unterminated header, oversized bulk declaration,
  aggregate item count, nesting depth, slow progress past absolute deadline,
  exact-limit binary nested frame, and independently charged pipelined frames.

- [ ] **Step 3: Observe RED**

  Run:

  ```powershell
  python -m pytest tests/python/test_vector_error_matrix.py -q -k "raw_client and (budget or deadline or pipeline or nesting or unterminated)"
  ```

  Expected: the current reader waits for EOF, accepts oversized declarations,
  exceeds recursion/deadline expectations, or misbehaves because the limit API
  does not yet exist. Test import/setup errors do not count as RED.

## Task 4: Raw RESP Shared Budget Implementation

**Files:**

- Modify: `tests/python/raw_resp_client.py`
- Verify: `tests/python/test_vector_error_matrix.py`

- [ ] **Step 1: Add validated limits and a per-frame budget**

  Implement immutable defaults of 16 MiB frame bytes, 64 KiB header bytes,
  100,000 items, and depth 64. Validate finite positive timeout and limits.

- [ ] **Step 2: Account consumed frame bytes**

  Route line/exact reads through budget-aware consume operations. Preflight bulk
  length and aggregate child counts before body recursion. Charge only bytes
  removed from `_buffer`.

- [ ] **Step 3: Apply one monotonic deadline**

  `execute_raw()` creates one deadline before `sendall`; every blocking socket
  operation receives only the remaining duration. Check the deadline around
  every send/receive/recursive boundary.

- [ ] **Step 4: Invalidate failed connections**

  On malformed input, limit failure, timeout, or EOF, close the connection and
  clear the buffer. Make test teardown tolerate an already-closed connection.

- [ ] **Step 5: Run GREEN and collection integrity**

  ```powershell
  python -m pytest tests/python/test_vector_error_matrix.py -q -k "raw_client or resp2_client_is_not_polluted"
  python -m pytest tests/python/test_vector_set_differential.py --collect-only -q --strict-markers -p no:cacheprovider
  ```

  Expected: focused tests pass and required differential collection remains at
  the registry's exact 40 node IDs until Task 6 intentionally changes only the
  registry schema, not node count.

## Task 5: Transcript and Final-state Contract RED Tests

**Files:**

- Modify: `tools/compat/tests/manifest.rs`
- Modify: `tools/compat/tests/ci_contract.rs`
- Modify: `tests/python/test_vector_set_differential.py`

- [ ] **Step 1: Define strict evidence fixture fields in tests**

  A valid wire record contains request/response Base64 and SHA-256 fields. A
  valid final-state envelope contains node ID, applicability, known keys, raw
  `TYPE`/`PTTL` evidence, first cleanup result, and idempotent cleanup result.

- [ ] **Step 2: Add validator mutants**

  Tests must reject hash-only coverage, invalid Base64, hash mismatch, missing
  or duplicate raw cases, unregistered comparison differences, missing
  final-state envelopes, missing `PTTL`, wrong `-1`/`-2` sentinels, extra files,
  and oversized logs/evidence.

- [ ] **Step 3: Add recorder unit tests**

  Verify the recorder stores `encode_command(*parts)` exactly, including binary
  NUL bytes, and that final-state reconciliation fails on a Kiwi/Redis TYPE or
  PTTL mismatch.

- [ ] **Step 4: Observe RED**

  ```powershell
  cargo test -p kiwi-compat --test manifest required_vector -- --nocapture
  cargo test -p kiwi-compat --test ci_contract vector_differential -- --nocapture
  python -m pytest tests/python/test_vector_set_differential.py -q -k "transcript or final_state or ttl" -p no:cacheprovider
  ```

  Expected: current hash-only schema and missing final-state recorder fail the
  new assertions.

## Task 6: Transcript, Registry, and Final-state Producers

**Files:**

- Modify: `tests/python/test_vector_set_differential.py`
- Modify: `tests/compat/redis-8.8.1/vector-required-jobs.yaml`
- Modify: `tools/compat/src/manifest.rs`
- Modify: `scripts/compat/run-vector-differential.sh`

- [ ] **Step 1: Replace hash-only coverage with raw transcript**

  Encode exact request and both response frames with strict Base64, keep
  recomputable hashes, and derive registry coverage from transcript records.

- [ ] **Step 2: Assign final-state applicability in the registry**

  Add one strict mapping that identifies server-backed nodes and explicit
  `not-applicable` parser/comparator nodes. Keep `expected_node_ids` and
  `expected_item_count` unchanged.

- [ ] **Step 3: Reconcile known keys before and after cleanup**

  Server-backed nodes write raw TYPE/PTTL and type-specific observations before
  teardown, then TYPE/PTTL plus two DEL results after cleanup. Require exact
  persistent `-1`, missing `-2`, and idempotent cleanup equality.

- [ ] **Step 4: Validate fixed evidence inputs**

  The Bash runner strictly validates transcript, final-state, collection/run
  summaries, and bounded logs from a fixed allowlist. It must reject missing,
  extra, duplicate, truncated, or oversized inputs.

- [ ] **Step 5: Run GREEN**

  Re-run Task 5 commands plus required collection validation. Do not run the
  real Oracle yet.

## Task 7: Frozen Callback and Provenance v4 RED Tests

**Files:**

- Modify: `tools/compat/tests/oracle.rs`
- Modify: `tools/compat/tests/ci_contract.rs`
- Modify: `tools/compat/src/oracle.rs`

- [ ] **Step 1: Add provenance v4 canonical fixture tests**

  Require callback expected/actual Head, tree OID, input manifest hash,
  Kiwi/helper hashes, frozen/revalidated booleans, and differential evidence
  schema/path/size/hash/atomic publication fields.

- [ ] **Step 2: Add frozen-input mutants**

  Controller probes replace or rewrite tracked scripts, binaries, and Python
  dependencies while the callback runs. They must prove the callback executes
  frozen bytes and that original-input drift prevents final publication.

- [ ] **Step 3: Add publication fault mutants**

  Inject failure at evidence write, fsync, rename, provenance publish, and
  post-publish rehash. No final evidence or provenance may remain.

- [ ] **Step 4: Observe RED on Linux**

  ```bash
  cargo test -p kiwi-compat --test oracle frozen_callback -- --nocapture
  cargo test -p kiwi-compat --test oracle provenance_v4 -- --nocapture
  cargo test -p kiwi-compat --test ci_contract trusted_vector -- --nocapture
  ```

  On Windows, compile and non-Linux schema tests may run, but namespace/mount
  probes are explicitly deferred to WSL/Linux rather than claimed passing.

## Task 8: Controller-owned Frozen Tree and Atomic Evidence

**Files:**

- Modify: `scripts/compat/oracle_controller.py`
- Modify: `scripts/compat/run-vector-differential.sh`
- Modify: `scripts/compat/verify-redis-8.8.1.ps1`
- Modify: `tools/compat/src/oracle.rs`

- [ ] **Step 1: Validate expected Kiwi Head**

  Add verifier arguments for expected Head and evidence output. Validate the
  exact commit/tree and tracked-clean state using controlled Git invocation.

- [ ] **Step 2: Materialize a detached frozen tree**

  Populate every tracked entry from Git HEAD objects, then copy only Kiwi,
  required-jobs helper, and `.oracle-python`. Enforce type/symlink/count/size
  limits and generate a canonical input manifest.

- [ ] **Step 3: Mount and execute immutable input**

  Remount the detached filesystem read-only and attach it to `/callback-input`.
  Run only the frozen callback script, binaries, tests, and dependencies. Set
  Python user-site/write/plugin isolation variables.

- [ ] **Step 4: Build bounded canonical evidence**

  Read the fixed `/work` inputs through held descriptors, validate them, and
  construct `kiwi-vector-differential-evidence/v1` below the 128 MiB cap.

- [ ] **Step 5: Publish evidence then provenance v4**

  Complete process/directory/FD cleanup and identity revalidation, atomically
  publish evidence, then atomically publish provenance last. Roll back visible
  evidence if provenance or post-publish verification fails.

- [ ] **Step 6: Run GREEN on Linux**

  Re-run all Task 7 tests, including namespace mutation and publication fault
  probes.

## Task 9: Exact-head Workflow and Upload Contract

**Files:**

- Modify: `.github/workflows/ci.yml`
- Modify: `tools/compat/tests/ci_contract.rs`
- Modify: `docs/compatibility/redis-8.8.1.md`

- [ ] **Step 1: Add workflow RED mutants**

  Reject synthetic merge checkout, missing expected Head, provenance-only
  upload, broad work-directory upload, premature upload, conditional failure
  upload, or missing exact evidence path.

- [ ] **Step 2: Bind checkout and runner Head**

  The trusted job uses:

  ```yaml
  ref: ${{ github.event.pull_request.head.sha || github.sha }}
  ```

  Pass the same expression as `KIWI_EXPECTED_HEAD` and produce sibling final
  provenance/evidence paths under the runner temp directory.

- [ ] **Step 3: Upload both exact artifacts**

  Upload only the final provenance and evidence files after successful runner
  completion, with `if-no-files-found: error`; do not use `if: always()`.

- [ ] **Step 4: Update compatibility evidence documentation**

  Document exact request/response retention, final-state persistent/missing TTL
  sentinels, evidence/provenance publication order, and bounded retention.

- [ ] **Step 5: Run GREEN**

  ```powershell
  cargo test -p kiwi-compat --test ci_contract -- --nocapture
  cargo test -p kiwi-compat --test manifest -- --nocapture
  ```

## Task 10: Integration and Independent Review

**Files:**

- Verify all changed files.

- [ ] **Step 1: Run common hygiene gates**

  ```powershell
  git diff --check
  cargo fmt --all -- --check
  python scripts/validate_sdd.py --self-test
  python scripts/validate_sdd.py
  ```

- [ ] **Step 2: Run changed-path suites**

  Run the final Storage, raw RESP, manifest, Oracle schema, and CI contract
  commands from Tasks 2, 4, 6, 8, and 9. Inspect actual output, not only exit
  status.

- [ ] **Step 3: Run one real Linux exact-Head gate**

  Execute the existing required `trusted-vector-differential` flow with exact
  Redis source/metadata and both final output paths. Confirm nonzero collection,
  zero skip/xfail, exact Head, valid raw/final-state evidence, cleanup, and no
  residual processes or verifier directories.

- [ ] **Step 4: Independent specification and quality review**

  Use file-disjoint reviewers to compare implementation with the approved spec,
  then review for correctness, security, test quality, and scope. Fix any
  confirmed P0/P1/P2 issue using a new RED test where behavior changes.

- [ ] **Step 5: Final residue and authority check**

  Confirm the worktree contains only task-owned changes, branch/Head did not
  drift unexpectedly, and no commit, push, PR metadata update, Resolve, Issue
  closure, or merge occurred.

## Git Checkpoint

No Git checkpoint is executed under the current authority. After all tests and
review pass, report the exact changed files and validation evidence. Commit and
push require a separate explicit user authorization.
