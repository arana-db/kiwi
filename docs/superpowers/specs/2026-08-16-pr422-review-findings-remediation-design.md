# PR #422 Review Findings Remediation Design

## Document Status

- Date: 2026-08-16
- Status: approved design; pending written-spec review
- Pull request: `arana-db/kiwi#422`
- Base: `733888fc90ad8ef039947e87b08d7500a405954a`
- Exact Head: `b16e57c3afdf91ef339130cb331ebda03d201451`
- Worktree: `D:\test\github\kiwi\.worktrees\vector-set-post-merge-remediation`
- User decision: approved option B, exact source and evidence binding

This document narrows the remediation to the four confirmed code findings from
the exact-Head review. It does not authorize commit, push, merge, PR metadata
changes, review-thread resolution, Issue closure, or branch-protection changes.

## 1. Problem Statement

PR #422 currently has four correctness and auditability gaps:

1. Storage migration can rewrite a structurally valid foreign V2 instance
   manifest to the current Root before proving that the instance came from that
   Root or its immediately preceding durable migration state.
2. The Trusted Oracle callback executes files from a read-only bind mount of a
   caller-owned checkout. A host-side writer can still change nested files while
   the callback runs, and the current metadata-only tree snapshot does not bind
   the executed bytes.
3. The required Vector differential keeps only response hashes and callback
   exit metadata. It deletes the raw requests, raw responses, logs, final-state
   reconciliation, and exact Kiwi Head evidence before publishing provenance.
4. The raw RESP client trusts remote header lengths and aggregate counts, has no
   frame byte/item/depth limits, and applies only a per-socket-operation timeout
   rather than one absolute deadline for a complete frame.

The repair must close these gaps without changing the Storage manifest format,
adding Vector command features, creating a second independently reproducible
Kiwi build system, or rewriting the RESP parser.

## 2. Goals and Non-goals

### 2.1 Goals

- Reject a foreign or stale V2 instance before any manifest rewrite.
- Preserve legitimate journal-first crash repair for exactly one durable Root
  transition.
- Execute the differential exclusively from a controller-owned immutable tree
  bound to the expected Kiwi Head and explicit runtime inputs.
- Publish enough bounded evidence to independently replay the raw comparison
  audit: exact request bytes, both response frames, summaries, logs, final state,
  TTL sentinels, tool and binary identities, and exact Head.
- Preserve cleanup-before-publish and make provenance the final success marker.
- Bound RESP parsing by total frame bytes, header bytes, node count, nesting
  depth, and one monotonic absolute deadline.
- Add regression tests that first fail on exact Head `b16e57c3` for the intended
  reason and then pass after the minimum implementation.

### 2.2 Non-goals

- No new Vector commands, quantization modes, search algorithms, or cluster
  behavior.
- No Storage manifest version or persistent field changes.
- No independent rebuild of Kiwi inside the Oracle controller.
- No timed-expiration compatibility expansion. Final-state evidence covers the
  deterministic persistent (`PTTL = -1`) and missing (`PTTL = -2`) states used
  by the current required suite.
- No iterative RESP parser rewrite.
- No unrelated refactoring, dependency upgrades, or workflow restructuring.
- No PR Ready/Draft, title, body, label, reviewer, or review-thread changes.

## 3. Rejected Alternatives

### 3.1 Metadata or Hash Snapshots Around the Existing Callback

Recomputing checkout hashes before and after the callback does not prevent a
host writer from changing a file, allowing it to execute, and restoring the
original bytes before the final check. A read-only bind mount restricts writes
from the callback namespace but does not freeze the host-side objects.

### 3.2 Root ID-only Storage Validation

Checking only `root_manifest_id` rejects a different Root but accepts an
arbitrary stale digest from the same Root. Journal-first recovery provides a
stronger invariant: an instance can be bound only to the current Root or the
unique immediately preceding durable Root state.

### 3.3 Persisted Instance Authority Map

Persisting instance UUID or digest authority in the Root would require a
manifest format/version migration. Persisting an instance digest also risks a
digest cycle because the instance digest includes its Root binding. That design
is broader than the confirmed finding.

### 3.4 Controller-side Kiwi Rebuild

Rebuilding Kiwi under the Oracle controller would require a second controlled
Rust/Cargo/native-toolchain reproducibility contract. The current finding only
requires exact-Head and executed-byte binding for the already-built candidate.

### 3.5 Buffer-size-only RESP Protection

Charging all bytes returned by `recv()` would incorrectly charge a pipelined
second response to the first frame. A per-operation socket timeout can also be
renewed indefinitely by a peer that sends one byte before every timeout.

## 4. Storage Migration Source Validation

### 4.1 Required Invariants

Before `rebind_root()` or any atomic manifest write, every source or shadow V2
instance manifest must satisfy all of the following:

1. `read_from_dir()` has validated the manifest's own digest.
2. `instance_id` equals the instance ID implied by the directory being scanned.
3. `root_manifest_id` equals the current Root's immutable manifest ID.
4. `root_manifest_digest` equals either the current Root digest or the unique
   immediate-predecessor Root digest for the current durable transition.

An instance already bound to the current Root is not rewritten. An instance
bound to the allowed predecessor is rebound to the current Root. Any other ID,
digest, phase, or instance mismatch fails before the first write.

### 4.2 Immediate-predecessor Reconstruction

For an in-process transition, `persist_transition()` retains the Root digest
from before `set_migration()` and passes it to instance rebinding after the new
Root journal has been durably written.

For resume after process restart, a migration-private helper reconstructs the
unique preceding transaction from the current `phase` and `current_instance`,
applies it to a clone of the current Root through the existing `set_migration()`
path, and uses the resulting Root digest as the only allowed predecessor.

The predecessor graph is:

```text
ShadowPrepared(0)          <- SourceDetected(0)
InstanceCopied(0)          <- ShadowPrepared(0)
InstanceCopied(i > 0)      <- InstanceUpgraded(i - 1)
InstanceUpgraded(i)        <- InstanceCopied(i)
AllInstancesVerified(last) <- InstanceUpgraded(last)
SwitchPrepared(0)          <- AllInstancesVerified(last)
OldMovedToBackup(0)        <- SwitchPrepared(0)
OldMovedToBackup(i > 0)    <- ShadowPromoted(i - 1)
ShadowPromoted(i)          <- OldMovedToBackup(i)
NewStorageOpened(last)     <- ShadowPromoted(last)
Committed(last)            <- NewStorageOpened(last)
RollbackWindowClosed(last) <- Committed(last)
```

`SourceDetected` has no V2 predecessor. Invalid phase/instance combinations
fail closed rather than guessing a digest.

### 4.3 Storage Tests

Two RED tests create independent Roots A and B, complete their V2 migrations,
replace A's instance directory with B's complete instance, and then attempt
resume at `Committed` and `RollbackWindowClosed` respectively. Each test proves:

- migration/open returns an identity or digest mismatch;
- the foreign instance remains bound to Root B after failure;
- no data from Root B is admitted through Root A;
- failure occurs before any manifest rewrite.

Existing journal-first crash tests for `NewStorageOpened -> Committed` and
`Committed -> RollbackWindowClosed`, plus the phase retry matrix, must remain
green.

## 5. Frozen Callback Input

### 5.1 Exact Head Binding

The trusted CI job checks out the PR Head SHA, not GitHub's synthetic merge
commit, and passes the same 40-character lowercase SHA as `KIWI_EXPECTED_HEAD`.
The runner and controller independently validate:

- expected Head syntax;
- actual `HEAD` equality;
- `HEAD^{tree}` identity;
- no tracked worktree changes before building callback runtime inputs.

The provenance records expected Head, actual Head, tree OID, branch/ref context,
and the input-manifest SHA-256.

### 5.2 Controller-owned Tree

The controller creates a detached private filesystem for `/callback-input`.
All tracked files are materialized from Git `HEAD` object contents, not copied
from mutable worktree paths. The current tree has 549 tracked files and is about
7.6 MiB, so a complete tracked tree is simpler and less fragile than a manually
maintained transitive file allowlist.

Only these untracked runtime roots may be added:

- `target/debug/kiwi`;
- `target/debug/kiwi-required-vector-jobs`;
- `.oracle-python/**`.

Other untracked workspace files are neither copied nor mounted and therefore
cannot influence execution. Each tracked entry records Git mode, blob OID,
size, and SHA-256. Each untracked entry records path, type, mode, size, and
SHA-256.

The tree rejects absolute or escaping symlinks, excessive symlink chains,
special files, duplicate paths, and unexpected hard-link relationships. Bounds
are 8192 entries, 512 MiB per file, and 1 GiB total input.

After population, the detached filesystem is remounted read-only and attached
directly as `/callback-input` in the callback namespace. The callback script,
Kiwi binary, required-jobs helper, Python tests, and Python dependencies execute
only from that tree.

### 5.3 Runtime Revalidation

Before and after the callback, the controller recomputes the frozen-tree
manifest through its held filesystem descriptor. It also revalidates the
original repository Head and the explicit untracked runtime inputs. After Kiwi
starts, `/proc/<pid>/exe` must match the frozen Kiwi binary identity and hash.

The callback environment sets:

```text
PYTHONNOUSERSITE=1
PYTHONDONTWRITEBYTECODE=1
PYTEST_DISABLE_PLUGIN_AUTOLOAD=1
```

Required pytest plugins are loaded explicitly. Ambient user-site or auto-loaded
plugins cannot participate in the required differential.

## 6. Canonical Differential Evidence

### 6.1 Schemas

The final Oracle provenance schema becomes
`kiwi-redis-oracle-provenance/v4`. Its existing Redis primary/rebuild/runtime
contracts remain unchanged and it adds strict `callback_input` and
`differential_evidence` objects.

The separate evidence file uses
`kiwi-vector-differential-evidence/v1`. It is canonical JSON with a fixed field
set and deterministic ordering. It contains:

- exact Kiwi Head/tree and frozen callback-input manifest;
- platform, kernel, architecture, controlled tool identities, and Kiwi/helper
  identities;
- canonical required-jobs document;
- collection log and collection summary;
- pytest log and run summary;
- bounded Kiwi and Redis runtime logs;
- raw wire transcripts;
- final-state and TTL reconciliation;
- cleanup and residue observations that belong to the differential callback.

The provenance stores the evidence schema, file name, byte size, SHA-256, and
`published_atomically = true`. The evidence cap is 128 MiB. Raw transcripts are
limited to 16 MiB decoded bytes, final-state evidence to 4 MiB, and each log to
8 MiB. Missing, truncated, oversized, duplicate, extra, or schema-invalid
evidence fails the callback.

### 6.2 Raw Wire Transcript

Every registry-declared raw case stores:

- node ID, case ID, protocol, command, and comparison kind;
- exact encoded request bytes as strict Base64 and SHA-256;
- exact Kiwi response bytes as strict Base64 and SHA-256;
- exact Redis response bytes as strict Base64 and SHA-256;
- only the command-specific registered difference IDs, where applicable.

The validator decodes Base64 with strict validation, recomputes all hashes,
reconstructs the coverage set from transcript records, and re-runs exact-frame
or command-specific schema comparison. The existing hash-only
`raw-coverage.jsonl` is removed rather than retained as a second source of
truth.

### 6.3 Final-state and TTL Evidence

Every required pytest node has a final-state envelope. Server-backed nodes
record each known test key before teardown and after idempotent cleanup:

- `TYPE` raw request and both raw responses;
- `PTTL` raw request and both raw responses;
- type-specific observable Vector state used by that node;
- first and second cleanup `DEL` responses.

The current suite creates persistent rather than expiring Vector keys, so
before cleanup `PTTL` must be exactly `-1` on both servers and after cleanup it
must be exactly `-2`. Pure parser/comparator nodes use an explicit
registry-declared `not-applicable` envelope; a test cannot self-declare a skip.
No clock tolerance or new expiration behavior is introduced.

### 6.4 Publication Order

1. The callback writes only to `/work`.
2. The controller opens the fixed evidence inputs through held descriptors,
   validates schemas and bounds, and constructs the canonical evidence file in
   the output parent's private staging area.
3. Kiwi, Redis, callback process groups, runtime directories, rebuild checkout,
   logs, and verifier temporary roots are stopped, reaped, closed, and removed.
4. Frozen input, source, tools, evidence, and output parent are revalidated.
5. The evidence file is fsynced and atomically published with no replacement.
6. Provenance v4 referencing the visible evidence hash is fsynced and atomically
   published last.
7. Both files are reopened and rehashed, then the output parent is fsynced.

If provenance publication or post-publication verification fails, the
controller identity-checks and removes the just-published evidence. A failed run
leaves neither final provenance nor final evidence. CI uploads only after the
runner succeeds and uploads both exact paths with `if-no-files-found: error`.

## 7. Bounded Raw RESP Reader

### 7.1 Public Configuration

The existing `connect(host, port, protocol, timeout=5.0)` call remains
compatible and gains an optional `limits` argument. Defaults are:

```text
max_frame_bytes  = 16 * 1024 * 1024
max_header_bytes = 64 * 1024
max_items        = 100_000
max_depth        = 64
```

Byte/header/item limits must be positive, depth must be nonnegative, and timeout
must be finite and positive.

### 7.2 Shared Frame Budget

Each `execute_raw()` creates one budget containing the limits and a deadline
computed with `time.monotonic()`. The same budget is passed to every recursive
child frame. Root depth is zero and the root counts as one item.

- Frame bytes include all headers, payloads, and terminating CRLF bytes.
- Header scanning fails as soon as the header exceeds its limit without CRLF.
- A bulk declaration is checked against remaining frame bytes before its body
  is read.
- Aggregate declarations are checked against remaining item capacity before
  child iteration. Map and attribute counts are checked with overflow-safe
  multiplication by two.
- Nesting is checked before entering each child container.
- Bytes are charged only when removed from `_buffer` for the current frame, not
  when a socket read appends data.

Consequently, a socket read containing `frame1 + frame2` charges only `frame1`;
the unused bytes remain buffered for the next response and its fresh budget.

### 7.3 Absolute Deadline and Failure State

The reader checks the deadline before and after `sendall`, before and after each
`recv`, at recursive entry, and before successful return. Before each blocking
socket operation it sets the timeout to the remaining deadline rather than the
original duration.

Malformed input, EOF before frame completion, resource-limit failure, or
deadline failure closes the connection and clears its buffer. The connection is
not reusable after framing synchronization is lost. Teardown must tolerate an
already-closed raw client so it does not obscure the original error.

### 7.4 RESP Reader Tests

Focused scripted-socket RED tests cover:

1. an unterminated header exceeding the header budget;
2. an oversized bulk declaration rejected before reading its payload;
3. oversized array and map child counts;
4. nesting beyond the configured depth;
5. one-byte progress that exceeds the shared absolute deadline;
6. a binary nested RESP3 frame accepted exactly at byte/item/depth limits;
7. two pipelined frames received together but charged independently.

These tests remain outside the 40-node required differential registry. The
required differential collection count and node identities must not drift as a
side effect of reader unit tests.

## 8. TDD and Verification Strategy

Implementation proceeds one finding at a time:

1. Add the smallest regression test.
2. Run it against exact Head and record the expected RED failure.
3. Implement only enough production code to satisfy that test.
4. Run the targeted GREEN check and the nearest existing regression checks.
5. Continue to the next finding only after output has been inspected.

Risk-proportional gates are:

- Storage: two foreign-Root tests, two legitimate journal-first repair tests,
  and the migration phase retry matrix.
- RESP: focused scripted-socket tests, unchanged required-node collection, and
  the existing binary nested-frame test.
- Oracle/evidence: Rust controller and CI contract tests, hash/Base64/final-state
  mutants, callback-input replacement probes, and publication fault injection.
- Integration: one final exact-Head trusted Vector differential run on Linux,
  because namespace mount behavior and the real Redis rebuild cannot be proven
  by Windows unit tests.
- Common hygiene: `git diff --check`, formatting checks for changed languages,
  and SDD validation if planning references change.

An expensive full workspace test is not an automatic gate. It is run only if a
shared interface change or targeted failure provides a concrete reason.

## 9. Success Criteria

The remediation is complete only when all of the following are true:

- foreign V2 instances are rejected before mutation at both reviewed terminal
  migration phases;
- every legitimate immediate-predecessor repair still succeeds;
- callback execution is bound to the expected Kiwi Head, frozen tracked bytes,
  and hash-recorded runtime inputs;
- raw requests, both raw responses, summaries, logs, final state, and TTL
  sentinels survive as validated evidence referenced by final provenance;
- provenance is published only after cleanup and is the final success marker;
- oversized, deeply nested, slow-drip, and unterminated RESP frames fail within
  their configured budgets while valid binary and pipelined frames are
  preserved exactly;
- required differential collection remains exact and has zero skip/xfail;
- no unauthorized commit, push, PR metadata update, Resolve, Issue closure, or
  merge has occurred.
