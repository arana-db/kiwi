# Bounded Request Processing Implementation Plan

> **For AI workers:** Required sub-skill: use superpowers:subagent-driven-development to implement this plan task by task. Track steps with checkboxes.

**Goal:** Bound unauthenticated RESP parsing and optional pipeline queuing, and stop active connections from retaining duplicate command history.

**Architecture:** Keep the existing parser and network APIs, add explicit parser limits with depth-aware recursive parsing, enforce the unauthenticated limit at the active network boundary, drain the legacy command queue, and replace the optional pipeline's unbounded channel with a bounded sender. Each behavior is introduced through a failing regression test before production code changes.

**Tech Stack:** Rust 1.97.1, nom 8, Tokio mpsc, Cargo tests, Windows MSVC and WSL/Linux validation.

---

### Task 1: Bound RESP lengths, allocation, buffering, and nesting

**Files:**
- Modify: `src/resp/src/parse.rs`
- Modify: `src/resp/src/error.rs` only if a structured limit variant is required

- [x] Add parser tests using small private limits for bulk length, aggregate length, exact boundaries, nesting depth, chunked buffer growth, and reset-after-error.
- [x] Run `cargo +1.97.1-x86_64-pc-windows-msvc test -p resp --lib parse::tests --locked` and confirm the new assertions fail because over-limit frames are incomplete or complete.
- [x] Add immutable parser limits, pre-append buffer checks, depth-aware aggregate parsing, bounded initial capacity, and fallible incremental reservation.
- [x] Add uniform first-line limits, a decoded-node budget, and a cumulative incomplete-frame parse-work budget after code-quality review.
- [x] Re-run the parser tests and all `resp` tests; require zero failures.
- [x] Commit `fix(resp): bound request parsing resources`.

### Task 2: Enforce pre-authentication limits and drain parser history

**Files:**
- Modify: `src/net/src/network_handle.rs`
- Modify: `src/net/src/optimized_handler.rs`
- Modify: `src/net/tests/storage_command_e2e_tests.rs`
- Modify: `src/resp/src/parse.rs` only for read-only buffer/queue observability

- [x] Add a TCP regression test that sends an incomplete request beyond 1 MiB before `AUTH`, observes connection closure, then authenticates and pings through a healthy connection.
- [x] Add a parser/network regression proving complete frames do not accumulate undrained legacy commands in active consumers.
- [x] Run the two new tests and confirm timeout or retained-command failures on the baseline implementation.
- [x] Check `buffered_len + read_len` before unauthenticated parsing and drain one legacy command result after every complete frame in all consumers.
- [x] Re-run the new tests plus the existing bad-connection protocol test; require zero failures under WSL/Linux.
- [x] Commit `fix(net): cap unauthenticated request buffering`.

### Task 3: Enforce bounded pipeline admission

**Files:**
- Modify: `src/net/src/pipeline.rs`

- [x] Add tests proving zero capacity normalizes to one and that the production channel helper rejects immediate admission after its configured capacity is occupied.
- [x] Run `cargo +1.97.1-x86_64-pc-windows-msvc test -p net pipeline::tests --locked` and confirm the bounded-capacity assertions fail.
- [x] Replace `UnboundedSender/UnboundedReceiver` with bounded Tokio mpsc, await admission under the existing timeout, and report actual normalized capacity.
- [x] Re-run pipeline and net tests; require zero failures.
- [x] Commit `fix(net): enforce pipeline queue capacity`.

### Task 4: Integrate, review, and publish one PR

**Files:**
- Modify: this plan's checkboxes as each gate completes

- [x] Run `cargo fmt --all -- --check`.
- [x] Run strict Clippy for `resp` and `net` with all targets/features and `-D warnings -D clippy::unwrap_used`.
- [x] Run `cargo test --workspace --all-features --locked` with `RUST_TEST_THREADS=1`; all reached unit suites pass, while the 19 Windows TCP tests reproduce the repository baseline inability to start the server.
- [x] Run the targeted RESP and network tests under WSL/Linux with an independent target directory.
- [x] Run `git diff --check` and verify only planned files changed.
- [x] Complete specification compliance review, then code-quality review, and resolve every important finding.
- [x] Fast-forward the existing PR #404 head `codex/fix-resp-parser-limits` with the consolidated fix; do not merge it.
