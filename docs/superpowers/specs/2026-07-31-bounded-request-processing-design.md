# Bounded Request Processing Design

## Goal

Prevent unauthenticated or slow clients from causing unbounded memory growth in
the active RESP parsing path and the optional command pipeline, while preserving
Redis-compatible request limits and existing command behavior.

## Scope

The change covers three proven ownership problems:

1. RESP length headers can trigger oversized aggregate reservations before
   authentication, bulk frames can grow the parser buffer without a declared
   length check, and recursive aggregates have no nesting limit.
2. The active network path returns parsed `RespData` but leaves a second
   `RespCommand` copy in `RespParse`, retaining command history for the lifetime
   of a connection.
3. `PipelineConfig::command_queue_size` is reported but ignored because the
   optional pipeline uses an unbounded channel.

The storage findings from issue #395 are not implementation scope. Normal
client `DEL` and `MSETNX` calls already hold the executor's
`STORAGE_EXCLUSIVE` gate, Raft apply is single-writer, and expiration compaction
is currently a no-op. Those facts invalidate their current P0 data-loss
descriptions; adding local locks would not repair the claimed Raft interleaving.

## RESP Limits

`RespParse` owns immutable limits. Production constructors use:

- 64 KiB maximum first-line length for inline commands and every RESP type or
  aggregate header.
- 512 MiB maximum bulk, bulk-error, and verbatim-string payload length.
- 1 GiB maximum buffered frame length for authenticated/general parsing.
- `i32::MAX` maximum aggregate item or pair count, matching Redis' multibulk
  header boundary.
- 128 maximum aggregate nesting depth.
- Aggregate capacity never derives from the declared remaining length. Each
  successfully decoded element may request only the capacity needed for that
  element through fallible reservation, preserving merged PR #406's zero
  declaration-driven allocation contract.
- At most 65,536 decoded `RespData` nodes in one frame. The `i32::MAX` wire
  count remains accepted as a protocol boundary, but it cannot force that many
  objects to be materialized.
- At most 1,000,000 cumulative node visits while an incomplete frame is
  reparsed. This bounds repeated aggregate-prefix work without changing the
  public parser API.

Tests use a private constructor with small limits, so boundaries are exercised
without allocating large buffers. Limits are checked before extending the
buffer or reserving aggregate memory. Limit violations are terminal parser
errors rather than incomplete frames.

The active network path also enforces a 1 MiB buffer limit before authentication.
It checks the existing incomplete-frame size plus the next read before passing
bytes to the parser. A violation closes only that connection through the
existing protocol-error path.

## Parser Ownership

The public parser API remains compatible in this PR. All network-side parser
consumers route through one helper that drains the corresponding legacy
`next_command()` entry after each complete frame. This keeps the internal queue
bounded without combining an API removal with the security fix. Removing the
duplicate command representation is a follow-up API cleanup.

## Pipeline Backpressure

The optional pipeline uses `tokio::sync::mpsc::channel` with a minimum capacity
of one. Merged PR #403 supplies one outer timeout around queue admission and
response delivery, so saturation cannot consume 30 seconds before starting a
second response timeout. The real `submit_command` entry point is covered to
ensure it delegates to that shared budget. Channel closure remains a distinct
error. Statistics report the normalized, real queue capacity.

## Error Handling

- Malformed or over-limit RESP frames become `RespParseResult::Error`.
- An over-limit unauthenticated connection is closed without affecting other
  clients or the server process.
- Queue admission timeout returns `PipelineError::Timeout`.
- No production path uses `unwrap`, `expect`, or infallible allocation for
  attacker-controlled aggregate capacity.

## Verification

The implementation must prove red-green behavior for oversized first lines and
length headers, nesting, decoded-node amplification, cumulative incomplete
aggregate work, chunked buffer growth, parser reset, legacy queue draining,
bounded pipeline capacity, declaration-independent aggregate growth, and one
total pipeline deadline. Final gates are targeted tests, workspace tests,
strict Clippy, formatting, `git diff --check`, Linux/WSL verification, and
current PR Head/check reconciliation.
