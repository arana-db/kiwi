# TOML Configuration Support — Design Doc

**Issue:** #247 — Add proper TOML configuration file support
**Date:** 2026-05-27
**Status:** Approved

## Problem

Current config system uses a hand-rolled Redis-style parser (`parse_redis_config`) that:
- Only handles `key = value` and `key value` format
- Cannot parse TOML sections (`[raft]`), arrays, or nested structures
- All values are parsed as strings, then manually converted
- The existing `config.example.toml` uses TOML format that the parser cannot read

## Solution

Replace the manual parser with the `toml` crate + serde `Deserialize`:

### TOML Structure

```toml
# Basic
binding = "127.0.0.1"
port = 7379
timeout = 300
memory = "1GB"
log-dir = "./logs"
db-dir = "./db"
db-path = ""
db-instance-num = 3
redis-compatible-mode = true
# requirepass = "secret"

# Compaction
small-compaction-threshold = 10
small-compaction-duration-threshold = 180

# RocksDB
rocksdb-max-subcompactions = 1
rocksdb-max-background-jobs = 2
rocksdb-max-write-buffer-number = 2
rocksdb-min-write-buffer-number-to-merge = 1
rocksdb-write-buffer-size = 67108864
rocksdb-level0-file-num-compaction-trigger = 4
rocksdb-num-levels = 7
rocksdb-enable-pipelined-write = false
rocksdb-level0-slowdown-writes-trigger = 20
rocksdb-level0-stop-writes-trigger = 36
rocksdb-ttl-second = 0
rocksdb-periodic-second = 0
rocksdb-level-compaction-dynamic-level-bytes = false
rocksdb-max-open-files = 10000
rocksdb-target-file-size-base = 67108864
rocksdb-compression-type = "lz4"

# Raft (optional section)
[raft]
node-id = 1
raft-addr = "127.0.0.1:9220"
resp-addr = "127.0.0.1:7379"
data-dir = "./raft-data"
# heartbeat-interval-ms = 500
# election-timeout-min-ms = 1500
# election-timeout-max-ms = 3000
use-memory-log-store = false
```

### Key Design Decisions

1. **Flat top-level keys** — RocksDB settings stay flat (not nested under `[rocksdb]`) to minimize migration friction
2. **`[raft]` section** — Uses TOML table syntax, maps to `Option<RaftClusterConfig>`
3. **`memory` field** — Keep string format ("1GB") with custom serde deserializer (`deserialize_memory`)
4. **Backward compat** — Drop Redis-style format entirely (it was never a real parser)
5. **Error messages** — `toml` crate gives line/column numbers in errors

### Files to Modify

| File | Change |
|------|--------|
| `src/conf/Cargo.toml` | Add `toml = "0.8"` |
| `src/conf/src/config.rs` | Add `Deserialize` derive + serde attrs, rewrite `Config::load()` |
| `src/conf/src/de_func.rs` | Keep `deserialize_memory`/`deserialize_bool_from_yes_no`, remove `parse_redis_config`/`parse_config_line`/`redis_config_to_ini` |
| `src/conf/src/lib.rs` | Update tests to use TOML format |
