# Cluster Quickstart & Write-Path Verification

This guide shows how to run Kiwi in standalone and Raft cluster modes, and how to
manually verify the Raft write path (leader writes replicate to followers).

> Automated multi-node integration tests are tracked separately. The steps below
> are the manual procedure used to validate the write-path integration.

## Prerequisites

- Rust toolchain (stable) and `protoc` (see the project README / `CLAUDE.md`).
- `redis-cli` — to drive the RESP port (`brew install redis` / `apt install redis-tools`).
- `grpcurl` — to call the Raft admin gRPC API for cluster init (`brew install grpcurl`).
  The Raft gRPC server has reflection enabled, so no `.proto` files are needed.

Build the server binary:

```bash
cargo build --release --bin kiwi   # binary at target/release/kiwi
```

## Standalone mode (default)

With no Raft configuration, Kiwi runs as a single standalone node. Writes go
directly to local RocksDB (no consensus).

```bash
cargo run --release --bin kiwi          # listens on 127.0.0.1:7379 by default
redis-cli -p 7379 set k1 hello          # OK
redis-cli -p 7379 get k1                # "hello"
```

## Cluster mode

A node runs in cluster mode when its config file contains the Raft keys below.
Config files use the Redis-style `key value` format (one per line).

### 1. Write a config per node

`node1.conf`:

```conf
port 7401
binding 127.0.0.1
db-path /tmp/kiwi/n1/db
raft-node-id 1
raft-addr 127.0.0.1:8501
raft-resp-addr 127.0.0.1:7401
raft-data-dir /tmp/kiwi/n1/raft
raft-use-memory-log-store true
```

Create `node2.conf` and `node3.conf` likewise, changing `port`/`raft-addr`/
`raft-resp-addr`/`db-path`/`raft-data-dir` and `raft-node-id` (2 and 3). Keep
`raft-resp-addr` equal to `<binding>:<port>` for each node — it is the address
returned to clients on redirect.

Raft config keys:

| Key | Meaning |
|-----|---------|
| `raft-node-id` | Unique node id (`u64`) |
| `raft-addr` | gRPC address for Raft internal traffic |
| `raft-resp-addr` | RESP address advertised to clients (used in `MOVED`) |
| `raft-data-dir` | Directory for Raft logs / snapshots |
| `raft-use-memory-log-store` | `true` = in-memory log (testing); `false` = RocksDB-backed |

### 2. Start the nodes

```bash
RUST_LOG=info ./target/release/kiwi --config node1.conf &
RUST_LOG=info ./target/release/kiwi --config node2.conf &
RUST_LOG=info ./target/release/kiwi --config node3.conf &
```

Each node now listens on its RESP port and its Raft gRPC port. Until the cluster
is initialized there is no leader, so **writes are rejected** with `ERR not leader`.

### 3. Initialize the cluster

Call `Initialize` once, on any node's Raft gRPC port, listing all members:

```bash
grpcurl -plaintext -d '{"nodes":[
  {"node_id":1,"raft_addr":"127.0.0.1:8501","resp_addr":"127.0.0.1:7401"},
  {"node_id":2,"raft_addr":"127.0.0.1:8502","resp_addr":"127.0.0.1:7402"},
  {"node_id":3,"raft_addr":"127.0.0.1:8503","resp_addr":"127.0.0.1:7403"}
]}' 127.0.0.1:8501 kiwi.raft.v1.RaftAdminService/Initialize
# => { "response": { "success": true, "message": "OK" }, "leaderId": "1" }
```

A leader is elected within the election-timeout window (a second or two).

## Verify the write path

### Find the leader

A write returns `OK` on the leader and `MOVED <leader-resp-addr>` on followers:

```bash
redis-cli -p 7401 set probe v   # OK            -> node1 is leader
redis-cli -p 7402 set probe v   # MOVED 127.0.0.1:7401
redis-cli -p 7403 set probe v   # MOVED 127.0.0.1:7401
```

### Leader write → follower read (replication)

```bash
# Write on the leader
redis-cli -p 7401 set repltest hello_from_leader   # OK
redis-cli -p 7401 incr counter                     # 1

# Read from a follower (eventually consistent — allow replication to arrive)
sleep 1
redis-cli -p 7402 get repltest    # "hello_from_leader"
redis-cli -p 7403 get repltest    # "hello_from_leader"
redis-cli -p 7402 get counter     # "1"
```

What this exercises end to end: command → leader gate (passes on leader) →
`BinlogBatch` captures the encoded CF mutations → Raft `client_write` (consensus)
→ each node's state machine applies via `on_binlog_write` to local RocksDB →
follower local reads observe the replicated value.

## Notes & current limitations

- **Reads are eventually consistent.** Followers serve local reads; a value is
  visible only after the entry has replicated and applied. Linearizable reads
  (a read barrier) are not implemented yet.
- **`MOVED` is simplified.** Kiwi returns `MOVED <addr>` (no hash slot), unlike
  Redis Cluster's `MOVED <slot> <ip:port>`. Off-the-shelf cluster-aware clients
  will not auto-follow it; reconnect to the returned address directly.
- **Snapshot install.** After a Raft snapshot install, a node's storage is
  swapped and currently does not re-arm the Raft write hook — writes on that node
  would bypass consensus until restarted. This is a known follow-up to fix before
  production cluster use.
- **Writes only on the leader.** Followers reject writes with `MOVED` / `ERR not
  leader`. Reads are accepted on any node.
