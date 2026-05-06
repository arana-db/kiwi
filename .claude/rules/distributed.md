# Distributed Patterns

Rules for Raft consensus and dual runtime architecture in Kiwi.

## Dual Runtime Rules

- **Never block network runtime** with synchronous storage calls
- Use `StorageClient` async API for all storage operations from network runtime
- Use bounded channels (`tokio::sync::mpsc`) to prevent unbounded memory growth
- Circuit breaker must be checked before sending requests to storage

## Raft Rules

- **Idempotency**: State machine operations must be safe to replay
- **Crash safety**: Log store must use sync writes for durability
- **Atomic snapshots**: Use `ArcSwap` for hot-swapping storage during snapshot install
- **Serialization**: All Raft entries must be `serde::Serialize + serde::Deserialize`

## ArcSwap Pattern

```rust
use arc_swap::ArcSwap;

pub struct GlobalStorage {
    inner: ArcSwap<Storage>,
}

impl GlobalStorage {
    pub fn load(&self) -> arc_swap::Guard<Arc<Storage>> {
        self.inner.load()
    }

    pub fn swap(&self, new: Arc<Storage>) {
        self.inner.store(new);
    }
}
```

## Channel Communication

```rust
// Bounded channel for backpressure (tokio::sync::mpsc)
let (tx, rx) = tokio::sync::mpsc::channel(1024);

// Send with timeout
tokio::time::timeout(Duration::from_secs(5), tx.send(request)).await??;

// Receive and process
while let Ok(request) = rx.recv().await {
    process_request(request).await;
}
```

## Error Propagation

- Storage errors must be serializable for cross-runtime transmission
- Use structured error types, not string messages
- Circuit breaker should convert storage errors to "service unavailable"

## Graceful Shutdown

1. Stop accepting new connections
2. Drain in-flight requests (with timeout)
3. Flush storage writes
4. Close channels
5. Join runtime threads

## Debug Environment

```bash
RUST_LOG=runtime=trace     # Runtime communication
RUST_LOG=raft=debug        # Raft consensus
RUST_LOG=storage=debug     # Storage operations
```
