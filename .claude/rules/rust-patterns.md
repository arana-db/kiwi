# Rust Patterns

Rust-specific patterns and idioms for Kiwi.

## Ownership & Borrowing

- Prefer references over owned types in function parameters
- Use `&str` over `&String`, `&[T]` over `&Vec<T>`
- Use `Cow<'_, str>` when sometimes owned, sometimes borrowed
- Use `Arc<T>` for shared ownership across threads, `Rc<T>` for single-thread

## Concurrency

- **tokio** for async I/O (network layer)
- **parking_lot** for sync primitives (`Mutex`, `RwLock`) -- faster than std
- **arc-swap** for hot-swappable shared state (GlobalStorage)
- **tokio::sync::mpsc** for bounded async channels between runtimes
- **async-channel** for bounded work dispatch in executor

```rust
// Good: parking_lot for sync
use parking_lot::Mutex;
let data = Arc::new(Mutex::new(Vec::new()));

// Good: arc-swap for hot replacement
use arc_swap::ArcSwap;
let storage = ArcSwap::from_pointee(initial_storage);
storage.store(Arc::new(new_storage));
```

## Error Handling Patterns

```rust
// Library errors: thiserror
#[derive(Debug, thiserror::Error)]
pub enum StorageError {
    #[error("key not found: {0}")]
    KeyNotFound(String),
    #[error("wrong type: expected {expected}, got {actual}")]
    WrongType { expected: &'static str, actual: &'static str },
}

// Propagation
fn get(key: &str) -> Result<Option<Vec<u8>>, StorageError> {
    let cf = self.cf_handle(CF_NAME)
        .ok_or(StorageError::ColumnFamilyNotFound)?;
    // ...
}
```

## Async Patterns

```rust
// Channel communication between runtimes (tokio::sync::mpsc)
let (tx, rx) = tokio::sync::mpsc::channel(1024);

// Spawn blocking for CPU-intensive work
let result = tokio::task::spawn_blocking(move || {
    // synchronous RocksDB operation
}).await?;

// Timeout for operations
let result = tokio::time::timeout(
    Duration::from_secs(5),
    storage_client.get(key)
).await??;
```

## Trait Design

- Keep traits focused (single responsibility)
- Use `#[async_trait]` for async trait methods
- Add `Send + Sync` bounds for cross-thread usage
- Prefer associated types over type parameters when there's one implementation

## Testing Patterns

```rust
// Isolated test databases
fn setup() -> (TempDir, Storage) {
    let dir = tempfile::tempdir().unwrap();
    let storage = Storage::open(dir.path()).unwrap();
    (dir, storage) // dir kept alive for cleanup
}

// Property-based testing
proptest! {
    #[test]
    fn test_roundtrip(key in "[a-z]{1,100}") {
        // ...
    }
}
```
