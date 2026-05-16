---
name: add-test
description: Step-by-step guide for writing tests in Kiwi.
---

# Add Tests

Guide for writing tests in Kiwi's Rust codebase.

## Test Types

| Type | Location | Requires | Framework |
|------|----------|----------|-----------|
| Unit tests | `#[cfg(test)]` in source files | Nothing | `#[test]` |
| Crate tests | `src/<crate>/tests/` directory | Nothing | `#[test]` |
| Integration | `tests/*.rs` | Nothing | `#[test]` |
| Python integration | `tests/python/` | Running server | `pytest` |
| Benchmarks | `benches/` | Nothing | `criterion` |
| Property-based | Any test module | Nothing | `proptest` |

## Unit Test Pattern

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    // Helper: create isolated test database
    fn setup_test_db() -> (TempDir, Storage) {
        let dir = tempfile::tempdir().unwrap();
        let storage = Storage::open(dir.path()).unwrap();
        (dir, storage)
    }

    #[test]
    fn test_<what>_<condition>_<expected>() {
        let (_dir, storage) = setup_test_db();

        // Arrange: set up test data
        // Act: perform the operation
        // Assert: verify the result
    }
}
```

## Rules

- **Naming**: `test_<what>_<condition>_<expected>()`
- **Isolation**: Always use `tempfile::TempDir` for database tests
- **Cleanup**: Use `safe_cleanup_test_db()` helper when available
- **No unwrap in production**: Tests may use `.unwrap()` (allowed by clippy config)
- **Descriptive failures**: Use `assert_eq!` with messages: `assert_eq!(result, expected, "description")`

## Property-Based Testing

```rust
use proptest::prelude::*;

proptest! {
    #[test]
    fn test_roundtrip(value in any::<Vec<u8>>()) {
        let encoded = encode(&value);
        let decoded = decode(&encoded).unwrap();
        prop_assert_eq!(value, decoded);
    }
}
```

## Python Integration Tests

```python
# tests/python/test_<feature>.py
import redis
import pytest

@pytest.fixture
def client():
    r = redis.Redis(host='127.0.0.1', port=7379)
    yield r
    r.flushall()

def test_<what>(client):
    # Arrange
    # Act
    # Assert
```

## Verify

```bash
cargo test                              # All unit tests
cargo test --package <crate>            # Specific crate
cargo test test_<name>                  # Specific test
pytest tests/python/ -v                 # Integration tests
```
