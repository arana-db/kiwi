# Testing Rules

Testing strategy and requirements for Kiwi.

## Test Organization

| Type | Location | When |
|------|----------|------|
| Unit | `#[cfg(test)]` in source | Always, alongside code |
| Crate | `src/<crate>/tests/` | Complex test suites |
| Integration | `tests/*.rs` | Cross-crate tests |
| Python | `tests/python/` | End-to-end with running server |
| Benchmark | `benches/` | Performance regression |

## Naming Convention

```text
test_<what>_<condition>_<expected>()
```

Examples:
```rust
test_get_existing_key_returns_value()
test_set_with_expiry_key_expires()
test_hset_wrong_type_returns_error()
```

## Test Structure (Arrange/Act/Assert)

```rust
#[test]
fn test_get_existing_key_returns_value() {
    // Arrange
    let (_dir, storage) = setup_test_db();
    storage.set("key", b"value").unwrap();

    // Act
    let result = storage.get("key").unwrap();

    // Assert
    assert_eq!(result, Some(b"value".to_vec()));
}
```

## Database Tests

- **Always** use `tempfile::TempDir` for isolation
- Use `unique_test_db_path()` helper when available
- Use `safe_cleanup_test_db()` for cleanup
- Never share database state between tests

## Assertions

- Use `assert_eq!` / `assert_ne!` with descriptive messages
- Use `assert_matches!` for pattern matching
- Use `assert!(result.is_err())` for error cases

## Markers

- `#[cfg(test)]` -- standard test module
- `#[ignore]` -- skip by default (document why)
- `#[should_panic]` -- expected panics

## Coverage Requirements

- New commands: must have do_initial + do_cmd tests
- Storage operations: must have CRUD + error case tests
- Raft operations: must have state machine transition tests
- Bug fixes: must have regression test

## Integration Tests (Python)

```python
import redis
import pytest

@pytest.fixture
def client():
    r = redis.Redis(host='127.0.0.1', port=7379)
    yield r
    r.flushall()

def test_set_and_get(client):
    client.set("key", "value")
    assert client.get("key") == b"value"
```
