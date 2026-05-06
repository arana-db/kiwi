# Code Style Rules

Rules beyond clippy and rustfmt.

## Error Handling

- **No `.unwrap()` in non-test code**: CI enforces `-D clippy::unwrap_used`
  - Use `.expect("reason")` for truly infallible cases
  - Use `?` operator for propagation
  - Use `match`/`if let` for handled cases
- **Use `thiserror`** for library error types, `snafu` for structured context
- **No `anyhow`** in library code (only in binary/server)

## Logging

- Use `log::info!`, `log::debug!`, `log::warn!`, `log::error!`
- Never use `println!` or `eprintln!` in production code
- Use `env_logger` for initialization (already in deps)

## Naming Conventions

| Type | Pattern | Example |
|------|---------|---------|
| Crate | `kebab-case` | `common-runtime` |
| Module | `snake_case` | `redis_strings.rs` |
| Struct | `PascalCase` | `StorageClient` |
| Function | `snake_case` | `handle_get` |
| Constant | `SCREAMING_SNAKE_CASE` | `DEFAULT_PORT` |
| Error type | `PascalCase` + `Error` | `StorageError` |

## Performance Patterns

- **No blocking in async**: Use `spawn_blocking` for synchronous RocksDB ops if needed
- **Prefer `bytes::Bytes`** over `Vec<u8>` for data passing
- **Use `WriteBatch`** for multi-key operations (atomicity + performance)
- **Avoid unnecessary cloning**: Use references where possible, `Arc` for shared ownership

## Import Style

- Group: stdlib, third-party, crate-internal (rustfmt handles via `reorder_imports`)
- No wildcard imports (`use x::*`)
- Prefer explicit imports

## Design Patterns

- **Composition over inheritance**: Rust traits + delegation
- **Builder pattern**: For complex construction (see `executor/builder.rs`)
- **Newtype pattern**: For type safety (wrap primitive types)
- **Trait objects**: Use `dyn Trait` when dynamic dispatch is needed
