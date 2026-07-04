# Redis Type Check Unification Design

## Context

Kiwi storage currently supports five Redis data families in this area:
String, Hash, Set, List, and ZSet.

The current type-checking code has two visible paths:

- `Redis::check_type(value_raw, expected)` checks an already-read encoded value.
- `Redis::get_key_type(key)` re-encodes the user key, reads the top-level value from `MetaCF`, and returns its `DataType`.

This creates inconsistent command implementations. Non-string commands generally read a top-level meta value and call `check_type`. String commands often read the string value first, then call `get_key_type`, which encodes the same key again and performs another database read.

This is especially visible in commands like `GETBIT`: it reads the string value with `BaseKey::new(key).encode()?`, then calls `get_key_type(key)`, whose `BaseMetaKey::new(key).encode()?` uses the same key encoding because `BaseMetaKey` is a type alias of `BaseKey`.

## Problem

The problem is not only duplicate code. The two functions also hide different state semantics:

- `check_type` folds empty values, stale values, and matched live values into `Ok(())`.
- `get_key_type` folds missing and stale values into `KeyNotFound`, then returns a live type.
- Commands still need different Redis-level behavior for missing and stale keys. For example, `GETBIT` returns `0`, while `SCARD` currently returns `KeyNotFound`.

Because the state is not explicit, command code repeatedly combines `is_stale`, `check_type`, `get_key_type`, parser construction, and `is_valid` in slightly different orders.

## Goals

- Give all five data families one shared type/status check model.
- Avoid duplicate key encoding and duplicate DB reads when a command already has the encoded top-level raw value.
- Keep `WRONGTYPE` generation in one place.
- Preserve command-specific behavior for missing and stale keys.
- Refactor incrementally without forcing every command's return semantics into one shape.

## Non-Goals

- Do not redesign key encoding.
- Do not change `BaseMetaKey = BaseKey`.
- Do not merge all command read paths into one large command helper.
- Do not change Redis-visible behavior intentionally unless tests show the current behavior is already inconsistent with Redis compatibility.

## Design

Add an explicit raw-value state enum near the existing type helpers in `src/storage/src/redis.rs`:

```rust
pub enum TypeCheckState {
    Missing,
    Stale,
    Match,
}
```

Add one shared raw-value helper:

```rust
pub fn check_type_state(
    &self,
    value_raw: &[u8],
    expected: DataType,
) -> Result<TypeCheckState>
```

Its semantics are:

- Empty raw value returns `TypeCheckState::Missing`.
- Stale raw value returns `TypeCheckState::Stale`.
- Live raw value whose first byte matches `expected` returns `TypeCheckState::Match`.
- Live raw value whose first byte differs from `expected` returns the standard `WRONGTYPE` error.
- Invalid encoded raw values continue to return format/type errors through existing decoding and `is_stale` logic.

Keep `check_type(value_raw, expected)` as a compatibility wrapper:

```rust
pub fn check_type(&self, value_raw: &[u8], expected: DataType) -> Result<()> {
    self.check_type_state(value_raw, expected).map(|_| ())
}
```

Keep `get_key_type(key)` for the `TYPE` command and other key-only entry points. It should not be the normal command-level type check once a command has already read the top-level raw value.

## Command Rule

Every command should follow one rule:

If the command already has the top-level encoded value for the key, call `check_type_state(raw, expected)`. Do not call `get_key_type(key)`.

If the command only has a key and genuinely needs to report the key's type without reading/parsing the command's value path, call `get_key_type(key)`.

## Family Mapping

String commands:

- Use the raw string value read from `MetaCF` / default CF.
- Replace repeated `get_key_type(key)? != DataType::String` checks after reading `encode_value`.
- Map `Missing` and `Stale` per command. Examples: `GETBIT` returns `0`; `BITPOS` returns its existing missing-key default; read commands that currently return `KeyNotFound` keep that behavior unless Redis compatibility tests require adjustment.

Hash commands:

- Use the top-level hash meta raw value.
- Replace ad hoc `is_stale + check_type` ordering with `check_type_state`.
- Keep command-specific handling for empty or stale hash meta.

Set commands:

- Use the top-level set meta raw value.
- `SCARD` maps `Missing` or `Stale` to the existing `KeyNotFound` behavior.
- Commands such as `SMEMBERS` that return an empty collection on missing keys continue to do so.

List commands:

- Use the top-level list meta raw value.
- Commands that create a list on missing/stale keys keep their create path.
- Commands that return empty/zero/none on missing/stale keys keep their current command-specific return values.

ZSet commands:

- Use the top-level zset meta raw value.
- Preserve existing command-specific mappings for missing/stale keys and wrong-type live keys.

## Migration Plan

1. Add `TypeCheckState`, a shared `wrong_type` constructor/helper, and `check_type_state` in `redis.rs`.
2. Update `check_type` to delegate to `check_type_state`.
3. Update string commands first, because they currently have the most repeated `get_key_type` checks after already reading the raw value.
4. Update hash, set, list, and zset commands to use `check_type_state` where they currently do explicit stale checks plus `check_type`.
5. Keep `get_key_type` calls only in key-only paths such as storage `TYPE` handling, `key_exists_live`, or existence checks where reading a raw value would otherwise be duplicated.
6. After each family is migrated, run focused storage tests before moving to the next family.

## Testing Plan

Add or keep tests for these behavior classes:

- Missing key behavior for representative commands in each family.
- Stale key behavior for representative commands in each family.
- Live wrong-type behavior returns `WRONGTYPE`.
- Live matching type proceeds normally.
- String commands such as `GETBIT`, `BITCOUNT`, and `GETRANGE` no longer require an extra `get_key_type` lookup after reading `encode_value`.
- Existing `TYPE` command behavior still reports live key types and returns `none` for missing/stale keys.

## Risks

The main risk is accidentally changing Redis-visible missing/stale behavior while unifying internal type checks. To control that risk, the helper should only classify raw value state and raise `WRONGTYPE`; each command should still decide how to map `Missing` and `Stale` to its return value.

Another risk is parsing stale or wrong-type values after the state check. The migration should ensure parser construction only happens after `TypeCheckState::Match`, except in command paths that intentionally parse for another reason.

## Acceptance Criteria

- Five command families use the same raw-value state helper for command-level type checks.
- Ordinary command paths do not call `get_key_type` after they have already read the top-level raw value for the same key.
- `WRONGTYPE` construction is centralized.
- Existing missing/stale command behavior is either preserved or explicitly changed with tests.
- Focused storage tests pass.
