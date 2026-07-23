# Redis Vector Set Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 在 Kiwi 中实现 Redis 8 Vector Set 的 standalone Phase 1：持久化 FP32 向量，提供精确 FLAT 相似度查询，并支持 `VADD`、`VSIM`、`VREM`、`VCARD`、`VDIM`、`VEMB`、`VISMEMBER`。

**Architecture:** 沿用 Kiwi 现有 Hash/Set/ZSet 的复合类型模型。一个 VectorSet key 只按 user key 路由到一个 RocksDB instance；`MetaCF` 保存类型、数量、生命周期 version 和维度，新增 `VectorDataCF` 保存 `key + version + element` 对应的 canonical vector。写命令通过同一个 RocksDB `WriteBatch` 原子更新 Meta 和成员；`VSIM` 在该 instance 内按 generation prefix 扫描并使用有界 Top-K heap 返回精确结果。

**Tech Stack:** Rust 2021、Tokio 双 runtime、RocksDB Column Family / WriteBatch / Snapshot、RESP2/RESP3、Python redis-py 集成测试。

## Global Constraints

- 基线为 `origin/main` 的 `cdada8b`；忽略尚未合入的 error-catalog/error-model 分支，错误处理遵循当前主分支模式。
- 首版只支持 standalone；所有 Vector Set storage API 在 Raft append hook 已安装时返回 `ERR Vector Set is not supported in cluster mode`。
- 首版只支持 cosine、canonical FP32 little-endian 和显式 `NOQUANT`；默认 Q8、显式 `Q8`、`BIN`、VEMB `RAW` 均返回明确的 unsupported 错误。
- 首版只实现 FLAT；`TRUTH` 与普通 `VSIM` 都走同一个精确引擎，但保留不同的搜索模式枚举。
- 首版不实现 `VINFO`、`INFO VECTOR`、HNSW、FAISS/IVF、`storage_incarnation`、O(1) `DEL`、Raft logical mutation、ReadIndex、snapshot barrier 和滚动升级门禁。
- 复用 `MemberDataKey` 的 `key + version + element` 布局，不引入第二套 Vector member key codec。
- Meta 保持现有统一 envelope；reserve 的前 8 字节固定为 `format(1) + encoding(1) + metric(1) + flags(1) + dimension(4 LE)`，其余 8 字节写零。
- 所有新增 `.rs` 文件复制现有 Apache 2.0 license header；生产代码不得使用 `unwrap()`。
- 不修改已有未跟踪的 `tests/python/test_vector_basic.py`；该文件验证的是旧 FT/HASH 原型，不属于 Vector Set。
- 每个任务先写失败测试，再做最小实现；最终执行 `make fmt && make lint && make build && make test`。

---

## File Map

| 文件 | 职责 |
|---|---|
| `src/storage/src/format_base_value.rs` | 追加 `DataType::VectorSet = 7`、字符串和 tag 映射 |
| `src/storage/src/redis.rs` | 追加 `VectorDataCF = 6`，创建 CF、配置 compaction filter、加入 batch handles |
| `src/storage/src/vector.rs` | 定义 canonical vector、查询参数、命中结果和数值计算 |
| `src/storage/src/format_vector.rs` | 编解码 VectorSet meta 和 VectorDataCF value |
| `src/storage/src/redis_vectors.rs` | 单个 RocksDB instance 上的 VADD/VREM/point read/FLAT scan |
| `src/storage/src/storage_impl.rs` | 按 user key 路由所有 Vector Set 操作 |
| `src/storage/src/data_compaction_filter.rs` | 让 VectorDataCF 复用现有 version/TTL 垃圾清理 |
| `src/storage/src/meta_compaction_filter.rs` | 让过期或空 VectorSet meta 复用现有清理规则 |
| `src/storage/src/redis_strings.rs` | 将 VectorDataCF 纳入 DEL 与 FLUSHDB 的物理清理 |
| `src/storage/src/batch.rs` | 将 CF index 6 映射到 VectorDataCF |
| `src/storage/src/storage.rs` | Raft apply 的 CF index 6 解码；即使首版拒绝集群也保持 schema 完整 |
| `src/conf/src/raft_type.rs` | 追加跨 crate 使用的 VectorDataCF index |
| `src/storage/src/logindex/types.rs` | 追加 VectorDataCF 的 log-index 元数据 |
| `src/raft/src/lib.rs` | 追加 CF 名称和一致性断言 |
| `src/cmd/src/vector.rs` | 七个命令、参数解析、错误和 RESP reply 构造 |
| `src/cmd/src/lib.rs` | 导出 vector 命令模块 |
| `src/cmd/src/table.rs` | 注册七个命令 |
| `src/resp/src/encode.rs` | RESP2 下递归降级 Map/Double，RESP3 保持原生类型 |
| `src/storage/tests/redis_vector_test.rs` | storage 行为、生命周期、多 instance 和 FLAT 排序测试 |
| `tests/python/test_vector_set_commands.py` | 真实 server 的 RESP2/RESP3 命令兼容测试 |

---

### Task 1: Add the VectorSet data type and VectorDataCF plumbing

**Files:**

- Modify: `src/storage/src/format_base_value.rs`
- Modify: `src/storage/src/redis.rs`
- Modify: `src/storage/src/batch.rs`
- Modify: `src/storage/src/storage.rs`
- Modify: `src/conf/src/raft_type.rs`
- Modify: `src/storage/src/logindex/types.rs`
- Modify: `src/raft/src/lib.rs`
- Modify: `src/storage/tests/redis_basic_test.rs`

**Interfaces:**

- Produces: `DataType::VectorSet = 7`
- Produces: `ColumnFamilyIndex::VectorDataCF = 6`
- Produces: RocksDB CF name `vector_data_cf`
- Preserves: all existing DataType and CF numeric values

- [x] **Step 1: Extend the CF expectations in the existing storage test**

Update `test_open_redis` and `test_column_family_index` in `src/storage/tests/redis_basic_test.rs` to require seven handles and the exact final mapping:

```rust
assert_eq!(redis.handles.len(), 7);

let expected_cf_names = [
    "default",
    "hash_data_cf",
    "set_data_cf",
    "list_data_cf",
    "zset_data_cf",
    "zset_score_cf",
    "vector_data_cf",
];

assert_eq!(ColumnFamilyIndex::VectorDataCF as usize, 6);
assert_eq!(ColumnFamilyIndex::COUNT, 7);
```

- [x] **Step 2: Run the focused test and confirm the expected failure**

Run:

```bash
cargo test -p storage --test redis_basic_test test_open_redis -- --exact
```

Expected: compile failure because `ColumnFamilyIndex::VectorDataCF` does not exist, or assertion failure because only six CFs are opened.

- [x] **Step 3: Append DataType without renumbering existing values**

Apply this exact public shape in `src/storage/src/format_base_value.rs`:

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DataType {
    String = 0,
    Hash = 1,
    Set = 2,
    List = 3,
    ZSet = 4,
    None = 5,
    All = 6,
    VectorSet = 7,
}

pub const DATA_TYPE_STRINGS: [&str; 8] = [
    "string", "hash", "set", "list", "zset", "none", "all", "vectorset",
];
pub const DATA_TYPE_TAG: [char; 8] = ['k', 'h', 's', 'l', 'z', 'n', 'a', 'v'];
```

Update both exhaustive matches:

```rust
DataType::Hash | DataType::Set | DataType::ZSet | DataType::VectorSet => {
    Ok(BASE_META_VALUE_LENGTH)
}
```

```rust
7 => Ok(DataType::VectorSet),
```

- [x] **Step 4: Append VectorDataCF to every schema mapping**

Use the following invariant in all listed files:

```rust
VectorDataCF = 6
ColumnFamilyIndex::COUNT = 7
VectorDataCF.name() = "vector_data_cf"
VectorDataCF.data_type() = Some(DataType::VectorSet)
```

Update these exact mapping sites:

- `src/storage/src/redis.rs`: enum, `COUNT`, `name`, `data_type`, `CF_CONFIGS`, compaction-filter CF list, and `create_rocks_batch` handle list.
- `src/storage/src/batch.rs`: `cf_index_to_usize(ColumnFamilyIndex::VectorDataCF) => 6`.
- `src/storage/src/storage.rs`: `entry.cf_idx == 6` maps to `ColumnFamilyIndex::VectorDataCF`.
- `src/conf/src/raft_type.rs`: append variant and `from_u32(6)`.
- `src/storage/src/logindex/types.rs`: count becomes 7 and both CF-name arrays append `vector_data_cf`.
- `src/raft/src/lib.rs`: append `vector_data_cf` to `CF_NAMES` and `test_cf_names_match_storage` variants.

Do not insert the new value in the middle of any enum or array.

- [x] **Step 5: Run the schema tests**

Run:

```bash
cargo test -p storage --test redis_basic_test test_open_redis
cargo test -p storage --test redis_basic_test test_column_family_index
cargo test -p raft test_cf_names_match_storage
```

Expected: all tests PASS and a newly opened database reports seven CF handles in index order.

- [x] **Step 6: Commit the schema slice**

```bash
git add src/storage/src/format_base_value.rs src/storage/src/redis.rs src/storage/src/batch.rs src/storage/src/storage.rs src/conf/src/raft_type.rs src/storage/src/logindex/types.rs src/raft/src/lib.rs src/storage/tests/redis_basic_test.rs
git commit -m "feat(storage): add vector data column family"
```

---

### Task 2: Implement canonical vector, meta, and value codecs

**Files:**

- Create: `src/storage/src/vector.rs`
- Create: `src/storage/src/format_vector.rs`
- Modify: `src/storage/src/lib.rs`

**Interfaces:**

- Produces: `CanonicalVector::from_fp32_le(&[u8]) -> Result<CanonicalVector>`
- Produces: `CanonicalVector::from_values(&[f32]) -> Result<CanonicalVector>`
- Produces: `CanonicalVector::score(&CanonicalVector) -> Result<f64>`
- Produces: `CanonicalVector::restore() -> Vec<f64>`
- Produces: `VectorMeta::{new, encode, decode, count, set_count, version, dimension, is_stale}`
- Produces: `VectorDataValue::{from_canonical, encode, decode}`
- Produces: `VectorQuery`, `VectorSearchMode`, `VectorSearchOptions`, `VectorHit`

- [x] **Step 1: Write codec and numerical unit tests before exporting modules**

Place unit tests in the two new files. Cover these exact cases:

```rust
#[test]
fn canonical_vector_normalizes_and_restores_values() {
    let vector = CanonicalVector::from_values(&[3.0, 4.0]).expect("valid vector");
    assert_eq!(vector.dimension(), 2);
    assert!((vector.original_l2() - 5.0).abs() < 1e-6);
    let restored = vector.restore();
    assert!((restored[0] - 3.0).abs() < 1e-6);
    assert!((restored[1] - 4.0).abs() < 1e-6);
}

#[test]
fn canonical_vector_rejects_invalid_inputs() {
    assert!(CanonicalVector::from_values(&[]).is_err());
    assert!(CanonicalVector::from_values(&[0.0, 0.0]).is_err());
    assert!(CanonicalVector::from_values(&[f32::NAN]).is_err());
    assert!(CanonicalVector::from_fp32_le(&[0, 1, 2]).is_err());
}

#[test]
fn cosine_score_maps_to_redis_range() {
    let x = CanonicalVector::from_values(&[1.0, 0.0]).expect("valid x");
    let same = CanonicalVector::from_values(&[1.0, 0.0]).expect("valid same");
    let opposite = CanonicalVector::from_values(&[-1.0, 0.0]).expect("valid opposite");
    assert!((x.score(&same).expect("score") - 1.0).abs() < 1e-12);
    assert!(x.score(&opposite).expect("score").abs() < 1e-12);
}
```

Codec tests must round-trip an empty binary element independently through `MemberDataKey`, a two-dimensional vector value, and a `VectorMeta` with `count=2`, `version=42`, `dimension=2`.

- [x] **Step 2: Run tests and confirm the modules are missing**

Run:

```bash
cargo test -p storage vector::tests
cargo test -p storage format_vector::tests
```

Expected: compile failure until the modules and types are implemented and exported.

- [x] **Step 3: Implement the public vector types**

Define the following exact public API in `src/storage/src/vector.rs`:

```rust
#[derive(Debug, Clone, PartialEq)]
pub struct CanonicalVector {
    dimension: u32,
    original_l2: f32,
    normalized: Vec<f32>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum VectorQuery {
    Element(Vec<u8>),
    Vector(CanonicalVector),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VectorSearchMode {
    Approximate,
    Truth,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct VectorSearchOptions {
    pub count: usize,
    pub mode: VectorSearchMode,
}

#[derive(Debug, Clone, PartialEq)]
pub struct VectorHit {
    pub element: Vec<u8>,
    pub score: f64,
}
```

Numerical rules:

- FP32 blob length must be non-zero and divisible by four.
- Parse every component with `f32::from_le_bytes`; reject non-finite values.
- Accumulate `norm²` in `f64`; reject zero or non-finite norm.
- Store normalized components as `f32`, store original L2 as `f32`.
- Compute dot product in `f64`, clamp it to `[-1, 1]`, then return `(dot + 1.0) / 2.0` clamped to `[0, 1]`.
- Reject score comparisons when dimensions differ.

- [x] **Step 4: Implement fixed codecs**

Use this exact metadata layout in `src/storage/src/format_vector.rs`:

```text
| type=7 | count | version | format | encoding | metric | flags | dimension | zero reserve | ctime | etime |
|   1B   |  8B   |   8B    |   1B   |    1B    |   1B   |  1B   |   4B LE   |     8B      |  8B   |  8B   |
```

Constants:

```rust
pub const VECTOR_META_FORMAT: u8 = 1;
pub const VECTOR_ENCODING_FP32_LE: u8 = 1;
pub const VECTOR_METRIC_COSINE: u8 = 1;
pub const VECTOR_VALUE_MAGIC: u8 = 0x56;
pub const VECTOR_VALUE_FORMAT: u8 = 1;
```

Use this value layout:

```text
| magic=0x56 | format=1 | dimension | original_l2 | normalized FP32 payload |
|     1B     |    1B    |   4B LE   |   4B LE     |    dimension * 4B       |
```

Decoders must check exact length, magic, format, non-zero dimension, finite positive `original_l2`, finite payload components, and payload dimension. Return `InvalidFormatSnafu` on malformed persisted bytes; never panic.

- [x] **Step 5: Export the new modules and rerun tests**

Add to `src/storage/src/lib.rs`:

```rust
mod format_vector;
pub mod vector;

pub use vector::{
    CanonicalVector, VectorHit, VectorQuery, VectorSearchMode, VectorSearchOptions,
};
```

Run:

```bash
cargo test -p storage vector::tests
cargo test -p storage format_vector::tests
```

Expected: all canonicalization and codec tests PASS.

- [x] **Step 6: Commit the codec slice**

```bash
git add src/storage/src/lib.rs src/storage/src/vector.rs src/storage/src/format_vector.rs
git commit -m "feat(storage): add vector codecs"
```

---

### Task 3: Implement atomic VectorSet mutations and point reads

**Files:**

- Create: `src/storage/src/redis_vectors.rs`
- Modify: `src/storage/src/lib.rs`
- Create: `src/storage/tests/redis_vector_test.rs`

**Interfaces:**

- Consumes: `CanonicalVector`, `VectorMeta`, `VectorDataValue`, `MemberDataKey`
- Produces: `Redis::vadd(key, element, vector) -> Result<bool>`
- Produces: `Redis::vrem(key, element) -> Result<bool>`
- Produces: `Redis::vcard(key) -> Result<u64>`
- Produces: `Redis::vdim(key) -> Result<u32>`
- Produces: `Redis::vemb(key, element) -> Result<Option<Vec<f64>>>`
- Produces: `Redis::vismember(key, element) -> Result<bool>`
- Produces: `Redis::is_cluster_mode() -> bool`

- [x] **Step 1: Write storage behavior tests**

Create the test file using `unique_test_db_path`, `safe_cleanup_test_db`, `StorageOptions`, `BgTaskHandler`, and `LockMgr`, matching existing storage test setup. Add these independently named tests:

- `test_vadd_create_update_and_dimension_guard`
- `test_vadd_is_binary_safe_and_accepts_empty_element`
- `test_vcard_vdim_vemb_and_vismember_missing_semantics`
- `test_vrem_deletes_last_member_and_meta`
- `test_vector_commands_return_wrongtype_for_string_key`
- `test_vector_meta_and_member_are_committed_together`

The core assertions are:

```rust
let a = CanonicalVector::from_values(&[1.0, 0.0]).expect("valid vector");
let b = CanonicalVector::from_values(&[0.0, 1.0]).expect("valid vector");

assert!(redis.vadd(b"vectors", b"a", &a).expect("insert a"));
assert!(redis.vadd(b"vectors", b"b", &b).expect("insert b"));
assert!(!redis.vadd(b"vectors", b"a", &b).expect("update a"));
assert_eq!(redis.vcard(b"vectors").expect("card"), 2);
assert_eq!(redis.vdim(b"vectors").expect("dim"), 2);
assert_eq!(redis.vemb(b"vectors", b"a").expect("emb"), Some(vec![0.0, 1.0]));
assert!(redis.vismember(b"vectors", b"a").expect("member"));
assert!(redis.vadd(b"vectors", b"\x00binary", &a).expect("binary member"));
assert!(redis.vadd(b"empty-element", b"", &a).expect("empty member"));
```

For dimension mismatch, assert `is_err()` and confirm that `VCARD` and the old member value remain unchanged. For last-member removal, assert `get_key_type(key)` returns a missing-key error after `VREM`.

- [x] **Step 2: Run the behavior tests and confirm missing methods**

Run:

```bash
cargo test -p storage --test redis_vector_test
```

Expected: compile failure because the six `Redis` methods are not implemented.

- [x] **Step 3: Implement one locked read-modify-write path for VADD**

Add this private module declaration to `src/storage/src/lib.rs`:

```rust
mod redis_vectors;
```

Implementation sequence inside `Redis::vadd`:

1. Reject cluster mode by checking `self.append_log_fn.get().is_some()`.
2. Acquire `ScopeRecordLock` using the user key.
3. Read `MetaCF[BaseMetaKey::new(key)]`.
4. Missing or stale meta creates `VectorMeta::new(1, dimension)` with a new timestamp-based version and does not increment it again.
5. Live non-VectorSet meta returns the same WRONGTYPE text used by `check_type_state`.
6. Live VectorSet requires equal dimension.
7. Point-read `VectorDataCF[MemberDataKey(key, version, element)]` to distinguish insert from update.
8. Inserting into an existing live VectorSet increments count; creating a new VectorSet keeps the initial count at one; updating an existing element preserves count.
9. Build one batch containing the member put and meta put; commit once.
10. Return `true` for insert and `false` for update.

The public signature must be:

```rust
pub fn vadd(
    &self,
    key: &[u8],
    element: &[u8],
    vector: &CanonicalVector,
) -> Result<bool>;
```

- [x] **Step 4: Implement VREM and point-read commands**

Use `VectorMeta::decode` and the same `MemberDataKey` generation for every operation.

VREM rules:

- Missing/stale key or missing element returns `Ok(false)`.
- Wrong type returns WRONGTYPE.
- If count is greater than one, delete member and write `count - 1` meta in one batch.
- If count is one, delete member and delete MetaCF in one batch.

Read rules:

- `vcard`: missing/stale returns `0`.
- `vdim`: missing/stale returns `KeyNotFoundSnafu`.
- `vemb`: missing/stale member or key returns `None`; live value returns `CanonicalVector::restore()`.
- `vismember`: missing/stale member or key returns `false`.
- All live wrong-type keys return WRONGTYPE.

- [x] **Step 5: Run storage tests**

Run:

```bash
RUST_TEST_THREADS=1 cargo test -p storage --test redis_vector_test
cargo test -p storage vector::tests format_vector::tests
```

Expected: all tests PASS, including rollback-visible behavior after dimension mismatch.

- [x] **Step 6: Commit point operations**

```bash
git add src/storage/src/lib.rs src/storage/src/redis_vectors.rs src/storage/tests/redis_vector_test.rs
git commit -m "feat(storage): persist vector set members"
```

---

### Task 4: Implement exact FLAT search with stable Top-K ordering

**Files:**

- Modify: `src/storage/src/redis_vectors.rs`
- Modify: `src/storage/src/vector.rs`
- Modify: `src/storage/tests/redis_vector_test.rs`

**Interfaces:**

- Consumes: `VectorQuery`, `VectorSearchOptions`
- Produces: `Redis::vsim(key, query, options) -> Result<Vec<VectorHit>>`
- Ordering contract: score descending, then raw element bytes ascending
- Complexity contract: `O(N × DIM + N log K)` time and `O(K + DIM)` extra memory

- [x] **Step 1: Add failing FLAT search tests**

Add these tests:

- `test_vsim_direct_vector_returns_exact_top_k`
- `test_vsim_ele_uses_stored_member_as_query`
- `test_vsim_stable_tie_breaks_by_raw_element_bytes`
- `test_vsim_truth_matches_approximate_in_phase_one`
- `test_vsim_missing_key_is_empty_and_missing_ele_is_error`
- `test_vsim_rejects_query_dimension_mismatch`

Use this deterministic dataset:

```rust
let x = CanonicalVector::from_values(&[1.0, 0.0]).expect("x");
let y = CanonicalVector::from_values(&[0.0, 1.0]).expect("y");
let neg_x = CanonicalVector::from_values(&[-1.0, 0.0]).expect("negative x");
redis.vadd(b"search", b"b", &y).expect("insert b");
redis.vadd(b"search", b"a", &y).expect("insert a");
redis.vadd(b"search", b"x", &x).expect("insert x");
redis.vadd(b"search", b"neg", &neg_x).expect("insert neg");
```

For query `x` and `COUNT 3`, require `x` first, then `a`, then `b`; `a` and `b` tie on score and raw bytes decide their order.

- [x] **Step 2: Run the focused tests and confirm VSim is missing**

```bash
cargo test -p storage --test redis_vector_test test_vsim
```

Expected: compile failure because `Redis::vsim` is not defined.

- [x] **Step 3: Add a heap item with total ordering**

Define a private `HeapHit` that compares worse hits as the heap maximum, so a heap capped at K can replace its worst element. Equality and ordering must use both `score.total_cmp()` and raw element bytes; do not use `partial_cmp().unwrap()`.

Required result ordering after draining the heap:

```rust
hits.sort_by(|left, right| {
    right
        .score
        .total_cmp(&left.score)
        .then_with(|| left.element.cmp(&right.element))
});
```

- [x] **Step 4: Implement one RocksDB snapshot scan**

The exact search sequence is:

1. Return an empty vector when the key is missing or stale.
2. Return WRONGTYPE for a live non-VectorSet key.
3. Decode meta and resolve an `Element` query with a point read from the same RocksDB snapshot.
4. Reject missing query element and dimension mismatch.
5. Build `MemberDataKey::new(key, version, b"").encode_seek_key()`.
6. Iterate `VectorDataCF` forward from the prefix using snapshot read options.
7. Stop at the first key not starting with the prefix.
8. Strip prefix and the 16-byte suffix reserve to recover raw element bytes.
9. Decode each vector value and compute score.
10. Keep at most `options.count` heap entries.
11. Drain and sort using the stable ordering above.

Reject `options.count == 0` before creating the iterator.

- [x] **Step 5: Run FLAT and regression tests**

```bash
RUST_TEST_THREADS=1 cargo test -p storage --test redis_vector_test test_vsim
RUST_TEST_THREADS=1 cargo test -p storage --test redis_set_test
```

Expected: Vector search tests PASS and existing Set prefix-iteration tests remain green.

- [x] **Step 6: Commit the search engine**

```bash
git add src/storage/src/vector.rs src/storage/src/redis_vectors.rs src/storage/tests/redis_vector_test.rs
git commit -m "feat(storage): add flat vector similarity search"
```

---

### Task 5: Wire Storage routing, TYPE, TTL cleanup, DEL, and FLUSHDB

**Files:**

- Modify: `src/storage/src/storage_impl.rs`
- Modify: `src/storage/src/data_compaction_filter.rs`
- Modify: `src/storage/src/meta_compaction_filter.rs`
- Modify: `src/storage/src/redis.rs`
- Modify: `src/storage/src/redis_strings.rs`
- Modify: `src/storage/tests/redis_vector_test.rs`

**Interfaces:**

- Produces: the same seven operations on `Storage`, each routed exactly once by user key
- Preserves: `TYPE key -> vectorset`
- Preserves: generic EXPIRE/PERSIST behavior through the common Meta envelope
- Preserves: generic DEL and FLUSHDB physically delete VectorDataCF entries

- [x] **Step 1: Add facade and lifecycle tests**

Add tests using `Storage::new(3, 0)` for:

- `test_storage_routes_all_members_of_one_vectorset_to_one_instance`
- `test_type_returns_vectorset`
- `test_expired_vectorset_reads_as_missing`
- `test_del_removes_vector_meta_and_members`
- `test_flushdb_removes_vector_meta_and_members`
- `test_vector_storage_rejects_cluster_mode`

The multi-instance test must compute `key_to_slot_id(key)` and confirm only the selected instance contains the MetaCF key; it must not scan or fan out for VSIM.

- [x] **Step 2: Run tests and confirm missing Storage methods**

```bash
cargo test -p storage --test redis_vector_test test_storage_routes
cargo test -p storage --test redis_vector_test test_del_removes_vector
```

Expected: compile failure because the Vector methods exist only on `Redis`.

- [x] **Step 3: Add exact one-key routing methods**

Add these signatures to `src/storage/src/storage_impl.rs`:

```rust
pub fn vadd(&self, key: &[u8], element: &[u8], vector: &CanonicalVector) -> Result<bool>;
pub fn vsim(
    &self,
    key: &[u8],
    query: VectorQuery,
    options: VectorSearchOptions,
) -> Result<Vec<VectorHit>>;
pub fn vrem(&self, key: &[u8], element: &[u8]) -> Result<bool>;
pub fn vcard(&self, key: &[u8]) -> Result<u64>;
pub fn vdim(&self, key: &[u8]) -> Result<u32>;
pub fn vemb(&self, key: &[u8], element: &[u8]) -> Result<Option<Vec<f64>>>;
pub fn vismember(&self, key: &[u8], element: &[u8]) -> Result<bool>;
```

Every method must perform the same two routing statements and then call its matching Redis method. For example, `Storage::vadd` is:

```rust
let slot_id = key_to_slot_id(key);
let instance_id = self.slot_indexer.get_instance_id(slot_id);
self.insts[instance_id].vadd(key, element, vector)
```

Apply the identical prefix to `vsim`, `vrem`, `vcard`, `vdim`, `vemb`, and `vismember`, forwarding their declared arguments unchanged. There is no element routing and no instance fan-out.

- [x] **Step 4: Extend common lifecycle handling**

Apply these exact additions:

- `src/storage/src/redis.rs::is_stale_static`: include `DataType::VectorSet` in the common count/version meta branch.
- `src/storage/src/redis_strings.rs::set_key_etime`: parse `DataType::VectorSet` with `ParsedBaseMetaValue`, identical to Hash/Set/ZSet.
- `src/storage/src/meta_compaction_filter.rs`: parse `DataType::VectorSet` with `ParsedBaseMetaValue`.
- `src/storage/src/data_compaction_filter.rs::parse_meta_value` and its test-only meta parser: parse VectorSet with `ParsedBaseMetaValue`.
- `Redis::del_key`: add `ColumnFamilyIndex::VectorDataCF` to the composite data CF scan list.
- `Redis::flush_db`: add `ColumnFamilyIndex::VectorDataCF` to `all_cf_indexes`.

Do not introduce a wildcard in the schema matches above; each VectorSet/VectorDataCF branch must be explicit.

- [x] **Step 5: Run lifecycle and existing TTL tests**

```bash
RUST_TEST_THREADS=1 cargo test -p storage --test redis_vector_test
RUST_TEST_THREADS=1 cargo test -p storage --test ttl_test
RUST_TEST_THREADS=1 cargo test -p storage --test redis_basic_test
```

Expected: all tests PASS; expired vector data becomes invisible immediately and eligible for compaction, while DEL/FLUSHDB remove physical VectorDataCF entries.

- [x] **Step 6: Commit routing and lifecycle support**

```bash
git add src/storage/src/storage_impl.rs src/storage/src/data_compaction_filter.rs src/storage/src/meta_compaction_filter.rs src/storage/src/redis.rs src/storage/src/redis_strings.rs src/storage/tests/redis_vector_test.rs
git commit -m "feat(storage): route and clean up vector sets"
```

---

### Task 6: Add Vector Set command parsing and command-table registration

**Files:**

- Create: `src/cmd/src/vector.rs`
- Modify: `src/cmd/src/lib.rs`
- Modify: `src/cmd/src/table.rs`

**Interfaces:**

- Produces: `VAddCmd`, `VSimCmd`, `VRemCmd`, `VCardCmd`, `VDimCmd`, `VEmbCmd`, `VIsMemberCmd`
- Consumes: all seven `Storage` APIs from Task 5
- Produces: binary-safe argv parsing and `RespData` replies

- [x] **Step 1: Write parser and metadata unit tests in the new module**

Cover all supported command shapes:

```text
VADD key FP32 blob element NOQUANT
VADD key VALUES num value [value ...] element NOQUANT
VSIM key ELE element [WITHSCORES] [COUNT num] [TRUTH]
VSIM key FP32 blob [WITHSCORES] [COUNT num] [TRUTH]
VSIM key VALUES num value [value ...] [WITHSCORES] [COUNT num] [TRUTH]
VREM key element
VCARD key
VDIM key
VEMB key element
VISMEMBER key element
```

Add explicit failure tests for malformed FP32 length, invalid VALUES dimension/count, invalid float, missing `NOQUANT`, `Q8`, `BIN`, duplicate VSIM options, zero/invalid COUNT, `VEMB RAW`, and trailing unknown options.

Metadata assertions:

```rust
assert_eq!(VAddCmd::new().meta().arity, -5);
assert_eq!(VSimCmd::new().meta().arity, -4);
assert_eq!(VRemCmd::new().meta().arity, 3);
assert_eq!(VCardCmd::new().meta().arity, 2);
assert_eq!(VDimCmd::new().meta().arity, 2);
assert_eq!(VEmbCmd::new().meta().arity, -3);
assert_eq!(VIsMemberCmd::new().meta().arity, 3);
```

- [x] **Step 2: Run command tests and confirm the module is not registered**

```bash
cargo test -p cmd vector::tests
```

Expected: compile failure until the new module, command structs, and parsers exist.

- [x] **Step 3: Implement shared parsers without UTF-8 assumptions for keys/elements/blobs**

Only option keywords and numeric VALUES tokens are decoded as ASCII/UTF-8. Keep key, element, and FP32 blob as raw bytes.

Define private parsed forms:

```rust
struct ParsedVAdd {
    vector: CanonicalVector,
    element: Vec<u8>,
}

struct ParsedVSim {
    query: VectorQuery,
    options: VectorSearchOptions,
    with_scores: bool,
}
```

Error strings must be exact and already include the Redis error class:

```text
ERR invalid vector specification
ERR vector dimension mismatch
ERR default Q8 quantization is not supported in Phase 1; specify NOQUANT
ERR VADD option Q8 is not supported yet
ERR VADD option BIN is not supported yet
ERR VEMB option RAW is not supported yet
ERR element not found in set
ERR key does not exist
WRONGTYPE Operation against a key holding the wrong kind of value
```

Add one local reply helper that avoids double-prefixing current storage error strings:

```rust
fn error_reply(message: impl Into<String>) -> RespData {
    RespData::Error(message.into().into())
}
```

Map internal non-Redis storage failures to `ERR storage error`; log the detailed error with `log::error!`.

- [x] **Step 4: Implement command replies**

Reply contracts:

- VADD/VREM/VISMEMBER: integer `1` or `0`.
- VCARD/VDIM: integer after checked `u64/u32 -> i64` conversion.
- VEMB: null bulk for missing key/member; otherwise array of `RespData::Double`.
- VSIM without scores: array of bulk-string elements.
- VSIM with scores: `RespData::Map` of bulk-string element to `RespData::Double`; Task 7 performs RESP2 downgrade at the encoder boundary.

Flags and ACL categories:

- VADD/VREM: `WRITE | FAST | MODULE_NO_CLUSTER`, `KEYSPACE | WRITE`.
- VSIM: `READONLY | MODULE_NO_CLUSTER`, `KEYSPACE | READ | SLOW`.
- VCARD/VDIM/VEMB/VISMEMBER: `READONLY | FAST | MODULE_NO_CLUSTER`, `KEYSPACE | READ`.

- [x] **Step 5: Export and register all seven commands**

Add `pub mod vector;` to `src/cmd/src/lib.rs`. Append these types to `register_cmd!` in `src/cmd/src/table.rs`:

```rust
crate::vector::VAddCmd,
crate::vector::VSimCmd,
crate::vector::VRemCmd,
crate::vector::VCardCmd,
crate::vector::VDimCmd,
crate::vector::VEmbCmd,
crate::vector::VIsMemberCmd,
```

Add a table test asserting all lowercase names resolve.

- [x] **Step 6: Run command and runtime dispatch tests**

```bash
cargo test -p cmd vector::tests
cargo test -p cmd table
cargo test -p runtime handle_execute_command
```

Expected: all tests PASS and the generic `StorageCommand::Execute` path finds every Vector command without a new runtime message variant.

- [x] **Step 7: Commit the command layer**

```bash
git add src/cmd/src/vector.rs src/cmd/src/lib.rs src/cmd/src/table.rs
git commit -m "feat(cmd): add redis vector set commands"
```

---

### Task 7: Make VSIM WITHSCORES correct in RESP2 and RESP3

**Files:**

- Modify: `src/resp/src/encode.rs`
- Modify: `src/resp/src/negotiation.rs`

**Interfaces:**

- Consumes: command-layer `RespData::Map` and `RespData::Double`
- Produces: RESP3 `%` map with `,` doubles
- Produces: RESP2 flat array with bulk-string scores

- [x] **Step 1: Add encoder regression tests**

Add tests that encode this response under both protocol versions:

```rust
let response = RespData::Map(vec![
    (
        RespData::BulkString(Some(Bytes::from_static(b"a"))),
        RespData::Double(1.0),
    ),
    (
        RespData::BulkString(Some(Bytes::from_static(b"b"))),
        RespData::Double(0.5),
    ),
]);
```

Expected RESP3 bytes:

```text
%2\r\n$1\r\na\r\n,1\r\n$1\r\nb\r\n,0.5\r\n
```

Expected RESP2 bytes:

```text
*4\r\n$1\r\na\r\n$1\r\n1\r\n$1\r\nb\r\n$3\r\n0.5\r\n
```

- [x] **Step 2: Run the test and confirm RESP2 currently emits RESP3 types**

```bash
cargo test -p resp encode_vsim_withscores
```

Expected: RESP2 assertion FAIL because `RespEncoder` currently stores the version but does not use it when encoding Map/Double.

- [x] **Step 3: Normalize once at the encoder boundary**

Refactor `RespEncoder` so the public trait method converts the complete response once, then calls a private recursive encoder:

```rust
fn encode_resp_data(&mut self, data: &RespData) -> &mut Self {
    let normalized = if self.is_resp3() {
        data.clone()
    } else {
        ProtocolNegotiator::convert_to_resp2(data)
    };
    self.encode_resp_data_inner(&normalized)
}
```

Move the existing recursive match into `encode_resp_data_inner`, and make all nested array/map/set/push recursion call `encode_resp_data_inner` rather than invoking the version-normalizing method again.

Keep `ProtocolNegotiator::convert_to_resp2` recursive for Array, Map, Set, Push, Null, Boolean, Double, BigNumber, BulkError, and VerbatimString.

- [x] **Step 4: Run all RESP tests**

```bash
cargo test -p resp
```

Expected: all RESP tests PASS; existing RESP3 HELLO behavior remains unchanged and RESP2 never emits RESP3-only prefixes.

- [x] **Step 5: Commit protocol-aware encoding**

```bash
git add src/resp/src/encode.rs src/resp/src/negotiation.rs
git commit -m "fix(resp): downgrade vector scores for resp2"
```

---

### Task 8: Add end-to-end compatibility tests and run the Kiwi verification pipeline

**Files:**

- Create: `tests/python/test_vector_set_commands.py`
- Modify: `docs/superpowers/plans/2026-07-19-redis-vector-set.md` only to check completed boxes during execution

**Interfaces:**

- Verifies: command parsing through TCP, dual runtime dispatch, storage persistence, RESP2/RESP3 encoding, TYPE/DEL/EXPIRE interaction
- Does not consume: `tests/python/test_vector_basic.py`, FT.CREATE, FT.SEARCH, HSET vector fields

- [x] **Step 1: Write binary-safe Python integration tests**

Create a local fixture in the new test file using:

```python
import os
import struct

import pytest
import redis


@pytest.fixture(params=[2, 3])
def vector_client(request):
    client = redis.Redis(
        host=os.getenv("KIWI_HOST", "127.0.0.1"),
        port=int(os.getenv("KIWI_PORT", "7379")),
        decode_responses=False,
        protocol=request.param,
    )
    client.ping()
    yield client
    for key in client.scan_iter(match=b"test_vset:*"):
        client.delete(key)
    client.close()
```

Add tests for:

- VALUES create/update return values and dimension mismatch.
- FP32 little-endian blob via `struct.pack("<2f", 1.0, 0.0)`.
- Binary key and element including `b"\x00"`; empty element.
- VCARD, VDIM, VEMB, VISMEMBER, VREM and last-member key deletion.
- VSIM direct vector, ELE query, COUNT, WITHSCORES and TRUTH.
- Stable tie order.
- TYPE returns `b"vectorset"`.
- WRONGTYPE against an existing string key.
- Missing-key semantics for every read command.
- EXPIRE makes the key immediately behave as missing.
- DEL removes members and allows same-name recreation with a new dimension.
- Missing NOQUANT, Q8, BIN, RAW and malformed input errors.

For WITHSCORES, assert redis-py returns a list under RESP2 and a dict under RESP3, with the same element order and numerical scores.

- [x] **Step 2: Start standalone Kiwi and run only Vector integration tests**

Terminal 1:

```bash
make standalone
```

Terminal 2:

```bash
make -C tests install-deps
KIWI_PORT=7379 pytest -q tests/python/test_vector_set_commands.py
```

Expected: all RESP2 and RESP3 parameterized cases PASS.

- [x] **Step 3: Run focused Rust regression suites**

```bash
RUST_TEST_THREADS=1 cargo test -p storage --test redis_vector_test
RUST_TEST_THREADS=1 cargo test -p storage --test redis_hash_test
RUST_TEST_THREADS=1 cargo test -p storage --test redis_set_test
RUST_TEST_THREADS=1 cargo test -p storage --test ttl_test
cargo test -p cmd vector::tests
cargo test -p resp
```

Expected: all commands exit 0.

- [x] **Step 4: Run repository-standard verification**

Run exactly in this order:

```bash
make fmt
make lint
make build
make test
```

Expected: all commands exit 0. The first RocksDB build may be long; retain sccache and do not disable it repository-wide.

- [x] **Step 5: Inspect the final diff for scope and generated files**

```bash
git status --short
git diff --stat origin/main...HEAD
git diff --check origin/main...HEAD
```

Expected:

- No changes to `tests/python/test_vector_basic.py`.
- No FT.CREATE/FT.SEARCH implementation.
- No HNSW, Raft mutation, snapshot, or O(1) deletion code.
- No `__pycache__`, RocksDB data directory, build artifacts, or scratch notes staged.
- `git diff --check` produces no output.

- [x] **Step 6: Commit end-to-end tests**

```bash
git add tests/python/test_vector_set_commands.py docs/superpowers/plans/2026-07-19-redis-vector-set.md
git commit -m "test: cover redis vector set commands"
```

---

## Completion Criteria

The Phase 1 implementation is complete only when all statements below are true:

- A VectorSet key and all its elements live in one RocksDB instance selected only from the user key.
- Meta and member mutations commit in one batch; failed validation leaves both unchanged.
- `TYPE` reports `vectorset`; generic TTL, DEL and FLUSHDB semantics work.
- VADD accepts FP32 and VALUES only with explicit NOQUANT and is binary-safe for elements.
- VSIM returns exact, deterministic Top-K results without collecting and sorting every hit.
- RESP2 and RESP3 WITHSCORES replies use their native compatible shapes.
- Cluster mode rejects all seven Vector Set operations before creating a batch or snapshot.
- Focused Rust/Python tests and the complete Kiwi verification pipeline pass.
- The final diff contains no FT prototype extraction, HNSW, Raft/snapshot design implementation, or unrelated cleanup.

## Deferred Follow-up Plans

Create separate reviewed designs and plans before implementing any of these independent projects:

1. Redis-compatible VINFO and INFO VECTOR observability.
2. Raft logical VectorSet mutations, leader-linearizable reads and snapshots.
3. O(1) generation deletion with background compaction cleanup.
4. HNSW derived index, rebuild/recovery and rolling-upgrade capability gates.
