# Redis Vector Set 实施计划

> **致智能体执行者:** 必备子技能:使用 superpowers:subagent-driven-development(推荐)或 superpowers:executing-plans 按任务逐步实现本计划。各步骤使用复选框(`- [ ]`)语法进行跟踪。

**目标:** 在 Kiwi 中实现 Redis 8 Vector Set 的 standalone Phase 1:持久化 FP32 向量,提供精确 FLAT 相似度查询,并支持 `VADD`、`VSIM`、`VREM`、`VCARD`、`VDIM`、`VEMB`、`VISMEMBER`。

**架构:** 沿用 Kiwi 现有 Hash/Set/ZSet 的复合类型模型。一个 VectorSet key 只按 user key 路由到一个 RocksDB instance;`MetaCF` 保存类型、数量、生命周期 version 和维度,新增 `VectorDataCF` 保存 `key + version + element` 对应的 canonical vector。写命令通过同一个 RocksDB `WriteBatch` 原子更新 Meta 和成员;`VSIM` 在该 instance 内按 generation prefix 扫描并使用有界 Top-K heap 返回精确结果。

**技术栈:** Rust 2021、Tokio 双 runtime、RocksDB Column Family / WriteBatch / Snapshot、RESP2/RESP3、Python redis-py 集成测试。

## 全局约束

- 基线为 `origin/main` 的 `cdada8b`;忽略尚未合入的 error-catalog/error-model 分支,错误处理遵循当前主分支模式。
- 首版支持 standalone 与 Raft Group cluster 模式;暂不支持 Redis Cluster 的槽位路由。
- 首版只支持 cosine、canonical FP32 little-endian 和显式 `NOQUANT`;默认 Q8、显式 `Q8`、`BIN`、VEMB `RAW` 均返回明确的 unsupported 错误。
- 首版只实现 FLAT;`TRUTH` 与普通 `VSIM` 都走同一个精确引擎,但保留不同的搜索模式枚举。
- 首版不实现 `VINFO`、`INFO VECTOR`、HNSW、FAISS/IVF、`storage_incarnation`、O(1) `DEL`、Raft logical mutation、ReadIndex、snapshot barrier 和滚动升级门禁。
- 复用 `MemberDataKey` 的 `key + version + element` 布局,不引入第二套 Vector member key codec。
- Meta 保持现有统一 envelope;reserve 的前 8 字节固定为 `format(1) + encoding(1) + metric(1) + flags(1) + dimension(4 LE)`,其余 8 字节写零。
- 所有新增 `.rs` 文件复制现有 Apache 2.0 license header;生产代码不得使用 `unwrap()`。
- 不修改已有未跟踪的 `tests/python/test_vector_basic.py`;该文件验证的是旧 FT/HASH 原型,不属于 Vector Set。
- 每个任务先写失败测试,再做最小实现;最终执行 `make fmt && make lint && make build && make test`。

---

## 文件地图

| 文件 | 职责 |
|---|---|
| `src/storage/src/format_base_value.rs` | 追加 `DataType::VectorSet = 7`、字符串和 tag 映射 |
| `src/storage/src/redis.rs` | 追加 `VectorDataCF = 6`,创建 CF、配置 compaction filter、加入 batch handles |
| `src/storage/src/vector.rs` | 定义 canonical vector、查询参数、命中结果和数值计算 |
| `src/storage/src/format_vector.rs` | 编解码 VectorSet meta 和 VectorDataCF value |
| `src/storage/src/redis_vectors.rs` | 单个 RocksDB instance 上的 VADD/VREM/point read/FLAT scan |
| `src/storage/src/storage_impl.rs` | 按 user key 路由所有 Vector Set 操作 |
| `src/storage/src/data_compaction_filter.rs` | 让 VectorDataCF 复用现有 version/TTL 垃圾清理 |
| `src/storage/src/meta_compaction_filter.rs` | 让过期或空 VectorSet meta 复用现有清理规则 |
| `src/storage/src/redis_strings.rs` | 将 VectorDataCF 纳入 DEL 与 FLUSHDB 的物理清理 |
| `src/storage/src/batch.rs` | 将 CF index 6 映射到 VectorDataCF |
| `src/storage/src/storage.rs` | Raft apply 的 CF index 6 解码;即使首版拒绝集群也保持 schema 完整 |
| `src/conf/src/raft_type.rs` | 追加跨 crate 使用的 VectorDataCF index |
| `src/storage/src/logindex/types.rs` | 追加 VectorDataCF 的 log-index 元数据 |
| `src/raft/src/lib.rs` | 追加 CF 名称和一致性断言 |
| `src/cmd/src/vector/mod.rs` | 共享解析器、错误/回复辅助函数与命令注册测试 |
| `src/cmd/src/vector/vadd.rs` | `VAddCmd` 实现 |
| `src/cmd/src/vector/vsim.rs` | `VSimCmd` 实现 |
| `src/cmd/src/vector/vrem.rs` | `VRemCmd` 实现 |
| `src/cmd/src/vector/vcard.rs` | `VCardCmd` 实现 |
| `src/cmd/src/vector/vdim.rs` | `VDimCmd` 实现 |
| `src/cmd/src/vector/vemb.rs` | `VEmbCmd` 实现 |
| `src/cmd/src/vector/vismember.rs` | `VIsMemberCmd` 实现 |
| `src/cmd/src/lib.rs` | 导出 vector 命令模块 |
| `src/cmd/src/table.rs` | 注册七个命令 |
| `src/resp/src/encode.rs` | RESP2 下递归降级 Map/Double,RESP3 保持原生类型 |
| `src/storage/tests/redis_vector_test.rs` | storage 行为、生命周期、多 instance 和 FLAT 排序测试 |
| `tests/python/test_vector_set_commands.py` | 真实 server 的 RESP2/RESP3 命令兼容测试 |

---

### 任务 1:添加 VectorSet 数据类型和 VectorDataCF 管线

**文件:**

- 修改:`src/storage/src/format_base_value.rs`
- 修改:`src/storage/src/redis.rs`
- 修改:`src/storage/src/batch.rs`
- 修改:`src/storage/src/storage.rs`
- 修改:`src/conf/src/raft_type.rs`
- 修改:`src/storage/src/logindex/types.rs`
- 修改:`src/raft/src/lib.rs`
- 修改:`src/storage/tests/redis_basic_test.rs`

**接口:**

- 产出:`DataType::VectorSet = 7`
- 产出:`ColumnFamilyIndex::VectorDataCF = 6`
- 产出:RocksDB CF 名称 `vector_data_cf`
- 保持:所有既有 DataType 和 CF 数值不变

- [x] **步骤 1:扩展现有 storage 测试中的 CF 预期**

更新 `src/storage/tests/redis_basic_test.rs` 中的 `test_open_redis` 和 `test_column_family_index`,要求七个 handle 以及如下的精确最终映射:

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

- [x] **步骤 2:运行聚焦测试并确认预期失败**

运行:

```bash
cargo test -p storage --test redis_basic_test test_open_redis -- --exact
```

预期:编译失败,因为 `ColumnFamilyIndex::VectorDataCF` 尚不存在;或者断言失败,因为只打开了六个 CF。

- [x] **步骤 3:追加 DataType,不重排既有数值**

在 `src/storage/src/format_base_value.rs` 中应用如下精确的公开形态:

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

更新两处穷尽式 match:

```rust
DataType::Hash | DataType::Set | DataType::ZSet | DataType::VectorSet => {
    Ok(BASE_META_VALUE_LENGTH)
}
```

```rust
7 => Ok(DataType::VectorSet),
```

- [x] **步骤 4:将 VectorDataCF 追加到每一处 schema 映射**

在所有列出的文件中使用以下不变量:

```rust
VectorDataCF = 6
ColumnFamilyIndex::COUNT = 7
VectorDataCF.name() = "vector_data_cf"
VectorDataCF.data_type() = Some(DataType::VectorSet)
```

更新以下精确的映射位置:

- `src/storage/src/redis.rs`:enum、`COUNT`、`name`、`data_type`、`CF_CONFIGS`、compaction-filter CF 列表,以及 `create_rocks_batch` 的 handle 列表。
- `src/storage/src/batch.rs`:`cf_index_to_usize(ColumnFamilyIndex::VectorDataCF) => 6`。
- `src/storage/src/storage.rs`:`entry.cf_idx == 6` 映射到 `ColumnFamilyIndex::VectorDataCF`。
- `src/conf/src/raft_type.rs`:追加变体和 `from_u32(6)`。
- `src/storage/src/logindex/types.rs`:count 变为 7,两个 CF 名称数组都追加 `vector_data_cf`。
- `src/raft/src/lib.rs`:在 `CF_NAMES` 和 `test_cf_names_match_storage` 的变体中追加 `vector_data_cf`。

不要将新值插入任何 enum 或数组的中间位置。

- [x] **步骤 5:运行 schema 测试**

运行:

```bash
cargo test -p storage --test redis_basic_test test_open_redis
cargo test -p storage --test redis_basic_test test_column_family_index
cargo test -p raft test_cf_names_match_storage
```

预期:所有测试通过,新打开的数据库按索引顺序报告七个 CF handle。

- [x] **步骤 6:提交 schema 切片**

```bash
git add src/storage/src/format_base_value.rs src/storage/src/redis.rs src/storage/src/batch.rs src/storage/src/storage.rs src/conf/src/raft_type.rs src/storage/src/logindex/types.rs src/raft/src/lib.rs src/storage/tests/redis_basic_test.rs
git commit -m "feat(storage): add vector data column family"
```

---

### 任务 2:实现 canonical vector、meta 和 value 编解码器

**文件:**

- 新建:`src/storage/src/vector.rs`
- 新建:`src/storage/src/format_vector.rs`
- 修改:`src/storage/src/lib.rs`

**接口:**

- 产出:`CanonicalVector::from_fp32_le(&[u8]) -> Result<CanonicalVector>`
- 产出:`CanonicalVector::from_values(&[f32]) -> Result<CanonicalVector>`
- 产出:`CanonicalVector::score(&CanonicalVector) -> Result<f64>`
- 产出:`CanonicalVector::restore() -> Vec<f64>`
- 产出:`VectorMeta::{new, encode, decode, count, set_count, version, dimension, is_stale}`
- 产出:`VectorDataValue::{from_canonical, encode, decode}`
- 产出:`VectorQuery`、`VectorSearchMode`、`VectorSearchOptions`、`VectorHit`

- [x] **步骤 1:在导出模块之前先写编解码和数值单元测试**

将单元测试放在两个新文件中。覆盖以下精确的用例:

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

编解码测试必须独立地对空二进制 element 通过 `MemberDataKey` 往返、对二维 vector value 往返,以及对 `count=2`、`version=42`、`dimension=2` 的 `VectorMeta` 往返。

- [x] **步骤 2:运行测试并确认模块尚不存在**

运行:

```bash
cargo test -p storage vector::tests
cargo test -p storage format_vector::tests
```

预期:在模块和类型实现并导出之前编译失败。

- [x] **步骤 3:实现公开的 vector 类型**

在 `src/storage/src/vector.rs` 中定义如下精确的公开 API:

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

数值规则:

- FP32 blob 长度必须非零且能被四整除。
- 每个分量用 `f32::from_le_bytes` 解析;拒绝非有限值。
- 用 `f64` 累加 `norm²`;拒绝零或非有限的 norm。
- 归一化分量按 `f32` 存储,原始 L2 按 `f32` 存储。
- 用 `f64` 计算点积,将其钳制到 `[-1, 1]`,然后返回 `(dot + 1.0) / 2.0` 并钳制到 `[0, 1]`。
- 维度不同时拒绝分数比较。

- [x] **步骤 4:实现固定编解码器**

在 `src/storage/src/format_vector.rs` 中使用如下精确的 metadata 布局:

```text
| type=7 | count | version | format | encoding | metric | flags | dimension | zero reserve | ctime | etime |
|   1B   |  8B   |   8B    |   1B   |    1B    |   1B   |  1B   |   4B LE   |     8B      |  8B   |  8B   |
```

常量:

```rust
pub const VECTOR_META_FORMAT: u8 = 1;
pub const VECTOR_ENCODING_FP32_LE: u8 = 1;
pub const VECTOR_METRIC_COSINE: u8 = 1;
pub const VECTOR_VALUE_MAGIC: u8 = 0x56;
pub const VECTOR_VALUE_FORMAT: u8 = 1;
```

使用如下 value 布局:

```text
| magic=0x56 | format=1 | dimension | original_l2 | normalized FP32 payload |
|     1B     |    1B    |   4B LE   |   4B LE     |    dimension * 4B       |
```

解码器必须检查精确长度、magic、format、非零 dimension、有限且为正的 `original_l2`、有限的 payload 分量,以及 payload 维度。对格式错误的持久化字节返回 `InvalidFormatSnafu`;永不 panic。

- [x] **步骤 5:导出新模块并重新运行测试**

在 `src/storage/src/lib.rs` 中添加:

```rust
mod format_vector;
pub mod vector;

pub use vector::{
    CanonicalVector, VectorHit, VectorQuery, VectorSearchMode, VectorSearchOptions,
};
```

运行:

```bash
cargo test -p storage vector::tests
cargo test -p storage format_vector::tests
```

预期:所有 canonicalization 和编解码测试通过。

- [x] **步骤 6:提交编解码器切片**

```bash
git add src/storage/src/lib.rs src/storage/src/vector.rs src/storage/src/format_vector.rs
git commit -m "feat(storage): add vector codecs"
```

---

### 任务 3:实现原子的 VectorSet 变更和点读

**文件:**

- 新建:`src/storage/src/redis_vectors.rs`
- 修改:`src/storage/src/lib.rs`
- 新建:`src/storage/tests/redis_vector_test.rs`

**接口:**

- 消费:`CanonicalVector`、`VectorMeta`、`VectorDataValue`、`MemberDataKey`
- 产出:`Redis::vadd(key, element, vector) -> Result<bool>`
- 产出:`Redis::vrem(key, element) -> Result<bool>`
- 产出:`Redis::vcard(key) -> Result<u64>`
- 产出:`Redis::vdim(key) -> Result<u32>`
- 产出:`Redis::vemb(key, element) -> Result<Option<Vec<f64>>>`
- 产出:`Redis::vismember(key, element) -> Result<bool>`
- 产出:`Redis::is_cluster_mode() -> bool`

- [x] **步骤 1:编写 storage 行为测试**

使用 `unique_test_db_path`、`safe_cleanup_test_db`、`StorageOptions`、`BgTaskHandler` 和 `LockMgr` 创建测试文件,与现有 storage 测试的搭建方式保持一致。添加以下独立命名的测试:

- `test_vadd_create_update_and_dimension_guard`
- `test_vadd_is_binary_safe_and_accepts_empty_element`
- `test_vcard_vdim_vemb_and_vismember_missing_semantics`
- `test_vrem_deletes_last_member_and_meta`
- `test_vector_commands_return_wrongtype_for_string_key`
- `test_vector_meta_and_member_are_committed_together`

核心断言如下:

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

对于维度不匹配,断言 `is_err()` 并确认 `VCARD` 和旧成员值保持不变。对于最后一个成员的删除,断言 `VREM` 之后 `get_key_type(key)` 返回 key 不存在的错误。

- [x] **步骤 2:运行行为测试并确认方法缺失**

运行:

```bash
cargo test -p storage --test redis_vector_test
```

预期:编译失败,因为六个 `Redis` 方法尚未实现。

- [x] **步骤 3:为 VADD 实现一条加锁的读-改-写路径**

在 `src/storage/src/lib.rs` 中添加这个私有模块声明:

```rust
mod redis_vectors;
```

`Redis::vadd` 内部的实现顺序:

1. 使用 user key 获取 `ScopeRecordLock`。
3. 读取 `MetaCF[BaseMetaKey::new(key)]`。
4. meta 缺失或过期时创建 `VectorMeta::new_after(1, dimension, previous_generation)`,其 version 为钳制在前一代 generation 之上的当前时间戳(单调递增,因此重建的 VectorSet 永远不可能寻址到过期的 `VectorDataCF` 行),且不再二次递增。
5. 存活的非 VectorSet meta 返回与 `check_type_state` 相同的 WRONGTYPE 文本。
6. 存活的 VectorSet 要求维度相等。
7. 点读 `VectorDataCF[MemberDataKey(key, version, element)]` 以区分插入与更新。
8. 向存活的既有 VectorSet 插入时递增 count;创建新 VectorSet 时保持初始 count 为一;更新既有 element 时保持 count 不变。
9. 构建一个同时包含 member put 和 meta put 的 batch;提交一次。
10. 插入返回 `true`,更新返回 `false`。

公开签名必须是:

```rust
pub fn vadd(
    &self,
    key: &[u8],
    element: &[u8],
    vector: &CanonicalVector,
) -> Result<bool>;
```

- [x] **步骤 4:实现 VREM 和点读命令**

每个操作都使用 `VectorMeta::decode` 和相同的 `MemberDataKey` generation。

VREM 规则:

- key 缺失/过期或 element 缺失时返回 `Ok(false)`。
- 类型错误时返回 WRONGTYPE。
- count 大于一时,在一个 batch 中删除 member 并写入 `count - 1` 的 meta。
- count 等于一时,在一个 batch 中删除 member 并删除 MetaCF。

读取规则:

- `vcard`:缺失/过期返回 `0`。
- `vdim`:缺失/过期返回 `KeyNotFoundSnafu`。
- `vemb`:member 或 key 缺失/过期返回 `None`;存活值返回 `CanonicalVector::restore()`。
- `vismember`:member 或 key 缺失/过期返回 `false`。
- 所有存活的类型错误 key 都返回 WRONGTYPE。

- [x] **步骤 5:运行 storage 测试**

运行:

```bash
RUST_TEST_THREADS=1 cargo test -p storage --test redis_vector_test
cargo test -p storage vector::tests
cargo test -p storage format_vector::tests
```

预期:所有测试通过,包括维度不匹配后可见的回滚行为。

- [x] **步骤 6:提交点操作**

```bash
git add src/storage/src/lib.rs src/storage/src/redis_vectors.rs src/storage/tests/redis_vector_test.rs
git commit -m "feat(storage): persist vector set members"
```

---

### 任务 4:实现带稳定 Top-K 排序的精确 FLAT 搜索

**文件:**

- 修改:`src/storage/src/redis_vectors.rs`
- 修改:`src/storage/src/vector.rs`
- 修改:`src/storage/tests/redis_vector_test.rs`

**接口:**

- 消费:`VectorQuery`、`VectorSearchOptions`
- 产出:`Redis::vsim(key, query, options) -> Result<Vec<VectorHit>>`
- 排序约定:分数降序,然后按原始 element 字节升序
- 复杂度约定:`O(N × DIM + N log K)` 时间,`O(K + DIM)` 额外内存

- [x] **步骤 1:添加失败的 FLAT 搜索测试**

添加以下测试:

- `test_vsim_direct_vector_returns_exact_top_k`
- `test_vsim_ele_uses_stored_member_as_query`
- `test_vsim_stable_tie_breaks_by_raw_element_bytes`
- `test_vsim_truth_matches_approximate_in_phase_one`
- `test_vsim_missing_key_is_empty_and_missing_ele_is_error`
- `test_vsim_rejects_query_dimension_mismatch`

使用这个确定性数据集:

```rust
let x = CanonicalVector::from_values(&[1.0, 0.0]).expect("x");
let y = CanonicalVector::from_values(&[0.0, 1.0]).expect("y");
let neg_x = CanonicalVector::from_values(&[-1.0, 0.0]).expect("negative x");
redis.vadd(b"search", b"b", &y).expect("insert b");
redis.vadd(b"search", b"a", &y).expect("insert a");
redis.vadd(b"search", b"x", &x).expect("insert x");
redis.vadd(b"search", b"neg", &neg_x).expect("insert neg");
```

对于查询 `x` 和 `COUNT 3`,要求 `x` 在前,然后是 `a`,然后是 `b`;`a` 和 `b` 分数相同,由原始字节决定其顺序。

- [x] **步骤 2:运行聚焦测试并确认 VSim 缺失**

```bash
cargo test -p storage --test redis_vector_test test_vsim
```

预期:编译失败,因为 `Redis::vsim` 尚未定义。

- [x] **步骤 3:添加具有全序关系的堆元素**

定义一个私有的 `HeapHit`,将更差的命中比较为堆顶最大值,这样容量为 K 的堆可以替换其最差元素。相等性和排序必须同时使用 `score.total_cmp()` 和原始 element 字节;不得使用 `partial_cmp().unwrap()`。

排空堆之后要求的结果排序:

```rust
hits.sort_by(|left, right| {
    right
        .score
        .total_cmp(&left.score)
        .then_with(|| left.element.cmp(&right.element))
});
```

- [x] **步骤 4:实现一次 RocksDB 快照扫描**

精确的搜索顺序是:

1. key 缺失或过期时返回空 vector。
2. 存活的非 VectorSet key 返回 WRONGTYPE。
3. 解码 meta,并从同一个 RocksDB 快照中通过点读解析 `Element` 查询。
4. 拒绝缺失的查询 element 和维度不匹配。
5. 构建 `MemberDataKey::new(key, version, b"").encode_seek_key()`。
6. 使用快照读选项从该前缀开始向前迭代 `VectorDataCF`。
7. 在第一个不以该前缀开头的 key 处停止。
8. 剥掉前缀和 16 字节的后缀 reserve,恢复原始 element 字节。
9. 解码每个 vector value 并计算分数。
10. 堆中最多保留 `options.count` 个条目。
11. 排空并使用上述稳定排序。

在创建迭代器之前拒绝 `options.count == 0`。

- [x] **步骤 5:运行 FLAT 和回归测试**

```bash
RUST_TEST_THREADS=1 cargo test -p storage --test redis_vector_test test_vsim
RUST_TEST_THREADS=1 cargo test -p storage --test redis_set_test
```

预期:Vector 搜索测试通过,且现有 Set 前缀迭代测试保持绿色。

- [x] **步骤 6:提交搜索引擎**

```bash
git add src/storage/src/vector.rs src/storage/src/redis_vectors.rs src/storage/tests/redis_vector_test.rs
git commit -m "feat(storage): add flat vector similarity search"
```

---

### 任务 5:接通 Storage 路由、TYPE、TTL 清理、DEL 和 FLUSHDB

**文件:**

- 修改:`src/storage/src/storage_impl.rs`
- 修改:`src/storage/src/data_compaction_filter.rs`
- 修改:`src/storage/src/meta_compaction_filter.rs`
- 修改:`src/storage/src/redis.rs`
- 修改:`src/storage/src/redis_strings.rs`
- 修改:`src/storage/tests/redis_vector_test.rs`

**接口:**

- 产出:`Storage` 上相同的七个操作,每个都按 user key 恰好路由一次
- 保持:`TYPE key -> vectorset`
- 保持:通过通用 Meta envelope 的通用 EXPIRE/PERSIST 行为
- 保持:通用 DEL 和 FLUSHDB 物理删除 VectorDataCF 条目

- [x] **步骤 1:添加门面和生命周期测试**

使用 `Storage::new(3, 0)` 添加以下测试:

- `test_storage_routes_all_members_of_one_vectorset_to_one_instance`
- `test_type_returns_vectorset`
- `test_expired_vectorset_reads_as_missing`
- `test_del_removes_vector_meta_and_members`
- `test_flushdb_removes_vector_meta_and_members`
- `test_vector_storage_rejects_cluster_mode`

多 instance 测试必须计算 `key_to_slot_id(key)`,并确认只有被选中的 instance 包含该 MetaCF key;不得为 VSIM 做扫描或扇出。

- [x] **步骤 2:运行测试并确认 Storage 方法缺失**

```bash
cargo test -p storage --test redis_vector_test test_storage_routes
cargo test -p storage --test redis_vector_test test_del_removes_vector
```

预期:编译失败,因为 Vector 方法只存在于 `Redis` 上。

- [x] **步骤 3:添加精确的单 key 路由方法**

在 `src/storage/src/storage_impl.rs` 中添加以下签名:

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

每个方法都必须执行相同的两条路由语句,然后调用对应的 Redis 方法。例如,`Storage::vadd` 是:

```rust
let slot_id = key_to_slot_id(key);
let instance_id = self.slot_indexer.get_instance_id(slot_id);
self.insts[instance_id].vadd(key, element, vector)
```

对 `vsim`、`vrem`、`vcard`、`vdim`、`vemb` 和 `vismember` 应用相同的前缀,按声明的原样转发参数。没有 element 路由,也没有 instance 扇出。

- [x] **步骤 4:扩展通用生命周期处理**

应用以下精确的增补:

- `src/storage/src/redis.rs::is_stale_static`:在通用的 count/version meta 分支中包含 `DataType::VectorSet`。
- `src/storage/src/redis_strings.rs::set_key_etime`:用 `ParsedBaseMetaValue` 解析 `DataType::VectorSet`,与 Hash/Set/ZSet 完全一致。
- `src/storage/src/meta_compaction_filter.rs`:用 `ParsedBaseMetaValue` 解析 `DataType::VectorSet`。
- `src/storage/src/data_compaction_filter.rs::parse_meta_value` 及其仅用于测试的 meta 解析器:用 `ParsedBaseMetaValue` 解析 VectorSet。
- `Redis::del_key`:将 `ColumnFamilyIndex::VectorDataCF` 加入复合数据 CF 扫描列表。
- `Redis::flush_db`:将 `ColumnFamilyIndex::VectorDataCF` 加入 `all_cf_indexes`。

不要在上述 schema match 中引入通配符;每个 VectorSet/VectorDataCF 分支都必须显式写出。

- [x] **步骤 5:运行生命周期和现有 TTL 测试**

```bash
RUST_TEST_THREADS=1 cargo test -p storage --test redis_vector_test
RUST_TEST_THREADS=1 cargo test -p storage --test ttl_test
RUST_TEST_THREADS=1 cargo test -p storage --test redis_basic_test
```

预期:所有测试通过;过期的 vector 数据立即不可见并具备被 compaction 清理的资格,而 DEL/FLUSHDB 会移除物理的 VectorDataCF 条目。

- [x] **步骤 6:提交路由和生命周期支持**

```bash
git add src/storage/src/storage_impl.rs src/storage/src/data_compaction_filter.rs src/storage/src/meta_compaction_filter.rs src/storage/src/redis.rs src/storage/src/redis_strings.rs src/storage/tests/redis_vector_test.rs
git commit -m "feat(storage): route and clean up vector sets"
```

---

### 任务 6:添加 Vector Set 命令解析和命令表注册

**文件:**

- 新建:`src/cmd/src/vector.rs`
- 修改:`src/cmd/src/lib.rs`
- 修改:`src/cmd/src/table.rs`

**接口:**

- 产出:`VAddCmd`、`VSimCmd`、`VRemCmd`、`VCardCmd`、`VDimCmd`、`VEmbCmd`、`VIsMemberCmd`
- 消费:任务 5 中的全部七个 `Storage` API
- 产出:二进制安全的 argv 解析和 `RespData` 回复

- [x] **步骤 1:在新模块中编写解析器和元数据单元测试**

覆盖所有受支持的命令形态:

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

为以下情况添加显式的失败测试:FP32 长度错误、VALUES 维度/数量非法、浮点数非法、缺少 `NOQUANT`、`Q8`、`BIN`、重复的 VSIM 选项、COUNT 为零/非法、`VEMB RAW`,以及结尾出现未知选项。

元数据断言:

```rust
assert_eq!(VAddCmd::new().meta().arity, -5);
assert_eq!(VSimCmd::new().meta().arity, -4);
assert_eq!(VRemCmd::new().meta().arity, 3);
assert_eq!(VCardCmd::new().meta().arity, 2);
assert_eq!(VDimCmd::new().meta().arity, 2);
assert_eq!(VEmbCmd::new().meta().arity, -3);
assert_eq!(VIsMemberCmd::new().meta().arity, 3);
```

- [x] **步骤 2:运行命令测试并确认模块尚未注册**

```bash
cargo test -p cmd vector::tests
```

预期:在新模块、命令结构体和解析器存在之前编译失败。

- [x] **步骤 3:实现共享解析器,对 key/element/blob 不做 UTF-8 假设**

只有选项关键字和 VALUES 的数字 token 按 ASCII/UTF-8 解码。key、element 和 FP32 blob 保持为原始字节。

定义私有的解析结果形式:

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

错误字符串必须精确匹配,且已包含 Redis 错误类别前缀:

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

添加一个本地回复辅助函数,避免对现有 storage 错误字符串重复加前缀:

```rust
fn error_reply(message: impl Into<String>) -> RespData {
    RespData::Error(message.into().into())
}
```

将内部的非 Redis storage 失败映射为 `ERR storage error`;用 `log::error!` 记录详细错误。

- [x] **步骤 4:实现命令回复**

回复约定:

- VADD/VREM/VISMEMBER:整数 `1` 或 `0`。
- VCARD/VDIM:经检查的 `u64/u32 -> i64` 转换后的整数。
- VEMB:key/member 缺失时返回 null bulk;否则返回 `RespData::Double` 数组。
- 不带分数的 VSIM:bulk-string element 数组。
- 带分数的 VSIM:bulk-string element 到 `RespData::Double` 的 `RespData::Map`;RESP2 降级由任务 7 在编码器边界执行。

标志和 ACL 类别:

- VADD/VREM:`WRITE | FAST`,`KEYSPACE | WRITE`。
- VSIM:`READONLY`,`KEYSPACE | READ | SLOW`。
- VCARD/VDIM/VEMB/VISMEMBER:`READONLY | FAST`,`KEYSPACE | READ`。

- [x] **步骤 5:导出并注册全部七个命令**

在 `src/cmd/src/lib.rs` 中添加 `pub mod vector;`。在 `src/cmd/src/table.rs` 的 `register_cmd!` 中追加以下类型:

```rust
crate::vector::VAddCmd,
crate::vector::VSimCmd,
crate::vector::VRemCmd,
crate::vector::VCardCmd,
crate::vector::VDimCmd,
crate::vector::VEmbCmd,
crate::vector::VIsMemberCmd,
```

添加一个表测试,断言所有小写名称都能解析。

- [x] **步骤 6:运行命令和 runtime 分发测试**

```bash
cargo test -p cmd vector::tests
cargo test -p cmd table
cargo test -p runtime handle_execute_command
```

预期:所有测试通过,且通用的 `StorageCommand::Execute` 路径无需新增 runtime 消息变体即可找到每个 Vector 命令。

- [x] **步骤 7:提交命令层**

```bash
git add src/cmd/src/vector.rs src/cmd/src/lib.rs src/cmd/src/table.rs
git commit -m "feat(cmd): add redis vector set commands"
```

---

### 任务 7:让 VSIM WITHSCORES 在 RESP2 和 RESP3 下都正确

**文件:**

- 修改:`src/resp/src/encode.rs`
- 修改:`src/resp/src/negotiation.rs`

**接口:**

- 消费:命令层的 `RespData::Map` 和 `RespData::Double`
- 产出:RESP3 的 `%` map 与 `,` double
- 产出:RESP2 的扁平数组,分数为 bulk-string

- [x] **步骤 1:添加编码器回归测试**

添加在两种协议版本下编码以下响应的测试:

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

预期的 RESP3 字节:

```text
%2\r\n$1\r\na\r\n,1\r\n$1\r\nb\r\n,0.5\r\n
```

预期的 RESP2 字节:

```text
*4\r\n$1\r\na\r\n$1\r\n1\r\n$1\r\nb\r\n$3\r\n0.5\r\n
```

- [x] **步骤 2:运行测试并确认 RESP2 当前会输出 RESP3 类型**

```bash
cargo test -p resp encode_vsim_withscores
```

预期:RESP2 断言失败,因为 `RespEncoder` 目前只是存储了版本号,但在编码 Map/Double 时并未使用它。

- [x] **步骤 3:用版本感知分支直接编码**

递归 match 直接位于 trait 方法 `encode_resp_data` 中;版本差异在编码点处内联处理(null,以及经由 `append_map`/`append_set`/`append_push` 的 map/set/push 前缀):

```rust
fn encode_resp_data(&mut self, data: &RespData) -> &mut Self {
    match data {
        // ...
        RespData::BulkString(None) | RespData::Array(None) if self.is_resp3() => {
            self.append_null()
        }
        RespData::BulkString(None) => self.set_bulk_string_len(-1),
        RespData::Array(Some(array)) => {
            self.append_array_len(array.len() as i64);
            for item in array {
                self.encode_resp_data(item);
            }
            self
        }
        // ...
    }
}
```

所有嵌套的 array/map/set/push 递归都调用 `encode_resp_data` 自身;不存在单独的内部/归一化方法。

`ProtocolNegotiator::convert_to_resp2` 保持可用,并对 Array、Map、Set、Push、Null、Boolean、Double、BigNumber、BulkError 和 VerbatimString 递归。

- [x] **步骤 4:运行所有 RESP 测试**

```bash
cargo test -p resp
```

预期:所有 RESP 测试通过;现有 RESP3 HELLO 行为保持不变,且 RESP2 永不输出仅属于 RESP3 的前缀。

- [x] **步骤 5:提交协议感知编码**

```bash
git add src/resp/src/encode.rs src/resp/src/negotiation.rs
git commit -m "fix(resp): downgrade vector scores for resp2"
```

---

### 任务 8:添加端到端兼容性测试并运行 Kiwi 验证流水线

**文件:**

- 新建:`tests/python/test_vector_set_commands.py`
- 修改:`docs/superpowers/plans/2026-07-19-redis-vector-set.md`,仅在执行过程中勾选已完成的复选框

**接口:**

- 验证:经由 TCP 的命令解析、双 runtime 分发、storage 持久化、RESP2/RESP3 编码、TYPE/DEL/EXPIRE 交互
- 不消费:`tests/python/test_vector_basic.py`、FT.CREATE、FT.SEARCH、HSET vector 字段

- [x] **步骤 1:编写二进制安全的 Python 集成测试**

在新测试文件中使用以下代码创建本地 fixture:

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

添加以下测试:

- VALUES 创建/更新的返回值,以及维度不匹配。
- 通过 `struct.pack("<2f", 1.0, 0.0)` 构造的 FP32 little-endian blob。
- 包含 `b"\x00"` 的二进制 key 和 element;空 element。
- VCARD、VDIM、VEMB、VISMEMBER、VREM,以及最后一个成员删除后 key 被删除。
- VSIM 直接向量查询、ELE 查询、COUNT、WITHSCORES 和 TRUTH。
- 稳定的同分顺序。
- TYPE 返回 `b"vectorset"`。
- 对既有 string key 的 WRONGTYPE。
- 每个读命令的缺失 key 语义。
- EXPIRE 使 key 立即表现为缺失。
- DEL 移除成员,并允许以新维度重建同名 key。
- 缺少 NOQUANT、Q8、BIN、RAW 以及格式错误输入的报错。

对于 WITHSCORES,断言 redis-py 在 RESP2 下返回 list、在 RESP3 下返回 dict,且 element 顺序和数值分数相同。

- [x] **步骤 2:启动 standalone Kiwi 并只运行 Vector 集成测试**

终端 1:

```bash
make standalone
```

终端 2:

```bash
make -C tests install-deps
KIWI_PORT=7379 pytest -q tests/python/test_vector_set_commands.py
```

预期:所有 RESP2 和 RESP3 参数化用例通过。

- [x] **步骤 3:运行聚焦的 Rust 回归套件**

```bash
RUST_TEST_THREADS=1 cargo test -p storage --test redis_vector_test
RUST_TEST_THREADS=1 cargo test -p storage --test redis_hash_test
RUST_TEST_THREADS=1 cargo test -p storage --test redis_set_test
RUST_TEST_THREADS=1 cargo test -p storage --test ttl_test
cargo test -p cmd vector::tests
cargo test -p resp
```

预期:所有命令退出码为 0。

- [x] **步骤 4:运行仓库标准验证**

严格按以下顺序运行:

```bash
make fmt
make lint
make build
make test
```

预期:所有命令退出码为 0。首次 RocksDB 构建可能耗时较长;保留 sccache,不要在仓库范围内禁用它。

- [x] **步骤 5:检查最终 diff 的范围和生成物**

```bash
git status --short
git diff --stat origin/main...HEAD
git diff --check origin/main...HEAD
```

预期:

- 没有改动 `tests/python/test_vector_basic.py`。
- 没有 FT.CREATE/FT.SEARCH 实现。
- 没有 HNSW、Raft mutation、snapshot 或 O(1) 删除代码。
- 没有 `__pycache__`、RocksDB 数据目录、构建产物或临时笔记被加入暂存区。
- `git diff --check` 无输出。

- [x] **步骤 6:提交端到端测试**

```bash
git add tests/python/test_vector_set_commands.py docs/superpowers/plans/2026-07-19-redis-vector-set.md
git commit -m "test: cover redis vector set commands"
```

---

## 完成标准

只有当以下所有陈述都为真时,Phase 1 实现才算完成:

- 一个 VectorSet key 及其全部 element 只存在于由 user key 唯一选定的一个 RocksDB instance 中。
- Meta 和 member 的变更在一个 batch 中提交;校验失败时两者都保持不变。
- `TYPE` 报告 `vectorset`;通用的 TTL、DEL 和 FLUSHDB 语义正常工作。
- VADD 只在显式指定 NOQUANT 时接受 FP32 和 VALUES,且对 element 二进制安全。
- VSIM 返回精确、确定性的 Top-K 结果,且不收集并排序所有命中。
- RESP2 和 RESP3 的 WITHSCORES 回复使用各自原生兼容的形态。
- Raft Group cluster 模式支持所有七个命令的写入和读取(通过 binlog 复制到 follower);Redis Cluster 槽位路由不在本版范围内。
- 聚焦的 Rust/Python 测试和完整的 Kiwi 验证流水线全部通过。
- 最终 diff 不包含 FT 原型抽取、HNSW、Raft/snapshot 设计实现或无关清理。

## 推迟的后续计划

在实现以下任何独立项目之前,先创建单独的、经过评审的设计和计划:

1. Redis Cluster 槽位路由与跨 slot 查询。
2. Redis 兼容的 VINFO 和 INFO VECTOR 可观测性。
3. Raft 逻辑的 VectorSet 变更、领导者线性化读取和快照。
4. O(1) generation 删除与后台 compaction 清理。
5. HNSW 派生索引、重建/恢复以及滚动升级能力门禁。
