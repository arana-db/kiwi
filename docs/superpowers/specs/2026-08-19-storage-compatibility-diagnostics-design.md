# Storage 兼容拒绝诊断契约设计

## 文档状态

- 日期：2026-08-19
- 状态：用户已批准，待实施与独立复审
- 仓库：`arana-db/kiwi`
- 实施基线：`main@9a8a64aca12a825912f299450e10fc6043eca610`
- 目标 Issue：[Issue #342](https://github.com/arana-db/kiwi/issues/342)
- Related implementation：[PR #422](https://github.com/arana-db/kiwi/pull/422)

PR #422 已实现 Root/Instance StorageManifest v2、已知 Base-v1 与 Vector-v1 staged
migration、恢复/回滚和未来/无效格式的 fail-closed 门禁。Issue #342 的剩余硬性
验收缺口是：从生产 `Storage::open` 拒绝已有目录时，错误没有统一、稳定地同时给出
当前格式、落盘格式和可执行处理方式。

本文只补诊断合同，不改变任何可接受格式、迁移状态机、回滚窗口、磁盘字节、目录
切换或 RocksDB 打开顺序。

## 1. 问题

当前兼容拒绝由 `storage_manifest.rs` 和 `storage_migration.rs` 的多个
`InvalidFormat`/RocksDB 错误直接向上传播。详细 cause 通常准确，但调用者无法稳定
解析以下三个运维问题：

```text
current=<当前二进制支持的存储合同>
on_disk=<本次被拒绝目录的可观察格式>
action=<不会破坏原目录的下一步>
```

现有测试主要断言内部 parser 或 migration helper 的局部 substring；它们不能证明
server 实际调用的 `Storage::open` 在 RocksDB 对外服务前返回完整诊断，也不能防止
以后某条新增拒绝路径漏掉字段。

## 2. 目标与非目标

### 2.1 目标

- 为 `Storage::open` admission 阶段的每个格式/兼容性拒绝增加统一 envelope，固定
  `current=...; on_disk=...; action=...; cause=...` 四部分。
- `current` 来自编译期 StorageManifest/Schema 常量，不能由错误字符串猜测。
- `on_disk` 来自失败时对目标根目录的只读、有界观察；即使 manifest 损坏、来自未来
  版本或缺失，也必须返回确定 descriptor，而不是产生第二个错误覆盖原 cause。
- `action` 只建议仓库真实支持的安全操作：使用匹配版本、运行已有 staged migration、
  恢复兼容备份或先离线检查；不得承诺不存在的在线自动修复或 CLI。
- 保留原始 cause 文本，方便定位 manifest、topology、CF、comparator 或 migration
  phase 的精确拒绝原因。
- 通过真实 `Storage::open` 回归测试证明拒绝发生在 state publication 之前，目录没有
  被静默采用。

### 2.2 非目标

- 不新增 StorageManifest 字段、版本或文件。
- 不改变 Base-v1 / Vector-v1 的已知迁移支持矩阵。
- 不允许未来版本、未知 CF、损坏 digest、topology mismatch 或非法 migration journal
  继续打开。
- 不为所有历史开发版本新增迁移器，也不新增 `adopt`/`repair` 命令。
- 不把普通运行时 I/O、后台任务或业务数据错误错误标记为格式兼容拒绝。
- 不修改 `Redis::open` 的单实例内部测试入口来代替生产 `Storage::open` 证据。

## 3. 诊断格式

统一字符串使用单行、稳定 key 顺序：

```text
storage compatibility refusal: current=<descriptor>; on_disk=<descriptor>; action=<instruction>; cause=<original error>
```

字段值必须经过稳定的单行编码：至少编码 `%`、`;`、`=`、CR、LF 和控制字符；每个
字段有固定最大长度和显式截断标记，防止路径或 RocksDB 文本伪造第二个字段、破坏
日志行或无限放大。`InvalidFormat` cause 保留原始 message；strict RocksDB open 的
cause 必须使用内层 `rocksdb::Error::into_string()`，不得使用只显示 `RocksDB error`
的外层 Kiwi `Error::Rocks`。

### 3.1 current descriptor

`current` 至少包含：

- Root manifest version；
- storage schema version；
- Instance manifest version；
- canonical CF schema 身份或版本。

它由现有常量组合，固定为类似：

```text
root-manifest-v2/storage-schema-v2/instance-manifest-v2/slot-mapping-v1/cf-contract=storage-schema-v2
```

不得复制魔法数字，也不得发明没有持久化常量的 `cf-schema-current`；CF contract 复用
`CANONICAL_COLUMN_FAMILIES` 和 storage schema v2。常量变化时诊断和 test fixture
必须同时变化。

### 3.2 on_disk descriptor

失败后只读观察按以下优先级生成：

1. 根目录不存在或为空：`empty`；这种情况通常不属于 compatibility refusal。
2. Root manifest 存在且可安全提取版本字段：记录
   `root-manifest-vX/storage-schema-vY`；如有 migration journal，再附 source profile、
   phase 和 current instance；不要求 manifest 整体验证成功。
3. Root manifest 存在但不是受支持、可解析的 bounded JSON：
   `root-manifest-present-unreadable`。
4. Root manifest 缺失但存在实例/legacy 内容：`legacy-without-root-manifest`。
5. 目录本身无法安全观察：`unavailable`，并保留原 cause。

对于 strict RocksDB comparator/CF mismatch，descriptor 还附
`rocksdb-strict-open=invalid-argument`；具体 comparator 名称保存在 bounded cause。
Root manifest 的 canonical CF contract mismatch 必须在 cause 中给出第一个 mismatch
的 CF index/name、field、current/expected 和 on-disk/actual，不能继续只报泛化的
`canonical column-family contract mismatch`。

观察器必须是 fail-safe：只读、held handle、最多读取 `MAX+1` 字节；Root manifest
必须是 regular file。Unix 使用 nofollow/nonblock 语义，Windows 拒绝 reparse point；
打开后复核 handle metadata，FIFO/socket/device/directory 均直接降级为 unreadable/
unavailable。观察器不打开 RocksDB、不写 manifest、不创建目录。观察失败只能降级
descriptor，不能覆盖导致 `Storage::open` 失败的原错误。

### 3.3 action

最小稳定 action 按 descriptor/cause 分类：

- registered legacy：保持目录不变并用当前 Kiwi 重试；当前启动本身会在 admission 前
  执行 staged migration，若仍失败则按 cause 修复精确 registered-profile 问题。
- unregistered/corrupt legacy：用真正能够打开源格式的二进制做逻辑导出，在空的
  current-v2 storage 中导入；不得手写 manifest 或静默 adopt。
- future/unsupported version：使用能够读取该版本的 Kiwi，或从兼容备份恢复；不要用
  当前二进制修改目录。
- corrupt/unreadable/unknown layout：离线检查并从已验证备份恢复；不要静默初始化。
- runtime topology mismatch：使用匹配的 `db_instance_num`，或通过受支持的离线迁移
  产生匹配目录。
- 其他兼容拒绝：保留目录，依据 cause 选择兼容版本、已有 staged migration 或备份
  恢复。

文案可以共享 helper，但不能宣称系统拥有实际不存在的自动修复入口。

## 4. 注入边界

诊断只在 `Storage::open` 的 admission 路径注入，覆盖：

1. `prepare_or_resume_migration`；
2. `load_or_create_root_manifest`；
3. `validate_existing_instance_manifests`；
4. instances 尚未发布前的严格 RocksDB/manifest binding open；
5. runtime finalize migration 后的第二次 root/instance 验证与 reopen。

五个 admission step 返回的所有 `InvalidFormat` 进入统一 envelope，避免靠 cause
substring 猜测类型。strict RocksDB open 必须先在两个真实调用点分类：

- `redis.rs` 的 existing-v2 instance open：仅当 `allow_manifest_creation=false` 且
  `ErrorKind::InvalidArgument` 时转换成保留内层 source 的 `InvalidFormat`；
- `storage_migration.rs::open_instance_strict`：existing legacy 的
  `ErrorKind::InvalidArgument` 同样转换；
- `IOError`、`Busy`、`TimedOut`、`Corruption` 等保持原 `Error::Rocks`；`DB::list_cf`、
  iterator、copy 等非 strict-open Rocks 错误也不转换。

随后 `Storage::open` admission wrapper 只包装 `InvalidFormat`。普通 I/O 等非兼容错误
保持原 error category。`self.insts`、`self.db_path`、background task 和 expiration
manager 的发布顺序不变。

内部 parser/migration helper 保持现有详细错误，避免把 envelope 重复嵌套。只有公开
生产 admission 边界负责一次包装。

## 5. TDD 与变异证明

至少增加以下真实入口测试，均调用 `Storage::open`：

1. future Root 和 Instance manifest version：修改版本后刷新 canonical digest，再通过
   `Storage::open` 要求四字段；
2. configured topology 与 on-disk Root 不匹配：要求 current/on_disk/action/cause；
3. Root canonical CF name/role/comparator/key-codec/value-codec mismatch：逐项刷新 digest，
   要求 cause 给出首个 actual/expected mismatch；
4. unregistered legacy CF layout 或缺失/非法 legacy manifest：要求 safe action 且打开
   后 Storage 未发布实例；
5. 损坏 Root manifest：要求 `on_disk=root-manifest-present-unreadable` 并保留 digest/
   JSON cause；
6. current-v2 persisted comparator mismatch：合法 v2 manifests + canonical CF names，
   但 `list_data_cf` 使用错误持久化 comparator，必须覆盖 `redis.rs` strict open；
7. Base-v1 persisted comparator mismatch：六 CF legacy 且同样错误 comparator，必须在
   migration journal 写入前覆盖 `open_instance_strict`；
8. 合法 v2 DB 保持 LOCK/Busy：仍返回原 Rocks error，不能出现 compatibility envelope；
9. nonregular/symlink/FIFO/超大 manifest descriptor 有界失败；
10. 正向 empty/current/known migration fixture 继续成功，证明没有扩大拒绝范围。

RED 必须在基线实现上因缺少字段失败，而不是 fixture、RocksDB lock 或路径清理失败。
mutant 至少分别删除字段、双重包装、绕过任一 admission step、只修 v2 strict open、
只修 legacy strict open、退回外层 `RocksDB error`、包装所有 Rocks kinds、future fixture
不刷新 digest、observer 跟随 symlink/阻塞 FIFO/无读取上限、action 解析 cause
substring，以及字段注入 `; current=`/换行；每个 mutant 都必须被权威测试拒绝。

## 6. Issue 关闭顺序

1. 按 RED/GREEN 实施最小代码和测试。
2. 运行 targeted Storage tests、format、Clippy、diff check，并通过独立规格与质量/
   Test Guard 复审。
3. 提交、push、创建独立 PR；等待 exact Head checks 成功后合并。
4. 等待合并后新 main 的 CI 成功，并在 exact-main 重跑最强相关测试或确认对应 CI job。
5. 在 #342 评论九项验收映射、PR、merge SHA、exact-main run 和诊断示例，然后以
   completed 关闭。

任何一步未完成都保持 #342 OPEN。不得因为 PR #422 已合并而跳过本诊断 PR 的
exact-main 证据。

## 7. 验收标准

- 每个生产 `Storage::open` compatibility refusal 都恰好包含一次 `current=`、
  `on_disk=`、`action=` 和 `cause=`。
- future、corrupt、unknown legacy、topology mismatch 至少四种真实目录 fixture 通过。
- 原错误 cause 保留；非兼容 I/O 错误不被错误包装。
- 已知 migration、rollback/finalize 状态机和磁盘格式无行为变化。
- 失败打开不会发布 instances、db path 或 background tasks。
- 独立规格复审与质量/Test Guard 复审均为 P0=0/P1=0/P2=0。
- PR exact Head 与合并后的 exact main 验证成功后才关闭 #342。
