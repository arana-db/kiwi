# VectorSet Storage、Snapshot 与 VSIM 恢复实施计划

> **面向 AI 代理的工作者：** 必需子技能：使用 superpowers:subagent-driven-development 逐任务执行，每个生产修改前使用 superpowers:test-driven-development，每个提交前使用 superpowers:verification-before-completion。步骤使用复选框（`- [ ]`）语法跟踪进度。

**目标：** 建立可中断恢复的 Base 六 CF→七 CF 升级、已合并 Vector-v1 七 CF manifest v1→v2 升级及对应 source rollback，支持已知 Base v1 / Head v2 snapshot，把 snapshot install 改为可继续/可回退的 marker 状态机，用全量 Vector meta/member 校验取代 64 条抽样，并使 VSIM 结果对应一个 key-scoped 合法串行时刻。

**架构：** Root manifest 是 topology、CF schema、migration 和 rollback 的唯一权威；instance manifest 只保存 instance identity、root digest、incarnation 和 generation allocator。Live open 不再自动创建 CF；只有已分类的 sibling shadow 迁移路径可以创建 `VectorDataCF`。Snapshot 在 stage 中先分类、迁移、全量验证和 close/reopen，再进入 pause/install。

**技术栈：** Rust、RocksDB checkpoint/CF APIs、serde JSON、SHA-256、UUID、OpenRaft snapshot state machine、Tokio、PowerShell/WSL Bash、pytest raw RESP fixture。

---

## 固定合同

- Root manifest 文件名：`__kiwi_root_storage_manifest`；instance manifest 文件名保持 `__kiwi_storage_manifest`。
- Manifest v2 使用固定字段顺序的 Rust struct 序列化为 compact JSON；对不含 `digest` 的 bytes 计算 SHA-256，保存 lowercase hex。
- 路径字段只允许单个相对 basename：非空、不是 `.`/`..`、不含 `/` 或 `\`、不是绝对路径。
- 目录 identity 是 `root_manifest_id + migration_transaction_id + instance_id + manifest_digest`，不依赖 filesystem inode/file ID。
- canonical CF registry 是唯一来源，固定 stable ID 0..6、name、role、comparator、key codec、value codec 和 snapshot read/write version。
- Base compatibility ref 固定为 `688d905fec31b54aec76f36676f55efd8b5cfa17`；Head 验收使用当前聚合 PR exact Head。
- 已合并 Vector-v1 compatibility ref 固定为 `733888fc90ad8ef039947e87b08d7500a405954a`；它产生七 CF、每实例 manifest v1、无 Root manifest 的已知 source profile。
- `data_revision` 在 restore 校验中只验证格式、非零和操作后单调性；不从未携带 revision 的 member record 伪造可重算合同。
- `RollbackWindowClosed` 在 server 开放 network admission 之前持久化；进入该 phase 后禁止自动恢复 Base backup。
- `RaftMetadataPersisted` 必须在 current snapshot metadata/data 与 applied/membership durable state 持久化并 reopen 复验后才写入 marker。

## Task 1：唯一 CF registry 与 Root/Instance Manifest v2

**Requirement：** `REQ-STORAGE-001`、`REQ-STORAGE-002`、`REQ-STORAGE-004`、`REQ-VECTOR-001`

**文件：**

- Create: `src/storage/src/storage_schema.rs`
- Modify: `src/storage/src/storage_manifest.rs`
- Modify: `src/storage/src/redis.rs`
- Modify: `src/storage/src/storage.rs`
- Modify: `src/storage/src/checkpoint.rs`
- Modify: `src/storage/src/batch.rs`
- Modify: `src/storage/src/logindex/types.rs`
- Modify: `src/storage/src/redis_strings.rs`
- Modify: `src/storage/src/lib.rs`
- Modify: `src/raft/src/lib.rs`
- Modify: `src/storage/Cargo.toml`
- Modify: `Cargo.toml`
- Modify: `Cargo.lock`
- Create: `src/storage/tests/storage_manifest_v2_test.rs`
- Modify: `src/storage/tests/redis_basic_test.rs`

- [ ] **步骤 1：写入失败测试**

  新增并单独运行：

  - `root_manifest_roundtrips_topology_and_canonical_cf_contract`
  - `instance_manifest_binds_instance_to_root_manifest_digest`
  - `root_manifest_rejects_unknown_version_or_corrupt_digest`
  - `instance_manifest_rejects_wrong_instance_id_or_root_digest`
  - `root_manifest_rejects_absolute_parent_or_nested_migration_paths`
  - `storage_open_rejects_instance_count_or_slot_mapping_mismatch`
  - `canonical_cf_registry_matches_every_column_family_index_variant`
  - `raft_logindex_flush_and_checkpoint_consumers_match_canonical_registry`
  - `column_family_handle_and_binlog_wire_indices_use_registry_stable_ids`
  - `on_binlog_write_rejects_noncanonical_binlogs_before_commit`

  ```powershell
  cargo test -p storage --test storage_manifest_v2_test -- --nocapture
  ```

  预期 RED：缺少 root manifest、digest 和 canonical registry API。

- [ ] **步骤 2：实现 schema 与 manifest**

  - 在 workspace 和 storage crate 增加 `sha2 = "0.10"`，用 `format!("{byte:02x}")` 生成 lowercase hex，不增加第二个 hex crate。
  - `storage_schema.rs` 定义 `ColumnFamilySpec`、`ColumnFamilyRole`、`ComparatorId`、`CANONICAL_COLUMN_FAMILIES`。
  - `storage_manifest.rs` 定义 `RootStorageManifestV2`、`InstanceStorageManifestV2`、`MigrationTransaction`、`MigrationPhase`、`ManifestDigest`，保留 instance generation allocator。
  - `Redis::open`、`create_cf_options`、checkpoint、batch handle、binlog validate/apply、logindex、flush、Raft CF export 全部消费 registry，删除重复 CF 数组和独立 wire ID 数字 match。
  - `Storage::open` 在创建 Redis handle 或后台任务前验证 root/instance pairing。

- [ ] **步骤 3：回归与变异检查**

  ```powershell
  cargo test -p storage --test storage_manifest_v2_test -- --nocapture
  cargo test -p storage --test redis_basic_test -- --nocapture
  cargo test -p storage storage_manifest --lib -- --nocapture
  cargo test -p raft cf_names_match_storage_indices -- --nocapture
  ```

  删除 registry 中 `VectorDataCF`、调换 stable ID 或替换 root digest 时对应测试必须失败。

## Task 2：Base 六 CF 与 Vector-v1 七 CF staged migration、retry 与 source rollback

**Requirement：** `REQ-STORAGE-003`、`REQ-STORAGE-005`、`REQ-VECTOR-001`

**文件：**

- Create: `src/storage/src/storage_migration.rs`
- Modify: `src/storage/src/storage.rs`
- Modify: `src/storage/src/redis.rs`
- Modify: `src/storage/src/options.rs`
- Modify: `src/storage/src/lib.rs`
- Modify: `src/server/src/main.rs`
- Create: `src/storage/tests/storage_migration_test.rs`
- Create: `src/storage/tests/support/mod.rs`
- Create: `src/storage/tests/support/legacy_storage.rs`
- Create: `src/storage/tests/support/vector_v1_storage.rs`
- Modify: `src/storage/tests/redis_vector_test.rs`

- [ ] **步骤 1：对每个 durable phase 写入失败测试**

  新增：

  - `legacy_six_cf_storage_migrates_all_instances_and_reopens`
  - `vector_v1_seven_cf_storage_preserves_vector_data_incarnation_and_generation`
  - `unknown_cf_partial_manifest_or_mixed_v1_v2_fails_before_shadow_creation`
  - `migration_retries_after_source_detected_for_each_registered_profile`
  - `migration_retries_after_shadow_prepared`
  - `migration_retries_after_each_instance_copied`
  - `migration_retries_after_vector_cf_created_before_instance_manifest`
  - `migration_retries_after_each_instance_upgraded`
  - `migration_retries_after_all_instances_verified`
  - `migration_retries_after_switch_prepared`
  - `migration_retries_after_old_moved_to_backup`
  - `migration_retries_after_shadow_promoted`
  - `migration_retries_after_new_storage_opened`
  - `migration_retries_after_committed_before_rollback_window_closed`
  - `pre_admission_failure_restores_verified_base_backup`
  - `rollback_window_closed_rejects_automatic_backup_restore`
  - `migration_never_removes_the_only_verified_copy`

  ```powershell
  cargo test -p storage --features test-fault-injection --test storage_migration_test -- --nocapture
  ```

  每个测试同时断言 source profile、journal phase、source/shadow/backup basename、root/instance digest、实际 CF 集合和 String/Hash/ZSet/TTL 用户数据。`VectorSetV1SevenCf` profile 还必须断言 Vector meta/member、`storage_incarnation` 和 `next_generation` 保持不变。

- [ ] **步骤 2：实现 migration 状态机**

  `storage_migration.rs` 定义 `MigrationSourceProfile::{BaseV1SixCf, VectorSetV1SevenCf}`、`StorageMigration`、`MigrationLayout`、`MigrationFaultPoint`、`classify_storage_root`、`prepare_or_resume_migration`、`verify_shadow_instances`、`promote_shadow_instances`、`recover_or_rollback_before_admission`、`close_rollback_window`。Root journal 必须持久化 source profile，使后续 phase 和 rollback 不依赖目录猜测。

  Durable phase 必须与设计一致：

  ```text
  SourceDetected
  -> ShadowPrepared
  -> InstanceCopied(i)
  -> InstanceUpgraded(i)
  -> AllInstancesVerified
  -> SwitchPrepared
  -> OldMovedToBackup(i)
  -> ShadowPromoted(i)
  -> NewStorageOpened
  -> Committed
  -> RollbackWindowClosed
  ```

  两个 source profile 都必须执行完整 phase 矩阵。`InstanceCopied(i)` 和 `InstanceUpgraded(i)` 测试至少使用两个 instance，逐个参数化注入失败，证明恢复不会跳过前一 instance 或重复发布后一 instance。`Committed` 后、`RollbackWindowClosed` 前崩溃必须仍能识别无客户端新写入的 backup 是可验证回退点，并使用与 source profile 对应的 exact old binary reopen/read。

  - `StorageOptions::default` 关闭 `create_missing_column_families`。
  - `Redis` 提供按已分类 CF 严格 reopen 的 helper；只有 `BaseV1SixCf` shadow migration 创建 `VectorDataCF`，`VectorSetV1SevenCf` 必须复制并验证已有 Vector CF，保留 v1 manifest 中的 incarnation/generation，再写绑定 Root digest 的 Instance v2。
  - 切换使用 source→backup、shadow→source，每个 rename 和 phase 文件都执行可用的 file/directory sync。
  - server 只在 storage 完成 reopen 和验证后调用 `close_rollback_window`，随后才开放 network admission。

- [ ] **步骤 3：回归**

  ```powershell
  cargo test -p storage --features test-fault-injection --test storage_migration_test -- --nocapture
  cargo test -p storage --features test-fault-injection --test redis_vector_test legacy -- --nocapture
  cargo test -p storage --features test-fault-injection --test redis_vector_test vector_v1_manifest -- --nocapture
  cargo test -p storage --features test-fault-injection --test redis_vector_test manifest -- --nocapture
  cargo test -p server -- --nocapture
  ```

## Task 3：Base v1 / Head v2 staged snapshot restore

**Requirement：** `REQ-RAFT-002`、`REQ-STORAGE-006`、`REQ-VECTOR-002`

**文件：**

- Modify: `src/storage/src/checkpoint.rs`
- Modify: `src/storage/src/storage.rs`
- Modify: `src/storage/src/redis.rs`
- Modify: `src/storage/src/storage_migration.rs`
- Modify: `src/storage/src/lib.rs`
- Modify: `src/raft/src/state_machine.rs`
- Modify: `src/storage/tests/checkpoint_test.rs`
- Create: `src/raft/tests/snapshot_compatibility_test.rs`
- Create: `src/raft/tests/support/mod.rs`
- Create: `src/raft/tests/support/base_v1_snapshot.rs`

- [ ] **步骤 1：写入 version/digest/stage 失败测试**

  - 用 `snapshot_meta_classifies_known_base_v1` 取代现有 `test_snapshot_meta_rejects_v1`。
  - 新增 `snapshot_meta_rejects_unknown_future_version`。
  - 新增 `base_v1_snapshot_restores_string_hash_zset_and_ttl_after_stage_migration`。
  - 新增 `base_v1_snapshot_restore_generates_new_root_and_instance_incarnations`。
  - 新增 `base_v1_snapshot_with_vector_data_cf_fails_before_pause`。
  - 新增 `base_v1_snapshot_with_vector_meta_fails_before_pause`。
  - 新增 `base_v1_snapshot_with_unknown_cf_fails_before_pause`。
  - 新增 `v2_snapshot_requires_exact_root_manifest_digest`。
  - 新增 `v2_snapshot_requires_every_instance_manifest_digest_and_incarnation`。
  - 新增 `restore_lists_actual_cfs_before_opening_staged_rocksdb`。
  - 新增 `staged_storage_is_closed_reopened_and_revalidated_before_install`。

  ```powershell
  cargo test -p storage --test checkpoint_test snapshot_meta -- --nocapture
  cargo test -p raft --test snapshot_compatibility_test -- --nocapture
  ```

- [ ] **步骤 2：实现已知版本分类和 stage-only migration**

  - 定义 `ParsedSnapshotMeta::{LegacyV1, CurrentV2}`。
  - v1 只在实际 CF 为已知六 CF、无 Vector meta/member 时进入 stage migration。
  - v2 metadata 增加 root manifest ID/digest 和按 instance ID 排序的 instance digest/incarnation。
  - `prepare_checkpoint_restore` 接收已分类 metadata，在严格 CF 列表校验之前不调用 live `Storage::open`。
  - checkpoint 必须包含 root/instance manifests，metadata 与文件 digest 互相对账。

- [ ] **步骤 3：回归**

  ```powershell
  cargo test -p storage --test checkpoint_test snapshot_meta -- --nocapture
  cargo test -p raft --test snapshot_compatibility_test -- --nocapture
  cargo test -p raft --test snapshot_roundtrip_test snapshot_incarnation -- --nocapture
  ```

## Task 4：SnapshotInstallMarker v2 可恢复状态机

**Requirement：** `REQ-RAFT-005`、`REQ-RAFT-008`、`REQ-VECTOR-002`

**文件：**

- Create: `src/raft/src/snapshot_install.rs`
- Modify: `src/raft/src/lib.rs`
- Modify: `src/raft/src/state_machine.rs`
- Modify: `src/raft/src/node.rs`
- Modify: `src/storage/src/checkpoint.rs`
- Modify: `src/server/src/main.rs`
- Create: `src/raft/tests/snapshot_install_recovery_test.rs`
- Modify: `src/raft/tests/snapshot_roundtrip_test.rs`

- [x] **步骤 1：写入 restart/fault 失败测试**

  - `marker_rejects_absolute_or_non_basename_paths`
  - `restart_resumes_from_staged_validated`
  - `abandoning_partial_staged_install_removes_intent_before_cleanup`
  - `restart_resumes_after_storage_paused_before_marker_persisted`
  - `storage_paused_closes_live_handles_and_keeps_network_admission_closed`
  - `restart_resumes_from_marker_persisted`
  - `restart_restores_backup_after_old_renamed_before_new_promoted`
  - `rollback_pending_survives_restart_after_backup_restore`
  - `rollback_cleanup_pending_survives_partial_cleanup_restart`
  - `restart_resumes_after_new_renamed_to_target`
  - `restart_reopens_and_verifies_new_storage_before_publication`
  - `restart_persists_raft_metadata_before_cleanup`
  - `cleanup_pending_survives_restart_and_completes_idempotently`
  - `cleanup_pending_survives_partial_backup_deletion_restart`
  - `target_and_backup_both_present_with_digest_mismatch_fail_closed`
  - `marker_snapshot_metadata_digest_mismatch_fail_closed`
  - `marker_root_or_instance_manifest_digest_mismatch_fail_closed`
  - `cleanup_never_runs_before_new_storage_raft_metadata_and_current_snapshot_are_durable`

  ```powershell
  cargo test -p raft --test snapshot_install_recovery_test -- --nocapture
  ```

- [x] **步骤 2：实现 marker 编排和恢复决策**

  `snapshot_install.rs` 定义 `SnapshotInstallPhase`、`SnapshotInstallMarkerV2`、`SnapshotInstallLayout`、`SnapshotInstallRecoveryDecision`、`persist_phase`、`recover_snapshot_install`、`validate_install_layout_and_digests`、`complete_pending_cleanup`。

  phase 固定为：

  ```text
  StagedValidated
  -> StoragePaused
  -> MarkerPersisted
  -> OldRenamedToBackup
  -> NewRenamedToTarget
  -> NewStorageReopened
  -> RaftMetadataPersisted
  -> CleanupPending
  -> Complete
  ```

  在 `MarkerPersisted` 或 `OldRenamedToBackup` 尚未完成新库 promotion 时，恢复分支固定为：

  ```text
  MarkerPersisted | OldRenamedToBackup
  -> RollbackPending
  -> RollbackCleanupPending
  -> marker removed
  ```

  **计划偏差：** 原计划只持久化正向安装阶段，但回滚包含 `backup -> target`、删除 stage/pending 文件和删除 marker 等多次独立落盘操作；若回滚本身在任一步骤后崩溃，旧 phase 无法唯一解释磁盘布局。为满足同一 fail-closed/restart-safe 要求，回滚也必须先持久化 `RollbackPending`，恢复旧库后持久化 `RollbackCleanupPending`，再执行可重入清理。

  `StagedValidated` 放弃路径尚未触碰 live target，因此必须先持久删除 marker、再清理 disposable stage/pending 文件；清理中途崩溃只允许留下无权威性的孤儿文件，不能留下指向残缺 stage 的恢复意图。正向和回滚 cleanup phase 在进入前完成副本 identity 校验，phase 持久化后只复验权威 target 和待删路径类型，以允许递归删除中途崩溃后的可重入清理。server preflight 同时要求 marker 的 `db_instance_num` 与启动配置一致，任何配置漂移必须在 rename/cleanup 之前失败。

  `StoragePaused` 必须在任何 target/backup rename 之前关闭全部 live RocksDB handle、停止 background task 并保持 network admission 关闭。在 `StoragePaused` 后、`MarkerPersisted` 前崩溃时，启动预检必须使用 snapshot/install intent 和未改名的 target 恢复到可重试状态，不得开放未复验的 storage。`PreparedCheckpointRestore::commit` 不再删除 target；所有 target/backup/stage 判定由 marker phase 和 digest 决定。无法确定唯一权威副本时 fail closed。

- [x] **步骤 3：回归**

  ```powershell
  cargo test -p raft --test snapshot_install_recovery_test -- --nocapture
  cargo test -p raft --test snapshot_roundtrip_test install_snapshot -- --nocapture
  cargo test -p raft marker_cleanup_tests --lib -- --nocapture
  cargo test -p raft malformed_snapshot_install_marker -- --nocapture
  cargo test -p raft unknown_snapshot_install_marker_version -- --nocapture
  ```

## Task 5：全量 Vector meta/member/incarnation 一致性

**Requirement：** `REQ-VECTOR-001`、`REQ-VECTOR-002`

**文件：**

- Create: `src/storage/src/vector_consistency.rs`
- Modify: `src/storage/src/redis_vectors.rs`
- Modify: `src/storage/src/storage.rs`
- Modify: `src/storage/src/storage_migration.rs`
- Modify: `src/storage/src/format_vector.rs`
- Modify: `src/storage/src/lib.rs`
- Modify: `src/raft/src/state_machine.rs`
- Modify: `src/raft/src/snapshot_install.rs`
- Create: `src/storage/tests/vector_consistency_test.rs`
- Modify: `src/storage/tests/redis_vector_test.rs`
- Modify: `src/storage/tests/checkpoint_test.rs`

- [x] **步骤 1：写入第 65 条损坏和双向闭包失败测试**

  - `full_validation_detects_corrupt_member_after_first_64_entries`
  - `full_validation_rejects_wrong_storage_incarnation`
  - `full_validation_rejects_member_generation_without_matching_meta`
  - `full_validation_rejects_member_without_base_meta`
  - `full_validation_rejects_non_vector_base_meta_for_member`
  - `full_validation_rejects_meta_count_greater_than_members`
  - `full_validation_rejects_meta_count_less_than_members`
  - `full_validation_rejects_meta_without_member_range`
  - `full_validation_rejects_member_dimension_mismatch`
  - `full_validation_rejects_member_quantization_mismatch`
  - `full_validation_rejects_unknown_member_key_codec_version`
  - `full_validation_rejects_unknown_vector_value_codec_version`
  - `full_validation_rejects_invalid_meta_metric_or_format`
  - `full_validation_rejects_zero_or_invalid_data_revision`
  - `vector_mutations_never_decrease_data_revision`
  - `full_validation_accepts_multiple_sets_generations_and_instances`
  - `snapshot_corruption_after_64th_member_fails_before_storage_pause`

  ```powershell
  cargo test -p storage --test vector_consistency_test -- --nocapture
  ```

- [x] **步骤 2：实现全量双遍校验**

  - 第一遍遍历 `VectorDataCF`，严格验证 member key codec version 和 vector value codec version，解码 key/value，按 instance/incarnation/user-key/generation 统计 member 数并验证 dimension/quantization。
  - 第二遍遍历 `MetaCF` 的 `DataType::VectorSet`，核对 generation、count、dimension、quantization、metric、member range 和 `data_revision` 的非零/可解码合同；VADD/VREM/DEL+recreate 操作测试另外证明 revision 不回退。
  - orphan member、orphan meta、跨 incarnation/generation、count 不一致均 fail closed。
  - snapshot restore 删除 `RESTORED_VECTOR_SAMPLE_SIZE` 和 `validate_vector_data_sample(64)`，必须执行全量 validator。

- [x] **步骤 3：回归**

  ```powershell
  cargo test -p storage --test vector_consistency_test -- --nocapture
  cargo test -p storage --test redis_vector_test vector_consistency -- --nocapture
  cargo test -p storage --test redis_vector_test data_revision -- --nocapture
  cargo test -p raft snapshot_corruption_after_64th_member_fails_before_storage_pause -- --nocapture
  ```

## Task 6：`PreparedVsimSession` key-scoped 单一串行视图

**Requirement：** `REQ-VECTOR-004`

**文件：**

- Create: `src/storage/src/vsim_session.rs`
- Modify: `src/storage/src/redis_vectors.rs`
- Modify: `src/storage/src/storage_impl.rs`
- Modify: `src/storage/src/format_vector.rs`
- Modify: `src/storage/src/redis.rs`
- Modify: `src/storage/src/vector.rs`
- Modify: `src/storage/src/vector_fault.rs`
- Modify: `src/storage/src/lib.rs`
- Modify: `src/cmd/src/vector/vsim.rs`
- Create: `src/storage/tests/vsim_session_test.rs`
- Modify: `src/cmd/src/vector/vsim.rs` 内部测试

- [x] **步骤 1：写入确定性交错失败测试**

  - `vsim_session_blocks_query_member_update_until_search_finishes`
  - `vsim_session_blocks_other_member_update_until_search_finishes`
  - `vsim_session_blocks_query_member_remove_until_search_finishes`
  - `vsim_session_blocks_del_and_same_name_recreate_until_search_finishes`
  - `vsim_session_does_not_block_write_to_different_key`
  - `vsim_session_direct_vector_and_ele_use_same_captured_time`
  - `vsim_command_does_not_call_unlocked_prepare_then_unlocked_search`
  - `vsim_session_releases_key_lock_on_parse_error`
  - `vsim_session_releases_key_lock_on_flat_timeout_or_cancel`

  测试使用 channel/Condvar/Barrier，不用 `sleep` 猜测 writer 是否阻塞。

  ```powershell
  cargo test -p storage --test vsim_session_test -- --nocapture
  ```

- [x] **步骤 2：实现 RAII session**

  - `PreparedVsimSession<'a>` 持有 `&'a Redis`、`PreparedVectorQuery`、捕获的 `logical_now_micros`、RocksDB snapshot 和与 VADD/VREM/DEL 相同 key 的 `ScopeRecordLock<'a>`。
  - `prepare_vsim_session` 先加锁，再读 missing/WRONGTYPE/meta/ELE query；`search` 使用同一 generation、snapshot 和 logical time。
  - `Storage::{prepare_vsim, vsim}` 双入口删除或收窄为不可绕过 session 的 crate-private helper。
  - `VSimCmd::do_cmd` 在 guard 存活期内完成 direct vector/options 解析和 search。

- [x] **步骤 3：回归**

  ```powershell
  cargo test -p storage --test vsim_session_test -- --nocapture
  cargo test -p storage --test redis_vector_test vsim -- --nocapture
  cargo test -p cmd vector::vsim::tests -- --nocapture
  ```

## Task 7：真实 Base/Head upgrade、retry、rollback 和 v1 snapshot 矩阵

**Requirement：** `REQ-COMPAT-007`、`REQ-STORAGE-005`、`REQ-VECTOR-001`、`REQ-VECTOR-002`

**文件：**

- Create: `scripts/test-vector-storage-compat.sh`
- Create: `scripts/test-vector-storage-compat.ps1`
- Create: `tests/compat/vector_storage_fixture.py`
- Modify: `docs/superpowers/plans/2026-08-07-vector-set-storage-recovery.md` 的 Task 7 完成证据。
- Deferred: `.github/workflows/ci.yml` 的 required Linux compatibility job 接线由 Oracle/CI 工作流完成；本 Task 只交付可独立运行的 runner。

- [x] **步骤 1：先固定 runner 失败门禁**

  runner 逐项输出且 fail closed：

  - `base_688d905f_creates_real_six_cf_nonempty_storage`
  - `vector_v1_733888fc_creates_real_seven_cf_manifest_v1_storage`
  - `head_upgrades_and_reopens_real_base_storage`
  - `head_upgrades_and_reopens_real_vector_v1_storage`
  - `head_retries_every_migration_phase_for_both_source_profiles`
  - `base_reopens_verified_pre_admission_rollback`
  - `vector_v1_reopens_verified_pre_admission_rollback`
  - `head_rejects_base_rollback_after_rollback_window_closed`
  - `base_v1_snapshot_restores_on_head`
  - `v1_snapshot_with_unknown_or_vector_schema_is_rejected`
  - `head_v2_snapshot_reopens_with_exact_manifest_pairing`

  Base/Head build 或 fixture 数为零、任一 phase 未执行、任一数据读回失败或 cleanup 失败均返回非零。

- [x] **步骤 2：实现 exact-ref 矩阵**

  - 在 Linux 临时目录分别创建 exact Base 与 exact Vector-v1 worktree/build，不写入当前实现 worktree。
  - Base 写入 String、Hash、ZSet、TTL 并生成 v1 snapshot。
  - Vector-v1 写入 String、Hash、ZSet、TTL、Vector meta/member，并记录每实例 v1 manifest 的 incarnation/generation。
  - Head 对两种 source profile 的复制目录逐 phase 注入失败，每次检查目录、journal、CF、manifest 和用户数据。
  - 受控 rollback 后必须分别使用 Base binary 或 Vector-v1 binary 真实 reopen/read，不使用 Head parser 推断旧格式可读。
  - cleanup 移除临时 worktree、build 输出和 server 进程，并在退出前检查无遗留。

- [x] **步骤 3：Linux 验收**

  ```powershell
  wsl bash -lc 'cd /mnt/d/test/github/kiwi/.worktrees/wp8-storage-recovery && ./scripts/test-vector-storage-compat.sh --base-ref 688d905fec31b54aec76f36676f55efd8b5cfa17 --vector-v1-ref 733888fc90ad8ef039947e87b08d7500a405954a --head-ref "$(git rev-parse HEAD)"'
  ```

  实际结果：11 个 gate、30 个 migration phase 全部执行并通过；cleanup 移除 3 个临时 worktree、全部 build 输出且无 runner 进程遗留。

## 工作流最终门禁

- [ ] `git diff --check`
- [ ] `cargo fmt --check`
- [ ] `cargo clippy -p storage -p raft -p cmd -p server --all-targets --all-features -- -D warnings`
- [ ] Task 1→6 的全部 targeted Rust tests。
- [ ] Task 7 的 exact Base/Head Linux 矩阵，所有 phase 非零执行。
- [ ] `cargo test -p storage`、`cargo test -p raft`、`cargo test -p cmd vector::vsim`、`cargo test -p server`。
- [ ] 规格 reviewer 确认未回退到 live `create_missing_column_families(true)`、64 条抽样、先删 target 再 rename 或解锁后二次 VSIM snapshot。
- [ ] 代码质量 reviewer 确认无绝对 marker path、无非确定性 sleep 测试、无不受控 `remove_dir_all`、无新生产 `unwrap/expect`。
