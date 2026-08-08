// Copyright (c) 2024-present, arana-db Community.  All rights reserved.
//
// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

extern crate core;

mod format_base_data_value;
mod format_member_data_key;
pub mod format_vector;
pub mod format_vector_member_key;
mod storage_manifest;
mod storage_migration;
pub mod vector;
mod vector_consistency;
pub mod vector_fault;
mod vector_flat;
pub mod vector_metrics;
pub mod vector_mutation;

mod data_compaction_filter;
mod durable_fs;
mod meta_compaction_filter;

mod custom_comparator;
mod format_base_key;
pub mod format_base_meta_value;
pub mod format_base_value;

pub mod format_strings_value;

pub mod format_list_meta_value;
mod format_lists_data_key;

mod coding;
mod expiration_manager;
mod merge_iterator;
pub mod slot_indexer;
mod statistics;
mod storage_schema;
mod util;

mod batch;
pub mod checkpoint;
mod redis;
mod storage_define;
mod storage_impl;
mod storage_murmur3;
mod storage_scan;

// commands
mod redis_hashes;
mod redis_lists;
mod redis_sets;
mod redis_strings;
mod redis_vectors;

pub mod error;
mod format_zset_score_key;
pub mod options;
pub mod storage;

pub mod redis_zsets;

// LogIndex module for Raft snapshot integration
pub mod logindex;

#[cfg(any(test, feature = "test-fault-injection"))]
pub use batch::fail_next_rocks_batch_commit;
pub use batch::{AppendLogFn, Batch, BinlogBatch, RocksBatch};
pub use checkpoint::{
    CURRENT_SNAPSHOT_VERSION, ParsedSnapshotMeta, PreparedCheckpointRestore,
    RAFT_SNAPSHOT_META_FILE, RaftSnapshotMeta, STORAGE_SCHEMA_VERSION, SnapshotInstanceManifest,
    prepare_checkpoint_restore, prepare_classified_checkpoint_restore, restore_checkpoint_layout,
};
pub use durable_fs::{sync_directory, sync_parent_directory};
pub use error::Result;
pub use expiration_manager::ExpirationManager;
pub use format_base_key::BaseMetaKey;
pub use format_base_value::*;
pub use format_zset_score_key::{ScoreMember, ZsetScoreMember};
pub use options::StorageOptions;
#[cfg(any(test, feature = "test-fault-injection"))]
pub use redis::fail_next_redis_open;
pub use redis::{GenerationProvider, Redis, TypeCheckState};
pub use statistics::KeyStatistics;
pub use storage::{BgTask, BgTaskHandler};
pub use storage_impl::BeforeOrAfter;
#[cfg(any(test, feature = "test-fault-injection"))]
pub use storage_manifest::fail_next_storage_manifest_persist;
pub use storage_manifest::{
    INSTANCE_STORAGE_MANIFEST_VERSION, InstanceStorageManifestV2, ManifestDigest, MigrationPhase,
    MigrationSourceProfile, MigrationTransaction, ROOT_STORAGE_MANIFEST_FILE,
    ROOT_STORAGE_MANIFEST_VERSION, RootStorageManifestV2, SLOT_MAPPING_VERSION,
    STORAGE_MANIFEST_FILE, STORAGE_SCHEMA_VERSION_V2, slot_mapping_digest,
};
#[cfg(any(test, feature = "test-fault-injection"))]
pub use storage_migration::fail_next_storage_migration;
pub use storage_migration::{
    MigrationFaultPoint, MigrationLayout, classify_storage_root, close_rollback_window,
    finalize_migration_after_storage_open, logical_snapshot_digests_from_root,
    prepare_or_resume_migration, recover_or_rollback_before_admission,
};
pub use storage_schema::{
    CANONICAL_COLUMN_FAMILIES, CANONICAL_COLUMN_FAMILY_NAMES, ColumnFamilyIndex, ColumnFamilyRole,
    ColumnFamilySpec, ComparatorId, canonical_column_family_names,
};
pub use util::{safe_cleanup_test_db, unique_test_db_path};
pub use vector::{
    CanonicalVector, PreparedVectorQuery, QuantizationType, VectorHit, VectorInfo, VectorQuery,
    VectorSearchEngine, VectorSearchMode, VectorSearchOptions,
};
pub use vector_consistency::VectorConsistencyReport;
pub use vector_fault::VectorFaultHooks;
pub use vector_flat::{FlatQueryCancel, FlatQueryGate, FlatScanGuard};
pub use vector_metrics::{VectorMetrics, VectorMetricsSnapshot};
pub use vector_mutation::{
    VectorSetApplyError, VectorSetApplyResult, VectorSetBusinessError, VectorSetMutationV1,
};
