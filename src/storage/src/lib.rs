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

mod data_compaction_filter;
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
pub mod slot_indexer;
mod statistics;
mod util;

mod batch;
pub mod checkpoint;
mod redis;
mod storage_define;
mod storage_impl;
mod storage_murmur3;

// commands
mod redis_hashes;
mod redis_lists;
mod redis_sets;
mod redis_strings;

pub mod error;
mod format_zset_score_key;
pub mod options;
pub mod storage;

pub mod redis_zsets;

pub use batch::{Batch, BinlogBatch, RocksBatch};
pub use checkpoint::{RAFT_SNAPSHOT_META_FILE, RaftSnapshotMeta, restore_checkpoint_layout};
pub use error::Result;
pub use expiration_manager::ExpirationManager;
pub use format_base_key::BaseMetaKey;
pub use format_base_value::*;
pub use format_zset_score_key::{ScoreMember, ZsetScoreMember};
pub use options::StorageOptions;
pub use redis::{ColumnFamilyIndex, Redis};
pub use statistics::KeyStatistics;
pub use storage::{BgTask, BgTaskHandler};
pub use storage_impl::BeforeOrAfter;
pub use util::{safe_cleanup_test_db, unique_test_db_path};
