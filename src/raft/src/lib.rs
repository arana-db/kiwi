// Copyright (c) 2024-present, arana-db Community.  All rights reserved.
//
// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Raft module for Kiwi
//!
//! Re-exports logindex types from storage::logindex to avoid code duplication.

pub mod api;
pub mod db_access; // Shim for backward compatibility with tests
pub mod log_store;
pub mod log_store_rocksdb;
pub mod network;
pub mod node;
pub mod snapshot_archive;
pub mod state_machine;

// Re-export logindex types from storage::logindex (no duplicate implementations)
pub use storage::logindex::{
    DbCfAccess, FlushTrigger, LogIndex, LogIndexAndSequenceCollector,
    LogIndexAndSequenceCollectorPurger, LogIndexAndSequencePair, LogIndexOfColumnFamilies,
    LogIndexSeqnoPair, LogIndexTablePropertiesCollectorFactory, PROPERTY_KEY, SequenceNumber,
    SmallestIndexRes, SnapshotCallback, cf_name_to_index, get_largest_log_index_from_collection,
    read_stats_from_table_props,
};

// Re-export error types for tests implementing DbCfAccess trait
pub use storage::logindex::types::LogIndexError;

/// Number of column families, consistent with storage::ColumnFamilyIndex::COUNT
pub const COLUMN_FAMILY_COUNT: usize = storage::ColumnFamilyIndex::COUNT;

/// List of CF names, in the same order as storage::ColumnFamilyIndex
pub const CF_NAMES: [&str; COLUMN_FAMILY_COUNT] = [
    "default",       // MetaCF = 0
    "hash_data_cf",  // HashesDataCF = 1
    "set_data_cf",   // SetsDataCF = 2
    "list_data_cf",  // ListsDataCF = 3
    "zset_data_cf",  // ZsetsDataCF = 4
    "zset_score_cf", // ZsetsScoreCF = 5
];

const _: () = assert!(
    CF_NAMES.len() == storage::ColumnFamilyIndex::COUNT,
    "CF_NAMES length must match storage::ColumnFamilyIndex::COUNT"
);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cf_names_match_storage() {
        use storage::ColumnFamilyIndex;
        let variants: [ColumnFamilyIndex; ColumnFamilyIndex::COUNT] = [
            ColumnFamilyIndex::MetaCF,
            ColumnFamilyIndex::HashesDataCF,
            ColumnFamilyIndex::SetsDataCF,
            ColumnFamilyIndex::ListsDataCF,
            ColumnFamilyIndex::ZsetsDataCF,
            ColumnFamilyIndex::ZsetsScoreCF,
        ];
        for (i, cf_index) in variants.iter().enumerate() {
            assert_eq!(
                cf_index.name(),
                CF_NAMES[i],
                "CF_NAMES[{}] mismatch: expected '{}', got '{}'",
                i,
                cf_index.name(),
                CF_NAMES[i]
            );
        }
    }
}
