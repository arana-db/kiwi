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

//! LogIndex module for tracking raft log index in RocksDB SST files
//!
//! This module provides:
//! - `LogIndexAndSequenceCollector`: maintains (log_index, seqno) mappings
//! - `LogIndexOfColumnFamilies`: tracks applied/flushed state per CF
//! - `LogIndexTablePropertiesCollectorFactory`: writes log_index to SST properties
//! - `LogIndexAndSequenceCollectorPurger`: EventListener for flush completion

pub mod cf_tracker;
pub mod collector;
pub mod db_access;
pub mod event_listener;
pub mod table_properties;
pub mod types;

// Re-export main types for convenience
pub use cf_tracker::{LogIndexOfColumnFamilies, SmallestIndexRes};
pub use collector::LogIndexAndSequenceCollector;
pub use db_access::DbCfAccess;
pub use event_listener::{
    FlushTrigger, LogIndexAndSequenceCollectorPurger, SnapshotCallback, cf_name_to_index,
};
pub use table_properties::{
    LogIndexTablePropertiesCollectorFactory, PROPERTY_KEY, get_largest_log_index_from_collection,
    read_stats_from_table_props,
};
pub use types::{
    LogIndex, LogIndexAndSequencePair, LogIndexError, LogIndexSeqnoPair, Result, SequenceNumber,
};
// Re-export cf_metadata for unified CF name access
pub use types::cf_metadata;
