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

mod base_data_value_format;
mod member_data_key_format;

mod base_filter;
mod base_key_format;
mod base_meta_value_format;
mod base_value_format;
mod custom_comparator;

mod strings_value_format;

mod list_meta_value_format;
mod lists_data_key_format;

mod coding;
mod slot_indexer;
mod statistics;
mod util;

mod redis;
mod storage_define;
mod storage_impl;
mod storage_murmur3;

// commands
mod redis_hashes;
mod redis_sets;
mod redis_strings;

pub mod error;
pub mod options;
pub mod storage;

pub use base_key_format::BaseMetaKey;
pub use base_value_format::*;
pub use error::Result;
pub use options::StorageOptions;
pub use redis::{ColumnFamilyIndex, Redis};
pub use statistics::KeyStatistics;
pub use storage::{BgTask, BgTaskHandler};
pub use util::unique_test_db_path;
