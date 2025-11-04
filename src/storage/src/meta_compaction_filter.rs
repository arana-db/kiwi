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

use chrono::Utc;
use rocksdb::{
    CompactionDecision, compaction_filter::CompactionFilter,
    compaction_filter_factory::CompactionFilterFactory,
};

use crate::{
    DataType, base_key_format::ParsedBaseKey, base_meta_value_format::ParsedBaseMetaValue,
    list_meta_value_format::ParsedListsMetaValue, strings_value_format::ParsedStringsValue,
};

const META_FILTER_NAME: &std::ffi::CStr = c"MetaCompactionFilter";
const META_FILTER_FACTORY_NAME: &std::ffi::CStr = c"MetaCompactionFilterFactory";

#[derive(Debug, Default)]
pub struct MetaCompactionFilter;

impl CompactionFilter for MetaCompactionFilter {
    fn name(&self) -> &std::ffi::CStr {
        META_FILTER_NAME
    }

    fn filter(&mut self, _level: u32, key: &[u8], value: &[u8]) -> CompactionDecision {
        let cur_time = Utc::now().timestamp_micros() as u64;

        let parsed_key = match ParsedBaseKey::new(key) {
            Ok(k) => k,
            Err(e) => {
                log::warn!("Failed to parse key {key:?}: {e}, remove.");
                return CompactionDecision::Remove;
            }
        };

        if value.is_empty() {
            log::warn!("Empty value for key {:?}, remove.", parsed_key.key());
            return CompactionDecision::Remove;
        }

        let data_type = match DataType::try_from(value[0]) {
            Ok(dt) => dt,
            Err(_) => {
                log::warn!(
                    "Invalid data type byte {} for key {:?}, remove.",
                    value[0],
                    parsed_key.key()
                );
                return CompactionDecision::Remove;
            }
        };

        fn parse_and_check<T>(
            value: &[u8],
            parse_fn: impl Fn(&[u8]) -> Result<T, crate::error::Error>,
            check_fn: impl Fn(&T) -> bool,
        ) -> CompactionDecision {
            match parse_fn(value) {
                Ok(v) if check_fn(&v) => CompactionDecision::Remove,
                Ok(_) => CompactionDecision::Keep,
                Err(e) => {
                    log::warn!("Failed to parse value {value:?}: {e}, remove.");
                    CompactionDecision::Remove
                }
            }
        }

        match data_type {
            DataType::String => parse_and_check(
                value,
                |v| ParsedStringsValue::new(v),
                |v| v.etime() != 0 && v.etime() < cur_time,
            ),
            DataType::List => parse_and_check(
                value,
                |v| ParsedListsMetaValue::new(v),
                |m| {
                    (m.etime() != 0 && m.etime() < cur_time)
                        || (m.count() == 0 && m.version() < cur_time)
                },
            ),
            DataType::Hash | DataType::Set | DataType::ZSet => parse_and_check(
                value,
                |v| ParsedBaseMetaValue::new(v),
                |m| {
                    (m.etime() != 0 && m.etime() < cur_time)
                        || (m.count() == 0 && m.version() < cur_time)
                },
            ),
            _ => CompactionDecision::Keep,
        }
    }
}

#[derive(Debug, Default)]
pub struct MetaCompactionFilterFactory;

impl CompactionFilterFactory for MetaCompactionFilterFactory {
    type Filter = MetaCompactionFilter;

    fn create(
        &mut self,
        _context: rocksdb::compaction_filter_factory::CompactionFilterContext,
    ) -> Self::Filter {
        MetaCompactionFilter
    }

    fn name(&self) -> &std::ffi::CStr {
        META_FILTER_FACTORY_NAME
    }
}
