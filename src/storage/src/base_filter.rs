/*
 * Copyright (c) 2024-present, arana-db Community.  All rights reserved.
 * 
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */



use crate::{
    base_key_format::ParsedBaseKey, base_value_format::DataType,
    strings_value_format::ParsedStringsValue,
};
use bytes::BytesMut;
use chrono::Utc;
use log::debug;
use rocksdb::{
    compaction_filter::CompactionFilter, compaction_filter_factory::CompactionFilterFactory,
    ColumnFamily, CompactionDecision, ReadOptions, DB,
};
use std::sync::Arc;

#[derive(Debug, Default)]
pub struct BaseMetaFilter;

#[allow(dead_code)]
#[derive(Debug, Default)]
pub struct BaseMetaFilterFactory;

/// TODO: remove allow dead code
#[allow(dead_code)]
pub struct BaseDataFilter {
    db: Arc<DB>,
    cf_handles: Arc<Vec<Arc<ColumnFamily>>>,
    target_data_type: DataType,
    default_read_opts: ReadOptions,
    cur_key: BytesMut,
    meta_not_found: bool,
    cur_meta_version: u64,
    cur_meta_etime: u64,
}

impl CompactionFilter for BaseMetaFilter {
    fn name(&self) -> &std::ffi::CStr {
        c"BaseMetaFilter"
    }

    fn filter(&mut self, _level: u32, key: &[u8], value: &[u8]) -> CompactionDecision {
        let current_time = Utc::now().timestamp_micros() as u64;

        let parsed_key_result = ParsedBaseKey::new(key);
        if let Err(e) = parsed_key_result {
            debug!("BaseMetaFilter: Failed to parse key {key:?}: {e}, remove.",);
            return CompactionDecision::Remove;
        }
        let parsed_key = parsed_key_result.unwrap();

        if value.is_empty() {
            debug!(
                "BaseMetaFilter: Value for key {:?} is empty, remove.",
                parsed_key.key()
            );
            return CompactionDecision::Remove;
        }

        let data_type = match DataType::try_from(value[0]) {
            Ok(dt) => dt,
            Err(_) => {
                debug!(
                    "BaseMetaFilter: Invalid data type byte {} for key {:?}, remove.",
                    value[0],
                    parsed_key.key()
                );
                return CompactionDecision::Remove;
            }
        };
        match data_type {
            DataType::String => match ParsedStringsValue::new(value) {
                Ok(pv) => pv.filter_decision(current_time),
                Err(e) => {
                    debug!(
                        "BaseMetaFilter: Failed to parse Strings value for key {:?}: {}, remove.",
                        parsed_key.key(),
                        e
                    );
                    CompactionDecision::Remove
                }
            },
            DataType::List => {
                todo!()
            }
            _ => {
                todo!()
            }
        }
    }
}

impl CompactionFilterFactory for BaseMetaFilterFactory {
    type Filter = BaseMetaFilter;

    fn create(
        &mut self,
        _context: rocksdb::compaction_filter_factory::CompactionFilterContext,
    ) -> Self::Filter {
        BaseMetaFilter
    }

    fn name(&self) -> &std::ffi::CStr {
        c"BaseMetaFilterFactory"
    }
}

/// TODO: remove allow dead code
#[allow(dead_code)]
impl BaseDataFilter {
    pub fn new(
        db: Arc<DB>,
        cf_handles: Arc<Vec<Arc<ColumnFamily>>>,
        target_data_type: DataType,
    ) -> Self {
        Self {
            db,
            cf_handles,
            target_data_type,
            default_read_opts: ReadOptions::default(),
            cur_key: BytesMut::new(),
            meta_not_found: false,
            cur_meta_version: 0,
            cur_meta_etime: 0,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::strings_value_format::StringValue;

    #[test]
    fn test_strings_base_filter() {
        let mut filter = BaseMetaFilter::default();
        let ttl = 1_000_000;

        let string_val: &'static [u8] = b"filter_val";
        let mut string_val = StringValue::new(string_val);
        assert!(matches!(string_val.set_relative_etime(ttl), Ok(())));

        let decision = filter.filter(0, string_val.encode().as_ref(), &string_val.encode());
        assert!(matches!(decision, CompactionDecision::Keep));

        std::thread::sleep(std::time::Duration::from_secs(2));
        let decision = filter.filter(0, string_val.encode().as_ref(), &string_val.encode());
        assert!(matches!(decision, CompactionDecision::Remove));
    }
}