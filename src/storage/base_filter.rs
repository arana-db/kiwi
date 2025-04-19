// Copyright 2024 The Kiwi-rs Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//  of patent rights can be found in the PATENTS file in the same directory.

use chrono::Utc;
use log::debug;
use rocksdb::{
    CompactionDecision, compaction_filter::CompactionFilter,
    compaction_filter_factory::CompactionFilterFactory,
};

use crate::storage::{
    base_key_format::ParsedBaseKey,
    base_value_format::{DataType, ParsedInternalValue},
    strings_value_format::ParsedStringsValue,
};

#[derive(Debug, Default)]
pub struct BaseMetaFilter;

#[derive(Debug, Default)]
pub struct BaseMetaFilterFactory;

impl CompactionFilter for BaseMetaFilter {
    fn name(&self) -> &std::ffi::CStr {
        c"BaseMetaFilter"
    }

    fn filter(&mut self, _level: u32, key: &[u8], value: &[u8]) -> CompactionDecision {
        let current_time = Utc::now().timestamp_micros() as u64;

        let parsed_key = ParsedBaseKey::new(key);

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
                    "BaseMetaFilter: Invalid data type byte {} for key {:?}, remove",
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_strings_filter() {
        let mut filter = BaseMetaFilter::default();

        let string_val: &'static [u8] = b"filter_val";
        let mut string_val = crate::storage::strings_value_format::StringValue::new(string_val);
        let ttl = 1_000_000;
        crate::storage::base_value_format::InternalValue::set_relative_timestamp(
            &mut string_val,
            ttl,
        );

        let decision = filter.filter(
            0,
            b"filter_key",
            &crate::storage::base_value_format::InternalValue::encode(&string_val),
        );
        assert!(matches!(decision, CompactionDecision::Keep));

        std::thread::sleep(std::time::Duration::from_secs(2));

        let decision = filter.filter(
            0,
            b"filter_key",
            &crate::storage::base_value_format::InternalValue::encode(&string_val),
        );
        assert!(matches!(decision, CompactionDecision::Remove));
    }
}
