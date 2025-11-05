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

#[cfg(test)]
mod tests {
    use chrono::Utc;
    use rocksdb::{
        CompactionDecision,
        compaction_filter::CompactionFilter,
        compaction_filter_factory::{CompactionFilterContext, CompactionFilterFactory},
    };

    use crate::{
        DataType,
        base_key_format::BaseKey,
        base_meta_value_format::BaseMetaValue,
        list_meta_value_format::ListsMetaValue,
        meta_compaction_filter::{MetaCompactionFilter, MetaCompactionFilterFactory},
        strings_value_format::StringValue,
    };

    #[test]
    fn test_meta_compaction_filter_factory() {
        let mut factory = MetaCompactionFilterFactory::default();
        let context = CompactionFilterContext {
            is_full_compaction: false,
            is_manual_compaction: false,
        };
        let filter = factory.create(context);

        assert_eq!(filter.name().to_bytes(), c"MetaCompactionFilter".to_bytes());
        assert_eq!(
            factory.name().to_bytes(),
            c"MetaCompactionFilterFactory".to_bytes()
        );
    }

    #[test]
    fn test_string_value_not_expired() {
        let mut filter = MetaCompactionFilter::default();
        let key = BaseKey::new(b"test_key");
        let encoded_key = key.encode().unwrap();

        let mut string_value = StringValue::new(b"test_value".as_slice());
        // Set etime to future (1 hour from now)
        let future_time = Utc::now().timestamp_micros() as u64 + 3_600_000_000;
        string_value.set_etime(future_time);
        let encoded_value = string_value.encode();

        let decision = filter.filter(0, &encoded_key, &encoded_value);
        assert!(matches!(decision, CompactionDecision::Keep));
    }

    #[test]
    fn test_string_value_expired() {
        let mut filter = MetaCompactionFilter::default();
        let key = BaseKey::new(b"test_key");
        let encoded_key = key.encode().unwrap();

        let mut string_value = StringValue::new(b"test_value".as_slice());
        // Set etime to past (1 hour ago)
        let past_time = Utc::now().timestamp_micros() as u64 - 3_600_000_000;
        string_value.set_etime(past_time);
        let encoded_value = string_value.encode();

        let decision = filter.filter(0, &encoded_key, &encoded_value);
        assert!(matches!(decision, CompactionDecision::Remove));
    }

    #[test]
    fn test_string_value_permanent() {
        let mut filter = MetaCompactionFilter::default();
        let key = BaseKey::new(b"test_key");
        let encoded_key = key.encode().unwrap();

        let string_value = StringValue::new(b"test_value".as_slice());
        // etime is 0 by default (permanent)
        let encoded_value = string_value.encode();

        let decision = filter.filter(0, &encoded_key, &encoded_value);
        assert!(matches!(decision, CompactionDecision::Keep));
    }

    #[test]
    fn test_list_meta_value_not_expired() {
        let mut filter = MetaCompactionFilter::default();
        let key = BaseKey::new(b"test_list_key");
        let encoded_key = key.encode().unwrap();

        let mut list_meta = ListsMetaValue::new(bytes::Bytes::copy_from_slice(&1u64.to_le_bytes()));
        list_meta.update_version();
        // Set etime to future
        let future_time = Utc::now().timestamp_micros() as u64 + 3_600_000_000;
        list_meta.set_etime(future_time);
        let encoded_value = list_meta.encode();

        let decision = filter.filter(0, &encoded_key, &encoded_value);
        assert!(matches!(decision, CompactionDecision::Keep));
    }

    #[test]
    fn test_list_meta_value_expired() {
        let mut filter = MetaCompactionFilter::default();
        let key = BaseKey::new(b"test_list_key");
        let encoded_key = key.encode().unwrap();

        let mut list_meta = ListsMetaValue::new(bytes::Bytes::copy_from_slice(&1u64.to_le_bytes()));
        // Set etime to past and version to past
        let past_time = Utc::now().timestamp_micros() as u64 - 3_600_000_000;
        list_meta.set_etime(past_time);
        list_meta.set_version(past_time);
        let encoded_value = list_meta.encode();

        let decision = filter.filter(0, &encoded_key, &encoded_value);
        assert!(matches!(decision, CompactionDecision::Remove));
    }

    #[test]
    fn test_list_meta_value_empty_and_expired_version() {
        let mut filter = MetaCompactionFilter::default();
        let key = BaseKey::new(b"test_list_key");
        let encoded_key = key.encode().unwrap();

        let mut list_meta = ListsMetaValue::new(bytes::Bytes::copy_from_slice(&0u64.to_le_bytes())); // count = 0
        // Set version to past
        let past_time = Utc::now().timestamp_micros() as u64 - 3_600_000_000;
        list_meta.set_version(past_time);
        let encoded_value = list_meta.encode();

        let decision = filter.filter(0, &encoded_key, &encoded_value);
        assert!(matches!(decision, CompactionDecision::Remove));
    }

    #[test]
    fn test_list_meta_value_empty_but_recent_version() {
        let mut filter = MetaCompactionFilter::default();
        let key = BaseKey::new(b"test_list_key");
        let encoded_key = key.encode().unwrap();

        let mut list_meta = ListsMetaValue::new(bytes::Bytes::copy_from_slice(&0u64.to_le_bytes())); // count = 0
        // Set version to a future time to ensure it's "recent"
        let future_time = Utc::now().timestamp_micros() as u64 + 1_000_000; // 1 second in the future
        list_meta.set_version(future_time);
        let encoded_value = list_meta.encode();

        let decision = filter.filter(0, &encoded_key, &encoded_value);
        assert!(matches!(decision, CompactionDecision::Keep));
    }

    #[test]
    fn test_hash_meta_value_not_expired() {
        let mut filter = MetaCompactionFilter::default();
        let key = BaseKey::new(b"test_hash_key");
        let encoded_key = key.encode().unwrap();

        let mut hash_meta = BaseMetaValue::new(bytes::Bytes::copy_from_slice(&1u64.to_le_bytes()));
        hash_meta.inner.data_type = DataType::Hash;
        hash_meta.update_version();
        // Set etime to future
        let future_time = Utc::now().timestamp_micros() as u64 + 3_600_000_000;
        hash_meta.set_etime(future_time);
        let encoded_value = hash_meta.encode();

        let decision = filter.filter(0, &encoded_key, &encoded_value);
        assert!(matches!(decision, CompactionDecision::Keep));
    }

    #[test]
    fn test_hash_meta_value_expired() {
        let mut filter = MetaCompactionFilter::default();
        let key = BaseKey::new(b"test_hash_key");
        let encoded_key = key.encode().unwrap();

        let mut hash_meta = BaseMetaValue::new(bytes::Bytes::copy_from_slice(&1u64.to_le_bytes()));
        hash_meta.inner.data_type = DataType::Hash;
        // Set etime to past and version to past
        let past_time = Utc::now().timestamp_micros() as u64 - 3_600_000_000;
        hash_meta.set_etime(past_time);
        hash_meta.set_version(past_time);
        let encoded_value = hash_meta.encode();

        let decision = filter.filter(0, &encoded_key, &encoded_value);
        assert!(matches!(decision, CompactionDecision::Remove));
    }

    #[test]
    fn test_hash_meta_value_empty_and_expired_version() {
        let mut filter = MetaCompactionFilter::default();
        let key = BaseKey::new(b"test_hash_key");
        let encoded_key = key.encode().unwrap();

        let mut hash_meta = BaseMetaValue::new(bytes::Bytes::copy_from_slice(&0u64.to_le_bytes())); // count = 0
        hash_meta.inner.data_type = DataType::Hash;
        // Set version to past
        let past_time = Utc::now().timestamp_micros() as u64 - 3_600_000_000;
        hash_meta.set_version(past_time);
        let encoded_value = hash_meta.encode();

        let decision = filter.filter(0, &encoded_key, &encoded_value);
        assert!(matches!(decision, CompactionDecision::Remove));
    }

    #[test]
    fn test_set_meta_value_expired() {
        let mut filter = MetaCompactionFilter::default();
        let key = BaseKey::new(b"test_set_key");
        let encoded_key = key.encode().unwrap();

        let mut set_meta = BaseMetaValue::new(bytes::Bytes::copy_from_slice(&1u64.to_le_bytes()));
        set_meta.inner.data_type = DataType::Set;
        let past_time = Utc::now().timestamp_micros() as u64 - 3_600_000_000;
        set_meta.set_etime(past_time);
        set_meta.set_version(past_time);
        let encoded_value = set_meta.encode();

        let decision = filter.filter(0, &encoded_key, &encoded_value);
        assert!(matches!(decision, CompactionDecision::Remove));
    }

    #[test]
    fn test_zset_meta_value_expired() {
        let mut filter = MetaCompactionFilter::default();
        let key = BaseKey::new(b"test_zset_key");
        let encoded_key = key.encode().unwrap();

        let mut zset_meta = BaseMetaValue::new(bytes::Bytes::copy_from_slice(&1u64.to_le_bytes()));
        zset_meta.inner.data_type = DataType::ZSet;
        let past_time = Utc::now().timestamp_micros() as u64 - 3_600_000_000;
        zset_meta.set_etime(past_time);
        zset_meta.set_version(past_time);
        let encoded_value = zset_meta.encode();

        let decision = filter.filter(0, &encoded_key, &encoded_value);
        assert!(matches!(decision, CompactionDecision::Remove));
    }

    #[test]
    fn test_invalid_key() {
        let mut filter = MetaCompactionFilter::default();
        let invalid_key = b"short"; // Key is too short to be valid

        let string_value = StringValue::new(b"test_value".as_slice());
        let encoded_value = string_value.encode();

        let decision = filter.filter(0, invalid_key, &encoded_value);
        assert!(matches!(decision, CompactionDecision::Remove));
    }

    #[test]
    fn test_empty_value() {
        let mut filter = MetaCompactionFilter::default();
        let key = BaseKey::new(b"test_key");
        let encoded_key = key.encode().unwrap();

        let empty_value = b"";

        let decision = filter.filter(0, &encoded_key, empty_value);
        assert!(matches!(decision, CompactionDecision::Remove));
    }

    #[test]
    fn test_invalid_data_type() {
        let mut filter = MetaCompactionFilter::default();
        let key = BaseKey::new(b"test_key");
        let encoded_key = key.encode().unwrap();

        // Create invalid value with invalid data type byte
        let invalid_value = vec![255, 1, 2, 3];

        let decision = filter.filter(0, &encoded_key, &invalid_value);
        assert!(matches!(decision, CompactionDecision::Remove));
    }

    #[test]
    fn test_invalid_string_value_format() {
        let mut filter = MetaCompactionFilter::default();
        let key = BaseKey::new(b"test_key");
        let encoded_key = key.encode().unwrap();

        // Create value with String type but invalid format
        let invalid_value = vec![DataType::String as u8, 1, 2, 3];

        let decision = filter.filter(0, &encoded_key, &invalid_value);
        assert!(matches!(decision, CompactionDecision::Remove));
    }

    #[test]
    fn test_unknown_data_type() {
        let mut filter = MetaCompactionFilter::default();
        let key = BaseKey::new(b"test_key");
        let encoded_key = key.encode().unwrap();

        // Create value with unknown but valid data type (e.g., None)
        let value = vec![DataType::None as u8, 1, 2, 3];

        // Unknown types should be kept
        let decision = filter.filter(0, &encoded_key, &value);
        assert!(matches!(decision, CompactionDecision::Keep));
    }

    #[test]
    fn test_list_meta_value_with_etime_zero_but_expired_version() {
        let mut filter = MetaCompactionFilter::default();
        let key = BaseKey::new(b"test_list_key");
        let encoded_key = key.encode().unwrap();

        let mut list_meta = ListsMetaValue::new(bytes::Bytes::copy_from_slice(&0u64.to_le_bytes()));
        list_meta.set_etime(0); // Permanent
        // But version is expired
        let past_time = Utc::now().timestamp_micros() as u64 - 3_600_000_000;
        list_meta.set_version(past_time);
        let encoded_value = list_meta.encode();

        let decision = filter.filter(0, &encoded_key, &encoded_value);
        assert!(matches!(decision, CompactionDecision::Remove));
    }

    #[test]
    fn test_hash_meta_value_with_etime_zero_but_expired_version() {
        let mut filter = MetaCompactionFilter::default();
        let key = BaseKey::new(b"test_hash_key");
        let encoded_key = key.encode().unwrap();

        let mut hash_meta = BaseMetaValue::new(bytes::Bytes::copy_from_slice(&0u64.to_le_bytes()));
        hash_meta.inner.data_type = DataType::Hash;
        hash_meta.set_etime(0); // Permanent
        // But version is expired
        let past_time = Utc::now().timestamp_micros() as u64 - 3_600_000_000;
        hash_meta.set_version(past_time);
        let encoded_value = hash_meta.encode();

        let decision = filter.filter(0, &encoded_key, &encoded_value);
        assert!(matches!(decision, CompactionDecision::Remove));
    }
}
