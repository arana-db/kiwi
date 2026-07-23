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

#![allow(clippy::unwrap_used)]

#[cfg(test)]
mod redis_basic_test {
    use std::sync::{Arc, atomic::Ordering};

    use bytes::Bytes;
    use kstd::lock_mgr::LockMgr;
    use storage::{
        BgTaskHandler, ColumnFamilyIndex, DataType, Redis, StorageOptions, TypeCheckState,
        format_base_meta_value::BaseMetaValue, format_strings_value::StringValue,
        safe_cleanup_test_db, unique_test_db_path,
    };

    #[test]
    fn test_redis_creation() {
        let storage_options = Arc::new(StorageOptions::default());
        let (bg_task_handler, _) = BgTaskHandler::new();
        let lock_mgr = Arc::new(LockMgr::new(1000));
        let redis = Redis::new(storage_options, 1, Arc::new(bg_task_handler), lock_mgr);

        assert_eq!(redis.get_index(), 1);
        assert!(redis.is_starting.load(Ordering::SeqCst));
        assert!(redis.db.is_none());
        assert_eq!(redis.handles.len(), 0);
    }

    #[test]
    fn test_check_type_state_reports_missing_stale_match_and_wrongtype() {
        let storage_options = Arc::new(StorageOptions::default());
        let (bg_task_handler, _) = BgTaskHandler::new();
        let lock_mgr = Arc::new(LockMgr::new(1000));
        let redis = Redis::new(storage_options, 1, Arc::new(bg_task_handler), lock_mgr);

        assert_eq!(
            redis.check_type_state(&[], DataType::String).unwrap(),
            TypeCheckState::Missing
        );

        let string_raw = StringValue::new(b"value".to_vec()).encode();
        assert_eq!(
            redis
                .check_type_state(string_raw.as_ref(), DataType::String)
                .unwrap(),
            TypeCheckState::Match
        );

        let mut stale_raw = StringValue::new(b"value".to_vec()).encode().to_vec();
        let etime_start = stale_raw.len() - 8;
        stale_raw[etime_start..].copy_from_slice(&1u64.to_le_bytes());
        assert_eq!(
            redis
                .check_type_state(stale_raw.as_ref(), DataType::String)
                .unwrap(),
            TypeCheckState::Stale
        );

        let mut set_meta = BaseMetaValue::new(Bytes::from(1u64.to_le_bytes().to_vec()));
        set_meta.inner.data_type = DataType::Set;
        let set_raw = set_meta.encode();
        let err = redis
            .check_type_state(set_raw.as_ref(), DataType::String)
            .unwrap_err();
        assert!(err.to_string().contains("WRONGTYPE"));
    }

    #[test]
    fn test_check_type_wrapper_treats_stale_foreign_type_as_missing() {
        let storage_options = Arc::new(StorageOptions::default());
        let (bg_task_handler, _) = BgTaskHandler::new();
        let lock_mgr = Arc::new(LockMgr::new(1000));
        let redis = Redis::new(storage_options, 1, Arc::new(bg_task_handler), lock_mgr);

        let mut set_meta = BaseMetaValue::new(Bytes::from(1u64.to_le_bytes().to_vec()));
        set_meta.inner.data_type = DataType::Set;
        let mut stale_set_raw = set_meta.encode().to_vec();
        let etime_start = stale_set_raw.len() - 8;
        stale_set_raw[etime_start..].copy_from_slice(&1u64.to_le_bytes());

        redis
            .check_type(stale_set_raw.as_ref(), DataType::String)
            .unwrap();
    }

    #[test]
    fn test_key_exists_live_propagates_non_key_not_found_errors() {
        let storage_options = Arc::new(StorageOptions::default());
        let (bg_task_handler, _) = BgTaskHandler::new();
        let lock_mgr = Arc::new(LockMgr::new(1000));
        let redis = Redis::new(storage_options, 1, Arc::new(bg_task_handler), lock_mgr);

        let err = redis.key_exists_live(b"missing-db").unwrap_err();
        assert!(err.to_string().contains("db is not initialized"));
    }

    #[test]
    fn test_redis_open() {
        let test_db_path = unique_test_db_path();

        safe_cleanup_test_db(&test_db_path);

        let storage_options = Arc::new(StorageOptions::default());
        let (bg_task_handler, _) = BgTaskHandler::new();
        let lock_mgr = Arc::new(LockMgr::new(1000));
        let mut redis = Redis::new(storage_options, 1, Arc::new(bg_task_handler), lock_mgr);

        let result = redis.open(test_db_path.to_str().unwrap());
        assert!(result.is_ok(), "open redis db failed: {:?}", result.err());

        assert!(!redis.is_starting.load(Ordering::SeqCst));
        assert!(redis.db.is_some());
        assert_eq!(redis.handles.len(), 7);

        for cf_index in 0..7 {
            let cf_enum = match cf_index {
                0 => ColumnFamilyIndex::MetaCF,
                1 => ColumnFamilyIndex::HashesDataCF,
                2 => ColumnFamilyIndex::SetsDataCF,
                3 => ColumnFamilyIndex::ListsDataCF,
                4 => ColumnFamilyIndex::ZsetsDataCF,
                5 => ColumnFamilyIndex::ZsetsScoreCF,
                6 => ColumnFamilyIndex::VectorDataCF,
                _ => panic!("Invalid CF index"),
            };

            let handle = redis.get_cf_handle(cf_enum);
            assert!(
                handle.is_some(),
                "column family handle {} not found",
                cf_index
            );
        }

        let expected_cf_names = [
            "default",        // MetaCF
            "hash_data_cf",   // HashesDataCF
            "set_data_cf",    // SetsDataCF
            "list_data_cf",   // ListsDataCF
            "zset_data_cf",   // ZsetsDataCF
            "zset_score_cf",  // ZsetsScoreCF
            "vector_data_cf", // VectorDataCF
        ];

        for (i, expected_name) in expected_cf_names.iter().enumerate() {
            assert_eq!(
                &redis.handles[i], expected_name,
                "column family name mismatch at index {}",
                i
            );
        }

        redis.set_need_close(true);
        drop(redis);
    }

    #[test]
    fn test_redis_drop_releases_rocksdb_for_same_path_reopen() {
        let test_db_path = unique_test_db_path();
        safe_cleanup_test_db(&test_db_path);

        {
            let storage_options = Arc::new(StorageOptions::default());
            let (bg_task_handler, _) = BgTaskHandler::new();
            let lock_mgr = Arc::new(LockMgr::new(1000));
            let mut redis = Redis::new(storage_options, 1, Arc::new(bg_task_handler), lock_mgr);

            redis
                .open(
                    test_db_path
                        .to_str()
                        .expect("test DB path should be valid UTF-8"),
                )
                .expect("first Redis owner should open RocksDB");
            redis.set_need_close(true);
        }

        {
            let storage_options = Arc::new(StorageOptions::default());
            let (bg_task_handler, _) = BgTaskHandler::new();
            let lock_mgr = Arc::new(LockMgr::new(1000));
            let mut reopened = Redis::new(storage_options, 1, Arc::new(bg_task_handler), lock_mgr);

            reopened
                .open(
                    test_db_path
                        .to_str()
                        .expect("test DB path should be valid UTF-8"),
                )
                .expect("dropping the first Redis owner should release the RocksDB lock");
            reopened.set_need_close(true);
        }

        safe_cleanup_test_db(&test_db_path);
    }

    #[test]
    fn test_column_family_index() {
        assert_eq!(ColumnFamilyIndex::MetaCF as usize, 0);
        assert_eq!(ColumnFamilyIndex::HashesDataCF as usize, 1);
        assert_eq!(ColumnFamilyIndex::SetsDataCF as usize, 2);
        assert_eq!(ColumnFamilyIndex::ListsDataCF as usize, 3);
        assert_eq!(ColumnFamilyIndex::ZsetsDataCF as usize, 4);
        assert_eq!(ColumnFamilyIndex::ZsetsScoreCF as usize, 5);
        assert_eq!(ColumnFamilyIndex::VectorDataCF as usize, 6);
        assert_eq!(ColumnFamilyIndex::COUNT, 7);

        assert_eq!(ColumnFamilyIndex::MetaCF.name(), "default");
        assert_eq!(ColumnFamilyIndex::HashesDataCF.name(), "hash_data_cf");
        assert_eq!(ColumnFamilyIndex::SetsDataCF.name(), "set_data_cf");
        assert_eq!(ColumnFamilyIndex::ListsDataCF.name(), "list_data_cf");
        assert_eq!(ColumnFamilyIndex::ZsetsDataCF.name(), "zset_data_cf");
        assert_eq!(ColumnFamilyIndex::ZsetsScoreCF.name(), "zset_score_cf");
        assert_eq!(ColumnFamilyIndex::VectorDataCF.name(), "vector_data_cf");
    }

    #[test]
    fn test_redis_properties() {
        let test_db_path = unique_test_db_path();

        safe_cleanup_test_db(&test_db_path);

        let storage_options = Arc::new(StorageOptions::default());
        let (bg_task_handler, _) = BgTaskHandler::new();
        let lock_mgr = Arc::new(LockMgr::new(1000));
        let mut redis = Redis::new(storage_options, 1, Arc::new(bg_task_handler), lock_mgr);

        let result = redis.open(test_db_path.to_str().unwrap());
        assert!(result.is_ok(), "open redis db failed: {:?}", result.err());

        let num_files = redis.get_property("rocksdb.num-files-at-level0");
        assert!(
            num_files.is_ok(),
            "get property failed: {:?}",
            num_files.err()
        );

        let num_files_value = num_files.unwrap();
        assert_eq!(
            num_files_value, 0,
            "new database should have 0 files at level0"
        );

        redis.set_need_close(true);
        drop(redis);
    }
}

#[cfg(test)]
mod is_stale_tests {
    use bytes::Bytes;
    use chrono::Utc;
    use kstd::lock_mgr::LockMgr;
    use std::sync::Arc;
    use storage::format_base_meta_value::HashesMetaValue;
    use storage::format_base_value::DataType;
    use storage::format_list_meta_value::ListsMetaValue;
    use storage::format_strings_value::StringValue;
    use storage::{BgTaskHandler, Redis, StorageOptions};

    fn now_micros() -> u64 {
        Utc::now().timestamp_micros() as u64
    }

    fn create_redis_instance() -> Redis {
        let storage_options = Arc::new(StorageOptions::default());
        let (bg_task_handler, _) = BgTaskHandler::new();
        let lock_mgr = Arc::new(LockMgr::new(1000));
        Redis::new(storage_options, 1, Arc::new(bg_task_handler), lock_mgr)
    }

    fn create_hash_meta_value(data_type: DataType, count: u64, etime: u64) -> Vec<u8> {
        let mut meta = HashesMetaValue::new(Bytes::copy_from_slice(&count.to_le_bytes()));
        meta.inner.data_type = data_type;
        meta.set_etime(etime);
        meta.encode().to_vec()
    }

    fn create_list_meta_value(count: u64, etime: u64) -> Vec<u8> {
        let mut meta = ListsMetaValue::new(Bytes::copy_from_slice(&count.to_le_bytes()));
        meta.set_etime(etime);
        meta.encode().to_vec()
    }

    fn create_string_value(value: &[u8], etime: u64) -> Vec<u8> {
        let mut string_val = StringValue::new(Bytes::copy_from_slice(value));
        string_val.set_etime(etime);
        string_val.encode().to_vec()
    }

    #[test]
    fn test_is_stale_empty_value() {
        let redis = create_redis_instance();
        let result = redis.is_stale(&[]);
        assert!(result.is_ok());
        assert!(!result.unwrap());
    }

    #[test]
    fn test_is_stale_invalid_data_type_none() {
        let redis = create_redis_instance();
        let meta = HashesMetaValue::new(Bytes::copy_from_slice(&1u64.to_le_bytes()));
        let value = meta.encode();

        let result = redis.is_stale(&value);
        assert!(
            result.is_err(),
            "DataType::None should return error, not panic"
        );
        let err_msg = format!("{:?}", result.unwrap_err());
        assert!(
            err_msg.contains("Unsupported meta data type")
                || err_msg.contains("should not be used"),
            "Error message should mention unsupported data type, got: {}",
            err_msg
        );
    }

    #[test]
    fn test_is_stale_invalid_data_type_all() {
        let redis = create_redis_instance();
        let mut meta = HashesMetaValue::new(Bytes::copy_from_slice(&1u64.to_le_bytes()));
        meta.inner.data_type = DataType::All;
        let value = meta.encode();

        let result = redis.is_stale(&value);
        assert!(
            result.is_err(),
            "DataType::All should return error, not panic"
        );
        let err_msg = format!("{:?}", result.unwrap_err());
        assert!(
            err_msg.contains("Unsupported meta data type")
                || err_msg.contains("should not be used"),
            "Error message should mention unsupported data type, got: {}",
            err_msg
        );
    }

    #[test]
    fn test_is_stale_invalid_unknown_type() {
        let redis = create_redis_instance();
        let value = vec![255u8; 50];

        let result = redis.is_stale(&value);
        assert!(result.is_err(), "Invalid type byte should return error");
        let err_msg = format!("{:?}", result.unwrap_err());
        assert!(err_msg.contains("Invalid data type byte"));
    }

    #[test]
    fn test_is_stale_insufficient_length_string() {
        let redis = create_redis_instance();
        let value = vec![DataType::String as u8, 0, 0, 0, 0, 0];

        let result = redis.is_stale(&value);
        assert!(result.is_err());
        let err_msg = format!("{:?}", result.unwrap_err());
        assert!(err_msg.contains("Invalid value length") || err_msg.contains("length"));
    }

    #[test]
    fn test_is_stale_insufficient_length_hash() {
        let redis = create_redis_instance();
        let value = vec![DataType::Hash as u8; 10];

        let result = redis.is_stale(&value);
        assert!(result.is_err());
    }

    #[test]
    fn test_is_stale_string_permanent() {
        let redis = create_redis_instance();
        let value = create_string_value(b"test_value", 0);

        let result = redis.is_stale(&value);
        assert!(result.is_ok());
        assert!(!result.unwrap(), "Permanent string should not be stale");
    }

    #[test]
    fn test_is_stale_string_not_expired() {
        let redis = create_redis_instance();
        let future_time = now_micros() + 10_000_000;
        let value = create_string_value(b"test_value", future_time);

        let result = redis.is_stale(&value);
        assert!(result.is_ok());
        assert!(
            !result.unwrap(),
            "Future-expiring string should not be stale"
        );
    }

    #[test]
    fn test_is_stale_string_expired() {
        let redis = create_redis_instance();
        let past_time = now_micros() - 1_000_000;
        let value = create_string_value(b"test_value", past_time);

        let result = redis.is_stale(&value);
        assert!(result.is_ok());
        assert!(result.unwrap(), "Expired string should be stale");
    }

    #[test]
    fn test_is_stale_string_empty_value() {
        let redis = create_redis_instance();
        let value = create_string_value(b"", 0);

        let result = redis.is_stale(&value);
        assert!(result.is_ok());
        assert!(
            !result.unwrap(),
            "Empty permanent string should not be stale"
        );
    }

    #[test]
    fn test_is_stale_hash_count_zero() {
        let redis = create_redis_instance();
        let value = create_hash_meta_value(DataType::Hash, 0, 0);

        let result = redis.is_stale(&value);
        assert!(result.is_ok());
        assert!(result.unwrap(), "Hash with count=0 should be stale");
    }

    #[test]
    fn test_is_stale_hash_permanent_with_count() {
        let redis = create_redis_instance();
        let value = create_hash_meta_value(DataType::Hash, 5, 0);

        let result = redis.is_stale(&value);
        assert!(result.is_ok());
        assert!(
            !result.unwrap(),
            "Permanent hash with count>0 should not be stale"
        );
    }

    #[test]
    fn test_is_stale_hash_not_expired_with_count() {
        let redis = create_redis_instance();
        let future_time = now_micros() + 10_000_000;
        let value = create_hash_meta_value(DataType::Hash, 5, future_time);

        let result = redis.is_stale(&value);
        assert!(result.is_ok());
        assert!(
            !result.unwrap(),
            "Future-expiring hash with count>0 should not be stale"
        );
    }

    #[test]
    fn test_is_stale_hash_expired_with_count() {
        let redis = create_redis_instance();
        let past_time = now_micros() - 1_000_000;
        let value = create_hash_meta_value(DataType::Hash, 5, past_time);

        let result = redis.is_stale(&value);
        assert!(result.is_ok());
        assert!(result.unwrap(), "Expired hash should be stale");
    }

    #[test]
    fn test_is_stale_hash_count_zero_not_expired() {
        let redis = create_redis_instance();
        let future_time = now_micros() + 10_000_000;
        let value = create_hash_meta_value(DataType::Hash, 0, future_time);

        let result = redis.is_stale(&value);
        assert!(result.is_ok());
        assert!(
            result.unwrap(),
            "Hash with count=0 should be stale even if not expired"
        );
    }

    #[test]
    fn test_is_stale_set_count_zero() {
        let redis = create_redis_instance();
        let value = create_hash_meta_value(DataType::Set, 0, 0);

        let result = redis.is_stale(&value);
        assert!(result.is_ok());
        assert!(result.unwrap(), "Set with count=0 should be stale");
    }

    #[test]
    fn test_is_stale_set_permanent_with_count() {
        let redis = create_redis_instance();
        let value = create_hash_meta_value(DataType::Set, 3, 0);

        let result = redis.is_stale(&value);
        assert!(result.is_ok());
        assert!(
            !result.unwrap(),
            "Permanent set with count>0 should not be stale"
        );
    }

    #[test]
    fn test_is_stale_set_expired_with_count() {
        let redis = create_redis_instance();
        let past_time = now_micros() - 1_000_000;
        let value = create_hash_meta_value(DataType::Set, 3, past_time);

        let result = redis.is_stale(&value);
        assert!(result.is_ok());
        assert!(result.unwrap(), "Expired set should be stale");
    }

    #[test]
    fn test_is_stale_zset_count_zero() {
        let redis = create_redis_instance();
        let value = create_hash_meta_value(DataType::ZSet, 0, 0);

        let result = redis.is_stale(&value);
        assert!(result.is_ok());
        assert!(result.unwrap(), "ZSet with count=0 should be stale");
    }

    #[test]
    fn test_is_stale_zset_permanent_with_count() {
        let redis = create_redis_instance();
        let value = create_hash_meta_value(DataType::ZSet, 10, 0);

        let result = redis.is_stale(&value);
        assert!(result.is_ok());
        assert!(
            !result.unwrap(),
            "Permanent zset with count>0 should not be stale"
        );
    }

    #[test]
    fn test_is_stale_zset_expired_with_count() {
        let redis = create_redis_instance();
        let past_time = now_micros() - 1_000_000;
        let value = create_hash_meta_value(DataType::ZSet, 10, past_time);

        let result = redis.is_stale(&value);
        assert!(result.is_ok());
        assert!(result.unwrap(), "Expired zset should be stale");
    }

    #[test]
    fn test_is_stale_list_count_zero() {
        let redis = create_redis_instance();
        let value = create_list_meta_value(0, 0);

        let result = redis.is_stale(&value);
        assert!(result.is_ok());
        assert!(result.unwrap(), "List with count=0 should be stale");
    }

    #[test]
    fn test_is_stale_list_permanent_with_count() {
        let redis = create_redis_instance();
        let value = create_list_meta_value(8, 0);

        let result = redis.is_stale(&value);
        assert!(result.is_ok());
        assert!(
            !result.unwrap(),
            "Permanent list with count>0 should not be stale"
        );
    }

    #[test]
    fn test_is_stale_list_not_expired_with_count() {
        let redis = create_redis_instance();
        let future_time = now_micros() + 10_000_000;
        let value = create_list_meta_value(8, future_time);

        let result = redis.is_stale(&value);
        assert!(result.is_ok());
        assert!(
            !result.unwrap(),
            "Future-expiring list with count>0 should not be stale"
        );
    }

    #[test]
    fn test_is_stale_list_expired_with_count() {
        let redis = create_redis_instance();
        let past_time = now_micros() - 1_000_000;
        let value = create_list_meta_value(8, past_time);

        let result = redis.is_stale(&value);
        assert!(result.is_ok());
        assert!(result.unwrap(), "Expired list should be stale");
    }

    #[test]
    fn test_is_stale_boundary_just_expired() {
        let redis = create_redis_instance();
        let just_past = now_micros() - 1_000_000;
        let value = create_string_value(b"test", just_past);

        let result = redis.is_stale(&value);
        assert!(result.is_ok());
        assert!(result.unwrap(), "Value just expired should be stale");
    }

    #[test]
    fn test_is_stale_boundary_just_not_expired() {
        let redis = create_redis_instance();
        let just_future = now_micros() + 1_000_000;
        let value = create_string_value(b"test", just_future);

        let result = redis.is_stale(&value);
        assert!(result.is_ok());
        assert!(
            !result.unwrap(),
            "Value not yet expired should not be stale"
        );
    }

    #[test]
    fn test_is_stale_all_valid_data_types() {
        let redis = create_redis_instance();
        let future_time = now_micros() + 1_000_000;

        let test_cases = vec![
            ("String", create_string_value(b"test", future_time)),
            (
                "Hash",
                create_hash_meta_value(DataType::Hash, 1, future_time),
            ),
            ("Set", create_hash_meta_value(DataType::Set, 1, future_time)),
            (
                "ZSet",
                create_hash_meta_value(DataType::ZSet, 1, future_time),
            ),
            ("List", create_list_meta_value(1, future_time)),
        ];

        for (name, value) in test_cases {
            let result = redis.is_stale(&value);
            assert!(result.is_ok(), "{} type should be handled", name);
            assert!(!result.unwrap(), "Valid {} should not be stale", name);
        }
    }

    #[test]
    fn test_is_stale_large_count_values() {
        let redis = create_redis_instance();

        let large_count = u64::MAX;
        let value = create_hash_meta_value(DataType::Hash, large_count, 0);

        let result = redis.is_stale(&value);
        assert!(result.is_ok());
        assert!(!result.unwrap(), "Hash with max count should not be stale");
    }
}
