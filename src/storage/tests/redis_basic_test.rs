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

#[cfg(test)]
mod redis_basic_test {
    use std::sync::{Arc, atomic::Ordering};

    use kstd::lock_mgr::LockMgr;
    use storage::{BgTaskHandler, ColumnFamilyIndex, Redis, StorageOptions, safe_cleanup_test_db, unique_test_db_path};

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
        assert_eq!(redis.handles.len(), 6);

        for cf_index in 0..6 {
            let cf_enum = match cf_index {
                0 => ColumnFamilyIndex::MetaCF,
                1 => ColumnFamilyIndex::HashesDataCF,
                2 => ColumnFamilyIndex::SetsDataCF,
                3 => ColumnFamilyIndex::ListsDataCF,
                4 => ColumnFamilyIndex::ZsetsDataCF,
                5 => ColumnFamilyIndex::ZsetsScoreCF,
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
            "default",       // MetaCF
            "hash_data_cf",  // HashesDataCF
            "set_data_cf",   // SetsDataCF
            "list_data_cf",  // ListsDataCF
            "zset_data_cf",  // ZsetsDataCF
            "zset_score_cf", // ZsetsScoreCF
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
    fn test_column_family_index() {
        assert_eq!(ColumnFamilyIndex::MetaCF as usize, 0);
        assert_eq!(ColumnFamilyIndex::HashesDataCF as usize, 1);
        assert_eq!(ColumnFamilyIndex::SetsDataCF as usize, 2);
        assert_eq!(ColumnFamilyIndex::ListsDataCF as usize, 3);
        assert_eq!(ColumnFamilyIndex::ZsetsDataCF as usize, 4);
        assert_eq!(ColumnFamilyIndex::ZsetsScoreCF as usize, 5);

        assert_eq!(ColumnFamilyIndex::MetaCF.name(), "default");
        assert_eq!(ColumnFamilyIndex::HashesDataCF.name(), "hash_data_cf");
        assert_eq!(ColumnFamilyIndex::SetsDataCF.name(), "set_data_cf");
        assert_eq!(ColumnFamilyIndex::ListsDataCF.name(), "list_data_cf");
        assert_eq!(ColumnFamilyIndex::ZsetsDataCF.name(), "zset_data_cf");
        assert_eq!(ColumnFamilyIndex::ZsetsScoreCF.name(), "zset_score_cf");
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
