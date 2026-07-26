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
mod scan_test {
    use std::{collections::HashSet, sync::Arc};

    use storage::{StorageOptions, ZsetScoreMember, storage::Storage};

    fn open_storage(instances: usize) -> (Storage, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let mut storage = Storage::new(instances, 0);
        let _rx = storage
            .open(Arc::new(StorageOptions::default()), dir.path())
            .unwrap();
        (storage, dir)
    }

    /// Drive `SCAN` to completion and return every key it yields.
    fn scan_all(
        storage: &Storage,
        count: usize,
        type_filter: Option<&[u8]>,
        pattern: &[u8],
    ) -> Vec<Vec<u8>> {
        let mut cursor = 0u64;
        let mut out = Vec::new();
        let mut steps = 0;
        loop {
            let (next, keys) = storage.scan(cursor, count, type_filter, pattern).unwrap();
            out.extend(keys);
            cursor = next;
            steps += 1;
            assert!(steps < 1_000_000, "scan failed to terminate");
            if cursor == 0 {
                break;
            }
        }
        out
    }

    fn as_set(keys: Vec<Vec<u8>>) -> HashSet<Vec<u8>> {
        keys.into_iter().collect()
    }

    #[tokio::test]
    async fn scan_covers_every_key_across_instances_with_small_count() {
        let (storage, _dir) = open_storage(3);

        let mut expected = HashSet::new();
        for i in 0..50 {
            let key = format!("key:{i}").into_bytes();
            storage.set(&key, b"v").unwrap();
            expected.insert(key);
        }

        // COUNT=1 forces many steps and instance boundary crossings; coverage
        // must still be exact with no duplicates.
        let found = scan_all(&storage, 1, None, b"*");
        assert_eq!(
            found.len(),
            expected.len(),
            "scan returned {} keys, expected {} (duplicates or misses)",
            found.len(),
            expected.len()
        );
        assert_eq!(as_set(found), expected);
    }

    #[tokio::test]
    async fn scan_count_hint_does_not_change_the_result_set() {
        let (storage, _dir) = open_storage(3);
        let mut expected = HashSet::new();
        for i in 0..30 {
            let key = format!("k{i}").into_bytes();
            storage.set(&key, b"v").unwrap();
            expected.insert(key);
        }

        for count in [1usize, 3, 10, 100] {
            assert_eq!(
                as_set(scan_all(&storage, count, None, b"*")),
                expected,
                "COUNT={count} changed the full result set"
            );
        }
    }

    #[tokio::test]
    async fn scan_match_filters_by_glob() {
        let (storage, _dir) = open_storage(3);
        storage.set(b"user:1", b"v").unwrap();
        storage.set(b"user:2", b"v").unwrap();
        storage.set(b"admin:1", b"v").unwrap();

        assert_eq!(
            as_set(scan_all(&storage, 2, None, b"user:*")),
            HashSet::from([b"user:1".to_vec(), b"user:2".to_vec()])
        );
    }

    #[tokio::test]
    async fn scan_type_filters_by_data_type() {
        let (storage, _dir) = open_storage(3);
        storage.set(b"a_string", b"v").unwrap();
        storage.hset(b"a_hash", b"f", b"v").unwrap();
        storage.sadd(b"a_set", &[b"m"]).unwrap();
        storage
            .zadd(b"a_zset", &[ZsetScoreMember::new(1.0, b"m".to_vec())])
            .unwrap();
        storage.lpush(b"a_list", &[b"m".to_vec()]).unwrap();

        assert_eq!(
            as_set(scan_all(&storage, 2, Some(b"string"), b"*")),
            HashSet::from([b"a_string".to_vec()])
        );
        assert_eq!(
            as_set(scan_all(&storage, 2, Some(b"hash"), b"*")),
            HashSet::from([b"a_hash".to_vec()])
        );
        assert_eq!(
            as_set(scan_all(&storage, 2, Some(b"set"), b"*")),
            HashSet::from([b"a_set".to_vec()])
        );
        assert_eq!(
            as_set(scan_all(&storage, 2, Some(b"zset"), b"*")),
            HashSet::from([b"a_zset".to_vec()])
        );
        assert_eq!(
            as_set(scan_all(&storage, 2, Some(b"list"), b"*")),
            HashSet::from([b"a_list".to_vec()])
        );
        // An unknown type matches nothing but still terminates.
        assert!(scan_all(&storage, 2, Some(b"stream"), b"*").is_empty());
    }

    #[tokio::test]
    async fn scan_empty_keyspace_completes_immediately() {
        let (storage, _dir) = open_storage(3);
        let (cursor, keys) = storage.scan(0, 10, None, b"*").unwrap();
        assert_eq!(cursor, 0);
        assert!(keys.is_empty());
    }

    #[tokio::test]
    async fn scan_nonzero_cursor_is_returned_until_complete() {
        let (storage, _dir) = open_storage(3);
        for i in 0..20 {
            storage.set(format!("k{i}").as_bytes(), b"v").unwrap();
        }
        // With COUNT=1 the first step cannot drain 20 keys across 3 instances,
        // so the cursor must be non-zero (more to come).
        let (cursor, _keys) = storage.scan(0, 1, None, b"*").unwrap();
        assert_ne!(
            cursor, 0,
            "cursor must be non-zero while iteration continues"
        );
    }
}
