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
mod keys_order_test {
    use std::sync::Arc;

    use storage::{StorageOptions, storage::Storage};

    #[tokio::test]
    async fn keys_are_globally_sorted_across_instances() {
        let dir = tempfile::tempdir().unwrap();
        let mut storage = Storage::new(3, 0);
        let _rx = storage
            .open(Arc::new(StorageOptions::default()), dir.path())
            .unwrap();

        // Insert in a deliberately unsorted order; keys hash to different
        // instances, so a per-instance concatenation would not be sorted.
        let inserted = [
            "delta", "alpha", "omega", "bravo", "kilo", "echo", "zulu", "charlie", "mike", "golf",
        ];
        for key in inserted {
            storage.set(key.as_bytes(), b"v").unwrap();
        }

        let keys = storage.keys(b"*").unwrap();

        let mut expected: Vec<Vec<u8>> = inserted.iter().map(|k| k.as_bytes().to_vec()).collect();
        expected.sort();

        assert_eq!(keys, expected, "KEYS must return globally sorted results");
    }
}
