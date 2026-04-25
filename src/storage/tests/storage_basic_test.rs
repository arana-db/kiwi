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

use std::sync::Arc;

use storage::storage::Storage;
use storage::{BgTask, BgTaskHandler, DataType, StorageOptions};

// This test ensures:
// - All tasks are sent successfully (no panic)
// - The worker can process tasks and exit cleanly (no deadlock)
#[tokio::test]
async fn test_bg_task_worker_concurrent() {
    let mut storage = Storage::new(1, 0);
    let (handler, receiver) = BgTaskHandler::new();
    storage.bg_task_handler = Some(Arc::new(handler));
    let storage = Arc::new(storage);

    let worker_storage = Arc::clone(&storage);
    let worker_handle = tokio::spawn(async move {
        Storage::bg_task_worker(worker_storage, receiver).await;
    });

    let handler = storage.bg_task_handler.as_ref().unwrap().clone();
    let mut handles = vec![];
    for _ in 0..10 {
        let handler_clone = handler.clone();
        handles.push(tokio::spawn(async move {
            handler_clone
                .send(BgTask::CleanAll {
                    dtype: DataType::All,
                })
                .await
                .unwrap();
        }));
        let handler_clone = handler.clone();
        handles.push(tokio::spawn(async move {
            handler_clone
                .send(BgTask::CompactRange {
                    dtype: DataType::All,
                    start: "a".into(),
                    end: "z".into(),
                })
                .await
                .unwrap();
        }));
    }
    for h in handles {
        h.await.unwrap();
    }

    handler.send(BgTask::Shutdown).await.unwrap();
    worker_handle.await.unwrap();
}

/// Test get_global_smallest_flushed_log_index returns minimum across all instances
#[tokio::test]
async fn test_get_global_smallest_flushed_log_index() {
    let db_path = tempfile::tempdir().unwrap().path().to_path_buf();
    let mut storage = Storage::new(3, 0);
    let options = Arc::new(StorageOptions::default());
    let _rx = storage.open(options, &db_path).unwrap();

    // Set different flushed states for each instance via cf_tracker
    // Instance 0: flushed = 80
    // Instance 1: flushed = 95
    // Instance 2: flushed = 90
    // Global smallest should be 80
    if let Some(tracker0) = storage.get_logindex_cf_tracker(0) {
        tracker0.set_flushed_log_index_global(80, 800);
    }
    if let Some(tracker1) = storage.get_logindex_cf_tracker(1) {
        tracker1.set_flushed_log_index_global(95, 950);
    }
    if let Some(tracker2) = storage.get_logindex_cf_tracker(2) {
        tracker2.set_flushed_log_index_global(90, 900);
    }

    let smallest = storage.get_global_smallest_flushed_log_index();
    assert_eq!(
        smallest, 80,
        "Global smallest should be the minimum flushed index across all instances"
    );
}

/// Test get_global_smallest_flushed_log_index returns 0 when no flush has occurred
#[tokio::test]
async fn test_get_global_smallest_flushed_log_index_initial_state() {
    let db_path = tempfile::tempdir().unwrap().path().to_path_buf();
    let mut storage = Storage::new(2, 0);
    let options = Arc::new(StorageOptions::default());
    let _rx = storage.open(options, &db_path).unwrap();

    // Initial state - no flushes, should return 0
    let smallest = storage.get_global_smallest_flushed_log_index();
    assert_eq!(
        smallest, 0,
        "Initial state should return 0 when no flushes have occurred"
    );
}
