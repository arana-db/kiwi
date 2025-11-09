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

//! Concurrent operation tests for Raft storage and state machine
//!
//! These tests verify that:
//! 1. Concurrent reads don't block each other
//! 2. Concurrent writes are properly serialized
//! 3. The locking strategy prevents deadlocks
//! 4. Performance is acceptable under concurrent load

use std::sync::Arc;
use std::time::Duration;
use tokio::time::Instant;

use crate::state_machine::KiwiStateMachine;
use crate::storage::RaftStorage;
use crate::types::{ClientRequest, ConsistencyLevel, NodeId, RedisCommand, RequestId};
use bytes::Bytes;
use tempfile::TempDir;

#[tokio::test]
async fn test_concurrent_storage_reads() {
    // Create storage
    let temp_dir = TempDir::new().unwrap();
    let storage = Arc::new(RaftStorage::new(temp_dir.path()).unwrap());

    // Set initial state
    storage.set_current_term(5).unwrap();
    storage.set_voted_for(Some(1)).unwrap();
    storage.set_last_applied(100).unwrap();

    // Spawn multiple concurrent readers
    let mut handles = vec![];
    let num_readers = 50;

    let start = Instant::now();

    for _ in 0..num_readers {
        let storage_clone = storage.clone();
        let handle = tokio::spawn(async move {
            // Each reader performs multiple reads
            for _ in 0..100 {
                let term = storage_clone.get_current_term();
                let voted_for = storage_clone.get_voted_for();
                let last_applied = storage_clone.get_last_applied();

                // Verify consistency
                assert_eq!(term, 5);
                assert_eq!(voted_for, Some(1));
                assert_eq!(last_applied, 100);
            }
        });
        handles.push(handle);
    }

    // Wait for all readers to complete
    for handle in handles {
        handle.await.unwrap();
    }

    let elapsed = start.elapsed();
    println!(
        "Concurrent reads: {} readers x 100 reads = {} total reads in {:?}",
        num_readers,
        num_readers * 100,
        elapsed
    );

    // Verify reads completed quickly (should be < 1 second for 5000 reads)
    assert!(
        elapsed < Duration::from_secs(1),
        "Concurrent reads took too long: {:?}",
        elapsed
    );
}

#[tokio::test]
async fn test_concurrent_storage_writes() {
    // Create storage
    let temp_dir = TempDir::new().unwrap();
    let storage = Arc::new(RaftStorage::new(temp_dir.path()).unwrap());

    // Spawn multiple concurrent writers
    let mut handles = vec![];
    let num_writers = 10;

    let start = Instant::now();

    for i in 0..num_writers {
        let storage_clone = storage.clone();
        let handle = tokio::spawn(async move {
            // Each writer performs multiple writes
            for j in 0..10 {
                let term = (i * 10 + j) as u64;
                storage_clone.set_current_term(term).unwrap();
            }
        });
        handles.push(handle);
    }

    // Wait for all writers to complete
    for handle in handles {
        handle.await.unwrap();
    }

    let elapsed = start.elapsed();
    println!(
        "Concurrent writes: {} writers x 10 writes = {} total writes in {:?}",
        num_writers,
        num_writers * 10,
        elapsed
    );

    // Verify final state is consistent (one of the written values)
    let final_term = storage.get_current_term();
    assert!(final_term < 100, "Final term should be < 100");
}

#[tokio::test]
async fn test_concurrent_state_machine_applies() {
    // Create state machine
    let state_machine = Arc::new(KiwiStateMachine::new(1));

    // Spawn multiple concurrent apply operations
    let mut handles = vec![];
    let num_appliers = 10;

    let start = Instant::now();

    for i in 0..num_appliers {
        let sm_clone = state_machine.clone();
        let handle = tokio::spawn(async move {
            // Each applier performs multiple applies
            for j in 0..10 {
                let request = ClientRequest {
                    id: RequestId::new(),
                    command: RedisCommand::new(
                        "SET".to_string(),
                        vec![
                            Bytes::from(format!("key-{}-{}", i, j)),
                            Bytes::from(format!("value-{}-{}", i, j)),
                        ],
                    ),
                    consistency_level: ConsistencyLevel::Linearizable,
                };

                let response = sm_clone.apply_redis_command(&request).await.unwrap();
                assert!(response.result.is_ok());
            }
        });
        handles.push(handle);
    }

    // Wait for all appliers to complete
    for handle in handles {
        handle.await.unwrap();
    }

    let elapsed = start.elapsed();
    println!(
        "Concurrent applies: {} appliers x 10 applies = {} total applies in {:?}",
        num_appliers,
        num_appliers * 10,
        elapsed
    );

    // Verify applies completed in reasonable time
    assert!(
        elapsed < Duration::from_secs(5),
        "Concurrent applies took too long: {:?}",
        elapsed
    );
}

#[tokio::test]
async fn test_concurrent_snapshot_operations() {
    // Create state machine
    let state_machine = Arc::new(KiwiStateMachine::new(1));

    // Spawn concurrent snapshot readers and one writer
    let mut handles = vec![];

    // Spawn snapshot writer
    let sm_clone = state_machine.clone();
    let writer_handle = tokio::spawn(async move {
        for i in 0..5 {
            tokio::time::sleep(Duration::from_millis(10)).await;
            let snapshot = sm_clone.create_snapshot().await.unwrap();
            assert_eq!(snapshot.applied_index, 0);
            println!("Created snapshot {}", i);
        }
    });
    handles.push(writer_handle);

    // Spawn multiple snapshot readers
    for i in 0..20 {
        let sm_clone = state_machine.clone();
        let handle = tokio::spawn(async move {
            for j in 0..10 {
                let snapshot = sm_clone.get_current_snapshot_data().await.unwrap();
                // Snapshot may or may not exist depending on timing
                if let Some(snap) = snapshot {
                    assert_eq!(snap.applied_index, 0);
                }
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
            println!("Reader {} completed", i);
        });
        handles.push(handle);
    }

    // Wait for all operations to complete
    for handle in handles {
        handle.await.unwrap();
    }
}

#[tokio::test]
async fn test_no_deadlock_under_load() {
    // Create state machine
    let state_machine = Arc::new(KiwiStateMachine::new(1));

    // Spawn many concurrent operations of different types
    let mut handles = vec![];

    // Appliers
    for i in 0..10 {
        let sm_clone = state_machine.clone();
        let handle = tokio::spawn(async move {
            for j in 0..20 {
                let request = ClientRequest {
                    id: RequestId::new(),
                    command: RedisCommand::new(
                        "SET".to_string(),
                        vec![
                            Bytes::from(format!("key-{}-{}", i, j)),
                            Bytes::from(format!("value-{}-{}", i, j)),
                        ],
                    ),
                    consistency_level: ConsistencyLevel::Linearizable,
                };
                sm_clone.apply_redis_command(&request).await.unwrap();
            }
        });
        handles.push(handle);
    }

    // Snapshot creators
    for _ in 0..3 {
        let sm_clone = state_machine.clone();
        let handle = tokio::spawn(async move {
            for _ in 0..5 {
                tokio::time::sleep(Duration::from_millis(20)).await;
                sm_clone.create_snapshot().await.unwrap();
            }
        });
        handles.push(handle);
    }

    // Snapshot readers
    for _ in 0..10 {
        let sm_clone = state_machine.clone();
        let handle = tokio::spawn(async move {
            for _ in 0..50 {
                sm_clone.get_current_snapshot_data().await.unwrap();
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        });
        handles.push(handle);
    }

    // Wait for all operations with timeout to detect deadlocks
    let timeout_duration = Duration::from_secs(30);
    let start = Instant::now();

    for handle in handles {
        tokio::time::timeout(timeout_duration, handle)
            .await
            .expect("Operation timed out - possible deadlock!")
            .unwrap();
    }

    let elapsed = start.elapsed();
    println!("All concurrent operations completed in {:?}", elapsed);

    // Verify no deadlock occurred (all operations completed within timeout)
    assert!(
        elapsed < timeout_duration,
        "Operations took too long, possible deadlock"
    );
}

#[tokio::test]
async fn test_lock_performance_impact() {
    // Create state machine
    let state_machine = Arc::new(KiwiStateMachine::new(1));

    // Measure sequential performance
    let start = Instant::now();
    for i in 0..100 {
        let request = ClientRequest {
            id: RequestId::new(),
            command: RedisCommand::new(
                "SET".to_string(),
                vec![
                    Bytes::from(format!("key-{}", i)),
                    Bytes::from(format!("value-{}", i)),
                ],
            ),
            consistency_level: ConsistencyLevel::Linearizable,
        };
        state_machine.apply_redis_command(&request).await.unwrap();
    }
    let sequential_time = start.elapsed();

    // Measure concurrent performance (10 concurrent tasks, 10 operations each)
    let start = Instant::now();
    let mut handles = vec![];
    for i in 0..10 {
        let sm_clone = state_machine.clone();
        let handle = tokio::spawn(async move {
            for j in 0..10 {
                let request = ClientRequest {
                    id: RequestId::new(),
                    command: RedisCommand::new(
                        "SET".to_string(),
                        vec![
                            Bytes::from(format!("key-{}-{}", i, j)),
                            Bytes::from(format!("value-{}-{}", i, j)),
                        ],
                    ),
                    consistency_level: ConsistencyLevel::Linearizable,
                };
                sm_clone.apply_redis_command(&request).await.unwrap();
            }
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.await.unwrap();
    }
    let concurrent_time = start.elapsed();

    println!("Sequential time (100 ops): {:?}", sequential_time);
    println!("Concurrent time (10x10 ops): {:?}", concurrent_time);

    // Concurrent should not be significantly slower than sequential
    // (allowing for some overhead from task spawning and coordination)
    let overhead_ratio = concurrent_time.as_secs_f64() / sequential_time.as_secs_f64();
    println!("Overhead ratio: {:.2}x", overhead_ratio);

    // Verify overhead is reasonable (less than 3x)
    assert!(
        overhead_ratio < 3.0,
        "Lock overhead is too high: {:.2}x",
        overhead_ratio
    );
}
