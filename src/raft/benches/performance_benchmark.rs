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

//! Performance benchmarks for Raft implementation
//!
//! This benchmark suite measures:
//! - Throughput (operations per second)
//! - Latency (P50, P95, P99)
//! - Adaptor layer overhead
//!
//! Target metrics:
//! - Throughput: > 10,000 ops/sec
//! - P99 Latency: < 10ms
//! - Adaptor overhead: < 1ms

use bytes::Bytes;
use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use raft::error::RaftResult;
use raft::state_machine::{KiwiStateMachine, StorageEngine};
use raft::types::{ClientRequest, ConsistencyLevel, RedisCommand, RequestId};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::runtime::Runtime;
use tokio::sync::RwLock;

/// Simple in-memory storage engine for benchmarking
pub struct BenchStorageEngine {
    data: Arc<RwLock<HashMap<Vec<u8>, Vec<u8>>>>,
}

impl BenchStorageEngine {
    pub fn new() -> Self {
        Self {
            data: Arc::new(RwLock::new(HashMap::new())),
        }
    }
}

#[async_trait::async_trait]
impl StorageEngine for BenchStorageEngine {
    async fn get(&self, key: &[u8]) -> RaftResult<Option<Vec<u8>>> {
        let data = self.data.read().await;
        Ok(data.get(key).cloned())
    }

    async fn put(&self, key: &[u8], value: &[u8]) -> RaftResult<()> {
        let mut data = self.data.write().await;
        data.insert(key.to_vec(), value.to_vec());
        Ok(())
    }

    async fn delete(&self, key: &[u8]) -> RaftResult<()> {
        let mut data = self.data.write().await;
        data.remove(key);
        Ok(())
    }

    async fn create_snapshot(&self) -> RaftResult<Vec<u8>> {
        let data = self.data.read().await;
        let entries: Vec<_> = data.iter().map(|(k, v)| (k, v)).collect();
        let serialized = serde_json::to_vec(&entries)
            .map_err(|e| raft::error::RaftError::state_machine(format!("Serialization error: {}", e)))?;
        Ok(serialized)
    }

    async fn restore_from_snapshot(&self, snapshot_data: &[u8]) -> RaftResult<()> {
        let restored_entries: Vec<(Vec<u8>, Vec<u8>)> = serde_json::from_slice(snapshot_data)
            .map_err(|e| raft::error::RaftError::state_machine(format!("Deserialization error: {}", e)))?;
        let mut data = self.data.write().await;
        data.clear();
        for (key, value) in restored_entries {
            data.insert(key, value);
        }
        Ok(())
    }

    async fn batch_put(&self, operations: Vec<(Vec<u8>, Vec<u8>)>) -> RaftResult<()> {
        let mut data = self.data.write().await;
        for (key, value) in operations {
            data.insert(key, value);
        }
        Ok(())
    }

    async fn batch_delete(&self, keys: Vec<Vec<u8>>) -> RaftResult<()> {
        let mut data = self.data.write().await;
        for key in keys {
            data.remove(&key);
        }
        Ok(())
    }
}

/// Benchmark single command application
fn bench_single_command(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let storage = Arc::new(BenchStorageEngine::new());
    let state_machine = Arc::new(KiwiStateMachine::with_storage_engine(1, storage));

    let mut group = c.benchmark_group("single_command");
    group.throughput(Throughput::Elements(1));

    group.bench_function("set_command", |b| {
        b.to_async(&rt).iter(|| async {
            let request = ClientRequest {
                id: RequestId::new(),
                command: RedisCommand::new(
                    "SET".to_string(),
                    vec![Bytes::from("key"), Bytes::from("value")],
                ),
                consistency_level: ConsistencyLevel::Linearizable,
            };
            black_box(state_machine.apply_redis_command(&request).await.unwrap());
        });
    });

    group.bench_function("get_command", |b| {
        b.to_async(&rt).iter(|| async {
            let request = ClientRequest {
                id: RequestId::new(),
                command: RedisCommand::new("GET".to_string(), vec![Bytes::from("key")]),
                consistency_level: ConsistencyLevel::Linearizable,
            };
            black_box(state_machine.apply_redis_command(&request).await.unwrap());
        });
    });

    group.finish();
}

/// Benchmark batch command application
fn bench_batch_commands(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let storage = Arc::new(BenchStorageEngine::new());
    let state_machine = Arc::new(KiwiStateMachine::with_storage_engine(1, storage));

    let mut group = c.benchmark_group("batch_commands");

    for batch_size in [10, 50, 100, 500].iter() {
        group.throughput(Throughput::Elements(*batch_size as u64));

        group.bench_with_input(
            BenchmarkId::from_parameter(batch_size),
            batch_size,
            |b, &size| {
                b.to_async(&rt).iter(|| async {
                    let requests: Vec<_> = (0..size)
                        .map(|i| ClientRequest {
                            id: RequestId::new(),
                            command: RedisCommand::new(
                                "SET".to_string(),
                                vec![
                                    Bytes::from(format!("key{}", i)),
                                    Bytes::from(format!("value{}", i)),
                                ],
                            ),
                            consistency_level: ConsistencyLevel::Linearizable,
                        })
                        .collect();

                    black_box(
                        state_machine
                            .apply_redis_commands_batch(&requests)
                            .await
                            .unwrap(),
                    );
                });
            },
        );
    }

    group.finish();
}

/// Benchmark throughput over time
fn bench_throughput(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let storage = Arc::new(BenchStorageEngine::new());
    let state_machine = Arc::new(KiwiStateMachine::with_storage_engine(1, storage));

    let mut group = c.benchmark_group("throughput");
    group.measurement_time(Duration::from_secs(10));
    group.sample_size(100);

    group.bench_function("sustained_writes", |b| {
        let mut counter = 0;
        b.to_async(&rt).iter(|| async {
            counter += 1;
            let request = ClientRequest {
                id: RequestId::new(),
                command: RedisCommand::new(
                    "SET".to_string(),
                    vec![
                        Bytes::from(format!("key{}", counter)),
                        Bytes::from(format!("value{}", counter)),
                    ],
                ),
                consistency_level: ConsistencyLevel::Linearizable,
            };
            black_box(state_machine.apply_redis_command(&request).await.unwrap());
        });
    });

    group.finish();
}

/// Benchmark latency distribution
fn bench_latency(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let storage = Arc::new(BenchStorageEngine::new());
    let state_machine = Arc::new(KiwiStateMachine::with_storage_engine(1, storage));

    let mut group = c.benchmark_group("latency");
    group.sample_size(1000);

    group.bench_function("write_latency", |b| {
        let mut counter = 0;
        b.to_async(&rt).iter(|| async {
            counter += 1;
            let start = std::time::Instant::now();
            let request = ClientRequest {
                id: RequestId::new(),
                command: RedisCommand::new(
                    "SET".to_string(),
                    vec![
                        Bytes::from(format!("key{}", counter)),
                        Bytes::from(format!("value{}", counter)),
                    ],
                ),
                consistency_level: ConsistencyLevel::Linearizable,
            };
            state_machine.apply_redis_command(&request).await.unwrap();
            black_box(start.elapsed());
        });
    });

    group.finish();
}

/// Benchmark adaptor overhead
fn bench_adaptor_overhead(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let storage = Arc::new(BenchStorageEngine::new());
    let state_machine = Arc::new(KiwiStateMachine::with_storage_engine(1, storage.clone()));

    let mut group = c.benchmark_group("adaptor_overhead");

    // Direct storage access (baseline)
    group.bench_function("direct_storage", |b| {
        b.to_async(&rt).iter(|| async {
            black_box(
                storage
                    .put(b"test_key", b"test_value")
                    .await
                    .unwrap(),
            );
        });
    });

    // Through state machine (with adaptor)
    group.bench_function("through_state_machine", |b| {
        b.to_async(&rt).iter(|| async {
            let request = ClientRequest {
                id: RequestId::new(),
                command: RedisCommand::new(
                    "SET".to_string(),
                    vec![Bytes::from("test_key"), Bytes::from("test_value")],
                ),
                consistency_level: ConsistencyLevel::Linearizable,
            };
            black_box(state_machine.apply_redis_command(&request).await.unwrap());
        });
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_single_command,
    bench_batch_commands,
    bench_throughput,
    bench_latency,
    bench_adaptor_overhead
);
criterion_main!(benches);
