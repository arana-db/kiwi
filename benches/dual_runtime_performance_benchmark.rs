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

//! Comprehensive performance benchmarks for dual runtime architecture
//! 
//! This benchmark suite tests:
//! - Single vs dual runtime performance comparison
//! - Throughput and latency measurements under various loads
//! - Performance during RocksDB blocking scenarios (compaction, write stall)
//! - Load testing with multiple concurrent clients
//! - Network isolation during storage operations

use std::sync::Arc;
use std::time::{Duration, Instant};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, AtomicBool, Ordering};

use tokio::sync::{mpsc, Barrier, Semaphore};
use tokio::time::{sleep, timeout};
use log::{info, warn, debug};
use rand::{Rng, thread_rng};

// Import dual runtime components
use runtime::{RuntimeManager, RuntimeConfig, MessageChannel, StorageClient as RuntimeStorageClient};
use net::{NetworkServer, StorageClient};
use storage::storage::Storage;
use cmd::table::create_command_table;
use executor::CmdExecutorBuilder;
use client::Client;
use resp::RespData;

/// Benchmark configuration for dual runtime performance tests
#[derive(Debug, Clone)]
pub struct DualRuntimeBenchmarkConfig {
    /// Duration of each benchmark test
    pub test_duration: Duration,
    /// Number of concurrent clients for load testing
    pub concurrent_clients: usize,
    /// Operations per client during the test
    pub operations_per_client: usize,
    /// Warmup duration before starting measurements
    pub warmup_duration: Duration,
    /// Size of data values in bytes
    pub data_size_bytes: usize,
    /// Percentage of read operations (vs writes)
    pub read_percentage: f64,
    /// Enable RocksDB blocking simulation
    pub simulate_rocksdb_blocking: bool,
    /// Duration of simulated blocking operations
    pub blocking_duration: Duration,
    /// Network runtime thread count for testing
    pub network_threads: usize,
    /// Storage runtime thread count for testing
    pub storage_threads: usize,
}

impl Default for DualRuntimeBenchmarkConfig {
    fn default() -> Self {
        Self {
            test_duration: Duration::from_secs(60),
            concurrent_clients: 100,
            operations_per_client: 10000,
            warmup_duration: Duration::from_secs(10),
            data_size_bytes: 1024,
            read_percentage: 0.7, // 70% reads, 30% writes
            simulate_rocksdb_blocking: false,
            blocking_duration: Duration::from_millis(100),
            network_threads: 4,
            storage_threads: 8,
        }
    }
}

/// Performance metrics collected during benchmarks
#[derive(Debug, Clone)]
pub struct PerformanceMetrics {
    pub test_name: String,
    pub total_operations: u64,
    pub successful_operations: u64,
    pub failed_operations: u64,
    pub operations_per_second: f64,
    pub average_latency_ms: f64,
    pub p50_latency_ms: f64,
    pub p95_latency_ms: f64,
    pub p99_latency_ms: f64,
    pub max_latency_ms: f64,
    pub min_latency_ms: f64,
    pub test_duration: Duration,
    pub memory_usage_mb: f64,
    pub cpu_usage_percent: f64,
    pub network_isolation_score: f64, // How well network remained responsive during storage blocking
    pub throughput_improvement_percent: f64, // Improvement over single runtime
}

/// Benchmark results aggregator
pub struct BenchmarkResults {
    pub single_runtime_results: Vec<PerformanceMetrics>,
    pub dual_runtime_results: Vec<PerformanceMetrics>,
    pub comparison_summary: ComparisonSummary,
}

/// Summary of performance comparison between single and dual runtime
#[derive(Debug, Clone)]
pub struct ComparisonSummary {
    pub throughput_improvement: f64,
    pub latency_improvement: f64,
    pub network_isolation_improvement: f64,
    pub memory_overhead: f64,
    pub cpu_overhead: f64,
    pub recommendation: String,
}

/// Main benchmark runner for dual runtime performance tests
pub struct DualRuntimeBenchmarkRunner {
    config: DualRuntimeBenchmarkConfig,
    storage: Arc<Storage>,
}

impl DualRuntimeBenchmarkRunner {
    /// Create a new benchmark runner with the given configuration
    pub async fn new(config: DualRuntimeBenchmarkConfig) -> Result<Self, Box<dyn std::error::Error>> {
        // Create storage instance for testing
        let storage = Arc::new(Storage::new()?);
        
        Ok(Self {
            config,
            storage,
        })
    }

    /// Run comprehensive dual runtime performance benchmarks
    pub async fn run_comprehensive_benchmarks(&self) -> Result<BenchmarkResults, Box<dyn std::error::Error>> {
        info!("Starting comprehensive dual runtime performance benchmarks");
        
        let mut single_runtime_results = Vec::new();
        let mut dual_runtime_results = Vec::new();

        // 1. Basic throughput comparison
        info!("Running basic throughput comparison...");
        let single_throughput = self.benchmark_single_runtime_throughput().await?;
        let dual_throughput = self.benchmark_dual_runtime_throughput().await?;
        single_runtime_results.push(single_throughput);
        dual_runtime_results.push(dual_throughput);

        // 2. Latency comparison under load
        info!("Running latency comparison under load...");
        let single_latency = self.benchmark_single_runtime_latency().await?;
        let dual_latency = self.benchmark_dual_runtime_latency().await?;
        single_runtime_results.push(single_latency);
        dual_runtime_results.push(dual_latency);

        // 3. Performance during RocksDB blocking scenarios
        info!("Running RocksDB blocking scenario tests...");
        let single_blocking = self.benchmark_single_runtime_with_blocking().await?;
        let dual_blocking = self.benchmark_dual_runtime_with_blocking().await?;
        single_runtime_results.push(single_blocking);
        dual_runtime_results.push(dual_blocking);

        // 4. Concurrent client load testing
        info!("Running concurrent client load tests...");
        let single_concurrent = self.benchmark_single_runtime_concurrent_load().await?;
        let dual_concurrent = self.benchmark_dual_runtime_concurrent_load().await?;
        single_runtime_results.push(single_concurrent);
        dual_runtime_results.push(dual_concurrent);

        // 5. Network isolation testing
        info!("Running network isolation tests...");
        let dual_isolation = self.benchmark_network_isolation().await?;
        dual_runtime_results.push(dual_isolation);

        // Generate comparison summary
        let comparison_summary = self.generate_comparison_summary(&single_runtime_results, &dual_runtime_results);

        Ok(BenchmarkResults {
            single_runtime_results,
            dual_runtime_results,
            comparison_summary,
        })
    }

    /// Benchmark single runtime throughput (baseline)
    async fn benchmark_single_runtime_throughput(&self) -> Result<PerformanceMetrics, Box<dyn std::error::Error>> {
        info!("Benchmarking single runtime throughput");
        
        // Create single runtime setup (simulated)
        let start_time = Instant::now();
        let operations = Arc::new(AtomicU64::new(0));
        let successful_ops = Arc::new(AtomicU64::new(0));
        let failed_ops = Arc::new(AtomicU64::new(0));
        let latencies = Arc::new(tokio::sync::Mutex::new(Vec::new()));

        // Simulate single runtime workload
        let mut handles = Vec::new();
        for _ in 0..self.config.concurrent_clients {
            let operations = Arc::clone(&operations);
            let successful_ops = Arc::clone(&successful_ops);
            let failed_ops = Arc::clone(&failed_ops);
            let latencies = Arc::clone(&latencies);
            let storage = Arc::clone(&self.storage);
            let config = self.config.clone();

            let handle = tokio::spawn(async move {
                Self::simulate_single_runtime_client(
                    storage, operations, successful_ops, failed_ops, latencies, config
                ).await;
            });
            handles.push(handle);
        }

        // Wait for test duration
        sleep(self.config.test_duration).await;

        // Wait for all clients to finish
        for handle in handles {
            let _ = handle.await;
        }

        let test_duration = start_time.elapsed();
        let total_ops = operations.load(Ordering::Relaxed);
        let successful = successful_ops.load(Ordering::Relaxed);
        let failed = failed_ops.load(Ordering::Relaxed);

        let latency_vec = latencies.lock().await.clone();
        let metrics = Self::calculate_performance_metrics(
            "Single Runtime Throughput".to_string(),
            total_ops,
            successful,
            failed,
            latency_vec,
            test_duration,
            0.0, // No network isolation score for single runtime
        );

        Ok(metrics)
    }

    /// Benchmark dual runtime throughput
    async fn benchmark_dual_runtime_throughput(&self) -> Result<PerformanceMetrics, Box<dyn std::error::Error>> {
        info!("Benchmarking dual runtime throughput");
        
        // Create dual runtime setup
        let runtime_config = RuntimeConfig::new(
            self.config.network_threads,
            self.config.storage_threads,
            10000,
            Duration::from_secs(30),
            100,
            Duration::from_millis(10),
        )?;

        let mut runtime_manager = RuntimeManager::new(runtime_config)?;
        runtime_manager.start().await?;

        // Create message channel and storage client
        let message_channel = Arc::new(MessageChannel::new(10000));
        let runtime_storage_client = Arc::new(RuntimeStorageClient::new(
            message_channel,
            Duration::from_secs(30),
        ));
        let storage_client = Arc::new(StorageClient::new(runtime_storage_client));

        let start_time = Instant::now();
        let operations = Arc::new(AtomicU64::new(0));
        let successful_ops = Arc::new(AtomicU64::new(0));
        let failed_ops = Arc::new(AtomicU64::new(0));
        let latencies = Arc::new(tokio::sync::Mutex::new(Vec::new()));

        // Simulate dual runtime workload
        let mut handles = Vec::new();
        for _ in 0..self.config.concurrent_clients {
            let operations = Arc::clone(&operations);
            let successful_ops = Arc::clone(&successful_ops);
            let failed_ops = Arc::clone(&failed_ops);
            let latencies = Arc::clone(&latencies);
            let storage_client = Arc::clone(&storage_client);
            let config = self.config.clone();

            let handle = tokio::spawn(async move {
                Self::simulate_dual_runtime_client(
                    storage_client, operations, successful_ops, failed_ops, latencies, config
                ).await;
            });
            handles.push(handle);
        }

        // Wait for test duration
        sleep(self.config.test_duration).await;

        // Wait for all clients to finish
        for handle in handles {
            let _ = handle.await;
        }

        let test_duration = start_time.elapsed();
        let total_ops = operations.load(Ordering::Relaxed);
        let successful = successful_ops.load(Ordering::Relaxed);
        let failed = failed_ops.load(Ordering::Relaxed);

        let latency_vec = latencies.lock().await.clone();
        let mut metrics = Self::calculate_performance_metrics(
            "Dual Runtime Throughput".to_string(),
            total_ops,
            successful,
            failed,
            latency_vec,
            test_duration,
            0.0, // Network isolation score calculated separately
        );

        // Calculate throughput improvement
        // This would be calculated by comparing with single runtime results
        metrics.throughput_improvement_percent = 0.0; // Placeholder

        runtime_manager.stop().await?;
        Ok(metrics)
    }

    /// Benchmark single runtime latency under load
    async fn benchmark_single_runtime_latency(&self) -> Result<PerformanceMetrics, Box<dyn std::error::Error>> {
        info!("Benchmarking single runtime latency under load");
        
        let start_time = Instant::now();
        let operations = Arc::new(AtomicU64::new(0));
        let successful_ops = Arc::new(AtomicU64::new(0));
        let failed_ops = Arc::new(AtomicU64::new(0));
        let latencies = Arc::new(tokio::sync::Mutex::new(Vec::new()));

        // Create high load scenario
        let high_load_config = DualRuntimeBenchmarkConfig {
            concurrent_clients: self.config.concurrent_clients * 2, // Double the load
            ..self.config.clone()
        };

        // Simulate single runtime under high load
        let mut handles = Vec::new();
        for _ in 0..high_load_config.concurrent_clients {
            let operations = Arc::clone(&operations);
            let successful_ops = Arc::clone(&successful_ops);
            let failed_ops = Arc::clone(&failed_ops);
            let latencies = Arc::clone(&latencies);
            let storage = Arc::clone(&self.storage);
            let config = high_load_config.clone();

            let handle = tokio::spawn(async move {
                Self::simulate_single_runtime_client(
                    storage, operations, successful_ops, failed_ops, latencies, config
                ).await;
            });
            handles.push(handle);
        }

        // Wait for test duration
        sleep(self.config.test_duration).await;

        // Wait for all clients to finish
        for handle in handles {
            let _ = handle.await;
        }

        let test_duration = start_time.elapsed();
        let total_ops = operations.load(Ordering::Relaxed);
        let successful = successful_ops.load(Ordering::Relaxed);
        let failed = failed_ops.load(Ordering::Relaxed);

        let latency_vec = latencies.lock().await.clone();
        let metrics = Self::calculate_performance_metrics(
            "Single Runtime High Load Latency".to_string(),
            total_ops,
            successful,
            failed,
            latency_vec,
            test_duration,
            0.0,
        );

        Ok(metrics)
    }

    /// Benchmark dual runtime latency under load
    async fn benchmark_dual_runtime_latency(&self) -> Result<PerformanceMetrics, Box<dyn std::error::Error>> {
        info!("Benchmarking dual runtime latency under load");
        
        // Create dual runtime setup
        let runtime_config = RuntimeConfig::new(
            self.config.network_threads,
            self.config.storage_threads,
            10000,
            Duration::from_secs(30),
            100,
            Duration::from_millis(10),
        )?;

        let mut runtime_manager = RuntimeManager::new(runtime_config)?;
        runtime_manager.start().await?;

        // Create message channel and storage client
        let message_channel = Arc::new(MessageChannel::new(10000));
        let runtime_storage_client = Arc::new(RuntimeStorageClient::new(
            message_channel,
            Duration::from_secs(30),
        ));
        let storage_client = Arc::new(StorageClient::new(runtime_storage_client));

        let start_time = Instant::now();
        let operations = Arc::new(AtomicU64::new(0));
        let successful_ops = Arc::new(AtomicU64::new(0));
        let failed_ops = Arc::new(AtomicU64::new(0));
        let latencies = Arc::new(tokio::sync::Mutex::new(Vec::new()));

        // Create high load scenario
        let high_load_config = DualRuntimeBenchmarkConfig {
            concurrent_clients: self.config.concurrent_clients * 2, // Double the load
            ..self.config.clone()
        };

        // Simulate dual runtime under high load
        let mut handles = Vec::new();
        for _ in 0..high_load_config.concurrent_clients {
            let operations = Arc::clone(&operations);
            let successful_ops = Arc::clone(&successful_ops);
            let failed_ops = Arc::clone(&failed_ops);
            let latencies = Arc::clone(&latencies);
            let storage_client = Arc::clone(&storage_client);
            let config = high_load_config.clone();

            let handle = tokio::spawn(async move {
                Self::simulate_dual_runtime_client(
                    storage_client, operations, successful_ops, failed_ops, latencies, config
                ).await;
            });
            handles.push(handle);
        }

        // Wait for test duration
        sleep(self.config.test_duration).await;

        // Wait for all clients to finish
        for handle in handles {
            let _ = handle.await;
        }

        let test_duration = start_time.elapsed();
        let total_ops = operations.load(Ordering::Relaxed);
        let successful = successful_ops.load(Ordering::Relaxed);
        let failed = failed_ops.load(Ordering::Relaxed);

        let latency_vec = latencies.lock().await.clone();
        let metrics = Self::calculate_performance_metrics(
            "Dual Runtime High Load Latency".to_string(),
            total_ops,
            successful,
            failed,
            latency_vec,
            test_duration,
            0.0,
        );

        runtime_manager.stop().await?;
        Ok(metrics)
    }
}