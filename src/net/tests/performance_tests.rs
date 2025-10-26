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

use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use tokio::sync::mpsc;
use tokio::time::timeout;

use crate::buffer::{BufferConfig, BufferManager, BufferedReader};
use crate::pipeline::{CommandPipeline, PipelineConfig};
use crate::pool::{ConnectionPool, PoolConfig};

/// Performance test results
#[derive(Debug, Clone)]
pub struct PerformanceResults {
    pub test_name: String,
    pub operations_per_second: f64,
    pub average_latency_ms: f64,
    pub p95_latency_ms: f64,
    pub p99_latency_ms: f64,
    pub total_operations: usize,
    pub total_duration_ms: u64,
    pub success_rate: f64,
}

/// Performance test configuration
#[derive(Debug, Clone)]
pub struct PerformanceTestConfig {
    pub duration: Duration,
    pub concurrent_clients: usize,
    pub operations_per_client: usize,
    pub warmup_duration: Duration,
}

impl Default for PerformanceTestConfig {
    fn default() -> Self {
        Self {
            duration: Duration::from_secs(10),
            concurrent_clients: 10,
            operations_per_client: 1000,
            warmup_duration: Duration::from_secs(2),
        }
    }
}

/// Network performance test suite
pub struct NetworkPerformanceTests;

impl NetworkPerformanceTests {
    /// Test connection pool performance under high load
    pub async fn test_connection_pool_performance() -> PerformanceResults {
        let config = PoolConfig {
            max_connections: 100,
            connection_timeout: Duration::from_secs(1),
            idle_timeout: Duration::from_secs(60),
            min_connections: 10,
        };

        let pool = Arc::new(ConnectionPool::new(config));
        let test_config = PerformanceTestConfig::default();
        
        // Warmup
        Self::warmup_connection_pool(&pool).await;

        let start_time = Instant::now();
        let mut handles = Vec::new();
        let mut latencies = Vec::new();
        let mut successful_ops = 0;
        let mut total_ops = 0;

        // Spawn concurrent clients
        for _ in 0..test_config.concurrent_clients {
            let pool_clone = pool.clone();
            let ops_per_client = test_config.operations_per_client;
            
            let handle = tokio::spawn(async move {
                let mut client_latencies = Vec::new();
                let mut client_successful = 0;
                
                for _ in 0..ops_per_client {
                    let op_start = Instant::now();
                    
                    // Simulate getting and returning a connection
                    match pool_clone.get_connection(|| async {
                        // Simulate connection creation
                        tokio::time::sleep(Duration::from_micros(100)).await;
                        Ok(MockConnection { id: 1 })
                    }).await {
                        Ok(conn) => {
                            // Simulate some work
                            tokio::time::sleep(Duration::from_micros(50)).await;
                            pool_clone.return_connection(conn).await;
                            client_successful += 1;
                        }
                        Err(_) => {}
                    }
                    
                    client_latencies.push(op_start.elapsed());
                }
                
                (client_latencies, client_successful)
            });
            
            handles.push(handle);
        }

        // Collect results
        for handle in handles {
            if let Ok((client_latencies, client_successful)) = handle.await {
                latencies.extend(client_latencies);
                successful_ops += client_successful;
                total_ops += test_config.operations_per_client;
            }
        }

        let total_duration = start_time.elapsed();
        Self::calculate_performance_results(
            "Connection Pool Performance".to_string(),
            latencies,
            successful_ops,
            total_ops,
            total_duration,
        )
    }

    /// Test buffer manager performance
    pub async fn test_buffer_manager_performance() -> PerformanceResults {
        let config = BufferConfig {
            initial_buffer_size: 8192,
            max_buffer_size: 1024 * 1024,
            max_pool_size: 50,
            min_pool_size: 10,
            ..Default::default()
        };

        let buffer_manager = Arc::new(BufferManager::new(config));
        let test_config = PerformanceTestConfig::default();

        let start_time = Instant::now();
        let mut handles = Vec::new();
        let mut latencies = Vec::new();
        let mut successful_ops = 0;
        let mut total_ops = 0;

        // Spawn concurrent clients
        for _ in 0..test_config.concurrent_clients {
            let manager_clone = buffer_manager.clone();
            let ops_per_client = test_config.operations_per_client;
            
            let handle = tokio::spawn(async move {
                let mut client_latencies = Vec::new();
                let mut client_successful = 0;
                
                for _ in 0..ops_per_client {
                    let op_start = Instant::now();
                    
                    // Get buffer, use it, return it
                    let mut buffer = manager_clone.get_buffer().await;
                    
                    // Simulate buffer usage
                    buffer.buffer.extend_from_slice(b"Hello, World! This is test data.");
                    let _slice = manager_clone.create_bytes_slice(&buffer, 0, 5);
                    
                    manager_clone.return_buffer(buffer).await;
                    client_successful += 1;
                    
                    client_latencies.push(op_start.elapsed());
                }
                
                (client_latencies, client_successful)
            });
            
            handles.push(handle);
        }

        // Collect results
        for handle in handles {
            if let Ok((client_latencies, client_successful)) = handle.await {
                latencies.extend(client_latencies);
                successful_ops += client_successful;
                total_ops += test_config.operations_per_client;
            }
        }

        let total_duration = start_time.elapsed();
        Self::calculate_performance_results(
            "Buffer Manager Performance".to_string(),
            latencies,
            successful_ops,
            total_ops,
            total_duration,
        )
    }

    /// Test buffered reader performance
    pub async fn test_buffered_reader_performance() -> PerformanceResults {
        let buffer_manager = BufferManager::new(BufferConfig::default());
        let test_config = PerformanceTestConfig::default();

        let start_time = Instant::now();
        let mut handles = Vec::new();
        let mut latencies = Vec::new();
        let mut successful_ops = 0;
        let mut total_ops = 0;

        // Test data
        let test_data = b"GET key1\r\nSET key2 value2\r\nDEL key3\r\n";

        // Spawn concurrent readers
        for _ in 0..test_config.concurrent_clients {
            let manager_clone = buffer_manager.clone();
            let ops_per_client = test_config.operations_per_client;
            let data = test_data.clone();
            
            let handle = tokio::spawn(async move {
                let mut client_latencies = Vec::new();
                let mut client_successful = 0;
                
                for _ in 0..ops_per_client {
                    let op_start = Instant::now();
                    
                    let mut reader = BufferedReader::new(manager_clone.clone());
                    
                    // Read data
                    let bytes_read = reader.read_data(data).await;
                    if bytes_read > 0 {
                        // Process available data
                        if let Some(available) = reader.available_data() {
                            // Simulate parsing
                            let _parsed = available.len();
                            reader.consume(available.len());
                        }
                        client_successful += 1;
                    }
                    
                    reader.reset().await;
                    client_latencies.push(op_start.elapsed());
                }
                
                (client_latencies, client_successful)
            });
            
            handles.push(handle);
        }

        // Collect results
        for handle in handles {
            if let Ok((client_latencies, client_successful)) = handle.await {
                latencies.extend(client_latencies);
                successful_ops += client_successful;
                total_ops += test_config.operations_per_client;
            }
        }

        let total_duration = start_time.elapsed();
        Self::calculate_performance_results(
            "Buffered Reader Performance".to_string(),
            latencies,
            successful_ops,
            total_ops,
            total_duration,
        )
    }

    /// Test memory allocation patterns
    pub async fn test_memory_allocation_performance() -> PerformanceResults {
        let test_config = PerformanceTestConfig {
            operations_per_client: 10000,
            ..Default::default()
        };

        let start_time = Instant::now();
        let mut handles = Vec::new();
        let mut latencies = Vec::new();
        let mut successful_ops = 0;
        let mut total_ops = 0;

        // Test traditional allocation vs buffer pooling
        for client_id in 0..test_config.concurrent_clients {
            let ops_per_client = test_config.operations_per_client;
            let use_pooling = client_id % 2 == 0; // Alternate between pooling and traditional
            
            let handle = tokio::spawn(async move {
                let mut client_latencies = Vec::new();
                let mut client_successful = 0;
                
                let buffer_manager = if use_pooling {
                    Some(BufferManager::new(BufferConfig::default()))
                } else {
                    None
                };
                
                for _ in 0..ops_per_client {
                    let op_start = Instant::now();
                    
                    if let Some(ref manager) = buffer_manager {
                        // Use buffer pooling
                        let buffer = manager.get_buffer().await;
                        // Simulate usage
                        let _capacity = buffer.capacity();
                        manager.return_buffer(buffer).await;
                    } else {
                        // Traditional allocation
                        let _buffer = vec![0u8; 8192];
                        // Buffer will be dropped
                    }
                    
                    client_successful += 1;
                    client_latencies.push(op_start.elapsed());
                }
                
                (client_latencies, client_successful, use_pooling)
            });
            
            handles.push(handle);
        }

        // Collect results
        for handle in handles {
            if let Ok((client_latencies, client_successful, _use_pooling)) = handle.await {
                latencies.extend(client_latencies);
                successful_ops += client_successful;
                total_ops += test_config.operations_per_client;
            }
        }

        let total_duration = start_time.elapsed();
        Self::calculate_performance_results(
            "Memory Allocation Performance".to_string(),
            latencies,
            successful_ops,
            total_ops,
            total_duration,
        )
    }

    /// Run comprehensive network performance benchmark
    pub async fn run_comprehensive_benchmark() -> Vec<PerformanceResults> {
        println!("Running comprehensive network performance benchmark...");
        
        let mut results = Vec::new();
        
        println!("Testing connection pool performance...");
        results.push(Self::test_connection_pool_performance().await);
        
        println!("Testing buffer manager performance...");
        results.push(Self::test_buffer_manager_performance().await);
        
        println!("Testing buffered reader performance...");
        results.push(Self::test_buffered_reader_performance().await);
        
        println!("Testing memory allocation performance...");
        results.push(Self::test_memory_allocation_performance().await);
        
        results
    }

    /// Print performance results in a formatted table
    pub fn print_results(results: &[PerformanceResults]) {
        println!("\n{:=<100}", "");
        println!("{:^100}", "NETWORK PERFORMANCE BENCHMARK RESULTS");
        println!("{:=<100}", "");
        
        println!(
            "{:<30} {:>12} {:>12} {:>12} {:>12} {:>12}",
            "Test Name", "Ops/Sec", "Avg (ms)", "P95 (ms)", "P99 (ms)", "Success %"
        );
        println!("{:-<100}", "");
        
        for result in results {
            println!(
                "{:<30} {:>12.0} {:>12.2} {:>12.2} {:>12.2} {:>12.1}",
                result.test_name,
                result.operations_per_second,
                result.average_latency_ms,
                result.p95_latency_ms,
                result.p99_latency_ms,
                result.success_rate * 100.0
            );
        }
        
        println!("{:=<100}", "");
    }

    /// Warmup connection pool
    async fn warmup_connection_pool(pool: &Arc<ConnectionPool<MockConnection>>) {
        let warmup_ops = 100;
        let mut handles = Vec::new();
        
        for _ in 0..warmup_ops {
            let pool_clone = pool.clone();
            let handle = tokio::spawn(async move {
                if let Ok(conn) = pool_clone.get_connection(|| async {
                    Ok(MockConnection { id: 1 })
                }).await {
                    pool_clone.return_connection(conn).await;
                }
            });
            handles.push(handle);
        }
        
        // Wait for warmup to complete
        for handle in handles {
            let _ = handle.await;
        }
    }

    /// Calculate performance results from latency data
    fn calculate_performance_results(
        test_name: String,
        mut latencies: Vec<Duration>,
        successful_ops: usize,
        total_ops: usize,
        total_duration: Duration,
    ) -> PerformanceResults {
        latencies.sort();
        
        let operations_per_second = successful_ops as f64 / total_duration.as_secs_f64();
        
        let average_latency_ms = if !latencies.is_empty() {
            latencies.iter().map(|d| d.as_secs_f64() * 1000.0).sum::<f64>() / latencies.len() as f64
        } else {
            0.0
        };
        
        let p95_latency_ms = if !latencies.is_empty() {
            let p95_index = (latencies.len() as f64 * 0.95) as usize;
            latencies[p95_index.min(latencies.len() - 1)].as_secs_f64() * 1000.0
        } else {
            0.0
        };
        
        let p99_latency_ms = if !latencies.is_empty() {
            let p99_index = (latencies.len() as f64 * 0.99) as usize;
            latencies[p99_index.min(latencies.len() - 1)].as_secs_f64() * 1000.0
        } else {
            0.0
        };
        
        let success_rate = if total_ops > 0 {
            successful_ops as f64 / total_ops as f64
        } else {
            0.0
        };
        
        PerformanceResults {
            test_name,
            operations_per_second,
            average_latency_ms,
            p95_latency_ms,
            p99_latency_ms,
            total_operations: successful_ops,
            total_duration_ms: total_duration.as_millis() as u64,
            success_rate,
        }
    }
}

/// Mock connection for testing
#[derive(Debug)]
struct MockConnection {
    id: u32,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_performance_test_config() {
        let config = PerformanceTestConfig::default();
        assert_eq!(config.duration, Duration::from_secs(10));
        assert_eq!(config.concurrent_clients, 10);
        assert_eq!(config.operations_per_client, 1000);
    }

    #[tokio::test]
    async fn test_calculate_performance_results() {
        let latencies = vec![
            Duration::from_millis(1),
            Duration::from_millis(2),
            Duration::from_millis(3),
            Duration::from_millis(4),
            Duration::from_millis(5),
        ];
        
        let results = NetworkPerformanceTests::calculate_performance_results(
            "Test".to_string(),
            latencies,
            5,
            5,
            Duration::from_secs(1),
        );
        
        assert_eq!(results.test_name, "Test");
        assert_eq!(results.operations_per_second, 5.0);
        assert_eq!(results.success_rate, 1.0);
        assert!(results.average_latency_ms > 0.0);
    }

    #[tokio::test]
    async fn test_buffer_manager_basic_performance() {
        let result = NetworkPerformanceTests::test_buffer_manager_performance().await;
        assert!(result.operations_per_second > 0.0);
        assert!(result.success_rate > 0.0);
    }
}