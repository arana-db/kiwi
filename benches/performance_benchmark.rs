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

//! Comprehensive performance benchmarks for Kiwi Redis implementation
//! 
//! This benchmark suite tests:
//! - List operations performance against Redis
//! - Network layer performance improvements
//! - RESP3 vs RESP2 protocol performance

use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio;

mod list_benchmarks;
mod network_benchmarks;
mod protocol_benchmarks;

use list_benchmarks::ListBenchmarks;
use network_benchmarks::NetworkBenchmarks;
use protocol_benchmarks::ProtocolBenchmarks;

/// Performance benchmark results
#[derive(Debug, Clone)]
pub struct BenchmarkResults {
    pub test_name: String,
    pub operations_per_second: f64,
    pub average_latency_ms: f64,
    pub p95_latency_ms: f64,
    pub p99_latency_ms: f64,
    pub total_operations: usize,
    pub total_duration_ms: u64,
    pub success_rate: f64,
    pub memory_usage_mb: f64,
}

/// Benchmark configuration
#[derive(Debug, Clone)]
pub struct BenchmarkConfig {
    pub duration: Duration,
    pub concurrent_clients: usize,
    pub operations_per_client: usize,
    pub warmup_duration: Duration,
    pub data_size_bytes: usize,
}

impl Default for BenchmarkConfig {
    fn default() -> Self {
        Self {
            duration: Duration::from_secs(30),
            concurrent_clients: 50,
            operations_per_client: 10000,
            warmup_duration: Duration::from_secs(5),
            data_size_bytes: 1024,
        }
    }
}

#[tokio::main]
async fn main() {
    env_logger::init();
    
    println!("Starting Kiwi Comprehensive Performance Benchmark Suite");
    println!("======================================================");
    
    let config = BenchmarkConfig::default();
    
    // Run all benchmark suites
    let mut all_results = Vec::new();
    
    println!("\n1. Running List Operations Benchmarks...");
    println!("----------------------------------------");
    let list_results = ListBenchmarks::run_comprehensive_benchmark(&config).await;
    all_results.extend(list_results);
    
    println!("\n2. Running Network Layer Benchmarks...");
    println!("--------------------------------------");
    let network_results = NetworkBenchmarks::run_comprehensive_benchmark(&config).await;
    all_results.extend(network_results);
    
    println!("\n3. Running Protocol Performance Benchmarks...");
    println!("----------------------------------------------");
    let protocol_results = ProtocolBenchmarks::run_comprehensive_benchmark(&config).await;
    all_results.extend(protocol_results);
    
    // Print comprehensive results
    print_comprehensive_results(&all_results);
    
    // Generate performance report
    generate_performance_report(&all_results);
    
    println!("\nBenchmark suite completed successfully!");
}

/// Print comprehensive benchmark results
fn print_comprehensive_results(results: &[BenchmarkResults]) {
    println!("\n{:=<120}", "");
    println!("{:^120}", "COMPREHENSIVE PERFORMANCE BENCHMARK RESULTS");
    println!("{:=<120}", "");
    
    println!(
        "{:<40} {:>12} {:>12} {:>12} {:>12} {:>12} {:>12}",
        "Test Name", "Ops/Sec", "Avg (ms)", "P95 (ms)", "P99 (ms)", "Success %", "Memory (MB)"
    );
    println!("{:-<120}", "");
    
    for result in results {
        println!(
            "{:<40} {:>12.0} {:>12.2} {:>12.2} {:>12.2} {:>12.1} {:>12.1}",
            result.test_name,
            result.operations_per_second,
            result.average_latency_ms,
            result.p95_latency_ms,
            result.p99_latency_ms,
            result.success_rate * 100.0,
            result.memory_usage_mb
        );
    }
    
    println!("{:=<120}", "");
    
    // Calculate summary statistics
    let total_ops: usize = results.iter().map(|r| r.total_operations).sum();
    let avg_ops_per_sec: f64 = results.iter().map(|r| r.operations_per_second).sum::<f64>() / results.len() as f64;
    let avg_success_rate: f64 = results.iter().map(|r| r.success_rate).sum::<f64>() / results.len() as f64;
    let total_memory_mb: f64 = results.iter().map(|r| r.memory_usage_mb).sum::<f64>();
    
    println!("\nSUMMARY:");
    println!("Total operations executed: {}", total_ops);
    println!("Average operations per second: {:.0}", avg_ops_per_sec);
    println!("Average success rate: {:.1}%", avg_success_rate * 100.0);
    println!("Total memory usage: {:.1} MB", total_memory_mb);
}

/// Generate performance report with recommendations
fn generate_performance_report(results: &[BenchmarkResults]) {
    println!("\nPERFORMANCE ANALYSIS:");
    println!("=====================");
    
    // Categorize results by performance
    let mut high_performers = Vec::new();
    let mut medium_performers = Vec::new();
    let mut low_performers = Vec::new();
    
    for result in results {
        if result.operations_per_second > 10000.0 && result.p99_latency_ms < 10.0 {
            high_performers.push(result);
        } else if result.operations_per_second > 1000.0 && result.p99_latency_ms < 100.0 {
            medium_performers.push(result);
        } else {
            low_performers.push(result);
        }
    }
    
    if !high_performers.is_empty() {
        println!("\n✅ HIGH PERFORMANCE OPERATIONS:");
        for result in high_performers {
            println!("   {} - {:.0} ops/sec, {:.2}ms P99", result.test_name, result.operations_per_second, result.p99_latency_ms);
        }
    }
    
    if !medium_performers.is_empty() {
        println!("\n⚠️  MEDIUM PERFORMANCE OPERATIONS:");
        for result in medium_performers {
            println!("   {} - {:.0} ops/sec, {:.2}ms P99", result.test_name, result.operations_per_second, result.p99_latency_ms);
        }
    }
    
    if !low_performers.is_empty() {
        println!("\n❌ LOW PERFORMANCE OPERATIONS (NEED OPTIMIZATION):");
        for result in low_performers {
            println!("   {} - {:.0} ops/sec, {:.2}ms P99", result.test_name, result.operations_per_second, result.p99_latency_ms);
        }
    }
    
    // Performance recommendations
    println!("\nRECOMMENDations:");
    for result in results {
        if result.operations_per_second < 1000.0 {
            println!("⚠️  {} shows low throughput ({:.0} ops/sec) - consider optimization", result.test_name, result.operations_per_second);
        }
        if result.p99_latency_ms > 100.0 {
            println!("⚠️  {} shows high P99 latency ({:.2}ms) - investigate bottlenecks", result.test_name, result.p99_latency_ms);
        }
        if result.success_rate < 0.99 {
            println!("⚠️  {} shows low success rate ({:.1}%) - check error handling", result.test_name, result.success_rate * 100.0);
        }
        if result.memory_usage_mb > 100.0 {
            println!("⚠️  {} shows high memory usage ({:.1}MB) - optimize memory allocation", result.test_name, result.memory_usage_mb);
        }
    }
}

/// Calculate benchmark results from latency data
pub fn calculate_benchmark_results(
    test_name: String,
    mut latencies: Vec<Duration>,
    successful_ops: usize,
    total_ops: usize,
    total_duration: Duration,
    memory_usage_mb: f64,
) -> BenchmarkResults {
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
    
    BenchmarkResults {
        test_name,
        operations_per_second,
        average_latency_ms,
        p95_latency_ms,
        p99_latency_ms,
        total_operations: successful_ops,
        total_duration_ms: total_duration.as_millis() as u64,
        success_rate,
        memory_usage_mb,
    }
}