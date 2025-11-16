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

#[path = "../tests/performance_tests.rs"]
mod performance_tests;

use performance_tests::NetworkPerformanceTests;

#[tokio::main]
async fn main() {
    env_logger::init();

    println!("Starting Kiwi Network Layer Performance Benchmark");
    println!("================================================");

    let results = NetworkPerformanceTests::run_comprehensive_benchmark().await;

    NetworkPerformanceTests::print_results(&results);

    // Generate summary
    let total_ops: usize = results.iter().map(|r| r.total_operations).sum();
    let avg_ops_per_sec: f64 =
        results.iter().map(|r| r.operations_per_second).sum::<f64>() / results.len() as f64;
    let avg_success_rate: f64 =
        results.iter().map(|r| r.success_rate).sum::<f64>() / results.len() as f64;

    println!("\nSUMMARY:");
    println!("Total operations executed: {}", total_ops);
    println!("Average operations per second: {:.0}", avg_ops_per_sec);
    println!("Average success rate: {:.1}%", avg_success_rate * 100.0);

    // Performance recommendations
    println!("\nPERFORMANCE RECOMMENDATIONS:");
    for result in &results {
        if result.operations_per_second < 1000.0 {
            println!(
                "⚠️  {} shows low throughput ({:.0} ops/sec)",
                result.test_name, result.operations_per_second
            );
        }
        if result.p99_latency_ms > 100.0 {
            println!(
                "⚠️  {} shows high P99 latency ({:.2}ms)",
                result.test_name, result.p99_latency_ms
            );
        }
        if result.success_rate < 0.99 {
            println!(
                "⚠️  {} shows low success rate ({:.1}%)",
                result.test_name,
                result.success_rate * 100.0
            );
        }
    }

    println!("\nBenchmark completed successfully!");
}
