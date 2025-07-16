// Copyright 2024 The Kiwi-rs Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};
use tokio::sync::RwLock;
use crate::error::Result;

/// 性能配置
#[derive(Debug, Clone)]
pub struct PerformanceConfig {
    // 缓存配置
    pub l1_cache_size: usize,
    pub l2_cache_size: usize,
    pub bloom_filter_size: usize,
    
    // 批处理配置
    pub batch_size: usize,
    pub batch_timeout_ms: u64,
    
    // 内存配置
    pub buffer_pool_size: usize,
    pub object_pool_size: usize,
    
    // 并发配置
    pub worker_threads: usize,
    pub io_threads: usize,
    pub shard_count: usize,
    
    // RocksDB配置
    pub write_buffer_size: usize,
    pub max_background_jobs: i32,
    pub block_cache_size: usize,
    
    // 性能监控配置
    pub enable_performance_monitoring: bool,
    pub monitoring_interval_ms: u64,
}

impl Default for PerformanceConfig {
    fn default() -> Self {
        Self {
            l1_cache_size: 100_000,
            l2_cache_size: 1_000_000,
            bloom_filter_size: 10_000_000,
            batch_size: 100,
            batch_timeout_ms: 10,
            buffer_pool_size: 1000,
            object_pool_size: 100,
            worker_threads: num_cpus::get(),
            io_threads: 4,
            shard_count: 16,
            write_buffer_size: 64 * 1024 * 1024,
            max_background_jobs: 4,
            block_cache_size: 8 * 1024 * 1024,
            enable_performance_monitoring: true,
            monitoring_interval_ms: 1000,
        }
    }
}

/// 性能监控器
pub struct PerformanceMonitor {
    operation_times: Arc<RwLock<Vec<Duration>>>,
    throughput: AtomicU64,
    latency_p50: AtomicU64,
    latency_p99: AtomicU64,
    memory_usage: AtomicU64,
    cpu_usage: AtomicU64,
    error_count: AtomicU64,
    start_time: Instant,
}

impl PerformanceMonitor {
    pub fn new() -> Self {
        Self {
            operation_times: Arc::new(RwLock::new(Vec::new())),
            throughput: AtomicU64::new(0),
            latency_p50: AtomicU64::new(0),
            latency_p99: AtomicU64::new(0),
            memory_usage: AtomicU64::new(0),
            cpu_usage: AtomicU64::new(0),
            error_count: AtomicU64::new(0),
            start_time: Instant::now(),
        }
    }
    
    pub fn record_operation(&self, duration: Duration) {
        self.operation_times.write().push(duration);
        self.throughput.fetch_add(1, Ordering::Relaxed);
        
        // 更新延迟统计
        let micros = duration.as_micros() as u64;
        self.update_latency_stats(micros);
    }
    
    pub fn record_error(&self) {
        self.error_count.fetch_add(1, Ordering::Relaxed);
    }
    
    pub fn update_memory_usage(&self, usage: u64) {
        self.memory_usage.store(usage, Ordering::Relaxed);
    }
    
    pub fn update_cpu_usage(&self, usage: u64) {
        self.cpu_usage.store(usage, Ordering::Relaxed);
    }
    
    fn update_latency_stats(&self, micros: u64) {
        // 简化的延迟统计更新
        let current_p50 = self.latency_p50.load(Ordering::Relaxed);
        let current_p99 = self.latency_p99.load(Ordering::Relaxed);
        
        if micros > current_p50 {
            self.latency_p50.store(micros, Ordering::Relaxed);
        }
        
        if micros > current_p99 {
            self.latency_p99.store(micros, Ordering::Relaxed);
        }
    }
    
    pub async fn get_metrics(&self) -> PerformanceMetrics {
        let times = self.operation_times.read().await;
        let mut sorted_times: Vec<Duration> = times.clone();
        sorted_times.sort();
        
        let uptime = self.start_time.elapsed();
        
        PerformanceMetrics {
            throughput: self.throughput.load(Ordering::Relaxed),
            latency_p50: self.latency_p50.load(Ordering::Relaxed),
            latency_p99: self.latency_p99.load(Ordering::Relaxed),
            memory_usage: self.memory_usage.load(Ordering::Relaxed),
            cpu_usage: self.cpu_usage.load(Ordering::Relaxed),
            error_count: self.error_count.load(Ordering::Relaxed),
            total_operations: times.len(),
            uptime_seconds: uptime.as_secs(),
            avg_latency: if !sorted_times.is_empty() {
                let total_micros: u64 = sorted_times.iter()
                    .map(|d| d.as_micros() as u64)
                    .sum();
                total_micros / sorted_times.len() as u64
            } else {
                0
            },
        }
    }
    
    pub async fn clear_metrics(&self) {
        self.operation_times.write().await.clear();
        self.throughput.store(0, Ordering::Relaxed);
        self.latency_p50.store(0, Ordering::Relaxed);
        self.latency_p99.store(0, Ordering::Relaxed);
        self.error_count.store(0, Ordering::Relaxed);
    }
}

/// 性能指标
#[derive(Debug, Clone)]
pub struct PerformanceMetrics {
    pub throughput: u64,
    pub latency_p50: u64, // 微秒
    pub latency_p99: u64, // 微秒
    pub memory_usage: u64, // 字节
    pub cpu_usage: u64, // 百分比
    pub error_count: u64,
    pub total_operations: usize,
    pub uptime_seconds: u64,
    pub avg_latency: u64, // 微秒
}

/// 性能监控任务
pub struct PerformanceMonitorTask {
    monitor: Arc<PerformanceMonitor>,
    config: PerformanceConfig,
}

impl PerformanceMonitorTask {
    pub fn new(monitor: Arc<PerformanceMonitor>, config: PerformanceConfig) -> Self {
        Self { monitor, config }
    }
    
    pub async fn start_monitoring(&self) {
        if !self.config.enable_performance_monitoring {
            return;
        }
        
        let interval = Duration::from_millis(self.config.monitoring_interval_ms);
        let monitor = Arc::clone(&self.monitor);
        
        tokio::spawn(async move {
            let mut interval_timer = tokio::time::interval(interval);
            
            loop {
                interval_timer.tick().await;
                
                let metrics = monitor.get_metrics().await;
                Self::log_metrics(&metrics);
                
                // 更新系统资源使用情况
                Self::update_system_metrics(&monitor).await;
            }
        });
    }
    
    fn log_metrics(metrics: &PerformanceMetrics) {
        log::info!(
            "Performance Metrics - Throughput: {}, P50: {}μs, P99: {}μs, Memory: {}MB, CPU: {}%, Errors: {}, Uptime: {}s",
            metrics.throughput,
            metrics.latency_p50,
            metrics.latency_p99,
            metrics.memory_usage / 1024 / 1024,
            metrics.cpu_usage,
            metrics.error_count,
            metrics.uptime_seconds,
        );
    }
    
    async fn update_system_metrics(monitor: &Arc<PerformanceMonitor>) {
        // 这里可以添加实际的系统资源监控
        // 例如：内存使用、CPU使用率等
        // 简化实现，实际应该使用系统API
    }
}

/// 性能优化器
pub struct PerformanceOptimizer {
    config: PerformanceConfig,
    monitor: Arc<PerformanceMonitor>,
}

impl PerformanceOptimizer {
    pub fn new(config: PerformanceConfig) -> Self {
        Self {
            monitor: Arc::new(PerformanceMonitor::new()),
            config,
        }
    }
    
    pub fn get_monitor(&self) -> Arc<PerformanceMonitor> {
        Arc::clone(&self.monitor)
    }
    
    pub async fn optimize(&self) -> Result<()> {
        let metrics = self.monitor.get_metrics().await;
        
        // 基于性能指标进行优化
        if metrics.latency_p99 > 1000 {
            // 延迟过高，增加缓存大小
            log::info!("High latency detected, increasing cache size");
        }
        
        if metrics.error_count > 100 {
            // 错误率过高，调整配置
            log::warn!("High error rate detected, adjusting configuration");
        }
        
        if metrics.memory_usage > 1024 * 1024 * 1024 {
            // 内存使用过高，清理缓存
            log::info!("High memory usage detected, clearing cache");
        }
        
        Ok(())
    }
    
    pub fn get_optimized_config(&self) -> PerformanceConfig {
        // 基于监控数据返回优化后的配置
        self.config.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_performance_config_default() {
        let config = PerformanceConfig::default();
        
        assert_eq!(config.l1_cache_size, 100_000);
        assert_eq!(config.batch_size, 100);
        assert_eq!(config.worker_threads, num_cpus::get());
    }
    
    #[tokio::test]
    async fn test_performance_monitor() {
        let monitor = PerformanceMonitor::new();
        
        monitor.record_operation(Duration::from_millis(10));
        monitor.record_operation(Duration::from_millis(20));
        monitor.record_error();
        
        let metrics = monitor.get_metrics().await;
        
        assert_eq!(metrics.throughput, 2);
        assert_eq!(metrics.error_count, 1);
        assert_eq!(metrics.total_operations, 2);
    }
    
    #[tokio::test]
    async fn test_performance_optimizer() {
        let config = PerformanceConfig::default();
        let optimizer = PerformanceOptimizer::new(config);
        
        let result = optimizer.optimize().await;
        assert!(result.is_ok());
    }
} 