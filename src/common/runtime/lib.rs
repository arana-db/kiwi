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

//! Dual Runtime Architecture Module
//! 
//! This module provides the infrastructure for managing separate tokio runtimes
//! for network and storage operations to improve concurrency and prevent blocking.

pub mod config;
pub mod manager;
pub mod error;
pub mod error_logging;
pub mod message;
pub mod storage_server;
pub mod metrics;

#[cfg(test)]
mod tests;

pub use config::RuntimeConfig;
pub use manager::{RuntimeManager, RuntimeHealth as ManagerRuntimeHealth, RuntimeStats};
pub use error::DualRuntimeError;
pub use message::{
    MessageChannel, StorageRequest, StorageResponse, StorageCommand, 
    RequestId, RequestPriority, StorageStats, ChannelStats, StorageClient,
    BackpressureConfig, RetryConfig, RequestError, CircuitBreaker,
    RequestQueue, QueuedRequest, RecoveryManager, RecoveryState, RecoveryConfig, QueueStats, RecoveryStats
};
pub use error_logging::{
    ErrorLogger, ErrorLoggingConfig, ErrorEvent, ErrorMetrics, ErrorCategory, RuntimeContext,
    CorrelationId, ErrorRates, init_global_error_logger, get_global_error_logger
};
pub use storage_server::{
    StorageServer, StorageServerConfig, BatchProcessor, BackgroundTaskManager,
    BatchConfig, BatchStats, BackgroundTaskConfig, BackgroundTaskStats, RocksDbStats
};
pub use metrics::{
    MetricsCollector, MetricsConfig, RuntimeMetrics, NetworkRuntimeMetrics, StorageRuntimeMetrics,
    ChannelMetrics, HealthStatus, RuntimeHealth as MetricsRuntimeHealth, ChannelHealth, SystemHealth,
    NetworkMetricsTracker, StorageMetricsTracker, ChannelMetricsTracker, HealthMonitor,
    RocksDBMetrics, CompactionStats, FlushStats, BatchMetrics,
    HealthCheckEndpoints, HealthCheckConfig, HealthThresholds, HealthCheckResponse, ComponentHealth
};