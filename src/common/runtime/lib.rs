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
pub mod error;
pub mod error_logging;
pub mod manager;
pub mod message;
pub mod metrics;
pub mod storage_server;

#[cfg(test)]
mod tests;

#[cfg(test)]
mod additional_unit_tests;

#[cfg(test)]
mod stress_tests;

pub use config::RuntimeConfig;
pub use error::DualRuntimeError;
pub use error_logging::{
    CorrelationId, ErrorCategory, ErrorEvent, ErrorLogger, ErrorLoggingConfig, ErrorMetrics,
    ErrorRates, RuntimeContext, get_global_error_logger, init_global_error_logger,
};
pub use manager::{RuntimeHealth as ManagerRuntimeHealth, RuntimeManager, RuntimeStats};
pub use message::{
    BackpressureConfig, ChannelStats, CircuitBreaker, MessageChannel, QueueStats, QueuedRequest,
    RecoveryConfig, RecoveryManager, RecoveryState, RecoveryStats, RequestError, RequestId,
    RequestPriority, RequestQueue, RetryConfig, StorageClient, StorageCommand, StorageRequest,
    StorageResponse, StorageStats,
};
pub use metrics::{
    BatchMetrics, ChannelHealth, ChannelMetrics, ChannelMetricsTracker, CompactionStats,
    ComponentHealth, FlushStats, HealthCheckConfig, HealthCheckEndpoints, HealthCheckResponse,
    HealthMonitor, HealthStatus, HealthThresholds, MetricsCollector, MetricsConfig,
    NetworkMetricsTracker, NetworkRuntimeMetrics, RocksDBMetrics,
    RuntimeHealth as MetricsRuntimeHealth, RuntimeMetrics, StorageMetricsTracker,
    StorageRuntimeMetrics, SystemHealth,
};
pub use storage_server::{
    BackgroundTaskConfig, BackgroundTaskManager, BackgroundTaskStats, BatchConfig, BatchProcessor,
    BatchStats, RocksDbStats, StorageServer, StorageServerConfig,
};
