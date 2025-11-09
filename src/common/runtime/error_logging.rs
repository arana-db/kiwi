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

//! Comprehensive error logging and correlation system for dual runtime architecture
//!
//! This module provides structured logging, error categorization, and correlation
//! across runtime boundaries for monitoring and alerting integration.

use log::{debug, error, info, warn};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tokio::sync::{Mutex, RwLock};
use uuid::Uuid;

use crate::error::{DualRuntimeError, ErrorSeverity};
use crate::message::RequestId;

/// Unique identifier for error correlation across runtime boundaries
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct CorrelationId(Uuid);

impl Default for CorrelationId {
    fn default() -> Self {
        Self::new()
    }
}

impl CorrelationId {
    /// Create a new correlation ID
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }

    /// Get the inner UUID
    pub fn inner(&self) -> &Uuid {
        &self.0
    }
}

impl std::fmt::Display for CorrelationId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Error category for classification and routing
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ErrorCategory {
    /// Network-related errors (connection, protocol, etc.)
    Network,
    /// Storage-related errors (RocksDB, disk I/O, etc.)
    Storage,
    /// Communication errors between runtimes
    Communication,
    /// Configuration and setup errors
    Configuration,
    /// Runtime lifecycle errors
    Lifecycle,
    /// Performance and resource errors
    Performance,
    /// Security and authentication errors
    Security,
    /// Unknown or uncategorized errors
    Unknown,
}

/// Runtime context where the error occurred
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum RuntimeContext {
    /// Error occurred in network runtime
    Network,
    /// Error occurred in storage runtime
    Storage,
    /// Error occurred in main process
    Main,
    /// Error occurred across runtime boundaries
    CrossRuntime,
}

/// Structured error event for logging and monitoring
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ErrorEvent {
    /// Unique identifier for this error event
    pub event_id: Uuid,
    /// Correlation ID for tracking across runtime boundaries
    pub correlation_id: CorrelationId,
    /// Request ID if associated with a specific request
    pub request_id: Option<RequestId>,
    /// Timestamp when the error occurred
    pub timestamp: SystemTime,
    /// Error category for classification
    pub category: ErrorCategory,
    /// Severity level of the error
    pub severity: ErrorSeverity,
    /// Runtime context where error occurred
    pub runtime_context: RuntimeContext,
    /// The actual error that occurred (as string for serialization)
    pub error: String,
    /// The original error type for programmatic access
    #[serde(skip)]
    pub error_type: Option<DualRuntimeError>,
    /// Additional context and metadata
    pub context: HashMap<String, String>,
    /// Stack trace if available
    pub stack_trace: Option<String>,
    /// Related error events (for error chains)
    pub related_events: Vec<Uuid>,
}

/// Error metrics for monitoring and alerting
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ErrorMetrics {
    /// Total number of errors by category
    pub errors_by_category: HashMap<ErrorCategory, u64>,
    /// Total number of errors by severity
    pub errors_by_severity: HashMap<ErrorSeverity, u64>,
    /// Total number of errors by runtime context
    pub errors_by_runtime: HashMap<RuntimeContext, u64>,
    /// Error rate over time windows
    pub error_rates: ErrorRates,
    /// Most recent errors for quick access
    pub recent_errors: Vec<ErrorEvent>,
}

/// Error rates over different time windows
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ErrorRates {
    /// Errors per minute in the last minute
    pub last_minute: f64,
    /// Errors per minute in the last 5 minutes
    pub last_5_minutes: f64,
    /// Errors per minute in the last hour
    pub last_hour: f64,
}

/// Configuration for error logging behavior
#[derive(Debug, Clone)]
pub struct ErrorLoggingConfig {
    /// Maximum number of recent errors to keep in memory
    pub max_recent_errors: usize,
    /// Whether to include stack traces in error events
    pub include_stack_traces: bool,
    /// Minimum severity level to log
    pub min_severity: ErrorSeverity,
    /// Whether to enable structured JSON logging
    pub structured_logging: bool,
    /// Whether to enable metrics collection
    pub enable_metrics: bool,
    /// Time window for error rate calculations
    pub rate_window: Duration,
}

impl Default for ErrorLoggingConfig {
    fn default() -> Self {
        Self {
            max_recent_errors: 1000,
            include_stack_traces: true,
            min_severity: ErrorSeverity::Low,
            structured_logging: true,
            enable_metrics: true,
            rate_window: Duration::from_secs(300), // 5 minutes
        }
    }
}

/// Comprehensive error logger for dual runtime architecture
pub struct ErrorLogger {
    /// Configuration for logging behavior
    config: ErrorLoggingConfig,
    /// Error metrics and statistics
    metrics: Arc<RwLock<ErrorMetrics>>,
    /// Error correlation tracking
    correlations: Arc<Mutex<HashMap<CorrelationId, Vec<Uuid>>>>,
    /// Time-based error tracking for rate calculations
    error_timeline: Arc<Mutex<Vec<(SystemTime, ErrorSeverity)>>>,
}

impl ErrorLogger {
    /// Create a new error logger with default configuration
    pub fn new() -> Self {
        Self::with_config(ErrorLoggingConfig::default())
    }

    /// Create a new error logger with custom configuration
    pub fn with_config(config: ErrorLoggingConfig) -> Self {
        Self {
            config,
            metrics: Arc::new(RwLock::new(ErrorMetrics::default())),
            correlations: Arc::new(Mutex::new(HashMap::new())),
            error_timeline: Arc::new(Mutex::new(Vec::new())),
        }
    }

    /// Log an error event with full context
    pub async fn log_error(
        &self,
        error: DualRuntimeError,
        runtime_context: RuntimeContext,
        correlation_id: Option<CorrelationId>,
        request_id: Option<RequestId>,
        context: HashMap<String, String>,
    ) -> CorrelationId {
        let correlation_id = correlation_id.unwrap_or_default();
        let event_id = Uuid::new_v4();
        let timestamp = SystemTime::now();
        let severity = error.severity();
        let category = self.categorize_error(&error);

        // Skip logging if below minimum severity
        if severity < self.config.min_severity {
            return correlation_id;
        }

        let stack_trace = if self.config.include_stack_traces {
            Some(self.capture_stack_trace())
        } else {
            None
        };

        let error_event = ErrorEvent {
            event_id,
            correlation_id: correlation_id.clone(),
            request_id,
            timestamp,
            category: category.clone(),
            severity,
            runtime_context: runtime_context.clone(),
            error: error.to_string(),
            error_type: Some(error.clone()),
            context,
            stack_trace,
            related_events: Vec::new(),
        };

        // Update correlation tracking
        {
            let mut correlations = self.correlations.lock().await;
            correlations
                .entry(correlation_id.clone())
                .or_insert_with(Vec::new)
                .push(event_id);
        }

        // Update metrics if enabled
        if self.config.enable_metrics {
            self.update_metrics(&error_event).await;
        }

        // Perform structured logging
        if self.config.structured_logging {
            self.log_structured(&error_event).await;
        } else {
            self.log_traditional(&error_event).await;
        }

        // Update error timeline for rate calculations
        {
            let mut timeline = self.error_timeline.lock().await;
            timeline.push((timestamp, severity));

            // Clean up old entries (keep only last hour)
            let cutoff = timestamp - Duration::from_secs(3600);
            timeline.retain(|(ts, _)| *ts > cutoff);
        }

        correlation_id
    }

    /// Log an error with minimal context (convenience method)
    pub async fn log_simple_error(
        &self,
        error: DualRuntimeError,
        runtime_context: RuntimeContext,
    ) -> CorrelationId {
        self.log_error(error, runtime_context, None, None, HashMap::new())
            .await
    }

    /// Log an error associated with a specific request
    pub async fn log_request_error(
        &self,
        error: DualRuntimeError,
        runtime_context: RuntimeContext,
        request_id: RequestId,
        correlation_id: Option<CorrelationId>,
    ) -> CorrelationId {
        let mut context = HashMap::new();
        context.insert("request_id".to_string(), request_id.to_string());

        self.log_error(
            error,
            runtime_context,
            correlation_id,
            Some(request_id),
            context,
        )
        .await
    }

    /// Log a correlated error (part of an error chain)
    pub async fn log_correlated_error(
        &self,
        error: DualRuntimeError,
        runtime_context: RuntimeContext,
        correlation_id: CorrelationId,
        parent_event_id: Uuid,
    ) -> CorrelationId {
        let mut context = HashMap::new();
        context.insert("parent_event_id".to_string(), parent_event_id.to_string());

        self.log_error(error, runtime_context, Some(correlation_id), None, context)
            .await
    }

    /// Get current error metrics
    pub async fn get_metrics(&self) -> ErrorMetrics {
        let mut metrics = self.metrics.read().await.clone();

        // Update error rates
        metrics.error_rates = self.calculate_error_rates().await;

        metrics
    }

    /// Get errors by correlation ID
    pub async fn get_correlated_errors(&self, correlation_id: &CorrelationId) -> Vec<Uuid> {
        let correlations = self.correlations.lock().await;
        correlations
            .get(correlation_id)
            .cloned()
            .unwrap_or_default()
    }

    /// Clear old error data (for memory management)
    pub async fn cleanup_old_data(&self, max_age: Duration) {
        let cutoff = SystemTime::now() - max_age;

        // Clean up metrics
        {
            let mut metrics = self.metrics.write().await;
            metrics
                .recent_errors
                .retain(|event| event.timestamp > cutoff);
        }

        // Clean up timeline
        {
            let mut timeline = self.error_timeline.lock().await;
            timeline.retain(|(ts, _)| *ts > cutoff);
        }

        // Note: We don't clean up correlations as they might be needed for debugging
    }

    /// Export error data for external monitoring systems
    pub async fn export_metrics(&self) -> serde_json::Value {
        let metrics = self.get_metrics().await;
        serde_json::to_value(metrics).unwrap_or_default()
    }

    /// Categorize an error based on its type
    fn categorize_error(&self, error: &DualRuntimeError) -> ErrorCategory {
        match error {
            DualRuntimeError::NetworkRuntime(_) => ErrorCategory::Network,
            DualRuntimeError::StorageRuntime(_) => ErrorCategory::Storage,
            DualRuntimeError::Channel(_) => ErrorCategory::Communication,
            DualRuntimeError::Timeout { .. } => ErrorCategory::Performance,
            DualRuntimeError::Configuration(_) => ErrorCategory::Configuration,
            DualRuntimeError::Lifecycle(_) => ErrorCategory::Lifecycle,
            DualRuntimeError::HealthCheck(_) => ErrorCategory::Performance,
            DualRuntimeError::Storage(_) => ErrorCategory::Storage,
            DualRuntimeError::Io(_) => ErrorCategory::Network,
            DualRuntimeError::CircuitBreakerOpen { .. } => ErrorCategory::Performance,
            DualRuntimeError::RuntimeIsolation { .. } => ErrorCategory::Communication,
            DualRuntimeError::ErrorBoundary { .. } => ErrorCategory::Communication,
            DualRuntimeError::FaultIsolation { .. } => ErrorCategory::Performance,
            DualRuntimeError::RecoveryFailed { .. } => ErrorCategory::Performance,
        }
    }

    /// Capture stack trace for debugging
    fn capture_stack_trace(&self) -> String {
        // In a real implementation, you might use backtrace crate
        // For now, we'll return a placeholder
        format!(
            "Stack trace captured at {}",
            chrono::Utc::now().format("%Y-%m-%d %H:%M:%S UTC")
        )
    }

    /// Update error metrics
    async fn update_metrics(&self, event: &ErrorEvent) {
        let mut metrics = self.metrics.write().await;

        // Update category counts
        *metrics
            .errors_by_category
            .entry(event.category.clone())
            .or_insert(0) += 1;

        // Update severity counts
        *metrics
            .errors_by_severity
            .entry(event.severity)
            .or_insert(0) += 1;

        // Update runtime context counts
        *metrics
            .errors_by_runtime
            .entry(event.runtime_context.clone())
            .or_insert(0) += 1;

        // Add to recent errors
        metrics.recent_errors.push(event.clone());

        // Trim recent errors if needed
        if metrics.recent_errors.len() > self.config.max_recent_errors {
            metrics.recent_errors.remove(0);
        }
    }

    /// Perform structured JSON logging
    async fn log_structured(&self, event: &ErrorEvent) {
        let json_event = serde_json::to_string(event).unwrap_or_else(|_| {
            format!(
                "{{\"error\": \"Failed to serialize error event\", \"event_id\": \"{}\"}}",
                event.event_id
            )
        });

        match event.severity {
            ErrorSeverity::Critical => error!(target: "dual_runtime_errors", "{}", json_event),
            ErrorSeverity::High => error!(target: "dual_runtime_errors", "{}", json_event),
            ErrorSeverity::Medium => warn!(target: "dual_runtime_errors", "{}", json_event),
            ErrorSeverity::Low => info!(target: "dual_runtime_errors", "{}", json_event),
        }
    }

    /// Perform traditional text-based logging
    async fn log_traditional(&self, event: &ErrorEvent) {
        let context_str = if event.context.is_empty() {
            String::new()
        } else {
            format!(
                " [{}]",
                event
                    .context
                    .iter()
                    .map(|(k, v)| format!("{}={}", k, v))
                    .collect::<Vec<_>>()
                    .join(", ")
            )
        };

        let message = format!(
            "[{}] {} error in {:?} runtime: {} (correlation_id: {}, event_id: {}){}",
            event.severity as u8,
            format!("{:?}", event.category).to_uppercase(),
            event.runtime_context,
            event.error,
            event.correlation_id,
            event.event_id,
            context_str
        );

        match event.severity {
            ErrorSeverity::Critical => error!(target: "dual_runtime", "{}", message),
            ErrorSeverity::High => error!(target: "dual_runtime", "{}", message),
            ErrorSeverity::Medium => warn!(target: "dual_runtime", "{}", message),
            ErrorSeverity::Low => info!(target: "dual_runtime", "{}", message),
        }

        // Log stack trace if available
        if let Some(ref stack_trace) = event.stack_trace {
            debug!(target: "dual_runtime", "Stack trace for event {}: {}", event.event_id, stack_trace);
        }
    }

    /// Calculate error rates over different time windows
    async fn calculate_error_rates(&self) -> ErrorRates {
        let timeline = self.error_timeline.lock().await;
        let now = SystemTime::now();

        let last_minute = now - Duration::from_secs(60);
        let last_5_minutes = now - Duration::from_secs(300);
        let last_hour = now - Duration::from_secs(3600);

        let errors_last_minute = timeline.iter().filter(|(ts, _)| *ts > last_minute).count() as f64;

        let errors_last_5_minutes = timeline
            .iter()
            .filter(|(ts, _)| *ts > last_5_minutes)
            .count() as f64;

        let errors_last_hour = timeline.iter().filter(|(ts, _)| *ts > last_hour).count() as f64;

        ErrorRates {
            last_minute: errors_last_minute,
            last_5_minutes: errors_last_5_minutes / 5.0,
            last_hour: errors_last_hour / 60.0,
        }
    }
}

impl Default for ErrorLogger {
    fn default() -> Self {
        Self::new()
    }
}

/// Global error logger instance
static mut GLOBAL_ERROR_LOGGER: Option<Arc<ErrorLogger>> = None;
static INIT: std::sync::Once = std::sync::Once::new();

/// Initialize the global error logger
pub fn init_global_error_logger(config: ErrorLoggingConfig) {
    INIT.call_once(|| unsafe {
        GLOBAL_ERROR_LOGGER = Some(Arc::new(ErrorLogger::with_config(config)));
    });
}

/// Get the global error logger instance
pub fn get_global_error_logger() -> Option<Arc<ErrorLogger>> {
    #[allow(static_mut_refs)]
    unsafe {
        GLOBAL_ERROR_LOGGER.as_ref().cloned()
    }
}

/// Convenience macro for logging errors with the global logger
#[macro_export]
macro_rules! log_dual_runtime_error {
    ($error:expr, $runtime:expr) => {
        if let Some(logger) = $crate::runtime::error_logging::get_global_error_logger() {
            tokio::spawn(async move {
                logger.log_simple_error($error, $runtime).await;
            });
        }
    };
    ($error:expr, $runtime:expr, $correlation_id:expr) => {
        if let Some(logger) = $crate::runtime::error_logging::get_global_error_logger() {
            tokio::spawn(async move {
                logger
                    .log_error(
                        $error,
                        $runtime,
                        Some($correlation_id),
                        None,
                        std::collections::HashMap::new(),
                    )
                    .await;
            });
        }
    };
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_correlation_id_creation() {
        let id1 = CorrelationId::new();
        let id2 = CorrelationId::new();

        assert_ne!(id1, id2);
        assert_ne!(id1.inner(), id2.inner());
    }

    #[test]
    fn test_error_categorization() {
        let logger = ErrorLogger::new();

        let network_error = DualRuntimeError::network_runtime("test");
        assert_eq!(
            logger.categorize_error(&network_error),
            ErrorCategory::Network
        );

        let storage_error = DualRuntimeError::storage_runtime("test");
        assert_eq!(
            logger.categorize_error(&storage_error),
            ErrorCategory::Storage
        );

        let channel_error = DualRuntimeError::channel("test");
        assert_eq!(
            logger.categorize_error(&channel_error),
            ErrorCategory::Communication
        );
    }

    #[tokio::test]
    async fn test_error_logging() {
        let logger = ErrorLogger::new();
        let error = DualRuntimeError::network_runtime("test error");

        let correlation_id = logger
            .log_simple_error(error, RuntimeContext::Network)
            .await;

        let metrics = logger.get_metrics().await;
        assert_eq!(
            metrics.errors_by_category.get(&ErrorCategory::Network),
            Some(&1)
        );
        assert_eq!(metrics.recent_errors.len(), 1);

        let correlated_errors = logger.get_correlated_errors(&correlation_id).await;
        assert_eq!(correlated_errors.len(), 1);
    }

    #[tokio::test]
    async fn test_error_correlation() {
        let logger = ErrorLogger::new();
        let correlation_id = CorrelationId::new();

        let error1 = DualRuntimeError::network_runtime("first error");
        let error2 = DualRuntimeError::storage_runtime("second error");

        logger
            .log_error(
                error1,
                RuntimeContext::Network,
                Some(correlation_id.clone()),
                None,
                HashMap::new(),
            )
            .await;
        logger
            .log_error(
                error2,
                RuntimeContext::Storage,
                Some(correlation_id.clone()),
                None,
                HashMap::new(),
            )
            .await;

        let correlated_errors = logger.get_correlated_errors(&correlation_id).await;
        assert_eq!(correlated_errors.len(), 2);
    }

    #[tokio::test]
    async fn test_metrics_export() {
        let logger = ErrorLogger::new();
        let error = DualRuntimeError::configuration("test config error");

        logger.log_simple_error(error, RuntimeContext::Main).await;

        let exported = logger.export_metrics().await;
        assert!(exported.is_object());
    }
}
