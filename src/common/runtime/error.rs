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

use std::time::Duration;
use thiserror::Error;

/// Errors that can occur in the dual runtime architecture
#[derive(Debug, Error, Clone)]
pub enum DualRuntimeError {
    #[error("Network runtime error: {0}")]
    NetworkRuntime(String),

    #[error("Storage runtime error: {0}")]
    StorageRuntime(String),

    #[error("Channel communication error: {0}")]
    Channel(String),

    #[error("Request timeout after {timeout:?}")]
    Timeout { timeout: Duration },

    #[error("Runtime configuration error: {0}")]
    Configuration(String),

    #[error("Runtime lifecycle error: {0}")]
    Lifecycle(String),

    #[error("Runtime health check failed: {0}")]
    HealthCheck(String),

    #[error("Storage operation failed: {0}")]
    Storage(String),

    #[error("IO error: {0}")]
    Io(String),

    #[error("Circuit breaker is open: {reason}")]
    CircuitBreakerOpen { reason: String },

    #[error("Runtime isolation failure: {runtime} - {reason}")]
    RuntimeIsolation { runtime: String, reason: String },

    #[error("Error boundary triggered: {boundary} - {error}")]
    ErrorBoundary { boundary: String, error: String },

    #[error("Fault isolation activated: {component} - {details}")]
    FaultIsolation { component: String, details: String },

    #[error("Recovery mechanism failed: {mechanism} - {reason}")]
    RecoveryFailed { mechanism: String, reason: String },
}

impl DualRuntimeError {
    /// Create a network runtime error
    pub fn network_runtime<S: Into<String>>(msg: S) -> Self {
        Self::NetworkRuntime(msg.into())
    }

    /// Create a storage runtime error
    pub fn storage_runtime<S: Into<String>>(msg: S) -> Self {
        Self::StorageRuntime(msg.into())
    }

    /// Create a channel communication error
    pub fn channel<S: Into<String>>(msg: S) -> Self {
        Self::Channel(msg.into())
    }

    /// Create a timeout error
    pub fn timeout(timeout: Duration) -> Self {
        Self::Timeout { timeout }
    }

    /// Create a configuration error
    pub fn configuration<S: Into<String>>(msg: S) -> Self {
        Self::Configuration(msg.into())
    }

    /// Create a lifecycle error
    pub fn lifecycle<S: Into<String>>(msg: S) -> Self {
        Self::Lifecycle(msg.into())
    }

    /// Create a health check error
    pub fn health_check<S: Into<String>>(msg: S) -> Self {
        Self::HealthCheck(msg.into())
    }

    /// Create a circuit breaker error
    pub fn circuit_breaker_open<S: Into<String>>(reason: S) -> Self {
        Self::CircuitBreakerOpen {
            reason: reason.into(),
        }
    }

    /// Create a runtime isolation error
    pub fn runtime_isolation<S: Into<String>>(runtime: S, reason: S) -> Self {
        Self::RuntimeIsolation {
            runtime: runtime.into(),
            reason: reason.into(),
        }
    }

    /// Create an error boundary error
    pub fn error_boundary<S: Into<String>>(boundary: S, error: S) -> Self {
        Self::ErrorBoundary {
            boundary: boundary.into(),
            error: error.into(),
        }
    }

    /// Create a fault isolation error
    pub fn fault_isolation<S: Into<String>>(component: S, details: S) -> Self {
        Self::FaultIsolation {
            component: component.into(),
            details: details.into(),
        }
    }

    /// Create a recovery failed error
    pub fn recovery_failed<S: Into<String>>(mechanism: S, reason: S) -> Self {
        Self::RecoveryFailed {
            mechanism: mechanism.into(),
            reason: reason.into(),
        }
    }

    /// Create a storage error from storage::error::Error
    pub fn from_storage_error(err: storage::error::Error) -> Self {
        Self::Storage(err.to_string())
    }

    /// Check if a storage error is recoverable based on its type
    /// This provides a more accurate assessment than string matching
    pub fn is_storage_error_recoverable(err: &storage::error::Error) -> bool {
        use storage::error::Error as StorageError;

        match err {
            // Recoverable: transient errors that can be retried
            StorageError::Io { .. } => true,
            StorageError::Mpsc { .. } => true,
            StorageError::Compaction { .. } => true,
            StorageError::Transaction { .. } => true,
            StorageError::Batch { .. } => true,
            StorageError::System { .. } => true,
            StorageError::KeyNotFound { .. } => true, // Not found is recoverable (can retry or handle)

            // Non-recoverable: structural or configuration errors
            StorageError::Encoding { .. } => false,
            StorageError::InvalidFormat { .. } => false,
            StorageError::Config { .. } => false,
            StorageError::InvalidArgument { .. } => false,
            StorageError::OptionNotDynamicallyModifiable { .. } => false,

            // RocksDB errors need inspection - some are recoverable
            StorageError::Rocks { error, .. } => {
                let msg = error.to_string().to_lowercase();
                // Recoverable RocksDB errors unless corruption is reported
                (msg.contains("busy")
                    || msg.contains("locked")
                    || msg.contains("timeout")
                    || msg.contains("write buffer")
                    || msg.contains("compaction"))
                    && !msg.contains("corrupt")
            }

            // Unknown errors - default to non-recoverable for safety
            StorageError::Unknown { .. } => false,
            StorageError::OptionNone { .. } => false,
            StorageError::RedisErr { .. } => true, // Redis protocol errors are typically recoverable
        }
    }

    /// Create an IO error from std::io::Error
    pub fn from_io_error(err: std::io::Error) -> Self {
        Self::Io(err.to_string())
    }

    /// Check if this error indicates a recoverable condition
    ///
    /// Storage errors are evaluated based on their content to distinguish between:
    /// - Recoverable: transient I/O issues, compaction conflicts, write buffer full,
    ///   timeouts, busy/locked resources, transaction/batch failures
    /// - Non-recoverable: corruption, invalid format, configuration errors, encoding errors
    ///
    /// For more accurate assessment with the actual storage error type, use
    /// `is_storage_error_recoverable()` instead.
    pub fn is_recoverable(&self) -> bool {
        match self {
            Self::NetworkRuntime(_) => true,
            Self::StorageRuntime(_) => true,
            Self::Channel(_) => true,
            Self::Timeout { .. } => true,
            Self::CircuitBreakerOpen { .. } => true,
            Self::RuntimeIsolation { .. } => true,
            Self::ErrorBoundary { .. } => true,
            Self::FaultIsolation { .. } => true,
            Self::Configuration(_) => false,
            Self::Lifecycle(_) => false,
            Self::HealthCheck(_) => true,
            Self::Storage(msg) => {
                // Distinguish between recoverable and non-recoverable storage errors
                // Recoverable: transient I/O issues, compaction conflicts, write buffer full, timeouts
                // Non-recoverable: corruption, invalid format, configuration errors
                let msg_lower = msg.to_lowercase();

                // Non-recoverable errors
                if msg_lower.contains("corrupt")
                    || msg_lower.contains("invalid format")
                    || msg_lower.contains("configuration error")
                    || msg_lower.contains("encoding error")
                {
                    return false;
                }

                // Recoverable errors
                if msg_lower.contains("io error")
                    || msg_lower.contains("compaction")
                    || msg_lower.contains("write buffer")
                    || msg_lower.contains("timeout")
                    || msg_lower.contains("busy")
                    || msg_lower.contains("locked")
                    || msg_lower.contains("mpsc")
                    || msg_lower.contains("transaction")
                    || msg_lower.contains("batch")
                {
                    return true;
                }

                // Default to non-recoverable for unknown storage errors to be safe
                false
            }
            Self::Io(_) => true,
            Self::RecoveryFailed { .. } => false,
        }
    }

    /// Check if this error should trigger circuit breaker
    pub fn should_trigger_circuit_breaker(&self) -> bool {
        matches!(
            self,
            Self::NetworkRuntime(_)
                | Self::StorageRuntime(_)
                | Self::Channel(_)
                | Self::Timeout { .. }
                | Self::RuntimeIsolation { .. }
                | Self::ErrorBoundary { .. }
                | Self::FaultIsolation { .. }
        )
    }

    /// Get the severity level of this error
    pub fn severity(&self) -> ErrorSeverity {
        match self {
            Self::Configuration(_) => ErrorSeverity::Critical,
            Self::Lifecycle(_) => ErrorSeverity::Critical,
            Self::RecoveryFailed { .. } => ErrorSeverity::Critical,
            Self::Storage(_) => ErrorSeverity::High,
            Self::RuntimeIsolation { .. } => ErrorSeverity::High,
            Self::ErrorBoundary { .. } => ErrorSeverity::High,
            Self::FaultIsolation { .. } => ErrorSeverity::Medium,
            Self::CircuitBreakerOpen { .. } => ErrorSeverity::Medium,
            Self::NetworkRuntime(_) => ErrorSeverity::Medium,
            Self::StorageRuntime(_) => ErrorSeverity::Medium,
            Self::Channel(_) => ErrorSeverity::Medium,
            Self::Timeout { .. } => ErrorSeverity::Low,
            Self::HealthCheck(_) => ErrorSeverity::Low,
            Self::Io(_) => ErrorSeverity::Low,
        }
    }
}

/// Error severity levels for monitoring and alerting
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize, serde::Deserialize,
)]
pub enum ErrorSeverity {
    Low = 1,
    Medium = 2,
    High = 3,
    Critical = 4,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_storage_error_recoverability() {
        // Recoverable storage errors
        assert!(
            DualRuntimeError::Storage("IO error: connection failed".to_string()).is_recoverable()
        );
        assert!(
            DualRuntimeError::Storage("Compaction conflict detected".to_string()).is_recoverable()
        );
        assert!(DualRuntimeError::Storage("Write buffer is full".to_string()).is_recoverable());
        assert!(DualRuntimeError::Storage("Transaction timeout".to_string()).is_recoverable());
        assert!(DualRuntimeError::Storage("Database is busy".to_string()).is_recoverable());
        assert!(DualRuntimeError::Storage("Resource locked".to_string()).is_recoverable());
        assert!(DualRuntimeError::Storage("Mpsc channel error".to_string()).is_recoverable());
        assert!(DualRuntimeError::Storage("Batch operation failed".to_string()).is_recoverable());

        // Non-recoverable storage errors
        assert!(
            !DualRuntimeError::Storage("Data corruption detected".to_string()).is_recoverable()
        );
        assert!(
            !DualRuntimeError::Storage("Invalid format: bad header".to_string()).is_recoverable()
        );
        assert!(
            !DualRuntimeError::Storage("Configuration error: invalid path".to_string())
                .is_recoverable()
        );
        assert!(
            !DualRuntimeError::Storage("Encoding error: invalid UTF-8".to_string())
                .is_recoverable()
        );

        // Unknown storage errors default to non-recoverable
        assert!(!DualRuntimeError::Storage("Unknown storage issue".to_string()).is_recoverable());
    }

    #[test]
    fn test_other_error_recoverability() {
        // Recoverable errors
        assert!(DualRuntimeError::NetworkRuntime("connection reset".to_string()).is_recoverable());
        assert!(DualRuntimeError::StorageRuntime("thread panic".to_string()).is_recoverable());
        assert!(DualRuntimeError::Channel("send failed".to_string()).is_recoverable());
        assert!(
            DualRuntimeError::Timeout {
                timeout: Duration::from_secs(5)
            }
            .is_recoverable()
        );
        assert!(
            DualRuntimeError::CircuitBreakerOpen {
                reason: "too many failures".to_string()
            }
            .is_recoverable()
        );
        assert!(DualRuntimeError::Io("file not found".to_string()).is_recoverable());
        assert!(DualRuntimeError::HealthCheck("unhealthy".to_string()).is_recoverable());

        // Non-recoverable errors
        assert!(!DualRuntimeError::Configuration("invalid config".to_string()).is_recoverable());
        assert!(!DualRuntimeError::Lifecycle("already stopped".to_string()).is_recoverable());
        assert!(
            !DualRuntimeError::RecoveryFailed {
                mechanism: "retry".to_string(),
                reason: "max attempts".to_string()
            }
            .is_recoverable()
        );
    }

    #[test]
    fn test_error_severity() {
        assert_eq!(
            DualRuntimeError::Configuration("test".to_string()).severity(),
            ErrorSeverity::Critical
        );
        assert_eq!(
            DualRuntimeError::Lifecycle("test".to_string()).severity(),
            ErrorSeverity::Critical
        );
        assert_eq!(
            DualRuntimeError::Storage("test".to_string()).severity(),
            ErrorSeverity::High
        );
        assert_eq!(
            DualRuntimeError::NetworkRuntime("test".to_string()).severity(),
            ErrorSeverity::Medium
        );
        assert_eq!(
            DualRuntimeError::Timeout {
                timeout: Duration::from_secs(1)
            }
            .severity(),
            ErrorSeverity::Low
        );
    }

    #[test]
    fn test_circuit_breaker_trigger() {
        assert!(
            DualRuntimeError::NetworkRuntime("test".to_string()).should_trigger_circuit_breaker()
        );
        assert!(
            DualRuntimeError::StorageRuntime("test".to_string()).should_trigger_circuit_breaker()
        );
        assert!(DualRuntimeError::Channel("test".to_string()).should_trigger_circuit_breaker());
        assert!(
            DualRuntimeError::Timeout {
                timeout: Duration::from_secs(1)
            }
            .should_trigger_circuit_breaker()
        );

        assert!(
            !DualRuntimeError::Configuration("test".to_string()).should_trigger_circuit_breaker()
        );
        assert!(!DualRuntimeError::Lifecycle("test".to_string()).should_trigger_circuit_breaker());
    }
}
