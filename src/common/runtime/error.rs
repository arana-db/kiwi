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
        Self::CircuitBreakerOpen { reason: reason.into() }
    }
    
    /// Create a runtime isolation error
    pub fn runtime_isolation<S: Into<String>>(runtime: S, reason: S) -> Self {
        Self::RuntimeIsolation { 
            runtime: runtime.into(), 
            reason: reason.into() 
        }
    }
    
    /// Create an error boundary error
    pub fn error_boundary<S: Into<String>>(boundary: S, error: S) -> Self {
        Self::ErrorBoundary { 
            boundary: boundary.into(), 
            error: error.into() 
        }
    }
    
    /// Create a fault isolation error
    pub fn fault_isolation<S: Into<String>>(component: S, details: S) -> Self {
        Self::FaultIsolation { 
            component: component.into(), 
            details: details.into() 
        }
    }
    
    /// Create a recovery failed error
    pub fn recovery_failed<S: Into<String>>(mechanism: S, reason: S) -> Self {
        Self::RecoveryFailed { 
            mechanism: mechanism.into(), 
            reason: reason.into() 
        }
    }

    /// Create a storage error from storage::error::Error
    pub fn from_storage_error(err: storage::error::Error) -> Self {
        Self::Storage(err.to_string())
    }

    /// Create an IO error from std::io::Error
    pub fn from_io_error(err: std::io::Error) -> Self {
        Self::Io(err.to_string())
    }
    
    /// Check if this error indicates a recoverable condition
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
            Self::Storage(_) => false, // Storage errors are typically not recoverable
            Self::Io(_) => true,
            Self::RecoveryFailed { .. } => false,
        }
    }
    
    /// Check if this error should trigger circuit breaker
    pub fn should_trigger_circuit_breaker(&self) -> bool {
        match self {
            Self::NetworkRuntime(_) => true,
            Self::StorageRuntime(_) => true,
            Self::Channel(_) => true,
            Self::Timeout { .. } => true,
            Self::RuntimeIsolation { .. } => true,
            Self::ErrorBoundary { .. } => true,
            Self::FaultIsolation { .. } => true,
            _ => false,
        }
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
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize, serde::Deserialize)]
pub enum ErrorSeverity {
    Low = 1,
    Medium = 2,
    High = 3,
    Critical = 4,
}