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

//! Health monitoring for dual runtime architecture
//! 
//! This module provides health check capabilities for monitoring the status
//! of both runtimes in the dual runtime system.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;
use serde::{Deserialize, Serialize};
use crate::common::runtime::error::RuntimeError;

/// Health status of a runtime component
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum HealthStatus {
    /// Component is healthy and operational
    Healthy,
    /// Component is degraded but still functional
    Degraded,
    /// Component is unhealthy and may not be functional
    Unhealthy,
    /// Component status is unknown
    Unknown,
}

/// Health check result for a component
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthCheck {
    /// Component name
    pub component: String,
    /// Current health status
    pub status: HealthStatus,
    /// Last check timestamp
    pub last_check: Instant,
    /// Optional message with additional details
    pub message: Option<String>,
    /// Response time for the health check
    pub response_time: Duration,
}

/// Health monitor for dual runtime system
pub struct HealthMonitor {
    /// Health checks for all components
    checks: Arc<RwLock<HashMap<String, HealthCheck>>>,
    /// Health check interval
    check_interval: Duration,
}

impl HealthMonitor {
    /// Creates a new health monitor
    pub fn new(check_interval: Duration) -> Self {
        Self {
            checks: Arc::new(RwLock::new(HashMap::new())),
            check_interval,
        }
    }

    /// Registers a component for health monitoring
    pub async fn register_component(&self, component: &str) -> Result<(), RuntimeError> {
        let mut checks = self.checks.write().await;
        let health_check = HealthCheck {
            component: component.to_string(),
            status: HealthStatus::Unknown,
            last_check: Instant::now(),
            message: None,
            response_time: Duration::from_millis(0),
        };
        checks.insert(component.to_string(), health_check);
        Ok(())
    }

    /// Updates the health status of a component
    /// 
    /// If the component is not registered, it will be auto-registered with the provided status.
    /// The response_time is reset to zero since this is a manual update, not a measured check.
    pub async fn update_health(&self, component: &str, status: HealthStatus, message: Option<String>) -> Result<(), RuntimeError> {
        let mut checks = self.checks.write().await;
        if let Some(check) = checks.get_mut(component) {
            check.status = status;
            check.last_check = Instant::now();
            check.message = message;
            // Reset response_time to zero for manual updates (not measured checks)
            check.response_time = Duration::from_millis(0);
        } else {
            // Auto-register component if not found (consistent with perform_health_check)
            let health_check = HealthCheck {
                component: component.to_string(),
                status,
                last_check: Instant::now(),
                message,
                response_time: Duration::from_millis(0),
            };
            checks.insert(component.to_string(), health_check);
        }
        Ok(())
    }

    /// Gets the health status of a component
    pub async fn get_health(&self, component: &str) -> Option<HealthCheck> {
        let checks = self.checks.read().await;
        checks.get(component).cloned()
    }

    /// Gets the overall health status of all components
    pub async fn get_overall_health(&self) -> HealthStatus {
        let checks = self.checks.read().await;
        let statuses: Vec<&HealthStatus> = checks.values().map(|c| &c.status).collect();
        
        if statuses.is_empty() {
            return HealthStatus::Unknown;
        }
        
        if statuses.iter().any(|&s| s == &HealthStatus::Unhealthy) {
            HealthStatus::Unhealthy
        } else if statuses.iter().any(|&s| s == &HealthStatus::Degraded) {
            HealthStatus::Degraded
        } else if statuses.iter().all(|&s| s == &HealthStatus::Healthy) {
            HealthStatus::Healthy
        } else {
            HealthStatus::Unknown
        }
    }

    /// Performs a health check on a component
    pub async fn perform_health_check(&self, component: &str) -> Result<HealthCheck, RuntimeError> {
        let start_time = Instant::now();
        
        // Simulate health check logic - in real implementation this would
        // check actual component health (network connectivity, resource usage, etc.)
        let status = HealthStatus::Healthy;
        let response_time = start_time.elapsed();
        
        let health_check = HealthCheck {
            component: component.to_string(),
            status,
            last_check: Instant::now(),
            message: Some("Health check completed successfully".to_string()),
            response_time,
        };
        
        // Update stored health check
        let mut checks = self.checks.write().await;
        checks.insert(component.to_string(), health_check.clone());
        
        Ok(health_check)
    }
}

impl Default for HealthMonitor {
    fn default() -> Self {
        Self::new(Duration::from_secs(30))
    }
}