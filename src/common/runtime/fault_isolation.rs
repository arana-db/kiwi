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

//! Fault isolation mechanisms for dual runtime architecture
//! 
//! This module provides fault isolation capabilities to ensure that failures
//! in one runtime do not cascade to the other runtime.

use std::sync::Arc;
use tokio::sync::RwLock;
use crate::common::runtime::error::RuntimeError;

/// Fault isolation manager for dual runtime system
pub struct FaultIsolationManager {
    /// Isolation barriers between runtimes
    barriers: Arc<RwLock<Vec<IsolationBarrier>>>,
}

/// Represents an isolation barrier between runtime components
#[derive(Debug, Clone)]
pub struct IsolationBarrier {
    /// Unique identifier for the barrier
    pub id: String,
    /// Whether the barrier is active
    pub active: bool,
    /// Associated runtime component
    pub component: String,
}

impl FaultIsolationManager {
    /// Creates a new fault isolation manager
    pub fn new() -> Self {
        Self {
            barriers: Arc::new(RwLock::new(Vec::new())),
        }
    }

    /// Activates fault isolation for a component
    pub async fn activate_isolation(&self, component: &str) -> Result<(), RuntimeError> {
        let mut barriers = self.barriers.write().await;
        let barrier = IsolationBarrier {
            id: format!("barrier_{}", component),
            active: true,
            component: component.to_string(),
        };
        barriers.push(barrier);
        Ok(())
    }

    /// Deactivates fault isolation for a component
    pub async fn deactivate_isolation(&self, component: &str) -> Result<(), RuntimeError> {
        let mut barriers = self.barriers.write().await;
        barriers.retain(|b| b.component != component);
        Ok(())
    }

    /// Checks if a component is isolated
    pub async fn is_isolated(&self, component: &str) -> bool {
        let barriers = self.barriers.read().await;
        barriers.iter().any(|b| b.component == component && b.active)
    }
}

impl Default for FaultIsolationManager {
    fn default() -> Self {
        Self::new()
    }
}