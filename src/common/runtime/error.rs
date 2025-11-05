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
#[derive(Debug, Error)]
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
    Storage(#[from] storage::error::Error),
    
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
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
}