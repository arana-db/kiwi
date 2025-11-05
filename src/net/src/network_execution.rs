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

//! Network command execution for dual runtime architecture
//! 
//! This module provides network-aware command execution that uses StorageClient
//! instead of direct storage access, enabling communication between network
//! and storage runtimes.

use std::sync::Arc;

use client::Client;
use cmd::Cmd;


use crate::storage_client::StorageClient;

/// Network command execution context for dual runtime architecture
/// 
/// This struct contains the necessary components for executing commands
/// in the network runtime while communicating with the storage runtime
/// through StorageClient.
pub struct NetworkCmdExecution {
    /// The command to execute
    pub cmd: Arc<dyn Cmd>,
    /// The client connection
    pub client: Arc<Client>,
    /// The storage client for network-to-storage communication
    pub storage_client: Arc<StorageClient>,
}

impl NetworkCmdExecution {
    /// Create a new NetworkCmdExecution
    pub fn new(
        cmd: Arc<dyn Cmd>,
        client: Arc<Client>,
        storage_client: Arc<StorageClient>,
    ) -> Self {
        Self {
            cmd,
            client,
            storage_client,
        }
    }
}