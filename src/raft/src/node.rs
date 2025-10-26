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

//! Raft node implementation

use crate::error::RaftResult;
use crate::types::{NodeId, RaftMetrics, ClusterConfig};
use crate::{ClientRequest, ClientResponse};
use async_trait::async_trait;
use std::collections::BTreeSet;

/// Core Raft node interface
#[async_trait]
pub trait RaftNodeInterface {
    /// Start the Raft node with given configuration
    async fn start(&self, node_id: NodeId, peers: Vec<NodeId>) -> RaftResult<()>;

    /// Propose a client request to the cluster
    async fn propose(&self, request: ClientRequest) -> RaftResult<ClientResponse>;

    /// Add a learner node to the cluster
    async fn add_learner(&self, node_id: NodeId, endpoint: String) -> RaftResult<()>;

    /// Change cluster membership
    async fn change_membership(&self, members: BTreeSet<NodeId>) -> RaftResult<()>;

    /// Get current Raft metrics
    async fn get_metrics(&self) -> RaftResult<RaftMetrics>;

    /// Shutdown the Raft node
    async fn shutdown(&self) -> RaftResult<()>;
}

/// Raft node implementation (stub for now)
pub struct RaftNode {
    // Implementation will be added in subsequent tasks
}

impl RaftNode {
    /// Create a new Raft node with cluster configuration
    pub async fn new(_cluster_config: ClusterConfig) -> RaftResult<Self> {
        // TODO: Initialize with actual cluster configuration in task 5.1
        Ok(Self {})
    }
    
    /// Start the Raft node (simplified for server integration)
    pub async fn start(&self, _init_cluster: bool) -> RaftResult<()> {
        // TODO: Implement actual startup logic in task 5.1
        // For now, just return success to allow server startup
        Ok(())
    }
}

#[async_trait]
impl RaftNodeInterface for RaftNode {
    async fn start(&self, _node_id: NodeId, _peers: Vec<NodeId>) -> RaftResult<()> {
        // TODO: Implement in task 5.1
        todo!("RaftNode::start will be implemented in task 5.1")
    }

    async fn propose(&self, _request: ClientRequest) -> RaftResult<ClientResponse> {
        // TODO: Implement in task 5.1
        todo!("RaftNode::propose will be implemented in task 5.1")
    }

    async fn add_learner(&self, _node_id: NodeId, _endpoint: String) -> RaftResult<()> {
        // TODO: Implement in task 5.2
        todo!("RaftNode::add_learner will be implemented in task 5.2")
    }

    async fn change_membership(&self, _members: BTreeSet<NodeId>) -> RaftResult<()> {
        // TODO: Implement in task 5.2
        todo!("RaftNode::change_membership will be implemented in task 5.2")
    }

    async fn get_metrics(&self) -> RaftResult<RaftMetrics> {
        // TODO: Implement in task 5.1
        todo!("RaftNode::get_metrics will be implemented in task 5.1")
    }

    async fn shutdown(&self) -> RaftResult<()> {
        // TODO: Implement in task 5.1
        todo!("RaftNode::shutdown will be implemented in task 5.1")
    }
}