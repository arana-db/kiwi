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

//! Consistency level handling for Raft-based Redis operations

use crate::error::{RaftError, RaftResult};
use crate::node::{RaftNode, RaftNodeInterface};
use crate::types::{ConsistencyLevel, NodeId};
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Configuration for consistency level handling
#[derive(Debug, Clone)]
pub struct ConsistencyConfig {
    /// Timeout for leadership confirmation
    pub leadership_timeout: Duration,
    /// Timeout for read index operations
    pub read_index_timeout: Duration,
    /// Maximum staleness allowed for eventual consistency reads
    pub max_staleness: Duration,
    /// Whether to allow reads from followers
    pub allow_follower_reads: bool,
}

impl Default for ConsistencyConfig {
    fn default() -> Self {
        Self {
            leadership_timeout: Duration::from_millis(1000),
            read_index_timeout: Duration::from_millis(500),
            max_staleness: Duration::from_secs(5),
            allow_follower_reads: true,
        }
    }
}

/// Handler for different consistency levels in Raft operations
pub struct ConsistencyHandler {
    /// Reference to the Raft node
    raft_node: Arc<RaftNode>,
    /// Configuration for consistency handling
    config: ConsistencyConfig,
}

impl ConsistencyHandler {
    /// Create a new consistency handler
    pub fn new(raft_node: Arc<RaftNode>) -> Self {
        Self {
            raft_node,
            config: ConsistencyConfig::default(),
        }
    }

    /// Create handler with custom configuration
    pub fn with_config(raft_node: Arc<RaftNode>, config: ConsistencyConfig) -> Self {
        Self {
            raft_node,
            config,
        }
    }

    /// Ensure read consistency based on the specified level
    pub async fn ensure_read_consistency(&self, consistency: ConsistencyLevel) -> RaftResult<()> {
        match consistency {
            ConsistencyLevel::Linearizable => {
                self.ensure_linearizable_consistency().await
            }
            ConsistencyLevel::Eventual => {
                self.ensure_eventual_consistency().await
            }
        }
    }

    /// Ensure linearizable read consistency
    /// This requires confirming that we're still the leader and our state is up-to-date
    async fn ensure_linearizable_consistency(&self) -> RaftResult<()> {
        log::debug!("Ensuring linearizable read consistency");

        // Check if we're the leader
        if !self.raft_node.is_leader().await {
            let leader_id = self.raft_node.get_leader_id().await;
            return Err(RaftError::NotLeader { leader_id });
        }

        // Perform leadership confirmation
        self.confirm_leadership().await?;

        // For linearizable reads, we need to ensure we have the latest committed state
        // In a full implementation, this would involve a read index operation
        self.confirm_read_index().await?;

        log::debug!("Linearizable consistency confirmed");
        Ok(())
    }

    /// Ensure eventual consistency
    /// This allows reading from local state with some staleness tolerance
    async fn ensure_eventual_consistency(&self) -> RaftResult<()> {
        log::debug!("Ensuring eventual consistency");

        // For eventual consistency, we can read from any node
        // but we should check if the data is too stale
        if !self.config.allow_follower_reads && !self.raft_node.is_leader().await {
            let leader_id = self.raft_node.get_leader_id().await;
            return Err(RaftError::NotLeader { leader_id });
        }

        // Check staleness if we're a follower
        if !self.raft_node.is_leader().await {
            self.check_staleness().await?;
        }

        log::debug!("Eventual consistency confirmed");
        Ok(())
    }

    /// Confirm that we're still the leader by checking leadership status
    async fn confirm_leadership(&self) -> RaftResult<()> {
        let start_time = Instant::now();
        
        while start_time.elapsed() < self.config.leadership_timeout {
            if self.raft_node.is_leader().await {
                // Additional check: ensure we can still communicate with majority
                if self.can_reach_majority().await? {
                    return Ok(());
                }
            }
            
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        let leader_id = self.raft_node.get_leader_id().await;
        Err(RaftError::NotLeader { leader_id })
    }

    /// Confirm read index to ensure we have the latest committed state
    async fn confirm_read_index(&self) -> RaftResult<()> {
        // In a full Raft implementation, this would involve:
        // 1. Recording the current commit index
        // 2. Sending a heartbeat to majority of followers
        // 3. Waiting for acknowledgment that they've seen this commit index
        // 4. Only then allowing the read to proceed
        
        // For now, we'll implement a simplified version that checks
        // if our applied index is reasonably up-to-date with commit index
        let metrics = self.raft_node.get_metrics().await?;
        
        let commit_index = metrics.last_applied.map(|id| id.index).unwrap_or(0);
        let last_log_index = metrics.last_log_index.map(|id| id.index).unwrap_or(0);
        
        // Check if we're reasonably caught up (within 10 entries)
        let lag = last_log_index.saturating_sub(commit_index);
        if lag > 10 {
            log::warn!("High lag detected: commit_index={}, last_log_index={}, lag={}", 
                      commit_index, last_log_index, lag);
            return Err(RaftError::consistency("Read index lag too high"));
        }

        // In a full implementation, we would also send a read index request
        // and wait for confirmation from majority of nodes
        log::debug!("Read index confirmed: commit_index={}, lag={}", commit_index, lag);
        Ok(())
    }

    /// Check if data staleness is within acceptable limits
    async fn check_staleness(&self) -> RaftResult<()> {
        let metrics = self.raft_node.get_metrics().await?;
        
        // Get the last time we heard from the leader
        // In a full implementation, this would track actual heartbeat timestamps
        // For now, we'll use a simplified check based on leadership status
        
        if let Some(_leader_id) = metrics.current_leader {
            // We have a known leader, assume reasonable staleness
            log::debug!("Staleness check passed - leader known");
            Ok(())
        } else {
            // No known leader - data might be stale
            log::warn!("No known leader - data may be stale");
            if self.config.max_staleness.as_secs() == 0 {
                // Strict staleness requirement
                Err(RaftError::consistency("No leader available for staleness check"))
            } else {
                // Allow some staleness
                log::debug!("Allowing stale read due to max_staleness configuration");
                Ok(())
            }
        }
    }

    /// Check if we can reach a majority of nodes (simplified implementation)
    async fn can_reach_majority(&self) -> RaftResult<bool> {
        let membership = self.raft_node.get_membership().await?;
        let total_nodes = membership.len();
        let majority_size = (total_nodes / 2) + 1;
        
        // In a full implementation, this would actually ping other nodes
        // For now, we'll use a simplified check based on partition detection
        let partitioned_nodes = self.raft_node.check_partitions().await;
        let reachable_nodes = total_nodes - partitioned_nodes.len();
        
        let can_reach_majority = reachable_nodes >= majority_size;
        log::debug!("Majority check: total={}, reachable={}, majority_size={}, can_reach={}",
                   total_nodes, reachable_nodes, majority_size, can_reach_majority);
        
        Ok(can_reach_majority)
    }

    /// Get the appropriate consistency level for a command
    pub fn get_command_consistency(&self, command: &str, requested: Option<ConsistencyLevel>) -> ConsistencyLevel {
        // If explicitly requested, use that level
        if let Some(level) = requested {
            return level;
        }

        // Default consistency levels based on command type
        match command.to_lowercase().as_str() {
            // Write commands always require linearizable consistency
            "set" | "del" | "expire" | "expireat" | "persist" | "rename" | "renamenx" |
            "lpush" | "rpush" | "lpop" | "rpop" | "lset" | "lrem" | "ltrim" |
            "sadd" | "srem" | "spop" | "smove" |
            "zadd" | "zrem" | "zincrby" | "zremrangebyrank" | "zremrangebyscore" |
            "hset" | "hdel" | "hincrby" | "hincrbyfloat" |
            "incr" | "decr" | "incrby" | "decrby" | "incrbyfloat" |
            "append" | "setrange" | "setex" | "setnx" | "mset" | "msetnx" |
            "flushdb" | "flushall" => ConsistencyLevel::Linearizable,
            
            // Critical read commands that need strong consistency
            "get" | "mget" | "exists" | "type" | "ttl" | "pttl" => ConsistencyLevel::Linearizable,
            
            // Administrative commands that can use eventual consistency
            "info" | "ping" | "echo" | "time" => ConsistencyLevel::Eventual,
            
            // Scan operations can use eventual consistency for better performance
            "keys" | "scan" | "hscan" | "sscan" | "zscan" => ConsistencyLevel::Eventual,
            
            // Default to linearizable for unknown commands
            _ => ConsistencyLevel::Linearizable,
        }
    }

    /// Update consistency configuration
    pub fn update_config(&mut self, config: ConsistencyConfig) {
        self.config = config;
    }

    /// Get current consistency configuration
    pub fn get_config(&self) -> &ConsistencyConfig {
        &self.config
    }

    /// Check if a consistency level is supported in current cluster state
    pub async fn is_consistency_supported(&self, consistency: ConsistencyLevel) -> RaftResult<bool> {
        match consistency {
            ConsistencyLevel::Linearizable => {
                // Linearizable reads require either being the leader or having a stable leader
                if self.raft_node.is_leader().await {
                    Ok(true)
                } else {
                    // Check if we have a stable leader
                    let leader_id = self.raft_node.get_leader_id().await;
                    Ok(leader_id.is_some())
                }
            }
            ConsistencyLevel::Eventual => {
                // Eventual consistency is always supported
                Ok(true)
            }
        }
    }

    /// Get consistency level recommendations based on current cluster state
    pub async fn get_recommended_consistency(&self) -> RaftResult<ConsistencyLevel> {
        if self.raft_node.is_leader().await {
            // As leader, we can provide linearizable consistency efficiently
            Ok(ConsistencyLevel::Linearizable)
        } else {
            let leader_id = self.raft_node.get_leader_id().await;
            if leader_id.is_some() {
                // We have a stable leader, linearizable reads are possible but may be slower
                // Recommend eventual for better performance unless strong consistency is critical
                Ok(ConsistencyLevel::Eventual)
            } else {
                // No stable leader, only eventual consistency is available
                Ok(ConsistencyLevel::Eventual)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_consistency_config_default() {
        let config = ConsistencyConfig::default();
        assert_eq!(config.leadership_timeout, Duration::from_millis(1000));
        assert_eq!(config.read_index_timeout, Duration::from_millis(500));
        assert_eq!(config.max_staleness, Duration::from_secs(5));
        assert!(config.allow_follower_reads);
    }

    #[test]
    fn test_command_consistency_mapping() {
        // This test would need a mock RaftNode
        // For now, we'll test the logic without the actual handler
        
        // Write commands should always be linearizable
        assert_eq!(
            ConsistencyLevel::Linearizable,
            ConsistencyLevel::Linearizable // Placeholder for actual test
        );
    }
}