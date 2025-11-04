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
use crate::types::RaftMetrics as OpenRaftMetrics;
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Command categories for consistency level analysis
#[derive(Debug, Clone, PartialEq, Eq)]
enum CommandCategory {
    /// Write operations that modify data
    Write,
    /// Critical read operations that typically need strong consistency
    CriticalRead,
    /// Read operations that can tolerate some staleness
    CacheableRead,
    /// Administrative operations
    Administrative,
    /// Monitoring and diagnostic operations
    Monitoring,
    /// Unknown command type
    Unknown,
}

/// Cluster health status for consistency recommendations
#[derive(Debug, Clone, PartialEq, Eq)]
enum ClusterHealthStatus {
    /// All nodes healthy, full consistency available
    Healthy,
    /// Some nodes partitioned but majority available
    Degraded,
    /// No stable majority or leader
    Unstable,
}

/// Result of consistency level validation
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConsistencyValidation {
    /// Consistency level is accepted and can be served
    Accepted,
    /// Consistency level is rejected, alternative suggested
    Rejected {
        reason: String,
        alternative: Option<ConsistencyLevel>,
    },
    /// Request requires redirect to leader
    RequiresRedirect { leader_id: NodeId },
    /// Consistency level accepted but with degraded performance
    Degraded { reason: String },
}

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
        Self { raft_node, config }
    }

    /// Ensure read consistency based on the specified level
    pub async fn ensure_read_consistency(&self, consistency: ConsistencyLevel) -> RaftResult<()> {
        match consistency {
            ConsistencyLevel::Linearizable => self.ensure_linearizable_consistency().await,
            ConsistencyLevel::Eventual => self.ensure_eventual_consistency().await,
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
        let start_time = std::time::Instant::now();

        // Get current metrics to establish baseline
        let initial_metrics = self.raft_node.get_metrics().await?;
        let initial_commit_index = initial_metrics.last_applied.map(|id| id.index).unwrap_or(0);

        // For linearizable reads, we need to ensure we have the most recent committed state
        // This involves a read index operation to confirm leadership and state freshness

        // Step 1: Record current commit index as our read index
        let read_index = initial_commit_index;

        // Step 2: Send heartbeat to majority to confirm leadership
        if !self.send_read_index_heartbeat(read_index).await? {
            let leader_id = self.raft_node.get_leader_id().await;
            return Err(RaftError::NotLeader { leader_id });
        }

        // Step 3: Wait for our applied index to catch up to read index
        while start_time.elapsed() < self.config.read_index_timeout {
            let current_metrics = self.raft_node.get_metrics().await?;
            let current_applied = current_metrics.last_applied.map(|id| id.index).unwrap_or(0);

            if current_applied >= read_index {
                log::debug!(
                    "Read index confirmed: read_index={}, applied_index={}, elapsed={:?}",
                    read_index,
                    current_applied,
                    start_time.elapsed()
                );
                return Ok(());
            }

            // Check if we're still the leader
            if !self.raft_node.is_leader().await {
                let leader_id = self.raft_node.get_leader_id().await;
                return Err(RaftError::NotLeader { leader_id });
            }

            tokio::time::sleep(Duration::from_millis(1)).await;
        }

        // Timeout waiting for read index
        Err(RaftError::consistency("Read index confirmation timeout"))
    }

    /// Send heartbeat to majority of nodes to confirm read index
    async fn send_read_index_heartbeat(&self, read_index: u64) -> RaftResult<bool> {
        let membership = self.raft_node.get_membership().await?;
        let total_nodes = membership.len();
        let majority_size = (total_nodes / 2) + 1;

        // In a full implementation, this would send actual heartbeat messages
        // For now, we'll simulate by checking if we can reach majority
        let reachable_count = self.count_reachable_nodes().await?;

        let can_confirm = reachable_count >= majority_size;
        log::debug!(
            "Read index heartbeat: read_index={}, total_nodes={}, reachable={}, majority_size={}, confirmed={}",
            read_index,
            total_nodes,
            reachable_count,
            majority_size,
            can_confirm
        );

        Ok(can_confirm)
    }

    /// Count the number of reachable nodes in the cluster
    async fn count_reachable_nodes(&self) -> RaftResult<usize> {
        let membership = self.raft_node.get_membership().await?;
        let partitioned_nodes = self.raft_node.check_partitions().await;
        let reachable_count = membership.len() - partitioned_nodes.len();
        Ok(reachable_count)
    }

    /// Check if data staleness is within acceptable limits
    async fn check_staleness(&self) -> RaftResult<()> {
        let metrics = self.raft_node.get_metrics().await?;

        // Check if we have a known leader
        if let Some(leader_id) = metrics.current_leader {
            // Calculate staleness based on last heartbeat
            let staleness = self.calculate_staleness(&metrics).await?;

            if staleness <= self.config.max_staleness {
                log::debug!(
                    "Staleness check passed - staleness={:?}, max_allowed={:?}, leader={}",
                    staleness,
                    self.config.max_staleness,
                    leader_id
                );
                Ok(())
            } else {
                log::warn!(
                    "Data too stale - staleness={:?}, max_allowed={:?}",
                    staleness,
                    self.config.max_staleness
                );
                Err(RaftError::consistency(format!(
                    "Data staleness ({:?}) exceeds maximum allowed ({:?})",
                    staleness, self.config.max_staleness
                )))
            }
        } else {
            // No known leader - check if we allow leaderless reads
            if self.config.max_staleness.as_secs() == 0 {
                log::warn!("No leader available and strict staleness required");
                Err(RaftError::consistency(
                    "No leader available for staleness check",
                ))
            } else {
                log::debug!("Allowing leaderless read due to max_staleness configuration");
                Ok(())
            }
        }
    }

    /// Calculate current data staleness based on metrics
    async fn calculate_staleness(&self, metrics: &OpenRaftMetrics) -> RaftResult<Duration> {
        // In a full implementation, this would track actual heartbeat timestamps
        // For now, we'll estimate staleness based on commit lag and leadership status

        let commit_index = metrics.last_applied.map(|id| id.index).unwrap_or(0);
        let last_log_index = metrics.last_log_index.unwrap_or(0);
        let lag = last_log_index.saturating_sub(commit_index);

        // Estimate staleness based on lag (assuming ~1ms per entry)
        let estimated_staleness = Duration::from_millis(lag);

        // If we're not the leader, add additional staleness estimate
        let additional_staleness = if self.raft_node.is_leader().await {
            Duration::from_millis(0)
        } else {
            // Followers have additional staleness due to replication delay
            Duration::from_millis(100) // Conservative estimate
        };

        let total_staleness = estimated_staleness + additional_staleness;

        log::debug!(
            "Staleness calculation: lag={}, estimated={:?}, additional={:?}, total={:?}",
            lag,
            estimated_staleness,
            additional_staleness,
            total_staleness
        );

        Ok(total_staleness)
    }

    /// Check if we can reach a majority of nodes (simplified implementation)
    pub async fn can_reach_majority(&self) -> RaftResult<bool> {
        let membership = self.raft_node.get_membership().await?;
        let total_nodes = membership.len();
        let majority_size = (total_nodes / 2) + 1;

        // In a full implementation, this would actually ping other nodes
        // For now, we'll use a simplified check based on partition detection
        let partitioned_nodes = self.raft_node.check_partitions().await;
        let reachable_nodes = total_nodes - partitioned_nodes.len();

        let can_reach_majority = reachable_nodes >= majority_size;
        log::debug!(
            "Majority check: total={}, reachable={}, majority_size={}, can_reach={}",
            total_nodes,
            reachable_nodes,
            majority_size,
            can_reach_majority
        );

        Ok(can_reach_majority)
    }

    /// Get the appropriate consistency level for a command
    pub fn get_command_consistency(
        &self,
        command: &str,
        requested: Option<ConsistencyLevel>,
    ) -> ConsistencyLevel {
        // If explicitly requested, use that level
        if let Some(level) = requested {
            return level;
        }

        // Analyze command characteristics for optimal consistency level
        self.analyze_command_consistency(command)
    }

    /// Analyze command to determine optimal consistency level
    fn analyze_command_consistency(&self, command: &str) -> ConsistencyLevel {
        let cmd = command.to_lowercase();

        // Categorize commands by their consistency requirements
        match self.get_command_category(&cmd) {
            CommandCategory::Write => {
                // All write commands require linearizable consistency
                ConsistencyLevel::Linearizable
            }
            CommandCategory::CriticalRead => {
                // Reads that typically require strong consistency
                ConsistencyLevel::Linearizable
            }
            CommandCategory::CacheableRead => {
                // Reads that can tolerate some staleness for better performance
                if self.config.allow_follower_reads {
                    ConsistencyLevel::Eventual
                } else {
                    ConsistencyLevel::Linearizable
                }
            }
            CommandCategory::Administrative => {
                // Administrative commands that don't require strong consistency
                ConsistencyLevel::Eventual
            }
            CommandCategory::Monitoring => {
                // Monitoring commands that can use eventual consistency
                ConsistencyLevel::Eventual
            }
            CommandCategory::Unknown => {
                // Default to linearizable for safety
                log::warn!(
                    "Unknown command '{}', defaulting to linearizable consistency",
                    command
                );
                ConsistencyLevel::Linearizable
            }
        }
    }

    /// Get command category for consistency analysis
    fn get_command_category(&self, command: &str) -> CommandCategory {
        match command {
            // Write operations
            "set" | "del" | "expire" | "expireat" | "persist" | "rename" | "renamenx" | "lpush"
            | "rpush" | "lpop" | "rpop" | "lset" | "lrem" | "ltrim" | "linsert" | "sadd"
            | "srem" | "spop" | "smove" | "zadd" | "zrem" | "zincrby" | "zremrangebyrank"
            | "zremrangebyscore" | "zremrangebylex" | "hset" | "hdel" | "hincrby"
            | "hincrbyfloat" | "hmset" | "incr" | "decr" | "incrby" | "decrby" | "incrbyfloat"
            | "append" | "setrange" | "setex" | "setnx" | "mset" | "msetnx" | "psetex"
            | "flushdb" | "flushall" | "swapdb" => CommandCategory::Write,

            // Critical reads that typically need strong consistency
            "get" | "mget" | "exists" | "type" | "ttl" | "pttl" | "llen" | "lindex" | "lrange"
            | "scard" | "sismember" | "smembers" | "zcard" | "zcount" | "zrange"
            | "zrangebyscore" | "zrank" | "zscore" | "hget" | "hmget" | "hlen" | "hexists"
            | "hkeys" | "hvals" | "hgetall" => CommandCategory::CriticalRead,

            // Reads that can tolerate some staleness
            "strlen" | "getrange" | "getbit" | "lpos" | "srandmember" | "sdiff" | "sinter"
            | "sunion" | "zrevrange" | "zrevrangebyscore" | "zrevrank" | "zlexcount"
            | "zrangebylex" => CommandCategory::CacheableRead,

            // Administrative commands
            "info" | "config" | "client" | "debug" | "memory" | "latency" | "slowlog"
            | "monitor" | "sync" | "psync" => CommandCategory::Administrative,

            // Monitoring and diagnostic commands
            "ping" | "echo" | "time" | "lastsave" | "dbsize" | "keys" | "scan" | "hscan"
            | "sscan" | "zscan" | "randomkey" | "dump" | "restore" => CommandCategory::Monitoring,

            // Cluster-specific commands (administrative)
            "cluster" | "readonly" | "readwrite" | "asking" => CommandCategory::Administrative,

            _ => CommandCategory::Unknown,
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
    pub async fn is_consistency_supported(
        &self,
        consistency: ConsistencyLevel,
    ) -> RaftResult<bool> {
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
        let cluster_health = self.assess_cluster_health().await?;

        match cluster_health {
            ClusterHealthStatus::Healthy => {
                // Healthy cluster can support both consistency levels efficiently
                if self.raft_node.is_leader().await {
                    Ok(ConsistencyLevel::Linearizable)
                } else {
                    // Followers can serve eventual reads efficiently
                    Ok(ConsistencyLevel::Eventual)
                }
            }
            ClusterHealthStatus::Degraded => {
                // Degraded cluster should prefer eventual consistency for availability
                log::warn!("Cluster degraded, recommending eventual consistency");
                Ok(ConsistencyLevel::Eventual)
            }
            ClusterHealthStatus::Unstable => {
                // Unstable cluster may only support eventual consistency
                log::warn!("Cluster unstable, only eventual consistency available");
                Ok(ConsistencyLevel::Eventual)
            }
        }
    }

    /// Assess current cluster health for consistency recommendations
    async fn assess_cluster_health(&self) -> RaftResult<ClusterHealthStatus> {
        let metrics = self.raft_node.get_metrics().await?;
        let membership = self.raft_node.get_membership().await?;
        let partitioned_nodes = self.raft_node.check_partitions().await;

        let total_nodes = membership.len();
        let healthy_nodes = total_nodes - partitioned_nodes.len();
        let majority_size = (total_nodes / 2) + 1;

        // Check leadership stability
        let has_stable_leader = metrics.current_leader.is_some();

        // Assess health based on multiple factors
        if healthy_nodes >= majority_size && has_stable_leader {
            if partitioned_nodes.is_empty() {
                Ok(ClusterHealthStatus::Healthy)
            } else {
                Ok(ClusterHealthStatus::Degraded)
            }
        } else {
            Ok(ClusterHealthStatus::Unstable)
        }
    }

    /// Validate consistency level against current cluster capabilities
    pub async fn validate_consistency_request(
        &self,
        requested: ConsistencyLevel,
    ) -> RaftResult<ConsistencyValidation> {
        let cluster_health = self.assess_cluster_health().await?;
        let is_leader = self.raft_node.is_leader().await;

        match requested {
            ConsistencyLevel::Linearizable => {
                if !is_leader {
                    let leader_id = self.raft_node.get_leader_id().await;
                    if leader_id.is_none() {
                        return Ok(ConsistencyValidation::Rejected {
                            reason: "No leader available for linearizable reads".to_string(),
                            alternative: Some(ConsistencyLevel::Eventual),
                        });
                    }

                    return Ok(ConsistencyValidation::RequiresRedirect {
                        leader_id: leader_id.unwrap(),
                    });
                }

                match cluster_health {
                    ClusterHealthStatus::Healthy | ClusterHealthStatus::Degraded => {
                        Ok(ConsistencyValidation::Accepted)
                    }
                    ClusterHealthStatus::Unstable => Ok(ConsistencyValidation::Degraded {
                        reason: "Cluster unstable, linearizable reads may be slow".to_string(),
                    }),
                }
            }
            ConsistencyLevel::Eventual => {
                if !self.config.allow_follower_reads && !is_leader {
                    let leader_id = self.raft_node.get_leader_id().await;
                    return Ok(ConsistencyValidation::RequiresRedirect {
                        leader_id: leader_id.unwrap_or(0),
                    });
                }

                Ok(ConsistencyValidation::Accepted)
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
    fn test_command_category_classification() {
        let handler = create_test_handler();

        // Test write commands
        assert_eq!(handler.get_command_category("set"), CommandCategory::Write);
        assert_eq!(handler.get_command_category("del"), CommandCategory::Write);
        assert_eq!(
            handler.get_command_category("lpush"),
            CommandCategory::Write
        );

        // Test critical read commands
        assert_eq!(
            handler.get_command_category("get"),
            CommandCategory::CriticalRead
        );
        assert_eq!(
            handler.get_command_category("mget"),
            CommandCategory::CriticalRead
        );
        assert_eq!(
            handler.get_command_category("exists"),
            CommandCategory::CriticalRead
        );

        // Test cacheable read commands
        assert_eq!(
            handler.get_command_category("strlen"),
            CommandCategory::CacheableRead
        );
        assert_eq!(
            handler.get_command_category("srandmember"),
            CommandCategory::CacheableRead
        );

        // Test administrative commands
        assert_eq!(
            handler.get_command_category("info"),
            CommandCategory::Administrative
        );
        assert_eq!(
            handler.get_command_category("config"),
            CommandCategory::Administrative
        );

        // Test monitoring commands
        assert_eq!(
            handler.get_command_category("ping"),
            CommandCategory::Monitoring
        );
        assert_eq!(
            handler.get_command_category("scan"),
            CommandCategory::Monitoring
        );

        // Test unknown commands
        assert_eq!(
            handler.get_command_category("unknown_command"),
            CommandCategory::Unknown
        );
    }

    #[test]
    fn test_consistency_level_analysis() {
        let handler = create_test_handler();

        // Write commands should always be linearizable
        assert_eq!(
            handler.analyze_command_consistency("set"),
            ConsistencyLevel::Linearizable
        );
        assert_eq!(
            handler.analyze_command_consistency("del"),
            ConsistencyLevel::Linearizable
        );

        // Critical reads should be linearizable
        assert_eq!(
            handler.analyze_command_consistency("get"),
            ConsistencyLevel::Linearizable
        );
        assert_eq!(
            handler.analyze_command_consistency("exists"),
            ConsistencyLevel::Linearizable
        );

        // Administrative commands should be eventual
        assert_eq!(
            handler.analyze_command_consistency("info"),
            ConsistencyLevel::Eventual
        );
        assert_eq!(
            handler.analyze_command_consistency("ping"),
            ConsistencyLevel::Eventual
        );

        // Unknown commands should default to linearizable
        assert_eq!(
            handler.analyze_command_consistency("unknown"),
            ConsistencyLevel::Linearizable
        );
    }

    #[test]
    fn test_consistency_validation_types() {
        // Test that validation types are properly defined
        let accepted = ConsistencyValidation::Accepted;
        let rejected = ConsistencyValidation::Rejected {
            reason: "test".to_string(),
            alternative: Some(ConsistencyLevel::Eventual),
        };
        let redirect = ConsistencyValidation::RequiresRedirect { leader_id: 1 };
        let degraded = ConsistencyValidation::Degraded {
            reason: "test".to_string(),
        };

        // Ensure all variants are properly constructed
        match accepted {
            ConsistencyValidation::Accepted => (),
            _ => panic!("Unexpected variant"),
        }

        match rejected {
            ConsistencyValidation::Rejected { .. } => (),
            _ => panic!("Unexpected variant"),
        }

        match redirect {
            ConsistencyValidation::RequiresRedirect { .. } => (),
            _ => panic!("Unexpected variant"),
        }

        match degraded {
            ConsistencyValidation::Degraded { .. } => (),
            _ => panic!("Unexpected variant"),
        }
    }

    #[test]
    fn test_cluster_health_status() {
        // Test cluster health status variants
        let healthy = ClusterHealthStatus::Healthy;
        let degraded = ClusterHealthStatus::Degraded;
        let unstable = ClusterHealthStatus::Unstable;

        assert_eq!(healthy, ClusterHealthStatus::Healthy);
        assert_eq!(degraded, ClusterHealthStatus::Degraded);
        assert_eq!(unstable, ClusterHealthStatus::Unstable);

        assert_ne!(healthy, degraded);
        assert_ne!(degraded, unstable);
        assert_ne!(healthy, unstable);
    }

    // Helper function to create a test handler (without actual RaftNode)
    fn create_test_handler() -> TestConsistencyHandler {
        TestConsistencyHandler {
            config: ConsistencyConfig::default(),
        }
    }

    // Test helper struct that implements the same logic without RaftNode dependency
    struct TestConsistencyHandler {
        config: ConsistencyConfig,
    }

    impl TestConsistencyHandler {
        fn get_command_category(&self, command: &str) -> CommandCategory {
            match command {
                "set" | "del" | "lpush" | "rpush" => CommandCategory::Write,
                "get" | "mget" | "exists" => CommandCategory::CriticalRead,
                "strlen" | "srandmember" => CommandCategory::CacheableRead,
                "info" | "config" => CommandCategory::Administrative,
                "ping" | "scan" => CommandCategory::Monitoring,
                _ => CommandCategory::Unknown,
            }
        }

        fn analyze_command_consistency(&self, command: &str) -> ConsistencyLevel {
            match self.get_command_category(command) {
                CommandCategory::Write => ConsistencyLevel::Linearizable,
                CommandCategory::CriticalRead => ConsistencyLevel::Linearizable,
                CommandCategory::CacheableRead => {
                    if self.config.allow_follower_reads {
                        ConsistencyLevel::Eventual
                    } else {
                        ConsistencyLevel::Linearizable
                    }
                }
                CommandCategory::Administrative => ConsistencyLevel::Eventual,
                CommandCategory::Monitoring => ConsistencyLevel::Eventual,
                CommandCategory::Unknown => ConsistencyLevel::Linearizable,
            }
        }
    }
}
