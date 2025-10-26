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

//! Raft logging and debugging support
//!
//! This module provides comprehensive logging for Raft operations and state changes,
//! along with debugging tools for cluster state inspection.

use crate::types::{NodeId, Term, LogIndex};
use log::{debug, info, warn, error, trace};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt;
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

/// Raft event types for structured logging
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RaftEvent {
    /// Node state transitions
    StateTransition {
        from: String,
        to: String,
        term: Term,
        reason: String,
    },
    /// Leader election events
    Election {
        event_type: ElectionEventType,
        term: Term,
        candidate_id: NodeId,
        votes_received: usize,
        total_nodes: usize,
    },
    /// Log replication events
    Replication {
        event_type: ReplicationEventType,
        target_node: NodeId,
        log_index: LogIndex,
        term: Term,
        success: bool,
        error: Option<String>,
    },
    /// Client request events
    ClientRequest {
        request_id: String,
        command: String,
        processing_time_ms: u64,
        success: bool,
        error: Option<String>,
    },
    /// Snapshot events
    Snapshot {
        event_type: SnapshotEventType,
        last_included_index: LogIndex,
        last_included_term: Term,
        size_bytes: u64,
        duration_ms: u64,
    },
    /// Network events
    Network {
        event_type: NetworkEventType,
        peer_id: NodeId,
        message_type: String,
        bytes: u64,
        latency_ms: Option<u64>,
        error: Option<String>,
    },
    /// Configuration change events
    ConfigChange {
        event_type: ConfigChangeEventType,
        old_config: Vec<NodeId>,
        new_config: Vec<NodeId>,
        success: bool,
        error: Option<String>,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ElectionEventType {
    Started,
    VoteRequested,
    VoteReceived,
    VoteGranted,
    VoteDenied,
    Won,
    Lost,
    Timeout,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ReplicationEventType {
    AppendEntries,
    AppendEntriesResponse,
    InstallSnapshot,
    InstallSnapshotResponse,
    HeartbeatSent,
    HeartbeatReceived,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SnapshotEventType {
    Started,
    Completed,
    Failed,
    Installed,
    Sent,
    Received,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum NetworkEventType {
    ConnectionEstablished,
    ConnectionLost,
    MessageSent,
    MessageReceived,
    Timeout,
    Error,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ConfigChangeEventType {
    AddNode,
    RemoveNode,
    ReplaceNode,
    JointConsensus,
}

/// Structured log entry for Raft events
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RaftLogEntry {
    pub timestamp: u64,
    pub node_id: NodeId,
    pub level: LogLevel,
    pub event: RaftEvent,
    pub context: HashMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum LogLevel {
    Trace,
    Debug,
    Info,
    Warn,
    Error,
}

impl fmt::Display for LogLevel {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            LogLevel::Trace => write!(f, "TRACE"),
            LogLevel::Debug => write!(f, "DEBUG"),
            LogLevel::Info => write!(f, "INFO"),
            LogLevel::Warn => write!(f, "WARN"),
            LogLevel::Error => write!(f, "ERROR"),
        }
    }
}

/// Raft logger that provides structured logging for Raft operations
pub struct RaftLogger {
    node_id: NodeId,
    log_buffer: Arc<Mutex<Vec<RaftLogEntry>>>,
    max_buffer_size: usize,
}

impl RaftLogger {
    /// Create a new Raft logger
    pub fn new(node_id: NodeId) -> Self {
        Self {
            node_id,
            log_buffer: Arc::new(Mutex::new(Vec::new())),
            max_buffer_size: 1000, // Keep last 1000 log entries
        }
    }

    /// Log a Raft event
    pub fn log_event(&self, level: LogLevel, event: RaftEvent, context: HashMap<String, String>) {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or_else(|_| {
                log::warn!("System clock appears to be before UNIX_EPOCH, using 0");
                0
            });

        let log_entry = RaftLogEntry {
            timestamp,
            node_id: self.node_id,
            level: level.clone(),
            event: event.clone(),
            context: context.clone(),
        };

        // Add to buffer
        {
            let mut buffer = self.log_buffer.lock().unwrap();
            buffer.push(log_entry.clone());
            
            // Keep buffer size under limit
            if buffer.len() > self.max_buffer_size {
                buffer.remove(0);
            }
        }

        // Log to standard logger
        let event_str = format!("{:?}", event);
        let context_str = if context.is_empty() {
            String::new()
        } else {
            format!(" [{}]", 
                context.iter()
                    .map(|(k, v)| format!("{}={}", k, v))
                    .collect::<Vec<_>>()
                    .join(", ")
            )
        };

        match level {
            LogLevel::Trace => trace!("[Node {}] {}{}", self.node_id, event_str, context_str),
            LogLevel::Debug => debug!("[Node {}] {}{}", self.node_id, event_str, context_str),
            LogLevel::Info => info!("[Node {}] {}{}", self.node_id, event_str, context_str),
            LogLevel::Warn => warn!("[Node {}] {}{}", self.node_id, event_str, context_str),
            LogLevel::Error => error!("[Node {}] {}{}", self.node_id, event_str, context_str),
        }
    }

    /// Log state transition
    pub fn log_state_transition(&self, from: &str, to: &str, term: Term, reason: &str) {
        let event = RaftEvent::StateTransition {
            from: from.to_string(),
            to: to.to_string(),
            term,
            reason: reason.to_string(),
        };
        
        let mut context = HashMap::new();
        context.insert("transition".to_string(), format!("{} -> {}", from, to));
        context.insert("term".to_string(), term.to_string());
        
        self.log_event(LogLevel::Info, event, context);
    }

    /// Log election event
    pub fn log_election(&self, event_type: ElectionEventType, term: Term, candidate_id: NodeId, votes_received: usize, total_nodes: usize) {
        let event = RaftEvent::Election {
            event_type: event_type.clone(),
            term,
            candidate_id,
            votes_received,
            total_nodes,
        };
        
        let mut context = HashMap::new();
        context.insert("election_type".to_string(), format!("{:?}", event_type));
        context.insert("candidate".to_string(), candidate_id.to_string());
        context.insert("votes".to_string(), format!("{}/{}", votes_received, total_nodes));
        
        self.log_event(LogLevel::Info, event, context);
    }

    /// Log replication event
    pub fn log_replication(&self, event_type: ReplicationEventType, target_node: NodeId, log_index: LogIndex, term: Term, success: bool, error: Option<String>) {
        let event = RaftEvent::Replication {
            event_type: event_type.clone(),
            target_node,
            log_index,
            term,
            success,
            error: error.clone(),
        };
        
        let mut context = HashMap::new();
        context.insert("replication_type".to_string(), format!("{:?}", event_type));
        context.insert("target".to_string(), target_node.to_string());
        context.insert("log_index".to_string(), log_index.to_string());
        context.insert("success".to_string(), success.to_string());
        
        let level = if success { LogLevel::Debug } else { LogLevel::Warn };
        self.log_event(level, event, context);
    }

    /// Log client request
    pub fn log_client_request(&self, request_id: &str, command: &str, processing_time_ms: u64, success: bool, error: Option<String>) {
        let event = RaftEvent::ClientRequest {
            request_id: request_id.to_string(),
            command: command.to_string(),
            processing_time_ms,
            success,
            error: error.clone(),
        };
        
        let mut context = HashMap::new();
        context.insert("request_id".to_string(), request_id.to_string());
        context.insert("command".to_string(), command.to_string());
        context.insert("duration_ms".to_string(), processing_time_ms.to_string());
        
        let level = if success { LogLevel::Debug } else { LogLevel::Error };
        self.log_event(level, event, context);
    }

    /// Get recent log entries
    pub fn get_recent_logs(&self, count: usize) -> Vec<RaftLogEntry> {
        let buffer = self.log_buffer.lock().unwrap();
        let start = if buffer.len() > count {
            buffer.len() - count
        } else {
            0
        };
        buffer[start..].to_vec()
    }

    /// Get all log entries
    pub fn get_all_logs(&self) -> Vec<RaftLogEntry> {
        self.log_buffer.lock().unwrap().clone()
    }

    /// Clear log buffer
    pub fn clear_logs(&self) {
        self.log_buffer.lock().unwrap().clear();
    }
}

/// Debugging utilities for Raft cluster state inspection
pub struct RaftDebugger {
    node_id: NodeId,
}

impl RaftDebugger {
    /// Create a new Raft debugger
    pub fn new(node_id: NodeId) -> Self {
        Self { node_id }
    }

    /// Format cluster state for debugging
    pub fn format_cluster_state(&self, 
        current_term: Term,
        current_leader: Option<NodeId>,
        node_state: &str,
        last_log_index: LogIndex,
        commit_index: LogIndex,
        applied_index: LogIndex,
        cluster_members: &[NodeId],
    ) -> String {
        format!(
            "=== Raft Cluster State Debug (Node {}) ===\n\
             Current Term: {}\n\
             Current Leader: {:?}\n\
             Node State: {}\n\
             Last Log Index: {}\n\
             Commit Index: {}\n\
             Applied Index: {}\n\
             Cluster Members: {:?}\n\
             Log Lag: {} entries\n\
             Apply Lag: {} entries\n\
             ==========================================",
            self.node_id,
            current_term,
            current_leader,
            node_state,
            last_log_index,
            commit_index,
            applied_index,
            cluster_members,
            last_log_index.saturating_sub(commit_index),
            commit_index.saturating_sub(applied_index)
        )
    }

    /// Format replication status for debugging
    pub fn format_replication_status(&self, replication_status: &HashMap<NodeId, (LogIndex, bool)>) -> String {
        let mut output = format!("=== Replication Status (Node {}) ===\n", self.node_id);
        
        for (node_id, (matched_index, is_active)) in replication_status {
            output.push_str(&format!(
                "Node {}: matched_index={}, active={}\n",
                node_id, matched_index, is_active
            ));
        }
        
        output.push_str("=====================================");
        output
    }

    /// Create a debug snapshot of current state
    pub fn create_debug_snapshot(&self, 
        current_term: Term,
        current_leader: Option<NodeId>,
        node_state: &str,
        last_log_index: LogIndex,
        commit_index: LogIndex,
        applied_index: LogIndex,
        cluster_members: &[NodeId],
        replication_status: &HashMap<NodeId, (LogIndex, bool)>,
        recent_logs: &[RaftLogEntry],
    ) -> DebugSnapshot {
        DebugSnapshot {
            timestamp: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs(),
            node_id: self.node_id,
            current_term,
            current_leader,
            node_state: node_state.to_string(),
            last_log_index,
            commit_index,
            applied_index,
            cluster_members: cluster_members.to_vec(),
            replication_status: replication_status.clone(),
            recent_logs: recent_logs.to_vec(),
        }
    }
}

/// Debug snapshot containing comprehensive cluster state
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DebugSnapshot {
    pub timestamp: u64,
    pub node_id: NodeId,
    pub current_term: Term,
    pub current_leader: Option<NodeId>,
    pub node_state: String,
    pub last_log_index: LogIndex,
    pub commit_index: LogIndex,
    pub applied_index: LogIndex,
    pub cluster_members: Vec<NodeId>,
    pub replication_status: HashMap<NodeId, (LogIndex, bool)>,
    pub recent_logs: Vec<RaftLogEntry>,
}

impl fmt::Display for DebugSnapshot {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, 
            "DebugSnapshot[Node {}, Term {}, State {}, Leader {:?}, LogIndex {}, CommitIndex {}]",
            self.node_id, self.current_term, self.node_state, 
            self.current_leader, self.last_log_index, self.commit_index
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_raft_logger_creation() {
        let logger = RaftLogger::new(1);
        assert_eq!(logger.node_id, 1);
        assert_eq!(logger.get_all_logs().len(), 0);
    }

    #[test]
    fn test_log_state_transition() {
        let logger = RaftLogger::new(1);
        logger.log_state_transition("Follower", "Candidate", 5, "Election timeout");
        
        let logs = logger.get_all_logs();
        assert_eq!(logs.len(), 1);
        
        match &logs[0].event {
            RaftEvent::StateTransition { from, to, term, reason } => {
                assert_eq!(from, "Follower");
                assert_eq!(to, "Candidate");
                assert_eq!(*term, 5);
                assert_eq!(reason, "Election timeout");
            }
            _ => panic!("Expected StateTransition event"),
        }
    }

    #[test]
    fn test_log_election() {
        let logger = RaftLogger::new(1);
        logger.log_election(ElectionEventType::Won, 3, 1, 3, 5);
        
        let logs = logger.get_all_logs();
        assert_eq!(logs.len(), 1);
        
        match &logs[0].event {
            RaftEvent::Election { event_type, term, candidate_id, votes_received, total_nodes } => {
                assert!(matches!(event_type, ElectionEventType::Won));
                assert_eq!(*term, 3);
                assert_eq!(*candidate_id, 1);
                assert_eq!(*votes_received, 3);
                assert_eq!(*total_nodes, 5);
            }
            _ => panic!("Expected Election event"),
        }
    }

    #[test]
    fn test_log_buffer_limit() {
        let mut logger = RaftLogger::new(1);
        logger.max_buffer_size = 3; // Set small limit for testing
        
        // Add more logs than the limit
        for i in 0..5 {
            logger.log_state_transition("Follower", "Candidate", i, "Test");
        }
        
        let logs = logger.get_all_logs();
        assert_eq!(logs.len(), 3); // Should be limited to max_buffer_size
        
        // Should contain the last 3 entries
        match &logs[0].event {
            RaftEvent::StateTransition { term, .. } => assert_eq!(*term, 2),
            _ => panic!("Expected StateTransition event"),
        }
    }

    #[test]
    fn test_debugger_format_cluster_state() {
        let debugger = RaftDebugger::new(1);
        let state = debugger.format_cluster_state(
            5, Some(2), "Follower", 100, 95, 90, &[1, 2, 3]
        );
        
        assert!(state.contains("Node 1"));
        assert!(state.contains("Current Term: 5"));
        assert!(state.contains("Current Leader: Some(2)"));
        assert!(state.contains("Node State: Follower"));
        assert!(state.contains("Log Lag: 5 entries"));
        assert!(state.contains("Apply Lag: 5 entries"));
    }

    #[test]
    fn test_debug_snapshot_creation() {
        let debugger = RaftDebugger::new(1);
        let mut replication_status = HashMap::new();
        replication_status.insert(2, (95, true));
        replication_status.insert(3, (90, false));
        
        let snapshot = debugger.create_debug_snapshot(
            5, Some(2), "Follower", 100, 95, 90, &[1, 2, 3], 
            &replication_status, &[]
        );
        
        assert_eq!(snapshot.node_id, 1);
        assert_eq!(snapshot.current_term, 5);
        assert_eq!(snapshot.current_leader, Some(2));
        assert_eq!(snapshot.node_state, "Follower");
        assert_eq!(snapshot.cluster_members, vec![1, 2, 3]);
        assert_eq!(snapshot.replication_status.len(), 2);
    }
}