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

use crate::types::{LogIndex, NodeId, Term};
use log::{debug, error, info, trace, warn};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt;
use std::io::Write;
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

/// Configuration for Raft logger
#[derive(Debug, Clone)]
pub struct RaftLoggerConfig {
    /// Maximum number of log entries to keep in memory buffer
    pub max_buffer_size: usize,
    /// Minimum log level to record
    pub min_log_level: LogLevel,
    /// Whether to enable structured JSON logging
    pub structured_logging: bool,
    /// Whether to log to file
    pub log_to_file: bool,
    /// Log file path (if log_to_file is true)
    pub log_file_path: Option<String>,
    /// Maximum log file size in bytes before rotation
    pub max_log_file_size: usize,
    /// Number of rotated log files to keep
    pub max_log_files: usize,
}

impl Default for RaftLoggerConfig {
    fn default() -> Self {
        Self {
            max_buffer_size: 1000,
            min_log_level: LogLevel::Info,
            structured_logging: true,
            log_to_file: false,
            log_file_path: None,
            max_log_file_size: 10 * 1024 * 1024, // 10MB
            max_log_files: 5,
        }
    }
}

/// Raft logger that provides structured logging for Raft operations
pub struct RaftLogger {
    node_id: NodeId,
    config: RaftLoggerConfig,
    log_buffer: Arc<Mutex<Vec<RaftLogEntry>>>,
    log_file: Arc<Mutex<Option<std::fs::File>>>,
    current_log_file_size: Arc<Mutex<usize>>,
}

impl RaftLogger {
    /// Create a new Raft logger with default configuration
    pub fn new(node_id: NodeId) -> Self {
        Self::with_config(node_id, RaftLoggerConfig::default())
    }

    /// Create a new Raft logger with custom configuration
    pub fn with_config(node_id: NodeId, config: RaftLoggerConfig) -> Self {
        let log_file = if config.log_to_file {
            if let Some(ref path) = config.log_file_path {
                match std::fs::OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(path)
                {
                    Ok(file) => Some(file),
                    Err(e) => {
                        log::error!("Failed to open log file {}: {}", path, e);
                        None
                    }
                }
            } else {
                None
            }
        } else {
            None
        };

        Self {
            node_id,
            config,
            log_buffer: Arc::new(Mutex::new(Vec::new())),
            log_file: Arc::new(Mutex::new(log_file)),
            current_log_file_size: Arc::new(Mutex::new(0)),
        }
    }

    /// Log a Raft event
    pub fn log_event(&self, level: LogLevel, event: RaftEvent, context: HashMap<String, String>) {
        // Check if we should log this level
        if !self.should_log_level(&level) {
            return;
        }

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
        if let Ok(mut buffer) = self.log_buffer.lock() {
            buffer.push(log_entry.clone());

            // Keep buffer size under limit
            if buffer.len() > self.config.max_buffer_size {
                buffer.remove(0);
            }
        } else {
            log::error!("Failed to acquire log buffer lock (poisoned)");
        }

        // Write to file if configured
        if self.config.log_to_file {
            self.write_to_file(&log_entry);
        }

        // Log to standard logger
        if self.config.structured_logging {
            self.log_structured(&log_entry);
        } else {
            self.log_traditional(&log_entry);
        }
    }

    /// Check if we should log at this level
    fn should_log_level(&self, level: &LogLevel) -> bool {
        self.level_to_priority(level) >= self.level_to_priority(&self.config.min_log_level)
    }

    /// Convert log level to numeric priority for comparison
    fn level_to_priority(&self, level: &LogLevel) -> u8 {
        match level {
            LogLevel::Trace => 0,
            LogLevel::Debug => 1,
            LogLevel::Info => 2,
            LogLevel::Warn => 3,
            LogLevel::Error => 4,
        }
    }

    /// Write structured JSON log entry
    fn log_structured(&self, entry: &RaftLogEntry) {
        if let Ok(json) = serde_json::to_string(entry) {
            match entry.level {
                LogLevel::Trace => trace!("{}", json),
                LogLevel::Debug => debug!("{}", json),
                LogLevel::Info => info!("{}", json),
                LogLevel::Warn => warn!("{}", json),
                LogLevel::Error => error!("{}", json),
            }
        } else {
            log::error!("Failed to serialize log entry to JSON");
        }
    }

    /// Write traditional formatted log entry
    fn log_traditional(&self, entry: &RaftLogEntry) {
        let event_str = format!("{:?}", entry.event);
        let context_str = if entry.context.is_empty() {
            String::new()
        } else {
            format!(
                " [{}]",
                entry
                    .context
                    .iter()
                    .map(|(k, v)| format!("{}={}", k, v))
                    .collect::<Vec<_>>()
                    .join(", ")
            )
        };

        let message = format!("[Node {}] {}{}", entry.node_id, event_str, context_str);

        match entry.level {
            LogLevel::Trace => trace!("{}", message),
            LogLevel::Debug => debug!("{}", message),
            LogLevel::Info => info!("{}", message),
            LogLevel::Warn => warn!("{}", message),
            LogLevel::Error => error!("{}", message),
        }
    }

    /// Write log entry to file
    fn write_to_file(&self, entry: &RaftLogEntry) {
        if let Ok(mut file_opt) = self.log_file.lock() {
            if let Some(ref mut file) = *file_opt {
                let log_line = if self.config.structured_logging {
                    match serde_json::to_string(entry) {
                        Ok(json) => format!("{}\n", json),
                        Err(_) => return,
                    }
                } else {
                    let timestamp = chrono::DateTime::from_timestamp(entry.timestamp as i64, 0)
                        .map(|dt| dt.format("%Y-%m-%d %H:%M:%S").to_string())
                        .unwrap_or_else(|| entry.timestamp.to_string());

                    format!(
                        "{} [{}] [Node {}] {:?} {:?}\n",
                        timestamp, entry.level, entry.node_id, entry.event, entry.context
                    )
                };

                if let Err(e) = std::io::Write::write_all(file, log_line.as_bytes()) {
                    log::error!("Failed to write to log file: {}", e);
                } else {
                    // Update file size and check for rotation
                    if let Ok(mut size) = self.current_log_file_size.lock() {
                        *size += log_line.len();
                        if *size > self.config.max_log_file_size {
                            self.rotate_log_file();
                            *size = 0;
                        }
                    }
                }
            }
        }
    }

    /// Rotate log file when it gets too large
    fn rotate_log_file(&self) {
        if let Some(ref log_path) = self.config.log_file_path {
            // Close current file
            if let Ok(mut file_opt) = self.log_file.lock() {
                *file_opt = None;
            }

            // Rotate existing files
            for i in (1..self.config.max_log_files).rev() {
                let old_path = format!("{}.{}", log_path, i);
                let new_path = format!("{}.{}", log_path, i + 1);
                let _ = std::fs::rename(&old_path, &new_path);
            }

            // Move current log to .1
            let backup_path = format!("{}.1", log_path);
            let _ = std::fs::rename(log_path, &backup_path);

            // Create new log file
            match std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(log_path)
            {
                Ok(file) => {
                    if let Ok(mut file_opt) = self.log_file.lock() {
                        *file_opt = Some(file);
                    }
                }
                Err(e) => {
                    log::error!("Failed to create new log file after rotation: {}", e);
                }
            }
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
    pub fn log_election(
        &self,
        event_type: ElectionEventType,
        term: Term,
        candidate_id: NodeId,
        votes_received: usize,
        total_nodes: usize,
    ) {
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
        context.insert(
            "votes".to_string(),
            format!("{}/{}", votes_received, total_nodes),
        );

        self.log_event(LogLevel::Info, event, context);
    }

    /// Log replication event
    pub fn log_replication(
        &self,
        event_type: ReplicationEventType,
        target_node: NodeId,
        log_index: LogIndex,
        term: Term,
        success: bool,
        error: Option<String>,
    ) {
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

        let level = if success {
            LogLevel::Debug
        } else {
            LogLevel::Warn
        };
        self.log_event(level, event, context);
    }

    /// Log client request
    pub fn log_client_request(
        &self,
        request_id: &str,
        command: &str,
        processing_time_ms: u64,
        success: bool,
        error: Option<String>,
    ) {
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

        let level = if success {
            LogLevel::Debug
        } else {
            LogLevel::Error
        };
        self.log_event(level, event, context);
    }

    /// Get recent log entries
    pub fn get_recent_logs(&self, count: usize) -> Vec<RaftLogEntry> {
        if let Ok(buffer) = self.log_buffer.lock() {
            let start = if buffer.len() > count {
                buffer.len() - count
            } else {
                0
            };
            buffer[start..].to_vec()
        } else {
            log::error!("Failed to acquire log buffer lock (poisoned)");
            Vec::new()
        }
    }

    /// Get all log entries
    pub fn get_all_logs(&self) -> Vec<RaftLogEntry> {
        if let Ok(buffer) = self.log_buffer.lock() {
            buffer.clone()
        } else {
            log::error!("Failed to acquire log buffer lock (poisoned)");
            Vec::new()
        }
    }

    /// Clear log buffer
    pub fn clear_logs(&self) {
        if let Ok(mut buffer) = self.log_buffer.lock() {
            buffer.clear();
        } else {
            log::error!("Failed to acquire log buffer lock (poisoned)");
        }
    }

    /// Log snapshot event
    pub fn log_snapshot(
        &self,
        event_type: SnapshotEventType,
        last_included_index: LogIndex,
        last_included_term: Term,
        size_bytes: u64,
        duration_ms: u64,
    ) {
        let event = RaftEvent::Snapshot {
            event_type: event_type.clone(),
            last_included_index,
            last_included_term,
            size_bytes,
            duration_ms,
        };

        let mut context = HashMap::new();
        context.insert("snapshot_type".to_string(), format!("{:?}", event_type));
        context.insert("last_index".to_string(), last_included_index.to_string());
        context.insert("last_term".to_string(), last_included_term.to_string());
        context.insert(
            "size_mb".to_string(),
            format!("{:.2}", size_bytes as f64 / 1024.0 / 1024.0),
        );
        context.insert("duration_ms".to_string(), duration_ms.to_string());

        let level = match event_type {
            SnapshotEventType::Failed => LogLevel::Error,
            _ => LogLevel::Info,
        };

        self.log_event(level, event, context);
    }

    /// Log network event
    pub fn log_network(
        &self,
        event_type: NetworkEventType,
        peer_id: NodeId,
        message_type: &str,
        bytes: u64,
        latency_ms: Option<u64>,
        error: Option<String>,
    ) {
        let event = RaftEvent::Network {
            event_type: event_type.clone(),
            peer_id,
            message_type: message_type.to_string(),
            bytes,
            latency_ms,
            error: error.clone(),
        };

        let mut context = HashMap::new();
        context.insert("network_type".to_string(), format!("{:?}", event_type));
        context.insert("peer".to_string(), peer_id.to_string());
        context.insert("message".to_string(), message_type.to_string());
        context.insert("bytes".to_string(), bytes.to_string());

        if let Some(latency) = latency_ms {
            context.insert("latency_ms".to_string(), latency.to_string());
        }

        let level = if error.is_some() {
            LogLevel::Error
        } else {
            LogLevel::Debug
        };
        self.log_event(level, event, context);
    }

    /// Log configuration change event
    pub fn log_config_change(
        &self,
        event_type: ConfigChangeEventType,
        old_config: Vec<NodeId>,
        new_config: Vec<NodeId>,
        success: bool,
        error: Option<String>,
    ) {
        let event = RaftEvent::ConfigChange {
            event_type: event_type.clone(),
            old_config: old_config.clone(),
            new_config: new_config.clone(),
            success,
            error: error.clone(),
        };

        let mut context = HashMap::new();
        context.insert("change_type".to_string(), format!("{:?}", event_type));
        context.insert("old_nodes".to_string(), format!("{:?}", old_config));
        context.insert("new_nodes".to_string(), format!("{:?}", new_config));
        context.insert("success".to_string(), success.to_string());

        let level = if success {
            LogLevel::Info
        } else {
            LogLevel::Error
        };
        self.log_event(level, event, context);
    }

    /// Log performance metrics periodically
    pub fn log_performance_metrics(&self, metrics: &PerformanceMetrics) {
        let mut context = HashMap::new();
        context.insert(
            "avg_latency_ms".to_string(),
            format!("{:.2}", metrics.avg_request_latency_ms),
        );
        context.insert(
            "p95_latency_ms".to_string(),
            format!("{:.2}", metrics.p95_request_latency_ms),
        );
        context.insert(
            "p99_latency_ms".to_string(),
            format!("{:.2}", metrics.p99_request_latency_ms),
        );
        context.insert(
            "rps".to_string(),
            format!("{:.2}", metrics.requests_per_second),
        );
        context.insert(
            "memory_mb".to_string(),
            format!("{:.2}", metrics.memory_usage_bytes as f64 / 1024.0 / 1024.0),
        );

        let event = RaftEvent::ClientRequest {
            request_id: "performance_metrics".to_string(),
            command: "METRICS".to_string(),
            processing_time_ms: 0,
            success: true,
            error: None,
        };

        self.log_event(LogLevel::Info, event, context);
    }

    /// Get log statistics
    pub fn get_log_stats(&self) -> LogStats {
        if let Ok(buffer) = self.log_buffer.lock() {
            let mut stats = LogStats::default();

            for entry in buffer.iter() {
                stats.total_entries += 1;
                match entry.level {
                    LogLevel::Trace => stats.trace_count += 1,
                    LogLevel::Debug => stats.debug_count += 1,
                    LogLevel::Info => stats.info_count += 1,
                    LogLevel::Warn => stats.warn_count += 1,
                    LogLevel::Error => stats.error_count += 1,
                }
            }

            stats.buffer_size = buffer.len();
            stats.max_buffer_size = self.config.max_buffer_size;

            if let Ok(file_size) = self.current_log_file_size.lock() {
                stats.current_file_size = *file_size;
            }

            stats
        } else {
            LogStats::default()
        }
    }

    /// Set minimum log level
    pub fn set_min_log_level(&mut self, level: LogLevel) {
        self.config.min_log_level = level;
    }

    /// Enable or disable structured logging
    pub fn set_structured_logging(&mut self, enabled: bool) {
        self.config.structured_logging = enabled;
    }
}

/// Configuration for debug information collection
#[derive(Debug, Clone)]
pub struct DebugConfig {
    /// Whether to collect detailed election debugging info
    pub collect_election_details: bool,
    /// Whether to collect replication debugging info
    pub collect_replication_details: bool,
    /// Whether to collect snapshot debugging info
    pub collect_snapshot_details: bool,
    /// Maximum number of debug snapshots to keep
    pub max_debug_snapshots: usize,
    /// Interval for automatic debug snapshot collection (in seconds)
    pub auto_snapshot_interval_secs: u64,
}

impl Default for DebugConfig {
    fn default() -> Self {
        Self {
            collect_election_details: true,
            collect_replication_details: true,
            collect_snapshot_details: true,
            max_debug_snapshots: 10,
            auto_snapshot_interval_secs: 300, // 5 minutes
        }
    }
}

/// Debugging utilities for Raft cluster state inspection
pub struct RaftDebugger {
    node_id: NodeId,
    config: DebugConfig,
    debug_snapshots: Arc<Mutex<Vec<DebugSnapshot>>>,
    election_debug_info: Arc<Mutex<Vec<ElectionDebugInfo>>>,
    replication_debug_info: Arc<Mutex<HashMap<NodeId, Vec<ReplicationDebugInfo>>>>,
    last_auto_snapshot: Arc<Mutex<SystemTime>>,
}

impl RaftDebugger {
    /// Create a new Raft debugger with default configuration
    pub fn new(node_id: NodeId) -> Self {
        Self::with_config(node_id, DebugConfig::default())
    }

    /// Create a new Raft debugger with custom configuration
    pub fn with_config(node_id: NodeId, config: DebugConfig) -> Self {
        Self {
            node_id,
            config,
            debug_snapshots: Arc::new(Mutex::new(Vec::new())),
            election_debug_info: Arc::new(Mutex::new(Vec::new())),
            replication_debug_info: Arc::new(Mutex::new(HashMap::new())),
            last_auto_snapshot: Arc::new(Mutex::new(SystemTime::now())),
        }
    }

    /// Format cluster state for debugging
    pub fn format_cluster_state(
        &self,
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
    pub fn format_replication_status(
        &self,
        replication_status: &HashMap<NodeId, (LogIndex, bool)>,
    ) -> String {
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
    pub fn create_debug_snapshot(
        &self,
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
        let snapshot = DebugSnapshot {
            timestamp: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|d| d.as_secs())
                .unwrap_or_else(|_| {
                    log::warn!("System clock appears to be before UNIX_EPOCH, using 0");
                    0
                }),
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
        };

        // Store the snapshot
        if let Ok(mut snapshots) = self.debug_snapshots.lock() {
            snapshots.push(snapshot.clone());

            // Keep only the most recent snapshots
            if snapshots.len() > self.config.max_debug_snapshots {
                snapshots.remove(0);
            }
        }

        snapshot
    }

    /// Record election debug information
    pub fn record_election_debug(
        &self,
        election_term: Term,
        candidate_id: NodeId,
        votes_requested: Vec<NodeId>,
        votes_received: Vec<NodeId>,
        votes_denied: Vec<NodeId>,
        election_timeout_ms: u64,
        election_result: ElectionResult,
        additional_info: HashMap<String, String>,
    ) {
        if !self.config.collect_election_details {
            return;
        }

        let debug_info = ElectionDebugInfo {
            timestamp: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|d| d.as_secs())
                .unwrap_or(0),
            election_term,
            candidate_id,
            votes_requested,
            votes_received,
            votes_denied,
            election_timeout_ms,
            election_result,
            additional_info,
        };

        if let Ok(mut election_info) = self.election_debug_info.lock() {
            election_info.push(debug_info);

            // Keep only recent election info (last 50 elections)
            if election_info.len() > 50 {
                election_info.remove(0);
            }
        }
    }

    /// Record replication debug information
    pub fn record_replication_debug(
        &self,
        target_node: NodeId,
        log_index: LogIndex,
        term: Term,
        entries_count: usize,
        success: bool,
        latency_ms: u64,
        error_details: Option<String>,
        retry_count: u32,
    ) {
        if !self.config.collect_replication_details {
            return;
        }

        let debug_info = ReplicationDebugInfo {
            timestamp: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|d| d.as_secs())
                .unwrap_or(0),
            log_index,
            term,
            entries_count,
            success,
            latency_ms,
            error_details,
            retry_count,
        };

        if let Ok(mut replication_info) = self.replication_debug_info.lock() {
            let node_info = replication_info.entry(target_node).or_insert_with(Vec::new);
            node_info.push(debug_info);

            // Keep only recent replication info per node (last 100 operations)
            if node_info.len() > 100 {
                node_info.remove(0);
            }
        }
    }

    /// Get recent election debug information
    pub fn get_election_debug_info(&self, count: usize) -> Vec<ElectionDebugInfo> {
        if let Ok(election_info) = self.election_debug_info.lock() {
            let start = if election_info.len() > count {
                election_info.len() - count
            } else {
                0
            };
            election_info[start..].to_vec()
        } else {
            Vec::new()
        }
    }

    /// Get replication debug information for a specific node
    pub fn get_replication_debug_info(
        &self,
        node_id: NodeId,
        count: usize,
    ) -> Vec<ReplicationDebugInfo> {
        if let Ok(replication_info) = self.replication_debug_info.lock() {
            if let Some(node_info) = replication_info.get(&node_id) {
                let start = if node_info.len() > count {
                    node_info.len() - count
                } else {
                    0
                };
                node_info[start..].to_vec()
            } else {
                Vec::new()
            }
        } else {
            Vec::new()
        }
    }

    /// Get all stored debug snapshots
    pub fn get_debug_snapshots(&self) -> Vec<DebugSnapshot> {
        if let Ok(snapshots) = self.debug_snapshots.lock() {
            snapshots.clone()
        } else {
            Vec::new()
        }
    }

    /// Check if it's time for an automatic debug snapshot
    pub fn should_take_auto_snapshot(&self) -> bool {
        if let Ok(last_snapshot) = self.last_auto_snapshot.lock() {
            let elapsed = last_snapshot.elapsed().unwrap_or_default();
            elapsed.as_secs() >= self.config.auto_snapshot_interval_secs
        } else {
            false
        }
    }

    /// Update the last auto snapshot time
    pub fn update_auto_snapshot_time(&self) {
        if let Ok(mut last_snapshot) = self.last_auto_snapshot.lock() {
            *last_snapshot = SystemTime::now();
        }
    }

    /// Generate comprehensive debug report
    pub fn generate_debug_report(&self) -> DebugReport {
        let snapshots = self.get_debug_snapshots();
        let recent_elections = self.get_election_debug_info(10);

        let mut replication_summary = HashMap::new();
        if let Ok(replication_info) = self.replication_debug_info.lock() {
            for (node_id, info_list) in replication_info.iter() {
                let recent_info = if info_list.len() > 10 {
                    &info_list[info_list.len() - 10..]
                } else {
                    info_list
                };
                replication_summary.insert(*node_id, recent_info.to_vec());
            }
        }

        DebugReport {
            node_id: self.node_id,
            generated_at: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|d| d.as_secs())
                .unwrap_or(0),
            snapshots,
            recent_elections,
            replication_summary,
        }
    }

    /// Clear all debug information
    pub fn clear_debug_info(&self) {
        if let Ok(mut snapshots) = self.debug_snapshots.lock() {
            snapshots.clear();
        }
        if let Ok(mut election_info) = self.election_debug_info.lock() {
            election_info.clear();
        }
        if let Ok(mut replication_info) = self.replication_debug_info.lock() {
            replication_info.clear();
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
        write!(
            f,
            "DebugSnapshot[Node {}, Term {}, State {}, Leader {:?}, LogIndex {}, CommitIndex {}]",
            self.node_id,
            self.current_term,
            self.node_state,
            self.current_leader,
            self.last_log_index,
            self.commit_index
        )
    }
}

/// Statistics about logging activity
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct LogStats {
    pub total_entries: usize,
    pub trace_count: usize,
    pub debug_count: usize,
    pub info_count: usize,
    pub warn_count: usize,
    pub error_count: usize,
    pub buffer_size: usize,
    pub max_buffer_size: usize,
    pub current_file_size: usize,
}

/// Performance metrics structure for logging
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PerformanceMetrics {
    pub avg_request_latency_ms: f64,
    pub p95_request_latency_ms: f64,
    pub p99_request_latency_ms: f64,
    pub requests_per_second: f64,
    pub memory_usage_bytes: u64,
}

/// Election result for debugging
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ElectionResult {
    Won,
    Lost,
    Timeout,
    Aborted,
}

/// Detailed election debugging information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ElectionDebugInfo {
    pub timestamp: u64,
    pub election_term: Term,
    pub candidate_id: NodeId,
    pub votes_requested: Vec<NodeId>,
    pub votes_received: Vec<NodeId>,
    pub votes_denied: Vec<NodeId>,
    pub election_timeout_ms: u64,
    pub election_result: ElectionResult,
    pub additional_info: HashMap<String, String>,
}

/// Detailed replication debugging information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplicationDebugInfo {
    pub timestamp: u64,
    pub log_index: LogIndex,
    pub term: Term,
    pub entries_count: usize,
    pub success: bool,
    pub latency_ms: u64,
    pub error_details: Option<String>,
    pub retry_count: u32,
}

/// Comprehensive debug report
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DebugReport {
    pub node_id: NodeId,
    pub generated_at: u64,
    pub snapshots: Vec<DebugSnapshot>,
    pub recent_elections: Vec<ElectionDebugInfo>,
    pub replication_summary: HashMap<NodeId, Vec<ReplicationDebugInfo>>,
}

impl fmt::Display for DebugReport {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "=== Raft Debug Report for Node {} ===", self.node_id)?;
        writeln!(f, "Generated at: {}", self.generated_at)?;
        writeln!(f, "Debug snapshots: {}", self.snapshots.len())?;
        writeln!(f, "Recent elections: {}", self.recent_elections.len())?;
        writeln!(
            f,
            "Replication info for {} nodes",
            self.replication_summary.len()
        )?;

        if let Some(latest_snapshot) = self.snapshots.last() {
            writeln!(f, "\nLatest State:")?;
            writeln!(f, "  Term: {}", latest_snapshot.current_term)?;
            writeln!(f, "  Leader: {:?}", latest_snapshot.current_leader)?;
            writeln!(f, "  State: {}", latest_snapshot.node_state)?;
            writeln!(f, "  Log Index: {}", latest_snapshot.last_log_index)?;
            writeln!(f, "  Commit Index: {}", latest_snapshot.commit_index)?;
        }

        if let Some(latest_election) = self.recent_elections.last() {
            writeln!(f, "\nLatest Election:")?;
            writeln!(f, "  Term: {}", latest_election.election_term)?;
            writeln!(f, "  Candidate: {}", latest_election.candidate_id)?;
            writeln!(f, "  Result: {:?}", latest_election.election_result)?;
            writeln!(
                f,
                "  Votes: {}/{}",
                latest_election.votes_received.len(),
                latest_election.votes_requested.len()
            )?;
        }

        writeln!(f, "=====================================")
    }
}

/// Performance logger for tracking operation timings and metrics
pub struct PerformanceLogger {
    node_id: NodeId,
    config: PerformanceLogConfig,
    metrics_buffer: Arc<Mutex<Vec<PerformanceMetricEntry>>>,
    operation_timings: Arc<Mutex<HashMap<String, Vec<OperationTiming>>>>,
    periodic_metrics: Arc<Mutex<Vec<PeriodicMetrics>>>,
    last_cleanup: Arc<Mutex<SystemTime>>,
}

/// Configuration for performance logging
#[derive(Debug, Clone)]
pub struct PerformanceLogConfig {
    /// Whether to enable performance logging
    pub enabled: bool,
    /// Maximum number of metric entries to keep in memory
    pub max_metrics_buffer: usize,
    /// Maximum number of operation timings per operation type
    pub max_operation_timings: usize,
    /// Interval for collecting periodic metrics (in seconds)
    pub periodic_metrics_interval_secs: u64,
    /// Whether to log performance metrics to file
    pub log_to_file: bool,
    /// Performance log file path
    pub performance_log_path: Option<String>,
    /// Cleanup interval for old metrics (in seconds)
    pub cleanup_interval_secs: u64,
}

impl Default for PerformanceLogConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            max_metrics_buffer: 1000,
            max_operation_timings: 100,
            periodic_metrics_interval_secs: 60, // 1 minute
            log_to_file: false,
            performance_log_path: None,
            cleanup_interval_secs: 3600, // 1 hour
        }
    }
}

/// Individual performance metric entry
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PerformanceMetricEntry {
    pub timestamp: u64,
    pub operation: String,
    pub duration_ms: u64,
    pub success: bool,
    pub additional_data: HashMap<String, String>,
}

/// Operation timing information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OperationTiming {
    pub timestamp: u64,
    pub duration_ms: u64,
    pub success: bool,
    pub context: HashMap<String, String>,
}

/// Periodic metrics snapshot
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PeriodicMetrics {
    pub timestamp: u64,
    pub operations_per_second: HashMap<String, f64>,
    pub average_latencies: HashMap<String, f64>,
    pub success_rates: HashMap<String, f64>,
    pub memory_usage_bytes: u64,
    pub active_connections: u32,
}

impl PerformanceLogger {
    /// Create a new performance logger
    pub fn new(node_id: NodeId) -> Self {
        Self::with_config(node_id, PerformanceLogConfig::default())
    }

    /// Create a new performance logger with custom configuration
    pub fn with_config(node_id: NodeId, config: PerformanceLogConfig) -> Self {
        Self {
            node_id,
            config,
            metrics_buffer: Arc::new(Mutex::new(Vec::new())),
            operation_timings: Arc::new(Mutex::new(HashMap::new())),
            periodic_metrics: Arc::new(Mutex::new(Vec::new())),
            last_cleanup: Arc::new(Mutex::new(SystemTime::now())),
        }
    }

    /// Record an operation timing
    pub fn record_operation(
        &self,
        operation: &str,
        duration_ms: u64,
        success: bool,
        context: HashMap<String, String>,
    ) {
        if !self.config.enabled {
            return;
        }

        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);

        // Record in metrics buffer
        let metric_entry = PerformanceMetricEntry {
            timestamp,
            operation: operation.to_string(),
            duration_ms,
            success,
            additional_data: context.clone(),
        };

        if let Ok(mut buffer) = self.metrics_buffer.lock() {
            buffer.push(metric_entry);

            // Keep buffer size under limit
            if buffer.len() > self.config.max_metrics_buffer {
                buffer.remove(0);
            }
        }

        // Record in operation timings
        let timing = OperationTiming {
            timestamp,
            duration_ms,
            success,
            context: context.clone(),
        };

        if let Ok(mut timings) = self.operation_timings.lock() {
            let operation_timings = timings
                .entry(operation.to_string())
                .or_insert_with(Vec::new);
            operation_timings.push(timing);

            // Keep timing history under limit
            if operation_timings.len() > self.config.max_operation_timings {
                operation_timings.remove(0);
            }
        }

        // Log to file if configured
        if self.config.log_to_file {
            self.write_performance_log(&format!(
                "{} [Node {}] {} {}ms success={} {:?}",
                timestamp, self.node_id, operation, duration_ms, success, context
            ));
        }
    }

    /// Start a timing measurement
    pub fn start_timing(&self, operation: &str) -> TimingGuard<'_> {
        TimingGuard::new(self, operation.to_string())
    }

    /// Collect periodic metrics
    pub fn collect_periodic_metrics(&self) -> PeriodicMetrics {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);

        let mut operations_per_second = HashMap::new();
        let mut average_latencies = HashMap::new();
        let mut success_rates = HashMap::new();

        if let Ok(timings) = self.operation_timings.lock() {
            let one_minute_ago = timestamp.saturating_sub(60);

            for (operation, timing_list) in timings.iter() {
                // Filter to last minute
                let recent_timings: Vec<_> = timing_list
                    .iter()
                    .filter(|t| t.timestamp >= one_minute_ago)
                    .collect();

                if !recent_timings.is_empty() {
                    // Calculate operations per second
                    let ops_per_sec = recent_timings.len() as f64 / 60.0;
                    operations_per_second.insert(operation.clone(), ops_per_sec);

                    // Calculate average latency
                    let total_latency: u64 = recent_timings.iter().map(|t| t.duration_ms).sum();
                    let avg_latency = total_latency as f64 / recent_timings.len() as f64;
                    average_latencies.insert(operation.clone(), avg_latency);

                    // Calculate success rate
                    let successful = recent_timings.iter().filter(|t| t.success).count();
                    let success_rate = successful as f64 / recent_timings.len() as f64 * 100.0;
                    success_rates.insert(operation.clone(), success_rate);
                }
            }
        }

        let metrics = PeriodicMetrics {
            timestamp,
            operations_per_second,
            average_latencies,
            success_rates,
            memory_usage_bytes: 0, // TODO: Implement actual memory tracking
            active_connections: 0, // TODO: Implement actual connection tracking
        };

        // Store periodic metrics
        if let Ok(mut periodic) = self.periodic_metrics.lock() {
            periodic.push(metrics.clone());

            // Keep only recent periodic metrics (last 24 hours)
            let one_day_ago = timestamp.saturating_sub(24 * 3600);
            periodic.retain(|m| m.timestamp >= one_day_ago);
        }

        metrics
    }

    /// Get performance statistics for an operation
    pub fn get_operation_stats(&self, operation: &str) -> Option<OperationStats> {
        if let Ok(timings) = self.operation_timings.lock() {
            if let Some(timing_list) = timings.get(operation) {
                if timing_list.is_empty() {
                    return None;
                }

                let total_operations = timing_list.len();
                let successful_operations = timing_list.iter().filter(|t| t.success).count();
                let total_duration: u64 = timing_list.iter().map(|t| t.duration_ms).sum();

                let mut durations: Vec<u64> = timing_list.iter().map(|t| t.duration_ms).collect();
                durations.sort_unstable();

                let p50 = durations[durations.len() / 2];
                let p95 = durations[(durations.len() as f64 * 0.95) as usize];
                let p99 = durations[(durations.len() as f64 * 0.99) as usize];

                return Some(OperationStats {
                    operation: operation.to_string(),
                    total_operations,
                    successful_operations,
                    success_rate: successful_operations as f64 / total_operations as f64 * 100.0,
                    average_duration_ms: total_duration as f64 / total_operations as f64,
                    p50_duration_ms: p50,
                    p95_duration_ms: p95,
                    p99_duration_ms: p99,
                    min_duration_ms: durations[0],
                    max_duration_ms: durations[durations.len() - 1],
                });
            }
        }
        None
    }

    /// Get all operation statistics
    pub fn get_all_operation_stats(&self) -> HashMap<String, OperationStats> {
        let mut stats = HashMap::new();

        if let Ok(timings) = self.operation_timings.lock() {
            for operation in timings.keys() {
                if let Some(operation_stats) = self.get_operation_stats(operation) {
                    stats.insert(operation.clone(), operation_stats);
                }
            }
        }

        stats
    }

    /// Get recent periodic metrics
    pub fn get_recent_periodic_metrics(&self, count: usize) -> Vec<PeriodicMetrics> {
        if let Ok(periodic) = self.periodic_metrics.lock() {
            let start = if periodic.len() > count {
                periodic.len() - count
            } else {
                0
            };
            periodic[start..].to_vec()
        } else {
            Vec::new()
        }
    }

    /// Cleanup old metrics
    pub fn cleanup_old_metrics(&self) {
        if let Ok(mut last_cleanup) = self.last_cleanup.lock() {
            let now = SystemTime::now();
            if now
                .duration_since(*last_cleanup)
                .unwrap_or_default()
                .as_secs()
                < self.config.cleanup_interval_secs
            {
                return;
            }
            *last_cleanup = now;
        }

        let cutoff_time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| {
                d.as_secs()
                    .saturating_sub(self.config.cleanup_interval_secs)
            })
            .unwrap_or(0);

        // Cleanup metrics buffer
        if let Ok(mut buffer) = self.metrics_buffer.lock() {
            buffer.retain(|entry| entry.timestamp >= cutoff_time);
        }

        // Cleanup operation timings
        if let Ok(mut timings) = self.operation_timings.lock() {
            for timing_list in timings.values_mut() {
                timing_list.retain(|timing| timing.timestamp >= cutoff_time);
            }
            // Remove empty operation entries
            timings.retain(|_, timing_list| !timing_list.is_empty());
        }
    }

    /// Write performance log to file
    fn write_performance_log(&self, message: &str) {
        if let Some(ref log_path) = self.config.performance_log_path {
            if let Ok(mut file) = std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(log_path)
            {
                let _ = writeln!(file, "{}", message);
            }
        }
    }

    /// Enable or disable performance logging
    pub fn set_enabled(&mut self, enabled: bool) {
        self.config.enabled = enabled;
    }
}

/// Statistics for a specific operation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OperationStats {
    pub operation: String,
    pub total_operations: usize,
    pub successful_operations: usize,
    pub success_rate: f64,
    pub average_duration_ms: f64,
    pub p50_duration_ms: u64,
    pub p95_duration_ms: u64,
    pub p99_duration_ms: u64,
    pub min_duration_ms: u64,
    pub max_duration_ms: u64,
}

/// RAII timing guard for automatic timing measurement
pub struct TimingGuard<'a> {
    logger: &'a PerformanceLogger,
    operation: String,
    start_time: SystemTime,
    context: HashMap<String, String>,
}

impl<'a> TimingGuard<'a> {
    fn new(logger: &'a PerformanceLogger, operation: String) -> Self {
        Self {
            logger,
            operation,
            start_time: SystemTime::now(),
            context: HashMap::new(),
        }
    }

    /// Add context information to the timing
    pub fn add_context(&mut self, key: String, value: String) {
        self.context.insert(key, value);
    }

    /// Complete the timing with success status
    pub fn complete(self, success: bool) {
        let duration = self.start_time.elapsed().unwrap_or_default();
        self.logger.record_operation(
            &self.operation,
            duration.as_millis() as u64,
            success,
            self.context.clone(),
        );
    }
}

impl<'a> Drop for TimingGuard<'a> {
    fn drop(&mut self) {
        let duration = self.start_time.elapsed().unwrap_or_default();
        self.logger.record_operation(
            &self.operation,
            duration.as_millis() as u64,
            true, // Default to success if not explicitly completed
            self.context.clone(),
        );
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
            RaftEvent::StateTransition {
                from,
                to,
                term,
                reason,
            } => {
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
            RaftEvent::Election {
                event_type,
                term,
                candidate_id,
                votes_received,
                total_nodes,
            } => {
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
        logger.config.max_buffer_size = 3; // Set small limit for testing

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
        let state = debugger.format_cluster_state(5, Some(2), "Follower", 100, 95, 90, &[1, 2, 3]);

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
            5,
            Some(2),
            "Follower",
            100,
            95,
            90,
            &[1, 2, 3],
            &replication_status,
            &[],
        );

        assert_eq!(snapshot.node_id, 1);
        assert_eq!(snapshot.current_term, 5);
        assert_eq!(snapshot.current_leader, Some(2));
        assert_eq!(snapshot.node_state, "Follower");
        assert_eq!(snapshot.cluster_members, vec![1, 2, 3]);
        assert_eq!(snapshot.replication_status.len(), 2);
    }
}
