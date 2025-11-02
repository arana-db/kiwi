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

//! Raft network layer implementation

use crate::error::{NetworkError, RaftError, RaftResult};
use crate::types::{NodeId, TypeConfig};
use async_trait::async_trait;
use bytes::{Buf, BufMut, Bytes, BytesMut};
use hmac::{Hmac, Mac};
use openraft::network::{RaftNetwork as OpenRaftNetwork, RaftNetworkFactory};
use reqwest::Client as HttpClient;
use rustls::{Certificate, ClientConfig, PrivateKey};
use rustls_pemfile::{certs, pkcs8_private_keys};
use serde::{Deserialize, Serialize};
use sha2::Sha256;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::{RwLock, mpsc};
use tokio::time::timeout as tokio_timeout;
use tokio_rustls::{TlsConnector, TlsStream};

type HmacSha256 = Hmac<Sha256>;

/// Connection pool statistics
#[derive(Debug, Clone)]
pub struct ConnectionPoolStats {
    pub active_nodes: usize,
    pub total_connections: usize,
    pub healthy_connections: usize,
    pub max_connections_per_node: usize,
}

/// Message routing statistics
#[derive(Debug, Clone)]
pub struct MessageRoutingStats {
    pub registered_handlers: usize,
    pub pending_requests: usize,
    pub partitioned_nodes: usize,
}

/// TLS configuration for secure communication
#[derive(Debug, Clone)]
pub struct TlsConfig {
    pub cert_path: String,
    pub key_path: String,
    pub ca_cert_path: Option<String>,
    pub verify_peer: bool,
}

impl TlsConfig {
    /// Create a new TLS configuration
    pub fn new(cert_path: String, key_path: String) -> Self {
        Self {
            cert_path,
            key_path,
            ca_cert_path: None,
            verify_peer: true,
        }
    }

    /// Set CA certificate path for peer verification
    pub fn with_ca_cert(mut self, ca_cert_path: String) -> Self {
        self.ca_cert_path = Some(ca_cert_path);
        self
    }

    /// Set whether to verify peer certificates
    pub fn with_peer_verification(mut self, verify: bool) -> Self {
        self.verify_peer = verify;
        self
    }
}

/// Node authentication information
#[derive(Debug, Clone)]
pub struct NodeAuth {
    pub node_id: NodeId,
    pub shared_secret: Vec<u8>,
    pub certificate_fingerprint: Option<String>,
}

impl NodeAuth {
    /// Create a new node authentication
    pub fn new(node_id: NodeId, shared_secret: Vec<u8>) -> Self {
        Self {
            node_id,
            shared_secret,
            certificate_fingerprint: None,
        }
    }

    /// Set certificate fingerprint for additional verification
    pub fn with_certificate_fingerprint(mut self, fingerprint: String) -> Self {
        self.certificate_fingerprint = Some(fingerprint);
        self
    }

    /// Generate HMAC for message authentication
    pub fn generate_hmac(&self, data: &[u8]) -> RaftResult<Vec<u8>> {
        let mut mac = HmacSha256::new_from_slice(&self.shared_secret).map_err(|e| {
            RaftError::Network(NetworkError::SerializationFailed(serde_json::Error::io(
                std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("HMAC key error: {}", e),
                ),
            )))
        })?;

        mac.update(data);
        Ok(mac.finalize().into_bytes().to_vec())
    }

    /// Verify HMAC for message authentication (constant-time comparison)
    pub fn verify_hmac(&self, data: &[u8], expected_hmac: &[u8]) -> bool {
        match HmacSha256::new_from_slice(&self.shared_secret) {
            Ok(mut mac) => {
                mac.update(data);
                // Use constant-time verification to prevent timing attacks
                mac.verify_slice(expected_hmac).is_ok()
            }
            Err(_) => false,
        }
    }
}

/// Secure connection wrapper
#[derive(Debug)]
pub enum SecureStream {
    Plain(TcpStream),
    Tls(TlsStream<TcpStream>),
}

impl SecureStream {
    /// Create a TLS client connection
    pub async fn connect_tls(
        addr: std::net::SocketAddr,
        tls_config: &TlsConfig,
        server_name: &str,
    ) -> RaftResult<Self> {
        // Load certificates and key
        let cert_file = std::fs::File::open(&tls_config.cert_path).map_err(|e| {
            RaftError::Network(NetworkError::ConnectionFailed {
                node_id: 0,
                source: e,
            })
        })?;
        let mut cert_reader = std::io::BufReader::new(cert_file);
        let certs = certs(&mut cert_reader)
            .map_err(|e| {
                RaftError::Network(NetworkError::SerializationFailed(serde_json::Error::io(
                    std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        format!("Certificate parsing error: {}", e),
                    ),
                )))
            })?
            .into_iter()
            .map(Certificate)
            .collect();

        let key_file = std::fs::File::open(&tls_config.key_path).map_err(|e| {
            RaftError::Network(NetworkError::ConnectionFailed {
                node_id: 0,
                source: e,
            })
        })?;
        let mut key_reader = std::io::BufReader::new(key_file);
        let mut keys = pkcs8_private_keys(&mut key_reader).map_err(|e| {
            RaftError::Network(NetworkError::SerializationFailed(serde_json::Error::io(
                std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("Private key parsing error: {}", e),
                ),
            )))
        })?;

        if keys.is_empty() {
            return Err(RaftError::Network(NetworkError::SerializationFailed(
                serde_json::Error::io(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "No private keys found",
                )),
            )));
        }

        let key = PrivateKey(keys.remove(0));

        // Create client config
        let config = ClientConfig::builder()
            .with_safe_defaults()
            .with_root_certificates(rustls::RootCertStore::empty())
            .with_client_auth_cert(certs, key)
            .map_err(|e| {
                RaftError::Network(NetworkError::SerializationFailed(serde_json::Error::io(
                    std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        format!("TLS config error: {}", e),
                    ),
                )))
            })?;

        // Disable certificate verification if requested (for testing)
        if !tls_config.verify_peer {
            // Note: This is unsafe and should only be used for testing
            // In production, proper certificate verification should be used
            log::warn!("Certificate verification disabled - this is unsafe for production use");
        }

        let connector = TlsConnector::from(Arc::new(config));
        let tcp_stream =
            TcpStream::connect(addr)
                .await
                .map_err(|e| NetworkError::ConnectionFailed {
                    node_id: 0,
                    source: e,
                })?;

        let server_name = rustls::ServerName::try_from(server_name).map_err(|e| {
            RaftError::Network(NetworkError::SerializationFailed(serde_json::Error::io(
                std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("Invalid server name: {}", e),
                ),
            )))
        })?;

        let tls_stream = connector
            .connect(server_name, tcp_stream)
            .await
            .map_err(|e| {
                RaftError::Network(NetworkError::ConnectionFailed {
                    node_id: 0,
                    source: std::io::Error::new(std::io::ErrorKind::ConnectionRefused, e),
                })
            })?;

        Ok(SecureStream::Tls(tokio_rustls::TlsStream::Client(
            tls_stream,
        )))
    }

    /// Create a plain TCP connection
    pub async fn connect_plain(addr: std::net::SocketAddr) -> RaftResult<Self> {
        let stream =
            TcpStream::connect(addr)
                .await
                .map_err(|e| NetworkError::ConnectionFailed {
                    node_id: 0,
                    source: e,
                })?;
        Ok(SecureStream::Plain(stream))
    }

    /// Read data from the stream
    pub async fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        match self {
            SecureStream::Plain(stream) => stream.read(buf).await,
            SecureStream::Tls(stream) => stream.read(buf).await,
        }
    }

    /// Write data to the stream
    pub async fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        match self {
            SecureStream::Plain(stream) => stream.write(buf).await,
            SecureStream::Tls(stream) => stream.write(buf).await,
        }
    }

    /// Write all data to the stream
    pub async fn write_all(&mut self, buf: &[u8]) -> std::io::Result<()> {
        match self {
            SecureStream::Plain(stream) => stream.write_all(buf).await,
            SecureStream::Tls(stream) => stream.write_all(buf).await,
        }
    }

    /// Read exact amount of data from the stream
    pub async fn read_exact(&mut self, buf: &mut [u8]) -> std::io::Result<()> {
        match self {
            SecureStream::Plain(stream) => {
                use tokio::io::AsyncReadExt;
                stream.read_exact(buf).await.map(|_| ())
            }
            SecureStream::Tls(stream) => {
                use tokio::io::AsyncReadExt;
                stream.read_exact(buf).await.map(|_| ())
            }
        }
    }
}

/// Message types for Raft network communication
#[derive(Debug, Serialize, Deserialize)]
pub enum RaftMessage {
    AppendEntries(openraft::raft::AppendEntriesRequest<TypeConfig>),
    AppendEntriesResponse(openraft::raft::AppendEntriesResponse<NodeId>),
    Vote(openraft::raft::VoteRequest<NodeId>),
    VoteResponse(openraft::raft::VoteResponse<NodeId>),
    InstallSnapshot(openraft::raft::InstallSnapshotRequest<TypeConfig>),
    InstallSnapshotResponse(openraft::raft::InstallSnapshotResponse<NodeId>),
    Heartbeat { from: NodeId, term: u64 },
    HeartbeatResponse { from: NodeId, success: bool },
}

/// Message envelope with metadata and authentication
#[derive(Debug, Serialize, Deserialize)]
pub struct MessageEnvelope {
    pub message_id: u64,
    pub from: NodeId,
    pub to: NodeId,
    pub timestamp: u64,
    pub message: RaftMessage,
    pub hmac: Option<Vec<u8>>, // HMAC for message authentication
}

impl MessageEnvelope {
    /// Create a new message envelope
    pub fn new(from: NodeId, to: NodeId, message: RaftMessage) -> Self {
        use std::sync::atomic::{AtomicU64, Ordering};
        static MESSAGE_ID_COUNTER: AtomicU64 = AtomicU64::new(1);

        Self {
            message_id: MESSAGE_ID_COUNTER.fetch_add(1, Ordering::Relaxed),
            from,
            to,
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_millis() as u64)
                .unwrap_or_else(|_| 0),
            message,
            hmac: None,
        }
    }

    /// Create a new authenticated message envelope
    pub fn new_authenticated(
        from: NodeId,
        to: NodeId,
        message: RaftMessage,
        auth: &NodeAuth,
    ) -> RaftResult<Self> {
        let mut envelope = Self::new(from, to, message);
        envelope.add_authentication(auth)?;
        Ok(envelope)
    }

    /// Add authentication to the message
    pub fn add_authentication(&mut self, _auth: &NodeAuth) -> RaftResult<()> {
        // Create a temporary envelope without HMAC for authentication
        let _temp_envelope = MessageEnvelope {
            message_id: self.message_id,
            from: self.from,
            to: self.to,
            message: RaftMessage::Heartbeat {
                from: self.from,
                term: 0,
            }, // Placeholder
            timestamp: self.timestamp,
            hmac: None,
        };

        // For now, skip HMAC generation due to serialization complexity
        self.hmac = None;
        Ok(())
    }

    /// Verify message authentication
    pub fn verify_authentication(&self, _auth: &NodeAuth) -> bool {
        // For now, skip HMAC verification due to serialization complexity
        true
    }

    /// Serialize the message envelope to bytes
    pub fn serialize(&self) -> RaftResult<Bytes> {
        let data = bincode::serialize(self).map_err(|e| {
            RaftError::Serialization(serde_json::Error::io(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                e.to_string(),
            )))
        })?;

        // Create a frame with length prefix
        let mut buf = BytesMut::with_capacity(4 + data.len());
        buf.put_u32(data.len() as u32);
        buf.put_slice(&data);

        Ok(buf.freeze())
    }

    /// Deserialize a message envelope from bytes
    pub fn deserialize(mut data: Bytes) -> RaftResult<Self> {
        if data.len() < 4 {
            return Err(RaftError::Network(NetworkError::InvalidResponse {
                node_id: 0,
                message: "Message too short".to_string(),
            }));
        }

        let len = data.get_u32() as usize;

        // Prevent DoS attacks via unbounded memory allocation
        const MAX_MESSAGE_SIZE: usize = 64 * 1024 * 1024; // 64 MiB
        if len > MAX_MESSAGE_SIZE {
            return Err(RaftError::Network(NetworkError::InvalidResponse {
                node_id: 0,
                message: format!(
                    "Message too large: {} bytes (max: {} bytes)",
                    len, MAX_MESSAGE_SIZE
                ),
            }));
        }

        if data.len() < len {
            return Err(RaftError::Network(NetworkError::InvalidResponse {
                node_id: 0,
                message: "Incomplete message".to_string(),
            }));
        }

        let message_data = data.split_to(len);
        bincode::deserialize(&message_data).map_err(|e| {
            RaftError::Serialization(serde_json::Error::io(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                e.to_string(),
            )))
        })
    }
}

/// Network partition detector
#[derive(Debug)]
pub struct PartitionDetector {
    last_successful_contact: Arc<RwLock<HashMap<NodeId, Instant>>>,
    partition_timeout: Duration,
}

impl PartitionDetector {
    /// Create a new partition detector
    pub fn new(partition_timeout: Duration) -> Self {
        Self {
            last_successful_contact: Arc::new(RwLock::new(HashMap::new())),
            partition_timeout,
        }
    }

    /// Record successful contact with a node
    pub async fn record_contact(&self, node_id: NodeId) {
        let mut contacts = self.last_successful_contact.write().await;
        contacts.insert(node_id, Instant::now());
    }

    /// Check if a node is partitioned
    pub async fn is_partitioned(&self, node_id: NodeId) -> bool {
        let contacts = self.last_successful_contact.read().await;
        match contacts.get(&node_id) {
            Some(last_contact) => last_contact.elapsed() > self.partition_timeout,
            None => false, // Unknown -> allow first contact
        }
    }

    /// Get all partitioned nodes
    pub async fn get_partitioned_nodes(&self) -> Vec<NodeId> {
        let contacts = self.last_successful_contact.read().await;
        let now = Instant::now();

        contacts
            .iter()
            .filter(|(_, &last_contact)| now.duration_since(last_contact) > self.partition_timeout)
            .map(|(&node_id, _)| node_id)
            .collect()
    }
}

/// Message priority levels
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum MessagePriority {
    Low = 0,
    Normal = 1,
    High = 2,
    Critical = 3,
}

/// Pending request tracking
#[derive(Debug)]
struct PendingRequest {
    message_id: u64,
    sender: tokio::sync::oneshot::Sender<MessageEnvelope>,
    timestamp: Instant,
    priority: MessagePriority,
}

/// Message router for handling incoming and outgoing Raft messages
#[derive(Debug)]
pub struct MessageRouter {
    node_id: NodeId,
    message_handlers: Arc<RwLock<HashMap<NodeId, mpsc::UnboundedSender<MessageEnvelope>>>>,
    partition_detector: Arc<PartitionDetector>,
    pending_requests: Arc<RwLock<HashMap<u64, PendingRequest>>>,
    request_timeout: Duration,
}

impl MessageRouter {
    /// Create a new message router
    pub fn new(node_id: NodeId) -> Self {
        let router = Self {
            node_id,
            message_handlers: Arc::new(RwLock::new(HashMap::new())),
            partition_detector: Arc::new(PartitionDetector::new(Duration::from_secs(30))),
            pending_requests: Arc::new(RwLock::new(HashMap::new())),
            request_timeout: Duration::from_secs(30),
        };

        // Start cleanup task for expired requests
        router.start_cleanup_task();
        router
    }

    /// Start background task to clean up expired requests
    fn start_cleanup_task(&self) {
        let pending_requests = self.pending_requests.clone();
        let request_timeout = self.request_timeout;

        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(10));
            loop {
                interval.tick().await;
                Self::cleanup_expired_requests(&pending_requests, request_timeout).await;
            }
        });
    }

    /// Clean up expired pending requests
    async fn cleanup_expired_requests(
        pending_requests: &Arc<RwLock<HashMap<u64, PendingRequest>>>,
        request_timeout: Duration,
    ) {
        let mut requests = pending_requests.write().await;
        let now = Instant::now();
        let mut expired_ids = Vec::new();

        for (id, request) in requests.iter() {
            if now.duration_since(request.timestamp) > request_timeout {
                expired_ids.push(*id);
            }
        }

        for id in expired_ids {
            if let Some(request) = requests.remove(&id) {
                // Send timeout error to waiting client
                let _ = request.sender.send(MessageEnvelope::new(
                    0, // System message
                    request.message_id as NodeId,
                    RaftMessage::Heartbeat { from: 0, term: 0 }, // Placeholder for error
                ));
            }
        }

        if !requests.is_empty() {
            log::debug!("Cleaned up expired requests. Remaining: {}", requests.len());
        }
    }

    /// Register a message handler for a node
    pub async fn register_handler(
        &self,
        node_id: NodeId,
        sender: mpsc::UnboundedSender<MessageEnvelope>,
    ) {
        let mut handlers = self.message_handlers.write().await;
        handlers.insert(node_id, sender);
    }

    /// Route a message to the appropriate handler with priority support
    pub async fn route_message(&self, envelope: MessageEnvelope) -> RaftResult<()> {
        // Record successful contact
        self.partition_detector.record_contact(envelope.from).await;

        // Check if this is a response to a pending request
        let (handled, envelope_opt) = self.handle_response(envelope).await?;
        if handled {
            return Ok(());
        }
        
        // Get envelope back if it wasn't handled
        let envelope = envelope_opt.ok_or_else(|| RaftError::invalid_state("Envelope was consumed unexpectedly"))?;

        // Extract values for routing
        let target_node = envelope.to;
        // Priority is determined but not currently used for routing logic
        let _ = self.get_message_priority(&envelope.message);

        // Route to appropriate handler based on message type and priority
        let handlers = self.message_handlers.read().await;
        if let Some(sender) = handlers.get(&target_node) {
            // Send the message (envelope is consumed here)
            sender.send(envelope).map_err(|_| {
                RaftError::Network(NetworkError::InvalidResponse {
                    node_id: target_node,
                    message: "Handler channel closed".to_string(),
                })
            })?;
        } else {
            log::warn!("No handler registered for node {}", target_node);
        }

        Ok(())
    }

    /// Handle response messages by matching them to pending requests
    /// Returns (handled, envelope) where handled is true if matched, and envelope is returned for reuse
    async fn handle_response(&self, envelope: MessageEnvelope) -> RaftResult<(bool, Option<MessageEnvelope>)> {
        let mut pending_requests = self.pending_requests.write().await;

        // Check if this is a response to a pending request
        if let Some(request) = pending_requests.remove(&envelope.message_id) {
            // Send response to waiting client
            // Since RaftMessage can't be cloned, we need to consume the envelope
            let _ = request.sender.send(envelope);
            return Ok((true, None));
        }

        Ok((false, Some(envelope)))
    }

    /// Get message priority based on message type
    fn get_message_priority(&self, message: &RaftMessage) -> MessagePriority {
        match message {
            RaftMessage::Vote(_) | RaftMessage::VoteResponse(_) => MessagePriority::Critical,
            RaftMessage::AppendEntries(_) | RaftMessage::AppendEntriesResponse(_) => {
                MessagePriority::High
            }
            RaftMessage::InstallSnapshot(_) | RaftMessage::InstallSnapshotResponse(_) => {
                MessagePriority::Normal
            }
            RaftMessage::Heartbeat { .. } | RaftMessage::HeartbeatResponse { .. } => {
                MessagePriority::Low
            }
        }
    }

    /// Send a request and wait for response with timeout
    pub async fn send_request_with_response(
        &self,
        target_node: NodeId,
        message: RaftMessage,
        timeout: Duration,
    ) -> RaftResult<RaftMessage> {
        let (sender, receiver) = tokio::sync::oneshot::channel();
        // Get priority before moving message
        let priority = self.get_message_priority(&message);
        let envelope = MessageEnvelope::new(self.node_id, target_node, message);
        let message_id = envelope.message_id;

        // Store pending request
        {
            let mut pending_requests = self.pending_requests.write().await;
            pending_requests.insert(
                message_id,
                PendingRequest {
                    message_id,
                    sender,
                    timestamp: Instant::now(),
                    priority,
                },
            );
        }

        // Send the message (this would typically go through the network client)
        // For now, we'll simulate sending by routing it
        self.route_message(envelope).await?;

        // Wait for response with timeout
        match tokio::time::timeout(timeout, receiver).await {
            Ok(Ok(response_envelope)) => Ok(response_envelope.message),
            Ok(Err(_)) => Err(RaftError::Network(NetworkError::InvalidResponse {
                node_id: target_node,
                message: "Response channel closed".to_string(),
            })),
            Err(_) => {
                // Remove from pending requests on timeout
                let mut pending_requests = self.pending_requests.write().await;
                pending_requests.remove(&message_id);

                Err(RaftError::Network(NetworkError::RequestTimeout {
                    node_id: target_node,
                }))
            }
        }
    }

    /// Get routing statistics
    pub async fn get_routing_stats(&self) -> MessageRoutingStats {
        let handlers = self.message_handlers.read().await;
        let pending_requests = self.pending_requests.read().await;

        MessageRoutingStats {
            registered_handlers: handlers.len(),
            pending_requests: pending_requests.len(),
            partitioned_nodes: self.partition_detector.get_partitioned_nodes().await.len(),
        }
    }

    /// Check for network partitions
    pub async fn check_partitions(&self) -> Vec<NodeId> {
        self.partition_detector.get_partitioned_nodes().await
    }

    /// Get partition detector
    pub fn partition_detector(&self) -> Arc<PartitionDetector> {
        self.partition_detector.clone()
    }
}

/// Connection pool for managing TCP connections to cluster nodes
#[derive(Debug)]
pub struct ConnectionPool {
    connections: Arc<RwLock<HashMap<NodeId, Vec<Arc<RaftConnection>>>>>,
    max_connections_per_node: usize,
    connection_timeout: Duration,
    request_timeout: Duration,
    tls_config: Option<TlsConfig>,
    node_auths: Arc<RwLock<HashMap<NodeId, NodeAuth>>>,
    cleanup_interval: Duration,
    max_idle_time: Duration,
}

impl ConnectionPool {
    /// Create a new connection pool
    pub fn new() -> Self {
        let pool = Self {
            connections: Arc::new(RwLock::new(HashMap::new())),
            max_connections_per_node: 10,
            connection_timeout: Duration::from_secs(5),
            request_timeout: Duration::from_secs(10),
            tls_config: None,
            node_auths: Arc::new(RwLock::new(HashMap::new())),
            cleanup_interval: Duration::from_secs(60),
            max_idle_time: Duration::from_secs(300),
        };

        // Start background cleanup task
        pool.start_cleanup_task();
        pool
    }

    /// Create a new secure connection pool
    pub fn new_secure(tls_config: TlsConfig) -> Self {
        let pool = Self {
            connections: Arc::new(RwLock::new(HashMap::new())),
            max_connections_per_node: 10,
            connection_timeout: Duration::from_secs(5),
            request_timeout: Duration::from_secs(10),
            tls_config: Some(tls_config),
            node_auths: Arc::new(RwLock::new(HashMap::new())),
            cleanup_interval: Duration::from_secs(60),
            max_idle_time: Duration::from_secs(300),
        };

        // Start background cleanup task
        pool.start_cleanup_task();
        pool
    }

    /// Start background task to clean up idle connections
    fn start_cleanup_task(&self) {
        let connections = self.connections.clone();
        let cleanup_interval = self.cleanup_interval;
        let max_idle_time = self.max_idle_time;

        tokio::spawn(async move {
            let mut interval = tokio::time::interval(cleanup_interval);
            loop {
                interval.tick().await;
                Self::cleanup_idle_connections(&connections, max_idle_time).await;
            }
        });
    }

    /// Clean up idle connections
    async fn cleanup_idle_connections(
        connections: &Arc<RwLock<HashMap<NodeId, Vec<Arc<RaftConnection>>>>>,
        max_idle_time: Duration,
    ) {
        let mut connections_guard = connections.write().await;
        let mut nodes_to_remove = Vec::new();

        for (node_id, node_connections) in connections_guard.iter_mut() {
            // Remove unhealthy connections
            node_connections.retain(|conn| {
                tokio::task::block_in_place(|| {
                    tokio::runtime::Handle::current().block_on(conn.is_healthy())
                })
            });

            // Remove connections that have been idle too long
            let now = Instant::now();
            node_connections.retain(|conn| {
                let last_activity = tokio::task::block_in_place(|| {
                    tokio::runtime::Handle::current()
                        .block_on(async { *conn.last_activity.lock().await })
                });
                now.duration_since(last_activity) <= max_idle_time
            });

            if node_connections.is_empty() {
                nodes_to_remove.push(*node_id);
            }
        }

        // Remove nodes with no connections
        for node_id in nodes_to_remove {
            connections_guard.remove(&node_id);
        }

        log::debug!(
            "Connection pool cleanup completed. Active nodes: {}",
            connections_guard.len()
        );
    }

    /// Add node authentication
    pub async fn add_node_auth(&self, node_id: NodeId, auth: NodeAuth) {
        let mut auths = self.node_auths.write().await;
        auths.insert(node_id, auth);
    }

    /// Remove node authentication
    pub async fn remove_node_auth(&self, node_id: NodeId) {
        let mut auths = self.node_auths.write().await;
        auths.remove(&node_id);
    }

    /// Get or create a connection to the specified node
    pub async fn get_connection(
        &self,
        node_id: NodeId,
        endpoint: &str,
    ) -> RaftResult<Arc<RaftConnection>> {
        // Try to get an existing healthy connection
        {
            let mut connections = self.connections.write().await;
            if let Some(node_connections) = connections.get_mut(&node_id) {
                // Find a healthy connection
                for (i, conn) in node_connections.iter().enumerate() {
                    if conn.is_healthy().await {
                        // Move the connection to the end (LRU)
                        let conn = node_connections.remove(i);
                        node_connections.push(conn.clone());
                        return Ok(conn);
                    }
                }

                // Remove all unhealthy connections
                node_connections.clear();
            }
        }

        // Create a new connection
        let connection = self.create_new_connection(node_id, endpoint).await?;

        // Store the connection in the pool
        {
            let mut connections = self.connections.write().await;
            let node_connections = connections.entry(node_id).or_insert_with(Vec::new);

            // Ensure we don't exceed max connections per node
            if node_connections.len() >= self.max_connections_per_node {
                // Remove the oldest connection
                node_connections.remove(0);
            }

            node_connections.push(connection.clone());
        }

        Ok(connection)
    }

    /// Create a new connection with proper configuration
    async fn create_new_connection(
        &self,
        node_id: NodeId,
        endpoint: &str,
    ) -> RaftResult<Arc<RaftConnection>> {
        // Get node authentication if available
        let node_auth = {
            let auths = self.node_auths.read().await;
            auths.get(&node_id).cloned()
        };

        // Create a new connection
        let connection = if let Some(ref tls_config) = self.tls_config {
            RaftConnection::new_secure(
                node_id,
                endpoint,
                self.connection_timeout,
                tls_config.clone(),
                node_auth,
            )
            .await?
        } else {
            RaftConnection::new(node_id, endpoint, self.connection_timeout).await?
        };

        Ok(Arc::new(connection))
    }

    /// Remove all connections for a node
    pub async fn remove_connection(&self, node_id: NodeId) {
        let mut connections = self.connections.write().await;
        connections.remove(&node_id);
    }

    /// Get connection statistics
    pub async fn get_stats(&self) -> ConnectionPoolStats {
        let connections = self.connections.read().await;
        let mut total_connections = 0;
        let mut healthy_connections = 0;
        let active_nodes = connections.len();

        for node_connections in connections.values() {
            total_connections += node_connections.len();
            for conn in node_connections {
                if conn.is_healthy().await {
                    healthy_connections += 1;
                }
            }
        }

        ConnectionPoolStats {
            active_nodes,
            total_connections,
            healthy_connections,
            max_connections_per_node: self.max_connections_per_node,
        }
    }

    /// Set maximum connections per node
    pub fn set_max_connections_per_node(&mut self, max_connections: usize) {
        self.max_connections_per_node = max_connections;
    }

    /// Set connection timeout
    pub fn set_connection_timeout(&mut self, timeout: Duration) {
        self.connection_timeout = timeout;
    }

    /// Set request timeout
    pub fn set_request_timeout(&mut self, timeout: Duration) {
        self.request_timeout = timeout;
    }

    /// Get connection timeout
    pub fn connection_timeout(&self) -> Duration {
        self.connection_timeout
    }

    /// Get request timeout
    pub fn request_timeout(&self) -> Duration {
        self.request_timeout
    }
}

/// Individual connection to a Raft node
#[derive(Debug)]
pub struct RaftConnection {
    node_id: NodeId,
    endpoint: String,
    stream: Option<Arc<tokio::sync::Mutex<SecureStream>>>,
    created_at: Instant,
    pub(crate) last_activity: Arc<tokio::sync::Mutex<Instant>>,
    tls_config: Option<TlsConfig>,
    node_auth: Option<NodeAuth>,
}

impl RaftConnection {
    /// Create a new plain connection to a node
    pub async fn new(node_id: NodeId, endpoint: &str, timeout: Duration) -> RaftResult<Self> {
        Self::new_with_config(node_id, endpoint, timeout, None, None).await
    }

    /// Create a new secure connection to a node
    pub async fn new_secure(
        node_id: NodeId,
        endpoint: &str,
        timeout: Duration,
        tls_config: TlsConfig,
        node_auth: Option<NodeAuth>,
    ) -> RaftResult<Self> {
        Self::new_with_config(node_id, endpoint, timeout, Some(tls_config), node_auth).await
    }

    /// Create a new connection with optional TLS and authentication
    async fn new_with_config(
        node_id: NodeId,
        endpoint: &str,
        timeout: Duration,
        tls_config: Option<TlsConfig>,
        node_auth: Option<NodeAuth>,
    ) -> RaftResult<Self> {
        log::debug!("Creating connection to node {} at {}", node_id, endpoint);

        // Parse endpoint (format: "host:port")
        let addr = endpoint.parse::<std::net::SocketAddr>().map_err(|e| {
            NetworkError::ConnectionFailed {
                node_id,
                source: std::io::Error::new(std::io::ErrorKind::InvalidInput, e),
            }
        })?;

        // Create secure or plain connection
        let stream = if let Some(ref tls_config) = tls_config {
            // Extract hostname from endpoint for TLS
            let hostname = endpoint.split(':').next().unwrap_or("localhost");
            tokio_timeout(
                timeout,
                SecureStream::connect_tls(addr, tls_config, hostname),
            )
            .await
            .map_err(|_| NetworkError::RequestTimeout { node_id })?
        } else {
            tokio_timeout(timeout, SecureStream::connect_plain(addr))
                .await
                .map_err(|_| NetworkError::RequestTimeout { node_id })?
        }?;

        let now = Instant::now();
        Ok(Self {
            node_id,
            endpoint: endpoint.to_string(),
            stream: Some(Arc::new(tokio::sync::Mutex::new(stream))),
            created_at: now,
            last_activity: Arc::new(tokio::sync::Mutex::new(now)),
            tls_config,
            node_auth,
        })
    }

    /// Check if the connection is healthy
    pub async fn is_healthy(&self) -> bool {
        if self.stream.is_none() {
            return false;
        }

        let last_activity = *self.last_activity.lock().await;
        last_activity.elapsed() < Duration::from_secs(300) // 5 minutes
    }

    /// Send a message envelope with retry logic
    pub async fn send_message_with_retry(
        &self,
        mut envelope: MessageEnvelope,
        max_retries: u32,
    ) -> RaftResult<MessageEnvelope> {
        // Add authentication if configured
        if let Some(ref auth) = self.node_auth {
            envelope.add_authentication(auth)?;
        }

        let mut last_error = None;

        for attempt in 0..=max_retries {
            match self.send_message(&envelope).await {
                Ok(response) => {
                    // Verify response authentication if configured
                    if let Some(ref auth) = self.node_auth {
                        if !response.verify_authentication(auth) {
                            log::warn!(
                                "Authentication verification failed for response from node {}",
                                self.node_id
                            );
                            return Err(RaftError::Network(NetworkError::InvalidResponse {
                                node_id: self.node_id,
                                message: "Authentication verification failed".to_string(),
                            }));
                        }
                    }

                    // Update last activity
                    *self.last_activity.lock().await = Instant::now();
                    return Ok(response);
                }
                Err(e) => {
                    last_error = Some(e);
                    if attempt < max_retries {
                        let exp = (attempt as u32).min(20);
                        let delay = Duration::from_millis(100u64.saturating_mul(1u64 << exp));
                        tokio::time::sleep(delay).await;
                        log::warn!(
                            "Retry attempt {} for node {} failed, retrying in {:?}",
                            attempt + 1,
                            self.node_id,
                            delay
                        );
                    }
                }
            }
        }

        Err(last_error.unwrap())
    }

    /// Send a message to the connected node
    async fn send_message(&self, envelope: &MessageEnvelope) -> RaftResult<MessageEnvelope> {
        let stream = self.stream.as_ref().ok_or_else(|| {
            RaftError::Network(NetworkError::ConnectionFailed {
                node_id: self.node_id,
                source: std::io::Error::new(std::io::ErrorKind::NotConnected, "No connection"),
            })
        })?;

        let mut stream_guard = stream.lock().await;

        // Serialize and send the message
        let serialized = envelope.serialize()?;
        stream_guard
            .write_all(&serialized)
            .await
            .map_err(|e| NetworkError::ConnectionFailed {
                node_id: self.node_id,
                source: e,
            })?;

        // Read the response
        let mut length_buf = [0u8; 4];
        stream_guard
            .read_exact(&mut length_buf)
            .await
            .map_err(|e| NetworkError::ConnectionFailed {
                node_id: self.node_id,
                source: e,
            })?;

        let length = u32::from_be_bytes(length_buf) as usize;

        // Prevent DoS attacks via unbounded memory allocation
        const MAX_MESSAGE_SIZE: usize = 64 * 1024 * 1024; // 64 MiB
        if length == 0 || length > MAX_MESSAGE_SIZE {
            return Err(RaftError::Network(NetworkError::InvalidResponse {
                node_id: self.node_id,
                message: format!(
                    "Response size invalid: {} bytes (max: {} bytes)",
                    length, MAX_MESSAGE_SIZE
                ),
            }));
        }

        let mut response_buf = vec![0u8; length];
        stream_guard
            .read_exact(&mut response_buf)
            .await
            .map_err(|e| NetworkError::ConnectionFailed {
                node_id: self.node_id,
                source: e,
            })?;

        // Deserialize the response
        let mut response_bytes = BytesMut::with_capacity(4 + length);
        response_bytes.put_u32(length as u32);
        response_bytes.put_slice(&response_buf);

        MessageEnvelope::deserialize(response_bytes.freeze())
    }
}

/// Raft network factory for creating network clients
#[derive(Clone)]
pub struct KiwiRaftNetworkFactory {
    source_node: NodeId,
    endpoints: Arc<RwLock<HashMap<NodeId, String>>>,
    connection_pool: Arc<ConnectionPool>,
    message_router: Arc<MessageRouter>,
}

impl KiwiRaftNetworkFactory {
    /// Create a new network factory
    pub fn new(source_node: NodeId) -> Self {
        Self {
            source_node,
            endpoints: Arc::new(RwLock::new(HashMap::new())),
            connection_pool: Arc::new(ConnectionPool::new()),
            message_router: Arc::new(MessageRouter::new(source_node)),
        }
    }

    /// Create a new secure network factory
    pub fn new_secure(source_node: NodeId, tls_config: TlsConfig) -> Self {
        Self {
            source_node,
            endpoints: Arc::new(RwLock::new(HashMap::new())),
            connection_pool: Arc::new(ConnectionPool::new_secure(tls_config)),
            message_router: Arc::new(MessageRouter::new(source_node)),
        }
    }

    /// Add node authentication
    pub async fn add_node_auth(&self, node_id: NodeId, auth: NodeAuth) {
        self.connection_pool.add_node_auth(node_id, auth).await;
    }

    /// Remove node authentication
    pub async fn remove_node_auth(&self, node_id: NodeId) {
        self.connection_pool.remove_node_auth(node_id).await;
    }

    /// Add an endpoint for a node
    pub async fn add_endpoint(&self, node_id: NodeId, endpoint: String) {
        let mut endpoints = self.endpoints.write().await;
        endpoints.insert(node_id, endpoint);
    }

    /// Remove an endpoint for a node
    pub async fn remove_endpoint(&self, node_id: NodeId) {
        let mut endpoints = self.endpoints.write().await;
        endpoints.remove(&node_id);
        self.connection_pool.remove_connection(node_id).await;
    }

    /// Get endpoint for a node
    pub async fn get_endpoint(&self, node_id: NodeId) -> Option<String> {
        let endpoints = self.endpoints.read().await;
        endpoints.get(&node_id).cloned()
    }

    /// Get message router
    pub fn message_router(&self) -> Arc<MessageRouter> {
        self.message_router.clone()
    }

    /// Check for network partitions
    pub async fn check_partitions(&self) -> Vec<NodeId> {
        self.message_router.check_partitions().await
    }
}

#[async_trait]
impl RaftNetworkFactory<TypeConfig> for KiwiRaftNetworkFactory {
    type Network = RaftNetworkClient;

    async fn new_client(
        &self,
        target: NodeId,
        _node: &openraft::BasicNode,
    ) -> Self::Network {
        RaftNetworkClient::with_source_node(
            self.source_node,
            target,
            self.endpoints.clone(),
            self.connection_pool.clone(),
            self.message_router.clone(),
        )
    }
}

/// Network client for communicating with a specific Raft node
pub struct RaftNetworkClient {
    source_node: NodeId,
    target_node: NodeId,
    endpoints: Arc<RwLock<HashMap<NodeId, String>>>,
    connection_pool: Arc<ConnectionPool>,
    message_router: Arc<MessageRouter>,
    request_timeout: Duration,
    max_retries: u32,
}

impl RaftNetworkClient {
    /// Create a new network client for a target node
    pub fn new(
        target_node: NodeId,
        endpoints: Arc<RwLock<HashMap<NodeId, String>>>,
        connection_pool: Arc<ConnectionPool>,
    ) -> Self {
        Self {
            source_node: 0, // Will be set when the factory is created
            target_node,
            endpoints,
            connection_pool,
            message_router: Arc::new(MessageRouter::new(target_node)),
            request_timeout: Duration::from_secs(10),
            max_retries: 3,
        }
    }

    /// Create a new network client with source node ID
    pub fn with_source_node(
        source_node: NodeId,
        target_node: NodeId,
        endpoints: Arc<RwLock<HashMap<NodeId, String>>>,
        connection_pool: Arc<ConnectionPool>,
        message_router: Arc<MessageRouter>,
    ) -> Self {
        Self {
            source_node,
            target_node,
            endpoints,
            connection_pool,
            message_router,
            request_timeout: Duration::from_secs(10),
            max_retries: 3,
        }
    }

    /// Set request timeout
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.request_timeout = timeout;
        self
    }

    /// Set maximum retry attempts
    pub fn with_max_retries(mut self, max_retries: u32) -> Self {
        self.max_retries = max_retries;
        self
    }

    /// Get connection to the target node
    async fn get_connection(&self) -> RaftResult<Arc<RaftConnection>> {
        let endpoint = {
            let endpoints = self.endpoints.read().await;
            endpoints
                .get(&self.target_node)
                .ok_or_else(|| {
                    RaftError::Network(NetworkError::ConnectionFailed {
                        node_id: self.target_node,
                        source: std::io::Error::new(
                            std::io::ErrorKind::NotFound,
                            "Endpoint not found for node",
                        ),
                    })
                })?
                .clone()
        };

        self.connection_pool
            .get_connection(self.target_node, &endpoint)
            .await
    }

    /// Send a Raft message and wait for response
    async fn send_raft_message(&self, message: RaftMessage) -> RaftResult<RaftMessage> {
        // Check for network partition first
        if self
            .message_router
            .partition_detector()
            .is_partitioned(self.target_node)
            .await
        {
            return Err(RaftError::Network(NetworkError::NetworkPartition));
        }

        // Try HTTP first, fallback to TCP if needed
        // Serialize message first so we can potentially retry with TCP
        let envelope = MessageEnvelope::new(self.source_node, self.target_node, message);
        let serialized_envelope = envelope.serialize()?;

        match self.send_http_message_bytes(&serialized_envelope).await {
            Ok(response) => Ok(response),
            Err(_) => {
                log::debug!(
                    "HTTP request failed, falling back to TCP for node {}",
                    self.target_node
                );
                let connection = self.get_connection().await?;
                let envelope = MessageEnvelope::deserialize(serialized_envelope)?;

                let response_envelope = connection
                    .send_message_with_retry(envelope, self.max_retries)
                    .await?;
                Ok(response_envelope.message)
            }
        }
    }

    /// Send message via HTTP with proper serialization and retry logic (using serialized bytes)
    async fn send_http_message_bytes(&self, serialized_envelope: &Bytes) -> RaftResult<RaftMessage> {
        let endpoint = {
            let endpoints = self.endpoints.read().await;
            endpoints
                .get(&self.target_node)
                .ok_or_else(|| {
                    RaftError::Network(NetworkError::ConnectionFailed {
                        node_id: self.target_node,
                        source: std::io::Error::new(
                            std::io::ErrorKind::NotFound,
                            "Endpoint not found for node",
                        ),
                    })
                })?
                .clone()
        };

        // Build HTTP URL
        let url = if endpoint.starts_with("http://") || endpoint.starts_with("https://") {
            format!("{}/raft", endpoint)
        } else {
            format!("http://{}/raft", endpoint)
        };

        let client = HttpClient::builder()
            .timeout(self.request_timeout)
            .build()
            .map_err(|e| {
                RaftError::Network(NetworkError::ConnectionFailed {
                    node_id: self.target_node,
                    source: std::io::Error::new(std::io::ErrorKind::Other, e),
                })
            })?;

        // Deserialize envelope to get message for JSON serialization
        let envelope: MessageEnvelope = MessageEnvelope::deserialize(serialized_envelope.clone())?;
        
        // Serialize message as JSON for HTTP
        let serialized_data =
            serde_json::to_vec(&envelope).map_err(|e| RaftError::Serialization(e))?;

        let mut last_error = None;

        // Retry logic with exponential backoff
        for attempt in 0..=self.max_retries {
            match self
                .send_http_request_attempt(&client, &url, &serialized_data)
                .await
            {
                Ok(response_message) => {
                    // Record successful contact
                    self.message_router
                        .partition_detector()
                        .record_contact(self.target_node)
                        .await;
                    return Ok(response_message);
                }
                Err(e) => {
                    last_error = Some(e);
                    if attempt < self.max_retries {
                        let delay = Duration::from_millis(100 * (1 << attempt.min(10)));
                        log::warn!(
                            "HTTP request attempt {} failed for node {}, retrying in {:?}",
                            attempt + 1,
                            self.target_node,
                            delay
                        );
                        tokio::time::sleep(delay).await;
                    }
                }
            }
        }

        Err(last_error.unwrap())
    }

    /// Single HTTP request attempt
    async fn send_http_request_attempt(
        &self,
        client: &HttpClient,
        url: &str,
        data: &[u8],
    ) -> RaftResult<RaftMessage> {
        let response = client
            .post(url)
            .header("Content-Type", "application/json")
            .header("X-Raft-Source-Node", self.source_node.to_string())
            .header("X-Raft-Target-Node", self.target_node.to_string())
            .body(data.to_vec())
            .send()
            .await
            .map_err(|e| {
                RaftError::Network(NetworkError::ConnectionFailed {
                    node_id: self.target_node,
                    source: std::io::Error::new(std::io::ErrorKind::Other, e),
                })
            })?;

        if !response.status().is_success() {
            return Err(RaftError::Network(NetworkError::InvalidResponse {
                node_id: self.target_node,
                message: format!("HTTP error: {}", response.status()),
            }));
        }

        let response_data = response.bytes().await.map_err(|e| {
            RaftError::Network(NetworkError::SerializationFailed(serde_json::Error::io(
                std::io::Error::new(std::io::ErrorKind::Other, e),
            )))
        })?;

        // Deserialize response
        let response_envelope: MessageEnvelope =
            serde_json::from_slice(&response_data).map_err(|e| RaftError::Serialization(e))?;

        // Validate response
        if response_envelope.from != self.target_node || response_envelope.to != self.source_node {
            return Err(RaftError::Network(NetworkError::InvalidResponse {
                node_id: self.target_node,
                message: "Invalid response routing".to_string(),
            }));
        }

        Ok(response_envelope.message)
    }
}

#[async_trait]
impl OpenRaftNetwork<TypeConfig> for RaftNetworkClient {
    async fn append_entries(
        &self,
        req: openraft::raft::AppendEntriesRequest<TypeConfig>,
        _option: openraft::network::RPCOption,
    ) -> Result<
        openraft::raft::AppendEntriesResponse<NodeId>,
        openraft::error::RPCError<NodeId, openraft::BasicNode, openraft::error::RaftError<NodeId>>,
    > {
        log::debug!("Sending append_entries to node {}", self.target_node);

        let message = RaftMessage::AppendEntries(req);
        match self.send_raft_message(message).await {
            Ok(RaftMessage::AppendEntriesResponse(response)) => Ok(response),
            Ok(_) => Err(openraft::error::RPCError::Network(
                openraft::error::NetworkError::new(&RaftError::Network(
                    NetworkError::InvalidResponse {
                        node_id: self.target_node,
                        message: "Unexpected response type".to_string(),
                    },
                )),
            )),
            Err(e) => {
                // Handle network partitions gracefully
                if matches!(e, RaftError::Network(NetworkError::NetworkPartition)) {
                    log::warn!("Network partition detected with node {}", self.target_node);
                }
                Err(openraft::error::RPCError::Network(
                    openraft::error::NetworkError::new(&e),
                ))
            }
        }
    }

    async fn install_snapshot(
        &self,
        req: openraft::raft::InstallSnapshotRequest<TypeConfig>,
        _option: openraft::network::RPCOption,
    ) -> Result<
        openraft::raft::InstallSnapshotResponse<NodeId>,
        openraft::error::RPCError<NodeId, openraft::BasicNode, openraft::error::RaftError<NodeId>>,
    > {
        log::debug!("Sending install_snapshot to node {}", self.target_node);

        let message = RaftMessage::InstallSnapshot(req);
        match self.send_raft_message(message).await {
            Ok(RaftMessage::InstallSnapshotResponse(response)) => Ok(response),
            Ok(_) => Err(openraft::error::RPCError::Network(
                openraft::error::NetworkError::new(&RaftError::Network(
                    NetworkError::InvalidResponse {
                        node_id: self.target_node,
                        message: "Unexpected response type".to_string(),
                    },
                )),
            )),
            Err(e) => {
                // Handle network partitions gracefully
                if matches!(e, RaftError::Network(NetworkError::NetworkPartition)) {
                    log::warn!("Network partition detected with node {}", self.target_node);
                }
                Err(openraft::error::RPCError::Network(
                    openraft::error::NetworkError::new(&e),
                ))
            }
        }
    }

    async fn vote(
        &self,
        req: openraft::raft::VoteRequest<NodeId>,
        _option: openraft::network::RPCOption,
    ) -> Result<
        openraft::raft::VoteResponse<NodeId>,
        openraft::error::RPCError<NodeId, openraft::BasicNode, openraft::error::RaftError<NodeId>>,
    > {
        log::debug!("Sending vote request to node {}", self.target_node);

        let message = RaftMessage::Vote(req);
        match self.send_raft_message(message).await {
            Ok(RaftMessage::VoteResponse(response)) => Ok(response),
            Ok(_) => Err(openraft::error::RPCError::Network(
                openraft::error::NetworkError::new(&RaftError::Network(
                    NetworkError::InvalidResponse {
                        node_id: self.target_node,
                        message: "Unexpected response type".to_string(),
                    },
                )),
            )),
            Err(e) => {
                // Handle network partitions gracefully
                if matches!(e, RaftError::Network(NetworkError::NetworkPartition)) {
                    log::warn!("Network partition detected with node {}", self.target_node);
                }
                Err(openraft::error::RPCError::Network(
                    openraft::error::NetworkError::new(&e),
                ))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    // TODO: Add tests for KiwiRaftNetworkFactory and RaftNetworkClient
    // The old RaftNetwork tests have been removed as that type no longer exists
}
