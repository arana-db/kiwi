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

//! HTTP API for monitoring Raft cluster metrics and health
//!
//! This module provides REST endpoints for accessing cluster metrics,
//! health status, and exporting monitoring data.

use crate::error::RaftResult;
use crate::health_monitor::{ClusterHealthMonitor, HealthRecord};
use crate::metrics::{MetricsCollector, RaftMetrics};
use crate::types::NodeId;
use serde::{Deserialize, Serialize};
use std::net::SocketAddr;
use std::sync::Arc;
use warp::{Filter, Reply};

/// Monitoring API configuration
#[derive(Debug, Clone)]
pub struct MonitoringApiConfig {
    /// Address to bind the HTTP server
    pub bind_address: SocketAddr,
    /// Enable CORS for cross-origin requests
    pub enable_cors: bool,
    /// API key for authentication (optional)
    pub api_key: Option<String>,
}

impl Default for MonitoringApiConfig {
    fn default() -> Self {
        Self {
            bind_address: "127.0.0.1:8080".parse().unwrap(),
            enable_cors: true,
            api_key: None,
        }
    }
}

/// API response wrapper
#[derive(Debug, Serialize)]
pub struct ApiResponse<T> {
    pub success: bool,
    pub data: Option<T>,
    pub error: Option<String>,
    pub timestamp: u64,
}

impl<T> ApiResponse<T> {
    pub fn success(data: T) -> Self {
        Self {
            success: true,
            data: Some(data),
            error: None,
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
        }
    }

    pub fn error(message: String) -> Self {
        Self {
            success: false,
            data: None,
            error: Some(message),
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
        }
    }
}

/// Query parameters for metrics endpoint
#[derive(Debug, Deserialize)]
pub struct MetricsQuery {
    /// Format for metrics export (json, prometheus)
    pub format: Option<String>,
    /// Include detailed breakdown
    pub detailed: Option<bool>,
}

/// Query parameters for health history endpoint
#[derive(Debug, Deserialize)]
pub struct HealthHistoryQuery {
    /// Number of records to return
    pub limit: Option<usize>,
    /// Start timestamp (Unix seconds)
    pub start: Option<u64>,
    /// End timestamp (Unix seconds)
    pub end: Option<u64>,
}

/// Monitoring API server
pub struct MonitoringApiServer {
    config: MonitoringApiConfig,
    metrics_collector: Arc<MetricsCollector>,
    health_monitor: Arc<ClusterHealthMonitor>,
}

impl MonitoringApiServer {
    /// Create a new monitoring API server
    pub fn new(
        config: MonitoringApiConfig,
        metrics_collector: Arc<MetricsCollector>,
        health_monitor: Arc<ClusterHealthMonitor>,
    ) -> Self {
        Self {
            config,
            metrics_collector,
            health_monitor,
        }
    }

    /// Start the HTTP API server
    pub async fn start(&self) -> RaftResult<()> {
        let metrics_collector = Arc::clone(&self.metrics_collector);
        let health_monitor = Arc::clone(&self.health_monitor);
        let api_key = self.config.api_key.clone();

        // Authentication filter
        let auth = warp::header::optional::<String>("authorization").and_then(
            move |auth_header: Option<String>| {
                let api_key = api_key.clone();
                async move {
                    if let Some(expected_key) = api_key {
                        match auth_header {
                            Some(header) if header == format!("Bearer {}", expected_key) => Ok(()),
                            _ => Err(warp::reject::custom(AuthError)),
                        }
                    } else {
                        Ok(())
                    }
                }
            },
        );

        // CORS configuration
        let cors = if self.config.enable_cors {
            warp::cors()
                .allow_any_origin()
                .allow_headers(vec!["content-type", "authorization"])
                .allow_methods(vec!["GET", "POST", "OPTIONS"])
                .build()
        } else {
            warp::cors().build()
        };

        // Health endpoint
        let health = warp::path("health")
            .and(warp::get())
            .and(auth.clone())
            .and(with_health_monitor(health_monitor.clone()))
            .and_then(get_health_status);

        // Metrics endpoint
        let metrics = warp::path("metrics")
            .and(warp::get())
            .and(warp::query::<MetricsQuery>())
            .and(auth.clone())
            .and(with_metrics_collector(metrics_collector.clone()))
            .and_then(get_metrics);

        // Health history endpoint
        let health_history = warp::path!("health" / "history")
            .and(warp::get())
            .and(warp::query::<HealthHistoryQuery>())
            .and(auth.clone())
            .and(with_health_monitor(health_monitor.clone()))
            .and_then(get_health_history);

        // Anomalies endpoint
        let anomalies = warp::path!("health" / "anomalies")
            .and(warp::get())
            .and(auth.clone())
            .and(with_health_monitor(health_monitor.clone()))
            .and_then(get_anomalies);

        // Export endpoint
        let export = warp::path("export")
            .and(warp::get())
            .and(warp::query::<MetricsQuery>())
            .and(auth.clone())
            .and(with_metrics_collector(metrics_collector.clone()))
            .and(with_health_monitor(health_monitor.clone()))
            .and_then(export_data);

        // Status endpoint (simple health check)
        let status = warp::path("status")
            .and(warp::get())
            .map(|| warp::reply::with_status("OK", warp::http::StatusCode::OK));

        let routes = health
            .or(metrics)
            .or(health_history)
            .or(anomalies)
            .or(export)
            .or(status)
            .with(cors)
            .recover(handle_rejection);

        log::info!(
            "Starting monitoring API server on {}",
            self.config.bind_address
        );

        warp::serve(routes).run(self.config.bind_address).await;

        Ok(())
    }
}

// Helper functions for dependency injection
fn with_metrics_collector(
    collector: Arc<MetricsCollector>,
) -> impl Filter<Extract = (Arc<MetricsCollector>,), Error = std::convert::Infallible> + Clone {
    warp::any().map(move || collector.clone())
}

fn with_health_monitor(
    monitor: Arc<ClusterHealthMonitor>,
) -> impl Filter<Extract = (Arc<ClusterHealthMonitor>,), Error = std::convert::Infallible> + Clone {
    warp::any().map(move || monitor.clone())
}

// API handlers
async fn get_health_status(
    _: (),
    health_monitor: Arc<ClusterHealthMonitor>,
) -> Result<impl warp::Reply, warp::Rejection> {
    let status = health_monitor.get_current_status();
    Ok(warp::reply::json(&ApiResponse::success(status)))
}

async fn get_metrics(
    query: MetricsQuery,
    _: (),
    metrics_collector: Arc<MetricsCollector>,
) -> Result<Box<dyn warp::Reply>, warp::Rejection> {
    let metrics = metrics_collector.get_metrics();

    match query.format.as_deref() {
        Some("prometheus") => {
            let prometheus_format = format_metrics_as_prometheus(&metrics);
            let response = warp::reply::with_header(
                prometheus_format,
                "content-type",
                "text/plain; version=0.0.4",
            );
            Ok(Box::new(response) as Box<dyn warp::Reply>)
        }
        _ => {
            if query.detailed.unwrap_or(false) {
                let response = warp::reply::json(&ApiResponse::success(metrics));
                Ok(Box::new(response) as Box<dyn warp::Reply>)
            } else {
                let summary = create_metrics_summary(&metrics);
                let response = warp::reply::json(&ApiResponse::success(summary));
                Ok(Box::new(response) as Box<dyn warp::Reply>)
            }
        }
    }
}

async fn get_health_history(
    query: HealthHistoryQuery,
    _: (),
    health_monitor: Arc<ClusterHealthMonitor>,
) -> Result<impl warp::Reply, warp::Rejection> {
    let mut history = health_monitor.get_health_history(query.limit);

    // Filter by time range if specified
    if let (Some(start), Some(end)) = (query.start, query.end) {
        history.retain(|record| record.timestamp >= start && record.timestamp <= end);
    } else if let Some(start) = query.start {
        history.retain(|record| record.timestamp >= start);
    } else if let Some(end) = query.end {
        history.retain(|record| record.timestamp <= end);
    }

    Ok(warp::reply::json(&ApiResponse::success(history)))
}

async fn get_anomalies(
    _: (),
    health_monitor: Arc<ClusterHealthMonitor>,
) -> Result<impl warp::Reply, warp::Rejection> {
    let anomalies = health_monitor.get_active_anomalies();
    Ok(warp::reply::json(&ApiResponse::success(anomalies)))
}

async fn export_data(
    query: MetricsQuery,
    _: (),
    metrics_collector: Arc<MetricsCollector>,
    health_monitor: Arc<ClusterHealthMonitor>,
) -> Result<Box<dyn warp::Reply>, warp::Rejection> {
    let metrics = metrics_collector.get_metrics();
    let health_status = health_monitor.get_current_status();
    let health_history = health_monitor.get_health_history(Some(100));

    let export_data = ExportData {
        metrics,
        health_status,
        health_history,
        export_timestamp: std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs(),
    };

    match query.format.as_deref() {
        Some("csv") => {
            let csv_data = format_export_as_csv(&export_data);
            let response = warp::reply::with_header(csv_data, "content-type", "text/csv");
            Ok(Box::new(response) as Box<dyn warp::Reply>)
        }
        _ => {
            let response = warp::reply::json(&ApiResponse::success(export_data));
            Ok(Box::new(response) as Box<dyn warp::Reply>)
        }
    }
}

// Data structures for export
#[derive(Debug, Serialize)]
struct ExportData {
    metrics: RaftMetrics,
    health_status: crate::health_monitor::ClusterHealthStatus,
    health_history: Vec<HealthRecord>,
    export_timestamp: u64,
}

#[derive(Debug, Serialize)]
struct MetricsSummary {
    node_id: NodeId,
    current_term: u64,
    node_state: String,
    is_leader: bool,
    cluster_size: usize,
    avg_latency_ms: f64,
    requests_per_second: f64,
    error_rate_percent: f64,
    memory_usage_mb: f64,
    active_connections: u32,
    last_updated: u64,
}

fn create_metrics_summary(metrics: &RaftMetrics) -> MetricsSummary {
    MetricsSummary {
        node_id: metrics.state.node_id,
        current_term: metrics.state.current_term,
        node_state: metrics.state.node_state.clone(),
        is_leader: metrics.state.node_state.contains("Leader"),
        cluster_size: metrics.state.cluster_size,
        avg_latency_ms: metrics.performance.avg_request_latency_ms,
        requests_per_second: metrics.performance.requests_per_second,
        error_rate_percent: metrics.errors.error_rate_percent,
        memory_usage_mb: metrics.performance.memory_usage_bytes as f64 / 1024.0 / 1024.0,
        active_connections: metrics.network.active_connections,
        last_updated: metrics.timestamp,
    }
}

fn format_metrics_as_prometheus(metrics: &RaftMetrics) -> String {
    let mut output = String::new();

    // Basic Raft metrics
    output.push_str(&format!("# HELP raft_current_term Current Raft term\n"));
    output.push_str(&format!("# TYPE raft_current_term gauge\n"));
    output.push_str(&format!(
        "raft_current_term{{node_id=\"{}\"}} {}\n",
        metrics.state.node_id, metrics.state.current_term
    ));

    output.push_str(&format!("# HELP raft_last_log_index Last log index\n"));
    output.push_str(&format!("# TYPE raft_last_log_index gauge\n"));
    output.push_str(&format!(
        "raft_last_log_index{{node_id=\"{}\"}} {}\n",
        metrics.state.node_id, metrics.state.last_log_index
    ));

    // Performance metrics
    output.push_str(&format!(
        "# HELP raft_request_latency_ms Average request latency in milliseconds\n"
    ));
    output.push_str(&format!("# TYPE raft_request_latency_ms gauge\n"));
    output.push_str(&format!(
        "raft_request_latency_ms{{node_id=\"{}\",quantile=\"avg\"}} {}\n",
        metrics.state.node_id, metrics.performance.avg_request_latency_ms
    ));
    output.push_str(&format!(
        "raft_request_latency_ms{{node_id=\"{}\",quantile=\"0.95\"}} {}\n",
        metrics.state.node_id, metrics.performance.p95_request_latency_ms
    ));
    output.push_str(&format!(
        "raft_request_latency_ms{{node_id=\"{}\",quantile=\"0.99\"}} {}\n",
        metrics.state.node_id, metrics.performance.p99_request_latency_ms
    ));

    output.push_str(&format!(
        "# HELP raft_requests_per_second Requests per second\n"
    ));
    output.push_str(&format!("# TYPE raft_requests_per_second gauge\n"));
    output.push_str(&format!(
        "raft_requests_per_second{{node_id=\"{}\"}} {}\n",
        metrics.state.node_id, metrics.performance.requests_per_second
    ));

    // Error metrics
    output.push_str(&format!(
        "# HELP raft_error_rate_percent Error rate percentage\n"
    ));
    output.push_str(&format!("# TYPE raft_error_rate_percent gauge\n"));
    output.push_str(&format!(
        "raft_error_rate_percent{{node_id=\"{}\"}} {}\n",
        metrics.state.node_id, metrics.errors.error_rate_percent
    ));

    // Network metrics
    output.push_str(&format!(
        "# HELP raft_network_bytes_sent Total bytes sent\n"
    ));
    output.push_str(&format!("# TYPE raft_network_bytes_sent counter\n"));
    output.push_str(&format!(
        "raft_network_bytes_sent{{node_id=\"{}\"}} {}\n",
        metrics.state.node_id, metrics.network.bytes_sent
    ));

    output.push_str(&format!(
        "# HELP raft_network_bytes_received Total bytes received\n"
    ));
    output.push_str(&format!("# TYPE raft_network_bytes_received counter\n"));
    output.push_str(&format!(
        "raft_network_bytes_received{{node_id=\"{}\"}} {}\n",
        metrics.state.node_id, metrics.network.bytes_received
    ));

    output
}

fn format_export_as_csv(export_data: &ExportData) -> String {
    let mut csv = String::new();

    // CSV header
    csv.push_str("timestamp,node_id,term,state,latency_ms,rps,error_rate,memory_mb\n");

    // Current metrics
    csv.push_str(&format!(
        "{},{},{},{},{:.2},{:.2},{:.2},{:.2}\n",
        export_data.export_timestamp,
        export_data.metrics.state.node_id,
        export_data.metrics.state.current_term,
        export_data.metrics.state.node_state,
        export_data.metrics.performance.avg_request_latency_ms,
        export_data.metrics.performance.requests_per_second,
        export_data.metrics.errors.error_rate_percent,
        export_data.metrics.performance.memory_usage_bytes as f64 / 1024.0 / 1024.0
    ));

    csv
}

// Error handling
#[derive(Debug)]
struct AuthError;

impl warp::reject::Reject for AuthError {}

async fn handle_rejection(
    err: warp::Rejection,
) -> Result<impl warp::Reply, std::convert::Infallible> {
    let code;
    let message;

    if err.is_not_found() {
        code = warp::http::StatusCode::NOT_FOUND;
        message = "Not Found";
    } else if let Some(_) = err.find::<AuthError>() {
        code = warp::http::StatusCode::UNAUTHORIZED;
        message = "Unauthorized";
    } else if let Some(_) = err.find::<warp::reject::MethodNotAllowed>() {
        code = warp::http::StatusCode::METHOD_NOT_ALLOWED;
        message = "Method Not Allowed";
    } else {
        log::error!("Unhandled rejection: {:?}", err);
        code = warp::http::StatusCode::INTERNAL_SERVER_ERROR;
        message = "Internal Server Error";
    }

    let json = warp::reply::json(&ApiResponse::<()>::error(message.to_string()));
    Ok(warp::reply::with_status(json, code))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::health_monitor::{ClusterHealthMonitor, HealthMonitorConfig};
    use crate::metrics::MetricsCollector;
    use std::time::Duration;
    use tokio::time::timeout;

    #[tokio::test]
    async fn test_api_response_creation() {
        let success_response = ApiResponse::success("test data");
        assert!(success_response.success);
        assert_eq!(success_response.data, Some("test data"));
        assert!(success_response.error.is_none());

        let error_response = ApiResponse::<()>::error("test error".to_string());
        assert!(!error_response.success);
        assert!(error_response.data.is_none());
        assert_eq!(error_response.error, Some("test error".to_string()));
    }

    #[test]
    fn test_metrics_summary_creation() {
        let metrics_collector = MetricsCollector::new(1);
        let metrics = metrics_collector.get_metrics();
        let summary = create_metrics_summary(&metrics);

        assert_eq!(summary.node_id, 1);
        assert_eq!(summary.current_term, 0);
        assert!(!summary.is_leader); // Should be false for "Follower" state
    }

    #[test]
    fn test_prometheus_format() {
        let metrics_collector = MetricsCollector::new(1);
        let metrics = metrics_collector.get_metrics();
        let prometheus_output = format_metrics_as_prometheus(&metrics);

        assert!(prometheus_output.contains("raft_current_term"));
        assert!(prometheus_output.contains("raft_request_latency_ms"));
        assert!(prometheus_output.contains("node_id=\"1\""));
    }

    #[test]
    fn test_csv_export_format() {
        let metrics_collector = MetricsCollector::new(1);
        let health_monitor =
            ClusterHealthMonitor::new(HealthMonitorConfig::default(), Arc::new(metrics_collector));

        let export_data = ExportData {
            metrics: MetricsCollector::new(1).get_metrics(),
            health_status: health_monitor.get_current_status(),
            health_history: Vec::new(),
            export_timestamp: 1234567890,
        };

        let csv_output = format_export_as_csv(&export_data);
        assert!(csv_output.contains("timestamp,node_id,term,state"));
        assert!(csv_output.contains("1234567890,1,0,Follower"));
    }

    #[tokio::test]
    async fn test_monitoring_api_server_creation() {
        let config = MonitoringApiConfig::default();
        let metrics_collector = Arc::new(MetricsCollector::new(1));
        let health_monitor = Arc::new(ClusterHealthMonitor::new(
            HealthMonitorConfig::default(),
            metrics_collector.clone(),
        ));

        let _server = MonitoringApiServer::new(config, metrics_collector, health_monitor);
        // Just test that creation doesn't panic
    }

    #[test]
    fn test_query_parameters() {
        let query = MetricsQuery {
            format: Some("prometheus".to_string()),
            detailed: Some(true),
        };

        assert_eq!(query.format.as_deref(), Some("prometheus"));
        assert_eq!(query.detailed, Some(true));

        let history_query = HealthHistoryQuery {
            limit: Some(50),
            start: Some(1234567890),
            end: Some(1234567900),
        };

        assert_eq!(history_query.limit, Some(50));
        assert_eq!(history_query.start, Some(1234567890));
        assert_eq!(history_query.end, Some(1234567900));
    }
}
