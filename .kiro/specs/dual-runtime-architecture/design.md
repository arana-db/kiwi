# Design Document: Dual Runtime Architecture

## Overview

This document outlines the design for implementing a dual-runtime architecture that separates network I/O and storage I/O operations into independent tokio runtimes. The current implementation uses a single tokio runtime (`#[tokio::main]`) where both network operations and RocksDB storage operations compete for the same thread pool resources, leading to potential blocking and performance issues.

The new architecture will create two dedicated runtimes:
- **Network Runtime**: Handles TCP connections, RESP protocol parsing, and client management
- **Storage Runtime**: Handles RocksDB operations, background tasks, and storage processing

## Architecture

### High-Level Architecture Diagram

```
┌─────────────────────────────────────────────────────────────────┐
│                        Main Process                             │
├─────────────────────────┬───────────────────────────────────────┤
│     Network Runtime     │            Storage Runtime            │
│                         │                                       │
│ ┌─────────────────────┐ │ ┌─────────────────────────────────────┐ │
│ │   TCP Listener      │ │ │        Storage Worker Pool          │ │
│ │   - Accept conns    │ │ │        - RocksDB operations         │ │
│ │   - RESP parsing    │ │ │        - Background tasks           │ │
│ │   - Client mgmt     │ │ │        - Compaction handling        │ │
│ └─────────────────────┘ │ └─────────────────────────────────────┘ │
│           │             │                   │                     │
│           │             │                   │                     │
│ ┌─────────▼───────────┐ │ ┌─────────────────▼───────────────────┐ │
│ │  Request Processor  │ │ │       Request Handler              │ │
│ │  - Validate cmds    │ │ │       - Execute storage ops        │ │
│ │  - Route requests   │ │ │       - Batch processing           │ │
│ │  - Handle responses │ │ │       - Error handling              │ │
│ └─────────────────────┘ │ └─────────────────────────────────────┘ │
└─────────────┬───────────┴───────────────┬─────────────────────────┘
              │                           │
              │    ┌─────────────────────┐│
              └────┤   Message Channel   ├┘
                   │   - Async mpsc      │
                   │   - Request/Response│
                   │   - Backpressure    │
                   └─────────────────────┘
```

### Component Architecture

#### 1. Runtime Manager
A new component responsible for creating and managing both runtimes:

```rust
pub struct RuntimeManager {
    network_runtime: tokio::runtime::Runtime,
    storage_runtime: tokio::runtime::Runtime,
    message_channel: MessageChannel,
}
```

#### 2. Message Channel System
Handles communication between the two runtimes:

```rust
pub struct MessageChannel {
    request_sender: mpsc::Sender<StorageRequest>,
    request_receiver: mpsc::Receiver<StorageRequest>,
    response_sender: mpsc::Sender<StorageResponse>,
    response_receiver: mpsc::Receiver<StorageResponse>,
}

pub struct StorageRequest {
    id: RequestId,
    command: StorageCommand,
    response_channel: oneshot::Sender<StorageResponse>,
    timeout: Duration,
}

pub struct StorageResponse {
    id: RequestId,
    result: Result<RespData, StorageError>,
    execution_time: Duration,
}
```

#### 3. Network Layer Components

**NetworkServer**: Replaces the current TcpServer with runtime-aware implementation:
```rust
pub struct NetworkServer {
    addr: String,
    storage_client: StorageClient,
    cmd_table: Arc<CmdTable>,
    connection_pool: Arc<ConnectionPool<NetworkResources>>,
}
```

**StorageClient**: Network-side interface for communicating with storage runtime:
```rust
pub struct StorageClient {
    request_sender: mpsc::Sender<StorageRequest>,
    pending_requests: Arc<Mutex<HashMap<RequestId, oneshot::Receiver<StorageResponse>>>>,
}
```

#### 4. Storage Layer Components

**StorageServer**: Dedicated storage processing server:
```rust
pub struct StorageServer {
    storage: Arc<Storage>,
    request_receiver: mpsc::Receiver<StorageRequest>,
    batch_processor: BatchProcessor,
    background_task_manager: BackgroundTaskManager,
}
```

**BatchProcessor**: Optimizes storage operations through batching:
```rust
pub struct BatchProcessor {
    batch_size: usize,
    batch_timeout: Duration,
    pending_requests: Vec<StorageRequest>,
}
```

## Components and Interfaces

### 1. Runtime Configuration

```rust
pub struct RuntimeConfig {
    pub network_threads: usize,
    pub storage_threads: usize,
    pub channel_buffer_size: usize,
    pub request_timeout: Duration,
    pub batch_size: usize,
    pub batch_timeout: Duration,
}

impl Default for RuntimeConfig {
    fn default() -> Self {
        Self {
            network_threads: num_cpus::get().min(4),
            storage_threads: num_cpus::get().min(8),
            channel_buffer_size: 10000,
            request_timeout: Duration::from_secs(30),
            batch_size: 100,
            batch_timeout: Duration::from_millis(10),
        }
    }
}
```

### 2. Storage Command Interface

```rust
#[derive(Debug, Clone)]
pub enum StorageCommand {
    Get { key: Vec<u8> },
    Set { key: Vec<u8>, value: Vec<u8>, ttl: Option<Duration> },
    Del { keys: Vec<Vec<u8>> },
    Exists { keys: Vec<Vec<u8>> },
    Expire { key: Vec<u8>, ttl: Duration },
    // ... other Redis commands
    Batch { commands: Vec<StorageCommand> },
}
```

### 3. Error Handling Interface

```rust
#[derive(Debug, thiserror::Error)]
pub enum DualRuntimeError {
    #[error("Network runtime error: {0}")]
    NetworkRuntime(String),
    
    #[error("Storage runtime error: {0}")]
    StorageRuntime(String),
    
    #[error("Channel communication error: {0}")]
    Channel(String),
    
    #[error("Request timeout after {timeout:?}")]
    Timeout { timeout: Duration },
    
    #[error("Storage operation failed: {0}")]
    Storage(#[from] storage::StorageError),
}
```

## Data Models

### 1. Request/Response Models

```rust
#[derive(Debug, Clone)]
pub struct RequestId(uuid::Uuid);

impl RequestId {
    pub fn new() -> Self {
        Self(uuid::Uuid::new_v4())
    }
}

#[derive(Debug)]
pub struct StorageRequest {
    pub id: RequestId,
    pub command: StorageCommand,
    pub client_id: ClientId,
    pub timestamp: Instant,
    pub priority: RequestPriority,
}

#[derive(Debug)]
pub struct StorageResponse {
    pub id: RequestId,
    pub result: Result<RespData, StorageError>,
    pub execution_time: Duration,
    pub storage_stats: StorageStats,
}
```

### 2. Monitoring Models

```rust
#[derive(Debug, Clone)]
pub struct RuntimeMetrics {
    pub network_metrics: NetworkMetrics,
    pub storage_metrics: StorageMetrics,
    pub channel_metrics: ChannelMetrics,
}

#[derive(Debug, Clone)]
pub struct NetworkMetrics {
    pub active_connections: usize,
    pub requests_per_second: f64,
    pub average_response_time: Duration,
    pub error_rate: f64,
}

#[derive(Debug, Clone)]
pub struct StorageMetrics {
    pub pending_requests: usize,
    pub average_execution_time: Duration,
    pub rocksdb_stats: RocksDbStats,
    pub batch_efficiency: f64,
}
```

## Error Handling

### 1. Fault Isolation Strategy

- **Network Runtime Errors**: Isolated to individual connections, don't affect storage operations
- **Storage Runtime Errors**: Returned as error responses, don't crash network connections
- **Channel Errors**: Implement retry logic with exponential backoff
- **Timeout Handling**: Configurable timeouts with graceful degradation

### 2. Error Recovery Mechanisms

```rust
pub struct ErrorRecoveryManager {
    retry_policy: RetryPolicy,
    circuit_breaker: CircuitBreaker,
    fallback_handler: FallbackHandler,
}

pub struct RetryPolicy {
    max_retries: usize,
    base_delay: Duration,
    max_delay: Duration,
    backoff_multiplier: f64,
}
```

### 3. Graceful Degradation

- **Storage Unavailable**: Queue requests with backpressure
- **High Latency**: Implement request prioritization
- **Memory Pressure**: Apply flow control mechanisms

## Testing Strategy

### 1. Unit Testing

- **Runtime Manager**: Test runtime creation and lifecycle
- **Message Channel**: Test message passing and error scenarios
- **Storage Client**: Test request/response handling
- **Batch Processor**: Test batching logic and efficiency

### 2. Integration Testing

- **End-to-End Flow**: Test complete request processing pipeline
- **Error Scenarios**: Test various failure modes and recovery
- **Performance**: Benchmark throughput and latency improvements
- **Concurrency**: Test high-load scenarios with multiple clients

### 3. Performance Testing

```rust
#[cfg(test)]
mod performance_tests {
    use super::*;
    
    #[tokio::test]
    async fn test_dual_runtime_throughput() {
        // Test that dual runtime achieves >20% throughput improvement
    }
    
    #[tokio::test]
    async fn test_network_isolation_during_storage_blocking() {
        // Test that network remains responsive during RocksDB compaction
    }
    
    #[tokio::test]
    async fn test_concurrent_load_handling() {
        // Test system behavior under high concurrent load
    }
}
```

### 4. Stress Testing

- **RocksDB Blocking**: Simulate compaction and write stalls
- **High Connection Count**: Test with thousands of concurrent connections
- **Memory Pressure**: Test behavior under memory constraints
- **Network Partitions**: Test resilience to network issues

## Implementation Phases

### Phase 1: Core Infrastructure
1. Create RuntimeManager with dual runtime setup
2. Implement basic MessageChannel communication
3. Create StorageClient and StorageServer interfaces
4. Update main.rs to use RuntimeManager instead of #[tokio::main]

### Phase 2: Network Layer Refactoring
1. Refactor TcpServer to use NetworkServer
2. Implement StorageClient for network-to-storage communication
3. Update connection handling to use storage client
4. Add basic error handling and timeouts

### Phase 3: Storage Layer Optimization
1. Implement StorageServer with dedicated runtime
2. Add BatchProcessor for request batching
3. Implement background task management
4. Add storage-specific error handling

### Phase 4: Performance and Monitoring
1. Add comprehensive metrics collection
2. Implement request prioritization
3. Add performance monitoring and alerting
4. Optimize batch processing and channel buffer sizes

### Phase 5: Testing and Validation
1. Comprehensive unit and integration tests
2. Performance benchmarking
3. Stress testing under various load conditions
4. Documentation and deployment guides

## Migration Strategy

### 1. Backward Compatibility
- Maintain existing API interfaces
- Gradual migration with feature flags
- Fallback to single-runtime mode if needed

### 2. Configuration Migration
- Extend existing configuration with runtime settings
- Provide sensible defaults for new parameters
- Allow runtime configuration tuning

### 3. Deployment Strategy
- Blue-green deployment support
- Gradual rollout with monitoring
- Rollback procedures for issues

## Performance Expectations

### 1. Throughput Improvements
- **Target**: 20%+ improvement in overall throughput
- **Network Operations**: Consistent low-latency responses
- **Storage Operations**: Better resource utilization through batching

### 2. Latency Improvements
- **Network Latency**: <10ms response time even during storage blocking
- **Storage Latency**: Reduced through batching and dedicated resources
- **Tail Latency**: Improved P99 latency through isolation

### 3. Resource Utilization
- **CPU**: Better distribution across cores
- **Memory**: More predictable memory usage patterns
- **I/O**: Reduced contention between network and storage I/O

This design provides a robust foundation for implementing the dual-runtime architecture while maintaining system reliability and performance.