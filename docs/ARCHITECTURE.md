# Kiwi Architecture

## Overview

Kiwi uses a dual runtime architecture to achieve high performance and fault isolation. This document describes the key components and their interactions.

## Dual Runtime Architecture

### Design Philosophy

The dual runtime architecture separates I/O-bound operations (network) from CPU-bound operations (storage) into independent Tokio runtimes. This provides:

1. **Performance Isolation**: Network latency doesn't block storage operations
2. **Fault Isolation**: Failures in one runtime don't crash the other
3. **Resource Optimization**: Independent thread pool sizing based on workload characteristics
4. **Scalability**: Each runtime can scale independently

### Components

```
┌─────────────────────────────────────────────────────────────┐
│                      RuntimeManager                          │
│  ┌────────────────────┐         ┌────────────────────┐     │
│  │  Network Runtime   │         │  Storage Runtime   │     │
│  │  (I/O optimized)   │         │  (CPU optimized)   │     │
│  │                    │         │                    │     │
│  │  - TCP Server      │         │  - StorageServer   │     │
│  │  - Protocol Parser │         │  - RocksDB Ops     │     │
│  │  - StorageClient   │────────▶│  - Request Queue   │     │
│  │                    │ Channel │                    │     │
│  └────────────────────┘         └────────────────────┘     │
└─────────────────────────────────────────────────────────────┘
```

## Core Components

### 1. RuntimeManager

**Location**: `src/common/runtime/manager.rs`

The RuntimeManager is responsible for:
- Creating and managing both network and storage runtimes
- Initializing the message channel for inter-runtime communication
- Providing access to the StorageClient
- Health monitoring and lifecycle management

**Key Methods**:
```rust
// Create a new RuntimeManager
RuntimeManager::new(config: RuntimeConfig) -> Result<Self>

// Start both runtimes
async fn start(&mut self) -> Result<()>

// Initialize storage components (must be called after start)
fn initialize_storage_components(&mut self) -> Result<Receiver<StorageRequest>>

// Get the storage client for network runtime
fn storage_client(&self) -> Result<Arc<StorageClient>>

// Stop both runtimes gracefully
async fn stop(&mut self) -> Result<()>
```

### 2. MessageChannel

**Location**: `src/common/runtime/message.rs`

The MessageChannel provides async communication between runtimes:
- **Sender**: Used by StorageClient in network runtime
- **Receiver**: Used by StorageServer in storage runtime
- **Backpressure**: Automatic flow control when channel is full
- **Statistics**: Monitoring of channel health and performance

**Key Features**:
- Bounded channel with configurable buffer size
- Backpressure handling with configurable thresholds
- Request/response pattern with oneshot channels
- Comprehensive statistics tracking

### 3. StorageClient

**Location**: `src/common/runtime/message.rs`

The StorageClient is used by the network runtime to send requests to the storage runtime:

**Features**:
- Automatic retry with exponential backoff
- Circuit breaker for fault tolerance
- Request queuing during storage unavailability
- Recovery management for degraded storage
- Timeout handling
- Priority-based request processing

**Usage Example**:
```rust
let storage_client = runtime_manager.storage_client()?;

// Send a GET request
let result = storage_client.send_request(
    StorageCommand::Get { key: b"mykey".to_vec() }
).await?;

// Send with custom timeout
let result = storage_client.send_request_with_timeout(
    StorageCommand::Set { 
        key: b"mykey".to_vec(),
        value: b"myvalue".to_vec(),
        ttl: None 
    },
    Duration::from_secs(5)
).await?;
```

### 4. StorageServer

**Location**: `src/common/runtime/storage_server.rs`

The StorageServer runs in the storage runtime and processes requests:

**Responsibilities**:
- Receive requests from the message channel
- Execute storage operations on RocksDB
- Send responses back to the network runtime
- Track operation statistics
- Handle graceful shutdown

**Request Processing Flow**:
1. Receive `StorageRequest` from channel
2. Execute the storage command on RocksDB
3. Collect operation statistics
4. Send `StorageResponse` back via oneshot channel

### 5. Storage Commands

**Location**: `src/common/runtime/message.rs`

Supported storage commands:
- `Get { key }` - Retrieve a value
- `Set { key, value, ttl }` - Store a key-value pair
- `Del { keys }` - Delete one or more keys
- `Exists { keys }` - Check if keys exist
- `Expire { key, ttl }` - Set expiration time
- `Ttl { key }` - Get time to live
- `Incr { key }` - Increment a numeric value
- `IncrBy { key, increment }` - Increment by amount
- `Decr { key }` - Decrement a numeric value
- `DecrBy { key, decrement }` - Decrement by amount
- `MSet { pairs }` - Multiple set operations
- `MGet { keys }` - Multiple get operations
- `Batch { commands }` - Batch multiple commands

## Request Flow

### Normal Operation

```
Client Request
     │
     ▼
┌─────────────────┐
│ Network Runtime │
│                 │
│ 1. Parse Redis  │
│    Protocol     │
│                 │
│ 2. Create       │
│    StorageCmd   │
│                 │
│ 3. Send via     │
│    StorageClient│
└────────┬────────┘
         │ MessageChannel
         ▼
┌─────────────────┐
│ Storage Runtime │
│                 │
│ 4. Receive      │
│    Request      │
│                 │
│ 5. Execute on   │
│    RocksDB      │
│                 │
│ 6. Send         │
│    Response     │
└────────┬────────┘
         │ Oneshot Channel
         ▼
┌─────────────────┐
│ Network Runtime │
│                 │
│ 7. Format Redis │
│    Response     │
│                 │
│ 8. Send to      │
│    Client       │
└─────────────────┘
```

### Error Handling

The system includes multiple layers of error handling:

1. **Retry Logic**: Automatic retry with exponential backoff
2. **Circuit Breaker**: Fast-fail when storage is consistently failing
3. **Request Queuing**: Queue requests during temporary storage unavailability
4. **Recovery Management**: Detect and recover from degraded states
5. **Timeout Handling**: Prevent indefinite blocking

## Configuration

### RuntimeConfig

**Location**: `src/common/runtime/config.rs`

```rust
pub struct RuntimeConfig {
    pub network_threads: usize,      // Network runtime thread pool size
    pub storage_threads: usize,      // Storage runtime thread pool size
    pub channel_buffer_size: usize,  // Message channel buffer size
    pub request_timeout: Duration,   // Default request timeout
    pub max_pending_requests: usize, // Max pending requests
    pub backpressure_threshold: Duration, // Backpressure threshold
}
```

**Default Behavior**:
- Network threads: 25% of CPU cores (min 2, max 8)
- Storage threads: 50% of CPU cores (min 2, max 16)
- Channel buffer: 10,000 requests
- Request timeout: 30 seconds

## Monitoring and Observability

### Runtime Statistics

Both runtimes provide statistics:
- Active tasks
- Total tasks spawned
- Uptime
- Health status
- Last health check timestamp

### Channel Statistics

The message channel tracks:
- Requests sent/received
- Responses sent
- Timeouts
- Send failures
- Pending requests
- Backpressure events
- Average processing time

### Health Checks

The RuntimeManager performs periodic health checks:
- Network runtime responsiveness
- Storage runtime responsiveness
- Channel health
- Circuit breaker state
- Recovery state

## Lifecycle Management

### Startup Sequence

1. Create `RuntimeManager` with configuration
2. Call `start()` to create both runtimes
3. Call `initialize_storage_components()` to:
   - Extract the message channel receiver
   - Create the StorageClient
4. Start the StorageServer with the receiver
5. Start the network server with access to StorageClient

### Shutdown Sequence

1. Stop accepting new client connections
2. Drain pending requests
3. Stop the StorageServer
4. Stop the network server
5. Call `RuntimeManager::stop()` to:
   - Stop health monitoring
   - Shutdown storage runtime (with 30s timeout)
   - Shutdown network runtime (with 10s timeout)

## Best Practices

### For Developers

1. **Always use StorageClient**: Never access RocksDB directly from network runtime
2. **Handle timeouts**: Set appropriate timeouts for storage operations
3. **Check health**: Monitor runtime health and channel statistics
4. **Graceful shutdown**: Always call `stop()` to ensure clean shutdown
5. **Error handling**: Handle all error cases from StorageClient

### For Operations

1. **Monitor metrics**: Track channel statistics and runtime health
2. **Tune thread pools**: Adjust based on workload characteristics
3. **Watch backpressure**: High backpressure indicates storage bottleneck
4. **Circuit breaker**: Monitor circuit breaker state for storage issues
5. **Resource limits**: Set appropriate memory and thread limits

## Future Enhancements

- [ ] Dynamic thread pool sizing based on load
- [ ] Request prioritization and QoS
- [ ] Distributed tracing integration
- [ ] Advanced metrics and dashboards
- [ ] Hot configuration reload
- [ ] Multi-storage runtime support for sharding
