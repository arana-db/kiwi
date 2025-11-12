# Design Document

## Overview

This document describes the design for enhancing the Kiwi architecture with dynamic resource management, request prioritization, fine-grained Raft monitoring, fault injection capabilities, and multi-storage backend support.

## Architecture Components

### 1. Dynamic Thread Pool Manager

**Location**: src/common/runtime/thread_pool_manager.rs

**Purpose**: Automatically adjust thread pool sizes based on workload metrics.

**Key Components**:
- ThreadPoolScaler: Monitors queue lengths and adjusts pool sizes
- ScalingPolicy: Defines thresholds and scaling parameters
- ScalingMetrics: Tracks scaling events and effectiveness

**Design**:
`ust
pub struct ThreadPoolScaler {
    config: ScalingConfig,
    metrics: Arc<RwLock<ScalingMetrics>>,
    runtime_handle: tokio::runtime::Handle,
}

pub struct ScalingConfig {
    pub min_threads: usize,
    pub max_threads: usize,
    pub scale_up_threshold: usize,
    pub scale_down_threshold: usize,
    pub scale_increment: usize,
    pub evaluation_interval: Duration,
}
`

### 2. Priority Queue System

**Location**: src/common/runtime/priority_queue.rs

**Purpose**: Process requests based on priority levels.

**Key Components**:
- PriorityMessageChannel: Replaces MessageChannel with priority support
- PriorityQueue<T>: Internal priority-based queue implementation
- Priority levels: Critical, High, Normal, Low

**Design**:
`ust
pub struct PriorityMessageChannel {
    queues: [mpsc::Sender<StorageRequest>; 4], // One per priority
    stats: Arc<Mutex<PriorityChannelStats>>,
}
`

### 3. Raft Metrics Collector

**Location**: src/raft/src/metrics/collector.rs

**Purpose**: Collect fine-grained Raft operation metrics.

**Key Metrics**:
- Log replication latency per follower
- Leader election count and duration
- Commit rate and apply rate
- Current Raft state per node

**Design**:
`ust
pub struct RaftMetricsCollector {
    replication_latencies: HashMap<NodeId, MovingAverage>,
    election_events: Vec<ElectionEvent>,
    commit_rate: RateTracker,
    apply_rate: RateTracker,
}
`

### 4. Fault Injection Framework

**Location**: src/raft/src/testing/fault_injection.rs

**Purpose**: Simulate failures for resilience testing.

**Capabilities**:
- Network delay injection
- Network partition simulation
- Node crash simulation
- Message loss simulation

**Design**:
`ust
pub struct FaultInjector {
    active_faults: Arc<RwLock<Vec<ActiveFault>>>,
    event_log: Arc<Mutex<Vec<FaultEvent>>>,
}

pub enum FaultType {
    NetworkDelay { duration: Duration },
    NetworkPartition { isolated_nodes: Vec<NodeId> },
    NodeCrash { node_id: NodeId },
    MessageLoss { drop_rate: f64 },
}
`

### 5. Storage Backend Abstraction

**Location**: src/raft/src/storage/backend.rs

**Purpose**: Abstract storage operations for multiple backends.

**Design**:
`ust
#[async_trait]
pub trait StorageBackend: Send + Sync {
    async fn read(&self, key: &[u8]) -> Result<Option<Vec<u8>>>;
    async fn write(&self, key: &[u8], value: &[u8]) -> Result<()>;
    async fn delete(&self, key: &[u8]) -> Result<()>;
    async fn batch_write(&self, ops: Vec<WriteOp>) -> Result<()>;
}

pub struct RocksDBBackend { /* ... */ }
pub struct LevelDBBackend { /* ... */ }
`

## Integration Points

### RuntimeManager Integration
- Add ThreadPoolScaler to RuntimeManager
- Replace MessageChannel with PriorityMessageChannel
- Add configuration for scaling and priority

### Raft Integration
- Integrate RaftMetricsCollector into Raft node
- Add FaultInjector hooks in network layer
- Replace direct RocksDB usage with StorageBackend trait

### Configuration
- Extend RuntimeConfig with scaling parameters
- Add PriorityConfig for queue configuration
- Add RaftMetricsConfig for metrics collection
- Add FaultInjectionConfig for testing

## Data Flow

### Priority Request Flow
1. Request arrives with priority level
2. PriorityMessageChannel routes to appropriate queue
3. StorageServer processes high-priority requests first
4. Metrics track processing time by priority

### Dynamic Scaling Flow
1. ThreadPoolScaler monitors queue lengths
2. When threshold exceeded, increase thread pool size
3. When queue drains, decrease thread pool size
4. Metrics track scaling events and effectiveness

### Metrics Collection Flow
1. Raft operations emit metric events
2. RaftMetricsCollector aggregates metrics
3. Metrics exposed via Prometheus endpoint
4. Dashboard displays real-time metrics

## Testing Strategy

### Unit Tests
- Test each component in isolation
- Mock dependencies
- Verify correct behavior under various conditions

### Integration Tests
- Test component interactions
- Verify end-to-end flows
- Test failure scenarios

### Performance Tests
- Benchmark dynamic scaling effectiveness
- Measure priority queue overhead
- Verify metrics collection impact

### Fault Injection Tests
- Test cluster resilience under failures
- Verify recovery mechanisms
- Validate monitoring during failures

## Monitoring and Observability

### Metrics Endpoints
- /metrics/runtime - Thread pool and queue metrics
- /metrics/raft - Raft operation metrics
- /metrics/storage - Storage backend metrics

### Logging
- Structured logging for all components
- Log levels: TRACE, DEBUG, INFO, WARN, ERROR
- Correlation IDs for request tracking

### Dashboards
- Runtime health dashboard
- Raft cluster dashboard
- Storage performance dashboard

## Configuration Example

`	oml
[runtime.scaling]
enabled = true
min_network_threads = 2
max_network_threads = 16
min_storage_threads = 4
max_storage_threads = 32
scale_up_threshold = 80  # percent
scale_down_threshold = 20  # percent
evaluation_interval_ms = 1000

[runtime.priority]
enabled = true
high_priority_weight = 3
normal_priority_weight = 2
low_priority_weight = 1

[raft.metrics]
enabled = true
collection_interval_ms = 100
retention_period_sec = 3600

[raft.fault_injection]
enabled = false  # Only for testing
`

## Migration Plan

### Phase 1: Foundation
1. Implement storage backend abstraction
2. Add basic metrics collection
3. Update configuration system

### Phase 2: Core Features
1. Implement dynamic thread pool scaling
2. Add priority queue system
3. Integrate metrics collector

### Phase 3: Advanced Features
1. Add fault injection framework
2. Implement priority-based circuit breaker
3. Add comprehensive monitoring

### Phase 4: Testing and Optimization
1. Performance testing and tuning
2. Fault injection testing
3. Documentation and examples

## Security Considerations

- Fault injection only enabled in test environments
- Metrics endpoints require authentication
- Configuration validation to prevent misuse
- Rate limiting on metrics collection

## Performance Considerations

- Minimal overhead for priority queue (<5%)
- Metrics collection uses sampling for high-frequency events
- Thread pool scaling has hysteresis to prevent thrashing
- Storage abstraction uses zero-copy where possible

## Future Enhancements

- Machine learning-based scaling predictions
- Adaptive priority adjustment
- Distributed tracing integration
- Advanced anomaly detection
