---
name: debug-runtime
description: Dual runtime debugging guide for Kiwi. Use when diagnosing runtime issues.
---

# Debug Dual Runtime

Guide for debugging Kiwi's dual runtime architecture.

## Architecture Overview

```text
Network Runtime (I/O-optimized)     Storage Runtime (CPU-optimized)
  |                                    |
  | RESP parsing                       | RocksDB operations
  | Connection management              | Data structure logic
  |                                    |
  +------- StorageClient/Server -------+
              (async channels)
```

## Common Issues

### 1. Channel Deadlock

**Symptoms**: Request hangs, no response

**Diagnosis**:
- Check if storage runtime is blocked (full channel buffer)
- Check if network runtime is waiting for response that never comes

**Fix**:
- Ensure bounded channels have appropriate capacity
- Add timeouts to channel operations
- Check for panics in storage runtime (kills the runtime)

### 2. Circuit Breaker Open

**Symptoms**: Requests rejected immediately with error

**Diagnosis**:
- Check circuit breaker state (metrics)
- Check storage runtime health
- Look for RocksDB errors in logs

**Fix**:
- Fix underlying storage issue
- Adjust circuit breaker thresholds
- Implement proper error propagation

### 3. Backpressure Triggered

**Symptoms**: Requests queued, slow response

**Diagnosis**:
- Check in-flight request count
- Check storage operation latency
- Look for slow queries or large batch operations

**Fix**:
- Optimize slow storage operations
- Adjust backpressure thresholds
- Implement request prioritization

### 4. Graceful Shutdown Issues

**Symptoms**: Data loss on shutdown, connection errors

**Diagnosis**:
- Check shutdown order (drain first, then close)
- Check for in-flight requests during shutdown

**Fix**:
- Ensure proper shutdown sequence:
  1. Stop accepting new connections
  2. Drain in-flight requests (with timeout)
  3. Flush storage writes
  4. Close channels
  5. Join runtime threads

## Debug Tools

### Logging

```bash
RUST_LOG=debug cargo run --release
RUST_LOG=runtime=trace cargo run --release  # Just runtime logs
```

### Metrics

Check runtime metrics for:
- Channel utilization (buffer size / capacity)
- In-flight request count
- Circuit breaker state (closed/open/half-open)
- Request latency percentiles
- Error rates

### Health Checks

```bash
# Check if server is responsive
redis-cli -p 7379 PING

# Check storage health
redis-cli -p 7379 INFO
```

## Debug Checklist

When diagnosing runtime issues:

- [ ] Check `RUST_LOG` output for errors/warnings
- [ ] Check channel buffer utilization
- [ ] Check circuit breaker state
- [ ] Check for panics in either runtime
- [ ] Check RocksDB logs for storage errors
- [ ] Test with single-threaded runtime (`--runtime-threads 1`)
- [ ] Reproduce with minimal config
