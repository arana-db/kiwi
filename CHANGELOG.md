# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- **Dual Runtime Architecture**: Major architectural improvement for performance and fault isolation
  - Separate network runtime (I/O optimized) and storage runtime (CPU optimized)
  - Message channel-based async communication between runtimes
  - `StorageClient` for network runtime to send requests to storage runtime
  - `StorageServer` for processing storage operations in dedicated runtime
  - Automatic retry with exponential backoff
  - Circuit breaker for fault tolerance
  - Request queuing during storage unavailability
  - Recovery management for degraded storage states
  - Comprehensive health monitoring and statistics
  - Backpressure handling to prevent overload
- Architecture documentation in `docs/ARCHITECTURE.md`
- Runtime configuration options for thread pool sizing
- Independent health checks for network and storage runtimes

### Changed

- **BREAKING CHANGE**: Default port changed from `9221` to `7379` for Redis compatibility
  - The configuration system has been migrated to Redis-style format (similar to `redis.conf`)
  - Configuration file moved from INI format to Redis-style format at `src/conf/kiwi.conf`
  - Default port is now `7379` (a Redis-compatible variant of the standard `6379` port)
- **BREAKING CHANGE**: Storage initialization now requires `RuntimeManager::initialize_storage_components()`
  - Must be called after `RuntimeManager::start()` and before starting storage server
  - Returns the receiver that must be passed to storage server initialization
- Storage operations now go through `StorageClient` instead of direct access
- Network and storage operations run in isolated thread pools

### Migration Guide

#### For Users Upgrading from Previous Versions

If you are upgrading from a previous version that used port `9221`:

1. **Update client connections**: Change any client applications connecting to Kiwi from port `9221` to port `7379`

2. **Update firewall rules**: If you have firewall rules allowing port `9221`, update them to allow port `7379`

3. **Configuration file migration**: The configuration format has changed from INI to Redis-style:
   - Old format: `config.ini` with `port=9221`
   - New format: `kiwi.conf` with `port 7379`

4. **Custom port configuration**: If you need to use a different port, update the `port` setting in `src/conf/kiwi.conf`:
   ```conf
   # Accept connections on the specified port, default is 7379.
   # port 7379
   port YOUR_CUSTOM_PORT
   ```

5. **Docker/Container deployments**: Update any port mappings from `9221` to `7379`

**Note**: Port `7379` was chosen to indicate Redis compatibility while avoiding conflicts with the standard Redis port (`6379`).

#### For Developers

If you have custom code that integrates with Kiwi:

1. **RuntimeManager initialization**: The storage components must now be initialized explicitly:
   ```rust
   let mut runtime_manager = RuntimeManager::new(config)?;
   runtime_manager.start().await?;
   
   // NEW: Initialize storage components
   let storage_receiver = runtime_manager.initialize_storage_components()?;
   
   // Pass receiver to storage server
   let storage_handle = runtime_manager.storage_handle()?;
   storage_handle.spawn(async move {
       initialize_storage_server(storage_receiver).await
   });
   ```

2. **Storage access**: Use `StorageClient` instead of direct RocksDB access:
   ```rust
   // Get the storage client
   let storage_client = runtime_manager.storage_client()?;
   
   // Send storage requests
   let result = storage_client.send_request(
       StorageCommand::Get { key: b"mykey".to_vec() }
   ).await?;
   ```

3. **Error handling**: Handle new error types from `StorageClient`:
   - `DualRuntimeError::Timeout` - Request timeout
   - `DualRuntimeError::Channel` - Channel communication error
   - `DualRuntimeError::Storage` - Storage operation error

4. **Configuration**: New runtime configuration options available:
   - `network_threads` - Network runtime thread pool size
   - `storage_threads` - Storage runtime thread pool size
   - `channel_buffer_size` - Message channel buffer size
   - `request_timeout` - Default request timeout

See `docs/ARCHITECTURE.md` for detailed architecture documentation.

### Fixed

- **Critical**: Storage server now properly receives requests from network runtime
  - Fixed message channel lifecycle management
  - Sender side now stays alive for the lifetime of RuntimeManager
  - Network runtime can now successfully communicate with storage runtime
- Port number consistency across codebase
- Configuration default values now properly aligned

### Performance

- Network and storage operations now run in isolated thread pools
- Automatic backpressure handling prevents overload
- Request queuing during high load
- Circuit breaker prevents cascading failures
