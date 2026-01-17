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

//! Storage backend abstraction for Raft
//!
//! This module provides a trait-based abstraction layer that decouples Raft storage
//! operations from specific storage engine implementations. This allows for:
//! - Multiple storage backend support (RocksDB, LevelDB, etc.)
//! - Easy testing with mock implementations
//! - Performance optimization per backend
//! - Runtime backend selection via configuration

use async_trait::async_trait;
use std::ops::RangeBounds;
use std::path::Path;

use crate::error::RaftError;

/// Storage backend type enumeration
///
/// Specifies which storage engine implementation to use.
/// This allows runtime selection of storage backends via configuration.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BackendType {
    /// RocksDB storage backend
    RocksDB,
    /// LevelDB storage backend (not yet implemented)
    LevelDB,
}

impl std::fmt::Display for BackendType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BackendType::RocksDB => write!(f, "RocksDB"),
            BackendType::LevelDB => write!(f, "LevelDB"),
        }
    }
}

impl std::str::FromStr for BackendType {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "rocksdb" => Ok(BackendType::RocksDB),
            "leveldb" => Ok(BackendType::LevelDB),
            _ => Err(format!("Unknown backend type: {}", s)),
        }
    }
}

/// Write operation for batch writes
#[derive(Debug, Clone)]
pub enum WriteOp {
    /// Put a key-value pair
    Put { key: Vec<u8>, value: Vec<u8> },
    /// Delete a key
    Delete { key: Vec<u8> },
}

/// Storage backend abstraction trait
///
/// This trait defines the interface that all storage backends must implement.
/// It provides basic key-value operations with support for:
/// - Single key reads and writes
/// - Batch operations for atomicity
/// - Range queries for log entry retrieval
/// - Async operations to avoid blocking the runtime
///
/// # Thread Safety
///
/// Implementations must be thread-safe (`Send + Sync`) as they will be shared
/// across multiple async tasks.
///
/// # Error Handling
///
/// All operations return `Result<T, RaftError>` to provide consistent error
/// handling across different backend implementations.
#[async_trait]
pub trait StorageBackend: Send + Sync {
    /// Read a value by key
    ///
    /// # Arguments
    ///
    /// * `key` - The key to read
    ///
    /// # Returns
    ///
    /// * `Ok(Some(value))` - If the key exists
    /// * `Ok(None)` - If the key does not exist
    /// * `Err(RaftError)` - If an error occurred during the read operation
    async fn read(&self, key: &[u8]) -> Result<Option<Vec<u8>>, RaftError>;

    /// Write a key-value pair
    ///
    /// # Arguments
    ///
    /// * `key` - The key to write
    /// * `value` - The value to write
    ///
    /// # Returns
    ///
    /// * `Ok(())` - If the write was successful
    /// * `Err(RaftError)` - If an error occurred during the write operation
    async fn write(&self, key: &[u8], value: &[u8]) -> Result<(), RaftError>;

    /// Delete a key
    ///
    /// # Arguments
    ///
    /// * `key` - The key to delete
    ///
    /// # Returns
    ///
    /// * `Ok(())` - If the delete was successful (even if key didn't exist)
    /// * `Err(RaftError)` - If an error occurred during the delete operation
    async fn delete(&self, key: &[u8]) -> Result<(), RaftError>;

    /// Execute a batch of write operations atomically
    ///
    /// All operations in the batch are applied atomically - either all succeed
    /// or all fail. This is critical for maintaining Raft log consistency.
    ///
    /// # Arguments
    ///
    /// * `ops` - Vector of write operations to execute
    ///
    /// # Returns
    ///
    /// * `Ok(())` - If all operations were successful
    /// * `Err(RaftError)` - If any operation failed (no operations are applied)
    async fn batch_write(&self, ops: Vec<WriteOp>) -> Result<(), RaftError>;

    /// Read a range of keys
    ///
    /// Returns all key-value pairs where the key falls within the specified range.
    /// This is used for reading log entries in a specific index range.
    ///
    /// # Arguments
    ///
    /// * `range` - The range of keys to read (supports standard Rust range syntax)
    ///
    /// # Returns
    ///
    /// * `Ok(Vec<(key, value)>)` - Vector of key-value pairs in the range
    /// * `Err(RaftError)` - If an error occurred during the range query
    async fn read_range<R>(&self, range: R) -> Result<Vec<(Vec<u8>, Vec<u8>)>, RaftError>
    where
        R: RangeBounds<Vec<u8>> + Send + 'static;

    /// Get the last key-value pair in the storage
    ///
    /// This is used to efficiently retrieve the last log entry without
    /// scanning all entries.
    ///
    /// # Returns
    ///
    /// * `Ok(Some((key, value)))` - If storage is not empty
    /// * `Ok(None)` - If storage is empty
    /// * `Err(RaftError)` - If an error occurred
    async fn get_last(&self) -> Result<Option<(Vec<u8>, Vec<u8>)>, RaftError>;

    /// Flush any pending writes to disk
    ///
    /// Ensures all previous writes are persisted to durable storage.
    /// This is important for crash recovery.
    ///
    /// # Returns
    ///
    /// * `Ok(())` - If flush was successful
    /// * `Err(RaftError)` - If an error occurred during flush
    async fn flush(&self) -> Result<(), RaftError>;
}

/// RocksDB storage backend implementation
///
/// This implementation wraps RocksDB to provide the StorageBackend trait interface.
/// It uses tokio::task::spawn_blocking to avoid blocking the async runtime during
/// synchronous RocksDB operations.
pub struct RocksDBBackend {
    db: std::sync::Arc<rocksdb::DB>,
}

impl RocksDBBackend {
    /// Create a new RocksDB backend
    ///
    /// # Arguments
    ///
    /// * `path` - Path to the RocksDB database directory
    ///
    /// # Returns
    ///
    /// * `Ok(RocksDBBackend)` - Successfully opened database
    /// * `Err(RaftError)` - Failed to open database
    pub fn new<P: AsRef<std::path::Path>>(path: P) -> Result<Self, RaftError> {
        let mut opts = rocksdb::Options::default();
        opts.create_if_missing(true);
        opts.create_missing_column_families(true);

        let db = rocksdb::DB::open(&opts, path).map_err(|e| {
            RaftError::Storage(crate::error::StorageError::rocksdb_with_context(
                e,
                "Failed to open RocksDB",
            ))
        })?;

        Ok(Self {
            db: std::sync::Arc::new(db),
        })
    }

    /// Create a new RocksDB backend with custom options
    ///
    /// # Arguments
    ///
    /// * `path` - Path to the RocksDB database directory
    /// * `opts` - RocksDB options
    ///
    /// # Returns
    ///
    /// * `Ok(RocksDBBackend)` - Successfully opened database
    /// * `Err(RaftError)` - Failed to open database
    pub fn with_options<P: AsRef<std::path::Path>>(
        path: P,
        opts: rocksdb::Options,
    ) -> Result<Self, RaftError> {
        let db = rocksdb::DB::open(&opts, path).map_err(|e| {
            RaftError::Storage(crate::error::StorageError::rocksdb_with_context(
                e,
                "Failed to open RocksDB with custom options",
            ))
        })?;

        Ok(Self {
            db: std::sync::Arc::new(db),
        })
    }
}

#[async_trait]
impl StorageBackend for RocksDBBackend {
    async fn read(&self, key: &[u8]) -> Result<Option<Vec<u8>>, RaftError> {
        let db = self.db.clone();
        let key = key.to_vec();

        tokio::task::spawn_blocking(move || {
            db.get(&key).map_err(|e| {
                RaftError::Storage(crate::error::StorageError::rocksdb_with_context(
                    e,
                    "RocksDB read error",
                ))
            })
        })
        .await
        .map_err(|e| {
            RaftError::Storage(crate::error::StorageError::DataInconsistency {
                message: format!("Task join error: {}", e),
                context: "read".to_string(),
            })
        })?
    }

    async fn write(&self, key: &[u8], value: &[u8]) -> Result<(), RaftError> {
        let db = self.db.clone();
        let key = key.to_vec();
        let value = value.to_vec();

        tokio::task::spawn_blocking(move || {
            db.put(&key, &value).map_err(|e| {
                RaftError::Storage(crate::error::StorageError::rocksdb_with_context(
                    e,
                    "RocksDB write error",
                ))
            })
        })
        .await
        .map_err(|e| {
            RaftError::Storage(crate::error::StorageError::DataInconsistency {
                message: format!("Task join error: {}", e),
                context: "write".to_string(),
            })
        })?
    }

    async fn delete(&self, key: &[u8]) -> Result<(), RaftError> {
        let db = self.db.clone();
        let key = key.to_vec();

        tokio::task::spawn_blocking(move || {
            db.delete(&key).map_err(|e| {
                RaftError::Storage(crate::error::StorageError::rocksdb_with_context(
                    e,
                    "RocksDB delete error",
                ))
            })
        })
        .await
        .map_err(|e| {
            RaftError::Storage(crate::error::StorageError::DataInconsistency {
                message: format!("Task join error: {}", e),
                context: "delete".to_string(),
            })
        })?
    }

    async fn batch_write(&self, ops: Vec<WriteOp>) -> Result<(), RaftError> {
        let db = self.db.clone();

        tokio::task::spawn_blocking(move || {
            let mut batch = rocksdb::WriteBatch::default();

            for op in ops {
                match op {
                    WriteOp::Put { key, value } => {
                        batch.put(&key, &value);
                    }
                    WriteOp::Delete { key } => {
                        batch.delete(&key);
                    }
                }
            }

            db.write(&batch).map_err(|e| {
                RaftError::Storage(crate::error::StorageError::rocksdb_with_context(
                    e,
                    "RocksDB batch write error",
                ))
            })
        })
        .await
        .map_err(|e| {
            RaftError::Storage(crate::error::StorageError::DataInconsistency {
                message: format!("Task join error: {}", e),
                context: "batch_write".to_string(),
            })
        })?
    }

    async fn read_range<R>(&self, range: R) -> Result<Vec<(Vec<u8>, Vec<u8>)>, RaftError>
    where
        R: RangeBounds<Vec<u8>> + Send + 'static,
    {
        let db = self.db.clone();

        // Determine the start key for the iterator
        let start_key = match range.start_bound() {
            std::ops::Bound::Included(k) => Some(k.clone()),
            std::ops::Bound::Excluded(k) => {
                // For excluded bound, we need to find the next key
                let mut next_key = k.clone();
                next_key.push(0);
                Some(next_key)
            }
            std::ops::Bound::Unbounded => None,
        };

        tokio::task::spawn_blocking(move || {
            let mut results = Vec::new();

            let iter = if let Some(start) = start_key {
                db.iterator(rocksdb::IteratorMode::From(
                    &start,
                    rocksdb::Direction::Forward,
                ))
            } else {
                db.iterator(rocksdb::IteratorMode::Start)
            };

            for item in iter {
                let (key, value) = item.map_err(|e| {
                    RaftError::Storage(crate::error::StorageError::rocksdb_with_context(
                        e,
                        "RocksDB iterator error",
                    ))
                })?;

                let key_vec = key.to_vec();

                // Check if key is within range
                let in_range = match range.end_bound() {
                    std::ops::Bound::Included(end) => key_vec <= *end,
                    std::ops::Bound::Excluded(end) => key_vec < *end,
                    std::ops::Bound::Unbounded => true,
                };

                if !in_range {
                    break;
                }

                results.push((key_vec, value.to_vec()));
            }

            Ok(results)
        })
        .await
        .map_err(|e| {
            RaftError::Storage(crate::error::StorageError::DataInconsistency {
                message: format!("Task join error: {}", e),
                context: "read_range".to_string(),
            })
        })?
    }

    async fn get_last(&self) -> Result<Option<(Vec<u8>, Vec<u8>)>, RaftError> {
        let db = self.db.clone();

        tokio::task::spawn_blocking(move || {
            let mut iter = db.iterator(rocksdb::IteratorMode::End);

            if let Some(result) = iter.next() {
                let (key, value) = result.map_err(|e| {
                    RaftError::Storage(crate::error::StorageError::rocksdb_with_context(
                        e,
                        "RocksDB iterator error",
                    ))
                })?;
                Ok(Some((key.to_vec(), value.to_vec())))
            } else {
                Ok(None)
            }
        })
        .await
        .map_err(|e| {
            RaftError::Storage(crate::error::StorageError::DataInconsistency {
                message: format!("Task join error: {}", e),
                context: "get_last".to_string(),
            })
        })?
    }

    async fn flush(&self) -> Result<(), RaftError> {
        let db = self.db.clone();

        tokio::task::spawn_blocking(move || {
            db.flush().map_err(|e| {
                RaftError::Storage(crate::error::StorageError::rocksdb_with_context(
                    e,
                    "RocksDB flush error",
                ))
            })
        })
        .await
        .map_err(|e| {
            RaftError::Storage(crate::error::StorageError::DataInconsistency {
                message: format!("Task join error: {}", e),
                context: "flush".to_string(),
            })
        })?
    }
}

/// Storage backend enum that wraps different backend implementations
///
/// This enum provides a unified interface for working with different storage backends
/// while maintaining type safety and avoiding the limitations of trait objects.
/// It implements the `StorageBackend` trait by delegating to the wrapped implementation.
#[derive(Clone)]
pub enum StorageBackendImpl {
    /// RocksDB storage backend
    RocksDB(std::sync::Arc<RocksDBBackend>),
    // LevelDB will be added here when implemented
    // LevelDB(std::sync::Arc<LevelDBBackend>),
}

#[async_trait]
impl StorageBackend for StorageBackendImpl {
    async fn read(&self, key: &[u8]) -> Result<Option<Vec<u8>>, RaftError> {
        match self {
            StorageBackendImpl::RocksDB(backend) => backend.read(key).await,
        }
    }

    async fn write(&self, key: &[u8], value: &[u8]) -> Result<(), RaftError> {
        match self {
            StorageBackendImpl::RocksDB(backend) => backend.write(key, value).await,
        }
    }

    async fn delete(&self, key: &[u8]) -> Result<(), RaftError> {
        match self {
            StorageBackendImpl::RocksDB(backend) => backend.delete(key).await,
        }
    }

    async fn batch_write(&self, ops: Vec<WriteOp>) -> Result<(), RaftError> {
        match self {
            StorageBackendImpl::RocksDB(backend) => backend.batch_write(ops).await,
        }
    }

    async fn read_range<R>(&self, range: R) -> Result<Vec<(Vec<u8>, Vec<u8>)>, RaftError>
    where
        R: RangeBounds<Vec<u8>> + Send + 'static,
    {
        match self {
            StorageBackendImpl::RocksDB(backend) => backend.read_range(range).await,
        }
    }

    async fn get_last(&self) -> Result<Option<(Vec<u8>, Vec<u8>)>, RaftError> {
        match self {
            StorageBackendImpl::RocksDB(backend) => backend.get_last().await,
        }
    }

    async fn flush(&self) -> Result<(), RaftError> {
        match self {
            StorageBackendImpl::RocksDB(backend) => backend.flush().await,
        }
    }
}

/// Factory function to create a storage backend based on type
///
/// This function provides a unified interface for creating storage backends
/// without requiring the caller to know the specific implementation details.
///
/// # Arguments
///
/// * `backend_type` - The type of backend to create
/// * `path` - Path to the storage directory
///
/// # Returns
///
/// * `Ok(StorageBackendImpl)` - Successfully created backend
/// * `Err(RaftError)` - Failed to create backend
///
/// # Examples
///
/// ```no_run
/// use raft::storage::backend::{create_backend, BackendType, StorageBackend};
///
/// # async fn example() -> Result<(), raft::error::RaftError> {
/// let backend = create_backend(BackendType::RocksDB, "/tmp/raft_storage")?;
/// backend.write(b"key", b"value").await?;
/// # Ok(())
/// # }
/// ```
pub fn create_backend<P: AsRef<Path>>(
    backend_type: BackendType,
    path: P,
) -> Result<StorageBackendImpl, RaftError> {
    match backend_type {
        BackendType::RocksDB => {
            let backend = RocksDBBackend::new(path)?;
            Ok(StorageBackendImpl::RocksDB(std::sync::Arc::new(backend)))
        }
        BackendType::LevelDB => {
            // LevelDB implementation not yet available
            Err(RaftError::Storage(
                crate::error::StorageError::DataInconsistency {
                    message: "LevelDB backend is not yet implemented".to_string(),
                    context: "create_backend".to_string(),
                },
            ))
        }
    }
}

/// Factory function to create a storage backend with custom RocksDB options
///
/// This function allows creating a RocksDB backend with custom configuration options.
/// For other backend types, this function will return an error.
///
/// # Arguments
///
/// * `backend_type` - The type of backend to create (must be RocksDB)
/// * `path` - Path to the storage directory
/// * `opts` - RocksDB options
///
/// # Returns
///
/// * `Ok(StorageBackendImpl)` - Successfully created backend
/// * `Err(RaftError)` - Failed to create backend or unsupported backend type
///
/// # Examples
///
/// ```no_run
/// use raft::storage::backend::{create_backend_with_options, BackendType};
/// use rocksdb::Options;
///
/// # async fn example() -> Result<(), raft::error::RaftError> {
/// let mut opts = Options::default();
/// opts.create_if_missing(true);
/// opts.set_max_open_files(1000);
///
/// let backend = create_backend_with_options(
///     BackendType::RocksDB,
///     "/tmp/raft_storage",
///     opts
/// )?;
/// # Ok(())
/// # }
/// ```
pub fn create_backend_with_options<P: AsRef<Path>>(
    backend_type: BackendType,
    path: P,
    opts: rocksdb::Options,
) -> Result<StorageBackendImpl, RaftError> {
    match backend_type {
        BackendType::RocksDB => {
            let backend = RocksDBBackend::with_options(path, opts)?;
            Ok(StorageBackendImpl::RocksDB(std::sync::Arc::new(backend)))
        }
        BackendType::LevelDB => Err(RaftError::Storage(
            crate::error::StorageError::DataInconsistency {
                message: "LevelDB backend does not support custom options yet".to_string(),
                context: "create_backend_with_options".to_string(),
            },
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Mock storage backend for testing
    struct MockBackend {
        data: std::sync::Arc<tokio::sync::RwLock<std::collections::HashMap<Vec<u8>, Vec<u8>>>>,
    }

    impl MockBackend {
        fn new() -> Self {
            Self {
                data: std::sync::Arc::new(tokio::sync::RwLock::new(
                    std::collections::HashMap::new(),
                )),
            }
        }
    }

    #[async_trait]
    impl StorageBackend for MockBackend {
        async fn read(&self, key: &[u8]) -> Result<Option<Vec<u8>>, RaftError> {
            let data = self.data.read().await;
            Ok(data.get(key).cloned())
        }

        async fn write(&self, key: &[u8], value: &[u8]) -> Result<(), RaftError> {
            let mut data = self.data.write().await;
            data.insert(key.to_vec(), value.to_vec());
            Ok(())
        }

        async fn delete(&self, key: &[u8]) -> Result<(), RaftError> {
            let mut data = self.data.write().await;
            data.remove(key);
            Ok(())
        }

        async fn batch_write(&self, ops: Vec<WriteOp>) -> Result<(), RaftError> {
            let mut data = self.data.write().await;
            for op in ops {
                match op {
                    WriteOp::Put { key, value } => {
                        data.insert(key, value);
                    }
                    WriteOp::Delete { key } => {
                        data.remove(&key);
                    }
                }
            }
            Ok(())
        }

        async fn read_range<R>(&self, range: R) -> Result<Vec<(Vec<u8>, Vec<u8>)>, RaftError>
        where
            R: RangeBounds<Vec<u8>> + Send + 'static,
        {
            let data = self.data.read().await;
            let mut results = Vec::new();

            for (key, value) in data.iter() {
                if range.contains(key) {
                    results.push((key.clone(), value.clone()));
                }
            }

            results.sort_by(|a, b| a.0.cmp(&b.0));
            Ok(results)
        }

        async fn get_last(&self) -> Result<Option<(Vec<u8>, Vec<u8>)>, RaftError> {
            let data = self.data.read().await;
            Ok(data
                .iter()
                .max_by(|a, b| a.0.cmp(b.0))
                .map(|(k, v)| (k.clone(), v.clone())))
        }

        async fn flush(&self) -> Result<(), RaftError> {
            // No-op for in-memory storage
            Ok(())
        }
    }

    #[tokio::test]
    async fn test_storage_backend_read_write() {
        let backend = MockBackend::new();

        // Write a value
        backend.write(b"key1", b"value1").await.unwrap();

        // Read it back
        let value = backend.read(b"key1").await.unwrap();
        assert_eq!(value, Some(b"value1".to_vec()));

        // Read non-existent key
        let value = backend.read(b"key2").await.unwrap();
        assert_eq!(value, None);
    }

    #[tokio::test]
    async fn test_storage_backend_delete() {
        let backend = MockBackend::new();

        // Write and delete
        backend.write(b"key1", b"value1").await.unwrap();
        backend.delete(b"key1").await.unwrap();

        // Verify deleted
        let value = backend.read(b"key1").await.unwrap();
        assert_eq!(value, None);
    }

    #[tokio::test]
    async fn test_storage_backend_batch_write() {
        let backend = MockBackend::new();

        // Batch write
        let ops = vec![
            WriteOp::Put {
                key: b"key1".to_vec(),
                value: b"value1".to_vec(),
            },
            WriteOp::Put {
                key: b"key2".to_vec(),
                value: b"value2".to_vec(),
            },
            WriteOp::Delete {
                key: b"key1".to_vec(),
            },
        ];

        backend.batch_write(ops).await.unwrap();

        // Verify results
        assert_eq!(backend.read(b"key1").await.unwrap(), None);
        assert_eq!(
            backend.read(b"key2").await.unwrap(),
            Some(b"value2".to_vec())
        );
    }

    #[tokio::test]
    async fn test_storage_backend_get_last() {
        let backend = MockBackend::new();

        // Empty storage
        assert_eq!(backend.get_last().await.unwrap(), None);

        // Add some entries
        backend.write(b"key1", b"value1").await.unwrap();
        backend.write(b"key3", b"value3").await.unwrap();
        backend.write(b"key2", b"value2").await.unwrap();

        // Get last (should be key3 lexicographically)
        let last = backend.get_last().await.unwrap();
        assert_eq!(last, Some((b"key3".to_vec(), b"value3".to_vec())));
    }

    #[tokio::test]
    async fn test_storage_backend_flush() {
        let backend = MockBackend::new();

        backend.write(b"key1", b"value1").await.unwrap();
        backend.flush().await.unwrap();

        // Verify data still accessible after flush
        let value = backend.read(b"key1").await.unwrap();
        assert_eq!(value, Some(b"value1".to_vec()));
    }

    // RocksDBBackend tests
    #[tokio::test]
    async fn test_rocksdb_backend_read_write() {
        let temp_dir = tempfile::TempDir::new().unwrap();
        let backend = RocksDBBackend::new(temp_dir.path()).unwrap();

        // Write a value
        backend.write(b"key1", b"value1").await.unwrap();

        // Read it back
        let value = backend.read(b"key1").await.unwrap();
        assert_eq!(value, Some(b"value1".to_vec()));

        // Read non-existent key
        let value = backend.read(b"key2").await.unwrap();
        assert_eq!(value, None);
    }

    #[tokio::test]
    async fn test_rocksdb_backend_delete() {
        let temp_dir = tempfile::TempDir::new().unwrap();
        let backend = RocksDBBackend::new(temp_dir.path()).unwrap();

        // Write and delete
        backend.write(b"key1", b"value1").await.unwrap();
        backend.delete(b"key1").await.unwrap();

        // Verify deleted
        let value = backend.read(b"key1").await.unwrap();
        assert_eq!(value, None);
    }

    #[tokio::test]
    async fn test_rocksdb_backend_batch_write() {
        let temp_dir = tempfile::TempDir::new().unwrap();
        let backend = RocksDBBackend::new(temp_dir.path()).unwrap();

        // Batch write
        let ops = vec![
            WriteOp::Put {
                key: b"key1".to_vec(),
                value: b"value1".to_vec(),
            },
            WriteOp::Put {
                key: b"key2".to_vec(),
                value: b"value2".to_vec(),
            },
            WriteOp::Delete {
                key: b"key1".to_vec(),
            },
        ];

        backend.batch_write(ops).await.unwrap();

        // Verify results
        assert_eq!(backend.read(b"key1").await.unwrap(), None);
        assert_eq!(
            backend.read(b"key2").await.unwrap(),
            Some(b"value2".to_vec())
        );
    }

    #[tokio::test]
    async fn test_rocksdb_backend_read_range() {
        let temp_dir = tempfile::TempDir::new().unwrap();
        let backend = RocksDBBackend::new(temp_dir.path()).unwrap();

        // Write multiple keys
        backend.write(b"key1", b"value1").await.unwrap();
        backend.write(b"key2", b"value2").await.unwrap();
        backend.write(b"key3", b"value3").await.unwrap();
        backend.write(b"key4", b"value4").await.unwrap();

        // Read range
        let results = backend
            .read_range(b"key2".to_vec()..b"key4".to_vec())
            .await
            .unwrap();
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].0, b"key2");
        assert_eq!(results[1].0, b"key3");
    }

    #[tokio::test]
    async fn test_rocksdb_backend_get_last() {
        let temp_dir = tempfile::TempDir::new().unwrap();
        let backend = RocksDBBackend::new(temp_dir.path()).unwrap();

        // Empty storage
        assert_eq!(backend.get_last().await.unwrap(), None);

        // Add some entries
        backend.write(b"key1", b"value1").await.unwrap();
        backend.write(b"key3", b"value3").await.unwrap();
        backend.write(b"key2", b"value2").await.unwrap();

        // Get last (should be key3 lexicographically)
        let last = backend.get_last().await.unwrap();
        assert_eq!(last, Some((b"key3".to_vec(), b"value3".to_vec())));
    }

    #[tokio::test]
    async fn test_rocksdb_backend_flush() {
        let temp_dir = tempfile::TempDir::new().unwrap();
        let backend = RocksDBBackend::new(temp_dir.path()).unwrap();

        backend.write(b"key1", b"value1").await.unwrap();
        backend.flush().await.unwrap();

        // Verify data still accessible after flush
        let value = backend.read(b"key1").await.unwrap();
        assert_eq!(value, Some(b"value1".to_vec()));
    }

    #[tokio::test]
    async fn test_rocksdb_backend_persistence() {
        let temp_dir = tempfile::TempDir::new().unwrap();
        let path = temp_dir.path().to_path_buf();

        // Write data and drop backend
        {
            let backend = RocksDBBackend::new(&path).unwrap();
            backend.write(b"key1", b"value1").await.unwrap();
            backend.flush().await.unwrap();
        }

        // Reopen and verify data persisted
        {
            let backend = RocksDBBackend::new(&path).unwrap();
            let value = backend.read(b"key1").await.unwrap();
            assert_eq!(value, Some(b"value1".to_vec()));
        }
    }

    #[tokio::test]
    async fn test_rocksdb_backend_concurrent_operations() {
        let temp_dir = tempfile::TempDir::new().unwrap();
        let backend = std::sync::Arc::new(RocksDBBackend::new(temp_dir.path()).unwrap());

        // Spawn multiple concurrent write tasks
        let mut handles = vec![];
        for i in 0..10 {
            let backend = backend.clone();
            let handle = tokio::spawn(async move {
                let key = format!("key{}", i);
                let value = format!("value{}", i);
                backend
                    .write(key.as_bytes(), value.as_bytes())
                    .await
                    .unwrap();
            });
            handles.push(handle);
        }

        // Wait for all writes to complete
        for handle in handles {
            handle.await.unwrap();
        }

        // Verify all writes succeeded
        for i in 0..10 {
            let key = format!("key{}", i);
            let expected_value = format!("value{}", i);
            let value = backend.read(key.as_bytes()).await.unwrap();
            assert_eq!(value, Some(expected_value.as_bytes().to_vec()));
        }
    }

    // Factory pattern tests
    #[tokio::test]
    async fn test_create_backend_rocksdb() {
        let temp_dir = tempfile::TempDir::new().unwrap();
        let backend = super::create_backend(super::BackendType::RocksDB, temp_dir.path()).unwrap();

        // Test basic operations
        backend.write(b"key1", b"value1").await.unwrap();
        let value = backend.read(b"key1").await.unwrap();
        assert_eq!(value, Some(b"value1".to_vec()));
    }

    #[tokio::test]
    async fn test_create_backend_leveldb_not_implemented() {
        let temp_dir = tempfile::TempDir::new().unwrap();
        let result = super::create_backend(super::BackendType::LevelDB, temp_dir.path());

        // Should return error since LevelDB is not yet implemented
        assert!(result.is_err());
        if let Err(e) = result {
            assert!(e.to_string().contains("not yet implemented"));
        }
    }

    #[tokio::test]
    async fn test_create_backend_with_options() {
        let temp_dir = tempfile::TempDir::new().unwrap();

        let mut opts = rocksdb::Options::default();
        opts.create_if_missing(true);
        opts.set_max_open_files(100);

        let backend =
            super::create_backend_with_options(super::BackendType::RocksDB, temp_dir.path(), opts)
                .unwrap();

        // Test basic operations
        backend.write(b"key1", b"value1").await.unwrap();
        let value = backend.read(b"key1").await.unwrap();
        assert_eq!(value, Some(b"value1".to_vec()));
    }

    #[test]
    fn test_backend_type_display() {
        assert_eq!(super::BackendType::RocksDB.to_string(), "RocksDB");
        assert_eq!(super::BackendType::LevelDB.to_string(), "LevelDB");
    }

    #[test]
    fn test_backend_type_from_str() {
        use std::str::FromStr;

        assert_eq!(
            super::BackendType::from_str("rocksdb").unwrap(),
            super::BackendType::RocksDB
        );
        assert_eq!(
            super::BackendType::from_str("RocksDB").unwrap(),
            super::BackendType::RocksDB
        );
        assert_eq!(
            super::BackendType::from_str("ROCKSDB").unwrap(),
            super::BackendType::RocksDB
        );
        assert_eq!(
            super::BackendType::from_str("leveldb").unwrap(),
            super::BackendType::LevelDB
        );
        assert_eq!(
            super::BackendType::from_str("LevelDB").unwrap(),
            super::BackendType::LevelDB
        );

        // Test invalid backend type
        assert!(super::BackendType::from_str("invalid").is_err());
    }

    #[tokio::test]
    async fn test_factory_creates_independent_backends() {
        let temp_dir1 = tempfile::TempDir::new().unwrap();
        let temp_dir2 = tempfile::TempDir::new().unwrap();

        let backend1 =
            super::create_backend(super::BackendType::RocksDB, temp_dir1.path()).unwrap();
        let backend2 =
            super::create_backend(super::BackendType::RocksDB, temp_dir2.path()).unwrap();

        // Write to backend1
        backend1.write(b"key1", b"value1").await.unwrap();

        // Verify backend2 doesn't have the data
        let value = backend2.read(b"key1").await.unwrap();
        assert_eq!(value, None);

        // Write to backend2
        backend2.write(b"key1", b"value2").await.unwrap();

        // Verify backends have different values
        let value1 = backend1.read(b"key1").await.unwrap();
        let value2 = backend2.read(b"key1").await.unwrap();
        assert_eq!(value1, Some(b"value1".to_vec()));
        assert_eq!(value2, Some(b"value2".to_vec()));
    }

    #[tokio::test]
    async fn test_storage_backend_impl_clone() {
        let temp_dir = tempfile::TempDir::new().unwrap();
        let backend = super::create_backend(super::BackendType::RocksDB, temp_dir.path()).unwrap();

        // Clone the backend
        let backend_clone = backend.clone();

        // Write using original
        backend.write(b"key1", b"value1").await.unwrap();

        // Read using clone (should see the same data since they share the same Arc)
        let value = backend_clone.read(b"key1").await.unwrap();
        assert_eq!(value, Some(b"value1".to_vec()));
    }

    #[tokio::test]
    async fn test_storage_backend_impl_all_operations() {
        let temp_dir = tempfile::TempDir::new().unwrap();
        let backend = super::create_backend(super::BackendType::RocksDB, temp_dir.path()).unwrap();

        // Test write and read
        backend.write(b"key1", b"value1").await.unwrap();
        assert_eq!(
            backend.read(b"key1").await.unwrap(),
            Some(b"value1".to_vec())
        );

        // Test delete
        backend.delete(b"key1").await.unwrap();
        assert_eq!(backend.read(b"key1").await.unwrap(), None);

        // Test batch write
        let ops = vec![
            super::WriteOp::Put {
                key: b"key2".to_vec(),
                value: b"value2".to_vec(),
            },
            super::WriteOp::Put {
                key: b"key3".to_vec(),
                value: b"value3".to_vec(),
            },
        ];
        backend.batch_write(ops).await.unwrap();
        assert_eq!(
            backend.read(b"key2").await.unwrap(),
            Some(b"value2".to_vec())
        );
        assert_eq!(
            backend.read(b"key3").await.unwrap(),
            Some(b"value3".to_vec())
        );

        // Test read_range
        let range_results = backend
            .read_range(b"key2".to_vec()..b"key4".to_vec())
            .await
            .unwrap();
        assert_eq!(range_results.len(), 2);

        // Test get_last
        let last = backend.get_last().await.unwrap();
        assert!(last.is_some());

        // Test flush
        backend.flush().await.unwrap();
    }
}
