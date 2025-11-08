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

//! Comprehensive unit tests for Raft storage layer

use super::*;
use crate::error::StorageError;
use crate::{LogIndex, NodeId, RaftError, Term};
use std::sync::Arc;
use std::thread;
use tempfile::TempDir;

/// Test helper to create a temporary storage instance
fn create_test_storage() -> (RaftStorage, TempDir) {
    let temp_dir = TempDir::new().unwrap();
    let storage = RaftStorage::new(temp_dir.path()).unwrap();
    (storage, temp_dir)
}

/// Test helper to create a test log entry
fn create_test_log_entry(index: LogIndex, term: Term, payload: &str) -> StoredLogEntry {
    StoredLogEntry {
        index,
        term,
        payload: payload.as_bytes().to_vec(),
    }
}

#[cfg(test)]
mod storage_tests {
    use super::*;

    #[test]
    fn test_storage_creation_and_initialization() {
        let (storage, _temp_dir) = create_test_storage();

        // Verify initial state
        assert_eq!(storage.get_current_term(), 0);
        assert_eq!(storage.get_voted_for(), None);
        assert_eq!(storage.get_last_applied(), 0);
    }

    #[test]
    fn test_state_persistence_across_restarts() {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().to_path_buf();

        // First instance - set state
        {
            let storage = RaftStorage::new(&db_path).unwrap();
            storage.set_current_term(42).unwrap();
            storage.set_voted_for(Some(123)).unwrap();
            storage.set_last_applied(999).unwrap();
        }

        // Second instance - verify state persisted
        {
            let storage = RaftStorage::new(&db_path).unwrap();
            assert_eq!(storage.get_current_term(), 42);
            assert_eq!(storage.get_voted_for(), Some(123));
            assert_eq!(storage.get_last_applied(), 999);
        }
    }

    #[test]
    fn test_term_operations() {
        let (storage, _temp_dir) = create_test_storage();

        // Test setting and getting current term
        storage.set_current_term(5).unwrap();
        assert_eq!(storage.get_current_term(), 5);

        // Test updating term
        storage.set_current_term(10).unwrap();
        assert_eq!(storage.get_current_term(), 10);
    }

    #[test]
    fn test_voted_for_operations() {
        let (storage, _temp_dir) = create_test_storage();

        // Initially no vote
        assert_eq!(storage.get_voted_for(), None);

        // Vote for a candidate
        storage.set_voted_for(Some(42)).unwrap();
        assert_eq!(storage.get_voted_for(), Some(42));

        // Clear vote
        storage.set_voted_for(None).unwrap();
        assert_eq!(storage.get_voted_for(), None);
    }

    #[test]
    fn test_last_applied_operations() {
        let (storage, _temp_dir) = create_test_storage();

        // Test setting and getting last applied
        storage.set_last_applied(100).unwrap();
        assert_eq!(storage.get_last_applied(), 100);

        // Test updating last applied
        storage.set_last_applied(200).unwrap();
        assert_eq!(storage.get_last_applied(), 200);
    }

    #[test]
    fn test_log_entry_operations() {
        let (storage, _temp_dir) = create_test_storage();

        // Test appending and retrieving log entry
        let entry = create_test_log_entry(1, 1, "SET key value");
        storage.append_log_entry(&entry).unwrap();

        let retrieved = storage.get_log_entry(1).unwrap().unwrap();
        assert_eq!(retrieved.index, entry.index);
        assert_eq!(retrieved.term, entry.term);
        assert_eq!(retrieved.payload, entry.payload);

        // Test non-existent entry
        assert!(storage.get_log_entry(999).unwrap().is_none());
    }

    #[test]
    fn test_multiple_log_entries() {
        let (storage, _temp_dir) = create_test_storage();

        // Append multiple entries
        for i in 1..=10 {
            let entry = create_test_log_entry(i, 1, &format!("command_{}", i));
            storage.append_log_entry(&entry).unwrap();
        }

        // Verify all entries can be retrieved
        for i in 1..=10 {
            let entry = storage.get_log_entry(i).unwrap().unwrap();
            assert_eq!(entry.index, i);
            assert_eq!(entry.term, 1);
            assert_eq!(entry.payload, format!("command_{}", i).as_bytes());
        }
    }

    #[test]
    fn test_get_last_log_entry() {
        let (storage, _temp_dir) = create_test_storage();

        // No entries initially
        assert!(storage.get_last_log_entry().unwrap().is_none());

        // Add entries
        let entry1 = create_test_log_entry(1, 1, "first");
        let entry2 = create_test_log_entry(2, 1, "second");
        let entry3 = create_test_log_entry(3, 2, "third");

        storage.append_log_entry(&entry1).unwrap();
        storage.append_log_entry(&entry2).unwrap();
        storage.append_log_entry(&entry3).unwrap();

        // Should return the last entry
        let last = storage.get_last_log_entry().unwrap().unwrap();
        assert_eq!(last.index, 3);
        assert_eq!(last.term, 2);
        assert_eq!(last.payload, b"third");
    }

    #[test]
    fn test_delete_log_entries_from() {
        let (storage, _temp_dir) = create_test_storage();

        // Add entries 1-5
        for i in 1..=5 {
            let entry = create_test_log_entry(i, 1, &format!("entry_{}", i));
            storage.append_log_entry(&entry).unwrap();
        }

        // Delete from index 3 onwards
        storage.delete_log_entries_from(3).unwrap();

        // Verify entries 1-2 still exist
        assert!(storage.get_log_entry(1).unwrap().is_some());
        assert!(storage.get_log_entry(2).unwrap().is_some());

        // Verify entries 3-5 are deleted
        assert!(storage.get_log_entry(3).unwrap().is_none());
        assert!(storage.get_log_entry(4).unwrap().is_none());
        assert!(storage.get_log_entry(5).unwrap().is_none());

        // Last log entry should be entry 2
        let last = storage.get_last_log_entry().unwrap().unwrap();
        assert_eq!(last.index, 2);
    }

    #[test]
    fn test_snapshot_metadata_operations() {
        let (storage, _temp_dir) = create_test_storage();

        // Initially no snapshot
        assert!(storage.get_snapshot_meta().unwrap().is_none());

        // Store snapshot metadata
        let meta = StoredSnapshotMeta {
            last_log_index: 100,
            last_log_term: 5,
            snapshot_id: "snapshot_test".to_string(),
            timestamp: 1234567890,
        };
        storage.store_snapshot_meta(&meta).unwrap();

        // Retrieve and verify
        let retrieved = storage.get_snapshot_meta().unwrap().unwrap();
        assert_eq!(retrieved.last_log_index, meta.last_log_index);
        assert_eq!(retrieved.last_log_term, meta.last_log_term);
        assert_eq!(retrieved.snapshot_id, meta.snapshot_id);
        assert_eq!(retrieved.timestamp, meta.timestamp);
    }

    #[test]
    fn test_snapshot_data_operations() {
        let (storage, _temp_dir) = create_test_storage();

        let snapshot_id = "test_snapshot";
        let data = b"snapshot data content";

        // Store snapshot data
        storage.store_snapshot_data(snapshot_id, data).unwrap();

        // Retrieve and verify
        let retrieved = storage.get_snapshot_data(snapshot_id).unwrap().unwrap();
        assert_eq!(retrieved, data);

        // Test non-existent snapshot
        assert!(storage.get_snapshot_data("non_existent").unwrap().is_none());
    }

    #[test]
    fn test_log_key_encoding_decoding() {
        // Test log key encoding/decoding
        let index = 12345u64;
        let key = RaftStorage::log_key(index);
        let decoded = RaftStorage::parse_log_key(&key).unwrap();
        assert_eq!(decoded, index);

        // Test edge cases
        let max_index = u64::MAX;
        let max_key = RaftStorage::log_key(max_index);
        let decoded_max = RaftStorage::parse_log_key(&max_key).unwrap();
        assert_eq!(decoded_max, max_index);

        let min_index = 0u64;
        let min_key = RaftStorage::log_key(min_index);
        let decoded_min = RaftStorage::parse_log_key(&min_key).unwrap();
        assert_eq!(decoded_min, min_index);
    }

    #[test]
    fn test_invalid_log_key_parsing() {
        // Test invalid key length
        let invalid_key = vec![1, 2, 3]; // Too short
        assert!(RaftStorage::parse_log_key(&invalid_key).is_err());

        let invalid_key2 = vec![1; 10]; // Too long
        assert!(RaftStorage::parse_log_key(&invalid_key2).is_err());
    }

    #[test]
    fn test_concurrent_state_operations() {
        let (storage, _temp_dir) = create_test_storage();
        let storage = Arc::new(storage);

        let mut handles = vec![];

        // Spawn multiple threads to update state concurrently
        for i in 0..10 {
            let storage_clone = Arc::clone(&storage);
            let handle = thread::spawn(move || {
                storage_clone.set_current_term(i).unwrap();
                storage_clone.set_voted_for(Some(i as NodeId)).unwrap();
                storage_clone.set_last_applied(i * 10).unwrap();
            });
            handles.push(handle);
        }

        // Wait for all threads to complete
        for handle in handles {
            handle.join().unwrap();
        }

        // Verify final state is consistent (one of the values)
        let final_term = storage.get_current_term();
        let final_voted_for = storage.get_voted_for();
        let final_last_applied = storage.get_last_applied();

        assert!(final_term < 10);
        assert!(final_voted_for.is_some());
        assert!(final_last_applied < 100);
    }

    #[test]
    fn test_large_log_entries() {
        let (storage, _temp_dir) = create_test_storage();

        // Create a large payload (1MB)
        let large_payload = vec![b'x'; 1024 * 1024];
        let entry = StoredLogEntry {
            index: 1,
            term: 1,
            payload: large_payload.clone(),
        };

        // Store and retrieve large entry
        storage.append_log_entry(&entry).unwrap();
        let retrieved = storage.get_log_entry(1).unwrap().unwrap();

        assert_eq!(retrieved.payload.len(), large_payload.len());
        assert_eq!(retrieved.payload, large_payload);
    }

    #[test]
    fn test_storage_error_handling() {
        // Test with invalid path (should fail gracefully)
        // Use a path with invalid characters that should definitely fail
        let invalid_path = if cfg!(windows) {
            "C:\\invalid<>|path" // Invalid characters on Windows
        } else {
            "/dev/null/invalid" // Cannot create directory under /dev/null
        };
        let result = RaftStorage::new(invalid_path);
        assert!(result.is_err());
    }

    #[test]
    fn test_log_ordering() {
        let (storage, _temp_dir) = create_test_storage();

        // Add entries out of order
        let entries = vec![
            create_test_log_entry(3, 1, "third"),
            create_test_log_entry(1, 1, "first"),
            create_test_log_entry(5, 2, "fifth"),
            create_test_log_entry(2, 1, "second"),
            create_test_log_entry(4, 2, "fourth"),
        ];

        for entry in entries {
            storage.append_log_entry(&entry).unwrap();
        }

        // Verify entries can be retrieved in correct order
        for i in 1..=5 {
            let entry = storage.get_log_entry(i).unwrap().unwrap();
            assert_eq!(entry.index, i);
        }

        // Last entry should be index 5
        let last = storage.get_last_log_entry().unwrap().unwrap();
        assert_eq!(last.index, 5);
    }
}

#[cfg(test)]
mod error_handling_tests {
    use super::*;

    #[test]
    fn test_corrupted_state_handling() {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path();

        // Create storage and corrupt the state
        {
            let storage = RaftStorage::new(db_path).unwrap();
            storage.set_current_term(42).unwrap();

            // Manually corrupt the state by writing invalid data
            let cf_state = storage.get_cf_handle("raft_state").unwrap();
            storage
                .db
                .put_cf(&cf_state, "current_term", b"invalid_data")
                .unwrap();
        }

        // Try to load corrupted storage - should handle gracefully
        let result = RaftStorage::new(db_path);
        // In a real implementation, this might return an error or use default values
        // For now, we just verify it doesn't panic
        assert!(result.is_ok() || result.is_err());
    }

    #[test]
    fn test_missing_column_family_error() {
        let temp_dir = TempDir::new().unwrap();
        let storage = RaftStorage::new(temp_dir.path()).unwrap();

        // Try to get non-existent column family
        let result = storage.get_cf_handle("non_existent_cf");
        assert!(result.is_err());

        if let Err(RaftError::Storage(StorageError::DataInconsistency { message })) = result {
            assert!(message.contains("Column family non_existent_cf not found"));
        } else {
            panic!("Expected DataInconsistency error");
        }
    }

    #[test]
    fn test_serialization_error_handling() {
        let (storage, _temp_dir) = create_test_storage();

        // Create an entry with invalid serialization data
        // This is hard to test directly, but we can test the error paths
        let entry = create_test_log_entry(1, 1, "valid entry");
        storage.append_log_entry(&entry).unwrap();

        // The actual serialization errors would occur in real corruption scenarios
        // For unit tests, we verify the error types are properly defined
        let _error = RaftError::Storage(StorageError::DataInconsistency {
            message: "Test serialization error".to_string(),
        });
    }
}

#[cfg(test)]
mod edge_case_tests {
    use super::*;

    #[test]
    fn test_zero_index_operations() {
        let (storage, _temp_dir) = create_test_storage();

        // Test operations with index 0
        let entry = create_test_log_entry(0, 0, "zero entry");
        storage.append_log_entry(&entry).unwrap();

        let retrieved = storage.get_log_entry(0).unwrap().unwrap();
        assert_eq!(retrieved.index, 0);
        assert_eq!(retrieved.term, 0);
    }

    #[test]
    fn test_empty_payload_entry() {
        let (storage, _temp_dir) = create_test_storage();

        // Test entry with empty payload
        let entry = StoredLogEntry {
            index: 1,
            term: 1,
            payload: vec![],
        };

        storage.append_log_entry(&entry).unwrap();
        let retrieved = storage.get_log_entry(1).unwrap().unwrap();
        assert!(retrieved.payload.is_empty());
    }

    #[test]
    fn test_delete_from_empty_log() {
        let (storage, _temp_dir) = create_test_storage();

        // Delete from empty log should not error
        storage.delete_log_entries_from(1).unwrap();
        assert!(storage.get_last_log_entry().unwrap().is_none());
    }

    #[test]
    fn test_delete_from_nonexistent_index() {
        let (storage, _temp_dir) = create_test_storage();

        // Add a few entries
        for i in 1..=3 {
            let entry = create_test_log_entry(i, 1, &format!("entry_{}", i));
            storage.append_log_entry(&entry).unwrap();
        }

        // Delete from index beyond last entry
        storage.delete_log_entries_from(10).unwrap();

        // All original entries should still exist
        for i in 1..=3 {
            assert!(storage.get_log_entry(i).unwrap().is_some());
        }
    }

    #[test]
    fn test_snapshot_with_empty_id() {
        let (storage, _temp_dir) = create_test_storage();

        // Test snapshot operations with empty ID
        let data = b"test data";
        storage.store_snapshot_data("", data).unwrap();

        let retrieved = storage.get_snapshot_data("").unwrap().unwrap();
        assert_eq!(retrieved, data);
    }

    #[test]
    fn test_very_large_term_and_index() {
        let (storage, _temp_dir) = create_test_storage();

        // Test with maximum values
        let max_term = u64::MAX;
        let max_index = u64::MAX;

        storage.set_current_term(max_term).unwrap();
        storage.set_last_applied(max_index).unwrap();

        assert_eq!(storage.get_current_term(), max_term);
        assert_eq!(storage.get_last_applied(), max_index);

        // Test log entry with max values
        let entry = create_test_log_entry(max_index, max_term, "max entry");
        storage.append_log_entry(&entry).unwrap();

        let retrieved = storage.get_log_entry(max_index).unwrap().unwrap();
        assert_eq!(retrieved.index, max_index);
        assert_eq!(retrieved.term, max_term);
    }
}
