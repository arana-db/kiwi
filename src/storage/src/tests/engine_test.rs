//  Copyright (c) 2017-present, arana-db Community.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

use super::engine::*;
use rocksdb::{WriteBatch, ReadOptions};
use std::fs;
use tempfile::TempDir;

#[cfg(test)]
mod tests {
    use super::*;

    fn setup() -> (TempDir, Engine) {
        let temp_dir = TempDir::new().unwrap();
        let opts = StorageOptions {
            db_path: temp_dir.path().to_str().unwrap().to_string(),
            create_if_missing: true,
            max_open_files: 100,
            write_buffer_size: 64 * 1024 * 1024,
            max_write_buffer_number: 3,
            target_file_size_base: 64 * 1024 * 1024,
            background_jobs: 2,
            use_direct_io_for_flush_and_compaction: false,
        };

        let engine = Engine::open(opts).unwrap();
        (temp_dir, engine)
    }

    #[test]
    fn test_basic_operations() {
        let (_temp_dir, engine) = setup();

        // Test put and get
        let key = b"test_key";
        let value = b"test_value";
        engine.put(key, value).unwrap();

        let result = engine.get(key).unwrap();
        assert_eq!(result.as_ref().map(|v| v.as_slice()), Some(value));

        // Test delete
        engine.delete(key).unwrap();
        let result = engine.get(key).unwrap();
        assert_eq!(result, None);
    }

    #[test]
    fn test_batch_operations() {
        let (_temp_dir, engine) = setup();

        let mut batch = WriteBatch::default();
        batch.put(b"key1", b"value1");
        batch.put(b"key2", b"value2");
        batch.delete(b"key1");

        engine.batch_write(batch).unwrap();

        assert_eq!(engine.get(b"key1").unwrap(), None);
        assert_eq!(engine.get(b"key2").unwrap().map(|v| v), Some(b"value2".to_vec()));
    }

    #[test]
    fn test_iterator() {
        let (_temp_dir, engine) = setup();

        // Insert some data
        engine.put(b"key1", b"value1").unwrap();
        engine.put(b"key2", b"value2").unwrap();
        engine.put(b"key3", b"value3").unwrap();

        let mut iter = engine.new_iterator(ReadOptions::default());
        let mut count = 0;
        iter.seek_to_first();

        while iter.valid() {
            count += 1;
            iter.next();
        }

        assert_eq!(count, 3);
    }

    #[test]
    fn test_snapshot() {
        let (_temp_dir, engine) = setup();

        // Insert initial data
        engine.put(b"key1", b"value1").unwrap();

        // Create snapshot
        let snapshot = engine.snapshot();

        // Modify data after snapshot
        engine.put(b"key1", b"value2").unwrap();
        engine.put(b"key2", b"value2").unwrap();

        // Read from snapshot should see old value
        let mut read_opts = ReadOptions::default();
        read_opts.set_snapshot(&snapshot);
        
        let result = engine.get_with_snapshot(b"key1", &snapshot).unwrap();
        assert_eq!(result.map(|v| v), Some(b"value1".to_vec()));

        // Key2 should not exist in snapshot
        let result = engine.get_with_snapshot(b"key2", &snapshot).unwrap();
        assert_eq!(result, None);
    }

    #[test]
    fn test_flush_and_compact() {
        let (_temp_dir, engine) = setup();

        // Insert some data
        for i in 0..1000 {
            let key = format!("key{}", i).into_bytes();
            let value = format!("value{}", i).into_bytes();
            engine.put(&key, &value).unwrap();
        }

        // Flush memtable to disk
        engine.flush().unwrap();

        // Compact the entire database
        engine.compact_range::<&[u8]>(None, None).unwrap();

        // Verify data after compaction
        for i in 0..1000 {
            let key = format!("key{}", i).into_bytes();
            let expected_value = format!("value{}", i).into_bytes();
            let result = engine.get(&key).unwrap();
            assert_eq!(result.map(|v| v), Some(expected_value));
        }
    }
}