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

use std::sync::Arc;

use rocksdb::{
    BoundColumnFamily, ColumnFamilyRef, DB, DBIteratorWithThreadMode, IteratorMode, ReadOptions,
    Snapshot, WriteBatch, WriteOptions,
};

use crate::engine::{Engine, Result};

/// RocksDB implementation of the Engine trait
pub struct RocksdbEngine {
    db: Arc<DB>,
}

impl RocksdbEngine {
    /// Create a new RocksdbEngine instance
    pub fn new(db: DB) -> Self {
        Self { db: Arc::new(db) }
    }

    pub fn db(&self) -> &DB {
        &self.db
    }

    pub fn shared_db(&self) -> Arc<DB> {
        Arc::clone(&self.db)
    }
}

impl Engine for RocksdbEngine {
    // Basic KV operations
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.db.get(key)
    }

    fn get_opt(&self, key: &[u8], readopts: &ReadOptions) -> Result<Option<Vec<u8>>> {
        self.db.get_opt(key, readopts)
    }

    fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.db.put(key, value)
    }

    fn put_opt(&self, key: &[u8], value: &[u8], writeopts: &WriteOptions) -> Result<()> {
        self.db.put_opt(key, value, writeopts)
    }

    fn delete(&self, key: &[u8]) -> Result<()> {
        self.db.delete(key)
    }

    fn delete_opt(&self, key: &[u8], writeopts: &WriteOptions) -> Result<()> {
        self.db.delete_opt(key, writeopts)
    }

    fn write(&self, batch: WriteBatch) -> Result<()> {
        self.db.write(batch)
    }

    fn write_opt(&self, batch: WriteBatch, writeopts: &WriteOptions) -> Result<()> {
        self.db.write_opt(batch, writeopts)
    }

    fn set_options(&self, options: &[(&str, &str)]) -> Result<()> {
        self.db.set_options(options)
    }

    fn set_options_cf(&self, cf: &ColumnFamilyRef<'_>, options: &[(&str, &str)]) -> Result<()> {
        self.db.set_options_cf(cf, options)
    }

    fn property_int_value(&self, property: &str) -> Result<Option<u64>> {
        self.db.property_int_value(property)
    }

    // Column family operations
    fn cf_handle(&self, name: &str) -> Option<Arc<BoundColumnFamily<'_>>> {
        self.db.cf_handle(name)
    }

    fn get_cf<'a>(&'a self, cf: &ColumnFamilyRef<'a>, key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.db.get_cf(cf, key)
    }

    fn get_cf_opt<'a>(
        &'a self,
        cf: &ColumnFamilyRef<'a>,
        key: &[u8],
        readopts: &ReadOptions,
    ) -> Result<Option<Vec<u8>>> {
        self.db.get_cf_opt(cf, key, readopts)
    }

    fn put_cf<'a>(&'a self, cf: &ColumnFamilyRef<'a>, key: &[u8], value: &[u8]) -> Result<()> {
        self.db.put_cf(cf, key, value)
    }

    fn put_cf_opt<'a>(
        &'a self,
        cf: &ColumnFamilyRef<'a>,
        key: &[u8],
        value: &[u8],
        writeopts: &WriteOptions,
    ) -> Result<()> {
        self.db.put_cf_opt(cf, key, value, writeopts)
    }

    fn delete_cf<'a>(&'a self, cf: &ColumnFamilyRef<'a>, key: &[u8]) -> Result<()> {
        self.db.delete_cf(cf, key)
    }

    fn delete_cf_opt<'a>(
        &'a self,
        cf: &ColumnFamilyRef<'a>,
        key: &[u8],
        writeopts: &WriteOptions,
    ) -> Result<()> {
        self.db.delete_cf_opt(cf, key, writeopts)
    }

    // Iterator operations
    fn iterator(&self, mode: IteratorMode) -> DBIteratorWithThreadMode<'_, DB> {
        self.db.iterator(mode)
    }

    fn iterator_opt(
        &self,
        mode: IteratorMode,
        readopts: ReadOptions,
    ) -> DBIteratorWithThreadMode<'_, DB> {
        self.db.iterator_opt(mode, readopts)
    }

    fn iterator_cf<'a>(
        &'a self,
        cf: &ColumnFamilyRef<'a>,
        mode: IteratorMode,
    ) -> DBIteratorWithThreadMode<'a, DB> {
        self.db.iterator_cf(cf, mode)
    }

    fn iterator_cf_opt<'a>(
        &'a self,
        cf: &ColumnFamilyRef<'a>,
        readopts: ReadOptions,
        mode: IteratorMode,
    ) -> DBIteratorWithThreadMode<'a, DB> {
        self.db.iterator_cf_opt(cf, readopts, mode)
    }

    // Maintenance operations
    fn flush(&self) -> Result<()> {
        self.db.flush()
    }

    fn compact_range(&self, start: Option<&[u8]>, end: Option<&[u8]>) {
        self.db.compact_range(start, end);
    }

    fn compact_range_cf<'a>(
        &'a self,
        cf: &ColumnFamilyRef<'a>,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
    ) {
        self.db.compact_range_cf(cf, start, end);
    }

    // Snapshot operations
    fn snapshot(&self) -> Snapshot<'_> {
        self.db.snapshot()
    }
}

impl Clone for RocksdbEngine {
    fn clone(&self) -> Self {
        Self {
            db: Arc::clone(&self.db),
        }
    }
}

impl Drop for RocksdbEngine {
    fn drop(&mut self) {
        self.db.cancel_all_background_work(true);
    }
}

#[cfg(test)]
mod tests {
    use rocksdb::{DB, Options};
    use tempfile::TempDir;

    use super::*;

    fn create_test_db() -> (TempDir, DB) {
        let temp_dir = TempDir::new().unwrap();
        let mut opts = Options::default();
        opts.create_if_missing(true);
        let db = DB::open(&opts, temp_dir.path()).unwrap();
        (temp_dir, db)
    }

    #[test]
    fn test_basic_operations() {
        let (_temp_dir, db) = create_test_db();
        let engine = RocksdbEngine::new(db);

        // Test put and get
        let key = b"test_key";
        let value = b"test_value";
        engine.put(key, value).unwrap();

        let retrieved = engine.get(key).unwrap();
        assert_eq!(retrieved, Some(value.to_vec()));

        // Test delete
        engine.delete(key).unwrap();
        let retrieved = engine.get(key).unwrap();
        assert_eq!(retrieved, None);
    }

    #[test]
    fn test_write_batch() {
        let (_temp_dir, db) = create_test_db();
        let engine = RocksdbEngine::new(db);

        let mut batch = WriteBatch::default();
        batch.put(b"key1", b"value1");
        batch.put(b"key2", b"value2");
        batch.delete(b"key3");

        engine.write(batch).unwrap();

        assert_eq!(engine.get(b"key1").unwrap(), Some(b"value1".to_vec()));
        assert_eq!(engine.get(b"key2").unwrap(), Some(b"value2".to_vec()));
        assert_eq!(engine.get(b"key3").unwrap(), None);
    }

    #[test]
    fn test_iterator() {
        let (_temp_dir, db) = create_test_db();
        let engine = RocksdbEngine::new(db);

        // Insert some test data
        engine.put(b"a", b"1").unwrap();
        engine.put(b"b", b"2").unwrap();
        engine.put(b"c", b"3").unwrap();

        // Test forward iteration
        let iter = engine.iterator(IteratorMode::Start);
        let mut count = 0;
        for item in iter {
            let (key, value) = item.unwrap();
            assert!(!key.is_empty() && !value.is_empty());
            count += 1;
        }
        assert_eq!(count, 3);
    }
}
