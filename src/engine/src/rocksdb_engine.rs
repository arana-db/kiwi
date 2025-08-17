use crate::engine::Engine;
use crate::error::{Error, Result};
use rocksdb::{
    AsColumnFamilyRef, BoundColumnFamily, ColumnFamilyRef, DBIteratorWithThreadMode, IteratorMode,
    ReadOptions, Snapshot, WriteBatch, WriteOptions, DB,
};
use std::sync::Arc;

/// RocksDB implementation of the Engine trait
pub struct RocksdbEngine {
    db: Arc<DB>,
}

impl RocksdbEngine {
    /// Create a new RocksdbEngine instance
    pub fn new(db: DB) -> Self {
        Self { db: Arc::new(db) }
    }

    /// Get a reference to the underlying RocksDB instance
    pub fn db(&self) -> &DB {
        &self.db
    }
}

impl<'a> Engine<'a> for RocksdbEngine {
    // Basic KV operations
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.db.get(key).map_err(Error::from)
    }

    fn get_opt(&self, key: &[u8], readopts: &ReadOptions) -> Result<Option<Vec<u8>>> {
        self.db.get_opt(key, readopts).map_err(Error::from)
    }

    fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.db.put(key, value).map_err(Error::from)
    }

    fn put_opt(&self, key: &[u8], value: &[u8], writeopts: &WriteOptions) -> Result<()> {
        self.db.put_opt(key, value, writeopts).map_err(Error::from)
    }

    fn delete(&self, key: &[u8]) -> Result<()> {
        self.db.delete(key).map_err(Error::from)
    }

    fn delete_opt(&self, key: &[u8], writeopts: &WriteOptions) -> Result<()> {
        self.db.delete_opt(key, writeopts).map_err(Error::from)
    }

    fn write(&self, batch: WriteBatch) -> Result<()> {
        self.db.write(batch).map_err(Error::from)
    }

    fn write_opt(&self, batch: WriteBatch, writeopts: &WriteOptions) -> Result<()> {
        self.db.write_opt(batch, writeopts).map_err(Error::from)
    }

    // Column family operations
    fn cf_handle(&self, name: &str) -> Option<Arc<BoundColumnFamily<'_>>> {
        self.db.cf_handle(name)
    }

    fn get_cf(&'a self, cf: &ColumnFamilyRef<'a>, key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.db.get_cf(cf, key).map_err(Error::from)
    }

    fn get_cf_opt(
        &'a self,
        cf: &ColumnFamilyRef<'a>,
        key: &[u8],
        readopts: &ReadOptions,
    ) -> Result<Option<Vec<u8>>> {
        self.db.get_cf_opt(cf, key, readopts).map_err(Error::from)
    }

    fn put_cf(&'a self, cf: &ColumnFamilyRef<'a>, key: &[u8], value: &[u8]) -> Result<()> {
        self.db.put_cf(cf, key, value).map_err(Error::from)
    }

    fn put_cf_opt(
        &'a self,
        cf: &ColumnFamilyRef<'a>,
        key: &[u8],
        value: &[u8],
        writeopts: &WriteOptions,
    ) -> Result<()> {
        self.db
            .put_cf_opt(cf, key, value, writeopts)
            .map_err(Error::from)
    }

    fn delete_cf(&'a self, cf: &ColumnFamilyRef<'a>, key: &[u8]) -> Result<()> {
        self.db.delete_cf(cf, key).map_err(Error::from)
    }

    fn delete_cf_opt(
        &'a self,
        cf: &ColumnFamilyRef<'a>,
        key: &[u8],
        writeopts: &WriteOptions,
    ) -> Result<()> {
        self.db
            .delete_cf_opt(cf, key, writeopts)
            .map_err(Error::from)
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

    fn iterator_cf(
        &'a self,
        cf: &ColumnFamilyRef<'a>,
        mode: IteratorMode,
    ) -> DBIteratorWithThreadMode<'_, DB> {
        self.db.iterator_cf(cf, mode)
    }

    fn iterator_cf_opt(
        &'a self,
        cf: &ColumnFamilyRef<'a>,
        readopts: ReadOptions,
        mode: IteratorMode,
    ) -> DBIteratorWithThreadMode<'a, DB> {
        self.db.iterator_cf_opt(cf, readopts, mode)
    }

    // Maintenance operations
    fn flush(&self) -> Result<()> {
        self.db.flush().map_err(Error::from)
    }

    fn compact_range(&self, start: Option<&[u8]>, end: Option<&[u8]>) {
        self.db.compact_range(start, end);
    }

    fn compact_range_cf(
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

#[cfg(test)]
mod tests {
    use super::*;
    use rocksdb::{Options, DB};
    use tempfile::TempDir;

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
        let mut iter = engine.iterator(IteratorMode::Start);
        let mut count = 0;
        for item in iter {
            let (key, value) = item.unwrap();
            assert!(!key.is_empty() && !value.is_empty());
            count += 1;
        }
        assert_eq!(count, 3);
    }
}
