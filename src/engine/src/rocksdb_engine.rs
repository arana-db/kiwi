use crate::error::{Error, Result};
use crate::traits::{Engine, Iterator as EngineIterator, Snapshot as EngineSnapshot};
use crate::types::*;
use rocksdb::{ColumnFamilyDescriptor, DBIteratorWithThreadMode, DBWithThreadMode, MultiThreaded, Options, DB};
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

pub struct RocksDBEngine {
    db: Arc<DB>,
    cf_handles: HashMap<String, ColumnFamilyHandle>,
}

impl RocksDBEngine {
    pub fn open<P: AsRef<Path>>(path: P, cfs: Vec<ColumnFamilyDescriptor>) -> Result<Self> {
        let mut cf_names = Vec::new();
        for cf in &cfs {
            cf_names.push(cf.name().to_string());
        }

        let db = DB::open_cf_descriptors(&Options::default(), path, cfs)?;
        let db = Arc::new(db);
        
        let mut cf_handles = HashMap::new();
        for name in cf_names {
            if let Some(cf_ref) = db.cf_handle(&name) {
                let handle = ColumnFamilyHandle::with_rocksdb_ref(
                    name.clone(),
                    cf_ref.clone(),
                );
                cf_handles.insert(name, handle);
            }
        }

        Ok(Self { db, cf_handles })
    }

    pub fn open_with_opts<P: AsRef<Path>>(
        path: P,
        opts: &Options,
        cfs: Vec<ColumnFamilyDescriptor>,
    ) -> Result<Self> {
        let mut cf_names = Vec::new();
        for cf in &cfs {
            cf_names.push(cf.name().to_string());
        }

        let db = DB::open_cf_descriptors(opts, path, cfs)?;
        let db = Arc::new(db);
        
        let mut cf_handles = HashMap::new();
        for name in cf_names {
            if let Some(cf_ref) = db.cf_handle(&name) {
                let handle = ColumnFamilyHandle::with_rocksdb_ref(
                    name.clone(),
                    cf_ref.clone(),
                );
                cf_handles.insert(name, handle);
            }
        }

        Ok(Self { db, cf_handles })
    }

    fn convert_read_opts(opts: &ReadOptions) -> rocksdb::ReadOptions {
        let mut rocksdb_opts = rocksdb::ReadOptions::default();
        rocksdb_opts.fill_cache(opts.fill_cache);
        rocksdb_opts.set_verify_checksums(opts.verify_checksums);
        rocksdb_opts
    }

    fn convert_write_opts(opts: &WriteOptions) -> rocksdb::WriteOptions {
        let mut rocksdb_opts = rocksdb::WriteOptions::default();
        rocksdb_opts.set_sync(opts.sync);
        rocksdb_opts.disable_wal(opts.disable_wal);
        rocksdb_opts
    }

    fn convert_iter_mode(mode: IteratorMode) -> rocksdb::IteratorMode {
        match mode {
            IteratorMode::Start => rocksdb::IteratorMode::Start,
            IteratorMode::End => rocksdb::IteratorMode::End,
            IteratorMode::From(key, Direction::Forward) => {
                rocksdb::IteratorMode::From(key, rocksdb::Direction::Forward)
            }
            IteratorMode::From(key, Direction::Reverse) => {
                rocksdb::IteratorMode::From(key, rocksdb::Direction::Reverse)
            }
        }
    }
}

impl Engine for RocksDBEngine {
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        Ok(self.db.get(key)?)
    }

    fn get_opt(&self, key: &[u8], opts: &ReadOptions) -> Result<Option<Vec<u8>>> {
        let rocksdb_opts = Self::convert_read_opts(opts);
        Ok(self.db.get_opt(key, &rocksdb_opts)?)
    }

    fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        Ok(self.db.put(key, value)?)
    }

    fn put_opt(&self, key: &[u8], value: &[u8], opts: &WriteOptions) -> Result<()> {
        let rocksdb_opts = Self::convert_write_opts(opts);
        Ok(self.db.put_opt(key, value, &rocksdb_opts)?)
    }

    fn delete(&self, key: &[u8]) -> Result<()> {
        Ok(self.db.delete(key)?)
    }

    fn delete_opt(&self, key: &[u8], opts: &WriteOptions) -> Result<()> {
        let rocksdb_opts = Self::convert_write_opts(opts);
        Ok(self.db.delete_opt(key, &rocksdb_opts)?)
    }

    fn write(&self, batch: WriteBatch) -> Result<()> {
        Ok(self.db.write(batch.into_inner())?)
    }

    fn write_opt(&self, batch: WriteBatch, opts: &WriteOptions) -> Result<()> {
        let rocksdb_opts = Self::convert_write_opts(opts);
        Ok(self.db.write_opt(batch.into_inner(), &rocksdb_opts)?)
    }

    fn cf_handle(&self, name: &str) -> Option<ColumnFamilyHandle> {
        self.cf_handles.get(name).cloned()
    }

    fn get_cf(&self, cf: &ColumnFamilyHandle, key: &[u8]) -> Result<Option<Vec<u8>>> {
        if let Some(cf_ref) = cf.as_rocksdb_ref() {
            Ok(self.db.get_cf(cf_ref, key)?)
        } else {
            Err(Error::ColumnFamilyNotFound {
                name: cf.name().to_string(),
            })
        }
    }

    fn get_cf_opt(
        &self,
        cf: &ColumnFamilyHandle,
        key: &[u8],
        opts: &ReadOptions,
    ) -> Result<Option<Vec<u8>>> {
        if let Some(cf_ref) = cf.as_rocksdb_ref() {
            let rocksdb_opts = Self::convert_read_opts(opts);
            Ok(self.db.get_cf_opt(cf_ref, key, &rocksdb_opts)?)
        } else {
            Err(Error::ColumnFamilyNotFound {
                name: cf.name().to_string(),
            })
        }
    }

    fn put_cf(&self, cf: &ColumnFamilyHandle, key: &[u8], value: &[u8]) -> Result<()> {
        if let Some(cf_ref) = cf.as_rocksdb_ref() {
            Ok(self.db.put_cf(cf_ref, key, value)?)
        } else {
            Err(Error::ColumnFamilyNotFound {
                name: cf.name().to_string(),
            })
        }
    }

    fn put_cf_opt(
        &self,
        cf: &ColumnFamilyHandle,
        key: &[u8],
        value: &[u8],
        opts: &WriteOptions,
    ) -> Result<()> {
        if let Some(cf_ref) = cf.as_rocksdb_ref() {
            let rocksdb_opts = Self::convert_write_opts(opts);
            Ok(self.db.put_cf_opt(cf_ref, key, value, &rocksdb_opts)?)
        } else {
            Err(Error::ColumnFamilyNotFound {
                name: cf.name().to_string(),
            })
        }
    }

    fn delete_cf(&self, cf: &ColumnFamilyHandle, key: &[u8]) -> Result<()> {
        if let Some(cf_ref) = cf.as_rocksdb_ref() {
            Ok(self.db.delete_cf(cf_ref, key)?)
        } else {
            Err(Error::ColumnFamilyNotFound {
                name: cf.name().to_string(),
            })
        }
    }

    fn delete_cf_opt(&self, cf: &ColumnFamilyHandle, key: &[u8], opts: &WriteOptions) -> Result<()> {
        if let Some(cf_ref) = cf.as_rocksdb_ref() {
            let rocksdb_opts = Self::convert_write_opts(opts);
            Ok(self.db.delete_cf_opt(cf_ref, key, &rocksdb_opts)?)
        } else {
            Err(Error::ColumnFamilyNotFound {
                name: cf.name().to_string(),
            })
        }
    }

    fn iterator(&self, mode: IteratorMode) -> Box<dyn EngineIterator> {
        let rocksdb_mode = Self::convert_iter_mode(mode);
        Box::new(RocksDBIterator::new(self.db.iterator(rocksdb_mode)))
    }

    fn iterator_opt(&self, mode: IteratorMode, opts: ReadOptions) -> Box<dyn EngineIterator> {
        let rocksdb_mode = Self::convert_iter_mode(mode);
        let rocksdb_opts = Self::convert_read_opts(&opts);
        Box::new(RocksDBIterator::new(self.db.iterator_opt(
            rocksdb_mode,
            rocksdb_opts,
        )))
    }

    fn iterator_cf(&self, cf: &ColumnFamilyHandle, mode: IteratorMode) -> Box<dyn EngineIterator> {
        if let Some(cf_ref) = cf.as_rocksdb_ref() {
            let rocksdb_mode = Self::convert_iter_mode(mode);
            Box::new(RocksDBIterator::new(self.db.iterator_cf(cf_ref, rocksdb_mode)))
        } else {
            Box::new(EmptyIterator {})
        }
    }

    fn iterator_cf_opt(
        &self,
        cf: &ColumnFamilyHandle,
        mode: IteratorMode,
        opts: ReadOptions,
    ) -> Box<dyn EngineIterator> {
        if let Some(cf_ref) = cf.as_rocksdb_ref() {
            let rocksdb_mode = Self::convert_iter_mode(mode);
            let rocksdb_opts = Self::convert_read_opts(&opts);
            Box::new(RocksDBIterator::new(self.db.iterator_cf_opt(
                cf_ref,
                rocksdb_opts,
                rocksdb_mode,
            )))
        } else {
            Box::new(EmptyIterator {})
        }
    }

    fn flush(&self) -> Result<()> {
        Ok(self.db.flush()?)
    }

    fn compact_range(&self, start: Option<&[u8]>, end: Option<&[u8]>) {
        self.db.compact_range(start, end);
    }

    fn compact_range_cf(&self, cf: &ColumnFamilyHandle, start: Option<&[u8]>, end: Option<&[u8]>) {
        if let Some(cf_ref) = cf.as_rocksdb_ref() {
            self.db.compact_range_cf(cf_ref, start, end);
        }
    }

    fn snapshot(&self) -> Arc<dyn EngineSnapshot> {
        Arc::new(RocksDBSnapshot::new(self.db.snapshot()))
    }
}

struct RocksDBIterator {
    iter: DBIteratorWithThreadMode<'static, DB>,
}

impl RocksDBIterator {
    fn new(iter: DBIteratorWithThreadMode<'static, DB>) -> Self {
        Self { iter }
    }
}

impl EngineIterator for RocksDBIterator {
    fn valid(&self) -> bool {
        self.iter.valid()
    }

    fn seek(&mut self, key: &[u8]) {
        self.iter.seek(key);
    }

    fn seek_to_first(&mut self) {
        self.iter.seek_to_first();
    }

    fn seek_to_last(&mut self) {
        self.iter.seek_to_last();
    }

    fn next(&mut self) {
        self.iter.next();
    }

    fn prev(&mut self) {
        self.iter.prev();
    }

    fn key(&self) -> Option<Vec<u8>> {
        if self.valid() {
            self.iter.key().map(|k| k.to_vec())
        } else {
            None
        }
    }

    fn value(&self) -> Option<Vec<u8>> {
        if self.valid() {
            self.iter.value().map(|v| v.to_vec())
        } else {
            None
        }
    }
}

struct EmptyIterator;

impl EngineIterator for EmptyIterator {
    fn valid(&self) -> bool {
        false
    }

    fn seek(&mut self, _key: &[u8]) {}
    fn seek_to_first(&mut self) {}
    fn seek_to_last(&mut self) {}
    fn next(&mut self) {}
    fn prev(&mut self) {}

    fn key(&self) -> Option<Vec<u8>> {
        None
    }

    fn value(&self) -> Option<Vec<u8>> {
        None
    }
}

struct RocksDBSnapshot {
    snapshot: rocksdb::Snapshot<'static>,
}

impl RocksDBSnapshot {
    fn new(snapshot: rocksdb::Snapshot<'static>) -> Self {
        Self { snapshot }
    }
}

impl EngineSnapshot for RocksDBSnapshot {
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        Ok(self.snapshot.get(key)?)
    }

    fn get_cf(&self, cf: &ColumnFamilyHandle, key: &[u8]) -> Result<Option<Vec<u8>>> {
        if let Some(cf_ref) = cf.as_rocksdb_ref() {
            Ok(self.snapshot.get_cf(cf_ref, key)?)
        } else {
            Err(Error::ColumnFamilyNotFound {
                name: cf.name().to_string(),
            })
        }
    }
}