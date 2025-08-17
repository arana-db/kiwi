use bytes::Bytes;
use std::fmt::Debug;

#[derive(Debug, Clone)]
pub struct WriteOptions {
    pub sync: bool,
    pub disable_wal: bool,
}

impl Default for WriteOptions {
    fn default() -> Self {
        Self {
            sync: false,
            disable_wal: false,
        }
    }
}

#[derive(Debug, Clone)]
pub struct ReadOptions {
    pub fill_cache: bool,
    pub verify_checksums: bool,
}

impl Default for ReadOptions {
    fn default() -> Self {
        Self {
            fill_cache: true,
            verify_checksums: false,
        }
    }
}

#[derive(Debug, Clone)]
pub enum IteratorMode<'a> {
    Start,
    End,
    From(&'a [u8], Direction),
}

#[derive(Debug, Clone, Copy)]
pub enum Direction {
    Forward,
    Reverse,
}

pub struct WriteBatch {
    inner: rocksdb::WriteBatch,
}

impl WriteBatch {
    pub fn new() -> Self {
        Self {
            inner: rocksdb::WriteBatch::default(),
        }
    }

    pub fn put(&mut self, key: &[u8], value: &[u8]) {
        self.inner.put(key, value);
    }

    pub fn put_cf(&mut self, cf: &ColumnFamilyHandle, key: &[u8], value: &[u8]) {
        if let Some(cf_ref) = cf.as_rocksdb_ref() {
            self.inner.put_cf(cf_ref, key, value);
        }
    }

    pub fn delete(&mut self, key: &[u8]) {
        self.inner.delete(key);
    }

    pub fn delete_cf(&mut self, cf: &ColumnFamilyHandle, key: &[u8]) {
        if let Some(cf_ref) = cf.as_rocksdb_ref() {
            self.inner.delete_cf(cf_ref, key);
        }
    }

    pub fn clear(&mut self) {
        self.inner.clear();
    }

    pub(crate) fn into_inner(self) -> rocksdb::WriteBatch {
        self.inner
    }
}

impl Default for WriteBatch {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Clone)]
pub struct ColumnFamilyHandle {
    name: String,
    rocksdb_ref: Option<std::sync::Arc<rocksdb::BoundColumnFamily<'static>>>,
}

impl ColumnFamilyHandle {
    pub fn new(name: String) -> Self {
        Self {
            name,
            rocksdb_ref: None,
        }
    }

    pub(crate) fn with_rocksdb_ref(
        name: String,
        cf_ref: std::sync::Arc<rocksdb::BoundColumnFamily<'static>>,
    ) -> Self {
        Self {
            name,
            rocksdb_ref: Some(cf_ref),
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub(crate) fn as_rocksdb_ref(&self) -> Option<&std::sync::Arc<rocksdb::BoundColumnFamily<'static>>> {
        self.rocksdb_ref.as_ref()
    }
}

impl Debug for ColumnFamilyHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ColumnFamilyHandle")
            .field("name", &self.name)
            .finish()
    }
}