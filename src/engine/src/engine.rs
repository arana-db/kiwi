use crate::error::Result;
use rocksdb::{
    BoundColumnFamily, ColumnFamilyRef, DBIteratorWithThreadMode, IteratorMode, ReadOptions,
    Snapshot, WriteBatch, WriteOptions, DB,
};
use std::sync::Arc;

pub trait Engine<'a>: Send + Sync {
    // Basic key-value operations - signatures are fully aligned with RocksDB methods
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>>;

    fn get_opt(&self, key: &[u8], readopts: &ReadOptions) -> Result<Option<Vec<u8>>>;

    fn put(&self, key: &[u8], value: &[u8]) -> Result<()>;

    fn put_opt(&self, key: &[u8], value: &[u8], writeopts: &WriteOptions) -> Result<()>;

    fn delete(&self, key: &[u8]) -> Result<()>;

    fn delete_opt(&self, key: &[u8], writeopts: &WriteOptions) -> Result<()>;

    fn write(&self, batch: WriteBatch) -> Result<()>;

    fn write_opt(&self, batch: WriteBatch, writeopts: &WriteOptions) -> Result<()>;

    // Column family operations
    fn cf_handle(&self, name: &str) -> Option<Arc<BoundColumnFamily<'_>>>;

    fn get_cf(&'a self, cf: &ColumnFamilyRef<'a>, key: &[u8]) -> Result<Option<Vec<u8>>>;

    fn get_cf_opt(
        &'a self,
        cf: &ColumnFamilyRef<'a>,
        key: &[u8],
        readopts: &ReadOptions,
    ) -> Result<Option<Vec<u8>>>;

    fn put_cf(&'a self, cf: &ColumnFamilyRef<'a>, key: &[u8], value: &[u8]) -> Result<()>;

    fn put_cf_opt(
        &'a self,
        cf: &ColumnFamilyRef<'a>,
        key: &[u8],
        value: &[u8],
        writeopts: &WriteOptions,
    ) -> Result<()>;

    fn delete_cf(&'a self, cf: &ColumnFamilyRef<'a>, key: &[u8]) -> Result<()>;

    fn delete_cf_opt(
        &'a self,
        cf: &ColumnFamilyRef<'a>,
        key: &[u8],
        writeopts: &WriteOptions,
    ) -> Result<()>;

    // 迭代器
    fn iterator(&self, mode: IteratorMode) -> DBIteratorWithThreadMode<'_, DB>;

    fn iterator_opt(
        &self,
        mode: IteratorMode,
        readopts: ReadOptions,
    ) -> DBIteratorWithThreadMode<'_, DB>;

    fn iterator_cf(
        &'a self,
        cf: &ColumnFamilyRef<'a>,
        mode: IteratorMode,
    ) -> DBIteratorWithThreadMode<'a, DB>;

    fn iterator_cf_opt(
        &'a self,
        cf: &ColumnFamilyRef<'a>,
        readopts: ReadOptions,
        mode: IteratorMode,
    ) -> DBIteratorWithThreadMode<'a, DB>;

    // 维护操作
    fn flush(&self) -> Result<()>;

    fn compact_range(&self, start: Option<&[u8]>, end: Option<&[u8]>);

    fn compact_range_cf(
        &'a self,
        cf: &ColumnFamilyRef<'a>,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
    );

    // 快照
    fn snapshot(&self) -> Snapshot<'_>;
}
