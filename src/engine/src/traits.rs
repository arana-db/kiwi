use crate::error::Result;
use rocksdb::{
    BoundColumnFamily, DBIteratorWithThreadMode, IteratorMode, ReadOptions, Snapshot, WriteBatch,
    WriteOptions, DB,
};
use std::sync::Arc;

pub trait Engine<'a>: Send + Sync {
    // 基础 KV 操作 - 完全和 RocksDB 的方法签名对齐
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>>;

    fn get_opt(&self, key: &[u8], readopts: &ReadOptions) -> Result<Option<Vec<u8>>>;

    fn put(&self, key: &[u8], value: &[u8]) -> Result<()>;

    fn put_opt(&self, key: &[u8], value: &[u8], writeopts: &WriteOptions) -> Result<()>;

    fn delete(&self, key: &[u8]) -> Result<()>;

    fn delete_opt(&self, key: &[u8], writeopts: &WriteOptions) -> Result<()>;

    fn write(&self, batch: WriteBatch) -> Result<()>;

    fn write_opt(&self, batch: WriteBatch, writeopts: &WriteOptions) -> Result<()>;

    // 列族操作
    fn cf_handle(&self, name: &str) -> Option<Arc<BoundColumnFamily<'_>>>;

    fn get_cf(&'a self, cf: &BoundColumnFamily<'a>, key: &[u8]) -> Result<Option<Vec<u8>>>;

    fn get_cf_opt(
        &'a self,
        cf: &BoundColumnFamily<'a>,
        key: &[u8],
        readopts: &ReadOptions,
    ) -> Result<Option<Vec<u8>>>;

    fn put_cf(&'a self, cf: &BoundColumnFamily<'a>, key: &[u8], value: &[u8]) -> Result<()>;

    fn put_cf_opt(
        &'a self,
        cf: &BoundColumnFamily<'a>,
        key: &[u8],
        value: &[u8],
        writeopts: &WriteOptions,
    ) -> Result<()>;

    fn delete_cf(&'a self, cf: &BoundColumnFamily<'a>, key: &[u8]) -> Result<()>;

    fn delete_cf_opt(
        &'a self,
        cf: &BoundColumnFamily<'a>,
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
        cf: &BoundColumnFamily<'a>,
        mode: IteratorMode,
    ) -> DBIteratorWithThreadMode<'_, DB>;

    fn iterator_cf_opt(
        &'a self,
        cf: &BoundColumnFamily<'a>,
        readopts: ReadOptions,
        mode: IteratorMode,
    ) -> DBIteratorWithThreadMode<'_, DB>;

    // 维护操作
    fn flush(&self) -> Result<()>;

    fn compact_range(&self, start: Option<&[u8]>, end: Option<&[u8]>);

    fn compact_range_cf(
        &'a self,
        cf: &BoundColumnFamily<'a>,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
    );

    // 快照
    fn snapshot(&self) -> Snapshot<'_>;
}
