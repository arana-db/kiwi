use rocksdb::{
    BoundColumnFamily, ColumnFamilyRef, DBIteratorWithThreadMode, Error, IteratorMode, ReadOptions,
    Snapshot, WriteBatch, WriteOptions, DB,
};
use std::sync::Arc;

pub type Result<T> = std::result::Result<T, Error>;

pub trait Engine: Send + Sync {
    // Basic key-value operations - signatures are fully aligned with RocksDB methods
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>>;

    fn get_opt(&self, key: &[u8], readopts: &ReadOptions) -> Result<Option<Vec<u8>>>;

    fn put(&self, key: &[u8], value: &[u8]) -> Result<()>;

    fn put_opt(&self, key: &[u8], value: &[u8], writeopts: &WriteOptions) -> Result<()>;

    fn delete(&self, key: &[u8]) -> Result<()>;

    fn delete_opt(&self, key: &[u8], writeopts: &WriteOptions) -> Result<()>;

    fn write(&self, batch: WriteBatch) -> Result<()>;

    fn write_opt(&self, batch: WriteBatch, writeopts: &WriteOptions) -> Result<()>;

    fn set_options(&self, options: &[(&str, &str)]) -> Result<()>;

    fn set_options_cf(&self, cf: &ColumnFamilyRef<'_>, options: &[(&str, &str)]) -> Result<()>;

    fn property_int_value(&self, property: &str) -> Result<Option<u64>>;

    // Column family operations
    fn cf_handle(&self, name: &str) -> Option<Arc<BoundColumnFamily<'_>>>;

    fn get_cf<'a>(&'a self, cf: &ColumnFamilyRef<'a>, key: &[u8]) -> Result<Option<Vec<u8>>>;

    fn get_cf_opt<'a>(
        &'a self,
        cf: &ColumnFamilyRef<'a>,
        key: &[u8],
        readopts: &ReadOptions,
    ) -> Result<Option<Vec<u8>>>;

    fn put_cf<'a>(&'a self, cf: &ColumnFamilyRef<'a>, key: &[u8], value: &[u8]) -> Result<()>;

    fn put_cf_opt<'a>(
        &'a self,
        cf: &ColumnFamilyRef<'a>,
        key: &[u8],
        value: &[u8],
        writeopts: &WriteOptions,
    ) -> Result<()>;

    fn delete_cf<'a>(&'a self, cf: &ColumnFamilyRef<'a>, key: &[u8]) -> Result<()>;

    fn delete_cf_opt<'a>(
        &'a self,
        cf: &ColumnFamilyRef<'a>,
        key: &[u8],
        writeopts: &WriteOptions,
    ) -> Result<()>;

    // Iterator
    fn iterator(&self, mode: IteratorMode) -> DBIteratorWithThreadMode<'_, DB>;

    fn iterator_opt(
        &self,
        mode: IteratorMode,
        readopts: ReadOptions,
    ) -> DBIteratorWithThreadMode<'_, DB>;

    fn iterator_cf<'a>(
        &'a self,
        cf: &ColumnFamilyRef<'a>,
        mode: IteratorMode,
    ) -> DBIteratorWithThreadMode<'a, DB>;

    fn iterator_cf_opt<'a>(
        &'a self,
        cf: &ColumnFamilyRef<'a>,
        readopts: ReadOptions,
        mode: IteratorMode,
    ) -> DBIteratorWithThreadMode<'a, DB>;

    fn flush(&self) -> Result<()>;

    fn compact_range(&self, start: Option<&[u8]>, end: Option<&[u8]>);

    fn compact_range_cf<'a>(
        &'a self,
        cf: &ColumnFamilyRef<'a>,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
    );

    // Snapshot
    fn snapshot(&self) -> Snapshot<'_>;
}
