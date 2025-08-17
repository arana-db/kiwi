use crate::error::Result;
use crate::types::*;
use std::sync::Arc;

pub trait Engine: Send + Sync {
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>>;
    
    fn get_opt(&self, key: &[u8], opts: &ReadOptions) -> Result<Option<Vec<u8>>>;
    
    fn put(&self, key: &[u8], value: &[u8]) -> Result<()>;
    
    fn put_opt(&self, key: &[u8], value: &[u8], opts: &WriteOptions) -> Result<()>;
    
    fn delete(&self, key: &[u8]) -> Result<()>;
    
    fn delete_opt(&self, key: &[u8], opts: &WriteOptions) -> Result<()>;
    
    fn write(&self, batch: WriteBatch) -> Result<()>;
    
    fn write_opt(&self, batch: WriteBatch, opts: &WriteOptions) -> Result<()>;
    
    fn cf_handle(&self, name: &str) -> Option<ColumnFamilyHandle>;
    
    fn get_cf(&self, cf: &ColumnFamilyHandle, key: &[u8]) -> Result<Option<Vec<u8>>>;
    
    fn get_cf_opt(
        &self,
        cf: &ColumnFamilyHandle,
        key: &[u8],
        opts: &ReadOptions,
    ) -> Result<Option<Vec<u8>>>;
    
    fn put_cf(&self, cf: &ColumnFamilyHandle, key: &[u8], value: &[u8]) -> Result<()>;
    
    fn put_cf_opt(
        &self,
        cf: &ColumnFamilyHandle,
        key: &[u8],
        value: &[u8],
        opts: &WriteOptions,
    ) -> Result<()>;
    
    fn delete_cf(&self, cf: &ColumnFamilyHandle, key: &[u8]) -> Result<()>;
    
    fn delete_cf_opt(&self, cf: &ColumnFamilyHandle, key: &[u8], opts: &WriteOptions) -> Result<()>;
    
    fn iterator(&self, mode: IteratorMode) -> Box<dyn Iterator>;
    
    fn iterator_opt(&self, mode: IteratorMode, opts: ReadOptions) -> Box<dyn Iterator>;
    
    fn iterator_cf(&self, cf: &ColumnFamilyHandle, mode: IteratorMode) -> Box<dyn Iterator>;
    
    fn iterator_cf_opt(
        &self,
        cf: &ColumnFamilyHandle,
        mode: IteratorMode,
        opts: ReadOptions,
    ) -> Box<dyn Iterator>;
    
    fn flush(&self) -> Result<()>;
    
    fn compact_range(&self, start: Option<&[u8]>, end: Option<&[u8]>);
    
    fn compact_range_cf(&self, cf: &ColumnFamilyHandle, start: Option<&[u8]>, end: Option<&[u8]>);
    
    fn snapshot(&self) -> Arc<dyn Snapshot>;
}

pub trait Iterator: Send {
    fn valid(&self) -> bool;
    fn seek(&mut self, key: &[u8]);
    fn seek_to_first(&mut self);
    fn seek_to_last(&mut self);
    fn next(&mut self);
    fn prev(&mut self);
    fn key(&self) -> Option<Vec<u8>>;
    fn value(&self) -> Option<Vec<u8>>;
}

pub trait Snapshot: Send + Sync {
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>>;
    fn get_cf(&self, cf: &ColumnFamilyHandle, key: &[u8]) -> Result<Option<Vec<u8>>>;
}