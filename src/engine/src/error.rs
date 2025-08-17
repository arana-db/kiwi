use snafu::Snafu;
use std::fmt;

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("RocksDB error: {}", source))]
    RocksDB { source: rocksdb::Error },
    
    #[snafu(display("IO error: {}", source))]
    Io { source: std::io::Error },
    
    #[snafu(display("Invalid operation: {}", message))]
    InvalidOperation { message: String },
    
    #[snafu(display("Column family not found: {}", name))]
    ColumnFamilyNotFound { name: String },
}

pub type Result<T> = std::result::Result<T, Error>;

impl From<rocksdb::Error> for Error {
    fn from(source: rocksdb::Error) -> Self {
        Error::RocksDB { source }
    }
}