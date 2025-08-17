pub mod error;
pub mod traits;
pub use error::{Error, Result};
pub use traits::Engine;

// Re-export RocksDB types that are used in the trait
pub use rocksdb::{DBIteratorWithThreadMode, Snapshot, DB};
