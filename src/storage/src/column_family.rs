//  Copyright (c) 2017-present, arana-db Community.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

//! Column family management for storage engine

use rocksdb::{ColumnFamilyDescriptor, Options, DB};
use crate::{Result, StorageError};
use crate::options::ColumnFamilyType;

/// Default column family names
pub const CF_DEFAULT: &str = "default";
pub const CF_META: &str = "meta";
pub const CF_DATA: &str = "data";

/// Column family manager
pub struct ColumnFamilyManager {
    descriptors: Vec<ColumnFamilyDescriptor>,
}

impl ColumnFamilyManager {
    /// Create a new column family manager
    pub fn new() -> Self {
        Self {
            descriptors: Vec::new(),
        }
    }

    /// Add a column family descriptor
    pub fn add_column_family(&mut self, name: &str, options: Options) {
        self.descriptors.push(ColumnFamilyDescriptor::new(name, options));
    }

    /// Get column family descriptors
    pub fn get_descriptors(&self) -> &[ColumnFamilyDescriptor] {
        &self.descriptors
    }

    /// Create column families in database
    pub fn create_column_families(&self, db: &DB) -> Result<()> {
        for cf in &self.descriptors {
            if !db.cf_handle(cf.name()).is_some() {
                db.create_cf(cf).map_err(StorageError::from)?;
            }
        }
        Ok(())
    }

    /// Drop column families from database
    pub fn drop_column_families(&self, db: &DB) -> Result<()> {
        for cf in &self.descriptors {
            if db.cf_handle(cf.name()).is_some() {
                db.drop_cf(cf.name()).map_err(StorageError::from)?;
            }
        }
        Ok(())
    }

    /// Get column family type from name
    pub fn get_cf_type(name: &str) -> ColumnFamilyType {
        match name {
            CF_META => ColumnFamilyType::Meta,
            CF_DATA => ColumnFamilyType::Data,
            _ => ColumnFamilyType::MetaAndData,
        }
    }

    /// Get column family name from type
    pub fn get_cf_name(cf_type: ColumnFamilyType) -> &'static str {
        match cf_type {
            ColumnFamilyType::Meta => CF_META,
            ColumnFamilyType::Data => CF_DATA,
            ColumnFamilyType::MetaAndData => CF_DEFAULT,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cf_type_name_conversion() {
        assert_eq!(ColumnFamilyManager::get_cf_type(CF_META), ColumnFamilyType::Meta);
        assert_eq!(ColumnFamilyManager::get_cf_type(CF_DATA), ColumnFamilyType::Data);
        assert_eq!(ColumnFamilyManager::get_cf_type(CF_DEFAULT), ColumnFamilyType::MetaAndData);

        assert_eq!(ColumnFamilyManager::get_cf_name(ColumnFamilyType::Meta), CF_META);
        assert_eq!(ColumnFamilyManager::get_cf_name(ColumnFamilyType::Data), CF_DATA);
        assert_eq!(ColumnFamilyManager::get_cf_name(ColumnFamilyType::MetaAndData), CF_DEFAULT);
    }
}