//  Copyright (c) 2017-present, arana-db Community.  All rights reserved.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

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