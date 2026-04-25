// Copyright (c) 2024-present, arana-db Community.  All rights reserved.
//
// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! DbCfAccess trait: thin wrapper interface for getting TableProperties by CF

use rocksdb::DB;

use crate::logindex::types::{LogIndexError, cf_metadata::CF_NAMES_STR};

/// Result type for DbCfAccess operations
pub type Result<T> = std::result::Result<T, LogIndexError>;

/// Thin wrapper interface: provides ability to get TableProperties by CF
///
/// Equivalent to C++ Redis's GetDB() + GetColumnFamilyHandles()[cf_id],
/// used for LogIndexOfColumnFamilies::Init to iterate CFs and call GetPropertiesOfAllTables.
pub trait DbCfAccess {
    /// Get TableProperties of all SSTs for specified CF
    ///
    /// cf_id=0 means default CF, cf_id>=1 means other CFs
    fn get_properties_of_all_tables_cf(
        &self,
        cf_id: usize,
    ) -> Result<rocksdb::table_properties::TablePropertiesCollection>;
}

/// Concrete implementation of DbCfAccess for rocksdb::DB
pub struct DbAccess<'a> {
    db: &'a DB,
}

impl<'a> DbAccess<'a> {
    pub fn new(db: &'a DB) -> Self {
        Self { db }
    }
}

impl DbCfAccess for DbAccess<'_> {
    fn get_properties_of_all_tables_cf(
        &self,
        cf_id: usize,
    ) -> Result<rocksdb::table_properties::TablePropertiesCollection> {
        let cf_name = CF_NAMES_STR
            .get(cf_id)
            .ok_or(LogIndexError::InvalidCfId { cf_id })?;
        let cf = self
            .db
            .cf_handle(cf_name)
            .ok_or_else(|| LogIndexError::CfNotFound {
                cf_name: cf_name.to_string(),
            })?;
        self.db
            .get_properties_of_all_tables_cf(&cf)
            .map_err(|e| LogIndexError::RocksDb { source: e })
    }
}
