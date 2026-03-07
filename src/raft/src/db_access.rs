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

// Copyright (c) 2024-present, arana-db Community.  All rights reserved.
//
// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the language governing permissions and
// limitations under the License.

use rocksdb::table_properties::TablePropertiesCollection;

/// Error type (reuse rocksdb::Error or custom)
pub type Result<T> = std::result::Result<T, rocksdb::Error>;

/// Thin wrapper interface: provides ability to get TableProperties by CF
///
/// Equivalent to C++ Redis's GetDB() + GetColumnFamilyHandles()[cf_id],
/// used for LogIndexOfColumnFamilies::Init to iterate CFs and call GetPropertiesOfAllTables.
pub trait DbCfAccess {
    /// Get TableProperties of all SSTs for specified CF
    ///
    /// # Arguments
    /// * `cf_id` - ColumnFamily index, range [0, COLUMN_FAMILY_COUNT)
    fn get_properties_of_all_tables_cf(&self, cf_id: usize) -> Result<TablePropertiesCollection>;
}
