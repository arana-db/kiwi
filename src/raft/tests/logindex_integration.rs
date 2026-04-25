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
// Unless required by applicable law or distributed under the License is
// distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the specific
// language governing permissions and limitations under the License.

use std::sync::Arc;

use raft::{
    CF_NAMES, LogIndexAndSequenceCollector, LogIndexOfColumnFamilies,
    LogIndexTablePropertiesCollectorFactory, PROPERTY_KEY, get_largest_log_index_from_collection,
    read_stats_from_table_props,
};
use rocksdb::{ColumnFamilyDescriptor, DB, Options};
use tempfile::TempDir;

#[test]
fn test_full_flow_multi_cf_properties() {
    let temp_dir = TempDir::new().expect("temp dir");
    let path = temp_dir.path();

    let collector = Arc::new(LogIndexAndSequenceCollector::new(0));
    let factory = LogIndexTablePropertiesCollectorFactory::new(collector.clone());

    let mut opts = Options::default();
    opts.create_if_missing(true);
    opts.create_missing_column_families(true);
    opts.set_table_properties_collector_factory(factory);

    let cfs: Vec<ColumnFamilyDescriptor> = CF_NAMES
        .iter()
        .map(|n| ColumnFamilyDescriptor::new(*n, opts.clone()))
        .collect();

    let db = DB::open_cf_descriptors(&opts, path, cfs).expect("open");

    db.put(b"k", b"v").expect("put");
    db.flush().expect("flush");

    let seq_after_first = db.latest_sequence_number();
    const TEST_LOG_INDEX: i64 = 233333;
    collector.update(TEST_LOG_INDEX, seq_after_first);
    db.put(b"k2", b"v2").expect("put");
    db.flush().expect("flush");

    let expected_seq = db.latest_sequence_number();

    let cf_tracker = LogIndexOfColumnFamilies::new();
    for i in 0..CF_NAMES.len() {
        let cf = db.cf_handle(CF_NAMES[i]).expect("cf handle");
        let collection = db
            .get_properties_of_all_tables_cf(&cf)
            .expect("get properties");
        if let Some(pair) = get_largest_log_index_from_collection(&collection) {
            assert_eq!(pair.applied_log_index(), TEST_LOG_INDEX);
            assert_eq!(pair.seqno(), expected_seq);
        }
    }

    let default_cf = db.cf_handle("default").expect("cf");
    let collection = db
        .get_properties_of_all_tables_cf(&default_cf)
        .expect("get properties");
    let pair = get_largest_log_index_from_collection(&collection)
        .expect("at least one SST with properties");
    assert_eq!(pair.applied_log_index(), TEST_LOG_INDEX);
    assert_eq!(pair.seqno(), expected_seq);

    let mut found_expected_format = false;
    for (_table_name, props) in collection.iter() {
        let user_props = props.user_collected_properties();
        if let Some(value_bytes) = user_props.get(PROPERTY_KEY.as_bytes()) {
            let value_str = String::from_utf8_lossy(value_bytes);
            assert!(
                value_str.contains('/'),
                "value format must be <log_index>/<seq> (C++ compatible)"
            );
            let pair = read_stats_from_table_props(&user_props).expect("parse");
            if pair.applied_log_index() == TEST_LOG_INDEX && pair.seqno() == expected_seq {
                found_expected_format = true;
            }
        }
    }
    assert!(
        found_expected_format,
        "must find SST with {}/{}",
        TEST_LOG_INDEX, expected_seq
    );

    cf_tracker.init(&DbAccess { db: &db }).expect("init");
    let (applied, _) = cf_tracker.get_cf_applied(0);
    let (flushed, _) = cf_tracker.get_cf_flushed(0);
    assert_eq!(applied, TEST_LOG_INDEX);
    assert_eq!(flushed, TEST_LOG_INDEX);
}

struct DbAccess<'a> {
    db: &'a DB,
}

impl raft::db_access::DbCfAccess for DbAccess<'_> {
    fn get_properties_of_all_tables_cf(
        &self,
        cf_id: usize,
    ) -> storage::logindex::db_access::Result<rocksdb::table_properties::TablePropertiesCollection>
    {
        if cf_id < CF_NAMES.len() {
            let cf = self.db.cf_handle(CF_NAMES[cf_id]).expect("cf handle");
            self.db
                .get_properties_of_all_tables_cf(&cf)
                .map_err(|e| storage::logindex::types::LogIndexError::RocksDb { source: e })
        } else {
            self.db
                .get_properties_of_all_tables()
                .map_err(|e| storage::logindex::types::LogIndexError::RocksDb { source: e })
        }
    }
}
