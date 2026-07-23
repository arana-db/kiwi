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
    CF_NAMES, LogIndexAndSequenceCollector, LogIndexAndSequencePair, LogIndexOfColumnFamilies,
    LogIndexTablePropertiesCollectorFactory, PROPERTY_KEY, get_largest_log_index_from_collection,
};
use rocksdb::{ColumnFamilyDescriptor, DB, Options};
use storage::logindex::db_access::DbAccess;
use tempfile::TempDir;

#[test]
fn test_full_flow_multi_cf_properties() {
    let temp_dir = TempDir::new().expect("temp dir");
    let path = temp_dir.path();

    let expected_pairs = {
        let collector = Arc::new(LogIndexAndSequenceCollector::new(0));
        let factory = LogIndexTablePropertiesCollectorFactory::new(collector.clone());

        let mut opts = Options::default();
        opts.create_if_missing(true);
        opts.create_missing_column_families(true);
        opts.set_table_properties_collector_factory(factory);

        let cfs: Vec<ColumnFamilyDescriptor> = CF_NAMES
            .iter()
            .map(|name| ColumnFamilyDescriptor::new(*name, opts.clone()))
            .collect();
        let db = DB::open_cf_descriptors(&opts, path, cfs).expect("open");

        let mut expected_pairs = Vec::with_capacity(CF_NAMES.len());
        for (cf_id, cf_name) in CF_NAMES.iter().enumerate() {
            let cf = db.cf_handle(cf_name).expect("cf handle");
            let log_index = 233_333 + cf_id as i64;
            db.put_cf(&cf, format!("key-{cf_id}"), format!("value-{cf_id}"))
                .expect("put cf");
            let expected_seq = db.latest_sequence_number();
            collector.update(log_index, expected_seq);
            db.flush_cf(&cf).expect("flush cf");

            let collection = db
                .get_properties_of_all_tables_cf(&cf)
                .expect("get properties");
            let expected_pair = LogIndexAndSequencePair::new(log_index, expected_seq);
            assert_collection_has_pair(cf_name, &collection, expected_pair);
            expected_pairs.push(expected_pair);
        }

        // The loop-scoped CF handles and collections are already gone. Drop the DB and
        // every remaining owner of the native factory/collector before reopening by path.
        drop(db);
        drop(opts);
        drop(collector);

        expected_pairs
    };

    let reopen_opts = Options::default();
    let reopen_cfs: Vec<ColumnFamilyDescriptor> = CF_NAMES
        .iter()
        .map(|name| ColumnFamilyDescriptor::new(*name, Options::default()))
        .collect();
    let reopened_db =
        DB::open_cf_descriptors(&reopen_opts, path, reopen_cfs).expect("reopen same DB path");

    for (cf_name, expected_pair) in CF_NAMES.iter().zip(&expected_pairs) {
        let cf = reopened_db.cf_handle(cf_name).expect("reopened cf handle");
        let collection = reopened_db
            .get_properties_of_all_tables_cf(&cf)
            .expect("get reopened properties");
        assert_collection_has_pair(cf_name, &collection, *expected_pair);
    }

    let cf_tracker = LogIndexOfColumnFamilies::new();
    cf_tracker
        .init(&DbAccess::new(&reopened_db))
        .expect("restore CF tracker from reopened DB");
    for (cf_id, expected_pair) in expected_pairs.iter().enumerate() {
        let expected = (expected_pair.applied_log_index(), expected_pair.seqno());
        assert_eq!(cf_tracker.get_cf_applied(cf_id), expected);
        assert_eq!(cf_tracker.get_cf_flushed(cf_id), expected);
    }
}

fn assert_collection_has_pair(
    cf_name: &str,
    collection: &rocksdb::table_properties::TablePropertiesCollection,
    expected_pair: LogIndexAndSequencePair,
) {
    let pair = get_largest_log_index_from_collection(collection)
        .unwrap_or_else(|| panic!("{cf_name} must contain an SST with log index properties"));
    assert_eq!(pair, expected_pair, "unexpected pair for {cf_name}");

    let expected_value = format!(
        "{}/{}",
        expected_pair.applied_log_index(),
        expected_pair.seqno()
    );
    let found_exact_property = collection.iter().any(|(_, props)| {
        props
            .user_collected_properties()
            .get(PROPERTY_KEY.as_bytes())
            .is_some_and(|value| value == expected_value.as_bytes())
    });
    assert!(
        found_exact_property,
        "{cf_name} must contain the exact {PROPERTY_KEY}={expected_value} property"
    );
}
