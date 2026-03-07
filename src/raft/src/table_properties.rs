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

use std::collections::HashMap;
use std::ffi::{CStr, CString};
use std::sync::Arc;

use rocksdb::table_properties::TablePropertiesCollection;
use rocksdb::table_properties_collector::{DBEntryType, TablePropertiesCollector};
use rocksdb::table_properties_collector_factory::{
    TablePropertiesCollectorContext, TablePropertiesCollectorFactory,
};

use crate::collector::LogIndexAndSequenceCollector;
use crate::types::{LogIndex, LogIndexAndSequencePair, SequenceNumber};

pub const PROPERTY_KEY: &str = "LargestLogIndex/LargestSequenceNumber";

const COLLECTOR_NAME: &str = "LogIndexTablePropertiesCollector";
const FACTORY_NAME: &str = "LogIndexTablePropertiesCollectorFactory";

/// Tracks largest_seqno during SST construction, writes "<log_index>/<seq>" on finish
pub struct LogIndexTablePropertiesCollector {
    name: CString,
    collector: Arc<LogIndexAndSequenceCollector>,
    largest_seqno: u64,
    cache: Option<LogIndex>,
}

impl LogIndexTablePropertiesCollector {
    pub fn new(collector: Arc<LogIndexAndSequenceCollector>) -> Self {
        Self {
            name: CString::new(COLLECTOR_NAME).expect("CString"),
            collector,
            largest_seqno: 0,
            cache: None,
        }
    }

    fn materialize(&mut self) -> (Vec<u8>, Vec<u8>) {
        let log_index = self.cache.unwrap_or_else(|| {
            let li = self.collector.find_applied_log_index(self.largest_seqno);
            self.cache = Some(li);
            li
        });
        let value = format!("{}/{}", log_index, self.largest_seqno);
        (PROPERTY_KEY.as_bytes().to_vec(), value.into_bytes())
    }
}

impl TablePropertiesCollector for LogIndexTablePropertiesCollector {
    fn name(&self) -> &CStr {
        self.name.as_c_str()
    }

    fn add(
        &mut self,
        _key: &[u8],
        _value: &[u8],
        _entry_type: DBEntryType,
        seq: u64,
        _file_size: u64,
    ) {
        if seq > self.largest_seqno {
            self.largest_seqno = seq;
        }
    }

    fn finish(&mut self) -> HashMap<Vec<u8>, Vec<u8>> {
        let mut m = HashMap::new();
        let (k, v) = self.materialize();
        m.insert(k, v);
        m
    }

    fn get_readable_properties(&self) -> HashMap<Vec<u8>, Vec<u8>> {
        let mut m = HashMap::new();
        let log_index = self
            .cache
            .unwrap_or_else(|| self.collector.find_applied_log_index(self.largest_seqno));
        let value = format!("{}/{}", log_index, self.largest_seqno);
        m.insert(PROPERTY_KEY.as_bytes().to_vec(), value.into_bytes());
        m
    }
}

pub struct LogIndexTablePropertiesCollectorFactory {
    name: CString,
    collector: Arc<LogIndexAndSequenceCollector>,
}

impl LogIndexTablePropertiesCollectorFactory {
    pub fn new(collector: Arc<LogIndexAndSequenceCollector>) -> Self {
        Self {
            name: CString::new(FACTORY_NAME).expect("CString"),
            collector,
        }
    }
}

impl TablePropertiesCollectorFactory for LogIndexTablePropertiesCollectorFactory {
    type Collector = LogIndexTablePropertiesCollector;

    fn create(&mut self, _context: TablePropertiesCollectorContext) -> Self::Collector {
        LogIndexTablePropertiesCollector::new(self.collector.clone())
    }

    fn name(&self) -> &CStr {
        self.name.as_c_str()
    }
}

/// Parse log_index/seqno from TableProperties' user_collected_properties
pub fn read_stats_from_table_props(
    user_properties: &HashMap<Vec<u8>, Vec<u8>>,
) -> Option<LogIndexAndSequencePair> {
    let key = PROPERTY_KEY.as_bytes();
    let value = user_properties.get(key)?;
    let s = String::from_utf8_lossy(value);
    let mut parts = s.split('/');
    let log_index: LogIndex = parts.next()?.parse().ok()?;
    let seqno: SequenceNumber = parts.next()?.parse().ok()?;
    if parts.next().is_some() {
        return None;
    }
    Some(LogIndexAndSequencePair::new(log_index, seqno))
}

/// Find the largest log index from TablePropertiesCollection
pub fn get_largest_log_index_from_collection(
    collection: &TablePropertiesCollection,
) -> Option<LogIndexAndSequencePair> {
    let mut max_log_index: LogIndex = -1;
    let mut max_seqno: SequenceNumber = 0;

    for (_, props) in collection.iter() {
        let user_props = props.user_collected_properties();
        if let Some(pair) = read_stats_from_table_props(&user_props) {
            if pair.applied_log_index() > max_log_index
                || (pair.applied_log_index() == max_log_index && pair.seqno() > max_seqno)
            {
                max_log_index = pair.applied_log_index();
                max_seqno = pair.seqno();
            }
        }
    }

    if max_log_index == -1 {
        None
    } else {
        Some(LogIndexAndSequencePair::new(max_log_index, max_seqno))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rocksdb::{DB, Options};
    use tempfile::TempDir;

    #[test]
    fn test_table_properties_collector_with_rocksdb() {
        let temp_dir = TempDir::new().expect("temp dir");
        let db_path = temp_dir.path();

        let collector = Arc::new(LogIndexAndSequenceCollector::new(0));
        let factory = LogIndexTablePropertiesCollectorFactory::new(collector.clone());

        let mut opts = Options::default();
        opts.create_if_missing(true);
        opts.set_table_properties_collector_factory(factory);

        let db = DB::open(&opts, db_path).expect("open");

        let key = b"table-property-test";
        db.put(key, key).expect("put");
        let res = db.get(key).expect("get");
        assert_eq!(res, Some(key.to_vec()));

        let latest_seq = db.latest_sequence_number();
        const TEST_LOG_INDEX: i64 = 233333;
        collector.update(TEST_LOG_INDEX, latest_seq);

        db.flush().expect("flush");

        let properties = db.get_properties_of_all_tables().expect("get properties");
        assert!(!properties.is_empty());

        let mut found = false;
        for (_table_name, props) in properties.iter() {
            let user_props = props.user_collected_properties();
            if let Some(value_bytes) = user_props.get(PROPERTY_KEY.as_bytes()) {
                let value_str = String::from_utf8_lossy(value_bytes);
                let expected = format!("{}/{}", TEST_LOG_INDEX, latest_seq);
                assert_eq!(value_str, expected);
                found = true;
                break;
            }
        }
        assert!(found, "Should find {} in table properties", PROPERTY_KEY);
    }

    #[test]
    fn test_read_stats_from_table_props() {
        let mut m = HashMap::new();
        m.insert(PROPERTY_KEY.as_bytes().to_vec(), b"233333/5".to_vec());
        let pair = read_stats_from_table_props(&m).unwrap();
        assert_eq!(pair.applied_log_index(), 233333);
        assert_eq!(pair.seqno(), 5);

        assert!(read_stats_from_table_props(&HashMap::new()).is_none());
    }

    #[test]
    fn test_read_stats_rejects_malformed_values() {
        let mut m = HashMap::new();
        m.insert(PROPERTY_KEY.as_bytes().to_vec(), b"233333/5/extra".to_vec());
        assert!(
            read_stats_from_table_props(&m).is_none(),
            "Should reject extra segments"
        );

        let mut m = HashMap::new();
        m.insert(
            PROPERTY_KEY.as_bytes().to_vec(),
            b"233333/5/extra/segments".to_vec(),
        );
        assert!(
            read_stats_from_table_props(&m).is_none(),
            "Should reject multiple extra segments"
        );
    }

    #[test]
    fn test_property_key_matches_cpp() {
        const CPP_K_PROPERTY_NAME: &str = "LargestLogIndex/LargestSequenceNumber";
        assert_eq!(
            PROPERTY_KEY, CPP_K_PROPERTY_NAME,
            "Rust PROPERTY_KEY must match C++ log_index.h kPropertyName"
        );
    }

    #[test]
    fn test_value_format_cpp_compatible() {
        let cases: &[(i64, u64)] = &[
            (0, 0),
            (233333, 5),
            (-1, 1),
            (i64::MAX, u64::MAX),
            (i64::MIN, 0),
        ];
        for &(log_index, seqno) in cases {
            let value = format!("{}/{}", log_index, seqno);
            let mut m = HashMap::new();
            m.insert(PROPERTY_KEY.as_bytes().to_vec(), value.into_bytes());
            let pair = read_stats_from_table_props(&m).expect("parse");
            assert_eq!(pair.applied_log_index(), log_index);
            assert_eq!(pair.seqno(), seqno);
        }
    }
}
