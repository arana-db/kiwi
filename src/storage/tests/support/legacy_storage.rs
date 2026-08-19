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

use std::ffi::CString;
use std::path::Path;

use rocksdb::{ColumnFamilyDescriptor, DB, Options};

pub const BASE_CF_NAMES: [&str; 6] = [
    "default",
    "hash_data_cf",
    "set_data_cf",
    "list_data_cf",
    "zset_data_cf",
    "zset_score_cf",
];

pub const VECTOR_CF_NAMES: [&str; 7] = [
    "default",
    "hash_data_cf",
    "set_data_cf",
    "list_data_cf",
    "zset_data_cf",
    "zset_score_cf",
    "vector_data_cf",
];

fn cf_options(name: &str) -> Options {
    let mut options = Options::default();
    match name {
        "list_data_cf" => options.set_comparator(
            CString::new("floyd.ListsDataKeyComparator").expect("comparator name"),
            Box::new(|left, right| left.cmp(right)),
        ),
        "zset_score_cf" => options.set_comparator(
            CString::new("floyd.ZSetsScoreKeyComparator").expect("comparator name"),
            Box::new(|left, right| left.cmp(right)),
        ),
        _ => {}
    }
    options
}

pub fn descriptors(names: &[&str]) -> Vec<ColumnFamilyDescriptor> {
    names
        .iter()
        .map(|name| ColumnFamilyDescriptor::new(*name, cf_options(name)))
        .collect()
}

pub fn create_legacy_root(root: &Path, instance_count: usize, vector: bool) {
    std::fs::create_dir_all(root).expect("create legacy root");
    let names = if vector {
        VECTOR_CF_NAMES.as_slice()
    } else {
        BASE_CF_NAMES.as_slice()
    };
    for instance_id in 0..instance_count {
        let instance = root.join(instance_id.to_string());
        let mut options = Options::default();
        options.create_if_missing(true);
        options.create_missing_column_families(true);
        let db = DB::open_cf_descriptors(&options, &instance, descriptors(names))
            .expect("create legacy RocksDB");

        let default = db.cf_handle("default").expect("default CF");
        db.put_cf(&default, b"string:alpha", format!("value-{instance_id}"))
            .expect("write String sentinel");
        db.put_cf(
            &default,
            b"ttl:alpha",
            (1_900_000_000_u64 + instance_id as u64).to_le_bytes(),
        )
        .expect("write TTL sentinel");

        for (cf_name, key) in [
            ("hash_data_cf", b"hash:field".as_slice()),
            ("set_data_cf", b"set:member".as_slice()),
            ("list_data_cf", b"list:item".as_slice()),
            ("zset_data_cf", b"zset:member".as_slice()),
            (
                "zset_score_cf",
                b"00000000zset-score-key-0000000000000000".as_slice(),
            ),
        ] {
            let cf = db.cf_handle(cf_name).expect("legacy data CF");
            db.put_cf(&cf, key, format!("payload-{instance_id}-{cf_name}"))
                .expect("write legacy sentinel");
        }
    }
}

pub fn create_base_v1_root_with_wrong_list_comparator(root: &Path) {
    std::fs::create_dir_all(root).expect("create legacy root");
    let instance = root.join("0");
    let descriptors = BASE_CF_NAMES.iter().map(|name| {
        let options = if *name == "list_data_cf" {
            Options::default()
        } else {
            cf_options(name)
        };
        ColumnFamilyDescriptor::new(*name, options)
    });
    let mut options = Options::default();
    options.create_if_missing(true);
    options.create_missing_column_families(true);
    let db = DB::open_cf_descriptors(&options, &instance, descriptors)
        .expect("create Base-v1 RocksDB with wrong persisted list comparator");
    let default = db.cf_handle("default").expect("default CF");
    db.put_cf(&default, b"string:alpha", b"value-0")
        .expect("write Base-v1 sentinel");
}

pub fn list_cf(instance: &Path) -> Vec<String> {
    let mut names = DB::list_cf(&Options::default(), instance).expect("list column families");
    names.sort();
    names
}

pub fn read_sentinel(instance: &Path, cf_name: &str, key: &[u8]) -> Vec<u8> {
    let names = DB::list_cf(&Options::default(), instance).expect("list column families");
    let borrowed: Vec<&str> = names.iter().map(String::as_str).collect();
    let mut options = Options::default();
    options.create_if_missing(false);
    options.create_missing_column_families(false);
    let db = DB::open_cf_descriptors(&options, instance, descriptors(&borrowed))
        .expect("strict reopen migrated RocksDB");
    let cf = db.cf_handle(cf_name).expect("sentinel CF");
    db.get_cf(&cf, key)
        .expect("read sentinel")
        .expect("sentinel exists")
}
