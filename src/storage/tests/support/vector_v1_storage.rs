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

use std::path::Path;

use rocksdb::{DB, IteratorMode, Options};
use serde::Serialize;
use storage::format_vector::{
    VECTOR_ENCODING_NOQUANT, VECTOR_META_FORMAT, VECTOR_METRIC_COSINE, VECTOR_VALUE_FORMAT,
    VECTOR_VALUE_MAGIC,
};
use storage::format_vector_member_key::{ParsedVectorMemberDataKey, VectorMemberDataKey};
use storage::{BaseMetaKey, DataType, STORAGE_MANIFEST_FILE};

use super::legacy_storage::{VECTOR_CF_NAMES, create_legacy_root, descriptors};

pub const VECTOR_USER_KEY: &[u8] = b"vector:set:alpha";
pub const VECTOR_ELEMENT: &[u8] = b"member:alpha";

#[derive(Serialize)]
struct VectorV1Manifest {
    version: u32,
    storage_incarnation: u64,
    next_generation: u64,
}

pub fn vector_generation(next_generation: u64) -> u64 {
    next_generation.checked_sub(1).expect("fixture generation")
}

pub fn vector_meta_key() -> Vec<u8> {
    BaseMetaKey::new(VECTOR_USER_KEY)
        .encode()
        .expect("encode vector meta key")
        .to_vec()
}

pub fn vector_member_key(storage_incarnation: u64, next_generation: u64) -> Vec<u8> {
    VectorMemberDataKey {
        key: VECTOR_USER_KEY,
        storage_incarnation,
        generation_sequence: vector_generation(next_generation),
        element: VECTOR_ELEMENT,
    }
    .encode_full()
    .expect("encode vector member key")
}

pub fn vector_meta_value(next_generation: u64) -> Vec<u8> {
    let mut value = Vec::new();
    value.push(DataType::VectorSet as u8);
    value.extend_from_slice(&1_u64.to_le_bytes());
    value.extend_from_slice(&vector_generation(next_generation).to_le_bytes());
    value.push(VECTOR_META_FORMAT);
    value.push(VECTOR_ENCODING_NOQUANT);
    value.push(VECTOR_METRIC_COSINE);
    value.push(0);
    value.extend_from_slice(&2_u32.to_le_bytes());
    value.extend_from_slice(&1_u64.to_le_bytes());
    value.extend_from_slice(&1_u64.to_le_bytes());
    value.extend_from_slice(&0_u64.to_le_bytes());
    value
}

pub fn vector_member_value() -> Vec<u8> {
    let mut value = vec![
        VECTOR_VALUE_MAGIC,
        VECTOR_VALUE_FORMAT,
        VECTOR_ENCODING_NOQUANT,
        0,
    ];
    value.extend_from_slice(&2_u32.to_le_bytes());
    value.extend_from_slice(&5.0_f32.to_le_bytes());
    value.extend_from_slice(&8_u32.to_le_bytes());
    value.extend_from_slice(&0.6_f32.to_le_bytes());
    value.extend_from_slice(&0.8_f32.to_le_bytes());
    value
}

pub fn create_vector_v1_root(root: &Path, instance_count: usize) -> Vec<(u64, u64)> {
    create_legacy_root(root, instance_count, true);
    let identities: Vec<(u64, u64)> = (0..instance_count)
        .map(|instance_id| (10_000 + instance_id as u64, 20_000 + instance_id as u64))
        .collect();
    for (instance_id, (storage_incarnation, next_generation)) in
        identities.iter().copied().enumerate()
    {
        let bytes = serde_json::to_vec(&VectorV1Manifest {
            version: 1,
            storage_incarnation,
            next_generation,
        })
        .expect("serialize Vector-v1 manifest");
        std::fs::write(
            root.join(instance_id.to_string())
                .join(STORAGE_MANIFEST_FILE),
            bytes,
        )
        .expect("write Vector-v1 manifest");

        let instance = root.join(instance_id.to_string());
        let db = DB::open_cf_descriptors(
            &Options::default(),
            &instance,
            descriptors(&VECTOR_CF_NAMES),
        )
        .expect("open Vector-v1 fixture");
        let meta_cf = db.cf_handle("default").expect("meta CF");
        db.put_cf(
            &meta_cf,
            vector_meta_key(),
            vector_meta_value(next_generation),
        )
        .expect("write vector meta");
        let vector_cf = db.cf_handle("vector_data_cf").expect("vector CF");
        db.put_cf(
            &vector_cf,
            vector_member_key(storage_incarnation, next_generation),
            vector_member_value(),
        )
        .expect("write vector member");
    }
    identities
}

pub fn rewrite_first_member_incarnation(instance: &Path, storage_incarnation: u64) {
    let db = DB::open_cf_descriptors(&Options::default(), instance, descriptors(&VECTOR_CF_NAMES))
        .expect("open Vector-v1 fixture for mutation");
    let vector_cf = db.cf_handle("vector_data_cf").expect("vector CF");
    let (old_key, value) = db
        .iterator_cf(&vector_cf, IteratorMode::Start)
        .next()
        .expect("vector member")
        .expect("read vector member");
    let parsed = ParsedVectorMemberDataKey::decode(&old_key).expect("decode vector member");
    let new_key = VectorMemberDataKey {
        key: parsed.key(),
        storage_incarnation,
        generation_sequence: parsed.generation_sequence(),
        element: parsed.element(),
    }
    .encode_full()
    .expect("encode mutated vector member");
    db.delete_cf(&vector_cf, old_key)
        .expect("delete original vector member");
    db.put_cf(&vector_cf, new_key, value)
        .expect("write mutated vector member");
}
