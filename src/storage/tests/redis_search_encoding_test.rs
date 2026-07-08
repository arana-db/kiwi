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
// See the License for the specific language governing permissions and
// limitations under the License.

#![allow(clippy::unwrap_used)]

use storage::search_encoding::{
    SearchKey, SearchKeyKind, decode_search_meta_value, encode_search_meta_value,
};
use storage::search_types::{
    DistanceMetric, SearchDataType, SearchFieldSchema, SearchIndexSchema, VectorAlgorithm,
    VectorFieldSchema, VectorValueType,
};

fn vec_u8(data: &[u8]) -> Vec<u8> {
    data.to_vec()
}

#[test]
fn search_key_roundtrip() {
    let original =
        SearchKey::flat_vector_entry(vec_u8(b"idx"), vec_u8(b"emb"), vec_u8(b"\x00\xffk"));
    let encoded = original.encode().unwrap();
    let decoded = SearchKey::decode(&encoded).unwrap();

    assert_eq!(decoded.kind, SearchKeyKind::FlatVectorEntry);
    assert_eq!(decoded.index, b"idx");
    assert_eq!(decoded.field, b"emb");
    assert_eq!(decoded.doc_key, b"\x00\xffk");
}

#[test]
fn index_meta_roundtrip() {
    let schema = SearchIndexSchema {
        name: b"idx".to_vec(),
        on: SearchDataType::Hash,
        prefixes: vec![b"doc:".to_vec(), b"item:".to_vec()],
        fields: vec![(
            b"emb".to_vec(),
            SearchFieldSchema::Vector(VectorFieldSchema {
                algorithm: VectorAlgorithm::Flat,
                value_type: VectorValueType::Float32,
                dim: 3,
                distance_metric: DistanceMetric::L2,
            }),
        )],
    };

    let encoded = encode_search_meta_value(&schema).unwrap();
    let decoded: SearchIndexSchema = decode_search_meta_value(&encoded).unwrap();
    assert_eq!(decoded, schema);
}

#[test]
fn rejects_bad_search_key_prefix() {
    let mut encoded = SearchKey::index_meta(b"idx".to_vec()).encode().unwrap();
    encoded[0] = 0x00;
    assert!(SearchKey::decode(&encoded).is_err());
}

#[test]
fn rejects_truncated_search_key() {
    let encoded = SearchKey::field_meta(b"idx".to_vec(), b"emb".to_vec())
        .encode()
        .unwrap();
    for len in 0..encoded.len() {
        assert!(SearchKey::decode(&encoded[..len]).is_err());
    }
}
