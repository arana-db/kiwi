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

//! Canonical storage schema registry.
//!
//! Every RocksDB column-family consumer derives names, stable identifiers,
//! comparators, codecs, and snapshot compatibility from this module.

use serde::de::Error as DeError;
use serde::{Deserialize, Deserializer, Serialize};

use crate::DataType;

/// Stable column-family identifier used in manifests, snapshots, and logindex state.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[repr(u32)]
pub enum ColumnFamilyIndex {
    #[default]
    MetaCF = 0,
    HashesDataCF = 1,
    SetsDataCF = 2,
    ListsDataCF = 3,
    ZsetsDataCF = 4,
    ZsetsScoreCF = 5,
    VectorDataCF = 6,
}

/// Logical role of a column family in the storage schema.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ColumnFamilyRole {
    Metadata,
    HashData,
    SetData,
    ListData,
    ZsetData,
    ZsetScore,
    VectorData,
}

/// Stable comparator identity persisted in the root manifest.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ComparatorId {
    Bytewise,
    ListsDataKey,
    ZsetsScoreKey,
}

/// Complete contract for one RocksDB column family.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ColumnFamilySpec {
    #[serde(skip)]
    pub index: ColumnFamilyIndex,
    pub stable_id: u32,
    pub name: &'static str,
    pub role: ColumnFamilyRole,
    pub comparator_id: ComparatorId,
    pub key_codec_version: u32,
    pub value_codec_version: u32,
    pub snapshot_read_min_version: u32,
    pub snapshot_write_version: u32,
    #[serde(skip)]
    pub use_bloom_filter: bool,
    #[serde(skip)]
    pub block_size: Option<usize>,
}

impl ColumnFamilySpec {
    pub fn data_type(&self) -> Option<DataType> {
        match self.role {
            ColumnFamilyRole::Metadata => None,
            ColumnFamilyRole::HashData => Some(DataType::Hash),
            ColumnFamilyRole::SetData => Some(DataType::Set),
            ColumnFamilyRole::ListData => Some(DataType::List),
            ColumnFamilyRole::ZsetData | ColumnFamilyRole::ZsetScore => Some(DataType::ZSet),
            ColumnFamilyRole::VectorData => Some(DataType::VectorSet),
        }
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ColumnFamilySpecWire {
    stable_id: u32,
    name: String,
    role: ColumnFamilyRole,
    comparator_id: ComparatorId,
    key_codec_version: u32,
    value_codec_version: u32,
    snapshot_read_min_version: u32,
    snapshot_write_version: u32,
}

impl<'de> Deserialize<'de> for ColumnFamilySpec {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let wire = ColumnFamilySpecWire::deserialize(deserializer)?;
        let expected = CANONICAL_COLUMN_FAMILIES
            .get(wire.stable_id as usize)
            .filter(|spec| spec.stable_id == wire.stable_id)
            .ok_or_else(|| D::Error::custom(format!("unknown stable CF id {}", wire.stable_id)))?;
        if wire.name != expected.name {
            return Err(D::Error::custom(format!(
                "CF {} name mismatch: expected {}, got {}",
                wire.stable_id, expected.name, wire.name
            )));
        }
        Ok(Self {
            index: expected.index,
            stable_id: wire.stable_id,
            name: expected.name,
            role: wire.role,
            comparator_id: wire.comparator_id,
            key_codec_version: wire.key_codec_version,
            value_codec_version: wire.value_codec_version,
            snapshot_read_min_version: wire.snapshot_read_min_version,
            snapshot_write_version: wire.snapshot_write_version,
            use_bloom_filter: expected.use_bloom_filter,
            block_size: expected.block_size,
        })
    }
}

/// Number of column families in the current storage schema.
pub const COLUMN_FAMILY_COUNT: usize = 7;

/// The only authoritative column-family registry.
pub const CANONICAL_COLUMN_FAMILIES: &[ColumnFamilySpec; COLUMN_FAMILY_COUNT] = &[
    ColumnFamilySpec {
        index: ColumnFamilyIndex::MetaCF,
        stable_id: 0,
        name: "default",
        role: ColumnFamilyRole::Metadata,
        comparator_id: ComparatorId::Bytewise,
        key_codec_version: 1,
        value_codec_version: 1,
        snapshot_read_min_version: 1,
        snapshot_write_version: 2,
        use_bloom_filter: true,
        block_size: None,
    },
    ColumnFamilySpec {
        index: ColumnFamilyIndex::HashesDataCF,
        stable_id: 1,
        name: "hash_data_cf",
        role: ColumnFamilyRole::HashData,
        comparator_id: ComparatorId::Bytewise,
        key_codec_version: 1,
        value_codec_version: 1,
        snapshot_read_min_version: 1,
        snapshot_write_version: 2,
        use_bloom_filter: true,
        block_size: None,
    },
    ColumnFamilySpec {
        index: ColumnFamilyIndex::SetsDataCF,
        stable_id: 2,
        name: "set_data_cf",
        role: ColumnFamilyRole::SetData,
        comparator_id: ComparatorId::Bytewise,
        key_codec_version: 1,
        value_codec_version: 1,
        snapshot_read_min_version: 1,
        snapshot_write_version: 2,
        use_bloom_filter: false,
        block_size: None,
    },
    ColumnFamilySpec {
        index: ColumnFamilyIndex::ListsDataCF,
        stable_id: 3,
        name: "list_data_cf",
        role: ColumnFamilyRole::ListData,
        comparator_id: ComparatorId::ListsDataKey,
        key_codec_version: 1,
        value_codec_version: 1,
        snapshot_read_min_version: 1,
        snapshot_write_version: 2,
        use_bloom_filter: true,
        block_size: None,
    },
    ColumnFamilySpec {
        index: ColumnFamilyIndex::ZsetsDataCF,
        stable_id: 4,
        name: "zset_data_cf",
        role: ColumnFamilyRole::ZsetData,
        comparator_id: ComparatorId::Bytewise,
        key_codec_version: 1,
        value_codec_version: 1,
        snapshot_read_min_version: 1,
        snapshot_write_version: 2,
        use_bloom_filter: false,
        block_size: Some(16 * 1024),
    },
    ColumnFamilySpec {
        index: ColumnFamilyIndex::ZsetsScoreCF,
        stable_id: 5,
        name: "zset_score_cf",
        role: ColumnFamilyRole::ZsetScore,
        comparator_id: ComparatorId::ZsetsScoreKey,
        key_codec_version: 1,
        value_codec_version: 1,
        snapshot_read_min_version: 1,
        snapshot_write_version: 2,
        use_bloom_filter: false,
        block_size: Some(16 * 1024),
    },
    ColumnFamilySpec {
        index: ColumnFamilyIndex::VectorDataCF,
        stable_id: 6,
        name: "vector_data_cf",
        role: ColumnFamilyRole::VectorData,
        comparator_id: ComparatorId::Bytewise,
        key_codec_version: 1,
        value_codec_version: 1,
        snapshot_read_min_version: 2,
        snapshot_write_version: 2,
        use_bloom_filter: true,
        block_size: None,
    },
];

/// Fixed-size name projection for consumers that require a const array.
pub const CANONICAL_COLUMN_FAMILY_NAMES: [&str; COLUMN_FAMILY_COUNT] = [
    CANONICAL_COLUMN_FAMILIES[0].name,
    CANONICAL_COLUMN_FAMILIES[1].name,
    CANONICAL_COLUMN_FAMILIES[2].name,
    CANONICAL_COLUMN_FAMILIES[3].name,
    CANONICAL_COLUMN_FAMILIES[4].name,
    CANONICAL_COLUMN_FAMILIES[5].name,
    CANONICAL_COLUMN_FAMILIES[6].name,
];

pub fn canonical_column_family_names() -> Vec<&'static str> {
    CANONICAL_COLUMN_FAMILIES
        .iter()
        .map(|spec| spec.name)
        .collect()
}

impl ColumnFamilyIndex {
    pub const COUNT: usize = COLUMN_FAMILY_COUNT;

    pub const ALL: [Self; Self::COUNT] = [
        CANONICAL_COLUMN_FAMILIES[0].index,
        CANONICAL_COLUMN_FAMILIES[1].index,
        CANONICAL_COLUMN_FAMILIES[2].index,
        CANONICAL_COLUMN_FAMILIES[3].index,
        CANONICAL_COLUMN_FAMILIES[4].index,
        CANONICAL_COLUMN_FAMILIES[5].index,
        CANONICAL_COLUMN_FAMILIES[6].index,
    ];

    pub fn stable_id(self) -> u32 {
        self.spec().stable_id
    }

    pub fn spec(self) -> &'static ColumnFamilySpec {
        &CANONICAL_COLUMN_FAMILIES[self as usize]
    }

    pub fn name(&self) -> &'static str {
        self.spec().name
    }

    pub fn data_type(&self) -> Option<DataType> {
        self.spec().data_type()
    }
}

impl TryFrom<u32> for ColumnFamilyIndex {
    type Error = ();

    fn try_from(value: u32) -> Result<Self, Self::Error> {
        CANONICAL_COLUMN_FAMILIES
            .get(value as usize)
            .filter(|spec| spec.stable_id == value)
            .map(|spec| spec.index)
            .ok_or(())
    }
}
