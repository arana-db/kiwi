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

use std::io::{Cursor, Read};

use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use serde::{Serialize, de::DeserializeOwned};
use snafu::Location;

use crate::error::Result;

pub const SEARCH_KEY_MAGIC: u8 = 0x53;
pub const SEARCH_KEY_VERSION: u8 = 0x01;
pub const SEARCH_KEY_KIND_INDEX_META: u8 = 0x01;
pub const SEARCH_KEY_KIND_FIELD_META: u8 = 0x02;
pub const SEARCH_KEY_KIND_FLAT_VECTOR_ENTRY: u8 = 0x10;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SearchKeyKind {
    IndexMeta,
    FieldMeta,
    FlatVectorEntry,
}

impl SearchKeyKind {
    fn as_u8(self) -> u8 {
        match self {
            SearchKeyKind::IndexMeta => SEARCH_KEY_KIND_INDEX_META,
            SearchKeyKind::FieldMeta => SEARCH_KEY_KIND_FIELD_META,
            SearchKeyKind::FlatVectorEntry => SEARCH_KEY_KIND_FLAT_VECTOR_ENTRY,
        }
    }

    fn try_from_u8(value: u8) -> Result<Self> {
        match value {
            SEARCH_KEY_KIND_INDEX_META => Ok(SearchKeyKind::IndexMeta),
            SEARCH_KEY_KIND_FIELD_META => Ok(SearchKeyKind::FieldMeta),
            SEARCH_KEY_KIND_FLAT_VECTOR_ENTRY => Ok(SearchKeyKind::FlatVectorEntry),
            _ => Err(crate::error::Error::InvalidFormat {
                message: format!("invalid search key kind: {value}"),
                location: Location::new(file!(), line!(), column!()),
            }),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
// SearchCF key layout. Length fields are little-endian u16 values.
//
// Common header:
// | magic(1B=0x53) | version(1B=0x01) | kind(1B) |
//
// IndexMeta:
// | header | index_len(2B LE) | index | field_len(2B LE=0) | doc_key_len(2B LE=0) |
//
// FieldMeta:
// | header | index_len(2B LE) | index | field_len(2B LE) | field | doc_key_len(2B LE=0) |
//
// FlatVectorEntry:
// | header | index_len(2B LE) | index | field_len(2B LE) | field | doc_key_len(2B LE) | doc_key |
//
// FlatVectorEntry scan prefix:
// | header | index_len(2B LE) | index | field_len(2B LE) | field |
//
// `encode_prefix()` intentionally omits the trailing `doc_key_len + doc_key` section so RocksDB
// can seek to the first entry for one `(index, field)` pair and forward-scan that contiguous range.
pub struct SearchKey {
    pub kind: SearchKeyKind,
    pub index: Vec<u8>,
    pub field: Vec<u8>,
    pub doc_key: Vec<u8>,
}

impl SearchKey {
    pub fn index_meta(index: impl Into<Vec<u8>>) -> Self {
        Self {
            kind: SearchKeyKind::IndexMeta,
            index: index.into(),
            field: Vec::new(),
            doc_key: Vec::new(),
        }
    }

    pub fn field_meta(index: impl Into<Vec<u8>>, field: impl Into<Vec<u8>>) -> Self {
        Self {
            kind: SearchKeyKind::FieldMeta,
            index: index.into(),
            field: field.into(),
            doc_key: Vec::new(),
        }
    }

    pub fn flat_vector_entry(
        index: impl Into<Vec<u8>>,
        field: impl Into<Vec<u8>>,
        doc_key: impl Into<Vec<u8>>,
    ) -> Self {
        Self {
            kind: SearchKeyKind::FlatVectorEntry,
            index: index.into(),
            field: field.into(),
            doc_key: doc_key.into(),
        }
    }

    pub fn encode(&self) -> Result<Vec<u8>> {
        let mut buf = Vec::with_capacity(
            1 + 1 + 1 + 2 + self.index.len() + 2 + self.field.len() + 2 + self.doc_key.len(),
        );
        buf.write_u8(SEARCH_KEY_MAGIC)
            .map_err(|_| crate::error::Error::InvalidArgument {
                message: "failed to write search key magic".to_string(),
                location: Location::new(file!(), line!(), column!()),
            })?;
        buf.write_u8(SEARCH_KEY_VERSION)
            .map_err(|_| crate::error::Error::InvalidArgument {
                message: "failed to write search key version".to_string(),
                location: Location::new(file!(), line!(), column!()),
            })?;
        buf.write_u8(self.kind.as_u8())
            .map_err(|_| crate::error::Error::InvalidArgument {
                message: "failed to write search key kind".to_string(),
                location: Location::new(file!(), line!(), column!()),
            })?;
        buf.write_u16::<LittleEndian>(checked_len_u16(self.index.len(), "search index")?)
            .map_err(|_| crate::error::Error::InvalidArgument {
                message: "failed to write search index length".to_string(),
                location: Location::new(file!(), line!(), column!()),
            })?;
        buf.extend_from_slice(&self.index);
        buf.write_u16::<LittleEndian>(checked_len_u16(self.field.len(), "search field")?)
            .map_err(|_| crate::error::Error::InvalidArgument {
                message: "failed to write search field length".to_string(),
                location: Location::new(file!(), line!(), column!()),
            })?;
        buf.extend_from_slice(&self.field);
        buf.write_u16::<LittleEndian>(checked_len_u16(self.doc_key.len(), "search doc key")?)
            .map_err(|_| crate::error::Error::InvalidArgument {
                message: "failed to write search doc key length".to_string(),
                location: Location::new(file!(), line!(), column!()),
            })?;
        buf.extend_from_slice(&self.doc_key);
        Ok(buf)
    }

    pub fn encode_prefix(&self) -> Result<Vec<u8>> {
        let mut buf = Vec::with_capacity(1 + 1 + 1 + 2 + self.index.len() + 2 + self.field.len());
        buf.write_u8(SEARCH_KEY_MAGIC)
            .map_err(|_| crate::error::Error::InvalidArgument {
                message: "failed to write search key magic".to_string(),
                location: Location::new(file!(), line!(), column!()),
            })?;
        buf.write_u8(SEARCH_KEY_VERSION)
            .map_err(|_| crate::error::Error::InvalidArgument {
                message: "failed to write search key version".to_string(),
                location: Location::new(file!(), line!(), column!()),
            })?;
        buf.write_u8(self.kind.as_u8())
            .map_err(|_| crate::error::Error::InvalidArgument {
                message: "failed to write search key kind".to_string(),
                location: Location::new(file!(), line!(), column!()),
            })?;
        buf.write_u16::<LittleEndian>(checked_len_u16(self.index.len(), "search index")?)
            .map_err(|_| crate::error::Error::InvalidArgument {
                message: "failed to write search index length".to_string(),
                location: Location::new(file!(), line!(), column!()),
            })?;
        buf.extend_from_slice(&self.index);
        buf.write_u16::<LittleEndian>(checked_len_u16(self.field.len(), "search field")?)
            .map_err(|_| crate::error::Error::InvalidArgument {
                message: "failed to write search field length".to_string(),
                location: Location::new(file!(), line!(), column!()),
            })?;
        buf.extend_from_slice(&self.field);
        Ok(buf)
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let mut cursor = Cursor::new(bytes);
        let magic = cursor
            .read_u8()
            .map_err(|_| crate::error::Error::InvalidFormat {
                message: "search key too short".to_string(),
                location: Location::new(file!(), line!(), column!()),
            })?;
        if magic != SEARCH_KEY_MAGIC {
            return Err(crate::error::Error::InvalidFormat {
                message: format!("invalid search key magic: {magic}"),
                location: Location::new(file!(), line!(), column!()),
            });
        }

        let version = cursor
            .read_u8()
            .map_err(|_| crate::error::Error::InvalidFormat {
                message: "search key missing version".to_string(),
                location: Location::new(file!(), line!(), column!()),
            })?;
        if version != SEARCH_KEY_VERSION {
            return Err(crate::error::Error::InvalidFormat {
                message: format!("invalid search key version: {version}"),
                location: Location::new(file!(), line!(), column!()),
            });
        }

        let kind = SearchKeyKind::try_from_u8(cursor.read_u8().map_err(|_| {
            crate::error::Error::InvalidFormat {
                message: "search key missing kind".to_string(),
                location: Location::new(file!(), line!(), column!()),
            }
        })?)?;

        let index_len =
            cursor
                .read_u16::<LittleEndian>()
                .map_err(|_| crate::error::Error::InvalidFormat {
                    message: "search key missing index length".to_string(),
                    location: Location::new(file!(), line!(), column!()),
                })? as usize;
        let mut index = vec![0; index_len];
        cursor
            .read_exact(&mut index)
            .map_err(|_| crate::error::Error::InvalidFormat {
                message: "search key truncated index".to_string(),
                location: Location::new(file!(), line!(), column!()),
            })?;

        let field_len =
            cursor
                .read_u16::<LittleEndian>()
                .map_err(|_| crate::error::Error::InvalidFormat {
                    message: "search key missing field length".to_string(),
                    location: Location::new(file!(), line!(), column!()),
                })? as usize;
        let mut field = vec![0; field_len];
        cursor
            .read_exact(&mut field)
            .map_err(|_| crate::error::Error::InvalidFormat {
                message: "search key truncated field".to_string(),
                location: Location::new(file!(), line!(), column!()),
            })?;

        let doc_key_len =
            cursor
                .read_u16::<LittleEndian>()
                .map_err(|_| crate::error::Error::InvalidFormat {
                    message: "search key missing doc key length".to_string(),
                    location: Location::new(file!(), line!(), column!()),
                })? as usize;
        let mut doc_key = vec![0; doc_key_len];
        cursor
            .read_exact(&mut doc_key)
            .map_err(|_| crate::error::Error::InvalidFormat {
                message: "search key truncated doc key".to_string(),
                location: Location::new(file!(), line!(), column!()),
            })?;

        if cursor.position() as usize != bytes.len() {
            return Err(crate::error::Error::InvalidFormat {
                message: "search key has trailing bytes".to_string(),
                location: Location::new(file!(), line!(), column!()),
            });
        }

        Ok(Self {
            kind,
            index,
            field,
            doc_key,
        })
    }
}

fn checked_len_u16(len: usize, label: &str) -> Result<u16> {
    u16::try_from(len).map_err(|_| crate::error::Error::InvalidArgument {
        message: format!("{label} length exceeds u16"),
        location: Location::new(file!(), line!(), column!()),
    })
}

pub fn encode_search_meta_value<T: Serialize>(value: &T) -> Result<Vec<u8>> {
    serde_json::to_vec(value).map_err(|error| crate::error::Error::InvalidArgument {
        message: format!("failed to serialize search metadata: {error}"),
        location: Location::new(file!(), line!(), column!()),
    })
}

pub fn decode_search_meta_value<T: DeserializeOwned>(bytes: &[u8]) -> Result<T> {
    serde_json::from_slice(bytes).map_err(|error| crate::error::Error::InvalidFormat {
        message: format!("failed to deserialize search metadata: {error}"),
        location: Location::new(file!(), line!(), column!()),
    })
}
