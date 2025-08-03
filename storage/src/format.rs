/*
 * Copyright (c) 2024-present, arana-db Community.  All rights reserved.
 * 
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


//! Data format definitions for different types of storage entries

use std::io;
use byteorder::{LittleEndian, WriteBytesExt, ReadBytesExt};
use crate::coding;
use crate::StorageError;
use crate::Result;

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ValueType {
    TypeDeletion = 0,
    TypeValue = 1,
    TypeMeta = 2,
    TypeList = 3,
    TypeSet = 4,
    TypeZSet = 5,
    TypeHash = 6,
}

impl From<u8> for ValueType {
    fn from(v: u8) -> Self {
        match v {
            0 => ValueType::TypeDeletion,
            1 => ValueType::TypeValue,
            2 => ValueType::TypeMeta,
            3 => ValueType::TypeList,
            4 => ValueType::TypeSet,
            5 => ValueType::TypeZSet,
            6 => ValueType::TypeHash,
            _ => ValueType::TypeDeletion,
        }
    }
}

#[derive(Debug, Clone)]
pub struct DataKey {
    pub key: Vec<u8>,
    pub version: u64,
    pub sub_key: Option<Vec<u8>>,
}

impl DataKey {
    pub fn new(key: Vec<u8>, version: u64) -> Self {
        Self {
            key,
            version,
            sub_key: None,
        }
    }

    pub fn with_sub_key(key: Vec<u8>, version: u64, sub_key: Vec<u8>) -> Self {
        Self {
            key,
            version,
            sub_key: Some(sub_key),
        }
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(self.key.len() + 8);
        buf.extend_from_slice(&self.key);
        coding::put_fixed_64(&mut buf, self.version);
        if let Some(sub_key) = &self.sub_key {
            buf.extend_from_slice(sub_key);
        }
        buf
    }

    pub fn decode(data: &[u8]) -> Result<Self> {
        if data.len() < 8 {
            return Err(StorageError::InvalidFormat("Invalid data key format".into()));
        }
        let key_len = data.len() - 8;
        let key = data[..key_len].to_vec();
        let version = coding::decode_fixed_64(&data[key_len..]);
        Ok(Self {
            key,
            version,
            sub_key: None,
        })
    }
}

#[derive(Debug, Clone)]
pub struct MetaValue {
    pub value_type: ValueType,
    pub version: u64,
    pub size: u64,
    pub extra: Option<Vec<u8>>,
}

impl MetaValue {
    pub fn new(value_type: ValueType, version: u64, size: u64) -> Self {
        Self {
            value_type,
            version,
            size,
            extra: None,
        }
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(24);
        buf.push(self.value_type as u8);
        coding::put_fixed_64(&mut buf, self.version);
        coding::put_fixed_64(&mut buf, self.size);
        if let Some(extra) = &self.extra {
            buf.extend_from_slice(extra);
        }
        buf
    }

    pub fn decode(data: &[u8]) -> Result<Self> {
        if data.len() < 17 {
            return Err(StorageError::InvalidFormat("Invalid meta value format".into()));
        }
        let value_type = ValueType::from(data[0]);
        let version = coding::decode_fixed_64(&data[1..9]);
        let size = coding::decode_fixed_64(&data[9..17]);
        let extra = if data.len() > 17 {
            Some(data[17..].to_vec())
        } else {
            None
        };
        Ok(Self {
            value_type,
            version,
            size,
            extra,
        })
    }
}