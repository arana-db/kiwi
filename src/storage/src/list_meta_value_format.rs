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


use crate::{
    base_value_format::{DataType, InternalValue, ParsedInternalValue},
    delegate_internal_value, delegate_parsed_value,
    error::{InvalidFormatSnafu, Result},
    storage_define::{
        BASE_META_VALUE_COUNT_LENGTH, SUFFIX_RESERVE_LENGTH, TIMESTAMP_LENGTH, TYPE_LENGTH,
        VERSION_LENGTH,
    },
};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use chrono::Utc;
use snafu::ensure;
use std::io::Cursor;

// Constants from C++ version
const INITIAL_LEFT_INDEX: u64 = 9223372036854775807;
const INITIAL_RIGHT_INDEX: u64 = 9223372036854775808;
const LIST_VALUE_INDEX_LENGTH: usize = 8;

/*
 * | type  | list_size | version | left index | right index | reserve |  cdate | timestamp |
 * |  1B   |     8B    |    8B   |     8B     |      8B     |   16B   |    8B  |     8B    |
 */
#[allow(dead_code)]
pub struct ListsMetaValue {
    pub inner: InternalValue,
    left_index: u64,
    right_index: u64,
}

delegate_internal_value!(ListsMetaValue);
#[allow(dead_code)]
impl ListsMetaValue {
    pub fn new<T>(list_size: T) -> Self
    where
        T: Into<Bytes>,
    {
        Self {
            inner: InternalValue::new(DataType::List, list_size),
            left_index: INITIAL_LEFT_INDEX,
            right_index: INITIAL_RIGHT_INDEX,
        }
    }

    pub fn update_version(&mut self) -> u64 {
        let now = Utc::now().timestamp_micros() as u64;
        self.inner.version = match self.inner.version >= now {
            true => self.inner.version + 1,
            false => now,
        };
        self.inner.version
    }

    pub fn left_index(&self) -> u64 {
        self.left_index
    }

    pub fn modify_left_index(&mut self, index: u64) {
        self.left_index -= index;
    }

    pub fn right_index(&self) -> u64 {
        self.right_index
    }

    pub fn modify_right_index(&mut self, index: u64) {
        self.right_index += index;
    }

    fn encode(&self) -> BytesMut {
        // type(1) + user_value + version(8) + left_index(8) + right_index(8) + reserve(16) + ctime(8) + etime(8)
        let needed = TYPE_LENGTH
            + self.inner.user_value.len()
            + VERSION_LENGTH
            + 2 * LIST_VALUE_INDEX_LENGTH
            + SUFFIX_RESERVE_LENGTH
            + 2 * TIMESTAMP_LENGTH;
        let mut buf = BytesMut::with_capacity(needed);

        buf.put_u8(self.inner.data_type as u8);
        buf.extend_from_slice(&self.inner.user_value);
        buf.put_u64_le(self.inner.version);
        buf.put_u64_le(self.left_index);
        buf.put_u64_le(self.right_index);
        buf.extend_from_slice(&self.inner.reserve);
        buf.put_u64_le(self.inner.ctime);
        buf.put_u64_le(self.inner.etime);

        buf
    }
}

#[allow(dead_code)]
pub struct ParsedListsMetaValue {
    inner: ParsedInternalValue,
    count: u64,
    left_index: u64,
    right_index: u64,
}

delegate_parsed_value! {ParsedListsMetaValue}
#[allow(dead_code)]
impl ParsedListsMetaValue {
    const LISTS_META_VALUE_SUFFIX_LENGTH: usize =
        VERSION_LENGTH + 2 * LIST_VALUE_INDEX_LENGTH + SUFFIX_RESERVE_LENGTH + 2 * TIMESTAMP_LENGTH;
    const LISTS_META_VALUE_LENGTH: usize =
        TYPE_LENGTH + BASE_META_VALUE_COUNT_LENGTH + Self::LISTS_META_VALUE_SUFFIX_LENGTH;

    pub fn new<T>(internal_value: T) -> Result<Self>
    where
        T: Into<BytesMut>,
    {
        let value: BytesMut = internal_value.into();
        let value_len = value.len();
        ensure!(
            value_len >= Self::LISTS_META_VALUE_LENGTH,
            InvalidFormatSnafu {
                message: format!(
                    "invalid lists meta value length: {} < {}",
                    value.len(),
                    Self::LISTS_META_VALUE_LENGTH,
                )
            }
        );

        let mut val_reader = Cursor::new(&value[..]);
        let data_type: DataType = val_reader.get_u8().try_into()?;
        let pos = val_reader.position() as usize;

        let count_range = pos..pos + BASE_META_VALUE_COUNT_LENGTH;
        let count = val_reader.get_u64_le();

        let version = val_reader.get_u64_le();
        let left_index = val_reader.get_u64_le();
        let right_index = val_reader.get_u64_le();
        let pos = val_reader.position() as usize;

        let reserve_range = pos..pos + SUFFIX_RESERVE_LENGTH;
        val_reader.advance(SUFFIX_RESERVE_LENGTH);
        let ctime = val_reader.get_u64_le();
        let etime = val_reader.get_u64_le();

        Ok(Self {
            inner: ParsedInternalValue::new(
                value,
                data_type,
                count_range,
                reserve_range,
                version,
                ctime,
                etime,
            ),
            count,
            left_index,
            right_index,
        })
    }

    pub fn initial_meta_value(&mut self) -> u64 {
        self.set_count(0);
        self.set_left_index(INITIAL_LEFT_INDEX);
        self.set_right_index(INITIAL_RIGHT_INDEX);
        self.set_etime(0);
        self.set_ctime(0);
        self.update_version()
    }

    fn set_version_to_value(&mut self) {
        let suffix_start = TYPE_LENGTH + BASE_META_VALUE_COUNT_LENGTH;
        let version_bytes = self.inner.version.to_le_bytes();
        let dst = &mut self.inner.value[suffix_start..suffix_start + VERSION_LENGTH];
        dst.copy_from_slice(&version_bytes);
    }

    fn set_ctime_to_value(&mut self) {
        let suffix_start = self.inner.value.len() - 2 * TIMESTAMP_LENGTH;
        let ctime_bytes = self.inner.ctime.to_le_bytes();
        let dst = &mut self.inner.value[suffix_start..suffix_start + TIMESTAMP_LENGTH];
        dst.copy_from_slice(&ctime_bytes)
    }

    fn set_etime_to_value(&mut self) {
        let suffix_start = self.inner.value.len() - TIMESTAMP_LENGTH;
        let etime_bytes = self.inner.etime.to_le_bytes();
        let dst = &mut self.inner.value[suffix_start..suffix_start + TIMESTAMP_LENGTH];
        dst.copy_from_slice(&etime_bytes)
    }

    fn set_count_to_value(&mut self) {
        let suffix_start = TYPE_LENGTH;
        let count_bytes = self.count.to_le_bytes();
        let dst = &mut self.inner.value[suffix_start..suffix_start + BASE_META_VALUE_COUNT_LENGTH];
        dst.copy_from_slice(&count_bytes);
    }

    fn set_index_to_value(&mut self) {
        let suffix_start = TYPE_LENGTH + BASE_META_VALUE_COUNT_LENGTH + VERSION_LENGTH;

        // Set left_index
        let left_index_bytes = self.left_index.to_le_bytes();
        let dst = &mut self.inner.value[suffix_start..suffix_start + LIST_VALUE_INDEX_LENGTH];
        dst.copy_from_slice(&left_index_bytes);

        // Set right_index
        let right_index_bytes = self.right_index.to_le_bytes();
        let dst = &mut self.inner.value
            [suffix_start + LIST_VALUE_INDEX_LENGTH..suffix_start + 2 * LIST_VALUE_INDEX_LENGTH];
        dst.copy_from_slice(&right_index_bytes);
    }

    pub fn is_valid(&self) -> bool {
        !self.inner.is_stale() && self.count != 0
    }

    pub fn count(&self) -> u64 {
        self.count
    }

    pub fn set_count(&mut self, count: u64) {
        self.count = count;
        self.set_count_to_value();
    }

    pub fn modify_count(&mut self, delta: u64) {
        self.count += delta;
        self.set_count_to_value();
    }

    pub fn set_etime(&mut self, etime: u64) {
        self.inner.etime = etime;
        self.set_etime_to_value();
    }

    pub fn set_ctime(&mut self, ctime: u64) {
        self.inner.ctime = ctime;
        self.set_ctime_to_value();
    }

    pub fn update_version(&mut self) -> u64 {
        let now = Utc::now().timestamp_micros() as u64;
        self.inner.version = match self.inner.version >= now {
            true => self.inner.version + 1,
            false => now,
        };

        self.set_version_to_value();
        self.inner.version
    }

    pub fn left_index(&self) -> u64 {
        self.left_index
    }

    pub fn set_left_index(&mut self, index: u64) {
        self.left_index = index;
        self.set_index_to_value();
    }

    pub fn modify_left_index(&mut self, index: u64) {
        self.left_index -= index;
        self.set_index_to_value();
    }

    pub fn right_index(&self) -> u64 {
        self.right_index
    }

    pub fn set_right_index(&mut self, index: u64) {
        self.right_index = index;
        self.set_index_to_value();
    }

    pub fn modify_right_index(&mut self, index: u64) {
        self.right_index += index;
        self.set_index_to_value();
    }

    pub fn strip_suffix(&mut self) {
        if !self.inner.value.is_empty() {
            let len = self.inner.value.len();
            if len >= Self::LISTS_META_VALUE_SUFFIX_LENGTH {
                self.inner
                    .value
                    .truncate(len - Self::LISTS_META_VALUE_SUFFIX_LENGTH);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const TEST_COUNT: u64 = 10;
    const TEST_VERSION: u64 = 123456789;
    const TEST_LEFT_INDEX: u64 = 1000;
    const TEST_RIGHT_INDEX: u64 = 2000;
    const TEST_CTIME: u64 = 1620000000;
    const TEST_ETIME: u64 = 1630000000;

    fn create_test_lists_meta_value() -> ListsMetaValue {
        let mut meta = ListsMetaValue::new(TEST_COUNT.to_le_bytes().to_vec());
        meta.inner.version = TEST_VERSION;
        meta.inner.ctime = TEST_CTIME;
        meta.inner.etime = TEST_ETIME;
        meta.left_index = TEST_LEFT_INDEX;
        meta.right_index = TEST_RIGHT_INDEX;
        meta
    }

    fn build_test_buffer() -> BytesMut {
        let mut buf = BytesMut::new();
        buf.put_u8(DataType::List as u8);
        buf.put_u64_le(TEST_COUNT);
        buf.put_u64_le(TEST_VERSION);
        buf.put_u64_le(TEST_LEFT_INDEX);
        buf.put_u64_le(TEST_RIGHT_INDEX);
        buf.put(&vec![0u8; SUFFIX_RESERVE_LENGTH][..]); // Reserve
        buf.put_u64_le(TEST_CTIME);
        buf.put_u64_le(TEST_ETIME);
        buf
    }

    #[test]
    fn test_lists_meta_value_new() {
        let meta = create_test_lists_meta_value();
        assert_eq!(meta.inner.data_type, DataType::List);
        assert_eq!(meta.inner.user_value, TEST_COUNT.to_le_bytes().as_slice());
        assert_eq!(meta.left_index, TEST_LEFT_INDEX);
        assert_eq!(meta.right_index, TEST_RIGHT_INDEX);
    }

    #[test]
    fn test_lists_meta_value_encode() {
        let meta = create_test_lists_meta_value();
        let encoded = meta.encode();

        let mut expected = BytesMut::new();
        expected.put_u8(DataType::List as u8);
        expected.put_slice(&TEST_COUNT.to_le_bytes());
        expected.put_u64_le(TEST_VERSION);
        expected.put_u64_le(TEST_LEFT_INDEX);
        expected.put_u64_le(TEST_RIGHT_INDEX);
        expected.extend_from_slice(&vec![0u8; SUFFIX_RESERVE_LENGTH]); // reserve
        expected.put_u64_le(TEST_CTIME);
        expected.put_u64_le(TEST_ETIME);

        assert_eq!(encoded, expected);
    }

    #[test]
    fn test_lists_meta_value_update_version() {
        let mut meta = create_test_lists_meta_value();
        meta.inner.version = 0;

        let now = Utc::now().timestamp_micros() as u64;
        let new_version = meta.update_version();

        assert!(new_version >= now);
        assert_eq!(meta.inner.version, new_version);
    }

    #[test]
    fn test_lists_meta_value_modify_indices() {
        let mut meta = create_test_lists_meta_value();

        meta.modify_left_index(100);
        assert_eq!(meta.left_index, TEST_LEFT_INDEX - 100);

        meta.modify_right_index(200);
        assert_eq!(meta.right_index, TEST_RIGHT_INDEX + 200);
    }

    #[test]
    fn test_parsed_lists_meta_value_new() {
        let buf = build_test_buffer();
        let parsed = ParsedListsMetaValue::new(buf);

        assert!(parsed.is_ok());
        let parsed = parsed.unwrap();
        assert_eq!(parsed.inner.data_type, DataType::List);
        assert_eq!(parsed.count, TEST_COUNT);
        assert_eq!(parsed.inner.version, TEST_VERSION);
        assert_eq!(parsed.left_index, TEST_LEFT_INDEX);
        assert_eq!(parsed.right_index, TEST_RIGHT_INDEX);
        assert_eq!(parsed.inner.ctime, TEST_CTIME);
        assert_eq!(parsed.inner.etime, TEST_ETIME);
    }

    #[test]
    fn test_parsed_lists_meta_value_initial_meta_value() {
        let buf = build_test_buffer();
        let mut parsed = ParsedListsMetaValue::new(buf).unwrap();

        let version = parsed.initial_meta_value();

        assert_eq!(parsed.count, 0);
        assert_eq!(parsed.left_index, INITIAL_LEFT_INDEX);
        assert_eq!(parsed.right_index, INITIAL_RIGHT_INDEX);
        assert_eq!(parsed.inner.ctime, 0);
        assert_eq!(parsed.inner.etime, 0);
        assert!(version > 0);
    }

    #[test]
    fn test_parsed_lists_meta_value_set_count() {
        let buf = build_test_buffer();
        let mut parsed = ParsedListsMetaValue::new(buf).unwrap();

        let new_count = 42;
        parsed.set_count(new_count);
        assert_eq!(parsed.count, new_count);
    }

    #[test]
    fn test_parsed_lists_meta_value_modify_count() {
        let buf = build_test_buffer();
        let mut parsed = ParsedListsMetaValue::new(buf).unwrap();

        let delta = 5;
        parsed.modify_count(delta);
        assert_eq!(parsed.count, TEST_COUNT + delta);
    }

    #[test]
    fn test_parsed_lists_meta_value_set_indices() {
        let buf = build_test_buffer();
        let mut parsed = ParsedListsMetaValue::new(buf).unwrap();

        let new_left = 500;
        let new_right = 1500;

        parsed.set_left_index(new_left);
        assert_eq!(parsed.left_index, new_left);

        parsed.set_right_index(new_right);
        assert_eq!(parsed.right_index, new_right);
    }

    #[test]
    fn test_parsed_lists_meta_value_modify_indices() {
        let buf = build_test_buffer();
        let mut parsed = ParsedListsMetaValue::new(buf).unwrap();

        parsed.modify_left_index(100);
        assert_eq!(parsed.left_index, TEST_LEFT_INDEX - 100);

        parsed.modify_right_index(200);
        assert_eq!(parsed.right_index, TEST_RIGHT_INDEX + 200);
    }

    #[test]
    fn test_parsed_lists_meta_value_is_valid() {
        let buf = build_test_buffer();
        let mut parsed = ParsedListsMetaValue::new(buf).unwrap();
        parsed.set_count(0);
        assert!(!parsed.is_valid());
    }

    #[test]
    fn test_parsed_lists_meta_value_strip_suffix() {
        let buf = build_test_buffer();
        let mut parsed = ParsedListsMetaValue::new(buf.clone()).unwrap();

        parsed.strip_suffix();

        let expected_len = buf.len() - ParsedListsMetaValue::LISTS_META_VALUE_SUFFIX_LENGTH;
        assert_eq!(parsed.inner.value.len(), expected_len);
    }

    #[test]
    fn test_parsed_lists_meta_value_roundtrip() {
        let meta = create_test_lists_meta_value();
        let encoded = meta.encode();
        let parsed = ParsedListsMetaValue::new(encoded).unwrap();

        assert_eq!(parsed.inner.data_type, DataType::List);
        assert_eq!(parsed.count, TEST_COUNT);
        assert_eq!(parsed.inner.version, TEST_VERSION);
        assert_eq!(parsed.left_index, TEST_LEFT_INDEX);
        assert_eq!(parsed.right_index, TEST_RIGHT_INDEX);
        assert_eq!(parsed.inner.ctime, TEST_CTIME);
        assert_eq!(parsed.inner.etime, TEST_ETIME);
    }
}