//  Copyright (c) 2017-present, arana-db Community.  All rights reserved.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

use crate::{
    base_value_format::{DataType, InternalValue, ParsedInternalValue},
    delegate_internal_value, delegate_parsed_value,
    error::{InvalidFormatSnafu, Result},
    storage_define::{
        BASE_META_VALUE_COUNT_LENGTH, BASE_META_VALUE_LENGTH, SUFFIX_RESERVE_LENGTH,
        TIMESTAMP_LENGTH, TYPE_LENGTH, VERSION_LENGTH,
    },
};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use chrono::Utc;
use snafu::ensure;
use std::io::Cursor;

#[allow(dead_code)]
type HashesMetaValue = BaseMetaValue;
#[allow(dead_code)]
type ParsedHashesMetaValue = ParsedBaseMetaValue;
#[allow(dead_code)]
type SetsMetaValue = BaseMetaValue;
#[allow(dead_code)]
type ParsedSetsMetaValue = ParsedBaseMetaValue;
#[allow(dead_code)]
type ZSetsMetaValue = BaseMetaValue;
#[allow(dead_code)]
type ParsedZSetsMetaValue = ParsedBaseMetaValue;

/*
 * | type | len | version | reserve | cdate | timestamp |
 * |  1B  | 4B  |    8B   |   16B   |   8B  |     8B    |
 */
#[allow(dead_code)]
pub struct BaseMetaValue {
    pub inner: InternalValue,
}

delegate_internal_value!(BaseMetaValue);
#[allow(dead_code)]
impl BaseMetaValue {
    pub fn new<T>(user_value: T) -> Self
    where
        T: Into<Bytes>,
    {
        Self {
            inner: InternalValue::new(DataType::None, user_value),
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

    fn encode(&self) -> BytesMut {
        // type(1) + user_value + version(8) + reserve(16) + ctime(8) + etime(8)
        let needed = TYPE_LENGTH
            + self.inner.user_value.len()
            + VERSION_LENGTH
            + SUFFIX_RESERVE_LENGTH
            + 2 * TIMESTAMP_LENGTH;
        let mut buf = BytesMut::with_capacity(needed);

        buf.put_u8(self.inner.data_type as u8);
        buf.extend_from_slice(&self.inner.user_value);
        buf.put_u64_le(self.inner.version);
        buf.extend_from_slice(&self.inner.reserve);
        buf.put_u64_le(self.inner.ctime);
        buf.put_u64_le(self.inner.etime);

        buf
    }
}

#[allow(dead_code)]
pub struct ParsedBaseMetaValue {
    inner: ParsedInternalValue,
    count: u32,
}

delegate_parsed_value! {ParsedBaseMetaValue}
#[allow(dead_code)]
impl ParsedBaseMetaValue {
    pub fn new<T>(internal_value: T) -> Result<Self>
    where
        T: Into<BytesMut>,
    {
        let value: BytesMut = internal_value.into();
        let value_len = value.len();
        ensure!(
            value_len >= BASE_META_VALUE_LENGTH,
            InvalidFormatSnafu {
                message: format!(
                    "invalid meta value length: {} < {}",
                    value.len(),
                    BASE_META_VALUE_LENGTH,
                )
            }
        );

        let mut val_reader = Cursor::new(&value[..]);
        let data_type: DataType = val_reader.get_u8().try_into()?;
        let pos = val_reader.position() as usize;

        let count_range = pos..pos + BASE_META_VALUE_COUNT_LENGTH;
        let count = val_reader.get_u32_le();
        let version = val_reader.get_u64_le();

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
        })
    }

    pub fn initial_meta_value(&mut self) -> u64 {
        self.set_count(0);
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

    pub fn is_valid(&self) -> bool {
        !self.inner.is_stale() && self.count != 0
    }

    pub fn check_set_count(&self, count: usize) -> bool {
        count <= u32::MAX as usize
    }

    pub fn count(&self) -> u32 {
        self.count
    }

    pub fn set_count(&mut self, count: u32) {
        self.count = count;
    }

    pub fn set_etime(&mut self, etime: u64) {
        self.inner.etime = etime;
        self.set_etime_to_value();
    }

    pub fn set_ctime(&mut self, ctime: u64) {
        self.inner.ctime = ctime;
        self.set_ctime_to_value();
    }

    pub fn check_modify_count(&mut self, delta: u32) -> bool {
        self.count.checked_add(delta).is_some()
    }

    pub fn modify_count(&mut self, delta: u32) {
        self.count = self.count.saturating_add(delta);
        let count_bytes = self.count.to_le_bytes();
        let dst = &mut self.inner.value[TYPE_LENGTH..TYPE_LENGTH + BASE_META_VALUE_COUNT_LENGTH];
        dst.copy_from_slice(&count_bytes);
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
}

#[cfg(test)]
mod base_meta_value_tests {
    use super::*;
    use bytes::Buf;

    const TEST_COUNT: u32 = 10;
    const TEST_VERSION: u64 = 123456789;
    const TEST_CTIME: u64 = 1620000000;
    const TEST_ETIME: u64 = 1630000000;

    fn create_test_base_meta_value() -> BaseMetaValue {
        let user_value = TEST_COUNT.to_le_bytes().to_vec();
        let mut meta = BaseMetaValue::new(Bytes::from(user_value));
        meta.inner.version = TEST_VERSION;
        meta.inner.ctime = TEST_CTIME;
        meta.inner.etime = TEST_ETIME;
        meta
    }

    #[test]
    fn test_base_meta_value_new() {
        let meta = create_test_base_meta_value();
        assert_eq!(meta.inner.data_type, DataType::None);

        let mut buf = &meta.inner.user_value[..]; // &mut &[u8]
        let user_value = buf.get_u32_le();
        assert_eq!(user_value, TEST_COUNT);
    }

    #[test]
    fn test_base_meta_value_encode() {
        let meta = create_test_base_meta_value();
        let encoded = meta.encode();

        let mut expected = BytesMut::new();
        expected.put_u8(DataType::None as u8);
        expected.put_u32_le(TEST_COUNT); // count -> user_value
        expected.put_u64_le(TEST_VERSION);
        expected.extend_from_slice(&vec![0u8; SUFFIX_RESERVE_LENGTH]); // reserve
        expected.put_u64_le(TEST_CTIME);
        expected.put_u64_le(TEST_ETIME);

        assert_eq!(encoded, expected);
    }

    #[test]
    fn test_base_meta_value_update_version() {
        let mut meta = create_test_base_meta_value();
        meta.inner.version = 0;

        let now = Utc::now().timestamp_micros() as u64;
        let new_version = meta.update_version();

        assert!(new_version >= now);
        assert_eq!(meta.inner.version, new_version);
    }

    #[test]
    fn test_base_meta_value_update_version_with_existing_greater() {
        let mut meta = create_test_base_meta_value();
        meta.inner.version = 10000000000000000; // some large value

        let new_version = meta.update_version();
        assert_eq!(new_version, meta.inner.version);
        assert_eq!(new_version, 10000000000000001); // version should be +1
    }

    #[test]
    fn test_base_meta_value_roundtrip_with_parsed() {
        let meta = create_test_base_meta_value();
        let encoded = meta.encode();

        let parsed = ParsedBaseMetaValue::new(encoded).unwrap();

        assert_eq!(parsed.inner.data_type, DataType::None);

        let user_value = (&parsed.inner.value
            [TYPE_LENGTH..TYPE_LENGTH + BASE_META_VALUE_COUNT_LENGTH])
            .get_u32_le();
        assert_eq!(user_value, TEST_COUNT);

        assert_eq!(parsed.inner.version, TEST_VERSION);
        assert_eq!(parsed.inner.ctime, TEST_CTIME);
        assert_eq!(parsed.inner.etime, TEST_ETIME);
    }
}

#[cfg(test)]
mod parsed_base_meta_value_tests {
    use super::*;
    use crate::base_value_format::DataType;
    use bytes::BytesMut;

    const TEST_VERSION: u64 = 123456789;
    const TEST_CTIME: u64 = 1620000000;
    const TEST_ETIME: u64 = 1630000000;
    const TEST_COUNT: u32 = 42;

    fn build_test_buffer() -> BytesMut {
        let mut buf = BytesMut::new();
        buf.put_u8(DataType::Hash as u8);
        buf.put_u32_le(TEST_COUNT);
        buf.put_u64_le(TEST_VERSION);
        buf.put(&vec![0u8; SUFFIX_RESERVE_LENGTH][..]); // Reserve
        buf.put_u64_le(TEST_CTIME);
        buf.put_u64_le(TEST_ETIME);
        buf
    }

    #[test]
    fn test_parsed_base_meta_value_parse_success() {
        let buf = build_test_buffer();
        let meta = ParsedBaseMetaValue::new(buf);

        assert!(meta.is_ok());
        let meta = meta.unwrap();
        assert_eq!(meta.inner.data_type, DataType::Hash);
        assert_eq!(meta.count, TEST_COUNT);
        assert_eq!(meta.inner.version, TEST_VERSION);
        assert_eq!(meta.inner.ctime, TEST_CTIME);
        assert_eq!(meta.inner.etime, TEST_ETIME);
    }

    #[test]
    fn test_parsed_base_meta_value_parse_invalid_length() {
        let mut buf = BytesMut::new();
        buf.put_u8(DataType::Hash as u8);
        buf.put_u32_le(TEST_COUNT);

        let meta = ParsedBaseMetaValue::new(buf);
        assert!(meta.is_err());
    }

    #[test]
    fn test_parsed_base_meta_value_set_version() {
        let buf = build_test_buffer();
        let mut meta = ParsedBaseMetaValue::new(buf).unwrap();

        let now = Utc::now().timestamp_micros() as u64;
        let new_version = meta.update_version();
        assert!(new_version >= now);

        let suffix_start = TYPE_LENGTH + BASE_META_VALUE_COUNT_LENGTH;
        let stored_version =
            (&meta.inner.value[suffix_start..suffix_start + VERSION_LENGTH]).get_u64_le();
        assert_eq!(stored_version, new_version);
    }

    #[test]
    fn test_parsed_base_meta_value_set_ctime() {
        let buf = build_test_buffer();
        let mut meta = ParsedBaseMetaValue::new(buf).unwrap();

        let new_ctime = 1640000000;
        meta.set_ctime(new_ctime);

        assert_eq!(meta.inner.ctime, new_ctime);

        let suffix_start = meta.inner.value.len() - 2 * TIMESTAMP_LENGTH;
        let stored_ctime =
            (&meta.inner.value[suffix_start..suffix_start + TIMESTAMP_LENGTH]).get_u64_le();
        assert_eq!(stored_ctime, new_ctime);
    }

    #[test]
    fn test_parsed_base_meta_value_set_etime() {
        let buf = build_test_buffer();
        let mut meta = ParsedBaseMetaValue::new(buf).unwrap();

        let new_etime = 1650000000;
        meta.set_etime(new_etime);

        assert_eq!(meta.inner.etime, new_etime);

        let suffix_start = meta.inner.value.len() - TIMESTAMP_LENGTH;
        let stored_etime =
            (&meta.inner.value[suffix_start..suffix_start + TIMESTAMP_LENGTH]).get_u64_le();
        assert_eq!(stored_etime, new_etime);
    }

    #[test]
    fn test_parsed_base_meta_value_modify_count() {
        let buf = build_test_buffer();
        let mut meta = ParsedBaseMetaValue::new(buf).unwrap();

        let delta = 10;
        meta.modify_count(delta);

        assert_eq!(meta.count, TEST_COUNT + delta);

        let stored_count = (&meta.inner.value
            [TYPE_LENGTH..TYPE_LENGTH + BASE_META_VALUE_COUNT_LENGTH])
            .get_u32_le();
        assert_eq!(stored_count, TEST_COUNT + delta);
    }

    #[test]
    fn test_check_modify_count_overflow() {
        let buf = build_test_buffer();
        let mut meta = ParsedBaseMetaValue::new(buf).unwrap();

        meta.count = u32::MAX - 1;
        assert!(meta.check_modify_count(1)); // 4294967294 + 1 = 4294967295 (u32::MAX)
        assert!(!meta.check_modify_count(2)); // 4294967294 + 2 = overflow
    }

    #[test]
    fn test_parsed_base_meta_value_is_valid() {
        let buf = build_test_buffer();
        let mut meta = ParsedBaseMetaValue::new(buf).unwrap();

        assert!(meta.is_valid());

        meta.set_etime(0);
        meta.set_count(0);
        assert!(!meta.is_valid());
    }

    #[test]
    fn test_parsed_base_meta_value_check_set_count() {
        assert!(ParsedBaseMetaValue::new(build_test_buffer())
            .unwrap()
            .check_set_count(100));
        assert!(!ParsedBaseMetaValue::new(build_test_buffer())
            .unwrap()
            .check_set_count(u32::MAX as usize + 1));
    }
}
