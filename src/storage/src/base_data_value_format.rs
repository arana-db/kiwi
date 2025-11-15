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

use bytes::{Buf, BufMut, Bytes, BytesMut};
use snafu::ensure;

use crate::{
    base_value_format::{DataType, InternalValue, ParsedInternalValue},
    delegate_internal_value, delegate_parsed_value,
    error::{InvalidFormatSnafu, Result},
    storage_define::{SUFFIX_RESERVE_LENGTH, TIMESTAMP_LENGTH},
};

// hash/set/zset/list data value format
// | value | reserve | ctime |
// |       |   16B   |   8B  |

#[allow(dead_code)]
pub struct BaseDataValue {
    inner: InternalValue,
}

delegate_internal_value!(BaseDataValue);
#[allow(dead_code)]
impl BaseDataValue {
    pub fn new<T>(user_value: T) -> Self
    where
        T: Into<Bytes>,
    {
        Self {
            inner: InternalValue::new(DataType::None, user_value),
        }
    }

    pub fn encode(&self) -> BytesMut {
        let user_value_size = self.inner.user_value.len();
        // hash/set/zset/list data value format:
        //          |     value      |       reserve         |     ctime       |
        //          |                |         16B           |      8B         |
        let needed = user_value_size + SUFFIX_RESERVE_LENGTH + TIMESTAMP_LENGTH;
        let mut buf = BytesMut::with_capacity(needed);

        buf.put_slice(&self.inner.user_value);
        buf.put_bytes(0, SUFFIX_RESERVE_LENGTH);
        buf.put_u64_le(self.inner.ctime);

        buf
    }
}

delegate_parsed_value!(ParsedBaseDataValue);
#[allow(dead_code)]
pub struct ParsedBaseDataValue {
    inner: ParsedInternalValue,
}

#[allow(dead_code)]
impl ParsedBaseDataValue {
    const BASEDATAVALUESUFFIXLENGTH: usize = SUFFIX_RESERVE_LENGTH + TIMESTAMP_LENGTH;

    pub fn new<T>(internal_value: T) -> Result<Self>
    where
        T: Into<BytesMut>,
    {
        let value: BytesMut = internal_value.into();
        ensure!(
            value.len() >= Self::BASEDATAVALUESUFFIXLENGTH,
            InvalidFormatSnafu {
                message: format!(
                    "invalid base data value length: {} < {}",
                    value.len(),
                    Self::BASEDATAVALUESUFFIXLENGTH
                )
            }
        );

        let user_value_len = value.len() - Self::BASEDATAVALUESUFFIXLENGTH;
        let user_value_range = 0..user_value_len;

        let reserve_end = user_value_len + SUFFIX_RESERVE_LENGTH;
        let reserve_range = user_value_len..reserve_end;

        let mut time_reader = &value[reserve_end..];
        ensure!(
            time_reader.len() >= TIMESTAMP_LENGTH,
            InvalidFormatSnafu {
                message: format!(
                    "invalid base data value length: {} < {}",
                    time_reader.len(),
                    TIMESTAMP_LENGTH
                )
            }
        );
        let ctime = time_reader.get_u64_le();

        Ok(Self {
            inner: ParsedInternalValue::new(
                value,
                DataType::None,
                user_value_range,
                reserve_range,
                0,
                ctime,
                0,
            ),
        })
    }

    pub fn set_ctime(&mut self, ctime: u64) {
        self.inner.ctime = ctime;
        self.set_ctime_to_value();
    }

    fn set_ctime_to_value(&mut self) {
        let suffix_start = self.inner.value.len() - TIMESTAMP_LENGTH;

        let ctime_bytes = self.inner.ctime.to_le_bytes();
        let dst = &mut self.inner.value[suffix_start..suffix_start + TIMESTAMP_LENGTH];
        dst.copy_from_slice(&ctime_bytes);
    }

    pub fn strip_suffix(&mut self) {
        if !self.inner.value.is_empty() {
            let len = self.inner.value.len();
            if len >= Self::BASEDATAVALUESUFFIXLENGTH {
                self.inner
                    .value
                    .truncate(len - Self::BASEDATAVALUESUFFIXLENGTH);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const TEST_CTIME: u64 = 1620000000;
    const TEST_VALUE: &[u8] = b"test_data";
    const TEST_VALUE_EMPTY: &[u8] = b"";
    const TEST_VALUE_LARGE: &[u8] = b"this_is_a_very_long_test_value_for_testing_large_data_values";

    fn build_test_buffer() -> BytesMut {
        let mut buf = BytesMut::new();
        buf.put_slice(TEST_VALUE);
        buf.put_bytes(0, SUFFIX_RESERVE_LENGTH); // reserve
        buf.put_u64_le(TEST_CTIME);
        buf
    }

    fn build_test_buffer_with_value(value: &[u8]) -> BytesMut {
        let mut buf = BytesMut::new();
        buf.put_slice(value);
        buf.put_bytes(0, SUFFIX_RESERVE_LENGTH); // reserve
        buf.put_u64_le(TEST_CTIME);
        buf
    }

    // ==================== BaseDataValue Tests ====================

    #[test]
    fn test_base_data_value_new() {
        let data_value = BaseDataValue::new(TEST_VALUE);
        assert_eq!(data_value.inner.data_type, DataType::None);
        assert_eq!(data_value.inner.user_value, TEST_VALUE);
        assert_eq!(data_value.inner.etime, 0);
        assert_eq!(data_value.inner.version, 0);
    }

    #[test]
    fn test_base_data_value_new_empty() {
        let data_value = BaseDataValue::new(TEST_VALUE_EMPTY);
        assert_eq!(data_value.inner.data_type, DataType::None);
        assert_eq!(data_value.inner.user_value, TEST_VALUE_EMPTY);
        assert_eq!(data_value.inner.user_value.len(), 0);
    }

    #[test]
    fn test_base_data_value_new_large() {
        let data_value = BaseDataValue::new(TEST_VALUE_LARGE);
        assert_eq!(data_value.inner.data_type, DataType::None);
        assert_eq!(data_value.inner.user_value, TEST_VALUE_LARGE);
        assert_eq!(data_value.inner.user_value.len(), TEST_VALUE_LARGE.len());
    }

    #[test]
    fn test_base_data_value_encode() {
        let mut data_value = BaseDataValue::new(TEST_VALUE);
        data_value.inner.ctime = TEST_CTIME;

        let encoded = data_value.encode();

        let mut expected = BytesMut::new();
        expected.put_slice(TEST_VALUE);
        expected.put_bytes(0, SUFFIX_RESERVE_LENGTH); // reserve
        expected.put_u64_le(TEST_CTIME);

        assert_eq!(encoded, expected);
    }

    #[test]
    fn test_base_data_value_encode_empty() {
        let mut data_value = BaseDataValue::new(TEST_VALUE_EMPTY);
        data_value.inner.ctime = TEST_CTIME;

        let encoded = data_value.encode();

        let mut expected = BytesMut::new();
        expected.put_slice(TEST_VALUE_EMPTY);
        expected.put_bytes(0, SUFFIX_RESERVE_LENGTH); // reserve
        expected.put_u64_le(TEST_CTIME);

        assert_eq!(encoded, expected);
        assert_eq!(encoded.len(), SUFFIX_RESERVE_LENGTH + TIMESTAMP_LENGTH);
    }

    #[test]
    fn test_base_data_value_encode_large() {
        let mut data_value = BaseDataValue::new(TEST_VALUE_LARGE);
        data_value.inner.ctime = TEST_CTIME;

        let encoded = data_value.encode();

        let mut expected = BytesMut::new();
        expected.put_slice(TEST_VALUE_LARGE);
        expected.put_bytes(0, SUFFIX_RESERVE_LENGTH); // reserve
        expected.put_u64_le(TEST_CTIME);

        assert_eq!(encoded, expected);
        assert_eq!(
            encoded.len(),
            TEST_VALUE_LARGE.len() + SUFFIX_RESERVE_LENGTH + TIMESTAMP_LENGTH
        );
    }

    #[test]
    fn test_base_data_value_roundtrip() {
        let mut data_value = BaseDataValue::new(TEST_VALUE);
        data_value.inner.ctime = TEST_CTIME;
        data_value.inner.data_type = DataType::Hash;

        let encoded = data_value.encode();
        let parsed = ParsedBaseDataValue::new(encoded).unwrap();

        assert_eq!(parsed.inner.data_type, DataType::None);
        assert_eq!(parsed.user_value(), TEST_VALUE);
        assert_eq!(parsed.inner.ctime, TEST_CTIME);
    }

    // ==================== ParsedBaseDataValue Tests ====================

    #[test]
    fn test_parsed_base_data_value_new_success() {
        let buf = build_test_buffer();
        let parsed = ParsedBaseDataValue::new(buf);

        assert!(parsed.is_ok());
        let parsed = parsed.unwrap();
        assert_eq!(parsed.inner.data_type, DataType::None);
        assert_eq!(parsed.user_value(), TEST_VALUE);
        assert_eq!(parsed.inner.ctime, TEST_CTIME);
        assert_eq!(parsed.inner.etime, 0);
        assert_eq!(parsed.inner.version, 0);
    }

    #[test]
    fn test_parsed_base_data_value_new_empty_value() {
        let buf = build_test_buffer_with_value(TEST_VALUE_EMPTY);
        let parsed = ParsedBaseDataValue::new(buf);

        assert!(parsed.is_ok());
        let parsed = parsed.unwrap();
        assert_eq!(parsed.inner.data_type, DataType::None);
        assert_eq!(parsed.user_value(), TEST_VALUE_EMPTY);
        assert_eq!(parsed.user_value().len(), 0);
        assert_eq!(parsed.inner.ctime, TEST_CTIME);
    }

    #[test]
    fn test_parsed_base_data_value_new_large_value() {
        let buf = build_test_buffer_with_value(TEST_VALUE_LARGE);
        let parsed = ParsedBaseDataValue::new(buf);

        assert!(parsed.is_ok());
        let parsed = parsed.unwrap();
        assert_eq!(parsed.inner.data_type, DataType::None);
        assert_eq!(parsed.user_value(), TEST_VALUE_LARGE);
        assert_eq!(parsed.user_value().len(), TEST_VALUE_LARGE.len());
        assert_eq!(parsed.inner.ctime, TEST_CTIME);
    }

    #[test]
    fn test_parsed_base_data_value_new_too_short() {
        let mut buf = BytesMut::new();
        buf.put_slice(&[0u8; SUFFIX_RESERVE_LENGTH + TIMESTAMP_LENGTH - 1]); // Just short of required length

        let parsed = ParsedBaseDataValue::new(buf);
        assert!(parsed.is_err());
    }

    #[test]
    fn test_parsed_base_data_value_new_exact_minimum_length() {
        let mut buf = BytesMut::new();
        buf.put_slice(&[0u8; SUFFIX_RESERVE_LENGTH + TIMESTAMP_LENGTH]); // Exact minimum length

        let parsed = ParsedBaseDataValue::new(buf);
        assert!(parsed.is_ok());
    }

    #[test]
    fn test_parsed_base_data_value_set_ctime() {
        let buf = build_test_buffer();
        let mut parsed = ParsedBaseDataValue::new(buf).unwrap();

        let new_ctime = 1640000000;
        parsed.set_ctime(new_ctime);

        assert_eq!(parsed.inner.ctime, new_ctime);

        // Verify the change is reflected in the underlying bytes
        let suffix_start = parsed.inner.value.len() - TIMESTAMP_LENGTH;
        let stored_ctime =
            (&parsed.inner.value[suffix_start..suffix_start + TIMESTAMP_LENGTH]).get_u64_le();
        assert_eq!(stored_ctime, new_ctime);
    }

    #[test]
    fn test_parsed_base_data_value_set_ctime_zero() {
        let buf = build_test_buffer();
        let mut parsed = ParsedBaseDataValue::new(buf).unwrap();

        parsed.set_ctime(0);

        assert_eq!(parsed.inner.ctime, 0);

        let suffix_start = parsed.inner.value.len() - TIMESTAMP_LENGTH;
        let stored_ctime =
            (&parsed.inner.value[suffix_start..suffix_start + TIMESTAMP_LENGTH]).get_u64_le();
        assert_eq!(stored_ctime, 0);
    }

    #[test]
    fn test_parsed_base_data_value_set_ctime_max() {
        let buf = build_test_buffer();
        let mut parsed = ParsedBaseDataValue::new(buf).unwrap();

        let max_ctime = u64::MAX;
        parsed.set_ctime(max_ctime);

        assert_eq!(parsed.inner.ctime, max_ctime);

        let suffix_start = parsed.inner.value.len() - TIMESTAMP_LENGTH;
        let stored_ctime =
            (&parsed.inner.value[suffix_start..suffix_start + TIMESTAMP_LENGTH]).get_u64_le();
        assert_eq!(stored_ctime, max_ctime);
    }

    #[test]
    fn test_parsed_base_data_value_strip_suffix() {
        let buf = build_test_buffer();
        let mut parsed = ParsedBaseDataValue::new(buf.clone()).unwrap();

        parsed.strip_suffix();

        // After stripping suffix, only the user value should remain
        let expected_len = TEST_VALUE.len();
        assert_eq!(parsed.inner.value.len(), expected_len);

        let mut expected = BytesMut::new();
        expected.put_slice(TEST_VALUE);
        assert_eq!(parsed.inner.value, expected);
    }

    #[test]
    fn test_parsed_base_data_value_strip_suffix_empty() {
        let mut parsed = ParsedBaseDataValue {
            inner: ParsedInternalValue::new(BytesMut::new(), DataType::None, 0..0, 0..0, 0, 0, 0),
        };

        // Should not panic on empty buffer
        parsed.strip_suffix();
        assert!(parsed.inner.value.is_empty());
    }

    #[test]
    fn test_parsed_base_data_value_strip_suffix_short_buffer() {
        let mut buf = BytesMut::new();
        buf.put_slice(&[0u8; SUFFIX_RESERVE_LENGTH + TIMESTAMP_LENGTH - 1]); // Shorter than suffix length

        let mut parsed = ParsedBaseDataValue {
            inner: ParsedInternalValue::new(buf, DataType::None, 0..0, 0..0, 0, 0, 0),
        };

        // Should not panic on short buffer
        parsed.strip_suffix();
        assert_eq!(
            parsed.inner.value.len(),
            SUFFIX_RESERVE_LENGTH + TIMESTAMP_LENGTH - 1
        );
    }

    #[test]
    fn test_parsed_base_data_value_strip_suffix_exact_suffix_length() {
        let mut buf = BytesMut::new();
        buf.put_slice(&[0u8; SUFFIX_RESERVE_LENGTH + TIMESTAMP_LENGTH]); // Exact suffix length

        let mut parsed = ParsedBaseDataValue {
            inner: ParsedInternalValue::new(buf, DataType::None, 0..0, 0..0, 0, 0, 0),
        };

        parsed.strip_suffix();
        assert_eq!(parsed.inner.value.len(), 0);
    }

    #[test]
    fn test_parsed_base_data_value_strip_suffix_large_value() {
        let buf = build_test_buffer_with_value(TEST_VALUE_LARGE);
        let mut parsed = ParsedBaseDataValue::new(buf.clone()).unwrap();

        parsed.strip_suffix();

        let expected_len = TEST_VALUE_LARGE.len();
        assert_eq!(parsed.inner.value.len(), expected_len);

        let mut expected = BytesMut::new();
        expected.put_slice(TEST_VALUE_LARGE);
        assert_eq!(parsed.inner.value, expected);
    }

    // ==================== Edge Cases and Error Handling ====================
    #[test]
    fn test_parsed_base_data_value_multiple_operations() {
        let buf = build_test_buffer();
        let mut parsed = ParsedBaseDataValue::new(buf).unwrap();

        // Set ctime multiple times
        parsed.set_ctime(1000);
        assert_eq!(parsed.inner.ctime, 1000);

        parsed.set_ctime(2000);
        assert_eq!(parsed.inner.ctime, 2000);

        // Strip suffix
        parsed.strip_suffix();
        assert_eq!(parsed.inner.value.len(), TEST_VALUE.len());

        // Verify user value is still correct
        let mut expected = BytesMut::new();
        expected.put_slice(TEST_VALUE);
        assert_eq!(parsed.inner.value, expected);
    }

    #[test]
    fn test_parsed_base_data_value_roundtrip_with_modifications() {
        let buf = build_test_buffer();
        let mut parsed = ParsedBaseDataValue::new(buf).unwrap();

        // Modify ctime
        let new_ctime = 1640000000;
        parsed.set_ctime(new_ctime);

        // Strip suffix
        parsed.strip_suffix();

        // Verify only user value remains
        assert_eq!(parsed.inner.value, TEST_VALUE);
        assert_eq!(parsed.inner.ctime, new_ctime);
    }
}
