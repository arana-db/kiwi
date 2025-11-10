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

use crate::base_value_format::{DataType, InternalValue, ParsedInternalValue};
use crate::delegate_internal_value;
use crate::delegate_parsed_value;
use crate::error::{InvalidFormatSnafu, Result};
use crate::storage_define::{
    STRING_VALUE_SUFFIXLENGTH, SUFFIX_RESERVE_LENGTH, TIMESTAMP_LENGTH, TYPE_LENGTH,
};

// | type | value | reserve | cdate | timestamp |
// |  1B  |       |   16B   |   8B  |     8B    |
#[derive(Debug, Clone)]
pub struct StringValue {
    inner: InternalValue,
}

delegate_internal_value!(StringValue);
impl StringValue {
    pub fn new<T>(user_value: T) -> Self
    where
        T: Into<Bytes>,
    {
        Self {
            inner: InternalValue::new(DataType::String, user_value),
        }
    }

    pub fn user_value_len(&self) -> usize {
        self.inner.user_value.len()
    }

    pub fn encode(&self) -> BytesMut {
        let needed = TYPE_LENGTH
            + self.inner.user_value.len()
            + SUFFIX_RESERVE_LENGTH
            + 2 * TIMESTAMP_LENGTH;
        let mut buf = BytesMut::with_capacity(needed);

        buf.put_u8(DataType::String as u8);
        buf.put_slice(&self.inner.user_value);
        buf.put_bytes(0, SUFFIX_RESERVE_LENGTH);
        buf.put_u64_le(self.inner.ctime);
        buf.put_u64_le(self.inner.etime);

        buf
    }
}

#[allow(dead_code)]
pub struct ParsedStringsValue {
    inner: ParsedInternalValue,
}

delegate_parsed_value!(ParsedStringsValue);
impl ParsedStringsValue {
    pub fn new<T>(internal_value: T) -> Result<Self>
    where
        T: Into<BytesMut>,
    {
        let value: BytesMut = internal_value.into();
        ensure!(
            value.len() >= STRING_VALUE_SUFFIXLENGTH,
            InvalidFormatSnafu {
                message: format!(
                    "invalid string value length: {} < {STRING_VALUE_SUFFIXLENGTH}",
                    value.len()
                )
            }
        );

        let data_type = DataType::try_from(value[0])?;

        let user_value_len = value.len() - TYPE_LENGTH - STRING_VALUE_SUFFIXLENGTH;
        let user_value_start = TYPE_LENGTH;
        let user_value_end = user_value_start + user_value_len;
        let user_value_range = user_value_start..user_value_end;

        let suffix_start = user_value_end;
        let reserve_start = suffix_start;
        let reserve_end = reserve_start + SUFFIX_RESERVE_LENGTH;
        let reserve_range = reserve_start..reserve_end;

        let mut time_reader = &value[reserve_end..];
        debug_assert!(time_reader.len() >= 2 * TIMESTAMP_LENGTH);
        ensure!(
            time_reader.len() >= 2 * TIMESTAMP_LENGTH,
            InvalidFormatSnafu {
                message: format!(
                    "invalid string value length: {} < {}",
                    time_reader.len(),
                    2 * TIMESTAMP_LENGTH,
                )
            }
        );
        let ctime = time_reader.get_u64_le();
        let etime = time_reader.get_u64_le();

        Ok(Self {
            inner: ParsedInternalValue::new(
                value,
                data_type,
                user_value_range,
                reserve_range,
                0,
                ctime,
                etime,
            ),
        })
    }

    #[allow(dead_code)]
    pub fn strip_suffix(&mut self) {
        self.inner.value.advance(TYPE_LENGTH);

        let len = self.inner.value.len();
        if len >= STRING_VALUE_SUFFIXLENGTH {
            self.inner.value.truncate(len - STRING_VALUE_SUFFIXLENGTH);
        }
    }

    #[allow(dead_code)]
    pub fn set_ctime(&mut self, ctime: u64) {
        self.inner.ctime = ctime;
        self.set_ctime_to_value();
    }

    #[allow(dead_code)]
    pub fn set_etime(&mut self, etime: u64) {
        self.inner.etime = etime;
        self.set_etime_to_value();
    }

    #[allow(dead_code)]
    fn set_ctime_to_value(&mut self) {
        let suffix_start =
            self.inner.value.len() - STRING_VALUE_SUFFIXLENGTH + SUFFIX_RESERVE_LENGTH;

        let ctime_bytes = self.inner.ctime.to_le_bytes();
        let dst = &mut self.inner.value[suffix_start..suffix_start + TIMESTAMP_LENGTH];
        dst.copy_from_slice(&ctime_bytes);
    }

    #[allow(dead_code)]
    fn set_etime_to_value(&mut self) {
        let suffix_start = self.inner.value.len() - STRING_VALUE_SUFFIXLENGTH
            + SUFFIX_RESERVE_LENGTH
            + TIMESTAMP_LENGTH;

        let bytes = self.inner.etime.to_le_bytes();
        let dst = &mut self.inner.value[suffix_start..suffix_start + TIMESTAMP_LENGTH];
        dst.copy_from_slice(&bytes);
    }
}

#[cfg(test)]
mod tests_string_value {
    use super::*;

    const TEST_CTIME: u64 = 1620000000;
    const TEST_ETIME: u64 = 1630000000;
    const TEST_VALUE: &[u8] = b"kiwi-rs";

    fn create_test_string_value() -> StringValue {
        let mut string_value = StringValue::new(TEST_VALUE);
        string_value.set_ctime(TEST_CTIME);
        string_value.set_etime(TEST_ETIME);
        string_value
    }

    #[test]
    fn test_string_value_new() {
        let string_value = create_test_string_value();
        assert_eq!(string_value.inner.data_type, DataType::String);
        assert_eq!(string_value.inner.ctime, TEST_CTIME);
        assert_eq!(string_value.inner.etime, TEST_ETIME);
        let buf = string_value.inner.user_value;
        assert_eq!(buf, TEST_VALUE);
    }

    #[test]
    fn test_string_value_encode() {
        let string_value = create_test_string_value();
        let encoded = string_value.encode();
        let mut expected = BytesMut::new();
        expected.put_u8(DataType::String as u8);
        expected.put_slice(TEST_VALUE);
        expected.put_bytes(0, SUFFIX_RESERVE_LENGTH); // reserve
        expected.put_u64_le(TEST_CTIME);
        expected.put_u64_le(TEST_ETIME);
        assert_eq!(encoded, expected);
    }

    #[test]
    fn test_string_value_roundtrip_with_parsed() {
        let string_value = create_test_string_value();
        let encoded = string_value.encode();
        let parsed = ParsedStringsValue::new(encoded).unwrap();
        assert_eq!(parsed.inner.data_type, DataType::String);
        assert_eq!(parsed.user_value(), TEST_VALUE);
        assert_eq!(parsed.inner.ctime, TEST_CTIME);
        assert_eq!(parsed.inner.etime, TEST_ETIME);
    }
}

#[allow(dead_code)]
mod tests_parsed_string_value {
    use super::*;

    const TEST_CTIME: u64 = 1620000000;
    const TEST_ETIME: u64 = 1630000000;
    const TEST_VALUE: &[u8] = b"kiwi-rs";

    fn build_test_buffer() -> BytesMut {
        let mut buf = BytesMut::new();
        buf.put_u8(DataType::String as u8);
        buf.put_slice(TEST_VALUE);
        buf.put_bytes(0, SUFFIX_RESERVE_LENGTH); // reserve
        buf.put_u64_le(TEST_CTIME);
        buf.put_u64_le(TEST_ETIME);
        buf
    }

    #[test]
    fn test_parsed_string_value_parse() {
        let buf = build_test_buffer();
        let parsed = ParsedStringsValue::new(buf);
        assert!(parsed.is_ok());
        let parsed = parsed.unwrap();
        assert_eq!(parsed.inner.data_type, DataType::String);
        assert_eq!(parsed.user_value(), TEST_VALUE);
        assert_eq!(parsed.inner.ctime, TEST_CTIME);
        assert_eq!(parsed.inner.etime, TEST_ETIME);
    }

    #[test]
    fn test_parsed_string_value_parse_invalid_length() {
        let mut buf = BytesMut::new();
        buf.put_u8(DataType::String as u8);
        buf.put_slice(TEST_VALUE);
        let parsed = ParsedStringsValue::new(buf);
        assert!(parsed.is_err());
    }

    #[test]
    fn test_parsed_string_value_set_ctime_to_value() {
        let buf = build_test_buffer();
        let mut parsed = ParsedStringsValue::new(buf).unwrap();

        let new_ctime = 1640000000;
        parsed.set_ctime(new_ctime);
        assert_eq!(new_ctime, parsed.ctime());

        let suffix_start = parsed.inner.value.len() - 2 * TIMESTAMP_LENGTH;
        let stored_ctime =
            (&parsed.inner.value[suffix_start..suffix_start + TIMESTAMP_LENGTH]).get_u64_le();
        assert_eq!(stored_ctime, new_ctime);
    }

    #[test]
    fn test_parsed_string_value_set_etime_to_value() {
        let buf = build_test_buffer();
        let mut parsed = ParsedStringsValue::new(buf).unwrap();

        let new_etime = 1650000000;
        parsed.set_etime(new_etime);
        assert_eq!(new_etime, parsed.etime());

        let suffix_start = parsed.inner.value.len() - TIMESTAMP_LENGTH;
        let stored_ctime =
            (&parsed.inner.value[suffix_start..suffix_start + TIMESTAMP_LENGTH]).get_u64_le();
        assert_eq!(stored_ctime, new_etime);
    }

    #[test]
    fn test_parsed_string_value_strip_suffix() {
        let buf = build_test_buffer();
        let mut parsed = ParsedStringsValue::new(buf.clone()).unwrap();

        parsed.strip_suffix();
        let expected_len = buf.len() - TYPE_LENGTH - SUFFIX_RESERVE_LENGTH - 2 * TIMESTAMP_LENGTH;
        assert_eq!(parsed.inner.value.len(), expected_len);

        let mut expected = BytesMut::new();
        expected.put_slice(&buf[TYPE_LENGTH..buf.len() - STRING_VALUE_SUFFIXLENGTH]);
        assert_eq!(parsed.inner.value, expected);
    }
}
