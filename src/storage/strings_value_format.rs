// Copyright 2024 The Kiwi-rs Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//  of patent rights can be found in the PATENTS file in the same directory.

use super::base_value_format::DataType;
use super::base_value_format::InternalValue;
use super::base_value_format::ParsedInternalValue;
use super::error::Result;
use super::storage_define::{
    STRING_VALUE_SUFFIXLENGTH, SUFFIX_RESERVE_LENGTH, TIMESTAMP_LENGTH, TYPE_LENGTH,
};
use bytes::Buf;
use bytes::BufMut;
use bytes::Bytes;
use bytes::BytesMut;
use chrono::Utc;
use std::ops::Range;

/*
 * | type | value | reserve | cdate | timestamp |
 * |  1B  |       |   16B   |   8B  |     8B    |
 */

#[derive(Debug, Clone)]
pub struct StringValue {
    user_value: Bytes,
    etime: u64,
    ctime: u64,
}

impl StringValue {
    /// TODO: remove allow dead code
    #[allow(dead_code)]
    pub fn new<T>(user_value: T) -> Self
    where
        T: Into<Bytes>,
    {
        Self {
            user_value: user_value.into(),
            etime: 0,
            ctime: Utc::now().timestamp_micros() as u64,
        }
    }
}

impl InternalValue for StringValue {
    fn encode(&self) -> BytesMut {
        let needed =
            TYPE_LENGTH + self.user_value.len() + SUFFIX_RESERVE_LENGTH + 2 * TIMESTAMP_LENGTH;

        let mut buf = BytesMut::with_capacity(needed);
        buf.put_u8(DataType::String as u8);
        buf.put_slice(&self.user_value);
        buf.put_bytes(0, SUFFIX_RESERVE_LENGTH);
        buf.put_u64_le(self.ctime);
        buf.put_u64_le(self.etime);

        buf
    }

    fn set_etime(&mut self, etime: u64) {
        self.etime = etime;
    }

    fn set_ctime(&mut self, ctime: u64) {
        self.ctime = ctime;
    }

    fn set_relative_timestamp(&mut self, ttl: u64) {
        self.etime = Utc::now().timestamp_micros() as u64 + ttl;
    }
}

pub struct ParsedStringsValue {
    data: Bytes,
    user_value_range: Range<usize>,
    reserve_range: Range<usize>,
    data_type: DataType,
    ctime: u64,
    etime: u64,
}

impl<'a> ParsedInternalValue<'a> for ParsedStringsValue {
    fn new(input_data: &'a [u8]) -> Result<Self> {
        debug_assert!(input_data.len() >= STRING_VALUE_SUFFIXLENGTH);

        let data = Bytes::copy_from_slice(input_data);
        let data_type = DataType::try_from(data[0])?;

        let user_value_len = data.len() - TYPE_LENGTH - STRING_VALUE_SUFFIXLENGTH;
        let user_value_start = TYPE_LENGTH;
        let user_value_end = user_value_start + user_value_len;
        let user_value_range = user_value_start..user_value_end;

        let suffix_start = user_value_end;
        let reserve_start = suffix_start;
        let reserve_end = reserve_start + SUFFIX_RESERVE_LENGTH;
        let reserve_range = reserve_start..reserve_end;

        let mut time_reader = &data[reserve_end..];
        debug_assert!(time_reader.len() >= 2 * TIMESTAMP_LENGTH);
        let ctime = time_reader.get_u64_le();
        let etime = time_reader.get_u64_le();

        Ok(ParsedStringsValue {
            data,
            data_type,
            user_value_range,
            reserve_range,
            ctime,
            etime,
        })
    }

    fn data_type(&self) -> DataType {
        self.data_type
    }

    fn user_value(&self) -> &[u8] {
        &self.data[self.user_value_range.clone()]
    }

    fn ctime(&self) -> u64 {
        self.ctime
    }

    fn etime(&self) -> u64 {
        self.etime
    }

    fn reserve(&self) -> &[u8] {
        &self.data[self.reserve_range.clone()]
    }

    fn filter_decision(&self, current_time: u64) -> rocksdb::CompactionDecision {
        if self.etime != 0 && self.etime < current_time {
            return rocksdb::CompactionDecision::Remove;
        }
        rocksdb::CompactionDecision::Keep
    }
}
