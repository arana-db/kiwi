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

use crate::{
    delegate_internal_value, delegate_parsed_value,
    storage::{
        base_value_format::{DataType, InternalValue, ParsedInternalValue},
        error::{InvalidFormatSnafu, Result},
        storage_define::{
            STRING_VALUE_SUFFIXLENGTH, SUFFIX_RESERVE_LENGTH, TIMESTAMP_LENGTH, TYPE_LENGTH,
        },
    },
};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use rocksdb::CompactionDecision;
use snafu::ensure;

/*
 * | type | value | reserve | cdate | timestamp |
 * |  1B  |       |   16B   |   8B  |     8B    |
 */
#[derive(Debug, Clone)]
pub struct StringValue {
    inner: InternalValue,
}

delegate_internal_value!(StringValue);
#[allow(dead_code)]
impl StringValue {
    pub fn new<T>(user_value: T) -> Self
    where
        T: Into<Bytes>,
    {
        Self {
            inner: InternalValue::new(DataType::String, user_value),
        }
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
#[allow(dead_code)]
impl ParsedStringsValue {
    pub fn new<T>(internal_value: T) -> Result<Self>
    where
        T: Into<BytesMut>,
    {
        let value: BytesMut = internal_value.into();
        debug_assert!(value.len() >= STRING_VALUE_SUFFIXLENGTH);
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

    pub fn strip_suffix(&mut self) {
        self.inner.value.advance(TYPE_LENGTH);

        let len = self.inner.value.len();
        if len >= STRING_VALUE_SUFFIXLENGTH {
            self.inner.value.truncate(len - STRING_VALUE_SUFFIXLENGTH);
        }
    }

    pub fn set_ctime_to_value(&mut self) {
        let suffix_start =
            self.inner.value.len() - STRING_VALUE_SUFFIXLENGTH + SUFFIX_RESERVE_LENGTH;

        let ctime_bytes = self.inner.ctime.to_le_bytes();
        let dst = &mut self.inner.value[suffix_start..suffix_start + TIMESTAMP_LENGTH];
        dst.copy_from_slice(&ctime_bytes);
    }

    pub fn set_etime_to_value(&mut self) {
        let suffix_start = self.inner.value.len() - STRING_VALUE_SUFFIXLENGTH
            + SUFFIX_RESERVE_LENGTH
            + TIMESTAMP_LENGTH;

        let bytes = self.inner.etime.to_le_bytes();
        let dst = &mut self.inner.value[suffix_start..suffix_start + TIMESTAMP_LENGTH];
        dst.copy_from_slice(&bytes);
    }

    pub fn filter_decision(&self, cur_time: u64) -> CompactionDecision {
        if self.inner.etime != 0 && self.inner.etime < cur_time {
            CompactionDecision::Remove
        } else {
            CompactionDecision::Keep
        }
    }
}
