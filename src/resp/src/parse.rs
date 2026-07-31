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

use std::collections::VecDeque;
use std::str;

use bytes::{Buf, Bytes, BytesMut};
use nom::Parser;
use nom::{
    IResult,
    bytes::streaming::take,
    character::streaming::{char, digit1, line_ending, not_line_ending},
    combinator::{map_res, opt, recognize},
    sequence::terminated,
};

use crate::{
    command::{Command, RespCommand},
    error::{RespError, RespResult},
    types::{RespData, RespVersion},
};

#[derive(Debug, PartialEq)]
pub enum RespParseResult {
    Complete(RespData),
    Incomplete,
    Error(RespError),
}

pub trait Parse {
    fn parse(&mut self, data: Bytes) -> RespParseResult;

    fn next_command(&mut self) -> Option<RespResult<RespCommand>>;

    fn reset(&mut self);
}

pub struct RespParse {
    version: RespVersion,
    buffer: BytesMut,
    commands: VecDeque<RespResult<RespCommand>>,
    is_pipeline: bool,
    version_detected: bool,
}

impl Default for RespParse {
    fn default() -> Self {
        Self::new(RespVersion::default())
    }
}

impl RespParse {
    const MAX_AGGREGATE_LENGTH: i64 = i32::MAX as i64;
    const MAX_AGGREGATE_NESTING_DEPTH: usize = 128;

    pub fn new(version: RespVersion) -> Self {
        Self {
            version,
            buffer: BytesMut::new(),
            commands: VecDeque::new(),
            is_pipeline: false,
            version_detected: false,
        }
    }

    pub fn version(&self) -> RespVersion {
        self.version
    }

    pub fn set_version(&mut self, version: RespVersion) {
        self.version = version;
    }

    fn validate_aggregate_length(
        input: &[u8],
        len: i64,
    ) -> Result<(), nom::Err<nom::error::Error<&[u8]>>> {
        if len > Self::MAX_AGGREGATE_LENGTH {
            return Err(nom::Err::Failure(nom::error::Error::new(
                input,
                nom::error::ErrorKind::Verify,
            )));
        }

        Ok(())
    }

    fn nested_aggregate_depth(
        input: &[u8],
        depth: usize,
    ) -> Result<usize, nom::Err<nom::error::Error<&[u8]>>> {
        if depth >= Self::MAX_AGGREGATE_NESTING_DEPTH {
            return Err(nom::Err::Failure(nom::error::Error::new(
                input,
                nom::error::ErrorKind::Verify,
            )));
        }

        Ok(depth + 1)
    }

    /// Detect protocol version from the first byte of input
    pub fn detect_version(input: &[u8]) -> RespVersion {
        if input.is_empty() {
            return RespVersion::RESP2;
        }

        // RESP3 specific prefixes that don't exist in RESP2
        match input[0] {
            b'_' | b'#' | b',' | b'(' | b'!' | b'=' | b'%' | b'~' | b'>' => RespVersion::RESP3,
            _ => RespVersion::RESP2,
        }
    }

    /// Auto-detect and switch protocol version based on input
    pub fn auto_detect_version(&mut self, data: &[u8]) {
        let detected_version = Self::detect_version(data);
        if detected_version != self.version {
            self.set_version(detected_version);
        }
        self.version_detected = true;
    }

    fn parse_inline(input: &[u8]) -> IResult<&[u8], RespData> {
        let (input, line) = terminated(not_line_ending, line_ending).parse(input)?;

        let parts = line
            .split(|byte| byte.is_ascii_whitespace())
            .filter(|part| !part.is_empty())
            .map(Bytes::copy_from_slice)
            .collect::<Vec<_>>();

        if parts.is_empty() {
            return Err(nom::Err::Error(nom::error::Error::new(
                input,
                nom::error::ErrorKind::Verify,
            )));
        }

        Ok((
            input,
            RespData::Array(Some(
                parts
                    .into_iter()
                    .map(|part| RespData::BulkString(Some(part)))
                    .collect(),
            )),
        ))
    }

    fn skip_empty_lines(&mut self) {
        loop {
            if self.buffer.starts_with(b"\r\n") {
                self.buffer.advance(2);
            } else if self.buffer.starts_with(b"\n") {
                self.buffer.advance(1);
            } else {
                break;
            }
        }
    }

    fn parse_simple_string(input: &[u8]) -> IResult<&[u8], RespData> {
        let (input, _) = char('+')(input)?;
        let mut ter_parser = terminated(not_line_ending, line_ending);
        let (input, data) = ter_parser.parse(input)?;
        Ok((input, RespData::SimpleString(Bytes::copy_from_slice(data))))
    }

    fn parse_error(input: &[u8]) -> IResult<&[u8], RespData> {
        let (input, _) = char('-')(input)?;
        let mut ter_parser = terminated(not_line_ending, line_ending);
        let (input, data) = ter_parser.parse(input)?;
        Ok((input, RespData::Error(Bytes::copy_from_slice(data))))
    }

    fn parse_integer(input: &[u8]) -> IResult<&[u8], RespData> {
        let (input, _) = char(':')(input)?;
        let mut map_parser = map_res(
            terminated(recognize((opt(char('-')), digit1)), line_ending),
            |s: &[u8]| {
                str::from_utf8(s)
                    .map_err(|_| ())
                    .and_then(|s| s.parse::<i64>().map_err(|_| ()))
            },
        );
        let (input, num) = map_parser.parse(input)?;
        Ok((input, RespData::Integer(num)))
    }

    fn parse_bulk_string(input: &[u8]) -> IResult<&[u8], RespData> {
        let (input, _) = char('$')(input)?;
        let mut map_parser = map_res(
            terminated(recognize((opt(char('-')), digit1)), line_ending),
            |s: &[u8]| {
                str::from_utf8(s)
                    .map_err(|_| ())
                    .and_then(|s| s.parse::<i64>().map_err(|_| ()))
            },
        );
        let (input, len) = map_parser.parse(input)?;

        if len < 0 {
            return Ok((input, RespData::BulkString(None)));
        }

        let mut ter_parser = terminated(take(len as usize), line_ending);
        let (input, data) = ter_parser.parse(input)?;
        Ok((
            input,
            RespData::BulkString(Some(Bytes::copy_from_slice(data))),
        ))
    }

    fn parse_array(input: &[u8], depth: usize) -> IResult<&[u8], RespData> {
        let (input, _) = char('*')(input)?;
        let mut mut_parser = map_res(
            terminated(recognize((opt(char('-')), digit1)), line_ending),
            |s: &[u8]| {
                str::from_utf8(s)
                    .map_err(|_| ())
                    .and_then(|s| s.parse::<i64>().map_err(|_| ()))
            },
        );
        let (input, len) = mut_parser.parse(input)?;

        if len < 0 {
            return Ok((input, RespData::Array(None)));
        }

        Self::validate_aggregate_length(input, len)?;
        let mut remaining = input;
        let mut elements = Vec::new();

        for _ in 0..len {
            let (new_remaining, element) = Self::parse_resp_data(remaining, depth)?;
            elements.push(element);
            remaining = new_remaining;
        }

        Ok((remaining, RespData::Array(Some(elements))))
    }

    // RESP3 parsing functions
    fn parse_null(input: &[u8]) -> IResult<&[u8], RespData> {
        let (input, _) = char('_')(input)?;
        let (input, _) = line_ending(input)?;
        Ok((input, RespData::Null))
    }

    fn parse_boolean(input: &[u8]) -> IResult<&[u8], RespData> {
        let (input, _) = char('#')(input)?;
        let mut map_parser = map_res(
            terminated(recognize(char('t').or(char('f'))), line_ending),
            |s: &[u8]| match s {
                b"t" => Ok(true),
                b"f" => Ok(false),
                _ => Err(()),
            },
        );
        let (input, value) = map_parser.parse(input)?;
        Ok((input, RespData::Boolean(value)))
    }

    fn parse_double(input: &[u8]) -> IResult<&[u8], RespData> {
        let (input, _) = char(',')(input)?;
        let (input, raw) = terminated(not_line_ending, line_ending).parse(input)?;
        let s = str::from_utf8(raw).map_err(|_| {
            nom::Err::Error(nom::error::Error::new(raw, nom::error::ErrorKind::MapRes))
        })?;
        let sl = s.to_ascii_lowercase();
        let value = match sl.as_str() {
            "inf" => f64::INFINITY,
            "-inf" => f64::NEG_INFINITY,
            "nan" => f64::NAN,
            _ => s.parse::<f64>().map_err(|_| {
                nom::Err::Error(nom::error::Error::new(raw, nom::error::ErrorKind::MapRes))
            })?,
        };
        Ok((input, RespData::Double(value)))
    }

    fn parse_big_number(input: &[u8]) -> IResult<&[u8], RespData> {
        let (input, _) = char('(')(input)?;
        let mut ter_parser = terminated(not_line_ending, line_ending);
        let (input, data) = ter_parser.parse(input)?;
        Ok((input, RespData::BigNumber(Bytes::copy_from_slice(data))))
    }

    fn parse_bulk_error(input: &[u8]) -> IResult<&[u8], RespData> {
        let (input, _) = char('!')(input)?;
        let mut map_parser = map_res(
            terminated(recognize((opt(char('-')), digit1)), line_ending),
            |s: &[u8]| {
                str::from_utf8(s)
                    .map_err(|_| ())
                    .and_then(|s| s.parse::<i64>().map_err(|_| ()))
            },
        );
        let (input, len) = map_parser.parse(input)?;

        if len < 0 {
            return Ok((input, RespData::BulkError(Bytes::new())));
        }

        let mut ter_parser = terminated(take(len as usize), line_ending);
        let (input, data) = ter_parser.parse(input)?;
        Ok((input, RespData::BulkError(Bytes::copy_from_slice(data))))
    }

    fn parse_verbatim_string(input: &[u8]) -> IResult<&[u8], RespData> {
        let (input, _) = char('=')(input)?;
        let mut map_parser = map_res(terminated(recognize(digit1), line_ending), |s: &[u8]| {
            str::from_utf8(s)
                .map_err(|_| ())
                .and_then(|s| s.parse::<i64>().map_err(|_| ()))
        });
        let (input, len) = map_parser.parse(input)?;

        if len < 4 {
            return Err(nom::Err::Error(nom::error::Error::new(
                input,
                nom::error::ErrorKind::Verify,
            )));
        }

        let mut ter_parser = terminated(take(len as usize), line_ending);
        let (input, data) = ter_parser.parse(input)?;

        if data.len() < 4 {
            return Err(nom::Err::Error(nom::error::Error::new(
                input,
                nom::error::ErrorKind::Verify,
            )));
        }

        // Validate that byte 3 is the ':' separator (format is "fmt:data")
        if data[3] != b':' {
            return Err(nom::Err::Error(nom::error::Error::new(
                input,
                nom::error::ErrorKind::Verify,
            )));
        }

        let format = Bytes::copy_from_slice(&data[0..3]);
        let content = Bytes::copy_from_slice(&data[4..]);

        Ok((
            input,
            RespData::VerbatimString {
                format,
                data: content,
            },
        ))
    }

    fn parse_map(input: &[u8], depth: usize) -> IResult<&[u8], RespData> {
        let (input, _) = char('%')(input)?;
        let mut map_parser = map_res(
            terminated(recognize((opt(char('-')), digit1)), line_ending),
            |s: &[u8]| {
                str::from_utf8(s)
                    .map_err(|_| ())
                    .and_then(|s| s.parse::<i64>().map_err(|_| ()))
            },
        );
        let (input, len) = map_parser.parse(input)?;

        if len < 0 {
            return Ok((input, RespData::Map(vec![])));
        }

        Self::validate_aggregate_length(input, len)?;
        let mut remaining = input;
        let mut pairs = Vec::new();

        for _ in 0..len {
            let (new_remaining, key) = Self::parse_resp_data(remaining, depth)?;
            let (new_remaining, value) = Self::parse_resp_data(new_remaining, depth)?;
            pairs.push((key, value));
            remaining = new_remaining;
        }

        Ok((remaining, RespData::Map(pairs)))
    }

    fn parse_set(input: &[u8], depth: usize) -> IResult<&[u8], RespData> {
        let (input, _) = char('~')(input)?;
        let mut map_parser = map_res(
            terminated(recognize((opt(char('-')), digit1)), line_ending),
            |s: &[u8]| {
                str::from_utf8(s)
                    .map_err(|_| ())
                    .and_then(|s| s.parse::<i64>().map_err(|_| ()))
            },
        );
        let (input, len) = map_parser.parse(input)?;

        if len < 0 {
            return Ok((input, RespData::Set(vec![])));
        }

        Self::validate_aggregate_length(input, len)?;
        let mut remaining = input;
        let mut elements = Vec::new();

        for _ in 0..len {
            let (new_remaining, element) = Self::parse_resp_data(remaining, depth)?;
            elements.push(element);
            remaining = new_remaining;
        }

        Ok((remaining, RespData::Set(elements)))
    }

    fn parse_push(input: &[u8], depth: usize) -> IResult<&[u8], RespData> {
        let (input, _) = char('>')(input)?;
        let mut map_parser = map_res(
            terminated(recognize((opt(char('-')), digit1)), line_ending),
            |s: &[u8]| {
                str::from_utf8(s)
                    .map_err(|_| ())
                    .and_then(|s| s.parse::<i64>().map_err(|_| ()))
            },
        );
        let (input, len) = map_parser.parse(input)?;

        if len < 0 {
            return Ok((input, RespData::Push(vec![])));
        }

        Self::validate_aggregate_length(input, len)?;
        let mut remaining = input;
        let mut elements = Vec::new();

        for _ in 0..len {
            let (new_remaining, element) = Self::parse_resp_data(remaining, depth)?;
            elements.push(element);
            remaining = new_remaining;
        }

        Ok((remaining, RespData::Push(elements)))
    }

    fn parse_resp_data(input: &[u8], aggregate_depth: usize) -> IResult<&[u8], RespData> {
        if input.is_empty() {
            return Err(nom::Err::Incomplete(nom::Needed::Unknown));
        }

        match input[0] {
            b'+' => Self::parse_simple_string(input),
            b'-' => Self::parse_error(input),
            b':' => Self::parse_integer(input),
            b'$' => Self::parse_bulk_string(input),
            b'*' => Self::parse_array(input, Self::nested_aggregate_depth(input, aggregate_depth)?),
            // RESP3 types
            b'_' => Self::parse_null(input),
            b'#' => Self::parse_boolean(input),
            b',' => Self::parse_double(input),
            b'(' => Self::parse_big_number(input),
            b'!' => Self::parse_bulk_error(input),
            b'=' => Self::parse_verbatim_string(input),
            b'%' => Self::parse_map(input, Self::nested_aggregate_depth(input, aggregate_depth)?),
            b'~' => Self::parse_set(input, Self::nested_aggregate_depth(input, aggregate_depth)?),
            b'>' => Self::parse_push(input, Self::nested_aggregate_depth(input, aggregate_depth)?),
            _ => Self::parse_inline(input),
        }
    }

    fn process_buffer(&mut self) -> RespParseResult {
        self.skip_empty_lines();

        if self.buffer.is_empty() {
            return RespParseResult::Incomplete;
        }

        match Self::parse_resp_data(&self.buffer, 0) {
            Ok((remaining, resp_data)) => {
                let consumed = self.buffer.len() - remaining.len();
                self.buffer.advance(consumed);

                match resp_data.to_command() {
                    Ok(mut command) => {
                        command.is_pipeline = self.is_pipeline;
                        self.is_pipeline = !self.buffer.is_empty();

                        self.commands.push_back(Ok(command));
                    }
                    Err(err) => {
                        self.commands.push_back(Err(err));
                    }
                }

                RespParseResult::Complete(resp_data)
            }
            Err(nom::Err::Incomplete(_)) => RespParseResult::Incomplete,
            Err(nom::Err::Error(e)) | Err(nom::Err::Failure(e)) => {
                let error_msg = format!("Parse error: {e:?}");
                RespParseResult::Error(RespError::ParseError(error_msg))
            }
        }
    }
}

impl Parse for RespParse {
    fn parse(&mut self, data: Bytes) -> RespParseResult {
        // Auto-detect protocol version on first data received (handles partial initial data)
        if !self.version_detected && !data.is_empty() {
            self.auto_detect_version(&data);
        }

        self.buffer.extend_from_slice(&data);

        self.process_buffer()
    }

    fn next_command(&mut self) -> Option<RespResult<RespCommand>> {
        self.commands.pop_front()
    }

    fn reset(&mut self) {
        self.buffer.clear();
        self.commands.clear();
        self.is_pipeline = false;
        self.version_detected = false;
    }
}

impl Drop for RespParse {
    fn drop(&mut self) {
        self.reset();
    }
}

#[allow(clippy::unwrap_used)]
#[cfg(test)]
mod tests {
    use std::alloc::{GlobalAlloc, Layout, System};
    use std::cell::Cell;

    use crate::command::CommandType;

    use super::Bytes;
    use super::{Parse, RespData, RespParse, RespParseResult, RespVersion};

    struct MeasuringAllocator;

    // Measurements are thread-local so unrelated parallel tests do not affect the totals.
    thread_local! {
        static MEASURE_ALLOCATIONS: Cell<bool> = const { Cell::new(false) };
        static ALLOCATED_BYTES: Cell<usize> = const { Cell::new(0) };
    }

    fn record_allocation(size: usize) {
        if MEASURE_ALLOCATIONS.try_with(Cell::get).unwrap_or(false) {
            let _ = ALLOCATED_BYTES.try_with(|total| {
                total.set(total.get().saturating_add(size));
            });
        }
    }

    // SAFETY: Every allocation operation is forwarded to System with the original pointer and
    // layout; the extra bookkeeping only updates non-allocating thread-local Cell values.
    unsafe impl GlobalAlloc for MeasuringAllocator {
        unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
            record_allocation(layout.size());
            // SAFETY: The caller provides the layout required by GlobalAlloc.
            unsafe { System.alloc(layout) }
        }

        unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
            record_allocation(layout.size());
            // SAFETY: The caller provides the layout required by GlobalAlloc.
            unsafe { System.alloc_zeroed(layout) }
        }

        unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
            // SAFETY: The pointer and layout are forwarded unchanged from the caller.
            unsafe { System.dealloc(ptr, layout) }
        }

        unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
            record_allocation(new_size);
            // SAFETY: The pointer, layout, and requested size are forwarded unchanged.
            unsafe { System.realloc(ptr, layout, new_size) }
        }
    }

    #[global_allocator]
    static TEST_ALLOCATOR: MeasuringAllocator = MeasuringAllocator;

    struct AllocationMeasurement;

    impl AllocationMeasurement {
        fn start() -> Self {
            ALLOCATED_BYTES.with(|total| total.set(0));
            MEASURE_ALLOCATIONS.with(|enabled| enabled.set(true));
            Self
        }

        fn finish(self) -> usize {
            MEASURE_ALLOCATIONS.with(|enabled| enabled.set(false));
            ALLOCATED_BYTES.with(Cell::get)
        }
    }

    impl Drop for AllocationMeasurement {
        fn drop(&mut self) {
            let _ = MEASURE_ALLOCATIONS.try_with(|enabled| enabled.set(false));
        }
    }

    #[test]
    fn test_parse_simple_string_ok() {
        let mut parser = RespParse::new(RespVersion::RESP2);
        let res = parser.parse(Bytes::from("+OK\r\n"));
        assert_eq!(
            res,
            RespParseResult::Complete(RespData::SimpleString(Bytes::from("OK")))
        );
    }

    #[test]
    fn test_parse_error() {
        let mut parser = RespParse::new(RespVersion::RESP2);
        let res = parser.parse(Bytes::from("-Error message\r\n"));
        assert_eq!(
            res,
            RespParseResult::Complete(RespData::Error(Bytes::from("Error message")))
        );
    }

    #[test]
    fn test_parse_integer() {
        let mut parser = RespParse::new(RespVersion::RESP2);
        let res = parser.parse(Bytes::from(":1000\r\n"));
        assert_eq!(res, RespParseResult::Complete(RespData::Integer(1000)));
    }

    #[test]
    fn test_parse_inline() {
        let mut parser = RespParse::new(RespVersion::RESP2);
        let res = parser.parse(Bytes::from("ping\r\n"));
        assert_eq!(
            res,
            RespParseResult::Complete(RespData::Array(Some(vec![RespData::BulkString(Some(
                Bytes::from("ping"),
            ))])))
        );
        parser.reset();

        let res = parser.parse(Bytes::from("PING\r\n\r\n"));
        assert_eq!(
            res,
            RespParseResult::Complete(RespData::Array(Some(vec![RespData::BulkString(Some(
                Bytes::from("PING"),
            ))])))
        );
        parser.reset();

        let res = parser.parse(Bytes::from("PING\n"));
        assert_eq!(
            res,
            RespParseResult::Complete(RespData::Array(Some(vec![RespData::BulkString(Some(
                Bytes::from("PING"),
            ))])))
        );
        parser.reset();

        let res = parser.parse(Bytes::from("PING\n\n"));
        assert_eq!(
            res,
            RespParseResult::Complete(RespData::Array(Some(vec![RespData::BulkString(Some(
                Bytes::from("PING"),
            ))])))
        );
        parser.reset();

        let res = parser.parse(Bytes::from("PING\r\n\n"));
        assert_eq!(
            res,
            RespParseResult::Complete(RespData::Array(Some(vec![RespData::BulkString(Some(
                Bytes::from("PING"),
            ))])))
        );
        parser.reset();
    }

    #[test]
    fn test_parse_inline_info_command() {
        let mut parser = RespParse::new(RespVersion::RESP2);
        let res = parser.parse(Bytes::from("info\r\n"));
        assert_eq!(
            res,
            RespParseResult::Complete(RespData::Array(Some(vec![RespData::BulkString(Some(
                Bytes::from("info"),
            ))])))
        );

        let command = parser.next_command().unwrap().unwrap();
        assert_eq!(command.command_type, CommandType::Info);
        assert!(command.args.is_empty());
    }

    #[test]
    fn test_parse_inline_command_with_args() {
        let mut parser = RespParse::new(RespVersion::RESP2);
        let res = parser.parse(Bytes::from("ping hello\r\n"));
        assert_eq!(
            res,
            RespParseResult::Complete(RespData::Array(Some(vec![
                RespData::BulkString(Some(Bytes::from("ping"))),
                RespData::BulkString(Some(Bytes::from("hello"))),
            ])))
        );

        let command = parser.next_command().unwrap().unwrap();
        assert_eq!(command.command_type, CommandType::Ping);
        assert_eq!(command.arg_count(), 1);
        assert_eq!(command.arg(0), Some(&Bytes::from("hello")));
    }

    #[test]
    fn test_parse_inline_with_surrounding_whitespace() {
        let mut parser = RespParse::new(RespVersion::RESP2);
        let res = parser.parse(Bytes::from(" \tinfo \t\r\n"));
        assert_eq!(
            res,
            RespParseResult::Complete(RespData::Array(Some(vec![RespData::BulkString(Some(
                Bytes::from("info"),
            ))])))
        );

        let command = parser.next_command().unwrap().unwrap();
        assert_eq!(command.command_type, CommandType::Info);
    }

    #[test]
    fn test_parse_inline_params() {
        let mut parser = RespParse::new(RespVersion::RESP2);
        let res = parser.parse(Bytes::from("hmget fruit apple banana watermelon\r\n"));
        assert_eq!(
            res,
            RespParseResult::Complete(RespData::Array(Some(vec![
                RespData::BulkString(Some(Bytes::from("hmget"))),
                RespData::BulkString(Some(Bytes::from("fruit"))),
                RespData::BulkString(Some(Bytes::from("apple"))),
                RespData::BulkString(Some(Bytes::from("banana"))),
                RespData::BulkString(Some(Bytes::from("watermelon"))),
            ])))
        );
    }

    #[test]
    fn test_parse_multiple_inline() {
        let mut parser = RespParse::new(RespVersion::RESP2);

        let res = parser.parse(Bytes::from(
            "ping\r\nhmget fruit apple banana watermelon\r\n",
        ));
        assert_eq!(
            res,
            RespParseResult::Complete(RespData::Array(Some(vec![RespData::BulkString(Some(
                Bytes::from("ping"),
            ))])))
        );

        let res = parser.parse(Bytes::new());
        assert_eq!(
            res,
            RespParseResult::Complete(RespData::Array(Some(vec![
                RespData::BulkString(Some(Bytes::from("hmget"))),
                RespData::BulkString(Some(Bytes::from("fruit"))),
                RespData::BulkString(Some(Bytes::from("apple"))),
                RespData::BulkString(Some(Bytes::from("banana"))),
                RespData::BulkString(Some(Bytes::from("watermelon"))),
            ])))
        );
    }

    #[test]
    fn test_parse_multiple_inline_with_blank_lines() {
        let mut parser = RespParse::new(RespVersion::RESP2);

        let res = parser.parse(Bytes::from("ping\r\n\r\ninfo\r\n"));
        assert_eq!(
            res,
            RespParseResult::Complete(RespData::Array(Some(vec![RespData::BulkString(Some(
                Bytes::from("ping"),
            ))])))
        );

        let res = parser.parse(Bytes::new());
        assert_eq!(
            res,
            RespParseResult::Complete(RespData::Array(Some(vec![RespData::BulkString(Some(
                Bytes::from("info"),
            ))])))
        );

        let command = parser.next_command().unwrap().unwrap();
        assert_eq!(command.command_type, CommandType::Ping);

        let command = parser.next_command().unwrap().unwrap();
        assert_eq!(command.command_type, CommandType::Info);
    }

    #[test]
    fn test_parse_bulk_string() {
        let mut parser = RespParse::new(RespVersion::RESP2);
        let res = parser.parse(Bytes::from("$6\r\nfoobar\r\n"));
        assert_eq!(
            res,
            RespParseResult::Complete(RespData::BulkString(Option::from(Bytes::from("foobar"))))
        );
    }

    #[test]
    fn test_parse_array() {
        let mut parser = RespParse::new(RespVersion::RESP2);
        let res = parser.parse(Bytes::from("*3\r\n$3\r\nfoo\r\n$3\r\nbar\r\n$-1\r\n"));
        assert_eq!(
            res,
            RespParseResult::Complete(RespData::Array(Option::from(vec![
                RespData::BulkString(Some(Bytes::from("foo"))),
                RespData::BulkString(Some(Bytes::from("bar"))),
                RespData::BulkString(None),
            ])))
        );
    }

    #[test]
    fn test_parse_array_rest_swap() {
        let mut parser = RespParse::new(RespVersion::RESP2);
        let res = parser.parse(Bytes::from("*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n"));
        assert_eq!(
            res,
            RespParseResult::Complete(RespData::Array(Option::from(vec![
                RespData::BulkString(Some(Bytes::from("foo"))),
                RespData::BulkString(Some(Bytes::from("bar"))),
            ])))
        );

        let res = parser.parse(Bytes::new());
        assert_eq!(res, RespParseResult::Incomplete);
    }

    #[test]
    fn test_parse_empty_bulk_string() {
        let mut parser = RespParse::new(RespVersion::RESP2);
        let res = parser.parse(Bytes::from("$0\r\n\r\n"));
        assert_eq!(
            res,
            RespParseResult::Complete(RespData::BulkString(Option::from(Bytes::from(""))))
        );
    }

    #[test]
    fn test_parse_empty_array() {
        let mut parser = RespParse::new(RespVersion::RESP2);
        let res = parser.parse(Bytes::from("*0\r\n"));
        assert_eq!(
            res,
            RespParseResult::Complete(RespData::Array(Option::from(vec![])))
        );
    }

    #[test]
    fn test_parse_incomplete() {
        let mut parser = RespParse::new(RespVersion::RESP2);
        let res = parser.parse(Bytes::from("$10\r\nfoobar"));
        assert_eq!(res, RespParseResult::Incomplete);
    }

    // RESP3 specific tests
    #[test]
    fn test_parse_resp3_null() {
        let mut parser = RespParse::new(RespVersion::RESP3);
        let res = parser.parse(Bytes::from("_\r\n"));
        assert_eq!(res, RespParseResult::Complete(RespData::Null));
    }

    #[test]
    fn test_parse_resp3_boolean_true() {
        let mut parser = RespParse::new(RespVersion::RESP3);
        let res = parser.parse(Bytes::from("#t\r\n"));
        assert_eq!(res, RespParseResult::Complete(RespData::Boolean(true)));
    }

    #[test]
    fn test_parse_resp3_boolean_false() {
        let mut parser = RespParse::new(RespVersion::RESP3);
        let res = parser.parse(Bytes::from("#f\r\n"));
        assert_eq!(res, RespParseResult::Complete(RespData::Boolean(false)));
    }

    #[test]
    fn test_parse_resp3_double() {
        let mut parser = RespParse::new(RespVersion::RESP3);
        let res = parser.parse(Bytes::from(format!(",{}\r\n", std::f64::consts::PI)));
        assert_eq!(
            res,
            RespParseResult::Complete(RespData::Double(std::f64::consts::PI))
        );
    }

    #[test]
    fn test_parse_resp3_double_negative() {
        let mut parser = RespParse::new(RespVersion::RESP3);
        let res = parser.parse(Bytes::from(",-2.5\r\n"));
        assert_eq!(res, RespParseResult::Complete(RespData::Double(-2.5)));
    }

    #[test]
    fn test_parse_resp3_double_scientific() {
        let mut parser = RespParse::new(RespVersion::RESP3);
        let res = parser.parse(Bytes::from(",1.23e-4\r\n"));
        assert_eq!(res, RespParseResult::Complete(RespData::Double(1.23e-4)));
    }

    #[test]
    fn test_parse_resp3_big_number() {
        let mut parser = RespParse::new(RespVersion::RESP3);
        let res = parser.parse(Bytes::from("(123456789012345678901234567890\r\n"));
        assert_eq!(
            res,
            RespParseResult::Complete(RespData::BigNumber(Bytes::from(
                "123456789012345678901234567890"
            )))
        );
    }

    #[test]
    fn test_parse_resp3_bulk_error() {
        let mut parser = RespParse::new(RespVersion::RESP3);
        let res = parser.parse(Bytes::from("!21\r\nSYNTAX invalid syntax\r\n"));
        assert_eq!(
            res,
            RespParseResult::Complete(RespData::BulkError(Bytes::from("SYNTAX invalid syntax")))
        );
    }

    #[test]
    fn test_parse_resp3_verbatim_string() {
        let mut parser = RespParse::new(RespVersion::RESP3);
        let res = parser.parse(Bytes::from("=15\r\ntxt:Some string\r\n"));
        assert_eq!(
            res,
            RespParseResult::Complete(RespData::VerbatimString {
                format: Bytes::from("txt"),
                data: Bytes::from("Some string"),
            })
        );
    }

    #[test]
    fn test_parse_resp3_map() {
        let mut parser = RespParse::new(RespVersion::RESP3);
        let res = parser.parse(Bytes::from("%2\r\n+first\r\n:1\r\n+second\r\n:2\r\n"));
        assert_eq!(
            res,
            RespParseResult::Complete(RespData::Map(vec![
                (
                    RespData::SimpleString(Bytes::from("first")),
                    RespData::Integer(1)
                ),
                (
                    RespData::SimpleString(Bytes::from("second")),
                    RespData::Integer(2)
                ),
            ]))
        );
    }

    #[test]
    fn test_parse_resp3_set() {
        let mut parser = RespParse::new(RespVersion::RESP3);
        let res = parser.parse(Bytes::from("~3\r\n+orange\r\n+apple\r\n#t\r\n"));
        assert_eq!(
            res,
            RespParseResult::Complete(RespData::Set(vec![
                RespData::SimpleString(Bytes::from("orange")),
                RespData::SimpleString(Bytes::from("apple")),
                RespData::Boolean(true),
            ]))
        );
    }

    #[test]
    fn test_parse_resp3_push() {
        let mut parser = RespParse::new(RespVersion::RESP3);
        let res = parser.parse(Bytes::from(">2\r\n+pubsub\r\n+message\r\n"));
        assert_eq!(
            res,
            RespParseResult::Complete(RespData::Push(vec![
                RespData::SimpleString(Bytes::from("pubsub")),
                RespData::SimpleString(Bytes::from("message")),
            ]))
        );
    }

    // Regression for #395: untrusted aggregate lengths must not drive allocation size.
    #[test]
    fn test_reject_oversized_aggregate_lengths_without_panicking() {
        for frame in [
            "*2147483648\r\n",
            "%2147483648\r\n",
            "~2147483648\r\n",
            ">2147483648\r\n",
            "*9223372036854775807\r\n",
            "%9223372036854775807\r\n",
            "~9223372036854775807\r\n",
            ">9223372036854775807\r\n",
        ] {
            let mut parser = RespParse::new(RespVersion::RESP3);
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                parser.parse(Bytes::copy_from_slice(frame.as_bytes()))
            }));

            assert!(
                matches!(result, Ok(RespParseResult::Error(_))),
                "expected a parse error for {frame:?}, got {result:?}"
            );
        }
    }

    #[test]
    fn test_maximum_aggregate_lengths_do_not_preallocate_declared_size() {
        for frame in [
            "*2147483647\r\n",
            "%2147483647\r\n",
            "~2147483647\r\n",
            ">2147483647\r\n",
        ] {
            let mut parser = RespParse::new(RespVersion::RESP3);
            assert_eq!(
                parser.parse(Bytes::copy_from_slice(frame.as_bytes())),
                RespParseResult::Incomplete,
                "unexpected parse result for {frame:?}"
            );
        }
    }

    #[test]
    fn test_incomplete_aggregate_headers_do_not_allocate_declared_capacity() {
        for frame in [
            b"*2147483647\r\n".as_slice(),
            b"%2147483647\r\n".as_slice(),
            b"~2147483647\r\n".as_slice(),
            b">2147483647\r\n".as_slice(),
        ] {
            let mut parser = RespParse::new(RespVersion::RESP3);
            parser.buffer.reserve(frame.len());
            let input = Bytes::from_static(frame);
            let measurement = AllocationMeasurement::start();
            let result = parser.parse(input);
            let allocated = measurement.finish();

            assert_eq!(result, RespParseResult::Incomplete, "{frame:?}");
            assert_eq!(
                allocated, 0,
                "incomplete aggregate header {frame:?} allocated container storage"
            );
        }
    }

    #[test]
    fn test_nested_incomplete_aggregate_headers_have_input_bounded_allocation() {
        let frame = "*2147483647\r\n".repeat(RespParse::MAX_AGGREGATE_NESTING_DEPTH);
        let mut parser = RespParse::new(RespVersion::RESP3);
        parser.buffer.reserve(frame.len());
        let input = Bytes::copy_from_slice(frame.as_bytes());
        let measurement = AllocationMeasurement::start();
        let result = parser.parse(input);
        let allocated = measurement.finish();

        assert_eq!(result, RespParseResult::Incomplete);
        assert_eq!(
            allocated, 0,
            "nested incomplete headers allocated container storage"
        );
    }

    #[test]
    fn test_accept_aggregate_nesting_at_limit() {
        let frame = format!(
            "{}:1\r\n",
            "*1\r\n".repeat(RespParse::MAX_AGGREGATE_NESTING_DEPTH)
        );
        let mut parser = RespParse::new(RespVersion::RESP2);

        assert!(matches!(
            parser.parse(Bytes::copy_from_slice(frame.as_bytes())),
            RespParseResult::Complete(_)
        ));
    }

    // Regression for #395: nested aggregate headers must not grow parser resources without bound.
    #[test]
    fn test_reject_aggregate_nesting_beyond_limit() {
        for header in [
            "*2147483647\r\n",
            "%2147483647\r\n",
            "~2147483647\r\n",
            ">2147483647\r\n",
        ] {
            let frame = header.repeat(RespParse::MAX_AGGREGATE_NESTING_DEPTH + 1);
            let mut parser = RespParse::new(RespVersion::RESP3);

            assert!(
                matches!(
                    parser.parse(Bytes::copy_from_slice(frame.as_bytes())),
                    RespParseResult::Error(_)
                ),
                "aggregate nesting beyond the parser limit must be rejected for {header:?}"
            );
        }
    }

    #[test]
    fn test_auto_detect_resp3() {
        let mut parser = RespParse::new(RespVersion::RESP2);
        assert_eq!(parser.version(), RespVersion::RESP2);

        // Parse RESP3 data should auto-detect and switch
        let res = parser.parse(Bytes::from("_\r\n"));
        assert_eq!(res, RespParseResult::Complete(RespData::Null));
        assert_eq!(parser.version(), RespVersion::RESP3);
    }

    #[test]
    fn test_version_detection() {
        assert_eq!(RespParse::detect_version(b"_\r\n"), RespVersion::RESP3);
        assert_eq!(RespParse::detect_version(b"#t\r\n"), RespVersion::RESP3);
        assert_eq!(RespParse::detect_version(b",3.14\r\n"), RespVersion::RESP3);
        assert_eq!(RespParse::detect_version(b"+OK\r\n"), RespVersion::RESP2);
        assert_eq!(RespParse::detect_version(b":42\r\n"), RespVersion::RESP2);
        assert_eq!(RespParse::detect_version(b""), RespVersion::RESP2);
    }
}
