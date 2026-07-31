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

pub const MAX_UNAUTHENTICATED_BUFFER_SIZE: usize = 1024 * 1024;

#[derive(Clone, Copy, Debug)]
struct RespLimits {
    max_inline_len: usize,
    max_bulk_len: usize,
    max_buffer_len: usize,
    max_aggregate_len: usize,
    max_nesting_depth: usize,
    initial_aggregate_capacity: usize,
    /// Maximum RESP nodes materialized by one decoded frame.
    max_decoded_nodes: usize,
    /// Maximum cumulative node visits while an incomplete frame is replayed.
    max_parse_work: usize,
}

impl Default for RespLimits {
    fn default() -> Self {
        Self {
            max_inline_len: 64 * 1024,
            max_bulk_len: 512 * 1024 * 1024,
            max_buffer_len: 1024 * 1024 * 1024,
            max_aggregate_len: i32::MAX as usize,
            max_nesting_depth: 128,
            initial_aggregate_capacity: 1024,
            max_decoded_nodes: 64 * 1024,
            max_parse_work: 1_000_000,
        }
    }
}

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
    limits: RespLimits,
    parse_work: usize,
}

impl Default for RespParse {
    fn default() -> Self {
        Self::new(RespVersion::default())
    }
}

impl RespParse {
    pub fn new(version: RespVersion) -> Self {
        Self {
            version,
            buffer: BytesMut::new(),
            commands: VecDeque::new(),
            is_pipeline: false,
            version_detected: false,
            limits: RespLimits::default(),
            parse_work: 0,
        }
    }

    #[cfg(test)]
    fn with_limits(version: RespVersion, limits: RespLimits) -> Self {
        Self {
            version,
            buffer: BytesMut::new(),
            commands: VecDeque::new(),
            is_pipeline: false,
            version_detected: false,
            limits,
            parse_work: 0,
        }
    }

    pub fn buffered_len(&self) -> usize {
        self.buffer.len()
    }

    pub fn version(&self) -> RespVersion {
        self.version
    }

    pub fn set_version(&mut self, version: RespVersion) {
        self.version = version;
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

    fn resource_limit_error(input: &[u8]) -> nom::Err<nom::error::Error<&[u8]>> {
        nom::Err::Failure(nom::error::Error::new(
            input,
            nom::error::ErrorKind::TooLarge,
        ))
    }

    fn checked_length(
        input: &[u8],
        len: i64,
        max: usize,
    ) -> Result<usize, nom::Err<nom::error::Error<&[u8]>>> {
        let len = usize::try_from(len).map_err(|_| Self::resource_limit_error(input))?;
        if len > max {
            return Err(Self::resource_limit_error(input));
        }
        Ok(len)
    }

    fn push_aggregate<'a, T>(
        elements: &mut Vec<T>,
        element: T,
        total_len: usize,
        input: &'a [u8],
        limits: &RespLimits,
    ) -> Result<(), nom::Err<nom::error::Error<&'a [u8]>>> {
        if elements.len() == elements.capacity() {
            let remaining = total_len.saturating_sub(elements.len());
            let additional = remaining.min(limits.initial_aggregate_capacity).max(1);
            elements
                .try_reserve(additional)
                .map_err(|_| Self::resource_limit_error(input))?;
        }
        elements.push(element);
        Ok(())
    }

    fn check_aggregate_depth<'a>(
        input: &'a [u8],
        depth: usize,
        limits: &RespLimits,
    ) -> Result<(), nom::Err<nom::error::Error<&'a [u8]>>> {
        if depth >= limits.max_nesting_depth {
            return Err(Self::resource_limit_error(input));
        }
        Ok(())
    }

    fn check_line_length<'a>(
        input: &'a [u8],
        limits: &RespLimits,
    ) -> Result<(), nom::Err<nom::error::Error<&'a [u8]>>> {
        let line_len = input
            .iter()
            .position(|byte| matches!(byte, b'\r' | b'\n'))
            .unwrap_or(input.len());
        if line_len > limits.max_inline_len {
            return Err(Self::resource_limit_error(input));
        }
        Ok(())
    }

    fn charge_parse_node<'a>(
        input: &'a [u8],
        limits: &RespLimits,
        parse_work: &mut usize,
        decoded_nodes: &mut usize,
    ) -> Result<(), nom::Err<nom::error::Error<&'a [u8]>>> {
        *parse_work = parse_work
            .checked_add(1)
            .ok_or_else(|| Self::resource_limit_error(input))?;
        *decoded_nodes = decoded_nodes
            .checked_add(1)
            .ok_or_else(|| Self::resource_limit_error(input))?;
        if *parse_work > limits.max_parse_work || *decoded_nodes > limits.max_decoded_nodes {
            return Err(Self::resource_limit_error(input));
        }
        Ok(())
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

    fn parse_bulk_string<'a>(input: &'a [u8], limits: &RespLimits) -> IResult<&'a [u8], RespData> {
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

        let len = Self::checked_length(input, len, limits.max_bulk_len)?;
        let mut ter_parser = terminated(take(len), line_ending);
        let (input, data) = ter_parser.parse(input)?;
        Ok((
            input,
            RespData::BulkString(Some(Bytes::copy_from_slice(data))),
        ))
    }

    fn parse_array<'a>(
        input: &'a [u8],
        limits: &RespLimits,
        depth: usize,
        parse_work: &mut usize,
        decoded_nodes: &mut usize,
    ) -> IResult<&'a [u8], RespData> {
        Self::check_aggregate_depth(input, depth, limits)?;
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

        let len = Self::checked_length(input, len, limits.max_aggregate_len)?;
        let mut remaining = input;
        let mut elements = Vec::new();

        for _ in 0..len {
            let (new_remaining, element) =
                Self::parse_resp_data(remaining, limits, depth + 1, parse_work, decoded_nodes)?;
            Self::push_aggregate(&mut elements, element, len, remaining, limits)?;
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

    fn parse_bulk_error<'a>(input: &'a [u8], limits: &RespLimits) -> IResult<&'a [u8], RespData> {
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

        let len = Self::checked_length(input, len, limits.max_bulk_len)?;
        let mut ter_parser = terminated(take(len), line_ending);
        let (input, data) = ter_parser.parse(input)?;
        Ok((input, RespData::BulkError(Bytes::copy_from_slice(data))))
    }

    fn parse_verbatim_string<'a>(
        input: &'a [u8],
        limits: &RespLimits,
    ) -> IResult<&'a [u8], RespData> {
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

        let len = Self::checked_length(input, len, limits.max_bulk_len)?;
        let mut ter_parser = terminated(take(len), line_ending);
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

    fn parse_map<'a>(
        input: &'a [u8],
        limits: &RespLimits,
        depth: usize,
        parse_work: &mut usize,
        decoded_nodes: &mut usize,
    ) -> IResult<&'a [u8], RespData> {
        Self::check_aggregate_depth(input, depth, limits)?;
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

        let len = Self::checked_length(input, len, limits.max_aggregate_len)?;
        let mut remaining = input;
        let mut pairs = Vec::new();

        for _ in 0..len {
            let (new_remaining, key) =
                Self::parse_resp_data(remaining, limits, depth + 1, parse_work, decoded_nodes)?;
            let (new_remaining, value) =
                Self::parse_resp_data(new_remaining, limits, depth + 1, parse_work, decoded_nodes)?;
            Self::push_aggregate(&mut pairs, (key, value), len, remaining, limits)?;
            remaining = new_remaining;
        }

        Ok((remaining, RespData::Map(pairs)))
    }

    fn parse_set<'a>(
        input: &'a [u8],
        limits: &RespLimits,
        depth: usize,
        parse_work: &mut usize,
        decoded_nodes: &mut usize,
    ) -> IResult<&'a [u8], RespData> {
        Self::check_aggregate_depth(input, depth, limits)?;
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

        let len = Self::checked_length(input, len, limits.max_aggregate_len)?;
        let mut remaining = input;
        let mut elements = Vec::new();

        for _ in 0..len {
            let (new_remaining, element) =
                Self::parse_resp_data(remaining, limits, depth + 1, parse_work, decoded_nodes)?;
            Self::push_aggregate(&mut elements, element, len, remaining, limits)?;
            remaining = new_remaining;
        }

        Ok((remaining, RespData::Set(elements)))
    }

    fn parse_push<'a>(
        input: &'a [u8],
        limits: &RespLimits,
        depth: usize,
        parse_work: &mut usize,
        decoded_nodes: &mut usize,
    ) -> IResult<&'a [u8], RespData> {
        Self::check_aggregate_depth(input, depth, limits)?;
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

        let len = Self::checked_length(input, len, limits.max_aggregate_len)?;
        let mut remaining = input;
        let mut elements = Vec::new();

        for _ in 0..len {
            let (new_remaining, element) =
                Self::parse_resp_data(remaining, limits, depth + 1, parse_work, decoded_nodes)?;
            Self::push_aggregate(&mut elements, element, len, remaining, limits)?;
            remaining = new_remaining;
        }

        Ok((remaining, RespData::Push(elements)))
    }

    fn parse_resp_data<'a>(
        input: &'a [u8],
        limits: &RespLimits,
        depth: usize,
        parse_work: &mut usize,
        decoded_nodes: &mut usize,
    ) -> IResult<&'a [u8], RespData> {
        if input.is_empty() {
            return Err(nom::Err::Incomplete(nom::Needed::Unknown));
        }

        Self::check_line_length(input, limits)?;
        Self::charge_parse_node(input, limits, parse_work, decoded_nodes)?;

        match input[0] {
            b'+' => Self::parse_simple_string(input),
            b'-' => Self::parse_error(input),
            b':' => Self::parse_integer(input),
            b'$' => Self::parse_bulk_string(input, limits),
            b'*' => Self::parse_array(input, limits, depth, parse_work, decoded_nodes),
            // RESP3 types
            b'_' => Self::parse_null(input),
            b'#' => Self::parse_boolean(input),
            b',' => Self::parse_double(input),
            b'(' => Self::parse_big_number(input),
            b'!' => Self::parse_bulk_error(input, limits),
            b'=' => Self::parse_verbatim_string(input, limits),
            b'%' => Self::parse_map(input, limits, depth, parse_work, decoded_nodes),
            b'~' => Self::parse_set(input, limits, depth, parse_work, decoded_nodes),
            b'>' => Self::parse_push(input, limits, depth, parse_work, decoded_nodes),
            _ => Self::parse_inline(input),
        }
    }

    fn process_buffer(&mut self) -> RespParseResult {
        self.skip_empty_lines();

        if self.buffer.is_empty() {
            return RespParseResult::Incomplete;
        }

        let mut parse_work = self.parse_work;
        let mut decoded_nodes = 0;
        let result = Self::parse_resp_data(
            &self.buffer,
            &self.limits,
            0,
            &mut parse_work,
            &mut decoded_nodes,
        );
        self.parse_work = parse_work;

        match result {
            Ok((remaining, resp_data)) => {
                self.parse_work = 0;
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
                let error_msg = if e.code == nom::error::ErrorKind::TooLarge {
                    "RESP resource limit exceeded".to_string()
                } else {
                    format!("Parse error: {e:?}")
                };
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

        let Some(buffered_len) = self.buffer.len().checked_add(data.len()) else {
            return RespParseResult::Error(RespError::ParseError(
                "RESP buffer limit exceeded".to_string(),
            ));
        };
        if buffered_len > self.limits.max_buffer_len {
            return RespParseResult::Error(RespError::ParseError(format!(
                "RESP buffer limit exceeded: {buffered_len} bytes exceeds {} bytes",
                self.limits.max_buffer_len
            )));
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
        self.parse_work = 0;
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
    use crate::command::CommandType;

    use super::Bytes;
    use super::{
        MAX_UNAUTHENTICATED_BUFFER_SIZE, Parse, RespData, RespError, RespLimits, RespParse,
        RespParseResult, RespVersion,
    };

    fn small_limits() -> RespLimits {
        RespLimits {
            max_inline_len: 8,
            max_bulk_len: 8,
            max_buffer_len: 32,
            max_aggregate_len: 3,
            max_nesting_depth: 2,
            initial_aggregate_capacity: 2,
            max_decoded_nodes: 4,
            max_parse_work: 8,
        }
    }

    fn assert_limit_error(result: RespParseResult) {
        assert!(
            matches!(result, RespParseResult::Error(RespError::ParseError(ref message)) if message.contains("limit")),
            "expected a resource limit error, got {result:?}"
        );
    }

    #[test]
    fn test_default_resource_limits() {
        let limits = RespLimits::default();
        assert_eq!(limits.max_inline_len, 64 * 1024);
        assert_eq!(limits.max_bulk_len, 512 * 1024 * 1024);
        assert_eq!(limits.max_buffer_len, 1024 * 1024 * 1024);
        assert_eq!(limits.max_aggregate_len, i32::MAX as usize);
        assert_eq!(limits.max_nesting_depth, 128);
        assert_eq!(limits.initial_aggregate_capacity, 1024);
        assert_eq!(limits.max_decoded_nodes, 64 * 1024);
        assert_eq!(limits.max_parse_work, 1_000_000);
        assert_eq!(MAX_UNAUTHENTICATED_BUFFER_SIZE, 1024 * 1024);
    }

    #[test]
    fn test_bulk_lengths_accept_exact_limit_and_reject_larger_headers() {
        let mut parser = RespParse::with_limits(RespVersion::RESP3, small_limits());

        assert_eq!(
            parser.parse(Bytes::from("$8\r\n12345678\r\n")),
            RespParseResult::Complete(RespData::BulkString(Some(Bytes::from("12345678"))))
        );

        parser.reset();
        assert_eq!(
            parser.parse(Bytes::from("!8\r\n12345678\r\n")),
            RespParseResult::Complete(RespData::BulkError(Bytes::from("12345678")))
        );

        parser.reset();
        assert_eq!(
            parser.parse(Bytes::from("=8\r\ntxt:data\r\n")),
            RespParseResult::Complete(RespData::VerbatimString {
                format: Bytes::from("txt"),
                data: Bytes::from("data"),
            })
        );

        for header in ["$9\r\n", "!9\r\n", "=9\r\n"] {
            parser.reset();
            assert_limit_error(parser.parse(Bytes::from(header)));
        }
    }

    #[test]
    fn test_aggregate_lengths_accept_exact_limit_and_reject_larger_headers() {
        let mut parser = RespParse::with_limits(RespVersion::RESP3, small_limits());

        assert_eq!(
            parser.parse(Bytes::from("*3\r\n:1\r\n:2\r\n:3\r\n")),
            RespParseResult::Complete(RespData::Array(Some(vec![
                RespData::Integer(1),
                RespData::Integer(2),
                RespData::Integer(3),
            ])))
        );

        for header in ["*4\r\n", "%4\r\n", "~4\r\n", ">4\r\n"] {
            parser.reset();
            assert_limit_error(parser.parse(Bytes::from(header)));
        }
    }

    #[test]
    fn test_inline_length_accepts_exact_limit_and_rejects_larger_input() {
        let mut parser = RespParse::with_limits(RespVersion::RESP2, small_limits());

        assert!(matches!(
            parser.parse(Bytes::from("ping abc\r\n")),
            RespParseResult::Complete(_)
        ));

        parser.reset();
        assert_limit_error(parser.parse(Bytes::from("ping abcd\r\n")));
    }

    #[test]
    fn test_all_resp_first_lines_use_the_inline_length_limit() {
        let oversized_lines = [
            "+12345678",
            "-12345678",
            ":12345678",
            ",12345678",
            "(12345678",
            "$00000000",
            "*00000000",
            "!00000000",
            "=00000000",
            "%00000000",
            "~00000000",
            ">00000000",
        ];

        for line in oversized_lines {
            let mut parser = RespParse::with_limits(RespVersion::RESP3, small_limits());
            assert_limit_error(parser.parse(Bytes::from(line)));
        }
    }

    #[test]
    fn test_decoded_node_budget_counts_aggregate_children_and_map_values() {
        let mut parser = RespParse::with_limits(RespVersion::RESP3, small_limits());

        assert!(matches!(
            parser.parse(Bytes::from("*3\r\n:1\r\n:2\r\n:3\r\n")),
            RespParseResult::Complete(_)
        ));

        parser.reset();
        assert_limit_error(parser.parse(Bytes::from("%2\r\n:1\r\n:2\r\n:3\r\n:4\r\n")));
    }

    #[test]
    fn test_replayed_incomplete_aggregate_has_a_cumulative_work_budget() {
        let mut parser = RespParse::with_limits(RespVersion::RESP2, small_limits());

        assert_eq!(
            parser.parse(Bytes::from("*3\r\n:1\r\n")),
            RespParseResult::Incomplete
        );
        assert_eq!(
            parser.parse(Bytes::from(":2\r\n")),
            RespParseResult::Incomplete
        );
        assert_limit_error(parser.parse(Bytes::from(":3\r\n")));
    }

    #[test]
    fn test_aggregate_nesting_accepts_exact_limit_and_rejects_deeper_input() {
        let mut parser = RespParse::with_limits(RespVersion::RESP2, small_limits());

        assert!(matches!(
            parser.parse(Bytes::from("*1\r\n*1\r\n:1\r\n")),
            RespParseResult::Complete(_)
        ));

        parser.reset();
        assert_limit_error(parser.parse(Bytes::from("*1\r\n*1\r\n*1\r\n:1\r\n")));
    }

    #[test]
    fn test_chunked_buffer_growth_is_checked_before_append() {
        let mut limits = small_limits();
        limits.max_bulk_len = 64;
        limits.max_buffer_len = 17;
        let mut parser = RespParse::with_limits(RespVersion::RESP2, limits);

        assert_eq!(
            parser.parse(Bytes::from("$10\r\n1234567890\r")),
            RespParseResult::Incomplete
        );
        assert_eq!(parser.buffered_len(), 16);
        assert!(matches!(
            parser.parse(Bytes::from("\n")),
            RespParseResult::Complete(_)
        ));
        assert_eq!(parser.buffered_len(), 0);

        assert_eq!(
            parser.parse(Bytes::from("$20\r\n12345678901")),
            RespParseResult::Incomplete
        );
        assert_eq!(parser.buffered_len(), 16);

        assert_limit_error(parser.parse(Bytes::from("23")));
        assert_eq!(parser.buffered_len(), 16);
    }

    #[test]
    fn test_maximum_aggregate_header_does_not_require_payload_allocation() {
        let mut parser = RespParse::new(RespVersion::RESP2);

        assert_eq!(
            parser.parse(Bytes::from(format!("*{}\r\n", i32::MAX))),
            RespParseResult::Incomplete
        );
    }

    #[test]
    fn test_reset_recovers_after_resource_limit_error() {
        let mut parser = RespParse::with_limits(RespVersion::RESP2, small_limits());

        assert_limit_error(parser.parse(Bytes::from("$9\r\n")));
        assert_ne!(parser.buffered_len(), 0);

        parser.reset();
        assert_eq!(parser.buffered_len(), 0);
        assert!(matches!(
            parser.parse(Bytes::from("$4\r\nping\r\n")),
            RespParseResult::Complete(_)
        ));
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
