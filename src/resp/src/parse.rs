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
    bytes::streaming::{take, take_while1},
    character::streaming::{char, digit1, line_ending, not_line_ending, space1},
    combinator::{map, map_res, opt, recognize},
    multi::separated_list0,
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
        let mut parse_parts = separated_list0(
            space1,
            map(
                take_while1(|c| c != b' ' && c != b'\r' && c != b'\n'),
                |s: &[u8]| Bytes::copy_from_slice(s),
            ),
        );

        let (input, parts) = parse_parts.parse(input)?;

        let (input, _) = line_ending(input)?;

        if parts.is_empty() {
            return Err(nom::Err::Error(nom::error::Error::new(
                input,
                nom::error::ErrorKind::Verify,
            )));
        }

        Ok((input, RespData::Inline(parts)))
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

    fn parse_array(input: &[u8]) -> IResult<&[u8], RespData> {
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

        let mut remaining = input;
        let mut elements = Vec::with_capacity(len as usize);

        for _ in 0..len {
            let (new_remaining, element) = Self::parse_resp_data(remaining)?;
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

    fn parse_map(input: &[u8]) -> IResult<&[u8], RespData> {
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

        let mut remaining = input;
        let mut pairs = Vec::with_capacity(len as usize);

        for _ in 0..len {
            let (new_remaining, key) = Self::parse_resp_data(remaining)?;
            let (new_remaining, value) = Self::parse_resp_data(new_remaining)?;
            pairs.push((key, value));
            remaining = new_remaining;
        }

        Ok((remaining, RespData::Map(pairs)))
    }

    fn parse_set(input: &[u8]) -> IResult<&[u8], RespData> {
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

        let mut remaining = input;
        let mut elements = Vec::with_capacity(len as usize);

        for _ in 0..len {
            let (new_remaining, element) = Self::parse_resp_data(remaining)?;
            elements.push(element);
            remaining = new_remaining;
        }

        Ok((remaining, RespData::Set(elements)))
    }

    fn parse_push(input: &[u8]) -> IResult<&[u8], RespData> {
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

        let mut remaining = input;
        let mut elements = Vec::with_capacity(len as usize);

        for _ in 0..len {
            let (new_remaining, element) = Self::parse_resp_data(remaining)?;
            elements.push(element);
            remaining = new_remaining;
        }

        Ok((remaining, RespData::Push(elements)))
    }

    fn parse_resp_data(input: &[u8]) -> IResult<&[u8], RespData> {
        if input.is_empty() {
            return Err(nom::Err::Incomplete(nom::Needed::Unknown));
        }

        match input[0] {
            b'+' => Self::parse_simple_string(input),
            b'-' => Self::parse_error(input),
            b':' => Self::parse_integer(input),
            b'$' => Self::parse_bulk_string(input),
            b'*' => Self::parse_array(input),
            // RESP3 types
            b'_' => Self::parse_null(input),
            b'#' => Self::parse_boolean(input),
            b',' => Self::parse_double(input),
            b'(' => Self::parse_big_number(input),
            b'!' => Self::parse_bulk_error(input),
            b'=' => Self::parse_verbatim_string(input),
            b'%' => Self::parse_map(input),
            b'~' => Self::parse_set(input),
            b'>' => Self::parse_push(input),
            _ => Self::parse_inline(input),
        }
    }

    fn process_buffer(&mut self) -> RespParseResult {
        if self.buffer.is_empty() {
            return RespParseResult::Incomplete;
        }

        match Self::parse_resp_data(&self.buffer) {
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

#[cfg(test)]
mod tests {
    use super::Bytes;
    use super::{Parse, RespData, RespParse, RespParseResult, RespVersion};

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
            RespParseResult::Complete(RespData::Inline(vec![Bytes::from("ping")]))
        );
        parser.reset();

        let res = parser.parse(Bytes::from("PING\r\n\r\n"));
        assert_eq!(
            res,
            RespParseResult::Complete(RespData::Inline(vec![Bytes::from("PING")]))
        );
        parser.reset();

        let res = parser.parse(Bytes::from("PING\n"));
        assert_eq!(
            res,
            RespParseResult::Complete(RespData::Inline(vec![Bytes::from("PING")]))
        );
        parser.reset();

        let res = parser.parse(Bytes::from("PING\n\n"));
        assert_eq!(
            res,
            RespParseResult::Complete(RespData::Inline(vec![Bytes::from("PING")]))
        );
        parser.reset();

        let res = parser.parse(Bytes::from("PING\r\n\n"));
        assert_eq!(
            res,
            RespParseResult::Complete(RespData::Inline(vec![Bytes::from("PING")]))
        );
        parser.reset();
    }

    #[test]
    fn test_parse_inline_params() {
        let mut parser = RespParse::new(RespVersion::RESP2);
        let res = parser.parse(Bytes::from("hmget fruit apple banana watermelon\r\n"));
        assert_eq!(
            res,
            RespParseResult::Complete(RespData::Inline(vec![
                Bytes::from("hmget"),
                Bytes::from("fruit"),
                Bytes::from("apple"),
                Bytes::from("banana"),
                Bytes::from("watermelon")
            ]))
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
            RespParseResult::Complete(RespData::Inline(vec![Bytes::from("ping")]))
        );

        let res = parser.parse(Bytes::new());
        assert_eq!(
            res,
            RespParseResult::Complete(RespData::Inline(vec![
                Bytes::from("hmget"),
                Bytes::from("fruit"),
                Bytes::from("apple"),
                Bytes::from("banana"),
                Bytes::from("watermelon")
            ]))
        );
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
