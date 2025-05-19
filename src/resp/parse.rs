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
use std::collections::VecDeque;
use std::str;

use crate::resp::{
    command::{Command, RespCommand},
    error::{RespError, RespResult},
    types::{RespData, RespVersion},
};

#[derive(Debug, PartialEq, Eq)]
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
        }
    }

    pub fn version(&self) -> RespVersion {
        self.version
    }

    pub fn set_version(&mut self, version: RespVersion) {
        self.version = version;
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
                let error_msg = format!("Parse error: {:?}", e);
                RespParseResult::Error(RespError::ParseError(error_msg))
            }
        }
    }
}

impl Parse for RespParse {
    fn parse(&mut self, data: Bytes) -> RespParseResult {
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
    }
}

impl Drop for RespParse {
    fn drop(&mut self) {
        self.reset();
    }
}
