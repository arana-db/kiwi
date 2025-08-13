use crate::{RespError, RespValue, Result};
use bytes::Bytes;

/// Result of a parsing operation
#[derive(Debug, Clone, PartialEq)]
pub enum ParseResult {
    /// Parsing completed successfully with the value and remaining bytes
    Complete(RespValue, Bytes),
    /// More data is needed to complete parsing
    Incomplete(usize), // Number of additional bytes needed (hint)
}

/// Trait for RESP protocol parsers
pub trait RespParser: Send + Sync {
    /// Parse a complete RESP message from input bytes
    fn parse(&self, input: &[u8]) -> Result<ParseResult>;

    /// Parse multiple RESP messages (pipeline) from input bytes
    fn parse_multiple(&self, input: &[u8]) -> Result<(Vec<RespValue>, Bytes)> {
        let mut values = Vec::new();
        let mut remaining = Bytes::copy_from_slice(input);

        while !remaining.is_empty() {
            match self.parse(&remaining)? {
                ParseResult::Complete(value, rest) => {
                    values.push(value);
                    remaining = rest;
                }
                ParseResult::Incomplete(_) => break,
            }
        }

        Ok((values, remaining))
    }

    /// Get the RESP version this parser supports
    fn version(&self) -> crate::types::RespVersion;
}

/// Common parsing utilities used across different RESP versions
pub mod utils {
    use super::*;
    use nom::{
        bytes::complete::{tag, take},
        character::complete::{digit1, line_ending, not_line_ending},
        combinator::opt,
        sequence::terminated,
        IResult, Parser,
    };

    /// Parse CRLF line ending
    pub fn crlf(input: &[u8]) -> IResult<&[u8], &[u8]> {
        line_ending(input)
    }

    /// Parse a line terminated by CRLF
    pub fn parse_line(input: &[u8]) -> IResult<&[u8], &[u8]> {
        terminated(not_line_ending, crlf).parse(input)
    }

    /// Parse a signed integer
    pub fn parse_integer(input: &[u8]) -> IResult<&[u8], i64> {
        let (input, sign) = opt(tag(&b"-"[..])).parse(input)?;
        let (input, digits) = digit1(input)?;

        let digits_str = std::str::from_utf8(digits)
            .map_err(|_| nom::Err::Error(nom::error::Error::new(input, nom::error::ErrorKind::Digit)))?;

        let mut number: i64 = digits_str.parse()
            .map_err(|_| nom::Err::Error(nom::error::Error::new(input, nom::error::ErrorKind::Digit)))?;

        if sign.is_some() {
            number = -number;
        }

        Ok((input, number))
    }

    /// Parse bulk string length and content
    pub fn parse_bulk_string_content(input: &[u8], length: i64) -> IResult<&[u8], Option<Bytes>> {
        if length < 0 {
            // Null bulk string
            return Ok((input, None));
        }

        if length == 0 {
            // Empty bulk string
            let (input, _) = crlf(input)?;
            return Ok((input, Some(Bytes::new())));
        }

        let (input, content) = take(length as usize)(input)?;
        let (input, _) = crlf(input)?;

        Ok((input, Some(Bytes::copy_from_slice(content))))
    }

    /// Parse array length
    pub fn parse_array_length(input: &[u8]) -> IResult<&[u8], i64> {
        terminated(parse_integer, crlf).parse(input)
    }

    /// Split inline command into arguments
    pub fn split_inline_command(line: &str) -> Vec<String> {
        line.split_whitespace()
            .map(|s| s.to_string())
            .collect()
    }

    /// Convert nom error to RespError
    pub fn nom_error_to_resp_error(err: nom::Err<nom::error::Error<&[u8]>>) -> RespError {
        match err {
            nom::Err::Incomplete(_) => RespError::IncompleteData { needed: 0 },
            nom::Err::Error(e) | nom::Err::Failure(e) => {
                RespError::InvalidFormat {
                    message: format!("Parsing error: {:?}", e.code)
                }
            }
        }
    }
}