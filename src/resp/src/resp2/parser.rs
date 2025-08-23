use crate::{
    parser::{utils, ParseResult, RespParser},
    types::{RespValue, RespVersion},
    Result,
};
use bytes::Bytes;
use nom::{
    bytes::complete::tag,
    character::complete::not_line_ending
    ,
    sequence::terminated,
    IResult, Parser,
};

/// RESP2 protocol parser
///
/// Implements the standard RESP2 protocol with support for:
/// - Simple Strings (+)
/// - Errors (-)
/// - Integers (:)
/// - Bulk Strings ($)
/// - Arrays (*)
/// - Inline commands (for backwards compatibility)
pub struct Resp2Parser;

impl Resp2Parser {
    pub fn new() -> Self {
        Self
    }

    /// Parse a RESP2 message
    fn parse_resp2(input: &[u8]) -> IResult<&[u8], RespValue> {
        if input.is_empty() {
            return Err(nom::Err::Incomplete(nom::Needed::Size(std::num::NonZero::new(1).unwrap())));
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

    /// Parse simple string (+OK\r\n)
    fn parse_simple_string(input: &[u8]) -> IResult<&[u8], RespValue> {
        let (input, _) = tag(&b"+"[..]).parse(input)?;
        let (input, content) = terminated(not_line_ending, utils::crlf).parse(input)?;
        let content_str = std::str::from_utf8(content)
            .map_err(|_| nom::Err::Error(nom::error::Error::new(input, nom::error::ErrorKind::Char)))?;

        Ok((input, RespValue::SimpleString(content_str.to_string())))
    }

    /// Parse error (-ERR message\r\n)
    fn parse_error(input: &[u8]) -> IResult<&[u8], RespValue> {
        let (input, _) = tag(&b"-"[..]).parse(input)?;
        let (input, content) = terminated(not_line_ending, utils::crlf).parse(input)?;
        let content_str = std::str::from_utf8(content)
            .map_err(|_| nom::Err::Error(nom::error::Error::new(input, nom::error::ErrorKind::Char)))?;

        Ok((input, RespValue::Error(content_str.to_string())))
    }

    /// Parse integer (:42\r\n)
    fn parse_integer(input: &[u8]) -> IResult<&[u8], RespValue> {
        let (input, _) = tag(&b":"[..]).parse(input)?;
        let (input, number) = terminated(utils::parse_integer, utils::crlf).parse(input)?;

        Ok((input, RespValue::Integer(number)))
    }

    /// Parse bulk string ($6\r\nfoobar\r\n or $-1\r\n)
    fn parse_bulk_string(input: &[u8]) -> IResult<&[u8], RespValue> {
        let (input, _) = tag(&b"$"[..]).parse(input)?;
        let (input, length) = terminated(utils::parse_integer, utils::crlf).parse(input)?;
        let (input, content) = utils::parse_bulk_string_content(input, length)?;

        Ok((input, RespValue::BulkString(content)))
    }

    /// Parse array (*2\r\n$5\r\nhello\r\n$5\r\nworld\r\n or *-1\r\n)
    fn parse_array(input: &[u8]) -> IResult<&[u8], RespValue> {
        let (input, _) = tag(&b"*"[..]).parse(input)?;
        let (input, length) = terminated(utils::parse_integer, utils::crlf).parse(input)?;

        if length < 0 {
            return Ok((input, RespValue::Array(None)));
        }

        if length == 0 {
            return Ok((input, RespValue::Array(Some(vec![]))));
        }

        let mut elements = Vec::with_capacity(length as usize);
        let mut remaining = input;

        for _ in 0..length {
            let (new_remaining, element) = Self::parse_resp2(remaining)?;
            elements.push(element);
            remaining = new_remaining;
        }

        Ok((remaining, RespValue::Array(Some(elements))))
    }

    /// Parse inline command (PING\r\n or GET key\r\n)
    fn parse_inline(input: &[u8]) -> IResult<&[u8], RespValue> {
        let (input, line) = terminated(not_line_ending, utils::crlf).parse(input)?;
        let line_str = std::str::from_utf8(line)
            .map_err(|_| nom::Err::Error(nom::error::Error::new(input, nom::error::ErrorKind::Char)))?;

        let args = utils::split_inline_command(line_str);
        Ok((input, RespValue::Inline(args)))
    }
}

impl Default for Resp2Parser {
    fn default() -> Self {
        Self::new()
    }
}

impl RespParser for Resp2Parser {
    fn parse(&self, input: &[u8]) -> Result<ParseResult> {
        match Self::parse_resp2(input) {
            Ok((remaining, value)) => {
                Ok(ParseResult::Complete(value, Bytes::copy_from_slice(remaining)))
            }
            Err(nom::Err::Incomplete(_)) => {
                Ok(ParseResult::Incomplete(1))
            }
            Err(nom::Err::Error(e)) | Err(nom::Err::Failure(e)) => {
                // Check if the error is due to incomplete data or EOF
                match e.code {
                    nom::error::ErrorKind::Eof => {
                        Ok(ParseResult::Incomplete(1))
                    }
                    _ => Err(utils::nom_error_to_resp_error(nom::Err::Error(e))),
                }
            }
        }
    }

    fn version(&self) -> RespVersion {
        RespVersion::Resp2
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_simple_string() {
        let parser = Resp2Parser::new();
        let input = b"+OK\r\n";

        let result = parser.parse(input).unwrap();
        match result {
            ParseResult::Complete(RespValue::SimpleString(s), _) => {
                assert_eq!(s, "OK");
            }
            _ => panic!("Expected simple string"),
        }
    }

    #[test]
    fn test_parse_error() {
        let parser = Resp2Parser::new();
        let input = b"-Error message\r\n";

        let result = parser.parse(input).unwrap();
        match result {
            ParseResult::Complete(RespValue::Error(e), _) => {
                assert_eq!(e, "Error message");
            }
            _ => panic!("Expected error"),
        }
    }

    #[test]
    fn test_parse_integer() {
        let parser = Resp2Parser::new();
        let input = b":1000\r\n";

        let result = parser.parse(input).unwrap();
        match result {
            ParseResult::Complete(RespValue::Integer(n), _) => {
                assert_eq!(n, 1000);
            }
            _ => panic!("Expected integer"),
        }
    }

    #[test]
    fn test_parse_negative_integer() {
        let parser = Resp2Parser::new();
        let input = b":-42\r\n";

        let result = parser.parse(input).unwrap();
        match result {
            ParseResult::Complete(RespValue::Integer(n), _) => {
                assert_eq!(n, -42);
            }
            _ => panic!("Expected negative integer"),
        }
    }

    #[test]
    fn test_parse_bulk_string() {
        let parser = Resp2Parser::new();
        let input = b"$6\r\nfoobar\r\n";

        let result = parser.parse(input).unwrap();
        match result {
            ParseResult::Complete(RespValue::BulkString(Some(data)), _) => {
                assert_eq!(data.as_ref(), b"foobar");
            }
            _ => panic!("Expected bulk string"),
        }
    }

    #[test]
    fn test_parse_null_bulk_string() {
        let parser = Resp2Parser::new();
        let input = b"$-1\r\n";

        let result = parser.parse(input).unwrap();
        match result {
            ParseResult::Complete(RespValue::BulkString(None), _) => {}
            _ => panic!("Expected null bulk string"),
        }
    }

    #[test]
    fn test_parse_empty_bulk_string() {
        let parser = Resp2Parser::new();
        let input = b"$0\r\n\r\n";

        let result = parser.parse(input).unwrap();
        match result {
            ParseResult::Complete(RespValue::BulkString(Some(data)), _) => {
                assert!(data.is_empty());
            }
            _ => panic!("Expected empty bulk string"),
        }
    }

    #[test]
    fn test_parse_array() {
        let parser = Resp2Parser::new();
        let input = b"*3\r\n$3\r\nfoo\r\n$3\r\nbar\r\n$-1\r\n";

        let result = parser.parse(input).unwrap();
        match result {
            ParseResult::Complete(RespValue::Array(Some(arr)), _) => {
                assert_eq!(arr.len(), 3);
                match &arr[0] {
                    RespValue::BulkString(Some(data)) => assert_eq!(data.as_ref(), b"foo"),
                    _ => panic!("Expected first bulk string"),
                }
                match &arr[1] {
                    RespValue::BulkString(Some(data)) => assert_eq!(data.as_ref(), b"bar"),
                    _ => panic!("Expected second bulk string"),
                }
                match &arr[2] {
                    RespValue::BulkString(None) => {}
                    _ => panic!("Expected null bulk string"),
                }
            }
            _ => panic!("Expected array"),
        }
    }

    #[test]
    fn test_parse_null_array() {
        let parser = Resp2Parser::new();
        let input = b"*-1\r\n";

        let result = parser.parse(input).unwrap();
        match result {
            ParseResult::Complete(RespValue::Array(None), _) => {}
            _ => panic!("Expected null array"),
        }
    }

    #[test]
    fn test_parse_empty_array() {
        let parser = Resp2Parser::new();
        let input = b"*0\r\n";

        let result = parser.parse(input).unwrap();
        match result {
            ParseResult::Complete(RespValue::Array(Some(arr)), _) => {
                assert!(arr.is_empty());
            }
            _ => panic!("Expected empty array"),
        }
    }

    #[test]
    fn test_parse_inline_command() {
        let parser = Resp2Parser::new();
        let input = b"ping\r\n";

        let result = parser.parse(input).unwrap();
        match result {
            ParseResult::Complete(RespValue::Inline(args), _) => {
                assert_eq!(args, vec!["ping"]);
            }
            _ => panic!("Expected inline command"),
        }
    }

    #[test]
    fn test_parse_inline_with_args() {
        let parser = Resp2Parser::new();
        let input = b"hmget fruit apple banana watermelon\r\n";

        let result = parser.parse(input).unwrap();
        match result {
            ParseResult::Complete(RespValue::Inline(args), _) => {
                assert_eq!(args, vec!["hmget", "fruit", "apple", "banana", "watermelon"]);
            }
            _ => panic!("Expected inline command with args"),
        }
    }

    #[test]
    fn test_parse_incomplete() {
        let parser = Resp2Parser::new();
        let input = b"$10\r\nfoobar";

        let result = parser.parse(input).unwrap();
        match result {
            ParseResult::Incomplete(_) => {}
            _ => panic!("Expected incomplete result"),
        }
    }

    #[test]
    fn test_parse_fragment() {
        let parser = Resp2Parser::new();
        // First fragment
        let input1 = b"*5\r\n$5\r\nhmget\r\n$5\r\nfruit\r\n$5\r\napple\r\n";
        let result1 = parser.parse(input1).unwrap();
        match result1 {
            ParseResult::Incomplete(_) => {}
            _ => panic!("Expected incomplete result for first fragment"),
        }

        // Complete message
        let input2 = b"*5\r\n$5\r\nhmget\r\n$5\r\nfruit\r\n$5\r\napple\r\n$6\r\nbanana\r\n$10\r\nwatermelon\r\n";
        let result2 = parser.parse(input2).unwrap();
        match result2 {
            ParseResult::Complete(RespValue::Array(Some(arr)), _) => {
                assert_eq!(arr.len(), 5);
                match &arr[0] {
                    RespValue::BulkString(Some(data)) => assert_eq!(data.as_ref(), b"hmget"),
                    _ => panic!("Expected hmget command"),
                }
                match &arr[4] {
                    RespValue::BulkString(Some(data)) => assert_eq!(data.as_ref(), b"watermelon"),
                    _ => panic!("Expected watermelon"),
                }
            }
            _ => panic!("Expected complete array"),
        }
    }

    #[test]
    fn test_parse_pipeline() {
        let parser = Resp2Parser::new();
        let input = b"*3\r\n$3\r\nset\r\n$5\r\nfruit\r\n$5\r\napple\r\n+OK\r\n";

        let (values, _) = parser.parse_multiple(input).unwrap();
        assert_eq!(values.len(), 2);

        match &values[0] {
            RespValue::Array(Some(arr)) => {
                assert_eq!(arr.len(), 3);
                match &arr[0] {
                    RespValue::BulkString(Some(data)) => assert_eq!(data.as_ref(), b"set"),
                    _ => panic!("Expected set command"),
                }
            }
            _ => panic!("Expected array for first command"),
        }

        match &values[1] {
            RespValue::SimpleString(s) => assert_eq!(s, "OK"),
            _ => panic!("Expected OK response"),
        }
    }
}