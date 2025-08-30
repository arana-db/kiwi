use crate::{
    parser::{utils, ParseResult, RespParser},
    types::{RespValue, RespVersion},
    Result,
};
use bytes::Bytes;
use nom::{
    bytes::complete::{tag, take},
    character::complete::not_line_ending
    ,
    sequence::terminated,
    IResult, Parser,
};
use std::collections::HashMap;

/// RESP3 protocol parser
///
/// Implements the enhanced RESP3 protocol with support for all RESP2 types plus:
/// - Null values (_)
/// - Boolean values (#)
/// - Double values (,)
/// - Big numbers (()
/// - Bulk errors (!)
/// - Verbatim strings (=)
/// - Maps (%)
/// - Sets (~)
/// - Push messages (>)
pub struct Resp3Parser;

impl Resp3Parser {
    pub fn new() -> Self {
        Self
    }

    /// Parse a RESP3 message
    fn parse_resp3(input: &[u8]) -> IResult<&[u8], RespValue> {
        if input.is_empty() {
            return Err(nom::Err::Incomplete(nom::Needed::Size(std::num::NonZero::new(1).unwrap())));
        }

        match input[0] {
            // RESP2 compatible types
            b'+' => Self::parse_simple_string(input),
            b'-' => Self::parse_error(input),
            b':' => Self::parse_integer(input),
            b'$' => Self::parse_bulk_string(input),
            b'*' => Self::parse_array(input),

            // RESP3 specific types
            b'_' => Self::parse_null(input),
            b'#' => Self::parse_boolean(input),
            b',' => Self::parse_double(input),
            b'(' => Self::parse_big_number(input),
            b'!' => Self::parse_bulk_error(input),
            b'=' => Self::parse_verbatim_string(input),
            b'%' => Self::parse_map(input),
            b'~' => Self::parse_set(input),
            b'>' => Self::parse_push(input),

            // Fallback to inline
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
            let (new_remaining, element) = Self::parse_resp3(remaining)?;
            elements.push(element);
            remaining = new_remaining;
        }

        Ok((remaining, RespValue::Array(Some(elements))))
    }

    /// Parse null (_\r\n)
    fn parse_null(input: &[u8]) -> IResult<&[u8], RespValue> {
        let (input, _) = tag(&b"_"[..]).parse(input)?;
        let (input, _) = utils::crlf(input)?;

        Ok((input, RespValue::Null))
    }

    /// Parse boolean (#t\r\n or #f\r\n)
    fn parse_boolean(input: &[u8]) -> IResult<&[u8], RespValue> {
        let (input, _) = tag(&b"#"[..]).parse(input)?;
        let (input, value) = terminated(not_line_ending, utils::crlf).parse(input)?;

        let boolean_value = match value {
            b"t" => true,
            b"f" => false,
            _ => return Err(nom::Err::Error(nom::error::Error::new(input, nom::error::ErrorKind::Char))),
        };

        Ok((input, RespValue::Boolean(boolean_value)))
    }

    /// Parse double (,3.14159\r\n or ,inf\r\n or ,-inf\r\n or ,nan\r\n)
    fn parse_double(input: &[u8]) -> IResult<&[u8], RespValue> {
        let (input, _) = tag(&b","[..]).parse(input)?;
        let (input, value_str) = terminated(not_line_ending, utils::crlf).parse(input)?;
        let value_str = std::str::from_utf8(value_str)
            .map_err(|_| nom::Err::Error(nom::error::Error::new(input, nom::error::ErrorKind::Char)))?;

        let double_value = match value_str {
            "inf" => f64::INFINITY,
            "-inf" => f64::NEG_INFINITY,
            "nan" => f64::NAN,
            s => s.parse::<f64>()
                .map_err(|_| nom::Err::Error(nom::error::Error::new(input, nom::error::ErrorKind::Float)))?,
        };

        Ok((input, RespValue::Double(double_value)))
    }

    /// Parse big number ((123456789012345678901234567890\r\n)
    fn parse_big_number(input: &[u8]) -> IResult<&[u8], RespValue> {
        let (input, _) = tag(&b"("[..]).parse(input)?;
        let (input, number_str) = terminated(not_line_ending, utils::crlf).parse(input)?;
        let number_str = std::str::from_utf8(number_str)
            .map_err(|_| nom::Err::Error(nom::error::Error::new(input, nom::error::ErrorKind::Char)))?;

        Ok((input, RespValue::BigNumber(number_str.to_string())))
    }

    /// Parse bulk error (!21\r\nSYNTAX invalid syntax\r\n)
    fn parse_bulk_error(input: &[u8]) -> IResult<&[u8], RespValue> {
        let (input, _) = tag(&b"!"[..]).parse(input)?;
        let (input, length) = terminated(utils::parse_integer, utils::crlf).parse(input)?;

        if length < 0 {
            return Err(nom::Err::Error(nom::error::Error::new(input, nom::error::ErrorKind::LengthValue)));
        }

        let (input, content) = take(length as usize)(input)?;
        let (input, _) = utils::crlf(input)?;

        Ok((input, RespValue::BulkError(Bytes::copy_from_slice(content))))
    }

    /// Parse verbatim string (=15\r\ntxt:Some string\r\n)
    fn parse_verbatim_string(input: &[u8]) -> IResult<&[u8], RespValue> {
        let (input, _) = tag(&b"="[..]).parse(input)?;
        let (input, length) = terminated(utils::parse_integer, utils::crlf).parse(input)?;

        if length < 4 {
            return Err(nom::Err::Error(nom::error::Error::new(input, nom::error::ErrorKind::LengthValue)));
        }

        let (input, content) = take(length as usize)(input)?;
        let (input, _) = utils::crlf(input)?;

        let content_str = std::str::from_utf8(content)
            .map_err(|_| nom::Err::Error(nom::error::Error::new(input, nom::error::ErrorKind::Char)))?;

        // Format is "fmt:data"
        let parts: Vec<&str> = content_str.splitn(2, ':').collect();
        if parts.len() != 2 {
            return Err(nom::Err::Error(nom::error::Error::new(input, nom::error::ErrorKind::Char)));
        }

        Ok((input, RespValue::VerbatimString {
            format: parts[0].to_string(),
            data: parts[1].to_string(),
        }))
    }

    /// Parse map (%2\r\n+first\r\n:1\r\n+second\r\n:2\r\n)
    fn parse_map(input: &[u8]) -> IResult<&[u8], RespValue> {
        let (input, _) = tag(&b"%"[..]).parse(input)?;
        let (input, length) = terminated(utils::parse_integer, utils::crlf).parse(input)?;

        if length < 0 {
            return Err(nom::Err::Error(nom::error::Error::new(input, nom::error::ErrorKind::LengthValue)));
        }

        let mut map = HashMap::new();
        let mut remaining = input;

        for _ in 0..length {
            let (new_remaining, key) = Self::parse_resp3(remaining)?;
            let (new_remaining, value) = Self::parse_resp3(new_remaining)?;
            map.insert(key, value);
            remaining = new_remaining;
        }

        Ok((remaining, RespValue::Map(map)))
    }

    /// Parse set (~3\r\n+apple\r\n+orange\r\n+banana\r\n)
    fn parse_set(input: &[u8]) -> IResult<&[u8], RespValue> {
        let (input, _) = tag(&b"~"[..]).parse(input)?;
        let (input, length) = terminated(utils::parse_integer, utils::crlf).parse(input)?;

        if length < 0 {
            return Err(nom::Err::Error(nom::error::Error::new(input, nom::error::ErrorKind::LengthValue)));
        }

        let mut elements = Vec::with_capacity(length as usize);
        let mut remaining = input;

        for _ in 0..length {
            let (new_remaining, element) = Self::parse_resp3(remaining)?;
            elements.push(element);
            remaining = new_remaining;
        }

        Ok((remaining, RespValue::Set(elements)))
    }

    /// Parse push message (>2\r\n+pubsub\r\n+message\r\n)
    fn parse_push(input: &[u8]) -> IResult<&[u8], RespValue> {
        let (input, _) = tag(&b">"[..]).parse(input)?;
        let (input, length) = terminated(utils::parse_integer, utils::crlf).parse(input)?;

        if length < 0 {
            return Err(nom::Err::Error(nom::error::Error::new(input, nom::error::ErrorKind::LengthValue)));
        }

        let mut elements = Vec::with_capacity(length as usize);
        let mut remaining = input;

        for _ in 0..length {
            let (new_remaining, element) = Self::parse_resp3(remaining)?;
            elements.push(element);
            remaining = new_remaining;
        }

        Ok((remaining, RespValue::Push(elements)))
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

impl Default for Resp3Parser {
    fn default() -> Self {
        Self::new()
    }
}

impl RespParser for Resp3Parser {
    fn parse(&self, input: &[u8]) -> Result<ParseResult> {
        match Self::parse_resp3(input) {
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
        RespVersion::Resp3
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_null() {
        let parser = Resp3Parser::new();
        let input = b"_\r\n";

        let result = parser.parse(input).unwrap();
        match result {
            ParseResult::Complete(RespValue::Null, _) => {}
            _ => panic!("Expected null value"),
        }
    }

    #[test]
    fn test_parse_boolean_true() {
        let parser = Resp3Parser::new();
        let input = b"#t\r\n";

        let result = parser.parse(input).unwrap();
        match result {
            ParseResult::Complete(RespValue::Boolean(true), _) => {}
            _ => panic!("Expected boolean true"),
        }
    }

    #[test]
    fn test_parse_boolean_false() {
        let parser = Resp3Parser::new();
        let input = b"#f\r\n";

        let result = parser.parse(input).unwrap();
        match result {
            ParseResult::Complete(RespValue::Boolean(false), _) => {}
            _ => panic!("Expected boolean false"),
        }
    }

    #[test]
    fn test_parse_double() {
        let parser = Resp3Parser::new();
        let input = b",3.14159\r\n";

        let result = parser.parse(input).unwrap();
        match result {
            ParseResult::Complete(RespValue::Double(d), _) => {
                assert!((d - 3.14159).abs() < f64::EPSILON);
            }
            _ => panic!("Expected double value"),
        }
    }

    #[test]
    fn test_parse_double_infinity() {
        let parser = Resp3Parser::new();
        let input = b",inf\r\n";

        let result = parser.parse(input).unwrap();
        match result {
            ParseResult::Complete(RespValue::Double(d), _) => {
                assert!(d.is_infinite() && d.is_sign_positive());
            }
            _ => panic!("Expected positive infinity"),
        }
    }

    #[test]
    fn test_parse_double_negative_infinity() {
        let parser = Resp3Parser::new();
        let input = b",-inf\r\n";

        let result = parser.parse(input).unwrap();
        match result {
            ParseResult::Complete(RespValue::Double(d), _) => {
                assert!(d.is_infinite() && d.is_sign_negative());
            }
            _ => panic!("Expected negative infinity"),
        }
    }

    #[test]
    fn test_parse_double_nan() {
        let parser = Resp3Parser::new();
        let input = b",nan\r\n";

        let result = parser.parse(input).unwrap();
        match result {
            ParseResult::Complete(RespValue::Double(d), _) => {
                assert!(d.is_nan());
            }
            _ => panic!("Expected NaN"),
        }
    }

    #[test]
    fn test_parse_big_number() {
        let parser = Resp3Parser::new();
        let input = b"(123456789012345678901234567890\r\n";

        let result = parser.parse(input).unwrap();
        match result {
            ParseResult::Complete(RespValue::BigNumber(n), _) => {
                assert_eq!(n, "123456789012345678901234567890");
            }
            _ => panic!("Expected big number"),
        }
    }

    #[test]
    fn test_parse_bulk_error() {
        let parser = Resp3Parser::new();
        let input = b"!21\r\nSYNTAX invalid syntax\r\n";

        let result = parser.parse(input).unwrap();
        match result {
            ParseResult::Complete(RespValue::BulkError(data), _) => {
                assert_eq!(data.as_ref(), b"SYNTAX invalid syntax");
            }
            _ => panic!("Expected bulk error"),
        }
    }

    #[test]
    fn test_parse_verbatim_string() {
        let parser = Resp3Parser::new();
        let input = b"=15\r\ntxt:Some string\r\n";

        let result = parser.parse(input).unwrap();
        match result {
            ParseResult::Complete(RespValue::VerbatimString { format, data }, _) => {
                assert_eq!(format, "txt");
                assert_eq!(data, "Some string");
            }
            _ => panic!("Expected verbatim string"),
        }
    }

    #[test]
    fn test_parse_map() {
        let parser = Resp3Parser::new();
        let input = b"%2\r\n+first\r\n:1\r\n+second\r\n:2\r\n";

        let result = parser.parse(input).unwrap();
        match result {
            ParseResult::Complete(RespValue::Map(map), _) => {
                assert_eq!(map.len(), 2);

                let first_key = RespValue::SimpleString("first".to_string());
                let first_value = map.get(&first_key).unwrap();
                assert_eq!(*first_value, RespValue::Integer(1));

                let second_key = RespValue::SimpleString("second".to_string());
                let second_value = map.get(&second_key).unwrap();
                assert_eq!(*second_value, RespValue::Integer(2));
            }
            _ => panic!("Expected map"),
        }
    }

    #[test]
    fn test_parse_set() {
        let parser = Resp3Parser::new();
        let input = b"~3\r\n+apple\r\n+orange\r\n+banana\r\n";

        let result = parser.parse(input).unwrap();
        match result {
            ParseResult::Complete(RespValue::Set(elements), _) => {
                assert_eq!(elements.len(), 3);
                assert!(elements.contains(&RespValue::SimpleString("apple".to_string())));
                assert!(elements.contains(&RespValue::SimpleString("orange".to_string())));
                assert!(elements.contains(&RespValue::SimpleString("banana".to_string())));
            }
            _ => panic!("Expected set"),
        }
    }

    #[test]
    fn test_parse_push() {
        let parser = Resp3Parser::new();
        let input = b">2\r\n+pubsub\r\n+message\r\n";

        let result = parser.parse(input).unwrap();
        match result {
            ParseResult::Complete(RespValue::Push(elements), _) => {
                assert_eq!(elements.len(), 2);
                assert_eq!(elements[0], RespValue::SimpleString("pubsub".to_string()));
                assert_eq!(elements[1], RespValue::SimpleString("message".to_string()));
            }
            _ => panic!("Expected push message"),
        }
    }

    #[test]
    fn test_parse_resp2_compatibility() {
        let parser = Resp3Parser::new();

        // Test all RESP2 types work in RESP3 parser
        let inputs = vec![
            (b"+OK\r\n".as_ref(), "simple string"),
            (b"-ERR message\r\n".as_ref(), "error"),
            (b":42\r\n".as_ref(), "integer"),
            (b"$5\r\nhello\r\n".as_ref(), "bulk string"),
            (b"*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n".as_ref(), "array"),
            (b"ping\r\n".as_ref(), "inline"),
        ];

        for (input, type_name) in inputs {
            let result = parser.parse(input).unwrap();
            match result {
                ParseResult::Complete(_, _) => {}
                _ => panic!("Failed to parse {} in RESP3", type_name),
            }
        }
    }
}