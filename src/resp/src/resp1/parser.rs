use crate::{
    parser::{utils, ParseResult, RespParser},
    types::{RespValue, RespVersion},
    Result,
};
use bytes::Bytes;
use nom::{
    character::complete::not_line_ending
    ,
    sequence::terminated,
    IResult, Parser,
};

/// RESP1 protocol parser
///
/// RESP1 primarily handles inline commands and simple string responses.
/// This parser focuses on basic protocol compatibility.
pub struct Resp1Parser;

impl Resp1Parser {
    pub fn new() -> Self {
        Self
    }

    /// Parse a RESP1 message
    fn parse_resp1(input: &[u8]) -> IResult<&[u8], RespValue> {
        // RESP1 is primarily inline commands
        let (input, line) = terminated(not_line_ending, utils::crlf).parse(input)?;
        let line_str = std::str::from_utf8(line)
            .map_err(|_| nom::Err::Error(nom::error::Error::new(input, nom::error::ErrorKind::Char)))?;

        // Split the line into command arguments
        let args = utils::split_inline_command(line_str);

        if args.is_empty() {
            Ok((input, RespValue::Inline(vec![])))
        } else {
            Ok((input, RespValue::Inline(args)))
        }
    }
}

impl Default for Resp1Parser {
    fn default() -> Self {
        Self::new()
    }
}

impl RespParser for Resp1Parser {
    fn parse(&self, input: &[u8]) -> Result<ParseResult> {
        match Self::parse_resp1(input) {
            Ok((remaining, value)) => {
                Ok(ParseResult::Complete(value, Bytes::copy_from_slice(remaining)))
            }
            Err(nom::Err::Incomplete(_)) => {
                Ok(ParseResult::Incomplete(1))
            }
            Err(nom::Err::Error(e)) | Err(nom::Err::Failure(e)) => {
                // Check if the error is due to incomplete line ending or EOF
                match e.code {
                    nom::error::ErrorKind::CrLf | nom::error::ErrorKind::Eof => {
                        Ok(ParseResult::Incomplete(1))
                    }
                    _ => Err(utils::nom_error_to_resp_error(nom::Err::Error(e))),
                }
            }
        }
    }

    fn version(&self) -> RespVersion {
        RespVersion::Resp1
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_inline_command() {
        let parser = Resp1Parser::new();
        let input = b"PING\r\n";

        let result = parser.parse(input).unwrap();
        match result {
            ParseResult::Complete(RespValue::Inline(args), _) => {
                assert_eq!(args, vec!["PING"]);
            }
            _ => panic!("Expected inline command"),
        }
    }

    #[test]
    fn test_parse_command_with_args() {
        let parser = Resp1Parser::new();
        let input = b"GET mykey\r\n";

        let result = parser.parse(input).unwrap();
        match result {
            ParseResult::Complete(RespValue::Inline(args), _) => {
                assert_eq!(args, vec!["GET", "mykey"]);
            }
            _ => panic!("Expected inline command with arguments"),
        }
    }

    #[test]
    fn test_parse_multiple_args() {
        let parser = Resp1Parser::new();
        let input = b"SET key value EX 30\r\n";

        let result = parser.parse(input).unwrap();
        match result {
            ParseResult::Complete(RespValue::Inline(args), _) => {
                assert_eq!(args, vec!["SET", "key", "value", "EX", "30"]);
            }
            _ => panic!("Expected inline command with multiple arguments"),
        }
    }

    #[test]
    fn test_parse_empty_line() {
        let parser = Resp1Parser::new();
        let input = b"\r\n";

        let result = parser.parse(input).unwrap();
        match result {
            ParseResult::Complete(RespValue::Inline(args), _) => {
                assert!(args.is_empty());
            }
            _ => panic!("Expected empty inline command"),
        }
    }

    #[test]
    fn test_parse_incomplete() {
        let parser = Resp1Parser::new();
        let input = b"PING";

        let result = parser.parse(input).unwrap();
        match result {
            ParseResult::Incomplete(_) => {}
            _ => panic!("Expected incomplete result"),
        }
    }

    #[test]
    fn test_parse_multiple_commands() {
        let parser = Resp1Parser::new();
        let input = b"PING\r\nGET key\r\n";

        let (values, _) = parser.parse_multiple(input).unwrap();
        assert_eq!(values.len(), 2);

        match &values[0] {
            RespValue::Inline(args) => assert_eq!(args, &vec!["PING"]),
            _ => panic!("Expected first inline command"),
        }

        match &values[1] {
            RespValue::Inline(args) => assert_eq!(args, &vec!["GET", "key"]),
            _ => panic!("Expected second inline command"),
        }
    }
}