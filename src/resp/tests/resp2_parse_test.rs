use crate::resp::{Parse, RespData, RespParse, RespParseResult, RespVersion};
use bytes::Bytes;

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
