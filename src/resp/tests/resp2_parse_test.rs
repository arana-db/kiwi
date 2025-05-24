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
