use bytes::Bytes;
use resp::{RespData, RespVersion, decode_many, new_decoder};

#[test]
fn parse_simple_string_ok() {
    let mut dec = new_decoder(RespVersion::RESP2);
    let out = decode_many(&mut *dec, Bytes::from("+OK\r\n"));
    assert!(out.len() >= 1);
    let v = out[0].as_ref().unwrap();
    match v {
        RespData::SimpleString(s) => assert_eq!(s.as_ref(), b"OK"),
        _ => panic!(),
    }
}

#[test]
fn parse_integer() {
    let mut dec = new_decoder(RespVersion::RESP2);
    let out = decode_many(&mut *dec, Bytes::from(":1000\r\n"));
    let v = out[0].as_ref().unwrap();
    match v {
        RespData::Integer(n) => assert_eq!(*n, 1000),
        _ => panic!(),
    }
}

#[test]
fn parse_bulk_and_array() {
    let mut dec = new_decoder(RespVersion::RESP2);
    let out = decode_many(&mut *dec, Bytes::from("*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n"));
    let v = out[0].as_ref().unwrap();
    match v {
        RespData::Array(Some(items)) => {
            assert_eq!(items.len(), 2);
        }
        _ => panic!(),
    }
}
