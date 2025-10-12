use bytes::Bytes;
use resp::{RespData, RespVersion, decode_many, new_decoder};

#[test]
fn inline_ping() {
    let mut dec = new_decoder(RespVersion::RESP1);
    let out = decode_many(&mut *dec, Bytes::from("PING\r\n"));
    // Not required to convert to command, just verify no crash and produces a frame
    assert!(out.len() >= 1);
}

#[test]
fn simple_string_ok() {
    let mut dec = new_decoder(RespVersion::RESP1);
    let out = decode_many(&mut *dec, Bytes::from("+OK\r\n"));
    let v = out[0].as_ref().unwrap();
    match v {
        RespData::SimpleString(s) => assert_eq!(s.as_ref(), b"OK"),
        _ => panic!(),
    }
}
