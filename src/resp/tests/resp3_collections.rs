use bytes::Bytes;
use resp::{RespData, RespVersion, decode_many, encode_many, new_decoder, new_encoder};

#[test]
fn set_roundtrip() {
    let mut enc = new_encoder(RespVersion::RESP3);
    let data = RespData::Set(vec![
        RespData::Boolean(true),
        RespData::Null,
        RespData::Double(2.5),
    ]);
    let bytes = encode_many(&mut *enc, &[data]).unwrap();
    let mut dec = new_decoder(RespVersion::RESP3);
    let out = decode_many(&mut *dec, bytes);
    match out[0].as_ref().unwrap() {
        RespData::Set(items) => assert_eq!(items.len(), 3),
        _ => panic!(),
    }
}

#[test]
fn map_roundtrip() {
    let mut enc = new_encoder(RespVersion::RESP3);
    let data = RespData::Map(vec![
        (RespData::Boolean(true), RespData::Double(1.0)),
        (RespData::Null, RespData::BulkError(Bytes::from("ERR x"))),
    ]);
    let bytes = encode_many(&mut *enc, &[data]).unwrap();
    let mut dec = new_decoder(RespVersion::RESP3);
    let out = decode_many(&mut *dec, bytes);
    match out[0].as_ref().unwrap() {
        RespData::Map(entries) => assert_eq!(entries.len(), 2),
        _ => panic!(),
    }
}

#[test]
fn push_roundtrip() {
    let mut enc = new_encoder(RespVersion::RESP3);
    let data = RespData::Push(vec![RespData::Boolean(false), RespData::Double(3.14)]);
    let bytes = encode_many(&mut *enc, &[data]).unwrap();
    let mut dec = new_decoder(RespVersion::RESP3);
    let out = decode_many(&mut *dec, bytes);
    match out[0].as_ref().unwrap() {
        RespData::Push(items) => assert_eq!(items.len(), 2),
        _ => panic!(),
    }
}
