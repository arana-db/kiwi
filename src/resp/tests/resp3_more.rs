use bytes::Bytes;
use resp::{RespData, RespVersion, decode_many, encode_many, new_decoder, new_encoder};

#[test]
fn bulk_error_roundtrip() {
    let mut enc = new_encoder(RespVersion::RESP3);
    let data = RespData::BulkError(Bytes::from("ERR something"));
    let bytes = encode_many(&mut *enc, &[data.clone()]).unwrap();
    let mut dec = new_decoder(RespVersion::RESP3);
    let out = decode_many(&mut *dec, bytes);
    match out[0].as_ref().unwrap() {
        RespData::BulkError(s) => assert_eq!(s.as_ref(), b"ERR something"),
        _ => panic!(),
    }
}

#[test]
fn verbatim_string_roundtrip() {
    let mut enc = new_encoder(RespVersion::RESP3);
    let data = RespData::VerbatimString {
        format: *b"txt",
        data: Bytes::from("hello"),
    };
    let bytes = encode_many(&mut *enc, &[data]).unwrap();
    let mut dec = new_decoder(RespVersion::RESP3);
    let out = decode_many(&mut *dec, bytes);
    match out[0].as_ref().unwrap() {
        RespData::VerbatimString { format, data } => {
            assert_eq!(format, b"txt");
            assert_eq!(data.as_ref(), b"hello");
        }
        _ => panic!(),
    }
}

#[test]
fn bignumber_roundtrip() {
    let mut enc = new_encoder(RespVersion::RESP3);
    let data = RespData::BigNumber("12345678901234567890".into());
    let bytes = encode_many(&mut *enc, &[data.clone()]).unwrap();
    let mut dec = new_decoder(RespVersion::RESP3);
    let out = decode_many(&mut *dec, bytes);
    match out[0].as_ref().unwrap() {
        RespData::BigNumber(s) => assert_eq!(s, "12345678901234567890"),
        _ => panic!(),
    }
}
