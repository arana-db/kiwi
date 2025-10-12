use bytes::Bytes;
use resp::{RespVersion, new_decoder, new_encoder};

#[test]
fn selects_resp1_impl() {
    let mut dec = new_decoder(RespVersion::RESP1);
    let enc = new_encoder(RespVersion::RESP1);
    assert_eq!(dec.version(), RespVersion::RESP1);
    assert_eq!(enc.version(), RespVersion::RESP1);

    // minimal smoke: inline ping
    dec.push(Bytes::from("PING\r\n"));
    // even if command extraction differs, API shape should not panic
    let _ = dec.next();
}

#[test]
fn selects_resp2_impl() {
    let mut dec = new_decoder(RespVersion::RESP2);
    let enc = new_encoder(RespVersion::RESP2);
    assert_eq!(dec.version(), RespVersion::RESP2);
    assert_eq!(enc.version(), RespVersion::RESP2);

    // minimal smoke: +OK\r\n
    dec.push(Bytes::from("+OK\r\n"));
    let _ = dec.next();
}
