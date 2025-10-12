use bytes::Bytes;
use resp::{RespData, RespVersion, new_decoder};

#[test]
fn resp3_boolean_and_null() {
    let mut dec = new_decoder(RespVersion::RESP3);
    dec.push(Bytes::from("#t\r\n"));
    dec.push(Bytes::from("_\r\n"));
    
    // Verify Boolean(true) parsing
    let result1 = dec.next().unwrap().unwrap();
    match result1 {
        RespData::Boolean(true) => {},
        _ => panic!("expected Boolean(true), got {:?}", result1),
    }
    
    // Verify Null parsing
    let result2 = dec.next().unwrap().unwrap();
    match result2 {
        RespData::Null => {},
        _ => panic!("expected Null, got {:?}", result2),
    }
}
