use resp::{
    BooleanMode, DoubleMode, DownlevelPolicy, MapMode, RespData, RespVersion,
    new_encoder_with_policy,
};

#[test]
fn boolean_as_simplestring() {
    let policy = DownlevelPolicy {
        boolean_mode: BooleanMode::SimpleString,
        ..Default::default()
    };
    let mut enc = new_encoder_with_policy(RespVersion::RESP2, policy);
    let bytes = resp::encode_many(&mut *enc, &[
        RespData::Boolean(true),
        RespData::Boolean(false),
    ])
    .unwrap();
    let s = String::from_utf8(bytes.to_vec()).unwrap();
    assert!(s.contains("+OK\r\n"));
    assert!(s.contains("+ERR\r\n"));
}

#[test]
fn double_as_integer_if_whole() {
    let policy = DownlevelPolicy {
        double_mode: DoubleMode::IntegerIfWhole,
        ..Default::default()
    };
    let mut enc = new_encoder_with_policy(RespVersion::RESP1, policy);
    let bytes =
        resp::encode_many(&mut *enc, &[RespData::Double(2.0), RespData::Double(2.5)]).unwrap();
    let s = String::from_utf8(bytes.to_vec()).unwrap();
    assert!(s.contains(":2\r\n"));
    assert!(s.contains("2.5"));
}

#[test]
fn map_as_array_of_pairs() {
    let policy = DownlevelPolicy {
        map_mode: MapMode::ArrayOfPairs,
        ..Default::default()
    };
    let mut enc = new_encoder_with_policy(RespVersion::RESP2, policy);
    let data = RespData::Map(vec![(RespData::Boolean(true), RespData::Boolean(false))]);
    let bytes = resp::encode_many(&mut *enc, &[data]).unwrap();
    let s = String::from_utf8(bytes.to_vec()).unwrap();
    // *1\r\n*2\r\n:1\r\n:0\r\n (or simple string, depends on boolean_mode default)
    assert!(s.starts_with("*1\r\n"));
}
