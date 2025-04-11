use crate::storage::base_data_value_format::*;

#[test]
fn test_data_type_conversion() {
    // Test DataType conversion
    assert_eq!(DataType::String.to_char(), 'k');
    assert_eq!(DataType::Hash.to_char(), 'h');
    assert_eq!(DataType::List.to_char(), 'l');
    assert_eq!(DataType::Set.to_char(), 's');
    assert_eq!(DataType::Zset.to_char(), 'z');
    assert_eq!(DataType::All.to_char(), 'a');

    assert_eq!(DataType::from_char('k'), Some(DataType::String));
    assert_eq!(DataType::from_char('h'), Some(DataType::Hash));
    assert_eq!(DataType::from_char('l'), Some(DataType::List));
    assert_eq!(DataType::from_char('s'), Some(DataType::Set));
    assert_eq!(DataType::from_char('z'), Some(DataType::Zset));
    assert_eq!(DataType::from_char('a'), Some(DataType::All));
    assert_eq!(DataType::from_char('x'), None);

    assert_eq!(DataType::String.to_string(), "string");
    assert_eq!(DataType::Hash.to_string(), "hash");
    assert_eq!(DataType::List.to_string(), "list");
    assert_eq!(DataType::Set.to_string(), "set");
    assert_eq!(DataType::Zset.to_string(), "zset");
    assert_eq!(DataType::All.to_string(), "all");
}

#[test]
fn test_encode_type_conversion() {
    // Test EncodeType conversion
    assert_eq!(EncodeType::String.to_char(), 'k');
    assert_eq!(EncodeType::Hash.to_char(), 'h');
    assert_eq!(EncodeType::List.to_char(), 'l');
    assert_eq!(EncodeType::Set.to_char(), 's');
    assert_eq!(EncodeType::Zset.to_char(), 'z');

    assert_eq!(EncodeType::from_char('k'), Some(EncodeType::String));
    assert_eq!(EncodeType::from_char('h'), Some(EncodeType::Hash));
    assert_eq!(EncodeType::from_char('l'), Some(EncodeType::List));
    assert_eq!(EncodeType::from_char('s'), Some(EncodeType::Set));
    assert_eq!(EncodeType::from_char('z'), Some(EncodeType::Zset));
    assert_eq!(EncodeType::from_char('x'), None);
}

#[test]
fn test_value_type_conversion() {
    // Test ValueType conversion
    assert_eq!(ValueType::Deletion.to_u8(), 0);
    assert_eq!(ValueType::Value.to_u8(), 1);
    assert_eq!(ValueType::Merge.to_u8(), 2);
    assert_eq!(ValueType::Other.to_u8(), 3);

    assert_eq!(ValueType::from_u8(0), Some(ValueType::Deletion));
    assert_eq!(ValueType::from_u8(1), Some(ValueType::Value));
    assert_eq!(ValueType::from_u8(2), Some(ValueType::Merge));
    assert_eq!(ValueType::from_u8(3), Some(ValueType::Other));
    assert_eq!(ValueType::from_u8(4), None);
}
