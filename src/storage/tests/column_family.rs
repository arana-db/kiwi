use crate::storage::column_family::*;

#[test]
fn test_cf_type_name_conversion() {
    assert_eq!(
        ColumnFamilyManager::get_cf_type(CF_META),
        ColumnFamilyType::Meta
    );
    assert_eq!(
        ColumnFamilyManager::get_cf_type(CF_DATA),
        ColumnFamilyType::Data
    );
    assert_eq!(
        ColumnFamilyManager::get_cf_type(CF_DEFAULT),
        ColumnFamilyType::MetaAndData
    );

    assert_eq!(
        ColumnFamilyManager::get_cf_name(ColumnFamilyType::Meta),
        CF_META
    );
    assert_eq!(
        ColumnFamilyManager::get_cf_name(ColumnFamilyType::Data),
        CF_DATA
    );
    assert_eq!(
        ColumnFamilyManager::get_cf_name(ColumnFamilyType::MetaAndData),
        CF_DEFAULT
    );
}
