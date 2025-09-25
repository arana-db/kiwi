#[cfg(test)]
mod cursor_management_test {
    use storage::{DataType, storage::Storage};

    #[test]
    fn test_store_and_load_cursor_basic() {
        let storage = Storage::new(1, 0);

        // Test basic storage and load
        let result =
            storage.store_cursor_start_key(DataType::Set, 12345, 's', "test_key_001".to_string());
        assert!(result.is_ok());

        let mut cursor_type = '\0';
        let mut start_key = String::new();
        let result =
            storage.load_cursor_start_key(DataType::Set, 12345, &mut cursor_type, &mut start_key);

        assert!(result.is_ok());
        assert_eq!(cursor_type, 's');
        assert_eq!(start_key, "test_key_001");
    }

    #[test]
    fn test_load_nonexist_cursor() {
        let storage = Storage::new(1, 0);

        let mut cursor_type = '\0';
        let mut start_key = String::new();
        let result =
            storage.load_cursor_start_key(DataType::Set, 99999, &mut cursor_type, &mut start_key);

        assert!(result.is_err());
    }

    #[test]
    fn test_multiple_data_types() {
        let storage = Storage::new(1, 0);

        // Test different data types
        let test_cases = vec![
            (DataType::String, 'k', "string_key"),
            (DataType::Hash, 'h', "hash_key"),
            (DataType::Set, 's', "set_key"),
            (DataType::List, 'l', "list_key"),
            (DataType::ZSet, 'z', "zset_key"),
        ];

        for (dtype, expected_type, key) in test_cases {
            storage
                .store_cursor_start_key(dtype, 1001, expected_type, key.to_string())
                .unwrap();

            let mut cursor_type = '\0';
            let mut start_key = String::new();
            storage
                .load_cursor_start_key(dtype, 1001, &mut cursor_type, &mut start_key)
                .unwrap();

            assert_eq!(cursor_type, expected_type);
            assert_eq!(start_key, key);
        }
    }
}
