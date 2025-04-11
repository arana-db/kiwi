use crate::storage::lru_cache::*;

#[test]
fn test_set_capacity_case1() {
    let mut lru_cache = LRUCache::with_capacity(15);

    // ***************** Step 1 *****************
    lru_cache.insert("k1", "v1", 1);
    lru_cache.insert("k2", "v2", 2);
    lru_cache.insert("k3", "v3", 3);
    lru_cache.insert("k4", "v4", 4);
    lru_cache.insert("k5", "v5", 5);
    assert_eq!(lru_cache.size(), 5);
    assert_eq!(lru_cache.usage(), 15);
    assert!(lru_cache.lru_and_handle_table_consistent());
    assert!(lru_cache.lru_as_expected(vec![
        ("k5", "v5"),
        ("k4", "v4"),
        ("k3", "v3"),
        ("k2", "v2"),
        ("k1", "v1")
    ]));

    // ***************** Step 2 *****************
    lru_cache.set_capacity(12);
    assert_eq!(lru_cache.size(), 3);
    assert_eq!(lru_cache.usage(), 12);
    assert!(lru_cache.lru_and_handle_table_consistent());
    assert!(lru_cache.lru_as_expected(vec![("k5", "v5"), ("k4", "v4"), ("k3", "v3")]));

    // ***************** Step 3 *****************
    lru_cache.set_capacity(5);
    assert_eq!(lru_cache.size(), 1);
    assert_eq!(lru_cache.usage(), 5);
    assert!(lru_cache.lru_and_handle_table_consistent());
    assert!(lru_cache.lru_as_expected(vec![("k5", "v5")]));

    // ***************** Step 4 *****************
    lru_cache.set_capacity(15);
    assert_eq!(lru_cache.size(), 1);
    assert_eq!(lru_cache.usage(), 5);
    assert!(lru_cache.lru_and_handle_table_consistent());
    assert!(lru_cache.lru_as_expected(vec![("k5", "v5")]));

    // ***************** Step 5 *****************
    lru_cache.set_capacity(1);
    assert_eq!(lru_cache.size(), 0);
    assert_eq!(lru_cache.usage(), 0);
    assert!(lru_cache.lru_and_handle_table_consistent());
    assert!(lru_cache.lru_as_expected(vec![]));
}

#[test]
fn test_lookup_case1() {
    let mut lru_cache = LRUCache::new();
    lru_cache.set_capacity(5);

    // ***************** Step 1 *****************
    lru_cache.insert("k1".to_string(), "v1".to_string(), 1);
    lru_cache.insert("k2".to_string(), "v2".to_string(), 1);
    lru_cache.insert("k3".to_string(), "v3".to_string(), 1);
    lru_cache.insert("k4".to_string(), "v4".to_string(), 1);
    lru_cache.insert("k5".to_string(), "v5".to_string(), 1);
    assert_eq!(lru_cache.size(), 5);
    assert_eq!(lru_cache.usage(), 5);
    assert!(lru_cache.lru_and_handle_table_consistent());
    assert!(lru_cache.lru_as_expected(vec![
        ("k5".to_string(), "v5".to_string()),
        ("k4".to_string(), "v4".to_string()),
        ("k3".to_string(), "v3".to_string()),
        ("k2".to_string(), "v2".to_string()),
        ("k1".to_string(), "v1".to_string()),
    ]));

    // ***************** Step 2 *****************
    let mut value = lru_cache.lookup(&"k3".to_string()).unwrap();
    assert_eq!(value, "v3".to_string());
    assert_eq!(lru_cache.size(), 5);
    assert_eq!(lru_cache.usage(), 5);
    assert!(lru_cache.lru_and_handle_table_consistent());
    assert!(lru_cache.lru_as_expected(vec![
        ("k3".to_string(), "v3".to_string()),
        ("k5".to_string(), "v5".to_string()),
        ("k4".to_string(), "v4".to_string()),
        ("k2".to_string(), "v2".to_string()),
        ("k1".to_string(), "v1".to_string()),
    ]));

    // ***************** Step 3 *****************
    value = lru_cache.lookup(&"k1".to_string()).unwrap();
    assert_eq!(value, "v1".to_string());
    assert_eq!(lru_cache.size(), 5);
    assert_eq!(lru_cache.usage(), 5);
    assert!(lru_cache.lru_and_handle_table_consistent());
    assert!(lru_cache.lru_as_expected(vec![
        ("k1".to_string(), "v1".to_string()),
        ("k3".to_string(), "v3".to_string()),
        ("k5".to_string(), "v5".to_string()),
        ("k4".to_string(), "v4".to_string()),
        ("k2".to_string(), "v2".to_string()),
    ]));

    // ***************** Step 4 *****************
    value = lru_cache.lookup(&"k4".to_string()).unwrap();
    assert_eq!(value, "v4".to_string());
    assert_eq!(lru_cache.size(), 5);
    assert_eq!(lru_cache.usage(), 5);
    assert!(lru_cache.lru_and_handle_table_consistent());
    assert!(lru_cache.lru_as_expected(vec![
        ("k4".to_string(), "v4".to_string()),
        ("k1".to_string(), "v1".to_string()),
        ("k3".to_string(), "v3".to_string()),
        ("k5".to_string(), "v5".to_string()),
        ("k2".to_string(), "v2".to_string()),
    ]));

    // ***************** Step 5 *****************
    value = lru_cache.lookup(&"k5".to_string()).unwrap();
    assert_eq!(value, "v5".to_string());
    assert_eq!(lru_cache.size(), 5);
    assert_eq!(lru_cache.usage(), 5);
    assert!(lru_cache.lru_and_handle_table_consistent());
    assert!(lru_cache.lru_as_expected(vec![
        ("k5".to_string(), "v5".to_string()),
        ("k4".to_string(), "v4".to_string()),
        ("k1".to_string(), "v1".to_string()),
        ("k3".to_string(), "v3".to_string()),
        ("k2".to_string(), "v2".to_string()),
    ]));

    // ***************** Step 6 *****************
    value = lru_cache.lookup(&"k5".to_string()).unwrap();
    assert_eq!(value, "v5".to_string());
    assert_eq!(lru_cache.size(), 5);
    assert_eq!(lru_cache.usage(), 5);
    assert!(lru_cache.lru_and_handle_table_consistent());
    assert!(lru_cache.lru_as_expected(vec![
        ("k5".to_string(), "v5".to_string()),
        ("k4".to_string(), "v4".to_string()),
        ("k1".to_string(), "v1".to_string()),
        ("k3".to_string(), "v3".to_string()),
        ("k2".to_string(), "v2".to_string()),
    ]));
}

#[test]
fn test_insert_case1() {
    let mut lru_cache = LRUCache::new();
    lru_cache.set_capacity(3);

    // ***************** Step 1 *****************
    lru_cache.insert("k1".to_string(), "v1".to_string(), 1);
    assert_eq!(lru_cache.size(), 1);
    assert_eq!(lru_cache.usage(), 1);
    assert!(lru_cache.lru_and_handle_table_consistent());
    assert!(lru_cache.lru_as_expected(vec![("k1".to_string(), "v1".to_string())]));

    // ***************** Step 2 *****************
    lru_cache.insert("k2".to_string(), "v2".to_string(), 1);
    assert_eq!(lru_cache.size(), 2);
    assert_eq!(lru_cache.usage(), 2);
    assert!(lru_cache.lru_and_handle_table_consistent());
    assert!(lru_cache.lru_as_expected(vec![
        ("k2".to_string(), "v2".to_string()),
        ("k1".to_string(), "v1".to_string())
    ]));

    // ***************** Step 3 *****************
    lru_cache.insert("k3".to_string(), "v3".to_string(), 1);
    assert_eq!(lru_cache.size(), 3);
    assert_eq!(lru_cache.usage(), 3);
    assert!(lru_cache.lru_and_handle_table_consistent());
    assert!(lru_cache.lru_as_expected(vec![
        ("k3".to_string(), "v3".to_string()),
        ("k2".to_string(), "v2".to_string()),
        ("k1".to_string(), "v1".to_string())
    ]));

    // ***************** Step 4 *****************
    lru_cache.insert("k4".to_string(), "v4".to_string(), 1);
    assert_eq!(lru_cache.size(), 3);
    assert_eq!(lru_cache.usage(), 3);
    assert!(lru_cache.lru_and_handle_table_consistent());
    assert!(lru_cache.lru_as_expected(vec![
        ("k4".to_string(), "v4".to_string()),
        ("k3".to_string(), "v3".to_string()),
        ("k2".to_string(), "v2".to_string())
    ]));

    // ***************** Step 5 *****************
    lru_cache.insert("k5".to_string(), "v5".to_string(), 1);
    assert_eq!(lru_cache.size(), 3);
    assert_eq!(lru_cache.usage(), 3);
    assert!(lru_cache.lru_and_handle_table_consistent());
    assert!(lru_cache.lru_as_expected(vec![
        ("k5".to_string(), "v5".to_string()),
        ("k4".to_string(), "v4".to_string()),
        ("k3".to_string(), "v3".to_string())
    ]));
}

#[test]
fn test_insert_case2() {
    let mut lru_cache = LRUCache::new();
    lru_cache.set_capacity(5);

    // ***************** Step 1 *****************
    lru_cache.insert("k1".to_string(), "v1".to_string(), 1);
    lru_cache.insert("k2".to_string(), "v2".to_string(), 1);
    lru_cache.insert("k3".to_string(), "v3".to_string(), 1);
    lru_cache.insert("k4".to_string(), "v4".to_string(), 1);
    lru_cache.insert("k5".to_string(), "v5".to_string(), 1);
    assert_eq!(lru_cache.size(), 5);
    assert_eq!(lru_cache.usage(), 5);
    assert!(lru_cache.lru_and_handle_table_consistent());
    assert!(lru_cache.lru_as_expected(vec![
        ("k5".to_string(), "v5".to_string()),
        ("k4".to_string(), "v4".to_string()),
        ("k3".to_string(), "v3".to_string()),
        ("k2".to_string(), "v2".to_string()),
        ("k1".to_string(), "v1".to_string())
    ]));

    // ***************** Step 2 *****************
    lru_cache.insert("k3".to_string(), "v3".to_string(), 1);
    assert_eq!(lru_cache.size(), 5);
    assert_eq!(lru_cache.usage(), 5);
    assert!(lru_cache.lru_and_handle_table_consistent());
    assert!(lru_cache.lru_as_expected(vec![
        ("k3".to_string(), "v3".to_string()),
        ("k5".to_string(), "v5".to_string()),
        ("k4".to_string(), "v4".to_string()),
        ("k2".to_string(), "v2".to_string()),
        ("k1".to_string(), "v1".to_string())
    ]));

    // ***************** Step 3 *****************
    lru_cache.insert("k2".to_string(), "v2".to_string(), 1);
    assert_eq!(lru_cache.size(), 5);
    assert_eq!(lru_cache.usage(), 5);
    assert!(lru_cache.lru_and_handle_table_consistent());
    assert!(lru_cache.lru_as_expected(vec![
        ("k2".to_string(), "v2".to_string()),
        ("k3".to_string(), "v3".to_string()),
        ("k5".to_string(), "v5".to_string()),
        ("k4".to_string(), "v4".to_string()),
        ("k1".to_string(), "v1".to_string())
    ]));

    // ***************** Step 4 *****************
    lru_cache.insert("k1".to_string(), "v1".to_string(), 1);
    assert_eq!(lru_cache.size(), 5);
    assert_eq!(lru_cache.usage(), 5);
    assert!(lru_cache.lru_and_handle_table_consistent());
    assert!(lru_cache.lru_as_expected(vec![
        ("k1".to_string(), "v1".to_string()),
        ("k2".to_string(), "v2".to_string()),
        ("k3".to_string(), "v3".to_string()),
        ("k5".to_string(), "v5".to_string()),
        ("k4".to_string(), "v4".to_string())
    ]));

    // ***************** Step 5 *****************
    lru_cache.insert("k4".to_string(), "v4".to_string(), 1);
    assert_eq!(lru_cache.size(), 5);
    assert_eq!(lru_cache.usage(), 5);
    assert!(lru_cache.lru_and_handle_table_consistent());
    assert!(lru_cache.lru_as_expected(vec![
        ("k4".to_string(), "v4".to_string()),
        ("k1".to_string(), "v1".to_string()),
        ("k2".to_string(), "v2".to_string()),
        ("k3".to_string(), "v3".to_string()),
        ("k5".to_string(), "v5".to_string())
    ]));

    // ***************** Step 6 *****************
    lru_cache.insert("k0".to_string(), "v0".to_string(), 1);
    assert_eq!(lru_cache.size(), 5);
    assert_eq!(lru_cache.usage(), 5);
    assert!(lru_cache.lru_and_handle_table_consistent());
    assert!(lru_cache.lru_as_expected(vec![
        ("k0".to_string(), "v0".to_string()),
        ("k4".to_string(), "v4".to_string()),
        ("k1".to_string(), "v1".to_string()),
        ("k2".to_string(), "v2".to_string()),
        ("k3".to_string(), "v3".to_string())
    ]));
}

#[test]
fn test_insert_case3() {
    let mut lru_cache = LRUCache::new();
    lru_cache.set_capacity(10);

    // ***************** Step 1 *****************
    lru_cache.insert("k1".to_string(), "v1".to_string(), 1);
    assert_eq!(lru_cache.size(), 1);
    assert_eq!(lru_cache.usage(), 1);
    assert!(lru_cache.lru_and_handle_table_consistent());
    assert!(lru_cache.lru_as_expected(vec![("k1".to_string(), "v1".to_string())]));

    // ***************** Step 2 *****************
    lru_cache.insert("k2".to_string(), "v2".to_string(), 2);
    assert_eq!(lru_cache.size(), 2);
    assert_eq!(lru_cache.usage(), 3);
    assert!(lru_cache.lru_and_handle_table_consistent());
    assert!(lru_cache.lru_as_expected(vec![
        ("k2".to_string(), "v2".to_string()),
        ("k1".to_string(), "v1".to_string())
    ]));

    // ***************** Step 3 *****************
    lru_cache.insert("k3".to_string(), "v3".to_string(), 3);
    assert_eq!(lru_cache.size(), 3);
    assert_eq!(lru_cache.usage(), 6);
    assert!(lru_cache.lru_and_handle_table_consistent());
    assert!(lru_cache.lru_as_expected(vec![
        ("k3".to_string(), "v3".to_string()),
        ("k2".to_string(), "v2".to_string()),
        ("k1".to_string(), "v1".to_string())
    ]));

    // ***************** Step 4 *****************
    lru_cache.insert("k4".to_string(), "v4".to_string(), 4);
    assert_eq!(lru_cache.size(), 4);
    assert_eq!(lru_cache.usage(), 10);
    assert!(lru_cache.lru_and_handle_table_consistent());
    assert!(lru_cache.lru_as_expected(vec![
        ("k4".to_string(), "v4".to_string()),
        ("k3".to_string(), "v3".to_string()),
        ("k2".to_string(), "v2".to_string()),
        ("k1".to_string(), "v1".to_string())
    ]));

    // ***************** Step 5 *****************
    lru_cache.insert("k5".to_string(), "v5".to_string(), 5);
    assert_eq!(lru_cache.size(), 2);
    assert_eq!(lru_cache.usage(), 9);
    assert!(lru_cache.lru_and_handle_table_consistent());
    assert!(lru_cache.lru_as_expected(vec![
        ("k5".to_string(), "v5".to_string()),
        ("k4".to_string(), "v4".to_string())
    ]));

    // ***************** Step 6 *****************
    lru_cache.insert("k6".to_string(), "v6".to_string(), 6);
    assert_eq!(lru_cache.size(), 1);
    assert_eq!(lru_cache.usage(), 6);
    assert!(lru_cache.lru_and_handle_table_consistent());
    assert!(lru_cache.lru_as_expected(vec![("k6".to_string(), "v6".to_string())]));
}

#[test]
fn test_insert_case4() {
    let mut lru_cache = LRUCache::new();
    lru_cache.set_capacity(10);

    // ***************** Step 1 *****************
    lru_cache.insert("k1".to_string(), "v1".to_string(), 1);
    lru_cache.insert("k2".to_string(), "v2".to_string(), 2);
    lru_cache.insert("k3".to_string(), "v3".to_string(), 3);
    lru_cache.insert("k4".to_string(), "v4".to_string(), 4);
    assert_eq!(lru_cache.size(), 4);
    assert_eq!(lru_cache.usage(), 10);
    assert!(lru_cache.lru_and_handle_table_consistent());
    assert!(lru_cache.lru_as_expected(vec![
        ("k4".to_string(), "v4".to_string()),
        ("k3".to_string(), "v3".to_string()),
        ("k2".to_string(), "v2".to_string()),
        ("k1".to_string(), "v1".to_string())
    ]));

    // ***************** Step 2 *****************
    lru_cache.insert("k11".to_string(), "v11".to_string(), 11);
    assert_eq!(lru_cache.size(), 0);
    assert_eq!(lru_cache.usage(), 0);
    assert!(lru_cache.lru_and_handle_table_consistent());
    assert!(lru_cache.lru_as_expected(vec![]));

    // ***************** Step 3 *****************
    lru_cache.insert("k11".to_string(), "v11".to_string(), 11);
    assert_eq!(lru_cache.size(), 0);
    assert_eq!(lru_cache.usage(), 0);
    assert!(lru_cache.lru_and_handle_table_consistent());
    assert!(lru_cache.lru_as_expected(vec![]));

    // ***************** Step 4 *****************
    lru_cache.insert("k1".to_string(), "v1".to_string(), 1);
    lru_cache.insert("k2".to_string(), "v2".to_string(), 2);
    lru_cache.insert("k3".to_string(), "v3".to_string(), 3);
    lru_cache.insert("k4".to_string(), "v4".to_string(), 4);
    assert_eq!(lru_cache.size(), 4);
    assert_eq!(lru_cache.usage(), 10);
    assert!(lru_cache.lru_and_handle_table_consistent());
    assert!(lru_cache.lru_as_expected(vec![
        ("k4".to_string(), "v4".to_string()),
        ("k3".to_string(), "v3".to_string()),
        ("k2".to_string(), "v2".to_string()),
        ("k1".to_string(), "v1".to_string())
    ]));

    // ***************** Step 5 *****************
    lru_cache.insert("k5".to_string(), "v5".to_string(), 5);
    assert_eq!(lru_cache.size(), 2);
    assert_eq!(lru_cache.usage(), 9);
    assert!(lru_cache.lru_and_handle_table_consistent());
    assert!(lru_cache.lru_as_expected(vec![
        ("k5".to_string(), "v5".to_string()),
        ("k4".to_string(), "v4".to_string()),
    ]));

    // ***************** Step 6 *****************
    lru_cache.insert("k1".to_string(), "v1".to_string(), 1);
    assert_eq!(lru_cache.size(), 3);
    assert_eq!(lru_cache.usage(), 10);
    assert!(lru_cache.lru_and_handle_table_consistent());
    assert!(lru_cache.lru_as_expected(vec![
        ("k1".to_string(), "v1".to_string()),
        ("k5".to_string(), "v5".to_string()),
        ("k4".to_string(), "v4".to_string()),
    ]));

    // ***************** Step 7 *****************
    lru_cache.insert("k5".to_string(), "v5".to_string(), 5);
    assert_eq!(lru_cache.size(), 3);
    assert_eq!(lru_cache.usage(), 10);
    assert!(lru_cache.lru_and_handle_table_consistent());
    assert!(lru_cache.lru_as_expected(vec![
        ("k5".to_string(), "v5".to_string()),
        ("k1".to_string(), "v1".to_string()),
        ("k4".to_string(), "v4".to_string()),
    ]));

    // ***************** Step 8 *****************
    lru_cache.insert("k6".to_string(), "v6".to_string(), 6);
    assert_eq!(lru_cache.size(), 1);
    assert_eq!(lru_cache.usage(), 6);
    assert!(lru_cache.lru_and_handle_table_consistent());
    assert!(lru_cache.lru_as_expected(vec![("k6".to_string(), "v6".to_string()),]));

    // ***************** Step 8(2) *****************
    lru_cache.insert("k2".to_string(), "v2".to_string(), 2);
    assert_eq!(lru_cache.size(), 2);
    assert_eq!(lru_cache.usage(), 8);
    assert!(lru_cache.lru_and_handle_table_consistent());
    assert!(lru_cache.lru_as_expected(vec![
        ("k2".to_string(), "v2".to_string()),
        ("k6".to_string(), "v6".to_string()),
    ]));

    // ***************** Step 9 *****************
    lru_cache.insert("k1".to_string(), "v1".to_string(), 1);
    assert_eq!(lru_cache.size(), 3);
    assert_eq!(lru_cache.usage(), 9);
    assert!(lru_cache.lru_and_handle_table_consistent());
    assert!(lru_cache.lru_as_expected(vec![
        ("k1".to_string(), "v1".to_string()),
        ("k2".to_string(), "v2".to_string()),
        ("k6".to_string(), "v6".to_string()),
    ]));

    // ***************** Step 10 *****************
    lru_cache.insert("k3".to_string(), "v3".to_string(), 3);
    assert_eq!(lru_cache.size(), 3);
    assert_eq!(lru_cache.usage(), 6);
    assert!(lru_cache.lru_and_handle_table_consistent());
    assert!(lru_cache.lru_as_expected(vec![
        ("k3".to_string(), "v3".to_string()),
        ("k1".to_string(), "v1".to_string()),
        ("k2".to_string(), "v2".to_string()),
    ]));
}

#[test]
fn test_insert_usage() {
    let mut lru_cache = LRUCache::with_capacity(100);

    lru_cache.insert("k1".to_string(), "v1".to_string(), 1);
    assert_eq!(lru_cache.size(), 1);
    assert_eq!(lru_cache.usage(), 1);
    lru_cache.insert("k1".to_string(), "big".to_string(), 100);
    assert_eq!(lru_cache.size(), 1);
    assert_eq!(lru_cache.usage(), 100);
}

#[test]
fn test_remove_case1() {
    let mut lru_cache = LRUCache::new();
    lru_cache.set_capacity(5);

    // ***************** Step 1 *****************
    lru_cache.insert("k1".to_string(), "v1".to_string(), 1);
    lru_cache.insert("k2".to_string(), "v2".to_string(), 1);
    lru_cache.insert("k3".to_string(), "v3".to_string(), 1);
    lru_cache.insert("k4".to_string(), "v4".to_string(), 1);
    lru_cache.insert("k5".to_string(), "v5".to_string(), 1);
    assert_eq!(lru_cache.size(), 5);
    assert_eq!(lru_cache.usage(), 5);
    assert!(lru_cache.lru_and_handle_table_consistent());
    assert!(lru_cache.lru_as_expected(vec![
        ("k5".to_string(), "v5".to_string()),
        ("k4".to_string(), "v4".to_string()),
        ("k3".to_string(), "v3".to_string()),
        ("k2".to_string(), "v2".to_string()),
        ("k1".to_string(), "v1".to_string())
    ]));

    // ***************** Step 2 *****************
    lru_cache.remove(&"k5".to_string());
    assert_eq!(lru_cache.size(), 4);
    assert_eq!(lru_cache.usage(), 4);
    assert!(lru_cache.lru_and_handle_table_consistent());
    assert!(lru_cache.lru_as_expected(vec![
        ("k4".to_string(), "v4".to_string()),
        ("k3".to_string(), "v3".to_string()),
        ("k2".to_string(), "v2".to_string()),
        ("k1".to_string(), "v1".to_string())
    ]));

    // ***************** Step 3 *****************
    lru_cache.remove(&"k1".to_string());
    assert_eq!(lru_cache.size(), 3);
    assert_eq!(lru_cache.usage(), 3);
    assert!(lru_cache.lru_and_handle_table_consistent());
    assert!(lru_cache.lru_as_expected(vec![
        ("k4".to_string(), "v4".to_string()),
        ("k3".to_string(), "v3".to_string()),
        ("k2".to_string(), "v2".to_string())
    ]));

    // ***************** Step 4 *****************
    lru_cache.remove(&"k3".to_string());
    assert_eq!(lru_cache.size(), 2);
    assert_eq!(lru_cache.usage(), 2);
    assert!(lru_cache.lru_and_handle_table_consistent());
    assert!(lru_cache.lru_as_expected(vec![
        ("k4".to_string(), "v4".to_string()),
        ("k2".to_string(), "v2".to_string())
    ]));

    // ***************** Step 5 *****************
    lru_cache.remove(&"k2".to_string());
    assert_eq!(lru_cache.size(), 1);
    assert_eq!(lru_cache.usage(), 1);
    assert!(lru_cache.lru_and_handle_table_consistent());
    assert!(lru_cache.lru_as_expected(vec![("k4".to_string(), "v4".to_string())]));

    // ***************** Step 6 *****************
    lru_cache.remove(&"k4".to_string());
    assert_eq!(lru_cache.size(), 0);
    assert_eq!(lru_cache.usage(), 0);
    assert!(lru_cache.lru_and_handle_table_consistent());
    assert!(lru_cache.lru_as_expected(vec![]));

    // ***************** Step 7 *****************
    lru_cache.remove(&"k4".to_string());
    assert_eq!(lru_cache.size(), 0);
    assert_eq!(lru_cache.usage(), 0);
    assert!(lru_cache.lru_and_handle_table_consistent());
    assert!(lru_cache.lru_as_expected(vec![]));
}
