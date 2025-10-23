// Simple test to verify MSET implementation
use std::sync::Arc;
use storage::{BgTaskHandler, Redis, StorageOptions};
use kstd::lock_mgr::LockMgr;

fn main() {
    let test_db_path = std::env::temp_dir().join("test_mset_db");
    
    if test_db_path.exists() {
        std::fs::remove_dir_all(&test_db_path).unwrap();
    }

    let storage_options = Arc::new(StorageOptions::default());
    let (bg_task_handler, _) = BgTaskHandler::new();
    let lock_mgr = Arc::new(LockMgr::new(1000));
    let mut redis = Redis::new(storage_options, 1, Arc::new(bg_task_handler), lock_mgr);

    let result = redis.open(test_db_path.to_str().unwrap());
    assert!(result.is_ok(), "Failed to open Redis DB: {:?}", result.err());

    // Test MSET
    let kvs = vec![
        (b"key1".to_vec(), b"value1".to_vec()),
        (b"key2".to_vec(), b"value2".to_vec()),
        (b"key3".to_vec(), b"value3".to_vec()),
    ];

    let result = redis.mset(&kvs);
    assert!(result.is_ok(), "MSET failed: {:?}", result.err());

    // Verify values
    assert_eq!(redis.get(b"key1").unwrap(), "value1");
    assert_eq!(redis.get(b"key2").unwrap(), "value2");
    assert_eq!(redis.get(b"key3").unwrap(), "value3");

    println!("MSET test passed!");

    redis.set_need_close(true);
    drop(redis);

    if test_db_path.exists() {
        std::fs::remove_dir_all(test_db_path).unwrap();
    }
}