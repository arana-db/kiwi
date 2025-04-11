use crate::kstd::lock_mgr::*;
use std::sync::Arc;
use std::thread;
use std::time::Duration;

fn func(mgr: Arc<LockMgr>, id: usize, key: String) {
    mgr.try_lock(&key);
    println!("thread {} TryLock {} success", id, key);
    thread::sleep(Duration::from_millis(100));
    mgr.unlock(&key);
    println!("thread {} UnLock {}", id, key);
}

#[test]
fn test_lock_mgr() {
    let mgr = Arc::new(LockMgr::with_max_locks(1, 3));

    let mgr_clone1 = Arc::clone(&mgr);
    let t1 = thread::spawn(move || func(mgr_clone1, 1, "key_1".to_string()));
    thread::sleep(Duration::from_millis(20));

    let mgr_clone2 = Arc::clone(&mgr);
    let t2 = thread::spawn(move || func(mgr_clone2, 2, "key_2".to_string()));
    thread::sleep(Duration::from_millis(20));

    let mgr_clone3 = Arc::clone(&mgr);
    let t3 = thread::spawn(move || func(mgr_clone3, 3, "key_3".to_string()));

    let mgr_clone4 = Arc::clone(&mgr);
    let t4 = thread::spawn(move || func(mgr_clone4, 4, "key_4".to_string()));

    thread::sleep(Duration::from_millis(20));

    let s = mgr.try_lock("key_1");
    println!("thread main TryLock key_1 ret {}", s.to_string());
    mgr.unlock("key_1");
    println!("thread main UnLock key_1");

    t1.join().unwrap();
    t2.join().unwrap();
    t3.join().unwrap();
    t4.join().unwrap();
}
