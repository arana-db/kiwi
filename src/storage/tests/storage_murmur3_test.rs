use crate::storage::storage_murmur3::*;

#[test]
fn test_basic_cases() {
    assert_eq!(murmur3_32("", 0), 0);
    assert_eq!(murmur3_32("hello", 0), 0x248bfa47);
    assert_eq!(murmur3_32("hello, world", 0), 0x149bbb7f);
    assert_eq!(murmur3_32("19 Jan 2038 at 3:14:07 AM", 0), 0xe31e8a70);
    assert_eq!(
        murmur3_32("The quick brown fox jumps over the lazy dog.", 0),
        0xd5c48bfc
    );

    assert_eq!(murmur3_32("", 0x01), 0x514e28b7);
    assert_eq!(murmur3_32("hello", 0x01), 0xbb4abcad);
    assert_eq!(murmur3_32("hello, world", 0x01), 0x6f5cb2e9);
    assert_eq!(murmur3_32("19 Jan 2038 at 3:14:07 AM", 0x01), 0xf50e1f30);
    assert_eq!(
        murmur3_32("The quick brown fox jumps over the lazy dog.", 0x01),
        0x846f6a36
    );

    assert_eq!(murmur3_32("", 0x2a), 0x087fcd5c);
    assert_eq!(murmur3_32("hello", 0x2a), 0xe2dbd2e1);
    assert_eq!(murmur3_32("hello, world", 0x2a), 0x7ec7c6c2);
    assert_eq!(murmur3_32("19 Jan 2038 at 3:14:07 AM", 0x2a), 0x58f745f6);
    assert_eq!(
        murmur3_32("The quick brown fox jumps over the lazy dog.", 0x2a),
        0xc02d1434
    );
}

#[test]
fn test_seed_behavior() {
    assert_ne!(murmur3_32("test", 1), murmur3_32("test", 2));

    assert_eq!(murmur3_32("data", 666), murmur3_32("data", 666));
}

#[test]
fn test_unicode() {
    assert_eq!(murmur3_32("€", 0), 0x5b43fca5);
}
