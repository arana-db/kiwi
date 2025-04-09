// TODO: remove allow dead code
#[allow(dead_code)]
pub fn murmur3_32<T: AsRef<[u8]>>(data: T, seed: u32) -> u32 {
    let data = data.as_ref();
    let mut hash = seed;
    let c1: u32 = 0xcc9e2d51;
    let c2: u32 = 0x1b873593;
    let total_len = data.len() as u32;

    let chunks = data.chunks_exact(4);
    let remainder = chunks.remainder();

    for chunk in chunks {
        let mut k = u32::from_le_bytes(chunk.try_into().unwrap());

        k = k.wrapping_mul(c1);
        k = k.rotate_left(15);
        k = k.wrapping_mul(c2);

        hash ^= k;
        hash = hash.rotate_left(13);
        hash = hash.wrapping_mul(5).wrapping_add(0xe6546b64);
    }

    if !remainder.is_empty() {
        let mut k1 = 0;
        match remainder.len() & 3 {
            3 => {
                k1 ^= (remainder[2] as u32) << 16;
                k1 ^= (remainder[1] as u32) << 8;
                k1 ^= remainder[0] as u32;
            }
            2 => {
                k1 ^= (remainder[1] as u32) << 8;
                k1 ^= remainder[0] as u32;
            }
            1 => {
                k1 ^= remainder[0] as u32;
            }
            _ => unreachable!(),
        }

        k1 = k1.wrapping_mul(c1);
        k1 = k1.rotate_left(15);
        k1 = k1.wrapping_mul(c2);
        hash ^= k1;
    }

    hash ^= total_len;
    hash ^= hash >> 16;
    hash = hash.wrapping_mul(0x85ebca6b);
    hash ^= hash >> 13;
    hash = hash.wrapping_mul(0xc2b2ae35);
    hash ^= hash >> 16;

    hash
}

#[cfg(test)]
mod tests {
    use super::*;

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
}
