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
