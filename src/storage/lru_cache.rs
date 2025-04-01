use std::collections::HashMap;
use std::ptr::NonNull;

/// The chain connects prev and next cache.
struct Chain {
    prev: NonNull<Chain>,
    next: NonNull<Chain>,
}

impl Chain {
    fn new() -> Chain {
        Chain {
            prev: NonNull::dangling(),
            next: NonNull::dangling(),
        }
    }
}

impl Drop for Chain {
    fn drop(&mut self) {
        unsafe {
            drop(Box::from_raw(self.prev.as_ptr()));
            drop(Box::from_raw(self.next.as_ptr()));
        }
    }
}

/// Cache is record the key and value.
/// Inside chain will help cache remember prev and next.
struct Cache<V>
where
    V: Clone,
{
    value: V,
    charge: usize, // the value size
    chain: NonNull<Chain>,
}

impl<V> Cache<V>
where
    V: Clone,
{
    fn new<U: Into<usize>>(value: V, charge: U, chain: NonNull<Chain>) -> Cache<V> {
        Cache {
            value,
            charge: charge.into(),
            chain,
        }
    }
}

/// LRUCache:
///             <-next        prev->
///          cache5 -> origin -> cache1
///             |                   |
///          cache4 <- cache3 <- cache2
/// LRUCache is constructed using a hash map and
/// a circular doubly linked list, with a time complexity of O(1).
/// NOTE: LRUCache is thread-unsafe.
/// We may currently need to wrap mutex externally to access LRUCache.
pub struct LRUCache<K, V>
where
    K: std::hash::Hash + Eq + Clone,
    V: Clone,
{
    map: HashMap<K, Cache<V>>,
    capacity: usize,
    usage: usize, // total charge
    size: usize,
    origin: NonNull<Chain>, // start and end node.
}

/// connect caches.
#[inline]
fn connect(mut front: NonNull<Chain>, mut back: NonNull<Chain>) {
    unsafe {
        front.as_mut().next = back;
        back.as_mut().prev = front;
    }
}

/// cut caches.
#[inline]
fn cut_out(cache: NonNull<Chain>) {
    unsafe { connect(cache.as_ref().prev, cache.as_ref().next) }
}

impl<K, V> LRUCache<K, V>
where
    K: std::hash::Hash + Eq + Clone,
    V: Clone,
{
    pub fn new() -> Self {
        let origin = Box::leak(Box::new(Chain::new())).into();
        // Form a circular linked list.
        // At first, connect itself.
        connect(origin, origin);

        Self {
            map: HashMap::default(),
            capacity: 0,
            usage: 0,
            size: 0,
            origin,
        }
    }

    /// Create a LRUCache with capacity.
    pub fn with_capacity(capacity: usize) -> Self {
        let origin = Box::leak(Box::new(Chain::new())).into();
        // Form a circular linked list.
        // At first, connect itself.
        connect(origin, origin);
        Self {
            map: HashMap::default(),
            capacity,
            usage: 0,
            size: 0,
            origin,
        }
    }

    /// Get the LRUCache's current size.
    #[inline]
    pub fn size(&self) -> usize {
        self.size
    }

    /// Get the LRUCache's total charge.
    #[inline]
    pub fn usage(&self) -> usize {
        self.usage
    }

    /// Get the LRUCache's capacity.
    #[inline]
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// Set the LRUCache's capacity.
    /// If the number of caches exceeds the capacity to be set,
    /// the excess caches will be removed.
    pub fn set_capacity(&mut self, capacity: usize) {
        self.capacity = capacity;
        self.trim();
    }

    /// Move the cache to the front.
    fn move_to_head(&mut self, chain: NonNull<Chain>) {
        unsafe {
            connect(self.origin.as_ref().prev, chain);
            connect(chain, self.origin);
        }
    }

    // charge is the value of the memory size.
    pub fn insert(&mut self, key: K, value: V, charge: usize) {
        if let Some(cache) = self.map.get_mut(&key) {
            cut_out(cache.chain);
            cache.value = value;
        } else {
            let chain = Box::leak(Box::new(Chain::new())).into();

            self.map
                .insert(key.clone(), Cache::new(value, charge, chain));

            self.size += 1;
            self.usage += charge;
        }

        let cache_chain = self.map.get_mut(&key).unwrap().chain;
        self.move_to_head(cache_chain);

        // Trim excess elements if exceeded capacity.
        self.trim();
    }

    pub fn lookup(&mut self, key: &K) -> Option<V> {
        match self.map.get_mut(key) {
            Some(cache) => {
                let chain = cache.chain;
                let value = cache.value.clone();
                cut_out(chain);
                self.move_to_head(chain);
                Some(value)
            }
            None => None,
        }
    }

    /// Ensures that the structure's size does not exceed its predefined capacity.
    fn trim(&mut self) {
        while self.usage > self.capacity {
            unsafe {
                let old_chain = self.origin.as_ref().next;
                cut_out(old_chain);
                if let Some((key, charge)) = self.map.iter().find_map(|(key, cache)| {
                    if cache.chain == old_chain {
                        Some((key.clone(), cache.charge))
                    } else {
                        None
                    }
                }) {
                    self.map.remove(&key);
                    self.usage -= charge;
                }

                self.size -= 1;
            }
        }
    }

    /// Remove a key-value pair from the cache.
    pub fn remove(&mut self, key: &K) -> Option<V> {
        if let Some(cache) = self.map.remove(key) {
            cut_out(cache.chain);
            // Deal with memory if needed (e.g., deallocate or reuse).
            self.size -= 1;
            self.usage -= cache.charge;
            return Some(cache.value);
        }
        None
    }

    pub fn clear(&mut self) {
        self.map.clear();
        connect(self.origin, self.origin);
    }
}

impl<K, V> Drop for LRUCache<K, V>
where
    K: std::hash::Hash + Eq + Clone,
    V: Clone,
{
    fn drop(&mut self) {
        self.clear();
    }
}

/// just for test
impl<K, V> LRUCache<K, V>
where
    K: std::hash::Hash + Eq + Clone,
    V: Clone,
{
    pub fn lru_and_handle_table_consistent(&self) -> bool {
        let mut curr = unsafe { self.origin.as_ref().next };
        let mut count = 0;
        while curr != self.origin {
            // Find the cache entry in map.
            let found = self.map.values().any(|cache| cache.chain == curr);
            if !found {
                return false;
            }
            count += 1;
            curr = unsafe { curr.as_ref().next };
        }
        // Check if the number of caches matches.
        count == self.size
    }
}

/// just for test
impl<K, V> LRUCache<K, V>
where
    K: std::hash::Hash + Eq + Clone,
    V: Clone + PartialEq,
{
    pub fn lru_as_expected(&self, expected: Vec<(K, V)>) -> bool {
        let mut curr = unsafe { self.origin.as_ref().prev };
        let mut idx = 0;
        while curr != self.origin && idx < expected.len() {
            let key_value = self.map.iter().find_map(|(k, cache)| {
                if cache.chain == curr {
                    Some((k.clone(), cache.value.clone()))
                } else {
                    None
                }
            });
            if let Some((key, value)) = key_value {
                if expected[idx] != (key, value) {
                    return false;
                }
            } else {
                return false; // Consistency problem: cache mismanagement.
            }
            curr = unsafe { curr.as_ref().prev };
            idx += 1;
        }
        // All elements checked; return if the total number fits expected count.
        idx == expected.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
}
