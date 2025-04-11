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
/// TODO: remove allow dead code
#[allow(dead_code)]
struct Cache<V>
where
    V: Clone,
{
    value: V,
    charge: usize, // the value size
    chain: NonNull<Chain>,
}

/// TODO: remove allow dead code
#[allow(dead_code)]
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
/// TODO: remove allow dead code
#[allow(dead_code)]
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

impl<K, V> Default for LRUCache<K, V>
where
    K: std::hash::Hash + Eq + Clone,
    V: Clone,
{
    fn default() -> Self {
        Self::new()
    }
}

/// TODO: remove allow dead code
#[allow(dead_code)]
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
            self.usage -= cache.charge;
            cut_out(cache.chain);
            cache.value = value;
            self.usage += charge;
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
                // TODO: Add the key to the chain, directly search for the key in the hashmap,
                // making the time complexity go from O(n) to O(1).
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
/// TODO: remove allow dead code
#[allow(dead_code)]
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
/// TODO: remove allow dead code
#[allow(dead_code)]
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
