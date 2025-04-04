pub struct SlotIndexer {
    inst_num: u32,
}

impl SlotIndexer {
    /// Create a new SlotIndexer with a defined instance number.
    pub fn new(inst_num: u32) -> Self {
        assert!(inst_num > 0, "Instance number must be greater than zero.");
        SlotIndexer { inst_num }
    }

    /// Calculate the instance ID from given slot ID.
    pub fn get_instance_id(&self, slot_id: u32) -> u32 {
        slot_id % self.inst_num
    }

    /// Placeholder for re-sharding slots functionality.
    pub fn reshard_slots(&self, _slots: Vec<u32>) {
        // TODO: Implement the logic for re-sharding slots.
        // When we implement this method, remove the underscore.
        // Don't forget add unit test.
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_slot_indexer() {
        let indexer = SlotIndexer::new(5);
        assert_eq!(indexer.inst_num, 5);
    }

    #[test]
    #[should_panic(expected = "Instance number must be greater than zero.")]
    fn test_create_slot_indexer_with_zero() {
        SlotIndexer::new(0); // This should panic
    }

    #[test]
    fn test_get_instance_id() {
        let indexer = SlotIndexer::new(10);
        assert_eq!(indexer.get_instance_id(25), 5);
        assert_eq!(indexer.get_instance_id(10), 0);
        assert_eq!(indexer.get_instance_id(8), 8);
        assert_eq!(indexer.get_instance_id(15), 5);
    }
}
