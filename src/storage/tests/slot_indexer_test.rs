use crate::storage::slot_indexer::*;

#[test]
fn test_create_slot_indexer() {
    let indexer = SlotIndexer::new(5);
    assert_eq!(indexer.get_instance_num(), 5);
}

#[test]
#[should_panic(expected = "Instance number must be greater than zero.")]
fn test_create_slot_indexer_with_zero() {
    // This should panic
    SlotIndexer::new(0);
}

#[test]
fn test_get_instance_id() {
    let indexer = SlotIndexer::new(10);
    assert_eq!(indexer.get_instance_id(25), 5);
    assert_eq!(indexer.get_instance_id(10), 0);
    assert_eq!(indexer.get_instance_id(8), 8);
    assert_eq!(indexer.get_instance_id(15), 5);
}
