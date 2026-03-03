pub mod api;
pub mod cf_tracker;
pub mod collector;
pub mod db_access;
pub mod event_listener;
pub mod log_store;
pub mod network;
pub mod node;
pub mod state_machine;
pub mod table_properties;
pub mod types;

pub use cf_tracker::{LogIndexOfColumnFamilies, SmallestIndexRes};
pub use collector::LogIndexAndSequenceCollector;
pub use event_listener::LogIndexAndSequenceCollectorPurger;
pub use table_properties::{
    LogIndexTablePropertiesCollectorFactory, PROPERTY_KEY, get_largest_log_index_from_collection,
    read_stats_from_table_props,
};
pub use types::{LogIndex, LogIndexAndSequencePair, LogIndexSeqnoPair, SequenceNumber};

pub const COLUMN_FAMILY_COUNT: usize = storage::ColumnFamilyIndex::COUNT;

pub const CF_NAMES: [&str; COLUMN_FAMILY_COUNT] = [
    "default",
    "hash_data_cf",
    "set_data_cf",
    "list_data_cf",
    "zset_data_cf",
    "zset_score_cf",
];

const _: () = assert!(
    CF_NAMES.len() == storage::ColumnFamilyIndex::COUNT,
    "CF_NAMES length must match storage::ColumnFamilyIndex::COUNT"
);

pub fn cf_name_to_index(name: &[u8]) -> Option<usize> {
    CF_NAMES.iter().position(|n| n.as_bytes() == name)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cf_names_match_storage() {
        use storage::ColumnFamilyIndex;
        let variants: [ColumnFamilyIndex; ColumnFamilyIndex::COUNT] = [
            ColumnFamilyIndex::MetaCF,
            ColumnFamilyIndex::HashesDataCF,
            ColumnFamilyIndex::SetsDataCF,
            ColumnFamilyIndex::ListsDataCF,
            ColumnFamilyIndex::ZsetsDataCF,
            ColumnFamilyIndex::ZsetsScoreCF,
        ];
        for (i, cf_index) in variants.iter().enumerate() {
            assert_eq!(
                cf_index.name(),
                CF_NAMES[i],
                "CF_NAMES[{}] mismatch: expected '{}', got '{}'",
                i,
                cf_index.name(),
                CF_NAMES[i]
            );
        }
    }
}
