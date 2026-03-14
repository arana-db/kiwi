use std::{io, sync::Arc};

use openraft::{
    EntryPayload, LogId, RaftSnapshotBuilder, Snapshot, SnapshotMeta, StorageError,
    StoredMembership, storage::RaftStateMachine,
};

use conf::raft_type::{Binlog, BinlogResponse, KiwiNode, KiwiTypeConfig};
use storage::storage::Storage;

pub struct KiwiStateMachine {
    node_id: u64,
    storage: Arc<Storage>,
    last_applied: Option<LogId<u64>>,
    last_membership: StoredMembership<u64, KiwiNode>,
    snapshot_idx: u64,
}

impl KiwiStateMachine {
    pub fn new(node_id: u64, storage: Arc<Storage>) -> Self {
        Self {
            node_id,
            storage,
            last_applied: None,
            last_membership: StoredMembership::default(),
            snapshot_idx: 0,
        }
    }
}

impl KiwiStateMachine {
    async fn apply_binlog(&self, binlog: &Binlog, log_idx: u64) -> Result<(), io::Error> {
        self.storage.on_binlog_write(binlog).map_err(|e| {
            io::Error::new(
                io::ErrorKind::Other,
                format!("Failed to apply binlog: {}", e),
            )
        })
    }
}

impl RaftStateMachine<KiwiTypeConfig> for KiwiStateMachine {
    type SnapshotBuilder = KiwiSnapshotBuilder;

    /// 应用 Raft 日志到状态机
    async fn apply<I>(
        &mut self,
        entries: I,
    ) -> Result<Vec<BinlogResponse>, openraft::StorageError<u64>>
    where
        I: IntoIterator<Item = openraft::Entry<KiwiTypeConfig>> + Send,
    {
        let mut responses = Vec::new();

        for entry in entries {
            self.last_applied = Some(entry.log_id);

            let response = match entry.payload {
                EntryPayload::Blank => BinlogResponse::ok(),
                EntryPayload::Normal(binlog) => {
                    // 调用 Storage::on_binlog_write() 应用 binlog
                    match self.apply_binlog(&binlog, entry.log_id.index).await {
                        Ok(_) => BinlogResponse::ok(),
                        Err(e) => BinlogResponse::error(e.to_string()),
                    }
                }
                EntryPayload::Membership(mem) => {
                    self.last_membership = StoredMembership::new(Some(entry.log_id), mem);
                    BinlogResponse::ok()
                }
            };

            responses.push(response);
        }

        Ok(responses)
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        self.snapshot_idx += 1;
        KiwiSnapshotBuilder::new(self.storage.clone(), self.snapshot_idx)
    }

    async fn begin_receiving_snapshot(
        &mut self,
    ) -> Result<Box<std::io::Cursor<Vec<u8>>>, openraft::StorageError<u64>> {
        Ok(Box::new(std::io::Cursor::new(Vec::new())))
    }

    async fn install_snapshot(
        &mut self,
        _meta: &SnapshotMeta<u64, KiwiNode>,
        _snapshot: Box<std::io::Cursor<Vec<u8>>>,
    ) -> Result<(), openraft::StorageError<u64>> {
        Ok(())
    }

    // poc 阶段暂时不支持快照
    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<Snapshot<KiwiTypeConfig>>, openraft::StorageError<u64>> {
        Ok(None)
    }

    async fn applied_state(
        &mut self,
    ) -> Result<(Option<LogId<u64>>, StoredMembership<u64, KiwiNode>), openraft::StorageError<u64>>
    {
        Ok((self.last_applied, self.last_membership.clone()))
    }
}

/// poc 阶段空实现
pub struct KiwiSnapshotBuilder {
    _storage: Arc<Storage>,
    _idx: u64,
}

impl KiwiSnapshotBuilder {
    pub fn new(storage: Arc<Storage>, idx: u64) -> Self {
        Self {
            _storage: storage,
            _idx: idx,
        }
    }
}

impl RaftSnapshotBuilder<KiwiTypeConfig> for KiwiSnapshotBuilder {
    async fn build_snapshot(&mut self) -> Result<Snapshot<KiwiTypeConfig>, StorageError<u64>> {
        // 1. 生成 snapshot ID
        self._idx += 1;
        let snapshot_id = format!("snapshot-{}", self._idx);

        // 2. 构建空的元数据
        let meta = SnapshotMeta {
            last_log_id: None,
            last_membership: StoredMembership::default(),
            snapshot_id,
        };

        // 3. 构建空的 snapshot 数据（Cursor<Vec<u8>> 是默认的 SnapshotData 类型）
        let snapshot_data = Box::new(std::io::Cursor::new(Vec::new()));

        // 4. 返回
        Ok(Snapshot {
            meta,
            snapshot: snapshot_data,
        })
    }
}
