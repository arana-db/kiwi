//! 内存实现的 Raft 日志存储
//!
//! 此实现使用 BTreeMap 存储日志条目，适用于 POC 阶段。
//! 生产环境应使用 RocksDB 等持久化存储。

use std::collections::BTreeMap;
use std::fmt::Debug;
use std::io;
use std::ops::RangeBounds;
use std::sync::Arc;

use openraft::LogState;
use openraft::RaftLogReader;
use openraft::RaftTypeConfig;
use openraft::storage::LogFlushed;

use conf::raft_type::KiwiTypeConfig;

/// 内存实现的 Raft 日志存储
#[derive(Debug, Clone, Default)]
pub struct LogStore {
    inner: Arc<tokio::sync::Mutex<LogStoreInner>>,
}

#[derive(Debug)]
struct LogStoreInner {
    /// 最后清理的日志 ID
    last_purged_log_id: Option<LogIdOf<KiwiTypeConfig>>,

    /// Raft 日志条目
    logs: BTreeMap<u64, EntryOf<KiwiTypeConfig>>,

    /// 当前授予的投票
    vote: Option<VoteOf<KiwiTypeConfig>>,

    /// 已提交的日志 ID
    committed: Option<LogIdOf<KiwiTypeConfig>>,
}

impl Default for LogStoreInner {
    fn default() -> Self {
        Self {
            last_purged_log_id: None,
            logs: BTreeMap::new(),
            committed: None,
            vote: None,
        }
    }
}

impl LogStore {
    /// 创建新的内存日志存储
    pub fn new() -> Self {
        Self {
            inner: Arc::new(tokio::sync::Mutex::new(LogStoreInner::default())),
        }
    }
}

impl LogStoreInner {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug>(
        &mut self,
        range: RB,
    ) -> Result<Vec<EntryOf<KiwiTypeConfig>>, io::Error> {
        let response = self
            .logs
            .range(range.clone())
            .map(|(_, val)| val.clone())
            .collect::<Vec<_>>();
        Ok(response)
    }

    async fn get_log_state(&mut self) -> Result<LogState<KiwiTypeConfig>, io::Error> {
        let last = self.logs.iter().next_back().map(|(_, ent)| ent.log_id());

        let last_purged = self.last_purged_log_id.clone();

        let last = match last {
            None => last_purged.clone(),
            Some(x) => Some(x),
        };

        Ok(LogState {
            last_purged_log_id: last_purged,
            last_log_id: last,
        })
    }

    async fn save_committed(
        &mut self,
        committed: Option<LogIdOf<KiwiTypeConfig>>,
    ) -> Result<(), io::Error> {
        self.committed = committed;
        Ok(())
    }

    async fn read_committed(&mut self) -> Result<Option<LogIdOf<KiwiTypeConfig>>, io::Error> {
        Ok(self.committed.clone())
    }

    async fn save_vote(&mut self, vote: &VoteOf<KiwiTypeConfig>) -> Result<(), io::Error> {
        self.vote = Some(vote.clone());
        Ok(())
    }

    async fn read_vote(&mut self) -> Result<Option<VoteOf<KiwiTypeConfig>>, io::Error> {
        Ok(self.vote.clone())
    }

    async fn append<I>(
        &mut self,
        entries: I,
        callback: IOFlushed<KiwiTypeConfig>,
    ) -> Result<(), io::Error>
    where
        I: IntoIterator<Item = EntryOf<KiwiTypeConfig>>,
    {
        // 简单实现：直接追加到内存，不进行磁盘持久化
        // POC 阶段可以接受，生产环境需要持久化
        for entry in entries {
            self.logs.insert(entry.index(), entry);
        }
        callback.io_completed(Ok(()));

        Ok(())
    }

    async fn truncate_after(
        &mut self,
        last_log_id: Option<LogIdOf<KiwiTypeConfig>>,
    ) -> Result<(), io::Error> {
        let start_index = match last_log_id {
            Some(log_id) => log_id.index() + 1,
            None => 0,
        };

        let keys = self
            .logs
            .range(start_index..)
            .map(|(k, _v)| *k)
            .collect::<Vec<_>>();
        for key in keys {
            self.logs.remove(&key);
        }

        Ok(())
    }

    async fn purge(&mut self, log_id: LogIdOf<KiwiTypeConfig>) -> Result<(), io::Error> {
        {
            let ld = &mut self.last_purged_log_id;
            assert!(ld.as_ref() <= Some(&log_id));
            *ld = Some(log_id.clone());
        }

        {
            let keys = self
                .logs
                .range(..=log_id.index())
                .map(|(k, _v)| *k)
                .collect::<Vec<_>>();
            for key in keys {
                self.logs.remove(&key);
            }
        }

        Ok(())
    }
}

impl RaftLogReader<KiwiTypeConfig> for LogStore {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug>(
        &mut self,
        range: RB,
    ) -> Result<Vec<EntryOf<KiwiTypeConfig>>, io::Error> {
        let mut inner = self.inner.lock().await;
        inner.try_get_log_entries(range).await
    }
}

impl openraft::RaftLogStorage<KiwiTypeConfig> for LogStore {
    type LogReader = Self;

    async fn get_log_state(&mut self) -> Result<LogState<KiwiTypeConfig>, io::Error> {
        let mut inner = self.inner.lock().await;
        inner.get_log_state().await
    }

    async fn save_committed(
        &mut self,
        committed: Option<LogIdOf<KiwiTypeConfig>>,
    ) -> Result<(), io::Error> {
        let mut inner = self.inner.lock().await;
        inner.save_committed(committed).await
    }

    async fn read_committed(&mut self) -> Result<Option<LogIdOf<KiwiTypeConfig>>, io::Error> {
        let mut inner = self.inner.lock().await;
        inner.read_committed().await
    }

    async fn save_vote(&mut self, vote: &VoteOf<KiwiTypeConfig>) -> Result<(), io::Error> {
        let mut inner = self.inner.lock().await;
        inner.save_vote(vote).await
    }

    async fn append<I>(
        &mut self,
        entries: I,
        callback: IOFlushed<KiwiTypeConfig>,
    ) -> Result<(), io::Error>
    where
        I: IntoIterator<Item = EntryOf<KiwiTypeConfig>>,
    {
        let mut inner = self.inner.lock().await;
        inner.append(entries, callback).await
    }

    async fn truncate_after(
        &mut self,
        last_log_id: Option<LogIdOf<KiwiTypeConfig>>,
    ) -> Result<(), io::Error> {
        let mut inner = self.inner.lock().await;
        inner.truncate_after(last_log_id).await
    }

    async fn purge(&mut self, log_id: LogIdOf<KiwiTypeConfig>) -> Result<(), io::Error> {
        let mut inner = self.inner.lock().await;
        inner.purge(log_id).await
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }
}
