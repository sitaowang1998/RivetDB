use std::ops::{Bound, RangeBounds};
use std::sync::Arc;

use openraft::storage::{LogFlushed, RaftLogReader, RaftLogStorage};
use openraft::{
    Entry, LogId, LogState, OptionalSend, StorageError as RaftStorageError, StorageIOError, Vote,
};
use tokio::sync::RwLock;

use crate::raft::RivetRaftConfig;

use super::core::RivetStorageCore;

#[derive(Clone)]
pub struct RivetLogStore {
    pub(super) core: Arc<RwLock<RivetStorageCore>>,
}

#[derive(Clone)]
pub struct RivetLogReader {
    core: Arc<RwLock<RivetStorageCore>>,
}

impl RaftLogReader<RivetRaftConfig> for RivetLogStore {
    async fn try_get_log_entries<RB>(
        &mut self,
        range: RB,
    ) -> Result<Vec<Entry<RivetRaftConfig>>, RaftStorageError<u64>>
    where
        RB: RangeBounds<u64> + Clone + std::fmt::Debug + OptionalSend,
    {
        let core = self.core.read().await;
        let entries = core
            .data
            .log
            .iter()
            .filter(|entry| range_contains(&range, entry.log_id.index))
            .cloned()
            .collect::<Vec<_>>();
        Ok(entries)
    }
}

impl RaftLogReader<RivetRaftConfig> for RivetLogReader {
    async fn try_get_log_entries<RB>(
        &mut self,
        range: RB,
    ) -> Result<Vec<Entry<RivetRaftConfig>>, RaftStorageError<u64>>
    where
        RB: RangeBounds<u64> + Clone + std::fmt::Debug + OptionalSend,
    {
        let core = self.core.read().await;
        let entries = core
            .data
            .log
            .iter()
            .filter(|entry| range_contains(&range, entry.log_id.index))
            .cloned()
            .collect::<Vec<_>>();
        Ok(entries)
    }
}

impl RaftLogStorage<RivetRaftConfig> for RivetLogStore {
    type LogReader = RivetLogReader;

    async fn get_log_state(&mut self) -> Result<LogState<RivetRaftConfig>, RaftStorageError<u64>> {
        let core = self.core.read().await;
        let last_purged = core.data.last_purged;
        let last_log = core
            .data
            .log
            .last()
            .map(|entry| entry.log_id)
            .or(last_purged);

        Ok(LogState {
            last_purged_log_id: last_purged,
            last_log_id: last_log,
        })
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        RivetLogReader {
            core: self.core.clone(),
        }
    }

    async fn save_vote(&mut self, vote: &Vote<u64>) -> Result<(), RaftStorageError<u64>> {
        let mut core = self.core.write().await;
        core.data.vote = Some(*vote);
        core.persist().map_err(|err| RaftStorageError::IO {
            source: StorageIOError::write_vote(&err),
        })?;
        Ok(())
    }

    async fn read_vote(&mut self) -> Result<Option<Vote<u64>>, RaftStorageError<u64>> {
        let guard = self.core.read().await;
        Ok(guard.data.vote)
    }

    async fn save_committed(
        &mut self,
        committed: Option<LogId<u64>>,
    ) -> Result<(), RaftStorageError<u64>> {
        let mut core = self.core.write().await;
        core.data.committed = committed;
        core.persist().map_err(|err| RaftStorageError::IO {
            source: StorageIOError::write_logs(&err),
        })?;
        Ok(())
    }

    async fn read_committed(&mut self) -> Result<Option<LogId<u64>>, RaftStorageError<u64>> {
        let guard = self.core.read().await;
        Ok(guard.data.committed)
    }

    async fn append<I>(
        &mut self,
        entries: I,
        callback: LogFlushed<RivetRaftConfig>,
    ) -> Result<(), RaftStorageError<u64>>
    where
        I: IntoIterator<Item = Entry<RivetRaftConfig>> + OptionalSend,
        I::IntoIter: OptionalSend,
    {
        let mut core = self.core.write().await;
        for entry in entries {
            truncate_from(&mut core.data.log, entry.log_id.index);
            core.data.log.push(entry);
        }
        core.persist().map_err(|err| RaftStorageError::IO {
            source: StorageIOError::write_logs(&err),
        })?;
        callback.log_io_completed(Ok(()));
        Ok(())
    }

    async fn truncate(&mut self, log_id: LogId<u64>) -> Result<(), RaftStorageError<u64>> {
        let mut core = self.core.write().await;
        core.data
            .log
            .retain(|entry| entry.log_id.index < log_id.index);
        core.persist().map_err(|err| RaftStorageError::IO {
            source: StorageIOError::write_logs(&err),
        })?;
        Ok(())
    }

    async fn purge(&mut self, log_id: LogId<u64>) -> Result<(), RaftStorageError<u64>> {
        let mut core = self.core.write().await;
        core.data
            .log
            .retain(|entry| entry.log_id.index > log_id.index);
        core.data.last_purged = Some(log_id);
        core.persist().map_err(|err| RaftStorageError::IO {
            source: StorageIOError::write_logs(&err),
        })?;
        Ok(())
    }
}

fn truncate_from(log: &mut Vec<Entry<RivetRaftConfig>>, start_index: u64) {
    if let Some(pos) = log.iter().position(|e| e.log_id.index >= start_index) {
        log.truncate(pos);
    }
}

fn range_contains<R: RangeBounds<u64>>(range: &R, value: u64) -> bool {
    let start_ok = match range.start_bound() {
        Bound::Included(&start) => value >= start,
        Bound::Excluded(&start) => value > start,
        Bound::Unbounded => true,
    };
    if !start_ok {
        return false;
    }
    match range.end_bound() {
        Bound::Included(&end) => value <= end,
        Bound::Excluded(&end) => value < end,
        Bound::Unbounded => true,
    }
}
