use std::io::Cursor;
use std::sync::Arc;

use openraft::storage::{RaftSnapshotBuilder, RaftStateMachine};
use openraft::{
    BasicNode, Entry, EntryPayload, LogId, OptionalSend, Snapshot, SnapshotMeta,
    StorageError as RaftStorageError, StorageIOError, StoredMembership,
};
use tokio::sync::RwLock;

use crate::raft::{RaftCommand, RivetRaftConfig};
use crate::storage::StorageEngine;

use super::super::decode_command;
use super::core::{RivetStorageCore, StoredSnapshot};

#[derive(Clone)]
pub struct RivetStateMachine<S: StorageEngine> {
    pub(super) core: Arc<RwLock<RivetStorageCore>>,
    pub(super) storage: Arc<S>,
}

pub struct RivetSnapshotBuilder {
    pub(super) core: Arc<RwLock<RivetStorageCore>>,
}

impl<S> RaftStateMachine<RivetRaftConfig> for RivetStateMachine<S>
where
    S: StorageEngine + 'static,
{
    type SnapshotBuilder = RivetSnapshotBuilder;

    async fn applied_state(
        &mut self,
    ) -> Result<(Option<LogId<u64>>, StoredMembership<u64, BasicNode>), RaftStorageError<u64>> {
        let core = self.core.read().await;
        Ok((
            core.data.state.last_applied,
            core.data.state.last_membership.clone(),
        ))
    }

    async fn apply<I>(&mut self, entries: I) -> Result<Vec<()>, RaftStorageError<u64>>
    where
        I: IntoIterator<Item = Entry<RivetRaftConfig>> + OptionalSend,
        I::IntoIter: OptionalSend,
    {
        let mut responses = Vec::new();

        for entry in entries {
            let log_id = entry.log_id;

            match entry.payload {
                EntryPayload::Blank => {
                    let mut core = self.core.write().await;
                    core.data.state.last_applied = Some(log_id);
                    core.persist().map_err(|err| RaftStorageError::IO {
                        source: StorageIOError::apply(log_id, &err),
                    })?;
                }
                EntryPayload::Normal(data) => {
                    let command = decode_command(&data)
                        .map_err(|err| map_apply_error(log_id, err.to_string()))?;
                    let RaftCommand::ApplyTransaction(txn) = command;
                    self.storage
                        .apply_committed(txn.commit_ts, txn.writes)
                        .await
                        .map_err(|err| map_apply_error(log_id, err.to_string()))?;

                    let mut core = self.core.write().await;
                    core.data.state.last_applied = Some(log_id);
                    core.data.state.last_commit_ts = txn.commit_ts;
                    core.data.state.applied_commands.push(data);
                    core.persist().map_err(|err| RaftStorageError::IO {
                        source: StorageIOError::apply(log_id, &err),
                    })?;
                }
                EntryPayload::Membership(membership) => {
                    let mut core = self.core.write().await;
                    core.data.state.last_membership =
                        StoredMembership::new(Some(log_id), membership);
                    core.data.state.last_applied = Some(log_id);
                    core.persist().map_err(|err| RaftStorageError::IO {
                        source: StorageIOError::apply(log_id, &err),
                    })?;
                }
            }

            responses.push(());
        }

        Ok(responses)
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        RivetSnapshotBuilder {
            core: self.core.clone(),
        }
    }

    async fn begin_receiving_snapshot(
        &mut self,
    ) -> Result<
        Box<<RivetRaftConfig as openraft::RaftTypeConfig>::SnapshotData>,
        RaftStorageError<u64>,
    > {
        Ok(Box::new(Cursor::new(Vec::new())))
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<u64, BasicNode>,
        snapshot: Box<<RivetRaftConfig as openraft::RaftTypeConfig>::SnapshotData>,
    ) -> Result<(), RaftStorageError<u64>> {
        let mut core = self.core.write().await;
        let data = snapshot.into_inner();
        core.data.snapshot = Some(StoredSnapshot {
            meta: meta.clone(),
            data,
        });
        core.data.state.last_applied = meta.last_log_id;
        core.data.state.last_membership = meta.last_membership.clone();
        core.persist().map_err(|err| RaftStorageError::IO {
            source: StorageIOError::write_snapshot(None, &err),
        })?;
        Ok(())
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<Snapshot<RivetRaftConfig>>, RaftStorageError<u64>> {
        let core = self.core.read().await;
        Ok(core.data.snapshot.as_ref().map(|stored| Snapshot {
            meta: stored.meta.clone(),
            snapshot: Box::new(Cursor::new(stored.data.clone())),
        }))
    }
}

impl RaftSnapshotBuilder<RivetRaftConfig> for RivetSnapshotBuilder {
    async fn build_snapshot(&mut self) -> Result<Snapshot<RivetRaftConfig>, RaftStorageError<u64>> {
        let mut core = self.core.write().await;
        core.data.snapshot_seq += 1;
        let snapshot_id = format!("rivet-snapshot-{}", core.data.snapshot_seq);
        let meta = SnapshotMeta {
            last_log_id: core.data.state.last_applied,
            last_membership: core.data.state.last_membership.clone(),
            snapshot_id,
        };
        let data = Vec::new();
        core.data.snapshot = Some(StoredSnapshot {
            meta: meta.clone(),
            data: data.clone(),
        });
        core.persist().map_err(|err| RaftStorageError::IO {
            source: StorageIOError::write_snapshot(None, &err),
        })?;
        Ok(Snapshot {
            meta,
            snapshot: Box::new(Cursor::new(data)),
        })
    }
}

fn map_apply_error(log_id: LogId<u64>, message: impl Into<String>) -> RaftStorageError<u64> {
    let io_error = std::io::Error::other(message.into());
    RaftStorageError::IO {
        source: StorageIOError::apply(log_id, &io_error),
    }
}
