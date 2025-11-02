use std::collections::HashMap;
use std::io::Cursor;
use std::ops::{Bound, RangeBounds};
use std::sync::Arc;

use once_cell::sync::Lazy;
use tokio::sync::RwLock;

use openraft::error::{
    Infallible, InstallSnapshotError, RPCError, RaftError, RemoteError, Unreachable,
};
use openraft::metrics::RaftMetrics;
use openraft::network::{RPCOption, RaftNetwork, RaftNetworkFactory};
use openraft::storage::{
    LogFlushed, RaftLogReader, RaftLogStorage, RaftSnapshotBuilder, RaftStateMachine,
};
use openraft::{
    self, BasicNode, Config, Entry, EntryPayload, LogId, LogState, OptionalSend, Snapshot,
    SnapshotMeta, StorageError, StoredMembership, Vote,
};

openraft::declare_raft_types!(
    /// Openraft type configuration used by RivetDB during early integration.
    ///
    /// The payloads remain opaque byte buffers for now.
    /// TODO: Wire transactional commands through these entries.
    pub RivetRaftConfig:
        D = Vec<u8>,
        R = (),
);

/// Convenience alias for the Openraft handle parameterised with [`RivetRaftConfig`].
pub type RivetRaft = openraft::Raft<RivetRaftConfig>;

/// Shared singleton routing table for in-memory Raft RPC forwarding during tests.
static GLOBAL_REGISTRY: Lazy<Arc<RaftRegistry>> = Lazy::new(|| Arc::new(RaftRegistry::default()));

/// Expose the global registry so node construction can share the same routing table.
pub fn registry() -> Arc<RaftRegistry> {
    GLOBAL_REGISTRY.clone()
}

/// Helper to wipe the registry between integration tests.
pub async fn reset_registry() {
    GLOBAL_REGISTRY.clear().await;
}

/// Create in-memory log/state-machine handles backed by a shared core.
pub struct RivetStore;

impl RivetStore {
    pub fn handles() -> (RivetLogStore, RivetStateMachine) {
        let core = Arc::new(RwLock::new(RivetStorageCore::default()));
        (
            RivetLogStore { core: core.clone() },
            RivetStateMachine { core },
        )
    }
}

#[derive(Default)]
struct RivetStorageCore {
    vote: Option<Vote<u64>>,
    committed: Option<LogId<u64>>,
    log: Vec<Entry<RivetRaftConfig>>,
    last_purged: Option<LogId<u64>>,
    state: RivetStateData,
    snapshot: Option<StoredSnapshot>,
    snapshot_seq: u64,
}

#[derive(Clone, Default)]
struct RivetStateData {
    last_applied: Option<LogId<u64>>,
    last_membership: StoredMembership<u64, BasicNode>,
    applied_commands: Vec<Vec<u8>>,
}

#[derive(Clone)]
struct StoredSnapshot {
    meta: SnapshotMeta<u64, BasicNode>,
    data: Vec<u8>,
}

#[derive(Clone)]
pub struct RivetLogStore {
    core: Arc<RwLock<RivetStorageCore>>,
}

#[derive(Clone)]
pub struct RivetLogReader {
    core: Arc<RwLock<RivetStorageCore>>,
}

#[derive(Clone)]
pub struct RivetStateMachine {
    core: Arc<RwLock<RivetStorageCore>>,
}

pub struct RivetSnapshotBuilder {
    core: Arc<RwLock<RivetStorageCore>>,
}

impl RaftLogReader<RivetRaftConfig> for RivetLogStore {
    async fn try_get_log_entries<RB>(
        &mut self,
        range: RB,
    ) -> Result<Vec<Entry<RivetRaftConfig>>, StorageError<u64>>
    where
        RB: RangeBounds<u64> + Clone + std::fmt::Debug + OptionalSend,
    {
        let core = self.core.read().await;
        let entries = core
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
    ) -> Result<Vec<Entry<RivetRaftConfig>>, StorageError<u64>>
    where
        RB: RangeBounds<u64> + Clone + std::fmt::Debug + OptionalSend,
    {
        let core = self.core.read().await;
        let entries = core
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

    async fn get_log_state(&mut self) -> Result<LogState<RivetRaftConfig>, StorageError<u64>> {
        let core = self.core.read().await;
        let last_purged = core.last_purged;
        let last_log = core.log.last().map(|entry| entry.log_id).or(last_purged);

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

    async fn save_vote(&mut self, vote: &Vote<u64>) -> Result<(), StorageError<u64>> {
        self.core.write().await.vote = Some(*vote);
        Ok(())
    }

    async fn read_vote(&mut self) -> Result<Option<Vote<u64>>, StorageError<u64>> {
        let guard = self.core.read().await;
        Ok(guard.vote)
    }

    async fn save_committed(
        &mut self,
        committed: Option<LogId<u64>>,
    ) -> Result<(), StorageError<u64>> {
        self.core.write().await.committed = committed;
        Ok(())
    }

    async fn read_committed(&mut self) -> Result<Option<LogId<u64>>, StorageError<u64>> {
        let guard = self.core.read().await;
        Ok(guard.committed)
    }

    async fn append<I>(
        &mut self,
        entries: I,
        callback: LogFlushed<RivetRaftConfig>,
    ) -> Result<(), StorageError<u64>>
    where
        I: IntoIterator<Item = Entry<RivetRaftConfig>> + OptionalSend,
        I::IntoIter: OptionalSend,
    {
        let mut core = self.core.write().await;
        for entry in entries {
            truncate_from(&mut core.log, entry.log_id.index);
            core.log.push(entry);
        }
        callback.log_io_completed(Ok(()));
        Ok(())
    }

    async fn truncate(&mut self, log_id: LogId<u64>) -> Result<(), StorageError<u64>> {
        let mut core = self.core.write().await;
        core.log.retain(|entry| entry.log_id.index < log_id.index);
        Ok(())
    }

    async fn purge(&mut self, log_id: LogId<u64>) -> Result<(), StorageError<u64>> {
        let mut core = self.core.write().await;
        core.log.retain(|entry| entry.log_id.index > log_id.index);
        core.last_purged = Some(log_id);
        Ok(())
    }
}

impl RaftStateMachine<RivetRaftConfig> for RivetStateMachine {
    type SnapshotBuilder = RivetSnapshotBuilder;

    async fn applied_state(
        &mut self,
    ) -> Result<(Option<LogId<u64>>, StoredMembership<u64, BasicNode>), StorageError<u64>> {
        let core = self.core.read().await;
        Ok((core.state.last_applied, core.state.last_membership.clone()))
    }

    async fn apply<I>(&mut self, entries: I) -> Result<Vec<()>, StorageError<u64>>
    where
        I: IntoIterator<Item = Entry<RivetRaftConfig>> + OptionalSend,
        I::IntoIter: OptionalSend,
    {
        let mut core = self.core.write().await;
        let mut responses = Vec::new();

        for entry in entries {
            let log_id = entry.log_id;
            core.state.last_applied = Some(log_id);

            match entry.payload {
                EntryPayload::Blank => {}
                EntryPayload::Normal(data) => {
                    core.state.applied_commands.push(data);
                }
                EntryPayload::Membership(membership) => {
                    core.state.last_membership = StoredMembership::new(Some(log_id), membership);
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
    ) -> Result<Box<<RivetRaftConfig as openraft::RaftTypeConfig>::SnapshotData>, StorageError<u64>>
    {
        Ok(Box::new(Cursor::new(Vec::new())))
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<u64, BasicNode>,
        snapshot: Box<<RivetRaftConfig as openraft::RaftTypeConfig>::SnapshotData>,
    ) -> Result<(), StorageError<u64>> {
        let mut core = self.core.write().await;
        let data = snapshot.into_inner();
        core.snapshot = Some(StoredSnapshot {
            meta: meta.clone(),
            data,
        });
        core.state.last_applied = meta.last_log_id;
        core.state.last_membership = meta.last_membership.clone();
        Ok(())
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<Snapshot<RivetRaftConfig>>, StorageError<u64>> {
        let core = self.core.read().await;
        Ok(core.snapshot.as_ref().map(|stored| Snapshot {
            meta: stored.meta.clone(),
            snapshot: Box::new(Cursor::new(stored.data.clone())),
        }))
    }
}

impl RaftSnapshotBuilder<RivetRaftConfig> for RivetSnapshotBuilder {
    async fn build_snapshot(&mut self) -> Result<Snapshot<RivetRaftConfig>, StorageError<u64>> {
        let mut core = self.core.write().await;
        core.snapshot_seq += 1;
        let snapshot_id = format!("rivet-snapshot-{}", core.snapshot_seq);
        let meta = SnapshotMeta {
            last_log_id: core.state.last_applied,
            last_membership: core.state.last_membership.clone(),
            snapshot_id,
        };
        let data = Vec::new();
        core.snapshot = Some(StoredSnapshot {
            meta: meta.clone(),
            data: data.clone(),
        });
        Ok(Snapshot {
            meta,
            snapshot: Box::new(Cursor::new(data)),
        })
    }
}

/// Representation of a node registered with the in-memory router.
#[derive(Clone)]
pub(crate) struct NodeEntry {
    raft: Option<RivetRaft>,
    node: Option<BasicNode>,
}

impl NodeEntry {
    fn new(raft: Option<RivetRaft>, node: Option<BasicNode>) -> Self {
        Self { raft, node }
    }
}

#[derive(Default)]
pub struct RaftRegistry {
    nodes: RwLock<HashMap<u64, NodeEntry>>,
}

impl RaftRegistry {
    pub async fn register(&self, node_id: u64, raft: RivetRaft, node: BasicNode) {
        self.nodes
            .write()
            .await
            .insert(node_id, NodeEntry::new(Some(raft), Some(node)));
    }

    pub async fn unregister(&self, node_id: u64) {
        self.nodes.write().await.remove(&node_id);
    }

    pub async fn set_node_info(&self, node_id: u64, node: BasicNode) {
        let mut guard = self.nodes.write().await;
        guard
            .entry(node_id)
            .and_modify(|entry| entry.node = Some(node.clone()))
            .or_insert_with(|| NodeEntry::new(None, Some(node)));
    }

    pub(crate) async fn get(&self, node_id: u64) -> Option<NodeEntry> {
        self.nodes.read().await.get(&node_id).cloned()
    }

    pub async fn clear(&self) {
        self.nodes.write().await.clear();
    }
}

/// Network factory handing out client transports backed by the in-memory registry.
#[derive(Clone)]
pub struct RivetNetworkFactory {
    registry: Arc<RaftRegistry>,
}

impl RivetNetworkFactory {
    pub fn new(registry: Arc<RaftRegistry>) -> Self {
        Self { registry }
    }
}

#[derive(Clone)]
pub struct RivetNetwork {
    registry: Arc<RaftRegistry>,
    target: u64,
}

impl RaftNetworkFactory<RivetRaftConfig> for RivetNetworkFactory {
    type Network = RivetNetwork;

    async fn new_client(&mut self, target: u64, node: &BasicNode) -> Self::Network {
        self.registry.set_node_info(target, node.clone()).await;
        RivetNetwork {
            registry: self.registry.clone(),
            target,
        }
    }
}

impl RaftNetwork<RivetRaftConfig> for RivetNetwork {
    async fn append_entries(
        &mut self,
        rpc: openraft::raft::AppendEntriesRequest<RivetRaftConfig>,
        _option: RPCOption,
    ) -> Result<openraft::raft::AppendEntriesResponse<u64>, RPCError<u64, BasicNode, RaftError<u64>>>
    {
        let entry = self
            .registry
            .get(self.target)
            .await
            .ok_or_else(|| unreachable_error::<Infallible>(self.target))?;
        let node_info = entry.node.clone();
        let raft = entry
            .raft
            .clone()
            .ok_or_else(|| unreachable_error::<Infallible>(self.target))?;

        match raft.append_entries(rpc).await {
            Ok(resp) => Ok(resp),
            Err(err) => {
                let remote = match node_info {
                    Some(node) => RemoteError::new_with_node(self.target, node, err),
                    None => RemoteError::new(self.target, err),
                };
                Err(RPCError::from(remote))
            }
        }
    }

    async fn install_snapshot(
        &mut self,
        rpc: openraft::raft::InstallSnapshotRequest<RivetRaftConfig>,
        _option: RPCOption,
    ) -> Result<
        openraft::raft::InstallSnapshotResponse<u64>,
        RPCError<u64, BasicNode, RaftError<u64, InstallSnapshotError>>,
    > {
        let entry = self
            .registry
            .get(self.target)
            .await
            .ok_or_else(|| unreachable_error::<InstallSnapshotError>(self.target))?;
        let node_info = entry.node.clone();
        let raft = entry
            .raft
            .clone()
            .ok_or_else(|| unreachable_error::<InstallSnapshotError>(self.target))?;

        match raft.install_snapshot(rpc).await {
            Ok(resp) => Ok(resp),
            Err(err) => {
                let remote = match node_info {
                    Some(node) => RemoteError::new_with_node(self.target, node, err),
                    None => RemoteError::new(self.target, err),
                };
                Err(RPCError::from(remote))
            }
        }
    }

    async fn vote(
        &mut self,
        rpc: openraft::raft::VoteRequest<u64>,
        _option: RPCOption,
    ) -> Result<openraft::raft::VoteResponse<u64>, RPCError<u64, BasicNode, RaftError<u64>>> {
        let entry = self
            .registry
            .get(self.target)
            .await
            .ok_or_else(|| unreachable_error::<Infallible>(self.target))?;
        let node_info = entry.node.clone();
        let raft = entry
            .raft
            .clone()
            .ok_or_else(|| unreachable_error::<Infallible>(self.target))?;

        match raft.vote(rpc).await {
            Ok(resp) => Ok(resp),
            Err(err) => {
                let remote = match node_info {
                    Some(node) => RemoteError::new_with_node(self.target, node, err),
                    None => RemoteError::new(self.target, err),
                };
                Err(RPCError::from(remote))
            }
        }
    }
}

/// Build an `Unreachable` error for missing peers.
fn unreachable_error<E>(target: u64) -> RPCError<u64, BasicNode, RaftError<u64, E>>
where
    E: std::error::Error + Send + Sync + 'static,
{
    let err = std::io::Error::new(
        std::io::ErrorKind::NotFound,
        format!("Raft peer {target} is not registered"),
    );
    RPCError::Unreachable(Unreachable::new(&err))
}

/// Utility to trim log entries starting from `start_index`.
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

/// Build a default Openraft configuration tuned for single-process integration tests.
pub fn default_raft_config() -> Arc<Config> {
    let config = Config {
        enable_tick: true,
        ..Config::default()
    };
    Arc::new(
        config
            .validate()
            .expect("default Raft config should validate"),
    )
}

/// Surface Raft metrics from the registry for debugging.
pub async fn collect_metrics(node_id: u64) -> Option<RaftMetrics<u64, BasicNode>> {
    registry().get(node_id).await.and_then(|entry| {
        entry
            .raft
            .as_ref()
            .map(|raft| raft.metrics().borrow().clone())
    })
}
