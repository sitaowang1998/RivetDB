use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;

use crate::storage::{StorageEngine, StorageError, VersionedValue};
use crate::transaction::{TransactionMetadata, WriteIntent};
use crate::types::{Key, Timestamp, TxnId, Value};

/// Simple file-backed storage implementing the `StorageEngine` trait.
///
/// This is intended for local persistence and benchmarks, not for production
/// durability guarantees.
pub struct OnDiskStorage {
    data_file: PathBuf,
    state: RwLock<StoredState>,
    staged_writes: RwLock<HashMap<TxnId, HashMap<Key, Value>>>,
}

impl OnDiskStorage {
    pub fn open(dir: impl AsRef<Path>) -> Result<Self, StorageError> {
        let dir = dir.as_ref();
        fs::create_dir_all(dir).map_err(to_storage_error)?;
        let data_file = dir.join("kv.json");
        let state = if data_file.exists() {
            let bytes = fs::read(&data_file).map_err(to_storage_error)?;
            serde_json::from_slice(&bytes).map_err(to_storage_error)?
        } else {
            StoredState::default()
        };

        Ok(Self {
            data_file,
            state: RwLock::new(state),
            staged_writes: RwLock::new(HashMap::new()),
        })
    }

    fn persist(&self, state: &StoredState) -> Result<(), StorageError> {
        let serialized = serde_json::to_vec(state).map_err(to_storage_error)?;
        let tmp = self.data_file.with_extension("tmp");
        fs::write(&tmp, serialized).map_err(to_storage_error)?;
        fs::rename(tmp, &self.data_file).map_err(to_storage_error)?;
        Ok(())
    }
}

#[derive(Default, Serialize, Deserialize)]
struct StoredState {
    chains: HashMap<Key, Vec<VersionedValue>>,
}

#[async_trait::async_trait]
impl StorageEngine for OnDiskStorage {
    async fn read(
        &self,
        key: &Key,
        snapshot_ts: Timestamp,
    ) -> Result<Option<VersionedValue>, StorageError> {
        let state = self.state.read().await;
        Ok(state
            .chains
            .get(key)
            .and_then(|chain| latest_visible(chain, snapshot_ts).cloned()))
    }

    async fn stage_write(
        &self,
        txn_id: &TxnId,
        _snapshot_ts: Timestamp,
        intent: WriteIntent,
    ) -> Result<(), StorageError> {
        let mut staged = self.staged_writes.write().await;
        let txn_entry = staged.entry(txn_id.clone()).or_insert_with(HashMap::new);

        let WriteIntent { key, value } = intent;
        txn_entry.insert(key, value);

        Ok(())
    }

    async fn validate(&self, txn: &TransactionMetadata) -> Result<(), StorageError> {
        let state = self.state.read().await;
        let snapshot_ts = txn.snapshot_ts();

        for key in txn.read_keys() {
            if let Some(chain) = state.chains.get(key)
                && chain
                    .iter()
                    .map(|v| v.commit_ts)
                    .max()
                    .map(|ts| ts > snapshot_ts)
                    .unwrap_or(false)
            {
                return Err(StorageError::ValidationConflict);
            }
        }

        Ok(())
    }

    async fn drain_writes(&self, txn_id: &TxnId) -> Vec<WriteIntent> {
        let mut staged = self.staged_writes.write().await;
        staged
            .remove(txn_id)
            .map(|writes| {
                writes
                    .into_iter()
                    .map(|(key, value)| WriteIntent { key, value })
                    .collect()
            })
            .unwrap_or_default()
    }

    async fn apply_committed(
        &self,
        commit_ts: Timestamp,
        writes: Vec<WriteIntent>,
    ) -> Result<(), StorageError> {
        if writes.is_empty() {
            return Ok(());
        }

        let mut state = self.state.write().await;
        for WriteIntent { key, value } in writes {
            let chain = state.chains.entry(key).or_insert_with(Vec::new);
            insert_version(chain, VersionedValue::new(value, commit_ts));
        }

        self.persist(&state)?;
        Ok(())
    }

    async fn abort(&self, txn_id: &TxnId) {
        let mut staged = self.staged_writes.write().await;
        staged.remove(txn_id);
    }
}

fn latest_visible(chain: &[VersionedValue], snapshot_ts: Timestamp) -> Option<&VersionedValue> {
    let idx = chain.partition_point(|version| version.commit_ts <= snapshot_ts);
    if idx == 0 { None } else { chain.get(idx - 1) }
}

fn insert_version(chain: &mut Vec<VersionedValue>, value: VersionedValue) {
    let idx = chain.partition_point(|existing| existing.commit_ts <= value.commit_ts);
    chain.insert(idx, value);
}

fn to_storage_error(err: impl ToString) -> StorageError {
    StorageError::CorruptedState(err.to_string())
}
