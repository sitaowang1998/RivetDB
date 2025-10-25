use std::collections::HashMap;

use tokio::sync::RwLock;

use crate::transaction::{TransactionMetadata, WriteIntent};
use crate::types::{Key, Timestamp, TxnId, Value};

use super::engine::{StorageEngine, StorageError};
use super::mvcc::{VersionChain, VersionedValue};

/// In-memory implementation of the `StorageEngine` trait backed by MVCC version chains.
#[derive(Debug, Default)]
pub struct InMemoryStorage {
    version_chains: RwLock<HashMap<Key, VersionChain>>,
    staged_writes: RwLock<HashMap<TxnId, HashMap<Key, Value>>>,
}

impl InMemoryStorage {
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait::async_trait]
impl StorageEngine for InMemoryStorage {
    async fn read(
        &self,
        key: &Key,
        snapshot_ts: Timestamp,
    ) -> Result<Option<VersionedValue>, StorageError> {
        let store = self.version_chains.read().await;
        Ok(store
            .get(key)
            .and_then(|chain| chain.latest_visible(snapshot_ts).cloned()))
    }

    async fn stage_write(
        &self,
        txn: &TransactionMetadata,
        intent: WriteIntent,
    ) -> Result<(), StorageError> {
        let mut staged = self.staged_writes.write().await;
        let txn_entry = staged.entry(txn.id().clone()).or_insert_with(HashMap::new);

        let WriteIntent { key, value } = intent;
        txn_entry.insert(key, value);

        Ok(())
    }

    async fn validate(&self, txn: &TransactionMetadata) -> Result<(), StorageError> {
        let store = self.version_chains.read().await;
        let snapshot_ts = txn.snapshot_ts();

        for key in txn.read_keys() {
            if let Some(chain) = store.get(key) {
                if chain
                    .latest_commit_ts()
                    .map(|ts| ts > snapshot_ts)
                    .unwrap_or(false)
                {
                    return Err(StorageError::ValidationConflict);
                }
            }
        }

        Ok(())
    }

    async fn commit(
        &self,
        txn: &TransactionMetadata,
        commit_ts: Timestamp,
    ) -> Result<(), StorageError> {
        let writes = {
            let mut staged = self.staged_writes.write().await;
            staged.remove(txn.id()).unwrap_or_default()
        };

        if writes.is_empty() {
            return Ok(());
        }

        let mut store = self.version_chains.write().await;
        for (key, value) in writes {
            let chain = store.entry(key).or_insert_with(VersionChain::default);
            chain.append(VersionedValue::new(value, commit_ts));
        }

        Ok(())
    }

    async fn abort(&self, txn_id: &TxnId) {
        let mut staged = self.staged_writes.write().await;
        staged.remove(txn_id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transaction::{Snapshot, TransactionMetadata};
    use crate::types::TxnId;

    fn make_txn(snapshot_ts: Timestamp) -> TransactionMetadata {
        TransactionMetadata::new(TxnId::new(), Snapshot::new(snapshot_ts))
    }

    async fn stage_and_commit(
        storage: &InMemoryStorage,
        key: &str,
        value: &[u8],
        commit_ts: Timestamp,
    ) {
        let txn = make_txn(commit_ts);
        storage
            .stage_write(
                &txn,
                WriteIntent {
                    key: key.to_string(),
                    value: value.to_vec(),
                },
            )
            .await
            .unwrap();
        storage.commit(&txn, commit_ts).await.unwrap();
    }

    #[tokio::test]
    async fn commit_persists_version() {
        let storage = InMemoryStorage::default();
        let key = "alpha".to_string();
        let txn = make_txn(5);

        storage
            .stage_write(
                &txn,
                WriteIntent {
                    key: key.clone(),
                    value: b"value1".to_vec(),
                },
            )
            .await
            .unwrap();

        storage.commit(&txn, 10).await.unwrap();

        let read = storage.read(&key, 10).await.unwrap();
        let version = read.expect("expected committed value");
        assert_eq!(version.commit_ts, 10);
        assert_eq!(version.value, b"value1");
    }

    #[tokio::test]
    async fn snapshot_reads_return_correct_version() {
        let storage = InMemoryStorage::default();
        stage_and_commit(&storage, "k", b"v1", 5).await;
        stage_and_commit(&storage, "k", b"v2", 15).await;

        let older = storage.read(&"k".to_string(), 10).await.unwrap();
        assert_eq!(older.unwrap().value, b"v1");

        let latest = storage.read(&"k".to_string(), 20).await.unwrap();
        assert_eq!(latest.unwrap().value, b"v2");
    }

    #[tokio::test]
    async fn validate_detects_conflicting_writes() {
        let storage = InMemoryStorage::default();
        stage_and_commit(&storage, "x", b"base", 5).await;

        let mut txn = make_txn(8);
        txn.record_read("x".to_string());

        storage
            .validate(&txn)
            .await
            .expect("validation should succeed with no conflicts");

        stage_and_commit(&storage, "x", b"new", 9).await;

        let err = storage.validate(&txn).await.unwrap_err();
        matches!(err, StorageError::ValidationConflict)
            .then_some(())
            .expect("expected validation conflict");
    }
}
