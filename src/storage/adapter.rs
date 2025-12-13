use std::path::Path;

use crate::storage::{InMemoryStorage, OnDiskStorage, StorageEngine, StorageError};
use crate::transaction::{TransactionMetadata, WriteIntent};
use crate::types::{Key, Timestamp, TxnId};

/// Runtime-selectable storage wrapper so binaries can switch between memory and disk.
pub enum StorageAdapter {
    Memory(InMemoryStorage),
    Disk(OnDiskStorage),
}

impl StorageAdapter {
    pub fn memory() -> Self {
        Self::Memory(InMemoryStorage::new())
    }

    pub fn disk(path: impl AsRef<Path>) -> Result<Self, StorageError> {
        Ok(Self::Disk(OnDiskStorage::open(path)?))
    }
}

#[async_trait::async_trait]
impl StorageEngine for StorageAdapter {
    async fn read(
        &self,
        key: &Key,
        snapshot_ts: Timestamp,
    ) -> Result<Option<crate::storage::VersionedValue>, StorageError> {
        match self {
            StorageAdapter::Memory(inner) => inner.read(key, snapshot_ts).await,
            StorageAdapter::Disk(inner) => inner.read(key, snapshot_ts).await,
        }
    }

    async fn stage_write(
        &self,
        txn_id: &TxnId,
        snapshot_ts: Timestamp,
        intent: WriteIntent,
    ) -> Result<(), StorageError> {
        match self {
            StorageAdapter::Memory(inner) => inner.stage_write(txn_id, snapshot_ts, intent).await,
            StorageAdapter::Disk(inner) => inner.stage_write(txn_id, snapshot_ts, intent).await,
        }
    }

    async fn validate(&self, txn: &TransactionMetadata) -> Result<(), StorageError> {
        match self {
            StorageAdapter::Memory(inner) => inner.validate(txn).await,
            StorageAdapter::Disk(inner) => inner.validate(txn).await,
        }
    }

    async fn drain_writes(&self, txn_id: &TxnId) -> Vec<WriteIntent> {
        match self {
            StorageAdapter::Memory(inner) => inner.drain_writes(txn_id).await,
            StorageAdapter::Disk(inner) => inner.drain_writes(txn_id).await,
        }
    }

    async fn apply_committed(
        &self,
        commit_ts: Timestamp,
        writes: Vec<WriteIntent>,
    ) -> Result<(), StorageError> {
        match self {
            StorageAdapter::Memory(inner) => inner.apply_committed(commit_ts, writes).await,
            StorageAdapter::Disk(inner) => inner.apply_committed(commit_ts, writes).await,
        }
    }

    async fn abort(&self, txn_id: &TxnId) {
        match self {
            StorageAdapter::Memory(inner) => inner.abort(txn_id).await,
            StorageAdapter::Disk(inner) => inner.abort(txn_id).await,
        }
    }
}
