use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use serde::{Deserialize, Serialize};

use crate::storage::{StorageEngine, StorageError, VersionedValue};
use crate::types::{Key, Timestamp, TxnId, Value};

/// Snapshot metadata captured when a transaction begins.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Snapshot {
    pub read_ts: Timestamp,
}

impl Snapshot {
    pub fn new(read_ts: Timestamp) -> Self {
        Self { read_ts }
    }
}

/// Simplified write intent captured during transaction execution.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct WriteIntent {
    pub key: Key,
    pub value: Value,
}

/// In-flight transaction metadata propagated through the system.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TransactionMetadata {
    pub id: TxnId,
    pub snapshot: Snapshot,
    pub read_keys: Vec<Key>,
}

impl TransactionMetadata {
    pub fn new(id: TxnId, snapshot: Snapshot) -> Self {
        Self {
            id,
            snapshot,
            read_keys: Vec::new(),
        }
    }

    pub fn snapshot_ts(&self) -> Timestamp {
        self.snapshot.read_ts
    }

    pub fn id(&self) -> &TxnId {
        &self.id
    }

    pub fn read_keys(&self) -> &[Key] {
        &self.read_keys
    }

    pub fn record_read(&mut self, key: Key) {
        self.read_keys.push(key);
    }
}

/// Lifecycle states for a transaction as it moves through validation.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum TransactionState {
    Pending,
    Validating,
    Committed,
    Aborted,
}

/// Mutable transaction handle owned by the caller while building up read/write sets.
#[derive(Debug, Clone)]
pub struct TransactionContext {
    metadata: TransactionMetadata,
}

impl TransactionContext {
    pub fn metadata(&self) -> &TransactionMetadata {
        &self.metadata
    }

    fn metadata_mut(&mut self) -> &mut TransactionMetadata {
        &mut self.metadata
    }

    pub fn id(&self) -> &TxnId {
        self.metadata.id()
    }

    pub fn snapshot_ts(&self) -> Timestamp {
        self.metadata.snapshot_ts()
    }
}

/// Result returned when a transaction successfully commits.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CommitReceipt {
    pub commit_ts: Timestamp,
}

/// Coordinates optimistic transactions over an MVCC storage engine.
pub struct TransactionManager<S: StorageEngine> {
    storage: Arc<S>,
    clock: AtomicU64,
}

impl<S: StorageEngine> TransactionManager<S> {
    pub fn new(storage: Arc<S>) -> Self {
        Self {
            storage,
            clock: AtomicU64::new(0),
        }
    }

    pub fn begin_transaction(&self) -> TransactionContext {
        let snapshot_ts = self.clock.load(Ordering::SeqCst);
        let metadata = TransactionMetadata::new(TxnId::new(), Snapshot::new(snapshot_ts));
        TransactionContext { metadata }
    }

    pub async fn read(
        &self,
        txn: &mut TransactionContext,
        key: &Key,
    ) -> Result<Option<VersionedValue>, StorageError> {
        let snapshot_ts = txn.snapshot_ts();
        let value = self.storage.read(key, snapshot_ts).await?;
        txn.metadata_mut().record_read(key.clone());
        Ok(value)
    }

    pub async fn write(
        &self,
        txn: &TransactionContext,
        key: Key,
        value: Value,
    ) -> Result<(), StorageError> {
        self.storage
            .stage_write(txn.id(), txn.snapshot_ts(), WriteIntent { key, value })
            .await
    }

    pub async fn commit(&self, txn: TransactionContext) -> Result<CommitReceipt, StorageError> {
        self.storage.validate(txn.metadata()).await?;
        let commit_ts = self.next_ts();
        self.storage.commit(txn.metadata(), commit_ts).await?;
        Ok(CommitReceipt { commit_ts })
    }

    pub async fn abort(&self, txn: TransactionContext) {
        self.storage.abort(txn.metadata().id()).await;
    }

    fn next_ts(&self) -> Timestamp {
        self.clock.fetch_add(1, Ordering::SeqCst) + 1
    }
}
