use async_trait::async_trait;
use thiserror::Error;

use crate::transaction::{TransactionMetadata, WriteIntent};
use crate::types::{Key, Timestamp, TxnId};

use super::mvcc::VersionedValue;

/// Errors surfaced by storage implementations.
#[derive(Debug, Error)]
pub enum StorageError {
    #[error("key {0} not found")]
    NotFound(Key),
    #[error("validation failed due to conflicting writes")]
    ValidationConflict,
    #[error("MVCC state is corrupted: {0}")]
    CorruptedState(String),
    #[error("operation not yet implemented")]
    Unimplemented,
}

/// Abstract storage contract for MVCC-backed data.
#[async_trait]
pub trait StorageEngine: Send + Sync {
    async fn read(
        &self,
        key: &Key,
        snapshot_ts: Timestamp,
    ) -> Result<Option<VersionedValue>, StorageError>;

    async fn stage_write(
        &self,
        txn_id: &TxnId,
        snapshot_ts: Timestamp,
        intent: WriteIntent,
    ) -> Result<(), StorageError>;

    async fn validate(&self, txn: &TransactionMetadata) -> Result<(), StorageError>;

    async fn commit(
        &self,
        txn: &TransactionMetadata,
        commit_ts: Timestamp,
    ) -> Result<(), StorageError>;

    async fn abort(&self, txn_id: &TxnId);
}
