use serde::{Deserialize, Serialize};

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
