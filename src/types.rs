use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Logical key within the database.
pub type Key = String;
/// Raw value payload stored per version. MVCC layers may interpret the bytes.
pub type Value = Vec<u8>;
/// Logical timestamp used for version ordering and snapshots.
pub type Timestamp = u64;

/// Unique identifier assigned to every transaction.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct TxnId(Uuid);

impl Default for TxnId {
    fn default() -> Self {
        Self::new()
    }
}

impl TxnId {
    /// Creates a new transaction identifier using a random UUID.
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }

    /// Wraps an existing UUID. Useful for tests and deterministic flows.
    pub fn from_uuid(id: Uuid) -> Self {
        Self(id)
    }

    /// Exposes the inner UUID for logging or serialization.
    pub fn as_uuid(&self) -> Uuid {
        self.0
    }
}
