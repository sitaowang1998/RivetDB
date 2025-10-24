use serde::{Deserialize, Serialize};

use crate::types::{Timestamp, Value};

/// MVCC value paired with the commit timestamp that produced it.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct VersionedValue {
    pub value: Value,
    pub commit_ts: Timestamp,
}

impl VersionedValue {
    pub fn new(value: Value, commit_ts: Timestamp) -> Self {
        Self { value, commit_ts }
    }
}

/// Chronological series of versions for a single key.
#[derive(Debug, Default, Clone)]
pub struct VersionChain {
    pub versions: Vec<VersionedValue>,
}

impl VersionChain {
    pub fn append(&mut self, value: VersionedValue) {
        self.versions.push(value);
        self.versions
            .sort_by(|a, b| a.commit_ts.cmp(&b.commit_ts));
    }

    pub fn latest_visible(&self, snapshot_ts: Timestamp) -> Option<&VersionedValue> {
        self.versions
            .iter()
            .rev()
            .find(|version| version.commit_ts <= snapshot_ts)
    }
}
