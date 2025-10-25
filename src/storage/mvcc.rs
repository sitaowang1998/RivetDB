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
        let insert_idx = self
            .versions
            .partition_point(|existing| existing.commit_ts <= value.commit_ts);
        self.versions.insert(insert_idx, value);
    }

    pub fn latest_visible(&self, snapshot_ts: Timestamp) -> Option<&VersionedValue> {
        let idx = self
            .versions
            .partition_point(|version| version.commit_ts <= snapshot_ts);
        if idx == 0 {
            None
        } else {
            self.versions.get(idx - 1)
        }
    }

    pub fn latest_commit_ts(&self) -> Option<Timestamp> {
        self.versions.last().map(|version| version.commit_ts)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn append_orders_versions() {
        let mut chain = VersionChain::default();
        chain.append(VersionedValue::new(b"third".to_vec(), 30));
        chain.append(VersionedValue::new(b"first".to_vec(), 10));
        chain.append(VersionedValue::new(b"second".to_vec(), 20));

        let commit_ts: Vec<Timestamp> = chain.versions.iter().map(|v| v.commit_ts).collect();
        assert_eq!(commit_ts, vec![10, 20, 30]);

        let values: Vec<Vec<u8>> = chain.versions.iter().map(|v| v.value.clone()).collect();
        assert_eq!(
            values,
            vec![b"first".to_vec(), b"second".to_vec(), b"third".to_vec()]
        );
    }

    #[test]
    fn latest_visible_returns_correct_version() {
        let mut chain = VersionChain::default();
        chain.append(VersionedValue::new(b"v1".to_vec(), 5));
        chain.append(VersionedValue::new(b"v2".to_vec(), 10));
        chain.append(VersionedValue::new(b"v3".to_vec(), 15));

        assert!(chain.latest_visible(0).is_none());
        assert_eq!(
            chain.latest_visible(10).map(|v| v.value.clone()),
            Some(b"v2".to_vec())
        );
        assert_eq!(
            chain.latest_visible(12).map(|v| v.value.clone()),
            Some(b"v2".to_vec())
        );
        assert_eq!(
            chain.latest_visible(20).map(|v| v.value.clone()),
            Some(b"v3".to_vec())
        );
    }
}
