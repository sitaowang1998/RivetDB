use std::sync::Arc;

#[path = "common.rs"]
mod common;

use common::{TestStorage, all_backends};
use rivetdb::storage::StorageEngine;
use rivetdb::transaction::{Snapshot, TransactionMetadata, WriteIntent};
use rivetdb::types::{Timestamp, TxnId};

fn new_txn(snapshot_ts: Timestamp) -> TransactionMetadata {
    TransactionMetadata::new(TxnId::new(), Snapshot::new(snapshot_ts))
}

async fn commit_flow<E: StorageEngine + 'static>(storage: Arc<E>) {
    let key = "inventory".to_string();

    let txn1 = new_txn(5);
    storage
        .stage_write(
            txn1.id(),
            txn1.snapshot_ts(),
            WriteIntent {
                key: key.clone(),
                value: b"v1".to_vec(),
            },
        )
        .await
        .unwrap();
    let writes = storage.drain_writes(txn1.id()).await;
    storage.apply_committed(10, writes).await.unwrap();

    let snapshot = storage.read(&key, 10).await.unwrap();
    assert_eq!(snapshot.unwrap().value, b"v1");

    let mut txn2 = new_txn(12);
    txn2.record_read(key.clone());
    storage.validate(&txn2).await.unwrap();
    storage
        .stage_write(
            txn2.id(),
            txn2.snapshot_ts(),
            WriteIntent {
                key: key.clone(),
                value: b"v2".to_vec(),
            },
        )
        .await
        .unwrap();
    let writes = storage.drain_writes(txn2.id()).await;
    storage.apply_committed(15, writes).await.unwrap();

    let latest = storage.read(&key, 20).await.unwrap();
    assert_eq!(latest.unwrap().value, b"v2");
}

async fn abort_discards_staged_write<E: StorageEngine + 'static>(storage: Arc<E>) {
    let key = "abort".to_string();
    let txn = new_txn(7);

    storage
        .stage_write(
            txn.id(),
            txn.snapshot_ts(),
            WriteIntent {
                key: key.clone(),
                value: b"pending".to_vec(),
            },
        )
        .await
        .unwrap();

    storage.abort(txn.id()).await;
    let writes = storage.drain_writes(txn.id()).await;
    storage.apply_committed(12, writes).await.unwrap();

    let read = storage.read(&key, 20).await.unwrap();
    assert!(read.is_none());
}

#[tokio::test]
async fn storage_backends_commit_flow() {
    for backend in all_backends() {
        let storage = TestStorage::new(backend);
        commit_flow(storage.storage()).await;
    }
}

#[tokio::test]
async fn storage_backends_abort_discards_intent() {
    for backend in all_backends() {
        let storage = TestStorage::new(backend);
        abort_discards_staged_write(storage.storage()).await;
    }
}
