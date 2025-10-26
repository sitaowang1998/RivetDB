use std::sync::Arc;

use rivetdb::storage::{InMemoryStorage, StorageEngine};
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
    storage.commit(&txn1, 10).await.unwrap();

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
    storage.commit(&txn2, 15).await.unwrap();

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
    storage.commit(&txn, 12).await.unwrap();

    let read = storage.read(&key, 20).await.unwrap();
    assert!(read.is_none());
}

#[tokio::test]
async fn in_memory_storage_commit_flow() {
    commit_flow(Arc::new(InMemoryStorage::new())).await;
}

#[tokio::test]
async fn in_memory_storage_abort_discards_intent() {
    abort_discards_staged_write(Arc::new(InMemoryStorage::new())).await;
}
