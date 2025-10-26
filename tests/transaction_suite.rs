use std::sync::Arc;

use rivetdb::storage::{InMemoryStorage, StorageEngine};
use rivetdb::{RivetConfig, RivetNode};

async fn setup_node() -> (RivetNode<InMemoryStorage>, Arc<InMemoryStorage>) {
    let storage = Arc::new(InMemoryStorage::new());
    let node = RivetNode::new(RivetConfig::default(), storage.clone());
    (node, storage)
}

#[tokio::test]
async fn transaction_commit_flow() {
    let (node, _) = setup_node().await;
    let manager = node.transaction_manager();

    let mut reader = manager.begin_transaction();
    assert!(
        manager
            .read(&mut reader, &"item".to_string())
            .await
            .unwrap()
            .is_none()
    );

    let txn = manager.begin_transaction();
    manager
        .write(&txn, "item".to_string(), b"value".to_vec())
        .await
        .unwrap();
    manager.commit(txn).await.unwrap();

    let mut snapshot = manager.begin_transaction();
    let value = manager
        .read(&mut snapshot, &"item".to_string())
        .await
        .unwrap();
    assert_eq!(value.unwrap().value, b"value");
}

#[tokio::test]
async fn transaction_detects_read_write_conflict() {
    let (node, _) = setup_node().await;
    let manager = node.transaction_manager();

    let key = "conflict".to_string();
    let seed = manager.begin_transaction();
    manager
        .write(&seed, key.clone(), b"orig".to_vec())
        .await
        .unwrap();
    manager.commit(seed).await.unwrap();

    let mut reader = manager.begin_transaction();
    manager.read(&mut reader, &key).await.unwrap();

    let writer = manager.begin_transaction();
    manager
        .write(&writer, key.clone(), b"new".to_vec())
        .await
        .unwrap();
    manager.commit(writer).await.unwrap();

    let err = manager.commit(reader).await.unwrap_err();
    assert!(matches!(
        err,
        rivetdb::storage::StorageError::ValidationConflict
    ));
}

#[tokio::test]
async fn transaction_abort_drops_staged_writes() {
    let (node, storage) = setup_node().await;
    let manager = node.transaction_manager();

    let key = "k".to_string();
    let txn = manager.begin_transaction();
    manager
        .write(&txn, key.clone(), b"temp".to_vec())
        .await
        .unwrap();
    manager.abort(txn).await;

    let mut reader = manager.begin_transaction();
    let value = manager.read(&mut reader, &key).await.unwrap();
    assert!(value.is_none());
    assert!(storage.read(&key, 100).await.unwrap().is_none());
}

#[tokio::test]
async fn concurrent_writers_conflict() {
    let (node, _) = setup_node().await;
    let manager = node.transaction_manager();

    let key = "shared".to_string();
    let writer_a = manager.begin_transaction();
    let mut writer_b = manager.begin_transaction();

    manager
        .write(&writer_a, key.clone(), b"a".to_vec())
        .await
        .unwrap();
    manager.read(&mut writer_b, &key).await.unwrap();
    manager
        .write(&writer_b, key.clone(), b"b".to_vec())
        .await
        .unwrap();

    manager.commit(writer_a).await.unwrap();
    let err = manager.commit(writer_b).await.unwrap_err();
    assert!(matches!(
        err,
        rivetdb::storage::StorageError::ValidationConflict
    ));
}

#[tokio::test]
async fn writer_abort_allows_reader_commit() {
    let (node, storage) = setup_node().await;
    let manager = node.transaction_manager();

    let key = "conflict".to_string();
    let seed = manager.begin_transaction();
    manager
        .write(&seed, key.clone(), b"orig".to_vec())
        .await
        .unwrap();
    manager.commit(seed).await.unwrap();

    let mut reader = manager.begin_transaction();
    manager.read(&mut reader, &key).await.unwrap();

    let mut writer = manager.begin_transaction();
    manager.read(&mut writer, &key).await.unwrap();
    manager
        .write(&writer, key.clone(), b"new".to_vec())
        .await
        .unwrap();

    manager.abort(writer).await;
    manager.commit(reader).await.unwrap();

    let snapshot = storage.read(&key, 200).await.unwrap().unwrap();
    assert_eq!(snapshot.value, b"orig");
}

#[tokio::test]
async fn reader_commits_before_writer() {
    let (node, storage) = setup_node().await;
    let manager = node.transaction_manager();

    let key = "conflict".to_string();
    let seed = manager.begin_transaction();
    manager
        .write(&seed, key.clone(), b"orig".to_vec())
        .await
        .unwrap();
    manager.commit(seed).await.unwrap();

    let mut reader = manager.begin_transaction();
    manager.read(&mut reader, &key).await.unwrap();

    let mut writer = manager.begin_transaction();
    manager.read(&mut writer, &key).await.unwrap();
    manager
        .write(&writer, key.clone(), b"new".to_vec())
        .await
        .unwrap();

    manager.commit(reader).await.unwrap();
    manager.commit(writer).await.unwrap();

    let snapshot = storage.read(&key, 300).await.unwrap().unwrap();
    assert_eq!(snapshot.value, b"new");
}
