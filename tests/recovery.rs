use std::sync::Arc;

use rivetdb::storage::InMemoryStorage;
use rivetdb::{RivetConfig, RivetNode, reset_registry};

#[tokio::test(flavor = "multi_thread")]
async fn node_recovers_committed_state() {
    reset_registry().await;

    let data_dir = tempfile::tempdir().expect("create temp dir");
    let config = RivetConfig::new(
        42,
        "127.0.0.1:7800",
        Vec::new(),
        Some(data_dir.path().join("node-data")),
    );

    let storage = Arc::new(InMemoryStorage::new());
    let node = RivetNode::new(config.clone(), storage.clone())
        .await
        .expect("bootstrap node");
    let manager = node.transaction_manager();

    let txn = manager.begin_transaction();
    manager
        .write(&txn, "persisted".to_string(), b"value".to_vec())
        .await
        .expect("stage write");
    let collected = manager.collect(txn).await.expect("collect txn");
    let receipt = node
        .replicate_commit(collected)
        .await
        .expect("replicate commit");
    assert!(receipt.commit_ts > 0);

    drop(node);
    drop(storage);

    let recovered_storage = Arc::new(InMemoryStorage::new());
    let recovered_node = RivetNode::new(config, recovered_storage.clone())
        .await
        .expect("recover node");
    let recovered_manager = recovered_node.transaction_manager();

    let mut reader = recovered_manager.begin_transaction();
    let recovered = recovered_manager
        .read(&mut reader, &"persisted".to_string())
        .await
        .expect("read result")
        .expect("value present");
    assert_eq!(recovered.value, b"value");
    recovered_manager.abort(reader).await;

    let txn2 = recovered_manager.begin_transaction();
    recovered_manager
        .write(&txn2, "persisted".to_string(), b"value2".to_vec())
        .await
        .expect("stage new write");
    let collected2 = recovered_manager.collect(txn2).await.expect("collect txn2");
    let receipt2 = recovered_node
        .replicate_commit(collected2)
        .await
        .expect("replicate second commit");
    assert!(
        receipt2.commit_ts > receipt.commit_ts,
        "clock should advance past recovered state"
    );
}
