use std::sync::Arc;
use std::time::Duration;

use once_cell::sync::Lazy;

#[path = "common.rs"]
mod common;
use openraft::ServerState;
use rivetdb::CommitReceipt;
use rivetdb::node::CommitError;
use rivetdb::storage::{StorageAdapter, StorageEngine};
use rivetdb::types::Timestamp;
use rivetdb::{PeerConfig, RivetConfig, RivetNode, collect_metrics, registry, reset_registry};
use tokio::sync::Mutex;
use tokio::time::{Instant, sleep};

static TEST_GUARD: Lazy<Mutex<()>> = Lazy::new(|| Mutex::new(()));
const LEADER_TIMEOUT: Duration = Duration::from_secs(30);
const CONVERGENCE_TIMEOUT: Duration = Duration::from_secs(60);

#[tokio::test(flavor = "multi_thread")]
async fn multi_node_cluster_elects_single_leader() {
    let _guard = TEST_GUARD.lock().await;
    for backend in common::all_backends() {
        reset_registry().await;

        let layout = vec![
            (0_u64, "127.0.0.1:6100".to_string()),
            (1_u64, "127.0.0.1:6101".to_string()),
            (2_u64, "127.0.0.1:6102".to_string()),
        ];

        let (nodes, _, _guards) = spawn_cluster(&layout, backend).await;

        let ids: Vec<u64> = layout.iter().map(|(id, _)| *id).collect();
        let leader = wait_for_unique_leader(&ids, LEADER_TIMEOUT)
            .await
            .expect("cluster should elect exactly one leader");

        assert!(
            wait_for_cluster_convergence(&ids, leader, CONVERGENCE_TIMEOUT).await,
            "cluster did not converge on leader {leader}"
        );

        drop(nodes);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn replicated_commit_visible_on_followers() {
    let _guard = TEST_GUARD.lock().await;
    for backend in common::all_backends() {
        reset_registry().await;

        let layout = vec![
            (10_u64, "127.0.0.1:6200".to_string()),
            (11_u64, "127.0.0.1:6201".to_string()),
            (12_u64, "127.0.0.1:6202".to_string()),
        ];

        let (nodes, storages, _guards) = spawn_cluster(&layout, backend).await;
        let ids: Vec<u64> = layout.iter().map(|(id, _)| *id).collect();
        let leader = wait_for_unique_leader(&ids, LEADER_TIMEOUT)
            .await
            .expect("cluster should elect a leader");
        assert!(
            wait_for_cluster_convergence(&ids, leader, CONVERGENCE_TIMEOUT).await,
            "cluster did not converge on leader {leader}"
        );

        let leader_index = ids
            .iter()
            .position(|id| *id == leader)
            .expect("leader should exist in ids");
        let leader_node = nodes[leader_index].clone();

        let manager = leader_node.transaction_manager();
        let txn = manager.begin_transaction();
        manager
            .write(&txn, "replicated".to_string(), b"value".to_vec())
            .await
            .expect("stage write");

        let collected = manager.collect(txn).await.expect("collect transaction");
        let receipt = leader_node
            .replicate_commit(collected)
            .await
            .expect("raft commit");

        assert!(
            wait_for_value_on_all(&storages, "replicated", receipt.commit_ts, b"value").await,
            "replicated value not visible on all nodes"
        );
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn cluster_commits_with_follower_down() {
    let _guard = TEST_GUARD.lock().await;
    for backend in common::all_backends() {
        reset_registry().await;

        let layout = vec![
            (20_u64, "127.0.0.1:6300".to_string()),
            (21_u64, "127.0.0.1:6301".to_string()),
            (22_u64, "127.0.0.1:6302".to_string()),
        ];

        let (nodes, storages, _guards) = spawn_cluster(&layout, backend).await;
        let ids: Vec<u64> = layout.iter().map(|(id, _)| *id).collect();
        let leader = wait_for_unique_leader(&ids, LEADER_TIMEOUT)
            .await
            .expect("leader should be elected");

        let follower_id = ids
            .iter()
            .copied()
            .find(|id| *id != leader)
            .expect("cluster needs follower");

        let leader_index = ids
            .iter()
            .position(|id| *id == leader)
            .expect("leader index");
        let leader_node = nodes[leader_index].clone();

        let manager = leader_node.transaction_manager();
        let txn = manager.begin_transaction();
        manager
            .write(&txn, "follower-down".to_string(), b"value".to_vec())
            .await
            .expect("stage write");
        let collected = manager.collect(txn).await.expect("collect txn");
        let leader_clone = leader_node.clone();
        let commit = tokio::spawn(async move { leader_clone.replicate_commit(collected).await });

        sleep(Duration::from_millis(50)).await;
        shutdown_node(follower_id, &nodes, &layout).await;

        let commit_result = commit.await.expect("join commit task");

        let survivors: Vec<u64> = ids
            .iter()
            .copied()
            .filter(|id| *id != follower_id)
            .collect();

        let receipt = match commit_result {
            Ok(receipt) => {
                assert!(
                    wait_for_cluster_convergence(&survivors, leader, CONVERGENCE_TIMEOUT).await,
                    "leader should remain stable after follower shutdown"
                );
                receipt
            }
            Err(err) => {
                handle_commit_error(err, &ids, &nodes, &survivors, "follower-down", b"value").await
            }
        };

        let survivor_storages = storages_for_ids(&survivors, &layout, &storages);
        assert!(
            wait_for_value_on_all(
                &survivor_storages,
                "follower-down",
                receipt.commit_ts,
                b"value"
            )
            .await,
            "surviving nodes should apply commit"
        );
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn leader_shutdown_during_commit_recovers() {
    let _guard = TEST_GUARD.lock().await;
    for backend in common::all_backends() {
        reset_registry().await;

        let layout = vec![
            (30_u64, "127.0.0.1:6400".to_string()),
            (31_u64, "127.0.0.1:6401".to_string()),
            (32_u64, "127.0.0.1:6402".to_string()),
        ];

        let (nodes, storages, _guards) = spawn_cluster(&layout, backend).await;
        let ids: Vec<u64> = layout.iter().map(|(id, _)| *id).collect();
        let leader = wait_for_unique_leader(&ids, LEADER_TIMEOUT)
            .await
            .expect("initial leader");

        let leader_index = ids
            .iter()
            .position(|id| *id == leader)
            .expect("leader index");
        let leader_node = nodes[leader_index].clone();

        let manager = leader_node.transaction_manager();
        let txn = manager.begin_transaction();
        manager
            .write(&txn, "leader-down".to_string(), b"survivor".to_vec())
            .await
            .expect("stage write");
        let collected = manager.collect(txn).await.expect("collect txn");
        let leader_clone = leader_node.clone();
        let commit = tokio::spawn(async move { leader_clone.replicate_commit(collected).await });

        shutdown_node(leader, &nodes, &layout).await;

        let commit_err = commit
            .await
            .expect("join commit task")
            .expect_err("commit should fail when leader steps down mid-flight");
        drop(commit_err);

        let survivors: Vec<u64> = ids.iter().copied().filter(|id| *id != leader).collect();
        let new_leader = wait_for_unique_leader(&survivors, LEADER_TIMEOUT)
            .await
            .expect("new leader after shutdown");
        assert!(
            wait_for_cluster_convergence(&survivors, new_leader, CONVERGENCE_TIMEOUT).await,
            "remaining nodes should converge on new leader"
        );

        let receipt =
            commit_from_node(&ids, &nodes, new_leader, "leader-failover", b"survivor").await;

        let survivor_storages = storages_for_ids(&survivors, &layout, &storages);
        assert!(
            wait_for_value_on_all(
                &survivor_storages,
                "leader-failover",
                receipt.commit_ts,
                b"survivor"
            )
            .await,
            "surviving nodes should apply commit after leader shutdown"
        );
    }
}

async fn wait_for_unique_leader(node_ids: &[u64], timeout: Duration) -> Option<u64> {
    let deadline = Instant::now() + timeout;
    loop {
        let mut leaders = Vec::new();
        for &node_id in node_ids {
            if let Some(metrics) = collect_metrics(node_id).await
                && metrics.state == ServerState::Leader
            {
                leaders.push(node_id);
            }
        }

        if leaders.len() == 1 {
            return leaders.pop();
        }

        if Instant::now() >= deadline {
            return None;
        }

        sleep(Duration::from_millis(100)).await;
    }
}

async fn spawn_cluster(
    layout: &[(u64, String)],
    backend: common::BackendKind,
) -> (
    Vec<Arc<RivetNode<StorageAdapter>>>,
    Vec<Arc<StorageAdapter>>,
    Vec<common::TestStorage>,
) {
    let mut nodes = Vec::new();
    let mut storages = Vec::new();
    let mut guards = Vec::new();

    for (node_id, addr) in layout {
        let peers = layout
            .iter()
            .filter(|(peer_id, _)| peer_id != node_id)
            .map(|(peer_id, peer_addr)| PeerConfig::new(*peer_id, peer_addr.clone()))
            .collect::<Vec<_>>();

        let storage = common::TestStorage::new(backend);
        let config = RivetConfig::new(*node_id, addr.clone(), peers, storage.data_dir())
            .with_storage(storage.storage_config());
        let storage_arc = storage.storage();
        let node = Arc::new(
            RivetNode::new(config, storage_arc.clone())
                .await
                .expect("node bootstrap should succeed"),
        );

        nodes.push(node);
        storages.push(storage_arc);
        guards.push(storage);
    }

    (nodes, storages, guards)
}

async fn wait_for_cluster_convergence(node_ids: &[u64], leader: u64, timeout: Duration) -> bool {
    let deadline = Instant::now() + timeout;
    loop {
        let mut consistent = true;
        for &node_id in node_ids {
            let metrics = match collect_metrics(node_id).await {
                Some(m) => m,
                None => {
                    consistent = false;
                    break;
                }
            };

            if metrics.current_leader != Some(leader) {
                consistent = false;
                break;
            }

            let expected_state = if node_id == leader {
                ServerState::Leader
            } else {
                ServerState::Follower
            };

            if metrics.state != expected_state {
                consistent = false;
                break;
            }
        }

        if consistent {
            return true;
        }

        if Instant::now() >= deadline {
            return false;
        }

        sleep(Duration::from_millis(100)).await;
    }
}

async fn wait_for_value_on_all<S>(
    storages: &[Arc<S>],
    key: &str,
    commit_ts: Timestamp,
    expected: &[u8],
) -> bool
where
    S: StorageEngine + 'static,
{
    let deadline = Instant::now() + Duration::from_secs(5);
    let key = key.to_string();

    loop {
        let mut satisfied = true;
        for storage in storages {
            match storage.read(&key, commit_ts).await {
                Ok(Some(version)) if version.value == expected => {}
                _ => {
                    satisfied = false;
                    break;
                }
            }
        }

        if satisfied {
            return true;
        }

        if Instant::now() >= deadline {
            return false;
        }

        sleep(Duration::from_millis(50)).await;
    }
}

fn storages_for_ids(
    ids: &[u64],
    layout: &[(u64, String)],
    storages: &[Arc<StorageAdapter>],
) -> Vec<Arc<StorageAdapter>> {
    ids.iter()
        .filter_map(|target| {
            layout
                .iter()
                .position(|(id, _)| id == target)
                .map(|idx| storages[idx].clone())
        })
        .collect()
}

async fn handle_commit_error(
    err: CommitError,
    ids: &[u64],
    nodes: &[Arc<RivetNode<StorageAdapter>>],
    survivors: &[u64],
    key: &str,
    value: &[u8],
) -> CommitReceipt {
    match err {
        CommitError::Raft(_) => {
            let new_leader = wait_for_unique_leader(survivors, LEADER_TIMEOUT)
                .await
                .expect("new leader after disruption");
            commit_from_node(ids, nodes, new_leader, key, value).await
        }
        other => panic!("unexpected commit error: {other:?}"),
    }
}

async fn commit_from_node(
    ids: &[u64],
    nodes: &[Arc<RivetNode<StorageAdapter>>],
    node_id: u64,
    key: &str,
    value: &[u8],
) -> CommitReceipt {
    let index = ids
        .iter()
        .position(|id| *id == node_id)
        .expect("node should exist");
    let node = nodes[index].clone();
    let manager = node.transaction_manager();
    let txn = manager.begin_transaction();
    manager
        .write(&txn, key.to_string(), value.to_vec())
        .await
        .expect("stage write");
    let collected = manager.collect(txn).await.expect("collect txn");
    node.replicate_commit(collected)
        .await
        .expect("replicate commit")
}

async fn shutdown_node(
    node_id: u64,
    nodes: &[Arc<RivetNode<StorageAdapter>>],
    layout: &[(u64, String)],
) {
    if let Some(index) = layout.iter().position(|(id, _)| *id == node_id) {
        registry().unregister(node_id).await;
        nodes[index]
            .raft()
            .clone()
            .shutdown()
            .await
            .expect("shutdown node");
    }
}
