use std::sync::Arc;
use std::time::Duration;

use openraft::ServerState;
use rivetdb::storage::InMemoryStorage;
use rivetdb::{PeerConfig, RivetConfig, RivetNode, collect_metrics, reset_registry};
use tokio::time::{Instant, sleep};

#[tokio::test(flavor = "multi_thread")]
async fn multi_node_cluster_elects_single_leader() {
    reset_registry().await;

    let layout = vec![
        (0_u64, "127.0.0.1:6100".to_string()),
        (1_u64, "127.0.0.1:6101".to_string()),
        (2_u64, "127.0.0.1:6102".to_string()),
    ];

    let mut nodes = Vec::new();
    for (node_id, addr) in &layout {
        let peers = layout
            .iter()
            .filter(|(peer_id, _)| peer_id != node_id)
            .map(|(peer_id, peer_addr)| PeerConfig::new(*peer_id, peer_addr.clone()))
            .collect::<Vec<_>>();

        let config = RivetConfig::new(*node_id, addr.clone(), peers, None);
        let storage = Arc::new(InMemoryStorage::new());
        let node = Arc::new(
            RivetNode::new(config, storage)
                .await
                .expect("node bootstrap should succeed"),
        );
        nodes.push(node);
    }

    let ids: Vec<u64> = layout.iter().map(|(id, _)| *id).collect();
    let leader = wait_for_unique_leader(&ids)
        .await
        .expect("cluster should elect exactly one leader");

    assert!(
        wait_for_cluster_convergence(&ids, leader).await,
        "cluster did not converge on leader {leader}"
    );

    drop(nodes);
}

async fn wait_for_unique_leader(node_ids: &[u64]) -> Option<u64> {
    let deadline = Instant::now() + Duration::from_secs(5);
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

async fn wait_for_cluster_convergence(node_ids: &[u64], leader: u64) -> bool {
    let deadline = Instant::now() + Duration::from_secs(5);
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
