//! Non-leader propose must return `MultiRaftError::NotLeader`.

use std::time::Duration;

use multiraft_core::ClusterConfig;
use multiraft_core::MultiRaftError;
use multiraft_fsm::CounterFsm;
use multiraft_net::wait_for_leader;
use multiraft_net::GroupServerState;
use multiraft_net::MultiRaft;

#[tokio::test]
async fn propose_on_follower_returns_not_leader() {
    let peer_ids = [1u64, 2, 3];
    let configs: Vec<_> = peer_ids
        .iter()
        .map(|&id| ClusterConfig::for_test(id, &peer_ids))
        .collect();

    let nodes = MultiRaft::start_cluster(configs)
        .await
        .expect("start_cluster");
    let members = peer_ids.to_vec();
    let group = 1u64;

    for n in &nodes {
        n.create_group(group, &members).await.expect("create_group");
    }

    let leader_id = wait_for_leader(&nodes, group, Duration::from_secs(5))
        .await
        .expect("leader elected");

    let follower = nodes
        .iter()
        .find(|n| n.node_id() != leader_id && !n.is_leader(group))
        .expect("follower handle");

    let data = CounterFsm::encode_add(1, /*idem=*/ 1);
    let err = follower
        .propose(group, data)
        .await
        .expect_err("follower propose must fail");

    match err {
        MultiRaftError::NotLeader { hint } => {
            // hint may be Some(leader) or None depending on metrics freshness
            if let Some(h) = hint {
                assert_eq!(h, leader_id);
            }
        }
        other => panic!("expected NotLeader, got {other:?}"),
    }
}

/// OpenRaft 0.10.1 refuses a fresh leader's write until a quorum acks its first
/// replication round; `propose` waits for that lease instead of returning
/// `NotLeader`. Each Group is a fresh election, written to on its first Leader
/// sample, before the own-term blank log can be known to commit.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn propose_on_first_leader_sample_waits_for_write_lease() {
    let peer_ids = [1u64, 2, 3];
    let configs: Vec<_> = peer_ids
        .iter()
        .map(|&id| ClusterConfig::for_test(id, &peer_ids))
        .collect();
    let nodes = MultiRaft::start_cluster(configs)
        .await
        .expect("start_cluster");
    for group in 1..=30u64 {
        let (tx, mut leaders) = tokio::sync::mpsc::unbounded_channel();
        for n in &nodes {
            n.create_group(group, &peer_ids)
                .await
                .expect("create_group");
            let (first, mut rx) = n.observe_group(group).expect("observe_group");
            let tx = tx.clone();
            let id = n.node_id();
            tokio::spawn(async move {
                let mut state = first.server_state;
                while state != GroupServerState::Leader {
                    match rx.changed().await {
                        Ok(o) => state = o.server_state,
                        Err(_) => return,
                    }
                }
                let _ = tx.send(id);
            });
        }
        let leader = tokio::time::timeout(Duration::from_secs(5), leaders.recv())
            .await
            .expect("leader elected")
            .expect("observer alive");
        let node = nodes.iter().find(|n| n.node_id() == leader).unwrap();
        if let Err(error) = node.propose(group, CounterFsm::encode_add(1, group)).await {
            panic!("group {group}: first write on new leader {leader}: {error:?}");
        }
    }
}
