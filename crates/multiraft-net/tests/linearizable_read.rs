//! Linearizable read API vs local FSM observation.

use std::time::Duration;

use multiraft_core::ClusterConfig;
use multiraft_core::MultiRaftError;
use multiraft_fsm::CounterFsm;
use multiraft_net::wait_for_leader;
use multiraft_net::MultiRaft;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn read_linearizable_sees_committed_write() {
    let peer_ids = [1u64, 2, 3];
    let configs: Vec<_> = peer_ids
        .iter()
        .map(|&id| ClusterConfig::for_test(id, &peer_ids))
        .collect();
    let nodes = MultiRaft::start_cluster(configs)
        .await
        .expect("start_cluster");
    let group = 1u64;
    for n in &nodes {
        n.create_group(group, &peer_ids)
            .await
            .expect("create_group");
    }
    let leader_id = wait_for_leader(&nodes, group, Duration::from_secs(10))
        .await
        .expect("leader");
    let leader = nodes
        .iter()
        .find(|n| n.node_id() == leader_id)
        .expect("leader handle");

    leader
        .propose(group, CounterFsm::encode_add(9, 1))
        .await
        .expect("propose");

    let v = leader
        .read_linearizable(group, |fsm| fsm.value(group))
        .await
        .expect("read_linearizable");
    assert_eq!(v, 9);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn read_linearizable_on_follower_returns_not_leader() {
    let peer_ids = [1u64, 2, 3];
    let configs: Vec<_> = peer_ids
        .iter()
        .map(|&id| ClusterConfig::for_test(id, &peer_ids))
        .collect();
    let nodes = MultiRaft::start_cluster(configs)
        .await
        .expect("start_cluster");
    let group = 1u64;
    for n in &nodes {
        n.create_group(group, &peer_ids)
            .await
            .expect("create_group");
    }
    let leader_id = wait_for_leader(&nodes, group, Duration::from_secs(10))
        .await
        .expect("leader");
    let follower = nodes
        .iter()
        .find(|n| n.node_id() != leader_id)
        .expect("follower");

    let err = follower
        .read_linearizable(group, |fsm| fsm.value(group))
        .await
        .expect_err("follower must not linearizable-read");
    match err {
        MultiRaftError::NotLeader { .. } => {}
        other => panic!("expected NotLeader, got {other:?}"),
    }
}

async fn three_node_group() -> (Vec<MultiRaft<CounterFsm>>, u64, u64) {
    let peer_ids = [1u64, 2, 3];
    let configs: Vec<_> = peer_ids
        .iter()
        .map(|&id| ClusterConfig::for_test(id, &peer_ids))
        .collect();
    let nodes = MultiRaft::start_cluster(configs)
        .await
        .expect("start_cluster");
    let group = 1u64;
    for n in &nodes {
        n.create_group(group, &peer_ids)
            .await
            .expect("create_group");
    }
    let leader_id = wait_for_leader(&nodes, group, Duration::from_secs(10))
        .await
        .expect("leader");
    (nodes, group, leader_id)
}

fn lease_deadline() -> tokio::time::Instant {
    tokio::time::Instant::now() + Duration::from_secs(10)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn read_leased_sees_committed_write() {
    let (nodes, group, leader_id) = three_node_group().await;
    let leader = nodes
        .iter()
        .find(|n| n.node_id() == leader_id)
        .expect("leader handle");
    let ok = leader
        .propose(group, CounterFsm::encode_add(9, 1))
        .await
        .expect("propose");
    let view = leader
        .read_leased_with_applied_at(group, lease_deadline(), |fsm| fsm.value(group))
        .await
        .expect("read_leased");
    assert_eq!(view.value, 9);
    assert!(
        view.applied.index >= ok.index,
        "{:?} vs {ok:?}",
        view.applied
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn read_leased_on_follower_returns_not_leader() {
    let (nodes, group, leader_id) = three_node_group().await;
    let follower = nodes
        .iter()
        .find(|n| n.node_id() != leader_id)
        .expect("follower");
    let err = follower
        .read_leased_with_applied_at(group, lease_deadline(), |fsm| fsm.value(group))
        .await
        .expect_err("follower must not lease-read");
    match err {
        MultiRaftError::NotLeader { .. } => {}
        other => panic!("expected NotLeader, got {other:?}"),
    }
}
