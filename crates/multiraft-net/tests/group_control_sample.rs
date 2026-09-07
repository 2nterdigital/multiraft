//! Best-effort group-control sampling over existing public Raft surfaces.

use std::collections::BTreeSet;
use std::time::Duration;

use multiraft_core::ClusterConfig;
use multiraft_fsm::CounterFsm;
use multiraft_net::wait_for_leader;
use multiraft_net::GroupControlLayoutObservation;
use multiraft_net::GroupControlPrecheckError;
use multiraft_net::GroupControlPrecheckRejection;
use multiraft_net::GroupControlPreconditions;
use multiraft_net::GroupControlRequestEcho;
use multiraft_net::GroupControlRequestResult;
use multiraft_net::GroupControlSample;
use multiraft_net::GroupControlSampleError;
use multiraft_net::GroupServerState;
use multiraft_net::MultiRaft;
use multiraft_net::SharedFabric;
use multiraft_net::TargetQualification;

fn set(ids: &[u64]) -> BTreeSet<u64> {
    ids.iter().copied().collect()
}

fn configs_with_temp_dirs(peer_ids: &[u64], root: &std::path::Path) -> Vec<ClusterConfig> {
    peer_ids
        .iter()
        .map(|&id| {
            let mut cfg = ClusterConfig::for_test(id, peer_ids);
            cfg.data_dir = root.join(format!("node-{id}"));
            std::fs::create_dir_all(&cfg.data_dir).expect("mkdir data_dir");
            cfg
        })
        .collect()
}

fn assert_debug_clone_eq<T: std::fmt::Debug + Clone + PartialEq + Eq>() {}

fn assert_error<E: std::error::Error>() {}

async fn start_on_fabric(fabric: &SharedFabric, configs: &[ClusterConfig]) -> Vec<MultiRaft> {
    let mut nodes = Vec::with_capacity(configs.len());
    for config in configs {
        nodes.push(
            fabric
                .start_node(config.clone())
                .await
                .unwrap_or_else(|error| panic!("start_node {}: {error:#}", config.node_id)),
        );
    }
    nodes
}

async fn wait_for_leader_among(
    nodes: &[MultiRaft],
    group: u64,
    dead: &[u64],
    timeout: Duration,
) -> u64 {
    let deadline = std::time::Instant::now() + timeout;
    while std::time::Instant::now() < deadline {
        for node in nodes {
            if dead.contains(&node.node_id()) || !node.is_leader(group) {
                continue;
            }
            if let Some(leader) = node.leader(group) {
                if !dead.contains(&leader) {
                    return leader;
                }
            }
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("no leader for group {group} among survivors (dead={dead:?})");
}

async fn wait_for_specific_leader(
    nodes: &[MultiRaft],
    group: u64,
    expected: u64,
    timeout: Duration,
) {
    let deadline = std::time::Instant::now() + timeout;
    while std::time::Instant::now() < deadline {
        for node in nodes {
            if node.node_id() == expected && node.is_leader(group) {
                if node.leader(group) == Some(expected) {
                    return;
                }
            }
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    panic!("node {expected} did not become leader for group {group}");
}

async fn propose_on_available_leader(nodes: &[MultiRaft], group: u64, dead: &[u64], data: Vec<u8>) {
    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    loop {
        for node in nodes {
            if dead.contains(&node.node_id()) || !node.is_leader(group) {
                continue;
            }
            match node.propose(group, data.clone()).await {
                Ok(_) => return,
                Err(multiraft_core::MultiRaftError::NotLeader { .. }) => {}
                Err(multiraft_core::MultiRaftError::UnknownGroup(_)) => {}
                Err(error) => panic!("propose failed: {error:?}"),
            }
        }
        if std::time::Instant::now() >= deadline {
            panic!("timed out proposing on group {group}");
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

async fn read_counter_from_available_leader(nodes: &[MultiRaft], group: u64, dead: &[u64]) -> i64 {
    let leader_id = wait_for_leader_among(nodes, group, dead, Duration::from_secs(20)).await;
    let leader = nodes
        .iter()
        .find(|node| node.node_id() == leader_id)
        .expect("leader handle");
    leader
        .read_linearizable(group, |fsm| fsm.value(group))
        .await
        .expect("linearizable counter read")
}

async fn restart_node_on_fabric(
    fabric: &SharedFabric,
    nodes: &mut [MultiRaft],
    configs: &[ClusterConfig],
    node_id: u64,
    groups: &[u64],
    members: &[u64],
) {
    let idx = nodes
        .iter()
        .position(|node| node.node_id() == node_id)
        .unwrap_or_else(|| panic!("node {node_id} missing"));
    nodes[idx] = fabric
        .start_node(configs[idx].clone())
        .await
        .expect("start_node restart");
    for &group in groups {
        nodes[idx]
            .create_group(group, members)
            .await
            .unwrap_or_else(|error| panic!("create_group {group} after restart: {error:?}"));
        let _ = nodes[idx]
            .wait_for_recovery(group, Duration::from_secs(20))
            .await;
    }
}

async fn wait_for_qualified_target(
    leader: &MultiRaft,
    group: u64,
    peer_ids: &[u64],
    target: u64,
) -> GroupControlSample {
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    loop {
        let sample = leader
            .read_group_control_sample(
                group,
                peer_ids,
                Duration::from_secs(10),
                Duration::from_secs(10),
            )
            .await
            .expect("group control sample");
        if matches!(
            sample.target_qualifications.get(&target),
            Some(TargetQualification::Qualified { .. })
        ) {
            return sample;
        }
        if std::time::Instant::now() >= deadline {
            panic!(
                "timed out waiting for target {target} to become qualified: {:?}",
                sample.target_qualifications
            );
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

async fn wait_for_target_observed_layout(
    nodes: &[MultiRaft],
    echo: GroupControlRequestEcho,
    peer_ids: &[u64],
) -> GroupControlLayoutObservation {
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    loop {
        for node in nodes {
            let observation = node
                .observe_group_control_layout(
                    echo,
                    peer_ids,
                    Duration::from_secs(10),
                    Duration::from_secs(10),
                )
                .await;
            if matches!(
                observation,
                GroupControlLayoutObservation::TargetObserved { .. }
            ) {
                return observation;
            }
        }
        if std::time::Instant::now() >= deadline {
            panic!("timed out waiting for target layout observation");
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

#[test]
fn public_group_control_sample_contract_is_typed() {
    assert_debug_clone_eq::<GroupControlSample>();
    assert_debug_clone_eq::<GroupControlSampleError>();
    assert_debug_clone_eq::<GroupControlPreconditions>();
    assert_debug_clone_eq::<GroupControlPrecheckError>();
    assert_debug_clone_eq::<GroupControlPrecheckRejection>();
    assert_debug_clone_eq::<GroupControlRequestEcho>();
    assert_debug_clone_eq::<GroupControlRequestResult>();
    assert_debug_clone_eq::<GroupControlLayoutObservation>();
    assert_debug_clone_eq::<TargetQualification>();
    assert_error::<GroupControlSampleError>();
    assert_error::<GroupControlPrecheckError>();
}

#[tokio::test]
async fn read_group_control_sample_unknown_group_does_not_create_state() {
    let nodes = MultiRaft::start_cluster(vec![ClusterConfig::for_test(1, &[1])])
        .await
        .expect("start_cluster");

    let err = nodes[0]
        .read_group_control_sample(99, &[1], Duration::from_secs(1), Duration::from_secs(1))
        .await
        .expect_err("unknown group must not create state");

    assert!(matches!(
        err,
        GroupControlSampleError::UnknownGroup { group_id: 99 }
    ));
    assert!(!nodes[0].is_leader(99));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn read_group_control_sample_returns_confirmed_leader_sample() {
    let group = 9;
    let nodes = MultiRaft::start_cluster(vec![ClusterConfig::for_test(1, &[1])])
        .await
        .expect("start_cluster");
    nodes[0]
        .create_group(group, &[1])
        .await
        .expect("create_group");
    assert_eq!(
        wait_for_leader(&nodes, group, Duration::from_secs(10)).await,
        Some(1)
    );

    let sample = nodes[0]
        .read_group_control_sample(
            group,
            &[1],
            Duration::from_secs(10),
            Duration::from_secs(10),
        )
        .await
        .expect("group control sample");

    assert_eq!(sample.group_id, group);
    assert_eq!(sample.local_node_id, 1);
    assert_eq!(sample.leader_id, 1);
    assert_eq!(sample.server_state, GroupServerState::Leader);
    assert_eq!(sample.flushed_vote.node_id, 1);
    assert!(sample.flushed_vote.committed);
    assert_eq!(sample.effective_membership.voter_configs, vec![set(&[1])]);
    assert_eq!(sample.committed_membership.voter_configs, vec![set(&[1])]);
    assert!(sample.effective_membership.learner_ids.is_empty());
    assert!(sample.committed_membership.learner_ids.is_empty());
    assert!(sample.read_log_id.is_some());
    assert!(sample.local_committed.is_some());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn read_group_control_sample_on_follower_returns_typed_not_leader() {
    let peer_ids = [1u64, 2, 3];
    let configs: Vec<_> = peer_ids
        .iter()
        .map(|&id| ClusterConfig::for_test(id, &peer_ids))
        .collect();
    let nodes = MultiRaft::start_cluster(configs)
        .await
        .expect("start_cluster");
    let group = 10;
    for node in &nodes {
        node.create_group(group, &peer_ids)
            .await
            .expect("create_group");
    }
    let leader_id = wait_for_leader(&nodes, group, Duration::from_secs(10))
        .await
        .expect("leader");
    let follower = nodes
        .iter()
        .find(|node| node.node_id() != leader_id)
        .expect("follower");

    let err = follower
        .read_group_control_sample(
            group,
            &peer_ids,
            Duration::from_secs(10),
            Duration::from_secs(10),
        )
        .await
        .expect_err("follower must not produce a control sample");

    assert!(matches!(
        err,
        GroupControlSampleError::NotLeader {
            group_id: 10,
            leader_hint: Some(_),
            ..
        }
    ));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn read_group_control_sample_rejects_unexpected_voter_identity() {
    let group = 11;
    let nodes = MultiRaft::start_cluster(vec![ClusterConfig::for_test(1, &[1])])
        .await
        .expect("start_cluster");
    nodes[0]
        .create_group(group, &[1])
        .await
        .expect("create_group");
    assert_eq!(
        wait_for_leader(&nodes, group, Duration::from_secs(10)).await,
        Some(1)
    );

    let err = nodes[0]
        .read_group_control_sample(
            group,
            &[1, 2],
            Duration::from_secs(10),
            Duration::from_secs(10),
        )
        .await
        .expect_err("wrong fixed voter set must not produce a control sample");

    assert!(matches!(
        err,
        GroupControlSampleError::UnexpectedMembership { group_id: 11, .. }
    ));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn read_group_control_sample_rejects_expired_age_budget() {
    let group = 12;
    let nodes = MultiRaft::start_cluster(vec![ClusterConfig::for_test(1, &[1])])
        .await
        .expect("start_cluster");
    nodes[0]
        .create_group(group, &[1])
        .await
        .expect("create_group");
    assert_eq!(
        wait_for_leader(&nodes, group, Duration::from_secs(10)).await,
        Some(1)
    );

    let err = nodes[0]
        .read_group_control_sample(group, &[1], Duration::ZERO, Duration::from_secs(10))
        .await
        .expect_err("expired age budget must reject the sample");

    assert!(matches!(
        err,
        GroupControlSampleError::SampleTooOld { group_id: 12, .. }
    ));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn read_group_control_sample_classifies_live_target_progress() {
    let peer_ids = [1u64, 2, 3];
    let configs: Vec<_> = peer_ids
        .iter()
        .map(|&id| ClusterConfig::for_test(id, &peer_ids))
        .collect();
    let nodes = MultiRaft::start_cluster(configs)
        .await
        .expect("start_cluster");
    let group = 13;
    for node in &nodes {
        node.create_group(group, &peer_ids)
            .await
            .expect("create_group");
    }
    let leader_id = wait_for_leader(&nodes, group, Duration::from_secs(10))
        .await
        .expect("leader");
    let leader = nodes
        .iter()
        .find(|node| node.node_id() == leader_id)
        .expect("leader handle");
    let target = *peer_ids
        .iter()
        .find(|&&node_id| node_id != leader_id)
        .expect("target");

    leader
        .propose(group, CounterFsm::encode_add(1, 1))
        .await
        .expect("drive replication");

    let sample = wait_for_qualified_target(leader, group, &peer_ids, target).await;

    match sample.target_qualifications.get(&target) {
        Some(TargetQualification::Qualified { ack_age, matched }) => {
            assert!(*ack_age < Duration::from_secs(10));
            assert_eq!(
                matched.term,
                sample.local_committed.expect("committed").term
            );
            assert!(matched.index >= sample.local_committed.expect("committed").index);
        }
        other => panic!("expected qualified target, got {other:?}"),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn read_group_control_sample_marks_stale_ack_with_local_instant() {
    let peer_ids = [1u64, 2, 3];
    let configs: Vec<_> = peer_ids
        .iter()
        .map(|&id| ClusterConfig::for_test(id, &peer_ids))
        .collect();
    let nodes = MultiRaft::start_cluster(configs)
        .await
        .expect("start_cluster");
    let group = 14;
    for node in &nodes {
        node.create_group(group, &peer_ids)
            .await
            .expect("create_group");
    }
    let leader_id = wait_for_leader(&nodes, group, Duration::from_secs(10))
        .await
        .expect("leader");
    let leader = nodes
        .iter()
        .find(|node| node.node_id() == leader_id)
        .expect("leader handle");
    let target = *peer_ids
        .iter()
        .find(|&&node_id| node_id != leader_id)
        .expect("target");
    leader
        .propose(group, CounterFsm::encode_add(1, 1))
        .await
        .expect("drive replication");
    let _qualified = wait_for_qualified_target(leader, group, &peer_ids, target).await;

    let stale = leader
        .read_group_control_sample(group, &peer_ids, Duration::from_secs(10), Duration::ZERO)
        .await
        .expect("stale target sample still returns current layout");

    assert!(matches!(
        stale.target_qualifications.get(&target),
        Some(TargetQualification::AckTooOld { max, .. }) if max.is_zero()
    ));
    let preconditions = stale.observed_preconditions_for(target);
    let err = stale
        .check_transfer_preconditions(&preconditions)
        .expect_err("stale ack must reject before trigger");
    assert!(matches!(
        err,
        GroupControlPrecheckError::TargetNotRecentlyAcked {
            target: actual,
            ..
        } if actual == target
    ));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn try_transfer_group_leader_reports_queued_and_layout_separately() {
    let peer_ids = [1u64, 2, 3];
    let configs: Vec<_> = peer_ids
        .iter()
        .map(|&id| ClusterConfig::for_test(id, &peer_ids))
        .collect();
    let nodes = MultiRaft::start_cluster(configs)
        .await
        .expect("start_cluster");
    let group = 15;
    for node in &nodes {
        node.create_group(group, &peer_ids)
            .await
            .expect("create_group");
    }
    let leader_id = wait_for_leader(&nodes, group, Duration::from_secs(10))
        .await
        .expect("leader");
    let leader = nodes
        .iter()
        .find(|node| node.node_id() == leader_id)
        .expect("leader handle");
    let target = *peer_ids
        .iter()
        .find(|&&node_id| node_id != leader_id)
        .expect("target");
    leader
        .propose(group, CounterFsm::encode_add(1, 1))
        .await
        .expect("drive replication");
    let sample = wait_for_qualified_target(leader, group, &peer_ids, target).await;
    let preconditions = sample.observed_preconditions_for(target);

    let result = leader
        .try_transfer_group_leader(
            &preconditions,
            &peer_ids,
            Duration::from_secs(10),
            Duration::from_secs(10),
        )
        .await;

    let echo = GroupControlRequestEcho {
        group_id: group,
        source: leader_id,
        target,
    };
    assert_eq!(result, GroupControlRequestResult::TriggerQueued { echo });

    let observation = wait_for_target_observed_layout(&nodes, echo, &peer_ids).await;
    assert!(matches!(
        observation,
        GroupControlLayoutObservation::TargetObserved {
            echo: observed_echo,
            observed_leader,
            ..
        } if observed_echo == echo && observed_leader == target
    ));
    assert_eq!(result, GroupControlRequestResult::TriggerQueued { echo });
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn try_transfer_group_leader_rejects_changed_preconditions_before_trigger() {
    let peer_ids = [1u64, 2, 3];
    let configs: Vec<_> = peer_ids
        .iter()
        .map(|&id| ClusterConfig::for_test(id, &peer_ids))
        .collect();
    let nodes = MultiRaft::start_cluster(configs)
        .await
        .expect("start_cluster");
    let group = 16;
    for node in &nodes {
        node.create_group(group, &peer_ids)
            .await
            .expect("create_group");
    }
    let leader_id = wait_for_leader(&nodes, group, Duration::from_secs(10))
        .await
        .expect("leader");
    let leader = nodes
        .iter()
        .find(|node| node.node_id() == leader_id)
        .expect("leader handle");
    let target = *peer_ids
        .iter()
        .find(|&&node_id| node_id != leader_id)
        .expect("target");
    leader
        .propose(group, CounterFsm::encode_add(1, 1))
        .await
        .expect("drive replication");
    let sample = wait_for_qualified_target(leader, group, &peer_ids, target).await;
    let mut preconditions = sample.observed_preconditions_for(target);
    preconditions.expected_source = target;

    let result = leader
        .try_transfer_group_leader(
            &preconditions,
            &peer_ids,
            Duration::from_secs(10),
            Duration::from_secs(10),
        )
        .await;

    assert!(matches!(
        result,
        GroupControlRequestResult::PrecheckRejected { .. }
    ));
    assert_eq!(leader.leader(group), Some(leader_id));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rf3_transfer_target_failure_and_same_dir_recovery_stays_writable() {
    let peer_ids = [1u64, 2, 3];
    let tmp = tempfile::tempdir().expect("tempdir");
    let configs = configs_with_temp_dirs(&peer_ids, tmp.path());
    let fabric = SharedFabric::new();
    let mut nodes = start_on_fabric(&fabric, &configs).await;
    let group = 17;
    for node in &nodes {
        node.create_group(group, &peer_ids)
            .await
            .expect("create_group");
    }
    let leader_id = wait_for_leader(&nodes, group, Duration::from_secs(10))
        .await
        .expect("leader");
    let leader = nodes
        .iter()
        .find(|node| node.node_id() == leader_id)
        .expect("leader handle");
    let target = *peer_ids
        .iter()
        .find(|&&node_id| node_id != leader_id)
        .expect("target");

    propose_on_available_leader(&nodes, group, &[], CounterFsm::encode_add(2, 17_001)).await;
    let baseline = read_counter_from_available_leader(&nodes, group, &[]).await;
    let sample = wait_for_qualified_target(leader, group, &peer_ids, target).await;
    let preconditions = sample.observed_preconditions_for(target);

    let result = leader
        .try_transfer_group_leader(
            &preconditions,
            &peer_ids,
            Duration::from_secs(10),
            Duration::from_secs(10),
        )
        .await;
    assert!(matches!(
        result,
        GroupControlRequestResult::TriggerQueued { .. }
    ));
    wait_for_specific_leader(&nodes, group, target, Duration::from_secs(10)).await;
    assert!(read_counter_from_available_leader(&nodes, group, &[]).await >= baseline);

    let target_idx = nodes
        .iter()
        .position(|node| node.node_id() == target)
        .expect("target index");
    nodes[target_idx]
        .shutdown()
        .await
        .expect("shutdown transferred target");
    let dead = [target];
    let _ = wait_for_leader_among(&nodes, group, &dead, Duration::from_secs(20)).await;
    propose_on_available_leader(&nodes, group, &dead, CounterFsm::encode_add(3, 17_002)).await;
    let after_target_failure = read_counter_from_available_leader(&nodes, group, &dead).await;
    assert!(after_target_failure >= baseline + 3);

    restart_node_on_fabric(&fabric, &mut nodes, &configs, target, &[group], &peer_ids).await;
    let _ = wait_for_leader_among(&nodes, group, &[], Duration::from_secs(30)).await;
    propose_on_available_leader(&nodes, group, &[], CounterFsm::encode_add(4, 17_003)).await;
    let after_restart = read_counter_from_available_leader(&nodes, group, &[]).await;
    assert!(after_restart >= after_target_failure + 4);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn four_node_control_rejects_non_fixed_voter_target() {
    let all_node_ids = [1u64, 2, 3, 4];
    let fixed_voters = [1u64, 2, 3];
    let configs: Vec<_> = all_node_ids
        .iter()
        .map(|&id| ClusterConfig::for_test(id, &all_node_ids))
        .collect();
    let nodes = MultiRaft::start_cluster(configs)
        .await
        .expect("start_cluster");
    let group = 18;
    for node in nodes
        .iter()
        .filter(|node| fixed_voters.contains(&node.node_id()))
    {
        node.create_group(group, &fixed_voters)
            .await
            .expect("create_group");
    }
    let leader_id = wait_for_leader(&nodes, group, Duration::from_secs(10))
        .await
        .expect("leader");
    let leader = nodes
        .iter()
        .find(|node| node.node_id() == leader_id)
        .expect("leader handle");
    leader
        .propose(group, CounterFsm::encode_add(1, 18_001))
        .await
        .expect("drive replication");
    let sample = leader
        .read_group_control_sample(
            group,
            &fixed_voters,
            Duration::from_secs(10),
            Duration::from_secs(10),
        )
        .await
        .expect("sample");
    let preconditions = sample.observed_preconditions_for(4);

    let result = leader
        .try_transfer_group_leader(
            &preconditions,
            &fixed_voters,
            Duration::from_secs(10),
            Duration::from_secs(10),
        )
        .await;

    assert!(matches!(
        result,
        GroupControlRequestResult::PrecheckRejected {
            reason: GroupControlPrecheckRejection::Preconditions(
                GroupControlPrecheckError::TargetNotVoter { target: 4 }
            ),
            ..
        }
    ));
    assert_eq!(leader.leader(group), Some(leader_id));
}
