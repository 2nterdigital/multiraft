use std::collections::BTreeSet;
use std::path::Path;
use std::time::Duration;

use futures::FutureExt;
use multiraft_core::ClusterConfig;
use multiraft_core::MultiRaftError;
use multiraft_core::NodeRole;
use multiraft_net::wait_for_leader;
use multiraft_net::GroupControlRequestResult;
use multiraft_net::GroupControlSample;
use multiraft_net::GroupObservation;
use multiraft_net::GroupObservationReceiver;
use multiraft_net::GroupServerState;
use multiraft_net::LocalMembershipRole;
use multiraft_net::MembershipObservation;
use multiraft_net::MultiRaft;
use multiraft_net::ObservationClosed;
use multiraft_net::ObservedLogId;
use multiraft_net::SharedFabric;
use multiraft_net::TargetQualification;
use multiraft_net::VoteObservation;

fn assert_debug_clone_eq<T: std::fmt::Debug + Clone + PartialEq + Eq>() {}

fn assert_send<T: Send>() {}

fn set(ids: &[u64]) -> BTreeSet<u64> {
    ids.iter().copied().collect()
}

fn config_with_dir(node_id: u64, peer_ids: &[u64], root: &Path, role: NodeRole) -> ClusterConfig {
    let mut config = ClusterConfig::for_test(node_id, peer_ids);
    config.data_dir = root.join(format!("node-{node_id}"));
    std::fs::create_dir_all(&config.data_dir).expect("create data dir");
    config.role = role;
    config
}

async fn wait_for_observer_change(
    receiver: &mut GroupObservationReceiver,
    timeout: Duration,
    mut accept: impl FnMut(&GroupObservation) -> bool,
) -> GroupObservation {
    let deadline = std::time::Instant::now() + timeout;
    loop {
        let remaining = deadline.saturating_duration_since(std::time::Instant::now());
        if remaining.is_zero() {
            panic!("timed out waiting for matching observation");
        }
        match tokio::time::timeout(remaining, receiver.changed()).await {
            Ok(Ok(observation)) if accept(&observation) => return observation,
            Ok(Ok(_)) => {}
            Ok(Err(err)) => panic!("observer closed unexpectedly: {err}"),
            Err(_) => panic!("timed out waiting for matching observation"),
        }
    }
}

async fn start_on_fabric(fabric: &SharedFabric, configs: &[ClusterConfig]) -> Vec<MultiRaft> {
    let mut nodes = Vec::with_capacity(configs.len());
    for config in configs {
        nodes.push(
            fabric
                .start_node(config.clone())
                .await
                .unwrap_or_else(|err| panic!("start node {}: {err:#}", config.node_id)),
        );
    }
    nodes
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

#[test]
fn public_observation_contract_is_backend_neutral() {
    fn read_sample_fields(sample: GroupObservation) {
        let _: u64 = sample.group_id;
        let _: u64 = sample.local_node_id;
        let _: LocalMembershipRole = sample.local_membership_role;
        let _: GroupServerState = sample.server_state;
        let _: Option<u64> = sample.leader_hint;
        let _: u64 = sample.flushed_vote.term;
        let _: u64 = sample.flushed_vote.node_id;
        let _: bool = sample.flushed_vote.committed;
        let _: usize = sample.effective_membership.voter_configs.len();
        let _: usize = sample.effective_membership.learner_ids.len();
        if let Some(log_id) = sample.effective_membership.log_id {
            let _: u64 = log_id.term;
            let _: u64 = log_id.node_id;
            let _: u64 = log_id.index;
        }
        let _: usize = sample.committed_membership.voter_configs.len();
        let _: usize = sample.committed_membership.learner_ids.len();
        if let Some(log_id) = sample.committed_membership.log_id {
            let _: u64 = log_id.term;
            let _: u64 = log_id.node_id;
            let _: u64 = log_id.index;
        }
    }

    assert_debug_clone_eq::<GroupObservation>();
    assert_debug_clone_eq::<VoteObservation>();
    assert_debug_clone_eq::<MembershipObservation>();
    assert_debug_clone_eq::<ObservedLogId>();
    assert_debug_clone_eq::<LocalMembershipRole>();
    assert_debug_clone_eq::<GroupServerState>();
    assert_debug_clone_eq::<ObservationClosed>();
    assert_send::<GroupObservationReceiver>();

    let _read_sample_fields: fn(GroupObservation) = read_sample_fields;

    let err: MultiRaftError = ObservationClosed::new(9).into();
    assert!(matches!(err, MultiRaftError::ObservationClosed(_)));
}

#[tokio::test]
async fn observe_group_returns_unknown_group_without_creating_state() {
    let nodes = MultiRaft::start_cluster(vec![ClusterConfig::for_test(1, &[1])])
        .await
        .expect("start_cluster");

    let err = match nodes[0].observe_group(99) {
        Ok(_) => panic!("unknown group must not create state"),
        Err(err) => err,
    };

    match err {
        MultiRaftError::UnknownGroup(group) => assert_eq!(group, 99),
        other => panic!("expected UnknownGroup, got {other:?}"),
    }
    assert!(!nodes[0].is_leader(99));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn observe_group_returns_initial_control_snapshot() {
    let group = 7;
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

    let (initial, _receiver) = nodes[0].observe_group(group).expect("observe group");

    assert_eq!(initial.group_id, group);
    assert_eq!(initial.local_node_id, 1);
    assert_eq!(initial.local_membership_role, LocalMembershipRole::Voter);
    assert_eq!(initial.server_state, GroupServerState::Leader);
    assert_eq!(initial.leader_hint, Some(1));
    assert_eq!(
        initial.effective_membership.voter_configs,
        vec![BTreeSet::from([1])]
    );
    assert_eq!(
        initial.committed_membership.voter_configs,
        vec![BTreeSet::from([1])]
    );
    assert!(initial.effective_membership.learner_ids.is_empty());
    assert!(initial.committed_membership.learner_ids.is_empty());
    assert!(initial.effective_membership.log_id.is_some());
    assert!(initial.committed_membership.log_id.is_some());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn public_observe_group_marks_initial_sample_seen() {
    let group = 8;
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

    let (initial, mut receiver) = nodes[0].observe_group(group).expect("observe group");
    assert_eq!(initial.server_state, GroupServerState::Leader);
    assert_eq!(initial.local_membership_role, LocalMembershipRole::Voter);

    assert!(
        receiver.changed().now_or_never().is_none(),
        "public facade must mark the initial sample as seen"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn observer_reports_effective_and_committed_learner_membership() {
    let temp = tempfile::tempdir().expect("tempdir");
    let peer_ids = [1u64, 2, 3, 4];
    let voter_ids = [1u64, 2, 3];
    let standby_id = 4;
    let group = 41;
    let fabric = SharedFabric::new();
    let configs = [
        config_with_dir(1, &peer_ids, temp.path(), NodeRole::Voter),
        config_with_dir(2, &peer_ids, temp.path(), NodeRole::Voter),
        config_with_dir(3, &peer_ids, temp.path(), NodeRole::Voter),
        config_with_dir(4, &peer_ids, temp.path(), NodeRole::Standby),
    ];
    let nodes = start_on_fabric(&fabric, &configs).await;
    let voters = &nodes[..3];
    let standby = &nodes[3];

    for node in voters {
        node.create_group(group, &voter_ids)
            .await
            .expect("create voter group");
    }
    standby
        .create_group(group, &voter_ids)
        .await
        .expect("create standby group");

    let leader_id = wait_for_leader(voters, group, Duration::from_secs(10))
        .await
        .expect("leader");
    let leader = voters
        .iter()
        .find(|node| node.node_id() == leader_id)
        .expect("leader handle");
    let (_initial, mut receiver) = standby.observe_group(group).expect("observe standby");

    leader
        .add_standby(group, standby_id)
        .await
        .expect("add standby");

    let observed = wait_for_observer_change(&mut receiver, Duration::from_secs(15), |obs| {
        obs.local_membership_role == LocalMembershipRole::Learner
            && obs.effective_membership.learner_ids.contains(&standby_id)
            && obs.committed_membership.learner_ids.contains(&standby_id)
    })
    .await;

    assert_eq!(observed.local_node_id, standby_id);
    assert_eq!(
        observed.effective_membership.voter_configs,
        vec![set(&voter_ids)]
    );
    assert_eq!(
        observed.committed_membership.voter_configs,
        vec![set(&voter_ids)]
    );
    let effective_log_id = observed
        .effective_membership
        .log_id
        .expect("effective membership log id");
    let committed_log_id = observed
        .committed_membership
        .log_id
        .expect("committed membership log id");
    assert!(effective_log_id.term > 0);
    assert!(effective_log_id.node_id > 0);
    assert!(effective_log_id.index > 0);
    assert!(committed_log_id.term > 0);
    assert!(committed_log_id.node_id > 0);
    assert!(committed_log_id.index > 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn survivor_observer_reports_a_new_leader_after_failover() {
    let temp = tempfile::tempdir().expect("tempdir");
    let peer_ids = [1u64, 2, 3];
    let group = 42;
    let fabric = SharedFabric::new();
    let configs: Vec<_> = peer_ids
        .iter()
        .map(|&id| config_with_dir(id, &peer_ids, temp.path(), NodeRole::Voter))
        .collect();
    let nodes = start_on_fabric(&fabric, &configs).await;

    for node in &nodes {
        node.create_group(group, &peer_ids)
            .await
            .expect("create group");
    }

    let old_leader = wait_for_leader(&nodes, group, Duration::from_secs(10))
        .await
        .expect("initial leader");
    let survivor = nodes
        .iter()
        .find(|node| node.node_id() != old_leader)
        .expect("survivor");
    let (_initial, mut receiver) = survivor.observe_group(group).expect("observe survivor");
    let old_leader_node = nodes
        .iter()
        .find(|node| node.node_id() == old_leader)
        .expect("old leader");

    old_leader_node
        .shutdown()
        .await
        .expect("shutdown old leader");

    let survivors: BTreeSet<_> = peer_ids
        .iter()
        .copied()
        .filter(|id| *id != old_leader)
        .collect();
    let observed = wait_for_observer_change(&mut receiver, Duration::from_secs(15), |obs| {
        obs.leader_hint
            .is_some_and(|leader| leader != old_leader && survivors.contains(&leader))
    })
    .await;

    let new_leader = observed.leader_hint.expect("new leader hint");
    assert_ne!(new_leader, old_leader);
    assert!(survivors.contains(&new_leader));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn shutdown_closes_old_observer_and_restart_requires_resubscribe() {
    let temp = tempfile::tempdir().expect("tempdir");
    let peer_ids = [1u64];
    let group = 43;
    let fabric = SharedFabric::new();
    let config = config_with_dir(1, &peer_ids, temp.path(), NodeRole::Voter);
    let node = fabric.start_node(config.clone()).await.expect("start node");

    node.create_group(group, &peer_ids)
        .await
        .expect("create group");
    assert_eq!(
        wait_for_leader(std::slice::from_ref(&node), group, Duration::from_secs(10)).await,
        Some(1)
    );
    let (_initial, mut old_receiver) = node.observe_group(group).expect("old observe");
    node.shutdown().await.expect("shutdown");

    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    let closed = loop {
        let remaining = deadline.saturating_duration_since(std::time::Instant::now());
        if remaining.is_zero() {
            panic!("old observer did not close after shutdown");
        }
        match tokio::time::timeout(remaining, old_receiver.changed()).await {
            Ok(Ok(_stale)) => {}
            Ok(Err(err)) => break err,
            Err(_) => panic!("old observer did not close after shutdown"),
        }
    };
    assert_eq!(closed.group_id(), group);

    let closed_again = tokio::time::timeout(Duration::from_secs(1), old_receiver.changed())
        .await
        .expect("old receiver should be closed")
        .expect_err("old receiver must remain closed");
    assert_eq!(closed_again.group_id(), group);

    let restarted = fabric
        .start_node(config.clone())
        .await
        .expect("restart node");
    restarted
        .create_group(group, &peer_ids)
        .await
        .expect("recreate group");
    assert_eq!(
        wait_for_leader(
            std::slice::from_ref(&restarted),
            group,
            Duration::from_secs(10),
        )
        .await,
        Some(1)
    );

    let still_closed = tokio::time::timeout(Duration::from_secs(1), old_receiver.changed())
        .await
        .expect("old receiver should stay closed")
        .expect_err("old receiver must not rebind");
    assert_eq!(still_closed.group_id(), group);

    let (new_initial, mut new_receiver) = restarted.observe_group(group).expect("new observe");
    assert_eq!(new_initial.group_id, group);
    assert_eq!(new_initial.local_node_id, 1);
    assert!(
        new_receiver.changed().now_or_never().is_none(),
        "new receiver should start from a marked-seen initial sample"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn control_transfer_reuses_existing_observation_stream() {
    let peer_ids = [1u64, 2, 3];
    let group = 44;
    let configs: Vec<_> = peer_ids
        .iter()
        .map(|&id| ClusterConfig::for_test(id, &peer_ids))
        .collect();
    let nodes = MultiRaft::start_cluster(configs)
        .await
        .expect("start_cluster");
    for node in &nodes {
        node.create_group(group, &peer_ids)
            .await
            .expect("create_group");
    }
    let leader_id = wait_for_leader(&nodes, group, Duration::from_secs(10))
        .await
        .expect("leader");
    let target = *peer_ids
        .iter()
        .find(|&&node_id| node_id != leader_id)
        .expect("target");
    let leader = nodes
        .iter()
        .find(|node| node.node_id() == leader_id)
        .expect("leader handle");
    let target_node = nodes
        .iter()
        .find(|node| node.node_id() == target)
        .expect("target handle");
    let (_initial, mut receiver) = target_node.observe_group(group).expect("observe target");

    leader
        .propose(group, multiraft_fsm::CounterFsm::encode_add(1, 44_001))
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
    assert!(matches!(
        result,
        GroupControlRequestResult::TriggerQueued { .. }
    ));

    let observed = wait_for_observer_change(&mut receiver, Duration::from_secs(10), |obs| {
        obs.local_node_id == target
            && obs.server_state == GroupServerState::Leader
            && obs.leader_hint == Some(target)
    })
    .await;

    assert_eq!(observed.local_node_id, target);
    assert_eq!(observed.leader_hint, Some(target));
}
