//! Public read-only group-control sampling over owned gRPC Raft handles.

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use multiraft_core::GroupId;
use multiraft_fsm::CounterFsm;
use multiraft_net::{
    classify_group_control_layout, read_group_control_sample, GroupApp,
    GroupControlLayoutObservation, GroupControlPrecheckError, GroupControlSampleError,
    GrpcNetworkFactory, GrpcRouter, GrpcServer, TargetQualification,
};
use multiraft_store::{FileLogStoreOf, Raft, StateMachineStore};
use openraft::async_runtime::WatchReceiver as _;
use openraft::{BasicNode, Config, SnapshotPolicy};
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

type GroupMap = Arc<Mutex<BTreeMap<GroupId, GroupApp<CounterFsm>>>>;

struct RawNode {
    node_id: u64,
    raft: Raft<CounterFsm>,
    stop: Option<oneshot::Sender<()>>,
    server: Option<JoinHandle<anyhow::Result<()>>>,
}

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn public_sampler_observes_real_rf3_and_rejects_stale_or_wrong_identity() {
    tokio::time::timeout(Duration::from_secs(50), scenario())
        .await
        .expect("public consumer deadline");
}

async fn scenario() {
    let scratch = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .join(".tmp/issue38");
    std::fs::create_dir_all(&scratch).expect("create test scratch");
    let root = tempfile::Builder::new()
        .prefix("consumer-")
        .tempdir_in(scratch)
        .expect("test root");
    let node_ids = [1_u64, 2, 3];
    let addresses = reserve_addresses();
    let group = 38;
    let peers = node_ids.iter().copied().zip(addresses).collect::<Vec<_>>();
    let mut nodes = Vec::new();
    for &node_id in &node_ids {
        nodes.push(
            start_node(
                node_id,
                group,
                &peers,
                root.path().join(format!("node-{node_id}")),
            )
            .await,
        );
    }

    let members: BTreeMap<u64, BasicNode> = node_ids
        .iter()
        .copied()
        .zip(addresses)
        .map(|(node_id, address)| {
            (
                node_id,
                BasicNode {
                    addr: address.to_string(),
                },
            )
        })
        .collect();
    nodes[0]
        .raft
        .initialize(members)
        .await
        .expect("initialize RF3 group");
    for node in &nodes {
        node.raft
            .wait_for_recovery(Some(Duration::from_secs(20)))
            .await
            .expect("recover RF3 group");
    }

    let leader = wait_for_leader(&nodes, Duration::from_secs(20)).await;
    let leader_index = nodes
        .iter()
        .position(|node| node.node_id == leader)
        .expect("leader handle");
    let leader_raft = &nodes[leader_index].raft;
    let sample = wait_for_qualified_target(
        leader_raft,
        group,
        leader,
        &node_ids,
        Duration::from_secs(20),
    )
    .await;

    assert_eq!(sample.group_id, group);
    assert_eq!(sample.local_node_id, leader);
    assert_eq!(sample.leader_id, leader);
    assert!(sample.read_log_id.is_some());
    assert!(sample.local_committed.is_some());
    assert_eq!(sample.effective_membership.voter_configs.len(), 1);
    assert_eq!(sample.committed_membership.voter_configs.len(), 1);
    for &node_id in &node_ids {
        assert!(sample.effective_membership.voter_configs[0].contains(&node_id));
        assert!(sample.committed_membership.voter_configs[0].contains(&node_id));
    }

    let target = node_ids
        .iter()
        .copied()
        .find(|&node_id| node_id != leader)
        .expect("RF3 follower target");
    assert!(matches!(
        sample.target_qualifications.get(&target),
        Some(TargetQualification::Qualified { .. })
    ));
    let preconditions = sample.observed_preconditions_for(target);
    let echo = preconditions.echo();
    assert!(matches!(
        classify_group_control_layout(echo, Ok(&sample)),
        GroupControlLayoutObservation::SourceObserved {
            observed_leader,
            ..
        } if observed_leader == leader
    ));

    let mut stale_vote = preconditions.clone();
    stale_vote.observed_vote.term += 1;
    assert!(matches!(
        sample.check_transfer_preconditions(&stale_vote),
        Err(GroupControlPrecheckError::VoteChanged { .. })
    ));

    let follower_index = nodes
        .iter()
        .position(|node| node.node_id != leader)
        .expect("follower handle");
    let follower_error = read_group_control_sample(
        &nodes[follower_index].raft,
        group,
        nodes[follower_index].node_id,
        &node_ids,
        Duration::from_secs(10),
        Duration::from_secs(10),
    )
    .await
    .expect_err("follower cannot produce a leader sample");
    assert!(matches!(
        follower_error,
        GroupControlSampleError::NotLeader { .. }
    ));

    let wrong_local_error = read_group_control_sample(
        leader_raft,
        group,
        99,
        &node_ids,
        Duration::from_secs(10),
        Duration::from_secs(10),
    )
    .await
    .expect_err("wrong local identity cannot authorize a sample");
    assert!(matches!(
        wrong_local_error,
        GroupControlSampleError::NotLeader { .. }
    ));

    let expired_sample = read_group_control_sample(
        leader_raft,
        group,
        leader,
        &node_ids,
        Duration::ZERO,
        Duration::from_secs(1),
    )
    .await
    .expect_err("zero sample budget");
    assert!(matches!(
        expired_sample,
        GroupControlSampleError::SampleTooOld { .. }
    ));
    assert_eq!(
        leader_raft.metrics().borrow_watched().current_leader,
        Some(leader)
    );
    shutdown_nodes(nodes).await;
    let released = addresses
        .map(|address| std::net::TcpListener::bind(address).expect("released consumer listener"));
    drop(released);
}

async fn start_node(
    node_id: u64,
    group: GroupId,
    peers: &[(u64, SocketAddr)],
    data_dir: std::path::PathBuf,
) -> RawNode {
    std::fs::create_dir_all(&data_dir).expect("node data directory");
    let listener = TcpListener::bind(
        peers
            .iter()
            .find(|(peer, _)| *peer == node_id)
            .map(|(_, address)| *address)
            .expect("node listener address"),
    )
    .await
    .expect("bind node listener");
    let groups: GroupMap = Arc::new(Mutex::new(BTreeMap::new()));
    let server_groups = Arc::clone(&groups);
    let (stop, stopped) = oneshot::channel();
    let server = tokio::spawn(async move {
        tokio::select! {
            result = GrpcServer::serve_with_listener(listener, server_groups) => result,
            _ = stopped => Ok(()),
        }
    });

    let config = Config {
        heartbeat_interval: 100,
        election_timeout_min: 300,
        election_timeout_max: 600,
        snapshot_policy: SnapshotPolicy::Never,
        ..Default::default()
    };
    let config = Arc::new(config.validate().expect("valid RF3 config"));
    let state_machine = StateMachineStore::new(group, CounterFsm::new());
    let log_store = FileLogStoreOf::open(data_dir.join("raft")).expect("open Raft log");
    let network = GrpcNetworkFactory::new(GrpcRouter::new(peers.to_vec(), node_id), group);
    let raft = Raft::new(node_id, config, network, log_store, state_machine.clone())
        .await
        .expect("start raw Raft handle");
    groups.lock().expect("group map mutex").insert(
        group,
        GroupApp {
            node_id,
            group_id: group,
            raft: raft.clone(),
            state_machine,
        },
    );
    RawNode {
        node_id,
        raft,
        stop: Some(stop),
        server: Some(server),
    }
}

async fn wait_for_leader(nodes: &[RawNode], timeout: Duration) -> u64 {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        for node in nodes {
            let metrics = node.raft.metrics().borrow_watched().clone();
            if let (Ok(()), Some(leader)) = (metrics.running_state, metrics.current_leader) {
                return leader;
            }
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "RF3 leader deadline"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

async fn wait_for_qualified_target(
    raft: &Raft<CounterFsm>,
    group: GroupId,
    local_node_id: u64,
    voters: &[u64],
    timeout: Duration,
) -> multiraft_net::GroupControlSample {
    let target = voters
        .iter()
        .copied()
        .find(|&node_id| node_id != local_node_id)
        .expect("follower target");
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        match read_group_control_sample(
            raft,
            group,
            local_node_id,
            voters,
            Duration::from_secs(10),
            Duration::from_secs(10),
        )
        .await
        {
            Ok(sample)
                if matches!(
                    sample.target_qualifications.get(&target),
                    Some(TargetQualification::Qualified { .. })
                ) =>
            {
                return sample;
            }
            Ok(_) | Err(GroupControlSampleError::ReadIndexFailed { .. }) => {}
            Err(error) => panic!("control sample failed before qualification: {error}"),
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "target qualification deadline"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

async fn shutdown_nodes(mut nodes: Vec<RawNode>) {
    for node in &mut nodes {
        node.raft.shutdown().await.expect("shutdown raw Raft");
        if let Some(stop) = node.stop.take() {
            let _ = stop.send(());
        }
    }
    for node in &mut nodes {
        node.server
            .as_mut()
            .expect("retained listener")
            .await
            .expect("join gRPC server")
            .expect("server");
        node.server.take();
    }
}

impl Drop for RawNode {
    fn drop(&mut self) {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
        if let Some(server) = self.server.take() {
            server.abort();
        }
    }
}

fn reserve_addresses() -> [SocketAddr; 3] {
    let listeners = std::array::from_fn::<_, 3, _>(|_| {
        std::net::TcpListener::bind("127.0.0.1:0").expect("reserve RF3 address")
    });
    listeners.map(|listener| listener.local_addr().expect("read RF3 address"))
}
