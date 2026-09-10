//! Cross-process tonic MultiRaft: 3 nodes, 1 group, O(nodes) peer channels.

use std::net::SocketAddr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use multiraft_core::ClusterConfig;
use multiraft_fsm::{ApplyOut, CounterFsm, StateMachine};
use multiraft_net::wait_for_leader;
use multiraft_net::MultiRaft;

#[derive(Default)]
struct GrpcProbeFsm {
    value: i64,
}

impl StateMachine for GrpcProbeFsm {
    type Error = std::io::Error;

    fn apply(&mut self, _group: u64, _index: u64, data: &[u8]) -> Result<ApplyOut, Self::Error> {
        let bytes: [u8; 8] = data.try_into().map_err(|_| {
            std::io::Error::new(std::io::ErrorKind::InvalidData, "gRPC probe delta")
        })?;
        self.value += i64::from_le_bytes(bytes);
        Ok(ApplyOut::default())
    }

    fn snapshot(&self, _group: u64) -> Result<Vec<u8>, Self::Error> {
        Ok(self.value.to_le_bytes().to_vec())
    }

    fn restore(&mut self, _group: u64, snapshot: &[u8]) -> Result<(), Self::Error> {
        let bytes: [u8; 8] = snapshot.try_into().map_err(|_| {
            std::io::Error::new(std::io::ErrorKind::InvalidData, "gRPC probe snapshot")
        })?;
        self.value = i64::from_le_bytes(bytes);
        Ok(())
    }
}

fn free_addr() -> SocketAddr {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind free port");
    let addr = listener.local_addr().expect("local_addr");
    drop(listener);
    addr
}

async fn wait_for_leader_for<S: StateMachine>(
    nodes: &[MultiRaft<S>],
    group: u64,
    timeout: Duration,
) -> Option<u64> {
    let deadline = std::time::Instant::now() + timeout;
    while std::time::Instant::now() < deadline {
        for node in nodes {
            if node.is_leader(group) {
                return Some(node.node_id());
            }
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    None
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn grpc_three_node_propose() {
    let addrs = [free_addr(), free_addr(), free_addr()];
    let peer_ids = [1u64, 2, 3];
    let peers: Vec<(u64, SocketAddr)> = peer_ids
        .iter()
        .zip(addrs.iter())
        .map(|(&id, &addr)| (id, addr))
        .collect();

    let mut nodes = Vec::with_capacity(3);
    for &id in &peer_ids {
        let mut config = ClusterConfig::for_test(id, &peer_ids);
        config.peers = peers.clone();
        config.data_dir = Default::default();
        nodes.push(
            MultiRaft::start_grpc(config)
                .await
                .unwrap_or_else(|e| panic!("start_grpc node {id}: {e:#}")),
        );
    }

    let members = peer_ids.to_vec();
    let group = 1u64;
    for n in &nodes {
        n.create_group(group, &members)
            .await
            .unwrap_or_else(|e| panic!("create_group on {}: {e:?}", n.node_id()));
    }

    let leader_id = wait_for_leader(&nodes, group, Duration::from_secs(15))
        .await
        .expect("leader elected over gRPC");

    let leader = nodes
        .iter()
        .find(|n| n.node_id() == leader_id)
        .expect("leader handle");

    let data = CounterFsm::encode_add(7, /*idem=*/ 42);
    leader
        .propose(group, data)
        .await
        .expect("propose from leader");

    // Wait for all FSMs to apply.
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    loop {
        let mut values = Vec::new();
        for n in &nodes {
            let v = n
                .with_fsm(group, |fsm| fsm.value(group))
                .await
                .expect("fsm present");
            values.push(v);
        }
        if values.iter().all(|&v| v == 7) {
            break;
        }
        if std::time::Instant::now() >= deadline {
            panic!("FSM values did not converge to 7: {values:?}");
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    // Each node opens channels to the other 2 peers → sum ≈ 6, O(nodes) not O(groups).
    let link_sum: usize = nodes.iter().map(|n| n.unique_peer_links()).sum();
    assert!(
        link_sum < 10,
        "expected O(nodes) total unique_peer_links (<10), got {link_sum}"
    );
    assert!(
        link_sum <= 6,
        "3 nodes × ≤2 outbound peers → sum ≤ 6, got {link_sum}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn grpc_three_node_custom_factory_propose() {
    let calls = Arc::new(AtomicUsize::new(0));
    let addrs = [free_addr(), free_addr(), free_addr()];
    let peer_ids = [1u64, 2, 3];
    let peers = peer_ids
        .iter()
        .zip(addrs)
        .map(|(&id, addr)| (id, addr))
        .collect::<Vec<_>>();
    let mut nodes = Vec::new();
    for &id in &peer_ids {
        let mut config = ClusterConfig::for_test(id, &peer_ids);
        config.peers = peers.clone();
        let created = calls.clone();
        nodes.push(
            MultiRaft::start_grpc_with_factory(config, move |_| {
                created.fetch_add(1, Ordering::SeqCst);
                Ok(GrpcProbeFsm::default())
            })
            .await
            .expect("start custom gRPC node"),
        );
    }
    for node in &nodes {
        node.create_group(21, &peer_ids)
            .await
            .expect("create group");
    }
    let deadline = std::time::Instant::now() + Duration::from_secs(15);
    loop {
        let remaining = deadline.saturating_duration_since(std::time::Instant::now());
        let leader = wait_for_leader_for(&nodes, 21, remaining)
            .await
            .expect("gRPC leader");
        let node = nodes.iter().find(|node| node.node_id() == leader).unwrap();
        match node.propose(21, 7i64.to_le_bytes().to_vec()).await {
            Ok(_) => break,
            // Retry only a definite forwarding rejection, never an unknown write.
            Err(multiraft_core::MultiRaftError::NotLeader { .. }) => {
                assert!(
                    std::time::Instant::now() < deadline,
                    "leader changed throughout proposal deadline"
                );
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            Err(error) => panic!("propose: {error:?}"),
        }
    }
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while std::time::Instant::now() < deadline {
        let mut values = Vec::new();
        for node in &nodes {
            values.push(node.with_fsm(21, |fsm| fsm.value).await);
        }
        if values.iter().all(|value| *value == Some(7)) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert_eq!(calls.load(Ordering::SeqCst), 3);
    for node in &nodes {
        assert_eq!(node.with_fsm(21, |fsm| fsm.value).await, Some(7));
    }
    for node in &nodes {
        node.shutdown().await.expect("shutdown custom gRPC node");
    }
}
