use multiraft_core::{ClusterConfig, NodeRole, SnapshotMode};
use multiraft_fsm::{ApplyOut, StateMachine};
use multiraft_net::{FsmFactoryContext, MultiRaft, SharedFabric, StateMachineFactory};
use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};

#[derive(Default)]
struct ProbeFsm {
    value: i64,
}

impl ProbeFsm {
    fn new() -> Self {
        Self::default()
    }

    #[allow(dead_code)]
    fn encode_add(delta: i64) -> Vec<u8> {
        delta.to_le_bytes().to_vec()
    }

    #[allow(dead_code)]
    fn value(&self) -> i64 {
        self.value
    }
}

impl StateMachine for ProbeFsm {
    type Error = std::io::Error;

    fn apply(&mut self, _group: u64, _index: u64, data: &[u8]) -> Result<ApplyOut, Self::Error> {
        let bytes: [u8; 8] = data
            .try_into()
            .map_err(|_| std::io::Error::new(std::io::ErrorKind::InvalidData, "probe delta"))?;
        self.value += i64::from_le_bytes(bytes);
        Ok(ApplyOut::default())
    }

    fn snapshot(&self, _group: u64) -> Result<Vec<u8>, Self::Error> {
        Ok(self.value.to_le_bytes().to_vec())
    }

    fn restore(&mut self, _group: u64, snapshot: &[u8]) -> Result<(), Self::Error> {
        let bytes: [u8; 8] = snapshot
            .try_into()
            .map_err(|_| std::io::Error::new(std::io::ErrorKind::InvalidData, "probe snapshot"))?;
        self.value = i64::from_le_bytes(bytes);
        Ok(())
    }
}

async fn wait_for_probe_leader(
    nodes: &[MultiRaft<ProbeFsm>],
    group: u64,
    timeout: Duration,
) -> Option<u64> {
    let deadline = std::time::Instant::now() + timeout;
    while std::time::Instant::now() < deadline {
        for node in nodes {
            if let Some(leader) = node.leader(group) {
                if nodes.iter().any(|peer| peer.is_leader(group)) {
                    return Some(leader);
                }
            }
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    None
}

#[test]
fn closure_factory_is_public_and_receives_immutable_context() {
    let factory = |context: FsmFactoryContext| -> anyhow::Result<ProbeFsm> {
        let _node_id = context.node_id();
        let _group_id = context.group_id();
        Ok(ProbeFsm::new())
    };
    fn assert_factory<F: StateMachineFactory<ProbeFsm>>(_: &F) {}
    assert_factory(&factory);
}

#[tokio::test]
async fn start_with_factory_closure_applies_probe_fsm() {
    let config = ClusterConfig::for_test(1, &[1]);
    let node = MultiRaft::start_with_factory(config, |_| Ok(ProbeFsm::new()))
        .await
        .expect("start custom fsm");
    node.create_group(9, &[1])
        .await
        .expect("create first group");
    node.create_group(10, &[1])
        .await
        .expect("create second group");
    node.propose(9, ProbeFsm::encode_add(7))
        .await
        .expect("propose");
    assert_eq!(node.with_fsm(9, ProbeFsm::value).await, Some(7));
    assert_eq!(node.with_fsm(10, ProbeFsm::value).await, Some(0));
    node.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn counter_constructors_remain_type_inferred() {
    let single: MultiRaft = MultiRaft::start(ClusterConfig::for_test(1, &[1]))
        .await
        .expect("Counter start");
    single.shutdown().await.expect("shutdown Counter node");
}

#[tokio::test]
async fn shared_fabric_start_node_with_factory_uses_the_supplied_fsm() {
    let contexts = Arc::new(Mutex::new(Vec::new()));
    let recorded = contexts.clone();
    let fabric = SharedFabric::new();
    let node = fabric
        .start_node_with_factory(
            ClusterConfig::for_test(4, &[4]),
            move |ctx: FsmFactoryContext| {
                recorded
                    .lock()
                    .unwrap()
                    .push((ctx.node_id(), ctx.group_id()));
                Ok(ProbeFsm::new())
            },
        )
        .await
        .expect("start through fabric");
    node.create_group(12, &[4]).await.expect("create group");
    assert_eq!(*contexts.lock().unwrap(), vec![(4, 12)]);
    node.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn start_cluster_with_factory_converges_without_sharing_fsm_values() {
    let ids = [1, 2, 3];
    let configs = ids
        .iter()
        .map(|&id| ClusterConfig::for_test(id, &ids))
        .collect();
    let contexts = Arc::new(Mutex::new(Vec::new()));
    let recorded = contexts.clone();
    let nodes = MultiRaft::start_cluster_with_factory(configs, move |ctx: FsmFactoryContext| {
        recorded
            .lock()
            .unwrap()
            .push((ctx.node_id(), ctx.group_id()));
        Ok(ProbeFsm::new())
    })
    .await
    .expect("start custom cluster");
    for node in &nodes {
        node.create_group(5, &ids).await.expect("create group");
    }
    let leader = wait_for_probe_leader(&nodes, 5, Duration::from_secs(5))
        .await
        .expect("leader");
    nodes
        .iter()
        .find(|node| node.node_id() == leader)
        .expect("leader handle")
        .propose(5, ProbeFsm::encode_add(13))
        .await
        .expect("propose");
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while std::time::Instant::now() < deadline {
        let mut values = Vec::new();
        for node in &nodes {
            values.push(node.with_fsm(5, ProbeFsm::value).await);
        }
        if values.iter().all(|value| *value == Some(13)) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_eq!(contexts.lock().unwrap().len(), 3);
    for node in &nodes {
        assert_eq!(node.with_fsm(5, ProbeFsm::value).await, Some(13));
    }
    for node in &nodes {
        node.shutdown().await.expect("shutdown");
    }
}

#[tokio::test]
async fn factory_is_not_called_before_member_or_config_validation() {
    let calls = Arc::new(AtomicUsize::new(0));
    let called = calls.clone();
    let node = MultiRaft::start_with_factory(ClusterConfig::for_test(1, &[1]), move |_| {
        called.fetch_add(1, Ordering::SeqCst);
        Ok(ProbeFsm::new())
    })
    .await
    .expect("start");
    node.create_group(1, &[])
        .await
        .expect_err("invalid members");
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    node.shutdown().await.expect("shutdown");

    let mut invalid = ClusterConfig::for_test(1, &[1]);
    invalid.election_timeout_min_ms = 600;
    invalid.election_timeout_max_ms = 300;
    let calls = Arc::new(AtomicUsize::new(0));
    let called = calls.clone();
    let node = MultiRaft::start_with_factory(invalid, move |_| {
        called.fetch_add(1, Ordering::SeqCst);
        Ok(ProbeFsm::new())
    })
    .await
    .expect("start");
    node.create_group(2, &[1])
        .await
        .expect_err("invalid OpenRaft config");
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    node.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn factory_error_is_contextual_leaves_group_unpublished_and_is_retryable() {
    let calls = Arc::new(AtomicUsize::new(0));
    let recorded = calls.clone();
    let node = MultiRaft::start_with_factory(ClusterConfig::for_test(1, &[1]), move |_| {
        let call = recorded.fetch_add(1, Ordering::SeqCst);
        if call == 0 {
            Err(anyhow::anyhow!("reject probe group"))
        } else {
            Ok(ProbeFsm::new())
        }
    })
    .await
    .expect("start");
    let error = node
        .create_group(8, &[1])
        .await
        .expect_err("factory rejects first call");
    assert!(error.to_string().contains("create FSM for node 1, group 8"));
    assert_eq!(node.with_fsm(8, |_| ()).await, None);
    node.create_group(8, &[1]).await.expect("serialized retry");
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    node.shutdown().await.expect("shutdown");
}

struct DropProbeFsm {
    inner: ProbeFsm,
    drops: Arc<AtomicUsize>,
}

impl Drop for DropProbeFsm {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::SeqCst);
    }
}

impl StateMachine for DropProbeFsm {
    type Error = std::io::Error;

    fn apply(&mut self, group: u64, index: u64, data: &[u8]) -> Result<ApplyOut, Self::Error> {
        self.inner.apply(group, index, data)
    }

    fn snapshot(&self, group: u64) -> Result<Vec<u8>, Self::Error> {
        self.inner.snapshot(group)
    }

    fn restore(&mut self, group: u64, snapshot: &[u8]) -> Result<(), Self::Error> {
        self.inner.restore(group, snapshot)
    }
}

#[tokio::test]
async fn file_log_open_failure_drops_factory_fsm_on_default_voter_path_and_keeps_group_unpublished()
{
    let temp = tempfile::tempdir().expect("temporary data dir");
    let blocked = temp.path().join("group-7");
    std::fs::write(&blocked, b"not a directory").expect("block FileLog directory");
    let mut config = ClusterConfig::for_test(1, &[1]);
    config.data_dir = temp.path().to_path_buf();
    config.role = NodeRole::Voter;
    config.snapshot_mode = SnapshotMode::Disabled;
    let drops = Arc::new(AtomicUsize::new(0));
    let observed = drops.clone();
    let node = MultiRaft::start_with_factory(config, move |_| {
        Ok(DropProbeFsm {
            inner: ProbeFsm::new(),
            drops: observed.clone(),
        })
    })
    .await
    .expect("start");
    node.create_group(7, &[1])
        .await
        .expect_err("FileLog open fails");
    assert_eq!(node.with_fsm(7, |_| ()).await, None);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    std::fs::remove_file(blocked).expect("unblock FileLog directory");
    node.create_group(7, &[1])
        .await
        .expect("serialized retry after FileLog failure");
    node.shutdown().await.expect("shutdown");
}
