use multiraft_core::ClusterConfig;
use multiraft_fsm::{ApplyOut, StateMachine};
use multiraft_net::{FsmFactoryContext, MultiRaft, SharedFabric, StateMachineFactory};
use std::{
    sync::{Arc, Mutex},
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
