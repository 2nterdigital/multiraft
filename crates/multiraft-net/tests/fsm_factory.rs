use multiraft_core::ClusterConfig;
use multiraft_fsm::{ApplyOut, StateMachine};
use multiraft_net::{FsmFactoryContext, MultiRaft, StateMachineFactory};

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
